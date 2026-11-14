"""
Unit tests for source account handling in process_real_time_event()
in aggregator_worker.py

Tests cover how process_real_time_event() handles events whose resource account
differs from the EventBridge envelope account, and how tag mutations are applied.

AWS mocking: unittest.mock.patch is used to stub S3 read_state/write_state/write_deltas
so we can test the account validation logic without real AWS credentials.
"""

import os
import sys
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_eb_event(
    envelope_account="123456789012",
    resource_arn="arn:aws:ec2:us-east-1:123456789012:instance/i-abc123",
    event_name="CreateTags",
    tags=None,
    request_parameters=None,
):
    """
    Build a minimal EventBridge-wrapped CloudTrail event.
    The top-level 'account' is the EventBridge envelope (set by AWS service).
    The 'detail.userIdentity.accountId' is the CloudTrail identity.
    """
    if request_parameters is None:
        request_parameters = {
            "resourceArn": resource_arn,
            "tags": tags or {"Env": "prod"},
        }

    return {
        "version": "0",
        "id": "test-event-id",
        "source": "aws.ec2",
        "account": envelope_account,  # EventBridge envelope — authoritative
        "time": "2026-03-15T12:00:00Z",
        "region": "us-east-1",
        "detail-type": "AWS API Call via CloudTrail",
        "detail": {
            "eventName": event_name,
            "awsRegion": "us-east-1",
            "userIdentity": {
                "accountId": envelope_account,
            },
            "requestParameters": request_parameters,
        },
    }


class _NoSuchKey(Exception):
    pass


def make_s3_mock():
    """Return a mock s3 client that behaves like an empty state store."""
    mock = MagicMock()
    mock.exceptions.NoSuchKey = _NoSuchKey
    mock.get_object.side_effect = _NoSuchKey("Not Found")
    mock.put_object.return_value = {}
    return mock


# ---------------------------------------------------------------------------
# Import with patched boto3 so tests don't need AWS credentials
# ---------------------------------------------------------------------------

# We patch boto3.client at module import time. The actual client objects used
# inside aggregator_worker are module-level globals (s3, sqs), so we also patch
# those directly in each test where needed.

_import_s3_mock = make_s3_mock()
with patch("boto3.client", return_value=_import_s3_mock):
    import src.lambda_handlers.aggregator_worker as worker


# ---------------------------------------------------------------------------
# Account validation: matching accounts
# ---------------------------------------------------------------------------


class TestProcessRealTimeEventAccountValidation:

    def test_matching_account_processes_resource(self):
        """
        When EventBridge envelope account matches the resource ARN account,
        the resource should be processed (added as new_resource delta).
        """
        event = make_eb_event(
            envelope_account="123456789012",
            resource_arn="arn:aws:ec2:us-east-1:123456789012:instance/i-good",
        )
        mock_s3 = make_s3_mock()
        with patch.object(worker, "s3", mock_s3):
            with patch.object(worker, "sqs", MagicMock()):
                result = worker.process_real_time_event(event)

        assert result["resources_in_event"] == 1
        assert result.get("security_rejections", 0) == 0

    def test_arn_from_other_account_is_dropped(self):
        """
        A member account can only report on its own resources: ARNs naming a
        different account than the EventBridge envelope are dropped.
        """
        event = make_eb_event(
            envelope_account="111111111111",
            resource_arn="arn:aws:ec2:us-east-1:999999999999:instance/i-foreign",
        )
        with patch.object(worker, "read_state") as read_state:
            result = worker.process_real_time_event(event)

        assert result == {"processed": 0, "changed": 0}
        read_state.assert_not_called()

    def test_forged_user_identity_does_not_choose_state_owner(self):
        event = make_eb_event(
            envelope_account="111122223333",
            resource_arn="arn:aws:lambda:us-east-1:111122223333:function:f",
        )
        event["detail"]["userIdentity"]["accountId"] = "123456789012"
        with (
            patch.object(worker, "read_state", return_value={}) as read_state,
            patch.object(worker, "write_deltas", return_value="k"),
            patch.object(worker, "write_state"),
            patch.object(worker, "notify_results_q"),
        ):
            worker.process_real_time_event(event)
        assert read_state.call_args.args[0] == "111122223333"

    def test_s3_arn_no_account_component_allowed(self):
        """
        S3 bucket ARNs have no account ID in position [4] of the colon-split.
        These should NOT be rejected — pass-through when account field is empty.
        """
        event = make_eb_event(
            envelope_account="123456789012",
            resource_arn="arn:aws:s3:::amzn-s3-demo-bucket",
        )
        mock_s3 = make_s3_mock()
        with patch.object(worker, "s3", mock_s3):
            with patch.object(worker, "sqs", MagicMock()):
                result = worker.process_real_time_event(event)

        # S3 ARN has no account to validate — should pass through
        assert result.get("security_rejections", 0) == 0

    def test_envelope_account_preferred_over_cloudtrail_account(self):
        """
        The EventBridge envelope 'account' field is the authoritative source.
        Even if cloudtrail userIdentity.accountId is different (e.g. assumed role),
        we validate against the envelope account.
        """
        event = make_eb_event(
            envelope_account="123456789012",
            resource_arn="arn:aws:ec2:us-east-1:123456789012:instance/i-ok",
        )
        # Tamper with cloudtrail account to a different value
        event["detail"]["userIdentity"]["accountId"] = "999999999999"

        mock_s3 = make_s3_mock()
        with patch.object(worker, "s3", mock_s3):
            with patch.object(worker, "sqs", MagicMock()):
                result = worker.process_real_time_event(event)

        # Should use envelope_account (123456789012) and accept the resource
        assert result.get("security_rejections", 0) == 0

    def test_no_envelope_account_is_skipped(self):
        """
        Without the EventBridge envelope 'account' there is no trusted owner;
        userIdentity.accountId is caller-controlled, so the event is skipped.
        """
        event = make_eb_event(
            envelope_account="123456789012",
            resource_arn="arn:aws:ec2:us-east-1:123456789012:instance/i-ok",
        )
        del event["account"]  # Remove envelope account

        mock_s3 = make_s3_mock()
        with patch.object(worker, "s3", mock_s3):
            with patch.object(worker, "sqs", MagicMock()):
                result = worker.process_real_time_event(event)

        assert result == {"processed": 0, "changed": 0}

    def test_no_account_at_all_returns_zero_processed(self):
        """
        If neither envelope account nor cloudtrail account can be extracted,
        the event should be skipped entirely.
        """
        event = make_eb_event(
            envelope_account="",
            resource_arn="arn:aws:ec2:us-east-1:123456789012:instance/i-ok",
        )
        event["detail"]["userIdentity"]["accountId"] = ""

        mock_s3 = make_s3_mock()
        with patch.object(worker, "s3", mock_s3):
            with patch.object(worker, "sqs", MagicMock()):
                result = worker.process_real_time_event(event)

        assert result["processed"] == 0
        assert result["changed"] == 0

    def test_result_includes_account_id_and_region(self):
        """
        Successful processing should return account_id and region in result.
        """
        event = make_eb_event(
            envelope_account="123456789012",
            resource_arn="arn:aws:ec2:us-east-1:123456789012:instance/i-ok",
        )
        mock_s3 = make_s3_mock()
        with patch.object(worker, "s3", mock_s3):
            with patch.object(worker, "sqs", MagicMock()):
                result = worker.process_real_time_event(event)

        assert result.get("account_id") == "123456789012"
        assert result.get("region") == "us-east-1"
        assert result.get("trigger") == "realtime_event"


class TestProcessRealTimeEventTagMutation:

    def test_tag_resource_merges_tags_and_notifies_analytics(self):
        arn = "arn:aws:lambda:us-east-1:123456789012:function:demo"
        event = make_eb_event(
            resource_arn=arn,
            event_name="TagResource20170331v2",
            tags={"Team": "blue"},
        )
        previous_state = {arn: {"Environment": "prod"}}

        with (
            patch.object(worker, "read_state", return_value=previous_state),
            patch.object(worker, "write_deltas", return_value="deltas/test.json") as write_deltas,
            patch.object(worker, "write_state") as write_state,
            patch.object(worker, "notify_results_q") as notify_results,
        ):
            result = worker.process_real_time_event(event)

        delta = write_deltas.call_args.args[3][0]
        assert delta["previous_tags"] == {"Environment": "prod"}
        assert delta["tags"] == {"Environment": "prod", "Team": "blue"}
        assert write_state.call_args.args[2][arn] == delta["tags"]
        notify_results.assert_called_once_with(
            "123456789012", "us-east-1", "deltas/test.json", 1
        )
        assert result["deltas_written"] == 1

    def test_untag_resource_removes_only_requested_key(self):
        arn = "arn:aws:lambda:us-east-1:123456789012:function:demo"
        event = make_eb_event(
            resource_arn=arn,
            event_name="UntagResource20170331v2",
            request_parameters={
                "resource": arn,
                "tagKeys": ["ExampleTag"],
            },
        )
        previous_state = {
            arn: {
                "ExampleTag": "linked-us-east-1",
                "Environment": "prod",
            }
        }

        with (
            patch.object(worker, "read_state", return_value=previous_state),
            patch.object(worker, "write_deltas", return_value="deltas/test.json") as write_deltas,
            patch.object(worker, "write_state") as write_state,
            patch.object(worker, "notify_results_q") as notify_results,
        ):
            worker.process_real_time_event(event)

        delta = write_deltas.call_args.args[3][0]
        assert delta["previous_tags"]["ExampleTag"] == "linked-us-east-1"
        assert delta["tags"] == {"Environment": "prod"}
        assert write_state.call_args.args[2][arn] == {"Environment": "prod"}
        notify_results.assert_called_once()

    def test_ec2_delete_tags_parses_tag_set(self):
        arn = "arn:aws:ec2:us-east-1:123456789012:security-group/sg-123"
        event = make_eb_event(
            event_name="DeleteTags",
            request_parameters={
                "resourcesSet": {"items": [{"resourceId": "sg-123"}]},
                "tagSet": {"items": [{"key": "ExampleTag"}]},
            },
        )
        previous_state = {
            arn: {
                "ExampleTag": "linked-us-east-1",
                "Owner": "platform",
            }
        }

        with (
            patch.object(worker, "read_state", return_value=previous_state),
            patch.object(worker, "write_deltas", return_value="deltas/test.json") as write_deltas,
            patch.object(worker, "write_state"),
            patch.object(worker, "notify_results_q"),
        ):
            worker.process_real_time_event(event)

        delta = write_deltas.call_args.args[3][0]
        assert delta["tags"] == {"Owner": "platform"}
        assert delta["previous_tags"]["ExampleTag"] == "linked-us-east-1"

    def test_event_without_explicit_tag_mutation_does_not_clobber_state(self):
        arn = "arn:aws:lambda:us-east-1:123456789012:function:demo"
        event = make_eb_event(
            resource_arn=arn,
            event_name="UpdateFunctionConfiguration",
            request_parameters={"resource": arn},
        )
        previous_state = {arn: {"Environment": "prod"}}

        with (
            patch.object(worker, "read_state", return_value=previous_state),
            patch.object(worker, "write_deltas") as write_deltas,
            patch.object(worker, "write_state") as write_state,
            patch.object(worker, "notify_results_q") as notify_results,
        ):
            result = worker.process_real_time_event(event)

        write_deltas.assert_not_called()
        write_state.assert_not_called()
        notify_results.assert_not_called()
        assert result["deltas_written"] == 0

    def test_sqs_tag_queue_resolves_queue_url_to_arn(self):
        arn = "arn:aws:sqs:us-east-1:123456789012:orders"
        event = make_eb_event(
            event_name="TagQueue",
            request_parameters={
                "queueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/orders",
                "tags": {"Team": "blue"},
            },
        )

        with (
            patch.object(worker, "read_state", return_value={arn: {"Env": "prod"}}),
            patch.object(worker, "write_deltas", return_value="deltas/test.json") as write_deltas,
            patch.object(worker, "write_state"),
            patch.object(worker, "notify_results_q"),
        ):
            worker.process_real_time_event(event)

        delta = write_deltas.call_args.args[3][0]
        assert delta["arn"] == arn
        assert delta["tags"] == {"Env": "prod", "Team": "blue"}

    def test_sqs_untag_queue_removes_key(self):
        arn = "arn:aws:sqs:us-west-2:123456789012:orders"
        event = make_eb_event(
            event_name="UntagQueue",
            request_parameters={
                "queueUrl": "https://sqs.us-west-2.amazonaws.com/123456789012/orders",
                "tagKeys": ["Team"],
            },
        )

        with (
            patch.object(worker, "read_state", return_value={arn: {"Env": "prod", "Team": "blue"}}),
            patch.object(worker, "write_deltas", return_value="deltas/test.json") as write_deltas,
            patch.object(worker, "write_state"),
            patch.object(worker, "notify_results_q"),
        ):
            worker.process_real_time_event(event)

        delta = write_deltas.call_args.args[3][0]
        assert delta["arn"] == arn
        assert delta["tags"] == {"Env": "prod"}

    def test_dynamodb_tag_list_is_parsed(self):
        arn = "arn:aws:dynamodb:us-east-1:123456789012:table/orders"
        event = make_eb_event(
            event_name="TagResource",
            request_parameters={
                "resourceArn": arn,
                "tags": [{"key": "Team", "value": "blue"}],
            },
        )

        with (
            patch.object(worker, "read_state", return_value={arn: {"Env": "prod"}}),
            patch.object(worker, "write_deltas", return_value="deltas/test.json") as write_deltas,
            patch.object(worker, "write_state"),
            patch.object(worker, "notify_results_q"),
        ):
            worker.process_real_time_event(event)

        assert write_deltas.call_args.args[3][0]["tags"] == {"Env": "prod", "Team": "blue"}


class TestRealTimeDeltaShape:

    def _deltas(self, event, previous=None):
        with (
            patch.object(worker, "read_state", return_value=dict(previous or {})),
            patch.object(worker, "write_deltas", return_value="k") as write_deltas,
            patch.object(worker, "write_state"),
            patch.object(worker, "notify_results_q"),
        ):
            worker.process_real_time_event(event)
        return write_deltas.call_args.args[3] if write_deltas.called else []

    def test_records_caller_and_api_name_with_schema_change_type(self):
        arn = "arn:aws:lambda:us-east-1:123456789012:function:demo"
        event = make_eb_event(resource_arn=arn, event_name="TagResource20170331v2", tags={"Team": "blue"})
        event["detail"]["userIdentity"]["arn"] = "arn:aws:sts::123456789012:assumed-role/Admin/alice"
        delta = self._deltas(event, {arn: {"Env": "prod"}})[0]
        assert delta["change_type"] == "tags_changed"
        assert delta["event_name"] == "TagResource20170331v2"
        assert delta["changed_by"] == "arn:aws:sts::123456789012:assumed-role/Admin/alice"

    def test_changed_by_falls_back_to_principal_id(self):
        arn = "arn:aws:lambda:us-east-1:123456789012:function:demo"
        event = make_eb_event(resource_arn=arn, tags={"Team": "blue"})
        event["detail"]["userIdentity"]["principalId"] = "AROAEXAMPLE:alice"
        assert self._deltas(event)[0]["changed_by"] == "AROAEXAMPLE:alice"

    def test_rds_add_tags_to_resource(self):
        arn = "arn:aws:rds:us-east-1:123456789012:db:orders"
        event = make_eb_event(event_name="AddTagsToResource", request_parameters={
            "resourceName": arn, "tags": [{"key": "Team", "value": "blue"}]})
        delta = self._deltas(event, {arn: {"Env": "prod"}})[0]
        assert delta["arn"] == arn and delta["tags"] == {"Env": "prod", "Team": "blue"}

    def test_rds_remove_tags_from_resource(self):
        arn = "arn:aws:rds:us-east-1:123456789012:db:orders"
        event = make_eb_event(event_name="RemoveTagsFromResource", request_parameters={
            "resourceName": arn, "tagKeys": ["Team"]})
        assert self._deltas(event, {arn: {"Env": "prod", "Team": "blue"}})[0]["tags"] == {"Env": "prod"}

    def test_elbv2_add_tags_resource_arns(self):
        arns = [f"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/lb{i}/1" for i in range(2)]
        event = make_eb_event(event_name="AddTags", request_parameters={
            "resourceArns": arns, "tags": [{"key": "Team", "value": "blue"}]})
        assert sorted(d["arn"] for d in self._deltas(event)) == arns

    def test_fallback_ignores_caller_identity_arns(self):
        event = make_eb_event(event_name="TagSomething", request_parameters={
            "tags": {"Team": "blue"},
            "role": "arn:aws:iam::123456789012:role/not-a-target"})
        event["detail"]["userIdentity"]["arn"] = "arn:aws:sts::123456789012:assumed-role/Admin/alice"
        event["detail"]["responseElements"] = {
            "thing": {"arn": "arn:aws:kinesis:us-east-1:123456789012:stream/s1"}}
        assert [d["arn"] for d in self._deltas(event)] == ["arn:aws:kinesis:us-east-1:123456789012:stream/s1"]


class TestTagChangeCallerRecords:
    """Caller identity goes to tag-change-events/ even when state already matches."""

    ARN = "arn:aws:lambda:us-east-1:123456789012:function:demo"

    def _run(self, event, previous):
        s3 = make_s3_mock()
        with (
            patch.object(worker, "s3", s3),
            patch.object(worker, "read_state", return_value=dict(previous)),
            patch.object(worker, "write_deltas", return_value="k") as write_deltas,
            patch.object(worker, "write_state"),
            patch.object(worker, "notify_results_q"),
        ):
            result = worker.process_real_time_event(event)
        return result, s3, write_deltas

    def _event(self):
        event = make_eb_event(resource_arn=self.ARN, event_name="TagResource20170331v2", tags={"Env": "prod"})
        event["detail"].update({
            "eventID": "6f1c2d3e-0000-4000-8000-abcdefabcdef",
            "eventTime": "2026-11-12T23:13:43Z",
            "userIdentity": {"accountId": "123456789012", "type": "AssumedRole",
                             "arn": "arn:aws:sts::123456789012:assumed-role/Admin/alice"},
        })
        return event

    def test_caller_recorded_when_aws_tag_already_updated_state(self):
        # The native aws.tag event already applied Env=prod, so no delta is written...
        result, s3, write_deltas = self._run(self._event(), {self.ARN: {"Env": "prod"}})
        assert not write_deltas.called
        # ...but the caller is still recorded.
        assert result["callers_recorded"] == 1
        put = s3.put_object.call_args.kwargs
        assert put["Key"] == ("tag-change-events/year=2026/month=11/day=12/"
                              "123456789012-6f1c2d3e-0000-4000-8000-abcdefabcdef.json")
        row = __import__("json").loads(put["Body"].decode().strip())
        assert row == {
            "event_time": 1794525223, "event_id": "6f1c2d3e-0000-4000-8000-abcdefabcdef",
            "event_name": "TagResource20170331v2", "account_id": "123456789012", "region": "us-east-1",
            "arn": self.ARN, "changed_by": "arn:aws:sts::123456789012:assumed-role/Admin/alice",
            "principal_type": "AssumedRole", "tags_set": {"Env": "prod"}, "tags_removed": [],
        }

    def test_untrusted_event_id_is_not_used_in_key(self):
        event = self._event()
        event["detail"]["eventID"] = "../../state/x"
        event["id"] = "bad id/.."
        result, s3, _ = self._run(event, {})
        assert result["callers_recorded"] == 0
        assert not any(c.kwargs.get("Key", "").startswith("tag-change-events/")
                       for c in s3.put_object.call_args_list)

    def test_event_without_tag_mutation_records_nothing(self):
        event = make_eb_event(resource_arn=self.ARN, request_parameters={"resourceArn": self.ARN})
        result, s3, _ = self._run(event, {})
        assert result.get("callers_recorded", 0) == 0


class TestFailedApiCalls:
    """G1: CloudTrail also records calls that failed (errorCode set)."""

    ARN = "arn:aws:lambda:us-east-1:123456789012:function:demo"

    def test_failed_tag_call_changes_nothing(self):
        event = make_eb_event(resource_arn=self.ARN, event_name="TagResource20170331v2",
                              tags={"CostCenter": "forged"})
        event["detail"].update({
            "eventID": "6f1c2d3e-0000-4000-8000-abcdefabcdef",
            "errorCode": "AccessDenied",
            "errorMessage": "User is not authorized to perform: lambda:TagResource",
        })
        s3 = make_s3_mock()
        with (
            patch.object(worker, "s3", s3),
            patch.object(worker, "read_state", return_value={self.ARN: {"Env": "prod"}}),
            patch.object(worker, "write_deltas", return_value="k") as write_deltas,
            patch.object(worker, "write_state") as write_state,
            patch.object(worker, "notify_results_q"),
        ):
            result = worker.process_real_time_event(event)
        assert not write_deltas.called
        assert not write_state.called
        s3.put_object.assert_not_called()  # no tag-change-events row either
        assert result.get("deltas_written", 0) == 0


class TestPartitionAwareArns:
    """G5: ARNs built from IDs, bucket names and queue URLs use the Region's partition."""

    ACCT = "123456789012"

    def _arns(self, region, request_parameters):
        detail = {"eventName": "CreateTags", "awsRegion": region,
                  "requestParameters": request_parameters}
        return [r["arn"] for r in worker.extract_resources_from_cloudtrail_event(detail, self.ACCT)]

    def test_ec2_id_in_govcloud(self):
        arns = self._arns("us-gov-west-1", {
            "resourcesSet": {"items": [{"resourceId": "i-0abc"}]},
            "tagSet": {"items": [{"key": "Env", "value": "prod"}]}})
        assert arns == ["arn:aws-us-gov:ec2:us-gov-west-1:123456789012:instance/i-0abc"]

    def test_s3_bucket_in_china(self):
        arns = self._arns("cn-north-1", {"bucketName": "amzn-s3-demo-bucket", "tags": {"Env": "prod"}})
        assert arns == ["arn:aws-cn:s3:::amzn-s3-demo-bucket"]

    def test_sqs_queue_url_in_china(self):
        arns = self._arns("cn-north-1", {
            "queueUrl": "https://sqs.cn-north-1.amazonaws.com.cn/123456789012/orders",
            "tags": {"Env": "prod"}})
        assert arns == ["arn:aws-cn:sqs:cn-north-1:123456789012:orders"]

    def test_commercial_unchanged(self):
        arns = self._arns("us-east-1", {"bucketName": "amzn-s3-demo-bucket", "tags": {"Env": "prod"}})
        assert arns == ["arn:aws:s3:::amzn-s3-demo-bucket"]

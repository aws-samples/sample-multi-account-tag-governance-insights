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

    def test_cross_account_event_still_processed(self):
        """
        Events where resource ARN account differs from envelope account
        are still processed (strict ARN-account validation was removed).
        """
        event = make_eb_event(
            envelope_account="111111111111",
            resource_arn="arn:aws:ec2:us-east-1:999999999999:instance/i-foreign",
        )
        mock_s3 = make_s3_mock()
        with patch.object(worker, "s3", mock_s3):
            with patch.object(worker, "sqs", MagicMock()):
                result = worker.process_real_time_event(event)

        assert result["resources_in_event"] == 1
        assert result["account_id"] == "111111111111"

    def test_s3_arn_no_account_component_allowed(self):
        """
        S3 bucket ARNs have no account ID in position [4] of the colon-split.
        These should NOT be rejected — pass-through when account field is empty.
        """
        event = make_eb_event(
            envelope_account="123456789012",
            resource_arn="arn:aws:s3:::my-bucket",
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

    def test_no_envelope_account_falls_back_to_cloudtrail(self):
        """
        If the EventBridge envelope 'account' field is absent (direct invocation
        without EB wrapper), fall back to cloudtrail userIdentity.accountId.
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

        # Should fall back to cloudtrail account and still accept matching resource
        assert result.get("security_rejections", 0) == 0

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

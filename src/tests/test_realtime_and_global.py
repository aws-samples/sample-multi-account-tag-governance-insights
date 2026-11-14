"""
Realtime tag parsing, "who changed" coverage and global resources.

Ported from fix/e-realtime-and-global (review M1, M2, M3, m3, m14) onto the
conditional-write state code. Event shapes come from AWS docs, not captured
events. Each test names its page.
"""

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class _NoSuchKey(Exception):
    pass


def _s3():
    m = MagicMock()
    m.exceptions.NoSuchKey = _NoSuchKey
    m.get_object.side_effect = _NoSuchKey("nf")
    return m


with patch("boto3.client", return_value=_s3()):
    import src.lambda_handlers.aggregator_worker as worker

ACCT = "111122223333"
CALLER = f"arn:aws:sts::{ACCT}:assumed-role/Admin/alice"
BUCKET = "amzn-s3-demo-bucket"
BUCKET_ARN = f"arn:aws:s3:::{BUCKET}"


def _event(event_name, request_parameters, region="us-east-1", source="aws.ec2",
           event_time="2026-11-12T23:13:43Z", event_id="6f1c2d3e-0000-4000-8000-abcdefabcdef"):
    return {
        "account": ACCT, "region": region, "source": source,
        "detail-type": "AWS API Call via CloudTrail",
        "detail": {
            "eventName": event_name, "awsRegion": region, "eventTime": event_time,
            "eventID": event_id,
            "userIdentity": {"type": "AssumedRole", "accountId": ACCT, "arn": CALLER},
            "requestParameters": request_parameters,
        },
    }


def _run(event, previous=None):
    """Run process_real_time_event; return (result, {region: state written}, deltas, caller rows)."""
    previous = previous or {}
    s3 = _s3()
    with (
        patch.object(worker, "s3", s3),
        patch.object(worker, "read_state", side_effect=lambda a, r: dict(previous.get(r, {}))),
        patch.object(worker, "write_deltas", return_value="k") as write_deltas,
        patch.object(worker, "write_state") as write_state,
        patch.object(worker, "notify_results_q"),
    ):
        result = worker.process_real_time_event(event)
    states = {c.args[1]: c.args[2] for c in write_state.call_args_list}
    deltas = [d for c in write_deltas.call_args_list for d in c.args[3]]
    rows = [json.loads(line)
            for c in s3.put_object.call_args_list
            if c.kwargs.get("Key", "").startswith("tag-change-events/")
            for line in c.kwargs["Body"].decode().splitlines()]
    return result, states, deltas, rows


# ── S3 PutBucketTagging / DeleteBucketTagging: whole-set replace ──────────────

class TestS3BucketTagging:
    # Tagging > TagSet > Tag (Key, Value); PutBucketTagging replaces the set:
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html

    def test_put_bucket_tagging_list_replaces_whole_set(self):
        result, states, deltas, rows = _run(_event("PutBucketTagging", {
            "bucketName": BUCKET,
            "Tagging": {"TagSet": {"Tag": [{"Key": "Team", "Value": "blue"},
                                           {"Key": "Env", "Value": "prod"}]}}}, source="aws.s3"),
            previous={"us-east-1": {BUCKET_ARN: {"Old": "gone", "Env": "dev"}}})
        assert result["callers_recorded"] == 1
        assert states["us-east-1"][BUCKET_ARN] == {"Team": "blue", "Env": "prod"}
        assert deltas[0]["tags"] == {"Team": "blue", "Env": "prod"}
        assert deltas[0]["previous_tags"] == {"Old": "gone", "Env": "dev"}
        assert rows[0]["tags_set"] == {"Team": "blue", "Env": "prod"}
        assert rows[0]["tags_removed"] == ["Old"]

    def test_put_bucket_tagging_single_tag_dict(self):
        result, _, _, rows = _run(_event("PutBucketTagging", {
            "bucketName": BUCKET,
            "Tagging": {"TagSet": {"Tag": {"Key": "Team", "Value": "blue"}}}}, source="aws.s3"))
        assert result["callers_recorded"] == 1 and rows[0]["tags_set"] == {"Team": "blue"}

    def test_delete_bucket_tagging_removes_every_key(self):
        # https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketTagging.html
        result, states, deltas, rows = _run(
            _event("DeleteBucketTagging", {"bucketName": BUCKET, "tagging": ""}, source="aws.s3"),
            previous={"us-east-1": {BUCKET_ARN: {"Team": "blue", "Env": "prod"}}})
        assert result["callers_recorded"] == 1 and rows[0]["event_name"] == "DeleteBucketTagging"
        assert rows[0]["tags_set"] == {} and rows[0]["tags_removed"] == ["Env", "Team"]
        assert states["us-east-1"][BUCKET_ARN] == {}
        assert deltas[0]["previous_tags"] == {"Team": "blue", "Env": "prod"} and deltas[0]["tags"] == {}

    def test_put_bucket_tagging_row_names_the_caller(self):
        # userIdentity.arn is the caller:
        # https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-event-reference-user-identity.html
        _, _, _, rows = _run(_event("PutBucketTagging", {
            "bucketName": BUCKET,
            "Tagging": {"TagSet": {"Tag": [{"Key": "Team", "Value": "blue"}]}}}, source="aws.s3"))
        assert len(rows) == 1
        assert rows[0]["arn"] == BUCKET_ARN
        assert rows[0]["changed_by"] == CALLER
        assert rows[0]["principal_type"] == "AssumedRole"
        assert rows[0]["event_name"] == "PutBucketTagging"

    def test_merge_replace_drops_key_a_concurrent_writer_added(self):
        base = {BUCKET_ARN: {"Old": "x"}}
        ours = {BUCKET_ARN: {"Team": "a"}}
        theirs = {BUCKET_ARN: {"Old": "x", "Owner": "b"}}
        assert worker._merge_state(base, ours, theirs, replaced={BUCKET_ARN}) == ours
        # Without a replace it is an ordinary per-key merge.
        assert worker._merge_state(base, ours, theirs) == {BUCKET_ARN: {"Team": "a", "Owner": "b"}}


# ── CloudFront {items: [...]} wrapper ─────────────────────────────────────────

class TestItemsWrapper:
    # CloudFront Tags and TagKeys wrap their lists in Items:
    # https://docs.aws.amazon.com/cloudfront/latest/APIReference/API_Tags.html
    # https://docs.aws.amazon.com/cloudfront/latest/APIReference/API_TagKeys.html
    ARN = f"arn:aws:cloudfront::{ACCT}:distribution/EDFDVBD6EXAMPLE"

    def test_tag_resource_items_unwrapped(self):
        updates, removed = worker._extract_tag_mutation({"eventName": "TagResource", "requestParameters": {
            "resource": self.ARN, "tags": {"items": [{"key": "Team", "value": "blue"}]}}})[:2]
        assert updates == {"Team": "blue"} and removed == []

    def test_untag_resource_items_unwrapped(self):
        updates, removed = worker._extract_tag_mutation({"eventName": "UntagResource", "requestParameters": {
            "resource": self.ARN, "tagKeys": {"items": ["Team"]}}})[:2]
        assert updates == {} and removed == ["Team"]


# ── ARN builders: KMS keyId, classic ELB, EC2 ID prefixes ─────────────────────

KEY_ID = "1234abcd-12ab-34cd-56ef-1234567890ab"


class TestArnBuilders:

    def test_kms_tag_resource_key_arn(self):
        # CloudTrail example (keyId ARN, tags [{tagKey, tagValue}]):
        # https://docs.aws.amazon.com/kms/latest/developerguide/ct-tagresource.html
        arn = f"arn:aws:kms:us-west-2:{ACCT}:key/{KEY_ID}"
        result, states, _, rows = _run(_event("TagResource", {
            "keyId": arn, "tags": [{"tagKey": "Department", "tagValue": "IT"}]},
            region="us-west-2", source="aws.kms"))
        assert result["callers_recorded"] == 1
        assert rows[0]["arn"] == arn and rows[0]["tags_set"] == {"Department": "IT"}
        assert states["us-west-2"][arn] == {"Department": "IT"}

    def test_kms_tag_resource_bare_key_id(self):
        # KeyId may be a key ID or key ARN:
        # https://docs.aws.amazon.com/kms/latest/APIReference/API_TagResource.html
        _, _, _, rows = _run(_event("TagResource", {
            "keyId": KEY_ID, "tags": [{"tagKey": "Department", "tagValue": "IT"}]},
            region="us-west-2", source="aws.kms"))
        assert rows[0]["arn"] == f"arn:aws:kms:us-west-2:{ACCT}:key/{KEY_ID}"

    def test_kms_untag_resource(self):
        # https://docs.aws.amazon.com/kms/latest/APIReference/API_UntagResource.html
        arn = f"arn:aws:kms:us-west-2:{ACCT}:key/{KEY_ID}"
        _, states, _, _ = _run(_event("UntagResource", {"keyId": KEY_ID, "tagKeys": ["Department"]},
                                      region="us-west-2", source="aws.kms"),
                               previous={"us-west-2": {arn: {"Department": "IT", "Owner": "x"}}})
        assert states["us-west-2"][arn] == {"Owner": "x"}

    def test_classic_elb_add_tags(self):
        # LoadBalancerNames + Tags (Key, Value):
        # https://docs.aws.amazon.com/elasticloadbalancing/2012-06-01/APIReference/API_AddTags.html
        result, _, _, rows = _run(_event("AddTags", {
            "loadBalancerNames": ["my-classic-lb"], "tags": [{"key": "Team", "value": "blue"}]},
            source="aws.elasticloadbalancing"))
        assert result["callers_recorded"] == 1
        assert rows[0]["arn"] == f"arn:aws:elasticloadbalancing:us-east-1:{ACCT}:loadbalancer/my-classic-lb"

    @pytest.mark.parametrize("region, partition", [("cn-north-1", "aws-cn"), ("us-gov-west-1", "aws-us-gov")])
    def test_new_builders_are_partition_aware(self, region, partition):
        # ARN partitions: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html
        def arns(params):
            return [r["arn"] for r in worker.extract_resources_from_cloudtrail_event(
                {"eventName": "TagResource", "awsRegion": region, "requestParameters": params}, ACCT)]
        assert arns({"keyId": KEY_ID, "tags": [{"tagKey": "a", "tagValue": "b"}]}) == \
            [f"arn:{partition}:kms:{region}:{ACCT}:key/{KEY_ID}"]
        assert arns({"loadBalancerNames": ["lb"], "tags": [{"key": "a", "value": "b"}]}) == \
            [f"arn:{partition}:elasticloadbalancing:{region}:{ACCT}:loadbalancer/lb"]
        assert worker._construct_ec2_arn("igw-0123456789abcdef0", ACCT, region) == \
            f"arn:{partition}:ec2:{region}:{ACCT}:internet-gateway/igw-0123456789abcdef0"

    @pytest.mark.parametrize("resource_id, resource_type", [
        ("igw-0123456789abcdef0", "internet-gateway"),
        ("rtb-0123456789abcdef0", "route-table"),
        ("nat-0123456789abcdef0", "natgateway"),
        ("vpce-0123456789abcdef0", "vpc-endpoint"),
        ("lt-0123456789abcdef0", "launch-template"),
        ("tgw-0123456789abcdef0", "transit-gateway"),
        ("tgw-attach-0123456789abcdef0", "transit-gateway-attachment"),
        ("eipalloc-0123456789abcdef0", "elastic-ip"),
        ("acl-0123456789abcdef0", "network-acl"),
    ])
    def test_ec2_prefix_table(self, resource_id, resource_type):
        # ARN formats: "Resource types defined by Amazon EC2"
        # https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazonec2.html
        assert worker._construct_ec2_arn(resource_id, ACCT, "us-east-1") == \
            f"arn:aws:ec2:us-east-1:{ACCT}:{resource_type}/{resource_id}"


# ── Global resources: state region 'global' ───────────────────────────────────

MEMBER = "444455556666"
CF_ARN = f"arn:aws:cloudfront::{ACCT}:distribution/EDFDVBD6EXAMPLE"


class TestGlobalResources:
    # region:global is the Resource Explorer filter for global resources:
    # https://docs.aws.amazon.com/resource-explorer/latest/userguide/using-search-query-syntax.html

    def test_cloudfront_tag_is_filed_under_global(self):
        # CloudFront ARNs have no Region field:
        # https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazoncloudfront.html
        _, states, deltas, rows = _run(_event("TagResource", {
            "resource": CF_ARN, "tags": {"items": [{"key": "Team", "value": "blue"}]}}))
        assert list(states) == ["global"]
        assert states["global"][CF_ARN] == {"Team": "blue"}
        assert deltas[0]["region"] == "global"
        assert rows[0]["region"] == "global"

    def test_tag_change_event_for_global_arn_is_filed_under_global(self):
        event = {"account": ACCT, "region": "us-east-1", "detail-type": "Tag Change on Resource",
                 "resources": [CF_ARN], "detail": {"tags": {"Team": "blue"}}}
        with (
            patch.object(worker, "read_state", return_value={}) as read_state,
            patch.object(worker, "write_deltas", return_value="k"),
            patch.object(worker, "write_state") as write_state,
            patch.object(worker, "notify_results_q"),
        ):
            worker.process_tag_change_event(event)
        read_state.assert_called_once_with(ACCT, "global")
        assert write_state.call_args.args[1] == "global"

    def test_empty_global_member_inventory_does_not_fail_forever(self):
        previous = {f"arn:aws:cloudfront::{MEMBER}:distribution/E1": {"Team": "blue"}}
        with (
            patch.object(worker, "_HUB_ACCOUNT_ID", ACCT),  # never call STS from tests
            patch.object(worker, "discover_resources_for_account_region", return_value={}),
            patch.object(worker, "read_state", return_value=previous),
            patch.object(worker, "write_deltas") as write_deltas,
            patch.object(worker, "write_state") as write_state,
        ):
            result = worker.process_account_region_scan(MEMBER, "global", "scan-1")
        write_deltas.assert_not_called()
        write_state.assert_not_called()  # state kept, nothing recorded as removed
        assert result["skipped"] == "empty_global_inventory"


# ── Unparsed tag events are counted ───────────────────────────────────────────

def test_event_without_mutation_emits_metric(capsys):
    # CloudWatch embedded metric format:
    # https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format_Specification.html
    _run(_event("TagResource", {"somethingElse": "x"}))
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    metric = [m for m in lines if "UnparsedTagEvents" in m]
    assert metric and metric[0]["EventName"] == "TagResource"
    assert metric[0]["_aws"]["CloudWatchMetrics"][0]["Metrics"][0]["Name"] == "UnparsedTagEvents"


# ── m3: delta timestamp is CloudTrail eventTime ───────────────────────────────

def test_realtime_delta_uses_cloudtrail_event_time():
    # eventTime is ISO 8601 UTC:
    # https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-event-reference-record-contents.html
    _, _, deltas, rows = _run(_event("TagResource", {
        "resourceArn": f"arn:aws:lambda:us-east-1:{ACCT}:function:f", "tags": {"Team": "blue"}},
        event_time="2026-11-12T23:13:43Z"))
    assert deltas[0]["timestamp"] == 1794525223 == rows[0]["event_time"]


# ── m14: EC2 DeleteTags with a value only removes on match ────────────────────

class TestEc2DeleteTagsValue:
    # Key with value deletes only if the value matches; key alone deletes any value:
    # https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DeleteTags.html
    ARN = f"arn:aws:ec2:us-east-1:{ACCT}:instance/i-0123456789abcdef0"

    def _delete(self, tag_item):
        return _event("DeleteTags", {
            "resourcesSet": {"items": [{"resourceId": "i-0123456789abcdef0"}]},
            "tagSet": {"items": [tag_item]}})

    def test_value_mismatch_keeps_tag(self):
        _, states, deltas, _ = _run(self._delete({"key": "Env", "value": "dev"}),
                                    previous={"us-east-1": {self.ARN: {"Env": "prod"}}})
        assert deltas == [] and states == {}

    def test_empty_value_only_matches_empty(self):
        _, states, _, _ = _run(self._delete({"key": "Env", "value": ""}),
                               previous={"us-east-1": {self.ARN: {"Env": "prod"}}})
        assert states == {}

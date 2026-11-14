"""
Unit tests for read_state() and write_state() in aggregator_worker.py.

These functions manage compressed S3 state files for per-account/region
tag snapshots used in delta computation.
"""

import gzip
import json
import os
import sys

import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from botocore.exceptions import ClientError


class NoSuchKey(Exception):
    pass


def _make_s3_mock():
    """Return a mock S3 client with proper exceptions attribute."""
    mock = MagicMock()
    mock.exceptions.NoSuchKey = NoSuchKey
    return mock


with patch("boto3.client", return_value=_make_s3_mock()):
    import src.lambda_handlers.aggregator_worker as worker


def make_compressed_state(state: dict) -> bytes:
    return gzip.compress(json.dumps(state).encode("utf-8"))


class TestReadState:

    def test_first_scan_returns_empty_dict(self):
        """On first scan (NoSuchKey), state is {}."""
        mock_s3 = _make_s3_mock()
        mock_s3.get_object.side_effect = NoSuchKey("Not Found")

        with patch.object(worker, "s3", mock_s3):
            state = worker.read_state("123456789012", "us-east-1")

        assert state == {}

    def test_existing_state_returns_parsed_state(self):
        """When state file exists, returns parsed dict."""
        existing_state = {"arn:aws:ec2:us-east-1:123:instance/i-abc": {"Env": "prod"}}
        compressed = make_compressed_state(existing_state)

        mock_s3 = _make_s3_mock()
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=compressed)),
        }

        with patch.object(worker, "s3", mock_s3):
            state = worker.read_state("123456789012", "us-east-1")

        assert state == existing_state

    def test_s3_error_raises(self):
        """Non-NoSuchKey S3 errors raise: an unreadable state must not be treated as empty."""
        mock_s3 = _make_s3_mock()
        mock_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "ServiceUnavailable", "Message": "Try later"}},
            "GetObject",
        )

        with patch.object(worker, "s3", mock_s3):
            with pytest.raises(ClientError):
                worker.read_state("123456789012", "us-east-1")

    def test_read_state_uses_correct_key_format(self):
        """read_state calls s3.get_object with the expected key pattern."""
        mock_s3 = _make_s3_mock()
        mock_s3.get_object.side_effect = NoSuchKey("Not Found")

        with patch.object(worker, "s3", mock_s3):
            worker.read_state("123456789012", "us-east-1")

        call_kwargs = mock_s3.get_object.call_args[1]
        assert "123456789012" in call_kwargs["Key"]
        assert "us-east-1" in call_kwargs["Key"]


class TestWriteState:

    def test_write_state_compresses_data(self):
        """State data written to S3 must be gzip-compressed."""
        mock_s3 = _make_s3_mock()
        mock_s3.put_object.return_value = {}
        state = {"arn:aws:ec2:us-east-1:123:instance/i-x": {"Env": "test"}}

        with patch.object(worker, "s3", mock_s3):
            worker.write_state("123456789012", "us-east-1", state)

        call_kwargs = mock_s3.put_object.call_args[1]
        body = call_kwargs["Body"]
        decompressed = json.loads(gzip.decompress(body))
        assert decompressed == state

    def test_write_state_sets_content_type(self):
        """put_object should set ContentType and ContentEncoding."""
        mock_s3 = _make_s3_mock()
        mock_s3.put_object.return_value = {}

        with patch.object(worker, "s3", mock_s3):
            worker.write_state("123456789012", "us-east-1", {"k": "v"})

        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["ContentType"] == "application/json"
        assert call_kwargs["ContentEncoding"] == "gzip"

    def test_write_state_uses_correct_key_format(self):
        """write_state calls s3.put_object with the expected key pattern."""
        mock_s3 = _make_s3_mock()
        mock_s3.put_object.return_value = {}

        with patch.object(worker, "s3", mock_s3):
            worker.write_state("999888777666", "eu-west-1", {})

        call_kwargs = mock_s3.put_object.call_args[1]
        assert "999888777666" in call_kwargs["Key"]
        assert "eu-west-1" in call_kwargs["Key"]


# ---------------------------------------------------------------------------
# G2: concurrent read-modify-write of the same state file
# ---------------------------------------------------------------------------

import io  # noqa: E402

ACCT, REGION = "123456789012", "us-east-1"


class FakeS3:
    """In-memory S3 that honours IfMatch / IfNoneMatch like real conditional writes."""

    def __init__(self):
        self.objects = {}
        self.version = 0
        self.exceptions = MagicMock()
        self.exceptions.NoSuchKey = NoSuchKey

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise NoSuchKey(Key)
        body, etag = self.objects[Key]
        return {"Body": io.BytesIO(body), "ETag": etag}

    def put_object(self, Bucket, Key, Body, IfMatch=None, IfNoneMatch=None, **_):
        current = self.objects.get(Key)
        if (IfNoneMatch == "*" and current is not None) or (
                IfMatch is not None and (current is None or current[1] != IfMatch)):
            raise ClientError({"Error": {"Code": "PreconditionFailed", "Message": "x"}}, "PutObject")
        self.version += 1
        etag = f'"{self.version}"'
        self.objects[Key] = (Body if isinstance(Body, bytes) else Body.encode(), etag)
        return {"ETag": etag}

    def state(self):
        return json.loads(gzip.decompress(self.objects[worker.state_key(ACCT, REGION)][0]))


def _tag_event(arn, tags):
    return {
        "account": ACCT, "detail-type": "AWS API Call via CloudTrail",
        "detail": {"eventName": "TagResource", "awsRegion": REGION,
                   "requestParameters": {"resourceArn": arn, "tags": tags}},
    }


ARN1 = f"arn:aws:sqs:{REGION}:{ACCT}:q1"
ARN2 = f"arn:aws:sqs:{REGION}:{ACCT}:q2"


class TestConcurrentStateWrites:

    def _seed(self, fake, state):
        fake.put_object(Bucket="b", Key=worker.state_key(ACCT, REGION),
                        Body=gzip.compress(json.dumps(state).encode()))

    def _run_with_interleave(self, fake, outer, inner):
        """Run outer(); while it publishes (after reading state), inner() runs to completion."""
        real_publish = worker.publish_deltas
        fired = []

        def publish(*args):
            if not fired:
                fired.append(1)
                inner()
            return real_publish(*args)

        with patch.object(worker, "s3", fake), patch.object(worker, "sqs", MagicMock()), \
                patch.object(worker, "publish_deltas", side_effect=publish):
            outer()

    def test_two_realtime_events_keep_both_changes(self):
        fake = FakeS3()
        self._seed(fake, {ARN1: {"Env": "prod"}, ARN2: {"Env": "prod"}})
        self._run_with_interleave(
            fake,
            outer=lambda: worker.process_real_time_event(_tag_event(ARN1, {"Team": "a"})),
            inner=lambda: worker.process_real_time_event(_tag_event(ARN2, {"Team": "b"})),
        )
        assert fake.state() == {ARN1: {"Env": "prod", "Team": "a"},
                                ARN2: {"Env": "prod", "Team": "b"}}

    def test_scan_does_not_overwrite_realtime_change(self):
        fake = FakeS3()
        self._seed(fake, {ARN1: {"Env": "prod"}})
        inventory = {ARN1: {"Env": "prod"}, ARN2: {}}
        worker._HUB_ACCOUNT_ID = ACCT
        with patch.object(worker, "discover_resources_for_account_region", return_value=inventory):
            self._run_with_interleave(
                fake,
                outer=lambda: worker.process_account_region_scan(ACCT, REGION, "scan-1"),
                inner=lambda: worker.process_real_time_event(_tag_event(ARN1, {"Team": "a"})),
            )
        assert fake.state() == {ARN1: {"Env": "prod", "Team": "a"}, ARN2: {}}

    def test_first_write_is_create_only(self):
        fake = FakeS3()
        with patch.object(worker, "s3", fake):
            base = worker.read_state(ACCT, REGION)
            self._seed(fake, {ARN2: {"Env": "dev"}})  # someone else created it meanwhile
            worker.save_state(ACCT, REGION, {ARN1: {"Env": "prod"}}, base)
        assert fake.state() == {ARN1: {"Env": "prod"}, ARN2: {"Env": "dev"}}

    def test_two_realtime_events_on_same_arn_keep_both_tags(self):
        """L1: two writers tagging the SAME ARN with different keys must not drop either key."""
        fake = FakeS3()
        self._seed(fake, {ARN1: {"Env": "prod"}})
        self._run_with_interleave(
            fake,
            outer=lambda: worker.process_real_time_event(_tag_event(ARN1, {"Team": "a"})),
            inner=lambda: worker.process_real_time_event(_tag_event(ARN1, {"Owner": "b"})),
        )
        assert fake.state() == {ARN1: {"Env": "prod", "Team": "a", "Owner": "b"}}

    def test_scan_change_on_arn_keeps_realtime_key(self):
        """L1: a scan that changed one key of an ARN keeps a key a real-time writer added."""
        fake = FakeS3()
        self._seed(fake, {ARN1: {"Env": "dev", "Old": "x"}})
        inventory = {ARN1: {"Env": "prod"}}  # scan saw Env changed and Old removed
        worker._HUB_ACCOUNT_ID = ACCT
        with patch.object(worker, "discover_resources_for_account_region", return_value=inventory):
            self._run_with_interleave(
                fake,
                outer=lambda: worker.process_account_region_scan(ACCT, REGION, "scan-1"),
                inner=lambda: worker.process_real_time_event(_tag_event(ARN1, {"Team": "a"})),
            )
        assert fake.state() == {ARN1: {"Env": "prod", "Team": "a"}}

    def test_scan_removal_wins_over_concurrent_change(self):
        """A resource the scan saw deleted stays deleted after a merge."""
        fake = FakeS3()
        self._seed(fake, {ARN1: {"Env": "prod"}, ARN2: {"Env": "prod"}})
        worker._HUB_ACCOUNT_ID = ACCT
        with patch.object(worker, "discover_resources_for_account_region",
                          return_value={ARN2: {"Env": "prod"}}):
            self._run_with_interleave(
                fake,
                outer=lambda: worker.process_account_region_scan(ACCT, REGION, "scan-1"),
                inner=lambda: worker.process_real_time_event(_tag_event(ARN1, {"Team": "a"})),
            )
        assert fake.state() == {ARN2: {"Env": "prod"}}

    def test_s3_replace_is_not_undone_by_concurrent_writer(self):
        """PutBucketTagging replaces the whole set: a racing writer cannot resurrect a dropped key."""
        bucket_arn = "arn:aws:s3:::amzn-s3-demo-bucket"
        fake = FakeS3()
        self._seed(fake, {bucket_arn: {"Old": "x"}, ARN1: {"Env": "prod"}})
        put = {
            "account": ACCT, "detail-type": "AWS API Call via CloudTrail",
            "detail": {"eventName": "PutBucketTagging", "awsRegion": REGION,
                       "requestParameters": {"bucketName": "amzn-s3-demo-bucket",
                                             "Tagging": {"TagSet": {"Tag": [{"Key": "Team", "Value": "a"}]}}}},
        }
        # The racing writer keeps Old and adds Owner on the bucket, and changes ARN1.
        racing = {"account": ACCT, "region": REGION, "detail-type": "Tag Change on Resource",
                  "resources": [bucket_arn], "detail": {"tags": {"Old": "x", "Owner": "b"}}}

        def inner():
            worker.process_tag_change_event(racing)
            worker.process_real_time_event(_tag_event(ARN1, {"Team": "b"}))

        self._run_with_interleave(fake, outer=lambda: worker.process_real_time_event(put), inner=inner)
        assert fake.state() == {bucket_arn: {"Team": "a"}, ARN1: {"Env": "prod", "Team": "b"}}

    def test_retries_exhausted_raises(self):
        """Every put conflicting must surface as StateConflict, not a silent drop."""
        fake = FakeS3()
        self._seed(fake, {ARN1: {"Env": "prod"}})
        real_put = fake.put_object

        def always_conflict(**kwargs):
            raise ClientError({"Error": {"Code": "PreconditionFailed", "Message": "x"}}, "PutObject")

        with patch.object(worker, "s3", fake):
            base = worker.read_state(ACCT, REGION)
            fake.put_object = always_conflict
            with pytest.raises(worker.StateConflict):
                worker.save_state(ACCT, REGION, {ARN1: {"Env": "dev"}}, base)
        fake.put_object = real_put
        assert fake.state() == {ARN1: {"Env": "prod"}}

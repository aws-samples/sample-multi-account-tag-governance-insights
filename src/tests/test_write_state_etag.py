"""
Unit tests for read_state() and write_state() in aggregator_worker.py.

These functions manage compressed S3 state files for per-account/region
tag snapshots used in delta computation.
"""

import gzip
import json
import os
import sys
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

    def test_s3_error_returns_empty_dict(self):
        """Non-NoSuchKey S3 errors return {} rather than raising."""
        mock_s3 = _make_s3_mock()
        mock_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "ServiceUnavailable", "Message": "Try later"}},
            "GetObject",
        )

        with patch.object(worker, "s3", mock_s3):
            state = worker.read_state("123456789012", "us-east-1")

        assert state == {}

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

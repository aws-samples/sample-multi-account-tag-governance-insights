"""Tests for analytics_writer_handler Lambda handler."""

import json
import os
import sys
import time
from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

ENV_VARS = {
    "CENTRAL_ANALYTICS_BUCKET": "amzn-s3-demo-analytics-bucket",
    "ATHENA_DATABASE": "test_db",
    "ENVIRONMENT": "test",
    "MAX_DELTA_FILE_SIZE_MB": "10",
    "LOG_LEVEL": "WARNING",
}


class NoSuchKey(Exception):
    pass


class EntityNotFoundException(Exception):
    pass


class AlreadyExistsException(Exception):
    pass


def _make_s3_mock():
    mock = MagicMock()
    mock.exceptions.NoSuchKey = NoSuchKey
    return mock


def _make_glue_mock():
    mock = MagicMock()
    mock.exceptions.EntityNotFoundException = EntityNotFoundException
    mock.exceptions.AlreadyExistsException = AlreadyExistsException
    return mock


@pytest.fixture(autouse=True)
def reset_module_cache():
    """Remove cached module so each test gets a fresh import."""
    mods_to_remove = [k for k in sys.modules if "analytics_writer" in k]
    for mod in mods_to_remove:
        del sys.modules[mod]
    yield


def _import_module(s3_mock=None, sqs_mock=None, glue_mock=None):
    s3 = s3_mock or _make_s3_mock()
    sqs = sqs_mock or MagicMock()
    cw = MagicMock()
    glue = glue_mock or _make_glue_mock()
    clients = {"s3": s3, "sqs": sqs, "cloudwatch": cw, "glue": glue}

    with patch.dict(os.environ, ENV_VARS):
        with patch(
            "boto3.client", side_effect=lambda svc, **kw: clients.get(svc, MagicMock())
        ):
            with patch.dict(
                sys.modules, {"pandas": None, "pyarrow": None, "pyarrow.parquet": None}
            ):
                import lambda_handlers.analytics_writer_handler as mod

                mod.s3_client = s3
                mod.sqs_client = sqs
                mod.cloudwatch = cw
                mod.glue_client = glue
                mod.HAS_PARQUET = False
                mod.CENTRAL_ANALYTICS_BUCKET = "amzn-s3-demo-analytics-bucket"
                return mod


def _sample_deltas(count=2):
    return [
        {
            "arn": f"arn:aws:ec2:us-east-1:111111111111:instance/i-{i:04d}",
            "resource_type": "ec2:instance",
            "tags": {"Environment": "prod", "Owner": "team-a"},
            "previous_tags": {"Environment": "dev"},
            "change_type": "tags_changed",
            "timestamp": int(time.time()),
            "source_system": "resource-explorer",
            "account_id": "111111111111",
            "region": "us-east-1",
            "scan_id": "2026-06-15",
        }
        for i in range(count)
    ]


def _make_sqs_event(messages):
    return {
        "Records": [
            {
                "messageId": f"msg-{i}",
                "body": json.dumps(msg),
                "messageAttributes": {},
                "receiptHandle": "receipt-123",
                "eventSourceARN": "arn:aws:sqs:us-east-1:111111111111:test-queue",
            }
            for i, msg in enumerate(messages)
        ]
    }


class TestValidateDeltaFile:
    def test_valid_delta_file(self):
        mod = _import_module()
        deltas = _sample_deltas(2)
        data = json.dumps(deltas).encode("utf-8")
        result = mod._validate_delta_file(data, "test-key")
        assert result is not None
        assert len(result) == 2

    def test_oversized_file_rejected(self):
        mod = _import_module()
        # Make a file larger than 10MB
        mod.MAX_DELTA_FILE_SIZE_BYTES = 100  # Lower for testing
        data = b"x" * 200
        result = mod._validate_delta_file(data, "test-key")
        assert result is None

    def test_invalid_json_rejected(self):
        mod = _import_module()
        data = b"not valid json {"
        result = mod._validate_delta_file(data, "test-key")
        assert result is None

    def test_non_array_rejected(self):
        mod = _import_module()
        data = json.dumps({"key": "value"}).encode("utf-8")
        result = mod._validate_delta_file(data, "test-key")
        assert result is None

    def test_schema_is_found_from_source_tree(self):
        mod = _import_module()
        assert mod._load_delta_schema()["$id"] == "taginsights-delta-schema"

    def test_missing_schema_fails_closed(self):
        mod = _import_module()
        mod._DELTA_SCHEMA = None
        mod._SCHEMA_CANDIDATES = ("/nonexistent/delta_schema.json",)
        with pytest.raises(FileNotFoundError):
            mod._validate_delta_file(json.dumps(_sample_deltas(1)).encode(), "k")

    def test_schema_rejects_unknown_change_type(self):
        mod = _import_module()
        deltas = _sample_deltas(1)
        deltas[0]["change_type"] = "TagResource"
        assert mod._validate_delta_file(json.dumps(deltas).encode(), "k") is None

    def test_schema_accepts_realtime_fields_and_128_tags(self):
        mod = _import_module()
        deltas = _sample_deltas(2)
        deltas[0].update(changed_by="arn:aws:sts::111111111111:assumed-role/r/s", event_name="CreateTags",
                         tags={f"k{i}": "v" for i in range(128)})
        deltas[1].update(changed_by=None, event_name=None)
        assert mod._validate_delta_file(json.dumps(deltas).encode(), "k") is not None

    def test_empty_array_valid(self):
        mod = _import_module()
        data = json.dumps([]).encode("utf-8")
        result = mod._validate_delta_file(data, "test-key")
        assert result == []


class TestQuarantineMaliciousFile:
    def test_moves_file_to_quarantine(self):
        s3 = _make_s3_mock()
        mod = _import_module(s3_mock=s3)
        mod._quarantine_malicious_file("bucket", "deltas/bad-file.json")
        s3.copy_object.assert_called_once()
        s3.delete_object.assert_called_once()
        copy_kwargs = s3.copy_object.call_args[1]
        assert "quarantine/" in copy_kwargs["Key"]

    def test_handles_quarantine_failure_gracefully(self):
        s3 = _make_s3_mock()
        s3.copy_object.side_effect = Exception("access denied")
        mod = _import_module(s3_mock=s3)
        # Should not raise
        mod._quarantine_malicious_file("bucket", "deltas/bad-file.json")


class TestGenerateContentHash:
    def test_deterministic_output(self):
        mod = _import_module()
        deltas = _sample_deltas(3)
        hash1 = mod.generate_content_hash(deltas)
        hash2 = mod.generate_content_hash(deltas)
        assert hash1 == hash2
        assert len(hash1) == 16  # Truncated SHA-256

    def test_different_data_different_hash(self):
        mod = _import_module()
        deltas1 = _sample_deltas(2)
        deltas2 = _sample_deltas(2)
        deltas2[0]["arn"] = "arn:aws:s3:::amzn-s3-demo-other-bucket"
        assert mod.generate_content_hash(deltas1) != mod.generate_content_hash(deltas2)

    def test_repeat_change_at_other_time_or_by_other_caller_not_duplicate(self):
        # Same-day A->B, B->A, A->B: the third change has the same content as the
        # first and must not be dropped as a duplicate; nor must the same change
        # made by a different caller.
        mod = _import_module()
        first = _sample_deltas(1)
        later = _sample_deltas(1)
        later[0]["timestamp"] = first[0]["timestamp"] + 3600
        other_caller = _sample_deltas(1)
        other_caller[0]["timestamp"] = first[0]["timestamp"]
        other_caller[0]["changed_by"] = "arn:aws:sts::111111111111:assumed-role/r/s"
        assert mod.generate_content_hash(first) != mod.generate_content_hash(later)
        assert mod.generate_content_hash(first) != mod.generate_content_hash(other_caller)

    def test_order_independent(self):
        mod = _import_module()
        deltas = _sample_deltas(3)
        reversed_deltas = list(reversed(deltas))
        assert mod.generate_content_hash(deltas) == mod.generate_content_hash(
            reversed_deltas
        )


class TestExtractQueueUrl:
    def test_valid_arn(self):
        mod = _import_module()
        record = {"eventSourceARN": "arn:aws:sqs:us-east-1:111111111111:my-queue"}
        url = mod.extract_queue_url_from_record(record)
        assert url == "https://sqs.us-east-1.amazonaws.com/111111111111/my-queue"

    def test_invalid_arn_raises(self):
        mod = _import_module()
        record = {"eventSourceARN": "bad-arn"}
        with pytest.raises(ValueError):
            mod.extract_queue_url_from_record(record)


class TestGetRetryCount:
    def test_returns_zero_when_no_attribute(self):
        mod = _import_module()
        record = {"messageAttributes": {}}
        assert mod.get_retry_count(record) == 0

    def test_extracts_retry_count(self):
        mod = _import_module()
        record = {
            "messageAttributes": {
                "RetryCount": {"stringValue": "2", "dataType": "Number"}
            }
        }
        assert mod.get_retry_count(record) == 2


class TestExtractAccountFromArn:
    def test_valid_arn(self):
        mod = _import_module()
        assert (
            mod.extract_account_from_arn(
                "arn:aws:ec2:us-east-1:123456789012:instance/i-1234"
            )
            == "123456789012"
        )

    def test_invalid_arn(self):
        mod = _import_module()
        assert mod.extract_account_from_arn("not-an-arn") == "unknown"


class TestReadDeltasWithRetry:
    def test_successful_read(self):
        s3 = _make_s3_mock()
        deltas = _sample_deltas(2)
        raw = json.dumps(deltas).encode("utf-8")
        s3.get_object.return_value = {"Body": MagicMock(read=lambda: raw)}
        mod = _import_module(s3_mock=s3)

        result, pending = mod.read_deltas_from_regional_s3_with_retry(
            "bucket", "deltas/key.json", "us-east-1"
        )
        assert pending is False
        assert len(result) == 2

    def test_not_found_returns_pending(self):
        s3 = _make_s3_mock()
        from botocore.exceptions import ClientError

        s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "not found"}}, "GetObject"
        )
        mod = _import_module(s3_mock=s3)

        result, pending = mod.read_deltas_from_regional_s3_with_retry(
            "bucket", "deltas/key.json", "us-east-1"
        )
        assert pending is True
        assert result == []

    def test_invalid_file_quarantined(self):
        s3 = _make_s3_mock()
        raw = b"not valid json"
        s3.get_object.return_value = {"Body": MagicMock(read=lambda: raw)}
        mod = _import_module(s3_mock=s3)

        result, pending = mod.read_deltas_from_regional_s3_with_retry(
            "bucket", "deltas/key.json", "us-east-1"
        )
        assert pending is False
        assert result == []
        # Quarantine called (copy_object)
        s3.copy_object.assert_called_once()


class TestWriteDeltasWithDedup:
    def test_new_write_succeeds(self):
        s3 = _make_s3_mock()
        from botocore.exceptions import ClientError

        # HEAD has no body: a missing key is reported as code "404"
        s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
        )
        glue = _make_glue_mock()
        glue.get_partition.side_effect = EntityNotFoundException("not found")
        mod = _import_module(s3_mock=s3, glue_mock=glue)

        deltas = _sample_deltas(2)
        message = {
            "account_id": "111111111111",
            "timestamp": int(time.time()),
            "region": "us-east-1",
        }

        result = mod.write_deltas_to_central_s3_with_dedup(deltas, message)
        assert result["duplicate"] is False
        s3.put_object.assert_called()

    def test_duplicate_detected(self):
        s3 = _make_s3_mock()
        # HEAD returns success (file exists)
        s3.head_object.return_value = {"ContentLength": 1024}
        mod = _import_module(s3_mock=s3)

        deltas = _sample_deltas(2)
        message = {
            "account_id": "111111111111",
            "timestamp": int(time.time()),
            "region": "us-east-1",
        }

        result = mod.write_deltas_to_central_s3_with_dedup(deltas, message)
        assert result["duplicate"] is True
        # Should NOT call put_object for the parquet/json write
        s3.put_object.assert_not_called()


class TestHandleReplicationDelay:
    def test_requeues_message(self):
        sqs = MagicMock()
        sqs.send_message.return_value = {"MessageId": "new-msg-id"}
        mod = _import_module(sqs_mock=sqs)

        message = {
            "s3_bucket": "b",
            "s3_key": "k",
            "region": "us-east-1",
            "timestamp": 123,
        }
        record = {
            "receiptHandle": "receipt-abc",
            "eventSourceARN": "arn:aws:sqs:us-east-1:111111111111:test-queue",
        }
        mod.handle_replication_delay(message, record, retry_count=0)

        sqs.send_message.assert_called_once()
        call_kwargs = sqs.send_message.call_args[1]
        assert call_kwargs["DelaySeconds"] == 300
        assert call_kwargs["MessageAttributes"]["RetryCount"]["StringValue"] == "1"
        sqs.delete_message.assert_called_once()


class TestLambdaHandler:
    def test_empty_event_returns_success(self):
        mod = _import_module()
        result = mod.lambda_handler({"Records": []}, MagicMock())
        assert result["statusCode"] == 200
        assert result["processed_messages"] == 0

    def test_processes_valid_message(self):
        s3 = _make_s3_mock()
        sqs = MagicMock()
        glue = _make_glue_mock()
        raw = json.dumps(_sample_deltas(2)).encode("utf-8")
        s3.get_object.return_value = {"Body": MagicMock(read=lambda: raw)}
        from botocore.exceptions import ClientError

        s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "x"}}, "HeadObject"
        )
        glue.get_partition.side_effect = EntityNotFoundException("no")
        mod = _import_module(s3_mock=s3, sqs_mock=sqs, glue_mock=glue)
        msg = {
            "s3_bucket": "b",
            "s3_key": "deltas/test.json",
            "region": "us-east-1",
            "account_id": "111111111111",
            "timestamp": int(time.time()),
            "changed_count": 2,
            "correlation_id": "corr-123",
        }
        result = mod.lambda_handler(_make_sqs_event([msg]), MagicMock())
        assert result["statusCode"] == 200
        assert result["processed_messages"] == 1
        assert result["total_deltas_written"] == 2

    def test_replication_pending_retries(self):
        s3 = _make_s3_mock()
        sqs = MagicMock()
        sqs.send_message.return_value = {"MessageId": "retry-id"}
        from botocore.exceptions import ClientError

        s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "x"}}, "GetObject"
        )
        mod = _import_module(s3_mock=s3, sqs_mock=sqs)
        msg = {
            "s3_bucket": "b",
            "s3_key": "deltas/test.json",
            "region": "us-east-1",
            "account_id": "111111111111",
            "timestamp": int(time.time()),
            "changed_count": 2,
        }
        result = mod.lambda_handler(_make_sqs_event([msg]), MagicMock())
        assert result["retried_messages"] == 1
        sqs.send_message.assert_called_once()

    def test_max_retries_exceeded_reports_item_failure(self):
        s3 = _make_s3_mock()
        from botocore.exceptions import ClientError

        s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "x"}}, "GetObject"
        )
        mod = _import_module(s3_mock=s3)
        msg = {
            "s3_bucket": "b",
            "s3_key": "deltas/test.json",
            "region": "us-east-1",
            "account_id": "111111111111",
            "timestamp": int(time.time()),
            "changed_count": 2,
        }
        event = {
            "Records": [
                {
                    "messageId": "stuck",
                    "body": json.dumps(msg),
                    "messageAttributes": {
                        "RetryCount": {"stringValue": "3", "dataType": "Number"}
                    },
                    "receiptHandle": "r-123",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:111111111111:test-queue",
                }
            ]
        }
        result = mod.lambda_handler(event, MagicMock())
        assert result["batchItemFailures"] == [{"itemIdentifier": "stuck"}]

    def test_one_bad_record_does_not_fail_the_batch(self):
        s3 = _make_s3_mock()
        glue = _make_glue_mock()
        raw = json.dumps(_sample_deltas(1)).encode("utf-8")
        s3.get_object.return_value = {"Body": MagicMock(read=lambda: raw)}
        from botocore.exceptions import ClientError

        s3.head_object.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadObject")
        glue.get_partition.side_effect = EntityNotFoundException("no")
        mod = _import_module(s3_mock=s3, glue_mock=glue)
        good = {"s3_bucket": "b", "s3_key": "deltas/ok.json", "region": "us-east-1",
                "account_id": "111111111111", "timestamp": int(time.time())}
        bad = {"region": "us-east-1"}  # no s3_key
        result = mod.lambda_handler(_make_sqs_event([bad, good]), MagicMock())
        assert result["batchItemFailures"] == [{"itemIdentifier": "msg-0"}]
        assert result["processed_messages"] == 1


def test_parquet_row_matches_glue_recent_table_columns():
    """Writer columns must equal the Glue table columns, in order."""
    import yaml

    class _Loader(yaml.SafeLoader):
        pass

    _Loader.add_multi_constructor("!", lambda loader, suffix, node: None)
    template_path = os.path.join(os.path.dirname(__file__), "..", "..", "deployment", "core-stack.yaml")
    with open(template_path) as f:
        template = _Loader(f).get_single_data()
    mod = _import_module()
    row = mod.prepare_delta_row(_sample_deltas(1)[0])
    for table in ("TagDeltasRecentTable", "TagDeltasHistoricalTable"):
        columns = [c["Name"] for c in template["Resources"][table]["Properties"]["TableInput"]["StorageDescriptor"]["Columns"]]
        assert columns[-2:] == ["changed_by", "event_name"]
    recent = template["Resources"]["TagDeltasRecentTable"]["Properties"]["TableInput"]["StorageDescriptor"]["Columns"]
    assert list(row) == [c["Name"] for c in recent]
    assert row["changed_by"] is None and row["event_name"] is None


class TestRegisterPartition:
    def test_creates_new_partition(self):
        glue = _make_glue_mock()
        glue.get_partition.side_effect = EntityNotFoundException("not found")
        mod = _import_module(glue_mock=glue)

        mod.register_partition("111111111111", 2026, 6, 15)
        glue.create_partition.assert_called_once()

    def test_skips_existing_partition(self):
        glue = _make_glue_mock()
        glue.get_partition.return_value = {}  # Partition exists
        mod = _import_module(glue_mock=glue)

        mod.register_partition("111111111111", 2026, 6, 15)
        glue.create_partition.assert_not_called()


class TestH1PartitionRegistrationNotLost:
    """H1: recent-table partitions are Glue-registered (no projection, no MSCK
    job). A swallowed registration error, or a retry that hits the dedupe
    path, left written deltas invisible to Athena and to monthly compaction."""

    def test_registration_error_raises_so_sqs_retries(self):
        glue = _make_glue_mock()
        glue.get_partition.side_effect = EntityNotFoundException("no")
        glue.create_partition.side_effect = RuntimeError("ThrottlingException")
        mod = _import_module(glue_mock=glue)
        with pytest.raises(RuntimeError):
            mod.register_partition("111111111111", 2026, 6, 15)

    def test_already_exists_race_is_not_an_error(self):
        glue = _make_glue_mock()
        glue.get_partition.side_effect = EntityNotFoundException("no")
        glue.create_partition.side_effect = AlreadyExistsException("race")
        mod = _import_module(glue_mock=glue)
        mod.register_partition("111111111111", 2026, 6, 15)

    def test_duplicate_write_still_registers_partition(self):
        s3 = _make_s3_mock()
        s3.head_object.return_value = {"ContentLength": 1024}  # file already written
        glue = _make_glue_mock()
        glue.get_partition.side_effect = EntityNotFoundException("no")
        mod = _import_module(s3_mock=s3, glue_mock=glue)
        message = {"account_id": "111111111111", "timestamp": int(time.time()), "region": "us-east-1"}
        result = mod.write_deltas_to_central_s3_with_dedup(_sample_deltas(1), message)
        assert result["duplicate"] is True
        s3.put_object.assert_not_called()
        glue.create_partition.assert_called_once()


class TestH2OnlyDeltaKeysAreRead:
    """H2: the SQS message's s3_key was trusted. A key outside deltas/ (state
    file, historical Parquet) fails JSON validation and is 'quarantined', i.e.
    copied and DELETED from the central bucket."""

    def test_non_delta_key_is_rejected_and_never_deleted(self):
        s3 = _make_s3_mock()
        s3.get_object.return_value = {"Body": MagicMock(read=lambda: b"\x1f\x8b not json")}
        mod = _import_module(s3_mock=s3)
        msg = {"s3_bucket": "b", "region": "us-east-1", "account_id": "111111111111",
               "timestamp": int(time.time()),
               "s3_key": "aggregator-state/account_id=111111111111/region=us-east-1/latest.json.gz"}
        result = mod.lambda_handler(_make_sqs_event([msg]), MagicMock())
        assert result["batchItemFailures"] == [{"itemIdentifier": "msg-0"}]
        s3.get_object.assert_not_called()
        s3.copy_object.assert_not_called()
        s3.delete_object.assert_not_called()


class TestH3SizeCheckedBeforeRead:
    """H3: the size limit ran only after Body.read() had loaded the whole
    object, so an oversized file could exhaust Lambda memory on every retry."""

    def test_oversized_object_rejected_without_reading_body(self):
        s3 = _make_s3_mock()
        body = MagicMock()
        body.read.side_effect = AssertionError("body must not be read")
        s3.get_object.return_value = {"Body": body, "ContentLength": 3 * 1024 ** 3}
        mod = _import_module(s3_mock=s3)
        result, pending = mod.read_deltas_from_regional_s3_with_retry("b", "deltas/huge.json", "us-east-1")
        assert (result, pending) == ([], False)
        body.read.assert_not_called()
        s3.copy_object.assert_called_once()  # quarantined like any rejected file


class TestH5AccountIdValidated:
    """H5: the partition account_id came unchecked from the message (or the
    ARN fallback, which is '' for S3 bucket ARNs). The monthly aggregator skips
    partitions whose account_id is not 12 digits, so such deltas were written
    where compaction never reads or cleans them."""

    @pytest.mark.parametrize("message_account, arn", [
        (None, "arn:aws:s3:::amzn-s3-demo-bucket"),   # ARN fallback -> ''
        ("unknown", "arn:aws:ec2:us-east-1:111111111111:instance/i-1"),
        ("11111111111/x", "arn:aws:ec2:us-east-1:111111111111:instance/i-1"),
    ])
    def test_invalid_account_fails_before_write(self, message_account, arn):
        from botocore.exceptions import ClientError

        s3 = _make_s3_mock()
        s3.head_object.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadObject")
        glue = _make_glue_mock()
        glue.get_partition.side_effect = EntityNotFoundException("no")
        mod = _import_module(s3_mock=s3, glue_mock=glue)
        deltas = _sample_deltas(1)
        deltas[0]["arn"] = arn
        message = {"account_id": message_account, "timestamp": int(time.time()), "region": "us-east-1"}
        with pytest.raises(ValueError):
            mod.write_deltas_to_central_s3_with_dedup(deltas, message)
        s3.put_object.assert_not_called()
        glue.create_partition.assert_not_called()

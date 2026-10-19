"""Tests for S3CostWriter - Partitioned Parquet storage for cost analysis."""

import json
import os
import sys
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("ENABLE_TAG_COSTS_TABLE", "true")


@pytest.fixture
def mock_boto3():
    with patch("boto3.client") as mock_client_fn:
        mock_s3 = MagicMock()
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}

        def client_factory(service, **kwargs):
            if service == "s3":
                return mock_s3
            elif service == "sts":
                return mock_sts
            return MagicMock()

        mock_client_fn.side_effect = client_factory
        mock_s3.head_bucket.return_value = {}
        mock_s3.head_object.return_value = {"ContentLength": 1024}
        yield {"client_fn": mock_client_fn, "s3": mock_s3, "sts": mock_sts}


def _make_writer(mock_boto3):
    from lambda_handlers.cost.s3_cost_writer import S3CostWriter

    return S3CostWriter(region_name="us-east-1")


def _sample_resources(count=3):
    return [
        {
            "resource_arn": f"arn:aws:ec2:us-east-1:123456789012:instance/i-{i:016d}",
            "service_type": "ec2",
            "region": "us-east-1",
            "tags": {"Environment": "dev", "Project": "test"},
            "last_month_cost": 10.0 + i,
            "current_month_mtd": 5.0 + i,
            "current_month_projected": 15.0 + i,
            "avg_daily_cost": 0.5 + i * 0.1,
            "discovered_date": "2025-10-01T00:00:00+00:00",
        }
        for i in range(count)
    ]


class TestS3CostWriterInit:
    def test_init_bucket_name(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        assert "123456789012" in writer.bucket_name
        assert "cost-data" in writer.bucket_name

    def test_init_checks_bucket_exists(self, mock_boto3):
        _make_writer(mock_boto3)
        mock_boto3["s3"].head_bucket.assert_called_once()

    def test_init_bucket_not_found(self, mock_boto3):
        exc = mock_boto3["s3"].exceptions.NoSuchBucket = type(
            "NoSuchBucket", (Exception,), {}
        )
        mock_boto3["s3"].head_bucket.side_effect = exc("Not found")
        writer = _make_writer(mock_boto3)
        assert writer.bucket_name is not None

    def test_tag_costs_enabled(self, mock_boto3):
        assert _make_writer(mock_boto3).enable_tag_costs_table is True

    def test_tag_costs_disabled(self, mock_boto3, monkeypatch):
        monkeypatch.setenv("ENABLE_TAG_COSTS_TABLE", "false")
        assert _make_writer(mock_boto3).enable_tag_costs_table is False


class TestGroupByService:
    def test_groups_resources_correctly(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        resources = [
            {"service_type": "ec2", "resource_arn": "a1"},
            {"service_type": "s3", "resource_arn": "a2"},
            {"service_type": "ec2", "resource_arn": "a3"},
            {"service_type": "lambda", "resource_arn": "a4"},
        ]
        groups = writer._group_by_service(resources)
        assert len(groups) == 3
        assert len(groups["ec2"]) == 2

    def test_defaults_to_unknown(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        groups = writer._group_by_service([{"resource_arn": "a1"}])
        assert "unknown" in groups


class TestHasCostAllocationTags:
    def test_returns_true_for_cost_tags(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        assert (
            writer._has_cost_allocation_tags({"Environment": "prod", "Name": "x"})
            is True
        )

    def test_returns_false_for_no_cost_tags(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        assert (
            writer._has_cost_allocation_tags({"Name": "x", "CreatedBy": "tf"}) is False
        )

    def test_returns_false_for_empty_tags(self, mock_boto3):
        assert _make_writer(mock_boto3)._has_cost_allocation_tags({}) is False


class TestCalculateLastUsed:
    def test_uses_cur_activity(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        r = writer._calculate_last_used(
            {"cur_last_activity": "2025-10-15T12:00:00+00:00"},
            datetime(2025, 10, 20, tzinfo=timezone.utc),
        )
        assert "2025-10-15" in r

    def test_uses_most_recent_timestamp(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        r = writer._calculate_last_used(
            {
                "cur_last_activity": "2025-10-15T12:00:00+00:00",
                "discovered_date": "2025-10-18T00:00:00+00:00",
            },
            datetime(2025, 10, 20, tzinfo=timezone.utc),
        )
        assert "2025-10-18" in r

    def test_falls_back_to_analysis_time(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        r = writer._calculate_last_used({}, datetime(2025, 10, 20, tzinfo=timezone.utc))
        assert "2025-10-20" in r

    def test_handles_invalid_timestamp(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        r = writer._calculate_last_used(
            {"cur_last_activity": "not-a-date"},
            datetime(2025, 10, 20, tzinfo=timezone.utc),
        )
        assert "2025-10-20" in r


class TestPrepareParquetData:
    def test_produces_correct_fields(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        rows = writer._prepare_parquet_data(
            _sample_resources(1), datetime(2025, 10, 2, tzinfo=timezone.utc)
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["has_cost_tags"] is True
        assert row["is_dormant"] is False
        assert row["analysis_date"] == "2025-10-02"
        assert row["last_month_cost"] == 10.0

    def test_dormant_resource(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        resources = [
            {
                "resource_arn": "a",
                "service_type": "ec2",
                "tags": {},
                "current_month_projected": 0,
                "monthly_cost": 0,
            }
        ]
        rows = writer._prepare_parquet_data(
            resources, datetime(2025, 10, 2, tzinfo=timezone.utc)
        )
        assert rows[0]["is_dormant"] is True

    def test_tags_serialized_as_json(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        resources = [{"resource_arn": "a", "tags": {"k": "v"}, "service_type": "ec2"}]
        rows = writer._prepare_parquet_data(
            resources, datetime(2025, 10, 2, tzinfo=timezone.utc)
        )
        assert json.loads(rows[0]["tags"]) == {"k": "v"}


class TestWriteServicePartition:
    @patch("pandas.DataFrame.to_parquet")
    def test_writes_parquet_and_uploads(self, mock_parquet, mock_boto3):
        writer = _make_writer(mock_boto3)
        result = writer._write_service_partition(
            _sample_resources(2),
            "ec2",
            "20251002",
            datetime(2025, 10, 2, tzinfo=timezone.utc),
        )
        assert result["status"] == "success"
        assert result["resource_count"] == 2
        assert "ec2" in result["s3_key"]
        assert result["partition"] == "year=2025/month=10/service=ec2"
        mock_boto3["s3"].upload_file.assert_called_once()

    @patch("pandas.DataFrame.to_parquet", side_effect=Exception("Write error"))
    def test_handles_write_error(self, mock_parquet, mock_boto3):
        writer = _make_writer(mock_boto3)
        result = writer._write_service_partition(
            _sample_resources(1),
            "ec2",
            "20251002",
            datetime(2025, 10, 2, tzinfo=timezone.utc),
        )
        assert result["status"] == "error"


class TestWriteCostData:
    @patch("pandas.DataFrame.to_parquet")
    def test_empty_data_skipped(self, mock_parquet, mock_boto3):
        writer = _make_writer(mock_boto3)
        assert writer.write_cost_data([])["status"] == "skipped"

    @patch("pandas.DataFrame.to_parquet")
    def test_writes_multiple_partitions(self, mock_parquet, mock_boto3):
        writer = _make_writer(mock_boto3)
        resources = [
            {
                "resource_arn": "a1",
                "service_type": "ec2",
                "tags": {"Environment": "dev"},
                "current_month_projected": 10,
                "region": "us-east-1",
            },
            {
                "resource_arn": "a2",
                "service_type": "s3",
                "tags": {"Project": "x"},
                "current_month_projected": 5,
                "region": "us-east-1",
            },
        ]
        mock_boto3["s3"].get_object.return_value = {
            "Body": MagicMock(
                read=MagicMock(
                    return_value=json.dumps(
                        {"partitions": [], "last_updated": None}
                    ).encode()
                )
            )
        }
        result = writer.write_cost_data(resources, "20251002")
        assert result["status"] == "success"
        assert result["resources_written"] == 2

    @patch("pandas.DataFrame.to_parquet", side_effect=Exception("S3 failure"))
    def test_partition_failures_still_succeed_overall(self, mock_parquet, mock_boto3):
        exc_cls = type("NoSuchKey", (Exception,), {})
        mock_boto3["s3"].exceptions = MagicMock()
        mock_boto3["s3"].exceptions.NoSuchKey = exc_cls
        mock_boto3["s3"].get_object.side_effect = exc_cls("No metadata")
        writer = _make_writer(mock_boto3)
        result = writer.write_cost_data(_sample_resources(1), "20251002")
        assert result["status"] == "success"
        assert result["resources_written"] == 0

    def test_top_level_exception(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        with patch.object(writer, "_group_by_service", side_effect=Exception("Fatal")):
            result = writer.write_cost_data(_sample_resources(1))
        assert result["status"] == "error"

    @patch("pandas.DataFrame.to_parquet")
    def test_tag_costs_disabled(self, mock_parquet, mock_boto3, monkeypatch):
        monkeypatch.setenv("ENABLE_TAG_COSTS_TABLE", "false")
        writer = _make_writer(mock_boto3)
        mock_boto3["s3"].get_object.return_value = {
            "Body": MagicMock(
                read=MagicMock(
                    return_value=json.dumps(
                        {"partitions": [], "last_updated": None}
                    ).encode()
                )
            )
        }
        result = writer.write_cost_data(_sample_resources(1), "20251002")
        assert result["status"] == "success"
        assert result.get("tag_costs_result") == {}


class TestUpdatePartitionMetadata:
    def test_creates_new_metadata(self, mock_boto3):
        exc_cls = type("NoSuchKey", (Exception,), {})
        mock_boto3["s3"].exceptions = MagicMock()
        mock_boto3["s3"].exceptions.NoSuchKey = exc_cls
        mock_boto3["s3"].get_object.side_effect = exc_cls("Not found")
        writer = _make_writer(mock_boto3)
        writer._update_partition_metadata("20251002", ["ec2", "s3"])
        put_body = mock_boto3["s3"].put_object.call_args[1]["Body"]
        metadata = json.loads(put_body)
        assert len(metadata["partitions"]) == 2

    def test_appends_to_existing(self, mock_boto3):
        existing = {
            "partitions": [{"year": 2025, "month": 9, "service": "ec2"}],
            "last_updated": None,
        }
        mock_boto3["s3"].get_object.return_value = {
            "Body": MagicMock(
                read=MagicMock(return_value=json.dumps(existing).encode())
            )
        }
        exc_cls = type("NoSuchKey", (Exception,), {})
        mock_boto3["s3"].exceptions = MagicMock()
        mock_boto3["s3"].exceptions.NoSuchKey = exc_cls
        writer = _make_writer(mock_boto3)
        writer._update_partition_metadata("20251002", ["lambda"])
        metadata = json.loads(mock_boto3["s3"].put_object.call_args[1]["Body"])
        assert len(metadata["partitions"]) == 2

    def test_does_not_duplicate(self, mock_boto3):
        existing = {
            "partitions": [{"year": 2025, "month": 10, "service": "ec2"}],
            "last_updated": None,
        }
        mock_boto3["s3"].get_object.return_value = {
            "Body": MagicMock(
                read=MagicMock(return_value=json.dumps(existing).encode())
            )
        }
        exc_cls = type("NoSuchKey", (Exception,), {})
        mock_boto3["s3"].exceptions = MagicMock()
        mock_boto3["s3"].exceptions.NoSuchKey = exc_cls
        writer = _make_writer(mock_boto3)
        writer._update_partition_metadata("20251002", ["ec2"])
        metadata = json.loads(mock_boto3["s3"].put_object.call_args[1]["Body"])
        assert len(metadata["partitions"]) == 1


class TestNormalizeResourceTagsToRecords:
    def test_normalizes_tags(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        resources = [
            {
                "resource_arn": "arn:aws:ec2:us-east-1:111222333444:instance/i-abc",
                "service_type": "ec2",
                "region": "us-east-1",
                "tags": {"Env": "prod", "Team": "plat"},
                "last_month_cost": 100.0,
                "current_month_mtd": 50.0,
                "current_month_projected": 150.0,
                "avg_daily_cost": 5.0,
            }
        ]
        records = writer._normalize_resource_tags_to_records(
            resources, datetime(2025, 10, 2, tzinfo=timezone.utc)
        )
        assert len(records) == 2
        assert {r["tag_key"] for r in records} == {"Env", "Team"}
        assert records[0]["account_id"] == "111222333444"

    def test_skips_resources_without_tags(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        resources = [
            {"resource_arn": "a1", "tags": {}, "service_type": "ec2"},
            {"resource_arn": "a2", "service_type": "ec2"},
        ]
        records = writer._normalize_resource_tags_to_records(
            resources, datetime(2025, 10, 2, tzinfo=timezone.utc)
        )
        assert len(records) == 0

    def test_handles_json_string_tags(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        resources = [
            {
                "resource_arn": "arn:aws:s3:::b",
                "tags": json.dumps({"Name": "mybucket"}),
                "service_type": "s3",
            }
        ]
        records = writer._normalize_resource_tags_to_records(
            resources, datetime(2025, 10, 2, tzinfo=timezone.utc)
        )
        assert len(records) == 1
        assert records[0]["tag_key"] == "Name"

    def test_handles_double_encoded_json(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        inner = json.dumps({"App": "web"})
        resources = [
            {
                "resource_arn": "arn:aws:ec2:us-east-1:123:instance/i-1",
                "tags": json.dumps(inner),
                "service_type": "ec2",
            }
        ]
        records = writer._normalize_resource_tags_to_records(
            resources, datetime(2025, 10, 2, tzinfo=timezone.utc)
        )
        assert len(records) == 1
        assert records[0]["tag_key"] == "App"

    def test_handles_invalid_json_tags(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        resources = [
            {
                "resource_arn": "arn:aws:ec2:us-east-1:123:instance/i-1",
                "tags": "not-json{{{",
                "service_type": "ec2",
            }
        ]
        records = writer._normalize_resource_tags_to_records(
            resources, datetime(2025, 10, 2, tzinfo=timezone.utc)
        )
        assert len(records) == 0

    def test_none_tag_value_becomes_empty_string(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        resources = [
            {
                "resource_arn": "arn:aws:ec2:us-east-1:123:instance/i-1",
                "tags": {"Key1": None, "Key2": "val"},
                "service_type": "ec2",
            }
        ]
        records = writer._normalize_resource_tags_to_records(
            resources, datetime(2025, 10, 2, tzinfo=timezone.utc)
        )
        none_rec = next(r for r in records if r["tag_key"] == "Key1")
        assert none_rec["tag_value"] == ""

    def test_account_id_from_arn(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        resources = [
            {
                "resource_arn": "arn:aws:rds:us-west-2:999888777666:db:mydb",
                "tags": {"Env": "stage"},
                "service_type": "rds",
            }
        ]
        records = writer._normalize_resource_tags_to_records(
            resources, datetime(2025, 10, 2, tzinfo=timezone.utc)
        )
        assert records[0]["account_id"] == "999888777666"

    def test_invalid_arn_uses_unknown(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        resources = [
            {"resource_arn": "not-an-arn", "tags": {"K": "v"}, "service_type": "ec2"}
        ]
        records = writer._normalize_resource_tags_to_records(
            resources, datetime(2025, 10, 2, tzinfo=timezone.utc)
        )
        assert records[0]["account_id"] == "unknown"


class TestWriteNormalizedTagCosts:
    @patch("pandas.DataFrame.to_parquet")
    def test_writes_normalized_records(self, mock_parquet, mock_boto3):
        writer = _make_writer(mock_boto3)
        result = writer.write_normalized_tag_costs(
            _sample_resources(2), "20251002", datetime(2025, 10, 2, tzinfo=timezone.utc)
        )
        assert result["status"] == "success"
        assert result["records_written"] == 4  # 2 resources * 2 tags each
        assert "tag-costs/" in result["s3_key"]

    def test_empty_resources_skipped(self, mock_boto3):
        writer = _make_writer(mock_boto3)
        result = writer.write_normalized_tag_costs(
            [], "20251002", datetime(2025, 10, 2, tzinfo=timezone.utc)
        )
        assert result["status"] == "skipped"

    @patch("pandas.DataFrame.to_parquet")
    def test_no_tags_skipped(self, mock_parquet, mock_boto3):
        writer = _make_writer(mock_boto3)
        resources = [{"resource_arn": "a1", "tags": {}, "service_type": "ec2"}]
        result = writer.write_normalized_tag_costs(
            resources, "20251002", datetime(2025, 10, 2, tzinfo=timezone.utc)
        )
        assert result["status"] == "skipped"

    @patch("pandas.DataFrame.to_parquet", side_effect=Exception("Upload failed"))
    def test_handles_exception(self, mock_parquet, mock_boto3):
        writer = _make_writer(mock_boto3)
        result = writer.write_normalized_tag_costs(
            _sample_resources(1), "20251002", datetime(2025, 10, 2, tzinfo=timezone.utc)
        )
        assert result["status"] == "error"


class TestGetPartitionInfo:
    def test_returns_partitions(self, mock_boto3):
        metadata = {
            "partitions": [
                {"year": 2025, "month": 10, "service": "ec2"},
                {"year": 2025, "month": 10, "service": "s3"},
                {"year": 2025, "month": 9, "service": "ec2"},
            ],
            "last_updated": "2025-10-02T00:00:00",
        }
        mock_boto3["s3"].get_object.return_value = {
            "Body": MagicMock(
                read=MagicMock(return_value=json.dumps(metadata).encode())
            )
        }
        exc_cls = type("NoSuchKey", (Exception,), {})
        mock_boto3["s3"].exceptions = MagicMock()
        mock_boto3["s3"].exceptions.NoSuchKey = exc_cls
        writer = _make_writer(mock_boto3)
        assert writer.get_partition_info()["total_partitions"] == 3

    def test_filters_by_year(self, mock_boto3):
        metadata = {
            "partitions": [
                {"year": 2025, "month": 10, "service": "ec2"},
                {"year": 2024, "month": 12, "service": "ec2"},
            ],
            "last_updated": None,
        }
        mock_boto3["s3"].get_object.return_value = {
            "Body": MagicMock(
                read=MagicMock(return_value=json.dumps(metadata).encode())
            )
        }
        exc_cls = type("NoSuchKey", (Exception,), {})
        mock_boto3["s3"].exceptions = MagicMock()
        mock_boto3["s3"].exceptions.NoSuchKey = exc_cls
        writer = _make_writer(mock_boto3)
        assert writer.get_partition_info(year=2025)["total_partitions"] == 1

    def test_no_metadata_file(self, mock_boto3):
        exc_cls = type("NoSuchKey", (Exception,), {})
        mock_boto3["s3"].exceptions = MagicMock()
        mock_boto3["s3"].exceptions.NoSuchKey = exc_cls
        mock_boto3["s3"].get_object.side_effect = exc_cls("Not found")
        writer = _make_writer(mock_boto3)
        assert writer.get_partition_info()["total_partitions"] == 0

    def test_handles_exception(self, mock_boto3):
        mock_boto3["s3"].get_object.side_effect = Exception("Network error")
        writer = _make_writer(mock_boto3)
        result = writer.get_partition_info()
        assert result["total_partitions"] == 0
        assert "error" in result


class TestGetS3ObjectSize:
    def test_returns_content_length(self, mock_boto3):
        mock_boto3["s3"].head_object.return_value = {"ContentLength": 4096}
        writer = _make_writer(mock_boto3)
        assert writer._get_s3_object_size("k.parquet") == 4096

    def test_returns_zero_on_error(self, mock_boto3):
        mock_boto3["s3"].head_object.side_effect = Exception("Not found")
        writer = _make_writer(mock_boto3)
        assert writer._get_s3_object_size("missing.parquet") == 0

import calendar
"""Tests for Cost Analyzer Worker Lambda Handler."""

import json
import os
import sys
from datetime import date
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

mock_cur_matcher_cls = MagicMock()
mock_parquet_writer_cls = MagicMock()
mock_get_alloc_tags = MagicMock(return_value=["CostCenter", "Environment"])
mock_rate_limiter_fn = MagicMock()

_patches = [
    patch.dict(
        os.environ,
        {
            "AWS_REGION": "us-east-1",
            "S3_BUCKET": "test-bucket",
            "MONTHS_BACK": "2",
        },
    ),
    patch("boto3.client", return_value=MagicMock()),
]

for p in _patches:
    p.start()

# Patch the module-level imports before importing the handler
with patch.dict(
    "sys.modules",
    {
        "loguru": MagicMock(),
        "src.lambda_handlers.cost.cur_direct_matcher": MagicMock(
            CURDirectMatcher=mock_cur_matcher_cls
        ),
        "src.lambda_handlers.cost.parquet_writer": MagicMock(
            ParquetWriter=mock_parquet_writer_cls
        ),
        "src.lambda_handlers.cost.allocation_manager": MagicMock(
            get_active_cost_allocation_tags=mock_get_alloc_tags
        ),
        "src.lambda_handlers.cost.rate_limiter": MagicMock(
            get_athena_rate_limiter=mock_rate_limiter_fn
        ),
    },
):
    import lambda_handlers.cost.cost_analyzer_worker_handler as worker_module

CostAnalyzerWorker = worker_module.CostAnalyzerWorker
lambda_handler = worker_module.lambda_handler

for p in _patches:
    p.stop()


@pytest.fixture
def env_vars(monkeypatch):
    """Set required environment variables."""
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    monkeypatch.setenv("MONTHS_BACK", "2")


@pytest.fixture
def mock_deps():
    """Patch dependencies at module level for worker instantiation."""
    mock_cur = MagicMock()
    mock_writer = MagicMock()
    mock_cw = MagicMock()
    mock_rl = MagicMock()
    mock_rl.acquire_query_slot.return_value = True

    with patch.object(
        worker_module, "CURDirectMatcher", MagicMock(return_value=mock_cur)
    ), patch.object(
        worker_module, "ParquetWriter", MagicMock(return_value=mock_writer)
    ), patch.object(
        worker_module,
        "get_active_cost_allocation_tags",
        MagicMock(return_value=["CostCenter"]),
    ), patch.object(
        worker_module, "get_athena_rate_limiter", MagicMock(return_value=mock_rl)
    ), patch(
        "boto3.client", return_value=mock_cw
    ):
        worker = CostAnalyzerWorker()
        yield worker, mock_cur, mock_writer, mock_cw, mock_rl


@pytest.fixture
def sample_resources():
    """Sample resource batch for processing."""
    return [
        {
            "resource_arn": "arn:aws:ec2:us-east-1:123456789012:instance/i-abc123",
            "resource_type": "AWS::EC2::Instance",
            "region": "us-east-1",
            "account_id": "123456789012",
            "tags": {"Environment": "prod", "CostCenter": "engineering"},
        },
        {
            "resource_arn": "arn:aws:s3:::my-bucket",
            "resource_type": "AWS::S3::Bucket",
            "region": "us-east-1",
            "account_id": "123456789012",
            "tags": {"Team": "data"},
        },
        {
            "resource_arn": "arn:aws:lambda:us-west-2:987654321098:function:my-func",
            "resource_type": "AWS::Lambda::Function",
            "region": "us-west-2",
            "account_id": "987654321098",
            "tags": {},
        },
    ]


class TestWorkerInit:
    """Tests for CostAnalyzerWorker initialization."""

    def test_init_success(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        assert worker.region == "us-east-1"
        assert worker.s3_bucket == "test-bucket"
        assert worker.months_back == 2

    def test_init_missing_components_raises(self, env_vars):
        """When CURDirectMatcher is None, __init__ should raise RuntimeError."""
        with patch.object(worker_module, "CURDirectMatcher", None), patch.object(
            worker_module, "ParquetWriter", MagicMock()
        ), patch.object(
            worker_module, "get_active_cost_allocation_tags", MagicMock()
        ), patch.object(
            worker_module, "get_athena_rate_limiter", MagicMock()
        ), patch(
            "boto3.client", return_value=MagicMock()
        ):
            with pytest.raises(
                RuntimeError, match="Critical components failed to load"
            ) as exc_info:
                CostAnalyzerWorker()
            assert "src/lambda_handlers/cost modules" in str(exc_info.value)
            assert "src/cost/" not in str(exc_info.value)

    def test_init_defaults_when_env_missing(self):
        """Use defaults when env vars are not set."""
        with patch.dict(os.environ, {}, clear=True), patch.object(
            worker_module, "CURDirectMatcher", MagicMock(return_value=MagicMock())
        ), patch.object(
            worker_module, "ParquetWriter", MagicMock(return_value=MagicMock())
        ), patch.object(
            worker_module, "get_active_cost_allocation_tags", MagicMock(return_value=[])
        ), patch.object(
            worker_module, "get_athena_rate_limiter", MagicMock()
        ), patch(
            "boto3.client", return_value=MagicMock()
        ):
            worker = CostAnalyzerWorker()
            assert worker.region == "us-east-1"
            assert worker.months_back == 1


class TestParseTagsString:
    """Tests for _parse_tags_string method."""

    def test_json_format(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        result = worker._parse_tags_string('{"env":"prod","team":"data"}')
        assert result == {"env": "prod", "team": "data"}

    def test_athena_map_format(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        result = worker._parse_tags_string("{env=prod, team=data}")
        assert result == {"env": "prod", "team": "data"}

    def test_empty_string(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        assert worker._parse_tags_string("") == {}
        assert worker._parse_tags_string("   ") == {}

    def test_none_input(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        assert worker._parse_tags_string(None) == {}

    def test_empty_braces(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        assert worker._parse_tags_string("{}") == {}

    def test_unparseable_string(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        result = worker._parse_tags_string("not-a-tag-format")
        assert result == {}

    def test_athena_map_with_equals_in_value(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        result = worker._parse_tags_string("{key=val=ue}")
        assert result == {"key": "val=ue"}


class TestNormalizeResourceTags:
    """Tests for _normalize_resource_tags method."""

    def test_resource_with_dict_tags(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        resource = {
            "resource_arn": "arn:aws:ec2:us-east-1:123:instance/i-1",
            "resource_type": "EC2",
            "region": "us-east-1",
            "tags": {"CostCenter": "eng", "Name": "web"},
        }
        rows = worker._normalize_resource_tags(resource, None, ["CostCenter"])
        assert len(rows) == 2
        cost_row = next(r for r in rows if r["tag_key"] == "CostCenter")
        assert cost_row["is_cost_allocation_tag"] is True
        name_row = next(r for r in rows if r["tag_key"] == "Name")
        assert name_row["is_cost_allocation_tag"] is False

    def test_resource_with_string_tags(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        resource = {
            "resource_arn": "arn:aws:s3:::bucket",
            "resource_type": "S3",
            "region": "us-east-1",
            "tags": '{"Environment":"dev"}',
        }
        rows = worker._normalize_resource_tags(resource, None, ["Environment"])
        assert len(rows) == 1
        assert rows[0]["tag_key"] == "Environment"
        assert rows[0]["is_cost_allocation_tag"] is True

    def test_resource_with_no_tags(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        resource = {
            "resource_arn": "arn:aws:ec2:us-east-1:123:instance/i-2",
            "resource_type": "EC2",
            "region": "us-east-1",
            "tags": {},
        }
        rows = worker._normalize_resource_tags(resource, None, [])
        assert len(rows) == 1
        assert rows[0]["tag_key"] is None
        assert rows[0]["tag_value"] is None

    def test_resource_with_invalid_tags_type(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        resource = {
            "resource_arn": "arn:aws:ec2:us-east-1:123:instance/i-3",
            "resource_type": "EC2",
            "region": "us-east-1",
            "tags": 12345,  # invalid type
        }
        rows = worker._normalize_resource_tags(resource, None, [])
        assert len(rows) == 1
        assert rows[0]["tag_key"] is None


class TestCreateTagRow:
    """Tests for _create_tag_row method."""

    def test_with_cost_data(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        resource = {
            "resource_arn": "arn:test",
            "resource_type": "EC2",
            "region": "us-east-1",
        }
        # The matcher returns the current month's cost so far as "monthly_cost"
        cost_data = {"monthly_cost": 30.0}
        row = worker._create_tag_row(resource, "env", "prod", True, cost_data)
        today = date.today()
        days_in_month = calendar.monthrange(today.year, today.month)[1]
        assert row["tag_key"] == "env"
        assert row["tag_value"] == "prod"
        assert row["is_cost_allocation_tag"] is True
        assert row["current_month_mtd"] == 30.0
        assert row["last_month_cost"] == 0.0
        assert row["avg_daily_cost"] == pytest.approx(30.0 / today.day)
        assert row["current_month_projected"] == pytest.approx(30.0 / today.day * days_in_month)
        assert row["analysis_date"] == today

    def test_without_cost_data(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        resource = {
            "resource_arn": "arn:test",
            "resource_type": "S3",
            "region": "us-west-2",
        }
        row = worker._create_tag_row(resource, "team", "data", False, None)
        assert row["last_month_cost"] == 0.0
        assert row["current_month_mtd"] == 0.0
        assert row["avg_daily_cost"] == 0.0
        assert row["current_month_projected"] == 0.0

    def test_null_tag_key_and_value(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        resource = {
            "resource_arn": "arn:test",
            "resource_type": "EC2",
            "region": "us-east-1",
        }
        row = worker._create_tag_row(resource, None, None, False, None)
        assert row["tag_key"] is None
        assert row["tag_value"] is None


class TestGetResourceCosts:
    """Tests for _get_resource_costs method."""

    def test_returns_costs_from_cur(self, env_vars, mock_deps):
        worker, mock_cur, _, _, mock_rl = mock_deps
        mock_cur.get_costs_for_resources.return_value = [
            {"resource_arn": "arn:test", "monthly_cost": 50.0}
        ]
        result = worker._get_resource_costs([{"resource_arn": "arn:test"}])
        assert len(result) == 1
        assert result[0]["monthly_cost"] == 50.0
        mock_rl.acquire_query_slot.assert_called_once()

    def test_returns_empty_when_cur_unavailable(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        worker.cur_matcher = None
        result = worker._get_resource_costs([{"resource_arn": "arn:test"}])
        assert result == []

    def test_returns_empty_on_exception(self, env_vars, mock_deps):
        worker, mock_cur, _, _, _ = mock_deps
        mock_cur.get_costs_for_resources.side_effect = Exception("Athena timeout")
        result = worker._get_resource_costs([{"resource_arn": "arn:test"}])
        assert result == []


class TestGetCostAllocationTags:
    """Tests for _get_cost_allocation_tags caching."""

    def test_caches_result(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        mock_fn = MagicMock(return_value=["Tag1", "Tag2"])
        with patch.object(worker_module, "get_active_cost_allocation_tags", mock_fn):
            worker._cost_allocation_tags = None
            result1 = worker._get_cost_allocation_tags()
            result2 = worker._get_cost_allocation_tags()
            assert result1 == ["Tag1", "Tag2"]
            # Second call uses cache, so mock called only once
            assert mock_fn.call_count == 1

    def test_returns_empty_on_error(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        mock_fn = MagicMock(side_effect=Exception("API error"))
        with patch.object(worker_module, "get_active_cost_allocation_tags", mock_fn):
            worker._cost_allocation_tags = None
            result = worker._get_cost_allocation_tags()
            assert result == []


class TestPublishMetric:
    """Tests for _publish_metric method."""

    def test_publishes_to_cloudwatch(self, env_vars, mock_deps):
        worker, _, _, mock_cw, _ = mock_deps
        worker._publish_metric("TestMetric", 42)
        mock_cw.put_metric_data.assert_called_once()
        args = mock_cw.put_metric_data.call_args
        assert args[1]["Namespace"] == "TagInsights/CostAnalysis"
        assert args[1]["MetricData"][0]["MetricName"] == "TestMetric"
        assert args[1]["MetricData"][0]["Value"] == 42

    def test_handles_cloudwatch_error(self, env_vars, mock_deps):
        worker, _, _, mock_cw, _ = mock_deps
        mock_cw.put_metric_data.side_effect = Exception("CW error")
        # Should not raise
        worker._publish_metric("TestMetric", 1)


class TestProcessBatch:
    """Tests for process_batch method."""

    def test_processes_resources_grouped_by_account(
        self, env_vars, mock_deps, sample_resources
    ):
        worker, mock_cur, mock_writer, _, _ = mock_deps
        mock_cur.get_costs_for_resources.return_value = [
            {
                "resource_arn": "arn:aws:ec2:us-east-1:123456789012:instance/i-abc123",
                "monthly_cost": 100.0,
            },
            {"resource_arn": "arn:aws:s3:::my-bucket", "monthly_cost": 5.0},
        ]
        mock_writer.write_tag_costs.return_value = {"rows_written": 3}

        stats = worker.process_batch(sample_resources)
        assert stats["resources_processed"] == 3
        assert stats["accounts_count"] == 2
        assert "123456789012" in stats["accounts_processed"]

    def test_continues_on_account_error(self, env_vars, mock_deps):
        worker, mock_cur, mock_writer, _, _ = mock_deps
        # _get_resource_costs catches exceptions internally and returns [].
        # To trigger the outer except we need the writer to fail for acct1.
        mock_cur.get_costs_for_resources.return_value = []
        mock_writer.write_tag_costs.side_effect = [
            Exception("S3 write error"),  # acct1 fails at write
            {},  # acct2 succeeds
        ]

        resources = [
            {"resource_arn": "arn:1", "account_id": "acct1", "tags": {"k": "v"}},
            {"resource_arn": "arn:2", "account_id": "acct2", "tags": {"k": "v"}},
        ]
        stats = worker.process_batch(resources)
        # acct1 fails at write (after normalization), acct2 succeeds
        # Both accounts' resources are processed before write is attempted
        assert stats["accounts_count"] == 1
        assert "acct2" in stats["accounts_processed"]

    def test_empty_batch(self, env_vars, mock_deps):
        worker, _, _, _, _ = mock_deps
        stats = worker.process_batch([])
        assert stats["resources_processed"] == 0
        assert stats["normalized_rows_written"] == 0


class TestLambdaHandler:
    """Tests for lambda_handler function."""

    def test_processes_sqs_messages(self, env_vars):
        event = {
            "Records": [
                {
                    "body": json.dumps(
                        {
                            "batch_id": "batch-1",
                            "resources": [
                                {
                                    "resource_arn": "arn:test",
                                    "account_id": "111",
                                    "tags": {"k": "v"},
                                }
                            ],
                        }
                    )
                }
            ]
        }
        mock_worker = MagicMock()
        mock_worker.process_batch.return_value = {
            "resources_processed": 1,
            "normalized_rows_written": 1,
        }
        with patch.object(
            worker_module, "CostAnalyzerWorker", return_value=mock_worker
        ):
            result = lambda_handler(event, None)
            body = json.loads(result["body"])
            assert result["statusCode"] == 200
            assert body["status"] == "success"
            assert body["messages_processed"] == 1

    def test_partial_failure(self, env_vars):
        event = {
            "Records": [
                {"body": json.dumps({"batch_id": "b1", "resources": []})},
                {"body": "invalid-json"},
            ]
        }
        mock_worker = MagicMock()
        mock_worker.process_batch.return_value = {
            "resources_processed": 0,
            "normalized_rows_written": 0,
        }
        with patch.object(
            worker_module, "CostAnalyzerWorker", return_value=mock_worker
        ):
            result = lambda_handler(event, None)
            body = json.loads(result["body"])
            assert result["statusCode"] == 207
            assert body["status"] == "partial_success"
            assert body["messages_failed"] == 1

    def test_all_messages_fail(self, env_vars):
        event = {"Records": [{"body": "bad"}, {"body": "also-bad"}]}
        mock_worker = MagicMock()
        with patch.object(
            worker_module, "CostAnalyzerWorker", return_value=mock_worker
        ):
            result = lambda_handler(event, None)
            body = json.loads(result["body"])
            assert result["statusCode"] == 207
            assert body["messages_failed"] == 2

    def test_empty_event(self, env_vars):
        event = {"Records": []}
        mock_worker = MagicMock()
        with patch.object(
            worker_module, "CostAnalyzerWorker", return_value=mock_worker
        ):
            result = lambda_handler(event, None)
            body = json.loads(result["body"])
            assert result["statusCode"] == 200
            assert body["total_resources_processed"] == 0

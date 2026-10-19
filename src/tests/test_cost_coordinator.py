"""Tests for Cost Analyzer Coordinator Lambda Handler."""

import json
import os
import sys
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    """Set required env vars for all tests."""
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("ATHENA_DATABASE", "ti_test_db")
    monkeypatch.setenv(
        "COST_ANALYSIS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/queue.fifo"
    )
    monkeypatch.setenv("ATHENA_OUTPUT_BUCKET", "test-athena-output")
    monkeypatch.setenv("ATHENA_WORKGROUP", "test-workgroup")
    monkeypatch.setenv("BATCH_SIZE", "100")
    monkeypatch.setenv("TEST_MODE", "false")


def _build_coordinator(athena_results=None):
    """Create a CostAnalyzerCoordinator with mocked AWS clients."""
    mock_athena_executor = MagicMock()
    if athena_results is not None:
        mock_athena_executor.execute_query.return_value = athena_results

    with patch(
        "lambda_handlers.cost.cost_analyzer_coordinator_handler.boto3.client"
    ) as mock_client:
        mock_sqs = MagicMock()
        mock_cw = MagicMock()
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}

        def client_factory(service, **kwargs):
            if service == "sqs":
                return mock_sqs
            elif service == "cloudwatch":
                return mock_cw
            elif service == "sts":
                return mock_sts
            return MagicMock()

        mock_client.side_effect = client_factory

        with patch(
            "lambda_handlers.cost.cost_analyzer_coordinator_handler.AthenaQueryExecutor",
            return_value=mock_athena_executor,
        ):
            from lambda_handlers.cost.cost_analyzer_coordinator_handler import (
                CostAnalyzerCoordinator,
            )

            coordinator = CostAnalyzerCoordinator()

    coordinator.sqs = mock_sqs
    coordinator.cloudwatch = mock_cw
    coordinator.athena = mock_athena_executor
    return coordinator


def _sample_resources(count=5):
    """Generate sample changed resources."""
    return [
        {
            "resource_arn": f"arn:aws:ec2:us-east-1:123456789012:instance/i-{i:04d}",
            "resource_type": "ec2:instance",
            "region": "us-east-1",
            "account_id": "123456789012",
            "tags": json.dumps({"Environment": "prod", "Team": "backend"}),
            "timestamp": 1700000000 + i,
            "change_type": "TAG_CHANGE",
        }
        for i in range(count)
    ]


class TestCoordinatorInit:
    """Tests for coordinator initialization and configuration."""

    def test_reads_env_vars(self):
        """Reads configuration from environment variables."""
        coordinator = _build_coordinator()
        assert coordinator.database_name == "ti_test_db"
        assert coordinator.region == "us-east-1"
        assert coordinator.environment == "dev"
        assert coordinator.batch_size == 100

    def test_queue_url_from_env(self):
        """Gets SQS queue URL from env."""
        coordinator = _build_coordinator()
        assert (
            coordinator.queue_url
            == "https://sqs.us-east-1.amazonaws.com/123/queue.fifo"
        )

    def test_workgroup_from_env(self):
        """Gets Athena workgroup from env."""
        coordinator = _build_coordinator()
        assert coordinator.workgroup == "test-workgroup"

    def test_fallback_database_name(self, monkeypatch):
        """Constructs database name if ATHENA_DATABASE not set."""
        monkeypatch.delenv("ATHENA_DATABASE", raising=False)

        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "999888777666"}

        with patch(
            "lambda_handlers.cost.cost_analyzer_coordinator_handler.boto3.client"
        ) as mock_client:
            mock_client.return_value = mock_sts

            with patch(
                "lambda_handlers.cost.cost_analyzer_coordinator_handler.AthenaQueryExecutor",
                None,
            ):
                from lambda_handlers.cost.cost_analyzer_coordinator_handler import (
                    CostAnalyzerCoordinator,
                )

                coordinator = CostAnalyzerCoordinator()

        assert "999888777666" in coordinator.database_name


class TestGetChangedResources:
    """Tests for Athena query and resource retrieval."""

    def test_returns_athena_results(self):
        """Returns results from Athena query."""
        resources = _sample_resources(3)
        coordinator = _build_coordinator(athena_results=resources)
        result = coordinator.get_changed_resources(test_mode=False)
        assert len(result) == 3
        coordinator.athena.execute_query.assert_called_once()

    def test_normal_mode_includes_where_clause(self):
        """Normal mode query includes time filter WHERE clause."""
        coordinator = _build_coordinator(athena_results=[])
        coordinator.get_changed_resources(test_mode=False)
        query = coordinator.athena.execute_query.call_args[0][0]
        assert "WHERE" in query
        assert "date_add" in query

    def test_lookback_hours_sets_time_window(self):
        """lookback_hours widens the time filter (default 12)."""
        coordinator = _build_coordinator(athena_results=[])
        coordinator.get_changed_resources(test_mode=False, lookback_hours=96)
        query = coordinator.athena.execute_query.call_args[0][0]
        assert "date_add('hour', -96, now())" in query

    def test_test_mode_no_where_clause(self):
        """Test mode query has no WHERE clause."""
        coordinator = _build_coordinator(athena_results=[])
        coordinator.get_changed_resources(test_mode=True)
        query = coordinator.athena.execute_query.call_args[0][0]
        assert "WHERE" not in query

    def test_test_mode_from_env(self, monkeypatch):
        """Test mode reads from TEST_MODE env var when not explicitly set."""
        monkeypatch.setenv("TEST_MODE", "true")
        coordinator = _build_coordinator(athena_results=[])
        coordinator.get_changed_resources(test_mode=None)
        query = coordinator.athena.execute_query.call_args[0][0]
        assert "WHERE" not in query

    def test_hyphenated_database_gets_quoted(self, monkeypatch):
        """Database names with hyphens are quoted in query."""
        monkeypatch.setenv("ATHENA_DATABASE", "ti-analytics-db")
        coordinator = _build_coordinator(athena_results=[])
        coordinator.database_name = "ti-analytics-db"
        coordinator.get_changed_resources(test_mode=False)
        query = coordinator.athena.execute_query.call_args[0][0]
        assert '"ti-analytics-db"' in query

    def test_athena_error_raises(self):
        """Athena errors are propagated."""
        coordinator = _build_coordinator()
        coordinator.athena.execute_query.side_effect = Exception("Athena throttled")
        with pytest.raises(Exception, match="Athena throttled"):
            coordinator.get_changed_resources(test_mode=False)

    def test_publishes_metric_on_success(self):
        """Publishes ChangedResourcesFound metric."""
        coordinator = _build_coordinator(athena_results=_sample_resources(5))
        coordinator.get_changed_resources(test_mode=False)
        coordinator.cloudwatch.put_metric_data.assert_called()


class TestBatchResources:
    """Tests for resource batching logic."""

    def test_single_batch_for_small_list(self):
        """Resources fewer than batch_size produce one batch."""
        coordinator = _build_coordinator()
        resources = _sample_resources(50)
        batches = coordinator.batch_resources(resources)
        assert len(batches) == 1
        assert len(batches[0]) == 50

    def test_multiple_batches(self):
        """Resources exceeding batch_size are split correctly."""
        coordinator = _build_coordinator()
        coordinator.batch_size = 10
        resources = _sample_resources(25)
        batches = coordinator.batch_resources(resources)
        assert len(batches) == 3  # 10 + 10 + 5
        assert len(batches[0]) == 10
        assert len(batches[1]) == 10
        assert len(batches[2]) == 5

    def test_empty_list_returns_empty(self):
        """Empty resource list returns empty batch list."""
        coordinator = _build_coordinator()
        batches = coordinator.batch_resources([])
        assert batches == []

    def test_exact_batch_size(self):
        """Exactly batch_size resources produce one batch."""
        coordinator = _build_coordinator()
        coordinator.batch_size = 5
        resources = _sample_resources(5)
        batches = coordinator.batch_resources(resources)
        assert len(batches) == 1


class TestSendToSqs:
    """Tests for SQS message sending."""

    def test_sends_all_batches(self):
        """Sends one SQS message per batch."""
        coordinator = _build_coordinator()
        coordinator.sqs.send_message.return_value = {"MessageId": "msg-123"}
        batches = [_sample_resources(5), _sample_resources(3)]
        stats = coordinator.send_to_sqs(batches)
        assert stats["sent"] == 2
        assert stats["failed"] == 0
        assert coordinator.sqs.send_message.call_count == 2

    def test_message_body_structure(self):
        """SQS message body contains expected fields."""
        coordinator = _build_coordinator()
        coordinator.sqs.send_message.return_value = {"MessageId": "msg-123"}
        batches = [_sample_resources(2)]
        coordinator.send_to_sqs(batches)

        call_kwargs = coordinator.sqs.send_message.call_args[1]
        body = json.loads(call_kwargs["MessageBody"])
        assert "batch_id" in body
        assert "batch_size" in body
        assert "resources" in body
        assert "timestamp" in body
        assert "coordinator_run_id" in body

    def test_fifo_attributes(self):
        """Messages include MessageDeduplicationId and MessageGroupId."""
        coordinator = _build_coordinator()
        coordinator.sqs.send_message.return_value = {"MessageId": "msg-123"}
        batches = [_sample_resources(2)]
        coordinator.send_to_sqs(batches)

        call_kwargs = coordinator.sqs.send_message.call_args[1]
        assert "MessageDeduplicationId" in call_kwargs
        assert "MessageGroupId" in call_kwargs
        assert call_kwargs["MessageGroupId"].startswith("account-")

    def test_handles_send_failure(self):
        """Counts failed sends without raising."""
        coordinator = _build_coordinator()
        coordinator.sqs.send_message.side_effect = Exception("SQS error")
        batches = [_sample_resources(2), _sample_resources(2)]
        stats = coordinator.send_to_sqs(batches)
        assert stats["failed"] == 2
        assert stats["sent"] == 0

    def test_publishes_metrics(self):
        """Publishes CloudWatch metrics on send."""
        coordinator = _build_coordinator()
        coordinator.sqs.send_message.return_value = {"MessageId": "msg-123"}
        batches = [_sample_resources(3)]
        coordinator.send_to_sqs(batches)
        coordinator.cloudwatch.put_metric_data.assert_called()


class TestGenerateDedupId:
    """Tests for deduplication ID generation."""

    def test_deterministic(self):
        """Same batch produces same dedup ID."""
        coordinator = _build_coordinator()
        batch = _sample_resources(3)
        id1 = coordinator._generate_dedup_id(batch)
        id2 = coordinator._generate_dedup_id(batch)
        assert id1 == id2

    def test_different_batches_different_ids(self):
        """Different batches produce different IDs."""
        coordinator = _build_coordinator()
        batch1 = [{"resource_arn": "arn:aws:ec2:us-east-1:123:instance/i-001"}]
        batch2 = [{"resource_arn": "arn:aws:ec2:us-east-1:123:instance/i-002"}]
        id1 = coordinator._generate_dedup_id(batch1)
        id2 = coordinator._generate_dedup_id(batch2)
        assert id1 != id2

    def test_max_length(self):
        """Dedup ID does not exceed 128 characters."""
        coordinator = _build_coordinator()
        batch = _sample_resources(100)
        dedup_id = coordinator._generate_dedup_id(batch)
        assert len(dedup_id) <= 128


class TestGetRunId:
    """Tests for run ID generation."""

    def test_format(self):
        """Run ID has expected date-time format."""
        coordinator = _build_coordinator()
        run_id = coordinator._get_run_id()
        # Format: YYYYMMDD-HHMMSS
        assert len(run_id) == 15
        assert "-" in run_id


class TestPublishMetric:
    """Tests for CloudWatch metric publishing."""

    def test_publishes_to_correct_namespace(self):
        """Metric goes to TagInsights/CostAnalysis namespace."""
        coordinator = _build_coordinator()
        coordinator._publish_metric("TestMetric", 42)
        call_kwargs = coordinator.cloudwatch.put_metric_data.call_args[1]
        assert call_kwargs["Namespace"] == "TagInsights/CostAnalysis"

    def test_handles_publish_error(self):
        """Does not raise on metric publish failure."""
        coordinator = _build_coordinator()
        coordinator.cloudwatch.put_metric_data.side_effect = Exception("CW error")
        # Should not raise
        coordinator._publish_metric("TestMetric", 1)


def _patch_handler(coord_setup=None, coord_error=None):
    """Context manager helper for lambda_handler tests."""
    from contextlib import contextmanager

    @contextmanager
    def _ctx():
        with patch(
            "lambda_handlers.cost.cost_analyzer_coordinator_handler.boto3.client",
            return_value=MagicMock(),
        ):
            with patch(
                "lambda_handlers.cost.cost_analyzer_coordinator_handler.CostAnalyzerCoordinator"
            ) as MockCoord:
                if coord_error:
                    MockCoord.side_effect = coord_error
                else:
                    mock_coord = MagicMock()
                    MockCoord.return_value = mock_coord
                    if coord_setup:
                        coord_setup(mock_coord)
                    yield mock_coord
                    return
                yield None

    return _ctx()


class TestLambdaHandler:
    """Tests for the lambda_handler entry point."""

    def test_success_with_resources(self):
        resources = _sample_resources(5)

        def setup(c):
            c.get_changed_resources.return_value = resources
            c.batch_resources.return_value = [resources]
            c.send_to_sqs.return_value = {"sent": 1, "failed": 0, "total_resources": 5}
            c._get_run_id.return_value = "20240101-120000"

        with _patch_handler(coord_setup=setup):
            from lambda_handlers.cost.cost_analyzer_coordinator_handler import (
                lambda_handler,
            )

            result = lambda_handler({"source": "aws.events"}, MagicMock())
        assert result["statusCode"] == 200
        assert json.loads(result["body"])["resources_found"] == 5

    def test_no_changed_resources(self):
        def setup(c):
            c.get_changed_resources.return_value = []

        with _patch_handler(coord_setup=setup):
            from lambda_handlers.cost.cost_analyzer_coordinator_handler import (
                lambda_handler,
            )

            result = lambda_handler({}, MagicMock())
        assert result["statusCode"] == 200
        assert json.loads(result["body"])["resources_found"] == 0

    def test_error_returns_500(self):
        with _patch_handler(coord_error=Exception("Init failed")):
            from lambda_handlers.cost.cost_analyzer_coordinator_handler import (
                lambda_handler,
            )

            result = lambda_handler({}, MagicMock())
        assert result["statusCode"] == 500
        assert json.loads(result["body"])["status"] == "error"

    def test_test_mode_from_event(self):
        def setup(c):
            c.get_changed_resources.return_value = []

        with _patch_handler(coord_setup=setup) as mock_coord:
            from lambda_handlers.cost.cost_analyzer_coordinator_handler import (
                lambda_handler,
            )

            lambda_handler({"test_mode": True}, MagicMock())
            mock_coord.get_changed_resources.assert_called_once_with(test_mode=True, lookback_hours=12)

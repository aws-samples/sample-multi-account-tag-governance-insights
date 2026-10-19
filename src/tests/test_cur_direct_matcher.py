"""Tests for CUR Direct Matcher (cost/cur_direct_matcher.py) - ARN normalization,
batch processing, S3 format generation, error handling, and cost matching."""

import os
import sys
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

_mock_schema_mgr = MagicMock()
_mock_schema_mgr.validate_cur_table.return_value = False

with patch.dict(os.environ, {}, clear=False):
    with patch(
        "lambda_handlers.cost.schema_manager.CURSchemaManager",
        return_value=_mock_schema_mgr,
    ):
        with patch("lambda_handlers.cost.cur_provider.CURCostProvider", MagicMock()):
            from lambda_handlers.cost.cur_direct_matcher import CURDirectMatcher

from lambda_handlers.cost.query_builder import CURQueryBuilder
from lambda_handlers.cost.schema_manager import CURSchemaManager
from tests.test_cur_schema_compatibility import NATIVE_CUR2_TABLE


CUR2_PROFILE = CURSchemaManager.build_schema_profile(NATIVE_CUR2_TABLE)


@pytest.fixture
def matcher_no_cur():
    """Matcher with CUR unavailable."""
    with patch.dict(os.environ, {}, clear=False):
        with patch(
            "lambda_handlers.cost.cur_direct_matcher.CURSchemaManager"
        ):
            m = CURDirectMatcher(region_name="us-east-1")
            assert m.cur_available is False
            return m


@pytest.fixture
def matcher_with_cur():
    """Matcher with CUR available and mocked provider."""
    with patch.dict(
        os.environ, {"CUR_ATHENA_DATABASE": "test_db", "CUR_ATHENA_TABLE": "test_table"}
    ):
        with patch(
            "lambda_handlers.cost.cur_direct_matcher.CURSchemaManager"
        ) as mock_sm:
            mock_sm.return_value.session = MagicMock()
            with patch(
                "lambda_handlers.cost.cur_direct_matcher.CURCostProvider"
            ) as mock_prov:
                mock_prov.return_value.schema_profile = CUR2_PROFILE
                mock_prov.return_value.query_builder = MagicMock()
                m = CURDirectMatcher(region_name="us-east-1")
                return m


def _make_matcher():
    with patch("lambda_handlers.cost.cur_direct_matcher.CURSchemaManager"):
        return CURDirectMatcher()


# --- Initialization ---


class TestInitialization:
    def test_init_without_env_vars(self, matcher_no_cur):
        assert matcher_no_cur.cur_available is False
        assert matcher_no_cur.cur_provider is None
        assert matcher_no_cur.batch_size == 50

    def test_init_with_valid_cur(self, matcher_with_cur):
        assert matcher_with_cur.cur_available is True
        assert matcher_with_cur.cur_provider is not None

    def test_init_exception_sets_cur_unavailable(self):
        with patch.dict(
            os.environ, {"CUR_ATHENA_DATABASE": "db", "CUR_ATHENA_TABLE": "tbl"}
        ):
            with patch(
                "lambda_handlers.cost.cur_direct_matcher.CURSchemaManager"
            ) as mock_sm, patch(
                "lambda_handlers.cost.cur_direct_matcher.CURCostProvider",
                side_effect=RuntimeError("boom"),
            ):
                mock_sm.return_value.session = MagicMock()
                matcher = CURDirectMatcher()
                assert matcher.cur_available is False
                assert matcher.cur_provider is None
                assert matcher.query_builder is None


# --- ARN Normalization ---


class TestArnNormalization:
    def setup_method(self):
        self.m = _make_matcher()

    def test_empty_arn(self):
        assert self.m._normalize_arn_for_cur("", "ec2") == ""

    # Services that keep full ARN
    @pytest.mark.parametrize(
        "service",
        [
            "lambda",
            "rds",
            "aurora",
            "dynamodb",
            "sqs",
            "sns",
            "kms",
            "secretsmanager",
            "acm",
            "cognito",
            "glue",
            "sagemaker",
            "bedrock",
            "kendra",
            "comprehend",
            "cloudtrail",
            "cloudformation",
            "efs",
            "elb",
            "stepfunctions",
            "logs",
            "opensearch",
        ],
    )
    def test_full_arn_services(self, service):
        arn = f"arn:aws:{service}:us-east-1:123456789012:resource/my-res"
        assert self.m._normalize_arn_for_cur(arn, service) == arn

    def test_ec2_instance_extracts_id(self):
        arn = "arn:aws:ec2:us-east-1:123:instance/i-abc123"
        assert self.m._normalize_arn_for_cur(arn, "ec2") == "i-abc123"

    def test_ec2_volume_extracts_id(self):
        arn = "arn:aws:ec2:us-east-1:123:volume/vol-abc123"
        assert self.m._normalize_arn_for_cur(arn, "ec2") == "vol-abc123"

    def test_ec2_snapshot_keeps_full_arn(self):
        arn = "arn:aws:ec2:us-east-2:123:snapshot/snap-abc"
        assert self.m._normalize_arn_for_cur(arn, "ec2") == arn

    def test_ec2_eni_keeps_full_arn(self):
        arn = "arn:aws:ec2:us-east-1:123:network-interface/eni-abc"
        assert self.m._normalize_arn_for_cur(arn, "ec2") == arn

    def test_ec2_elastic_ip_keeps_full_arn(self):
        arn = "arn:aws:ec2:us-east-1:123:elastic-ip/eipalloc-abc"
        assert self.m._normalize_arn_for_cur(arn, "ec2") == arn

    def test_ec2_security_group_extracts_id(self):
        arn = "arn:aws:ec2:us-east-1:123:security-group/sg-abc"
        assert self.m._normalize_arn_for_cur(arn, "ec2") == "sg-abc"

    def test_ec2_vpc_extracts_id(self):
        arn = "arn:aws:ec2:us-east-1:123:vpc/vpc-abc"
        assert self.m._normalize_arn_for_cur(arn, "ec2") == "vpc-abc"

    def test_s3_full_arn(self):
        arn = "arn:aws:s3:::my-bucket"
        assert self.m._normalize_arn_for_cur(arn, "s3") == arn

    def test_s3_bucket_name_only(self):
        assert (
            self.m._normalize_arn_for_cur("my-bucket", "s3") == "arn:aws:s3:::my-bucket"
        )

    def test_ssm_extracts_id(self):
        arn = "arn:aws:ssm:us-east-1:123:parameter/my-param"
        assert self.m._normalize_arn_for_cur(arn, "ssm") == "my-param"

    def test_cloudfront_extracts_id(self):
        arn = "arn:aws:cloudfront::123:distribution/E123ABC"
        assert self.m._normalize_arn_for_cur(arn, "cloudfront") == "E123ABC"

    def test_iam_extracts_name(self):
        arn = "arn:aws:iam::123:role/my-role"
        assert self.m._normalize_arn_for_cur(arn, "iam") == "my-role"

    def test_unknown_service_with_slash(self):
        arn = "arn:aws:unknown:us-east-1:123:resource/my-id"
        assert self.m._normalize_arn_for_cur(arn, "unknown") == "my-id"

    def test_unknown_service_colon_only(self):
        arn = "arn:aws:unknown:us-east-1:123:my-resource"
        assert self.m._normalize_arn_for_cur(arn, "new-service") == "my-resource"


# --- Resource Explorer ARN Quirks ---


class TestFixResourceExplorerArnQuirks:
    def setup_method(self):
        self.m = _make_matcher()

    def test_no_rds_passes_through(self):
        arn = "arn:aws:ec2:us-east-1:123:instance/i-abc"
        assert self.m._fix_resource_explorer_arn_quirks(arn) == arn

    def test_empty_arn(self):
        assert self.m._fix_resource_explorer_arn_quirks("") == ""

    def test_cluster_snapshot_rds_prefix_removed(self):
        arn = "arn:aws:rds:us-east-1:123:cluster-snapshot:rds:my-snap"
        assert (
            self.m._fix_resource_explorer_arn_quirks(arn)
            == "arn:aws:rds:us-east-1:123:cluster-snapshot:my-snap"
        )

    def test_snapshot_rds_prefix_removed(self):
        arn = "arn:aws:rds:us-east-1:123:snapshot:rds:my-snap"
        assert (
            self.m._fix_resource_explorer_arn_quirks(arn)
            == "arn:aws:rds:us-east-1:123:snapshot:my-snap"
        )

    def test_normal_rds_arn_unchanged(self):
        arn = "arn:aws:rds:us-east-1:123:db:mydb"
        assert self.m._fix_resource_explorer_arn_quirks(arn) == arn


# --- S3 Search Formats ---


class TestGetS3SearchFormats:
    def setup_method(self):
        self.m = _make_matcher()

    def test_empty_arn(self):
        assert self.m._get_s3_search_formats("") == []

    def test_full_s3_arn(self):
        fmts = self.m._get_s3_search_formats("arn:aws:s3:::my-bucket")
        assert "arn:aws:s3:::my-bucket" in fmts
        assert "my-bucket" in fmts

    def test_bucket_name_only(self):
        fmts = self.m._get_s3_search_formats("my-bucket")
        assert "arn:aws:s3:::my-bucket" in fmts
        assert "my-bucket" in fmts

    def test_no_duplicates(self):
        fmts = self.m._get_s3_search_formats("arn:aws:s3:::x")
        assert len(fmts) == len(set(fmts))


# --- Group / Batch / Empty Results ---


class TestGroupBatchEmpty:
    def setup_method(self):
        self.m = _make_matcher()

    def test_group_empty(self):
        assert self.m._group_resources_by_service([]) == {}

    def test_group_multiple_services(self):
        resources = [
            {"resource_arn": "a", "service_type": "ec2"},
            {"resource_arn": "b", "service_type": "s3"},
            {"resource_arn": "c", "service_type": "ec2"},
        ]
        groups = self.m._group_resources_by_service(resources)
        assert len(groups) == 2
        assert len(groups["ec2"]) == 2

    def test_group_accepts_coordinator_resource_type(self):
        groups = self.m._group_resources_by_service(
            [{"resource_arn": "a", "resource_type": "dynamodb"}]
        )
        assert list(groups) == ["dynamodb"]

    def test_group_prefers_existing_service_type_contract(self):
        groups = self.m._group_resources_by_service(
            [
                {
                    "resource_arn": "a",
                    "service_type": "lambda",
                    "resource_type": "dynamodb",
                }
            ]
        )
        assert list(groups) == ["lambda"]

    def test_group_missing_service_defaults_unknown(self):
        groups = self.m._group_resources_by_service([{"resource_arn": "a"}])
        assert "unknown" in groups

    def test_batch_empty(self):
        assert list(self.m._batch_resources([], 10)) == []

    def test_batch_splits_correctly(self):
        resources = [{"id": i} for i in range(25)]
        batches = list(self.m._batch_resources(resources, 10))
        assert len(batches) == 3
        assert len(batches[2]) == 5

    def test_empty_cost_results_preserves_fields(self):
        results = self.m._create_empty_cost_results(
            [{"resource_arn": "x", "name": "y"}]
        )
        assert results[0]["resource_arn"] == "x"
        assert results[0]["monthly_cost"] == 0.0
        assert results[0]["matched_in_cur"] is False

    def test_empty_cost_results_empty_input(self):
        assert self.m._create_empty_cost_results([]) == []


# --- get_costs_for_resources ---


class TestGetCostsForResources:
    def test_cur_unavailable(self, matcher_no_cur):
        res = [
            {
                "resource_arn": "arn:aws:ec2:us-east-1:123:instance/i-a",
                "service_type": "ec2",
            }
        ]
        results = matcher_no_cur.get_costs_for_resources(res)
        assert results[0]["monthly_cost"] == 0.0

    def test_empty_resources(self, matcher_with_cur):
        assert matcher_with_cur.get_costs_for_resources([]) == []

    def test_successful_matching(self, matcher_with_cur):
        matcher_with_cur.query_builder.build_direct_arn_costs_query.return_value = (
            "SELECT 1"
        )
        matcher_with_cur.cur_provider._execute_athena_query.return_value = {
            "data": [
                {
                    "resource_id": "i-abc",
                    "monthly_cost": "10.5",
                    "usage_records": "5",
                    "avg_daily_cost": "0.35",
                    "first_usage": "2024-01-01",
                    "last_usage": "2024-01-31",
                }
            ]
        }
        res = [
            {
                "resource_arn": "arn:aws:ec2:us-east-1:123:instance/i-abc",
                "service_type": "ec2",
            }
        ]
        results = matcher_with_cur.get_costs_for_resources(res, months_back=1)
        assert results[0]["monthly_cost"] == 10.5
        assert results[0]["matched_in_cur"] is True

    def test_coordinator_resource_type_preserves_full_dynamodb_arn(
        self, matcher_with_cur
    ):
        resource_arn = (
            "arn:aws:dynamodb:us-east-1:123456789012:"
            "table/SpringCleanTable"
        )
        matcher_with_cur.query_builder.build_direct_arn_costs_query.return_value = (
            "SELECT 1"
        )
        matcher_with_cur.cur_provider._execute_athena_query.return_value = {
            "data": [
                {
                    "resource_id": resource_arn,
                    "monthly_cost": "0.25",
                    "usage_records": "10",
                    "avg_daily_cost": "0.01",
                    "first_usage": "2026-08-01",
                    "last_usage": "2026-08-25",
                }
            ]
        }

        results = matcher_with_cur.get_costs_for_resources(
            [{"resource_arn": resource_arn, "resource_type": "dynamodb"}]
        )

        query_args = (
            matcher_with_cur.query_builder
            .build_direct_arn_costs_query.call_args.args
        )
        assert query_args[0] == [resource_arn]
        assert query_args[1] == "dynamodb"
        assert results[0]["monthly_cost"] == 0.25
        assert results[0]["matched_in_cur"] is True

    def test_exception_fallback(self, matcher_with_cur):
        matcher_with_cur.query_builder.build_direct_arn_costs_query.side_effect = (
            Exception("fail")
        )
        res = [
            {
                "resource_arn": "arn:aws:ec2:us-east-1:123:instance/i-x",
                "service_type": "ec2",
            }
        ]
        results = matcher_with_cur.get_costs_for_resources(res)
        assert results[0]["monthly_cost"] == 0.0


# --- S3 Batch Processing ---


class TestProcessS3ResourceBatch:
    def test_matched(self, matcher_with_cur):
        matcher_with_cur.query_builder.build_direct_arn_costs_query.return_value = (
            "SELECT 1"
        )
        matcher_with_cur.cur_provider._execute_athena_query.return_value = {
            "data": [
                {
                    "resource_id": "arn:aws:s3:::my-bucket",
                    "monthly_cost": "5.0",
                    "usage_records": "10",
                    "avg_daily_cost": "0.17",
                    "first_usage": "2024-01-01",
                    "last_usage": "2024-01-31",
                }
            ]
        }
        res = [{"resource_arn": "arn:aws:s3:::my-bucket", "service_type": "s3"}]
        results = matcher_with_cur._process_s3_resource_batch(res, months_back=1)
        assert results[0]["monthly_cost"] == 5.0
        assert "s3_search_formats" in results[0]

    def test_no_match(self, matcher_with_cur):
        matcher_with_cur.query_builder.build_direct_arn_costs_query.return_value = (
            "SELECT 1"
        )
        matcher_with_cur.cur_provider._execute_athena_query.return_value = {"data": []}
        res = [{"resource_arn": "arn:aws:s3:::unknown", "service_type": "s3"}]
        results = matcher_with_cur._process_s3_resource_batch(res, months_back=1)
        assert results[0]["monthly_cost"] == 0.0


# --- Query CUR for ARNs ---


class TestQueryCurForArns:
    def test_no_query_built(self, matcher_with_cur):
        matcher_with_cur.query_builder.build_direct_arn_costs_query.return_value = None
        assert matcher_with_cur._query_cur_for_arns(["a"], "ec2", 1) == {}

    def test_query_error(self, matcher_with_cur):
        matcher_with_cur.query_builder.build_direct_arn_costs_query.return_value = (
            "SELECT 1"
        )
        matcher_with_cur.cur_provider._execute_athena_query.return_value = {
            "error": "timeout"
        }
        assert matcher_with_cur._query_cur_for_arns(["a"], "ec2", 1) == {}

    def test_successful_mapping(self, matcher_with_cur):
        matcher_with_cur.query_builder.build_direct_arn_costs_query.return_value = (
            "SELECT 1"
        )
        matcher_with_cur.cur_provider._execute_athena_query.return_value = {
            "data": [
                {
                    "resource_id": "arn1",
                    "monthly_cost": "25.0",
                    "usage_records": "100",
                    "avg_daily_cost": "0.83",
                    "first_usage": "2024-01-01",
                    "last_usage": "2024-01-31",
                }
            ]
        }
        result = matcher_with_cur._query_cur_for_arns(["arn1"], "lambda", 1)
        assert result["arn1"]["monthly_cost"] == 25.0
        assert result["arn1"]["matched_in_cur"] is True


# --- Supported Services / test_arn_normalization ---


class TestSupportedServices:
    def setup_method(self):
        self.m = _make_matcher()

    def test_list_has_expected_services(self):
        services = self.m.get_supported_services()
        assert len(services) > 40
        for s in ["ec2", "s3", "lambda", "rds", "dynamodb"]:
            assert s in services

    def test_test_arn_normalization(self):
        test_arns = [
            ("arn:aws:ec2:us-east-1:123:instance/i-abc", "ec2"),
            ("arn:aws:lambda:us-east-1:123:function:f", "lambda"),
        ]
        results = self.m.test_arn_normalization(test_arns)
        assert results["test_count"] == 2
        assert results["normalization_results"][0]["normalized_arn"] == "i-abc"


def make_cur2_matcher():
    matcher = CURDirectMatcher.__new__(CURDirectMatcher)
    matcher.region = "us-east-1"
    matcher.cur_available = False
    matcher.cur_provider = None
    matcher.query_builder = None
    matcher.schema_manager = MagicMock()
    matcher.schema_manager.session = MagicMock()
    return matcher


@patch("lambda_handlers.cost.cur_direct_matcher.CURCostProvider")
def test_direct_matcher_reuses_provider_session_profile_and_builder(mock_provider):
    matcher = make_cur2_matcher()
    provider = mock_provider.return_value
    provider.schema_profile = CUR2_PROFILE
    provider.query_builder = CURQueryBuilder("billing", "cur2", CUR2_PROFILE)

    with patch.dict(
        os.environ,
        {
            "CUR_ATHENA_DATABASE": "billing",
            "CUR_ATHENA_TABLE": "cur2",
        },
        clear=False,
    ):
        matcher._initialize_cur_provider()

    assert matcher.cur_available is True
    assert matcher.query_builder is provider.query_builder
    assert matcher.query_builder.schema_profile is provider.schema_profile
    mock_provider.assert_called_once_with(
        "billing",
        "cur2",
        "us-east-1",
        cur_session=matcher.schema_manager.session,
    )
    matcher.schema_manager.validate_cur_table.assert_not_called()


@patch("lambda_handlers.cost.cur_direct_matcher.CURCostProvider")
def test_direct_matcher_does_not_scan_glue_without_explicit_configuration(
    mock_provider,
):
    matcher = make_cur2_matcher()

    with patch.dict(os.environ, {}, clear=True):
        matcher._initialize_cur_provider()

    assert matcher.cur_available is False
    mock_provider.assert_not_called()
    matcher.schema_manager.glue_client.get_databases.assert_not_called()
    matcher.schema_manager.glue_client.get_tables.assert_not_called()


@patch("lambda_handlers.cost.cur_direct_matcher.CURCostProvider")
def test_direct_matcher_fails_closed_when_provider_initialization_fails(
    mock_provider,
):
    matcher = make_cur2_matcher()
    mock_provider.side_effect = RuntimeError("role assumption failed")

    with patch.dict(
        os.environ,
        {
            "CUR_ATHENA_DATABASE": "billing",
            "CUR_ATHENA_TABLE": "cur2",
        },
        clear=False,
    ):
        matcher._initialize_cur_provider()

    assert matcher.cur_available is False
    assert matcher.cur_provider is None
    assert matcher.query_builder is None


def test_direct_matcher_uses_cur2_direct_query_and_provider_execution():
    matcher = make_cur2_matcher()
    matcher.cur_available = True
    matcher.query_builder = CURQueryBuilder("billing", "cur2", CUR2_PROFILE)
    matcher.cur_provider = MagicMock()
    matcher.cur_provider._execute_athena_query.return_value = {
        "data": [
            {
                "resource_id": "arn:aws:lambda:us-east-1:123456789012:function:f",
                "monthly_cost": "5.0",
                "usage_records": "2",
                "avg_daily_cost": "0.16",
                "first_usage": "2026-08-01",
                "last_usage": "2026-08-20",
            }
        ]
    }

    result = matcher._query_cur_for_arns(
        ["arn:aws:lambda:us-east-1:123456789012:function:f"],
        "lambda",
        1,
    )

    assert result[
        "arn:aws:lambda:us-east-1:123456789012:function:f"
    ]["monthly_cost"] == 5.0
    query = matcher.cur_provider._execute_athena_query.call_args.args[0]
    assert "billing_period = '" in query
    assert "line_item_unblended_cost" in query


def test_direct_matcher_retains_fifty_resource_batch_bound():
    matcher = CURDirectMatcher.__new__(CURDirectMatcher)
    matcher.batch_size = 50

    batches = list(matcher._batch_resources([{"id": i} for i in range(101)], 50))

    assert [len(batch) for batch in batches] == [50, 50, 1]

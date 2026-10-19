"""
Comprehensive tests for CUR Query Builder (cost/query_builder.py)

Tests all public methods, input validation, SQL generation correctness,
edge cases, and the _escape_sql_string helper.
"""

import os
import re
import sys
from datetime import datetime
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lambda_handlers.cost.query_builder import (
    CURQueryBuilder,
    _escape_sql_string,
    _ARN_RE,
    _RESOURCE_ID_RE,
    _SQL_IDENTIFIER_RE,
    _TAG_KEY_RE,
    _TAG_VALUE_SAFE_RE,
    _SERVICE_TYPE_RE,
)
from lambda_handlers.cost.schema_manager import CURSchemaManager
from tests.test_cur_schema_compatibility import (
    CUR1_TABLE,
    NATIVE_CUR2_TABLE,
    TRANSFORMED_CUR2_TABLE,
)

VALID_ARN = "arn:aws:ec2:us-east-1:123456789012:instance/i-abc123"
VALID_ARN_2 = "arn:aws:s3:us-east-1:123456789012:bucket/my-bucket"


@pytest.fixture
def builder():
    return CURQueryBuilder(database="cur_db", table="cur_table")


@pytest.fixture
def available_columns():
    return {
        "resource_tags_user_environment",
        "resource_tags_user_team",
        "resource_tags_user_project",
        "resource_tags_user_costcenter",
    }


class TestEscapeSqlString:
    def test_no_quotes(self):
        assert _escape_sql_string("hello") == "hello"

    def test_single_quote(self):
        assert _escape_sql_string("it's") == "it''s"

    def test_multiple_quotes(self):
        assert _escape_sql_string("a'b'c") == "a''b''c"

    def test_empty_string(self):
        assert _escape_sql_string("") == ""

    def test_only_quotes(self):
        assert _escape_sql_string("'''") == "''''''"


class TestRegexValidators:
    def test_arn_re_valid(self):
        assert _ARN_RE.fullmatch(VALID_ARN)
        assert _ARN_RE.fullmatch("arn:aws-cn:s3:cn-north-1:123456789012:bucket/x")
        assert _ARN_RE.fullmatch("arn:aws-us-gov:iam:us-gov-west-1:123456789012:role/r")

    def test_arn_re_invalid(self):
        assert _ARN_RE.fullmatch("not-an-arn") is None
        assert _ARN_RE.fullmatch("arn:azure:ec2:us-east-1:123456789012:foo") is None
        assert _ARN_RE.fullmatch("") is None

    def test_resource_id_re_accepts_cur_ids_and_rejects_sql_payloads(self):
        assert _RESOURCE_ID_RE.fullmatch("i-abc123")
        assert _RESOURCE_ID_RE.fullmatch("my-bucket")
        assert _RESOURCE_ID_RE.fullmatch("arn:aws:s3:::my-bucket")
        assert _RESOURCE_ID_RE.fullmatch("x'); DROP TABLE cur; --") is None

    def test_identifier_and_tag_key_patterns(self):
        assert _SQL_IDENTIFIER_RE.fullmatch("billing_cur_2026")
        assert _SQL_IDENTIFIER_RE.fullmatch("billing-cur") is None
        assert _TAG_KEY_RE.fullmatch("aws:createdBy")
        assert _TAG_KEY_RE.fullmatch("Environment'); DROP") is None

    def test_tag_value_safe_re_valid(self):
        assert _TAG_VALUE_SAFE_RE.fullmatch("production")
        assert _TAG_VALUE_SAFE_RE.fullmatch("dev-team-1")
        assert _TAG_VALUE_SAFE_RE.fullmatch("")  # empty is valid (length 0-256)

    def test_tag_value_safe_re_invalid(self):
        assert _TAG_VALUE_SAFE_RE.fullmatch("val;ue") is None
        assert _TAG_VALUE_SAFE_RE.fullmatch("val'ue") is None
        assert _TAG_VALUE_SAFE_RE.fullmatch('val"ue') is None
        assert _TAG_VALUE_SAFE_RE.fullmatch("val\\ue") is None
        assert _TAG_VALUE_SAFE_RE.fullmatch("x" * 257) is None

    def test_service_type_re_valid(self):
        assert _SERVICE_TYPE_RE.fullmatch("ec2")
        assert _SERVICE_TYPE_RE.fullmatch("s3")
        assert _SERVICE_TYPE_RE.fullmatch("kinesis-firehose")
        assert _SERVICE_TYPE_RE.fullmatch("cloud9")

    def test_service_type_re_invalid(self):
        assert _SERVICE_TYPE_RE.fullmatch("") is None
        assert _SERVICE_TYPE_RE.fullmatch("EC2") is None
        assert _SERVICE_TYPE_RE.fullmatch("1ec2") is None
        assert _SERVICE_TYPE_RE.fullmatch("a" * 65) is None
        assert _SERVICE_TYPE_RE.fullmatch("ec2; DROP TABLE") is None


class TestConstructor:
    def test_basic_initialization(self, builder):
        assert builder.database == "cur_db"
        assert builder.table == "cur_table"
        assert builder.full_table_name == "cur_db.cur_table"

    def test_full_table_name_format(self):
        b = CURQueryBuilder("my_database", "my_table_2024")
        assert b.full_table_name == "my_database.my_table_2024"


class TestBuildCostsByTagQuery:
    def test_returns_valid_sql(self, builder, available_columns):
        query, missing = builder.build_costs_by_tag_query(
            "environment", 3, available_columns
        )
        assert query is not None
        assert "SELECT" in query
        assert "FROM cur_db.cur_table" in query
        assert "resource_tags_user_environment" in query
        assert "GROUP BY" in query
        assert "ORDER BY total_cost DESC" in query
        assert missing == []

    def test_column_not_found_returns_none(self, builder, available_columns):
        query, missing = builder.build_costs_by_tag_query(
            "nonexistent", 3, available_columns
        )
        assert query is None
        assert missing == []

    def test_tag_key_lowercased(self, builder, available_columns):
        query, _ = builder.build_costs_by_tag_query("Environment", 3, available_columns)
        assert "resource_tags_user_environment" in query

    def test_partition_filters_present(self, builder, available_columns):
        query, _ = builder.build_costs_by_tag_query("team", 6, available_columns)
        assert "year = '" in query
        assert "month = '" in query

    def test_months_back_1(self, builder, available_columns):
        query, _ = builder.build_costs_by_tag_query("environment", 1, available_columns)
        assert query is not None
        assert query.count("year = '") == 1


class TestBuildCostTrendsQuery:
    def test_returns_valid_sql(self, builder, available_columns):
        query = builder.build_cost_trends_query(
            "environment", "production", 3, available_columns
        )
        assert query is not None
        assert "month_key" in query
        assert "monthly_cost" in query
        assert "'production'" in query
        assert "GROUP BY year, month" in query

    def test_column_not_found_returns_none(self, builder, available_columns):
        query = builder.build_cost_trends_query(
            "missing_tag", "val", 3, available_columns
        )
        assert query is None

    def test_tag_value_escaped(self, builder, available_columns):
        query = builder.build_cost_trends_query(
            "environment", "prod-us-east", 3, available_columns
        )
        assert query is not None
        assert "prod-us-east" in query

    def test_unsafe_tag_value_rejected(self, builder, available_columns):
        query = builder.build_cost_trends_query(
            "environment", "val;drop", 3, available_columns
        )
        assert query is None

    def test_tag_value_with_single_quote_rejected(self, builder, available_columns):
        query = builder.build_cost_trends_query(
            "environment", "it's", 3, available_columns
        )
        assert query is None

    def test_tag_value_too_long_rejected(self, builder, available_columns):
        query = builder.build_cost_trends_query(
            "environment", "x" * 257, 3, available_columns
        )
        assert query is None


class TestBuildServiceCostsQuery:
    def test_returns_valid_sql(self, builder):
        query = builder.build_service_costs_query(3)
        assert "product_servicename" in query
        assert "FROM cur_db.cur_table" in query
        assert "LIMIT 50" in query
        assert "GROUP BY product_servicename" in query

    def test_partition_filters_for_months(self, builder):
        query = builder.build_service_costs_query(6)
        assert query.count("year = '") == 6


class TestBuildAccountCostsQuery:
    def test_returns_valid_sql(self, builder):
        query = builder.build_account_costs_query(3)
        assert "line_item_usage_account_id" in query
        assert "account_id" in query
        assert "LIMIT 25" in query
        assert "GROUP BY line_item_usage_account_id" in query

    def test_contains_partition_filters(self, builder):
        query = builder.build_account_costs_query(1)
        assert "year = '" in query
        assert "month = '" in query


class TestBuildDirectArnCostsQuery:
    def test_returns_valid_sql(self, builder):
        query = builder.build_direct_arn_costs_query([VALID_ARN], "ec2", 3)
        assert "SELECT" in query
        assert "line_item_resource_id" in query
        assert "IN (" in query
        assert "Amazon Elastic Compute Cloud" in query

    def test_empty_arns_returns_empty(self, builder):
        assert builder.build_direct_arn_costs_query([], "ec2", 3) == ""

    def test_invalid_resource_ids_filtered_out(self, builder):
        assert (
            builder.build_direct_arn_costs_query(
                ["x'); DROP TABLE cur; --", "bad value\nUNION SELECT"],
                "ec2",
                3,
            )
            == ""
        )

    def test_mixed_valid_invalid_arns(self, builder):
        query = builder.build_direct_arn_costs_query(
            [VALID_ARN, "x'); DROP TABLE cur; --", VALID_ARN_2], "ec2", 3
        )
        assert query != ""
        assert "i-abc123" in query
        assert "my-bucket" in query

    def test_no_service_filter_for_unknown(self, builder):
        query = builder.build_direct_arn_costs_query([VALID_ARN], "unknown", 3)
        assert "product_servicename LIKE" not in query

    def test_service_filter_for_s3(self, builder):
        query = builder.build_direct_arn_costs_query([VALID_ARN], "s3", 3)
        assert "Amazon Simple Storage Service" in query

    def test_invalid_service_type_raises(self, builder):
        with pytest.raises(ValueError, match="Invalid service_type"):
            builder.build_direct_arn_costs_query([VALID_ARN], "EC2; DROP", 3)

    def test_arn_with_single_quote_rejected(self, builder):
        arn_with_quote = "arn:aws:s3:us-east-1:123456789012:bucket/it's"
        query = builder.build_direct_arn_costs_query([arn_with_quote], "s3", 3)
        assert query == ""

    def test_multiple_arns_joined(self, builder):
        query = builder.build_direct_arn_costs_query([VALID_ARN, VALID_ARN_2], "ec2", 1)
        assert "', '" in query


class TestGeneratePartitionFilters:
    def test_returns_correct_count(self, builder):
        filters, info = builder._generate_partition_filters(3)
        assert len(filters) == 3
        assert info["partitions_generated"] == 3

    def test_filter_format(self, builder):
        filters, _ = builder._generate_partition_filters(1)
        assert re.search(r"\(year = '\d{4}' AND month = '\d{1,2}'\)", filters[0])

    def test_period_info_has_dates(self, builder):
        _, info = builder._generate_partition_filters(2)
        assert info["end_date"] is not None
        assert info["start_date"] is not None

    def test_months_covered_chronological(self, builder):
        _, info = builder._generate_partition_filters(3)
        assert len(info["months_covered"]) == 3
        months = [tuple(map(int, m.split("-"))) for m in info["months_covered"]]
        assert months == sorted(months)

    @patch.dict(os.environ, {"CUR_MONTH_FORMAT": "leading_zero"})
    def test_leading_zero_format(self, builder):
        filters, _ = builder._generate_partition_filters(1)
        match = re.search(r"month = '(\d+)'", filters[0])
        assert match
        assert len(match.group(1)) == 2

    @patch.dict(os.environ, {"CUR_MONTH_FORMAT": "no_leading_zero"})
    def test_no_leading_zero_format(self, builder):
        filters, _ = builder._generate_partition_filters(1)
        match = re.search(r"month = '(\d+)'", filters[0])
        assert match
        assert int(match.group(1)) == datetime.now().month


class TestEstimateQueryCost:
    def test_returns_expected_keys(self, builder):
        query = "SELECT x FROM t WHERE year = '2024' AND month = '6' GROUP BY x"
        result = builder.estimate_query_cost(query)
        assert "estimated_data_scanned_gb" in result
        assert "estimated_cost_usd" in result
        assert "partition_filters_detected" in result
        assert "cost_optimization_level" in result
        assert "recommendations" in result

    def test_explicit_data_scanned(self, builder):
        result = builder.estimate_query_cost("SELECT x FROM t", data_scanned_gb=5.0)
        expected_cost = round(5.0 * (5.0 / 1024), 4)
        assert result["estimated_cost_usd"] == expected_cost
        assert result["estimated_data_scanned_gb"] == 5.0

    def test_partition_filters_detected(self, builder):
        query = "WHERE year = '2024' AND month = '6' OR year = '2024' AND month = '5'"
        result = builder.estimate_query_cost(query)
        assert result["partition_filters_detected"] == 4

    def test_no_partitions_higher_cost(self, builder):
        cost_no = builder.estimate_query_cost("SELECT * FROM t")
        cost_yes = builder.estimate_query_cost(
            "SELECT x FROM t WHERE year = '2024' AND month = '6'"
        )
        assert (
            cost_no["estimated_data_scanned_gb"] > cost_yes["estimated_data_scanned_gb"]
        )


class TestAssessOptimizationLevel:
    def test_excellent(self, builder):
        query = (
            "SELECT x FROM t WHERE year = '2024' AND month = '6' GROUP BY x LIMIT 10"
        )
        assert builder._assess_optimization_level(query) == "excellent"

    def test_good(self, builder):
        query = "SELECT x FROM t WHERE year = '2024' AND month = '6' GROUP BY x"
        assert builder._assess_optimization_level(query) in ("excellent", "good")

    def test_poor(self, builder):
        assert builder._assess_optimization_level("SELECT * FROM t") == "poor"

    def test_fair(self, builder):
        query = "SELECT x FROM t WHERE year = '2024'"
        assert builder._assess_optimization_level(query) in ("fair", "good")


class TestGetCostOptimizationRecommendations:
    def test_select_star_recommendation(self, builder):
        recs = builder._get_cost_optimization_recommendations(
            "SELECT * FROM t WHERE year = '2024' AND month = '6'"
        )
        assert any("SELECT *" in r for r in recs)

    def test_no_partition_recommendation(self, builder):
        recs = builder._get_cost_optimization_recommendations(
            "SELECT x FROM t WHERE x > 0"
        )
        assert any("partition" in r.lower() for r in recs)

    def test_well_optimized(self, builder):
        recs = builder._get_cost_optimization_recommendations(
            "SELECT x FROM t WHERE year = '2024' AND month = '6' LIMIT 10"
        )
        assert any("well-optimized" in r for r in recs)

    def test_distinct_warning(self, builder):
        recs = builder._get_cost_optimization_recommendations(
            "SELECT DISTINCT x FROM t WHERE year = '2024' AND month = '6' GROUP BY x"
        )
        assert any("DISTINCT" in r for r in recs)

    def test_missing_limit_with_group_by(self, builder):
        recs = builder._get_cost_optimization_recommendations(
            "SELECT x FROM t WHERE year = '2024' AND month = '6' GROUP BY x"
        )
        assert any("LIMIT" in r for r in recs)


class TestGetQueryTemplate:
    def test_monthly_costs_template(self, builder):
        query = builder.get_query_template("monthly_costs", start_year="2024")
        assert query is not None
        assert "cur_db.cur_table" in query
        assert "2024" in query

    def test_top_resources_template(self, builder):
        query = builder.get_query_template(
            "top_resources", year="2024", month="06", limit="20"
        )
        assert query is not None
        assert "LIMIT 20" in query
        assert "year = '2024'" in query

    def test_service_summary_template(self, builder):
        query = builder.get_query_template("service_summary", year="2024", month="06")
        assert query is not None
        assert "product_servicename" in query

    def test_unknown_template_returns_none(self, builder):
        assert builder.get_query_template("nonexistent_template") is None

    def test_missing_parameter_returns_none(self, builder):
        assert builder.get_query_template("monthly_costs") is None

    def test_template_uses_full_table_name(self, builder):
        query = builder.get_query_template("monthly_costs", start_year="2025")
        assert "cur_db.cur_table" in query


CUR1_PROFILE = CURSchemaManager.build_schema_profile(CUR1_TABLE)
CUR2_PROFILE = CURSchemaManager.build_schema_profile(NATIVE_CUR2_TABLE)
TRANSFORMED_PROFILE = CURSchemaManager.build_schema_profile(TRANSFORMED_CUR2_TABLE)


def assert_partition_pruned(query):
    assert any(
        marker in query
        for marker in (
            "year = '",
            "year >= '",
            "billing_period = '",
            "billing_period >= '",
        )
    )


def test_database_and_table_identifiers_are_validated():
    with pytest.raises(ValueError, match="database"):
        CURQueryBuilder("billing; DROP", "cur")
    with pytest.raises(ValueError, match="table"):
        CURQueryBuilder("billing", "cur table")


def test_cur1_regular_and_direct_cost_semantics_are_preserved():
    builder = CURQueryBuilder("billing", "cur1", CUR1_PROFILE)
    available = {"resource_tags_user_environment"}

    tag_query, _ = builder.build_costs_by_tag_query("Environment", 1, available)
    trends_query = builder.build_cost_trends_query(
        "Environment",
        "production",
        1,
        available,
    )
    service_query = builder.build_service_costs_query(1)
    account_query = builder.build_account_costs_query(1)
    direct_query = builder.build_direct_arn_costs_query(["i-123"], "ec2", 1)

    for query in (tag_query, trends_query, service_query, account_query):
        assert "line_item_blended_cost" in query
        assert_partition_pruned(query)
    assert "line_item_unblended_cost" in direct_query
    assert_partition_pruned(direct_query)


def test_native_cur2_uses_map_tags_billing_period_and_unblended_cost():
    builder = CURQueryBuilder("billing", "cur2", CUR2_PROFILE)
    available = {"resource_tags"}

    tag_query, _ = builder.build_costs_by_tag_query("Environment", 2, available)
    trends_query = builder.build_cost_trends_query(
        "Environment",
        "production",
        2,
        available,
    )
    service_query = builder.build_service_costs_query(2)
    account_query = builder.build_account_costs_query(2)
    direct_query = builder.build_direct_arn_costs_query(
        ["arn:aws:lambda:us-east-1:123456789012:function:example"],
        "lambda",
        2,
    )

    for query in (
        tag_query,
        trends_query,
        service_query,
        account_query,
        direct_query,
    ):
        assert "billing_period = '" in query
        assert "line_item_unblended_cost" in query
        assert "year = '" not in query
    assert "element_at(resource_tags, 'Environment')" in tag_query
    assert "element_at(resource_tags, 'user_Environment')" in tag_query
    for query in (service_query, account_query, direct_query):
        assert "element_at(product, 'servicename')" in query
        assert "product_servicename" not in query
    assert "GROUP BY billing_period" in trends_query


def test_query_builder_rejects_untrusted_service_name_expression():
    profile = dict(CUR1_PROFILE)
    profile["service_name_expression"] = "product_servicename; DROP TABLE cur"

    with pytest.raises(ValueError, match="service_name_expression"):
        CURQueryBuilder("billing", "cur1", profile)


def test_transformed_cur2_uses_flattened_tags_with_cur2_cost_and_partition():
    builder = CURQueryBuilder("billing", "cur2_flat", TRANSFORMED_PROFILE)
    query, _ = builder.build_costs_by_tag_query(
        "Environment",
        1,
        {"resource_tags_user_environment"},
    )

    assert "resource_tags_user_environment" in query
    assert "line_item_unblended_cost" in query
    assert "billing_period = '" in query


def test_calendar_month_arithmetic_is_safe_at_month_end():
    builder = CURQueryBuilder("billing", "cur2", CUR2_PROFILE)

    filters, info = builder._generate_partition_filters(
        3,
        current_date=datetime(2026, 8, 31),
    )

    assert filters == [
        "(billing_period = '2026-08')",
        "(billing_period = '2026-07')",
        "(billing_period = '2026-06')",
    ]
    assert info["months_covered"] == ["2026-06", "2026-07", "2026-08"]


def test_cur2_tag_key_enumeration_is_bounded_and_partition_pruned():
    builder = CURQueryBuilder("billing", "cur2", CUR2_PROFILE)

    query = builder.build_available_tag_keys_query(months_back=1, limit=1000)

    assert "map_keys(resource_tags)" in query
    assert "billing_period = '" in query
    assert "LIMIT 1000" in query
    with pytest.raises(ValueError, match="one billing month"):
        builder.build_available_tag_keys_query(months_back=2)
    with pytest.raises(ValueError, match="between 1 and 1000"):
        builder.build_available_tag_keys_query(limit=1001)


@pytest.mark.parametrize("profile", [CUR1_PROFILE, CUR2_PROFILE])
def test_connectivity_rollups_direct_and_templates_are_partition_pruned(profile):
    builder = CURQueryBuilder("billing", "cur", profile)
    template_args = (
        {
            "monthly_costs": {"start_billing_period": "2026-07"},
            "top_resources": {"billing_period": "2026-08", "limit": 10},
            "service_summary": {"billing_period": "2026-08"},
        }
        if profile["partition_mode"] == "billing_period"
        else {
            "monthly_costs": {"start_year": "2026"},
            "top_resources": {"year": "2026", "month": "8", "limit": 10},
            "service_summary": {"year": "2026", "month": "8"},
        }
    )
    queries = [
        builder.build_connectivity_query(),
        builder.build_service_costs_query(1),
        builder.build_account_costs_query(1),
        builder.build_direct_arn_costs_query(["i-123"], "ec2", 1),
    ]
    queries.extend(
        builder.get_query_template(name, **kwargs)
        for name, kwargs in template_args.items()
    )

    for query in queries:
        assert query
        assert_partition_pruned(query)


def test_cur2_sql_injection_inputs_are_rejected_without_breaking_resource_ids():
    builder = CURQueryBuilder("billing", "cur1", CUR1_PROFILE)
    available = {"resource_tags_user_environment"}

    assert builder.build_cost_trends_query(
        "Environment",
        "prod'; DROP TABLE cur; --",
        1,
        available,
    ) is None
    assert builder.build_direct_arn_costs_query(
        ["i-123", "x'); DROP TABLE cur; --"],
        "ec2",
        1,
    ).count("DROP TABLE") == 0
    with pytest.raises(ValueError, match="Invalid service_type"):
        builder.build_direct_arn_costs_query(["i-123"], "EC2; DROP", 1)
    with pytest.raises(ValueError, match="Invalid tag_key"):
        builder.build_costs_by_tag_query(
            "Environment'); DROP TABLE cur; --",
            1,
            available,
        )


def test_zero_month_queries_are_rejected_before_losing_partition_pruning():
    builder = CURQueryBuilder("billing", "cur1", CUR1_PROFILE)

    with pytest.raises(ValueError, match="months_back"):
        builder.build_service_costs_query(0)


def test_cur2_cost_estimation_recognizes_billing_period_pruning():
    builder = CURQueryBuilder("billing", "cur2", CUR2_PROFILE)
    query, _ = builder.build_costs_by_tag_query(
        "Environment",
        1,
        {"resource_tags"},
    )

    estimate = builder.estimate_query_cost(query)

    assert estimate["partition_filters_detected"] > 0
    assert builder._assess_optimization_level(query) == "excellent"
    assert not any(
        recommendation.startswith("Add year/month")
        for recommendation in builder._get_cost_optimization_recommendations(query)
    )

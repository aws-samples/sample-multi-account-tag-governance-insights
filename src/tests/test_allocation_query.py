"""Tests for the set-based tag cost allocation query (cost/allocation_query.py)."""

import os
import re
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lambda_handlers.cost.allocation_query import build_allocation_select
from lambda_handlers.cost.schema_manager import CURSchemaManager
from tests.test_cur_schema_compatibility import (
    CUR1_TABLE,
    NATIVE_CUR2_TABLE,
    TRANSFORMED_CUR2_TABLE,
)

ADB = "ti_us-east-1_dev_123456789012"
CUR1 = CURSchemaManager.build_schema_profile(CUR1_TABLE)
CUR2 = CURSchemaManager.build_schema_profile(NATIVE_CUR2_TABLE)


def build(profile=CUR1, adb=ADB, db="cur_db", table="cur_table", year=2026, month=11):
    return build_allocation_select(adb, db, table, profile, year, month)


def test_cur1_expressions():
    sql = build(CUR1)
    assert "product_servicename AS service_name, product_region AS product_region" in sql
    assert "ELSE line_item_unblended_cost END) AS cost" in sql
    assert "FROM cur_db.cur_table" in sql
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("CUR_MONTH_FORMAT", None)
        assert "(year = '2026' AND month = '3')" in build(CUR1, month=3)


def test_cur2_native_expressions():
    sql = build(CUR2)
    assert "element_at(product, 'servicename') AS service_name" in sql
    assert "element_at(product, 'region') AS product_region" in sql
    assert "(billing_period = '2026-11')" in sql
    assert "year = " not in sql


def test_transformed_cur2_uses_flat_region():
    sql = build(CURSchemaManager.build_schema_profile(TRANSFORMED_CUR2_TABLE), month=4)
    assert "product_region AS product_region" in sql
    assert "(billing_period = '2026-04')" in sql


def test_cost_column_fallbacks():
    p = dict(CUR1, direct_cost_column=None, cost_column="line_item_blended_cost")
    assert "ELSE line_item_blended_cost END" in build(p)
    p = dict(CUR1, direct_cost_column=None, cost_column=None)
    assert "ELSE line_item_unblended_cost END" in build(p)
    with pytest.raises(ValueError):
        build(dict(CUR1, direct_cost_column="x; DROP"))
    with pytest.raises(ValueError):
        build(dict(CUR1, reservation_effective_cost_column="x; DROP"))
    with pytest.raises(ValueError):
        build(dict(CUR1, region_expression="1=1"))


def _with_columns(table, *names):
    cols = table["StorageDescriptor"]["Columns"] + [{"Name": n, "Type": "double"} for n in names]
    return {**table, "StorageDescriptor": {"Columns": cols}}


EFFECTIVE = ("savings_plan_savings_plan_effective_cost", "reservation_effective_cost")


@pytest.mark.parametrize("table", [CUR1_TABLE, NATIVE_CUR2_TABLE, TRANSFORMED_CUR2_TABLE],
                         ids=["cur1", "cur2-native", "cur2-transformed"])
def test_commitment_usage_uses_effective_cost(table):
    sql = build(CURSchemaManager.build_schema_profile(_with_columns(table, *EFFECTIVE)))
    assert "line_item_line_item_type IN ('Usage', 'SavingsPlanCoveredUsage', 'DiscountedUsage')" in sql
    assert "WHEN 'SavingsPlanCoveredUsage' THEN savings_plan_savings_plan_effective_cost" in sql
    assert "WHEN 'DiscountedUsage' THEN reservation_effective_cost" in sql
    assert "ELSE line_item_unblended_cost END) AS cost" in sql


@pytest.mark.parametrize("table", [CUR1_TABLE, NATIVE_CUR2_TABLE], ids=["cur1", "cur2"])
def test_missing_effective_cost_columns_fall_back_to_unblended(table):
    sql = build(CURSchemaManager.build_schema_profile(_with_columns(table, "reservation_effective_cost")))
    assert "WHEN 'SavingsPlanCoveredUsage' THEN line_item_unblended_cost" in sql
    assert "WHEN 'DiscountedUsage' THEN reservation_effective_cost" in sql


@pytest.mark.parametrize("table", [CUR1_TABLE, NATIVE_CUR2_TABLE], ids=["cur1", "cur2"])
def test_region_code_preferred_when_present(table):
    sql = build(CURSchemaManager.build_schema_profile(_with_columns(table, "product_region_code")))
    assert "product_region_code AS product_region" in sql


def test_cur_month_format_zero_padded():
    with patch.dict(os.environ, {"CUR_MONTH_FORMAT": "leading_zero"}):
        assert "(year = '2026' AND month = '03')" in build(CUR1, month=3)


def test_analytics_db_quoted():
    sql = build()
    assert f'FROM "{ADB}".resource_tag_deltas_recent' in sql
    assert f'FROM "{ADB}".resource_tag_deltas_historical' in sql


@pytest.mark.parametrize("kwargs", [
    {"adb": 'db"; DROP TABLE x; --'},
    {"adb": ""},
    {"adb": "a" * 256},
    {"db": "bad-db"},
    {"table": "t; DROP TABLE x"},
    {"table": 'db"; DROP TABLE x; --'},
    {"year": 1999},
    {"year": 2101},
    {"year": "2026"},
    {"month": 0},
    {"month": 13},
    {"month": "11"},
    {"month": True},
    {"profile": dict(CUR1, service_name_expression="1=1; --")},
])
def test_invalid_inputs_raise(kwargs):
    with pytest.raises(ValueError):
        build(**kwargs)


def test_output_columns_and_interval_logic():
    sql = build()
    final = sql[sql.rindex("\nSELECT "):]
    assert re.search(
        r"SELECT account_id, arn AS resource_arn, resource_type, region, usage_date,\s+"
        r"t\.tag_key, t\.tag_value, tags_assumed, SUM\(cost\) AS cost", final)
    assert "GROUP BY 1, 2, 3, 4, 5, 6, 7, 8" in final
    assert "THEN TIMESTAMP '1970-01-01 00:00:00' ELSE changed_at END AS valid_from" in sql
    assert "(i.assumed_before_first_seen AND c.usage_start < i.first_seen) AS tags_assumed" in sql
    assert "change_type <> 'resource_removed'" in sql
    assert "line_item_line_item_type IN ('Usage', 'SavingsPlanCoveredUsage', 'DiscountedUsage') " \
           "AND line_item_resource_id <> ''" in sql
    assert "ARRAY[CAST(NULL AS varchar)], map_keys(tags)" in sql
    assert not sql.rstrip().endswith(";")


def test_bare_id_guard_and_unguarded_arn_branch():
    sql = build()
    assert "ARRAY[l.arn, regexp_extract(l.arn, '[^/:]+$')]" in sql
    assert re.search(
        r"AND \(i\.is_arn OR \(\s+c\.usage_account_id = i\.account_id\s+"
        r"AND \(c\.product_region IS NULL OR c\.product_region = '' "
        r"OR c\.product_region = i\.region\)\s+AND c\.service_name LIKE '%' \|\| "
        r"COALESCE\(i\.service_pattern, i\.resource_type\) \|\| '%'\)\)", sql)


def test_service_patterns_rendered():
    from lambda_handlers.cost.query_builder import SERVICE_NAME_PATTERNS
    sql = build()
    block = sql[sql.index("patterns(resource_type, service_pattern) AS (VALUES"):sql.index("id_forms AS")]
    assert block.count("('") == len(SERVICE_NAME_PATTERNS)
    assert "('ec2', 'Amazon Elastic Compute Cloud')" in block


def test_cross_account_catalog_prefixes_cur_table_only():
    sql = build_allocation_select(ADB, "cur_db", "cur_table", CUR1, 2026, 11, cur_catalog="ti_cur_dev")
    assert 'FROM "ti_cur_dev".cur_db.cur_table' in sql
    assert f'FROM "{ADB}".resource_tag_deltas_recent' in sql
    assert '"ti_cur_dev"' not in sql.split("cur AS (")[0]


@pytest.mark.parametrize("catalog", ["", 'x"; DROP', "a.b", "x" * 128, 5])
def test_invalid_catalog_rejected(catalog):
    with pytest.raises(ValueError):
        build_allocation_select(ADB, "cur_db", "cur_table", CUR1, 2026, 11, cur_catalog=catalog)

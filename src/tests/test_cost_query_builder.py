"""Tests for the CUR table reference used by the cost allocator (cost/query_builder.py)."""

import os
import sys
from datetime import datetime
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lambda_handlers.cost.query_builder import CURQueryBuilder, _escape_sql_string
from lambda_handlers.cost.schema_manager import CURSchemaManager
from tests.test_cur_schema_compatibility import CUR1_TABLE, NATIVE_CUR2_TABLE, make_table


@pytest.mark.parametrize("value, expected", [
    ("plain", "plain"), ("O'Brien", "O''Brien"), ("''", "''''"), ("", ""),
])
def test_escape_sql_string(value, expected):
    assert _escape_sql_string(value) == expected


@pytest.mark.parametrize("database, table", [
    ("cur db", "t"), ("db", "t;DROP"), ("db", 'x"y'), ("1db", "t"), ("db", ""),
])
def test_identifiers_validated(database, table):
    with pytest.raises(ValueError):
        CURQueryBuilder(database, table)


def test_untrusted_service_name_expression_rejected():
    with pytest.raises(ValueError):
        CURQueryBuilder("db", "t", {"service_name_expression": "1; DROP TABLE x"})


def test_year_month_filters_and_month_format():
    builder = CURQueryBuilder("cur_db", "cur_table")
    assert builder.full_table_name == "cur_db.cur_table"
    with patch.dict(os.environ, {"CUR_MONTH_FORMAT": "no_leading_zero"}):
        filters, info = builder._generate_partition_filters(3, current_date=datetime(2026, 1, 31))
    assert filters == ["(year = '2026' AND month = '1')", "(year = '2025' AND month = '12')",
                       "(year = '2025' AND month = '11')"]
    assert info["months_covered"] == ["2025-11", "2025-12", "2026-1"]
    with patch.dict(os.environ, {"CUR_MONTH_FORMAT": "leading_zero"}):
        assert builder._generate_partition_filters(1, current_date=datetime(2026, 3, 1))[0] == [
            "(year = '2026' AND month = '03')"]


@pytest.mark.parametrize("month_format", ["no_leading_zero", "leading_zero"])
def test_int_partitioned_cur_gets_bare_int_filters(month_format):
    # Athena rejects year = '2026' on an int partition column (type mismatch).
    columns = [(c["Name"], c["Type"]) for c in CUR1_TABLE["StorageDescriptor"]["Columns"]]
    table = make_table(columns, [("year", "int"), ("month", "bigint")])
    builder = CURQueryBuilder("db", "t", CURSchemaManager.build_schema_profile(table))
    with patch.dict(os.environ, {"CUR_MONTH_FORMAT": month_format}):
        assert builder._generate_partition_filters(1, current_date=datetime(2026, 3, 1))[0] == [
            "(year = 2026 AND month = 3)"]


def test_string_partitioned_cur_keeps_quoted_filters():
    builder = CURQueryBuilder("db", "t", CURSchemaManager.build_schema_profile(CUR1_TABLE))
    with patch.dict(os.environ, {"CUR_MONTH_FORMAT": "no_leading_zero"}):
        assert builder._generate_partition_filters(1, current_date=datetime(2026, 3, 1))[0] == [
            "(year = '2026' AND month = '3')"]


def test_cur2_billing_period_filter():
    builder = CURQueryBuilder("db", "t", CURSchemaManager.build_schema_profile(NATIVE_CUR2_TABLE))
    assert builder._generate_partition_filters(1, current_date=datetime(2026, 11, 1))[0] == [
        "(billing_period = '2026-11')"]


@pytest.mark.parametrize("months", [0, 121, "1"])
def test_months_back_bounds(months):
    with pytest.raises(ValueError):
        CURQueryBuilder("db", "t")._generate_partition_filters(months)

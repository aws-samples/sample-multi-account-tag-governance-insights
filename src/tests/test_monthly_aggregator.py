"""Tests for Monthly Aggregator Lambda Handler."""

import os
import sys
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class _GlueEntityNotFoundException(Exception):
    pass


class _GlueAlreadyExistsException(Exception):
    pass


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.setenv("CENTRAL_ANALYTICS_BUCKET", "test-analytics-bucket")
    monkeypatch.setenv("ATHENA_RESULTS_BUCKET", "test-athena-results")
    monkeypatch.setenv("ATHENA_DATABASE", "test_db")
    monkeypatch.setenv("ATHENA_WORKGROUP", "test-workgroup")
    monkeypatch.setenv("RETENTION_DAYS", "90")
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.fixture
def mod():
    """Import module with mocked AWS clients; yield (module, s3, athena, glue)."""
    mock_s3, mock_athena, mock_glue = MagicMock(), MagicMock(), MagicMock()
    mock_glue.exceptions.EntityNotFoundException = _GlueEntityNotFoundException
    mock_glue.exceptions.AlreadyExistsException = _GlueAlreadyExistsException

    def factory(service, **kw):
        return {"s3": mock_s3, "athena": mock_athena, "glue": mock_glue}.get(
            service, MagicMock()
        )

    with patch("boto3.client", side_effect=factory):
        sys.modules.pop("lambda_handlers.monthly_aggregator_handler", None)
        import lambda_handlers.monthly_aggregator_handler as m

        m.s3_client, m.athena_client, m.glue_client = mock_s3, mock_athena, mock_glue
        yield m, mock_s3, mock_athena, mock_glue


def _athena_result(columns, rows):
    """Build a paginator return value for Athena results."""
    col_info = [{"Name": c} for c in columns]
    header = {"Data": [{"VarCharValue": c} for c in columns]}
    data_rows = [{"Data": [{"VarCharValue": v} for v in row]} for row in rows]
    return [
        {
            "ResultSet": {
                "ResultSetMetadata": {"ColumnInfo": col_info},
                "Rows": [header] + data_rows,
            }
        }
    ]


# --- lambda_handler ---


def test_lambda_handler_success(mod):
    m, s3, athena, glue = mod
    s3.list_objects_v2.return_value = {
        "CommonPrefixes": [
            {"Prefix": "tag-deltas/recent/account_id=123456789012/"},
            {"Prefix": "tag-deltas/recent/account_id=987654321098/"},
        ]
    }
    athena.start_query_execution.return_value = {"QueryExecutionId": "q1"}
    athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    pag = MagicMock()
    pag.paginate.return_value = _athena_result(
        ["arn", "change_type"], [["arn:aws:ec2:us-east-1:123:i/i-1", "ADDED"]]
    )
    athena.get_paginator.return_value = pag
    glue.delete_partition.side_effect = _GlueEntityNotFoundException("nf")
    m.HAS_PARQUET = False
    result = m.lambda_handler({}, None)
    assert result["accounts_processed"] == 2 and result["accounts_failed"] == 0


def test_lambda_handler_account_failure_continues(mod):
    m, s3, athena, glue = mod
    s3.list_objects_v2.return_value = {
        "CommonPrefixes": [
            {"Prefix": "tag-deltas/recent/account_id=111111111111/"},
            {"Prefix": "tag-deltas/recent/account_id=222222222222/"},
        ]
    }
    calls = [0]

    def start_q(**kw):
        calls[0] += 1
        if calls[0] <= 1:
            raise Exception("fail")
        return {"QueryExecutionId": "q2"}

    athena.start_query_execution.side_effect = start_q
    athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    pag = MagicMock()
    pag.paginate.return_value = _athena_result(["arn"], [["arn:aws:s3:::b"]])
    athena.get_paginator.return_value = pag
    glue.delete_partition.side_effect = _GlueEntityNotFoundException("nf")
    m.HAS_PARQUET = False
    result = m.lambda_handler({}, None)
    assert result["accounts_processed"] == 1 and result["accounts_failed"] == 1


def test_lambda_handler_fatal_error_raises(mod):
    m, s3, _, _ = mod
    s3.list_objects_v2.side_effect = Exception("Network error")
    with patch.object(
        m, "discover_accounts_via_athena", side_effect=Exception("fallback")
    ):
        with pytest.raises(Exception, match="fallback"):
            m.lambda_handler({}, None)


# --- discover_accounts ---


def test_discover_accounts_from_s3(mod):
    m, s3, _, _ = mod
    s3.list_objects_v2.return_value = {
        "CommonPrefixes": [
            {"Prefix": "tag-deltas/recent/account_id=123456789012/"},
            {"Prefix": "tag-deltas/recent/account_id=999888777666/"},
        ]
    }
    assert set(m.discover_accounts(2025, 1)) == {"123456789012", "999888777666"}


def test_discover_accounts_filters_invalid(mod):
    m, s3, _, _ = mod
    s3.list_objects_v2.return_value = {
        "CommonPrefixes": [
            {"Prefix": "tag-deltas/recent/account_id=123456789012/"},
            {"Prefix": "tag-deltas/recent/account_id=short/"},
            {"Prefix": "tag-deltas/recent/account_id=abcdefghijkl/"},
        ]
    }
    assert m.discover_accounts(2025, 3) == ["123456789012"]


def test_discover_accounts_athena_fallback(mod):
    m, s3, athena, _ = mod
    s3.list_objects_v2.side_effect = Exception("S3 error")
    athena.start_query_execution.return_value = {"QueryExecutionId": "qfb"}
    athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    pag = MagicMock()
    pag.paginate.return_value = _athena_result(["account_id"], [["111222333444"]])
    athena.get_paginator.return_value = pag
    assert m.discover_accounts(2025, 5) == ["111222333444"]


def test_discover_accounts_empty(mod):
    m, s3, _, _ = mod
    s3.list_objects_v2.return_value = {}
    assert m.discover_accounts(2025, 6) == []


# --- execute_athena_query ---


def test_execute_athena_query_success(mod):
    m, _, athena, _ = mod
    athena.start_query_execution.return_value = {"QueryExecutionId": "q"}
    athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    pag = MagicMock()
    pag.paginate.return_value = _athena_result(
        ["c1", "c2"], [["v1", "v2"], ["v3", "v4"]]
    )
    athena.get_paginator.return_value = pag
    results = m.execute_athena_query("SELECT 1")
    assert results == [{"c1": "v1", "c2": "v2"}, {"c1": "v3", "c2": "v4"}]


def test_execute_athena_query_failed(mod):
    m, _, athena, _ = mod
    athena.start_query_execution.return_value = {"QueryExecutionId": "q"}
    athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "FAILED", "StateChangeReason": "err"}}
    }
    with pytest.raises(Exception, match="Query failed"):
        m.execute_athena_query("BAD SQL")


def test_execute_athena_query_timeout(mod):
    m, _, athena, _ = mod
    athena.start_query_execution.return_value = {"QueryExecutionId": "q"}
    athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "RUNNING"}}
    }
    with patch("lambda_handlers.monthly_aggregator_handler.time.sleep"):
        with patch("lambda_handlers.monthly_aggregator_handler.time.time") as mt:
            mt.side_effect = [0, 0, 301, 301]
            with pytest.raises(Exception, match="Query timeout"):
                m.execute_athena_query("SELECT 1", timeout=300)


def test_execute_athena_query_cancelled(mod):
    m, _, athena, _ = mod
    athena.start_query_execution.return_value = {"QueryExecutionId": "q"}
    athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "CANCELLED", "StateChangeReason": "x"}}
    }
    with pytest.raises(Exception, match="Query failed"):
        m.execute_athena_query("SELECT 1")


# --- aggregate_account_month ---


def test_aggregate_account_invalid_id(mod):
    m, _, _, _ = mod
    assert m.aggregate_account_month("short", 2025, 1) == 0
    assert m.aggregate_account_month("abcdefghijkl", 2025, 1) == 0


def test_aggregate_account_no_data(mod):
    m, _, athena, _ = mod
    athena.start_query_execution.return_value = {"QueryExecutionId": "q"}
    athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    pag = MagicMock()
    pag.paginate.return_value = _athena_result(["arn"], [])
    athena.get_paginator.return_value = pag
    assert m.aggregate_account_month("123456789012", 2025, 6) == 0


def test_aggregate_account_with_data(mod):
    m, s3, athena, glue = mod
    m.HAS_PARQUET = False
    athena.start_query_execution.return_value = {"QueryExecutionId": "q"}
    athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    pag = MagicMock()
    pag.paginate.return_value = _athena_result(
        ["arn", "ct"], [["a1", "ADDED"], ["a2", "MOD"]]
    )
    athena.get_paginator.return_value = pag
    assert m.aggregate_account_month("123456789012", 2025, 3) == 2
    s3.put_object.assert_called_once()


# --- write_monthly_aggregate / write_monthly_json ---


def test_write_monthly_aggregate_empty(mod):
    m, s3, _, glue = mod
    m.write_monthly_aggregate([], "123456789012", 2025, 1)
    s3.put_object.assert_not_called()
    glue.create_partition.assert_not_called()


def test_write_monthly_json(mod):
    m, s3, _, _ = mod
    m.write_monthly_json([{"arn": "a1"}], "account_id=123456789012/year=2025/month=3/")
    kw = s3.put_object.call_args[1]
    assert kw["Bucket"] == "test-analytics-bucket"
    assert "historical" in kw["Key"] and "monthly-aggregate.json" in kw["Key"]


def test_write_monthly_json_error(mod):
    m, s3, _, _ = mod
    s3.put_object.side_effect = Exception("S3 write failed")
    with pytest.raises(Exception, match="S3 write failed"):
        m.write_monthly_json([{"a": 1}], "account_id=123/year=2025/month=1/")


# --- register_monthly_partition ---


def test_register_partition_success(mod):
    m, _, _, glue = mod
    m.register_monthly_partition("123456789012", 2025, 6)
    glue.create_partition.assert_called_once()
    assert (
        glue.create_partition.call_args[1]["TableName"]
        == "resource_tag_deltas_historical"
    )


def test_register_partition_already_exists(mod):
    m, _, _, glue = mod
    glue.create_partition.side_effect = _GlueAlreadyExistsException("exists")
    m.register_monthly_partition("123456789012", 2025, 6)  # no raise


def test_register_partition_other_error(mod):
    m, _, _, glue = mod
    glue.create_partition.side_effect = Exception("Glue unavailable")
    m.register_monthly_partition("123456789012", 2025, 6)  # logs warning, no raise


# --- delete_daily_partitions ---


def test_delete_daily_partitions_success(mod):
    m, s3, _, glue = mod

    def del_side(**kw):
        if int(kw["PartitionValues"][3]) <= 3:
            return {}
        raise _GlueEntityNotFoundException("nf")

    glue.delete_partition.side_effect = del_side
    pag = MagicMock()
    pag.paginate.return_value = [{"Contents": [{"Key": "k"}]}]
    s3.get_paginator.return_value = pag
    assert m.delete_daily_partitions("123456789012", 2025, 3) == 3


def test_delete_daily_partitions_all_missing(mod):
    m, _, _, glue = mod
    glue.delete_partition.side_effect = _GlueEntityNotFoundException("nf")
    assert m.delete_daily_partitions("123456789012", 2025, 1) == 0


def test_delete_daily_partitions_error_returns_zero(mod):
    m, _, _, glue = mod
    glue.delete_partition.side_effect = RuntimeError("unexpected")
    assert m.delete_daily_partitions("123456789012", 2025, 2) == 0


# --- delete_s3_prefix ---


def test_delete_s3_prefix_with_objects(mod):
    m, s3, _, _ = mod
    pag = MagicMock()
    pag.paginate.return_value = [{"Contents": [{"Key": "f1"}, {"Key": "f2"}]}]
    s3.get_paginator.return_value = pag
    m.delete_s3_prefix("prefix/")
    assert len(s3.delete_objects.call_args[1]["Delete"]["Objects"]) == 2


def test_delete_s3_prefix_empty(mod):
    m, s3, _, _ = mod
    pag = MagicMock()
    pag.paginate.return_value = [{"Contents": []}]
    s3.get_paginator.return_value = pag
    m.delete_s3_prefix("empty/")
    s3.delete_objects.assert_not_called()


def test_delete_s3_prefix_error(mod):
    m, s3, _, _ = mod
    pag = MagicMock()
    pag.paginate.side_effect = Exception("denied")
    s3.get_paginator.return_value = pag
    with pytest.raises(Exception, match="denied"):
        m.delete_s3_prefix("x/")


# --- get_query_results ---


def test_get_query_results_multiple_pages(mod):
    m, _, athena, _ = mod
    pag = MagicMock()
    pag.paginate.return_value = [
        {
            "ResultSet": {
                "ResultSetMetadata": {"ColumnInfo": [{"Name": "id"}]},
                "Rows": [
                    {"Data": [{"VarCharValue": "id"}]},
                    {"Data": [{"VarCharValue": "1"}]},
                ],
            }
        },
        {
            "ResultSet": {
                "ResultSetMetadata": {"ColumnInfo": [{"Name": "id"}]},
                "Rows": [
                    {"Data": [{"VarCharValue": "id"}]},
                    {"Data": [{"VarCharValue": "2"}]},
                ],
            }
        },
    ]
    athena.get_paginator.return_value = pag
    assert m.get_query_results("qid") == [{"id": "1"}, {"id": "2"}]

"""Tests for CUR Cost Provider (cost/cur_provider.py) - Athena query execution,
result processing, workgroup discovery, tag summary, and cost trend logic."""

import os
import sys
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

_mock_session = MagicMock()
_mock_athena = MagicMock()
_mock_session.client.return_value = _mock_athena

with patch(
    "lambda_handlers.cost.schema_manager._get_cur_session", return_value=_mock_session
):
    with patch("lambda_handlers.cost.schema_manager.CURSchemaManager"):
        with patch("lambda_handlers.cost.query_builder.CURQueryBuilder"):
            with patch(
                "boto3.client",
                return_value=MagicMock(
                    get_caller_identity=MagicMock(
                        return_value={"Account": "123456789012"}
                    )
                ),
            ):
                from lambda_handlers.cost.cur_provider import CURCostProvider

from lambda_handlers.cost.query_builder import CURQueryBuilder  # noqa: E402
from lambda_handlers.cost.schema_manager import CURSchemaManager  # noqa: E402
from tests.test_cur_schema_compatibility import (  # noqa: E402
    NATIVE_CUR2_TABLE,
    TRANSFORMED_CUR2_TABLE,
)


@pytest.fixture
def mock_athena_client():
    return MagicMock()


@pytest.fixture
def provider(mock_athena_client):
    """CURCostProvider with mocked dependencies."""
    with patch("lambda_handlers.cost.cur_provider._get_cur_session") as mock_sess:
        mock_sess.return_value.client.return_value = mock_athena_client
        with patch("lambda_handlers.cost.cur_provider.CURSchemaManager") as mock_sm:
            with patch("lambda_handlers.cost.cur_provider.CURQueryBuilder") as mock_qb:
                with patch.dict(
                    os.environ, {"ATHENA_RESULTS_BUCKET": "my-results-bucket"}
                ):
                    p = CURCostProvider("test_db", "test_table", "us-east-1")
                    p.athena_client = mock_athena_client
                    p.query_builder = mock_qb.return_value
                    p.schema_manager = mock_sm.return_value
                    p.schema_profile = {
                        "format": "cur1",
                        "tag_mode": "flattened",
                    }
                    p.available_tag_columns = set()
                    return p


# --- Initialization ---


class TestInitialization:
    def test_basic_properties(self, provider):
        assert provider.database == "test_db"
        assert provider.table == "test_table"
        assert provider.region == "us-east-1"
        assert provider.query_timeout == 300

    def test_results_bucket_from_env(self):
        with patch("lambda_handlers.cost.cur_provider._get_cur_session") as ms:
            ms.return_value.client.return_value = MagicMock()
            with patch("lambda_handlers.cost.cur_provider.CURSchemaManager"):
                with patch("lambda_handlers.cost.cur_provider.CURQueryBuilder"):
                    with patch.dict(
                        os.environ, {"ATHENA_RESULTS_BUCKET": "test-bucket"}
                    ):
                        p = CURCostProvider("db", "tbl")
                        assert p.results_bucket == "test-bucket"

    def test_results_bucket_cur_env_takes_priority(self):
        with patch("lambda_handlers.cost.cur_provider._get_cur_session") as ms:
            ms.return_value.client.return_value = MagicMock()
            with patch("lambda_handlers.cost.cur_provider.CURSchemaManager"):
                with patch("lambda_handlers.cost.cur_provider.CURQueryBuilder"):
                    with patch.dict(
                        os.environ,
                        {
                            "CUR_ATHENA_RESULTS_BUCKET": "cur-bucket",
                            "ATHENA_RESULTS_BUCKET": "other",
                        },
                    ):
                        p = CURCostProvider("db", "tbl")
                        assert p.results_bucket == "cur-bucket"

    def test_results_bucket_auto_derive(self):
        session = MagicMock()
        session.client.return_value.get_caller_identity.return_value = {
            "Account": "999"
        }
        with patch(
            "lambda_handlers.cost.cur_provider._get_cur_session",
            return_value=session,
        ), patch(
            "lambda_handlers.cost.cur_provider.CURSchemaManager"
        ), patch(
            "lambda_handlers.cost.cur_provider.CURQueryBuilder"
        ), patch.dict(
            os.environ,
            {},
            clear=True,
        ):
            p = CURCostProvider("db", "tbl", "us-west-2")
            assert "999" in p.results_bucket


# --- Workgroup Discovery ---


class TestDiscoverWorkgroup:
    def test_env_var_precedence(self, provider):
        with patch.dict(os.environ, {"ATHENA_WORKGROUP": "my-wg"}):
            assert provider._discover_workgroup() == "my-wg"

    def test_discovers_ti_workgroup(self, provider, mock_athena_client):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ATHENA_WORKGROUP", None)
            mock_athena_client.list_work_groups.return_value = {
                "WorkGroups": [
                    {"Name": "primary"},
                    {"Name": "ti-cost-analytics-us-east-1-dev-123456789012"},
                ]
            }
            with patch("boto3.client") as mc:
                mc.return_value.get_caller_identity.return_value = {
                    "Account": "123456789012"
                }
                assert provider._discover_workgroup().startswith("ti-cost-analytics")

    def test_fallback_to_primary(self, provider, mock_athena_client):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ATHENA_WORKGROUP", None)
            mock_athena_client.list_work_groups.return_value = {
                "WorkGroups": [{"Name": "primary"}]
            }
            with patch("boto3.client") as mc:
                mc.return_value.get_caller_identity.return_value = {"Account": "123"}
                assert provider._discover_workgroup() == "primary"

    def test_exception_fallback(self, provider, mock_athena_client):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ATHENA_WORKGROUP", None)
            mock_athena_client.list_work_groups.side_effect = Exception("err")
            assert provider._discover_workgroup() == "primary"


# --- Execute Athena Query ---


class TestExecuteAthenaQuery:
    def test_successful(self, provider, mock_athena_client):
        mock_athena_client.start_query_execution.return_value = {
            "QueryExecutionId": "qid-1"
        }
        mock_athena_client.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {
                    "DataScannedInBytes": 1024,
                    "EngineExecutionTimeInMillis": 500,
                },
            }
        }
        mock_athena_client.get_query_results.return_value = {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "col1"}]},
                    {"Data": [{"VarCharValue": "val1"}]},
                ],
                "ResultSetMetadata": {"ColumnInfo": [{"Name": "col1"}]},
            }
        }
        with patch.dict(os.environ, {"ATHENA_WORKGROUP": "primary"}):
            result = provider._execute_athena_query("SELECT 1", "test")
        assert result["success"] is True
        assert result["data"][0]["col1"] == "val1"

    def test_failed_query(self, provider, mock_athena_client):
        mock_athena_client.start_query_execution.return_value = {
            "QueryExecutionId": "qid-f"
        }
        mock_athena_client.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "FAILED", "StateChangeReason": "Syntax error"}
            }
        }
        with patch.dict(os.environ, {"ATHENA_WORKGROUP": "primary"}):
            result = provider._execute_athena_query("BAD", "fail")
        assert "error" in result
        assert "Syntax error" in result["error"]

    def test_exception(self, provider, mock_athena_client):
        mock_athena_client.start_query_execution.side_effect = Exception("Network")
        with patch.dict(os.environ, {"ATHENA_WORKGROUP": "primary"}):
            result = provider._execute_athena_query("SELECT 1", "err")
        assert result["success"] is False


# --- Wait for Query Completion ---


class TestWaitForQueryCompletion:
    def test_immediate_success(self, provider, mock_athena_client):
        mock_athena_client.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {
                    "DataScannedInBytes": 2048,
                    "EngineExecutionTimeInMillis": 1000,
                },
            }
        }
        result = provider._wait_for_query_completion("qid-1")
        assert result["success"] is True
        assert result["data_scanned_bytes"] == 2048

    def test_cancelled(self, provider, mock_athena_client):
        mock_athena_client.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "CANCELLED", "StateChangeReason": "User cancelled"}
            }
        }
        result = provider._wait_for_query_completion("qid-c")
        assert result["success"] is False

    def test_timeout(self, provider, mock_athena_client):
        provider.query_timeout = 1
        mock_athena_client.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "RUNNING"}}
        }
        with patch("time.sleep"):
            result = provider._wait_for_query_completion("qid-slow")
        assert result["success"] is False
        assert "timed out" in result["error"]

    def test_exception(self, provider, mock_athena_client):
        mock_athena_client.get_query_execution.side_effect = Exception("API fail")
        result = provider._wait_for_query_completion("qid-e")
        assert result["success"] is False

    def test_cost_estimation_1tb(self, provider, mock_athena_client):
        mock_athena_client.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {
                    "DataScannedInBytes": 1024**4,
                    "EngineExecutionTimeInMillis": 5000,
                },
            }
        }
        result = provider._wait_for_query_completion("qid-cost")
        assert result["estimated_cost_usd"] == pytest.approx(5.0, rel=0.01)


# --- Get Query Results ---


class TestGetQueryResults:
    def test_empty(self, provider, mock_athena_client):
        mock_athena_client.get_query_results.return_value = {
            "ResultSet": {"Rows": [], "ResultSetMetadata": {"ColumnInfo": []}}
        }
        assert provider._get_query_results("q")["data"] == []

    def test_header_only(self, provider, mock_athena_client):
        mock_athena_client.get_query_results.return_value = {
            "ResultSet": {
                "Rows": [{"Data": [{"VarCharValue": "c"}]}],
                "ResultSetMetadata": {"ColumnInfo": [{"Name": "c"}]},
            }
        }
        assert provider._get_query_results("q")["data"] == []

    def test_multi_row(self, provider, mock_athena_client):
        mock_athena_client.get_query_results.return_value = {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "tag"}, {"VarCharValue": "cost"}]},
                    {"Data": [{"VarCharValue": "prod"}, {"VarCharValue": "100"}]},
                    {"Data": [{"VarCharValue": "dev"}, {"VarCharValue": "25"}]},
                ],
                "ResultSetMetadata": {
                    "ColumnInfo": [{"Name": "tag"}, {"Name": "cost"}]
                },
            }
        }
        result = provider._get_query_results("q")
        assert result["row_count"] == 2
        assert result["data"][0]["tag"] == "prod"

    def test_pagination_flag(self, provider, mock_athena_client):
        mock_athena_client.get_query_results.return_value = {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "c"}]},
                    {"Data": [{"VarCharValue": "v"}]},
                ],
                "ResultSetMetadata": {"ColumnInfo": [{"Name": "c"}]},
            },
            "NextToken": "tok",
        }
        assert provider._get_query_results("q")["has_more_results"] is True

    def test_exception(self, provider, mock_athena_client):
        mock_athena_client.get_query_results.side_effect = Exception("denied")
        result = provider._get_query_results("q")
        assert "error" in result


# --- get_costs_by_tag ---


class TestGetCostsByTag:
    def test_tag_not_available(self, provider):
        provider.available_tag_columns = set()
        provider.query_builder.build_costs_by_tag_query.return_value = (None, [])
        result = provider.get_costs_by_tag("NonExistent")
        assert "error" in result

    def test_successful_breakdown(self, provider, mock_athena_client):
        provider.available_tag_columns = {
            "resource_tags_user_env"
        }
        provider.query_builder.build_costs_by_tag_query.return_value = (
            "SELECT ...",
            [],
        )
        provider.query_builder.estimate_query_cost.return_value = "$0.005"
        mock_athena_client.start_query_execution.return_value = {
            "QueryExecutionId": "qid"
        }
        mock_athena_client.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {
                    "DataScannedInBytes": 512,
                    "EngineExecutionTimeInMillis": 200,
                },
            }
        }
        mock_athena_client.get_query_results.return_value = {
            "ResultSet": {
                "Rows": [
                    {
                        "Data": [
                            {"VarCharValue": "tag_value"},
                            {"VarCharValue": "total_cost"},
                            {"VarCharValue": "resource_count"},
                        ]
                    },
                    {
                        "Data": [
                            {"VarCharValue": "prod"},
                            {"VarCharValue": "150.75"},
                            {"VarCharValue": "10"},
                        ]
                    },
                ],
                "ResultSetMetadata": {
                    "ColumnInfo": [
                        {"Name": "tag_value"},
                        {"Name": "total_cost"},
                        {"Name": "resource_count"},
                    ]
                },
            }
        }
        with patch.dict(os.environ, {"ATHENA_WORKGROUP": "primary"}):
            result = provider.get_costs_by_tag("Environment", months_back=1)
        assert result["tag_values_found"] == 1
        assert result["data_source"] == "cur_athena"
        assert "prod" in result["breakdown"]

    def test_query_exception(self, provider, mock_athena_client):
        provider.available_tag_columns = {
            "resource_tags_user_e"
        }
        provider.query_builder.build_costs_by_tag_query.return_value = (
            "SELECT ...",
            [],
        )
        mock_athena_client.start_query_execution.side_effect = Exception("down")
        with patch.dict(os.environ, {"ATHENA_WORKGROUP": "primary"}):
            result = provider.get_costs_by_tag("Environment")
        assert "error" in result


# --- get_cost_trends_for_tag_value ---


class TestGetCostTrends:
    def _setup_trend_query(self, provider, mock_athena_client, monthly_data):
        provider.available_tag_columns = {
            "resource_tags_user_e"
        }
        provider.query_builder.build_cost_trends_query.return_value = "SELECT ..."
        mock_athena_client.start_query_execution.return_value = {
            "QueryExecutionId": "qid"
        }
        mock_athena_client.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {
                    "DataScannedInBytes": 256,
                    "EngineExecutionTimeInMillis": 100,
                },
            }
        }
        rows = [
            {
                "Data": [
                    {"VarCharValue": "month_key"},
                    {"VarCharValue": "monthly_cost"},
                    {"VarCharValue": "resource_count"},
                ]
            }
        ]
        for month, cost, count in monthly_data:
            rows.append(
                {
                    "Data": [
                        {"VarCharValue": month},
                        {"VarCharValue": str(cost)},
                        {"VarCharValue": str(count)},
                    ]
                }
            )
        mock_athena_client.get_query_results.return_value = {
            "ResultSet": {
                "Rows": rows,
                "ResultSetMetadata": {
                    "ColumnInfo": [
                        {"Name": "month_key"},
                        {"Name": "monthly_cost"},
                        {"Name": "resource_count"},
                    ]
                },
            }
        }

    def test_tag_not_available(self, provider):
        provider.available_tag_columns = set()
        provider.query_builder.build_cost_trends_query.return_value = None
        assert "error" in provider.get_cost_trends_for_tag_value("X", "v")

    def test_increasing_trend(self, provider, mock_athena_client):
        self._setup_trend_query(
            provider,
            mock_athena_client,
            [("2024-01", 50, 5), ("2024-02", 75, 7), ("2024-03", 100, 8)],
        )
        with patch.dict(os.environ, {"ATHENA_WORKGROUP": "primary"}):
            result = provider.get_cost_trends_for_tag_value("Env", "prod", 3)
        assert result["trend"] == "increasing"
        assert result["months_with_data"] == 3

    def test_decreasing_trend(self, provider, mock_athena_client):
        self._setup_trend_query(
            provider, mock_athena_client, [("2024-01", 200, 10), ("2024-02", 100, 8)]
        )
        with patch.dict(os.environ, {"ATHENA_WORKGROUP": "primary"}):
            result = provider.get_cost_trends_for_tag_value("T", "p", 2)
        assert result["trend"] == "decreasing"

    def test_insufficient_data(self, provider, mock_athena_client):
        self._setup_trend_query(provider, mock_athena_client, [("2024-01", 50, 3)])
        with patch.dict(os.environ, {"ATHENA_WORKGROUP": "primary"}):
            result = provider.get_cost_trends_for_tag_value("T", "d", 6)
        assert result["trend"] == "insufficient_data"


# --- test_connectivity ---


class TestConnectivity:
    def test_schema_fails(self, provider):
        provider.schema_manager.validate_cur_table.return_value = False
        result = provider.test_connectivity()
        assert result["success"] is False

    def test_success(self, provider, mock_athena_client):
        provider.schema_manager.validate_cur_table.return_value = True
        mock_athena_client.start_query_execution.return_value = {
            "QueryExecutionId": "qid"
        }
        mock_athena_client.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {
                    "DataScannedInBytes": 100,
                    "EngineExecutionTimeInMillis": 50,
                },
            }
        }
        mock_athena_client.get_query_results.return_value = {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "record_count"}]},
                    {"Data": [{"VarCharValue": "1000"}]},
                ],
                "ResultSetMetadata": {"ColumnInfo": [{"Name": "record_count"}]},
            }
        }
        with patch.dict(os.environ, {"ATHENA_WORKGROUP": "primary"}):
            assert provider.test_connectivity()["success"] is True

    def test_exception(self, provider):
        provider.schema_manager.validate_cur_table.side_effect = Exception("No access")
        assert provider.test_connectivity()["success"] is False


# --- get_available_tags_summary ---


class TestGetAvailableTagsSummary:
    def test_returns_tag_list(self, provider):
        provider.available_tag_columns = {
            "resource_tags_user_environment",
            "resource_tags_user_team",
        }
        result = provider.get_available_tags_summary()
        assert result["available_tag_count"] == 2
        assert "environment" in result["available_tags"]

    def test_empty(self, provider):
        provider.available_tag_columns = set()
        assert provider.get_available_tags_summary()["available_tag_count"] == 0

    def test_non_tag_columns_excluded(self, provider):
        provider.available_tag_columns = {
            "resource_tags_user_env",
            "line_item_usage_amount",
        }
        result = provider.get_available_tags_summary()
        assert "env" in result["available_tags"]
        assert len(result["available_tags"]) == 1

    def test_reuses_initial_glue_definition(self, provider):
        provider.available_tag_columns = {"resource_tags_user_env"}

        result = provider.get_available_tags_summary()

        assert result["available_tags"] == ["env"]
        provider.schema_manager.get_available_tag_columns.assert_not_called()

    def test_sample_limited_to_10(self, provider):
        cols = {f"resource_tags_user_tag{i}" for i in range(20)}
        provider.available_tag_columns = cols
        result = provider.get_available_tags_summary()
        assert len(result["sample_tags"]) == 10
        assert result["available_tag_count"] == 20


def make_cur_session(table):
    session = MagicMock()
    glue = MagicMock()
    glue.get_table.return_value = {"Table": table}
    athena = MagicMock()
    athena.list_work_groups.return_value = {"WorkGroups": []}
    clients = {
        "glue": glue,
        "athena": athena,
        "s3": MagicMock(),
    }
    session.client.side_effect = lambda service_name, **_: clients[service_name]
    return session, clients


@pytest.mark.parametrize(
    ("table", "expected_tag_mode"),
    [
        (NATIVE_CUR2_TABLE, "map"),
        (TRANSFORMED_CUR2_TABLE, "flattened"),
    ],
)
def test_provider_fetches_one_glue_definition_and_owns_canonical_profile(
    table,
    expected_tag_mode,
):
    session, clients = make_cur_session(table)

    with patch(
        "lambda_handlers.cost.cur_provider._get_cur_session",
        return_value=session,
    ), patch(
        "lambda_handlers.cost.cur_provider.CURSchemaManager",
        CURSchemaManager,
    ), patch(
        "lambda_handlers.cost.cur_provider.CURQueryBuilder",
        CURQueryBuilder,
    ), patch.dict(
        os.environ,
        {"ATHENA_RESULTS_BUCKET": "results"},
        clear=False,
    ):
        cur2_provider = CURCostProvider("billing", "cur2")

    assert cur2_provider.schema_profile["format"] == "cur2"
    assert cur2_provider.schema_profile["tag_mode"] == expected_tag_mode
    assert (
        cur2_provider.query_builder.schema_profile
        is cur2_provider.schema_profile
    )
    clients["glue"].get_table.assert_called_once_with(
        DatabaseName="billing",
        Name="cur2",
    )
    assert not clients["glue"].get_databases.called
    assert not clients["glue"].get_tables.called


def test_provider_rejects_unsupported_configured_table_instead_of_defaulting_cur1():
    unsupported = {
        "StorageDescriptor": {"Columns": [{"Name": "not_cur", "Type": "string"}]},
        "PartitionKeys": [],
    }
    session, _ = make_cur_session(unsupported)

    with patch(
        "lambda_handlers.cost.cur_provider._get_cur_session",
        return_value=session,
    ), patch(
        "lambda_handlers.cost.cur_provider.CURSchemaManager",
        CURSchemaManager,
    ), patch(
        "lambda_handlers.cost.cur_provider.CURQueryBuilder",
        CURQueryBuilder,
    ), patch.dict(
        os.environ,
        {"ATHENA_RESULTS_BUCKET": "results"},
        clear=False,
    ):
        with pytest.raises(RuntimeError, match="Unsupported CUR schema"):
            CURCostProvider("billing", "invalid")


def test_provider_reuses_tag_columns_from_initial_glue_definition():
    profile = CURSchemaManager.build_schema_profile(NATIVE_CUR2_TABLE)
    cur2_provider = CURCostProvider.__new__(CURCostProvider)
    cur2_provider.database = "billing"
    cur2_provider.table = "cur2"
    cur2_provider.schema_profile = profile
    cur2_provider.available_tag_columns = {"resource_tags"}
    cur2_provider.query_builder = CURQueryBuilder("billing", "cur2", profile)
    cur2_provider.schema_manager = MagicMock()
    cur2_provider._execute_athena_query = MagicMock(
        return_value={
            "data": [
                {
                    "tag_value": "production",
                    "total_cost": "12.5",
                    "resource_count": "2",
                }
            ],
            "success": True,
        }
    )

    result = cur2_provider.get_costs_by_tag("Environment")

    assert result["breakdown"] == {"production": "$12.50"}
    cur2_provider.schema_manager.get_available_tag_columns.assert_not_called()


def test_map_tag_summary_is_one_month_bounded_deduplicated_and_cached():
    profile = CURSchemaManager.build_schema_profile(NATIVE_CUR2_TABLE)
    CURCostProvider._tag_key_summary_cache = {}
    cur2_provider = CURCostProvider.__new__(CURCostProvider)
    cur2_provider.database = "billing"
    cur2_provider.table = "cur2"
    cur2_provider.schema_profile = profile
    cur2_provider.query_builder = CURQueryBuilder("billing", "cur2", profile)
    cur2_provider.schema_manager = MagicMock()
    cur2_provider._execute_athena_query = MagicMock(
        return_value={
            "data": [
                {"tag_key": "user_Environment"},
                {"tag_key": "CostCenter"},
                {"tag_key": "Environment"},
            ],
            "success": True,
        }
    )

    summary = cur2_provider.get_available_tags_summary()
    cached = cur2_provider.get_available_tags_summary()

    assert summary["available_tags"] == ["CostCenter", "Environment"]
    assert cached == summary
    cur2_provider._execute_athena_query.assert_called_once()
    query = cur2_provider._execute_athena_query.call_args.args[0]
    assert "billing_period = '" in query
    assert "LIMIT 1000" in query


def test_provider_connectivity_uses_profile_aware_partition():
    profile = CURSchemaManager.build_schema_profile(NATIVE_CUR2_TABLE)
    cur2_provider = CURCostProvider.__new__(CURCostProvider)
    cur2_provider.database = "billing"
    cur2_provider.table = "cur2"
    cur2_provider.schema_profile = profile
    cur2_provider.query_builder = CURQueryBuilder("billing", "cur2", profile)
    cur2_provider.schema_manager = MagicMock()
    cur2_provider.schema_manager.validate_cur_table.return_value = True
    cur2_provider._execute_athena_query = MagicMock(return_value={"success": True})

    assert cur2_provider.test_connectivity()["success"] is True
    query = cur2_provider._execute_athena_query.call_args.args[0]
    assert "billing_period = '" in query
    assert "year = '" not in query

"""Tests for CUR Schema Manager - schema discovery, validation, cross-account access."""

import os
import sys
from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture
def mock_env(monkeypatch):
    """Clear cross-account env vars by default."""
    monkeypatch.delenv("CUR_ACCOUNT_ROLE_ARN", raising=False)
    monkeypatch.delenv("CUR_EXTERNAL_ID", raising=False)


@pytest.fixture
def mock_glue():
    return MagicMock()


@pytest.fixture
def mock_athena():
    return MagicMock()


def _make_table_info(column_names, partition_keys=None, table_name="cur_table"):
    """Build a Glue get_table response-like dict."""
    columns = [{"Name": name, "Type": "string"} for name in column_names]
    if partition_keys is None:
        partition_keys = ["year", "month"]
    pkeys = [{"Name": k, "Type": "string"} for k in partition_keys]
    return {
        "Table": {
            "Name": table_name,
            "StorageDescriptor": {
                "Columns": columns,
                "Location": "s3://bucket/cur/",
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            },
            "PartitionKeys": pkeys,
            "TableType": "EXTERNAL_TABLE",
            "CreateTime": datetime(2024, 1, 1),
            "LastAccessTime": datetime(2024, 6, 1),
        }
    }


# All minimum columns required
MINIMUM_COLS = [
    "line_item_blended_cost",
    "line_item_resource_id",
    "line_item_usage_start_date",
    "product_servicename",
    "bill_payer_account_id",
]

# Additional CUR columns for realistic tests
EXTRA_CUR_COLS = [
    "line_item_usage_account_id",
    "line_item_line_item_type",
    "product_product_name",
    "product_region",
    "bill_bill_type",
    "bill_billing_period_start_date",
]


def _build_schema_manager():
    """Create a CURSchemaManager with mocked AWS clients."""
    with patch("lambda_handlers.cost.schema_manager._get_cur_session") as mock_session:
        session_obj = MagicMock()
        mock_session.return_value = session_obj
        session_obj.client.return_value = MagicMock()
        from lambda_handlers.cost.schema_manager import CURSchemaManager

        mgr = CURSchemaManager(region_name="us-east-1")
    return mgr


class TestGetCurSession:
    """Tests for cross-account session creation."""

    def test_no_role_arn_returns_default_session(self, mock_env):
        """Without CUR_ACCOUNT_ROLE_ARN, returns a plain boto3.Session."""
        with patch("lambda_handlers.cost.schema_manager.boto3.Session") as mock_sess:
            mock_sess.return_value = MagicMock()
            from lambda_handlers.cost.schema_manager import _get_cur_session

            session = _get_cur_session()
            mock_sess.assert_called_once_with()

    def test_with_role_arn_assumes_role(self, monkeypatch):
        """With CUR_ACCOUNT_ROLE_ARN and CUR_EXTERNAL_ID, assumes role via STS."""
        monkeypatch.setenv(
            "CUR_ACCOUNT_ROLE_ARN", "arn:aws:iam::111122223333:role/test"
        )
        monkeypatch.setenv("CUR_EXTERNAL_ID", "ext-id-123")

        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIA...",
                "SecretAccessKey": "secret",
                "SessionToken": "token",
            }
        }

        with patch(
            "lambda_handlers.cost.schema_manager.boto3.client", return_value=mock_sts
        ):
            with patch(
                "lambda_handlers.cost.schema_manager.boto3.Session"
            ) as mock_sess:
                from lambda_handlers.cost.schema_manager import _get_cur_session

                _get_cur_session()

                mock_sts.assume_role.assert_called_once()
                call_kwargs = mock_sts.assume_role.call_args[1]
                assert call_kwargs["ExternalId"] == "ext-id-123"
                assert call_kwargs["RoleArn"] == "arn:aws:iam::111122223333:role/test"

    def test_missing_external_id_fails_closed(self, monkeypatch):
        """A configured cross-account role requires an ExternalId."""
        monkeypatch.setenv(
            "CUR_ACCOUNT_ROLE_ARN", "arn:aws:iam::111122223333:role/test"
        )
        monkeypatch.delenv("CUR_EXTERNAL_ID", raising=False)

        with patch(
            "lambda_handlers.cost.schema_manager.boto3.client"
        ):
            from lambda_handlers.cost.schema_manager import _get_cur_session

            with pytest.raises(RuntimeError, match="CUR_EXTERNAL_ID"):
                _get_cur_session()


class TestIsCurTable:
    """Tests for CUR table validation logic."""

    def test_valid_cur_table_lenient(self):
        """Table with minimum columns passes lenient validation."""
        mgr = _build_schema_manager()
        cols = MINIMUM_COLS + EXTRA_CUR_COLS
        table_info = _make_table_info(cols)["Table"]
        assert mgr._is_cur_table("mydb", "cur_table", table_info) is True

    def test_missing_minimum_columns_fails(self):
        """Table missing minimum columns fails lenient validation."""
        mgr = _build_schema_manager()
        cols = ["line_item_blended_cost", "product_servicename"]  # missing others
        table_info = _make_table_info(cols)["Table"]
        assert mgr._is_cur_table("mydb", "cur_table", table_info) is False

    def test_status_table_rejected(self):
        """Tables with status/metadata names are rejected."""
        mgr = _build_schema_manager()
        cols = MINIMUM_COLS + EXTRA_CUR_COLS
        table_info = _make_table_info(cols, table_name="cur_status")["Table"]
        assert mgr._is_cur_table("mydb", "cur_status", table_info) is False

    def test_temp_table_rejected(self):
        """Tables with temp in name are rejected."""
        mgr = _build_schema_manager()
        cols = MINIMUM_COLS + EXTRA_CUR_COLS
        table_info = _make_table_info(cols, table_name="temp_data")["Table"]
        assert mgr._is_cur_table("mydb", "temp_data", table_info) is False

    def test_strict_validation_requires_more_columns(self):
        """Strict validation requires comprehensive CUR schema."""
        mgr = _build_schema_manager()
        # Only minimum columns - not enough for strict
        cols = MINIMUM_COLS + EXTRA_CUR_COLS
        table_info = _make_table_info(cols)["Table"]
        # Strict will fail because <70% of REQUIRED_CUR_COLUMNS present
        result = mgr._is_cur_table(
            "mydb", "cur_table", table_info, strict_validation=True
        )
        assert result is False

    def test_tag_columns_are_optional(self):
        """Table without tag columns still passes validation."""
        mgr = _build_schema_manager()
        cols = MINIMUM_COLS + EXTRA_CUR_COLS
        table_info = _make_table_info(cols)["Table"]
        assert mgr._is_cur_table("mydb", "my_cur", table_info) is True

    def test_table_with_tag_columns(self):
        """Table with resource_tags columns still passes."""
        mgr = _build_schema_manager()
        cols = (
            MINIMUM_COLS
            + EXTRA_CUR_COLS
            + [
                "resource_tags_user_environment",
                "resource_tags_user_team",
            ]
        )
        table_info = _make_table_info(cols)["Table"]
        assert mgr._is_cur_table("mydb", "cur_table", table_info) is True

    def test_exception_returns_false(self):
        """If table_info is None and glue call fails, returns False."""
        mgr = _build_schema_manager()
        mgr.glue_client.get_table.side_effect = Exception("Glue unavailable")
        assert mgr._is_cur_table("mydb", "cur_table", table_info=None) is False


class TestValidateCurTable:
    """Tests for validate_cur_table with caching."""

    def test_valid_table_returns_true(self):
        """Valid CUR table returns True."""
        mgr = _build_schema_manager()
        cols = MINIMUM_COLS + EXTRA_CUR_COLS
        mgr.glue_client.get_table.return_value = _make_table_info(cols)
        assert mgr.validate_cur_table("mydb", "cur_table") is True

    def test_caches_result(self):
        """Second call uses cache, no extra API call."""
        mgr = _build_schema_manager()
        cols = MINIMUM_COLS + EXTRA_CUR_COLS
        mgr.glue_client.get_table.return_value = _make_table_info(cols)

        mgr.validate_cur_table("mydb", "cur_table")
        mgr.validate_cur_table("mydb", "cur_table")
        # Should only call glue once
        assert mgr.glue_client.get_table.call_count == 1

    def test_entity_not_found_returns_false(self):
        """EntityNotFoundException returns False."""
        from botocore.exceptions import ClientError

        mgr = _build_schema_manager()
        mgr.glue_client.get_table.side_effect = ClientError(
            {"Error": {"Code": "EntityNotFoundException", "Message": "Not found"}},
            "GetTable",
        )
        assert mgr.validate_cur_table("mydb", "missing_table") is False


class TestGetAvailableTagColumns:
    """Tests for tag column discovery."""

    def test_returns_tag_columns_only(self):
        """Only returns columns prefixed with resource_tags_user_."""
        mgr = _build_schema_manager()
        cols = MINIMUM_COLS + ["resource_tags_user_env", "resource_tags_user_team"]
        mgr.glue_client.get_table.return_value = _make_table_info(cols)

        result = mgr.get_available_tag_columns("mydb", "cur_table")
        assert result == {"resource_tags_user_env", "resource_tags_user_team"}

    def test_empty_when_no_tag_columns(self):
        """Returns empty set when no tag columns exist."""
        mgr = _build_schema_manager()
        mgr.glue_client.get_table.return_value = _make_table_info(MINIMUM_COLS)

        result = mgr.get_available_tag_columns("mydb", "cur_table")
        assert result == set()

    def test_caches_results(self):
        """Subsequent calls use cache."""
        mgr = _build_schema_manager()
        cols = MINIMUM_COLS + ["resource_tags_user_env"]
        mgr.glue_client.get_table.return_value = _make_table_info(cols)

        mgr.get_available_tag_columns("mydb", "cur_table")
        mgr.get_available_tag_columns("mydb", "cur_table")
        assert mgr.glue_client.get_table.call_count == 1

    def test_error_returns_empty_set(self):
        """Returns empty set on error."""
        mgr = _build_schema_manager()
        mgr.glue_client.get_table.side_effect = Exception("API error")
        result = mgr.get_available_tag_columns("mydb", "cur_table")
        assert result == set()


class TestTagColumnExists:
    """Tests for tag_column_exists."""

    def test_existing_tag_returns_true(self):
        """Returns True when the tag column exists."""
        mgr = _build_schema_manager()
        cols = MINIMUM_COLS + ["resource_tags_user_environment"]
        mgr.glue_client.get_table.return_value = _make_table_info(cols)

        assert mgr.tag_column_exists("mydb", "cur_table", "Environment") is True

    def test_missing_tag_returns_false(self):
        """Returns False when the tag column does not exist."""
        mgr = _build_schema_manager()
        mgr.glue_client.get_table.return_value = _make_table_info(MINIMUM_COLS)

        assert mgr.tag_column_exists("mydb", "cur_table", "Team") is False


class TestGetPartitionInfo:
    """Tests for partition info retrieval."""

    def test_returns_partition_keys(self):
        """Returns partition keys from table metadata."""
        mgr = _build_schema_manager()
        mgr.glue_client.get_table.return_value = _make_table_info(
            MINIMUM_COLS, partition_keys=["year", "month"]
        )
        mgr.glue_client.get_partitions.return_value = {
            "Partitions": [
                {"Values": ["2024", "01"]},
                {"Values": ["2024", "02"]},
            ]
        }
        result = mgr.get_partition_info("mydb", "cur_table")
        assert "year" in result["partition_keys"]
        assert "month" in result["partition_keys"]

    def test_unpartitioned_table(self):
        """Returns message for unpartitioned tables."""
        mgr = _build_schema_manager()
        mgr.glue_client.get_table.return_value = _make_table_info(
            MINIMUM_COLS, partition_keys=[]
        )
        result = mgr.get_partition_info("mydb", "cur_table")
        assert result["partitions"] == []

    def test_error_returns_error_dict(self):
        """Returns error dict on failure."""
        mgr = _build_schema_manager()
        mgr.glue_client.get_table.side_effect = Exception("Glue error")
        result = mgr.get_partition_info("mydb", "cur_table")
        assert "error" in result


class TestClearCache:
    """Tests for cache clearing."""

    def test_clears_both_caches(self):
        """clear_cache empties both schema and validation caches."""
        mgr = _build_schema_manager()
        mgr._schema_cache["key"] = {"data": True}
        mgr._table_validation_cache["key"] = {"valid": True}

        mgr.clear_cache()

        assert mgr._schema_cache == {}
        assert mgr._table_validation_cache == {}

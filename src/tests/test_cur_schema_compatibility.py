import os
from unittest.mock import Mock, patch

import pytest

from lambda_handlers.cost.schema_manager import CURSchemaManager, _get_cur_session


def make_table(columns, partitions):
    return {
        "StorageDescriptor": {
            "Columns": [
                {"Name": name, "Type": column_type}
                for name, column_type in columns
            ]
        },
        "PartitionKeys": [
            {"Name": name, "Type": partition_type}
            for name, partition_type in partitions
        ],
    }


RUNTIME_COLUMNS = [
    ("line_item_resource_id", "string"),
    ("line_item_usage_start_date", "timestamp"),
    ("line_item_usage_account_id", "string"),
    ("line_item_line_item_type", "string"),
]

CUR1_TABLE = make_table(
    RUNTIME_COLUMNS
    + [
        ("product_servicename", "string"),
        ("line_item_blended_cost", "double"),
        ("line_item_unblended_cost", "double"),
        ("resource_tags_user_environment", "string"),
    ],
    [("year", "string"), ("month", "string")],
)

NATIVE_CUR2_TABLE = make_table(
    RUNTIME_COLUMNS
    + [
        ("product", "map<string,string>"),
        ("line_item_unblended_cost", "double"),
        ("resource_tags", "map<string,string>"),
    ],
    [("billing_period", "string")],
)

TRANSFORMED_CUR2_TABLE = make_table(
    RUNTIME_COLUMNS
    + [
        ("product_servicename", "string"),
        ("line_item_unblended_cost", "double"),
        ("resource_tags_user_environment", "string"),
    ],
    [("billing_period", "string")],
)


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        (
            CUR1_TABLE,
            {
                "format": "cur1",
                "tag_mode": "flattened",
                "partition_mode": "year_month",
                "cost_column": "line_item_blended_cost",
                "direct_cost_column": "line_item_unblended_cost",
                "service_name_expression": "product_servicename",
            },
        ),
        (
            NATIVE_CUR2_TABLE,
            {
                "format": "cur2",
                "tag_mode": "map",
                "partition_mode": "billing_period",
                "cost_column": "line_item_unblended_cost",
                "direct_cost_column": "line_item_unblended_cost",
                "service_name_expression": (
                    "element_at(product, 'servicename')"
                ),
            },
        ),
        (
            TRANSFORMED_CUR2_TABLE,
            {
                "format": "cur2",
                "tag_mode": "flattened",
                "partition_mode": "billing_period",
                "cost_column": "line_item_unblended_cost",
                "direct_cost_column": "line_item_unblended_cost",
                "service_name_expression": "product_servicename",
            },
        ),
    ],
)
def test_schema_profile_detects_supported_cur_shapes(table, expected):
    profile = CURSchemaManager.build_schema_profile(table)

    assert profile["supported"] is True
    for key, value in expected.items():
        assert profile[key] == value


@pytest.mark.parametrize("missing_column", [name for name, _ in RUNTIME_COLUMNS])
def test_schema_profile_requires_every_runtime_column(missing_column):
    table = make_table(
        [column for column in RUNTIME_COLUMNS if column[0] != missing_column]
        + [
            ("line_item_unblended_cost", "double"),
            ("resource_tags", "map<string,string>"),
        ],
        [("billing_period", "string")],
    )

    assert CURSchemaManager.build_schema_profile(table)["supported"] is False


@pytest.mark.parametrize(
    "service_columns",
    [
        [],
        [("product", "string")],
    ],
)
def test_schema_profile_requires_supported_service_name_source(service_columns):
    table = make_table(
        RUNTIME_COLUMNS
        + service_columns
        + [
            ("line_item_unblended_cost", "double"),
            ("resource_tags", "map<string,string>"),
        ],
        [("billing_period", "string")],
    )

    profile = CURSchemaManager.build_schema_profile(table)

    assert profile["supported"] is False
    assert profile["service_name_expression"] is None


def test_validation_and_tag_discovery_use_schema_profile():
    manager = CURSchemaManager.__new__(CURSchemaManager)
    manager.glue_client = Mock()
    manager.glue_client.get_table.return_value = {"Table": NATIVE_CUR2_TABLE}
    manager._schema_cache = {}
    manager._table_validation_cache = {}
    manager._cache_ttl = 3600

    assert manager.validate_cur_table("billing", "cur2") is True
    profile = CURSchemaManager.build_schema_profile(NATIVE_CUR2_TABLE)
    assert manager.get_available_tag_columns(
        "billing",
        "cur2",
        schema_profile=profile,
    ) == {"resource_tags"}
    assert manager.tag_column_exists(
        "billing",
        "cur2",
        "Environment",
        schema_profile=profile,
    )


def test_transformed_cur2_keeps_flattened_tag_discovery():
    manager = CURSchemaManager.__new__(CURSchemaManager)
    manager.glue_client = Mock()
    manager.glue_client.get_table.return_value = {"Table": TRANSFORMED_CUR2_TABLE}
    manager._schema_cache = {}
    manager._table_validation_cache = {}
    manager._cache_ttl = 3600
    profile = CURSchemaManager.build_schema_profile(TRANSFORMED_CUR2_TABLE)

    assert manager.get_available_tag_columns(
        "billing",
        "cur2_transformed",
        schema_profile=profile,
    ) == {"resource_tags_user_environment"}


@patch("lambda_handlers.cost.schema_manager.boto3.client")
def test_configured_cross_account_role_uses_external_id_and_fails_closed(
    mock_boto_client,
):
    mock_boto_client.return_value.assume_role.side_effect = RuntimeError(
        "access denied"
    )

    with patch.dict(
        os.environ,
        {
            "CUR_ACCOUNT_ROLE_ARN": "arn:aws:iam::111122223333:role/cur-reader",
            "CUR_EXTERNAL_ID": "taginsights-cur",
        },
        clear=True,
    ):
        with pytest.raises(RuntimeError, match="Failed to assume configured CUR role"):
            _get_cur_session()

    assert (
        mock_boto_client.return_value.assume_role.call_args.kwargs["ExternalId"]
        == "taginsights-cur"
    )


def test_configured_cross_account_role_requires_external_id():
    with patch.dict(
        os.environ,
        {"CUR_ACCOUNT_ROLE_ARN": "arn:aws:iam::111122223333:role/cur-reader"},
        clear=True,
    ):
        with pytest.raises(RuntimeError, match="CUR_EXTERNAL_ID"):
            _get_cur_session()


def test_schema_manager_uses_injected_shared_session():
    session = Mock()
    session.client.side_effect = lambda service, **_: Mock(name=service)

    manager = CURSchemaManager("us-east-1", session=session)

    assert manager.session is session
    assert session.client.call_count == 3

import os

import pytest

from lambda_handlers.cost.schema_manager import CURSchemaManager


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


@pytest.mark.parametrize("table", [CUR1_TABLE, NATIVE_CUR2_TABLE, TRANSFORMED_CUR2_TABLE])
def test_schema_profile_reports_effective_cost_and_region_columns(table):
    base = CURSchemaManager.build_schema_profile(table)
    assert base["savings_plan_effective_cost_column"] is None
    assert base["reservation_effective_cost_column"] is None
    assert base["region_expression"] in ("product_region", "element_at(product, 'region')")

    extra = [("savings_plan_savings_plan_effective_cost", "double"), ("reservation_effective_cost", "double"),
             ("product_region_code", "string")]
    columns = [(c["Name"], c["Type"]) for c in table["StorageDescriptor"]["Columns"]] + extra
    partitions = [(p["Name"], p["Type"]) for p in table["PartitionKeys"]]
    profile = CURSchemaManager.build_schema_profile(make_table(columns, partitions))
    assert profile["savings_plan_effective_cost_column"] == "savings_plan_savings_plan_effective_cost"
    assert profile["reservation_effective_cost_column"] == "reservation_effective_cost"
    assert profile["region_expression"] == "product_region_code"

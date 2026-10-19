"""Tests for Cost Deduplicator Lambda Handler."""

import json
import os
import sys
from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class _GlueEntityNotFound(Exception):
    pass


class _GlueAlreadyExists(Exception):
    pass


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("ATHENA_DATABASE", "ti_test_db")
    monkeypatch.setenv("ATHENA_WORKGROUP", "test-workgroup")
    monkeypatch.setenv("S3_BUCKET", "test-cost-bucket")
    monkeypatch.setenv("ATHENA_OUTPUT_BUCKET", "test-athena-output")


def _build():
    """Create CostDeduplicator with mocked deps; return (dedup, s3, glue, cw, athena)."""
    athena_exec, s3, glue, cw = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    glue.exceptions.EntityNotFoundException = _GlueEntityNotFound
    glue.exceptions.AlreadyExistsException = _GlueAlreadyExists

    def factory(svc, **kw):
        return {"s3": s3, "glue": glue, "cloudwatch": cw}.get(svc, MagicMock())

    with patch(
        "lambda_handlers.cost.cost_deduplicator_handler.boto3.client",
        side_effect=factory,
    ):
        with patch(
            "lambda_handlers.cost.cost_deduplicator_handler.AthenaQueryExecutor",
            return_value=athena_exec,
        ):
            sys.modules.pop("lambda_handlers.cost.cost_deduplicator_handler", None)
            from lambda_handlers.cost.cost_deduplicator_handler import CostDeduplicator

            d = CostDeduplicator()
    d.s3, d.glue, d.cloudwatch, d.athena = s3, glue, cw, athena_exec
    return d, s3, glue, cw, athena_exec


# --- lambda_handler ---


def test_handler_success():
    d, s3, *_ = _build()
    pag = MagicMock()
    pag.paginate.return_value = [{"CommonPrefixes": []}]
    s3.get_paginator.return_value = pag
    with patch(
        "lambda_handlers.cost.cost_deduplicator_handler.CostDeduplicator",
        return_value=d,
    ):
        with patch("lambda_handlers.cost.cost_deduplicator_handler.boto3.client"):
            from lambda_handlers.cost.cost_deduplicator_handler import lambda_handler

            r = lambda_handler({}, None)
    assert r["statusCode"] == 200
    assert json.loads(r["body"])["accounts_processed"] == 0


def test_handler_with_process_date():
    d, s3, *_ = _build()
    pag = MagicMock()
    pag.paginate.return_value = [{"CommonPrefixes": []}]
    s3.get_paginator.return_value = pag
    with patch(
        "lambda_handlers.cost.cost_deduplicator_handler.CostDeduplicator",
        return_value=d,
    ):
        with patch("lambda_handlers.cost.cost_deduplicator_handler.boto3.client"):
            from lambda_handlers.cost.cost_deduplicator_handler import lambda_handler

            assert (
                lambda_handler({"process_date": "2025-06-15"}, None)["statusCode"]
                == 200
            )


def test_handler_invalid_date():
    d, s3, *_ = _build()
    pag = MagicMock()
    pag.paginate.return_value = [{"CommonPrefixes": []}]
    s3.get_paginator.return_value = pag
    with patch(
        "lambda_handlers.cost.cost_deduplicator_handler.CostDeduplicator",
        return_value=d,
    ):
        with patch("lambda_handlers.cost.cost_deduplicator_handler.boto3.client"):
            from lambda_handlers.cost.cost_deduplicator_handler import lambda_handler

            assert lambda_handler({"process_date": "bad"}, None)["statusCode"] == 200


def test_handler_error_500():
    with patch(
        "lambda_handlers.cost.cost_deduplicator_handler.CostDeduplicator",
        side_effect=Exception("Init failed"),
    ):
        from lambda_handlers.cost.cost_deduplicator_handler import lambda_handler

        r = lambda_handler({}, None)
    assert r["statusCode"] == 500
    assert "Init failed" in json.loads(r["body"])["error"]


# --- deduplicate_daily_partitions ---


def test_dedup_no_accounts():
    d, s3, *_ = _build()
    pag = MagicMock()
    pag.paginate.return_value = [{"CommonPrefixes": []}]
    s3.get_paginator.return_value = pag
    r = d.deduplicate_daily_partitions(datetime(2025, 6, 15))
    assert r["status"] == "success" and r["accounts_processed"] == 0


def test_dedup_success():
    d, s3, glue, cw, athena = _build()
    pag = MagicMock()
    pag.paginate.return_value = [
        {
            "CommonPrefixes": [
                {"Prefix": "tag-costs-daily/account_id=111111111111/"},
                {"Prefix": "tag-costs-daily/account_id=222222222222/"},
            ]
        }
    ]
    s3.get_paginator.return_value = pag
    s3.list_objects_v2.return_value = {"KeyCount": 1, "Contents": [{"Key": "f"}]}
    athena.execute_query.return_value = [{"row_count": "50"}]
    glue.get_table.return_value = {
        "Table": {
            "StorageDescriptor": {
                "Location": "s3://test-cost-bucket/out/",
                "Columns": [{"Name": "x", "Type": "string"}],
                "InputFormat": "pi",
                "OutputFormat": "po",
                "SerdeInfo": {"SerializationLibrary": "ps"},
            }
        }
    }
    glue.get_partition.side_effect = _GlueEntityNotFound("nf")
    r = d.deduplicate_daily_partitions(datetime(2025, 6, 15))
    assert r["status"] == "success" and r["total_rows_deduplicated"] == 100


def test_dedup_partial_failure():
    d, s3, glue, _, athena = _build()
    pag = MagicMock()
    pag.paginate.return_value = [
        {
            "CommonPrefixes": [
                {"Prefix": "tag-costs-daily/account_id=111111111111/"},
                {"Prefix": "tag-costs-daily/account_id=222222222222/"},
            ]
        }
    ]
    s3.get_paginator.return_value = pag
    s3.list_objects_v2.return_value = {"KeyCount": 1, "Contents": [{"Key": "f"}]}
    n = [0]

    def q_side(query):
        n[0] += 1
        if n[0] == 1:
            raise Exception("fail")
        return [{"row_count": "25"}]

    athena.execute_query.side_effect = q_side
    glue.get_table.return_value = {
        "Table": {
            "StorageDescriptor": {
                "Location": "s3://test-cost-bucket/o/",
                "Columns": [],
                "InputFormat": "x",
                "OutputFormat": "x",
                "SerdeInfo": {"SerializationLibrary": "x"},
            }
        }
    }
    glue.get_partition.side_effect = _GlueEntityNotFound("nf")
    r = d.deduplicate_daily_partitions(datetime(2025, 6, 15))
    assert r["status"] == "partial_success" and r["accounts_failed"] == 1


# --- _find_accounts_in_daily_partition ---


def test_find_accounts_with_data():
    d, s3, *_ = _build()
    pag = MagicMock()
    pag.paginate.return_value = [
        {
            "CommonPrefixes": [
                {"Prefix": "tag-costs-daily/account_id=111111111111/"},
                {"Prefix": "tag-costs-daily/account_id=222222222222/"},
                {"Prefix": "tag-costs-daily/account_id=333333333333/"},
            ]
        }
    ]
    s3.get_paginator.return_value = pag

    def lo(**kw):
        return (
            {"KeyCount": 1}
            if "333333333333" not in kw.get("Prefix", "")
            else {"KeyCount": 0}
        )

    s3.list_objects_v2.side_effect = lo
    assert sorted(d._find_accounts_in_daily_partition(2025, 6, 15)) == [
        "111111111111",
        "222222222222",
    ]


def test_find_accounts_filters_invalid():
    d, s3, *_ = _build()
    pag = MagicMock()
    pag.paginate.return_value = [
        {
            "CommonPrefixes": [
                {"Prefix": "tag-costs-daily/account_id=111111111111/"},
                {"Prefix": "tag-costs-daily/account_id=short/"},
            ]
        }
    ]
    s3.get_paginator.return_value = pag
    s3.list_objects_v2.return_value = {"KeyCount": 1}
    assert d._find_accounts_in_daily_partition(2025, 6, 15) == ["111111111111"]


def test_find_accounts_error():
    d, s3, *_ = _build()
    pag = MagicMock()
    pag.paginate.side_effect = Exception("denied")
    s3.get_paginator.return_value = pag
    assert d._find_accounts_in_daily_partition(2025, 6, 15) == []


# --- _copy_s3_files ---


def test_copy_files():
    d, s3, *_ = _build()
    s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "out/part-00000"},
            {"Key": "out/part-00001"},
            {"Key": "out/_metadata"},
            {"Key": "out/f.crc"},
        ]
    }
    assert d._copy_s3_files("s3://test-cost-bucket/out/", "dest/") == 2
    assert s3.copy_object.call_count == 2


def test_copy_invalid_source():
    d, *_ = _build()
    assert d._copy_s3_files("/local/", "dest/") == 0


def test_copy_empty():
    d, s3, *_ = _build()
    s3.list_objects_v2.return_value = {"Contents": []}
    assert d._copy_s3_files("s3://b/p/", "dest/") == 0


# --- _add_monthly_partition ---


def test_add_partition_new():
    d, _, glue, *_ = _build()
    glue.get_partition.side_effect = _GlueEntityNotFound("nf")
    glue.get_table.return_value = {
        "Table": {
            "StorageDescriptor": {
                "Columns": [{"Name": "x", "Type": "s"}],
                "Location": "s3://b/",
                "InputFormat": "i",
                "OutputFormat": "o",
                "SerdeInfo": {"SerializationLibrary": "s"},
            }
        }
    }
    d._add_monthly_partition("123456789012", 2025, 6)
    glue.create_partition.assert_called_once()
    assert glue.create_partition.call_args[1]["PartitionInput"]["Values"] == [
        "123456789012",
        "2025",
        "6",
    ]


def test_add_partition_exists():
    d, _, glue, *_ = _build()
    glue.get_partition.return_value = {"Partition": {}}
    d._add_monthly_partition("123456789012", 2025, 6)
    glue.create_partition.assert_not_called()


def test_add_partition_error():
    d, _, glue, *_ = _build()
    glue.get_partition.side_effect = _GlueEntityNotFound("nf")
    glue.get_table.return_value = {
        "Table": {
            "StorageDescriptor": {
                "Columns": [],
                "Location": "s3://b/",
                "InputFormat": "x",
                "OutputFormat": "x",
                "SerdeInfo": {"SerializationLibrary": "x"},
            }
        }
    }
    glue.create_partition.side_effect = Exception("Glue error")
    with pytest.raises(Exception, match="Glue error"):
        d._add_monthly_partition("123456789012", 2025, 6)


# --- _cleanup_daily_partition ---


def test_cleanup_deletes_all():
    d, s3, *_ = _build()
    pag = MagicMock()
    pag.paginate.return_value = [{"Contents": [{"Key": "k1"}, {"Key": "k2"}]}]
    s3.get_paginator.return_value = pag
    stats = d._cleanup_daily_partition(2025, 6, 15, ["111111111111", "222222222222"])
    assert stats["files_deleted"] == 4 and s3.delete_objects.call_count == 2


def test_cleanup_error_continues():
    d, s3, *_ = _build()
    pag = MagicMock()
    n = [0]

    def pag_side(**kw):
        n[0] += 1
        if n[0] == 1:
            raise Exception("err")
        return iter([{"Contents": [{"Key": "k"}]}])

    pag.paginate.side_effect = pag_side
    s3.get_paginator.return_value = pag
    assert (
        d._cleanup_daily_partition(2025, 6, 15, ["111111111111", "222222222222"])[
            "files_deleted"
        ]
        == 1
    )


# --- _publish_metric ---


def test_publish_metric():
    d, _, _, cw, _ = _build()
    d._publish_metric("TestMetric", 42)
    kw = cw.put_metric_data.call_args[1]
    assert kw["Namespace"] == "TagInsights/CostAnalysis"
    assert kw["MetricData"][0]["Value"] == 42


def test_publish_metric_error():
    d, _, _, cw, _ = _build()
    cw.put_metric_data.side_effect = Exception("throttled")
    d._publish_metric("X", 1)  # no raise


# --- _deduplicate_account_month ---


def test_dedup_account_success():
    d, s3, glue, _, athena = _build()
    athena.execute_query.return_value = [{"row_count": "100"}]
    glue.get_table.return_value = {
        "Table": {"StorageDescriptor": {"Location": "s3://test-cost-bucket/out/"}}
    }
    s3.list_objects_v2.return_value = {"Contents": [{"Key": "out/part-00000"}]}
    assert d._deduplicate_account_month("123456789012", 2025, 6, 15) == 100
    assert athena.execute_query.call_count == 5


def test_dedup_account_error_cleanup():
    d, _, _, _, athena = _build()
    n = [0]

    def q(query):
        n[0] += 1
        if n[0] == 2:
            raise Exception("CTAS failed")
        return []

    athena.execute_query.side_effect = q
    with pytest.raises(Exception, match="CTAS failed"):
        d._deduplicate_account_month("123456789012", 2025, 6, 15)
    drops = [c for c in athena.execute_query.call_args_list if "DROP TABLE" in str(c)]
    assert len(drops) == 2

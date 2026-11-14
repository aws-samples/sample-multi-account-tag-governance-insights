"""Tests for the set-based Cost Allocator Lambda handler."""

import os
import re
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lambda_handlers.cost import cost_allocator_handler as handler  # noqa: E402

MODULE = "lambda_handlers.cost.cost_allocator_handler"
BUCKET = "amzn-s3-demo-analytics-bucket"
SUPPORTED_PROFILE = {"supported": True, "format": "cur1"}


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("CENTRAL_ANALYTICS_BUCKET", BUCKET)
    monkeypatch.setenv("ATHENA_DATABASE", "ti_us-east-1_test_123456789012")
    monkeypatch.setenv("ATHENA_WORKGROUP", "test-workgroup")
    monkeypatch.setenv("ATHENA_RESULTS_BUCKET", "amzn-s3-demo-athena-results")
    monkeypatch.setenv("CUR_ATHENA_DATABASE", "cur_db")
    monkeypatch.setenv("CUR_ATHENA_TABLE", "cur_table")
    monkeypatch.delenv("CUR_DATA_CATALOG", raising=False)
    monkeypatch.delenv("CUR_CATALOG_ID", raising=False)
    monkeypatch.setattr(handler, "POLL_INTERVAL_SECONDS", 0)


@pytest.fixture
def aws():
    """Patch boto3, the Athena executor, schema profile and SELECT builder; record call order."""
    calls = []
    s3 = MagicMock()

    def pages(Bucket, Prefix):
        if Prefix.startswith("tag-cost-allocation-staging/"):
            return [{"Contents": [{"Key": f"{Prefix}part-0.parquet"}, {"Key": f"{Prefix}part-1.parquet"}]}]
        return [{"Contents": [{"Key": f"{Prefix}old{i}"} for i in range(1500)]},
                {"Contents": [{"Key": f"{Prefix}old-last"}]}]

    s3.get_paginator.return_value.paginate.side_effect = pages
    s3.delete_objects.side_effect = lambda **kw: calls.append(
        ("delete", kw["Delete"]["Objects"][0]["Key"].split("/")[0], len(kw["Delete"]["Objects"]))) or {}
    s3.copy_object.side_effect = lambda **kw: calls.append(("copy", kw["CopySource"]["Key"], kw["Key"]))

    athena = MagicMock()
    athena.execute_query_async.side_effect = lambda sql: calls.append(("unload", sql)) or f"qid-{len(calls)}"
    athena.get_query_status.return_value = {"state": "SUCCEEDED"}

    with patch(f"{MODULE}.boto3.client", return_value=s3), \
            patch(f"{MODULE}.AthenaQueryExecutor", return_value=athena), \
            patch.object(handler.CostAllocator, "schema_profile", return_value=SUPPORTED_PROFILE), \
            patch(f"{MODULE}.build_allocation_select",
                  side_effect=lambda adb, db, tbl, prof, y, m, cur_catalog=None: f"SELECT {y} AS y, {m} AS m") as build:
        yield {"s3": s3, "athena": athena, "calls": calls, "build": build}


def test_default_months_are_previous_and_current():
    assert handler.months_to_compute({}, datetime(2026, 11, 11, tzinfo=timezone.utc)) == [(2026, 10), (2026, 11)]


def test_default_months_january_rolls_back_to_december():
    assert handler.months_to_compute({}, datetime(2026, 1, 3, tzinfo=timezone.utc)) == [(2025, 12), (2026, 1)]


def test_handler_default_runs_two_months(aws):
    result = handler.lambda_handler({}, None)

    assert result["status"] == "success"
    assert len(result["months"]) == 2
    assert all(m["query_execution_id"].startswith("qid-") for m in result["months"])


def test_explicit_year_month(aws):
    result = handler.lambda_handler({"year": 2025, "month": 3}, None)

    assert [(m["year"], m["month"]) for m in result["months"]] == [(2025, 3)]
    aws["build"].assert_called_once_with(
        "ti_us-east-1_test_123456789012", "cur_db", "cur_table", SUPPORTED_PROFILE, 2025, 3, cur_catalog=None
    )


def test_live_prefix_replaced_only_after_unload(aws):
    result = handler.lambda_handler({"year": 2025, "month": 3}, None)

    kinds = [c[0] for c in aws["calls"]]
    # Copy the new objects in first, then drop the old ones: the month is never empty.
    assert kinds == ["unload", "copy", "copy", "delete", "delete", "delete"]
    assert [c[1:] for c in aws["calls"][3:5]] == [("tag-cost-allocation", 1000), ("tag-cost-allocation", 501)]
    copies = [c for c in aws["calls"] if c[0] == "copy"]
    assert all(src.startswith("tag-cost-allocation-staging/year=2025/month=3/") for _, src, _ in copies)
    run_id = copies[0][1].split("/")[3]
    assert re.fullmatch(r"[0-9a-f]{32}", run_id)
    assert [dst for _, _, dst in copies] == [f"tag-cost-allocation/year=2025/month=3/{run_id}_part-0.parquet",
                                             f"tag-cost-allocation/year=2025/month=3/{run_id}_part-1.parquet"]
    assert aws["calls"][-1][1:] == ("tag-cost-allocation-staging", 2)
    assert result["months"][0]["objects_deleted"] == 1501
    assert result["months"][0]["objects_published"] == 2


def test_failed_unload_leaves_live_prefix_untouched(aws):
    aws["athena"].get_query_status.return_value = {"state": "FAILED", "error": "boom"}

    with pytest.raises(RuntimeError):
        handler.lambda_handler({"year": 2025, "month": 3}, None)
    assert not any(c[0] == "delete" and c[1] == "tag-cost-allocation" for c in aws["calls"])
    aws["s3"].copy_object.assert_not_called()


def test_status_error_cancels_query(aws):
    aws["athena"].get_query_status.side_effect = ConnectionError("throttled")

    with pytest.raises(ConnectionError):
        handler.lambda_handler({"year": 2025, "month": 3}, None)
    aws["athena"].cancel_query.assert_called_once()


def test_unload_targets_run_specific_staging_prefix_as_parquet(aws):
    handler.lambda_handler({"year": 2025, "month": 3}, None)

    sql = next(c[1] for c in aws["calls"] if c[0] == "unload")
    assert sql.startswith("UNLOAD (SELECT 2025 AS y, 3 AS m)")
    assert re.search(rf"TO 's3://{BUCKET}/tag-cost-allocation-staging/year=2025/month=3/[0-9a-f]{{32}}/'", sql)
    assert "format = 'PARQUET'" in sql
    assert "compression = 'SNAPPY'" in sql
    assert ";" not in sql


@pytest.mark.parametrize("state", ["FAILED", "CANCELLED"])
def test_failed_query_raises_with_reason(aws, state):
    aws["athena"].get_query_status.return_value = {"state": state, "error": "HIVE_BAD_DATA"}

    with pytest.raises(RuntimeError, match=f"{state}: HIVE_BAD_DATA" if state == "FAILED" else state):
        handler.lambda_handler({"year": 2025, "month": 3}, None)


def test_poll_timeout_cancels_query(aws):
    aws["athena"].get_query_status.return_value = {"state": "RUNNING"}
    context = MagicMock()
    context.get_remaining_time_in_millis.return_value = 125_000  # 5 s poll budget

    with patch(f"{MODULE}.time.monotonic", side_effect=[0, 0, 100]):
        with pytest.raises(TimeoutError):
            handler.lambda_handler({"year": 2025, "month": 3}, context)
    aws["athena"].cancel_query.assert_called_once()


@pytest.mark.parametrize("event", [
    {"year": 2025, "month": 13},
    {"year": 2025, "month": 0},
    {"year": 2025, "month": "3"},
    {"year": 2025, "month": True},
    {"year": 1999, "month": 3},
    {"year": 2025},
    {"month": "3/../../x"},
])
def test_invalid_year_month_rejected(aws, event):
    with pytest.raises(ValueError):
        handler.lambda_handler(event, None)
    assert aws["calls"] == []


def test_cross_account_catalog_passed_to_query(aws, monkeypatch):
    monkeypatch.setenv("CUR_DATA_CATALOG", "ti_cur_dev")
    monkeypatch.setenv("CUR_CATALOG_ID", "123456789012")

    result = handler.lambda_handler({"year": 2025, "month": 3}, None)

    assert result["status"] == "success"
    assert aws["build"].call_args.kwargs == {"cur_catalog": "ti_cur_dev"}


def test_catalog_name_without_id_rejected(aws, monkeypatch):
    monkeypatch.setenv("CUR_DATA_CATALOG", "ti_cur_dev")
    with pytest.raises(ValueError, match="set together"):
        handler.lambda_handler({"year": 2025, "month": 3}, None)


def test_missing_cur_config_skips(aws, monkeypatch):
    monkeypatch.setenv("CUR_ATHENA_TABLE", "")

    assert handler.lambda_handler({}, None)["status"] == "skipped"
    assert aws["calls"] == []


def test_delete_errors_raise(aws):
    aws["s3"].delete_objects.side_effect = lambda **kw: {"Errors": [{"Key": "k0", "Code": "AccessDenied"}]}

    with pytest.raises(RuntimeError, match="Failed to delete"):
        handler.lambda_handler({"year": 2025, "month": 3}, None)


def test_failed_copy_keeps_live_month_and_rolls_back(aws):
    # A copy failure mid-publish must leave the old month intact (no live deletes)
    # and remove the new objects already copied, so the month is never empty or mixed.
    copied = []

    def copy(**kw):
        if copied:
            raise ConnectionError("copy failed")
        copied.append(kw["Key"])

    aws["s3"].copy_object.side_effect = copy

    with pytest.raises(ConnectionError):
        handler.lambda_handler({"year": 2025, "month": 3}, None)
    deleted = [o["Key"] for c in aws["s3"].delete_objects.call_args_list for o in c.kwargs["Delete"]["Objects"]]
    assert not any(k.startswith("tag-cost-allocation/year=2025/month=3/old") for k in deleted)
    assert copied and set(copied) <= set(deleted)


def test_staging_prefix_has_lifecycle_rule():
    # Staging objects are written and deleted daily in a versioned bucket; without
    # this rule every run leaves noncurrent versions that are never expired.
    from tests.test_aggregator_templates import _load

    rules = _load("core-stack.yaml")["Resources"]["CentralAnalyticsBucket"]["Properties"]["LifecycleConfiguration"]["Rules"]
    rule = next(r for r in rules if r.get("Prefix") == "tag-cost-allocation-staging/")
    assert rule["Status"] == "Enabled"
    assert rule["NoncurrentVersionExpirationInDays"] == 1
    assert rule["ExpiredObjectDeleteMarker"] is True
    assert rule["AbortIncompleteMultipartUpload"] == {"DaysAfterInitiation": 1}


def _allocator_with_glue(glue):
    clients = {"glue": glue}
    with patch(f"{MODULE}.boto3.client", side_effect=lambda name, **kw: clients.get(name, MagicMock())), \
            patch(f"{MODULE}.AthenaQueryExecutor"):
        return handler.CostAllocator()


def test_schema_profile_rejects_unsupported_cur_table():
    glue = MagicMock()
    glue.get_table.return_value = {"Table": {"StorageDescriptor": {"Columns": []}, "PartitionKeys": []}}
    with pytest.raises(RuntimeError, match="Unsupported CUR schema"):
        _allocator_with_glue(glue).schema_profile()
    glue.get_table.assert_called_once_with(DatabaseName="cur_db", Name="cur_table")


def test_schema_profile_reads_cross_account_catalog(monkeypatch):
    monkeypatch.setenv("CUR_DATA_CATALOG", "ti_cur_dev")
    monkeypatch.setenv("CUR_CATALOG_ID", "123456789012")
    glue = MagicMock()
    glue.get_table.return_value = {"Table": {"StorageDescriptor": {"Columns": []}, "PartitionKeys": []}}
    with pytest.raises(RuntimeError):
        _allocator_with_glue(glue).schema_profile()
    glue.get_table.assert_called_once_with(DatabaseName="cur_db", Name="cur_table", CatalogId="123456789012")


def test_poll_deadline_reserves_time_to_publish(aws):
    # I1: an UNLOAD that finishes in the last seconds of the invocation must not
    # start publishing; a timeout mid-copy leaves old + new objects (double cost)
    # until the next daily run.
    aws["athena"].get_query_status.side_effect = [{"state": "RUNNING"}, {"state": "SUCCEEDED"}]
    context = MagicMock()
    context.get_remaining_time_in_millis.return_value = 900_000

    with patch(f"{MODULE}.time.monotonic", side_effect=[0, 790]):
        with pytest.raises(TimeoutError):
            handler.lambda_handler({"year": 2025, "month": 3}, context)
    aws["s3"].copy_object.assert_not_called()
    aws["athena"].cancel_query.assert_called_once()


def test_failed_copy_rollback_includes_in_flight_key(aws):
    # I2: CopyObject can complete server-side and still raise (e.g. a read
    # timeout); that key must be rolled back too or it stays live next to the old month.
    attempted = []

    def copy(**kw):
        attempted.append(kw["Key"])
        if len(attempted) == 2:
            raise ConnectionError("read timeout after copy completed")

    aws["s3"].copy_object.side_effect = copy

    with pytest.raises(ConnectionError):
        handler.lambda_handler({"year": 2025, "month": 3}, None)
    deleted = [o["Key"] for c in aws["s3"].delete_objects.call_args_list for o in c.kwargs["Delete"]["Objects"]]
    assert len(attempted) == 2
    assert set(attempted) <= set(deleted)


def test_failed_publish_clears_staging(aws):
    # I3: the staging lifecycle rule only expires noncurrent versions, so staged
    # objects left by a failed publish would be kept forever.
    aws["s3"].copy_object.side_effect = ConnectionError("copy failed")

    with pytest.raises(ConnectionError):
        handler.lambda_handler({"year": 2025, "month": 3}, None)
    deleted = [o["Key"] for c in aws["s3"].delete_objects.call_args_list for o in c.kwargs["Delete"]["Objects"]]
    staged = [k for k in deleted if k.startswith("tag-cost-allocation-staging/year=2025/month=3/")]
    assert len(staged) == 2
    assert not any(k.startswith("tag-cost-allocation/year=2025/month=3/old") for k in deleted)


def test_second_month_not_started_inside_publish_reserve(aws):
    # N1: the default run does two months in one invocation. If the first month
    # leaves less than the publish reserve, the 5 s poll floor still started the
    # second UNLOAD and published it with < 120 s left (or the Lambda was killed
    # mid-poll, leaving an unwatched UNLOAD and its staged output behind).
    context = MagicMock()
    context.get_remaining_time_in_millis.side_effect = [900_000, 100_000]

    with pytest.raises(TimeoutError, match="publish reserve"):
        handler.lambda_handler({}, context)
    assert [c[0] for c in aws["calls"]].count("unload") == 1
    assert aws["s3"].copy_object.call_count == 2  # first month's two staged parts only

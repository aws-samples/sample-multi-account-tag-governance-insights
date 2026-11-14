"""Tests for the Monthly Aggregator Lambda Handler (UNLOAD-based compaction)."""

import os
import re
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

BUCKET = "amzn-s3-demo-analytics-bucket"
A1, A2 = "111111111111", "222222222222"
CORE_STACK = os.path.join(os.path.dirname(__file__), "..", "..", "deployment", "core-stack.yaml")


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.setenv("CENTRAL_ANALYTICS_BUCKET", BUCKET)
    monkeypatch.setenv("ATHENA_RESULTS_BUCKET", "amzn-s3-demo-athena-results")
    monkeypatch.setenv("ATHENA_DATABASE", "ti_us-east-1_dev_123456789012")
    monkeypatch.setenv("ATHENA_WORKGROUP", "ti-us-east-1-dev-123456789012")


@pytest.fixture
def mod():
    s3, athena, glue = MagicMock(), MagicMock(), MagicMock()
    with patch("boto3.client", side_effect=lambda svc, **kw: {"s3": s3, "athena": athena, "glue": glue}[svc]):
        sys.modules.pop("lambda_handlers.monthly_aggregator_handler", None)
        import lambda_handlers.monthly_aggregator_handler as m
    m.s3_client, m.athena_client, m.glue_client = s3, athena, glue
    with patch.object(m.time, "sleep"):
        yield m, s3, athena, glue
    sys.modules.pop("lambda_handlers.monthly_aggregator_handler", None)


def _setup(m, s3, athena, glue, recent, historical=(), s3_keys=None, state="SUCCEEDED"):
    """recent: {account: [days]}; historical: accounts already compacted."""
    def get_partitions_pages(DatabaseName, TableName, Expression):
        assert Expression == "year = 2026 AND month = 9"
        if TableName == "resource_tag_deltas_recent":
            parts = [{"Values": [a, "2026", "9", str(d)]} for a, days in recent.items() for d in days]
        else:
            parts = [{"Values": [a, "2026", "9"]} for a in historical]
        # two pages to prove all pages are consumed
        return [{"Partitions": parts[:1]}, {"Partitions": parts[1:]}]

    glue_pager, s3_pager = MagicMock(), MagicMock()
    glue_pager.paginate.side_effect = get_partitions_pages
    glue.get_paginator.return_value = glue_pager
    glue.batch_create_partition.return_value = {}
    glue.batch_delete_partition.return_value = {}

    s3_keys = s3_keys if s3_keys is not None else {}
    s3_pager.paginate.side_effect = lambda Bucket, Prefix: [
        {"Contents": [{"Key": k} for k in s3_keys.get(Prefix, [])][:1]},
        {"Contents": [{"Key": k} for k in s3_keys.get(Prefix, [])][1:]},
    ]
    s3.get_paginator.return_value = s3_pager
    s3.delete_objects.return_value = {}

    athena.start_query_execution.side_effect = [{"QueryExecutionId": f"q{i}"} for i in range(50)]
    athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": state, "StateChangeReason": "boom"}}}


def _ctx(seconds=900):
    ctx = MagicMock()
    ctx.get_remaining_time_in_millis.return_value = seconds * 1000
    return ctx


def _deleted_keys(s3):
    return [o["Key"] for c in s3.delete_objects.call_args_list for o in c.kwargs["Delete"]["Objects"]]


NOV2 = datetime(2026, 11, 2, 3, tzinfo=timezone.utc)


# --- target month ---

@pytest.mark.parametrize("now,expected", [
    (datetime(2026, 11, 2, 3, tzinfo=timezone.utc), (2026, 9)),
    (datetime(2027, 1, 2, 3, tzinfo=timezone.utc), (2026, 11)),
    (datetime(2027, 2, 2, 3, tzinfo=timezone.utc), (2026, 12)),   # January rollover
    (datetime(2027, 3, 2, 3, tzinfo=timezone.utc), (2027, 1)),    # short February does not skip January
    (datetime(2027, 4, 2, 3, tzinfo=timezone.utc), (2027, 2)),
])
def test_target_month_default(mod, now, expected):
    m = mod[0]
    assert m.target_month({}, now) == expected


def test_every_month_compacted_exactly_once_by_day2_schedule(mod):
    m = mod[0]
    targets = [m.target_month({}, datetime(y, mo, 2, 3, tzinfo=timezone.utc))
               for y in (2027, 2028) for mo in range(1, 13)]
    assert len(set(targets)) == 24
    assert targets[0] == (2026, 11) and targets[-1] == (2028, 10)


def test_target_month_override(mod):
    m = mod[0]
    assert m.target_month({"year": 2025, "month": 1}, NOV2) == (2025, 1)


@pytest.mark.parametrize("event", [
    {"year": 2019, "month": 1}, {"year": 2101, "month": 1}, {"year": 2025, "month": 0},
    {"year": 2025, "month": 13}, {"year": "2025", "month": 1}, {"year": 2025}, {"month": True, "year": 2025},
])
def test_target_month_override_invalid(mod, event):
    with pytest.raises(ValueError):
        mod[0].target_month(event, NOV2)


# --- SQL ---

def test_unload_sql_columns_types_and_partition_filter(mod):
    m = mod[0]
    sql = m.build_unload_sql(m._config(), 2026, 9, [A1, A2])
    assert sql.startswith("UNLOAD (")
    assert 'FROM "ti_us-east-1_dev_123456789012".resource_tag_deltas_recent' in sql
    assert "WHERE year = 2026 AND month = 9" in sql
    assert f"account_id IN ('{A1}', '{A2}')" in sql
    assert "GROUP BY account_id, year, month, arn" in sql
    assert f"TO 's3://{BUCKET}/tag-deltas/historical/'" in sql
    assert "partitioned_by = ARRAY['account_id', 'year', 'month']" in sql
    assert "format = 'PARQUET', compression = 'SNAPPY'" in sql
    assert "max_by(tags, timestamp) AS tags" in sql            # map stays a map
    assert "min(timestamp) AS first_seen_timestamp" in sql      # bigint
    assert "CAST(count(*) AS integer) AS change_count" in sql   # int
    assert "max_by(changed_by, timestamp) FILTER (WHERE changed_by IS NOT NULL) AS changed_by" in sql
    # output column order: data columns in Glue order, then partition columns last
    select = sql.split("SELECT", 1)[1].split("FROM", 1)[0]
    names = [re.split(r"\s+AS\s+", c.strip())[-1] for c in re.split(r",\s*\n", select)]
    names = names[:-1] + [n.strip() for n in names[-1].split(",")]
    assert names == [n for n, _ in m.HISTORICAL_COLUMNS] + ["account_id", "year", "month"]


def test_historical_columns_match_glue_table(mod):
    m = mod[0]

    class Loader(yaml.SafeLoader):
        pass
    Loader.add_multi_constructor("!", lambda loader, suffix, node: None)
    with open(CORE_STACK) as f:
        resources = Loader(f).get_single_data()["Resources"]
    table = resources["TagDeltasHistoricalTable"]["Properties"]["TableInput"]
    glue_cols = [c["Name"] for c in table["StorageDescriptor"]["Columns"]]
    ours = [n for n, _ in m.HISTORICAL_COLUMNS]
    if "changed_by" not in glue_cols:  # columns appended by a parallel change
        ours = [n for n in ours if n not in ("changed_by", "event_name")]
    assert glue_cols == ours
    assert [k["Name"] for k in table["PartitionKeys"]] == m.PARTITION_COLUMNS


@pytest.mark.parametrize("cfg_override,accounts", [
    ({"database": 'db"; DROP'}, [A1]),
    ({"bucket": "Bad_Bucket"}, [A1]),
    ({}, ["12345' OR '1"]),
    ({}, []),
    ({}, [f"{i:012d}" for i in range(101)]),
])
def test_unload_sql_rejects_bad_input(mod, cfg_override, accounts):
    m = mod[0]
    with pytest.raises(ValueError):
        m.build_unload_sql({**m._config(), **cfg_override}, 2026, 9, accounts)


def test_config_rejects_bad_database(mod, monkeypatch):
    monkeypatch.setenv("ATHENA_DATABASE", "db;drop")
    with pytest.raises(ValueError):
        mod[0]._config()


# --- handler flow ---

def test_handler_happy_path(mod):
    m, s3, athena, glue = mod
    rp = f"tag-deltas/recent/account_id={A1}/year=2026/month=9/"
    keys = {
        rp: [f"{rp}day=1/a.parquet", f"{rp}day=2/b.parquet", f"{rp}day=3/unregistered.parquet"],
        f"tag-deltas/historical/account_id={A1}/year=2026/month=9/": ["tag-deltas/historical/account_id=111111111111/year=2026/month=9/orphan"],
    }
    _setup(m, s3, athena, glue, {A1: [1, 2], A2: [5]}, s3_keys=keys)

    result = m.lambda_handler({}, _ctx(), now=NOV2)

    assert result["target_year"] == 2026 and result["target_month"] == 9
    assert result["accounts_compacted"] == 2
    assert athena.start_query_execution.call_count == 1
    kw = athena.start_query_execution.call_args.kwargs
    assert kw["WorkGroup"] == "ti-us-east-1-dev-123456789012"
    created = glue.batch_create_partition.call_args.kwargs["PartitionInputList"]
    assert [p["Values"] for p in created] == [[A1, "2026", "9"], [A2, "2026", "9"]]
    assert created[0]["StorageDescriptor"]["Location"] == f"s3://{BUCKET}/tag-deltas/historical/account_id={A1}/year=2026/month=9/"
    deleted = _deleted_keys(s3)
    # orphan cleared before UNLOAD; only registered days deleted after
    assert deleted == ["tag-deltas/historical/account_id=111111111111/year=2026/month=9/orphan",
                       f"{rp}day=1/a.parquet", f"{rp}day=2/b.parquet"]
    dropped = [p["Values"] for c in glue.batch_delete_partition.call_args_list for p in c.kwargs["PartitionsToDelete"]]
    assert dropped == [[A1, "2026", "9", "1"], [A1, "2026", "9", "2"], [A2, "2026", "9", "5"]]
    assert result["partitions_deleted"] == 3


def test_clear_happens_before_unload(mod):
    m, s3, athena, glue = mod
    hp = f"tag-deltas/historical/account_id={A1}/year=2026/month=9/"
    _setup(m, s3, athena, glue, {A1: [1]}, s3_keys={hp: [f"{hp}old"]})
    order = []
    s3.delete_objects.side_effect = lambda **kw: order.append("delete") or {}
    athena.start_query_execution.side_effect = lambda **kw: order.append("unload") or {"QueryExecutionId": "q"}
    m.lambda_handler({}, _ctx(), now=NOV2)
    assert order[:2] == ["delete", "unload"]


def test_batches_of_100_accounts(mod):
    m, s3, athena, glue = mod
    _setup(m, s3, athena, glue, {f"{i:012d}": [1] for i in range(1, 251)})
    result = m.lambda_handler({}, _ctx(), now=NOV2)
    assert athena.start_query_execution.call_count == 3
    assert result["accounts_compacted"] == 250
    assert glue.batch_create_partition.call_count == 3


def test_already_compacted_accounts_are_not_reunloaded(mod):
    m, s3, athena, glue = mod
    _setup(m, s3, athena, glue, {A1: [1], A2: [1]}, historical=[A1])
    result = m.lambda_handler({}, _ctx(), now=NOV2)
    sql = athena.start_query_execution.call_args.kwargs["QueryString"]
    assert f"'{A2}'" in sql and f"'{A1}'" not in sql
    assert result["accounts_already_compacted"] == 1
    dropped = [p["Values"][0] for c in glue.batch_delete_partition.call_args_list for p in c.kwargs["PartitionsToDelete"]]
    assert dropped == [A1, A2]  # resume: finishes cleanup for A1 too


def test_nothing_to_do(mod):
    m, s3, athena, glue = mod
    _setup(m, s3, athena, glue, {})
    result = m.lambda_handler({"year": 2026, "month": 9}, _ctx())
    athena.start_query_execution.assert_not_called()
    assert result["accounts_compacted"] == 0


@pytest.mark.parametrize("state", ["FAILED", "CANCELLED"])
def test_unload_failure_raises_and_deletes_nothing(mod, state):
    m, s3, athena, glue = mod
    _setup(m, s3, athena, glue, {A1: [1]}, state=state)
    with pytest.raises(RuntimeError, match=state):
        m.lambda_handler({}, _ctx(), now=NOV2)
    glue.batch_create_partition.assert_not_called()
    glue.batch_delete_partition.assert_not_called()
    s3.delete_objects.assert_not_called()


def test_partition_registration_failure_deletes_nothing(mod):
    m, s3, athena, glue = mod
    rp = f"tag-deltas/recent/account_id={A1}/year=2026/month=9/"
    _setup(m, s3, athena, glue, {A1: [1]}, s3_keys={rp: [f"{rp}day=1/a"]})
    glue.batch_create_partition.return_value = {"Errors": [
        {"PartitionValues": [A1, "2026", "9"], "ErrorDetail": {"ErrorCode": "AccessDeniedException"}}]}
    with pytest.raises(RuntimeError, match="register"):
        m.lambda_handler({}, _ctx(), now=NOV2)
    glue.batch_delete_partition.assert_not_called()
    s3.delete_objects.assert_not_called()


def test_partition_already_exists_is_ok(mod):
    m, s3, athena, glue = mod
    _setup(m, s3, athena, glue, {A1: [1]})
    glue.batch_create_partition.return_value = {"Errors": [
        {"PartitionValues": [A1, "2026", "9"], "ErrorDetail": {"ErrorCode": "AlreadyExistsException"}}]}
    m.lambda_handler({}, _ctx(), now=NOV2)
    glue.batch_delete_partition.assert_called_once()


def test_timeout_cancels_and_raises(mod):
    m, s3, athena, glue = mod
    _setup(m, s3, athena, glue, {f"{i:012d}": [1] for i in range(1, 151)}, state="RUNNING")
    m.POLL_INTERVAL_SECONDS = 0
    with pytest.raises(TimeoutError):
        m.lambda_handler({}, _ctx(seconds=0), now=NOV2)
    stopped = [c.kwargs["QueryExecutionId"] for c in athena.stop_query_execution.call_args_list]
    assert stopped == ["q0", "q1"]
    glue.batch_create_partition.assert_not_called()
    s3.delete_objects.assert_not_called()


def test_failure_cancels_other_running_unloads(mod):
    m, s3, athena, glue = mod
    _setup(m, s3, athena, glue, {f"{i:012d}": [1] for i in range(1, 151)})
    athena.get_query_execution.side_effect = lambda QueryExecutionId: {"QueryExecution": {"Status": {
        "State": "FAILED" if QueryExecutionId == "q0" else "RUNNING"}}}
    with pytest.raises(RuntimeError):
        m.lambda_handler({}, _ctx(), now=NOV2)
    assert [c.kwargs["QueryExecutionId"] for c in athena.stop_query_execution.call_args_list] == ["q1"]


def test_delete_objects_errors_raise_after_attempting_all_accounts(mod):
    m, s3, athena, glue = mod
    p1 = f"tag-deltas/recent/account_id={A1}/year=2026/month=9/"
    p2 = f"tag-deltas/recent/account_id={A2}/year=2026/month=9/"
    _setup(m, s3, athena, glue, {A1: [1], A2: [1]}, s3_keys={p1: [f"{p1}day=1/a"], p2: [f"{p2}day=1/b"]})
    s3.delete_objects.side_effect = lambda **kw: (
        {"Errors": [{"Key": "x", "Code": "AccessDenied"}]} if kw["Delete"]["Objects"][0]["Key"].startswith(p1) else {})
    with pytest.raises(RuntimeError, match="1 accounts"):
        m.lambda_handler({}, _ctx(), now=NOV2)
    # A1's Glue partitions kept (its S3 delete failed); A2 fully cleaned
    dropped = [p["Values"][0] for c in glue.batch_delete_partition.call_args_list for p in c.kwargs["PartitionsToDelete"]]
    assert dropped == [A2]


def test_clear_errors_raise_before_unload(mod):
    m, s3, athena, glue = mod
    hp = f"tag-deltas/historical/account_id={A1}/year=2026/month=9/"
    _setup(m, s3, athena, glue, {A1: [1]}, s3_keys={hp: [f"{hp}old"]})
    s3.delete_objects.return_value = {"Errors": [{"Key": f"{hp}old", "Code": "AccessDenied"}]}
    with pytest.raises(RuntimeError):
        m.lambda_handler({}, _ctx(), now=NOV2)
    athena.start_query_execution.assert_not_called()


def test_glue_delete_errors_raise(mod):
    m, s3, athena, glue = mod
    _setup(m, s3, athena, glue, {A1: [1]})
    glue.batch_delete_partition.return_value = {"Errors": [
        {"PartitionValues": [A1, "2026", "9", "1"], "ErrorDetail": {"ErrorCode": "InternalServiceException"}}]}
    with pytest.raises(RuntimeError):
        m.lambda_handler({}, _ctx(), now=NOV2)


def test_invalid_account_partitions_skipped(mod):
    m, s3, athena, glue = mod
    _setup(m, s3, athena, glue, {"not-an-account": [1], A1: [1]})
    result = m.lambda_handler({}, _ctx(), now=NOV2)
    assert result["accounts_compacted"] == 1
    assert "not-an-account" not in athena.start_query_execution.call_args.kwargs["QueryString"]


# --- lifecycle vs. compaction age (B3) ---

def _cfn(path):
    class Loader(yaml.SafeLoader):
        pass
    Loader.add_multi_constructor("!", lambda loader, suffix, node: None)
    with open(path) as f:
        return Loader(f).get_single_data()["Resources"]


def test_recent_lifecycle_outlives_compaction_with_a_missed_run(mod):
    """Every daily object of the target month must still exist when the monthly
    job reads it, even if one monthly run is missed (+31 days)."""
    m = mod[0]
    agg = _cfn(os.path.join(os.path.dirname(CORE_STACK), "aggregator-stack.yaml"))
    cron = agg["MonthlyAggregationScheduleRule"]["Properties"]["ScheduleExpression"]
    minute, hour, dom = re.fullmatch(r"cron\((\d+) (\d+) (\d+) \* \? \*\)", cron).groups()
    rules = _cfn(CORE_STACK)["CentralAnalyticsBucket"]["Properties"]["LifecycleConfiguration"]["Rules"]
    expiry = {r["Prefix"]: r for r in rules}["tag-deltas/recent/"]["ExpirationInDays"]

    oldest = 0
    for year in (2027, 2028):  # 2028 is a leap year
        for month in range(1, 13):
            run = datetime(year, month, int(dom), int(hour), int(minute), tzinfo=timezone.utc)
            ty, tm = m.target_month({}, run)
            # S3 expiry counts from creation, rounded up to the next midnight: +1 day.
            oldest = max(oldest, (run - datetime(ty, tm, 1, tzinfo=timezone.utc)).days + 1)
    assert expiry > oldest + 31, f"expiry {expiry}d, oldest object read at {oldest}d"


# --- H4: an API error must not leave started UNLOADs running ---

def test_start_error_cancels_already_started_unloads(mod):
    """A throttled 2nd StartQueryExecution left q0 running. The async retry
    then cleared q0's prefix and re-UNLOADed while q0 kept appending, so the
    month ended with duplicate historical rows."""
    m, s3, athena, glue = mod
    _setup(m, s3, athena, glue, {f"{i:012d}": [1] for i in range(1, 151)})
    athena.start_query_execution.side_effect = [{"QueryExecutionId": "q0"}, RuntimeError("TooManyRequestsException")]
    with pytest.raises(RuntimeError, match="TooManyRequests"):
        m.lambda_handler({}, _ctx(), now=NOV2)
    assert [c.kwargs["QueryExecutionId"] for c in athena.stop_query_execution.call_args_list] == ["q0"]
    glue.batch_create_partition.assert_not_called()


def test_poll_error_cancels_running_unloads(mod):
    m, s3, athena, glue = mod
    _setup(m, s3, athena, glue, {f"{i:012d}": [1] for i in range(1, 151)})
    athena.get_query_execution.side_effect = RuntimeError("ThrottlingException")
    with pytest.raises(RuntimeError, match="Throttling"):
        m.lambda_handler({}, _ctx(), now=NOV2)
    assert [c.kwargs["QueryExecutionId"] for c in athena.stop_query_execution.call_args_list] == ["q0", "q1"]


# --- H6: a manual run must not compact a month that has not ended ---

@pytest.mark.parametrize("event", [{"year": 2026, "month": 11}, {"year": 2026, "month": 12}, {"year": 2027, "month": 1}])
def test_target_month_override_rejects_unfinished_month(mod, event):
    """Compacting the running month marks its accounts done; deltas written
    later that month are then deleted by the next cleanup without ever being
    compacted."""
    with pytest.raises(ValueError, match="not ended"):
        mod[0].target_month(event, NOV2)


@pytest.mark.parametrize("event,now", [
    ({"year": 2026, "month": 10}, NOV2),
    ({"year": 2026, "month": 9}, datetime(2026, 10, 1, 0, 30, tzinfo=timezone.utc)),
    ({"year": 2026, "month": 9}, datetime(2026, 10, 28, 23, 59, tzinfo=timezone.utc)),
])
def test_target_month_override_rejects_month_ended_under_min_age(mod, event, now):
    """M1: deltas for a month keep landing after it ends (SQS redelivery, DLQ
    redrive within 14 days). An early override marks the accounts done, and
    the scheduled run for that month then deletes the late deltas in cleanup
    without compacting them."""
    with pytest.raises(ValueError, match="not ended"):
        mod[0].target_month(event, now)


def test_target_month_override_accepts_month_the_schedule_would_pick(mod):
    m = mod[0]
    assert m.target_month({"year": 2026, "month": 9}, NOV2) == m.target_month({}, NOV2) == (2026, 9)
    assert m.target_month({"year": 2026, "month": 9}, datetime(2026, 10, 29, tzinfo=timezone.utc)) == (2026, 9)

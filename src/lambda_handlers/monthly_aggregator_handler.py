"""
Monthly Aggregator Lambda Handler

Compacts one calendar month of daily tag deltas (resource_tag_deltas_recent,
partitioned account_id/year/month/day) into one row per (account, arn) in
resource_tag_deltas_historical (partitioned account_id/year/month), then
deletes the compacted daily data.

Flow (EventBridge cron, day 2 of each month; or {"year": Y, "month": M}):
1. Target month = most recent month that ended >= 28 days before the run
   (run on Nov 2 -> September; Mar 2 -> January, so February never skips one).
2. List the month's recent partitions (Glue) and the accounts already compacted
   (historical partitions registered). Registered historical partitions mark a
   finished UNLOAD, so a rerun after a partial cleanup never recompacts a
   partially-deleted month over complete data.
3. For the remaining accounts, clear any orphaned historical output, then run
   one Athena UNLOAD per <=100 accounts (UNLOAD's partition limit), all in
   parallel, partitioned_by account_id/year/month. Athena writes typed Parquet
   (maps stay maps, bigint/int stay numeric).
4. Only after every UNLOAD SUCCEEDED and every partition registered, delete
   the compacted daily objects and Glue partitions. Any failure raises so the
   Lambda Errors alarm fires.
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

s3_client = boto3.client('s3')
athena_client = boto3.client('athena')
glue_client = boto3.client('glue')

RECENT_TABLE = 'resource_tag_deltas_recent'
HISTORICAL_TABLE = 'resource_tag_deltas_historical'
RECENT_PREFIX = 'tag-deltas/recent/'
HISTORICAL_PREFIX = 'tag-deltas/historical/'

MIN_YEAR, MAX_YEAR = 2020, 2100
MIN_AGE_DAYS = 28            # month must have ended this long ago (see module docstring)
UNLOAD_ACCOUNT_BATCH = 100   # Athena UNLOAD writes at most 100 partitions
S3_DELETE_BATCH = 1000
GLUE_CREATE_BATCH = 100
GLUE_DELETE_BATCH = 25
POLL_INTERVAL_SECONDS = 5
DEFAULT_MAX_POLL_SECONDS = 600
CLEANUP_BUFFER_SECONDS = 120  # time reserved after UNLOAD for registration + cleanup

_BUCKET_RE = re.compile(r'^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$')
_DATABASE_RE = re.compile(r'^[A-Za-z0-9_-]{1,255}$')
_WORKGROUP_RE = re.compile(r'^[A-Za-z0-9._-]{1,128}$')
_ACCOUNT_RE = re.compile(r'^\d{12}$')
_DAY_IN_KEY_RE = re.compile(r'/day=(\d{1,2})/')

# Historical Glue columns in table order -> aggregate over the month's deltas.
# Last-state columns take the value of the latest delta; changed_by/event_name
# carry the last non-null value (NULL if none).
HISTORICAL_COLUMNS: List[Tuple[str, str]] = [
    ('arn', 'arn'),
    ('resource_type', 'max_by(resource_type, timestamp)'),
    ('tags', 'max_by(tags, timestamp)'),
    ('previous_tags', 'max_by(previous_tags, timestamp)'),
    ('change_type', 'max_by(change_type, timestamp)'),
    ('first_seen_timestamp', 'min(timestamp)'),
    ('last_seen_timestamp', 'max(timestamp)'),
    ('change_count', 'CAST(count(*) AS integer)'),
    ('source_system', 'max_by(source_system, timestamp)'),
    ('region', 'max_by(region, timestamp)'),
    ('changed_by', 'max_by(changed_by, timestamp) FILTER (WHERE changed_by IS NOT NULL)'),
    ('event_name', 'max_by(event_name, timestamp) FILTER (WHERE event_name IS NOT NULL)'),
]
PARTITION_COLUMNS = ['account_id', 'year', 'month']

_PARQUET_SD = {
    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
    'SerdeInfo': {
        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
        'Parameters': {'serialization.format': '1'},
    },
}


def _config() -> Dict[str, str]:
    """Read and validate identifiers that are interpolated into SQL / S3 paths."""
    cfg = {
        'bucket': os.environ.get('CENTRAL_ANALYTICS_BUCKET', ''),
        'results_bucket': os.environ.get('ATHENA_RESULTS_BUCKET', ''),
        'database': os.environ.get('ATHENA_DATABASE', ''),
        'workgroup': os.environ.get('ATHENA_WORKGROUP', ''),
    }
    for key, pattern in (('bucket', _BUCKET_RE), ('results_bucket', _BUCKET_RE),
                         ('database', _DATABASE_RE), ('workgroup', _WORKGROUP_RE)):
        if not pattern.fullmatch(cfg[key]):
            raise ValueError(f"Invalid or missing {key} configuration: {cfg[key]!r}")
    return cfg


def _validate_year_month(year: Any, month: Any) -> Tuple[int, int]:
    for name, value, low, high in (('year', year, MIN_YEAR, MAX_YEAR), ('month', month, 1, 12)):
        if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
            raise ValueError(f"Invalid {name}: {value!r} (expected int in {low}..{high})")
    return year, month


def target_month(event: Dict[str, Any], now: Optional[datetime] = None) -> Tuple[int, int]:
    """Explicit {year, month} from the event, else the most recent month that
    ended at least MIN_AGE_DAYS before now (UTC)."""
    now = now or datetime.now(timezone.utc)
    cutoff = now.astimezone(timezone.utc) - timedelta(days=MIN_AGE_DAYS)
    if 'year' in event or 'month' in event:
        year, month = _validate_year_month(event.get('year'), event.get('month'))
        # Compacting marks the month's accounts done, and deltas for a month keep
        # arriving after it ends (SQS redelivery, DLQ redrive). Those would be
        # deleted by cleanup without being compacted, so the override gets the
        # same MIN_AGE_DAYS margin as the scheduled run.
        if (year, month) >= (cutoff.year, cutoff.month):
            raise ValueError(f"Month {year}-{month:02d} has not ended {MIN_AGE_DAYS} days before now")
        return year, month
    # The month containing cutoff has not ended by cutoff; the one before it has.
    year, month = (cutoff.year - 1, 12) if cutoff.month == 1 else (cutoff.year, cutoff.month - 1)
    return _validate_year_month(year, month)


def _chunks(items: List[Any], size: int) -> Iterable[List[Any]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


def _historical_prefix(account_id: str, year: int, month: int) -> str:
    return f"{HISTORICAL_PREFIX}account_id={account_id}/year={year}/month={month}/"


def list_month_partitions(cfg: Dict[str, str], table: str, year: int, month: int) -> Dict[str, Set[int]]:
    """{account_id: {day, ...}} for the month's Glue partitions (day empty for historical)."""
    found: Dict[str, Set[int]] = {}
    paginator = glue_client.get_paginator('get_partitions')
    pages = paginator.paginate(
        DatabaseName=cfg['database'], TableName=table,
        Expression=f"year = {year} AND month = {month}",
    )
    for page in pages:
        for partition in page.get('Partitions', []):
            values = partition['Values']
            if not _ACCOUNT_RE.fullmatch(values[0]):
                logger.warning(f"Skipping partition with invalid account_id in {table}: {values!r}")
                continue
            days = found.setdefault(values[0], set())
            if len(values) > 3:
                days.add(int(values[3]))
    return found


def build_unload_sql(cfg: Dict[str, str], year: int, month: int, account_ids: List[str]) -> str:
    """One set-based UNLOAD compacting the month for up to 100 accounts."""
    _validate_year_month(year, month)
    if not account_ids or len(account_ids) > UNLOAD_ACCOUNT_BATCH:
        raise ValueError(f"UNLOAD batch must hold 1..{UNLOAD_ACCOUNT_BATCH} accounts")
    for account_id in account_ids:
        if not _ACCOUNT_RE.fullmatch(account_id):
            raise ValueError(f"Invalid account_id: {account_id!r}")
    if not _DATABASE_RE.fullmatch(cfg['database']) or not _BUCKET_RE.fullmatch(cfg['bucket']):
        raise ValueError("Invalid database or bucket identifier")

    select_list = ",\n       ".join(f"{expr} AS {name}" if expr != name else name
                                   for name, expr in HISTORICAL_COLUMNS)
    accounts = ", ".join(f"'{a}'" for a in account_ids)
    partitions = ", ".join(f"'{c}'" for c in PARTITION_COLUMNS)
    return f"""UNLOAD (
SELECT {select_list},
       account_id, year, month
FROM "{cfg['database']}".{RECENT_TABLE}
WHERE year = {year} AND month = {month}
  AND account_id IN ({accounts})
GROUP BY account_id, year, month, arn
)
TO 's3://{cfg['bucket']}/{HISTORICAL_PREFIX}'
WITH (format = 'PARQUET', compression = 'SNAPPY', partitioned_by = ARRAY[{partitions}])"""  # nosec B608 - database/bucket regex-validated (database double-quoted); account_ids match \d{12}; year/month validated ints; column expressions are constants


def delete_keys(cfg: Dict[str, str], keys: List[str]) -> None:
    """Batch-delete keys; raise if S3 reports any per-key error."""
    for batch in _chunks(keys, S3_DELETE_BATCH):
        response = s3_client.delete_objects(
            Bucket=cfg['bucket'],
            Delete={'Objects': [{'Key': k} for k in batch], 'Quiet': True},
        )
        errors = response.get('Errors', [])
        if errors:
            raise RuntimeError(f"Failed to delete {len(errors)} of {len(batch)} objects: {errors[:3]}")


def list_keys(cfg: Dict[str, str], prefix: str) -> List[str]:
    paginator = s3_client.get_paginator('list_objects_v2')
    return [obj['Key']
            for page in paginator.paginate(Bucket=cfg['bucket'], Prefix=prefix)
            for obj in page.get('Contents', [])]


def run_unloads(cfg: Dict[str, str], queries: List[str], deadline: float) -> List[str]:
    """Start all UNLOADs, wait for all to SUCCEED; cancel the rest and raise otherwise.

    Any error (a FAILED query, the deadline, or a start/poll API error) cancels
    every started, unfinished UNLOAD: one left running would keep appending to
    a prefix the retry clears and re-UNLOADs, duplicating historical rows."""
    pending: List[str] = []
    started: List[str] = []
    try:
        for sql in queries:
            response = athena_client.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={'Database': cfg['database']},
                ResultConfiguration={'OutputLocation': f"s3://{cfg['results_bucket']}/aggregator-queries/"},
                WorkGroup=cfg['workgroup'],
            )
            pending.append(response['QueryExecutionId'])
            started.append(response['QueryExecutionId'])

        while pending:
            for qid in list(pending):
                status = athena_client.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']
                state = status['State']
                if state == 'SUCCEEDED':
                    pending.remove(qid)
                elif state in ('FAILED', 'CANCELLED'):
                    pending.remove(qid)
                    raise RuntimeError(f"UNLOAD {qid} {state}: {status.get('StateChangeReason', 'Unknown')}")
            if not pending:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"UNLOAD queries {pending} not finished before deadline; cancelled")
            time.sleep(POLL_INTERVAL_SECONDS)
    except Exception:
        for qid in pending:
            try:
                athena_client.stop_query_execution(QueryExecutionId=qid)
            except Exception as e:  # best effort; the original failure is what we raise
                logger.warning(f"Could not cancel UNLOAD {qid}: {e}")
        raise
    return started


def register_partitions(cfg: Dict[str, str], account_ids: List[str], year: int, month: int) -> None:
    """Register historical partitions; raise on any error other than AlreadyExists."""
    for batch in _chunks(account_ids, GLUE_CREATE_BATCH):
        response = glue_client.batch_create_partition(
            DatabaseName=cfg['database'], TableName=HISTORICAL_TABLE,
            PartitionInputList=[{
                'Values': [a, str(year), str(month)],
                'StorageDescriptor': {**_PARQUET_SD,
                                      'Location': f"s3://{cfg['bucket']}/{_historical_prefix(a, year, month)}"},
            } for a in batch],
        )
        errors = [e for e in response.get('Errors', [])
                  if e.get('ErrorDetail', {}).get('ErrorCode') != 'AlreadyExistsException']
        if errors:
            raise RuntimeError(f"Failed to register {len(errors)} historical partitions: {errors[:3]}")


def delete_recent_month(cfg: Dict[str, str], recent: Dict[str, Set[int]], year: int, month: int) -> Dict[str, int]:
    """Delete the compacted daily objects and Glue partitions. Attempts every
    account, then raises if anything failed (rerun resumes: historical is registered)."""
    objects = partitions = 0
    failures = []
    for account_id, days in sorted(recent.items()):
        try:
            prefix = f"{RECENT_PREFIX}account_id={account_id}/year={year}/month={month}/"
            keys = [k for k in list_keys(cfg, prefix)
                    if (m := _DAY_IN_KEY_RE.search(k[len(prefix) - 1:])) and int(m.group(1)) in days]
            delete_keys(cfg, keys)
            objects += len(keys)
            for batch in _chunks(sorted(days), GLUE_DELETE_BATCH):
                response = glue_client.batch_delete_partition(
                    DatabaseName=cfg['database'], TableName=RECENT_TABLE,
                    PartitionsToDelete=[{'Values': [account_id, str(year), str(month), str(d)]} for d in batch],
                )
                errors = [e for e in response.get('Errors', [])
                          if e.get('ErrorDetail', {}).get('ErrorCode') != 'EntityNotFoundException']
                if errors:
                    raise RuntimeError(f"Failed to delete {len(errors)} recent partitions: {errors[:3]}")
                partitions += len(batch)
        except Exception as e:
            logger.error(f"Cleanup failed for account {account_id} {year}-{month:02d}: {e}")
            failures.append(account_id)
    if failures:
        raise RuntimeError(f"Daily cleanup failed for {len(failures)} accounts: {failures[:10]}")
    return {'objects_deleted': objects, 'partitions_deleted': partitions}


def _max_poll_seconds(context) -> float:
    remaining_ms = getattr(context, 'get_remaining_time_in_millis', None)
    if callable(remaining_ms):
        return max(remaining_ms() / 1000 - CLEANUP_BUFFER_SECONDS, POLL_INTERVAL_SECONDS)
    return DEFAULT_MAX_POLL_SECONDS


def lambda_handler(event, context, now: Optional[datetime] = None):
    """Compact one month of daily deltas into historical monthly rows."""
    event = event or {}
    logger.info(f"Monthly Aggregator started: {json.dumps(event, default=str)}")
    cfg = _config()
    year, month = target_month(event, now)
    deadline = time.monotonic() + _max_poll_seconds(context)

    recent = list_month_partitions(cfg, RECENT_TABLE, year, month)
    done = set(list_month_partitions(cfg, HISTORICAL_TABLE, year, month))
    todo = sorted(set(recent) - done)
    logger.info(f"{year}-{month:02d}: {len(recent)} accounts with daily data, "
                f"{len(recent) - len(todo)} already compacted, {len(todo)} to compact")

    # UNLOAD into partitions appends, so clear orphaned output of earlier failed runs.
    for account_id in todo:
        delete_keys(cfg, list_keys(cfg, _historical_prefix(account_id, year, month)))

    queries = [build_unload_sql(cfg, year, month, batch) for batch in _chunks(todo, UNLOAD_ACCOUNT_BATCH)]
    query_ids = run_unloads(cfg, queries, deadline) if queries else []
    register_partitions(cfg, todo, year, month)
    cleanup = delete_recent_month(cfg, recent, year, month)

    result = {
        'target_year': year,
        'target_month': month,
        'accounts_compacted': len(todo),
        'accounts_already_compacted': len(recent) - len(todo),
        'query_execution_ids': query_ids,
        **cleanup,
    }
    logger.info(f"Monthly aggregation completed: {result}")
    return result

#!/usr/bin/env python3
"""
Cost Allocator Lambda Handler

Set-based tag cost allocation: one Athena UNLOAD per billing month writes
Parquet to s3://<CENTRAL_ANALYTICS_BUCKET>/tag-cost-allocation/year=Y/month=M/,
read through the partition-projected ``tag_cost_allocation`` Glue table.

UNLOAD writes to a run-specific staging prefix
(tag-cost-allocation-staging/year=Y/month=M/<run-id>/, outside the table
location). Only after it succeeds is the live month prefix replaced
(server-side copy of the new objects, then delete of the old ones), so a
failed or timed-out query, or a failed copy, leaves the previous month's data
in place.

Default run recomputes the current and previous UTC month (CUR keeps revising
the previous month for a few days); ``{"year": Y, "month": M}`` recomputes one.
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import boto3

try:
    from loguru import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

from .allocation_query import build_allocation_select
from .athena_query_executor import AthenaQueryExecutor
from .schema_manager import CURSchemaManager

OUTPUT_PREFIX = 'tag-cost-allocation'
STAGING_PREFIX = 'tag-cost-allocation-staging'
MIN_YEAR, MAX_YEAR = 2020, 2100  # matches the Glue partition projection range
DELETE_BATCH_SIZE = 1000
POLL_INTERVAL_SECONDS = 5
DEFAULT_MAX_POLL_SECONDS = 840
# Time kept after the UNLOAD poll for publish (list + copy + delete). A timeout
# mid-publish would leave old and new objects live until the next run.
TIMEOUT_BUFFER_SECONDS = 120


def _validate_year_month(year: Any, month: Any) -> Tuple[int, int]:
    for name, value, low, high in (('year', year, MIN_YEAR, MAX_YEAR), ('month', month, 1, 12)):
        if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
            raise ValueError(f"Invalid {name}: {value!r} (expected int in {low}..{high})")
    return year, month


def months_to_compute(event: Dict[str, Any], now: datetime = None) -> List[Tuple[int, int]]:
    """Explicit {year, month} from the event, else previous + current UTC month."""
    if 'year' in event or 'month' in event:
        return [_validate_year_month(event.get('year'), event.get('month'))]
    now = now or datetime.now(timezone.utc)
    previous = (now.year - 1, 12) if now.month == 1 else (now.year, now.month - 1)
    return [_validate_year_month(*previous), _validate_year_month(now.year, now.month)]


class CostAllocator:
    """Recomputes tag cost allocation for a billing month via Athena UNLOAD."""

    def __init__(self):
        self.region = os.environ.get('AWS_REGION', 'us-east-1')
        self.bucket = os.environ['CENTRAL_ANALYTICS_BUCKET']
        self.analytics_database = os.environ['ATHENA_DATABASE']
        self.workgroup = os.environ['ATHENA_WORKGROUP']
        self.cur_database = os.environ.get('CUR_ATHENA_DATABASE', '').strip()
        self.cur_table = os.environ.get('CUR_ATHENA_TABLE', '').strip()
        # Cross-account CUR: billing account's Glue catalog registered as an Athena DataCatalog.
        self.cur_catalog = os.environ.get('CUR_DATA_CATALOG', '').strip() or None
        self.cur_catalog_id = os.environ.get('CUR_CATALOG_ID', '').strip() or None
        if bool(self.cur_catalog) != bool(self.cur_catalog_id):
            raise ValueError("CUR_DATA_CATALOG and CUR_CATALOG_ID must be set together")
        results_bucket = os.environ.get('ATHENA_RESULTS_BUCKET') or os.environ.get('ATHENA_OUTPUT_BUCKET') or self.bucket

        self.s3 = boto3.client('s3', region_name=self.region)
        self.glue = boto3.client('glue', region_name=self.region)
        self.athena = AthenaQueryExecutor(
            database=self.analytics_database,
            output_bucket=results_bucket,
            region=self.region,
            workgroup=self.workgroup,
        )
        self._schema_profile = None

    def schema_profile(self) -> Dict[str, Any]:
        if self._schema_profile is None:
            kwargs = {'CatalogId': self.cur_catalog_id} if self.cur_catalog_id else {}
            table_info = self.glue.get_table(
                DatabaseName=self.cur_database,
                Name=self.cur_table,
                **kwargs,
            )['Table']
            profile = CURSchemaManager.build_schema_profile(table_info)
            if not profile['supported']:
                raise RuntimeError(f"Unsupported CUR schema for configured table {self.cur_database}.{self.cur_table}")
            self._schema_profile = profile
        return self._schema_profile

    def list_keys(self, prefix: str) -> List[str]:
        keys = []
        paginator = self.s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj['Key'] for obj in page.get('Contents', []))
        return keys

    def delete_keys(self, keys: List[str], prefix: str) -> int:
        for start in range(0, len(keys), DELETE_BATCH_SIZE):
            batch = keys[start:start + DELETE_BATCH_SIZE]
            response = self.s3.delete_objects(
                Bucket=self.bucket,
                Delete={'Objects': [{'Key': key} for key in batch], 'Quiet': True},
            )
            errors = response.get('Errors', [])
            if errors:
                raise RuntimeError(f"Failed to delete {len(errors)} objects under s3://{self.bucket}/{prefix}: {errors[:3]}")
        return len(keys)

    def clear_prefix(self, prefix: str) -> int:
        """Delete every object under prefix."""
        return self.delete_keys(self.list_keys(prefix), prefix)

    def run_unload(self, unload_sql: str, max_poll_seconds: float) -> str:
        execution_id = self.athena.execute_query_async(unload_sql)
        deadline = time.monotonic() + max_poll_seconds
        while True:
            try:
                status = self.athena.get_query_status(execution_id)
            except Exception:
                # Don't leave an UNLOAD running (and billing) that nobody is watching.
                self.athena.cancel_query(execution_id)
                raise
            state = status['state']
            if state == 'SUCCEEDED':
                return execution_id
            if state in ('FAILED', 'CANCELLED'):
                reason = status.get('error', 'Unknown')
                raise RuntimeError(f"UNLOAD {execution_id} {state}: {reason}")
            if time.monotonic() >= deadline:
                self.athena.cancel_query(execution_id)
                raise TimeoutError(f"UNLOAD {execution_id} still {state} after {max_poll_seconds:.0f}s; cancelled")
            time.sleep(POLL_INTERVAL_SECONDS)

    def publish_staged(self, staging_prefix: str, live_prefix: str) -> Tuple[int, int]:
        """Replace the live month prefix with the staged UNLOAD output.

        Copy first, under run-id-prefixed names that cannot collide with the
        current objects, then delete the old objects. A failed copy rolls back
        the new copies and leaves the old month as it was.
        shortcut: readers can briefly see old + new rows (double cost) between
        the last copy and the old-key delete; switch the table location per run
        if that window ever matters.
        """
        staged = self.list_keys(staging_prefix)
        old_keys = self.list_keys(live_prefix)
        run_id = staging_prefix.rstrip('/').rsplit('/', 1)[-1]
        published = []
        try:
            for key in staged:
                new_key = f"{live_prefix}{run_id}_{key[len(staging_prefix):]}"
                # Recorded first: a copy can complete server-side and still raise,
                # so rollback must cover it (deleting a missing key is a no-op).
                published.append(new_key)
                self.s3.copy_object(
                    Bucket=self.bucket,
                    Key=new_key,
                    CopySource={'Bucket': self.bucket, 'Key': key},
                )
        except Exception:
            try:
                self.delete_keys(published, live_prefix)
            except Exception as cleanup_error:
                logger.warning(f"Could not roll back {len(published)} copies under {live_prefix}: {cleanup_error}")
            raise
        published_set = set(published)
        deleted = self.delete_keys([k for k in old_keys if k not in published_set], live_prefix)
        self.delete_keys(staged, staging_prefix)
        return deleted, len(staged)

    def allocate_month(self, year: int, month: int, max_poll_seconds: float) -> Dict[str, Any]:
        year, month = _validate_year_month(year, month)
        select_sql = build_allocation_select(
            self.analytics_database, self.cur_database, self.cur_table,
            self.schema_profile(), year, month, cur_catalog=self.cur_catalog,
        )
        prefix = f"{OUTPUT_PREFIX}/year={year}/month={month}/"
        staging_prefix = f"{STAGING_PREFIX}/year={year}/month={month}/{uuid.uuid4().hex}/"
        target = f"s3://{self.bucket}/{prefix}"
        staging_target = f"s3://{self.bucket}/{staging_prefix}"

        unload_sql = f"""UNLOAD ({select_sql})
TO '{staging_target}'
WITH (format = 'PARQUET', compression = 'SNAPPY')"""  # nosec B608 - SELECT built by build_allocation_select from validated identifiers; target from env bucket + validated int year/month + uuid hex

        try:
            execution_id = self.run_unload(unload_sql, max_poll_seconds)
            deleted, published = self.publish_staged(staging_prefix, prefix)
        except Exception:
            # Drop staged output without masking the error: the staging lifecycle
            # rule only expires noncurrent versions, so leftovers would stay forever.
            try:
                self.clear_prefix(staging_prefix)
            except Exception as cleanup_error:
                logger.warning(f"Could not clean {staging_target}: {cleanup_error}")
            raise

        result = {'year': year, 'month': month, 'status': 'succeeded', 'query_execution_id': execution_id,
                  'objects_deleted': deleted, 'objects_published': published, 'location': target}
        logger.info(f"Cost allocation month complete: {result}")
        return result




def _max_poll_seconds(context) -> float:
    remaining_ms = getattr(context, 'get_remaining_time_in_millis', None)
    if callable(remaining_ms):
        budget = remaining_ms() / 1000 - TIMEOUT_BUFFER_SECONDS
        # No floor: a later month started inside the reserve would publish with
        # too little time left, or be killed mid-poll with its UNLOAD unwatched.
        if budget < POLL_INTERVAL_SECONDS:
            raise TimeoutError(f"{budget + TIMEOUT_BUFFER_SECONDS:.0f}s left is within the "
                               f"{TIMEOUT_BUFFER_SECONDS}s publish reserve; month not started")
        return budget
    return DEFAULT_MAX_POLL_SECONDS


def lambda_handler(event, context):
    """Recompute tag cost allocation for the requested (or default) months."""
    event = event or {}
    logger.info(f"Cost Allocator started: {json.dumps(event, default=str)}")

    months = months_to_compute(event)

    if not (os.environ.get('CUR_ATHENA_DATABASE', '').strip() and os.environ.get('CUR_ATHENA_TABLE', '').strip()):
        logger.warning("CUR_ATHENA_DATABASE/CUR_ATHENA_TABLE not configured; skipping cost allocation")
        return {'status': 'skipped', 'reason': 'cur_not_configured', 'months': []}

    allocator = CostAllocator()
    results = []
    for year, month in months:
        results.append(allocator.allocate_month(year, month, _max_poll_seconds(context)))

    return {'status': 'success', 'months': results}

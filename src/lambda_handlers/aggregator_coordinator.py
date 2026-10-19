"""
TagInsights - Aggregator Coordinator Lambda

Responsibilities:
  1. Discover all AWS accounts in the org (via Organizations API, falling back
     to Resource Explorer index list if not the management account).
  2. Read the scan checkpoint from S3 to determine which account+regions have
     already been processed in the current scan cycle.
  3. Process account+regions sequentially, invoking the Worker Lambda for each.
  4. Save checkpoint progress to S3 before the Lambda approaches timeout so
     the next scheduled invocation can resume where it left off.
  5. If the checkpoint shows the scan has been running for more than MAX_SCAN_DAYS,
     switch to parallel fan-out mode: invoke all remaining workers concurrently.

Scan cycle:
  - A "scan cycle" is identified by a scan_id (date string: YYYY-MM-DD).
  - Each daily invocation either starts a new cycle (new scan_id) or resumes
    an in-progress one (same scan_id, different day).
  - A cycle is complete when all account+regions are marked done.
  - On completion, the checkpoint is deleted and a completion marker is written.

Cost:
  - 1 Lambda invocation/day (512MB, up to 15 min) = ~$0.11/month fixed cost.
  - Fan-out invocations are rare (first run only for most customers) and
    bounded by the number of account+regions.
"""

import json
import gzip
import os
import sys
import logging
import time
import hmac
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'WARNING'))

# ── Environment ───────────────────────────────────────────────────────────────
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'prod')
HUB_REGION = os.environ.get('HUB_REGION', os.environ.get('AWS_REGION', 'us-east-1'))
CENTRAL_ANALYTICS_BUCKET = os.environ.get('CENTRAL_ANALYTICS_BUCKET', '')
WORKER_FUNCTION_NAME = os.environ.get('WORKER_FUNCTION_NAME', '')
MAX_SCAN_DAYS = int(os.environ.get('MAX_SCAN_DAYS', '3'))

# Safety buffer: stop processing and save checkpoint with this many ms remaining
TIMEOUT_BUFFER_MS = 120_000  # 2 minutes

# S3 keys
CHECKPOINT_KEY = 'aggregator-state/scan-checkpoint.json'
COMPLETION_KEY_PREFIX = 'aggregator-state/completed/'

# ── AWS clients ───────────────────────────────────────────────────────────────
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda', region_name=HUB_REGION)


# ── Checkpoint helpers + HMAC integrity ───────────────────────────

_cached_signing_secret: Optional[str] = None


def _checkpoint_signature_secret() -> str:
    """
    Retrieve checkpoint signing secret from CHECKPOINT_SIGNING_SECRET env var.
    Set via CloudFormation parameter (generate with 'openssl rand -hex 32').
    """
    global _cached_signing_secret
    if _cached_signing_secret:
        return _cached_signing_secret
    secret = os.environ.get('CHECKPOINT_SIGNING_SECRET')
    if not secret:
        raise RuntimeError("CHECKPOINT_SIGNING_SECRET environment variable is required")
    _cached_signing_secret = secret
    return secret


def _generate_checkpoint_signature(checkpoint: Dict) -> str:
    """
    Generate HMAC-SHA256 signature of checkpoint content.
    Signs all fields except 'signature' itself for determinism.
    """
    content = {k: v for k, v in sorted(checkpoint.items()) if k != 'signature'}
    canonical = json.dumps(content, sort_keys=True)
    secret = _checkpoint_signature_secret()
    return hmac.new(secret.encode('utf-8'), canonical.encode('utf-8'), hashlib.sha256).hexdigest()


def _validate_checkpoint_signature(checkpoint: Dict) -> bool:
    """
    Validate checkpoint HMAC signature to detect tampering.
    Returns True if signature is valid, False if missing or tampered.
    Unsigned checkpoints are rejected — they must be re-signed on first run.
    """
    stored_sig = checkpoint.get('signature')
    if not stored_sig:
        logger.warning("Checkpoint has no signature — rejecting (unsigned checkpoints not accepted)")
        emit_security_metric('CheckpointUnsignedRejected', 1)
        return False
    expected_sig = _generate_checkpoint_signature(checkpoint)
    valid = hmac.compare_digest(stored_sig, expected_sig)
    if not valid:
        logger.error("Checkpoint signature INVALID — potential tampering detected!")
        emit_security_metric('CheckpointTamperingDetected', 1)
    return valid


def emit_security_metric(metric_name: str, value: float):
    """Emit CloudWatch security metric (non-fatal if fails)."""
    try:
        cw = boto3.client('cloudwatch')
        cw.put_metric_data(
            Namespace='TagInsights/Security',
            MetricData=[{
                'MetricName': metric_name,
                'Value': value,
                'Unit': 'Count'
            }]
        )
    except Exception:
        pass  # Non-fatal


def read_checkpoint() -> Optional[Dict]:
    """
    Read the current scan checkpoint from S3.
    Validates HMAC signature to detect tampering.
    Returns None if no checkpoint exists or if tampered.
    """
    try:
        response = s3.get_object(Bucket=CENTRAL_ANALYTICS_BUCKET, Key=CHECKPOINT_KEY)
        checkpoint = json.loads(response['Body'].read())

        # Validate checkpoint integrity
        if not _validate_checkpoint_signature(checkpoint):
            logger.error(
                "Checkpoint signature validation FAILED — treating checkpoint as corrupted. "
                "This may indicate tampering. Starting fresh scan."
            )
            emit_security_metric('CheckpointTamperingDetected', 1)
            # Delete tampered checkpoint and start fresh
            try:
                s3.delete_object(Bucket=CENTRAL_ANALYTICS_BUCKET, Key=CHECKPOINT_KEY)
            except Exception:
                pass
            return None

        return checkpoint
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as e:
        logger.warning(f"Error reading checkpoint: {e}")
        return None


def write_checkpoint(checkpoint: Dict):
    """
    Persist checkpoint to S3 with HMAC signature.
    Signature allows detection of tampering on next read.
    """
    try:
        # Sign checkpoint before writing
        checkpoint_with_sig = {**checkpoint}
        checkpoint_with_sig['signature'] = _generate_checkpoint_signature(checkpoint)

        s3.put_object(
            Bucket=CENTRAL_ANALYTICS_BUCKET,
            Key=CHECKPOINT_KEY,
            Body=json.dumps(checkpoint_with_sig),
            ContentType='application/json'
        )
        logger.info(f"Checkpoint saved (signed): {checkpoint['completed_count']}/{checkpoint['total_count']} account-regions done")
    except Exception as e:
        logger.error(f"Failed to write checkpoint: {e}")
        raise


def delete_checkpoint():
    """Remove the checkpoint once a scan cycle completes."""
    try:
        s3.delete_object(Bucket=CENTRAL_ANALYTICS_BUCKET, Key=CHECKPOINT_KEY)
        logger.info("Scan checkpoint deleted (cycle complete)")
    except Exception as e:
        logger.warning(f"Error deleting checkpoint: {e}")


def write_completion_marker(scan_id: str, stats: Dict):
    """Write a completion record so operators can audit scan history."""
    key = f"{COMPLETION_KEY_PREFIX}{scan_id}.json"
    try:
        s3.put_object(
            Bucket=CENTRAL_ANALYTICS_BUCKET,
            Key=key,
            Body=json.dumps({**stats, 'scan_id': scan_id, 'completed_at': datetime.utcnow().isoformat()}),
            ContentType='application/json'
        )
        logger.info(f"Completion marker written: {key}")
    except Exception as e:
        logger.warning(f"Could not write completion marker: {e}")


# ── Account discovery ─────────────────────────────────────────────────────────

def list_accounts_from_organizations() -> List[str]:
    """
    List all ACTIVE account IDs in the AWS Organization.
    Requires the hub account to be the management account or a delegated admin.
    """
    orgs = boto3.client('organizations')
    accounts = []
    paginator = orgs.get_paginator('list_accounts')
    for page in paginator.paginate():
        for account in page['Accounts']:
            if account['Status'] == 'ACTIVE':
                accounts.append(account['Id'])
    logger.info(f"Organizations API: discovered {len(accounts)} active accounts")
    return accounts


def list_accounts_from_resource_explorer() -> List[str]:
    """
    Fallback: extract unique account IDs from Resource Explorer index list.
    Works even if the hub account is not the management account.
    RE aggregator index stores local indexes from all member accounts.
    """
    re = boto3.client('resource-explorer-2', region_name=HUB_REGION)
    account_ids = set()
    try:
        response = re.list_indexes()
        for index in response.get('Indexes', []):
            # Index ARN format: arn:aws:resource-explorer-2:REGION:ACCOUNT:index/TYPE
            parts = index.get('Arn', '').split(':')
            if len(parts) > 4 and parts[4]:
                account_ids.add(parts[4])
    except Exception as e:
        logger.warning(f"RE fallback account discovery failed: {e}")
    logger.info(f"Resource Explorer fallback: discovered {len(account_ids)} accounts")
    return list(account_ids)


def discover_accounts() -> List[str]:
    """
    Discover all accounts. Tries Organizations first, falls back to RE indexes.
    """
    try:
        accounts = list_accounts_from_organizations()
        if accounts:
            return accounts
    except ClientError as e:
        code = e.response['Error']['Code']
        if code in ('AccessDeniedException', 'AWSOrganizationsNotInUseException'):
            logger.info(f"Organizations API not available ({code}), falling back to RE index list")
        else:
            logger.warning(f"Organizations API error: {e}")

    # Fallback: extract accounts from RE indexes
    return list_accounts_from_resource_explorer()


def get_active_regions_for_account(account_id: str) -> List[str]:
    """
    Return the list of regions that have a Resource Explorer local index for
    this account. These are the regions where RE has discovered resources.
    Only regions with a local index have data worth scanning.
    """
    re = boto3.client('resource-explorer-2', region_name=HUB_REGION)
    regions = []
    try:
        response = re.list_indexes()
        for index in response.get('Indexes', []):
            parts = index.get('Arn', '').split(':')
            if len(parts) > 4 and parts[4] == account_id:
                region = parts[3]
                if region:
                    regions.append(region)
    except Exception as e:
        logger.warning(f"Could not get regions for account {account_id}: {e}")
        # Fall back to common regions
        regions = [HUB_REGION]

    return regions if regions else [HUB_REGION]


def build_account_region_list(accounts: List[str]) -> List[Tuple[str, str]]:
    """
    Build the full list of (account_id, region) pairs to scan.

    Uses RE list_indexes to discover which regions the hub account has indexed.
    Then cross-products ALL org accounts (discovered via Organizations) with
    those regions — because member account indexes are not visible in the hub's
    list_indexes when org-level aggregation isn't explicitly linked, but the
    org-wide RE view still supports querying them via accountid: filter.
    """
    re = boto3.client('resource-explorer-2', region_name=HUB_REGION)
    account_regions = []

    # Step 1: Get hub-account regions from list_indexes
    hub_regions = []
    try:
        response = re.list_indexes()
        indexes = response.get('Indexes', [])

        for index in indexes:
            parts = index.get('Arn', '').split(':')
            if len(parts) > 4:
                region = parts[3]
                if region and region not in hub_regions:
                    hub_regions.append(region)

        logger.info(f"Discovered {len(hub_regions)} active regions from RE indexes")

    except Exception as e:
        logger.warning(f"Could not list RE indexes: {e}. Using hub region only.")
        hub_regions = [HUB_REGION]

    if not hub_regions:
        hub_regions = [HUB_REGION]

    # Step 2: Cross-product ALL org accounts × hub regions
    # The Worker uses the org-wide RE view (accountid:X region:Y filter) so
    # it correctly scopes results even for member accounts not visible in list_indexes
    for account_id in accounts:
        for region in hub_regions:
            account_regions.append((account_id, region))

    if not account_regions:
        logger.warning("No account-region pairs found. Scanning hub account in hub region only.")
        hub_account = boto3.client('sts').get_caller_identity()['Account']
        account_regions = [(hub_account, HUB_REGION)]

    logger.info(f"Built account-region list: {len(account_regions)} pairs across {len(set(a for a, _ in account_regions))} accounts")
    return account_regions


# ── Worker invocation ─────────────────────────────────────────────────────────

def invoke_worker(account_id: str, region: str, scan_id: str, async_mode: bool = False) -> bool:
    """
    Invoke the Worker Lambda for a specific account+region.

    async_mode=False: synchronous invoke (wait for result before proceeding).
      Used during sequential scan — coordinator waits for each worker.
    async_mode=True: asynchronous invoke (fire and forget).
      Used during fan-out mode — coordinator fires all workers in parallel.

    Returns True on success, False on error.
    """
    payload = {
        'account_id': account_id,
        'region': region,
        'scan_id': scan_id,
        'trigger_source': 'coordinator'
    }

    invocation_type = 'Event' if async_mode else 'RequestResponse'

    try:
        response = lambda_client.invoke(
            FunctionName=WORKER_FUNCTION_NAME,
            InvocationType=invocation_type,
            Payload=json.dumps(payload)
        )

        if async_mode:
            # For async invocations, 202 means accepted
            return response['StatusCode'] == 202

        # For sync invocations, check for function errors
        if response.get('FunctionError'):
            error_payload = json.loads(response['Payload'].read())
            logger.error(f"Worker error for {account_id}/{region}: {error_payload.get('errorMessage', 'unknown')}")
            return False

        return response['StatusCode'] == 200

    except Exception as e:
        logger.error(f"Failed to invoke worker for {account_id}/{region}: {e}")
        return False


# ── Scan modes ────────────────────────────────────────────────────────────────

def sequential_scan(
    account_regions: List[Tuple[str, str]],
    checkpoint: Dict,
    scan_id: str,
    context
) -> Dict:
    """
    Process account+regions one at a time. After each, check remaining Lambda time.
    If approaching timeout, save checkpoint and return early.

    Returns updated checkpoint dict.
    """
    completed = set(checkpoint.get('completed', []))
    errors = checkpoint.get('errors', [])
    processed_this_invocation = 0
    failed_this_invocation = 0

    for account_id, region in account_regions:
        pair_key = f"{account_id}/{region}"

        if pair_key in completed:
            continue  # Already done in a previous invocation

        # Check timeout before starting next worker
        remaining_ms = context.get_remaining_time_in_millis()
        if remaining_ms < TIMEOUT_BUFFER_MS:
            logger.info(f"Approaching timeout ({remaining_ms}ms remaining). Saving checkpoint.")
            break

        logger.info(f"Scanning {pair_key} ({len(completed)}/{len(account_regions)} done)")

        success = invoke_worker(account_id, region, scan_id, async_mode=False)

        if success:
            completed.add(pair_key)
            processed_this_invocation += 1
        else:
            errors.append({'pair': pair_key, 'timestamp': datetime.utcnow().isoformat()})
            failed_this_invocation += 1
            # Don't mark as completed — will retry on next invocation

        # Update checkpoint after each successful worker
        checkpoint['completed'] = list(completed)
        checkpoint['completed_count'] = len(completed)
        checkpoint['errors'] = errors
        checkpoint['last_updated'] = datetime.utcnow().isoformat()

        # Persist checkpoint every 10 completions to avoid losing progress
        if processed_this_invocation % 10 == 0:
            write_checkpoint(checkpoint)

    # Final checkpoint save for this invocation
    write_checkpoint(checkpoint)

    logger.info(
        f"Sequential scan invocation complete: "
        f"{processed_this_invocation} processed, {failed_this_invocation} failed, "
        f"{len(completed)}/{len(account_regions)} total done"
    )

    return checkpoint


def fanout_scan(
    remaining_pairs: List[Tuple[str, str]],
    scan_id: str
) -> Dict:
    """
    Fan-out mode: invoke all remaining workers asynchronously in parallel.
    Used when sequential scan has been running for more than MAX_SCAN_DAYS.

    Workers run in parallel bounded by Lambda's ReservedConcurrentExecutions (200).
    If more than 200 pairs, they'll queue up as Lambda scales.
    """
    logger.info(f"Fan-out mode: invoking {len(remaining_pairs)} workers in parallel")
    success_count = 0
    error_count = 0

    for account_id, region in remaining_pairs:
        success = invoke_worker(account_id, region, scan_id, async_mode=True)
        if success:
            success_count += 1
        else:
            error_count += 1
        # Brief pause to avoid Lambda invoke API throttling at scale
        if success_count % 50 == 0 and success_count > 0:
            time.sleep(0.5)

    logger.info(f"Fan-out dispatched: {success_count} workers invoked, {error_count} errors")
    return {'dispatched': success_count, 'errors': error_count}


# ── Main handler ──────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    """
    Coordinator entry point. Called by EventBridge schedule (daily).

    Flow:
      1. Read checkpoint from S3
      2. Determine if resuming an existing cycle or starting a new one
      3. If cycle is too old (> MAX_SCAN_DAYS), switch to fan-out for catch-up
      4. Sequential scan of remaining account+regions
      5. If timeout approaches, checkpoint and exit
      6. If all done, write completion marker and clean up checkpoint
    """
    logger.info(f"Coordinator started. Event: {event.get('trigger_source', 'unknown')}")
    start_time = datetime.utcnow()
    today = start_time.strftime('%Y-%m-%d')

    if not CENTRAL_ANALYTICS_BUCKET:
        raise ValueError("CENTRAL_ANALYTICS_BUCKET environment variable is not set")
    if not WORKER_FUNCTION_NAME:
        raise ValueError("WORKER_FUNCTION_NAME environment variable is not set")

    # ── Step 1: Read checkpoint ───────────────────────────────────────────────
    checkpoint = read_checkpoint()

    if checkpoint:
        scan_id = checkpoint.get('scan_id', today)
        logger.info(f"Resuming scan cycle: {scan_id} ({checkpoint.get('completed_count', 0)}/{checkpoint.get('total_count', '?')} done)")
    else:
        scan_id = today
        checkpoint = {
            'scan_id': scan_id,
            'started_at': start_time.isoformat(),
            'completed': [],
            'completed_count': 0,
            'total_count': 0,
            'errors': [],
            'last_updated': start_time.isoformat()
        }
        logger.info(f"Starting new scan cycle: {scan_id}")

    # ── Step 2: Discover account+regions ─────────────────────────────────────
    accounts = discover_accounts()
    if not accounts:
        logger.error("No accounts discovered. Check Organizations/RE permissions.")
        return {'status': 'error', 'message': 'No accounts discovered'}

    account_regions = build_account_region_list(accounts)
    total = len(account_regions)
    checkpoint['total_count'] = total

    # ── Step 3: Check scan cycle age ─────────────────────────────────────────
    scan_start = datetime.fromisoformat(checkpoint.get('started_at', start_time.isoformat()))
    scan_age_days = (start_time - scan_start).days

    completed_set = set(checkpoint.get('completed', []))
    remaining = [(a, r) for a, r in account_regions if f"{a}/{r}" not in completed_set]

    logger.info(
        f"Scan {scan_id}: {len(completed_set)}/{total} done, "
        f"{len(remaining)} remaining, age={scan_age_days}d (max={MAX_SCAN_DAYS}d)"
    )

    if not remaining:
        # All done — clean up
        stats = {
            'total_account_regions': total,
            'completed': len(completed_set),
            'errors': len(checkpoint.get('errors', [])),
            'duration_days': scan_age_days
        }
        write_completion_marker(scan_id, stats)
        delete_checkpoint()
        logger.info(f"Scan cycle {scan_id} complete: {total} account-regions scanned")
        return {'status': 'complete', 'scan_id': scan_id, **stats}

    # ── Step 4: Choose scan mode ──────────────────────────────────────────────
    if scan_age_days >= MAX_SCAN_DAYS and len(remaining) > 0:
        # Sequential scan is taking too long — fan out to catch up
        logger.warning(
            f"Scan cycle {scan_id} has been running for {scan_age_days} days "
            f"(MAX_SCAN_DAYS={MAX_SCAN_DAYS}). Switching to fan-out mode."
        )
        fanout_result = fanout_scan(remaining, scan_id)

        # Mark all as "in progress" in checkpoint — workers will complete independently
        # On the next daily invocation, we'll start a fresh cycle
        checkpoint['fanout_dispatched'] = True
        checkpoint['fanout_at'] = datetime.utcnow().isoformat()
        write_checkpoint(checkpoint)

        return {
            'status': 'fanout_dispatched',
            'scan_id': scan_id,
            'remaining': len(remaining),
            **fanout_result
        }

    # ── Step 5: Sequential scan ───────────────────────────────────────────────
    checkpoint = sequential_scan(account_regions, checkpoint, scan_id, context)

    # ── Step 6: Check if now complete ─────────────────────────────────────────
    completed_after = set(checkpoint.get('completed', []))
    remaining_after = [ar for ar in account_regions if f"{ar[0]}/{ar[1]}" not in completed_after]

    if not remaining_after:
        stats = {
            'total_account_regions': total,
            'completed': len(completed_after),
            'errors': len(checkpoint.get('errors', [])),
            'duration_days': scan_age_days
        }
        write_completion_marker(scan_id, stats)
        delete_checkpoint()
        logger.info(f"Scan cycle {scan_id} completed in this invocation: {total} account-regions")
        return {'status': 'complete', 'scan_id': scan_id, **stats}

    logger.info(
        f"Scan cycle {scan_id} partially complete: "
        f"{len(completed_after)}/{total} done. Will resume tomorrow."
    )
    return {
        'status': 'in_progress',
        'scan_id': scan_id,
        'completed': len(completed_after),
        'remaining': len(remaining_after),
        'total': total
    }

"""
TagInsights - Aggregator Worker Lambda

Responsibilities:
  1. Accept a scan job payload: {account_id, region, scan_id, trigger_source}
  2. Query Resource Explorer aggregator index for all resources in the given
     account+region (with tag data included).
  3. Compare current resources+tags against previous state stored in S3.
  4. Write only the delta (new resources, changed tags, removed resources) to S3.
  5. Update the S3 state file for this account+region.

Also handles real-time EventBridge events forwarded via SQS (HubAggregatorWithEvents
mode). In this case the payload is the original CloudTrail event, and only the
specific tagged resource is processed.

State storage:
  s3://{CENTRAL_ANALYTICS_BUCKET}/aggregator-state/account_id={account}/region={region}/latest.json.gz
  - Compressed JSON: {arn: {tag_key: tag_value, ...}, ...}
  - Read at scan start, written at scan end
  - If state file > STATE_SHARD_THRESHOLD resources, the worker logs a warning.
    Very large account+regions are not sharded automatically.

Delta storage:
  s3://{CENTRAL_ANALYTICS_BUCKET}/deltas/account_id={account}/region={region}/{timestamp}-{scan_id}.json
  - One file per worker invocation
  - Partitioned for Athena queries by account_id and region
"""

import json
import gzip
import os
import re
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'WARNING'))

# ── Environment ───────────────────────────────────────────────────────────────
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'prod')
HUB_REGION = os.environ.get('HUB_REGION', os.environ.get('AWS_REGION', 'us-east-1'))
CENTRAL_ANALYTICS_BUCKET = os.environ.get('CENTRAL_ANALYTICS_BUCKET', '')

# Log a warning if a single account+region state file exceeds this many resources.
STATE_SHARD_THRESHOLD = int(os.environ.get('STATE_SHARD_THRESHOLD', '50000'))

# ── AWS clients ───────────────────────────────────────────────────────────────
s3 = boto3.client('s3')
sqs = boto3.client('sqs')

# ── Environment ── SQS ───────────────────────────────────────────────────────
CENTRAL_RESULTS_QUEUE_URL = os.environ.get('CENTRAL_RESULTS_QUEUE_URL', '')

# ── Org view cache (fetched once per Lambda container lifetime) ───────────────
_ORG_VIEW_ARN: Optional[str] = None

# ── Per-account SQS rate limiter (persists across warm invocations) ───────
# Limits SQS notifications per account to prevent queue flooding from compromised account
_SQS_RATE_LIMITER: Dict[str, list] = {}  # {account_id: [timestamps]}
MAX_SQS_MESSAGES_PER_ACCOUNT_PER_MINUTE = int(
    os.environ.get('MAX_SQS_MESSAGES_PER_ACCOUNT_PER_MINUTE', '20')
)


def _is_sqs_rate_limited(account_id: str) -> bool:
    """
    Check if SQS notifications from this account are rate-limited.
    Uses in-memory sliding window: max N messages per account per 60 seconds.
    Resets on Lambda cold start (acceptable — limits burst within warm invocations).
    """
    now = time.time()
    window_start = now - 60  # 1-minute sliding window

    # Clean up expired timestamps
    timestamps = _SQS_RATE_LIMITER.get(account_id, [])
    timestamps = [t for t in timestamps if t > window_start]
    _SQS_RATE_LIMITER[account_id] = timestamps

    if len(timestamps) >= MAX_SQS_MESSAGES_PER_ACCOUNT_PER_MINUTE:
        logger.warning(
            f"RATE LIMIT: Account {account_id} exceeded "
            f"{MAX_SQS_MESSAGES_PER_ACCOUNT_PER_MINUTE} SQS messages/min. "
            f"Skipping notification to protect central queue."
        )
        try:
            cw = boto3.client('cloudwatch')
            cw.put_metric_data(
                Namespace='TagInsights/Security',
                MetricData=[{
                    'MetricName': 'SQSRateLimitExceeded',
                    'Value': 1,
                    'Unit': 'Count',
                    'Dimensions': [{'Name': 'AccountId', 'Value': account_id}]
                }]
            )
        except Exception:
            pass
        return True

    # Record this message
    _SQS_RATE_LIMITER[account_id] = timestamps + [now]
    return False


def notify_results_q(account_id: str, region: str, s3_key: str, changed_count: int):
    """
    Send an SQS notification to the central results queue after writing deltas.
    This triggers the Analytics Writer Lambda to convert the delta JSON to Parquet.

    Message format matches what Analytics Writer expects:
        {s3_bucket, s3_key, account_id, region, changed_count, timestamp}

    Per-account rate limiting prevents queue flooding from
    a compromised account that sends excessive delta notifications.
    """
    if not CENTRAL_RESULTS_QUEUE_URL:
        logger.warning("CENTRAL_RESULTS_QUEUE_URL not set — skipping SQS notification")
        return

    # Check per-account rate limit before sending
    if _is_sqs_rate_limited(account_id):
        logger.warning(
            f"SQS notification skipped for {account_id}/{region} (rate limited). "
            f"Delta is safely stored in S3 — Analytics Writer will process on next scan."
        )
        return

    try:
        message = {
            's3_bucket': CENTRAL_ANALYTICS_BUCKET,
            's3_key': s3_key,
            'account_id': account_id,
            'region': region,
            'changed_count': changed_count,
            'timestamp': int(time.time()),
            'source': 'aggregator_worker'
        }
        sqs.send_message(
            QueueUrl=CENTRAL_RESULTS_QUEUE_URL,
            MessageBody=json.dumps(message)
        )
        logger.info(f"Notified results queue: {changed_count} deltas in {s3_key}")
    except Exception as e:
        logger.error(f"Failed to notify results queue for {account_id}/{region}: {e}")
        # Don't raise — delta is written to S3, notification failure is non-fatal


def get_org_view_arn() -> Optional[str]:
    """
    Discover the org-scoped Resource Explorer view ARN.

    When AWS Organizations integration is enabled, an 'org' view is created
    that returns resources from ALL accounts in the org. Without specifying
    this view, search() uses the default account-scoped view and only returns
    hub account resources.

    Caches the result at module level so it's only fetched once per cold start.

    Returns:
        str: Org view ARN if found, None otherwise
    """
    global _ORG_VIEW_ARN
    if _ORG_VIEW_ARN is not None:
        return _ORG_VIEW_ARN

    try:
        re_client = boto3.client('resource-explorer-2', region_name=HUB_REGION)
        response = re_client.list_views()
        views = response.get('Views', [])
        # Look for a view with '/org/' in the ARN (org-wide view)
        for view_arn in views:
            if '/org/' in view_arn:
                logger.info(f"Found org-scoped RE view: {view_arn}")
                _ORG_VIEW_ARN = view_arn
                return _ORG_VIEW_ARN
        logger.info("No org-scoped RE view found — will use default view")
        return None
    except Exception as e:
        logger.warning(f"Could not list RE views: {e}")
        return None


# ── S3 state helpers ──────────────────────────────────────────────────────────

def state_key(account_id: str, region: str) -> str:
    return f"aggregator-state/account_id={account_id}/region={region}/latest.json.gz"


def delta_key(account_id: str, region: str, scan_id: str) -> str:
    timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')
    return f"deltas/account_id={account_id}/region={region}/{timestamp}-{scan_id}.json"


def read_state(account_id: str, region: str) -> Dict[str, Dict]:
    """
    Read the previous tag state for an account+region from S3.
    Returns empty dict on first run (no previous state).
    """
    key = state_key(account_id, region)
    try:
        response = s3.get_object(Bucket=CENTRAL_ANALYTICS_BUCKET, Key=key)
        compressed = response['Body'].read()
        state = json.loads(gzip.decompress(compressed))
        logger.info(f"Loaded state for {account_id}/{region}: {len(state)} resources")
        return state
    except s3.exceptions.NoSuchKey:
        logger.info(f"No previous state for {account_id}/{region} — first scan")
        return {}
    except Exception as e:
        logger.warning(f"Error reading state for {account_id}/{region}: {e}")
        return {}


def write_state(account_id: str, region: str, state: Dict[str, Dict]):
    """
    Write the current tag state for an account+region to S3 (compressed).
    """
    key = state_key(account_id, region)
    try:
        compressed = gzip.compress(json.dumps(state).encode('utf-8'), compresslevel=6)
        s3.put_object(
            Bucket=CENTRAL_ANALYTICS_BUCKET,
            Key=key,
            Body=compressed,
            ContentType='application/json',
            ContentEncoding='gzip'
        )

        resource_count = len(state)
        logger.info(f"Wrote state for {account_id}/{region}: {resource_count} resources")

        if resource_count > STATE_SHARD_THRESHOLD:
            logger.warning(
                f"State file for {account_id}/{region} has {resource_count} resources "
                f"(threshold={STATE_SHARD_THRESHOLD}). Consider sharding this account+region."
            )
    except Exception as e:
        logger.error(f"Error writing state for {account_id}/{region}: {e}")
        raise


def write_deltas(account_id: str, region: str, scan_id: str, deltas: List[Dict]) -> str:
    """
    Write delta records to S3. Returns the S3 key.
    """
    key = delta_key(account_id, region, scan_id)
    try:
        s3.put_object(
            Bucket=CENTRAL_ANALYTICS_BUCKET,
            Key=key,
            Body=json.dumps(deltas),
            ContentType='application/json'
        )
        logger.info(f"Wrote {len(deltas)} deltas to {key}")
        return key
    except Exception as e:
        logger.error(f"Error writing deltas for {account_id}/{region}: {e}")
        raise


# ── Resource Explorer discovery ───────────────────────────────────────────────

def discover_resources_for_account_region(
    account_id: str,
    region: str
) -> Dict[str, Dict]:
    """
    Query Resource Explorer aggregator index for all resources in the given
    account+region. Returns {arn: {tag_key: tag_value}} dict.

    Uses the Search API with account and region filters so the aggregator
    index returns only resources scoped to this account+region pair.
    This keeps each worker bounded and avoids loading the full org inventory.
    """
    re_client = boto3.client('resource-explorer-2', region_name=HUB_REGION)
    resources = {}

    # RE Search query: filter by account and region
    # RE query syntax: accountid:{account} region:{region}
    query_string = f"accountid:{account_id} region:{region}"

    try:
        paginator = re_client.get_paginator('search')
        page_count = 0

        # Use org-wide aggregator view if available, otherwise use default
        org_view_arn = get_org_view_arn()
        search_kwargs = {'QueryString': query_string}
        if org_view_arn:
            search_kwargs['ViewArn'] = org_view_arn

        for page in paginator.paginate(**search_kwargs):
            page_count += 1
            for resource in page.get('Resources', []):
                arn = resource.get('Arn', '')
                if not arn:
                    continue

                # Extract tags from RE response
                # RE returns tags as a list of {Key, Value} dicts under Properties
                tags = {}
                for prop in resource.get('Properties', []):
                    if prop.get('Name') == 'tags':
                        for tag in prop.get('Data', []):
                            if isinstance(tag, dict) and 'Key' in tag and 'Value' in tag:
                                tags[tag['Key']] = tag['Value']

                resources[arn] = tags

        logger.info(
            f"RE discovery for {account_id}/{region}: "
            f"{len(resources)} resources across {page_count} pages"
        )

    except re_client.exceptions.UnauthorizedException as e:
        logger.error(f"RE not enabled or no aggregator index for {account_id}/{region}: {e}")
    except Exception as e:
        logger.error(f"RE discovery failed for {account_id}/{region}: {e}")

    return resources


# ── Delta computation ─────────────────────────────────────────────────────────

def compute_deltas(
    account_id: str,
    region: str,
    scan_id: str,
    current: Dict[str, Dict],
    previous: Dict[str, Dict]
) -> List[Dict]:
    """
    Compare current resource state against previous state.
    Returns list of delta records for:
      - New resources (in current but not previous)
      - Changed tags (in both but tags differ)
      - Removed resources (in previous but not current)
    """
    deltas = []
    now = int(time.time())

    # New and changed resources
    for arn, current_tags in current.items():
        previous_tags = previous.get(arn)

        if previous_tags is None:
            # New resource
            deltas.append({
                'arn': arn,
                'resource_type': extract_service_from_arn(arn),
                'tags': current_tags,
                'previous_tags': {},
                'change_type': 'new_resource',
                'timestamp': now,
                'source_system': 'aggregator_worker',
                'account_id': account_id,
                'region': region,
                'scan_id': scan_id
            })
        elif current_tags != previous_tags:
            # Tags changed
            deltas.append({
                'arn': arn,
                'resource_type': extract_service_from_arn(arn),
                'tags': current_tags,
                'previous_tags': previous_tags,
                'change_type': 'tags_changed',
                'timestamp': now,
                'source_system': 'aggregator_worker',
                'account_id': account_id,
                'region': region,
                'scan_id': scan_id
            })

    # Removed resources
    for arn in previous:
        if arn not in current:
            deltas.append({
                'arn': arn,
                'resource_type': extract_service_from_arn(arn),
                'tags': {},
                'previous_tags': previous[arn],
                'change_type': 'resource_removed',
                'timestamp': now,
                'source_system': 'aggregator_worker',
                'account_id': account_id,
                'region': region,
                'scan_id': scan_id
            })

    return deltas


def extract_service_from_arn(arn: str) -> str:
    """Extract service name from ARN (e.g. 'ec2', 'lambda', 's3')."""
    try:
        parts = arn.split(':')
        return parts[2] if len(parts) > 2 else 'unknown'
    except Exception:
        return 'unknown'


# ── Real-time event processing ────────────────────────────────────────────────

def _extract_tag_mutation(detail: Dict) -> Tuple[Dict[str, str], List[str]]:
    """Extract tag additions/updates and removals from a CloudTrail event."""
    request_params = detail.get('requestParameters', {}) or {}
    event_name = detail.get('eventName', '')
    is_removal = (
        event_name.startswith('UntagResource')
        or event_name in {'DeleteTags', 'RemoveTagsFromResource'}
    )
    tag_updates: Dict[str, str] = {}
    removed_tag_keys = set()

    tags = request_params.get('tags')
    if isinstance(tags, list):
        # DynamoDB, RDS, ELB, ...: [{"key": "Team", "value": "blue"}] (or Key/Value)
        tags = {
            item.get('key', item.get('Key')): item.get('value', item.get('Value', ''))
            for item in tags
            if isinstance(item, dict) and item.get('key', item.get('Key'))
        }
    if isinstance(tags, dict):
        if is_removal:
            removed_tag_keys.update(tags)
        else:
            tag_updates.update(
                (str(key), str(value)) for key, value in tags.items()
            )

    tag_keys = request_params.get('tagKeys') or request_params.get('tagKeyList')
    if isinstance(tag_keys, list):
        for item in tag_keys:
            key = item.get('key') if isinstance(item, dict) else item
            if key:
                removed_tag_keys.add(str(key))

    tag_set = request_params.get('tagSet', {})
    if isinstance(tag_set, dict):
        for item in tag_set.get('items', []):
            if not isinstance(item, dict):
                continue
            key = item.get('key')
            if not key:
                continue
            if is_removal:
                removed_tag_keys.add(str(key))
            elif item.get('value') is not None:
                tag_updates[str(key)] = str(item['value'])

    return tag_updates, sorted(removed_tag_keys)


def extract_resources_from_cloudtrail_event(detail: Dict) -> List[Dict]:
    """
    Extract resource ARNs and tag mutations from a CloudTrail event detail.
    Handles the most common tag operation patterns across AWS services.
    Returns list of {arn, tags, removed_tag_keys} dicts.
    """
    arns = set()

    account_id = detail.get('userIdentity', {}).get('accountId', '')
    region = detail.get('awsRegion', HUB_REGION)
    request_params = detail.get('requestParameters', {}) or {}
    tag_updates, removed_tag_keys = _extract_tag_mutation(detail)

    # Pattern 1: resourceArn or resource (Lambda uses 'resource')
    for key in ('resourceArn', 'resource'):
        arn = request_params.get(key, '')
        if arn and arn.startswith('arn:aws:'):
            arns.add(arn)

    # Pattern 2: resources list
    for res in request_params.get('resources', []):
        if isinstance(res, str) and res.startswith('arn:aws:'):
            arns.add(res)
        elif isinstance(res, dict):
            arn = res.get('resourceArn') or res.get('arn', '')
            if arn and arn.startswith('arn:aws:'):
                arns.add(arn)

    # Pattern 3: EC2 resourcesSet (CreateTags, DeleteTags)
    resources_set = request_params.get('resourcesSet', {})
    if isinstance(resources_set, dict):
        for item in resources_set.get('items', []):
            resource_id = item.get('resourceId', '')
            if resource_id:
                arn = _construct_ec2_arn(resource_id, account_id, region)
                if arn:
                    arns.add(arn)

    # Pattern 4: S3 bucket
    bucket = request_params.get('bucketName', '')
    if bucket:
        arns.add(f"arn:aws:s3:::{bucket}")

    # Pattern 5: SQS queue URL (TagQueue, UntagQueue)
    queue_match = re.match(
        r'https://sqs\.([a-z0-9-]+)\.amazonaws\.com/(\d{12})/([^/?]+)$',
        request_params.get('queueUrl', ''),
    )
    if queue_match:
        q_region, q_account, q_name = queue_match.groups()
        arns.add(f"arn:aws:sqs:{q_region}:{q_account}:{q_name}")

    # Fallback: regex scan of entire event
    if not arns:
        event_str = json.dumps(detail)
        found = re.findall(r'arn:aws:[a-zA-Z0-9\-]+:[a-zA-Z0-9\-]*:[0-9]*:[^\s"\']+', event_str)
        for arn in found:
            if arn.startswith('arn:aws:'):
                arns.add(arn)

    return [
        {
            'arn': arn,
            'tags': tag_updates,
            'removed_tag_keys': removed_tag_keys,
        }
        for arn in arns
    ]


def _construct_ec2_arn(resource_id: str, account_id: str, region: str) -> Optional[str]:
    """Map EC2 resource IDs to ARNs."""
    if resource_id.startswith('arn:aws:'):
        return resource_id
    patterns = {
        'i-': f"arn:aws:ec2:{region}:{account_id}:instance/{resource_id}",
        'vol-': f"arn:aws:ec2:{region}:{account_id}:volume/{resource_id}",
        'snap-': f"arn:aws:ec2:{region}:{account_id}:snapshot/{resource_id}",
        'ami-': f"arn:aws:ec2:{region}:{account_id}:image/{resource_id}",
        'sg-': f"arn:aws:ec2:{region}:{account_id}:security-group/{resource_id}",
        'vpc-': f"arn:aws:ec2:{region}:{account_id}:vpc/{resource_id}",
        'subnet-': f"arn:aws:ec2:{region}:{account_id}:subnet/{resource_id}",
        'eni-': f"arn:aws:ec2:{region}:{account_id}:network-interface/{resource_id}",
    }
    for prefix, arn in patterns.items():
        if resource_id.startswith(prefix):
            return arn
    return None


def process_real_time_event(event: Dict) -> Dict:
    """
    Process a single forwarded CloudTrail tag change event.
    Updates the state for only the affected resources (not a full scan).
    """
    detail = event.get('detail', {})
    event_name = detail.get('eventName', 'unknown')
    region = detail.get('awsRegion', HUB_REGION)
    account_id = detail.get('userIdentity', {}).get('accountId', '')

    if not account_id:
        logger.warning(f"Could not extract account_id from event: {event_name}")
        return {'processed': 0, 'changed': 0}

    resources = extract_resources_from_cloudtrail_event(detail)
    if not resources:
        logger.warning(f"No resources extracted from event: {event_name}")
        return {'processed': 0, 'changed': 0}

    # Read current state for this account+region
    previous_state = read_state(account_id, region)

    deltas = []
    now = int(time.time())

    for resource in resources:
        arn = resource['arn']
        tag_updates = resource.get('tags', {})
        removed_tag_keys = resource.get('removed_tag_keys', [])
        if not tag_updates and not removed_tag_keys:
            logger.info(
                f"No explicit tag mutation in {event_name} for {arn}; "
                "periodic scan will reconcile state"
            )
            continue

        previous_tags = previous_state.get(arn, {})
        current_tags = dict(previous_tags)
        current_tags.update(tag_updates)
        for key in removed_tag_keys:
            current_tags.pop(key, None)

        is_new = arn not in previous_state and bool(current_tags)
        is_changed = arn in previous_state and current_tags != previous_tags

        if is_new or is_changed:
            change_type = 'new_resource' if is_new else event_name
            deltas.append({
                'arn': arn,
                'resource_type': extract_service_from_arn(arn),
                'tags': current_tags,
                'previous_tags': previous_tags,
                'change_type': change_type,
                'timestamp': now,
                'source_system': 'aggregator_worker_realtime',
                'account_id': account_id,
                'region': region,
                'scan_id': f"event_{event_name}_{now}"
            })
            previous_state[arn] = current_tags

    scan_id = f"event_{now}"
    if deltas:
        delta_s3_key = write_deltas(account_id, region, scan_id, deltas)
        notify_results_q(account_id, region, delta_s3_key, len(deltas))
        write_state(account_id, region, previous_state)

    return {
        'trigger': 'realtime_event',
        'event_name': event_name,
        'account_id': account_id,
        'region': region,
        'resources_in_event': len(resources),
        'deltas_written': len(deltas)
    }


# ── Full account+region scan ──────────────────────────────────────────────────

def process_account_region_scan(account_id: str, region: str, scan_id: str) -> Dict:
    """
    Full scan for one account+region:
      1. Discover all resources via RE aggregator (scoped to account+region)
      2. Read previous state
      3. Compute deltas
      4. Write deltas (if any)
      5. Write updated state
    """
    logger.info(f"Starting scan: {account_id}/{region} (scan_id={scan_id})")
    start = time.time()

    # Discover current resources
    current = discover_resources_for_account_region(account_id, region)

    if not current and account_id:
        # RE returned nothing — could be an empty account, or RE not enabled
        # Write empty state and return (don't fail the coordinator)
        logger.info(f"No resources found for {account_id}/{region} — skipping delta computation")
        write_state(account_id, region, {})
        return {
            'account_id': account_id,
            'region': region,
            'scan_id': scan_id,
            'total_resources': 0,
            'deltas': 0,
            'duration_seconds': round(time.time() - start, 1)
        }

    # Read previous state
    previous = read_state(account_id, region)

    # Compute deltas
    deltas = compute_deltas(account_id, region, scan_id, current, previous)

    # Write deltas (only if there are changes) and notify Analytics Writer via SQS
    if deltas:
        delta_s3_key = write_deltas(account_id, region, scan_id, deltas)
        notify_results_q(account_id, region, delta_s3_key, len(deltas))

    # Always write updated state (captures new resources even with no tag changes)
    write_state(account_id, region, current)

    duration = round(time.time() - start, 1)
    new_count = sum(1 for d in deltas if d['change_type'] == 'new_resource')
    changed_count = sum(1 for d in deltas if d['change_type'] == 'tags_changed')
    removed_count = sum(1 for d in deltas if d['change_type'] == 'resource_removed')

    logger.info(
        f"Scan complete: {account_id}/{region} — "
        f"{len(current)} resources, {len(deltas)} deltas "
        f"({new_count} new, {changed_count} changed, {removed_count} removed) "
        f"in {duration}s"
    )

    return {
        'account_id': account_id,
        'region': region,
        'scan_id': scan_id,
        'total_resources': len(current),
        'deltas': len(deltas),
        'new_resources': new_count,
        'changed_resources': changed_count,
        'removed_resources': removed_count,
        'duration_seconds': duration
    }


# ── Main handler ──────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    """
    Worker entry point. Called in two ways:

    1. Coordinator scan job (scheduled baseline/incremental scan):
       event = {'account_id': '123456789012', 'region': 'us-east-1',
                'scan_id': '2026-03-12', 'trigger_source': 'coordinator'}

    2. Real-time EventBridge event (via SQS from HubAggregatorWithEvents):
       event = {'Records': [{'body': '<CloudTrail event JSON>'}]}
       or directly: event = {'detail-type': 'AWS API Call via CloudTrail', ...}
    """
    if not CENTRAL_ANALYTICS_BUCKET:
        raise ValueError("CENTRAL_ANALYTICS_BUCKET environment variable is not set")

    # ── SQS batch of forwarded EventBridge events ─────────────────────────────
    if 'Records' in event:
        results = []
        for record in event['Records']:
            try:
                body = json.loads(record['body'])
                if body.get('detail-type') == 'AWS API Call via CloudTrail':
                    result = process_real_time_event(body)
                    results.append(result)
                else:
                    logger.warning(f"Unknown SQS message type: {list(body.keys())}")
            except Exception as e:
                logger.error(f"Error processing SQS record: {e}", exc_info=True)
                raise  # Re-raise so SQS can retry or route to DLQ

        total_deltas = sum(r.get('deltas_written', 0) for r in results)
        return {'processed': len(results), 'total_deltas': total_deltas}

    # ── Direct EventBridge event (real-time, non-SQS path) ───────────────────
    if event.get('detail-type') == 'AWS API Call via CloudTrail':
        return process_real_time_event(event)

    # ── Coordinator scan job ──────────────────────────────────────────────────
    account_id = event.get('account_id', '')
    region = event.get('region', HUB_REGION)
    scan_id = event.get('scan_id', datetime.utcnow().strftime('%Y-%m-%d'))

    if not account_id:
        raise ValueError("account_id is required in the event payload for coordinator scan jobs")

    return process_account_region_scan(account_id, region, scan_id)

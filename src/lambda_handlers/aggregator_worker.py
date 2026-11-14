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
  - Read at scan start, written at scan end with an S3 conditional write
    (If-Match the ETag read); a concurrent writer's changes are merged per ARN
  - If state file > STATE_SHARD_THRESHOLD resources, the worker logs a warning.
    Very large account+regions are not sharded automatically.

Delta storage:
  s3://{CENTRAL_ANALYTICS_BUCKET}/deltas/account_id={account}/region={region}/{timestamp}-{scan_id}-{random8}.json
  - Chunked: at most DELTA_CHUNK_ITEMS deltas / DELTA_CHUNK_BYTES per file,
    each file with its own SQS notification (Analytics Writer rejects >10 MB)
  - Partitioned for Athena queries by account_id and region
"""

import json
import gzip
import os
import re
import time
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'WARNING'))

# ── Environment ───────────────────────────────────────────────────────────────
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'prod')
HUB_REGION = os.environ.get('HUB_REGION', os.environ.get('AWS_REGION', 'us-east-1'))
CENTRAL_ANALYTICS_BUCKET = os.environ.get('CENTRAL_ANALYTICS_BUCKET', '')

# Log a warning if a single account+region state file exceeds this many resources.
STATE_SHARD_THRESHOLD = int(os.environ.get('STATE_SHARD_THRESHOLD', '50000'))

# ── AWS clients ───────────────────────────────────────────────────────────────
# Resource Explorer quotas are low and not adjustable (Search: 5 TPS). Adaptive
# retries back off client-side; Worker ReservedConcurrentExecutions caps how
# many containers share that budget.
# Limitation: per-container rate limiting only; add a shared token bucket if
# throttling still exhausts retries at high worker concurrency.
RE_CLIENT_CONFIG = Config(retries={'mode': 'adaptive', 'max_attempts': 10})

s3 = boto3.client('s3')
sqs = boto3.client('sqs')

# ── Environment ── SQS ───────────────────────────────────────────────────────
CENTRAL_RESULTS_QUEUE_URL = os.environ.get('CENTRAL_RESULTS_QUEUE_URL', '')

# Deltas per file. Kept well under the Analytics Writer limits (10 MB,
# 50,000 items) so a large scan never produces a file it would quarantine.
DELTA_CHUNK_ITEMS = 10_000
DELTA_CHUNK_BYTES = 8 * 1024 * 1024

# Optional explicit org view (stack parameter OrgViewArn). Empty = auto-discover.
ORG_VIEW_ARN = os.environ.get('ORG_VIEW_ARN', '')

# ── Caches (per Lambda container lifetime; None is never cached) ─────────────
_ORG_VIEW_ARN: Optional[str] = None
_HUB_ACCOUNT_ID: Optional[str] = None


def notify_results_q(account_id: str, region: str, s3_key: str, changed_count: int):
    """
    Send an SQS notification to the central results queue after writing deltas.
    This triggers the Analytics Writer Lambda to convert the delta JSON to Parquet.

    Message format matches what Analytics Writer expects:
        {s3_bucket, s3_key, account_id, region, changed_count, timestamp}

    Send failures raise: the caller has not yet advanced state, so the
    invocation fails and is retried instead of leaving a delta file that never
    reaches Athena. Volume is bounded by Worker concurrency, not a rate limiter.
    """
    if not CENTRAL_RESULTS_QUEUE_URL:
        logger.warning("CENTRAL_RESULTS_QUEUE_URL not set — skipping SQS notification")
        return

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


def get_org_view_arn() -> Optional[str]:
    """
    Return the org-scoped Resource Explorer view ARN (ORG_VIEW_ARN env var, or
    the first view whose ARN contains '/org/'), or None if there is none.

    Without it, ListResources uses the hub's default view, which only covers
    hub resources. API errors propagate; a found ARN is cached, None is not.
    """
    global _ORG_VIEW_ARN
    if ORG_VIEW_ARN:
        return ORG_VIEW_ARN
    if _ORG_VIEW_ARN:
        return _ORG_VIEW_ARN

    re_client = boto3.client('resource-explorer-2', region_name=HUB_REGION)
    for page in re_client.get_paginator('list_views').paginate():
        for view_arn in page.get('Views', []):
            if '/org/' in view_arn:
                logger.info(f"Found org-scoped RE view: {view_arn}")
                _ORG_VIEW_ARN = view_arn
                return _ORG_VIEW_ARN
    return None


def get_hub_account_id() -> str:
    """Account this Worker runs in (the hub). Cached."""
    global _HUB_ACCOUNT_ID
    if not _HUB_ACCOUNT_ID:
        _HUB_ACCOUNT_ID = boto3.client('sts').get_caller_identity()['Account']
    return _HUB_ACCOUNT_ID


# ── S3 state helpers ──────────────────────────────────────────────────────────

def state_key(account_id: str, region: str) -> str:
    return f"aggregator-state/account_id={account_id}/region={region}/latest.json.gz"


def delta_key(account_id: str, region: str, scan_id: str) -> str:
    # Random suffix: two writes in the same second (e.g. two real-time events in
    # one SQS batch) would otherwise share a key and the second would overwrite
    # the first, silently dropping a delta.
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d-%H-%M-%S')
    return f"deltas/account_id={account_id}/region={region}/{timestamp}-{scan_id}-{uuid.uuid4().hex[:8]}.json"


class StateConflict(Exception):
    """The state file changed since it was read (S3 conditional write refused)."""


class _ReadState(dict):
    """State as read from S3, plus the ETag it was read at (None = no object yet)."""
    etag: Optional[str] = None


# Attempts to merge and re-write after a concurrent writer replaced the state.
STATE_WRITE_ATTEMPTS = 5


def read_state(account_id: str, region: str) -> Dict[str, Dict]:
    """
    Read the previous tag state for an account+region from S3.
    Returns empty dict on first run (no previous state). The result remembers
    the object's ETag so save_state() can write it back conditionally.
    """
    key = state_key(account_id, region)
    try:
        response = s3.get_object(Bucket=CENTRAL_ANALYTICS_BUCKET, Key=key)
        compressed = response['Body'].read()
        state = _ReadState(json.loads(gzip.decompress(compressed)))
        state.etag = response.get('ETag')
        logger.info(f"Loaded state for {account_id}/{region}: {len(state)} resources")
        return state
    except s3.exceptions.NoSuchKey:
        logger.info(f"No previous state for {account_id}/{region} — first scan")
        return _ReadState()
    # Any other error propagates: treating an unreadable state as empty would
    # re-emit every resource as new_resource and corrupt the delta history.


def write_state(account_id: str, region: str, state: Dict[str, Dict], base: Optional[Dict] = None):
    """
    Write the current tag state for an account+region to S3 (compressed).

    base is the dict read_state() returned. When given, the put only succeeds
    if the object is still the one that was read (If-Match its ETag, or
    If-None-Match when it did not exist); otherwise StateConflict is raised.
    """
    key = state_key(account_id, region)
    condition = {}
    if isinstance(base, _ReadState):
        condition = {'IfMatch': base.etag} if base.etag else {'IfNoneMatch': '*'}
    try:
        compressed = gzip.compress(json.dumps(state).encode('utf-8'), compresslevel=6)
        try:
            s3.put_object(
                Bucket=CENTRAL_ANALYTICS_BUCKET,
                Key=key,
                Body=compressed,
                ContentType='application/json',
                ContentEncoding='gzip',
                **condition,
            )
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') in ('PreconditionFailed', 'ConditionalRequestConflict'):
                raise StateConflict(f"State for {account_id}/{region} changed since it was read") from e
            raise

        resource_count = len(state)
        logger.info(f"Wrote state for {account_id}/{region}: {resource_count} resources")

        if resource_count > STATE_SHARD_THRESHOLD:
            logger.warning(
                f"State file for {account_id}/{region} has {resource_count} resources "
                f"(threshold={STATE_SHARD_THRESHOLD}). Consider sharding this account+region."
            )
    except StateConflict:
        raise
    except Exception as e:
        logger.error(f"Error writing state for {account_id}/{region}: {e}")
        raise


def _merge_changes(base: Dict, ours: Dict, theirs: Dict, merge_values=None) -> Dict:
    """Three-way merge: keep the other writer's value unless we changed that key too."""
    merged = {}
    for key in set(base) | set(ours) | set(theirs):
        mine, other = ours.get(key), theirs.get(key)
        if mine == base.get(key):
            value = other
        elif merge_values and mine is not None and other is not None:
            value = merge_values(base.get(key) or {}, mine, other)
        else:
            value = mine
        if value is not None:
            merged[key] = value
    return merged


def _merge_state(base: Dict, ours: Dict, theirs: Dict, replaced=()) -> Dict:
    """
    Three-way merge per ARN, then per tag key when both writers changed the
    same ARN (e.g. two TagResource calls adding different keys). An ARN we
    removed stays removed.

    replaced: ARNs whose whole tag set we replaced (S3 PutBucketTagging /
    DeleteBucketTagging). We changed EVERY key on them, including keys we
    dropped, so our tag set wins as a whole and a concurrent writer cannot
    bring a dropped key back.
    """
    merged = _merge_changes(base, ours, theirs, merge_values=_merge_changes)
    for arn in replaced:
        if arn in ours:
            merged[arn] = ours[arn]
        else:
            merged.pop(arn, None)
    return merged


def save_state(account_id: str, region: str, state: Dict[str, Dict], base: Dict, replaced=()):
    """
    Write state that was computed from base (as read_state returned it).

    Scans and real-time events for one account+region run in parallel, so a
    plain put would drop whatever another writer saved in between. On a
    conflict, re-read, merge per ARN and retry. Deltas were already published
    against base, so they are not re-published. replaced: see _merge_state.
    """
    for _ in range(STATE_WRITE_ATTEMPTS):
        try:
            write_state(account_id, region, state, base)
            return
        except StateConflict:
            logger.warning(f"State for {account_id}/{region} changed concurrently; merging")
            theirs = read_state(account_id, region)
            state = _merge_state(base, state, theirs, replaced)
            base = theirs
    raise StateConflict(f"State for {account_id}/{region} kept changing; giving up")


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


def _chunk_deltas(deltas: List[Dict]) -> List[List[Dict]]:
    """Split deltas into files of <= DELTA_CHUNK_ITEMS items / ~DELTA_CHUNK_BYTES."""
    chunks, current, size = [], [], 0
    for delta in deltas:
        delta_size = len(json.dumps(delta)) + 2
        if current and (len(current) >= DELTA_CHUNK_ITEMS or size + delta_size > DELTA_CHUNK_BYTES):
            chunks.append(current)
            current, size = [], 0
        current.append(delta)
        size += delta_size
    if current:
        chunks.append(current)
    return chunks


def publish_deltas(account_id: str, region: str, scan_id: str, deltas: List[Dict]):
    """Write deltas in chunks, notifying the Analytics Writer once per file."""
    for chunk in _chunk_deltas(deltas):
        key = write_deltas(account_id, region, scan_id, chunk)
        notify_results_q(account_id, region, key, len(chunk))


# ── Resource Explorer discovery ───────────────────────────────────────────────

def discover_resources_for_account_region(
    account_id: str,
    region: str
) -> Dict[str, Dict]:
    """
    List every resource in one account+region from the Resource Explorer
    aggregator index. Returns {arn: {tag_key: tag_value}}.

    Uses ListResources, not Search: Search returns at most 1,000 results per
    query even when paginated, which silently truncates larger account+regions.

    Raises on any API error. A partial or empty inventory must never be
    written as state, or the next delta would report live resources as removed.
    """
    re_client = boto3.client('resource-explorer-2', region_name=HUB_REGION, config=RE_CLIENT_CONFIG)
    resources = {}

    list_kwargs = {
        'Filters': {'FilterString': f"accountid:{account_id} region:{region}"},
        'MaxResults': 1000,
    }
    org_view_arn = get_org_view_arn()
    if org_view_arn:
        list_kwargs['ViewArn'] = org_view_arn
    elif account_id != get_hub_account_id():
        # The default view only covers the hub: a member scan would return
        # nothing and record every member resource as removed.
        raise RuntimeError(
            f"No org-scoped Resource Explorer view found in {HUB_REGION}; cannot scan "
            f"member account {account_id}. Create an org view (or set OrgViewArn)."
        )

    page_count = 0
    for page in re_client.get_paginator('list_resources').paginate(**list_kwargs):
        page_count += 1
        for resource in page.get('Resources', []):
            arn = resource.get('Arn', '')
            if arn:
                resources[arn] = extract_tags_from_re_resource(resource)

    logger.info(
        f"RE discovery for {account_id}/{region}: "
        f"{len(resources)} resources across {page_count} pages"
    )
    return resources


def extract_tags_from_re_resource(resource: Dict) -> Dict[str, str]:
    """Tags are inline under Properties[Name=tags].Data when the view includes tags."""
    tags = {}
    for prop in resource.get('Properties', []):
        if prop.get('Name') == 'tags':
            for tag in prop.get('Data', []):
                if isinstance(tag, dict) and 'Key' in tag and 'Value' in tag:
                    tags[tag['Key']] = tag['Value']
    return tags


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

def _unwrap_items(value):
    """CloudFront wraps tag lists as {"items": [...]} (API types Tags, TagKeys)."""
    if isinstance(value, dict) and len(value) == 1 and isinstance(value.get('items'), list):
        return value['items']
    return value


def _tag_pair(item: Dict) -> Tuple[Optional[str], Optional[str]]:
    """(key, value) from key/value, Key/Value or tagKey/tagValue (KMS); value None if absent."""
    for key_name, value_name in (('key', 'value'), ('Key', 'Value'), ('tagKey', 'tagValue')):
        if item.get(key_name):
            return str(item[key_name]), item.get(value_name)
    return None, None


def _extract_tag_mutation(detail: Dict) -> Tuple[Dict[str, str], List[str], bool, Dict[str, str]]:
    """
    Extract tag changes from a CloudTrail event.

    Returns (tag_updates, removed_tag_keys, replace, removed_if_value).
    replace: the event sets the whole tag set (S3 PutBucketTagging /
    DeleteBucketTagging), so tag_updates is the new full set and every other
    key is gone. removed_if_value: key -> value it must still have to be
    removed (EC2 DeleteTags with a value only deletes a matching tag).
    """
    request_params = detail.get('requestParameters', {}) or {}
    event_name = detail.get('eventName', '')
    # Untag*, DeleteTags, RemoveTags (ELB) and RemoveTagsFromResource (RDS family)
    is_removal = event_name.startswith(('Untag', 'DeleteTags', 'RemoveTags'))
    tag_updates: Dict[str, str] = {}
    removed_tag_keys = set()
    removed_if_value: Dict[str, str] = {}
    replace = False

    tags = _unwrap_items(request_params.get('tags'))
    if isinstance(tags, list):
        # DynamoDB, RDS, ELB, CloudFront, KMS ...: [{"key": "Team", "value": "blue"}]
        pairs = [_tag_pair(item) for item in tags if isinstance(item, dict)]
        tags = {key: ('' if value is None else value) for key, value in pairs if key}
    if isinstance(tags, dict):
        if is_removal:
            removed_tag_keys.update(tags)
        else:
            tag_updates.update(
                (str(key), str(value)) for key, value in tags.items()
            )

    tag_keys = _unwrap_items(request_params.get('tagKeys') or request_params.get('tagKeyList'))
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
                if item.get('value') is None:
                    removed_tag_keys.add(str(key))
                else:
                    removed_if_value[str(key)] = str(item['value'])
            elif item.get('value') is not None:
                tag_updates[str(key)] = str(item['value'])

    # S3: Tagging.TagSet.Tag is a list, or a single dict for one tag.
    if event_name.startswith('PutBucketTagging'):
        tagging = request_params.get('Tagging')
        tag_set = tagging.get('TagSet') if isinstance(tagging, dict) else None
        if isinstance(tag_set, dict):
            replace = True
            tag_list = tag_set.get('Tag') or []
            for item in tag_list if isinstance(tag_list, list) else [tag_list]:
                key, value = _tag_pair(item) if isinstance(item, dict) else (None, None)
                if key:
                    tag_updates[key] = '' if value is None else str(value)
    elif event_name.startswith('DeleteBucketTagging'):
        replace = True

    return tag_updates, sorted(removed_tag_keys), replace, removed_if_value


_ARN_RE = re.compile(r'^arn:aws[a-z-]*:')
# Fallback scan: same shape as the old pattern, partition-aware.
_ARN_FIND_RE = re.compile(r'arn:aws[a-z-]*:[a-zA-Z0-9\-]+:[a-zA-Z0-9\-]*:[0-9]*:[^\s"\']+')


def _partition(region: str) -> str:
    """ARN partition for a Region (aws, aws-cn, aws-us-gov)."""
    # shortcut: covers the three public partitions; add ISO partitions if ever deployed there.
    if region.startswith('cn-'):
        return 'aws-cn'
    if region.startswith('us-gov-'):
        return 'aws-us-gov'
    return 'aws'


def _is_arn(value) -> bool:
    return isinstance(value, str) and bool(_ARN_RE.match(value))


def _arn_account(arn: str) -> str:
    return arn.split(':')[4] if arn.count(':') >= 5 else ''


def extract_resources_from_cloudtrail_event(detail: Dict, account_id: str) -> List[Dict]:
    """
    Extract resource ARNs and tag mutations from a CloudTrail event detail.
    Handles the most common tag operation patterns across AWS services.
    account_id is the trusted EventBridge envelope account: ARNs naming any
    other account are dropped. Returns list of
    {arn, tags, removed_tag_keys, removed_if_value, replace_tags}.
    """
    arns = set()

    region = detail.get('awsRegion', HUB_REGION)
    request_params = detail.get('requestParameters', {}) or {}
    tag_updates, removed_tag_keys, replace, removed_if_value = _extract_tag_mutation(detail)

    # Pattern 1: resourceArn, resource (Lambda), resourceName (RDS/DocumentDB/Neptune)
    for key in ('resourceArn', 'resource', 'resourceName'):
        arn = request_params.get(key, '')
        if _is_arn(arn):
            arns.add(arn)

    # Pattern 2: resources list; resourceArns (ELBv2 AddTags/RemoveTags)
    for res in list(request_params.get('resources', []) or []) + list(request_params.get('resourceArns', []) or []):
        if _is_arn(res):
            arns.add(res)
        elif isinstance(res, dict):
            arn = res.get('resourceArn') or res.get('arn', '')
            if _is_arn(arn):
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
        arns.add(f"arn:{_partition(region)}:s3:::{bucket}")

    # Pattern 4b: KMS keyId (key ARN or bare key ID; TagResource rejects aliases)
    key_id = request_params.get('keyId', '')
    if _is_arn(key_id):
        arns.add(key_id)
    elif isinstance(key_id, str) and _KMS_KEY_ID_RE.fullmatch(key_id):
        arns.add(f"arn:{_partition(region)}:kms:{region}:{account_id}:key/{key_id}")

    # Pattern 4c: classic ELB AddTags/RemoveTags (loadBalancerNames)
    for name in request_params.get('loadBalancerNames', []) or []:
        if isinstance(name, str) and _ELB_NAME_RE.fullmatch(name):
            arns.add(f"arn:{_partition(region)}:elasticloadbalancing:{region}:{account_id}:loadbalancer/{name}")

    # Pattern 5: SQS queue URL (TagQueue, UntagQueue)
    queue_match = re.match(
        r'https://sqs\.([a-z0-9-]+)\.amazonaws\.com(?:\.cn)?/(\d{12})/([^/?]+)$',
        request_params.get('queueUrl', ''),
    )
    if queue_match:
        q_region, q_account, q_name = queue_match.groups()
        arns.add(f"arn:{_partition(q_region)}:sqs:{q_region}:{q_account}:{q_name}")

    # Fallback: regex scan of the request/response only. userIdentity and
    # sessionIssuer carry caller ARNs (IAM/STS), which are not tagged resources.
    if not arns:
        scoped = json.dumps([request_params, detail.get('responseElements')])
        for arn in _ARN_FIND_RE.findall(scoped):
            if arn.split(':')[2] not in ('iam', 'sts'):
                arns.add(arn)

    resources = []
    for arn in sorted(arns):
        arn_account = _arn_account(arn)
        if arn_account and arn_account != account_id:
            logger.warning(f"ARN account {arn_account} != envelope {account_id}; skipping {arn}")
            continue
        resources.append({'arn': arn, 'tags': tag_updates, 'removed_tag_keys': list(removed_tag_keys),
                          'removed_if_value': removed_if_value, 'replace_tags': replace})
    return resources


_KMS_KEY_ID_RE = re.compile(r'(mrk-[0-9a-f]{32}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})')
_ELB_NAME_RE = re.compile(r'[A-Za-z0-9-]{1,32}')

# EC2 resource ID prefix -> ARN resource type, from "Resource types defined by
# Amazon EC2" (Service Authorization Reference). Longest prefix is tried first
# (tgw-attach- before tgw-). Key pairs are left out: their ARN uses the name.
_EC2_ID_PREFIXES = sorted({
    'i-': 'instance', 'vol-': 'volume', 'snap-': 'snapshot', 'ami-': 'image',
    'sg-': 'security-group', 'sgr-': 'security-group-rule', 'vpc-': 'vpc',
    'subnet-': 'subnet', 'eni-': 'network-interface', 'igw-': 'internet-gateway',
    'eigw-': 'egress-only-internet-gateway', 'rtb-': 'route-table', 'nat-': 'natgateway',
    'vpce-': 'vpc-endpoint', 'vpce-svc-': 'vpc-endpoint-service', 'lt-': 'launch-template',
    'tgw-': 'transit-gateway', 'tgw-attach-': 'transit-gateway-attachment',
    'tgw-rtb-': 'transit-gateway-route-table', 'eipalloc-': 'elastic-ip',
    'acl-': 'network-acl', 'dopt-': 'dhcp-options', 'pcx-': 'vpc-peering-connection',
    'cgw-': 'customer-gateway', 'vgw-': 'vpn-gateway', 'vpn-': 'vpn-connection',
    'fl-': 'vpc-flow-log', 'sir-': 'spot-instances-request', 'fleet-': 'fleet',
    'h-': 'dedicated-host', 'pl-': 'prefix-list', 'cr-': 'capacity-reservation',
}.items(), key=lambda item: -len(item[0]))


def _construct_ec2_arn(resource_id: str, account_id: str, region: str) -> Optional[str]:
    """Map EC2 resource IDs to ARNs."""
    if _is_arn(resource_id):
        return resource_id
    for prefix, resource_type in _EC2_ID_PREFIXES:
        if resource_id.startswith(prefix):
            return f"arn:{_partition(region)}:ec2:{region}:{account_id}:{resource_type}/{resource_id}"
    return None


TAG_CHANGE_EVENTS_PREFIX = 'tag-change-events'
_EVENT_ID_RE = re.compile(r'^[A-Za-z0-9-]{1,128}$')


def _cloudtrail_epoch(detail: Dict) -> int:
    """CloudTrail eventTime (ISO 8601, UTC) as epoch seconds; now if missing or malformed."""
    try:
        return int(datetime.strptime(detail['eventTime'], '%Y-%m-%dT%H:%M:%SZ')
                   .replace(tzinfo=timezone.utc).timestamp())
    except (KeyError, TypeError, ValueError):
        return int(time.time())


def write_tag_change_events(event: Dict, detail: Dict, account_id: str, region: str,
                            resources: List[Dict]) -> int:
    """
    Record who called a tag API, one JSON line per resource, in tag-change-events/.

    Written for every CloudTrail tag event, even when the native Tag Change on
    Resource event already updated state (it usually arrives first and has no
    caller identity). The delta history stays change-only; join the two on arn
    and time to answer "who changed this tag". Keyed by CloudTrail eventID, so a
    retried event overwrites its own record instead of duplicating it.
    """
    event_id = str(detail.get('eventID') or event.get('id') or '')
    if not _EVENT_ID_RE.fullmatch(event_id):
        logger.warning("CloudTrail event has no usable eventID; caller not recorded")
        return 0
    identity = detail.get('userIdentity', {}) or {}
    event_time = _cloudtrail_epoch(detail)
    rows = []
    for resource in resources:
        rows.append(json.dumps({
            'event_time': event_time,
            'event_id': event_id,
            'event_name': detail.get('eventName', ''),
            'account_id': account_id,
            'region': resource.get('region', region),
            'arn': resource['arn'],
            'changed_by': identity.get('arn') or identity.get('principalId'),
            'principal_type': identity.get('type'),
            'tags_set': resource.get('tags', {}) or {},
            # As requested; a value-conditional EC2 delete may not have matched.
            'tags_removed': sorted({*(resource.get('removed_tag_keys') or []),
                                    *(resource.get('removed_if_value') or {})}),
        }, separators=(',', ':')))
    if not rows:
        return 0
    day = datetime.fromtimestamp(event_time, tz=timezone.utc)
    key = (f"{TAG_CHANGE_EVENTS_PREFIX}/year={day.year}/month={day.month}/day={day.day}/"
           f"{account_id}-{event_id}.json")
    s3.put_object(Bucket=CENTRAL_ANALYTICS_BUCKET, Key=key,
                  Body=('\n'.join(rows) + '\n').encode('utf-8'), ContentType='application/json')
    return len(rows)


GLOBAL_REGION = 'global'


def _state_region(arn: str, region: str) -> str:
    """
    State partition for an ARN. ARNs without a Region field (IAM, CloudFront,
    Route 53, ...) are global resources: Resource Explorer files them under
    region:global, which the Coordinator scans as its own pair.

    shortcut: S3 bucket ARNs also lack a Region but Resource Explorer lists
    buckets in their own Region, so they stay under the event's awsRegion;
    revisit if another regional service turns up with Region-less ARNs.
    """
    parts = arn.split(':')
    if len(parts) > 3 and not parts[3] and parts[2] != 's3':
        return GLOBAL_REGION
    return region


def _has_mutation(resource: Dict) -> bool:
    return bool(resource.get('tags') or resource.get('removed_tag_keys')
                or resource.get('removed_if_value') or resource.get('replace_tags'))


def _emit_unparsed_metric(detail: Dict):
    """CloudWatch EMF metric (stdout, no API call) for a tag event that yielded no mutation."""
    print(json.dumps({
        '_aws': {
            'Timestamp': int(time.time() * 1000),
            'CloudWatchMetrics': [{
                'Namespace': 'TagInsights/Worker',
                'Dimensions': [['EventSource']],
                'Metrics': [{'Name': 'UnparsedTagEvents', 'Unit': 'Count'}],
            }],
        },
        'EventSource': str(detail.get('eventSource') or 'unknown'),
        'EventName': str(detail.get('eventName') or 'unknown'),
        'UnparsedTagEvents': 1,
    }))


def process_real_time_event(event: Dict) -> Dict:
    """
    Process a single forwarded CloudTrail tag change event.
    Updates the state for only the affected resources (not a full scan).

    The resource owner is the EventBridge envelope 'account' (set by AWS and
    preserved on cross-account forwarding), never detail.userIdentity, which
    a caller in another account could make point anywhere.
    """
    detail = event.get('detail', {}) or {}
    event_name = detail.get('eventName', 'unknown')
    region = detail.get('awsRegion', HUB_REGION)
    account_id = event.get('account', '')
    identity = detail.get('userIdentity', {}) or {}
    changed_by = identity.get('arn') or identity.get('principalId') or None

    if not account_id:
        logger.warning(f"Event has no EventBridge envelope account; skipping: {event_name}")
        return {'processed': 0, 'changed': 0}

    # CloudTrail also records calls that failed (AccessDenied, DryRun, invalid
    # ID, ...). They changed no tags; applying them would let any principal
    # forge a tag change just by attempting the call.
    if detail.get('errorCode'):
        logger.info(f"Skipping failed call {event_name}: {detail.get('errorCode')}")
        return {'processed': 0, 'changed': 0}

    resources = extract_resources_from_cloudtrail_event(detail, account_id)
    if not resources:
        logger.warning(f"No resources extracted from event: {event_name}")
        _emit_unparsed_metric(detail)
        return {'processed': 0, 'changed': 0}

    # Read current state for each state partition the event touches (the
    # event's Region, and 'global' for Region-less ARNs such as CloudFront).
    for resource in resources:
        resource['region'] = _state_region(resource['arn'], region)
    base_states = {r: read_state(account_id, r) for r in sorted({res['region'] for res in resources})}

    # A whole-set replace removes every key it does not set; record those as
    # removed in the caller row too. shortcut: taken from the state as read,
    # so a key added by a writer racing this event is not listed there.
    replaced = set()
    for resource in resources:
        if resource.get('replace_tags'):
            replaced.add(resource['arn'])
            resource['removed_tag_keys'] = sorted(
                set(base_states[resource['region']].get(resource['arn'], {}))
                - set(resource.get('tags', {})))

    mutations = [r for r in resources if _has_mutation(r)]
    if not mutations:
        logger.warning(f"No tag mutation parsed from {event_name}; caller not recorded")
        _emit_unparsed_metric(detail)
    callers_recorded = write_tag_change_events(event, detail, account_id, region, mutations)

    now = int(time.time())
    event_time = _cloudtrail_epoch(detail)  # the change time, not the queue delay
    deltas_written = 0
    for state_region, base_state in base_states.items():
        previous_state = dict(base_state)
        deltas = []
        for resource in resources:
            if resource['region'] != state_region:
                continue
            arn = resource['arn']
            tag_updates = resource.get('tags', {})
            removed_tag_keys = resource.get('removed_tag_keys', [])
            if not _has_mutation(resource):
                logger.info(
                    f"No explicit tag mutation in {event_name} for {arn}; "
                    "periodic scan will reconcile state"
                )
                continue

            previous_tags = previous_state.get(arn, {})
            current_tags = {} if resource.get('replace_tags') else dict(previous_tags)
            current_tags.update(tag_updates)
            for key in removed_tag_keys:
                current_tags.pop(key, None)
            for key, value in (resource.get('removed_if_value') or {}).items():
                if current_tags.get(key) == value:
                    current_tags.pop(key)

            is_new = arn not in previous_state and bool(current_tags)
            is_changed = arn in previous_state and current_tags != previous_tags

            if is_new or is_changed:
                deltas.append({
                    'arn': arn,
                    'resource_type': extract_service_from_arn(arn),
                    'tags': current_tags,
                    'previous_tags': previous_tags,
                    'change_type': 'new_resource' if is_new else 'tags_changed',
                    'timestamp': event_time,
                    'source_system': 'aggregator_worker_realtime',
                    'account_id': account_id,
                    'region': state_region,
                    'scan_id': f"event_{event_name}_{now}",
                    'changed_by': changed_by,
                    'event_name': event_name,
                })
                previous_state[arn] = current_tags

        if deltas:
            publish_deltas(account_id, state_region, f"event_{now}", deltas)
            save_state(account_id, state_region, previous_state, base_state, replaced)
        deltas_written += len(deltas)

    return {
        'trigger': 'realtime_event',
        'event_name': event_name,
        'account_id': account_id,
        'region': region,
        'resources_in_event': len(resources),
        'deltas_written': deltas_written,
        'callers_recorded': callers_recorded,
    }


TAG_CHANGE_DETAIL_TYPE = 'Tag Change on Resource'


def process_tag_change_event(event: Dict) -> Dict:
    """
    Process a native EventBridge "Tag Change on Resource" event (source aws.tag).

    Unlike CloudTrail tag APIs, whose names differ per service (CreateTags,
    PutBucketTagging, TagQueue, ...) and which miss tags set at creation, this
    event fires for tags set at creation too and carries the resource's full
    tag set, so state is replaced rather than patched. It has no caller
    identity; "who" still comes from the CloudTrail path.
    """
    account_id = event.get('account', '')  # EventBridge envelope, set by AWS
    region = event.get('region', HUB_REGION)
    detail = event.get('detail', {}) or {}
    tags = detail.get('tags') or {}
    arns = [a for a in event.get('resources', []) if isinstance(a, str) and a.startswith('arn:')]

    if not account_id or not arns or not isinstance(tags, dict):
        logger.warning("Malformed Tag Change on Resource event; skipping")
        return {'trigger': 'tag_change_event', 'deltas_written': 0}

    tags = {str(k): str(v) for k, v in tags.items()}
    now = int(time.time())
    deltas_written = 0
    for state_region in sorted({_state_region(arn, region) for arn in arns}):
        base_state = read_state(account_id, state_region)
        previous_state = dict(base_state)
        deltas = []
        for arn in arns:
            if _state_region(arn, region) != state_region:
                continue
            arn_account = _arn_account(arn)
            if arn_account and arn_account != account_id:
                logger.warning(f"Tag change ARN account {arn_account} != envelope {account_id}; skipping")
                continue
            previous_tags = previous_state.get(arn)
            if previous_tags == tags:
                continue  # already recorded (CloudTrail path or scan got there first)
            deltas.append({
                'arn': arn,
                'resource_type': extract_service_from_arn(arn),
                'tags': tags,
                'previous_tags': previous_tags or {},
                'change_type': 'new_resource' if previous_tags is None else 'tags_changed',
                'timestamp': now,
                'source_system': 'aggregator_worker_tag_event',
                'account_id': account_id,
                'region': state_region,
                'scan_id': f"tag_event_{now}",
                'changed_by': None,
                'event_name': 'TagChangeOnResource',
            })
            previous_state[arn] = tags

        if deltas:
            publish_deltas(account_id, state_region, f"tag_event_{now}", deltas)
            save_state(account_id, state_region, previous_state, base_state)
        deltas_written += len(deltas)

    return {'trigger': 'tag_change_event', 'account_id': account_id,
            'region': region, 'deltas_written': deltas_written}


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

    # Read previous state
    previous = read_state(account_id, region)

    # shortcut: a member account+region whose resources really were all deleted
    # fails here until its state file is removed; add an explicit opt-in if
    # that becomes a real case.
    if not current and previous and account_id != get_hub_account_id():
        if region == GLOBAL_REGION:
            # Global state can hold ARNs filed only by realtime events (e.g. a
            # member whose global resources are not indexed). Keep it and skip,
            # so the pair neither fails on every run nor records mass removals.
            logger.warning(
                f"Empty global inventory for {account_id} but previous state has "
                f"{len(previous)} resources; keeping state, skipping scan."
            )
            return {'account_id': account_id, 'region': region, 'scan_id': scan_id,
                    'total_resources': 0, 'deltas': 0, 'skipped': 'empty_global_inventory'}
        logger.error(
            f"Empty inventory for {account_id}/{region} but previous state has "
            f"{len(previous)} resources; refusing to record them all as removed."
        )
        raise RuntimeError(f"Empty Resource Explorer inventory for {account_id}/{region}")

    # Compute deltas
    deltas = compute_deltas(account_id, region, scan_id, current, previous)

    # Write deltas (only if there are changes) and notify Analytics Writer via SQS
    if deltas:
        publish_deltas(account_id, region, scan_id, deltas)

    # Always write updated state (captures new resources even with no tag changes)
    save_state(account_id, region, current, previous)

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
    # ESM uses ReportBatchItemFailures: only failed records are retried
    # (and reach the DLQ after maxReceiveCount); the rest are deleted.
    if 'Records' in event:
        failures = []
        for record in event['Records']:
            try:
                body = json.loads(record['body'])
                if body.get('detail-type') == 'AWS API Call via CloudTrail':
                    process_real_time_event(body)
                elif body.get('detail-type') == TAG_CHANGE_DETAIL_TYPE:
                    process_tag_change_event(body)
                else:
                    logger.warning(f"Unknown SQS message type: {list(body.keys())}")
            except Exception as e:
                logger.error(f"Error processing SQS record: {e}", exc_info=True)
                failures.append({'itemIdentifier': record['messageId']})
        return {'batchItemFailures': failures}

    # ── Direct EventBridge event (real-time, non-SQS path) ───────────────────
    if event.get('detail-type') == 'AWS API Call via CloudTrail':
        return process_real_time_event(event)
    if event.get('detail-type') == TAG_CHANGE_DETAIL_TYPE:
        return process_tag_change_event(event)

    # ── Coordinator scan job ──────────────────────────────────────────────────
    account_id = event.get('account_id', '')
    region = event.get('region', HUB_REGION)
    scan_id = event.get('scan_id', datetime.now(timezone.utc).strftime('%Y-%m-%d'))

    if not account_id:
        raise ValueError("account_id is required in the event payload for coordinator scan jobs")

    return process_account_region_scan(account_id, region, scan_id)

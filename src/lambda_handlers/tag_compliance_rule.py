"""
TagInsights - Tag Compliance Config Rule (Step 2)

Organization custom Lambda rule. One evaluation checks every required tag:
key presence, allowed values, and multi-value fields (each entry validated,
duplicates flagged). All violations are reported, not just the first.

Deployment model (see deployment/tag-compliance-stack.yaml):
  - This Lambda and the schema live in the hub (Config delegated admin) account.
  - AWS::Config::OrganizationConfigRule deploys the rule to every member account.
  - Member-account Config invokes this Lambda cross-account; the Lambda assumes
    ExecutionRoleName in the member account to call PutEvaluations
    (deployment/templates/tag-compliance-member-role.yaml, via StackSet).

Schema: read at runtime from SSM Parameter Store (SCHEMA_PARAMETER), not from
rule parameters, because Config caps InputParameters at 1,024 characters.

  {
    "delimiter": "+",
    "requiredTags": [
      {"key": "Environment", "allowed_values": ["Production", "Staging", "Development"]},
      {"key": "CustomerName", "multi_value": true, "allowed_values": ["ACME", "GLOBEX"]}
    ]
  }

Delimiter: commas are not valid in tag values for every service (S3
PutBucketTagging rejects them). "+" is in the character set every service
accepts (letters, numbers, spaces, and _ . : / = + - @). Matching is
case-sensitive on purpose: "Prod" and "Production" are different cost buckets.
"""

import json
import os
import re
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'WARNING'))

SCHEMA_PARAMETER = os.environ.get('SCHEMA_PARAMETER', '')
EXECUTION_ROLE_NAME = os.environ.get('EXECUTION_ROLE_NAME', '')
SCHEMA_CACHE_SECONDS = int(os.environ.get('SCHEMA_CACHE_SECONDS', '300'))
ANNOTATION_MAX = 256  # Config limit on Evaluation.Annotation
_VALUE_LIST = re.compile(r'(: (?:invalid|duplicates)) \[.*\]$')  # tag keys cannot contain '['

ssm = boto3.client('ssm')
sts = boto3.client('sts')

_schema_cache: Dict = {'value': None, 'loaded_at': 0.0}
_creds_cache: Dict[str, Dict] = {}  # account_id -> assume_role Credentials
CREDS_REFRESH_MARGIN = timedelta(minutes=5)
DELETED_STATUSES = ('ResourceDeleted', 'ResourceDeletedNotRecorded', 'ResourceNotRecorded')


# ── Pure evaluation logic ─────────────────────────────────────────────────────

def evaluate_tags(tags: Dict[str, str], schema: Dict) -> List[str]:
    """Return every violation for one resource's tags; empty list means compliant."""
    delimiter = schema.get('delimiter', '+')
    violations = []
    for spec in schema.get('requiredTags', []):
        key = spec['key']
        if key not in tags:
            violations.append(f"{key}: missing")
            continue
        value = tags[key]
        # Spaces around multi-value entries are a formatting convention; a single value is
        # matched as stored, because " Production" is its own cost bucket.
        entries = [v.strip() for v in value.split(delimiter)] if spec.get('multi_value') else [value]
        if any(e.strip() == '' for e in entries):
            violations.append(f"{key}: empty value")
        entries = [e for e in entries if e.strip()]
        allowed = spec.get('allowed_values')
        if allowed is not None:
            invalid = [e for e in entries if e not in allowed]
            if invalid:
                violations.append(f"{key}: invalid {invalid}")
        if spec.get('multi_value'):
            dupes = sorted({e for e in entries if entries.count(e) > 1})
            if dupes:
                violations.append(f"{key}: duplicates {dupes}")
    return violations


def build_annotation(violations: List[str]) -> str:
    parts = list(violations)
    text = '; '.join(parts)
    # Drop the offending values first so every tag key (which the daily report counts) still fits.
    # Longest lists go first, so short ones stay readable in the Config console.
    for i in sorted(range(len(parts)), key=lambda i: len(parts[i]), reverse=True):
        if len(text) <= ANNOTATION_MAX:
            break
        parts[i] = _VALUE_LIST.sub(r'\1 [...]', parts[i])
        text = '; '.join(parts)
    return text if len(text) <= ANNOTATION_MAX else text[:ANNOTATION_MAX - 3] + '...'


# ── AWS plumbing ──────────────────────────────────────────────────────────────

def validate_schema(schema: Dict) -> Dict:
    """Fail loudly on a malformed schema instead of mis-evaluating every resource."""
    if not isinstance(schema, dict) or not isinstance(schema.get('requiredTags'), list):
        raise ValueError(f"{SCHEMA_PARAMETER} must contain a requiredTags list")
    delimiter = schema.get('delimiter', '+')
    if not isinstance(delimiter, str) or not delimiter:
        raise ValueError(f"{SCHEMA_PARAMETER}: delimiter must be a non-empty string")
    for spec in schema['requiredTags']:
        if not isinstance(spec, dict) or not isinstance(spec.get('key'), str) or not spec['key']:
            raise ValueError(f"{SCHEMA_PARAMETER}: every requiredTags entry needs a non-empty string 'key'")
        allowed = spec.get('allowed_values')
        if allowed is not None and not (isinstance(allowed, list) and all(isinstance(v, str) for v in allowed)):
            raise ValueError(f"{SCHEMA_PARAMETER}: {spec['key']}.allowed_values must be a list of strings")
        if 'multi_value' in spec and not isinstance(spec['multi_value'], bool):
            # "false" (a string) is truthy and would silently split single-value tags.
            raise ValueError(f"{SCHEMA_PARAMETER}: {spec['key']}.multi_value must be true or false")
    return schema


def load_schema() -> Dict:
    now = time.time()
    if _schema_cache['value'] is None or now - _schema_cache['loaded_at'] > SCHEMA_CACHE_SECONDS:
        raw = ssm.get_parameter(Name=SCHEMA_PARAMETER)['Parameter']['Value']
        schema = validate_schema(json.loads(raw))
        _schema_cache.update(value=schema, loaded_at=now)
    return _schema_cache['value']


def member_credentials(account_id: str, partition: str) -> Dict:
    """Assumed-role credentials per account, reused until 5 minutes before they expire."""
    cached = _creds_cache.get(account_id)
    if cached and cached['Expiration'] - CREDS_REFRESH_MARGIN > datetime.now(timezone.utc):
        return cached
    role_arn = f"arn:{partition}:iam::{account_id}:role/{EXECUTION_ROLE_NAME}"
    creds = sts.assume_role(RoleArn=role_arn, RoleSessionName='ti-tag-compliance')['Credentials']
    _creds_cache[account_id] = creds
    return creds


def member_config_client(account_id: str, region: str):
    """Assume the evaluation role in the member account that owns the resource."""
    partition = boto3.session.Session().get_partition_for_region(region)
    creds = member_credentials(account_id, partition)
    return boto3.client(
        'config', region_name=region,
        aws_access_key_id=creds['AccessKeyId'],
        aws_secret_access_key=creds['SecretAccessKey'],
        aws_session_token=creds['SessionToken'],
    )


def evaluation_region(event: Dict, invoking_event: Dict) -> str:
    """Region of the member account's Config rule that invoked this function.

    The rule ARN is authoritative. A CI's awsRegion is 'global' for IAM and
    similar resources, which is not a Config endpoint.
    """
    parts = event.get('configRuleArn', '').split(':')
    if len(parts) > 3 and parts[3]:
        return parts[3]
    item = invoking_event.get('configurationItem') or invoking_event.get('configurationItemSummary') or {}
    region = item.get('awsRegion')
    if region and region != 'global':
        return region
    return os.environ.get('AWS_REGION', 'us-east-1')


def get_configuration_item(invoking_event: Dict, config_client) -> Dict:
    """Oversized CIs arrive as a summary; fetch the full item from config history.

    A deleted resource (by summary status, or with no history left) is returned
    with a deleted status so the caller reports NOT_APPLICABLE.
    """
    if invoking_event.get('messageType') == 'OversizedConfigurationItemChangeNotification':
        summary = invoking_event['configurationItemSummary']
        if summary.get('configurationItemStatus') in DELETED_STATUSES:
            return {**summary, 'tags': {}}
        history = config_client.get_resource_config_history(
            resourceType=summary['resourceType'], resourceId=summary['resourceId'], limit=1
        )
        items = history.get('configurationItems') or []
        if not items:
            return {**summary, 'configurationItemStatus': 'ResourceDeleted', 'tags': {}}
        item = items[0]
        capture_time = item['configurationItemCaptureTime']
        if isinstance(capture_time, datetime):
            # boto3 parses this to a datetime; the handler's return value must stay JSON-serializable.
            capture_time = capture_time.isoformat()
        return {
            'resourceType': item['resourceType'],
            'resourceId': item['resourceId'],
            'configurationItemCaptureTime': capture_time,
            'configurationItemStatus': item['configurationItemStatus'],
            'tags': item.get('tags', {}),
        }
    return invoking_event['configurationItem']


def lambda_handler(event, context):
    if not SCHEMA_PARAMETER or not EXECUTION_ROLE_NAME:
        raise ValueError("SCHEMA_PARAMETER and EXECUTION_ROLE_NAME must be set")

    invoking_event = json.loads(event['invokingEvent'])
    if 'configurationItem' not in invoking_event and 'configurationItemSummary' not in invoking_event:
        # ScheduledNotification (and any other resource-less message) has nothing to evaluate.
        logger.info("Ignoring %s event", invoking_event.get('messageType'))
        return None

    account_id = event['accountId']
    region = evaluation_region(event, invoking_event)
    config_client = member_config_client(account_id, region)
    item = get_configuration_item(invoking_event, config_client)

    if event.get('eventLeftScope'):
        compliance, annotation = 'NOT_APPLICABLE', 'Resource no longer in rule scope'
    elif item.get('configurationItemStatus') in DELETED_STATUSES:
        compliance, annotation = 'NOT_APPLICABLE', 'Resource deleted or not recorded'
    else:
        violations = evaluate_tags(item.get('tags') or {}, load_schema())
        compliance = 'NON_COMPLIANT' if violations else 'COMPLIANT'
        annotation = build_annotation(violations) if violations else 'All required tags valid'

    evaluation = {
        'ComplianceResourceType': item['resourceType'],
        'ComplianceResourceId': item['resourceId'],
        'ComplianceType': compliance,
        'Annotation': annotation,
        'OrderingTimestamp': item['configurationItemCaptureTime'],
    }
    response = config_client.put_evaluations(Evaluations=[evaluation], ResultToken=event['resultToken'])
    failed = response.get('FailedEvaluations') if isinstance(response, dict) else None
    if failed:
        raise RuntimeError(f"PutEvaluations rejected {len(failed)} evaluation(s): {failed}")
    return evaluation

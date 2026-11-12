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
import time
import logging
from typing import Dict, List

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'WARNING'))

SCHEMA_PARAMETER = os.environ.get('SCHEMA_PARAMETER', '')
EXECUTION_ROLE_NAME = os.environ.get('EXECUTION_ROLE_NAME', '')
SCHEMA_CACHE_SECONDS = int(os.environ.get('SCHEMA_CACHE_SECONDS', '300'))
ANNOTATION_MAX = 256  # Config limit on Evaluation.Annotation

ssm = boto3.client('ssm')
sts = boto3.client('sts')

_schema_cache: Dict = {'value': None, 'loaded_at': 0.0}


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
        entries = [v.strip() for v in value.split(delimiter)] if spec.get('multi_value') else [value.strip()]
        if any(e == '' for e in entries):
            violations.append(f"{key}: empty value")
        entries = [e for e in entries if e]
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
    text = '; '.join(violations)
    return text if len(text) <= ANNOTATION_MAX else text[:ANNOTATION_MAX - 3] + '...'


# ── AWS plumbing ──────────────────────────────────────────────────────────────

def load_schema() -> Dict:
    now = time.time()
    if _schema_cache['value'] is None or now - _schema_cache['loaded_at'] > SCHEMA_CACHE_SECONDS:
        raw = ssm.get_parameter(Name=SCHEMA_PARAMETER)['Parameter']['Value']
        schema = json.loads(raw)
        if not isinstance(schema.get('requiredTags'), list):
            raise ValueError(f"{SCHEMA_PARAMETER} must contain a requiredTags list")
        _schema_cache.update(value=schema, loaded_at=now)
    return _schema_cache['value']


def member_config_client(account_id: str, region: str):
    """Assume the evaluation role in the member account that owns the resource."""
    partition = boto3.session.Session().get_partition_for_region(region)
    role_arn = f"arn:{partition}:iam::{account_id}:role/{EXECUTION_ROLE_NAME}"
    creds = sts.assume_role(RoleArn=role_arn, RoleSessionName='ti-tag-compliance')['Credentials']
    return boto3.client(
        'config', region_name=region,
        aws_access_key_id=creds['AccessKeyId'],
        aws_secret_access_key=creds['SecretAccessKey'],
        aws_session_token=creds['SessionToken'],
    )


def get_configuration_item(invoking_event: Dict, config_client) -> Dict:
    """Oversized CIs arrive as a summary; fetch the full item from config history."""
    if invoking_event.get('messageType') == 'OversizedConfigurationItemChangeNotification':
        summary = invoking_event['configurationItemSummary']
        history = config_client.get_resource_config_history(
            resourceType=summary['resourceType'], resourceId=summary['resourceId'], limit=1
        )
        item = history['configurationItems'][0]
        return {
            'resourceType': item['resourceType'],
            'resourceId': item['resourceId'],
            'configurationItemCaptureTime': item['configurationItemCaptureTime'],
            'configurationItemStatus': item['configurationItemStatus'],
            'tags': item.get('tags', {}),
        }
    return invoking_event['configurationItem']


def lambda_handler(event, context):
    if not SCHEMA_PARAMETER or not EXECUTION_ROLE_NAME:
        raise ValueError("SCHEMA_PARAMETER and EXECUTION_ROLE_NAME must be set")

    invoking_event = json.loads(event['invokingEvent'])
    account_id = event['accountId']
    region = os.environ.get('AWS_REGION', 'us-east-1')
    if 'configurationItem' in invoking_event:
        region = invoking_event['configurationItem'].get('awsRegion', region)
    elif 'configurationItemSummary' in invoking_event:
        region = invoking_event['configurationItemSummary'].get('awsRegion', region)

    config_client = member_config_client(account_id, region)
    item = get_configuration_item(invoking_event, config_client)

    if item.get('configurationItemStatus') in ('ResourceDeleted', 'ResourceDeletedNotRecorded', 'ResourceNotRecorded'):
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
    config_client.put_evaluations(Evaluations=[evaluation], ResultToken=event['resultToken'])
    return evaluation

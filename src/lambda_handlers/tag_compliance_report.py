"""
TagInsights - Daily Tag Compliance Report (Step 2)

EventBridge schedule -> this Lambda -> AWS Config aggregator -> HTML email (Amazon SES).

GetAggregateComplianceDetailsByConfigRule needs AccountId and AwsRegion per
call (max 100 results per page), so the report first asks
DescribeAggregateComplianceByConfigRules which account+Region pairs are
NON_COMPLIANT for the rule, then pages details only for those pairs.

Organization rules are named "OrgConfigRule-<name>-<suffix>" in member
accounts, so the rule is matched by that prefix.

SES: the sender identity must be verified. In the SES sandbox you can only
send to verified recipients (200 messages/24h, 1/s); a distribution list
needs production access.
"""

import html
import os
import re
import logging
from collections import Counter, defaultdict
from typing import Dict, Iterable, List

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'WARNING'))

AGGREGATOR_NAME = os.environ.get('AGGREGATOR_NAME', '')
RULE_NAME = os.environ.get('RULE_NAME', '')
SENDER = os.environ.get('SENDER', '')
RECIPIENTS = [r.strip() for r in os.environ.get('RECIPIENTS', '').split(',') if r.strip()]
MAX_ROWS = int(os.environ.get('MAX_ROWS', '500'))

SES_MAX_RECIPIENTS = 50  # SendEmail limit across To/Cc/Bcc
# Violation formats written by tag_compliance_rule.evaluate_tags; the key ends at the first ': ' followed by a verb.
_VIOLATION = re.compile(r'^(.+?): (?:missing|empty value|invalid \[|duplicates \[)')

config = boto3.client('config')
ses = boto3.client('ses')


def rule_matches(name: str, rule: str) -> bool:
    # The member-account suffix is alphanumeric, so "OrgConfigRule-<rule>-v2-<suffix>" is another rule.
    return name == rule or re.fullmatch(f"OrgConfigRule-{re.escape(rule)}-[A-Za-z0-9]+", name) is not None


def noncompliant_pairs(rule: str) -> List[Dict]:
    pairs = []
    for page in config.get_paginator('describe_aggregate_compliance_by_config_rules').paginate(
        ConfigurationAggregatorName=AGGREGATOR_NAME, Filters={'ComplianceType': 'NON_COMPLIANT'}
    ):
        for item in page.get('AggregateComplianceByConfigRules', []):
            if rule_matches(item['ConfigRuleName'], rule):
                pairs.append({'rule': item['ConfigRuleName'], 'account': item['AccountId'], 'region': item['AwsRegion']})
    return pairs


def noncompliant_resources(pairs: Iterable[Dict]) -> List[Dict]:
    rows = []
    paginator = config.get_paginator('get_aggregate_compliance_details_by_config_rule')
    for p in pairs:
        for page in paginator.paginate(
            ConfigurationAggregatorName=AGGREGATOR_NAME, ConfigRuleName=p['rule'],
            AccountId=p['account'], AwsRegion=p['region'], ComplianceType='NON_COMPLIANT',
        ):
            for r in page.get('AggregateEvaluationResults', []):
                q = r['EvaluationResultIdentifier']['EvaluationResultQualifier']
                rows.append({'account': p['account'], 'region': p['region'],
                             'resource_type': q['ResourceType'], 'resource_id': q['ResourceId'],
                             'annotation': r.get('Annotation', '')})
    return rows


def tag_counts(rows: List[Dict]) -> Counter:
    """Count resources per violated tag key, from annotations like 'Env: missing; Owner: invalid [...]'.

    A resource with two violations on one key (invalid and duplicates) counts once.
    Tag keys may contain ':' (aws:..., team:owner), so each '; '-separated violation
    is split at the ': ' that precedes a known verb from tag_compliance_rule.
    """
    counts = Counter()
    for row in rows:
        keys = set()
        for part in row['annotation'].split('; '):
            m = _VIOLATION.match(part)
            if m:
                keys.add(m.group(1).strip())
        counts.update(keys)
    return counts


def render_html(rows: List[Dict]) -> str:
    """Every value is escaped: tag values and resource IDs are user-controlled."""
    e = html.escape
    by_account = Counter(r['account'] for r in rows)
    parts = [f"<h2>Tag compliance: {len(rows)} non-compliant resources</h2>"]
    if not rows:
        parts.append("<p>All evaluated resources are compliant.</p>")
        return ''.join(parts)
    parts.append("<h3>By tag</h3><table border='1' cellpadding='4'><tr><th>Tag</th><th>Resources</th></tr>")
    parts += [f"<tr><td>{e(k)}</td><td>{n}</td></tr>" for k, n in tag_counts(rows).most_common()]
    parts.append("</table><h3>By account</h3><table border='1' cellpadding='4'><tr><th>Account</th><th>Resources</th></tr>")
    parts += [f"<tr><td>{e(a)}</td><td>{n}</td></tr>" for a, n in by_account.most_common()]
    parts.append("</table><h3>Resources</h3><table border='1' cellpadding='4'>"
                 "<tr><th>Account</th><th>Region</th><th>Type</th><th>Resource</th><th>Violations</th></tr>")
    grouped = defaultdict(list)
    for r in rows:
        grouped[r['account']].append(r)
    shown = 0
    for account in sorted(grouped):
        for r in grouped[account]:
            if shown >= MAX_ROWS:
                break
            parts.append(f"<tr><td>{e(r['account'])}</td><td>{e(r['region'])}</td><td>{e(r['resource_type'])}</td>"
                         f"<td>{e(r['resource_id'])}</td><td>{e(r['annotation'])}</td></tr>")
            shown += 1
    parts.append("</table>")
    if len(rows) > shown:
        parts.append(f"<p>Showing {shown} of {len(rows)}. Query the Config aggregator for the full list.</p>")
    return ''.join(parts)


def lambda_handler(event, context):
    if not (AGGREGATOR_NAME and RULE_NAME and SENDER and RECIPIENTS):
        raise ValueError("AGGREGATOR_NAME, RULE_NAME, SENDER and RECIPIENTS must be set")
    rows = noncompliant_resources(noncompliant_pairs(RULE_NAME))
    message = {
        'Subject': {'Data': f"Tag compliance: {len(rows)} non-compliant resources"},
        'Body': {'Html': {'Data': render_html(rows)}},
    }
    for i in range(0, len(RECIPIENTS), SES_MAX_RECIPIENTS):
        ses.send_email(Source=SENDER, Destination={'ToAddresses': RECIPIENTS[i:i + SES_MAX_RECIPIENTS]}, Message=message)
    return {'noncompliant': len(rows), 'recipients': len(RECIPIENTS)}

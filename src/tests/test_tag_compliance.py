"""Tests for the Step 2 tag compliance Config rule and daily SES report."""

import json
import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

with patch("boto3.client", return_value=MagicMock()):
    import src.lambda_handlers.tag_compliance_rule as rule
    import src.lambda_handlers.tag_compliance_report as report

SCHEMA = {
    "delimiter": "+",
    "requiredTags": [
        {"key": "Environment", "allowed_values": ["Production", "Staging", "Development"]},
        {"key": "CostCenter"},
        {"key": "CustomerName", "multi_value": True, "allowed_values": ["ACME", "GLOBEX", "INITECH"]},
    ],
}
GOOD = {"Environment": "Production", "CostCenter": "CC-100", "CustomerName": "ACME+GLOBEX"}


class TestEvaluateTags:
    def test_compliant(self):
        assert rule.evaluate_tags(GOOD, SCHEMA) == []

    def test_reports_all_violations_not_just_first(self):
        tags = {"Environment": "Prod", "CustomerName": "ACME+HOOLI"}
        assert rule.evaluate_tags(tags, SCHEMA) == [
            "Environment: invalid ['Prod']",
            "CostCenter: missing",
            "CustomerName: invalid ['HOOLI']",
        ]

    def test_duplicates_flagged(self):
        v = rule.evaluate_tags({**GOOD, "CustomerName": "ACME+ACME"}, SCHEMA)
        assert v == ["CustomerName: duplicates ['ACME']"]

    def test_trailing_delimiter_is_empty_value(self):
        v = rule.evaluate_tags({**GOOD, "CustomerName": "ACME+"}, SCHEMA)
        assert v == ["CustomerName: empty value"]

    def test_whitespace_around_entries_ignored(self):
        assert rule.evaluate_tags({**GOOD, "CustomerName": "ACME + GLOBEX"}, SCHEMA) == []

    def test_case_sensitive(self):
        assert rule.evaluate_tags({**GOOD, "Environment": "production"}, SCHEMA) == ["Environment: invalid ['production']"]

    def test_comma_is_not_a_delimiter_by_default(self):
        v = rule.evaluate_tags({**GOOD, "CustomerName": "ACME,GLOBEX"}, SCHEMA)
        assert v == ["CustomerName: invalid ['ACME,GLOBEX']"]

    def test_annotation_truncated_to_config_limit(self):
        assert len(rule.build_annotation(["x" * 300])) == 256


def _event(item_status="OK", tags=None, message_type="ConfigurationItemChangeNotification"):
    ci = {"resourceType": "AWS::S3::Bucket", "resourceId": "b1", "awsRegion": "us-west-2",
          "configurationItemCaptureTime": "2026-11-10T00:00:00.000Z",
          "configurationItemStatus": item_status, "tags": tags if tags is not None else GOOD}
    inv = {"messageType": message_type}
    if message_type == "OversizedConfigurationItemChangeNotification":
        inv["configurationItemSummary"] = {k: ci[k] for k in ("resourceType", "resourceId", "awsRegion")}
    else:
        inv["configurationItem"] = ci
    return {"invokingEvent": json.dumps(inv), "accountId": "111122223333", "resultToken": "tok"}, ci


class TestHandler:
    def _run(self, event, history=None):
        cfg = MagicMock()
        if history:
            cfg.get_resource_config_history.return_value = {"configurationItems": [history]}
        with patch.object(rule, "SCHEMA_PARAMETER", "/ti/schema"), \
                patch.object(rule, "EXECUTION_ROLE_NAME", "ti-eval"), \
                patch.object(rule, "load_schema", return_value=SCHEMA), \
                patch.object(rule, "member_config_client", return_value=cfg) as mcc:
            result = rule.lambda_handler(event, None)
        return result, cfg, mcc

    def test_puts_evaluation_in_member_account(self):
        event, _ = _event(tags={"Environment": "Prod"})
        result, cfg, mcc = self._run(event)
        mcc.assert_called_once_with("111122223333", "us-west-2")
        ev = cfg.put_evaluations.call_args.kwargs
        assert ev["ResultToken"] == "tok"
        assert ev["Evaluations"][0]["ComplianceType"] == "NON_COMPLIANT"
        assert "CostCenter: missing" in result["Annotation"]

    def test_deleted_resource_not_applicable(self):
        event, _ = _event(item_status="ResourceDeleted")
        result, _, _ = self._run(event)
        assert result["ComplianceType"] == "NOT_APPLICABLE"

    def test_oversized_ci_fetched_from_history(self):
        event, ci = _event(message_type="OversizedConfigurationItemChangeNotification")
        result, cfg, _ = self._run(event, history=ci)
        cfg.get_resource_config_history.assert_called_once()
        assert result["ComplianceType"] == "COMPLIANT"


class TestReport:
    def test_org_rule_name_prefix_match(self):
        assert report.rule_matches("OrgConfigRule-ti-tags-abc123", "ti-tags")
        assert report.rule_matches("ti-tags", "ti-tags")
        assert not report.rule_matches("OrgConfigRule-ti-tags2-abc", "ti-tags")

    def test_tag_counts_parse_annotations(self):
        rows = [{"annotation": "Environment: invalid ['Prod']; CostCenter: missing"},
                {"annotation": "CostCenter: missing"}]
        assert report.tag_counts(rows) == {"CostCenter": 2, "Environment": 1}

    def test_tag_counts_count_resources_not_violations(self):
        rows = [{"annotation": "CustomerName: invalid ['HOOLI']; CustomerName: duplicates ['ACME']"}]
        assert report.tag_counts(rows) == {"CustomerName": 1}

    def test_html_escapes_user_controlled_values(self):
        rows = [{"account": "1", "region": "us-east-1", "resource_type": "AWS::S3::Bucket",
                 "resource_id": "<script>x</script>", "annotation": "Env: invalid ['<b>']"}]
        out = report.render_html(rows)
        assert "<script>" not in out and "&lt;script&gt;" in out

    def test_handler_sends_email(self):
        with patch.object(report, "AGGREGATOR_NAME", "agg"), patch.object(report, "RULE_NAME", "r"), \
                patch.object(report, "SENDER", "a@x.com"), patch.object(report, "RECIPIENTS", ["b@x.com"]), \
                patch.object(report, "noncompliant_pairs", return_value=[]), \
                patch.object(report, "noncompliant_resources", return_value=[]), \
                patch.object(report, "ses") as ses:
            assert report.lambda_handler({}, None) == {"noncompliant": 0, "recipients": 1}
        assert ses.send_email.call_args.kwargs["Destination"] == {"ToAddresses": ["b@x.com"]}

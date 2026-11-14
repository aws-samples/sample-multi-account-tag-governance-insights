"""Tests for the Step 2 tag compliance Config rule and daily SES report."""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

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


def _oversized(**summary_updates):
    event, ci = _event(message_type="OversizedConfigurationItemChangeNotification")
    inv = json.loads(event["invokingEvent"])
    inv["configurationItemSummary"].update({"configurationItemCaptureTime": ci["configurationItemCaptureTime"],
                                            "configurationItemStatus": "OK", **summary_updates})
    event["invokingEvent"] = json.dumps(inv)
    return event


class TestRuleHardening:
    def _run(self, event, cfg):
        with patch.object(rule, "SCHEMA_PARAMETER", "/ti/schema"), \
                patch.object(rule, "EXECUTION_ROLE_NAME", "ti-eval"), \
                patch.object(rule, "load_schema", return_value=SCHEMA), \
                patch.object(rule, "member_config_client", return_value=cfg) as mcc:
            return rule.lambda_handler(event, None), mcc

    def test_oversized_with_empty_history_not_applicable(self):
        cfg = MagicMock()
        cfg.get_resource_config_history.return_value = {"configurationItems": []}
        result, _ = self._run(_oversized(), cfg)
        assert result["ComplianceType"] == "NOT_APPLICABLE"

    def test_oversized_deleted_summary_skips_history(self):
        cfg = MagicMock()
        result, _ = self._run(_oversized(configurationItemStatus="ResourceDeleted"), cfg)
        cfg.get_resource_config_history.assert_not_called()
        assert result["ComplianceType"] == "NOT_APPLICABLE"

    def test_event_left_scope_not_applicable(self):
        event, _ = _event(tags={})
        event["eventLeftScope"] = True
        result, _ = self._run(event, MagicMock())
        assert result["ComplianceType"] == "NOT_APPLICABLE"

    def test_failed_evaluations_raise(self):
        event, _ = _event()
        cfg = MagicMock()
        cfg.put_evaluations.return_value = {"FailedEvaluations": [{"ComplianceResourceId": "b1"}]}
        with pytest.raises(RuntimeError, match="rejected 1"):
            self._run(event, cfg)

    def test_region_from_rule_arn_then_ci_then_lambda(self):
        assert rule.evaluation_region({"configRuleArn": "arn:aws:config:eu-west-1:1:config-rule/x"},
                                      {"configurationItem": {"awsRegion": "global"}}) == "eu-west-1"
        assert rule.evaluation_region({}, {"configurationItem": {"awsRegion": "us-west-2"}}) == "us-west-2"
        with patch.dict(os.environ, {"AWS_REGION": "ap-south-1"}):
            assert rule.evaluation_region({}, {"configurationItem": {"awsRegion": "global"}}) == "ap-south-1"

    def test_scheduled_notification_ignored(self):
        event = {"invokingEvent": json.dumps({"messageType": "ScheduledNotification"}),
                 "accountId": "111122223333", "resultToken": "tok"}
        result, mcc = self._run(event, MagicMock())
        assert result is None
        mcc.assert_not_called()

    @pytest.mark.parametrize("bad", [
        {"requiredTags": [{"key": "A", "allowed_values": "Prod"}]},
        {"requiredTags": [{"key": "A", "allowed_values": [1]}]},
        {"delimiter": "", "requiredTags": []},
        {"delimiter": 5, "requiredTags": []},
        {"requiredTags": [{"allowed_values": []}]},
    ])
    def test_schema_validation_rejects(self, bad):
        with pytest.raises(ValueError):
            rule.validate_schema(bad)

    def test_schema_validation_accepts_default(self):
        assert rule.validate_schema(SCHEMA) is SCHEMA

    def test_credentials_cached_until_near_expiry(self):
        now = datetime.now(timezone.utc)
        fresh = {"AccessKeyId": "a", "SecretAccessKey": "s", "SessionToken": "t",
                 "Expiration": now + timedelta(hours=1)}
        with patch.object(rule, "sts") as sts, patch.dict(rule._creds_cache, clear=True):
            sts.assume_role.return_value = {"Credentials": fresh}
            rule.member_credentials("111122223333", "aws")
            rule.member_credentials("111122223333", "aws")
            assert sts.assume_role.call_count == 1
            rule._creds_cache["111122223333"] = {**fresh, "Expiration": now + timedelta(minutes=4)}
            rule.member_credentials("111122223333", "aws")
            assert sts.assume_role.call_count == 2


class TestReportHardening:
    def test_tag_counts_keys_with_colons(self):
        rows = [{"annotation": "aws:team: missing; team:owner: invalid ['x: missing']; a: b: empty value"}]
        assert report.tag_counts(rows) == {"aws:team": 1, "team:owner": 1, "a: b": 1}

    def test_ses_recipients_chunked_by_50(self):
        recipients = [f"u{i}@x.com" for i in range(120)]
        with patch.object(report, "AGGREGATOR_NAME", "agg"), patch.object(report, "RULE_NAME", "r"), \
                patch.object(report, "SENDER", "a@x.com"), patch.object(report, "RECIPIENTS", recipients), \
                patch.object(report, "noncompliant_pairs", return_value=[]), \
                patch.object(report, "noncompliant_resources", return_value=[]), \
                patch.object(report, "ses") as ses:
            report.lambda_handler({}, None)
        sizes = [len(c.kwargs["Destination"]["ToAddresses"]) for c in ses.send_email.call_args_list]
        assert sizes == [50, 50, 20]


class TestAreaJReview:
    """Regression tests for review J (compliance rule + SES report)."""

    def test_j1_oversized_result_is_json_serializable(self):
        # boto3 parses configurationItemCaptureTime from history into a datetime;
        # the Lambda runtime JSON-encodes the return value (no datetime support).
        event = _oversized()
        cfg = MagicMock()
        cfg.get_resource_config_history.return_value = {"configurationItems": [{
            "resourceType": "AWS::S3::Bucket", "resourceId": "b1",
            "configurationItemCaptureTime": datetime(2026, 11, 10, tzinfo=timezone.utc),
            "configurationItemStatus": "OK", "tags": GOOD}]}
        with patch.object(rule, "SCHEMA_PARAMETER", "/ti/schema"), \
                patch.object(rule, "EXECUTION_ROLE_NAME", "ti-eval"), \
                patch.object(rule, "load_schema", return_value=SCHEMA), \
                patch.object(rule, "member_config_client", return_value=cfg):
            result = rule.lambda_handler(event, None)
        json.dumps(result)
        assert result["ComplianceType"] == "COMPLIANT"

    def test_j2_single_value_whitespace_is_not_an_allowed_value(self):
        # " Production" is a different tag value (cost bucket) from "Production".
        v = rule.evaluate_tags({**GOOD, "Environment": "Production "}, SCHEMA)
        assert v == ["Environment: invalid ['Production ']"]
        assert rule.evaluate_tags({**GOOD, "Environment": "   "}, SCHEMA) == ["Environment: empty value"]

    @pytest.mark.parametrize("flag", ["false", "true", 1, None])
    def test_j3_multi_value_must_be_boolean(self, flag):
        with pytest.raises(ValueError):
            rule.validate_schema({"requiredTags": [{"key": "A", "multi_value": flag}]})

    def test_j4_long_annotation_keeps_every_tag_key(self):
        long_value = "x" * 240
        tags = {"Environment": long_value, "CustomerName": "ACME+HOOLI"}
        annotation = rule.build_annotation(rule.evaluate_tags(tags, SCHEMA))
        assert len(annotation) <= rule.ANNOTATION_MAX
        counts = report.tag_counts([{"annotation": annotation}])
        assert counts == {"Environment": 1, "CostCenter": 1, "CustomerName": 1}

    def test_j5_org_rule_match_rejects_longer_rule_names(self):
        assert report.rule_matches("OrgConfigRule-ti-tags-abcd1234", "ti-tags")
        assert not report.rule_matches("OrgConfigRule-ti-tags-v2-abcd1234", "ti-tags")
        assert not report.rule_matches("OrgConfigRule-ti-tags-", "ti-tags")


class TestAreaOReview:
    def test_o1_short_value_list_survives_when_another_is_shortened(self):
        tags = {"Environment": "Prod", "CostCenter": "CC-100", "CustomerName": "H" * 250}
        annotation = rule.build_annotation(rule.evaluate_tags(tags, SCHEMA))
        assert len(annotation) <= rule.ANNOTATION_MAX
        assert annotation == "Environment: invalid ['Prod']; CustomerName: invalid [...]"

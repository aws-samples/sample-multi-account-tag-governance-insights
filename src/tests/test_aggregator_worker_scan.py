"""Scan-integrity and aws.tag event tests for aggregator_worker."""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class _NoSuchKey(Exception):
    pass


def _s3(state=None):
    m = MagicMock()
    m.exceptions.NoSuchKey = _NoSuchKey
    m.get_object.side_effect = _NoSuchKey("nf")
    return m


with patch("boto3.client", return_value=_s3()):
    import src.lambda_handlers.aggregator_worker as worker

ACCT, REGION = "123456789012", "us-east-1"
worker.CENTRAL_ANALYTICS_BUCKET = worker.CENTRAL_ANALYTICS_BUCKET or "test-bucket"


def _re_resource(i, tags=None):
    props = [{"Name": "tags", "Data": [{"Key": k, "Value": v} for k, v in (tags or {}).items()]}]
    return {"Arn": f"arn:aws:sqs:{REGION}:{ACCT}:q{i}", "Properties": props}


def _re_client(pages=None, error=None):
    client = MagicMock()
    paginator = MagicMock()
    if error:
        paginator.paginate.side_effect = error
    else:
        paginator.paginate.return_value = iter(pages)
    client.get_paginator.return_value = paginator
    return client


class TestDiscovery:
    def test_uses_list_resources_and_returns_more_than_1000(self):
        pages = [{"Resources": [_re_resource(p * 1000 + i, {"Env": "prod"}) for i in range(1000)]} for p in range(3)]
        client = _re_client(pages)
        with patch.object(worker.boto3, "client", return_value=client), \
                patch.object(worker, "get_org_view_arn", return_value="arn:view/org/x"):
            result = worker.discover_resources_for_account_region(ACCT, REGION)
        assert len(result) == 3000
        client.get_paginator.assert_called_once_with("list_resources")
        kwargs = client.get_paginator.return_value.paginate.call_args.kwargs
        assert kwargs["Filters"] == {"FilterString": f"accountid:{ACCT} region:{REGION}"}
        assert kwargs["ViewArn"] == "arn:view/org/x"

    def test_api_error_raises_and_state_is_not_overwritten(self):
        client = _re_client(error=RuntimeError("ThrottlingException"))
        s3 = _s3()
        with patch.object(worker.boto3, "client", return_value=client), \
                patch.object(worker, "get_org_view_arn", return_value=None), \
                patch.object(worker, "s3", s3):
            with pytest.raises(RuntimeError):
                worker.process_account_region_scan(ACCT, REGION, "scan-1")
        s3.put_object.assert_not_called()

    def test_empty_inventory_records_removals(self):
        previous = {"arn:aws:sqs:us-east-1:123456789012:gone": {"Env": "prod"}}
        with patch.object(worker, "discover_resources_for_account_region", return_value={}), \
                patch.object(worker, "read_state", return_value=previous), \
                patch.object(worker, "write_deltas", return_value="k") as wd, \
                patch.object(worker, "write_state"), \
                patch.object(worker, "notify_results_q"):
            result = worker.process_account_region_scan(ACCT, REGION, "scan-1")
        assert result["removed_resources"] == 1
        assert wd.call_args.args[3][0]["change_type"] == "resource_removed"


def _tag_event(arn, tags, account=ACCT):
    return {"detail-type": "Tag Change on Resource", "source": "aws.tag", "account": account,
            "region": REGION, "resources": [arn],
            "detail": {"changed-tag-keys": list(tags), "service": "sqs", "tags": tags}}


class TestTagChangeEvent:
    ARN = f"arn:aws:sqs:{REGION}:{ACCT}:q1"

    def _run(self, event, previous):
        with patch.object(worker, "read_state", return_value=dict(previous)), \
                patch.object(worker, "write_deltas", return_value="k") as wd, \
                patch.object(worker, "write_state") as ws, \
                patch.object(worker, "notify_results_q"):
            result = worker.lambda_handler(event, None)
        return result, wd, ws

    def test_creation_time_tags_recorded_as_new(self):
        result, wd, ws = self._run(_tag_event(self.ARN, {"Env": "prod"}), {})
        assert result["deltas_written"] == 1
        delta = wd.call_args.args[3][0]
        assert delta["change_type"] == "new_resource" and delta["tags"] == {"Env": "prod"}
        assert ws.call_args.args[2][self.ARN] == {"Env": "prod"}

    def test_full_tag_set_replaces_state(self):
        result, wd, _ = self._run(_tag_event(self.ARN, {"Env": "dev"}), {self.ARN: {"Env": "prod", "Old": "x"}})
        delta = wd.call_args.args[3][0]
        assert delta["change_type"] == "tags_changed"
        assert delta["tags"] == {"Env": "dev"} and delta["previous_tags"] == {"Env": "prod", "Old": "x"}

    def test_unchanged_tags_write_nothing(self):
        result, wd, ws = self._run(_tag_event(self.ARN, {"Env": "prod"}), {self.ARN: {"Env": "prod"}})
        assert result["deltas_written"] == 0
        wd.assert_not_called(); ws.assert_not_called()

    def test_arn_from_other_account_skipped(self):
        result, wd, _ = self._run(_tag_event(self.ARN, {"Env": "prod"}, account="999999999999"), {})
        assert result["deltas_written"] == 0

    def test_sqs_wrapped_event_routed(self):
        import json
        event = {"Records": [{"body": json.dumps(_tag_event(self.ARN, {"Env": "prod"}))}]}
        result, wd, _ = self._run(event, {})
        assert result["total_deltas"] == 1


def test_delta_keys_unique_within_same_second():
    keys = {worker.delta_key(ACCT, REGION, "tag_event_1") for _ in range(50)}
    assert len(keys) == 50
    assert all(k.startswith(f"deltas/account_id={ACCT}/region={REGION}/") and k.endswith(".json") for k in keys)

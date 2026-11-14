"""Offline checks for deployment/scripts/collect-resource-counts.py (sizing helper)."""

import importlib.util
import types
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "deployment" / "scripts" / "collect-resource-counts.py"
ORG_VIEW = "arn:aws:resource-explorer-2:us-east-1:111122223333:view/org/1a2b3c4d"
LOCAL_VIEW = "arn:aws:resource-explorer-2:us-east-1:111122223333:view/all-resources/5e6f7a8b"


def _load_script():
    spec = importlib.util.spec_from_file_location("collect_resource_counts", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Client:
    def __init__(self, views):
        self.views = views
        self.list_resources_calls = []

    def get_paginator(self, name):
        if name == "list_views":
            return types.SimpleNamespace(paginate=lambda **_: [{"Views": self.views}])

        def paginate(**kwargs):
            self.list_resources_calls.append(kwargs)
            return [{"Resources": [{"OwningAccountId": "444455556666", "Region": "us-east-1"}]}]
        return types.SimpleNamespace(paginate=paginate)


def _patch(monkeypatch, module, views):
    client = _Client(views)
    monkeypatch.setattr(module.boto3, "client", lambda name, **_: client)
    return client


def test_counts_use_the_org_view_not_the_default_view(monkeypatch):
    # Without ViewArn, ListResources uses the Region's default view, which
    # normally covers only the account running the script.
    module = _load_script()
    client = _patch(monkeypatch, module, [LOCAL_VIEW, ORG_VIEW])
    counts = module.collect_resource_counts()
    assert counts == {("444455556666", "us-east-1"): 1}
    assert [c["ViewArn"] for c in client.list_resources_calls] == [ORG_VIEW]


def test_explicit_view_arn_is_used_as_given(monkeypatch):
    module = _load_script()
    client = _patch(monkeypatch, module, [])
    module.collect_resource_counts(regions_filter=["us-east-1"], view_arn=LOCAL_VIEW)
    assert client.list_resources_calls == [
        {"MaxResults": 1000, "ViewArn": LOCAL_VIEW, "Filters": {"FilterString": "region:us-east-1"}}]


def test_missing_org_view_fails_instead_of_undercounting(monkeypatch):
    module = _load_script()
    client = _patch(monkeypatch, module, [LOCAL_VIEW])
    with pytest.raises(SystemExit, match="/org/"):
        module.collect_resource_counts()
    assert client.list_resources_calls == []

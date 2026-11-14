"""Offline checks for the inline BucketEmptier custom resource in core-stack.yaml."""

import sys
import types
from pathlib import Path

import pytest
import yaml

CORE = Path(__file__).resolve().parents[2] / "deployment" / "core-stack.yaml"


class _CfnLoader(yaml.SafeLoader):
    pass


def _tag(loader, suffix, node):
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    else:
        value = loader.construct_mapping(node, deep=True)
    return {suffix: value}


_CfnLoader.add_multi_constructor("!", _tag)

RESOURCES = _CfnLoader(CORE.read_text(encoding="utf-8")).get_single_data()["Resources"]
CODE = RESOURCES["BucketEmptierFunction"]["Properties"]["Code"]["ZipFile"]


def _load_emptier_module():
    """Load the inline ZipFile as a module (via a spec) instead of exec()."""
    import importlib.util
    import tempfile
    import os
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as fh:
        fh.write(CODE)
        path = fh.name
    try:
        spec = importlib.util.spec_from_file_location("ti_bucket_emptier_under_test", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        os.unlink(path)


class _S3:
    class exceptions:
        class NoSuchBucket(Exception):
            pass

    def __init__(self):
        self.deleted = []

    def get_paginator(self, _name):
        page = {"Versions": [{"Key": "tag-deltas/recent/x.parquet", "VersionId": "v1"}]}
        return types.SimpleNamespace(paginate=lambda **_: [page])

    def delete_objects(self, Bucket, Delete):
        self.deleted.extend(Delete["Objects"])
        return {}


class _Cfn:
    def __init__(self, status):
        self.status = status

    def describe_stacks(self, StackName):
        return {"Stacks": [{"StackId": StackName, "StackStatus": self.status}]}


def _run(monkeypatch, stack_status, request_type="Delete"):
    s3 = _S3()
    clients = {"s3": s3, "cloudformation": _Cfn(stack_status)}
    responses = []
    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(client=lambda name, **_: clients[name]))
    monkeypatch.setitem(sys.modules, "cfnresponse", types.SimpleNamespace(
        SUCCESS="SUCCESS", FAILED="FAILED",
        send=lambda event, context, status, data, physicalResourceId=None: responses.append(status)))
    namespace = _load_emptier_module().__dict__
    context = types.SimpleNamespace(get_remaining_time_in_millis=lambda: 300000)
    event = {
        "RequestType": request_type,
        "StackId": "arn:aws:cloudformation:us-east-1:111122223333:stack/ti-core/abc",
        "ResourceProperties": {"BucketName": "amzn-s3-demo-bucket"},
    }
    namespace["handler"](event, context)
    return s3.deleted, responses


def test_inline_code_fits_cloudformation_zipfile_limit():
    assert len(CODE) <= 4096


@pytest.mark.parametrize("status", ["DELETE_IN_PROGRESS", "ROLLBACK_IN_PROGRESS"])
def test_stack_delete_empties_bucket(monkeypatch, status):
    deleted, responses = _run(monkeypatch, status)
    assert deleted == [{"Key": "tag-deltas/recent/x.parquet", "VersionId": "v1"}]
    assert responses == ["SUCCESS"]


@pytest.mark.parametrize("status", [
    "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS",
    "UPDATE_ROLLBACK_IN_PROGRESS",
    "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
])
def test_update_that_removes_emptier_keeps_bucket_data(monkeypatch, status):
    # ForceDeleteOnStackRemoval true -> false removes the emptier on update;
    # CloudFormation sends it a Delete, which must not wipe the retained bucket.
    deleted, responses = _run(monkeypatch, status)
    assert deleted == []
    assert responses == ["SUCCESS"]


def test_emptier_role_may_read_its_own_stack_status():
    statements = RESOURCES["BucketEmptierRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
    assert any(s["Action"] == "cloudformation:DescribeStacks" and s["Resource"] == {"Ref": "AWS::StackId"}
               for s in statements)

"""Offline contract checks for the aggregator, core and event-forwarder templates."""

from pathlib import Path

import yaml

DEPLOYMENT = Path(__file__).resolve().parents[2] / "deployment"


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


def _load(name):
    return _CfnLoader((DEPLOYMENT / name).read_text(encoding="utf-8")).get_single_data()


AGG = _load("aggregator-stack.yaml")
RES = AGG["Resources"]


def test_code_signing_is_opt_in_and_never_on_layer_functions():
    assert AGG["Parameters"]["EnableCodeSigning"]["Default"] == "false"
    assert "IsProduction" not in AGG["Conditions"]
    assert RES["LambdaCodeSigningConfig"]["Condition"] == "UseCodeSigning"
    for name, res in RES.items():
        if res["Type"] != "AWS::Lambda::Function":
            continue
        props = res["Properties"]
        if "Layers" in props:
            assert "CodeSigningConfigArn" not in props, name
        else:
            assert props["CodeSigningConfigArn"]["If"][0] == "UseCodeSigning", name
        assert props["Code"]["S3Key"]["If"][0] == "UseCodeSigning", name


def test_pandas_layer_is_overridable():
    layer = RES["AnalyticsWriterLambda"]["Properties"]["Layers"][0]["If"]
    assert layer[0] == "HasPandasLayerArn" and layer[1] == {"Ref": "PandasLayerArn"}
    assert "${AWS::Partition}" in layer[2]["Sub"]


def test_sqs_event_sources_report_batch_item_failures():
    for name in ("AnalyticsWriterEventMapping", "ForwardedEventsEventSourceMapping"):
        assert RES[name]["Properties"]["FunctionResponseTypes"] == ["ReportBatchItemFailures"]
    reserved = RES["WorkerLambda"]["Properties"]["ReservedConcurrentExecutions"]
    esm = RES["ForwardedEventsEventSourceMapping"]["Properties"]["ScalingConfig"]["MaximumConcurrency"]
    assert esm < reserved


def test_worker_async_failures_are_bounded_and_recorded():
    cfg = RES["WorkerEventInvokeConfig"]["Properties"]
    assert cfg["MaximumRetryAttempts"] == 2
    assert cfg["DestinationConfig"]["OnFailure"]["Destination"] == {"GetAtt": "WorkerFailureQueue.Arn"}
    assert RES["WorkerFailureQueue"]["Properties"]["KmsMasterKeyId"]
    statements = RES["WorkerRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
    assert any(s.get("Resource") == {"GetAtt": "WorkerFailureQueue.Arn"} for s in statements)


def test_queue_visibility_covers_worker_timeout_and_single_coordinator():
    worker_timeout = RES["WorkerLambda"]["Properties"]["Timeout"]
    assert RES["ForwardedEventsQueue"]["Properties"]["VisibilityTimeout"] >= 6 * worker_timeout
    assert RES["CoordinatorLambda"]["Properties"]["ReservedConcurrentExecutions"] == 1


def test_hub_rule_accepts_only_aws_sources():
    for branch in RES["ForwardedEventsRule"]["Properties"]["EventPattern"]["$or"]:
        sources = branch["source"]
        assert sources in (["aws.tag"], [{"prefix": "aws."}])


def test_forwarder_covers_rds_family_and_restricts_trust():
    fwd = _load("templates/event-forwarder-stack.yaml")["Resources"]
    names = [list(n.values())[0] for n in fwd["CoreTagForwardingRule"]["Properties"]["EventPattern"]["detail"]["eventName"]]
    for event in ("AddTagsToResource", "RemoveTagsFromResource", "AddTags", "RemoveTags"):
        assert event in names
    trust = fwd["EventForwarderRole"]["Properties"]["AssumeRolePolicyDocument"]["Statement"][0]
    assert trust["Condition"]["StringEquals"]["aws:SourceAccount"] == {"Ref": "AWS::AccountId"}


def test_extended_forwarder_has_no_dead_sources():
    # DocumentDB/Neptune emit source aws.rds; Transit Gateway is an EC2 API (aws.ec2):
    # https://docs.aws.amazon.com/documentdb/latest/devguide/logging-with-cloudtrail.html
    # https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_CreateTransitGateway.html
    fwd = _load("templates/event-forwarder-stack.yaml")["Resources"]
    sources = fwd["ExtendedTagForwardingRule"]["Properties"]["EventPattern"]["source"]
    assert not {"aws.docdb", "aws.neptune-db", "aws.transitgateway"} & set(sources)


def test_forwarder_drops_failed_cloudtrail_calls():
    fwd = _load("templates/event-forwarder-stack.yaml")["Resources"]
    for name in ("CoreTagForwardingRule", "ExtendedTagForwardingRule"):
        detail = fwd[name]["Properties"]["EventPattern"]["detail"]
        assert detail["errorCode"] == [{"exists": False}], name


def test_core_lifecycle_rules_do_not_shorten_aggregator_state():
    rules = _load("core-stack.yaml")["Resources"]["CentralAnalyticsBucket"]["Properties"]["LifecycleConfiguration"]["Rules"]
    assert all(r.get("Prefix") for r in rules), "bucket-wide rules override prefix rules"
    by_prefix = {r["Prefix"]: r for r in rules}
    assert by_prefix["aggregator-state/"]["NoncurrentVersionExpirationInDays"] == 7
    assert by_prefix["tag-deltas/recent/"]["ExpirationInDays"] == 130
    assert by_prefix["quarantine/"]["ExpirationInDays"] == 30


def test_worker_can_call_list_resources():
    # ListResources is authorized by resource-explorer-2:Search; removing it breaks every scan.
    policy = AGG["Resources"]["WorkerRole"]["Properties"]["Policies"][0]["PolicyDocument"]
    actions = {a for st in policy["Statement"] for a in (st["Action"] if isinstance(st["Action"], list) else [st["Action"]])}
    assert {"resource-explorer-2:ListResources", "resource-explorer-2:Search"} <= actions


def test_tag_change_events_table_and_lifecycle():
    core = _load("core-stack.yaml")
    table = core["Resources"]["TagChangeEventsTable"]["Properties"]["TableInput"]
    assert table["Name"] == "tag_change_events"
    assert table["Parameters"]["projection.enabled"] == "true"
    # Tag keys are case-sensitive: the JSON SerDe must not lower-case map keys.
    assert table["StorageDescriptor"]["SerdeInfo"]["Parameters"]["case.insensitive"] == "false"
    cols = [c["Name"] for c in table["StorageDescriptor"]["Columns"]]
    assert {"event_time", "arn", "changed_by", "event_name", "tags_set", "tags_removed"} <= set(cols)
    rules = core["Resources"]["CentralAnalyticsBucket"]["Properties"]["LifecycleConfiguration"]["Rules"]
    assert any(r.get("Prefix") == "tag-change-events/" and r.get("ExpirationInDays") for r in rules)


ALL_TEMPLATES = sorted(DEPLOYMENT.glob("*.yaml")) + sorted((DEPLOYMENT / "templates").glob("*.yaml"))


def _as_list(value):
    return value if isinstance(value, list) else [value]


def _policy_statements(node):
    # Walks every PolicyDocument, including ones inside Fn::ForEach blocks.
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "PolicyDocument" and isinstance(value, dict):
                yield from _as_list(value.get("Statement", []))
            else:
                yield from _policy_statements(value)
    elif isinstance(node, list):
        for item in node:
            yield from _policy_statements(item)


def _iam_statements(template):
    for name, res in template.get("Resources", {}).items():
        for st in _policy_statements(res):
            yield name, st


def test_checkpoint_secret_env_var_absent_from_every_template():
    # A Lambda env var is readable by anyone with lambda:GetFunctionConfiguration.
    offenders = [p.name for p in ALL_TEMPLATES if "CHECKPOINT_SIGNING_SECRET" in p.read_text(encoding="utf-8")]
    assert offenders == []


def test_coordinator_reads_checkpoint_secret_from_secrets_manager():
    secret = RES["CheckpointSecret"]
    assert secret["Type"] == "AWS::SecretsManager::Secret"
    assert secret["Properties"]["SecretString"] == {"Ref": "CheckpointSigningSecret"}
    assert secret["Properties"]["KmsKeyId"] == {"ImportValue": {"Fn::Sub": "ti-messaging-key-arn-${Environment}"}}
    env = RES["CoordinatorLambda"]["Properties"]["Environment"]["Variables"]
    assert env["CHECKPOINT_SECRET_ARN"] == {"Ref": "CheckpointSecret"}
    param = AGG["Parameters"]["CheckpointSigningSecret"]
    assert param["NoEcho"] is True and param["MinLength"] == 32


def test_get_secret_value_scoped_to_checkpoint_secret_only():
    grants = []
    for path in ALL_TEMPLATES:
        for role, st in _iam_statements(_load(path.relative_to(DEPLOYMENT))):
            actions = _as_list(st.get("Action", []))
            if any(a.startswith("secretsmanager:") or a == "*" for a in actions):
                grants.append((path.name, role, actions, st["Resource"]))
    assert grants == [
        ("aggregator-stack.yaml", "CoordinatorRole", ["secretsmanager:GetSecretValue"], {"Ref": "CheckpointSecret"})
    ]
    stmts = RES["CoordinatorRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
    decrypt = [st for st in stmts if "kms:Decrypt" in _as_list(st["Action"])]
    assert len(decrypt) == 1 and decrypt[0]["Resource"] != "*"
    assert decrypt[0]["Condition"]["StringEquals"]["kms:ViaService"] == {
        "Sub": "secretsmanager.${AWS::Region}.${AWS::URLSuffix}"
    }


def test_forwarder_covers_kms_tag_calls():
    # The Worker builds ARNs from the KMS keyId, so the events must be forwarded.
    rule = _load("templates/event-forwarder-stack.yaml")["Resources"]["CoreTagForwardingRule"]
    assert "aws.kms" in rule["Properties"]["EventPattern"]["source"]

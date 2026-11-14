from pathlib import Path

import pytest
import yaml


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ROOT_TEMPLATE = REPOSITORY_ROOT / "deployment" / "deploy.yaml"
COST_TEMPLATE = REPOSITORY_ROOT / "deployment" / "cost-analysis-stack.yaml"
LAMBDA_TEMPLATES = {
    "AggregatorStack": (
        REPOSITORY_ROOT / "deployment" / "aggregator-stack.yaml",
        (
            "aggregator_coordinator",
            "aggregator_worker",
            "analytics_writer",
            "monthly_aggregator",
        ),
    ),
    "CostAnalysisStack": (
        COST_TEMPLATE,
        (
            "cost_allocator",
        ),
    ),
    "TagComplianceStack": (
        REPOSITORY_ROOT / "deployment" / "tag-compliance-stack.yaml",
        ("tag_compliance_rule", "tag_compliance_report"),
    ),
}


def resource_block(template, resource_name, next_resource):
    return template.split(f"  {resource_name}:", 1)[1].split(
        f"\n  {next_resource}:",
        1,
    )[0]


def test_root_declares_and_forwards_all_cur_contract_parameters():
    template = ROOT_TEMPLATE.read_text(encoding="utf-8")
    cost_stack = resource_block(template, "CostAnalysisStack", "DeploymentComplete")

    for parameter in (
        "CurDatabaseName",
        "CurTableName",
        "CurBucketName",
        "CurCatalogAccountId",
    ):
        assert f"\n  {parameter}:\n" in template
        assert f"{parameter}: !Ref {parameter}" in cost_stack

    assert "\n  DeploymentVersion:\n" in template
    assert "\n  CostDeploymentVersion:\n" not in template
    assert "DeploymentVersion: !Ref DeploymentVersion" in cost_stack
    version_section = template.split(
        "\n  DeploymentVersion:\n",
        1,
    )[1].split("\nConditions:", 1)[0]
    assert "AllowedPattern:" in version_section


def test_root_uses_only_versioned_nested_template_urls():
    template = ROOT_TEMPLATE.read_text(encoding="utf-8")
    nested_stacks = {
        "CoreStack": ("AggregatorStack", "core-stack.yaml"),
        "AggregatorStack": ("CostAnalysisStack", "aggregator-stack.yaml"),
        "CostAnalysisStack": ("DeploymentComplete", "cost-analysis-stack.yaml"),
    }
    template_urls = [
        line.strip()
        for line in template.splitlines()
        if line.strip().startswith("TemplateURL:")
    ]

    assert len(template_urls) == len(nested_stacks)
    assert all(
        "templates/v${DeploymentVersion}/" in template_url
        for template_url in template_urls
    )
    for resource_name, (next_resource, template_name) in nested_stacks.items():
        stack = resource_block(template, resource_name, next_resource)
        assert (
            'TemplateURL: !Sub "https://${LambdaCodeBucket}.s3.${AWS::Region}.${AWS::URLSuffix}/'
            f'templates/v${{DeploymentVersion}}/{template_name}"'
            in stack
        )
        assert f"templates/{template_name}" not in template


def test_nested_template_combines_cur_security_and_compatibility_contracts():
    template = COST_TEMPLATE.read_text(encoding="utf-8")

    for parameter in ("CurBucketName", "CurCatalogAccountId", "DeploymentVersion"):
        assert f"\n  {parameter}:\n" in template
    for removed in ("CurAccountRoleArn", "CurExternalId", "CurAthenaResultsBucket", "CUR_ACCOUNT_ROLE_ARN"):
        assert removed not in template
    assert "HasCurDatabaseAndTable:" in template
    assert "${CurDatabaseName}/${CurTableName}" in template
    assert 'arn:${AWS::Partition}:s3:::${CurBucketName}' in template
    assert '"arn:aws:' not in template
    assert "aws-cost-and-usage-*" not in template
    assert template.count("aws:SourceAccount: !Ref AWS::AccountId") >= 1


def test_root_forwards_deployment_version_to_every_lambda_stack():
    template = ROOT_TEMPLATE.read_text(encoding="utf-8")

    # tag-compliance-stack.yaml is deployed standalone in the Config delegated
    # administrator account, not nested under deploy.yaml.
    for stack_name in (n for n in LAMBDA_TEMPLATES if n != "TagComplianceStack"):
        next_resource = {
            "AggregatorStack": "CostAnalysisStack",
            "CostAnalysisStack": "DeploymentComplete",
        }[stack_name]
        stack = resource_block(template, stack_name, next_resource)
        assert "DeploymentVersion: !Ref DeploymentVersion" in stack


def test_all_lambda_artifact_keys_are_versioned_and_immutable():
    expected_functions = set()

    for _, (template_path, functions) in LAMBDA_TEMPLATES.items():
        template = template_path.read_text(encoding="utf-8")
        assert "\n  DeploymentVersion:\n" in template
        assert 'S3Key: "lambda-packages/' not in template
        assert 'S3Key: "signed/' not in template

        for function_name in functions:
            expected_functions.add(function_name)
            assert (
                f'!Sub "lambda-packages/v${{DeploymentVersion}}/'
                f'{function_name}.zip"'
            ) in template
            assert (
                f'!Sub "signed/v${{DeploymentVersion}}/'
                f'{function_name}.zip"'
            ) in template

    assert len(expected_functions) == 7


def test_deployed_allocator_shape_and_named_resources():
    template = COST_TEMPLATE.read_text(encoding="utf-8")
    allocator = resource_block(template, "CostAllocatorLambda", "CostAllocationSchedule")

    for contract in (
        'FunctionName: !Sub "${ProjectName}-cost-alloc-${Environment}-${AWS::AccountId}"',
        "Handler: index.lambda_handler",
        "ReservedConcurrentExecutions: 1",
    ):
        assert contract in allocator
    for resource_name in ("CostAllocatorRole", "CostAllocatorLambda", "CostAllocationSchedule", "TagCostAllocationTable"):
        assert f"\n  {resource_name}:\n" in template
    for removed in ("CostAnalyzerWorkerLambda", "CostAnalyzerCoordinatorLambda", "CostDeduplicatorLambda", "CostAnalysisWorkQueue"):
        assert f"\n  {removed}:\n" not in template


# ── Parsed-template contracts (CloudFormation short-form tags accepted) ────────

class _CfnLoader(yaml.SafeLoader):
    pass


def _cfn_tag(loader, suffix, node):
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    else:
        value = loader.construct_mapping(node, deep=True)
    name = "Ref" if suffix == "Ref" else f"Fn::{suffix}"
    return {name: value}


_CfnLoader.add_multi_constructor("!", _cfn_tag)

TAG_COMPLIANCE_TEMPLATE = REPOSITORY_ROOT / "deployment" / "tag-compliance-stack.yaml"
MEMBER_ROLE_TEMPLATE = REPOSITORY_ROOT / "deployment" / "templates" / "tag-compliance-member-role.yaml"


def load(path):
    return _CfnLoader(path.read_text(encoding="utf-8")).get_single_data()


@pytest.mark.parametrize("path", [ROOT_TEMPLATE, COST_TEMPLATE, TAG_COMPLIANCE_TEMPLATE, MEMBER_ROLE_TEMPLATE])
def test_templates_parse(path):
    assert "Resources" in load(path)


@pytest.mark.parametrize("path", [COST_TEMPLATE, TAG_COMPLIANCE_TEMPLATE])
def test_code_signing_is_opt_in_not_tied_to_environment(path):
    template = load(path)
    param = template["Parameters"]["EnableCodeSigning"]
    assert param["Default"] == "false" and param["AllowedValues"] == ["true", "false"]
    assert template["Conditions"]["UseCodeSigning"] == {"Fn::Equals": [{"Ref": "EnableCodeSigning"}, "true"]}
    assert "IsProduction" not in path.read_text(encoding="utf-8")
    for name, resource in template["Resources"].items():
        if name.startswith("Fn::ForEach"):
            continue
        if resource["Type"] == "AWS::Lambda::CodeSigningConfig":
            assert resource["Condition"] == "UseCodeSigning"
        if resource["Type"] == "AWS::Lambda::Function":
            props = resource["Properties"]
            assert props["CodeSigningConfigArn"]["Fn::If"][0] == "UseCodeSigning"
            assert props["Code"]["S3Key"]["Fn::If"][0] == "UseCodeSigning"


def test_root_forwards_new_parameters_to_nested_stacks():
    template = load(ROOT_TEMPLATE)
    resources = template["Resources"]
    agg = resources["AggregatorStack"]["Properties"]["Parameters"]
    cost = resources["CostAnalysisStack"]["Properties"]["Parameters"]
    core = resources["CoreStack"]["Properties"]["Parameters"]
    for name in ("EnableCodeSigning", "PandasLayerArn", "OrgViewArn"):
        assert agg[name] == {"Ref": name}
    for name in ("EnableCodeSigning", "LogLevel", "CurKmsKeyArn"):
        assert cost[name] == {"Ref": name}
    assert "EnableCodeSigning" not in core  # core-stack.yaml has no such parameter
    cost_params = load(COST_TEMPLATE)["Parameters"]
    assert set(cost) <= set(cost_params)


def test_root_parameter_constraints():
    params = load(ROOT_TEMPLATE)["Parameters"]
    assert params["CheckpointSigningSecret"]["MinLength"] == 32
    assert params["CheckpointSigningSecret"]["NoEcho"] is True
    assert "AllowedValues" not in params["HubRegion"]
    assert "AllowedPattern" in params["HubRegion"]
    for name in ("PandasLayerArn", "OrgViewArn", "CurKmsKeyArn"):
        assert params[name]["Default"] == ""
    assert params["EnableCodeSigning"]["Default"] == "false"


def test_root_rule_requires_cur_bucket_with_cost_analysis():
    rule = load(ROOT_TEMPLATE)["Rules"]["CostAnalysisRequiresCurBucket"]
    assert rule["RuleCondition"] == {"Fn::Equals": [{"Ref": "EnableCostAnalysis"}, "true"]}
    assert rule["Assertions"][0]["Assert"] == {
        "Fn::Not": [{"Fn::Equals": [{"Ref": "CurBucketName"}, "not-configured"]}]}


def test_next_step_branches_on_real_time_events():
    template = load(ROOT_TEMPLATE)
    assert template["Outputs"]["NextStep"]["Value"]["Fn::If"][0] == "EnableRealTimeEventsCondition"
    assert template["Conditions"]["EnableRealTimeEventsCondition"] == {
        "Fn::Equals": [{"Ref": "EnableRealTimeEvents"}, "true"]}


def test_cost_stateless_resources_are_not_retained():
    resources = load(COST_TEMPLATE)["Resources"]
    for name in ("CostAllocatorRole", "CostAllocatorLambda"):
        assert "DeletionPolicy" not in resources[name]
        assert "UpdateReplacePolicy" not in resources[name]


def _policy_statements():
    role = load(COST_TEMPLATE)["Resources"]["CostAllocatorRole"]
    return role["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]


def test_cur_kms_decrypt_is_optional_and_scoped_to_s3():
    template = load(COST_TEMPLATE)
    assert template["Parameters"]["CurKmsKeyArn"]["Default"] == ""
    kms = [s for s in _policy_statements() if "Fn::If" in s and s["Fn::If"][0] == "HasCurKmsKey"]
    assert len(kms) == 1
    statement, otherwise = kms[0]["Fn::If"][1:]
    assert otherwise == {"Ref": "AWS::NoValue"}
    assert statement["Action"] == ["kms:Decrypt"]
    assert statement["Resource"] == [{"Ref": "CurKmsKeyArn"}]
    assert statement["Condition"]["StringEquals"]["kms:ViaService"] == {
        "Fn::Sub": "s3.${AWS::Region}.${AWS::URLSuffix}"}


def test_allocator_can_write_staging_prefix():
    text = COST_TEMPLATE.read_text(encoding="utf-8")
    assert '"${BucketArn}/tag-cost-allocation-staging/*"' in text
    assert '- "tag-cost-allocation-staging/*"' in text


def test_member_role_documents_single_region_deployment():
    assert "ONE StackSet instance per account" in load(MEMBER_ROLE_TEMPLATE)["Description"]


def test_tag_compliance_documents_permission_ceiling():
    template = load(TAG_COMPLIANCE_TEMPLATE)
    assert "20 KB" in template["Parameters"]["MemberAccountIds"]["Description"]
    assert "ponytail" not in TAG_COMPLIANCE_TEMPLATE.read_text(encoding="utf-8")

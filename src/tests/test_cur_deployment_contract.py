from pathlib import Path


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
            "cost_analyzer_coordinator",
            "cost_analyzer_worker",
            "cost_deduplicator",
        ),
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
        "CurAccountRoleArn",
        "CurExternalId",
        "CurAthenaResultsBucket",
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
            'TemplateURL: !Sub "https://s3.amazonaws.com/${LambdaCodeBucket}/'
            f'templates/v${{DeploymentVersion}}/{template_name}"'
            in stack
        )
        assert f"templates/{template_name}" not in template


def test_nested_template_combines_cur_security_and_compatibility_contracts():
    template = COST_TEMPLATE.read_text(encoding="utf-8")

    for parameter in (
        "CurBucketName",
        "CurExternalId",
        "CurAthenaResultsBucket",
        "DeploymentVersion",
    ):
        assert f"\n  {parameter}:\n" in template
    assert "HasCurDatabaseAndTable:" in template
    assert "HasCurAccountRole:" in template
    assert "CUR_ATHENA_RESULTS_BUCKET: !Ref CurAthenaResultsBucket" in template
    assert "CUR_EXTERNAL_ID: !If [HasCurExternalId, !Ref CurExternalId, \"\"]" in template

    cur_glue = template.split(
        "# Glue access — CUR database",
        1,
    )[1].split("# Cross-account CUR role assumption", 1)[0]
    assert "- HasCurDatabaseAndTable" in cur_glue
    assert "${CurDatabaseName}/${CurTableName}" in cur_glue

    assume_role = template.split(
        "# Cross-account CUR role assumption",
        1,
    )[1].split("# S3 access for central analytics bucket", 1)[0]
    assert "- HasCurAccountRole" in assume_role
    assert "sts:AssumeRole" in assume_role
    assert "Resource: !Ref CurAccountRoleArn" in assume_role

    assert 'arn:aws:s3:::${CurBucketName}' in template
    assert "aws-cost-and-usage-*" not in template
    assert "SqsManagedSseEnabled: true" in template
    assert template.count("aws:SourceAccount: !Ref AWS::AccountId") >= 3


def test_root_forwards_deployment_version_to_every_lambda_stack():
    template = ROOT_TEMPLATE.read_text(encoding="utf-8")

    for stack_name in LAMBDA_TEMPLATES:
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


def test_deployed_worker_shape_and_named_resources_are_unchanged():
    template = COST_TEMPLATE.read_text(encoding="utf-8")
    worker = resource_block(
        template,
        "CostAnalyzerWorkerLambda",
        "CostDeduplicatorLambda",
    )

    for contract in (
        'FunctionName: !Sub "${ProjectName}-cost-worker-${Environment}-${AWS::AccountId}"',
        "Runtime: python3.11",
        "Handler: index.lambda_handler",
        "ReservedConcurrentExecutions: 10",
        "AWSSDKPandas-Python311:13",
    ):
        assert contract in worker
    for resource_name in (
        "CostAnalysisWorkQueue",
        "CostAnalysisDeadLetterQueue",
        "CostAnalyzerWorkerRole",
        "CostAnalyzerWorkerLambda",
        "CostAnalyzerWorkerEventMapping",
    ):
        assert f"\n  {resource_name}:\n" in template

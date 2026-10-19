from pathlib import Path
import re


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DOCS_ROOT = REPOSITORY_ROOT / "docs"
CORE_TEMPLATE = REPOSITORY_ROOT / "deployment" / "core-stack.yaml"


def doc_path(relative_path):
    # README.md lives at the repository root; other guides live under docs/.
    root = REPOSITORY_ROOT if relative_path == "README.md" else DOCS_ROOT
    return root / relative_path


def read_doc(relative_path):
    return doc_path(relative_path).read_text(encoding="utf-8")


def test_documented_analytics_identities_match_core_template():
    template = CORE_TEMPLATE.read_text(encoding="utf-8")
    assert 'Name: !Sub "ti_${AWS::Region}_${Environment}_${AWS::AccountId}"' in template
    assert 'Name: !Sub "ti-${AWS::Region}-${Environment}-${AWS::AccountId}"' in template

    documented_identity_files = (
        "ARCHITECTURE.md",
        "DEPLOYMENT.md",
        "OPERATIONS.md",
        "ORCHESTRATION.md",
        "QUICK-START.md",
        "README.md",
        "cost-analysis/QUERY_EXAMPLES.md",
        "cost-analysis/README.md",
    )
    combined = "\n".join(read_doc(path) for path in documented_identity_files)

    assert "ti_REGION_ENV_ACCOUNT" in combined
    assert "ti-REGION-ENV-ACCOUNT" in combined
    assert "taginsights_analytics_" not in combined
    assert "taginsights-analytics-" not in combined
    assert "taginsights_{REGION}_{ENV}_{ACCOUNT}" not in combined
    assert "taginsights_{region}_{env}_{account}" not in combined
    assert "taginsights_REGION_ENV_ACCOUNT" not in combined
    assert 'work-group "ti-analytics-' not in combined
    assert "workgroup: `ti-analytics-" not in combined
    assert "workgroup `ti-analytics-" not in combined


def test_core_usage_guide_matches_table_and_event_contracts():
    usage = read_doc("USAGE.md")

    for contract in (
        "resource_tag_deltas_recent",
        "ti_REGION_ENV_ACCOUNT",
        "ti-REGION-ENV-ACCOUNT",
        "MAP<STRING,STRING>",
        "new_resource",
        "tags_changed",
        "resource_removed",
    ):
        assert contract in usage

    assert "USAGE.md" in read_doc("README.md")
    assert "year = 2026 AND month = 3" not in usage


def test_optional_query_guides_disclose_required_features():
    cost_queries = read_doc("cost-analysis/QUERY_EXAMPLES.md")

    assert "Requires `EnableCostAnalysis=true`" in cost_queries
    assert "[USAGE.md](../USAGE.md)" in cost_queries


def test_query_examples_do_not_pin_a_past_month():
    query_guides = (
        "ARCHITECTURE.md",
        "QUICK-START.md",
        "cost-analysis/README.md",
        "USAGE.md",
    )

    for path in query_guides:
        content = read_doc(path)
        assert "year = 2026 AND month = 3" not in content


def test_changed_guides_have_valid_relative_links():
    changed_guides = (
        "ARCHITECTURE.md",
        "DEPLOYMENT.md",
        "OPERATIONS.md",
        "ORCHESTRATION.md",
        "QUICK-START.md",
        "README.md",
        "USAGE.md",
        "cost-analysis/QUERY_EXAMPLES.md",
        "cost-analysis/README.md",
    )

    for path in changed_guides:
        source = doc_path(path)
        for target in re.findall(r"\[[^\]]+\]\(([^)]+)\)", source.read_text()):
            target_path = target.split("#", 1)[0]
            if not target_path or "://" in target_path or target_path.startswith("mailto:"):
                continue
            assert (source.parent / target_path).resolve().exists(), (
                f"{path} links to missing path {target_path}"
            )


def test_current_deployment_guides_do_not_publish_mutable_lambda_keys():
    deployment_guides = (
        "DEPLOYMENT.md",
        "README.md",
        "QUICK-START.md",
    )

    for path in deployment_guides:
        assert "aws s3 sync deployment/lambda_builds/" not in read_doc(path)

    # README and QUICK-START defer upload steps to DEPLOYMENT.md.
    assert "lambda-packages/v${DEPLOYMENT_VERSION}/" in read_doc("DEPLOYMENT.md")

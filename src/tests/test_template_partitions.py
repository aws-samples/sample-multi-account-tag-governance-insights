"""Every hub template builds ARNs from AWS::Partition, so it deploys outside the aws partition."""

from pathlib import Path

import pytest

DEPLOYMENT = Path(__file__).resolve().parents[2] / "deployment"
TEMPLATES = sorted(DEPLOYMENT.glob("*.yaml")) + sorted((DEPLOYMENT / "templates").glob("*.yaml"))


def test_templates_found():
    assert {p.name for p in TEMPLATES} >= {"core-stack.yaml", "aggregator-stack.yaml", "event-forwarder-stack.yaml"}


@pytest.mark.parametrize("path", TEMPLATES, ids=lambda p: p.name)
def test_no_hardcoded_aws_partition_in_arns(path):
    hardcoded = [
        f"{path.name}:{number}: {line.strip()}"
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if "arn:aws:" in line
    ]
    assert hardcoded == []

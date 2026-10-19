"""
Unit tests for SQL injection validation in the CUR query builder
(src/lambda_handlers/cost/query_builder.py, CURQueryBuilder.build_direct_arn_costs_query).
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lambda_handlers.cost.query_builder import CURQueryBuilder

# --- Fixtures ---


@pytest.fixture
def cost_builder():
    return CURQueryBuilder(database="testdb", table="cur_table")


# --- CURQueryBuilder: service_type validation ---

VALID_ARNS = ["arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0"]


class TestCostBuilderServiceTypeValidation:
    """Tests for _SERVICE_TYPE_RE validation in build_direct_arn_costs_query."""

    @pytest.mark.parametrize(
        "service_type",
        [
            "ec2",
            "s3",
            "lambda",
            "rds-cluster",
            "dynamodb",
            "elasticache",
            "sagemaker-ground-truth",
        ],
    )
    def test_valid_service_type_accepted(self, cost_builder, service_type):
        """Valid service_type values produce a non-empty query."""
        result = cost_builder.build_direct_arn_costs_query(
            normalized_arns=VALID_ARNS,
            service_type=service_type,
            months_back=1,
        )
        assert result  # non-empty string means it passed

    @pytest.mark.parametrize(
        "service_type",
        [
            "'; DROP TABLE--",
            "ec2' OR '1'='1",
            "a" * 100,
            "EC2",  # uppercase not allowed
            "123abc",  # must start with letter
            "ec2; SELECT *",
            "s3\nDROP TABLE",
            "lambda --",
        ],
    )
    def test_sql_injection_service_type_rejected(self, cost_builder, service_type):
        """SQL injection attempts in service_type raise ValueError."""
        with pytest.raises(ValueError, match="Invalid service_type"):
            cost_builder.build_direct_arn_costs_query(
                normalized_arns=VALID_ARNS,
                service_type=service_type,
                months_back=1,
            )

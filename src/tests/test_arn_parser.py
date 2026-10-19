"""
Comprehensive tests for src/utils/arn_parser.py

Covers: parse_arn, extract_region_from_arn, extract_service_from_arn,
        is_global_service, normalize_region, validate_arn
"""

import importlib.util
import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Pre-mock loguru before importing arn_parser (not installed in test env)
sys.modules.setdefault("loguru", MagicMock())

import pytest

# Load arn_parser directly to avoid utils/__init__.py pulling in loguru
_spec = importlib.util.spec_from_file_location(
    "arn_parser",
    os.path.join(os.path.dirname(__file__), "..", "utils", "arn_parser.py"),
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

ARNComponents = _mod.ARNComponents
parse_arn = _mod.parse_arn
extract_region_from_arn = _mod.extract_region_from_arn
extract_service_from_arn = _mod.extract_service_from_arn
is_global_service = _mod.is_global_service
normalize_region = _mod.normalize_region
validate_arn = _mod.validate_arn


# ---------------------------------------------------------------------------
# parse_arn
# ---------------------------------------------------------------------------


class TestParseArn:
    """Tests for the parse_arn function."""

    @pytest.mark.parametrize(
        "arn,expected_service,expected_region,expected_account,expected_rtype,expected_rid",
        [
            # slash-separated resource
            (
                "arn:aws:ssm:eu-north-1:123456789012:patchbaseline/pb-04346f9d",
                "ssm",
                "eu-north-1",
                "123456789012",
                "patchbaseline",
                "pb-04346f9d",
            ),
            # colon-separated resource
            (
                "arn:aws:rds:us-east-1:123456789012:cluster:db-neptune-1",
                "rds",
                "us-east-1",
                "123456789012",
                "cluster",
                "db-neptune-1",
            ),
            # resource with no type (just id)
            (
                "arn:aws:codecommit:us-east-1:123456789012:TagInsights",
                "codecommit",
                "us-east-1",
                "123456789012",
                "",
                "TagInsights",
            ),
            # IAM with empty region
            (
                "arn:aws:iam::123456789012:role/MyRole",
                "iam",
                "",
                "123456789012",
                "role",
                "MyRole",
            ),
            # S3 with empty region and account
            ("arn:aws:s3:::my-bucket", "s3", "", "", "", "my-bucket"),
            # Lambda with function version (nested slash)
            (
                "arn:aws:lambda:us-west-2:123456789012:function:my-function:1",
                "lambda",
                "us-west-2",
                "123456789012",
                "function",
                "my-function:1",
            ),
            # GovCloud partition
            (
                "arn:aws-us-gov:s3:us-gov-west-1:123456789012:bucket/data",
                "s3",
                "us-gov-west-1",
                "123456789012",
                "bucket",
                "data",
            ),
            # China partition
            (
                "arn:aws-cn:ec2:cn-north-1:123456789012:instance/i-abc123",
                "ec2",
                "cn-north-1",
                "123456789012",
                "instance",
                "i-abc123",
            ),
        ],
    )
    def test_valid_arns(
        self,
        arn,
        expected_service,
        expected_region,
        expected_account,
        expected_rtype,
        expected_rid,
    ):
        result = parse_arn(arn)
        assert result is not None
        assert result.service == expected_service
        assert result.region == expected_region
        assert result.account_id == expected_account
        assert result.resource_type == expected_rtype
        assert result.resource_id == expected_rid

    def test_full_resource_field_preserved(self):
        arn = "arn:aws:ssm:us-east-1:111:patchbaseline/pb-123"
        result = parse_arn(arn)
        assert result.full_resource == "patchbaseline/pb-123"

    def test_partition_field(self):
        result = parse_arn("arn:aws-cn:ec2:cn-north-1:111:instance/i-1")
        assert result.partition == "aws-cn"

    @pytest.mark.parametrize(
        "invalid_input",
        [
            None,
            "",
            123,
            "not-an-arn",
            "arn:aws:s3",  # too few parts
            "arn:aws:ec2:us-east-1",  # only 4 colon-separated parts
            "arn:aws:ec2:us-east-1:123",  # only 5 parts
        ],
    )
    def test_invalid_arns_return_none(self, invalid_input):
        result = parse_arn(invalid_input)
        assert result is None

    def test_returns_named_tuple(self):
        result = parse_arn("arn:aws:s3:::my-bucket")
        assert isinstance(result, ARNComponents)
        # Verify named access works
        assert result.service == "s3"
        # Verify index access works
        assert result[1] == "s3"


# ---------------------------------------------------------------------------
# extract_region_from_arn
# ---------------------------------------------------------------------------


class TestExtractRegionFromArn:
    """Tests for extract_region_from_arn."""

    @pytest.mark.parametrize(
        "arn,expected_region",
        [
            ("arn:aws:ec2:us-west-2:123:instance/i-1", "us-west-2"),
            ("arn:aws:ssm:eu-north-1:123:patchbaseline/pb-1", "eu-north-1"),
            ("arn:aws:lambda:ap-southeast-1:123:function:f", "ap-southeast-1"),
        ],
    )
    def test_extracts_region(self, arn, expected_region):
        assert extract_region_from_arn(arn) == expected_region

    def test_returns_none_for_empty_region(self):
        # IAM ARN has empty region
        assert extract_region_from_arn("arn:aws:iam::123:role/Admin") is None

    def test_returns_none_for_invalid_arn(self):
        assert extract_region_from_arn("garbage") is None
        assert extract_region_from_arn("") is None
        assert extract_region_from_arn(None) is None


# ---------------------------------------------------------------------------
# extract_service_from_arn
# ---------------------------------------------------------------------------


class TestExtractServiceFromArn:
    """Tests for extract_service_from_arn."""

    @pytest.mark.parametrize(
        "arn,expected_service",
        [
            ("arn:aws:ec2:us-east-1:123:instance/i-1", "ec2"),
            ("arn:aws:lambda:us-east-1:123:function:f", "lambda"),
            ("arn:aws:s3:::my-bucket", "s3"),
            ("arn:aws:iam::123:role/Admin", "iam"),
        ],
    )
    def test_extracts_service(self, arn, expected_service):
        assert extract_service_from_arn(arn) == expected_service

    def test_returns_none_for_invalid_arn(self):
        assert extract_service_from_arn("invalid") is None
        assert extract_service_from_arn("") is None


# ---------------------------------------------------------------------------
# is_global_service
# ---------------------------------------------------------------------------


class TestIsGlobalService:
    """Tests for is_global_service."""

    @pytest.mark.parametrize(
        "service",
        [
            "iam",
            "cloudfront",
            "route53",
            "waf",
            "organizations",
            "support",
        ],
    )
    def test_global_services(self, service):
        assert is_global_service(service) is True

    @pytest.mark.parametrize(
        "service",
        [
            "IAM",
            "CloudFront",
            "ROUTE53",
            "WAF",
            "ORGANIZATIONS",
            "SUPPORT",
        ],
    )
    def test_case_insensitive(self, service):
        assert is_global_service(service) is True

    @pytest.mark.parametrize(
        "service",
        [
            "ec2",
            "s3",
            "lambda",
            "rds",
            "dynamodb",
            "sqs",
            "sns",
        ],
    )
    def test_regional_services(self, service):
        assert is_global_service(service) is False

    def test_empty_string(self):
        assert is_global_service("") is False


# ---------------------------------------------------------------------------
# normalize_region
# ---------------------------------------------------------------------------


class TestNormalizeRegion:
    """Tests for normalize_region."""

    def test_empty_string_defaults_to_us_east_1(self):
        assert normalize_region("") == "us-east-1"

    def test_none_defaults_to_us_east_1(self):
        assert normalize_region(None) == "us-east-1"

    @pytest.mark.parametrize(
        "region",
        [
            "us-east-1",
            "us-west-2",
            "eu-west-1",
            "ap-southeast-1",
        ],
    )
    def test_non_empty_region_unchanged(self, region):
        assert normalize_region(region) == region


# ---------------------------------------------------------------------------
# validate_arn
# ---------------------------------------------------------------------------


class TestValidateArn:
    """Tests for validate_arn."""

    @pytest.mark.parametrize(
        "arn",
        [
            "arn:aws:s3:::my-bucket",
            "arn:aws:ec2:us-east-1:123456789012:instance/i-1234",
            "arn:aws-cn:ec2:cn-north-1:123:instance/i-1",
            "arn:aws-us-gov:s3:us-gov-west-1:123:bucket/data",
            "arn:aws:iam::123456789012:role/Admin",
        ],
    )
    def test_valid_arns(self, arn):
        assert validate_arn(arn) is True

    @pytest.mark.parametrize(
        "arn,reason",
        [
            (None, "None input"),
            ("", "empty string"),
            (123, "non-string input"),
            ("not-an-arn", "missing arn: prefix"),
            ("arn:aws:ec2:us-east-1:123", "only 5 parts"),
            ("arn:invalid-partition:ec2:us-east-1:123:instance/i-1", "bad partition"),
            ("arn:aws::us-east-1:123:instance/i-1", "empty service"),
            ("arn:aws:ec2:us-east-1:123:", "empty resource"),
        ],
    )
    def test_invalid_arns(self, arn, reason):
        assert validate_arn(arn) is False, f"Expected False for: {reason}"

    def test_partition_aws(self):
        assert validate_arn("arn:aws:s3:::bucket") is True

    def test_partition_aws_cn(self):
        assert validate_arn("arn:aws-cn:s3:::bucket") is True

    def test_partition_aws_us_gov(self):
        assert validate_arn("arn:aws-us-gov:s3:::bucket") is True

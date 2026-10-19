"""
ARN parsing utilities for extracting region and other components from AWS ARNs.
"""

from typing import Optional, NamedTuple

from loguru import logger


class ARNComponents(NamedTuple):
    """Parsed ARN components"""

    partition: str
    service: str
    region: str
    account_id: str
    resource_type: str
    resource_id: str
    full_resource: str


def parse_arn(arn: str) -> Optional[ARNComponents]:
    """
    Parse an AWS ARN into its components.

    ARN format: arn:partition:service:region:account-id:resource-type/resource-id
    or: arn:partition:service:region:account-id:resource-type:resource-id

    Args:
        arn (str): AWS resource ARN

    Returns:
        Optional[ARNComponents]: Parsed ARN components or None if invalid

    Example:
        >>> components = parse_arn("arn:aws:ssm:eu-north-1:1234567890123:patchbaseline/pb-123")
        >>> print(components.region)  # "eu-north-1"
        >>> print(components.service)  # "ssm"
    """
    if not arn or not isinstance(arn, str):
        logger.error(f"Invalid ARN format: {arn}")
        return None

    # Basic ARN validation
    if not arn.startswith("arn:"):
        logger.error(f"ARN must start with 'arn:': {arn}")
        return None

    # Split ARN into main components
    parts = arn.split(":", 5)  # Split into max 6 parts
    if len(parts) < 6:
        logger.error(f"ARN must have at least 6 parts separated by ':': {arn}")
        return None

    partition = parts[1]
    service = parts[2]
    region = parts[3]
    account_id = parts[4]
    full_resource = parts[5]

    # Parse resource part (can be resource-type/resource-id or resource-type:resource-id)
    resource_type = ""
    resource_id = ""

    if "/" in full_resource:
        # Format: resource-type/resource-id
        resource_parts = full_resource.split("/", 1)
        resource_type = resource_parts[0]
        resource_id = resource_parts[1]
    elif ":" in full_resource:
        # Format: resource-type:resource-id
        resource_parts = full_resource.split(":", 1)
        resource_type = resource_parts[0]
        resource_id = resource_parts[1]
    else:
        # Just resource-id (some services don't have resource-type)
        resource_type = ""
        resource_id = full_resource

    return ARNComponents(
        partition=partition,
        service=service,
        region=region,
        account_id=account_id,
        resource_type=resource_type,
        resource_id=resource_id,
        full_resource=full_resource,
    )


def extract_region_from_arn(arn: str) -> Optional[str]:
    """
    Extract the region from an AWS ARN.

    Args:
        arn (str): AWS resource ARN

    Returns:
        Optional[str]: Region name or None if extraction fails

    Example:
        >>> region = extract_region_from_arn("arn:aws:ssm:eu-north-1:123:patchbaseline/pb-123")
        >>> print(region)  # "eu-north-1"
    """
    components = parse_arn(arn)
    if components:
        return components.region if components.region else None
    return None


def extract_service_from_arn(arn: str) -> Optional[str]:
    """
    Extract the service from an AWS ARN.

    Args:
        arn (str): AWS resource ARN

    Returns:
        Optional[str]: Service name or None if extraction fails

    Example:
        >>> service = extract_service_from_arn("arn:aws:ssm:eu-north-1:123:patchbaseline/pb-123")
        >>> print(service)  # "ssm"
    """
    components = parse_arn(arn)
    if components:
        return components.service
    return None


def is_global_service(service: str) -> bool:
    """
    Check if an AWS service is global (doesn't have regional resources).

    Args:
        service (str): AWS service name

    Returns:
        bool: True if the service is global
    """
    global_services = {
        "iam",
        "cloudfront",
        "route53",
        "waf",
        "organizations",
        "support",
    }
    return service.lower() in global_services


def normalize_region(region: str) -> str:
    """
    Normalize region name (handle empty regions for global services).

    Args:
        region (str): Region name from ARN

    Returns:
        str: Normalized region name (us-east-1 for global services)
    """
    if not region or region == "":
        # Global services often have empty region in ARN, default to us-east-1
        return "us-east-1"
    return region


def validate_arn(arn: str) -> bool:
    """
    Validate if a string is a properly formatted AWS ARN.

    Args:
        arn (str): String to validate

    Returns:
        bool: True if valid ARN format
    """
    if not arn or not isinstance(arn, str):
        return False

    # Basic format check
    if not arn.startswith("arn:"):
        return False

    # Must have at least 6 parts when split by ':'
    parts = arn.split(":", 5)
    if len(parts) < 6:
        return False

    # Partition should be 'aws' (or other valid partitions)
    valid_partitions = {"aws", "aws-cn", "aws-us-gov"}
    if parts[1] not in valid_partitions:
        return False

    # Service name should not be empty
    if not parts[2]:
        return False

    # Resource part should not be empty
    if not parts[5]:
        return False

    return True


if __name__ == "__main__":
    # Test the ARN parsing functionality
    test_arns = [
        "arn:aws:ssm:eu-north-1:1234567890123:patchbaseline/pb-04346f9d9ec8ca874",
        "arn:aws:codecommit:us-east-1:1234567890123:TagInsights",
        "arn:aws:rds:us-east-1:1234567890123:cluster:db-neptune-1",
        "arn:aws:iam::1234567890123:role/MyRole",
        "arn:aws:s3:::my-bucket",
        "arn:aws:lambda:us-west-2:123456789012:function:my-function:1",
    ]

    print("=== ARN Parsing Tests ===")
    for arn in test_arns:
        print(f"\nARN: {arn}")
        components = parse_arn(arn)
        if components:
            print(f"  Service: {components.service}")
            print(f"  Region: {components.region}")
            print(f"  Account: {components.account_id}")
            print(f"  Resource Type: {components.resource_type}")
            print(f"  Resource ID: {components.resource_id}")
        else:
            print("  Failed to parse")

        region = extract_region_from_arn(arn)
        print(f"  Extracted Region: {region}")
        print(f"  Normalized Region: {normalize_region(region or '')}")
        print(f"  Valid ARN: {validate_arn(arn)}")

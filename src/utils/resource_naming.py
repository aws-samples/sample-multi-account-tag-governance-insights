#!/usr/bin/env python3
"""
Standardized resource naming utility for TagInsights.

Implements the naming convention: ti-{component}-{region}-{environment}
"""

import os
from typing import Optional

import boto3


class ResourceNamingManager:
    """Centralized resource naming management for TagInsights."""

    def __init__(self, project_name: str = "ti"):
        self.project_name = project_name.lower()

    def get_resource_name(
        self,
        component: str,
        region: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> str:
        """
        Generate standardized resource name.

        Args:
            component (str): Resource component (e.g., 'central-analytics', 'work-queue')
            region (str): AWS region (defaults to current region)
            environment (str): Environment name (defaults to 'prod')

        Returns:
            str: Standardized resource name
        """
        region = region or self._get_current_region()
        environment = environment or self._get_current_environment()

        return f"{self.project_name}-{component}-{region}-{environment}"

    def get_table_name(
        self,
        table_type: str,
        region: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> str:
        """Generate standardized DynamoDB table name."""
        return self.get_resource_name(table_type, region, environment)

    def get_queue_name(
        self,
        queue_type: str,
        region: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> str:
        """Generate standardized SQS queue name (matches CloudFormation templates)."""
        # Don't add "-queue" suffix - CloudFormation templates use simple names
        return self.get_resource_name(queue_type, region, environment)

    def get_lambda_name(
        self,
        function_type: str,
        region: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> str:
        """Generate standardized Lambda function name."""
        return self.get_resource_name(function_type, region, environment)

    def get_bucket_name(
        self,
        bucket_type: str,
        region: Optional[str] = None,
        environment: Optional[str] = None,
        include_account_id: bool = True,
    ) -> str:
        """
        Generate standardized S3 bucket name with account ID for global uniqueness.

        Args:
            bucket_type (str): Type of bucket (e.g., 'cost-data', 'athena-results')
            region (str): AWS region (defaults to current region)
            environment (str): Environment name (defaults to 'prod')
            include_account_id (bool): Include account ID for global uniqueness (default: True)

        Returns:
            str: Standardized bucket name with account ID at the end (matches CloudFormation)
        """
        region = region or self._get_current_region()
        environment = environment or self._get_current_environment()

        if include_account_id:
            account_id = self._get_current_account_id()
            return (
                f"{self.project_name}-{bucket_type}-{region}-{environment}-{account_id}"
            )
        else:
            return f"{self.project_name}-{bucket_type}-{region}-{environment}"

    def get_lambda_name_with_account(
        self,
        function_type: str,
        region: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> str:
        """
        Generate standardized Lambda function name with account ID for global uniqueness.

        Args:
            function_type (str): Type of function (e.g., 'orchestrator', 'regional-worker')
            region (str): AWS region (defaults to current region)
            environment (str): Environment name (defaults to 'prod')

        Returns:
            str: Standardized Lambda function name with account ID at the end
        """
        region = region or self._get_current_region()
        environment = environment or self._get_current_environment()
        account_id = self._get_current_account_id()

        return (
            f"{self.project_name}-{function_type}-{region}-{environment}-{account_id}"
        )

    def get_role_name(
        self,
        role_type: str,
        region: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> str:
        """Generate standardized IAM role name."""
        component = f"{role_type}-role"
        return self.get_resource_name(component, region, environment)

    def get_stack_name(
        self,
        stack_type: str,
        region: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> str:
        """Generate standardized CloudFormation stack name."""
        component = f"{stack_type}-stack"
        return self.get_resource_name(component, region, environment)

    def get_dashboard_name(
        self,
        dashboard_type: str,
        region: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> str:
        """Generate standardized CloudWatch dashboard name."""
        component = f"{dashboard_type}-dashboard"
        return self.get_resource_name(component, region, environment)

    def get_alarm_name(
        self,
        alarm_type: str,
        region: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> str:
        """Generate standardized CloudWatch alarm name."""
        component = f"{alarm_type}-alarm"
        return self.get_resource_name(component, region, environment)

    def get_topic_name(
        self,
        topic_type: str,
        region: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> str:
        """Generate standardized SNS topic name."""
        component = f"{topic_type}-topic"
        return self.get_resource_name(component, region, environment)

    def get_rule_name(
        self,
        rule_type: str,
        region: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> str:
        """Generate standardized EventBridge rule name."""
        component = f"{rule_type}-rule"
        return self.get_resource_name(component, region, environment)

    def get_database_name(
        self,
        database_type: str = "cost_analytics",
        region: Optional[str] = None,
        environment: Optional[str] = None,
        include_account_id: bool = True,
    ) -> str:
        """
        Generate standardized Athena/Glue database name with proper hyphen handling.

        Args:
            database_type: Type of database (default: 'cost_analytics')
            region: AWS region (defaults to current region)
            environment: Environment name (defaults to current environment)
            include_account_id: Include account ID for global uniqueness (default: True)

        Returns:
            str: Standardized database name with underscores (Athena-compatible)
        """
        region = region or self._get_current_region()
        environment = environment or self._get_current_environment()

        # Convert hyphens to underscores for Athena compatibility
        region_underscore = region.replace("-", "_")

        if include_account_id:
            account_id = self._get_current_account_id()
            return f"{self.project_name}_{database_type}_{region_underscore}_{environment}_{account_id}"
        else:
            return (
                f"{self.project_name}_{database_type}_{region_underscore}_{environment}"
            )

    def parse_resource_name(self, resource_name: str) -> dict:
        """
        Parse a standardized resource name into components.

        Args:
            resource_name (str): Resource name to parse

        Returns:
            dict: Parsed components
        """
        parts = resource_name.split("-")

        if len(parts) < 4:
            raise ValueError(f"Invalid resource name format: {resource_name}")

        # Handle cases where region and component have multiple parts
        project = parts[0]
        environment = parts[-1]

        # Common AWS region patterns to identify region correctly
        common_regions = [
            "us-east-1",
            "us-east-2",
            "us-west-1",
            "us-west-2",
            "eu-west-1",
            "eu-west-2",
            "eu-west-3",
            "eu-central-1",
            "ap-southeast-1",
            "ap-southeast-2",
            "ap-northeast-1",
            "ca-central-1",
            "sa-east-1",
        ]

        # Try to identify region by matching common patterns
        region = None
        region_start_idx = None

        for i in range(len(parts) - 1):  # Don't include the last part (environment)
            for region_pattern in common_regions:
                region_parts = region_pattern.split("-")
                if i + len(region_parts) < len(parts):
                    candidate_region = "-".join(parts[i : i + len(region_parts)])
                    if candidate_region == region_pattern:
                        region = candidate_region
                        region_start_idx = i
                        break
            if region:
                break

        # If no standard region found, assume last 2 parts before environment are region
        if not region:
            if len(parts) >= 5:  # ti-component-region1-region2-env
                region = f"{parts[-3]}-{parts[-2]}"
                region_start_idx = len(parts) - 3
            else:  # ti-component-region-env
                region = parts[-2]
                region_start_idx = len(parts) - 2

        # Component is everything between project and region
        component_end_idx = region_start_idx
        component = "-".join(parts[1:component_end_idx])

        return {
            "project": project,
            "component": component,
            "region": region,
            "environment": environment,
            "full_name": resource_name,
        }

    def validate_resource_name(self, resource_name: str) -> bool:
        """
        Validate that a resource name follows the standard convention.

        Args:
            resource_name (str): Resource name to validate

        Returns:
            bool: True if valid
        """
        try:
            parsed = self.parse_resource_name(resource_name)

            # Check project name matches
            if parsed["project"] != self.project_name:
                return False

            # Check all parts are non-empty and lowercase
            for key, value in parsed.items():
                if key == "full_name":
                    continue
                if not value or value != value.lower():
                    return False

            return True

        except Exception:
            return False

    def _get_current_region(self) -> str:
        """Get current AWS region from environment or default."""
        return os.environ.get("AWS_REGION") or os.environ.get(
            "AWS_DEFAULT_REGION", "us-east-1"
        )

    def _get_current_environment(self) -> str:
        """Get current environment from environment variables or default."""
        return os.environ.get("ENVIRONMENT", "prod")

    def _get_current_account_id(self) -> str:
        """Get current AWS account ID."""
        try:
            # Try to get from STS
            sts_client = boto3.client("sts")
            return sts_client.get_caller_identity()["Account"]
        except Exception:
            # Fallback to environment variable if available
            account_id = os.environ.get("AWS_ACCOUNT_ID")
            if account_id:
                return account_id
            # If all else fails, raise an exception with helpful message
            raise ValueError(
                "Unable to determine AWS account ID. Please ensure AWS credentials are configured "
                "or set the AWS_ACCOUNT_ID environment variable."
            )


# Global instance for convenience
naming_manager = ResourceNamingManager()


# Convenience functions
def get_resource_name(
    component: str, region: Optional[str] = None, environment: Optional[str] = None
) -> str:
    """Generate standardized resource name using global manager."""
    return naming_manager.get_resource_name(component, region, environment)


def get_table_name(
    table_type: str, region: Optional[str] = None, environment: Optional[str] = None
) -> str:
    """Generate standardized DynamoDB table name using global manager."""
    return naming_manager.get_table_name(table_type, region, environment)


def get_queue_name(
    queue_type: str, region: Optional[str] = None, environment: Optional[str] = None
) -> str:
    """Generate standardized SQS queue name using global manager (matches CloudFormation)."""
    return naming_manager.get_queue_name(queue_type, region, environment)


def get_lambda_name(
    function_type: str, region: Optional[str] = None, environment: Optional[str] = None
) -> str:
    """Generate standardized Lambda function name using global manager."""
    return naming_manager.get_lambda_name(function_type, region, environment)


def get_role_name(
    role_type: str, region: Optional[str] = None, environment: Optional[str] = None
) -> str:
    """Generate standardized IAM role name using global manager."""
    return naming_manager.get_role_name(role_type, region, environment)


def get_rule_name(
    rule_type: str, region: Optional[str] = None, environment: Optional[str] = None
) -> str:
    """Generate standardized EventBridge rule name using global manager."""
    return naming_manager.get_rule_name(rule_type, region, environment)


def get_bucket_name(
    bucket_type: str,
    region: Optional[str] = None,
    environment: Optional[str] = None,
    include_account_id: bool = True,
) -> str:
    """Generate standardized S3 bucket name with account ID at the end (matches CloudFormation) using global manager."""
    return naming_manager.get_bucket_name(
        bucket_type, region, environment, include_account_id
    )


def get_lambda_name_with_account(
    function_type: str, region: Optional[str] = None, environment: Optional[str] = None
) -> str:
    """Generate standardized Lambda function name with account ID for global uniqueness using global manager."""
    return naming_manager.get_lambda_name_with_account(
        function_type, region, environment
    )


def parse_resource_name(resource_name: str) -> dict:
    """Parse a standardized resource name into components using global manager."""
    return naming_manager.parse_resource_name(resource_name)


def validate_resource_name(resource_name: str) -> bool:
    """Validate that a resource name follows the standard convention using global manager."""
    return naming_manager.validate_resource_name(resource_name)


def get_database_name(
    database_type: str = "cost_analytics",
    region: Optional[str] = None,
    environment: Optional[str] = None,
    include_account_id: bool = True,
) -> str:
    """Generate standardized Athena/Glue database name with proper hyphen handling using global manager."""
    return naming_manager.get_database_name(
        database_type, region, environment, include_account_id
    )


if __name__ == "__main__":
    # Test the naming utility
    print("=== TagInsights Resource Naming Utility Test ===")

    # Test standard naming
    print("\n1. Standard Resource Names:")
    print(f"Central Analytics Table: {get_table_name('central-analytics')}")
    print(f"Work Queue: {get_queue_name('work')}")
    print(f"Orchestrator: {get_lambda_name('orchestrator')}")
    print(f"Regional Worker Role: {get_role_name('regional-worker')}")
    print(f"EventBridge Rule: {get_rule_name('resource-changes')}")

    # Test with different regions/environments
    print("\n2. Different Regions/Environments:")
    print(f"Dev Work Queue: {get_queue_name('work', 'us-west-2', 'dev')}")
    print(
        f"Staging State Table: {get_table_name('regional-state', 'eu-west-1', 'staging')}"
    )

    # Test parsing
    print("\n3. Name Parsing:")
    test_name = "ti-work-queue-us-east-1-prod"
    parsed = parse_resource_name(test_name)
    print(f"Parsed '{test_name}':")
    for key, value in parsed.items():
        print(f"  {key}: {value}")

    # Test validation
    print("\n4. Name Validation:")
    valid_names = ["ti-work-queue-us-east-1-prod", "ti-central-analytics-us-west-2-dev"]
    invalid_names = [
        "TagInsights-work-us-east-1-prod",  # Mixed case
        "ti-work",  # Missing parts
        "other-work-queue-us-east-1-prod",  # Wrong project
    ]

    for name in valid_names:
        print(f"  ✓ {name}: {validate_resource_name(name)}")

    for name in invalid_names:
        print(f"  ✗ {name}: {validate_resource_name(name)}")

import re
from abc import ABC, abstractmethod
from typing import Dict, List

from botocore.exceptions import ClientError
from loguru import logger


class BaseTagRetriever(ABC):
    """
    Abstract base class for AWS resource tag retrievers.
    Each AWS service should implement this interface.
    """

    def __init__(self, region_name: str = "us-east-1"):
        """
        Initialize the tag retriever.

        Args:
            region_name (str): AWS region name
        """
        self.region_name = region_name
        self._client = None

    @property
    def client(self):
        """Lazy initialization of AWS client"""
        if self._client is None:
            self._client = self._create_client()
        return self._client

    @abstractmethod
    def _create_client(self):
        """Create the appropriate AWS service client"""
        pass

    @abstractmethod
    def get_service_name(self) -> str:
        """Return the AWS service name this retriever handles"""
        pass

    @abstractmethod
    def can_handle_arn(self, resource_arn: str) -> bool:
        """
        Check if this retriever can handle the given resource ARN.

        Args:
            resource_arn (str): The resource ARN to check

        Returns:
            bool: True if this retriever can handle the ARN
        """
        pass

    @abstractmethod
    def get_tags(self, resource_arn: str) -> Dict[str, str]:
        """
        Retrieve tags for a specific resource.

        Args:
            resource_arn (str): The resource ARN

        Returns:
            Dict[str, str]: Dictionary of tag key-value pairs
        """
        pass

    def get_tags_safe(self, resource_arn: str) -> Dict[str, str]:
        """
        Safely retrieve tags with error handling and stale resource detection.

        Args:
            resource_arn (str): The resource ARN

        Returns:
            Dict[str, str]: Dictionary of tag key-value pairs, empty dict if error
        """
        try:
            return self.get_tags(resource_arn)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            error_message = str(e)

            if error_code == "AccessDeniedException":
                # Expected when Resource Explorer is primary and service-specific fallback lacks permissions
                # Resource will be caught on next Resource Explorer index sync (1-2 hours) or weekly drift scan
                logger.debug(
                    f"Service fallback unavailable for {resource_arn} (expected with Resource Explorer-first architecture)"
                )
                return {}
            elif self._is_stale_resource_error(error_code, error_message):
                # Log as warning (not error) and track for cleanup visibility
                logger.warning(
                    f"Stale resource detected: {resource_arn} - Resource no longer exists but still in Resource Explorer index"
                )
                self._track_stale_resource(resource_arn, error_code, error_message)
                return {}
            else:
                # Real errors still logged as ERROR
                logger.error(f"ClientError getting tags for {resource_arn}: {e}")
                return {}
        except Exception as e:
            logger.error(f"Unexpected error getting tags for {resource_arn}: {e}")
            return {}

    def _is_stale_resource_error(self, error_code: str, error_message: str) -> bool:
        """
        Detect if an error indicates a stale/deleted resource.

        Args:
            error_code (str): AWS error code
            error_message (str): Full error message

        Returns:
            bool: True if this appears to be a stale resource error
        """
        # Common error codes for deleted/non-existent resources
        stale_error_codes = {
            "InvalidParameterValue",  # RDS snapshots
            "ResourceNotFoundException",
            "NoSuchEntity",  # IAM resources
            "NoSuchBucket",  # S3 buckets
            "InvalidInstanceID.NotFound",  # EC2 instances
            "DBInstanceNotFound",  # RDS instances
            "DBClusterNotFound",  # RDS clusters
            "InvalidDBSnapshotState",  # RDS snapshots
            "ClusterNotFound",  # Various cluster resources
        }

        # Common error message patterns for non-existent resources
        stale_message_patterns = [
            r"Unable to find.*matching.*resource",
            r"does not exist",
            r"not found",
            r"No such.*entity",
            r"Invalid.*ID.*NotFound",
            r"resource.*not.*found",
            r"cannot be found",
        ]

        # Check error code first
        if error_code in stale_error_codes:
            return True

        # Check message patterns
        for pattern in stale_message_patterns:
            if re.search(pattern, error_message, re.IGNORECASE):
                return True

        return False

    def _track_stale_resource(
        self, resource_arn: str, error_code: str, error_message: str
    ):
        """
        Track a stale resource for cleanup reporting.

        This is a simple implementation that tries to access the state manager
        if available (in aggregator worker context) or falls back to local tracking.

        Args:
            resource_arn (str): The stale resource ARN
            error_code (str): AWS error code
            error_message (str): Full error message
        """
        try:
            # Try to get state manager from TagManager if available
            # This is a simple approach that works within the existing architecture
            from ..tag_manager import TagManager

            # Create minimal TagManager just for state tracking
            temp_manager = TagManager(
                self.region_name, enable_optimization=True
            )  # Regional workers have state manager

            if temp_manager.state_manager:
                # Mark resource as stale in the state table
                temp_manager.state_manager.mark_resource_as_stale(
                    resource_arn, error_code, error_message
                )

        except Exception as tracking_error:
            # If state tracking fails, just log it for debugging (don't break the main flow)
            logger.debug(
                f"Could not track stale resource {resource_arn}: {tracking_error}"
            )

    def get_batch_tags(self, resource_arns: List[str]) -> Dict[str, Dict[str, str]]:
        """
        Retrieve tags for multiple resources.
        Default implementation calls get_tags_safe for each resource.
        Subclasses can override for batch operations if supported.

        Args:
            resource_arns (List[str]): List of resource ARNs

        Returns:
            Dict[str, Dict[str, str]]: Mapping of ARN to tag dictionary
        """
        result = {}
        for arn in resource_arns:
            if self.can_handle_arn(arn):
                result[arn] = self.get_tags_safe(arn)
        return result

    def __str__(self):
        return f"{self.__class__.__name__}(service={self.get_service_name()}, region={self.region_name})"

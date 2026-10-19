from typing import Dict, List, Optional

from loguru import logger

from .base import BaseTagRetriever
from .registry import TagRetrieverRegistry
from ..utils.arn_parser import (
    extract_region_from_arn,
    normalize_region,
    is_global_service,
    extract_service_from_arn,
)


class MultiRegionTagRetrieverRegistry:
    """
    Registry for managing tag retrievers across multiple AWS regions.
    Creates and caches region-specific retrievers on demand.
    """

    def __init__(self, default_region: str = "us-east-1"):
        """
        Initialize the multi-region registry.

        Args:
            default_region (str): Default AWS region to use
        """
        self.default_region = default_region
        self._regional_registries: Dict[str, TagRetrieverRegistry] = {}
        logger.info(
            f"MultiRegionTagRetrieverRegistry initialized with default region: {default_region}"
        )

    def get_registry_for_region(self, region: str) -> TagRetrieverRegistry:
        """
        Get or create a TagRetrieverRegistry for the specified region.

        Args:
            region (str): AWS region name

        Returns:
            TagRetrieverRegistry: Registry for the specified region
        """
        if region not in self._regional_registries:
            logger.info(f"Creating new TagRetrieverRegistry for region: {region}")
            self._regional_registries[region] = TagRetrieverRegistry(region)

        return self._regional_registries[region]

    def get_retriever_for_arn(self, resource_arn: str) -> Optional[BaseTagRetriever]:
        """
        Get the most appropriate retriever for a resource ARN, considering its region.

        Args:
            resource_arn (str): The resource ARN

        Returns:
            Optional[BaseTagRetriever]: The best retriever for this ARN, or None if none found
        """
        # Extract region from ARN
        arn_region = extract_region_from_arn(resource_arn)
        service = extract_service_from_arn(resource_arn)

        # Determine the correct region to use
        if service and is_global_service(service):
            # Global services typically use us-east-1
            target_region = "us-east-1"
            logger.debug(f"Using us-east-1 for global service {service}")
        elif arn_region:
            # Use the region from the ARN
            target_region = normalize_region(arn_region)
            logger.debug(f"Using region {target_region} extracted from ARN")
        else:
            # Fall back to default region
            target_region = self.default_region
            logger.debug(
                f"Using default region {target_region} for ARN: {resource_arn}"
            )

        # Get the appropriate regional registry
        registry = self.get_registry_for_region(target_region)
        retriever = registry.get_retriever_for_arn(resource_arn)

        if retriever:
            logger.debug(
                f"Found retriever {retriever.get_service_name()} in region {target_region} for ARN: {resource_arn}"
            )
        else:
            logger.warning(
                f"No retriever found for ARN in region {target_region}: {resource_arn}"
            )

        return retriever

    def get_all_regions(self) -> List[str]:
        """
        Get list of all regions with active registries.

        Returns:
            List[str]: List of region names
        """
        return list(self._regional_registries.keys())

    def get_supported_services(self) -> List[str]:
        """
        Get list of supported service names (from default region).

        Returns:
            List[str]: List of supported AWS service names
        """
        default_registry = self.get_registry_for_region(self.default_region)
        return default_registry.get_supported_services()

    def clear_registries(self):
        """Clear all regional registries (useful for testing)"""
        self._regional_registries.clear()
        logger.info("Cleared all regional registries")

    def get_registry_stats(self) -> Dict[str, int]:
        """
        Get statistics about active regional registries.

        Returns:
            Dict[str, int]: Mapping of region to service count
        """
        stats = {}
        for region, registry in self._regional_registries.items():
            stats[region] = len(registry.get_supported_services())
        return stats

    def preload_regions(self, regions: List[str]):
        """
        Preload registries for specified regions.

        Args:
            regions (List[str]): List of regions to preload
        """
        logger.info(f"Preloading registries for regions: {regions}")
        for region in regions:
            self.get_registry_for_region(region)
        logger.info(f"Preloaded {len(regions)} regional registries")

    def register_retriever(self, retriever: BaseTagRetriever, region: str = None):
        """
        Register a custom retriever in a specific region.

        Args:
            retriever (BaseTagRetriever): Custom retriever to register
            region (str): Region to register the retriever in (defaults to retriever's region)
        """
        target_region = region or getattr(retriever, "region_name", self.default_region)
        registry = self.get_registry_for_region(target_region)
        registry.register_retriever(retriever)
        logger.info(
            f"Registered custom retriever {retriever.get_service_name()} in region {target_region}"
        )

    def get_comprehensive_retriever(self):
        """
        Get comprehensive discovery retriever from default region registry

        Returns:
            Optional[ResourceExplorerTagRetriever]: Resource Explorer retriever if available
        """
        default_registry = self.get_registry_for_region(self.default_region)
        if hasattr(default_registry, "get_comprehensive_retriever"):
            return default_registry.get_comprehensive_retriever()
        return None

    def is_comprehensive_discovery_available(self) -> bool:
        """
        Check if comprehensive discovery is available in default region

        Returns:
            bool: True if Resource Explorer comprehensive discovery is available
        """
        default_registry = self.get_registry_for_region(self.default_region)
        if hasattr(default_registry, "is_comprehensive_discovery_available"):
            return default_registry.is_comprehensive_discovery_available()
        return False

    def __str__(self):
        active_regions = len(self._regional_registries)
        return f"MultiRegionTagRetrieverRegistry(default={self.default_region}, active_regions={active_regions})"


# Commonly used AWS regions for preloading
COMMON_AWS_REGIONS = [
    "us-east-1",  # US East (N. Virginia)
    "us-east-2",  # US East (Ohio)
    "us-west-1",  # US West (N. California)
    "us-west-2",  # US West (Oregon)
    "eu-west-1",  # Europe (Ireland)
    "eu-west-2",  # Europe (London)
    "eu-west-3",  # Europe (Paris)
    "eu-central-1",  # Europe (Frankfurt)
    "eu-north-1",  # Europe (Stockholm)
    "ap-northeast-1",  # Asia Pacific (Tokyo)
    "ap-northeast-2",  # Asia Pacific (Seoul)
    "ap-southeast-1",  # Asia Pacific (Singapore)
    "ap-southeast-2",  # Asia Pacific (Sydney)
    "ap-south-1",  # Asia Pacific (Mumbai)
    "ca-central-1",  # Canada (Central)
    "sa-east-1",  # South America (São Paulo)
]


def get_available_regions() -> List[str]:
    """
    Get list of available AWS regions.

    Returns:
        List[str]: List of AWS region names
    """
    return COMMON_AWS_REGIONS.copy()


if __name__ == "__main__":
    # Test the multi-region registry
    registry = MultiRegionTagRetrieverRegistry()

    test_arns = [
        "arn:aws:ssm:eu-north-1:1234567890123:patchbaseline/pb-04346f9d9ec8ca874",
        "arn:aws:codecommit:us-east-1:1234567890123:TagInsights",
        "arn:aws:rds:us-west-2:1234567890123:cluster:db-neptune-1",
        "arn:aws:iam::1234567890123:role/MyRole",  # Global service
        "arn:aws:s3:::my-bucket",  # Global service
    ]

    print("=== Multi-Region Registry Tests ===")
    for arn in test_arns:
        print(f"\nTesting ARN: {arn}")
        retriever = registry.get_retriever_for_arn(arn)
        if retriever:
            print(f"  Found retriever: {retriever.get_service_name()}")
            print(f"  Retriever region: {retriever.region_name}")
        else:
            print("  No retriever found")

    print(f"\nActive regions: {registry.get_all_regions()}")
    print(f"Registry stats: {registry.get_registry_stats()}")

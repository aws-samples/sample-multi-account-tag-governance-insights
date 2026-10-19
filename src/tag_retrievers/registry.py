from typing import List, Optional

from loguru import logger

from .base import BaseTagRetriever
from .resource_explorer import ResourceExplorerTagRetriever
from .resource_groups import ResourceGroupsTagRetriever


class TagRetrieverRegistry:
    """
    Registry for managing tag retrievers.
    Simplified to use only free AWS APIs:
    - ResourceGroupsTaggingAPI (free, works for all services)
    - Resource Explorer (free, enables comprehensive discovery)
    """

    def __init__(self, region_name: str = "us-east-1"):
        """
        Initialize the registry with default retrievers.

        Args:
            region_name (str): AWS region name
        """
        self.region_name = region_name
        self._retrievers: List[BaseTagRetriever] = []
        self._fallback_retriever: Optional[BaseTagRetriever] = None

        # Register default retrievers
        self._register_default_retrievers()

    def _register_default_retrievers(self):
        """Register all default tag retrievers"""
        # Initialize comprehensive discovery capability
        self._comprehensive_discovery_available = False
        self._comprehensive_retriever = None

        # Register comprehensive discovery retriever (Resource Explorer 2)
        # This enables finding both tagged and untagged resources
        try:
            explorer_retriever = ResourceExplorerTagRetriever(self.region_name)
            if explorer_retriever.validate_setup():
                self.register_retriever(explorer_retriever)
                self._comprehensive_discovery_available = True
                self._comprehensive_retriever = explorer_retriever
                logger.info(
                    f"Resource Explorer comprehensive discovery enabled in {self.region_name}"
                )
            else:
                self._comprehensive_discovery_available = False
                logger.info(
                    f"Resource Explorer not available in {self.region_name} - using standard discovery"
                )
        except Exception as e:
            logger.warning(f"Could not initialize Resource Explorer: {e}")
            self._comprehensive_discovery_available = False

        # Register fallback retriever (Resource Groups Tagging API)
        # This is the universal retriever that works for ALL AWS services
        # Uses only free APIs (no service-specific permissions needed)
        self._fallback_retriever = ResourceGroupsTagRetriever(self.region_name)

        total_retrievers = len(self._retrievers)
        explorer_status = (
            "with Resource Explorer"
            if self._comprehensive_discovery_available
            else "standard mode"
        )
        logger.info(
            f"Registry initialized with {total_retrievers} specialized retrievers and universal fallback ({explorer_status})"
        )

    def register_retriever(self, retriever: BaseTagRetriever):
        """
        Register a new tag retriever.

        Args:
            retriever (BaseTagRetriever): The retriever to register
        """
        self._retrievers.append(retriever)
        logger.debug(f"Registered retriever: {retriever}")

    def get_retriever_for_arn(self, resource_arn: str) -> Optional[BaseTagRetriever]:
        """
        Get the most appropriate retriever for a resource ARN.

        Args:
            resource_arn (str): The resource ARN

        Returns:
            Optional[BaseTagRetriever]: The best retriever for this ARN, or fallback retriever
        """
        # First, try specialized retrievers (e.g., Resource Explorer)
        for retriever in self._retrievers:
            if retriever.can_handle_arn(resource_arn):
                logger.debug(
                    f"Found specialized retriever {retriever} for ARN: {resource_arn}"
                )
                return retriever

        # Fall back to Resource Groups Tagging API (works for all services)
        if self._fallback_retriever and self._fallback_retriever.can_handle_arn(
            resource_arn
        ):
            logger.debug(
                f"Using universal Resource Groups retriever for ARN: {resource_arn}"
            )
            return self._fallback_retriever

        logger.warning(f"No retriever found for ARN: {resource_arn}")
        return None

    def get_all_retrievers(self) -> List[BaseTagRetriever]:
        """
        Get all registered retrievers.

        Returns:
            List[BaseTagRetriever]: List of all retrievers
        """
        retrievers = self._retrievers.copy()
        if self._fallback_retriever:
            retrievers.append(self._fallback_retriever)
        return retrievers

    def get_supported_services(self) -> List[str]:
        """
        Get list of supported service names.

        Returns:
            List[str]: List of supported AWS service names
        """
        services = [retriever.get_service_name() for retriever in self._retrievers]
        if self._fallback_retriever:
            services.append(self._fallback_retriever.get_service_name())
        return services

    def clear_retrievers(self):
        """Clear all registered retrievers (useful for testing)"""
        self._retrievers.clear()
        self._fallback_retriever = None

    def set_fallback_retriever(self, retriever: BaseTagRetriever):
        """
        Set the fallback retriever.

        Args:
            retriever (BaseTagRetriever): The fallback retriever
        """
        self._fallback_retriever = retriever
        logger.debug(f"Set fallback retriever: {retriever}")

    def get_comprehensive_retriever(self) -> Optional["ResourceExplorerTagRetriever"]:
        """
        Get the comprehensive discovery retriever if available

        Returns:
            Optional[ResourceExplorerTagRetriever]: Resource Explorer retriever or None
        """
        return getattr(self, "_comprehensive_retriever", None)

    def is_comprehensive_discovery_available(self) -> bool:
        """
        Check if comprehensive discovery (including untagged resources) is available

        Returns:
            bool: True if Resource Explorer is available for comprehensive discovery
        """
        return getattr(self, "_comprehensive_discovery_available", False)

    def __str__(self):
        service_count = len(self._retrievers)
        has_fallback = self._fallback_retriever is not None
        comprehensive = (
            "comprehensive"
            if getattr(self, "_comprehensive_discovery_available", False)
            else "standard"
        )
        return f"TagRetrieverRegistry(services={service_count}, fallback={has_fallback}, mode={comprehensive}, region={self.region_name})"

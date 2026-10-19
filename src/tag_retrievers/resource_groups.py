from typing import Dict, List

import boto3

from .base import BaseTagRetriever


class ResourceGroupsTagRetriever(BaseTagRetriever):
    """
    Tag retriever using AWS Resource Groups Tagging API.
    This serves as a fallback for services not specifically implemented.
    """

    def _create_client(self):
        """Create Resource Groups Tagging API client"""
        return boto3.client("resourcegroupstaggingapi", region_name=self.region_name)

    def get_service_name(self) -> str:
        """Return the AWS service name"""
        return "resourcegroupstaggingapi"

    def can_handle_arn(self, resource_arn: str) -> bool:
        """
        This retriever can handle any valid AWS ARN as a fallback.
        Should be used when no specific retriever is available.
        """
        return resource_arn.startswith("arn:aws:")

    def get_tags(self, resource_arn: str) -> Dict[str, str]:
        """
        Retrieve tags for a resource using Resource Groups Tagging API.

        Args:
            resource_arn (str): Resource ARN

        Returns:
            Dict[str, str]: Dictionary of tag key-value pairs
        """
        response = self.client.get_resources(ResourceARNList=[resource_arn])

        resources = response.get("ResourceTagMappingList", [])
        if resources:
            # Convert list of tag dictionaries to key-value pairs
            tags = {}
            for tag in resources[0].get("Tags", []):
                tags[tag["Key"]] = tag["Value"]
            return tags

        return {}

    def get_batch_tags(self, resource_arns: List[str]) -> Dict[str, Dict[str, str]]:
        """
        Retrieve tags for multiple resources in a batch operation.
        Resource Groups Tagging API supports batch operations efficiently.

        Args:
            resource_arns (List[str]): List of resource ARNs

        Returns:
            Dict[str, Dict[str, str]]: Mapping of ARN to tag dictionary
        """
        if not resource_arns:
            return {}

        # Resource Groups Tagging API has a limit of 20 resources per request
        batch_size = 20
        result = {}

        for i in range(0, len(resource_arns), batch_size):
            batch = resource_arns[i : i + batch_size]

            try:
                response = self.client.get_resources(ResourceARNList=batch)

                for resource in response.get("ResourceTagMappingList", []):
                    arn = resource.get("ResourceARN")
                    tags = {}
                    for tag in resource.get("Tags", []):
                        tags[tag["Key"]] = tag["Value"]
                    result[arn] = tags

            except Exception as e:
                # If batch fails, fall back to individual requests
                for arn in batch:
                    result[arn] = self.get_tags_safe(arn)

        return result

    def find_all_tagged_resources(self, tag_filters: List[Dict] = None) -> List[Dict]:
        """
        Find all tagged resources, optionally filtered by specific tags.

        Args:
            tag_filters (List[Dict]): Optional tag filters

        Returns:
            List[Dict]: List of resources with their tags
        """
        paginator = self.client.get_paginator("get_resources")

        page_kwargs = {}
        if tag_filters:
            page_kwargs["TagFilters"] = tag_filters

        resources = []

        for page in paginator.paginate(**page_kwargs):
            for resource in page.get("ResourceTagMappingList", []):
                tags = {}
                for tag in resource.get("Tags", []):
                    tags[tag["Key"]] = tag["Value"]

                resources.append(
                    {"ResourceARN": resource.get("ResourceARN"), "Tags": tags}
                )

        return resources

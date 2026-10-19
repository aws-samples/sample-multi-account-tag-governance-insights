from typing import Dict, List, Optional, Union
from loguru import logger
import os
import botocore.exceptions
from datetime import datetime
from .tag_retrievers.multi_region_registry import MultiRegionTagRetrieverRegistry
from .tag_retrievers.resource_groups import ResourceGroupsTagRetriever
from .cost.allocation_manager import CostAllocationManager

# ConfigDataRetriever not available - using free APIs only
ConfigDataRetriever = None


class TagManager:
    """
    Main orchestrator for AWS resource tag retrieval.
    Provides a simple interface for getting tags from various AWS services.
    """
    
    def __init__(self, region_name: str = 'us-east-1', prefer_config: bool = None, enable_comprehensive_discovery: bool = True):
        """
        Initialize the TagManager.
        
        Args:
            region_name (str): AWS region name
            prefer_config (bool): Enable Config-first hybrid approach (auto-detected if None)
            enable_comprehensive_discovery (bool): Enable Resource Explorer for complete resource discovery
        """
        self.region_name = region_name
        self.registry = MultiRegionTagRetrieverRegistry(region_name)
        self.cost_allocation_manager = CostAllocationManager(region_name)
        
        # Comprehensive discovery setup (NEW)
        self.comprehensive_discovery_enabled = enable_comprehensive_discovery
        self._resource_explorer_retriever = None
        
        if enable_comprehensive_discovery:
            self._setup_comprehensive_discovery()
        
        # Config-first hybrid support
        self.prefer_config = prefer_config
        if self.prefer_config is None:
            # Auto-detect based on environment variable
            self.prefer_config = os.environ.get('TAGINSIGHTS_PREFER_CONFIG', 'auto').lower() in ['true', 'auto']
        
        # Initialize Config retriever if hybrid mode enabled
        self.config_retriever = None
        self.config_available = False
        if self.prefer_config:
            try:
                self.config_retriever = ConfigDataRetriever(region_name)
                self.config_available = self.config_retriever.is_config_enabled()
                if self.config_available:
                    logger.info(f"Config-first hybrid mode enabled for region: {region_name}")
                else:
                    logger.info(f"Config not available in {region_name}, using standard approach")
            except Exception as e:
                logger.warning(f"Error initializing Config retriever: {e}")
                self.config_available = False
            
        logger.info(f"Supported services: {', '.join(self.registry.get_supported_services())}")
    
    def _setup_comprehensive_discovery(self):
        """Setup comprehensive discovery with graceful fallback"""
        try:
            comprehensive_retriever = self.registry.get_comprehensive_retriever()
            if comprehensive_retriever:
                self._resource_explorer_retriever = comprehensive_retriever
                logger.info(f"Comprehensive discovery enabled via Resource Explorer in {self.region_name}")
            else:
                logger.info(f"Comprehensive discovery not available in {self.region_name} - using standard mode")
        except Exception as e:
            logger.warning(f"Could not enable comprehensive discovery: {e}")
    
    def get_tags_for_resource(self, resource_arn: str, use_fallback: bool = True) -> Dict[str, str]:
        """
        Get tags for a specific AWS resource.
        
        Args:
            resource_arn (str): The AWS resource ARN
            use_fallback (bool): Whether to use fallback retriever if no specific retriever found
            
        Returns:
            Dict[str, str]: Dictionary of tag key-value pairs
        """
        logger.info(f"Getting tags for resource: {resource_arn}")
        
        retriever = self.registry.get_retriever_for_arn(resource_arn)
        if not retriever:
            logger.warning(f"No retriever found for ARN: {resource_arn}")
            return {}
        
        # If we got the fallback retriever but user doesn't want fallback, return empty
        if not use_fallback and isinstance(retriever, ResourceGroupsTagRetriever):
            logger.info(f"Fallback retriever available but use_fallback=False for: {resource_arn}")
            return {}
        
        tags = retriever.get_tags_safe(resource_arn)
        logger.info(f"Retrieved {len(tags)} tags using {retriever.get_service_name()} retriever")
        
        return tags
    
    def get_tags_for_resources(self, resource_arns: List[str], use_fallback: bool = True) -> Dict[str, Dict[str, str]]:
        """
        Get tags for multiple AWS resources efficiently.
        
        Args:
            resource_arns (List[str]): List of AWS resource ARNs
            use_fallback (bool): Whether to use fallback retriever if no specific retriever found
            
        Returns:
            Dict[str, Dict[str, str]]: Mapping of ARN to tag dictionary
        """
        logger.info(f"Getting tags for {len(resource_arns)} resources")
        
        if not resource_arns:
            return {}
        
        # Group ARNs by retriever for efficient batch processing
        retriever_groups = {}
        no_retriever_arns = []
        
        for arn in resource_arns:
            retriever = self.registry.get_retriever_for_arn(arn)
            if retriever:
                # Skip fallback if user doesn't want it
                if not use_fallback and isinstance(retriever, ResourceGroupsTagRetriever):
                    no_retriever_arns.append(arn)
                    continue
                
                if retriever not in retriever_groups:
                    retriever_groups[retriever] = []
                retriever_groups[retriever].append(arn)
            else:
                no_retriever_arns.append(arn)
        
        # Log retriever distribution
        for retriever, arns in retriever_groups.items():
            logger.info(f"Using {retriever.get_service_name()} retriever for {len(arns)} resources")
        
        if no_retriever_arns:
            logger.warning(f"No retriever found for {len(no_retriever_arns)} resources")
        
        # Get tags from each retriever group
        all_tags = {}
        for retriever, arns in retriever_groups.items():
            try:
                tags = retriever.get_batch_tags(arns)
                all_tags.update(tags)
                logger.debug(f"Retrieved tags for {len(tags)} resources from {retriever.get_service_name()}")
            except Exception as e:
                logger.error(f"Error getting batch tags from {retriever.get_service_name()}: {e}")
                # Fall back to individual requests
                for arn in arns:
                    all_tags[arn] = retriever.get_tags_safe(arn)
        
        # Add empty results for resources with no retriever
        for arn in no_retriever_arns:
            all_tags[arn] = {}
        
        logger.info(f"Retrieved tags for {len(all_tags)} resources total")
        return all_tags
    
    def find_tagged_resources(self, tag_filters: List[Dict] = None, region_name: str = None) -> List[Dict]:
        """
        Find all tagged resources using Resource Groups Tagging API.
        
        Args:
            tag_filters (List[Dict]): Optional tag filters in format [{'Key': 'Environment', 'Values': ['prod']}]
            region_name (str): Optional region override
            
        Returns:
            List[Dict]: List of resources with their tags
        """
        search_region = region_name or self.region_name
        logger.info(f"Searching for tagged resources in region: {search_region}")
        
        # Use Resource Groups retriever for searching
        rg_retriever = ResourceGroupsTagRetriever(search_region)
        
        try:
            resources = rg_retriever.find_all_tagged_resources(tag_filters)
            logger.info(f"Found {len(resources)} tagged resources")
            return resources
        except Exception as e:
            logger.error(f"Error searching for tagged resources: {e}")
            return []
    
    def get_resource_summary(self, resource_arns: List[str] = None) -> Dict:
        """
        Get a summary of resources and their tag coverage.
        
        Args:
            resource_arns (List[str]): Optional list of specific ARNs to analyze
            
        Returns:
            Dict: Summary statistics
        """
        if resource_arns:
            # Analyze specific resources
            all_tags = self.get_tags_for_resources(resource_arns)
            resources = [{'ResourceARN': arn, 'Tags': tags} for arn, tags in all_tags.items()]
        else:
            # Find all tagged resources
            resources = self.find_tagged_resources()
        
        # Calculate summary statistics
        total_resources = len(resources)
        tagged_resources = len([r for r in resources if r['Tags']])
        untagged_resources = total_resources - tagged_resources
        
        # Count unique tags
        all_tag_keys = set()
        for resource in resources:
            all_tag_keys.update(resource['Tags'].keys())
        
        # Group by service
        service_counts = {}
        for resource in resources:
            arn = resource['ResourceARN']
            if ':' in arn:
                service = arn.split(':')[2]
                service_counts[service] = service_counts.get(service, 0) + 1
        
        summary = {
            'total_resources': total_resources,
            'tagged_resources': tagged_resources,
            'untagged_resources': untagged_resources,
            'tag_coverage_percent': (tagged_resources / total_resources * 100) if total_resources > 0 else 0,
            'unique_tag_keys': len(all_tag_keys),
            'tag_keys': sorted(all_tag_keys),
            'service_distribution': service_counts,
            'region': self.region_name
        }
        
        logger.info(f"Resource summary: {tagged_resources}/{total_resources} resources tagged ({summary['tag_coverage_percent']:.1f}%)")
        return summary
    
    def add_custom_retriever(self, retriever, region: str = None):
        """
        Add a custom tag retriever to a specific region.
        
        Args:
            retriever (BaseTagRetriever): Custom retriever instance
            region (str): Region to register the retriever in (defaults to retriever's region)
        """
        self.registry.register_retriever(retriever, region)
        logger.info(f"Added custom retriever: {retriever}")
    
    def get_supported_services(self) -> List[str]:
        """Get list of supported AWS services"""
        return self.registry.get_supported_services()
    
    # Smart Scanning Optimization Methods
    
    # Config-First Hybrid Methods
    
    def get_tags_for_resources_hybrid(self, 
                                    resource_arns: List[str],
                                    use_fallback: bool = True) -> Dict[str, Dict[str, str]]:
        """
        Get tags using Config-first hybrid approach.
        Provides massive cost savings when Config is available.
        
        Args:
            resource_arns (List[str]): List of AWS resource ARNs
            use_fallback (bool): Whether to use fallback retriever for missing resources
            
        Returns:
            Dict[str, Dict[str, str]]: Mapping of ARN to tag dictionary
        """
        if not resource_arns:
            return {}
        
        logger.info(f"Using hybrid approach for {len(resource_arns)} resources")
        
        all_tags = {}
        config_resources_count = 0
        api_resources_count = 0
        
        # Step 1: Try Config first if available
        if self.config_available and self.config_retriever:
            logger.info(f"Attempting Config retrieval for {len(resource_arns)} resources")
            
            try:
                config_results = self.config_retriever.get_batch_tags(resource_arns)
                all_tags.update(config_results)
                config_resources_count = len(config_results)
                
                config_coverage = (config_resources_count / len(resource_arns)) * 100
                logger.info(f"Config coverage: {config_coverage:.1f}% ({config_resources_count}/{len(resource_arns)} resources)")
                
                # Calculate cost savings from Config usage
                config_cost = len(resource_arns) * 0.000001  # Approximate Config query cost
                api_cost_saved = config_resources_count * 0.0001  # Approximate API call cost saved
                logger.info(f"Config cost savings: ${api_cost_saved:.4f} (Config: ${config_cost:.6f} vs API: ${config_cost + api_cost_saved:.4f})")
                
            except Exception as e:
                logger.error(f"Error with Config retrieval: {e}")
                config_results = {}
        else:
            config_results = {}
            logger.debug("Config not available, skipping Config retrieval")
        
        # Step 2: API fallback for resources not found in Config
        missing_arns = [arn for arn in resource_arns if arn not in config_results]
        
        if missing_arns:
            logger.info(f"Using API fallback for {len(missing_arns)} resources not found in Config")
            
            try:
                api_results = self.get_tags_for_resources(missing_arns, use_fallback)
                all_tags.update(api_results)
                api_resources_count = len(api_results)
                
            except Exception as e:
                logger.error(f"Error with API fallback: {e}")
                # Add empty results for failed resources
                for arn in missing_arns:
                    if arn not in all_tags:
                        all_tags[arn] = {}
        
        # Log final hybrid metrics
        total_retrieved = len(all_tags)
        hybrid_efficiency = (config_resources_count / len(resource_arns)) * 100 if resource_arns else 0
        
        logger.info(f"Hybrid results: {total_retrieved} total, Config: {config_resources_count}, API: {api_resources_count} "
                   f"(Config efficiency: {hybrid_efficiency:.1f}%)")
        
        return all_tags
    
    def validate_hybrid_setup(self) -> Dict[str, bool]:
        """
        Validate that Config-first hybrid components are properly configured.
        
        Returns:
            Dict[str, bool]: Validation results for hybrid components
        """
        results = {
            'hybrid_enabled': self.prefer_config,
            'config_available': False,
            'config_permissions_valid': False,
            'config_recording': False,
            'hybrid_ready': False
        }
        
        if not self.prefer_config:
            results['message'] = 'Hybrid mode not enabled'
            return results
        
        if self.config_retriever:
            try:
                # Test Config availability
                results['config_available'] = self.config_retriever.is_config_enabled()
                
                if results['config_available']:
                    # Test Config permissions by attempting a simple query
                    try:
                        test_response = self.config_retriever.config_client.select_resource_config(
                            Expression="SELECT COUNT(*) FROM resources LIMIT 1"
                        )
                        results['config_permissions_valid'] = True
                        results['config_recording'] = True
                    except botocore.exceptions.ClientError as e:
                        error_code = e.response.get('Error', {}).get('Code', '')
                        results['config_permissions_valid'] = error_code != 'AccessDenied'
                        results['config_recording'] = False
                        
                # Overall hybrid readiness
                results['hybrid_ready'] = (
                    results['config_available'] and 
                    results['config_permissions_valid'] and
                    results['config_recording']
                )
                
            except Exception as e:
                logger.error(f"Error validating hybrid setup: {e}")
                results['error'] = str(e)
        else:
            results['message'] = 'Config retriever not initialized'
        
        return results
    
    def compare_config_vs_api_performance(self, sample_arns: List[str]) -> Dict:
        """
        Compare performance and cost between Config and API approaches.
        
        Args:
            sample_arns (List[str]): Sample resource ARNs for comparison
            
        Returns:
            Dict: Performance comparison metrics
        """
        if not sample_arns:
            return {'error': 'No sample ARNs provided'}
        
        logger.info(f"Comparing Config vs API performance for {len(sample_arns)} sample resources")
        
        comparison = {
            'sample_size': len(sample_arns),
            'config_approach': {},
            'api_approach': {},
            'cost_analysis': {}
        }
        
        # Test Config approach
        if self.config_available and self.config_retriever:
            try:
                start_time = datetime.now()
                config_results = self.config_retriever.get_batch_tags(sample_arns)
                config_time = (datetime.now() - start_time).total_seconds()
                
                comparison['config_approach'] = {
                    'success': True,
                    'resources_found': len(config_results),
                    'execution_time': config_time,
                    'resources_per_second': len(config_results) / config_time if config_time > 0 else 0,
                    'coverage_percent': (len(config_results) / len(sample_arns)) * 100
                }
                
            except Exception as e:
                comparison['config_approach'] = {
                    'success': False,
                    'error': str(e)
                }
        else:
            comparison['config_approach'] = {
                'success': False,
                'reason': 'Config not available'
            }
        
        # Test API approach
        try:
            start_time = datetime.now()
            api_results = self.get_tags_for_resources(sample_arns)
            api_time = (datetime.now() - start_time).total_seconds()
            
            comparison['api_approach'] = {
                'success': True,
                'resources_found': len(api_results),
                'execution_time': api_time,
                'resources_per_second': len(api_results) / api_time if api_time > 0 else 0,
                'coverage_percent': (len(api_results) / len(sample_arns)) * 100
            }
            
        except Exception as e:
            comparison['api_approach'] = {
                'success': False,
                'error': str(e)
            }
        
        # Cost analysis
        if comparison['config_approach'].get('success') and comparison['api_approach'].get('success'):
            config_coverage = comparison['config_approach']['resources_found']
            api_calls_needed = len(sample_arns) - config_coverage
            
            # Estimated costs (approximate)
            config_cost = 0.000001 * len(sample_arns)  # Config query cost
            api_cost_full = 0.0001 * len(sample_arns)  # Full API approach
            hybrid_cost = config_cost + (0.0001 * api_calls_needed)  # Hybrid approach
            
            comparison['cost_analysis'] = {
                'config_only_cost': config_cost,
                'api_only_cost': api_cost_full,
                'hybrid_cost': hybrid_cost,
                'savings_vs_api': api_cost_full - hybrid_cost,
                'savings_percent': ((api_cost_full - hybrid_cost) / api_cost_full) * 100 if api_cost_full > 0 else 0,
                'config_coverage_resources': config_coverage,
                'api_fallback_resources': api_calls_needed
            }
        
        return comparison
    
    # Cost Allocation Tag Methods
    
    def get_tags_with_cost_allocation_info(self, resource_arn: str, use_fallback: bool = True) -> Dict:
        """
        Get tags for a resource with cost allocation metadata.
        
        Args:
            resource_arn (str): The AWS resource ARN
            use_fallback (bool): Whether to use fallback retriever if no specific retriever found
            
        Returns:
            Dict: Enhanced tag information with cost allocation metadata
        """
        tags = self.get_tags_for_resource(resource_arn, use_fallback)
        enhanced_tags = self.cost_allocation_manager.identify_cost_allocation_tags(tags)
        
        return {
            'ResourceARN': resource_arn,
            'Tags': enhanced_tags,
            'Summary': {
                'total_tags': len(tags),
                'cost_allocation_tags': len([t for t in enhanced_tags.values() if t.get('is_cost_allocation_tag')]),
                'potential_cost_tags': len([t for t in enhanced_tags.values() if t.get('potential_cost_tag')])
            }
        }
    
    def get_active_cost_allocation_tags(self, force_refresh: bool = False) -> List[str]:
        """
        Get list of active cost allocation tags.
        
        Args:
            force_refresh (bool): Force refresh from AWS API
            
        Returns:
            List[str]: List of active cost allocation tag keys
        """
        active_tags = self.cost_allocation_manager.get_active_cost_allocation_tags(force_refresh)
        return sorted(list(active_tags))
    
    def analyze_cost_allocation_coverage(self, resource_arns: List[str] = None) -> Dict:
        """
        Analyze cost allocation tag coverage across resources.
        
        Args:
            resource_arns (List[str]): Optional list of specific ARNs to analyze
            
        Returns:
            Dict: Cost allocation coverage analysis results
        """
        if resource_arns:
            # Analyze specific resources
            all_tags = self.get_tags_for_resources(resource_arns)
            resources = [{'ResourceARN': arn, 'Tags': tags} for arn, tags in all_tags.items()]
        else:
            # Find all tagged resources
            resources = self.find_tagged_resources()
        
        return self.cost_allocation_manager.analyze_cost_allocation_coverage(resources)
    
    def suggest_cost_allocation_improvements(self, resource_arns: List[str] = None) -> Dict:
        """
        Suggest improvements for cost allocation tagging.
        
        Args:
            resource_arns (List[str]): Optional list of specific ARNs to analyze
            
        Returns:
            Dict: Suggestions for improving cost allocation tagging
        """
        if resource_arns:
            # Analyze specific resources
            all_tags = self.get_tags_for_resources(resource_arns)
            resources = [{'ResourceARN': arn, 'Tags': tags} for arn, tags in all_tags.items()]
        else:
            # Find all tagged resources
            resources = self.find_tagged_resources()
        
        return self.cost_allocation_manager.suggest_cost_allocation_improvements(resources)
    
    def get_cost_allocation_summary(self, resource_arns: List[str] = None) -> Dict:
        """
        Get comprehensive cost allocation tag summary.
        
        Args:
            resource_arns (List[str]): Optional list of specific ARNs to analyze
            
        Returns:
            Dict: Comprehensive cost allocation summary
        """
        if resource_arns:
            # Analyze specific resources
            all_tags = self.get_tags_for_resources(resource_arns)
            resources = [{'ResourceARN': arn, 'Tags': tags} for arn, tags in all_tags.items()]
        else:
            # Find all tagged resources
            resources = self.find_tagged_resources()
        
        return self.cost_allocation_manager.get_cost_allocation_summary(resources)
    
    def clear_cost_allocation_cache(self):
        """Clear the cost allocation tags cache"""
        self.cost_allocation_manager.clear_cache()
    
    # Comprehensive Discovery Methods (NEW - non-breaking additions)
    
    def find_all_resources_comprehensive(self, 
                                       include_untagged: bool = True,
                                       max_resources: Optional[int] = None) -> List[Dict]:
        """
        Find ALL resources including untagged ones using multi-service approach
        
        This method provides Tag Editor-level resource discovery by finding both
        tagged and untagged resources, solving the coverage gap issue.
        
        Args:
            include_untagged (bool): Whether to include untagged resources
            max_resources (Optional[int]): Limit results for testing
            
        Returns:
            List[Dict]: Complete resource inventory in standard format
            
        Example:
            >>> manager = TagManager(enable_comprehensive_discovery=True)
            >>> all_resources = manager.find_all_resources_comprehensive()
            >>> print(f"Found {len(all_resources)} total resources")
            >>> untagged = [r for r in all_resources if not r['Tags']]
            >>> print(f"Untagged resources: {len(untagged)}")
        """
        if not include_untagged:
            # Use existing method for backward compatibility
            logger.info("Comprehensive discovery disabled - using standard tagged resource discovery")
            return self.find_tagged_resources()
        
        if not self._resource_explorer_retriever:
            logger.warning("Comprehensive discovery not available - Resource Explorer not configured")
            logger.info("Falling back to standard tagged resource discovery")
            return self.find_tagged_resources()
        
        logger.info("Starting comprehensive resource discovery (tagged + untagged)")
        
        try:
            # Get complete inventory from Resource Explorer
            all_resources = self._resource_explorer_retriever.find_all_resources_comprehensive(
                max_resources=max_resources
            )
            
            # Enrich with detailed tag data from Resource Groups API for tagged resources
            enriched_resources = self._enrich_with_detailed_tags(all_resources)
            
            tagged_count = len([r for r in enriched_resources if r.get('Tags')])
            untagged_count = len(enriched_resources) - tagged_count
            
            logger.info(f"Comprehensive discovery completed:")
            logger.info(f"  - Total resources: {len(enriched_resources)}")
            logger.info(f"  - Tagged resources: {tagged_count}")  
            logger.info(f"  - Untagged resources: {untagged_count}")
            logger.info(f"  - Tag coverage: {(tagged_count / len(enriched_resources) * 100):.1f}%")
            
            return enriched_resources
            
        except Exception as e:
            logger.error(f"Comprehensive discovery failed: {e}")
            # Graceful fallback to existing method
            logger.info("Falling back to standard tagged resource discovery")
            return self.find_tagged_resources()
    
    def get_untagged_resources(self, max_results: Optional[int] = None) -> List[Dict]:
        """
        Get resources that have no tags (invisible to standard Resource Groups API)
        
        This method identifies resources that need tagging for compliance/governance.
        
        Args:
            max_results (Optional[int]): Limit results for testing
            
        Returns:
            List[Dict]: Resources with empty Tags dictionary
            
        Example:
            >>> manager = TagManager(enable_comprehensive_discovery=True)
            >>> untagged = manager.get_untagged_resources()
            >>> print(f"Found {len(untagged)} resources needing tags")
            >>> for resource in untagged:
            ...     print(f"Untagged: {resource['ResourceARN']}")
        """
        if not self._resource_explorer_retriever:
            logger.error("Cannot identify untagged resources - Resource Explorer not available")
            return []
        
        try:
            # Use Resource Explorer's efficient untagged resource query
            untagged_resources = self._resource_explorer_retriever.find_untagged_resources_only(
                max_results=max_results or 5000
            )
            
            logger.info(f"Found {len(untagged_resources)} completely untagged resources")
            
            # Group by service for analysis
            service_distribution = {}
            for resource in untagged_resources:
                service = resource.get('Service', 'unknown')
                service_distribution[service] = service_distribution.get(service, 0) + 1
            
            logger.info("Untagged resources by service:")
            for service, count in sorted(service_distribution.items()):
                logger.info(f"  {service}: {count}")
            
            return untagged_resources
            
        except Exception as e:
            logger.error(f"Untagged resource discovery failed: {e}")
            return []
    
    def get_comprehensive_resource_summary(self) -> Dict:
        """
        Enhanced resource summary including untagged resource analysis
        
        Provides complete visibility into resource inventory and tagging coverage,
        similar to what AWS Tag Editor provides.
        
        Returns:
            Dict: Comprehensive statistics including tag coverage gaps
            
        Example:
            >>> manager = TagManager(enable_comprehensive_discovery=True)
            >>> summary = manager.get_comprehensive_resource_summary()
            >>> print(f"Total resources: {summary['total_resources']}")
            >>> print(f"Tag coverage: {summary['tag_coverage_percent']:.1f}%")
            >>> print(f"Services with untagged resources: {len(summary['services_needing_attention'])}")
        """
        if not self._resource_explorer_retriever:
            logger.warning("Comprehensive summary not available - using standard summary")
            return self.get_resource_summary()
        
        try:
            # Get complete resource inventory
            all_resources = self.find_all_resources_comprehensive()
            
            if not all_resources:
                return {
                    'total_resources': 0,
                    'tagged_resources': 0,
                    'untagged_resources': 0,
                    'tag_coverage_percent': 0,
                    'error': 'No resources discovered'
                }
            
            tagged_resources = [r for r in all_resources if r.get('Tags')]
            untagged_resources = [r for r in all_resources if not r.get('Tags')]
            
            # Service distribution analysis
            service_stats = {}
            services_needing_attention = []
            
            for resource in all_resources:
                arn = resource['ResourceARN']
                service = resource.get('Service') or (arn.split(':')[2] if ':' in arn else 'unknown')
                
                if service not in service_stats:
                    service_stats[service] = {'total': 0, 'tagged': 0, 'untagged': 0}
                
                service_stats[service]['total'] += 1
                if resource.get('Tags'):
                    service_stats[service]['tagged'] += 1
                else:
                    service_stats[service]['untagged'] += 1
            
            # Identify services needing attention (high untagged counts)
            for service, stats in service_stats.items():
                untagged_percent = (stats['untagged'] / stats['total'] * 100) if stats['total'] > 0 else 0
                if untagged_percent > 50 and stats['untagged'] > 5:  # More than 50% untagged and at least 5 resources
                    services_needing_attention.append({
                        'service': service,
                        'untagged_count': stats['untagged'],
                        'total_count': stats['total'],
                        'untagged_percent': untagged_percent
                    })
            
            # Sort by untagged count for priority
            services_needing_attention.sort(key=lambda x: x['untagged_count'], reverse=True)
            
            # Count unique tags across all resources
            all_tag_keys = set()
            for resource in tagged_resources:
                all_tag_keys.update(resource.get('Tags', {}).keys())
            
            summary = {
                'total_resources': len(all_resources),
                'tagged_resources': len(tagged_resources),
                'untagged_resources': len(untagged_resources),
                'tag_coverage_percent': (len(tagged_resources) / len(all_resources) * 100) if all_resources else 0,
                'untagged_coverage_percent': (len(untagged_resources) / len(all_resources) * 100) if all_resources else 0,
                'unique_tag_keys': len(all_tag_keys),
                'tag_keys': sorted(all_tag_keys),
                'service_statistics': service_stats,
                'services_needing_attention': services_needing_attention,
                'discovery_method': 'comprehensive',
                'region': self.region_name
            }
            
            logger.info(f"Comprehensive resource summary: {len(all_resources)} total resources, "
                       f"{summary['tag_coverage_percent']:.1f}% tagged")
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating comprehensive summary: {e}")
            # Fallback to standard summary
            return self.get_resource_summary()
    
    def _enrich_with_detailed_tags(self, resources: List[Dict]) -> List[Dict]:
        """
        Enrich Resource Explorer data with detailed tag information from service APIs
        
        OPTIMIZATION: Tag enrichment is now DISABLED when using batch discovery mode.
        Resource Explorer already returns complete tag data with batch mode enabled,
        so re-fetching tags would waste API calls and cause throttling.
        
        Args:
            resources (List[Dict]): Resources from Resource Explorer
            
        Returns:
            List[Dict]: Resources with tag data (enrichment skipped in batch mode)
        """
        if not resources:
            return resources
        
        # Check if batch discovery is enabled (feature flag)
        use_batch_discovery = os.environ.get('USE_BATCH_DISCOVERY', 'false').lower() == 'true'
        
        if use_batch_discovery:
            # OPTIMIZATION: Skip enrichment in batch mode - Resource Explorer tags are already complete
            logger.info(f"Batch discovery mode: Using Resource Explorer tags directly for {len(resources)} resources (no enrichment needed)")
            return resources
        
        # Legacy enrichment for backward compatibility when batch mode disabled
        logger.info(f"Legacy mode: Enriching {len(resources)} resources with detailed tag data")
        
        # Group resources by whether they have tags from Resource Explorer
        tagged_arns = []
        enriched_resources = []
        
        for resource in resources:
            arn = resource['ResourceARN']
            explorer_tags = resource.get('Tags', {})
            
            if explorer_tags:
                # Resource has tags from Explorer, but get more detailed tags from service APIs
                tagged_arns.append(arn)
            
            enriched_resources.append(resource)
        
        if tagged_arns:
            try:
                # Get detailed tags for tagged resources using existing optimized batch methods
                detailed_tags = self.get_tags_for_resources(tagged_arns, use_fallback=True)
                
                # Merge detailed tags back into resource list
                for resource in enriched_resources:
                    arn = resource['ResourceARN']
                    if arn in detailed_tags:
                        # Use more detailed tags from service APIs
                        service_tags = detailed_tags[arn]
                        if service_tags:  # Only update if service API returned tags
                            resource['Tags'] = service_tags
                
                logger.info(f"Enriched {len(detailed_tags)} resources with detailed tag data")
                
            except Exception as e:
                logger.warning(f"Tag enrichment failed: {e} - using Resource Explorer tag data")
        
        return enriched_resources
    
    def is_comprehensive_discovery_available(self) -> bool:
        """
        Check if comprehensive discovery (including untagged resources) is available
        
        Returns:
            bool: True if Resource Explorer is configured and ready
        """
        return (self.comprehensive_discovery_enabled and 
                self._resource_explorer_retriever is not None and
                self._resource_explorer_retriever.validate_setup())
    
    def get_discovery_capabilities(self) -> Dict:
        """
        Get information about available discovery capabilities
        
        Returns:
            Dict: Discovery capabilities and configuration status
        """
        capabilities = {
            'standard_discovery': True,  # Always available via Resource Groups
            'comprehensive_discovery': self.is_comprehensive_discovery_available(),
            'optimization_enabled': False,
            'config_hybrid_available': self.config_available if hasattr(self, 'config_available') else False,
            'region': self.region_name,
            'supported_services': self.get_supported_services()
        }
        
        # Add Resource Explorer statistics if available
        if self._resource_explorer_retriever:
            try:
                explorer_stats = self._resource_explorer_retriever.get_discovery_statistics()
                capabilities['resource_explorer_stats'] = explorer_stats
            except Exception as e:
                logger.debug(f"Could not get Resource Explorer stats: {e}")
        
        return capabilities
    
    def __str__(self):
        services_count = len(self.get_supported_services())
        mode = "comprehensive" if self.is_comprehensive_discovery_available() else "standard"
        return f"TagManager(region={self.region_name}, services={services_count}, mode={mode})"

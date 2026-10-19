from typing import Dict, List, Optional, Set, Union
import boto3

# Graceful fallback for Lambda environments without loguru
try:
    from loguru import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info("Using standard logging (loguru not available)")

from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import os


class CostAllocationManager:
    """
    Manager for cost allocation tag operations.
    Provides functionality to identify cost allocation tags and analyze their usage.
    """
    
    def __init__(self, region_name: str = 'us-east-1', use_smart_provider: bool = True):
        """
        Initialize the CostAllocationManager.
        
        Args:
            region_name (str): AWS region name
            use_smart_provider (bool): Enable smart cost provider with CUR integration
        """
        self.region_name = region_name
        self._ce_client = None
        self._pricing_client = None
        self._cost_allocation_tags_cache = None
        self._cache_timestamp = None
        self._cache_ttl = 3600  # 1 hour cache
        
        # Smart provider removed - using Cost Explorer only
        self._smart_provider = None
        self._use_smart_provider = False
        logger.info("Using Cost Explorer only")
    
    @property
    def ce_client(self):
        """Lazy initialization of Cost Explorer client"""
        if self._ce_client is None:
            self._ce_client = boto3.client('ce', region_name='us-east-1')  # CE is only in us-east-1
        return self._ce_client
    
    @property
    def pricing_client(self):
        """Lazy initialization of Pricing client"""
        if self._pricing_client is None:
            self._pricing_client = boto3.client('pricing', region_name='us-east-1')  # Pricing is only in us-east-1
        return self._pricing_client
    
    def _is_cache_valid(self) -> bool:
        """Check if cached cost allocation tags are still valid"""
        if self._cost_allocation_tags_cache is None or self._cache_timestamp is None:
            return False
        
        age = (datetime.now() - self._cache_timestamp).total_seconds()
        return age < self._cache_ttl
    
    def get_active_cost_allocation_tags(self, force_refresh: bool = False) -> Set[str]:
        """
        Get the list of active cost allocation tags.
        
        Args:
            force_refresh (bool): Force refresh from AWS API
            
        Returns:
            Set[str]: Set of active cost allocation tag keys
        """
        if not force_refresh and self._is_cache_valid():
            logger.debug("Using cached cost allocation tags")
            return self._cost_allocation_tags_cache.copy()
        
        logger.info("Fetching active cost allocation tags from AWS")
        cost_allocation_tags = set()
        
        try:
            # Get active cost allocation tags using the proper Cost Explorer API
            response = self.ce_client.list_cost_allocation_tags(
                Status='Active',
                MaxResults=100
            )
            
            for tag_info in response.get('CostAllocationTags', []):
                tag_key = tag_info.get('TagKey', '')
                if tag_key:
                    cost_allocation_tags.add(tag_key)
                    logger.debug(f"Found active cost allocation tag: {tag_key}")
            
            # Handle pagination if more than 100 tags
            next_token = response.get('NextToken')
            while next_token and len(cost_allocation_tags) < 1000:  # Safety limit
                try:
                    response = self.ce_client.list_cost_allocation_tags(
                        Status='Active',
                        MaxResults=100,
                        NextToken=next_token
                    )
                    
                    for tag_info in response.get('CostAllocationTags', []):
                        tag_key = tag_info.get('TagKey', '')
                        if tag_key:
                            cost_allocation_tags.add(tag_key)
                    
                    next_token = response.get('NextToken')
                except Exception as e:
                    logger.error(f"Error fetching additional cost allocation tags: {e}")
                    break
            
            if cost_allocation_tags:
                logger.info(f"Retrieved {len(cost_allocation_tags)} active cost allocation tags from AWS API")
            else:
                logger.info("No active cost allocation tags found in AWS account")
                
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'DataUnavailableException':
                logger.warning("Cost Explorer data not available. Cost allocation tags may not be enabled.")
            elif error_code == 'UnauthorizedOperation':
                logger.warning("Insufficient permissions to access Cost Explorer data")
            elif error_code == 'InvalidParameterValueException':
                logger.warning("Cost allocation tags feature may not be available in this account")
            else:
                logger.error(f"Error fetching cost allocation tags: {e}")
        except Exception as e:
            logger.error(f"Unexpected error fetching cost allocation tags: {e}")
        
        # If no cost allocation tags found, return empty set (don't use fallback patterns)
        if not cost_allocation_tags:
            logger.warning("No active cost allocation tags found in AWS account. Cost allocation tags may need to be activated in the AWS Billing console.")
        
        # Cache the results
        self._cost_allocation_tags_cache = cost_allocation_tags
        self._cache_timestamp = datetime.now()
        
        return cost_allocation_tags.copy()
    
    def identify_cost_allocation_tags(self, resource_tags: Dict[str, str]) -> Dict[str, Dict[str, Union[str, bool]]]:
        """
        Identify which tags are cost allocation tags and enhance them with metadata.
        
        Args:
            resource_tags (Dict[str, str]): Resource tags to analyze
            
        Returns:
            Dict[str, Dict[str, Union[str, bool]]]: Enhanced tags with cost allocation info
        """
        active_cost_tags = self.get_active_cost_allocation_tags()
        enhanced_tags = {}
        
        for key, value in resource_tags.items():
            enhanced_tags[key] = {
                'value': value,
                'is_cost_allocation_tag': key in active_cost_tags
            }
        
        return enhanced_tags
    
    def analyze_cost_allocation_coverage(self, resources_with_tags: List[Dict]) -> Dict:
        """
        Analyze cost allocation tag coverage across resources.
        
        Args:
            resources_with_tags (List[Dict]): List of resources with their tags
                Format: [{'ResourceARN': 'arn...', 'Tags': {'key': 'value'}}]
                
        Returns:
            Dict: Analysis results including coverage statistics
        """
        active_cost_tags = self.get_active_cost_allocation_tags()
        
        total_resources = len(resources_with_tags)
        resources_with_cost_tags = 0
        cost_tag_usage = {}
        missing_cost_tags = []
        
        for resource in resources_with_tags:
            arn = resource.get('ResourceARN', '')
            tags = resource.get('Tags', {})
            
            # Check if resource has any cost allocation tags
            has_cost_tags = any(tag_key in active_cost_tags for tag_key in tags.keys())
            
            if has_cost_tags:
                resources_with_cost_tags += 1
            else:
                missing_cost_tags.append(arn)
            
            # Count usage of each cost allocation tag
            for tag_key in tags.keys():
                if tag_key in active_cost_tags:
                    cost_tag_usage[tag_key] = cost_tag_usage.get(tag_key, 0) + 1
        
        # Calculate coverage percentage
        coverage_percent = (resources_with_cost_tags / total_resources * 100) if total_resources > 0 else 0
        
        # Identify most and least used cost allocation tags
        sorted_usage = sorted(cost_tag_usage.items(), key=lambda x: x[1], reverse=True)
        most_used_tags = sorted_usage[:5] if sorted_usage else []
        least_used_tags = sorted_usage[-5:] if len(sorted_usage) > 5 else []
        
        return {
            'total_resources': total_resources,
            'resources_with_cost_tags': resources_with_cost_tags,
            'resources_without_cost_tags': total_resources - resources_with_cost_tags,
            'cost_allocation_coverage_percent': coverage_percent,
            'active_cost_allocation_tags': list(active_cost_tags),
            'cost_tag_usage': cost_tag_usage,
            'most_used_cost_tags': most_used_tags,
            'least_used_cost_tags': least_used_tags,
            'resources_missing_cost_tags': missing_cost_tags[:100],  # Limit to first 100
            'total_missing_count': len(missing_cost_tags)
        }
    
    def suggest_cost_allocation_improvements(self, resources_with_tags: List[Dict]) -> Dict:
        """
        Suggest improvements for cost allocation tagging.
        
        Args:
            resources_with_tags (List[Dict]): List of resources with their tags
                
        Returns:
            Dict: Suggestions for improving cost allocation tagging
            
        Note: This provides basic recommendations. For more intelligent suggestions,
              consider using LLM-based analysis of your specific environment.
        """
        analysis = self.analyze_cost_allocation_coverage(resources_with_tags)
        
        suggestions = {
            'recommendations': []
        }
        
        # Generate basic recommendations based on coverage analysis
        if analysis['cost_allocation_coverage_percent'] < 50:
            suggestions['recommendations'].append(
                "Low cost allocation coverage. Consider implementing mandatory cost allocation tags."
            )
        
        if analysis['resources_without_cost_tags'] > 0:
            suggestions['recommendations'].append(
                f"{analysis['resources_without_cost_tags']} resources lack cost allocation tags. "
                "Consider implementing automated tagging policies."
            )
        
        if len(analysis['active_cost_allocation_tags']) < 3:
            suggestions['recommendations'].append(
                "Consider using multiple cost allocation tags for better cost tracking granularity."
            )
        
        if analysis['cost_allocation_coverage_percent'] > 80:
            suggestions['recommendations'].append(
                "Excellent cost allocation tag coverage! Consider implementing cost optimization analysis."
            )
        
        return suggestions
    
    def get_cost_allocation_summary(self, resources_with_tags: List[Dict]) -> Dict:
        """
        Get a comprehensive summary of cost allocation tag usage.
        
        Args:
            resources_with_tags (List[Dict]): List of resources with their tags
                
        Returns:
            Dict: Comprehensive cost allocation summary
        """
        analysis = self.analyze_cost_allocation_coverage(resources_with_tags)
        suggestions = self.suggest_cost_allocation_improvements(resources_with_tags)
        
        return {
            'summary': {
                'total_resources': analysis['total_resources'],
                'cost_allocation_coverage': f"{analysis['cost_allocation_coverage_percent']:.1f}%",
                'active_cost_tags_count': len(analysis['active_cost_allocation_tags']),
                'resources_needing_tags': analysis['resources_without_cost_tags']
            },
            'coverage_analysis': analysis,
            'improvement_suggestions': suggestions,
            'timestamp': datetime.now().isoformat()
        }
    
    def clear_cache(self):
        """Clear the cost allocation tags cache"""
        self._cost_allocation_tags_cache = None
        self._cache_timestamp = None
        logger.info("Cost allocation tags cache cleared")
    
    # Note: Analytics table integration methods removed - now handled by AnalyticsTableReader class

    # Cost Analysis Methods with Smart Provider Integration
    
    def get_cost_by_tag_values(self, tag_key: str, months_back: int = 1, 
                              granularity: str = 'MONTHLY') -> Dict[str, float]:
        """
        Get costs broken down by values of a specific tag.
        Enhanced with smart provider support for CUR integration.
        
        Args:
            tag_key (str): The tag key to analyze (e.g., 'Environment', 'Project')
            months_back (int): Number of months to look back (default: 1)
            granularity (str): Cost granularity - 'DAILY', 'MONTHLY' (default: 'MONTHLY')
            
        Returns:
            Dict[str, float]: Mapping of tag values to their costs
            
        Example:
            >>> costs = manager.get_cost_by_tag_values("Environment")
            >>> print(costs)
            {"production": 12450.67, "development": 3291.45, "staging": 1876.23}
        """
        # Try smart provider first (CUR + caching)
        if self._use_smart_provider and self._smart_provider:
            try:
                smart_result = self._smart_provider.get_costs_by_tag(tag_key, months_back, granularity=granularity)
                if smart_result and smart_result.get('breakdown'):
                    # Convert formatted costs back to floats for backward compatibility
                    costs_dict = {}
                    for tag_value, cost_str in smart_result['breakdown'].items():
                        try:
                            # Remove currency formatting
                            cost_float = float(cost_str.replace('$', '').replace(',', ''))
                            costs_dict[tag_value] = cost_float
                        except (ValueError, AttributeError):
                            costs_dict[tag_value] = 0.0
                    
                    logger.info(f"Smart provider returned {len(costs_dict)} tag values for '{tag_key}' "
                               f"(source: {smart_result.get('data_source', 'unknown')})")
                    return costs_dict
                    
            except Exception as e:
                logger.warning(f"Smart provider failed for get_cost_by_tag_values: {e}, falling back to Cost Explorer")
        
        # Fallback to original Cost Explorer implementation
        return self._get_cost_by_tag_values_ce(tag_key, months_back, granularity)
    
    def _get_cost_by_tag_values_ce(self, tag_key: str, months_back: int = 1, 
                                  granularity: str = 'MONTHLY') -> Dict[str, float]:
        """
        Original Cost Explorer implementation for get_cost_by_tag_values
        """
        logger.info(f"Getting costs by tag values for '{tag_key}' via Cost Explorer (last {months_back} months)")
        
        try:
            # Calculate time period
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=months_back * 30)
            
            time_period = {
                'Start': start_date.strftime('%Y-%m-%d'),
                'End': end_date.strftime('%Y-%m-%d')
            }
            
            # Query Cost Explorer for costs grouped by tag
            response = self.ce_client.get_cost_and_usage(
                TimePeriod=time_period,
                Granularity=granularity,
                Metrics=['BlendedCost'],
                GroupBy=[
                    {
                        'Type': 'TAG',
                        'Key': tag_key
                    }
                ]
            )
            
            # Process results
            tag_costs = {}
            total_groups_processed = 0
            
            for result in response['ResultsByTime']:
                logger.debug(f"Processing time period: {result.get('TimePeriod', {})}")
                groups = result.get('Groups', [])
                logger.debug(f"Found {len(groups)} groups in this time period")
                
                for group in groups:
                    total_groups_processed += 1
                    # Extract tag value from the group key
                    tag_value = group['Keys'][0] if group['Keys'][0] else 'No Value'
                    # Get the cost amount
                    amount = float(group['Metrics']['BlendedCost']['Amount'])
                    
                    logger.debug(f"Processing: {tag_key}={tag_value}: ${amount:.2f}")
                    
                    if tag_value in tag_costs:
                        tag_costs[tag_value] += amount
                    else:
                        tag_costs[tag_value] = amount
            
            logger.debug(f"Processed {total_groups_processed} total groups, found {len(tag_costs)} unique tag values")
            
            # Sort by cost descending but keep all entries (including zero cost)
            tag_costs = dict(sorted(tag_costs.items(), key=lambda x: x[1], reverse=True))
            
            logger.info(f"Found costs for {len(tag_costs)} values of tag '{tag_key}'")
            return tag_costs
            
        except ClientError as e:
            logger.error(f"Error getting costs by tag values: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error getting costs by tag values: {e}")
            return {}
    
    def get_cost_trends_by_tag(self, tag_key: str, tag_value: str, 
                              months_back: int = 6) -> Dict[str, float]:
        """
        Get monthly cost trends for a specific tag value.
        
        Args:
            tag_key (str): The tag key (e.g., 'Environment')
            tag_value (str): The specific tag value (e.g., 'production')
            months_back (int): Number of months to look back (default: 6)
            
        Returns:
            Dict[str, float]: Mapping of month (YYYY-MM) to cost amount
            
        Example:
            >>> trends = manager.get_cost_trends_by_tag("Environment", "production")
            >>> print(trends)
            {"2024-01": 11200.45, "2024-02": 12100.23, "2024-03": 12800.67}
        """
        logger.info(f"Getting cost trends for {tag_key}='{tag_value}' (last {months_back} months)")
        
        try:
            # Calculate time period
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=months_back * 30)
            
            time_period = {
                'Start': start_date.strftime('%Y-%m-%d'),
                'End': end_date.strftime('%Y-%m-%d')
            }
            
            # Query Cost Explorer for monthly costs filtered by tag
            response = self.ce_client.get_cost_and_usage(
                TimePeriod=time_period,
                Granularity='MONTHLY',
                Metrics=['BlendedCost'],
                Filter={
                    'Tags': {
                        'Key': tag_key,
                        'Values': [tag_value],
                        'MatchOptions': ['EQUALS']
                    }
                }
            )
            
            # Process results
            monthly_costs = {}
            for result in response.get('ResultsByTime', []):
                period_start = result.get('TimePeriod', {}).get('Start', '')
                if period_start:
                    # Convert to YYYY-MM format
                    month_key = period_start[:7]  # Extract YYYY-MM from YYYY-MM-DD
                    
                    # Get the cost amount
                    amount = float(result.get('Total', {}).get('BlendedCost', {}).get('Amount', 0))
                    monthly_costs[month_key] = amount
            
            # Sort by month
            monthly_costs = dict(sorted(monthly_costs.items()))
            
            logger.info(f"Found cost trends for {len(monthly_costs)} months for {tag_key}='{tag_value}'")
            return monthly_costs
            
        except ClientError as e:
            logger.error(f"Error getting cost trends: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error getting cost trends: {e}")
            return {}
    
    def get_untagged_resources_cost(self, months_back: int = 1) -> Dict[str, Union[float, str]]:
        """
        Not implemented - resource cost analyzer removed.

        Args:
            months_back (int): Number of months to look back (default: 1)

        Returns:
            Dict: Empty result indicating feature is not available
        """
        logger.info("get_untagged_resources_cost: resource cost analyzer removed, returning empty result")
        return {
            'total_untagged_cost': 0,
            'total_cost': 0,
            'percentage_of_total': 0,
            'resources_with_costs': 0,
            'dormant_resources': 0,
            'data_source': 'not_available',
            'estimation_note': 'Resource cost analyzer removed'
        }
    
    def format_cost(self, amount: float) -> str:
        """
        Format cost amount as currency string.
        
        Args:
            amount (float): Cost amount
            
        Returns:
            str: Formatted currency string
            
        Example:
            >>> formatted = manager.format_cost(1234.56)
            >>> print(formatted)
            "$1,234.56"
        """
        return f"${amount:,.2f}"
    
    def identify_cost_optimization_opportunities(self, tag_key: str, threshold: float = 1000.0, 
                                               months_back: int = 1) -> Dict:
        """
        Identify cost optimization opportunities for a specific tag.
        
        Args:
            tag_key (str): Tag key to analyze for optimization
            threshold (float): Cost threshold above which to flag for attention (default: $1000)
            months_back (int): Number of months to analyze (default: 1)
            
        Returns:
            Dict: Cost optimization opportunities and recommendations
            
        Example:
            >>> opportunities = manager.identify_cost_optimization_opportunities('Environment', 1000)
            >>> print(opportunities['high_cost_values'])
            ["production: $12,450.67", "staging: $3,200.00"]
        """
        logger.info(f"Identifying cost optimization opportunities for '{tag_key}' (threshold: ${threshold})")
        
        try:
            # Get cost breakdown by tag values
            tag_costs = self.get_cost_by_tag_values(tag_key, months_back)
            
            if not tag_costs:
                return {
                    'tag_key': tag_key,
                    'opportunities': [],
                    'high_cost_values': [],
                    'recommendations': ['No cost data available for this tag'],
                    'threshold': threshold
                }
            
            # Identify high-cost tag values
            high_cost_values = []
            optimization_opportunities = []
            total_high_cost = 0
            
            for tag_value, cost in tag_costs.items():
                if cost >= threshold:
                    high_cost_values.append({
                        'tag_value': tag_value,
                        'cost': cost,
                        'formatted_cost': self.format_cost(cost)
                    })
                    total_high_cost += cost
                    
                    # Generate specific recommendations based on cost level
                    if cost > threshold * 5:  # Very high cost
                        optimization_opportunities.append(f"URGENT: {tag_value} costs ${cost:,.2f} - Review resource usage and consider rightsizing")
                    elif cost > threshold * 2:  # High cost  
                        optimization_opportunities.append(f"HIGH: {tag_value} costs ${cost:,.2f} - Audit resource allocation and utilization")
                    else:  # Above threshold
                        optimization_opportunities.append(f"MEDIUM: {tag_value} costs ${cost:,.2f} - Monitor for cost efficiency")
            
            # Calculate statistics
            total_cost = sum(tag_costs.values())
            high_cost_percentage = (total_high_cost / total_cost * 100) if total_cost > 0 else 0
            
            # Generate general recommendations
            recommendations = []
            if len(high_cost_values) > 0:
                recommendations.append(f"Found {len(high_cost_values)} tag values above ${threshold} threshold")
                recommendations.append(f"High-cost values represent {high_cost_percentage:.1f}% of total costs")
            
            if high_cost_percentage > 50:
                recommendations.append("Consider implementing more granular tagging for better cost attribution")
            
            if len(tag_costs) > 10:
                recommendations.append("Large number of tag values may indicate need for tag standardization")
            
            result = {
                'tag_key': tag_key,
                'threshold': threshold,
                'high_cost_values': high_cost_values,
                'optimization_opportunities': optimization_opportunities,
                'recommendations': recommendations,
                'statistics': {
                    'total_cost': total_cost,
                    'high_cost_total': total_high_cost, 
                    'high_cost_percentage': high_cost_percentage,
                    'num_high_cost_values': len(high_cost_values),
                    'total_tag_values': len(tag_costs)
                },
                'time_period': f'Last {months_back} month{"s" if months_back > 1 else ""}',
                'analysis_timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Found {len(high_cost_values)} high-cost values for '{tag_key}' above ${threshold} threshold")
            return result
            
        except Exception as e:
            logger.error(f"Error identifying optimization opportunities: {e}")
            return {
                'tag_key': tag_key,
                'error': str(e),
                'opportunities': [],
                'high_cost_values': [],
                'recommendations': ['Error occurred during analysis'],
                'threshold': threshold
            }


# Standalone functions for cost analysis (moved from src.main to break circular imports)

def get_active_cost_allocation_tags(region: str = 'us-east-1', force_refresh: bool = False) -> List[str]:
    """
    Get list of active cost allocation tags for the AWS account.
    
    Args:
        region (str): AWS region to query from
        force_refresh (bool): Force refresh from AWS API instead of using cache
        
    Returns:
        List[str]: List of active cost allocation tag keys
        
    Example:
        >>> tags = get_active_cost_allocation_tags()
        >>> print(f"Active cost allocation tags: {', '.join(tags)}")
    """
    try:
        manager = CostAllocationManager(region_name=region)
        return list(manager.get_active_cost_allocation_tags(force_refresh))
        
    except Exception as e:
        logger.error(f"Error getting active cost allocation tags: {e}")
        return []


def analyze_cost_allocation_coverage(region: str = 'us-east-1', resource_arns: List[str] = None) -> Dict:
    """
    Analyze cost allocation tag coverage across AWS resources.
    
    Args:
        region (str): AWS region to analyze
        resource_arns (List[str]): Optional list of specific ARNs to analyze
        
    Returns:
        Dict: Analysis results including coverage statistics
        
    Example:
        >>> analysis = analyze_cost_allocation_coverage()
        >>> print(f"Cost allocation coverage: {analysis['cost_allocation_coverage_percent']:.1f}%")
        >>> print(f"Resources without cost tags: {analysis['resources_without_cost_tags']}")
    """
    try:
        # Import TagManager locally to avoid circular imports
        from ..tag_manager import TagManager
        
        manager = TagManager(region_name=region)
        return manager.analyze_cost_allocation_coverage(resource_arns)
        
    except Exception as e:
        logger.error(f"Error analyzing cost allocation coverage: {e}")
        return {}

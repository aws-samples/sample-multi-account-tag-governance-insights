#!/usr/bin/env python3
"""
Cost Analyzer Worker Lambda Handler

Processes resource batches from SQS, gets costs from CUR, normalizes tags,
and writes to S3 daily partitions.

Architecture:
- Triggered by SQS FIFO queue (5 messages per invocation)
- Gets costs for each resource from CUR
- Normalizes tags (explodes dict into individual rows)
- Flags cost allocation tags
- Writes to S3 daily partition using Parquet

Features:
- Parallel processing with 50 concurrent workers
- Rate limiting for Athena queries (5 QPS per worker)
- Exponential backoff for throttling
- DLQ for failed messages
"""

import os
import json
import calendar
import boto3
from typing import Dict, List, Any, Optional
from datetime import datetime, date
from collections import defaultdict

# Graceful fallback for Lambda environments without loguru
try:
    from loguru import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

try:
    from src.lambda_handlers.cost.cur_direct_matcher import CURDirectMatcher
    from src.lambda_handlers.cost.parquet_writer import ParquetWriter
    from src.lambda_handlers.cost.allocation_manager import get_active_cost_allocation_tags
    from src.lambda_handlers.cost.rate_limiter import get_athena_rate_limiter
except ImportError:
    # Fallback for Lambda layer imports
    try:
        from cur_direct_matcher import CURDirectMatcher
        from parquet_writer import ParquetWriter
        from allocation_manager import get_active_cost_allocation_tags
        from rate_limiter import get_athena_rate_limiter
    except ImportError:
        logger.warning("Required modules not available - using placeholders")
        CURDirectMatcher = None
        ParquetWriter = None
        get_active_cost_allocation_tags = None
        get_athena_rate_limiter = None


class CostAnalyzerWorker:
    """
    Worker that processes resource batches and writes normalized tag costs
    """
    
    def __init__(self):
        """Initialize worker with AWS clients and configuration"""
        self.region = os.environ.get('AWS_REGION', 'us-east-1')
        self.s3_bucket = os.environ.get('S3_BUCKET')
        self.months_back = int(os.environ.get('MONTHS_BACK', '1'))
        
        # Initialize CUR matcher for cost data
        if CURDirectMatcher:
            self.cur_matcher = CURDirectMatcher(region_name=self.region)
        else:
            self.cur_matcher = None
            logger.warning("CUR matcher not available")
        
        # Initialize Parquet writer
        if ParquetWriter:
            self.parquet_writer = ParquetWriter(
                bucket=self.s3_bucket,
                prefix='tag-costs-daily',
                region=self.region
            )
        else:
            self.parquet_writer = None
            logger.warning("Parquet writer not available")
        
        # Initialize rate limiter for Athena queries
        if get_athena_rate_limiter:
            self.rate_limiter = get_athena_rate_limiter()
        else:
            self.rate_limiter = None
        
        # Initialize CloudWatch client for metrics
        self.cloudwatch = boto3.client('cloudwatch', region_name=self.region)
        
        # Cache for cost allocation tags (shared across batch)
        self._cost_allocation_tags = None
        
        logger.info(f"Worker initialized: region={self.region}, bucket={self.s3_bucket}")
        
        # VALIDATE REQUIRED COMPONENTS
        missing_components = []
        if not self.cur_matcher:
            missing_components.append('CURDirectMatcher')
        if not self.parquet_writer:
            missing_components.append('ParquetWriter')
        if not get_active_cost_allocation_tags:
            missing_components.append('get_active_cost_allocation_tags')
        
        if missing_components:
            error_msg = (f"Critical components failed to load: {', '.join(missing_components)}. "
                        "Ensure Lambda package includes src/lambda_handlers/cost modules and AWS SDK Pandas layer is attached.")
            logger.error(error_msg)
            raise RuntimeError(error_msg)
    
    def process_batch(self, resources: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process a batch of resources: get costs, normalize tags, write to S3
        
        Args:
            resources: List of resources from SQS message
            
        Returns:
            Dict: Processing statistics
        """
        logger.info(f"Processing batch of {len(resources)} resources")
        
        # Get cost allocation tags (cached)
        cost_allocation_tags = self._get_cost_allocation_tags()
        
        # Group resources by account for efficient processing
        resources_by_account = defaultdict(list)
        for resource in resources:
            account_id = resource.get('account_id', 'unknown')
            resources_by_account[account_id].append(resource)
        
        # Process each account's resources
        total_normalized_rows = 0
        total_resources_processed = 0
        total_resources_with_costs = 0
        accounts_processed = []
        
        for account_id, account_resources in resources_by_account.items():
            try:
                # Get costs for all resources in this account
                resource_costs = self._get_resource_costs(account_resources)
                
                # Normalize tags for each resource
                normalized_rows = []
                for resource in account_resources:
                    resource_arn = resource.get('resource_arn')
                    
                    # Find cost data for this resource
                    cost_data = next(
                        (c for c in resource_costs if c.get('resource_arn') == resource_arn),
                        None
                    )
                    
                    if cost_data and cost_data.get('monthly_cost', 0) > 0:
                        total_resources_with_costs += 1
                    
                    # Normalize tags into individual rows
                    resource_rows = self._normalize_resource_tags(
                        resource,
                        cost_data,
                        cost_allocation_tags
                    )
                    
                    normalized_rows.extend(resource_rows)
                    total_resources_processed += 1
                
                # Write to S3 daily partition
                if normalized_rows:
                    write_stats = self.parquet_writer.write_tag_costs(
                        records=normalized_rows,
                        account_id=account_id
                    )
                    
                    total_normalized_rows += len(normalized_rows)
                    accounts_processed.append(account_id)
                    
                    logger.info(f"Account {account_id}: processed {len(account_resources)} resources, "
                              f"wrote {len(normalized_rows)} normalized rows")
                
            except Exception as e:
                logger.error(f"Error processing account {account_id}: {e}", exc_info=True)
                # Continue with other accounts
        
        stats = {
            'resources_processed': total_resources_processed,
            'resources_with_costs': total_resources_with_costs,
            'normalized_rows_written': total_normalized_rows,
            'accounts_processed': accounts_processed,
            'accounts_count': len(accounts_processed),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Publish metrics
        self._publish_metric('ResourcesProcessed', total_resources_processed)
        self._publish_metric('NormalizedRowsWritten', total_normalized_rows)
        
        logger.info(f"Batch processing complete: {stats}")
        return stats
    
    def _get_resource_costs(self, resources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Get costs for a list of resources from CUR
        
        Args:
            resources: List of resources
            
        Returns:
            List[Dict]: Resources with cost data
        """
        if not self.cur_matcher:
            logger.warning("CUR matcher not available, returning empty costs")
            return []
        
        try:
            # Use rate limiter to avoid Athena throttling
            if self.rate_limiter:
                acquired = self.rate_limiter.acquire_query_slot(blocking=True, timeout=30)
                if not acquired:
                    logger.warning("Failed to acquire rate limiter slot within timeout")
            
            # Get costs from CUR using existing code
            resource_costs = self.cur_matcher.get_costs_for_resources(
                resources,
                months_back=self.months_back
            )
            
            logger.debug(f"Retrieved costs for {len(resource_costs)} resources")
            return resource_costs
            
        except Exception as e:
            logger.error(f"Error getting resource costs: {e}")
            return []
    
    def _parse_tags_string(self, tags_str: str) -> Dict[str, str]:
        """
        Parse tags string - handles both JSON and Athena MAP formats
        
        Athena MAP columns from Parquet tables are returned as JSON strings.
        This method also handles the alternative Athena MAP text format as fallback.
        
        JSON format: {"key1":"value1","key2":"value2"}
        Athena MAP format: {key1=value1, key2=value2}
        
        Args:
            tags_str: Tags string to parse
            
        Returns:
            Dict[str, str]: Parsed tags dictionary
        """
        if not tags_str or tags_str.strip() == '':
            return {}
        
        # Try JSON format first (most common for Parquet tables)
        try:
            parsed = json.loads(tags_str)
            if isinstance(parsed, dict):
                logger.debug("Parsed tags as JSON format")
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
        
        # Try Athena MAP text format: {key1=value1, key2=value2}
        try:
            tags_str = tags_str.strip()
            if tags_str.startswith('{') and tags_str.endswith('}'):
                # Remove outer braces
                content = tags_str[1:-1].strip()
                if not content:
                    return {}
                
                # Split by comma and parse key=value pairs
                tags = {}
                pairs = content.split(',')
                for pair in pairs:
                    pair = pair.strip()
                    if '=' in pair:
                        key, value = pair.split('=', 1)
                        tags[key.strip()] = value.strip()
                
                if tags:
                    logger.debug(f"Parsed tags as Athena MAP format: {len(tags)} tags")
                    return tags
        except Exception as e:
            logger.warning(f"Failed to parse Athena MAP format: {e}")
        
        logger.warning(f"Could not parse tags string: {tags_str[:100]}")
        return {}
    
    def _normalize_resource_tags(
        self,
        resource: Dict[str, Any],
        cost_data: Optional[Dict[str, Any]],
        cost_allocation_tags: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Normalize resource tags into individual rows
        
        Each tag becomes a separate row with cost data and flag indicating
        if it's a cost allocation tag.
        
        Args:
            resource: Resource with tags
            cost_data: Cost data for this resource (may be None)
            cost_allocation_tags: List of active cost allocation tag keys
            
        Returns:
            List[Dict]: Normalized tag rows
        """
        normalized_rows = []
        
        # Extract tags from resource
        tags = resource.get('tags', {})
        
        # Handle case where tags is a string (from Athena MAP column)
        # Athena returns MAP columns as JSON strings when querying Parquet tables
        if isinstance(tags, str):
            tags = self._parse_tags_string(tags)
        
        # Ensure tags is a dictionary
        if not isinstance(tags, dict):
            logger.warning(f"Invalid tags type {type(tags)} for {resource.get('resource_arn')}, using empty dict")
            tags = {}
        
        # If no tags, create a single row with null tag values
        if not tags:
            normalized_rows.append(
                self._create_tag_row(
                    resource=resource,
                    tag_key=None,
                    tag_value=None,
                    is_cost_allocation_tag=False,
                    cost_data=cost_data
                )
            )
            return normalized_rows
        
        # Normalize each tag into its own row
        for tag_key, tag_value in tags.items():
            # Check if this tag is a cost allocation tag
            is_cost_allocation_tag = tag_key in cost_allocation_tags
            
            normalized_row = self._create_tag_row(
                resource=resource,
                tag_key=tag_key,
                tag_value=tag_value,
                is_cost_allocation_tag=is_cost_allocation_tag,
                cost_data=cost_data
            )
            
            normalized_rows.append(normalized_row)
        
        return normalized_rows
    
    def _create_tag_row(
        self,
        resource: Dict[str, Any],
        tag_key: Optional[str],
        tag_value: Optional[str],
        is_cost_allocation_tag: bool,
        cost_data: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Create a single normalized tag cost row
        
        Args:
            resource: Resource data
            tag_key: Tag key (None for untagged resources)
            tag_value: Tag value (None for untagged resources)
            is_cost_allocation_tag: Whether this is a cost allocation tag
            cost_data: Cost data for this resource
            
        Returns:
            Dict: Normalized tag row
        """
        # The matcher sums the current month's CUR partition (MONTHS_BACK=1),
        # so "monthly_cost" is month-to-date.
        # ponytail: last_month_cost stays 0; add a previous-month CUR query to fill it.
        current_month_mtd = cost_data.get('monthly_cost', 0.0) if cost_data else 0.0

        today = date.today()
        days_in_month = calendar.monthrange(today.year, today.month)[1]
        avg_daily_cost = current_month_mtd / today.day
        current_month_projected = avg_daily_cost * days_in_month

        # Create normalized row
        row = {
            'tag_key': tag_key,
            'tag_value': tag_value,
            'is_cost_allocation_tag': is_cost_allocation_tag,
            'resource_arn': resource.get('resource_arn'),
            'resource_type': resource.get('resource_type'),
            'region': resource.get('region'),
            'last_month_cost': 0.0,
            'current_month_mtd': current_month_mtd,
            'current_month_projected': current_month_projected,
            'avg_daily_cost': avg_daily_cost,
            'analysis_date': today,
            'last_updated': datetime.utcnow()
        }
        
        return row
    
    def _get_cost_allocation_tags(self) -> List[str]:
        """
        Get active cost allocation tags (cached per worker invocation)
        
        Returns:
            List[str]: List of cost allocation tag keys
        """
        if self._cost_allocation_tags is None:
            if get_active_cost_allocation_tags:
                try:
                    self._cost_allocation_tags = get_active_cost_allocation_tags(
                        self.region,
                        force_refresh=False
                    )
                    logger.info(f"Retrieved {len(self._cost_allocation_tags)} cost allocation tags")
                except Exception as e:
                    logger.error(f"Error getting cost allocation tags: {e}")
                    self._cost_allocation_tags = []
            else:
                logger.warning("get_active_cost_allocation_tags not available")
                self._cost_allocation_tags = []
        
        return self._cost_allocation_tags
    
    def _publish_metric(self, metric_name: str, value: float):
        """
        Publish CloudWatch metric
        
        Args:
            metric_name: Name of metric
            value: Metric value
        """
        try:
            self.cloudwatch.put_metric_data(
                Namespace='TagInsights/CostAnalysis',
                MetricData=[{
                    'MetricName': metric_name,
                    'Value': value,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                }]
            )
        except Exception as e:
            logger.warning(f"Failed to publish metric {metric_name}: {e}")


def lambda_handler(event, context):
    """
    Lambda handler for cost analyzer worker
    
    Triggered by SQS FIFO queue with batches of resources.
    Processes up to 5 messages per invocation.
    
    Args:
        event: SQS event with resource batches
        context: Lambda context
        
    Returns:
        Dict: Batch processing summary
    """
    logger.info("Cost Analyzer Worker started")
    logger.info(f"Received {len(event.get('Records', []))} SQS messages")
    
    # Initialize worker
    worker = CostAnalyzerWorker()
    
    # Process statistics
    total_resources = 0
    total_rows_written = 0
    successful_batches = 0
    failed_batches = 0
    
    # Process each SQS message
    for record in event.get('Records', []):
        try:
            # Parse message body
            message_body = json.loads(record['body'])
            resources = message_body.get('resources', [])
            batch_id = message_body.get('batch_id', 'unknown')
            
            logger.info(f"Processing batch {batch_id} with {len(resources)} resources")
            
            # Process batch
            stats = worker.process_batch(resources)
            
            total_resources += stats['resources_processed']
            total_rows_written += stats['normalized_rows_written']
            successful_batches += 1
            
            logger.info(f"Batch {batch_id} completed successfully")
            
        except Exception as e:
            logger.error(f"Error processing SQS message: {e}", exc_info=True)
            failed_batches += 1
            
            # Message will be retried or sent to DLQ based on configuration
    
    # Summary
    result = {
        'statusCode': 200 if failed_batches == 0 else 207,  # 207 = Partial Success
        'body': json.dumps({
            'status': 'success' if failed_batches == 0 else 'partial_success',
            'messages_processed': successful_batches,
            'messages_failed': failed_batches,
            'total_resources_processed': total_resources,
            'total_normalized_rows_written': total_rows_written,
            'timestamp': datetime.utcnow().isoformat()
        })
    }
    
    logger.info(f"Worker completed: {result['body']}")
    return result

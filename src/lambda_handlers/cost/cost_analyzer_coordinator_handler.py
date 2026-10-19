#!/usr/bin/env python3
"""
Cost Analyzer Coordinator Lambda Handler

Queries Athena for resources changed in last 12 hours and batches them into SQS
for parallel cost analysis processing.

Architecture:
- Triggered by EventBridge every 12 hours
- Queries resource_tag_deltas_recent table for changed resources
- Batches resources (100 per message) and sends to SQS FIFO queue
- Workers process batches in parallel to normalize tags and get costs

Features:
- Incremental processing (only last 12 hours)
- Athena partition pruning for efficiency
- SQS FIFO for ordered, deduplicated processing
- Exponential backoff for Athena throttling
"""

import os
import json
import boto3
from typing import Dict, List, Any
from datetime import datetime
import hashlib

# Graceful fallback for Lambda environments without loguru
try:
    from loguru import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

try:
    from src.lambda_handlers.cost.athena_query_executor import AthenaQueryExecutor
except ImportError:
    # Fallback for Lambda layer imports
    try:
        from athena_query_executor import AthenaQueryExecutor
    except ImportError:
        logger.warning("AthenaQueryExecutor not available - using placeholder")
        AthenaQueryExecutor = None


class CostAnalyzerCoordinator:
    """
    Coordinates cost analysis by querying Athena and distributing work via SQS
    """
    
    def __init__(self):
        """Initialize coordinator with AWS clients and configuration"""
        self.region = os.environ.get('AWS_REGION', 'us-east-1')
        self.environment = os.environ.get('ENVIRONMENT', 'dev')
        
        # Get database name from environment or construct it using hub-stack naming convention
        self.database_name = os.environ.get('ATHENA_DATABASE')
        if not self.database_name:
            # Fallback: construct database name matching hub-stack.yaml format
            # Format: ti_{region}_{environment}_{account_id}
            try:
                sts_client = boto3.client('sts', region_name=self.region)
                account_id = sts_client.get_caller_identity()['Account']
                self.database_name = f"ti_{self.region}_{self.environment}_{account_id}"
                logger.warning(f"ATHENA_DATABASE env var not set, using constructed name: {self.database_name}")
            except Exception as e:
                logger.error(f"Could not determine account ID for database name: {e}")
                self.database_name = 'ti'  # Last resort fallback
        
        self.queue_url = os.environ.get('COST_ANALYSIS_QUEUE_URL')
        self.s3_output_bucket = os.environ.get('ATHENA_OUTPUT_BUCKET')
        
        # Get workgroup name from environment or construct it using hub-stack naming convention
        self.workgroup = os.environ.get('ATHENA_WORKGROUP')
        if not self.workgroup:
            # Fallback: construct workgroup name matching hub-stack.yaml format
            # Format: ti-analytics-{region}-{environment}-{account_id}
            try:
                sts_client = boto3.client('sts', region_name=self.region)
                account_id = sts_client.get_caller_identity()['Account']
                self.workgroup = f"ti-analytics-{self.region}-{self.environment}-{account_id}"
                logger.warning(f"ATHENA_WORKGROUP env var not set, using constructed name: {self.workgroup}")
            except Exception as e:
                logger.error(f"Could not determine account ID for workgroup name: {e}")
                self.workgroup = 'primary'  # Last resort fallback
        
        self.batch_size = int(os.environ.get('BATCH_SIZE', '100'))
        
        # Initialize AWS clients
        self.sqs = boto3.client('sqs', region_name=self.region)
        self.cloudwatch = boto3.client('cloudwatch', region_name=self.region)
        
        # Initialize Athena query executor
        if AthenaQueryExecutor:
            self.athena = AthenaQueryExecutor(
                database=self.database_name,
                output_bucket=self.s3_output_bucket,
                region=self.region,
                workgroup=self.workgroup
            )
        else:
            self.athena = None
            logger.warning("Athena query executor not available")
        
        logger.info(f"Coordinator initialized:")
        logger.info(f"  Region: {self.region}")
        logger.info(f"  Environment: {self.environment}")
        logger.info(f"  Database: {self.database_name}")
        logger.info(f"  Workgroup: {self.workgroup}")
        logger.info(f"  Queue: {self.queue_url}")
        logger.info(f"  Batch size: {self.batch_size}")
    
    def get_changed_resources(self, test_mode: bool = None, lookback_hours: int = 12) -> List[Dict[str, Any]]:
        """
        Query Athena for resources changed in last 12 hours (or all resources in test mode)
        
        Args:
            test_mode: If True, query all resources without time filter.
                      If None, check TEST_MODE environment variable (defaults to false).
        
        Returns:
            List[Dict]: Changed resources with ARN, type, region, tags, etc.
        """
        # Check environment variable if test_mode not explicitly set
        if test_mode is None:
            test_mode = os.environ.get('TEST_MODE', 'false').lower() == 'true'
        
        # Log test mode status with appropriate warnings
        if test_mode:
            logger.warning("⚠️ TEST MODE ENABLED - Processing ALL resources (no time filter)")
            logger.warning("⚠️ This may generate a large number of SQS messages and API calls")
            logger.warning("⚠️ Ensure TEST_MODE environment variable is set to 'false' for production use")
        else:
            logger.info(f"Normal mode - Processing resources changed in last {lookback_hours} hours")
        
        logger.info(f"Using database: {self.database_name}")
        
        # Quote database and table names if database contains hyphens
        # Athena requires double quotes (not backticks) for identifiers with special characters
        if '-' in self.database_name:
            db_name = f'"{self.database_name}"'
            table_name = '"resource_tag_deltas_recent"'
        else:
            db_name = self.database_name
            table_name = 'resource_tag_deltas_recent'
        
        # Build WHERE clause conditionally based on test mode
        # Test mode: No time filter (processes all resources)
        # Normal mode: Only resources changed in last 12 hours
        where_clause = "" if test_mode else \
            f"WHERE timestamp >= cast(to_unixtime(date_add('hour', -{int(lookback_hours)}, now())) as bigint)"
        
        # Query only resource_tag_deltas_recent (last 90 days)
        # No need to query historical table since we only need last 12 hours (or all in test mode)
        # timestamp is BIGINT (unix seconds), use cast for proper type matching
        query = f"""
        SELECT DISTINCT 
            arn AS resource_arn,
            resource_type,
            region,
            account_id,
            tags,
            timestamp,
            change_type
        FROM {db_name}.{table_name}
        {where_clause}
        ORDER BY timestamp DESC
        """
        
        try:
            # Execute query with exponential backoff for throttling
            results = self.athena.execute_query(query)
            
            logger.info(f"Retrieved {len(results)} changed resources from Athena")
            
            # Publish metric
            self._publish_metric('ChangedResourcesFound', len(results))
            
            return results
            
        except Exception as e:
            logger.error(f"Error querying Athena for changed resources: {e}")
            self._publish_metric('AthenaQueryErrors', 1)
            raise
    
    def batch_resources(self, resources: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """
        Batch resources into groups for SQS messages
        
        Args:
            resources: List of resources to batch
            
        Returns:
            List[List[Dict]]: Batched resources
        """
        batches = []
        for i in range(0, len(resources), self.batch_size):
            batch = resources[i:i + self.batch_size]
            batches.append(batch)
        
        logger.info(f"Created {len(batches)} batches of {self.batch_size} resources each")
        return batches
    
    def send_to_sqs(self, batches: List[List[Dict[str, Any]]]) -> Dict[str, int]:
        """
        Send resource batches to SQS FIFO queue for processing
        
        Args:
            batches: List of resource batches
            
        Returns:
            Dict: Statistics about messages sent
        """
        logger.info(f"Sending {len(batches)} batches to SQS queue")
        
        sent_count = 0
        failed_count = 0
        
        for batch_idx, batch in enumerate(batches):
            try:
                # Create SQS message
                message_body = json.dumps({
                    'batch_id': batch_idx,
                    'batch_size': len(batch),
                    'resources': batch,
                    'timestamp': datetime.utcnow().isoformat(),
                    'coordinator_run_id': self._get_run_id()
                })
                
                # Generate deduplication ID based on batch content
                deduplication_id = self._generate_dedup_id(batch)
                
                # Generate message group ID (use account_id for ordering within account)
                # This ensures resources from same account are processed in order
                account_id = batch[0].get('account_id', 'default') if batch else 'default'
                message_group_id = f"account-{account_id}"
                
                # Send to SQS FIFO queue
                response = self.sqs.send_message(
                    QueueUrl=self.queue_url,
                    MessageBody=message_body,
                    MessageDeduplicationId=deduplication_id,
                    MessageGroupId=message_group_id,
                    MessageAttributes={
                        'batch_id': {'StringValue': str(batch_idx), 'DataType': 'Number'},
                        'batch_size': {'StringValue': str(len(batch)), 'DataType': 'Number'},
                        'account_id': {'StringValue': account_id, 'DataType': 'String'}
                    }
                )
                
                sent_count += 1
                
                if batch_idx % 10 == 0:
                    logger.debug(f"Sent batch {batch_idx}/{len(batches)} to SQS")
                
            except Exception as e:
                logger.error(f"Error sending batch {batch_idx} to SQS: {e}")
                failed_count += 1
        
        stats = {
            'sent': sent_count,
            'failed': failed_count,
            'total_resources': sum(len(b) for b in batches)
        }
        
        logger.info(f"SQS send complete: {sent_count} sent, {failed_count} failed")
        
        # Publish metrics
        self._publish_metric('BatchesSent', sent_count)
        self._publish_metric('BatchesFailed', failed_count)
        self._publish_metric('ResourcesQueued', stats['total_resources'])
        
        return stats
    
    def _generate_dedup_id(self, batch: List[Dict[str, Any]]) -> str:
        """
        Generate content-based deduplication ID for SQS FIFO
        
        Args:
            batch: Resource batch
            
        Returns:
            str: Deduplication ID (max 128 chars)
        """
        # Create hash of resource ARNs in batch
        resource_arns = [r.get('resource_arn', '') for r in batch]
        content = '|'.join(sorted(resource_arns))
        hash_digest = hashlib.sha256(content.encode()).hexdigest()
        
        # Include timestamp hour to allow reprocessing after dedup window
        timestamp_hour = datetime.utcnow().strftime('%Y%m%d%H')
        
        return f"{timestamp_hour}-{hash_digest[:64]}"
    
    def _get_run_id(self) -> str:
        """Generate unique run ID for this coordinator execution"""
        return datetime.utcnow().strftime('%Y%m%d-%H%M%S')
    
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
    Lambda handler for cost analyzer coordinator
    
    Triggered by EventBridge every 12 hours to:
    1. Query Athena for resources changed in last 12 hours (or all resources in test mode)
    2. Batch resources (100 per message)
    3. Send batches to SQS for worker processing
    
    Test Mode:
    - Set TEST_MODE environment variable to 'true' to process all resources
    - Or pass test_mode=true in the event payload
    - Default is false (only last 12 hours)
    
    Args:
        event: EventBridge event (scheduled or manual trigger)
               Can include 'test_mode': true to override environment variable
        context: Lambda context
        
    Returns:
        Dict: Execution summary
    """
    logger.info("Cost Analyzer Coordinator started")
    logger.info(f"Event: {json.dumps(event)}")
    
    try:
        # Initialize coordinator
        coordinator = CostAnalyzerCoordinator()
        
        # Check for test mode in event (overrides environment variable)
        test_mode = event.get('test_mode', None)
        if test_mode is not None:
            logger.info(f"Test mode override from event: {test_mode}")
        
        # Step 1: Query Athena for changed resources
        logger.info("Step 1: Querying Athena for changed resources")
        changed_resources = coordinator.get_changed_resources(
            test_mode=test_mode,
            lookback_hours=int(event.get('lookback_hours', 12)),
        )
        
        if not changed_resources:
            logger.info("No changed resources found - nothing to process")
            # Determine actual test mode used
            actual_test_mode = test_mode if test_mode is not None else \
                              os.environ.get('TEST_MODE', 'false').lower() == 'true'
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'success',
                    'message': 'No changed resources found',
                    'resources_found': 0,
                    'batches_sent': 0,
                    'test_mode': actual_test_mode,
                    'timestamp': datetime.utcnow().isoformat()
                })
            }
        
        # Step 2: Batch resources
        logger.info("Step 2: Batching resources")
        batches = coordinator.batch_resources(changed_resources)
        
        # Step 3: Send to SQS
        logger.info("Step 3: Sending batches to SQS")
        stats = coordinator.send_to_sqs(batches)
        
        # Determine actual test mode used
        actual_test_mode = test_mode if test_mode is not None else \
                          os.environ.get('TEST_MODE', 'false').lower() == 'true'
        
        # Publish test mode metric if enabled
        if actual_test_mode:
            coordinator._publish_metric('TestModeInvocations', 1)
        
        # Success response
        result = {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'success',
                'resources_found': len(changed_resources),
                'batches_created': len(batches),
                'batches_sent': stats['sent'],
                'batches_failed': stats['failed'],
                'total_resources_queued': stats['total_resources'],
                'test_mode': actual_test_mode,
                'run_id': coordinator._get_run_id(),
                'timestamp': datetime.utcnow().isoformat()
            })
        }
        
        logger.info(f"Coordinator completed successfully: {result['body']}")
        return result
        
    except Exception as e:
        logger.error(f"Coordinator execution failed: {e}", exc_info=True)
        
        # Publish error metric
        try:
            cloudwatch = boto3.client('cloudwatch')
            cloudwatch.put_metric_data(
                Namespace='TagInsights/CostAnalysis',
                MetricData=[{
                    'MetricName': 'CoordinatorErrors',
                    'Value': 1,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                }]
            )
        except:
            pass
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            })
        }

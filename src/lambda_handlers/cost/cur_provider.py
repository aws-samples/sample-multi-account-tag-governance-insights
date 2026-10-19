#!/usr/bin/env python3
"""
CUR Cost Provider - Athena-based cost data provider using CUR data

Executes optimized Athena queries against CUR tables for cost analysis:
- Schema-aware query execution
- Automatic result formatting and processing
- Error handling and graceful degradation
- Performance monitoring and logging

Features:
- 99%+ cost savings vs Cost Explorer APIs
- Daily granularity cost data
- Resource-level cost attribution  
- Handles dynamic tag column schemas
"""

import os
import boto3
from typing import Dict, List, Optional, Set, Any
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

# Graceful fallback for Lambda environments without loguru
try:
    from loguru import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info("Using standard logging (loguru not available)")
import time
import json

try:
    # Try relative imports first (normal package structure)
    from .query_builder import CURQueryBuilder
    from .schema_manager import CURSchemaManager, _get_cur_session
except ImportError:
    try:
        # Try absolute imports from src.cost (Lambda environment)
        from src.lambda_handlers.cost.query_builder import CURQueryBuilder
        from src.lambda_handlers.cost.schema_manager import CURSchemaManager, _get_cur_session
    except ImportError:
        # Final fallback for direct imports (development)
        from query_builder import CURQueryBuilder
        from schema_manager import CURSchemaManager, _get_cur_session


class CURCostProvider:
    """
    Athena-based cost data provider using CUR tables
    
    Executes optimized queries with partition pruning for maximum cost efficiency
    while providing comprehensive cost analysis capabilities.
    """
    
    _tag_key_summary_cache = {}
    _tag_key_summary_cache_ttl_seconds = 3600

    def __init__(
        self,
        database: str,
        table: str,
        region_name: str = 'us-east-1',
        cur_session: Optional[boto3.Session] = None,
    ):
        """
        Initialize CUR cost provider.

        Supports cross-account CUR access via STS AssumeRole.  When the
        ``CUR_ACCOUNT_ROLE_ARN`` environment variable is set the provider
        assumes that role (in the billing/CUR account) and directs all
        Athena queries there.  Athena query results are written to the
        bucket named by ``CUR_ATHENA_RESULTS_BUCKET`` (also in the CUR
        account); if that variable is absent the hub account's
        ``ATHENA_RESULTS_BUCKET`` / ``ATHENA_OUTPUT_BUCKET`` is used
        instead (same-account mode, original behaviour).

        Args:
            database: CUR database name in Glue catalog
            table: CUR table name
            region_name: AWS region for Athena client
        """
        self.database = database
        self.table = table
        self.region = region_name
        self.environment = os.environ.get('ENVIRONMENT', 'dev')

        # Build a boto3 session — optionally via cross-account role assumption.
        # _get_cur_session() is defined in schema_manager and shared across all
        # CUR components so the same credentials are used everywhere.
        self.cur_session = cur_session or _get_cur_session()

        # Initialize AWS clients using the (possibly cross-account) session
        self.athena_client = self.cur_session.client(
            'athena',
            region_name=region_name,
        )

        self.schema_manager = CURSchemaManager(
            region_name,
            session=self.cur_session,
        )
        self.schema_profile = self._build_schema_profile(database, table)
        self.query_builder = CURQueryBuilder(
            database,
            table,
            schema_profile=self.schema_profile,
        )

        # Get Athena results configuration
        self.results_bucket = self._get_athena_results_bucket()

        # Query execution settings
        self.query_timeout = 300  # 5 minutes
        self.max_retries = 2

        logger.info(
            f"CURCostProvider initialized: {database}.{table} "
            f"(region: {region_name}, format: {self.schema_profile['format']})"
        )
        if os.environ.get('CUR_ACCOUNT_ROLE_ARN'):
            logger.info("Cross-account CUR role configured")

    def _build_schema_profile(self, database: str, table: str) -> Dict:
        """Fetch the configured Glue definition once and classify it."""
        table_info = self.schema_manager.glue_client.get_table(
            DatabaseName=database,
            Name=table,
        )['Table']
        profile = CURSchemaManager.build_schema_profile(table_info)
        if not profile['supported']:
            raise RuntimeError(
                f"Unsupported CUR schema for configured table "
                f"{database}.{table}"
            )
        self.available_tag_columns = (
            CURSchemaManager.extract_available_tag_columns(
                table_info,
                profile,
            )
        )
        return profile
    
    def _discover_workgroup(self) -> str:
        """Discover TagInsights workgroup or fallback to primary"""
        # Check environment variable first - explicit configuration takes precedence
        env_workgroup = os.environ.get('ATHENA_WORKGROUP')
        if env_workgroup:
            logger.debug(f"Using workgroup from environment variable: {env_workgroup}")
            return env_workgroup
        
        # Construct expected workgroup name from account ID and region
        try:
            # Get account ID for workgroup naming
            try:
                sts_client = boto3.client('sts', region_name=self.region)
                account_id = sts_client.get_caller_identity()['Account']
            except Exception:
                account_id = None
            
            # Expected workgroup name
            if account_id:
                expected_workgroup = f"ti-cost-analytics-{self.region}-{self.environment}-{account_id}"
            else:
                expected_workgroup = f"ti-cost-analytics-{self.region}-{self.environment}"
            
            # List workgroups to find TagInsights workgroup
            response = self.athena_client.list_work_groups()
            workgroups = [wg['Name'] for wg in response.get('WorkGroups', [])]
            
            # Look for TagInsights workgroup variants
            taginsights_workgroups = [wg for wg in workgroups if wg.startswith('ti-cost-analytics') and self.environment in wg]
            
            if taginsights_workgroups:
                actual_workgroup = taginsights_workgroups[0]
                if actual_workgroup != expected_workgroup:
                    logger.debug(f"Using discovered workgroup: {actual_workgroup} (expected: {expected_workgroup})")
                return actual_workgroup
            else:
                logger.debug(f"TagInsights workgroup not found, using 'primary'. Expected: {expected_workgroup}")
                return 'primary'
                
        except Exception as e:
            logger.debug(f"Workgroup discovery failed, using 'primary': {e}")
            return 'primary'
    
    def _get_athena_results_bucket(self) -> str:
        """
        Get or determine the S3 bucket for Athena query results.

        In cross-account mode the Athena instance runs inside the CUR/billing
        account, so query results must land in a bucket that account owns.
        Customers set ``CUR_ATHENA_RESULTS_BUCKET`` to point at that bucket.

        Priority order:
        1. ``CUR_ATHENA_RESULTS_BUCKET`` — dedicated bucket in the CUR account
           (required when CUR_ACCOUNT_ROLE_ARN is set and the CUR account's
           Athena workgroup enforces its own output location)
        2. ``ATHENA_RESULTS_BUCKET`` / ``ATHENA_OUTPUT_BUCKET`` — hub account
           bucket (same-account mode, original behaviour)
        3. Auto-derived default ``aws-athena-query-results-{account}-{region}``
        """
        # Cross-account: dedicated results bucket in the CUR account
        cur_results_bucket = os.environ.get('CUR_ATHENA_RESULTS_BUCKET', '').strip()
        if cur_results_bucket:
            logger.debug(f"Using CUR-account Athena results bucket: {cur_results_bucket}")
            return cur_results_bucket

        # Same-account: hub's own Athena results bucket
        bucket = os.environ.get('ATHENA_RESULTS_BUCKET') or os.environ.get('ATHENA_OUTPUT_BUCKET')
        if bucket:
            logger.debug(f"Using Athena results bucket from environment: {bucket}")
            return bucket

        # Auto-derive from caller identity (same-account fallback)
        try:
            account_id = self.cur_session.client(
                'sts',
                region_name=self.region,
            ).get_caller_identity()['Account']
            default_bucket = f"aws-athena-query-results-{account_id}-{self.region}"
            logger.debug(f"Using default Athena results bucket: {default_bucket}")
            return default_bucket
        except Exception as e:
            logger.warning(f"Could not determine account ID for Athena results bucket: {e}")
            return "aws-athena-query-results-default"
    
    def get_costs_by_tag(self, tag_key: str, months_back: int = 1, **kwargs) -> Dict:
        """
        Get cost breakdown by tag values using CUR data
        
        Args:
            tag_key: Tag key to analyze (e.g., 'Environment')
            months_back: Number of months to analyze
            **kwargs: Additional query parameters
            
        Returns:
            Dict: Cost breakdown with metadata
        """
        logger.info(f"Getting CUR costs by tag: {tag_key} (last {months_back} months)")
        
        try:
            # Get available tag columns
            available_columns = self.available_tag_columns
            
            # Build query
            query, missing_partitions = self.query_builder.build_costs_by_tag_query(
                tag_key, months_back, available_columns
            )
            
            if query is None:
                logger.warning(f"Cannot build query - tag column for '{tag_key}' not available")
                return {
                    'error': f'Tag column for \'{tag_key}\' not found in CUR table',
                    'available_tag_columns': len(available_columns),
                    'tag_key': tag_key
                }
            
            # Execute query
            results = self._execute_athena_query(query, f"costs_by_tag_{tag_key}")
            
            if not results or 'error' in results:
                return results or {'error': 'Query execution failed'}
            
            # Process and format results
            breakdown = {}
            total_cost = 0
            resource_count = 0
            
            for row in results.get('data', []):
                tag_value = row.get('tag_value', 'Unknown')
                cost = float(row.get('total_cost', 0))
                resources = int(row.get('resource_count', 0))
                
                breakdown[tag_value] = f"${cost:,.2f}"
                total_cost += cost
                resource_count += resources
            
            # Cost estimation
            cost_estimate = self.query_builder.estimate_query_cost(query)
            
            return {
                'breakdown': breakdown,
                'total_cost': f"${total_cost:,.2f}",
                'tag_key': tag_key,
                'time_period': f'Last {months_back} month{"s" if months_back > 1 else ""}',
                'data_source': 'cur_athena',
                'tag_values_found': len(breakdown),
                'total_resources': resource_count,
                'query_metadata': {
                    'query_id': results.get('query_execution_id'),
                    'execution_time_seconds': results.get('execution_time_seconds'),
                    'data_scanned_bytes': results.get('data_scanned_bytes'),
                    'cost_estimate': cost_estimate
                },
                'query_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting costs by tag from CUR: {e}")
            return {
                'error': str(e),
                'tag_key': tag_key,
                'data_source': 'cur_athena'
            }
    
    def get_cost_trends_for_tag_value(self, tag_key: str, tag_value: str, 
                                     months_back: int = 6, **kwargs) -> Dict:
        """
        Get cost trends for specific tag value using CUR data
        
        Args:
            tag_key: Tag key (e.g., 'Environment') 
            tag_value: Tag value (e.g., 'production')
            months_back: Number of months to analyze
            **kwargs: Additional query parameters
            
        Returns:
            Dict: Cost trends with monthly breakdown
        """
        logger.info(f"Getting CUR cost trends: {tag_key}='{tag_value}' (last {months_back} months)")
        
        try:
            # Get available tag columns
            available_columns = self.available_tag_columns
            
            # Build query
            query = self.query_builder.build_cost_trends_query(
                tag_key, tag_value, months_back, available_columns
            )
            
            if query is None:
                return {
                    'error': f'Tag column for \'{tag_key}\' not found in CUR table',
                    'tag_key': tag_key,
                    'tag_value': tag_value
                }
            
            # Execute query
            results = self._execute_athena_query(query, f"cost_trends_{tag_key}_{tag_value}")
            
            if not results or 'error' in results:
                return results or {'error': 'Query execution failed'}
            
            # Process results into monthly costs
            monthly_costs = {}
            total_cost = 0
            total_resources = 0
            
            for row in results.get('data', []):
                month_key = row.get('month_key', 'Unknown')
                cost = float(row.get('monthly_cost', 0))
                resources = int(row.get('resource_count', 0))
                
                monthly_costs[month_key] = f"${cost:,.2f}"
                total_cost += cost
                total_resources += resources
            
            # Calculate trend
            if len(monthly_costs) >= 2:
                costs = [float(cost.replace('$', '').replace(',', '')) for cost in monthly_costs.values()]
                if costs[-1] > costs[0]:
                    trend = "increasing"
                elif costs[-1] < costs[0]:
                    trend = "decreasing"
                else:
                    trend = "stable"
            else:
                trend = "insufficient_data"
            
            # Calculate averages
            avg_monthly = total_cost / len(monthly_costs) if monthly_costs else 0
            
            return {
                'monthly_costs': monthly_costs,
                'tag_key': tag_key,
                'tag_value': tag_value,
                'trend': trend,
                'total_cost': f"${total_cost:,.2f}",
                'average_monthly_cost': f"${avg_monthly:,.2f}",
                'months_with_data': len(monthly_costs),
                'total_resources': total_resources,
                'time_period': f'Last {months_back} months',
                'data_source': 'cur_athena',
                'query_metadata': {
                    'query_id': results.get('query_execution_id'),
                    'execution_time_seconds': results.get('execution_time_seconds')
                },
                'query_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting cost trends from CUR: {e}")
            return {
                'error': str(e),
                'tag_key': tag_key,
                'tag_value': tag_value,
                'data_source': 'cur_athena'
            }
    
    # Note: get_untagged_costs method removed - now handled by CURDirectMatcher with exact ARN matching
    
    def _execute_athena_query(self, query: str, query_name: str = "unnamed") -> Optional[Dict]:
        """
        Execute Athena query with error handling and result processing
        
        Args:
            query: SQL query string
            query_name: Descriptive name for logging
            
        Returns:
            Dict: Query results with metadata or error information
        """
        query_execution_id = None
        start_time = datetime.now()
        
        try:
            # Query text is not logged: it contains resource ARNs and tag values.
            logger.info(f"🔍 Executing Athena query '{query_name}' ({len(query)} chars)")
            
            # Discover workgroup (TagInsights workgroup or fallback to primary)
            workgroup = self._discover_workgroup()
            
            # Start query execution
            response = self.athena_client.start_query_execution(
                QueryString=query,
                ResultConfiguration={
                    'OutputLocation': f's3://{self.results_bucket}/ti-queries/'
                },
                WorkGroup=workgroup
            )
            
            query_execution_id = response['QueryExecutionId']
            logger.debug(f"Started Athena query: {query_execution_id}")
            
            # Wait for query completion
            execution_result = self._wait_for_query_completion(query_execution_id)
            
            if not execution_result['success']:
                return {
                    'error': execution_result['error'],
                    'query_execution_id': query_execution_id,
                    'query_name': query_name
                }
            
            # Get query results
            query_results = self._get_query_results(query_execution_id)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            return {
                'data': query_results['data'],
                'query_execution_id': query_execution_id,
                'execution_time_seconds': execution_time,
                'data_scanned_bytes': execution_result.get('data_scanned_bytes', 0),
                'query_name': query_name,
                'success': True
            }
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Error executing Athena query '{query_name}': {e}")
            return {
                'error': str(e),
                'query_execution_id': query_execution_id,
                'execution_time_seconds': execution_time,
                'query_name': query_name,
                'success': False
            }
    
    def _wait_for_query_completion(self, query_execution_id: str) -> Dict:
        """
        Wait for Athena query to complete with timeout
        
        Args:
            query_execution_id: Athena query execution ID
            
        Returns:
            Dict: Execution result with success status and metadata
        """
        wait_time = 0
        check_interval = 2  # seconds
        
        while wait_time < self.query_timeout:
            try:
                execution_response = self.athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                
                query_execution = execution_response['QueryExecution']
                status = query_execution['Status']['State']
                
                if status == 'SUCCEEDED':
                    # Get query statistics
                    statistics = query_execution.get('Statistics', {})
                    data_scanned_bytes = statistics.get('DataScannedInBytes', 0)
                    execution_time_ms = statistics.get('EngineExecutionTimeInMillis', 0)
                    
                    logger.debug(f"Query {query_execution_id} completed successfully")
                    logger.debug(f"Data scanned: {data_scanned_bytes:,} bytes ({data_scanned_bytes / (1024**3):.3f} GB)")
                    logger.debug(f"Execution time: {execution_time_ms / 1000:.2f} seconds")
                    
                    return {
                        'success': True,
                        'data_scanned_bytes': data_scanned_bytes,
                        'execution_time_ms': execution_time_ms,
                        'estimated_cost_usd': (data_scanned_bytes / (1024**4)) * 5.0  # $5 per TB
                    }
                    
                elif status in ['FAILED', 'CANCELLED']:
                    error_reason = query_execution['Status'].get('StateChangeReason', 'Unknown error')
                    logger.error(f"Query {query_execution_id} failed: {error_reason}")
                    return {
                        'success': False,
                        'error': f'Query failed: {error_reason}'
                    }
                
                # Still running, wait and check again
                time.sleep(check_interval)
                wait_time += check_interval
                
            except Exception as e:
                logger.error(f"Error checking query status {query_execution_id}: {e}")
                return {
                    'success': False,
                    'error': f'Error checking query status: {e}'
                }
        
        # Timeout
        logger.error(f"Query {query_execution_id} timed out after {self.query_timeout} seconds")
        return {
            'success': False,
            'error': f'Query timed out after {self.query_timeout} seconds'
        }
    
    def _get_query_results(self, query_execution_id: str) -> Dict:
        """
        Retrieve and process Athena query results
        
        Args:
            query_execution_id: Athena query execution ID
            
        Returns:
            Dict: Processed query results with data and metadata
        """
        try:
            # Get results
            results_response = self.athena_client.get_query_results(
                QueryExecutionId=query_execution_id,
                MaxResults=1000  # Limit result size for cost control
            )
            
            result_set = results_response['ResultSet']
            rows = result_set.get('Rows', [])
            
            if len(rows) == 0:
                logger.warning(f"Query {query_execution_id} returned no results")
                return {'data': [], 'row_count': 0}
            
            # Process column metadata
            column_info = result_set.get('ResultSetMetadata', {}).get('ColumnInfo', [])
            column_names = [col['Name'] for col in column_info]
            
            # Process data rows (skip header row)
            data_rows = []
            for row in rows[1:]:  # Skip header
                values = row.get('Data', [])
                row_data = {}
                
                for i, value in enumerate(values):
                    if i < len(column_names):
                        column_name = column_names[i]
                        # Extract value from DynamoDB-style format
                        cell_value = value.get('VarCharValue', '')
                        row_data[column_name] = cell_value
                
                data_rows.append(row_data)
            
            logger.debug(f"Processed {len(data_rows)} result rows from query {query_execution_id}")
            
            return {
                'data': data_rows,
                'row_count': len(data_rows),
                'column_names': column_names,
                'has_more_results': 'NextToken' in results_response
            }
            
        except Exception as e:
            logger.error(f"Error getting query results for {query_execution_id}: {e}")
            return {
                'error': f'Error retrieving results: {e}',
                'data': []
            }
    
    def test_connectivity(self) -> Dict:
        """
        Test CUR provider connectivity and basic functionality
        
        Returns:
            Dict: Test results with success status and details
        """
        logger.info("Testing CUR provider connectivity...")
        
        try:
            # Test 1: Schema access
            schema_test = self.schema_manager.validate_cur_table(self.database, self.table)
            if not schema_test:
                return {
                    'success': False,
                    'error': 'CUR table validation failed',
                    'database': self.database,
                    'table': self.table
                }
            
            test_query = self.query_builder.build_connectivity_query()
            
            result = self._execute_athena_query(test_query, "connectivity_test")
            
            if result and result.get('success'):
                return {
                    'success': True,
                    'message': 'CUR provider connectivity test successful',
                    'database': self.database,
                    'table': self.table,
                    'test_query_id': result.get('query_execution_id'),
                    'execution_time_seconds': result.get('execution_time_seconds'),
                    'data_scanned_bytes': result.get('data_scanned_bytes')
                }
            else:
                return {
                    'success': False,
                    'error': result.get('error', 'Unknown query execution error'),
                    'database': self.database,
                    'table': self.table
                }
                
        except Exception as e:
            logger.error(f"CUR provider connectivity test failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'database': self.database,
                'table': self.table
            }
    
    def get_available_tags_summary(self) -> Dict:
        """
        Get summary of available tag columns in CUR table
        
        Returns:
            Dict: Summary of tag availability and recommendations
        """
        try:
            if self.schema_profile.get('tag_mode') == 'map':
                cache_key = f"{self.database}.{self.table}:map_tag_keys"
                cached = self._tag_key_summary_cache.get(cache_key)
                if (
                    cached
                    and time.time() - cached['timestamp']
                    < self._tag_key_summary_cache_ttl_seconds
                ):
                    tag_names = cached['tag_names']
                else:
                    query = self.query_builder.build_available_tag_keys_query(
                        months_back=1,
                        limit=1000,
                    )
                    results = self._execute_athena_query(
                        query,
                        "available_cur2_tag_keys",
                    )
                    if not results or results.get('error'):
                        return {
                            'error': (results or {}).get(
                                'error',
                                'Could not enumerate CUR2 tag keys',
                            ),
                            'database': self.database,
                            'table': self.table,
                        }
                    tag_names = sorted({
                        row['tag_key'].removeprefix('user_')
                        for row in results.get('data', [])
                        if row.get('tag_key')
                    })
                    self._tag_key_summary_cache[cache_key] = {
                        'tag_names': tag_names,
                        'timestamp': time.time(),
                    }
            else:
                tag_names = sorted(
                    column.replace('resource_tags_user_', '')
                    for column in self.available_tag_columns
                    if column.startswith('resource_tags_user_')
                )
            
            return {
                'available_tag_count': len(tag_names),
                'available_tags': tag_names,
                'sample_tags': tag_names[:10],  # First 10 for display
                'database': self.database,
                'table': self.table,
                'recommendations': [
                    "These tags have historical data available in CUR",
                    "Tags not in this list may still be queryable via Cost Explorer",
                    "Consider using these tags for cost allocation analysis"
                ]
            }
            
        except Exception as e:
            logger.error(f"Error getting available tags summary: {e}")
            return {
                'error': str(e),
                'database': self.database,
                'table': self.table
            }


if __name__ == "__main__":
    # Simple test/demo
    print("🧪 Testing CUR Cost Provider...")
    
    # This would normally get these from environment variables or discovery
    test_database = os.environ.get('CUR_ATHENA_DATABASE', 'test_cur_db')
    test_table = os.environ.get('CUR_ATHENA_TABLE', 'test_cur_table')
    
    provider = CURCostProvider(test_database, test_table)
    
    # Test connectivity
    connectivity = provider.test_connectivity()
    if connectivity['success']:
        print("✅ Connectivity test passed")
        
        # Test available tags
        tags_summary = provider.get_available_tags_summary()
        if 'error' not in tags_summary:
            print(f"🏷️  Found {tags_summary['available_tag_count']} available tags")
            print(f"   Sample tags: {', '.join(tags_summary['sample_tags'])}")
        
        # Test a simple cost query (if Environment tag is available)
        if 'environment' in tags_summary.get('available_tags', []):
            print("🔍 Testing sample cost query...")
            costs = provider.get_costs_by_tag('Environment', months_back=1)
            if 'error' not in costs:
                print(f"✅ Cost query successful - found {costs.get('tag_values_found', 0)} values")
            else:
                print(f"❌ Cost query failed: {costs['error']}")
        
    else:
        print(f"❌ Connectivity test failed: {connectivity['error']}")
        print("💡 Check CUR_ATHENA_DATABASE and CUR_ATHENA_TABLE environment variables")
    
    print("🚀 CUR Cost Provider test completed")

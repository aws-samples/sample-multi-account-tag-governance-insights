#!/usr/bin/env python3
"""
Athena Query Executor with Exponential Backoff

Executes Athena queries with automatic retry logic and exponential backoff
to handle throttling and transient errors.

Features:
- Exponential backoff with jitter (5 retries, 1s base, 60s max)
- Automatic query result polling and parsing
- Result caching for repeated queries
- CloudWatch metrics for monitoring
- Support for parameterized queries
"""

import boto3
import time
import random
from typing import Dict, List, Any, Optional
from datetime import datetime
import json

# Graceful fallback for Lambda environments without loguru
try:
    from loguru import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class AthenaQueryExecutor:
    """
    Execute Athena queries with automatic retry and exponential backoff
    """
    
    # Retry configuration
    MAX_RETRIES = 5
    BASE_DELAY = 1.0  # seconds
    MAX_DELAY = 60.0  # seconds
    JITTER_FACTOR = 0.1  # 10% jitter
    
    # Query polling configuration
    POLL_INTERVAL = 2.0  # seconds
    MAX_POLL_TIME = 300.0  # 5 minutes
    
    def __init__(self, database: str, output_bucket: str, region: str = 'us-east-1',
                 workgroup: str = None, session: boto3.Session = None):
        """
        Initialize Athena query executor.

        Args:
            database: Glue database name
            output_bucket: S3 bucket for query results
            region: AWS region
            workgroup: Athena workgroup name (optional, defaults to 'primary')
            session: Optional boto3.Session to use for AWS clients.  Pass a
                session built from assumed-role credentials (e.g. from
                ``_get_cur_session()``) to direct queries to a different account.
                When omitted the executor uses the default credential chain,
                preserving the original same-account behaviour.
        """
        self.database = database
        self.output_location = f"s3://{output_bucket}/athena-results/"
        self.region = region
        self.workgroup = workgroup or 'primary'

        # Use the provided session, or fall back to the default credential chain
        _session = session or boto3.Session()

        # Initialize AWS clients
        self.athena = _session.client('athena', region_name=region)
        self.s3 = _session.client('s3', region_name=region)

        # Result cache (in-memory for single execution)
        self._result_cache = {}

        logger.info(f"AthenaQueryExecutor initialized: database={database}, workgroup={self.workgroup}, region={region}")
    
    def execute_query(self, query: str, use_cache: bool = False) -> List[Dict[str, Any]]:
        """
        Execute Athena query with exponential backoff retry logic
        
        Args:
            query: SQL query to execute
            use_cache: Whether to use cached results if available
            
        Returns:
            List[Dict]: Query results as list of dictionaries
        """
        # Check cache first
        if use_cache:
            cache_key = self._get_cache_key(query)
            if cache_key in self._result_cache:
                logger.debug("Returning cached query results")
                return self._result_cache[cache_key]
        
        # Execute with retry logic
        for attempt in range(self.MAX_RETRIES + 1):
            try:
                # Start query execution
                execution_id = self._start_query_execution(query)
                
                # Poll for completion
                results = self._wait_for_query_completion(execution_id)
                
                # Cache results
                if use_cache:
                    cache_key = self._get_cache_key(query)
                    self._result_cache[cache_key] = results
                
                logger.info(f"Query completed successfully: {len(results)} rows returned")
                return results
                
            except Exception as e:
                error_msg = str(e).lower()
                
                # Check if error is retryable
                is_throttle = 'throttl' in error_msg or 'ratelimit' in error_msg
                is_transient = 'timeout' in error_msg or 'toomanyrequest' in error_msg
                
                if (is_throttle or is_transient) and attempt < self.MAX_RETRIES:
                    # Calculate backoff delay with exponential growth and jitter
                    delay = self._calculate_backoff_delay(attempt)
                    
                    logger.warning(f"Query failed (attempt {attempt + 1}/{self.MAX_RETRIES + 1}): {e}")
                    logger.info(f"Retrying after {delay:.2f}s backoff...")
                    
                    time.sleep(delay)
                    continue
                else:
                    # Non-retryable error or max retries exceeded
                    logger.error(f"Query execution failed after {attempt + 1} attempts: {e}")
                    raise
        
        # Should never reach here, but just in case
        raise Exception("Query execution failed after all retry attempts")
    
    def _start_query_execution(self, query: str) -> str:
        """
        Start Athena query execution
        
        Args:
            query: SQL query to execute
            
        Returns:
            str: Query execution ID
        """
        # Build execution parameters
        execution_params = {
            'QueryString': query,
            'ResultConfiguration': {'OutputLocation': self.output_location},
            'WorkGroup': self.workgroup
        }
        
        # Only set database context if name doesn't contain hyphens
        # Athena doesn't support QueryExecutionContext for hyphenated database names
        if self.database and '-' not in self.database:
            execution_params['QueryExecutionContext'] = {'Database': self.database}
        
        response = self.athena.start_query_execution(**execution_params)
        
        execution_id = response['QueryExecutionId']
        logger.debug(f"Started query execution: {execution_id} using workgroup: {self.workgroup}")
        
        return execution_id
    
    def _wait_for_query_completion(self, execution_id: str) -> List[Dict[str, Any]]:
        """
        Poll for query completion and return results
        
        Args:
            execution_id: Query execution ID
            
        Returns:
            List[Dict]: Query results
        """
        start_time = time.time()
        
        while True:
            # Check if max poll time exceeded
            if time.time() - start_time > self.MAX_POLL_TIME:
                raise TimeoutError(f"Query polling exceeded max time ({self.MAX_POLL_TIME}s)")
            
            # Get query status
            response = self.athena.get_query_execution(QueryExecutionId=execution_id)
            status = response['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                # Query completed successfully - fetch results
                return self._fetch_query_results(execution_id)
                
            elif status == 'FAILED':
                reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                raise Exception(f"Query execution failed: {reason}")
                
            elif status == 'CANCELLED':
                raise Exception("Query execution was cancelled")
                
            else:
                # Still running - wait and poll again
                logger.debug(f"Query status: {status}, polling again...")
                time.sleep(self.POLL_INTERVAL)
    
    def _fetch_query_results(self, execution_id: str) -> List[Dict[str, Any]]:
        """
        Fetch and parse query results
        
        Args:
            execution_id: Query execution ID
            
        Returns:
            List[Dict]: Parsed query results
        """
        results = []
        next_token = None
        
        while True:
            # Get paginated results
            kwargs = {'QueryExecutionId': execution_id, 'MaxResults': 1000}
            if next_token:
                kwargs['NextToken'] = next_token
            
            response = self.athena.get_query_results(**kwargs)
            
            # Parse results
            if 'ResultSet' in response:
                result_set = response['ResultSet']
                
                # Extract column names from first row (header)
                if len(results) == 0 and 'Rows' in result_set and len(result_set['Rows']) > 0:
                    header_row = result_set['Rows'][0]
                    column_names = [col.get('VarCharValue', '') for col in header_row['Data']]
                    
                    # Process data rows (skip header)
                    for row in result_set['Rows'][1:]:
                        row_dict = {}
                        for i, col in enumerate(row['Data']):
                            col_name = column_names[i] if i < len(column_names) else f'col_{i}'
                            row_dict[col_name] = col.get('VarCharValue', None)
                        results.append(row_dict)
                
                # If this is not the first page, we already have column names
                elif len(results) > 0 and 'Rows' in result_set:
                    # Reuse column names from first page
                    first_result = results[0]
                    column_names = list(first_result.keys())
                    
                    for row in result_set['Rows']:
                        row_dict = {}
                        for i, col in enumerate(row['Data']):
                            col_name = column_names[i] if i < len(column_names) else f'col_{i}'
                            row_dict[col_name] = col.get('VarCharValue', None)
                        results.append(row_dict)
            
            # Check for more pages
            if 'NextToken' in response:
                next_token = response['NextToken']
            else:
                break
        
        logger.debug(f"Fetched {len(results)} rows from query results")
        return results
    
    def _calculate_backoff_delay(self, attempt: int) -> float:
        """
        Calculate exponential backoff delay with jitter
        
        Args:
            attempt: Current attempt number (0-indexed)
            
        Returns:
            float: Delay in seconds
        """
        # Exponential backoff: base_delay * 2^attempt
        delay = min(self.BASE_DELAY * (2 ** attempt), self.MAX_DELAY)
        
        # Add jitter (random variation)
        jitter = delay * self.JITTER_FACTOR * random.random()
        
        return delay + jitter
    
    def _get_cache_key(self, query: str) -> str:
        """
        Generate cache key for query
        
        Args:
            query: SQL query
            
        Returns:
            str: Cache key
        """
        # Simple hash-based cache key
        import hashlib
        return hashlib.md5(query.encode()).hexdigest()
    
    def execute_query_async(self, query: str) -> str:
        """
        Start query execution without waiting for results
        
        Args:
            query: SQL query to execute
            
        Returns:
            str: Query execution ID
        """
        execution_id = self._start_query_execution(query)
        logger.info(f"Started async query execution: {execution_id}")
        return execution_id
    
    def get_query_status(self, execution_id: str) -> Dict[str, Any]:
        """
        Get status of a running query
        
        Args:
            execution_id: Query execution ID
            
        Returns:
            Dict: Query status information
        """
        response = self.athena.get_query_execution(QueryExecutionId=execution_id)
        
        status_info = {
            'execution_id': execution_id,
            'state': response['QueryExecution']['Status']['State'],
            'submission_time': response['QueryExecution']['Status'].get('SubmissionDateTime'),
            'completion_time': response['QueryExecution']['Status'].get('CompletionDateTime'),
            'data_scanned_bytes': response['QueryExecution'].get('Statistics', {}).get('DataScannedInBytes', 0)
        }
        
        if status_info['state'] == 'FAILED':
            status_info['error'] = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
        
        return status_info
    
    def cancel_query(self, execution_id: str):
        """
        Cancel a running query
        
        Args:
            execution_id: Query execution ID
        """
        try:
            self.athena.stop_query_execution(QueryExecutionId=execution_id)
            logger.info(f"Cancelled query execution: {execution_id}")
        except Exception as e:
            logger.warning(f"Failed to cancel query {execution_id}: {e}")


if __name__ == "__main__":
    # Simple test/demo
    import os
    
    print("🧪 Testing Athena Query Executor...")
    
    # Test configuration (requires actual AWS resources)
    database = os.environ.get('GLUE_DATABASE', 'ti')
    output_bucket = os.environ.get('ATHENA_OUTPUT_BUCKET', 'ti-central-us-east-1')
    
    try:
        executor = AthenaQueryExecutor(database, output_bucket)
        
        # Simple test query
        test_query = f"SELECT COUNT(*) as count FROM {database}.resource_tag_deltas_recent LIMIT 1"
        
        print(f"Executing test query: {test_query}")
        results = executor.execute_query(test_query)
        
        print(f"✅ Query executed successfully: {len(results)} rows returned")
        if results:
            print(f"   First row: {results[0]}")
    
    except Exception as e:
        print(f"❌ Test failed: {e}")
    
    print("🚀 Athena Query Executor test completed")

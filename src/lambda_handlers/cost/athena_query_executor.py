#!/usr/bin/env python3
"""
Minimal Athena wrapper used by the cost allocator: start a query, read its
status, cancel it. Polling and timeouts live in cost_allocator_handler.
"""

from typing import Any, Dict

import boto3

try:
    from loguru import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)


class AthenaQueryExecutor:
    def __init__(self, database: str, output_bucket: str, region: str = 'us-east-1', workgroup: str = None):
        self.database = database
        self.output_location = f"s3://{output_bucket}/athena-results/"
        self.workgroup = workgroup or 'primary'
        self.athena = boto3.client('athena', region_name=region)

    def execute_query_async(self, query: str) -> str:
        """Start a query and return its execution ID without waiting."""
        params = {
            'QueryString': query,
            'ResultConfiguration': {'OutputLocation': self.output_location},
            'WorkGroup': self.workgroup,
        }
        # Athena rejects QueryExecutionContext for hyphenated database names; queries fully qualify tables.
        if self.database and '-' not in self.database:
            params['QueryExecutionContext'] = {'Database': self.database}
        execution_id = self.athena.start_query_execution(**params)['QueryExecutionId']
        logger.info(f"Started Athena query execution: {execution_id}")
        return execution_id

    def get_query_status(self, execution_id: str) -> Dict[str, Any]:
        execution = self.athena.get_query_execution(QueryExecutionId=execution_id)['QueryExecution']
        status = execution['Status']
        info = {
            'execution_id': execution_id,
            'state': status['State'],
            'data_scanned_bytes': execution.get('Statistics', {}).get('DataScannedInBytes', 0),
        }
        if info['state'] in ('FAILED', 'CANCELLED'):
            info['error'] = status.get('StateChangeReason', 'Unknown')
        return info

    def cancel_query(self, execution_id: str) -> None:
        """Best-effort stop; a failure to cancel is logged, not raised."""
        try:
            self.athena.stop_query_execution(QueryExecutionId=execution_id)
            logger.info(f"Cancelled query execution: {execution_id}")
        except Exception as e:
            logger.warning(f"Failed to cancel query {execution_id}: {e}")

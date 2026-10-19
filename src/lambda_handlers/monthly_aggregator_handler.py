"""
Monthly Aggregator Lambda Handler

Runs monthly to aggregate 90-day-old daily data into monthly summaries.
This reduces partition count and storage costs while maintaining query performance.

Architecture:
- Triggered by: EventBridge schedule (monthly, day 2)
- Reads from: resource_tag_deltas_recent (daily partitions)
- Writes to: resource_tag_deltas_historical (monthly partitions)
- Cleanup: Deletes old daily partitions after aggregation

Partition Reduction:
- Before: 1000 accounts × 90 days = 90,000 partitions
- After: 1000 accounts × 1 month = 1,000 partitions (90x reduction)
"""

import json
import os
import re
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
from io import BytesIO

import boto3
from botocore.exceptions import ClientError

# Optional dependencies
try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False
    logging.warning("Parquet libraries not available")

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# AWS clients
s3_client = boto3.client('s3')
athena_client = boto3.client('athena')
glue_client = boto3.client('glue')

# Environment variables
CENTRAL_ANALYTICS_BUCKET = os.environ.get('CENTRAL_ANALYTICS_BUCKET')
ATHENA_RESULTS_BUCKET = os.environ.get('ATHENA_RESULTS_BUCKET')
ATHENA_DATABASE = os.environ.get('ATHENA_DATABASE')
ATHENA_WORKGROUP = os.environ.get('ATHENA_WORKGROUP')
RETENTION_DAYS = int(os.environ.get('RETENTION_DAYS', '90'))
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'prod')


def lambda_handler(event, context):
    """
    Monthly aggregation handler
    
    Args:
        event: Trigger event (EventBridge schedule or manual invoke)
        context: Lambda context
        
    Returns:
        Dict: Aggregation results
    """
    try:
        logger.info("Monthly Aggregator started")
        
        # Determine which month to aggregate (90+ days ago)
        target_date = datetime.now() - timedelta(days=RETENTION_DAYS + 5)  # Buffer
        target_year = target_date.year
        target_month = target_date.month
        
        logger.info(f"Aggregating data for {target_year}-{target_month:02d}")
        
        # Get list of accounts from recent table
        accounts = discover_accounts(target_year, target_month)
        logger.info(f"Found {len(accounts)} accounts to process")
        
        results = {
            'target_year': target_year,
            'target_month': target_month,
            'accounts_processed': 0,
            'accounts_failed': 0,
            'total_deltas_aggregated': 0,
            'partitions_deleted': 0
        }
        
        # Process each account
        for account_id in accounts:
            try:
                logger.info(f"Processing account: {account_id}")
                
                # Aggregate daily data to monthly
                delta_count = aggregate_account_month(account_id, target_year, target_month)
                
                # Delete old daily partitions
                deleted = delete_daily_partitions(account_id, target_year, target_month)
                
                results['accounts_processed'] += 1
                results['total_deltas_aggregated'] += delta_count
                results['partitions_deleted'] += deleted
                
                logger.info(f"Completed {account_id}: {delta_count} deltas, {deleted} partitions deleted")
                
            except Exception as e:
                results['accounts_failed'] += 1
                logger.error(f"Error processing account {account_id}: {str(e)}", exc_info=True)
                # Continue with other accounts
        
        logger.info(f"Monthly aggregation completed: {results}")
        return results
        
    except Exception as e:
        logger.error(f"Fatal error in Monthly Aggregator: {str(e)}", exc_info=True)
        raise


def discover_accounts(year: int, month: int) -> List[str]:
    """
    Discover accounts that have data for the target month
    
    Args:
        year: Target year
        month: Target month
        
    Returns:
        List[str]: List of account IDs
    """
    try:
        # List partitions in recent table for this month
        prefix = f"tag-deltas/recent/"
        
        # Use S3 list to find account_id directories
        response = s3_client.list_objects_v2(
            Bucket=CENTRAL_ANALYTICS_BUCKET,
            Prefix=prefix,
            Delimiter='/'
        )
        
        accounts = set()
        
        # Extract account IDs from common prefixes
        for prefix_info in response.get('CommonPrefixes', []):
            prefix_path = prefix_info['Prefix']
            # Format: tag-deltas/recent/account_id=123456789012/
            if 'account_id=' in prefix_path:
                account_id = prefix_path.split('account_id=')[1].strip('/')
                if re.fullmatch(r'\d{12}', account_id):
                    accounts.add(account_id)
                else:
                    logger.warning(f"Skipping invalid account_id from S3 prefix: {account_id!r}")
        
        logger.info(f"Discovered {len(accounts)} accounts via S3 listing")
        return list(accounts)
        
    except Exception as e:
        logger.error(f"Error discovering accounts: {str(e)}")
        # Fallback: query Athena
        return discover_accounts_via_athena(year, month)


def discover_accounts_via_athena(year: int, month: int) -> List[str]:
    """Fallback: discover accounts via Athena query"""
    try:
        query = f"""
        SELECT DISTINCT account_id
        FROM resource_tag_deltas_recent
        WHERE year = {year} AND month = {month}
        """

        results = execute_athena_query(query)
        accounts = []
        for row in results:
            aid = row.get('account_id', '')
            if re.fullmatch(r'\d{12}', aid):
                accounts.append(aid)
            else:
                logger.warning(f"Skipping invalid account_id from Athena: {aid!r}")

        logger.info(f"Discovered {len(accounts)} accounts via Athena")
        return accounts

    except Exception as e:
        logger.error(f"Error discovering accounts via Athena: {str(e)}")
        return []


def aggregate_account_month(account_id: str, year: int, month: int) -> int:
    """
    Aggregate one month of daily data for one account into monthly summary
    
    Args:
        account_id: AWS account ID
        year: Target year
        month: Target month
        
    Returns:
        int: Number of delta records aggregated
    """
    if not re.fullmatch(r'\d{12}', account_id):
        logger.error(f"Invalid account_id passed to aggregate_account_month: {account_id!r}")
        return 0

    try:
        logger.info(f"Aggregating {account_id}/{year}/{month}")

        # Query daily data for this account/month
        query = f"""
        SELECT 
            arn,
            resource_type,
            tags,
            previous_tags,
            change_type,
            MIN(timestamp) as first_seen_timestamp,
            MAX(timestamp) as last_seen_timestamp,
            COUNT(*) as change_count,
            source_system,
            region,
            MAX(scan_id) as scan_id
        FROM resource_tag_deltas_recent
        WHERE account_id = '{account_id}'
          AND year = {year}
          AND month = {month}
        GROUP BY 
            arn, resource_type, tags, previous_tags,
            change_type, source_system, region
        """
        
        # Execute query
        results = execute_athena_query(query)
        
        if not results:
            logger.info(f"No data found for {account_id}/{year}/{month}")
            return 0
        
        logger.info(f"Aggregated {len(results)} records for {account_id}")
        
        # Write aggregated data to historical partition
        write_monthly_aggregate(results, account_id, year, month)
        
        return len(results)
        
    except Exception as e:
        logger.error(f"Error aggregating {account_id}/{year}/{month}: {str(e)}")
        raise


def execute_athena_query(query: str, timeout: int = 300) -> List[Dict]:
    """
    Execute Athena query and return results
    
    Args:
        query: SQL query
        timeout: Query timeout in seconds
        
    Returns:
        List[Dict]: Query results
    """
    try:
        # Start query execution
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': ATHENA_DATABASE},
            ResultConfiguration={
                'OutputLocation': f's3://{ATHENA_RESULTS_BUCKET}/aggregator-queries/'
            },
            WorkGroup=ATHENA_WORKGROUP
        )
        
        query_execution_id = response['QueryExecutionId']
        logger.debug(f"Started Athena query: {query_execution_id}")
        
        # Wait for completion
        start_time = time.time()
        while time.time() - start_time < timeout:
            execution_response = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            
            status = execution_response['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                # Get results
                return get_query_results(query_execution_id)
            elif status in ['FAILED', 'CANCELLED']:
                error = execution_response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                raise Exception(f"Query failed: {error}")
            
            time.sleep(2)
        
        raise Exception(f"Query timeout after {timeout} seconds")
        
    except Exception as e:
        logger.error(f"Error executing Athena query: {str(e)}")
        raise


def get_query_results(query_execution_id: str) -> List[Dict]:
    """Get Athena query results"""
    try:
        results = []
        paginator = athena_client.get_paginator('get_query_results')
        
        for page in paginator.paginate(QueryExecutionId=query_execution_id):
            result_set = page['ResultSet']
            
            # Get column names from first row
            if not results:
                column_info = result_set['ResultSetMetadata']['ColumnInfo']
                columns = [col['Name'] for col in column_info]
            
            # Parse data rows (skip header)
            for row in result_set['Rows'][1:]:
                row_data = {}
                for i, cell in enumerate(row['Data']):
                    if i < len(columns):
                        row_data[columns[i]] = cell.get('VarCharValue', '')
                results.append(row_data)
        
        return results
        
    except Exception as e:
        logger.error(f"Error getting query results: {str(e)}")
        raise


def write_monthly_aggregate(data: List[Dict], account_id: str, year: int, month: int):
    """
    Write monthly aggregate to historical partition
    
    Args:
        data: Aggregated data records
        account_id: AWS account ID
        year: Year
        month: Month
    """
    try:
        if not data:
            return
        
        # Build partition path: account_id/year/month
        partition_path = f"account_id={account_id}/year={year}/month={month}/"
        
        if HAS_PARQUET:
            write_monthly_parquet(data, partition_path)
        else:
            write_monthly_json(data, partition_path)
        
        # Register partition with Glue
        register_monthly_partition(account_id, year, month)
        
        logger.info(f"Wrote monthly aggregate: {partition_path}")
        
    except Exception as e:
        logger.error(f"Error writing monthly aggregate: {str(e)}")
        raise


def write_monthly_parquet(data: List[Dict], partition_path: str):
    """Write monthly aggregate as Parquet"""
    try:
        df = pd.DataFrame(data)
        
        # Convert to Parquet
        table = pa.Table.from_pandas(df)
        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer, compression='snappy')
        parquet_buffer.seek(0)
        
        # Write to S3
        s3_key = f"tag-deltas/historical/{partition_path}monthly-aggregate.parquet"
        
        s3_client.put_object(
            Bucket=CENTRAL_ANALYTICS_BUCKET,
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        
        logger.debug(f"Wrote monthly Parquet: {s3_key}")
        
    except Exception as e:
        logger.error(f"Error writing monthly Parquet: {str(e)}")
        raise


def write_monthly_json(data: List[Dict], partition_path: str):
    """Write monthly aggregate as JSON (fallback)"""
    try:
        s3_key = f"tag-deltas/historical/{partition_path}monthly-aggregate.json"
        
        s3_client.put_object(
            Bucket=CENTRAL_ANALYTICS_BUCKET,
            Key=s3_key,
            Body=json.dumps(data),
            ContentType='application/json'
        )
        
        logger.debug(f"Wrote monthly JSON: {s3_key}")
        
    except Exception as e:
        logger.error(f"Error writing monthly JSON: {str(e)}")
        raise


def register_monthly_partition(account_id: str, year: int, month: int):
    """Register monthly partition with Glue"""
    try:
        partition_location = (
            f"s3://{CENTRAL_ANALYTICS_BUCKET}/tag-deltas/historical/"
            f"account_id={account_id}/year={year}/month={month}/"
        )
        
        glue_client.create_partition(
            DatabaseName=ATHENA_DATABASE,
            TableName='resource_tag_deltas_historical',
            PartitionInput={
                'Values': [account_id, str(year), str(month)],
                'StorageDescriptor': {
                    'Location': partition_location,
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                }
            }
        )
        
        logger.debug(f"Registered monthly partition: {account_id}/{year}/{month}")
        
    except glue_client.exceptions.AlreadyExistsException:
        logger.debug(f"Monthly partition already exists: {account_id}/{year}/{month}")
    except Exception as e:
        logger.warning(f"Could not register monthly partition: {str(e)}")


def delete_daily_partitions(account_id: str, year: int, month: int) -> int:
    """
    Delete daily partitions for the aggregated month
    
    Args:
        account_id: AWS account ID
        year: Year
        month: Month
        
    Returns:
        int: Number of partitions deleted
    """
    try:
        deleted_count = 0
        
        # Delete each day's partition
        for day in range(1, 32):  # Max days in month
            try:
                # Drop partition from Glue
                glue_client.delete_partition(
                    DatabaseName=ATHENA_DATABASE,
                    TableName='resource_tag_deltas_recent',
                    PartitionValues=[account_id, str(year), str(month), str(day)]
                )
                
                # Delete S3 data
                prefix = (
                    f"tag-deltas/recent/account_id={account_id}/"
                    f"year={year}/month={month}/day={day}/"
                )
                
                delete_s3_prefix(prefix)
                
                deleted_count += 1
                logger.debug(f"Deleted partition: {account_id}/{year}/{month}/{day}")
                
            except glue_client.exceptions.EntityNotFoundException:
                # Partition doesn't exist (not all days have data)
                pass
            except Exception as e:
                logger.warning(f"Error deleting partition {account_id}/{year}/{month}/{day}: {str(e)}")
        
        logger.info(f"Deleted {deleted_count} daily partitions for {account_id}/{year}/{month}")
        return deleted_count
        
    except Exception as e:
        logger.error(f"Error deleting daily partitions: {str(e)}")
        return 0


def delete_s3_prefix(prefix: str):
    """Delete all objects under S3 prefix"""
    try:
        # List objects
        paginator = s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=CENTRAL_ANALYTICS_BUCKET, Prefix=prefix):
            objects = page.get('Contents', [])
            
            if objects:
                # Batch delete
                delete_keys = [{'Key': obj['Key']} for obj in objects]
                s3_client.delete_objects(
                    Bucket=CENTRAL_ANALYTICS_BUCKET,
                    Delete={'Objects': delete_keys}
                )
                logger.debug(f"Deleted {len(delete_keys)} objects from {prefix}")
        
    except Exception as e:
        logger.error(f"Error deleting S3 prefix {prefix}: {str(e)}")
        raise

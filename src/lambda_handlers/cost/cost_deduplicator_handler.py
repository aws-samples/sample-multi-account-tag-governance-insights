#!/usr/bin/env python3
"""
Cost Deduplicator Lambda Handler

Deduplicates daily partition data and writes to monthly partitions.

Architecture:
- Triggered after SQS queue is empty (EventBridge + CloudWatch alarm)
- Reads data from daily partitions using Athena
- Deduplicates using Athena CTAS with GROUP BY
- Writes to monthly partition in S3
- Deletes daily partition files
"""

import os
import re
import json
import boto3
from typing import Dict, List, Any
from datetime import datetime, timedelta

try:
    from loguru import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

try:
    from src.lambda_handlers.cost.athena_query_executor import AthenaQueryExecutor
except ImportError:
    try:
        from athena_query_executor import AthenaQueryExecutor
    except ImportError:
        logger.warning("AthenaQueryExecutor not available")
        AthenaQueryExecutor = None


class CostDeduplicator:
    """Deduplicates daily partition data and writes to monthly partitions"""

    def __init__(self):
        self.region = os.environ.get('AWS_REGION', 'us-east-1')
        self.database_name = os.environ.get('ATHENA_DATABASE', 'ti')
        self.athena_workgroup = os.environ.get('ATHENA_WORKGROUP', 'primary')
        self.s3_bucket = os.environ.get('S3_BUCKET') or os.environ.get('CENTRAL_ANALYTICS_BUCKET')
        self.athena_output_bucket = os.environ.get('ATHENA_OUTPUT_BUCKET', self.s3_bucket)

        self.s3 = boto3.client('s3', region_name=self.region)
        self.glue = boto3.client('glue', region_name=self.region)
        self.cloudwatch = boto3.client('cloudwatch', region_name=self.region)

        if AthenaQueryExecutor:
            self.athena = AthenaQueryExecutor(
                database=self.database_name,
                workgroup=self.athena_workgroup,
                output_bucket=self.athena_output_bucket,
                region=self.region
            )
        else:
            self.athena = None
            logger.warning("Athena query executor not available")

        logger.info(f"Deduplicator initialized: database={self.database_name}, bucket={self.s3_bucket}")

    def deduplicate_daily_partitions(self, process_date: datetime = None) -> Dict[str, Any]:
        if process_date is None:
            process_date = datetime.utcnow() - timedelta(days=1)

        year = process_date.year
        month = process_date.month
        day = process_date.day

        logger.info(f"Deduplicating daily partition: {year}/{month}/{day}")

        account_ids = self._find_accounts_in_daily_partition(year, month, day)

        if not account_ids:
            logger.info(f"No accounts found in daily partition {year}/{month}/{day}")
            return {'status': 'success', 'accounts_processed': 0, 'rows_deduplicated': 0, 'message': 'No data to process'}

        total_rows = 0
        accounts_processed = []
        accounts_failed = []

        for account_id in account_ids:
            try:
                rows = self._deduplicate_account_month(account_id, year, month, day)
                total_rows += rows
                accounts_processed.append(account_id)
                logger.info(f"Account {account_id}: deduplicated {rows} rows")
            except Exception as e:
                logger.error(f"Failed to deduplicate account {account_id}: {e}")
                accounts_failed.append(account_id)

        for account_id in accounts_processed:
            try:
                self._add_monthly_partition(account_id, year, month)
            except Exception as e:
                logger.warning(f"Failed to add partition for {account_id}: {e}")

        cleanup_stats = self._cleanup_daily_partition(year, month, day, accounts_processed)

        stats = {
            'status': 'success' if not accounts_failed else 'partial_success',
            'accounts_processed': len(accounts_processed),
            'accounts_failed': len(accounts_failed),
            'total_rows_deduplicated': total_rows,
            'daily_files_deleted': cleanup_stats['files_deleted'],
            'partition_date': f"{year}/{month}/{day}",
            'timestamp': datetime.utcnow().isoformat()
        }

        self._publish_metric('AccountsProcessed', len(accounts_processed))
        self._publish_metric('RowsDeduplicated', total_rows)
        self._publish_metric('DailyFilesDeleted', cleanup_stats['files_deleted'])

        logger.info(f"Deduplication complete: {stats}")
        return stats

    def _find_accounts_in_daily_partition(self, year: int, month: int, day: int) -> List[str]:
        prefix = f"tag-costs-daily/account_id="
        try:
            account_ids = set()
            paginator = self.s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix, Delimiter='/'):
                for prefix_obj in page.get('CommonPrefixes', []):
                    account_prefix = prefix_obj['Prefix']
                    account_id = account_prefix.split('account_id=')[1].rstrip('/')
                    if not re.fullmatch(r'\d{12}', account_id):
                        logger.warning(f"Skipping invalid account_id from S3 prefix: {account_id!r}")
                        continue
                    day_prefix = f"{account_prefix}year={year}/month={month}/day={day}/"
                    response = self.s3.list_objects_v2(Bucket=self.s3_bucket, Prefix=day_prefix, MaxKeys=1)
                    if response.get('KeyCount', 0) > 0:
                        account_ids.add(account_id)
            return sorted(list(account_ids))
        except Exception as e:
            logger.error(f"Error finding accounts in daily partition: {e}")
            return []

    def _deduplicate_account_month(self, account_id: str, year: int, month: int, day: int) -> int:
        temp_daily_table = f'`{self.database_name}`.`tag_costs_daily_{account_id}_{year}{month:02d}{day:02d}`'
        temp_dedup_table_ctas = f'"{self.database_name}"."tag_costs_dedup_{account_id}_{year}{month:02d}"'
        temp_daily_table_select = f'"{self.database_name}"."tag_costs_daily_{account_id}_{year}{month:02d}{day:02d}"'
        temp_dedup_table_drop = f'`{self.database_name}`.`tag_costs_dedup_{account_id}_{year}{month:02d}`'
        temp_dedup_table_name = f"tag_costs_dedup_{account_id}_{year}{month:02d}"
        daily_location = (f"s3://{self.s3_bucket}/tag-costs-daily/account_id={account_id}/year={year}/month={month}/day={day}/")

        try:
            self.athena.execute_query(f"""
            CREATE EXTERNAL TABLE {temp_daily_table} (
                tag_key string, tag_value string, is_cost_allocation_tag boolean,
                resource_arn string, resource_type string, region string,
                last_month_cost double, current_month_mtd double,
                current_month_projected double, avg_daily_cost double,
                analysis_date date, last_updated timestamp
            )
            STORED AS PARQUET
            LOCATION '{daily_location}'
            """)

            self.athena.execute_query(f"""
            CREATE TABLE {temp_dedup_table_ctas}
            WITH (format = 'PARQUET', parquet_compression = 'SNAPPY') AS
            SELECT tag_key, tag_value, ARBITRARY(is_cost_allocation_tag) as is_cost_allocation_tag,
                resource_arn, resource_type, region,
                MAX(last_month_cost) as last_month_cost, MAX(current_month_mtd) as current_month_mtd,
                MAX(current_month_projected) as current_month_projected, MAX(avg_daily_cost) as avg_daily_cost,
                MAX(analysis_date) as analysis_date, MAX(last_updated) as last_updated
            FROM {temp_daily_table_select}
            GROUP BY tag_key, tag_value, resource_arn, resource_type, region
            """)

            count_results = self.athena.execute_query(f'SELECT COUNT(*) as row_count FROM {temp_dedup_table_ctas}')
            row_count = int(count_results[0].get('row_count', 0)) if count_results else 0

            get_table_response = self.glue.get_table(DatabaseName=self.database_name, Name=temp_dedup_table_name)
            ctas_output_location = get_table_response['Table']['StorageDescriptor']['Location']

            monthly_location = f"tag-costs/account_id={account_id}/year={year}/month={month}/"
            self._copy_s3_files(ctas_output_location, monthly_location)

            self.athena.execute_query(f'DROP TABLE IF EXISTS {temp_daily_table}')
            self.athena.execute_query(f'DROP TABLE IF EXISTS {temp_dedup_table_drop}')

            return row_count

        except Exception as e:
            logger.error(f"Error deduplicating account {account_id}: {e}")
            try:
                self.athena.execute_query(f'DROP TABLE IF EXISTS {temp_daily_table}')
            except Exception:
                pass
            try:
                self.athena.execute_query(f'DROP TABLE IF EXISTS {temp_dedup_table_drop}')
            except Exception:
                pass
            raise

    def _copy_s3_files(self, source_location: str, dest_prefix: str) -> int:
        source_location = source_location.rstrip('/')
        if source_location.startswith('s3://'):
            parts = source_location[5:].split('/', 1)
            source_bucket = parts[0]
            source_prefix = parts[1] if len(parts) > 1 else ''
        else:
            return 0

        response = self.s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
        files_copied = 0
        for obj in response.get('Contents', []):
            source_key = obj['Key']
            filename = source_key.split('/')[-1]
            if not filename or '.' in filename or filename.startswith('_'):
                continue
            dest_key = f"{dest_prefix}{filename}"
            self.s3.copy_object(Bucket=self.s3_bucket, CopySource={'Bucket': source_bucket, 'Key': source_key}, Key=dest_key)
            files_copied += 1
        return files_copied

    def _add_monthly_partition(self, account_id: str, year: int, month: int):
        table_name = 'tag_costs'
        try:
            try:
                self.glue.get_partition(DatabaseName=self.database_name, TableName=table_name,
                                        PartitionValues=[account_id, str(year), str(month)])
                return
            except self.glue.exceptions.EntityNotFoundException:
                pass

            table_response = self.glue.get_table(DatabaseName=self.database_name, Name=table_name)
            parent_sd = table_response['Table']['StorageDescriptor']

            location = f"s3://{self.s3_bucket}/tag-costs/account_id={account_id}/year={year}/month={month}/"
            self.glue.create_partition(
                DatabaseName=self.database_name,
                TableName=table_name,
                PartitionInput={
                    'Values': [account_id, str(year), str(month)],
                    'StorageDescriptor': {
                        'Columns': parent_sd['Columns'],
                        'Location': location,
                        'InputFormat': parent_sd['InputFormat'],
                        'OutputFormat': parent_sd['OutputFormat'],
                        'SerdeInfo': parent_sd['SerdeInfo']
                    }
                }
            )
            logger.info(f"Added partition for {account_id}/{year}/{month}")
        except Exception as e:
            logger.error(f"Failed to add partition: {e}")
            raise

    def _cleanup_daily_partition(self, year: int, month: int, day: int, account_ids: List[str]) -> Dict[str, int]:
        files_deleted = 0
        for account_id in account_ids:
            prefix = f"tag-costs-daily/account_id={account_id}/year={year}/month={month}/day={day}/"
            try:
                paginator = self.s3.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix):
                    objects = page.get('Contents', [])
                    if objects:
                        self.s3.delete_objects(Bucket=self.s3_bucket,
                                               Delete={'Objects': [{'Key': o['Key']} for o in objects]})
                        files_deleted += len(objects)
            except Exception as e:
                logger.error(f"Error cleaning up partition for {account_id}: {e}")
        return {'files_deleted': files_deleted}

    def _publish_metric(self, metric_name: str, value: float):
        try:
            self.cloudwatch.put_metric_data(
                Namespace='TagInsights/CostAnalysis',
                MetricData=[{'MetricName': metric_name, 'Value': value, 'Unit': 'Count', 'Timestamp': datetime.utcnow()}]
            )
        except Exception as e:
            logger.warning(f"Failed to publish metric {metric_name}: {e}")


def lambda_handler(event, context):
    """
    Lambda handler for cost deduplicator.
    Triggered after SQS queue is empty to deduplicate daily cost partitions into monthly.
    """
    logger.info("Cost Deduplicator started")
    logger.info(f"Event: {json.dumps(event)}")

    try:
        deduplicator = CostDeduplicator()

        process_date = None
        if 'process_date' in event:
            try:
                process_date = datetime.strptime(event['process_date'], '%Y-%m-%d')
            except ValueError as e:
                logger.warning(f"Invalid process_date format: {e}. Using default (yesterday)")

        stats = deduplicator.deduplicate_daily_partitions(process_date)

        return {
            'statusCode': 200 if stats['status'] == 'success' else 207,
            'body': json.dumps(stats)
        }

    except Exception as e:
        logger.error(f"Deduplicator execution failed: {e}", exc_info=True)
        try:
            boto3.client('cloudwatch').put_metric_data(
                Namespace='TagInsights/CostAnalysis',
                MetricData=[{'MetricName': 'DeduplicatorErrors', 'Value': 1, 'Unit': 'Count', 'Timestamp': datetime.utcnow()}]
            )
        except Exception:
            pass
        return {
            'statusCode': 500,
            'body': json.dumps({'status': 'error', 'error': str(e), 'timestamp': datetime.utcnow().isoformat()})
        }

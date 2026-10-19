#!/usr/bin/env python3
"""
Parquet Writer for Tag Costs

Writes normalized tag cost data to S3 in Parquet format with partitioning
by account_id, year, and month.

Features:
- Writes to daily partitions during processing (allows duplicates)
- Partitioned by account_id/year/month/day for cleanup
- Automatic schema inference from data
- Batch writing for efficiency
- Compression (Snappy by default)
"""

import os
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Dict, List, Any, Optional
from datetime import datetime
import io
import json

# Graceful fallback for Lambda environments without loguru
try:
    from loguru import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class ParquetWriter:
    """
    Write tag cost data to S3 in Parquet format with partitioning
    """
    
    def __init__(self, bucket: str, prefix: str = 'tag-costs-daily', region: str = 'us-east-1'):
        """
        Initialize Parquet writer
        
        Args:
            bucket: S3 bucket name
            prefix: S3 prefix for daily partition data
            region: AWS region
        """
        self.bucket = bucket
        self.prefix = prefix
        self.region = region
        
        # Initialize S3 client
        self.s3 = boto3.client('s3', region_name=region)
        
        logger.info(f"ParquetWriter initialized: s3://{bucket}/{prefix}")
    
    def write_tag_costs(self, records: List[Dict[str, Any]], 
                       account_id: str,
                       partition_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Write normalized tag cost records to S3 daily partition
        
        Args:
            records: List of normalized tag cost records
            account_id: AWS account ID for partitioning
            partition_date: Date for partitioning (defaults to today)
            
        Returns:
            Dict: Write statistics
        """
        if not records:
            logger.warning("No records to write")
            return {'records_written': 0, 'bytes_written': 0}
        
        # Use current date if not specified
        if partition_date is None:
            partition_date = datetime.utcnow()
        
        # Extract partition components
        year = partition_date.year
        month = partition_date.month
        day = partition_date.day
        
        logger.info(f"Writing {len(records)} tag cost records for account {account_id} "
                   f"to partition {year}/{month}/{day}")
        
        try:
            # Convert records to PyArrow table
            table = self._records_to_table(records)
            
            # Generate S3 key with partitioning
            # Format: tag-costs-daily/account_id=123/year=2024/month=10/day=21/data_<timestamp>.parquet
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')
            s3_key = (f"{self.prefix}/"
                     f"account_id={account_id}/"
                     f"year={year}/"
                     f"month={month}/"
                     f"day={day}/"
                     f"data_{timestamp}.parquet")
            
            # Write to S3
            bytes_written = self._write_table_to_s3(table, s3_key)
            
            stats = {
                'records_written': len(records),
                'bytes_written': bytes_written,
                's3_path': f"s3://{self.bucket}/{s3_key}",
                'partition': {
                    'account_id': account_id,
                    'year': year,
                    'month': month,
                    'day': day
                },
                'timestamp': datetime.utcnow().isoformat()
            }
            
            logger.info(f"Successfully wrote {len(records)} records ({bytes_written:,} bytes) "
                       f"to {stats['s3_path']}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error writing tag cost records to S3: {e}")
            raise
    
    def _records_to_table(self, records: List[Dict[str, Any]]) -> pa.Table:
        """
        Convert list of dictionaries to PyArrow table
        
        Args:
            records: List of tag cost records
            
        Returns:
            pa.Table: PyArrow table
        """
        # Define schema for tag_costs table
        schema = pa.schema([
            ('tag_key', pa.string()),
            ('tag_value', pa.string()),
            ('is_cost_allocation_tag', pa.bool_()),
            ('resource_arn', pa.string()),
            ('resource_type', pa.string()),
            ('region', pa.string()),
            ('last_month_cost', pa.float64()),
            ('current_month_mtd', pa.float64()),
            ('current_month_projected', pa.float64()),
            ('avg_daily_cost', pa.float64()),
            ('analysis_date', pa.date32()),
            ('last_updated', pa.timestamp('ms'))
        ])
        
        # Extract columns from records
        columns = {
            'tag_key': [],
            'tag_value': [],
            'is_cost_allocation_tag': [],
            'resource_arn': [],
            'resource_type': [],
            'region': [],
            'last_month_cost': [],
            'current_month_mtd': [],
            'current_month_projected': [],
            'avg_daily_cost': [],
            'analysis_date': [],
            'last_updated': []
        }
        
        for record in records:
            columns['tag_key'].append(record.get('tag_key'))
            columns['tag_value'].append(record.get('tag_value'))
            columns['is_cost_allocation_tag'].append(record.get('is_cost_allocation_tag', False))
            columns['resource_arn'].append(record.get('resource_arn'))
            columns['resource_type'].append(record.get('resource_type'))
            columns['region'].append(record.get('region'))
            columns['last_month_cost'].append(record.get('last_month_cost', 0.0))
            columns['current_month_mtd'].append(record.get('current_month_mtd', 0.0))
            columns['current_month_projected'].append(record.get('current_month_projected', 0.0))
            columns['avg_daily_cost'].append(record.get('avg_daily_cost', 0.0))
            
            # Convert analysis_date string to date
            analysis_date = record.get('analysis_date')
            if isinstance(analysis_date, str):
                analysis_date = datetime.fromisoformat(analysis_date).date()
            elif isinstance(analysis_date, datetime):
                analysis_date = analysis_date.date()
            columns['analysis_date'].append(analysis_date)
            
            # Convert last_updated string to timestamp
            last_updated = record.get('last_updated')
            if isinstance(last_updated, str):
                last_updated = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
            elif not isinstance(last_updated, datetime):
                last_updated = datetime.utcnow()
            columns['last_updated'].append(last_updated)
        
        # Create PyArrow arrays
        arrays = [
            pa.array(columns['tag_key'], type=pa.string()),
            pa.array(columns['tag_value'], type=pa.string()),
            pa.array(columns['is_cost_allocation_tag'], type=pa.bool_()),
            pa.array(columns['resource_arn'], type=pa.string()),
            pa.array(columns['resource_type'], type=pa.string()),
            pa.array(columns['region'], type=pa.string()),
            pa.array(columns['last_month_cost'], type=pa.float64()),
            pa.array(columns['current_month_mtd'], type=pa.float64()),
            pa.array(columns['current_month_projected'], type=pa.float64()),
            pa.array(columns['avg_daily_cost'], type=pa.float64()),
            pa.array(columns['analysis_date'], type=pa.date32()),
            pa.array(columns['last_updated'], type=pa.timestamp('ms'))
        ]
        
        # Create table with schema
        table = pa.Table.from_arrays(arrays, schema=schema)
        
        logger.debug(f"Created PyArrow table with {table.num_rows} rows, {table.num_columns} columns")
        
        return table
    
    def _write_table_to_s3(self, table: pa.Table, s3_key: str) -> int:
        """
        Write PyArrow table to S3 as Parquet
        
        Args:
            table: PyArrow table
            s3_key: S3 key for the file
            
        Returns:
            int: Number of bytes written
        """
        # Write table to in-memory buffer
        buffer = io.BytesIO()
        
        # Write Parquet with Snappy compression
        pq.write_table(
            table,
            buffer,
            compression='snappy',
            use_dictionary=True,  # Dictionary encoding for repeated values
            write_statistics=True  # Include column statistics
        )
        
        # Get buffer size
        buffer.seek(0)
        bytes_written = len(buffer.getvalue())
        
        # Upload to S3
        buffer.seek(0)
        self.s3.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        
        logger.debug(f"Uploaded {bytes_written:,} bytes to s3://{self.bucket}/{s3_key}")
        
        return bytes_written
    
    def write_batch(self, batches: Dict[str, List[Dict[str, Any]]], 
                   partition_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Write multiple batches of tag costs grouped by account_id
        
        Args:
            batches: Dict mapping account_id to list of records
            partition_date: Date for partitioning (defaults to today)
            
        Returns:
            Dict: Aggregate statistics
        """
        total_records = 0
        total_bytes = 0
        successful_accounts = []
        failed_accounts = []
        
        for account_id, records in batches.items():
            try:
                stats = self.write_tag_costs(records, account_id, partition_date)
                total_records += stats['records_written']
                total_bytes += stats['bytes_written']
                successful_accounts.append(account_id)
            except Exception as e:
                logger.error(f"Failed to write batch for account {account_id}: {e}")
                failed_accounts.append(account_id)
        
        return {
            'total_records_written': total_records,
            'total_bytes_written': total_bytes,
            'successful_accounts': successful_accounts,
            'failed_accounts': failed_accounts,
            'accounts_processed': len(successful_accounts),
            'accounts_failed': len(failed_accounts)
        }
    
    def get_partition_path(self, account_id: str, year: int, month: int, day: int) -> str:
        """
        Get S3 path for a specific partition
        
        Args:
            account_id: AWS account ID
            year: Year
            month: Month
            day: Day
            
        Returns:
            str: S3 path
        """
        return (f"s3://{self.bucket}/{self.prefix}/"
                f"account_id={account_id}/"
                f"year={year}/"
                f"month={month}/"
                f"day={day}/")


if __name__ == "__main__":
    # Simple test/demo
    import os
    
    print("🧪 Testing Parquet Writer...")
    
    # Test configuration (requires actual S3 bucket)
    bucket = os.environ.get('S3_BUCKET', 'ti-central-us-east-1')
    
    try:
        writer = ParquetWriter(bucket, prefix='tag-costs-daily-test')
        
        # Create sample records
        sample_records = [
            {
                'tag_key': 'Environment',
                'tag_value': 'production',
                'is_cost_allocation_tag': True,
                'resource_arn': 'arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0',
                'resource_type': 'AWS::EC2::Instance',
                'region': 'us-east-1',
                'last_month_cost': 150.50,
                'current_month_mtd': 75.25,
                'current_month_projected': 151.00,
                'avg_daily_cost': 5.02,
                'analysis_date': datetime.utcnow().date(),
                'last_updated': datetime.utcnow()
            },
            {
                'tag_key': 'Project',
                'tag_value': 'web-app',
                'is_cost_allocation_tag': False,
                'resource_arn': 'arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0',
                'resource_type': 'AWS::EC2::Instance',
                'region': 'us-east-1',
                'last_month_cost': 150.50,
                'current_month_mtd': 75.25,
                'current_month_projected': 151.00,
                'avg_daily_cost': 5.02,
                'analysis_date': datetime.utcnow().date(),
                'last_updated': datetime.utcnow()
            }
        ]
        
        print(f"Writing {len(sample_records)} sample records...")
        stats = writer.write_tag_costs(sample_records, account_id='123456789012')
        
        print(f"✅ Write successful:")
        print(f"   Records written: {stats['records_written']}")
        print(f"   Bytes written: {stats['bytes_written']:,}")
        print(f"   S3 path: {stats['s3_path']}")
    
    except Exception as e:
        print(f"❌ Test failed: {e}")
    
    print("🚀 Parquet Writer test completed")

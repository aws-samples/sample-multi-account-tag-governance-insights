#!/usr/bin/env python3
"""
S3 Cost Data Writer - Partitioned Parquet storage for scalable cost analysis

Writes individual resource cost data to S3 in partitioned Parquet format
for efficient querying with Athena while keeping storage costs minimal.

Features:
- Partitioned by year/month/service for efficient querying
- Parquet format for compression and columnar performance
- Batch writing for optimal performance
- Automatic partition management
- Integration with existing cost analysis workflows

Storage Structure:
s3://ti-cost-data-{region}-{env}/
├── year=2025/month=10/service=ec2/resource_costs_20251002.parquet
├── year=2025/month=10/service=s3/resource_costs_20251002.parquet
└── _partitions_metadata/partitions.json
"""

import os
import boto3
import json
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import tempfile

# Graceful fallback for Lambda environments without loguru
try:
    from loguru import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info("Using standard logging (loguru not available)")
from pathlib import Path

try:
    from ..utils.resource_naming import get_bucket_name
except ImportError:
    # Fallback for development/testing
    def get_bucket_name(bucket_type: str, region: str, environment: str, include_account_id: bool = True) -> str:
        if include_account_id:
            import boto3
            try:
                account_id = boto3.client('sts').get_caller_identity()['Account']
                return f"ti-{bucket_type}-{account_id}-{region}-{environment}"
            except Exception:
                return f"ti-{bucket_type}-{region}-{environment}"
        return f"ti-{bucket_type}-{region}-{environment}"


class S3CostWriter:
    """
    Writes resource cost data to partitioned S3 storage for Athena analysis
    
    Handles batched writing of cost data with automatic partitioning by
    year/month/service for optimal query performance and cost efficiency.
    """
    
    def __init__(self, region_name: str = 'us-east-1'):
        """
        Initialize S3 cost writer
        
        Args:
            region_name: AWS region for S3 bucket
        """
        self.region = region_name
        self.environment = os.environ.get('ENVIRONMENT', 'dev')
        
        # Initialize AWS clients
        self.s3_client = boto3.client('s3', region_name=region_name)
        
        # Get bucket name
        self.bucket_name = get_bucket_name('cost-data', region_name, self.environment)
        
        # Configuration
        self.enable_tag_costs_table = os.environ.get('ENABLE_TAG_COSTS_TABLE', 'true').lower() == 'true'
        
        # Check if bucket exists, create if needed
        self._ensure_bucket_exists()
        
        logger.info(f"S3CostWriter initialized for bucket: {self.bucket_name}")
        logger.info(f"Tag costs table enabled: {self.enable_tag_costs_table}")
    
    def write_cost_data(self, resource_costs: List[Dict], analysis_date: Optional[str] = None) -> Dict:
        """
        Write resource cost data to partitioned S3 storage
        
        Args:
            resource_costs: List of resource cost dictionaries
            analysis_date: Optional analysis date (defaults to current date)
            
        Returns:
            Dict: Write operation results including files created and partition info
        """
        if not resource_costs:
            logger.warning("No resource cost data provided to write")
            return {
                'status': 'skipped',
                'files_written': 0,
                'resources_written': 0,
                'message': 'No cost data to write'
            }
        
        analysis_date = analysis_date or datetime.now(timezone.utc).strftime('%Y%m%d')
        analysis_datetime = datetime.now(timezone.utc)
        
        logger.info(f"Writing {len(resource_costs)} resource costs to S3 (analysis date: {analysis_date})")
        
        try:
            # Group resources by service for partitioning
            service_groups = self._group_by_service(resource_costs)
            
            files_written = []
            total_resources = 0
            
            # Write each service group to its own partition
            for service_type, service_resources in service_groups.items():
                partition_info = self._write_service_partition(
                    service_resources, 
                    service_type, 
                    analysis_date,
                    analysis_datetime
                )
                
                if partition_info['status'] == 'success':
                    files_written.append(partition_info)
                    total_resources += len(service_resources)
                    
                    logger.debug(f"Wrote {len(service_resources)} {service_type} resources to partition")
                else:
                    logger.error(f"Failed to write {service_type} partition: {partition_info.get('error')}")
            
            # Update partition metadata
            self._update_partition_metadata(analysis_date, service_groups.keys())
            
            # Write normalized tag costs data if enabled
            tag_costs_result = {}
            if self.enable_tag_costs_table:
                tag_costs_result = self.write_normalized_tag_costs(resource_costs, analysis_date, analysis_datetime)
                logger.info(f"Tag costs table write result: {tag_costs_result.get('status', 'unknown')}")
            
            result = {
                'status': 'success',
                'files_written': len(files_written),
                'resources_written': total_resources,
                'analysis_date': analysis_date,
                'partitions_created': list(service_groups.keys()),
                'bucket': self.bucket_name,
                'file_details': files_written,
                'tag_costs_result': tag_costs_result
            }
            
            logger.info(f"Successfully wrote {total_resources} resource costs across {len(files_written)} partitions")
            
            return result
            
        except Exception as e:
            logger.error(f"Error writing cost data to S3: {e}")
            return {
                'status': 'error',
                'files_written': 0,
                'resources_written': 0,
                'error': str(e)
            }
    
    def _group_by_service(self, resource_costs: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Group resource costs by service type for partitioning
        
        Args:
            resource_costs: List of resource cost dictionaries
            
        Returns:
            Dict[str, List[Dict]]: Resources grouped by service type
        """
        service_groups = {}
        
        for resource in resource_costs:
            service_type = resource.get('service_type', 'unknown')
            
            if service_type not in service_groups:
                service_groups[service_type] = []
            
            service_groups[service_type].append(resource)
        
        logger.debug(f"Grouped {len(resource_costs)} resources into {len(service_groups)} service partitions")
        
        return service_groups
    
    def _write_service_partition(self, service_resources: List[Dict], service_type: str, 
                               analysis_date: str, analysis_datetime: datetime) -> Dict:
        """
        Write a single service partition to S3
        
        Args:
            service_resources: Resources for this service
            service_type: AWS service type
            analysis_date: Analysis date string (YYYYMMDD)
            analysis_datetime: Analysis datetime object
            
        Returns:
            Dict: Write operation result
        """
        try:
            # Extract year and month for partitioning
            year = analysis_datetime.year
            month = analysis_datetime.month
            
            # Create S3 key for this partition with resource-costs/ prefix
            s3_key = f"resource-costs/year={year}/month={month}/service={service_type}/resource_costs_{analysis_date}.parquet"
            
            # Prepare data for Parquet
            parquet_data = self._prepare_parquet_data(service_resources, analysis_datetime)
            
            # Convert to DataFrame and write to Parquet
            df = pd.DataFrame(parquet_data)
            
            # Write to temporary file first
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                df.to_parquet(tmp_file.name, index=False, compression='snappy')
                
                # Upload to S3
                self.s3_client.upload_file(
                    tmp_file.name,
                    self.bucket_name,
                    s3_key,
                    ExtraArgs={
                        'ContentType': 'application/octet-stream',
                        'Metadata': {
                            'analysis-date': analysis_date,
                            'service-type': service_type,
                            'resource-count': str(len(service_resources)),
                            'partition': f"year={year}/month={month}/service={service_type}"
                        }
                    }
                )
                
                # Clean up temp file
                os.unlink(tmp_file.name)
            
            file_size = self._get_s3_object_size(s3_key)
            
            return {
                'status': 'success',
                'service_type': service_type,
                's3_key': s3_key,
                'resource_count': len(service_resources),
                'file_size_bytes': file_size,
                'partition': f"year={year}/month={month}/service={service_type}"
            }
            
        except Exception as e:
            logger.error(f"Error writing {service_type} partition: {e}")
            return {
                'status': 'error',
                'service_type': service_type,
                'error': str(e)
            }
    
    def _prepare_parquet_data(self, resources: List[Dict], analysis_datetime: datetime) -> List[Dict]:
        """
        Prepare resource data for Parquet format
        
        Args:
            resources: List of resource cost dictionaries
            analysis_datetime: Analysis datetime
            
        Returns:
            List[Dict]: Data formatted for Parquet storage
        """
        parquet_rows = []
        
        for resource in resources:
            # Extract cost allocation tags for has_cost_tags field
            tags = resource.get('tags', {})
            has_cost_tags = self._has_cost_allocation_tags(tags)
            
            # Determine if resource is dormant (no costs)
            current_cost = resource.get('current_month_projected', 0) or resource.get('monthly_cost', 0)
            is_dormant = current_cost == 0
            
            # Calculate last_used - max of CUR activity or analytics discovery
            last_used = self._calculate_last_used(resource, analysis_datetime)
            
            parquet_row = {
                'resource_arn': resource.get('resource_arn', ''),
                'last_updated': analysis_datetime.isoformat(),
                'last_used': last_used,
                'tags': json.dumps(tags),  # Store as JSON string for Athena compatibility
                'last_month_cost': float(resource.get('last_month_cost', 0)),
                'current_month_mtd': float(resource.get('current_month_mtd', 0)),
                'current_month_projected': float(resource.get('current_month_projected', 0) or resource.get('monthly_cost', 0)),
                'avg_daily_cost': float(resource.get('avg_daily_cost', 0)),
                'service_type': resource.get('service_type', 'unknown'),
                'region': resource.get('region', 'unknown'),
                'has_cost_tags': has_cost_tags,
                'is_dormant': is_dormant,
                'analysis_date': analysis_datetime.strftime('%Y-%m-%d')
            }
            
            parquet_rows.append(parquet_row)
        
        return parquet_rows
    
    def _has_cost_allocation_tags(self, tags: Dict) -> bool:
        """
        Check if resource has cost allocation tags
        
        Args:
            tags: Resource tags dictionary
            
        Returns:
            bool: True if resource has active cost allocation tags
        """
        # Common cost allocation tag keys
        common_cost_tags = [
            'Environment', 'Project', 'Team', 'Owner', 'Application',
            'CostCenter', 'Department', 'BusinessUnit', 'Product'
        ]
        
        return any(tag_key in tags for tag_key in common_cost_tags)
    
    def _calculate_last_used(self, resource: Dict, analysis_datetime: datetime) -> str:
        """
        Calculate last used timestamp from CUR and analytics data
        
        Args:
            resource: Resource dictionary
            analysis_datetime: Current analysis datetime
            
        Returns:
            str: ISO timestamp of last usage
        """
        # Try to get last usage from CUR data
        cur_last_used = resource.get('cur_last_activity')
        
        # Try to get discovery date from analytics
        analytics_last_updated = resource.get('discovered_date') or resource.get('last_updated')
        
        # Use the more recent timestamp
        timestamps = []
        
        if cur_last_used:
            try:
                if isinstance(cur_last_used, str):
                    timestamps.append(datetime.fromisoformat(cur_last_used.replace('Z', '+00:00')))
                else:
                    timestamps.append(cur_last_used)
            except Exception:
                pass
        
        if analytics_last_updated:
            try:
                if isinstance(analytics_last_updated, str):
                    timestamps.append(datetime.fromisoformat(analytics_last_updated.replace('Z', '+00:00')))
                else:
                    timestamps.append(analytics_last_updated)
            except Exception:
                pass
        
        if timestamps:
            latest_timestamp = max(timestamps)
            return latest_timestamp.isoformat()
        else:
            # Fallback to analysis time
            return analysis_datetime.isoformat()
    
    def _update_partition_metadata(self, analysis_date: str, service_types: List[str]):
        """
        Update partition metadata for Athena table management
        
        Args:
            analysis_date: Analysis date string
            service_types: List of service types written
        """
        try:
            metadata_key = "_partitions_metadata/partitions.json"
            
            # Try to get existing metadata
            try:
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=metadata_key)
                existing_metadata = json.loads(response['Body'].read().decode('utf-8'))
            except self.s3_client.exceptions.NoSuchKey:
                existing_metadata = {
                    'partitions': [],
                    'last_updated': None
                }
            
            # Extract year and month
            analysis_datetime = datetime.strptime(analysis_date, '%Y%m%d')
            year = analysis_datetime.year
            month = analysis_datetime.month
            
            # Add new partitions
            for service_type in service_types:
                partition_info = {
                    'year': year,
                    'month': month,
                    'service': service_type,
                    'analysis_date': analysis_date,
                    'created_timestamp': datetime.now(timezone.utc).isoformat()
                }
                
                # Check if partition already exists
                partition_exists = any(
                    p['year'] == year and p['month'] == month and p['service'] == service_type
                    for p in existing_metadata['partitions']
                )
                
                if not partition_exists:
                    existing_metadata['partitions'].append(partition_info)
            
            # Update metadata
            existing_metadata['last_updated'] = datetime.now(timezone.utc).isoformat()
            
            # Write back to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=metadata_key,
                Body=json.dumps(existing_metadata, indent=2),
                ContentType='application/json'
            )
            
            logger.debug(f"Updated partition metadata with {len(service_types)} service partitions")
            
        except Exception as e:
            logger.warning(f"Failed to update partition metadata: {e}")
    
    def _ensure_bucket_exists(self):
        """Ensure S3 bucket exists for cost data storage"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.debug(f"S3 bucket {self.bucket_name} exists")
        except self.s3_client.exceptions.NoSuchBucket:
            logger.warning(f"S3 bucket {self.bucket_name} does not exist - will be created by CloudFormation")
        except Exception as e:
            logger.error(f"Error checking S3 bucket {self.bucket_name}: {e}")
    
    def _get_s3_object_size(self, s3_key: str) -> int:
        """Get size of S3 object in bytes"""
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return response['ContentLength']
        except Exception:
            return 0
    
    def write_normalized_tag_costs(self, resource_costs: List[Dict], analysis_date: str, 
                                 analysis_datetime: datetime) -> Dict:
        """
        Write normalized tag costs data to partitioned S3 storage for BI analysis
        
        Converts resource costs into normalized tag rows where each tag key-value pair
        becomes a separate record with associated cost data. Optimized for BI tools
        and tag-based cost analysis.
        
        Args:
            resource_costs: List of resource cost dictionaries
            analysis_date: Analysis date string (YYYYMMDD)
            analysis_datetime: Analysis datetime object
            
        Returns:
            Dict: Write operation result
        """
        if not resource_costs:
            logger.warning("No resource cost data provided for tag normalization")
            return {
                'status': 'skipped',
                'message': 'No resource cost data to normalize'
            }
        
        logger.info(f"Writing normalized tag costs for {len(resource_costs)} resources")
        
        try:
            # Extract year and month for partitioning
            year = analysis_datetime.year
            month = analysis_datetime.month
            
            # Normalize tags into individual records
            normalized_records = self._normalize_resource_tags_to_records(
                resource_costs, analysis_datetime
            )
            
            if not normalized_records:
                logger.warning("No tag data to normalize from resources")
                return {
                    'status': 'skipped',
                    'message': 'No tags found in resource data'
                }
            
            # Create S3 key for tag costs partition
            s3_key = f"tag-costs/year={year}/month={month}/tag_costs_{analysis_date}.parquet"
            
            # Convert to DataFrame and write to Parquet
            df = pd.DataFrame(normalized_records)
            
            # Write to temporary file first
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                df.to_parquet(tmp_file.name, index=False, compression='snappy')
                
                # Upload to S3
                self.s3_client.upload_file(
                    tmp_file.name,
                    self.bucket_name,
                    s3_key,
                    ExtraArgs={
                        'ContentType': 'application/octet-stream',
                        'Metadata': {
                            'analysis-date': analysis_date,
                            'record-count': str(len(normalized_records)),
                            'partition': f"year={year}/month={month}",
                            'data-type': 'normalized_tag_costs'
                        }
                    }
                )
                
                # Clean up temp file
                os.unlink(tmp_file.name)
            
            file_size = self._get_s3_object_size(s3_key)
            
            logger.info(f"Successfully wrote {len(normalized_records)} normalized tag records to S3")
            
            return {
                'status': 'success',
                's3_key': s3_key,
                'records_written': len(normalized_records),
                'file_size_bytes': file_size,
                'partition': f"year={year}/month={month}",
                'unique_resources': len(resource_costs)
            }
            
        except Exception as e:
            logger.error(f"Error writing normalized tag costs: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'records_attempted': len(resource_costs) if resource_costs else 0
            }
    
    def _normalize_resource_tags_to_records(self, resources: List[Dict], 
                                          analysis_datetime: datetime) -> List[Dict]:
        """
        Convert resource cost data into normalized tag records
        
        Each resource with tags becomes multiple records - one for each tag key-value pair.
        Resources without tags are excluded from the normalized data.
        
        Args:
            resources: List of resource cost dictionaries
            analysis_datetime: Analysis datetime
            
        Returns:
            List[Dict]: Normalized tag cost records
        """
        normalized_records = []
        
        for resource in resources:
            # Get resource tags
            tags = resource.get('tags', {})
            
            # Handle tags as JSON string (common case when reading from Parquet files)
            if isinstance(tags, str):
                try:
                    # First JSON decode
                    tags = json.loads(tags) if tags else {}
                    
                    # If result is still a string, try second decode (double-encoded case)
                    if isinstance(tags, str):
                        try:
                            tags = json.loads(tags) if tags else {}
                        except json.JSONDecodeError:
                            logger.debug(f"Second JSON decode failed for {resource.get('resource_arn', 'unknown')}")
                            tags = {}
                            
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in tags for {resource.get('resource_arn', 'unknown')}: {tags}")
                    tags = {}
            
            if not tags or not isinstance(tags, dict):
                continue  # Skip resources without valid tags
            
            # Extract account ID from ARN
            resource_arn = resource.get('resource_arn', '')
            account_id = 'unknown'
            if resource_arn and resource_arn.startswith('arn:aws:'):
                try:
                    account_id = resource_arn.split(':')[4]
                except IndexError:
                    pass
            
            # Get cost data
            last_month_cost = float(resource.get('last_month_cost', 0))
            current_month_mtd = float(resource.get('current_month_mtd', 0))
            current_month_projected = float(resource.get('current_month_projected', 0) or 
                                          resource.get('monthly_cost', 0))
            avg_daily_cost = float(resource.get('avg_daily_cost', 0))
            
            # Determine if resource is dormant
            is_dormant = current_month_projected == 0
            
            # Calculate last_used timestamp
            last_used = self._calculate_last_used(resource, analysis_datetime)
            
            # Create one record for each tag key-value pair
            for tag_key, tag_value in tags.items():
                # Skip empty or None values
                if tag_value is None:
                    tag_value = ''
                
                normalized_record = {
                    'resource_arn': resource_arn,
                    'account_id': account_id,
                    'org_id': '',  # Future use - placeholder for now
                    'tag_key': str(tag_key),
                    'tag_value': str(tag_value),
                    'last_updated': analysis_datetime.isoformat(),
                    'last_used': last_used,
                    'last_month_cost': last_month_cost,
                    'current_month_mtd': current_month_mtd,
                    'current_month_projected': current_month_projected,
                    'avg_daily_cost': avg_daily_cost,
                    'service_type': resource.get('service_type', 'unknown'),
                    'region': resource.get('region', 'unknown'),
                    'is_dormant': is_dormant,
                    'analysis_date': analysis_datetime.strftime('%Y-%m-%d')
                }
                
                normalized_records.append(normalized_record)
        
        logger.debug(f"Normalized {len(resources)} resources into {len(normalized_records)} tag records")
        
        return normalized_records
    
    def get_partition_info(self, year: Optional[int] = None, month: Optional[int] = None) -> Dict:
        """
        Get information about existing partitions
        
        Args:
            year: Optional year filter
            month: Optional month filter
            
        Returns:
            Dict: Partition information
        """
        try:
            metadata_key = "_partitions_metadata/partitions.json"
            
            try:
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=metadata_key)
                metadata = json.loads(response['Body'].read().decode('utf-8'))
            except self.s3_client.exceptions.NoSuchKey:
                return {
                    'partitions': [],
                    'total_partitions': 0,
                    'message': 'No partition metadata found'
                }
            
            partitions = metadata.get('partitions', [])
            
            # Apply filters
            if year is not None:
                partitions = [p for p in partitions if p.get('year') == year]
            if month is not None:
                partitions = [p for p in partitions if p.get('month') == month]
            
            return {
                'partitions': partitions,
                'total_partitions': len(partitions),
                'last_updated': metadata.get('last_updated'),
                'bucket': self.bucket_name
            }
            
        except Exception as e:
            logger.error(f"Error getting partition info: {e}")
            return {
                'partitions': [],
                'total_partitions': 0,
                'error': str(e)
            }


if __name__ == "__main__":
    # Simple test/demo
    writer = S3CostWriter()
    
    print("🧪 Testing S3 Cost Writer...")
    
    # Test partition info
    try:
        partition_info = writer.get_partition_info()
        print(f"✅ Partition info retrieved: {partition_info['total_partitions']} partitions")
        
        # Test sample data write (small test)
        sample_data = [
            {
                'resource_arn': 'arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0',
                'service_type': 'ec2',
                'region': 'us-east-1',
                'monthly_cost': 45.67,
                'tags': {'Environment': 'test', 'Project': 'demo'},
                'discovered_date': datetime.now(timezone.utc).isoformat()
            }
        ]
        
        result = writer.write_cost_data(sample_data)
        if result['status'] == 'success':
            print(f"✅ Test write successful: {result['resources_written']} resources")
        else:
            print(f"❌ Test write failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
    
    print("🚀 S3 Cost Writer test completed")

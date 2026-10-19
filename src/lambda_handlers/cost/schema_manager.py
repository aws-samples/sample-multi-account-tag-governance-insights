#!/usr/bin/env python3
"""
CUR Schema Manager - Dynamic schema discovery and validation for CUR tables

Handles the challenge of CUR tables having variable tag columns that depend on:
- Which tags were used in each billing period
- Time period queried (tags may appear/disappear over time)

Features:
- Auto-discovery of CUR tables in Glue catalog
- Dynamic tag column detection with caching
- Schema validation and accessibility testing
- Handles missing columns gracefully
"""

import os
import boto3
from typing import Dict, List, Optional, Set
from datetime import datetime

# Graceful fallback for Lambda environments without loguru
try:
    from loguru import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info("Using standard logging (loguru not available)")
from botocore.exceptions import ClientError


def _get_cur_session() -> boto3.Session:
    """Return a same-account or assumed-role session for CUR resources."""
    role_arn = os.environ.get('CUR_ACCOUNT_ROLE_ARN', '').strip()
    if not role_arn:
        return boto3.Session()

    try:
        external_id = os.environ.get('CUR_EXTERNAL_ID', '').strip()
        if not external_id:
            raise RuntimeError(
                "CUR_EXTERNAL_ID is required when CUR_ACCOUNT_ROLE_ARN is configured"
            )
        response = boto3.client('sts').assume_role(
            RoleArn=role_arn,
            RoleSessionName='TagInsightsCURAccess',
            ExternalId=external_id,
            DurationSeconds=3600,
        )
        credentials = response['Credentials']
        logger.info(f"Assumed cross-account CUR role: {role_arn}")
        return boto3.Session(
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'],
        )
    except Exception as exc:
        logger.error(
            f"Failed to assume configured CUR role '{role_arn}': {exc}"
        )
        raise RuntimeError(
            f"Failed to assume configured CUR role '{role_arn}': {exc}"
        ) from exc


class CURSchemaManager:
    """
    Manages dynamic CUR schema discovery and caching
    
    CUR tables have dynamic schemas where tag columns (resource_tags_user_*)
    only exist when those tags were used in that billing period.
    """
    
    # Standard CUR schema - these columns should always be present in a valid CUR table
    REQUIRED_CUR_COLUMNS = {
        # Identity columns
        'identity_line_item_id',
        'identity_time_interval',
        
        # Bill columns
        'bill_invoice_id',
        'bill_invoicing_entity',
        'bill_billing_entity',
        'bill_bill_type',
        'bill_payer_account_id',
        'bill_billing_period_start_date',
        'bill_billing_period_end_date',
        
        # Line item columns (core set - most critical)
        'line_item_usage_account_id',
        'line_item_line_item_type',
        'line_item_usage_start_date',
        'line_item_usage_end_date',
        'line_item_product_code',
        'line_item_usage_type',
        'line_item_operation',
        'line_item_availability_zone',
        'line_item_resource_id',
        'line_item_usage_amount',
        'line_item_normalization_factor',
        'line_item_normalized_usage_amount',
        'line_item_currency_code',
        'line_item_unblended_rate',
        'line_item_unblended_cost',
        'line_item_blended_rate',
        'line_item_blended_cost',
        'line_item_line_item_description',
        'line_item_tax_type',
        'line_item_legal_entity',
        
        # Product columns (essential subset)
        'product_product_name',
        'product_servicename',
        'product_product_family',
        'product_region',
        'product_servicecode',
        'product_sku',
        
        # Pricing columns
        'pricing_rate_code',
        'pricing_rate_id',
        'pricing_currency',
        'pricing_public_on_demand_cost',
        'pricing_public_on_demand_rate',
        'pricing_term',
        'pricing_unit'
    }
    
    # Minimum required columns for basic CUR validation (legacy compatibility)
    MINIMUM_CUR_COLUMNS = {
        'line_item_blended_cost',
        'line_item_resource_id', 
        'line_item_usage_start_date',
        'product_servicename',
        'bill_payer_account_id'
    }

    RESOURCE_LEVEL_REQUIRED_COLUMNS = {
        'line_item_resource_id',
        'line_item_usage_start_date',
        'line_item_usage_account_id',
        'line_item_line_item_type',
    }

    NATIVE_CUR2_SERVICE_NAME_EXPRESSION = (
        "element_at(product, 'servicename')"
    )

    COST_COLUMN_CANDIDATES_BY_FORMAT = {
        'cur1': ('line_item_blended_cost', 'line_item_unblended_cost'),
        'cur2': ('line_item_unblended_cost', 'line_item_blended_cost'),
    }

    DIRECT_COST_COLUMN_CANDIDATES_BY_FORMAT = {
        'cur1': ('line_item_unblended_cost', 'line_item_blended_cost'),
        'cur2': ('line_item_unblended_cost', 'line_item_blended_cost'),
    }

    def __init__(
        self,
        region_name: str = 'us-east-1',
        session: Optional[boto3.Session] = None,
    ):
        """
        Initialize CUR schema manager

        Supports cross-account CUR access via STS AssumeRole. When the
        ``CUR_ACCOUNT_ROLE_ARN`` environment variable is set the manager
        assumes that role (in the billing/CUR account) before creating any
        AWS clients.  If the variable is absent the manager behaves exactly
        as before, using the Lambda's own execution role.

        Args:
            region_name: AWS region for Athena and Glue clients
        """
        self.region = region_name

        self.session = session or _get_cur_session()
        self.athena_client = self.session.client('athena', region_name=region_name)
        self.glue_client = self.session.client('glue', region_name=region_name)
        self.s3_client = self.session.client('s3', region_name=region_name)
        
        # Schema cache to avoid repeated API calls
        self._schema_cache = {}
        self._cache_ttl = 3600  # 1 hour cache
        self._table_validation_cache = {}
        
        logger.debug(f"CURSchemaManager initialized for region {region_name}")
    




    def _is_cur_table(self, database: str, table_name: str, table_info: Optional[Dict] = None, 
                     strict_validation: bool = False) -> bool:
        """
        Validate that table has CUR-like schema
        
        Args:
            database: Database name
            table_name: Table name
            table_info: Optional table info from Glue (to avoid extra API call)
            strict_validation: If True, use comprehensive schema validation
            
        Returns:
            bool: True if table appears to be a CUR table
        """
        try:
            if table_info is None:
                table_info = self.glue_client.get_table(
                    DatabaseName=database,
                    Name=table_name
                )['Table']
            
            # Get column names from table schema
            storage_descriptor = table_info.get('StorageDescriptor', {})
            columns = storage_descriptor.get('Columns', [])
            column_names = set(col['Name'].lower() for col in columns)
            
            # Avoid status/metadata tables first
            avoid_patterns = ['status', 'metadata', 'ctas', 'temp', 'test', '_tmp', 'backup']
            if any(pattern in table_name.lower() for pattern in avoid_patterns):
                logger.debug(f"Table {database}.{table_name} appears to be a status/metadata table")
                return False

            if strict_validation:
                missing_required = self.REQUIRED_CUR_COLUMNS - column_names
                if len(missing_required) > len(self.REQUIRED_CUR_COLUMNS) * 0.3:
                    logger.debug(
                        f"Table {database}.{table_name} missing too many "
                        f"strict CUR columns "
                        f"({len(missing_required)}/{len(self.REQUIRED_CUR_COLUMNS)})"
                    )
                    return False
            
            profile = self.build_schema_profile(table_info)
            if not profile['supported']:
                logger.debug(
                    f"Table {database}.{table_name} does not satisfy the "
                    "resource-level CUR query contract"
                )
                return False
            
            tag_columns = [col for col in column_names if col.startswith('resource_tags_')]
            logger.debug(f"Table {database}.{table_name} has {len(tag_columns)} tag columns (optional)")
            
            logger.debug(f"✅ Table {database}.{table_name} validated as CUR table")
            logger.debug(f"   - Format: {profile['format']}")
            logger.debug(f"   - Tag mode: {profile['tag_mode']}")
            logger.debug(f"   - Partition mode: {profile['partition_mode']}")
            logger.debug(f"   - Validation mode: {'strict' if strict_validation else 'lenient'}")
            
            return True
            
        except Exception as e:
            logger.debug(f"Table validation failed for {database}.{table_name}: {e}")
            return False

    @staticmethod
    def build_schema_profile(table_info: Dict) -> Dict:
        """Classify a Glue table into the runtime CUR query contract."""
        storage_descriptor = table_info.get('StorageDescriptor', {})
        columns = storage_descriptor.get('Columns', [])
        column_names = {column['Name'] for column in columns}
        column_types = {
            column['Name']: column.get('Type', '')
            for column in columns
        }
        partition_names = {
            partition['Name']
            for partition in table_info.get('PartitionKeys', [])
        }

        if 'billing_period' in partition_names:
            partition_mode = 'billing_period'
        elif {'year', 'month'}.issubset(partition_names):
            partition_mode = 'year_month'
        else:
            partition_mode = None

        tag_mode = None
        tag_column = None
        if (
            'resource_tags' in column_names
            and column_types.get('resource_tags', '').startswith('map')
        ):
            tag_mode = 'map'
            tag_column = 'resource_tags'
        elif any(
            name.startswith('resource_tags_user_')
            for name in column_names
        ):
            tag_mode = 'flattened'

        cur_format = (
            'cur2'
            if partition_mode == 'billing_period' or tag_mode == 'map'
            else 'cur1'
        )

        cost_column = next(
            (
                candidate
                for candidate in CURSchemaManager.COST_COLUMN_CANDIDATES_BY_FORMAT[
                    cur_format
                ]
                if candidate in column_names
            ),
            None,
        )
        direct_cost_column = next(
            (
                candidate
                for candidate in CURSchemaManager.DIRECT_COST_COLUMN_CANDIDATES_BY_FORMAT[
                    cur_format
                ]
                if candidate in column_names
            ),
            None,
        )
        if 'product_servicename' in column_names:
            service_name_expression = 'product_servicename'
        elif (
            'product' in column_names
            and column_types.get('product', '').startswith('map')
        ):
            service_name_expression = (
                CURSchemaManager.NATIVE_CUR2_SERVICE_NAME_EXPRESSION
            )
        else:
            service_name_expression = None
        has_resource_level_data = (
            CURSchemaManager.RESOURCE_LEVEL_REQUIRED_COLUMNS.issubset(
                column_names
            )
        )

        return {
            'supported': bool(
                has_resource_level_data
                and cost_column is not None
                and partition_mode is not None
                and service_name_expression is not None
            ),
            'format': cur_format,
            'tag_mode': tag_mode,
            'tag_column': tag_column,
            'partition_mode': partition_mode,
            'cost_column': cost_column,
            'direct_cost_column': direct_cost_column,
            'service_name_expression': service_name_expression,
        }
    
    def validate_cur_table(self, database: str, table: str) -> bool:
        """
        Validate CUR table exists and is accessible
        
        Args:
            database: Database name
            table: Table name
            
        Returns:
            bool: True if table is valid and accessible
        """
        cache_key = f"{database}.{table}"
        
        # Check cache first
        if cache_key in self._table_validation_cache:
            cached_entry = self._table_validation_cache[cache_key]
            if datetime.now().timestamp() - cached_entry['timestamp'] < 1800:  # 30 min cache
                return cached_entry['valid']
        
        try:
            # Check if table exists and get basic info
            table_response = self.glue_client.get_table(
                DatabaseName=database,
                Name=table
            )
            
            profile = self.build_schema_profile(table_response['Table'])
            is_valid = profile['supported']
            
            # Cache the result
            self._table_validation_cache[cache_key] = {
                'valid': is_valid,
                'timestamp': datetime.now().timestamp()
            }
            
            if is_valid:
                logger.info(f"✅ CUR table validation successful: {database}.{table}")
            else:
                logger.warning(f"❌ Table validation failed: {database}.{table} is not a valid CUR table")
            
            return is_valid
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'EntityNotFoundException':
                logger.warning(f"❌ CUR table not found: {database}.{table}")
            else:
                logger.error(f"❌ Error validating CUR table {database}.{table}: {e}")
            
            # Cache negative result
            self._table_validation_cache[cache_key] = {
                'valid': False,
                'timestamp': datetime.now().timestamp()
            }
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error validating CUR table {database}.{table}: {e}")
            return False
    
    def get_available_tag_columns(
        self,
        database: str,
        table: str,
        partition_filters: Optional[List[str]] = None,
        schema_profile: Optional[Dict] = None,
    ) -> Set[str]:
        """
        Get available resource tag columns for given partitions
        
        Note: This gets ALL tag columns from the table schema. For partition-specific
        columns, we'd need to query the actual data, which is expensive.
        
        Args:
            database: Database name
            table: Table name  
            partition_filters: Optional partition filters (not used in this implementation)
            
        Returns:
            Set[str]: Set of available resource tag column names
        """
        cache_key = f"{database}.{table}:all_columns"
        
        # Check cache first
        if cache_key in self._schema_cache:
            cached_entry = self._schema_cache[cache_key]
            if datetime.now().timestamp() - cached_entry['timestamp'] < self._cache_ttl:
                return cached_entry['columns']
        
        try:
            # Get table schema
            table_info = self.glue_client.get_table(
                DatabaseName=database,
                Name=table
            )
            
            # Extract column names
            schema_profile = (
                schema_profile
                or self.build_schema_profile(table_info['Table'])
            )
            tag_columns = self.extract_available_tag_columns(
                table_info['Table'],
                schema_profile,
            )
            
            # Cache the result
            self._schema_cache[cache_key] = {
                'columns': tag_columns,
                'timestamp': datetime.now().timestamp()
            }
            
            logger.debug(f"Found {len(tag_columns)} tag columns in {database}.{table}")
            return tag_columns
            
        except Exception as e:
            logger.error(f"Error getting tag columns from {database}.{table}: {e}")
            return set()

    @staticmethod
    def extract_available_tag_columns(
        table_info: Dict,
        schema_profile: Optional[Dict] = None,
    ) -> Set[str]:
        """Extract tag columns from an already-fetched Glue table."""
        schema_profile = (
            schema_profile
            or CURSchemaManager.build_schema_profile(table_info)
        )
        column_names = {
            column['Name']
            for column in table_info.get(
                'StorageDescriptor',
                {},
            ).get('Columns', [])
        }
        if schema_profile.get('tag_mode') == 'map':
            tag_column = schema_profile.get('tag_column') or 'resource_tags'
            return {tag_column} if tag_column in column_names else set()
        return {
            name
            for name in column_names
            if name.startswith('resource_tags_user_')
        }
    
    def tag_column_exists(
        self,
        database: str,
        table: str,
        tag_key: str,
        partition_filters: Optional[List[str]] = None,
        schema_profile: Optional[Dict] = None,
    ) -> bool:
        """
        Check if specific tag column exists in given partitions
        
        Args:
            database: Database name
            table: Table name
            tag_key: Tag key to check (e.g., 'Environment')
            partition_filters: Optional partition filters (not used in this implementation)
            
        Returns:
            bool: True if tag column exists
        """
        available_columns = self.get_available_tag_columns(
            database,
            table,
            partition_filters,
            schema_profile=schema_profile,
        )
        if schema_profile and schema_profile.get('tag_mode') == 'map':
            tag_column = schema_profile.get('tag_column') or 'resource_tags'
            return tag_column in available_columns

        expected_column = f'resource_tags_user_{tag_key.lower()}'
        
        # CUR columns are typically lowercase
        return expected_column in available_columns
    
    def get_partition_info(self, database: str, table: str) -> Dict[str, List[str]]:
        """
        Get partition information for the CUR table
        
        Args:
            database: Database name
            table: Table name
            
        Returns:
            Dict: Partition information with available values
        """
        try:
            # Get table info to understand partition structure
            table_info = self.glue_client.get_table(
                DatabaseName=database,
                Name=table
            )
            
            partition_keys = table_info['Table'].get('PartitionKeys', [])
            partition_names = [pk['Name'] for pk in partition_keys]
            
            if not partition_names:
                return {'partitions': [], 'message': 'Table is not partitioned'}
            
            # Get some sample partition values (limited to avoid large responses)
            try:
                partitions_response = self.glue_client.get_partitions(
                    DatabaseName=database,
                    TableName=table,
                    MaxResults=100  # Limit to prevent large responses
                )
                
                partitions = partitions_response.get('Partitions', [])
                
                # Extract partition values
                partition_values = {}
                for partition in partitions:
                    values = partition.get('Values', [])
                    for i, value in enumerate(values):
                        if i < len(partition_names):
                            key = partition_names[i]
                            if key not in partition_values:
                                partition_values[key] = set()
                            partition_values[key].add(value)
                
                # Convert sets to sorted lists
                for key in partition_values:
                    partition_values[key] = sorted(list(partition_values[key]))
                
                return {
                    'partition_keys': partition_names,
                    'available_values': partition_values,
                    'sample_count': len(partitions)
                }
                
            except Exception as e:
                logger.debug(f"Could not get partition values for {database}.{table}: {e}")
                return {
                    'partition_keys': partition_names,
                    'available_values': {},
                    'error': 'Could not retrieve partition values'
                }
                
        except Exception as e:
            logger.error(f"Error getting partition info for {database}.{table}: {e}")
            return {'error': str(e)}
    
    def _build_current_partition_filter(self, schema_profile: Dict) -> str:
        """Return a current-month predicate for either CUR partition shape."""
        current_date = datetime.now()
        if schema_profile.get('partition_mode') == 'billing_period':
            return (
                f"billing_period = "
                f"'{current_date.year}-{current_date.month:02d}'"
            )

        month_format = os.environ.get('CUR_MONTH_FORMAT', 'no_leading_zero')
        month = (
            str(current_date.month)
            if month_format == 'no_leading_zero'
            else f"{current_date.month:02d}"
        )
        return f"year = '{current_date.year}' AND month = '{month}'"

    def test_athena_access(self, database: str, table: str) -> Dict[str, any]:
        """
        Test if Athena can access the CUR table with a simple query
        
        Args:
            database: Database name
            table: Table name
            
        Returns:
            Dict: Test results with success status and details
        """
        try:
            table_info = self.glue_client.get_table(
                DatabaseName=database,
                Name=table,
            )['Table']
            schema_profile = self.build_schema_profile(table_info)
            if not schema_profile['supported']:
                return {
                    'success': False,
                    'error': 'CUR table validation failed',
                    'database': database,
                    'table': table,
                }

            partition_filter = self._build_current_partition_filter(
                schema_profile
            )
            test_query = f"""
            SELECT COUNT(*) as record_count 
            FROM {database}.{table} 
            WHERE {partition_filter}
            LIMIT 1
            """
            
            # Get Athena results bucket
            results_bucket = (
                os.environ.get('CUR_ATHENA_RESULTS_BUCKET')
                or os.environ.get('ATHENA_RESULTS_BUCKET')
                or os.environ.get('ATHENA_OUTPUT_BUCKET')
            )
            if not results_bucket:
                account_id = self.session.client(
                    'sts',
                    region_name=self.region,
                ).get_caller_identity()['Account']
                results_bucket = f"aws-athena-query-results-{account_id}-{self.region}"
            
            # Start query execution
            response = self.athena_client.start_query_execution(
                QueryString=test_query,
                ResultConfiguration={
                    'OutputLocation': f's3://{results_bucket}/'
                }
            )
            
            query_execution_id = response['QueryExecutionId']
            logger.debug(f"Started Athena test query: {query_execution_id}")
            
            # Wait for query to complete (with timeout)
            max_wait = 30  # seconds
            wait_time = 0
            
            while wait_time < max_wait:
                execution_response = self.athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                
                status = execution_response['QueryExecution']['Status']['State']
                
                if status == 'SUCCEEDED':
                    logger.info(f"✅ Athena access test successful for {database}.{table}")
                    return {
                        'success': True,
                        'query_id': query_execution_id,
                        'message': 'Athena can successfully query CUR table'
                    }
                elif status in ['FAILED', 'CANCELLED']:
                    error_reason = execution_response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    logger.warning(f"❌ Athena test query failed: {error_reason}")
                    return {
                        'success': False,
                        'query_id': query_execution_id,
                        'error': error_reason
                    }
                
                # Wait before checking again
                import time
                time.sleep(2)
                wait_time += 2
            
            # Timeout
            logger.warning(f"❌ Athena test query timed out after {max_wait} seconds")
            return {
                'success': False,
                'query_id': query_execution_id,
                'error': f'Query timed out after {max_wait} seconds'
            }
            
        except Exception as e:
            logger.error(f"❌ Error testing Athena access to {database}.{table}: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_table_summary(self, database: str, table: str) -> Dict:
        """
        Get comprehensive summary of CUR table
        
        Args:
            database: Database name
            table: Table name
            
        Returns:
            Dict: Comprehensive table summary
        """
        try:
            # Get basic table info
            table_info = self.glue_client.get_table(
                DatabaseName=database,
                Name=table
            )['Table']
            
            # Extract key information
            storage_desc = table_info.get('StorageDescriptor', {})
            columns = storage_desc.get('Columns', [])
            partition_keys = table_info.get('PartitionKeys', [])
            
            # Count different types of columns
            total_columns = len(columns)
            tag_columns = [
                col
                for col in columns
                if (
                    col['Name'].startswith('resource_tags_')
                    or (
                        col['Name'] == 'resource_tags'
                        and col.get('Type', '').startswith('map')
                    )
                )
            ]
            line_item_columns = [col for col in columns if col['Name'].startswith('line_item_')]
            product_columns = [col for col in columns if col['Name'].startswith('product_')]
            
            # Get partition info
            partition_info = self.get_partition_info(database, table)
            
            return {
                'database': database,
                'table': table,
                'location': storage_desc.get('Location', 'Unknown'),
                'input_format': storage_desc.get('InputFormat', 'Unknown'),
                'output_format': storage_desc.get('OutputFormat', 'Unknown'),
                'columns': {
                    'total': total_columns,
                    'tag_columns': len(tag_columns),
                    'line_item_columns': len(line_item_columns),
                    'product_columns': len(product_columns)
                },
                'partitioning': {
                    'partition_keys': [pk['Name'] for pk in partition_keys],
                    'partition_info': partition_info
                },
                'sample_tag_columns': [col['Name'] for col in tag_columns[:10]],  # First 10
                'table_type': table_info.get('TableType', 'Unknown'),
                'created': table_info.get('CreateTime', 'Unknown'),
                'last_accessed': table_info.get('LastAccessTime', 'Unknown'),
                'is_cur_table': self._is_cur_table(database, table, table_info)
            }
            
        except Exception as e:
            logger.error(f"Error getting table summary for {database}.{table}: {e}")
            return {
                'database': database,
                'table': table,
                'error': str(e)
            }
    
    def clear_cache(self):
        """Clear all cached schema information"""
        self._schema_cache.clear()
        self._table_validation_cache.clear()
        logger.info("Schema cache cleared")


if __name__ == "__main__":
    # Quick validation test — requires CUR_ATHENA_DATABASE and CUR_ATHENA_TABLE env vars
    import os
    schema_manager = CURSchemaManager()
    db = os.environ.get("CUR_ATHENA_DATABASE")
    table = os.environ.get("CUR_ATHENA_TABLE")
    if db and table:
        valid = schema_manager.validate_cur_table(db, table)
        print(f"CUR table {db}.{table} valid: {valid}")
        if valid:
            summary = schema_manager.get_table_summary(db, table)
            print(f"Columns: {summary['columns']['total']} total, {summary['columns']['tag_columns']} tag columns")
    else:
        print("Set CUR_ATHENA_DATABASE and CUR_ATHENA_TABLE env vars to test validation")

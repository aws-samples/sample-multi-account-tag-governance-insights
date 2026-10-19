#!/usr/bin/env python3
"""
CUR Query Builder - Builds optimized Athena queries for CUR data

Creates partition-pruned, column-optimized queries for maximum cost efficiency:
- Automatic partition pruning (year/month/day)
- Column selection optimization (avoid SELECT *)  
- Dynamic schema awareness (handles missing tag columns)
- Query cost estimation and optimization

Features:
- 95%+ cost reduction through partition pruning
- Dynamic tag column validation
- Query templates for common cost analysis patterns
- Built-in error handling for missing columns
"""

import os
from typing import Dict, List, Optional, Tuple, Set
from datetime import datetime
# Graceful fallback for Lambda environments without loguru
try:
    from loguru import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info("Using standard logging (loguru not available)")
import re

_ARN_RE = re.compile(
    r"^arn:(aws|aws-cn|aws-us-gov):[a-zA-Z0-9*-]+:"
    r"[a-z0-9*-]*:\d{12}:[A-Za-z0-9][A-Za-z0-9._:/+=,@-]{0,1023}$"
)
_RESOURCE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+=,@-]{0,1023}$")
_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_TAG_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+=@-]{0,127}$")
_TAG_VALUE_SAFE_RE = re.compile(r"^[^;'\"\\]{0,256}$")
_SERVICE_TYPE_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_YEAR_RE = re.compile(r"^\d{4}$")
_MONTH_RE = re.compile(r"^(?:[1-9]|0[1-9]|1[0-2])$")
_BILLING_PERIOD_RE = re.compile(r"^\d{4}-(?:0[1-9]|1[0-2])$")
_SERVICE_NAME_EXPRESSIONS = {
    'product_servicename',
    "element_at(product, 'servicename')",
}


def _escape_sql_string(value: str) -> str:
    """Escape a value for an Athena SQL string literal."""
    return value.replace("'", "''")


class CURQueryBuilder:
    """
    Builds optimized Athena queries for CUR data with intelligent cost optimization
    
    Handles dynamic schema challenges and partition pruning for maximum efficiency.
    """
    
    DEFAULT_SCHEMA_PROFILE = {
        'supported': True,
        'format': 'cur1',
        'tag_mode': 'flattened',
        'tag_column': None,
        'partition_mode': 'year_month',
        'cost_column': 'line_item_blended_cost',
        'direct_cost_column': 'line_item_unblended_cost',
        'service_name_expression': 'product_servicename',
    }

    def __init__(
        self,
        database: str,
        table: str,
        schema_profile: Optional[Dict] = None,
    ):
        """
        Initialize query builder for specific CUR table

        Args:
            database: CUR database name
            table: CUR table name
        """
        if not _SQL_IDENTIFIER_RE.fullmatch(database):
            raise ValueError(f"Invalid database identifier: {database!r}")
        if not _SQL_IDENTIFIER_RE.fullmatch(table):
            raise ValueError(f"Invalid table identifier: {table!r}")
        self.database = database
        self.table = table
        self.full_table_name = f"{database}.{table}"
        self.schema_profile = (
            schema_profile
            if schema_profile is not None
            else dict(self.DEFAULT_SCHEMA_PROFILE)
        )
        self.service_name_expression = self.schema_profile.get(
            'service_name_expression',
            'product_servicename',
        )
        if self.service_name_expression not in _SERVICE_NAME_EXPRESSIONS:
            raise ValueError(
                "Unsupported service_name_expression in schema profile"
            )

        logger.debug(
            f"CURQueryBuilder initialized for {self.full_table_name} "
            f"(format={self.schema_profile.get('format')})"
        )

    def _tag_expression(
        self,
        tag_key: str,
        available_columns: Set[str],
    ) -> Optional[str]:
        """Return the safe flattened-column or map expression for a tag."""
        if not _TAG_KEY_RE.fullmatch(tag_key):
            raise ValueError(f"Invalid tag_key: {tag_key!r}")

        if self.schema_profile.get('tag_mode') == 'map':
            tag_column = self.schema_profile.get('tag_column') or 'resource_tags'
            if tag_column not in available_columns:
                return None
            escaped_tag_key = _escape_sql_string(tag_key)
            return (
                f"COALESCE(element_at({tag_column}, 'user_{escaped_tag_key}'), "
                f"element_at({tag_column}, '{escaped_tag_key}'))"
            )

        tag_column = f"resource_tags_user_{tag_key.lower()}"
        return tag_column if tag_column in available_columns else None
    
    def build_costs_by_tag_query(self, tag_key: str, months_back: int, 
                                available_columns: Set[str]) -> Tuple[Optional[str], List[str]]:
        """
        Build cost-by-tag query with partition pruning and column validation
        
        Args:
            tag_key: Tag key to analyze (e.g., 'Environment')
            months_back: Number of months to query
            available_columns: Set of available tag columns in table
            
        Returns:
            Tuple[query_sql, missing_partitions]: SQL query string and list of missing partitions
            Returns (None, []) if tag column doesn't exist
        """
        tag_expression = self._tag_expression(tag_key, available_columns)
        if tag_expression is None:
            logger.warning(f"Tag column for '{tag_key}' not found in available columns")
            return None, []
        
        partition_filters, period_info = self._generate_partition_filters(months_back)
        cost_column = (
            self.schema_profile.get('cost_column')
            or 'line_item_blended_cost'
        )
        
        query = f"""
        SELECT 
            {tag_expression} as tag_value,
            SUM({cost_column}) as total_cost,
            COUNT(DISTINCT line_item_resource_id) as resource_count,
            COUNT(*) as line_item_count
        FROM {self.full_table_name}
        WHERE ({' OR '.join(partition_filters)})
          AND {tag_expression} IS NOT NULL
          AND {tag_expression} != ''
          AND {cost_column} > 0
          AND line_item_line_item_type = 'Usage'
        GROUP BY {tag_expression}
        ORDER BY total_cost DESC
        """

        logger.debug(f"Built costs-by-tag query for '{tag_key}' covering {len(partition_filters)} partitions")
        return query.strip(), []
    
    def build_cost_trends_query(self, tag_key: str, tag_value: str, 
                               months_back: int, available_columns: Set[str]) -> Optional[str]:
        """
        Build cost trends query for specific tag value over time
        
        Args:
            tag_key: Tag key (e.g., 'Environment')
            tag_value: Specific tag value (e.g., 'production')
            months_back: Number of months to analyze
            available_columns: Set of available tag columns
            
        Returns:
            Optional[str]: SQL query string or None if tag column doesn't exist
        """
        tag_expression = self._tag_expression(tag_key, available_columns)
        if tag_expression is None:
            logger.warning(f"Tag column for '{tag_key}' not found for trends query")
            return None
        
        partition_filters, period_info = self._generate_partition_filters(months_back)
        if not _TAG_VALUE_SAFE_RE.fullmatch(tag_value):
            logger.warning(f"Rejected unsafe tag value: {tag_value!r}")
            return None
        escaped_tag_value = _escape_sql_string(tag_value)
        cost_column = (
            self.schema_profile.get('cost_column')
            or 'line_item_blended_cost'
        )
        partition_mode = self.schema_profile.get(
            'partition_mode',
            'year_month',
        )
        month_expression = (
            "billing_period"
            if partition_mode == 'billing_period'
            else (
                "CONCAT(CAST(year AS VARCHAR), '-', "
                "LPAD(CAST(month AS VARCHAR), 2, '0'))"
            )
        )
        group_expression = (
            "billing_period"
            if partition_mode == 'billing_period'
            else "year, month"
        )
        
        query = f"""
        SELECT 
            {month_expression} as month_key,
            SUM({cost_column}) as monthly_cost,
            COUNT(DISTINCT line_item_resource_id) as resource_count,
            COUNT(*) as line_item_count
        FROM {self.full_table_name}
        WHERE ({' OR '.join(partition_filters)})
          AND {tag_expression} = '{escaped_tag_value}'
          AND {cost_column} > 0
          AND line_item_line_item_type = 'Usage'
        GROUP BY {group_expression}
        ORDER BY {group_expression}
        """
        
        logger.debug(f"Built cost trends query for {tag_key}='{tag_value}' covering {months_back} months")
        return query.strip()
    
    # Note: build_untagged_resources_query method removed - replaced by direct ARN matching in CURDirectMatcher
    
    def build_service_costs_query(self, months_back: int) -> str:
        """
        Build query to get costs by AWS service
        
        Args:
            months_back: Number of months to analyze
            
        Returns:
            str: SQL query string
        """
        partition_filters, period_info = self._generate_partition_filters(months_back)
        cost_column = (
            self.schema_profile.get('cost_column')
            or 'line_item_blended_cost'
        )
        
        query = f"""
        SELECT 
            {self.service_name_expression} as service_name,
            SUM({cost_column}) as total_cost,
            COUNT(DISTINCT line_item_resource_id) as resource_count,
            COUNT(*) as line_item_count
        FROM {self.full_table_name}
        WHERE ({' OR '.join(partition_filters)})
          AND {cost_column} > 0
          AND line_item_line_item_type = 'Usage'
          AND {self.service_name_expression} IS NOT NULL
          AND {self.service_name_expression} != ''
        GROUP BY {self.service_name_expression}
        ORDER BY total_cost DESC
        LIMIT 50
        """
        
        logger.debug(f"Built service costs query covering {months_back} months")
        return query.strip()
    
    def build_account_costs_query(self, months_back: int) -> str:
        """
        Build query to get costs by AWS account
        
        Args:
            months_back: Number of months to analyze
            
        Returns:
            str: SQL query string
        """
        partition_filters, period_info = self._generate_partition_filters(months_back)
        cost_column = (
            self.schema_profile.get('cost_column')
            or 'line_item_blended_cost'
        )
        
        query = f"""
        SELECT 
            line_item_usage_account_id as account_id,
            SUM({cost_column}) as total_cost,
            COUNT(DISTINCT line_item_resource_id) as resource_count,
            COUNT(DISTINCT {self.service_name_expression}) as service_count,
            COUNT(*) as line_item_count
        FROM {self.full_table_name}
        WHERE ({' OR '.join(partition_filters)})
          AND {cost_column} > 0
          AND line_item_line_item_type = 'Usage'
          AND line_item_usage_account_id IS NOT NULL
        GROUP BY line_item_usage_account_id
        ORDER BY total_cost DESC
        LIMIT 25
        """
        
        logger.debug(f"Built account costs query covering {months_back} months")
        return query.strip()
    
    # Note: build_resource_costs_query method removed - replaced by direct ARN matching in build_direct_arn_costs_query
    
    def build_direct_arn_costs_query(self, normalized_arns: List[str], 
                                    service_type: str, months_back: int) -> str:
        """
        Build optimized query for specific ARNs (direct ARN matching)
        
        Args:
            normalized_arns: List of ARNs in CUR format
            service_type: AWS service type for optimization
            months_back: Number of months to analyze
            
        Returns:
            str: SQL query string for direct ARN cost matching
        """
        if not normalized_arns:
            logger.warning("No ARNs provided for direct costs query")
            return ""

        partition_filters, _ = self._generate_partition_filters(months_back)
        cost_column = (
            self.schema_profile.get('direct_cost_column')
            or self.schema_profile.get('cost_column')
            or 'line_item_unblended_cost'
        )
        safe_resource_ids = []
        for resource_id in normalized_arns:
            if not _RESOURCE_ID_RE.fullmatch(resource_id):
                logger.warning(
                    f"Rejected invalid resource identifier in cost query: "
                    f"{resource_id!r}"
                )
                continue
            if resource_id.startswith('arn:') and not _ARN_RE.fullmatch(
                resource_id
            ):
                logger.warning(
                    f"Rejected invalid ARN in cost query: {resource_id!r}"
                )
                continue
            safe_resource_ids.append(_escape_sql_string(resource_id))
        if not safe_resource_ids:
            logger.warning("No safe resource identifiers for direct costs query")
            return ""
        arn_list = "', '".join(safe_resource_ids)
        
        # Build service filter (optional optimization)
        service_filter = ""
        if service_type and service_type != 'unknown':
            service_name_patterns = {
                'ec2': 'Amazon Elastic Compute Cloud',
                's3': 'Amazon Simple Storage Service', 
                'rds': 'Amazon Relational Database Service',
                'lambda': 'AWS Lambda',
                'ecs': 'Amazon EC2 Container Service',
                'elb': 'Elastic Load Balancing',
                'elasticache': 'Amazon ElastiCache',
                'dynamodb': 'Amazon DynamoDB',
                'sqs': 'Amazon Simple Queue Service',
                'sns': 'Amazon Simple Notification Service',
                'kms': 'AWS Key Management Service',
                'secretsmanager': 'AWS Secrets Manager',
                'secrets-manager': 'AWS Secrets Manager',
                'acm': 'AWS Certificate Manager',
                'certificate-manager': 'AWS Certificate Manager',
                'cognito': 'Amazon Cognito',
                'sagemaker': 'Amazon SageMaker',
                'glue': 'AWS Glue',
                'comprehend': 'Amazon Comprehend',
                'bedrock': 'Amazon Bedrock Service',
                'kendra': 'Amazon Kendra',
                'elasticsearch': 'Amazon Elasticsearch Service',
                'es': 'Amazon Elasticsearch Service',
                'opensearch': 'Amazon OpenSearch Service',
                'cloudformation': 'AWS CloudFormation',
                'cloudtrail': 'AWS CloudTrail',
                'logs': 'AmazonCloudWatch',
                'cloudwatch-logs': 'AmazonCloudWatch',
                'cloudwatch': 'AmazonCloudWatch',
                'systems-manager': 'AWS Systems Manager',
                'ssm': 'AWS Systems Manager',
                'batch': 'AWS Batch',
                'efs': 'Amazon Elastic File System',
                'fsx': 'Amazon FSx',
                'redshift': 'Amazon Redshift',
                'documentdb': 'Amazon DocumentDB',
                'docdb': 'Amazon DocumentDB',
                'neptune': 'Amazon Neptune',
                'qldb': 'Amazon QLDB',
                'timestream': 'Amazon Timestream',
                'cloudfront': 'Amazon CloudFront',
                'route53': 'Amazon Route 53',
                'nat-gateway': 'Amazon Virtual Private Cloud',
                'natgw': 'Amazon Virtual Private Cloud',
                'vpc-endpoint': 'Amazon Virtual Private Cloud',
                'vpce': 'Amazon Virtual Private Cloud',
                'elastic-ip': 'Amazon Virtual Private Cloud',
                'eip': 'Amazon Virtual Private Cloud',
                'transit-gateway': 'AWS Transit Gateway',
                'tgw': 'AWS Transit Gateway',
                'direct-connect': 'AWS Direct Connect',
                'dx': 'AWS Direct Connect',
                'vpn': 'Amazon Virtual Private Cloud',
                'globalaccelerator': 'AWS Global Accelerator',
                'waf': 'AWS WAF',
                'emr': 'Amazon Elastic MapReduce',
                'kinesis': 'Amazon Kinesis',
                'kinesis-data-streams': 'Amazon Kinesis',
                'kinesis-firehose': 'Amazon Kinesis Firehose',
                'firehose': 'Amazon Kinesis Firehose',
                'kinesis-analytics': 'Amazon Kinesis Analytics',
                'kinesisanalytics': 'Amazon Kinesis Analytics',
                'apigateway': 'Amazon API Gateway',
                'api-gateway': 'Amazon API Gateway',
                'appsync': 'AWS AppSync',
                'stepfunctions': 'AWS Step Functions',
                'step-functions': 'AWS Step Functions',
                'codebuild': 'AWS CodeBuild',
                'codedeploy': 'AWS CodeDeploy',
                'codepipeline': 'AWS CodePipeline',
                'codecommit': 'AWS CodeCommit',
                'cloud9': 'AWS Cloud9',
                'config': 'AWS Config',
                'guardduty': 'Amazon GuardDuty',
                'inspector': 'Amazon Inspector',
                'macie': 'Amazon Macie',
                'securityhub': 'AWS Security Hub',
                'trusted-advisor': 'AWS Support',
                'support': 'AWS Support',
                'workmail': 'AmazonWorkMail',
                'ses': 'Amazon Simple Email Service',
                'pinpoint': 'Amazon Pinpoint',
                'connect': 'Amazon Connect',
                'chime': 'Amazon Chime',
                'workspaces': 'Amazon WorkSpaces',
                'appstream': 'Amazon AppStream',
                'workdocs': 'Amazon WorkDocs',
                'worklink': 'Amazon WorkLink',
                'iot': 'AWS IoT',
                'iot-core': 'AWS IoT Core',
                'greengrass': 'AWS IoT Greengrass',
                'device-farm': 'AWS Device Farm',
                'xray': 'AWS X-Ray',
                'application-insights': 'Amazon CloudWatch Application Insights',
                'cost-explorer': 'AWS Cost Explorer',
                'budgets': 'AWS Budgets',
                'billing': 'AWS Billing',
                'organizations': 'AWS Organizations',
                'control-tower': 'AWS Control Tower',
                'service-catalog': 'AWS Service Catalog',
                'license-manager': 'AWS License Manager',
                'well-architected': 'AWS Well-Architected Tool',
                'personal-health-dashboard': 'AWS Personal Health Dashboard',
                'chatbot': 'AWS Chatbot',
                'data-exchange': 'AWS Data Exchange',
                'marketplace': 'AWS Marketplace',
                'ground-station': 'AWS Ground Station',
                'robomaker': 'AWS RoboMaker',
                'sumerian': 'Amazon Sumerian',
                'forecast': 'Amazon Forecast',
                'personalize': 'Amazon Personalize',
                'textract': 'Amazon Textract',
                'transcribe': 'Amazon Transcribe',
                'translate': 'Amazon Translate',
                'polly': 'Amazon Polly',
                'rekognition': 'Amazon Rekognition',
                'lex': 'Amazon Lex',
                'sagemaker-ground-truth': 'Amazon SageMaker Ground Truth',
                'quicksight': 'Amazon QuickSight',
                'athena': 'Amazon Athena',
                'msk': 'Amazon Managed Streaming for Apache Kafka',
                'kafka': 'Amazon Managed Streaming for Apache Kafka',
                'mq': 'Amazon MQ',
                'managed-blockchain': 'Amazon Managed Blockchain',
                'quantum-ledger-database': 'Amazon QLDB',
                'datazone': 'Amazon DataZone',
                'location': 'Amazon Location Service',
                'devops-guru': 'Amazon DevOps Guru'
            }
            
            if not _SERVICE_TYPE_RE.fullmatch(service_type):
                raise ValueError(f"Invalid service_type: {service_type!r}")
            service_pattern = service_name_patterns.get(service_type, service_type)
            service_filter = (
                f"AND {self.service_name_expression} "
                f"LIKE '%{service_pattern}%'"
            )
        
        query = f"""
        SELECT 
            line_item_resource_id as resource_id,
            {self.service_name_expression} as service_name,
            SUM({cost_column}) as monthly_cost,
            COUNT(*) as usage_records,
            AVG({cost_column}) as avg_daily_cost,
            MIN(line_item_usage_start_date) as first_usage,
            MAX(line_item_usage_start_date) as last_usage
        FROM {self.full_table_name}
        WHERE ({' OR '.join(partition_filters)})
          AND line_item_resource_id IN ('{arn_list}')
          AND line_item_line_item_type = 'Usage'
          {service_filter}
        GROUP BY line_item_resource_id, {self.service_name_expression}
        ORDER BY monthly_cost DESC
        """
        
        logger.debug(
            f"Built direct ARN costs query for {len(safe_resource_ids)} "
            f"{service_type} resources"
        )
        return query.strip()
    
    def _generate_partition_filters(
        self,
        months_back: int,
        current_date: Optional[datetime] = None,
    ) -> Tuple[List[str], Dict]:
        """
        Generate partition filters for given time period with configurable date format
        
        Args:
            months_back: Number of months to include
            
        Returns:
            Tuple[List[str], Dict]: List of partition filter conditions and period info
        """
        if not isinstance(months_back, int) or not 1 <= months_back <= 120:
            raise ValueError("months_back must be an integer between 1 and 120")

        partition_mode = self.schema_profile.get(
            'partition_mode',
            'year_month',
        )
        current_date = current_date or datetime.now()
        month_format = os.environ.get('CUR_MONTH_FORMAT', 'no_leading_zero')
        filters = []
        months_covered = []
        oldest_year = current_date.year
        oldest_month = current_date.month

        for i in range(months_back):
            total_months = (
                current_date.year * 12
                + current_date.month
                - 1
                - i
            )
            year, month_index = divmod(total_months, 12)
            month = month_index + 1
            oldest_year = year
            oldest_month = month

            if partition_mode == 'billing_period':
                billing_period = f"{year}-{month:02d}"
                filters.append(
                    f"(billing_period = '{billing_period}')"
                )
                months_covered.append(billing_period)
            else:
                month_str = (
                    str(month)
                    if month_format == 'no_leading_zero'
                    else f"{month:02d}"
                )
                filters.append(
                    f"(year = '{year}' AND month = '{month_str}')"
                )
                months_covered.append(
                    f"{year}-{month}"
                    if month_format == 'no_leading_zero'
                    else f"{year}-{month:02d}"
                )

        months_covered.reverse()
        period_info = {
            'start_date': datetime(
                oldest_year,
                oldest_month,
                1,
            ).strftime('%Y-%m-%d'),
            'end_date': current_date.strftime('%Y-%m-%d'),
            'partitions_generated': len(filters),
            'months_covered': months_covered,
            'partition_mode': partition_mode,
        }
        
        logger.info(
            f"Generated {len(filters)} partition filters covering "
            f"{months_back} months using partition_mode='{partition_mode}'"
        )
        return filters, period_info

    def build_connectivity_query(self) -> str:
        """Build a cheap partition-pruned provider preflight query."""
        partition_filters, _ = self._generate_partition_filters(1)
        return f"""
        SELECT COUNT(*) as record_count
        FROM {self.full_table_name}
        WHERE ({' OR '.join(partition_filters)})
        LIMIT 1
        """.strip()

    def build_available_tag_keys_query(
        self,
        months_back: int = 1,
        limit: int = 1000,
    ) -> Optional[str]:
        """Build the bounded native-CUR2 map-key enumeration query."""
        if self.schema_profile.get('tag_mode') != 'map':
            return None
        if months_back != 1:
            raise ValueError(
                "CUR2 tag-key discovery is limited to one billing month"
            )
        if not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError(
                "CUR2 tag-key discovery limit must be between 1 and 1000"
            )

        tag_column = self.schema_profile.get('tag_column') or 'resource_tags'
        partition_filters, _ = self._generate_partition_filters(months_back)
        return f"""
        SELECT DISTINCT REGEXP_REPLACE(tag_key, '^user_', '') as tag_key
        FROM {self.full_table_name}
        CROSS JOIN UNNEST(map_keys({tag_column})) AS keys(tag_key)
        WHERE ({' OR '.join(partition_filters)})
          AND tag_key IS NOT NULL
          AND tag_key != ''
        ORDER BY tag_key
        LIMIT {limit}
        """.strip()
    
    def estimate_query_cost(self, query: str, data_scanned_gb: Optional[float] = None) -> Dict:
        """
        Estimate Athena query cost based on query complexity and data volume
        
        Args:
            query: SQL query string
            data_scanned_gb: Optional estimated data scanned in GB
            
        Returns:
            Dict: Cost estimation details
        """
        # Athena pricing: $5 per TB of data scanned
        cost_per_tb = 5.0
        cost_per_gb = cost_per_tb / 1024
        
        # Count partition filters to estimate data reduction
        partition_count = (
            query.count("year = '")
            + query.count("year >= '")
            + query.count("month = '")
            + query.count("billing_period = '")
            + query.count("billing_period >= '")
        )
        
        # Estimate data scanned based on partition filters and query complexity
        if data_scanned_gb is None:
            # Default estimates based on query patterns
            base_scan_gb = 10.0  # Base assumption for 1 month of CUR data
            
            # Adjust based on partition filters
            if partition_count >= 2:  # year + month filters
                base_scan_gb *= 0.03  # Significant reduction with proper partitioning
            elif partition_count >= 1:
                base_scan_gb *= 0.1   # Some reduction
            
            # Adjust based on query complexity
            if 'GROUP BY' in query.upper():
                base_scan_gb *= 1.2   # Slightly more for aggregation
            if 'ORDER BY' in query.upper():
                base_scan_gb *= 1.1   # Slightly more for sorting
            
            data_scanned_gb = base_scan_gb
        
        estimated_cost = data_scanned_gb * cost_per_gb
        
        return {
            'estimated_data_scanned_gb': round(data_scanned_gb, 3),
            'estimated_cost_usd': round(estimated_cost, 4),
            'partition_filters_detected': partition_count,
            'cost_optimization_level': self._assess_optimization_level(query),
            'recommendations': self._get_cost_optimization_recommendations(query)
        }
    
    def _assess_optimization_level(self, query: str) -> str:
        """
        Assess the cost optimization level of a query
        
        Args:
            query: SQL query string
            
        Returns:
            str: Optimization level (excellent, good, fair, poor)
        """
        score = 0
        
        # Check for partition pruning
        if (
            ("year = '" in query and "month = '" in query)
            or "year >= '" in query
            or "billing_period = '" in query
            or "billing_period >= '" in query
        ):
            score += 40  # Major optimization
        elif "year = '" in query or "month = '" in query:
            score += 20  # Some optimization
        
        # Check for column selection (avoid SELECT *)
        if "SELECT *" not in query.upper():
            score += 30  # Good column selection
        
        # Check for WHERE clauses
        where_conditions = query.upper().count("WHERE")
        if where_conditions > 0:
            score += 20  # Has filtering
        
        # Check for appropriate LIMIT
        if "LIMIT" in query.upper():
            score += 10  # Has result limiting
        
        # Classify optimization level
        if score >= 80:
            return "excellent"
        elif score >= 60:
            return "good" 
        elif score >= 40:
            return "fair"
        else:
            return "poor"
    
    def _get_cost_optimization_recommendations(self, query: str) -> List[str]:
        """
        Get cost optimization recommendations for a query
        
        Args:
            query: SQL query string
            
        Returns:
            List[str]: List of optimization recommendations
        """
        recommendations = []
        
        # Check for missing partition filters
        has_year_month = (
            ("year = '" in query and "month = '" in query)
            or "year >= '" in query
        )
        has_billing_period = (
            "billing_period = '" in query
            or "billing_period >= '" in query
        )
        if not has_year_month and not has_billing_period:
            recommendations.append(
                "Add year/month or billing_period partition filters "
                "to reduce data scanned by 95%+"
            )
        
        # Check for SELECT *
        if "SELECT *" in query.upper():
            recommendations.append("Avoid SELECT * - specify only needed columns to reduce data transfer")
        
        # Check for missing WHERE clauses
        if query.upper().count("WHERE") == 0:
            recommendations.append("Add WHERE clauses to filter unnecessary data")
        
        # Check for missing LIMIT
        if "LIMIT" not in query.upper() and "GROUP BY" in query.upper():
            recommendations.append("Consider adding LIMIT clause to prevent large result sets")
        
        # Check for expensive operations
        if "DISTINCT" in query.upper():
            recommendations.append("DISTINCT operations can be expensive - ensure they're necessary")
        
        if not recommendations:
            recommendations.append("Query appears well-optimized for cost efficiency")
        
        return recommendations
    
    def get_query_template(self, template_name: str, **kwargs) -> Optional[str]:
        """Return a profile-aware predefined query with validated literals."""
        cost_column = (
            self.schema_profile.get('cost_column')
            or 'line_item_blended_cost'
        )
        partition_mode = self.schema_profile.get(
            'partition_mode',
            'year_month',
        )

        try:
            if partition_mode == 'billing_period':
                billing_period = kwargs.get('billing_period')
                if template_name == 'monthly_costs':
                    start_billing_period = str(
                        kwargs['start_billing_period']
                    )
                    if not _BILLING_PERIOD_RE.fullmatch(
                        start_billing_period
                    ):
                        raise ValueError("invalid start_billing_period")
                    return f"""
                    SELECT billing_period as month,
                           SUM({cost_column}) as total_cost,
                           COUNT(DISTINCT line_item_resource_id) as resource_count
                    FROM {self.full_table_name}
                    WHERE billing_period >= '{start_billing_period}'
                      AND {cost_column} > 0
                      AND line_item_line_item_type = 'Usage'
                    GROUP BY billing_period
                    ORDER BY billing_period
                    """.strip()

                if not _BILLING_PERIOD_RE.fullmatch(str(billing_period)):
                    raise ValueError("invalid billing_period")
                if template_name == 'top_resources':
                    limit = int(kwargs['limit'])
                    if not 1 <= limit <= 1000:
                        raise ValueError("limit must be between 1 and 1000")
                    return f"""
                    SELECT line_item_resource_id as resource_id,
                           {self.service_name_expression} as service,
                           SUM({cost_column}) as total_cost
                    FROM {self.full_table_name}
                    WHERE billing_period = '{billing_period}'
                      AND {cost_column} > 0
                      AND line_item_line_item_type = 'Usage'
                      AND line_item_resource_id IS NOT NULL
                    GROUP BY line_item_resource_id, {self.service_name_expression}
                    ORDER BY total_cost DESC
                    LIMIT {limit}
                    """.strip()
                if template_name == 'service_summary':
                    return f"""
                    SELECT {self.service_name_expression} as service,
                           SUM({cost_column}) as total_cost,
                           COUNT(DISTINCT line_item_resource_id) as resource_count
                    FROM {self.full_table_name}
                    WHERE billing_period = '{billing_period}'
                      AND {cost_column} > 0
                      AND line_item_line_item_type = 'Usage'
                    GROUP BY {self.service_name_expression}
                    ORDER BY total_cost DESC
                    """.strip()
            else:
                if template_name == 'monthly_costs':
                    start_year = str(kwargs['start_year'])
                    if not _YEAR_RE.fullmatch(start_year):
                        raise ValueError("invalid start_year")
                    return f"""
                    SELECT CONCAT(CAST(year AS VARCHAR), '-', LPAD(CAST(month AS VARCHAR), 2, '0')) as month,
                           SUM({cost_column}) as total_cost,
                           COUNT(DISTINCT line_item_resource_id) as resource_count
                    FROM {self.full_table_name}
                    WHERE year >= '{start_year}'
                      AND {cost_column} > 0
                      AND line_item_line_item_type = 'Usage'
                    GROUP BY year, month
                    ORDER BY year, month
                    """.strip()

                year = str(kwargs.get('year'))
                month = str(kwargs.get('month'))
                if not _YEAR_RE.fullmatch(year) or not _MONTH_RE.fullmatch(
                    month
                ):
                    raise ValueError("invalid year or month")
                if template_name == 'top_resources':
                    limit = int(kwargs['limit'])
                    if not 1 <= limit <= 1000:
                        raise ValueError("limit must be between 1 and 1000")
                    return f"""
                    SELECT line_item_resource_id as resource_id,
                           {self.service_name_expression} as service,
                           SUM({cost_column}) as total_cost
                    FROM {self.full_table_name}
                    WHERE (year = '{year}' AND month = '{month}')
                      AND {cost_column} > 0
                      AND line_item_line_item_type = 'Usage'
                      AND line_item_resource_id IS NOT NULL
                    GROUP BY line_item_resource_id, {self.service_name_expression}
                    ORDER BY total_cost DESC
                    LIMIT {limit}
                    """.strip()
                if template_name == 'service_summary':
                    return f"""
                    SELECT {self.service_name_expression} as service,
                           SUM({cost_column}) as total_cost,
                           COUNT(DISTINCT line_item_resource_id) as resource_count
                    FROM {self.full_table_name}
                    WHERE (year = '{year}' AND month = '{month}')
                      AND {cost_column} > 0
                      AND line_item_line_item_type = 'Usage'
                    GROUP BY {self.service_name_expression}
                    ORDER BY total_cost DESC
                    """.strip()
        except (KeyError, TypeError, ValueError) as exc:
            logger.error(
                f"Invalid parameter for template '{template_name}': {exc}"
            )
            return None

        logger.warning(f"Query template '{template_name}' not found")
        return None


if __name__ == "__main__":
    # Simple test/demo
    builder = CURQueryBuilder("test_db", "test_table")
    
    # Test partition filter generation
    filters, info = builder._generate_partition_filters(3)
    print(f"Generated partition filters: {filters}")
    print(f"Period info: {info}")
    
    # Test query cost estimation
    sample_query = """
    SELECT 
        resource_tags_user_environment as tag_value,
        SUM(line_item_blended_cost) as total_cost
    FROM test_db.test_table
    WHERE (year = '2024' AND month = '09')
      AND resource_tags_user_environment IS NOT NULL
    GROUP BY resource_tags_user_environment
    ORDER BY total_cost DESC
    """
    
    cost_estimate = builder.estimate_query_cost(sample_query)
    print(f"Cost estimate: {cost_estimate}")
    
    # Test query template
    template_query = builder.get_query_template(
        'monthly_costs',
        start_year='2024'
    )
    if template_query:
        print("Template query generated successfully")
    
    print("✅ CURQueryBuilder test completed")

#!/usr/bin/env python3
"""
CUR table reference for the cost allocator: validated identifiers, the
schema-dependent service-name expression, partition filters, and the
resource-type -> CUR service-name patterns.
"""

import os
from typing import Dict, List, Optional, Tuple
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


# CUR product_servicename substring per TagInsights service type.
SERVICE_NAME_PATTERNS = {
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
_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
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
                int_columns = self.schema_profile.get('int_partition_columns') or ()
                year_sql = str(year) if 'year' in int_columns else f"'{year}'"
                month_sql = str(month) if 'month' in int_columns else f"'{month_str}'"
                filters.append(
                    f"(year = {year_sql} AND month = {month_sql})"
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

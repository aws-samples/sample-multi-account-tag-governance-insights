#!/usr/bin/env python3
"""
CUR Direct Matcher - Direct ARN-to-cost matching using CUR data

Handles direct ARN-based cost matching with CUR records:
- Service-specific ARN normalization
- Optimized batch queries for performance
- Cost data extraction and aggregation
- Graceful handling of missing/unmatched ARNs

Features:
- ARN format conversion for CUR compatibility
- Batch processing to minimize query costs
- Service-specific optimization patterns
- Usage-based cost classification
"""

import os
import boto3
from typing import Dict, List, Optional, Set, Any, Tuple
from datetime import datetime, timedelta
import json

# Graceful fallback for Lambda environments without loguru
try:
    from loguru import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info("Using standard logging (loguru not available)")

try:
    # Try relative imports first (normal package structure)
    from .query_builder import CURQueryBuilder
    from .schema_manager import CURSchemaManager
    from .cur_provider import CURCostProvider
except ImportError:
    try:
        # Try absolute imports from src.cost (Lambda environment)
        from src.lambda_handlers.cost.query_builder import CURQueryBuilder
        from src.lambda_handlers.cost.schema_manager import CURSchemaManager
        from src.lambda_handlers.cost.cur_provider import CURCostProvider
    except ImportError:
        # Final fallback for direct imports (development)
        from query_builder import CURQueryBuilder
        from schema_manager import CURSchemaManager
        from cur_provider import CURCostProvider


class CURDirectMatcher:
    """
    Direct ARN-to-cost matching using CUR data
    
    Converts analytics table ARNs to CUR format and performs exact cost matching
    with optimized batch processing for cost efficiency.
    """
    
    def __init__(self, region_name: str = 'us-east-1'):
        """
        Initialize CUR direct matcher
        
        Args:
            region_name: AWS region for CUR/Athena operations
        """
        self.region = region_name
        
        # Initialize CUR components if available
        self.cur_available = False
        self.cur_provider = None
        self.query_builder = None
        self.schema_manager = CURSchemaManager(region_name)
        
        # Try to initialize CUR provider
        self._initialize_cur_provider()
        
        # Batch processing settings
        self.batch_size = 50  # ARNs per batch to avoid query complexity limits
        self.max_retries = 2
        
        logger.info(f"CURDirectMatcher initialized (CUR available: {self.cur_available})")
    
    def _initialize_cur_provider(self):
        """Initialize CUR provider if available"""
        try:
            # Check for CUR configuration
            cur_database = os.environ.get('CUR_ATHENA_DATABASE')
            cur_table = os.environ.get('CUR_ATHENA_TABLE')
            
            if cur_database and cur_table:
                self.cur_provider = CURCostProvider(
                    cur_database,
                    cur_table,
                    self.region,
                    cur_session=self.schema_manager.session,
                )
                self.query_builder = self.cur_provider.query_builder
                self.cur_available = True
                logger.info(
                    f"CUR provider initialized: {cur_database}.{cur_table} "
                    f"(format: {self.cur_provider.schema_profile['format']})"
                )
            else:
                # CUR not configured — explicit setup required.
                # Auto-discovery has been removed: it required broad glue:GetDatabases
                # permissions and scanned all databases on every cost analysis run.
                # Set CurDatabaseName + CurTableName in cost-analysis-stack.yaml to enable CUR.
                logger.info(
                    "CUR not configured (CUR_ATHENA_DATABASE / CUR_ATHENA_TABLE not set). "
                    "Set CurDatabaseName and CurTableName in cost-analysis-stack.yaml "
                    "to enable direct ARN-to-cost matching via CUR."
                )
                    
        except Exception as e:
            logger.error(f"CUR initialization failed: {e}")
            self.cur_available = False
            self.cur_provider = None
            self.query_builder = None
    
    def get_costs_for_resources(self, resources: List[Dict], months_back: int = 1) -> List[Dict]:
        """
        Get exact costs for specific resources using direct ARN matching
        
        Args:
            resources: List of resource dictionaries with ARNs and metadata
            months_back: Number of months to analyze for cost data
            
        Returns:
            List[Dict]: Resources with cost data attached
        """
        if not self.cur_available:
            logger.error("CUR not available - cannot perform direct ARN matching")
            return self._create_empty_cost_results(resources)
        
        if not resources:
            logger.debug("No resources provided for cost analysis")
            return []
        
        logger.info(f"Getting costs for {len(resources)} resources using direct ARN matching")
        
        try:
            # Group resources by service type for efficient querying
            resources_by_service = self._group_resources_by_service(resources)
            
            all_cost_results = []
            
            # Process each service type separately for optimization
            for service_type, service_resources in resources_by_service.items():
                logger.debug(f"Processing {len(service_resources)} {service_type} resources")
                
                service_costs = self._get_costs_for_service_resources(
                    service_resources, service_type, months_back
                )
                all_cost_results.extend(service_costs)
            
            logger.info(f"Cost analysis completed for {len(all_cost_results)} resources")
            return all_cost_results
            
        except Exception as e:
            logger.error(f"Error in direct ARN cost matching: {e}")
            return self._create_empty_cost_results(resources)
    
    def _get_costs_for_service_resources(self, resources: List[Dict], 
                                       service_type: str, months_back: int) -> List[Dict]:
        """
        Get costs for resources of a specific service type
        
        Args:
            resources: List of resources for this service
            service_type: AWS service type (e.g., 'ec2', 's3')
            months_back: Number of months to analyze
            
        Returns:
            List[Dict]: Resources with cost data
        """
        service_results = []
        
        try:
            # Process resources in batches to avoid query complexity limits
            for batch in self._batch_resources(resources, self.batch_size):
                batch_results = self._process_resource_batch(batch, service_type, months_back)
                service_results.extend(batch_results)
            
            return service_results
            
        except Exception as e:
            logger.error(f"Error processing {service_type} resources: {e}")
            # Return resources with zero costs rather than failing completely
            return self._create_empty_cost_results(resources)
    
    def _process_resource_batch(self, resources: List[Dict], 
                              service_type: str, months_back: int) -> List[Dict]:
        """
        Process a batch of resources for cost matching
        
        Args:
            resources: Batch of resources to process
            service_type: AWS service type
            months_back: Number of months to analyze
            
        Returns:
            List[Dict]: Resources with cost data
        """
        batch_results = []
        
        try:
            # Special handling for S3 - generate both formats for historical compatibility
            if service_type == 's3':
                return self._process_s3_resource_batch(resources, months_back)
            
            # Standard processing for non-S3 resources
            normalized_mapping = {}  # original_arn -> normalized_arn
            for resource in resources:
                original_arn = resource['resource_arn']
                normalized_arn = self._normalize_arn_for_cur(original_arn, service_type)
                normalized_mapping[original_arn] = normalized_arn
            
            # Get unique normalized ARNs (avoid duplicates)
            unique_normalized_arns = list(set(normalized_mapping.values()))
            
            if not unique_normalized_arns:
                logger.warning(f"No valid normalized ARNs for {service_type} batch")
                return self._create_empty_cost_results(resources)
            
            # Query CUR for these specific ARNs
            cur_costs = self._query_cur_for_arns(unique_normalized_arns, service_type, months_back)
            
            # Map results back to original resources
            for resource in resources:
                original_arn = resource['resource_arn']
                normalized_arn = normalized_mapping[original_arn]
                
                # Find cost data for this ARN
                cost_data = cur_costs.get(normalized_arn, {})
                
                # Merge resource info with cost data
                result = {
                    **resource,  # Original resource metadata
                    'monthly_cost': cost_data.get('monthly_cost', 0.0),
                    'usage_records': cost_data.get('usage_records', 0),
                    'avg_daily_cost': cost_data.get('avg_daily_cost', 0.0),
                    'first_usage': cost_data.get('first_usage', ''),
                    'last_usage': cost_data.get('last_usage', ''),
                    'matched_in_cur': cost_data.get('matched_in_cur', False),
                    'normalized_arn': normalized_arn
                }
                
                batch_results.append(result)
            
            return batch_results
            
        except Exception as e:
            logger.error(f"Error processing resource batch: {e}")
            return self._create_empty_cost_results(resources)
    
    def _process_s3_resource_batch(self, resources: List[Dict], months_back: int) -> List[Dict]:
        """
        Special processing for S3 resources - queries both ARN and bucket name formats
        for comprehensive historical data capture
        
        Args:
            resources: S3 resources to process
            months_back: Number of months to analyze
            
        Returns:
            List[Dict]: Resources with cost data from both formats
        """
        batch_results = []
        
        try:
            # Generate both formats for each S3 resource
            all_search_formats = set()
            resource_format_mapping = {}  # original_arn -> [format1, format2, ...]
            
            for resource in resources:
                original_arn = resource['resource_arn']
                formats = self._get_s3_search_formats(original_arn)
                resource_format_mapping[original_arn] = formats
                all_search_formats.update(formats)
            
            if not all_search_formats:
                logger.warning("No valid S3 search formats generated")
                return self._create_empty_cost_results(resources)
            
            # Query CUR for all S3 formats
            logger.debug(f"Querying CUR for {len(all_search_formats)} S3 format variations")
            cur_costs = self._query_cur_for_arns(list(all_search_formats), 's3', months_back)
            
            # Map results back to original resources (check all possible formats)
            for resource in resources:
                original_arn = resource['resource_arn']
                search_formats = resource_format_mapping[original_arn]
                
                # Try to find cost data in any of the formats
                cost_data = {}
                matched_format = None
                
                for format_variant in search_formats:
                    if format_variant in cur_costs:
                        cost_data = cur_costs[format_variant]
                        matched_format = format_variant
                        break
                
                # If no direct match, combine data from multiple formats (edge case)
                if not cost_data and len(search_formats) > 1:
                    combined_cost = 0.0
                    combined_usage = 0
                    matched_formats = []
                    
                    for format_variant in search_formats:
                        if format_variant in cur_costs:
                            format_cost_data = cur_costs[format_variant]
                            combined_cost += format_cost_data.get('monthly_cost', 0.0)
                            combined_usage += format_cost_data.get('usage_records', 0)
                            matched_formats.append(format_variant)
                    
                    if matched_formats:
                        cost_data = {
                            'monthly_cost': combined_cost,
                            'usage_records': combined_usage,
                            'avg_daily_cost': combined_cost / 30 if combined_cost > 0 else 0,
                            'first_usage': '',  # Would need aggregation logic
                            'last_usage': '',   # Would need aggregation logic  
                            'matched_in_cur': True
                        }
                        matched_format = f"combined({len(matched_formats)})"
                
                # Merge resource info with cost data
                result = {
                    **resource,  # Original resource metadata
                    'monthly_cost': cost_data.get('monthly_cost', 0.0),
                    'usage_records': cost_data.get('usage_records', 0),
                    'avg_daily_cost': cost_data.get('avg_daily_cost', 0.0),
                    'first_usage': cost_data.get('first_usage', ''),
                    'last_usage': cost_data.get('last_usage', ''),
                    'matched_in_cur': cost_data.get('matched_in_cur', False),
                    'normalized_arn': matched_format or search_formats[0] if search_formats else '',
                    's3_search_formats': search_formats  # Debug info
                }
                
                batch_results.append(result)
            
            logger.debug(f"S3 batch processing completed: {len(batch_results)} resources processed")
            return batch_results
            
        except Exception as e:
            logger.error(f"Error processing S3 resource batch: {e}")
            return self._create_empty_cost_results(resources)
    
    def _get_s3_search_formats(self, arn: str) -> List[str]:
        """
        Generate all possible S3 formats to search for in CUR data
        
        Args:
            arn: Original S3 ARN
            
        Returns:
            List[str]: All possible formats to search for
        """
        formats = []
        
        if not arn:
            return formats
        
        try:
            # Extract bucket name from ARN
            if ':::' in arn:
                bucket_part = arn.split(':::')[-1]
                bucket_name = bucket_part.split('/')[0]  # Remove any object keys
            else:
                bucket_name = arn
            
            # Format 1: Full ARN format (2025+ format)
            if arn.startswith('arn:aws:s3:::'):
                formats.append(arn)
            else:
                formats.append(f"arn:aws:s3:::{bucket_name}")
            
            # Format 2: Legacy bucket name only (historical format)
            if bucket_name and not bucket_name.startswith('arn:'):
                formats.append(bucket_name)
            
            # Remove duplicates while preserving order
            seen = set()
            unique_formats = []
            for fmt in formats:
                if fmt not in seen:
                    seen.add(fmt)
                    unique_formats.append(fmt)
            
            logger.debug(f"Generated S3 search formats for '{arn}': {unique_formats}")
            return unique_formats
            
        except Exception as e:
            logger.debug(f"Error generating S3 formats for '{arn}': {e}")
            return [arn]  # Fallback to original ARN
    
    def _query_cur_for_arns(self, normalized_arns: List[str],
                          service_type: str, months_back: int) -> Dict[str, Dict]:
        """
        Query CUR for specific normalized ARNs
        
        Args:
            normalized_arns: List of ARNs in CUR format
            service_type: AWS service type for optimization
            months_back: Number of months to query
            
        Returns:
            Dict[str, Dict]: Mapping of ARN to cost data
        """
        try:
            # Build optimized query for these ARNs
            query = self.query_builder.build_direct_arn_costs_query(
                normalized_arns, service_type, months_back
            )
            
            if not query:
                logger.warning(f"Could not build query for {service_type} ARNs")
                return {}
            
            # Execute query via CUR provider
            query_result = self.cur_provider._execute_athena_query(
                query, f"direct_arn_costs_{service_type}"
            )
            
            if not query_result or 'error' in query_result:
                logger.warning(f"CUR query failed for {service_type}: {query_result.get('error', 'Unknown error')}")
                return {}
            
            # Process query results into ARN -> cost mapping
            arn_costs = {}
            for row in query_result.get('data', []):
                arn = row.get('resource_id', '')
                if arn:
                    arn_costs[arn] = {
                        'monthly_cost': float(row.get('monthly_cost', 0)),
                        'usage_records': int(row.get('usage_records', 0)),
                        'avg_daily_cost': float(row.get('avg_daily_cost', 0)),
                        'first_usage': row.get('first_usage', ''),
                        'last_usage': row.get('last_usage', ''),
                        'matched_in_cur': True
                    }
            
            logger.debug(f"Retrieved costs for {len(arn_costs)} out of {len(normalized_arns)} ARNs")
            
            return arn_costs
            
        except Exception as e:
            logger.error(f"Error querying CUR for ARNs: {e}")
            return {}
    
    def _fix_resource_explorer_arn_quirks(self, arn: str) -> str:
        """
        Safety net to fix Resource Explorer ARN quirks that slip through preventive fixes
        
        Resource Explorer adds erroneous prefixes to some RDS snapshot ARNs:
        - cluster-snapshot:rds:name -> cluster-snapshot:name
        - snapshot:rds:name -> snapshot:name
        
        Args:
            arn: ARN that may have Resource Explorer quirks
            
        Returns:
            str: Fixed ARN with quirks removed
        """
        if not arn or ':rds:' not in arn:
            return arn
        
        try:
            original_arn = arn
            
            # Fix RDS cluster snapshot ARNs with erroneous rds: prefix
            if 'cluster-snapshot:rds:' in arn:
                arn = arn.replace('cluster-snapshot:rds:', 'cluster-snapshot:')
                logger.debug(f"Safety net fixed RDS cluster snapshot ARN: {original_arn} -> {arn}")
            
            # Fix RDS database snapshot ARNs with erroneous rds: prefix
            elif 'snapshot:rds:' in arn:
                arn = arn.replace('snapshot:rds:', 'snapshot:')
                logger.debug(f"Safety net fixed RDS snapshot ARN: {original_arn} -> {arn}")
            
            return arn
            
        except Exception as e:
            logger.warning(f"Error in safety net ARN fix for '{arn}': {e}")
            return arn  # Return original if fix fails
    
    def _normalize_arn_for_cur(self, arn: str, service_type: str) -> str:
        """
        Convert analytics table ARN to CUR line_item_resource_id format
        
        Based on ACTUAL CUR data analysis - updated to match real AWS CUR formats.
        Most services store FULL ARNs in CUR, not just resource identifiers.
        
        Args:
            arn: Original ARN from analytics table
            service_type: AWS service type for context
            
        Returns:
            str: Normalized resource ID for CUR matching
        """
        if not arn:
            return ''
        
        try:
            # SAFETY NET: Fix Resource Explorer RDS snapshot ARN quirks first
            # This handles any malformed ARNs that slip through the preventive fixes
            arn = self._fix_resource_explorer_arn_quirks(arn)
            
            # === SERVICES THAT USE FULL ARNS IN CUR ===
            # Based on actual CUR data analysis - these services store complete ARNs
            
            if service_type == 'lambda':
                # Lambda: CUR stores full ARNs like arn:aws:lambda:us-east-1:1234567890123:function:curte-lambda2
                return arn
            
            elif service_type == 'rds':
                # RDS: CUR stores full ARNs like arn:aws:rds:us-east-1:1234567890123:db:test
                return arn
            
            elif service_type == 'aurora':
                # Aurora: CUR stores full ARNs (part of RDS service)
                return arn
            
            elif service_type == 'dynamodb':
                # DynamoDB: CUR stores full ARNs like arn:aws:dynamodb:us-east-1:1234567890123:table/SpringClean-XUG3HH5R-SpringCleanDDBTable-3VBJY6NZZZN5
                return arn
            
            # Application Integration - ALL use full ARNs in CUR
            elif service_type == 'sqs':
                # SQS: CUR stores full ARNs like arn:aws:sqs:us-east-1:1234567890123:AmazonRekognitionQueue1674596400855
                return arn
            
            elif service_type == 'sns':
                # SNS: CUR stores full ARNs like arn:aws:sns:us-east-1:1234567890123:AmazonRekognitionExample1674594265425
                return arn
            
            elif service_type in ['stepfunctions', 'step-functions']:
                # Step Functions: CUR likely stores full ARNs
                return arn
            
            # Security & Identity - ALL use full ARNs in CUR
            elif service_type == 'kms':
                # KMS: CUR stores full ARNs like arn:aws:kms:ca-central-1:1234567890123:key/eeeeeee-bbbb-45f9-add2-e18f786e47eb
                return arn
            
            elif service_type in ['secretsmanager', 'secrets-manager']:
                # Secrets Manager: CUR stores full ARNs like arn:aws:secretsmanager:us-east-1:123456789012:secret:my-app-secret-AbCdEf
                return arn
            
            elif service_type in ['acm', 'certificate-manager']:
                # Certificate Manager: Keep full ARN (already correct)
                return arn
            
            elif service_type == 'cognito':
                # Cognito: CUR stores full ARNs like arn:aws:cognito-idp:us-east-1:1234567890123:userpool/us-east-1_fEUbKGNbi
                return arn
            
            # Analytics & ML - ALL use full ARNs in CUR  
            elif service_type == 'sagemaker':
                # SageMaker: CUR stores full ARNs like arn:aws:sagemaker:us-east-1:1234567890123:endpoint/hf-text2text-flan-t5-xl-2024-03-12-15-40-57-004
                return arn
            
            elif service_type == 'glue':
                # Glue: CUR stores full ARNs like arn:aws:glue:us-east-1:1234567890123:crawler/burnout_v2
                return arn
            
            elif service_type == 'comprehend':
                # Comprehend: CUR stores full ARNs like arn:aws:comprehend:us-east-1:1234567890123:sentiment-detection-job/6e6fd91be28ee3434f905a8f03310921
                return arn
            
            elif service_type == 'bedrock':
                # Bedrock: CUR stores full ARNs like arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-sonnet-20240229-v1:0
                return arn
            
            elif service_type == 'kendra':
                # Kendra: CUR stores full ARNs like arn:aws:kendra:us-east-1:1234567890123:index/e7c27357-da61-4632-8d7c-21f36eab7195
                return arn
            
            elif service_type in ['elasticsearch', 'es']:
                # Elasticsearch: Likely uses full ARNs
                return arn
            
            elif service_type == 'opensearch':
                # OpenSearch: Likely uses full ARNs
                return arn
            
            # Management & Governance - ALL use full ARNs in CUR
            elif service_type == 'cloudformation':
                # CloudFormation: CUR stores full ARNs with resource details
                return arn
            
            elif service_type == 'cloudtrail':
                # CloudTrail: CUR stores full ARNs like arn:aws:cloudtrail:us-east-1:1234567890123:eventdatastore/6a07f7d6-9b67-44e7-b320-911a9e0cbfd8
                return arn
            
            elif service_type in ['logs', 'cloudwatch-logs']:
                # CloudWatch Logs: CUR stores full ARNs like arn:aws:logs:us-east-1:1234567890123:log-group:/aws/lambda/SpringClean-XUG3HH5R-AutoUpdateLambda-1LP842Z3Z6S6A
                return arn
            
            # === SERVICES WITH MIXED FORMATS ===
            
            elif service_type == 'ec2':
                # EC2: Mixed format based on resource type (confirmed from CUR data)
                if ':snapshot/' in arn:
                    return arn  # Snapshots use full ARN: arn:aws:ec2:us-east-2:1234567890123:snapshot/snap-07078301551d3c44a
                elif ':network-interface/' in arn:
                    return arn  # ENIs use full ARN: arn:aws:ec2:us-east-1:1234567890123:network-interface/eni-0db813375f62dfa5b
                elif ':elastic-ip/' in arn:
                    return arn  # EIPs use full ARN: arn:aws:ec2:us-east-1:1234567890123:elastic-ip/eipalloc-0dd17409395f83a27
                elif ':vpc-endpoint/' in arn:
                    return arn  # VPC Endpoints use full ARN
                elif ':instance/' in arn:
                    return arn.split('/')[-1]  # Instances use ID only: i-097aa0878893f61c4
                elif ':volume/' in arn:
                    return arn.split('/')[-1]  # Volumes use ID only: vol-0a1ecf9bc082885a5
                elif ':image/' in arn:
                    return arn.split('/')[-1]  # AMIs likely use ID only
                elif ':security-group/' in arn:
                    return arn.split('/')[-1]  # Security groups likely use ID only
                elif ':subnet/' in arn:
                    return arn.split('/')[-1]  # Subnets likely use ID only
                elif ':vpc/' in arn:
                    return arn.split('/')[-1]  # VPCs likely use ID only
                elif ':internet-gateway/' in arn:
                    return arn.split('/')[-1]  # IGWs likely use ID only
                elif ':route-table/' in arn:
                    return arn.split('/')[-1]  # Route tables likely use ID only
                else:
                    return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            elif service_type == 's3':
                # S3: Dual format - newer CUR uses full ARNs, legacy uses bucket names
                # From CUR data: both "config-bucket-1234567890123" AND "arn:aws:s3:::burnout-sentiment-analysis"
                if arn.startswith('arn:aws:s3:::'):
                    # Already in full ARN format (2025+ format)
                    return arn
                elif ':::' in arn:
                    # Standard S3 ARN format
                    bucket_part = arn.split(':::')[-1]
                    # For 2025+ compatibility, return as full ARN
                    return arn if arn.startswith('arn:') else f"arn:aws:s3:::{bucket_part.split('/')[0]}"
                else:
                    # Legacy bucket name format - convert to full ARN for compatibility
                    return f"arn:aws:s3:::{arn}" if not arn.startswith('arn:') else arn
            
            # === SERVICES WITH SPECIAL ID EXTRACTION ===
            
            elif service_type in ['systems-manager', 'ssm']:
                # Systems Manager: CUR stores UUID only like 537c8ff7-6b61-4c37-87bc-973b7747a082
                return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            # === SERVICES THAT KEEP EXISTING LOGIC ===
            
            elif service_type == 'ecs':
                # ECS/Fargate: Keep full ARN (already correct)
                if ':task/' in arn or ':service/' in arn:
                    return arn
                else:
                    return arn
            
            elif service_type == 'batch':
                # Batch: Keep full ARN (already correct)
                if ':job/' in arn:
                    return arn
                else:
                    return arn
            
            elif service_type == 'lightsail':
                # Lightsail: instance-name (likely still ID-based)
                return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            elif service_type in ['ebs', 'ec2-volume']:
                # EBS: vol-1234567890abcdef0 (same as EC2 volumes)
                return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            elif service_type == 'efs':
                # EFS: CUR stores full ARNs like arn:aws:elasticfilesystem:us-east-1:1234567890123:file-system/fs-0e3aaa3e5ac9a3a2c
                return arn
            
            elif service_type == 'fsx':
                # FSx: Likely uses full ARNs
                return arn
            
            elif service_type == 'elasticache':
                # ElastiCache: Likely uses full ARNs or keep existing logic
                if ':cluster:' in arn:
                    return arn.split(':')[-1]
                else:
                    return arn.split(':')[-1]
            
            elif service_type == 'redshift':
                # Redshift: Likely uses full ARNs
                return arn
            
            elif service_type in ['documentdb', 'docdb']:
                # DocumentDB: Likely uses full ARNs
                return arn
            
            elif service_type == 'neptune':
                # Neptune: CUR shows database instances with full ARNs
                return arn
            
            elif service_type == 'qldb':
                # QLDB: Likely uses full ARNs
                return arn
            
            elif service_type == 'timestream':
                # Timestream: Likely uses full ARNs
                return arn
            
            # Networking & Content Delivery
            elif service_type == 'cloudfront':
                # CloudFront: May use full ARNs, keep existing for now
                return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            elif service_type == 'route53':
                # Route 53: May use full ARNs, keep existing for now
                return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            elif service_type in ['elb', 'elasticloadbalancing']:
                # ELB: CUR stores full ARNs like arn:aws:elasticloadbalancing:us-east-1:1234567890123:loadbalancer/app/test-alb/c4f8c1a0ff17f39e
                return arn
            
            elif service_type in ['nat-gateway', 'natgw']:
                # NAT Gateway: May use full ARNs
                return arn
            
            elif service_type in ['vpc-endpoint', 'vpce']:
                # VPC Endpoint: CUR stores full ARNs like arn:aws:ec2:us-east-1:1234567890123:vpc-endpoint/vpce-0aedea2dc574d6a71
                return arn
            
            elif service_type in ['elastic-ip', 'eip']:
                # Elastic IP: Already handled in EC2 section (full ARN)
                return arn
            
            elif service_type in ['transit-gateway', 'tgw']:
                # Transit Gateway: Likely uses full ARNs
                return arn
            
            elif service_type in ['direct-connect', 'dx']:
                # Direct Connect: Likely uses full ARNs
                return arn
            
            elif service_type == 'vpn':
                # VPN: Likely uses full ARNs
                return arn
            
            elif service_type == 'globalaccelerator':
                # Global Accelerator: Keep full ARN
                return arn
            
            elif service_type == 'iam':
                # IAM: Usually empty in CUR, but extract name if present
                return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            elif service_type == 'waf':
                # WAF: web-acl-id
                return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            elif service_type == 'emr':
                # EMR: cluster-id
                return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            elif service_type in ['kinesis', 'kinesis-data-streams']:
                # Kinesis Data Streams: stream-name
                return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            elif service_type in ['kinesis-firehose', 'firehose']:
                # Kinesis Data Firehose: delivery-stream-name
                return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            elif service_type in ['kinesis-analytics', 'kinesisanalytics']:
                # Kinesis Analytics: application-name
                return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            elif service_type in ['apigateway', 'api-gateway']:
                # API Gateway: api-id
                return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            elif service_type == 'appsync':
                # AppSync: api-id
                return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            # Developer Tools
            elif service_type == 'codebuild':
                # CodeBuild: project-name
                return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            elif service_type == 'codedeploy':
                # CodeDeploy: application-name
                return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            elif service_type == 'codepipeline':
                # CodePipeline: pipeline-name
                return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            elif service_type == 'codecommit':
                # CodeCommit: repository-name
                return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            elif service_type == 'cloud9':
                # Cloud9: environment-id
                return arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            
            # Generic fallback for other services
            else:
                # Try to extract resource identifier intelligently
                if '/' in arn:
                    return arn.split('/')[-1]
                elif ':' in arn:
                    parts = arn.split(':')
                    # Skip empty parts and return the last meaningful part
                    for part in reversed(parts):
                        if part.strip():
                            return part
                    return arn
                else:
                    return arn
        
        except Exception as e:
            logger.debug(f"Error normalizing ARN '{arn}' for service '{service_type}': {e}")
            return arn  # Return original if normalization fails
    
    def _group_resources_by_service(self, resources: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Group resources by service type for efficient processing
        
        Args:
            resources: List of resources to group
            
        Returns:
            Dict[str, List[Dict]]: Resources grouped by service type
        """
        service_groups = {}
        
        for resource in resources:
            # The cost coordinator emits resource_type; retain service_type for
            # callers that use the matcher's original input contract.
            service_type = (
                resource.get('service_type')
                or resource.get('resource_type')
                or 'unknown'
            )
            if service_type not in service_groups:
                service_groups[service_type] = []
            service_groups[service_type].append(resource)
        
        logger.debug(f"Grouped {len(resources)} resources into {len(service_groups)} service types")
        return service_groups
    
    def _batch_resources(self, resources: List[Dict], batch_size: int):
        """
        Split resources into batches for processing
        
        Args:
            resources: List of resources to batch
            batch_size: Maximum resources per batch
            
        Yields:
            List[Dict]: Batches of resources
        """
        for i in range(0, len(resources), batch_size):
            yield resources[i:i + batch_size]
    

    def _create_empty_cost_results(self, resources: List[Dict]) -> List[Dict]:
        """
        Create results with zero costs for resources (fallback)
        
        Args:
            resources: Original resources
            
        Returns:
            List[Dict]: Resources with empty cost data
        """
        return [
            {
                **resource,
                'monthly_cost': 0.0,
                'usage_records': 0,
                'avg_daily_cost': 0.0,
                'first_usage': '',
                'last_usage': '',
                'matched_in_cur': False,
                'normalized_arn': ''
            }
            for resource in resources
        ]
    
    def get_supported_services(self) -> List[str]:
        """
        Get list of AWS services supported for ARN normalization
        
        Returns:
            List[str]: Supported service types
        """
        return [
            # Compute Services
            'ec2', 'lambda', 'ecs', 'batch', 'lightsail',
            
            # Storage Services  
            's3', 'ebs', 'ec2-volume', 'efs', 'fsx',
            
            # Database Services
            'rds', 'aurora', 'dynamodb', 'elasticache', 'redshift',
            'documentdb', 'docdb', 'neptune', 'qldb', 'timestream',
            
            # Networking & Content Delivery
            'cloudfront', 'route53', 'elb', 'elasticloadbalancing',
            'nat-gateway', 'natgw', 'vpc-endpoint', 'vpce', 'elastic-ip', 'eip',
            'transit-gateway', 'tgw', 'direct-connect', 'dx', 'vpn', 'globalaccelerator',
            
            # Security & Identity
            'iam', 'cognito', 'kms', 'secretsmanager', 'secrets-manager',
            'acm', 'certificate-manager', 'waf',
            
            # Analytics & ML
            'emr', 'kinesis', 'kinesis-data-streams', 'kinesis-firehose', 'firehose',
            'kinesis-analytics', 'kinesisanalytics', 'glue', 'sagemaker',
            'elasticsearch', 'es', 'opensearch',
            
            # Application Integration
            'sqs', 'sns', 'apigateway', 'api-gateway', 'appsync',
            'stepfunctions', 'step-functions',
            
            # Developer Tools
            'codebuild', 'codedeploy', 'codepipeline', 'codecommit', 'cloud9',
            
            # Management & Governance
            'cloudtrail', 'cloudformation',
            
            # Generic/Unknown
            'unknown'
        ]
    
    def test_arn_normalization(self, test_arns: List[Tuple[str, str]]) -> Dict:
        """
        Test ARN normalization for various service types
        
        Args:
            test_arns: List of (arn, expected_service_type) tuples
            
        Returns:
            Dict: Test results with normalization examples
        """
        results = {
            'test_count': len(test_arns),
            'normalization_results': [],
            'supported_services': self.get_supported_services()
        }
        
        for arn, service_type in test_arns:
            normalized = self._normalize_arn_for_cur(arn, service_type)
            results['normalization_results'].append({
                'original_arn': arn,
                'service_type': service_type,
                'normalized_arn': normalized,
                'transformation': f"{arn} -> {normalized}"
            })
        
        return results


if __name__ == "__main__":
    # Comprehensive test/demo
    matcher = CURDirectMatcher()
    
    print("🧪 Testing CUR Direct Matcher with Comprehensive ARN Normalization...")
    
    # Comprehensive test cases covering major fixes
    test_arns = [
        # Compute Services
        ("arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0", "ec2"),
        ("arn:aws:ec2:us-east-1:123456789012:volume/vol-1234567890abcdef0", "ec2"),
        ("arn:aws:lambda:us-east-1:123456789012:function:my-function", "lambda"),
        ("arn:aws:lambda:us-east-1:123456789012:function:my-function:PROD", "lambda"),  # Alias
        ("arn:aws:ecs:us-east-1:123456789012:task/cluster-name/task-id", "ecs"),  # Keep full ARN
        
        # Storage Services
        ("arn:aws:s3:::my-bucket", "s3"),
        ("arn:aws:s3:::my-bucket/folder/file.jpg", "s3"),  # Should extract just bucket
        
        # Database Services - THE CRITICAL FIXES
        ("arn:aws:rds:us-east-1:123456789012:db:my-database", "rds"),
        ("arn:aws:rds:us-east-1:123456789012:cluster:aurora-cluster", "rds"),
        ("arn:aws:rds:us-east-1:123456789012:snapshot:db-snapshot-2024", "rds"),  # FIXED!
        ("arn:aws:rds:us-east-1:123456789012:cluster-snapshot:cluster-snap-2024", "rds"),  # FIXED!
        ("arn:aws:dynamodb:us-east-1:123456789012:table/my-table", "dynamodb"),
        
        # Load Balancers - MAJOR FIXES
        ("arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/my-alb/1234567890123456", "elb"),  # ALB format
        ("arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/net/my-nlb/1234567890123456", "elb"),  # NLB format
        ("arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/classic/my-clb", "elb"),  # Classic
        
        # Networking & Content Delivery
        ("arn:aws:cloudfront::123456789012:distribution/E1234567890ABC", "cloudfront"),
        ("arn:aws:route53:::hostedzone/Z1234567890ABC", "route53"),
        
        # Application Integration
        ("arn:aws:sqs:us-east-1:123456789012:queue-name", "sqs"),
        ("arn:aws:sns:us-east-1:123456789012:topic-name", "sns"),
        
        # Analytics & ML
        ("arn:aws:kinesis:us-east-1:123456789012:stream/stream-name", "kinesis"),
        ("arn:aws:glue:us-east-1:123456789012:job/job-name", "glue"),
        
        # Security & Identity
        ("arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012", "kms"),
        ("arn:aws:acm:us-east-1:123456789012:certificate/12345678-1234-1234-1234-123456789012", "acm"),  # Keep full ARN
    ]
    
    test_results = matcher.test_arn_normalization(test_arns)
    print(f"✅ ARN normalization test completed:")
    print(f"   Supported services: {len(test_results['supported_services'])}")
    print(f"   Test cases: {test_results['test_count']}")
    print()
    
    # Group results by service type for better display
    service_results = {}
    for result in test_results['normalization_results']:
        service = result['service_type']
        if service not in service_results:
            service_results[service] = []
        service_results[service].append(result)
    
    # Display results grouped by service
    for service_type, results in service_results.items():
        print(f"📋 {service_type.upper()} Service:")
        for result in results:
            original = result['original_arn']
            normalized = result['normalized_arn']
            # Highlight transformations
            if original != normalized:
                print(f"   ✅ {original}")
                print(f"      → {normalized}")
            else:
                print(f"   🔄 {original} (kept as-is)")
        print()
    
    
    print(f"🚀 CUR Direct Matcher updated with ACTUAL CUR format matching")
    print(f"📊 Total supported services: {len(test_results['supported_services'])}")
    print(f"🎯 CUR Available: {matcher.cur_available}")

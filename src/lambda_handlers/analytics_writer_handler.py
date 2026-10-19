"""
Analytics Writer Lambda Handler

Receives delta notifications from the aggregator worker via central SQS queue,
reads delta JSON from the central analytics S3 bucket, converts to Parquet format,
and writes back to the central S3 analytics bucket with proper partitioning for Athena queries.

Architecture:
- Triggered by: Central ResultQueue (SQS)
- Reads from: Central analytics S3 bucket (deltas written by aggregator worker)
- Writes to: Central S3 analytics bucket (Parquet, partitioned)
- Partitioning: account_id/year/month/day

Cost Optimization:
- Parquet format: 10x compression vs JSON
- Batch processing: Multiple deltas per invocation
- Partition pruning: Efficient Athena queries
"""

import json
import gzip
import os
import time
import logging
import hashlib
from datetime import datetime
from typing import Dict, List, Any, Optional
from io import BytesIO

import boto3
from botocore.exceptions import ClientError

# Optional: jsonschema for delta input validation (install via requirements-lambda.txt)
try:
    import jsonschema
    HAS_JSONSCHEMA = True
except ImportError:
    HAS_JSONSCHEMA = False
    logging.warning("jsonschema not available - schema validation disabled")

# Optional dependencies (include in Lambda layer or deployment package)
try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False
    logging.warning("Parquet libraries not available - will use JSON fallback")

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# AWS clients
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')
cloudwatch = boto3.client('cloudwatch')
glue_client = boto3.client('glue')

# Environment variables
CENTRAL_ANALYTICS_BUCKET = os.environ.get('CENTRAL_ANALYTICS_BUCKET')
ATHENA_DATABASE = os.environ.get('ATHENA_DATABASE')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'prod')

# ── Delta input validation constants ─────────────────────────────────────────────
MAX_DELTA_FILE_SIZE_MB = int(os.environ.get('MAX_DELTA_FILE_SIZE_MB', '10'))
MAX_DELTA_FILE_SIZE_BYTES = MAX_DELTA_FILE_SIZE_MB * 1024 * 1024
QUARANTINE_PREFIX = 'quarantine/'

# Load JSON schema for delta validation
_DELTA_SCHEMA = None

def _load_delta_schema():
    """Load delta JSON schema for validation. Cached after first load."""
    global _DELTA_SCHEMA
    if _DELTA_SCHEMA is not None:
        return _DELTA_SCHEMA
    try:
        import os as _os
        schema_path = _os.path.join(_os.path.dirname(__file__), '../../schemas/delta_schema.json')
        schema_path = _os.path.abspath(schema_path)
        if _os.path.exists(schema_path):
            with open(schema_path) as f:
                _DELTA_SCHEMA = json.load(f)
            logger.info("Delta schema loaded for validation")
        else:
            logger.warning(f"Delta schema not found at {schema_path}, validation disabled")
    except Exception as e:
        logger.warning(f"Could not load delta schema: {e}, validation disabled")
    return _DELTA_SCHEMA


def _validate_delta_file(data: bytes, source_key: str) -> Optional[List[Dict]]:
    """
    Validate delta file size, JSON parsability, and schema.

    Returns:
        List[Dict]: Validated deltas if valid
        None: If invalid (file will be quarantined)
    """
    # 1. Size check
    if len(data) > MAX_DELTA_FILE_SIZE_BYTES:
        logger.error(
            f"DELTA REJECTED: Delta file too large: {len(data):,} bytes "
            f"(max {MAX_DELTA_FILE_SIZE_BYTES:,}). Key: {source_key}"
        )
        emit_metric('OversizedDeltaFileRejected', 1)
        return None

    # 2. JSON parsability
    try:
        deltas = json.loads(data.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.error(f"DELTA REJECTED: Invalid JSON in delta file: {e}. Key: {source_key}")
        emit_metric('InvalidJsonDeltaFileRejected', 1)
        return None

    # 3. Basic structure check
    if not isinstance(deltas, list):
        logger.error(f"DELTA REJECTED: Delta file must be a JSON array. Key: {source_key}")
        emit_metric('InvalidStructureDeltaFileRejected', 1)
        return None

    # 4. Schema validation (if jsonschema available)
    if HAS_JSONSCHEMA:
        schema = _load_delta_schema()
        if schema:
            try:
                jsonschema.validate(instance=deltas, schema=schema)
            except jsonschema.ValidationError as e:
                logger.error(
                    f"DELTA REJECTED: Delta file schema validation failed: "
                    f"{e.message} at {list(e.path)}. Key: {source_key}"
                )
                emit_metric('SchemaValidationFailedDeltaFile', 1)
                return None
            except jsonschema.SchemaError as e:
                # Schema itself is broken — log but continue (defensive)
                logger.warning(f"Schema definition error (skipping validation): {e}")

    return deltas


def _quarantine_malicious_file(bucket: str, key: str):
    """
    Move rejected delta file to quarantine prefix for security review.
    Non-fatal: logs warning if quarantine fails.
    """
    try:
        quarantine_key = f"{QUARANTINE_PREFIX}{datetime.utcnow().strftime('%Y/%m/%d/')}{key.replace('/', '_')}"
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': key},
            Key=quarantine_key,
            Metadata={'original_key': key, 'quarantined_at': datetime.utcnow().isoformat()},
            MetadataDirective='REPLACE'
        )
        s3_client.delete_object(Bucket=bucket, Key=key)
        logger.warning(f"Quarantined malicious delta file: {key} → {quarantine_key}")
        emit_metric('DeltaFilesQuarantined', 1)
    except Exception as e:
        logger.warning(f"Could not quarantine file {key}: {e} (file remains in place)")


# Retry configuration for S3 replication delays
MAX_RETRIES = 3
RETRY_DELAYS = {
    0: 300,   # First retry: 5 minutes
    1: 600,   # Second retry: 10 minutes  
    2: 900    # Third retry: 15 minutes
}


def lambda_handler(event, context):
    """
    Process SQS messages with delta S3 locations
    Handles S3 replication delays with progressive retry logic
    Implements deduplication to prevent duplicate writes
    
    Args:
        event: SQS event with Records containing delta notifications
        context: Lambda context
        
    Returns:
        Dict: Processing results
    """
    try:
        logger.info(f"Analytics Writer started: {len(event.get('Records', []))} messages")
        
        processed = 0
        failed = 0
        retried = 0
        duplicates = 0
        total_deltas = 0
        
        for record in event.get('Records', []):
            try:
                # Parse SQS message
                message = json.loads(record['body'])
                
                # Extract correlation ID from message body or attributes
                correlation_id = message.get('correlation_id')
                if not correlation_id:
                    # Try message attributes as fallback
                    attributes = record.get('messageAttributes', {})
                    corr_attr = attributes.get('CorrelationId', {})
                    correlation_id = corr_attr.get('stringValue', 'unknown')
                
                # Extract retry count from message attributes
                retry_count = get_retry_count(record)
                
                logger.info(f"[{correlation_id}] Processing message from region: {message.get('region')}, "
                          f"account: {message.get('account_id')}, "
                          f"changed: {message.get('changed_count')}, "
                          f"retry_count: {retry_count}")
                
                # Read deltas from regional S3 (handles replication delays)
                deltas, replication_pending = read_deltas_from_regional_s3_with_retry(
                    bucket=message['s3_bucket'],
                    key=message['s3_key'],
                    region=message['region']
                )
                
                # Handle replication delay - re-queue with progressive delay
                if replication_pending:
                    if retry_count < MAX_RETRIES:
                        handle_replication_delay(message, record, retry_count)
                        retried += 1
                        logger.info(f"Message re-queued for retry {retry_count + 1}/{MAX_RETRIES}")
                        continue
                    else:
                        logger.error(f"Max retries ({MAX_RETRIES}) exceeded - file still not replicated")
                        emit_metric('ReplicationTimeout', 1)
                        raise Exception(f"S3 replication timeout after {MAX_RETRIES} retries")
                
                if not deltas:
                    logger.warning(f"No deltas found (may have been processed already)")
                    processed += 1
                    continue
                
                # Write to central analytics bucket with deduplication
                write_result = write_deltas_to_central_s3_with_dedup(deltas, message)
                
                if write_result.get('duplicate'):
                    duplicates += 1
                    logger.info(f"Duplicate write prevented: {len(deltas)} deltas")
                else:
                    total_deltas += len(deltas)
                
                processed += 1
                
                logger.info(f"Successfully processed {len(deltas)} deltas from {message['region']}")
                
            except Exception as e:
                failed += 1
                logger.error(f"Error processing SQS message: {str(e)}", exc_info=True)
                # Re-raise to trigger SQS retry (which will go to DLQ after maxReceiveCount)
                raise
        
        result = {
            'statusCode': 200,
            'processed_messages': processed,
            'failed_messages': failed,
            'retried_messages': retried,
            'duplicate_writes_prevented': duplicates,
            'total_deltas_written': total_deltas
        }
        
        logger.info(f"Analytics Writer completed: {result}")
        
        # Emit CloudWatch metrics
        emit_metric('ProcessedMessages', processed)
        emit_metric('DuplicateWritesPrevented', duplicates)
        emit_metric('RetriedMessages', retried)
        
        return result
        
    except Exception as e:
        logger.error(f"Fatal error in Analytics Writer: {str(e)}", exc_info=True)
        raise


def read_deltas_from_regional_s3(bucket: str, key: str, region: str) -> List[Dict]:
    """
    Read delta JSON from CENTRAL analytics bucket (replicated from regional)
    
    S3 Replication Architecture:
    - Regional worker writes to: s3://ti-state-{region}-{env}-{account}/deltas/...
    - S3 replicates to: s3://ti-central-analytics-{hub-region}-{env}-{hub-account}/deltas/...
    - Analytics writer reads from central bucket (same account, same region = no DTO costs)
    
    Args:
        bucket: IGNORED - kept for backward compatibility with SQS message format
        key: S3 key to delta JSON file (same path structure after replication)
        region: Source region for logging
        
    Returns:
        List[Dict]: List of delta records
    """
    try:
        # CHANGE: Read from CENTRAL bucket instead of regional spoke bucket
        # The key path is preserved during replication: deltas/timestamp-changes.json
        central_key = key  # Same key structure after replication
        
        logger.debug(f"Reading replicated deltas from s3://{CENTRAL_ANALYTICS_BUCKET}/{central_key}")
        logger.debug(f"Original regional location was: s3://{bucket}/{key} (region: {region})")
        
        # Read from central analytics bucket (local, no cross-account/cross-region access)
        response = s3_client.get_object(
            Bucket=CENTRAL_ANALYTICS_BUCKET,  # Read from central, not regional
            Key=central_key
        )
        
        # Read and parse JSON
        data = response['Body'].read().decode('utf-8')
        deltas = json.loads(data)
        
        logger.info(f"Read {len(deltas)} deltas from central S3 (replicated from {region})")
        return deltas
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            logger.warning(f"Delta file not found in central bucket: {CENTRAL_ANALYTICS_BUCKET}/{central_key}")
            logger.warning(f"This may indicate S3 replication has not completed yet from {region}")
            logger.warning(f"Original location: s3://{bucket}/{key}")
            return []
        elif error_code == 'AccessDenied':
            logger.error(f"Access denied to central analytics bucket: {CENTRAL_ANALYTICS_BUCKET}")
            logger.error(f"This should not happen - Lambda should have permissions to central bucket")
            raise
        else:
            logger.error(f"S3 error reading deltas from central bucket: {error_code}")
            raise
    except Exception as e:
        logger.error(f"Error reading deltas from central S3: {str(e)}")
        raise


def write_deltas_to_central_s3(deltas: List[Dict], message: Dict):
    """
    Write deltas to central S3 in Parquet format with proper partitioning
    
    Args:
        deltas: List of delta records
        message: Original SQS message with metadata
    """
    try:
        # Extract partition keys
        account_id = message.get('account_id')
        if not account_id:
            # Fallback: extract from first delta ARN
            account_id = extract_account_from_arn(deltas[0]['arn'])
        
        timestamp = datetime.fromtimestamp(message['timestamp'])
        
        # Build partition path: account_id/year/month/day
        partition_path = (
            f"account_id={account_id}/"
            f"year={timestamp.year}/"
            f"month={timestamp.month}/"
            f"day={timestamp.day}/"
        )
        
        # Write as Parquet (preferred) or JSON (fallback)
        if HAS_PARQUET:
            write_parquet(deltas, partition_path, timestamp)
        else:
            write_json(deltas, partition_path, timestamp)
        
        # Register partition with Glue (for immediate query availability)
        register_partition(account_id, timestamp.year, timestamp.month, timestamp.day)
        
        logger.info(f"Wrote {len(deltas)} deltas to partition: {partition_path}")
        
    except Exception as e:
        logger.error(f"Error writing deltas to central S3: {str(e)}")
        raise


def write_parquet(deltas: List[Dict], partition_path: str, timestamp: datetime):
    """Write deltas as Parquet file"""
    try:
        # Define explicit PyArrow schema that matches Athena table definition
        # This prevents schema inference issues with empty maps
        schema = pa.schema([
            ('arn', pa.string()),
            ('resource_type', pa.string()),
            ('tags', pa.map_(pa.string(), pa.string())),  # MAP<STRING,STRING>
            ('previous_tags', pa.map_(pa.string(), pa.string())),  # MAP<STRING,STRING>
            ('change_type', pa.string()),
            ('timestamp', pa.int64()),
            ('source_system', pa.string()),
            ('region', pa.string()),
            ('scan_id', pa.string())
        ])
        
        # Prepare data with proper defaults for missing columns
        prepared_deltas = []
        for delta in deltas:
            prepared = {
                'arn': delta.get('arn'),
                'resource_type': delta.get('resource_type'),
                'tags': delta.get('tags', {}),  # Keep empty dict, don't convert to None
                'previous_tags': delta.get('previous_tags', {}),  # Keep empty dict
                'change_type': delta.get('change_type'),
                'timestamp': delta.get('timestamp'),
                'source_system': delta.get('source_system'),
                'region': delta.get('region'),
                'scan_id': delta.get('scan_id')
            }
            prepared_deltas.append(prepared)
        
        # Convert to DataFrame
        df = pd.DataFrame(prepared_deltas)
        
        # Convert to PyArrow table with explicit schema
        # This ensures MAP types are preserved correctly
        table = pa.Table.from_pandas(df, schema=schema)
        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer, compression='snappy')
        parquet_buffer.seek(0)
        
        # Write to S3
        s3_key = (
            f"tag-deltas/recent/{partition_path}"
            f"deltas-{timestamp.strftime('%H-%M-%S')}.parquet"
        )
        
        s3_client.put_object(
            Bucket=CENTRAL_ANALYTICS_BUCKET,
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        
        logger.debug(f"Wrote Parquet: s3://{CENTRAL_ANALYTICS_BUCKET}/{s3_key}")
        
    except Exception as e:
        logger.error(f"Error writing Parquet: {str(e)}")
        raise


def write_json(deltas: List[Dict], partition_path: str, timestamp: datetime):
    """Write deltas as JSON (fallback when Parquet not available)"""
    try:
        # Write to S3 as JSON
        s3_key = (
            f"tag-deltas/recent/{partition_path}"
            f"deltas-{timestamp.strftime('%H-%M-%S')}.json"
        )
        
        s3_client.put_object(
            Bucket=CENTRAL_ANALYTICS_BUCKET,
            Key=s3_key,
            Body=json.dumps(deltas),
            ContentType='application/json'
        )
        
        logger.debug(f"Wrote JSON: s3://{CENTRAL_ANALYTICS_BUCKET}/{s3_key}")
        
    except Exception as e:
        logger.error(f"Error writing JSON: {str(e)}")
        raise


def register_partition(account_id: str, year: int, month: int, day: int):
    """
    Register partition with Glue for immediate Athena query availability
    
    Args:
        account_id: AWS account ID
        year: Year partition value
        month: Month partition value
        day: Day partition value
    """
    try:
        # Check if partition already exists
        try:
            glue_client.get_partition(
                DatabaseName=ATHENA_DATABASE,
                TableName='resource_tag_deltas_recent',
                PartitionValues=[account_id, str(year), str(month), str(day)]
            )
            logger.debug(f"Partition already exists: {account_id}/{year}/{month}/{day}")
            return  # Partition exists, no need to create
        except glue_client.exceptions.EntityNotFoundException:
            pass  # Partition doesn't exist, create it
        
        # Create partition
        partition_location = (
            f"s3://{CENTRAL_ANALYTICS_BUCKET}/tag-deltas/recent/"
            f"account_id={account_id}/year={year}/month={month}/day={day}/"
        )
        
        glue_client.create_partition(
            DatabaseName=ATHENA_DATABASE,
            TableName='resource_tag_deltas_recent',
            PartitionInput={
                'Values': [account_id, str(year), str(month), str(day)],
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
        
        logger.debug(f"Registered partition: {account_id}/{year}/{month}/{day}")
        
    except glue_client.exceptions.AlreadyExistsException:
        logger.debug(f"Partition already exists (race condition): {account_id}/{year}/{month}/{day}")
    except Exception as e:
        # Non-fatal - partition will be discovered on next MSCK REPAIR TABLE
        logger.warning(f"Could not register partition: {str(e)}")


def extract_account_from_arn(arn: str) -> str:
    """
    Extract account ID from AWS ARN
    
    Args:
        arn: AWS resource ARN
        
    Returns:
        str: Account ID
    """
    try:
        # ARN format: arn:aws:service:region:account-id:resource
        parts = arn.split(':')
        if len(parts) >= 5:
            return parts[4]
        else:
            logger.warning(f"Invalid ARN format: {arn}")
            return 'unknown'
    except Exception as e:
        logger.error(f"Error extracting account from ARN: {str(e)}")
        return 'unknown'


def read_deltas_from_regional_s3_with_retry(bucket: str, key: str, region: str) -> tuple:
    """
    Read delta JSON with retry logic for S3 replication delays
    
    Returns:
        tuple: (deltas, replication_pending)
            - deltas: List of delta records (empty if not found)
            - replication_pending: True if file not found (likely replication delay)
    """
    try:
        central_key = key
        
        logger.debug(f"Reading replicated deltas from s3://{CENTRAL_ANALYTICS_BUCKET}/{central_key}")
        
        response = s3_client.get_object(
            Bucket=CENTRAL_ANALYTICS_BUCKET,
            Key=central_key
        )

        raw_data = response['Body'].read()

        # Validate file before processing
        deltas = _validate_delta_file(raw_data, central_key)
        if deltas is None:
            # Validation failed — quarantine and skip this file
            _quarantine_malicious_file(CENTRAL_ANALYTICS_BUCKET, central_key)
            logger.error(f"Delta file failed validation and was quarantined: {central_key}")
            return [], False  # Return empty (not replication-pending — genuine rejection)

        logger.info(f"✓ Read {len(deltas)} deltas from central S3 (replicated from {region})")
        return deltas, False  # Found - not pending

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            logger.warning(f"⏳ Delta file not yet replicated: {CENTRAL_ANALYTICS_BUCKET}/{central_key}")
            logger.warning(f"   Original location: s3://{bucket}/{key} (region: {region})")
            emit_metric('ReplicationDelayDetected', 1)
            return [], True  # Not found - replication pending
        else:
            logger.error(f"S3 error reading deltas: {error_code}")
            raise
    except Exception as e:
        logger.error(f"Error reading deltas: {str(e)}")
        raise


def get_retry_count(record: Dict) -> int:
    """
    Extract retry count from SQS message attributes
    
    Args:
        record: SQS record
        
    Returns:
        int: Retry count (0 if not present)
    """
    try:
        attributes = record.get('messageAttributes', {})
        retry_attr = attributes.get('RetryCount', {})
        retry_count = int(retry_attr.get('stringValue', '0'))
        return retry_count
    except Exception as e:
        logger.warning(f"Could not extract retry count: {e}")
        return 0


def handle_replication_delay(message: Dict, record: Dict, retry_count: int):
    """
    Handle S3 replication delay by re-queuing message with progressive delay
    Transaction-safe: Only delete original after successful re-send
    
    Args:
        message: Original message body
        record: SQS record
        retry_count: Current retry count
    """
    try:
        # Calculate next delay
        next_delay = RETRY_DELAYS.get(retry_count, RETRY_DELAYS[MAX_RETRIES - 1])
        next_retry = retry_count + 1
        
        logger.info(f"Re-queueing message with {next_delay}s delay (retry {next_retry}/{MAX_RETRIES})")
        
        # Get queue URL from record
        queue_url = extract_queue_url_from_record(record)
        
        # Re-send message with delay and updated retry count
        # CRITICAL: Send first, delete second (transaction safety)
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message),
            DelaySeconds=next_delay,
            MessageAttributes={
                'RetryCount': {
                    'DataType': 'Number',
                    'StringValue': str(next_retry)
                },
                'OriginalTimestamp': {
                    'DataType': 'Number',
                    'StringValue': str(message.get('timestamp', int(time.time())))
                }
            }
        )
        
        new_message_id = response.get('MessageId')
        logger.info(f"✓ Message re-sent: {new_message_id}, delay={next_delay}s")
        
        # Only delete original after successful re-send
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=record['receiptHandle']
        )
        
        logger.info(f"✓ Original message deleted after successful re-send")
        
        # Emit metrics
        emit_metric('MessageRetried', 1)
        emit_metric('RetryDelay', next_delay, unit='Seconds')
        
    except Exception as e:
        logger.error(f"Failed to re-queue message: {str(e)}")
        # Don't delete message on error - let visibility timeout handle it
        raise


def extract_queue_url_from_record(record: Dict) -> str:
    """
    Extract SQS queue URL from event record
    
    Args:
        record: SQS record
        
    Returns:
        str: Queue URL
    """
    try:
        # Event source ARN format: arn:aws:sqs:region:account:queue-name
        event_source_arn = record.get('eventSourceARN', '')
        parts = event_source_arn.split(':')
        
        if len(parts) >= 6:
            region = parts[3]
            account = parts[4]
            queue_name = parts[5]
            queue_url = f"https://sqs.{region}.amazonaws.com/{account}/{queue_name}"
            return queue_url
        else:
            raise ValueError(f"Invalid event source ARN: {event_source_arn}")
            
    except Exception as e:
        logger.error(f"Could not extract queue URL: {e}")
        raise


def generate_content_hash(deltas: List[Dict]) -> str:
    """
    Generate deterministic content hash from delta data
    Ensures identical data always produces the same hash (idempotency)
    
    Args:
        deltas: List of delta records
        
    Returns:
        str: 16-character hex hash (SHA-256 truncated)
    """
    try:
        # Sort by ARN for deterministic ordering
        sorted_deltas = sorted(deltas, key=lambda x: x.get('arn', ''))
        
        # Create canonical representation (exclude processing metadata)
        canonical_deltas = []
        for d in sorted_deltas:
            canonical_deltas.append({
                'arn': d.get('arn'),
                'resource_type': d.get('resource_type'),
                'tags': d.get('tags', {}),
                'previous_tags': d.get('previous_tags', {}),
                'change_type': d.get('change_type')
            })
        
        # Generate hash
        canonical_json = json.dumps(canonical_deltas, sort_keys=True)
        content_hash = hashlib.sha256(canonical_json.encode()).hexdigest()[:16]
        
        return content_hash
        
    except Exception as e:
        logger.error(f"Error generating content hash: {e}")
        # Fallback to timestamp-based name
        return str(int(time.time()))


def write_deltas_to_central_s3_with_dedup(deltas: List[Dict], message: Dict) -> Dict:
    """
    Write deltas to central S3 with deduplication
    Uses content-based hashing and S3 HEAD to prevent duplicates
    
    Args:
        deltas: List of delta records
        message: Original SQS message with metadata
        
    Returns:
        Dict: Write result with 'duplicate' flag
    """
    try:
        # Extract partition keys
        account_id = message.get('account_id')
        if not account_id:
            account_id = extract_account_from_arn(deltas[0]['arn'])
        
        timestamp = datetime.fromtimestamp(message['timestamp'])
        
        # Build partition path
        partition_path = (
            f"account_id={account_id}/"
            f"year={timestamp.year}/"
            f"month={timestamp.month}/"
            f"day={timestamp.day}/"
        )
        
        # Generate content-based hash for deduplication
        content_hash = generate_content_hash(deltas)
        
        # Build S3 key with content hash
        s3_key = (
            f"tag-deltas/recent/{partition_path}"
            f"deltas-{content_hash}.parquet"
        )
        
        # Check if file already exists (deduplication)
        try:
            head_response = s3_client.head_object(
                Bucket=CENTRAL_ANALYTICS_BUCKET,
                Key=s3_key
            )
            
            file_size = head_response.get('ContentLength', 0)
            logger.info(f"✓ Duplicate detected and prevented: {s3_key}")
            logger.info(f"  Content hash: {content_hash}, {len(deltas)} deltas, {file_size:,} bytes")
            
            emit_metric('DuplicateWritesPrevented', 1)
            emit_metric('DuplicateDetectionLatency', 50, unit='Milliseconds')  # Approximate HEAD latency
            
            return {
                'duplicate': True,
                'key': s3_key,
                'content_hash': content_hash,
                'deltas_count': len(deltas)
            }
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                # File doesn't exist - proceed with write
                logger.info(f"✓ New write: {s3_key} (hash: {content_hash})")
            else:
                # Other S3 error - log but proceed
                logger.warning(f"S3 HEAD error: {e}, proceeding with write")
        
        # Write Parquet file
        if HAS_PARQUET:
            write_parquet_with_hash(deltas, partition_path, content_hash)
        else:
            write_json_with_hash(deltas, partition_path, content_hash)
        
        # Register partition with Glue
        register_partition(account_id, timestamp.year, timestamp.month, timestamp.day)
        
        logger.info(f"✓ Wrote {len(deltas)} deltas to partition: {partition_path}")
        
        emit_metric('NewFilesWritten', 1)
        
        return {
            'duplicate': False,
            'key': s3_key,
            'content_hash': content_hash,
            'deltas_count': len(deltas)
        }
        
    except Exception as e:
        logger.error(f"Error writing deltas with deduplication: {str(e)}")
        raise


def write_parquet_with_hash(deltas: List[Dict], partition_path: str, content_hash: str):
    """Write deltas as Parquet file with content-based filename"""
    try:
        schema = pa.schema([
            ('arn', pa.string()),
            ('resource_type', pa.string()),
            ('tags', pa.map_(pa.string(), pa.string())),
            ('previous_tags', pa.map_(pa.string(), pa.string())),
            ('change_type', pa.string()),
            ('timestamp', pa.int64()),
            ('source_system', pa.string()),
            ('region', pa.string()),
            ('scan_id', pa.string()),
        ])
        
        prepared_deltas = []
        for delta in deltas:
            prepared = {
                'arn': delta.get('arn'),
                'resource_type': delta.get('resource_type'),
                'tags': delta.get('tags', {}),
                'previous_tags': delta.get('previous_tags', {}),
                'change_type': delta.get('change_type'),
                'timestamp': delta.get('timestamp'),
                'source_system': delta.get('source_system'),
                'region': delta.get('region'),
                'scan_id': delta.get('scan_id'),
            }
            prepared_deltas.append(prepared)
        
        df = pd.DataFrame(prepared_deltas)
        table = pa.Table.from_pandas(df, schema=schema)
        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer, compression='snappy')
        parquet_buffer.seek(0)
        
        s3_key = f"tag-deltas/recent/{partition_path}deltas-{content_hash}.parquet"
        
        s3_client.put_object(
            Bucket=CENTRAL_ANALYTICS_BUCKET,
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        
        logger.debug(f"Wrote Parquet: s3://{CENTRAL_ANALYTICS_BUCKET}/{s3_key}")
        
    except Exception as e:
        logger.error(f"Error writing Parquet: {str(e)}")
        raise


def write_json_with_hash(deltas: List[Dict], partition_path: str, content_hash: str):
    """Write deltas as JSON with content-based filename"""
    try:
        s3_key = f"tag-deltas/recent/{partition_path}deltas-{content_hash}.json"
        
        s3_client.put_object(
            Bucket=CENTRAL_ANALYTICS_BUCKET,
            Key=s3_key,
            Body=json.dumps(deltas),
            ContentType='application/json'
        )
        
        logger.debug(f"Wrote JSON: s3://{CENTRAL_ANALYTICS_BUCKET}/{s3_key}")
        
    except Exception as e:
        logger.error(f"Error writing JSON: {str(e)}")
        raise


def emit_metric(metric_name: str, value: float, unit: str = 'Count'):
    """
    Emit CloudWatch metric for monitoring
    
    Args:
        metric_name: Metric name
        value: Metric value
        unit: Metric unit (Count, Seconds, Milliseconds, etc.)
    """
    try:
        cloudwatch.put_metric_data(
            Namespace='TagInsights/AnalyticsWriter',
            MetricData=[{
                'MetricName': metric_name,
                'Value': value,
                'Unit': unit,
                'Timestamp': datetime.utcnow()
            }]
        )
    except Exception as e:
        # Non-fatal - don't fail processing if metrics fail
        logger.warning(f"Failed to emit metric {metric_name}: {e}")

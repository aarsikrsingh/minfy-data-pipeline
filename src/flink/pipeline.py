import json
import logging
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
import boto3
from botocore.client import Config
import pyarrow as pa
import pyarrow.parquet as pq
import io

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka config
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'minfy.public.employee'
DLQ_TOPIC = 'minfy.public.employee.dlq'

# MinIO config
MINIO_ENDPOINT = 'http://localhost:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
BRONZE_BUCKET = 'bronze'
DLQ_BUCKET = 'dlq'

def get_minio_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4')
    )

def validate_record(record):
    """Validate the CDC record - returns (is_valid, error_message)"""
    after = record.get('payload', {}).get('after')
    
    if after is None:
        op = record.get('payload', {}).get('op')
        if op == 'd':
            return True, None  # deletes are valid
        return False, "after field is null for non-delete operation"
    
    if not after.get('emp_id'):
        return False, "emp_id is null or missing"
    
    if not after.get('name'):
        return False, "name is null or missing"
    
    if not after.get('dept_id'):
        return False, "dept_id is null or missing"

    return True, None

def write_to_bronze(minio_client, record, timestamp):
    """Write valid record to Bronze S3 layer"""
    now = datetime.utcnow()
    key = f"employee/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/{timestamp}.json"
    
    enriched = {
        **record,
        'pipeline_metadata': {
            'ingested_at': now.isoformat(),
            'source_system': 'debezium-postgresql',
            'layer': 'bronze',
            'pipeline_run_id': f"run_{timestamp}"
        }
    }
    
    minio_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=json.dumps(enriched).encode('utf-8'),
        ContentType='application/json'
    )
    logger.info(f"✅ Written to Bronze: {key}")

def write_to_dlq(minio_client, record, error_msg, timestamp):
    """Write failed record to DLQ"""
    key = f"employee/{timestamp}_failed.json"
    
    dlq_record = {
        'error_type': 'VALIDATION_FAILED',
        'error_msg': error_msg,
        'failed_at': datetime.utcnow().isoformat(),
        'original_payload': record
    }
    
    minio_client.put_object(
        Bucket=DLQ_BUCKET,
        Key=key,
        Body=json.dumps(dlq_record).encode('utf-8'),
        ContentType='application/json'
    )
    logger.warning(f"❌ Written to DLQ: {key} | Reason: {error_msg}")

def run_pipeline():
    logger.info("Starting Minfy Data Pipeline...")
    logger.info("Connecting to Kafka...")
    
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='minfy-flink-consumer'
    )
    
    minio_client = get_minio_client()
    logger.info("✅ Connected to Kafka and MinIO")
    logger.info("Watching for CDC events...")
    
    for message in consumer:
        try:
            record = message.value
            timestamp = str(int(datetime.utcnow().timestamp() * 1000))
            
            op = record.get('payload', {}).get('op', 'unknown')
            table = record.get('payload', {}).get('source', {}).get('table', 'unknown')
            
            logger.info(f"📨 Event received | table={table} | op={op}")
            
            is_valid, error_msg = validate_record(record)
            
            if is_valid:
                write_to_bronze(minio_client, record, timestamp)
            else:
                write_to_dlq(minio_client, record, error_msg, timestamp)
                
        except Exception as e:
            logger.error(f"Pipeline error: {str(e)}")

if __name__ == '__main__':
    run_pipeline()
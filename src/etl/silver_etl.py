import json
import logging
import boto3
from botocore.client import Config
from datetime import datetime
import base64

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MinIO config
MINIO_ENDPOINT = 'http://localhost:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
BRONZE_BUCKET = 'bronze'
SILVER_BUCKET = 'silver'

def get_minio_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4')
    )

def decode_salary(salary_raw):
    """Debezium encodes decimal as base64 bytes — decode it"""
    try:
        if isinstance(salary_raw, str):
            decoded = base64.b64decode(salary_raw)
            salary = int.from_bytes(decoded, byteorder='big') / 100
            return float(salary)
        return float(salary_raw)
    except:
        return 0.0

def extract_employee(record):
    """Extract clean employee record from raw Debezium CDC event"""
    payload = record.get('payload', {})
    op = payload.get('op')
    after = payload.get('after', {})
    before = payload.get('before', {})
    source = payload.get('source', {})

    if op == 'd':
        data = before
    else:
        data = after

    if not data:
        return None, op

    return {
        'emp_id': data.get('emp_id'),
        'name': data.get('name'),
        'email': data.get('email'),
        'dept_id': data.get('dept_id'),
        'salary': decode_salary(data.get('salary', 0)),
        'job_title': data.get('job_title'),
        'is_active': data.get('is_active', True),
        'operation': op,
        'source_table': source.get('table'),
        'source_db': source.get('db'),
        'event_timestamp': source.get('ts_ms'),
    }, op

def apply_scd_type2(minio_client, new_record):
    """
    SCD Type 2 logic:
    - Check if employee already exists in Silver
    - If yes: close old row (set effective_end_date)
    - Insert new row with new values
    """
    emp_id = new_record['emp_id']
    now = datetime.utcnow().isoformat()

    # Check for existing active record
    existing_key = f"employee/current/emp_{emp_id}.json"
    existing_record = None

    try:
        response = minio_client.get_object(Bucket=SILVER_BUCKET, Key=existing_key)
        existing_record = json.loads(response['Body'].read().decode('utf-8'))
        logger.info(f"Found existing record for emp_id={emp_id}")
    except:
        logger.info(f"No existing record for emp_id={emp_id} — first time insert")

    if existing_record:
        # Close old row — set effective_end_date
        existing_record['effective_end_date'] = now
        existing_record['is_current'] = False

        # Write closed record to history
        history_key = f"employee/history/emp_{emp_id}_{existing_record['effective_start_date']}.json"
        minio_client.put_object(
            Bucket=SILVER_BUCKET,
            Key=history_key,
            Body=json.dumps(existing_record).encode('utf-8'),
            ContentType='application/json'
        )
        logger.info(f"Closed old row → history: {history_key}")

    # Insert new current row
    silver_record = {
        **new_record,
        'effective_start_date': now,
        'effective_end_date': None,
        'is_current': True,
        'layer': 'silver',
        'processed_at': now
    }

    minio_client.put_object(
        Bucket=SILVER_BUCKET,
        Key=existing_key,
        Body=json.dumps(silver_record).encode('utf-8'),
        ContentType='application/json'
    )
    logger.info(f"✅ New current row written to Silver: emp_{emp_id}")
    return silver_record

def process_bronze_to_silver():
    """Read all Bronze records and apply SCD Type 2 into Silver"""
    logger.info("Starting Silver ETL...")
    minio_client = get_minio_client()

    # List all bronze employee files
    try:
        response = minio_client.list_objects_v2(
            Bucket=BRONZE_BUCKET,
            Prefix='employee/'
        )
    except Exception as e:
        logger.error(f"Cannot read Bronze bucket: {e}")
        return

    files = response.get('Contents', [])
    if not files:
        logger.warning("No files found in Bronze bucket")
        return

    logger.info(f"Found {len(files)} records in Bronze")

    processed = 0
    skipped = 0

    for obj in files:
        key = obj['Key']
        try:
            # Read Bronze record
            response = minio_client.get_object(Bucket=BRONZE_BUCKET, Key=key)
            raw = json.loads(response['Body'].read().decode('utf-8'))

            # Extract clean record
            clean_record, op = extract_employee(raw)

            if clean_record is None:
                skipped += 1
                continue

            if op == 'd':
                logger.info(f"🗑️ DELETE operation for emp_id={clean_record.get('emp_id')} — marking inactive")
                clean_record['is_active'] = False

            # Apply SCD Type 2
            apply_scd_type2(minio_client, clean_record)
            processed += 1

        except Exception as e:
            logger.error(f"Error processing {key}: {e}")
            skipped += 1

    logger.info(f"✅ Silver ETL complete — processed={processed} skipped={skipped}")
    logger.info("Check Silver bucket at http://localhost:9001")

if __name__ == '__main__':
    process_bronze_to_silver()
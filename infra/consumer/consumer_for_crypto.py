# Import requirements
import json
import boto3
import time
import os
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# MinIO Connection
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS").split(",")

# Topic for crypto quotes
KAFKA_TOPIC = "crypto-quotes"

# Validate MinIO credentials
if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise ValueError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set in .env file")

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

bucket_name = MINIO_BUCKET

# Ensure bucket exists (idempotent)
try:
    s3.head_bucket(Bucket=bucket_name)
    print(f"Bucket {bucket_name} already exists.")
except Exception:
    s3.create_bucket(Bucket=bucket_name)
    print(f"Created bucket {bucket_name}.")

# Define Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="crypto-consumer",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Consumer streaming crypto quotes and saving to MinIO...")

# Main Function
for message in consumer:
    record = message.value
    symbol = record.get("symbol", "unknown")
    ts = record.get("fetched_at", int(time.time()))
    key = f"crypto/{symbol}/{ts}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(record),
        ContentType="application/json"
    )
    print(f"Saved crypto record for {symbol} to s3://{bucket_name}/{key}")

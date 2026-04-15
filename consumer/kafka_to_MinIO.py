'''
This script consume the messages (event changes) from kafka topics
and saves them to MinIO in Parquet format for further processing or analytics.

Workflow:
- Load credentials and configuration of MinIO and Kafka.
- Connect to Kafka and subscribe to the relevant topics for the banking tables (customers, accounts, transactions).
- For each incoming message, extract the relevant data and batch it in memory.
- Once a batch of records is ready, convert it to Parquet format and upload it to MinIO, organized by table and date for easy retrieval.
'''

import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# -----------------------------
# Configure Kafka consumer to subscribe to Debezium CDC topics for banking tables
# -----------------------------
consumer = KafkaConsumer(
    'banking_server.public.customers',  # Topic for customer table changes
    'banking_server.public.accounts',   # Topic for account table changes
    'banking_server.public.transactions',  # Topic for transaction table changes
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),  # Kafka broker addresses
                                                     ## Broker is who manages the topics and messages
    auto_offset_reset='earliest',  # Start consuming from the oldest message
    enable_auto_commit=True,  # Automatically saves where you’ve read,
                              # if the consumer restarts, it will continue from the last committed offset
    group_id=os.getenv("KAFKA_GROUP"),  # Name of the consumer group
    # api_version=(0,11,5),
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Kafka sends bytes, deserialize JSON messages
)

# -----------------------------
# Initialize MinIO (S3-compatible) client for object storage
# -----------------------------
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),  # MinIO server URL
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),  # Access key
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")  # Secret key
)

bucket = os.getenv("MINIO_BUCKET")  # Target bucket name

# Ensure the bucket exists; create it if not
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket)

# -----------------------------
# Function to batch records, convert to Parquet, and upload to MinIO
# -----------------------------
def write_to_minio(table_name, records):
    if not records:  # Skip if no records to process
        return
    df = pd.DataFrame(records)  # Convert list of dicts to Pandas DataFrame
    date_str = datetime.now().strftime('%Y-%m-%d')  # Current date for partitioning
    file_path = f'{table_name}_{date_str}.parquet'  # Temporary local file path
    df.to_parquet(file_path, engine='fastparquet', index=False)  # Write to Parquet format
    # S3 key with partitioning: table/date=YYYY-MM-DD/filename.parquet
    s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
    s3.upload_file(file_path, bucket, s3_key)  # Upload to MinIO
    os.remove(file_path)  # Clean up local file
    print(f'✅ Uploaded {len(records)} records to s3://{bucket}/{s3_key}')

# -----------------------------
# Batch processing configuration
# -----------------------------
'''
- Functions of the buffer:
- Accumulate records in memory until a certain batch size is reached.
- Avoid uploading to MinIO for every single record, which can be inefficient.
- Once the batch size is reached, the buffer triggers the "write_to_minio" function to upload the batch of records.
- After uploading, the buffer is cleared to start accumulating the next batch of records.
'''
batch_size = 30  # Number of records to accumulate before uploading
buffer = {  # In-memory buffers for each topic
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': []
}

print("✅ Connected to Kafka. Listening for messages...")

# -----------------------------
# Main consumption loop
# -----------------------------
'''
Each Kafka message contains:
- topic: The Kafka topic name (e.g., 'banking_server.public.customers').
- value: The actual message payload, Debezium event (JSON).
- payload.op: operation type (c, u, d, r)
    c = create (INSERT)
    u = update
    d = delete
    r = snapshot (initial load)
'''

for message in consumer:
    topic = message.topic  # Extract topic name
    event = message.value  # Full Debezium event (JSON)
    payload = event.get("payload", {})  # Debezium payload containing change data
    
    op = payload.get("op")  # Operation type from Debezium
    record = None  # Initialize record

    # -----------------------------
    # Handle different CDC operations
    # -----------------------------

    if op in ["c", "r"]:
        # INSERT or SNAPSHOT → use "after" (new state)
        record = payload.get("after") # The actual row data after the change (e.g., new customer info, updated account details, etc.)
        if record:
            record["_op"] = "insert"

    elif op == "u":
        # UPDATE → use "after" (new state)
        record = payload.get("after")
        if record:
            record["_op"] = "update"

    elif op == "d":
        # DELETE → use "before" (old state, since after = null)
        record = payload.get("before")
        if record:
            record["_op"] = "delete"  

    if record:  # Only process if there's valid data
        buffer[topic].append(record)  # Add to buffer
        print(f"[{topic}] ({record['_op']}) -> {record}")  # Debug log the record with operation type

    # Check if buffer is full for this topic
    if len(buffer[topic]) >= batch_size:
        table_name = topic.split('.')[-1]  # Extract table name from topic (e.g., 'customers')
        write_to_minio(table_name, buffer[topic])  # Upload batch to MinIO
        buffer[topic] = []  # Reset buffer
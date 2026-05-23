import json
import os
from kafka import KafkaConsumer
from datetime import datetime
import boto3  # to connect minio to client s3
from dotenv import load_dotenv

# ---------- Load environment variables ----------
load_dotenv()

# ---------- Configuration ----------
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))  # 1 json file = 10 rows

# ---------- Connect to MinIO ----------
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

# Ensure bucket exists (idempotent)
try:
    s3.head_bucket(Bucket=MINIO_BUCKET)
    print(f"Bucket {MINIO_BUCKET} already exists.")
except Exception:
    s3.create_bucket(Bucket=MINIO_BUCKET)
    print(f"Created bucket {MINIO_BUCKET}.")

# ---------- Kafka Consumer Setup ----------
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
    auto_offset_reset="earliest",  # data from the beginning
    enable_auto_commit=True,  # consumer will automatically save (commit) the offset of messages it has read.
    group_id=KAFKA_GROUP_ID,  # if you change the group_id >> offset will restart to 0
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"🎧 Listening for events on Kafka topic '{KAFKA_TOPIC}'...")

batch = []

for message in consumer:
    event = message.value
    batch.append(event)

    if len(batch) >= BATCH_SIZE:
        now = datetime.utcnow()  # get current utc time
        date_path = now.strftime("date=%Y-%m-%d/hour=%H")  # eg date=2026-05-22/hour=15
        file_name = f"spotify_events_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"  # eg spotify_events_2026-05-22T15-30-45.json
        file_path = f"bronze/{date_path}/{file_name}"  # eg bronze/date=2026-05-22/hour=15/spotify_events_2026-05-22T15-30-45.json

        json_data = "\n".join(
            [json.dumps(e) for e in batch]
        )  # Each event becomes a JSON string.

        s3.put_object(
            Bucket=MINIO_BUCKET, Key=file_path, Body=json_data.encode("utf-8")
        )

        print(f"✅ Uploaded {len(batch)} events to MinIO: {file_path}")
        batch = []

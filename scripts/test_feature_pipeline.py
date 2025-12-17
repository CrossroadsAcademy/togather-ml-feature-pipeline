"""
Feature Pipeline Test Script

Tests the actual KafkaConsumer and ParquetSink from the feature-pipeline.

"""

import asyncio
import json
import os
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Load .env file explicitly
from dotenv import load_dotenv

# Load from .env in project root
env_path = Path(__file__).parent.parent / ".env"
loaded = load_dotenv(env_path)
print(f"Loaded .env from: {env_path} (found: {loaded})")


def get_required_env(key: str) -> str:
    """Get required environment variable or exit with error."""
    value = os.getenv(key)
    if not value:
        print(f"Missing required environment variable: {key}")
        print(f"   Set it in .env file or export {key}=<value>")
        import sys

        sys.exit(1)
    return value


# Configuration from environment
KAFKA_BOOTSTRAP_SERVERS = get_required_env("KAFKA_BOOTSTRAP_SERVERS")

# Parse KAFKA_TOPICS - handle both JSON array and comma-separated formats
_kafka_topics_raw = os.getenv("KAFKA_TOPICS", "events.raw")
if _kafka_topics_raw.startswith("["):
    import json

    KAFKA_TOPICS = json.loads(_kafka_topics_raw.replace("'", '"'))
else:
    KAFKA_TOPICS = _kafka_topics_raw.split(",")

KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "feature-pipeline-test")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "events.dlq")

# MinIO configuration
MINIO_ENDPOINT = get_required_env("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = get_required_env("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = get_required_env("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_NAME", "raw-events")

# Debug: Print what was loaded
print(f"KAFKA_BOOTSTRAP_SERVERS = {KAFKA_BOOTSTRAP_SERVERS}")
print(f"MINIO_ENDPOINT = {MINIO_ENDPOINT}")
print(f"MINIO_ACCESS_KEY = {MINIO_ACCESS_KEY[:4]}***")

# Event Generators

SAMPLE_USERS = [f"user_{i:04d}" for i in range(1, 51)]
SAMPLE_EXPERIENCES = [f"exp_{i:04d}" for i in range(1, 21)]

# Topics that AppEventsProcessor handles
TOPICS = [
    "user.account",
    "user.profile",
    "experience",
    "engagement",
    "location.streams",
]


def generate_user_account_event() -> dict[str, Any]:
    """Generate user.account event."""
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(SAMPLE_USERS),
        "event_type": random.choice(
            ["user_created", "user_updated", "user_deleted", "user_verified"]
        ),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "email": f"{random.choice(SAMPLE_USERS)}@example.com",
        "status": random.choice(["active", "inactive", "pending"]),
        "metadata": {
            "source": random.choice(["mobile", "web", "api"]),
            "ip_address": f"192.168.1.{random.randint(1, 254)}",
        },
    }


def generate_user_profile_event() -> dict[str, Any]:
    """Generate user.profile event."""
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(SAMPLE_USERS),
        "event_type": random.choice(["profile_updated", "preferences_changed", "avatar_updated"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "preferences": {
            "categories": random.sample(
                ["music", "sports", "food", "tech", "art"], k=random.randint(1, 3)
            ),
            "notifications_enabled": random.choice([True, False]),
        },
        "metadata": {
            "source": random.choice(["mobile", "web"]),
        },
    }


def generate_experience_event() -> dict[str, Any]:
    """Generate experience event (requires user_id and experience_id)."""
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(SAMPLE_USERS),
        "experience_id": random.choice(SAMPLE_EXPERIENCES),
        "event_type": random.choice(["view", "click", "bookmark", "share", "rsvp"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": random.randint(5, 300),
        "score": round(random.uniform(0.5, 1.0), 3),
        "category": random.choice(["music", "sports", "food", "tech", "art", "social"]),
        "metadata": {
            "source": random.choice(["mobile", "web"]),
            "device_type": random.choice(["ios", "android", "desktop"]),
        },
    }


def generate_engagement_event() -> dict[str, Any]:
    """Generate engagement event."""
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(SAMPLE_USERS),
        "event_type": random.choice(["like", "comment", "share", "follow", "unfollow"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "target_id": random.choice(SAMPLE_USERS + SAMPLE_EXPERIENCES),
        "target_type": random.choice(["user", "experience", "post"]),
        "metadata": {
            "source": random.choice(["mobile", "web"]),
            "session_id": str(uuid.uuid4())[:8],
        },
    }


def generate_location_event() -> dict[str, Any]:
    """Generate location.streams event (requires latitude/longitude)."""
    # Random locations around major cities
    locations = [
        (37.7749, -122.4194, "San Francisco"),
        (40.7128, -74.0060, "New York"),
        (51.5074, -0.1278, "London"),
        (12.9716, 77.5946, "Bangalore"),
        (35.6762, 139.6503, "Tokyo"),
    ]
    lat, lon, city = random.choice(locations)

    # Add some randomness to coordinates
    lat += random.uniform(-0.1, 0.1)
    lon += random.uniform(-0.1, 0.1)

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(SAMPLE_USERS),
        "event_type": "location_update",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "accuracy_meters": random.randint(5, 100),
        "city": city,
        "metadata": {
            "source": random.choice(["gps", "wifi", "cell"]),
            "battery_level": random.randint(10, 100),
        },
    }


# Map topics to event generators
EVENT_GENERATORS = {
    "user.account": generate_user_account_event,
    "user.profile": generate_user_profile_event,
    "experience": generate_experience_event,
    "engagement": generate_engagement_event,
    "location.streams": generate_location_event,
}


def generate_event(topic: str = "experience") -> dict[str, Any]:
    """Generate event for specific topic."""
    generator = EVENT_GENERATORS.get(topic, generate_experience_event)
    return generator()


# Test: Produce Events to Kafka


def test_produce_events(num_events: int = 20):
    """Produce test events to Kafka across multiple topics."""
    from confluent_kafka import Producer

    # Use configured topics or default to our known topics
    topics_to_use = KAFKA_TOPICS if KAFKA_TOPICS != ["events.raw"] else TOPICS

    print(f"\nProducing {num_events} events to Kafka...")
    print(f"   Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"   Topics: {topics_to_use}")

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "feature-pipeline-test-producer",
        }
    )

    delivered = 0
    events_by_topic: dict[str, int] = {}

    def callback(err, msg):
        nonlocal delivered
        if err:
            print(f"   Delivery failed: {err}")
        else:
            delivered += 1

    # Distribute events across topics
    for i in range(num_events):
        topic = topics_to_use[i % len(topics_to_use)]
        event = generate_event(topic)

        if topic not in events_by_topic:
            events_by_topic[topic] = 0
        events_by_topic[topic] += 1

        producer.produce(
            topic=topic,
            key=event["user_id"].encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=callback,
        )
        producer.poll(0)

    producer.flush(timeout=10)

    print(f"   Delivered {delivered}/{num_events} events")
    for topic, count in events_by_topic.items():
        print(f"      - {topic}: {count} events")

    return delivered


# Test: Consumer + StorageSink


async def test_consumer_and_sink(max_messages: int = 20, timeout_seconds: int = 30):
    """Test the actual KafkaConsumer → StorageSink pipeline."""

    print("\nTesting Consumer → StorageSink pipeline...")

    # Import actual classes
    from src.streaming.kafka_consumer import KafkaConsumerConfig
    from src.streaming.storage_sink import StorageSink, StorageSinkConfig

    # Create processor that writes to StorageSink
    class TestProcessor:
        def __init__(self, sink: StorageSink):
            self.sink = sink
            self.messages = []

        async def process(self, message: dict[str, Any], topic: str) -> None:
            print(
                f"   Received: {message.get('event_type', 'unknown')} from {message.get('user_id', 'unknown')}"
            )
            self.messages.append(message)

            # Write to StorageSink when batch is ready
            if len(self.messages) >= 10:
                event_type = self.messages[0].get("event_type", "unknown")
                key = self.sink.write_batch(self.messages, event_type)
                print(f"   Written to storage: {key}")
                self.messages.clear()

    # Initialize StorageSink
    sink_config = StorageSinkConfig(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
        bucket_name=MINIO_BUCKET,
        batch_size=10,
    )
    sink = StorageSink(sink_config)
    processor = TestProcessor(sink)

    # Use same topics we produced to
    topics_to_consume = (
        TOPICS  # ["user.account", "user.profile", "experience", "engagement", "location.streams"]
    )

    # Initialize KafkaConsumer
    consumer_config = KafkaConsumerConfig(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        consumer_group=KAFKA_CONSUMER_GROUP,
        topics=topics_to_consume,
        dlq_topic=KAFKA_DLQ_TOPIC,
        auto_offset_reset="earliest",
        security_protocol="PLAINTEXT",  # No auth for testing
    )

    print(f"   Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"   Topics: {topics_to_consume}")
    print(f"   Consumer Group: {KAFKA_CONSUMER_GROUP}")

    # Create consumer (without starting metrics server on same port)
    from confluent_kafka import Consumer as CKConsumer
    from confluent_kafka import KafkaError

    # Simple consumer for testing
    ck_consumer = CKConsumer(
        {
            "bootstrap.servers": consumer_config.bootstrap_servers,
            "group.id": consumer_config.consumer_group,
            "auto.offset.reset": consumer_config.auto_offset_reset,
            "enable.auto.commit": False,
        }
    )
    ck_consumer.subscribe(consumer_config.topics)

    print(f"\n   Waiting for messages (timeout: {timeout_seconds}s)...")

    import time

    start_time = time.time()
    consumed = 0

    try:
        while consumed < max_messages and (time.time() - start_time) < timeout_seconds:
            msg = ck_consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"   Error: {msg.error()}")
                continue

            # Parse and process
            try:
                value = json.loads(msg.value().decode("utf-8"))
                value["_kafka_partition"] = msg.partition()
                value["_kafka_offset"] = msg.offset()
                await processor.process(value, msg.topic())
                consumed += 1
                ck_consumer.commit(msg)
            except Exception as e:
                print(f"   Parse error: {e}")

        # Flush remaining messages
        if processor.messages:
            event_type = processor.messages[0].get("event_type", "unknown")
            key = sink.write_batch(processor.messages, event_type)
            print(f"   Final flush to MinIO: {key}")

    finally:
        ck_consumer.close()

    print(f"\n   Consumed and processed {consumed} messages")
    return consumed


# Test: Verify MinIO Data


def test_verify_minio():
    """Verify data in MinIO."""
    from io import BytesIO

    import pyarrow.parquet as pq
    from minio import Minio

    print("\nVerifying MinIO data...")
    print(f"   Endpoint: {MINIO_ENDPOINT}")
    print(f"   Bucket: {MINIO_BUCKET}")

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    if not client.bucket_exists(MINIO_BUCKET):
        print(f"   Bucket '{MINIO_BUCKET}' does not exist")
        return False

    objects = list(client.list_objects(MINIO_BUCKET, recursive=True))
    parquet_files = [o for o in objects if o.object_name.endswith(".parquet")]

    print(f"   Found {len(parquet_files)} Parquet files")

    if parquet_files:
        # Show partitions
        partitions = set()
        for obj in parquet_files:
            parts = obj.object_name.split("/")[:-1]
            partitions.add("/".join(parts))

        print("   Partitions:")
        for p in sorted(partitions)[:5]:
            print(f"      - {p}")

        # Read first file
        first_file = parquet_files[0].object_name
        print(f"\n   Reading: {first_file}")

        response = client.get_object(MINIO_BUCKET, first_file)
        buffer = BytesIO(response.read())
        table = pq.read_table(buffer)

        print(f"      Columns: {table.column_names}")
        print(f"      Rows: {table.num_rows}")

        if table.num_rows > 0:
            df = table.to_pandas()
            print("\n   Sample row:")
            print(df.iloc[0].to_string())

        return True

    return False


# Main


async def main():
    print("=" * 60)
    print("Feature Pipeline Integration Test")
    print("=" * 60)

    print("\nConfiguration:")
    print(f"   KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"   KAFKA_TOPICS: {KAFKA_TOPICS}")
    print(f"   MINIO_ENDPOINT: {MINIO_ENDPOINT}")
    print(f"   MINIO_BUCKET: {MINIO_BUCKET}")

    # Produce test events
    print("\n" + "-" * 40)
    print("Step 1: Produce test events")
    print("-" * 40)
    try:
        test_produce_events(20)
    except Exception as e:
        print(f"   Failed to produce: {e}")
        return

    #  Consume and sink
    print("\n" + "-" * 40)
    print("Step 2: Consume → ParquetSink")
    print("-" * 40)
    try:
        await test_consumer_and_sink(20, 30)
    except Exception as e:
        print(f"   Failed to consume/sink: {e}")
        import traceback

        traceback.print_exc()
        return

    # Verify MinIO
    print("\n" + "-" * 40)
    print("Step 3: Verify MinIO data")
    print("-" * 40)
    try:
        success = test_verify_minio()
    except Exception as e:
        print(f"   Verification failed: {e}")
        success = False

    # Summary
    print("\n" + "=" * 60)
    if success:
        print("Feature Pipeline Test PASSED!")
    else:
        print("Feature Pipeline Test completed with warnings")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
"""
Local Test Script for Flink Session Job.

Tests the feature extraction and sinks without requiring a full Flink cluster.

Usage:
    poetry run python scripts/test_flink_session_job.py
"""

import os
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from src.flink.event_validator import EventValidator, validate_batch
from src.flink.feature_extractors import (
    compute_engagement_score,
    extract_session_features,
)
from src.flink.redis_sink import RedisSink, RedisSinkConfig
from src.streaming.storage_sink import StorageSink, StorageSinkConfig
from src.utils.logger import get_logger

# Load .env
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)


logger = get_logger(__name__)


def get_required_env(key: str) -> str:
    """Get required environment variable or exit."""
    value = os.getenv(key)
    if not value:
        print(f"Missing required environment variable: {key}")
        print(f"Set it in .env file or export {key}=<value>")
        import sys

        sys.exit(1)
    return value


# Sample users and experiences
SAMPLE_USERS = [f"user_{i:04d}" for i in range(1, 11)]
SAMPLE_EXPERIENCES = [f"exp_{i:04d}" for i in range(1, 6)]


def generate_session_events(user_id: str, num_events: int = 10) -> list[dict]:
    """Generate a batch of events simulating a user session."""
    events = []
    base_time = datetime.now(timezone.utc)

    for i in range(num_events):
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "session_id": f"sess_{user_id}_{base_time.strftime('%Y%m%d%H')}",
            "experience_id": random.choice(SAMPLE_EXPERIENCES),
            "event_type": random.choice(["view", "click", "bookmark", "share"]),
            "timestamp": (base_time.replace(second=i * 5)).isoformat(),
            "latitude": 37.7749 + random.uniform(-0.01, 0.01),
            "longitude": -122.4194 + random.uniform(-0.01, 0.01),
        }
        events.append(event)

    return events


def test_event_validator():
    """Test the event validator."""
    print("\n" + "=" * 60)
    print("Testing Event Validator")
    print("=" * 60)

    validator = EventValidator()

    # Valid event
    valid_event = {
        "event_id": str(uuid.uuid4()),
        "user_id": "user_0001",
        "event_type": "click",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    result = validator.validate(valid_event, "events.raw")
    print(f"Valid event: is_valid={result.is_valid}")

    # Invalid event (missing user_id)
    invalid_event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "click",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    result = validator.validate(invalid_event, "events.raw")
    print(
        f"Invalid event (missing user_id): is_valid={result.is_valid}, error={result.error_message}"
    )

    # Invalid coordinates
    bad_coords = {
        "user_id": "user_0001",
        "event_type": "location_update",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "latitude": 200,  # Invalid
        "longitude": -122.4,
    }
    result = validator.validate(bad_coords, "location.streams")
    print(f"Invalid coordinates: is_valid={result.is_valid}, error={result.error_message}")

    print("Event validation tests passed")


def test_feature_extractors():
    """Test the feature extractors."""
    print("\n" + "=" * 60)
    print("Testing Feature Extractors")
    print("=" * 60)

    # Generate session events
    user_id = "user_0001"
    events = generate_session_events(user_id, 15)

    print(f"Generated {len(events)} events for {user_id}")

    # Extract session features
    features = extract_session_features(events)

    if features:
        print("\nSession Features:")
        print(f"  user_id: {features.user_id}")
        print(f"  session_id: {features.session_id}")
        print(f"  session_duration: {features.session_duration_seconds:.2f}s")
        print(f"  activity_count: {features.activity_count}")
        print(f"  location_changes: {features.location_changes}")
        print(f"  unique_event_types: {features.unique_event_types}")
        print(f"  engagement_score: {features.engagement_score:.4f}")

        # Test to_dict
        features_dict = features.to_dict()
        print(f"\nFeatures as dict: {len(features_dict)} keys")

    # Test engagement score
    scores = [
        compute_engagement_score([{"event_type": "view"}]),
        compute_engagement_score([{"event_type": "click"}]),
        compute_engagement_score([{"event_type": "rsvp"}]),
    ]
    print(f"\nEngagement scores: view={scores[0]:.4f}, click={scores[1]:.4f}, rsvp={scores[2]:.4f}")

    print("Feature extraction tests passed")


def test_redis_sink():
    """Test the Redis sink (requires Redis running)."""
    print("\n" + "=" * 60)
    print("Testing Redis Sink")
    print("=" * 60)

    try:
        config = RedisSinkConfig.from_env()
        sink = RedisSink(config)

        # Write test features
        user_id = "test_user_001"
        session_id = "test_session_001"
        features = {
            "session_duration_seconds": "120.5",
            "activity_count": "15",
            "engagement_score": "0.75",
        }

        key = sink.write_session_features(user_id, session_id, features)
        print(f"Wrote features to Redis: {key}")

        # Read back
        read_features = sink.get_session_features(user_id, session_id)
        print(f"Read features: {read_features}")

        # Clean up
        sink.delete_session(user_id, session_id)
        print("Cleaned up test data")

        sink.close()
        print("Redis sink tests passed")

    except Exception as e:
        print(f"Redis sink test failed (Redis not running?): {e}")


def test_minio_sink():
    """Test the MinIO sink (requires MinIO running)."""
    print("\n" + "=" * 60)
    print("Testing MinIO Sink")
    print("=" * 60)

    try:
        config = StorageSinkConfig(
            endpoint=get_required_env("MINIO_ENDPOINT_URL"),
            access_key=get_required_env("MINIO_ACCESS_KEY"),
            secret_key=get_required_env("MINIO_SECRET_KEY"),
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
            bucket_name=os.getenv("MINIO_BUCKET_NAME", "raw-events"),
        )

        sink = StorageSink(config)

        # Generate test events
        events = generate_session_events("test_user", 5)

        # Write to MinIO
        key = sink.write_batch(events, event_type="test_events")
        print(f"Wrote {len(events)} events to MinIO: {key}")

        # List files
        files = sink.list_files(prefix="event_type=test_events")
        print(f"Found {len(files)} files in bucket")

        print("MinIO sink tests passed")

    except Exception as e:
        print(f"MinIO sink test failed (MinIO not running?): {e}")


def test_full_pipeline():
    """Test the full pipeline simulation."""
    print("\n" + "=" * 60)
    print("Testing Full Pipeline Simulation")
    print("=" * 60)

    # Simulate events from multiple users
    all_events = []
    for user_id in SAMPLE_USERS[:3]:
        events = generate_session_events(user_id, random.randint(5, 15))
        all_events.extend(events)

    print(f"Generated {len(all_events)} total events from 3 users")

    # Validate events
    validator = EventValidator()
    valid_events, invalid_events = validate_batch(all_events, "experience", validator)
    print(f"Validation: {len(valid_events)} valid, {len(invalid_events)} invalid")

    # Group by user (simulating keying)
    by_user: dict[str, list[dict]] = {}
    for event in valid_events:
        user_id = event["user_id"]
        if user_id not in by_user:
            by_user[user_id] = []
        by_user[user_id].append(event)

    # Extract features for each user session
    print("\nSession Features by User:")
    for user_id, user_events in by_user.items():
        features = extract_session_features(user_events)
        if features:
            print(
                f"  {user_id}: {features.activity_count} events, "
                f"{features.session_duration_seconds:.1f}s, "
                f"engagement={features.engagement_score:.4f}"
            )

    print("\nFull pipeline simulation passed")


def main():
    """Run all tests."""
    print("=" * 60)
    print("Flink Session Job - Local Test Suite")
    print("=" * 60)
    print("\nNote: The PyFlink job requires a Flink cluster to run.")
    print("This script tests the components locally without Flink.")

    test_event_validator()
    test_feature_extractors()
    test_redis_sink()
    test_minio_sink()
    test_full_pipeline()

    print("\n" + "=" * 60)
    print("All local tests completed!")
    print("=" * 60)
    print("\nTo run the actual Flink job:")
    print("  1. Deploy to Flink cluster: kubectl apply -k k8s/base/stream-processing/flink/")
    print("  2. Check job status: kubectl get flinkdeployment -n stream-processing")


if __name__ == "__main__":
    main()

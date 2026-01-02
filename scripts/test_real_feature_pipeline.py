#!/usr/bin/env python3
"""
Real Feature Pipeline Test Script.

Tests the PRODUCTION flow: Kafka → Flink/AppEventsProcessor → MinIO → Spark

Generates mock data matching actual protobuf schemas:
- UserProfileCreated (user.v1)
- ExperienceCreated (experience.v1)
- RecommendationFeedback (feed.v1)
- RecommendationServed (feed.v1)

Usage:
    # Full production flow test:
    poetry run python scripts/test_real_feature_pipeline.py --publish-kafka --verify-flow --run-pipeline

    # Batch-only test (bypass Kafka):
    poetry run python scripts/test_real_feature_pipeline.py --direct-minio --run-pipeline

    # Just generate and inspect data:
    poetry run python scripts/test_real_feature_pipeline.py --generate-data --save-json
"""

import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# =============================================================================
# Configuration
# =============================================================================

# Sample data pools
CITIES = [
    {"city": "Mumbai", "country": "India", "lat": 19.0760, "lng": 72.8777},
    {"city": "Bangalore", "country": "India", "lat": 12.9716, "lng": 77.5946},
    {"city": "Delhi", "country": "India", "lat": 28.6139, "lng": 77.2090},
    {"city": "Chennai", "country": "India", "lat": 13.0827, "lng": 80.2707},
    {"city": "Hyderabad", "country": "India", "lat": 17.3850, "lng": 78.4867},
]

INTERESTS = [
    {"id": "int_music", "name": "Music"},
    {"id": "int_sports", "name": "Sports"},
    {"id": "int_food", "name": "Food & Drinks"},
    {"id": "int_tech", "name": "Technology"},
    {"id": "int_art", "name": "Art & Culture"},
    {"id": "int_outdoor", "name": "Outdoor Activities"},
    {"id": "int_fitness", "name": "Fitness"},
    {"id": "int_gaming", "name": "Gaming"},
    {"id": "int_travel", "name": "Travel"},
    {"id": "int_social", "name": "Social Events"},
]

CATEGORIES = [
    {"id": "cat_concerts", "name": "Concerts"},
    {"id": "cat_workshops", "name": "Workshops"},
    {"id": "cat_meetups", "name": "Meetups"},
    {"id": "cat_sports", "name": "Sports Events"},
    {"id": "cat_food", "name": "Food & Dining"},
    {"id": "cat_outdoor", "name": "Outdoor Adventures"},
    {"id": "cat_fitness", "name": "Fitness Classes"},
    {"id": "cat_nightlife", "name": "Nightlife"},
]

TAGS = [
    {"id": "tag_weekend", "name": "Weekend", "icon": "🗓"},
    {"id": "tag_family", "name": "Family Friendly", "icon": "👨‍👩‍👧"},
    {"id": "tag_free", "name": "Free Entry", "icon": "🆓"},
    {"id": "tag_popular", "name": "Popular", "icon": "🔥"},
    {"id": "tag_new", "name": "New", "icon": "✨"},
    {"id": "tag_trending", "name": "Trending", "icon": "📈"},
]

EXPERIENCE_NAMES = [
    "Live Jazz Night",
    "Yoga in the Park",
    "Food Truck Festival",
    "Tech Startup Meetup",
    "Street Art Tour",
    "Mountain Hiking Trip",
    "CrossFit Challenge",
    "Board Game Evening",
    "City Photography Walk",
    "Rooftop Party",
    "Cooking Masterclass",
    "Beach Volleyball Tournament",
    "Open Mic Night",
    "Wine Tasting Experience",
    "Salsa Dance Class",
]


# =============================================================================
# Protobuf-matching Event Generators
# =============================================================================


def generate_user_profile_created(user_id: str) -> dict[str, Any]:
    """
    Generate UserProfileCreated event matching user.v1.UserProfileCreated proto.
    """
    city_info = random.choice(CITIES)
    selected_interests = random.sample(INTERESTS, k=random.randint(2, 5))
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    created_ms = now_ms - random.randint(0, 30 * 24 * 60 * 60 * 1000)  # Up to 30 days ago

    return {
        "id": user_id,
        "email": f"{user_id}@example.com",
        "first_name": f"User{user_id[-4:]}",
        "last_name": random.choice(["Kumar", "Singh", "Patel", "Sharma", "Rao"]),
        "phone": f"+91{random.randint(7000000000, 9999999999)}",
        "google_sub": f"google_{uuid.uuid4().hex[:16]}",
        "current_address": {
            "id": str(uuid.uuid4()),
            "line1": f"{random.randint(1, 999)} Main Street",
            "line2": random.choice(["Apt 1", "Floor 2", None]),
            "city": city_info["city"],
            "postal_code": random.randint(100000, 999999),
            "country": city_info["country"],
            "coordinate": {
                "latitude": city_info["lat"] + random.uniform(-0.1, 0.1),
                "longitude": city_info["lng"] + random.uniform(-0.1, 0.1),
            },
            "created_at": created_ms,
            "updated_at": now_ms,
        },
        "interests": selected_interests,
        "display_name": f"User {user_id[-4:]}",
        "avatar_key": f"avatars/{user_id}.jpg",
        "dob": int(
            (datetime.now() - timedelta(days=random.randint(6570, 14600))).timestamp() * 1000
        ),
        "gender": random.choice([0, 1, 2, 3, 4]),  # GENDER enum
        "social_score": random.randint(0, 100),
        "status": random.choice([0, 1, 2]),  # USER_STATUS enum
        "created_at": created_ms,
        "updated_at": now_ms,
        "on_boarding_status": random.choice([0, 1, 2, 3]),  # ON_BOARDING_STATUS enum
        "on_boarded_at": now_ms if random.random() > 0.3 else None,
    }


def generate_experience_created(experience_id: str) -> dict[str, Any]:
    """
    Generate ExperienceCreated event matching experience.v1.ExperienceCreated proto.
    """
    city_info = random.choice(CITIES)
    category = random.choice(CATEGORIES)
    selected_tags = random.sample(TAGS, k=random.randint(1, 3))

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_time_ms = now_ms + random.randint(1, 14) * 24 * 60 * 60 * 1000  # 1-14 days from now
    duration_hours = random.choice([1, 2, 3, 4, 6, 8])
    end_time_ms = start_time_ms + duration_hours * 60 * 60 * 1000

    return {
        "id": experience_id,
        "name": random.choice(EXPERIENCE_NAMES),
        "description": f"An amazing {category['name'].lower()} experience in {city_info['city']}. "
        f"Join us for an unforgettable time!",
        "thumbnail_key": f"thumbnails/{experience_id}.jpg",
        "creator_type": random.choice(["individual", "partner"]),
        "creator_id": f"creator_{uuid.uuid4().hex[:8]}",
        "event_location": {
            "id": str(uuid.uuid4()),
            "line1": f"{random.randint(1, 999)} Event Street",
            "line2": random.choice(["Hall A", "Room 101", None]),
            "city": city_info["city"],
            "postal_code": random.randint(100000, 999999),
            "country": city_info["country"],
            "coordinate": {
                "latitude": city_info["lat"] + random.uniform(-0.05, 0.05),
                "longitude": city_info["lng"] + random.uniform(-0.05, 0.05),
            },
        },
        "price_currency": random.choice([0, 1, 2]),  # CURRENCY enum (0=unspecified, 1=INR, 2=USD)
        "price_amount": random.choice([0, 0, 0, 199, 499, 999, 1499, 2499]),  # 30% free
        "start_time": start_time_ms,
        "end_time": end_time_ms,
        "bucket_size": random.choice([10, 20, 50, 100]),
        "total_buckets": random.choice([1, 2, 3, 5]),
        "tags": [
            {
                "id": t["id"],
                "name": t["name"],
                "icon": t["icon"],
                "deleted_at": 0,
                "created_at": now_ms,
                "updated_at": now_ms,
            }
            for t in selected_tags
        ],
        "category": {
            "id": category["id"],
            "name": category["name"],
            "created_at": now_ms,
            "updated_at": now_ms,
        },
        "created_at": now_ms - random.randint(0, 7 * 24 * 60 * 60 * 1000),
        "updated_at": now_ms,
    }


def generate_recommendation_served(
    request_id: str,
    user_id: str,
    experience_ids: list[str],
) -> dict[str, Any]:
    """
    Generate RecommendationServedEvent matching feed.v1.RecommendationServedEvent proto.
    """
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    # Select subset of experiences for this recommendation
    served_experiences = random.sample(experience_ids, k=min(10, len(experience_ids)))

    return {
        "request_id": request_id,
        "served_at": now_ms,
        "user_id": user_id,
        "session_id": f"session_{uuid.uuid4().hex[:8]}",
        "retrieval_model": "two-tower-v1.0",
        "ranking_model": "ranking-v1.0",
        "trigger": random.choice([0, 1, 2, 3, 4]),  # REQUEST_TRIGGER enum
        "recommendations": [
            {
                "event_id": exp_id,
                "ranking_score": round(random.uniform(0.3, 0.95), 4),
            }
            for exp_id in served_experiences
        ],
    }


def generate_recommendation_feedback(
    request_id: str,
    user_id: str,
    experience_id: str,
    position: int,
    event_type: int,  # 1=impression, 2=click, 3=view, 4=action, 5=negative
) -> dict[str, Any]:
    """
    Generate RecommendationFeedback matching feed.v1.RecommendationFeedback proto.
    """
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    feedback = {
        "session_id": f"session_{uuid.uuid4().hex[:8]}",
        "user_id": user_id,
        "client_timestamp": now_ms,
        "device_context": {
            "platform": random.choice([0, 1, 2, 3]),  # PLATFORM enum
            "app_version": "1.2.3",
            "screen_width": random.choice([375, 414, 390, 428]),
            "screen_height": random.choice([812, 896, 844, 926]),
        },
        "event_type": event_type,
        "event_id": experience_id,
        "request_id": request_id,
        "model_version": "ranking-v1.0",
        "timestamp": now_ms,
        "position": position,
        "absolute_position": position,
    }

    # Add event-specific data
    if event_type == 1:  # IMPRESSION
        feedback["impression_data"] = {
            "visibility_threshold": random.uniform(0.5, 1.0),
            "viewport_position": random.choice([0, 1, 2, 3, 4]),
        }
    elif event_type == 2:  # CLICK
        feedback["click_data"] = {
            "click_target": random.choice([0, 1, 2, 3]),
        }
    elif event_type == 3:  # VIEW
        feedback["view_data"] = {
            "dwell_time_ms": random.choice([500, 1000, 2000, 5000, 10000, 30000]),
            "view_number": random.randint(1, 5),
            "is_revisit": random.choice([True, False]),
            "scroll_depth_percent": random.randint(10, 100),
        }
    elif event_type == 4:  # ACTION
        feedback["action_data"] = {
            "action_type": 1,  # BOOK_EXPERIENCE
            "time_since_impression_ms": random.randint(5000, 60000),
        }
    elif event_type == 5:  # NEGATIVE
        feedback["negative_data"] = {
            "action_type": 2,  # NOT_INTERESTED
        }

    return feedback


# =============================================================================
# Data Generation
# =============================================================================


def generate_test_data(
    num_users: int = 50,
    num_experiences: int = 20,
    num_requests: int = 100,
) -> dict[str, list[dict]]:
    """
    Generate complete test dataset matching real protobuf schemas.
    """
    print(
        f"Generating test data: {num_users} users, {num_experiences} experiences, {num_requests} requests"
    )

    # Generate user and experience IDs
    user_ids = [f"user_{uuid.uuid4().hex[:8]}" for _ in range(num_users)]
    experience_ids = [f"exp_{uuid.uuid4().hex[:8]}" for _ in range(num_experiences)]

    data = {
        "user_profiles": [],
        "experiences": [],
        "recommendation_served": [],
        "recommendation_feedback": [],
    }

    # Generate user profiles
    print("  Generating user profiles...")
    for user_id in user_ids:
        data["user_profiles"].append(generate_user_profile_created(user_id))

    # Generate experiences
    print("  Generating experiences...")
    for exp_id in experience_ids:
        data["experiences"].append(generate_experience_created(exp_id))

    # Generate recommendation requests and feedback
    print("  Generating recommendations and feedback...")
    for _ in range(num_requests):
        request_id = f"req_{uuid.uuid4().hex[:12]}"
        user_id = random.choice(user_ids)

        # Generate served event
        served = generate_recommendation_served(request_id, user_id, experience_ids)
        data["recommendation_served"].append(served)

        # Generate feedback for served items
        for i, rec in enumerate(served["recommendations"]):
            exp_id = rec["event_id"]

            # Always generate impression
            data["recommendation_feedback"].append(
                generate_recommendation_feedback(request_id, user_id, exp_id, i, 1)  # IMPRESSION
            )

            # Some get clicked (30%)
            if random.random() < 0.3:
                data["recommendation_feedback"].append(
                    generate_recommendation_feedback(request_id, user_id, exp_id, i, 2)  # CLICK
                )

                # Some views (80% of clicks)
                if random.random() < 0.8:
                    data["recommendation_feedback"].append(
                        generate_recommendation_feedback(request_id, user_id, exp_id, i, 3)  # VIEW
                    )

                # Some actions (10% of clicks)
                if random.random() < 0.1:
                    data["recommendation_feedback"].append(
                        generate_recommendation_feedback(
                            request_id, user_id, exp_id, i, 4
                        )  # ACTION
                    )

            # Some negatives (5%)
            elif random.random() < 0.05:
                data["recommendation_feedback"].append(
                    generate_recommendation_feedback(request_id, user_id, exp_id, i, 5)  # NEGATIVE
                )

    print("  Generated:")
    print(f"    - {len(data['user_profiles'])} user profiles")
    print(f"    - {len(data['experiences'])} experiences")
    print(f"    - {len(data['recommendation_served'])} served requests")
    print(f"    - {len(data['recommendation_feedback'])} feedback events")

    return data


# =============================================================================
# Storage Writers
# =============================================================================


def save_to_json(data: dict[str, list[dict]], output_dir: Path) -> None:
    """Save generated data to JSON files for inspection."""
    output_dir.mkdir(parents=True, exist_ok=True)

    for key, records in data.items():
        output_file = output_dir / f"{key}.json"
        with open(output_file, "w") as f:
            json.dump(records, f, indent=2, default=str)
        print(f"  Saved {len(records)} {key} to {output_file}")


# =============================================================================
# Kafka Publishing (Production Flow)
# =============================================================================


def get_kafka_config() -> dict:
    """Load Kafka configuration from environment."""
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).parent.parent / ".env")

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_servers:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS not set in .env")

    return {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "feature-pipeline-test-producer",
    }


def publish_to_kafka(data: dict[str, list[dict]], use_protobuf: bool = False) -> dict[str, int]:
    """
    Publish generated events to Kafka topics.

    Supports two modes:
    1. JSON mode (default): For local testing without togather-event-sdk
    2. Protobuf mode: Wraps payloads in EventEnvelope for cloud Kafka compatibility

    Topics:
    - user.profile.events: UserProfileCreated events
    - experience.events: ExperienceCreated events
    - feed.recommendation_served: RecommendationServed events
    - feed.recommendation_feedback: RecommendationFeedback events
    """
    from confluent_kafka import Producer

    print("\n" + "=" * 60)
    print(f"Publishing to Kafka ({'Protobuf/EventEnvelope' if use_protobuf else 'JSON'})")
    print("=" * 60)

    config = get_kafka_config()
    producer = Producer(config)

    print(f"  Bootstrap: {config['bootstrap.servers']}")
    print(f"  Mode: {'EventEnvelope (protobuf)' if use_protobuf else 'JSON (local testing)'}")

    results = {
        "user_profiles": 0,
        "experiences": 0,
        "recommendation_served": 0,
        "recommendation_feedback": 0,
    }

    def delivery_callback(err, msg):
        if err:
            print(f"  ❌ Delivery failed: {err}")

    def serialize_event(event_type: str, payload: dict) -> bytes:
        """Serialize event to JSON or EventEnvelope protobuf."""
        if not use_protobuf:
            return json.dumps(payload).encode("utf-8")

        # Use EventEnvelope for cloud Kafka
        try:
            import time

            from google.protobuf.json_format import ParseDict
            from togather_event_sdk.common.v1.event_envelop_pb2 import EventEnvelope

            # Get the inner payload class
            payload_class = _get_payload_class_for_event_type(event_type)
            if not payload_class:
                print(f"  ⚠ No proto class for {event_type}, using JSON")
                return json.dumps(payload).encode("utf-8")

            # Convert dict to protobuf
            payload_msg = payload_class()
            ParseDict(payload, payload_msg)
            payload_bytes = payload_msg.SerializeToString()

            # Wrap in EventEnvelope
            envelope = EventEnvelope()
            envelope.event_type = event_type
            envelope.event_version = 1
            envelope.timestamp = int(time.time() * 1000)
            envelope.payload = payload_bytes

            return envelope.SerializeToString()

        except ImportError:
            print("  ⚠ togather-event-sdk not installed, falling back to JSON")
            return json.dumps(payload).encode("utf-8")

    # Publish user profiles - always use production topic name
    topic = "user.profile.events"
    print(f"\n  Publishing user profiles to '{topic}'...")
    for event in data["user_profiles"]:
        producer.produce(
            topic=topic,
            key=event["id"].encode("utf-8"),
            value=serialize_event("user.v1.UserProfileCreated", event),
            callback=delivery_callback,
        )
        producer.poll(0)
        results["user_profiles"] += 1
    producer.flush()
    print(f"    ✓ Published {results['user_profiles']} user profiles")

    # Publish experiences - always use production topic name
    topic = "experience.events"
    print(f"\n  Publishing experiences to '{topic}'...")
    for event in data["experiences"]:
        producer.produce(
            topic=topic,
            key=event["id"].encode("utf-8"),
            value=serialize_event("experience.v1.ExperienceCreated", event),
            callback=delivery_callback,
        )
        producer.poll(0)
        results["experiences"] += 1
    producer.flush()
    print(f"    ✓ Published {results['experiences']} experiences")

    # Publish recommendation served - production topic name
    print("\n  Publishing to 'recommendation.served.v1'...")
    for event in data["recommendation_served"]:
        producer.produce(
            topic="recommendation.served.v1",
            key=event["request_id"].encode("utf-8"),
            value=serialize_event("feed.v1.RecommendationServedEvent", event),
            callback=delivery_callback,
        )
        producer.poll(0)
        results["recommendation_served"] += 1
    producer.flush()
    print(f"    ✓ Published {results['recommendation_served']} served events")

    # Publish recommendation feedback - production topic name
    print("\n  Publishing to 'recommendation.feedback.v1'...")
    for event in data["recommendation_feedback"]:
        producer.produce(
            topic="recommendation.feedback.v1",
            key=event["user_id"].encode("utf-8"),
            value=serialize_event("feed.v1.RecommendationFeedback", event),
            callback=delivery_callback,
        )
        producer.poll(0)
        results["recommendation_feedback"] += 1
    producer.flush()
    print(f"    ✓ Published {results['recommendation_feedback']} feedback events")

    print("\n" + "-" * 60)
    print("Kafka publishing complete!")
    print(f"  Total events published: {sum(results.values())}")
    print("-" * 60)

    return results


def _get_payload_class_for_event_type(event_type: str):
    """Get protobuf class for event_type string."""
    try:
        if event_type == "user.v1.UserProfileCreated":
            from togather_event_sdk.user.v1.user_profile_created_pb2 import (
                UserProfileCreated,
            )

            return UserProfileCreated
        elif event_type == "user.v1.UserAccountCreated":
            from togather_event_sdk.user.v1.user_account_created_pb2 import (
                UserAccountCreated,
            )

            return UserAccountCreated
        elif event_type == "experience.v1.ExperienceCreated":
            from togather_event_sdk.experience.v1.experience_created_pb2 import (
                ExperienceCreated,
            )

            return ExperienceCreated
        elif event_type == "feed.v1.RecommendationServedEvent":
            from togather_event_sdk.feed.v1.recommendation_served_pb2 import (
                RecommendationServedEvent,
            )

            return RecommendationServedEvent
        elif event_type == "feed.v1.RecommendationFeedback":
            from togather_event_sdk.feed.v1.recommendation_feedback_pb2 import (
                RecommendationFeedback,
            )

            return RecommendationFeedback
    except ImportError:
        pass
    return None


def verify_minio(timeout_seconds: int = 60) -> bool:
    """
    Verify events have flowed through Kafka → AppEventsProcessor → MinIO.
    """
    import time

    from dotenv import load_dotenv
    from minio import Minio

    load_dotenv(Path(__file__).parent.parent / ".env")

    endpoint = os.getenv("MINIO_ENDPOINT_URL", "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

    # Clean endpoint
    endpoint = endpoint.replace("http://", "").replace("https://", "")

    print("\nVerifying MinIO (Kafka → AppEventsProcessor → MinIO)...")
    print(f"  Endpoint: {endpoint}")
    print(f"  Waiting up to {timeout_seconds}s for events...")

    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )

    start_time = time.time()
    found = False

    while time.time() - start_time < timeout_seconds:
        try:
            objects = list(client.list_objects("raw-events", recursive=True))
            parquet_files = [o for o in objects if o.object_name.endswith(".parquet")]

            if parquet_files:
                print(f"  ✓ Found {len(parquet_files)} parquet files in raw-events/")
                for f in parquet_files[:5]:
                    print(f"      - {f.object_name}")
                found = True
                break
        except Exception:
            pass

        time.sleep(5)
        print(f"    Waiting... ({int(time.time() - start_time)}s)")

    if found:
        print("\n✓ MinIO verified!")
        return True
    else:
        print("\n⚠ Timeout. Check AppEventsProcessor logs.")
        return False


def verify_redis(timeout_seconds: int = 30) -> bool:
    """
    Verify real-time features have flowed through Kafka → Flink → Redis.
    """
    import time

    import redis
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).parent.parent / ".env")

    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    password = os.getenv("REDIS_PASSWORD", "")

    print("\nVerifying Redis (Kafka → Flink → Redis)...")
    print(f"  Host: {host}:{port}")

    try:
        client = redis.Redis(host=host, port=port, password=password, decode_responses=True)
        client.ping()
    except Exception as e:
        print(f"  ❌ Cannot connect to Redis: {e}")
        return False

    start_time = time.time()
    found = False

    # Look for feature keys written by Flink
    patterns = ["user:*:session", "user:*:features", "experience:*:features"]

    while time.time() - start_time < timeout_seconds:
        for pattern in patterns:
            keys = client.keys(pattern)
            if keys:
                print(f"  ✓ Found {len(keys)} keys matching '{pattern}'")
                for k in keys[:3]:
                    print(f"      - {k}")
                found = True
                break

        if found:
            break

        time.sleep(5)
        print(f"    Waiting... ({int(time.time() - start_time)}s)")

    if found:
        print("\n✓ Redis verified!")
        return True
    else:
        print("\n⚠ No Flink features found. Check Flink job logs.")
        return False


# =============================================================================
# Main
# =============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Test streaming pipeline: Kafka → Flink/AppEventsProcessor → MinIO/Redis"
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Generate proto-matching data and publish to Kafka",
    )
    parser.add_argument(
        "--proto",
        action="store_true",
        help="Use EventEnvelope protobuf format (for cloud Kafka). Default is JSON (local testing)",
    )
    parser.add_argument(
        "--verify-minio",
        action="store_true",
        help="Verify data arrived in MinIO (via AppEventsProcessor)",
    )
    parser.add_argument(
        "--verify-redis",
        action="store_true",
        help="Verify features in Redis (via Flink)",
    )
    parser.add_argument(
        "--save-json",
        action="store_true",
        help="Save generated data as JSON for inspection",
    )
    parser.add_argument(
        "--num-users",
        type=int,
        default=50,
        help="Number of users to generate",
    )
    parser.add_argument(
        "--num-experiences",
        type=int,
        default=20,
        help="Number of experiences to generate",
    )
    parser.add_argument(
        "--num-requests",
        type=int,
        default=100,
        help="Number of recommendation requests to generate",
    )

    args = parser.parse_args()

    if not any([args.publish, args.verify_minio, args.verify_redis]):
        parser.print_help()
        print("\n" + "=" * 60)
        print("USAGE EXAMPLES:")
        print("=" * 60)
        print("\n1. Local testing (JSON to local Kafka):")
        print(
            "   python scripts/test_real_feature_pipeline.py --publish --verify-minio --verify-redis"
        )
        print("\n2. Cloud Kafka (EventEnvelope protobuf):")
        print("   python scripts/test_real_feature_pipeline.py --publish --proto --verify-minio")
        print("\n3. Just publish to local Kafka:")
        print("   python scripts/test_real_feature_pipeline.py --publish --save-json")
        print("\n4. Just verify MinIO (if already published):")
        print("   python scripts/test_real_feature_pipeline.py --verify-minio")
        return

    print("=" * 60)
    print("Streaming Pipeline Test")
    print("Kafka → Flink → Redis")
    print("Kafka → AppEventsProcessor → MinIO")
    print("=" * 60)

    # Generate and publish
    if args.publish:
        data = generate_test_data(
            num_users=args.num_users,
            num_experiences=args.num_experiences,
            num_requests=args.num_requests,
        )

        if args.save_json:
            json_dir = Path(__file__).parent.parent / "mock_data" / "proto_matching"
            save_to_json(data, json_dir)

        try:
            publish_to_kafka(data, use_protobuf=args.proto)
        except Exception as e:
            print(f"\n❌ Failed to publish: {e}")
            print("   Check KAFKA_BOOTSTRAP_SERVERS in .env")
            return

    # Verify MinIO
    if args.verify_minio:
        verify_minio(timeout_seconds=60)

    # Verify Redis
    if args.verify_redis:
        verify_redis(timeout_seconds=30)

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()

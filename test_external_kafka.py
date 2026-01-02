#!/usr/bin/env python3
"""
Standalone script to test external Kafka connectivity with SSL/SASL authentication.

Uses togather-event-sdk for Protobuf deserialization.

Usage:
    1. Fill in the configuration section below with your cloud Kafka credentials
    2. Run: python scripts/test_external_kafka.py

This will:
    - Test consumer connectivity by consuming and deserializing Protobuf events
    - Test producer connectivity by sending a test message
"""

import json
import os
import tempfile
import time
import uuid
from datetime import datetime
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer

# =============================================================================
# CONFIGURATION - Fill in your external Kafka details here
# =============================================================================

# Broker address with port (e.g., "your-kafka.cloud.io:9093")
KAFKA_BOOTSTRAP_SERVERS = "<YOUR_KAFKA_BROKER>:9093"

# Security settings
KAFKA_SECURITY_PROTOCOL = "SASL_SSL"  # Options: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
KAFKA_SASL_MECHANISM = "PLAIN"  # Options: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512

# Credentials
KAFKA_USERNAME = "<YOUR_USERNAME>"
KAFKA_PASSWORD = "<YOUR_PASSWORD>"

# CA Certificate (paste the full PEM content between the triple quotes)
CA_CERTIFICATE = """-----BEGIN CERTIFICATE-----
<PASTE YOUR CA CERTIFICATE HERE>
-----END CERTIFICATE-----
"""

# Private Key (if using mTLS - leave empty if not needed)
PRIVATE_KEY = """
"""

# Available topics on your cloud Kafka:
#   - admin.account.event
#   - admin.auth.request.events
#   - experience.events
#   - partner.account.events
#   - partner.account.events.retry
#   - partner.auth.request.events
#   - partner.profile.events
#   - user.account.events
#   - user.account.events.dlq
#   - user.account.events.retry
#   - user.auth.request.events
#   - user.auth.request.events.retry
#   - user.password.reset-requested.event
#   - user.profile.events

# Test topic - using an existing topic for testing (consumer will read existing messages)
TEST_TOPIC = "user.account.events"

# =============================================================================
# PROTOBUF IMPORTS (from togather-event-sdk)
# =============================================================================

try:
    from togather_event_sdk.admin.v1.admin_account_created_pb2 import (
        AdminAccountCreated,
    )
    from togather_event_sdk.admin.v1.admin_email_verification_requested_pb2 import (
        AdminEmailVerificationRequested,
    )
    from togather_event_sdk.common.v1.event_envelop_pb2 import EventEnvelope
    from togather_event_sdk.experience.v1.experience_created_pb2 import (
        ExperienceCreated,
    )
    from togather_event_sdk.partner.v1.parter_account_created_pb2 import (
        PartnerAccountCreated,
    )
    from togather_event_sdk.partner.v1.partner_application_rejected_pb2 import (
        PartnerApplicationRejected,
    )
    from togather_event_sdk.partner.v1.partner_emai_verification_requested_pb2 import (
        PartnerEmailVerificationRequested,
    )
    from togather_event_sdk.partner.v1.partner_profile_created_pb2 import (
        PartnerProfileCreated,
    )
    from togather_event_sdk.user.v1.user_account_created_pb2 import UserAccountCreated
    from togather_event_sdk.user.v1.user_email_verification_requested_pb2 import (
        UserEmailVerificationRequested,
    )
    from togather_event_sdk.user.v1.user_forgot_password_pb2 import UserForgotPassword
    from togather_event_sdk.user.v1.user_profile_created_pb2 import UserProfileCreated

    PROTOBUF_AVAILABLE = True
    print("✅ togather-event-sdk loaded successfully")
except ImportError as e:
    PROTOBUF_AVAILABLE = False
    print(f"⚠️  togather-event-sdk not installed: {e}")
    print("   Install with: pip install togather-event-sdk==1.0.50")

# =============================================================================
# EVENT TYPE MAPPING
# Maps event_type string from EventEnvelope to the correct Protobuf class
# =============================================================================

EVENT_TYPE_MAPPING: dict[str, Any] = {}

if PROTOBUF_AVAILABLE:
    EVENT_TYPE_MAPPING = {
        # User events
        "user.account.created": UserAccountCreated,
        "user.profile.created": UserProfileCreated,
        "user.email.verification.requested": UserEmailVerificationRequested,
        "user.forgot.password": UserForgotPassword,
        # Admin events
        "admin.account.created": AdminAccountCreated,
        "admin.email.verification.requested": AdminEmailVerificationRequested,
        # Experience events
        "experience.created": ExperienceCreated,
        # Partner events
        "partner.account.created": PartnerAccountCreated,
        "partner.profile.created": PartnerProfileCreated,
        "partner.email.verification.requested": PartnerEmailVerificationRequested,
        "partner.application.rejected": PartnerApplicationRejected,
    }

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def create_temp_cert_file(content: str, suffix: str = ".pem") -> str | None:
    """Create a temporary file with certificate content."""
    if not content or content.strip() in [
        "",
        "-----BEGIN CERTIFICATE-----\n<PASTE YOUR CA CERTIFICATE HERE>\n-----END CERTIFICATE-----",
    ]:
        return None

    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False)
    temp_file.write(content.strip())
    temp_file.close()
    return temp_file.name


def get_consumer_config(ca_cert_path: str | None) -> dict:
    """Build consumer configuration."""
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"test-consumer-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "security.protocol": KAFKA_SECURITY_PROTOCOL,
    }

    # Add SASL config if using SASL
    if "SASL" in KAFKA_SECURITY_PROTOCOL:
        config["sasl.mechanism"] = KAFKA_SASL_MECHANISM
        config["sasl.username"] = KAFKA_USERNAME
        config["sasl.password"] = KAFKA_PASSWORD

    # Add SSL config if using SSL
    if "SSL" in KAFKA_SECURITY_PROTOCOL and ca_cert_path:
        config["ssl.ca.location"] = ca_cert_path

    return config


def get_producer_config(ca_cert_path: str | None) -> dict:
    """Build producer configuration."""
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": f"test-producer-{uuid.uuid4().hex[:8]}",
        "security.protocol": KAFKA_SECURITY_PROTOCOL,
    }

    # Add SASL config if using SASL
    if "SASL" in KAFKA_SECURITY_PROTOCOL:
        config["sasl.mechanism"] = KAFKA_SASL_MECHANISM
        config["sasl.username"] = KAFKA_USERNAME
        config["sasl.password"] = KAFKA_PASSWORD

    # Add SSL config if using SSL
    if "SSL" in KAFKA_SECURITY_PROTOCOL and ca_cert_path:
        config["ssl.ca.location"] = ca_cert_path

    return config


def deserialize_event(raw_value: bytes) -> dict[str, Any]:
    """
    Deserialize a Kafka message using the togather-event-sdk.

    Message format:
    1. Outer: EventEnvelope (protobuf)
       - event_type: string (identifies the payload schema)
       - event_version: int
       - timestamp: int64
       - trace_id: string (optional)
       - payload: bytes (inner protobuf message)

    2. Inner: Specific event type (e.g., UserAccountCreated)
    """
    result = {
        "raw_size": len(raw_value),
        "deserialize_success": False,
    }

    if not PROTOBUF_AVAILABLE:
        result["error"] = "togather-event-sdk not installed"
        result["raw_hex"] = raw_value[:100].hex()
        return result

    try:
        # Step 1: Deserialize the outer EventEnvelope
        envelope = EventEnvelope()
        envelope.ParseFromString(raw_value)

        result["event_type"] = envelope.event_type
        result["event_version"] = envelope.event_version
        result["timestamp"] = envelope.timestamp
        result["trace_id"] = envelope.trace_id if envelope.HasField("trace_id") else None
        result["payload_size"] = len(envelope.payload)

        # Step 2: Deserialize the inner payload based on event_type
        payload_class = EVENT_TYPE_MAPPING.get(envelope.event_type)

        if payload_class:
            payload_msg = payload_class()
            payload_msg.ParseFromString(envelope.payload)

            # Convert to dict for display
            from google.protobuf.json_format import MessageToDict

            result["payload"] = MessageToDict(payload_msg)
            result["deserialize_success"] = True
        else:
            result["payload_warning"] = f"Unknown event_type: {envelope.event_type}"
            result["payload_hex"] = envelope.payload[:100].hex() if envelope.payload else None
            result["known_event_types"] = list(EVENT_TYPE_MAPPING.keys())
            result["deserialize_success"] = True  # Envelope worked, just unknown payload type

    except Exception as e:
        result["error"] = str(e)
        result["raw_hex"] = raw_value[:100].hex()

    return result


def test_consumer(ca_cert_path: str | None, max_messages: int = 5):
    """Test consuming and deserializing messages from the external Kafka."""
    print("\n" + "=" * 60)
    print("TESTING CONSUMER (with Protobuf deserialization)")
    print("=" * 60)

    try:
        config = get_consumer_config(ca_cert_path)
        print(f"  Bootstrap Servers: {config['bootstrap.servers']}")
        print(f"  Security Protocol: {config['security.protocol']}")
        print(f"  Consumer Group: {config['group.id']}")

        print("\n  Creating consumer...")
        consumer = Consumer(config)

        print(f"  Subscribing to topic '{TEST_TOPIC}'...")
        consumer.subscribe([TEST_TOPIC])

        print(f"  Polling for messages (max: {max_messages}, timeout: 30 seconds)...")

        start_time = time.time()
        messages_received = 0

        while time.time() - start_time < 30 and messages_received < max_messages:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"    Reached end of partition {msg.partition()}")
                    continue
                else:
                    print(f"  ❌ Consumer error: {msg.error()}")
                    break

            messages_received += 1
            print(f"\n  ━━━ Message {messages_received} ━━━")
            print(f"    Topic: {msg.topic()}")
            print(f"    Partition: {msg.partition()}")
            print(f"    Offset: {msg.offset()}")
            print(f"    Key: {msg.key().decode('utf-8') if msg.key() else None}")

            # Deserialize the protobuf message
            if msg.value():
                result = deserialize_event(msg.value())

                if result.get("deserialize_success"):
                    print("    ✅ Deserialized successfully!")
                    print(f"    Event Type: {result.get('event_type')}")
                    print(f"    Event Version: {result.get('event_version')}")
                    print(f"    Timestamp: {result.get('timestamp')}")
                    print(f"    Trace ID: {result.get('trace_id')}")
                    if "payload" in result:
                        payload_str = json.dumps(result["payload"], indent=6)
                        # Truncate long payloads
                        if len(payload_str) > 500:
                            payload_str = payload_str[:500] + "..."
                        print(f"    Payload: {payload_str}")
                    if "payload_warning" in result:
                        print(f"    ⚠️  {result['payload_warning']}")
                else:
                    print(f"    ❌ Deserialization failed: {result.get('error')}")
                    print(f"    Raw (hex): {result.get('raw_hex')}")

        consumer.close()

        print("\n" + "-" * 60)
        if messages_received == 0:
            print("  ⚠️  No messages received within timeout.")
            print("      - The topic might be empty")
            print("      - Check if the topic name is correct")
        else:
            print(f"  ✅ Consumer test completed. Received {messages_received} message(s).")

    except Exception as e:
        print(f"\n  ❌ Consumer error: {e}")
        import traceback

        traceback.print_exc()


def test_producer_json(ca_cert_path: str | None):
    """Test producing a simple JSON message (for basic connectivity test)."""
    print("\n" + "=" * 60)
    print("TESTING PRODUCER (JSON message for connectivity)")
    print("=" * 60)

    try:
        config = get_producer_config(ca_cert_path)
        print(f"  Bootstrap Servers: {config['bootstrap.servers']}")
        print(f"  Security Protocol: {config['security.protocol']}")

        print("\n  Creating producer...")
        producer = Producer(config)

        # Generate test message (plain JSON for simplicity)
        test_message = {
            "test": True,
            "source": "test_external_kafka.py",
            "timestamp": datetime.utcnow().isoformat(),
        }

        message_key = f"test-{uuid.uuid4().hex[:8]}"
        message_value = json.dumps(test_message)

        print("  ⚠️  Note: This sends a JSON message, not Protobuf.")
        print(f"  Sending to topic '{TEST_TOPIC}'...")

        delivery_result = {"success": False, "error": None}

        def delivery_callback(err, msg):
            if err:
                delivery_result["error"] = str(err)
            else:
                delivery_result["success"] = True
                print("\n  ✅ Message delivered!")
                print(f"    Topic: {msg.topic()}")
                print(f"    Partition: {msg.partition()}")
                print(f"    Offset: {msg.offset()}")

        producer.produce(
            topic=TEST_TOPIC,
            key=message_key.encode("utf-8"),
            value=message_value.encode("utf-8"),
            callback=delivery_callback,
        )

        print("  Waiting for delivery...")
        producer.flush(timeout=30)

        if not delivery_result["success"]:
            print(f"\n  ❌ Delivery failed: {delivery_result['error']}")

    except Exception as e:
        print(f"\n  ❌ Producer error: {e}")


def main():
    print("\n" + "=" * 60)
    print("EXTERNAL KAFKA CONNECTIVITY TEST")
    print("with togather-event-sdk Protobuf deserialization")
    print("=" * 60)
    print(f"\nTimestamp: {datetime.now().isoformat()}")
    print(f"Target: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {TEST_TOPIC}")

    # Validate configuration
    if "<YOUR" in KAFKA_BOOTSTRAP_SERVERS:
        print("\n❌ ERROR: Please configure KAFKA_BOOTSTRAP_SERVERS")
        return

    if "<YOUR" in KAFKA_USERNAME or "<YOUR" in KAFKA_PASSWORD:
        print("\n❌ ERROR: Please configure KAFKA_USERNAME and KAFKA_PASSWORD")
        return

    # Create temporary cert files
    ca_cert_path = create_temp_cert_file(CA_CERTIFICATE)
    private_key_path = create_temp_cert_file(PRIVATE_KEY)

    if "SSL" in KAFKA_SECURITY_PROTOCOL and not ca_cert_path:
        print("\n⚠️  WARNING: SSL enabled but CA certificate not configured.")

    try:
        # Test 1: Consumer with Protobuf deserialization
        test_consumer(ca_cert_path)

        # Test 2: Producer (JSON for simplicity)
        # Uncomment if you want to test producing
        # test_producer_json(ca_cert_path)

        print("\n" + "=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)
        print("\n📝 If consumer test passed, you're ready to migrate!")
        print("   The feature-pipeline can now consume from external Kafka.")

    finally:
        # Cleanup temp files
        if ca_cert_path and os.path.exists(ca_cert_path):
            os.unlink(ca_cert_path)
        if private_key_path and os.path.exists(private_key_path):
            os.unlink(private_key_path)


if __name__ == "__main__":
    main()

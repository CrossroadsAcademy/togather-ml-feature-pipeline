"""
Event Envelope Parser for Flink.

Uses togather-event-sdk for protobuf parsing exclusively.
Expects raw bytes (passed through ISO-8859-1 encoding from Flink).
"""

import json
from typing import Any

# SDK IMPORTS

SDK_AVAILABLE = False
PAYLOAD_CLASSES: dict[str, Any] = {}

try:
    from togather_event_sdk.common.v1.event_envelop_pb2 import (
        EventEnvelope as SDKEventEnvelope,
    )

    SDK_AVAILABLE = True
except ImportError:
    print("[Parser] togather-event-sdk EventEnvelope not found.")

if SDK_AVAILABLE:

    def safe_sdk_import(module_path: str, class_name: str):
        """Safely import SDK class, returning None if not found."""
        try:
            module = __import__(module_path, fromlist=[class_name])
            return getattr(module, class_name)
        except (ImportError, AttributeError):
            return None

    # User events
    UserAccountCreated = safe_sdk_import(
        "togather_event_sdk.user.v1.user_account_created_pb2", "UserAccountCreated"
    )
    UserProfileCreated = safe_sdk_import(
        "togather_event_sdk.user.v1.user_profile_created_pb2", "UserProfileCreated"
    )

    # Experience events
    ExperienceCreated = safe_sdk_import(
        "togather_event_sdk.experience.v1.experience_created_pb2", "ExperienceCreated"
    )

    # Partner events
    PartnerProfileCreated = safe_sdk_import(
        "togather_event_sdk.partner.v1.partner_profile_created_pb2",
        "PartnerProfileCreated",
    )
    PartnerAccountCreatedPayload = safe_sdk_import(
        "togather_event_sdk.partner.v1.parter_account_created_pb2",
        "PartnerAccountCreatedPayload",
    )

    # Feed/Recommendation events
    RecommendationServedEvent = safe_sdk_import(
        "togather_event_sdk.feed.v1.recommendation_served_pb2",
        "RecommendationServedEvent",
    )
    RecommendationFeedback = safe_sdk_import(
        "togather_event_sdk.feed.v1.recommendation_feedback_pb2",
        "RecommendationFeedback",
    )

    from google.protobuf.json_format import MessageToDict

    print("[Parser] togather-event-sdk loaded successfully")

    # Build event type mapping (only verified events from production)
    PAYLOAD_CLASSES = {
        # User events
        "user.v1.UserAccountCreated": UserAccountCreated,
        "user.v1.UserProfileCreated": UserProfileCreated,
        # Experience events
        "experience.v1.ExperienceCreated": ExperienceCreated,
        # Partner events
        "partner.v1.PartnerProfileCreated": PartnerProfileCreated,
        "partner.v1.PartnerAccountCreatedPayload": PartnerAccountCreatedPayload,
        # Feed/Recommendation events
        "feed.v1.RecommendationServedEvent": RecommendationServedEvent,
        "feed.v1.RecommendationFeedback": RecommendationFeedback,
    }

    # Clean up None values if any imports failed
    PAYLOAD_CLASSES = {k: v for k, v in PAYLOAD_CLASSES.items() if v is not None}
else:
    print("[Parser] togather-event-sdk not available, parsing will fail.")


# PARSING FUNCTIONS


def parse_kafka_message(raw_string: str) -> dict[str, Any]:
    """
    Main entry point for Flink.

    Flink passes bytes as a string (ISO-8859-1 encoded).
    We convert back to bytes and parse using the SDK.
    """
    if not raw_string:
        return {"_parse_error": "Empty message"}

    # 1. Try JSON fallback (for debugging or test messages)
    try:
        if raw_string.strip().startswith("{"):
            data = json.loads(raw_string)
            data["_event_type"] = "json"
            return data
    except (json.JSONDecodeError, ValueError):
        pass

    # 2. Protobuf parsing using SDK
    try:
        if not SDK_AVAILABLE:
            return {"_parse_error": "togather-event-sdk not available"}

        # Convert the ISO-8859-1 string back to raw bytes
        if isinstance(raw_string, str):
            raw_bytes = raw_string.encode("iso-8859-1")
        else:
            raw_bytes = bytes(raw_string)

        if not raw_bytes:
            return {"_parse_error": "Empty bytes after encoding"}

        # Parse Outer Envelope
        envelope = SDKEventEnvelope()
        try:
            envelope.ParseFromString(raw_bytes)
        except Exception as e:
            # Add hex dump for debugging
            hex_dump = raw_bytes.hex()[:100]
            print(f"[Parser] ERROR: SDK failed to parse envelope. Error: {e}, Hex: {hex_dump}")
            return {
                "_parse_error": f"SDK envelope parse failed: {str(e)}",
                "_hex": hex_dump,
                "_raw_len": len(raw_bytes),
            }

        # Build result with envelope metadata
        result = {
            "_event_type": envelope.event_type,
            "_event_version": envelope.event_version,
            "_timestamp": envelope.timestamp,
        }

        if envelope.HasField("trace_id"):
            result["_trace_id"] = envelope.trace_id

        # Parse Inner Payload
        payload_class = PAYLOAD_CLASSES.get(envelope.event_type)
        if payload_class and envelope.payload:
            try:
                payload = payload_class()
                payload.ParseFromString(envelope.payload)
                payload_dict = MessageToDict(payload, preserving_proto_field_name=True)
                result.update(payload_dict)
            except Exception as e:
                result["_payload_error"] = f"Failed to parse payload {envelope.event_type}: {e}"
        elif envelope.event_type and envelope.event_type not in PAYLOAD_CLASSES:
            result["_payload_warning"] = f"Unknown event_type: {envelope.event_type}"

        return result

    except Exception as e:
        return {
            "_parse_error": f"Internal parser error: {str(e)}",
            "_raw_sample": str(raw_string)[:50] if raw_string else None,
        }


def get_user_id_from_event(event: dict[str, Any]) -> str | None:
    """Extract user_id from the parsed SDK dictionary."""
    if event.get("_parse_error"):
        return None

    # userId (SDK camelCase for feed events)
    if event.get("userId"):
        return str(event["userId"])

    # user_id (snake_case)
    if event.get("user_id"):
        return str(event["user_id"])

    # id field for user-related events
    event_type = str(event.get("_event_type", ""))
    if event_type.startswith("user.") or "AccountCreated" in event_type:
        if event.get("id"):
            return str(event["id"])

    # Look for numeric tags as last resort (if manual parser ran)
    # 2 is user_id in RecommendationFeedback
    # 1 is id in UserProfileCreated / UserAccountCreated
    for tag in ["2", "1", "user_id", "userId", "id"]:
        val = event.get(tag)
        if val and isinstance(val, str) and (val.startswith("user") or len(val) >= 20):
            return val

    return None


def get_timestamp_from_event(event: dict[str, Any]) -> int | None:
    """Extract timestamp in milliseconds."""
    if event.get("_timestamp"):
        return int(event["_timestamp"])

    for field in ["timestamp", "clientTimestamp", "createdAt"]:
        if event.get(field):
            val = event[field]
            try:
                if isinstance(val, (int | float)):
                    return int(val)
                elif isinstance(val, str) and val.isdigit():
                    return int(val)
            except (ValueError, TypeError):
                continue

    return None

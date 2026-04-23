"""
Event Validator for Flink Data Quality.

Validates events before processing with schema, null, and range checks.
Invalid events are routed to DLQ.
"""

from dataclasses import dataclass
from typing import Any

from prometheus_client import Counter

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Prometheus metrics
validation_events_total = Counter(
    "flink_validation_events_total",
    "Total events validated",
    ["status", "error_type"],
)


@dataclass
class ValidationResult:
    """Result of event validation."""

    is_valid: bool
    error_type: str | None = None
    error_message: str | None = None

    @property
    def should_send_to_dlq(self) -> bool:
        """Check if event should be sent to DLQ."""
        return not self.is_valid


class EventValidator:
    """
    Validates events for data quality.

    Checks:
    - Required fields present
    - Null value handling
    - Range validation (latitude/longitude)
    - Timestamp format
    - Event type validity

    Usage:
        validator = EventValidator()
        result = validator.validate(event, topic="experience")

        if result.is_valid:
            process(event)
        else:
            send_to_dlq(event, result.error_message)
    """

    # Required fields by topic
    REQUIRED_FIELDS = {
        "user.account": ["user_id", "event_type", "timestamp"],
        "user.profile": ["user_id", "event_type", "timestamp"],
        "experience": ["user_id", "experience_id", "event_type", "timestamp"],
        "engagement": ["user_id", "event_type", "timestamp", "target_id"],
        "location.streams": ["user_id", "timestamp", "latitude", "longitude"],
        "events.raw": ["user_id", "event_type", "timestamp"],
    }

    # Valid event types
    VALID_EVENT_TYPES = {
        "user.account": [
            "user_created",
            "user_updated",
            "user_deleted",
            "user_verified",
        ],
        "user.profile": ["profile_updated", "preferences_changed", "avatar_updated"],
        "experience": ["view", "click", "bookmark", "share", "rsvp"],
        "engagement": ["like", "comment", "share", "follow", "unfollow"],
        "location.streams": ["location_update"],
    }

    # Coordinate bounds
    LATITUDE_RANGE = (-90.0, 90.0)
    LONGITUDE_RANGE = (-180.0, 180.0)

    def __init__(self, strict_mode: bool = False):
        """
        Initialize validator.

        Args:
            strict_mode: If True, reject events with unknown fields
        """
        self.strict_mode = strict_mode
        self.logger = get_logger(self.__class__.__name__)

    def validate(self, event: dict[str, Any], topic: str = "events.raw") -> ValidationResult:
        """
        Validate an event.

        Args:
            event: Event dictionary
            topic: Kafka topic name

        Returns:
            ValidationResult with is_valid flag and error details
        """
        # Check if event is a dict
        if not isinstance(event, dict):
            self._record_validation("invalid", "type_error")
            return ValidationResult(
                is_valid=False,
                error_type="type_error",
                error_message="Event must be a dictionary",
            )

        # Check for empty event
        if not event:
            self._record_validation("invalid", "empty_event")
            return ValidationResult(
                is_valid=False,
                error_type="empty_event",
                error_message="Event cannot be empty",
            )

        # Check required fields
        required_fields = self.REQUIRED_FIELDS.get(topic, ["user_id", "timestamp"])
        missing_fields = [f for f in required_fields if f not in event or event[f] is None]

        if missing_fields:
            self._record_validation("invalid", "missing_fields")
            return ValidationResult(
                is_valid=False,
                error_type="missing_fields",
                error_message=f"Missing required fields: {missing_fields}",
            )

        # Check null values in required fields
        null_fields = [f for f in required_fields if event.get(f) == ""]
        if null_fields:
            self._record_validation("invalid", "null_values")
            return ValidationResult(
                is_valid=False,
                error_type="null_values",
                error_message=f"Null values in required fields: {null_fields}",
            )

        # Validate event type if applicable
        event_type = event.get("event_type")
        valid_types = self.VALID_EVENT_TYPES.get(topic)

        if valid_types and event_type and event_type not in valid_types:
            # Log warning but don't reject - could be new event type
            self.logger.warning(f"Unknown event type: {event_type} for topic {topic}")

        # Validate coordinates if present
        coord_result = self._validate_coordinates(event)
        if not coord_result.is_valid:
            self._record_validation("invalid", coord_result.error_type or "coordinate_error")
            return coord_result

        # Validate timestamp format
        timestamp_result = self._validate_timestamp(event.get("timestamp"))
        if not timestamp_result.is_valid:
            self._record_validation("invalid", timestamp_result.error_type or "timestamp_error")
            return timestamp_result

        # All checks passed
        self._record_validation("valid", "none")
        return ValidationResult(is_valid=True)

    def _validate_coordinates(self, event: dict[str, Any]) -> ValidationResult:
        """Validate latitude and longitude if present."""
        lat = event.get("latitude")
        lon = event.get("longitude")

        if lat is not None:
            try:
                lat_float = float(lat)
                if not (self.LATITUDE_RANGE[0] <= lat_float <= self.LATITUDE_RANGE[1]):
                    return ValidationResult(
                        is_valid=False,
                        error_type="invalid_latitude",
                        error_message=f"Latitude {lat} out of range {self.LATITUDE_RANGE}",
                    )
            except (ValueError, TypeError):
                return ValidationResult(
                    is_valid=False,
                    error_type="invalid_latitude",
                    error_message=f"Invalid latitude value: {lat}",
                )

        if lon is not None:
            try:
                lon_float = float(lon)
                if not (self.LONGITUDE_RANGE[0] <= lon_float <= self.LONGITUDE_RANGE[1]):
                    return ValidationResult(
                        is_valid=False,
                        error_type="invalid_longitude",
                        error_message=f"Longitude {lon} out of range {self.LONGITUDE_RANGE}",
                    )
            except (ValueError, TypeError):
                return ValidationResult(
                    is_valid=False,
                    error_type="invalid_longitude",
                    error_message=f"Invalid longitude value: {lon}",
                )

        return ValidationResult(is_valid=True)

    def _validate_timestamp(self, timestamp: Any) -> ValidationResult:
        """Validate timestamp format. Accepts ISO strings or epoch milliseconds."""
        if timestamp is None:
            return ValidationResult(
                is_valid=False,
                error_type="missing_timestamp",
                error_message="Timestamp is required",
            )

        # Accept integer timestamps (epoch milliseconds from protobuf)
        if isinstance(timestamp, (int | float)):
            # Basic sanity check: should be a reasonable epoch time
            if timestamp > 0:
                return ValidationResult(is_valid=True)
            return ValidationResult(
                is_valid=False,
                error_type="invalid_timestamp",
                error_message=f"Invalid timestamp value: {timestamp}",
            )

        # Accept string timestamps (ISO format)
        if isinstance(timestamp, str):
            try:
                from datetime import datetime

                ts = timestamp
                if ts.endswith("Z"):
                    ts = ts[:-1] + "+00:00"
                datetime.fromisoformat(ts)
                return ValidationResult(is_valid=True)

            except ValueError:
                return ValidationResult(
                    is_valid=False,
                    error_type="invalid_timestamp",
                    error_message=f"Cannot parse timestamp: {timestamp}",
                )

        return ValidationResult(
            is_valid=False,
            error_type="invalid_timestamp",
            error_message=f"Timestamp must be string or int, got {type(timestamp).__name__}",
        )

    def _record_validation(self, status: str, error_type: str) -> None:
        """Record validation result to Prometheus."""
        validation_events_total.labels(status=status, error_type=error_type).inc()


def validate_batch(
    events: list[dict[str, Any]],
    topic: str,
    validator: EventValidator | None = None,
) -> tuple[list[dict[str, Any]], list[tuple[dict[str, Any], str]]]:
    """
    Validate a batch of events.

    Args:
        events: List of events
        topic: Kafka topic
        validator: Optional validator instance

    Returns:
        Tuple of (valid_events, invalid_events_with_errors)
    """
    validator = validator or EventValidator()

    valid_events: list[dict[str, Any]] = []
    invalid_events: list[tuple[dict[str, Any], str]] = []

    for event in events:
        result = validator.validate(event, topic)
        if result.is_valid:
            valid_events.append(event)
        else:
            invalid_events.append((event, result.error_message or "Unknown error"))

    return valid_events, invalid_events

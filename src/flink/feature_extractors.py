"""
Feature Extractors for Flink Session Aggregation.

Extracts features from event streams for real-time ML inference.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class SessionFeatures:
    """Features computed for a user session."""

    user_id: str
    session_id: str
    session_start: datetime
    session_end: datetime
    session_duration_seconds: float = 0.0
    activity_count: int = 0
    location_changes: int = 0
    unique_event_types: int = 0
    last_event_type: str = ""
    last_latitude: float | None = None
    last_longitude: float | None = None
    engagement_score: float = 0.0
    event_type_counts: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for Redis storage."""
        return {
            "user_id": self.user_id,
            "session_id": self.session_id,
            "session_start": self.session_start.isoformat(),
            "session_end": self.session_end.isoformat(),
            "session_duration_seconds": str(self.session_duration_seconds),
            "activity_count": str(self.activity_count),
            "location_changes": str(self.location_changes),
            "unique_event_types": str(self.unique_event_types),
            "last_event_type": self.last_event_type,
            "last_latitude": str(self.last_latitude) if self.last_latitude else "",
            "last_longitude": str(self.last_longitude) if self.last_longitude else "",
            "engagement_score": str(self.engagement_score),
        }


def extract_session_features(events: list[dict[str, Any]]) -> SessionFeatures | None:
    """
    Extract session-level features from a list of events.

    Args:
        events: List of event dictionaries within a session window

    Returns:
        SessionFeatures dataclass with computed features
    """
    if not events:
        return None

    # Sort events by timestamp
    sorted_events = sorted(events, key=lambda e: e.get("timestamp", ""))

    first_event = sorted_events[0]
    last_event = sorted_events[-1]

    user_id = first_event.get("user_id", "unknown")
    session_id = first_event.get("session_id") or _generate_session_id(user_id, first_event)

    # Parse timestamps
    session_start = _parse_timestamp(first_event.get("timestamp"))
    session_end = _parse_timestamp(last_event.get("timestamp"))

    # Compute duration
    session_duration = (
        (session_end - session_start).total_seconds() if session_start and session_end else 0.0
    )

    # Count activities and event types
    event_type_counts: dict[str, int] = {}
    for event in sorted_events:
        event_type = event.get("event_type", "unknown")
        event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1

    # Count location changes
    location_changes = _count_location_changes(sorted_events)

    # Get last location
    last_lat, last_lon = _get_last_location(sorted_events)

    # Compute engagement score
    engagement_score = compute_engagement_score(sorted_events)

    return SessionFeatures(
        user_id=user_id,
        session_id=session_id,
        session_start=session_start or datetime.now(timezone.utc),
        session_end=session_end or datetime.now(timezone.utc),
        session_duration_seconds=session_duration,
        activity_count=len(events),
        location_changes=location_changes,
        unique_event_types=len(event_type_counts),
        last_event_type=last_event.get("event_type", ""),
        last_latitude=last_lat,
        last_longitude=last_lon,
        engagement_score=engagement_score,
        event_type_counts=event_type_counts,
    )


def compute_engagement_score(events: list[dict[str, Any]]) -> float:
    """
    Compute engagement score based on event types and patterns.

    Scoring:
    - view: 1 point
    - click: 2 points
    - bookmark: 3 points
    - share: 4 points
    - rsvp: 5 points
    - like: 2 points
    - comment: 3 points

    Args:
        events: List of events

    Returns:
        Normalized engagement score (0-1)
    """
    if not events:
        return 0.0

    event_weights = {
        "view": 1.0,
        "click": 2.0,
        "bookmark": 3.0,
        "share": 4.0,
        "rsvp": 5.0,
        "like": 2.0,
        "comment": 3.0,
        "follow": 2.0,
    }

    total_score = 0.0
    for event in events:
        event_type = event.get("event_type", "").lower()
        total_score += event_weights.get(event_type, 0.5)

    # Normalize by max possible score (5 points per event)
    max_possible = len(events) * 5.0
    normalized_score = min(total_score / max_possible, 1.0) if max_possible > 0 else 0.0

    return round(normalized_score, 4)


def extract_location_features(events: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Extract location-based features from events.

    Args:
        events: List of events with location data

    Returns:
        Dictionary with location features
    """
    location_events = [
        e for e in events if e.get("latitude") is not None and e.get("longitude") is not None
    ]

    if not location_events:
        return {
            "has_location": False,
            "location_count": 0,
            "location_changes": 0,
        }

    return {
        "has_location": True,
        "location_count": len(location_events),
        "location_changes": _count_location_changes(location_events),
        "first_latitude": location_events[0].get("latitude"),
        "first_longitude": location_events[0].get("longitude"),
        "last_latitude": location_events[-1].get("latitude"),
        "last_longitude": location_events[-1].get("longitude"),
    }


def _parse_timestamp(ts_str: str | None) -> datetime | None:
    """Parse ISO timestamp string to datetime."""
    if not ts_str:
        return None
    try:
        # Handle various ISO formats
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        return datetime.fromisoformat(ts_str)
    except (ValueError, TypeError):
        return None


def _generate_session_id(user_id: str, first_event: dict[str, Any]) -> str:
    """Generate session ID from user and timestamp."""
    import hashlib

    timestamp = first_event.get("timestamp", "")
    data = f"{user_id}:{timestamp}"
    return hashlib.md5(data.encode()).hexdigest()[:12]


def _count_location_changes(events: list[dict[str, Any]], threshold_meters: float = 100) -> int:
    """
    Count significant location changes in event sequence.

    Args:
        events: Sorted list of events
        threshold_meters: Minimum distance to count as a change

    Returns:
        Number of location changes
    """
    changes = 0
    prev_lat, prev_lon = None, None

    for event in events:
        lat = event.get("latitude")
        lon = event.get("longitude")

        if lat is not None and lon is not None:
            if prev_lat is not None and prev_lon is not None:
                distance = _haversine_distance(prev_lat, prev_lon, lat, lon)
                if distance > threshold_meters:
                    changes += 1
            prev_lat, prev_lon = lat, lon

    return changes


def _get_last_location(
    events: list[dict[str, Any]],
) -> tuple[float | None, float | None]:
    """Get last known location from events."""
    for event in reversed(events):
        lat = event.get("latitude")
        lon = event.get("longitude")
        if lat is not None and lon is not None:
            return lat, lon
    return None, None


def _haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate distance between two coordinates in meters.

    Uses Haversine formula for accuracy on Earth's surface.
    """
    import math

    R = 6371000  # Earth's radius in meters

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c

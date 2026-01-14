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
            "session_duration_seconds": self.session_duration_seconds,
            "activity_count": self.activity_count,
            "location_changes": self.location_changes,
            "unique_event_types": self.unique_event_types,
            "last_event_type": self.last_event_type,
            "last_latitude": (self.last_latitude if self.last_latitude is not None else ""),
            "last_longitude": (self.last_longitude if self.last_longitude is not None else ""),
            "engagement_score": self.engagement_score,
        }


@dataclass
class RealtimeUserFeatures:
    """Real-time user intent signals for recommendations.

    These features are computed from the current session and recent events,
    enabling:
    - Heuristic recommendations (cold start, category matching)
    - Ranking model personalization (fresh user signals)
    - Diversity in re-ranking (avoid repetition)
    """

    user_id: str
    session_id: str

    # Category/tag preferences from current session
    recent_categories_viewed: list[str] = field(default_factory=list)  # Last 5 categories (deduped)
    recent_tags_interacted: list[str] = field(default_factory=list)  # Last 10 tags (deduped)
    session_category_focus: str = ""  # Dominant category in session

    # Interaction history for negative sampling and diversity
    user_viewed_experiences: list[str] = field(default_factory=list)  # Last 50 experience IDs
    already_shown_session: list[str] = field(
        default_factory=list
    )  # Shown this session (for diversity)

    # Session context
    session_engagement_score: float = 0.0
    last_event_timestamp: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for Redis storage."""
        import json

        return {
            "user_id": self.user_id,
            "session_id": self.session_id,
            "recent_categories_viewed": json.dumps(self.recent_categories_viewed),
            "recent_tags_interacted": json.dumps(self.recent_tags_interacted),
            "session_category_focus": self.session_category_focus,
            "user_viewed_experiences": json.dumps(self.user_viewed_experiences),
            "already_shown_session": json.dumps(self.already_shown_session),
            "session_engagement_score": self.session_engagement_score,
            "last_event_timestamp": (
                self.last_event_timestamp.isoformat() if self.last_event_timestamp else ""
            ),
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

    # Use user_id injected by the parser/job
    user_id = first_event.get("user_id") or first_event.get("id") or "unknown"
    session_id = (
        first_event.get("session_id")
        or first_event.get("sessionId")
        or _generate_session_id(user_id, first_event)
    )

    # Parse timestamps - Prefer normalized _extracted_timestamp
    session_start = _parse_timestamp(
        first_event.get("_extracted_timestamp") or first_event.get("timestamp")
    )
    session_end = _parse_timestamp(
        last_event.get("_extracted_timestamp") or last_event.get("timestamp")
    )

    # Compute duration
    session_duration = (
        (session_end - session_start).total_seconds() if session_start and session_end else 0.0
    )

    # Count activities and event types
    event_type_counts: dict[str, int] = {}
    for event in sorted_events:
        event_type = _get_event_type(event)
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
        last_event_type=_get_event_type(last_event),
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
        event_type = _get_event_type(event).lower()
        if not event_type or event_type == "unknown":
            total_score += 0.5
            continue

        # Check specific feedback types if present
        if "feedback" in event_type:
            feedback_type = event.get("event_type", "").lower()
            if "view" in feedback_type:
                total_score += 1.0
            elif "click" in feedback_type:
                total_score += 2.0
            else:
                total_score += 0.5
        else:
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


def _get_event_type(event: dict[str, Any]) -> str:
    """Standardize event type extraction.

    Priority:
    1. Inner `event_type` field (e.g., "View", "Like", "Save") - actual user action
    2. Outer `_event_type` field (e.g., "feed.v1.RecommendationFeedback") - proto message name
    """
    # First, check for inner event_type (the actual action like View, Like, etc.)
    inner_type = event.get("event_type")
    if inner_type and isinstance(inner_type, str) and inner_type not in ("", "unknown"):
        return str(inner_type)

    # Fallback to outer _event_type (proto message name, strip package prefix)
    raw_type = event.get("_event_type") or "unknown"
    return str(raw_type.split(".")[-1]) if "." in str(raw_type) else str(raw_type)


def _parse_timestamp(ts_value: str | int | None) -> datetime | None:
    """Parse timestamp to datetime. Handles ISO strings and epoch milliseconds."""
    if ts_value is None:
        return None
    try:
        # Handle integer timestamps (epoch milliseconds from protobuf)
        if isinstance(ts_value, (int | float)):
            ts_float = float(ts_value)
            if ts_float > 1e12:  # Milliseconds
                return datetime.fromtimestamp(ts_float / 1000.0)
            else:  # Seconds
                return datetime.fromtimestamp(ts_float)

        # Handle string timestamps (ISO format)
        if isinstance(ts_value, str):
            ts_str = ts_value
            if ts_str.endswith("Z"):
                ts_str = ts_str[:-1] + "+00:00"
            return datetime.fromisoformat(ts_str)

        return None
    except (ValueError, TypeError, OSError):
        return None


def _generate_session_id(user_id: str, first_event: dict[str, Any]) -> str:
    """Generate session ID from user and timestamp."""
    import hashlib

    timestamp = first_event.get("timestamp", "")
    data = f"{user_id}:{timestamp}"
    return hashlib.md5(data.encode()).hexdigest()[:12]


def _extract_location(event: dict[str, Any]) -> tuple[float | None, float | None]:
    """
    Extract latitude/longitude from event, handling nested structures.

    Tries in order:
    1. event_location_coordinate (ExperienceCreated)
    2. current_address.coordinate (FeedMissTrigger, UserProfileCreated)
    3. Top-level latitude/longitude fields
    """
    # Try event_location_coordinate (ExperienceCreated events)
    event_location = event.get("event_location_coordinate") or event.get("eventLocationCoordinate")
    if event_location and isinstance(event_location, dict):
        lat = event_location.get("latitude")
        lon = event_location.get("longitude")
        if lat is not None and lon is not None:
            return float(lat), float(lon)

    # Try nested: current_address.coordinate.latitude/longitude
    current_address = event.get("current_address") or event.get("currentAddress")
    if current_address and isinstance(current_address, dict):
        coordinate = current_address.get("coordinate")
        if coordinate and isinstance(coordinate, dict):
            lat = coordinate.get("latitude")
            lon = coordinate.get("longitude")
            if lat is not None and lon is not None:
                return float(lat), float(lon)

    # Try flat structure
    lat = event.get("latitude")
    lon = event.get("longitude")
    if lat is not None and lon is not None:
        return float(lat), float(lon)

    return None, None


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
        lat, lon = _extract_location(event)

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
    """Get last known location from events, handling nested structures."""
    for event in reversed(events):
        lat, lon = _extract_location(event)
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


# Real-Time User Feature Extraction


def _extract_category_from_event(event: dict[str, Any]) -> str | None:
    """Extract category from an event.

    Tries multiple field names to handle different event types.
    """
    # Direct category field
    category = event.get("category_name") or event.get("category")
    if category and isinstance(category, str):
        return str(category)

    # Nested category object
    cat_obj = event.get("category")
    if cat_obj and isinstance(cat_obj, dict):
        val = cat_obj.get("name") or cat_obj.get("id")
        return str(val) if val else None

    # From experience context in recommendation events
    exp_context = event.get("experience_context") or event.get("experienceContext")
    if exp_context and isinstance(exp_context, dict):
        val = exp_context.get("category_name") or exp_context.get("category")
        return str(val) if val else None

    return None


def _extract_tags_from_event(event: dict[str, Any]) -> list[str]:
    """Extract tag names from an event."""
    tags = []

    # Direct tags array
    raw_tags = event.get("tags") or event.get("exp_tags")
    if raw_tags and isinstance(raw_tags, list):
        for tag in raw_tags:
            if isinstance(tag, str):
                tags.append(tag)
            elif isinstance(tag, dict):
                tag_name = tag.get("name") or tag.get("id")
                if tag_name:
                    tags.append(str(tag_name))

    # From experience context
    exp_context = event.get("experience_context") or event.get("experienceContext")
    if exp_context and isinstance(exp_context, dict):
        ctx_tags = exp_context.get("tags") or []
        for tag in ctx_tags:
            if isinstance(tag, str):
                tags.append(tag)
            elif isinstance(tag, dict):
                tag_name = tag.get("name")
                if tag_name:
                    tags.append(str(tag_name))

    return tags


def _extract_experience_id_from_event(event: dict[str, Any]) -> str | None:
    """Extract experience ID from an event."""
    # Direct fields
    exp_id = event.get("experience_id") or event.get("experienceId") or event.get("event_id")
    if exp_id:
        return str(exp_id)

    # From recommendations (served events)
    recommendations = event.get("recommendations") or []
    if recommendations and isinstance(recommendations, list):
        # Return first recommended experience
        first_rec = recommendations[0] if recommendations else {}
        if isinstance(first_rec, dict):
            val = first_rec.get("experience_id") or first_rec.get("experienceId")
            return str(val) if val else None

    return None


def extract_realtime_user_features(
    events: list[dict[str, Any]],
    max_categories: int = 5,
    max_tags: int = 10,
    max_viewed_experiences: int = 50,
) -> RealtimeUserFeatures | None:
    """
    Extract real-time user features from a list of events.

    These features power:
    - Heuristic recommendations (category/tag matching for cold start)
    - Ranking model (fresh user signals for personalization)
    - Diversity (avoid showing same experiences again)

    Args:
        events: List of event dictionaries within a session window
        max_categories: Maximum categories to track (default 5)
        max_tags: Maximum tags to track (default 10)
        max_viewed_experiences: Maximum viewed experience IDs to track

    Returns:
        RealtimeUserFeatures dataclass with computed features
    """
    if not events:
        return None

    # Helper to normalize timestamp to int for sorting (handles str/int mix)
    def _get_sortable_timestamp(event: dict) -> int:
        ts = (
            event.get("_extracted_timestamp")
            or event.get("timestamp")
            or event.get("_timestamp")
            or 0
        )
        if isinstance(ts, str):
            # Try to parse as int, else use 0
            try:
                return int(ts)
            except ValueError:
                # ISO format string - parse to timestamp
                try:
                    from datetime import datetime

                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    return int(dt.timestamp() * 1000)
                except (ValueError, AttributeError):
                    return 0
        return int(ts) if ts else 0

    # Sort by timestamp (normalized to int)
    sorted_events = sorted(events, key=_get_sortable_timestamp)

    first_event = sorted_events[0]
    last_event = sorted_events[-1]

    # Extract user and session IDs
    user_id = first_event.get("user_id") or first_event.get("id") or "unknown"
    session_id = first_event.get("session_id") or _generate_session_id(user_id, first_event)

    # Track categories (maintain order, dedupe)
    categories_seen: list[str] = []
    for event in sorted_events:
        category = _extract_category_from_event(event)
        if category and category not in categories_seen:
            categories_seen.append(category)
    recent_categories = categories_seen[-max_categories:]

    # Track tags (maintain order, dedupe)
    tags_seen: list[str] = []
    for event in sorted_events:
        tags = _extract_tags_from_event(event)
        for tag in tags:
            if tag not in tags_seen:
                tags_seen.append(tag)
    recent_tags = tags_seen[-max_tags:]

    # Determine dominant category (most frequent)
    category_counts: dict[str, int] = {}
    for event in sorted_events:
        category = _extract_category_from_event(event)
        if category:
            category_counts[category] = category_counts.get(category, 0) + 1

    session_category_focus = ""
    if category_counts:
        session_category_focus = max(category_counts, key=category_counts.get)  # type: ignore

    # Track viewed experiences (for negative sampling)
    viewed_experiences: list[str] = []
    for event in sorted_events:
        exp_id = _extract_experience_id_from_event(event)
        if exp_id and exp_id not in viewed_experiences:
            viewed_experiences.append(exp_id)
    viewed_experiences = viewed_experiences[-max_viewed_experiences:]

    # Track shown experiences this session (for diversity)
    # These are experience IDs from recommendation.served events
    already_shown: list[str] = []
    for event in sorted_events:
        event_type = event.get("_event_type") or ""
        if "Served" in event_type or "served" in event_type.lower():
            recommendations = event.get("recommendations") or []
            for rec in recommendations:
                if isinstance(rec, dict):
                    # RecommendedItem uses event_id (not experience_id) per protobuf
                    exp_id = (
                        rec.get("event_id") or rec.get("experience_id") or rec.get("experienceId")
                    )
                    if exp_id and exp_id not in already_shown:
                        already_shown.append(str(exp_id))

    # Compute engagement score
    engagement_score = compute_engagement_score(sorted_events)

    # Get last event timestamp
    last_ts = _parse_timestamp(
        last_event.get("_extracted_timestamp") or last_event.get("timestamp")
    )

    return RealtimeUserFeatures(
        user_id=user_id,
        session_id=session_id,
        recent_categories_viewed=recent_categories,
        recent_tags_interacted=recent_tags,
        session_category_focus=session_category_focus,
        user_viewed_experiences=viewed_experiences,
        already_shown_session=already_shown,
        session_engagement_score=engagement_score,
        last_event_timestamp=last_ts,
    )

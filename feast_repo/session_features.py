"""
Session Feature View for ToGather ML Platform.

Contains real-time session features computed by Flink from event streams.
These capture active user session behavior for contextual recommendations.

Used by Feast for:
- get_online_features() → Real-time inference (session context)

Note: Session features are stored in Redis by Flink, not in MinIO Parquet.
This FeatureView is for documentation and potential future batch materialization.
"""

from datetime import timedelta

from entities import session
from feast import FeatureView, Field, FileSource
from feast.types import Float64, Int64, String

# Data source - for batch access if needed
# In production, Flink writes directly to Redis for online serving
session_features_source = FileSource(
    name="session_features_source",
    path="s3://feast-offline-store/session_features/",
    timestamp_field="event_timestamp",
    s3_endpoint_override="http://minio.platform.svc.cluster.local:9000",
)

# Session feature view - matches Flink SessionFeatures output
session_features = FeatureView(
    name="session_features",
    entities=[session],
    ttl=timedelta(hours=2),  # Sessions are ephemeral
    schema=[
        # Session identifiers
        Field(name="user_id", dtype=String, description="User who owns the session"),
        # Timing
        Field(
            name="session_start",
            dtype=String,
            description="Session start timestamp (ISO format)",
        ),
        Field(
            name="session_end",
            dtype=String,
            description="Session end timestamp (ISO format)",
        ),
        Field(
            name="session_duration_seconds",
            dtype=Float64,
            description="Session duration in seconds",
        ),
        # Activity metrics
        Field(
            name="activity_count",
            dtype=Int64,
            description="Number of events in session",
        ),
        Field(
            name="unique_event_types",
            dtype=Int64,
            description="Number of unique event types",
        ),
        Field(name="last_event_type", dtype=String, description="Most recent event type"),
        # Location
        Field(
            name="location_changes",
            dtype=Int64,
            description="Number of location changes",
        ),
        Field(name="last_latitude", dtype=Float64, description="Last known latitude"),
        Field(name="last_longitude", dtype=Float64, description="Last known longitude"),
        # Engagement
        Field(
            name="engagement_score",
            dtype=Float64,
            description="Computed engagement score (0-1)",
        ),
    ],
    online=True,  # Served from Redis (written by Flink)
    source=session_features_source,
    tags={
        "team": "ml",
        "pipeline": "flink",
        "entity": "session",
        "feature_type": "realtime",
    },
)

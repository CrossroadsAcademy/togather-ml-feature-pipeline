"""
Feast Feature Definitions for Batch Pipeline.

Defines entities, data sources, and feature views for:
- User features
- Experience features
- Session features

These definitions are used by Feast for:
- get_historical_features() - training data generation
- get_online_features() - inference time feature retrieval
"""

from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float64, Int64, String

# =============================================================================
# Entities
# =============================================================================

user = Entity(
    name="user",
    join_keys=["user_id"],
    description="ToGather user entity",
)

experience = Entity(
    name="experience",
    join_keys=["experience_id"],
    description="ToGather experience/event entity",
)

session = Entity(
    name="session",
    join_keys=["session_id"],
    description="User session entity",
)

# =============================================================================
# Data Sources (MinIO Parquet-backed)
# =============================================================================

# FileSource pointing to Parquet files in MinIO

user_features_source = FileSource(
    name="user_features_source",
    path="s3a://feast-offline-store/user_features/",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
    file_format="parquet",
)

experience_features_source = FileSource(
    name="experience_features_source",
    path="s3a://feast-offline-store/experience_features/",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
    file_format="parquet",
)

session_features_source = FileSource(
    name="session_features_source",
    path="s3a://feast-offline-store/session_features/",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
    file_format="parquet",
)

# =============================================================================
# Feature Views
# =============================================================================

user_features_view = FeatureView(
    name="user_features",
    entities=[user],
    ttl=timedelta(days=7),
    schema=[
        Field(name="user_total_events_1d", dtype=Int64),
        Field(name="user_total_events_7d", dtype=Int64),
        Field(name="user_unique_experiences_7d", dtype=Int64),
        Field(name="user_avg_engagement_score_7d", dtype=Float64),
        Field(name="user_category_preferences", dtype=String),  # Comma-separated
        Field(name="user_location_count_7d", dtype=Int64),
    ],
    online=True,
    source=user_features_source,
    tags={"team": "ml", "pipeline": "batch"},
)

experience_features_view = FeatureView(
    name="experience_features",
    entities=[experience],
    ttl=timedelta(days=7),
    schema=[
        Field(name="exp_total_views_1d", dtype=Int64),
        Field(name="exp_total_views_7d", dtype=Int64),
        Field(name="exp_unique_users_7d", dtype=Int64),
        Field(name="exp_avg_engagement_7d", dtype=Float64),
        Field(name="exp_bookmark_rate_7d", dtype=Float64),
        Field(name="exp_share_rate_7d", dtype=Float64),
    ],
    online=True,
    source=experience_features_source,
    tags={"team": "ml", "pipeline": "batch"},
)

session_features_view = FeatureView(
    name="session_features",
    entities=[session],
    ttl=timedelta(days=1),  # Sessions are more ephemeral
    schema=[
        Field(name="user_id", dtype=String),
        Field(name="session_duration_seconds", dtype=Int64),
        Field(name="session_event_count", dtype=Int64),
        Field(name="session_location_changes", dtype=Int64),
        Field(name="session_unique_experiences", dtype=Int64),
    ],
    online=True,
    source=session_features_source,
    tags={"team": "ml", "pipeline": "batch"},
)

# =============================================================================
# Feature Services (for grouping features at inference time)
# =============================================================================

# Feature service for recommendation ranking
RECOMMENDATION_FEATURES = [
    "user_features:user_total_events_7d",
    "user_features:user_avg_engagement_score_7d",
    "user_features:user_category_preferences",
    "experience_features:exp_total_views_7d",
    "experience_features:exp_avg_engagement_7d",
    "experience_features:exp_bookmark_rate_7d",
]

# Feature service for user profiling
USER_PROFILE_FEATURES = [
    "user_features:user_total_events_1d",
    "user_features:user_total_events_7d",
    "user_features:user_unique_experiences_7d",
    "user_features:user_avg_engagement_score_7d",
    "user_features:user_category_preferences",
    "user_features:user_location_count_7d",
]


def get_feature_store_config() -> dict:
    """
    Get Feast feature store configuration.

    Returns config dict for feature_store.yaml
    """
    return {
        "project": "togather_ml",
        "registry": "s3a://feast-offline-store/registry.pb",
        "provider": "local",
        "online_store": {
            "type": "redis",
            "connection_string": "redis.platform.svc.cluster.local:6379",
        },
        "offline_store": {
            "type": "file",
        },
        "entity_key_serialization_version": 2,
    }

"""
User Profile Feature View for ToGather ML Platform.

Contains user profile features extracted by Spark batch job from UserProfileCreated events.
Used by Feast for:
- get_historical_features() → Training dataset generation
- get_online_features() → Real-time inference
"""

from datetime import timedelta

from entities import user
from feast import FeatureView, Field, FileSource
from feast.types import Array, Bool, Float64, Int64, String

# Data source pointing to MinIO Parquet files written by Spark batch job
user_profile_features_source = FileSource(
    name="user_profile_features_source",
    path="s3://feast-offline-store/user_profile_features/",
    timestamp_field="event_timestamp",
    s3_endpoint_override="http://minio.platform.svc.cluster.local:9000",
)

# User profile feature view - matches Spark batch output schema
user_profile_features = FeatureView(
    name="user_profile_features",
    entities=[user],
    ttl=timedelta(days=30),  # Profile data is relatively stable
    schema=[
        # Location features
        Field(
            name="user_location_lat",
            dtype=Float64,
            description="User's current latitude",
        ),
        Field(
            name="user_location_lng",
            dtype=Float64,
            description="User's current longitude",
        ),
        Field(name="user_city", dtype=String, description="User's current city"),
        Field(name="user_country", dtype=String, description="User's current country"),
        Field(
            name="user_has_location",
            dtype=Bool,
            description="Whether user has location set",
        ),
        # Profile features
        Field(
            name="user_gender",
            dtype=Int64,
            description="Gender enum (0=unspecified, 1=male, 2=female, etc.)",
        ),
        Field(
            name="user_social_score",
            dtype=Int64,
            description="Platform engagement score (0-100)",
        ),
        Field(
            name="user_onboarding_status",
            dtype=Int64,
            description="Onboarding status enum",
        ),
        Field(
            name="user_onboarding_completed",
            dtype=Bool,
            description="Whether onboarding is complete",
        ),
        # Interest features
        Field(
            name="user_interest_ids",
            dtype=Array(String),
            description="List of interest IDs",
        ),
        Field(
            name="user_interest_count",
            dtype=Int64,
            description="Number of interests selected",
        ),
        Field(
            name="user_interest_names",
            dtype=Array(String),
            description="List of interest names",
        ),
        # Account features
        Field(
            name="user_account_age_days",
            dtype=Int64,
            description="Days since account creation",
        ),
        Field(name="user_is_new", dtype=Bool, description="Whether user is new (<7 days)"),
        # Quality flag
        Field(name="is_valid", dtype=Bool, description="Whether all validations passed"),
    ],
    online=True,
    source=user_profile_features_source,
    tags={
        "team": "ml",
        "pipeline": "batch",
        "entity": "user",
        "feature_type": "profile",
    },
)

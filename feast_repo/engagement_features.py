"""
Engagement Feature Views for ToGather ML Platform.

Contains engagement aggregation features from RecommendationFeedback events.
These capture user behavior patterns and experience popularity.

Used by Feast for:
- get_historical_features() → Training dataset generation (labels + features)
- get_online_features() → Real-time inference (user engagement context)
"""

from datetime import timedelta

from entities import experience, user
from feast import FeatureView, Field, FileSource
from feast.types import Array, Bool, Float64, Int64, String

# User Engagement Features
user_engagement_features_source = FileSource(
    name="user_engagement_features_source",
    path="s3://feast-offline-store/user_features/",  # Matches batch job output
    timestamp_field="event_timestamp",
    s3_endpoint_override="http://minio.platform.svc.cluster.local:9000",
)

user_engagement_features = FeatureView(
    name="user_engagement_features",
    entities=[user],
    ttl=timedelta(days=1),  # Engagement features are recomputed daily
    schema=[
        # 7-day engagement counts
        Field(
            name="user_impressions_7d",
            dtype=Int64,
            description="Total impressions in 7 days",
        ),
        Field(name="user_clicks_7d", dtype=Int64, description="Total clicks in 7 days"),
        Field(
            name="user_views_7d",
            dtype=Int64,
            description="Total views (with dwell) in 7 days",
        ),
        Field(
            name="user_actions_7d",
            dtype=Int64,
            description="Total actions (bookings) in 7 days",
        ),
        Field(
            name="user_negatives_7d",
            dtype=Int64,
            description="Total negative signals in 7 days",
        ),
        Field(
            name="user_unique_clicks_7d",
            dtype=Int64,
            description="Unique experiences clicked",
        ),
        # 30-day engagement counts
        Field(
            name="user_impressions_30d",
            dtype=Int64,
            description="Total impressions in 30 days",
        ),
        Field(name="user_clicks_30d", dtype=Int64, description="Total clicks in 30 days"),
        Field(name="user_views_30d", dtype=Int64, description="Total views in 30 days"),
        Field(name="user_actions_30d", dtype=Int64, description="Total actions in 30 days"),
        # Engagement rates (with Laplace smoothing)
        Field(name="user_ctr_7d", dtype=Float64, description="Click-through rate (7d)"),
        Field(name="user_ctr_30d", dtype=Float64, description="Click-through rate (30d)"),
        Field(
            name="user_action_rate_7d",
            dtype=Float64,
            description="Action rate per click (7d)",
        ),
        Field(
            name="user_view_rate_7d",
            dtype=Float64,
            description="View rate per click (7d)",
        ),
        # Dwell time
        Field(
            name="user_avg_dwell_time_ms_7d",
            dtype=Float64,
            description="Avg view duration (ms)",
        ),
        # Profile attributes (from UserProfileCreated)
        Field(
            name="user_interests",
            dtype=Array(String),
            description="User's selected interest names",
        ),
        Field(name="user_latitude", dtype=Float64, description="User's home latitude"),
        Field(name="user_longitude", dtype=Float64, description="User's home longitude"),
        # Cold start indicator
        Field(
            name="user_is_cold_start",
            dtype=Bool,
            description="User has <10 impressions",
        ),
        # Quality flag
        Field(
            name="is_valid",
            dtype=Bool,
            description="Aggregation completed successfully",
        ),
    ],
    online=True,
    source=user_engagement_features_source,
    tags={
        "team": "ml",
        "pipeline": "batch",
        "entity": "user",
        "feature_type": "engagement",
    },
)


# Experience Engagement Features (Popularity)

experience_engagement_features_source = FileSource(
    name="experience_engagement_features_source",
    path="s3://feast-offline-store/experience_features/",  # Matches batch job output
    timestamp_field="event_timestamp",
    s3_endpoint_override="http://minio.platform.svc.cluster.local:9000",
)

experience_engagement_features = FeatureView(
    name="experience_engagement_features",
    entities=[experience],
    ttl=timedelta(days=1),  # Engagement features are recomputed daily
    schema=[
        # 7-day engagement counts
        Field(
            name="exp_impressions_7d",
            dtype=Int64,
            description="Total impressions in 7 days",
        ),
        Field(name="exp_clicks_7d", dtype=Int64, description="Total clicks in 7 days"),
        Field(name="exp_views_7d", dtype=Int64, description="Total views in 7 days"),
        Field(
            name="exp_total_views_1d",
            dtype=Int64,
            description="Total views in last 1 day",
        ),
        Field(
            name="exp_total_views_7d",
            dtype=Int64,
            description="Total views in 7 days (alias)",
        ),
        Field(
            name="exp_actions_7d",
            dtype=Int64,
            description="Total actions (bookings) in 7 days",
        ),
        # Unique viewers
        Field(
            name="exp_unique_viewers_7d",
            dtype=Int64,
            description="Unique users who viewed",
        ),
        Field(
            name="exp_unique_clickers_7d",
            dtype=Int64,
            description="Unique users who clicked",
        ),
        # Position analysis
        Field(
            name="exp_avg_position_7d",
            dtype=Float64,
            description="Avg position when shown",
        ),
        Field(
            name="exp_best_position_7d",
            dtype=Float64,
            description="Best position achieved",
        ),
        # Engagement rates
        Field(name="exp_ctr_7d", dtype=Float64, description="Click-through rate"),
        Field(
            name="exp_booking_rate_7d",
            dtype=Float64,
            description="Booking rate per click",
        ),
        Field(name="exp_view_rate_7d", dtype=Float64, description="View rate per click"),
        Field(name="exp_bookmark_rate_7d", dtype=Float64, description="Bookmark rate"),
        Field(name="exp_share_rate_7d", dtype=Float64, description="Share rate"),
        Field(
            name="exp_avg_engagement_7d",
            dtype=Float64,
            description="Average engagement score",
        ),
        # Content attributes (from ExperienceCreated)
        Field(name="exp_tags", dtype=Array(String), description="Experience tag names"),
        Field(name="exp_category", dtype=String, description="Experience category name"),
        Field(name="exp_latitude", dtype=Float64, description="Experience venue latitude"),
        Field(
            name="exp_longitude",
            dtype=Float64,
            description="Experience venue longitude",
        ),
        Field(name="exp_name", dtype=String, description="Experience name"),
        # Popularity tier
        Field(
            name="exp_popularity_tier",
            dtype=String,
            description="Tier: hot/popular/normal/cold",
        ),
        # Quality flag
        Field(
            name="is_valid",
            dtype=Bool,
            description="Aggregation completed successfully",
        ),
    ],
    online=True,
    source=experience_engagement_features_source,
    tags={
        "team": "ml",
        "pipeline": "batch",
        "entity": "experience",
        "feature_type": "engagement",
    },
)

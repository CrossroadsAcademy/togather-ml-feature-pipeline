"""
ToGather ML Feature Store - Feast Definitions.

This package contains all Feast feature view definitions for the ToGather recommendation system.

Feature Views:
- user_profile_features: Static user profile data (interests, location, account info)
- experience_features: Static experience catalog data (category, pricing, location)
- user_engagement_features: User behavioral aggregations from feedback events
- experience_engagement_features: Experience popularity metrics
- session_features: Real-time session context (from Flink)

Usage:
    # Offline training data generation
    from feast import FeatureStore

    store = FeatureStore(repo_path="feast_repo")
    training_df = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "user_profile_features:user_interest_ids",
            "user_engagement_features:user_ctr_7d",
            "experience_features:exp_category_id",
            "experience_engagement_features:exp_popularity_tier",
        ],
    ).to_df()

    # Online inference
    features = store.get_online_features(
        entity_rows=[{"user_id": "user_123", "experience_id": "exp_456"}],
        features=[...],
    ).to_dict()
"""

# Entities
# Feature Views - Engagement Aggregations
from engagement_features import (
    experience_engagement_features,
    experience_engagement_features_source,
    user_engagement_features,
    user_engagement_features_source,
)
from entities import experience, session, user
from experience_features import experience_features, experience_features_source

# Feature Views - Real-time Session
from session_features import session_features, session_features_source

# Feature Views - Profile/Catalog
from user_features import user_profile_features, user_profile_features_source

__all__ = [
    # Entities
    "user",
    "experience",
    "session",
    # User features
    "user_profile_features",
    "user_profile_features_source",
    "user_engagement_features",
    "user_engagement_features_source",
    # Experience features
    "experience_features",
    "experience_features_source",
    "experience_engagement_features",
    "experience_engagement_features_source",
    # Session features
    "session_features",
    "session_features_source",
]

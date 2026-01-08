"""
Feature Aggregations for Batch Pipeline.

Computes user, experience, and session features from raw events
with proper event_timestamp for Feast point-in-time joins.
"""

from datetime import date
from typing import Any

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from src.batch.observability import JobStageContext, record_metric
from src.utils.logger import get_logger

logger = get_logger(__name__)


# UDFs for Array Extraction


@F.udf(returnType=ArrayType(StringType()))
def _extract_interest_names(interests_array):
    """
    Extract interest names from interests array.

    Handles both Spark Row objects and Python dicts.
    Input format: [{'id': '...', 'name': 'History'}, ...] or [Row(id='...', name='History'), ...]
    Output: ['History', 'Sports', ...]
    """
    if not interests_array:
        return []
    try:
        result = []
        for item in interests_array:
            if item is None:
                continue
            # Try attribute access first (Spark Row)
            if hasattr(item, "name") and item.name:
                result.append(str(item.name))
            # Fall back to dict-style access
            elif isinstance(item, dict) and item.get("name"):
                result.append(str(item.get("name")))
        return result
    except (TypeError, AttributeError, Exception):
        return []


@F.udf(returnType=ArrayType(StringType()))
def _extract_tag_names(tags_array):
    """
    Extract tag names from tags array.

    Handles both Spark Row objects and Python dicts.
    Input format: [{'id': '...', 'name': 'Music', 'icon': '🎵'}, ...] or [Row(...), ...]
    Output: ['Music', ...]
    """
    if not tags_array:
        return []
    try:
        result = []
        for item in tags_array:
            if item is None:
                continue
            # Try attribute access first (Spark Row)
            if hasattr(item, "name") and item.name:
                result.append(str(item.name))
            # Fall back to dict-style access
            elif isinstance(item, dict) and item.get("name"):
                result.append(str(item.get("name")))
        return result
    except (TypeError, AttributeError, Exception):
        return []


# User Feature Aggregations


def aggregate_user_features(
    events_df: DataFrame,
    target_date: date,
) -> DataFrame:
    """
    Aggregate user-level features from raw events.

    Features computed:
        - user_total_events_1d: Event count in last 1 day
        - user_total_events_7d: Event count in last 7 days
        - user_unique_experiences_7d: Unique experiences in 7 days
        - user_avg_engagement_score_7d: Average engagement score
        - user_category_preferences: Top 3 categories (array)
        - user_location_count_7d: Unique locations visited

    Args:
        events_df: Raw events DataFrame (must have event_timestamp, user_id)
        target_date: Date for which to compute features

    Returns:
        DataFrame with user features + event_timestamp
    """
    with JobStageContext("aggregate_user_features") as ctx:
        # Define date boundaries
        target_ts = F.to_timestamp(F.lit(target_date.isoformat()))
        target_date_end = F.date_add(F.to_date(target_ts), 1)  # Next day midnight
        one_day_ago = F.date_sub(target_ts, 1)
        seven_days_ago = F.date_sub(target_ts, 7)
        thirty_days_ago = F.date_sub(target_ts, 30)

        # Filter to 30-day window (includes entire target day)
        df = events_df.filter(
            (F.col("event_timestamp") >= thirty_days_ago)
            & (F.col("event_timestamp") < target_date_end)
        )

        # Normalize column names first
        df = _normalize_entity_columns(df)

        # Add time window flags
        df = df.withColumn(
            "is_last_1d",
            F.when(F.col("event_timestamp") >= one_day_ago, 1).otherwise(0),
        )
        df = df.withColumn(
            "is_last_7d",
            F.when(F.col("event_timestamp") >= seven_days_ago, 1).otherwise(0),
        )

        # Add interaction type flags (robust parsing)
        df = df.withColumn(
            "is_view",
            F.when(F.upper(F.col("event_type")).isin(["VIEW", "EVENT_TYPE_VIEW"]), 1).otherwise(0),
        )
        df = df.withColumn(
            "is_click",
            F.when(F.upper(F.col("event_type")).isin(["CLICK", "EVENT_TYPE_CLICK"]), 1).otherwise(
                0
            ),
        )
        df = df.withColumn(
            "is_impression",
            F.when(
                F.upper(F.col("event_type")).isin(["IMPRESSION", "EVENT_TYPE_IMPRESSION"]),
                1,
            ).otherwise(0),
        )
        df = df.withColumn(
            "is_action",
            F.when(
                F.upper(F.col("event_type")).isin(["RSVP", "BOOK", "ACTION", "EVENT_TYPE_ACTION"]),
                1,
            ).otherwise(0),
        )
        df = df.withColumn(
            "is_negative",
            F.when(
                F.upper(F.col("event_type")).isin(["NEGATIVE", "EVENT_TYPE_NEGATIVE"]),
                1,
            ).otherwise(0),
        )

        # Aggregate by user
        user_features = df.groupBy("user_id").agg(
            # 7-Day Metrics
            F.sum(F.col("is_impression") * F.col("is_last_7d")).alias("user_impressions_7d"),
            F.sum(F.col("is_click") * F.col("is_last_7d")).alias("user_clicks_7d"),
            F.sum(F.col("is_view") * F.col("is_last_7d")).alias("user_views_7d"),
            F.sum(F.col("is_action") * F.col("is_last_7d")).alias("user_actions_7d"),
            F.sum(F.col("is_negative") * F.col("is_last_7d")).alias("user_negatives_7d"),
            F.countDistinct(
                F.when(
                    (F.col("is_click") == 1) & (F.col("is_last_7d") == 1),
                    F.col("_experience_id"),
                )
            ).alias("user_unique_clicks_7d"),
            # 30-Day Metrics (Whole window)
            F.sum("is_impression").alias("user_impressions_30d"),
            F.sum("is_click").alias("user_clicks_30d"),
            F.sum("is_view").alias("user_views_30d"),
            F.sum("is_action").alias("user_actions_30d"),
            # Dwell Time (Average for 7d views)
            # Use flatten column 'view_data_dwell_time_ms' if exists, else 0
            F.avg(
                F.when(
                    (F.col("is_view") == 1) & (F.col("is_last_7d") == 1),
                    F.coalesce(F.col("view_data_dwell_time_ms"), F.lit(0)),
                )
            ).alias("user_avg_dwell_time_ms_7d"),
        )

        # Fill nulls for counts
        fill_cols = [
            "user_impressions_7d",
            "user_clicks_7d",
            "user_views_7d",
            "user_actions_7d",
            "user_negatives_7d",
            "user_unique_clicks_7d",
            "user_impressions_30d",
            "user_clicks_30d",
            "user_views_30d",
            "user_actions_30d",
        ]
        user_features = user_features.na.fill(0, subset=fill_cols)

        # Compute Rates (avoid dbz)
        # 7d Rates
        user_features = (
            user_features.withColumn(
                "user_ctr_7d",
                F.when(
                    F.col("user_impressions_7d") > 0,
                    F.col("user_clicks_7d") / F.col("user_impressions_7d"),
                ).otherwise(0.0),
            )
            .withColumn(
                "user_action_rate_7d",
                F.when(
                    F.col("user_clicks_7d") > 0,
                    F.col("user_actions_7d") / F.col("user_clicks_7d"),
                ).otherwise(0.0),
            )
            .withColumn(
                "user_view_rate_7d",
                F.when(
                    F.col("user_clicks_7d") > 0,
                    F.col("user_views_7d") / F.col("user_clicks_7d"),
                ).otherwise(0.0),
            )
        )

        # 30d Rates
        user_features = user_features.withColumn(
            "user_ctr_30d",
            F.when(
                F.col("user_impressions_30d") > 0,
                F.col("user_clicks_30d") / F.col("user_impressions_30d"),
            ).otherwise(0.0),
        )

        # Cold Start Indicator (< 10 impressions total)
        user_features = user_features.withColumn(
            "user_is_cold_start",
            F.when(
                (F.col("user_impressions_30d") + F.col("user_impressions_7d")) < 10,
                F.lit(True),
            ).otherwise(F.lit(False)),
        )

        # Extract User Profile Attributes (interests, location)
        # From UserProfileCreated events (partition_event_type = user_profile_events)

        # Get user profile events (most recent per user)
        profile_cols_available = [  # noqa: F841
            c
            for c in events_df.columns
            if c
            in [
                "id",
                "interests",
                "current_address_coordinate",
                "_event_type",
                "partition_event_type",
                "event_timestamp",
            ]
        ]

        if "interests" in events_df.columns:
            # Filter to user profile events
            profile_filter = (
                (
                    F.col("_event_type").contains("UserProfile")
                    | (F.col("partition_event_type") == "user_profile_events")
                )
                if "partition_event_type" in events_df.columns
                else F.col("_event_type").contains("UserProfile")
            )

            user_profiles = events_df.filter(profile_filter).select(
                F.col("id").alias("profile_user_id"),
                F.col("interests"),
                F.col("event_timestamp").alias("profile_ts"),
                # Extract location from current_address_coordinate if available
                (
                    F.col("current_address_coordinate").alias("user_location_coord")
                    if "current_address_coordinate" in events_df.columns
                    else F.lit(None).alias("user_location_coord")
                ),
            )

            # Get most recent profile per user (in case of multiple updates)
            profile_window = Window.partitionBy("profile_user_id").orderBy(
                F.col("profile_ts").desc()
            )
            user_profiles = user_profiles.withColumn("_rn", F.row_number().over(profile_window))
            user_profiles = user_profiles.filter(F.col("_rn") == 1).drop("_rn", "profile_ts")

            # Extract interest names as array
            user_profiles = user_profiles.withColumn(
                "user_interests", _extract_interest_names(F.col("interests"))
            )

            # Extract location if available
            if "user_location_coord" in user_profiles.columns:
                user_profiles = user_profiles.withColumn(
                    "user_latitude",
                    F.when(
                        F.col("user_location_coord").isNotNull(),
                        F.col("user_location_coord").getItem("latitude"),
                    ),
                ).withColumn(
                    "user_longitude",
                    F.when(
                        F.col("user_location_coord").isNotNull(),
                        F.col("user_location_coord").getItem("longitude"),
                    ),
                )
            else:
                user_profiles = user_profiles.withColumn(
                    "user_latitude", F.lit(None).cast("double")
                )
                user_profiles = user_profiles.withColumn(
                    "user_longitude", F.lit(None).cast("double")
                )

            # Select final profile columns
            user_profiles = user_profiles.select(
                "profile_user_id",
                "user_interests",
                "user_latitude",
                "user_longitude",
            )

            # Left join with behavioral features
            user_features = user_features.join(
                user_profiles,
                user_features["user_id"] == user_profiles["profile_user_id"],
                "left",
            ).drop("profile_user_id")

            logger.info("Joined user profile attributes (interests, location)")
        else:
            # No interests column available, add empty defaults
            user_features = user_features.withColumn(
                "user_interests", F.array().cast(ArrayType(StringType()))
            )
            user_features = user_features.withColumn("user_latitude", F.lit(None).cast("double"))
            user_features = user_features.withColumn("user_longitude", F.lit(None).cast("double"))
            logger.info("No interests column in events, using empty defaults")

        # Valid Flag
        user_features = user_features.withColumn("is_valid", F.lit(True))

        # Add event_timestamp (target date) for Feast
        user_features = user_features.withColumn(
            "event_timestamp",
            F.to_timestamp(F.lit(target_date.isoformat())),
        )

        # Record metrics
        user_count = user_features.count()
        ctx.set_attribute("user_count", user_count)
        record_metric(
            "batch_records_processed_total",
            user_count,
            {"stage": "aggregate_user", "entity_type": "user"},
        )

        logger.info("User features aggregated", user_count=user_count)

        return user_features


# Experience Feature Aggregations


def aggregate_experience_features(
    events_df: DataFrame,
    target_date: date,
) -> DataFrame:
    """
    Aggregate experience-level features from raw events.

    Features computed:
        - exp_total_views_1d: Views in last 1 day
        - exp_total_views_7d: Views in last 7 days
        - exp_unique_users_7d: Unique users engaged
        - exp_avg_engagement_7d: Average engagement score
        - exp_bookmark_rate_7d: Bookmark/view ratio
        - exp_share_rate_7d: Share/view ratio

    Args:
        events_df: Raw events DataFrame
        target_date: Date for which to compute features

    Returns:
        DataFrame with experience features + event_timestamp
    """
    with JobStageContext("aggregate_experience_features") as ctx:
        target_ts = F.to_timestamp(F.lit(target_date.isoformat()))
        target_date_end = F.date_add(F.to_date(target_ts), 1)  # Next day midnight
        one_day_ago = F.date_sub(target_ts, 1)
        seven_days_ago = F.date_sub(target_ts, 7)

        # Normalize column names first
        events_df = _normalize_entity_columns(events_df)

        # Filter to 7-day window and only experience-related events
        df = events_df.filter(
            (F.col("event_timestamp") >= seven_days_ago)
            & (F.col("event_timestamp") < target_date_end)
            & F.col("_experience_id").isNotNull()
        )

        # Add flags for interaction types (handle both simplified and enum strings)
        # checks both "view" and "EVENT_TYPE_VIEW" to be safe
        df = df.withColumn(
            "is_view",
            F.when(F.upper(F.col("event_type")).isin(["VIEW", "EVENT_TYPE_VIEW"]), 1).otherwise(0),
        )
        df = df.withColumn(
            "is_click",
            F.when(F.upper(F.col("event_type")).isin(["CLICK", "EVENT_TYPE_CLICK"]), 1).otherwise(
                0
            ),
        )
        df = df.withColumn(
            "is_impression",
            F.when(
                F.upper(F.col("event_type")).isin(["IMPRESSION", "EVENT_TYPE_IMPRESSION"]),
                1,
            ).otherwise(0),
        )
        # Actions: Bookings (rsvp), shares, etc.
        df = df.withColumn(
            "is_action",
            F.when(
                F.upper(F.col("event_type")).isin(
                    ["RSVP", "BOOK", "ACTION", "EVENT_TYPE_ACTION", "SHARE"]
                ),
                1,
            ).otherwise(0),
        )

        df = df.withColumn(
            "engagement_score",
            _compute_engagement_score(F.col("event_type")),
        )

        # Add time window flags
        df = df.withColumn(
            "is_last_1d",
            F.when(F.col("event_timestamp") >= one_day_ago, 1).otherwise(0),
        )

        # Aggregate by experience (use normalized column)
        exp_features = df.groupBy("_experience_id").agg(
            # Impressions
            F.sum("is_impression").alias("exp_impressions_7d"),
            # Clicks
            F.sum("is_click").alias("exp_clicks_7d"),
            # Views
            F.sum(F.col("is_view") * F.col("is_last_1d")).alias("exp_total_views_1d"),
            F.sum("is_view").alias("exp_total_views_7d"),
            F.sum("is_view").alias("exp_views_7d"),  # Alias for Feast match
            # Actions
            F.sum("is_action").alias("exp_actions_7d"),
            # User diversity
            F.countDistinct("user_id").alias("exp_unique_viewers_7d"),  # renamed from unique_users
            F.countDistinct(F.when(F.col("is_click") == 1, F.col("user_id"))).alias(
                "exp_unique_clickers_7d"
            ),
            # Engagement Score
            F.avg("engagement_score").alias("exp_avg_engagement_7d"),
            # Position stats (if available, otherwise null/0)
            F.avg("position").alias("exp_avg_position_7d"),
            F.min("position").alias("exp_best_position_7d"),
        )

        # Fill nulls for counts
        exp_features = exp_features.na.fill(
            0,
            subset=[
                "exp_impressions_7d",
                "exp_clicks_7d",
                "exp_actions_7d",
                "exp_unique_viewers_7d",
                "exp_unique_clickers_7d",
            ],
        )

        # Compute rates (avoid division by zero)
        exp_features = (
            exp_features.withColumn(
                "exp_ctr_7d",
                F.when(
                    F.col("exp_impressions_7d") > 0,
                    F.col("exp_clicks_7d") / F.col("exp_impressions_7d"),
                ).otherwise(0.0),
            )
            .withColumn(
                "exp_booking_rate_7d",
                F.when(
                    F.col("exp_clicks_7d") > 0,
                    F.col("exp_actions_7d") / F.col("exp_clicks_7d"),
                ).otherwise(0.0),
            )
            .withColumn(
                "exp_view_rate_7d",
                F.when(
                    F.col("exp_clicks_7d") > 0,
                    F.col("exp_views_7d") / F.col("exp_clicks_7d"),
                ).otherwise(0.0),
            )
        )

        # Legacy rate columns (keep for safety if needed)
        exp_features = exp_features.withColumn("exp_bookmark_rate_7d", F.lit(0.0))
        exp_features = exp_features.withColumn("exp_share_rate_7d", F.lit(0.0))

        # Popularity Tier
        exp_features = exp_features.withColumn(
            "exp_popularity_tier",
            _classify_popularity_tier(F.col("exp_views_7d"), F.col("exp_clicks_7d")),
        )

        # Valid flag
        exp_features = exp_features.withColumn("is_valid", F.lit(True))

        # Rename _experience_id back to experience_id for Feast entity
        exp_features = exp_features.withColumnRenamed("_experience_id", "experience_id")

        # Extract Experience Content Attributes (tags, category, location, name)
        # From ExperienceCreated events (partition_event_type = experience_events)

        if "tags" in events_df.columns:
            # Filter to experience created events
            exp_filter = (
                (
                    F.col("_event_type").contains("Experience")
                    | (F.col("partition_event_type") == "experience_events")
                )
                if "partition_event_type" in events_df.columns
                else F.col("_event_type").contains("Experience")
            )

            # Select content columns from experience events
            exp_content_cols = [
                "id",
                "tags",
                "category_name",
                "event_location_coordinate",
                "name",
                "event_timestamp",
            ]
            available_cols = [c for c in exp_content_cols if c in events_df.columns]

            exp_content = events_df.filter(exp_filter).select(*[F.col(c) for c in available_cols])

            # Rename for join
            exp_content = exp_content.withColumnRenamed("id", "content_exp_id")
            exp_content = exp_content.withColumnRenamed("event_timestamp", "content_ts")

            # Get most recent content per experience (in case of updates)
            content_window = Window.partitionBy("content_exp_id").orderBy(
                F.col("content_ts").desc()
            )
            exp_content = exp_content.withColumn("_rn", F.row_number().over(content_window))
            exp_content = exp_content.filter(F.col("_rn") == 1).drop("_rn", "content_ts")

            # Extract tag names as array
            exp_content = exp_content.withColumn("exp_tags", _extract_tag_names(F.col("tags")))

            # Extract category
            if "category_name" in exp_content.columns:
                exp_content = exp_content.withColumnRenamed("category_name", "exp_category")
            else:
                exp_content = exp_content.withColumn("exp_category", F.lit(None).cast("string"))

            # Extract location
            if "event_location_coordinate" in exp_content.columns:
                exp_content = exp_content.withColumn(
                    "exp_latitude",
                    F.when(
                        F.col("event_location_coordinate").isNotNull(),
                        F.col("event_location_coordinate").getItem("latitude"),
                    ),
                ).withColumn(
                    "exp_longitude",
                    F.when(
                        F.col("event_location_coordinate").isNotNull(),
                        F.col("event_location_coordinate").getItem("longitude"),
                    ),
                )
            else:
                exp_content = exp_content.withColumn("exp_latitude", F.lit(None).cast("double"))
                exp_content = exp_content.withColumn("exp_longitude", F.lit(None).cast("double"))

            # Extract name for display/debugging
            if "name" in exp_content.columns:
                exp_content = exp_content.withColumnRenamed("name", "exp_name")
            else:
                exp_content = exp_content.withColumn("exp_name", F.lit(None).cast("string"))

            # Select final content columns
            exp_content = exp_content.select(
                "content_exp_id",
                "exp_tags",
                "exp_category",
                "exp_latitude",
                "exp_longitude",
                "exp_name",
            )

            # Left join with behavioral features
            exp_features = exp_features.join(
                exp_content,
                exp_features["experience_id"] == exp_content["content_exp_id"],
                "left",
            ).drop("content_exp_id", "tags", "event_location_coordinate")

            logger.info("Joined experience content attributes (tags, category, location)")
        else:
            # No tags column available, add empty defaults
            exp_features = exp_features.withColumn(
                "exp_tags", F.array().cast(ArrayType(StringType()))
            )
            exp_features = exp_features.withColumn("exp_category", F.lit(None).cast("string"))
            exp_features = exp_features.withColumn("exp_latitude", F.lit(None).cast("double"))
            exp_features = exp_features.withColumn("exp_longitude", F.lit(None).cast("double"))
            exp_features = exp_features.withColumn("exp_name", F.lit(None).cast("string"))
            logger.info("No tags column in events, using empty defaults for experience content")

        # Add event_timestamp for Feast
        exp_features = exp_features.withColumn(
            "event_timestamp",
            F.to_timestamp(F.lit(target_date.isoformat())),
        )

        # Record metrics
        exp_count = exp_features.count()
        ctx.set_attribute("experience_count", exp_count)
        record_metric(
            "batch_records_processed_total",
            exp_count,
            {"stage": "aggregate_experience", "entity_type": "experience"},
        )

        logger.info("Experience features aggregated", experience_count=exp_count)

        return exp_features


# Session Feature Aggregations


def aggregate_session_features(
    events_df: DataFrame,
    target_date: date,
) -> DataFrame:
    """
    Aggregate session-level features from raw events.

    Features computed:
        - session_duration_seconds: Session length
        - session_event_count: Events in session
        - session_location_changes: Location changes
        - session_unique_experiences: Unique experiences viewed

    Args:
        events_df: Raw events DataFrame
        target_date: Date for which to compute features

    Returns:
        DataFrame with session features + event_timestamp
    """
    with JobStageContext("aggregate_session_features") as ctx:
        target_ts = F.to_timestamp(F.lit(target_date.isoformat()))
        target_date_start = F.date_trunc("day", target_ts)
        target_date_end = F.date_add(target_date_start, 1)

        # Filter to target date
        df = events_df.filter(
            (F.col("event_timestamp") >= target_date_start)
            & (F.col("event_timestamp") < target_date_end)
        )

        # Generate session_id if not present (30-min gap = new session)
        window = Window.partitionBy("user_id").orderBy("event_timestamp")

        df = df.withColumn("prev_timestamp", F.lag("event_timestamp").over(window))
        df = df.withColumn(
            "time_gap_seconds",
            F.when(
                F.col("prev_timestamp").isNotNull(),
                F.unix_timestamp("event_timestamp") - F.unix_timestamp("prev_timestamp"),
            ).otherwise(0),
        )
        df = df.withColumn(
            "new_session",
            F.when(F.col("time_gap_seconds") > 1800, 1).otherwise(0),  # 30 min gap
        )
        df = df.withColumn(
            "session_num",
            F.sum("new_session").over(window),
        )
        df = df.withColumn(
            "session_id",
            F.concat(F.col("user_id"), F.lit("_"), F.col("session_num")),
        )

        # Normalize columns
        df = _normalize_entity_columns(df)

        # Track location changes (use normalized _city)
        df = df.withColumn("prev_city", F.lag("_city").over(window))
        df = df.withColumn(
            "location_changed",
            F.when(
                (F.col("prev_city").isNotNull()) & (F.col("_city") != F.col("prev_city")),
                1,
            ).otherwise(0),
        )

        # Aggregate by session
        session_features = df.groupBy("session_id", "user_id").agg(
            F.min("event_timestamp").alias("session_start"),
            F.max("event_timestamp").alias("session_end"),
            F.count("*").alias("session_event_count"),
            F.sum("location_changed").alias("session_location_changes"),
            F.countDistinct("_experience_id").alias("session_unique_experiences"),
        )

        # Compute duration
        session_features = session_features.withColumn(
            "session_duration_seconds",
            F.unix_timestamp("session_end") - F.unix_timestamp("session_start"),
        )

        # Add event_timestamp for Feast
        session_features = session_features.withColumn(
            "event_timestamp",
            F.to_timestamp(F.lit(target_date.isoformat())),
        )

        # Select final columns
        session_features = session_features.select(
            "session_id",
            "user_id",
            "session_duration_seconds",
            "session_event_count",
            "session_location_changes",
            "session_unique_experiences",
            "event_timestamp",
        )

        session_count = session_features.count()
        ctx.set_attribute("session_count", session_count)
        record_metric(
            "batch_records_processed_total",
            session_count,
            {"stage": "aggregate_session", "entity_type": "session"},
        )

        logger.info("Session features aggregated", session_count=session_count)

        return session_features


# Helper Functions


def _compute_engagement_score(event_type_col: Any) -> Any:
    """
    Compute engagement score based on event type.

    Matches the scoring logic in Flink feature_extractors.py.
    """
    return (
        F.when(event_type_col == "view", 0.2)
        .when(event_type_col == "click", 0.4)
        .when(event_type_col == "bookmark", 0.6)
        .when(event_type_col == "share", 0.8)
        .when(event_type_col == "rsvp", 1.0)
        .when(event_type_col == "like", 0.4)
        .when(event_type_col == "comment", 0.6)
        .when(event_type_col == "follow", 0.4)
        .otherwise(0.1)
    )


def _normalize_entity_columns(df: DataFrame) -> DataFrame:
    """
    Normalize column names for schema compatibility across event types.

    Different event types have different column names (after protobuf flattening):
    - experience_events: id, category_id, category_name, event_location_city
    - recommendation_feedback_v1: event_id, user_id (no category/city)
    - user_profile_events: id, current_address_city (no category)
    - user_account_events: id (no category/city)

    This function creates normalized columns with fallbacks.
    """
    columns = df.columns

    # Normalize experience_id
    # Proto fields: ExperienceCreated.id, RecommendationFeedback.event_id
    if "experience_id" in columns:
        df = df.withColumn("_experience_id", F.col("experience_id"))
    elif "event_id" in columns:
        # RecommendationFeedback.event_id IS the experience_id (ID of the experience being recommended)
        df = df.withColumn("_experience_id", F.col("event_id"))
    elif "id" in columns:
        # ExperienceCreated uses 'id' as the experience identifier
        df = df.withColumn("_experience_id", F.col("id"))
    else:
        df = df.withColumn("_experience_id", F.lit(None).cast("string"))

    # Normalize category
    # Proto field: ExperienceCreated.category.name -> category_name (after flattening)
    if "category" in columns:
        df = df.withColumn("_category", F.col("category"))
    elif "category_name" in columns:
        df = df.withColumn("_category", F.col("category_name"))
    elif "category_id" in columns:
        df = df.withColumn("_category", F.col("category_id"))
    else:
        df = df.withColumn("_category", F.lit(None).cast("string"))

    # Normalize city
    # Proto fields:
    # - ExperienceCreated.event_location.city -> event_location_city
    # - UserProfileCreated.current_address.city -> current_address_city
    if "city" in columns:
        df = df.withColumn("_city", F.col("city"))
    elif "event_location_city" in columns:
        df = df.withColumn("_city", F.col("event_location_city"))
    elif "current_address_city" in columns:
        df = df.withColumn("_city", F.col("current_address_city"))
    else:
        df = df.withColumn("_city", F.lit(None).cast("string"))

    return df


@F.udf(returnType=ArrayType(StringType()))
def _get_top_k_elements(arr: list[str], k: int = 3) -> list[str]:
    """Get top K most frequent elements from array."""
    if not arr:
        return []

    from collections import Counter

    counts = Counter(arr)
    return [item for item, _ in counts.most_common(k)]


@F.udf(returnType=StringType())
def _classify_popularity_tier(views: int, clicks: int) -> str:
    """Classify experience into popularity tiers."""
    # Simple rule-based logic (can be replaced by quantiles later)
    score = (views * 1) + (clicks * 5)

    if score > 1000:
        return "hot"
    elif score > 100:
        return "popular"
    elif score > 10:
        return "normal"
    else:
        return "cold"


def compute_all_features(
    events_df: DataFrame,
    target_date: date,
) -> dict[str, DataFrame]:
    """
    Compute all feature aggregations for a target date.

    Args:
        events_df: Raw events DataFrame
        target_date: Date for which to compute features

    Returns:
        Dict with keys: user_features, experience_features, session_features
    """
    logger.info("Computing all features", target_date=target_date.isoformat())

    return {
        "user_features": aggregate_user_features(events_df, target_date),
        "experience_features": aggregate_experience_features(events_df, target_date),
        "session_features": aggregate_session_features(events_df, target_date),
    }

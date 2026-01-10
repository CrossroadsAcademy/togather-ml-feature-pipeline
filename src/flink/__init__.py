"""
Flink Real-Time Feature Pipeline Package.

This package contains PyFlink jobs for real-time feature aggregation:
- Session windowing and feature extraction
- Redis sink for feature serving
- Data quality validation
"""

from src.flink.event_validator import EventValidator
from src.flink.feature_extractors import (
    compute_engagement_score,
    extract_session_features,
)
from src.flink.flink_session_job import FlinkSessionJob
from src.flink.redis_sink import RedisSink

__all__ = [
    "FlinkSessionJob",
    "extract_session_features",
    "compute_engagement_score",
    "RedisSink",
    "EventValidator",
]

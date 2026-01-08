"""Streaming module exports."""

from src.streaming.app_events_processor import AppEventsProcessor
from src.streaming.kafka_consumer import (
    EventProcessor,
    KafkaConsumer,
    KafkaConsumerConfig,
    NonRetryableException,
    RetryableException,
)
from src.streaming.storage_sink import (
    DataArchiver,
    # Backward compatibility aliases
    ParquetSink,
    ParquetSinkConfig,
    StorageSink,
    StorageSinkConfig,
)

# Note: Schema Registry removed - using two-layer protobuf (EventEnvelope) with togather-event-sdk

__all__ = [
    # Kafka Consumer
    "KafkaConsumer",
    "KafkaConsumerConfig",
    "EventProcessor",
    "AppEventsProcessor",
    "RetryableException",
    "NonRetryableException",
    # Storage Sink (unified)
    "StorageSink",
    "StorageSinkConfig",
    "DataArchiver",
    # Backward compatibility
    "ParquetSink",
    "ParquetSinkConfig",
]

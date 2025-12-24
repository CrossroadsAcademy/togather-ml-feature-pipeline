"""Streaming module exports."""

from src.streaming.kafka_consumer import (
    AppEventsProcessor,
    EventProcessor,
    KafkaConsumer,
    KafkaConsumerConfig,
    NonRetryableException,
    RetryableException,
)
from src.streaming.schema_registry import (
    ProtobufDeserializer,
    ProtobufSerializer,
    SchemaMetadata,
    SchemaRegistryClient,
    get_key_subject,
    get_value_subject,
)
from src.streaming.storage_sink import (
    DataArchiver,
    # Backward compatibility aliases
    ParquetSink,
    ParquetSinkConfig,
    StorageSink,
    StorageSinkConfig,
)

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
    # Schema Registry / Protobuf
    "SchemaRegistryClient",
    "SchemaMetadata",
    "ProtobufSerializer",
    "ProtobufDeserializer",
    "get_value_subject",
    "get_key_subject",
]

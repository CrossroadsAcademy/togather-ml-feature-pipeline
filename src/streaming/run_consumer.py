#!/usr/bin/env python3
"""
Run the AppEventsProcessor Kafka consumer.

This module provides the entrypoint for the consumer that:
1. Consumes events from Kafka topics
2. Parses EventEnvelope protobuf messages using togather-event-sdk
3. Archives raw events to MinIO as Hive-partitioned Parquet

Usage:
    python -m src.streaming.run_consumer

Environment Variables:
    KAFKA_BOOTSTRAP_SERVERS: Kafka bootstrap servers
    KAFKA_TOPICS: Comma-separated list of topics
    MINIO_ENDPOINT_URL: MinIO endpoint
    MINIO_ACCESS_KEY: MinIO access key
    MINIO_SECRET_KEY: MinIO secret key
    MINIO_BUCKET_NAME: Bucket for raw events (default: raw-events)
"""

import asyncio
import os
import signal
import sys
from pathlib import Path

from src.streaming.app_events_processor import AppEventsProcessor
from src.streaming.kafka_consumer import KafkaConsumer, KafkaConsumerConfig
from src.utils.config import settings
from src.utils.logger import get_logger, setup_logging

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


logger = get_logger(__name__)


async def run_consumer():
    """Run the Kafka consumer with AppEventsProcessor."""
    setup_logging()

    logger.info("Starting AppEventsProcessor consumer...")

    # Load configuration from environment
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", settings.kafka.bootstrap_servers)
    topics_str = os.getenv("KAFKA_TOPICS", settings.kafka.topics)
    topics = [t.strip() for t in topics_str.split(",") if t.strip()]
    consumer_group = os.getenv("KAFKA_CONSUMER_GROUP", "app-events-processor")

    logger.info(f"Bootstrap servers: {bootstrap_servers}")
    logger.info(f"Topics: {topics}")
    logger.info(f"Consumer group: {consumer_group}")

    # Build Kafka config
    kafka_config = KafkaConsumerConfig(
        bootstrap_servers=bootstrap_servers,
        consumer_group=consumer_group,
        topics=topics,
        dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", "app-events-dlq"),
        auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest"),
        enable_auto_commit=False,  # Manual commit for exactly-once
    )

    # Add SASL config if provided
    sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM")
    if sasl_mechanism:
        kafka_config.security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        kafka_config.sasl_mechanism = sasl_mechanism
        kafka_config.sasl_username = os.getenv("KAFKA_SASL_USERNAME")
        kafka_config.sasl_password = os.getenv("KAFKA_SASL_PASSWORD")
        kafka_config.ssl_ca_location = os.getenv("KAFKA_SSL_CA_LOCATION")

    # Create processor and consumer
    processor = AppEventsProcessor()
    consumer = KafkaConsumer(config=kafka_config, processor=processor)

    # Handle shutdown signals
    shutdown_event = asyncio.Event()

    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        shutdown_event.set()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # Start consumer in background
        consumer_task = asyncio.create_task(consumer.start())

        # Wait for shutdown signal
        await shutdown_event.wait()

        # Graceful shutdown
        logger.info("Shutting down consumer...")
        await consumer.stop()
        processor.shutdown()

        # Cancel consumer task
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

        logger.info("Consumer shutdown complete")

    except Exception as e:
        logger.error(f"Consumer error: {e}")
        raise


def main():
    """Main entrypoint."""
    try:
        asyncio.run(run_consumer())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

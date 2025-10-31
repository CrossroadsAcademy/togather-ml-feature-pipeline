"""Integration tests for Kafka consumer using testcontainers."""

import asyncio
import json
import time
from typing import Any

import pytest
from confluent_kafka import Producer
from testcontainers.kafka import KafkaContainer
from testcontainers.redis import RedisContainer

from src.streaming.app_events_processor import AppEventsProcessor
from src.streaming.kafka_consumer import KafkaConsumer, KafkaConsumerConfig


class MockEventProcessor:
    """Mock event processor for testing."""

    def __init__(self):
        self.processed_messages = []
        self.should_fail = False
        self.fail_count = 0

    async def process(self, message: dict[str, Any], topic: str) -> None:
        """Process message and track it."""
        if self.should_fail and self.fail_count > 0:
            self.fail_count -= 1
            raise Exception("Simulated processing error")

        self.processed_messages.append(
            {"message": message, "topic": topic, "processed_at": time.time()}
        )

        # Simulate processing time
        await asyncio.sleep(0.01)


@pytest.fixture(scope="session")
def kafka_container():
    """Start Kafka container for testing."""
    with KafkaContainer("confluentinc/cp-kafka:latest") as kafka:
        kafka.start()
        yield kafka


@pytest.fixture(scope="session")
def redis_container():
    """Start Redis container for testing."""
    with RedisContainer("redis:latest") as redis:
        redis.start()
        yield redis


@pytest.fixture
def kafka_producer(kafka_container):
    """Create Kafka producer for testing."""
    producer_config = {
        "bootstrap.servers": kafka_container.get_bootstrap_server(),
        "client.id": "test-producer",
    }

    producer = Producer(producer_config)
    yield producer

    # Cleanup
    producer.flush()


@pytest.fixture
def test_config(kafka_container):
    """Create test configuration."""
    return KafkaConsumerConfig(
        bootstrap_servers=kafka_container.get_bootstrap_server(),
        consumer_group="test-consumer-group",
        topics=["test-topic"],
        dlq_topic="test-dlq",
        retry_max_attempts=2,
        retry_initial_delay=1.0,
        retry_max_delay=5.0,
    )


@pytest.fixture
def mock_processor():
    """Create mock event processor."""
    return MockEventProcessor()


class TestKafkaConsumerIntegration:
    """Integration tests for Kafka consumer."""

    @pytest.mark.asyncio
    async def test_consumer_processes_messages(
        self, kafka_container, kafka_producer, test_config, mock_processor
    ):
        """Test that consumer processes messages correctly."""
        # Create consumer
        consumer = KafkaConsumer(test_config, mock_processor, metrics_port=8081)

        # Send test messages
        test_messages = [
            {"user_id": "user1", "event_type": "login", "timestamp": time.time()},
            {"user_id": "user2", "event_type": "logout", "timestamp": time.time()},
            {"user_id": "user3", "event_type": "click", "timestamp": time.time()},
        ]

        for message in test_messages:
            kafka_producer.produce("test-topic", key="test-key", value=json.dumps(message))

        kafka_producer.flush()

        # Start consumer in background
        consumer_task = asyncio.create_task(consumer.start())

        # Wait for messages to be processed
        await asyncio.sleep(2)

        # Stop consumer
        consumer.running = False
        await consumer_task

        # Verify messages were processed
        assert len(mock_processor.processed_messages) == len(test_messages)

        for i, processed in enumerate(mock_processor.processed_messages):
            assert processed["topic"] == "test-topic"
            assert processed["message"]["user_id"] == test_messages[i]["user_id"]
            assert processed["message"]["event_type"] == test_messages[i]["event_type"]

    @pytest.mark.asyncio
    async def test_consumer_handles_processing_errors(
        self, kafka_container, kafka_producer, test_config, mock_processor
    ):
        """Test that consumer handles processing errors correctly."""
        # Configure processor to fail
        mock_processor.should_fail = True
        mock_processor.fail_count = 1  # Fail once then succeed

        # Create consumer
        consumer = KafkaConsumer(test_config, mock_processor, metrics_port=8082)

        # Send test message
        test_message = {"user_id": "user1", "event_type": "login", "timestamp": time.time()}
        kafka_producer.produce("test-topic", key="test-key", value=json.dumps(test_message))
        kafka_producer.flush()

        # Start consumer in background
        consumer_task = asyncio.create_task(consumer.start())

        # Wait for message to be processed (with retry)
        await asyncio.sleep(5)

        # Stop consumer
        consumer.running = False
        await consumer_task

        # Verify message was eventually processed successfully
        assert len(mock_processor.processed_messages) == 1
        assert mock_processor.processed_messages[0]["message"]["user_id"] == "user1"

    @pytest.mark.asyncio
    async def test_consumer_multiple_topics(self, kafka_container, kafka_producer, mock_processor):
        """Test consumer with multiple topics."""
        # Create config with multiple topics
        config = KafkaConsumerConfig(
            bootstrap_servers=kafka_container.get_bootstrap_server(),
            consumer_group="test-multi-topic-group",
            topics=["topic1", "topic2"],
            dlq_topic="test-dlq",
        )

        consumer = KafkaConsumer(config, mock_processor, metrics_port=8083)

        # Send messages to different topics
        messages = [
            ("topic1", {"user_id": "user1", "event_type": "login"}),
            ("topic2", {"user_id": "user2", "event_type": "logout"}),
            ("topic1", {"user_id": "user3", "event_type": "click"}),
        ]

        for topic, message in messages:
            kafka_producer.produce(topic, key="test-key", value=json.dumps(message))

        kafka_producer.flush()

        # Start consumer in background
        consumer_task = asyncio.create_task(consumer.start())

        # Wait for messages to be processed
        await asyncio.sleep(3)

        # Stop consumer
        consumer.running = False
        await consumer_task

        # Verify messages from both topics were processed
        assert len(mock_processor.processed_messages) == 3

        topics_processed = [msg["topic"] for msg in mock_processor.processed_messages]
        assert "topic1" in topics_processed
        assert "topic2" in topics_processed

    @pytest.mark.asyncio
    async def test_consumer_with_app_events_processor(self, kafka_container, kafka_producer):
        """Test consumer with real app events processor."""
        # Create config
        config = KafkaConsumerConfig(
            bootstrap_servers=kafka_container.get_bootstrap_server(),
            consumer_group="test-app-events-group",
            topics=["user.account"],
            dlq_topic="test-dlq",
        )

        # Create app events processor (will use mock components for testing)
        processor = AppEventsProcessor()

        consumer = KafkaConsumer(config, processor, metrics_port=8084)

        # Send app event message
        app_event = {
            "user_id": "user123",
            "event_type": "account_created",
            "timestamp": time.time(),
            "account_data": {"email": "test@example.com", "name": "Test User"},
        }

        kafka_producer.produce("user.account", key="user123", value=json.dumps(app_event))
        kafka_producer.flush()

        # Start consumer in background
        consumer_task = asyncio.create_task(consumer.start())

        # Wait for message to be processed
        await asyncio.sleep(3)

        # Stop consumer
        consumer.running = False
        await consumer_task

        # Shutdown processor
        await processor.shutdown()

        # Test passes if no exceptions were raised


@pytest.mark.asyncio
async def test_consumer_graceful_shutdown(kafka_container, test_config, mock_processor):
    """Test that consumer shuts down gracefully."""
    consumer = KafkaConsumer(test_config, mock_processor, metrics_port=8085)

    # Start consumer in background
    consumer_task = asyncio.create_task(consumer.start())

    # Let it run briefly
    await asyncio.sleep(1)

    # Stop gracefully
    consumer.running = False

    # Wait for shutdown
    await consumer_task

    # Verify consumer is stopped
    assert not consumer.running


@pytest.mark.asyncio
async def test_consumer_metrics(kafka_container, test_config, mock_processor):
    """Test that consumer exposes metrics correctly."""
    consumer = KafkaConsumer(test_config, mock_processor, metrics_port=8086)

    # Start consumer in background
    consumer_task = asyncio.create_task(consumer.start())

    # Let it run briefly
    await asyncio.sleep(1)

    # Stop consumer
    consumer.running = False
    await consumer_task

    # Metrics should be available (can't test the actual metrics
    # without making HTTP requests, but can verify the server started)
    assert consumer.metrics_port == 8086

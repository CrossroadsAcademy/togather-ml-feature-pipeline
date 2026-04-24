"""Unit tests for Kafka consumer components."""

from unittest.mock import AsyncMock, Mock, patch

import pytest
from confluent_kafka import Message

from src.streaming.kafka_consumer import (
    DLQMessage,
    KafkaConsumer,
    KafkaConsumerConfig,
    NonRetryableException,
    RetryableException,
)
from src.streaming.schema_registry import (
    MessageValidator,
    ProtobufDeserializer,
    SchemaRegistryClient,
)
from src.utils.secrets import InfisicalClient, SecretsManager


class TestKafkaConsumerConfig:
    """Test Kafka consumer configuration."""

    def test_config_creation(self):
        """Test config creation with defaults."""
        config = KafkaConsumerConfig(
            bootstrap_servers="localhost:9092",
            consumer_group="test-group",
            topics=["test-topic"],
            dlq_topic="test-dlq",
        )

        assert config.bootstrap_servers == "localhost:9092"
        assert config.consumer_group == "test-group"
        assert config.topics == ["test-topic"]
        assert config.dlq_topic == "test-dlq"
        assert config.auto_offset_reset == "latest"
        assert config.enable_auto_commit is True
        assert config.session_timeout_ms == 30000
        assert config.max_poll_records == 500
        assert config.retry_max_attempts == 3

    def test_config_with_custom_values(self):
        """Test config creation with custom values."""
        config = KafkaConsumerConfig(
            bootstrap_servers="kafka:9092",
            consumer_group="custom-group",
            topics=["topic1", "topic2"],
            dlq_topic="custom-dlq",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            session_timeout_ms=60000,
            max_poll_records=1000,
            retry_max_attempts=5,
        )

        assert config.bootstrap_servers == "kafka:9092"
        assert config.consumer_group == "custom-group"
        assert config.topics == ["topic1", "topic2"]
        assert config.dlq_topic == "custom-dlq"
        assert config.auto_offset_reset == "earliest"
        assert config.enable_auto_commit is False
        assert config.session_timeout_ms == 60000
        assert config.max_poll_records == 1000
        assert config.retry_max_attempts == 5


class TestDLQMessage:
    """Test DLQ message structure."""

    def test_dlq_message_creation(self):
        """Test DLQ message creation."""
        dlq_msg = DLQMessage(
            original_topic="test-topic",
            original_partition=0,
            original_offset=123,
            original_key="test-key",
            original_value='{"test": "data"}',
            error_reason="Test error",
        )

        assert dlq_msg.original_topic == "test-topic"
        assert dlq_msg.original_partition == 0
        assert dlq_msg.original_offset == 123
        assert dlq_msg.original_key == "test-key"
        assert dlq_msg.original_value == '{"test": "data"}'
        assert dlq_msg.error_reason == "Test error"
        assert dlq_msg.retry_count == 0
        assert dlq_msg.error_timestamp is not None

    def test_dlq_message_json_serialization(self):
        """Test DLQ message JSON serialization."""
        dlq_msg = DLQMessage(
            original_topic="test-topic",
            original_partition=0,
            original_offset=123,
            original_key="test-key",
            original_value='{"test": "data"}',
            error_reason="Test error",
        )

        # Should be able to serialize to JSON
        json_data = dlq_msg.model_dump_json()
        assert "test-topic" in json_data
        assert "Test error" in json_data


class TestKafkaConsumer:
    """Test Kafka consumer functionality."""

    @pytest.fixture
    def mock_processor(self):
        """Create mock event processor."""
        processor = AsyncMock()
        processor.process = AsyncMock()
        return processor

    @pytest.fixture
    def test_config(self):
        """Create test configuration."""
        return KafkaConsumerConfig(
            bootstrap_servers="localhost:9092",
            consumer_group="test-group",
            topics=["test-topic"],
            dlq_topic="test-dlq",
        )

    @pytest.fixture
    def consumer(self, test_config, mock_processor):
        """Create Kafka consumer instance."""
        with patch("src.streaming.kafka_consumer.Consumer"):
            with patch("src.streaming.kafka_consumer.start_http_server"):
                return KafkaConsumer(test_config, mock_processor, metrics_port=8080)

    def test_consumer_initialization(self, consumer, test_config):
        """Test consumer initialization."""
        assert consumer.config == test_config
        assert consumer.running is False
        assert consumer.metrics_port == 8080

    @patch("src.streaming.kafka_consumer.Consumer")
    def test_create_consumer(self, mock_consumer_class, test_config, mock_processor):
        """Test consumer creation."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer

        with patch("src.streaming.kafka_consumer.start_http_server"):
            consumer = KafkaConsumer(test_config, mock_processor, metrics_port=8080)
            consumer._create_consumer()

        # Verify consumer was created with correct config
        mock_consumer_class.assert_called_once()
        call_args = mock_consumer_class.call_args[1]

        assert call_args["bootstrap.servers"] == "localhost:9092"
        assert call_args["group.id"] == "test-group"
        assert call_args["auto.offset.reset"] == "latest"
        assert call_args["enable.auto.commit"] is False
        assert call_args["session.timeout.ms"] == 30000
        assert "max.poll.records" not in call_args

        # Verify subscription
        mock_consumer.subscribe.assert_called_once_with(["test-topic"])

    def test_parse_message_json(self, consumer):
        """Test parsing JSON message."""
        # Create mock message
        mock_message = Mock(spec=Message)
        mock_message.value.return_value = b'{"user_id": "123", "event_type": "login"}'

        result = consumer._parse_message(mock_message)

        assert result == {"user_id": "123", "event_type": "login"}

    def test_parse_message_binary(self, consumer):
        """Test parsing binary message."""
        # Create mock message with binary data
        mock_message = Mock(spec=Message)
        mock_message.value.return_value = b"\x00\x00\x00\x01binary_data"

        result = consumer._parse_message(mock_message)

        assert "raw_value" in result
        assert result["raw_value"] == b"\x00\x00\x00\x01binary_data"

    def test_parse_message_empty(self, consumer):
        """Test parsing empty message."""
        # Create mock message with no value
        mock_message = Mock(spec=Message)
        mock_message.value.return_value = None

        result = consumer._parse_message(mock_message)

        assert result == {}

    @pytest.mark.asyncio
    async def test_process_message_success(self, consumer, mock_processor):
        """Test successful message processing."""
        # Create mock message
        mock_message = Mock(spec=Message)
        mock_message.value.return_value = b'{"user_id": "123", "event_type": "login"}'
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 123
        mock_message.key.return_value = None

        # Mock processor to succeed
        mock_processor.process = AsyncMock()

        await consumer._handle_message(mock_message)

        # Verify processor was called
        mock_processor.process.assert_called_once()
        call_args = mock_processor.process.call_args[0]
        assert call_args[0] == {"user_id": "123", "event_type": "login"}
        assert call_args[1] == "test-topic"

    @pytest.mark.asyncio
    async def test_process_message_retryable_error(self, consumer, mock_processor):
        """Test message processing with retryable error."""
        # Create mock message
        mock_message = Mock(spec=Message)
        mock_message.value.return_value = b'{"user_id": "123", "event_type": "login"}'
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 123
        mock_message.key.return_value = None

        # Mock processor to raise retryable exception
        mock_processor.process = AsyncMock(side_effect=RetryableException("Test retryable error"))

        with patch.object(consumer, "_send_to_dlq", new_callable=AsyncMock) as mock_dlq:
            await consumer._handle_message(mock_message)

        # Verify DLQ was called after retries
        mock_dlq.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_message_non_retryable_error(self, consumer, mock_processor):
        """Test message processing with non-retryable error."""
        # Create mock message
        mock_message = Mock(spec=Message)
        mock_message.value.return_value = b'{"user_id": "123", "event_type": "login"}'
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 123
        mock_message.key.return_value = None

        # Mock processor to raise non-retryable exception
        mock_processor.process = AsyncMock(
            side_effect=NonRetryableException("Test non-retryable error")
        )

        with patch.object(consumer, "_send_to_dlq", new_callable=AsyncMock) as mock_dlq:
            await consumer._handle_message(mock_message)

        # Verify DLQ was called immediately
        mock_dlq.assert_called_once()


class TestSchemaRegistryClient:
    """Test Schema Registry client."""

    @pytest.fixture
    def mock_requests(self):
        """Mock requests module."""
        with patch("src.streaming.schema_registry.requests") as mock:
            yield mock

    def test_client_initialization(self, mock_requests):
        """Test client initialization."""
        client = SchemaRegistryClient("http://localhost:8081")
        assert client.base_url == "http://localhost:8081"

    def test_client_with_auth(self, mock_requests):
        """Test client initialization with authentication."""
        client = SchemaRegistryClient("http://localhost:8081", auth=("user", "pass"))
        assert client.session.auth == ("user", "pass")

    def test_get_schema_by_id_success(self, mock_requests):
        """Test successful schema retrieval by ID."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": 1,
            "version": 1,
            "schemaType": "PROTOBUF",
            "subject": "test-subject",
            "schema": "message Test { required string field = 1; }",
        }
        mock_requests.Session.return_value.get.return_value = mock_response

        client = SchemaRegistryClient("http://localhost:8081")
        schema = client.get_schema_by_id(1)

        assert schema is not None
        assert schema.schema_id == 1
        assert schema.version == 1
        assert schema.schema_type == "PROTOBUF"
        assert schema.subject == "test-subject"

    def test_get_schema_by_id_not_found(self, mock_requests):
        """Test schema retrieval when schema not found."""
        # Mock 404 response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_requests.Session.return_value.get.return_value = mock_response

        client = SchemaRegistryClient("http://localhost:8081")
        schema = client.get_schema_by_id(999)

        assert schema is None


class TestProtobufDeserializer:
    """Test Protobuf deserializer."""

    @pytest.fixture
    def mock_schema_client(self):
        """Create mock schema registry client."""
        client = Mock(spec=SchemaRegistryClient)
        client.get_schema_by_id = Mock()
        return client

    @pytest.fixture
    def deserializer(self, mock_schema_client):
        """Create protobuf deserializer."""
        return ProtobufDeserializer(mock_schema_client)

    def test_extract_schema_id_success(self, deserializer):
        """Test successful schema ID extraction."""
        # Confluent format: magic byte (0) + 4 bytes schema ID (1)
        message_bytes = b"\x00\x00\x00\x00\x01binary_data"

        schema_id = deserializer.extract_schema_id(message_bytes)

        assert schema_id == 1

    def test_extract_schema_id_invalid_format(self, deserializer):
        """Test schema ID extraction with invalid format."""
        # Invalid magic byte
        message_bytes = b"\x01\x00\x00\x00\x01binary_data"

        schema_id = deserializer.extract_schema_id(message_bytes)

        assert schema_id is None

    def test_extract_schema_id_too_short(self, deserializer):
        """Test schema ID extraction with message too short."""
        message_bytes = b"\x00\x00\x00"

        schema_id = deserializer.extract_schema_id(message_bytes)

        assert schema_id is None

    def test_deserialize_message_success(self, deserializer, mock_schema_client):
        """Test successful message deserialization."""
        # Mock schema
        from src.streaming.schema_registry import SchemaMetadata

        mock_schema = SchemaMetadata(
            schema_id=1,
            version=1,
            schema_type="PROTOBUF",
            subject="test-subject",
            schema_content="message Test { required string field = 1; }",
        )
        mock_schema_client.get_schema_by_id.return_value = mock_schema

        # Message with schema ID 1
        message_bytes = b"\x00\x00\x00\x00\x01binary_data"

        result, error = deserializer.deserialize_message(message_bytes, "test-topic")

        assert error is None
        assert result is not None
        assert result["schema_id"] == 1
        assert result["schema_subject"] == "test-subject"
        assert result["topic"] == "test-topic"


class TestMessageValidator:
    """Test message validator."""

    @pytest.fixture
    def mock_schema_client(self):
        """Create mock schema registry client."""
        client = Mock(spec=SchemaRegistryClient)
        client.get_latest_schema = Mock()
        return client

    @pytest.fixture
    def validator(self, mock_schema_client):
        """Create message validator."""
        return MessageValidator(mock_schema_client)

    def test_validate_message_success(self, validator, mock_schema_client):
        """Test successful message validation."""
        # Mock schema
        from src.streaming.schema_registry import SchemaMetadata

        mock_schema = SchemaMetadata(
            schema_id=1,
            version=1,
            schema_type="PROTOBUF",
            subject="user.account",
            schema_content="message UserAccount { required string user_id = 1; }",
        )
        mock_schema_client.get_latest_schema.return_value = mock_schema

        # Valid message
        message_data = {"user_id": "123", "event_type": "account_created", "timestamp": 1234567890}

        is_valid, error = validator.validate_message(message_data, "user.account")

        assert is_valid is True
        assert error is None

    def test_validate_message_missing_field(self, validator, mock_schema_client):
        """Test message validation with missing required field."""
        # Mock schema
        from src.streaming.schema_registry import SchemaMetadata

        mock_schema = SchemaMetadata(
            schema_id=1,
            version=1,
            schema_type="PROTOBUF",
            subject="user.account",
            schema_content="message UserAccount { required string user_id = 1; }",
        )
        mock_schema_client.get_latest_schema.return_value = mock_schema

        # Invalid message (missing user_id)
        message_data = {"event_type": "account_created", "timestamp": 1234567890}

        is_valid, error = validator.validate_message(message_data, "user.account")

        assert is_valid is False
        assert "Missing required field: user_id" in error


class TestSecretsManager:
    """Test secrets manager."""

    @pytest.fixture
    def mock_infisical_client(self):
        """Create mock Infisical client."""
        client = Mock(spec=InfisicalClient)
        client.get_secret = Mock()
        return client

    @pytest.fixture
    def secrets_manager(self, mock_infisical_client):
        """Create secrets manager."""
        return SecretsManager(mock_infisical_client, enable_infisical=True)

    def test_get_secret_from_environment(self, secrets_manager):
        """Test getting secret from environment variable."""
        import os

        # Set environment variable
        os.environ["TEST_SECRET"] = "env_value"

        try:
            secret = secrets_manager.get_secret("TEST_SECRET")
            assert secret == "env_value"
        finally:
            # Clean up
            del os.environ["TEST_SECRET"]

    def test_get_secret_from_infisical(self, secrets_manager, mock_infisical_client):
        """Test getting secret from Infisical."""
        # Mock Infisical response
        mock_infisical_client.get_secret.return_value = "infisical_value"

        secret = secrets_manager.get_secret("TEST_SECRET")

        assert secret == "infisical_value"
        mock_infisical_client.get_secret.assert_called_once_with("TEST_SECRET", "/")

    def test_get_secret_with_default(self, secrets_manager, mock_infisical_client):
        """Test getting secret with default value."""
        # Mock Infisical to return None
        mock_infisical_client.get_secret.return_value = None

        secret = secrets_manager.get_secret("TEST_SECRET", default="default_value")

        assert secret == "default_value"

    def test_get_required_secret_success(self, secrets_manager, mock_infisical_client):
        """Test getting required secret successfully."""
        # Mock Infisical response
        mock_infisical_client.get_secret.return_value = "required_value"

        secret = secrets_manager.get_required_secret("REQUIRED_SECRET")

        assert secret == "required_value"

    def test_get_required_secret_not_found(self, secrets_manager, mock_infisical_client):
        """Test getting required secret when not found."""
        # Mock Infisical to return None
        mock_infisical_client.get_secret.return_value = None

        with pytest.raises(ValueError, match="Required secret not found"):
            secrets_manager.get_required_secret("REQUIRED_SECRET")

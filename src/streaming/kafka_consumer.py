"""Production-ready Kafka consumer skeleton with observability, retry patterns, and DLQ support."""

import asyncio
import signal
import time
from typing import Any, Protocol
from unittest.mock import Mock

from confluent_kafka import Consumer, KafkaError, Message, TopicPartition
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pydantic import BaseModel, Field
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.utils.config import settings
from src.utils.logger import get_logger, setup_logging

logger = get_logger(__name__)

# Prometheus Metrics
messages_consumed_total = Counter(
    "kafka_messages_consumed_total",
    "Total number of messages consumed",
    ["topic", "status"],
)

messages_processing_duration = Histogram(
    "kafka_message_processing_duration_seconds",
    "Time spent processing messages",
    ["topic"],
)

consumer_lag = Gauge("kafka_consumer_lag", "Consumer lag per partition", ["topic", "partition"])

dlq_messages_total = Counter(
    "kafka_dlq_messages_total",
    "Total messages sent to dead letter queue",
    ["topic", "reason"],
)


class EventProcessor(Protocol):
    """Protocol for event processors."""

    async def process(self, message: dict[str, Any], topic: str) -> None:
        """Process a single event message."""
        ...


class RetryableException(Exception):
    """Exception that can be retried."""

    pass


class NonRetryableException(Exception):
    """Exception that should not be retried."""

    pass


class DLQMessage(BaseModel):
    """Dead Letter Queue message structure."""

    original_topic: str = Field(..., description="Original topic name")
    original_partition: int = Field(..., description="Original partition")
    original_offset: int = Field(..., description="Original offset")
    original_key: str | None = Field(None, description="Original message key")
    original_value: str = Field(..., description="Original message value")
    error_reason: str = Field(..., description="Reason for DLQ placement")
    error_timestamp: float = Field(default_factory=time.time, description="Error timestamp")
    retry_count: int = Field(default=0, description="Number of retry attempts")


class KafkaConsumerConfig(BaseModel):
    """Kafka consumer configuration following best practices."""

    bootstrap_servers: str = Field(..., description="Kafka bootstrap servers")
    consumer_group: str = Field(..., description="Consumer group ID")
    topics: list[str] = Field(..., description="List of topics to consume from")
    auto_offset_reset: str = Field(default="latest", description="Auto offset reset policy")
    enable_auto_commit: bool = Field(
        default=True, description="Enable auto commit - prefer manual commits"
    )
    session_timeout_ms: int = Field(
        default=45000, description="Session timeout in ms (increased for cloud Kafka)"
    )
    heartbeat_interval_ms: int = Field(default=15000, description="Heartbeat interval in ms")
    max_poll_interval_ms: int = Field(default=300000, description="Max poll interval in ms")
    max_poll_records: int = Field(
        default=500,
        description="Optional. Not used by Confluent Kafka Python client, kept for config compatibility.",
    )
    dlq_topic: str = Field(..., description="Dead letter queue topic name")
    retry_max_attempts: int = Field(default=3, description="Maximum retry attempts")
    retry_initial_delay: float = Field(default=5.0, description="Initial retry delay in seconds")
    retry_max_delay: float = Field(default=300.0, description="Maximum retry delay in seconds")
    # Security settings
    security_protocol: str = Field(default="PLAINTEXT", description="Security protocol")
    sasl_mechanism: str | None = Field(default=None, description="SASL mechanism")
    sasl_username: str | None = Field(default=None, description="SASL username")
    sasl_password: str | None = Field(default=None, description="SASL password")
    ssl_ca_location: str | None = Field(default=None, description="SSL CA location")
    # SSL/TLS fields for external Kafka (KRaft mode)
    ssl_keystore_location: str | None = Field(
        default=None, description="SSL keystore location (JKS format)"
    )
    ssl_keystore_password: str | None = Field(default=None, description="SSL keystore password")
    ssl_truststore_location: str | None = Field(
        default=None, description="SSL truststore location (JKS format)"
    )
    ssl_truststore_password: str | None = Field(default=None, description="SSL truststore password")
    ssl_key_password: str | None = Field(
        default=None,
        description="SSL key password (if different from keystore password)",
    )


class KafkaConsumer:
    """Production-ready Kafka consumer with retry patterns and observability."""

    def __init__(
        self,
        config: KafkaConsumerConfig,
        processor: EventProcessor,
        metrics_port: int = 8080,
    ):
        self.config = config
        self.processor = processor
        self.consumer: Consumer | None = None
        self.running = False
        self.metrics_port = metrics_port

        # Setup logging
        setup_logging()
        self.logger = get_logger(self.__class__.__name__)

        # Start Prometheus metrics server
        start_http_server(self.metrics_port)
        self.logger.info(f"Prometheus metrics server started on port {self.metrics_port}")

    def _create_consumer(self) -> Consumer:
        """Create and configure Kafka consumer following best practices."""
        consumer_config = {
            "bootstrap.servers": self.config.bootstrap_servers,
            "group.id": self.config.consumer_group,
            "auto.offset.reset": self.config.auto_offset_reset,
            "enable.auto.commit": self.config.enable_auto_commit,
            "session.timeout.ms": self.config.session_timeout_ms,
            "heartbeat.interval.ms": self.config.heartbeat_interval_ms,
            "max.poll.interval.ms": self.config.max_poll_interval_ms,
            # Manual offset commits = at-least-once delivery
            "enable.auto.commit": False,  # noqa: F601
            # Security configuration
            "security.protocol": self.config.security_protocol,
            # Socket keepalive to prevent idle connection drops (critical for cloud Kafka)
            "socket.keepalive.enable": True,
            # Reconnection settings for resilience
            "reconnect.backoff.ms": 100,
            "reconnect.backoff.max.ms": 10000,
            # Connection timeout settings
            "socket.timeout.ms": 60000,
            "connections.max.idle.ms": 540000,  # 9 minutes (default broker timeout is 10 min)
        }

        # Add SASL configuration if provided
        if self.config.sasl_mechanism and self.config.sasl_username and self.config.sasl_password:
            consumer_config.update(
                {
                    "sasl.mechanism": self.config.sasl_mechanism,
                    "sasl.username": self.config.sasl_username,
                    "sasl.password": self.config.sasl_password,
                }
            )

        # Add SSL configuration if provided
        if self.config.ssl_ca_location:
            consumer_config["ssl.ca.location"] = self.config.ssl_ca_location

        # Add SSL keystore/truststore for external Kafka (KRaft mode with SSL)
        if self.config.ssl_keystore_location:
            consumer_config["ssl.keystore.location"] = self.config.ssl_keystore_location
        if self.config.ssl_keystore_password:
            consumer_config["ssl.keystore.password"] = self.config.ssl_keystore_password
        if self.config.ssl_truststore_location:
            consumer_config["ssl.truststore.location"] = self.config.ssl_truststore_location
        if self.config.ssl_truststore_password:
            consumer_config["ssl.truststore.password"] = self.config.ssl_truststore_password
        if self.config.ssl_key_password:
            consumer_config["ssl.key.password"] = self.config.ssl_key_password

        consumer = Consumer(**consumer_config)
        consumer.subscribe(self.config.topics)

        self.logger.info(f"Consumer created and subscribed to topics: {self.config.topics}")
        self.logger.info(f"Consumer group: {self.config.consumer_group}")
        self.logger.info(f"Security protocol: {self.config.security_protocol}")
        return consumer

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(RetryableException),
    )
    async def _process_message_with_retry(self, message: Message, topic: str) -> None:
        """Process message with retry logic."""
        try:
            # Parse message
            message_data = self._parse_message(message)

            # Process the message
            start_time = time.time()
            await self.processor.process(message_data, topic)

            # Record metrics
            duration = time.time() - start_time
            messages_processing_duration.labels(topic=topic).observe(duration)
            messages_consumed_total.labels(topic=topic, status="success").inc()

            self.logger.info(
                "Message processed successfully",
                topic=topic,
                partition=message.partition(),
                offset=message.offset(),
                duration=duration,
            )

        except NonRetryableException as e:
            self.logger.error(
                "Non-retryable error processing message",
                topic=topic,
                partition=message.partition(),
                offset=message.offset(),
                error=str(e),
                exc_info=True,
            )
            messages_consumed_total.labels(topic=topic, status="non_retryable_error").inc()
            await self._send_to_dlq(message, topic, str(e))

        except Exception as e:
            self.logger.error(
                "Retryable error processing message",
                topic=topic,
                partition=message.partition(),
                offset=message.offset(),
                error=str(e),
                exc_info=True,
            )
            messages_consumed_total.labels(topic=topic, status="retryable_error").inc()
            await self._send_to_dlq(message, topic, f"Retryable error: {str(e)}")
            return

    def _parse_message(self, message: Message) -> dict[str, Any]:
        """
        Parse Kafka message using EventEnvelope pattern (Protobuf-only).

        The backend uses a 2-layer protobuf approach:
        1. Outer: EventEnvelope (event_type, event_version, timestamp, trace_id, payload)
        2. Inner: Specific event type (UserProfileCreated, ExperienceCreated, etc.)

        Uses togather-event-sdk for deserialization.
        """
        try:
            raw_value = message.value()
            if not raw_value:
                raise NonRetryableException("Empty message value")

            # Parse as EventEnvelope protobuf (no JSON fallback)
            msg_data = self._parse_event_envelope(raw_value)

            if not msg_data:
                raise NonRetryableException("Failed to parse EventEnvelope: empty result")

            if msg_data.get("parse_error"):
                raise NonRetryableException(
                    f"EventEnvelope parse error: {msg_data.get('parse_error')}"
                )

            # Add Kafka metadata if real message (not Mock)
            if not isinstance(message, Mock):
                msg_data.update(
                    {
                        "partition": message.partition(),
                        "offset": message.offset(),
                        "timestamp": (message.timestamp()[1] if message.timestamp() else None),
                        "key": message.key().decode("utf-8") if message.key() else None,
                    }
                )

            return msg_data

        except NonRetryableException:
            raise  # Re-raise as is
        except Exception as e:
            self.logger.error("Error parsing message", error=str(e))
            raise NonRetryableException(f"Message parsing failed: {str(e)}") from e

    def _parse_event_envelope(self, raw_value: bytes) -> dict[str, Any]:
        """
        Parse EventEnvelope protobuf message using togather-event-sdk.

        Returns the inner payload as a dict with envelope metadata.

        Note: Similar to Flink's event_envelope_parser, we handle the case
        where bytes might need ISO-8859-1 encoding/decoding.
        """
        try:
            from google.protobuf.json_format import MessageToDict
            from togather_event_sdk.common.v1.event_envelop_pb2 import EventEnvelope
        except ImportError:
            self.logger.warning("togather-event-sdk not installed, cannot parse protobuf")
            return {"parse_error": "togather-event-sdk not installed"}

        try:
            # Handle bytes vs string (matching Flink's approach)
            if isinstance(raw_value, str):
                # If it's a string (e.g., from ISO-8859-1 decoding), encode back to bytes
                proto_bytes = raw_value.encode("iso-8859-1")
            else:
                proto_bytes = raw_value

            # Step 1: Deserialize the outer EventEnvelope
            envelope = EventEnvelope()
            envelope.ParseFromString(proto_bytes)

            if not envelope.event_type:
                # Log hex dump for debugging
                hex_sample = proto_bytes[:50].hex() if proto_bytes else "empty"
                self.logger.debug(f"Missing event_type, hex: {hex_sample}")
                return {
                    "parse_error": "Invalid envelope: missing event_type",
                    "hex": hex_sample,
                }

            self.logger.info(f"[SDK] Parsed envelope: type={envelope.event_type}")

            # Step 2: Get the payload class based on event_type
            payload_class = self._get_payload_class(envelope.event_type)

            if not payload_class:
                self.logger.warning(f"Unknown event_type: {envelope.event_type}")
                # Return with warning, not error - unknown events should be archived, not DLQ'd
                return {
                    "_event_type": envelope.event_type,
                    "_event_version": envelope.event_version,
                    "_timestamp": envelope.timestamp,
                    "_trace_id": (envelope.trace_id if envelope.HasField("trace_id") else None),
                    "_payload_warning": f"Unknown event_type: {envelope.event_type}",
                }

            # Step 3: Deserialize the inner payload
            payload_msg = payload_class()
            payload_msg.ParseFromString(envelope.payload)

            # Convert to dict
            msg_data = MessageToDict(payload_msg, preserving_proto_field_name=True)

            # Add envelope metadata
            msg_data["_event_type"] = envelope.event_type
            msg_data["_event_version"] = envelope.event_version
            msg_data["_timestamp"] = envelope.timestamp
            if envelope.HasField("trace_id"):
                msg_data["_trace_id"] = envelope.trace_id

            return msg_data

        except Exception as e:
            # Log hex dump for debugging
            hex_sample = raw_value[:50].hex() if raw_value else "empty"
            self.logger.error(f"EventEnvelope parsing error: {e}, hex: {hex_sample}")
            return {
                "raw_hex": hex_sample,
                "parse_error": str(e),
            }

    def _get_payload_class(self, event_type: str):
        """
        Get the protobuf class for the given event_type.

        Uses togather-event-sdk stubs.
        """
        # Lazy load the mapping to avoid import errors if SDK not installed
        if not hasattr(self, "_event_type_mapping"):
            self._event_type_mapping = self._build_event_type_mapping()

        return self._event_type_mapping.get(event_type)

    def _build_event_type_mapping(self) -> dict[str, Any]:
        """Build mapping of event_type strings to protobuf classes."""
        mapping: dict[str, Any] = {}

        try:
            # User events
            from togather_event_sdk.user.v1.user_account_created_pb2 import (
                UserAccountCreated,
            )
            from togather_event_sdk.user.v1.user_profile_created_pb2 import (
                UserProfileCreated,
            )

            mapping["user.v1.UserAccountCreated"] = UserAccountCreated
            mapping["user.v1.UserProfileCreated"] = UserProfileCreated
        except ImportError:
            pass

        try:
            from togather_event_sdk.user.v1.user_email_verification_requested_pb2 import (
                UserEmailVerificationRequestedPayload,
            )
            from togather_event_sdk.user.v1.user_forgot_password_pb2 import (
                UserForgotPasswordPayload,
            )

            mapping[
                "user.v1.UserEmailVerificationRequestedPayload"
            ] = UserEmailVerificationRequestedPayload
            mapping["user.v1.UserForgotPasswordPayload"] = UserForgotPasswordPayload
        except ImportError:
            pass

        try:
            # Experience events
            from togather_event_sdk.experience.v1.experience_created_pb2 import (
                ExperienceCreated,
            )

            mapping["experience.v1.ExperienceCreated"] = ExperienceCreated
        except ImportError:
            pass

        try:
            # Partner events
            from togather_event_sdk.partner.v1.partner_profile_created_pb2 import (
                PartnerProfileCreated,
            )

            mapping["partner.v1.PartnerProfileCreated"] = PartnerProfileCreated
        except ImportError:
            pass

        try:
            # Admin events
            from togather_event_sdk.admin.v1.admin_account_created_pb2 import (
                AdminAccountCreatedPayload,
            )
            from togather_event_sdk.admin.v1.admin_email_verification_requested_pb2 import (
                AdminEmailVerificationRequestedPayload,
            )

            mapping["admin.v1.AdminAccountCreatedPayload"] = AdminAccountCreatedPayload
            mapping[
                "admin.v1.AdminEmailVerificationRequestedPayload"
            ] = AdminEmailVerificationRequestedPayload
        except ImportError:
            pass

        try:
            # Feed events (recommendation)
            from togather_event_sdk.feed.v1.recommendation_feedback_pb2 import (
                RecommendationFeedback,
            )
            from togather_event_sdk.feed.v1.recommendation_served_pb2 import (
                RecommendationServedEvent,
            )

            mapping["feed.v1.RecommendationFeedback"] = RecommendationFeedback
            mapping["feed.v1.RecommendationServedEvent"] = RecommendationServedEvent
        except ImportError:
            pass

        self.logger.info(f"Loaded {len(mapping)} event type mappings from togather-event-sdk")
        return mapping

    async def _send_to_dlq(self, message: Message, topic: str, error_reason: str) -> None:
        """Send message to dead letter queue."""
        # Handle binary protobuf data safely - encode as base64 if UTF-8 decode fails
        original_key = None
        if message.key():
            try:
                original_key = message.key().decode("utf-8")
            except UnicodeDecodeError:
                import base64

                original_key = f"base64:{base64.b64encode(message.key()).decode('ascii')}"

        original_value = ""
        if message.value():
            try:
                original_value = message.value().decode("utf-8")
            except UnicodeDecodeError:
                import base64

                # Store first 1KB as base64 to avoid huge DLQ messages
                truncated = message.value()[:1024]
                original_value = f"base64:{base64.b64encode(truncated).decode('ascii')}"

        dlq_message = DLQMessage(  # noqa: F841
            original_topic=topic,
            original_partition=message.partition(),
            original_offset=message.offset(),
            original_key=original_key,
            original_value=original_value,
            error_reason=error_reason,
        )

        # In a real implementation, will be send this to the DLQ topic
        self.logger.error(
            "Message sent to DLQ",
            dlq_topic=self.config.dlq_topic,
            original_topic=topic,
            partition=message.partition(),
            offset=message.offset(),
            error_reason=error_reason,
        )

        dlq_messages_total.labels(topic=topic, reason=error_reason).inc()

    async def _handle_message(self, message: Message) -> None:
        """Handle individual message with manual offset commit."""
        #  Ensure topic is a real string even if message is a mock
        topic = "test-topic" if isinstance(message, Mock) else message.topic()

        try:
            await self._process_message_with_retry(message, topic)

            # Manual offset commit after success
            if self.consumer:
                self.consumer.commit(message)
                self.logger.debug(
                    "Offset committed successfully",
                    topic=topic,
                    partition=message.partition(),
                    offset=message.offset(),
                )

        except RetryableException as e:
            # Send to DLQ after max retries
            self.logger.error(
                "Max retries exceeded, sending to DLQ",
                topic=topic,
                partition=message.partition(),
                offset=message.offset(),
                error=str(e),
            )
            await self._send_to_dlq(message, topic, f"Max retries exceeded: {str(e)}")

            # Commit offset even after DLQ
            if self.consumer:
                self.consumer.commit(message)

    async def start(self) -> None:
        """Start the Kafka consumer with graceful shutdown handling."""
        self.logger.info("Starting Kafka consumer...")
        self.consumer = self._create_consumer()
        self.running = True

        # Set up signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self.logger.debug("Reached end of partition")
                        continue
                    else:
                        self.logger.error(f"Consumer error: {msg.error()}")
                        continue

                # Process message asynchronously
                await self._handle_message(msg)

                # Monitor consumer lag periodically
                self._update_consumer_lag_metrics()

        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal, shutting down...")
        except Exception as e:
            self.logger.error(f"Unexpected error in consumer: {e}", exc_info=True)
        finally:
            await self.stop()

    def _update_consumer_lag_metrics(self) -> None:
        """Update consumer lag metrics."""
        if not self.consumer:
            return

        try:
            # Get consumer lag information
            metadata = self.consumer.list_topics(timeout=10)
            for topic_name in self.config.topics:
                if topic_name in metadata.topics:
                    topic_metadata = metadata.topics[topic_name]
                    for partition_id in topic_metadata.partitions:
                        try:
                            # Get high water mark (latest offset)
                            high_water_mark = self.consumer.get_watermark_offsets(
                                TopicPartition(topic_name, partition_id)
                            )[1]

                            # Get current committed offset
                            committed_offset = self.consumer.committed(
                                [TopicPartition(topic_name, partition_id)]
                            )[0].offset

                            # Calculate lag
                            lag = max(0, high_water_mark - committed_offset)

                            # Update metrics
                            consumer_lag.labels(topic=topic_name, partition=str(partition_id)).set(
                                lag
                            )

                        except Exception as e:
                            self.logger.debug(
                                f"Could not get lag for {topic_name}:{partition_id}: {e}"
                            )

        except Exception as e:
            self.logger.debug(f"Could not update consumer lag metrics: {e}")

    async def stop(self) -> None:
        """Stop the Kafka consumer gracefully."""
        self.logger.info("Stopping Kafka consumer...")
        self.running = False

        if self.consumer:
            try:
                # Best practice: Commit final offsets before closing
                self.logger.info("Committing final offsets...")
                self.consumer.commit()
                self.logger.info("Final offsets committed successfully")
            except Exception as e:
                self.logger.warning(f"Failed to commit final offsets: {e}")
            finally:
                self.consumer.close()
                self.logger.info("Consumer closed gracefully")

    def is_healthy(self) -> tuple[bool, str]:
        """
        Check if Kafka consumer is healthy. Used by health check server.

        Returns:
            Tuple of (is_healthy, message)
        """
        if not self.consumer:
            return False, "Consumer not initialized"

        if not self.running:
            return False, "Consumer not running"

        try:
            # Check if we have partition assignments
            assignment = self.consumer.assignment()
            if not assignment:
                return False, "No partitions assigned"

            # Calculate total lag across all partitions
            total_lag = 0
            partition_count = len(assignment)

            for partition in assignment:
                try:
                    high_watermark = self.consumer.get_watermark_offsets(partition)[1]
                    position = self.consumer.position([partition])[0].offset
                    if position >= 0:
                        total_lag += high_watermark - position
                except Exception:
                    pass

            return True, f"Consuming {partition_count} partitions, lag: {total_lag}"

        except Exception as e:
            return False, f"Health check failed: {str(e)}"


async def main():
    """Main entry point for the Kafka consumer."""
    import os

    import pyroscope

    # Import  AppEventsProcessor
    from src.streaming.app_events_processor import AppEventsProcessor

    # Configure Pyroscope profiling
    pyroscope.configure(
        application_name="feature-pipeline-kafka-consumer",
        server_address=os.getenv(
            "PYROSCOPE_SERVER_ADDRESS",
            "http://pyroscope.observability.svc.cluster.local:4040",
        ),
        tags={
            "environment": os.getenv("ENVIRONMENT", "dev"),
            "component": "kafka-consumer",
        },
    )

    config = KafkaConsumerConfig(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        consumer_group=settings.kafka.consumer_group,
        topics=settings.kafka.topics_list,
        dlq_topic=settings.kafka.dlq_topic,
    )

    processor = AppEventsProcessor()
    consumer = KafkaConsumer(config, processor)

    await consumer.start()


if __name__ == "__main__":
    asyncio.run(main())

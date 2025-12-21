"""
Flink Session Aggregation Job.

PyFlink job for real-time session feature extraction.
Consumes events from Kafka, aggregates into sessions, and writes features to Redis.

Note: Raw events to MinIO are handled separately by AppEventsProcessor.

Usage:
    # Local execution (requires Flink cluster)
    poetry run python -m src.flink.flink_session_job

    # Submit to Flink cluster
    flink run -py src/flink/flink_session_job.py
"""

import json
import os
from pathlib import Path
from typing import Any

# Load .env for local development
from dotenv import load_dotenv
from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration, Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    ProcessWindowFunction,
    RuntimeContext,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import (
    SessionWindowTimeGapExtractor,
    TumblingEventTimeWindows,
)

from src.flink.event_validator import EventValidator
from src.flink.feature_extractors import extract_session_features
from src.flink.redis_sink import RedisSink, RedisSinkConfig

env_path = Path(__file__).parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)


# Note: Using print() instead of structlog to avoid PyFlink CustomPrint flush issue


def get_required_env(key: str) -> str:
    """Get required environment variable or raise error."""
    value = os.getenv(key)
    if not value:
        raise ValueError(f"Missing required environment variable: {key}")
    return value


class FlinkSessionJobConfig:
    """Configuration for Flink session job."""

    def __init__(self):
        # Kafka settings
        self.kafka_bootstrap_servers = get_required_env("KAFKA_BOOTSTRAP_SERVERS")
        self.kafka_topics = os.getenv("KAFKA_TOPICS", "events.raw").split(",")
        self.kafka_consumer_group = os.getenv("KAFKA_CONSUMER_GROUP", "flink-session-job")
        self.kafka_dlq_topic = os.getenv("KAFKA_DLQ_TOPIC", "events.dlq")

        # Window settings (in seconds for real-time features)
        self.window_seconds = int(os.getenv("WINDOW_SECONDS", "30"))

        # Redis settings
        self.redis_config = RedisSinkConfig.from_env()

        # Checkpoint settings
        self.checkpoint_interval_ms = int(os.getenv("CHECKPOINT_INTERVAL_MS", "60000"))
        self.checkpoint_dir = os.getenv("CHECKPOINT_DIR", "s3://flink-checkpoints/session-job")


class SessionWindowGap(SessionWindowTimeGapExtractor):
    """Dynamic session gap extractor based on event type."""

    def __init__(self, default_gap_ms: int = 30 * 60 * 1000):
        self.default_gap_ms = default_gap_ms

    def extract(self, element: Any) -> int:
        """Extract session gap based on event."""
        # Could implement dynamic gaps based on event type
        # For now, use default 30 minute gap
        return self.default_gap_ms


class SessionAggregator(ProcessWindowFunction):
    """
    Aggregate events in a session window.

    Computes session-level features and outputs to Redis.
    """

    def __init__(self, redis_config: RedisSinkConfig):
        self.redis_config = redis_config
        self._redis_sink: RedisSink | None = None
        self._validator: EventValidator | None = None

    def open(self, runtime_context: RuntimeContext) -> None:
        """Initialize resources on task startup."""
        self._redis_sink = RedisSink(self.redis_config)
        self._validator = EventValidator()
        print("SessionAggregator opened")

    def close(self) -> None:
        """Clean up resources on task shutdown."""
        if self._redis_sink:
            self._redis_sink.close()
        print("SessionAggregator closed")

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Process session window elements.

        Args:
            key: User ID
            context: Window context
            elements: Events in the session window

        Returns:
            List containing session features
        """
        if not elements:
            return []

        # Extract session features
        features = extract_session_features(list(elements))
        if not features:
            return []

        # Write to Redis
        if self._redis_sink:
            try:
                self._redis_sink.write_session_features(
                    user_id=features.user_id,
                    session_id=features.session_id,
                    features=features.to_dict(),
                )
                print(f"Written session features for {features.user_id}:{features.session_id}")
            except Exception as e:
                print(f"Failed to write to Redis: {e}")

        # Return features for potential downstream processing
        return [features.to_dict()]


class EventRouter(KeyedProcessFunction):
    """
    Route events based on validation.

    Valid events continue downstream, invalid events go to DLQ.
    """

    def __init__(self):
        self._validator: EventValidator | None = None
        self._invalid_count_state = None

    def open(self, runtime_context: RuntimeContext) -> None:
        """Initialize validator."""
        self._validator = EventValidator()

        # Track invalid event count per key
        self._invalid_count_state = runtime_context.get_state(
            ValueStateDescriptor("invalid_count", Types.LONG())
        )

    def process_element(
        self,
        value: dict[str, Any],
        ctx: KeyedProcessFunction.Context,
    ) -> list[dict[str, Any]]:
        """
        Process and validate event.

        Args:
            value: Event dictionary
            ctx: Process context

        Returns:
            List with valid event or empty list
        """
        if not self._validator:
            return [value]

        result = self._validator.validate(value)

        if result.is_valid:
            return [value]
        else:
            # Track invalid events
            current_count = self._invalid_count_state.value() or 0
            self._invalid_count_state.update(current_count + 1)

            print(
                f"Warning: Invalid event for user {ctx.get_current_key()}: {result.error_message}"
            )

            # In production, send to DLQ via side output
            return []


class FlinkSessionJob:
    """
    Main Flink job for session aggregation.

    Pipeline:
    1. Consume from Kafka topics
    2. Parse and validate events
    3. Key by user_id
    4. Apply session window (30 min gap)
    5. Aggregate into session features
    6. Write features to Redis

    Note: Raw events → MinIO is handled by AppEventsProcessor (separate consumer)
    """

    def __init__(self, config: FlinkSessionJobConfig | None = None):
        self.config = config or FlinkSessionJobConfig()
        self.env: StreamExecutionEnvironment | None = None

    def setup_environment(self) -> StreamExecutionEnvironment:
        """Configure Flink streaming environment."""
        env = StreamExecutionEnvironment.get_execution_environment()

        # Enable checkpointing for fault tolerance
        env.enable_checkpointing(self.config.checkpoint_interval_ms)

        # Configure state backend (uses cluster config in production)
        # env.get_checkpoint_config().set_checkpoint_storage(self.config.checkpoint_dir)

        # Set parallelism
        env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "2")))

        self.env = env
        return env

    def create_kafka_source(self) -> KafkaSource:
        """Create Kafka source connector."""
        return (
            KafkaSource.builder()
            .set_bootstrap_servers(self.config.kafka_bootstrap_servers)
            .set_topics(*self.config.kafka_topics)
            .set_group_id(self.config.kafka_consumer_group)
            .set_starting_offsets(KafkaOffsetsInitializer.latest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )

    def run(self) -> None:
        """Execute the Flink job."""
        if not self.env:
            self.setup_environment()

        print("Starting Flink Session Aggregation Job")
        print(f"Kafka topics: {self.config.kafka_topics}")
        print(f"Window size: {self.config.window_seconds} seconds")

        # Create Kafka source
        kafka_source = self.create_kafka_source()

        # Define watermark strategy (event time with 5 second bounded out-of-orderness)
        watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))

        # Build pipeline
        if self.env:
            # Source: Kafka
            stream = self.env.from_source(
                kafka_source,
                watermark_strategy,
                "Kafka Source",
            )

            # Parse JSON - use PICKLED_BYTE_ARRAY to handle nested dicts
            parsed = stream.map(
                lambda x: json.loads(x) if x else {},
                output_type=Types.PICKLED_BYTE_ARRAY(),
            )

            # Filter and validate
            # In production, use EventRouter with side outputs for DLQ
            validated = parsed.filter(lambda x: x.get("user_id") is not None)

            # Note: Raw events to MinIO are handled by AppEventsProcessor (separate consumer)
            # This Flink job focuses on session aggregation to Redis

            # Key by user_id
            keyed = validated.key_by(lambda x: x.get("user_id", "unknown"))

            # Tumbling window for near real-time feature updates
            window_duration = Time.seconds(self.config.window_seconds)
            windowed = keyed.window(TumblingEventTimeWindows.of(window_duration))

            # Aggregate sessions and write features to Redis
            aggregated = windowed.process(SessionAggregator(self.config.redis_config))

            # Print results (for debugging)
            aggregated.print()

            # Execute
            self.env.execute("Session Feature Aggregation Job")


def main():
    """Entry point for local execution."""
    print("Initializing Flink Session Job")

    try:
        job = FlinkSessionJob()
        job.run()
    except Exception as e:
        print(f"Job failed: {e}")
        raise


if __name__ == "__main__":
    main()

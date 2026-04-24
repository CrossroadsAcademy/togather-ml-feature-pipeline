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

import os
import threading

# Note: Using print() instead of structlog to avoid PyFlink CustomPrint flush issue
import time
from datetime import datetime
from pathlib import Path
from typing import Any

# Load .env for local development
from dotenv import load_dotenv
from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
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
    TumblingProcessingTimeWindows,
)

from src.flink.event_validator import EventValidator
from src.flink.feature_extractors import (
    extract_realtime_user_features,
    extract_session_features,
)
from src.flink.redis_sink import RedisSink, RedisSinkConfig

env_path = Path(__file__).parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)


class StatusLogger:
    """
    Background status logger for Flink job health monitoring.

    Logs periodic heartbeat messages with event counts and idle duration.
    Useful for detecting when no events are flowing from upstream.
    """

    _instance: "StatusLogger | None" = None
    _lock = threading.Lock()
    _initialized: bool = False

    def __new__(cls):
        """Singleton pattern to ensure one logger per job."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.events_processed = 0
        self.last_event_time: float | None = None
        self.start_time = time.time()
        self.status_interval_seconds = int(
            os.getenv("STATUS_LOG_INTERVAL", "300")
        )  # 5 minutes default
        self._running = False
        self._thread: threading.Thread | None = None
        self._initialized = True

    def start(self):
        """Start background status logging thread."""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._log_status_loop, daemon=True)
        self._thread.start()
        print(f"[STATUS] Status logger started (interval: {self.status_interval_seconds}s)")

    def stop(self):
        """Stop the status logging thread."""
        self._running = False

    def record_event(self):
        """Record that an event was processed."""
        self.events_processed += 1
        self.last_event_time = time.time()

    def _log_status_loop(self):
        """Background loop that logs status periodically."""
        while self._running:
            time.sleep(self.status_interval_seconds)
            self._log_status()

    def _log_status(self):
        """Log current status."""
        uptime = time.time() - self.start_time
        uptime_str = f"{int(uptime // 3600)}h {int((uptime % 3600) // 60)}m"

        if self.last_event_time:
            idle_seconds = time.time() - self.last_event_time
            idle_str = f"{int(idle_seconds)}s ago"
            if idle_seconds > 300:  # More than 5 min idle
                idle_str = f"⚠️ {int(idle_seconds // 60)}m {int(idle_seconds % 60)}s ago (IDLE)"
        else:
            idle_str = "No events yet"

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(
            f"[STATUS] {current_time} | "
            f"Events: {self.events_processed} | "
            f"Last event: {idle_str} | "
            f"Uptime: {uptime_str}"
        )


# Global status logger instance
status_logger = StatusLogger()


class EventTimestampAssigner:
    """
    Extract event timestamps for watermark generation.

    Handles multiple timestamp sources with fallbacks:
    1. _timestamp from EventEnvelope (proto) - milliseconds
    2. timestamp from JSON payload - auto-detect seconds vs milliseconds
    3. Processing time fallback
    """

    def extract_timestamp(self, event: dict, recorded_timestamp: int) -> int:
        """
        Extract event timestamp in milliseconds.

        Args:
            event: Parsed event dictionary
            recorded_timestamp: Processing time in milliseconds (fallback)

        Returns:
            Event timestamp in milliseconds
        """
        # 1. Try EventEnvelope timestamp (from proto parsing)
        envelope_ts = event.get("_timestamp")
        if envelope_ts and isinstance(envelope_ts, int | float) and envelope_ts > 0:
            # Already in milliseconds from proto
            return int(envelope_ts)

        # 2. Try payload timestamp field (JSON events)
        payload_ts = event.get("timestamp")
        if payload_ts and isinstance(payload_ts, int | float) and payload_ts > 0:
            # Auto-detect seconds vs milliseconds
            # Timestamps after year 2001 in seconds would be > 1e9
            # Timestamps in milliseconds would be > 1e12
            if payload_ts > 1e12:
                return int(payload_ts)  # Already milliseconds
            else:
                return int(payload_ts * 1000)  # Convert seconds to ms

        # 3. Try created_at field (common in some protos)
        created_at = event.get("created_at")
        if created_at and isinstance(created_at, int | float) and created_at > 0:
            if created_at > 1e12:
                return int(created_at)
            else:
                return int(created_at * 1000)

        # 4. Fallback to processing time
        return recorded_timestamp if recorded_timestamp > 0 else int(time.time() * 1000)


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

        # Kafka security settings (for cloud Kafka)
        self.kafka_security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
        self.kafka_sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM", "")
        self.kafka_sasl_username = os.getenv("KAFKA_SASL_USERNAME", "")
        self.kafka_sasl_password = os.getenv("KAFKA_SASL_PASSWORD", "")
        self.kafka_ssl_ca_location = os.getenv("KAFKA_SSL_CA_LOCATION", "")

        # MinIO settings (for DLQ)
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio.platform.svc.cluster.local:9000")
        self.minio_access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("MINIO_ACCESS_KEY", "")
        self.minio_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv(
            "MINIO_SECRET_KEY", ""
        )
        self.dlq_bucket = os.getenv("DLQ_BUCKET", "raw-events-dlq")

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
        # default 30 minute gap
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

        events_list = list(elements)

        # Extract session features
        session_features = extract_session_features(events_list)
        if not session_features:
            return []

        # Extract realtime user features (for ranking service)
        realtime_features = extract_realtime_user_features(events_list)

        # Write to Redis
        if self._redis_sink:
            # Write session features
            try:
                self._redis_sink.write_session_features(
                    user_id=session_features.user_id,
                    session_id=session_features.session_id,
                    features=session_features.to_dict(),
                )
            except Exception as e:
                print(f"Failed to write session features to Redis: {e}")

            # Write realtime user features (for ranking service)
            if realtime_features:
                try:
                    self._redis_sink.write_realtime_user_features(
                        user_id=realtime_features.user_id,
                        features=realtime_features.to_dict(),
                    )
                except Exception as e:
                    print(f"Failed to write realtime features to Redis: {e}")

        # Return features for potential downstream processing
        return [session_features.to_dict()]


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
            if self._invalid_count_state is not None:
                current_count = self._invalid_count_state.value() or 0
                self._invalid_count_state.update(current_count + 1)

            print(
                f"Warning: Invalid event for user {ctx.get_current_key()}: {result.error_message}"
            )

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
        """Create Kafka source connector with SASL/SSL support for cloud Kafka."""
        builder = (
            KafkaSource.builder()
            .set_bootstrap_servers(self.config.kafka_bootstrap_servers)
            .set_topics(*self.config.kafka_topics)
            .set_group_id(self.config.kafka_consumer_group)
            .set_starting_offsets(KafkaOffsetsInitializer.latest())
            .set_value_only_deserializer(SimpleStringSchema("iso-8859-1"))
        )

        # Add SASL/SSL properties for cloud Kafka
        if self.config.kafka_security_protocol != "PLAINTEXT":
            print(
                f"Configuring Kafka with security protocol: {self.config.kafka_security_protocol}"
            )
            builder = builder.set_property("security.protocol", self.config.kafka_security_protocol)

            if self.config.kafka_sasl_mechanism:
                builder = builder.set_property("sasl.mechanism", self.config.kafka_sasl_mechanism)
                # Build JAAS config for SCRAM authentication
                jaas_config = (
                    f"org.apache.kafka.common.security.scram.ScramLoginModule required "
                    f'username="{self.config.kafka_sasl_username}" '
                    f'password="{self.config.kafka_sasl_password}";'
                )
                builder = builder.set_property("sasl.jaas.config", jaas_config)

            if self.config.kafka_ssl_ca_location:
                # For Java Kafka client, use truststore if JKS, otherwise PEM
                if self.config.kafka_ssl_ca_location.endswith(".jks"):
                    builder = builder.set_property(
                        "ssl.truststore.location", self.config.kafka_ssl_ca_location
                    )
                else:
                    builder = builder.set_property("ssl.truststore.type", "PEM")
                    builder = builder.set_property(
                        "ssl.truststore.location", self.config.kafka_ssl_ca_location
                    )

        return builder.build()

    def write_to_dlq(self, dlq_record: dict) -> None:
        """Write a record to MinIO DLQ bucket."""
        import json
        from datetime import datetime

        import boto3

        try:
            # Create S3 client for MinIO
            s3_client = boto3.client(
                "s3",
                endpoint_url=f"http://{self.config.minio_endpoint}",
                aws_access_key_id=self.config.minio_access_key,
                aws_secret_access_key=self.config.minio_secret_key,
            )

            # Generate unique key with timestamp partitioning
            now = datetime.utcnow()
            key = (
                f"flink-session-job/"
                f"year={now.year:04d}/month={now.month:02d}/day={now.day:02d}/"
                f"hour={now.hour:02d}/dlq-{now.strftime('%Y%m%d%H%M%S')}-{id(dlq_record)}.json"
            )

            # Write to MinIO
            s3_client.put_object(
                Bucket=self.config.dlq_bucket,
                Key=key,
                Body=json.dumps(dlq_record).encode("utf-8"),
                ContentType="application/json",
            )

            print(f"DLQ record written to s3://{self.config.dlq_bucket}/{key}")

        except Exception as e:
            print(f"Failed to write DLQ record to MinIO: {e}")

    def run(self) -> None:
        """Execute the Flink job."""
        if not self.env:
            self.setup_environment()

        print("Starting Flink Session Aggregation Job")
        print(f"Kafka topics: {self.config.kafka_topics}")
        print(f"Window size: {self.config.window_seconds} seconds")

        # Create Kafka source
        kafka_source = self.create_kafka_source()

        # Initial watermark strategy for Kafka source (will refine after parsing)
        initial_watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()

        # Build pipeline
        if self.env:
            # Source: Kafka
            stream = self.env.from_source(
                kafka_source,
                initial_watermark_strategy,
                "Kafka Source",
            )

            # Parse messages using EventEnvelope parser
            # Import here to ensure module is available after job submission
            from src.flink.event_envelope_parser import (
                get_user_id_from_event,
                parse_kafka_message,
            )

            # Create timestamp assigner instance
            ts_assigner = EventTimestampAssigner()

            def parse_and_assign_timestamp(raw_string: str) -> dict:
                """Parse message and embed timestamp for watermark generation."""
                event = parse_kafka_message(raw_string) if raw_string else {}
                # Extract event timestamp
                event_ts = ts_assigner.extract_timestamp(event, int(time.time() * 1000))
                # Store for later use and debugging
                event["_extracted_timestamp"] = event_ts
                return event

            parsed = stream.map(
                parse_and_assign_timestamp,
                output_type=Types.PICKLED_BYTE_ARRAY(),
            )

            # DEBUG: Log ALL parsed events before filtering
            def debug_all_events(event):
                user_id = get_user_id_from_event(event)
                event_type = event.get("_event_type", "N/A")
                parse_error = event.get("_parse_error", None)

                # Print detailed error info
                if parse_error:
                    raw = event.get("_raw", "")[:50] if event.get("_raw") else ""
                    print(f"[PRE-FILTER] PARSE_ERROR: {parse_error}, raw={raw}")
                else:
                    keys = list(event.keys())[:10]
                    print(f"[PRE-FILTER] type={event_type}, user_id={user_id}, keys={keys}")

                # IMPORTANT: Enrich event with user_id so feature extractors can find it
                if user_id:
                    event["user_id"] = user_id

                return event

            parsed_debug = parsed.map(debug_all_events, output_type=Types.PICKLED_BYTE_ARRAY())

            # Split stream: valid events continue, parse errors go to DLQ
            import json

            # Get events with parse errors for DLQ
            def has_parse_error(event) -> bool:
                return (
                    event.get("_parse_error") is not None or get_user_id_from_event(event) is None
                )

            def event_to_dlq_json(event) -> str:
                """Serialize event to JSON for DLQ."""
                try:
                    # Add metadata for DLQ analysis
                    dlq_record = {
                        "original_event": {
                            k: str(v)[:500] for k, v in event.items() if not k.startswith("_raw")
                        },
                        "error": event.get("_parse_error", "Missing user_id"),
                        "timestamp": int(time.time() * 1000),
                        "source": "flink-session-job",
                    }
                    return json.dumps(dlq_record)
                except Exception as e:
                    return json.dumps({"error": f"DLQ serialization failed: {str(e)}"})

            # Route parse errors to DLQ (MinIO bucket)
            dlq_events = parsed_debug.filter(has_parse_error)

            # Write DLQ events to MinIO using a simple sink
            def write_dlq_event(event):
                """Write single DLQ event to MinIO."""
                import json
                from datetime import datetime

                import boto3

                try:
                    # Create DLQ record
                    dlq_record = {
                        "original_event": {
                            k: str(v)[:500] for k, v in event.items() if not k.startswith("_raw")
                        },
                        "error": event.get("_parse_error", "Missing user_id"),
                        "timestamp": int(time.time() * 1000),
                        "source": "flink-session-job",
                    }

                    # Get MinIO config from env
                    endpoint = os.getenv("MINIO_ENDPOINT", "minio.platform.svc.cluster.local:9000")
                    access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("MINIO_ACCESS_KEY", "")
                    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv(
                        "MINIO_SECRET_KEY", ""
                    )
                    bucket = os.getenv("DLQ_BUCKET", "raw-events-dlq")

                    # Create S3 client for MinIO
                    s3_client = boto3.client(
                        "s3",
                        endpoint_url=f"http://{endpoint}",
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key,
                    )

                    # Generate unique key
                    now = datetime.utcnow()
                    key = (
                        f"flink-session-job/"
                        f"year={now.year:04d}/month={now.month:02d}/day={now.day:02d}/"
                        f"hour={now.hour:02d}/dlq-{now.strftime('%Y%m%d%H%M%S')}-{id(event)}.json"
                    )

                    # Write to MinIO
                    s3_client.put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=json.dumps(dlq_record).encode("utf-8"),
                        ContentType="application/json",
                    )

                    print(f"DLQ record written to s3://{bucket}/{key}")

                except Exception as e:
                    print(f"Failed to write DLQ record: {e}")

                return event  # Pass through for debugging

            # Apply DLQ write to each error event
            dlq_events.map(write_dlq_event, output_type=Types.PICKLED_BYTE_ARRAY())
            print(
                f"DLQ routing enabled - errors will be sent to MinIO bucket: {self.config.dlq_bucket}"
            )

            # Filter events that have a valid user_id (for normal processing)
            # Uses helper that handles different proto field names (id vs user_id)
            validated = parsed_debug.filter(
                lambda x: get_user_id_from_event(x) is not None and x.get("_parse_error") is None
            )

            # Add debug counter for validated events
            def debug_and_pass(event):
                user_id = get_user_id_from_event(event)
                ts = event.get("_extracted_timestamp", 0)
                print(
                    f"[DEBUG] Valid event: user={user_id}, ts={ts}, type={event.get('_event_type', 'json')}"
                )
                return event

            debugged = validated.map(debug_and_pass, output_type=Types.PICKLED_BYTE_ARRAY())

            # This Flink job focuses on session aggregation to Redis

            # Key by user_id (extracted from various fields depending on event type)
            keyed = debugged.key_by(lambda x: get_user_id_from_event(x) or "unknown")

            # Tumbling window for near real-time feature updates
            # Using ProcessingTime since StringSchema source doesn't have native event-time
            # Event timestamps are still extracted and stored in events for feature computation
            window_duration = Time.seconds(self.config.window_seconds)
            windowed = keyed.window(TumblingProcessingTimeWindows.of(window_duration))

            # Aggregate sessions and write features to Redis
            aggregated = windowed.process(SessionAggregator(self.config.redis_config))

            # Print results (for debugging)
            aggregated.print()

            # Execute
            self.env.execute("Session Feature Aggregation Job")


def main():
    """Entry point for local execution."""
    print("Initializing Flink Session Job")

    # Start status logger for pipeline health monitoring
    status_logger.start()

    try:
        job = FlinkSessionJob()
        job.run()
    except Exception as e:
        print(f"Job failed: {e}")
        raise


if __name__ == "__main__":
    main()

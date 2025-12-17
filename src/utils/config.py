"""Configuration management using Pydantic Settings."""

from typing import Literal  # noqa: UP035

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    """Kafka configuration settings following best practices."""

    bootstrap_servers: str = Field(default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    consumer_group: str = Field(default="app-events-consumer", alias="KAFKA_CONSUMER_GROUP")
    topics: list[str] = Field(
        default=[
            "user.account",
            "user.profile",
            "experience",
            "engagement",
            "location.streams",
        ],
        alias="KAFKA_TOPICS",
    )
    auto_offset_reset: str = Field(default="latest", alias="KAFKA_AUTO_OFFSET_RESET")
    enable_auto_commit: bool = Field(
        default=False, alias="KAFKA_ENABLE_AUTO_COMMIT"
    )  # Best practice: manual commits
    session_timeout_ms: int = Field(default=30000, alias="KAFKA_SESSION_TIMEOUT_MS")
    heartbeat_interval_ms: int = Field(default=10000, alias="KAFKA_HEARTBEAT_INTERVAL_MS")
    max_poll_records: int = Field(default=500, alias="KAFKA_MAX_POLL_RECORDS")
    max_poll_interval_ms: int = Field(default=300000, alias="KAFKA_MAX_POLL_INTERVAL_MS")
    dlq_topic: str = Field(default="app-events-dlq", alias="KAFKA_DLQ_TOPIC")
    # Security settings
    security_protocol: str = Field(default="PLAINTEXT", alias="KAFKA_SECURITY_PROTOCOL")
    sasl_mechanism: str | None = Field(default=None, alias="KAFKA_SASL_MECHANISM")
    sasl_username: str | None = Field(default=None, alias="KAFKA_SASL_USERNAME")
    sasl_password: str | None = Field(default=None, alias="KAFKA_SASL_PASSWORD")
    ssl_ca_location: str | None = Field(default=None, alias="KAFKA_SSL_CA_LOCATION")


class SchemaRegistrySettings(BaseSettings):
    """Schema Registry configuration settings."""

    url: str = Field(default="http://localhost:8081", alias="SCHEMA_REGISTRY_URL")
    username: str | None = Field(default=None, alias="SCHEMA_REGISTRY_USERNAME")
    password: str | None = Field(default=None, alias="SCHEMA_REGISTRY_PASSWORD")
    ssl_ca_location: str | None = Field(default=None, alias="SCHEMA_REGISTRY_SSL_CA_LOCATION")


class RedisSettings(BaseSettings):
    """Redis configuration settings."""

    host: str = Field(default="localhost", alias="REDIS_HOST")
    port: int = Field(default=6379, alias="REDIS_PORT")
    password: str | None = Field(default=None, alias="REDIS_PASSWORD")
    db: int = Field(default=0, alias="REDIS_DB")
    ttl: int = Field(default=86400, alias="REDIS_TTL")


class S3Settings(BaseSettings):
    """S3/MinIO configuration settings."""

    endpoint_url: str | None = Field(default=None, alias="S3_ENDPOINT_URL")
    access_key_id: str | None = Field(default=None, alias="AWS_ACCESS_KEY_ID")
    secret_access_key: str | None = Field(default=None, alias="AWS_SECRET_ACCESS_KEY")
    bucket_name: str = Field(default="togather-ml-features", alias="S3_BUCKET_NAME")
    region: str = Field(default="us-east-1", alias="AWS_REGION")


class MinIOSettings(BaseSettings):
    """MinIO configuration settings."""

    endpoint_url: str = Field(default="localhost:9000", alias="MINIO_ENDPOINT_URL")
    access_key_id: str = Field(default="minioadmin", alias="MINIO_ACCESS_KEY")
    secret_access_key: str = Field(default="minioadmin123", alias="MINIO_SECRET_KEY")
    bucket_name: str = Field(default="togather-ml-features", alias="MINIO_BUCKET_NAME")
    secure: bool = Field(default=False, alias="MINIO_SECURE")
    region: str = Field(default="us-east-1", alias="MINIO_REGION")


class InfisicalSettings(BaseSettings):
    """Infisical configuration settings."""

    url: str = Field(default="https://app.infisical.com", alias="INFISICAL_URL")
    service_token: str | None = Field(default=None, alias="INFISICAL_SERVICE_TOKEN")
    project_id: str | None = Field(default=None, alias="INFISICAL_PROJECT_ID")
    environment: str = Field(default="development", alias="INFISICAL_ENVIRONMENT")
    enable: bool = Field(default=True, alias="ENABLE_INFISICAL")


class RetrySettings(BaseSettings):
    """Retry configuration settings."""

    max_attempts: int = Field(default=3, alias="RETRY_MAX_ATTEMPTS")
    initial_delay: float = Field(default=5.0, alias="RETRY_INITIAL_DELAY")
    max_delay: float = Field(default=300.0, alias="RETRY_MAX_DELAY")


class MetricsSettings(BaseSettings):
    """Metrics configuration settings."""

    port: int = Field(default=8080, alias="METRICS_PORT")
    enabled: bool = Field(default=True, alias="METRICS_ENABLED")


class Settings(BaseSettings):
    """Application settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",  # Allow extra env vars without validation errors
    )

    # Application
    app_env: Literal["development", "staging", "production"] = Field(
        default="development", alias="APP_ENV"
    )
    app_name: str = Field(default="togather-feature-pipeline", alias="APP_NAME")
    debug: bool = Field(default=False, alias="DEBUG")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    # API
    api_host: str = Field(default="0.0.0.0", alias="API_HOST")
    api_port: int = Field(default=8000, alias="API_PORT")

    # Component settings
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    schema_registry: SchemaRegistrySettings = Field(default_factory=SchemaRegistrySettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    s3: S3Settings = Field(default_factory=S3Settings)
    minio: MinIOSettings = Field(default_factory=MinIOSettings)
    infisical: InfisicalSettings = Field(default_factory=InfisicalSettings)
    retry: RetrySettings = Field(default_factory=RetrySettings)
    metrics: MetricsSettings = Field(default_factory=MetricsSettings)


settings = Settings()

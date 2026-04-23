"""
Health Check Server for Streaming Components.

Provides HTTP endpoints for Kubernetes liveness and readiness probes.
Also exposes Prometheus metrics endpoint.

Usage:
    # Start as part of consumer application
    from src.streaming.health import HealthServer

    health = HealthServer(port=8080)
    health.register_component("kafka", kafka_consumer.is_healthy)
    health.register_component("redis", redis_sink.is_healthy)
    health.register_component("minio", storage_sink.is_healthy)
    await health.start()
"""

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest

from src.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ComponentHealth:
    """Health status of a single component."""

    name: str
    healthy: bool
    last_check: float
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class OverallHealth:
    """Overall health status."""

    status: str  # "healthy", "degraded", "unhealthy"
    components: list[ComponentHealth]
    uptime_seconds: float
    version: str = "1.0.0"

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "uptime_seconds": round(self.uptime_seconds, 2),
            "version": self.version,
            "components": [
                {
                    "name": c.name,
                    "healthy": c.healthy,
                    "message": c.message,
                    "details": c.details,
                }
                for c in self.components
            ],
        }


class HealthServer:
    """
    HTTP server for health checks and metrics.

    Endpoints:
        GET /health      - Full health check (for readinessProbe)
        GET /healthz     - Simple liveness check (for livenessProbe)
        GET /metrics     - Prometheus metrics
        GET /ready       - Readiness check (alias for /health)
    """

    def __init__(self, port: int = 8080, host: str = "0.0.0.0"):
        self.port = port
        self.host = host
        self.start_time = time.time()
        self._components: dict[str, Callable[[], bool | tuple[bool, str]]] = {}
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None

    def register_component(
        self, name: str, check_fn: Callable[[], bool | tuple[bool, str]]
    ) -> None:
        """
        Register a component health check function.

        Args:
            name: Component name (e.g., "kafka", "redis", "minio")
            check_fn: Function that returns True/False or (bool, message)
        """
        self._components[name] = check_fn
        logger.info(f"Registered health check for component: {name}")

    def _check_component(self, name: str, check_fn: Callable) -> ComponentHealth:
        """Check a single component's health."""
        try:
            result = check_fn()
            if isinstance(result, tuple):
                healthy, message = result
            else:
                healthy = bool(result)
                message = "OK" if healthy else "Unhealthy"

            return ComponentHealth(
                name=name,
                healthy=healthy,
                last_check=time.time(),
                message=message,
            )
        except Exception as e:
            return ComponentHealth(
                name=name,
                healthy=False,
                last_check=time.time(),
                message=f"Check failed: {str(e)}",
            )

    def check_health(self) -> OverallHealth:
        """Perform full health check of all components."""
        components = [self._check_component(name, fn) for name, fn in self._components.items()]

        healthy_count = sum(1 for c in components if c.healthy)
        total_count = len(components)

        if healthy_count == total_count:
            status = "healthy"
        elif healthy_count > 0:
            status = "degraded"
        else:
            status = "unhealthy"

        return OverallHealth(
            status=status,
            components=components,
            uptime_seconds=time.time() - self.start_time,
        )

    async def _handle_health(self, request: web.Request) -> web.Response:
        """Handle /health endpoint (full check)."""
        health = self.check_health()
        status_code = 200 if health.status == "healthy" else 503

        return web.json_response(health.to_dict(), status=status_code)

    async def _handle_healthz(self, request: web.Request) -> web.Response:
        """Handle /healthz endpoint (simple liveness)."""
        # Simple check - just verify the server is running
        return web.json_response(
            {
                "status": "alive",
                "uptime_seconds": round(time.time() - self.start_time, 2),
            }
        )

    async def _handle_ready(self, request: web.Request) -> web.Response:
        """Handle /ready endpoint (readiness check)."""
        health = self.check_health()

        # Ready if healthy or degraded (can still serve some traffic)
        is_ready = health.status in ("healthy", "degraded")
        status_code = 200 if is_ready else 503

        return web.json_response(
            {
                "ready": is_ready,
                "status": health.status,
            },
            status=status_code,
        )

    async def _handle_metrics(self, request: web.Request) -> web.Response:
        """Handle /metrics endpoint (Prometheus)."""
        metrics = generate_latest(REGISTRY)
        return web.Response(
            body=metrics,
            content_type=CONTENT_TYPE_LATEST,
        )

    async def start(self) -> None:
        """Start the health check server."""
        self._app = web.Application()
        self._app.router.add_get("/health", self._handle_health)
        self._app.router.add_get("/healthz", self._handle_healthz)
        self._app.router.add_get("/ready", self._handle_ready)
        self._app.router.add_get("/metrics", self._handle_metrics)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()

        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()

        logger.info(f"Health server started on http://{self.host}:{self.port}")

    async def stop(self) -> None:
        """Stop the health check server."""
        if self._runner:
            await self._runner.cleanup()
            logger.info("Health server stopped")


# Convenience functions for creating health checks
def create_kafka_health_check(consumer) -> Callable[[], tuple[bool, str]]:
    """Create health check for Kafka consumer."""

    def check() -> tuple[bool, str]:
        try:
            # Check if consumer is subscribed and can reach broker
            assignment = consumer.assignment()
            if not assignment:
                return False, "No partitions assigned"
            return True, f"Consuming from {len(assignment)} partitions"
        except Exception as e:
            return False, str(e)

    return check


def create_redis_health_check(redis_client) -> Callable[[], tuple[bool, str]]:
    """Create health check for Redis connection."""

    def check() -> tuple[bool, str]:
        try:
            redis_client.ping()
            info = redis_client.info("memory")
            used_memory_mb = info.get("used_memory", 0) / 1024 / 1024
            return True, f"Connected, memory: {used_memory_mb:.1f}MB"
        except Exception as e:
            return False, str(e)

    return check


def create_minio_health_check(minio_client, bucket: str) -> Callable[[], tuple[bool, str]]:
    """Create health check for MinIO connection."""

    def check() -> tuple[bool, str]:
        try:
            exists = minio_client.bucket_exists(bucket)
            if exists:
                return True, f"Bucket '{bucket}' accessible"
            return False, f"Bucket '{bucket}' not found"
        except Exception as e:
            return False, str(e)

    return check

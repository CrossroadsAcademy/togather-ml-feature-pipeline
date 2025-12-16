import os

import pyroscope
from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI(
    title="ToGather Feature Pipeline",
    description="Feature engineering pipeline (MOCK)",
    version="0.1.0",
)


# OpenTelemetry Tracing
Instrumentator().instrument(app).expose(app)


# Configure tracing
resource = Resource(attributes={"service.name": "feature-pipeline-service"})
trace.set_tracer_provider(TracerProvider(resource=resource))
otlp_exporter = OTLPSpanExporter(
    endpoint=os.getenv(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://tempo.observability.svc.cluster.local:4317",
    ),
    insecure=True,
)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(otlp_exporter))  # type: ignore[attr-defined]
FastAPIInstrumentor.instrument_app(app)

# Pyroscope Profiling

pyroscope.configure(
    application_name="feature-pipeline-service",
    server_address=os.getenv(
        "PYROSCOPE_SERVER_ADDRESS",
        "http://pyroscope.observability.svc.cluster.local:4040",
    ),
    tags={
        "environment": os.getenv("ENVIRONMENT", "dev"),
    },
)


@app.get("/")
def root():
    return {"service": "feature-pipeline", "status": "running", "mode": "mock"}


@app.get("/health")
def health():
    return {"status": "healthy"}

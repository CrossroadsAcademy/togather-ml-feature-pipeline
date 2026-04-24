
# Build Dependencies - Compile time
FROM python:3.10-slim-bookworm AS builder

# Set work directory
WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    python3-dev \
    gcc g++ make \
    curl \
    && rm -rf /var/lib/apt/lists/*


# Install Poetry

ENV POETRY_HOME="/opt/poetry"
ENV PATH="$POETRY_HOME/bin:$PATH"
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    poetry --version


# Dependency install (cached)

COPY pyproject.toml poetry.lock* ./
RUN poetry config virtualenvs.in-project true && \
    poetry install --no-interaction --no-ansi --no-root --without dev,jvm


# Copy application code

COPY src/ ./src


# Build - Runtime (clean and light)
FROM python:3.10-slim-bookworm

WORKDIR /app

#Copy from builder
COPY --from=builder /app /app


# Environment variables

ENV PYTHONUNBUFFERED=1
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV PYTHONPATH=/app/src
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64


# Expose API port

EXPOSE 8000


# Health check

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1


# Switch to non-root user

RUN useradd -m appuser
USER appuser


# Default command

CMD ["/app/.venv/bin/uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

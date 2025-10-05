
FROM python:3.10-slim-bookworm

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
RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi --no-root --without dev,jvm


# Copy application code

COPY src/ ./src
COPY README.md .


# Environment variables

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64


# Expose API port

EXPOSE 8000


# Health check

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1


# Switch to non-root user (optional)

RUN useradd -m appuser
USER appuser


# Default command

CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

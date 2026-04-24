# Togather ML Feature Pipeline

A feature engineering layer for the Togather ML stack.
Supports **batch (PySpark)** and **real-time (Flink)** feature generation.
Integrates with **Feast**, **MinIO (S3-compatible)**, and **Redis** for online feature freshness.

---
## Data Flow

1. **Ingestion**: Services publish events to Kafka topics
2. **Streaming**: Real-time features computed via Flink/Kafka
3. **Batch**: Daily aggregations computed via Spark
4. **Storage**:
   - Hot features → Redis (online serving)
   - Historical features → S3/Delta Lake (training)

---

## Prerequisites

- Python 3.10+
- Poetry 1.7+
- Docker & Docker Compose
- Java 11+ (for Flink/Spark)
- Access to:
  - Kafka cluster
  - Redis instance
  - S3 bucket (or local storage)
  - Flink cluster
  - Spark cluster


---

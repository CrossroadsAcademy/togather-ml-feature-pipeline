"""
Script to initialize DVC bucket in MinIO.

This script creates the necessary S3 bucket for DVC storage:
- dvc-data: For dataset versioning (used by feature-pipeline and training-pipeline)

Models are managed via MLflow, not DVC.

Usage:
    poetry run python scripts/init_dvc_buckets.py
"""

import os
import sys

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

# Load environment variables
load_dotenv()


def get_required_env(key: str) -> str:
    """Get required environment variable or exit with error."""
    value = os.getenv(key)
    if not value:
        print(f" Missing required environment variable: {key}")
        print(f"   Set it in .env file or export {key}=<value>")
        sys.exit(1)
    return value


# MinIO configuration
MINIO_ENDPOINT = get_required_env("MINIO_ENDPOINT_URL")
MINIO_ACCESS_KEY = get_required_env("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = get_required_env("MINIO_SECRET_KEY")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# DVC bucket
DVC_BUCKETS = ["dvc-data"]


def init_dvc_buckets():
    """Initialize DVC bucket in MinIO."""
    print(f"Connecting to MinIO at {MINIO_ENDPOINT}")

    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )

        for bucket_name in DVC_BUCKETS:
            if client.bucket_exists(bucket_name):
                print(f"   Bucket '{bucket_name}' already exists")
            else:
                client.make_bucket(bucket_name)
                print(f"   Created bucket '{bucket_name}'")

        print("\n DVC bucket initialized successfully!")
        print("\nUsage:")
        print("  feature-pipeline:  make dvc-push    # Push datasets to MinIO")
        print("  training-pipeline: make dvc-pull    # Pull datasets from MinIO")

        return True

    except S3Error as e:
        print(f" MinIO error: {e}")
        return False
    except Exception as e:
        print(f" Error: {e}")
        print("\nMake sure MinIO is running and port-forwarded:")
        print("  kubectl port-forward svc/minio -n platform 9000:9000")
        return False


if __name__ == "__main__":
    success = init_dvc_buckets()
    sys.exit(0 if success else 1)

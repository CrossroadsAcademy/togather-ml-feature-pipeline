#!/usr/bin/env python3
"""
Test script to verify Spark S3A connectivity to MinIO.

This script tests if Spark can:
1. Connect to MinIO via S3A
2. List files in a bucket
3. Read a single parquet file

Usage (inside Spark driver pod or locally with port-forward):
    spark-submit test_s3a_connection.py

Or run directly in the driver pod:
    kubectl exec -it <driver-pod> -- python /path/to/test_s3a_connection.py
"""

import os
import sys
import time


def test_s3a_connection():
    """Test S3A connectivity to MinIO."""

    print("=" * 60)
    print("S3A Connection Test for MinIO")
    print("=" * 60)

    # Configuration from environment variables
    minio_endpoint = os.getenv(
        "MINIO_ENDPOINT_URL", "http://minio-0.minio.platform.svc.cluster.local:9000"
    )
    access_key = os.getenv("MINIO_ACCESS_KEY", os.getenv("AWS_ACCESS_KEY_ID"))
    secret_key = os.getenv("MINIO_SECRET_KEY", os.getenv("AWS_SECRET_ACCESS_KEY"))
    bucket = os.getenv("MINIO_BUCKET", "raw-events")

    if not access_key or not secret_key:
        print(
            "ERROR: Missing credentials. Set MINIO_ACCESS_KEY/MINIO_SECRET_KEY or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY"
        )
        return False

    print("\n1. Configuration:")
    print(f"   Endpoint: {minio_endpoint}")
    print(f"   Bucket: {bucket}")
    print(f"   Access Key: {access_key[:4]}...{access_key[-2:]}")

    try:
        from pyspark.sql import SparkSession

        print("\n2. Creating SparkSession with S3A config...")
        start = time.time()

        spark = (
            SparkSession.builder.appName("S3A-Test")
            .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
        )

        print(f"   SparkSession created in {time.time() - start:.2f}s")
        print(f"   App ID: {spark.sparkContext.applicationId}")

        # Test 1: List bucket root
        print("\n3. Testing S3A file listing...")
        start = time.time()

        try:
            # Get hadoop filesystem
            sc = spark.sparkContext
            hadoop_conf = sc._jsc.hadoopConfiguration()

            # Set configs again (in case they weren't picked up)
            hadoop_conf.set("fs.s3a.endpoint", minio_endpoint)
            hadoop_conf.set("fs.s3a.access.key", access_key)
            hadoop_conf.set("fs.s3a.secret.key", secret_key)
            hadoop_conf.set("fs.s3a.path.style.access", "true")
            hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

            # Try to list using Hadoop API
            path = sc._jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/")
            fs = path.getFileSystem(hadoop_conf)

            print(f"   Listing s3a://{bucket}/...")
            status_list = fs.listStatus(path)

            file_count = len(status_list)
            print(f"   Found {file_count} items in bucket root")

            for _, status in enumerate(status_list[:5]):
                print(f"     - {status.getPath().getName()}")

            print(f"   Listing completed in {time.time() - start:.2f}s")

        except Exception as e:
            print(f"   ERROR during listing: {e}")
            import traceback

            traceback.print_exc()
            return False

        # Test 2: Read a single parquet file
        print("\n4. Testing Parquet read...")
        start = time.time()

        # Known file path from MinIO
        test_path = f"s3a://{bucket}/event_type=comment/year=2025/month=12/day=27/hour=07/"

        try:
            print(f"   Reading: {test_path}")
            df = spark.read.parquet(test_path)

            count = df.count()
            print(f"   Records read: {count}")
            print(f"   Schema: {df.schema.simpleString()[:100]}...")

            if count > 0:
                print("   Sample record:")
                df.show(1, truncate=50)

            print(f"   Parquet read completed in {time.time() - start:.2f}s")

        except Exception as e:
            print(f"   ERROR during parquet read: {e}")
            import traceback

            traceback.print_exc()
            return False

        # Test 3: Try glob pattern
        print("\n5. Testing glob pattern read...")
        start = time.time()

        try:
            glob_path = f"s3a://{bucket}/event_type=comment/year=2025/month=12/day=27/*/*.parquet"
            print(f"   Reading: {glob_path}")

            df = spark.read.parquet(glob_path)
            count = df.count()
            print(f"   Records via glob: {count}")
            print(f"   Glob read completed in {time.time() - start:.2f}s")

        except Exception as e:
            print(f"   WARNING during glob read: {e}")

        print("\n" + "=" * 60)
        print("ALL TESTS PASSED!")
        print("=" * 60)

        spark.stop()
        return True

    except ImportError as e:
        print(f"ERROR: PySpark not available - {e}")
        return False
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_s3a_connection()
    sys.exit(0 if success else 1)

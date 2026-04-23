"""
Batch Feature Pipeline - Kubeflow Pipelines Definition.

Orchestrates:
1. Submit SparkApplication for batch feature computation
2. Wait for Spark job completion
3. Run Feast materialization to online store
4. Validate features in online store

Usage:
    # Compile pipeline
    python pipelines/batch_feature_pipeline.py

    # Upload to Kubeflow
    kfp --endpoint http://localhost:8080 pipeline upload -p batch-feature-pipeline pipeline.yaml
"""

from datetime import datetime, timedelta

from kfp import dsl

# Pipeline Components


@dsl.component(
    base_image="python:3.10-slim",
    packages_to_install=["pyyaml", "kubernetes"],
)
def submit_spark_job(
    target_date: str,
    mode: str = "incremental",
    namespace: str = "default",  # Spark operator watches 'default'
) -> str:
    """
    Submit SparkApplication to Kubernetes.

    Returns the job name for status tracking.
    """
    from datetime import datetime

    from kubernetes import client, config

    # Load in-cluster config
    config.load_incluster_config()

    # Generate unique job name
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    job_name = f"batch-feature-job-{timestamp}"

    # Create SparkApplication manifest
    manifest = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": job_name,
            "namespace": namespace,
        },
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": "togather-ml/spark-batch-pipeline:latest",
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": "local:///opt/spark/work-dir/src/batch/batch_feature_job.py",
            "arguments": [
                "--target-date",
                target_date,
                "--mode",
                mode,
            ],
            "sparkVersion": "3.5.0",
            # S3A/MinIO configuration - credentials from K8s secrets via env vars
            "sparkConf": {
                "spark.hadoop.fs.s3a.endpoint": "http://minio.platform.svc.cluster.local:9000",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                # Use environment variables for credentials (injected from K8s secrets below)
                "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
                # Event logging disabled due to hadoop-aws S3Guard compatibility
                "spark.eventLog.enabled": "false",
            },
            "restartPolicy": {
                "type": "OnFailure",
                "onFailureRetries": 3,
                "onFailureRetryInterval": 30,
            },
            "driver": {
                "cores": 1,
                "memory": "1g",
                "serviceAccount": "spark",
                "env": [
                    {
                        "name": "AWS_ACCESS_KEY_ID",
                        "valueFrom": {
                            "secretKeyRef": {
                                "name": "spark-s3-credentials",
                                "key": "AWS_ACCESS_KEY_ID",
                            }
                        },
                    },
                    {
                        "name": "AWS_SECRET_ACCESS_KEY",
                        "valueFrom": {
                            "secretKeyRef": {
                                "name": "spark-s3-credentials",
                                "key": "AWS_SECRET_ACCESS_KEY",
                            }
                        },
                    },
                    {
                        "name": "MINIO_ENDPOINT_URL",
                        "value": "http://minio.platform.svc.cluster.local:9000",
                    },
                    {"name": "REDIS_HOST", "value": "redis.platform.svc.cluster.local"},
                    {"name": "REDIS_PORT", "value": "6379"},
                    {
                        "name": "REDIS_PASSWORD",
                        "valueFrom": {
                            "secretKeyRef": {
                                "name": "redis-secret",
                                "key": "REDIS_PASSWORD",
                            }
                        },
                    },
                ],
            },
            "executor": {
                "cores": 1,
                "memory": "2g",
                "instances": 2,
                "env": [
                    {
                        "name": "AWS_ACCESS_KEY_ID",
                        "valueFrom": {
                            "secretKeyRef": {
                                "name": "spark-s3-credentials",
                                "key": "AWS_ACCESS_KEY_ID",
                            }
                        },
                    },
                    {
                        "name": "AWS_SECRET_ACCESS_KEY",
                        "valueFrom": {
                            "secretKeyRef": {
                                "name": "spark-s3-credentials",
                                "key": "AWS_SECRET_ACCESS_KEY",
                            }
                        },
                    },
                ],
            },
        },
    }

    # Apply using Kubernetes API
    api = client.CustomObjectsApi()
    try:
        api.create_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=namespace,
            plural="sparkapplications",
            body=manifest,
        )
        print(f"Submitted SparkApplication: {job_name}")
    except client.ApiException as e:
        raise RuntimeError(f"Failed to submit SparkApplication: {e}") from e

    return job_name


@dsl.component(
    base_image="python:3.10-slim",
    packages_to_install=["kubernetes"],
)
def wait_for_spark_job(
    job_name: str,
    namespace: str = "default",  # Spark operator watches 'default'
    timeout_minutes: int = 60,
) -> str:
    """
    Wait for SparkApplication to complete.

    Returns final status (COMPLETED, FAILED).
    """
    import time

    from kubernetes import client, config

    # Load in-cluster config
    config.load_incluster_config()
    api = client.CustomObjectsApi()

    start_time = time.time()
    timeout_seconds = timeout_minutes * 60

    while True:
        # Check elapsed time
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise TimeoutError(f"Spark job {job_name} timed out after {timeout_minutes} minutes")

        # Get SparkApplication status
        try:
            spark_app = api.get_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=namespace,
                plural="sparkapplications",
                name=job_name,
            )
            status = spark_app.get("status", {}).get("applicationState", {}).get("state", "UNKNOWN")
            print(f"Job {job_name} status: {status}")

            if status == "COMPLETED":
                return "COMPLETED"
            elif status == "FAILED":
                error_msg = (
                    spark_app.get("status", {})
                    .get("applicationState", {})
                    .get("errorMessage", "Unknown error")
                )
                raise RuntimeError(f"Spark job failed: {error_msg}")

        except client.ApiException as e:
            print(f"Error getting job status: {e}")

        # Wait before next check
        time.sleep(30)


@dsl.component(
    base_image="python:3.10-slim",
    packages_to_install=["feast[redis]", "s3fs", "boto3"],
)
def materialize_feast_features(
    target_date: str,
    feast_repo_path: str = "/app/feast_repo",
    redis_host: str = "redis.platform.svc.cluster.local",
    redis_port: int = 6379,
    minio_endpoint: str = "http://minio.platform.svc.cluster.local:9000",
) -> str:
    """
    Materialize features from MinIO offline store to Redis online store.

    This syncs the latest features computed by Spark batch job
    to Redis for low-latency inference.
    """
    import os
    from datetime import datetime, timedelta

    from feast import FeatureStore

    # Set environment variables for Feast
    os.environ["REDIS_HOST"] = redis_host
    os.environ["REDIS_PORT"] = str(redis_port)
    os.environ["AWS_ENDPOINT_URL"] = minio_endpoint

    # Parse target date
    target = datetime.strptime(target_date, "%Y-%m-%d")
    start_date = target - timedelta(days=7)
    end_date = target + timedelta(days=1)

    print(f"Materializing features from {start_date.date()} to {end_date.date()}")
    print(f"Feast repo: {feast_repo_path}")
    print(f"Redis: {redis_host}:{redis_port}")
    print(f"MinIO: {minio_endpoint}")

    # Initialize Feast store
    store = FeatureStore(repo_path=feast_repo_path)

    # List feature views to materialize
    feature_views = store.list_feature_views()
    print(f"Feature views to materialize: {[fv.name for fv in feature_views]}")

    # Materialize to online store
    store.materialize(
        start_date=start_date,
        end_date=end_date,
    )

    # Log materialization summary
    for fv in feature_views:
        print(f"✓ Materialized: {fv.name}")

    print(f"Materialization complete for {len(feature_views)} feature views")
    return "SUCCESS"


@dsl.component(
    base_image="python:3.10-slim",
    packages_to_install=["redis"],
)
def validate_online_features(
    entity_type: str = "user",
    sample_size: int = 10,
    redis_host: str = "redis.platform.svc.cluster.local",
    redis_port: int = 6379,
    redis_password: str = "",  # From K8s secret via Kubeflow UI or default empty
) -> str:
    """
    Validate that features exist in Redis online store.
    """
    import redis

    client = redis.Redis(
        host=redis_host,
        port=redis_port,
        password=redis_password,
        decode_responses=True,
    )

    # Get sample keys
    pattern = f"feast:{entity_type}:*"
    keys = list(client.scan_iter(pattern, count=sample_size))

    if not keys:
        raise RuntimeError(f"No features found in Redis for pattern: {pattern}")

    # Check first few keys
    for key in keys[:5]:
        features = client.hgetall(key)
        print(f"Key: {key}, Features: {len(features)}")
        if not features:
            raise RuntimeError(f"Empty features for key: {key}")

    print(f"Validated {len(keys)} feature keys for entity: {entity_type}")
    return "VALIDATED"


# Pipeline Definition


@dsl.pipeline(
    name="Batch Feature Pipeline",
    description="Compute batch features from raw events and write to offline/online stores",
)
def batch_feature_pipeline(
    target_date: str = "",
    mode: str = "incremental",
):
    """
    Batch Feature Pipeline.

    Spark job reads raw events, computes features, and writes directly to:
    - MinIO (offline store) - for historical queries
    - Redis (online store) - for low-latency serving

    Args:
        target_date: Date for feature computation (YYYY-MM-DD). Defaults to yesterday.
        mode: Processing mode (incremental or full)
    """
    # Default to yesterday if not specified
    if not target_date:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    # Submit Spark job
    submit_task = submit_spark_job(
        target_date=target_date,
        mode=mode,
    )
    submit_task.set_caching_options(False)

    # Wait for completion
    wait_task = wait_for_spark_job(
        job_name=submit_task.output,
    )

    # Validate online features (Spark writes to Redis directly, no Feast materialize needed)
    validate_user = validate_online_features(entity_type="user")
    validate_user.after(wait_task)

    validate_exp = validate_online_features(entity_type="experience")
    validate_exp.after(wait_task)


# Compile Pipeline


if __name__ == "__main__":
    from kfp import compiler

    output_path = "pipelines/batch_feature_pipeline.yaml"
    compiler.Compiler().compile(
        pipeline_func=batch_feature_pipeline,
        package_path=output_path,
    )
    print(f"Pipeline compiled to: {output_path}")

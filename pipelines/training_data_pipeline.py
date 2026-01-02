"""
Training Data Pipeline - Kubeflow Pipelines Definition.

Orchestrates:
1. Submit SparkApplication for training data generation (Two-Tower & Ranking)
2. Wait for Spark job completion
3. Validate output in MinIO

Usage:
    # Compile pipeline
    python pipelines/training_data_pipeline.py
"""

from datetime import datetime, timedelta

from kfp import dsl

# Re-use components where possible, or define new ones if needed.
# Since submit_spark_job in batch_feature_pipeline hardcodes the mainApplicationFile,
# we need a modified version here.


@dsl.component(
    base_image="python:3.10-slim",
    packages_to_install=["pyyaml", "kubernetes"],
)
def submit_training_data_job(
    target_date: str,
    output_bucket: str = "dvc-data",
    feature_bucket: str = "raw-events",
    namespace: str = "default",
) -> str:
    """
    Submit SparkApplication for Training Data Generation.
    Returns: job_name
    """
    from datetime import datetime

    from kubernetes import client, config

    config.load_incluster_config()

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    job_name = f"training-data-job-{timestamp}"

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
            "imagePullPolicy": "IfNotPresent",  # Ensure this matches your dev setup (e.g. Never/IfNotPresent)
            "mainApplicationFile": "local:///opt/spark/work-dir/src/batch/generate_training_data_job.py",
            "arguments": [
                "--target-date",
                target_date,
                "--output-bucket",
                output_bucket,
                "--feature-bucket",
                feature_bucket,
            ],
            "sparkVersion": "3.5.0",
            # S3/MinIO Config
            "sparkConf": {
                "spark.hadoop.fs.s3a.endpoint": "http://minio-0.minio.platform.svc.cluster.local:9000",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                # Credentials (TODO: Secrets)
                "spark.hadoop.fs.s3a.access.key": "minioadmin",
                "spark.hadoop.fs.s3a.secret.key": "minioadmin123",
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
                    # Credentials env vars for consistency
                    {
                        "name": "MINIO_ENDPOINT_URL",
                        "value": "http://minio-0.minio.platform.svc.cluster.local:9000",
                    },
                    {"name": "AWS_ACCESS_KEY_ID", "value": "minioadmin"},
                    {"name": "AWS_SECRET_ACCESS_KEY", "value": "minioadmin123"},
                ],
            },
            "executor": {
                "cores": 1,
                "memory": "1g",
                "instances": 1,
                "env": [
                    {"name": "AWS_ACCESS_KEY_ID", "value": "minioadmin"},
                    {"name": "AWS_SECRET_ACCESS_KEY", "value": "minioadmin123"},
                ],
            },
        },
    }

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
        raise RuntimeError(f"Failed to submit SparkApplication: {e}")  # noqa: B904

    return job_name


@dsl.component(
    base_image="python:3.10-slim",
    packages_to_install=["kubernetes"],
)
def wait_for_spark_job(
    job_name: str,
    namespace: str = "default",
    timeout_minutes: int = 60,
) -> str:
    """Wait for SparkApplication to complete."""
    import time

    from kubernetes import client, config

    config.load_incluster_config()
    api = client.CustomObjectsApi()
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise TimeoutError(f"Spark job {job_name} timed out")

        try:
            spark_app = api.get_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=namespace,
                plural="sparkapplications",
                name=job_name,
            )
            state = spark_app.get("status", {}).get("applicationState", {}).get("state", "UNKNOWN")
            print(f"Job {job_name} State: {state}")

            if state == "COMPLETED":
                return "COMPLETED"
            elif state == "FAILED":
                err = (
                    spark_app.get("status", {})
                    .get("applicationState", {})
                    .get("errorMessage", "Unknown")
                )
                raise RuntimeError(f"Spark job failed: {err}")
        except client.ApiException as e:
            print(f"Error checking status: {e}")

        time.sleep(30)


@dsl.pipeline(
    name="Training Data Pipeline",
    description="Generate Two-Tower and Ranking training data from features",
)
def training_data_pipeline(
    target_date: str = "",
    output_bucket: str = "dvc-data",
):
    """
    Pipeline to generate training data.
    """
    if not target_date:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    submit_op = submit_training_data_job(target_date=target_date, output_bucket=output_bucket)
    submit_op.set_caching_options(False)

    wait_for_spark_job(job_name=submit_op.output)


if __name__ == "__main__":
    from kfp import compiler

    output_path = "pipelines/training_data_pipeline.yaml"
    compiler.Compiler().compile(
        pipeline_func=training_data_pipeline,
        package_path=output_path,
    )
    print(f"Pipeline compiled to: {output_path}")

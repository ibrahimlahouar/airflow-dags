from __future__ import annotations

import json
import time
from datetime import timedelta

import boto3
import requests
from botocore.client import Config

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# NOTE: This DAG is meant to be a simple end-to-end demo:
# API (HTTPS) -> raw JSON on MinIO (S3) -> SparkOperator job writes Iceberg tables via Nessie -> queryable by Trino.

# -----------------
# Cluster endpoints
# -----------------
MINIO_ENDPOINT = "http://minio.minio.svc:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

NESSIE_URI = "http://nessie.nessie.svc:19120/api/v1"

# We use a single bucket to keep the demo simple and aligned with Trino's Iceberg warehouse: s3://warehouse
BUCKET_WAREHOUSE = "warehouse"
RAW_PREFIX = "raw/crypto_prices"
SCRIPTS_PREFIX = "scripts"

# SparkOperator (SparkApplication CRD)
SPARKAPP_NAMESPACE = "spark"

# -----------------
# Airflow DAG config
# -----------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        verify=False,
    )


def _ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.create_bucket(Bucket=bucket)
    except Exception:
        # bucket may already exist
        pass


def fetch_crypto_prices(**context) -> dict:
    """Fetch a public API and store raw JSON in MinIO."""

    # Using CoinGecko API (free, no auth required)
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page=20"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    # Wrap response in dict format similar to old API
    import time
    payload = {"data": resp.json(), "timestamp": int(time.time() * 1000)}

    s3 = _minio_client()
    _ensure_bucket(s3, BUCKET_WAREHOUSE)

    ts = context["ts_nodash"]
    raw_key = f"{RAW_PREFIX}/{ts}/data.json"
    s3.put_object(Bucket=BUCKET_WAREHOUSE, Key=raw_key, Body=json.dumps(payload).encode("utf-8"))

    # Upload Spark script to the same bucket so Spark can load it via s3a://
    script_key = f"{SCRIPTS_PREFIX}/crypto_to_iceberg.py"
    s3.put_object(Bucket=BUCKET_WAREHOUSE, Key=script_key, Body=_SPARK_SCRIPT.encode("utf-8"))

    return {
        "raw_s3a_path": f"s3a://{BUCKET_WAREHOUSE}/{raw_key}",
        "script_s3a_path": f"s3a://{BUCKET_WAREHOUSE}/{script_key}",
        "ts": ts,
    }


def submit_spark_application(**context) -> str:
    """Create a SparkApplication in the spark namespace and wait for completion."""

    # Kubernetes client is included with Airflow KubernetesExecutor images.
    from kubernetes import client, config

    xcom = context["ti"].xcom_pull(task_ids="fetch_crypto_prices")
    raw_path = xcom["raw_s3a_path"]
    script_path = xcom["script_s3a_path"]
    ts = xcom["ts"]

    app_name = f"crypto-iceberg-{ts.lower()}".replace("_", "-")

    config.load_incluster_config()
    api = client.CustomObjectsApi()

    # SparkApplication spec (SparkOperator v1beta2)
    spark_app = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": app_name, "namespace": SPARKAPP_NAMESPACE},
        "spec": {
            "type": "Python",
            "mode": "cluster",
            "image": "apache/spark-py:v3.5.0",
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": script_path,
            "arguments": [raw_path],
            "sparkVersion": "3.5.0",
            "restartPolicy": {"type": "Never"},
            "deps": {
                "packages": [
                    # Iceberg + Nessie extensions for Spark 3.5 (Scala 2.12)
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
                    "org.projectnessie:nessie-spark-extensions-3.5_2.12:0.87.0",
                    # S3A / AWS SDK v1 bundle (common with Hadoop 3.3.x)
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                ]
            },
            "sparkConf": {
                # Nessie + Iceberg
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
                "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
                "spark.sql.catalog.nessie.uri": NESSIE_URI,
                "spark.sql.catalog.nessie.ref": "main",
                "spark.sql.catalog.nessie.authentication.type": "NONE",
                "spark.sql.catalog.nessie.warehouse": f"s3a://{BUCKET_WAREHOUSE}/",
                # MinIO (S3A)
                "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
                "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            },
            "driver": {
                "cores": 1,
                "coreLimit": "1200m",
                "memory": "1024m",
                "serviceAccount": "spark",  # default SA in spark namespace is typically fine
            },
            "executor": {
                "instances": 1,
                "cores": 1,
                "memory": "1024m",
            },
        },
    }

    # Best-effort cleanup if the name already exists
    try:
        api.delete_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=SPARKAPP_NAMESPACE,
            plural="sparkapplications",
            name=app_name,
        )
        time.sleep(3)
    except Exception:
        pass

    api.create_namespaced_custom_object(
        group="sparkoperator.k8s.io",
        version="v1beta2",
        namespace=SPARKAPP_NAMESPACE,
        plural="sparkapplications",
        body=spark_app,
    )

    # Wait for completion
    timeout_s = 30 * 60
    start = time.time()
    while True:
        obj = api.get_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=SPARKAPP_NAMESPACE,
            plural="sparkapplications",
            name=app_name,
        )
        app_state = (((obj.get("status") or {}).get("applicationState") or {}).get("state") or "").upper()
        if app_state in {"COMPLETED", "FAILED"}:
            if app_state == "FAILED":
                raise RuntimeError(f"SparkApplication {app_name} failed: {obj.get('status')}")
            break
        if time.time() - start > timeout_s:
            raise TimeoutError(f"Timed out waiting for SparkApplication {app_name} to complete")
        time.sleep(10)

    return app_name


_SPARK_SCRIPT = """\
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp
import sys


def main(raw_input_path: str) -> None:
    spark = SparkSession.builder.appName("crypto_to_iceberg").getOrCreate()

    df = spark.read.json(raw_input_path)

    # CoinGecko shape: { data: [{id, symbol, name, current_price, ...}], timestamp: <int> }
    exploded = (
        df.select(explode(col("data")).alias("coin"), col("timestamp").alias("api_timestamp"))
        .select(
            col("coin.id").alias("id"),
            col("coin.symbol").alias("symbol"),
            col("coin.name").alias("name"),
            col("coin.current_price").cast("double").alias("price_usd"),
            col("api_timestamp").cast("long").alias("api_timestamp"),
            current_timestamp().alias("ingestion_time"),
        )
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.crypto")
    spark.sql(
        "CREATE TABLE IF NOT EXISTS nessie.crypto.crypto_prices ("
        "id STRING, symbol STRING, name STRING, price_usd DOUBLE, "
        "api_timestamp BIGINT, ingestion_time TIMESTAMP) USING iceberg"
    )

    exploded.writeTo("nessie.crypto.crypto_prices").append()

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1])
"""


with DAG(
    dag_id="crypto_iceberg_pipeline",
    default_args=DEFAULT_ARGS,
    description="Demo: API -> MinIO (raw) -> Spark(Iceberg+Nessie) -> Trino",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["demo", "crypto", "minio", "iceberg", "nessie", "spark"],
) as dag:
    fetch = PythonOperator(
        task_id="fetch_crypto_prices",
        python_callable=fetch_crypto_prices,
    )

    spark_ingest = PythonOperator(
        task_id="spark_ingest_iceberg",
        python_callable=submit_spark_application,
    )

    fetch >> spark_ingest

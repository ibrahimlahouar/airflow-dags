from __future__ import annotations

import json
import logging
import time
from datetime import timedelta

import boto3
import requests
from botocore.client import Config

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# OpenMetadata Lineage Operator
try:
    from airflow_provider_openmetadata.lineage.operator import OpenMetadataLineageOperator
    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
        OpenMetadataConnection,
    )
    from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
        OpenMetadataJWTClientConfig,
    )
    OPENMETADATA_AVAILABLE = True
except ImportError:
    OPENMETADATA_AVAILABLE = False

# -----------------
# Logging Configuration
# -----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# NOTE: Data Platform Pipeline
# API (HTTPS) -> raw JSON on MinIO (S3) -> SparkOperator job writes Iceberg tables via Nessie -> queryable by Trino.
#
# Data is organized by date:
#   - Raw: s3://warehouse/raw/crypto_prices/dt=YYYY-MM-DD/data.json
#   - Iceberg: Partitioned by ingestion_date column
#
# For bigger data volumes, consider: GitHub Archive (~3-5M events/day), Wikipedia Pageviews, OpenSky Network

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

# OpenMetadata Configuration
OPENMETADATA_HOST = "http://openmetadata.openmetadata.svc:8585/api"
OPENMETADATA_SERVICE_NAME = "airflow"  # Pipeline service name in OpenMetadata

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


def _get_openmetadata_config():
    """Build OpenMetadata connection config for lineage operator."""
    if not OPENMETADATA_AVAILABLE:
        return None
    
    try:
        jwt_token = Variable.get("openmetadata_jwt_token")
    except KeyError:
        logger.warning("Variable 'openmetadata_jwt_token' not found. OpenMetadata lineage disabled.")
        return None
    
    return OpenMetadataConnection(
        hostPort=OPENMETADATA_HOST,
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=jwt_token),
    )


def _ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.create_bucket(Bucket=bucket)
    except Exception:
        # bucket may already exist
        pass


def fetch_crypto_prices(**context) -> dict:
    """Fetch a public API and store raw JSON in MinIO."""

    # Fetch data from CoinGecko (attempting to fetch ~1000 coins to increase volume)
    # Note: Getting 100MB from this free API is not feasible (would require ~100k+ calls).
    # We will fetch 4 pages of 250 items to demonstrate a larger dataset (~1000 rows).
    all_coins = []
    
    # Imports are at top level, but ensuring time is available if not
    import time

    base_url = "https://api.coingecko.com/api/v3/coins/markets"
    
    for page in range(1, 5):
        try:
            params = {
                "vs_currency": "usd",
                "per_page": 250,
                "page": page
            }
            resp = requests.get(base_url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            if not data:
                break
                
            all_coins.extend(data)
            logger.info(f"Fetched page {page}: {len(data)} coins")
            
            # Respect rate limits for free tier
            time.sleep(1.5)
            
        except Exception as e:
            logger.warning(f"Stopping fetch at page {page} due to error: {e}")
            break

    # Wrap response in dict format 
    payload = {"data": all_coins, "timestamp": int(time.time() * 1000)}

    s3 = _minio_client()
    _ensure_bucket(s3, BUCKET_WAREHOUSE)

    # Extract execution date for partitioning (format: YYYY-MM-DD)
    execution_date = context["ds"]  # e.g., "2026-01-04"
    ts = context["ts_nodash"]
    
    # Organize raw data by date: raw/crypto_prices/dt=YYYY-MM-DD/data.json
    raw_key = f"{RAW_PREFIX}/dt={execution_date}/data.json"
    s3.put_object(Bucket=BUCKET_WAREHOUSE, Key=raw_key, Body=json.dumps(payload).encode("utf-8"))
    logger.info(f"Saved raw data to s3://{BUCKET_WAREHOUSE}/{raw_key}")

    # Upload Spark script to the same bucket so Spark can load it via s3a://
    script_key = f"{SCRIPTS_PREFIX}/crypto_to_iceberg.py"
    s3.put_object(Bucket=BUCKET_WAREHOUSE, Key=script_key, Body=_SPARK_SCRIPT.encode("utf-8"))

    return {
        "raw_s3a_path": f"s3a://{BUCKET_WAREHOUSE}/{raw_key}",
        "script_s3a_path": f"s3a://{BUCKET_WAREHOUSE}/{script_key}",
        "ts": ts,
        "execution_date": execution_date,  # Pass date for Iceberg partitioning
    }


def _build_spark_conf(om_jwt_token: str | None, app_name: str) -> dict:
    """Build Spark configuration with optional OpenMetadata integration."""
    spark_conf = {
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
        # Ivy Cache for dependency resolution
        "spark.jars.ivy": "/tmp/ivy2",
    }

    # Note: Spark-level lineage disabled. Using Airflow OpenMetadataLineageOperator instead.
    _ = om_jwt_token, app_name  # Suppress unused warnings
    
    return spark_conf


def submit_spark_application(**context) -> str:
    """Create a SparkApplication in the spark namespace and wait for completion."""

    # Kubernetes client is included with Airflow KubernetesExecutor images.
    from kubernetes import client, config

    xcom = context["ti"].xcom_pull(task_ids="fetch_crypto_prices")
    raw_path = xcom["raw_s3a_path"]
    script_path = xcom["script_s3a_path"]
    ts = xcom["ts"]
    execution_date = xcom["execution_date"]  # For Iceberg partitioning

    app_name = f"crypto-iceberg-{ts.lower()}".replace("_", "-")

    config.load_incluster_config()
    api = client.CustomObjectsApi()

    # SparkApplication spec (SparkOperator v1beta2)
    # Retrieve OpenMetadata JWT Token from Airflow Variable
    try:
        om_jwt_token = Variable.get("openmetadata_jwt_token")
        logger.info("OpenMetadata JWT token retrieved from Airflow Variable")
    except KeyError:
        logger.warning(
            "Variable 'openmetadata_jwt_token' not found. "
            "OpenMetadata lineage will not be available for this Spark job."
        )
        om_jwt_token = None

    spark_app = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": app_name, "namespace": SPARKAPP_NAMESPACE},
        "spec": {
            "type": "Python",
            "mode": "cluster",
            "image": "apache/spark:3.5.0",
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": script_path,
            "arguments": [raw_path, execution_date],  # Pass date for partitioning
            "sparkVersion": "3.5.0",
            "restartPolicy": {"type": "Never"},
            "timeToLiveSeconds": 60,  # Auto-cleanup pods 60s after job completion
            "volumes": [{"name": "ivy-cache", "emptyDir": {}}],
            "deps": {
                "packages": [
                    # Iceberg + Nessie extensions for Spark 3.5 (Scala 2.12)
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
                    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.104.5",
                    # S3A / AWS SDK v1 bundle (common with Hadoop 3.3.x)
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                ]
            },
            "sparkConf": _build_spark_conf(om_jwt_token, app_name),
            "driver": {
                "cores": 1,
                "coreLimit": "1200m",
                "memory": "1024m",
                "serviceAccount": "spark",
                "securityContext": {"runAsUser": 0},
                "volumeMounts": [{"name": "ivy-cache", "mountPath": "/tmp/ivy2"}],
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
    core_api = client.CoreV1Api()
    last_state = None
    final_status = None
    
    logger.info(f"Waiting for SparkApplication {app_name} to complete (timeout: {timeout_s}s)...")
    
    try:
        while True:
            obj = api.get_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=SPARKAPP_NAMESPACE,
                plural="sparkapplications",
                name=app_name,
            )
            app_state = (((obj.get("status") or {}).get("applicationState") or {}).get("state") or "").upper()
            
            # Log state only when it changes
            if app_state != last_state:
                elapsed = int(time.time() - start)
                logger.info(f"SparkApplication {app_name} state: {app_state or 'UNKNOWN'} (elapsed: {elapsed}s)")
                last_state = app_state
            
            if app_state in {"COMPLETED", "FAILED"}:
                final_status = obj.get("status")
                if app_state == "FAILED":
                    logger.error(f"SparkApplication {app_name} FAILED. Full status: {json.dumps(final_status, indent=2)}")
                    raise RuntimeError(f"SparkApplication {app_name} failed: {final_status}")
                logger.info(f"SparkApplication {app_name} completed successfully")
                break
            
            if time.time() - start > timeout_s:
                final_status = obj.get("status")
                logger.error(f"SparkApplication {app_name} TIMEOUT. Full status: {json.dumps(final_status, indent=2)}")
                raise TimeoutError(f"Timed out waiting for SparkApplication {app_name} to complete")
            
            time.sleep(10)

    finally:
        # Fetch and save driver logs to Airflow worker /tmp and MinIO
        _save_driver_logs(core_api, app_name)

    return app_name


def _save_driver_logs(core_api, app_name: str) -> None:
    """Fetch driver logs, save locally and upload to MinIO for persistence."""
    driver_pod_name = f"{app_name}-driver"
    
    try:
        logger.info(f"Fetching logs for pod {driver_pod_name} in namespace {SPARKAPP_NAMESPACE}...")
        logs = core_api.read_namespaced_pod_log(name=driver_pod_name, namespace=SPARKAPP_NAMESPACE)
        
        # Save locally to /tmp
        log_path = f"/tmp/{app_name}.log"
        with open(log_path, "w") as f:
            f.write(logs)
        logger.info(f"Saved Spark Driver logs to: {log_path}")
        
        # Log last 100 lines to Airflow logs for quick debugging
        log_lines = logs.splitlines()
        last_100_lines = log_lines[-100:] if len(log_lines) > 100 else log_lines
        logger.info(f"=== Last {len(last_100_lines)} lines of Spark Driver logs ===")
        for line in last_100_lines:
            logger.info(f"[DRIVER] {line}")
        logger.info("=== End of Spark Driver logs ===")
        
        # Upload to MinIO for persistence
        try:
            s3 = _minio_client()
            _ensure_bucket(s3, BUCKET_WAREHOUSE)
            s3_log_key = f"airflow-logs/spark/{app_name}.log"
            s3.put_object(
                Bucket=BUCKET_WAREHOUSE,
                Key=s3_log_key,
                Body=logs.encode("utf-8"),
            )
            logger.info(f"Uploaded Spark Driver logs to s3://{BUCKET_WAREHOUSE}/{s3_log_key}")
        except Exception as upload_err:
            logger.warning(f"Failed to upload driver logs to MinIO: {upload_err}")
            
    except Exception as e:
        logger.warning(f"Could not fetch/save driver logs: {e}")


_SPARK_SCRIPT = """\
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp, lit, to_date
import sys


def main(raw_input_path: str, ingestion_date: str) -> None:
    \"\"\"
    Process crypto prices and write to Iceberg table partitioned by ingestion_date.
    
    Args:
        raw_input_path: S3 path to raw JSON data
        ingestion_date: Date string (YYYY-MM-DD) for partitioning
    \"\"\"
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
            col("coin.market_cap").cast("double").alias("market_cap"),
            col("coin.total_volume").cast("double").alias("volume_24h"),
            col("api_timestamp").cast("long").alias("api_timestamp"),
            current_timestamp().alias("ingestion_time"),
            to_date(lit(ingestion_date)).alias("ingestion_date"),  # Partition column
        )
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.crypto")
    
    # Create table with partitioning by ingestion_date
    spark.sql(\"\"\"
        CREATE TABLE IF NOT EXISTS nessie.crypto.crypto_prices (
            id STRING,
            symbol STRING,
            name STRING,
            price_usd DOUBLE,
            market_cap DOUBLE,
            volume_24h DOUBLE,
            api_timestamp BIGINT,
            ingestion_time TIMESTAMP,
            ingestion_date DATE
        ) USING iceberg
        PARTITIONED BY (ingestion_date)
    \"\"\")

    exploded.writeTo("nessie.crypto.crypto_prices").append()
    
    print(f"Successfully wrote {exploded.count()} records for date {ingestion_date}")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: crypto_to_iceberg.py <raw_input_path> <ingestion_date>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
"""


with DAG(
    dag_id="crypto_iceberg_pipeline",
    default_args=DEFAULT_ARGS,
    description="Demo: API -> MinIO (raw) -> Spark(Iceberg+Nessie) -> Trino",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["demo", "crypto", "minio", "iceberg", "nessie", "spark", "openmetadata"],
) as dag:
    fetch = PythonOperator(
        task_id="fetch_crypto_prices",
        python_callable=fetch_crypto_prices,
    )

    spark_ingest = PythonOperator(
        task_id="spark_ingest_iceberg",
        python_callable=submit_spark_application,
    )

    # OpenMetadata Lineage - publish pipeline metadata after completion
    if OPENMETADATA_AVAILABLE:
        om_config = _get_openmetadata_config()
        if om_config:
            publish_lineage = OpenMetadataLineageOperator(
                task_id="publish_openmetadata_lineage",
                server_config=om_config,
                service_name=OPENMETADATA_SERVICE_NAME,
                only_keep_dag_lineage=False,  # Keep task-level lineage too
                trigger_rule="all_done",  # Run even if upstream fails
            )
            fetch >> spark_ingest >> publish_lineage
        else:
            logger.warning("OpenMetadata config not available - lineage task skipped")
            fetch >> spark_ingest
    else:
        logger.info("OpenMetadata provider not installed - lineage task skipped")
        fetch >> spark_ingest

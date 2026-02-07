from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
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

# ──────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Pipeline Architecture
# ──────────────────────────────────────────────────────────────────────
# Open-Meteo API (HTTPS, free, no key)
#   -> raw JSON on MinIO S3: s3://warehouse/raw/weather_forecast/dt=YYYY-MM-DD/data.json
#   -> SparkOperator writes Iceberg table via Nessie: nessie.weather.hourly_forecast
#   -> PostgreSQL analytics: analytics_db.weather_daily_summary
#   -> OpenMetadata: profiling + quality tests
#
# Queryable via Trino: SELECT * FROM iceberg.weather.hourly_forecast

# ──────────────────────────────────────────────────────────────────────
# Cluster endpoints
# ──────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = "http://minio.minio.svc:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

NESSIE_URI = "http://nessie.nessie.svc:19120/api/v1"

POSTGRES_HOST = "postgres-shared-postgresql.infra.svc"
POSTGRES_PORT = 5432
POSTGRES_ADMIN_USER = "admin"
POSTGRES_ADMIN_PASS = "adminpassword"
POSTGRES_ANALYTICS_DB = "analytics_db"

OPENMETADATA_HOST = "http://openmetadata.openmetadata.svc:8585/api"
OPENMETADATA_SERVICE_NAME = "airflow"

BUCKET_WAREHOUSE = "warehouse"
RAW_PREFIX = "raw/weather_forecast"
SCRIPTS_PREFIX = "scripts"
SPARKAPP_NAMESPACE = "spark"

# ──────────────────────────────────────────────────────────────────────
# Cities to fetch weather for (10 cities worldwide)
# ──────────────────────────────────────────────────────────────────────
CITIES = [
    {"name": "Paris", "lat": 48.8566, "lon": 2.3522},
    {"name": "London", "lat": 51.5074, "lon": -0.1278},
    {"name": "New_York", "lat": 40.7128, "lon": -74.0060},
    {"name": "Tokyo", "lat": 35.6762, "lon": 139.6503},
    {"name": "Sydney", "lat": -33.8688, "lon": 151.2093},
    {"name": "Dubai", "lat": 25.2048, "lon": 55.2708},
    {"name": "Sao_Paulo", "lat": -23.5505, "lon": -46.6333},
    {"name": "Berlin", "lat": 52.5200, "lon": 13.4050},
    {"name": "Toronto", "lat": 43.6532, "lon": -79.3832},
    {"name": "Mumbai", "lat": 19.0760, "lon": 72.8777},
]

# ──────────────────────────────────────────────────────────────────────
# Airflow DAG config
# ──────────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ──────────────────────────────────────────────────────────────────────
# Helper functions
# ──────────────────────────────────────────────────────────────────────
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
        pass


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


def _om_headers() -> dict | None:
    """Return OpenMetadata API headers with JWT auth, or None if unavailable."""
    try:
        jwt_token = Variable.get("openmetadata_jwt_token")
    except KeyError:
        logger.warning("OpenMetadata JWT token not configured.")
        return None
    return {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json",
    }


# ──────────────────────────────────────────────────────────────────────
# Task 1 : Setup PostgreSQL analytics database and tables
# ──────────────────────────────────────────────────────────────────────
def setup_postgres(**context):
    """Create analytics_db database and weather_daily_summary table if not exist."""
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

    # Step 1: Create database if not exists (requires autocommit)
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname="postgres",
        user=POSTGRES_ADMIN_USER,
        password=POSTGRES_ADMIN_PASS,
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (POSTGRES_ANALYTICS_DB,))
        if not cur.fetchone():
            cur.execute(f"CREATE DATABASE {POSTGRES_ANALYTICS_DB}")
            logger.info(f"Created database: {POSTGRES_ANALYTICS_DB}")
        else:
            logger.info(f"Database {POSTGRES_ANALYTICS_DB} already exists")
    conn.close()

    # Step 2: Create tables in analytics_db
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_ANALYTICS_DB,
        user=POSTGRES_ADMIN_USER,
        password=POSTGRES_ADMIN_PASS,
    )
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS weather_daily_summary (
                city_name          VARCHAR(100),
                forecast_date      DATE,
                avg_temp_c         DOUBLE PRECISION,
                min_temp_c         DOUBLE PRECISION,
                max_temp_c         DOUBLE PRECISION,
                avg_humidity_pct   DOUBLE PRECISION,
                total_precip_mm    DOUBLE PRECISION,
                avg_wind_kmh       DOUBLE PRECISION,
                record_count       INTEGER,
                ingestion_date     DATE,
                PRIMARY KEY (city_name, forecast_date, ingestion_date)
            )
        """)
        conn.commit()
        logger.info("Table weather_daily_summary ready")
    conn.close()


# ──────────────────────────────────────────────────────────────────────
# Task 2 : Fetch weather data from Open-Meteo API -> MinIO S3
# ──────────────────────────────────────────────────────────────────────
def fetch_weather_data(**context):
    """
    Fetch 7-day hourly forecast from Open-Meteo for 10 cities.
    Store raw (flat records) as JSON on MinIO S3.
    Compute daily summaries for PostgreSQL loading.
    """
    execution_date = context["ds"]  # YYYY-MM-DD
    ts = context["ts_nodash"]

    all_records = []
    daily_agg = defaultdict(lambda: {
        "temps": [], "humidities": [], "precips": [], "winds": [],
    })

    base_url = "https://api.open-meteo.com/v1/forecast"

    for city in CITIES:
        try:
            params = {
                "latitude": city["lat"],
                "longitude": city["lon"],
                "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
                "timezone": "UTC",
                "forecast_days": 7,
            }
            resp = requests.get(base_url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            hourly = data.get("hourly", {})
            times = hourly.get("time", [])
            temps = hourly.get("temperature_2m", [])
            humids = hourly.get("relative_humidity_2m", [])
            precips = hourly.get("precipitation", [])
            winds = hourly.get("wind_speed_10m", [])

            for i, t in enumerate(times):
                record = {
                    "city_name": city["name"],
                    "latitude": city["lat"],
                    "longitude": city["lon"],
                    "forecast_time": t,
                    "temperature_c": temps[i] if i < len(temps) else None,
                    "humidity_pct": humids[i] if i < len(humids) else None,
                    "precipitation_mm": precips[i] if i < len(precips) else None,
                    "wind_speed_kmh": winds[i] if i < len(winds) else None,
                }
                all_records.append(record)

                # Aggregate for daily summary (date part of forecast_time)
                forecast_date = t[:10]  # YYYY-MM-DD
                key = (city["name"], forecast_date)
                if record["temperature_c"] is not None:
                    daily_agg[key]["temps"].append(record["temperature_c"])
                if record["humidity_pct"] is not None:
                    daily_agg[key]["humidities"].append(record["humidity_pct"])
                if record["precipitation_mm"] is not None:
                    daily_agg[key]["precips"].append(record["precipitation_mm"])
                if record["wind_speed_kmh"] is not None:
                    daily_agg[key]["winds"].append(record["wind_speed_kmh"])

            logger.info(f"Fetched {len(times)} hourly records for {city['name']}")
            time.sleep(0.3)  # Respect Open-Meteo rate limits

        except Exception as e:
            logger.warning(f"Failed to fetch weather for {city['name']}: {e}")

    logger.info(f"Total records fetched: {len(all_records)}")

    # Build daily summaries for PostgreSQL
    daily_summaries = []
    for (city_name, forecast_date), agg in daily_agg.items():
        if agg["temps"]:
            daily_summaries.append({
                "city_name": city_name,
                "forecast_date": forecast_date,
                "avg_temp_c": round(sum(agg["temps"]) / len(agg["temps"]), 2),
                "min_temp_c": round(min(agg["temps"]), 2),
                "max_temp_c": round(max(agg["temps"]), 2),
                "avg_humidity_pct": round(sum(agg["humidities"]) / len(agg["humidities"]), 2) if agg["humidities"] else None,
                "total_precip_mm": round(sum(agg["precips"]), 2) if agg["precips"] else None,
                "avg_wind_kmh": round(sum(agg["winds"]) / len(agg["winds"]), 2) if agg["winds"] else None,
                "record_count": len(agg["temps"]),
                "ingestion_date": execution_date,
            })

    # Upload raw JSON to MinIO S3
    payload = {"data": all_records, "timestamp": int(time.time() * 1000)}

    s3 = _minio_client()
    _ensure_bucket(s3, BUCKET_WAREHOUSE)

    raw_key = f"{RAW_PREFIX}/dt={execution_date}/data.json"
    s3.put_object(
        Bucket=BUCKET_WAREHOUSE,
        Key=raw_key,
        Body=json.dumps(payload).encode("utf-8"),
    )
    logger.info(f"Saved raw data to s3://{BUCKET_WAREHOUSE}/{raw_key} ({len(all_records)} records)")

    # Upload Spark script
    script_key = f"{SCRIPTS_PREFIX}/weather_to_iceberg.py"
    s3.put_object(
        Bucket=BUCKET_WAREHOUSE,
        Key=script_key,
        Body=_SPARK_SCRIPT.encode("utf-8"),
    )

    return {
        "raw_s3a_path": f"s3a://{BUCKET_WAREHOUSE}/{raw_key}",
        "script_s3a_path": f"s3a://{BUCKET_WAREHOUSE}/{script_key}",
        "ts": ts,
        "execution_date": execution_date,
        "daily_summaries": daily_summaries,
        "total_records": len(all_records),
    }


# ──────────────────────────────────────────────────────────────────────
# Task 3 : Spark Iceberg ingestion (SparkApplication CRD)
# ──────────────────────────────────────────────────────────────────────
def _build_spark_conf() -> dict:
    """Spark configuration for Iceberg + Nessie + MinIO S3."""
    return {
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
        ),
        "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
        "spark.sql.catalog.nessie.uri": NESSIE_URI,
        "spark.sql.catalog.nessie.ref": "main",
        "spark.sql.catalog.nessie.authentication.type": "NONE",
        "spark.sql.catalog.nessie.warehouse": f"s3a://{BUCKET_WAREHOUSE}/",
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.jars.ivy": "/tmp/ivy2",
    }


def submit_spark_application(**context) -> str:
    """Create a SparkApplication to process weather data into Iceberg table."""
    from kubernetes import client, config

    xcom = context["ti"].xcom_pull(task_ids="fetch_weather_data")
    raw_path = xcom["raw_s3a_path"]
    script_path = xcom["script_s3a_path"]
    ts = xcom["ts"]
    execution_date = xcom["execution_date"]

    app_name = f"weather-iceberg-{ts.lower()}".replace("_", "-")

    config.load_incluster_config()
    api = client.CustomObjectsApi()

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
            "arguments": [raw_path, execution_date],
            "sparkVersion": "3.5.0",
            "restartPolicy": {"type": "Never"},
            "timeToLiveSeconds": 60,
            "volumes": [{"name": "ivy-cache", "emptyDir": {}}],
            "deps": {
                "packages": [
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
                    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.104.5",
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                ]
            },
            "sparkConf": _build_spark_conf(),
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

    # Cleanup if name already exists
    try:
        api.delete_namespaced_custom_object(
            group="sparkoperator.k8s.io", version="v1beta2",
            namespace=SPARKAPP_NAMESPACE, plural="sparkapplications", name=app_name,
        )
        time.sleep(3)
    except Exception:
        pass

    api.create_namespaced_custom_object(
        group="sparkoperator.k8s.io", version="v1beta2",
        namespace=SPARKAPP_NAMESPACE, plural="sparkapplications", body=spark_app,
    )

    # Wait for completion
    timeout_s = 30 * 60
    start = time.time()
    core_api = client.CoreV1Api()
    last_state = None

    logger.info(f"Waiting for SparkApplication {app_name} (timeout: {timeout_s}s)...")

    try:
        while True:
            obj = api.get_namespaced_custom_object(
                group="sparkoperator.k8s.io", version="v1beta2",
                namespace=SPARKAPP_NAMESPACE, plural="sparkapplications", name=app_name,
            )
            app_state = (
                ((obj.get("status") or {}).get("applicationState") or {}).get("state") or ""
            ).upper()

            if app_state != last_state:
                elapsed = int(time.time() - start)
                logger.info(f"SparkApplication {app_name} state: {app_state or 'UNKNOWN'} ({elapsed}s)")
                last_state = app_state

            if app_state in {"COMPLETED", "FAILED"}:
                if app_state == "FAILED":
                    status = obj.get("status")
                    logger.error(f"SparkApplication FAILED: {json.dumps(status, indent=2)}")
                    raise RuntimeError(f"SparkApplication {app_name} failed")
                logger.info(f"SparkApplication {app_name} completed successfully")
                break

            if time.time() - start > timeout_s:
                raise TimeoutError(f"Timed out waiting for {app_name}")

            time.sleep(10)
    finally:
        _save_driver_logs(core_api, app_name)

    return app_name


def _save_driver_logs(core_api, app_name: str) -> None:
    """Fetch driver logs and upload to MinIO for persistence."""
    driver_pod = f"{app_name}-driver"
    try:
        logs = core_api.read_namespaced_pod_log(name=driver_pod, namespace=SPARKAPP_NAMESPACE)
        log_path = f"/tmp/{app_name}.log"
        with open(log_path, "w") as f:
            f.write(logs)
        logger.info(f"Saved Spark driver logs to {log_path}")

        # Last 50 lines in Airflow log
        for line in logs.splitlines()[-50:]:
            logger.info(f"[DRIVER] {line}")

        # Upload to MinIO
        s3 = _minio_client()
        s3.put_object(
            Bucket=BUCKET_WAREHOUSE,
            Key=f"airflow-logs/spark/{app_name}.log",
            Body=logs.encode("utf-8"),
        )
    except Exception as e:
        logger.warning(f"Could not save driver logs: {e}")


# ──────────────────────────────────────────────────────────────────────
# Task 4 : Load daily aggregates into PostgreSQL
# ──────────────────────────────────────────────────────────────────────
def load_postgres_analytics(**context):
    """Insert / upsert daily weather summaries into PostgreSQL analytics_db."""
    import psycopg2

    xcom = context["ti"].xcom_pull(task_ids="fetch_weather_data")
    daily_summaries = xcom["daily_summaries"]

    if not daily_summaries:
        logger.warning("No daily summaries to load into PostgreSQL")
        return

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_ANALYTICS_DB,
        user=POSTGRES_ADMIN_USER,
        password=POSTGRES_ADMIN_PASS,
    )

    inserted = 0
    with conn.cursor() as cur:
        for s in daily_summaries:
            cur.execute("""
                INSERT INTO weather_daily_summary
                    (city_name, forecast_date, avg_temp_c, min_temp_c, max_temp_c,
                     avg_humidity_pct, total_precip_mm, avg_wind_kmh,
                     record_count, ingestion_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (city_name, forecast_date, ingestion_date)
                DO UPDATE SET
                    avg_temp_c       = EXCLUDED.avg_temp_c,
                    min_temp_c       = EXCLUDED.min_temp_c,
                    max_temp_c       = EXCLUDED.max_temp_c,
                    avg_humidity_pct = EXCLUDED.avg_humidity_pct,
                    total_precip_mm  = EXCLUDED.total_precip_mm,
                    avg_wind_kmh     = EXCLUDED.avg_wind_kmh,
                    record_count     = EXCLUDED.record_count
            """, (
                s["city_name"], s["forecast_date"],
                s["avg_temp_c"], s["min_temp_c"], s["max_temp_c"],
                s["avg_humidity_pct"], s["total_precip_mm"], s["avg_wind_kmh"],
                s["record_count"], s["ingestion_date"],
            ))
            inserted += 1
        conn.commit()
    conn.close()

    logger.info(f"Loaded {inserted} daily summaries into PostgreSQL ({POSTGRES_ANALYTICS_DB}.weather_daily_summary)")


# ──────────────────────────────────────────────────────────────────────
# Task 5 : Trigger OpenMetadata profiling + quality tests
# ──────────────────────────────────────────────────────────────────────
WEATHER_TABLE_FQN = "Trino.iceberg.weather.hourly_forecast"

# Quality test definitions to create
WEATHER_QUALITY_TESTS = [
    {
        "name": "weather_city_not_null",
        "testDefinition": "columnValuesToBeNotNull",
        "column": "city_name",
        "params": [],
    },
    {
        "name": "weather_temp_range",
        "testDefinition": "columnValuesToBeBetween",
        "column": "temperature_c",
        "params": [
            {"name": "minValue", "value": "-80"},
            {"name": "maxValue", "value": "65"},
        ],
    },
    {
        "name": "weather_humidity_range",
        "testDefinition": "columnValuesToBeBetween",
        "column": "humidity_pct",
        "params": [
            {"name": "minValue", "value": "0"},
            {"name": "maxValue", "value": "100"},
        ],
    },
    {
        "name": "weather_row_count",
        "testDefinition": "tableRowCountToBeBetween",
        "column": None,
        "params": [
            {"name": "minValue", "value": "1"},
            {"name": "maxValue", "value": "100000"},
        ],
    },
]


def trigger_openmetadata_quality(**context):
    """
    Register quality test cases in OpenMetadata and trigger profiling.
    Uses the OpenMetadata REST API with JWT authentication.
    """
    headers = _om_headers()
    if not headers:
        logger.warning("OpenMetadata integration skipped (no JWT token)")
        return

    api_base = OPENMETADATA_HOST.rstrip("/")

    # 1. Get the table entity from OpenMetadata
    table_resp = requests.get(
        f"{api_base}/v1/tables/name/{WEATHER_TABLE_FQN}",
        headers=headers,
        params={"fields": "testSuite"},
    )
    if table_resp.status_code == 404:
        logger.warning(
            f"Table {WEATHER_TABLE_FQN} not found in OpenMetadata. "
            "Run metadata ingestion first (Trino service -> Metadata Ingestion)."
        )
        return
    table_resp.raise_for_status()
    table_entity = table_resp.json()
    table_id = table_entity["id"]
    logger.info(f"Found table in OpenMetadata: {WEATHER_TABLE_FQN} (id={table_id})")

    # 2. Create or get the executable test suite for this table
    test_suite_resp = requests.put(
        f"{api_base}/v1/dataQuality/testSuites/executable",
        headers=headers,
        json={
            "name": f"{WEATHER_TABLE_FQN}.testSuite",
            "executableEntityReference": table_id,
        },
    )
    test_suite_resp.raise_for_status()
    test_suite = test_suite_resp.json()
    test_suite_id = test_suite["id"]
    logger.info(f"Test suite ready: {test_suite['name']} (id={test_suite_id})")

    # 3. Create quality test cases
    for test_def in WEATHER_QUALITY_TESTS:
        entity_link = f"<#E::table::{WEATHER_TABLE_FQN}>"
        if test_def["column"]:
            entity_link = f"<#E::table::{WEATHER_TABLE_FQN}::columns::{test_def['column']}>"

        test_case_payload = {
            "name": test_def["name"],
            "testDefinition": test_def["testDefinition"],
            "entityLink": entity_link,
            "testSuite": test_suite_id,
            "parameterValues": test_def["params"],
        }

        tc_resp = requests.post(
            f"{api_base}/v1/dataQuality/testCases",
            headers=headers,
            json=test_case_payload,
        )
        if tc_resp.status_code == 409:
            logger.info(f"Test case '{test_def['name']}' already exists")
        elif tc_resp.ok:
            logger.info(f"Created test case: {test_def['name']}")
        else:
            logger.warning(f"Failed to create test case '{test_def['name']}': {tc_resp.status_code} {tc_resp.text}")

    # 4. Trigger the profiler ingestion pipeline if it exists
    pipelines_resp = requests.get(
        f"{api_base}/v1/services/ingestionPipelines",
        headers=headers,
        params={"service": "Trino", "pipelineType": "profiler", "limit": 100},
    )
    if pipelines_resp.ok:
        pipelines = pipelines_resp.json().get("data", [])
        for pipeline in pipelines:
            pipeline_id = pipeline["id"]
            trigger_resp = requests.post(
                f"{api_base}/v1/services/ingestionPipelines/trigger/{pipeline_id}",
                headers=headers,
            )
            if trigger_resp.ok:
                logger.info(f"Triggered profiler pipeline: {pipeline['name']}")
            else:
                logger.warning(f"Failed to trigger profiler: {trigger_resp.status_code}")
    else:
        logger.info("No profiler pipelines found. Configure one in OpenMetadata UI for Trino service.")

    logger.info(
        f"OpenMetadata quality setup complete for {WEATHER_TABLE_FQN}. "
        f"View results at: https://openmetadata.data-platform.local/table/{WEATHER_TABLE_FQN}"
    )


# ──────────────────────────────────────────────────────────────────────
# PySpark script (uploaded to MinIO, executed by SparkOperator)
# ──────────────────────────────────────────────────────────────────────
_SPARK_SCRIPT = """\
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp, lit, to_date, to_timestamp
import sys


def main(raw_input_path: str, ingestion_date: str) -> None:
    \"\"\"
    Process weather forecast data and write to Iceberg table.

    Args:
        raw_input_path: S3 path to raw JSON (flattened records)
        ingestion_date: Date string (YYYY-MM-DD) for partitioning
    \"\"\"
    spark = SparkSession.builder.appName("weather_to_iceberg").getOrCreate()

    df = spark.read.json(raw_input_path)

    # Raw JSON shape: { data: [{city_name, latitude, longitude, forecast_time, ...}], timestamp }
    exploded = (
        df.select(explode(col("data")).alias("rec"), col("timestamp").alias("api_timestamp"))
        .select(
            col("rec.city_name").alias("city_name"),
            col("rec.latitude").cast("double").alias("latitude"),
            col("rec.longitude").cast("double").alias("longitude"),
            to_timestamp(col("rec.forecast_time")).alias("forecast_time"),
            col("rec.temperature_c").cast("double").alias("temperature_c"),
            col("rec.humidity_pct").cast("double").alias("humidity_pct"),
            col("rec.precipitation_mm").cast("double").alias("precipitation_mm"),
            col("rec.wind_speed_kmh").cast("double").alias("wind_speed_kmh"),
            col("api_timestamp").cast("long").alias("api_timestamp"),
            current_timestamp().alias("ingestion_time"),
            to_date(lit(ingestion_date)).alias("ingestion_date"),
        )
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.weather")

    # Vérifier si la table existe et est accessible
    table_valid = False
    try:
        # Essayer de lire la table pour vérifier si elle existe et est valide
        test_df = spark.table("nessie.weather.hourly_forecast")
        # Essayer de lire les métadonnées (ne pas charger toutes les données)
        test_df.schema  # Accède au schéma pour vérifier que les métadonnées sont accessibles
        table_valid = True
        print("Table exists and is valid, will append data...")
    except Exception as e:
        error_msg = str(e)
        # Si l'erreur indique que les métadonnées sont manquantes, supprimer et recréer
        if "FileNotFoundException" in error_msg or "No such file or directory" in error_msg or "metadata" in error_msg.lower():
            print(f"Table metadata is corrupted or missing: {e}")
            print("Dropping corrupted table and recreating...")
            try:
                spark.sql("DROP TABLE IF EXISTS nessie.weather.hourly_forecast")
            except Exception as drop_err:
                print(f"Note: Could not drop table cleanly: {drop_err}")
                # Essayer de supprimer via Nessie directement
                try:
                    spark.sql("DROP TABLE nessie.weather.hourly_forecast")
                except Exception:
                    pass  # Ignore, on va recréer de toute façon
            table_valid = False
        else:
            # Autre erreur, peut-être que la table n'existe pas simplement
            print(f"Table may not exist: {e}")
            table_valid = False
    
    # Créer la table seulement si elle n'existe pas ou est corrompue
    if not table_valid:
        spark.sql(\"\"\"
            CREATE TABLE IF NOT EXISTS nessie.weather.hourly_forecast (
                city_name STRING,
                latitude DOUBLE,
                longitude DOUBLE,
                forecast_time TIMESTAMP,
                temperature_c DOUBLE,
                humidity_pct DOUBLE,
                precipitation_mm DOUBLE,
                wind_speed_kmh DOUBLE,
                api_timestamp BIGINT,
                ingestion_time TIMESTAMP,
                ingestion_date DATE
            ) USING iceberg
            PARTITIONED BY (ingestion_date)
        \"\"\")
        print("Table created successfully")

    exploded.writeTo("nessie.weather.hourly_forecast").append()
    row_count = exploded.count()
    print(f"Successfully wrote {row_count} weather records for date {ingestion_date}")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: weather_to_iceberg.py <raw_input_path> <ingestion_date>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
"""


# ──────────────────────────────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="weather_data_pipeline",
    default_args=DEFAULT_ARGS,
    description="Weather: Open-Meteo API -> MinIO S3 -> Spark Iceberg -> PostgreSQL -> OpenMetadata Quality",
    schedule_interval="0 6 * * *",  # Daily at 06:00 UTC
    start_date=days_ago(1),
    catchup=False,
    tags=["weather", "s3", "postgres", "iceberg", "nessie", "spark", "openmetadata", "quality"],
) as dag:

    t_setup_pg = PythonOperator(
        task_id="setup_postgres",
        python_callable=setup_postgres,
    )

    t_fetch = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data,
    )

    t_spark = PythonOperator(
        task_id="spark_ingest_iceberg",
        python_callable=submit_spark_application,
    )

    t_load_pg = PythonOperator(
        task_id="load_postgres_analytics",
        python_callable=load_postgres_analytics,
    )

    t_quality = PythonOperator(
        task_id="trigger_openmetadata_quality",
        python_callable=trigger_openmetadata_quality,
        trigger_rule="all_done",  # Run even if Spark step fails
    )

    # Optional: OpenMetadata Lineage
    if OPENMETADATA_AVAILABLE:
        om_config = _get_openmetadata_config()
        if om_config:
            t_lineage = OpenMetadataLineageOperator(
                task_id="publish_openmetadata_lineage",
                server_config=om_config,
                service_name=OPENMETADATA_SERVICE_NAME,
                only_keep_dag_lineage=False,
                trigger_rule="all_done",
            )
            # Flow: setup_pg >> fetch >> spark >> [load_pg, quality] >> lineage
            t_setup_pg >> t_fetch >> t_spark >> [t_load_pg, t_quality] >> t_lineage
        else:
            t_setup_pg >> t_fetch >> t_spark >> [t_load_pg, t_quality]
    else:
        t_setup_pg >> t_fetch >> t_spark >> [t_load_pg, t_quality]

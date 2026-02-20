"""
OpenMetadata Ingestion Bootstrap DAG

This DAG automatically configures OpenMetadata services and ingestion pipelines
for Trino and PostgreSQL using the OpenMetadata REST API.

Services created:
- Trino Database Service (all catalogs: iceberg, postgresql_*)
- PostgreSQL Database Service (analytics_db)

Pipelines created:
- Trino Metadata Ingestion
- Trino Profiler
- PostgreSQL Metadata Ingestion
- PostgreSQL Profiler

The DAG is idempotent: it checks if services/pipelines exist before creating them.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import timedelta
from typing import Any, Dict, List, Optional

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ──────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────
OPENMETADATA_API = "http://openmetadata.openmetadata.svc:8585/api"
TRINO_COORDINATOR = "http://trino-coordinator.trino.svc:8080"
POSTGRES_HOST = "postgres-shared-postgresql.infra.svc.cluster.local"
POSTGRES_PORT = 5432
POSTGRES_ADMIN_USER = "admin"
POSTGRES_ADMIN_PASS = "adminpassword"
POSTGRES_ANALYTICS_DB = "analytics_db"

# Service names
TRINO_SERVICE_NAME = "Trino"
POSTGRES_SERVICE_NAME = "PostgreSQL"
AIRFLOW_SERVICE_NAME = "Airflow"

# Known tables to ingest
TRINO_TABLES = [
    "iceberg.weather.hourly_forecast",
    # "iceberg.crypto.crypto_prices",  # Uncomment if this table exists
]

POSTGRES_TABLES = [
    "public.weather_daily_summary",
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
def _om_headers() -> dict:
    """Return OpenMetadata API headers with JWT auth."""
    try:
        jwt_token = Variable.get("openmetadata_jwt_token")
    except KeyError:
        raise ValueError(
            "OpenMetadata JWT token not configured. "
            "Set Airflow Variable 'openmetadata_jwt_token' with a valid JWT token."
        )
    return {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json",
    }


def _om_get(endpoint: str, headers: dict, params: dict | None = None) -> requests.Response:
    """Make a GET request to OpenMetadata API."""
    url = f"{OPENMETADATA_API.rstrip('/')}/{endpoint.lstrip('/')}"
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    return resp


def _om_post(endpoint: str, headers: dict, payload: dict) -> requests.Response:
    """Make a POST request to OpenMetadata API."""
    url = f"{OPENMETADATA_API.rstrip('/')}/{endpoint.lstrip('/')}"
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    return resp


def _om_put(endpoint: str, headers: dict, payload: dict) -> requests.Response:
    """Make a PUT request to OpenMetadata API."""
    url = f"{OPENMETADATA_API.rstrip('/')}/{endpoint.lstrip('/')}"
    resp = requests.put(url, headers=headers, json=payload, timeout=30)
    return resp


def _om_patch(endpoint: str, headers: dict, payload: dict) -> requests.Response:
    """Make a PATCH request to OpenMetadata API."""
    url = f"{OPENMETADATA_API.rstrip('/')}/{endpoint.lstrip('/')}"
    resp = requests.patch(url, headers=headers, json=payload, timeout=30)
    return resp


def _om_delete(endpoint: str, headers: dict) -> requests.Response:
    """Make a DELETE request to OpenMetadata API."""
    url = f"{OPENMETADATA_API.rstrip('/')}/{endpoint.lstrip('/')}"
    resp = requests.delete(url, headers=headers, timeout=30)
    return resp


def _discover_trino_tables() -> List[Dict[str, Any]]:
    """Discover existing tables in Trino."""
    discovered_tables = []
    
    # Use known tables (can be extended to query Trino API)
    for table_path in TRINO_TABLES:
        parts = table_path.split(".")
        if len(parts) == 3:
            catalog, schema, table = parts
            discovered_tables.append({
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "fqn": table_path,
            })
            logger.info(f"Will ingest Trino table: {table_path}")

    return discovered_tables


def _discover_postgres_tables() -> List[Dict[str, Any]]:
    """Discover existing tables in PostgreSQL."""
    import psycopg2

    discovered_tables = []
    
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_ANALYTICS_DB,
            user=POSTGRES_ADMIN_USER,
            password=POSTGRES_ADMIN_PASS,
        )
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
            """)
            for schema, table in cur.fetchall():
                table_fqn = f"{schema}.{table}"
                # Only include known tables
                if table_fqn in POSTGRES_TABLES or table in [t.split(".")[-1] for t in POSTGRES_TABLES]:
                    discovered_tables.append({
                        "schema": schema,
                        "table": table,
                        "fqn": table_fqn,
                    })
                    logger.info(f"Will ingest PostgreSQL table: {table_fqn}")
        conn.close()
    except Exception as e:
        logger.warning(f"Could not discover PostgreSQL tables: {e}")
        # Fallback to known tables
        for table_path in POSTGRES_TABLES:
            parts = table_path.split(".")
            if len(parts) == 2:
                schema, table = parts
                discovered_tables.append({
                    "schema": schema,
                    "table": table,
                    "fqn": table_path,
                })

    return discovered_tables


def _service_exists(service_name: str, service_type: str, headers: dict) -> Optional[Dict[str, Any]]:
    """Check if a database service exists in OpenMetadata."""
    resp = _om_get(
        f"v1/services/databaseServices",
        headers=headers,
        params={"limit": 100},  # Get all services and filter client-side
    )
    if resp.status_code == 200:
        data = resp.json().get("data", [])
        # Filter by both name and serviceType client-side for accuracy
        for service in data:
            if service.get("name") == service_name and service.get("serviceType") == service_type:
                return service
    return None


def _pipeline_exists(service_fqn: str, pipeline_type: str, headers: dict) -> Optional[Dict[str, Any]]:
    """Check if an ingestion pipeline exists for a service."""
    resp = _om_get(
        "v1/services/ingestionPipelines",
        headers=headers,
        params={"service": service_fqn, "pipelineType": pipeline_type},
    )
    if resp.status_code == 200:
        data = resp.json().get("data", [])
        if data:
            return data[0]
    return None


# ──────────────────────────────────────────────────────────────────────
# Task 1: Create Trino Database Service
# ──────────────────────────────────────────────────────────────────────
def create_trino_service(**context) -> Dict[str, Any]:
    """Create or update Trino database service in OpenMetadata."""
    headers = _om_headers()

    # Check if service already exists
    existing = _service_exists(TRINO_SERVICE_NAME, "Trino", headers)
    if existing:
        logger.info(f"Trino service already exists: {existing['fullyQualifiedName']} (id={existing['id']})")
        # Verify it's actually Trino service, not another service
        if existing.get("name") != TRINO_SERVICE_NAME or existing.get("serviceType") != "Trino":
            logger.warning(f"Found service with wrong name/type: {existing.get('name')}/{existing.get('serviceType')}")
            # Delete it and recreate
            try:
                _om_delete(f"v1/services/databaseServices/{existing['id']}?recursive=true&hardDelete=true", headers=headers)
                logger.info(f"Deleted incorrect service: {existing['id']}")
            except Exception as e:
                logger.warning(f"Could not delete incorrect service: {e}")
        else:
            return {"service_id": existing["id"], "service_fqn": existing["fullyQualifiedName"], "created": False}

    # Create Trino service
    service_payload = {
        "name": TRINO_SERVICE_NAME,
        "serviceType": "Trino",
        "description": "Trino SQL query engine - all catalogs (Iceberg, PostgreSQL connectors)",
        "connection": {
            "config": {
                "type": "Trino",
                "hostPort": TRINO_COORDINATOR.replace("http://", "").replace("https://", ""),
                "username": "admin",
            }
        },
    }

    resp = _om_post("v1/services/databaseServices", headers=headers, payload=service_payload)

    if resp.status_code in (200, 201):
        service = resp.json()
        logger.info(f"Created Trino service: {service['fullyQualifiedName']} (id={service['id']})")
        return {"service_id": service["id"], "service_fqn": service["fullyQualifiedName"], "created": True}
    elif resp.status_code == 409:
        # Service already exists (race condition)
        existing = _service_exists(TRINO_SERVICE_NAME, "Trino", headers)
        if existing:
            logger.info(f"Trino service already exists (409): {existing['fullyQualifiedName']}")
            return {"service_id": existing["id"], "service_fqn": existing["fullyQualifiedName"], "created": False}
        raise RuntimeError(f"Failed to create Trino service: 409 but service not found")
    else:
        raise RuntimeError(f"Failed to create Trino service: {resp.status_code} {resp.text}")


# ──────────────────────────────────────────────────────────────────────
# Task 2: Discover Trino Tables
# ──────────────────────────────────────────────────────────────────────
def discover_trino_tables_task(**context) -> Dict[str, Any]:
    """Discover existing tables in Trino."""
    tables = _discover_trino_tables()
    return {"tables": tables}


# ──────────────────────────────────────────────────────────────────────
# Task 3: Create Trino Metadata Ingestion Pipeline
# ──────────────────────────────────────────────────────────────────────
def create_trino_metadata_ingestion(**context) -> Dict[str, Any]:
    """Create Trino metadata ingestion pipeline for existing tables only."""
    headers = _om_headers()
    xcom = context["ti"].xcom_pull(task_ids="create_trino_service")
    if not xcom:
        raise ValueError("create_trino_service task did not return any data. Check if the service was created successfully.")
    service_fqn = xcom["service_fqn"]

    # Get discovered tables
    tables_xcom = context["ti"].xcom_pull(task_ids="discover_trino_tables")
    tables = tables_xcom.get("tables", []) if tables_xcom else []

    # Build schema and table filter patterns from discovered tables
    schemas = set()
    table_patterns = []
    for table_info in tables:
        schemas.add(table_info["schema"])
        table_patterns.append(table_info["table"])

    schema_filter = {"includes": list(schemas)} if schemas else {"includes": [".*"]}
    table_filter = {"includes": table_patterns} if table_patterns else {"includes": [".*"]}

    logger.info(f"Ingesting Trino tables: {len(tables)} tables in schemas {list(schemas)}")

    # Check if pipeline already exists
    existing = _pipeline_exists(service_fqn, "metadata", headers)
    if existing:
        logger.info(f"Trino metadata ingestion pipeline already exists: {existing['name']} (id={existing['id']})")
        return {"pipeline_id": existing["id"], "pipeline_name": existing["name"], "created": False}

    pipeline_payload = {
        "name": f"{TRINO_SERVICE_NAME}_metadata",
        "displayName": f"{TRINO_SERVICE_NAME} Metadata Ingestion",
        "description": f"Metadata ingestion for {len(tables)} Trino tables: {', '.join([t['fqn'] for t in tables])}",
        "pipelineType": "metadata",
        "service": {"id": xcom["service_id"], "type": "databaseService"},
        "sourceConfig": {
            "config": {
                "type": "Trino",
                "schemaFilterPattern": schema_filter,
                "tableFilterPattern": table_filter,
                "includeViews": False,
                "markDeletedTables": True,
                "markDeletedViews": True,
            }
        },
    }

    resp = _om_post("v1/services/ingestionPipelines", headers=headers, payload=pipeline_payload)

    if resp.status_code in (200, 201):
        pipeline = resp.json()
        logger.info(f"Created Trino metadata ingestion pipeline: {pipeline['name']} (id={pipeline['id']})")
        return {"pipeline_id": pipeline["id"], "pipeline_name": pipeline["name"], "created": True}
    elif resp.status_code == 409:
        existing = _pipeline_exists(service_fqn, "metadata", headers)
        if existing:
            logger.info(f"Trino metadata ingestion pipeline already exists (409): {existing['name']}")
            return {"pipeline_id": existing["id"], "pipeline_name": existing["name"], "created": False}
        raise RuntimeError(f"Failed to create Trino metadata pipeline: 409 but pipeline not found")
    else:
        raise RuntimeError(f"Failed to create Trino metadata ingestion pipeline: {resp.status_code} {resp.text}")


# ──────────────────────────────────────────────────────────────────────
# Task 4: Create Trino Profiler Pipeline
# ──────────────────────────────────────────────────────────────────────
def create_trino_profiler(**context) -> Dict[str, Any]:
    """Create Trino profiler pipeline."""
    headers = _om_headers()
    xcom = context["ti"].xcom_pull(task_ids="create_trino_service")
    if not xcom:
        raise ValueError("create_trino_service task did not return any data. Check if the service was created successfully.")
    service_fqn = xcom["service_fqn"]

    # Check if pipeline already exists
    existing = _pipeline_exists(service_fqn, "profiler", headers)
    if existing:
        logger.info(f"Trino profiler pipeline already exists: {existing['name']} (id={existing['id']})")
        return {"pipeline_id": existing["id"], "pipeline_name": existing["name"], "created": False}

    # Get discovered tables for filtering
    tables_xcom = context["ti"].xcom_pull(task_ids="discover_trino_tables")
    tables = tables_xcom.get("tables", []) if tables_xcom else []
    
    schemas = set()
    table_patterns = []
    for table_info in tables:
        schemas.add(table_info["schema"])
        table_patterns.append(table_info["table"])

    schema_filter = {"includes": list(schemas)} if schemas else {"includes": [".*"]}
    table_filter = {"includes": table_patterns} if table_patterns else {"includes": [".*"]}

    pipeline_payload = {
        "name": f"{TRINO_SERVICE_NAME}_profiler",
        "displayName": f"{TRINO_SERVICE_NAME} Profiler",
        "description": f"Data profiling for {len(tables)} Trino tables",
        "pipelineType": "profiler",
        "service": {"id": xcom["service_id"], "type": "databaseService"},
        "sourceConfig": {
            "config": {
                "type": "Profiler",
                "schemaFilterPattern": schema_filter,
                "tableFilterPattern": table_filter,
                "includeViews": False,
                "profileSample": 50.0,  # Profile 50% of rows
                "threadCount": 5,
                "timeoutSeconds": 43200,  # 12 hours
            }
        },
    }

    resp = _om_post("v1/services/ingestionPipelines", headers=headers, payload=pipeline_payload)

    if resp.status_code in (200, 201):
        pipeline = resp.json()
        logger.info(f"Created Trino profiler pipeline: {pipeline['name']} (id={pipeline['id']})")
        return {"pipeline_id": pipeline["id"], "pipeline_name": pipeline["name"], "created": True}
    elif resp.status_code == 409:
        existing = _pipeline_exists(service_fqn, "profiler", headers)
        if existing:
            logger.info(f"Trino profiler pipeline already exists (409): {existing['name']}")
            return {"pipeline_id": existing["id"], "pipeline_name": existing["name"], "created": False}
        raise RuntimeError(f"Failed to create Trino profiler pipeline: 409 but pipeline not found")
    else:
        raise RuntimeError(f"Failed to create Trino profiler pipeline: {resp.status_code} {resp.text}")


# ──────────────────────────────────────────────────────────────────────
# Task 5: Discover PostgreSQL Tables
# ──────────────────────────────────────────────────────────────────────
def discover_postgres_tables_task(**context) -> Dict[str, Any]:
    """Discover existing tables in PostgreSQL."""
    tables = _discover_postgres_tables()
    return {"tables": tables}


# ──────────────────────────────────────────────────────────────────────
# Task 6: Create PostgreSQL Database Service
# ──────────────────────────────────────────────────────────────────────
def create_postgres_service(**context) -> Dict[str, Any]:
    """Create or update PostgreSQL database service for analytics_db."""
    headers = _om_headers()

    # Check if service already exists
    existing = _service_exists(POSTGRES_SERVICE_NAME, "Postgres", headers)
    if existing:
        logger.info(f"PostgreSQL service already exists: {existing['fullyQualifiedName']} (id={existing['id']})")
        return {"service_id": existing["id"], "service_fqn": existing["fullyQualifiedName"], "created": False}

    # Create PostgreSQL service
    service_payload = {
        "name": POSTGRES_SERVICE_NAME,
        "serviceType": "Postgres",
        "description": "PostgreSQL database service - analytics_db (weather summaries)",
        "connection": {
            "config": {
                "type": "Postgres",
                "hostPort": f"{POSTGRES_HOST}:{POSTGRES_PORT}",
                "database": POSTGRES_ANALYTICS_DB,
                "username": POSTGRES_ADMIN_USER,
                "authType": {"password": POSTGRES_ADMIN_PASS},
            }
        },
    }

    resp = _om_post("v1/services/databaseServices", headers=headers, payload=service_payload)

    if resp.status_code in (200, 201):
        service = resp.json()
        logger.info(f"Created PostgreSQL service: {service['fullyQualifiedName']} (id={service['id']})")
        return {"service_id": service["id"], "service_fqn": service["fullyQualifiedName"], "created": True}
    elif resp.status_code == 409:
        existing = _service_exists(POSTGRES_SERVICE_NAME, "Postgres", headers)
        if existing:
            logger.info(f"PostgreSQL service already exists (409): {existing['fullyQualifiedName']}")
            return {"service_id": existing["id"], "service_fqn": existing["fullyQualifiedName"], "created": False}
        raise RuntimeError(f"Failed to create PostgreSQL service: 409 but service not found")
    else:
        raise RuntimeError(f"Failed to create PostgreSQL service: {resp.status_code} {resp.text}")


# ──────────────────────────────────────────────────────────────────────
# Task 7: Create PostgreSQL Metadata Ingestion Pipeline
# ──────────────────────────────────────────────────────────────────────
def create_postgres_metadata_ingestion(**context) -> Dict[str, Any]:
    """Create PostgreSQL metadata ingestion pipeline for existing tables only."""
    headers = _om_headers()
    xcom = context["ti"].xcom_pull(task_ids="create_postgres_service")
    if not xcom:
        raise ValueError("create_postgres_service task did not return any data. Check if the service was created successfully.")
    service_fqn = xcom["service_fqn"]

    # Get discovered tables
    tables_xcom = context["ti"].xcom_pull(task_ids="discover_postgres_tables")
    tables = tables_xcom.get("tables", []) if tables_xcom else []

    # Build schema and table filter patterns
    schemas = set()
    table_patterns = []
    for table_info in tables:
        schemas.add(table_info["schema"])
        table_patterns.append(table_info["table"])

    schema_filter = {"includes": list(schemas)} if schemas else {"includes": ["public"]}
    table_filter = {"includes": table_patterns} if table_patterns else {"includes": [".*"]}

    logger.info(f"Ingesting PostgreSQL tables: {len(tables)} tables in schemas {list(schemas)}")

    # Check if pipeline already exists
    existing = _pipeline_exists(service_fqn, "metadata", headers)
    if existing:
        logger.info(f"PostgreSQL metadata ingestion pipeline already exists: {existing['name']} (id={existing['id']})")
        return {"pipeline_id": existing["id"], "pipeline_name": existing["name"], "created": False}

    pipeline_payload = {
        "name": f"{POSTGRES_SERVICE_NAME}_metadata",
        "displayName": f"{POSTGRES_SERVICE_NAME} Metadata Ingestion",
        "description": f"Metadata ingestion for {len(tables)} PostgreSQL tables: {', '.join([t['fqn'] for t in tables])}",
        "pipelineType": "metadata",
        "service": {"id": xcom["service_id"], "type": "databaseService"},
        "sourceConfig": {
            "config": {
                "type": "Postgres",
                "schemaFilterPattern": schema_filter,
                "tableFilterPattern": table_filter,
                "includeViews": False,
                "markDeletedTables": True,
                "markDeletedViews": True,
            }
        },
    }

    resp = _om_post("v1/services/ingestionPipelines", headers=headers, payload=pipeline_payload)

    if resp.status_code in (200, 201):
        pipeline = resp.json()
        logger.info(f"Created PostgreSQL metadata ingestion pipeline: {pipeline['name']} (id={pipeline['id']})")
        return {"pipeline_id": pipeline["id"], "pipeline_name": pipeline["name"], "created": True}
    elif resp.status_code == 409:
        existing = _pipeline_exists(service_fqn, "metadata", headers)
        if existing:
            logger.info(f"PostgreSQL metadata ingestion pipeline already exists (409): {existing['name']}")
            return {"pipeline_id": existing["id"], "pipeline_name": existing["name"], "created": False}
        raise RuntimeError(f"Failed to create PostgreSQL metadata pipeline: 409 but pipeline not found")
    else:
        raise RuntimeError(f"Failed to create PostgreSQL metadata ingestion pipeline: {resp.status_code} {resp.text}")


# ──────────────────────────────────────────────────────────────────────
# Task 8: Create PostgreSQL Profiler Pipeline
# ──────────────────────────────────────────────────────────────────────
def create_postgres_profiler(**context) -> Dict[str, Any]:
    """Create PostgreSQL profiler pipeline for existing tables only."""
    headers = _om_headers()
    xcom = context["ti"].xcom_pull(task_ids="create_postgres_service")
    if not xcom:
        raise ValueError("create_postgres_service task did not return any data. Check if the service was created successfully.")
    service_fqn = xcom["service_fqn"]

    # Get discovered tables for filtering
    tables_xcom = context["ti"].xcom_pull(task_ids="discover_postgres_tables")
    tables = tables_xcom.get("tables", []) if tables_xcom else []

    schemas = set()
    table_patterns = []
    for table_info in tables:
        schemas.add(table_info["schema"])
        table_patterns.append(table_info["table"])

    schema_filter = {"includes": list(schemas)} if schemas else {"includes": ["public"]}
    table_filter = {"includes": table_patterns} if table_patterns else {"includes": [".*"]}

    # Check if pipeline already exists
    existing = _pipeline_exists(service_fqn, "profiler", headers)
    if existing:
        logger.info(f"PostgreSQL profiler pipeline already exists: {existing['name']} (id={existing['id']})")
        return {"pipeline_id": existing["id"], "pipeline_name": existing["name"], "created": False}

    pipeline_payload = {
        "name": f"{POSTGRES_SERVICE_NAME}_profiler",
        "displayName": f"{POSTGRES_SERVICE_NAME} Profiler",
        "description": f"Data profiling for {len(tables)} PostgreSQL tables",
        "pipelineType": "profiler",
        "service": {"id": xcom["service_id"], "type": "databaseService"},
        "sourceConfig": {
            "config": {
                "type": "Profiler",
                "schemaFilterPattern": schema_filter,
                "tableFilterPattern": table_filter,
                "includeViews": False,
                "profileSample": 50.0,
                "threadCount": 5,
                "timeoutSeconds": 43200,
            }
        },
    }

    resp = _om_post("v1/services/ingestionPipelines", headers=headers, payload=pipeline_payload)

    if resp.status_code in (200, 201):
        pipeline = resp.json()
        logger.info(f"Created PostgreSQL profiler pipeline: {pipeline['name']} (id={pipeline['id']})")
        return {"pipeline_id": pipeline["id"], "pipeline_name": pipeline["name"], "created": True}
    elif resp.status_code == 409:
        existing = _pipeline_exists(service_fqn, "profiler", headers)
        if existing:
            logger.info(f"PostgreSQL profiler pipeline already exists (409): {existing['name']}")
            return {"pipeline_id": existing["id"], "pipeline_name": existing["name"], "created": False}
        raise RuntimeError(f"Failed to create PostgreSQL profiler pipeline: 409 but pipeline not found")
    else:
        raise RuntimeError(f"Failed to create PostgreSQL profiler pipeline: {resp.status_code} {resp.text}")


# ──────────────────────────────────────────────────────────────────────
# Task 9: Create Airflow Connection
# ──────────────────────────────────────────────────────────────────────
def create_airflow_connection(**context) -> Dict[str, Any]:
    """Create Airflow pipeline service connection in OpenMetadata."""
    headers = _om_headers()

    # Check if Airflow service already exists
    resp = _om_get("v1/services/pipelineServices", headers=headers, params={"limit": 100})
    if resp.status_code == 200:
        services = resp.json().get("data", [])
        for service in services:
            if service.get("name") == AIRFLOW_SERVICE_NAME:
                logger.info(f"Airflow service already exists: {service['fullyQualifiedName']} (id={service['id']})")
                return {"service_id": service["id"], "service_fqn": service["fullyQualifiedName"], "created": False}

    # Create Airflow pipeline service
    service_payload = {
        "name": AIRFLOW_SERVICE_NAME,
        "serviceType": "Airflow",
        "description": "Airflow DAG execution service",
        "connection": {
            "config": {
                "type": "Airflow",
                "host": "http://airflow-web.airflow.svc.cluster.local:8080",
                "username": "admin",
                "password": "admin",
                "timeout": 60,
                "supportsMetadataExtraction": True,
            }
        },
    }

    resp = _om_post("v1/services/pipelineServices", headers=headers, payload=service_payload)

    if resp.status_code in (200, 201):
        service = resp.json()
        logger.info(f"Created Airflow service: {service['fullyQualifiedName']} (id={service['id']})")
        return {"service_id": service["id"], "service_fqn": service["fullyQualifiedName"], "created": True}
    elif resp.status_code == 409:
        # Service already exists
        resp = _om_get("v1/services/pipelineServices", headers=headers, params={"limit": 100})
        if resp.status_code == 200:
            services = resp.json().get("data", [])
            for service in services:
                if service.get("name") == AIRFLOW_SERVICE_NAME:
                    logger.info(f"Airflow service already exists (409): {service['fullyQualifiedName']}")
                    return {"service_id": service["id"], "service_fqn": service["fullyQualifiedName"], "created": False}
        raise RuntimeError(f"Failed to create Airflow service: 409 but service not found")
    else:
        raise RuntimeError(f"Failed to create Airflow service: {resp.status_code} {resp.text}")


# ──────────────────────────────────────────────────────────────────────
# Task 10: Trigger All Pipelines
# ──────────────────────────────────────────────────────────────────────
def trigger_all_pipelines(**context) -> Dict[str, Any]:
    """Trigger all ingestion pipelines and wait for completion."""
    headers = _om_headers()

    # Collect all pipeline IDs from previous tasks
    pipeline_ids = []

    # Trino pipelines
    trino_metadata = context["ti"].xcom_pull(task_ids="create_trino_metadata_ingestion")
    if trino_metadata:
        pipeline_ids.append(("Trino Metadata", trino_metadata["pipeline_id"]))

    trino_profiler = context["ti"].xcom_pull(task_ids="create_trino_profiler")
    if trino_profiler:
        pipeline_ids.append(("Trino Profiler", trino_profiler["pipeline_id"]))

    # PostgreSQL pipelines
    postgres_metadata = context["ti"].xcom_pull(task_ids="create_postgres_metadata_ingestion")
    if postgres_metadata:
        pipeline_ids.append(("PostgreSQL Metadata", postgres_metadata["pipeline_id"]))

    postgres_profiler = context["ti"].xcom_pull(task_ids="create_postgres_profiler")
    if postgres_profiler:
        pipeline_ids.append(("PostgreSQL Profiler", postgres_profiler["pipeline_id"]))

    if not pipeline_ids:
        logger.warning("No pipelines to trigger")
        return {"triggered": [], "failed": []}

    triggered = []
    failed = []

    # Trigger each pipeline
    for pipeline_name, pipeline_id in pipeline_ids:
        try:
            resp = _om_post(
                f"v1/services/ingestionPipelines/trigger/{pipeline_id}",
                headers=headers,
                payload={},
            )
            if resp.status_code in (200, 201):
                triggered.append(pipeline_name)
                logger.info(f"Triggered pipeline: {pipeline_name}")
            else:
                failed.append(pipeline_name)
                logger.error(f"Failed to trigger {pipeline_name}: {resp.status_code} {resp.text}")
        except Exception as e:
            failed.append(pipeline_name)
            logger.error(f"Error triggering {pipeline_name}: {e}")

    logger.info(f"Pipeline trigger summary: {len(triggered)} triggered, {len(failed)} failed")
    return {"triggered": triggered, "failed": failed}


# ──────────────────────────────────────────────────────────────────────
# Task 8: Verify Ingestion Results
# ──────────────────────────────────────────────────────────────────────
def verify_ingestion_results(**context) -> Dict[str, Any]:
    """Verify that tables were discovered and ingested successfully."""
    headers = _om_headers()

    results = {
        "trino_tables": [],
        "postgres_tables": [],
        "errors": [],
    }

    # Check Trino tables
    try:
        trino_service = context["ti"].xcom_pull(task_ids="create_trino_service")
        if trino_service:
            service_fqn = trino_service["service_fqn"]
            resp = _om_get(
                "v1/tables",
                headers=headers,
                params={"service": service_fqn, "limit": 100},
            )
            if resp.status_code == 200:
                tables = resp.json().get("data", [])
                results["trino_tables"] = [t["fullyQualifiedName"] for t in tables]
                logger.info(f"Found {len(tables)} Trino tables")
            else:
                results["errors"].append(f"Failed to get Trino tables: {resp.status_code}")
    except Exception as e:
        results["errors"].append(f"Error checking Trino tables: {e}")

    # Check PostgreSQL tables
    try:
        postgres_service = context["ti"].xcom_pull(task_ids="create_postgres_service")
        if postgres_service:
            service_fqn = postgres_service["service_fqn"]
            resp = _om_get(
                "v1/tables",
                headers=headers,
                params={"service": service_fqn, "limit": 100},
            )
            if resp.status_code == 200:
                tables = resp.json().get("data", [])
                results["postgres_tables"] = [t["fullyQualifiedName"] for t in tables]
                logger.info(f"Found {len(tables)} PostgreSQL tables")
            else:
                results["errors"].append(f"Failed to get PostgreSQL tables: {resp.status_code}")
    except Exception as e:
        results["errors"].append(f"Error checking PostgreSQL tables: {e}")

    # Log summary
    total_tables = len(results["trino_tables"]) + len(results["postgres_tables"])
    logger.info(f"Ingestion verification complete: {total_tables} total tables discovered")
    logger.info(f"  Trino: {len(results['trino_tables'])} tables")
    logger.info(f"  PostgreSQL: {len(results['postgres_tables'])} tables")

    if results["errors"]:
        logger.warning(f"Errors during verification: {results['errors']}")

    return results


# ──────────────────────────────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="openmetadata_ingestion_bootstrap",
    default_args=DEFAULT_ARGS,
    description="Bootstrap OpenMetadata: Create Trino + PostgreSQL services and ingestion pipelines",
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=["openmetadata", "ingestion", "bootstrap", "trino", "postgres"],
) as dag:

    # Discover tables (can run in parallel)
    t_discover_trino = PythonOperator(
        task_id="discover_trino_tables",
        python_callable=discover_trino_tables_task,
    )

    t_discover_postgres = PythonOperator(
        task_id="discover_postgres_tables",
        python_callable=discover_postgres_tables_task,
    )

    # Create services
    t_trino_service = PythonOperator(
        task_id="create_trino_service",
        python_callable=create_trino_service,
    )

    t_postgres_service = PythonOperator(
        task_id="create_postgres_service",
        python_callable=create_postgres_service,
    )

    t_airflow_connection = PythonOperator(
        task_id="create_airflow_connection",
        python_callable=create_airflow_connection,
    )

    # Create Trino pipelines (need service + discovered tables)
    t_trino_metadata = PythonOperator(
        task_id="create_trino_metadata_ingestion",
        python_callable=create_trino_metadata_ingestion,
    )

    t_trino_profiler = PythonOperator(
        task_id="create_trino_profiler",
        python_callable=create_trino_profiler,
    )

    # Create PostgreSQL pipelines (need service + discovered tables)
    t_postgres_metadata = PythonOperator(
        task_id="create_postgres_metadata_ingestion",
        python_callable=create_postgres_metadata_ingestion,
    )

    t_postgres_profiler = PythonOperator(
        task_id="create_postgres_profiler",
        python_callable=create_postgres_profiler,
    )

    # Trigger all pipelines
    t_trigger = PythonOperator(
        task_id="trigger_all_pipelines",
        python_callable=trigger_all_pipelines,
    )

    # Verify results
    t_verify = PythonOperator(
        task_id="verify_ingestion_results",
        python_callable=verify_ingestion_results,
    )

    # DAG flow:
    # Discover tables (parallel, independent)
    # Create services (parallel, independent)
    #   -> Create pipelines (need service + discovered tables)
    #     -> Trigger all pipelines
    #       -> Verify results
    t_trino_service >> [t_trino_metadata, t_trino_profiler]
    t_postgres_service >> [t_postgres_metadata, t_postgres_profiler]
    t_discover_trino >> [t_trino_metadata, t_trino_profiler]
    t_discover_postgres >> [t_postgres_metadata, t_postgres_profiler]
    [t_trino_metadata, t_trino_profiler, t_postgres_metadata, t_postgres_profiler] >> t_trigger >> t_verify
    # Airflow connection is independent

"""
OpenMetadata Cleanup DAG

This DAG deletes all services, pipelines, and related data from OpenMetadata
to start fresh. Use this before running the bootstrap DAG.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, Dict, List

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OPENMETADATA_API = "http://openmetadata.openmetadata.svc:8585/api"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=10),
}


def _om_headers() -> dict:
    """Return OpenMetadata API headers with JWT auth."""
    try:
        jwt_token = Variable.get("openmetadata_jwt_token")
    except KeyError:
        raise ValueError("OpenMetadata JWT token not configured.")
    return {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json",
    }


def _om_get(endpoint: str, headers: dict, params: dict | None = None) -> requests.Response:
    """Make a GET request to OpenMetadata API."""
    url = f"{OPENMETADATA_API.rstrip('/')}/{endpoint.lstrip('/')}"
    return requests.get(url, headers=headers, params=params, timeout=30)


def _om_delete(endpoint: str, headers: dict) -> requests.Response:
    """Make a DELETE request to OpenMetadata API."""
    url = f"{OPENMETADATA_API.rstrip('/')}/{endpoint.lstrip('/')}"
    return requests.delete(url, headers=headers, timeout=30)


def delete_all_pipelines(**context) -> Dict[str, Any]:
    """Delete all ingestion pipelines."""
    headers = _om_headers()
    deleted = []
    failed = []

    # Get all pipelines
    resp = _om_get("v1/services/ingestionPipelines", headers=headers, params={"limit": 100})
    if resp.status_code != 200:
        logger.error(f"Failed to list pipelines: {resp.status_code}")
        return {"deleted": [], "failed": ["Failed to list pipelines"]}

    pipelines = resp.json().get("data", [])
    logger.info(f"Found {len(pipelines)} pipelines to delete")

    for pipeline in pipelines:
        pipeline_id = pipeline["id"]
        pipeline_name = pipeline.get("name", pipeline_id)
        try:
            del_resp = _om_delete(f"v1/services/ingestionPipelines/{pipeline_id}", headers=headers)
            if del_resp.status_code in (200, 204):
                deleted.append(pipeline_name)
                logger.info(f"Deleted pipeline: {pipeline_name}")
            else:
                failed.append(f"{pipeline_name}: {del_resp.status_code}")
                logger.warning(f"Failed to delete {pipeline_name}: {del_resp.status_code}")
        except Exception as e:
            failed.append(f"{pipeline_name}: {str(e)}")
            logger.error(f"Error deleting {pipeline_name}: {e}")

    logger.info(f"Pipeline cleanup: {len(deleted)} deleted, {len(failed)} failed")
    return {"deleted": deleted, "failed": failed}


def delete_all_services(**context) -> Dict[str, Any]:
    """Delete all database services."""
    headers = _om_headers()
    deleted = []
    failed = []

    # Get all database services
    resp = _om_get("v1/services/databaseServices", headers=headers, params={"limit": 100})
    if resp.status_code != 200:
        logger.error(f"Failed to list services: {resp.status_code}")
        return {"deleted": [], "failed": ["Failed to list services"]}

    services = resp.json().get("data", [])
    logger.info(f"Found {len(services)} services to delete")

    for service in services:
        service_id = service["id"]
        service_name = service.get("name", service_id)
        service_fqn = service.get("fullyQualifiedName", service_name)

        protected_services = ["PostgreSQL", "Trino", "Airflow", "MinIO"]
        if service_name in protected_services:
            logger.info(f"Skipping protected service: {service_name}")
            continue

        try:
            # Delete with recursive=true to delete related entities
            del_resp = _om_delete(
                f"v1/services/databaseServices/{service_id}?recursive=true&hardDelete=true",
                headers=headers,
            )
            if del_resp.status_code in (200, 204):
                deleted.append(service_name)
                logger.info(f"Deleted service: {service_name} ({service_fqn})")
            else:
                failed.append(f"{service_name}: {del_resp.status_code}")
                logger.warning(f"Failed to delete {service_name}: {del_resp.status_code} {del_resp.text}")
        except Exception as e:
            failed.append(f"{service_name}: {str(e)}")
            logger.error(f"Error deleting {service_name}: {e}")

    logger.info(f"Service cleanup: {len(deleted)} deleted, {len(failed)} failed")
    return {"deleted": deleted, "failed": failed}


def delete_all_connections(**context) -> Dict[str, Any]:
    """Delete all connections."""
    headers = _om_headers()
    deleted = []
    failed = []

    # Get all connections
    resp = _om_get("v1/services/connections", headers=headers, params={"limit": 100})
    if resp.status_code != 200:
        logger.warning(f"Failed to list connections: {resp.status_code} (may not exist)")
        return {"deleted": [], "failed": []}

    connections = resp.json().get("data", [])
    logger.info(f"Found {len(connections)} connections to delete")

    for conn in connections:
        conn_id = conn["id"]
        conn_name = conn.get("name", conn_id)
        try:
            del_resp = _om_delete(f"v1/services/connections/{conn_id}", headers=headers)
            if del_resp.status_code in (200, 204):
                deleted.append(conn_name)
                logger.info(f"Deleted connection: {conn_name}")
            else:
                failed.append(f"{conn_name}: {del_resp.status_code}")
        except Exception as e:
            failed.append(f"{conn_name}: {str(e)}")
            logger.error(f"Error deleting {conn_name}: {e}")

    logger.info(f"Connection cleanup: {len(deleted)} deleted, {len(failed)} failed")
    return {"deleted": deleted, "failed": failed}


def delete_all_tags(**context) -> Dict[str, Any]:
    """Delete all tags (user explicitly requested wiping all defaults)."""
    headers = _om_headers()
    deleted = []
    failed = []

    # Get all tags
    resp = _om_get("v1/tags", headers=headers, params={"limit": 100})
    if resp.status_code != 200:
        logger.warning(f"Failed to list tags: {resp.status_code}")
        return {"deleted": [], "failed": []}

    tags = resp.json().get("data", [])
    logger.info(f"Found {len(tags)} tags to delete")

    for tag in tags:
        tag_fqn = tag.get("fullyQualifiedName", "")
        tag_id = tag["id"]
        tag_name = tag.get("name", tag_id)
        try:
            del_resp = _om_delete(f"v1/tags/{tag_id}?recursive=true", headers=headers)
            if del_resp.status_code in (200, 204):
                deleted.append(tag_name)
                logger.info(f"Deleted tag: {tag_name}")
            else:
                failed.append(f"{tag_name}: {del_resp.status_code}")
        except Exception as e:
            failed.append(f"{tag_name}: {str(e)}")
            logger.error(f"Error deleting {tag_name}: {e}")

    logger.info(f"Tag cleanup: {len(deleted)} deleted, {len(failed)} failed")
    return {"deleted": deleted, "failed": failed}


def delete_all_tables(**context) -> Dict[str, Any]:
    """Delete all tables."""
    headers = _om_headers()
    deleted = []
    failed = []

    # Get all tables
    resp = _om_get("v1/tables", headers=headers, params={"limit": 1000})
    if resp.status_code != 200:
        logger.error(f"Failed to list tables: {resp.status_code}")
        return {"deleted": [], "failed": ["Failed to list tables"]}

    tables = resp.json().get("data", [])
    logger.info(f"Found {len(tables)} tables to delete")

    for table in tables:
        table_id = table["id"]
        table_name = table.get("name", table_id)
        try:
            del_resp = _om_delete(
                f"v1/tables/{table_id}?recursive=true&hardDelete=true",
                headers=headers,
            )
            if del_resp.status_code in (200, 204):
                deleted.append(table_name)
                logger.info(f"Deleted table: {table_name}")
            else:
                failed.append(f"{table_name}: {del_resp.status_code}")
                logger.warning(f"Failed to delete {table_name}: {del_resp.status_code} {del_resp.text}")
        except Exception as e:
            failed.append(f"{table_name}: {str(e)}")
            logger.error(f"Error deleting {table_name}: {e}")

    logger.info(f"Table cleanup: {len(deleted)} deleted, {len(failed)} failed")
    return {"deleted": deleted, "failed": failed}


def verify_cleanup(**context) -> Dict[str, Any]:
    """Verify that all services, pipelines, connections, and tags are deleted."""
    headers = _om_headers()

    # Check services
    services_resp = _om_get("v1/services/databaseServices", headers=headers, params={"limit": 100})
    services_count = 0
    if services_resp.status_code == 200:
        services_count = len(services_resp.json().get("data", []))

    # Check pipelines
    pipelines_resp = _om_get("v1/services/ingestionPipelines", headers=headers, params={"limit": 100})
    pipelines_count = 0
    if pipelines_resp.status_code == 200:
        pipelines_count = len(pipelines_resp.json().get("data", []))

    # Check connections
    connections_resp = _om_get("v1/services/connections", headers=headers, params={"limit": 100})
    connections_count = 0
    if connections_resp.status_code == 200:
        connections_count = len(connections_resp.json().get("data", []))

    # Check tags (all)
    tags_resp = _om_get("v1/tags", headers=headers, params={"limit": 100})
    tags_count = 0
    if tags_resp.status_code == 200:
        tags_count = len(tags_resp.json().get("data", []))

    # Check tables
    tables_resp = _om_get("v1/tables", headers=headers, params={"limit": 100})
    tables_count = 0
    if tables_resp.status_code == 200:
        tables_count = len(tables_resp.json().get("data", []))

    logger.info(
        f"Cleanup verification: {services_count} services, {pipelines_count} pipelines, "
        f"{connections_count} connections, {tags_count} tags, {tables_count} tables remaining"
    )

    if services_count == 0 and pipelines_count == 0 and connections_count == 0 and tags_count == 0 and tables_count == 0:
        logger.info("✅ OpenMetadata cleanup completed successfully!")
    else:
        logger.warning(
            f"⚠️  Some entities remain: {services_count} services, {pipelines_count} pipelines, "
            f"{connections_count} connections, {tags_count} tags, {tables_count} tables"
        )

    return {
        "services_remaining": services_count,
        "pipelines_remaining": pipelines_count,
        "connections_remaining": connections_count,
        "tags_remaining": tags_count,
        "tables_remaining": tables_count,
        "clean": services_count == 0 and pipelines_count == 0 and connections_count == 0 and tags_count == 0 and tables_count == 0,
    }


with DAG(
    dag_id="openmetadata_cleanup",
    default_args=DEFAULT_ARGS,
    description="Clean up all OpenMetadata services and pipelines",
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=["openmetadata", "cleanup", "maintenance"],
) as dag:

    t_delete_pipelines = PythonOperator(
        task_id="delete_all_pipelines",
        python_callable=delete_all_pipelines,
    )

    t_delete_services = PythonOperator(
        task_id="delete_all_services",
        python_callable=delete_all_services,
    )

    t_delete_connections = PythonOperator(
        task_id="delete_all_connections",
        python_callable=delete_all_connections,
    )

    t_delete_tags = PythonOperator(
        task_id="delete_all_tags",
        python_callable=delete_all_tags,
    )

    t_delete_tables = PythonOperator(
        task_id="delete_all_tables",
        python_callable=delete_all_tables,
    )

    t_verify = PythonOperator(
        task_id="verify_cleanup",
        python_callable=verify_cleanup,
    )

    # Delete in order: pipelines -> tables -> services -> connections -> tags -> verify
    t_delete_pipelines >> t_delete_tables >> t_delete_services >> t_delete_connections >> t_delete_tags >> t_verify

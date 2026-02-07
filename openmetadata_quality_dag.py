from __future__ import annotations

import json
import logging
from datetime import timedelta

import requests

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ──────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# OpenMetadata Data Quality DAG
# ──────────────────────────────────────────────────────────────────────
# Reusable DAG that monitors data quality on tables registered in OpenMetadata.
#
# Features:
#   1. Validates that tables exist in OpenMetadata catalog
#   2. Retrieves latest data profiles (column statistics)
#   3. Creates standard quality test cases (nulls, ranges, freshness, row count)
#   4. Triggers profiler pipelines if available
#   5. Checks quality test results and reports status
#
# Configurable via DAG params:
#   - table_fqns: list of fully-qualified table names in OpenMetadata
#
# Default tables monitored:
#   - Trino.iceberg.crypto.crypto_prices  (from crypto_iceberg_pipeline)
#   - Trino.iceberg.weather.hourly_forecast (from weather_data_pipeline)

# ──────────────────────────────────────────────────────────────────────
# Cluster endpoints
# ──────────────────────────────────────────────────────────────────────
OPENMETADATA_API = "http://openmetadata.openmetadata.svc:8585/api"

# PostgreSQL connection for direct quality checks
POSTGRES_HOST = "postgres-shared-postgresql.infra.svc"
POSTGRES_PORT = 5432
POSTGRES_ADMIN_USER = "admin"
POSTGRES_ADMIN_PASS = "adminpassword"

# Default tables to monitor
DEFAULT_TABLE_FQNS = [
    "Trino.iceberg.crypto.crypto_prices",
    "Trino.iceberg.weather.hourly_forecast",
]

# Standard quality tests per table (auto-created if they don't exist)
# Format: { table_fqn_suffix: [ test_definitions ] }
QUALITY_TESTS_REGISTRY = {
    "crypto.crypto_prices": [
        {
            "name": "crypto_id_not_null",
            "testDefinition": "columnValuesToBeNotNull",
            "column": "id",
            "params": [],
            "description": "Crypto ID should never be null",
        },
        {
            "name": "crypto_price_positive",
            "testDefinition": "columnValuesToBeBetween",
            "column": "price_usd",
            "params": [{"name": "minValue", "value": "0"}, {"name": "maxValue", "value": "10000000"}],
            "description": "Price USD must be between 0 and 10M",
        },
        {
            "name": "crypto_symbol_not_null",
            "testDefinition": "columnValuesToBeNotNull",
            "column": "symbol",
            "params": [],
            "description": "Symbol should never be null",
        },
        {
            "name": "crypto_row_count",
            "testDefinition": "tableRowCountToBeBetween",
            "column": None,
            "params": [{"name": "minValue", "value": "1"}, {"name": "maxValue", "value": "1000000"}],
            "description": "Table must have between 1 and 1M rows",
        },
    ],
    "weather.hourly_forecast": [
        {
            "name": "weather_city_not_null",
            "testDefinition": "columnValuesToBeNotNull",
            "column": "city_name",
            "params": [],
            "description": "City name should never be null",
        },
        {
            "name": "weather_temp_range",
            "testDefinition": "columnValuesToBeBetween",
            "column": "temperature_c",
            "params": [{"name": "minValue", "value": "-80"}, {"name": "maxValue", "value": "65"}],
            "description": "Temperature must be between -80°C and 65°C",
        },
        {
            "name": "weather_humidity_range",
            "testDefinition": "columnValuesToBeBetween",
            "column": "humidity_pct",
            "params": [{"name": "minValue", "value": "0"}, {"name": "maxValue", "value": "100"}],
            "description": "Humidity must be between 0% and 100%",
        },
        {
            "name": "weather_precip_positive",
            "testDefinition": "columnValuesToBeBetween",
            "column": "precipitation_mm",
            "params": [{"name": "minValue", "value": "0"}, {"name": "maxValue", "value": "500"}],
            "description": "Precipitation must be between 0 and 500mm",
        },
        {
            "name": "weather_row_count",
            "testDefinition": "tableRowCountToBeBetween",
            "column": None,
            "params": [{"name": "minValue", "value": "1"}, {"name": "maxValue", "value": "500000"}],
            "description": "Table must have between 1 and 500K rows",
        },
    ],
}

# ──────────────────────────────────────────────────────────────────────
# Airflow DAG config
# ──────────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


# ──────────────────────────────────────────────────────────────────────
# Helper functions
# ──────────────────────────────────────────────────────────────────────
def _om_headers() -> dict:
    """Return OpenMetadata API headers with JWT auth."""
    jwt_token = Variable.get("openmetadata_jwt_token")
    return {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json",
    }


def _om_get(endpoint: str, headers: dict, params: dict | None = None):
    """Make a GET request to OpenMetadata API."""
    url = f"{OPENMETADATA_API.rstrip('/')}/{endpoint.lstrip('/')}"
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    return resp


def _om_post(endpoint: str, headers: dict, payload: dict):
    """Make a POST request to OpenMetadata API."""
    url = f"{OPENMETADATA_API.rstrip('/')}/{endpoint.lstrip('/')}"
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    return resp


def _om_put(endpoint: str, headers: dict, payload: dict):
    """Make a PUT request to OpenMetadata API."""
    url = f"{OPENMETADATA_API.rstrip('/')}/{endpoint.lstrip('/')}"
    resp = requests.put(url, headers=headers, json=payload, timeout=30)
    return resp


# ──────────────────────────────────────────────────────────────────────
# Task 1 : Validate tables exist in OpenMetadata
# ──────────────────────────────────────────────────────────────────────
def validate_tables(**context):
    """
    Check that all configured tables are registered in the OpenMetadata catalog.
    Returns list of validated table entities.
    """
    headers = _om_headers()
    table_fqns = context["params"].get("table_fqns", DEFAULT_TABLE_FQNS)

    validated_tables = []
    missing_tables = []

    for fqn in table_fqns:
        resp = _om_get(
            f"v1/tables/name/{fqn}",
            headers=headers,
            params={"fields": "columns,profile,testSuite,tags,owner"},
        )

        if resp.status_code == 200:
            table = resp.json()
            col_count = len(table.get("columns", []))
            has_profile = table.get("profile") is not None
            has_owner = table.get("owner") is not None

            validated_tables.append({
                "fqn": fqn,
                "id": table["id"],
                "columns": col_count,
                "has_profile": has_profile,
                "has_owner": has_owner,
            })
            logger.info(
                f"[OK] {fqn} - {col_count} columns, "
                f"profile={'yes' if has_profile else 'no'}, "
                f"owner={'yes' if has_owner else 'no'}"
            )
        elif resp.status_code == 404:
            missing_tables.append(fqn)
            logger.warning(f"[MISSING] {fqn} - not found in OpenMetadata")
        else:
            logger.error(f"[ERROR] {fqn} - API returned {resp.status_code}: {resp.text}")

    if missing_tables:
        logger.warning(
            f"{len(missing_tables)} table(s) not found in OpenMetadata. "
            "Ensure metadata ingestion has been run for the Trino service. "
            f"Missing: {missing_tables}"
        )

    if not validated_tables:
        raise ValueError("No tables found in OpenMetadata. Run metadata ingestion first.")

    return {"validated_tables": validated_tables, "missing_tables": missing_tables}


# ──────────────────────────────────────────────────────────────────────
# Task 2 : Retrieve and log latest data profiles
# ──────────────────────────────────────────────────────────────────────
def check_data_profiles(**context):
    """
    Retrieve the latest data profile for each validated table.
    Logs column-level statistics (row count, null counts, min, max, mean).
    """
    headers = _om_headers()
    xcom = context["ti"].xcom_pull(task_ids="validate_tables")
    validated_tables = xcom["validated_tables"]

    profile_results = []

    for table_info in validated_tables:
        fqn = table_info["fqn"]
        table_id = table_info["id"]

        # Get latest table profile
        resp = _om_get(f"v1/tables/{table_id}/tableProfile", headers=headers, params={"limit": 1})

        if resp.ok:
            profiles = resp.json().get("data", [])
            if profiles:
                profile = profiles[0]
                row_count = profile.get("rowCount", "N/A")
                col_count = profile.get("columnCount", "N/A")
                timestamp = profile.get("timestamp", "N/A")

                logger.info(
                    f"[PROFILE] {fqn} - rows: {row_count}, columns: {col_count}, "
                    f"last profiled: {timestamp}"
                )
                profile_results.append({
                    "fqn": fqn,
                    "row_count": row_count,
                    "column_count": col_count,
                    "timestamp": timestamp,
                    "status": "profiled",
                })
            else:
                logger.warning(f"[PROFILE] {fqn} - no profile data available")
                profile_results.append({"fqn": fqn, "status": "no_profile"})
        else:
            logger.warning(f"[PROFILE] {fqn} - failed to get profile: {resp.status_code}")
            profile_results.append({"fqn": fqn, "status": "error"})

        # Get column-level profile
        resp = _om_get(
            f"v1/tables/name/{fqn}",
            headers=headers,
            params={"fields": "profile,columns"},
        )
        if resp.ok:
            table_data = resp.json()
            columns = table_data.get("columns", [])
            for col_info in columns:
                col_profile = col_info.get("profile")
                if col_profile:
                    logger.info(
                        f"  Column '{col_info['name']}': "
                        f"nulls={col_profile.get('nullCount', 'N/A')}, "
                        f"distinct={col_profile.get('distinctCount', 'N/A')}, "
                        f"min={col_profile.get('min', 'N/A')}, "
                        f"max={col_profile.get('max', 'N/A')}, "
                        f"mean={col_profile.get('mean', 'N/A')}"
                    )

    return {"profile_results": profile_results}


# ──────────────────────────────────────────────────────────────────────
# Task 3 : Create quality test cases in OpenMetadata
# ──────────────────────────────────────────────────────────────────────
def setup_quality_tests(**context):
    """
    Create standard quality test cases in OpenMetadata for each validated table.
    Uses the QUALITY_TESTS_REGISTRY to determine which tests to create.
    """
    headers = _om_headers()
    xcom = context["ti"].xcom_pull(task_ids="validate_tables")
    validated_tables = xcom["validated_tables"]

    created_tests = []
    existing_tests = []

    for table_info in validated_tables:
        fqn = table_info["fqn"]
        table_id = table_info["id"]

        # Find matching test definitions from registry
        matching_tests = []
        for suffix, tests in QUALITY_TESTS_REGISTRY.items():
            if fqn.endswith(suffix):
                matching_tests = tests
                break

        if not matching_tests:
            logger.info(f"No predefined quality tests for {fqn}. Skipping.")
            continue

        # Create or get executable test suite for this table
        ts_resp = _om_put(
            "v1/dataQuality/testSuites/executable",
            headers=headers,
            payload={
                "name": f"{fqn}.testSuite",
                "executableEntityReference": table_id,
            },
        )
        if not ts_resp.ok:
            logger.error(f"Failed to create test suite for {fqn}: {ts_resp.status_code} {ts_resp.text}")
            continue

        test_suite = ts_resp.json()
        test_suite_id = test_suite["id"]
        logger.info(f"Test suite ready for {fqn} (id={test_suite_id})")

        # Create each test case
        for test_def in matching_tests:
            entity_link = f"<#E::table::{fqn}>"
            if test_def["column"]:
                entity_link = f"<#E::table::{fqn}::columns::{test_def['column']}>"

            tc_payload = {
                "name": test_def["name"],
                "description": test_def.get("description", ""),
                "testDefinition": test_def["testDefinition"],
                "entityLink": entity_link,
                "testSuite": test_suite_id,
                "parameterValues": test_def["params"],
            }

            tc_resp = _om_post("v1/dataQuality/testCases", headers=headers, payload=tc_payload)

            if tc_resp.status_code == 409:
                existing_tests.append(test_def["name"])
                logger.info(f"  [EXISTS] {test_def['name']}")
            elif tc_resp.ok:
                created_tests.append(test_def["name"])
                logger.info(f"  [CREATED] {test_def['name']} - {test_def.get('description', '')}")
            else:
                logger.warning(
                    f"  [FAILED] {test_def['name']}: {tc_resp.status_code} {tc_resp.text}"
                )

    logger.info(
        f"Quality tests setup complete. Created: {len(created_tests)}, "
        f"Already existing: {len(existing_tests)}"
    )

    return {"created": created_tests, "existing": existing_tests}


# ──────────────────────────────────────────────────────────────────────
# Task 4 : Trigger profiler pipelines in OpenMetadata
# ──────────────────────────────────────────────────────────────────────
def trigger_profiling(**context):
    """
    Trigger profiler and test suite ingestion pipelines in OpenMetadata.
    These pipelines compute column statistics and run quality tests.
    """
    headers = _om_headers()

    triggered = []
    failed = []

    # Find all profiler and testSuite pipelines
    for pipeline_type in ["profiler", "TestSuite"]:
        resp = _om_get(
            "v1/services/ingestionPipelines",
            headers=headers,
            params={"pipelineType": pipeline_type, "limit": 100},
        )

        if not resp.ok:
            logger.warning(f"Failed to list {pipeline_type} pipelines: {resp.status_code}")
            continue

        pipelines = resp.json().get("data", [])
        logger.info(f"Found {len(pipelines)} {pipeline_type} pipeline(s)")

        for pipeline in pipelines:
            pid = pipeline["id"]
            pname = pipeline.get("name", pid)

            trigger_resp = _om_post(
                f"v1/services/ingestionPipelines/trigger/{pid}",
                headers=headers,
                payload={},
            )

            if trigger_resp.ok:
                triggered.append(pname)
                logger.info(f"  [TRIGGERED] {pname}")
            else:
                failed.append(pname)
                logger.warning(f"  [FAILED] {pname}: {trigger_resp.status_code}")

    if not triggered and not failed:
        logger.info(
            "No profiler/test pipelines found in OpenMetadata. "
            "To enable automatic profiling, create a Profiler Ingestion pipeline "
            "for the Trino service in the OpenMetadata UI."
        )

    return {"triggered": triggered, "failed": failed}


# ──────────────────────────────────────────────────────────────────────
# Task 5 : Check quality results and report status
# ──────────────────────────────────────────────────────────────────────
def check_quality_results(**context):
    """
    Retrieve and report the latest quality test results from OpenMetadata.
    Logs a summary report with pass/fail status for each test.
    """
    headers = _om_headers()
    xcom = context["ti"].xcom_pull(task_ids="validate_tables")
    validated_tables = xcom["validated_tables"]

    report = []
    total_pass = 0
    total_fail = 0
    total_no_result = 0

    for table_info in validated_tables:
        fqn = table_info["fqn"]

        # Get test cases for this table
        resp = _om_get(
            "v1/dataQuality/testCases",
            headers=headers,
            params={
                "entityLink": f"<#E::table::{fqn}>",
                "limit": 100,
                "include": "all",
                "includeAllTests": True,
                "fields": "testDefinition,testSuite,testCaseResult",
            },
        )

        if not resp.ok:
            logger.warning(f"Failed to get test cases for {fqn}: {resp.status_code}")
            continue

        test_cases = resp.json().get("data", [])
        logger.info(f"\n{'='*60}")
        logger.info(f"QUALITY REPORT: {fqn}")
        logger.info(f"{'='*60}")

        if not test_cases:
            logger.info("  No test cases configured for this table.")
            continue

        for tc in test_cases:
            tc_name = tc.get("name", "unknown")
            tc_result = tc.get("testCaseResult")

            if tc_result:
                status = tc_result.get("testCaseStatus", "UNKNOWN")
                timestamp = tc_result.get("timestamp", "N/A")
                result_msg = tc_result.get("result", "")

                if status in ("Success", "success"):
                    total_pass += 1
                    status_icon = "PASS"
                else:
                    total_fail += 1
                    status_icon = "FAIL"

                logger.info(f"  [{status_icon}] {tc_name} - {result_msg} (at {timestamp})")
                report.append({
                    "table": fqn,
                    "test": tc_name,
                    "status": status,
                    "message": result_msg,
                })
            else:
                total_no_result += 1
                logger.info(f"  [PENDING] {tc_name} - no results yet")
                report.append({
                    "table": fqn,
                    "test": tc_name,
                    "status": "PENDING",
                    "message": "No test results available",
                })

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("QUALITY SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"  PASS: {total_pass}")
    logger.info(f"  FAIL: {total_fail}")
    logger.info(f"  PENDING: {total_no_result}")
    logger.info(f"  TOTAL: {total_pass + total_fail + total_no_result}")
    logger.info(f"{'='*60}")

    if total_fail > 0:
        logger.warning(
            f"{total_fail} quality test(s) FAILED. "
            "Check OpenMetadata UI for details: https://openmetadata.data-platform.local"
        )

    return {
        "report": report,
        "summary": {
            "pass": total_pass,
            "fail": total_fail,
            "pending": total_no_result,
        },
    }


# ──────────────────────────────────────────────────────────────────────
# Task 6 : Direct PostgreSQL quality checks (complement to OpenMetadata)
# ──────────────────────────────────────────────────────────────────────
def check_postgres_quality(**context):
    """
    Run direct quality checks on PostgreSQL analytics tables.
    Complements OpenMetadata quality by checking data freshness and completeness
    directly at the source.
    """
    import psycopg2

    checks = []

    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname="analytics_db",
            user=POSTGRES_ADMIN_USER,
            password=POSTGRES_ADMIN_PASS,
        )

        with conn.cursor() as cur:
            # Check 1: weather_daily_summary row count
            cur.execute("SELECT COUNT(*) FROM weather_daily_summary")
            row_count = cur.fetchone()[0]
            status = "PASS" if row_count > 0 else "FAIL"
            checks.append({"check": "weather_daily_summary.row_count", "value": row_count, "status": status})
            logger.info(f"  [{status}] weather_daily_summary row count: {row_count}")

            # Check 2: Data freshness (latest ingestion_date within 2 days)
            cur.execute("SELECT MAX(ingestion_date) FROM weather_daily_summary")
            latest = cur.fetchone()[0]
            if latest:
                from datetime import date
                days_old = (date.today() - latest).days
                status = "PASS" if days_old <= 2 else "WARN"
                checks.append({"check": "weather_daily_summary.freshness", "days_old": days_old, "status": status})
                logger.info(f"  [{status}] weather_daily_summary freshness: {days_old} day(s) old")
            else:
                checks.append({"check": "weather_daily_summary.freshness", "status": "NO_DATA"})
                logger.info("  [NO_DATA] weather_daily_summary - no data yet")

            # Check 3: Null checks on critical columns
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE city_name IS NULL) AS null_city,
                    COUNT(*) FILTER (WHERE avg_temp_c IS NULL) AS null_temp,
                    COUNT(*) FILTER (WHERE forecast_date IS NULL) AS null_date,
                    COUNT(*) AS total
                FROM weather_daily_summary
            """)
            null_city, null_temp, null_date, total = cur.fetchone()
            if total > 0:
                for col_name, null_count in [("city_name", null_city), ("avg_temp_c", null_temp), ("forecast_date", null_date)]:
                    pct = (null_count / total) * 100
                    status = "PASS" if null_count == 0 else "FAIL"
                    checks.append({"check": f"weather_daily_summary.{col_name}_nulls", "null_count": null_count, "pct": pct, "status": status})
                    logger.info(f"  [{status}] {col_name} nulls: {null_count}/{total} ({pct:.1f}%)")

            # Check 4: Temperature range validation
            cur.execute("""
                SELECT MIN(min_temp_c), MAX(max_temp_c)
                FROM weather_daily_summary
            """)
            temp_min, temp_max = cur.fetchone()
            if temp_min is not None:
                status = "PASS" if -80 <= temp_min and temp_max <= 65 else "FAIL"
                checks.append({"check": "weather_daily_summary.temp_range", "min": temp_min, "max": temp_max, "status": status})
                logger.info(f"  [{status}] Temperature range: [{temp_min}°C, {temp_max}°C]")

            # Check 5: Distinct cities count
            cur.execute("SELECT COUNT(DISTINCT city_name) FROM weather_daily_summary")
            city_count = cur.fetchone()[0]
            status = "PASS" if city_count >= 5 else "WARN"
            checks.append({"check": "weather_daily_summary.city_count", "value": city_count, "status": status})
            logger.info(f"  [{status}] Distinct cities: {city_count}")

        conn.close()

    except Exception as e:
        logger.warning(f"PostgreSQL quality checks failed (analytics_db may not exist yet): {e}")
        checks.append({"check": "postgres_connection", "status": "ERROR", "message": str(e)})

    return {"postgres_checks": checks}


# ──────────────────────────────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="openmetadata_quality_monitor",
    default_args=DEFAULT_ARGS,
    description="Data Quality: profiling + quality tests on all tables via OpenMetadata API + PostgreSQL checks",
    schedule_interval="0 8 * * *",  # Daily at 08:00 UTC (after data pipelines)
    start_date=days_ago(1),
    catchup=False,
    params={
        "table_fqns": Param(
            default=DEFAULT_TABLE_FQNS,
            type="array",
            description="List of fully-qualified table names in OpenMetadata (e.g. Trino.iceberg.crypto.crypto_prices)",
        ),
    },
    tags=["openmetadata", "quality", "profiling", "monitoring", "postgres"],
) as dag:

    t_validate = PythonOperator(
        task_id="validate_tables",
        python_callable=validate_tables,
    )

    t_profiles = PythonOperator(
        task_id="check_data_profiles",
        python_callable=check_data_profiles,
    )

    t_setup_tests = PythonOperator(
        task_id="setup_quality_tests",
        python_callable=setup_quality_tests,
    )

    t_trigger = PythonOperator(
        task_id="trigger_profiling",
        python_callable=trigger_profiling,
    )

    t_results = PythonOperator(
        task_id="check_quality_results",
        python_callable=check_quality_results,
    )

    t_pg_quality = PythonOperator(
        task_id="check_postgres_quality",
        python_callable=check_postgres_quality,
    )

    # Flow:
    #   validate_tables -> check_profiles -> setup_tests -> trigger_profiling -> check_results
    #                   \-> check_postgres_quality (parallel, independent)
    t_validate >> t_profiles >> t_setup_tests >> t_trigger >> t_results
    t_validate >> t_pg_quality

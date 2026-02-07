from __future__ import annotations

import logging
import subprocess
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ──────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# DAG Configuration
# ──────────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def cleanup_minio(**context):
    """
    Exécute le script de nettoyage MinIO.
    Nettoie tous les buckets sauf 'warehouse' et garde uniquement
    les dossiers nécessaires dans warehouse/.
    """
    script_path = "/opt/airflow/dags/repo/cleanup_minio.py"
    
    logger.info("Exécution du nettoyage MinIO...")
    logger.info(f"Script: {script_path}")
    
    try:
        # Exécuter le script avec --confirm
        result = subprocess.run(
            [sys.executable, script_path, "--confirm"],
            capture_output=True,
            text=True,
            timeout=600,  # 10 minutes max
        )
        
        # Afficher la sortie
        if result.stdout:
            logger.info("Sortie du script:")
            for line in result.stdout.splitlines():
                logger.info(f"  {line}")
        
        if result.stderr:
            logger.warning("Erreurs du script:")
            for line in result.stderr.splitlines():
                logger.warning(f"  {line}")
        
        if result.returncode != 0:
            raise RuntimeError(
                f"Le script de nettoyage a échoué avec le code {result.returncode}.\n"
                f"Stdout: {result.stdout}\n"
                f"Stderr: {result.stderr}"
            )
        
        logger.info("✓ Nettoyage MinIO terminé avec succès")
        
    except subprocess.TimeoutExpired:
        logger.error("✗ Le script de nettoyage a dépassé le timeout (10 minutes)")
        raise
    except Exception as e:
        logger.error(f"✗ Erreur lors du nettoyage: {e}")
        raise


with DAG(
    dag_id="minio_cleanup",
    default_args=DEFAULT_ARGS,
    description="Nettoyage automatique de MinIO: garde uniquement les buckets et dossiers nécessaires",
    schedule_interval="0 2 * * 0",  # Tous les dimanches à 02:00 UTC
    start_date=days_ago(1),
    catchup=False,
    tags=["minio", "cleanup", "maintenance", "s3"],
) as dag:

    cleanup_task = PythonOperator(
        task_id="cleanup_minio_buckets",
        python_callable=cleanup_minio,
    )

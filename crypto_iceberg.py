from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import json
import boto3
from botocore.client import Config

# Configuration
MINIO_ENDPOINT = "http://minio.minio.svc:9000"  # Interne cluster
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET_NAME = "bronze"
SCRIPT_BUCKET = "scripts"
NESSIE_URI = "http://nessie.nessie.svc:19120/api/v1"
OPENMETADATA_SERVER = "http://openmetadata.openmetadata.svc:8585/api"
OPENMETADATA_JWT = "YOUR_OM_JWT_TOKEN"  # À remplacer par la variable/secret

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_crypto_data(**kwargs):
    """Récupère les données CoinCap et les stocke sur MinIO"""
    url = "https://api.coincap.io/v2/assets?limit=10"
    response = requests.get(url)
    data = response.json()
    
    # Client S3/MinIO
    s3 = boto3.client('s3',
                      endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=MINIO_ACCESS_KEY,
                      aws_secret_access_key=MINIO_SECRET_KEY,
                      config=Config(signature_version='s3v4'),
                      verify=False)
    
    # Sauvegarde des données brutes
    timestamp = kwargs['ts_nodash']
    file_key = f"crypto/prices/{timestamp}/data.json"
    
    # On s'assure que le bucket existe
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
    except:
        pass

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_key,
        Body=json.dumps(data)
    )
    
    # Upload du script Spark (défini ci-dessous)
    try:
        s3.create_bucket(Bucket=SCRIPT_BUCKET)
    except:
        pass
        
    script_content = """
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, explode, col
import sys

def run_spark_job(input_path):
    spark = SparkSession.builder \\
        .appName("CryptoIcebergIngestion") \\
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \\
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \\
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \\
        .config("spark.sql.catalog.nessie.uri", "{nessie_uri}") \\
        .config("spark.sql.catalog.nessie.ref", "main") \\
        .config("spark.sql.catalog.nessie.authentication.type", "NONE") \\
        .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/") \\
        .config("spark.hadoop.fs.s3a.endpoint", "{minio_endpoint}") \\
        .config("spark.hadoop.fs.s3a.access.key", "{access_key}") \\
        .config("spark.hadoop.fs.s3a.secret.key", "{secret_key}") \\
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \\
        .getOrCreate()

    # Lecture des données brutes JSON
    df = spark.read.json(input_path)
    
    # Transformation simple (explode data array)
    exploded_df = df.select(explode(col("data")).alias("coin"), col("timestamp").alias("api_timestamp")) \\
        .select(
            col("coin.id"),
            col("coin.symbol"),
            col("coin.priceUsd").cast("double"),
            col("api_timestamp"),
            current_timestamp().alias("ingestion_time")
        )

    # Création de la table Iceberg si elle n'existe pas
    spark.sql("CREATE TABLE IF NOT EXISTS nessie.crypto_prices (id STRING, symbol STRING, priceUsd DOUBLE, api_timestamp LONG, ingestion_time TIMESTAMP) USING iceberg")

    # Écriture dans Iceberg (Append)
    exploded_df.writeTo("nessie.crypto_prices").append()

    spark.stop()

if __name__ == "__main__":
    input_file = sys.argv[1]
    run_spark_job(input_file)
    """.format(
        nessie_uri=NESSIE_URI,
        minio_endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY
    )
    
    s3.put_object(
        Bucket=SCRIPT_BUCKET,
        Key="ingest_crypto.py",
        Body=script_content
    )
    
    return f"s3a://{BUCKET_NAME}/{file_key}"

with DAG(
    'crypto_iceberg_pipeline',
    default_args=default_args,
    description='Fetch crypto prices and ingest into Iceberg via Spark',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    tags=['crypto', 'iceberg', 'spark'],
    catchup=False
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_prices_and_upload_script',
        python_callable=fetch_crypto_data,
        provide_context=True
    )

    # Note: L'image Spark doit avoir les dépendances :
    # org.apache.iceberg:iceberg-spark-runtime-3.5_2.12
    # org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12
    # org.apache.hadoop:hadoop-aws
    spark_task = SparkKubernetesOperator(
        task_id='spark_ingest_iceberg',
        namespace='spark-operator',
        application_file=None, # On utilise spec direcement ou modèle YAML si besoin, ici on simplifie
        image="ibrahimlahouar/spark-iceberg:3.5.0", # Image custom recommandée
        kubernetes_conn_id="kubernetes_default",
        config_map_mounts={},
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "k8s://https://kubernetes.default.svc",
            "--deploy-mode", "cluster",
            "--name", "crypto-ingest",
            "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,org.apache.hadoop:hadoop-aws:3.3.4,org.open-metadata:openmetadata-spark-agent:1.2.3",
            "--conf", "spark.kubernetes.container.image=apache/spark:3.5.0",
            "--conf", f"spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT}",
            "--conf", "spark.hadoop.fs.s3a.access.key=minioadmin",
            "--conf", "spark.hadoop.fs.s3a.secret.key=minioadmin123",
            "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
            "--conf", "spark.extraListeners=org.openmetadata.spark.agent.OpenMetadataSparkListener",
            "--conf", "spark.openmetadata.transport.hostPort=http://openmetadata.openmetadata.svc:8585",
            "--conf", "spark.openmetadata.identity.authorization.provider=no-auth",
            "--conf", "spark.openmetadata.transport.pipelineName=crypto_iceberg_pipeline",
            "--conf", "spark.openmetadata.transport.pipelineSourceUrl=http://airflow.airflow.svc:8080/tree?dag_id=crypto_iceberg_pipeline",
            f"s3a://{SCRIPT_BUCKET}/ingest_crypto.py",
            "{{ task_instance.xcom_pull(task_ids='fetch_prices_and_upload_script') }}"
        ]
    )

    fetch_task >> spark_task

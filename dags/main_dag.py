# Importaciones de módulos necesarios para crear el DAG y tareas
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator  
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.models import Variable
from datetime import timedelta

# Configuración por defecto del DAG (reintentos, correos, etc.)
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,                      # No depende de ejecuciones anteriores
    "email_on_failure": True,                      # Notifica por email si falla
    "email": ["alertas@empresa.com"],              
    "retries": 1,
    "retry_delay": timedelta(minutes=5),           # Espera antes de reintentar
}

# Definición del DAG
with DAG(
    dag_id="airline_batch_pipeline",               # ID único del DAG
    description="ETL batch con Spark y GCP (Medallion)",
    default_args=default_args,
    schedule_interval="@daily",                    # Corre una vez por día
    start_date=days_ago(1),                        # Fecha de inicio
    catchup=False,                                 # No intenta ponerse al día con ejecuciones pasadas
    tags=["batch", "dataproc", "gcs", "bigquery"],
) as dag:
    
    # ========================
    # VARIABLES DE ENTORNO
    # ========================

    # Entorno actual: "dev" o "main" (se define como variable en Composer)
    ENV = Variable.get("env")

    # Variables comunes
    PROJECT_ID = Variable.get("project_id")
    REGION = Variable.get("region")
    CLUSTER = Variable.get("dataproc_cluster")

    # Buckets de GCS según capa y entorno
    BRONZE_BUCKET = f"airline-{ENV}-bronze-bucket"
    SILVER_BUCKET = f"airline-{ENV}-silver-bucket"
    GOLD_BUCKET   = f"airline-{ENV}-gold-bucket"

    # Ruta al archivo de configuración
    CONFIG_PATH = f"gs://{BRONZE_BUCKET}/config/{ENV}_config.json"
    
    # ================================
    # JOB 1: Ingesta (capa BRONZE)
    # ================================

    bronze_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{BRONZE_BUCKET}/jobs/bronze_ingestion.py",  # Script a ejecutar
            "args": [CONFIG_PATH],  # Ruta al archivo de configuración como argumento
        },
    }

    ingest_bronze = DataprocSubmitJobOperator(
        task_id="ingest_bronze_layer",
        job=bronze_job,
        region=REGION,
        project_id=PROJECT_ID,
    )
    
    # ================================
    # JOB 2: Transformación (SILVER)
    # ================================

    silver_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{SILVER_BUCKET}/jobs/silver_processing.py",
            "args": [CONFIG_PATH],
        },
    }

    process_silver = DataprocSubmitJobOperator(
        task_id="process_silver_layer",
        job=silver_job,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # ===============================
    # JOB 3: Exportación (GOLD/BIQ)
    # ===============================

    gold_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GOLD_BUCKET}/jobs/gold_export.py",
            "args": [CONFIG_PATH],
        },
    }

    export_gold = DataprocSubmitJobOperator(
        task_id="export_gold_layer",
        job=gold_job,
        region=REGION,
        project_id=PROJECT_ID,
    )
    
    ingest_bronze >> process_silver >> export_gold  
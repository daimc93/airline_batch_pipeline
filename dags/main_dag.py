from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
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
    "execution_timeout": timedelta(minutes=30)     # Tiempo máximo por ejecución  
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
    # VALIDACIONES PREVIAS
    # ================================

    # 1. Verifica que exista archivo CSV
    wait_for_csv = GCSObjectExistenceSensor(
        task_id="wait_for_csv_file",
        bucket=BRONZE_BUCKET,
        object="input/passengers.csv",  # ajusta según ubicación real
        timeout=300,  # espera máxima
        poke_interval=30
    )

    # 2. Verifica que la API esté disponible
    check_api = HttpSensor(
        task_id="check_api_availability",
        http_conn_id="api_default",  # definido en Airflow > Connections
        endpoint="health",           # endpoint para check
        method="GET",
        response_check=lambda r: r.status_code == 200,
        poke_interval=30,
        timeout=300
    )

    # 3. Mock: Verificación de conexión a SQL Server
    def check_sql_connection():
        import pyodbc
        # Ideal: leer desde Secret Manager o Variable segura
        conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=host;DATABASE=db;UID=user;PWD=pwd"
        try:
            conn = pyodbc.connect(conn_str, timeout=5)
            conn.close()
        except Exception as e:
            raise RuntimeError(f"Conexión a SQL Server fallida: {e}")

    check_sql = PythonOperator(
        task_id="check_sql_connection",
        python_callable=check_sql_connection,
    )

    # =======================================
    # INGESTA BRONZE (en paralelo)
    # =======================================

    def spark_job(file_name, bucket):
        return {
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{bucket}/jobs/{file_name}",
                "args": [CONFIG_PATH]
            },
        }

    ingest_csv = DataprocSubmitJobOperator(
        task_id="ingest_from_csv",
        job=spark_job("bronze_ingestion_csv.py", BRONZE_BUCKET),
        region=REGION,
        project_id=PROJECT_ID,
    )

    ingest_api = DataprocSubmitJobOperator(
        task_id="ingest_from_api",
        job=spark_job("bronze_ingestion_api.py", BRONZE_BUCKET),
        region=REGION,
        project_id=PROJECT_ID,
    )

    ingest_sql = DataprocSubmitJobOperator(
        task_id="ingest_from_sql",
        job=spark_job("bronze_ingestion_sql.py", BRONZE_BUCKET),
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Unión lógica tras la ingesta
    def log_merge():
        print("Todos los datos fueron ingeridos a capa bronze")

    merge_bronze = PythonOperator(
        task_id="merge_bronze_complete",
        python_callable=log_merge
    )

    # ================================
    # Procesamiento silver
    # ================================

    process_silver = DataprocSubmitJobOperator(
        task_id="process_silver_layer",
        job=spark_job("silver_processing.py", SILVER_BUCKET),
        region=REGION,
        project_id=PROJECT_ID,
    )

    # ================================
    # Carga gold / BigQuery
    # ================================

    export_gold = DataprocSubmitJobOperator(
        task_id="export_gold_layer",
        job=spark_job("gold_export.py", GOLD_BUCKET),
        region=REGION,
        project_id=PROJECT_ID,
    )

    
    [wait_for_csv, check_api, check_sql] >> [ingest_csv, ingest_api, ingest_sql] >> merge_bronze
    merge_bronze >> process_silver >> export_gold
from pyspark.sql import SparkSession
import sys, json
from google.cloud import bigquery

# Leer config
config_path = sys.argv[1]
with open(config_path, "r") as f:
    config = json.load(f)

project_id = config["project_id"]
dataset = config["bigquery"]["dataset"]
bucket = config["buckets"]["bronze"]
source = "csv"

# Leer última fecha desde BigQuery
client = bigquery.Client(project=project_id)
query = f'''
    SELECT ultima_fecha FROM `{project_id}.{dataset}.control_ingesta`
    WHERE fuente = '{source}' ORDER BY ultima_fecha DESC LIMIT 1
'''
result = list(client.query(query).result())
ultima_fecha = result[0].ultima_fecha if result else "1970-01-01"

# Spark
spark = SparkSession.builder.appName("IngestCSVIncremental").getOrCreate()
csv_path = f"gs://{bucket}/input/"
output_path = f"gs://{bucket}/bronze/passengers_csv/"

# Leer CSV y filtrar por nueva fecha
df = spark.read.option("header", True).csv(csv_path)
df = df.withColumn("created_at", df["created_at"].cast("timestamp"))
df_filtrado = df.filter(df["created_at"] > ultima_fecha)

if df_filtrado.count() > 0:
    df_filtrado.write.mode("append").parquet(output_path)
    max_date = df_filtrado.agg({"created_at": "max"}).collect()[0][0]
    insert_query = f"""
        INSERT INTO `{project_id}.{dataset}.control_ingesta` (fuente, ultima_fecha)
        VALUES ('{source}', TIMESTAMP('{max_date}'))
    """
    client.query(insert_query).result()
    print(f"CSV cargado: nuevos registros hasta {max_date}")
else:
    print("No hay registros nuevos en CSV.")

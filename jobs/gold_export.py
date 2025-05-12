from pyspark.sql import SparkSession
import sys, json

# Leer archivo de configuración
config_path = sys.argv[1]
with open(config_path, "r") as f:
    config = json.load(f)

project_id = config["project_id"]
dataset = config["bigquery"]["dataset"]
silver_bucket = config["buckets"]["silver"]

# Crear sesión de Spark con soporte para BigQuery
spark = SparkSession.builder \
    .appName("GoldExport") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.31.0") \
    .getOrCreate()

# Leer datos desde capa Silver
input_path = f"gs://{silver_bucket}/silver/passengers_cleaned/"
df = spark.read.parquet(input_path)

# Crear una columna de fecha (para particionado en BigQuery)
from pyspark.sql.functions import to_date
df = df.withColumn("fecha", to_date(df["created_at"]))

# Definir tabla de destino en BigQuery
table_id = f"{project_id}.{dataset}.passenger_summary"

# Escribir en BigQuery (modo overwrite en pruebas, usar append en producción)
df.write \
    .format("bigquery") \
    .option("table", table_id) \
    .option("partitionField", "fecha") \
    .mode("overwrite") \
    .save()

print("Datos exportados correctamente a BigQuery.")

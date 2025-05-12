from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce
import sys, json

# Leer archivo de configuración
config_path = sys.argv[1]
with open(config_path, "r") as f:
    config = json.load(f)

project_id = config["project_id"]
bucket = config["buckets"]["bronze"]
silver_bucket = config["buckets"]["silver"]

# Iniciar Spark
spark = SparkSession.builder.appName("SilverProcessing").getOrCreate()

# ========================
# 1. Leer Parquets desde Bronze
# ========================
df_csv = spark.read.parquet(f"gs://{bucket}/bronze/passengers_csv/")
df_sql = spark.read.parquet(f"gs://{bucket}/bronze/passengers_sql/")
df_api = spark.read.parquet(f"gs://{bucket}/bronze/passengers_api/")

# ========================
# 2. Seleccionar y renombrar columnas
# ========================

# CSV: pasajeros básicos (check-in)
csv_clean = df_csv.select(
    col("passenger_id").cast("string"),
    col("name"),
    col("flight_id"),
    col("origin"),
    col("destination"),
    col("created_at").cast("timestamp")
)

# SQL: pasajeros y precio del ticket
sql_clean = df_sql.select(
    col("passenger_id").cast("string"),
    col("name").alias("sql_name"),
    col("flight_id").alias("sql_flight_id"),
    col("ticket_price").cast("double"),
    col("fecha_actualizacion").alias("created_at").cast("timestamp")
)

# API: estado del vuelo
api_clean = df_api.select(
    col("flight_id").cast("string"),
    col("weather_conditions"),
    col("updated_at").cast("timestamp")
)

# ========================
# 3. Unir CSV + SQL por passenger_id
# ========================
df_joined = csv_clean.alias("csv").join(
    sql_clean.alias("sql"),
    on="passenger_id",
    how="outer"
)

# Combinar campos (evita duplicados de nombre y flight_id)
df_combined = df_joined.select(
    col("passenger_id"),
    coalesce(col("csv.name"), col("sql.sql_name")).alias("name"),
    coalesce(col("csv.flight_id"), col("sql.sql_flight_id")).alias("flight_id"),
    col("origin"),
    col("destination"),
    col("ticket_price"),
    coalesce(col("csv.created_at"), col("sql.created_at")).alias("created_at")
)

# ========================
# 4. Enriquecer con condiciones meteorológicas desde API
# ========================
df_final = df_combined.join(
    api_clean,
    on="flight_id",
    how="left"
)

# ========================
# 5. Escribir a capa Silver
# ========================
output_path = f"gs://{silver_bucket}/silver/passengers_cleaned/"
df_final.write.mode("overwrite").parquet(output_path)

print("Datos procesados y guardados en capa Silver.")

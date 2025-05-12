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
source = "sql"

client = bigquery.Client(project=project_id)
query = f'''
    SELECT ultima_fecha FROM `{project_id}.{dataset}.control_ingesta`
    WHERE fuente = '{source}' ORDER BY ultima_fecha DESC LIMIT 1
'''
result = list(client.query(query).result())
ultima_fecha = result[0].ultima_fecha if result else "1970-01-01"

# Spark
spark = SparkSession.builder.appName("IngestSQLIncremental").getOrCreate()
jdbc_url = config["sql_server"]["jdbc_url"]
user = config["sql_server"]["user"]
password = config["sql_server"]["password"]
sql_query = f"(SELECT * FROM dbo.passenger_data WHERE fecha_actualizacion > '{ultima_fecha}') as t"
output_path = f"gs://{bucket}/bronze/passengers_sql/"

df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", user) \
    .option("password", password) \
    .option("dbtable", sql_query) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

if df.count() > 0:
    df.write.mode("append").parquet(output_path)
    max_date = df.agg({"fecha_actualizacion": "max"}).collect()[0][0]
    insert_query = f"""
        INSERT INTO `{project_id}.{dataset}.control_ingesta` (fuente, ultima_fecha)
        VALUES ('{source}', TIMESTAMP('{max_date}'))
    """
    client.query(insert_query).result()
    print(f"✅ SQL Server cargado: nuevos registros hasta {max_date}")
else:
    print("✅ No hay registros nuevos en SQL.")


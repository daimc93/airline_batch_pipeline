from pyspark.sql import SparkSession
import sys, json
import requests
import pandas as pd
from google.cloud import bigquery

# Leer config
config_path = sys.argv[1]
with open(config_path, "r") as f:
    config = json.load(f)

project_id = config["project_id"]
dataset = config["bigquery"]["dataset"]
bucket = config["buckets"]["bronze"]
source = "api"

client = bigquery.Client(project=project_id)
query = f'''
    SELECT ultima_fecha FROM `{project_id}.{dataset}.control_ingesta`
    WHERE fuente = '{source}' ORDER BY ultima_fecha DESC LIMIT 1
'''
result = list(client.query(query).result())
ultima_fecha = result[0].ultima_fecha.isoformat() if result else "1970-01-01T00:00:00Z"

# Llamar API
api_url = config["api"]["base_url"]
token = config["api"]["auth_token"]
headers = {"Authorization": f"Bearer {token}"}
response = requests.get(f"{api_url}?updated_after={ultima_fecha}", headers=headers)
data = response.json()

# Convertir a Spark DataFrame
pdf = pd.DataFrame(data)
spark = SparkSession.builder.appName("IngestAPIIncremental").getOrCreate()
df = spark.createDataFrame(pdf)
output_path = f"gs://{bucket}/bronze/passengers_api/"

if df.count() > 0:
    df.write.mode("append").parquet(output_path)
    max_date = df.agg({"updated_at": "max"}).collect()[0][0]
    insert_query = f"""
        INSERT INTO `{project_id}.{dataset}.control_ingesta` (fuente, ultima_fecha)
        VALUES ('{source}', TIMESTAMP('{max_date}'))
    """
    client.query(insert_query).result()
    print(f"API cargada: nuevos registros hasta {max_date}")
else:
    print("No hay registros nuevos desde API.")

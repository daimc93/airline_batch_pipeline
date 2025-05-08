#!/bin/bash

# Uso: bash ci-cd/upload_jobs.sh dev
#       o   bash ci-cd/upload_jobs.sh main

ENVIRONMENT=$1

if [[ -z "$ENVIRONMENT" ]]; then
  echo "Debes especificar el ambiente: dev o main"
  exit 1
fi

CONFIG_FILE="config/${ENVIRONMENT}_config.json"

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "No se encontró el archivo de configuración: $CONFIG_FILE"
  exit 1
fi

# Leer datos desde el archivo JSON
PROJECT_ID=$(jq -r '.project_id' $CONFIG_FILE)
BRONZE_BUCKET=$(jq -r '.buckets.bronze' $CONFIG_FILE)
SILVER_BUCKET=$(jq -r '.buckets.silver' $CONFIG_FILE)
GOLD_BUCKET=$(jq -r '.buckets.gold' $CONFIG_FILE)

echo "Subiendo scripts al ambiente $ENVIRONMENT..."

# Subir scripts PySpark
gsutil cp jobs/bronze_ingestion.py gs://$BRONZE_BUCKET/jobs/
gsutil cp jobs/silver_processing.py gs://$SILVER_BUCKET/jobs/
gsutil cp jobs/gold_export.py gs://$GOLD_BUCKET/jobs/

# Subir archivo de configuración
gsutil cp $CONFIG_FILE gs://$BRONZE_BUCKET/config/

echo "Subida completa al entorno $ENVIRONMENT"


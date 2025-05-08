#!/bin/bash

# CONFIG
#!/bin/bash

# Leer ambiente como argumento
ENVIRONMENT=$1

if [[ -z "$ENVIRONMENT" ]]; then
  echo "Debes especificar el ambiente: dev o main"
  exit 1
fi

# CONFIGURACIÓN SEGÚN AMBIENTE
PROJECT_ID="your-gcp-project-id"
REGION="us-central1"

BRONZE_BUCKET="airline-${ENVIRONMENT}-bronze-bucket"
SILVER_BUCKET="airline-${ENVIRONMENT}-silver-bucket"
GOLD_BUCKET="airline-${ENVIRONMENT}-gold-bucket"
BQ_DATASET="airline_${ENVIRONMENT}_analytics"

echo "Creando recursos para el ambiente: $ENVIRONMENT..."

# Crear buckets
gsutil mb -p $PROJECT_ID -l $REGION gs://$BRONZE_BUCKET/
gsutil mb -p $PROJECT_ID -l $REGION gs://$SILVER_BUCKET/
gsutil mb -p $PROJECT_ID -l $REGION gs://$GOLD_BUCKET/

# Crear dataset de BigQuery
bq --location=$REGION mk --dataset $PROJECT_ID:$BQ_DATASET

echo "Recursos creados exitosamente para el ambiente $ENVIRONMENT"


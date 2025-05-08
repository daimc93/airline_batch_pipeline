#!/bin/bash

# CONFIG
PROJECT_ID="your-gcp-project-id"
REGION="us-central1"
BRONZE_BUCKET="airline-bronze-bucket"
SILVER_BUCKET="airline-silver-bucket"
GOLD_BUCKET="airline-gold-bucket"
BQ_DATASET="airline_analytics"

# CREATE BUCKETS
echo "Creating GCS buckets..."
gsutil mb -p $PROJECT_ID -l $REGION gs://$BRONZE_BUCKET/
gsutil mb -p $PROJECT_ID -l $REGION gs://$SILVER_BUCKET/
gsutil mb -p $PROJECT_ID -l $REGION gs://$GOLD_BUCKET/

# CREATE BIGQUERY DATASET
echo "Creating BigQuery dataset..."
bq --location=$REGION mk --dataset $PROJECT_ID:$BQ_DATASET

echo "Resources created successfully."

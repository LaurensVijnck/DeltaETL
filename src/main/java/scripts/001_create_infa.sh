#!/usr/bin/env bash

# The following resources should be Terraformed.

GCP_PROJECT=geometric-ocean-284614
GCP_LOCATION=EU

BUCKET_NAME=delta-etl
MUTEX_BUCKET_NAME=delta-etl-locks
DATASET_ID=delta_etl
TABLE_ID=deltas

# Create content bucket
gsutil mb -p ${GCP_PROJECT} gs://${BUCKET_NAME}

# Create lock bucket
gsutil mb -p ${GCP_PROJECT} gs://${MUTEX_BUCKET_NAME}

# Set event-based hols for bucket by default
gsutil retention event-default set gs://${MUTEX_BUCKET_NAME}

# Create dataset
bq --location=${GCP_LOCATION} mk \
  --dataset \
  --description "Delta ETL dataset" \
${GCP_PROJECT}:${DATASET_ID}


# Create table
bq mk \
  --table \
  --description "Delta table" \
${GCP_PROJECT}:${DATASET_ID}.${TABLE_ID} \
src/main/java/scripts/delta_schema.json
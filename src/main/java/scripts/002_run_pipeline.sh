#!/usr/bin/env bash

export GOOGLE_APPLICATION_CREDENTIALS="/Users/lvijnck/Desktop/geometric-ocean-284614-77fba73ca7b0.json"

GCP_PROJECT=geometric-ocean-284614
BUCKET_NAME=delta-etl
MUTEX_BUCKET_NAME=delta-etl-locks
DATASET_ID=delta_etl
TABLE_ID=deltas

mvn compile exec:java \
    -Pdataflow-runner \
    -Dexec.mainClass="pipelines.DeltaETL" \
    -Dexec.args="\
    --runner=DataflowRunner \
    --jobName=delta-ETL \
    --update=false \
    --project=${GCP_PROJECT} \
    --region=europe-west1 \
    --zone=europe-west1-b \
    --workerMachineType=n1-standard-1 \
    --diskSizeGb=30 \
    --streaming=true \
    "

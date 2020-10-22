#!/usr/bin/env bash

export GOOGLE_APPLICATION_CREDENTIALS="/Users/lvijnck/Desktop/geometric-ocean-284614-77fba73ca7b0.json"

mvn compile exec:java \
    -Pdataflow-runner \
    -Dexec.mainClass="pipelines.DeltaETL" \
    -Dexec.args="\
    --runner=DataflowRunner \
    --jobName=delta-ETL \
    --update=false \
    --project=geometric-ocean-284614 \
    --region=europe-west1 \
    --zone=europe-west1-b \
    --workerMachineType=n1-standard-1 \
    --diskSizeGb=30 \
    --streaming=true \
    "

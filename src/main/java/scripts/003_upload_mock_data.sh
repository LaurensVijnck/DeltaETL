#!/usr/bin/env bash

BUCKET_NAME=delta-etl

gsutil cp src/main/resources/data/shard-003.csv gs://${BUCKET_NAME}
#!/usr/bin/env bash

BUCKET_NAME=delta-etl

gsutil cp src/main/resources/data/*.csv gs://${BUCKET_NAME}
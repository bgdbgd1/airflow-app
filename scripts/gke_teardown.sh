#!/usr/bin/env bash
# Deletes the cluster (stops most of the cost). Buckets/images are kept; delete them manually if you want.
set -euo pipefail
: "${PROJECT_ID:?set PROJECT_ID}"
gcloud container clusters delete "${CLUSTER:-airflow-cluster}" --region="${REGION:-europe-west1}" --project="$PROJECT_ID" --quiet

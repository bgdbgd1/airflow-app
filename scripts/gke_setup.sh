#!/usr/bin/env bash
# One-shot GKE setup. Creates billable resources: GKE Autopilot cluster,
# 2 GCS buckets, an Artifact Registry repo, and a service account.
# Usage:  PROJECT_ID=my-project ./scripts/gke_setup.sh
set -euo pipefail

: "${PROJECT_ID:?set PROJECT_ID}"
REGION="${REGION:-europe-west1}"
CLUSTER="${CLUSTER:-airflow-cluster}"
REPO="${REPO:-airflow}"
NS=airflow
TAG="${TAG:-$(date +%Y%m%d%H%M%S)}"
RAW_BUCKET="${RAW_BUCKET:-${PROJECT_ID}-airflow-raw}"
PUBLISHED_BUCKET="${PUBLISHED_BUCKET:-${PROJECT_ID}-airflow-published}"
GSA_NAME=airflow-pipeline
GSA_EMAIL="${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}"
cd "$(dirname "$0")/.."
PROCESSOR_REPO="${PROCESSOR_REPO:-../csv-processor}"   # separate repo with the data code
[ -f "$PROCESSOR_REPO/Dockerfile" ] || { echo "csv-processor repo not found at $PROCESSOR_REPO"; exit 1; }

gcloud config set project "$PROJECT_ID"
gcloud services enable container.googleapis.com artifactregistry.googleapis.com storage.googleapis.com

echo ">> Buckets"
gcloud storage buckets describe "gs://$RAW_BUCKET" >/dev/null 2>&1 || gcloud storage buckets create "gs://$RAW_BUCKET" --location="$REGION"
gcloud storage buckets describe "gs://$PUBLISHED_BUCKET" >/dev/null 2>&1 || gcloud storage buckets create "gs://$PUBLISHED_BUCKET" --location="$REGION"

echo ">> Images"
gcloud artifacts repositories describe "$REPO" --location="$REGION" >/dev/null 2>&1 || \
  gcloud artifacts repositories create "$REPO" --repository-format=docker --location="$REGION"
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet
docker build --platform linux/amd64 -t "$REGISTRY/airflow-app:$TAG" .
docker build --platform linux/amd64 -t "$REGISTRY/csv-processor:$TAG" "$PROCESSOR_REPO"
docker push "$REGISTRY/airflow-app:$TAG"
docker push "$REGISTRY/csv-processor:$TAG"

echo ">> Cluster (Autopilot)"
gcloud container clusters describe "$CLUSTER" --region="$REGION" >/dev/null 2>&1 || \
  gcloud container clusters create-auto "$CLUSTER" --region="$REGION"
gcloud container clusters get-credentials "$CLUSTER" --region="$REGION"

echo ">> Service account + Workload Identity"
gcloud iam service-accounts describe "$GSA_EMAIL" >/dev/null 2>&1 || \
  gcloud iam service-accounts create "$GSA_NAME"
for b in "$RAW_BUCKET" "$PUBLISHED_BUCKET"; do
  gcloud storage buckets add-iam-policy-binding "gs://$b" \
    --member="serviceAccount:$GSA_EMAIL" --role=roles/storage.objectAdmin >/dev/null
done
for ksa in pipeline-sa airflow-worker airflow-scheduler; do
  gcloud iam service-accounts add-iam-policy-binding "$GSA_EMAIL" \
    --role=roles/iam.workloadIdentityUser \
    --member="serviceAccount:${PROJECT_ID}.svc.id.goog[${NS}/${ksa}]" >/dev/null
done

echo ">> Namespace, secrets, warehouse DB"
kubectl get ns "$NS" >/dev/null 2>&1 || kubectl create ns "$NS"
if ! kubectl -n "$NS" get secret warehouse-db >/dev/null 2>&1; then
  PW="$(openssl rand -hex 16)"
  kubectl -n "$NS" create secret generic warehouse-db \
    --from-literal=POSTGRES_PASSWORD="$PW" \
    --from-literal=WAREHOUSE_DSN="postgresql://warehouse:${PW}@warehouse-db.${NS}.svc.cluster.local:5432/warehouse"
fi
kubectl -n "$NS" get secret airflow-webserver-secret >/dev/null 2>&1 || \
  kubectl -n "$NS" create secret generic airflow-webserver-secret --from-literal=webserver-secret-key="$(openssl rand -hex 32)"
sed "s|GSA_EMAIL|$GSA_EMAIL|" k8s/rbac.yaml | kubectl apply -f -
kubectl apply -f k8s/warehouse-postgres.yaml

echo ">> Airflow (Helm)"
helm repo add apache-airflow https://airflow.apache.org >/dev/null 2>&1 || true
helm repo update apache-airflow
sed -e "s|AIRFLOW_IMAGE_REPO|$REGISTRY/airflow-app|" \
    -e "s|AIRFLOW_IMAGE_TAG|$TAG|" \
    -e "s|PROCESSOR_IMAGE_FULL|$REGISTRY/csv-processor:$TAG|" \
    -e "s|RAW_BUCKET|$RAW_BUCKET|" \
    -e "s|PUBLISHED_BUCKET|$PUBLISHED_BUCKET|" \
    -e "s|GSA_EMAIL|$GSA_EMAIL|g" \
    k8s/airflow-values.yaml > k8s/.airflow-values.rendered.yaml
helm upgrade --install airflow apache-airflow/airflow -n "$NS" \
  -f k8s/.airflow-values.rendered.yaml --timeout 15m --wait

echo
echo "Done. Open the UI with:"
echo "  kubectl -n $NS port-forward svc/airflow-api-server 8080:8080"
echo "Reports land in gs://$PUBLISHED_BUCKET/daily_revenue/"

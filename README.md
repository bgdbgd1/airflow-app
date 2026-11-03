# airflow-app: orchestration for bucket drop → Postgres → published report

This repo holds orchestration only. All data code lives in the separate **`csv-processor`** repo, which is expected next to this one at `../csv-processor`. Each data step runs that image in its own pod or container, and Airflow waits for it to finish.

```
wait_for_files (sensor, every 30s) → claim_files → init_db [pod]
  → process_file ×N [one pod per CSV] → plan_followups
  → publish_report [pod] → archive_inputs [pod]
```

| Step | Runs where | Code |
|---|---|---|
| `wait_for_files`, `claim_files` | Airflow (lists the bucket) | this repo |
| `plan_followups` | Airflow (turns per-file results into the next pods' arguments) | this repo |
| `init_db`, `process_file`, `publish_report`, `archive_inputs`, `make_samples` | **csv-processor pod/container** | csv-processor |

| | Local (Docker Compose) | Cloud (GKE) |
|---|---|---|
| Launcher | `DockerOperator` (via docker-socket-proxy) | `KubernetesPodOperator` |
| Drop files into | `pipeline-data` volume: `raw/incoming/` | `gs://<project>-airflow-raw/incoming/` |
| Published report | `pipeline-data` volume: `published/revenue/<batch>/` | `gs://<project>-airflow-published/revenue/<batch>/` |

## Run locally
Requires Docker Desktop. This also builds `../csv-processor` as `csv-processor:local`.
```bash
docker compose up -d --build
```
Open http://localhost:8080 and unpause `csv_to_db_pipeline`. Feed it files by triggering the `drop_sample_csvs` DAG, or copy one in:
```bash
docker compose cp my_orders.csv airflow-scheduler:/opt/airflow/data/raw/incoming/
```
After changing processor code, rebuild just that image with `docker compose build csv-processor-image`. Airflow doesn't need a restart.

## Deploy to GKE
**This creates billable resources.**
```bash
PROJECT_ID=my-project ./scripts/gke_setup.sh        # PROCESSOR_REPO=../csv-processor by default
gcloud storage cp my_orders.csv gs://my-project-airflow-raw/incoming/
kubectl -n airflow get pods -w
```
Tear down with `PROJECT_ID=my-project ./scripts/gke_teardown.sh`.

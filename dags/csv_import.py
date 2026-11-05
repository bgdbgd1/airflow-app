"""
csv_import DAG.

Flow:
  wait_for_files (sensor) → for each file, in parallel:
      process_file.run_pod (Kubernetes pod) → process_file.move_to_processed

The sensor waits until at least one .csv file is in data/incoming/ and returns
the names of all of them (oldest first, up to MAX_FILES_PER_RUN). Each file gets
its own pod, but at most MAX_PARALLEL_PODS pods run at the same time; the rest
queue until a slot frees up.

The DAG knows nothing about file types — the csv-jobs image picks the right
processor (orders, customers, ...) from the filename. Adding a new file type
only requires a new processor in the image.

A file is moved to data/processed/ only after its own pod succeeds. If a pod
fails, that file stays in incoming/ and is retried in the next run; the other
files in the batch are not affected.

Only one run is active at a time (max_active_runs=1), so two pods can never
pick the same file. Files that arrive mid-run are picked up by the next run.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import PokeReturnValue, dag, get_current_context, task, task_group
from kubernetes.client import models as k8s

# Same folder (./data in this repo), seen from two places:
#   Airflow mounts it at /opt/airflow/data — override with CSV_IMPORT_DATA_DIR
#     when running the DAG outside the containers (see `dag.test()` at the bottom)
#   job pods mount the kind node's /data at /data (via extraMounts in kind-config.yaml)
AIRFLOW_DATA_DIR = os.environ.get("CSV_IMPORT_DATA_DIR", "/opt/airflow/data")
AIRFLOW_INCOMING_DIR = os.path.join(AIRFLOW_DATA_DIR, "incoming")
AIRFLOW_PROCESSED_DIR = os.path.join(AIRFLOW_DATA_DIR, "processed")
POD_INCOMING_DIR = "/data/incoming"

# Connection string for the app-db Service running inside the kind cluster.
DB_DSN = "postgresql://app:app@app-db.default.svc.cluster.local:5432/app"

MAX_PARALLEL_PODS = 4    # pods running at the same time within a run
MAX_FILES_PER_RUN = 100  # keeps runs short; the rest are picked up by the next run
                         # (must stay below Airflow's max_map_length, default 1024)


def to_pod_job(name: str) -> dict:
    """Per-file inputs for the process_file task group."""
    return {"name": name, "pod_args": ["--file", f"{POD_INCOMING_DIR}/{name}"]}


@dag(
    schedule="@continuous",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["csv-import"],
)
def csv_import():

    @task.sensor(
        task_id="wait_for_files",
        poke_interval=10,   # check every 10 seconds
        timeout=60 * 60,    # stop waiting after 1 hour
        mode="reschedule",  # release the worker slot between checks
    )
    def wait_for_files() -> PokeReturnValue:
        """Done as soon as a .csv file exists; returns all names, oldest first."""
        files = [
            f for f in os.listdir(AIRFLOW_INCOMING_DIR)
            if f.endswith(".csv") and os.path.isfile(os.path.join(AIRFLOW_INCOMING_DIR, f))
        ]
        if not files:
            return PokeReturnValue(is_done=False)
        files.sort(key=lambda f: os.path.getmtime(os.path.join(AIRFLOW_INCOMING_DIR, f)))
        return PokeReturnValue(is_done=True, xcom_value=files[:MAX_FILES_PER_RUN])

    @task(map_index_template="{{ file_name }}")  # label shown in the UI instead of the index
    def move_to_processed(name: str) -> None:
        """Runs only if this file's pod succeeded (default all_success trigger rule)."""
        get_current_context()["file_name"] = name
        os.makedirs(AIRFLOW_PROCESSED_DIR, exist_ok=True)
        target = os.path.join(AIRFLOW_PROCESSED_DIR, name)
        if os.path.exists(target):
            # Don't overwrite an earlier file with the same name.
            stem, ext = os.path.splitext(name)
            target = os.path.join(
                AIRFLOW_PROCESSED_DIR, f"{stem}_{datetime.now():%Y%m%d%H%M%S}{ext}"
            )
        os.replace(os.path.join(AIRFLOW_INCOMING_DIR, name), target)

    # Mapped task group: one copy per file. Inside a copy, move_to_processed
    # depends only on that copy's pod, not on the pods of the other files.
    @task_group
    def process_file(name: str, pod_args: list[str]):
        run_pod = KubernetesPodOperator(
            task_id="run_pod",
            name="csv-import",       # Airflow appends a random suffix per pod
            namespace="default",

            # Must be loaded into kind with:
            #   kind load docker-image csv-jobs:local --name airflow-local
            image="csv-jobs:local",
            image_pull_policy="Never",

            # Only the file path is passed; the image decides how to process it.
            arguments=pod_args,

            env_vars=[
                k8s.V1EnvVar(name="DB_DSN", value=DB_DSN),
            ],
            volumes=[
                k8s.V1Volume(
                    name="data",
                    host_path=k8s.V1HostPathVolumeSource(path="/data"),
                )
            ],
            volume_mounts=[
                k8s.V1VolumeMount(name="data", mount_path="/data"),
            ],

            get_logs=True,           # stream pod logs into the Airflow task log
            on_finish_action="delete_pod",

            # Cap on concurrent pods across all files in this run.
            max_active_tis_per_dagrun=MAX_PARALLEL_PODS,

            # Label shown in the UI instead of the map index: the file name.
            # Rendered after the pod finishes; `task.arguments` is still unresolved
            # at that point, so look the name up in the sensor's list by map index.
            map_index_template=(
                "{{ ti.xcom_pull(task_ids='wait_for_files', map_indexes=None)[ti.map_index] }}"
            ),
        )

        run_pod >> move_to_processed(name)

    process_file.expand_kwargs(wait_for_files().map(to_pod_job))

csv_import()

# FOR DEBUG TOGETHER WITH THE LOCATION VARS AT THE BEGINNING OF THE FILE
# dag = csv_import()
# if __name__ == "__main__":
# # Run the whole DAG in this process, so IDE breakpoints work.
# # Use the "csv_import (dag.test)" PyCharm run config, or see README-style notes
# # in that config for the env vars it needs (AIRFLOW_HOME, KUBECONFIG, CSV_IMPORT_DATA_DIR).
#     dag.test()
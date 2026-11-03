"""Orchestration only. All data work runs in the external `csv-processor` image:
one pod (GKE, KubernetesPodOperator) or container (local, DockerOperator) per step,
and Airflow just launches it and waits for it to finish.

  wait_for_files (sensor) -> claim_files -> [init_db]
    -> process_file xN (one pod per CSV) -> plan_followups
    -> publish_report (pod) -> archive_inputs (pod)

The DAG is "@continuous" with max_active_runs=1, so it reacts to CSVs dropped into
<RAW_URI>/incoming/ (a GCS bucket on GKE).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task

try:
    from airflow.sdk import PokeReturnValue
except ImportError:
    from airflow.sensors.base import PokeReturnValue

MODE = os.getenv("PIPELINE_MODE", "local")  # local | k8s
RAW_URI = os.getenv("RAW_URI", "file:///opt/airflow/data/raw").rstrip("/")
PUBLISHED_URI = os.getenv("PUBLISHED_URI", "file:///opt/airflow/data/published").rstrip("/")
PROCESSOR_IMAGE = os.getenv("PROCESSOR_IMAGE", "csv-processor:local")
POKE_SECONDS = int(os.getenv("POKE_SECONDS", "30"))
MAX_FILES_PER_BATCH = int(os.getenv("MAX_FILES_PER_BATCH", "50"))


# ---------------------------------------------------------------- container launching
def _launcher():
    """(operator class, shared kwargs, name of the 'arguments' field) for the current mode."""
    if MODE == "k8s":
        from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
        from kubernetes.client import models as k8s

        return KubernetesPodOperator, dict(
            namespace=os.getenv("K8S_NAMESPACE", "airflow"),
            image=PROCESSOR_IMAGE,
            image_pull_policy="IfNotPresent",
            service_account_name=os.getenv("PROCESSOR_KSA", "pipeline-sa"),
            env_from=[k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="warehouse-db"))],
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": "250m", "memory": "512Mi"}, limits={"memory": "1Gi"}
            ),
            do_xcom_push=True,          # reads /airflow/xcom/return.json
            get_logs=True,
            on_finish_action="delete_pod",
            startup_timeout_seconds=600,  # Autopilot may need to add a node first
        ), "arguments"

    from airflow.providers.docker.operators.docker import DockerOperator
    from docker.types import Mount

    return DockerOperator, dict(
        image=PROCESSOR_IMAGE,
        docker_url=os.getenv("DOCKER_URL", "tcp://docker-proxy:2375"),
        network_mode=os.getenv("DOCKER_NETWORK", "airflow-net"),
        mounts=[Mount(source="pipeline-data", target="/opt/airflow/data", type="volume")],
        environment={"WAREHOUSE_DSN": os.getenv("WAREHOUSE_DSN", "")},
        user="50000:0",
        auto_remove="success",
        mount_tmp_dir=False,
        do_xcom_push=True,              # last stdout line = JSON result
    ), "command"


def run_step(task_id: str, args, **kw):
    """One container/pod running `csv-processor <args...>`."""
    cls, common, field = _launcher()
    extra = {"name": task_id.replace("_", "-")} if MODE == "k8s" else {}
    return cls(task_id=task_id, **{field: args}, **common, **extra, **kw)


def run_step_per_item(task_id: str, args_list, **kw):
    """Dynamically mapped: one container/pod per element of args_list."""
    cls, common, field = _launcher()
    extra = {"name": task_id.replace("_", "-")} if MODE == "k8s" else {}
    return cls.partial(task_id=task_id, **common, **extra, **kw).expand(**{field: args_list})


def _as_dict(result):
    return json.loads(result) if isinstance(result, str) else result


# ---------------------------------------------------------------- file discovery
def _list_incoming() -> list[str]:
    import fsspec
    fs, root = fsspec.core.url_to_fs(f"{RAW_URI}/incoming")
    fs.invalidate_cache()
    if not fs.exists(root):
        return []
    paths = sorted(p for p in fs.find(root) if p.lower().endswith(".csv"))
    return [fs.unstrip_protocol(p) for p in paths[:MAX_FILES_PER_BATCH]]


@dag(
    schedule="@continuous",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["csv", "warehouse", "file-triggered", MODE],
)
def csv_to_db_pipeline():

    @task.sensor(
        poke_interval=POKE_SECONDS,
        timeout=6 * 60 * 60,
        mode="poke" if MODE == "k8s" else "reschedule",
        soft_fail=True,
        retries=0,
    )
    def wait_for_files() -> PokeReturnValue:
        files = _list_incoming()
        return PokeReturnValue(is_done=bool(files), xcom_value=files)

    @task
    def claim_files(files: list[str]) -> list[str]:
        return _list_incoming() or files

    files = claim_files(wait_for_files())
    init_db = run_step("init_db", ["init-db"])

    processed = run_step_per_item(
        "process_file",
        files.map(lambda uri: ["process", uri]),
        max_active_tis_per_dag=4,
    )
    files >> init_db >> processed

    @task(trigger_rule="all_done", multiple_outputs=True)
    def plan_followups(files: list[str] | None, results) -> dict:
        """Turn per-file results into arguments for the publish and archive pods."""
        if not files:
            raise AirflowSkipException("no files in this batch")
        results = [_as_dict(r) for r in (results or []) if r]
        ok = [r["file"] for r in results]
        batch = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        summary = {
            "batch": batch,
            "files_total": len(files),
            "files_ok": len(ok),
            "rows_in": sum(r["rows_in"] for r in results),
            "rows_loaded": sum(r["rows_loaded"] for r in results),
            "rows_rejected": sum(r["rows_rejected"] for r in results),
        }
        return {
            "publish_args": ["publish", "--files", json.dumps(ok), "--summary", json.dumps(summary),
                             "--out", f"{PUBLISHED_URI}/revenue/{batch}"],
            "archive_args": ["archive", "--raw", RAW_URI, "--batch", batch,
                             "--files", json.dumps(files), "--ok", json.dumps(ok)],
        }

    plan = plan_followups(files, processed.output)
    publish = run_step("publish_report", plan["publish_args"])
    # Archive even if publishing failed, but not when the batch was empty (plan skipped).
    archive = run_step("archive_inputs", plan["archive_args"], trigger_rule="none_skipped")
    publish >> archive


csv_to_db_pipeline()


@dag(schedule=None, start_date=datetime(2026, 11, 1), catchup=False, tags=["csv", "helper"])
def drop_sample_csvs():
    """Manually triggered: drop sample CSVs into the watched folder."""
    run_step("make_samples", ["make-samples", "--raw", RAW_URI, "--files", "3", "--rows", "200"])


drop_sample_csvs()

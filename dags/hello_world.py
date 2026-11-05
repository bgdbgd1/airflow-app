"""
Step 1 placeholder DAG.
Goal: prove that Airflow is running and can execute a task.
Nothing here touches Kubernetes or databases yet — that comes later.
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    schedule=None,          # manual trigger only — no automatic schedule yet
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["step-1"],
)
def hello_world():

    @task
    def say_hello():
        print("Airflow is working!")

    say_hello()


hello_world()
"""
Airflow event-driven DAG template for DRE quality gating.

Flow:
  file sensor -> call DRE evaluate endpoint -> branch on status
"""

from __future__ import annotations

import json
import os
from datetime import datetime

import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor


DRE_API_URL = os.getenv("DRE_API_URL", "http://localhost:8000").rstrip("/")
LANDING_TEMPLATE = os.getenv("DRE_AIRFLOW_FILE_TEMPLATE", "data/landing/orders_{{ ds }}.csv")
DATASET_NAME = os.getenv("DRE_AIRFLOW_DATASET", "orders")


def _call_dre(**context):
    response = requests.post(f"{DRE_API_URL}/evaluate/{DATASET_NAME}", timeout=120)
    response.raise_for_status()
    payload = response.json()
    context["ti"].xcom_push(key="dre_result", value=payload)
    return payload


def _branch_on_verdict(**context):
    payload = context["ti"].xcom_pull(task_ids="dre_evaluate", key="dre_result") or {}
    status = str(payload.get("status", payload.get("mode", "UNKNOWN"))).upper()

    if status == "BLOCKED":
        return "blocked_path"
    if status in {"PAUSED_HITL", "HITL"}:
        return "hitl_path"
    return "pass_path"


def _raise_blocked(**context):
    payload = context["ti"].xcom_pull(task_ids="dre_evaluate", key="dre_result") or {}
    raise AirflowException(f"DRE blocked dataset {DATASET_NAME}: {json.dumps(payload)}")


with DAG(
    dag_id="dre_event_driven_gate",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dre", "quality", "event-driven"],
) as dag:
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=LANDING_TEMPLATE,
        poke_interval=30,
        timeout=3600,
    )

    dre_evaluate = PythonOperator(
        task_id="dre_evaluate",
        python_callable=_call_dre,
    )

    verdict_branch = BranchPythonOperator(
        task_id="verdict_branch",
        python_callable=_branch_on_verdict,
    )

    pass_path = EmptyOperator(task_id="pass_path")
    hitl_path = EmptyOperator(task_id="hitl_path")

    blocked_path = PythonOperator(
        task_id="blocked_path",
        python_callable=_raise_blocked,
    )

    wait_for_file >> dre_evaluate >> verdict_branch
    verdict_branch >> [pass_path, hitl_path, blocked_path]


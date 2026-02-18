"""
Dagster event-driven orchestration for DRE.

This sensor watches `data/landing/` and triggers a DRE evaluation run for each new file.
It mirrors a production event-driven quality gate:
  file arrival -> evaluate -> pass/warn/block handling
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Dict

import requests
from dagster import (
    DefaultSensorStatus,
    Failure,
    OpExecutionContext,
    RunRequest,
    SkipReason,
    job,
    op,
    sensor,
)


WATCH_DIR = Path(os.getenv("DRE_WATCH_DIR", "data/landing"))
DRE_API_URL = os.getenv("DRE_API_URL", "http://localhost:8000").rstrip("/")
SUPPORTED_SUFFIXES = {".csv", ".parquet", ".json"}


def _extract_dataset_name(file_path: Path) -> str:
    name = file_path.stem
    parts = name.split("_")
    if len(parts) > 1:
        token = parts[1]
        if token.replace("-", "").replace(":", "").isdigit():
            return parts[0]
        if token in ["latest", "current", "new", "final", "v1", "v2"]:
            return parts[0]
    return name


def _is_supported(path: Path) -> bool:
    return (
        path.is_file()
        and path.suffix.lower() in SUPPORTED_SUFFIXES
        and ".verdict." not in path.name
    )


@op
def run_dre_evaluation(context: OpExecutionContext) -> Dict[str, object]:
    dataset_name = context.op_config["dataset_name"]
    source_file = context.op_config["source_file"]

    context.log.info(f"Triggering DRE evaluate for dataset={dataset_name} source={source_file}")
    response = requests.post(f"{DRE_API_URL}/evaluate/{dataset_name}", timeout=120)
    response.raise_for_status()
    payload = response.json()

    status = str(payload.get("status", payload.get("mode", "UNKNOWN"))).upper()
    if status == "BLOCKED":
        raise Failure(
            description=f"DRE gate blocked dataset {dataset_name}: {payload.get('reason', 'unknown')}",
            metadata={"dataset_name": dataset_name, "source_file": source_file},
        )

    if status in {"PAUSED_HITL", "HITL"}:
        raise Failure(
            description=f"DRE requires HITL approval for dataset {dataset_name}.",
            metadata={"dataset_name": dataset_name, "source_file": source_file},
        )

    context.log.info(f"DRE evaluation status for {dataset_name}: {status}")
    return payload


@job
def dre_event_gate_job():
    run_dre_evaluation()


@sensor(job=dre_event_gate_job, default_status=DefaultSensorStatus.RUNNING)
def landing_file_sensor(context):
    watch_dir = WATCH_DIR
    watch_dir.mkdir(parents=True, exist_ok=True)

    cursor = context.cursor or "{}"
    try:
        seen = json.loads(cursor)
    except Exception:
        seen = {}

    if not isinstance(seen, dict):
        seen = {}

    run_requests = []
    new_seen = dict(seen)

    for file_path in sorted(watch_dir.glob("*")):
        if not _is_supported(file_path):
            continue

        stat = file_path.stat()
        fingerprint = hashlib.sha1(
            f"{file_path.resolve()}:{stat.st_mtime_ns}:{stat.st_size}".encode("utf-8")
        ).hexdigest()
        file_key = str(file_path.resolve())

        if new_seen.get(file_key) == fingerprint:
            continue

        dataset_name = _extract_dataset_name(file_path)
        run_requests.append(
            RunRequest(
                run_key=fingerprint,
                run_config={
                    "ops": {
                        "run_dre_evaluation": {
                            "config": {
                                "dataset_name": dataset_name,
                                "source_file": str(file_path),
                            }
                        }
                    }
                },
                tags={
                    "dre/event_source": "landing_file_sensor",
                    "dre/dataset": dataset_name,
                    "dre/source_file": str(file_path),
                },
            )
        )
        new_seen[file_key] = fingerprint

    # Garbage collect entries for files that no longer exist.
    existing = {str(p.resolve()) for p in watch_dir.glob("*") if p.exists()}
    for key in list(new_seen.keys()):
        if key not in existing:
            new_seen.pop(key, None)

    context.update_cursor(json.dumps(new_seen))

    if not run_requests:
        return SkipReason("No new landing files detected.")

    return run_requests


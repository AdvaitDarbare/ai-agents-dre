#!/usr/bin/env python3
"""
Continuous single-dataset pipeline simulator for DRE.

This script emits sequential CSV batches for ONE dataset to simulate real operations:
1) baseline normal runs
2) volume anomaly spike
3) YAML constraint violations
4) schema mismatch
5) recovery run

It is designed for watcher-driven flows (drop files into data/landing), but can
optionally trigger API evaluate for each batch.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
import yaml


@dataclass(frozen=True)
class BatchPlan:
    key: str
    rows: int
    description: str
    expectation: str


def build_contract(dataset_name: str) -> Dict:
    return {
        "kind": "DataContract",
        "apiVersion": "v3.1.0",
        "id": f"urn:datacontract:{dataset_name}",
        "table_name": dataset_name,
        "description": "Single-dataset continuous pipeline simulation contract",
        "info": {
            "title": dataset_name,
            "owner": "Pipeline QA",
            "domain": "Simulation",
            "lifecycle": "active",
            "version": "1.0.0",
        },
        "quality": {
            "min_rows": 80,
            "anomaly_thresholds": {
                "z_score_warning": 2.5,
                "z_score_critical": 3.0,
                "quality_score_warn": 80,
                "quality_score_block": 50,
            },
            "slos": {
                "min_quality_score": 85,
                "max_anomaly_count": 0,
            },
            "custom_checks": [
                {
                    "name": "ok_amount_cap",
                    "sql_condition": "status != 'ok' OR amount <= 1000",
                    "severity": "error",
                }
            ],
        },
        "columns": [
            {
                "name": "event_id",
                "data_type": "varchar",
                "nullable": False,
                "isPrimaryKey": True,
                "description": "Unique event identifier.",
            },
            {
                "name": "user_id",
                "data_type": "varchar",
                "nullable": False,
                "description": "User identifier.",
            },
            {
                "name": "event_time",
                "data_type": "timestamp",
                "nullable": False,
                "description": "Event timestamp.",
            },
            {
                "name": "amount",
                "data_type": "double",
                "nullable": False,
                "min_value": 0.0,
                "max_value": 5000.0,
                "description": "Transaction amount.",
            },
            {
                "name": "status",
                "data_type": "varchar",
                "nullable": False,
                "allowed_values": ["ok", "pending", "failed"],
                "description": "Event status.",
            },
        ],
    }


def normal_frame(*, rows: int, seq_start: int, base_ts: datetime, rng: random.Random) -> pd.DataFrame:
    records: List[Dict[str, object]] = []
    statuses = ["ok", "pending", "failed"]
    for i in range(rows):
        event_idx = seq_start + i
        status = statuses[event_idx % len(statuses)]
        # Keep baseline distribution stable across batches so post-recovery runs
        # are deterministic and not accidentally flagged by percentile drift noise.
        amount = round(120.0 + ((event_idx % 90) * 8.1), 2)
        records.append(
            {
                "event_id": f"EVT{event_idx:08d}",
                "user_id": f"USR{(event_idx % 5000):06d}",
                "event_time": (base_ts + timedelta(seconds=i * 12)).strftime("%Y-%m-%d %H:%M:%S"),
                "amount": amount,
                "status": status,
            }
        )
    return pd.DataFrame.from_records(records)


def apply_constraint_violations(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Introduce controlled invalid statuses + invalid numeric range.
    out.loc[out.index % 3 == 0, "status"] = "unknown"
    out.loc[out.index % 4 == 0, "amount"] = -25.0
    return out


def apply_schema_mismatch(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Force hard schema failure: remove required column.
    out = out.drop(columns=["status"])
    return out


def write_csv_atomic(df: pd.DataFrame, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(target.suffix + ".tmp")
    df.to_csv(temp, index=False)
    temp.replace(target)


def find_verdict_file(landing_dir: Path, basename: str) -> Optional[Path]:
    candidate = landing_dir / f"{basename}.verdict.json"
    if candidate.exists():
        return candidate

    root = landing_dir.parent
    for p in root.rglob(f"{basename}.verdict.json"):
        if p.is_file():
            return p
    return None


def wait_for_verdict(landing_dir: Path, filename: str, timeout_seconds: int) -> Optional[Dict]:
    basename = filename
    start = time.time()
    while time.time() - start <= timeout_seconds:
        verdict_file = find_verdict_file(landing_dir, basename)
        if verdict_file:
            try:
                return json.loads(verdict_file.read_text())
            except Exception:
                return None
        time.sleep(1)
    return None


def maybe_trigger_api(api_base: Optional[str], dataset_name: str) -> None:
    if not api_base:
        return
    url = f"{api_base.rstrip('/')}/evaluate/{dataset_name}"
    resp = requests.post(url, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(f"API evaluate failed ({resp.status_code}): {resp.text}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Simulate a continuous one-dataset DRE pipeline.")
    parser.add_argument("--dataset", default="continuous_pipeline_demo", help="Dataset name (single stream).")
    parser.add_argument("--landing-dir", default="data/landing", help="Landing directory to drop files into.")
    parser.add_argument("--contracts-dir", default="config/expectations", help="Contract directory.")
    parser.add_argument("--interval-seconds", type=float, default=4.0, help="Seconds between batches.")
    parser.add_argument("--wait-for-verdict", action="store_true", help="Wait for *.verdict.json after each drop.")
    parser.add_argument("--verdict-timeout", type=int, default=90, help="Per-batch verdict wait timeout seconds.")
    parser.add_argument("--api-base", default="", help="Optional API base (e.g., http://127.0.0.1:8000).")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducible data.")
    parser.add_argument("--dry-run", action="store_true", help="Print plan only; do not write files.")
    args = parser.parse_args()

    dataset_name = str(args.dataset).strip()
    if not dataset_name:
        raise SystemExit("--dataset is required")

    landing_dir = Path(args.landing_dir)
    contracts_dir = Path(args.contracts_dir)
    contract_path = contracts_dir / f"{dataset_name}.yaml"

    plan = [
        BatchPlan("baseline_001", 120, "Baseline steady run 1", "PASSED"),
        BatchPlan("baseline_002", 118, "Baseline steady run 2", "PASSED"),
        BatchPlan("baseline_003", 122, "Baseline steady run 3", "PASSED"),
        BatchPlan("volume_spike", 360, "Volume spike for anomaly detection", "WARNING (anomaly)"),
        BatchPlan("constraint_violation", 120, "YAML constraint violations", "WARNING or BLOCKED"),
        BatchPlan("schema_mismatch", 120, "Missing required column", "BLOCKED"),
        BatchPlan("recovery", 121, "Recovery with valid schema/data", "PASSED + Doris load"),
        BatchPlan("post_recovery_steady", 119, "Healthy flow continues after recovery", "PASSED + Doris load"),
    ]

    print("\n=== Continuous Pipeline Simulation Plan ===")
    print(f"Dataset: {dataset_name}")
    print(f"Landing Dir: {landing_dir}")
    print(f"Contract: {contract_path}")
    for idx, step in enumerate(plan, start=1):
        print(f"{idx}. {step.key:<20} rows={step.rows:<4} expected={step.expectation}  ({step.description})")

    if args.dry_run:
        print("\nDry run only. No files written.")
        return 0

    # Always write a deterministic contract for this dedicated simulation dataset.
    contracts_dir.mkdir(parents=True, exist_ok=True)
    contract_payload = build_contract(dataset_name)
    contract_path.write_text(yaml.safe_dump(contract_payload, sort_keys=False))
    print(f"\n✅ Contract written: {contract_path}")

    rng = random.Random(args.seed)
    seq = 1
    current_id = 1
    simulation_start = datetime.now() - timedelta(minutes=40)

    for step in plan:
        base_df = normal_frame(rows=step.rows, seq_start=current_id, base_ts=simulation_start, rng=rng)
        current_id += step.rows + 1
        simulation_start = simulation_start + timedelta(minutes=5)

        if step.key == "constraint_violation":
            df = apply_constraint_violations(base_df)
        elif step.key == "schema_mismatch":
            df = apply_schema_mismatch(base_df)
        else:
            df = base_df

        filename = f"{dataset_name}_{seq:03d}_{step.key}.csv"
        target = landing_dir / filename
        write_csv_atomic(df, target)
        print(f"\n📥 Dropped: {target}  ({len(df)} rows) :: {step.description}")

        if args.api_base.strip():
            maybe_trigger_api(args.api_base.strip(), dataset_name)
            print("   ↳ Triggered API evaluate")

        if args.wait_for_verdict:
            verdict = wait_for_verdict(landing_dir, filename, timeout_seconds=max(5, args.verdict_timeout))
            if verdict:
                status = verdict.get("status", "UNKNOWN")
                reason = str(verdict.get("reason", ""))
                load_status = verdict.get("load_status")
                if len(reason) > 180:
                    reason = reason[:177] + "..."
                print(f"   ↳ Verdict: {status} | {reason}")
                if load_status is not None:
                    print(f"   ↳ Load: {load_status}")
            else:
                print("   ↳ Verdict: timeout (watcher/API may not be running)")

        seq += 1
        time.sleep(max(0.0, args.interval_seconds))

    print("\n✅ Simulation complete.")
    print("Next checks:")
    print(f"- GET /history/{dataset_name}?limit=20")
    print(f"- GET /metrics/{dataset_name}/timeseries?metric=row_count&limit=20")
    print(f"- GET /incidents?dataset_name={dataset_name}&limit=20")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

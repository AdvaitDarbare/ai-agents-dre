#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd


def build_signup_funnel_rows() -> List[Dict[str, object]]:
    channels = ["organic", "paid", "referral"]
    rows: List[Dict[str, object]] = []
    start = datetime(2026, 1, 1)
    for idx in range(21):
        signups = 120 + (idx * 7) + (15 if idx % 5 == 0 else 0)
        verified = signups - (8 + (idx % 4))
        rows.append(
            {
                "event_date": (start + timedelta(days=idx)).strftime("%Y-%m-%d"),
                "channel": channels[idx % len(channels)],
                "campaign_id": f"CMP-{100 + (idx % 9)}",
                "signups": signups,
                "verified_signups": verified,
            }
        )
    return rows


def build_support_ticket_rows() -> List[Dict[str, object]]:
    priorities = ["low", "medium", "high", "urgent"]
    statuses = ["open", "in_progress", "resolved"]
    tiers = ["free", "pro", "enterprise"]
    rows: List[Dict[str, object]] = []
    start = datetime(2026, 2, 1, 8, 0, 0)
    for idx in range(36):
        status = statuses[idx % len(statuses)]
        resolution_minutes = None if status != "resolved" else (25 + (idx % 8) * 10)
        rows.append(
            {
                "ticket_id": f"TKT-{1000 + idx}",
                "priority": priorities[idx % len(priorities)],
                "status": status,
                "opened_at": (start + timedelta(minutes=45 * idx)).isoformat(),
                "resolution_minutes": resolution_minutes,
                "customer_tier": tiers[idx % len(tiers)],
            }
        )
    return rows


def build_sensor_rows() -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    start = datetime(2026, 2, 10, 0, 0, 0)
    devices = ["sensor-a", "sensor-b", "sensor-c"]
    for idx in range(72):
        battery = max(35.0, 100.0 - (idx * 0.8))
        rows.append(
            {
                "device_id": devices[idx % len(devices)],
                "observed_at": start + timedelta(hours=idx),
                "temperature_c": round(18.0 + ((idx % 24) * 0.35), 2),
                "humidity_pct": round(40.0 + ((idx % 12) * 2.1), 2),
                "battery_pct": round(battery, 2),
                "status": "ok" if idx % 19 else "warn",
            }
        )
    return rows


def cleanup_legacy_artifacts() -> int:
    patterns = [
        ("data/staged_connector", "orders_*.csv"),
        ("data/history", "orders_*.json"),
        ("data/history", "stable_*.json"),
        ("data/history", "stables_*.json"),
    ]
    removed = 0
    for root, pattern in patterns:
        root_path = Path(root)
        if not root_path.exists():
            continue
        candidates = root_path.rglob(pattern) if "**" in pattern else root_path.rglob(pattern)
        for path in candidates:
            if not path.is_file():
                continue
            path.unlink()
            removed += 1
    return removed


def seed(output_dir: Path) -> Dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)

    signup_rows = build_signup_funnel_rows()
    tickets_rows = build_support_ticket_rows()
    sensor_rows = build_sensor_rows()

    signup_path = output_dir / "demo_signup_funnel.csv"
    tickets_path = output_dir / "demo_support_tickets.json"
    sensor_path = output_dir / "demo_sensor_telemetry.parquet"

    pd.DataFrame(signup_rows).to_csv(signup_path, index=False)
    with tickets_path.open("w", encoding="utf-8") as f:
        json.dump(tickets_rows, f, indent=2)
    pd.DataFrame(sensor_rows).to_parquet(sensor_path, index=False)

    return {
        "csv": str(signup_path),
        "json": str(tickets_path),
        "parquet": str(sensor_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Seed mixed-format dummy datasets for DRE demos."
    )
    parser.add_argument(
        "--output-dir",
        default="data/test",
        help="Directory where dataset files are written (default: data/test)",
    )
    parser.add_argument(
        "--cleanup-legacy",
        action="store_true",
        help="Remove old local orders/stable demo artifacts from staged/history folders.",
    )
    args = parser.parse_args()

    outputs = seed(Path(args.output_dir))
    print("Seeded dummy datasets:")
    for fmt, path in outputs.items():
        print(f"  - {fmt}: {path}")

    if args.cleanup_legacy:
        removed = cleanup_legacy_artifacts()
        print(f"Removed legacy artifacts: {removed}")

    print("Contracts expected:")
    print("  - config/expectations/demo_signup_funnel.yaml")
    print("  - config/expectations/demo_support_tickets.yaml")
    print("  - config/expectations/demo_sensor_telemetry.yaml")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from src.tools.monitor_backtesting import MonitorBacktestingHarness


def main() -> None:
    parser = argparse.ArgumentParser(description="Run monitor backtesting for anomaly FP/FN tuning")
    parser.add_argument("dataset_name", help="Dataset name")
    parser.add_argument("--metric", default="row_count", help="Metric name (default: row_count)")
    parser.add_argument("--limit", type=int, default=500, help="Max history points")
    args = parser.parse_args()

    harness = MonitorBacktestingHarness()
    report = harness.run(dataset_name=args.dataset_name, metric_name=args.metric, limit=args.limit)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()

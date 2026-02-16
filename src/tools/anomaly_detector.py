"""
Anomaly Detector Tool - Statistical AI for Data Observability

    This tool implements statistical anomaly detection using industrial observability patterns.
    It uses PostgreSQL as a persistent memory store to learn historical patterns and detect:
    1. Volume anomalies (row count shifts)
    2. Distribution shifts (null rate changes, mean value changes)

    The core logic uses a Z-Score algorithm with seasonality awareness:
    - It compares today's metrics against previous data for the SAME day of the week.
    - Fallback to global history if seasonal history is insufficient (Cold Start).

    This tool provides the "Mathematical Intuition" for the Agentic Platform.
    """

import uuid
import json
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple, Union

from src.utils.database import get_connection, init_tables


class AnomalyDetector:
    """
    The Statistical Engine - Detects anomalies using Z-Score and Seasonality.
    Uses PostgreSQL for persistent metric storage.
    """

    def __init__(self):
        """Initialize the AnomalyDetector — ensures tables exist in PostgreSQL."""
        init_tables()
        self.z_threshold = 3.0
        self.robust_z_threshold = 3.5
        self.iqr_multiplier = 1.5
        self.max_profiled_columns = 40

    def save_run_to_history(self, dataset_name: str, status: str,
                           quality_score: float, anomaly_count: int,
                           z_score_max: float, reason: str,
                           duration_ms: int, run_id: str = None, dimension_scores: dict = None,
                           full_verdict: dict = None) -> str:
        """Save a run outcome to the run_history system table."""
        if run_id is None:
            run_id = str(uuid.uuid4())

        import json
        dimension_scores_json = json.dumps(dimension_scores) if dimension_scores else None
        full_verdict_json = json.dumps(full_verdict) if full_verdict else None

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO run_history
                    (run_id, timestamp, dataset_name, status, quality_score,
                     anomaly_count, z_score_max, reason, duration_ms, dimension_scores, full_verdict)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (run_id, datetime.now(timezone.utc), dataset_name, status,
                      quality_score, anomaly_count, z_score_max, reason, duration_ms,
                      dimension_scores_json, full_verdict_json))
        return run_id

    def save_learned_threshold(self, dataset_name: str, metric_name: str,
                               mean: float, std: float,
                               baseline_type: str, sample_count: int):
        """Cache a learned threshold so agents don't re-learn every run."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Upsert using ON CONFLICT
                cur.execute("""
                    INSERT INTO learned_thresholds
                    (dataset_name, metric_name, baseline_mean, baseline_std,
                     baseline_type, last_updated, sample_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (dataset_name, metric_name)
                    DO UPDATE SET
                        baseline_mean = EXCLUDED.baseline_mean,
                        baseline_std = EXCLUDED.baseline_std,
                        baseline_type = EXCLUDED.baseline_type,
                        last_updated = EXCLUDED.last_updated,
                        sample_count = EXCLUDED.sample_count
                """, (dataset_name, metric_name, mean, std,
                      baseline_type, datetime.now(), sample_count))

    def update_dataset_registry(self, dataset_name: str, contract_path: str,
                                lifecycle: str, criticality: str,
                                status: str = None, file_mtime: float = None):
        """Update or insert a dataset's registry entry."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT scan_count FROM dataset_registry WHERE dataset_name = %s",
                    (dataset_name,)
                )
                existing = cur.fetchone()

                if existing:
                    scan_count = (existing[0] or 0) + 1
                    cur.execute("""
                        UPDATE dataset_registry SET
                            contract_path = %s,
                            lifecycle = %s,
                            criticality = %s,
                            last_scanned = %s,
                            last_status = COALESCE(%s, last_status),
                            last_file_mtime = COALESCE(%s, last_file_mtime),
                            scan_count = %s
                        WHERE dataset_name = %s
                    """, (contract_path, lifecycle, criticality,
                          datetime.now(), status, file_mtime, scan_count, dataset_name))
                else:
                    cur.execute("""
                        INSERT INTO dataset_registry
                        (dataset_name, contract_path, lifecycle, criticality,
                         last_scanned, last_status, last_file_mtime, scan_count)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, 1)
                    """, (dataset_name, contract_path, lifecycle, criticality,
                          datetime.now(), status, file_mtime))

    def save_run_metrics(self, dataset_name: str, metrics_dict: Dict[str, Union[float, Dict[str, Any]]],
                         run_id: Optional[str] = None) -> str:
        """
        Save metrics for a specific run to history.

        Args:
            dataset_name: Name of the dataset (e.g., 'transactions')
            metrics_dict: Dictionary of metrics.
                Supports legacy float values and rich dict payloads:
                {
                  "row_count": 100,
                  "null_rate_email": {
                    "value": 0.05,
                    "metric_group": "completeness",
                    "column_name": "email",
                    "segment": "global",
                    "tags": {"source": "profile"}
                  }
                }
            run_id: Optional existing run_id to associate metrics with.

        Returns:
            str: The run_id used for this batch.
        """
        if not run_id:
            run_id = str(uuid.uuid4())
        timestamp = datetime.now(timezone.utc)
        day_of_week = timestamp.weekday()

        normalized = self._normalize_metric_inputs(metrics_dict)

        with get_connection() as conn:
            with conn.cursor() as cur:
                for metric_name, payload in normalized.items():
                    cur.execute("""
                        INSERT INTO metric_history
                        (run_id, timestamp, dataset_name, metric_name, metric_value, day_of_week,
                         metric_group, column_name, segment, tags)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    """, (
                        run_id,
                        timestamp,
                        dataset_name,
                        metric_name,
                        payload["value"],
                        day_of_week,
                        payload.get("metric_group", "general"),
                        payload.get("column_name"),
                        payload.get("segment", "global"),
                        json.dumps(payload.get("tags", {})),
                    ))

            print(f"🧠 MEMORY: Saved {len(normalized)} metrics for '{dataset_name}' (Day {day_of_week})")

        return run_id

    def save_slo_results(self, run_id: str, dataset_name: str, slo_results: List[Dict[str, Any]]) -> None:
        """Persist per-run SLO checks."""
        if not run_id or not slo_results:
            return

        with get_connection() as conn:
            with conn.cursor() as cur:
                for result in slo_results:
                    cur.execute(
                        """
                        INSERT INTO slo_history
                        (run_id, timestamp, dataset_name, slo_name, operator,
                         target_value, observed_value, status, error_budget_burn, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        """,
                        (
                            run_id,
                            datetime.now(timezone.utc),
                            dataset_name,
                            result.get("slo_name"),
                            result.get("operator"),
                            result.get("target"),
                            result.get("observed"),
                            result.get("status", "UNKNOWN"),
                            float(result.get("error_budget_burn", 0.0)),
                            json.dumps(result.get("metadata", {})),
                        ),
                    )

    @staticmethod
    def _safe_metric_suffix(column_name: Any) -> str:
        """Normalize column names for stable metric ids."""
        value = str(column_name).strip().replace(" ", "_")
        return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in value)

    def _normalize_metric_inputs(
        self, metrics: Dict[str, Union[float, Dict[str, Any]]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Normalize incoming metric payloads into a typed structure.
        Non-numeric values are skipped.
        """
        normalized: Dict[str, Dict[str, Any]] = {}
        for metric_name, raw_value in metrics.items():
            payload: Dict[str, Any]
            if isinstance(raw_value, dict):
                payload = dict(raw_value)
                value = payload.get("value")
            else:
                payload = {}
                value = raw_value

            try:
                numeric_value = float(value)
            except (TypeError, ValueError):
                continue

            normalized[metric_name] = {
                "value": numeric_value,
                "metric_group": payload.get("metric_group", "general"),
                "column_name": payload.get("column_name"),
                "segment": payload.get("segment", "global"),
                "tags": payload.get("tags", {}) if isinstance(payload.get("tags", {}), dict) else {},
            }
        return normalized

    def _collect_dataframe_metrics(self, dataframe: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Derive a richer metric set from the current dataframe.
        Keeps metric names backward-compatible (e.g. null_rate_email, mean_amount).
        """
        metrics: Dict[str, Dict[str, Any]] = {}
        row_count = float(len(dataframe))
        metrics["row_count"] = {
            "value": row_count,
            "metric_group": "volume",
            "segment": "global",
            "tags": {"source": "dataframe"},
        }

        if row_count <= 0:
            return metrics

        duplicate_rate = float(dataframe.duplicated().mean())
        metrics["duplicate_rate"] = {
            "value": duplicate_rate,
            "metric_group": "uniqueness",
            "segment": "global",
            "tags": {"source": "dataframe"},
        }

        columns = list(dataframe.columns)[: self.max_profiled_columns]
        numeric_cols = list(dataframe.select_dtypes(include=[np.number]).columns)

        for col in columns:
            safe_col = self._safe_metric_suffix(col)
            null_rate = float(dataframe[col].isnull().mean())
            distinct_ratio = float(dataframe[col].nunique(dropna=True) / max(1, len(dataframe)))

            metrics[f"null_rate_{safe_col}"] = {
                "value": null_rate,
                "metric_group": "completeness",
                "column_name": str(col),
                "segment": "global",
                "tags": {"source": "dataframe"},
            }
            metrics[f"distinct_ratio_{safe_col}"] = {
                "value": distinct_ratio,
                "metric_group": "uniqueness",
                "column_name": str(col),
                "segment": "global",
                "tags": {"source": "dataframe"},
            }

        for col in numeric_cols[: self.max_profiled_columns]:
            safe_col = self._safe_metric_suffix(col)
            numeric_series = pd.to_numeric(dataframe[col], errors="coerce").dropna()
            if numeric_series.empty:
                continue

            metrics[f"mean_{safe_col}"] = {
                "value": float(numeric_series.mean()),
                "metric_group": "distribution",
                "column_name": str(col),
                "segment": "global",
                "tags": {"source": "dataframe"},
            }
            metrics[f"std_{safe_col}"] = {
                "value": float(numeric_series.std(ddof=0)),
                "metric_group": "distribution",
                "column_name": str(col),
                "segment": "global",
                "tags": {"source": "dataframe"},
            }
            metrics[f"p95_{safe_col}"] = {
                "value": float(np.percentile(numeric_series, 95)),
                "metric_group": "distribution",
                "column_name": str(col),
                "segment": "global",
                "tags": {"source": "dataframe"},
            }

        return metrics

    def _get_baseline_values(self, dataset_name: str, metric_name: str) -> Tuple[List[float], str]:
        """
        Fetch baseline history values.
        Prefers same-day-of-week history, then falls back to global history.
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                current_day = datetime.now().weekday()
                cur.execute(
                    """
                    SELECT metric_value
                    FROM metric_history
                    WHERE dataset_name = %s
                      AND metric_name = %s
                      AND day_of_week = %s
                    ORDER BY timestamp DESC
                    LIMIT 90
                    """,
                    (dataset_name, metric_name, current_day),
                )
                seasonal_values = [float(row[0]) for row in cur.fetchall() if row[0] is not None]
                if len(seasonal_values) >= 3:
                    return seasonal_values, "seasonal"

                cur.execute(
                    """
                    SELECT metric_value
                    FROM metric_history
                    WHERE dataset_name = %s
                      AND metric_name = %s
                    ORDER BY timestamp DESC
                    LIMIT 90
                    """,
                    (dataset_name, metric_name),
                )
                global_values = [float(row[0]) for row in cur.fetchall() if row[0] is not None]
                if len(global_values) >= 3:
                    return global_values, "global"

        return [], "initializing"

    @staticmethod
    def _compute_distribution_stats(values: List[float]) -> Dict[str, float]:
        arr = np.asarray(values, dtype=float)
        mean = float(np.mean(arr))
        std_dev = float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0
        median = float(np.median(arr))
        q1 = float(np.percentile(arr, 25))
        q3 = float(np.percentile(arr, 75))
        mad = float(np.median(np.abs(arr - median)))
        return {
            "mean": mean,
            "std_dev": std_dev,
            "median": median,
            "q1": q1,
            "q3": q3,
            "mad": mad,
            "sample_count": float(len(arr)),
        }

    def _get_baseline_stats(self, dataset_name: str, metric_name: str) -> Dict[str, Any]:
        values, baseline_type = self._get_baseline_values(dataset_name, metric_name)
        if len(values) < 3:
            return {
                "mean": 0.0,
                "std_dev": 0.0,
                "median": 0.0,
                "q1": 0.0,
                "q3": 0.0,
                "mad": 0.0,
                "sample_count": 0,
                "baseline_type": "initializing",
            }

        stats = self._compute_distribution_stats(values)
        stats["sample_count"] = int(stats["sample_count"])
        stats["baseline_type"] = baseline_type
        return stats

    def get_seasonal_baseline(self, dataset_name: str, metric_name: str) -> Tuple[float, float, str]:
        """
        Get the statistical baseline (Mean, StdDev) for a metric.

        Prioritizes Seasonal History (same day of week).
        Falls back to Global History (last 30 runs) if insufficient seasonal data.

        Returns:
            Tuple[mean, std_dev, status]
            status can be: 'seasonal', 'global', 'initializing'
        """
        stats = self._get_baseline_stats(dataset_name, metric_name)
        return (
            float(stats.get("mean", 0.0)),
            float(stats.get("std_dev", 0.0)),
            str(stats.get("baseline_type", "initializing")),
        )

    def evaluate_run(self, dataset_name: str, current_metrics: Dict[str, float],
                    dataframe: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """
        Analyze the current run for anomalies against historical baselines.

        This is the main entry point for the Agent.

        Args:
            dataset_name: Name of the dataset
            current_metrics: Dictionary of current metrics (e.g. row_count)
            dataframe: Optional Pandas DataFrame to calculate distribution metrics

        Returns:
            JSON-compatible dictionary containing the diagnostic report
        """
        # Calculate richer distribution metrics from dataframe.
        if dataframe is not None:
            derived_metrics = self._collect_dataframe_metrics(dataframe)
            # Explicitly provided metrics take precedence over inferred ones.
            for metric_name, payload in derived_metrics.items():
                if metric_name not in current_metrics:
                    current_metrics[metric_name] = payload

        normalized_metrics = self._normalize_metric_inputs(current_metrics)

        report = {
            "dataset": dataset_name,
            "timestamp": datetime.now().isoformat(),
            "status": "PASS",
            "anomalies": [],
            "metrics": {},
            "summary": {},
        }

        anomaly_count = 0

        print("\n" + "📉" * 40)
        print(f"STATISTICAL ENGINE: Analyzing '{dataset_name}'")
        print("📉" * 40)

        for metric_name, payload in normalized_metrics.items():
            current_value = payload["value"]
            baseline_stats = self._get_baseline_stats(dataset_name, metric_name)
            mean = baseline_stats["mean"]
            std_dev = baseline_stats["std_dev"]
            median = baseline_stats["median"]
            mad = baseline_stats["mad"]
            q1 = baseline_stats["q1"]
            q3 = baseline_stats["q3"]
            sample_count = baseline_stats["sample_count"]
            baseline_type = baseline_stats["baseline_type"]

            z_score = 0.0
            robust_z_score = 0.0
            iqr_low = None
            iqr_high = None
            is_anomaly = False
            reason = ""
            methods_triggered: List[str] = []

            if baseline_type == "initializing":
                reason = "Baseline initializing (insufficient history)"
            else:
                if std_dev == 0:
                    z_score = 0.0 if current_value == mean else (10.0 if current_value > mean else -10.0)
                else:
                    z_score = (current_value - mean) / std_dev

                if mad == 0:
                    robust_z_score = 0.0 if current_value == median else (10.0 if current_value > median else -10.0)
                else:
                    robust_z_score = (current_value - median) / (1.4826 * mad)

                if sample_count >= 8:
                    iqr = q3 - q1
                    if iqr > 0:
                        iqr_low = q1 - (self.iqr_multiplier * iqr)
                        iqr_high = q3 + (self.iqr_multiplier * iqr)

                if abs(z_score) > self.z_threshold:
                    methods_triggered.append("z_score")
                if abs(robust_z_score) > self.robust_z_threshold:
                    methods_triggered.append("robust_z")
                if iqr_low is not None and iqr_high is not None and (
                    current_value < iqr_low or current_value > iqr_high
                ):
                    methods_triggered.append("iqr")

                if methods_triggered:
                    is_anomaly = True
                    anomaly_count += 1
                    reason = (
                        f"Anomalous by {', '.join(methods_triggered)} "
                        f"(z={z_score:.2f}, robust_z={robust_z_score:.2f})"
                    )
                else:
                    reason = f"Normal (z={z_score:.2f}, robust_z={robust_z_score:.2f})"

                self.save_learned_threshold(
                    dataset_name=dataset_name,
                    metric_name=metric_name,
                    mean=mean,
                    std=std_dev,
                    baseline_type=baseline_type,
                    sample_count=sample_count,
                )

            metric_data = {
                "value": current_value,
                "baseline_mean": float(f"{mean:.4f}"),
                "baseline_std_dev": float(f"{std_dev:.4f}"),
                "baseline_median": float(f"{median:.4f}"),
                "baseline_mad": float(f"{mad:.4f}"),
                "baseline_type": baseline_type,
                "sample_count": sample_count,
                "z_score": float(f"{z_score:.4f}"),
                "robust_z_score": float(f"{robust_z_score:.4f}"),
                "iqr_bounds": [iqr_low, iqr_high] if iqr_low is not None and iqr_high is not None else None,
                "methods_triggered": methods_triggered,
                "is_anomaly": is_anomaly,
                "reason": reason,
                "metric_group": payload.get("metric_group", "general"),
                "column_name": payload.get("column_name"),
                "segment": payload.get("segment", "global"),
                "tags": payload.get("tags", {}),
            }
            report["metrics"][metric_name] = metric_data

            if is_anomaly:
                severity = "CRITICAL"
                if (
                    abs(metric_data["z_score"]) < self.z_threshold + 0.75
                    and abs(metric_data["robust_z_score"]) < self.robust_z_threshold + 0.75
                    and len(methods_triggered) == 1
                ):
                    severity = "WARNING"

                report["anomalies"].append({
                    "metric_name": metric_name,
                    "metric": metric_name,
                    "severity": severity,
                    "z_score": metric_data["z_score"],
                    "robust_z_score": metric_data["robust_z_score"],
                    "reason": reason,
                    "details": reason,
                    "methods_triggered": methods_triggered,
                    "context": (
                        f"Expected {mean:.2f} ±{3*std_dev:.2f}, got {current_value:.2f}"
                        if baseline_type != "initializing"
                        else "Insufficient baseline history"
                    ),
                })
                print(f"🚨 {metric_name}: {reason}")
                print(
                    f"   Context: {metric_data['reason']} | "
                    f"Expected: {mean:.2f} vs Actual: {current_value:.2f}"
                )

        if anomaly_count > 0:
            report["status"] = "ANOMALY_DETECTED"
            print(f"\n❌ FAILED: Detected {anomaly_count} statistical anomalies")
        else:
            print("\n✅ PASSED: No statistical anomalies detected")

        report["summary"] = {
            "metrics_checked": len(normalized_metrics),
            "anomaly_count": anomaly_count,
            "detectors": {
                "z_score_threshold": self.z_threshold,
                "robust_z_threshold": self.robust_z_threshold,
                "iqr_multiplier": self.iqr_multiplier,
            },
        }

        print("📉" * 40)
        return report


if __name__ == "__main__":
    # Test the Anomaly Detector
    detector = AnomalyDetector()

    import random
    print("🧠 Training Memory with 10 runs of data...")
    for i in range(10):
        detector.save_run_metrics("transactions", {"row_count": random.gauss(1000, 50)})

    print("\n🔍 Evaluating Normal Run:")
    detector.evaluate_run("transactions", {"row_count": 1020})

    print("\n🔍 Evaluating Anomalous Run (Volume Drop):")
    report = detector.evaluate_run("transactions", {"row_count": 500})

    print("\n🤖 JSON Output for LLM Agent:")
    print(json.dumps(report, indent=2))

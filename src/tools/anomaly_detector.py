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

    def save_run_to_history(self, dataset_name: str, status: str,
                           quality_score: float, anomaly_count: int,
                           z_score_max: float, reason: str,
                           duration_ms: int, run_id: str = None, dimension_scores: dict = None) -> str:
        """Save a run outcome to the run_history system table."""
        if run_id is None:
            run_id = str(uuid.uuid4())

        import json
        dimension_scores_json = json.dumps(dimension_scores) if dimension_scores else None

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO run_history
                    (run_id, timestamp, dataset_name, status, quality_score,
                     anomaly_count, z_score_max, reason, duration_ms, dimension_scores)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (run_id, datetime.now(timezone.utc), dataset_name, status,
                      quality_score, anomaly_count, z_score_max, reason, duration_ms, dimension_scores_json))
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

    def save_run_metrics(self, dataset_name: str, metrics_dict: Dict[str, float],
                         run_id: Optional[str] = None) -> str:
        """
        Save metrics for a specific run to history.

        Args:
            dataset_name: Name of the dataset (e.g., 'transactions')
            metrics_dict: Dictionary of metrics (e.g., {'row_count': 100, 'null_rate': 0.0})
            run_id: Optional existing run_id to associate metrics with.

        Returns:
            str: The run_id used for this batch.
        """
        if not run_id:
            run_id = str(uuid.uuid4())
        timestamp = datetime.now(timezone.utc)
        day_of_week = timestamp.weekday()

        with get_connection() as conn:
            with conn.cursor() as cur:
                for metric_name, value in metrics_dict.items():
                    cur.execute("""
                        INSERT INTO metric_history
                        (run_id, timestamp, dataset_name, metric_name, metric_value, day_of_week)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (run_id, timestamp, dataset_name, metric_name, float(value), day_of_week))

            print(f"🧠 MEMORY: Saved {len(metrics_dict)} metrics for '{dataset_name}' (Day {day_of_week})")

        return run_id

    def get_seasonal_baseline(self, dataset_name: str, metric_name: str) -> Tuple[float, float, str]:
        """
        Get the statistical baseline (Mean, StdDev) for a metric.

        Prioritizes Seasonal History (same day of week).
        Falls back to Global History (last 30 runs) if insufficient seasonal data.

        Returns:
            Tuple[mean, std_dev, status]
            status can be: 'seasonal', 'global', 'initializing'
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                current_day = datetime.now().weekday()

                # 1. Try Seasonal History (Same Day of Week)
                cur.execute("""
                    SELECT
                        AVG(metric_value) as mean,
                        STDDEV_SAMP(metric_value) as std_dev,
                        COUNT(*) as count
                    FROM metric_history
                    WHERE dataset_name = %s
                      AND metric_name = %s
                      AND day_of_week = %s
                """, (dataset_name, metric_name, current_day))
                result = cur.fetchone()
                mean, std_dev, count = result

                if count and count >= 3:
                    if std_dev is None:
                        std_dev = 0.0
                    return float(mean), float(std_dev), "seasonal"

                # 2. Fallback to Global History (Last 30 runs regardless of day)
                cur.execute("""
                    SELECT
                        AVG(metric_value) as mean,
                        STDDEV_SAMP(metric_value) as std_dev,
                        COUNT(*) as count
                    FROM (
                        SELECT metric_value
                        FROM metric_history
                        WHERE dataset_name = %s AND metric_name = %s
                        ORDER BY timestamp DESC
                        LIMIT 30
                    ) recent_history
                """, (dataset_name, metric_name))
                result = cur.fetchone()
                mean, std_dev, count = result

                if count and count >= 3:
                    if std_dev is None:
                        std_dev = 0.0
                    return float(mean), float(std_dev), "global"

                # 3. Cold Start / Initializing
                return 0.0, 0.0, "initializing"

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
        # Calculate distribution drift metrics from dataframe
        if dataframe is not None:
            for col in dataframe.columns:
                null_rate = dataframe[col].isnull().mean()
                current_metrics[f"{col}_null_rate"] = float(null_rate)

            numeric_cols = dataframe.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                col_mean = dataframe[col].mean()
                if not pd.isna(col_mean):
                    current_metrics[f"{col}_mean"] = float(col_mean)

        report = {
            "dataset": dataset_name,
            "timestamp": datetime.now().isoformat(),
            "status": "PASS",
            "anomalies": [],
            "metrics": {}
        }

        anomaly_count = 0

        print("\n" + "📉" * 40)
        print(f"STATISTICAL ENGINE: Analyzing '{dataset_name}'")
        print("📉" * 40)

        for metric_name, current_value in current_metrics.items():
            mean, std_dev, baseline_type = self.get_seasonal_baseline(dataset_name, metric_name)

            z_score = 0.0
            is_anomaly = False
            reason = ""

            if baseline_type == "initializing":
                reason = "Baseline Initializing (insufficient history)"
                z_score = 0.0
            else:
                if std_dev == 0:
                    if current_value == mean:
                        z_score = 0.0
                    else:
                        z_score = 10.0 if current_value > mean else -10.0
                else:
                    z_score = (current_value - mean) / std_dev

                if abs(z_score) > 3.0:
                    is_anomaly = True
                    anomaly_count += 1
                    reason = f"CRITICAL ANOMALY: Z-Score {z_score:.2f} > 3.0"
                else:
                    reason = f"Normal (Z-Score: {z_score:.2f})"

            metric_data = {
                "value": current_value,
                "baseline_mean": float(f"{mean:.2f}"),
                "baseline_std_dev": float(f"{std_dev:.2f}"),
                "baseline_type": baseline_type,
                "z_score": float(f"{z_score:.2f}"),
                "is_anomaly": is_anomaly,
                "reason": reason
            }
            report["metrics"][metric_name] = metric_data

            if is_anomaly:
                report["anomalies"].append({
                    "metric": metric_name,
                    "severity": "CRITICAL",
                    "z_score": metric_data["z_score"],
                    "details": reason,
                    "context": f"Expected {mean:.2f} ±{3*std_dev:.2f}, got {current_value}"
                })
                print(f"🚨 {metric_name}: {reason}")
                print(f"   Context: {metric_data['reason']} | Expected: {mean:.2f} vs Actual: {current_value}")

        if anomaly_count > 0:
            report["status"] = "ANOMALY_DETECTED"
            print(f"\n❌ FAILED: Detected {anomaly_count} statistical anomalies")
        else:
            print("\n✅ PASSED: No statistical anomalies detected")

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

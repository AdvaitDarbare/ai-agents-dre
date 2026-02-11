"""
Anomaly Detector Tool - Statistical AI for Data Observability

    This tool implements statistical anomaly detection inspired by Monte Carlo and Databricks.
    It uses DuckDB as a persistent memory store to learn historical patterns and detect:
    1. Volume anomalies (row count shifts)
    2. Distribution shifts (null rate changes, mean value changes)

    The core logic uses a Z-Score algorithm with seasonality awareness:
    - It compares today's metrics against previous data for the SAME day of the week.
    - Fallback to global history if seasonal history is insufficient (Cold Start).

    This tool provides the "Mathematical Intuition" for the Agentic Platform.
    """

import duckdb
import uuid
import json
import numpy as np
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Union

class AnomalyDetector:
    """
    The Statistical Engine - Detects anomalies using Z-Score and Seasonality.
    
    Attributes:
        db_path (str): Path to the persistent DuckDB database.
    """
    
    def __init__(self, db_path: str = "data/system/agent_memory.db"):
        """
        Initialize the AnomalyDetector with a persistent memory store.
        
        Args:
            db_path: Path to the DuckDB database file.
        """
        self.db_path = db_path
        self._init_memory()

    def _init_memory(self):
        """Initialize the persistent metric store in DuckDB."""
        # Ensure directory exists
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
        conn = duckdb.connect(self.db_path)
        try:
            # Create metric_history table if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metric_history (
                    run_id VARCHAR,
                    timestamp TIMESTAMP,
                    dataset_name VARCHAR,
                    metric_name VARCHAR,
                    metric_value DOUBLE,
                    day_of_week INTEGER
                )
            """)
            
            # Create index for faster lookups
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_metrics 
                ON metric_history(dataset_name, metric_name, day_of_week)
            """)
            
            # ---------------------------------------------------------
            # Phase 3: System Tables
            # ---------------------------------------------------------
            
            # Run History — structured outcomes per health check run
            conn.execute("""
                CREATE TABLE IF NOT EXISTS run_history (
                    run_id VARCHAR,
                    timestamp TIMESTAMP,
                    dataset_name VARCHAR,
                    status VARCHAR,
                    quality_score DOUBLE,
                    anomaly_count INTEGER,
                    z_score_max DOUBLE,
                    reason VARCHAR,
                    duration_ms INTEGER
                )
            """)
            
            # Learned Thresholds — cached baselines so agents don't re-learn
            conn.execute("""
                CREATE TABLE IF NOT EXISTS learned_thresholds (
                    dataset_name VARCHAR,
                    metric_name VARCHAR,
                    baseline_mean DOUBLE,
                    baseline_std DOUBLE,
                    baseline_type VARCHAR,
                    last_updated TIMESTAMP,
                    sample_count INTEGER
                )
            """)
            
            # Dataset Registry — auto-discovery metadata + scan state
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dataset_registry (
                    dataset_name VARCHAR PRIMARY KEY,
                    contract_path VARCHAR,
                    lifecycle VARCHAR,
                    criticality VARCHAR,
                    last_scanned TIMESTAMP,
                    last_status VARCHAR,
                    last_file_mtime DOUBLE,
                    scan_count INTEGER DEFAULT 0
                )
            """)
            
        finally:
            conn.close()

    def save_run_to_history(self, dataset_name: str, status: str, 
                           quality_score: float, anomaly_count: int,
                           z_score_max: float, reason: str, 
                           duration_ms: int) -> str:
        """Save a run outcome to the run_history system table."""
        run_id = str(uuid.uuid4())
        conn = duckdb.connect(self.db_path)
        try:
            conn.execute("""
                INSERT INTO run_history 
                (run_id, timestamp, dataset_name, status, quality_score, 
                 anomaly_count, z_score_max, reason, duration_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (run_id, datetime.now(), dataset_name, status, 
                  quality_score, anomaly_count, z_score_max, reason, duration_ms))
        finally:
            conn.close()
        return run_id

    def save_learned_threshold(self, dataset_name: str, metric_name: str,
                               mean: float, std: float, 
                               baseline_type: str, sample_count: int):
        """Cache a learned threshold so agents don't re-learn every run."""
        conn = duckdb.connect(self.db_path)
        try:
            # Upsert: delete existing then insert
            conn.execute("""
                DELETE FROM learned_thresholds 
                WHERE dataset_name = ? AND metric_name = ?
            """, (dataset_name, metric_name))
            conn.execute("""
                INSERT INTO learned_thresholds
                (dataset_name, metric_name, baseline_mean, baseline_std, 
                 baseline_type, last_updated, sample_count)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (dataset_name, metric_name, mean, std, 
                  baseline_type, datetime.now(), sample_count))
        finally:
            conn.close()

    def update_dataset_registry(self, dataset_name: str, contract_path: str,
                                lifecycle: str, criticality: str,
                                status: str = None, file_mtime: float = None):
        """Update or insert a dataset's registry entry."""
        conn = duckdb.connect(self.db_path)
        try:
            existing = conn.execute(
                "SELECT scan_count FROM dataset_registry WHERE dataset_name = ?",
                (dataset_name,)
            ).fetchone()
            
            if existing:
                scan_count = (existing[0] or 0) + 1
                conn.execute("""
                    UPDATE dataset_registry SET
                        contract_path = ?,
                        lifecycle = ?,
                        criticality = ?,
                        last_scanned = ?,
                        last_status = COALESCE(?, last_status),
                        last_file_mtime = COALESCE(?, last_file_mtime),
                        scan_count = ?
                    WHERE dataset_name = ?
                """, (contract_path, lifecycle, criticality,
                      datetime.now(), status, file_mtime, scan_count, dataset_name))
            else:
                conn.execute("""
                    INSERT INTO dataset_registry 
                    (dataset_name, contract_path, lifecycle, criticality,
                     last_scanned, last_status, last_file_mtime, scan_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """, (dataset_name, contract_path, lifecycle, criticality,
                      datetime.now(), status, file_mtime))
        finally:
            conn.close()

    def save_run_metrics(self, dataset_name: str, metrics_dict: Dict[str, float]) -> str:
        """
        Save metrics for a specific run to history.
        
        Args:
            dataset_name: Name of the dataset (e.g., 'transactions')
            metrics_dict: Dictionary of metrics (e.g., {'row_count': 100, 'null_rate': 0.0})
            
        Returns:
            str: The unique run_id for this batch.
        """
        run_id = str(uuid.uuid4())
        timestamp = datetime.now()
        day_of_week = timestamp.weekday()  # 0=Monday, 6=Sunday
        
        conn = duckdb.connect(self.db_path)
        try:
            # Prepare batch insert
            for metric_name, value in metrics_dict.items():
                conn.execute("""
                    INSERT INTO metric_history 
                    (run_id, timestamp, dataset_name, metric_name, metric_value, day_of_week)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (run_id, timestamp, dataset_name, metric_name, float(value), day_of_week))
                
            print(f"🧠 MEMORY: Saved {len(metrics_dict)} metrics for '{dataset_name}' (Day {day_of_week})")
        except Exception as e:
            print(f"❌ ERROR: Failed to save metrics: {e}")
        finally:
            conn.close()
            
        return run_id

    def get_seasonal_baseline(self, dataset_name: str, metric_name: str) -> Tuple[float, float, str]:
        """
        Get the statistical baseline (Mean, StdDev) for a metric.
        
        Prioritizes Seasonal History (same day of week).
        Falls back to Global History (last 30 runs) if insufficient seasonal data.
        
        Args:
            dataset_name: Name of the dataset
            metric_name: Name of the metric
            
        Returns:
            Tuple[mean, std_dev, status]
            status can be: 'seasonal', 'global', 'initializing'
        """
        conn = duckdb.connect(self.db_path)
        try:
            current_day = datetime.now().weekday()
            
            # 1. Try Seasonal History (Same Day of Week)
            # We need at least 3 data points to have a meaningful distribution
            seasonal_query = """
                SELECT 
                    AVG(metric_value) as mean,
                    STDDEV(metric_value) as std_dev,
                    COUNT(*) as count
                FROM metric_history
                WHERE dataset_name = ? 
                  AND metric_name = ? 
                  AND day_of_week = ?
            """
            result = conn.execute(seasonal_query, (dataset_name, metric_name, current_day)).fetchone()
            mean, std_dev, count = result
            
            if count >= 3:
                # If std_dev is 0 (perfect consistency), use a tiny epsilon to avoid div-by-zero later
                if std_dev is None: std_dev = 0.0
                return mean, std_dev, "seasonal"
            
            # 2. Fallback to Global History (Last 30 runs regardless of day)
            global_query = """
                WITH recent_history AS (
                    SELECT metric_value
                    FROM metric_history
                    WHERE dataset_name = ? AND metric_name = ?
                    ORDER BY timestamp DESC
                    LIMIT 30
                )
                SELECT 
                    AVG(metric_value) as mean,
                    STDDEV(metric_value) as std_dev,
                    COUNT(*) as count
                FROM recent_history
            """
            result = conn.execute(global_query, (dataset_name, metric_name)).fetchone()
            mean, std_dev, count = result
            
            if count >= 3:
                if std_dev is None: std_dev = 0.0
                return mean, std_dev, "global"
            
            # 3. Cold Start / Initializing
            return 0.0, 0.0, "initializing"
            
        finally:
            conn.close()

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
        # specialized logic to calculate distribution drift metrics from dataframe
        if dataframe is not None:
            # 1. Null Rates for all columns
            for col in dataframe.columns:
                null_rate = dataframe[col].isnull().mean()
                current_metrics[f"null_rate_{col}"] = float(null_rate)
                
            # 2. Mean values for numeric columns
            numeric_cols = dataframe.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                # Skip IDs or other non-metric numerics if needed, or just track everything
                col_mean = dataframe[col].mean()
                # Handle potential NaN from empty cols
                if not pd.isna(col_mean):
                    current_metrics[f"mean_{col}"] = float(col_mean)
        
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
        
        # Save current metrics to history FIRST so we build history over time
        # In a real prod system, you might only save "valid" runs, but for
        # this agentic loop, we want to learn from everything or have a separate training step.
        # For now, let's assume we save AFTER validation in the orchestrator, 
        # or we strictly use this method to EVALUATE and save explicitly elsewhere.
        # However, to keep the "Simulation" working, I will not auto-save here 
        # to avoid polluting history with bad runs during testing.
        
        for metric_name, current_value in current_metrics.items():
            mean, std_dev, baseline_type = self.get_seasonal_baseline(dataset_name, metric_name)
            
            # Calculate Z-Score
            # Z = (X - Mean) / StdDev
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
                        # Infinite Z-Score (deviation from a constant)
                        # We cap it at 10.0 for safety, preserving sign
                        z_score = 10.0 if current_value > mean else -10.0
                else:
                    z_score = (current_value - mean) / std_dev
                
                # Check Threshold (|Z| > 3.0 is standard for 99.7% confidence)
                if abs(z_score) > 3.0:
                    is_anomaly = True
                    anomaly_count += 1
                    reason = f"CRITICAL ANOMALY: Z-Score {z_score:.2f} > 3.0"
                else:
                    reason = f"Normal (Z-Score: {z_score:.2f})"
            
            # Add to report
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
            
            # Log specific anomalies to the top-level list
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
    
    # 1. Simulate History (Training the Brain)
    print("🧠 Training Memory with 30 days of data...")
    import random
    
    # Simulate a "normal" Tuesday (Day 1) transaction volume of ~1000
    for i in range(10):
        detector.save_run_metrics("transactions", {"row_count": random.gauss(1000, 50)})
        
    # 2. Simulate a "normal" run
    print("\n🔍 Evaluating Normal Run:")
    detector.evaluate_run("transactions", {"row_count": 1020})
    
    # 3. Simulate an ANOMALY (Volume Drop)
    print("\n🔍 Evaluating Anomalous Run (Volume Drop):")
    report = detector.evaluate_run("transactions", {"row_count": 500})
    
    # 4. Print the JSON output meant for the LLM
    print("\n🤖 JSON Output for LLM Agent:")
    print(json.dumps(report, indent=2))

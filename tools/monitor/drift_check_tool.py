"""
Tool E: DriftCheckTool (The History)
Compares current data against historical 7-day rolling average.
"""
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, Optional


class DriftCheckTool:
    """
    Detects data drift by comparing current stats against historical baseline.
    Uses SQLite to store and query historical metrics.
    """
    
    def __init__(self, db_path: str = "data/metrics_history.db"):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize SQLite database for historical metrics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metrics_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                table_name TEXT NOT NULL,
                file_hash TEXT,
                row_count INTEGER,
                column_stats TEXT,  -- JSON string of column statistics
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
    
    def save_current_run(self, table_name: str, file_hash: str, row_count: int, 
                         column_stats: Dict[str, Any]):
        """Save current run metrics to history."""
        import json
        import numpy as np
        
        # Convert numpy types to native Python types for JSON serialization
        def convert_to_native(obj):
            if isinstance(obj, (np.integer, np.int64, np.int32)):
                return int(obj)
            elif isinstance(obj, (np.floating, np.float64, np.float32)):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, dict):
                return {k: convert_to_native(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_to_native(item) for item in obj]
            return obj
        
        column_stats_clean = convert_to_native(column_stats)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO metrics_history (timestamp, table_name, file_hash, row_count, column_stats)
            VALUES (?, ?, ?, ?, ?)
        """, (
            datetime.now().isoformat(),
            table_name,
            file_hash,
            row_count,
            json.dumps(column_stats_clean)
        ))
        
        conn.commit()
        conn.close()
    
    def run(self, current_stats: Dict[str, Any], table_name: str, 
            lookback_days: int = 7, threshold_pct: float = 30.0) -> Dict[str, Any]:
        """
        Check for drift against historical baseline.
        
        Args:
            current_stats: Stats from StatsAnalysisTool
            table_name: Name of the table/dataset
            lookback_days: Number of days to look back for baseline
            threshold_pct: Percentage deviation to trigger warning
        
        Returns:
            {
                "status": "PASS" | "DRIFT_DETECTED",
                "drift_warnings": [
                    {
                        "metric": str,
                        "current": float,
                        "baseline": float,
                        "deviation_pct": float
                    }
                ],
                "decision": "CONTINUE"  # Drift is always a WARNING, not CRITICAL
            }
        """
        import json
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Query historical data
        cutoff_date = (datetime.now() - timedelta(days=lookback_days)).isoformat()
        
        cursor.execute("""
            SELECT row_count, column_stats
            FROM metrics_history
            WHERE table_name = ? AND timestamp >= ?
            ORDER BY timestamp DESC
        """, (table_name, cutoff_date))
        
        historical_rows = cursor.fetchall()
        conn.close()
        
        if len(historical_rows) == 0:
            return {
                "status": "NO_BASELINE",
                "drift_warnings": [],
                "decision": "CONTINUE",
                "message": "No historical data available for comparison"
            }
        
        # Calculate baseline metrics
        historical_row_counts = [row[0] for row in historical_rows]
        avg_row_count = sum(historical_row_counts) / len(historical_row_counts)
        
        # Parse column stats from historical data
        historical_col_stats = {}
        for _, col_stats_json in historical_rows:
            col_stats = json.loads(col_stats_json)
            for col_name, stats in col_stats.items():
                if col_name not in historical_col_stats:
                    historical_col_stats[col_name] = []
                if 'mean' in stats:
                    historical_col_stats[col_name].append(stats['mean'])
        
        # Calculate baseline averages
        baseline_means = {
            col: sum(values) / len(values)
            for col, values in historical_col_stats.items()
            if len(values) > 0
        }
        
        # Check for drift
        drift_warnings = []
        
        # Row count drift
        current_row_count = current_stats.get('row_count', 0)
        if avg_row_count > 0:
            row_deviation = abs(current_row_count - avg_row_count) / avg_row_count * 100
            if row_deviation > threshold_pct:
                drift_warnings.append({
                    "metric": "row_count",
                    "current": current_row_count,
                    "baseline": round(avg_row_count, 2),
                    "deviation_pct": round(row_deviation, 2)
                })
        
        # Column mean drift
        current_profiles = current_stats.get('profiles', {})
        for col_name, baseline_mean in baseline_means.items():
            if col_name in current_profiles and 'mean' in current_profiles[col_name]:
                current_mean = current_profiles[col_name]['mean']
                if baseline_mean != 0:
                    mean_deviation = abs(current_mean - baseline_mean) / abs(baseline_mean) * 100
                    if mean_deviation > threshold_pct:
                        drift_warnings.append({
                            "metric": f"{col_name}_mean",
                            "current": current_mean,
                            "baseline": round(baseline_mean, 4),
                            "deviation_pct": round(mean_deviation, 2)
                        })
        
        status = "DRIFT_DETECTED" if drift_warnings else "PASS"
        
        return {
            "status": status,
            "drift_warnings": drift_warnings,
            "baseline_period": f"Last {lookback_days} days ({len(historical_rows)} runs)",
            "decision": "CONTINUE",  # Drift is a WARNING, not a blocker
            "summary": f"{len(drift_warnings)} drift warnings detected" if drift_warnings else "No drift detected"
        }

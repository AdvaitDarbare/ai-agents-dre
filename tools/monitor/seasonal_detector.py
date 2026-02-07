"""
Seasonal Detector Tool
Learns historical patterns and seasonal behaviors to intelligently detect anomalies.
Inspired by Databricks' Agentic Data Quality Monitoring.
"""
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import numpy as np


class SeasonalDetector:
    """
    Learns normal data patterns including seasonal variations.
    
    Key Databricks-inspired features:
    - Learns day-of-week patterns (e.g., dips on weekends)
    - Learns monthly patterns (e.g., tax season spikes)
    - Adapts thresholds based on historical variance
    - Distinguishes real anomalies from expected variation
    """
    
    def __init__(self, db_path: str = "data/metrics_history.db"):
        self.db_path = db_path
        self._init_seasonal_tables()
    
    def _init_seasonal_tables(self):
        """Create tables for seasonal pattern storage."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Store learned patterns by day-of-week and hour
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS seasonal_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                day_of_week INTEGER,  -- 0=Monday, 6=Sunday
                hour_of_day INTEGER,  -- 0-23
                expected_mean REAL,
                expected_std REAL,
                sample_count INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Store monthly patterns (for things like tax season)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS monthly_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                month INTEGER,  -- 1-12
                expected_mean REAL,
                expected_std REAL,
                sample_count INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
    
    def learn_patterns(self, table_name: str, metric_name: str, 
                       historical_data: List[Dict[str, Any]]):
        """
        Learn seasonal patterns from historical data.
        
        Args:
            table_name: Name of the table
            metric_name: Name of the metric (e.g., 'row_count', 'amount_mean')
            historical_data: List of {timestamp, value} dicts
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Group by day of week
        dow_groups = {i: [] for i in range(7)}
        month_groups = {i: [] for i in range(1, 13)}
        
        for record in historical_data:
            ts = datetime.fromisoformat(record['timestamp'])
            value = record['value']
            
            dow_groups[ts.weekday()].append(value)
            month_groups[ts.month].append(value)
        
        # Store day-of-week patterns
        for dow, values in dow_groups.items():
            if len(values) >= 3:  # Need at least 3 samples
                mean_val = np.mean(values)
                std_val = np.std(values) if len(values) > 1 else mean_val * 0.1
                
                cursor.execute("""
                    INSERT OR REPLACE INTO seasonal_patterns 
                    (table_name, metric_name, day_of_week, expected_mean, expected_std, sample_count)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (table_name, metric_name, dow, mean_val, std_val, len(values)))
        
        # Store monthly patterns
        for month, values in month_groups.items():
            if len(values) >= 2:
                mean_val = np.mean(values)
                std_val = np.std(values) if len(values) > 1 else mean_val * 0.1
                
                cursor.execute("""
                    INSERT OR REPLACE INTO monthly_patterns 
                    (table_name, metric_name, month, expected_mean, expected_std, sample_count)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (table_name, metric_name, month, mean_val, std_val, len(values)))
        
        conn.commit()
        conn.close()
    
    def check_anomaly(self, table_name: str, metric_name: str, 
                      current_value: float, timestamp: datetime = None) -> Dict[str, Any]:
        """
        Check if current value is an anomaly considering seasonal patterns.
        
        Returns:
            {
                "is_anomaly": bool,
                "severity": "NORMAL" | "WARNING" | "CRITICAL",
                "expected_range": (low, high),
                "deviation_sigma": float,
                "context": str  # Explanation of why/why not anomaly
            }
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check day-of-week pattern
        cursor.execute("""
            SELECT expected_mean, expected_std, sample_count
            FROM seasonal_patterns
            WHERE table_name = ? AND metric_name = ? AND day_of_week = ?
        """, (table_name, metric_name, timestamp.weekday()))
        
        dow_pattern = cursor.fetchone()
        
        # Check monthly pattern
        cursor.execute("""
            SELECT expected_mean, expected_std, sample_count
            FROM monthly_patterns
            WHERE table_name = ? AND metric_name = ? AND month = ?
        """, (table_name, metric_name, timestamp.month))
        
        month_pattern = cursor.fetchone()
        conn.close()
        
        # No learned patterns yet
        if not dow_pattern and not month_pattern:
            return {
                "is_anomaly": False,
                "severity": "UNKNOWN",
                "expected_range": None,
                "deviation_sigma": 0,
                "context": "Insufficient historical data for seasonal analysis"
            }
        
        # Combine patterns (prefer day-of-week for short-term variation)
        if dow_pattern:
            expected_mean, expected_std, sample_count = dow_pattern
            pattern_type = f"day-of-week ({self._dow_name(timestamp.weekday())})"
        else:
            expected_mean, expected_std, sample_count = month_pattern
            pattern_type = f"monthly ({timestamp.strftime('%B')})"
        
        # Avoid division by zero
        if expected_std == 0:
            expected_std = expected_mean * 0.1
        
        # Calculate deviation in terms of standard deviations
        deviation_sigma = abs(current_value - expected_mean) / expected_std
        
        # Determine severity
        low_bound = expected_mean - 2 * expected_std
        high_bound = expected_mean + 2 * expected_std
        
        if deviation_sigma <= 2:
            is_anomaly = False
            severity = "NORMAL"
            context = f"Value is within expected {pattern_type} range"
        elif deviation_sigma <= 3:
            is_anomaly = True
            severity = "WARNING"
            context = f"Value deviates {deviation_sigma:.1f}σ from {pattern_type} norm"
        else:
            is_anomaly = True
            severity = "CRITICAL"
            context = f"Significant anomaly: {deviation_sigma:.1f}σ from {pattern_type} norm"
        
        return {
            "is_anomaly": is_anomaly,
            "severity": severity,
            "expected_range": (round(low_bound, 2), round(high_bound, 2)),
            "expected_mean": round(expected_mean, 2),
            "current_value": round(current_value, 2),
            "deviation_sigma": round(deviation_sigma, 2),
            "pattern_type": pattern_type,
            "context": context
        }
    
    def _dow_name(self, dow: int) -> str:
        """Convert day-of-week number to name."""
        names = ["Monday", "Tuesday", "Wednesday", "Thursday", 
                 "Friday", "Saturday", "Sunday"]
        return names[dow]

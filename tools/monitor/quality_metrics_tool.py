"""
Quality Metrics Tool
Enhanced data profiling with comprehensive quality signals.
Includes metrics: freshness, completeness, validity, uniqueness.
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from datetime import datetime


class QualityMetricsTool:
    """
    Comprehensive data quality profiling that captures:
    - Freshness: Is the data up-to-date?
    - Completeness: How much data is missing?
    - Validity: Does data conform to expected formats/ranges?
    - Uniqueness: Are there unexpected duplicates?
    - Consistency: Are values internally consistent?
    
    These metrics provide historical context for anomaly detection.
    """
    
    def run(self, df: pd.DataFrame, config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Calculate comprehensive quality metrics for a DataFrame.
        
        Returns:
            {
                "overall_health_score": float (0-100),
                "health_status": "HEALTHY" | "DEGRADED" | "CRITICAL",
                "metrics": {
                    "freshness": {...},
                    "completeness": {...},
                    "validity": {...},
                    "uniqueness": {...}
                },
                "column_health": {...},
                "recommendations": [...]
            }
        """
        config = config or {}
        
        metrics = {
            "freshness": self._check_freshness(df, config),
            "completeness": self._check_completeness(df, config),
            "validity": self._check_validity(df, config),
            "uniqueness": self._check_uniqueness(df, config)
        }
        
        # Calculate column-level health
        column_health = self._calculate_column_health(df, config)
        
        # Calculate overall score
        scores = [
            metrics['freshness'].get('score', 100),
            metrics['completeness'].get('score', 100),
            metrics['validity'].get('score', 100),
            metrics['uniqueness'].get('score', 100)
        ]
        overall_score = sum(scores) / len(scores)
        
        # Determine status
        if overall_score >= 90:
            health_status = "HEALTHY"
        elif overall_score >= 70:
            health_status = "DEGRADED"
        else:
            health_status = "CRITICAL"
        
        # Generate recommendations
        recommendations = self._generate_recommendations(metrics, column_health)
        
        return {
            "timestamp": datetime.now().isoformat(),
            "row_count": len(df),
            "column_count": len(df.columns),
            "overall_health_score": round(overall_score, 2),
            "health_status": health_status,
            "metrics": metrics,
            "column_health": column_health,
            "recommendations": recommendations
        }
    
    def _check_freshness(self, df: pd.DataFrame, config: Dict) -> Dict[str, Any]:
        """Check data freshness by analyzing timestamp columns."""
        timestamp_cols = config.get('timestamp_columns', [])
        
        # Auto-detect datetime columns
        if not timestamp_cols:
            for col in df.columns:
                if 'date' in col.lower() or 'time' in col.lower():
                    timestamp_cols.append(col)
        
        if not timestamp_cols:
            return {
                "score": 100,
                "status": "UNKNOWN",
                "reason": "No timestamp columns detected"
            }
        
        freshness_results = {}
        for col in timestamp_cols:
            if col not in df.columns:
                continue
            
            try:
                # Try to parse as datetime
                ts_series = pd.to_datetime(df[col], errors='coerce')
                max_ts = ts_series.max()
                min_ts = ts_series.min()
                
                if pd.isna(max_ts):
                    freshness_results[col] = {
                        "status": "ERROR",
                        "reason": "Could not parse timestamps"
                    }
                else:
                    age_hours = (datetime.now() - max_ts.to_pydatetime().replace(tzinfo=None)).total_seconds() / 3600
                    freshness_results[col] = {
                        "newest_record": max_ts.isoformat(),
                        "oldest_record": min_ts.isoformat(),
                        "age_hours": round(age_hours, 2),
                        "status": "FRESH" if age_hours < 24 else "STALE"
                    }
            except Exception as e:
                freshness_results[col] = {
                    "status": "ERROR",
                    "reason": str(e)
                }
        
        # Calculate score based on freshness
        fresh_count = sum(1 for v in freshness_results.values() if v.get('status') == 'FRESH')
        score = (fresh_count / len(freshness_results) * 100) if freshness_results else 100
        
        return {
            "score": round(score, 2),
            "status": "FRESH" if score >= 80 else "STALE",
            "columns": freshness_results
        }
    
    def _check_completeness(self, df: pd.DataFrame, config: Dict) -> Dict[str, Any]:
        """Check data completeness (null/missing values)."""
        completeness_results = {}
        
        for col in df.columns:
            null_count = df[col].isna().sum()
            null_pct = (null_count / len(df)) * 100 if len(df) > 0 else 0
            
            completeness_results[col] = {
                "null_count": int(null_count),
                "null_pct": round(null_pct, 2),
                "status": "COMPLETE" if null_pct == 0 else 
                         "MOSTLY_COMPLETE" if null_pct < 5 else
                         "INCOMPLETE" if null_pct < 50 else "MOSTLY_NULL"
            }
        
        # Calculate overall score
        avg_completeness = 100 - np.mean([r['null_pct'] for r in completeness_results.values()])
        
        return {
            "score": round(avg_completeness, 2),
            "status": "COMPLETE" if avg_completeness >= 95 else 
                     "MOSTLY_COMPLETE" if avg_completeness >= 80 else "INCOMPLETE",
            "total_null_cells": int(df.isna().sum().sum()),
            "total_cells": int(df.size),
            "columns": completeness_results
        }
    
    def _check_validity(self, df: pd.DataFrame, config: Dict) -> Dict[str, Any]:
        """Check data validity (format, range, pattern compliance)."""
        validity_rules = config.get('validity_rules', {})
        validity_results = {}
        
        for col in df.columns:
            col_result = {"checks": []}
            
            # Type-based validation
            dtype = str(df[col].dtype)
            
            if 'int' in dtype or 'float' in dtype:
                # Numeric validation
                clean_data = df[col].dropna()
                
                # Check for negative values (might be invalid)
                neg_count = (clean_data < 0).sum()
                if neg_count > 0:
                    col_result['checks'].append({
                        "check": "non_negative",
                        "passed": False,
                        "count": int(neg_count)
                    })
                
                # Check for extreme outliers (beyond 5 sigma)
                if len(clean_data) > 0:
                    mean_val = clean_data.mean()
                    std_val = clean_data.std()
                    if std_val > 0:
                        extreme_outliers = ((clean_data - mean_val).abs() > 5 * std_val).sum()
                        if extreme_outliers > 0:
                            col_result['checks'].append({
                                "check": "extreme_outliers",
                                "passed": False,
                                "count": int(extreme_outliers)
                            })
            
            elif dtype == 'object':
                # String validation
                clean_data = df[col].dropna()
                
                # Check for empty strings
                empty_count = (clean_data == '').sum()
                if empty_count > 0:
                    col_result['checks'].append({
                        "check": "empty_strings",
                        "passed": False,
                        "count": int(empty_count)
                    })
                
                # Check for whitespace-only strings
                whitespace_count = clean_data.str.strip().eq('').sum()
                if whitespace_count > empty_count:
                    col_result['checks'].append({
                        "check": "whitespace_only",
                        "passed": False,
                        "count": int(whitespace_count - empty_count)
                    })
            
            # Calculate column validity score
            failed_checks = sum(1 for c in col_result['checks'] if not c['passed'])
            col_result['score'] = 100 if failed_checks == 0 else max(0, 100 - failed_checks * 20)
            col_result['status'] = "VALID" if col_result['score'] >= 80 else "INVALID"
            
            validity_results[col] = col_result
        
        # Overall score
        avg_score = np.mean([r['score'] for r in validity_results.values()])
        
        return {
            "score": round(avg_score, 2),
            "status": "VALID" if avg_score >= 80 else "INVALID",
            "columns": validity_results
        }
    
    def _check_uniqueness(self, df: pd.DataFrame, config: Dict) -> Dict[str, Any]:
        """Check uniqueness and detect unexpected duplicates."""
        unique_cols = config.get('unique_columns', [])
        
        # Auto-detect potential ID columns
        if not unique_cols:
            for col in df.columns:
                if 'id' in col.lower() or 'key' in col.lower():
                    unique_cols.append(col)
        
        uniqueness_results = {}
        
        # Check each potential unique column
        for col in unique_cols:
            if col not in df.columns:
                continue
            
            total_count = len(df[col].dropna())
            unique_count = df[col].nunique()
            duplicate_count = total_count - unique_count
            
            uniqueness_results[col] = {
                "total_values": total_count,
                "unique_values": unique_count,
                "duplicate_count": duplicate_count,
                "uniqueness_pct": round(unique_count / total_count * 100, 2) if total_count > 0 else 100,
                "status": "UNIQUE" if duplicate_count == 0 else "DUPLICATES_FOUND"
            }
        
        # Check for full row duplicates
        full_dupes = df.duplicated().sum()
        
        # Calculate score
        if uniqueness_results:
            scores = [r['uniqueness_pct'] for r in uniqueness_results.values()]
            avg_score = np.mean(scores)
        else:
            avg_score = 100 if full_dupes == 0 else 100 - (full_dupes / len(df) * 100)
        
        return {
            "score": round(avg_score, 2),
            "status": "UNIQUE" if avg_score >= 99 else "DUPLICATES_FOUND",
            "full_row_duplicates": int(full_dupes),
            "columns": uniqueness_results
        }
    
    def _calculate_column_health(self, df: pd.DataFrame, config: Dict) -> Dict[str, Any]:
        """Calculate per-column health scores."""
        column_health = {}
        
        for col in df.columns:
            null_pct = (df[col].isna().sum() / len(df)) * 100 if len(df) > 0 else 0
            unique_pct = (df[col].nunique() / len(df)) * 100 if len(df) > 0 else 0
            
            # Calculate health score
            health_score = 100
            health_score -= null_pct * 0.5  # Penalize nulls
            
            issues = []
            if null_pct > 50:
                issues.append("HIGH_NULL_RATE")
            if null_pct > 0 and null_pct <= 50:
                issues.append("SOME_NULLS")
            
            column_health[col] = {
                "health_score": max(0, round(health_score, 2)),
                "null_pct": round(null_pct, 2),
                "unique_pct": round(unique_pct, 2),
                "dtype": str(df[col].dtype),
                "issues": issues
            }
        
        return column_health
    
    def _generate_recommendations(self, metrics: Dict, column_health: Dict) -> List[str]:
        """Generate actionable recommendations based on metrics."""
        recommendations = []
        
        # Freshness recommendations
        if metrics['freshness']['score'] < 80:
            recommendations.append("🕐 Data freshness is degraded. Check upstream pipelines.")
        
        # Completeness recommendations
        if metrics['completeness']['score'] < 90:
            high_null_cols = [col for col, health in column_health.items() 
                            if health['null_pct'] > 20]
            if high_null_cols:
                recommendations.append(
                    f"📊 Columns with high null rates: {', '.join(high_null_cols[:3])}"
                )
        
        # Validity recommendations
        if metrics['validity']['score'] < 80:
            recommendations.append("⚠️ Data validity issues detected. Review data types and formats.")
        
        # Uniqueness recommendations
        if metrics['uniqueness']['score'] < 99:
            recommendations.append("🔄 Duplicate records detected. Check for pipeline replays.")
        
        if not recommendations:
            recommendations.append("✅ All quality metrics are healthy.")
        
        return recommendations

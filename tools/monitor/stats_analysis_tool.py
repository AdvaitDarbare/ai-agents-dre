"""
Tool D: StatsAnalysisTool (The Smart One)
Adaptive outlier detection based on data distribution.
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List
from scipy import stats


class StatsAnalysisTool:
    """
    Statistical profiling with adaptive outlier detection.
    Uses Z-Score for normal distributions, IQR for skewed distributions.
    """
    
    def run(self, df: pd.DataFrame, columns_to_check: List[str] = None) -> Dict[str, Any]:
        """
        Calculate statistics and detect outliers adaptively.
        
        Returns:
            {
                "profiles": {
                    "column_name": {
                        "null_pct": float,
                        "unique_pct": float,
                        "mean": float,
                        "median": float,
                        "std": float,
                        "skewness": float,
                        "kurtosis": float,
                        "outlier_method": "Z-Score" | "IQR",
                        "outlier_count": int,
                        "outlier_indices": list
                    }
                },
                "decision": "CONTINUE"
            }
        """
        if columns_to_check is None:
            # Auto-detect numeric columns
            columns_to_check = df.select_dtypes(include=[np.number]).columns.tolist()
        
        profiles = {}
        
        for col in columns_to_check:
            if col not in df.columns:
                continue
            
            # Basic statistics
            null_pct = (df[col].isna().sum() / len(df)) * 100
            unique_pct = (df[col].nunique() / len(df)) * 100
            
            # Skip non-numeric columns for advanced stats
            if not pd.api.types.is_numeric_dtype(df[col]):
                profiles[col] = {
                    "null_pct": round(null_pct, 2),
                    "unique_pct": round(unique_pct, 2),
                    "type": "non-numeric"
                }
                continue
            
            # Numeric statistics
            clean_data = df[col].dropna()
            
            if len(clean_data) == 0:
                profiles[col] = {
                    "null_pct": 100.0,
                    "error": "All values are null"
                }
                continue
            
            mean_val = clean_data.mean()
            median_val = clean_data.median()
            std_val = clean_data.std()
            skew_val = clean_data.skew()
            kurt_val = clean_data.kurtosis()
            
            # Adaptive outlier detection
            if abs(skew_val) < 1.0:
                # Distribution is relatively normal -> Use Z-Score
                outlier_method = "Z-Score"
                outlier_indices, outlier_count = self._detect_outliers_zscore(
                    df, col, threshold=3.0
                )
            else:
                # Distribution is skewed -> Use IQR
                outlier_method = "IQR"
                outlier_indices, outlier_count = self._detect_outliers_iqr(
                    df, col, multiplier=1.5
                )
            
            profiles[col] = {
                "null_pct": round(null_pct, 2),
                "unique_pct": round(unique_pct, 2),
                "mean": round(mean_val, 4),
                "median": round(median_val, 4),
                "std": round(std_val, 4),
                "min": round(clean_data.min(), 4),
                "max": round(clean_data.max(), 4),
                "skewness": round(skew_val, 4),
                "kurtosis": round(kurt_val, 4),
                "outlier_method": outlier_method,
                "outlier_count": outlier_count,
                "outlier_indices": outlier_indices[:100]  # Limit to first 100
            }
        
        return {
            "profiles": profiles,
            "decision": "CONTINUE"
        }
    
    def _detect_outliers_zscore(self, df: pd.DataFrame, col: str, threshold: float = 3.0) -> tuple:
        """
        Detect outliers using Z-Score method.
        Suitable for normally distributed data.
        """
        clean_data = df[col].dropna()
        z_scores = np.abs(stats.zscore(clean_data))
        outlier_mask = z_scores > threshold
        outlier_indices = clean_data[outlier_mask].index.tolist()
        
        return outlier_indices, len(outlier_indices)
    
    def _detect_outliers_iqr(self, df: pd.DataFrame, col: str, multiplier: float = 1.5) -> tuple:
        """
        Detect outliers using Interquartile Range (IQR) method.
        Suitable for skewed distributions.
        """
        clean_data = df[col].dropna()
        Q1 = clean_data.quantile(0.25)
        Q3 = clean_data.quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - multiplier * IQR
        upper_bound = Q3 + multiplier * IQR
        
        outlier_mask = (clean_data < lower_bound) | (clean_data > upper_bound)
        outlier_indices = clean_data[outlier_mask].index.tolist()
        
        return outlier_indices, len(outlier_indices)

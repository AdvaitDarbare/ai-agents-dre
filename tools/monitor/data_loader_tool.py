"""
Tool B: DataLoaderTool (With Sampling)
Loads CSV/Parquet/JSON with intelligent sampling for large files.
"""
import pandas as pd
from typing import Dict, Any, Optional


class DataLoaderTool:
    """Smart data loader with automatic sampling for large files."""
    
    def __init__(self, sampling_threshold_mb: int = 500):
        self.sampling_threshold_mb = sampling_threshold_mb
    
    def run(self, file_path: str, size_mb: float, sample_rate: float = 0.1) -> Dict[str, Any]:
        """
        Load data file with automatic sampling if too large.
        
        Args:
            file_path: Path to data file
            size_mb: File size in MB (from FileMetadataTool)
            sample_rate: Fraction to sample if file is large (default 10%)
        
        Returns:
            {
                "status": "success" | "error",
                "dataframe": pd.DataFrame or None,
                "sampled": bool,
                "rows_loaded": int,
                "columns": list,
                "dtypes": dict,
                "preview": dict (first 5 rows),
                "decision": "CONTINUE" | "CRITICAL_STOP"
            }
        """
        try:
            # Determine if we need to sample
            should_sample = size_mb > self.sampling_threshold_mb
            
            # Load based on file type
            if file_path.endswith('.csv'):
                df = self._load_csv(file_path, should_sample, sample_rate)
            elif file_path.endswith('.parquet'):
                df = self._load_parquet(file_path, should_sample, sample_rate)
            elif file_path.endswith('.json'):
                df = self._load_json(file_path, should_sample, sample_rate)
            else:
                return {
                    "status": "error",
                    "decision": "CRITICAL_STOP",
                    "reason": f"Unsupported file type: {file_path}"
                }
            
            # Generate summary
            return {
                "status": "success",
                "dataframe": df,
                "sampled": should_sample,
                "sample_rate": sample_rate if should_sample else 1.0,
                "rows_loaded": len(df),
                "columns": df.columns.tolist(),
                "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
                "preview": df.head(5).to_dict('records'),
                "decision": "CONTINUE"
            }
            
        except Exception as e:
            return {
                "status": "error",
                "decision": "CRITICAL_STOP",
                "reason": f"Failed to load file: {str(e)}"
            }
    
    def _load_csv(self, file_path: str, should_sample: bool, sample_rate: float) -> pd.DataFrame:
        """Load CSV with optional sampling."""
        if should_sample:
            # Read with skiprows for sampling
            return pd.read_csv(
                file_path,
                skiprows=lambda i: i > 0 and pd.np.random.random() > sample_rate
            )
        else:
            return pd.read_csv(file_path)
    
    def _load_parquet(self, file_path: str, should_sample: bool, sample_rate: float) -> pd.DataFrame:
        """Load Parquet with optional sampling."""
        df = pd.read_parquet(file_path)
        if should_sample:
            return df.sample(frac=sample_rate, random_state=42)
        return df
    
    def _load_json(self, file_path: str, should_sample: bool, sample_rate: float) -> pd.DataFrame:
        """Load JSON with optional sampling."""
        df = pd.read_json(file_path, lines=True)
        if should_sample:
            return df.sample(frac=sample_rate, random_state=42)
        return df

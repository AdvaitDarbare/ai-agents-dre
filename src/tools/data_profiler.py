"""
Data Profiler Tool - Value-Level Quality Checker

This is the MISSING SENSOR identified in the architecture audit.
It goes beyond schema structure to check actual data VALUES:

1. Range Validation: Is `amount` between min_value and max_value?
2. Uniqueness Enforcement: Is `transaction_id` truly unique (isPrimaryKey)?
3. Custom SQL Checks: Executes the `custom_checks` defined in YAML.
4. Null Enforcement: Checks nullable=false columns for actual nulls.
5. Per-Column Quality Score: Returns a 0-100% score per column.

This bridges the gap between "the data LOOKS right" (schema) and "the data IS right" (values).
"""

import duckdb
import yaml
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field


@dataclass
class ColumnProfile:
    """Profile result for a single column."""
    name: str
    total_rows: int = 0
    null_count: int = 0
    null_rate: float = 0.0
    unique_count: int = 0
    uniqueness_rate: float = 0.0
    min_value: Any = None
    max_value: Any = None
    mean_value: float = None
    violations: List[str] = field(default_factory=list)
    violation_examples: List[dict] = field(default_factory=list)  # List of {type, examples, count}
    quality_score: float = 100.0  # 0-100%
    type: str = "unknown"

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "type": self.type,
            "total_rows": self.total_rows,
            "null_count": self.null_count,
            "null_rate": round(self.null_rate, 4),
            "unique_count": self.unique_count,
            "uniqueness_rate": round(self.uniqueness_rate, 4),
            "min_value": str(self.min_value) if self.min_value is not None else None,
            "max_value": str(self.max_value) if self.max_value is not None else None,
            "mean_value": round(self.mean_value, 4) if self.mean_value is not None else None,
            "violations": self.violations,
            "violation_examples": self.violation_examples,
            "quality_score": round(self.quality_score, 2)
        }


@dataclass
class ProfileReport:
    """Full profiling report for a dataset."""
    dataset_name: str
    total_rows: int = 0
    total_columns: int = 0

    # ⚠️  DEPRECATED: Use weighted dimension scores from DimensionScorer instead
    # Kept for backward compatibility during gradual migration.
    overall_quality_score: float = 100.0
    column_profiles: Dict[str, ColumnProfile] = field(default_factory=dict)
    constraint_violations: List[Dict[str, Any]] = field(default_factory=list)
    custom_check_results: List[Dict[str, Any]] = field(default_factory=list)
    memory_usage_mb: float = 0.0

    def to_dict(self) -> dict:
        return {
            "dataset_name": self.dataset_name,
            "total_rows": self.total_rows,
            "total_columns": self.total_columns,
            "memory_usage_mb": round(self.memory_usage_mb, 2),
            "overall_quality_score": round(self.overall_quality_score, 2),
            "column_profiles": {k: v.to_dict() for k, v in self.column_profiles.items()},
            "constraint_violations": self.constraint_violations,
            "custom_check_results": self.custom_check_results
        }


class DataProfiler:
    """
    The Value-Level Quality Checker.
    
    Goes beyond schema validation to check actual data content
    against the rules defined in the YAML data contract.
    """

    # Numeric types for range checking
    NUMERIC_TYPES = {"integer", "bigint", "smallint", "float", "double", "decimal", "int"}

    def __init__(self, contracts_path: Union[str, Path] = "config/expectations"):
        """Initialize the Data Profiler."""
        self.contracts_path = Path(contracts_path)
        self.logger = logging.getLogger(__name__)

    def profile(self, df: pd.DataFrame, contract_path: Union[str, Path], 
                dataset_name: str = "unknown") -> ProfileReport:
        """
        Profile a DataFrame against its data contract.
        
        Args:
            df: The Pandas DataFrame to profile.
            contract_path: Path to the YAML data contract.
            dataset_name: Name of the dataset for reporting.
            
        Returns:
            ProfileReport with per-column quality scores and violations.
        """
        self.logger.info(f"📊 Starting profile for '{dataset_name}' "
                         f"({len(df)} rows, {len(df.columns)} columns)")
        
        report = ProfileReport(
            dataset_name=dataset_name,
            total_rows=len(df),
            total_columns=len(df.columns),
            memory_usage_mb=df.memory_usage(deep=True).sum() / 1024 / 1024 if not df.empty else 0.0
        )

        # Load the contract
        contract = self._load_contract(contract_path)
        if not contract:
            # Fallback: Profile all columns in DF if no contract exists
            # This enables "Profile -> Propose" workflow
            for col in df.columns:
                # Create a dummy spec
                dtype = str(df[col].dtype)
                col_spec = {"name": col, "data_type": dtype}
                profile = self._profile_column(df, col, col_spec)
                report.column_profiles[col] = profile
            
            quality_config = {}
        else:
            columns_spec = contract.get("columns", [])
            quality_config = contract.get("quality", {})

            # -------------------------------------------------------
            # 1. Per-Column Profiling
            # -------------------------------------------------------
            for col_spec in columns_spec:
                col_name = col_spec.get("name")
                if col_name not in df.columns:
                    # Column missing from data — already caught by SchemaValidator
                    continue

                profile = self._profile_column(df, col_name, col_spec)
                report.column_profiles[col_name] = profile

        # -------------------------------------------------------
        # 2. Row Count Validation (min_rows / max_rows)
        # -------------------------------------------------------
        min_rows = quality_config.get("min_rows")
        max_rows = quality_config.get("max_rows")

        if min_rows is not None and len(df) < min_rows:
            report.constraint_violations.append({
                "type": "ROW_COUNT_BELOW_MIN",
                "severity": "error",
                "message": f"Row count ({len(df)}) is below minimum ({min_rows})",
                "expected": min_rows,
                "actual": len(df)
            })

        if max_rows is not None and len(df) > max_rows:
            report.constraint_violations.append({
                "type": "ROW_COUNT_ABOVE_MAX",
                "severity": "error",
                "message": f"Row count ({len(df)}) exceeds maximum ({max_rows})",
                "expected": max_rows,
                "actual": len(df)
            })

        # -------------------------------------------------------
        # 3. Custom SQL Checks (via DuckDB)
        # -------------------------------------------------------
        custom_checks = quality_config.get("custom_checks", [])
        if custom_checks:
            report.custom_check_results = self._run_custom_checks(df, custom_checks, columns_spec)

        # -------------------------------------------------------
        # 4. Calculate Overall Quality Score
        # -------------------------------------------------------
        report.overall_quality_score = self._calculate_overall_score(report)

        return report

    def analyze(self, file_path: str, table_name: str) -> List[str]:
        """
        Backwards-compatible wrapper returning legacy string errors.

        This keeps older tests/scripts operational while the platform uses the
        structured `ProfileReport` interface.
        """
        path = Path(file_path)
        if not path.exists():
            return [f"❌ FILE ERROR: File not found: {file_path}"]

        try:
            if path.suffix.lower() == ".parquet":
                df = pd.read_parquet(path)
            else:
                df = pd.read_csv(path)
        except Exception as exc:
            return [f"❌ FILE ERROR: Failed to read file: {exc}"]

        contract_path = self.contracts_path / f"{table_name}.yaml"
        report = self.profile(df, contract_path, table_name)

        errors: List[str] = []

        for violation in report.constraint_violations:
            message = violation.get("message", "Unknown constraint violation")
            vtype = violation.get("type", "CONSTRAINT")
            if "ROW_COUNT" in vtype:
                errors.append(f"❌ VOLUME: {message}")
            else:
                errors.append(f"❌ {vtype}: {message}")

        for col_name, col_profile in report.column_profiles.items():
            for violation_msg in col_profile.violations:
                upper = violation_msg.upper()
                if "NOT NULL" in upper:
                    errors.append(f"❌ COMPLETENESS: {col_name} NULL violation - {violation_msg}")
                elif "RANGE" in upper:
                    normalized = (
                        violation_msg.replace("below minimum", "below min value")
                        .replace("above maximum", "above max value")
                    )
                    errors.append(f"❌ RANGE: {col_name} {normalized}")
                elif "PRIMARY KEY" in upper:
                    errors.append(f"❌ UNIQUENESS: {col_name} {violation_msg}")
                elif "PATTERN" in upper:
                    # Legacy analyze() behavior did not fail on regex style checks.
                    continue
                elif "ALLOWED VALUES" in upper:
                    errors.append(f"❌ ALLOWED VALUES: {col_name} {violation_msg}")
                else:
                    errors.append(f"❌ QUALITY: {col_name} {violation_msg}")

        for check in report.custom_check_results:
            if check.get("passed", True):
                continue
            check_name = check.get("name", "Unnamed Check")
            if check.get("error"):
                errors.append(f"❌ CONSISTENCY: {check_name} failed - {check['error']}")
            else:
                violations = check.get("violation_count", 0)
                errors.append(
                    f"❌ CONSISTENCY: {check_name} failed - {violations} rows violate rule"
                )

        return errors

    def _load_contract(self, contract_path: Union[str, Path]) -> Optional[Dict]:
        """Load and parse the YAML data contract."""
        path = Path(contract_path)
        if not path.exists():
            print(f"⚠️ Contract file not found: {path}")
            return None
        try:
            with open(path, "r") as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"❌ Failed to parse contract: {e}")
            return None

    def _profile_column(self, df: pd.DataFrame, col_name: str, 
                        col_spec: Dict) -> ColumnProfile:
        """Profile a single column against its specification."""
        series = df[col_name]
        total = len(series)

        profile = ColumnProfile(
            name=col_name,
            total_rows=total,
            null_count=int(series.isnull().sum()),
            null_rate=float(series.isnull().mean()),
            unique_count=int(series.nunique()),
            uniqueness_rate=float(series.nunique() / total) if total > 0 else 0.0,
            type=str(col_spec.get("data_type", str(series.dtype)))
        )

        violations_count = 0

        # --- Nullable Check ---
        if col_spec.get("nullable") is False and profile.null_count > 0:
            profile.violations.append(
                f"NOT NULL violation: {profile.null_count} null values found "
                f"({profile.null_rate:.1%} of rows)"
            )
            violations_count += profile.null_count

            # Collect sample row indices with null values
            null_indices = df[series.isnull()].index.tolist()[:10]
            null_examples = []
            for idx in null_indices:
                row_dict = df.loc[idx].to_dict()
                # Convert to JSON-serializable types
                row_dict = {k: (None if pd.isna(v) else str(v)) for k, v in row_dict.items()}
                null_examples.append(row_dict)

            profile.violation_examples.append({
                "type": "NULL",
                "count": profile.null_count,
                "examples": null_examples
            })

        # --- Primary Key / Uniqueness Check ---
        if col_spec.get("isPrimaryKey") is True:
            duplicate_count = total - profile.unique_count
            if duplicate_count > 0:
                profile.violations.append(
                    f"PRIMARY KEY violation: {duplicate_count} duplicate values found "
                    f"(uniqueness: {profile.uniqueness_rate:.1%})"
                )
                violations_count += duplicate_count

                # Collect sample duplicate rows
                duplicates = df[series.duplicated(keep=False)].head(10)
                dup_examples = []
                for idx, row in duplicates.iterrows():
                    row_dict = row.to_dict()
                    row_dict = {k: (None if pd.isna(v) else str(v)) for k, v in row_dict.items()}
                    dup_examples.append(row_dict)

                profile.violation_examples.append({
                    "type": "DUPLICATE",
                    "count": duplicate_count,
                    "examples": dup_examples
                })

        # --- Range Checks (for numeric columns) ---
        data_type = col_spec.get("data_type", "").lower()
        if data_type in self.NUMERIC_TYPES or pd.api.types.is_numeric_dtype(series):
            # Force numeric conversion, coercing errors to NaN
            numeric_series = pd.to_numeric(series, errors='coerce')
            non_null = numeric_series.dropna()
            
            if len(non_null) > 0:
                profile.min_value = float(non_null.min())
                profile.max_value = float(non_null.max())
                profile.mean_value = float(non_null.mean())

                # Check min_value constraint
                spec_min = col_spec.get("min_value")
                if spec_min is not None:
                    below_min_mask = non_null < spec_min
                    below_min = below_min_mask.sum()
                    if below_min > 0:
                        profile.violations.append(
                            f"RANGE violation: {below_min} values below minimum ({spec_min}). "
                            f"Actual min: {profile.min_value}"
                        )
                        violations_count += int(below_min)

                        # Get original indices where value is below min
                        below_min_indices = numeric_series[numeric_series < spec_min].index.tolist()[:10]
                        range_examples = []
                        for idx in below_min_indices:
                            row_dict = df.loc[idx].to_dict()
                            row_dict = {k: (None if pd.isna(v) else str(v)) for k, v in row_dict.items()}
                            range_examples.append(row_dict)

                        profile.violation_examples.append({
                            "type": "RANGE_MIN",
                            "count": int(below_min),
                            "examples": range_examples
                        })

                # Check max_value constraint
                spec_max = col_spec.get("max_value")
                if spec_max is not None:
                    above_max_mask = non_null > spec_max
                    above_max = above_max_mask.sum()
                    if above_max > 0:
                        profile.violations.append(
                            f"RANGE violation: {above_max} values above maximum ({spec_max}). "
                            f"Actual max: {profile.max_value}"
                        )
                        violations_count += int(above_max)

                        # Get original indices where value is above max
                        above_max_indices = numeric_series[numeric_series > spec_max].index.tolist()[:10]
                        range_examples = []
                        for idx in above_max_indices:
                            row_dict = df.loc[idx].to_dict()
                            row_dict = {k: (None if pd.isna(v) else str(v)) for k, v in row_dict.items()}
                            range_examples.append(row_dict)

                        profile.violation_examples.append({
                            "type": "RANGE_MAX",
                            "count": int(above_max),
                            "examples": range_examples
                        })

        # --- Regex Pattern Check ---
        pattern = col_spec.get("pattern")
        if pattern and (pd.api.types.is_string_dtype(series) or series.dtype == 'object'):
            import re
            non_null_str = series.dropna().astype(str)
            if len(non_null_str) > 0:
                matches = non_null_str.apply(lambda x: bool(re.match(pattern, x)))
                mismatches = (~matches).sum()
                if mismatches > 0:
                    profile.violations.append(
                        f"PATTERN violation: {mismatches} values don't match '{pattern}' "
                        f"({mismatches/total:.1%} of rows)"
                    )
                    violations_count += int(mismatches)

                    # Collect sample pattern violations
                    mismatch_indices = non_null_str[~matches].index.tolist()[:10]
                    pattern_examples = []
                    for idx in mismatch_indices:
                        row_dict = df.loc[idx].to_dict()
                        row_dict = {k: (None if pd.isna(v) else str(v)) for k, v in row_dict.items()}
                        pattern_examples.append(row_dict)

                    profile.violation_examples.append({
                        "type": "PATTERN",
                        "count": int(mismatches),
                        "examples": pattern_examples
                    })

        # --- Allowed Values Check ---
        allowed_values = col_spec.get("allowed_values")
        if allowed_values and len(allowed_values) > 0:
            non_null_vals = series.dropna()
            if len(non_null_vals) > 0:
                invalid = ~non_null_vals.isin(allowed_values)
                invalid_count = invalid.sum()
                if invalid_count > 0:
                    sample_invalids = list(non_null_vals[invalid].unique()[:5])
                    profile.violations.append(
                        f"ALLOWED VALUES violation: {invalid_count} values not in {allowed_values}. "
                        f"Examples: {sample_invalids}"
                    )
                    violations_count += int(invalid_count)

                    # Collect sample allowed values violations
                    invalid_indices = non_null_vals[invalid].index.tolist()[:10]
                    allowed_examples = []
                    for idx in invalid_indices:
                        row_dict = df.loc[idx].to_dict()
                        row_dict = {k: (None if pd.isna(v) else str(v)) for k, v in row_dict.items()}
                        allowed_examples.append(row_dict)

                    profile.violation_examples.append({
                        "type": "ALLOWED_VALUES",
                        "count": int(invalid_count),
                        "examples": allowed_examples
                    })

        # --- Quality Score ---
        if total > 0:
            profile.quality_score = max(0.0, ((total - violations_count) / total) * 100)
        else:
            profile.quality_score = 0.0

        return profile

    # Types that should be coerced to datetime for DuckDB compatibility
    DATETIME_TYPES = {"date", "timestamp", "datetime"}

    def _run_custom_checks(self, df: pd.DataFrame, 
                           checks: List[Dict],
                           columns_spec: Optional[List[Dict]] = None) -> List[Dict[str, Any]]:
        """
        Execute custom SQL checks defined in the YAML contract.
        Uses DuckDB to run SQL against the DataFrame.
        
        Auto-casts date/timestamp columns to avoid DuckDB type mismatch errors
        (e.g., VARCHAR vs TIMESTAMP when comparing with now()).
        """
        results = []
        
        # Pre-cast DataFrame columns to proper types for DuckDB
        df_cast = df.copy()
        if columns_spec:
            for col_spec in columns_spec:
                col_name = col_spec.get("name", "")
                col_type = col_spec.get("data_type", "").lower()
                if col_name in df_cast.columns and col_type in self.DATETIME_TYPES:
                    try:
                        df_cast[col_name] = pd.to_datetime(df_cast[col_name], errors="coerce")
                    except Exception:
                        pass  # Leave as-is if conversion fails

        conn = duckdb.connect()
        conn.register("data_table", df_cast)

        for check in checks:
            check_name = check.get("name", "Unnamed Check")
            sql_condition = check.get("sql_condition", "")
            severity = check.get("severity", "warning")

            if not sql_condition:
                continue

            try:
                # Count rows that VIOLATE the condition (NOT matching)
                query = f"SELECT COUNT(*) FROM data_table WHERE NOT ({sql_condition})"
                try:
                    violation_count = conn.execute(query).fetchone()[0]
                except Exception as cast_err:
                    if "cast" in str(cast_err).lower() or "compare" in str(cast_err).lower():
                        # DuckDB timestamp precision mismatch — create a view with explicit casts
                        cast_cols = []
                        for col in df_cast.columns:
                            if pd.api.types.is_datetime64_any_dtype(df_cast[col]):
                                cast_cols.append(f"CAST(\"{col}\" AS TIMESTAMP) AS \"{col}\"")
                            else:
                                cast_cols.append(f"\"{col}\"")
                        view_sql = f"CREATE OR REPLACE VIEW data_casted AS SELECT {', '.join(cast_cols)} FROM data_table"
                        conn.execute(view_sql)
                        query_retry = f"SELECT COUNT(*) FROM data_casted WHERE NOT ({sql_condition})"
                        violation_count = conn.execute(query_retry).fetchone()[0]
                    else:
                        raise cast_err
                total_count = len(df)

                passed = violation_count == 0
                results.append({
                    "name": check_name,
                    "severity": severity,
                    "passed": passed,
                    "violation_count": violation_count,
                    "total_rows": total_count,
                    "violation_rate": round(violation_count / total_count, 4) if total_count > 0 else 0,
                    "sql": sql_condition
                })

            except Exception as e:
                results.append({
                    "name": check_name,
                    "severity": severity,
                    "passed": False,
                    "error": str(e),
                    "sql": sql_condition
                })

        conn.close()
        return results

    def _calculate_overall_score(self, report: ProfileReport) -> float:
        """
        Calculate the overall quality score for the dataset.

        ⚠️  DEPRECATED: This simple averaging approach is superseded by the
        weighted 6-dimensional quality scoring in DimensionScorer.

        Kept for backward compatibility with:
        - Legacy tests that assert on this field
        - Gradual migration path for existing pipelines
        - Fallback when dimension scoring fails

        NEW CODE SHOULD USE: DimensionScorer.calculate_dimension_scores()
        which provides industry-standard weighted multi-dimensional scoring.

        Formula: Average of all column quality scores,
                 penalized by constraint violations (-5% each) and
                 failed custom checks (-3% each).
        """
        if not report.column_profiles:
            return 100.0

        # Average column quality
        col_scores = [p.quality_score for p in report.column_profiles.values()]
        avg_col_score = sum(col_scores) / len(col_scores)

        # Penalty for constraint violations (-5% each)
        constraint_penalty = len(report.constraint_violations) * 5.0

        # Penalty for failed custom checks (-3% each)
        custom_penalty = sum(
            1 for r in report.custom_check_results 
            if not r.get("passed", True)
        ) * 3.0

        final_score = avg_col_score - constraint_penalty - custom_penalty
        return max(0.0, min(100.0, final_score))

    def extract_metadata(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Extract deterministic metadata from a DataFrame for LLM consumption.
        
        Returns:
            Dict containing column stats, inferred types, and value samples.
        """
        metadata = {
            "total_rows": len(df),
            "columns": []
        }
        
        for col in df.columns:
            series = df[col]
            col_meta = {
                "name": col,
                "inferred_type": str(series.dtype),
                "nullable": bool(series.isnull().any()),
                "unique_values": int(series.nunique()),
                "percent_unique": round(series.nunique() / len(df), 4) if len(df) > 0 else 0,
                "sample_values": series.dropna().head(5).tolist()
            }
            
            # Heuristic for low cardinality (potential enum/categorical)
            if series.nunique() < 20 and pd.api.types.is_object_dtype(series):
                col_meta["possible_values"] = series.unique().tolist()
                
            # Numeric range
            if pd.api.types.is_numeric_dtype(series):
                col_meta["min_value"] = float(series.min())
                col_meta["max_value"] = float(series.max())
                col_meta["mean_value"] = float(series.mean())
                
            metadata["columns"].append(col_meta)
            
        return metadata


if __name__ == "__main__":
    import json

    # Test with mock data
    df_test = pd.DataFrame({
        "transaction_id": ["txn_1", "txn_2", "txn_3", "txn_1"],  # Duplicate PK!
        "user_id": ["u1", None, "u3", "u4"],  # Null in non-nullable!
        "amount": [100.0, 200.0, -50.0, 15000.0],  # Out of range!
        "timestamp": pd.to_datetime(["2023-01-01"] * 4),
        "status": ["completed", "pending", "completed", "failed"]
    })

    profiler = DataProfiler()
    report = profiler.profile(
        df_test, 
        "config/expectations/transactions.yaml",
        "transactions"
    )

    print("\n🔍 Data Profile Report:")
    print(json.dumps(report.to_dict(), indent=2))
    print(f"\n📊 Overall Quality Score: {report.overall_quality_score:.1f}%")

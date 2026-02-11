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
    quality_score: float = 100.0  # 0-100%

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "total_rows": self.total_rows,
            "null_count": self.null_count,
            "null_rate": round(self.null_rate, 4),
            "unique_count": self.unique_count,
            "uniqueness_rate": round(self.uniqueness_rate, 4),
            "min_value": str(self.min_value) if self.min_value is not None else None,
            "max_value": str(self.max_value) if self.max_value is not None else None,
            "mean_value": round(self.mean_value, 4) if self.mean_value is not None else None,
            "violations": self.violations,
            "quality_score": round(self.quality_score, 2)
        }


@dataclass
class ProfileReport:
    """Full profiling report for a dataset."""
    dataset_name: str
    total_rows: int = 0
    total_columns: int = 0
    overall_quality_score: float = 100.0
    column_profiles: Dict[str, ColumnProfile] = field(default_factory=dict)
    constraint_violations: List[Dict[str, Any]] = field(default_factory=list)
    custom_check_results: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "dataset_name": self.dataset_name,
            "total_rows": self.total_rows,
            "total_columns": self.total_columns,
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

    def __init__(self):
        """Initialize the Data Profiler."""
        pass

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
        report = ProfileReport(
            dataset_name=dataset_name,
            total_rows=len(df),
            total_columns=len(df.columns)
        )

        # Load the contract
        contract = self._load_contract(contract_path)
        if not contract:
            report.constraint_violations.append({
                "type": "CONTRACT_ERROR",
                "message": f"Could not load contract from {contract_path}"
            })
            return report

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
            uniqueness_rate=float(series.nunique() / total) if total > 0 else 0.0
        )

        violations_count = 0

        # --- Nullable Check ---
        if col_spec.get("nullable") is False and profile.null_count > 0:
            profile.violations.append(
                f"NOT NULL violation: {profile.null_count} null values found "
                f"({profile.null_rate:.1%} of rows)"
            )
            violations_count += profile.null_count

        # --- Primary Key / Uniqueness Check ---
        if col_spec.get("isPrimaryKey") is True:
            duplicate_count = total - profile.unique_count
            if duplicate_count > 0:
                profile.violations.append(
                    f"PRIMARY KEY violation: {duplicate_count} duplicate values found "
                    f"(uniqueness: {profile.uniqueness_rate:.1%})"
                )
                violations_count += duplicate_count

        # --- Range Checks (for numeric columns) ---
        data_type = col_spec.get("data_type", "").lower()
        if data_type in self.NUMERIC_TYPES or pd.api.types.is_numeric_dtype(series):
            non_null = series.dropna()
            if len(non_null) > 0:
                profile.min_value = float(non_null.min())
                profile.max_value = float(non_null.max())
                profile.mean_value = float(non_null.mean())

                # Check min_value constraint
                spec_min = col_spec.get("min_value")
                if spec_min is not None:
                    below_min = (non_null < spec_min).sum()
                    if below_min > 0:
                        profile.violations.append(
                            f"RANGE violation: {below_min} values below minimum ({spec_min}). "
                            f"Actual min: {profile.min_value}"
                        )
                        violations_count += int(below_min)

                # Check max_value constraint
                spec_max = col_spec.get("max_value")
                if spec_max is not None:
                    above_max = (non_null > spec_max).sum()
                    if above_max > 0:
                        profile.violations.append(
                            f"RANGE violation: {above_max} values above maximum ({spec_max}). "
                            f"Actual max: {profile.max_value}"
                        )
                        violations_count += int(above_max)

        # --- Regex Pattern Check ---
        pattern = col_spec.get("pattern")
        if pattern and pd.api.types.is_string_dtype(series):
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
        
        Formula: Average of all column quality scores, 
                 penalized by constraint violations.
        """
        if not report.column_profiles:
            return 100.0

        # Average column quality
        col_scores = [p.quality_score for p in report.column_profiles.values()]
        avg_col_score = sum(col_scores) / len(col_scores)

        # Penalty for constraint violations (-5% each)
        constraint_penalty = len(report.constraint_violations) * 5.0

        # Penalty for failed custom checks (-3% each)
        failed_custom = sum(
            1 for r in report.custom_check_results 
            if not r.get("passed", True)
        )
        custom_penalty = failed_custom * 3.0

        return max(0.0, avg_col_score - constraint_penalty - custom_penalty)


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

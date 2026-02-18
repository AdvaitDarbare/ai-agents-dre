"""
Integration Tests for the Agentic Data Reliability Engine

Tests the full pipeline end-to-end across all 5 stages:
  Stage A  — Schema Validation
  Stage A2 — Data Profiling (value-level)
  Stage B  — Anomaly Detection
  Stage C  — Impact Analysis
  Stage D  — Remediation Safety

Run:  PYTHONPATH=. python -m pytest tests/test_integration.py -v
"""

import os
import sys
import yaml
import shutil
import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime

# Ensure the project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.tools.data_profiler import DataProfiler
from src.tools.schema_validator import validate_schema
from src.tools.schema_remediator import SchemaRemediator
from src.tools.anomaly_detector import AnomalyDetector
from src.tools.impact_analyzer import ImpactAnalyzer


# -------------------------------------------------------
# Fixtures
# -------------------------------------------------------

@pytest.fixture
def contracts_path(tmp_path):
    """Create a temporary contract directory with a test YAML."""
    contract = {
        "kind": "DataContract",
        "table_name": "test_data",
        "quality": {
            "min_rows": 5,
            "max_rows": 1000,
            "anomaly_thresholds": {
                "z_score_warning": 2.5,
                "z_score_critical": 3.0,
                "quality_score_warn": 80,
                "quality_score_block": 50,
            },
            "custom_checks": [
                {
                    "name": "Positive Amounts",
                    "sql_condition": "amount > 0",
                    "severity": "error"
                }
            ],
        },
        "columns": [
            {"name": "id", "data_type": "varchar", "nullable": False, "isPrimaryKey": True, "pattern": "^id_\\d+$"},
            {"name": "amount", "data_type": "double", "nullable": False, "min_value": 0.0, "max_value": 999.0},
            {"name": "status", "data_type": "varchar", "nullable": False, "allowed_values": ["active", "closed"]},
            {"name": "created_at", "data_type": "timestamp", "nullable": False},
        ],
    }
    contract_file = tmp_path / "test_data.yaml"
    with open(contract_file, "w") as f:
        yaml.dump(contract, f)
    return tmp_path


@pytest.fixture
def clean_df():
    """A perfectly clean DataFrame that should pass all checks."""
    return pd.DataFrame({
        "id": [f"id_{i}" for i in range(20)],
        "amount": [float(i * 10) for i in range(1, 21)],
        "status": ["active"] * 10 + ["closed"] * 10,
        "created_at": pd.to_datetime(["2023-06-15"] * 20),
    })


@pytest.fixture
def dirty_df():
    """A DataFrame with intentional violations."""
    return pd.DataFrame({
        "id": ["id_1", "id_1", "INVALID", "id_4"],  # duplicate PK + pattern violation
        "amount": [100.0, -50.0, 1500.0, 200.0],     # negative + above max
        "status": ["active", "closed", "UNKNOWN", "active"],  # bad allowed value
        "created_at": pd.to_datetime(["2023-06-15"] * 4),
    })


# -------------------------------------------------------
# Test 1: DataProfiler — Clean Data
# -------------------------------------------------------

class TestDataProfilerClean:
    def test_quality_score_is_100(self, contracts_path, clean_df):
        profiler = DataProfiler()
        report = profiler.profile(clean_df, contracts_path / "test_data.yaml", "test_data")
        assert report.overall_quality_score == 100.0, f"Expected 100%, got {report.overall_quality_score}%"

    def test_no_violations(self, contracts_path, clean_df):
        profiler = DataProfiler()
        report = profiler.profile(clean_df, contracts_path / "test_data.yaml", "test_data")
        for col_name, profile in report.column_profiles.items():
            assert len(profile.violations) == 0, f"Unexpected violations in {col_name}: {profile.violations}"

    def test_custom_sql_checks_pass(self, contracts_path, clean_df):
        profiler = DataProfiler()
        report = profiler.profile(clean_df, contracts_path / "test_data.yaml", "test_data")
        for check in report.custom_check_results:
            assert check["passed"], f"Custom check failed: {check['name']}"


# -------------------------------------------------------
# Test 2: DataProfiler — Dirty Data
# -------------------------------------------------------

class TestDataProfilerDirty:
    def test_quality_below_100(self, contracts_path, dirty_df):
        profiler = DataProfiler()
        report = profiler.profile(dirty_df, contracts_path / "test_data.yaml", "test_data")
        assert report.overall_quality_score < 100.0

    def test_pk_violation_detected(self, contracts_path, dirty_df):
        profiler = DataProfiler()
        report = profiler.profile(dirty_df, contracts_path / "test_data.yaml", "test_data")
        id_violations = report.column_profiles["id"].violations
        assert any("PRIMARY KEY" in v for v in id_violations)

    def test_range_violation_detected(self, contracts_path, dirty_df):
        profiler = DataProfiler()
        report = profiler.profile(dirty_df, contracts_path / "test_data.yaml", "test_data")
        amount_violations = report.column_profiles["amount"].violations
        assert any("RANGE" in v for v in amount_violations)

    def test_pattern_violation_detected(self, contracts_path, dirty_df):
        profiler = DataProfiler()
        report = profiler.profile(dirty_df, contracts_path / "test_data.yaml", "test_data")
        id_violations = report.column_profiles["id"].violations
        assert any("PATTERN" in v for v in id_violations)

    def test_allowed_values_violation_detected(self, contracts_path, dirty_df):
        profiler = DataProfiler()
        report = profiler.profile(dirty_df, contracts_path / "test_data.yaml", "test_data")
        status_violations = report.column_profiles["status"].violations
        assert any("ALLOWED VALUES" in v for v in status_violations)

    def test_custom_sql_fails_for_negative_amount(self, contracts_path, dirty_df):
        profiler = DataProfiler()
        report = profiler.profile(dirty_df, contracts_path / "test_data.yaml", "test_data")
        positive_check = [c for c in report.custom_check_results if c["name"] == "Positive Amounts"]
        assert len(positive_check) == 1
        assert not positive_check[0]["passed"]


# -------------------------------------------------------
# Test 3: SchemaRemediator Safety Gates
# -------------------------------------------------------

class TestSchemaRemediatorSafety:
    def test_invalid_yaml_rejected(self):
        remediator = SchemaRemediator()
        result = remediator._validate_yaml("this: is: not: valid: yaml: [[[")
        # PyYAML may or may not parse weird YAML — test a really broken one
        bad_yaml = "```yaml\ncolumns:\n  - {broken"
        result = remediator._validate_yaml(bad_yaml)
        assert result is False

    def test_missing_columns_key_rejected(self):
        remediator = SchemaRemediator()
        result = remediator._validate_yaml("table_name: test\nno_columns_here: true")
        assert result is False

    def test_valid_yaml_accepted(self):
        remediator = SchemaRemediator()
        valid = "table_name: test\ncolumns:\n  - name: id\n    data_type: varchar"
        result = remediator._validate_yaml(valid)
        assert result is True

    def test_column_removal_blocked(self):
        remediator = SchemaRemediator()
        original = "columns:\n  - name: id\n  - name: amount\n  - name: status"
        proposed = "columns:\n  - name: id\n  - name: amount"  # status removed
        result = remediator._validate_no_columns_removed(original, proposed)
        assert result is False

    def test_column_addition_allowed(self):
        remediator = SchemaRemediator()
        original = "columns:\n  - name: id\n  - name: amount"
        proposed = "columns:\n  - name: id\n  - name: amount\n  - name: new_col"
        result = remediator._validate_no_columns_removed(original, proposed)
        assert result is True

    def test_backup_creates_file(self, tmp_path):
        # Create a test file
        test_file = tmp_path / "test.yaml"
        test_file.write_text("original content")
        
        backup_path = SchemaRemediator.create_backup(str(test_file))
        
        assert backup_path != ""
        assert Path(backup_path).exists()
        assert Path(backup_path).read_text() == "original content"


# -------------------------------------------------------
# Test 4: Impact Analyzer
# -------------------------------------------------------

class TestImpactAnalyzer:
    def test_high_criticality_detected(self, tmp_path):
        lineage = {
            "datasets": {
                "critical_data": {
                    "consumers": [
                        {"name": "CEO Dashboard", "type": "dashboard", "criticality": "HIGH"}
                    ]
                }
            }
        }
        lineage_file = tmp_path / "lineage.yaml"
        with open(lineage_file, "w") as f:
            yaml.dump(lineage, f)
        
        analyzer = ImpactAnalyzer(str(lineage_file))
        impact = analyzer.get_downstream_impact("critical_data")
        assert impact["overall_criticality"] == "HIGH"

    def test_unknown_dataset_returns_low(self, tmp_path):
        lineage = {"datasets": {}}
        lineage_file = tmp_path / "lineage.yaml"
        with open(lineage_file, "w") as f:
            yaml.dump(lineage, f)
        
        analyzer = ImpactAnalyzer(str(lineage_file))
        impact = analyzer.get_downstream_impact("nonexistent")
        assert impact["overall_criticality"] == "LOW"


# -------------------------------------------------------
# Test 5: Configurable Thresholds
# -------------------------------------------------------

class TestConfigurableThresholds:
    def test_thresholds_loaded_from_yaml(self, contracts_path):
        with open(contracts_path / "test_data.yaml") as f:
            contract = yaml.safe_load(f)
        
        thresholds = contract["quality"]["anomaly_thresholds"]
        assert thresholds["z_score_warning"] == 2.5
        assert thresholds["z_score_critical"] == 3.0
        assert thresholds["quality_score_warn"] == 80
        assert thresholds["quality_score_block"] == 50


# -------------------------------------------------------
# Test 6: Null Rate Tracking
# -------------------------------------------------------

class TestNullRateTracking:
    def test_null_rates_in_profile(self, contracts_path, dirty_df):
        profiler = DataProfiler()
        report = profiler.profile(dirty_df, contracts_path / "test_data.yaml", "test_data")
        
        for col_name, profile in report.column_profiles.items():
            assert hasattr(profile, "null_rate")
            assert 0.0 <= profile.null_rate <= 1.0


# -------------------------------------------------------
# Test 7: Auto-Discovery (Phase 1)
# -------------------------------------------------------

class TestAutoDiscovery:
    def test_discovers_all_contracts(self, tmp_path):
        """discover_datasets() should find all .yaml files in contracts dir."""
        from src.agents.monitor_agent import MonitorAgent
        
        # Put contracts in a subdirectory to isolate from lineage file
        contracts_dir = tmp_path / "contracts"
        contracts_dir.mkdir()
        
        for name in ["alpha", "beta"]:
            contract = {
                "info": {"owner": "Test Team", "domain": "test", "version": "1.0.0"},
                "columns": [{"name": "id", "data_type": "varchar"}],
                "quality": {},
            }
            with open(contracts_dir / f"{name}.yaml", "w") as f:
                yaml.dump(contract, f)
        
        lineage = {"datasets": {"alpha": {"consumers": [{"name": "Dashboard", "criticality": "HIGH"}]}}}
        lineage_file = tmp_path / "lineage.yaml"
        with open(lineage_file, "w") as f:
            yaml.dump(lineage, f)
        
        agent = MonitorAgent(contracts_path=str(contracts_dir), lineage_path=str(lineage_file))
        datasets = agent.discover_datasets()
        
        names = [ds["name"] for ds in datasets]
        assert "alpha" in names
        assert "beta" in names
        # Runtime may include unmanaged datasets discovered from local data dirs.
        assert len(datasets) >= 2

    def test_skips_backup_files(self, tmp_path):
        """discover_datasets() should ignore .backup_* files."""
        from src.agents.monitor_agent import MonitorAgent
        
        contracts_dir = tmp_path / "contracts"
        contracts_dir.mkdir()
        
        contract = {"columns": [{"name": "id", "data_type": "varchar"}], "quality": {}}
        with open(contracts_dir / "data.yaml", "w") as f:
            yaml.dump(contract, f)
        with open(contracts_dir / "data.backup_20260211_120000", "w") as f:
            yaml.dump(contract, f)
        
        lineage_file = tmp_path / "lineage.yaml"
        with open(lineage_file, "w") as f:
            yaml.dump({"datasets": {}}, f)
        
        agent = MonitorAgent(contracts_path=str(contracts_dir), lineage_path=str(lineage_file))
        datasets = agent.discover_datasets()

        names = [d["name"] for d in datasets]
        assert "data" in names
        assert all(".backup_" not in name for name in names)

    def test_returns_metadata_fields(self, tmp_path):
        """Each discovered dataset should have the expected metadata fields."""
        from src.agents.monitor_agent import MonitorAgent
        
        contracts_dir = tmp_path / "contracts"
        contracts_dir.mkdir()
        
        contract = {
            "info": {"owner": "Data Team", "domain": "finance", "version": "2.0.0"},
            "columns": [{"name": "a"}, {"name": "b"}, {"name": "c"}],
            "quality": {"custom_checks": [{"name": "test", "sql_condition": "1=1"}]},
        }
        with open(contracts_dir / "finance_data.yaml", "w") as f:
            yaml.dump(contract, f)
        
        lineage_file = tmp_path / "lineage.yaml"
        with open(lineage_file, "w") as f:
            yaml.dump({"datasets": {}}, f)
        
        agent = MonitorAgent(contracts_path=str(contracts_dir), lineage_path=str(lineage_file))
        datasets = agent.discover_datasets()
        
        ds = datasets[0]
        assert ds["name"] == "finance_data"
        assert ds["column_count"] == 3
        assert ds["owner"] == "Data Team"
        assert ds["domain"] == "finance"
        assert ds["version"] == "2.0.0"
        assert ds["has_quality_rules"] is True
        assert ds["lifecycle"] == "active"

    def test_evaluate_all_skips_missing_data(self, tmp_path):
        """evaluate_all() should skip datasets without data files."""
        from src.agents.monitor_agent import MonitorAgent
        
        contracts_dir = tmp_path / "contracts"
        contracts_dir.mkdir()
        
        contract = {"columns": [{"name": "id", "data_type": "varchar"}], "quality": {}}
        with open(contracts_dir / "no_data.yaml", "w") as f:
            yaml.dump(contract, f)
        
        lineage_file = tmp_path / "lineage.yaml"
        with open(lineage_file, "w") as f:
            yaml.dump({"datasets": {}}, f)
        
        agent = MonitorAgent(contracts_path=str(contracts_dir), lineage_path=str(lineage_file))
        result = agent.evaluate_all(data_dir=str(tmp_path / "nonexistent"))
        
        assert result["summary"]["skipped"] == 1
        assert result["results"]["no_data"]["status"] == "SKIPPED"


# -------------------------------------------------------
# Test 8: System Tables (Phase 3)
# -------------------------------------------------------

class TestSystemTables:
    def test_run_history_table_created(self, tmp_path):
        """PostgreSQL system tables should be created on detector init."""
        _ = tmp_path  # fixture retained for compatibility with existing test signature
        detector = AnomalyDetector()
        _ = detector

        from src.utils.database import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.run_history')")
                assert cur.fetchone()[0] is not None
                cur.execute("SELECT to_regclass('public.learned_thresholds')")
                assert cur.fetchone()[0] is not None
                cur.execute("SELECT to_regclass('public.dataset_registry')")
                assert cur.fetchone()[0] is not None

    def test_save_run_to_history(self, tmp_path):
        """save_run_to_history should insert a row into run_history (PostgreSQL)."""
        _ = tmp_path
        detector = AnomalyDetector()
        
        run_id = detector.save_run_to_history(
            dataset_name="test_data",
            status="PASSED",
            quality_score=98.5,
            anomaly_count=0,
            z_score_max=0.5,
            reason="All checks passed.",
            duration_ms=1200,
        )
        
        assert run_id is not None

        from src.utils.database import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, dataset_name, status, quality_score
                    FROM run_history
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                row = cur.fetchone()
        
        assert row is not None
        assert row[1] == "test_data"  # dataset_name
        assert row[2] == "PASSED"     # status
        assert float(row[3]) == 98.5  # quality_score

    def test_dataset_registry_upsert(self, tmp_path):
        """update_dataset_registry should insert then update on subsequent calls (PostgreSQL)."""
        _ = tmp_path
        detector = AnomalyDetector()

        from src.utils.database import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT scan_count FROM dataset_registry WHERE dataset_name = %s",
                    ("alpha",),
                )
                before = cur.fetchone()
                before_count = int(before[0]) if before and before[0] is not None else 0
        
        detector.update_dataset_registry("alpha", "/path/alpha.yaml", "active", "HIGH", "PASSED", 1000.0)
        detector.update_dataset_registry("alpha", "/path/alpha.yaml", "active", "HIGH", "BLOCKED", 2000.0)

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT scan_count, last_status, last_file_mtime
                    FROM dataset_registry
                    WHERE dataset_name = %s
                    """,
                    ("alpha",),
                )
                row = cur.fetchone()
        
        assert row[0] == before_count + 2  # scan_count incremented by two updates
        assert row[1] == "BLOCKED"  # updated status
        assert row[2] == 2000.0  # updated mtime

    def test_learned_threshold_upsert(self, tmp_path):
        """save_learned_threshold should upsert (replace) on repeated calls (PostgreSQL)."""
        _ = tmp_path
        detector = AnomalyDetector()
        
        detector.save_learned_threshold("ds", "row_count", 1000.0, 50.0, "global", 10)
        detector.save_learned_threshold("ds", "row_count", 1050.0, 45.0, "seasonal", 15)

        from src.utils.database import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT baseline_mean
                    FROM learned_thresholds
                    WHERE dataset_name = %s AND metric_name = %s
                    """,
                    ("ds", "row_count"),
                )
                rows = cur.fetchall()
        
        assert len(rows) == 1  # Only one row (upserted)
        assert float(rows[0][0]) == 1050.0  # Updated mean


# -------------------------------------------------------
# Test 9: Intelligent Scan Scheduling (Phase 2)
# -------------------------------------------------------

class TestScanScheduling:
    def test_skip_unchanged_works(self, tmp_path):
        """evaluate_all with skip_unchanged=True should skip files that haven't changed."""
        from src.agents.monitor_agent import MonitorAgent
        
        contracts_dir = tmp_path / "contracts"
        contracts_dir.mkdir()
        
        # Create a minimal contract
        contract = {
            "columns": [{"name": "id", "data_type": "varchar", "nullable": False}],
            "quality": {},
        }
        with open(contracts_dir / "stable.yaml", "w") as f:
            yaml.dump(contract, f)
        
        # Create a data file
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        pd.DataFrame({"id": ["a", "b", "c"]}).to_csv(data_dir / "stable.csv", index=False)
        
        lineage_file = tmp_path / "lineage.yaml"
        with open(lineage_file, "w") as f:
            yaml.dump({"datasets": {}}, f)
        
        agent = MonitorAgent(contracts_path=str(contracts_dir), lineage_path=str(lineage_file))
        
        # First run: should evaluate normally
        result1 = agent.evaluate_all(data_dir=str(data_dir), skip_unchanged=False)
        assert result1["summary"]["skipped"] == 0 or result1["results"]["stable"]["status"] != "UNCHANGED"
        
        # Second run with skip_unchanged: should detect no change
        result2 = agent.evaluate_all(data_dir=str(data_dir), skip_unchanged=True)
        assert result2["results"]["stable"]["status"] == "UNCHANGED"
        assert result2["summary"]["unchanged"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

"""
Monitor Agent - Sentinel of Data Quality

This orchestrator ties together all data observability tools.
It acts as the "Gatekeeper" before data is loaded into the Data Warehouse.

Key Responsibilities:
1. Schema Validation (Hard Gate): Blocks missing columns/type mismatches.
2. Anomaly Detection (Soft Gate): Checks for drift and volume anomalies.
3. Impact Analysis (Context): Decides if an anomaly is critical based on lineage.
4. LLM Reasoning: Analyzing the combined report to produce actionable advice.

Outputs:
A structured JSON verdict + Human-readable summary.
"""

import os
import pandas as pd
import json
import time
import re
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

# Tool Imports
from src.contracts.store import ContractStore, FileContractStore
from src.tools.contract_diff import merge_contracts
from src.tools.schema_validator import validate_schema, ValidationResult, ValidationStatus
from src.tools.anomaly_detector import AnomalyDetector
from src.tools.impact_analyzer import ImpactAnalyzer
from src.tools.doris_loader import DorisLoader
from src.tools.schema_remediator import SchemaRemediator
from src.utils.tool_logger import ToolLogger
from src.tools.data_profiler import DataProfiler
from src.tools.system_health import SystemHealthCheck
from src.tools.alert_router import AlertRouter
from src.tools.dimension_scorer import DimensionScorer

# Agno Agent Imports
from agno.agent import Agent
from agno.models.openai import OpenAIChat

class MonitorAgent:
    """
    The Agentic Orchestrator - Coordinates detection, impact analysis, and decision making.
    """
    
    def __init__(
        self,
        contracts_path: str = "config/expectations",
        lineage_path: str = "config/lineage.yaml",
        contract_store: Optional[ContractStore] = None,
    ):
        """
        Initialize the Monitor Agent with all sub-tools.
        """
        self.contract_store = contract_store or FileContractStore(contracts_path)
        self.contracts_path = Path(getattr(self.contract_store, "root_path", contracts_path))
        
        # Initialize Detectors
        # SchemaValidator is functional, so we initiate it per run usually, 
        # but here we keep paths ready.
        
        self.anomaly_detector = AnomalyDetector()
        self.impact_analyzer = ImpactAnalyzer(lineage_path)
        self.loader = DorisLoader()
        self.remediator = SchemaRemediator()
        self.profiler = DataProfiler(contracts_path=str(self.contracts_path))
        self.system_health = SystemHealthCheck()
        self.alert_router = AlertRouter()
        self.dimension_scorer = None  # Will be created per-dataset with custom weights in evaluate_data_file()
        
        # Initialize the Reasoning Engine (LLM)
        # Using Agno's Agent with OpenAI
        self.reasoning_agent = Agent(
            model=OpenAIChat(id=os.getenv("OPENAI_MODEL_NAME", "gpt-4o")),
            description="You are a Senior Data Reliability Engineer. You analyze data quality reports and recommend actions.",
            instructions=[
                "Analyze the provided JSON verdict from the data pipeline.",
                "If status is BLOCKED, explain exactly why (e.g. schema violation).",
                "If status is WARNING, explain the anomaly and why we are allowing it (e.g. low impact).",
                "If status is PASSED, confirm data is clean.",
                "Provide specific, technical advice on next steps (e.g. 'Update schema.yaml', 'Quarantine file').",
                "Do not be generic. Use the specific metric names and values provided."
            ],
            markdown=True
        )

    def propose_contract(self, dataset_name: str, data_path: str, include_metadata: bool = False) -> Any:
        # Orchestrate the contract generation using datacontract-cli.
        # If an existing contract exists, we merge the new schema into it to preserve metadata.
        print(f"🕵️ Profiling data for {dataset_name} using datacontract-cli...")
        
        # 1. Generate new schema from data (Observation)
        generation = self.remediator.generate_initial_contract_with_report(data_path, dataset_name)
        observed_yaml = generation.yaml_content

        # 2. Check for existing contract
        existing_contract = self.contract_store.read(dataset_name)
        final_yaml = observed_yaml
        
        if existing_contract:
            print(f"📄 Found existing contract for {dataset_name}. Merging metadata...")
            try:
                current_yaml = existing_contract.content
                
                # Merge observed columns/types into current metadata
                merged_yaml, _ = merge_contracts(current_yaml, observed_yaml)
                final_yaml = merged_yaml
            except Exception as e:
                print(f"⚠️ Failed to merge with existing contract: {e}. Returning fresh generation.")

        if include_metadata:
            payload = generation.to_dict()
            # Override content with merged version
            payload["yaml_content"] = final_yaml
            payload["dataset_name"] = dataset_name
            payload["source_path"] = data_path
            return payload
            
        return final_yaml

    def check_timeliness(self, file_path: str, max_age_hours: float = 24) -> tuple[bool, Optional[str]]:
        """
        Compatibility helper for timeliness checks used by legacy tests/scripts.
        """
        path = Path(file_path)
        if not path.exists():
            return False, f"❌ TIMELINESS: File not found: {file_path}"

        import time as _time

        age_hours = (_time.time() - path.stat().st_mtime) / 3600.0
        if age_hours >= max_age_hours:
            return (
                False,
                f"❌ TIMELINESS: File is stale ({age_hours:.1f} hours old, threshold: {max_age_hours} hours).",
            )

        return True, None

    def _parse_duration_to_minutes(self, value: Any) -> Optional[float]:
        """
        Parse duration values like '6h', '30m', '1d', or numeric minutes.
        """
        if value is None:
            return None

        if isinstance(value, (int, float)):
            return float(value)

        text = str(value).strip().lower()
        if not text:
            return None

        match = re.match(r"^(\d+(?:\.\d+)?)\s*([mhd])?$", text)
        if not match:
            return None

        amount = float(match.group(1))
        unit = match.group(2) or "m"
        if unit == "m":
            return amount
        if unit == "h":
            return amount * 60.0
        if unit == "d":
            return amount * 1440.0
        return None

    def _extract_slo_targets(self, contract_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Read SLO targets from the contract with sensible defaults.
        Supported keys:
        quality:
          slos:
            min_quality_score: 85
            max_anomaly_count: 0
            max_freshness_minutes: 360
            freshness_sla: "6h"
          freshness_sla: "6h"   # legacy shorthand
        """
        quality = contract_data.get("quality", {}) if isinstance(contract_data, dict) else {}
        slo_cfg = quality.get("slos", {})
        if not isinstance(slo_cfg, dict):
            slo_cfg = {}

        freshness_raw = (
            slo_cfg.get("max_freshness_minutes")
            if slo_cfg.get("max_freshness_minutes") is not None
            else slo_cfg.get("freshness_sla")
        )
        if freshness_raw is None:
            freshness_raw = quality.get("freshness_sla")

        return {
            "min_quality_score": float(slo_cfg.get("min_quality_score", 80.0)),
            "max_anomaly_count": int(slo_cfg.get("max_anomaly_count", 0)),
            "freshness_max_minutes": self._parse_duration_to_minutes(freshness_raw),
            "source": "contract" if bool(slo_cfg or quality.get("freshness_sla")) else "default",
        }

    def _evaluate_slos(self, dataset_name: str, verdict: Dict[str, Any], file_path: str) -> Dict[str, Any]:
        """
        Evaluate per-run SLO compliance and return normalized SLO checks.
        """
        contract_data: Dict[str, Any] = {}
        try:
            contract_doc = self.contract_store.read(dataset_name)
            if contract_doc:
                import yaml as _yaml

                contract_data = _yaml.safe_load(contract_doc.content) or {}
        except Exception:
            contract_data = {}

        targets = self._extract_slo_targets(contract_data)
        checks: List[Dict[str, Any]] = []

        def add_check(name: str, operator: str, target: float, observed: float, metadata: Dict[str, Any]):
            passed = False
            if operator == ">=":
                passed = observed >= target
            elif operator == "<=":
                passed = observed <= target
            checks.append(
                {
                    "slo_name": name,
                    "operator": operator,
                    "target": float(target),
                    "observed": float(observed),
                    "status": "PASS" if passed else "FAIL",
                    "error_budget_burn": 0.0 if passed else 1.0,
                    "metadata": metadata,
                }
            )

        status = verdict.get("status", "UNKNOWN")
        add_check(
            name="availability",
            operator=">=",
            target=1.0,
            observed=1.0 if status != "BLOCKED" else 0.0,
            metadata={"source": "system", "description": "Run status must not be BLOCKED"},
        )

        quality_score = verdict.get("profile", {}).get(
            "weighted_quality_score",
            verdict.get("profile", {}).get("overall_quality_score", 0.0),
        )
        add_check(
            name="quality_score_min",
            operator=">=",
            target=targets["min_quality_score"],
            observed=quality_score,
            metadata={"source": targets["source"]},
        )

        anomaly_count = float(len(verdict.get("anomalies", [])))
        add_check(
            name="anomaly_count_max",
            operator="<=",
            target=float(targets["max_anomaly_count"]),
            observed=anomaly_count,
            metadata={"source": targets["source"]},
        )

        freshness_target = targets.get("freshness_max_minutes")
        if freshness_target is not None:
            freshness_age = None
            try:
                freshness_age = max(0.0, (time.time() - Path(file_path).stat().st_mtime) / 60.0)
            except Exception:
                freshness_age = None

            if freshness_age is not None:
                add_check(
                    name="freshness_age_minutes_max",
                    operator="<=",
                    target=float(freshness_target),
                    observed=float(freshness_age),
                    metadata={"source": targets["source"]},
                )

        pass_count = sum(1 for c in checks if c["status"] == "PASS")
        total = len(checks)
        return {
            "dataset_name": dataset_name,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "overall_status": "PASS" if pass_count == total else "FAIL",
            "pass_rate": round((pass_count / total) * 100, 2) if total else 100.0,
            "checks": checks,
        }

    def evaluate_data_file(self, file_path: str, dataset_name: str) -> Dict[str, Any]:
        """
        Execute the Sequential Logic Pipeline to evaluate a data file.

        Args:
            file_path: Path to the CSV/Parquet file
            dataset_name: Name of the dataset (e.g. 'transactions')

        Returns:
            Structured dictionary containing the final verdict.
        """
        import time as _time
        import uuid
        _start_time = _time.time()

        # Generate run_id at the start for tool logging
        run_id = str(uuid.uuid4())
        tool_logger = ToolLogger(run_id=run_id, dataset_name=dataset_name)

        verdict = {
            "status": "PASSED",
            "reason": "All checks passed.",
            "anomalies": [],
            "schema_evolution": {"new_columns": [], "missing_columns": []},
            "actions": ["Proceed to Load"],
            "dataset": dataset_name,
            "timestamp": datetime.now().isoformat(),
            "run_id": run_id
        }
        
        # 0. Load Data
        file_ext = Path(file_path).suffix.lower()
        if file_ext == ".parquet":
            source_type = "parquet"
        elif file_ext == ".json":
            source_type = "json"
        else:
            source_type = "csv"

        try:
            if source_type == "parquet":
                df = pd.read_parquet(file_path)
            elif source_type == "json":
                df = pd.read_json(file_path)
            else:
                df = pd.read_csv(file_path)
        except FileNotFoundError:
            return {
                "status": "BLOCKED", 
                "reason": f"File not found: {file_path}",
                "actions": ["Abort"]
            }
        except Exception as e:
            return {
                "status": "BLOCKED",
                "reason": f"Failed to load file: {str(e)}",
                "actions": ["Abort"]
            }

        # ---------------------------------------------------------
        # Stage A: Schema Validation (The "Hard" Gate)
        # ---------------------------------------------------------
        print(f"\n🔍 [Stage A] Validating Schema for '{dataset_name}'...")
        contract_file = self.contract_store.path_for(dataset_name)

        # We use the functional wrapper from schema_validator
        # but we need to pass the file_path, not the dataframe directly to the existing tool
        # The existing tool reads the file itself.
        # Ideally, we'd refactor to accept DF, but for now let's pass file path.
        _schema_start = time.time()
        schema_result = validate_schema(contract_file, file_path, source_type=source_type)
        _schema_duration = int((time.time() - _schema_start) * 1000)
        schema_diff = schema_result.get_schema_diff()

        # Log schema validation
        tool_logger.log_simple(
            tool_name="schema_validator",
            status="SUCCESS" if schema_result.is_valid else "FAILED",
            output=schema_result.to_dict(),
            duration_ms=_schema_duration
        )
        
        # Store diff in verdict
        verdict["schema_evolution"] = schema_diff
        verdict["schema_result"] = schema_result.to_dict() if hasattr(schema_result, "to_dict") else {}
        
        # Logic: Breaking vs Non-Breaking
        if schema_diff["missing_columns"] or schema_diff["type_mismatches"]:
            error_parts = []
            if schema_diff["missing_columns"]:
                error_parts.append(f"Missing columns: {', '.join(schema_diff['missing_columns'])}")
            if schema_diff["type_mismatches"]:
                mismatches = [f"{m['column']} (expected {m['expected']}, got {m['actual']})" for m in schema_diff["type_mismatches"]]
                error_parts.append(f"Type mismatches: {', '.join(mismatches)}")
            
            verdict["status"] = "BLOCKED"
            verdict["reason"] = f"Schema Violation: {'; '.join(error_parts)}"
            verdict["actions"] = ["Quarantine", "Fix Schema/Data"]
            verdict["load_status"] = "SKIPPED (Blocked by Agent)"

            # Even on schema-blocking runs, compute and persist a minimal 6D snapshot
            # so /quality-dimensions can explain *why* the run failed.
            try:
                custom_weights = DimensionScorer.load_weights_from_contract(contract_file)
                dimension_scorer = DimensionScorer(weights=custom_weights)
                dimension_report = dimension_scorer.calculate_dimension_scores(
                    dataset_name=dataset_name,
                    schema_result=verdict["schema_result"],
                    profile_report={},
                    anomaly_report={"status": "PASS", "anomalies": [], "metrics": {}},
                )
                verdict["quality_dimensions"] = dimension_report.to_dict()
            except Exception as dim_err:
                print(f"⚠️ Failed to compute blocked-run dimension scores: {dim_err}")

            self._record_run(dataset_name, verdict, file_path, _start_time)
            return self._enrich_with_llm(verdict)
            
        if schema_diff["new_columns"]:
            print(f"⚠️  Schema Evolution Detected: {len(schema_diff['new_columns'])} new columns.")
            # We don't block, but we note it.

        # Load configurable thresholds from the contract
        import yaml as _yaml
        _contract_data = {}
        try:
            contract_doc = self.contract_store.read(dataset_name)
            if contract_doc:
                _contract_data = _yaml.safe_load(contract_doc.content) or {}
        except Exception:
            pass
        _thresholds = _contract_data.get("quality", {}).get("anomaly_thresholds", {})
        z_warn = _thresholds.get("z_score_warning", 2.5)
        z_critical = _thresholds.get("z_score_critical", 3.0)
        qs_warn = _thresholds.get("quality_score_warn", 80)
        qs_block = _thresholds.get("quality_score_block", 50)
        slo_targets = self._extract_slo_targets(_contract_data)

        # ---------------------------------------------------------
        # Stage A2: Data Profiling (Value-Level Quality)
        # ---------------------------------------------------------
        print(f"\n🔬 [Stage A2] Profiling Data Values for '{dataset_name}'...")
        _profile_start = time.time()
        profile_report = self.profiler.profile(df, contract_file, dataset_name)
        _profile_duration = int((time.time() - _profile_start) * 1000)

        verdict["profile"] = {
            "overall_quality_score": profile_report.overall_quality_score,
            "constraint_violations": profile_report.constraint_violations,
            "custom_check_results": profile_report.custom_check_results,
            "column_scores": {k: v.quality_score for k, v in profile_report.column_profiles.items()},
            "null_rates": {k: v.null_rate for k, v in profile_report.column_profiles.items()},
            "violations_detail": {k: v.violations for k, v in profile_report.column_profiles.items() if v.violations},
            "column_profiles": {k: v.to_dict() for k, v in profile_report.column_profiles.items()}
        }

        # Log data profiling
        tool_logger.log_simple(
            tool_name="data_profiler",
            status="SUCCESS",
            output={
                "overall_quality_score": profile_report.overall_quality_score,
                "total_violations": len(profile_report.constraint_violations),
                "column_count": len(profile_report.column_profiles)
            },
            duration_ms=_profile_duration
        )

        # ---------------------------------------------------------
        # Stage A3: Calculate 6-Dimensional Quality Scores (EARLY)
        # ---------------------------------------------------------
        print(f"\n📊 [Stage A3] Calculating 6-Dimensional Quality Scores...")

        # Load custom weights from YAML contract
        _weights_start = time.time()
        custom_weights = DimensionScorer.load_weights_from_contract(contract_file)

        # Initialize scorer with custom or default weights
        dimension_scorer = DimensionScorer(weights=custom_weights)
        _weights_duration = int((time.time() - _weights_start) * 1000)

        print(f"   Using {'custom' if custom_weights else 'default'} weights: " +
              f"Completeness {dimension_scorer.weights['Completeness']*100:.0f}%, " +
              f"Validity {dimension_scorer.weights['Validity']*100:.0f}%, " +
              f"Accuracy {dimension_scorer.weights['Accuracy']*100:.0f}%")

        # Calculate dimension scores
        _dimension_start = time.time()
        try:
            dimension_report = dimension_scorer.calculate_dimension_scores(
                dataset_name=dataset_name,
                schema_result=schema_result.to_dict() if hasattr(schema_result, 'to_dict') else {},
                profile_report=verdict.get("profile", {}),
                anomaly_report={
                    "status": "PASS",  # Anomaly detection hasn't run yet
                    "anomalies": [],
                    "metrics": {}
                }
            )

            # Store in verdict for quality gates
            verdict["quality_dimensions"] = dimension_report.to_dict()

            # Use dimension overall_score as primary quality metric
            weighted_quality_score = dimension_report.overall_score
            verdict["profile"]["weighted_quality_score"] = weighted_quality_score

            # Extract dimension breakdown for logging
            dim_scores = {d.name: d.score for d in dimension_report.dimensions}

            print(f"   Weighted Quality Score: {weighted_quality_score:.1f}% " +
                  f"(Completeness: {dim_scores.get('Completeness', 0):.0f}%, " +
                  f"Validity: {dim_scores.get('Validity', 0):.0f}%, " +
                  f"Accuracy: {dim_scores.get('Accuracy', 0):.0f}%)")

            dimension_scoring_success = True

        except Exception as e:
            print(f"⚠️  Failed to calculate dimension scores: {e}")
            # Fallback to old quality score
            weighted_quality_score = profile_report.overall_quality_score
            verdict["quality_dimensions"] = None
            dimension_scoring_success = False

        _dimension_duration = int((time.time() - _dimension_start) * 1000)

        # Log dimension scoring to tool_outputs
        tool_logger.log_simple(
            tool_name="dimension_scorer",
            status="SUCCESS" if dimension_scoring_success else "ERROR",
            output={
                "overall_score": weighted_quality_score,
                "weights_source": "custom" if custom_weights else "default",
                "dimension_count": 6 if dimension_scoring_success else 0
            },
            duration_ms=_dimension_duration
        )

        # Block if quality score is critically low
        # Use weighted dimension score if available, fallback to old score
        effective_quality_score = verdict["profile"].get("weighted_quality_score",
                                                           profile_report.overall_quality_score)
        score_type = "Weighted 6D" if "weighted_quality_score" in verdict["profile"] else "Simple average"

        if effective_quality_score < qs_block:
            verdict["status"] = "BLOCKED"
            verdict["reason"] = (
                f"Data Quality Score critically low: {effective_quality_score:.1f}% "
                f"(threshold: {qs_block}%). Score type: {score_type}"
            )
            verdict["actions"] = ["Quarantine", "Investigate Value Violations"]
            verdict["load_status"] = "SKIPPED (Quality too low)"
            self._record_run(dataset_name, verdict, file_path, _start_time)
            return self._enrich_with_llm(verdict)

        elif effective_quality_score < qs_warn:
            verdict["status"] = "WARNING"
            verdict["reason"] = (
                f"Data Quality Score below threshold: {effective_quality_score:.1f}% "
                f"(threshold: {qs_warn}%). Score type: {score_type}"
            )

        # ---------------------------------------------------------
        # Stage B: Anomaly Detection (The "Soft" Gate)
        # ---------------------------------------------------------
        print(f"\n📉 [Stage B] Checking Network Anomalies for '{dataset_name}'...")

        anomaly_inputs: Dict[str, Any] = {
            "row_count": {
                "value": len(df),
                "metric_group": "volume",
                "segment": "global",
                "tags": {"source": "pipeline"},
            }
        }
        try:
            freshness_age_minutes = max(0.0, (time.time() - Path(file_path).stat().st_mtime) / 60.0)
            freshness_payload = {
                "value": freshness_age_minutes,
                "metric_group": "freshness",
                "segment": "global",
                "tags": {"source": "filesystem"},
            }
            if slo_targets.get("freshness_max_minutes") is not None:
                freshness_payload["tags"]["slo_target_minutes"] = float(slo_targets["freshness_max_minutes"])
            anomaly_inputs["freshness_age_minutes"] = freshness_payload
        except Exception:
            pass

        # Run Detection (Pass DF for distribution checks)
        _anomaly_start = time.time()
        anomaly_report = self.anomaly_detector.evaluate_run(dataset_name,
                                                            anomaly_inputs,
                                                            dataframe=df)
        _anomaly_duration = int((time.time() - _anomaly_start) * 1000)

        # Log anomaly detection
        tool_logger.log_simple(
            tool_name="anomaly_detector",
            status=anomaly_report["status"],
            output={
                "status": anomaly_report["status"],
                "anomaly_count": len(anomaly_report.get("anomalies", [])),
                "metrics_checked": len(anomaly_report.get("metrics", {}))
            },
            duration_ms=_anomaly_duration
        )

        if anomaly_report["status"] == "ANOMALY_DETECTED":
            # Check Impact Analysis
            print(f"\n🎯 [Impact Analysis] Assessing Criticality...")
            _impact_start = time.time()
            impact = self.impact_analyzer.get_downstream_impact(dataset_name)
            _impact_duration = int((time.time() - _impact_start) * 1000)
            criticality = impact.get("overall_criticality", "LOW")

            # Log impact analysis
            tool_logger.log_simple(
                tool_name="impact_analyzer",
                status="SUCCESS",
                output={
                    "overall_criticality": criticality,
                    "downstream_count": len(impact.get("downstream", []))
                },
                duration_ms=_impact_duration
            )
            
            verdict["anomalies"] = anomaly_report["anomalies"]
            verdict["metrics"] = anomaly_report.get("metrics", {})
            
            # Decision Matrix
            # We look at the max Z-Score to determine severity
            max_z = 0.0
            for anomaly in anomaly_report["anomalies"]:
                z = abs(anomaly.get("z_score", 0))
                if z > max_z: max_z = z
            
            if criticality in ["HIGH", "CRITICAL"] and max_z > z_critical:
                verdict["status"] = "BLOCKED"
                verdict["reason"] = f"CRITICAL ANOMALY (Z={max_z:.1f}, threshold={z_critical}) on HIGH IMPACT dataset."
                verdict["actions"] = ["Quarantine", "Alert Execs"]
            
            elif criticality == "LOW" and max_z > z_critical:
                verdict["status"] = "WARNING"
                verdict["reason"] = f"Anomaly detected (Z={max_z:.1f}), but impact is LOW."
                verdict["actions"] = ["Proceed to Load", "Log Warning"]
            
            elif max_z > z_warn:
                # Above warning threshold but below critical
                verdict["status"] = "WARNING"
                verdict["reason"] = f"Anomaly detected (Z={max_z:.1f}, warning threshold={z_warn})."
        
        # Always attach statistical metrics to verdict so they get saved to history
        verdict["metrics"] = anomaly_report.get("metrics", {})
        
        # ---------------------------------------------------------
        # Stage C: The Action (Load or Skip)
        # ---------------------------------------------------------
        if verdict["status"] in ["PASSED", "WARNING"]:
            try:
                print(f"🚀 [Stage C] Loading Data into Doris...")
                _load_start = time.time()
                load_result = self.loader.load_data(df, dataset_name)
                _load_duration = int((time.time() - _load_start) * 1000)
                verdict["load_status"] = load_result

                # Log doris loader
                tool_logger.log_simple(
                    tool_name="doris_loader",
                    status="SUCCESS" if load_result.get("success") else "ERROR",
                    output=load_result,
                    duration_ms=_load_duration
                )
            except Exception as e:
                # Don't BLOCK purely on infra failures (like local DB down)
                # Keep the Quality Verdict (PASSED/WARNING) but note the infra failure
                error_msg = str(e).lower()
                if "connection refused" in error_msg or "max retries exceeded" in error_msg:
                    root_cause = self._diagnose_root_cause(dataset_name)
                    note = f" (Note: Load skipped - Local DB unreachable. Root Cause: {root_cause})"
                    verdict["reason"] += note
                    verdict["load_status"] = "SKIPPED (Infra Error)"
                    verdict["status"] = "WARNING"
                else:
                    verdict["status"] = "BLOCKED"  # Real load errors might still block
                    verdict["reason"] += f" (Load Failed: {str(e)})"
                    verdict["load_status"] = {"error": str(e)}
        else:
            verdict["load_status"] = "SKIPPED (Blocked by Agent)"

        # ---------------------------------------------------------
        # Stage D: Record to System Tables & Return Verdict
        # ---------------------------------------------------------
        self._record_run(dataset_name, verdict, file_path, _start_time)
        return self._enrich_with_llm(verdict)

    def _record_run(self, dataset_name: str, verdict: Dict[str, Any],
                    file_path: str, start_time: float):
        """Record run outcome to system tables (run_history + registry)."""
        import time as _time
        duration_ms = int((_time.time() - start_time) * 1000)

        quality_score = verdict.get("profile", {}).get("overall_quality_score", 0.0)
        anomaly_count = len(verdict.get("anomalies", []))
        max_z = max((abs(a.get("z_score", 0)) for a in verdict.get("anomalies", [])), default=0.0)
        verdict["slos"] = self._evaluate_slos(dataset_name, verdict, file_path)

        # Use run_id from verdict (generated at pipeline start for tool logging)
        run_id = verdict.get("run_id")

        try:
            # Save to run_history
            saved_run_id = self.anomaly_detector.save_run_to_history(
                dataset_name=dataset_name,
                status=verdict["status"],
                quality_score=quality_score,
                anomaly_count=anomaly_count,
                z_score_max=max_z,
                reason=verdict.get("reason", ""),
                duration_ms=duration_ms,
                run_id=run_id,  # Pass our pre-generated run_id
                dimension_scores=verdict.get("quality_dimensions"),  # Save dimension scores
                full_verdict=verdict  # Save complete verdict with all tool outputs
            )
            # Use the returned run_id (should be the same as we passed)
            run_id = saved_run_id
            
            # Save metrics to metric_history
            metrics_payload: Dict[str, Any] = {}
            # Base metrics from Profile (Quality Scores)
            if "profile" in verdict:
                metrics_payload["quality_score"] = {
                    "value": verdict["profile"]["overall_quality_score"],
                    "metric_group": "quality",
                    "segment": "global",
                    "tags": {"source": "profile"},
                }
                # Add column-level scores
                for col, score in verdict["profile"].get("column_scores", {}).items():
                    metrics_payload[f"{col}_quality_score"] = {
                        "value": score,
                        "metric_group": "quality",
                        "column_name": str(col),
                        "segment": "global",
                        "tags": {"source": "profile"},
                    }
            
            # Add Statistical Metrics from Anomaly Detector (Mean, Null Rates, Row Count)
            # This ensures we save everything the detector calculated (including means)
            if "metrics" in verdict:
                 for m_name, m_data in verdict["metrics"].items():
                     if isinstance(m_data, dict) and "value" in m_data:
                         metrics_payload[m_name] = {
                             "value": m_data["value"],
                             "metric_group": m_data.get("metric_group", "anomaly"),
                             "column_name": m_data.get("column_name"),
                             "segment": m_data.get("segment", "global"),
                             "tags": {
                                 "baseline_type": m_data.get("baseline_type"),
                                 "is_anomaly": bool(m_data.get("is_anomaly", False)),
                                 **(m_data.get("tags", {}) if isinstance(m_data.get("tags"), dict) else {}),
                             },
                         }
                     else:
                         metrics_payload[m_name] = {
                             "value": m_data,
                             "metric_group": "anomaly",
                             "segment": "global",
                         }

            if metrics_payload:
                self.anomaly_detector.save_run_metrics(dataset_name, metrics_payload, run_id=run_id)

            slo_checks = verdict.get("slos", {}).get("checks", [])
            if slo_checks:
                self.anomaly_detector.save_slo_results(
                    run_id=run_id,
                    dataset_name=dataset_name,
                    slo_results=slo_checks,
                )
            
            # Update dataset registry
            file_mtime = None
            try:
                file_mtime = Path(file_path).stat().st_mtime
            except Exception:
                pass
            
            # Get criticality from impact analyzer
            impact = {}
            try:
                impact = self.impact_analyzer.get_downstream_impact(dataset_name)
                criticality = impact.get("overall_criticality", "UNKNOWN")
            except Exception:
                criticality = "UNKNOWN"
            
            contract_path = str(self.contract_store.path_for(dataset_name))
            
            # Reads contact info for alerts
            owner = "Unknown"
            try:
                ds_info = self.impact_analyzer.lineage_graph.get("datasets", {}).get(dataset_name, {})
                owner = ds_info.get("owner", "Unknown")
            except Exception:
                pass

            # Send Alert
            self.alert_router.send_alert(verdict, {
                "criticality": criticality, 
                "owner": owner
            })

            self.anomaly_detector.update_dataset_registry(
                dataset_name=dataset_name,
                contract_path=contract_path,
                lifecycle="active",
                criticality=criticality,
                status=verdict["status"],
                file_mtime=file_mtime,
            )
            
            print(f"📊 System Tables: Recorded run for '{dataset_name}' "
                  f"(status={verdict['status']}, duration={duration_ms}ms)")
            
            # Log full verdict to JSON file (Audit Trail)
            self._log_verdict_to_json(dataset_name, verdict)
            
        except Exception as e:
            print(f"⚠️ Failed to record run to system tables: {e}")

    def _log_verdict_to_json(self, dataset_name: str, verdict: Dict[str, Any]):
        """
        Log the full verdict to a JSON file for auditing.
        Location: data/history/{date}/{dataset}_{timestamp}.json
        """
        try:
            timestamp = datetime.now()
            date_str = timestamp.strftime("%Y-%m-%d")
            time_str = timestamp.strftime("%H%M%S")
            
            # Ensure history directory exists
            log_dir = Path(f"data/history/{date_str}")
            log_dir.mkdir(parents=True, exist_ok=True)
            
            filename = f"{dataset_name}_{time_str}.json"
            log_path = log_dir / filename
            
            with open(log_path, "w") as f:
                json.dump(verdict, f, indent=2, default=str)
                
            print(f"📝 Verdict logged to: {log_path}")
            
        except Exception as e:
            print(f"⚠️ Failed to log verdict to JSON: {e}")

    def _enrich_with_llm(self, verdict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Use Agno LLM Agent to generate a human-readable summary/advice.
        """
        print(f"\n🤖 [Stage C] Generating Agentic Advice...")
        
        # Convert verdict to string for LLM
        verdict_str = json.dumps(verdict, indent=2)
        
        try:
            # Ask the LLM
            response = self.reasoning_agent.run(f"Current Verdict:\n{verdict_str}")
            
            # Extract the content
            advice = response.content
            verdict["llm_advice"] = advice
        except Exception as e:
            print(f"❌ LLM Error: {e}")
            verdict["llm_advice"] = "Could not generate advice due to LLM error."
            
        return verdict

    def get_schema_content(self, dataset_name: str) -> str:
        """Read the raw content of a schema file."""
        doc = self.contract_store.read(dataset_name)
        return doc.content if doc else ""

    def remediate_schema(self, dataset_name: str, new_yaml_content: str) -> bool:
        """Overwrite the existing schema with the new agreed-upon contract.
        Creates a backup of the original file before overwriting."""
        path = self.contract_store.path_for(dataset_name)
        try:
            # SAFETY: Create backup before overwrite
            SchemaRemediator.create_backup(str(path))
            
            self.contract_store.write(dataset_name, new_yaml_content)
            print(f"✅ Schema remediated for {dataset_name}")
            return True
        except Exception as e:
            print(f"❌ Failed to remediate schema: {e}")
            return False

    def _diagnose_root_cause(self, dataset_name: str) -> str:
        """
        Check upstream systems defined in lineage.yaml to see if they are down.
        Returns a string explaining the root cause, or 'Unknown' if all up.
        """
        try:
            # 1. Get upstream config from lineage
            impact = self.impact_analyzer.get_downstream_impact(dataset_name)
            # ImpactAnalyzer mostly does downstream, let's read the file directly or extend ImpactAnalyzer.
            # For speed, let's read lineage.yaml directly here since ImpactAnalyzer might not return upstream.
            import yaml
            with open(self.impact_analyzer.lineage_path, 'r') as f:
                lineage = yaml.safe_load(f) or {}
            
            dataset_conf = lineage.get("datasets", {}).get(dataset_name, {})
            upstreams = dataset_conf.get("upstream", [])
            
            if not upstreams:
                return "Local Infrastructure Issue"

            # 2. Check each upstream
            down_services = []
            for upstream in upstreams:
                health = self.system_health.check_upstream_health(upstream)
                if health["status"] == "DOWN":
                    down_services.append(f"{health['name']} ({health['details']})")
            
            if down_services:
                return f"Upstream Outage: {', '.join(down_services)}"
            
            return "Local Infrastructure Issue (Upstream services are UP)"

        except Exception as e:
            return f"Diagnosis Failed: {e}"

    def _extract_dataset_name_from_stem(self, stem: str) -> str:
        """
        Normalize a data filename stem into a logical dataset name.
        Examples:
            newdata_2026-02-15 -> newdata
            orders_latest -> orders
            yellow_tripdata_2025-01 -> yellow_tripdata_2025-01
        """
        parts = stem.split("_")
        if len(parts) > 1:
            token = parts[1]
            if token.replace("-", "").replace(":", "").isdigit():
                return parts[0]
            if token in ["latest", "current", "new", "final", "v1", "v2"]:
                return parts[0]
        return stem

    def _is_supported_data_file(self, file_path: Path) -> bool:
        return (
            file_path.is_file()
            and file_path.suffix.lower() in [".csv", ".parquet", ".json"]
            and ".verdict." not in file_path.name
        )

    def _find_latest_data_file(self, dataset_name: str) -> Optional[str]:
        """
        Locate the newest data file for a dataset across common local zones.
        """
        search_dirs = [
            Path("data/landing"),
            Path("data/pending_approval"),
            Path("data/test"),
            Path("data"),
            Path("data/quarantine"),
        ]
        candidates = []
        for directory in search_dirs:
            if not directory.exists():
                continue
            for file_path in directory.glob("*"):
                if not self._is_supported_data_file(file_path):
                    continue
                logical_name = self._extract_dataset_name_from_stem(file_path.stem)
                if logical_name == dataset_name:
                    candidates.append(file_path)

        if not candidates:
            return None

        latest = max(candidates, key=lambda p: p.stat().st_mtime)
        return str(latest)

    # ---------------------------------------------------------
    # Phase 1: Schema-Level Auto-Discovery
    # ---------------------------------------------------------

    def discover_datasets(self) -> List[Dict[str, Any]]:
        """
        Auto-discover all dataset contracts from the contracts directory.
        
        Scans config/expectations/*.yaml and returns metadata for each dataset:
        - name, column_count, has_quality_rules, lifecycle, criticality
        
        This replaces manual dataset selection with automated
        schema-level monitoring.
        
        Returns:
            List of dataset metadata dicts.
        """
        import yaml
        from pathlib import Path
        
        datasets = []
        contract_files = self.contract_store.list_paths()
        
        for contract_file in contract_files:
            # Skip backup files
            if ".backup_" in contract_file.name:
                continue
            
            try:
                with open(contract_file, "r") as f:
                    contract = yaml.safe_load(f) or {}
                
                dataset_name = contract_file.stem
                columns = contract.get("columns", [])
                quality = contract.get("quality", {})
                info = contract.get("info", {})
                
                # Determine lifecycle (default: active)
                lifecycle = info.get("lifecycle", "active")
                
                # Get criticality from lineage
                criticality = "UNKNOWN"
                try:
                    impact = self.impact_analyzer.get_downstream_impact(dataset_name)
                    criticality = impact.get("overall_criticality", "UNKNOWN")
                except Exception:
                    pass
                
                # Resolve the latest physical file currently associated with this dataset
                data_file = self._find_latest_data_file(dataset_name)
                
                datasets.append({
                    "name": dataset_name,
                    "contract_path": str(contract_file),
                    "data_file": data_file,
                    "column_count": len(columns),
                    "columns": [c.get("name", "?") for c in columns],
                    "has_quality_rules": bool(quality.get("custom_checks")),
                    "has_anomaly_thresholds": bool(quality.get("anomaly_thresholds")),
                    "lifecycle": lifecycle,
                    "criticality": criticality,
                    "owner": info.get("owner", "Unknown"),
                    "domain": info.get("domain", "Unknown"),
                    "version": info.get("version", "0.0.0"),
                })
                
            except Exception as e:
                print(f"⚠️ Failed to parse {contract_file.name}: {e}")
                datasets.append({
                    "name": contract_file.stem,
                    "contract_path": str(contract_file),
                    "data_file": None,
                    "column_count": 0,
                    "columns": [],
                    "has_quality_rules": False,
                    "has_anomaly_thresholds": False,
                    "lifecycle": "error",
                    "criticality": "UNKNOWN",
                    "owner": "Unknown",
                    "domain": "Unknown",
                    "version": "0.0.0",
                    "error": str(e),
                })
        
        # -------------------------------------------------------------
        # Phase 1.5: Discover Unmanaged Files (No Contract Yet)
        # -------------------------------------------------------------
        managed_datasets = {d["name"] for d in datasets}
        data_dirs = ["data/test", "data/landing", "data/pending_approval", "data"]
        
        for d_dir in data_dirs:
            path = Path(d_dir)
            if not path.exists():
                continue
                
            # Scan for supported data files
            for file_path in path.glob("*"):
                if not self._is_supported_data_file(file_path):
                    continue
                    
                # Extract stable dataset id (e.g. newdata_2026-02-15 -> newdata)
                candidate_name = self._extract_dataset_name_from_stem(file_path.stem)
                    
                # If already managed, skip
                if candidate_name in managed_datasets:
                    continue
                
                # If we've already found this unmanaged dataset in a previous loop, skip
                # (e.g. found in data/test, don't add again from data/landing)
                if candidate_name in managed_datasets: 
                    continue

                # Add as "Unconfigured" dataset
                print(f"   🆕 Discovered unmanaged file: {file_path.name}")
                datasets.append({
                    "name": candidate_name,
                    "contract_path": None,
                    "data_file": str(file_path),
                    "column_count": 0,
                    "columns": [],
                    "has_quality_rules": False,
                    "has_anomaly_thresholds": False,
                    "lifecycle": "unconfigured",  # Special status for UI
                    "criticality": "UNKNOWN",
                    "owner": "Unassigned",
                    "domain": "Discovered",
                    "version": "0.0.0"
                })
                managed_datasets.add(candidate_name) # Prevent duplicates across dirs
        
        print(f"\n📂 Auto-Discovery: Found {len(datasets)} dataset contract(s)")
        for ds in datasets:
            icon = "✅" if ds.get("data_file") else "⚠️"
            print(f"   {icon} {ds['name']} ({ds['column_count']} cols, "
                  f"criticality={ds['criticality']}, lifecycle={ds['lifecycle']})")
        
        return datasets

    def evaluate_all(self, data_dir: str = "data/test", 
                     skip_unchanged: bool = False) -> Dict[str, Any]:
        """
        Run health checks on ALL discovered datasets.
        
        This is the enterprise-grade 'schema-level monitoring':
        one click → monitor everything.
        
        Args:
            data_dir: Directory containing data files (looks for {name}.csv)
            skip_unchanged: If True, skip datasets whose data file hasn't 
                           changed since the last scan (Phase 2: Intelligent Scheduling).
            
        Returns:
            Dict with overall summary and per-dataset results.
        """
        datasets = self.discover_datasets()
        
        results = {}
        summary = {
            "total": len(datasets),
            "passed": 0,
            "warning": 0,
            "blocked": 0,
            "skipped": 0,
            "unchanged": 0,
            "timestamp": datetime.now().isoformat(),
        }
        
        for ds in datasets:
            name = ds["name"]
            
            # Skip deprecated datasets
            if ds["lifecycle"] == "deprecated":
                print(f"\n⏭️  Skipping '{name}' (lifecycle=deprecated)")
                results[name] = {"status": "SKIPPED", "reason": "Dataset is deprecated"}
                summary["skipped"] += 1
                continue
            
            # Find data file
            data_file = ds.get("data_file")
            if not data_file:
                data_file = self._find_latest_data_file(name)
            
            if not data_file:
                print(f"\n⏭️  Skipping '{name}' (no data file found)")
                results[name] = {"status": "SKIPPED", "reason": "No data file found"}
                summary["skipped"] += 1
                continue
            
            # ---------------------------------------------------------
            # Phase 2: Intelligent Scan Scheduling
            # Skip if the data file hasn't changed since last scan
            # ---------------------------------------------------------
            if skip_unchanged:
                try:
                    from src.utils.database import get_connection
                    current_mtime = Path(data_file).stat().st_mtime
                    with get_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "SELECT last_file_mtime FROM dataset_registry WHERE dataset_name = %s",
                                (name,)
                            )
                            row = cur.fetchone()
                            if row and row[0] is not None and abs(current_mtime - row[0]) < 0.01:
                                print(f"\n⏩ Skipping '{name}' (file unchanged since last scan)")
                                results[name] = {
                                    "status": "UNCHANGED",
                                    "reason": "Data file not modified since last scan",
                                }
                                summary["unchanged"] += 1
                                continue
                except Exception:
                    pass  # If registry check fails, just scan anyway
            
            print(f"\n{'='*60}")
            print(f"🔍 Evaluating: {name} ({data_file})")
            print(f"{'='*60}")
            
            try:
                result = self.evaluate_data_file(data_file, name)
                results[name] = result
                
                status = result.get("status", "UNKNOWN")
                if status == "PASSED":
                    summary["passed"] += 1
                elif status == "WARNING":
                    summary["warning"] += 1
                else:
                    summary["blocked"] += 1
                    
            except Exception as e:
                print(f"❌ Error evaluating {name}: {e}")
                results[name] = {
                    "status": "BLOCKED",
                    "reason": f"Evaluation error: {str(e)}",
                    "dataset": name,
                }
                summary["blocked"] += 1
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"📊 SCHEMA HEALTH SUMMARY")
        print(f"{'='*60}")
        print(f"   Total:     {summary['total']}")
        print(f"   ✅ Passed:   {summary['passed']}")
        print(f"   ⚠️  Warning:  {summary['warning']}")
        print(f"   🚫 Blocked:  {summary['blocked']}")
        print(f"   ⏭️  Skipped:  {summary['skipped']}")
        print(f"   ⏩ Unchanged: {summary['unchanged']}")
        print(f"{'='*60}")
        
        return {
            "summary": summary,
            "results": results,
        }

    def get_run_history(self, dataset_name: str = None, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Query run history from the system tables.
        
        Args:
            dataset_name: Optional filter by dataset name. None = all datasets.
            limit: Maximum rows to return.
            
        Returns:
            List of run history dicts.
        """
        from src.utils.database import get_connection
        with get_connection() as conn:
            with conn.cursor() as cur:
                if dataset_name:
                    cur.execute("""
                        SELECT run_id, timestamp, dataset_name, status,
                               quality_score, anomaly_count, z_score_max, reason, duration_ms
                        FROM run_history
                        WHERE dataset_name = %s
                        ORDER BY timestamp DESC LIMIT %s
                    """, (dataset_name, limit))
                else:
                    cur.execute("""
                        SELECT run_id, timestamp, dataset_name, status,
                               quality_score, anomaly_count, z_score_max, reason, duration_ms
                        FROM run_history
                        ORDER BY timestamp DESC LIMIT %s
                    """, (limit,))

                rows = cur.fetchall()
                return [
                    {
                        "run_id": r[0], "timestamp": r[1].isoformat() if r[1] else None,
                        "dataset": r[2], "status": r[3], "quality_score": r[4],
                        "anomaly_count": r[5], "z_score_max": r[6],
                        "reason": r[7], "duration_ms": r[8],
                    }
                    for r in rows
                ]

    def request_copilot_chat(self, user_query: str, context_data: Dict[str, Any]) -> str:
        """
        Specialized chat interface for the dashboard Copilot.
        
        Uses a highly specific system prompt to enforce:
        1. Scope (Specific dataset vs Global)
        2. Tiers (Status vs Deep Dive vs Remediation)
        3. Conciseness (Just-in-Time info)
        """
        # 1. Prepare Context String
        discovered = context_data.get("discovered", [])
        results = context_data.get("results", {})
        
        context_str = "### SYSTEM STATE ###\n"
        context_str += f"Total Datasets: {len(discovered)}\n"
        
        for ds in discovered:
            name = ds["name"]
            res = results.get(name, {})
            status = res.get("status", "UNKNOWN")
            reason = res.get("reason", "No data")
            crit = ds.get("criticality", "UNKNOWN")
            # Calculate health score dynamically if possible, or pass it in.
            # approximating from status for context
            health_est = "100%" if status == "PASSED" else "60%" if status == "WARNING" else "10%"
            
            context_str += f"- Dataset: {name} | Status: {status} | Health: {health_est} | Crit: {crit} | Reason: {reason}\n"
            if res.get("anomalies"):
                context_str += f"  - Anomalies: {len(res['anomalies'])} detected (Max Z-Score: {max((a.get('z_score',0) for a in res['anomalies']), default=0):.1f})\n"
            if res.get("schema_evolution", {}).get("missing_columns"):
                context_str += f"  - Schema Issues: Missing cols {res['schema_evolution']['missing_columns']}\n"

        # 2. Define System Prompt
        system_prompt = """
You are the Agentic DRE Copilot. Your goal is to answer questions about the data pipeline PRECISELY.

### INSTRUCTIONS:

1. **INTENT EXTRACTION**:
   - Is the user asking about a SPECIFIC dataset (e.g., 'transactions')?
   - Or the GLOBAL system state (e.g., 'overview', 'summary')?

2. **SCOPE ENFORCEMENT**:
   - If a specific dataset is mentioned, ONLY provide info for that dataset. IGNORE others.
   - Do NOT provide a remediation plan unless the user explicitly asks "How do I fix it?".

3. **RESPONSE TIERS**:
   - **Tier 1 (Status Check)**: User asks "What is the status/health?" 
     -> Provide ONLY: Status, Criticality, Health Score, and the PRIMARY reason.
   - **Tier 2 (Deep Dive)**: User asks "Why?" or "Details?" 
     -> Provide: Z-Scores, specific column names, exact schema mismatches.
   - **Tier 3 (Remediation)**: User asks "How do I fix this?" 
     -> Provide: Step-by-step action plan.

4. **FORMATTING**:
   - Use Markdown.
   - If the answer is short (Tier 1), use a single paragraph or checkmark list. No big headers.
   - **Conciseness Rule**: If you can answer in 2 sentences, do it.

### AVAILABLE CONTEXT:
{context_str}
"""
        # 3. Create Ephemeral Agent for this turn
        chat_agent = Agent(
            model=OpenAIChat(id=os.getenv("OPENAI_MODEL_NAME", "gpt-4o")),
            description="You are a precise Data Reliability Engineer.",
            instructions=system_prompt.format(context_str=context_str),
            markdown=True
        )
        
        response = chat_agent.run(user_query)
        return response.content

    def get_dataset_sample(self, dataset_name: str, limit: int = 100) -> Dict[str, Any]:
        """
        Read and return a sample of data from the dataset file.
        Supports CSV and Parquet formats.
        """
        # 1. Find the file
        datasets = self.discover_datasets()
        meta = next((d for d in datasets if d["name"] == dataset_name), None)
        
        if not meta or not meta.get("data_file"):
            raise FileNotFoundError(f"Data file for {dataset_name} not found")
            
        file_path = meta["data_file"]
        
        # 2. Read data based on extension
        try:
            import pandas as pd
            
            if file_path.lower().endswith('.parquet'):
                df = pd.read_parquet(file_path)
            elif file_path.lower().endswith('.json'):
                df = pd.read_json(file_path)
            else:
                # Default to CSV for .csv and other text-based formats
                df = pd.read_csv(file_path)
                
            # 3. Sample and convert to dict
            # Replace NaN with None for JSON compatibility
            df = df.head(limit).replace({float('nan'): None})
            
            return {
                "columns": list(df.columns),
                "data": df.to_dict(orient="records"),
                "total_rows": len(df), # This might correspond to the full file if read_parquet reads all locally
                "preview_limit": limit
            }
        except Exception as e:
            raise RuntimeError(f"Failed to read data file: {str(e)}")

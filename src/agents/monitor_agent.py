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
import json
import time
import re
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from pathlib import Path

# Tool Imports
from src.connectors import ConnectorDataset, build_connectors
from src.contracts.store import ContractStore, FileContractStore
from src.tools.contract_diff import merge_contracts
from src.tools.anomaly_detector import AnomalyDetector
from src.tools.impact_analyzer import ImpactAnalyzer
from src.tools.doris_loader import DorisLoader
from src.tools.schema_remediator import SchemaRemediator
from src.utils.tool_logger import ToolLogger
from src.tools.data_profiler import DataProfiler
from src.tools.system_health import SystemHealthCheck
from src.tools.alert_router import AlertRouter
from src.pipeline.context import PipelineContext
from src.pipeline.stages import action_stage, anomaly_stage, ingest_stage, profile_stage, schema_stage

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
        self.connectors = build_connectors()
        self.connectors_by_name = {str(getattr(c, "name", "")).strip(): c for c in self.connectors}
        
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
        # Orchestrate contract generation via datacontract-cli (preferred) with deterministic fallback.
        # If an existing contract exists, we merge the new schema into it to preserve metadata.
        print(f"🕵️ Profiling data for {dataset_name} and generating contract...")
        
        # 1. Generate new schema from data (Observation)
        generation = self.remediator.generate_initial_contract_with_report(data_path, dataset_name)
        observed_yaml = generation.yaml_content
        print(f"   Generator engine: {generation.engine}")

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

        def _compute_error_budget_burn(operator: str, target: float, observed: float) -> Dict[str, float]:
            delta = 0.0
            if operator == ">=":
                delta = max(0.0, target - observed)
            elif operator == "<=":
                delta = max(0.0, observed - target)
            denominator = abs(target) if abs(target) > 1e-9 else max(abs(observed), 1.0)
            ratio = (delta / denominator) if denominator > 0 else 0.0
            return {
                "delta": float(delta),
                "ratio": float(ratio),
                "burn": float(min(1.0, max(0.0, ratio))),
            }

        def add_check(name: str, operator: str, target: float, observed: float, metadata: Dict[str, Any]):
            passed = False
            if operator == ">=":
                passed = observed >= target
            elif operator == "<=":
                passed = observed <= target
            burn_stats = _compute_error_budget_burn(operator, float(target), float(observed))
            severity = "NONE"
            if not passed:
                if burn_stats["burn"] >= 0.5:
                    severity = "CRITICAL"
                elif burn_stats["burn"] >= 0.2:
                    severity = "HIGH"
                else:
                    severity = "MEDIUM"

            check_metadata = dict(metadata or {})
            check_metadata.update(
                {
                    "severity": severity,
                    "breach_delta": burn_stats["delta"],
                    "breach_ratio": burn_stats["ratio"],
                }
            )
            checks.append(
                {
                    "slo_name": name,
                    "operator": operator,
                    "target": float(target),
                    "observed": float(observed),
                    "status": "PASS" if passed else "FAIL",
                    "error_budget_burn": burn_stats["burn"],
                    "metadata": check_metadata,
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
        fail_count = sum(1 for c in checks if c["status"] != "PASS")
        total = len(checks)
        burn_total = float(sum(float(c.get("error_budget_burn", 0.0) or 0.0) for c in checks))
        burn_avg = (burn_total / total) if total else 0.0
        failing_slos = [str(c.get("slo_name") or "") for c in checks if c.get("status") != "PASS"]
        return {
            "dataset_name": dataset_name,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "overall_status": "PASS" if pass_count == total else "FAIL",
            "pass_rate": round((pass_count / total) * 100, 2) if total else 100.0,
            "fail_count": fail_count,
            "error_budget_burn_total": round(burn_total, 4),
            "error_budget_burn_avg": round(burn_avg, 4),
            "failing_slos": failing_slos,
            "checks": checks,
        }

    def evaluate_data_file(self, file_path: str, dataset_name: str, force_load: bool = False) -> Dict[str, Any]:
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
        start_time = _time.time()

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
            "run_id": run_id,
        }
        ctx = PipelineContext(
            dataset_name=dataset_name,
            file_path=file_path,
            start_time=start_time,
            run_id=run_id,
            tool_logger=tool_logger,
            verdict=verdict,
            force_load=force_load,
        )

        ingest_failure = ingest_stage.run(ctx)
        if ingest_failure is not None:
            return ingest_failure

        if not schema_stage.run(self, ctx):
            self._record_run(dataset_name, ctx.verdict, file_path, start_time)
            return self._enrich_with_llm(ctx.verdict)

        if not profile_stage.run(self, ctx):
            self._record_run(dataset_name, ctx.verdict, file_path, start_time)
            return self._enrich_with_llm(ctx.verdict)

        anomaly_stage.run(self, ctx)
        action_stage.run(self, ctx)

        self._record_run(dataset_name, ctx.verdict, file_path, start_time)
        return self._enrich_with_llm(ctx.verdict)

    def _record_run(self, dataset_name: str, verdict: Dict[str, Any],
                    file_path: str, start_time: float):
        """Record run outcome to system tables (run_history + registry)."""
        import time as _time
        duration_ms = int((_time.time() - start_time) * 1000)

        profile_payload = verdict.get("profile", {}) if isinstance(verdict.get("profile"), dict) else {}
        quality_score = profile_payload.get("weighted_quality_score", profile_payload.get("overall_quality_score", 0.0))
        try:
            quality_score = float(quality_score)
        except Exception:
            quality_score = 0.0
        verdict["quality_score"] = quality_score
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
                    "value": quality_score,
                    "metric_group": "quality",
                    "segment": "global",
                    "tags": {"source": "profile", "quality_score_type": "weighted_6d"},
                }
                legacy_overall = verdict["profile"].get("overall_quality_score")
                if isinstance(legacy_overall, (int, float)):
                    metrics_payload["profile_overall_quality_score"] = {
                        "value": float(legacy_overall),
                        "metric_group": "quality",
                        "segment": "global",
                        "tags": {"source": "profile", "quality_score_type": "legacy_profile_average"},
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

            # Persist diagnostics evidence (failed rows/checks) for faster triage.
            try:
                from src.services.diagnostics_service import DiagnosticsService

                diagnostics_service = DiagnosticsService()
                inserted = diagnostics_service.record_from_verdict(
                    run_id=str(run_id),
                    dataset_name=dataset_name,
                    verdict=verdict,
                )
                verdict["diagnostics_record_count"] = int(inserted)
            except Exception as diag_err:
                print(f"⚠️ Diagnostics persistence failed: {diag_err}")
            
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
            lineage_context = {}
            try:
                lineage_context = self.impact_analyzer.get_lineage_context(dataset_name, max_depth=2)
            except Exception:
                lineage_context = {}
            
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
                "owner": owner,
                "lineage_impact": impact,
                "lineage_context": lineage_context,
            })

            self.anomaly_detector.update_dataset_registry(
                dataset_name=dataset_name,
                contract_path=contract_path,
                lifecycle="active",
                criticality=criticality,
                status=verdict["status"],
                file_mtime=file_mtime,
            )

            # Incident lifecycle sync (OPEN/ACK/RESOLVED)
            try:
                from src.services.incident_service import IncidentService

                incident_service = IncidentService()
                incident_service.sync_with_run(
                    run_id=run_id,
                    dataset_name=dataset_name,
                    run_status=verdict["status"],
                    reason=verdict.get("reason", ""),
                    quality_score=float(quality_score or 0.0),
                    anomaly_count=int(anomaly_count or 0),
                    z_score_max=float(max_z or 0.0),
                    owner=owner if owner and owner != "Unknown" else None,
                    metadata={
                        "criticality": criticality,
                        "impacted_consumers": impact.get("impacted_consumers", []) if isinstance(impact, dict) else [],
                        "lineage_context": lineage_context if isinstance(lineage_context, dict) else {},
                    },
                )
            except Exception as incident_err:
                print(f"⚠️ Incident sync failed: {incident_err}")
            
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
                stem = file_path.stem
                # Prefer exact/prefix matching for managed datasets (supports names with underscores).
                if stem == dataset_name or stem.startswith(f"{dataset_name}_"):
                    candidates.append(file_path)
                    continue
                logical_name = self._extract_dataset_name_from_stem(stem)
                if logical_name == dataset_name:
                    candidates.append(file_path)

        if not candidates:
            return None

        latest = max(candidates, key=lambda p: p.stat().st_mtime)
        return str(latest)

    def _discover_connector_index(self) -> Dict[str, Dict[str, Any]]:
        """
        Discover datasets from configured connectors and index by dataset name.
        """
        index: Dict[str, Dict[str, Any]] = {}
        for connector in self.connectors:
            connector_name = str(getattr(connector, "name", "")).strip() or "connector"
            try:
                discovered = connector.discover()
            except Exception as exc:
                print(f"⚠️ Connector discover failed ({connector_name}): {exc}")
                continue

            for item in discovered or []:
                metadata = dict(item.metadata or {})
                metadata.setdefault("connector", connector_name)
                index[item.name] = {
                    "connector_name": connector_name,
                    "source_type": connector_name,
                    "source_format": item.format,
                    "source_location": item.location,
                    "source_metadata": metadata,
                }
        return index

    def _build_connector_dataset(self, dataset_meta: Dict[str, Any]) -> ConnectorDataset:
        name = str(dataset_meta.get("name") or "").strip()
        location = str(dataset_meta.get("source_location") or name).strip()
        data_format = str(dataset_meta.get("source_format") or "connector_table").strip()
        metadata = dataset_meta.get("source_metadata")
        if not isinstance(metadata, dict):
            metadata = {}
        return ConnectorDataset(name=name, location=location, format=data_format, metadata=metadata)

    def evaluate_discovered_dataset(self, dataset_meta: Dict[str, Any], force_load: bool = False) -> Dict[str, Any]:
        """
        Evaluate a discovered dataset entry from discover_datasets().
        Supports local file datasets and connector-backed datasets.
        """
        dataset_name = str(dataset_meta.get("name") or "").strip()
        if not dataset_name:
            raise ValueError("dataset_name is required")

        data_file = dataset_meta.get("data_file")
        if data_file:
            return self.evaluate_data_file(str(data_file), dataset_name, force_load=force_load)

        connector_name = str(dataset_meta.get("connector_name") or "").strip()
        if not connector_name:
            raise FileNotFoundError(f"Data file for {dataset_name} not found")

        connector = self.connectors_by_name.get(connector_name)
        if connector is None:
            raise RuntimeError(f"Connector '{connector_name}' is not configured")

        connector_dataset = self._build_connector_dataset(dataset_meta)
        sample_limit = int(os.getenv("DRE_CONNECTOR_EVAL_SAMPLE_LIMIT", "1000"))
        rows = connector.read_sample(connector_dataset, limit=max(1, min(sample_limit, 10000)))
        if not rows:
            raise RuntimeError(f"Connector sample for {dataset_name} returned no rows")

        import pandas as pd

        stage_dir = Path("data/staged_connector")
        stage_dir.mkdir(parents=True, exist_ok=True)
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", dataset_name)
        staged_path = stage_dir / f"{safe_name}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.csv"
        pd.DataFrame(rows).to_csv(staged_path, index=False)
        return self.evaluate_data_file(str(staged_path), dataset_name)

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
        connector_index = self._discover_connector_index()
        
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
                source_info = connector_index.get(dataset_name, {})
                
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
                    "connector_name": source_info.get("connector_name"),
                    "source_type": source_info.get("source_type"),
                    "source_format": source_info.get("source_format"),
                    "source_location": source_info.get("source_location"),
                    "source_metadata": source_info.get("source_metadata"),
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
                    "connector_name": None,
                    "source_type": None,
                    "source_format": None,
                    "source_location": None,
                    "source_metadata": {},
                })
        
        # -------------------------------------------------------------
        # Phase 1.5: Discover Unmanaged Files (No Contract Yet)
        # -------------------------------------------------------------
        # Unmanaged discovery is useful in the default runtime, but it makes unit
        # tests brittle when they point contracts_path at a temp directory while
        # the repo root contains unrelated sample data files.
        managed_datasets = {d["name"] for d in datasets}
        try:
            contracts_root = Path(getattr(self.contract_store, "root_path", self.contracts_path)).resolve()
        except Exception:
            contracts_root = Path(self.contracts_path).resolve()
        default_contracts_root = Path(os.getenv("CONTRACTS_PATH", "config/expectations")).resolve()
        discover_unmanaged = os.getenv("DRE_DISCOVER_UNMANAGED", "1").strip() != "0" and contracts_root == default_contracts_root

        data_dirs = ["data/test", "data/landing", "data/pending_approval", "data"]

        if not discover_unmanaged:
            print(f"\n📂 Auto-Discovery: Found {len(datasets)} dataset contract(s)")
            for ds in datasets:
                icon = "✅" if ds.get("data_file") or ds.get("connector_name") else "⚠️"
                print(
                    f"   {icon} {ds['name']} ({ds['column_count']} cols, "
                    f"criticality={ds['criticality']}, lifecycle={ds['lifecycle']})"
                )
            return datasets
        
        for d_dir in data_dirs:
            path = Path(d_dir)
            if not path.exists():
                continue
                
            # Scan for supported data files
            for file_path in path.glob("*"):
                if not self._is_supported_data_file(file_path):
                    continue

                stem = file_path.stem
                if any(stem == managed or stem.startswith(f"{managed}_") for managed in managed_datasets):
                    continue
                    
                # Extract stable dataset id (e.g. newdata_2026-02-15 -> newdata)
                candidate_name = self._extract_dataset_name_from_stem(stem)
                    
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
                    "version": "0.0.0",
                    "connector_name": None,
                    "source_type": "local_files",
                    "source_format": file_path.suffix.lstrip(".").lower(),
                    "source_location": str(file_path),
                    "source_metadata": {},
                })
                managed_datasets.add(candidate_name) # Prevent duplicates across dirs

        for connector_dataset_name, source_info in connector_index.items():
            if connector_dataset_name in managed_datasets:
                continue
            print(
                f"   🆕 Discovered unmanaged connector dataset: "
                f"{source_info.get('source_location') or connector_dataset_name}"
            )
            datasets.append(
                {
                    "name": connector_dataset_name,
                    "contract_path": None,
                    "data_file": None,
                    "column_count": 0,
                    "columns": [],
                    "has_quality_rules": False,
                    "has_anomaly_thresholds": False,
                    "lifecycle": "unconfigured",
                    "criticality": "UNKNOWN",
                    "owner": "Unassigned",
                    "domain": f"Discovered/{source_info.get('source_type') or 'connector'}",
                    "version": "0.0.0",
                    "connector_name": source_info.get("connector_name"),
                    "source_type": source_info.get("source_type"),
                    "source_format": source_info.get("source_format"),
                    "source_location": source_info.get("source_location"),
                    "source_metadata": source_info.get("source_metadata") or {},
                }
            )
            managed_datasets.add(connector_dataset_name)
        
        print(f"\n📂 Auto-Discovery: Found {len(datasets)} dataset contract(s)")
        for ds in datasets:
            icon = "✅" if ds.get("data_file") or ds.get("connector_name") else "⚠️"
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
                # Prefer explicit data_dir argument for batch evaluation.
                try:
                    base = Path(data_dir)
                    if base.exists():
                        for candidate in base.glob(f"{name}.*"):
                            if self._is_supported_data_file(candidate):
                                data_file = str(candidate)
                                break
                except Exception:
                    data_file = None

            if not data_file:
                data_file = self._find_latest_data_file(name)
            
            connector_available = bool(ds.get("connector_name"))

            if not data_file and not connector_available:
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
            
            source_ref = data_file or ds.get("source_location") or "<connector>"
            print(f"\n{'='*60}")
            print(f"🔍 Evaluating: {name} ({source_ref})")
            print(f"{'='*60}")
            
            try:
                effective_meta = dict(ds)
                if data_file and not effective_meta.get("data_file"):
                    effective_meta["data_file"] = data_file

                result = self.evaluate_discovered_dataset(effective_meta)
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
                               quality_score, anomaly_count, z_score_max, reason, duration_ms, dimension_scores
                        FROM run_history
                        WHERE dataset_name = %s
                        ORDER BY timestamp DESC LIMIT %s
                    """, (dataset_name, limit))
                else:
                    cur.execute("""
                        SELECT run_id, timestamp, dataset_name, status,
                               quality_score, anomaly_count, z_score_max, reason, duration_ms, dimension_scores
                        FROM run_history
                        ORDER BY timestamp DESC LIMIT %s
                    """, (limit,))

                rows = cur.fetchall()
                history = []
                for r in rows:
                    effective_quality = r[4]
                    dim_payload = r[9]
                    if isinstance(dim_payload, str):
                        try:
                            dim_payload = json.loads(dim_payload)
                        except Exception:
                            dim_payload = None
                    if isinstance(dim_payload, dict):
                        overall = dim_payload.get("overall_score")
                        if isinstance(overall, (int, float)):
                            effective_quality = float(overall)

                    history.append(
                        {
                            "run_id": r[0],
                            "timestamp": r[1].isoformat() if r[1] else None,
                            "dataset": r[2],
                            "status": r[3],
                            "quality_score": effective_quality,
                            "anomaly_count": r[5],
                            "z_score_max": r[6],
                            "reason": r[7],
                            "duration_ms": r[8],
                        }
                    )
                return history

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
        request_context = context_data.get("request_context", {})
        
        context_str = "### SYSTEM STATE ###\n"
        context_str += f"Total Datasets: {len(discovered)}\n"
        
        for ds in discovered:
            name = ds["name"]
            res = results.get(name, {})
            status = res.get("status", "UNKNOWN")
            reason = res.get("reason", "No data")
            crit = ds.get("criticality", "UNKNOWN")
            quality_score = res.get("quality_score")
            try:
                quality_text = f"{float(quality_score):.2f}%"
            except Exception:
                quality_text = "n/a"

            context_str += f"- Dataset: {name} | Status: {status} | Quality Score: {quality_text} | Crit: {crit} | Reason: {reason}\n"
            if res.get("anomalies"):
                context_str += f"  - Anomalies: {len(res['anomalies'])} detected (Max Z-Score: {max((a.get('z_score',0) for a in res['anomalies']), default=0):.1f})\n"
            if res.get("schema_evolution", {}).get("missing_columns"):
                context_str += f"  - Schema Issues: Missing cols {res['schema_evolution']['missing_columns']}\n"

        context_json = ""
        if isinstance(request_context, dict) and request_context:
            try:
                context_json = json.dumps(request_context, default=str, indent=2)
            except Exception:
                context_json = "{}"
            if len(context_json) > 12000:
                context_json = context_json[:12000] + "\n... (truncated)"

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
     -> Provide: exact evidence fields from context (run_id, status, reason, failing metrics/checks, failing tool output).
   - **Tier 3 (Remediation)**: User asks "How do I fix this?" 
     -> Provide: Step-by-step action plan.

4. **FORMATTING**:
   - Use Markdown.
   - If the answer is short (Tier 1), use a single paragraph or checkmark list. No big headers.
   - **Conciseness Rule**: If you can answer in 2 sentences, do it.

5. **FACTUALITY (HARD RULE)**:
   - Never infer "Health Score" from status.
   - Use provided numeric quality score when available.
   - If a value is missing, say "unavailable" instead of guessing.
   - If load failed but checks passed, explicitly state that distinction.
6. **MULTI-TURN CONTEXT**:
   - If REQUEST CONTEXT includes `conversation_turns`, use it to resolve follow-up references like "that", "it", "this run".
   - If REQUEST CONTEXT includes `dataset_context`, prioritize those concrete artifacts (contract, 6D scores, latest verdict, sample rows).

### AVAILABLE CONTEXT:
{context_str}

### REQUEST CONTEXT (if provided by UI):
{context_json}
"""
        # 3. Create Ephemeral Agent for this turn
        chat_agent = Agent(
            model=OpenAIChat(id=os.getenv("OPENAI_MODEL_NAME", "gpt-4o")),
            description="You are a precise Data Reliability Engineer.",
            instructions=system_prompt.format(context_str=context_str, context_json=context_json or "{}"),
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

        if not meta:
            raise FileNotFoundError(f"Data file for {dataset_name} not found")

        data_file = meta.get("data_file")
        if data_file:
            # 2. Read local data based on extension
            try:
                import pandas as pd

                if data_file.lower().endswith('.parquet'):
                    df = pd.read_parquet(data_file)
                elif data_file.lower().endswith('.json'):
                    df = pd.read_json(data_file)
                else:
                    # Default to CSV for .csv and other text-based formats
                    df = pd.read_csv(data_file)

                total_rows = len(df)
                # 3. Sample and convert to dict
                # Replace NaN with None for JSON compatibility
                df = df.head(limit).replace({float('nan'): None})

                return {
                    "columns": list(df.columns),
                    "data": df.to_dict(orient="records"),
                    "total_rows": total_rows,
                    "preview_limit": limit
                }
            except Exception as e:
                raise RuntimeError(f"Failed to read data file: {str(e)}")

        connector_name = str(meta.get("connector_name") or "").strip()
        connector = self.connectors_by_name.get(connector_name) if connector_name else None
        if connector is None:
            raise FileNotFoundError(f"Data source for {dataset_name} not found")

        try:
            rows = connector.read_sample(self._build_connector_dataset(meta), limit=limit)
            columns = list(rows[0].keys()) if rows else []
            return {
                "columns": columns,
                "data": rows,
                "total_rows": len(rows),
                "preview_limit": limit,
            }
        except Exception as exc:
            raise RuntimeError(f"Failed to read connector dataset: {exc}")

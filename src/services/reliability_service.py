from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml
from fastapi import HTTPException

from src.agents.monitor_agent import MonitorAgent
from src.contracts.store import FileContractStore
from src.services.action_audit_service import ActionAuditService
from src.utils.database import get_connection


class ReliabilityService:
    """
    Service layer for core reliability operations used by API routes and MCP tools.
    """

    def __init__(
        self,
        agent: MonitorAgent,
        contract_store: FileContractStore,
        hitl_workflow: Optional[Any] = None,
        audit_service: Optional[ActionAuditService] = None,
        agentic_workflow: Optional[Any] = None,
    ):
        self.agent = agent
        self.contract_store = contract_store
        self.hitl_workflow = hitl_workflow
        self.audit_service = audit_service
        self.agentic_workflow = agentic_workflow

        if self.agentic_workflow is None:
            try:
                from src.workflows.agentic_reliability_workflow import AgenticReliabilityWorkflow

                self.agentic_workflow = AgenticReliabilityWorkflow(service=self)
            except Exception:
                # Keep legacy direct execution fallback when LangGraph runtime is unavailable.
                self.agentic_workflow = None

    def _audit(self, *, action: str, dataset_name: str, status: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        if not self.audit_service:
            return
        try:
            self.audit_service.record(
                action=action,
                dataset_name=dataset_name,
                status=status,
                actor="system",
                source="reliability_service",
                metadata=metadata or {},
            )
        except Exception:
            # Never break reliability execution due to audit logging.
            return

    @staticmethod
    def _matches_dataset_artifact(name: str, dataset_name: str) -> bool:
        """
        True when a file/table name belongs to the dataset or its timestamped variants.
        """
        normalized_name = name.lower()
        normalized_dataset = dataset_name.lower()

        if normalized_name == normalized_dataset:
            return True

        for sep in ("_", ".", "-"):
            if normalized_name.startswith(f"{normalized_dataset}{sep}"):
                return True

        return False

    def evaluate_dataset(self, dataset_name: str, force_load: bool = False) -> Dict[str, Any]:
        self._audit(action="evaluate_started", dataset_name=dataset_name, status="RUNNING", metadata={"force_load": force_load})
        datasets = self.agent.discover_datasets()
        meta = next((d for d in datasets if d["name"] == dataset_name), None)
        if not meta:
            self._audit(action="evaluate_failed", dataset_name=dataset_name, status="FAILED", metadata={"error": "data file not found"})
            raise HTTPException(status_code=404, detail=f"Data file for {dataset_name} not found.")
        data_file = str(meta["data_file"]) if meta.get("data_file") else None
        has_connector_source = bool(meta.get("connector_name"))

        if not data_file and not has_connector_source:
            self._audit(action="evaluate_failed", dataset_name=dataset_name, status="FAILED", metadata={"error": "data file not found"})
            raise HTTPException(status_code=404, detail=f"Data file for {dataset_name} not found.")
        if not data_file and has_connector_source and not self.contract_store.exists(dataset_name):
            self._audit(
                action="evaluate_failed",
                dataset_name=dataset_name,
                status="FAILED",
                metadata={"error": "contract not found for connector dataset"},
            )
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Connector dataset '{dataset_name}' has no approved contract. "
                    "Approve a contract before evaluation."
                ),
            )

        if self.hitl_workflow is not None and data_file:
            try:
                workflow_result = self.hitl_workflow.run_for_file(
                    dataset_name=dataset_name,
                    file_path=data_file,
                    source="api",
                    apply_file_actions=False,
                    force_load=force_load,
                )
                if workflow_result.get("mode") == "evaluated" and isinstance(workflow_result.get("verdict"), dict):
                    verdict = workflow_result["verdict"]
                    run_id = verdict.get("run_id")
                    self._audit(
                        action="evaluate_completed",
                        dataset_name=dataset_name,
                        status=str(verdict.get("status") or "COMPLETED"),
                        metadata={"run_id": run_id} if isinstance(run_id, str) and run_id.strip() else {},
                    )
                    return workflow_result["verdict"]
                # HITL modes (pending/interrupt) are still useful to audit.
                self._audit(
                    action="evaluate_completed",
                    dataset_name=dataset_name,
                    status=str(workflow_result.get("mode") or "COMPLETED"),
                    metadata={"mode": workflow_result.get("mode")},
                )
                return workflow_result
            except Exception:
                # Keep direct-eval fallback for compatibility when graph/checkpointer is unavailable.
                if self.contract_store.exists(dataset_name):
                    verdict = self.agent.evaluate_data_file(data_file, dataset_name, force_load=force_load)
                    run_id = verdict.get("run_id") if isinstance(verdict, dict) else None
                    self._audit(
                        action="evaluate_completed",
                        dataset_name=dataset_name,
                        status=str((verdict or {}).get("status") or "COMPLETED"),
                        metadata={"run_id": run_id} if isinstance(run_id, str) and run_id.strip() else {},
                    )
                    return verdict
                self._audit(action="evaluate_failed", dataset_name=dataset_name, status="FAILED", metadata={"error": "hitl workflow error"})
                raise

        try:
            if data_file:
                verdict = self.agent.evaluate_data_file(data_file, dataset_name, force_load=force_load)
            else:
                verdict = self.agent.evaluate_discovered_dataset(meta, force_load=force_load)
            run_id = verdict.get("run_id") if isinstance(verdict, dict) else None
            self._audit(
                action="evaluate_completed",
                dataset_name=dataset_name,
                status=str((verdict or {}).get("status") or "COMPLETED"),
                metadata={"run_id": run_id} if isinstance(run_id, str) and run_id.strip() else {},
            )
            return verdict
        except Exception as exc:
            self._audit(action="evaluate_failed", dataset_name=dataset_name, status="FAILED", metadata={"error": str(exc)})
            raise

    def delete_dataset(self, dataset_name: str) -> Dict[str, Any]:
        """
        Hard-delete a dataset and all known local artifacts.
        """
        dataset_name = dataset_name.strip()
        if not dataset_name:
            raise HTTPException(status_code=400, detail="dataset_name is required")

        deleted_files: List[str] = []
        db_deleted_counts: Dict[str, Optional[int]] = {}
        duckdb_summary: Dict[str, Any] = {}

        def safe_unlink(path: Path) -> None:
            if not path.exists() or not path.is_file():
                return
            path.unlink()
            deleted_files.append(str(path))

        try:
            # 1) Filesystem cleanup
            try:
                contract_file = self.contract_store.path_for(dataset_name)
            except ValueError:
                # Fallback for legacy dataset names containing unsupported characters.
                contract_file = Path(f"config/expectations/{dataset_name}.yaml")

            explicit_files = [
                contract_file,
                Path(f"config/proposals/{dataset_name}.yaml"),
                Path(f"config/proposals/{dataset_name}.meta.json"),
                Path(f"data/{dataset_name}.csv"),
                Path(f"data/{dataset_name}.parquet"),
                Path(f"data/{dataset_name}.json"),
                Path(f"data/test/{dataset_name}.csv"),
                Path(f"data/test/{dataset_name}.parquet"),
                Path(f"data/test/{dataset_name}.json"),
            ]
            for path in explicit_files:
                safe_unlink(path)

            wildcard_candidates: List[Path] = []
            wildcard_candidates.extend(self.contract_store.root_path.glob(f"{dataset_name}.backup_*.yaml"))
            wildcard_candidates.extend(Path("config/history").glob(f"{dataset_name}_*.yaml"))

            # Data + queue directories where files are named <dataset>_<suffix>.*
            for directory in (
                Path("data/landing"),
                Path("data/pending_approval"),
                Path("data/quarantine"),
                Path("data/test"),
                Path("data"),
                Path("logs/runs"),
            ):
                if not directory.exists():
                    continue
                for file_path in directory.glob("*"):
                    if file_path.is_file() and self._matches_dataset_artifact(file_path.name, dataset_name):
                        wildcard_candidates.append(file_path)

            # Historical verdict logs are nested by date
            history_root = Path("data/history")
            if history_root.exists():
                for file_path in history_root.rglob("*"):
                    if file_path.is_file() and self._matches_dataset_artifact(file_path.name, dataset_name):
                        wildcard_candidates.append(file_path)

            # De-duplicate candidates before deleting
            for path in {p.resolve() for p in wildcard_candidates}:
                safe_unlink(path)

            # 2) PostgreSQL cleanup
            # Some tables may not exist in all environments. Guard with to_regclass.
            dataset_tables = [
                "run_history",
                "metric_history",
                "learned_thresholds",
                "dataset_registry",
                "schema_audit_log",
                "remediation_history",
                "tool_outputs",
                "contract_versions",
                "diagnostics_records",
                "agentic_remediation_runs",
            ]

            with get_connection() as conn:
                with conn.cursor() as cur:
                    for table in dataset_tables:
                        cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
                        exists = cur.fetchone()[0] is not None
                        if not exists:
                            db_deleted_counts[table] = None
                            continue

                        cur.execute(f"DELETE FROM {table} WHERE dataset_name = %s", (dataset_name,))
                        db_deleted_counts[table] = cur.rowcount

                    # Cleanup attempt rows via run linkage (attempts table has no dataset_name).
                    cur.execute("SELECT to_regclass(%s)", ("public.agentic_remediation_attempts",))
                    attempts_exists = cur.fetchone()[0] is not None
                    if attempts_exists:
                        cur.execute(
                            """
                            DELETE FROM agentic_remediation_attempts
                            WHERE remediation_run_id IN (
                                SELECT id FROM agentic_remediation_runs WHERE dataset_name = %s
                            )
                            """,
                            (dataset_name,),
                        )
                        db_deleted_counts["agentic_remediation_attempts"] = cur.rowcount
                    else:
                        db_deleted_counts["agentic_remediation_attempts"] = None

            # 3) DuckDB cleanup (if any local DuckDB files exist)
            duckdb_files: set[Path] = set()
            for directory in (Path("."), Path("data"), Path("logs")):
                if not directory.exists():
                    continue
                duckdb_files.update(directory.glob("*.duckdb"))
                duckdb_files.update(directory.glob("*.db"))

            if duckdb_files:
                try:
                    import duckdb

                    for db_path in sorted(duckdb_files):
                        dropped_tables: List[str] = []
                        try:
                            conn = duckdb.connect(str(db_path))
                            table_rows = conn.execute("SHOW TABLES").fetchall()
                            for row in table_rows:
                                table_name = row[0]
                                if self._matches_dataset_artifact(table_name, dataset_name):
                                    conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                                    dropped_tables.append(table_name)
                            conn.close()
                        except Exception as duck_err:
                            duckdb_summary[str(db_path)] = {"error": str(duck_err)}
                            continue

                        if dropped_tables:
                            duckdb_summary[str(db_path)] = {"dropped_tables": dropped_tables}
                except Exception as import_err:
                    duckdb_summary["duckdb_import"] = {"error": str(import_err)}

            return {
                "status": "deleted",
                "dataset_name": dataset_name,
                "deleted_file_count": len(deleted_files),
                "deleted_files": sorted(deleted_files),
                "postgres_deleted_rows": db_deleted_counts,
                "duckdb_cleanup": duckdb_summary,
            }
        except HTTPException:
            raise

    def reset_runtime_state(
        self,
        *,
        clear_generated_contracts: bool = True,
        preserve_contract_names: Optional[List[str]] = None,
        clear_langgraph_checkpoints: bool = True,
    ) -> Dict[str, Any]:
        """
        Reset runtime state for clean-slate testing.

        This clears runtime DB state and local generated artifacts while preserving
        code/config files. Contract cleanup is optional and can preserve a safelist.
        """
        baseline_preserved = {"transactions"}
        preserved = baseline_preserved | {
            str(name).strip().lower()
            for name in (preserve_contract_names or [])
            if str(name).strip()
        }

        runtime_tables = [
            "action_audit_log",
            "agentic_remediation_attempts",
            "agentic_remediation_runs",
            "async_jobs",
            "contract_versions",
            "dataset_registry",
            "diagnostics_records",
            "incidents",
            "learned_thresholds",
            "metric_history",
            "remediation_history",
            "run_history",
            "schema_audit_log",
            "slo_history",
            "tool_outputs",
        ]
        checkpoint_tables = [
            "checkpoint_blobs",
            "checkpoint_writes",
            "checkpoints",
        ]

        truncated_tables = list(runtime_tables)
        if clear_langgraph_checkpoints:
            truncated_tables.extend(checkpoint_tables)

        if truncated_tables:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    sql = "TRUNCATE TABLE " + ", ".join(truncated_tables) + " RESTART IDENTITY"
                    cur.execute(sql)

        removed_files: List[str] = []

        def remove_files(directory: Path, *, recursive: bool = False) -> int:
            if not directory.exists():
                return 0
            removed = 0
            iterator = directory.rglob("*") if recursive else directory.glob("*")
            for path in iterator:
                if not path.is_file():
                    continue
                if path.name in {".keep", ".gitkeep"}:
                    continue
                path.unlink()
                removed += 1
                removed_files.append(str(path))
            return removed

        files_summary = {
            "data_landing": remove_files(Path("data/landing")),
            "data_pending_approval": remove_files(Path("data/pending_approval")),
            "data_quarantine": remove_files(Path("data/quarantine")),
            "data_staged_connector": remove_files(Path("data/staged_connector")),
            "data_test": remove_files(Path("data/test")),
            "data_history": remove_files(Path("data/history"), recursive=True),
            "config_proposals": remove_files(Path("config/proposals")),
            "logs_runs": remove_files(Path("logs/runs"), recursive=True),
        }

        removed_contracts = 0
        removed_contract_history = 0
        if clear_generated_contracts:
            def should_preserve_contract_stem(stem: str) -> bool:
                # Contract history naming is commonly: <dataset>_v<timestamp>
                # Support preserved dataset names containing underscores.
                for name in preserved:
                    if stem == name:
                        return True
                    if stem.startswith(f"{name}_v"):
                        return True
                return False

            root = Path(self.contract_store.root_path)
            if root.exists():
                for path in root.glob("*.yaml"):
                    stem = path.stem.lower()
                    if should_preserve_contract_stem(stem):
                        continue
                    path.unlink()
                    removed_contracts += 1
                    removed_files.append(str(path))

            history_dir = Path("config/history")
            if history_dir.exists():
                for path in history_dir.glob("*.yaml"):
                    stem = path.stem.lower()
                    if should_preserve_contract_stem(stem):
                        continue
                    path.unlink()
                    removed_contract_history += 1
                    removed_files.append(str(path))

        return {
            "status": "reset_completed",
            "generated_at": datetime.now().isoformat(),
            "db": {
                "truncated_tables": truncated_tables,
                "checkpoint_tables_cleared": bool(clear_langgraph_checkpoints),
            },
            "files": {
                "summary": files_summary,
                "removed_count": len(removed_files),
                "removed_examples": removed_files[:50],
            },
            "contracts": {
                "generated_contracts_cleared": bool(clear_generated_contracts),
                "preserved_contract_names": sorted(preserved),
                "removed_contract_count": removed_contracts,
                "removed_contract_history_count": removed_contract_history,
            },
        }

    def get_run_history(self, dataset_name: str, limit: int = 50) -> List[Dict[str, Any]]:
        return self.agent.get_run_history(dataset_name, limit)

    def get_run_verdict(self, run_id: str) -> Dict[str, Any]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, timestamp, dataset_name, status, quality_score,
                           anomaly_count, z_score_max, reason, duration_ms,
                           dimension_scores, full_verdict
                    FROM run_history
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                row = cur.fetchone()

                if not row:
                    raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

                # Parse JSONB columns robustly:
                # psycopg may return JSONB as dict/list (already decoded) or as raw JSON string.
                dimension_scores_raw = self._normalize_json(row[9]) if row[9] is not None else None
                full_verdict_raw = self._normalize_json(row[10]) if row[10] is not None else None
                dimension_scores = None if isinstance(dimension_scores_raw, str) else dimension_scores_raw
                full_verdict = None if isinstance(full_verdict_raw, str) else full_verdict_raw
                effective_quality_score = row[4]
                if isinstance(dimension_scores, dict):
                    overall = dimension_scores.get("overall_score")
                    if isinstance(overall, (int, float)):
                        effective_quality_score = float(overall)

                return {
                    "run_id": row[0],
                    "timestamp": row[1].isoformat() if row[1] else None,
                    "dataset_name": row[2],
                    "status": row[3],
                    "quality_score": effective_quality_score,
                    "anomaly_count": row[5],
                    "z_score_max": row[6],
                    "reason": row[7],
                    "duration_ms": row[8],
                    "dimension_scores": dimension_scores,
                    "full_verdict": full_verdict,
                }

    def chat_with_copilot(self, query: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        context_data = {"discovered": self.agent.discover_datasets(), "results": {}}

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT dataset_name, last_status, criticality FROM dataset_registry")
                rows = cur.fetchall()
                for row in rows:
                    name = row[0]
                    history = self.agent.get_run_history(name, limit=1)
                    latest = history[0] if history else {}
                    context_data["results"][name] = {
                        "status": latest.get("status") or row[1],
                        "quality_score": latest.get("quality_score"),
                        "run_id": latest.get("run_id"),
                        "timestamp": latest.get("timestamp"),
                        "reason": latest.get("reason", "No recent run data"),
                        "anomalies": [],
                        "schema_evolution": {},
                    }

        if isinstance(context, dict) and context:
            context_data["request_context"] = context

        response = self.agent.request_copilot_chat(query, context_data)
        return {"response": response}

    def propose_contract(self, dataset_name: str, file_path: Optional[str] = None) -> Dict[str, Any]:
        data_path = self._resolve_dataset_file(dataset_name=dataset_name, explicit_path=file_path)
        if not data_path:
            raise HTTPException(status_code=404, detail=f"Data file not found for dataset {dataset_name}")

        proposal = self.agent.propose_contract(dataset_name, data_path, include_metadata=True)
        return {
            "status": "success",
            "proposed_yaml": proposal.get("yaml_content", ""),
            "generation": {
                "engine": proposal.get("engine"),
                "success": proposal.get("success"),
                "cli_available": proposal.get("cli_available"),
                "errors": proposal.get("errors", []),
                "warnings": proposal.get("warnings", []),
                "generated_at": proposal.get("generated_at"),
            },
        }

    def _resolve_dataset_file(self, *, dataset_name: str, explicit_path: Optional[str] = None) -> Optional[str]:
        if explicit_path and Path(explicit_path).exists():
            return explicit_path

        datasets = self.agent.discover_datasets()
        meta = next((d for d in datasets if d["name"] == dataset_name), None)
        if meta and meta.get("data_file"):
            resolved = str(meta["data_file"])
            if Path(resolved).exists():
                return resolved

        fallback = Path(f"data/{dataset_name}.csv")
        if fallback.exists():
            return str(fallback)
        return None

    @staticmethod
    def _read_dataframe(path: str) -> pd.DataFrame:
        source = Path(path)
        ext = source.suffix.lower()
        if ext == ".parquet":
            return pd.read_parquet(source)
        if ext == ".json":
            return pd.read_json(source)
        return pd.read_csv(source)

    def run_contract_gate(self, *, dataset_name: str, file_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Shift-left contract gate for CI/CD and orchestration pre-checks.
        Runs schema + profile checks without downstream load side effects.
        """
        contract_doc = self.contract_store.read(dataset_name)
        if not contract_doc:
            raise HTTPException(status_code=404, detail=f"No active contract found for dataset {dataset_name}")

        data_path = self._resolve_dataset_file(dataset_name=dataset_name, explicit_path=file_path)
        if not data_path:
            raise HTTPException(status_code=404, detail=f"Data file not found for dataset {dataset_name}")

        from src.tools.schema_validator import SchemaValidator

        validator = SchemaValidator(contract_doc.location)
        ext = Path(data_path).suffix.lower().lstrip(".") or "csv"
        schema_result = validator.validate_file(data_path, file_format=ext)
        schema_payload = schema_result.to_dict()

        df = self._read_dataframe(data_path)
        profile_report = self.agent.profiler.profile(df, contract_doc.location, dataset_name)
        profile_payload = profile_report.to_dict()

        schema_errors = [
            issue for issue in schema_payload.get("issues", [])
            if str(issue.get("severity") or "").lower() == "fail"
        ]
        schema_warnings = [
            issue for issue in schema_payload.get("issues", [])
            if str(issue.get("severity") or "").lower() == "warning"
        ]

        failed_custom_checks = [
            check for check in (profile_payload.get("custom_check_results") or [])
            if isinstance(check, dict) and not bool(check.get("passed", True))
        ]
        constraint_violations = profile_payload.get("constraint_violations") or []
        column_violations = 0
        for col_payload in (profile_payload.get("column_profiles") or {}).values():
            if isinstance(col_payload, dict):
                column_violations += len(col_payload.get("violations") or [])

        blocked = bool(schema_errors or failed_custom_checks or constraint_violations or column_violations > 0)

        checks = {
            "schema_errors": len(schema_errors),
            "schema_warnings": len(schema_warnings),
            "constraint_violations": len(constraint_violations) if isinstance(constraint_violations, list) else 0,
            "failed_custom_checks": len(failed_custom_checks),
            "column_violations": int(column_violations),
            "quality_score": float(profile_payload.get("overall_quality_score") or 0.0),
        }

        return {
            "dataset_name": dataset_name,
            "file_path": data_path,
            "contract_path": contract_doc.location,
            "gate": "contract_ci",
            "status": "BLOCKED" if blocked else "PASSED",
            "checks": checks,
            "schema": {
                "status": schema_payload.get("status"),
                "errors": schema_errors,
                "warnings": schema_warnings,
                "schema_diff": schema_payload.get("schema_diff"),
            },
            "profile": {
                "overall_quality_score": profile_payload.get("overall_quality_score"),
                "constraint_violations": constraint_violations,
                "failed_custom_checks": failed_custom_checks,
            },
            "reason": (
                "Contract gate failed due to schema/profile violations."
                if blocked
                else "Contract gate passed. Safe to continue pipeline execution."
            ),
        }

    def generate_autopilot_contract(
        self,
        *,
        dataset_name: str,
        file_path: Optional[str] = None,
        confidence_threshold: float = 0.75,
    ) -> Dict[str, Any]:
        """
        Produce generic, confidence-scored contract recommendations with rationale.
        Avoids domain-hallucinated constraints.
        """
        data_path = self._resolve_dataset_file(dataset_name=dataset_name, explicit_path=file_path)
        if not data_path:
            raise HTTPException(status_code=404, detail=f"Data file not found for dataset {dataset_name}")

        proposal = self.agent.propose_contract(dataset_name, data_path, include_metadata=True)
        proposed_yaml = str(proposal.get("yaml_content") or "")
        contract = yaml.safe_load(proposed_yaml) or {}
        if not isinstance(contract, dict):
            contract = {}

        df = self._read_dataframe(data_path)
        row_count = len(df)
        columns = contract.get("columns") if isinstance(contract.get("columns"), list) else []
        recommendations: List[Dict[str, Any]] = []
        safe_threshold = max(0.0, min(float(confidence_threshold), 1.0))

        def append_reco(*, path: str, current: Any, proposed: Any, confidence: float, rationale: str, reco_type: str):
            score = round(max(0.0, min(confidence, 1.0)), 2)
            recommendations.append(
                {
                    "type": reco_type,
                    "path": path,
                    "current": current,
                    "proposed": proposed,
                    "confidence": score,
                    "rationale": rationale,
                    "auto_apply": score >= safe_threshold,
                }
            )

        # Dataset-level recommendation: avoid zero-row contracts in CI.
        quality = contract.setdefault("quality", {})
        if not isinstance(quality, dict):
            quality = {}
            contract["quality"] = quality
        current_min_rows = quality.get("min_rows")
        recommended_min_rows = max(1, int(row_count * 0.2)) if row_count > 0 else 1
        if current_min_rows is None or int(current_min_rows) < 1:
            append_reco(
                path="quality.min_rows",
                current=current_min_rows,
                proposed=recommended_min_rows,
                confidence=0.95,
                rationale="Observed dataset contains rows. Enforcing non-zero minimum catches empty-load regressions.",
                reco_type="volume_floor",
            )
            quality["min_rows"] = recommended_min_rows

        for idx, col in enumerate(columns):
            if not isinstance(col, dict):
                continue
            col_name = str(col.get("name") or "").strip()
            if not col_name or col_name not in df.columns:
                continue

            series = df[col_name]
            non_null = series.dropna()
            null_rate = float(series.isna().mean()) if len(series) else 0.0
            unique_ratio = float(non_null.nunique() / max(len(non_null), 1))
            lower_name = col_name.lower()

            # Non-null constraints when observed null rate is exactly zero.
            if null_rate == 0.0 and col.get("nullable", True) is True:
                append_reco(
                    path=f"columns[{idx}].nullable",
                    current=col.get("nullable"),
                    proposed=False,
                    confidence=0.88,
                    rationale="No nulls observed in sample. Non-null constraint is generic and low-risk.",
                    reco_type="completeness",
                )
                col["nullable"] = False

            # Candidate PK: id-like names with near-perfect uniqueness and no nulls.
            is_id_like = lower_name == "id" or lower_name.endswith("_id")
            if is_id_like and null_rate == 0.0 and unique_ratio >= 0.995 and not bool(col.get("isPrimaryKey")):
                append_reco(
                    path=f"columns[{idx}].isPrimaryKey",
                    current=col.get("isPrimaryKey"),
                    proposed=True,
                    confidence=0.82,
                    rationale="ID-like column is highly unique and non-null in observed data.",
                    reco_type="uniqueness",
                )
                col["isPrimaryKey"] = True

            # Categorical allowed-values for stable low-cardinality string fields.
            inferred_dtype = str(series.dtype).lower()
            cardinality = int(non_null.nunique())
            if (
                inferred_dtype in {"object", "string"}
                and 2 <= cardinality <= 12
                and unique_ratio <= 0.2
                and not col.get("allowed_values")
            ):
                distinct_values = sorted({str(v) for v in non_null.unique() if str(v).strip()})[:12]
                if distinct_values:
                    append_reco(
                        path=f"columns[{idx}].allowed_values",
                        current=col.get("allowed_values"),
                        proposed=distinct_values,
                        confidence=0.76,
                        rationale="Low-cardinality categorical column detected; allow-list reduces invalid state drift.",
                        reco_type="validity",
                    )
                    col["allowed_values"] = distinct_values

            # Timestamp normalization for CSV/JSON imports that default to varchar.
            current_type = str(col.get("data_type") or "").strip().lower()
            if current_type in {"varchar", "string", "text"}:
                lower_col = lower_name
                if any(token in lower_col for token in ("timestamp", "time", "date", "_at")):
                    parsed = pd.to_datetime(series, errors="coerce", utc=True)
                    if float(parsed.notna().mean()) < 0.8:
                        # Pandas mixed parser handles heterogeneous ISO variants (with/without micros).
                        try:
                            parsed = pd.to_datetime(series, errors="coerce", utc=True, format="mixed")
                        except Exception:
                            pass
                    parse_ratio = float(parsed.notna().mean()) if len(series) else 0.0
                    if parse_ratio >= 0.95:
                        append_reco(
                            path=f"columns[{idx}].data_type",
                            current=col.get("data_type"),
                            proposed="timestamp",
                            confidence=0.91,
                            rationale=(
                                "Column name and observed values are strongly datetime-like; "
                                "normalizing to timestamp prevents schema drift blocks."
                            ),
                            reco_type="type_normalization",
                        )
                        col["data_type"] = "timestamp"

        return {
            "status": "success",
            "dataset_name": dataset_name,
            "file_path": data_path,
            "recommendation_count": len(recommendations),
            "confidence_threshold": safe_threshold,
            "recommendations": recommendations,
            "proposed_yaml": yaml.safe_dump(contract, sort_keys=False),
            "generation": {
                "engine": proposal.get("engine"),
                "cli_available": proposal.get("cli_available"),
                "warnings": proposal.get("warnings", []),
                "errors": proposal.get("errors", []),
            },
            "notes": [
                "Recommendations are schema/value-pattern based and domain-agnostic.",
                "No semantic hard bounds (for example age ranges) are invented automatically.",
            ],
        }

    def get_diagnostics_records(
        self,
        *,
        dataset_name: str,
        run_id: Optional[str] = None,
        check_type: Optional[str] = None,
        limit: int = 200,
    ) -> Dict[str, Any]:
        from src.services.diagnostics_service import DiagnosticsService

        service = DiagnosticsService()
        return service.list_records(
            dataset_name=dataset_name,
            run_id=run_id,
            check_type=check_type,
            limit=limit,
        )

    def get_pending_contracts(self) -> List[Dict[str, Any]]:
        proposals_dir = Path("config/proposals")
        pending_dir = Path("data/pending_approval")

        pending_contracts: List[Dict[str, Any]] = []

        if proposals_dir.exists():
            for meta_file in proposals_dir.glob("*.meta.json"):
                dataset_name = meta_file.stem.replace(".meta", "")

                with open(meta_file, "r") as f:
                    metadata = json.load(f)

                yaml_file = proposals_dir / f"{dataset_name}.yaml"
                if yaml_file.exists():
                    with open(yaml_file, "r") as f:
                        proposed_yaml = f.read()

                    pending_files = list(pending_dir.glob(f"{dataset_name}*"))

                    pending_contracts.append(
                        {
                            "dataset_name": dataset_name,
                            "proposed_at": metadata.get("proposed_at"),
                            "source_file": metadata.get("source_file"),
                            "row_count": metadata.get("row_count"),
                            "column_count": metadata.get("column_count"),
                            "proposed_yaml": proposed_yaml,
                            "pending_files": [p.name for p in pending_files],
                            "status": metadata.get("status", "pending_approval"),
                        }
                    )

        return pending_contracts

    def get_slo_summary(self, dataset_name: str, window: int = 200) -> Dict[str, Any]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH recent AS (
                        SELECT *
                        FROM slo_history
                        WHERE dataset_name = %s
                        ORDER BY timestamp DESC
                        LIMIT %s
                    )
                    SELECT
                        slo_name,
                        COUNT(*) AS total_checks,
                        COUNT(*) FILTER (WHERE status = 'PASS') AS pass_checks,
                        AVG(error_budget_burn) AS avg_error_budget_burn,
                        SUM(error_budget_burn) AS total_error_budget_burn,
                        MAX(timestamp) AS last_seen
                    FROM recent
                    GROUP BY slo_name
                    ORDER BY slo_name
                    """,
                    (dataset_name, window),
                )
                grouped = cur.fetchall()

                cur.execute(
                    """
                    WITH recent AS (
                        SELECT slo_name, status, error_budget_burn, timestamp
                        FROM slo_history
                        WHERE dataset_name = %s
                        ORDER BY timestamp DESC
                        LIMIT %s
                    )
                    SELECT slo_name, status, error_budget_burn, timestamp
                    FROM recent
                    ORDER BY slo_name, timestamp DESC
                    """,
                    (dataset_name, window),
                )
                recent_rows = cur.fetchall()

                cur.execute(
                    """
                    WITH recent AS (
                        SELECT status, error_budget_burn
                        FROM slo_history
                        WHERE dataset_name = %s
                        ORDER BY timestamp DESC
                        LIMIT %s
                    )
                    SELECT
                        COUNT(*) AS total_checks,
                        COUNT(*) FILTER (WHERE status = 'PASS') AS pass_checks,
                        AVG(error_budget_burn) AS avg_error_budget_burn,
                        SUM(error_budget_burn) AS total_error_budget_burn
                    FROM recent
                    """,
                    (dataset_name, window),
                )
                totals = cur.fetchone()
                total_checks = int(totals[0] or 0)
                pass_checks = int(totals[1] or 0)
                fail_checks = max(0, total_checks - pass_checks)
                overall_burn_avg = float(totals[2]) if totals[2] is not None else None
                overall_burn_total = float(totals[3]) if totals[3] is not None else 0.0

                statuses_by_slo: Dict[str, List[str]] = {}
                for row in recent_rows:
                    slo_name = str(row[0] or "").strip()
                    status = str(row[1] or "UNKNOWN").upper()
                    if not slo_name:
                        continue
                    statuses_by_slo.setdefault(slo_name, []).append(status)

                failing_slos: List[str] = []
                for slo_name, statuses in statuses_by_slo.items():
                    if statuses and statuses[0] != "PASS":
                        failing_slos.append(slo_name)

                def _last_status(slo_name: str) -> str:
                    statuses = statuses_by_slo.get(slo_name, [])
                    return statuses[0] if statuses else "UNKNOWN"

                def _recent_fail_streak(slo_name: str) -> int:
                    statuses = statuses_by_slo.get(slo_name, [])
                    streak = 0
                    for status in statuses:
                        if status == "PASS":
                            break
                        streak += 1
                    return streak

                return {
                    "dataset_name": dataset_name,
                    "window": window,
                    "overall_status": "PASS" if fail_checks == 0 else "FAIL",
                    "overall_pass_rate": round((pass_checks / total_checks) * 100, 2) if total_checks else None,
                    "overall_fail_rate": round((fail_checks / total_checks) * 100, 2) if total_checks else None,
                    "total_checks": total_checks,
                    "failing_checks": fail_checks,
                    "failing_slo_count": len(failing_slos),
                    "failing_slos": sorted(failing_slos),
                    "overall_error_budget_burn_avg": round(overall_burn_avg, 4) if overall_burn_avg is not None else None,
                    "overall_error_budget_burn_total": round(overall_burn_total, 4),
                    "checks": [
                        {
                            "slo_name": row[0],
                            "total_checks": row[1],
                            "pass_checks": row[2],
                            "pass_rate": round((row[2] / row[1]) * 100, 2) if row[1] else None,
                            "avg_error_budget_burn": float(row[3]) if row[3] is not None else None,
                            "total_error_budget_burn": float(row[4]) if row[4] is not None else 0.0,
                            "last_seen": row[5].isoformat() if row[5] else None,
                            "last_status": _last_status(str(row[0] or "")),
                            "recent_fail_streak": _recent_fail_streak(str(row[0] or "")),
                        }
                        for row in grouped
                    ],
                }

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except Exception:
            return default

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            if value is None:
                return default
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _normalize_json(value: Any) -> Any:
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, str):
            try:
                return json.loads(value)
            except Exception:
                return value
        return value

    @staticmethod
    def _dimension_score_map(payload: Any) -> Dict[str, float]:
        """
        Normalize saved dimension score payload into {dimension_name: score}.
        """
        result: Dict[str, float] = {}
        if not isinstance(payload, dict):
            return result

        dims = payload.get("dimensions")
        if isinstance(dims, list):
            for item in dims:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or "").strip()
                if not name:
                    continue
                score = ReliabilityService._safe_float(item.get("score"), None)  # type: ignore[arg-type]
                if score is not None:
                    result[name] = score
            if result:
                return result

        for key, value in payload.items():
            if isinstance(value, (int, float)):
                result[str(key)] = float(value)
        return result

    @staticmethod
    def _criticality_weight(criticality: str) -> float:
        mapping = {
            "CRITICAL": 25.0,
            "HIGH": 18.0,
            "MEDIUM": 10.0,
            "LOW": 5.0,
        }
        return mapping.get(str(criticality or "").upper(), 8.0)

    @staticmethod
    def _status_weight(status: str) -> float:
        mapping = {
            "BLOCKED": 60.0,
            "WARNING": 35.0,
            "PASSED": 5.0,
        }
        return mapping.get(str(status or "").upper(), 20.0)

    def _compute_risk_score(
        self,
        *,
        status: str,
        quality_score: float,
        anomaly_count: int,
        criticality: str,
        open_incidents: int,
    ) -> float:
        score = 0.0
        score += self._status_weight(status)
        score += min(max(0, anomaly_count) * 2.0, 20.0)
        score += max(0.0, 80.0 - max(0.0, quality_score)) * 0.3
        score += self._criticality_weight(criticality)
        score += min(max(0, open_incidents) * 10.0, 30.0)
        return round(score, 2)

    def compare_runs(self, run_id_1: str, run_id_2: str) -> Dict[str, Any]:
        """
        Compare two runs and return normalized deltas/regression summary.
        """
        run_a = self.get_run_verdict(run_id_1)
        run_b = self.get_run_verdict(run_id_2)

        qa = self._safe_float(run_a.get("quality_score"))
        qb = self._safe_float(run_b.get("quality_score"))
        aa = self._safe_int(run_a.get("anomaly_count"))
        ab = self._safe_int(run_b.get("anomaly_count"))
        za = self._safe_float(run_a.get("z_score_max"))
        zb = self._safe_float(run_b.get("z_score_max"))

        dims_a = self._dimension_score_map(run_a.get("dimension_scores"))
        dims_b = self._dimension_score_map(run_b.get("dimension_scores"))
        dimension_delta: Dict[str, float] = {}
        for name in sorted(set(dims_a.keys()) | set(dims_b.keys())):
            dimension_delta[name] = round(dims_b.get(name, 0.0) - dims_a.get(name, 0.0), 4)

        quality_delta = round(qb - qa, 4)
        anomaly_delta = ab - aa
        z_delta = round(zb - za, 4)

        regression_flags: List[str] = []
        if quality_delta < 0:
            regression_flags.append("quality_degraded")
        if anomaly_delta > 0:
            regression_flags.append("anomalies_increased")
        if z_delta > 0:
            regression_flags.append("zscore_worsened")
        if str(run_b.get("status", "")).upper() == "BLOCKED" and str(run_a.get("status", "")).upper() != "BLOCKED":
            regression_flags.append("status_regressed_to_blocked")

        return {
            "run_1": {
                "run_id": run_a.get("run_id"),
                "dataset_name": run_a.get("dataset_name"),
                "timestamp": run_a.get("timestamp"),
                "status": run_a.get("status"),
                "quality_score": qa,
                "anomaly_count": aa,
                "z_score_max": za,
            },
            "run_2": {
                "run_id": run_b.get("run_id"),
                "dataset_name": run_b.get("dataset_name"),
                "timestamp": run_b.get("timestamp"),
                "status": run_b.get("status"),
                "quality_score": qb,
                "anomaly_count": ab,
                "z_score_max": zb,
            },
            "delta": {
                "quality_score": quality_delta,
                "anomaly_count": anomaly_delta,
                "z_score_max": z_delta,
                "dimensions": dimension_delta,
            },
            "regression_detected": bool(regression_flags),
            "regression_flags": regression_flags,
        }

    def investigate_anomaly(
        self,
        dataset_name: str,
        *,
        metric: Optional[str] = None,
        run_id: Optional[str] = None,
        history_window: int = 30,
    ) -> Dict[str, Any]:
        """
        Build an investigation packet for anomalous behavior on a dataset.
        """
        selected_run_id: Optional[str] = run_id
        if not selected_run_id:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT run_id
                        FROM run_history
                        WHERE dataset_name = %s
                          AND status IN ('WARNING', 'BLOCKED')
                        ORDER BY timestamp DESC
                        LIMIT 1
                        """,
                        (dataset_name,),
                    )
                    row = cur.fetchone()
                    if row:
                        selected_run_id = row[0]
                    else:
                        cur.execute(
                            """
                            SELECT run_id
                            FROM run_history
                            WHERE dataset_name = %s
                            ORDER BY timestamp DESC
                            LIMIT 1
                            """,
                            (dataset_name,),
                        )
                        row = cur.fetchone()
                        if row:
                            selected_run_id = row[0]

        if not selected_run_id:
            raise HTTPException(status_code=404, detail=f"No run history for dataset {dataset_name}")

        run = self.get_run_verdict(selected_run_id)
        full_verdict = run.get("full_verdict") if isinstance(run.get("full_verdict"), dict) else {}
        anomalies = full_verdict.get("anomalies", []) if isinstance(full_verdict, dict) else []
        if not isinstance(anomalies, list):
            anomalies = []

        metric_filter = str(metric or "").strip()
        selected_anomalies = anomalies
        if metric_filter:
            needle = metric_filter.lower()
            selected_anomalies = [
                item
                for item in anomalies
                if needle in str(item.get("metric_name") or item.get("metric") or "").lower()
            ]

        selected_metric = metric_filter
        if not selected_metric and selected_anomalies:
            selected_metric = str(selected_anomalies[0].get("metric_name") or selected_anomalies[0].get("metric") or "")
        if not selected_metric:
            selected_metric = "row_count"

        metric_history: List[Dict[str, Any]] = []
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT timestamp, metric_value
                    FROM metric_history
                    WHERE dataset_name = %s
                      AND metric_name = %s
                    ORDER BY timestamp DESC
                    LIMIT %s
                    """,
                    (dataset_name, selected_metric, max(5, min(history_window, 200))),
                )
                rows = cur.fetchall()
                metric_history = [
                    {"timestamp": r[0].isoformat() if r[0] else None, "value": self._safe_float(r[1])}
                    for r in rows
                ]

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM incidents
                    WHERE dataset_name = %s
                      AND status IN ('OPEN', 'ACK')
                    """,
                    (dataset_name,),
                )
                incident_row = cur.fetchone()
                open_incidents = int((incident_row or [0])[0] or 0)

        reason = str(run.get("reason") or "")
        schema_evolution = full_verdict.get("schema_evolution") if isinstance(full_verdict, dict) else {}
        infra_hint = ""
        try:
            infra_hint = self.agent._diagnose_root_cause(dataset_name)
        except Exception:
            infra_hint = "Unavailable"

        root_cause_type = "unknown"
        confidence = 0.45
        evidence: List[str] = []

        if "schema violation" in reason.lower():
            root_cause_type = "schema_drift"
            confidence = 0.9
            evidence.append("Run reason indicates explicit schema violation.")
        elif selected_anomalies:
            peak_z = max(abs(self._safe_float(a.get("z_score"))) for a in selected_anomalies)
            if peak_z >= 3.0:
                root_cause_type = "statistical_shift"
                confidence = 0.75
                evidence.append(f"High anomaly z-score detected ({peak_z:.2f}).")
            else:
                root_cause_type = "mild_statistical_shift"
                confidence = 0.6
                evidence.append(f"Anomaly detected with moderate z-score ({peak_z:.2f}).")
        elif str(run.get("status", "")).upper() == "WARNING":
            root_cause_type = "quality_degradation"
            confidence = 0.55
            evidence.append("Run status is WARNING without explicit schema failure.")

        if isinstance(schema_evolution, dict):
            missing_cols = schema_evolution.get("missing_columns") or []
            if missing_cols:
                evidence.append(f"Missing columns detected: {', '.join(map(str, missing_cols[:5]))}")

        if infra_hint and "Local Infrastructure Issue" not in infra_hint:
            evidence.append(f"Infrastructure hint: {infra_hint}")

        root_summary = (
            f"Hypothesis: {root_cause_type}. "
            f"Confidence {confidence:.2f}. "
            f"Primary reason: {reason or 'No explicit reason.'}"
        )

        return {
            "dataset_name": dataset_name,
            "run": {
                "run_id": run.get("run_id"),
                "timestamp": run.get("timestamp"),
                "status": run.get("status"),
                "quality_score": run.get("quality_score"),
                "anomaly_count": run.get("anomaly_count"),
                "z_score_max": run.get("z_score_max"),
                "reason": reason,
            },
            "anomalies": selected_anomalies,
            "metric_focus": selected_metric,
            "metric_history": metric_history,
            "open_incidents": open_incidents,
            "root_cause_hypothesis": {
                "type": root_cause_type,
                "summary": root_summary,
                "confidence": round(confidence, 2),
                "evidence": evidence,
            },
        }

    def explain_quality(self, dataset_name: str, *, run_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Explain quality score composition and top drivers for a dataset run.
        """
        selected_run_id = run_id
        if not selected_run_id:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT run_id
                        FROM run_history
                        WHERE dataset_name = %s
                        ORDER BY timestamp DESC
                        LIMIT 1
                        """,
                        (dataset_name,),
                    )
                    row = cur.fetchone()
                    if row:
                        selected_run_id = row[0]

        if not selected_run_id:
            raise HTTPException(status_code=404, detail=f"No run history for dataset {dataset_name}")

        run = self.get_run_verdict(selected_run_id)
        full_verdict = run.get("full_verdict") if isinstance(run.get("full_verdict"), dict) else {}
        profile = full_verdict.get("profile") if isinstance(full_verdict, dict) else {}
        if not isinstance(profile, dict):
            profile = {}

        dim_map = self._dimension_score_map(run.get("dimension_scores"))
        dim_sorted = sorted(dim_map.items(), key=lambda item: item[1])

        column_scores = profile.get("column_scores")
        weakest_columns: List[Dict[str, Any]] = []
        if isinstance(column_scores, dict):
            ranked = sorted(
                ((str(k), self._safe_float(v, 0.0)) for k, v in column_scores.items()),
                key=lambda item: item[1],
            )
            weakest_columns = [{"column": k, "score": v} for k, v in ranked[:5]]

        violations = profile.get("constraint_violations")
        if not isinstance(violations, list):
            violations = []

        quality_score = self._safe_float(run.get("quality_score"), 0.0)
        band = "GOOD"
        if quality_score < 50:
            band = "CRITICAL"
        elif quality_score < 80:
            band = "WARNING"

        return {
            "dataset_name": dataset_name,
            "run_id": run.get("run_id"),
            "timestamp": run.get("timestamp"),
            "status": run.get("status"),
            "quality_score": quality_score,
            "quality_band": band,
            "dimension_scores": [{"name": name, "score": score} for name, score in dim_sorted],
            "lowest_dimension": {"name": dim_sorted[0][0], "score": dim_sorted[0][1]} if dim_sorted else None,
            "highest_dimension": {"name": dim_sorted[-1][0], "score": dim_sorted[-1][1]} if dim_sorted else None,
            "weakest_columns": weakest_columns,
            "constraint_violations": violations[:20],
            "violation_count": len(violations),
        }

    def list_datasets_by_risk(self, *, limit: int = 20) -> Dict[str, Any]:
        """
        Rank datasets by computed reliability risk score.
        """
        rows: List[Any] = []
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH latest_run AS (
                        SELECT DISTINCT ON (dataset_name)
                               dataset_name, status, quality_score, anomaly_count, z_score_max, timestamp, reason
                        FROM run_history
                        ORDER BY dataset_name, timestamp DESC
                    ),
                    open_incidents AS (
                        SELECT dataset_name, COUNT(*) AS open_count
                        FROM incidents
                        WHERE status IN ('OPEN', 'ACK')
                        GROUP BY dataset_name
                    )
                    SELECT
                        dr.dataset_name,
                        dr.criticality,
                        dr.lifecycle,
                        COALESCE(lr.status, dr.last_status, 'UNKNOWN') AS run_status,
                        COALESCE(lr.quality_score, 0.0) AS quality_score,
                        COALESCE(lr.anomaly_count, 0) AS anomaly_count,
                        COALESCE(lr.z_score_max, 0.0) AS z_score_max,
                        COALESCE(oi.open_count, 0) AS open_incidents,
                        lr.timestamp,
                        COALESCE(lr.reason, '') AS reason
                    FROM dataset_registry dr
                    LEFT JOIN latest_run lr ON lr.dataset_name = dr.dataset_name
                    LEFT JOIN open_incidents oi ON oi.dataset_name = dr.dataset_name
                    ORDER BY dr.dataset_name
                    """
                )
                rows = cur.fetchall() or []

        ranked: List[Dict[str, Any]] = []
        for row in rows:
            dataset_name = row[0]
            criticality = str(row[1] or "UNKNOWN")
            lifecycle = str(row[2] or "unknown")
            status = str(row[3] or "UNKNOWN")
            quality_score = self._safe_float(row[4], 0.0)
            anomaly_count = self._safe_int(row[5], 0)
            z_score_max = self._safe_float(row[6], 0.0)
            open_incidents = self._safe_int(row[7], 0)
            timestamp = row[8].isoformat() if row[8] else None
            reason = str(row[9] or "")

            risk_score = self._compute_risk_score(
                status=status,
                quality_score=quality_score,
                anomaly_count=anomaly_count,
                criticality=criticality,
                open_incidents=open_incidents,
            )
            ranked.append(
                {
                    "dataset_name": dataset_name,
                    "risk_score": risk_score,
                    "criticality": criticality,
                    "lifecycle": lifecycle,
                    "status": status,
                    "quality_score": quality_score,
                    "anomaly_count": anomaly_count,
                    "z_score_max": z_score_max,
                    "open_incidents": open_incidents,
                    "last_run_at": timestamp,
                    "reason": reason,
                }
            )

        ranked.sort(key=lambda item: item["risk_score"], reverse=True)
        safe_limit = max(1, min(limit, 200))
        return {
            "limit": safe_limit,
            "datasets": ranked[:safe_limit],
            "total_ranked": len(ranked),
        }

    def get_outcome_metrics(self, *, days: int = 30) -> Dict[str, Any]:
        """
        Product outcome metrics for reliability effectiveness over a time window.
        """
        safe_days = max(1, min(int(days), 365))
        metrics: Dict[str, Any] = {
            "window_days": safe_days,
            "generated_at": datetime.now().isoformat(),
        }

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS total_runs,
                        COUNT(*) FILTER (WHERE status = 'PASSED') AS passed_runs,
                        COUNT(*) FILTER (WHERE status = 'WARNING') AS warning_runs,
                        COUNT(*) FILTER (WHERE status = 'BLOCKED') AS blocked_runs,
                        AVG(quality_score) AS avg_quality_score
                    FROM run_history
                    WHERE timestamp >= NOW() - (%s * INTERVAL '1 day')
                    """,
                    (safe_days,),
                )
                run_row = cur.fetchone() or [0, 0, 0, 0, None]
                total_runs = self._safe_int(run_row[0], 0)
                passed_runs = self._safe_int(run_row[1], 0)
                warning_runs = self._safe_int(run_row[2], 0)
                blocked_runs = self._safe_int(run_row[3], 0)
                avg_quality_score = self._safe_float(run_row[4], 0.0) if run_row[4] is not None else None

                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS incidents_created,
                        COUNT(*) FILTER (WHERE status = 'RESOLVED') AS incidents_resolved,
                        COUNT(*) FILTER (WHERE status IN ('OPEN', 'ACK')) AS incidents_open
                    FROM incidents
                    WHERE created_at >= NOW() - (%s * INTERVAL '1 day')
                    """,
                    (safe_days,),
                )
                incident_row = cur.fetchone() or [0, 0, 0]
                incidents_created = self._safe_int(incident_row[0], 0)
                incidents_resolved = self._safe_int(incident_row[1], 0)
                incidents_open = self._safe_int(incident_row[2], 0)

                cur.execute(
                    """
                    SELECT AVG(EXTRACT(EPOCH FROM (resolved_at - created_at)) / 60.0)
                    FROM incidents
                    WHERE resolved_at IS NOT NULL
                      AND created_at >= NOW() - (%s * INTERVAL '1 day')
                    """,
                    (safe_days,),
                )
                mttr_row = cur.fetchone()
                mttr_minutes = self._safe_float(mttr_row[0], 0.0) if mttr_row and mttr_row[0] is not None else None

                cur.execute(
                    """
                    SELECT COUNT(DISTINCT dataset_name)
                    FROM run_history
                    WHERE timestamp >= NOW() - (%s * INTERVAL '1 day')
                    """,
                    (safe_days,),
                )
                datasets_scanned = self._safe_int((cur.fetchone() or [0])[0], 0)

        pass_rate = (passed_runs / total_runs * 100.0) if total_runs else None
        blocked_rate = (blocked_runs / total_runs * 100.0) if total_runs else None
        warning_rate = (warning_runs / total_runs * 100.0) if total_runs else None
        incident_resolution_rate = (incidents_resolved / incidents_created * 100.0) if incidents_created else None

        metrics.update(
            {
                "runs": {
                    "total": total_runs,
                    "passed": passed_runs,
                    "warning": warning_runs,
                    "blocked": blocked_runs,
                    "pass_rate_pct": round(pass_rate, 2) if pass_rate is not None else None,
                    "warning_rate_pct": round(warning_rate, 2) if warning_rate is not None else None,
                    "blocked_rate_pct": round(blocked_rate, 2) if blocked_rate is not None else None,
                    "avg_quality_score": round(avg_quality_score, 2) if avg_quality_score is not None else None,
                },
                "incidents": {
                    "created": incidents_created,
                    "resolved": incidents_resolved,
                    "open": incidents_open,
                    "resolution_rate_pct": round(incident_resolution_rate, 2)
                    if incident_resolution_rate is not None
                    else None,
                    "mttr_minutes_avg": round(mttr_minutes, 2) if mttr_minutes is not None else None,
                },
                "coverage": {
                    "datasets_scanned": datasets_scanned,
                },
            }
        )
        return metrics

    def generate_ai_brief(self, dataset_name: str, *, run_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Build a deterministic + AI summary for a dataset's current reliability posture.
        """
        investigation: Dict[str, Any] = {}
        quality: Dict[str, Any] = {}
        impact: Dict[str, Any] = {}
        remediation: Dict[str, Any] = {}
        slo_summary: Dict[str, Any] = {}
        risk_item: Dict[str, Any] = {}

        def capture(fn, fallback: Dict[str, Any]) -> Dict[str, Any]:
            try:
                result = fn()
                return result if isinstance(result, dict) else fallback
            except Exception as exc:
                out = dict(fallback)
                out["error"] = str(exc)
                return out

        investigation = capture(
            lambda: self.investigate_anomaly(dataset_name, run_id=run_id),
            {"status": "unavailable"},
        )
        quality = capture(
            lambda: self.explain_quality(dataset_name, run_id=run_id),
            {"status": "unavailable"},
        )
        remediation = capture(
            lambda: self.get_remediation_plan(dataset_name),
            {"status": "unavailable"},
        )
        impact = capture(
            lambda: self.agent.impact_analyzer.get_downstream_impact(dataset_name),
            {"dataset": dataset_name, "overall_criticality": "UNKNOWN", "impacted_consumers": []},
        )
        slo_summary = capture(
            lambda: self.get_slo_summary(dataset_name, window=200),
            {"overall_status": "UNKNOWN", "checks": []},
        )

        try:
            ranked = self.list_datasets_by_risk(limit=200).get("datasets", [])
            if isinstance(ranked, list):
                risk_item = next(
                    (item for item in ranked if str(item.get("dataset_name") or "") == dataset_name),
                    {},
                )
        except Exception:
            risk_item = {}

        deterministic_actions: List[str] = []
        run_status = str((investigation.get("run") or {}).get("status") or "").upper()
        if run_status == "BLOCKED":
            deterministic_actions.append("Treat as active incident and stop downstream publish until resolved.")
        elif run_status == "WARNING":
            deterministic_actions.append("Allow with guardrails; monitor next run and investigate root-cause evidence.")
        else:
            deterministic_actions.append("Dataset currently stable; continue scheduled monitoring.")

        root = investigation.get("root_cause_hypothesis") or {}
        if isinstance(root, dict) and root.get("type"):
            deterministic_actions.append(f"Validate root-cause hypothesis: {root.get('type')}.")

        if str(remediation.get("status") or "") == "remediation_available":
            deterministic_actions.append("Review remediation proposal with policy/HITL controls before apply.")

        impacted = impact.get("impacted_consumers")
        if isinstance(impacted, list) and impacted:
            deterministic_actions.append("Notify downstream owners listed in lineage impact context.")

        llm_summary = ""
        llm_available = False
        try:
            model = getattr(getattr(self.agent, "reasoning_agent", None), "model", None)
            llm_available = model is not None
        except Exception:
            llm_available = False

        if llm_available:
            prompt_payload = {
                "dataset_name": dataset_name,
                "run": investigation.get("run"),
                "root_cause_hypothesis": root,
                "quality": {
                    "quality_score": quality.get("quality_score"),
                    "quality_band": quality.get("quality_band"),
                    "lowest_dimension": quality.get("lowest_dimension"),
                    "weakest_columns": quality.get("weakest_columns"),
                },
                "impact": {
                    "overall_criticality": impact.get("overall_criticality"),
                    "impacted_consumers": impact.get("impacted_consumers"),
                },
                "risk": risk_item,
                "slo": {
                    "overall_status": slo_summary.get("overall_status"),
                    "overall_pass_rate": slo_summary.get("overall_pass_rate"),
                    "failing_slos": slo_summary.get("failing_slos"),
                },
                "remediation_status": remediation.get("status"),
                "deterministic_actions": deterministic_actions,
            }
            prompt = (
                "You are a data reliability copilot. "
                "Write a concise incident-style brief with:\n"
                "1) Current status\n2) Most likely root cause\n3) Business impact\n"
                "4) Immediate next actions (3 bullets max).\n"
                "Be concrete and avoid generic language.\n\n"
                f"Context JSON:\n{json.dumps(prompt_payload, default=str)}"
            )
            try:
                response = self.agent.reasoning_agent.run(prompt)
                llm_summary = str(getattr(response, "content", "") or "").strip()
            except Exception as exc:
                llm_summary = f"LLM summary unavailable: {exc}"
        else:
            llm_summary = "LLM summary unavailable in current runtime."

        return {
            "dataset_name": dataset_name,
            "generated_at": datetime.now().isoformat(),
            "run_id": run_id or (investigation.get("run") or {}).get("run_id"),
            "risk": risk_item,
            "investigation": investigation,
            "quality": quality,
            "impact": impact,
            "slo": slo_summary,
            "remediation": remediation,
            "deterministic_actions": deterministic_actions,
            "ai_summary": llm_summary,
        }

    def get_workflow_timeline(
        self,
        *,
        dataset_name: Optional[str] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """
        Unified workflow visibility payload across runs, tools, jobs, incidents, and audit logs.
        """
        safe_limit = max(10, min(int(limit), 500))
        dataset_filter = str(dataset_name or "").strip() or None

        combined: List[Dict[str, Any]] = []

        def add_event(
            *,
            event_id: str,
            timestamp: Any,
            channel: str,
            event: str,
            status: Optional[str],
            dataset: Optional[str],
            message: str,
            details: Optional[Dict[str, Any]] = None,
            refs: Optional[Dict[str, Any]] = None,
        ) -> None:
            iso = timestamp.isoformat() if hasattr(timestamp, "isoformat") else None
            if hasattr(timestamp, "timestamp"):
                try:
                    sort_ts = float(timestamp.timestamp())
                except Exception:
                    sort_ts = 0.0
            else:
                sort_ts = 0.0
            combined.append(
                {
                    "event_id": event_id,
                    "timestamp": iso,
                    "channel": channel,
                    "event": event,
                    "status": status,
                    "dataset_name": dataset,
                    "message": message,
                    "details": details or {},
                    "refs": refs or {},
                    "_sort_ts": sort_ts,
                }
            )

        with get_connection() as conn:
            with conn.cursor() as cur:
                audit_params: List[Any] = [safe_limit]
                audit_where = ""
                if dataset_filter:
                    audit_where = "WHERE dataset_name = %s"
                    audit_params.insert(0, dataset_filter)
                cur.execute(
                    f"""
                    SELECT id, timestamp, action, status, dataset_name, source, actor, metadata
                    FROM action_audit_log
                    {audit_where}
                    ORDER BY timestamp DESC
                    LIMIT %s
                    """,
                    tuple(audit_params),
                )
                for row in cur.fetchall() or []:
                    metadata = self._normalize_json(row[7])
                    refs = {
                        "job_id": metadata.get("job_id") if isinstance(metadata, dict) else None,
                        "run_id": metadata.get("run_id") if isinstance(metadata, dict) else None,
                        "incident_id": metadata.get("incident_id") if isinstance(metadata, dict) else None,
                    }
                    add_event(
                        event_id=f"audit:{row[0]}",
                        timestamp=row[1],
                        channel="audit",
                        event=str(row[2] or "audit_event"),
                        status=row[3],
                        dataset=row[4],
                        message=f"{row[2]} ({row[3] or 'N/A'})",
                        details={
                            "source": row[5],
                            "actor": row[6],
                            "metadata": metadata if isinstance(metadata, dict) else {},
                        },
                        refs=refs,
                    )

                job_params: List[Any] = [safe_limit]
                job_where = ""
                if dataset_filter:
                    job_where = "WHERE dataset_name = %s"
                    job_params.insert(0, dataset_filter)
                cur.execute(
                    f"""
                    SELECT job_id, action, dataset_name, status,
                           requested_at, started_at, finished_at,
                           request_json, result_json, error_text
                    FROM async_jobs
                    {job_where}
                    ORDER BY requested_at DESC
                    LIMIT %s
                    """,
                    tuple(job_params),
                )
                for row in cur.fetchall() or []:
                    ts = row[6] or row[5] or row[4]
                    details: Dict[str, Any] = {
                        "action": row[1],
                        "requested_at": row[4].isoformat() if row[4] else None,
                        "started_at": row[5].isoformat() if row[5] else None,
                        "finished_at": row[6].isoformat() if row[6] else None,
                        "request": self._normalize_json(row[7]) if row[7] is not None else {},
                    }
                    result_payload = self._normalize_json(row[8]) if row[8] is not None else None
                    if result_payload is not None:
                        details["result"] = result_payload
                    if row[9]:
                        details["error"] = str(row[9])

                    add_event(
                        event_id=f"job:{row[0]}",
                        timestamp=ts,
                        channel="job",
                        event=f"job_{row[1]}",
                        status=row[3],
                        dataset=row[2],
                        message=f"{row[1]} job {str(row[3] or '').lower()}",
                        details=details,
                        refs={"job_id": row[0]},
                    )

                run_params: List[Any] = [safe_limit]
                run_where = ""
                if dataset_filter:
                    run_where = "WHERE dataset_name = %s"
                    run_params.insert(0, dataset_filter)
                cur.execute(
                    f"""
                    SELECT run_id, timestamp, dataset_name, status,
                           quality_score, anomaly_count, z_score_max, reason, duration_ms
                    FROM run_history
                    {run_where}
                    ORDER BY timestamp DESC
                    LIMIT %s
                    """,
                    tuple(run_params),
                )
                for row in cur.fetchall() or []:
                    add_event(
                        event_id=f"run:{row[0]}",
                        timestamp=row[1],
                        channel="run",
                        event="dataset_evaluation",
                        status=row[3],
                        dataset=row[2],
                        message=f"Run {row[3]} (quality={self._safe_float(row[4], 0.0):.1f})",
                        details={
                            "quality_score": self._safe_float(row[4], 0.0),
                            "anomaly_count": self._safe_int(row[5], 0),
                            "z_score_max": self._safe_float(row[6], 0.0),
                            "reason": row[7],
                            "duration_ms": self._safe_int(row[8], 0),
                        },
                        refs={"run_id": row[0]},
                    )

                tool_params: List[Any] = [safe_limit]
                tool_where = ""
                if dataset_filter:
                    tool_where = "WHERE dataset_name = %s"
                    tool_params.insert(0, dataset_filter)
                cur.execute(
                    f"""
                    SELECT id, timestamp, dataset_name, run_id, tool_name, status, duration_ms, output
                    FROM tool_outputs
                    {tool_where}
                    ORDER BY timestamp DESC
                    LIMIT %s
                    """,
                    tuple(tool_params),
                )
                for row in cur.fetchall() or []:
                    add_event(
                        event_id=f"tool:{row[0]}",
                        timestamp=row[1],
                        channel="tool",
                        event=str(row[4] or "tool"),
                        status=row[5],
                        dataset=row[2],
                        message=f"Tool {row[4]} {str(row[5] or '').lower()}",
                        details={
                            "duration_ms": self._safe_int(row[6], 0),
                            "output": self._normalize_json(row[7]),
                        },
                        refs={"run_id": row[3]},
                    )

                incident_params: List[Any] = [safe_limit]
                incident_where = ""
                if dataset_filter:
                    incident_where = "WHERE dataset_name = %s"
                    incident_params.insert(0, dataset_filter)
                cur.execute(
                    f"""
                    SELECT incident_id,
                           COALESCE(updated_at, created_at) AS event_ts,
                           dataset_name,
                           severity,
                           status,
                           owner,
                           title,
                           run_id,
                           metadata
                    FROM incidents
                    {incident_where}
                    ORDER BY event_ts DESC
                    LIMIT %s
                    """,
                    tuple(incident_params),
                )
                for row in cur.fetchall() or []:
                    add_event(
                        event_id=f"incident:{row[0]}",
                        timestamp=row[1],
                        channel="incident",
                        event="incident_lifecycle",
                        status=row[4],
                        dataset=row[2],
                        message=f"Incident {row[4]} ({row[3]})",
                        details={
                            "severity": row[3],
                            "owner": row[5],
                            "title": row[6],
                            "metadata": self._normalize_json(row[8]),
                        },
                        refs={"incident_id": row[0], "run_id": row[7]},
                    )

        combined.sort(key=lambda item: float(item.get("_sort_ts") or 0.0), reverse=True)
        trimmed = combined[:safe_limit]

        channel_counts: Dict[str, int] = {}
        status_counts: Dict[str, int] = {}
        running_jobs = 0
        active_incidents = 0
        for item in trimmed:
            channel = str(item.get("channel") or "unknown")
            channel_counts[channel] = channel_counts.get(channel, 0) + 1

            status_value = str(item.get("status") or "").upper()
            if status_value:
                status_counts[status_value] = status_counts.get(status_value, 0) + 1
                if channel == "job" and status_value in {"QUEUED", "RUNNING"}:
                    running_jobs += 1
                if channel == "incident" and status_value in {"OPEN", "ACK"}:
                    active_incidents += 1

            item.pop("_sort_ts", None)

        return {
            "generated_at": datetime.now().isoformat(),
            "dataset_name": dataset_filter,
            "limit": safe_limit,
            "events": trimmed,
            "summary": {
                "total_events": len(trimmed),
                "channels": channel_counts,
                "statuses": status_counts,
                "active_jobs": running_jobs,
                "active_incidents": active_incidents,
            },
            "runtime": {
                "langgraph_hitl_enabled": self.hitl_workflow is not None,
                "langgraph_agentic_enabled": self.agentic_workflow is not None,
            },
        }

    def get_agentic_workflow_graph(self) -> Dict[str, Any]:
        if self.agentic_workflow is None or not hasattr(self.agentic_workflow, "mermaid"):
            raise HTTPException(status_code=503, detail="Agentic LangGraph workflow runtime is unavailable.")
        return {
            "engine": "langgraph",
            "mermaid": str(self.agentic_workflow.mermaid()),
        }

    def run_agentic_reliability_loop(
        self,
        *,
        dataset_name: str,
        metric: Optional[str] = None,
        auto_execute: bool = False,
        confidence_threshold: float = 0.8,
        policy_approved: bool = False,
        policy_reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Agentic loop:
        investigate -> root-cause hypothesis -> remediation proposal -> confidence/policy-gated execution.
        """
        if self.agentic_workflow is not None:
            try:
                return self.agentic_workflow.run(
                    dataset_name=dataset_name,
                    metric=metric,
                    auto_execute=auto_execute,
                    confidence_threshold=confidence_threshold,
                    policy_approved=policy_approved,
                    policy_reason=policy_reason,
                )
            except Exception:
                # Keep legacy fallback behavior if workflow runtime errors.
                pass

        return self._run_agentic_reliability_loop_legacy(
            dataset_name=dataset_name,
            metric=metric,
            auto_execute=auto_execute,
            confidence_threshold=confidence_threshold,
            policy_approved=policy_approved,
            policy_reason=policy_reason,
        )

    @staticmethod
    def _extract_json_object(raw: Any) -> Dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        if raw is None:
            raise ValueError("Empty AI response.")

        text = str(raw).strip()
        if not text:
            raise ValueError("Empty AI response.")

        if text.startswith("```"):
            lines = text.splitlines()
            if len(lines) >= 3:
                text = "\n".join(lines[1:-1]).strip()

        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            candidate = text[start : end + 1]
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed

        raise ValueError("AI response is not a valid JSON object.")

    @staticmethod
    def _extract_status_and_run_id(payload: Dict[str, Any]) -> Dict[str, Optional[str]]:
        status = None
        run_id = None
        if not isinstance(payload, dict):
            return {"status": status, "run_id": run_id}

        status = str(payload.get("status") or payload.get("mode") or "").strip() or None
        run_id = str(payload.get("run_id") or "").strip() or None

        verdict = payload.get("verdict")
        if isinstance(verdict, dict):
            if not run_id:
                run_id = str(verdict.get("run_id") or "").strip() or None
            if not status:
                status = str(verdict.get("status") or "").strip() or None

        return {"status": status, "run_id": run_id}

    @staticmethod
    def _format_plan(
        *,
        classification: str,
        why_stopped: str,
        reason: str,
        tool_outputs: List[Dict[str, Any]],
        run_id: Optional[str],
    ) -> Dict[str, Any]:
        classification_owner = {
            "schema_mismatch": "data_producer",
            "constraint_violation": "data_producer",
            "anomaly_only": "dre_operator",
            "load_failure": "platform",
            "platform_failure": "platform",
            "unknown": "dre_operator",
        }
        owner_hint = classification_owner.get(classification, "dre_operator")
        evidence_refs = [f"run_history:{run_id}"] if run_id else []
        for item in tool_outputs[:5]:
            tool_name = str(item.get("tool_name") or "tool")
            event_id = item.get("id")
            if event_id is not None:
                evidence_refs.append(f"tool_outputs:{event_id}:{tool_name}")
            else:
                evidence_refs.append(f"tool_outputs:{tool_name}")

        recommended_actions: List[str] = []
        if classification == "schema_mismatch":
            recommended_actions = [
                "Review upstream schema changes and align contract columns/types.",
                "Validate backward compatibility for downstream consumers before promoting the patch.",
                "Re-run contract gate in CI before next load.",
            ]
        elif classification == "constraint_violation":
            recommended_actions = [
                "Inspect failing constraints and identify whether producer logic or business rules changed.",
                "Tighten or relax constraints with explicit change rationale in contract history.",
                "Replay the batch after contract update approval.",
            ]
        elif classification == "load_failure":
            recommended_actions = [
                "Inspect Doris/load connector error logs and field mappings.",
                "Fix loader mapping or sink permissions and re-run ingestion.",
                "Keep contract unchanged until load path is healthy.",
            ]
        elif classification == "platform_failure":
            recommended_actions = [
                "Check upstream platform/network/auth health before retrying.",
                "Restore source availability and credentials.",
                "Re-trigger evaluation after platform incident is resolved.",
            ]
        elif classification == "anomaly_only":
            recommended_actions = [
                "Compare recent metric history and confirm whether behavior is expected.",
                "Tune anomaly thresholds only with human review and business context.",
                "Monitor next runs for persistence before editing contract.",
            ]
        else:
            recommended_actions = [
                "Inspect run/tool evidence to determine deterministic root cause.",
                "Apply manual remediation and re-run validation.",
                "Escalate to DRE operator if failure repeats.",
            ]

        return {
            "root_cause_hypothesis": f"{classification}: {reason or 'No explicit reason available.'}",
            "why_auto_fix_stopped": why_stopped,
            "recommended_actions": recommended_actions,
            "owner_hint": owner_hint,
            "evidence": evidence_refs,
            "rollback_advice": "Restore previous contract from config/history if a manual patch introduces regressions.",
        }

    def _latest_run_record(self, dataset_name: str, *, statuses: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        where_parts = ["dataset_name = %s"]
        params: List[Any] = [dataset_name]
        if statuses:
            normalized = [str(s).upper() for s in statuses if str(s).strip()]
            if normalized:
                where_parts.append("status = ANY(%s)")
                params.append(normalized)

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT run_id, dataset_name, timestamp, status, reason, quality_score,
                           anomaly_count, z_score_max, full_verdict
                    FROM run_history
                    WHERE {" AND ".join(where_parts)}
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    tuple(params),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return {
                    "run_id": row[0],
                    "dataset_name": row[1],
                    "timestamp": row[2].isoformat() if row[2] else None,
                    "status": row[3],
                    "reason": row[4],
                    "quality_score": self._safe_float(row[5], 0.0),
                    "anomaly_count": self._safe_int(row[6], 0),
                    "z_score_max": self._safe_float(row[7], 0.0),
                    "full_verdict": self._normalize_json(row[8]) if row[8] is not None else {},
                }

    def _recent_run_records(self, dataset_name: str, *, limit: int = 5) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 50))
        rows: List[Any] = []
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, timestamp, status, reason, quality_score, anomaly_count, z_score_max
                    FROM run_history
                    WHERE dataset_name = %s
                    ORDER BY timestamp DESC
                    LIMIT %s
                    """,
                    (dataset_name, safe_limit),
                )
                rows = cur.fetchall() or []
        return [
            {
                "run_id": row[0],
                "timestamp": row[1].isoformat() if row[1] else None,
                "status": row[2],
                "reason": row[3],
                "quality_score": self._safe_float(row[4], 0.0),
                "anomaly_count": self._safe_int(row[5], 0),
                "z_score_max": self._safe_float(row[6], 0.0),
            }
            for row in rows
        ]

    def _tool_outputs_for_run(self, run_id: Optional[str]) -> List[Dict[str, Any]]:
        if not run_id:
            return []
        rows: List[Any] = []
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, timestamp, run_id, tool_name, status, duration_ms, output
                    FROM tool_outputs
                    WHERE run_id = %s
                    ORDER BY timestamp ASC
                    """,
                    (run_id,),
                )
                rows = cur.fetchall() or []
        outputs: List[Dict[str, Any]] = []
        for row in rows:
            outputs.append(
                {
                    "id": row[0],
                    "timestamp": row[1].isoformat() if row[1] else None,
                    "run_id": row[2],
                    "tool_name": row[3],
                    "status": row[4],
                    "duration_ms": self._safe_int(row[5], 0),
                    "output": self._normalize_json(row[6]),
                }
            )
        return outputs

    def _classify_failure(
        self,
        *,
        run: Dict[str, Any],
        tool_outputs: List[Dict[str, Any]],
        recent_runs: List[Dict[str, Any]],
    ) -> Dict[str, str]:
        reason = str(run.get("reason") or "")
        reason_l = reason.lower()
        status = str(run.get("status") or "").upper()
        full_verdict = run.get("full_verdict") if isinstance(run.get("full_verdict"), dict) else {}

        schema_payload = full_verdict.get("schema_validation") if isinstance(full_verdict, dict) else {}
        if isinstance(schema_payload, dict):
            schema_status = str(schema_payload.get("status") or "").lower()
            diff = schema_payload.get("schema_diff") if isinstance(schema_payload.get("schema_diff"), dict) else {}
            if schema_status == "fail":
                return {"classification": "schema_mismatch", "reason": "Schema validator reported fail status."}
            if any(diff.get(key) for key in ("missing_columns", "new_columns", "type_mismatches")):
                return {"classification": "schema_mismatch", "reason": "Schema diff indicates column/type mismatch."}

        profile_payload = full_verdict.get("profile") if isinstance(full_verdict, dict) else {}
        if isinstance(profile_payload, dict):
            if profile_payload.get("constraint_violations"):
                return {"classification": "constraint_violation", "reason": "Constraint violations were recorded in profiling."}
            custom_checks = profile_payload.get("custom_check_results")
            if isinstance(custom_checks, list):
                failed_custom = [c for c in custom_checks if isinstance(c, dict) and not bool(c.get("passed", True))]
                if failed_custom:
                    return {"classification": "constraint_violation", "reason": "Custom quality checks failed."}

        tool_text = " ".join(
            " ".join(
                [
                    str(item.get("tool_name") or ""),
                    str(item.get("status") or ""),
                    str(item.get("output") or ""),
                ]
            )
            for item in tool_outputs
        ).lower()
        if any(token in tool_text for token in ("doris", "stream load", "load failed", "load_error", "warehouse")):
            return {"classification": "load_failure", "reason": "Warehouse/load stage failure detected in tool outputs."}
        if any(
            token in tool_text
            for token in (
                "timeout",
                "connection refused",
                "network",
                "authentication",
                "permission denied",
                "service unavailable",
                "platform",
            )
        ):
            return {"classification": "platform_failure", "reason": "Platform/network/auth failure detected in tool outputs."}

        if any(
            token in reason_l
            for token in ("schema", "column", "type mismatch", "missing column", "unexpected column", "contract mismatch")
        ):
            return {"classification": "schema_mismatch", "reason": reason or "Schema mismatch in run reason."}
        if any(
            token in reason_l
            for token in (
                "constraint",
                "duplicate",
                "null",
                "missing value",
                "invalid value",
                "custom check",
                "quality check",
            )
        ):
            return {"classification": "constraint_violation", "reason": reason or "Constraint violation in run reason."}
        if any(token in reason_l for token in ("doris", "load", "sink", "warehouse")):
            return {"classification": "load_failure", "reason": reason or "Load failure in run reason."}
        if any(
            token in reason_l
            for token in ("timeout", "network", "upstream", "service unavailable", "connection", "auth")
        ):
            return {"classification": "platform_failure", "reason": reason or "Platform failure in run reason."}
        if any(token in reason_l for token in ("anomaly", "z-score", "drift", "seasonal")):
            return {"classification": "anomaly_only", "reason": reason or "Anomaly-only signal in run reason."}

        if status in {"WARNING", "BLOCKED"} and self._safe_int(run.get("anomaly_count"), 0) > 0:
            return {"classification": "anomaly_only", "reason": "Run is warning/blocked with anomaly_count > 0."}

        # Repeated failures with non-specific reason are treated as unknown non-fixable.
        latest_statuses = [str(item.get("status") or "").upper() for item in recent_runs[:3]]
        if latest_statuses and all(s in {"BLOCKED", "WARNING"} for s in latest_statuses):
            return {"classification": "unknown", "reason": "Repeated failing runs without deterministic class."}

        return {"classification": "unknown", "reason": reason or "Unable to classify failure deterministically."}

    @staticmethod
    def _is_fixable_classification(classification: str) -> bool:
        return classification in {"schema_mismatch", "constraint_violation", "load_failure"}

    def _persist_agentic_remediation_run(
        self,
        *,
        remediation_run_id: str,
        dataset_name: str,
        initial_run_id: Optional[str],
        final_run_id: Optional[str],
        status: str,
        attempt_count: int,
        policy_blocks: int,
        summary: Dict[str, Any],
    ) -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO agentic_remediation_runs (
                        id, dataset_name, initial_run_id, final_run_id, status,
                        attempt_count, policy_blocks, summary, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        final_run_id = EXCLUDED.final_run_id,
                        status = EXCLUDED.status,
                        attempt_count = EXCLUDED.attempt_count,
                        policy_blocks = EXCLUDED.policy_blocks,
                        summary = EXCLUDED.summary,
                        updated_at = NOW()
                    """,
                    (
                        remediation_run_id,
                        dataset_name,
                        initial_run_id,
                        final_run_id,
                        status,
                        int(attempt_count),
                        int(policy_blocks),
                        json.dumps(summary or {}, default=str),
                    ),
                )

    def _persist_agentic_remediation_attempt(
        self,
        *,
        remediation_run_id: str,
        attempt_no: int,
        input_run_id: Optional[str],
        classification: str,
        proposed_diff_summary: Optional[str],
        confidence: Optional[float],
        applied: bool,
        output_run_id: Optional[str],
        result_status: str,
        error: Optional[str],
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO agentic_remediation_attempts (
                        remediation_run_id, attempt_no, input_run_id, classification,
                        proposed_diff_summary, confidence, applied, output_run_id,
                        result_status, error, details, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
                    """,
                    (
                        remediation_run_id,
                        int(attempt_no),
                        input_run_id,
                        classification,
                        proposed_diff_summary,
                        confidence,
                        bool(applied),
                        output_run_id,
                        result_status,
                        error,
                        json.dumps(details or {}, default=str),
                    ),
                )

    def _build_contract_patch_with_ai(
        self,
        *,
        dataset_name: str,
        current_yaml: str,
        classification: str,
        classification_reason: str,
        run: Dict[str, Any],
        tool_outputs: List[Dict[str, Any]],
        recent_runs: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if not getattr(self.agent, "reasoning_agent", None):
            raise ValueError("Reasoning agent is unavailable.")

        evidence_payload = {
            "dataset_name": dataset_name,
            "classification": classification,
            "classification_reason": classification_reason,
            "run": {
                "run_id": run.get("run_id"),
                "status": run.get("status"),
                "reason": run.get("reason"),
                "quality_score": run.get("quality_score"),
                "anomaly_count": run.get("anomaly_count"),
            },
            "tool_outputs": tool_outputs[:20],
            "recent_runs": recent_runs[:5],
        }

        prompt = (
            "You are a deterministic data-contract repair assistant.\n"
            "Task: propose ONLY a contract YAML modification to remediate the failure.\n"
            "Rules:\n"
            "1) Return ONLY valid JSON object.\n"
            "2) Required keys: modified_yaml, change_summary, risk_level, confidence, expected_effect.\n"
            "3) confidence must be 0.0-1.0.\n"
            "4) risk_level must be one of low|medium|high.\n"
            "5) Keep contract/table identity unchanged. Do not rename dataset/table.\n"
            "6) Do not add markdown fences or extra prose.\n\n"
            f"Current contract YAML:\n{current_yaml}\n\n"
            f"Evidence JSON:\n{json.dumps(evidence_payload, default=str)}\n"
        )
        response = self.agent.reasoning_agent.run(prompt)
        raw_content = getattr(response, "content", response)
        parsed = self._extract_json_object(raw_content)

        modified_yaml = str(parsed.get("modified_yaml") or "").strip()
        if not modified_yaml:
            raise ValueError("AI output missing modified_yaml.")

        risk_level = str(parsed.get("risk_level") or "").strip().lower()
        if risk_level not in {"low", "medium", "high"}:
            raise ValueError("AI output risk_level must be low|medium|high.")

        confidence = self._safe_float(parsed.get("confidence"), -1.0)
        if confidence < 0.0 or confidence > 1.0:
            raise ValueError("AI output confidence must be in [0,1].")

        current_doc = yaml.safe_load(current_yaml) or {}
        modified_doc = yaml.safe_load(modified_yaml) or {}
        if not isinstance(current_doc, dict) or not isinstance(modified_doc, dict):
            raise ValueError("Contract YAML must deserialize into an object.")

        for key in ("kind", "apiVersion", "columns"):
            if key in current_doc and key not in modified_doc:
                raise ValueError(f"Modified contract removed required key: {key}")

        current_table = str(current_doc.get("table_name") or "").strip()
        modified_table = str(modified_doc.get("table_name") or "").strip()
        if current_table and modified_table and current_table != modified_table:
            raise ValueError("table_name cannot be changed by auto-remediation.")

        current_id = str(current_doc.get("id") or "").strip()
        modified_id = str(modified_doc.get("id") or "").strip()
        if current_id and modified_id and current_id != modified_id:
            raise ValueError("id cannot be changed by auto-remediation.")

        if not isinstance(modified_doc.get("columns"), list):
            raise ValueError("Modified contract must include a valid columns list.")

        return {
            "modified_yaml": yaml.safe_dump(modified_doc, sort_keys=False),
            "change_summary": str(parsed.get("change_summary") or "Auto-remediation contract update"),
            "risk_level": risk_level,
            "confidence": round(confidence, 4),
            "expected_effect": str(parsed.get("expected_effect") or "").strip(),
        }

    def get_agentic_remediation_run(self, remediation_run_id: str) -> Dict[str, Any]:
        run_row = None
        attempt_rows: List[Any] = []
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, dataset_name, initial_run_id, final_run_id, status,
                           attempt_count, policy_blocks, summary, created_at, updated_at
                    FROM agentic_remediation_runs
                    WHERE id = %s
                    """,
                    (remediation_run_id,),
                )
                run_row = cur.fetchone()
                if not run_row:
                    raise HTTPException(status_code=404, detail=f"Remediation run {remediation_run_id} not found")

                cur.execute(
                    """
                    SELECT attempt_no, input_run_id, classification, proposed_diff_summary, confidence,
                           applied, output_run_id, result_status, error, details, created_at
                    FROM agentic_remediation_attempts
                    WHERE remediation_run_id = %s
                    ORDER BY attempt_no ASC
                    """,
                    (remediation_run_id,),
                )
                attempt_rows = cur.fetchall() or []

        summary = self._normalize_json(run_row[7]) if run_row[7] is not None else {}
        if not isinstance(summary, dict):
            summary = {}

        attempts: List[Dict[str, Any]] = []
        timeline: List[Dict[str, Any]] = []
        for row in attempt_rows:
            details = self._normalize_json(row[9]) if row[9] is not None else {}
            details = details if isinstance(details, dict) else {}
            attempt = {
                "attempt_no": int(row[0]),
                "input_run_id": row[1],
                "classification": row[2],
                "proposed_diff_summary": row[3],
                "confidence": float(row[4]) if row[4] is not None else None,
                "applied": bool(row[5]),
                "output_run_id": row[6],
                "result_status": row[7],
                "error": row[8],
                "details": details,
                "created_at": row[10].isoformat() if row[10] else None,
            }
            attempts.append(attempt)

            stage_events = details.get("timeline")
            if isinstance(stage_events, list):
                for stage in stage_events:
                    if not isinstance(stage, dict):
                        continue
                    timeline.append(
                        {
                            "attempt_no": int(row[0]),
                            "step": stage.get("step"),
                            "status": stage.get("status"),
                            "message": stage.get("message"),
                            "timestamp": stage.get("timestamp") or attempt["created_at"],
                        }
                    )

        payload = {
            "id": run_row[0],
            "dataset_name": run_row[1],
            "initial_run_id": run_row[2],
            "final_run_id": run_row[3],
            "status": run_row[4],
            "attempt_count": int(run_row[5] or 0),
            "policy_blocks": int(run_row[6] or 0),
            "summary": summary,
            "created_at": run_row[8].isoformat() if run_row[8] else None,
            "updated_at": run_row[9].isoformat() if run_row[9] else None,
            "attempts": attempts,
            "timeline": timeline,
            "plan": summary.get("plan"),
            "applied_changes": summary.get("applied_changes", []),
        }
        return payload

    def run_agentic_contract_remediation(
        self,
        *,
        dataset_name: str,
        max_retries: int = 2,
        autonomy_mode: str = "full_auto",
    ) -> Dict[str, Any]:
        from src.services.policy_service import PolicyService

        safe_retries = max(1, min(int(max_retries), 5))
        normalized_mode = str(autonomy_mode or "full_auto").strip().lower()
        if normalized_mode != "full_auto":
            raise HTTPException(status_code=400, detail="Only autonomy_mode=full_auto is currently supported.")

        latest_target = self._latest_run_record(dataset_name, statuses=["BLOCKED", "WARNING"])
        latest_any = latest_target or self._latest_run_record(dataset_name)
        if not latest_any:
            raise HTTPException(status_code=404, detail=f"No run history for dataset {dataset_name}")

        initial_status = str(latest_any.get("status") or "").upper()
        initial_run_id = str(latest_any.get("run_id") or "").strip() or None
        remediation_run_id = str(uuid.uuid4())
        policy_blocks = 0
        attempts_made = 0
        applied_changes: List[Dict[str, Any]] = []
        final_run_id: Optional[str] = initial_run_id
        terminal_status = "FAILED"
        terminal_plan: Optional[Dict[str, Any]] = None

        self._persist_agentic_remediation_run(
            remediation_run_id=remediation_run_id,
            dataset_name=dataset_name,
            initial_run_id=initial_run_id,
            final_run_id=final_run_id,
            status="RUNNING",
            attempt_count=0,
            policy_blocks=0,
            summary={"autonomy_mode": normalized_mode, "initial_status": initial_status, "applied_changes": []},
        )

        current_run = latest_any
        status_gate = {"BLOCKED", "WARNING"}
        if initial_status not in status_gate:
            terminal_status = "PLAN_REQUIRED"
            terminal_plan = self._format_plan(
                classification="unknown",
                why_stopped=f"Latest run status is {initial_status or 'UNKNOWN'}; trigger status not met.",
                reason=str(current_run.get("reason") or ""),
                tool_outputs=self._tool_outputs_for_run(current_run.get("run_id")),
                run_id=current_run.get("run_id"),
            )
        else:
            for attempt_no in range(1, safe_retries + 1):
                attempts_made = attempt_no
                input_run_id = str(current_run.get("run_id") or "").strip() or None
                tool_outputs = self._tool_outputs_for_run(input_run_id)
                recent_runs = self._recent_run_records(dataset_name, limit=5)
                classification_info = self._classify_failure(
                    run=current_run,
                    tool_outputs=tool_outputs,
                    recent_runs=recent_runs,
                )
                classification = classification_info["classification"]
                classification_reason = classification_info["reason"]

                timeline_events = [
                    {
                        "step": "classify",
                        "status": "completed",
                        "message": f"Classified as {classification}.",
                        "timestamp": datetime.now().isoformat(),
                    }
                ]

                if not self._is_fixable_classification(classification):
                    terminal_status = "PLAN_REQUIRED"
                    terminal_plan = self._format_plan(
                        classification=classification,
                        why_stopped="Classification marked as non-fixable for contract-only loop.",
                        reason=classification_reason,
                        tool_outputs=tool_outputs,
                        run_id=input_run_id,
                    )
                    self._persist_agentic_remediation_attempt(
                        remediation_run_id=remediation_run_id,
                        attempt_no=attempt_no,
                        input_run_id=input_run_id,
                        classification=classification,
                        proposed_diff_summary=None,
                        confidence=None,
                        applied=False,
                        output_run_id=input_run_id,
                        result_status="PLAN_REQUIRED",
                        error=None,
                        details={
                            "classification_reason": classification_reason,
                            "timeline": timeline_events,
                        },
                    )
                    break

                contract_doc = self.contract_store.read(dataset_name)
                if not contract_doc:
                    raise HTTPException(status_code=404, detail=f"No active contract found for dataset {dataset_name}")
                current_yaml = contract_doc.content

                try:
                    patch = self._build_contract_patch_with_ai(
                        dataset_name=dataset_name,
                        current_yaml=current_yaml,
                        classification=classification,
                        classification_reason=classification_reason,
                        run=current_run,
                        tool_outputs=tool_outputs,
                        recent_runs=recent_runs,
                    )
                    timeline_events.append(
                        {
                            "step": "propose",
                            "status": "completed",
                            "message": "AI generated a contract patch proposal.",
                            "timestamp": datetime.now().isoformat(),
                        }
                    )
                except Exception as exc:
                    timeline_events.append(
                        {
                            "step": "propose",
                            "status": "failed",
                            "message": f"Patch generation failed: {exc}",
                            "timestamp": datetime.now().isoformat(),
                        }
                    )
                    self._persist_agentic_remediation_attempt(
                        remediation_run_id=remediation_run_id,
                        attempt_no=attempt_no,
                        input_run_id=input_run_id,
                        classification=classification,
                        proposed_diff_summary=None,
                        confidence=None,
                        applied=False,
                        output_run_id=input_run_id,
                        result_status="FAILED",
                        error=str(exc),
                        details={
                            "classification_reason": classification_reason,
                            "timeline": timeline_events,
                        },
                    )
                    if attempt_no >= safe_retries:
                        terminal_status = "PLAN_REQUIRED"
                        terminal_plan = self._format_plan(
                            classification=classification,
                            why_stopped="Exceeded retry limit due to malformed or unusable AI patch output.",
                            reason=classification_reason,
                            tool_outputs=tool_outputs,
                            run_id=input_run_id,
                        )
                    continue

                policy = PolicyService(self.agent).evaluate_action(
                    action="remediation_apply",
                    dataset_name=dataset_name,
                )
                policy_decision = str(policy.get("decision") or "").lower()
                if policy_decision == "deny":
                    policy_blocks += 1
                    terminal_status = "BLOCKED_BY_POLICY"
                    timeline_events.append(
                        {
                            "step": "policy",
                            "status": "blocked",
                            "message": "Policy denied auto-remediation apply.",
                            "timestamp": datetime.now().isoformat(),
                        }
                    )
                    self._persist_agentic_remediation_attempt(
                        remediation_run_id=remediation_run_id,
                        attempt_no=attempt_no,
                        input_run_id=input_run_id,
                        classification=classification,
                        proposed_diff_summary=patch.get("change_summary"),
                        confidence=self._safe_float(patch.get("confidence"), None),  # type: ignore[arg-type]
                        applied=False,
                        output_run_id=input_run_id,
                        result_status="BLOCKED_BY_POLICY",
                        error="Policy denied auto-remediation apply.",
                        details={
                            "classification_reason": classification_reason,
                            "policy": policy,
                            "timeline": timeline_events,
                        },
                    )
                    break

                timeline_events.append(
                    {
                        "step": "policy",
                        "status": "completed",
                        "message": f"Policy decision: {policy.get('decision')}.",
                        "timestamp": datetime.now().isoformat(),
                    }
                )

                try:
                    apply_result = self.apply_remediation(
                        dataset_name=dataset_name,
                        proposed_yaml=str(patch.get("modified_yaml") or ""),
                        error_context=str(
                            patch.get("change_summary")
                            or classification_reason
                            or "Agentic auto-remediation loop contract patch"
                        ),
                    )
                    timeline_events.append(
                        {
                            "step": "apply",
                            "status": "completed",
                            "message": "Contract patch applied.",
                            "timestamp": datetime.now().isoformat(),
                        }
                    )
                except Exception as exc:
                    timeline_events.append(
                        {
                            "step": "apply",
                            "status": "failed",
                            "message": f"Contract apply failed: {exc}",
                            "timestamp": datetime.now().isoformat(),
                        }
                    )
                    self._persist_agentic_remediation_attempt(
                        remediation_run_id=remediation_run_id,
                        attempt_no=attempt_no,
                        input_run_id=input_run_id,
                        classification=classification,
                        proposed_diff_summary=patch.get("change_summary"),
                        confidence=self._safe_float(patch.get("confidence"), None),  # type: ignore[arg-type]
                        applied=False,
                        output_run_id=input_run_id,
                        result_status="FAILED",
                        error=str(exc),
                        details={
                            "classification_reason": classification_reason,
                            "policy": policy,
                            "patch_meta": {
                                "risk_level": patch.get("risk_level"),
                                "expected_effect": patch.get("expected_effect"),
                            },
                            "timeline": timeline_events,
                        },
                    )
                    if attempt_no >= safe_retries:
                        terminal_status = "PLAN_REQUIRED"
                        terminal_plan = self._format_plan(
                            classification=classification,
                            why_stopped="Exceeded retry limit because contract patch could not be applied.",
                            reason=classification_reason,
                            tool_outputs=tool_outputs,
                            run_id=input_run_id,
                        )
                    continue

                verify_result = self.evaluate_dataset(dataset_name)
                verify_meta = self._extract_status_and_run_id(verify_result if isinstance(verify_result, dict) else {})
                verify_run_id = verify_meta.get("run_id")
                verify_status = str(verify_meta.get("status") or "").upper()
                if not verify_run_id:
                    latest_after_verify = self._latest_run_record(dataset_name)
                    if latest_after_verify:
                        verify_run_id = latest_after_verify.get("run_id")
                        verify_status = str(latest_after_verify.get("status") or "").upper()
                        current_run = latest_after_verify
                else:
                    current_run = self._latest_run_record(dataset_name) or current_run

                final_run_id = verify_run_id or final_run_id
                timeline_events.append(
                    {
                        "step": "re_run",
                        "status": "completed",
                        "message": f"Validation re-run finished with status {verify_status or 'UNKNOWN'}.",
                        "timestamp": datetime.now().isoformat(),
                    }
                )

                applied_changes.append(
                    {
                        "attempt_no": attempt_no,
                        "change_summary": patch.get("change_summary"),
                        "risk_level": patch.get("risk_level"),
                        "confidence": patch.get("confidence"),
                        "expected_effect": patch.get("expected_effect"),
                        "input_run_id": input_run_id,
                        "output_run_id": verify_run_id,
                        "result_status": verify_status or "UNKNOWN",
                    }
                )

                attempt_result_status = "AUTO_FIXED" if verify_status == "PASSED" else "FAILED"
                self._persist_agentic_remediation_attempt(
                    remediation_run_id=remediation_run_id,
                    attempt_no=attempt_no,
                    input_run_id=input_run_id,
                    classification=classification,
                    proposed_diff_summary=patch.get("change_summary"),
                    confidence=self._safe_float(patch.get("confidence"), None),  # type: ignore[arg-type]
                    applied=True,
                    output_run_id=verify_run_id,
                    result_status=attempt_result_status,
                    error=None if verify_status == "PASSED" else f"Re-run status remained {verify_status or 'UNKNOWN'}",
                    details={
                        "classification_reason": classification_reason,
                        "policy": policy,
                        "patch_meta": {
                            "risk_level": patch.get("risk_level"),
                            "expected_effect": patch.get("expected_effect"),
                        },
                        "apply_result": apply_result,
                        "verify_result": verify_result,
                        "timeline": timeline_events,
                    },
                )

                if verify_status == "PASSED":
                    terminal_status = "AUTO_FIXED"
                    terminal_plan = None
                    break

                if attempt_no >= safe_retries:
                    terminal_status = "PLAN_REQUIRED"
                    terminal_plan = self._format_plan(
                        classification=classification,
                        why_stopped=f"Exceeded retry limit ({safe_retries}) without achieving PASSED status.",
                        reason=str(current_run.get("reason") or classification_reason),
                        tool_outputs=self._tool_outputs_for_run(verify_run_id),
                        run_id=verify_run_id,
                    )

        if terminal_status == "FAILED" and terminal_plan is None:
            terminal_status = "PLAN_REQUIRED"
            terminal_plan = self._format_plan(
                classification="unknown",
                why_stopped="Loop ended unexpectedly before convergence.",
                reason=str(current_run.get("reason") or ""),
                tool_outputs=self._tool_outputs_for_run(current_run.get("run_id")),
                run_id=current_run.get("run_id"),
            )

        summary_payload: Dict[str, Any] = {
            "autonomy_mode": normalized_mode,
            "initial_status": initial_status,
            "applied_changes": applied_changes,
        }
        if terminal_plan:
            summary_payload["plan"] = terminal_plan

        self._persist_agentic_remediation_run(
            remediation_run_id=remediation_run_id,
            dataset_name=dataset_name,
            initial_run_id=initial_run_id,
            final_run_id=final_run_id,
            status=terminal_status,
            attempt_count=attempts_made,
            policy_blocks=policy_blocks,
            summary=summary_payload,
        )

        result = self.get_agentic_remediation_run(remediation_run_id)
        return {
            "id": remediation_run_id,
            "status": terminal_status,
            "attempts": attempts_made,
            "initial_run_id": initial_run_id,
            "final_run_id": final_run_id,
            "applied_changes": applied_changes,
            "plan": terminal_plan,
            "run": result,
        }

    def _run_agentic_reliability_loop_legacy(
        self,
        *,
        dataset_name: str,
        metric: Optional[str] = None,
        auto_execute: bool = False,
        confidence_threshold: float = 0.8,
        policy_approved: bool = False,
        policy_reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Legacy deterministic implementation retained as fallback.
        """
        from src.services.policy_service import PolicyService

        investigation = self.investigate_anomaly(dataset_name, metric=metric)
        remediation = self.get_remediation_plan(dataset_name)
        confidence = self._safe_float(
            (investigation.get("root_cause_hypothesis") or {}).get("confidence"),
            0.0,
        )
        threshold = max(0.0, min(confidence_threshold, 1.0))

        policy = PolicyService(self.agent).evaluate_action(
            action="remediation_apply",
            dataset_name=dataset_name,
        )

        execution: Dict[str, Any] = {
            "requested_auto_execute": bool(auto_execute),
            "decision": "no_action",
            "confidence": round(confidence, 2),
            "confidence_threshold": threshold,
            "policy": policy,
        }

        if remediation.get("status") != "remediation_available":
            execution["decision"] = "no_remediation_needed"
            execution["reason"] = remediation.get("message", "No remediation available.")
            return {
                "dataset_name": dataset_name,
                "investigation": investigation,
                "remediation": remediation,
                "execution": execution,
            }

        proposed_yaml = str(remediation.get("proposed_yaml") or "")
        if not proposed_yaml.strip() or proposed_yaml.lstrip().startswith("# Error"):
            execution["decision"] = "requires_hitl"
            execution["reason"] = "Remediation proposal is unavailable or invalid."
            return {
                "dataset_name": dataset_name,
                "investigation": investigation,
                "remediation": remediation,
                "execution": execution,
            }

        if confidence < threshold:
            execution["decision"] = "requires_hitl"
            execution["reason"] = "Confidence below threshold; human approval required."
            return {
                "dataset_name": dataset_name,
                "investigation": investigation,
                "remediation": remediation,
                "execution": execution,
            }

        if not auto_execute:
            execution["decision"] = "proposed_only"
            execution["reason"] = "Auto execution disabled. Proposal is ready for human review."
            return {
                "dataset_name": dataset_name,
                "investigation": investigation,
                "remediation": remediation,
                "execution": execution,
            }

        if policy.get("decision") == "approval_required" and not (policy_approved and str(policy_reason or "").strip()):
            execution["decision"] = "approval_required"
            execution["reason"] = "Policy approval required before remediation execution."
            execution["missing_controls"] = policy.get("required_controls", [])
            return {
                "dataset_name": dataset_name,
                "investigation": investigation,
                "remediation": remediation,
                "execution": execution,
            }

        # Strict policy enforcement before mutation.
        PolicyService.enforce(
            policy,
            approved=bool(policy_approved),
            reason=policy_reason,
        )

        apply_result = self.apply_remediation(
            dataset_name=dataset_name,
            proposed_yaml=proposed_yaml,
            error_context=str(
                (investigation.get("root_cause_hypothesis") or {}).get("summary")
                or investigation.get("run", {}).get("reason")
                or "Agentic remediation loop execution"
            ),
        )

        execution["decision"] = "executed"
        execution["reason"] = "Remediation applied automatically."
        execution["result"] = apply_result
        return {
            "dataset_name": dataset_name,
            "investigation": investigation,
            "remediation": remediation,
            "execution": execution,
        }

    def _approve_contract_direct(self, dataset_name: str, approved_yaml: str) -> Dict[str, Any]:
        """
        Approve a contract and trigger validation of pending files.
        """
        saved_contract = self.contract_store.write(dataset_name, approved_yaml)
        contract_path = Path(saved_contract.location)

        print(f"✅ Contract approved and saved: {contract_path}")

        pending_dir = Path("data/pending_approval")
        pending_files = list(pending_dir.glob(f"{dataset_name}*"))

        validation_results: List[Dict[str, Any]] = []

        if pending_files:
            print(f"📋 Found {len(pending_files)} pending file(s) for validation")

            for file_path in pending_files:
                if ".verdict." in file_path.name:
                    continue

                print(f"   Validating: {file_path.name}")

                verdict = self.agent.evaluate_data_file(file_path=str(file_path), dataset_name=dataset_name)

                verdict_path = file_path.with_suffix(file_path.suffix + ".verdict.json")
                with open(verdict_path, "w") as f:
                    json.dump(verdict, f, indent=2)

                if verdict["status"] == "BLOCKED":
                    dest_dir = Path("data/quarantine")
                    dest_dir.mkdir(exist_ok=True)
                    dest = dest_dir / file_path.name
                    shutil.move(str(file_path), str(dest))
                    print("   🚫 BLOCKED → Moved to quarantine")
                else:
                    dest_dir = Path("data/landing")
                    dest_dir.mkdir(exist_ok=True)
                    dest = dest_dir / file_path.name
                    shutil.move(str(file_path), str(dest))
                    print("   ✅ PASSED → Moved to landing")

                validation_results.append(
                    {
                        "file": file_path.name,
                        "status": verdict["status"],
                        "quality_score": verdict.get("quality_score"),
                    }
                )

        proposals_dir = Path("config/proposals")
        proposal_yaml = proposals_dir / f"{dataset_name}.yaml"
        proposal_meta = proposals_dir / f"{dataset_name}.meta.json"

        if proposal_yaml.exists():
            proposal_yaml.unlink()
        if proposal_meta.exists():
            proposal_meta.unlink()

        return {
            "status": "approved",
            "dataset_name": dataset_name,
            "contract_path": str(contract_path),
            "validated_files": validation_results,
            "message": f"Contract approved. Validated {len(validation_results)} pending file(s).",
        }

    def approve_contract(self, dataset_name: str, approved_yaml: str) -> Dict[str, Any]:
        if self.hitl_workflow is not None:
            resumed = self.hitl_workflow.resume(
                dataset_name=dataset_name,
                decision="approve",
                approved_yaml=approved_yaml,
            )
            if resumed.get("handled"):
                return resumed.get("result", {})

        return self._approve_contract_direct(dataset_name=dataset_name, approved_yaml=approved_yaml)

    def _reject_contract_proposal_direct(self, dataset_name: str) -> Dict[str, Any]:
        """
        Reject a contract proposal.
        Moves pending files to quarantine and removes proposal.
        """
        pending_dir = Path("data/pending_approval")
        quarantine_dir = Path("data/quarantine")
        quarantine_dir.mkdir(exist_ok=True)

        pending_files = list(pending_dir.glob(f"{dataset_name}*"))
        moved_files: List[str] = []

        for file_path in pending_files:
            dest = quarantine_dir / file_path.name
            shutil.move(str(file_path), str(dest))
            moved_files.append(file_path.name)

        proposals_dir = Path("config/proposals")
        proposal_yaml = proposals_dir / f"{dataset_name}.yaml"
        proposal_meta = proposals_dir / f"{dataset_name}.meta.json"

        if proposal_yaml.exists():
            proposal_yaml.unlink()
        if proposal_meta.exists():
            proposal_meta.unlink()

        return {
            "status": "rejected",
            "dataset_name": dataset_name,
            "quarantined_files": moved_files,
            "message": f"Proposal rejected. {len(moved_files)} file(s) moved to quarantine.",
        }

    def reject_contract_proposal(self, dataset_name: str) -> Dict[str, Any]:
        if self.hitl_workflow is not None:
            resumed = self.hitl_workflow.resume(
                dataset_name=dataset_name,
                decision="reject",
            )
            if resumed.get("handled"):
                return resumed.get("result", {})

        return self._reject_contract_proposal_direct(dataset_name=dataset_name)

    def _get_dataset_meta(self, dataset_name: str) -> Dict[str, Any]:
        datasets = self.agent.discover_datasets()
        meta = next((d for d in datasets if d["name"] == dataset_name), None)
        if not meta:
            raise HTTPException(status_code=404, detail=f"Dataset {dataset_name} not found")
        return meta

    def get_remediation_plan(self, dataset_name: str) -> Dict[str, Any]:
        """
        Build a remediation proposal for the latest failing run.
        """
        history = self.agent.get_run_history(dataset_name, limit=1)
        if not history:
            return {"status": "no_history", "message": "No run history found."}

        last_run = history[0]
        if last_run.get("status") == "PASSED":
            return {"status": "healthy", "message": "Dataset is healthy. No remediation needed."}

        error_details = last_run.get("reason", "Unknown error")
        meta = self._get_dataset_meta(dataset_name)

        contract_path = meta.get("contract_path")
        if not contract_path:
            raise HTTPException(status_code=404, detail=f"No active contract found for {dataset_name}")

        with open(contract_path, "r") as f:
            current_yaml = f.read()

        try:
            impact = self.agent.impact_analyzer.get_downstream_impact(dataset_name)
            downstream_str = ", ".join(impact.get("downstream_systems", ["None"]))
            impact_context = (
                f"This dataset is {impact.get('overall_criticality')} criticality. "
                f"Downstream affected systems: {downstream_str}."
            )
        except Exception:
            impact_context = "Impact unknown."

        proposed_yaml = None
        deterministic_yaml = None
        observed_yaml = None
        merge_summary = None
        generation_meta = None

        data_path = meta.get("data_file")
        if data_path and Path(data_path).exists():
            try:
                hybrid = self.agent.remediator.propose_schema_update_hybrid(
                    current_yaml=current_yaml,
                    data_path=data_path,
                    error_details=error_details,
                    impact_context=impact_context,
                    enable_llm=True,
                )
                deterministic_yaml = hybrid.get("deterministic_yaml")
                proposed_yaml = hybrid.get("llm_yaml") or deterministic_yaml
                observed_yaml = hybrid.get("observed_yaml")
                merge_summary = hybrid.get("merge_summary")
                generation_meta = hybrid.get("generation")
            except Exception as hybrid_err:
                proposed_yaml = f"# Error generating fix: {hybrid_err}"
        else:
            try:
                proposed_yaml = self.agent.remediator.propose_schema_update(
                    current_yaml, error_details, impact_context
                )
            except Exception as llm_err:
                proposed_yaml = f"# Error generating fix: {llm_err}"

        return {
            "status": "remediation_available",
            "dataset": dataset_name,
            "error": error_details,
            "original_yaml": current_yaml,
            "proposed_yaml": proposed_yaml,
            "deterministic_yaml": deterministic_yaml,
            "observed_yaml": observed_yaml,
            "merge_summary": merge_summary,
            "generation": generation_meta,
        }

    def apply_remediation(
        self,
        *,
        dataset_name: str,
        proposed_yaml: str,
        error_context: str,
    ) -> Dict[str, Any]:
        """
        Apply an approved remediation YAML update and persist audit history.
        """
        meta = self._get_dataset_meta(dataset_name)
        contract_path = meta.get("contract_path")
        if not contract_path:
            raise HTTPException(status_code=404, detail=f"No active contract found for {dataset_name}")

        with open(contract_path, "r") as f:
            original_yaml = f.read()

        from src.tools.schema_remediator import SchemaRemediator

        remediator = SchemaRemediator()
        version_path = remediator.apply_fix(contract_path, proposed_yaml)
        version_filename = os.path.basename(version_path)

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO remediation_history (dataset_name, error_context, original_yaml, proposed_yaml, backup_path)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (dataset_name, error_context, original_yaml, proposed_yaml, version_path),
                )
                cur.execute(
                    """
                    INSERT INTO schema_audit_log (id, dataset_name, filename, timestamp, change_summary)
                    VALUES (%s, %s, %s, NOW(), %s)
                    """,
                    (
                        str(uuid.uuid4()),
                        dataset_name,
                        version_filename,
                        f"AI Remediation: {error_context}",
                    ),
                )

        return {
            "status": "success",
            "message": f"Remediation applied. Version saved: {version_filename}",
            "backup_path": version_path,
        }

    def bulk_delete_datasets(self, dataset_names: List[str]) -> Dict[str, Any]:
        """
        Delete many datasets and return per-dataset success/failure details.
        """
        normalized = []
        seen = set()
        for name in dataset_names:
            candidate = str(name or "").strip()
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            normalized.append(candidate)

        if not normalized:
            raise HTTPException(status_code=400, detail="dataset_names must include at least one dataset")

        results: List[Dict[str, Any]] = []
        success_count = 0
        failure_count = 0

        for dataset_name in normalized:
            try:
                result = self.delete_dataset(dataset_name)
                success_count += 1
                results.append(
                    {
                        "dataset_name": dataset_name,
                        "status": "deleted",
                        "result": result,
                    }
                )
            except Exception as exc:
                failure_count += 1
                if isinstance(exc, HTTPException):
                    error_text = f"{exc.status_code}: {exc.detail}"
                else:
                    error_text = str(exc)
                results.append(
                    {
                        "dataset_name": dataset_name,
                        "status": "failed",
                        "error": error_text,
                    }
                )

        return {
            "status": "completed",
            "total": len(normalized),
            "success_count": success_count,
            "failure_count": failure_count,
            "results": results,
        }

    def bulk_evaluate_datasets(self, dataset_names: List[str], force_load: bool = False) -> Dict[str, Any]:
        """
        Evaluate many datasets and return per-dataset success/failure details.

        This is used by async jobs to support "scan all" / bulk scan semantics without
        fanning out many long-running requests from a browser.
        """
        normalized = []
        seen = set()
        for name in dataset_names:
            candidate = str(name or "").strip()
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            normalized.append(candidate)

        if not normalized:
            raise HTTPException(status_code=400, detail="dataset_names must include at least one dataset")

        results: List[Dict[str, Any]] = []
        success_count = 0
        failure_count = 0

        for dataset_name in normalized:
            try:
                verdict = self.evaluate_dataset(dataset_name, force_load=force_load)
                success_count += 1
                results.append(
                    {
                        "dataset_name": dataset_name,
                        "status": "evaluated",
                        "result": verdict,
                    }
                )
            except Exception as exc:
                failure_count += 1
                if isinstance(exc, HTTPException):
                    error_text = f"{exc.status_code}: {exc.detail}"
                else:
                    error_text = str(exc)
                results.append(
                    {
                        "dataset_name": dataset_name,
                        "status": "failed",
                        "error": error_text,
                    }
                )

        return {
            "status": "completed",
            "total": len(normalized),
            "success_count": success_count,
            "failure_count": failure_count,
            "results": results,
        }

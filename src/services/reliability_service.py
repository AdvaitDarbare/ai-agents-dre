from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from src.agents.monitor_agent import MonitorAgent
from src.contracts.store import FileContractStore
from src.utils.database import get_connection


class ReliabilityService:
    """
    Service layer for core reliability operations used by API routes and MCP tools.
    """

    def __init__(self, agent: MonitorAgent, contract_store: FileContractStore):
        self.agent = agent
        self.contract_store = contract_store

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

    def evaluate_dataset(self, dataset_name: str) -> Dict[str, Any]:
        datasets = self.agent.discover_datasets()
        meta = next((d for d in datasets if d["name"] == dataset_name), None)
        if not meta or not meta.get("data_file"):
            raise HTTPException(status_code=404, detail=f"Data file for {dataset_name} not found.")

        return self.agent.evaluate_data_file(meta["data_file"], dataset_name)

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

                # Parse JSONB columns
                dimension_scores = json.loads(row[9]) if row[9] else None
                full_verdict = json.loads(row[10]) if row[10] else None

                return {
                    "run_id": row[0],
                    "timestamp": row[1].isoformat() if row[1] else None,
                    "dataset_name": row[2],
                    "status": row[3],
                    "quality_score": row[4],
                    "anomaly_count": row[5],
                    "z_score_max": row[6],
                    "reason": row[7],
                    "duration_ms": row[8],
                    "dimension_scores": dimension_scores,
                    "full_verdict": full_verdict,
                }

    def chat_with_copilot(self, query: str) -> Dict[str, str]:
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
                        "status": row[1],
                        "reason": latest.get("reason", "No recent run data"),
                        "anomalies": [],
                        "schema_evolution": {},
                    }

        response = self.agent.request_copilot_chat(query, context_data)
        return {"response": response}

    def propose_contract(self, dataset_name: str, file_path: Optional[str] = None) -> Dict[str, Any]:
        data_path = file_path
        if not data_path:
            datasets = self.agent.discover_datasets()
            meta = next((d for d in datasets if d["name"] == dataset_name), None)
            if meta:
                data_path = meta.get("data_file")

        if not data_path:
            data_path = f"data/{dataset_name}.csv"

        if not Path(data_path).exists():
            raise HTTPException(status_code=404, detail=f"Data file not found at {data_path}")

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
                        SELECT status
                        FROM slo_history
                        WHERE dataset_name = %s
                        ORDER BY timestamp DESC
                        LIMIT %s
                    )
                    SELECT
                        COUNT(*) AS total_checks,
                        COUNT(*) FILTER (WHERE status = 'PASS') AS pass_checks
                    FROM recent
                    """,
                    (dataset_name, window),
                )
                totals = cur.fetchone()
                total_checks = int(totals[0] or 0)
                pass_checks = int(totals[1] or 0)

                return {
                    "dataset_name": dataset_name,
                    "window": window,
                    "overall_pass_rate": round((pass_checks / total_checks) * 100, 2) if total_checks else None,
                    "total_checks": total_checks,
                    "checks": [
                        {
                            "slo_name": row[0],
                            "total_checks": row[1],
                            "pass_checks": row[2],
                            "pass_rate": round((row[2] / row[1]) * 100, 2) if row[1] else None,
                            "avg_error_budget_burn": float(row[3]) if row[3] is not None else None,
                            "last_seen": row[4].isoformat() if row[4] else None,
                        }
                        for row in grouped
                    ],
                }

    def approve_contract(self, dataset_name: str, approved_yaml: str) -> Dict[str, Any]:
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

    def reject_contract_proposal(self, dataset_name: str) -> Dict[str, Any]:
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

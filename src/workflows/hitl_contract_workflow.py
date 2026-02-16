from __future__ import annotations

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict
from urllib.parse import quote_plus

from dotenv import load_dotenv
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

from src.agents.monitor_agent import MonitorAgent
from src.contracts.store import FileContractStore

try:
    from langgraph.checkpoint.postgres import PostgresSaver
except Exception as exc:  # pragma: no cover
    PostgresSaver = None
    _IMPORT_ERR = exc
else:
    _IMPORT_ERR = None

load_dotenv()


class HITLState(TypedDict, total=False):
    dataset_name: str
    file_path: str
    pending_file_path: str
    contract_exists: bool
    proposal_yaml: str
    approval_decision: str
    approved_yaml: str
    validated_files: List[Dict[str, Any]]
    contract_path: str
    quality_score: Optional[float]
    anomaly_summary: Dict[str, Any]
    tool_outputs: List[Dict[str, Any]]
    message: str
    error: Optional[str]
    status: str


class HITLContractWorkflow:
    """
    LangGraph-backed HITL workflow for contract-missing datasets.

    Flow:
      check_contract -> prepare_pending_and_propose -> wait_for_approval (interrupt)
      -> apply_approved | apply_rejected

    Uses PostgreSQL checkpointer for durable pause/resume.
    """

    def __init__(self, agent: MonitorAgent, contract_store: FileContractStore):
        self.agent = agent
        self.contract_store = contract_store
        self.pending_dir = Path("data/pending_approval")
        self.landing_dir = Path("data/landing")
        self.quarantine_dir = Path("data/quarantine")
        self.proposals_dir = Path("config/proposals")

        self.pending_dir.mkdir(parents=True, exist_ok=True)
        self.landing_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
        self.proposals_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _thread_id(dataset_name: str) -> str:
        return f"hitl-contract:{dataset_name.strip().lower()}"

    @staticmethod
    def _matches_dataset_artifact(name: str, dataset_name: str) -> bool:
        normalized_name = name.lower()
        normalized_dataset = dataset_name.lower()

        if normalized_name == normalized_dataset:
            return True

        for sep in ("_", ".", "-"):
            if normalized_name.startswith(f"{normalized_dataset}{sep}"):
                return True

        return False

    @staticmethod
    def _to_conn_string() -> str:
        explicit = os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL")
        if explicit:
            return explicit

        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB", "dre")
        user = os.getenv("POSTGRES_USER", "dre_user")
        password = quote_plus(os.getenv("POSTGRES_PASSWORD", "dre_password"))
        sslmode = os.getenv("POSTGRES_SSLMODE", "disable")
        return f"postgresql://{user}:{password}@{host}:{port}/{db}?sslmode={sslmode}"

    def _open_checkpointer(self):
        if PostgresSaver is None:
            raise RuntimeError(
                "langgraph.checkpoint.postgres unavailable. "
                "Install langgraph-checkpoint-postgres<3 and langgraph-checkpoint<3."
            ) from _IMPORT_ERR

        return PostgresSaver.from_conn_string(self._to_conn_string())

    def _build_graph(self, checkpointer):
        graph = StateGraph(HITLState)

        graph.add_node("check_contract", self._node_check_contract)
        graph.add_node("prepare_pending_and_propose", self._node_prepare_pending_and_propose)
        graph.add_node("wait_for_approval", self._node_wait_for_approval)
        graph.add_node("apply_approved", self._node_apply_approved)
        graph.add_node("apply_rejected", self._node_apply_rejected)
        graph.add_node("invalid_decision", self._node_invalid_decision)
        graph.add_node("already_configured", self._node_already_configured)

        graph.add_edge(START, "check_contract")
        graph.add_conditional_edges(
            "check_contract",
            self._route_after_contract_check,
            {
                "existing": "already_configured",
                "missing": "prepare_pending_and_propose",
            },
        )
        graph.add_edge("prepare_pending_and_propose", "wait_for_approval")
        graph.add_conditional_edges(
            "wait_for_approval",
            self._route_after_approval,
            {
                "approve": "apply_approved",
                "reject": "apply_rejected",
                "invalid": "invalid_decision",
            },
        )
        graph.add_edge("apply_approved", END)
        graph.add_edge("apply_rejected", END)
        graph.add_edge("invalid_decision", END)
        graph.add_edge("already_configured", END)

        return graph.compile(checkpointer=checkpointer)

    def _node_check_contract(self, state: HITLState) -> HITLState:
        dataset_name = state["dataset_name"]
        exists = self.contract_store.exists(dataset_name)
        return {
            "contract_exists": exists,
            "status": "running",
            "error": None,
        }

    def _route_after_contract_check(self, state: HITLState) -> str:
        if state.get("contract_exists"):
            return "existing"
        return "missing"

    def _node_already_configured(self, state: HITLState) -> HITLState:
        return {
            "status": "completed",
            "message": "Contract already exists. HITL workflow not required.",
            "quality_score": None,
            "anomaly_summary": {},
            "tool_outputs": [],
            "error": None,
        }

    def _node_prepare_pending_and_propose(self, state: HITLState) -> HITLState:
        dataset_name = state["dataset_name"]
        source_file = Path(state["file_path"])

        proposal_yaml_path = self.proposals_dir / f"{dataset_name}.yaml"
        proposal_meta_path = self.proposals_dir / f"{dataset_name}.meta.json"

        pending_path = self.pending_dir / source_file.name

        # Move to pending once (idempotent for retries)
        if source_file.resolve() != pending_path.resolve():
            if source_file.exists():
                shutil.move(str(source_file), str(pending_path))
            elif not pending_path.exists():
                raise FileNotFoundError(f"Source file not found: {source_file}")
        else:
            pending_path = source_file

        if proposal_yaml_path.exists():
            proposal_yaml = proposal_yaml_path.read_text()
            stats: Dict[str, Any] = {}
        else:
            proposal = self.agent.propose_contract(
                dataset_name=dataset_name,
                data_path=str(pending_path),
                include_metadata=True,
            )
            proposal_yaml = proposal.get("yaml_content", "")
            proposal_yaml_path.write_text(proposal_yaml)
            stats = proposal.get("stats", {}) if isinstance(proposal, dict) else {}

        proposal_meta_path.write_text(
            json.dumps(
                {
                    "dataset_name": dataset_name,
                    "proposed_at": datetime.now().isoformat(),
                    "source_file": str(pending_path),
                    "status": "pending_approval",
                    "row_count": stats.get("row_count"),
                    "column_count": stats.get("column_count"),
                },
                indent=2,
            )
        )

        return {
            "pending_file_path": str(pending_path),
            "proposal_yaml": proposal_yaml,
            "status": "paused_hitl",
            "quality_score": None,
            "anomaly_summary": {},
            "tool_outputs": [
                {
                    "tool": "propose_contract",
                    "status": "ok",
                    "dataset_name": dataset_name,
                }
            ],
            "error": None,
            "message": "Awaiting human approval.",
        }

    def _node_wait_for_approval(self, state: HITLState) -> HITLState:
        approval = interrupt(
            {
                "kind": "contract_approval_required",
                "dataset_name": state["dataset_name"],
                "pending_file_path": state.get("pending_file_path"),
                "proposal_yaml": state.get("proposal_yaml", ""),
            }
        )

        decision = (approval or {}).get("decision", "").strip().lower()
        approved_yaml = (approval or {}).get("approved_yaml")

        return {
            "approval_decision": decision,
            "approved_yaml": approved_yaml,
            "status": "running",
            "error": None,
        }

    def _route_after_approval(self, state: HITLState) -> str:
        decision = (state.get("approval_decision") or "").strip().lower()
        if decision == "approve":
            return "approve"
        if decision == "reject":
            return "reject"
        return "invalid"

    def _node_apply_approved(self, state: HITLState) -> HITLState:
        dataset_name = state["dataset_name"]
        approved_yaml = state.get("approved_yaml") or state.get("proposal_yaml", "")

        saved_contract = self.contract_store.write(dataset_name, approved_yaml)
        contract_path = Path(saved_contract.location)

        pending_files = [
            p
            for p in self.pending_dir.glob(f"{dataset_name}*")
            if p.is_file() and ".verdict." not in p.name
        ]

        validation_results: List[Dict[str, Any]] = []
        quality_scores: List[float] = []
        anomaly_count_total = 0
        z_score_max = 0.0
        collected_tool_outputs: List[Dict[str, Any]] = []

        for file_path in pending_files:
            verdict = self.agent.evaluate_data_file(file_path=str(file_path), dataset_name=dataset_name)

            verdict_path = file_path.with_suffix(file_path.suffix + ".verdict.json")
            verdict_path.write_text(json.dumps(verdict, indent=2))

            if verdict["status"] == "BLOCKED":
                dest = self.quarantine_dir / file_path.name
                shutil.move(str(file_path), str(dest))
            else:
                dest = self.landing_dir / file_path.name
                shutil.move(str(file_path), str(dest))

            quality = verdict.get("quality_score")
            if isinstance(quality, (int, float)):
                quality_scores.append(float(quality))

            anomaly_count_total += int(verdict.get("anomaly_count", 0) or 0)
            z_score_max = max(z_score_max, float(verdict.get("z_score_max", 0.0) or 0.0))

            outputs = verdict.get("tool_outputs")
            if isinstance(outputs, list):
                collected_tool_outputs.extend(outputs)

            validation_results.append(
                {
                    "file": file_path.name,
                    "status": verdict["status"],
                    "quality_score": quality,
                }
            )

        proposal_yaml = self.proposals_dir / f"{dataset_name}.yaml"
        proposal_meta = self.proposals_dir / f"{dataset_name}.meta.json"
        if proposal_yaml.exists():
            proposal_yaml.unlink()
        if proposal_meta.exists():
            proposal_meta.unlink()

        quality_score: Optional[float] = None
        if quality_scores:
            quality_score = sum(quality_scores) / len(quality_scores)

        return {
            "status": "approved",
            "dataset_name": dataset_name,
            "contract_path": str(contract_path),
            "validated_files": validation_results,
            "message": f"Contract approved. Validated {len(validation_results)} pending file(s).",
            "quality_score": quality_score,
            "anomaly_summary": {
                "anomaly_count": anomaly_count_total,
                "z_score_max": z_score_max,
            },
            "tool_outputs": collected_tool_outputs,
            "error": None,
        }

    def _node_apply_rejected(self, state: HITLState) -> HITLState:
        dataset_name = state["dataset_name"]

        pending_files = [
            p
            for p in self.pending_dir.glob(f"{dataset_name}*")
            if p.is_file() and ".verdict." not in p.name
        ]
        moved_files: List[str] = []
        for file_path in pending_files:
            dest = self.quarantine_dir / file_path.name
            shutil.move(str(file_path), str(dest))
            moved_files.append(file_path.name)

        proposal_yaml = self.proposals_dir / f"{dataset_name}.yaml"
        proposal_meta = self.proposals_dir / f"{dataset_name}.meta.json"
        if proposal_yaml.exists():
            proposal_yaml.unlink()
        if proposal_meta.exists():
            proposal_meta.unlink()

        return {
            "status": "rejected",
            "dataset_name": dataset_name,
            "quarantined_files": moved_files,
            "message": f"Proposal rejected. {len(moved_files)} file(s) moved to quarantine.",
            "quality_score": None,
            "anomaly_summary": {},
            "tool_outputs": [],
            "error": None,
        }

    def _node_invalid_decision(self, state: HITLState) -> HITLState:
        decision = state.get("approval_decision")
        return {
            "status": "failed",
            "error": f"Invalid approval decision: {decision}",
            "message": "Approval decision must be 'approve' or 'reject'.",
            "quality_score": None,
            "anomaly_summary": {},
            "tool_outputs": [],
        }

    def _state_payload(self, snapshot) -> Dict[str, Any]:
        values = dict(snapshot.values or {})
        interrupts = [intr.value for intr in (snapshot.interrupts or ())]
        return {
            "state": values,
            "interrupts": interrupts,
            "next": list(snapshot.next or ()),
        }

    def start_missing_contract(self, dataset_name: str, file_path: str) -> Dict[str, Any]:
        thread_id = self._thread_id(dataset_name)
        config = {"configurable": {"thread_id": thread_id}}

        with self._open_checkpointer() as checkpointer:
            checkpointer.setup()
            app = self._build_graph(checkpointer)
            app.invoke(
                {
                    "dataset_name": dataset_name,
                    "file_path": file_path,
                    "status": "running",
                },
                config=config,
            )
            snapshot = app.get_state(config)

        payload = self._state_payload(snapshot)
        state = payload["state"]
        return {
            "handled": True,
            "thread_id": thread_id,
            "status": state.get("status", "running"),
            "message": state.get("message"),
            "interrupts": payload["interrupts"],
            "next": payload["next"],
            "state": state,
        }

    def resume(
        self,
        dataset_name: str,
        decision: str,
        approved_yaml: Optional[str] = None,
    ) -> Dict[str, Any]:
        thread_id = self._thread_id(dataset_name)
        config = {"configurable": {"thread_id": thread_id}}

        with self._open_checkpointer() as checkpointer:
            checkpointer.setup()
            app = self._build_graph(checkpointer)
            snapshot = app.get_state(config)
            if not snapshot.interrupts:
                return {
                    "handled": False,
                    "thread_id": thread_id,
                    "status": "not_waiting",
                    "message": "No interrupted HITL workflow found for dataset.",
                }

            resume_payload: Dict[str, Any] = {"decision": decision}
            if approved_yaml is not None:
                resume_payload["approved_yaml"] = approved_yaml

            app.invoke(Command(resume=resume_payload), config=config)
            after = app.get_state(config)

        state = dict(after.values or {})
        return {
            "handled": True,
            "thread_id": thread_id,
            "status": state.get("status", "completed"),
            "result": state,
            "interrupts": [intr.value for intr in (after.interrupts or ())],
            "next": list(after.next or ()),
        }

    def is_waiting_for_approval(self, dataset_name: str) -> bool:
        thread_id = self._thread_id(dataset_name)
        config = {"configurable": {"thread_id": thread_id}}

        with self._open_checkpointer() as checkpointer:
            checkpointer.setup()
            app = self._build_graph(checkpointer)
            snapshot = app.get_state(config)
            return bool(snapshot.interrupts)

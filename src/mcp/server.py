from __future__ import annotations

import argparse
import os
from typing import Any, Dict, Optional

from fastapi import HTTPException
from mcp.server.fastmcp import FastMCP

from src.agents.monitor_agent import MonitorAgent
from src.contracts.store import build_contract_store
from src.services.reliability_service import ReliabilityService
from src.workflows.hitl_contract_workflow import HITLContractWorkflow


def _build_service() -> ReliabilityService:
    """Build the default runtime service used by MCP tools."""
    contract_store = build_contract_store(os.getenv("CONTRACTS_PATH", "config/expectations"))
    agent = MonitorAgent(
        contracts_path=str(contract_store.root_path),
        lineage_path="config/lineage.yaml",
        contract_store=contract_store,
    )
    hitl_workflow = HITLContractWorkflow(agent=agent, contract_store=contract_store)
    return ReliabilityService(
        agent=agent,
        contract_store=contract_store,
        hitl_workflow=hitl_workflow,
    )


def _normalize_error(exc: Exception) -> RuntimeError:
    if isinstance(exc, HTTPException):
        return RuntimeError(f"{exc.status_code}: {exc.detail}")
    return RuntimeError(str(exc))


def create_mcp_server(
    service: Optional[ReliabilityService] = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8001,
    streamable_http_path: str = "/mcp",
) -> FastMCP:
    """
    Create a FastMCP server exposing DataPulse reliability operations as tools.
    """
    runtime = service or _build_service()

    mcp = FastMCP(
        name="DataPulse Reliability MCP",
        instructions=(
            "Data reliability tool server for dataset evaluation, contract HITL workflows, "
            "run history/verdict retrieval, SLO summaries, anomaly investigation, and copilot chat."
        ),
        host=host,
        port=port,
        streamable_http_path=streamable_http_path,
    )

    @mcp.tool(
        name="evaluate_dataset",
        description="Evaluate the latest discovered file for a dataset through the reliability pipeline.",
    )
    def evaluate_dataset(dataset_name: str) -> Dict[str, Any]:
        try:
            return runtime.evaluate_dataset(dataset_name)
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="propose_contract",
        description="Generate a proposed contract YAML from dataset data.",
    )
    def propose_contract(dataset_name: str, file_path: Optional[str] = None) -> Dict[str, Any]:
        try:
            return runtime.propose_contract(dataset_name=dataset_name, file_path=file_path)
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="run_contract_gate",
        description="Run contract CI gate (schema + profile) without downstream load side effects.",
    )
    def run_contract_gate(dataset_name: str, file_path: Optional[str] = None) -> Dict[str, Any]:
        try:
            return runtime.run_contract_gate(dataset_name=dataset_name, file_path=file_path)
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="generate_contract_autopilot",
        description="Generate confidence-scored contract recommendations with rationale and proposed YAML.",
    )
    def generate_contract_autopilot(
        dataset_name: str,
        file_path: Optional[str] = None,
        confidence_threshold: float = 0.75,
    ) -> Dict[str, Any]:
        try:
            return runtime.generate_autopilot_contract(
                dataset_name=dataset_name,
                file_path=file_path,
                confidence_threshold=confidence_threshold,
            )
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="approve_contract",
        description="Approve a contract YAML and validate pending files for that dataset.",
    )
    def approve_contract(dataset_name: str, approved_yaml: str) -> Dict[str, Any]:
        try:
            return runtime.approve_contract(dataset_name=dataset_name, approved_yaml=approved_yaml)
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="get_pending_contracts",
        description="List datasets waiting for contract approval with proposed YAML and pending files.",
    )
    def get_pending_contracts() -> Dict[str, Any]:
        try:
            return {"pending_contracts": runtime.get_pending_contracts()}
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="get_run_verdict",
        description="Get full verdict payload for a run ID.",
    )
    def get_run_verdict(run_id: str) -> Dict[str, Any]:
        try:
            return runtime.get_run_verdict(run_id)
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="get_run_history",
        description="Get run history for a dataset.",
    )
    def get_run_history(dataset_name: str, limit: int = 50) -> Dict[str, Any]:
        try:
            return {
                "dataset_name": dataset_name,
                "runs": runtime.get_run_history(dataset_name=dataset_name, limit=limit),
            }
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="compare_runs",
        description="Compare two run IDs and return quality/anomaly deltas and regression flags.",
    )
    def compare_runs(run_id_1: str, run_id_2: str) -> Dict[str, Any]:
        try:
            return runtime.compare_runs(run_id_1=run_id_1, run_id_2=run_id_2)
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="investigate_anomaly",
        description=(
            "Investigate a dataset anomaly with root-cause hypothesis, evidence, "
            "metric trend context, and incident state."
        ),
    )
    def investigate_anomaly(
        dataset_name: str,
        metric: Optional[str] = None,
        run_id: Optional[str] = None,
        history_window: int = 30,
    ) -> Dict[str, Any]:
        try:
            return runtime.investigate_anomaly(
                dataset_name=dataset_name,
                metric=metric,
                run_id=run_id,
                history_window=history_window,
            )
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="explain_quality",
        description="Explain quality score breakdown (dimensions, weak columns, and top violations).",
    )
    def explain_quality(dataset_name: str, run_id: Optional[str] = None) -> Dict[str, Any]:
        try:
            return runtime.explain_quality(dataset_name=dataset_name, run_id=run_id)
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="generate_ai_brief",
        description=(
            "Generate a concise AI reliability brief combining investigation, quality drivers, "
            "lineage impact, risk ranking, SLO posture, and remediation guidance."
        ),
    )
    def generate_ai_brief(dataset_name: str, run_id: Optional[str] = None) -> Dict[str, Any]:
        try:
            return runtime.generate_ai_brief(dataset_name=dataset_name, run_id=run_id)
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="list_datasets_by_risk",
        description="Rank datasets by reliability risk score using status, quality, anomalies, criticality, and open incidents.",
    )
    def list_datasets_by_risk(limit: int = 20) -> Dict[str, Any]:
        try:
            return runtime.list_datasets_by_risk(limit=limit)
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="get_diagnostics_records",
        description="Retrieve diagnostics warehouse records for a dataset/run.",
    )
    def get_diagnostics_records(
        dataset_name: str,
        run_id: Optional[str] = None,
        check_type: Optional[str] = None,
        limit: int = 200,
    ) -> Dict[str, Any]:
        try:
            return runtime.get_diagnostics_records(
                dataset_name=dataset_name,
                run_id=run_id,
                check_type=check_type,
                limit=limit,
            )
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="get_outcome_metrics",
        description="Get reliability outcome metrics (pass rates, MTTR, and dataset coverage).",
    )
    def get_outcome_metrics(days: int = 30) -> Dict[str, Any]:
        try:
            return runtime.get_outcome_metrics(days=days)
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="get_workflow_timeline",
        description=(
            "Get a unified timeline of workflow activity across runs, jobs, audit actions, "
            "incidents, and tool outputs."
        ),
    )
    def get_workflow_timeline(dataset_name: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        try:
            return runtime.get_workflow_timeline(dataset_name=dataset_name, limit=limit)
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="get_agentic_workflow_graph",
        description="Return the current LangGraph mermaid diagram for the agentic workflow.",
    )
    def get_agentic_workflow_graph() -> Dict[str, Any]:
        try:
            return runtime.get_agentic_workflow_graph()
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="run_agentic_reliability_loop",
        description=(
            "Run investigation -> root-cause hypothesis -> remediation proposal -> "
            "confidence/policy-gated execution. Auto execution stays blocked unless "
            "confidence threshold and policy controls are satisfied."
        ),
    )
    def run_agentic_reliability_loop(
        dataset_name: str,
        metric: Optional[str] = None,
        auto_execute: bool = False,
        confidence_threshold: float = 0.8,
        policy_approved: bool = False,
        policy_reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            return runtime.run_agentic_reliability_loop(
                dataset_name=dataset_name,
                metric=metric,
                auto_execute=auto_execute,
                confidence_threshold=confidence_threshold,
                policy_approved=policy_approved,
                policy_reason=policy_reason,
            )
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="get_slo_summary",
        description="Get SLO attainment summary for a dataset.",
    )
    def get_slo_summary(dataset_name: str, window: int = 200) -> Dict[str, Any]:
        try:
            return runtime.get_slo_summary(dataset_name=dataset_name, window=window)
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="chat_with_copilot",
        description="Ask the reliability copilot a question with live platform context.",
    )
    def chat_with_copilot(query: str) -> Dict[str, Any]:
        try:
            return runtime.chat_with_copilot(query=query)
        except Exception as exc:
            raise _normalize_error(exc)

    @mcp.tool(
        name="delete_dataset",
        description=(
            "Delete dataset artifacts and related records. "
            "Set confirm=true to execute this destructive operation."
        ),
    )
    def delete_dataset(dataset_name: str, confirm: bool = False) -> Dict[str, Any]:
        if not confirm:
            raise RuntimeError("Refusing destructive operation. Re-run with confirm=true.")

        try:
            return runtime.delete_dataset(dataset_name=dataset_name)
        except Exception as exc:
            raise _normalize_error(exc)

    return mcp


def create_streamable_http_app(
    service: Optional[ReliabilityService] = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8001,
    streamable_http_path: str = "/mcp",
):
    """Build a Starlette app for Streamable HTTP MCP hosting."""
    server = create_mcp_server(
        service=service,
        host=host,
        port=port,
        streamable_http_path=streamable_http_path,
    )
    return server.streamable_http_app()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run DataPulse MCP server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "streamable-http"],
        default="streamable-http",
        help="MCP transport mode.",
    )
    parser.add_argument("--host", default=os.getenv("MCP_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MCP_PORT", "8001")))
    parser.add_argument(
        "--path",
        default=os.getenv("MCP_PATH", "/mcp"),
        help="Streamable HTTP mount path (used for streamable-http transport).",
    )
    args = parser.parse_args()

    server = create_mcp_server(
        host=args.host,
        port=args.port,
        streamable_http_path=args.path,
    )

    if args.transport == "stdio":
        server.run(transport="stdio")
    else:
        server.run(transport="streamable-http")


if __name__ == "__main__":
    main()

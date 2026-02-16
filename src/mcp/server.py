from __future__ import annotations

import argparse
import os
from typing import Any, Dict, Optional

from fastapi import HTTPException
from mcp.server.fastmcp import FastMCP

from src.agents.monitor_agent import MonitorAgent
from src.contracts.store import FileContractStore
from src.services.reliability_service import ReliabilityService


def _build_service() -> ReliabilityService:
    """Build the default runtime service used by MCP tools."""
    contract_store = FileContractStore(os.getenv("CONTRACTS_PATH", "config/expectations"))
    agent = MonitorAgent(
        contracts_path=str(contract_store.root_path),
        lineage_path="config/lineage.yaml",
        contract_store=contract_store,
    )
    return ReliabilityService(agent=agent, contract_store=contract_store)


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
            "run history/verdict retrieval, SLO summaries, and copilot chat."
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

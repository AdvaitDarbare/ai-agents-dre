from __future__ import annotations

import os
from typing import Any, Tuple

from src.agents.monitor_agent import MonitorAgent
from src.contracts.store import build_contract_store
from src.services.reliability_service import ReliabilityService
from src.workflows.agentic_reliability_workflow import AgenticReliabilityWorkflow
from src.workflows.hitl_contract_workflow import HITLContractWorkflow


def _build_runtime() -> Tuple[ReliabilityService, HITLContractWorkflow]:
    """
    Build a Studio runtime that mirrors API/MCP wiring.
    """
    contract_store = build_contract_store(os.getenv("CONTRACTS_PATH", "config/expectations"))
    agent = MonitorAgent(
        contracts_path=str(contract_store.root_path),
        lineage_path="config/lineage.yaml",
        contract_store=contract_store,
    )
    hitl_workflow = HITLContractWorkflow(agent=agent, contract_store=contract_store)
    service = ReliabilityService(
        agent=agent,
        contract_store=contract_store,
        hitl_workflow=hitl_workflow,
        audit_service=None,
    )
    return service, hitl_workflow


_service, _hitl_workflow = _build_runtime()

# Studio graph: deterministic investigation/remediation loop.
agentic_graph: Any = AgenticReliabilityWorkflow(service=_service)._build_graph()

# Studio graph: evaluate + HITL contract path graph without external checkpointer.
hitl_graph: Any = _hitl_workflow._build_graph(checkpointer=None, include_existing_evaluation=True)

__all__ = ["agentic_graph", "hitl_graph"]

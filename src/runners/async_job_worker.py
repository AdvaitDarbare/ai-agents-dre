"""
External async job worker.

Runs queued jobs from PostgreSQL using the same ReliabilityService logic as API,
without relying on in-process ThreadPool execution.

Usage:
    ASYNC_JOB_EXECUTION_MODE=external_worker python -m src.runners.async_job_worker
"""

from __future__ import annotations

import os
import time

from src.agents.monitor_agent import MonitorAgent
from src.contracts.store import build_contract_store
from src.services.action_audit_service import ActionAuditService
from src.services.async_job_service import AsyncJobService
from src.services.reliability_service import ReliabilityService
from src.workflows.hitl_contract_workflow import HITLContractWorkflow


def build_worker_service() -> AsyncJobService:
    contract_store = build_contract_store(os.getenv("CONTRACTS_PATH", "config/expectations"))
    agent = MonitorAgent(
        contracts_path=str(contract_store.root_path),
        lineage_path="config/lineage.yaml",
        contract_store=contract_store,
    )
    audit_service = ActionAuditService()
    workflow = HITLContractWorkflow(agent=agent, contract_store=contract_store)
    reliability_service = ReliabilityService(
        agent=agent,
        contract_store=contract_store,
        hitl_workflow=workflow,
        audit_service=audit_service,
    )
    return AsyncJobService(
        reliability_service=reliability_service,
        max_workers=int(os.getenv("ASYNC_JOB_MAX_WORKERS", "4")),
        max_queued_jobs=int(os.getenv("ASYNC_JOB_MAX_QUEUED", "100")),
        audit_service=audit_service,
    )


def run_forever() -> None:
    service = build_worker_service()
    poll_seconds = max(0.2, float(os.getenv("ASYNC_JOB_WORKER_POLL_SECONDS", "1.0")))
    actions_env = os.getenv("ASYNC_JOB_WORKER_ACTIONS", "").strip()
    actions = [item.strip().lower() for item in actions_env.split(",") if item.strip()] or None

    print("╔══════════════════════════════════════════════════════════════╗")
    print("║                 DRE Async Job Worker                        ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print(f"Execution mode: {service.execution_mode}")
    print(f"Poll interval: {poll_seconds}s")
    print(f"Actions filter: {actions if actions else 'ALL'}")

    try:
        while True:
            processed = service.run_worker_once(actions=actions)
            if not processed:
                time.sleep(poll_seconds)
    except KeyboardInterrupt:
        print("\nStopping async job worker.")


if __name__ == "__main__":
    run_forever()

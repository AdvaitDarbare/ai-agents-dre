from fastapi import FastAPI, HTTPException, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import os
import json
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import shutil
import uuid

# Import our MonitorAgent
from src.agents.monitor_agent import MonitorAgent
from src.contracts.store import build_contract_store
from src.services.async_job_service import AsyncJobService
from src.services.action_audit_service import ActionAuditService
from src.services.incident_service import IncidentService
from src.services.policy_service import PolicyService
from src.services.rbac_service import RBACService
from src.services.reliability_service import ReliabilityService
from src.utils.database import get_connection, init_tables
from src.workflows.hitl_contract_workflow import HITLContractWorkflow
from src.tools.monitor_backtesting import MonitorBacktestingHarness

load_dotenv()

class RemediationApplyRequest(BaseModel):
    dataset_name: str
    proposed_yaml: str
    error_context: str
    policy_approved: bool = False
    policy_reason: Optional[str] = None

class GovernanceRollbackRequest(BaseModel):
    dataset_name: str
    filename: str

class ContractProposeRequest(BaseModel):
    dataset_name: str
    file_path: Optional[str] = None

class ContractGateRequest(BaseModel):
    dataset_name: str
    file_path: Optional[str] = None

class ContractAutopilotRequest(BaseModel):
    dataset_name: str
    file_path: Optional[str] = None
    confidence_threshold: float = 0.75

class ContractApprovalRequest(BaseModel):
    dataset_name: str
    approved_yaml: str  # The human-reviewed YAML content

class ChatRequest(BaseModel):
    query: Optional[str] = None
    context: Dict[str, Any] = Field(default_factory=dict)


class AsyncDeleteRequest(BaseModel):
    confirm: bool = False
    policy_approved: bool = False
    policy_reason: Optional[str] = None


class AsyncBulkDeleteRequest(BaseModel):
    dataset_names: List[str]
    confirm: bool = False
    policy_approved: bool = False
    policy_reason: Optional[str] = None


class AsyncEvaluateRequest(BaseModel):
    force_load: bool = False

class AsyncBulkEvaluateRequest(BaseModel):
    dataset_names: List[str]
    force_load: bool = False

class AsyncEvaluateAllRequest(BaseModel):
    include_unconfigured: bool = True
    force_load: bool = False


class IncidentUpdateRequest(BaseModel):
    status: str
    owner: Optional[str] = None
    note: Optional[str] = None


class PolicyCheckRequest(BaseModel):
    action: str
    dataset_name: Optional[str] = None
    dataset_names: Optional[List[str]] = None


class AgenticLoopRequest(BaseModel):
    dataset_name: str
    metric: Optional[str] = None
    auto_execute: bool = False
    confidence_threshold: float = 0.8
    policy_approved: bool = False
    policy_reason: Optional[str] = None


class AgenticRemediationRequest(BaseModel):
    dataset_name: str
    max_retries: int = 2
    autonomy_mode: str = "full_auto"


class RuntimeResetRequest(BaseModel):
    confirm_phrase: str
    clear_generated_contracts: bool = True
    preserve_contracts: List[str] = Field(default_factory=list)
    clear_langgraph_checkpoints: bool = True

app = FastAPI(title="Agentic DRE API")

# Enable CORS for frontend clients (Next.js dashboard, local tools)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, restrict this to your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global instances (simplified for demo)
contract_store = build_contract_store(os.getenv("CONTRACTS_PATH", "config/expectations"))
agent = MonitorAgent(
    contracts_path=str(contract_store.root_path),
    lineage_path="config/lineage.yaml",
    contract_store=contract_store,
)
audit_service = ActionAuditService()
hitl_workflow = HITLContractWorkflow(agent=agent, contract_store=contract_store)
service = ReliabilityService(
    agent=agent,
    contract_store=contract_store,
    hitl_workflow=hitl_workflow,
    audit_service=audit_service,
)
async_jobs = AsyncJobService(
    reliability_service=service,
    max_workers=int(os.getenv("ASYNC_JOB_MAX_WORKERS", "4")),
    max_queued_jobs=int(os.getenv("ASYNC_JOB_MAX_QUEUED", "100")),
    audit_service=audit_service,
)
incident_service = IncidentService()
policy_service = PolicyService(agent=agent)
rbac_service = RBACService()
backtesting_harness = MonitorBacktestingHarness()

def _audit(action: str, *, dataset_name: Optional[str] = None, status: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
    try:
        audit_service.record(
            action=action,
            dataset_name=dataset_name,
            status=status,
            actor="user",
            source="api",
            metadata=metadata or {},
        )
    except Exception:
        # Never break request execution due to audit logging.
        return


def _extract_latest_user_query(messages: List[Dict[str, Any]]) -> str:
    for message in reversed(messages):
        if str(message.get("role", "")).lower() != "user":
            continue

        content = message.get("content")
        if isinstance(content, str) and content.strip():
            return content.strip()

        parts = message.get("parts")
        if isinstance(parts, list):
            text_bits = []
            for part in parts:
                if not isinstance(part, dict):
                    continue
                if part.get("type") == "text":
                    piece = part.get("text")
                    if isinstance(piece, str) and piece.strip():
                        text_bits.append(piece.strip())
            if text_bits:
                return " ".join(text_bits).strip()

    return ""


def _enforce_role(request: Request, permission: str) -> None:
    role = request.headers.get("x-dre-role")
    rbac_service.enforce(role, permission)

@app.get("/health")
def health_check():
    return {"status": "operational", "timestamp": datetime.now().isoformat()}


@app.post("/policy/check")
def check_policy(request: PolicyCheckRequest):
    """Evaluate policy decision for an action without executing it."""
    try:
        return policy_service.evaluate_action(
            action=request.action,
            dataset_name=request.dataset_name,
            dataset_names=request.dataset_names,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/audit")
def list_audit_actions(
    limit: int = 100,
    action: Optional[str] = None,
    dataset_name: Optional[str] = None,
    status: Optional[str] = None,
    incident_id: Optional[str] = None,
    job_id: Optional[str] = None,
    run_id: Optional[str] = None,
):
    """List audit log actions for operators/agents."""
    try:
        return audit_service.list_actions(
            limit=limit,
            action=action,
            dataset_name=dataset_name,
            status=status,
            incident_id=incident_id,
            job_id=job_id,
            run_id=run_id,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/audit/summary")
def audit_summary(
    window_minutes: int = 60,
    action: Optional[str] = None,
    dataset_name: Optional[str] = None,
    status: Optional[str] = None,
):
    """Summarize audit actions grouped by action+status over a recent time window."""
    try:
        return audit_service.summarize(
            window_minutes=window_minutes,
            action=action,
            dataset_name=dataset_name,
            status=status,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/datasets")
def get_datasets():
    """Discover and return all dataset metadata."""
    try:
        datasets = agent.discover_datasets()
        return datasets
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pulse")
def get_pulse():
    """Get the current health status of all datasets."""
    try:
        discovered = agent.discover_datasets()
        discovered_by_name = {str(d.get("name")): d for d in discovered if d.get("name")}

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT dataset_name, last_status, criticality, lifecycle,
                           last_file_mtime, last_scanned, scan_count
                    FROM dataset_registry
                """)
                rows = cur.fetchall()

                results_by_name = {}
                for r in rows:
                    name = r[0]
                    history = agent.get_run_history(dataset_name=name, limit=10)
                    results_by_name[name] = {
                        "name": name,
                        "status": r[1] or "UNKNOWN",
                        "criticality": r[2] or "UNKNOWN",
                        "lifecycle": r[3] or discovered_by_name.get(name, {}).get("lifecycle", "unknown"),
                        "last_mtime": r[4],
                        "last_scanned": r[5].isoformat() if r[5] else None,
                        "scan_count": int(r[6] or 0),
                        "history": [h["quality_score"] for h in reversed(history)],
                        "quality_score": history[0]["quality_score"] if history else None,
                        "reason": history[0].get("reason", "") if history else ""
                    }

                # Include discovered datasets not yet in registry (no scan yet)
                for name, ds in discovered_by_name.items():
                    if name in results_by_name:
                        continue
                    results_by_name[name] = {
                        "name": name,
                        "status": "UNKNOWN",
                        "criticality": ds.get("criticality", "UNKNOWN"),
                        "lifecycle": ds.get("lifecycle", "unknown"),
                        "last_mtime": None,
                        "last_scanned": None,
                        "scan_count": 0,
                        "history": [],
                        "quality_score": None,
                        "reason": "No scans run yet",
                    }

                return sorted(results_by_name.values(), key=lambda item: item["name"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/scan-all")
def run_scan_all(request: Request, payload: AsyncEvaluateAllRequest = Body(default_factory=AsyncEvaluateAllRequest)):
    """Evaluate all configured datasets (sync)."""
    try:
        _enforce_role(request, "evaluate")
        all_meta = service.agent.discover_datasets(include_unconfigured=payload.include_unconfigured)
        names = [d["name"] for d in all_meta]
        return service.bulk_evaluate_datasets(names, force_load=bool(payload.force_load))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/evaluate/{dataset_name}")
def evaluate_dataset(dataset_name: str, request: Request, payload: AsyncEvaluateRequest = Body(default_factory=AsyncEvaluateRequest)):
    """Trigger a health check for a specific dataset."""
    try:
        _enforce_role(request, "evaluate")
        return service.evaluate_dataset(dataset_name, force_load=bool(payload.force_load))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/jobs/evaluate/{dataset_name}")
def enqueue_evaluate_dataset(dataset_name: str, request: Request, payload: AsyncEvaluateRequest = Body(default_factory=AsyncEvaluateRequest)):
    """Queue a background health check job for a dataset."""
    try:
        _enforce_role(request, "jobs.evaluate")
        job = async_jobs.submit_evaluate(dataset_name, force_load=bool(payload.force_load))
        _audit("job_evaluate_requested", dataset_name=dataset_name, status="QUEUED", metadata={"job_id": job.get("job_id")})
        return job
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/jobs/delete/{dataset_name}")
def enqueue_delete_dataset(dataset_name: str, request: AsyncDeleteRequest, http_request: Request):
    """
    Queue a background hard-delete job for a dataset.
    Requires confirm=true because this is destructive.
    """
    if not request.confirm:
        raise HTTPException(status_code=400, detail="confirm=true is required for delete jobs")

    try:
        _enforce_role(http_request, "jobs.delete")
        decision = policy_service.evaluate_action(action="delete", dataset_name=dataset_name)
        policy_service.enforce(
            decision,
            approved=bool(request.policy_approved),
            reason=request.policy_reason,
        )
        job = async_jobs.submit_delete(dataset_name)
        _audit("job_delete_requested", dataset_name=dataset_name, status="QUEUED", metadata={"job_id": job.get("job_id")})
        return job
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/jobs/delete-bulk")
def enqueue_bulk_delete(request: AsyncBulkDeleteRequest, http_request: Request):
    """
    Queue a background bulk-delete job for multiple datasets.
    Requires confirm=true because this is destructive.
    """
    if not request.confirm:
        raise HTTPException(status_code=400, detail="confirm=true is required for bulk delete jobs")

    try:
        _enforce_role(http_request, "jobs.bulk_delete")
        decision = policy_service.evaluate_action(
            action="bulk_delete",
            dataset_names=request.dataset_names,
        )
        policy_service.enforce(
            decision,
            approved=bool(request.policy_approved),
            reason=request.policy_reason,
        )
        job = async_jobs.submit_bulk_delete(request.dataset_names)
        _audit(
            "job_bulk_delete_requested",
            status="QUEUED",
            metadata={"job_id": job.get("job_id"), "count": len(request.dataset_names or [])},
        )
        return job
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/jobs/bulk-evaluate")
def enqueue_bulk_evaluate(request: Request, payload: AsyncBulkEvaluateRequest):
    """Queue a background health check job for multiple datasets."""
    try:
        _enforce_role(request, "jobs.evaluate")
        job = async_jobs.submit_bulk_evaluate(payload.dataset_names, force_load=bool(payload.force_load))
        _audit(
            "job_bulk_evaluate_requested",
            status="QUEUED",
            metadata={"job_id": job.get("job_id"), "count": len(payload.dataset_names or [])},
        )
        return job
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/jobs/evaluate-all")
def enqueue_evaluate_all(http_request: Request, request: AsyncEvaluateAllRequest = Body(default_factory=AsyncEvaluateAllRequest)):
    """Queue a background bulk-evaluate job for all discovered datasets."""
    try:
        _enforce_role(http_request, "jobs.evaluate_all")
        discovered = agent.discover_datasets()
        dataset_names = []
        for item in discovered:
            try:
                name = str(item.get("name") or "").strip()
            except Exception:
                name = ""
            if not name:
                continue
            if not request.include_unconfigured and str(item.get("lifecycle") or "").lower() == "unconfigured":
                continue
            if not item.get("data_file") and not item.get("connector_name"):
                continue
            dataset_names.append(name)

        if not dataset_names:
            raise HTTPException(status_code=400, detail="No datasets discovered with data files to evaluate")

        job = async_jobs.submit_bulk_evaluate(dataset_names)
        _audit(
            "job_evaluate_all_requested",
            status="QUEUED",
            metadata={"job_id": job.get("job_id"), "count": len(dataset_names), "include_unconfigured": bool(request.include_unconfigured)},
        )
        return job
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/jobs/remediation/apply")
def enqueue_apply_remediation(request: RemediationApplyRequest, http_request: Request):
    """Queue a background remediation apply job."""
    try:
        _enforce_role(http_request, "jobs.remediation_apply")
        decision = policy_service.evaluate_action(
            action="remediation_apply",
            dataset_name=request.dataset_name,
        )
        policy_service.enforce(
            decision,
            approved=bool(request.policy_approved),
            reason=request.policy_reason,
        )
        job = async_jobs.submit_apply_remediation(
            dataset_name=request.dataset_name,
            proposed_yaml=request.proposed_yaml,
            error_context=request.error_context,
        )
        _audit(
            "job_remediation_apply_requested",
            dataset_name=request.dataset_name,
            status="QUEUED",
            metadata={"job_id": job.get("job_id")},
        )
        return job
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jobs")
def list_async_jobs(
    limit: int = 50,
    status: Optional[str] = None,
    action: Optional[str] = None,
    dataset_name: Optional[str] = None,
):
    """List async background jobs with optional filters."""
    try:
        return async_jobs.list_jobs(limit=limit, status=status, action=action, dataset_name=dataset_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jobs/{job_id}")
def get_async_job(job_id: str):
    """Get current status/details of a background job."""
    try:
        return async_jobs.get_job(job_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/datasets/{dataset_name}/data")
def get_dataset_data(dataset_name: str, limit: int = 100):
    """Get a sample/preview of the dataset content."""
    try:
        data = agent.get_dataset_sample(dataset_name, limit)
        return data
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Dataset file not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/datasets/{dataset_name}")
def delete_dataset(
    dataset_name: str,
    request: Request,
    policy_approved: bool = False,
    policy_reason: Optional[str] = None,
):
    """
    Hard-delete a dataset and all known local artifacts:
    - Data files/logs/verdicts
    - Contract/proposal/history YAML files
    - PostgreSQL rows across dataset-scoped tables
    - DuckDB tables in local *.duckdb/*.db files (if present)
    """
    try:
        _enforce_role(request, "dataset.delete")
        decision = policy_service.evaluate_action(action="delete", dataset_name=dataset_name)
        policy_service.enforce(
            decision,
            approved=bool(policy_approved),
            reason=policy_reason,
        )
        result = service.delete_dataset(dataset_name)
        _audit("dataset_deleted_sync", dataset_name=dataset_name, status="COMPLETED")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/runs")
def get_recent_runs(limit: int = 50):
    """Get recent run history across all datasets."""
    try:
        rows = agent.get_run_history(dataset_name=None, limit=limit)
        results = []
        for i, row in enumerate(rows):
            duration_ms = row.get("duration_ms") or 0
            results.append({
                "id": f"run-{i}-{row.get('dataset')}",
                "dataset": row.get("dataset"),
                "status": row.get("status"),
                "timestamp": row.get("timestamp"),
                "duration": f"{duration_ms / 1000:.1f}s" if duration_ms else None,
                "quality_score": row.get("quality_score"),
                "reason": row.get("reason"),
            })
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/history/{dataset_name}")
def get_history(dataset_name: str, limit: int = 50):
    """Get run history for a dataset."""
    try:
        return service.get_run_history(dataset_name, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/verdict/{run_id}")
def get_full_verdict(run_id: str):
    """Get the full verdict with all tool outputs for a specific run."""
    try:
        return service.get_run_verdict(run_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/chat")
def chat_with_copilot(query: Optional[str] = None, payload: Optional[ChatRequest] = None):
    """Interact with the Agent reasoning engine (query param or JSON body)."""
    try:
        effective_query = query or (payload.query if payload else None)
        if not effective_query:
            raise HTTPException(status_code=400, detail="query is required")
        return service.chat_with_copilot(effective_query, context=(payload.context if payload else None))
    except Exception as e:
        print(f"Chat Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/chat/stream")
async def chat_with_copilot_stream(payload: Dict[str, Any] = Body(default_factory=dict)):
    """
    Streaming chat endpoint compatible with AI SDK useChat + TextStreamChatTransport.
    Expects AI SDK-style payload with a `messages` array.
    """
    try:
        messages = payload.get("messages", [])
        if not isinstance(messages, list):
            raise HTTPException(status_code=400, detail="messages must be a list")

        query = _extract_latest_user_query(messages)
        if not query:
            raise HTTPException(status_code=400, detail="No user message found")

        response_payload = service.chat_with_copilot(query)
        full_text = str(response_payload.get("response", "")).strip()
        if not full_text:
            full_text = "No response generated."

        async def stream_text():
            # Emit in small chunks to provide incremental UI updates.
            words = full_text.split(" ")
            for i, token in enumerate(words):
                suffix = " " if i < len(words) - 1 else ""
                yield f"{token}{suffix}"
                await asyncio.sleep(0.005)

        return StreamingResponse(
            stream_text(),
            media_type="text/plain; charset=utf-8",
        )
    except HTTPException:
        raise
    except Exception as e:
        print(f"Chat Stream Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/lineage")
def get_lineage(dataset: Optional[str] = None, depth: int = 2):
    """Get the dependency graph, optionally filtered by dataset."""
    try:
        full_graph = agent.impact_analyzer.refresh()
        lineage_meta = agent.impact_analyzer.summarize_lineage(full_graph)
        if not dataset:
            discovered = agent.discover_datasets() or []
            discovered_names = {
                str(item.get("name"))
                for item in discovered
                if isinstance(item, dict) and item.get("name")
            }

            datasets_map = full_graph.get("datasets", {}) if isinstance(full_graph, dict) else {}
            if isinstance(datasets_map, dict) and discovered_names:
                filtered_datasets = {
                    name: info
                    for name, info in datasets_map.items()
                    if str(name) in discovered_names
                }
                full_graph = dict(full_graph)
                full_graph["datasets"] = filtered_datasets
                lineage_meta = agent.impact_analyzer.summarize_lineage(full_graph)

            payload = dict(full_graph or {})
            payload["summary"] = lineage_meta.get("summary", {})
            payload["issues"] = lineage_meta.get("issues", {})
            payload["graph"] = lineage_meta.get("graph", {})
            return payload
            
        # Filter for specific dataset context
        if dataset not in full_graph.get("datasets", {}):
            empty_meta = agent.impact_analyzer.summarize_lineage({"datasets": {}})
            return {
                "datasets": {},
                "summary": empty_meta.get("summary", {}),
                "issues": empty_meta.get("issues", {}),
                "graph": empty_meta.get("graph", {}),
                "context": {"dataset": dataset, "upstream": [], "downstream": [], "max_depth": max(1, min(int(depth), 5))},
            }

        subset = {"datasets": {}}
        datasets_map = full_graph.get("datasets", {}) if isinstance(full_graph, dict) else {}
        target_node = datasets_map[dataset]
        if not isinstance(target_node, dict):
            target_node = {}
        subset["datasets"][dataset] = target_node

        context = agent.impact_analyzer.get_lineage_context(dataset, max_depth=depth, graph=full_graph)
        for item in context.get("upstream", []):
            name = str(item.get("name") or "")
            if name and name in datasets_map:
                subset["datasets"][name] = datasets_map[name]
        for item in context.get("downstream", []):
            name = str(item.get("name") or "")
            if name and name in datasets_map:
                subset["datasets"][name] = datasets_map[name]

        subset_meta = agent.impact_analyzer.summarize_lineage(subset)
        subset["summary"] = subset_meta.get("summary", {})
        subset["issues"] = subset_meta.get("issues", {})
        subset["graph"] = subset_meta.get("graph", {})
        subset["context"] = context
        return subset
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/risk/datasets")
def list_datasets_by_risk(limit: int = 20):
    """Rank datasets by reliability risk score."""
    try:
        return service.list_datasets_by_risk(limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics/outcomes")
def get_outcome_metrics(days: int = 30):
    """Reliability outcome metrics (pass rates, incident MTTR, and coverage)."""
    try:
        return service.get_outcome_metrics(days=days)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/workflow/timeline")
def get_workflow_timeline(dataset_name: Optional[str] = None, limit: int = 100):
    """Unified workflow visibility feed (runs, tools, jobs, incidents, and audit actions)."""
    try:
        return service.get_workflow_timeline(dataset_name=dataset_name, limit=limit)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/workflow/timeline/stream")
async def stream_workflow_timeline(
    dataset_name: Optional[str] = None,
    limit: int = 100,
    interval_ms: int = 3000,
):
    """Server-sent events stream for live workflow timeline updates."""
    safe_interval_ms = max(1000, min(int(interval_ms), 30000))

    async def event_stream():
        last_payload = None
        while True:
            try:
                payload = service.get_workflow_timeline(dataset_name=dataset_name, limit=limit)
                serialized = json.dumps(payload, default=str, sort_keys=True)

                if serialized != last_payload:
                    yield f"event: timeline\ndata: {serialized}\n\n"
                    last_payload = serialized
                else:
                    yield "event: heartbeat\ndata: {}\n\n"

                await asyncio.sleep(safe_interval_ms / 1000.0)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                error_payload = json.dumps({"error": str(exc)}, default=str)
                yield f"event: error\ndata: {error_payload}\n\n"
                await asyncio.sleep(safe_interval_ms / 1000.0)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/workflow/agentic/graph")
def get_agentic_workflow_graph():
    """Return the LangGraph diagram for the agentic investigation/remediation workflow."""
    try:
        return service.get_agentic_workflow_graph()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/workflow/agentic/run")
def run_agentic_workflow(request: AgenticLoopRequest, http_request: Request):
    """Run the agentic investigation/remediation loop with strict policy controls."""
    try:
        _enforce_role(http_request, "jobs.remediation_apply")
        return service.run_agentic_reliability_loop(
            dataset_name=request.dataset_name,
            metric=request.metric,
            auto_execute=request.auto_execute,
            confidence_threshold=request.confidence_threshold,
            policy_approved=request.policy_approved,
            policy_reason=request.policy_reason,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/workflow/agentic/remediate")
def run_agentic_contract_remediation(request: AgenticRemediationRequest, http_request: Request):
    """Run deterministic full-auto contract remediation loop for latest WARNING/BLOCKED runs."""
    try:
        _enforce_role(http_request, "jobs.remediation_apply")
        return service.run_agentic_contract_remediation(
            dataset_name=request.dataset_name,
            max_retries=request.max_retries,
            autonomy_mode=request.autonomy_mode,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/workflow/agentic/remediate/{remediation_run_id}")
def get_agentic_contract_remediation(remediation_run_id: str):
    """Get persisted run/attempt timeline for an auto-remediation execution."""
    try:
        return service.get_agentic_remediation_run(remediation_run_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/workflow/agentic/remediate/{remediation_run_id}/stream")
async def stream_agentic_contract_remediation(
    remediation_run_id: str,
    interval_ms: int = 2000,
):
    """SSE stream for live state updates of a remediation run."""
    safe_interval_ms = max(1000, min(int(interval_ms), 30000))
    terminal = {"AUTO_FIXED", "PLAN_REQUIRED", "BLOCKED_BY_POLICY", "FAILED"}

    async def event_stream():
        last_payload = None
        while True:
            try:
                payload = service.get_agentic_remediation_run(remediation_run_id)
                serialized = json.dumps(payload, default=str, sort_keys=True)
                if serialized != last_payload:
                    yield f"event: remediation\ndata: {serialized}\n\n"
                    last_payload = serialized
                else:
                    yield "event: heartbeat\ndata: {}\n\n"

                if str(payload.get("status") or "").upper() in terminal:
                    break
                await asyncio.sleep(safe_interval_ms / 1000.0)
            except asyncio.CancelledError:
                break
            except HTTPException as exc:
                error_payload = json.dumps({"error": exc.detail}, default=str)
                yield f"event: error\ndata: {error_payload}\n\n"
                break
            except Exception as exc:
                error_payload = json.dumps({"error": str(exc)}, default=str)
                yield f"event: error\ndata: {error_payload}\n\n"
                await asyncio.sleep(safe_interval_ms / 1000.0)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/ai/brief/{dataset_name}")
def get_ai_brief(dataset_name: str, run_id: Optional[str] = None):
    """Generate a concise AI reliability brief for a dataset."""
    try:
        return service.generate_ai_brief(dataset_name=dataset_name, run_id=run_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/diagnostics/{dataset_name}")
def get_diagnostics(
    dataset_name: str,
    run_id: Optional[str] = None,
    check_type: Optional[str] = None,
    limit: int = 200,
):
    """Diagnostics warehouse lookup for failed-record evidence and check context."""
    try:
        return service.get_diagnostics_records(
            dataset_name=dataset_name,
            run_id=run_id,
            check_type=check_type,
            limit=limit,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/integrations/sources")
def get_source_integrations():
    """Return factual source integration status from configured connectors."""
    try:
        now = datetime.now().isoformat()
        discovered = agent.discover_datasets()
        integrations = []

        for connector in agent.connectors:
            connector_name = str(getattr(connector, "name", "")).strip() or "connector"
            try:
                connector_datasets = connector.discover() or []
                status = "CONNECTED"
                error = None
            except Exception as exc:
                connector_datasets = []
                status = "ERROR"
                error = str(exc)

            dataset_count = 0
            if connector_name == "local_files":
                dataset_count = sum(1 for row in discovered if row.get("data_file"))
            else:
                dataset_count = sum(1 for row in discovered if row.get("connector_name") == connector_name)

            integrations.append(
                {
                    "id": connector_name,
                    "name": connector_name.replace("_", " ").title(),
                    "type": "Connector",
                    "status": status,
                    "dataset_count": int(dataset_count),
                    "discovered_count": int(len(connector_datasets)),
                    "last_checked": now,
                    "details": {
                        "error": error,
                    },
                }
            )

        return {
            "integrations": integrations,
            "count": len(integrations),
            "generated_at": now,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/platform/config")
def get_platform_config():
    """Expose runtime platform configuration (read-only)."""
    try:
        connector_names = [str(getattr(conn, "name", "")).strip() for conn in agent.connectors if str(getattr(conn, "name", "")).strip()]
        return {
            "generated_at": datetime.now().isoformat(),
            "runtime": {
                "api": {"title": app.title},
                "langgraph_workflow_enabled": service.hitl_workflow is not None,
                "hitl_contracts_enabled": service.hitl_workflow is not None,
                "policy_gates_enabled": policy_service is not None,
                "rbac_enabled": bool(getattr(rbac_service, "enabled", False)),
                "incident_lifecycle_enabled": incident_service is not None,
                "async_jobs": {
                    "execution_mode": async_jobs.execution_mode,
                    "max_workers": async_jobs.max_workers,
                    "max_queued_jobs": async_jobs.max_queued_jobs,
                    "stale_job_minutes": async_jobs.stale_job_minutes,
                },
                "contract_store_backend": os.getenv("CONTRACT_STORE_BACKEND", "file").strip().lower(),
                "connectors_enabled": connector_names,
                "watch_dir": os.getenv("DRE_WATCH_DIR", "data/landing"),
                "doris": {
                    "load_enabled": os.getenv("DRE_DORIS_LOAD_ENABLED", "1").strip() != "0",
                    "mock_mode": os.getenv("DORIS_MOCK_MODE", "False").lower() == "true",
                    "db": os.getenv("DORIS_DB", "test_db"),
                    "fe_host": os.getenv("DORIS_FE_HOST", "127.0.0.1"),
                    "fe_http_port": os.getenv("DORIS_FE_HTTP_PORT", "8030"),
                },
                "llm": {
                    "provider": "openai",
                    "model": os.getenv("OPENAI_MODEL_NAME", "gpt-4o"),
                },
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/platform/reset-runtime")
def reset_runtime_state(request_payload: RuntimeResetRequest, request: Request):
    """
    Reset runtime state for clean-slate testing (DB runtime rows + generated artifacts).
    Requires explicit confirmation phrase.
    """
    try:
        _enforce_role(request, "platform.reset")

        if request_payload.confirm_phrase.strip().upper() != "RESET":
            raise HTTPException(status_code=400, detail="confirm_phrase must be exactly RESET")

        result = service.reset_runtime_state(
            clear_generated_contracts=bool(request_payload.clear_generated_contracts),
            preserve_contract_names=list(request_payload.preserve_contracts or []),
            clear_langgraph_checkpoints=bool(request_payload.clear_langgraph_checkpoints),
        )
        _audit(
            "platform_runtime_reset",
            status="COMPLETED",
            metadata={
                "clear_generated_contracts": bool(request_payload.clear_generated_contracts),
                "preserve_contracts": list(request_payload.preserve_contracts or []),
                "clear_langgraph_checkpoints": bool(request_payload.clear_langgraph_checkpoints),
            },
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health/system")
def get_system_health():
    """Check health of all upstream services defined in lineage."""
    try:
        lineage = agent.impact_analyzer.refresh()
        health_results = []
        
        datasets = lineage.get("datasets", {})
        for ds_name, ds_info in datasets.items():
            upstreams = ds_info.get("upstream", [])
            for upstream_config in upstreams:
                # Run the health check tool
                status = agent.system_health.check_upstream_health(upstream_config)
                health_results.append({
                    "dataset": ds_name,
                    "upstream": status
                })
                
        return health_results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats/global")
def get_global_stats():
    """Get global platform statistics."""
    try:
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) as total_runs,
                        COUNT(CASE WHEN status = 'PASSED' THEN 1 END) as passed_runs
                    FROM run_history
                    WHERE timestamp >= %s
                """, (today_start,))
                today_stats = cur.fetchone()

                cur.execute("SELECT AVG(duration_ms) FROM run_history")
                avg_dur = cur.fetchone()[0]

                pass_rate = 0.0
                if today_stats and today_stats[0] > 0:
                    pass_rate = (today_stats[1] / today_stats[0]) * 100.0

                return {
                    "total_runs_today": today_stats[0] if today_stats else 0,
                    "pass_rate_today": round(pass_rate, 1),
                    "avg_duration": round(avg_dur or 0, 0)
                }
    except Exception:
        return {"total_runs_today": 0, "pass_rate_today": 0, "avg_duration": 0}


@app.get("/backtesting/{dataset_name}")
def run_backtesting(dataset_name: str, metric: str = "row_count", limit: int = 500):
    """Run anomaly monitor backtesting to estimate FP/FN behavior."""
    try:
        return backtesting_harness.run(dataset_name=dataset_name, metric_name=metric, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics/{dataset_name}")
def get_dataset_metrics(dataset_name: str):
    """Get latest cached metrics for a dataset."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT run_id, timestamp FROM run_history
                    WHERE dataset_name = %s
                    ORDER BY timestamp DESC LIMIT 1
                """, (dataset_name,))
                last_run = cur.fetchone()

                if not last_run:
                    return {"metrics": {}}

                cur.execute("""
                    SELECT metric_name, metric_value
                    FROM metric_history
                    WHERE run_id = %s
                """, (last_run[0],))
                metrics_rows = cur.fetchall()
                metrics = {r[0]: r[1] for r in metrics_rows}
                return {
                    "run_timestamp": last_run[1].isoformat() if last_run[1] else None,
                    "metrics": metrics
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/profile/{dataset_name}")
def get_dataset_profile(dataset_name: str):
    """Run a deep data profile check."""
    try:
        import pandas as pd
        from pathlib import Path

        def _cached_profile_from_latest_run() -> Optional[Dict[str, Any]]:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT full_verdict
                        FROM run_history
                        WHERE dataset_name = %s
                        ORDER BY timestamp DESC
                        LIMIT 1
                        """,
                        (dataset_name,),
                    )
                    row = cur.fetchone()
            if not row or row[0] is None:
                return None

            full_verdict_raw = row[0]
            if isinstance(full_verdict_raw, str):
                try:
                    full_verdict_raw = json.loads(full_verdict_raw)
                except Exception:
                    return None
            if not isinstance(full_verdict_raw, dict):
                return None

            profile = full_verdict_raw.get("profile_report")
            if not isinstance(profile, dict):
                return None

            cached = dict(profile)
            cached["_source"] = "cached_run_history"
            cached["_warning"] = "Source data file not found; returning profile from latest completed run."
            return cached
        
        # 1. Locate the dataset
        datasets = agent.discover_datasets()
        meta = next((d for d in datasets if d["name"] == dataset_name), None)
        
        if not meta or not meta.get("data_file"):
            cached = _cached_profile_from_latest_run()
            if cached:
                return cached
            raise HTTPException(status_code=404, detail="Dataset or data file not found.")

        has_contract = bool(meta.get("contract_path"))
        has_completed_scan = False
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM run_history
                    WHERE dataset_name = %s
                    LIMIT 1
                    """,
                    (dataset_name,),
                )
                has_completed_scan = cur.fetchone() is not None

        if not has_contract and not has_completed_scan:
            raise HTTPException(
                status_code=409,
                detail="Generate/approve YAML first. Deep profile is available only after contract approval or first completed scan.",
            )
            
        data_path = Path(meta["data_file"])
        if not data_path.exists():
            cached = _cached_profile_from_latest_run()
            if cached:
                return cached
            raise HTTPException(status_code=404, detail="Dataset or data file not found.")
        contract_val = meta.get("contract_path")
        contract_path = Path(contract_val) if contract_val else Path("dummy_non_existent.yaml")
        
        # 2. Load Data (Support CSV, limited to 5000 rows for real-time profiling)
        # In prod, this would be a SQL query or Spark job
        if data_path.suffix == ".csv":
            df = pd.read_csv(data_path, nrows=5000)
        else:
            df = pd.read_parquet(data_path)
            if len(df) > 5000:
                df = df.head(5000)
                
        # 3. Run Profiler
        profile = agent.profiler.profile(df, contract_path, dataset_name)
        return profile.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        print(f"Profile Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/remediation/{dataset_name}")
def get_remediation_plan(dataset_name: str):
    """Get remediation for a broken dataset (hybrid deterministic + LLM)."""
    try:
        return service.get_remediation_plan(dataset_name)
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting remediation plan for {dataset_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
        
@app.post("/remediation/apply")
def apply_remediation(request: RemediationApplyRequest, http_request: Request):
    """Apply an AI-generated fix and log it to the audit trail."""
    try:
        _enforce_role(http_request, "jobs.remediation_apply")
        decision = policy_service.evaluate_action(
            action="remediation_apply",
            dataset_name=request.dataset_name,
        )
        policy_service.enforce(
            decision,
            approved=bool(request.policy_approved),
            reason=request.policy_reason,
        )
        return service.apply_remediation(
            dataset_name=request.dataset_name,
            proposed_yaml=request.proposed_yaml,
            error_context=request.error_context,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- Governance Endpoints ---

@app.get("/governance/{dataset_name}/history")
def get_governance_history(dataset_name: str):
    """Get the full history of schema versions for a dataset."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT filename, timestamp, change_summary
                    FROM schema_audit_log
                    WHERE dataset_name = %s
                    ORDER BY timestamp DESC
                """, (dataset_name,))
                rows = cur.fetchall()
                return [
                    {
                        "filename": r[0],
                        "timestamp": r[1].isoformat() if r[1] else "",
                        "summary": r[2]
                    }
                    for r in rows
                ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/governance/file/{filename}")
def get_historical_file(filename: str):
    """Read the content of a historical schema version."""
    try:
        # Security: Ensure we only read from config/history
        history_dir = Path("config/history")
        file_path = history_dir / filename
        
        # Simple path traversal check
        if not file_path.resolve().is_relative_to(history_dir.resolve()):
             raise HTTPException(status_code=403, detail="Access denied")
             
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="File not found")
            
        with open(file_path, "r") as f:
            return {"content": f.read()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/governance/rollback")
def rollback_schema(request: GovernanceRollbackRequest, http_request: Request):
    """Revert the active schema to a selected historical version."""
    try:
        _enforce_role(http_request, "governance.rollback")
        # 1. Verify files
        history_dir = Path("config/history")
        source_path = history_dir / request.filename
        
        datasets = agent.discover_datasets()
        meta = next((d for d in datasets if d["name"] == request.dataset_name), None)
        if not meta:
            raise HTTPException(status_code=404, detail="Dataset not found")
        target_path = Path(meta["contract_path"])
        
        if not source_path.exists():
             raise HTTPException(status_code=404, detail="Historical version not found")

        # 2. Perform Rollback (Copy history -> active)
        # We also create a NEW history entry for the rollback event itself
        import shutil
        import uuid
        
        # Read content to log it
        with open(source_path, "r") as f:
            content = f.read()
            
        # Overwrite active file
        shutil.copy2(source_path, target_path)
        
        # Log the rollback
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO schema_audit_log (id, dataset_name, filename, timestamp, change_summary)
                    VALUES (%s, %s, %s, NOW(), %s)
                """, (str(uuid.uuid4()), request.dataset_name, request.filename, f"Rollback to version: {request.filename}"))

        # 3. Trigger Smart Scan (Immediate Validation)
        scan_result = {}
        try:
            # Re-using the evaluate_dataset logic directly
            scan_result = service.evaluate_dataset(request.dataset_name)
        except Exception as scan_err:
            print(f"Post-rollback scan failed: {scan_err}")
            scan_result = {"status": "ERROR", "message": "Rollback successful, but immediate scan failed."}
            
        result = {
            "status": "success", 
            "message": f"Rolled back to {request.filename}. Scan Result: {scan_result.get('status', 'Unknown')}",
            "scan_details": scan_result
        }
        _audit("governance_rollback", dataset_name=request.dataset_name, status="COMPLETED", metadata={"filename": request.filename})
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/contracts/propose")
def propose_contract_endpoint(request: ContractProposeRequest, http_request: Request):
    """Generate a proposed contract from data."""
    try:
        _enforce_role(http_request, "contract.propose")
        result = service.propose_contract(request.dataset_name, request.file_path)

        autoscan = {"enqueued": False}
        try:
            scan_job = async_jobs.submit_evaluate(request.dataset_name)
            autoscan = {
                "enqueued": True,
                "job_id": scan_job.get("job_id"),
                "status": scan_job.get("status"),
            }
        except HTTPException as scan_err:
            autoscan = {"enqueued": False, "error": scan_err.detail}
        except Exception as scan_err:
            autoscan = {"enqueued": False, "error": str(scan_err)}

        if isinstance(result, dict):
            result["scan"] = autoscan

        _audit(
            "contract_proposed",
            dataset_name=request.dataset_name,
            status="COMPLETED",
            metadata={
                "file_path": request.file_path,
                "scan_enqueued": bool(autoscan.get("enqueued")),
                "scan_job_id": autoscan.get("job_id"),
            },
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/contracts/gate")
def run_contract_gate(request: ContractGateRequest, http_request: Request):
    """
    Shift-left contract CI gate.
    Returns deterministic PASS/BLOCKED without loading data downstream.
    """
    try:
        _enforce_role(http_request, "contract.propose")
        result = service.run_contract_gate(dataset_name=request.dataset_name, file_path=request.file_path)
        _audit(
            "contract_gate",
            dataset_name=request.dataset_name,
            status=result.get("status"),
            metadata={"file_path": request.file_path},
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/contracts/autopilot")
def run_contract_autopilot(request: ContractAutopilotRequest, http_request: Request):
    """
    AI-assisted contract recommendation pass with confidence/rationale.
    """
    try:
        _enforce_role(http_request, "contract.propose")
        result = service.generate_autopilot_contract(
            dataset_name=request.dataset_name,
            file_path=request.file_path,
            confidence_threshold=request.confidence_threshold,
        )
        _audit(
            "contract_autopilot",
            dataset_name=request.dataset_name,
            status="COMPLETED",
            metadata={
                "file_path": request.file_path,
                "recommendation_count": result.get("recommendation_count"),
                "confidence_threshold": request.confidence_threshold,
            },
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/contracts/pending")
def get_pending_contracts():
    """Get all contracts pending human approval."""
    try:
        return service.get_pending_contracts()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/contracts/{dataset_name}")
def get_active_contract_endpoint(dataset_name: str):
    """Get the current active contract content."""
    try:
        doc = contract_store.read(dataset_name)
        if not doc:
            return {"content": "# No contract found"}
        return {"content": doc.content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class ContractSaveRequest(BaseModel):
    dataset_name: str
    yaml_content: str
    summary: str = "Manual definition update"

@app.post("/contracts/save")
def save_contract_endpoint(request: ContractSaveRequest, http_request: Request):
    """Save a user-approved contract to file."""
    try:
        _enforce_role(http_request, "contract.save")
        # Find the contract path
        datasets = agent.discover_datasets()
        meta = next((d for d in datasets if d["name"] == request.dataset_name), None)
        
        if not meta or not meta.get("contract_path"):
            # Fallback path if not found in metadata
            contract_path = str(contract_store.path_for(request.dataset_name))
        else:
            contract_path = meta["contract_path"]

        # Use Remediator to apply safely (creates backup + history)
        from src.tools.schema_remediator import SchemaRemediator
        remediator = SchemaRemediator()
        
        # apply_fix returns the path to the backup/history file
        remediator.apply_fix(contract_path, request.yaml_content)
        
        # Log to audit table
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO schema_audit_log (id, dataset_name, filename, timestamp, change_summary)
                    VALUES (%s, %s, %s, NOW(), %s)
                """, (str(uuid.uuid4()), request.dataset_name, "manual_update.yaml", request.summary))

        autoscan = {"enqueued": False}
        try:
            scan_job = async_jobs.submit_evaluate(request.dataset_name)
            autoscan = {
                "enqueued": True,
                "job_id": scan_job.get("job_id"),
                "status": scan_job.get("status"),
            }
        except HTTPException as scan_err:
            autoscan = {"enqueued": False, "error": scan_err.detail}
        except Exception as scan_err:
            autoscan = {"enqueued": False, "error": str(scan_err)}

        result = {
            "status": "success",
            "message": "Contract saved successfully. Auto-scan requested.",
            "path": contract_path,
            "scan": autoscan,
        }
        _audit(
            "contract_saved",
            dataset_name=request.dataset_name,
            status="COMPLETED",
            metadata={
                "path": contract_path,
                "scan_enqueued": bool(autoscan.get("enqueued")),
                "scan_job_id": autoscan.get("job_id"),
            },
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/contract/{dataset_name}")
def get_current_contract(dataset_name: str):
    """Get the current active contract YAML content."""
    try:
        doc = contract_store.read(dataset_name)
        if not doc:
            contract_path = str(contract_store.path_for(dataset_name))
            raise HTTPException(status_code=404, detail=f"Contract not found: {contract_path}")

        return {"yaml_content": doc.content, "path": doc.location}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/contract-history/{dataset_name}")
def get_contract_history(dataset_name: str):
    """Get version history for a contract from contract_versions table."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, dataset_name, created_at, created_by, contract_content, change_type
                    FROM contract_versions
                    WHERE dataset_name = %s
                    ORDER BY created_at DESC
                    LIMIT 50
                """, (dataset_name,))

                rows = cur.fetchall()
                versions = []
                for row in rows:
                    versions.append({
                        "version_id": str(row[0]),  # Use id as version_id
                        "dataset_name": row[1],
                        "timestamp": row[2].isoformat() if row[2] else None,
                        "changed_by": row[3] or "user",
                        "yaml_content": row[4],
                        "change_type": row[5] or "manual_edit"
                    })

                return versions
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/contract/{dataset_name}/version/{version_id}")
def get_contract_version(dataset_name: str, version_id: str):
    """Get a specific version of a contract."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT contract_content
                    FROM contract_versions
                    WHERE dataset_name = %s AND id::text = %s
                """, (dataset_name, version_id))

                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Version not found")

                return {"yaml_content": row[0]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/contract/{dataset_name}")
def save_contract_version(dataset_name: str, request: dict):
    """Save a new version of the contract."""
    try:
        import uuid

        yaml_content = request.get("yaml_content")
        change_type = request.get("change_type", "manual_edit")
        changed_by = request.get("changed_by", "user")

        if not yaml_content:
            raise HTTPException(status_code=400, detail="yaml_content is required")

        # Save to active contract store
        saved = contract_store.write(dataset_name, yaml_content)
        contract_path = saved.location

        # Calculate hash for the content
        import hashlib
        contract_hash = hashlib.sha256(yaml_content.encode()).hexdigest()

        # Save version to database
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO contract_versions (dataset_name, contract_path, contract_content, contract_hash, created_by, change_type)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (dataset_name, contract_path, yaml_content, contract_hash, changed_by, change_type))

                version_id = cur.fetchone()[0]

        autoscan = {"enqueued": False}
        try:
            scan_job = async_jobs.submit_evaluate(dataset_name)
            autoscan = {
                "enqueued": True,
                "job_id": scan_job.get("job_id"),
                "status": scan_job.get("status"),
            }
        except HTTPException as scan_err:
            autoscan = {"enqueued": False, "error": scan_err.detail}
        except Exception as scan_err:
            autoscan = {"enqueued": False, "error": str(scan_err)}

        result = {
            "status": "success",
            "version_id": str(version_id),
            "message": "Contract saved successfully. Auto-scan requested.",
            "scan": autoscan,
        }
        _audit(
            "contract_version_saved",
            dataset_name=dataset_name,
            status="COMPLETED",
            metadata={
                "version_id": str(version_id),
                "change_type": change_type,
                "changed_by": changed_by,
                "scan_enqueued": bool(autoscan.get("enqueued")),
                "scan_job_id": autoscan.get("job_id"),
            },
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/contract/{dataset_name}/ai-modify")
def ai_modify_contract(dataset_name: str, request: dict):
    """Use AI to modify the contract based on natural language instruction."""
    try:
        import copy
        import re
        import yaml as yaml_lib
        from agno.agent import Agent
        from agno.models.openai import OpenAIChat

        instruction = request.get("instruction")
        current_yaml = request.get("current_yaml")

        if not instruction or not current_yaml:
            raise HTTPException(status_code=400, detail="instruction and current_yaml are required")

        lower_instruction = str(instruction or "").lower()
        current_doc = yaml_lib.safe_load(current_yaml) or {}
        columns = current_doc.get("columns") or []
        column_names = [
            str(col.get("name")).strip()
            for col in columns
            if isinstance(col, dict) and col.get("name")
        ]

        def _norm_name(s: str) -> str:
            return re.sub(r"[^a-z0-9]", "", str(s or "").lower())

        def _find_target_columns(text: str, names: list[str]) -> set[str]:
            lowered = text.lower()
            direct = {name for name in names if name.lower() in lowered}

            quoted = set()
            for match in re.findall(r"['\"]([^'\"]+)['\"]", text):
                m_norm = _norm_name(match)
                for name in names:
                    if _norm_name(name) == m_norm:
                        quoted.add(name)

            token_match = set()
            for name in names:
                n = _norm_name(name)
                if n and n in _norm_name(text):
                    token_match.add(name)

            return direct or quoted or token_match

        def _detect_global_intent(text: str) -> bool:
            return any(
                phrase in text.lower()
                for phrase in (
                    "all columns",
                    "every column",
                    "entire schema",
                    "globally",
                    "across all columns",
                    "all fields",
                )
            )

        target_columns = _find_target_columns(str(instruction), column_names)
        global_intent = _detect_global_intent(str(instruction))

        def _coerce_number(value: str):
            try:
                num = float(value)
                return int(num) if num.is_integer() else num
            except Exception:
                return value

        def _extract_pattern(text: str) -> str | None:
            m = re.search(r"(?:pattern|regex)[^'\"/]*['\"]([^'\"]+)['\"]", text, flags=re.IGNORECASE)
            if m:
                return m.group(1)
            m = re.search(r"(?:pattern|regex)[^/]*\/([^/]+)\/", text, flags=re.IGNORECASE)
            if m:
                return m.group(1)
            return None

        def _extract_allowed_values(text: str) -> list[str] | None:
            m = re.search(r"allowed[_\s]?values?.*?\[([^\]]+)\]", text, flags=re.IGNORECASE)
            if m:
                raw = m.group(1)
                vals = [v.strip().strip("'\"") for v in raw.split(",") if v.strip()]
                return vals or None
            m = re.search(r"allowed[_\s]?values?.*?:\s*(.+)$", text, flags=re.IGNORECASE)
            if m:
                raw = m.group(1)
                vals = [v.strip().strip("'\"") for v in raw.split(",") if v.strip()]
                return vals or None
            return None

        def _apply_column_updates(doc: dict, targets: set[str], text: str) -> bool:
            changed = False
            if not targets:
                return changed

            parsed_pattern = _extract_pattern(text)
            parsed_allowed_values = _extract_allowed_values(text)
            min_match = re.search(r"\bmin(?:_value)?\b[^0-9-]*(-?\d+(?:\.\d+)?)", text, flags=re.IGNORECASE)
            max_match = re.search(r"\bmax(?:_value)?\b[^0-9-]*(-?\d+(?:\.\d+)?)", text, flags=re.IGNORECASE)
            type_match = re.search(
                r"\b(?:set|change|update)\s+(?:data\s*)?type\b.*?\bto\b\s+([a-zA-Z0-9_]+)",
                text,
                flags=re.IGNORECASE,
            )

            for col in doc.get("columns", []):
                if not isinstance(col, dict):
                    continue
                name = str(col.get("name") or "")
                if name not in targets:
                    continue

                if any(k in text.lower() for k in ("not nullable", "non-nullable", "non nullable", "not null", "required")):
                    if col.get("nullable") is not False:
                        col["nullable"] = False
                        changed = True
                elif "nullable" in text.lower():
                    if col.get("nullable") is not True:
                        col["nullable"] = True
                        changed = True

                if any(k in text.lower() for k in ("remove pattern", "drop pattern", "clear pattern")) and "pattern" in col:
                    del col["pattern"]
                    changed = True
                elif parsed_pattern:
                    if col.get("pattern") != parsed_pattern:
                        col["pattern"] = parsed_pattern
                        changed = True

                if any(k in text.lower() for k in ("remove allowed", "drop allowed", "clear allowed")) and "allowed_values" in col:
                    del col["allowed_values"]
                    changed = True
                elif parsed_allowed_values is not None:
                    if col.get("allowed_values") != parsed_allowed_values:
                        col["allowed_values"] = parsed_allowed_values
                        changed = True

                if min_match:
                    new_min = _coerce_number(min_match.group(1))
                    if col.get("min_value") != new_min:
                        col["min_value"] = new_min
                        changed = True

                if max_match:
                    new_max = _coerce_number(max_match.group(1))
                    if col.get("max_value") != new_max:
                        col["max_value"] = new_max
                        changed = True

                if any(k in text.lower() for k in ("primary key", "isprimarykey", "unique")):
                    if col.get("isPrimaryKey") is not True:
                        col["isPrimaryKey"] = True
                        changed = True
                if any(k in text.lower() for k in ("remove primary key", "not primary key")):
                    if "isPrimaryKey" in col:
                        del col["isPrimaryKey"]
                        changed = True

                if type_match:
                    new_type = type_match.group(1).strip()
                    if new_type and col.get("data_type") != new_type:
                        col["data_type"] = new_type
                        changed = True

            return changed

        def _apply_quality_updates(doc: dict, text: str) -> bool:
            changed = False
            quality = doc.setdefault("quality", {})
            if not isinstance(quality, dict):
                return False

            min_rows = re.search(r"\bmin[_\s]?rows?\b[^0-9-]*(-?\d+)", text, flags=re.IGNORECASE)
            max_rows = re.search(r"\bmax[_\s]?rows?\b[^0-9-]*(-?\d+)", text, flags=re.IGNORECASE)
            if min_rows:
                val = int(min_rows.group(1))
                if quality.get("min_rows") != val:
                    quality["min_rows"] = val
                    changed = True
            if max_rows:
                val = int(max_rows.group(1))
                if quality.get("max_rows") != val:
                    quality["max_rows"] = val
                    changed = True
            return changed

        deterministic_doc = copy.deepcopy(current_doc)
        deterministic_changed = False
        deterministic_changed = _apply_quality_updates(deterministic_doc, str(instruction)) or deterministic_changed
        deterministic_changed = _apply_column_updates(deterministic_doc, target_columns, str(instruction)) or deterministic_changed
        if deterministic_changed:
            deterministic_yaml = yaml_lib.safe_dump(deterministic_doc, sort_keys=False)
            return {
                "modified_yaml": deterministic_yaml,
                "explanation": f"Deterministic update applied from instruction: {instruction}",
            }

        # Use Agno to modify the contract
        agent = Agent(
            model=OpenAIChat(id="gpt-4o"),
            markdown=False
        )

        prompt = f"""You are a data contract modification assistant. The user wants to modify a YAML data contract.

Current YAML contract:
```yaml
{current_yaml}
```

User instruction: {instruction}

Modify ONLY what is explicitly requested. Preserve all other fields exactly as-is.
Do not change nullable/default/type/pattern/allowed_values on unrelated columns.
If the request targets a single column, do not modify other columns.
Return ONLY the modified YAML (no explanations, no markdown code blocks, just the raw YAML).

Common modifications:
- "enforce null values for X" or "make X not nullable" → set nullable: false
- "make X nullable" → set nullable: true
- "add pattern to X" → add pattern: 'regex'
- "add allowed_values to X" → add allowed_values: [...]
- "add unique constraint to X" → add isPrimaryKey: true
- "set min/max for X" → add min_value/max_value

Return the complete modified YAML contract."""

        response = agent.run(prompt)
        modified_yaml = response.content.strip()

        # Remove markdown code blocks if present
        if modified_yaml.startswith("```"):
            lines = modified_yaml.split("\n")
            modified_yaml = "\n".join(lines[1:-1]) if len(lines) > 2 else modified_yaml

        # Validate it's valid YAML
        try:
            yaml_lib.safe_load(modified_yaml)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"AI generated invalid YAML: {str(e)}")

        # Safety guard: prevent broad column mutations when the instruction is scoped.
        try:
            modified_doc = yaml_lib.safe_load(modified_yaml) or {}
            if target_columns and not global_intent:
                original_map = {
                    str(c.get("name")): c
                    for c in (current_doc.get("columns") or [])
                    if isinstance(c, dict) and c.get("name")
                }
                llm_map = {
                    str(c.get("name")): c
                    for c in (modified_doc.get("columns") or [])
                    if isinstance(c, dict) and c.get("name")
                }
                restricted = copy.deepcopy(current_doc)
                rebuilt_columns = []
                for name, original_col in original_map.items():
                    if name in target_columns and name in llm_map:
                        rebuilt_columns.append(llm_map[name])
                    else:
                        rebuilt_columns.append(original_col)
                restricted["columns"] = rebuilt_columns
                modified_yaml = yaml_lib.safe_dump(restricted, sort_keys=False)
        except HTTPException:
            raise
        except Exception:
            # If safety parsing fails, allow original validated output through.
            pass

        return {
            "modified_yaml": modified_yaml,
            "explanation": f"Modified contract based on: {instruction}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- New Visualization Endpoints ---

@app.get("/incidents")
def get_incidents(
    limit: int = 50,
    status: Optional[str] = None,
    severity: Optional[str] = None,
    dataset_name: Optional[str] = None,
    owner: Optional[str] = None,
):
    """Get incidents with lifecycle status (OPEN/ACK/RESOLVED)."""
    try:
        return incident_service.list_incidents(
            limit=limit,
            status=status,
            severity=severity,
            dataset_name=dataset_name,
            owner=owner,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/incidents/{incident_id}")
def get_incident(incident_id: str):
    """Get a single incident by ID."""
    try:
        return incident_service.get_incident(incident_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/incidents/{incident_id}")
def update_incident(incident_id: str, request: IncidentUpdateRequest, http_request: Request):
    """Update incident lifecycle state and optional ownership/note."""
    try:
        _enforce_role(http_request, "incident.update")
        result = incident_service.update_incident(
            incident_id,
            status=request.status,
            owner=request.owner,
            note=request.note,
        )
        ds = result.get("dataset_name") if isinstance(result, dict) else None
        _audit(
            "incident_updated",
            dataset_name=ds,
            status=request.status,
            metadata={"incident_id": incident_id, "note": request.note, "owner": request.owner},
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/quality-dimensions/{dataset_name}")
def get_quality_dimensions(dataset_name: str):
    """
    Get 6-dimensional quality scores for a dataset from the latest run.

    Returns:
        {
            "dataset_name": "orders",
            "timestamp": "2026-02-15T14:00:00Z",
            "overall_score": 88.5,
            "dimensions": [
                {"name": "Validity", "score": 98.0, "weight": 0.25, "status": "PASS", ...},
                {"name": "Completeness", "score": 75.2, "weight": 0.25, "status": "FAIL", ...},
                ...
            ]
        }
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Get latest run with dimension_scores
                cur.execute("""
                    SELECT dimension_scores
                    FROM run_history
                    WHERE dataset_name = %s AND dimension_scores IS NOT NULL
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, (dataset_name,))

                row = cur.fetchone()

                if not row or not row[0]:
                    raise HTTPException(status_code=404, detail="No dimension scores found. Run evaluation first.")

                payload = row[0]
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except Exception:
                        payload = row[0]

                return payload
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics/{dataset_name}/timeseries")
def get_metric_timeseries(dataset_name: str, metric: str = "row_count", limit: int = 30):
    """Get time-series data for a specific metric (for charts)."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT mh.timestamp, mh.metric_value, mh.run_id, mh.day_of_week,
                           mh.metric_group, mh.column_name, mh.segment, mh.tags
                    FROM metric_history mh
                    WHERE mh.dataset_name = %s AND mh.metric_name = %s
                    ORDER BY mh.timestamp DESC
                    LIMIT %s
                """, (dataset_name, metric, limit))
                rows = cur.fetchall()

                # Also get baseline for confidence bands
                cur.execute("""
                    SELECT baseline_mean, baseline_std, baseline_type, sample_count
                    FROM learned_thresholds
                    WHERE dataset_name = %s AND metric_name = %s
                """, (dataset_name, metric))
                baseline_row = cur.fetchone()

                baseline = None
                if baseline_row:
                    mean_val = baseline_row[0] or 0.0
                    std_val = baseline_row[1] or 0.0
                    baseline = {
                        "mean": mean_val,
                        "std": std_val,
                        "type": baseline_row[2],
                        "sample_count": baseline_row[3],
                        "upper_3sigma": mean_val + 3 * std_val,
                        "lower_3sigma": mean_val - 3 * std_val,
                        "upper_2sigma": mean_val + 2 * std_val,
                        "lower_2sigma": mean_val - 2 * std_val,
                    }

                return {
                    "dataset": dataset_name,
                    "metric": metric,
                    "baseline": baseline,
                    "data": [
                        {
                            "timestamp": r[0].isoformat() if r[0] else None,
                            "value": r[1],
                            "run_id": r[2],
                            "day_of_week": r[3],
                            "metric_group": r[4],
                            "column_name": r[5],
                            "segment": r[6],
                            "tags": r[7] or {},
                        }
                        for r in reversed(rows)  # chronological order
                    ]
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/baselines/{dataset_name}")
def get_baselines(dataset_name: str):
    """Get all learned thresholds/baselines for a dataset."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT metric_name, baseline_mean, baseline_std,
                           baseline_type, last_updated, sample_count
                    FROM learned_thresholds
                    WHERE dataset_name = %s
                    ORDER BY metric_name
                """, (dataset_name,))
                rows = cur.fetchall()
                return [
                    {
                        "metric": r[0],
                        "mean": r[1],
                        "std": r[2],
                        "type": r[3],
                        "last_updated": r[4].isoformat() if r[4] else None,
                        "sample_count": r[5],
                        "upper_3sigma": (r[1] or 0.0) + 3 * (r[2] or 0.0),
                        "lower_3sigma": (r[1] or 0.0) - 3 * (r[2] or 0.0),
                    }
                    for r in rows
                ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/slos/{dataset_name}")
def get_slo_history(dataset_name: str, limit: int = 100):
    """Get per-run SLO check history for a dataset."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT timestamp, run_id, slo_name, operator,
                           target_value, observed_value, status, error_budget_burn, metadata
                    FROM slo_history
                    WHERE dataset_name = %s
                    ORDER BY timestamp DESC
                    LIMIT %s
                    """,
                    (dataset_name, limit),
                )
                rows = cur.fetchall()
                return [
                    {
                        "timestamp": r[0].isoformat() if r[0] else None,
                        "run_id": r[1],
                        "slo_name": r[2],
                        "operator": r[3],
                        "target_value": r[4],
                        "observed_value": r[5],
                        "status": r[6],
                        "error_budget_burn": r[7],
                        "metadata": r[8] or {},
                    }
                    for r in rows
                ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/slos/{dataset_name}/summary")
def get_slo_summary(dataset_name: str, window: int = 200):
    """Get SLO attainment summary and error budget burn for a dataset."""
    try:
        return service.get_slo_summary(dataset_name, window)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/contracts/approve")
def approve_contract(request: ContractApprovalRequest, http_request: Request):
    """
    Approve a contract and trigger validation of pending files.

    Flow:
    1. Save approved YAML to config/expectations/
    2. Find pending files for this dataset
    3. Trigger validation on each
    4. Move from pending_approval to landing or quarantine
    5. Clean up proposal files
    """
    try:
        _enforce_role(http_request, "contract.approve")
        result = service.approve_contract(request.dataset_name, request.approved_yaml)
        _audit("contract_approved", dataset_name=request.dataset_name, status="COMPLETED")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/contracts/pending/{dataset_name}")
def reject_contract_proposal(dataset_name: str, http_request: Request):
    """
    Reject a contract proposal.
    Moves pending files to quarantine and removes proposal.
    """
    try:
        _enforce_role(http_request, "contract.reject")
        result = service.reject_contract_proposal(dataset_name)
        _audit("contract_rejected", dataset_name=dataset_name, status="COMPLETED")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

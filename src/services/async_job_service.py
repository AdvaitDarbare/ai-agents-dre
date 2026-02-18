from __future__ import annotations

import json
import os
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from src.services.reliability_service import ReliabilityService
from src.services.action_audit_service import ActionAuditService
from src.utils.database import get_connection


class AsyncJobService:
    """
    Lightweight async job executor backed by PostgreSQL state.

    This keeps long-running operations (scan/delete) out of request threads while
    preserving durable status and results for polling UIs.
    """

    def __init__(
        self,
        reliability_service: ReliabilityService,
        max_workers: int = 4,
        max_queued_jobs: int = 100,
        audit_service: Optional[ActionAuditService] = None,
    ):
        self.reliability_service = reliability_service
        self.max_workers = max(1, int(max_workers))
        self.max_queued_jobs = max(1, int(max_queued_jobs))
        self.stale_job_minutes = max(1, int(os.getenv("ASYNC_JOB_STALE_MINUTES", "30")))
        requested_mode = os.getenv("ASYNC_JOB_EXECUTION_MODE", "inprocess").strip().lower()
        self.execution_mode = "external_worker" if requested_mode in {"external", "worker", "external_worker"} else "inprocess"
        self.executor: Optional[ThreadPoolExecutor] = None
        if self.execution_mode == "inprocess":
            self.executor = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="dre-job")
        self.audit_service = audit_service

    def _reconcile_stale_jobs(self) -> None:
        """
        Fail jobs that have been RUNNING/QUEUED far beyond normal bounds.
        This prevents the UI from showing indefinite 'Scanning...' when worker
        state is lost (e.g., process restart mid-job).
        """
        stale_before = datetime.now(timezone.utc) - timedelta(minutes=self.stale_job_minutes)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE async_jobs
                    SET status = 'FAILED',
                        finished_at = NOW(),
                        error_text = COALESCE(error_text, %s)
                    WHERE status = 'RUNNING'
                      AND started_at IS NOT NULL
                      AND started_at < %s
                    """,
                    (
                        f"Job timed out after {self.stale_job_minutes} minutes and was marked FAILED by stale-job reconciliation.",
                        stale_before,
                    ),
                )
                cur.execute(
                    """
                    UPDATE async_jobs
                    SET status = 'FAILED',
                        finished_at = NOW(),
                        error_text = COALESCE(error_text, %s)
                    WHERE status = 'QUEUED'
                      AND requested_at < %s
                    """,
                    (
                        f"Queued job exceeded {self.stale_job_minutes} minutes without starting and was marked FAILED by stale-job reconciliation.",
                        stale_before,
                    ),
                )

    def _audit(self, *, action: str, dataset_name: str, status: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        if not self.audit_service:
            return
        try:
            self.audit_service.record(
                action=action,
                dataset_name=dataset_name,
                status=status,
                actor="system",
                source="async_jobs",
                metadata=metadata or {},
            )
        except Exception:
            # Never block job execution on audit logging.
            return

    @staticmethod
    def _normalize_payload(payload: Any) -> Any:
        """Ensure payload is JSON-serializable."""
        return json.loads(json.dumps(payload, default=str))

    @staticmethod
    def _row_to_dict(row: Any) -> Dict[str, Any]:
        result_payload = row[7]
        if isinstance(result_payload, str):
            try:
                result_payload = json.loads(result_payload)
            except Exception:
                result_payload = None

        started_at = row[5]
        finished_at = row[6]
        duration_ms: Optional[int] = None
        if started_at and finished_at:
            duration_ms = int((finished_at - started_at).total_seconds() * 1000)

        return {
            "job_id": row[0],
            "action": row[1],
            "dataset_name": row[2],
            "status": row[3],
            "requested_at": row[4].isoformat() if row[4] else None,
            "started_at": started_at.isoformat() if started_at else None,
            "finished_at": finished_at.isoformat() if finished_at else None,
            "duration_ms": duration_ms,
            "result": result_payload,
            "error": row[8],
        }

    def _create_job(self, action: str, dataset_name: str, request_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        job_id = str(uuid.uuid4())
        payload = self._normalize_payload(request_payload or {})
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO async_jobs (job_id, action, dataset_name, status, request_json)
                    VALUES (%s, %s, %s, 'QUEUED', %s::jsonb)
                    """,
                    (job_id, action, dataset_name, json.dumps(payload)),
                )

        self._audit(
            action=f"job_{action}_enqueued",
            dataset_name=dataset_name,
            status="QUEUED",
            metadata={"job_id": job_id, "action": action},
        )
        return self.get_job(job_id)

    def _active_job_count(self) -> int:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM async_jobs
                    WHERE status IN ('QUEUED', 'RUNNING')
                    """
                )
                row = cur.fetchone()
        return int(row[0] or 0) if row else 0

    def _ensure_capacity(self) -> None:
        active_jobs = self._active_job_count()
        if active_jobs >= self.max_queued_jobs:
            raise HTTPException(
                status_code=429,
                detail=(
                    f"Async job queue is full ({active_jobs}/{self.max_queued_jobs}). "
                    "Try again after current jobs finish."
                ),
            )

    def _mark_running(self, job_id: str) -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE async_jobs
                    SET status = 'RUNNING', started_at = NOW(), error_text = NULL
                    WHERE job_id = %s
                    """,
                    (job_id,),
                )

        try:
            job = self.get_job(job_id)
            self._audit(action=f"job_{job.get('action','job')}_running", dataset_name=job.get("dataset_name", ""), status="RUNNING", metadata={"job_id": job_id})
        except Exception:
            pass

    def _mark_completed(self, job_id: str, result: Dict[str, Any]) -> None:
        payload = self._normalize_payload(result)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE async_jobs
                    SET status = 'COMPLETED', finished_at = NOW(), result_json = %s::jsonb, error_text = NULL
                    WHERE job_id = %s
                    """,
                    (json.dumps(payload), job_id),
                )

        try:
            job = self.get_job(job_id)
            run_id = None
            if job.get("action") == "evaluate":
                run_id = self._extract_run_id(payload)

            self._audit(
                action=f"job_{job.get('action','job')}_completed",
                dataset_name=job.get("dataset_name", ""),
                status="COMPLETED",
                metadata={"job_id": job_id, "run_id": run_id} if run_id else {"job_id": job_id},
            )
        except Exception:
            pass

    def _mark_failed(self, job_id: str, error_text: str) -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE async_jobs
                    SET status = 'FAILED', finished_at = NOW(), error_text = %s
                    WHERE job_id = %s
                    """,
                    (error_text[:5000], job_id),
                )

        try:
            job = self.get_job(job_id)
            self._audit(
                action=f"job_{job.get('action','job')}_failed",
                dataset_name=job.get("dataset_name", ""),
                status="FAILED",
                metadata={"job_id": job_id},
            )
        except Exception:
            pass

    def _execute_action(self, action: str, dataset_name: str, request_payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = str(action or "").strip().lower()
        payload = request_payload or {}

        if normalized == "evaluate":
            return self.reliability_service.evaluate_dataset(dataset_name, force_load=bool(payload.get("force_load")))

        if normalized == "delete":
            return self.reliability_service.delete_dataset(dataset_name)

        if normalized == "remediation_apply":
            return self.reliability_service.apply_remediation(
                dataset_name=str(payload.get("dataset_name") or dataset_name),
                proposed_yaml=str(payload.get("proposed_yaml") or ""),
                error_context=str(payload.get("error_context") or ""),
            )

        if normalized == "bulk_delete":
            dataset_names = payload.get("dataset_names") or []
            if not isinstance(dataset_names, list):
                raise HTTPException(status_code=400, detail="bulk_delete request must include dataset_names[]")
            return self.reliability_service.bulk_delete_datasets([str(name) for name in dataset_names if str(name or "").strip()])

        if normalized == "bulk_evaluate":
            dataset_names = payload.get("dataset_names") or []
            force_load = bool(payload.get("force_load", False))
            if not isinstance(dataset_names, list):
                raise HTTPException(status_code=400, detail="bulk_evaluate request must include dataset_names[]")
            return self.reliability_service.bulk_evaluate_datasets(
                [str(name) for name in dataset_names if str(name or "").strip()],
                force_load=force_load
            )

        raise HTTPException(status_code=400, detail=f"Unsupported async action: {normalized}")

    def _run_job(self, job_id: str, action: str, dataset_name: str, request_payload: Optional[Dict[str, Any]] = None) -> None:
        self._mark_running(job_id)
        try:
            result = self._execute_action(action=action, dataset_name=dataset_name, request_payload=request_payload or {})
            self._mark_completed(job_id, result if isinstance(result, dict) else {"result": result})
        except Exception as exc:  # pragma: no cover - exercised in runtime path
            details = f"{exc}\n{traceback.format_exc()}"
            self._mark_failed(job_id, details)

    @staticmethod
    def _extract_run_id(payload: Any) -> Optional[str]:
        """
        Best-effort run_id extraction from a completed evaluate payload.

        We see multiple shapes depending on whether evaluation ran directly or via HITL workflow:
        - {"run_id": "...", ...}
        - {"verdict": {"run_id": "..."}}
        - {"mode": "evaluated", "verdict": {"run_id": "..."}, ...}
        """

        def walk(value: Any, depth: int) -> Optional[str]:
            if depth <= 0:
                return None

            if isinstance(value, dict):
                direct = value.get("run_id")
                if isinstance(direct, str) and direct.strip():
                    return direct.strip()

                # Check common nesting first.
                for key in ("verdict", "result", "scan_details", "full_verdict"):
                    if key in value:
                        found = walk(value.get(key), depth - 1)
                        if found:
                            return found

                for nested in value.values():
                    found = walk(nested, depth - 1)
                    if found:
                        return found
                return None

            if isinstance(value, list):
                for item in value:
                    found = walk(item, depth - 1)
                    if found:
                        return found
                return None

            return None

        return walk(payload, 4)

    def submit_evaluate(self, dataset_name: str, force_load: bool = False) -> Dict[str, Any]:
        self._ensure_capacity()
        payload = {"dataset_name": dataset_name, "force_load": force_load}
        job = self._create_job(action="evaluate", dataset_name=dataset_name, request_payload=payload)
        if self.execution_mode == "inprocess" and self.executor is not None:
            self.executor.submit(self._run_job, job["job_id"], "evaluate", dataset_name, payload)
        return job

    def submit_delete(self, dataset_name: str) -> Dict[str, Any]:
        self._ensure_capacity()
        job = self._create_job(action="delete", dataset_name=dataset_name, request_payload={"dataset_name": dataset_name})
        if self.execution_mode == "inprocess" and self.executor is not None:
            self.executor.submit(self._run_job, job["job_id"], "delete", dataset_name, {"dataset_name": dataset_name})
        return job

    def submit_apply_remediation(
        self,
        *,
        dataset_name: str,
        proposed_yaml: str,
        error_context: str,
    ) -> Dict[str, Any]:
        self._ensure_capacity()
        payload = {
            "dataset_name": dataset_name,
            "proposed_yaml": proposed_yaml,
            "error_context": error_context,
        }
        job = self._create_job(action="remediation_apply", dataset_name=dataset_name, request_payload=payload)
        if self.execution_mode == "inprocess" and self.executor is not None:
            self.executor.submit(self._run_job, job["job_id"], "remediation_apply", dataset_name, payload)
        return job

    def submit_bulk_delete(self, dataset_names: List[str]) -> Dict[str, Any]:
        normalized = [str(name or "").strip() for name in dataset_names if str(name or "").strip()]
        if not normalized:
            raise HTTPException(status_code=400, detail="dataset_names must include at least one dataset")

        self._ensure_capacity()
        display_name = ",".join(sorted(set(normalized))[:3])
        if len(set(normalized)) > 3:
            display_name += ",..."
        payload = {"dataset_names": normalized}
        job = self._create_job(action="bulk_delete", dataset_name=display_name, request_payload=payload)
        if self.execution_mode == "inprocess" and self.executor is not None:
            self.executor.submit(self._run_job, job["job_id"], "bulk_delete", display_name, payload)
        return job

    def submit_bulk_evaluate(self, dataset_names: List[str], force_load: bool = False) -> Dict[str, Any]:
        normalized = [str(name or "").strip() for name in dataset_names if str(name or "").strip()]
        if not normalized:
            raise HTTPException(status_code=400, detail="dataset_names must include at least one dataset")

        self._ensure_capacity()
        display_name = ",".join(sorted(set(normalized))[:3])
        if len(set(normalized)) > 3:
            display_name += ",..."
        payload = {"dataset_names": normalized, "force_load": force_load}
        job = self._create_job(action="bulk_evaluate", dataset_name=display_name, request_payload=payload)
        if self.execution_mode == "inprocess" and self.executor is not None:
            self.executor.submit(self._run_job, job["job_id"], "bulk_evaluate", display_name, payload)
        return job

    def claim_next_job(self, *, actions: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """
        Atomically claim one QUEUED job and mark it RUNNING.
        Intended for external worker processes.
        """
        action_filter_sql = ""
        params: List[Any] = []
        normalized_actions = [str(item or "").strip().lower() for item in (actions or []) if str(item or "").strip()]
        if normalized_actions:
            action_filter_sql = "AND action = ANY(%s)"
            params.append(normalized_actions)

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    WITH candidate AS (
                        SELECT job_id
                        FROM async_jobs
                        WHERE status = 'QUEUED'
                        {action_filter_sql}
                        ORDER BY requested_at ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE async_jobs aj
                    SET status = 'RUNNING',
                        started_at = NOW(),
                        error_text = NULL
                    FROM candidate c
                    WHERE aj.job_id = c.job_id
                    RETURNING
                        aj.job_id,
                        aj.action,
                        aj.dataset_name,
                        aj.request_json
                    """,
                    tuple(params),
                )
                row = cur.fetchone()

        if not row:
            return None

        payload = row[3] or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}

        claimed = {
            "job_id": row[0],
            "action": row[1],
            "dataset_name": row[2],
            "request_payload": payload if isinstance(payload, dict) else {},
        }
        self._audit(
            action=f"job_{claimed['action']}_running",
            dataset_name=claimed["dataset_name"],
            status="RUNNING",
            metadata={"job_id": claimed["job_id"], "execution_mode": "external_worker"},
        )
        return claimed

    def run_worker_once(self, *, actions: Optional[List[str]] = None) -> bool:
        """
        Claim and execute a single queued job.
        Returns True when one job was processed, False when queue is empty.
        """
        claimed = self.claim_next_job(actions=actions)
        if not claimed:
            return False

        job_id = str(claimed.get("job_id") or "")
        action = str(claimed.get("action") or "")
        dataset_name = str(claimed.get("dataset_name") or "")
        request_payload = claimed.get("request_payload") if isinstance(claimed.get("request_payload"), dict) else {}
        try:
            result = self._execute_action(action=action, dataset_name=dataset_name, request_payload=request_payload)
            self._mark_completed(job_id, result if isinstance(result, dict) else {"result": result})
        except Exception as exc:  # pragma: no cover - runtime path
            details = f"{exc}\n{traceback.format_exc()}"
            self._mark_failed(job_id, details)
        return True

    def get_job(self, job_id: str) -> Dict[str, Any]:
        self._reconcile_stale_jobs()
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT job_id, action, dataset_name, status,
                           requested_at, started_at, finished_at,
                           result_json, error_text
                    FROM async_jobs
                    WHERE job_id = %s
                    """,
                    (job_id,),
                )
                row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        return self._row_to_dict(row)

    def list_jobs(
        self,
        *,
        limit: int = 50,
        status: Optional[str] = None,
        action: Optional[str] = None,
        dataset_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        self._reconcile_stale_jobs()
        clauses: List[str] = []
        params: List[Any] = []

        if status:
            clauses.append("status = %s")
            params.append(status.upper())
        if action:
            clauses.append("action = %s")
            params.append(action.lower())
        if dataset_name:
            clauses.append("dataset_name = %s")
            params.append(dataset_name)

        where_sql = ""
        if clauses:
            where_sql = "WHERE " + " AND ".join(clauses)

        sql = f"""
            SELECT job_id, action, dataset_name, status,
                   requested_at, started_at, finished_at,
                   result_json, error_text
            FROM async_jobs
            {where_sql}
            ORDER BY requested_at DESC
            LIMIT %s
        """
        params.append(max(1, min(limit, 500)))

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()

        return [self._row_to_dict(row) for row in rows]

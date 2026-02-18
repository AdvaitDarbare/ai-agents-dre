from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Optional

from src.utils.database import get_connection


class ActionAuditService:
    """
    Store and query a structured audit log of operator/agent actions.

    This is intentionally lightweight: a single table with JSON metadata so we can
    evolve the schema without migrations for each new action type.
    """

    @staticmethod
    def _normalize_metadata(metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not metadata:
            return {}
        # Ensure JSON-serializable payloads.
        return json.loads(json.dumps(metadata, default=str))

    def record(
        self,
        *,
        action: str,
        dataset_name: Optional[str] = None,
        status: Optional[str] = None,
        actor: str = "user",
        source: str = "api",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        action_id = str(uuid.uuid4())
        payload = self._normalize_metadata(metadata)

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO action_audit_log (id, actor, source, action, dataset_name, status, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        action_id,
                        actor,
                        source,
                        str(action or "").strip().lower(),
                        dataset_name,
                        status,
                        json.dumps(payload),
                    ),
                )

        return action_id

    def list_actions(
        self,
        *,
        limit: int = 100,
        action: Optional[str] = None,
        dataset_name: Optional[str] = None,
        status: Optional[str] = None,
        incident_id: Optional[str] = None,
        job_id: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []

        if action:
            clauses.append("action = %s")
            params.append(str(action).strip().lower())
        if dataset_name:
            clauses.append("dataset_name = %s")
            params.append(dataset_name)
        if status:
            clauses.append("status = %s")
            params.append(status)
        if incident_id:
            clauses.append("metadata ->> 'incident_id' = %s")
            params.append(incident_id)
        if job_id:
            clauses.append("metadata ->> 'job_id' = %s")
            params.append(job_id)
        if run_id:
            clauses.append("metadata ->> 'run_id' = %s")
            params.append(run_id)

        where_sql = ""
        if clauses:
            where_sql = "WHERE " + " AND ".join(clauses)

        sql = f"""
            SELECT id, timestamp, actor, source, action, dataset_name, status, metadata
            FROM action_audit_log
            {where_sql}
            ORDER BY timestamp DESC
            LIMIT %s
        """
        params.append(max(1, min(int(limit), 500)))

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()

        results: List[Dict[str, Any]] = []
        for row in rows:
            results.append(
                {
                    "id": row[0],
                    "timestamp": row[1].isoformat() if row[1] else None,
                    "actor": row[2],
                    "source": row[3],
                    "action": row[4],
                    "dataset_name": row[5],
                    "status": row[6],
                    "metadata": row[7] or {},
                }
            )

        return results

    def summarize(
        self,
        *,
        window_minutes: int = 60,
        action: Optional[str] = None,
        dataset_name: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Return counts grouped by action + status over a recent time window.
        """
        window_minutes = max(1, min(int(window_minutes), 7 * 24 * 60))

        clauses: List[str] = ["timestamp >= NOW() - (%s * INTERVAL '1 minute')"]
        params: List[Any] = [window_minutes]

        if action:
            clauses.append("action = %s")
            params.append(str(action).strip().lower())
        if dataset_name:
            clauses.append("dataset_name = %s")
            params.append(dataset_name)
        if status:
            clauses.append("status = %s")
            params.append(status)

        where_sql = "WHERE " + " AND ".join(clauses)

        sql = f"""
            SELECT action, status, COUNT(*)::int
            FROM action_audit_log
            {where_sql}
            GROUP BY action, status
            ORDER BY COUNT(*) DESC, action ASC
        """

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()

        return {
            "window_minutes": window_minutes,
            "rows": [{"action": r[0], "status": r[1], "count": int(r[2] or 0)} for r in rows],
        }

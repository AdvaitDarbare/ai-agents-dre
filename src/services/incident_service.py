from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from src.utils.database import get_connection


class IncidentService:
    """Incident lifecycle persistence and transitions (OPEN/ACK/RESOLVED)."""

    VALID_STATUSES = {"OPEN", "ACK", "RESOLVED"}

    @staticmethod
    def _severity_for_run(run_status: str) -> str:
        return "CRITICAL" if str(run_status).upper() == "BLOCKED" else "WARNING"

    @staticmethod
    def _row_to_dict(row: Any) -> Dict[str, Any]:
        metadata = row[15]
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except Exception:
                metadata = {}
        if metadata is None:
            metadata = {}

        created_at = row[11]

        # Keep legacy field names for existing IncidentFeed consumers.
        return {
            "incident_id": row[0],
            "run_id": row[1],
            "dataset_name": row[2],
            "dataset": row[2],
            "severity": row[3],
            "status": row[4],
            "owner": row[5],
            "title": row[6],
            "description": row[7],
            "reason": row[7],
            "quality_score": row[8],
            "anomaly_count": row[9],
            "z_score_max": row[10],
            "timestamp": created_at.isoformat() if created_at else None,
            "created_at": created_at.isoformat() if created_at else None,
            "updated_at": row[12].isoformat() if row[12] else None,
            "acknowledged_at": row[13].isoformat() if row[13] else None,
            "resolved_at": row[14].isoformat() if row[14] else None,
            "metadata": metadata,
        }

    def _fetch_one(self, incident_id: str) -> Dict[str, Any]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT incident_id, run_id, dataset_name, severity, status, owner,
                           title, description, quality_score, anomaly_count, z_score_max,
                           created_at, updated_at, acknowledged_at, resolved_at, metadata
                    FROM incidents
                    WHERE incident_id = %s
                    """,
                    (incident_id,),
                )
                row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"Incident {incident_id} not found")

        return self._row_to_dict(row)

    def create_incident_from_run(
        self,
        *,
        run_id: str,
        dataset_name: str,
        run_status: str,
        reason: str,
        quality_score: float,
        anomaly_count: int,
        z_score_max: float,
        owner: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        incident_id = str(uuid.uuid4())
        severity = self._severity_for_run(run_status)
        title = f"{severity} incident on {dataset_name}"
        base_metadata = {"run_status": run_status}
        if isinstance(metadata, dict):
            base_metadata.update(metadata)

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO incidents (
                        incident_id, run_id, dataset_name, severity, status, owner,
                        title, description, quality_score, anomaly_count, z_score_max,
                        metadata
                    ) VALUES (
                        %s, %s, %s, %s, 'OPEN', %s,
                        %s, %s, %s, %s, %s,
                        %s::jsonb
                    )
                    """,
                    (
                        incident_id,
                        run_id,
                        dataset_name,
                        severity,
                        owner,
                        title,
                        reason,
                        quality_score,
                        anomaly_count,
                        z_score_max,
                        json.dumps(base_metadata),
                    ),
                )

        return self._fetch_one(incident_id)

    def resolve_active_for_dataset(self, dataset_name: str, *, note: str = "Auto-resolved on healthy run") -> int:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE incidents
                    SET status = 'RESOLVED',
                        resolved_at = NOW(),
                        updated_at = NOW(),
                        description = CASE
                            WHEN COALESCE(description, '') = '' THEN %s
                            ELSE description || ' | ' || %s
                        END
                    WHERE dataset_name = %s
                      AND status IN ('OPEN', 'ACK')
                    """,
                    (note, note, dataset_name),
                )
                return int(cur.rowcount or 0)

    def sync_with_run(
        self,
        *,
        run_id: str,
        dataset_name: str,
        run_status: str,
        reason: str,
        quality_score: float,
        anomaly_count: int,
        z_score_max: float,
        owner: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        normalized = str(run_status or "").upper()

        if normalized in {"WARNING", "BLOCKED"}:
            return self.create_incident_from_run(
                run_id=run_id,
                dataset_name=dataset_name,
                run_status=normalized,
                reason=reason,
                quality_score=float(quality_score or 0.0),
                anomaly_count=int(anomaly_count or 0),
                z_score_max=float(z_score_max or 0.0),
                owner=owner,
                metadata=metadata,
            )

        if normalized == "PASSED":
            self.resolve_active_for_dataset(dataset_name)

        return None

    def list_incidents(
        self,
        *,
        limit: int = 50,
        status: Optional[str] = None,
        severity: Optional[str] = None,
        dataset_name: Optional[str] = None,
        owner: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []

        if status:
            clauses.append("status = %s")
            params.append(status.upper())
        if severity:
            clauses.append("severity = %s")
            params.append(severity.upper())
        if dataset_name:
            clauses.append("dataset_name = %s")
            params.append(dataset_name)
        if owner:
            clauses.append("owner = %s")
            params.append(owner)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        sql = f"""
            SELECT incident_id, run_id, dataset_name, severity, status, owner,
                   title, description, quality_score, anomaly_count, z_score_max,
                   created_at, updated_at, acknowledged_at, resolved_at, metadata
            FROM incidents
            {where_sql}
            ORDER BY created_at DESC
            LIMIT %s
        """
        params.append(max(1, min(limit, 500)))

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()

        return [self._row_to_dict(row) for row in rows]

    def get_incident(self, incident_id: str) -> Dict[str, Any]:
        return self._fetch_one(incident_id)

    def update_incident(
        self,
        incident_id: str,
        *,
        status: str,
        owner: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        normalized = str(status or "").upper()
        if normalized not in self.VALID_STATUSES:
            raise HTTPException(status_code=400, detail=f"status must be one of {sorted(self.VALID_STATUSES)}")

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, description
                    FROM incidents
                    WHERE incident_id = %s
                    """,
                    (incident_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail=f"Incident {incident_id} not found")

                current_description = row[1] or ""
                next_description = current_description
                if note:
                    next_description = f"{current_description} | {note}" if current_description else note

                acknowledged_sql = "acknowledged_at"
                resolved_sql = "resolved_at"
                if normalized == "ACK":
                    acknowledged_sql = "COALESCE(acknowledged_at, NOW())"
                    resolved_sql = "NULL"
                elif normalized == "RESOLVED":
                    acknowledged_sql = "COALESCE(acknowledged_at, NOW())"
                    resolved_sql = "NOW()"
                else:  # OPEN
                    acknowledged_sql = "NULL"
                    resolved_sql = "NULL"

                cur.execute(
                    f"""
                    UPDATE incidents
                    SET status = %s,
                        owner = COALESCE(%s, owner),
                        description = %s,
                        updated_at = NOW(),
                        acknowledged_at = {acknowledged_sql},
                        resolved_at = {resolved_sql}
                    WHERE incident_id = %s
                    """,
                    (normalized, owner, next_description, incident_id),
                )

        return self._fetch_one(incident_id)

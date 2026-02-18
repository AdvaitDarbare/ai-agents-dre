from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from src.utils.database import get_connection


class DiagnosticsService:
    """
    Persists check-level failure evidence so debugging can happen from one place.
    """

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _normalize_json(value: Any, default: Any) -> Any:
        if value is None:
            return default
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, (dict, list)):
                    return parsed
            except Exception:
                return default
        return default

    def record_from_verdict(
        self,
        *,
        run_id: str,
        dataset_name: str,
        verdict: Dict[str, Any],
        max_examples_per_check: int = 25,
    ) -> int:
        """
        Extract diagnostics from profile/custom/anomaly output and persist them.
        """
        if not run_id or not dataset_name or not isinstance(verdict, dict):
            return 0

        profile = verdict.get("profile") if isinstance(verdict.get("profile"), dict) else {}
        rows: List[Dict[str, Any]] = []

        def _dimension_note_rows(payload: Any) -> List[Dict[str, Any]]:
            if not isinstance(payload, dict):
                return []
            dims = payload.get("dimensions")
            if not isinstance(dims, list):
                return []

            extracted: List[Dict[str, Any]] = []

            for dim in dims:
                if not isinstance(dim, dict):
                    continue
                name = str(dim.get("name") or "").strip()
                if not name:
                    continue
                violations = dim.get("violations")
                if not isinstance(violations, list):
                    continue

                for violation in violations:
                    text = str(violation or "").strip()
                    if not text:
                        continue

                    lower = text.lower()
                    check_type = f"DIMENSION_{name.upper()}_VIOLATION"
                    column_name = None
                    severity = "warning"
                    if isinstance(dim.get("status"), str):
                        status = dim.get("status").upper()
                        if status == "FAIL":
                            severity = "error"
                        elif status == "PASS":
                            severity = "info"

                    # Heuristics so UI dimension mapping can match by check_type substring.
                    if name.lower() == "completeness" and "null" in lower:
                        check_type = "COMPLETENESS_NULL"
                        if ":" in text:
                            candidate = text.split(":", 1)[0].strip()
                            if candidate:
                                column_name = candidate
                    elif name.lower() == "completeness" and "row" in lower and "count" in lower:
                        check_type = "COMPLETENESS_ROW_COUNT"
                    elif name.lower() == "validity" and "pattern" in lower:
                        check_type = "VALIDITY_PATTERN"
                        if ":" in text:
                            candidate = text.split(":", 1)[0].strip()
                            if candidate:
                                column_name = candidate
                    elif name.lower() == "validity" and "allowed" in lower:
                        check_type = "VALIDITY_ALLOWED"
                        if ":" in text:
                            candidate = text.split(":", 1)[0].strip()
                            if candidate:
                                column_name = candidate

                    extracted.append(
                        {
                            "column_name": column_name,
                            "check_type": check_type,
                            "severity": severity,
                            "violation_count": 1,
                            "sample_records": [],
                            "metadata": {
                                "source": "quality_dimensions",
                                "dimension": name,
                                "note": text,
                            },
                        }
                    )

            return extracted

        # Schema-level issues.
        schema_payload = (
            verdict.get("schema_result")
            if isinstance(verdict.get("schema_result"), dict)
            else (verdict.get("schema") if isinstance(verdict.get("schema"), dict) else {})
        )
        schema_issues = schema_payload.get("issues") if isinstance(schema_payload.get("issues"), list) else []
        for issue in schema_issues:
            if not isinstance(issue, dict):
                continue
            issue_type = str(issue.get("issue_type") or "SCHEMA_ISSUE").upper()
            rows.append(
                {
                    "column_name": issue.get("column"),
                    "check_type": f"SCHEMA_{issue_type}",
                    "severity": str(issue.get("severity") or "error"),
                    "violation_count": 1,
                    "sample_records": [],
                    "metadata": issue,
                }
            )

        # Column-level violations with sample records.
        column_profiles = profile.get("column_profiles") if isinstance(profile.get("column_profiles"), dict) else {}
        for column_name, payload in column_profiles.items():
            if not isinstance(payload, dict):
                continue
            example_groups = payload.get("violation_examples")
            if not isinstance(example_groups, list):
                continue
            for group in example_groups:
                if not isinstance(group, dict):
                    continue
                examples = group.get("examples")
                sample_records = examples if isinstance(examples, list) else []
                rows.append(
                    {
                        "column_name": str(column_name),
                        "check_type": f"COLUMN_{str(group.get('type') or 'VIOLATION').upper()}",
                        "severity": "error",
                        "violation_count": self._safe_int(group.get("count"), len(sample_records)),
                        "sample_records": sample_records[:max_examples_per_check],
                        "metadata": {"source": "profile", "column_quality_score": payload.get("quality_score")},
                    }
                )

        # Dataset-level constraint violations.
        constraint_violations = profile.get("constraint_violations")
        if isinstance(constraint_violations, list):
            for item in constraint_violations:
                if not isinstance(item, dict):
                    continue
                rows.append(
                    {
                        "column_name": None,
                        "check_type": str(item.get("type") or "CONSTRAINT_VIOLATION"),
                        "severity": str(item.get("severity") or "error"),
                        "violation_count": self._safe_int(item.get("actual"), 1),
                        "sample_records": [],
                        "metadata": item,
                    }
                )

        # Custom checks that failed.
        custom_results = profile.get("custom_check_results")
        if isinstance(custom_results, list):
            for item in custom_results:
                if not isinstance(item, dict) or item.get("passed") is True:
                    continue
                rows.append(
                    {
                        "column_name": None,
                        "check_type": f"CUSTOM_CHECK_{str(item.get('name') or 'unknown').upper()}",
                        "severity": str(item.get("severity") or "warning"),
                        "violation_count": self._safe_int(item.get("violation_count"), 1),
                        "sample_records": [],
                        "metadata": item,
                    }
                )

        # Anomaly events captured as diagnostics signals.
        anomalies = verdict.get("anomalies")
        if isinstance(anomalies, list):
            for item in anomalies:
                if not isinstance(item, dict):
                    continue
                rows.append(
                    {
                        "column_name": str(item.get("column_name") or item.get("column") or "") or None,
                        "check_type": f"ANOMALY_{str(item.get('metric_name') or item.get('metric') or 'metric').upper()}",
                        "severity": "warning",
                        "violation_count": 1,
                        "sample_records": [],
                        "metadata": item,
                    }
                )

        # Dimension-level notes (6D) so evidence exists even when contracts allow nulls.
        rows.extend(_dimension_note_rows(verdict.get("quality_dimensions")))

        if not rows:
            return 0

        inserted = 0
        with get_connection() as conn:
            with conn.cursor() as cur:
                for row in rows:
                    cur.execute(
                        """
                        INSERT INTO diagnostics_records
                        (run_id, dataset_name, column_name, check_type, severity, violation_count, sample_records, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                        """,
                        (
                            run_id,
                            dataset_name,
                            row["column_name"],
                            row["check_type"],
                            row["severity"],
                            max(0, self._safe_int(row.get("violation_count"), 0)),
                            json.dumps(row.get("sample_records") or []),
                            json.dumps(row.get("metadata") or {}),
                        ),
                    )
                    inserted += 1
        return inserted

    def list_records(
        self,
        *,
        dataset_name: str,
        run_id: Optional[str] = None,
        check_type: Optional[str] = None,
        limit: int = 200,
    ) -> Dict[str, Any]:
        safe_limit = max(1, min(int(limit), 1000))
        clauses = ["dataset_name = %s"]
        params: List[Any] = [dataset_name]

        if run_id:
            clauses.append("run_id = %s")
            params.append(run_id)
        if check_type:
            clauses.append("check_type ILIKE %s")
            params.append(check_type)

        where_sql = " AND ".join(clauses)
        params.append(safe_limit)

        records: List[Dict[str, Any]] = []
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, run_id, dataset_name, column_name, check_type, severity,
                           violation_count, sample_records, metadata, created_at
                    FROM diagnostics_records
                    WHERE {where_sql}
                    ORDER BY created_at DESC, id DESC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                for row in cur.fetchall() or []:
                    sample_records = self._normalize_json(row[7], [])
                    metadata = self._normalize_json(row[8], {})
                    records.append(
                        {
                            "id": int(row[0]),
                            "run_id": row[1],
                            "dataset_name": row[2],
                            "column_name": row[3],
                            "check_type": row[4],
                            "severity": row[5],
                            "violation_count": self._safe_int(row[6], 0),
                            "sample_records": sample_records if isinstance(sample_records, list) else [],
                            "metadata": metadata if isinstance(metadata, dict) else {},
                            "created_at": row[9].isoformat() if row[9] else None,
                        }
                    )

        return {
            "dataset_name": dataset_name,
            "run_id": run_id,
            "check_type": check_type,
            "limit": safe_limit,
            "records": records,
            "total": len(records),
        }

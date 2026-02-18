from datetime import datetime, timedelta

import src.services.reliability_service as reliability_module
from src.services.reliability_service import ReliabilityService


class _StubAgent:
    def discover_datasets(self):
        return []


class _StubContractStore:
    def exists(self, _dataset_name):
        return True


class _FakeCursor:
    def __init__(self, rows_by_key):
        self.rows_by_key = rows_by_key
        self.last_sql = ""
        self.execute_calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=()):
        self.last_sql = sql
        self.execute_calls.append((sql, params))

    def fetchall(self):
        sql = self.last_sql
        if "FROM action_audit_log" in sql:
            return self.rows_by_key.get("audit", [])
        if "FROM async_jobs" in sql:
            return self.rows_by_key.get("jobs", [])
        if "FROM run_history" in sql:
            return self.rows_by_key.get("runs", [])
        if "FROM tool_outputs" in sql:
            return self.rows_by_key.get("tools", [])
        if "FROM incidents" in sql:
            return self.rows_by_key.get("incidents", [])
        return []


class _FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self._cursor


def _build_service(monkeypatch, rows_by_key):
    cursor = _FakeCursor(rows_by_key)
    conn = _FakeConnection(cursor)
    monkeypatch.setattr(reliability_module, "get_connection", lambda: conn)
    service = ReliabilityService(
        agent=_StubAgent(),
        contract_store=_StubContractStore(),
        hitl_workflow=object(),
        audit_service=None,
        agentic_workflow=object(),
    )
    return service, cursor


def test_workflow_timeline_merges_channels_and_summarizes(monkeypatch):
    now = datetime(2026, 2, 18, 12, 0, 0)
    service, _cursor = _build_service(
        monkeypatch,
        {
            "audit": [
                (
                    "aud-1",
                    now - timedelta(minutes=2),
                    "job_evaluate_enqueued",
                    "QUEUED",
                    "orders",
                    "api",
                    "user",
                    {"job_id": "job-1", "run_id": "run-1"},
                )
            ],
            "jobs": [
                (
                    "job-1",
                    "evaluate",
                    "orders",
                    "RUNNING",
                    now - timedelta(minutes=3),
                    now - timedelta(minutes=2),
                    None,
                    {"dataset_name": "orders"},
                    None,
                    None,
                )
            ],
            "runs": [
                (
                    "run-1",
                    now - timedelta(minutes=4),
                    "orders",
                    "WARNING",
                    71.2,
                    2,
                    3.4,
                    "anomaly spike",
                    250,
                )
            ],
            "tools": [
                (
                    10,
                    now - timedelta(minutes=5),
                    "orders",
                    "run-1",
                    "anomaly_detector",
                    "ok",
                    25,
                    {"anomaly_count": 2},
                )
            ],
            "incidents": [
                (
                    "inc-1",
                    now - timedelta(minutes=1),
                    "orders",
                    "HIGH",
                    "OPEN",
                    None,
                    "Data quality incident",
                    "run-1",
                    {"ticket": "INC-100"},
                )
            ],
        },
    )

    payload = service.get_workflow_timeline(dataset_name="orders", limit=50)

    assert payload["summary"]["total_events"] == 5
    assert payload["summary"]["channels"]["audit"] == 1
    assert payload["summary"]["channels"]["job"] == 1
    assert payload["summary"]["channels"]["run"] == 1
    assert payload["summary"]["channels"]["tool"] == 1
    assert payload["summary"]["channels"]["incident"] == 1
    assert payload["summary"]["active_jobs"] == 1
    assert payload["summary"]["active_incidents"] == 1

    # Most recent event should be incident due to latest timestamp.
    assert payload["events"][0]["event_id"] == "incident:inc-1"

    # Audit event should preserve cross-refs from metadata.
    audit_event = next(event for event in payload["events"] if event["channel"] == "audit")
    assert audit_event["refs"]["job_id"] == "job-1"
    assert audit_event["refs"]["run_id"] == "run-1"


def test_workflow_timeline_applies_dataset_filter_to_all_queries(monkeypatch):
    service, cursor = _build_service(
        monkeypatch,
        {
            "audit": [],
            "jobs": [],
            "runs": [],
            "tools": [],
            "incidents": [],
        },
    )

    service.get_workflow_timeline(dataset_name="orders", limit=25)

    filtered_query_count = 0
    for sql, params in cursor.execute_calls:
        if "WHERE dataset_name = %s" in sql:
            filtered_query_count += 1
            assert params[0] == "orders"

    assert filtered_query_count == 5

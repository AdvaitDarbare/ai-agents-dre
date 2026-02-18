from __future__ import annotations

import src.services.diagnostics_service as diagnostics_module
from src.services.diagnostics_service import DiagnosticsService


class _FakeCursor:
    def __init__(self):
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=()):
        self.executed.append((sql, params))


class _FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self._cursor


def test_diagnostics_service_persists_schema_result_issues(monkeypatch):
    cursor = _FakeCursor()
    monkeypatch.setattr(diagnostics_module, "get_connection", lambda: _FakeConnection(cursor))

    verdict = {
        "schema_result": {
            "issues": [
                {
                    "issue_type": "missing_column",
                    "column": "email",
                    "severity": "error",
                    "message": "Missing required column email",
                }
            ]
        },
        "profile": {},
        "anomalies": [],
    }

    inserted = DiagnosticsService().record_from_verdict(
        run_id="run-1",
        dataset_name="orders",
        verdict=verdict,
    )

    assert inserted == 1
    assert cursor.executed, "Expected INSERT into diagnostics_records"

    sql, params = cursor.executed[0]
    assert "INSERT INTO diagnostics_records" in sql
    assert params[0] == "run-1"
    assert params[1] == "orders"
    assert params[2] == "email"
    assert params[3] == "SCHEMA_MISSING_COLUMN"


def test_diagnostics_service_persists_dimension_completeness_null_notes(monkeypatch):
    cursor = _FakeCursor()
    monkeypatch.setattr(diagnostics_module, "get_connection", lambda: _FakeConnection(cursor))

    verdict = {
        "schema_result": {"issues": []},
        "profile": {},
        "anomalies": [],
        "quality_dimensions": {
            "dataset_name": "orders",
            "timestamp": "2026-02-18T10:00:00Z",
            "overall_score": 81.8,
            "dimensions": [
                {
                    "name": "Completeness",
                    "score": 81.8,
                    "weight": 0.25,
                    "status": "WARN",
                    "check_count": {"total": 11, "passed": 9, "failed": 2},
                    "violations": ["Visit Date: 817 null values (81.7% null rate)"],
                }
            ],
        },
    }

    inserted = DiagnosticsService().record_from_verdict(
        run_id="run-2",
        dataset_name="orders",
        verdict=verdict,
    )

    assert inserted == 1
    sql, params = cursor.executed[0]
    assert "INSERT INTO diagnostics_records" in sql
    assert params[0] == "run-2"
    assert params[1] == "orders"
    assert params[2] == "Visit Date"
    assert params[3] == "COMPLETENESS_NULL"

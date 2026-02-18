import json
from datetime import datetime

import src.services.reliability_service as reliability_module
from src.services.reliability_service import ReliabilityService


class _StubAgent:
    def discover_datasets(self):
        return []


class _StubContractStore:
    def exists(self, _dataset_name):
        return True


class _FakeCursor:
    def __init__(self, row):
        self._row = row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, _sql, _params=()):
        return None

    def fetchone(self):
        return self._row


class _FakeConnection:
    def __init__(self, row):
        self._cursor = _FakeCursor(row)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self._cursor


def _build_service(monkeypatch, row):
    monkeypatch.setattr(reliability_module, "get_connection", lambda: _FakeConnection(row))
    return ReliabilityService(
        agent=_StubAgent(),
        contract_store=_StubContractStore(),
        hitl_workflow=None,
    )


def test_get_run_verdict_handles_jsonb_dict_values(monkeypatch):
    row = (
        "run-1",
        datetime(2026, 2, 18, 10, 0, 0),
        "orders",
        "WARNING",
        81.0,
        2,
        3.1,
        "spike",
        120,
        {"dimensions": [{"name": "Completeness", "score": 95.0}]},
        {"anomalies": [{"metric": "row_count", "z_score": 3.1}]},
    )
    service = _build_service(monkeypatch, row)

    verdict = service.get_run_verdict("run-1")

    assert isinstance(verdict["dimension_scores"], dict)
    assert isinstance(verdict["full_verdict"], dict)
    assert verdict["full_verdict"]["anomalies"][0]["metric"] == "row_count"


def test_get_run_verdict_handles_jsonb_string_values(monkeypatch):
    row = (
        "run-2",
        datetime(2026, 2, 18, 10, 1, 0),
        "orders",
        "PASSED",
        98.0,
        0,
        0.2,
        "ok",
        90,
        json.dumps({"dimensions": [{"name": "Validity", "score": 99.0}]}),
        json.dumps({"anomalies": []}),
    )
    service = _build_service(monkeypatch, row)

    verdict = service.get_run_verdict("run-2")

    assert isinstance(verdict["dimension_scores"], dict)
    assert isinstance(verdict["full_verdict"], dict)
    assert verdict["status"] == "PASSED"

from contextlib import contextmanager
from datetime import datetime, timezone

from fastapi.testclient import TestClient

import src.api as api


class _Cursor:
    def __init__(self, responses):
        self.responses = list(responses)
        self.idx = -1
        self.current = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, _sql, _params=None):
        self.idx += 1
        self.current = self.responses[self.idx] if self.idx < len(self.responses) else None

    def fetchall(self):
        if isinstance(self.current, list):
            return self.current
        return []

    def fetchone(self):
        if isinstance(self.current, tuple):
            return self.current
        if isinstance(self.current, list) and self.current:
            return self.current[0]
        return None


class _Conn:
    def __init__(self, responses):
        self.responses = responses

    def cursor(self):
        return _Cursor(self.responses)


def _fake_connection(responses):
    @contextmanager
    def _ctx():
        yield _Conn(responses)

    return _ctx


def test_metric_timeseries_returns_enriched_rows_and_baseline(monkeypatch):
    ts1 = datetime(2026, 2, 15, 10, 0, tzinfo=timezone.utc)
    ts2 = datetime(2026, 2, 15, 11, 0, tzinfo=timezone.utc)
    rows = [
        (ts2, 1200.0, "run-2", 0, "volume", None, "global", {"source": "detector"}),
        (ts1, 1000.0, "run-1", 0, "volume", None, "global", {"source": "detector"}),
    ]
    baseline = (1100.0, 50.0, "rolling", 14)

    monkeypatch.setattr(api, "get_connection", _fake_connection([rows, baseline]))

    client = TestClient(api.app)
    response = client.get("/metrics/orders/timeseries?metric=row_count&limit=2")

    assert response.status_code == 200
    body = response.json()
    assert body["dataset"] == "orders"
    assert body["metric"] == "row_count"
    assert body["baseline"]["mean"] == 1100.0
    assert body["baseline"]["upper_3sigma"] == 1250.0
    assert len(body["data"]) == 2
    assert body["data"][0]["run_id"] == "run-1"
    assert body["data"][1]["run_id"] == "run-2"
    assert body["data"][0]["metric_group"] == "volume"


def test_slo_history_endpoint_returns_rows(monkeypatch):
    ts = datetime(2026, 2, 16, 2, 42, tzinfo=timezone.utc)
    slo_rows = [
        (ts, "run-2", "availability", ">=", 0.99, 1.0, "PASS", 0.0, {"window": 2}),
        (ts, "run-1", "quality_score_min", ">=", 0.95, 0.92, "FAIL", 0.5, {"window": 2}),
    ]
    monkeypatch.setattr(api, "get_connection", _fake_connection([slo_rows]))

    client = TestClient(api.app)
    response = client.get("/slos/orders?limit=2")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    assert body[0]["slo_name"] == "availability"
    assert body[1]["status"] == "FAIL"
    assert body[1]["metadata"]["window"] == 2


def test_slo_summary_delegates_to_service(monkeypatch):
    class _Service:
        def get_slo_summary(self, dataset_name: str, window: int):
            return {
                "dataset_name": dataset_name,
                "window": window,
                "overall_pass_rate": 75.0,
                "total_checks": 4,
                "checks": [],
            }

    monkeypatch.setattr(api, "service", _Service())

    client = TestClient(api.app)
    response = client.get("/slos/orders/summary?window=42")

    assert response.status_code == 200
    body = response.json()
    assert body["dataset_name"] == "orders"
    assert body["window"] == 42
    assert body["overall_pass_rate"] == 75.0

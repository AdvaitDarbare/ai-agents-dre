from contextlib import contextmanager
from datetime import datetime, timezone

import src.services.reliability_service as rs_module
from src.services.reliability_service import ReliabilityService


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


class _Agent:
    pass


class _Store:
    pass


def test_slo_summary_includes_streak_and_budget_fields(monkeypatch):
    ts = datetime(2026, 2, 17, 12, 0, tzinfo=timezone.utc)
    grouped_rows = [
        ("availability", 3, 2, 0.1, 0.3, ts),
        ("quality_score_min", 3, 1, 0.5, 1.5, ts),
    ]
    recent_rows = [
        ("availability", "FAIL", 0.3, ts),
        ("availability", "PASS", 0.0, ts),
        ("availability", "PASS", 0.0, ts),
        ("quality_score_min", "FAIL", 1.0, ts),
        ("quality_score_min", "FAIL", 0.5, ts),
        ("quality_score_min", "PASS", 0.0, ts),
    ]
    totals = (6, 3, 0.3, 1.8)

    monkeypatch.setattr(
        rs_module,
        "get_connection",
        _fake_connection([grouped_rows, recent_rows, totals]),
    )

    service = ReliabilityService(agent=_Agent(), contract_store=_Store())
    summary = service.get_slo_summary("orders", window=50)

    assert summary["dataset_name"] == "orders"
    assert summary["overall_status"] == "FAIL"
    assert summary["failing_checks"] == 3
    assert summary["failing_slo_count"] == 2
    assert summary["failing_slos"] == ["availability", "quality_score_min"]
    assert summary["overall_error_budget_burn_total"] == 1.8

    by_name = {row["slo_name"]: row for row in summary["checks"]}
    assert by_name["availability"]["last_status"] == "FAIL"
    assert by_name["availability"]["recent_fail_streak"] == 1
    assert by_name["quality_score_min"]["recent_fail_streak"] == 2

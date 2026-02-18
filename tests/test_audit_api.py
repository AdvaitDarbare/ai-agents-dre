from fastapi.testclient import TestClient

import src.api as api


class _StubAudit:
    def __init__(self):
        self.calls = []

    def list_actions(self, **kwargs):
        self.calls.append(("list_actions", kwargs))
        return [
            {
                "id": "a1",
                "timestamp": "2026-02-16T00:00:00Z",
                "actor": "system",
                "source": "async_jobs",
                "action": "job_evaluate_enqueued",
                "dataset_name": "orders",
                "status": "QUEUED",
                "metadata": {"job_id": "job-1"},
            }
        ]

    def summarize(self, **kwargs):
        self.calls.append(("summarize", kwargs))
        return {
            "window_minutes": kwargs.get("window_minutes", 60),
            "rows": [{"action": "job_evaluate_enqueued", "status": "QUEUED", "count": 3}],
        }


def test_list_audit_endpoint(monkeypatch):
    stub = _StubAudit()
    monkeypatch.setattr(api, "audit_service", stub)

    client = TestClient(api.app)
    response = client.get(
        "/audit?limit=25&action=job_evaluate_enqueued&dataset_name=orders&status=QUEUED&incident_id=inc-1&job_id=job-1&run_id=run-1"
    )

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, list)
    assert body[0]["action"] == "job_evaluate_enqueued"
    assert stub.calls


def test_audit_summary_endpoint(monkeypatch):
    stub = _StubAudit()
    monkeypatch.setattr(api, "audit_service", stub)

    client = TestClient(api.app)
    response = client.get("/audit/summary?window_minutes=120&action=job_evaluate_enqueued&status=QUEUED")

    assert response.status_code == 200
    body = response.json()
    assert body["window_minutes"] == 120
    assert isinstance(body["rows"], list)
    assert stub.calls

from fastapi.testclient import TestClient

import src.api as api


class StubIncidentService:
    def __init__(self):
        self.calls = []

    def list_incidents(self, **kwargs):
        self.calls.append(("list", kwargs))
        return [
            {
                "incident_id": "inc-1",
                "run_id": "run-1",
                "dataset": "orders",
                "severity": "CRITICAL",
                "status": "OPEN",
            }
        ]

    def get_incident(self, incident_id: str):
        self.calls.append(("get", incident_id))
        return {
            "incident_id": incident_id,
            "status": "OPEN",
            "dataset": "orders",
            "severity": "CRITICAL",
        }

    def update_incident(self, incident_id: str, *, status: str, owner=None, note=None):
        self.calls.append(("update", incident_id, status, owner, note))
        return {
            "incident_id": incident_id,
            "status": status.upper(),
            "owner": owner,
            "note": note,
        }


def test_list_incidents_with_filters(monkeypatch):
    stub = StubIncidentService()
    monkeypatch.setattr(api, "incident_service", stub)

    client = TestClient(api.app)
    response = client.get("/incidents?limit=20&status=OPEN&severity=CRITICAL&dataset_name=orders&owner=alice")

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, list)
    assert body[0]["incident_id"] == "inc-1"


def test_get_incident(monkeypatch):
    stub = StubIncidentService()
    monkeypatch.setattr(api, "incident_service", stub)

    client = TestClient(api.app)
    response = client.get("/incidents/inc-123")

    assert response.status_code == 200
    assert response.json()["incident_id"] == "inc-123"


def test_update_incident(monkeypatch):
    stub = StubIncidentService()
    monkeypatch.setattr(api, "incident_service", stub)

    client = TestClient(api.app)
    response = client.patch(
        "/incidents/inc-123",
        json={"status": "ACK", "owner": "alice", "note": "Investigating"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ACK"
    assert body["owner"] == "alice"

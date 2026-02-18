from fastapi.testclient import TestClient

import src.api as api


class _StubAudit:
    def __init__(self):
        self.records = []

    def record(self, **kwargs):
        self.records.append(kwargs)
        return "audit-1"


def test_audit_on_incident_update(monkeypatch):
    audit = _StubAudit()
    monkeypatch.setattr(api, "audit_service", audit)

    class _StubIncidents:
        def update_incident(self, incident_id: str, *, status: str, owner=None, note=None):
            _ = (owner, note)
            return {"incident_id": incident_id, "dataset_name": "orders", "status": status}

    monkeypatch.setattr(api, "incident_service", _StubIncidents())

    client = TestClient(api.app)
    response = client.patch("/incidents/inc-1", json={"status": "ACK", "note": "test"})
    assert response.status_code == 200
    assert audit.records
    assert audit.records[-1]["action"] == "incident_updated"


def test_audit_on_contract_approve(monkeypatch):
    audit = _StubAudit()
    monkeypatch.setattr(api, "audit_service", audit)

    class _StubService:
        def approve_contract(self, dataset_name: str, approved_yaml: str):
            _ = approved_yaml
            return {"status": "approved", "dataset_name": dataset_name}

    monkeypatch.setattr(api, "service", _StubService())

    client = TestClient(api.app)
    response = client.post("/contracts/approve", json={"dataset_name": "orders", "approved_yaml": "kind: DataContract\n"})
    assert response.status_code == 200
    assert audit.records[-1]["action"] == "contract_approved"


def test_audit_on_contract_reject(monkeypatch):
    audit = _StubAudit()
    monkeypatch.setattr(api, "audit_service", audit)

    class _StubService:
        def reject_contract_proposal(self, dataset_name: str):
            return {"status": "rejected", "dataset_name": dataset_name}

    monkeypatch.setattr(api, "service", _StubService())

    client = TestClient(api.app)
    response = client.delete("/contracts/pending/orders")
    assert response.status_code == 200
    assert audit.records[-1]["action"] == "contract_rejected"


def test_audit_on_job_delete_requested_includes_job_id(monkeypatch):
    audit = _StubAudit()
    monkeypatch.setattr(api, "audit_service", audit)

    class _StubPolicy:
        def evaluate_action(self, **kwargs):
            return {"decision": "allow", "action": kwargs.get("action", "")}

        def enforce(self, decision, *, approved=False, reason=None):
            _ = (decision, approved, reason)
            return None

    class _StubAsyncJobs:
        def submit_delete(self, dataset_name: str):
            return {"job_id": "job-del-123", "action": "delete", "dataset_name": dataset_name, "status": "QUEUED"}

    monkeypatch.setattr(api, "policy_service", _StubPolicy())
    monkeypatch.setattr(api, "async_jobs", _StubAsyncJobs())

    client = TestClient(api.app)
    response = client.post("/jobs/delete/orders", json={"confirm": True})
    assert response.status_code == 200
    assert audit.records
    assert audit.records[-1]["action"] == "job_delete_requested"
    assert audit.records[-1]["metadata"]["job_id"] == "job-del-123"

from fastapi import HTTPException
from fastapi.testclient import TestClient

import src.api as api


class _StubPolicy:
    def __init__(self, require=False):
        self.require = require

    def evaluate_action(self, **kwargs):
        decision = "approval_required" if self.require else "allow"
        return {
            "action": kwargs.get("action", ""),
            "decision": decision,
            "reason": "stub-policy",
            "required_controls": ["policy_approved", "policy_reason"] if self.require else [],
            "targets": [],
            "criticalities": {},
        }

    def enforce(self, decision, *, approved=False, reason=None):
        if decision.get("decision") != "approval_required":
            return
        if approved and str(reason or "").strip():
            return
        required = decision.get("required_controls") or []
        missing = []
        if "policy_approved" in required and not approved:
            missing.append("policy_approved")
        if "policy_reason" in required and not str(reason or "").strip():
            missing.append("policy_reason")
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Policy approval required for this action.",
                "policy": decision,
                "missing_controls": missing,
            },
        )


class _StubAsyncJobs:
    def submit_delete(self, dataset_name):
        return {
            "job_id": "job-del-1",
            "action": "delete",
            "dataset_name": dataset_name,
            "status": "QUEUED",
        }

    def submit_bulk_delete(self, dataset_names):
        return {
            "job_id": "job-bulk-del-1",
            "action": "bulk_delete",
            "dataset_name": ",".join(dataset_names),
            "status": "QUEUED",
        }

    def submit_apply_remediation(self, *, dataset_name: str, proposed_yaml: str, error_context: str):
        _ = (proposed_yaml, error_context)
        return {
            "job_id": "job-remed-1",
            "action": "remediation_apply",
            "dataset_name": dataset_name,
            "status": "QUEUED",
        }


class _StubService:
    def delete_dataset(self, dataset_name):
        return {"status": "deleted", "dataset_name": dataset_name}


def test_policy_check_endpoint(monkeypatch):
    monkeypatch.setattr(api, "policy_service", _StubPolicy(require=True))

    client = TestClient(api.app)
    response = client.post("/policy/check", json={"action": "delete", "dataset_name": "orders"})

    assert response.status_code == 200
    body = response.json()
    assert body["decision"] == "approval_required"
    assert body["action"] == "delete"


def test_jobs_delete_blocked_when_policy_requires_approval(monkeypatch):
    monkeypatch.setattr(api, "policy_service", _StubPolicy(require=True))
    monkeypatch.setattr(api, "async_jobs", _StubAsyncJobs())

    client = TestClient(api.app)
    response = client.post("/jobs/delete/orders", json={"confirm": True})

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert "policy" in detail
    assert "missing_controls" in detail


def test_jobs_delete_allowed_when_policy_approved(monkeypatch):
    monkeypatch.setattr(api, "policy_service", _StubPolicy(require=True))
    monkeypatch.setattr(api, "async_jobs", _StubAsyncJobs())

    client = TestClient(api.app)
    response = client.post(
        "/jobs/delete/orders",
        json={
            "confirm": True,
            "policy_approved": True,
            "policy_reason": "Approved by on-call engineer",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "QUEUED"

def test_jobs_bulk_delete_blocked_when_policy_requires_approval(monkeypatch):
    monkeypatch.setattr(api, "policy_service", _StubPolicy(require=True))
    monkeypatch.setattr(api, "async_jobs", _StubAsyncJobs())

    client = TestClient(api.app)
    response = client.post(
        "/jobs/delete-bulk",
        json={"dataset_names": ["orders", "customers"], "confirm": True},
    )

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert "policy" in detail
    assert "missing_controls" in detail


def test_jobs_bulk_delete_allowed_when_policy_approved(monkeypatch):
    monkeypatch.setattr(api, "policy_service", _StubPolicy(require=True))
    monkeypatch.setattr(api, "async_jobs", _StubAsyncJobs())

    client = TestClient(api.app)
    response = client.post(
        "/jobs/delete-bulk",
        json={
            "dataset_names": ["orders", "customers"],
            "confirm": True,
            "policy_approved": True,
            "policy_reason": "Approved by ops",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "QUEUED"


def test_jobs_remediation_blocked_when_policy_requires_approval(monkeypatch):
    monkeypatch.setattr(api, "policy_service", _StubPolicy(require=True))
    monkeypatch.setattr(api, "async_jobs", _StubAsyncJobs())

    client = TestClient(api.app)
    response = client.post(
        "/jobs/remediation/apply",
        json={
            "dataset_name": "orders",
            "proposed_yaml": "kind: DataContract\napiVersion: v3.1.0\ncolumns: []\n",
            "error_context": "Unit test",
        },
    )

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert "policy" in detail
    assert "missing_controls" in detail


def test_jobs_remediation_allowed_when_policy_approved(monkeypatch):
    monkeypatch.setattr(api, "policy_service", _StubPolicy(require=True))
    monkeypatch.setattr(api, "async_jobs", _StubAsyncJobs())

    client = TestClient(api.app)
    response = client.post(
        "/jobs/remediation/apply",
        json={
            "dataset_name": "orders",
            "proposed_yaml": "kind: DataContract\napiVersion: v3.1.0\ncolumns: []\n",
            "error_context": "Unit test",
            "policy_approved": True,
            "policy_reason": "Reviewed by owner",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "QUEUED"


def test_sync_delete_blocked_without_policy_approval(monkeypatch):
    monkeypatch.setattr(api, "policy_service", _StubPolicy(require=True))
    monkeypatch.setattr(api, "service", _StubService())

    client = TestClient(api.app)
    response = client.delete("/datasets/orders")

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert "policy" in detail
    assert detail.get("missing_controls") == ["policy_approved", "policy_reason"]


def test_sync_delete_blocked_when_policy_reason_missing(monkeypatch):
    monkeypatch.setattr(api, "policy_service", _StubPolicy(require=True))
    monkeypatch.setattr(api, "service", _StubService())

    client = TestClient(api.app)
    response = client.delete("/datasets/orders?policy_approved=true")

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert "policy" in detail
    assert detail.get("missing_controls") == ["policy_reason"]


def test_sync_delete_allowed_with_policy_approval(monkeypatch):
    monkeypatch.setattr(api, "policy_service", _StubPolicy(require=True))
    monkeypatch.setattr(api, "service", _StubService())

    client = TestClient(api.app)
    response = client.delete(
        "/datasets/orders?policy_approved=true&policy_reason=approved%20by%20ops",
    )

    assert response.status_code == 200
    assert response.json()["status"] == "deleted"

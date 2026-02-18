from fastapi.testclient import TestClient

import src.api as api


class StubAsyncJobs:
    def __init__(self):
        self.calls = []

    def submit_evaluate(self, dataset_name: str):
        self.calls.append(("submit_evaluate", dataset_name))
        return {
            "job_id": "job-eval-1",
            "action": "evaluate",
            "dataset_name": dataset_name,
            "status": "QUEUED",
        }

    def submit_delete(self, dataset_name: str):
        self.calls.append(("submit_delete", dataset_name))
        return {
            "job_id": "job-del-1",
            "action": "delete",
            "dataset_name": dataset_name,
            "status": "QUEUED",
        }

    def submit_bulk_delete(self, dataset_names):
        self.calls.append(("submit_bulk_delete", dataset_names))
        return {
            "job_id": "job-bulk-del-1",
            "action": "bulk_delete",
            "dataset_name": "orders,customers",
            "status": "QUEUED",
        }

    def submit_bulk_evaluate(self, dataset_names):
        self.calls.append(("submit_bulk_evaluate", dataset_names))
        return {
            "job_id": "job-bulk-eval-1",
            "action": "bulk_evaluate",
            "dataset_name": "orders,customers",
            "status": "QUEUED",
        }

    def submit_apply_remediation(self, *, dataset_name: str, proposed_yaml: str, error_context: str):
        self.calls.append(("submit_apply_remediation", dataset_name, proposed_yaml, error_context))
        return {
            "job_id": "job-remed-1",
            "action": "remediation_apply",
            "dataset_name": dataset_name,
            "status": "QUEUED",
        }

    def list_jobs(self, **kwargs):
        self.calls.append(("list_jobs", kwargs))
        return [
            {
                "job_id": "job-eval-1",
                "action": "evaluate",
                "dataset_name": "orders",
                "status": "RUNNING",
            }
        ]

    def get_job(self, job_id: str):
        self.calls.append(("get_job", job_id))
        return {
            "job_id": job_id,
            "action": "evaluate",
            "dataset_name": "orders",
            "status": "COMPLETED",
        }


class StubAllowPolicy:
    def evaluate_action(self, **kwargs):
        return {"decision": "allow", "action": kwargs.get("action", "")}

    def enforce(self, decision, *, approved=False, reason=None):
        _ = (decision, approved, reason)
        return None


def _patch_allow_policy(monkeypatch):
    monkeypatch.setattr(api, "policy_service", StubAllowPolicy())


def test_enqueue_evaluate_job(monkeypatch):
    stub = StubAsyncJobs()
    monkeypatch.setattr(api, "async_jobs", stub)

    client = TestClient(api.app)
    response = client.post("/jobs/evaluate/orders")

    assert response.status_code == 200
    body = response.json()
    assert body["job_id"] == "job-eval-1"
    assert body["status"] == "QUEUED"


def test_enqueue_delete_job_requires_confirm(monkeypatch):
    stub = StubAsyncJobs()
    monkeypatch.setattr(api, "async_jobs", stub)
    _patch_allow_policy(monkeypatch)

    client = TestClient(api.app)
    response = client.post("/jobs/delete/orders", json={"confirm": False})

    assert response.status_code == 400
    assert "confirm=true" in response.json()["detail"]


def test_enqueue_delete_job(monkeypatch):
    stub = StubAsyncJobs()
    monkeypatch.setattr(api, "async_jobs", stub)
    _patch_allow_policy(monkeypatch)

    client = TestClient(api.app)
    response = client.post("/jobs/delete/orders", json={"confirm": True})

    assert response.status_code == 200
    body = response.json()
    assert body["job_id"] == "job-del-1"
    assert body["action"] == "delete"


def test_enqueue_bulk_delete_requires_confirm(monkeypatch):
    stub = StubAsyncJobs()
    monkeypatch.setattr(api, "async_jobs", stub)
    _patch_allow_policy(monkeypatch)

    client = TestClient(api.app)
    response = client.post("/jobs/delete-bulk", json={"dataset_names": ["orders"], "confirm": False})

    assert response.status_code == 400
    assert "confirm=true" in response.json()["detail"]


def test_enqueue_bulk_delete_job(monkeypatch):
    stub = StubAsyncJobs()
    monkeypatch.setattr(api, "async_jobs", stub)
    _patch_allow_policy(monkeypatch)

    client = TestClient(api.app)
    response = client.post(
        "/jobs/delete-bulk",
        json={"dataset_names": ["orders", "customers"], "confirm": True},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["job_id"] == "job-bulk-del-1"
    assert body["action"] == "bulk_delete"


def test_enqueue_apply_remediation_job(monkeypatch):
    stub = StubAsyncJobs()
    monkeypatch.setattr(api, "async_jobs", stub)
    _patch_allow_policy(monkeypatch)

    client = TestClient(api.app)
    response = client.post(
        "/jobs/remediation/apply",
        json={
            "dataset_name": "orders",
            "proposed_yaml": "kind: DataContract\napiVersion: v3.0.0\ncolumns: []\n",
            "error_context": "Missing column",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["job_id"] == "job-remed-1"
    assert body["action"] == "remediation_apply"

def test_enqueue_bulk_evaluate_job(monkeypatch):
    stub = StubAsyncJobs()
    monkeypatch.setattr(api, "async_jobs", stub)

    client = TestClient(api.app)
    response = client.post(
        "/jobs/evaluate-bulk",
        json={"dataset_names": ["orders", "customers"]},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["job_id"] == "job-bulk-eval-1"
    assert body["action"] == "bulk_evaluate"

def test_enqueue_evaluate_all_job(monkeypatch):
    stub = StubAsyncJobs()
    monkeypatch.setattr(api, "async_jobs", stub)

    class _StubAgent:
        def discover_datasets(self):
            return [
                {"name": "orders", "data_file": "data/orders.csv", "lifecycle": "active"},
                {"name": "customers", "data_file": "data/customers.csv", "lifecycle": "unconfigured"},
                {"name": "no_file", "data_file": None, "lifecycle": "active"},
            ]

    monkeypatch.setattr(api, "agent", _StubAgent())

    client = TestClient(api.app)
    response = client.post("/jobs/evaluate-all", json={"include_unconfigured": True})

    assert response.status_code == 200
    body = response.json()
    assert body["action"] == "bulk_evaluate"

def test_enqueue_evaluate_all_excludes_unconfigured(monkeypatch):
    stub = StubAsyncJobs()
    monkeypatch.setattr(api, "async_jobs", stub)

    class _StubAgent:
        def discover_datasets(self):
            return [
                {"name": "orders", "data_file": "data/orders.csv", "lifecycle": "active"},
                {"name": "customers", "data_file": "data/customers.csv", "lifecycle": "unconfigured"},
            ]

    monkeypatch.setattr(api, "agent", _StubAgent())

    client = TestClient(api.app)
    response = client.post("/jobs/evaluate-all", json={"include_unconfigured": False})

    assert response.status_code == 200
    # Ensure only the active dataset was passed through.
    assert ("submit_bulk_evaluate", ["orders"]) in stub.calls


def test_enqueue_evaluate_all_returns_400_when_no_data_files(monkeypatch):
    stub = StubAsyncJobs()
    monkeypatch.setattr(api, "async_jobs", stub)

    class _StubAgent:
        def discover_datasets(self):
            return [
                {"name": "orders", "data_file": None, "lifecycle": "active"},
                {"name": "customers", "lifecycle": "active"},
            ]

    monkeypatch.setattr(api, "agent", _StubAgent())

    client = TestClient(api.app)
    response = client.post("/jobs/evaluate-all", json={"include_unconfigured": True})

    assert response.status_code == 400
    assert "No datasets discovered" in response.json()["detail"]


def test_enqueue_evaluate_all_returns_400_when_only_unconfigured(monkeypatch):
    stub = StubAsyncJobs()
    monkeypatch.setattr(api, "async_jobs", stub)

    class _StubAgent:
        def discover_datasets(self):
            return [
                {"name": "customers", "data_file": "data/customers.csv", "lifecycle": "unconfigured"},
            ]

    monkeypatch.setattr(api, "agent", _StubAgent())

    client = TestClient(api.app)
    response = client.post("/jobs/evaluate-all", json={"include_unconfigured": False})

    assert response.status_code == 400
    assert "No datasets discovered" in response.json()["detail"]


def test_list_jobs_and_get_job(monkeypatch):
    stub = StubAsyncJobs()
    monkeypatch.setattr(api, "async_jobs", stub)

    client = TestClient(api.app)

    list_response = client.get("/jobs?limit=25&status=RUNNING&action=evaluate&dataset_name=orders")
    assert list_response.status_code == 200
    assert isinstance(list_response.json(), list)

    get_response = client.get("/jobs/job-eval-1")
    assert get_response.status_code == 200
    assert get_response.json()["status"] == "COMPLETED"

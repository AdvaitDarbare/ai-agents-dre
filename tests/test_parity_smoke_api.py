from contextlib import contextmanager
from pathlib import Path

from fastapi.testclient import TestClient

import src.api as api_module


class _DummyCursor:
    def execute(self, *_args, **_kwargs):
        return None

    def fetchall(self):
        return []

    def fetchone(self):
        return [0]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyConn:
    def cursor(self):
        return _DummyCursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


@contextmanager
def _fake_connection():
    yield _DummyConn()


def test_parity_smoke_contract_approve_and_jobs(monkeypatch):
    client = TestClient(api_module.app)

    monkeypatch.setattr(api_module.service, "approve_contract", lambda dataset_name, approved_yaml: {"status": "approved", "dataset_name": dataset_name})
    monkeypatch.setattr(api_module.async_jobs, "submit_evaluate", lambda dataset_name: {"job_id": "job-eval", "dataset_name": dataset_name, "status": "QUEUED"})
    monkeypatch.setattr(api_module.async_jobs, "submit_delete", lambda dataset_name: {"job_id": "job-del", "dataset_name": dataset_name, "status": "QUEUED"})
    monkeypatch.setattr(api_module.async_jobs, "submit_bulk_evaluate", lambda dataset_names: {"job_id": "job-bulk", "dataset_name": ",".join(dataset_names), "status": "QUEUED"})
    monkeypatch.setattr(api_module.agent, "discover_datasets", lambda: [{"name": "orders", "lifecycle": "active", "data_file": "data/orders.csv"}])
    monkeypatch.setattr(api_module.policy_service, "evaluate_action", lambda **_kwargs: {"allowed": True})
    monkeypatch.setattr(api_module.policy_service, "enforce", lambda *_args, **_kwargs: None)

    res = client.post("/contracts/approve", json={"dataset_name": "orders", "approved_yaml": "kind: DataContract\n"})
    assert res.status_code == 200
    assert res.json()["status"] == "approved"

    res = client.post("/jobs/evaluate/orders")
    assert res.status_code == 200
    assert res.json()["job_id"] == "job-eval"

    res = client.post("/jobs/delete/orders", json={"confirm": True})
    assert res.status_code == 200
    assert res.json()["job_id"] == "job-del"

    res = client.post("/jobs/evaluate-all", json={"include_unconfigured": True})
    assert res.status_code == 200
    assert res.json()["job_id"] == "job-bulk"


def test_parity_smoke_incident_and_governance(monkeypatch, tmp_path):
    client = TestClient(api_module.app)

    monkeypatch.setattr(
        api_module.incident_service,
        "update_incident",
        lambda incident_id, status, owner=None, note=None: {"incident_id": incident_id, "status": status, "dataset_name": "orders"},
    )
    monkeypatch.setattr(api_module, "get_connection", _fake_connection)
    monkeypatch.setattr(api_module.service, "evaluate_dataset", lambda dataset_name: {"status": "PASSED", "dataset": dataset_name})

    history_dir = tmp_path / "config/history"
    contracts_dir = tmp_path / "config/expectations"
    history_dir.mkdir(parents=True)
    contracts_dir.mkdir(parents=True)

    source = history_dir / "orders_v1.yaml"
    target = contracts_dir / "orders.yaml"
    source.write_text("kind: DataContract\nid: orders\n")
    target.write_text("kind: DataContract\nid: orders\n")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(api_module.agent, "discover_datasets", lambda: [{"name": "orders", "contract_path": str(target)}])

    res = client.patch("/incidents/inc-1", json={"status": "ACK"})
    assert res.status_code == 200
    assert res.json()["status"] == "ACK"

    res = client.post("/governance/rollback", json={"dataset_name": "orders", "filename": "orders_v1.yaml"})
    assert res.status_code == 200
    assert res.json()["status"] == "success"

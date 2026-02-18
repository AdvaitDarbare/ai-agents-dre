from fastapi.testclient import TestClient

import src.api as api


def test_contract_gate_endpoint_delegates_to_service(monkeypatch):
    class _Service:
        def run_contract_gate(self, *, dataset_name: str, file_path=None):
            return {
                "dataset_name": dataset_name,
                "file_path": file_path,
                "status": "PASSED",
                "gate": "contract_ci",
            }

    monkeypatch.setattr(api, "service", _Service())
    monkeypatch.setattr(api, "_enforce_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(api, "_audit", lambda *_args, **_kwargs: None)

    client = TestClient(api.app)
    response = client.post("/contracts/gate", json={"dataset_name": "orders", "file_path": "data/orders.csv"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["dataset_name"] == "orders"
    assert payload["status"] == "PASSED"


def test_contract_autopilot_endpoint_delegates_to_service(monkeypatch):
    class _Service:
        def generate_autopilot_contract(self, *, dataset_name: str, file_path=None, confidence_threshold: float = 0.75):
            return {
                "dataset_name": dataset_name,
                "file_path": file_path,
                "confidence_threshold": confidence_threshold,
                "recommendation_count": 2,
            }

    monkeypatch.setattr(api, "service", _Service())
    monkeypatch.setattr(api, "_enforce_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(api, "_audit", lambda *_args, **_kwargs: None)

    client = TestClient(api.app)
    response = client.post(
        "/contracts/autopilot",
        json={"dataset_name": "orders", "file_path": "data/orders.csv", "confidence_threshold": 0.8},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["dataset_name"] == "orders"
    assert payload["confidence_threshold"] == 0.8


def test_diagnostics_and_outcome_endpoints(monkeypatch):
    class _Service:
        def get_diagnostics_records(self, *, dataset_name: str, run_id=None, check_type=None, limit: int = 200):
            return {"dataset_name": dataset_name, "run_id": run_id, "check_type": check_type, "limit": limit, "records": []}

        def get_outcome_metrics(self, *, days: int = 30):
            return {"window_days": days, "runs": {"total": 5}}

    monkeypatch.setattr(api, "service", _Service())

    client = TestClient(api.app)

    diag_response = client.get("/diagnostics/orders?run_id=abc&check_type=ANOMALY&limit=50")
    assert diag_response.status_code == 200
    diag_payload = diag_response.json()
    assert diag_payload["dataset_name"] == "orders"
    assert diag_payload["limit"] == 50

    outcome_response = client.get("/metrics/outcomes?days=14")
    assert outcome_response.status_code == 200
    outcome_payload = outcome_response.json()
    assert outcome_payload["window_days"] == 14


def test_agentic_remediation_endpoints_delegate(monkeypatch):
    class _Service:
        def run_agentic_contract_remediation(self, *, dataset_name: str, max_retries: int = 2, autonomy_mode: str = "full_auto"):
            return {
                "id": "rem-1",
                "status": "AUTO_FIXED",
                "dataset_name": dataset_name,
                "attempts": 1,
                "initial_run_id": "run-1",
                "final_run_id": "run-2",
                "applied_changes": [],
            }

        def get_agentic_remediation_run(self, remediation_run_id: str):
            return {
                "id": remediation_run_id,
                "dataset_name": "orders",
                "status": "AUTO_FIXED",
                "attempt_count": 1,
                "attempts": [],
                "timeline": [],
            }

    monkeypatch.setattr(api, "service", _Service())
    monkeypatch.setattr(api, "_enforce_role", lambda *_args, **_kwargs: None)

    client = TestClient(api.app)
    run_response = client.post(
        "/workflow/agentic/remediate",
        json={"dataset_name": "orders", "max_retries": 2, "autonomy_mode": "full_auto"},
    )
    assert run_response.status_code == 200
    assert run_response.json()["status"] == "AUTO_FIXED"

    get_response = client.get("/workflow/agentic/remediate/rem-1")
    assert get_response.status_code == 200
    payload = get_response.json()
    assert payload["id"] == "rem-1"
    assert payload["status"] == "AUTO_FIXED"


def test_agentic_remediation_stream_emits_payload(monkeypatch):
    class _Service:
        def get_agentic_remediation_run(self, remediation_run_id: str):
            return {
                "id": remediation_run_id,
                "dataset_name": "orders",
                "status": "AUTO_FIXED",
                "attempt_count": 1,
                "attempts": [],
                "timeline": [],
            }

    monkeypatch.setattr(api, "service", _Service())
    client = TestClient(api.app)

    with client.stream("GET", "/workflow/agentic/remediate/rem-2/stream?interval_ms=1000") as response:
        assert response.status_code == 200
        body = "".join(list(response.iter_text()))
        assert "event: remediation" in body
        assert '"id": "rem-2"' in body

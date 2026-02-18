import pytest

from fastapi import HTTPException

from src.services.reliability_service import ReliabilityService


class _StubContracts:
    def exists(self, dataset_name: str) -> bool:
        return True

    def path_for(self, dataset_name: str):
        _ = dataset_name
        raise ValueError("unused")


class _StubAgent:
    def __init__(self, verdict=None, should_raise=False):
        self._verdict = verdict or {"status": "PASSED", "run_id": "run-1"}
        self._should_raise = should_raise

    def discover_datasets(self):
        return [{"name": "orders", "data_file": "data/orders.csv"}]

    def evaluate_data_file(self, file_path: str, dataset_name: str):
        _ = (file_path, dataset_name)
        if self._should_raise:
            raise RuntimeError("boom")
        return self._verdict


class _StubAudit:
    def __init__(self):
        self.calls = []

    def record(self, **kwargs):
        self.calls.append(kwargs)
        return "audit-1"


def test_reliability_service_emits_audit_on_success():
    audit = _StubAudit()
    service = ReliabilityService(agent=_StubAgent(), contract_store=_StubContracts(), audit_service=audit)

    verdict = service.evaluate_dataset("orders")
    assert verdict["run_id"] == "run-1"
    assert [c["action"] for c in audit.calls][:1] == ["evaluate_started"]
    assert any(c["action"] == "evaluate_completed" and c["metadata"].get("run_id") == "run-1" for c in audit.calls)


def test_reliability_service_emits_audit_on_failure():
    audit = _StubAudit()
    service = ReliabilityService(agent=_StubAgent(should_raise=True), contract_store=_StubContracts(), audit_service=audit)

    with pytest.raises(RuntimeError):
        service.evaluate_dataset("orders")

    assert any(c["action"] == "evaluate_failed" for c in audit.calls)


def test_reliability_service_audits_missing_dataset_file():
    audit = _StubAudit()

    class _EmptyAgent(_StubAgent):
        def discover_datasets(self):
            return []

    service = ReliabilityService(agent=_EmptyAgent(), contract_store=_StubContracts(), audit_service=audit)

    with pytest.raises(HTTPException) as exc:
        service.evaluate_dataset("orders")

    assert exc.value.status_code == 404
    assert any(c["action"] == "evaluate_failed" for c in audit.calls)


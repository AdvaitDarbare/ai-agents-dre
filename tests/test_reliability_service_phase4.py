import pytest
from fastapi import HTTPException

from src.services.reliability_service import ReliabilityService


class StubAgent:
    def __init__(self, datasets=None, verdict=None):
        self._datasets = datasets or []
        self._verdict = verdict or {"status": "PASSED", "reason": "ok"}
        self.evaluate_calls = []
        self.evaluate_discovered_calls = []

    def discover_datasets(self):
        return self._datasets

    def evaluate_data_file(self, file_path, dataset_name):
        self.evaluate_calls.append((file_path, dataset_name))
        return self._verdict

    def evaluate_discovered_dataset(self, dataset_meta):
        self.evaluate_discovered_calls.append(dataset_meta)
        return self._verdict


class StubContractStore:
    def __init__(self, exists_result=True):
        self.exists_result = exists_result

    def exists(self, dataset_name):
        return self.exists_result


class StubWorkflow:
    def __init__(self, result=None, to_raise=None):
        self.result = result or {}
        self.to_raise = to_raise
        self.calls = []

    def run_for_file(self, dataset_name, file_path, source="api", apply_file_actions=False):
        self.calls.append((dataset_name, file_path, source, apply_file_actions))
        if self.to_raise is not None:
            raise self.to_raise
        return self.result


def test_evaluate_dataset_returns_verdict_when_workflow_evaluated_mode():
    agent = StubAgent(
        datasets=[{"name": "orders", "data_file": "data/orders.csv"}],
        verdict={"status": "PASSED", "reason": "direct"},
    )
    workflow = StubWorkflow(result={"mode": "evaluated", "verdict": {"status": "WARNING", "reason": "graph"}})
    service = ReliabilityService(agent=agent, contract_store=StubContractStore(), hitl_workflow=workflow)

    result = service.evaluate_dataset("orders")

    assert result["status"] == "WARNING"
    assert workflow.calls == [("orders", "data/orders.csv", "api", False)]
    assert agent.evaluate_calls == []


def test_evaluate_dataset_returns_hitl_payload_when_contract_missing():
    agent = StubAgent(datasets=[{"name": "newdata", "data_file": "data/newdata.csv"}])
    payload = {
        "mode": "hitl",
        "status": "paused_hitl",
        "message": "Awaiting human approval.",
        "state": {"pending_file_path": "data/pending_approval/newdata.csv"},
    }
    workflow = StubWorkflow(result=payload)
    service = ReliabilityService(agent=agent, contract_store=StubContractStore(exists_result=False), hitl_workflow=workflow)

    result = service.evaluate_dataset("newdata")

    assert result["status"] == "paused_hitl"
    assert result["state"]["pending_file_path"].endswith("newdata.csv")


def test_evaluate_dataset_falls_back_to_direct_eval_if_workflow_fails_and_contract_exists():
    agent = StubAgent(
        datasets=[{"name": "orders", "data_file": "data/orders.csv"}],
        verdict={"status": "PASSED", "reason": "fallback"},
    )
    workflow = StubWorkflow(to_raise=RuntimeError("checkpoint unavailable"))
    service = ReliabilityService(
        agent=agent,
        contract_store=StubContractStore(exists_result=True),
        hitl_workflow=workflow,
    )

    result = service.evaluate_dataset("orders")

    assert result["status"] == "PASSED"
    assert agent.evaluate_calls == [("data/orders.csv", "orders")]


def test_evaluate_dataset_raises_if_workflow_fails_and_no_contract_exists():
    agent = StubAgent(datasets=[{"name": "newdata", "data_file": "data/newdata.csv"}])
    workflow = StubWorkflow(to_raise=RuntimeError("checkpoint unavailable"))
    service = ReliabilityService(
        agent=agent,
        contract_store=StubContractStore(exists_result=False),
        hitl_workflow=workflow,
    )

    with pytest.raises(RuntimeError):
        service.evaluate_dataset("newdata")


def test_evaluate_dataset_404_when_data_file_missing():
    agent = StubAgent(datasets=[{"name": "orders", "data_file": None}])
    service = ReliabilityService(agent=agent, contract_store=StubContractStore(), hitl_workflow=None)

    with pytest.raises(HTTPException) as exc:
        service.evaluate_dataset("orders")

    assert exc.value.status_code == 404


def test_evaluate_dataset_uses_connector_path_when_no_data_file():
    agent = StubAgent(
        datasets=[
            {
                "name": "orders",
                "data_file": None,
                "connector_name": "postgres",
                "source_location": "public.orders",
                "source_format": "postgres_table",
                "source_metadata": {"schema": "public", "table": "orders"},
            }
        ],
        verdict={"status": "WARNING", "reason": "connector"},
    )
    service = ReliabilityService(agent=agent, contract_store=StubContractStore(), hitl_workflow=None)

    result = service.evaluate_dataset("orders")

    assert result["status"] == "WARNING"
    assert len(agent.evaluate_discovered_calls) == 1
    assert agent.evaluate_calls == []


def test_evaluate_dataset_connector_requires_contract():
    agent = StubAgent(
        datasets=[
            {
                "name": "orders",
                "data_file": None,
                "connector_name": "postgres",
            }
        ]
    )
    service = ReliabilityService(agent=agent, contract_store=StubContractStore(exists_result=False), hitl_workflow=None)

    with pytest.raises(HTTPException) as exc:
        service.evaluate_dataset("orders")

    assert exc.value.status_code == 409

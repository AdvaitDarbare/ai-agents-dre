import json
from pathlib import Path

import pytest

from src.workflows.hitl_contract_workflow import HITLContractWorkflow


class _Agent:
    def __init__(self, verdict):
        self._verdict = verdict
        self.calls = []

    def evaluate_data_file(self, file_path: str, dataset_name: str):
        self.calls.append((file_path, dataset_name))
        return self._verdict

    def propose_contract(self, dataset_name: str, data_path: str, include_metadata: bool = False):
        raise AssertionError("propose_contract should not be called in evaluate flow tests")


class _ContractStore:
    def exists(self, dataset_name: str) -> bool:  # pragma: no cover
        return True

    def write(self, dataset_name: str, content: str):  # pragma: no cover
        raise AssertionError("write should not be called in evaluate flow tests")


def test_evaluate_flow_nodes_persist_verdict(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    data_file = Path("data.csv")
    data_file.write_text("a,b\n1,2\n")

    agent = _Agent(
        verdict={
            "status": "PASSED",
            "reason": "ok",
            "profile": {"weighted_quality_score": 91.0},
            "anomalies": [],
            "tool_outputs": [{"tool": "schema_validator", "status": "ok"}],
        }
    )
    wf = HITLContractWorkflow(agent=agent, contract_store=_ContractStore())

    state = {"dataset_name": "orders", "file_path": str(data_file), "apply_file_actions": False}
    state = {**state, **wf._node_evaluate_pipeline(state)}
    assert state["status"] == "PASSED"
    assert isinstance(state.get("verdict"), dict)
    assert agent.calls == [(str(data_file), "orders")]

    state = {**state, **wf._node_persist_verdict(state)}
    verdict_path = Path(state["verdict_path"])
    assert verdict_path.exists()
    persisted = json.loads(verdict_path.read_text())
    assert persisted["status"] == "PASSED"

    state = {**state, **wf._node_apply_file_actions(state)}
    assert Path(state["evaluated_file_path"]).resolve() == data_file.resolve()


def test_evaluate_flow_nodes_quarantine_when_blocked_and_apply_actions(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    data_file = Path("bad.csv")
    data_file.write_text("a\nx\n")

    agent = _Agent(verdict={"status": "BLOCKED", "reason": "schema", "profile": {"overall_quality_score": 10.0}})
    wf = HITLContractWorkflow(agent=agent, contract_store=_ContractStore())

    state = {"dataset_name": "bad", "file_path": str(data_file), "apply_file_actions": True}
    state = {**state, **wf._node_evaluate_pipeline(state)}
    state = {**state, **wf._node_persist_verdict(state)}
    state = {**state, **wf._node_apply_file_actions(state)}

    moved_path = Path(state["evaluated_file_path"])
    assert moved_path.exists()
    assert moved_path.parent.as_posix().endswith("data/quarantine")
    assert not data_file.exists()


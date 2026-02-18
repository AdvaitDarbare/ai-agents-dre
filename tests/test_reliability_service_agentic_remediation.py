from __future__ import annotations

from typing import Any, Dict, List

from src.services.reliability_service import ReliabilityService


class _StubAgent:
    def __init__(self, criticality: str = "LOW"):
        self.criticality = criticality

    def discover_datasets(self):
        return [{"name": "orders", "criticality": self.criticality}]


class _ContractDoc:
    def __init__(self, content: str):
        self.content = content


class _StubContractStore:
    def __init__(self):
        self._yaml = "kind: DataContract\napiVersion: v3.1.0\ntable_name: orders\ncolumns: []\n"

    def read(self, _dataset_name):
        return _ContractDoc(self._yaml)

    def exists(self, _dataset_name):
        return True


def _build_service() -> ReliabilityService:
    return ReliabilityService(agent=_StubAgent(), contract_store=_StubContractStore(), hitl_workflow=None)


def test_agentic_contract_remediation_success(monkeypatch):
    service = _build_service()
    persisted_attempts: List[Dict[str, Any]] = []

    initial_run = {
        "run_id": "run-1",
        "dataset_name": "orders",
        "status": "BLOCKED",
        "reason": "schema mismatch",
        "quality_score": 62.0,
        "anomaly_count": 0,
        "z_score_max": 0.0,
        "full_verdict": {},
    }
    passed_run = {
        "run_id": "run-2",
        "dataset_name": "orders",
        "status": "PASSED",
        "reason": "healthy",
        "quality_score": 99.0,
        "anomaly_count": 0,
        "z_score_max": 0.0,
        "full_verdict": {},
    }

    monkeypatch.setattr(
        service,
        "_latest_run_record",
        lambda dataset_name, statuses=None: initial_run if statuses else passed_run,
    )
    monkeypatch.setattr(service, "_tool_outputs_for_run", lambda _run_id: [])
    monkeypatch.setattr(service, "_recent_run_records", lambda *_args, **_kwargs: [initial_run])
    monkeypatch.setattr(
        service,
        "_classify_failure",
        lambda **_kwargs: {"classification": "schema_mismatch", "reason": "schema mismatch"},
    )
    monkeypatch.setattr(
        service,
        "_build_contract_patch_with_ai",
        lambda **_kwargs: {
            "modified_yaml": "kind: DataContract\napiVersion: v3.1.0\ntable_name: orders\ncolumns: []\n",
            "change_summary": "Align schema type",
            "risk_level": "low",
            "confidence": 0.91,
            "expected_effect": "Pass schema gate",
        },
    )
    monkeypatch.setattr(service, "apply_remediation", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr(service, "evaluate_dataset", lambda _dataset_name: {"status": "PASSED", "run_id": "run-2"})
    monkeypatch.setattr(service, "_persist_agentic_remediation_run", lambda **_kwargs: None)
    monkeypatch.setattr(
        service,
        "_persist_agentic_remediation_attempt",
        lambda **kwargs: persisted_attempts.append(kwargs),
    )
    monkeypatch.setattr(
        service,
        "get_agentic_remediation_run",
        lambda remediation_run_id: {"id": remediation_run_id, "status": "AUTO_FIXED", "attempt_count": 1},
    )

    result = service.run_agentic_contract_remediation(dataset_name="orders", max_retries=2, autonomy_mode="full_auto")

    assert result["status"] == "AUTO_FIXED"
    assert result["attempts"] == 1
    assert result["final_run_id"] == "run-2"
    assert len(result["applied_changes"]) == 1
    assert len(persisted_attempts) == 1


def test_agentic_contract_remediation_exhausted_to_plan(monkeypatch):
    service = _build_service()
    attempt_results: List[str] = []

    blocked_run = {
        "run_id": "run-1",
        "dataset_name": "orders",
        "status": "BLOCKED",
        "reason": "schema mismatch",
        "quality_score": 60.0,
        "anomaly_count": 0,
        "z_score_max": 0.0,
        "full_verdict": {},
    }

    monkeypatch.setattr(service, "_latest_run_record", lambda *_args, **_kwargs: blocked_run)
    monkeypatch.setattr(service, "_tool_outputs_for_run", lambda _run_id: [])
    monkeypatch.setattr(service, "_recent_run_records", lambda *_args, **_kwargs: [blocked_run])
    monkeypatch.setattr(
        service,
        "_classify_failure",
        lambda **_kwargs: {"classification": "schema_mismatch", "reason": "schema mismatch"},
    )
    monkeypatch.setattr(
        service,
        "_build_contract_patch_with_ai",
        lambda **_kwargs: (_ for _ in ()).throw(ValueError("malformed AI output")),
    )
    monkeypatch.setattr(service, "_persist_agentic_remediation_run", lambda **_kwargs: None)
    monkeypatch.setattr(
        service,
        "_persist_agentic_remediation_attempt",
        lambda **kwargs: attempt_results.append(str(kwargs.get("result_status"))),
    )
    monkeypatch.setattr(
        service,
        "get_agentic_remediation_run",
        lambda remediation_run_id: {"id": remediation_run_id, "status": "PLAN_REQUIRED", "attempt_count": 2},
    )

    result = service.run_agentic_contract_remediation(dataset_name="orders", max_retries=2, autonomy_mode="full_auto")

    assert result["status"] == "PLAN_REQUIRED"
    assert result["attempts"] == 2
    assert result["plan"] is not None
    assert attempt_results == ["FAILED", "FAILED"]


def test_agentic_contract_remediation_policy_deny(monkeypatch):
    service = _build_service()
    blocked_run = {
        "run_id": "run-1",
        "dataset_name": "orders",
        "status": "BLOCKED",
        "reason": "schema mismatch",
        "quality_score": 60.0,
        "anomaly_count": 0,
        "z_score_max": 0.0,
        "full_verdict": {},
    }

    monkeypatch.setattr(service, "_latest_run_record", lambda *_args, **_kwargs: blocked_run)
    monkeypatch.setattr(service, "_tool_outputs_for_run", lambda _run_id: [])
    monkeypatch.setattr(service, "_recent_run_records", lambda *_args, **_kwargs: [blocked_run])
    monkeypatch.setattr(
        service,
        "_classify_failure",
        lambda **_kwargs: {"classification": "schema_mismatch", "reason": "schema mismatch"},
    )
    monkeypatch.setattr(
        service,
        "_build_contract_patch_with_ai",
        lambda **_kwargs: {
            "modified_yaml": "kind: DataContract\napiVersion: v3.1.0\ntable_name: orders\ncolumns: []\n",
            "change_summary": "schema update",
            "risk_level": "low",
            "confidence": 0.9,
            "expected_effect": "pass",
        },
    )
    monkeypatch.setattr(service, "_persist_agentic_remediation_run", lambda **_kwargs: None)
    monkeypatch.setattr(service, "_persist_agentic_remediation_attempt", lambda **_kwargs: None)
    monkeypatch.setattr(
        service,
        "get_agentic_remediation_run",
        lambda remediation_run_id: {"id": remediation_run_id, "status": "BLOCKED_BY_POLICY", "attempt_count": 1},
    )
    monkeypatch.setattr(
        "src.services.policy_service.PolicyService.evaluate_action",
        lambda *_args, **_kwargs: {"decision": "deny", "reason": "hard deny"},
    )

    result = service.run_agentic_contract_remediation(dataset_name="orders", max_retries=2, autonomy_mode="full_auto")

    assert result["status"] == "BLOCKED_BY_POLICY"
    assert result["attempts"] == 1


def test_agentic_contract_remediation_non_fixable_classification(monkeypatch):
    service = _build_service()
    blocked_run = {
        "run_id": "run-1",
        "dataset_name": "orders",
        "status": "BLOCKED",
        "reason": "platform timeout",
        "quality_score": 20.0,
        "anomaly_count": 0,
        "z_score_max": 0.0,
        "full_verdict": {},
    }

    monkeypatch.setattr(service, "_latest_run_record", lambda *_args, **_kwargs: blocked_run)
    monkeypatch.setattr(service, "_tool_outputs_for_run", lambda _run_id: [])
    monkeypatch.setattr(service, "_recent_run_records", lambda *_args, **_kwargs: [blocked_run])
    monkeypatch.setattr(
        service,
        "_classify_failure",
        lambda **_kwargs: {"classification": "platform_failure", "reason": "platform timeout"},
    )
    monkeypatch.setattr(service, "_persist_agentic_remediation_run", lambda **_kwargs: None)
    monkeypatch.setattr(service, "_persist_agentic_remediation_attempt", lambda **_kwargs: None)
    monkeypatch.setattr(
        service,
        "get_agentic_remediation_run",
        lambda remediation_run_id: {"id": remediation_run_id, "status": "PLAN_REQUIRED", "attempt_count": 1},
    )

    result = service.run_agentic_contract_remediation(dataset_name="orders", max_retries=2, autonomy_mode="full_auto")

    assert result["status"] == "PLAN_REQUIRED"
    assert result["attempts"] == 1
    assert result["plan"] is not None

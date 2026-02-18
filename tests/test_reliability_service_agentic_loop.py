from src.services.reliability_service import ReliabilityService


class _StubAgent:
    def __init__(self, criticality: str = "LOW"):
        self.criticality = criticality

    def discover_datasets(self):
        return [{"name": "orders", "criticality": self.criticality}]


class _StubContractStore:
    def exists(self, _dataset_name):
        return True


def test_compare_runs_detects_regression(monkeypatch):
    service = ReliabilityService(agent=_StubAgent(), contract_store=_StubContractStore(), hitl_workflow=None)

    def _fake_get_run_verdict(run_id):
        if run_id == "run-1":
            return {
                "run_id": "run-1",
                "dataset_name": "orders",
                "status": "PASSED",
                "quality_score": 95.0,
                "anomaly_count": 0,
                "z_score_max": 0.5,
                "dimension_scores": {"dimensions": [{"name": "Completeness", "score": 98.0}]},
            }
        return {
            "run_id": "run-2",
            "dataset_name": "orders",
            "status": "BLOCKED",
            "quality_score": 70.0,
            "anomaly_count": 4,
            "z_score_max": 4.2,
            "dimension_scores": {"dimensions": [{"name": "Completeness", "score": 80.0}]},
        }

    monkeypatch.setattr(service, "get_run_verdict", _fake_get_run_verdict)

    result = service.compare_runs("run-1", "run-2")

    assert result["regression_detected"] is True
    assert "quality_degraded" in result["regression_flags"]
    assert result["delta"]["quality_score"] == -25.0
    assert result["delta"]["anomaly_count"] == 4


def test_agentic_loop_proposal_only_when_auto_execute_disabled(monkeypatch):
    service = ReliabilityService(agent=_StubAgent("LOW"), contract_store=_StubContractStore(), hitl_workflow=None)
    apply_calls = {"count": 0}

    monkeypatch.setattr(
        service,
        "investigate_anomaly",
        lambda *_args, **_kwargs: {
            "root_cause_hypothesis": {"confidence": 0.92, "summary": "Schema drift likely"}
        },
    )
    monkeypatch.setattr(
        service,
        "get_remediation_plan",
        lambda *_args, **_kwargs: {
            "status": "remediation_available",
            "proposed_yaml": "columns: []",
        },
    )

    def _fake_apply(**_kwargs):
        apply_calls["count"] += 1
        return {"status": "success"}

    monkeypatch.setattr(service, "apply_remediation", _fake_apply)

    result = service.run_agentic_reliability_loop(dataset_name="orders", auto_execute=False)

    assert result["execution"]["decision"] == "proposed_only"
    assert apply_calls["count"] == 0


def test_agentic_loop_requires_hitl_when_confidence_low(monkeypatch):
    service = ReliabilityService(agent=_StubAgent("LOW"), contract_store=_StubContractStore(), hitl_workflow=None)
    apply_calls = {"count": 0}

    monkeypatch.setattr(
        service,
        "investigate_anomaly",
        lambda *_args, **_kwargs: {
            "root_cause_hypothesis": {"confidence": 0.4, "summary": "Uncertain anomaly cause"}
        },
    )
    monkeypatch.setattr(
        service,
        "get_remediation_plan",
        lambda *_args, **_kwargs: {
            "status": "remediation_available",
            "proposed_yaml": "columns: []",
        },
    )
    monkeypatch.setattr(
        service,
        "apply_remediation",
        lambda **_kwargs: apply_calls.update({"count": apply_calls["count"] + 1}) or {"status": "success"},
    )

    result = service.run_agentic_reliability_loop(
        dataset_name="orders",
        auto_execute=True,
        confidence_threshold=0.8,
    )

    assert result["execution"]["decision"] == "requires_hitl"
    assert apply_calls["count"] == 0


def test_agentic_loop_requires_policy_approval_for_high_criticality(monkeypatch):
    service = ReliabilityService(agent=_StubAgent("HIGH"), contract_store=_StubContractStore(), hitl_workflow=None)

    monkeypatch.setattr(
        service,
        "investigate_anomaly",
        lambda *_args, **_kwargs: {
            "root_cause_hypothesis": {"confidence": 0.95, "summary": "Contract mismatch"}
        },
    )
    monkeypatch.setattr(
        service,
        "get_remediation_plan",
        lambda *_args, **_kwargs: {
            "status": "remediation_available",
            "proposed_yaml": "columns: []",
        },
    )
    monkeypatch.setattr(service, "apply_remediation", lambda **_kwargs: {"status": "success"})

    result = service.run_agentic_reliability_loop(dataset_name="orders", auto_execute=True)

    assert result["execution"]["decision"] == "approval_required"


def test_agentic_loop_executes_when_safe(monkeypatch):
    service = ReliabilityService(agent=_StubAgent("LOW"), contract_store=_StubContractStore(), hitl_workflow=None)
    apply_calls = {"count": 0}

    monkeypatch.setattr(
        service,
        "investigate_anomaly",
        lambda *_args, **_kwargs: {
            "root_cause_hypothesis": {"confidence": 0.9, "summary": "Schema violation confirmed"},
            "run": {"reason": "Schema Violation"},
        },
    )
    monkeypatch.setattr(
        service,
        "get_remediation_plan",
        lambda *_args, **_kwargs: {
            "status": "remediation_available",
            "proposed_yaml": "columns: []",
        },
    )

    def _fake_apply(**_kwargs):
        apply_calls["count"] += 1
        return {"status": "success", "message": "applied"}

    monkeypatch.setattr(service, "apply_remediation", _fake_apply)

    result = service.run_agentic_reliability_loop(
        dataset_name="orders",
        auto_execute=True,
        confidence_threshold=0.8,
    )

    assert result["execution"]["decision"] == "executed"
    assert apply_calls["count"] == 1


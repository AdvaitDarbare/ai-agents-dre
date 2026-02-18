from fastapi import HTTPException
import pytest

from src.services.policy_service import PolicyService


class _StubAgent:
    def __init__(self, datasets):
        self._datasets = datasets

    def discover_datasets(self):
        return self._datasets


def test_policy_allows_low_criticality_delete():
    service = PolicyService(
        agent=_StubAgent([{"name": "orders", "criticality": "LOW"}]),
    )

    decision = service.evaluate_action(action="delete", dataset_name="orders")

    assert decision["decision"] == "allow"


def test_policy_requires_approval_for_critical_delete():
    service = PolicyService(
        agent=_StubAgent([{"name": "payments", "criticality": "CRITICAL"}]),
    )

    decision = service.evaluate_action(action="delete", dataset_name="payments")
    assert decision["decision"] == "approval_required"

    with pytest.raises(HTTPException) as exc:
        service.enforce(decision, approved=False, reason=None)

    assert exc.value.status_code == 409


def test_policy_enforce_passes_with_approval_and_reason():
    service = PolicyService(agent=_StubAgent([]))
    decision = {
        "decision": "approval_required",
        "required_controls": ["policy_approved", "policy_reason"],
    }

    service.enforce(decision, approved=True, reason="Reviewed by owner")

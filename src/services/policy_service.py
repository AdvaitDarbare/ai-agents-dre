from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from src.agents.monitor_agent import MonitorAgent


class PolicyService:
    """
    Action safety policy evaluation and enforcement.

    Decisions:
    - allow
    - approval_required
    """

    CRITICALITY_APPROVAL = {"HIGH", "CRITICAL"}
    DESTRUCTIVE_ACTIONS = {"delete", "bulk_delete"}

    def __init__(self, agent: MonitorAgent):
        self.agent = agent

    def _dataset_criticality(self, dataset_name: str) -> str:
        try:
            datasets = self.agent.discover_datasets()
            meta = next((d for d in datasets if d.get("name") == dataset_name), None)
            return str((meta or {}).get("criticality", "UNKNOWN")).upper()
        except Exception:
            return "UNKNOWN"

    def evaluate_action(
        self,
        *,
        action: str,
        dataset_name: Optional[str] = None,
        dataset_names: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        normalized = str(action or "").strip().lower()
        targets = []

        if dataset_name:
            targets.append(dataset_name)
        if dataset_names:
            targets.extend([d for d in dataset_names if str(d or "").strip()])

        unique_targets = sorted({str(t).strip() for t in targets if str(t).strip()})
        target_criticalities = {
            target: self._dataset_criticality(target) for target in unique_targets
        }

        requires_approval = False
        reason = "Action allowed by policy."
        required_controls: List[str] = []

        if normalized in self.DESTRUCTIVE_ACTIONS:
            if any(c in self.CRITICALITY_APPROVAL for c in target_criticalities.values()):
                requires_approval = True
                reason = "Destructive action targets HIGH/CRITICAL dataset(s)."
                required_controls = ["confirm", "policy_approved", "policy_reason"]
        elif normalized == "remediation_apply":
            if any(c in self.CRITICALITY_APPROVAL for c in target_criticalities.values()):
                requires_approval = True
                reason = "Remediation on HIGH/CRITICAL dataset requires explicit approval."
                required_controls = ["policy_approved", "policy_reason"]

        return {
            "action": normalized,
            "decision": "approval_required" if requires_approval else "allow",
            "reason": reason,
            "required_controls": required_controls,
            "targets": unique_targets,
            "criticalities": target_criticalities,
        }

    @staticmethod
    def enforce(decision: Dict[str, Any], *, approved: bool = False, reason: Optional[str] = None) -> None:
        if decision.get("decision") != "approval_required":
            return

        if approved and str(reason or "").strip():
            return

        required_controls = decision.get("required_controls") or []
        missing_controls: List[str] = []
        if "policy_approved" in required_controls and not approved:
            missing_controls.append("policy_approved")
        if "policy_reason" in required_controls and not str(reason or "").strip():
            missing_controls.append("policy_reason")

        raise HTTPException(
            status_code=409,
            detail={
                "message": "Policy approval required for this action.",
                "policy": decision,
                "missing_controls": missing_controls,
            },
        )

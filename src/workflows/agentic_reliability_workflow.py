from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from src.services.policy_service import PolicyService


class AgenticLoopState(TypedDict, total=False):
    dataset_name: str
    metric: Optional[str]
    auto_execute: bool
    confidence_threshold: float
    policy_approved: bool
    policy_reason: Optional[str]
    investigation: Dict[str, Any]
    remediation: Dict[str, Any]
    policy: Dict[str, Any]
    execution: Dict[str, Any]
    stage_events: List[Dict[str, Any]]


class AgenticReliabilityWorkflow:
    """
    Deterministic LangGraph orchestration for investigation/remediation execution.

    Stages:
      investigate -> remediation_plan -> policy_eval -> execution_gate -> [apply]
    """

    def __init__(self, service: Any):
        self.service = service

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except Exception:
            return default

    @staticmethod
    def _append_event(
        state: AgenticLoopState,
        *,
        stage: str,
        status: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        events = list(state.get("stage_events") or [])
        event: Dict[str, Any] = {
            "stage": stage,
            "status": status,
            "message": message,
        }
        if details:
            event["details"] = details
        events.append(event)
        return events

    def _build_graph(self):
        graph = StateGraph(AgenticLoopState)

        graph.add_node("investigate", self._node_investigate)
        graph.add_node("plan_remediation", self._node_plan_remediation)
        graph.add_node("evaluate_policy", self._node_evaluate_policy)
        graph.add_node("decide_execution", self._node_decide_execution)
        graph.add_node("apply_remediation", self._node_apply_remediation)

        graph.add_edge(START, "investigate")
        graph.add_edge("investigate", "plan_remediation")
        graph.add_edge("plan_remediation", "evaluate_policy")
        graph.add_edge("evaluate_policy", "decide_execution")
        graph.add_conditional_edges(
            "decide_execution",
            self._route_after_decision,
            {
                "apply": "apply_remediation",
                "done": END,
            },
        )
        graph.add_edge("apply_remediation", END)

        return graph.compile()

    def _node_investigate(self, state: AgenticLoopState) -> AgenticLoopState:
        dataset_name = state["dataset_name"]
        metric = state.get("metric")

        investigation = self.service.investigate_anomaly(dataset_name, metric=metric)
        confidence = self._safe_float(
            (investigation.get("root_cause_hypothesis") or {}).get("confidence"),
            0.0,
        )

        return {
            "investigation": investigation,
            "stage_events": self._append_event(
                state,
                stage="investigation",
                status="completed",
                message="Investigation packet generated.",
                details={"confidence": round(confidence, 2)},
            ),
        }

    def _node_plan_remediation(self, state: AgenticLoopState) -> AgenticLoopState:
        dataset_name = state["dataset_name"]
        remediation = self.service.get_remediation_plan(dataset_name)

        return {
            "remediation": remediation,
            "stage_events": self._append_event(
                state,
                stage="remediation_plan",
                status="completed",
                message="Remediation plan generated.",
                details={"status": remediation.get("status")},
            ),
        }

    def _node_evaluate_policy(self, state: AgenticLoopState) -> AgenticLoopState:
        dataset_name = state["dataset_name"]
        policy = PolicyService(self.service.agent).evaluate_action(
            action="remediation_apply",
            dataset_name=dataset_name,
        )

        return {
            "policy": policy,
            "stage_events": self._append_event(
                state,
                stage="policy",
                status="completed",
                message="Policy decision evaluated.",
                details={"decision": policy.get("decision")},
            ),
        }

    def _node_decide_execution(self, state: AgenticLoopState) -> AgenticLoopState:
        investigation = state.get("investigation") if isinstance(state.get("investigation"), dict) else {}
        remediation = state.get("remediation") if isinstance(state.get("remediation"), dict) else {}
        policy = state.get("policy") if isinstance(state.get("policy"), dict) else {}

        confidence = self._safe_float(
            (investigation.get("root_cause_hypothesis") or {}).get("confidence"),
            0.0,
        )
        threshold = max(0.0, min(self._safe_float(state.get("confidence_threshold"), 0.8), 1.0))

        execution: Dict[str, Any] = {
            "requested_auto_execute": bool(state.get("auto_execute")),
            "decision": "no_action",
            "confidence": round(confidence, 2),
            "confidence_threshold": threshold,
            "policy": policy,
        }

        if remediation.get("status") != "remediation_available":
            execution["decision"] = "no_remediation_needed"
            execution["reason"] = remediation.get("message", "No remediation available.")
        else:
            proposed_yaml = str(remediation.get("proposed_yaml") or "")
            if not proposed_yaml.strip() or proposed_yaml.lstrip().startswith("# Error"):
                execution["decision"] = "requires_hitl"
                execution["reason"] = "Remediation proposal is unavailable or invalid."
            elif confidence < threshold:
                execution["decision"] = "requires_hitl"
                execution["reason"] = "Confidence below threshold; human approval required."
            elif not bool(state.get("auto_execute")):
                execution["decision"] = "proposed_only"
                execution["reason"] = "Auto execution disabled. Proposal is ready for human review."
            elif policy.get("decision") == "approval_required" and not (
                bool(state.get("policy_approved")) and str(state.get("policy_reason") or "").strip()
            ):
                execution["decision"] = "approval_required"
                execution["reason"] = "Policy approval required before remediation execution."
                execution["missing_controls"] = policy.get("required_controls", [])
            else:
                execution["decision"] = "ready_to_execute"
                execution["reason"] = "All controls satisfied. Applying remediation."

        return {
            "execution": execution,
            "stage_events": self._append_event(
                state,
                stage="execution_gate",
                status="completed",
                message="Execution gate decision computed.",
                details={"decision": execution.get("decision")},
            ),
        }

    def _route_after_decision(self, state: AgenticLoopState) -> str:
        execution = state.get("execution") if isinstance(state.get("execution"), dict) else {}
        if execution.get("decision") == "ready_to_execute":
            return "apply"
        return "done"

    def _node_apply_remediation(self, state: AgenticLoopState) -> AgenticLoopState:
        dataset_name = state["dataset_name"]
        remediation = state.get("remediation") if isinstance(state.get("remediation"), dict) else {}
        investigation = state.get("investigation") if isinstance(state.get("investigation"), dict) else {}
        execution = dict(state.get("execution") or {})
        policy = state.get("policy") if isinstance(state.get("policy"), dict) else {}

        PolicyService.enforce(
            policy,
            approved=bool(state.get("policy_approved")),
            reason=state.get("policy_reason"),
        )

        apply_result = self.service.apply_remediation(
            dataset_name=dataset_name,
            proposed_yaml=str(remediation.get("proposed_yaml") or ""),
            error_context=str(
                (investigation.get("root_cause_hypothesis") or {}).get("summary")
                or (investigation.get("run") or {}).get("reason")
                or "Agentic remediation loop execution"
            ),
        )

        execution["decision"] = "executed"
        execution["reason"] = "Remediation applied automatically."
        execution["result"] = apply_result

        return {
            "execution": execution,
            "stage_events": self._append_event(
                state,
                stage="apply_remediation",
                status="completed",
                message="Remediation applied successfully.",
                details={"status": apply_result.get("status") if isinstance(apply_result, dict) else None},
            ),
        }

    def run(
        self,
        *,
        dataset_name: str,
        metric: Optional[str] = None,
        auto_execute: bool = False,
        confidence_threshold: float = 0.8,
        policy_approved: bool = False,
        policy_reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        app = self._build_graph()
        state = app.invoke(
            {
                "dataset_name": dataset_name,
                "metric": metric,
                "auto_execute": bool(auto_execute),
                "confidence_threshold": confidence_threshold,
                "policy_approved": bool(policy_approved),
                "policy_reason": policy_reason,
                "stage_events": [],
            }
        )

        return {
            "dataset_name": dataset_name,
            "investigation": state.get("investigation", {}),
            "remediation": state.get("remediation", {}),
            "execution": state.get("execution", {}),
            "workflow": {
                "engine": "langgraph",
                "stages": state.get("stage_events", []),
            },
        }

    def mermaid(self) -> str:
        app = self._build_graph()
        return app.get_graph().draw_mermaid()

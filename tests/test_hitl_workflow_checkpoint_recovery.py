from dataclasses import dataclass

from src.workflows.hitl_contract_workflow import HITLContractWorkflow


class _Agent:
    pass


class _ContractStore:
    pass


class _CheckpointerCtx:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def setup(self):
        return None


@dataclass
class _Interrupt:
    value: dict


class _Snapshot:
    def __init__(self, values=None, interrupts=None, next_nodes=None):
        self.values = values or {}
        self.interrupts = tuple(interrupts or ())
        self.next = tuple(next_nodes or ())


class _FakeGraphApp:
    def __init__(self, store):
        self.store = store

    def invoke(self, payload, config):
        thread_id = config["configurable"]["thread_id"]
        entry = self.store.setdefault(
            thread_id,
            {"values": {}, "interrupts": [], "next": []},
        )

        # Resume path (payload is langgraph.types.Command)
        if hasattr(payload, "resume"):
            decision = ((payload.resume or {}).get("decision") or "").strip().lower()
            if decision == "approve":
                entry["values"] = {
                    **entry["values"],
                    "status": "approved",
                    "message": "Contract approved.",
                    "approval_decision": "approve",
                }
            elif decision == "reject":
                entry["values"] = {
                    **entry["values"],
                    "status": "rejected",
                    "message": "Contract rejected.",
                    "approval_decision": "reject",
                }
            else:
                entry["values"] = {
                    **entry["values"],
                    "status": "failed",
                    "message": "Invalid decision.",
                    "approval_decision": decision,
                }
            entry["interrupts"] = []
            entry["next"] = []
            self.store[thread_id] = entry
            return

        dataset_name = payload["dataset_name"]
        entry["values"] = {
            "dataset_name": dataset_name,
            "pending_file_path": f"data/pending_approval/{dataset_name}.csv",
            "proposal_yaml": f"kind: DataContract\nid: urn:datacontract:{dataset_name}\ncolumns: []\n",
            "status": "paused_hitl",
            "message": "Awaiting human approval.",
        }
        entry["interrupts"] = [
            _Interrupt(
                {
                    "kind": "contract_approval_required",
                    "dataset_name": dataset_name,
                }
            )
        ]
        entry["next"] = ["wait_for_approval"]
        self.store[thread_id] = entry

    def get_state(self, config):
        thread_id = config["configurable"]["thread_id"]
        entry = self.store.get(thread_id, {"values": {}, "interrupts": [], "next": []})
        return _Snapshot(values=entry["values"], interrupts=entry["interrupts"], next_nodes=entry["next"])


def test_hitl_checkpoint_restart_resume(monkeypatch):
    shared_state = {}

    def _fake_open_checkpointer(self):
        return _CheckpointerCtx()

    def _fake_build_graph(self, _checkpointer, include_existing_evaluation=False):
        _ = include_existing_evaluation
        return _FakeGraphApp(shared_state)

    monkeypatch.setattr(HITLContractWorkflow, "_open_checkpointer", _fake_open_checkpointer)
    monkeypatch.setattr(HITLContractWorkflow, "_build_graph", _fake_build_graph)

    workflow_a = HITLContractWorkflow(agent=_Agent(), contract_store=_ContractStore())
    started = workflow_a.start_missing_contract(
        dataset_name="newdata",
        file_path="data/landing/newdata.csv",
    )
    assert started["handled"] is True
    assert started["status"] == "paused_hitl"
    assert started["interrupts"][0]["kind"] == "contract_approval_required"

    # Simulate process restart by creating a new workflow instance.
    workflow_b = HITLContractWorkflow(agent=_Agent(), contract_store=_ContractStore())
    assert workflow_b.is_waiting_for_approval("newdata") is True

    resumed = workflow_b.resume(
        dataset_name="newdata",
        decision="approve",
        approved_yaml="kind: DataContract\nid: urn:datacontract:newdata\ncolumns: []\n",
    )
    assert resumed["handled"] is True
    assert resumed["status"] == "approved"
    assert resumed["result"]["approval_decision"] == "approve"
    assert workflow_b.is_waiting_for_approval("newdata") is False

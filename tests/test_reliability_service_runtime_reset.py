from pathlib import Path

import src.services.reliability_service as reliability_module
from src.services.reliability_service import ReliabilityService


class _StubAgent:
    def discover_datasets(self):
        return []


class _StubContractStore:
    def __init__(self, root_path: Path):
        self.root_path = root_path


class _FakeCursor:
    def __init__(self):
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=()):
        self.executed.append((sql, params))


class _FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self._cursor


def test_reset_runtime_state_clears_runtime_artifacts(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    # Create minimal runtime dirs/files.
    (tmp_path / "data/history").mkdir(parents=True)
    (tmp_path / "data/landing").mkdir(parents=True)
    (tmp_path / "data/pending_approval").mkdir(parents=True)
    (tmp_path / "data/quarantine").mkdir(parents=True)
    (tmp_path / "data/staged_connector").mkdir(parents=True)
    (tmp_path / "data/test").mkdir(parents=True)
    (tmp_path / "config/proposals").mkdir(parents=True)
    (tmp_path / "config/expectations").mkdir(parents=True)
    (tmp_path / "config/history").mkdir(parents=True)
    (tmp_path / "logs/runs").mkdir(parents=True)

    (tmp_path / "data/history/.keep").write_text("")
    (tmp_path / "logs/runs/.gitkeep").write_text("")
    (tmp_path / "data/history/sample.json").write_text("{}")
    (tmp_path / "data/landing/orders.csv").write_text("a,b\n1,2\n")
    (tmp_path / "config/proposals/orders.yaml").write_text("kind: DataContract\n")
    (tmp_path / "config/expectations/orders.yaml").write_text("kind: DataContract\n")
    (tmp_path / "config/expectations/transactions.yaml").write_text("kind: DataContract\n")
    (tmp_path / "config/history/orders_v20260218.yaml").write_text("kind: DataContract\n")

    fake_cursor = _FakeCursor()
    monkeypatch.setattr(reliability_module, "get_connection", lambda: _FakeConnection(fake_cursor))

    service = ReliabilityService(
        agent=_StubAgent(),
        contract_store=_StubContractStore(tmp_path / "config/expectations"),
        hitl_workflow=None,
    )

    result = service.reset_runtime_state(
        clear_generated_contracts=True,
        preserve_contract_names=["transactions"],
        clear_langgraph_checkpoints=True,
    )

    # Runtime files should be removed except keep markers.
    assert not (tmp_path / "data/landing/orders.csv").exists()
    assert not (tmp_path / "config/proposals/orders.yaml").exists()
    assert not (tmp_path / "config/expectations/orders.yaml").exists()
    assert (tmp_path / "config/expectations/transactions.yaml").exists()
    assert not (tmp_path / "config/history/orders_v20260218.yaml").exists()
    assert (tmp_path / "data/history/.keep").exists()
    assert (tmp_path / "logs/runs/.gitkeep").exists()

    # Verify DB truncate was issued.
    assert fake_cursor.executed, "Expected TRUNCATE statement"
    assert "TRUNCATE TABLE" in fake_cursor.executed[0][0]
    assert "run_history" in fake_cursor.executed[0][0]
    assert "diagnostics_records" in fake_cursor.executed[0][0]
    assert "checkpoints" in fake_cursor.executed[0][0]

    assert result["status"] == "reset_completed"
    assert result["contracts"]["removed_contract_count"] == 1

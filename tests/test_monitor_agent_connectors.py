from pathlib import Path

from src.connectors.base import ConnectorDataset
from src.agents.monitor_agent import MonitorAgent


class _StubConnector:
    name = "postgres"

    def __init__(self):
        self.read_calls = []

    def discover(self):
        return [
            ConnectorDataset(
                name="orders",
                location="public.orders",
                format="postgres_table",
                metadata={"schema": "public", "table": "orders"},
            )
        ]

    def read_sample(self, dataset, limit=100):
        self.read_calls.append((dataset, limit))
        return [{"id": 1, "amount": 10.0}, {"id": 2, "amount": 20.0}]


def test_discover_datasets_includes_unmanaged_connector_dataset(tmp_path, monkeypatch):
    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir(parents=True, exist_ok=True)
    stub_connector = _StubConnector()

    monkeypatch.setenv("CONTRACTS_PATH", str(contracts_dir))
    monkeypatch.setenv("DRE_DISCOVER_UNMANAGED", "1")
    monkeypatch.setattr("src.agents.monitor_agent.build_connectors", lambda: [stub_connector])

    agent = MonitorAgent(contracts_path=str(contracts_dir), lineage_path="config/lineage.yaml")

    discovered = agent.discover_datasets()
    orders = next((item for item in discovered if item.get("name") == "orders"), None)

    assert orders is not None
    assert orders.get("connector_name") == "postgres"
    assert orders.get("data_file") is None
    assert orders.get("source_location") == "public.orders"


def test_evaluate_discovered_dataset_uses_connector_sampling(tmp_path, monkeypatch):
    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir(parents=True, exist_ok=True)
    stub_connector = _StubConnector()

    monkeypatch.setattr("src.agents.monitor_agent.build_connectors", lambda: [stub_connector])

    agent = MonitorAgent(contracts_path=str(contracts_dir), lineage_path="config/lineage.yaml")

    captured = {}

    def _fake_eval(file_path: str, dataset_name: str):
        captured["file_path"] = file_path
        captured["dataset_name"] = dataset_name
        return {"status": "PASSED", "dataset": dataset_name}

    monkeypatch.setattr(agent, "evaluate_data_file", _fake_eval)

    verdict = agent.evaluate_discovered_dataset(
        {
            "name": "orders",
            "data_file": None,
            "connector_name": "postgres",
            "source_format": "postgres_table",
            "source_location": "public.orders",
            "source_metadata": {"schema": "public", "table": "orders"},
        }
    )

    assert verdict["status"] == "PASSED"
    assert captured["dataset_name"] == "orders"
    assert Path(captured["file_path"]).exists()
    assert "data/staged_connector/" in captured["file_path"]
    assert len(stub_connector.read_calls) == 1


import asyncio

from fastapi.testclient import TestClient

import src.api as api_module


class _LocalConnector:
    name = "local_files"

    def discover(self):
        return [{"name": "orders"}]


class _PostgresConnector:
    name = "postgres"

    def discover(self):
        return [{"name": "public.payments"}, {"name": "public.refunds"}]


class _BrokenConnector:
    name = "trino"

    def discover(self):
        raise RuntimeError("cannot connect to trino")


def test_lineage_missing_dataset_returns_empty_summary(monkeypatch):
    client = TestClient(api_module.app)
    graph = {
        "datasets": {
            "orders": {
                "upstream": ["raw_orders"],
                "consumers": [],
            }
        }
    }

    def _fake_summarize(payload=None):
        datasets = (payload or graph).get("datasets", {})
        return {
            "summary": {
                "dataset_count": len(datasets),
                "upstream_edge_count": 0,
                "managed_upstream_edge_count": 0,
                "external_upstream_count": 0,
                "consumer_count": 0,
                "owner_coverage_pct": 0.0,
            },
            "issues": {"external_upstream_refs": []},
        }

    monkeypatch.setattr(api_module.agent.impact_analyzer, "refresh", lambda: graph)
    monkeypatch.setattr(api_module.agent.impact_analyzer, "summarize_lineage", _fake_summarize)

    res = client.get("/lineage?dataset=missing")
    assert res.status_code == 200
    payload = res.json()
    assert payload["datasets"] == {}
    assert payload["summary"]["dataset_count"] == 0


def test_lineage_filter_handles_string_consumers(monkeypatch):
    client = TestClient(api_module.app)
    graph = {
        "datasets": {
            "orders": {
                "upstream": [],
                "consumers": ["payments"],
            },
            "payments": {
                "upstream": ["orders"],
                "consumers": [],
            },
        }
    }

    monkeypatch.setattr(api_module.agent.impact_analyzer, "refresh", lambda: graph)
    monkeypatch.setattr(api_module.agent.impact_analyzer, "summarize_lineage", lambda payload=None: {"summary": {"dataset_count": len((payload or graph).get("datasets", {}))}, "issues": {"external_upstream_refs": []}})

    res = client.get("/lineage?dataset=orders")
    assert res.status_code == 200
    payload = res.json()
    assert set(payload["datasets"].keys()) == {"orders", "payments"}


def test_source_integrations_reports_connector_status(monkeypatch):
    client = TestClient(api_module.app)

    monkeypatch.setattr(
        api_module.agent,
        "discover_datasets",
        lambda: [
            {"name": "orders", "data_file": "data/orders.csv"},
            {"name": "payments", "connector_name": "postgres"},
        ],
    )
    monkeypatch.setattr(
        api_module.agent,
        "connectors",
        [_LocalConnector(), _PostgresConnector(), _BrokenConnector()],
    )

    res = client.get("/integrations/sources")
    assert res.status_code == 200
    payload = res.json()
    by_id = {item["id"]: item for item in payload["integrations"]}

    assert by_id["local_files"]["status"] == "CONNECTED"
    assert by_id["local_files"]["dataset_count"] == 1
    assert by_id["postgres"]["status"] == "CONNECTED"
    assert by_id["postgres"]["dataset_count"] == 1
    assert by_id["postgres"]["discovered_count"] == 2
    assert by_id["trino"]["status"] == "ERROR"
    assert "cannot connect" in by_id["trino"]["details"]["error"]


def test_platform_config_returns_runtime_values(monkeypatch):
    client = TestClient(api_module.app)

    monkeypatch.setattr(api_module.agent, "connectors", [_PostgresConnector()])
    monkeypatch.setattr(api_module.rbac_service, "enabled", True)
    monkeypatch.setenv("CONTRACT_STORE_BACKEND", "file")

    res = client.get("/platform/config")
    assert res.status_code == 200
    runtime = res.json()["runtime"]

    assert runtime["contract_store_backend"] == "file"
    assert runtime["rbac_enabled"] is True
    assert runtime["connectors_enabled"] == ["postgres"]
    assert "max_workers" in runtime["async_jobs"]


def test_risk_endpoint_delegates_to_service(monkeypatch):
    client = TestClient(api_module.app)

    class _Service:
        def list_datasets_by_risk(self, limit: int = 20):
            return {
                "limit": limit,
                "total_ranked": 1,
                "datasets": [{"dataset_name": "orders", "risk_score": 88.5}],
            }

    monkeypatch.setattr(api_module, "service", _Service())

    res = client.get("/risk/datasets?limit=5")
    assert res.status_code == 200
    payload = res.json()
    assert payload["limit"] == 5
    assert payload["datasets"][0]["dataset_name"] == "orders"


def test_workflow_timeline_endpoint_delegates_to_service(monkeypatch):
    client = TestClient(api_module.app)

    class _Service:
        def get_workflow_timeline(self, dataset_name=None, limit: int = 100):
            return {
                "dataset_name": dataset_name,
                "limit": limit,
                "events": [{"event_id": "run:r1", "channel": "run"}],
                "summary": {"total_events": 1},
            }

    monkeypatch.setattr(api_module, "service", _Service())

    res = client.get("/workflow/timeline?dataset_name=orders&limit=12")
    assert res.status_code == 200
    payload = res.json()
    assert payload["dataset_name"] == "orders"
    assert payload["limit"] == 12
    assert payload["summary"]["total_events"] == 1


def test_agentic_graph_endpoint_delegates_to_service(monkeypatch):
    client = TestClient(api_module.app)

    class _Service:
        def get_agentic_workflow_graph(self):
            return {"engine": "langgraph", "mermaid": "graph TD;A-->B;"}

    monkeypatch.setattr(api_module, "service", _Service())

    res = client.get("/workflow/agentic/graph")
    assert res.status_code == 200
    payload = res.json()
    assert payload["engine"] == "langgraph"
    assert "graph TD" in payload["mermaid"]


def test_agentic_run_endpoint_delegates_to_service(monkeypatch):
    client = TestClient(api_module.app)

    class _RBAC:
        @staticmethod
        def enforce(_role, _permission):
            return None

    class _Service:
        def run_agentic_reliability_loop(self, **kwargs):
            return {"execution": {"decision": "proposed_only"}, "kwargs": kwargs}

    monkeypatch.setattr(api_module, "rbac_service", _RBAC())
    monkeypatch.setattr(api_module, "service", _Service())

    res = client.post(
        "/workflow/agentic/run",
        json={
            "dataset_name": "orders",
            "metric": "row_count",
            "auto_execute": False,
            "confidence_threshold": 0.8,
        },
    )
    assert res.status_code == 200
    payload = res.json()
    assert payload["execution"]["decision"] == "proposed_only"
    assert payload["kwargs"]["dataset_name"] == "orders"
    assert payload["kwargs"]["metric"] == "row_count"


def test_workflow_timeline_stream_emits_timeline_event(monkeypatch):
    class _Service:
        def get_workflow_timeline(self, dataset_name=None, limit: int = 100):
            return {
                "dataset_name": dataset_name,
                "limit": limit,
                "events": [{"event_id": "run:r1", "channel": "run"}],
                "summary": {"total_events": 1},
            }

    monkeypatch.setattr(api_module, "service", _Service())
    monkeypatch.setattr(api_module.asyncio, "sleep", lambda _secs: (_ for _ in ()).throw(asyncio.CancelledError()))

    response = asyncio.run(
        api_module.stream_workflow_timeline(dataset_name="orders", limit=5, interval_ms=1000)
    )
    assert response.media_type == "text/event-stream"

    first_chunk = asyncio.run(response.body_iterator.__anext__())
    text = first_chunk.decode() if isinstance(first_chunk, bytes) else str(first_chunk)
    assert "event: timeline" in text
    assert "\"dataset_name\": \"orders\"" in text

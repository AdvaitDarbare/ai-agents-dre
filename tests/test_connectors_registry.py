from src.connectors.local_files import LocalFilesConnector
from src.connectors.registry import build_connectors


class _DummyPostgres:
    name = "postgres"


class _DummyS3:
    name = "s3"


def test_build_connectors_defaults_to_local_files(monkeypatch):
    monkeypatch.delenv("DRE_CONNECTOR_LOCAL_FILES", raising=False)
    monkeypatch.delenv("DRE_CONNECTOR_POSTGRES", raising=False)
    monkeypatch.delenv("DRE_CONNECTOR_S3", raising=False)

    connectors = build_connectors()

    assert any(isinstance(conn, LocalFilesConnector) for conn in connectors)


def test_build_connectors_can_enable_postgres(monkeypatch):
    monkeypatch.setenv("DRE_CONNECTOR_LOCAL_FILES", "0")
    monkeypatch.setenv("DRE_CONNECTOR_POSTGRES", "1")
    monkeypatch.setattr(
        "src.connectors.registry.PostgresConnector.from_env",
        lambda: _DummyPostgres(),
    )

    connectors = build_connectors()

    assert [getattr(conn, "name", "") for conn in connectors] == ["postgres"]


def test_build_connectors_can_enable_s3(monkeypatch):
    monkeypatch.setenv("DRE_CONNECTOR_LOCAL_FILES", "0")
    monkeypatch.setenv("DRE_CONNECTOR_POSTGRES", "0")
    monkeypatch.setenv("DRE_CONNECTOR_S3", "1")
    monkeypatch.setattr(
        "src.connectors.registry.S3Connector.from_env",
        lambda: _DummyS3(),
    )

    connectors = build_connectors()

    assert [getattr(conn, "name", "") for conn in connectors] == ["s3"]

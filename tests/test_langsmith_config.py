from src.utils.langsmith import build_runnable_config


def test_build_runnable_config_without_tracing(monkeypatch):
    monkeypatch.delenv("LANGSMITH_TRACING", raising=False)
    config = build_runnable_config(configurable={"thread_id": "abc"}, run_name="run", tags=["x"], metadata={"k": "v"})
    assert config == {"configurable": {"thread_id": "abc"}}


def test_build_runnable_config_with_tracing(monkeypatch):
    monkeypatch.setenv("LANGSMITH_TRACING", "true")
    config = build_runnable_config(
        configurable={"thread_id": "abc"},
        run_name="run",
        tags=["x", "y"],
        metadata={"k": "v"},
    )
    assert config["configurable"]["thread_id"] == "abc"
    assert config["run_name"] == "run"
    assert config["tags"] == ["x", "y"]
    assert config["metadata"] == {"k": "v"}

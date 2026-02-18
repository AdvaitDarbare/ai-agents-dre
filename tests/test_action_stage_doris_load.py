from types import SimpleNamespace

from src.pipeline.stages import action_stage


class _DummyLoader:
    def __init__(self):
        self.calls = []

    def load_data(self, df, table_name):
        self.calls.append((df, table_name))
        return {"success": True, "Status": "Success", "NumberLoadedRows": 2}


class _FailingLoader:
    def __init__(self, exc: Exception):
        self.exc = exc
        self.calls = []

    def load_data(self, df, table_name):
        self.calls.append((df, table_name))
        raise self.exc


class _DummyToolLogger:
    def __init__(self):
        self.calls = []

    def log_simple(self, **kwargs):
        self.calls.append(kwargs)


class _DummyAgent:
    def __init__(self):
        self.loader = _DummyLoader()

    def _diagnose_root_cause(self, _dataset_name):
        return "db_unreachable"


def _build_ctx(status: str):
    return SimpleNamespace(
        dataset_name="orders",
        df=[{"id": 1}, {"id": 2}],
        verdict={"status": status, "reason": "test"},
        tool_logger=_DummyToolLogger(),
    )


def test_action_stage_loads_doris_for_passed(monkeypatch):
    monkeypatch.setenv("DRE_DORIS_LOAD_ENABLED", "1")
    agent = _DummyAgent()
    ctx = _build_ctx("PASSED")

    action_stage.run(agent, ctx)

    assert len(agent.loader.calls) == 1
    assert ctx.verdict["load_status"]["success"] is True
    assert ctx.tool_logger.calls and ctx.tool_logger.calls[0]["tool_name"] == "doris_loader"


def test_action_stage_skips_warning(monkeypatch):
    monkeypatch.setenv("DRE_DORIS_LOAD_ENABLED", "1")
    agent = _DummyAgent()
    ctx = _build_ctx("WARNING")

    action_stage.run(agent, ctx)

    assert len(agent.loader.calls) == 0
    assert ctx.verdict["load_status"] == "SKIPPED (Only PASSED loads to Doris)"


def test_action_stage_skips_when_disabled(monkeypatch):
    monkeypatch.setenv("DRE_DORIS_LOAD_ENABLED", "0")
    agent = _DummyAgent()
    ctx = _build_ctx("PASSED")

    action_stage.run(agent, ctx)

    assert len(agent.loader.calls) == 0
    assert ctx.verdict["load_status"] == "SKIPPED (Doris load disabled)"


def test_action_stage_missing_pymysql_is_warning(monkeypatch):
    monkeypatch.setenv("DRE_DORIS_LOAD_ENABLED", "1")
    agent = _DummyAgent()
    agent.loader = _FailingLoader(RuntimeError("DORIS_AUTO_CREATE_TABLE requires PyMySQL"))
    ctx = _build_ctx("PASSED")

    action_stage.run(agent, ctx)

    assert len(agent.loader.calls) == 1
    assert ctx.verdict["status"] == "WARNING"
    assert ctx.verdict["load_status"] == "SKIPPED (Infra Error)"

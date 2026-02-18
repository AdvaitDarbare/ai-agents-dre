from src.tools.doris_loader import DorisLoader


def test_rewrite_private_redirect_for_localhost(monkeypatch):
    monkeypatch.setenv("DORIS_FE_HOST", "127.0.0.1")
    monkeypatch.delenv("DORIS_STREAM_LOAD_REDIRECT_HOST", raising=False)
    monkeypatch.delenv("DORIS_STREAM_LOAD_REDIRECT_PORT", raising=False)

    loader = DorisLoader()
    rewritten = loader._resolve_redirect_url(
        "http://172.21.80.3:8040/api/test_db/demo/_stream_load"
    )

    assert rewritten == "http://127.0.0.1:8040/api/test_db/demo/_stream_load"


def test_rewrite_redirect_with_explicit_override(monkeypatch):
    monkeypatch.setenv("DORIS_FE_HOST", "127.0.0.1")
    monkeypatch.setenv("DORIS_STREAM_LOAD_REDIRECT_HOST", "localhost")
    monkeypatch.setenv("DORIS_STREAM_LOAD_REDIRECT_PORT", "18040")

    loader = DorisLoader()
    rewritten = loader._resolve_redirect_url(
        "http://172.21.80.3:8040/api/test_db/demo/_stream_load"
    )

    assert rewritten == "http://localhost:18040/api/test_db/demo/_stream_load"

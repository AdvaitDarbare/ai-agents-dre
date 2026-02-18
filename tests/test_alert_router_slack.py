from pathlib import Path

from src.tools.alert_router import AlertRouter


def _write_config(path: Path) -> None:
    path.write_text(
        """
channels:
  slack_general:
    type: slack
    webhook_env: SLACK_WEBHOOK_URL
routing:
  WARNING:
    channels: [slack_general]
    cooldown_minutes: 0
  BLOCKED:
    channels: [slack_general]
    cooldown_minutes: 0
"""
    )


def test_send_alert_skips_passed(tmp_path, monkeypatch):
    config_path = tmp_path / "alerts.yaml"
    _write_config(config_path)

    called = {"count": 0}

    def _fake_post(*_args, **_kwargs):
        called["count"] += 1
        raise AssertionError("requests.post should not be called for PASSED")

    monkeypatch.setattr("src.tools.alert_router.requests.post", _fake_post)

    router = AlertRouter(config_path=str(config_path))
    router.send_alert({"status": "PASSED", "dataset": "orders"}, {"criticality": "HIGH", "owner": "data"})

    assert called["count"] == 0


def test_send_alert_posts_slack_with_llm_report(tmp_path, monkeypatch):
    config_path = tmp_path / "alerts.yaml"
    _write_config(config_path)
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")
    monkeypatch.setenv("ALERTS_SLACK_ENABLED", "1")

    captured = {}

    class _Resp:
        status_code = 200
        text = "ok"

    def _fake_post(url, json=None, timeout=0):
        captured["url"] = url
        captured["json"] = json
        captured["timeout"] = timeout
        return _Resp()

    monkeypatch.setattr("src.tools.alert_router.requests.post", _fake_post)

    router = AlertRouter(config_path=str(config_path))
    router.send_alert(
        {
            "status": "WARNING",
            "dataset": "orders",
            "reason": "Row count dropped 40%",
            "llm_advice": "Potential upstream ingestion delay. Verify source connector and replay missing window.",
            "run_id": "run-123",
            "profile": {"weighted_quality_score": 71.5},
            "anomalies": [{"metric": "row_count"}],
        },
        {"criticality": "HIGH", "owner": "DataOps"},
    )

    assert captured["url"].startswith("https://hooks.slack.com/services/")
    assert "blocks" in captured["json"]
    block_text = " ".join(
        block.get("text", {}).get("text", "")
        for block in captured["json"]["blocks"]
        if isinstance(block, dict)
    )
    assert "LLM Report" in block_text
    assert "Row count dropped 40%" in block_text


def test_send_alert_skips_slack_when_webhook_missing(tmp_path, monkeypatch):
    config_path = tmp_path / "alerts.yaml"
    _write_config(config_path)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)

    called = {"count": 0}

    def _fake_post(*_args, **_kwargs):
        called["count"] += 1
        return None

    monkeypatch.setattr("src.tools.alert_router.requests.post", _fake_post)

    router = AlertRouter(config_path=str(config_path))
    router.send_alert({"status": "WARNING", "dataset": "orders", "reason": "x"}, {"criticality": "HIGH"})

    assert called["count"] == 0


def test_send_alert_uses_owner_route_channels(tmp_path, monkeypatch):
    config_path = tmp_path / "alerts.yaml"
    config_path.write_text(
        """
channels:
  slack_general:
    type: slack
    webhook_env: SLACK_WEBHOOK_URL
  slack_dataops:
    type: slack
    webhook_env: SLACK_WEBHOOK_URL_DATAOPS
routing:
  WARNING:
    channels: [slack_general]
    cooldown_minutes: 0
owner_routes:
  DataOps: [slack_dataops]
"""
    )

    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/general")
    monkeypatch.setenv("SLACK_WEBHOOK_URL_DATAOPS", "https://hooks.slack.com/services/T/B/dataops")
    monkeypatch.setenv("ALERTS_SLACK_ENABLED", "1")

    calls = []

    class _Resp:
        status_code = 200
        text = "ok"

    def _fake_post(url, json=None, timeout=0):
        calls.append({"url": url, "json": json, "timeout": timeout})
        return _Resp()

    monkeypatch.setattr("src.tools.alert_router.requests.post", _fake_post)

    router = AlertRouter(config_path=str(config_path))
    router.send_alert(
        {"status": "WARNING", "dataset": "orders", "reason": "dq drift"},
        {"criticality": "HIGH", "owner": "DataOps"},
    )

    urls = {item["url"] for item in calls}
    assert "https://hooks.slack.com/services/T/B/general" in urls
    assert "https://hooks.slack.com/services/T/B/dataops" in urls

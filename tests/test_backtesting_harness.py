from src.tools.monitor_backtesting import MonitorBacktestingHarness


def test_backtesting_labels_and_predictions_shapes():
    harness = MonitorBacktestingHarness()
    values = [90.0, 95.0, 100.0, 105.0, 110.0] * 6 + [260.0]
    labels = harness._robust_labels(values)  # noqa: SLF001
    preds = harness._rolling_z_predictions(values)  # noqa: SLF001

    assert len(labels) == len(values)
    assert len(preds) == len(values)
    assert any(v == 1 for v in labels)


def test_backtesting_insufficient_history(monkeypatch):
    harness = MonitorBacktestingHarness()
    monkeypatch.setattr(harness, "_load_metric_history", lambda dataset_name, metric_name, limit=500: [1.0, 2.0])
    report = harness.run("orders", "row_count")
    assert report["status"] == "insufficient_history"

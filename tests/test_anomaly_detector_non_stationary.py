from src.tools.anomaly_detector import AnomalyDetector


def test_baseline_switches_to_ewma_on_drift(monkeypatch):
    detector = AnomalyDetector()

    # Newest->oldest ordering (matching SQL fetch)
    values = [200.0] * 12 + [100.0] * 12
    monkeypatch.setattr(detector, "_get_baseline_values", lambda dataset_name, metric_name: (values, "global"))

    stats = detector._get_baseline_stats("orders", "row_count")  # noqa: SLF001

    assert str(stats["baseline_type"]).endswith("_ewma")
    assert stats["sample_count"] == len(values)

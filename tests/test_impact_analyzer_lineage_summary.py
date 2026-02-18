import yaml

from src.tools.impact_analyzer import ImpactAnalyzer


def test_summarize_lineage_counts_and_external_refs(tmp_path):
    lineage = {
        "datasets": {
            "orders": {
                "owner": "analytics",
                "upstream": ["raw_orders", "payments"],
                "consumers": [{"name": "orders_dashboard"}],
            },
            "payments": {
                "upstream": [],
                "consumers": [],
            },
        }
    }
    lineage_file = tmp_path / "lineage.yaml"
    lineage_file.write_text(yaml.safe_dump(lineage))

    analyzer = ImpactAnalyzer(str(lineage_file))
    result = analyzer.summarize_lineage()

    assert result["summary"]["dataset_count"] == 2
    assert result["summary"]["upstream_edge_count"] == 2
    assert result["summary"]["managed_upstream_edge_count"] == 1
    assert result["summary"]["external_upstream_count"] == 1
    assert result["summary"]["consumer_count"] == 1
    assert result["summary"]["owner_coverage_pct"] == 50.0
    assert result["issues"]["external_upstream_refs"] == [
        {"dataset": "orders", "upstream": "raw_orders"}
    ]


def test_refresh_reloads_lineage_from_disk(tmp_path):
    lineage_file = tmp_path / "lineage.yaml"
    lineage_file.write_text(yaml.safe_dump({"datasets": {"orders": {"upstream": []}}}))

    analyzer = ImpactAnalyzer(str(lineage_file))
    assert set(analyzer.lineage_graph.get("datasets", {}).keys()) == {"orders"}

    lineage_file.write_text(
        yaml.safe_dump(
            {
                "datasets": {
                    "orders": {"upstream": []},
                    "payments": {"upstream": ["orders"]},
                }
            }
        )
    )

    refreshed = analyzer.refresh()
    assert set(refreshed.get("datasets", {}).keys()) == {"orders", "payments"}


def test_get_downstream_impact_accepts_string_consumers(tmp_path):
    lineage = {
        "datasets": {
            "orders": {
                "consumers": ["finance_dashboard"],
            }
        }
    }
    lineage_file = tmp_path / "lineage.yaml"
    lineage_file.write_text(yaml.safe_dump(lineage))

    analyzer = ImpactAnalyzer(str(lineage_file))
    result = analyzer.get_downstream_impact("orders")

    assert result["overall_criticality"] == "LOW"
    assert result["impacted_consumers"] == [
        {
            "name": "finance_dashboard",
            "type": "unknown",
            "owner": "Unknown",
            "criticality": "LOW",
        }
    ]

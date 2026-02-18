import pandas as pd
import yaml

from src.tools.contract_generator import DataContractGenerator


def _write_sample_csv(tmp_path):
    path = tmp_path / "sample.csv"
    df = pd.DataFrame(
        {
            "age": [21, 35, 42],
            "amount": [10.5, 99.9, 40.25],
            "event_date": ["2026-01-01", "2026-01-02", "2026-01-03"],
            "status": ["ok", "ok", "warn"],
        }
    )
    df.to_csv(path, index=False)
    return path


def test_fallback_defaults_avoid_hard_bounds(tmp_path, monkeypatch):
    csv_path = _write_sample_csv(tmp_path)

    monkeypatch.delenv("DRE_CONTRACT_GENERATOR_HARD_BOUNDS", raising=False)
    monkeypatch.setenv("DRE_CONTRACT_GENERATOR_STRICT_NULLABLE", "0")

    generator = DataContractGenerator(cli_binary="datacontract-missing")
    result = generator.generate_from_source(str(csv_path), dataset_name="sample")

    payload = yaml.safe_load(result.yaml_content)
    columns = payload.get("columns", [])

    assert result.engine == "fallback"
    assert result.success is True
    assert payload.get("quality", {}).get("min_rows") == 1
    assert "anomaly_thresholds" in payload.get("quality", {})
    assert "slos" in payload.get("quality", {})
    assert all("min_value" not in col and "max_value" not in col for col in columns if isinstance(col, dict))
    assert any("Semantic rules" in warning for warning in result.warnings)


def test_hard_bounds_are_opt_in(tmp_path, monkeypatch):
    csv_path = _write_sample_csv(tmp_path)

    monkeypatch.setenv("DRE_CONTRACT_GENERATOR_HARD_BOUNDS", "1")

    generator = DataContractGenerator(cli_binary="datacontract-missing")
    result = generator.generate_from_source(str(csv_path), dataset_name="sample")

    payload = yaml.safe_load(result.yaml_content)
    columns = {col.get("name"): col for col in payload.get("columns", []) if isinstance(col, dict)}

    assert "amount" in columns
    assert "min_value" in columns["amount"]
    assert "max_value" in columns["amount"]


def test_cli_yaml_harmonizes_with_observed_schema(tmp_path, monkeypatch):
    csv_path = _write_sample_csv(tmp_path)

    monkeypatch.setenv("DRE_CONTRACT_GENERATOR_HARD_BOUNDS", "0")
    monkeypatch.setenv("DRE_CONTRACT_GENERATOR_HARMONIZE_TYPES", "1")

    generator = DataContractGenerator(cli_binary="datacontract-missing")

    seed_yaml = yaml.safe_dump(
        {
            "kind": "DataContract",
            "apiVersion": "v3.1.0",
            "id": "urn:datacontract:sample",
            "table_name": "sample",
            "columns": [
                {"name": "age", "data_type": "varchar", "nullable": False},
                {"name": "amount", "data_type": "varchar", "nullable": False},
                {"name": "event_date", "data_type": "varchar", "nullable": False},
                {"name": "status", "data_type": "varchar", "nullable": False},
            ],
        },
        sort_keys=False,
    )

    harmonized_yaml, warnings = generator._harmonize_yaml_with_observed_schema(
        seed_yaml,
        source_path=csv_path,
        fmt="csv",
    )
    payload = yaml.safe_load(harmonized_yaml)
    columns = {col.get("name"): col for col in payload.get("columns", []) if isinstance(col, dict)}

    assert columns["age"].get("data_type") == "integer"
    assert columns["amount"].get("data_type") == "double"
    assert payload.get("quality", {}).get("min_rows") == 1
    assert any("Aligned generated column types" in warning for warning in warnings)

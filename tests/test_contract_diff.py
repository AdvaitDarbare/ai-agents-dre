import yaml

from src.tools.contract_diff import merge_contracts


def test_merge_adds_new_columns():
    current = """
columns:
  - name: id
    data_type: varchar
  - name: amount
    data_type: double
"""
    observed = """
columns:
  - name: id
    data_type: varchar
  - name: amount
    data_type: double
  - name: status
    data_type: varchar
    nullable: false
"""
    merged_yaml, summary = merge_contracts(current, observed)
    merged = yaml.safe_load(merged_yaml)
    names = [c["name"] for c in merged["columns"]]
    assert "status" in names
    assert "status" in summary["added_columns"]


def test_merge_updates_types():
    current = """
columns:
  - name: amount
    data_type: varchar
"""
    observed = """
columns:
  - name: amount
    data_type: double
"""
    merged_yaml, summary = merge_contracts(current, observed)
    merged = yaml.safe_load(merged_yaml)
    assert merged["columns"][0]["data_type"] == "double"
    assert summary["updated_types"][0]["name"] == "amount"


def test_merge_preserves_existing_fields():
    current = """
columns:
  - name: status
    data_type: varchar
    description: Current description
"""
    observed = """
columns:
  - name: status
    data_type: varchar
    description: Observed description
    allowed_values: ["A", "B"]
"""
    merged_yaml, _ = merge_contracts(current, observed)
    merged = yaml.safe_load(merged_yaml)
    status = merged["columns"][0]
    assert status["description"] == "Current description"
    assert status["allowed_values"] == ["A", "B"]

"""
Deterministic contract diff/merge utilities.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Tuple

import yaml


def parse_contract(yaml_content: str) -> Dict[str, Any]:
    data = yaml.safe_load(yaml_content) or {}
    if not isinstance(data, dict):
        return {}
    return data


def merge_contracts(current_yaml: str, observed_yaml: str) -> Tuple[str, Dict[str, Any]]:
    """
    Deterministically merge observed schema into current contract.

    Rules:
    - Never remove existing columns.
    - Add new observed columns.
    - Update data_type when mismatched.
    - Fill missing attributes from observed when current lacks them.
    """
    current = parse_contract(current_yaml)
    observed = parse_contract(observed_yaml)

    summary = {
        "added_columns": [],
        "updated_types": [],
        "filled_fields": [],
        "warnings": [],
    }

    if not current.get("columns") or not observed.get("columns"):
        summary["warnings"].append("Missing columns in current or observed contract; returning current YAML.")
        return current_yaml, summary

    merged = deepcopy(current)
    merged_columns: List[Dict[str, Any]] = merged.get("columns", [])

    current_index = {c.get("name"): c for c in merged_columns if isinstance(c, dict)}
    observed_columns = [c for c in observed.get("columns", []) if isinstance(c, dict)]

    for observed_col in observed_columns:
        name = observed_col.get("name")
        if not name:
            continue

        if name not in current_index:
            merged_columns.append(_sanitize_observed_column(observed_col))
            summary["added_columns"].append(name)
            continue

        current_col = current_index[name]
        current_type = _normalize_type(current_col.get("data_type"))
        observed_type = _normalize_type(observed_col.get("data_type"))
        if observed_type and current_type and current_type != observed_type:
            summary["updated_types"].append(
                {"name": name, "from": current_col.get("data_type"), "to": observed_col.get("data_type")}
            )
            current_col["data_type"] = observed_col.get("data_type")

        # Update statistical fields from observed data (Trust the data for these)
        for stat_field in ["min_value", "max_value", "allowed_values", "unique_values", "null_count"]:
            if stat_field in observed_col:
                # If value changed, update it
                if current_col.get(stat_field) != observed_col[stat_field]:
                    # Only log if actually updating an existing value
                    if stat_field in current_col:
                        # Optional: log specific value updates if needed, for now just silent update
                        pass 
                    current_col[stat_field] = observed_col[stat_field]
            elif stat_field in current_col and _is_inferred_column(current_col):
                # Prevent stale auto-generated constraints from lingering forever.
                # Human-authored contracts can still keep these fields by using non-inferred descriptions.
                del current_col[stat_field]
                summary["filled_fields"].append({"column": current_col.get("name"), "field": f"removed_{stat_field}"})

        # Fill missing governance fields from observed (only if missing in current)
        _fill_missing_governance_fields(current_col, observed_col, summary)

    merged["columns"] = merged_columns
    return yaml.safe_dump(merged, sort_keys=False), summary


def _fill_missing_governance_fields(current_col: Dict[str, Any], observed_col: Dict[str, Any], summary: Dict[str, Any]) -> None:
    # Fields we PRESERVE from current if they exist (Governance/Human-authored)
    # We only fill them if they are completely missing in current.
    for key in ("nullable", "description", "pattern", "pii", "isPrimaryKey"):
        if key in current_col and current_col[key] is not None:
            continue
        if key in observed_col and observed_col[key] is not None:
            current_col[key] = observed_col[key]
            summary["filled_fields"].append({"column": current_col.get("name"), "field": key})


def _sanitize_observed_column(observed_col: Dict[str, Any]) -> Dict[str, Any]:
    allowed_keys = {
        "name",
        "data_type",
        "nullable",
        "description",
        "pattern",
        "allowed_values",
        "min_value",
        "max_value",
        "isPrimaryKey",
    }
    sanitized = {k: v for k, v in observed_col.items() if k in allowed_keys}
    if "description" not in sanitized or not sanitized["description"]:
        sanitized["description"] = "Imported from observed schema"
    return sanitized


def _normalize_type(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _is_inferred_column(column: Dict[str, Any]) -> bool:
    desc = str(column.get("description") or "").lower()
    return "inferred from" in desc or "imported from observed schema" in desc

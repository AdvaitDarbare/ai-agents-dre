from __future__ import annotations

from pathlib import Path
from typing import Any

import time
import yaml as _yaml

from src.pipeline.context import PipelineContext
from src.tools.dimension_scorer import DimensionScorer
from src.tools.schema_validator import validate_schema


def run(agent: Any, ctx: PipelineContext) -> bool:
    """
    Stage A: schema validation + contract threshold load.
    Returns False when verdict is terminal and pipeline must stop.
    """
    print(f"\n🔍 [Stage A] Validating Schema for '{ctx.dataset_name}'...")
    ctx.contract_file = agent.contract_store.path_for(ctx.dataset_name)

    schema_start = time.time()
    ctx.schema_result = validate_schema(ctx.contract_file, ctx.file_path, source_type=ctx.source_type)
    schema_duration = int((time.time() - schema_start) * 1000)
    ctx.schema_diff = ctx.schema_result.get_schema_diff()

    ctx.tool_logger.log_simple(
        tool_name="schema_validator",
        status="SUCCESS" if ctx.schema_result.is_valid else "FAILED",
        output=ctx.schema_result.to_dict(),
        duration_ms=schema_duration,
    )

    ctx.verdict["schema_evolution"] = ctx.schema_diff
    ctx.verdict["schema_result"] = (
        ctx.schema_result.to_dict() if hasattr(ctx.schema_result, "to_dict") else {}
    )

    if ctx.schema_diff["missing_columns"] or ctx.schema_diff["type_mismatches"]:
        error_parts = []
        if ctx.schema_diff["missing_columns"]:
            error_parts.append(f"Missing columns: {', '.join(ctx.schema_diff['missing_columns'])}")
        if ctx.schema_diff["type_mismatches"]:
            mismatches = [
                f"{m['column']} (expected {m['expected']}, got {m['actual']})"
                for m in ctx.schema_diff["type_mismatches"]
            ]
            error_parts.append(f"Type mismatches: {', '.join(mismatches)}")

        ctx.verdict["status"] = "BLOCKED"
        ctx.verdict["reason"] = f"Schema Violation: {'; '.join(error_parts)}"
        ctx.verdict["actions"] = ["Quarantine", "Fix Schema/Data"]
        ctx.verdict["load_status"] = "SKIPPED (Blocked by Agent)"

        try:
            custom_weights = DimensionScorer.load_weights_from_contract(ctx.contract_file)
            dimension_scorer = DimensionScorer(weights=custom_weights)
            dimension_report = dimension_scorer.calculate_dimension_scores(
                dataset_name=ctx.dataset_name,
                schema_result=ctx.verdict["schema_result"],
                profile_report={},
                anomaly_report={"status": "PASS", "anomalies": [], "metrics": {}},
            )
            ctx.verdict["quality_dimensions"] = dimension_report.to_dict()
        except Exception as dim_err:
            print(f"⚠️ Failed to compute blocked-run dimension scores: {dim_err}")
        return False

    if ctx.schema_diff["new_columns"]:
        print(f"⚠️  Schema Evolution Detected: {len(ctx.schema_diff['new_columns'])} new columns.")

    ctx.contract_data = {}
    try:
        contract_doc = agent.contract_store.read(ctx.dataset_name)
        if contract_doc:
            ctx.contract_data = _yaml.safe_load(contract_doc.content) or {}
    except Exception:
        pass

    threshold_cfg = ctx.contract_data.get("quality", {}).get("anomaly_thresholds", {})
    ctx.thresholds = {
        "z_warn": threshold_cfg.get("z_score_warning", 2.5),
        "z_critical": threshold_cfg.get("z_score_critical", 3.0),
        "qs_warn": threshold_cfg.get("quality_score_warn", 80),
        "qs_block": threshold_cfg.get("quality_score_block", 50),
    }
    ctx.slo_targets = agent._extract_slo_targets(ctx.contract_data)

    print("\n🎯 [Impact Analysis] Assessing Criticality...")
    impact_start = time.time()
    impact = agent.impact_analyzer.get_downstream_impact(ctx.dataset_name)
    impact_duration = int((time.time() - impact_start) * 1000)
    criticality = impact.get("overall_criticality", "LOW")

    ctx.tool_logger.log_simple(
        tool_name="impact_analyzer",
        status="SUCCESS",
        output={
            "overall_criticality": criticality,
            "downstream_count": len(impact.get("downstream", [])),
        },
        duration_ms=impact_duration,
    )
    ctx.verdict["criticality"] = criticality

    return True


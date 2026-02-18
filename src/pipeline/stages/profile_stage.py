from __future__ import annotations

from typing import Any

import time

from src.pipeline.context import PipelineContext
from src.tools.dimension_scorer import DimensionScorer


def run(agent: Any, ctx: PipelineContext) -> bool:
    """
    Stage A2 + A3: value profiling, quality dimensions, and quality gates.
    Returns False when verdict is terminal and pipeline must stop.
    """
    print(f"\n🔬 [Stage A2] Profiling Data Values for '{ctx.dataset_name}'...")
    profile_start = time.time()
    ctx.profile_report = agent.profiler.profile(ctx.df, ctx.contract_file, ctx.dataset_name)
    profile_duration = int((time.time() - profile_start) * 1000)

    ctx.verdict["profile"] = {
        "overall_quality_score": ctx.profile_report.overall_quality_score,
        "constraint_violations": ctx.profile_report.constraint_violations,
        "custom_check_results": ctx.profile_report.custom_check_results,
        "column_scores": {k: v.quality_score for k, v in ctx.profile_report.column_profiles.items()},
        "null_rates": {k: v.null_rate for k, v in ctx.profile_report.column_profiles.items()},
        "violations_detail": {
            k: v.violations for k, v in ctx.profile_report.column_profiles.items() if v.violations
        },
        "column_profiles": {k: v.to_dict() for k, v in ctx.profile_report.column_profiles.items()},
    }

    ctx.tool_logger.log_simple(
        tool_name="data_profiler",
        status="SUCCESS",
        output={
            "overall_quality_score": ctx.profile_report.overall_quality_score,
            "total_violations": len(ctx.profile_report.constraint_violations),
            "column_count": len(ctx.profile_report.column_profiles),
        },
        duration_ms=profile_duration,
    )

    print("\n📊 [Stage A3] Calculating 6-Dimensional Quality Scores...")

    weights_start = time.time()
    custom_weights = DimensionScorer.load_weights_from_contract(ctx.contract_file)
    dimension_scorer = DimensionScorer(weights=custom_weights)
    _ = int((time.time() - weights_start) * 1000)

    print(
        f"   Using {'custom' if custom_weights else 'default'} weights: "
        f"Completeness {dimension_scorer.weights['Completeness']*100:.0f}%, "
        f"Validity {dimension_scorer.weights['Validity']*100:.0f}%, "
        f"Accuracy {dimension_scorer.weights['Accuracy']*100:.0f}%"
    )

    dimension_start = time.time()
    try:
        # Use anomaly_report from context if already available (e.g. re-evaluation),
        # otherwise use a neutral placeholder — anomaly stage runs after profile stage
        # so on first pass we don't yet have anomaly data.
        anomaly_report_for_scoring = getattr(ctx, "anomaly_report", None) or {
            "status": "PASS",
            "anomalies": [],
            "metrics": {},
        }
        dimension_report = dimension_scorer.calculate_dimension_scores(
            dataset_name=ctx.dataset_name,
            schema_result=ctx.schema_result.to_dict() if hasattr(ctx.schema_result, "to_dict") else {},
            profile_report=ctx.verdict.get("profile", {}),
            anomaly_report=anomaly_report_for_scoring,
        )
        ctx.verdict["quality_dimensions"] = dimension_report.to_dict()
        weighted_quality_score = dimension_report.overall_score
        ctx.verdict["profile"]["weighted_quality_score"] = weighted_quality_score

        dim_scores = {d.name: d.score for d in dimension_report.dimensions}
        print(
            f"   Weighted Quality Score: {weighted_quality_score:.1f}% "
            f"(Completeness: {dim_scores.get('Completeness', 0):.0f}%, "
            f"Validity: {dim_scores.get('Validity', 0):.0f}%, "
            f"Accuracy: {dim_scores.get('Accuracy', 0):.0f}%)"
        )
        dimension_scoring_success = True
    except Exception as exc:
        print(f"⚠️  Failed to calculate dimension scores: {exc}")
        weighted_quality_score = ctx.profile_report.overall_quality_score
        ctx.verdict["quality_dimensions"] = None
        dimension_scoring_success = False

    dimension_duration = int((time.time() - dimension_start) * 1000)
    ctx.tool_logger.log_simple(
        tool_name="dimension_scorer",
        status="SUCCESS" if dimension_scoring_success else "ERROR",
        output={
            "overall_score": weighted_quality_score,
            "weights_source": "custom" if custom_weights else "default",
            "dimension_count": 6 if dimension_scoring_success else 0,
        },
        duration_ms=dimension_duration,
    )

    effective_quality_score = ctx.verdict["profile"].get(
        "weighted_quality_score",
        ctx.profile_report.overall_quality_score,
    )
    score_type = "Weighted 6D" if "weighted_quality_score" in ctx.verdict["profile"] else "Simple average"
    
    # Unified SLO Enforcement
    min_qs_slo = ctx.slo_targets.get("min_quality_score", 80.0)
    qs_critical_threshold = ctx.thresholds.get("qs_block", 50.0)

    # 1. Block if critically low or if below SLO on a high-impact dataset
    is_high_impact = ctx.verdict.get("criticality", "LOW") in ["HIGH", "CRITICAL"]
    
    if effective_quality_score < qs_critical_threshold:
        ctx.verdict["status"] = "BLOCKED"
        ctx.verdict["reason"] = (
            f"Data Quality Score critically low: {effective_quality_score:.1f}% "
            f"(threshold: {qs_critical_threshold}%). Score type: {score_type}"
        )
        ctx.verdict["actions"] = ["Quarantine", "Investigate Value Violations"]
        ctx.verdict["load_status"] = "SKIPPED (Quality too low)"
        return False
    
    if effective_quality_score < min_qs_slo:
        if is_high_impact:
            ctx.verdict["status"] = "BLOCKED"
            ctx.verdict["reason"] = (
                f"SLO Violation: Quality Score {effective_quality_score:.1f}% is below target "
                f"({min_qs_slo}%) for a HIGH IMPACT dataset."
            )
            ctx.verdict["actions"] = ["Quarantine", "Fix Data to meet SLO"]
            ctx.verdict["load_status"] = "SKIPPED (SLO Violation)"
            return False
        else:
            ctx.verdict["status"] = "WARNING"
            ctx.verdict["reason"] = (
                f"SLO Warning: Quality Score {effective_quality_score:.1f}% is below target ({min_qs_slo}%)."
            )

    return True


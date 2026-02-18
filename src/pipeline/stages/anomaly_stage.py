from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import time

from src.pipeline.context import PipelineContext


def run(agent: Any, ctx: PipelineContext) -> None:
    """Stage B: anomaly detection + impact-aware severity decisioning."""
    print(f"\n📉 [Stage B] Checking Network Anomalies for '{ctx.dataset_name}'...")

    anomaly_inputs: Dict[str, Any] = {
        "row_count": {
            "value": len(ctx.df),
            "metric_group": "volume",
            "segment": "global",
            "tags": {"source": "pipeline"},
        }
    }
    try:
        freshness_age_minutes = max(0.0, (time.time() - Path(ctx.file_path).stat().st_mtime) / 60.0)
        freshness_payload = {
            "value": freshness_age_minutes,
            "metric_group": "freshness",
            "segment": "global",
            "tags": {"source": "filesystem"},
        }
        if ctx.slo_targets.get("freshness_max_minutes") is not None:
            freshness_payload["tags"]["slo_target_minutes"] = float(ctx.slo_targets["freshness_max_minutes"])
        anomaly_inputs["freshness_age_minutes"] = freshness_payload
    except Exception:
        pass

    anomaly_start = time.time()
    ctx.anomaly_report = agent.anomaly_detector.evaluate_run(
        ctx.dataset_name,
        anomaly_inputs,
        dataframe=ctx.df,
    )
    anomaly_duration = int((time.time() - anomaly_start) * 1000)

    ctx.tool_logger.log_simple(
        tool_name="anomaly_detector",
        status=ctx.anomaly_report["status"],
        output={
            "status": ctx.anomaly_report["status"],
            "anomaly_count": len(ctx.anomaly_report.get("anomalies", [])),
            "metrics_checked": len(ctx.anomaly_report.get("metrics", {})),
        },
        duration_ms=anomaly_duration,
    )

    ctx.verdict["anomalies"] = ctx.anomaly_report.get("anomalies", [])
    ctx.verdict["metrics"] = ctx.anomaly_report.get("metrics", {})
    criticality = ctx.verdict.get("criticality", "LOW")

    # --- SLO Enforcement (Freshness & Anomaly Count) ---
    is_high_impact = criticality in ["HIGH", "CRITICAL"]
    
    # 1. Freshness SLO Check
    freshness_val = anomaly_inputs.get("freshness_age_minutes", {}).get("value")
    freshness_slo = ctx.slo_targets.get("freshness_max_minutes")
    
    if freshness_val and freshness_slo and freshness_val > freshness_slo:
        if is_high_impact:
            ctx.verdict["status"] = "BLOCKED"
            ctx.verdict["reason"] = f"SLO Violation: Data is {freshness_val:.1f}m old (max {freshness_slo}m) on HIGH IMPACT dataset."
            ctx.verdict["actions"] = ["Quarantine", "Check Ingest Pipeline"]
        elif ctx.verdict["status"] != "BLOCKED":
            ctx.verdict["status"] = "WARNING"
            ctx.verdict["reason"] = f"SLA Warning: Data is {freshness_val:.1f}m old (max {freshness_slo}m)."

    # 2. Anomaly Count SLO Check
    anomaly_count = len(ctx.anomaly_report.get("anomalies", []))
    max_anomalies_slo = ctx.slo_targets.get("max_anomaly_count", 0)
    
    if anomaly_count > max_anomalies_slo:
        if is_high_impact:
            ctx.verdict["status"] = "BLOCKED"
            ctx.verdict["reason"] = f"SLO Violation: {anomaly_count} anomalies detected (limit {max_anomalies_slo}) on HIGH IMPACT dataset."
            ctx.verdict["actions"] = ["Quarantine", "Investigate Spikes"]
        elif ctx.verdict["status"] != "BLOCKED":
            ctx.verdict["status"] = "WARNING"
            ctx.verdict["reason"] = f"SLO Warning: {anomaly_count} anomalies detected (limit {max_anomalies_slo})."

    # --- Statistical Anomaly Decisioning (Existing Logic) ---
    if ctx.anomaly_report["status"] == "ANOMALY_DETECTED" and ctx.verdict["status"] != "BLOCKED":
        max_z = 0.0
        for anomaly in ctx.anomaly_report["anomalies"]:
            z = abs(anomaly.get("z_score", 0))
            if z > max_z:
                max_z = z

        z_warn = ctx.thresholds.get("z_warn", 2.5)
        z_critical = ctx.thresholds.get("z_critical", 3.0)

        if is_high_impact and max_z > z_critical:
            ctx.verdict["status"] = "BLOCKED"
            ctx.verdict["reason"] = (
                f"CRITICAL ANOMALY (Z={max_z:.1f}, threshold={z_critical}) on HIGH IMPACT dataset."
            )
            ctx.verdict["actions"] = ["Quarantine", "Alert Execs"]
        elif not is_high_impact and max_z > z_critical:
            ctx.verdict["status"] = "WARNING"
            ctx.verdict["reason"] = f"Anomaly detected (Z={max_z:.1f}), but impact is LOW."
            ctx.verdict["actions"] = ["Proceed to Load", "Log Warning"]
        elif max_z > z_warn:
            ctx.verdict["status"] = "WARNING"
            ctx.verdict["reason"] = f"Anomaly detected (Z={max_z:.1f}, warning threshold={z_warn})."

    ctx.verdict["metrics"] = ctx.anomaly_report.get("metrics", {})


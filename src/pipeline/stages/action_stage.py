from __future__ import annotations

import os
from typing import Any

import time

from src.pipeline.context import PipelineContext


def run(agent: Any, ctx: PipelineContext) -> None:
    """Stage C: downstream load execution or skip decision."""
    load_enabled = os.getenv("DRE_DORIS_LOAD_ENABLED", "1").strip() != "0"
    if not load_enabled:
        ctx.verdict["load_status"] = "SKIPPED (Doris load disabled)"
        return

    if ctx.force_load and ctx.verdict["status"] != "PASSED":
        ctx.verdict["reason"] = f"FORCE LOAD: {ctx.verdict['reason']}"
        print(f"⚠️  [Stage C] Force Load requested for {ctx.dataset_name}. Status is {ctx.verdict['status']}.")

    if ctx.verdict["status"] == "PASSED" or ctx.force_load:
        try:
            print("🚀 [Stage C] Loading Data into Doris...")
            load_start = time.time()
            load_result = agent.loader.load_data(ctx.df, ctx.dataset_name)
            load_duration = int((time.time() - load_start) * 1000)
            ctx.verdict["load_status"] = load_result
            load_ok = bool(
                load_result.get("success")
                or str(load_result.get("Status", "")).strip().lower() == "success"
            )

            ctx.tool_logger.log_simple(
                tool_name="doris_loader",
                status="SUCCESS" if load_ok else "ERROR",
                output=load_result,
                duration_ms=load_duration,
            )
        except Exception as exc:
            error_msg = str(exc).lower()
            if (
                "connection refused" in error_msg
                or "max retries exceeded" in error_msg
                or "pymysql" in error_msg
                or "requires" in error_msg
                or "import" in error_msg
                or "install" in error_msg
            ):
                root_cause = agent._diagnose_root_cause(ctx.dataset_name)
                note = f" (Note: Load skipped - Local DB unreachable. Root Cause: {root_cause})"
                ctx.verdict["reason"] += note
                ctx.verdict["load_status"] = "SKIPPED (Infra Error)"
                ctx.verdict["status"] = "WARNING"
            else:
                ctx.verdict["status"] = "BLOCKED"
                ctx.verdict["reason"] += f" (Load Failed: {exc})"
                ctx.verdict["load_status"] = {"error": str(exc)}
    else:
        status = ctx.verdict["status"]
        if status == "WARNING":
            ctx.verdict["load_status"] = "SKIPPED (Warning: Quality targets missed)"
        else:
            ctx.verdict["load_status"] = f"SKIPPED (Blocked: {ctx.verdict.get('reason', 'Unknown reason')})"

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from src.pipeline.context import PipelineContext


def run(ctx: PipelineContext) -> Optional[Dict[str, Any]]:
    """
    Stage 0: source detection and dataframe load.
    Returns a terminal verdict payload when the pipeline cannot proceed.
    """
    file_ext = Path(ctx.file_path).suffix.lower()
    if file_ext == ".parquet":
        ctx.source_type = "parquet"
    elif file_ext == ".json":
        ctx.source_type = "json"
    else:
        ctx.source_type = "csv"

    try:
        if ctx.source_type == "parquet":
            ctx.df = pd.read_parquet(ctx.file_path)
        elif ctx.source_type == "json":
            ctx.df = pd.read_json(ctx.file_path)
        else:
            ctx.df = pd.read_csv(ctx.file_path)
    except FileNotFoundError:
        return {
            "status": "BLOCKED",
            "reason": f"File not found: {ctx.file_path}",
            "actions": ["Abort"],
        }
    except Exception as exc:
        return {
            "status": "BLOCKED",
            "reason": f"Failed to load file: {exc}",
            "actions": ["Abort"],
        }

    return None


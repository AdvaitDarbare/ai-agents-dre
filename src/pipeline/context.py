from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

from src.utils.tool_logger import ToolLogger


@dataclass
class PipelineContext:
    """Shared state passed across deterministic pipeline stages."""

    dataset_name: str
    file_path: str
    start_time: float
    run_id: str
    tool_logger: ToolLogger
    verdict: Dict[str, Any]
    source_type: str = "csv"
    df: Any = None
    contract_file: Optional[Path] = None
    contract_data: Dict[str, Any] = field(default_factory=dict)
    thresholds: Dict[str, Any] = field(default_factory=dict)
    slo_targets: Dict[str, Any] = field(default_factory=dict)
    schema_result: Any = None
    schema_diff: Dict[str, Any] = field(default_factory=dict)
    profile_report: Any = None
    anomaly_report: Dict[str, Any] = field(default_factory=dict)
    force_load: bool = False


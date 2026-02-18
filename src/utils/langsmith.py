from __future__ import annotations

import os
from typing import Any, Dict, Iterable, Optional


def langsmith_enabled() -> bool:
    value = os.getenv("LANGSMITH_TRACING", "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def build_runnable_config(
    *,
    configurable: Optional[Dict[str, Any]] = None,
    run_name: Optional[str] = None,
    tags: Optional[Iterable[str]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build LangChain/LangGraph runnable config with optional LangSmith tracing metadata.
    """
    config: Dict[str, Any] = {"configurable": configurable or {}}

    if not langsmith_enabled():
        return config

    if run_name:
        config["run_name"] = run_name
    if tags:
        config["tags"] = [tag for tag in tags if isinstance(tag, str) and tag.strip()]
    if metadata:
        config["metadata"] = metadata
    return config

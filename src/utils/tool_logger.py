"""
Tool Logger - Logs execution details of each tool to PostgreSQL.

This provides granular visibility into which tools ran, their outputs, and performance.
"""

import json
import time
from typing import Any, Dict, Optional
from contextlib import contextmanager
from src.utils.database import get_connection


class ToolLogger:
    """Logs tool execution to the tool_outputs table."""

    def __init__(self, run_id: str, dataset_name: str):
        self.run_id = run_id
        self.dataset_name = dataset_name

    @contextmanager
    def log_tool(self, tool_name: str):
        """
        Context manager to log tool execution.

        Usage:
            with logger.log_tool("schema_validator") as log:
                result = validate_schema(...)
                log.set_output(result.to_dict())
        """
        log_entry = ToolLogEntry(
            run_id=self.run_id,
            dataset_name=self.dataset_name,
            tool_name=tool_name
        )

        start_time = time.time()
        try:
            yield log_entry
            log_entry.status = "SUCCESS"
        except Exception as e:
            log_entry.status = "ERROR"
            log_entry.set_output({"error": str(e)})
            raise
        finally:
            log_entry.duration_ms = int((time.time() - start_time) * 1000)
            log_entry.save()

    def log_simple(self, tool_name: str, status: str, output: Dict[str, Any], duration_ms: int = 0):
        """Simple one-shot logging without context manager."""
        log_entry = ToolLogEntry(
            run_id=self.run_id,
            dataset_name=self.dataset_name,
            tool_name=tool_name
        )
        log_entry.status = status
        log_entry.output = output
        log_entry.duration_ms = duration_ms
        log_entry.save()


class ToolLogEntry:
    """Represents a single tool execution log entry."""

    def __init__(self, run_id: str, dataset_name: str, tool_name: str):
        self.run_id = run_id
        self.dataset_name = dataset_name
        self.tool_name = tool_name
        self.status = "PENDING"
        self.output: Dict[str, Any] = {}
        self.duration_ms = 0

    def set_output(self, output: Dict[str, Any]):
        """Set the output dictionary for this tool execution."""
        self.output = output

    def save(self):
        """Save this log entry to PostgreSQL."""
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO tool_outputs (run_id, dataset_name, tool_name, status, output, duration_ms)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        self.run_id,
                        self.dataset_name,
                        self.tool_name,
                        self.status,
                        json.dumps(self.output),
                        self.duration_ms
                    ))
        except Exception as e:
            # Don't fail the pipeline if logging fails
            print(f"⚠️  Failed to save tool log for {self.tool_name}: {e}")

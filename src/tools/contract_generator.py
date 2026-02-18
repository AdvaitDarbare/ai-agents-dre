"""
Data Contract generation tool.

This module wraps datacontract-cli with deterministic fallback generation so
the platform can still produce YAML when source imports fail.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml


@dataclass
class ContractGenerationResult:
    """Structured result for contract generation."""

    yaml_content: str
    engine: str
    success: bool
    cli_available: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    command: List[str] = field(default_factory=list)
    generated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "yaml_content": self.yaml_content,
            "engine": self.engine,
            "success": self.success,
            "cli_available": self.cli_available,
            "errors": self.errors,
            "warnings": self.warnings,
            "command": self.command,
            "generated_at": self.generated_at,
        }


class DataContractGenerator:
    """
    Contract generation tool that prefers datacontract-cli and falls back to
    deterministic Pandas-based inference.
    """

    FORMAT_BY_EXTENSION = {
        ".csv": "csv",
        ".parquet": "parquet",
        ".json": "json",
    }
    DEFAULT_ANOMALY_THRESHOLDS = {
        "z_score_warning": 2.5,
        "z_score_critical": 3.0,
        "quality_score_warn": 80,
        "quality_score_block": 50,
    }
    DEFAULT_SLOS = {
        "min_quality_score": 80,
        "max_anomaly_count": 0,
    }

    def __init__(self, cli_binary: str = "datacontract", timeout_seconds: int = 45):
        self.cli_binary = cli_binary
        self.timeout_seconds = timeout_seconds
        self.include_hard_bounds = os.getenv("DRE_CONTRACT_GENERATOR_HARD_BOUNDS", "0").strip() == "1"
        self.strict_nullable = os.getenv("DRE_CONTRACT_GENERATOR_STRICT_NULLABLE", "0").strip() == "1"
        self.harmonize_with_observed_types = os.getenv(
            "DRE_CONTRACT_GENERATOR_HARMONIZE_TYPES",
            "1",
        ).strip() != "0"

    def generate_from_source(
        self,
        data_path: str,
        dataset_name: Optional[str] = None,
        fmt: Optional[str] = None,
    ) -> ContractGenerationResult:
        """Generate a contract YAML from a data source."""
        source_path = Path(data_path)
        dataset = dataset_name or source_path.stem or "unknown_dataset"
        inferred_format = fmt or self.FORMAT_BY_EXTENSION.get(source_path.suffix.lower(), "csv")
        cli_available = shutil.which(self.cli_binary) is not None

        if cli_available:
            cli_result = self._run_cli_import(source_path, inferred_format)
            if cli_result:
                return cli_result

        fallback = self._generate_fallback_contract(source_path, dataset)
        fallback.cli_available = cli_available
        if not cli_available:
            fallback.warnings.append(f"{self.cli_binary} not found on PATH; used fallback generator.")
        return fallback

    def _run_cli_import(self, source_path: Path, fmt: str) -> Optional[ContractGenerationResult]:
        """Attempt generation via datacontract-cli."""
        command = [self.cli_binary, "import", "--format", fmt, "--source", str(source_path)]

        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=False,
                timeout=self.timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            return None
        except Exception:
            return None

        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()
        if not stdout:
            return None

        if not self._looks_like_yaml(stdout):
            return None

        normalized_yaml, normalize_warning = self._normalize_cli_output(stdout, source_path)
        normalized_warnings: List[str] = []
        if self.harmonize_with_observed_types:
            normalized_yaml, normalized_warnings = self._harmonize_yaml_with_observed_schema(
                normalized_yaml,
                source_path=source_path,
                fmt=fmt,
            )

        payload = ContractGenerationResult(
            yaml_content=normalized_yaml,
            engine="datacontract-cli",
            success=result.returncode == 0,
            cli_available=True,
            command=command,
        )

        if normalize_warning:
            payload.warnings.append(normalize_warning)
        payload.warnings.extend(normalized_warnings)
        if stderr:
            payload.warnings.append(stderr)
        if result.returncode != 0:
            payload.errors.append(f"datacontract-cli exited with code {result.returncode}")
        return payload

    def _generate_fallback_contract(self, source_path: Path, dataset_name: str) -> ContractGenerationResult:
        """Deterministic fallback when CLI generation is unavailable or fails."""
        if not source_path.exists():
            yaml_content = yaml.safe_dump(
                {
                    "kind": "DataContract",
                    "apiVersion": "v3.1.0",
                    "id": f"urn:datacontract:{dataset_name}",
                    "table_name": dataset_name,
                    "columns": [],
                    "error": f"Source file not found: {source_path}",
                },
                sort_keys=False,
            )
            return ContractGenerationResult(
                yaml_content=yaml_content,
                engine="fallback",
                success=False,
                cli_available=False,
                errors=[f"Source file not found: {source_path}"],
            )

        try:
            if source_path.suffix.lower() == ".parquet":
                df = pd.read_parquet(source_path)
            elif source_path.suffix.lower() == ".json":
                df = pd.read_json(source_path)
            else:
                df = pd.read_csv(source_path)
        except Exception as exc:
            yaml_content = yaml.safe_dump(
                {
                    "kind": "DataContract",
                    "apiVersion": "v3.1.0",
                    "id": f"urn:datacontract:{dataset_name}",
                    "table_name": dataset_name,
                    "columns": [],
                    "error": f"Fallback inference failed: {exc}",
                },
                sort_keys=False,
            )
            return ContractGenerationResult(
                yaml_content=yaml_content,
                engine="fallback",
                success=False,
                cli_available=False,
                errors=[f"Fallback inference failed: {exc}"],
            )

        observed_schema, observed_warnings = self._collect_observed_schema(source_path, inferred_format=source_path.suffix.lower().lstrip("."))
        columns = []
        parquet_type_hints: Dict[str, str] = {}
        parquet_raw_types: Dict[str, str] = {}
        parquet_metadata_source: Optional[str] = None
        metadata_warnings: List[str] = []

        if source_path.suffix.lower() == ".parquet":
            (
                parquet_type_hints,
                parquet_raw_types,
                parquet_metadata_source,
                metadata_warnings,
            ) = self._infer_parquet_types(source_path)

        for col_name in df.columns:
            dtype = str(df[col_name].dtype)
            observed = observed_schema.get(col_name, {})
            col_type = (
                str(observed.get("contract_type") or "").strip()
                or parquet_type_hints.get(col_name)
                or self._map_pandas_type(dtype)
            )
            null_rate = float(observed.get("null_rate", 0.0) or 0.0)
            if col_name in parquet_type_hints and parquet_metadata_source:
                inferred_desc = (
                    f"Inferred from Parquet metadata ({parquet_metadata_source}): "
                    f"{parquet_raw_types.get(col_name, 'unknown')}"
                )
            elif observed.get("raw_type"):
                inferred_desc = f"Inferred from source schema (duckdb): {observed.get('raw_type')}"
            else:
                inferred_desc = f"Inferred from source column type: {dtype}"

            col_def: Dict[str, Any] = {
                "name": col_name,
                "data_type": col_type,
                # Default to nullable=true unless strict mode is explicitly enabled.
                "nullable": False if self.strict_nullable and null_rate <= 0.0 else True,
                "description": inferred_desc,
            }
            if self.include_hard_bounds and col_type in {"integer", "double"}:
                numeric = pd.to_numeric(df[col_name], errors="coerce").dropna()
                if not numeric.empty:
                    col_def["min_value"] = float(numeric.min())
                    col_def["max_value"] = float(numeric.max())
            columns.append(col_def)

        contract = {
            "kind": "DataContract",
            "apiVersion": "v3.1.0",
            "id": f"urn:datacontract:{dataset_name}",
            "table_name": dataset_name,
            "description": f"Deterministic fallback contract generated from {source_path.name}",
            "info": {
                "title": dataset_name,
                "owner": "Unassigned",
                "domain": "Unknown",
                "lifecycle": "active",
                "version": "1.0.0",
            },
            "quality": {
                "min_rows": 1,
                "anomaly_thresholds": dict(self.DEFAULT_ANOMALY_THRESHOLDS),
                "slos": dict(self.DEFAULT_SLOS),
            },
            "columns": columns,
        }
        yaml_content = yaml.safe_dump(contract, sort_keys=False)
        warnings = ["Generated via deterministic fallback."]
        warnings.extend(observed_warnings)
        if not self.include_hard_bounds:
            warnings.append(
                "No hard min/max constraints were inferred automatically. Add domain constraints manually where needed."
            )
        warnings.append(
            "Semantic rules (for example, valid age range) are intentionally not auto-generated to avoid domain hallucinations."
        )
        if source_path.suffix.lower() == ".parquet":
            if parquet_metadata_source:
                warnings.append(
                    f"Parquet types inferred via {parquet_metadata_source} metadata."
                )
            else:
                warnings.append(
                    "Parquet metadata inference unavailable; used pandas dtype fallback."
                )
        warnings.extend(metadata_warnings)

        return ContractGenerationResult(
            yaml_content=yaml_content,
            engine="fallback",
            success=True,
            cli_available=False,
            warnings=warnings,
        )

    def _collect_observed_schema(
        self,
        source_path: Path,
        *,
        inferred_format: str,
    ) -> tuple[Dict[str, Dict[str, Any]], List[str]]:
        """
        Collect observed type/nullability hints from the source data.
        Prefers DuckDB type introspection so generated contracts align with schema validation.
        """
        observed: Dict[str, Dict[str, Any]] = {}
        warnings: List[str] = []

        # Type hints via DuckDB DESCRIBE (same engine used by SchemaValidator).
        try:
            import duckdb

            conn = duckdb.connect(":memory:")
            if inferred_format == "parquet":
                rows = conn.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(source_path)]).fetchall()
            elif inferred_format == "json":
                rows = conn.execute("DESCRIBE SELECT * FROM read_json_auto(?)", [str(source_path)]).fetchall()
            else:
                rows = conn.execute("DESCRIBE SELECT * FROM read_csv_auto(?)", [str(source_path)]).fetchall()
            conn.close()

            for row in rows:
                col_name = str(row[0])
                raw_type = str(row[1])
                observed[col_name] = {
                    "raw_type": raw_type,
                    "contract_type": self._map_duckdb_type(raw_type),
                }
        except Exception as exc:
            warnings.append(f"DuckDB type introspection unavailable: {exc}")

        # Null-rate hints via Pandas for generic nullable defaults.
        try:
            if inferred_format == "parquet":
                df = pd.read_parquet(source_path)
            elif inferred_format == "json":
                df = pd.read_json(source_path)
            else:
                df = pd.read_csv(source_path)

            row_count = max(len(df), 1)
            for col_name in df.columns:
                series = df[col_name]
                if pd.api.types.is_string_dtype(series) or series.dtype == "object":
                    as_text = series.astype("string")
                    null_mask = as_text.isna() | (as_text.str.strip() == "")
                    null_rate = float(null_mask.sum()) / float(row_count)
                else:
                    null_rate = float(series.isna().sum()) / float(row_count)

                entry = observed.setdefault(str(col_name), {})
                entry["null_rate"] = round(float(null_rate), 6)
        except Exception as exc:
            warnings.append(f"Pandas nullability introspection unavailable: {exc}")

        return observed, warnings

    def _harmonize_yaml_with_observed_schema(
        self,
        yaml_content: str,
        *,
        source_path: Path,
        fmt: str,
    ) -> tuple[str, List[str]]:
        """
        Align generated YAML types/nullability with observed source schema.
        This prevents immediate false BLOCKED runs caused by generator/validator type drift.
        """
        parsed = yaml.safe_load(yaml_content) or {}
        if not isinstance(parsed, dict):
            return yaml_content, []

        observed, warnings = self._collect_observed_schema(source_path, inferred_format=fmt)
        columns = parsed.get("columns")
        if not isinstance(columns, list):
            return yaml_content, warnings

        changed_types: List[str] = []
        for col in columns:
            if not isinstance(col, dict):
                continue
            name = str(col.get("name") or "").strip()
            if not name:
                continue
            hint = observed.get(name) or {}
            observed_type = str(hint.get("contract_type") or "").strip()
            if observed_type and observed_type != str(col.get("data_type") or "").strip():
                col["data_type"] = observed_type
                changed_types.append(name)

            null_rate = float(hint.get("null_rate", 0.0) or 0.0)
            col["nullable"] = False if self.strict_nullable and null_rate <= 0.0 else True

            # Drop automatically inferred hard bounds unless explicitly requested.
            if not self.include_hard_bounds:
                col.pop("min_value", None)
                col.pop("max_value", None)

        self._ensure_quality_defaults(parsed)
        if changed_types:
            warnings.append(
                "Aligned generated column types with observed source schema for: "
                + ", ".join(changed_types[:12])
            )
        return yaml.safe_dump(parsed, sort_keys=False), warnings

    def _ensure_quality_defaults(self, contract: Dict[str, Any]) -> None:
        quality = contract.get("quality")
        if not isinstance(quality, dict):
            quality = {}
            contract["quality"] = quality
        quality.setdefault("min_rows", 1)

        anomaly = quality.get("anomaly_thresholds")
        if not isinstance(anomaly, dict):
            anomaly = {}
            quality["anomaly_thresholds"] = anomaly
        for key, value in self.DEFAULT_ANOMALY_THRESHOLDS.items():
            anomaly.setdefault(key, value)

        slos = quality.get("slos")
        if not isinstance(slos, dict):
            slos = {}
            quality["slos"] = slos
        for key, value in self.DEFAULT_SLOS.items():
            slos.setdefault(key, value)

    def _infer_parquet_types(
        self, source_path: Path
    ) -> tuple[Dict[str, str], Dict[str, str], Optional[str], List[str]]:
        """
        Infer Parquet column types using file metadata (pyarrow first, duckdb fallback).
        Returns:
            mapped_types: column -> contract type
            raw_types: column -> metadata type string
            source: "pyarrow" | "duckdb" | None
            warnings: non-fatal inference warnings
        """
        warnings: List[str] = []

        # 1) Preferred: pyarrow schema metadata
        try:
            import pyarrow.parquet as pq  # type: ignore

            parquet_file = pq.ParquetFile(source_path)
            schema = parquet_file.schema_arrow
            mapped_types: Dict[str, str] = {}
            raw_types: Dict[str, str] = {}
            for field in schema:
                raw = str(field.type)
                mapped_types[field.name] = self._map_arrow_type(raw)
                raw_types[field.name] = raw
            if mapped_types:
                return mapped_types, raw_types, "pyarrow", warnings
        except Exception as exc:
            warnings.append(f"pyarrow metadata inference failed: {exc}")

        # 2) Fallback: duckdb DESCRIBE read_parquet()
        try:
            import duckdb
            conn = duckdb.connect(":memory:")
            rows = conn.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)",
                [str(source_path)],
            ).fetchall()
            conn.close()

            mapped_types = {}
            raw_types = {}
            for row in rows:
                col_name = row[0]
                raw = str(row[1])
                mapped_types[col_name] = self._map_duckdb_type(raw)
                raw_types[col_name] = raw
            if mapped_types:
                return mapped_types, raw_types, "duckdb", warnings
        except Exception as exc:
            warnings.append(f"duckdb metadata inference failed: {exc}")

        return {}, {}, None, warnings

    @staticmethod
    def _map_pandas_type(dtype: str) -> str:
        value = dtype.lower()
        if value.startswith("datetime64") or value.startswith("timedelta64"):
            return "timestamp"
        if value.startswith("int") or value.startswith("uint"):
            return "integer"
        if value.startswith("float"):
            return "double"
        if value in {"bool", "boolean"}:
            return "boolean"
        return "varchar"

    @staticmethod
    def _map_arrow_type(arrow_type: str) -> str:
        value = arrow_type.lower()
        if "timestamp" in value:
            return "timestamp"
        if value.startswith("date"):
            return "date"
        if value.startswith("int") or value.startswith("uint"):
            return "integer"
        if value.startswith("float") or value.startswith("double") or value.startswith("decimal"):
            return "double"
        if "bool" in value:
            return "boolean"
        if "json" in value:
            return "json"
        if "binary" in value:
            return "blob"
        return "varchar"

    @staticmethod
    def _map_duckdb_type(duckdb_type: str) -> str:
        value = duckdb_type.upper().split("(")[0]
        if value in {
            "INTEGER",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
        }:
            return "integer"
        if value in {"FLOAT", "DOUBLE", "REAL", "DECIMAL"}:
            return "double"
        if value in {"BOOLEAN"}:
            return "boolean"
        if value in {"DATE"}:
            return "date"
        if value in {"TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP_NS", "TIMESTAMP_MS", "TIMESTAMP_S"}:
            return "timestamp"
        if value in {"JSON"}:
            return "json"
        if value in {"BLOB"}:
            return "blob"
        return "varchar"

    @staticmethod
    def _looks_like_yaml(content: str) -> bool:
        """Cheap YAML sanity check."""
        try:
            parsed = yaml.safe_load(content)
        except Exception:
            return False
        return isinstance(parsed, dict)

    def _normalize_cli_output(self, yaml_content: str, source_path: Path) -> tuple[str, str]:
        """
        Normalize datacontract-cli ODCS output into this platform's expected
        root-level `columns` format.
        """
        parsed = yaml.safe_load(yaml_content)
        if not isinstance(parsed, dict):
            return yaml_content, ""

        if "columns" in parsed:
            return yaml_content, ""

        schema_items = parsed.get("schema")
        if not isinstance(schema_items, list) or not schema_items:
            return yaml_content, ""

        model = schema_items[0] or {}
        props = model.get("properties", []) or []
        columns: List[Dict[str, Any]] = []
        for prop in props:
            if not isinstance(prop, dict):
                continue
            name = prop.get("name")
            if not name:
                continue

            data_type = self._map_contract_type(prop)
            column: Dict[str, Any] = {
                "name": name,
                "data_type": data_type,
                "nullable": not bool(prop.get("required", False)),
                "description": prop.get("description") or f"Imported from {source_path.name}",
            }

            logical_opts = prop.get("logicalTypeOptions") or {}
            if isinstance(logical_opts, dict):
                if "minimum" in logical_opts:
                    column["min_value"] = logical_opts["minimum"]
                if "maximum" in logical_opts:
                    column["max_value"] = logical_opts["maximum"]
                if isinstance(logical_opts.get("enum"), list):
                    column["allowed_values"] = logical_opts["enum"]

            # Only keep explicit PK markers from the source model.
            # Do not infer PK from *_id/unique heuristics; that creates false positives.
            explicit_pk = (
                bool(prop.get("primaryKey"))
                or bool(prop.get("primary_key"))
                or str(prop.get("logicalType", "")).lower() == "primary_key"
            )
            if explicit_pk:
                column["isPrimaryKey"] = True

            columns.append(column)

        normalized = {
            "kind": parsed.get("kind", "DataContract"),
            "apiVersion": parsed.get("apiVersion", "v3.1.0"),
            "id": parsed.get("id", f"urn:datacontract:{source_path.stem}"),
            "info": {
                "title": parsed.get("name", source_path.stem),
                "version": parsed.get("version", "1.0.0"),
                "description": model.get("description")
                or f"Imported from {source_path.name} via datacontract-cli",
                "owner": "Unknown",
                "domain": "Unknown",
            },
            "table_name": model.get("physicalName") or model.get("name") or source_path.stem,
            "description": model.get("description") or f"Generated from {source_path.name}",
            "quality": {
                "min_rows": 1,
            },
            "columns": columns,
        }

        return (
            yaml.safe_dump(normalized, sort_keys=False),
            "Normalized datacontract-cli ODCS output to platform `columns` schema.",
        )

    @staticmethod
    def _map_contract_type(prop: Dict[str, Any]) -> str:
        """Map ODCS/imported types to this platform's internal data_type values."""
        logical = str(prop.get("logicalType", "")).lower()
        physical = str(prop.get("physicalType", "")).lower()
        source = logical or physical

        if source in {"integer", "int", "int32", "int64", "long"}:
            return "integer"
        if source in {"number", "float", "double", "decimal"}:
            return "double"
        if source in {"boolean", "bool"}:
            return "boolean"
        if source in {"date", "datetime", "timestamp"}:
            return "timestamp"
        return "varchar"

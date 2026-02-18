from __future__ import annotations

import io
import os
from pathlib import Path
from typing import Dict, List, Sequence

import pandas as pd

from .base import ConnectorDataset

try:
    import boto3
    from botocore.client import Config as BotoConfig
except Exception:  # pragma: no cover
    boto3 = None
    BotoConfig = None


class S3Connector:
    """
    Read-only S3 connector for object discovery and sampled reads.

    Designed for lake-style ingestion zones where raw files land in S3 before
    downstream processing.
    """

    name = "s3"

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "",
        region: str = "us-east-1",
        max_discovered: int = 200,
        extensions: Sequence[str] = ("csv", "json", "parquet"),
        endpoint_url: str | None = None,
        force_path_style: bool = False,
    ):
        if boto3 is None:  # pragma: no cover
            raise RuntimeError("boto3 is required for S3Connector")

        normalized_bucket = str(bucket or "").strip()
        if not normalized_bucket:
            raise RuntimeError("DRE_CONNECTOR_S3_BUCKET is required")

        self.bucket = normalized_bucket
        self.prefix = str(prefix or "").lstrip("/")
        self.region = str(region or "us-east-1").strip() or "us-east-1"
        self.max_discovered = max(1, int(max_discovered))
        self.endpoint_url = str(endpoint_url or "").strip() or None
        self.force_path_style = bool(force_path_style)
        self.extensions = tuple(sorted({str(ext).strip().lower().lstrip(".") for ext in extensions if str(ext).strip()}))
        if not self.extensions:
            self.extensions = ("csv", "json", "parquet")

        session = boto3.session.Session(region_name=self.region)
        client_kwargs: Dict[str, object] = {}
        if self.endpoint_url:
            client_kwargs["endpoint_url"] = self.endpoint_url
        if self.force_path_style and BotoConfig is not None:
            client_kwargs["config"] = BotoConfig(s3={"addressing_style": "path"})
        self.client = session.client("s3", **client_kwargs)

    @classmethod
    def from_env(cls) -> "S3Connector":
        ext = [
            item.strip()
            for item in os.getenv("DRE_CONNECTOR_S3_EXTENSIONS", "csv,json,parquet").split(",")
            if item.strip()
        ]
        return cls(
            bucket=os.getenv("DRE_CONNECTOR_S3_BUCKET", ""),
            prefix=os.getenv("DRE_CONNECTOR_S3_PREFIX", ""),
            region=os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1")),
            max_discovered=int(os.getenv("DRE_CONNECTOR_S3_MAX_OBJECTS", "200")),
            extensions=ext,
            endpoint_url=os.getenv("DRE_CONNECTOR_S3_ENDPOINT_URL"),
            force_path_style=os.getenv("DRE_CONNECTOR_S3_FORCE_PATH_STYLE", "0").strip() in {"1", "true", "yes"},
        )

    def _matches_extension(self, key: str) -> bool:
        suffix = Path(key).suffix.lower().lstrip(".")
        return bool(suffix) and suffix in self.extensions

    @staticmethod
    def _dataset_name_for_key(key: str) -> str:
        stem = Path(key).stem
        return stem or key.replace("/", "_")

    def discover(self) -> List[ConnectorDataset]:
        paginator = self.client.get_paginator("list_objects_v2")
        params: Dict[str, object] = {"Bucket": self.bucket}
        if self.prefix:
            params["Prefix"] = self.prefix

        discovered: List[ConnectorDataset] = []
        for page in paginator.paginate(**params):
            for row in page.get("Contents", []) or []:
                key = str(row.get("Key") or "")
                if not key or key.endswith("/"):
                    continue
                if ".verdict." in key:
                    continue
                if not self._matches_extension(key):
                    continue

                discovered.append(
                    ConnectorDataset(
                        name=self._dataset_name_for_key(key),
                        location=f"s3://{self.bucket}/{key}",
                        format=Path(key).suffix.lower().lstrip("."),
                        metadata={
                            "connector": self.name,
                            "bucket": self.bucket,
                            "key": key,
                            "size_bytes": int(row.get("Size") or 0),
                            "last_modified": str(row.get("LastModified") or ""),
                            "region": self.region,
                        },
                    )
                )
                if len(discovered) >= self.max_discovered:
                    return discovered

        return discovered

    def read_sample(self, dataset: ConnectorDataset, limit: int = 100) -> List[Dict[str, object]]:
        safe_limit = max(1, min(int(limit), 10000))
        metadata = dataset.metadata or {}
        key = str(metadata.get("key") or "").strip()
        if not key:
            location = str(dataset.location or "")
            prefix = f"s3://{self.bucket}/"
            if location.startswith(prefix):
                key = location[len(prefix):]
            else:
                key = location
        if not key:
            raise RuntimeError("S3 dataset key could not be resolved")

        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        payload = obj["Body"].read()
        data_format = str(dataset.format or Path(key).suffix.lower().lstrip(".")).lower()

        if data_format == "parquet":
            df = pd.read_parquet(io.BytesIO(payload)).head(safe_limit)
        elif data_format == "json":
            try:
                df = pd.read_json(io.BytesIO(payload)).head(safe_limit)
            except ValueError:
                df = pd.read_json(io.BytesIO(payload), lines=True).head(safe_limit)
        else:
            text = payload.decode("utf-8")
            df = pd.read_csv(io.StringIO(text), nrows=safe_limit)

        return df.to_dict(orient="records")

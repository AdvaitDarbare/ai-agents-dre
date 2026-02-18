from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import pandas as pd

from .base import ConnectorDataset


class LocalFilesConnector:
    name = "local_files"

    def __init__(self, root: str = "data"):
        self.root = Path(root)

    def discover(self) -> List[ConnectorDataset]:
        results: List[ConnectorDataset] = []
        if not self.root.exists():
            return results
        for path in sorted(self.root.glob("*")):
            if not path.is_file():
                continue
            if path.suffix.lower() not in {".csv", ".parquet", ".json"}:
                continue
            if ".verdict." in path.name:
                continue
            results.append(
                ConnectorDataset(
                    name=path.stem,
                    location=str(path),
                    format=path.suffix.lstrip(".").lower(),
                    metadata={},
                )
            )
        return results

    def read_sample(self, dataset: ConnectorDataset, limit: int = 100) -> List[Dict[str, object]]:
        path = Path(dataset.location)
        if dataset.format == "parquet":
            df = pd.read_parquet(path).head(limit)
        elif dataset.format == "json":
            df = pd.read_json(path).head(limit)
        else:
            df = pd.read_csv(path, nrows=limit)
        return df.to_dict(orient="records")

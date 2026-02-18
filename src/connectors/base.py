from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Protocol


@dataclass
class ConnectorDataset:
    name: str
    location: str
    format: str
    metadata: Dict[str, Any]


class Connector(Protocol):
    """
    Connector interface for warehouse/cloud integrations.
    """

    name: str

    def discover(self) -> List[ConnectorDataset]:
        ...

    def read_sample(self, dataset: ConnectorDataset, limit: int = 100) -> List[Dict[str, Any]]:
        ...

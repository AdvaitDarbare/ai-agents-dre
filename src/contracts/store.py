"""
Contract store abstraction.

The application currently uses a filesystem-backed store, but the interface is
designed so we can later swap in Git-backed or remote runtime stores.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import List, Optional, Protocol, runtime_checkable
import re


@dataclass(frozen=True)
class ContractDocument:
    dataset_name: str
    content: str
    location: str
    version: Optional[str] = None
    source: str = "file"


@runtime_checkable
class ContractStore(Protocol):
    def exists(self, dataset_name: str) -> bool:
        ...

    def path_for(self, dataset_name: str) -> Path:
        ...

    def read(self, dataset_name: str) -> Optional[ContractDocument]:
        ...

    def write(self, dataset_name: str, content: str) -> ContractDocument:
        ...

    def list_paths(self) -> List[Path]:
        ...


class FileContractStore:
    """
    Filesystem implementation of ContractStore.
    """

    _DATASET_RE = re.compile(r"^[A-Za-z0-9_.-]+$")

    def __init__(self, root_path: str = "config/expectations"):
        self.root_path = Path(root_path)
        self.root_path.mkdir(parents=True, exist_ok=True)

    def _validate_dataset_name(self, dataset_name: str) -> str:
        name = dataset_name.strip()
        if not name:
            raise ValueError("dataset_name is required")
        if not self._DATASET_RE.match(name):
            raise ValueError(f"Invalid dataset_name: {dataset_name}")
        return name

    def path_for(self, dataset_name: str) -> Path:
        safe_name = self._validate_dataset_name(dataset_name)
        return self.root_path / f"{safe_name}.yaml"

    def exists(self, dataset_name: str) -> bool:
        return self.path_for(dataset_name).exists()

    def read(self, dataset_name: str) -> Optional[ContractDocument]:
        path = self.path_for(dataset_name)
        if not path.exists():
            return None
        content = path.read_text()
        return ContractDocument(
            dataset_name=dataset_name,
            content=content,
            location=str(path),
            version=sha256(content.encode("utf-8")).hexdigest(),
            source="file",
        )

    def write(self, dataset_name: str, content: str) -> ContractDocument:
        path = self.path_for(dataset_name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        return ContractDocument(
            dataset_name=dataset_name,
            content=content,
            location=str(path),
            version=sha256(content.encode("utf-8")).hexdigest(),
            source="file",
        )

    def delete(self, dataset_name: str) -> bool:
        path = self.path_for(dataset_name)
        if not path.exists():
            return False
        path.unlink()
        return True

    def list_paths(self) -> List[Path]:
        return sorted(self.root_path.glob("*.yaml"))

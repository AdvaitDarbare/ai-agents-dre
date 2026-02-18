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
import os
import re
import subprocess


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


class GitContractStore(FileContractStore):
    """
    Git-backed contract store.

    Contracts are still normal files on disk, but writes can be auto-committed
    to a git repository for versioned contract governance.
    """

    def __init__(self, root_path: str = "config/expectations", repo_root: Optional[str] = None):
        super().__init__(root_path=root_path)
        self.repo_root = Path(repo_root or os.getenv("CONTRACT_STORE_GIT_ROOT", ".")).resolve()
        self.auto_commit = os.getenv("CONTRACT_STORE_GIT_AUTO_COMMIT", "0").strip() == "1"
        self.committer = os.getenv("CONTRACT_STORE_GIT_COMMITTER", "dre-bot")

    def _run_git(self, *args: str) -> bool:
        try:
            subprocess.run(
                ["git", *args],
                cwd=str(self.repo_root),
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return True
        except Exception:
            return False

    def _relative_contract_path(self, path: Path) -> Optional[str]:
        try:
            return str(path.resolve().relative_to(self.repo_root))
        except Exception:
            return None

    def write(self, dataset_name: str, content: str) -> ContractDocument:
        doc = super().write(dataset_name, content)
        rel_path = self._relative_contract_path(Path(doc.location))
        if not rel_path:
            return doc

        self._run_git("add", rel_path)

        if self.auto_commit:
            message = f"contracts: update {dataset_name}"
            self._run_git(
                "-c",
                f"user.name={self.committer}",
                "-c",
                f"user.email={self.committer}@local",
                "commit",
                "-m",
                message,
                "--",
                rel_path,
            )
        return doc


def build_contract_store(root_path: str = "config/expectations"):
    """
    Factory to choose contract backend by env:
    - CONTRACT_STORE_BACKEND=file (default)
    - CONTRACT_STORE_BACKEND=git
    """
    backend = os.getenv("CONTRACT_STORE_BACKEND", "file").strip().lower()
    if backend == "git":
        return GitContractStore(root_path=root_path)
    return FileContractStore(root_path=root_path)

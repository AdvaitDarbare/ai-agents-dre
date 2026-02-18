from pathlib import Path
import subprocess

from src.contracts.store import GitContractStore


def test_git_contract_store_write_and_read(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init"], cwd=repo, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    monkeypatch.setenv("CONTRACT_STORE_GIT_AUTO_COMMIT", "0")
    store = GitContractStore(root_path=str(repo / "config/expectations"), repo_root=str(repo))
    doc = store.write("orders", "kind: DataContract\nid: orders\n")

    assert Path(doc.location).exists()
    loaded = store.read("orders")
    assert loaded is not None
    assert "DataContract" in loaded.content

    paths = store.list_paths()
    assert any(path.name == "orders.yaml" for path in paths)

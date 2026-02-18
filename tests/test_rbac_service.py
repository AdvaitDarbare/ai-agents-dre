import pytest
from fastapi import HTTPException

from src.services.rbac_service import RBACService


def test_rbac_can_for_admin_permissions():
    service = RBACService()
    assert service.can("admin", "dataset.delete") is True
    assert service.can("admin", "contract.save") is True


def test_rbac_denies_viewer_for_sensitive_action():
    service = RBACService()
    assert service.can("viewer", "dataset.delete") is False


def test_rbac_enforce_when_enabled(monkeypatch):
    monkeypatch.setenv("DRE_RBAC_ENABLED", "1")
    service = RBACService()
    with pytest.raises(HTTPException) as exc:
        service.enforce("viewer", "jobs.delete")
    assert exc.value.status_code == 403

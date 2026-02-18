from __future__ import annotations

import os
from typing import Dict, Set

from fastapi import HTTPException


class RBACService:
    """
    Lightweight role-based access checks for sensitive operator actions.

    Roles (ascending privilege):
    viewer < operator < admin
    """

    _PERMISSIONS: Dict[str, Set[str]] = {
        "viewer": {
            "read",
        },
        "operator": {
            "read",
            "evaluate",
            "contract.propose",
            "contract.approve",
            "contract.reject",
            "incident.update",
            "governance.rollback",
            "jobs.evaluate",
            "jobs.evaluate_all",
            "jobs.remediation_apply",
        },
        "admin": {
            "read",
            "evaluate",
            "contract.propose",
            "contract.approve",
            "contract.reject",
            "contract.save",
            "incident.update",
            "governance.rollback",
            "dataset.delete",
            "platform.reset",
            "jobs.evaluate",
            "jobs.evaluate_all",
            "jobs.delete",
            "jobs.bulk_delete",
            "jobs.remediation_apply",
        },
    }

    def __init__(self):
        self.enabled = os.getenv("DRE_RBAC_ENABLED", "0").strip() == "1"
        self.default_role = os.getenv("DRE_RBAC_DEFAULT_ROLE", "viewer").strip().lower() or "viewer"

    def normalize_role(self, role: str | None) -> str:
        value = (role or self.default_role or "viewer").strip().lower()
        return value if value in self._PERMISSIONS else self.default_role

    def can(self, role: str | None, permission: str) -> bool:
        normalized = self.normalize_role(role)
        return permission in self._PERMISSIONS.get(normalized, set())

    def enforce(self, role: str | None, permission: str) -> None:
        if not self.enabled:
            return
        if self.can(role, permission):
            return
        normalized = self.normalize_role(role)
        raise HTTPException(
            status_code=403,
            detail=f"Role '{normalized}' is not allowed to perform '{permission}'",
        )

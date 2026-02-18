from __future__ import annotations

import os
from typing import List

from .base import Connector
from .local_files import LocalFilesConnector
from .postgres import PostgresConnector
from .s3 import S3Connector


def build_connectors() -> List[Connector]:
    """
    Connector strategy entrypoint.
    Local-files remains default. Additional connectors can be enabled via env.
    """
    connectors: List[Connector] = []

    enable_local = os.getenv("DRE_CONNECTOR_LOCAL_FILES", "1").strip() != "0"
    if enable_local:
        connectors.append(LocalFilesConnector(root=os.getenv("DRE_CONNECTOR_LOCAL_ROOT", "data")))

    enable_postgres = os.getenv("DRE_CONNECTOR_POSTGRES", "0").strip() == "1"
    if enable_postgres:
        try:
            connectors.append(PostgresConnector.from_env())
        except Exception as exc:
            # Discovery remains available even if one connector cannot initialize.
            print(f"⚠️ Postgres connector disabled: {exc}")

    enable_s3 = os.getenv("DRE_CONNECTOR_S3", "0").strip() == "1"
    if enable_s3:
        try:
            connectors.append(S3Connector.from_env())
        except Exception as exc:
            # Discovery remains available even if one connector cannot initialize.
            print(f"⚠️ S3 connector disabled: {exc}")

    return connectors

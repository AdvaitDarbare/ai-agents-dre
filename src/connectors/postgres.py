from __future__ import annotations

import os
from typing import Dict, List, Sequence, Tuple

from .base import ConnectorDataset

try:
    import psycopg2
    from psycopg2 import sql
except Exception:  # pragma: no cover
    psycopg2 = None
    sql = None


class PostgresConnector:
    """
    Read-only PostgreSQL connector for table discovery and sampled reads.
    """

    name = "postgres"

    def __init__(
        self,
        *,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        schemas: Sequence[str],
        connect_timeout: int = 5,
        sslmode: str = "prefer",
    ):
        if psycopg2 is None:  # pragma: no cover
            raise RuntimeError("psycopg2 is required for PostgresConnector")

        self.host = host
        self.port = int(port)
        self.database = database
        self.user = user
        self.password = password
        self.schemas = [s.strip() for s in schemas if str(s).strip()] or ["public"]
        self.connect_timeout = max(1, int(connect_timeout))
        self.sslmode = sslmode

    @classmethod
    def from_env(cls) -> "PostgresConnector":
        schemas = [
            item.strip()
            for item in os.getenv("DRE_CONNECTOR_POSTGRES_SCHEMAS", "public").split(",")
            if item.strip()
        ]
        return cls(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "dre"),
            user=os.getenv("POSTGRES_USER", "dre_user"),
            password=os.getenv("POSTGRES_PASSWORD", "dre_password"),
            schemas=schemas,
            connect_timeout=int(os.getenv("DRE_CONNECTOR_POSTGRES_CONNECT_TIMEOUT", "5")),
            sslmode=os.getenv("POSTGRES_SSLMODE", "prefer"),
        )

    def _connect(self):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
            connect_timeout=self.connect_timeout,
            sslmode=self.sslmode,
        )
        conn.set_session(readonly=True, autocommit=True)
        return conn

    def discover(self) -> List[ConnectorDataset]:
        rows: List[Tuple[str, str]] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_type = 'BASE TABLE'
                      AND table_schema = ANY(%s)
                    ORDER BY table_schema, table_name
                    """,
                    (self.schemas,),
                )
                rows = cur.fetchall() or []

        datasets: List[ConnectorDataset] = []
        for schema_name, table_name in rows:
            dataset_name = table_name if schema_name == "public" else f"{schema_name}.{table_name}"
            datasets.append(
                ConnectorDataset(
                    name=dataset_name,
                    location=f"{schema_name}.{table_name}",
                    format="postgres_table",
                    metadata={
                        "connector": self.name,
                        "schema": schema_name,
                        "table": table_name,
                        "database": self.database,
                        "host": self.host,
                        "port": self.port,
                    },
                )
            )

        return datasets

    def read_sample(self, dataset: ConnectorDataset, limit: int = 100) -> List[Dict[str, object]]:
        safe_limit = max(1, min(int(limit), 10000))
        metadata = dataset.metadata or {}
        schema_name = str(metadata.get("schema") or "").strip()
        table_name = str(metadata.get("table") or "").strip()

        if not schema_name or not table_name:
            location = str(dataset.location or "").strip()
            if "." in location:
                schema_name, table_name = location.split(".", 1)
            else:
                schema_name = "public"
                table_name = location or dataset.name

        query = sql.SQL("SELECT * FROM {}.{} LIMIT %s").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (safe_limit,))
                rows = cur.fetchall() or []
                columns = [desc[0] for desc in (cur.description or [])]

        return [dict(zip(columns, row)) for row in rows]


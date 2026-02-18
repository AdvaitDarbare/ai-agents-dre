# Connector Strategy

Last updated: 2026-02-17

## Goal

Provide a clean connector abstraction for warehouse/cloud integrations while keeping local-first behavior as default.

## Current Implementation

- Connector interface: `src/connectors/base.py`
- Default connector registry: `src/connectors/registry.py`
- Local files connector: `src/connectors/local_files.py`
- PostgreSQL connector: `src/connectors/postgres.py`
- S3 connector (MinIO-compatible): `src/connectors/s3.py`

`build_connectors()` is env-driven:

- local files enabled by default (`DRE_CONNECTOR_LOCAL_FILES=1`)
- postgres optional (`DRE_CONNECTOR_POSTGRES=1`)
- s3 optional (`DRE_CONNECTOR_S3=1`)

When enabled, Postgres discovery is read-only via `information_schema`.
When enabled, S3 discovery is read-only via `list_objects_v2` and sampled object reads.

## Current Connector Targets (Open-Source-First)

1. Postgres tables (source-of-truth operational data)
2. S3/MinIO landing zone objects (raw ingest files)
3. Local files (developer/test loop)

Warehouse load target remains Doris in Stage C (`src/pipeline/stages/action_stage.py`).

Each connector should support:

1. Dataset discovery
2. Read-only sampling for profiling
3. Metadata capture for ownership, freshness, and lineage enrichment

## Rollout Notes

- Keep write operations policy-gated and RBAC-protected.
- Prefer read-only first for safety.
- Reuse `ConnectorDataset` metadata to avoid provider-specific branching in the UI/service layer.

## Postgres MVP Env

```bash
export DRE_CONNECTOR_POSTGRES=1
export DRE_CONNECTOR_POSTGRES_SCHEMAS=public
export DRE_CONNECTOR_EVAL_SAMPLE_LIMIT=1000
```

Connection fields reuse existing backend Postgres env:

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- optional `POSTGRES_SSLMODE`

## S3 / MinIO Env

```bash
export DRE_CONNECTOR_S3=1
export DRE_CONNECTOR_S3_BUCKET=dre-landing
export DRE_CONNECTOR_S3_PREFIX=raw/
export DRE_CONNECTOR_S3_EXTENSIONS=csv,json,parquet
export DRE_CONNECTOR_S3_MAX_OBJECTS=200
export AWS_REGION=us-east-1
export DRE_CONNECTOR_S3_ENDPOINT_URL=http://localhost:9000
export DRE_CONNECTOR_S3_FORCE_PATH_STYLE=1
```

For MinIO, set endpoint/path-style flags and standard AWS credential env vars.

## Current Behavior

- `/datasets` now includes connector-backed datasets.
- `POST /evaluate/{dataset}` and `/jobs/evaluate-all` can evaluate connector-backed datasets.
- Connector-backed evaluation stages a sampled, read-only CSV under `data/staged_connector/` before running the existing deterministic pipeline.
- Stage C Doris load can be toggled with `DRE_DORIS_LOAD_ENABLED=0|1`.

# AGENTS.md

Instructions for coding agents working in this repository.

## Project Overview

Agentic Data Reliability Engineering (DRE) platform for contract-first data validation:

- Ingest files or connector-backed datasets
- Run deterministic reliability checks (schema, profiling, anomaly, impact, SLO)
- Persist run history/metrics/incidents in PostgreSQL
- Use HITL approval for contract lifecycle and policy-gated remediation
- Expose operations via FastAPI and MCP tools

## Setup Commands

- Start PostgreSQL:
  - `docker-compose up -d`
- Install Python deps:
  - `pip install -r requirements.txt`
- Start backend API:
  - `uvicorn src.api:app --reload --port 8000`
- Start Next.js frontend:
  - `cd web && npm install --legacy-peer-deps && npm run dev`
- Optional file watcher:
  - `python3 -m src.runners.file_watcher`
- Optional MCP server:
  - `python3 -m src.mcp.server --transport streamable-http --host 0.0.0.0 --port 8001 --path /mcp`

## Testing Instructions

- Run full tests:
  - `pytest tests -v`
- Run focused tests for changed areas first (examples):
  - `pytest -q tests/test_reliability_service_phase4.py`
  - `pytest -q tests/test_langgraph_evaluate_flow_nodes.py`
  - `pytest -q tests/test_async_jobs_api.py tests/test_parity_smoke_api.py`

## Code Style And Conventions

- Use PostgreSQL via `src.utils.database.get_connection()` connection pool.
- Use `%s` parameterized SQL with psycopg2; never use SQL f-strings.
- Keep DuckDB usage limited to in-memory DataFrame operations in:
  - `src/tools/data_profiler.py`
  - `src/tools/schema_validator.py`
- Preserve deterministic execution in the core reliability pipeline.
- Add tests for behavioral changes.

## Security And Safety

- Do not bypass policy/HITL gates for destructive or remediation actions.
- Keep connector access read-only by default.
- Avoid writing secrets or credentials into code or docs.
- For connector-backed datasets, require approved contracts before evaluation.

## Architecture Pointers

- Runtime architecture: `docs/architecture.md`
- API contract: `docs/api.md`
- Database schema: `docs/database.md`
- MCP tools: `docs/mcp.md`
- Connector strategy: `docs/connectors.md`
- Event-driven orchestration: `docs/event_driven_orchestration.md`
- Data lifecycle / watcher flow: `docs/file_watcher_guide.md`
- Coding patterns: `docs/patterns.md`
- Roadmap: `docs/plan.md`

## Workflow Guardrails

- Keep `README.md` human-focused; keep agent-operational detail here.
- Prefer low-risk, behavior-preserving refactors first.
- When changing APIs/services/docs, update related tests and docs in the same change.
- If both `web/` and `frontend/` are touched, preserve Next.js as the primary lane.

## Postgres Connector (Current MVP)

Enable connector discovery/evaluation:

- `export DRE_CONNECTOR_POSTGRES=1`
- `export DRE_CONNECTOR_POSTGRES_SCHEMAS=public`
- `export DRE_CONNECTOR_EVAL_SAMPLE_LIMIT=1000`

Connection settings reuse:

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- optional `POSTGRES_SSLMODE`

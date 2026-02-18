# Current State Snapshot (2026-02-18)

This file summarizes what is currently implemented in the DRE platform.

## What We Built

1. Contract-first reliability control plane
- YAML contracts define schema/value expectations per dataset.
- Contract-missing datasets enter pending approval with proposal generation.

2. Deterministic reliability pipeline
- Schema validation
- Data profiling
- Anomaly detection (z-score, robust z-score, IQR, EWMA fallback)
- Impact/criticality handling
- 6D quality scoring
- Verdict persistence and file actions
- Conservative contract generation with observed-schema harmonization (avoids arbitrary semantic bounds by default)

3. Agentic reliability loop (targeted, high-value)
- Investigation
- Root-cause hypothesis
- Remediation proposal
- Confidence + policy/HITL-gated execution
- AI brief endpoint for concise incident-style summaries (`GET /ai/brief/{dataset_name}`)

4. Durable workflow and governance
- LangGraph HITL checkpointing in Postgres
- Incident lifecycle (`OPEN`/`ACK`/`RESOLVED`)
- Action audit log
- Contract version history and rollback

5. Integrations and interfaces
- FastAPI backend
- MCP server with investigation-grade tools
- Next.js dashboard
- Connector strategy (local files default, Postgres + optional S3/MinIO)
- Postgres-backed async queue with in-process or external worker execution mode

## Architecture Model

1. Ingestion trigger
- Event-driven watcher (`src/runners/file_watcher.py`) and API-triggered jobs.

2. Workflow dispatch
- `HITLContractWorkflow.run_for_file()` routes:
  - configured datasets to evaluate graph
  - unconfigured datasets to proposal + HITL interrupt/resume

3. Reliability execution
- `MonitorAgent` executes deterministic stage pipeline.
- `ReliabilityService` provides orchestration APIs, incident logic, policy gates, and agentic loop.

4. Persistence
- PostgreSQL for run history, metrics, baselines, SLOs, incidents, async jobs, governance, and audit.

## Operational Process (How Teams Use It)

1. New dataset onboarding
- Data lands.
- No contract -> pending queue + proposal.
- Human approves.
- Dataset moves to active monitoring.

2. Continuous checks
- File-based event-driven validation.
- Connector-backed scheduled evaluation.

3. Incident response
- Investigate anomaly.
- Assess lineage impact.
- Apply policy-gated remediation.

4. Governance
- Track contract versions.
- Roll back safely when needed.

## Tooling Surface

1. API
- Core dataset/scan/history/incidents/contracts/governance/metrics/SLO endpoints.
- Integrations/status endpoints:
  - `GET /integrations/sources`
  - `GET /platform/config`
  - `GET /risk/datasets`
- Lineage endpoint with summary and validation issues:
  - `GET /lineage`

2. MCP tools (selected)
- `compare_runs`
- `investigate_anomaly`
- `explain_quality`
- `generate_ai_brief`
- `list_datasets_by_risk`
- `get_workflow_timeline`
- `run_agentic_reliability_loop`

SLO summary now includes:
- pass/fail rate
- failing SLO count and names
- recent fail streaks per SLO
- average/total error-budget burn

3. Demo data kit
- Mixed-format dummy seed script:
  - `python3 scripts/seed_dummy_datasets.py --cleanup-legacy`
- Produces CSV/JSON/Parquet demo datasets under `data/test`.

## Validation Status

Comprehensive checks executed on 2026-02-18:

1. `pytest -q` -> `158 passed`
2. `cd web && npm run lint` -> pass (TypeScript check)
3. `cd web && npm run build` -> pass

Notes:
- Build passes; Next.js emits a known informational warning from `baseline-browser-mapping` data freshness.

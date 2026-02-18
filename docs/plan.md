# Plan — Roadmap & Tech Debt

Last updated: 2026-02-17

## Recently Completed

### Contract Lifecycle + HITL
- [x] File watcher flow for landing zone ingestion
- [x] Pending approval queue + proposal generation
- [x] Contract approve/reject endpoints
- [x] Contract store abstraction (`src/contracts/store.py`)

### Reliability Engine Upgrades
- [x] Robust anomaly detection (z-score + robust z-score + IQR)
- [x] Enriched metric history metadata (`metric_group`, `column_name`, `segment`, `tags`)
- [x] SLO evaluation per run (availability, quality, anomaly count, freshness)
- [x] SLO persistence + summary APIs (`/slos/*`)
- [x] Lineage refresh + summary metadata and validation issues exposed from API (`/lineage`)
- [x] Agentic reliability loop exposed in MCP (`run_agentic_reliability_loop`) with confidence/policy gating

### UI Upgrades
- [x] Unified datasets workflow with active/unconfigured/pending states
- [x] Dataset delete action (with backend artifact cleanup)
- [x] SLO tab in expanded dataset detail (summary cards + tables)
- [x] Sidebar behavior/polish improvements
- [x] AI SDK-powered Copilot panel (`@ai-sdk/react` + `TextStreamChatTransport`)
- [x] FastAPI streaming chat endpoint for `useChat` (`POST /chat/stream`)
- [x] Frontend decomposition started: extracted `frontend/src/components/CopilotPanel.jsx`
- [x] Next.js migration lane bootstrapped from Vercel AI template (`web/`)
- [x] Next.js dashboard decomposed into feature modules (`web/dre/components/*`)
- [x] AI SDK Generative UI Copilot cards in Next lane (`/api/chat/ui` + `tool-*` rendering)
- [x] Next.js parity tabs added (Overview / History / Incidents / Lineage)
- [x] Next.js incident lifecycle controls (ACK / RESOLVE)
- [x] Next.js contract review queue actions (approve / reject with editable YAML)
- [x] Next.js selected-dataset SLO inspector panel
- [x] Next.js connections/settings switched from placeholders to runtime API-backed panels (`/integrations/sources`, `/platform/config`)
- [x] Lineage panel expanded with summary cards and unresolved upstream validation display

### Architecture Migration Progress
- [x] Phase 1: service layer extraction from `src/api.py` (`src/services/reliability_service.py`)
- [x] Phase 2: MCP server with core tools (`src/mcp/server.py`)
- [x] Phase 3: LangGraph durable HITL workflow for contract-missing path (`src/workflows/hitl_contract_workflow.py`)
- [x] Phase 4 (incremental): unified workflow dispatch for API + watcher evaluate paths (`HITLContractWorkflow.run_for_file`)
- [x] Phase 4 (full): LangGraph evaluate flow split into staged nodes (evaluate -> persist verdict -> apply file actions) for contract-configured datasets
- [x] Postgres-backed async jobs support external worker mode (`ASYNC_JOB_EXECUTION_MODE=external_worker`, `src/runners/async_job_worker.py`)
- [x] Frontend consolidation complete: Next.js (`web/`) promoted as primary lane; Vite (`frontend/`) archived

## Next Priorities

### P0 (Near-term)
- [x] Introduce async job execution for long-running actions (scan, delete) with `/jobs/*` status APIs
- [x] Add incident lifecycle model (OPEN/ACK/RESOLVED) + ownership (`/incidents` + `PATCH /incidents/{id}`)
- [x] Add policy engine for action safety gates (auto vs approval-required)
- [x] Fix chat API shape consistency (query param + JSON body support)
- [x] Add automated tests for `/slos/*` and enriched `/metrics/*/timeseries`
- [x] Add backend tests for `/chat/stream` AI SDK transport compatibility
- [x] Add LangGraph checkpoint recovery test coverage (restart/resume)
- [x] Extend async jobs to remediation + bulk delete with bounded worker controls

### P1
- [x] Continue extracting `frontend/src/App.jsx` into feature modules/components (legacy lane while Next migration is active) (legacy lane frozen for cutover; new work consolidated in Next modules under `web/dre/components/*`)
- [x] Optional AI Elements UI pass for Copilot panel (completed lightweight pass in Next lane while keeping existing copilot transport/tool rendering stable)
- [x] Port full contract governance version editor + AI modify workflow to Next lane (core editor + history + ai-modify)
- [x] Port deep profile modal and expanded dataset quality drilldowns to Next lane (tabs exist; charts still WIP)
- [x] Add structured action audit log for all operator/agent actions (audit table + API + job + operator hooks)
- [x] Add monitor backtesting harness (false positive / false negative tuning) (`src/tools/monitor_backtesting.py`, `/backtesting/{dataset_name}`, `scripts/run_backtesting.py`)
- [x] Improve baseline models for non-stationary metrics (EWMA trend-aware fallback in `src/tools/anomaly_detector.py`)
- [x] Phase 4: expand LangGraph coverage beyond contract-missing (full evaluate flow)
- [x] Mixed-format demo data seed flow (CSV/JSON/Parquet) + demo contracts/lineage (`scripts/seed_dummy_datasets.py`)

### P2
- [x] Git-backed contract store implementation (`GitContractStore` + `build_contract_store`, env-selectable backend)
- [x] Connector strategy for warehouse/cloud integrations (`src/connectors/*`, `docs/connectors.md`)
- [x] Role-based access control for governance/remediation/delete actions (`src/services/rbac_service.py`, header-based role checks in API)

## Next Steps: Next.js Cutover

- [x] Capture parity checklist vs `frontend/src/App.jsx` for dataset detail drilldowns, governance/remediation workflows, AI-assisted contract wizard, system health ribbon, and connections/settings views before retiring the Vite UI.
- [x] Centralize Next dashboard data orchestration into reusable hooks powering `web/dre/dashboard.tsx` so pulse, jobs, incidents, SLOs, lineage, and pending-contract feeds share refresh logic and error handling.
- [x] Port legacy modules (dataset detail tabs, governance/remediation panels, contract editor, data preview modal, deep profile modal) into the Next lane and wire each to matching FastAPI endpoints.
- [x] Harden backend surface consumed by the Next UI (policy-gated async job endpoints, `/slos/*`, `/metrics/*/timeseries`, `/chat/stream`, plus audit endpoints) so the UI has stable data.
- [x] Run parity smoke tests (pending contract approval, enqueueing scan/delete/bulk scan jobs, incident ack/resolve, rollback, and governance history updates) before updating docs/start commands to only mention `http://localhost:3000/`. (Automated in `tests/test_parity_smoke_api.py`)

### Parity Checklist (Required For 5173 Shutdown)

- [x] `REQUIRED`: Dataset detail drilldowns in Next include tabs for quality, anomalies, SLOs, governance, and lineage (legacy: expanded row + tabbed detail panes).
- [x] `REQUIRED`: Dataset preview table modal exists in Next using `GET /datasets/{dataset}/data` (legacy: DataPreviewModal).
- [x] `REQUIRED`: Deep profile modal exists in Next using `GET /profile/{dataset}` (legacy: ProfileModal).
- [x] `REQUIRED`: Governance history + rollback exists in Next:
  - `GET /governance/{dataset}/history`
  - `GET /governance/file/{filename}`
  - `POST /governance/rollback`
- [x] `REQUIRED`: Remediation panel exists in Next:
  - `GET /remediation/{dataset}`
  - `POST /jobs/remediation/apply` (preferred) or `POST /remediation/apply`
  - Handles policy `409` approval-required responses.
- [x] `REQUIRED`: Contract governance/version editor exists in Next:
  - `GET /contracts/{dataset}` and/or `GET /contract/{dataset}`
  - `POST /contracts/save` and/or `POST /contract/{dataset}`
  - `GET /contract-history/{dataset}` and `GET /contract/{dataset}/version/{id}`
  - `POST /contract/{dataset}/ai-modify`
- [x] `REQUIRED`: Contract wizard flow exists in Next for unmanaged/pending datasets:
  - Profile -> propose -> approve loop (`/profile`, `/contracts/propose`, `/contracts/approve`)
  - Integrates with pending queue view (`/contracts/pending`).
- [x] `REQUIRED`: System health + global stats exist in Next:
  - `GET /health/system`
  - `GET /stats/global`
- [x] `REQUIRED`: “Scan all” action exists in Next as async job(s) (avoid N parallel request fan-out from browser).
- [x] `NICE`: Connections + settings tabs (legacy-only today) reintroduced in Next as read-first panels.

### Parity Notes

- Charts/visualizations in dataset detail tabs are still in progress. Tabs exist, but some panels render raw JSON instead of legacy charts.

## Framework Migration Exploration

### Current
- Custom orchestrator (`MonitorAgent`) + Agno for LLM runtime

### Under Evaluation
- LangGraph for durable HITL action workflows
- OpenAI Agents SDK for chat/tool orchestration and tracing

### Decision Direction (current)
- Keep existing runtime for now
- Pilot LangGraph on one bounded flow: `propose -> approve -> execute -> verify`
- Optionally migrate chatbot/tool-calling path to OpenAI Agents SDK first

## Technical Debt

| Item | Location | Impact | Notes |
|---|---|---|---|
| Monolithic frontend shell | `frontend/src/App.jsx` | Maintainability | Large file with mixed concerns |
| Sync execution path | `src/api.py`, `src/agents/monitor_agent.py` | Reliability | Long tasks still in request path |
| Incident workflow UX depth | API + UI | Ops maturity | Backend lifecycle exists; continue improving UI/operator ergonomics |
| Incomplete connector abstraction | ingestion/runtime | Product expansion | Local-first currently |
| Partial docs drift risk | repo docs | Onboarding | Keep docs synced with rapid iteration |

# Plan — Roadmap & Tech Debt

Last updated: 2026-02-16

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

### UI Upgrades
- [x] Unified datasets workflow with active/unconfigured/pending states
- [x] Dataset delete action (with backend artifact cleanup)
- [x] SLO tab in expanded dataset detail (summary cards + tables)
- [x] Sidebar behavior/polish improvements
- [x] AI SDK-powered Copilot panel (`@ai-sdk/react` + `TextStreamChatTransport`)
- [x] FastAPI streaming chat endpoint for `useChat` (`POST /chat/stream`)
- [x] Frontend decomposition started: extracted `frontend/src/components/CopilotPanel.jsx`

### Architecture Migration Progress
- [x] Phase 1: service layer extraction from `src/api.py` (`src/services/reliability_service.py`)
- [x] Phase 2: MCP server with core tools (`src/mcp/server.py`)
- [x] Phase 3: LangGraph durable HITL workflow for contract-missing path (`src/workflows/hitl_contract_workflow.py`)

## Next Priorities

### P0 (Near-term)
- [ ] Introduce async job execution for long-running actions (scan, remediation, bulk delete)
- [ ] Add incident lifecycle model (OPEN/ACK/RESOLVED) + ownership
- [ ] Add policy engine for action safety gates (auto vs approval-required)
- [x] Fix chat API shape consistency (query param + JSON body support)
- [ ] Add automated tests for `/slos/*` and enriched `/metrics/*/timeseries`
- [ ] Add backend tests for `/chat/stream` AI SDK transport compatibility
- [ ] Add LangGraph checkpoint recovery test coverage (restart/resume)

### P1
- [ ] Continue extracting `frontend/src/App.jsx` into feature modules/components
- [ ] Optional AI Elements UI pass for Copilot panel
- [ ] Add structured action audit log for all operator/agent actions
- [ ] Add monitor backtesting harness (false positive / false negative tuning)
- [ ] Improve baseline models for non-stationary metrics
- [ ] Phase 4: expand LangGraph coverage beyond contract-missing (full evaluate flow)

### P2
- [ ] Git-backed contract store implementation
- [ ] Connector strategy for warehouse/cloud integrations
- [ ] Role-based access control for governance/remediation/delete actions

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
| Limited incident workflow | API + UI | Ops maturity | Lacks ack/ownership/state machine |
| Incomplete connector abstraction | ingestion/runtime | Product expansion | Local-first currently |
| Partial docs drift risk | repo docs | Onboarding | Keep docs synced with rapid iteration |

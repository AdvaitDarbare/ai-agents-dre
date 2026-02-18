# Resume Feature Base (Soda-Style Categories)

This project is positioned as an **agentic data reliability control plane** with deterministic quality gates and targeted agentic investigation/remediation.

## Core Categories To Showcase

1. Data quality checks + anomaly detection
- Implemented:
  - Contract-based schema/value checks
  - Statistical anomaly detection (z-score, robust z-score, IQR, EWMA fallback)
- Proof:
  - `POST /evaluate/{dataset_name}`
  - `src/tools/anomaly_detector.py`
  - `src/tools/schema_validator.py`

2. Data contracts
- Implemented:
  - YAML contracts, proposal generation, HITL approval/reject, version history, rollback
- Proof:
  - `GET /contracts/pending`
  - `POST /contracts/approve`
  - `GET /contract-history/{dataset_name}`

3. Observability + incident workflow
- Implemented:
  - Async job queue + status tracking
  - Incident lifecycle (`OPEN`/`ACK`/`RESOLVED`)
  - Audit log for operator/system actions
- Proof:
  - `GET /jobs`
  - `GET /incidents`
  - `PATCH /incidents/{incident_id}`
  - `GET /audit`

4. SLO management
- Implemented:
  - Per-run SLO checks
  - Summary with pass/fail rates, burn, failing SLOs, fail streaks
- Proof:
  - `GET /slos/{dataset_name}`
  - `GET /slos/{dataset_name}/summary`

5. Lineage + impact analysis
- Implemented:
  - Lineage graph with upstream/consumer impact
  - Validation issues for unresolved/invalid upstream refs
- Proof:
  - `GET /lineage`
  - `src/tools/impact_analyzer.py`

6. Risk prioritization
- Implemented:
  - Reliability risk ranking by quality, anomalies, criticality, and incident load
- Proof:
  - `GET /risk/datasets`
  - `src/services/reliability_service.py::list_datasets_by_risk`

7. Agentic operations surface
- Implemented:
  - MCP server tools
  - Agentic loop: investigate -> root-cause hypothesis -> remediation proposal -> confidence/policy gating
- Proof:
  - `src/mcp/server.py`
  - `run_agentic_reliability_loop`

## Resume-Friendly Framing

Use this positioning:

1. Built a contract-first, event-driven data reliability platform with policy/HITL-governed automation.
2. Combined deterministic pipeline controls with agentic investigation/remediation for safe autonomy.
3. Implemented end-to-end reliability operations: checks, SLOs, incidents, lineage, risk ranking, and MCP tool exposure.

## Honest Gaps (Good to Mention)

1. Lineage is config-driven (not warehouse query-log auto-discovery yet).
2. Connector set is Postgres + optional S3/MinIO + local files (expandable).
3. Alert routing integrations (PagerDuty/Slack/Jira) can be expanded further.

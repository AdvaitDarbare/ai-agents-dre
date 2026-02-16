# Architecture

## System Overview

```text
Data File (CSV/Parquet/JSON)
      |
      v
File Watcher / API Trigger
      |
      v
MonitorAgent.evaluate_data_file()
      |
      +--> Stage A   SchemaValidator (hard gate)
      +--> Stage A2  DataProfiler (value-level quality)
      +--> Stage A3  DimensionScorer (6D weighted score)
      +--> Stage B   AnomalyDetector (z-score + robust z + IQR)
      +--> Stage C   Load / Quarantine decision
      +--> Stage D   Persist run + metrics + baselines + SLO checks
      +--> Stage E   LLM explanation/advice (Agno)
```

## Core Runtime Components

- `src/api.py`: FastAPI API surface
- `src/agents/monitor_agent.py`: orchestrator
- `src/runners/file_watcher.py`: event-driven ingest watcher
- `src/contracts/store.py`: contract store abstraction (file-backed)
- `src/tools/*`: validators, profiler, anomaly engine, lineage, remediation
- `src/utils/database.py`: PostgreSQL pool + schema init

## Contract Lifecycle Model

### Path 1: Contract-first (preferred)

1. Dataset contract exists in `config/expectations`
2. New file arrives
3. Auto-validation runs
4. Verdict persisted and surfaced in UI

### Path 2: Observation + HITL fallback

1. New file arrives with no approved contract
2. File moved to `data/pending_approval`
3. Proposal YAML generated in `config/proposals`
4. Human approves/rejects in UI/API
5. On approval: contract saved and pending files validated automatically

## Data Persistence Model

PostgreSQL stores:

- run outcomes (`run_history`)
- metric time-series (`metric_history`)
- learned baselines (`learned_thresholds`)
- SLO checks (`slo_history`)
- dataset registry (`dataset_registry`)
- governance and remediation history

DuckDB is used in-memory for profiling/validation workflows only.

## Decisioning

### Quality gates

- Schema-breaking issues: `BLOCKED`
- Weighted quality score thresholds:
  - below block threshold -> `BLOCKED`
  - below warning threshold -> `WARNING`

### Anomaly gates

- Metrics compared against learned baselines
- Detectors: z-score, robust z-score (MAD), IQR bounds
- Impact criticality influences final severity handling

### SLO checks

Per-run SLO checks include:

- availability
- minimum quality score
- maximum anomaly count
- freshness SLA (if configured)

## Frontend View Model

`frontend/src/App.jsx` consumes API endpoints and renders:

- Health pulse table
- Dataset cards (active + pending/unconfigured)
- Expanded row detail tabs
  - Data Quality
  - Anomalies & Violations
  - SLOs & Budget
  - Governance & History
  - Impact Lineage

## Current Constraints

- Single-process orchestrator (no async job queue yet)
- Monolithic frontend app file (planned extraction)
- Contract source-of-truth is local filesystem (Git-backed contract store is future path)

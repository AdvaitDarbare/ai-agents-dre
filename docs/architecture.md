# Architecture

## System Overview

```
CSV/Parquet File
      |
      v
[MonitorAgent.evaluate_data_file()]
      |
      +---> Stage A:  SchemaValidator    (Hard Gate — blocks on missing cols / type mismatches)
      +---> Stage A2: DataProfiler       (Value-level quality — null rates, uniqueness, custom SQL)
      +---> Stage B:  AnomalyDetector    (Soft Gate — z-score vs learned baselines)
      |         +---> ImpactAnalyzer     (Criticality from lineage — HIGH/LOW determines block vs warn)
      +---> Stage C:  DorisLoader        (Load to warehouse if PASSED/WARNING)
      +---> Stage D:  _record_run()      (Persist to PostgreSQL: run_history, metric_history, registry)
      +---> Stage E:  _enrich_with_llm() (Agno Agent generates human-readable advice)
```

## Decision Matrix

The MonitorAgent uses a 2-axis decision matrix:

| Z-Score \ Criticality | LOW           | HIGH/CRITICAL  |
|------------------------|---------------|----------------|
| > z_critical (default 3.0) | WARNING  | BLOCKED        |
| > z_warn (default 2.5)     | WARNING  | WARNING        |
| Below thresholds            | PASSED   | PASSED         |

Quality Score also gates:
- Below `qs_block` (default 50%) -> BLOCKED
- Below `qs_warn` (default 80%) -> WARNING

Thresholds are configurable per-dataset in `config/expectations/{name}.yaml` under `quality.anomaly_thresholds`.

## Data Flow

### Write Path (evaluation run)
1. `MonitorAgent.evaluate_data_file()` orchestrates all tools
2. `AnomalyDetector.evaluate_run()` computes z-scores, saves to `metric_history`, updates `learned_thresholds`
3. `_record_run()` writes to `run_history`, `dataset_registry`, sends alerts via `AlertRouter`
4. Verdict JSON logged to `data/history/{date}/{dataset}_{time}.json`

### Read Path (API/frontend)
1. FastAPI endpoints in `src/api.py` query PostgreSQL via `get_connection()`
2. React frontend fetches from `localhost:8000`
3. Chart components (`frontend/src/components/charts/`) render time-series, quality bars, heatmaps

## Tech Stack

### Backend
- **Python 3.12** — main language
- **FastAPI** — REST API
- **psycopg2** — PostgreSQL driver with ThreadedConnectionPool
- **Agno** — LLM agent framework (wraps OpenAI)
- **pandas** — data loading and manipulation
- **DuckDB** — in-memory only for SQL-on-DataFrame in DataProfiler and SchemaValidator
- **PyYAML** — contract parsing

### Frontend
- **React 19** with Vite
- **Tailwind CSS** with HSL custom properties (primary: `#13c8ec`)
- **Recharts** — all charts (AreaChart, ComposedChart, LineChart)
- **Framer Motion** — animations
- **Lucide React** — icons
- **Axios** — HTTP client

### Infrastructure
- **PostgreSQL 16** (Alpine) via Docker Compose
- Potential future: Redis for caching, Celery for async scans

## Why These Choices

| Decision | Rationale |
|----------|-----------|
| psycopg2 over SQLAlchemy | Direct control, no ORM overhead, simpler for agents to reason about |
| Recharts over Tremor | Already in codebase, well-documented, large training corpus |
| YAML contracts over JSON Schema | Human-readable, easy diffing, already adopted |
| Monolith App.jsx | Rapid iteration — extract components when they stabilize |
| PostgreSQL over DuckDB for persistence | Production-ready concurrency, proper transactions, external tool access |

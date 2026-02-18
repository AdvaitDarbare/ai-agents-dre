# Agentic DRE Platform

Compatibility note: canonical coding-agent instructions live in `AGENTS.md`.

Agentic Data Reliability Engineering platform. Sequential pipeline: Schema Validation -> Data Profiling -> Anomaly Detection -> Impact Analysis -> Load/Quarantine -> LLM Reasoning.

## Quick Start

```bash
docker-compose up -d          # PostgreSQL 16 on :5432
cd frontend && npm run dev    # React frontend on :5173
uvicorn src.api:app --reload  # FastAPI backend on :8000
```

## Repo Structure

```
src/
  api.py                  # FastAPI backend — all endpoints
  main.py                 # CLI entry point
  agents/
    monitor_agent.py      # Orchestrator — runs the full pipeline
  tools/
    anomaly_detector.py   # Z-score engine, writes to PostgreSQL
    data_profiler.py      # Column-level quality (uses DuckDB in-memory only)
    schema_validator.py   # Contract validation (uses DuckDB in-memory only)
    impact_analyzer.py    # Lineage-based blast radius
    schema_remediator.py  # LLM-powered schema fixes
    alert_router.py       # Alerting dispatch
    doris_loader.py       # Data warehouse loader
    system_health.py      # Upstream health checks
  utils/
    database.py           # PostgreSQL connection pool (psycopg2)
frontend/
  src/App.jsx             # Main React app (~2400 lines, monolith)
  src/components/charts/  # Visualization components (Recharts-based)
  src/components/         # IncidentFeed and future components
  src/api/index.js        # Axios API client
config/
  expectations/*.yaml     # Data contracts per dataset
  lineage.yaml            # Dependency graph
  alerts.yaml             # Alert routing rules
```

## Deep Docs (read these when needed)

- `docs/architecture.md`   — System design, data flow, tech decisions
- `docs/database.md`       — PostgreSQL schema, all 6 tables, indexes
- `docs/api.md`            — Every endpoint with request/response shapes
- `docs/patterns.md`       — Coding conventions, do/don't patterns
- `docs/plan.md`           — Current roadmap, what's done, known tech debt
- `docs/OBSERVABILITY_GAP_ANALYSIS.md` — Industry comparison and gap analysis

## Key Decisions

- **PostgreSQL** for all persistent storage (not DuckDB)
- **DuckDB** kept ONLY in `data_profiler.py` and `schema_validator.py` for in-memory DataFrame SQL
- **React 19 + Vite + Tailwind + Recharts + Framer Motion** frontend
- **psycopg2 ThreadedConnectionPool** for DB connections (not SQLAlchemy)
- **YAML contracts** in `config/expectations/` define expected schema per dataset
- **Z-score with seasonality** for anomaly detection (STDDEV_SAMP in Postgres)

## Running Tests

```bash
pytest tests/ -v
```

## Environment Variables

```
POSTGRES_HOST=localhost  POSTGRES_PORT=5432
POSTGRES_DB=dre         POSTGRES_USER=dre_user  POSTGRES_PASSWORD=dre_password
OPENAI_API_KEY=...      OPENAI_MODEL_NAME=gpt-4o
```

# Plan — Current Roadmap & Tech Debt

Last updated: 2025-02-14

## What's Done

### PostgreSQL Migration (Complete)
- [x] `src/utils/database.py` — connection pool, `init_tables()`, `get_connection()` context manager
- [x] `docker-compose.yml` — PostgreSQL 16 Alpine
- [x] `src/tools/anomaly_detector.py` — fully migrated (STDDEV_SAMP, ON CONFLICT upsert, %s params)
- [x] `src/agents/monitor_agent.py` — migrated `evaluate_all` and `get_run_history`
- [x] `src/api.py` — all 10+ DuckDB call sites replaced with PostgreSQL

### New API Endpoints (Complete)
- [x] `GET /incidents` — BLOCKED/WARNING runs with severity mapping
- [x] `GET /metrics/{name}/timeseries` — time-series with baseline confidence bands
- [x] `GET /baselines/{name}` — all learned thresholds for a dataset

### Frontend Visualization Components (Complete)
- [x] `VolumeAnomalyChart` — row count time-series with 3-sigma bands (Monte Carlo style)
- [x] `DriftChart` — distribution drift with 2-sigma warning band (Databricks style)
- [x] `ColumnQualityBars` — horizontal quality bars per column (Soda/GX style)
- [x] `NullRateHeatmap` — color-coded null rate grid (Bigeye style)
- [x] `QualityScoreTrend` — quality over time with warning/block lines (Anomalo style)
- [x] `SchemaValidationTable` — column-level contract validation detail
- [x] `IncidentFeed` — filterable incident list with severity badges

### App.jsx Integration (Complete)
- [x] "Incidents" nav tab added
- [x] ExpandedRowDetail enhanced with "Anomaly Detection" and "Schema Detail" sub-tabs
- [x] Quality tab now uses ColumnQualityBars + QualityScoreTrend + NullRateHeatmap

## What's Next

### Short-term (Priority)
- [ ] Extract App.jsx into separate component files (it's ~2400 lines — too large)
- [ ] Add `requirements.txt` / `pyproject.toml` with `psycopg2-binary` dependency
- [ ] Add error boundary components in React for graceful failures
- [ ] Add loading skeletons to chart components for polish
- [ ] Implement real `GET /metrics/{name}/timeseries` with day_of_week grouping for seasonality

### Medium-term
- [ ] Upstream Health Dots component (Monte Carlo lineage style)
- [ ] Data freshness monitoring (SLA-based, not just file mtime)
- [ ] Async evaluation with background workers (Celery or similar)
- [ ] User authentication (JWT or OAuth)
- [ ] Role-based access to remediation actions

### Long-term
- [ ] Multi-tenant dataset isolation
- [ ] Custom SQL check editor in frontend
- [ ] Slack/PagerDuty alert integration (currently stubbed in AlertRouter)
- [ ] Historical trend comparison (week-over-week, month-over-month)

## Known Tech Debt

| Item | Location | Impact | Notes |
|------|----------|--------|-------|
| Monolith App.jsx | `frontend/src/App.jsx` | Maintainability | ~2400 lines, needs component extraction |
| Old Streamlit dashboard | `src/dashboard/app.py` | Dead code | Still uses DuckDB, not part of React app |
| Duplicate `discover_datasets()` calls | `src/api.py` | Performance | Several endpoints call it redundantly |
| Hardcoded data paths | `src/agents/monitor_agent.py` | Fragility | `data/test/`, `data/landing/` hardcoded |
| No connection retry logic | `src/utils/database.py` | Reliability | Pool fails hard if PG is down at startup |
| No migration system | `src/utils/database.py` | Schema evolution | `init_tables()` uses IF NOT EXISTS, no ALTER |
| Chat endpoint re-discovers all datasets | `src/api.py` `/chat` | Latency | Calls `discover_datasets()` on every chat message |
| No test coverage for new chart endpoints | `src/api.py` | Quality | `/incidents`, `/timeseries`, `/baselines` untested |
| `SchemaValidationTable` parses YAML with regex | `frontend/src/components/charts/` | Fragility | Should use a proper YAML parser or API endpoint |

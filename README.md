# Agentic Data Reliability Engineering (DRE) Platform

An agentic data reliability control plane for local-first data workflows.

It validates incoming datasets against contracts, detects anomalies, evaluates SLOs, and routes data through active/pending/quarantine flows with human-in-the-loop (HITL) controls.

## Key Capabilities

- Contract-first + HITL fallback lifecycle
  - Existing contract: auto-validate on ingest
  - No contract: move file to pending approval and generate proposal YAML
- Multi-stage reliability pipeline
  - Schema validation, data profiling, anomaly detection, impact analysis, SLO evaluation
- Robust anomaly detection
  - Seasonal/global baselines with z-score, robust z-score (MAD), and IQR checks
- Expanded metric tracking
  - Metric metadata (`metric_group`, `column_name`, `segment`, `tags`) for richer observability
- SLO tracking
  - Per-run SLO checks and summary endpoints
  - Frontend SLO summary cards and run-level table in dataset details
- Governance and remediation
  - Contract history, rollback, remediation history, and full verdict storage
- Modern React operations UI
  - Health pulse, dataset management, incident feed, lineage, contract workflows

## Architecture (Current)

### Runtime Components

- Frontend: React 19 + Vite + Tailwind + Recharts + Framer Motion
- Backend API: FastAPI (`src/api.py`)
- MCP server: FastMCP (`src/mcp/server.py`)
- Orchestrator: `MonitorAgent` (`src/agents/monitor_agent.py`)
- Event runner: file watcher (`src/runners/file_watcher.py`)
- HITL workflow runtime: LangGraph (`src/workflows/hitl_contract_workflow.py`)
- Persistence: PostgreSQL 16 (`run_history`, `metric_history`, `learned_thresholds`, `slo_history`, etc.)
- Profiling/validation engine: DuckDB in-memory + pandas
- LLM runtime: Agno with OpenAI model adapter

### Ingestion + Evaluation Flow

1. File lands in `data/landing`
2. File watcher resolves dataset name
3. If contract exists: run full evaluation pipeline
4. If no contract: move to `data/pending_approval`, generate proposal in `config/proposals`
5. On approval: save contract and validate pending files automatically
6. Persist runs, metrics, baselines, SLO checks, and verdict logs
7. Contract-missing path is durable via LangGraph interrupt/resume + PostgreSQL checkpointer

## Quick Start

### Prerequisites

- Python 3.12+
- Node.js 18+
- Docker (for PostgreSQL)

### 1) Start PostgreSQL

```bash
docker-compose up -d
```

### 2) Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3) Configure Environment

```bash
export OPENAI_API_KEY=your_key_here
export OPENAI_MODEL_NAME=gpt-4o
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=dre
export POSTGRES_USER=dre_user
export POSTGRES_PASSWORD=dre_password
```

### 4) Start Backend API

```bash
uvicorn src.api:app --reload
# http://localhost:8000
```

### 5) Start Frontend

```bash
cd frontend
npm install
npm run dev
# http://localhost:5173
```

### 6) Optional: Start Event-Driven Watcher

```bash
python3 -m src.runners.file_watcher
```

### 7) Optional: Start MCP Server (Streamable HTTP)

```bash
python3 -m src.mcp.server --transport streamable-http --host 0.0.0.0 --port 8001 --path /mcp
# MCP endpoint: http://localhost:8001/mcp
```

For local CLI/IDE integration:

```bash
python3 -m src.mcp.server --transport stdio
```

## Notable API Endpoints

- Health + datasets
  - `GET /health`
  - `GET /datasets`
  - `GET /pulse`
- Evaluation + history
  - `POST /evaluate/{dataset_name}`
  - `GET /runs`
  - `GET /history/{dataset_name}`
  - `GET /verdict/{run_id}`
- Contracts + HITL
  - `GET /contracts/pending`
  - `POST /contracts/approve`
  - `DELETE /contracts/pending/{dataset_name}`
  - `POST /contracts/propose`
- Metrics + baselines + SLOs
  - `GET /metrics/{dataset_name}/timeseries`
  - `GET /baselines/{dataset_name}`
  - `GET /slos/{dataset_name}`
  - `GET /slos/{dataset_name}/summary`

See `docs/api.md` for full request/response details.

## Repository Structure

```text
src/
  api.py
  main.py
  agents/
    monitor_agent.py
    file_actuator.py
  runners/
    file_watcher.py
  workflows/
    hitl_contract_workflow.py
  mcp/
    server.py
  tools/
    anomaly_detector.py
    data_profiler.py
    schema_validator.py
    impact_analyzer.py
    schema_remediator.py
    contract_generator.py
    dimension_scorer.py
  contracts/
    store.py
  utils/
    database.py
frontend/
  src/
    App.jsx
    api/index.js
    components/
docs/
  api.md
  architecture.md
  database.md
  mcp.md
  file_watcher_guide.md
  plan.md
```

## Tests

```bash
pytest tests -v
```

## License

MIT

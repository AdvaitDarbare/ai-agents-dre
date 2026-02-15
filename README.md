# Agentic Data Reliability Engineering (DRE) Platform

**Next-generation data observability platform** powered by autonomous AI agents and a modern React frontend. Acts as an intelligent gatekeeper for your data lake and warehouse, ensuring only high-quality, trusted data reaches production.

## ✨ Key Features

### 🎯 6-Dimensional Quality Framework
- **Completeness**: Null rate tracking, missing value detection
- **Validity**: Pattern matching, range checks, allowed values
- **Consistency**: Cross-column validation, referential integrity
- **Timeliness**: Data freshness monitoring, SLA tracking
- **Accuracy**: Statistical profiling, outlier detection
- **Uniqueness**: Duplicate detection, primary key validation

### 🤖 Agentic Orchestration
- **Monitor Agent**: Production-grade orchestrator coordinating detection tools via sequential pipeline
- **LLM Reasoning**: GPT-4o integration for anomaly analysis and remediation suggestions
- **Smart Triage**: Automated status handling (PASSED, WARNING, BLOCKED) based on criticality

### 📊 Multi-Layer Detection
- **Schema Validation**: Hard gate for column presence, type checks, row count constraints
- **Data Profiling**: Column-level quality scoring with violation examples
- **Anomaly Detection**: Z-score based volume/metric drift with seasonality awareness
- **Impact Analysis**: Lineage-aware criticality assessment for blast radius calculation

### 🔧 Automated Remediation
- **Contract Generator**: AI-powered YAML contract creation from data profiling
- **Schema Evolution**: Automatic detection and proposal of schema changes
- **Version Control**: Full history tracking with rollback capabilities

### 🎨 Modern UI
- **React 19 + Vite**: Lightning-fast frontend with hot reload
- **Violet Theme**: Professional color scheme with light/dark mode
- **Interactive Charts**: Quality radar, volume anomalies, drift detection, null heatmaps
- **Real-time Updates**: Live scan status and quality score tracking

## 🏗️ Architecture

### Tech Stack
- **Frontend**: React 19, Vite, Tailwind CSS, Recharts, Framer Motion
- **Backend**: FastAPI (Python 3.12), psycopg2
- **Database**: PostgreSQL 16 (persistent storage), DuckDB (in-memory profiling)
- **LLM**: OpenAI GPT-4o via Agno SDK

### Data Flow
```
1. Schema Validation → 2. Data Profiling → 3. Anomaly Detection →
4. Impact Analysis → 5. Load/Quarantine → 6. LLM Reasoning
```

## 🚀 Quick Start

### Prerequisites
- Python 3.12+
- Node.js 18+
- Docker (for PostgreSQL)

### 1. Start PostgreSQL
```bash
docker-compose up -d
```

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Set Environment Variables
```bash
export OPENAI_API_KEY=your_key_here
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=dre
export POSTGRES_USER=dre_user
export POSTGRES_PASSWORD=dre_password
```

### 4. Start Backend API
```bash
uvicorn src.api:app --reload
# API available at http://localhost:8000
```

### 5. Start Frontend
```bash
cd frontend
npm install
npm run dev
# UI available at http://localhost:5173
```

### 6. Run a Scan (CLI)
```bash
python src/main.py
```

## 📁 Project Structure

```
src/
  api.py                  # FastAPI backend (all endpoints)
  main.py                 # CLI entry point
  agents/
    monitor_agent.py      # Main orchestrator
  tools/
    anomaly_detector.py   # Z-score engine (PostgreSQL)
    data_profiler.py      # Column quality (DuckDB in-memory)
    schema_validator.py   # Contract validation (DuckDB in-memory)
    impact_analyzer.py    # Lineage-based blast radius
    schema_remediator.py  # LLM-powered schema fixes
    contract_generator.py # AI contract creation
    dimension_scorer.py   # 6D quality framework
  utils/
    database.py           # PostgreSQL connection pool
frontend/
  src/
    App.jsx               # Main React app (~2400 lines)
    components/           # Reusable UI components
    components/charts/    # Visualization components
    api/index.js          # Axios API client
config/
  expectations/*.yaml     # Data contracts per dataset
  lineage.yaml            # Dependency graph
  alerts.yaml             # Alert routing rules
docs/
  architecture.md         # System design & data flow
  database.md             # PostgreSQL schema (6 tables)
  api.md                  # All endpoints with shapes
  patterns.md             # Coding conventions
  plan.md                 # Roadmap & tech debt
```

## 📖 Documentation

- **[Architecture Guide](docs/architecture.md)** - System design, tech decisions, data flow
- **[Database Schema](docs/database.md)** - All 6 PostgreSQL tables with indexes
- **[API Reference](docs/api.md)** - Every endpoint with request/response shapes
- **[Coding Patterns](docs/patterns.md)** - Do/don't patterns, conventions
- **[Implementation Plan](docs/plan.md)** - Current roadmap, completed features, tech debt

## 🎯 Key Design Decisions

- **PostgreSQL** for all persistent storage (not DuckDB)
- **DuckDB** only for in-memory DataFrame SQL in profiler/validator
- **React 19** with Vite for modern, fast frontend
- **psycopg2 ThreadedConnectionPool** for DB connections (not SQLAlchemy)
- **YAML contracts** in `config/expectations/` define expected schema
- **Z-score with seasonality** for anomaly detection (compares same day of week)

## 🔮 Roadmap

### Completed ✅
- [x] 6-dimensional quality framework
- [x] React frontend with violet theme
- [x] PostgreSQL integration
- [x] Real-time dimension scoring
- [x] Violation examples display
- [x] Contract governance UI
- [x] Anomaly detection with baselines
- [x] Impact lineage visualization

### In Progress 🚧
- [ ] Advanced drift detection (K-S test, CUSUM)
- [ ] Streaming data support (Kafka/Pulsar)
- [ ] Agentic PII detection
- [ ] Source system notifications

### Planned 📋
- [ ] Cloud warehouse connectors (Snowflake, BigQuery, Databricks)
- [ ] Iceberg table support
- [ ] Custom alert channels (Slack, PagerDuty)
- [ ] Multi-tenancy support

## 🧪 Running Tests

```bash
pytest tests/ -v
```

## 🤝 Contributing

This is an agentic codebase following best practices from `.agent/skills/agent_best_practices/`:
- CLAUDE.md is a table of contents, not a manual
- All documentation lives in the repo (docs/)
- Plans are versioned (docs/plan.md)
- Progressive disclosure pattern for context management

## 📝 License

MIT

---

*Built for modern data teams who value reliability, automation, and speed.*

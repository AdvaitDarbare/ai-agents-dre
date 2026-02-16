# API Reference

FastAPI backend at `src/api.py`. Runs on `http://localhost:8000`.

## Health & Status

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Basic health check |
| GET | `/pulse` | All datasets with status, quality score, sparkline history |
| GET | `/stats/global` | Today's run count, pass rate, avg duration |
| GET | `/health/system` | Upstream service health checks |

## Datasets

| Method | Path | Description |
|--------|------|-------------|
| GET | `/datasets` | Auto-discover all dataset contracts from config/expectations/ |
| POST | `/evaluate/{dataset_name}` | Trigger full pipeline evaluation |
| GET | `/metrics/{dataset_name}` | Latest cached metrics for a dataset |
| GET | `/profile/{dataset_name}` | Deep data profile (runs DataProfiler live) |

## History & Incidents

| Method | Path | Description |
|--------|------|-------------|
| GET | `/runs?limit=50` | Recent runs across all datasets |
| GET | `/history/{dataset_name}?limit=50` | Run history for specific dataset |
| GET | `/incidents?limit=50` | BLOCKED/WARNING runs as incidents |
| GET | `/verdict/{run_id}` | **NEW** Full verdict with all tool outputs for a specific run |

### Verdict Response Shape
```json
{
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2026-02-15T16:03:22.877612+00:00",
  "dataset_name": "healthcare_messy_data",
  "status": "WARNING",
  "quality_score": 79.99,
  "anomaly_count": 1,
  "z_score_max": 2.5,
  "reason": "Data Quality Score below threshold...",
  "duration_ms": 1234,
  "dimension_scores": {
    "completeness": 85.5,
    "validity": 92.3,
    // ... other dimensions
  },
  "full_verdict": {
    "schema": { /* schema validation results */ },
    "profile": { /* data profiling results */ },
    "anomalies": [ /* anomaly objects */ ],
    "metrics": { /* statistical metrics */ },
    "quality_dimensions": { /* 6D scores */ },
    "llm_advice": "...",
    "load_status": "..."
  }
}
```

## Time-Series & Baselines

| Method | Path | Description |
|--------|------|-------------|
| GET | `/metrics/{name}/timeseries?metric=row_count&limit=30` | Metric time-series with baseline bands |
| GET | `/baselines/{name}` | All learned thresholds for a dataset |

### Timeseries Response Shape
```json
{
  "dataset": "transactions",
  "metric": "row_count",
  "baseline": {
    "mean": 1000.5,
    "std": 45.2,
    "type": "global",
    "sample_count": 25,
    "upper_3sigma": 1136.1,
    "lower_3sigma": 864.9,
    "upper_2sigma": 1090.9,
    "lower_2sigma": 910.1
  },
  "data": [
    {"timestamp": "2025-02-14T10:00:00", "value": 1023, "run_id": 42, "day_of_week": 4}
  ]
}
```

## Governance

| Method | Path | Description |
|--------|------|-------------|
| GET | `/governance/{dataset_name}/history` | Schema version history (audit log) |
| GET | `/governance/file/{filename}` | Read historical schema version content |
| POST | `/governance/rollback` | Revert to historical version + auto-rescan |

### Rollback Request
```json
{"dataset_name": "transactions", "filename": "transactions_20250214_v2.yaml"}
```

## Contracts

| Method | Path | Description |
|--------|------|-------------|
| GET | `/contracts/{dataset_name}` | Get active contract YAML |
| POST | `/contracts/propose` | Generate proposed contract from data |
| POST | `/contracts/save` | Save user-approved contract |

## Remediation

| Method | Path | Description |
|--------|------|-------------|
| GET | `/remediation/{dataset_name}` | Get hybrid remediation plan (deterministic + LLM) |
| POST | `/remediation/apply` | Apply AI-generated fix + log to audit trail |

## Chat

| Method | Path | Description |
|--------|------|-------------|
| POST | `/chat?query=...` | Copilot chat with LLM reasoning over pipeline state |

## Lineage

| Method | Path | Description |
|--------|------|-------------|
| GET | `/lineage?dataset=optional` | Full or filtered lineage graph |

## Frontend API Client

All endpoints are wrapped in `frontend/src/api/index.js`:

```javascript
import { getPulse, getIncidents, getMetricTimeseries, getBaselines } from './api';

const res = await getMetricTimeseries('transactions', 'row_count', 30);
console.log(res.data.baseline);  // { mean, std, upper_3sigma, ... }
console.log(res.data.data);      // [{ timestamp, value, run_id }]
```

# API Reference

FastAPI backend in `src/api.py`.
Base URL: `http://localhost:8000`.

## Health & System

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Service health check |
| GET | `/health/system` | Upstream service health checks from lineage config |
| GET | `/stats/global` | Global daily stats (runs, pass rate, avg duration) |

## Dataset Discovery & Evaluation

| Method | Path | Description |
|---|---|---|
| GET | `/datasets` | Discover managed + unmanaged datasets |
| GET | `/pulse` | Dataset pulse view for UI |
| POST | `/evaluate/{dataset_name}` | Trigger full pipeline evaluation |
| GET | `/datasets/{dataset_name}/data?limit=100` | Dataset preview/sample |
| DELETE | `/datasets/{dataset_name}` | Hard-delete dataset + artifacts + DB rows |

## Runs, History, Incidents

| Method | Path | Description |
|---|---|---|
| GET | `/runs?limit=50` | Recent runs across datasets |
| GET | `/history/{dataset_name}?limit=50` | Run history for dataset |
| GET | `/incidents?limit=50` | Warning/blocked incidents |
| GET | `/verdict/{run_id}` | Full verdict payload for one run |

## Metrics & Baselines

| Method | Path | Description |
|---|---|---|
| GET | `/metrics/{dataset_name}` | Latest metric snapshot |
| GET | `/metrics/{dataset_name}/timeseries?metric=row_count&limit=30` | Metric time-series + baseline |
| GET | `/baselines/{dataset_name}` | Learned thresholds per metric |

### Timeseries Notes

`/metrics/{dataset_name}/timeseries` returns enriched metric records:

- `metric_group`
- `column_name`
- `segment`
- `tags`

This supports richer UI filtering and grouping.

## SLOs

| Method | Path | Description |
|---|---|---|
| GET | `/slos/{dataset_name}?limit=100` | Run-level SLO checks |
| GET | `/slos/{dataset_name}/summary?window=200` | Aggregated SLO pass rates + error budget burn |

## Contracts & HITL Workflow

| Method | Path | Description |
|---|---|---|
| GET | `/contracts/pending` | Pending contract proposals |
| POST | `/contracts/propose` | Generate proposal from data file |
| POST | `/contracts/approve` | Approve contract + validate pending files |
| DELETE | `/contracts/pending/{dataset_name}` | Reject proposal + quarantine pending files |
| GET | `/contracts/{dataset_name}` | Read active contract content |
| POST | `/contracts/save` | Save contract update |
| GET | `/contract/{dataset_name}` | Get active contract (alternate endpoint) |
| GET | `/contract-history/{dataset_name}` | Contract version history |
| GET | `/contract/{dataset_name}/version/{version_id}` | Get a historical contract version |
| POST | `/contract/{dataset_name}` | Save new contract version |
| POST | `/contract/{dataset_name}/ai-modify` | AI modify contract YAML from instruction |

## Governance & Remediation

| Method | Path | Description |
|---|---|---|
| GET | `/governance/{dataset_name}/history` | Schema audit trail |
| GET | `/governance/file/{filename}` | Read historical schema file |
| POST | `/governance/rollback` | Roll back schema and trigger re-scan |
| GET | `/remediation/{dataset_name}` | Suggested remediation plan |
| POST | `/remediation/apply` | Apply remediation and log audit |

## Lineage & Chat

| Method | Path | Description |
|---|---|---|
| GET | `/lineage?dataset=optional` | Full or filtered lineage graph |
| POST | `/chat?query=...` | Copilot chat |

## Example

```bash
curl -s "http://localhost:8000/metrics/orders/timeseries?metric=row_count&limit=10" | jq
curl -s "http://localhost:8000/slos/orders/summary?window=200" | jq
```

# API Reference

FastAPI backend in `src/api.py`.
Base URL: `http://localhost:8000`.

## Health & System

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Service health check |
| GET | `/health/system` | Upstream service health checks from lineage config |
| GET | `/stats/global` | Global daily stats (runs, pass rate, avg duration) |
| GET | `/backtesting/{dataset_name}?metric=row_count&limit=500` | Backtesting harness report (precision/recall/FP/FN) |
| POST | `/policy/check` | Evaluate policy decision (allow vs approval-required) for an action |
| GET | `/integrations/sources` | Runtime connector health and discovered dataset coverage |
| GET | `/platform/config` | Read-only runtime platform configuration snapshot |
| POST | `/platform/reset-runtime` | Danger-zone runtime reset (truncate runtime tables + clear generated artifacts; requires `confirm_phrase=\"RESET\"`) |
| GET | `/risk/datasets?limit=20` | Reliability risk ranking across datasets |
| GET | `/metrics/outcomes?days=30` | Outcome metrics (pass rates, incident MTTR, coverage) |

## Dataset Discovery & Evaluation

| Method | Path | Description |
|---|---|---|
| GET | `/datasets` | Discover managed + unmanaged datasets |
| GET | `/pulse` | Dataset pulse view for UI |
| POST | `/evaluate/{dataset_name}` | Trigger full pipeline evaluation (supports optional `force_load: true` in body) |
| GET | `/datasets/{dataset_name}/data?limit=100` | Dataset preview/sample |
| DELETE | `/datasets/{dataset_name}` | Hard-delete dataset + artifacts + DB rows (policy gate on HIGH/CRITICAL) |

## Async Jobs

| Method | Path | Description |
|---|---|---|
| POST | `/jobs/evaluate/{dataset_name}` | Queue background dataset scan job (supports optional `force_load: true` in body) |
| POST | `/jobs/evaluate-bulk` | Queue background bulk scan job (`dataset_names`, `force_load`) |
| POST | `/jobs/evaluate-all` | Queue background bulk scan job for all discovered datasets (`include_unconfigured`, `force_load`) |
| POST | `/jobs/delete/{dataset_name}` | Queue background dataset delete job (`confirm=true`; policy gate on HIGH/CRITICAL) |
| POST | `/jobs/delete-bulk` | Queue background bulk delete (`dataset_names`, `confirm=true`; policy gate on HIGH/CRITICAL) |
| POST | `/jobs/remediation/apply` | Queue background remediation apply (`dataset_name`, `proposed_yaml`, `error_context`; policy gate on HIGH/CRITICAL) |
| GET | `/jobs?limit=50&status=&action=&dataset_name=` | List background jobs |
| GET | `/jobs/{job_id}` | Get single job status/result |

Runtime mode:

- `ASYNC_JOB_EXECUTION_MODE=inprocess` (default): API executes jobs via local thread pool.
- `ASYNC_JOB_EXECUTION_MODE=external_worker`: API only enqueues; separate worker executes claimed jobs.

### Policy-Gated Payload Fields

For policy-gated actions, include these fields when required:

- `policy_approved: true`
- `policy_reason: "why this action is approved"`

When policy requires approval and these are missing, API returns `409` with policy details.

`409` responses include a structured `detail` payload:

```json
{
  "detail": {
    "message": "Policy approval required for this action.",
    "policy": {
      "action": "delete",
      "decision": "approval_required",
      "reason": "...",
      "required_controls": ["confirm", "policy_approved", "policy_reason"],
      "targets": ["orders"],
      "criticalities": {"orders": "CRITICAL"}
    },
    "missing_controls": ["policy_approved", "policy_reason"]
  }
}
```

## Runs, History, Incidents

| Method | Path | Description |
|---|---|---|
| GET | `/runs?limit=50` | Recent runs across datasets |
| GET | `/history/{dataset_name}?limit=50` | Run history for dataset |
| GET | `/incidents?limit=50&status=&severity=&dataset_name=&owner=` | Incident list with lifecycle filtering |
| GET | `/incidents/{incident_id}` | Single incident details |
| PATCH | `/incidents/{incident_id}` | Update lifecycle status (`OPEN`, `ACK`, `RESOLVED`) and optional owner/note |
| GET | `/verdict/{run_id}` | Full verdict payload for one run |

## Workflow Visibility & Agentic Loop

| Method | Path | Description |
|---|---|---|
| GET | `/workflow/timeline?dataset_name=&limit=100` | Unified timeline across audit events, async jobs, run history, incidents, and tool outputs |
| GET | `/workflow/timeline/stream?dataset_name=&limit=100&interval_ms=3000` | Server-sent events stream for live timeline updates |
| GET | `/workflow/agentic/graph` | Mermaid graph for the LangGraph agentic remediation workflow |
| POST | `/workflow/agentic/run` | Execute investigation -> remediation proposal -> confidence/policy gate -> optional apply |
| POST | `/workflow/agentic/remediate` | Deterministic full-auto contract remediation loop (`dataset_name`, `max_retries`, `autonomy_mode`) |
| GET | `/workflow/agentic/remediate/{id}` | Fetch persisted remediation run state + attempt timeline |
| GET | `/workflow/agentic/remediate/{id}/stream?interval_ms=2000` | SSE stream for live remediation run updates |
| GET | `/ai/brief/{dataset_name}?run_id=` | Generate an investigation-grade AI brief (status, likely root cause, impact, and next actions) |

## Audit Log

| Method | Path | Description |
|---|---|---|
| GET | `/audit?limit=100&action=&dataset_name=&status=&incident_id=&job_id=&run_id=` | Structured action audit log (ops + UI timelines); filters can match JSONB metadata IDs |
| GET | `/audit/summary?window_minutes=60&action=&dataset_name=&status=` | Aggregate counts grouped by `action` + `status` over a recent window |

## Metrics & Baselines

| Method | Path | Description |
|---|---|---|
| GET | `/metrics/{dataset_name}` | Latest metric snapshot |
| GET | `/metrics/{dataset_name}/timeseries?metric=row_count&limit=30` | Metric time-series + baseline |
| GET | `/baselines/{dataset_name}` | Learned thresholds per metric |
| GET | `/diagnostics/{dataset_name}?run_id=&check_type=&limit=200` | Diagnostics warehouse records (failed checks and sample rows) |

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
| GET | `/slos/{dataset_name}/summary?window=200` | Aggregated SLO pass/fail rates, failing SLO streaks, and error budget burn |

## Contracts & HITL Workflow

| Method | Path | Description |
|---|---|---|
| GET | `/contracts/pending` | Pending contract proposals |
| POST | `/contracts/propose` | Generate proposal from data file |
| POST | `/contracts/gate` | Shift-left contract CI gate (schema + profile, no load side effects) |
| POST | `/contracts/autopilot` | Confidence-scored contract recommendations + proposed YAML |
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
| POST | `/remediation/apply` | Apply remediation and log audit (policy gate on HIGH/CRITICAL) |

## Lineage & Chat

| Method | Path | Description |
|---|---|---|
| GET | `/lineage?dataset=optional&depth=2` | Full or filtered lineage graph with `summary`, `issues`, `graph`, and dataset `context` |
| POST | `/chat` | Copilot chat (supports `query` param or JSON body `{ query, context? }`) |
| POST | `/chat/stream` | AI SDK-compatible text streaming endpoint (`useChat` + `TextStreamChatTransport`) |

### Lineage Response Notes

`GET /lineage` includes:

- `datasets`: graph nodes
- `summary`: dataset/upstream/consumer/owner coverage counters
- `issues`: validation findings (for example, unresolved external upstream refs)
- `graph`: normalized `nodes` + `edges` payload for direct UI rendering
- `context`: bounded upstream/downstream neighborhood when `dataset` is provided

## Example

```bash
curl -s "http://localhost:8000/metrics/orders/timeseries?metric=row_count&limit=10" | jq
curl -s "http://localhost:8000/slos/orders/summary?window=200" | jq
curl -s -X POST "http://localhost:8000/policy/check" \
  -H "content-type: application/json" \
  -d '{"action":"delete","dataset_name":"orders"}' | jq
```

## RBAC (Optional)

When `DRE_RBAC_ENABLED=1`, sensitive write/destructive actions enforce role checks
from `X-DRE-ROLE` request header:

- `viewer`: read-only
- `operator`: scan/evaluate + contract propose/approve + incident/governance ops
- `admin`: full permissions including delete + contract save

## Runtime Config Notes

`GET /platform/config` includes:

- `runtime.async_jobs.execution_mode`
- `runtime.connectors_enabled`
- `runtime.doris.load_enabled`

# Database Schema Reference

PostgreSQL 16 schema initialized in `src/utils/database.py` (`init_tables()`).

## Connection Pattern

```python
from src.utils.database import get_connection

with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
```

- Driver: `psycopg2`
- Pool: `ThreadedConnectionPool(minconn=2, maxconn=10)`
- Auto-commit on success, rollback on exception

## Tables

### `run_history`

One row per evaluation run.

| Column | Type | Notes |
|---|---|---|
| run_id | VARCHAR(64) PK | UUID run id |
| timestamp | TIMESTAMPTZ | Run timestamp |
| dataset_name | VARCHAR(255) | Dataset id |
| status | VARCHAR(32) | PASSED / WARNING / BLOCKED |
| quality_score | DOUBLE PRECISION | Overall quality |
| anomaly_count | INTEGER | Number of detected anomalies |
| z_score_max | DOUBLE PRECISION | Max absolute z-score from anomalies |
| reason | TEXT | Verdict reason |
| duration_ms | INTEGER | Run duration |
| dimension_scores | JSONB | 6D score payload |
| full_verdict | JSONB | Full pipeline output |

Indexes:
- `idx_run_history_dataset(dataset_name, timestamp DESC)`

### `metric_history`

Metric time-series records linked to runs.

| Column | Type | Notes |
|---|---|---|
| run_id | VARCHAR(64) | Associated run id |
| timestamp | TIMESTAMPTZ | Observation timestamp |
| dataset_name | VARCHAR(255) | Dataset id |
| metric_name | VARCHAR(255) | Metric key |
| metric_value | DOUBLE PRECISION | Numeric value |
| day_of_week | INTEGER | Seasonal baseline grouping |
| metric_group | VARCHAR(64) | quality / volume / freshness / distribution / etc. |
| column_name | VARCHAR(255) | Optional column-level association |
| segment | VARCHAR(255) | Optional segment key (default `global`) |
| tags | JSONB | Arbitrary metadata |

Indexes:
- `idx_metrics(dataset_name, metric_name, day_of_week)`
- `idx_metrics_run(run_id)`
- `idx_metrics_grouped(dataset_name, metric_group, column_name, timestamp DESC)`

### `learned_thresholds`

Cached anomaly baselines.

| Column | Type | Notes |
|---|---|---|
| dataset_name | VARCHAR(255) | Composite PK |
| metric_name | VARCHAR(255) | Composite PK |
| baseline_mean | DOUBLE PRECISION | Baseline mean |
| baseline_std | DOUBLE PRECISION | Baseline std dev |
| baseline_type | VARCHAR(32) | seasonal/global |
| last_updated | TIMESTAMPTZ | Last refresh |
| sample_count | INTEGER | Number of historical points |

Constraint:
- `PRIMARY KEY(dataset_name, metric_name)`

### `dataset_registry`

Dataset-level state and lifecycle metadata.

| Column | Type | Notes |
|---|---|---|
| dataset_name | VARCHAR(255) PK | Dataset id |
| contract_path | TEXT | Current contract path |
| lifecycle | VARCHAR(32) | active/unconfigured/deprecated/error |
| criticality | VARCHAR(32) | Lineage-derived criticality |
| last_scanned | TIMESTAMPTZ | Last evaluation time |
| last_status | VARCHAR(32) | Last verdict status |
| last_file_mtime | DOUBLE PRECISION | File mtime used for skip-unchanged |
| scan_count | INTEGER | Number of scans |

### `slo_history`

Per-run SLO compliance checks.

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | Row id |
| run_id | VARCHAR(64) | Run id |
| timestamp | TIMESTAMPTZ | Check timestamp |
| dataset_name | VARCHAR(255) | Dataset id |
| slo_name | VARCHAR(255) | SLO name |
| operator | VARCHAR(16) | `>=`, `<=` |
| target_value | DOUBLE PRECISION | Target |
| observed_value | DOUBLE PRECISION | Observed value |
| status | VARCHAR(16) | PASS / FAIL |
| error_budget_burn | DOUBLE PRECISION | Burn contribution for this check |
| metadata | JSONB | Extra context |

Indexes:
- `idx_slo_history_dataset(dataset_name, timestamp DESC)`
- `idx_slo_history_run(run_id)`

### `schema_audit_log`

Governance record of contract changes.

| Column | Type | Notes |
|---|---|---|
| id | VARCHAR(64) PK | UUID |
| dataset_name | VARCHAR(255) | Dataset id |
| filename | TEXT | Version file |
| timestamp | TIMESTAMPTZ | Change timestamp |
| change_summary | TEXT | Change reason/summary |

### `remediation_history`

Audit trail for remediation actions.

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | Row id |
| dataset_name | VARCHAR(255) | Dataset id |
| error_context | TEXT | Trigger context |
| original_yaml | TEXT | Contract before |
| proposed_yaml | TEXT | Contract after |
| backup_path | TEXT | Backup/version path |
| timestamp | TIMESTAMPTZ | Applied time |

### `async_jobs`

Background action execution state for long-running operations.

| Column | Type | Notes |
|---|---|---|
| job_id | VARCHAR(64) PK | Job id |
| action | VARCHAR(64) | `evaluate` / `delete` / `bulk_delete` / `bulk_evaluate` / `remediation_apply` |
| dataset_name | VARCHAR(255) | Primary dataset or summary key |
| status | VARCHAR(32) | `QUEUED` / `RUNNING` / `COMPLETED` / `FAILED` |
| requested_at | TIMESTAMPTZ | Enqueue time |
| started_at | TIMESTAMPTZ | Execution start |
| finished_at | TIMESTAMPTZ | Execution finish |
| request_json | JSONB | Original request payload used by worker execution |
| result_json | JSONB | Action result payload |
| error_text | TEXT | Failure trace/message |

Indexes:
- `idx_async_jobs_requested(requested_at DESC)`
- `idx_async_jobs_status(status, requested_at DESC)`
- `idx_async_jobs_dataset(dataset_name, requested_at DESC)`

### `incidents`

Operational incident lifecycle tracking.

| Column | Type | Notes |
|---|---|---|
| incident_id | VARCHAR(64) PK | Incident id |
| run_id | VARCHAR(64) | Source run id |
| dataset_name | VARCHAR(255) | Dataset id |
| severity | VARCHAR(32) | `CRITICAL` / `WARNING` |
| status | VARCHAR(16) | `OPEN` / `ACK` / `RESOLVED` |
| owner | VARCHAR(255) | Optional owner |
| title | TEXT | Incident title |
| description | TEXT | Human-readable reason/context |
| quality_score | DOUBLE PRECISION | Quality score at incident time |
| anomaly_count | INTEGER | Anomaly count |
| z_score_max | DOUBLE PRECISION | Max anomaly z-score |
| created_at | TIMESTAMPTZ | Created timestamp |
| updated_at | TIMESTAMPTZ | Updated timestamp |
| acknowledged_at | TIMESTAMPTZ | First ACK timestamp |
| resolved_at | TIMESTAMPTZ | Resolution timestamp |
| metadata | JSONB | Additional context |

Indexes:
- `idx_incidents_created(created_at DESC)`
- `idx_incidents_status(status, created_at DESC)`
- `idx_incidents_dataset(dataset_name, status, created_at DESC)`

### `action_audit_log`

Structured operator/agent action timeline.

| Column | Type | Notes |
|---|---|---|
| id | VARCHAR(64) PK | UUID id |
| timestamp | TIMESTAMPTZ | Action timestamp |
| actor | VARCHAR(255) | User/system actor |
| source | VARCHAR(64) | api / async_jobs / service / etc. |
| action | VARCHAR(64) | Action key |
| dataset_name | VARCHAR(255) | Optional dataset id |
| status | VARCHAR(32) | Action status |
| metadata | JSONB | Context payload (job id, incident id, run id, etc.) |

Indexes:
- `idx_action_audit_ts(timestamp DESC)`
- `idx_action_audit_dataset(dataset_name, timestamp DESC)`
- `idx_action_audit_action(action, timestamp DESC)`

### `diagnostics_records`

Diagnostics warehouse rows capturing failed checks + sample records.

| Column | Type | Notes |
|---|---|---|
| id | BIGSERIAL PK | Row id |
| run_id | VARCHAR(64) | Source run id |
| dataset_name | VARCHAR(255) | Dataset id |
| column_name | VARCHAR(255) | Optional column context |
| check_type | VARCHAR(128) | Check/violation key |
| severity | VARCHAR(32) | error/warning/info |
| violation_count | INTEGER | Count of failures |
| sample_records | JSONB | Example failing rows |
| metadata | JSONB | Check payload/context |
| created_at | TIMESTAMPTZ | Insert timestamp |

Indexes:
- `idx_diagnostics_dataset(dataset_name, created_at DESC)`
- `idx_diagnostics_run(run_id, created_at DESC)`
- `idx_diagnostics_check(check_type, created_at DESC)`

### `tool_outputs`

Per-tool telemetry for each run.

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | Row id |
| run_id | VARCHAR(64) | Associated run id |
| dataset_name | VARCHAR(255) | Dataset id |
| tool_name | VARCHAR(255) | Tool identifier |
| status | VARCHAR(32) | Tool status |
| output | JSONB | Tool result payload |
| duration_ms | INTEGER | Tool runtime |
| timestamp | TIMESTAMPTZ | Logged at |

### `agentic_remediation_runs`

Run-level trace for deterministic auto-remediation loop executions.

| Column | Type | Notes |
|---|---|---|
| id | VARCHAR(64) PK | Remediation run id |
| dataset_name | VARCHAR(255) | Dataset id |
| initial_run_id | VARCHAR(64) | Run id at loop start |
| final_run_id | VARCHAR(64) | Last run id after loop end |
| status | VARCHAR(32) | `AUTO_FIXED` / `PLAN_REQUIRED` / `BLOCKED_BY_POLICY` / `FAILED` / `RUNNING` |
| attempt_count | INTEGER | Number of attempts executed |
| policy_blocks | INTEGER | Count of policy hard blocks |
| summary | JSONB | Terminal summary (`applied_changes`, `plan`) |
| created_at | TIMESTAMPTZ | Insert timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

Indexes:
- `idx_agentic_runs_dataset(dataset_name, created_at DESC)`
- `idx_agentic_runs_status(status, created_at DESC)`

### `agentic_remediation_attempts`

Attempt-level trace for each remediation iteration.

| Column | Type | Notes |
|---|---|---|
| id | BIGSERIAL PK | Row id |
| remediation_run_id | VARCHAR(64) | Parent remediation run id |
| attempt_no | INTEGER | Attempt sequence number |
| input_run_id | VARCHAR(64) | Run id consumed by this attempt |
| classification | VARCHAR(64) | Failure class (schema/constraint/load/etc.) |
| proposed_diff_summary | TEXT | AI patch summary |
| confidence | DOUBLE PRECISION | AI confidence |
| applied | BOOLEAN | Whether patch was applied |
| output_run_id | VARCHAR(64) | Run id after verification |
| result_status | VARCHAR(32) | Attempt outcome |
| error | TEXT | Error detail when failed |
| details | JSONB | Stage timeline + policy/apply evidence |
| created_at | TIMESTAMPTZ | Insert timestamp |

Indexes:
- `idx_agentic_attempts_run(remediation_run_id, attempt_no ASC)`
- `idx_agentic_attempts_dataset_run(input_run_id, output_run_id)`

### `contract_versions`

Versioned contract snapshots and metadata.

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | Version id |
| dataset_name | VARCHAR(255) | Dataset id |
| contract_path | TEXT | Active path when saved |
| contract_content | TEXT | YAML snapshot |
| contract_hash | VARCHAR(128) | SHA hash |
| created_by | VARCHAR(255) | User/agent |
| change_type | VARCHAR(64) | `manual_edit`, `ai_generated`, etc. |
| created_at | TIMESTAMPTZ | Version timestamp |

## SQL Safety Pattern

Use `%s` placeholders with psycopg2, not string interpolation:

```python
cur.execute("SELECT * FROM run_history WHERE dataset_name = %s", (dataset_name,))
```

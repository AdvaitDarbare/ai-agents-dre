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

### `tool_outputs`

Per-tool telemetry for each run.

### `contract_versions`

Versioned contract snapshots and metadata.

## SQL Safety Pattern

Use `%s` placeholders with psycopg2, not string interpolation:

```python
cur.execute("SELECT * FROM run_history WHERE dataset_name = %s", (dataset_name,))
```

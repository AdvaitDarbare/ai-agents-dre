# Database Schema Reference

PostgreSQL 16 — connection managed by `src/utils/database.py`.

## Connection

```python
from src.utils.database import get_connection

with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT ... WHERE x = %s", (value,))
        rows = cur.fetchall()
```

- Pool: `psycopg2.pool.ThreadedConnectionPool(minconn=2, maxconn=10)`
- Auto-commit on success, auto-rollback on exception
- Config via env: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`

## Tables

### `run_history`
One row per pipeline evaluation run.

| Column | Type | Notes |
|--------|------|-------|
| run_id | SERIAL PK | Auto-increment |
| timestamp | TIMESTAMP DEFAULT NOW() | When the run executed |
| dataset_name | VARCHAR(255) | Dataset identifier |
| status | VARCHAR(50) | PASSED / WARNING / BLOCKED |
| quality_score | DOUBLE PRECISION | Overall quality % (0-100) |
| anomaly_count | INTEGER DEFAULT 0 | Number of anomalies detected |
| z_score_max | DOUBLE PRECISION DEFAULT 0 | Highest absolute z-score |
| reason | TEXT | Human-readable verdict reason |
| duration_ms | INTEGER | Wall-clock time of evaluation |

**Indexes:** `idx_run_history_dataset` on (dataset_name), `idx_run_history_ts` on (timestamp)

### `metric_history`
Per-metric time-series data tied to runs.

| Column | Type | Notes |
|--------|------|-------|
| id | SERIAL PK | Auto-increment |
| timestamp | TIMESTAMP DEFAULT NOW() | |
| dataset_name | VARCHAR(255) | |
| metric_name | VARCHAR(255) | e.g. `row_count`, `amount_null_rate` |
| metric_value | DOUBLE PRECISION | The observed value |
| day_of_week | INTEGER | 0=Mon ... 6=Sun (for seasonality) |
| run_id | INTEGER | FK-like reference to run_history |

**Indexes:** `idx_metric_history_lookup` on (dataset_name, metric_name)

### `learned_thresholds`
Adaptive baselines per metric. Updated by the anomaly detector as it learns.

| Column | Type | Notes |
|--------|------|-------|
| dataset_name | VARCHAR(255) | Composite PK with metric_name |
| metric_name | VARCHAR(255) | |
| baseline_mean | DOUBLE PRECISION | Rolling mean |
| baseline_std | DOUBLE PRECISION | Rolling stddev |
| baseline_type | VARCHAR(50) | `global`, `seasonal`, etc. |
| last_updated | TIMESTAMP DEFAULT NOW() | |
| sample_count | INTEGER DEFAULT 0 | Number of data points in baseline |

**Constraint:** `UNIQUE(dataset_name, metric_name)` — upserts via `ON CONFLICT DO UPDATE`

### `dataset_registry`
One row per known dataset. Updated after each scan.

| Column | Type | Notes |
|--------|------|-------|
| dataset_name | VARCHAR(255) PK | |
| contract_path | TEXT | Path to YAML contract |
| lifecycle | VARCHAR(50) DEFAULT 'active' | active / deprecated |
| criticality | VARCHAR(50) DEFAULT 'UNKNOWN' | From lineage analysis |
| last_status | VARCHAR(50) | Last evaluation result |
| last_file_mtime | DOUBLE PRECISION | File modification time (for skip_unchanged) |
| last_scanned | TIMESTAMP | |
| scan_count | INTEGER DEFAULT 0 | Total scans for this dataset |

### `schema_audit_log`
Governance trail for contract changes.

| Column | Type | Notes |
|--------|------|-------|
| id | VARCHAR(36) PK | UUID |
| dataset_name | VARCHAR(255) | |
| filename | VARCHAR(255) | Version filename in config/history/ |
| timestamp | TIMESTAMP DEFAULT NOW() | |
| change_summary | TEXT | What changed and why |

### `remediation_history`
Before/after YAML for AI-applied fixes.

| Column | Type | Notes |
|--------|------|-------|
| id | SERIAL PK | |
| dataset_name | VARCHAR(255) | |
| timestamp | TIMESTAMP DEFAULT NOW() | |
| error_context | TEXT | The error that triggered remediation |
| original_yaml | TEXT | Contract before fix |
| proposed_yaml | TEXT | Contract after fix |
| backup_path | TEXT | Path to backup file |

## Important Patterns

```python
# ALWAYS use %s placeholders (psycopg2), never f-strings
cur.execute("SELECT ... WHERE name = %s", (name,))

# ALWAYS use context manager — auto-commits on success
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute(...)

# Upsert pattern for learned_thresholds
cur.execute("""
    INSERT INTO learned_thresholds (dataset_name, metric_name, ...)
    VALUES (%s, %s, ...)
    ON CONFLICT (dataset_name, metric_name) DO UPDATE SET ...
""", params)
```

"""
PostgreSQL Database Module
--------------------------
Centralized database connection pool and table management for the DRE platform.
Replaces the previous DuckDB-based persistence with PostgreSQL for production readiness.

Config via environment variables:
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

import os
import psycopg2
from psycopg2 import pool, extras
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

# Connection pool (initialized lazily)
_pool = None


def _get_pool():
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "dre"),
            user=os.getenv("POSTGRES_USER", "dre_user"),
            password=os.getenv("POSTGRES_PASSWORD", "dre_password"),
        )
    return _pool


@contextmanager
def get_connection():
    """
    Context manager that yields a PostgreSQL connection from the pool.
    Auto-commits on success, rolls back on exception, and returns connection to pool.

    Usage:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    """
    p = _get_pool()
    conn = p.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        p.putconn(conn)


def init_tables():
    """
    Create all required tables if they don't exist.
    Safe to call multiple times (uses IF NOT EXISTS).
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Metric History — raw metric values per run
            cur.execute("""
                CREATE TABLE IF NOT EXISTS metric_history (
                    run_id VARCHAR(64),
                    timestamp TIMESTAMPTZ DEFAULT NOW(),
                    dataset_name VARCHAR(255),
                    metric_name VARCHAR(255),
                    metric_value DOUBLE PRECISION,
                    day_of_week INTEGER,
                    metric_group VARCHAR(64) DEFAULT 'general',
                    column_name VARCHAR(255),
                    segment VARCHAR(255) DEFAULT 'global',
                    tags JSONB DEFAULT '{}'::jsonb
                )
            """)
            cur.execute("""
                ALTER TABLE metric_history
                ADD COLUMN IF NOT EXISTS metric_group VARCHAR(64) DEFAULT 'general'
            """)
            cur.execute("""
                ALTER TABLE metric_history
                ADD COLUMN IF NOT EXISTS column_name VARCHAR(255)
            """)
            cur.execute("""
                ALTER TABLE metric_history
                ADD COLUMN IF NOT EXISTS segment VARCHAR(255) DEFAULT 'global'
            """)
            cur.execute("""
                ALTER TABLE metric_history
                ADD COLUMN IF NOT EXISTS tags JSONB DEFAULT '{}'::jsonb
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_metrics
                ON metric_history(dataset_name, metric_name, day_of_week)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_metrics_run
                ON metric_history(run_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_metrics_grouped
                ON metric_history(dataset_name, metric_group, column_name, timestamp DESC)
            """)

            # Run History — structured outcomes per health check run
            cur.execute("""
                CREATE TABLE IF NOT EXISTS run_history (
                    run_id VARCHAR(64) PRIMARY KEY,
                    timestamp TIMESTAMPTZ DEFAULT NOW(),
                    dataset_name VARCHAR(255),
                    status VARCHAR(32),
                    quality_score DOUBLE PRECISION,
                    anomaly_count INTEGER DEFAULT 0,
                    z_score_max DOUBLE PRECISION DEFAULT 0,
                    reason TEXT,
                    duration_ms INTEGER DEFAULT 0,
                    dimension_scores JSONB,
                    full_verdict JSONB
                )
            """)
            # Backfill columns for environments created before JSONB fields existed.
            cur.execute("""
                ALTER TABLE run_history
                ADD COLUMN IF NOT EXISTS dimension_scores JSONB
            """)
            cur.execute("""
                ALTER TABLE run_history
                ADD COLUMN IF NOT EXISTS full_verdict JSONB
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_run_history_dataset
                ON run_history(dataset_name, timestamp DESC)
            """)

            # Learned Thresholds — cached baselines
            cur.execute("""
                CREATE TABLE IF NOT EXISTS learned_thresholds (
                    dataset_name VARCHAR(255),
                    metric_name VARCHAR(255),
                    baseline_mean DOUBLE PRECISION,
                    baseline_std DOUBLE PRECISION,
                    baseline_type VARCHAR(32),
                    last_updated TIMESTAMPTZ DEFAULT NOW(),
                    sample_count INTEGER DEFAULT 0,
                    PRIMARY KEY (dataset_name, metric_name)
                )
            """)

            # Dataset Registry — auto-discovery metadata + scan state
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dataset_registry (
                    dataset_name VARCHAR(255) PRIMARY KEY,
                    contract_path TEXT,
                    lifecycle VARCHAR(32),
                    criticality VARCHAR(32),
                    last_scanned TIMESTAMPTZ,
                    last_status VARCHAR(32),
                    last_file_mtime DOUBLE PRECISION,
                    scan_count INTEGER DEFAULT 0
                )
            """)

            # Schema Audit Log — governance history
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schema_audit_log (
                    id VARCHAR(64) PRIMARY KEY,
                    dataset_name VARCHAR(255),
                    filename TEXT,
                    timestamp TIMESTAMPTZ DEFAULT NOW(),
                    change_summary TEXT
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_audit_dataset
                ON schema_audit_log(dataset_name, timestamp DESC)
            """)

            # Remediation History — AI fix audit trail
            cur.execute("""
                CREATE TABLE IF NOT EXISTS remediation_history (
                    id SERIAL PRIMARY KEY,
                    dataset_name VARCHAR(255),
                    error_context TEXT,
                    original_yaml TEXT,
                    proposed_yaml TEXT,
                    backup_path TEXT,
                    timestamp TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            # Tool-level execution traces used by ToolLogger
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tool_outputs (
                    id SERIAL PRIMARY KEY,
                    run_id VARCHAR(64),
                    dataset_name VARCHAR(255),
                    tool_name VARCHAR(255),
                    status VARCHAR(32),
                    output JSONB,
                    duration_ms INTEGER DEFAULT 0,
                    timestamp TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                ALTER TABLE tool_outputs
                ADD COLUMN IF NOT EXISTS run_id VARCHAR(64)
            """)
            cur.execute("""
                ALTER TABLE tool_outputs
                ADD COLUMN IF NOT EXISTS dataset_name VARCHAR(255)
            """)
            cur.execute("""
                ALTER TABLE tool_outputs
                ADD COLUMN IF NOT EXISTS tool_name VARCHAR(255)
            """)
            cur.execute("""
                ALTER TABLE tool_outputs
                ADD COLUMN IF NOT EXISTS status VARCHAR(32)
            """)
            cur.execute("""
                ALTER TABLE tool_outputs
                ADD COLUMN IF NOT EXISTS output JSONB
            """)
            cur.execute("""
                ALTER TABLE tool_outputs
                ADD COLUMN IF NOT EXISTS duration_ms INTEGER DEFAULT 0
            """)
            cur.execute("""
                ALTER TABLE tool_outputs
                ADD COLUMN IF NOT EXISTS timestamp TIMESTAMPTZ DEFAULT NOW()
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_tool_outputs_dataset
                ON tool_outputs(dataset_name, timestamp DESC)
            """)

            # Contract versioning table used by /contract* endpoints
            cur.execute("""
                CREATE TABLE IF NOT EXISTS contract_versions (
                    id SERIAL PRIMARY KEY,
                    dataset_name VARCHAR(255),
                    contract_path TEXT,
                    contract_content TEXT,
                    contract_hash VARCHAR(128),
                    created_by VARCHAR(255),
                    change_type VARCHAR(64),
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                ALTER TABLE contract_versions
                ADD COLUMN IF NOT EXISTS dataset_name VARCHAR(255)
            """)
            cur.execute("""
                ALTER TABLE contract_versions
                ADD COLUMN IF NOT EXISTS contract_path TEXT
            """)
            cur.execute("""
                ALTER TABLE contract_versions
                ADD COLUMN IF NOT EXISTS contract_content TEXT
            """)
            cur.execute("""
                ALTER TABLE contract_versions
                ADD COLUMN IF NOT EXISTS contract_hash VARCHAR(128)
            """)
            cur.execute("""
                ALTER TABLE contract_versions
                ADD COLUMN IF NOT EXISTS created_by VARCHAR(255)
            """)
            cur.execute("""
                ALTER TABLE contract_versions
                ADD COLUMN IF NOT EXISTS change_type VARCHAR(64)
            """)
            cur.execute("""
                ALTER TABLE contract_versions
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_contract_versions_dataset
                ON contract_versions(dataset_name, created_at DESC)
            """)

            # SLO History — per-run SLO compliance tracking
            cur.execute("""
                CREATE TABLE IF NOT EXISTS slo_history (
                    id SERIAL PRIMARY KEY,
                    run_id VARCHAR(64),
                    timestamp TIMESTAMPTZ DEFAULT NOW(),
                    dataset_name VARCHAR(255),
                    slo_name VARCHAR(255),
                    operator VARCHAR(16),
                    target_value DOUBLE PRECISION,
                    observed_value DOUBLE PRECISION,
                    status VARCHAR(16),
                    error_budget_burn DOUBLE PRECISION DEFAULT 0,
                    metadata JSONB DEFAULT '{}'::jsonb
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_slo_history_dataset
                ON slo_history(dataset_name, timestamp DESC)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_slo_history_run
                ON slo_history(run_id)
            """)

            # Async Jobs — background execution status for long-running operations
            cur.execute("""
                CREATE TABLE IF NOT EXISTS async_jobs (
                    job_id VARCHAR(64) PRIMARY KEY,
                    action VARCHAR(64) NOT NULL,
                    dataset_name VARCHAR(255) NOT NULL,
                    status VARCHAR(32) NOT NULL,
                    requested_at TIMESTAMPTZ DEFAULT NOW(),
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    request_json JSONB DEFAULT '{}'::jsonb,
                    result_json JSONB,
                    error_text TEXT
                )
            """)
            cur.execute("""
                ALTER TABLE async_jobs
                ADD COLUMN IF NOT EXISTS request_json JSONB DEFAULT '{}'::jsonb
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_async_jobs_requested
                ON async_jobs(requested_at DESC)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_async_jobs_status
                ON async_jobs(status, requested_at DESC)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_async_jobs_dataset
                ON async_jobs(dataset_name, requested_at DESC)
            """)

            # Incident lifecycle table (OPEN / ACK / RESOLVED)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS incidents (
                    incident_id VARCHAR(64) PRIMARY KEY,
                    run_id VARCHAR(64),
                    dataset_name VARCHAR(255) NOT NULL,
                    severity VARCHAR(32) NOT NULL,
                    status VARCHAR(16) NOT NULL DEFAULT 'OPEN',
                    owner VARCHAR(255),
                    title TEXT,
                    description TEXT,
                    quality_score DOUBLE PRECISION,
                    anomaly_count INTEGER DEFAULT 0,
                    z_score_max DOUBLE PRECISION DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    acknowledged_at TIMESTAMPTZ,
                    resolved_at TIMESTAMPTZ,
                    metadata JSONB DEFAULT '{}'::jsonb
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_incidents_created
                ON incidents(created_at DESC)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_incidents_status
                ON incidents(status, created_at DESC)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_incidents_dataset
                ON incidents(dataset_name, status, created_at DESC)
            """)

            # Diagnostics Warehouse — failed records and check-level context for faster triage.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS diagnostics_records (
                    id BIGSERIAL PRIMARY KEY,
                    run_id VARCHAR(64),
                    dataset_name VARCHAR(255) NOT NULL,
                    column_name VARCHAR(255),
                    check_type VARCHAR(128) NOT NULL,
                    severity VARCHAR(32) NOT NULL DEFAULT 'info',
                    violation_count INTEGER DEFAULT 0,
                    sample_records JSONB DEFAULT '[]'::jsonb,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_diagnostics_dataset
                ON diagnostics_records(dataset_name, created_at DESC)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_diagnostics_run
                ON diagnostics_records(run_id, created_at DESC)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_diagnostics_check
                ON diagnostics_records(check_type, created_at DESC)
            """)

            # Action Audit Log — structured operator/agent actions (for ops + UI timelines)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS action_audit_log (
                    id VARCHAR(64) PRIMARY KEY,
                    timestamp TIMESTAMPTZ DEFAULT NOW(),
                    actor VARCHAR(255),
                    source VARCHAR(64),
                    action VARCHAR(64) NOT NULL,
                    dataset_name VARCHAR(255),
                    status VARCHAR(32),
                    metadata JSONB DEFAULT '{}'::jsonb
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_action_audit_ts
                ON action_audit_log(timestamp DESC)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_action_audit_dataset
                ON action_audit_log(dataset_name, timestamp DESC)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_action_audit_action
                ON action_audit_log(action, timestamp DESC)
            """)

            # Agentic auto-remediation run-level state
            cur.execute("""
                CREATE TABLE IF NOT EXISTS agentic_remediation_runs (
                    id VARCHAR(64) PRIMARY KEY,
                    dataset_name VARCHAR(255) NOT NULL,
                    initial_run_id VARCHAR(64),
                    final_run_id VARCHAR(64),
                    status VARCHAR(32) NOT NULL,
                    attempt_count INTEGER DEFAULT 0,
                    policy_blocks INTEGER DEFAULT 0,
                    summary JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_agentic_runs_dataset
                ON agentic_remediation_runs(dataset_name, created_at DESC)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_agentic_runs_status
                ON agentic_remediation_runs(status, created_at DESC)
            """)

            # Agentic auto-remediation attempt-level trace
            cur.execute("""
                CREATE TABLE IF NOT EXISTS agentic_remediation_attempts (
                    id BIGSERIAL PRIMARY KEY,
                    remediation_run_id VARCHAR(64) NOT NULL,
                    attempt_no INTEGER NOT NULL,
                    input_run_id VARCHAR(64),
                    classification VARCHAR(64),
                    proposed_diff_summary TEXT,
                    confidence DOUBLE PRECISION,
                    applied BOOLEAN DEFAULT FALSE,
                    output_run_id VARCHAR(64),
                    result_status VARCHAR(32),
                    error TEXT,
                    details JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_agentic_attempts_run
                ON agentic_remediation_attempts(remediation_run_id, attempt_no ASC)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_agentic_attempts_dataset_run
                ON agentic_remediation_attempts(input_run_id, output_run_id)
            """)

    print("✅ PostgreSQL tables initialized")


def close_pool():
    """Shut down the connection pool (call on app shutdown)."""
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None

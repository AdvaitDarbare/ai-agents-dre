# Event-Driven Orchestration

This project supports event-driven quality gating in three ways:

1. Native watcher: `python3 -m src.runners.file_watcher`
2. Dagster sensor integration: `orchestration/dagster/defs.py`
3. Airflow DAG template: `orchestration/airflow/dre_event_dag.py`

For production-mirror execution, run async jobs in external worker mode:

1. API process: `ASYNC_JOB_EXECUTION_MODE=external_worker uvicorn src.api:app --reload`
2. Worker process: `ASYNC_JOB_EXECUTION_MODE=external_worker python3 -m src.runners.async_job_worker`

## Recommended Workflow

1. Data lands (`data/landing/` or upstream event)
2. Orchestrator triggers `POST /evaluate/{dataset}`
3. DRE returns one of:
   - `PASSED` / `WARNING`: continue pipeline
   - `BLOCKED`: fail gate, stop publish/load
   - `paused_hitl`: pause for contract approval/human review

## Why Event-Driven

- Lower latency than batch schedules
- Gate failures close to ingest time
- Better alignment with production pipeline controls

## Dagster Mode

See:

- `orchestration/dagster/README.md`
- `orchestration/dagster/defs.py`

Behavior:

- Sensor watches landing path
- New file triggers `dre_event_gate_job`
- Job calls DRE API and fails on `BLOCKED` / `paused_hitl`

## Airflow Mode

See:

- `orchestration/airflow/dre_event_dag.py`

Behavior:

- `FileSensor` waits for dataset file
- Python task calls DRE evaluate endpoint
- Branch task routes by verdict status

## Warehouse Load Position (Doris)

Current code can load to Doris in pipeline stage C (`src/pipeline/stages/action_stage.py`).
For strict external orchestration, you can treat DRE as gate-only and keep warehouse load in orchestrator branches.
Set `DRE_DORIS_LOAD_ENABLED=0` for gate-only mode.

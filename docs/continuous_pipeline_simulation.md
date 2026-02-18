# Continuous Pipeline Simulation (Single Dataset)

This runbook simulates a realistic continuous stream using **one dataset only**.
It is designed to test:

1. Normal baseline passes
2. Volume anomaly behavior
3. YAML constraint violations
4. Schema mismatch blocking
5. Recovery after failures

## Why this approach

- One dataset keeps the signal clear and avoids noisy multi-dataset test chaos.
- Sequential batches mirror production batch arrivals.
- The same contract is reused across all batches to test drift and enforcement over time.

## Script

`/Users/advaitdarbare/Desktop/ai-agents-dre/scripts/simulate_continuous_pipeline.py`

Default dataset name: `continuous_pipeline_demo`

## Prerequisites

1. Backend running:

```bash
uvicorn src.api:app --reload --port 8000
```

2. Watcher running (recommended for real event-driven behavior):

```bash
python -m src.runners.file_watcher
```

3. Optional for warehouse path:

```bash
export DRE_DORIS_LOAD_ENABLED=1
export DORIS_MOCK_MODE=False   # True for local simulation
```

## Run simulation

Watcher-driven mode with verdict wait:

```bash
python scripts/simulate_continuous_pipeline.py --wait-for-verdict --interval-seconds 4
```

If watcher is not running, trigger API evaluate per batch:

```bash
python scripts/simulate_continuous_pipeline.py --api-base http://127.0.0.1:8000 --wait-for-verdict
```

Dry run (no files written):

```bash
python scripts/simulate_continuous_pipeline.py --dry-run
```

## What gets generated

- Contract: `config/expectations/continuous_pipeline_demo.yaml`
- Landing files: `data/landing/continuous_pipeline_demo_###_<phase>.csv`

Planned phases:

1. `baseline_001` -> expected `PASSED`
2. `baseline_002` -> expected `PASSED`
3. `baseline_003` -> expected `PASSED`
4. `volume_spike` -> expected anomaly (`WARNING` likely)
5. `constraint_violation` -> expected YAML rule violations (`WARNING` or `BLOCKED`)
6. `schema_mismatch` -> expected `BLOCKED`
7. `recovery` -> expected `PASSED` and Doris load
8. `post_recovery_steady` -> expected `PASSED` and Doris load (flow continues)

## Validate outcomes

```bash
curl -s "http://127.0.0.1:8000/history/continuous_pipeline_demo?limit=20" | jq
curl -s "http://127.0.0.1:8000/metrics/continuous_pipeline_demo/timeseries?metric=row_count&limit=20" | jq
curl -s "http://127.0.0.1:8000/incidents?dataset_name=continuous_pipeline_demo&limit=20" | jq
```

# Dagster Event-Driven Integration

This folder provides a production-style event-driven quality gate:

- sensor watches `data/landing/`
- new file triggers DRE `POST /evaluate/{dataset}`
- run fails on `BLOCKED` or `paused_hitl`

## Prerequisites

- Running DRE API at `http://localhost:8000`
- Dagster installed in your orchestration environment

## Environment

```bash
export DRE_API_URL=http://localhost:8000
export DRE_WATCH_DIR=data/landing
```

## Run

From your Dagster project environment, load:

- `orchestration/dagster/defs.py`

The included definitions are:

- `dre_event_gate_job`
- `landing_file_sensor`

## Behavior Notes

- Dataset name is derived from filename (same convention as watcher).
- Sensor cursor tracks file fingerprint (`path + mtime + size`) to avoid duplicates.
- This integration gates pipeline progression using DRE verdict status.

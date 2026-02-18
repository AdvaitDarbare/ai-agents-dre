# MCP Server

This project includes a Model Context Protocol (MCP) server for agent/tool integration.

Implementation:
- `src/mcp/server.py`
- Service-backed tool logic in `src/services/reliability_service.py`

## Exposed Tools

- `evaluate_dataset(dataset_name)`
- `propose_contract(dataset_name, file_path?)`
- `run_contract_gate(dataset_name, file_path?)`
- `generate_contract_autopilot(dataset_name, file_path?, confidence_threshold=0.75)`
- `approve_contract(dataset_name, approved_yaml)`
- `get_pending_contracts()`
- `get_run_verdict(run_id)`
- `get_run_history(dataset_name, limit=50)`
- `compare_runs(run_id_1, run_id_2)`
- `investigate_anomaly(dataset_name, metric?, run_id?, history_window=30)`
- `explain_quality(dataset_name, run_id?)`
- `generate_ai_brief(dataset_name, run_id?)`
- `list_datasets_by_risk(limit=20)`
- `get_diagnostics_records(dataset_name, run_id?, check_type?, limit=200)`
- `get_outcome_metrics(days=30)`
- `get_workflow_timeline(dataset_name?, limit=100)`
- `get_agentic_workflow_graph()`
- `run_agentic_reliability_loop(dataset_name, metric?, auto_execute=false, confidence_threshold=0.8, policy_approved=false, policy_reason?)`
- `get_slo_summary(dataset_name, window=200)`
- `chat_with_copilot(query)`
- `delete_dataset(dataset_name, confirm=false)`

`approve_contract`/`delete /contracts/pending` resume a paused LangGraph HITL thread when one exists for the dataset.
`evaluate_dataset` now routes through the same LangGraph dispatch used by API/watcher:
- configured datasets: staged evaluate flow (`evaluate_pipeline -> persist_verdict -> apply_file_actions`)
- unconfigured datasets: durable HITL interrupt/resume contract path

`delete_dataset` requires `confirm=true`.

Policy gates (`approval_required` for HIGH/CRITICAL destructive actions) are currently enforced at FastAPI route layer. MCP tools call service methods directly and only enforce `confirm=true` for destructive delete tool calls.

Agentic execution safety:

- `run_agentic_reliability_loop` never auto-applies remediation unless:
  - remediation proposal exists,
  - confidence is above threshold,
  - and policy controls are satisfied (including approval reason when required).
- When controls are missing, the tool returns `approval_required` / `requires_hitl` decisions instead of mutating contracts.

Contract store backend is env-selectable:
- `CONTRACT_STORE_BACKEND=file` (default)
- `CONTRACT_STORE_BACKEND=git` (git-backed write path)

LangGraph tracing to LangSmith is optional and env-driven:
- `LANGSMITH_TRACING=true`
- `LANGSMITH_API_KEY=...`
- optional `LANGSMITH_PROJECT=...`

## Run (Streamable HTTP)

```bash
python3 -m src.mcp.server --transport streamable-http --host 0.0.0.0 --port 8001 --path /mcp
```

## Run (stdio)

```bash
python3 -m src.mcp.server --transport stdio
```

## Notes

- Streamable HTTP is the default transport.
- MCP server uses the same runtime wiring as backend (`MonitorAgent` + `FileContractStore` + `ReliabilityService`).

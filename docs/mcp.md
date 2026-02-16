# MCP Server

This project includes a Model Context Protocol (MCP) server for agent/tool integration.

Implementation:
- `src/mcp/server.py`
- Service-backed tool logic in `src/services/reliability_service.py`

## Exposed Tools

- `evaluate_dataset(dataset_name)`
- `propose_contract(dataset_name, file_path?)`
- `approve_contract(dataset_name, approved_yaml)`
- `get_pending_contracts()`
- `get_run_verdict(run_id)`
- `get_run_history(dataset_name, limit=50)`
- `get_slo_summary(dataset_name, window=200)`
- `chat_with_copilot(query)`
- `delete_dataset(dataset_name, confirm=false)`

`delete_dataset` requires `confirm=true`.

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

# personal-context-fast-mcp

Contained FastMCP Python server for Personal Context.

## What is migrated

- All Personal Context MCP tools from `personal-context-mcp`
  - `status_get`
  - `status_set_override`
  - `status_get_work`
  - `status_set_work`
  - `status_get_location`
  - `status_set_location`
  - `status_get_location_history`
  - `status_schedule_set`
  - `status_schedule_list`
  - `status_schedule_delete`
  - `holidays_list`
- Status/location/schedule resolution behavior
- Holiday cache behavior
- No UI/OAuth pages

## Project configuration (`fastmcp.json`)

This repository now includes a canonical `fastmcp.json` aligned with FastMCP project configuration docs:

- `source`: `server.py:mcp`
- `environment`: uv-managed Python environment from local `pyproject.toml`
- `deployment`: HTTP runtime defaults (`/mcp`) plus runtime env wiring

FastMCP CLI arguments still override config values when needed.

## Runtime env

- Core settings:
  - `DATABASE_URL` (default: `sqlite:///data/mcp.db`)
  - `LOCATION_STALE_HOURS` (default: `6`)
  - `HOLIDAY_FETCH_TIMEOUT_MS` (default: `5000`)

- Optional HTTP bearer auth:
  - `MCP_API_KEY` (single key), or
  - `MCP_API_KEYS` (comma-separated)
  - `BASE_URL` (if needed for token verifier metadata)

## Validate and run

```bash
# Validate tool discovery / entrypoint
fastmcp inspect fastmcp.json
fastmcp inspect server.py:mcp

# Run from project config
fastmcp run

# Override transport at runtime
fastmcp run --transport stdio
```

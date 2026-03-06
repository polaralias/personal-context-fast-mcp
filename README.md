# personal-context-fast-mcp

FastMCP Python server for Personal Context.

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

## Configuration

- `DATABASE_URL` (default: `sqlite:///data/mcp.db`)
- `LOCATION_STALE_HOURS` (default: `6`)
- `HOLIDAY_FETCH_TIMEOUT_MS` (default: `5000`)

Optional HTTP bearer auth:

- `MCP_API_KEY` (single key), or
- `MCP_API_KEYS` (comma-separated)

## Run

```bash
# HTTP (default)
python server.py

# stdio
FASTMCP_TRANSPORT=stdio python server.py
```

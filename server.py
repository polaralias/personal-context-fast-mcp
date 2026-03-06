from __future__ import annotations

import json
import os
import re
import secrets
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

from fastmcp import FastMCP
from fastmcp.server.auth import AccessToken, TokenVerifier
from starlette.responses import JSONResponse

HOLIDAY_URL = "https://www.gov.uk/bank-holidays.json"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_date(date_str: str) -> datetime:
    if not DATE_RE.match(date_str):
        raise ValueError("Invalid date format. Use YYYY-MM-DD")
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _parse_optional_datetime(value: str | None, label: str) -> datetime | None:
    if not value:
        return None
    parsed = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(parsed)
    except ValueError as exc:
        raise ValueError(f"Invalid {label} date") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _resolve_database_path() -> str:
    default = "sqlite:///data/mcp.db"
    raw = (os.getenv("DATABASE_URL") or default).strip()
    if raw in {":memory:", "sqlite::memory:", "file::memory:"}:
        return ":memory:"

    normalized = raw
    if normalized.startswith("sqlite:"):
        normalized = normalized[len("sqlite:") :]
    elif normalized.startswith("file:"):
        normalized = normalized[len("file:") :]

    if normalized.startswith("//"):
        normalized = normalized[2:]

    if normalized == ":memory:":
        return ":memory:"

    if normalized.startswith("/") and re.match(r"^[A-Za-z]:", normalized[1:3] or ""):
        normalized = normalized[1:]

    path = Path(normalized)
    if not path.is_absolute():
        path = Path.cwd() / path

    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path.resolve())


class StaticApiKeyVerifier(TokenVerifier):
    def __init__(self, api_keys: list[str], base_url: str | None = None) -> None:
        super().__init__(base_url=base_url)
        self._api_keys = [k for k in api_keys if k]

    async def verify_token(self, token: str) -> AccessToken | None:
        for key in self._api_keys:
            if secrets.compare_digest(token, key):
                return AccessToken(token=token, client_id="personal-context-fast-mcp", scopes=[])
        return None


@dataclass
class LocationRecord:
    latitude: float
    longitude: float
    location_name: str | None
    source: str
    timestamp: str


class PersonalContextStore:
    def __init__(self, database_path: str) -> None:
        self._conn = sqlite3.connect(database_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def _init_schema(self) -> None:
        statements = [
            """
            CREATE TABLE IF NOT EXISTS work_status_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER NOT NULL,
                source TEXT NOT NULL,
                status TEXT NOT NULL,
                reason TEXT,
                expires_at INTEGER
            )
            """,
            "CREATE INDEX IF NOT EXISTS work_status_events_created_at_idx ON work_status_events (created_at)",
            """
            CREATE TABLE IF NOT EXISTS location_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER NOT NULL,
                source TEXT NOT NULL,
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                name TEXT,
                expires_at INTEGER
            )
            """,
            "CREATE INDEX IF NOT EXISTS location_events_created_at_idx ON location_events (created_at)",
            """
            CREATE TABLE IF NOT EXISTS scheduled_status (
                date TEXT PRIMARY KEY,
                patch TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS bank_holidays_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT NOT NULL,
                year INTEGER NOT NULL,
                payload TEXT NOT NULL,
                fetched_at INTEGER NOT NULL,
                UNIQUE(region, year)
            )
            """,
        ]
        with self._lock:
            for statement in statements:
                self._conn.execute(statement)
            self._conn.commit()

    def insert_work_status(self, status: str, reason: str | None, ttl_seconds: int | None) -> dict[str, Any]:
        now = _now_utc()
        expires = now + timedelta(seconds=ttl_seconds) if ttl_seconds else None
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO work_status_events (created_at, source, status, reason, expires_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    int(now.timestamp() * 1000),
                    "manual",
                    status,
                    reason,
                    int(expires.timestamp() * 1000) if expires else None,
                ),
            )
            self._conn.commit()
        return {
            "source": "manual",
            "status": status,
            "reason": reason,
            "expiresAt": _to_iso(expires) if expires else None,
            "createdAt": _to_iso(now),
        }

    def insert_location(
        self,
        latitude: float,
        longitude: float,
        location_name: str | None,
        source: str,
        ttl_seconds: int | None,
    ) -> dict[str, Any]:
        now = _now_utc()
        expires = now + timedelta(seconds=ttl_seconds) if ttl_seconds else None
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO location_events (created_at, source, lat, lon, name, expires_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    int(now.timestamp() * 1000),
                    source,
                    latitude,
                    longitude,
                    location_name,
                    int(expires.timestamp() * 1000) if expires else None,
                ),
            )
            self._conn.commit()
        return {
            "source": source,
            "latitude": latitude,
            "longitude": longitude,
            "name": location_name,
            "expiresAt": _to_iso(expires) if expires else None,
            "createdAt": _to_iso(now),
        }

    def latest_valid_work_event(self, target: datetime) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                """
                SELECT *
                FROM work_status_events
                WHERE expires_at IS NULL OR expires_at > ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (int(target.timestamp() * 1000),),
            ).fetchone()

    def latest_work_event(self) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM work_status_events ORDER BY created_at DESC LIMIT 1"
            ).fetchone()

    def latest_location_event(self) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM location_events ORDER BY created_at DESC LIMIT 1"
            ).fetchone()

    def location_history(self, start: datetime | None, end: datetime | None, limit: int) -> list[sqlite3.Row]:
        clauses: list[str] = []
        params: list[Any] = []
        if start:
            clauses.append("created_at >= ?")
            params.append(int(start.timestamp() * 1000))
        if end:
            clauses.append("created_at <= ?")
            params.append(int(end.timestamp() * 1000))

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        query = f"SELECT * FROM location_events {where} ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        with self._lock:
            return self._conn.execute(query, params).fetchall()

    def upsert_schedule(
        self,
        date_str: str,
        work_status: str | None,
        location: dict[str, Any] | None,
        reason: str | None,
    ) -> dict[str, Any]:
        patch: dict[str, Any] = {}
        if work_status:
            patch["workStatus"] = work_status
        if location:
            patch["location"] = location
        if reason:
            patch["reason"] = reason

        now = _now_utc()
        patch_json = json.dumps(patch)

        with self._lock:
            self._conn.execute(
                """
                INSERT INTO scheduled_status (date, patch, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET patch = excluded.patch, updated_at = excluded.updated_at
                """,
                (date_str, patch_json, int(now.timestamp() * 1000), int(now.timestamp() * 1000)),
            )
            self._conn.commit()
            row = self._conn.execute(
                "SELECT * FROM scheduled_status WHERE date = ?",
                (date_str,),
            ).fetchone()

        return self._row_to_schedule(row)

    def list_schedules(self, start: str | None, end: str | None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if start:
            clauses.append("date >= ?")
            params.append(start)
        if end:
            clauses.append("date <= ?")
            params.append(end)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        query = f"SELECT * FROM scheduled_status {where} ORDER BY date"

        with self._lock:
            rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_schedule(row) for row in rows]

    def delete_schedule(self, date_str: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM scheduled_status WHERE date = ?", (date_str,))
            self._conn.commit()

    def get_schedule(self, date_str: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM scheduled_status WHERE date = ?",
                (date_str,),
            ).fetchone()
        return self._row_to_schedule(row) if row else None

    def holiday_cache(self, region: str, year: int) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM bank_holidays_cache WHERE region = ? AND year = ?",
                (region, year),
            ).fetchone()

    def upsert_holiday_cache(self, region: str, year: int, payload: list[dict[str, Any]]) -> None:
        now = _now_utc()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO bank_holidays_cache (region, year, payload, fetched_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(region, year) DO UPDATE SET
                  payload = excluded.payload,
                  fetched_at = excluded.fetched_at
                """,
                (region, year, json.dumps(payload), int(now.timestamp() * 1000)),
            )
            self._conn.commit()

    @staticmethod
    def _row_to_schedule(row: sqlite3.Row | None) -> dict[str, Any]:
        if row is None:
            return {}
        created = datetime.fromtimestamp(row["created_at"] / 1000, tz=timezone.utc)
        updated = datetime.fromtimestamp(row["updated_at"] / 1000, tz=timezone.utc)
        return {
            "date": row["date"],
            "patch": json.loads(row["patch"]),
            "createdAt": _to_iso(created),
            "updatedAt": _to_iso(updated),
        }


class HolidayService:
    def __init__(self, store: PersonalContextStore) -> None:
        self._store = store

    def fetch_holidays(self, region: str = "england-and-wales") -> list[dict[str, Any]]:
        year = _now_utc().year
        cached = self._store.holiday_cache(region, year)

        if cached:
            fetched_at = datetime.fromtimestamp(cached["fetched_at"] / 1000, tz=timezone.utc)
            if fetched_at > _now_utc() - timedelta(days=1):
                return json.loads(cached["payload"])

        timeout_ms = int(os.getenv("HOLIDAY_FETCH_TIMEOUT_MS", "5000") or "5000")
        timeout_s = max(timeout_ms, 1) / 1000

        try:
            request = Request(HOLIDAY_URL, method="GET")
            with urlopen(request, timeout=timeout_s) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if region not in payload:
                raise ValueError(f"Region {region} not found in holiday data")

            events = payload[region]["events"]
            self._store.upsert_holiday_cache(region, year, events)
            return events
        except (URLError, ValueError, TimeoutError, json.JSONDecodeError):
            if cached:
                return json.loads(cached["payload"])
            raise

    def is_bank_holiday(self, target: datetime, region: str = "england-and-wales") -> bool:
        try:
            holidays = self.fetch_holidays(region)
        except Exception:
            return False

        date_str = target.strftime("%Y-%m-%d")
        return any(item.get("date") == date_str for item in holidays)


class StatusResolver:
    def __init__(self, store: PersonalContextStore, holidays: HolidayService) -> None:
        self._store = store
        self._holidays = holidays

    def resolve(self, target: datetime | None = None) -> dict[str, Any]:
        date = (target or _now_utc()).astimezone(timezone.utc)
        date_str = date.strftime("%Y-%m-%d")
        now = _now_utc()

        is_weekend = date.weekday() >= 5
        is_holiday = self._holidays.is_bank_holiday(date)

        base_work_event = self._store.latest_valid_work_event(date)
        latest_location_event = self._store.latest_location_event()

        work_status = base_work_event["status"] if base_work_event else "off"

        if is_weekend or is_holiday:
            work_status = "off"

        schedule = self._store.get_schedule(date_str)
        patch = schedule.get("patch") if schedule else None
        if patch and patch.get("workStatus"):
            work_status = patch["workStatus"]

        if date_str == now.strftime("%Y-%m-%d"):
            latest = self._store.latest_work_event()
            if latest and latest["expires_at"] and latest["expires_at"] > int(now.timestamp() * 1000):
                work_status = latest["status"]

        location = None
        if latest_location_event:
            created = datetime.fromtimestamp(latest_location_event["created_at"] / 1000, tz=timezone.utc)
            expires_at = latest_location_event["expires_at"]
            stale_hours = float(os.getenv("LOCATION_STALE_HOURS", "6") or "6")
            stale_window = timedelta(hours=stale_hours if stale_hours > 0 else 6)

            expired = bool(expires_at and expires_at < int(now.timestamp() * 1000))
            stale = now - created > stale_window
            if not expired and not stale:
                location = LocationRecord(
                    latitude=latest_location_event["lat"],
                    longitude=latest_location_event["lon"],
                    location_name=latest_location_event["name"],
                    source=latest_location_event["source"],
                    timestamp=_to_iso(created),
                ).__dict__

        last_updated = _to_iso(now)
        if base_work_event:
            created = datetime.fromtimestamp(base_work_event["created_at"] / 1000, tz=timezone.utc)
            last_updated = _to_iso(created)

        return {
            "effectiveDate": date_str,
            "resolvedAt": _to_iso(now),
            "bankHoliday": is_holiday,
            "weekend": is_weekend,
            "workStatus": work_status,
            "location": location,
            "lastUpdated": last_updated,
        }


def _load_api_keys() -> list[str]:
    keys: list[str] = []
    single = os.getenv("MCP_API_KEY")
    if single:
        keys.append(single.strip())

    multi = os.getenv("MCP_API_KEYS")
    if multi:
        for raw in multi.split(","):
            token = raw.strip()
            if token:
                keys.append(token)

    return list(dict.fromkeys(keys))


database_path = _resolve_database_path()
store = PersonalContextStore(database_path)
holidays = HolidayService(store)
resolver = StatusResolver(store, holidays)

api_keys = _load_api_keys()
auth = StaticApiKeyVerifier(api_keys, base_url=os.getenv("BASE_URL")) if api_keys else None

server = FastMCP("personal-context-fast-mcp", auth=auth)


@server.custom_route("/", methods=["GET", "HEAD"], include_in_schema=False)
async def root_health(_request):
    return JSONResponse({"status": "ok", "server": "personal-context-fast-mcp"})


@server.custom_route("/health", methods=["GET", "HEAD"], include_in_schema=False)
async def health(_request):
    return JSONResponse({"status": "ok", "server": "personal-context-fast-mcp"})


@server.custom_route("/healthz", methods=["GET", "HEAD"], include_in_schema=False)
async def healthz(_request):
    return JSONResponse({"status": "ok", "server": "personal-context-fast-mcp"})


@server.tool
def status_get(date: str | None = None) -> dict[str, Any]:
    target = _parse_date(date) if date else None
    return resolver.resolve(target)


@server.tool
def status_set_override(status: str, reason: str | None = None, ttlSeconds: int | None = None) -> dict[str, Any]:
    store.insert_work_status(status, reason, ttlSeconds)
    return resolver.resolve()


@server.tool
def status_get_work(date: str | None = None) -> dict[str, Any]:
    target = _parse_date(date) if date else None
    resolved = resolver.resolve(target)
    return {
        "workStatus": resolved["workStatus"],
        "effectiveDate": resolved["effectiveDate"],
    }


@server.tool
def status_set_work(
    workStatus: str,
    reason: str | None = None,
    ttlSeconds: int | None = None,
) -> dict[str, Any]:
    store.insert_work_status(workStatus, reason, ttlSeconds)
    resolved = resolver.resolve()
    return {
        "workStatus": resolved["workStatus"],
        "effectiveDate": resolved["effectiveDate"],
    }


@server.tool
def status_get_location() -> dict[str, Any]:
    resolved = resolver.resolve()
    return {
        "location": resolved["location"],
        "effectiveDate": resolved["effectiveDate"],
    }


@server.tool
def status_set_location(
    latitude: float,
    longitude: float,
    locationName: str | None = None,
    source: str = "manual",
    ttlSeconds: int | None = None,
) -> dict[str, Any]:
    store.insert_location(latitude, longitude, locationName, source, ttlSeconds)
    resolved = resolver.resolve()
    return {
        "location": resolved["location"],
        "effectiveDate": resolved["effectiveDate"],
    }


@server.tool
def status_get_location_history(
    from_: str | None = None,
    to: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    start = _parse_optional_datetime(from_, "from")
    end = _parse_optional_datetime(to, "to")
    rows = store.location_history(start, end, limit or 50)

    events = []
    for row in rows:
        created = datetime.fromtimestamp(row["created_at"] / 1000, tz=timezone.utc)
        events.append(
            {
                "latitude": row["lat"],
                "longitude": row["lon"],
                "locationName": row["name"],
                "source": row["source"],
                "timestamp": _to_iso(created),
            }
        )

    return {"events": events}


@server.tool
def status_schedule_set(
    date: str,
    workStatus: str | None = None,
    location: dict[str, Any] | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    if not DATE_RE.match(date):
        raise ValueError("Invalid date format. Use YYYY-MM-DD")
    store.upsert_schedule(date, workStatus, location, reason)
    return {"success": True}


@server.tool
def status_schedule_list(from_: str | None = None, to: str | None = None) -> list[dict[str, Any]]:
    return store.list_schedules(from_, to)


@server.tool
def status_schedule_delete(date: str) -> dict[str, Any]:
    store.delete_schedule(date)
    return {"success": True}


@server.tool
def holidays_list(region: str | None = None) -> list[dict[str, Any]]:
    return holidays.fetch_holidays(region or "england-and-wales")


def main() -> None:
    transport_name = os.getenv("FASTMCP_TRANSPORT", "streamable-http").strip().lower()

    if transport_name == "stdio":
        server.run()
    else:
        host = os.getenv("HOST", "0.0.0.0")
        port = int(os.getenv("PORT", "8000"))
        server.run(transport=transport_name, host=host, port=port)


if __name__ == "__main__":
    main()

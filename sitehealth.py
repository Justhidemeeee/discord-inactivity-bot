"""On-demand health check for the DELTAChud wiki, used by /sitehealth.

Mirrors probe() in tools/uptime-check.cjs (Justhidemeeee/DELTAChud) so the bot
and the uptime monitor always agree. Three anonymous GETs, in order:

  1. /healthz                      — 200 + {"status": "ok"} (database-backed; the one that matters)
  2. /                             — 200
  3. /api/wiki?action=site-status  — 200 + boolean maintenanceEnabled

/livez is deliberately not used: it reports "alive" even when the database is down.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp

USER_AGENT = "DELTAChud-Bot-Health/1.0"
TIMEOUT_S = 15

UP, MAINTENANCE, DOWN = "UP", "MAINTENANCE", "DOWN"


@dataclass
class CheckResult:
    path: str
    ok: bool
    ms: Optional[int]       # None when no response arrived
    detail: str


@dataclass
class HealthReport:
    state: str              # UP / MAINTENANCE / DOWN
    reason: str
    checks: list[CheckResult]


async def _get(session: aiohttp.ClientSession, url: str) -> tuple[int, Optional[object], int]:
    """Return (status, parsed JSON or None, elapsed ms)."""
    start = time.monotonic()
    async with session.get(url, allow_redirects=False) as resp:
        body = await resp.read()
        ms = int((time.monotonic() - start) * 1000)
        try:
            data = json.loads(body)
        except ValueError:
            data = None
        return resp.status, data, ms


async def probe(base_url: str) -> HealthReport:
    base = base_url.rstrip("/")
    checks: list[CheckResult] = []
    timeout = aiohttp.ClientTimeout(total=TIMEOUT_S)
    async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": USER_AGENT}) as session:

        async def run(path: str, label: str, validate) -> Optional[str]:
            """Run one check; return a DOWN reason, or None if it passed."""
            try:
                status, data, ms = await _get(session, base + path)
            except asyncio.TimeoutError:
                reason = f"{label} timed out after {TIMEOUT_S} s"
                checks.append(CheckResult(path, False, None, "timed out"))
                return reason
            except aiohttp.ClientConnectorError:
                checks.append(CheckResult(path, False, None, "could not connect"))
                return f"Could not connect to {label}"
            except aiohttp.ClientError as e:
                checks.append(CheckResult(path, False, None, type(e).__name__))
                return f"{label} request failed ({type(e).__name__})"
            if status != 200:
                checks.append(CheckResult(path, False, ms, f"HTTP {status}"))
                return f"{label} returned HTTP {status}"
            problem = validate(data)
            checks.append(CheckResult(path, problem is None, ms, problem or "HTTP 200"))
            return f"{label}: {problem}" if problem else None

        def healthz_ok(data):
            if not isinstance(data, dict) or data.get("status") != "ok":
                return 'expected {"status":"ok"}'
            return None

        maintenance: dict[str, bool] = {}

        def site_status_ok(data):
            if not isinstance(data, dict) or not isinstance(data.get("maintenanceEnabled"), bool):
                return "missing boolean maintenanceEnabled"
            maintenance["on"] = data["maintenanceEnabled"]
            return None

        for path, label, validate in (
            ("/healthz", "/healthz", healthz_ok),
            ("/", "Homepage", lambda _: None),
            ("/api/wiki?action=site-status", "Site status", site_status_ok),
        ):
            reason = await run(path, label, validate)
            if reason:
                return HealthReport(DOWN, reason, checks)

    if maintenance.get("on"):
        return HealthReport(MAINTENANCE, "Site is up, but visitors see the maintenance page", checks)
    return HealthReport(UP, "All checks passed", checks)

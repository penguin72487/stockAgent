from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
import sqlite3
import sys
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

try:
    from downloader.openbb_archive_contracts import (
        DECLARED_LIMIT_STRICTLY_BELOW_CONTRACTS,
        ENTITLEMENT_CAPPED_NONPAGEABLE_CONTRACTS,
        FMP_MANIFEST_PAGINATED_ENDPOINTS,
        MANIFEST_SOURCE_CAP_LIMITS,
    )
except ModuleNotFoundError:  # Direct execution from scripts/.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from downloader.openbb_archive_contracts import (
        DECLARED_LIMIT_STRICTLY_BELOW_CONTRACTS,
        ENTITLEMENT_CAPPED_NONPAGEABLE_CONTRACTS,
        FMP_MANIFEST_PAGINATED_ENDPOINTS,
        MANIFEST_SOURCE_CAP_LIMITS,
    )

from stockagent.live.openbb_archive_dashboard import project_openbb_history_row


ACCEPTED_STATUSES = frozenset({"success", "empty"})
FRED_RELEASE_PAGE_SIZE = 1000
REQUIRED_ARCHIVE_COLUMNS = (
    "_openbb_endpoint",
    "_provider",
    "_scope_key",
    "_retrieved_at",
    "_query_json",
)
FMP_BASIC_DAILY_CALL_CAP = 250
QUOTA_FEASIBILITY_CRITICAL_DAYS = 365


def _active_local_cooldown_bypass_endpoints(
    state_dir: Path,
) -> set[tuple[str, str]]:
    """Return local bulk routes enabled by the live downloader invocation."""
    pid_path = state_dir / "downloader.pid"
    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip())
        command = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
    except (OSError, ValueError):
        return set()
    arguments = {
        item.decode("utf-8", errors="replace") for item in command if item
    }
    if not any(
        item.endswith("downloader/download_openbb_archive.py") for item in arguments
    ):
        return set()
    if "--bls-api-only" in arguments:
        return set()
    return {("bls", "economy.survey.bls_series")}


def _daily_quota_projection(
    provider: str,
    tier: str,
    provider_only_backlog: int,
    daily_call_cap: int,
    evidence_tasks: int,
) -> dict[str, Any] | None:
    """Return a fail-closed lower bound for a detected daily-call tier.

    One manifest task can consume more than one child HTTP call, so treating
    one task as one call is deliberately optimistic.  A projection that is
    already infeasible under this lower bound cannot be rescued by scheduler
    concurrency or per-second rate tuning.
    """
    backlog = max(0, int(provider_only_backlog))
    cap = max(1, int(daily_call_cap))
    evidence = max(0, int(evidence_tasks))
    if evidence == 0 or backlog == 0:
        return None
    minimum_days = (backlog + cap - 1) // cap
    return {
        "provider": provider,
        "detected_tier": tier,
        "provider_only_backlog_tasks": backlog,
        "daily_call_cap": cap,
        "minimum_days_at_call_cap": minimum_days,
        "minimum_years_at_call_cap": round(minimum_days / 365.25, 2),
        "evidence_tasks": evidence,
        "lower_bound_only": True,
    }


@contextmanager
def _sqlite_progress(
    connection: sqlite3.Connection,
    description: str,
    *,
    enabled: bool,
    vm_steps: int = 50_000,
):
    """Show an indeterminate work counter while SQLite scans the manifest."""
    progress = tqdm(
        total=None,
        desc=description[:64],
        unit="vm-batch",
        position=1,
        leave=False,
        mininterval=0.5,
        disable=not enabled,
    )

    def update() -> int:
        progress.update(1)
        return 0

    if enabled:
        connection.set_progress_handler(update, max(1_000, int(vm_steps)))
    try:
        yield
    finally:
        if enabled:
            connection.set_progress_handler(None, 0)
        progress.close()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect and audit the resumable OpenBB archive download."
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data_openBB"))
    parser.add_argument(
        "--max-total-attempts",
        type=int,
        default=20,
        help=(
            "Warning threshold for unusually repeated transient tasks; it is "
            "not a scheduler retry ceiling."
        ),
    )
    parser.add_argument("--stale-running-minutes", type=int, default=120)
    parser.add_argument(
        "--accepted-stall-minutes",
        type=int,
        default=120,
        help="Warn when no success/confirmed-empty task completes for this long.",
    )
    parser.add_argument(
        "--min-free-gib",
        type=float,
        default=100.0,
        help="Warn when free archive filesystem capacity falls below this value.",
    )
    parser.add_argument(
        "--audit-files",
        action="store_true",
        help="Open every successful Parquet shard.",
    )
    parser.add_argument("--write-snapshot", action="store_true")
    parser.add_argument("--append-history", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--field", help="Print one top-level scalar field only.")
    parser.add_argument("--fail-on-incomplete", action="store_true")
    parser.add_argument("--no-progress", action="store_true")
    return parser.parse_args(argv)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _open_read_only(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise FileNotFoundError(f"OpenBB manifest does not exist: {path}")
    connection = sqlite3.connect(
        f"file:{path.resolve()}?mode=ro", uri=True, timeout=60.0
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _read_pid(path: Path, expected_fragment: str) -> tuple[int | None, bool]:
    try:
        pid = int(path.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, ValueError, OSError):
        return None, False
    proc = Path("/proc") / str(pid)
    try:
        command = (
            (proc / "cmdline")
            .read_bytes()
            .replace(b"\0", b" ")
            .decode("utf-8", errors="replace")
        )
        os.kill(pid, 0)
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return pid, False
    return pid, expected_fragment in command


def _proc_runtime_metrics(pid: int | None) -> dict[str, int]:
    """Read lightweight Linux process resource metrics without external tools."""
    if pid is None:
        return {}
    proc_dir = Path("/proc") / str(pid)
    try:
        status_text = (proc_dir / "status").read_text(encoding="utf-8")
    except OSError:
        return {}
    raw: dict[str, int] = {}
    for line in status_text.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        token = value.strip().split()[0] if value.strip() else ""
        if token.isdigit():
            raw[key] = int(token)
    try:
        open_fds = sum(1 for _ in (proc_dir / "fd").iterdir())
    except OSError:
        open_fds = 0
    return {
        "rss_bytes": raw.get("VmRSS", 0) * 1024,
        "rss_peak_bytes": raw.get("VmHWM", 0) * 1024,
        "virtual_bytes": raw.get("VmSize", 0) * 1024,
        "swap_bytes": raw.get("VmSwap", 0) * 1024,
        "threads": raw.get("Threads", 0),
        "open_fds": open_fds,
    }


def _coverage_counts(output_dir: Path) -> dict[str, int]:
    path = output_dir / "catalog" / "coverage.parquet"
    if not path.is_file():
        return {}
    table = pq.read_table(path, columns=["decision"])
    return dict(
        sorted(
            Counter(str(value.as_py()) for value in table.column("decision")).items()
        )
    )


def _completeness_contract_summary(output_dir: Path) -> dict[str, Any]:
    path = output_dir / "catalog" / "completeness_contract_summary.json"
    if not path.is_file():
        # Old/test manifests predate the contract artifact.  A planner running
        # the current schema always writes it before downloads resume.
        return {
            "present": False,
            "passed": True,
            "unresolved": 0,
            "unresolved_by_axis": {},
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        return {
            "present": True,
            "passed": False,
            "unresolved": 1,
            "unresolved_by_axis": {"artifact": 1},
            "error": f"{type(exc).__name__}: {str(exc)[:300]}",
        }
    return {
        **payload,
        "present": True,
        "passed": bool(payload.get("passed", False)),
    }


def _active_plan_token(connection: sqlite3.Connection) -> str | None:
    table = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='archive_meta'"
    ).fetchone()
    if table is None:
        return None
    row = connection.execute(
        "SELECT value FROM archive_meta WHERE key='active_plan_token'"
    ).fetchone()
    return str(row[0]) if row is not None else None


def _active_where(plan_token: str | None) -> tuple[str, tuple[str, ...]]:
    clause = "active=1"
    parameters: tuple[str, ...] = ()
    if plan_token is not None:
        clause += " AND plan_token=?"
        parameters = (plan_token,)
    return clause, parameters


def _fred_release_pagination_gaps(
    connection: sqlite3.Connection,
    plan_token: str | None,
    *,
    show_progress: bool = False,
) -> tuple[int, list[dict[str, Any]]]:
    """Find full FRED release pages whose required continuation is absent."""
    active_where, active_parameters = _active_where(plan_token)
    with _sqlite_progress(
        connection,
        "monitor:load FRED pagination",
        enabled=show_progress,
    ):
        rows = connection.execute(
            f"""
        SELECT scope_key,kwargs_json,rows
        FROM tasks
        WHERE {active_where}
          AND endpoint='economy.fred_search'
          AND status='success' AND rows>=?
          AND scope_key LIKE 'release=%'
        ORDER BY scope_key
        """,
            (*active_parameters, FRED_RELEASE_PAGE_SIZE),
        ).fetchall()
        scopes = {
            str(row[0])
            for row in connection.execute(
                f"""
            SELECT scope_key FROM tasks
            WHERE {active_where} AND endpoint='economy.fred_search'
            """,
                active_parameters,
            )
        }
    gaps: list[dict[str, Any]] = []
    progress = tqdm(
        rows,
        total=len(rows),
        desc="monitor:verify FRED pages",
        unit="page",
        position=1,
        leave=False,
        disable=not show_progress,
    )
    try:
        for row in progress:
            kwargs = json.loads(str(row["kwargs_json"]))
            release_id = kwargs.get("release_id")
            if release_id is None:
                continue
            next_offset = int(kwargs.get("offset") or 0) + FRED_RELEASE_PAGE_SIZE
            expected_scope = f"release={int(release_id)}/offset={next_offset:07d}"
            if expected_scope not in scopes:
                gaps.append(
                    {
                        "scope_key": str(row["scope_key"]),
                        "rows": int(row["rows"]),
                        "expected_scope_key": expected_scope,
                    }
                )
    finally:
        progress.close()
    return len(gaps), gaps[:50]


def _fmp_manifest_pagination_gaps(
    connection: sqlite3.Connection,
    plan_token: str | None,
    *,
    show_progress: bool = False,
) -> tuple[int, list[dict[str, Any]]]:
    """Find full FMP pages whose next manifest page is absent.

    A successful page at the requested limit is not terminal evidence.  The
    next page must exist in the active plan and eventually prove termination
    with a short or empty response.  This mirrors startup reconciliation but
    remains read-only so the monitor independently blocks false completion.
    """
    active_where, active_parameters = _active_where(plan_token)
    endpoints = tuple(sorted(FMP_MANIFEST_PAGINATED_ENDPOINTS))
    placeholders = ",".join("?" for _ in endpoints)
    with _sqlite_progress(
        connection,
        "monitor:load FMP pagination",
        enabled=show_progress,
    ):
        rows = connection.execute(
            f"""
        SELECT endpoint,scope_key,kwargs_json,rows
        FROM tasks
        WHERE {active_where}
          AND endpoint IN ({placeholders})
          AND selected_provider='fmp'
          AND status='success'
          AND CAST(json_extract(kwargs_json,'$.limit') AS INTEGER)>0
          AND rows>=CAST(json_extract(kwargs_json,'$.limit') AS INTEGER)
        ORDER BY endpoint,scope_key
        """,
            (*active_parameters, *endpoints),
        ).fetchall()
        scopes = {
            (str(row["endpoint"]), str(row["scope_key"]))
            for row in connection.execute(
                f"""
            SELECT endpoint,scope_key FROM tasks
            WHERE {active_where} AND endpoint IN ({placeholders})
            """,
                (*active_parameters, *endpoints),
            )
        }
    gaps: list[dict[str, Any]] = []
    progress = tqdm(
        rows,
        total=len(rows),
        desc="monitor:verify FMP pages",
        unit="page",
        position=1,
        leave=False,
        disable=not show_progress,
    )
    try:
        for row in progress:
            kwargs = json.loads(str(row["kwargs_json"]))
            page = int(kwargs.get("page") or 0)
            limit = int(kwargs.get("limit") or 0)
            scope_key = str(row["scope_key"])
            expected_scope = re.sub(r"page=\d+", f"page={page + 1}", scope_key)
            if expected_scope == scope_key:
                expected_scope = f"{scope_key}/page={page + 1}"
            endpoint = str(row["endpoint"])
            if (endpoint, expected_scope) not in scopes:
                gaps.append(
                    {
                        "endpoint": endpoint,
                        "scope_key": scope_key,
                        "rows": int(row["rows"]),
                        "limit": limit,
                        "expected_scope_key": expected_scope,
                    }
                )
    finally:
        progress.close()
    return len(gaps), gaps[:50]


def _retryable_failure_clusters(
    connection: sqlite3.Connection,
    plan_token: str | None,
    *,
    max_total_attempts: int,
    show_progress: bool = False,
) -> list[dict[str, Any]]:
    """Group failed signatures; pending anomalies use aggregate attempt alerts."""
    active_where, active_parameters = _active_where(plan_token)
    with _sqlite_progress(
        connection,
        "monitor:cluster retryable failures",
        enabled=show_progress,
    ):
        rows = connection.execute(
            f"""
        SELECT endpoint,COALESCE(selected_provider,'') provider,error,
               COUNT(*) tasks,MIN(attempts) min_attempts,MAX(attempts) max_attempts
        FROM tasks
        WHERE {active_where}
          AND status='failed' AND attempts<? AND error IS NOT NULL
        GROUP BY endpoint,COALESCE(selected_provider,''),error
        ORDER BY tasks DESC,endpoint,provider
        LIMIT 50
        """,
            (*active_parameters, max(1, int(max_total_attempts))),
        ).fetchall()
    return [
        {
            "endpoint": str(row["endpoint"]),
            "provider": str(row["provider"]),
            "tasks": int(row["tasks"]),
            "min_attempts": int(row["min_attempts"]),
            "max_attempts": int(row["max_attempts"]),
            "error": str(row["error"])[:500],
        }
        for row in rows
    ]


def _non_authoritative_empty_clusters(
    connection: sqlite3.Connection,
    plan_token: str | None,
    *,
    show_progress: bool = False,
) -> list[dict[str, Any]]:
    """Find terminal empties whose last provider result is not a data fact.

    A fallback chain may contain earlier transient errors and still end in an
    authoritative EmptyDataError. Only the final combined-error segment is
    decisive here. Quota, network, provider-boundary, or route-permission
    errors in that position mean the task must remain retryable rather than
    being counted as a confirmed empty archive partition.
    """
    active_where, active_parameters = _active_where(plan_token)
    with _sqlite_progress(
        connection,
        "monitor:check terminal empty evidence",
        enabled=show_progress,
    ):
        rows = connection.execute(
            f"""
            SELECT endpoint,COALESCE(selected_provider,'') provider,error,
                   COUNT(*) tasks
            FROM tasks
            WHERE {active_where}
              AND status='empty' AND COALESCE(error,'')!=''
            GROUP BY endpoint,COALESCE(selected_provider,''),error
            """,
            active_parameters,
        ).fetchall()
    markers = (
        "hourly request allocation",
        "daily request allocation",
        "too many requests",
        "limit reach",
        "-> 429",
        "http 429",
        "status 429",
        "timeouterror",
        "timed out",
        "could not resolve",
        "temporary failure in name resolution",
        "name resolution error",
        "connection reset",
        "connection aborted",
        "__archive_yfinance_transport__",
        "must be between",
        "start date must be",
        "permission to access the news api",
    )
    clusters: list[dict[str, Any]] = []
    for row in rows:
        error = str(row["error"] or "")
        final_segment = error.rsplit(" | ", 1)[-1]
        lowered = final_segment.lower()
        # OpenBB's retail-prices EmptyDataError includes generic wording that
        # mentions possible rate limiting. The explicit exception type remains
        # authoritative empty evidence, so do not infer from that prose.
        if "emptydataerror" in lowered:
            continue
        if not any(marker in lowered for marker in markers):
            continue
        clusters.append(
            {
                "endpoint": str(row["endpoint"]),
                "provider": str(row["provider"]),
                "tasks": int(row["tasks"]),
                "final_error": final_segment[:500],
            }
        )
    clusters.sort(key=lambda item: (-int(item["tasks"]), item["endpoint"]))
    return clusters[:50]


def _non_authoritative_unavailable_clusters(
    connection: sqlite3.Connection,
    plan_token: str | None,
    *,
    show_progress: bool = False,
) -> list[dict[str, Any]]:
    """Find unavailable rows ending in retryable or adaptable evidence."""
    active_where, active_parameters = _active_where(plan_token)
    with _sqlite_progress(
        connection,
        "monitor:check terminal unavailable evidence",
        enabled=show_progress,
    ):
        rows = connection.execute(
            f"""
            SELECT endpoint,COALESCE(selected_provider,'') provider,error,
                   COUNT(*) tasks
            FROM tasks
            WHERE {active_where}
              AND status='unavailable' AND COALESCE(error,'')!=''
            GROUP BY endpoint,COALESCE(selected_provider,''),error
            """,
            active_parameters,
        ).fetchall()
    markers = (
        "hourly request allocation",
        "daily request allocation",
        "too many requests",
        "limit reach",
        "-> 429",
        "http 429",
        "status 429",
        "timeouterror",
        "timed out",
        "could not resolve",
        "temporary failure in name resolution",
        "name resolution error",
        "connection reset",
        "connection aborted",
        "__archive_yfinance_transport__",
        "must be between",
        "start date must be",
        "permission to access the news api",
    )
    clusters: list[dict[str, Any]] = []
    for row in rows:
        error = str(row["error"] or "")
        final_segment = error.rsplit(" | ", 1)[-1]
        lowered = final_segment.lower()
        if not any(marker in lowered for marker in markers):
            continue
        clusters.append(
            {
                "endpoint": str(row["endpoint"]),
                "provider": str(row["provider"]),
                "tasks": int(row["tasks"]),
                "final_error": final_segment[:500],
            }
        )
    clusters.sort(key=lambda item: (-int(item["tasks"]), item["endpoint"]))
    return clusters[:50]


def _is_authoritative_unavailable_evidence(value: object) -> bool:
    """Return whether text positively proves a stable capability denial.

    ``unavailable`` is a stronger terminal claim than a failed request. It
    therefore needs positive provider/credential/market evidence, not merely
    the absence of a timeout marker. Keep this predicate deliberately narrow:
    quota, transport, and adaptable query-boundary messages never satisfy it.
    """
    text = str(value or "").lower()
    if not text:
        return False
    return any(
        marker in text
        for marker in (
            "restricted endpoint",
            "not available under your current subscription",
            "not available under current subscription",
            "premium query parameter",
            "http 402",
            "-> 402",
            "payment required",
            "permission to access the news api",
            "missing credential",
            "missing api key",
            "api key is required",
            "invalid api key",
            "invalid registration key",
        )
    ) or ("observed " in text and " distinct " in text and "zero successful" in text)


def _provider_capability_evidence(
    state_dir: Path,
) -> tuple[
    dict[str, str],
    dict[tuple[str, str], str],
    dict[tuple[str, str, str], str],
    list[dict[str, str]],
]:
    """Load only provider capability checkpoints backed by hard evidence."""
    path = state_dir / "provider_cooldowns.json"
    if not path.is_file():
        return {}, {}, {}, []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return {}, {}, {}, []

    providers: dict[str, str] = {}
    routes: dict[tuple[str, str], str] = {}
    domains: dict[tuple[str, str, str], str] = {}
    invalid: list[dict[str, str]] = []

    raw_providers = payload.get("unavailable_providers", {})
    if isinstance(raw_providers, dict):
        for provider, reason in raw_providers.items():
            provider_name = str(provider).strip()
            reason_text = str(reason or "")
            if provider_name and _is_authoritative_unavailable_evidence(reason_text):
                providers[provider_name] = reason_text
            elif provider_name:
                invalid.append(
                    {
                        "scope": "provider",
                        "provider": provider_name,
                        "endpoint": "",
                        "domain": "",
                        "reason": reason_text[:500],
                    }
                )

    for key in ("unavailable_routes", "unavailable_domains"):
        raw_items = payload.get(key, [])
        if not isinstance(raw_items, list):
            continue
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            provider = str(item.get("provider") or "").strip()
            endpoint = str(item.get("endpoint") or "").strip()
            domain = str(item.get("domain") or "").strip()
            reason = str(item.get("reason") or "")
            valid = bool(
                provider and endpoint and _is_authoritative_unavailable_evidence(reason)
            )
            if key == "unavailable_domains":
                valid = valid and bool(domain)
                if valid:
                    domains[(provider, endpoint, domain)] = reason
            elif valid:
                routes[(provider, endpoint)] = reason
            if not valid and (provider or endpoint or domain):
                invalid.append(
                    {
                        "scope": "domain" if domain else "route",
                        "provider": provider,
                        "endpoint": endpoint,
                        "domain": domain,
                        "reason": reason[:500],
                    }
                )
    return providers, routes, domains, invalid


def _capability_domain(provider: object, kwargs_json: object) -> str | None:
    """Mirror the downloader's provider/route/market capability partition."""
    try:
        kwargs = json.loads(str(kwargs_json or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(kwargs, dict):
        return None
    provider_name = str(provider or "")
    raw_symbol = str(kwargs.get("symbol") or "").strip().upper()
    symbols = [item.strip() for item in raw_symbol.split(",") if item.strip()]
    if provider_name == "tiingo":
        return (
            "tw"
            if symbols
            and all(
                symbol.endswith(".TW") or symbol.endswith(".TWO") for symbol in symbols
            )
            else None
        )
    if provider_name != "fmp":
        return None
    if not raw_symbol:
        return "global"
    if symbols and all(
        symbol.endswith(".TW") or symbol.endswith(".TWO") for symbol in symbols
    ):
        return "tw"
    return "us"


def _unproven_unavailable_clusters(
    connection: sqlite3.Connection,
    plan_token: str | None,
    state_dir: Path,
    *,
    show_progress: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Find terminal unavailable tasks lacking positive capability evidence."""
    providers, routes, domains, invalid_constraints = _provider_capability_evidence(
        state_dir
    )

    def provider_is_proven(
        provider: object, endpoint: object, kwargs_json: object
    ) -> int:
        provider_name = str(provider or "")
        endpoint_name = str(endpoint or "")
        if provider_name in providers or (provider_name, endpoint_name) in routes:
            return 1
        domain = _capability_domain(provider_name, kwargs_json)
        return int(
            domain is not None and (provider_name, endpoint_name, domain) in domains
        )

    connection.create_function(
        "openbb_authoritative_unavailable_evidence",
        1,
        lambda value: int(_is_authoritative_unavailable_evidence(value)),
        deterministic=True,
    )
    task_columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(tasks)")
    }
    evidence_json = (
        "tasks.provider_evidence_json"
        if "provider_evidence_json" in task_columns
        else "'{}'"
    )
    connection.create_function(
        "openbb_provider_capability_proven",
        3,
        provider_is_proven,
        deterministic=True,
    )
    active_where, active_parameters = _active_where(plan_token)
    with _sqlite_progress(
        connection,
        "monitor:prove terminal unavailable claims",
        enabled=show_progress,
    ):
        rows = connection.execute(
            f"""
            SELECT endpoint,COALESCE(selected_provider,'') provider,error,
                   COUNT(*) tasks
            FROM tasks
            WHERE {active_where}
              AND status='unavailable'
              AND (
                  (
                      NOT EXISTS (
                          SELECT 1
                          FROM json_each(tasks.provider_outcomes_json) AS outcome
                          WHERE outcome.value='unavailable'
                      )
                      AND openbb_authoritative_unavailable_evidence(error)=0
                  )
                  OR EXISTS (
                      SELECT 1
                      FROM json_each(tasks.provider_outcomes_json) AS outcome
                      WHERE outcome.value='unavailable'
                        AND openbb_provider_capability_proven(
                            outcome.key,tasks.endpoint,tasks.kwargs_json
                        )=0
                        AND openbb_authoritative_unavailable_evidence(
                            COALESCE(
                                (
                                    SELECT evidence.value
                                    FROM json_each({evidence_json}) AS evidence
                                    WHERE evidence.key=outcome.key
                                ),
                                ''
                            )
                        )=0
                        AND NOT (
                            COALESCE(selected_provider,'')=outcome.key
                            AND openbb_authoritative_unavailable_evidence(error)=1
                        )
                  )
              )
            GROUP BY endpoint,COALESCE(selected_provider,''),error
            ORDER BY tasks DESC,endpoint,provider
            LIMIT 50
            """,
            active_parameters,
        ).fetchall()
    return (
        [
            {
                "endpoint": str(row["endpoint"]),
                "provider": str(row["provider"]),
                "tasks": int(row["tasks"]),
                "error": str(row["error"] or "")[:500],
            }
            for row in rows
        ],
        invalid_constraints,
    )


def _source_cap_saturations(
    connection: sqlite3.Connection,
    plan_token: str | None,
    *,
    show_progress: bool = False,
) -> list[dict[str, Any]]:
    """Reject realized partitions that may be truncated at a provider cap."""
    if not MANIFEST_SOURCE_CAP_LIMITS:
        return []
    active_where, active_parameters = _active_where(plan_token)
    endpoints = tuple(sorted(MANIFEST_SOURCE_CAP_LIMITS))
    placeholders = ",".join("?" for _ in endpoints)
    with _sqlite_progress(
        connection,
        "monitor:check source caps",
        enabled=show_progress,
    ):
        rows = connection.execute(
            f"""
        SELECT endpoint,scope_key,rows,selected_provider
        FROM tasks
        WHERE {active_where}
          AND status='success' AND endpoint IN ({placeholders})
        ORDER BY endpoint,scope_key
        """,
            (*active_parameters, *endpoints),
        ).fetchall()
    return [
        {
            "endpoint": str(row["endpoint"]),
            "scope_key": str(row["scope_key"]),
            "provider": str(row["selected_provider"] or ""),
            "rows": int(row["rows"]),
            "cap": int(MANIFEST_SOURCE_CAP_LIMITS[str(row["endpoint"])]),
        }
        for row in rows
        if int(row["rows"]) >= int(MANIFEST_SOURCE_CAP_LIMITS[str(row["endpoint"])])
    ][:50]


def _declared_limit_saturations(
    connection: sqlite3.Connection,
    plan_token: str | None,
    *,
    show_progress: bool = False,
) -> list[dict[str, Any]]:
    """Reject bounded-cardinality results that hit their declared ceiling.

    Manifest-paginated routes prove completion with a terminal short page and
    are audited separately.  Full-document/unbounded routes likewise have a
    different protocol proof.  This check covers every provider route whose
    contract says the complete result must be smaller than its finite limit,
    so a shared adapter regression cannot silently truncate one market while
    another market happens to expose the same failure first.
    """
    if not DECLARED_LIMIT_STRICTLY_BELOW_CONTRACTS:
        return []
    active_where, active_parameters = _active_where(plan_token)
    clauses = " OR ".join(
        "(endpoint=? AND selected_provider=?)"
        for _ in DECLARED_LIMIT_STRICTLY_BELOW_CONTRACTS
    )
    contract_parameters = tuple(
        value
        for endpoint, provider in sorted(DECLARED_LIMIT_STRICTLY_BELOW_CONTRACTS)
        for value in (endpoint, provider)
    )
    with _sqlite_progress(
        connection,
        "monitor:check declared limits",
        enabled=show_progress,
    ):
        rows = connection.execute(
            f"""
            SELECT endpoint,scope_key,rows,selected_provider,
                   CAST(json_extract(kwargs_json,'$.limit') AS INTEGER) AS cap
            FROM tasks
            WHERE {active_where}
              AND status='success'
              AND ({clauses})
              AND json_type(kwargs_json,'$.limit') IN ('integer','real')
              AND CAST(json_extract(kwargs_json,'$.limit') AS INTEGER)>0
              AND rows>=CAST(json_extract(kwargs_json,'$.limit') AS INTEGER)
            ORDER BY endpoint,scope_key
            LIMIT 50
            """,
            (*active_parameters, *contract_parameters),
        ).fetchall()
    return [
        {
            "endpoint": str(row["endpoint"]),
            "scope_key": str(row["scope_key"]),
            "provider": str(row["selected_provider"] or ""),
            "rows": int(row["rows"]),
            "cap": int(row["cap"]),
        }
        for row in rows
    ]


def _partition_entitlement_limit_saturations(
    saturations: Sequence[dict[str, Any]],
    parameter_maximums: Sequence[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Separate proven non-pageable entitlement caps from truncation bugs."""
    learned_limits = {
        (
            str(item.get("endpoint") or ""),
            str(item.get("provider") or ""),
            str(item.get("parameter") or ""),
        ): int(item.get("maximum") or 0)
        for item in parameter_maximums
        if int(item.get("maximum") or 0) > 0
    }
    entitlement: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    for item in saturations:
        key = (str(item["endpoint"]), str(item["provider"]))
        learned = learned_limits.get((*key, "limit"))
        if (
            key in ENTITLEMENT_CAPPED_NONPAGEABLE_CONTRACTS
            and learned is not None
            and int(item["cap"]) == learned
        ):
            entitlement.append(
                {
                    **item,
                    "constraint": "nonpageable_current_entitlement_limit",
                }
            )
        else:
            unresolved.append(item)
    return entitlement, unresolved


def _audit_catalog_followups(
    connection: sqlite3.Connection,
    *,
    plan_token: str | None,
    show_progress: bool,
) -> dict[str, Any]:
    """Rebuild expected child scopes from successful catalog Parquet files."""
    active_where, active_parameters = _active_where(plan_token)
    catalog_endpoints = (
        "cftc.cot_search",
        "currency.search",
        "economy.available_indicators",
        "economy.fred_search",
        "economy.survey.bls_search",
        "equity.fundamental.filings",
        "index.available",
        "regulators.sec.cik_map",
        "uscongress.amendments",
        "uscongress.bills",
    )
    paginated_endpoints = (
        "equity.discovery.filings",
        "equity.fundamental.filings",
        "equity.ownership.major_holders",
        "news.company",
        "news.world",
    )
    placeholders = ",".join("?" for _ in catalog_endpoints)
    pagination_placeholders = ",".join("?" for _ in paginated_endpoints)
    parent_rows = connection.execute(
        f"""
        SELECT task_id,endpoint,scope_key,kwargs_json,selected_provider,
               output_path,rows
        FROM tasks
        WHERE {active_where} AND status='success'
          AND (
              endpoint IN ({placeholders})
              OR (
                  endpoint IN ({pagination_placeholders})
                  AND selected_provider='fmp'
              )
          )
        ORDER BY endpoint,task_id
        """,
        (*active_parameters, *catalog_endpoints, *paginated_endpoints),
    ).fetchall()
    enabled_endpoints = {
        str(row[0])
        for row in connection.execute(
            f"SELECT DISTINCT endpoint FROM tasks WHERE {active_where}",
            active_parameters,
        )
    }
    expected: dict[str, set[str]] = {}
    skipped_disabled_children = 0
    parent_read_failures = 0
    samples: list[dict[str, str]] = []

    def first(record: dict[str, Any], names: Sequence[str]) -> Any:
        for name in names:
            value = record.get(name)
            if value not in {None, ""}:
                return value
        return None

    def require(child_endpoint: str, scope_key: str) -> None:
        nonlocal skipped_disabled_children
        if child_endpoint not in enabled_endpoints:
            skipped_disabled_children += 1
            return
        expected.setdefault(child_endpoint, set()).add(scope_key)

    progress = tqdm(
        parent_rows,
        total=len(parent_rows),
        desc="openbb:audit catalog parents",
        unit="file",
        position=1,
        leave=False,
        disable=not show_progress,
    )
    try:
        for parent in progress:
            endpoint = str(parent["endpoint"])
            scope_key = str(parent["scope_key"])
            try:
                kwargs = json.loads(str(parent["kwargs_json"]))
                records = pq.read_table(str(parent["output_path"])).to_pylist()
            except Exception as exc:
                parent_read_failures += 1
                if len(samples) < 50:
                    samples.append(
                        {
                            "parent_task_id": str(parent["task_id"]),
                            "parent_endpoint": endpoint,
                            "expected_endpoint": "-",
                            "expected_scope_key": "-",
                            "issue": (
                                "parent_unreadable "
                                f"{type(exc).__name__}: {str(exc)[:200]}"
                            ),
                        }
                    )
                continue

            if endpoint == "cftc.cot_search":
                report_type = str(kwargs.get("report_type") or "legacy")
                mode = (
                    "futures" if bool(kwargs.get("futures_only", False)) else "combined"
                )
                for record in records:
                    code = first(record, ("code", "cftc_contract_market_code"))
                    if code not in {None, ""}:
                        require(
                            "cftc.cot",
                            f"report={report_type}/mode={mode}/code={code}",
                        )
            elif endpoint == "economy.fred_search" and scope_key.startswith("release="):
                for record in records:
                    series_id = first(record, ("series_id", "symbol", "id"))
                    if series_id not in {None, ""}:
                        require("economy.fred_series", str(series_id))
            elif endpoint == "economy.survey.bls_search":
                series_ids: list[str] = []
                seen_series: set[str] = set()
                for record in records:
                    if record.get("_bls_record_type") == "code_map":
                        continue
                    series_id = first(record, ("series_id", "symbol", "code"))
                    normalized = str(series_id).strip().upper() if series_id else ""
                    if normalized and normalized not in seen_series:
                        seen_series.add(normalized)
                        series_ids.append(normalized)
                for batch_index, offset in enumerate(range(0, len(series_ids), 50)):
                    count = len(series_ids[offset : offset + 50])
                    require(
                        "economy.survey.bls_series",
                        f"{scope_key}/batch={batch_index:05d}/n={count}",
                    )
            elif endpoint == "economy.available_indicators":
                for record in records:
                    symbol = first(record, ("symbol", "indicator", "code", "series_id"))
                    if symbol not in {None, ""}:
                        require("economy.indicators", str(symbol))
            elif endpoint == "regulators.sec.cik_map":
                for record in records:
                    cik = first(record, ("cik",))
                    if cik not in {None, ""}:
                        require("regulators.sec.symbol_map", str(cik).strip())
            elif endpoint == "index.available":
                for record in records:
                    symbol = first(record, ("symbol", "code"))
                    if symbol not in {None, ""}:
                        require("index.price.historical", str(symbol))
            elif endpoint == "currency.search":
                for record in records:
                    symbol = first(record, ("symbol", "code"))
                    if symbol not in {None, ""}:
                        require("currency.price.historical", str(symbol))
            elif endpoint in {"uscongress.bills", "uscongress.amendments"}:
                child_endpoint = (
                    "uscongress.bill_info"
                    if endpoint == "uscongress.bills"
                    else "uscongress.amendment_info"
                )
                names = (
                    ("url", "bill_url")
                    if endpoint == "uscongress.bills"
                    else ("url", "amendment_url")
                )
                for record in records:
                    url = first(record, names)
                    if url not in {None, ""}:
                        require(child_endpoint, str(url))
            elif endpoint == "equity.fundamental.filings":
                for record in records:
                    url = first(record, ("filing_url", "report_url", "url"))
                    if url and "sec.gov" in str(url).lower():
                        require("regulators.sec.filing_headers", str(url))

            if endpoint in paginated_endpoints and parent["selected_provider"] == "fmp":
                limit = int(kwargs.get("limit") or 0)
                page = int(kwargs.get("page") or 0)
                if limit > 0 and int(parent["rows"]) >= limit:
                    next_scope = re.sub(r"page=\d+", f"page={page + 1}", scope_key)
                    if next_scope == scope_key:
                        next_scope = f"{scope_key}/page={page + 1}"
                    require(endpoint, next_scope)
    finally:
        progress.close()

    required_counts = {endpoint: len(scopes) for endpoint, scopes in expected.items()}
    actual_progress = tqdm(
        expected.items(),
        total=len(expected),
        desc="openbb:audit catalog children",
        unit="endpoint",
        position=1,
        leave=False,
        disable=not show_progress,
    )
    try:
        for child_endpoint, missing_scopes in actual_progress:
            for row in connection.execute(
                f"SELECT scope_key FROM tasks WHERE {active_where} AND endpoint=?",
                (*active_parameters, child_endpoint),
            ):
                missing_scopes.discard(str(row[0]))
    finally:
        actual_progress.close()

    missing_counts = {
        endpoint: len(scopes) for endpoint, scopes in expected.items() if scopes
    }
    for child_endpoint, scopes in expected.items():
        for scope in sorted(scopes):
            if len(samples) >= 50:
                break
            samples.append(
                {
                    "parent_task_id": "-",
                    "parent_endpoint": "-",
                    "expected_endpoint": child_endpoint,
                    "expected_scope_key": scope,
                    "issue": "missing_followup_task",
                }
            )
    required_followups = sum(required_counts.values())
    missing_followups = sum(missing_counts.values())
    return {
        "checked_parent_files": len(parent_rows),
        "required_unique_followups": required_followups,
        "required_by_endpoint": dict(sorted(required_counts.items())),
        "missing_followups": missing_followups,
        "missing_by_endpoint": dict(sorted(missing_counts.items())),
        "parent_read_failures": parent_read_failures,
        "skipped_disabled_children": skipped_disabled_children,
        "issue_samples": samples,
        "passed": parent_read_failures == 0 and missing_followups == 0,
    }


def _audit_success_files(
    connection: sqlite3.Connection,
    *,
    output_dir: Path,
    plan_token: str | None,
    show_progress: bool,
) -> dict[str, Any]:
    active_where, active_parameters = _active_where(plan_token)
    total = int(
        connection.execute(
            f"SELECT COUNT(*) FROM tasks WHERE {active_where} AND status='success'",
            active_parameters,
        ).fetchone()[0]
    )
    integrity = connection.execute(
        f"""
        SELECT
            SUM(CASE WHEN rows<=0 THEN 1 ELSE 0 END) zero_rows,
            SUM(
                CASE WHEN selected_provider IS NULL OR selected_provider='' THEN 1
                ELSE 0 END
            ) missing_provider,
            SUM(CASE WHEN output_path IS NULL OR output_path='' THEN 1 ELSE 0 END)
                missing_output_path
        FROM tasks WHERE {active_where} AND status='success'
        """,
        active_parameters,
    ).fetchone()
    duplicate_path_rows = connection.execute(
        f"""
        SELECT output_path,COUNT(*) count,MIN(task_id) sample_task_id
        FROM tasks
        WHERE {active_where} AND status='success'
          AND output_path IS NOT NULL AND output_path!=''
        GROUP BY output_path HAVING COUNT(*)>1
        ORDER BY output_path
        """,
        active_parameters,
    ).fetchall()
    success_zero_rows = int(integrity["zero_rows"] or 0)
    success_missing_provider = int(integrity["missing_provider"] or 0)
    success_missing_output_path = int(integrity["missing_output_path"] or 0)
    duplicate_output_paths = len(duplicate_path_rows)
    success_paths = {
        Path(str(row[0])).resolve()
        for row in connection.execute(
            f"SELECT output_path FROM tasks WHERE {active_where} "
            "AND status='success' AND output_path IS NOT NULL "
            "AND output_path!=''",
            active_parameters,
        )
    }
    non_success_parquet_files: list[Path] = []
    data_dir = output_dir / "data"
    orphan_progress = tqdm(
        data_dir.rglob("*.parquet") if data_dir.is_dir() else (),
        desc="openbb:audit non-success shards",
        unit="file",
        position=1,
        leave=False,
        disable=not show_progress,
    )
    try:
        for path in orphan_progress:
            if path.resolve() not in success_paths:
                non_success_parquet_files.append(path)
            if orphan_progress.n % 1000 == 0:
                orphan_progress.set_postfix(
                    non_success=len(non_success_parquet_files), refresh=False
                )
    finally:
        orphan_progress.close()
    cursor = connection.execute(
        f"SELECT task_id,endpoint,scope_key,kwargs_json,selected_provider,"
        f"output_path,rows FROM tasks "
        f"WHERE {active_where} AND status='success' ORDER BY endpoint,task_id",
        active_parameters,
    )
    missing = 0
    unreadable = 0
    row_mismatch = 0
    metadata_mismatch = 0
    encoded_wrapper = 0
    checked_rows = 0
    samples: list[dict[str, str]] = [
        {
            "task_id": str(row["sample_task_id"]),
            "path": str(row["output_path"]),
            "issue": f"duplicate_output_path tasks={int(row['count'])}",
        }
        for row in duplicate_path_rows[:50]
    ]
    samples.extend(
        {
            "task_id": "",
            "path": str(path),
            "issue": "parquet_exists_for_non_success_task",
        }
        for path in non_success_parquet_files[: max(0, 50 - len(samples))]
    )
    progress = tqdm(
        total=total,
        desc="openbb:audit parquet",
        unit="file",
        position=1,
        leave=False,
        disable=not show_progress,
    )
    try:
        for row in cursor:
            path = Path(str(row["output_path"]))
            issue: str | None = None
            if not path.is_file():
                missing += 1
                issue = "missing"
            else:
                try:
                    parquet_file = pq.ParquetFile(path)
                    parquet_rows = int(parquet_file.metadata.num_rows)
                    checked_rows += parquet_rows
                    if parquet_rows != int(row["rows"]):
                        row_mismatch += 1
                        issue = f"row_mismatch manifest={row['rows']} parquet={parquet_rows}"
                    missing_columns = sorted(
                        set(REQUIRED_ARCHIVE_COLUMNS)
                        - set(parquet_file.schema_arrow.names)
                    )
                    if issue is None and missing_columns:
                        metadata_mismatch += 1
                        issue = f"missing_archive_columns={missing_columns}"
                    if (
                        issue is None
                        and parquet_rows == 1
                        and {"result", "metadata"}.issubset(
                            parquet_file.schema_arrow.names
                        )
                    ):
                        wrapper = (
                            parquet_file.read_row_group(
                                0, columns=["result", "metadata"]
                            )
                            .slice(0, 1)
                            .to_pylist()[0]
                        )
                        raw_result = wrapper.get("result")
                        raw_metadata = wrapper.get("metadata")
                        if (
                            isinstance(raw_result, str)
                            and raw_result.lstrip().startswith(("[", "{"))
                            and isinstance(raw_metadata, str)
                            and raw_metadata.lstrip().startswith("{")
                        ):
                            encoded_wrapper += 1
                            issue = "encoded_result_wrapper_not_row_normalized"
                    if issue is None and parquet_rows > 0:
                        metadata = (
                            parquet_file.read_row_group(
                                0, columns=list(REQUIRED_ARCHIVE_COLUMNS)
                            )
                            .slice(0, 1)
                            .to_pylist()[0]
                        )
                        expected = {
                            "_openbb_endpoint": str(row["endpoint"]),
                            "_provider": str(row["selected_provider"]),
                            "_scope_key": str(row["scope_key"]),
                            "_query_json": str(row["kwargs_json"]),
                        }
                        differences = {
                            key: {"manifest": value, "parquet": metadata.get(key)}
                            for key, value in expected.items()
                            if metadata.get(key) != value
                        }
                        if not metadata.get("_retrieved_at"):
                            differences["_retrieved_at"] = {
                                "manifest": "non-empty",
                                "parquet": metadata.get("_retrieved_at"),
                            }
                        if differences:
                            metadata_mismatch += 1
                            issue = f"archive_metadata_mismatch={differences}"
                except (
                    Exception
                ) as exc:  # Corrupt files can raise several Arrow/OS exception types.
                    unreadable += 1
                    issue = f"unreadable {type(exc).__name__}: {str(exc)[:300]}"
            if issue is not None and len(samples) < 50:
                samples.append(
                    {"task_id": str(row["task_id"]), "path": str(path), "issue": issue}
                )
            progress.update(1)
            if progress.n % 1000 == 0:
                progress.set_postfix(
                    missing=missing,
                    unreadable=unreadable,
                    mismatch=row_mismatch,
                    metadata=metadata_mismatch,
                    refresh=False,
                )
    finally:
        progress.close()
    return {
        "checked_files": total,
        "checked_rows": checked_rows,
        "missing_files": missing,
        "unreadable_files": unreadable,
        "row_mismatch_files": row_mismatch,
        "metadata_mismatch_files": metadata_mismatch,
        "encoded_wrapper_files": encoded_wrapper,
        "success_zero_row_tasks": success_zero_rows,
        "success_missing_provider_tasks": success_missing_provider,
        "success_missing_output_path_tasks": success_missing_output_path,
        "duplicate_output_paths": duplicate_output_paths,
        "non_success_parquet_files": len(non_success_parquet_files),
        "issue_samples": samples,
        "passed": (
            missing == 0
            and unreadable == 0
            and row_mismatch == 0
            and metadata_mismatch == 0
            and encoded_wrapper == 0
            and success_zero_rows == 0
            and success_missing_provider == 0
            and success_missing_output_path == 0
            and duplicate_output_paths == 0
            and not non_success_parquet_files
        ),
    }


def collect_status(
    output_dir: Path,
    *,
    max_total_attempts: int = 20,
    stale_running_minutes: int = 120,
    accepted_stall_minutes: int = 120,
    min_free_gib: float = 100.0,
    audit_files: bool = False,
    show_progress: bool = True,
) -> dict[str, Any]:
    state_dir = output_dir / "_state"
    manifest_path = state_dir / "openbb_archive.sqlite3"
    connection = _open_read_only(manifest_path)
    connection.execute("BEGIN")
    task_columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(tasks)")
    }
    execution_tracking_available = "execution_started_at" in task_columns
    retry_tracking_available = {
        "retry_not_before",
        "transient_failures",
    }.issubset(task_columns)
    plan_token = _active_plan_token(connection)
    active_where, active_parameters = _active_where(plan_token)
    actionable_provider = """
        EXISTS (
            SELECT 1 FROM json_each(tasks.providers_json) AS provider
            WHERE NOT EXISTS (
                SELECT 1 FROM json_each(tasks.provider_outcomes_json) AS outcome
                WHERE outcome.key=provider.value
            )
        )
    """
    now = _utc_now()
    now_iso = now.isoformat()
    if retry_tracking_available:
        retry_eligible_expression = (
            "(retry_not_before IS NULL OR retry_not_before<=?)"
        )
        retry_deferred_expression = (
            "CASE WHEN status='pending' AND retry_not_before>? THEN 1 ELSE 0 END"
        )
        retry_deadline_expression = (
            "CASE WHEN status='pending' AND retry_not_before>? "
            "THEN retry_not_before ELSE NULL END"
        )
        retry_aggregate_parameters: tuple[str, ...] = (
            now_iso,
            now_iso,
            now_iso,
        )
    else:
        retry_eligible_expression = "1"
        retry_deferred_expression = "0"
        retry_deadline_expression = "NULL"
        retry_aggregate_parameters = ()
    # Read provider cooldowns before the single manifest aggregate so pristine
    # tasks can be classified from their remaining provider chain. A task does
    # not need an old per-row 429/error string to prove why it is deferred.
    active_provider_cooldowns = _active_provider_cooldowns(state_dir, now)
    cooldown_provider_names = tuple(sorted(active_provider_cooldowns))
    if cooldown_provider_names:
        cooldown_placeholders = ",".join("?" for _ in cooldown_provider_names)
        runtime_cooldown_expression = f"""
            SUM(
                CASE
                    WHEN status='pending'
                      AND ({actionable_provider})
                      AND NOT EXISTS (
                          SELECT 1
                          FROM json_each(tasks.providers_json) AS remaining_provider
                          WHERE NOT EXISTS (
                              SELECT 1
                              FROM json_each(tasks.provider_outcomes_json) AS outcome
                              WHERE outcome.key=remaining_provider.value
                          )
                            AND remaining_provider.value NOT IN (
                                {cooldown_placeholders}
                            )
                      )
                    THEN 1 ELSE 0
                END
            )
        """
    else:
        runtime_cooldown_expression = "0"
    stale_before = (now - timedelta(minutes=max(1, stale_running_minutes))).isoformat()
    recent_cutoff = (now - timedelta(minutes=15)).isoformat()
    status_progress = tqdm(
        total=6,
        desc="openbb:monitor",
        unit="stage",
        disable=not show_progress,
    )
    if execution_tracking_available:
        buffered_running_expression = (
            "CASE WHEN status='running' AND execution_started_at IS NULL "
            "THEN 1 ELSE 0 END"
        )
        executing_expression = (
            "CASE WHEN status='running' AND execution_started_at IS NOT NULL "
            "THEN 1 ELSE 0 END"
        )
        stale_buffered_expression = (
            "CASE WHEN status='running' AND execution_started_at IS NULL "
            "AND updated_at < ? THEN 1 ELSE 0 END"
        )
        stale_executing_expression = (
            "CASE WHEN status='running' AND execution_started_at IS NOT NULL "
            "AND execution_started_at < ? THEN 1 ELSE 0 END"
        )
        stale_parameters: tuple[str, ...] = (stale_before, stale_before)
    else:
        # Backward-compatible observation while an older downloader process is
        # still using a pre-migration manifest.  The next clean supervisor
        # start adds the timestamp column without requiring a destructive DB
        # rewrite during an active run.
        buffered_running_expression = "0"
        executing_expression = "CASE WHEN status='running' THEN 1 ELSE 0 END"
        stale_buffered_expression = "0"
        stale_executing_expression = (
            "CASE WHEN status='running' AND updated_at < ? THEN 1 ELSE 0 END"
        )
        stale_parameters = (stale_before,)
    try:
        # Keep the read snapshot short while the downloader is rapidly writing
        # millions of plan rows.  One grouped scan replaces the previous set of
        # status/attempt/time scans, which otherwise pinned a very large WAL for
        # much of every one-minute monitoring interval.
        status_progress.set_postfix(stage="aggregate manifest", refresh=False)
        with _sqlite_progress(
            connection,
            "monitor:aggregate manifest",
            enabled=show_progress,
        ):
            aggregate_rows = connection.execute(
                """
            SELECT
                status,
                endpoint,
                category,
                selected_provider,
                COUNT(*) count,
                COALESCE(SUM(CASE WHEN status='success' THEN rows ELSE 0 END),0) success_rows,
                SUM(
                    CASE WHEN status='pending' AND ({actionable_provider})
                              AND {retry_eligible_expression}
                         THEN 1 ELSE 0 END
                ) pending_eligible,
                SUM({retry_deferred_expression}) pending_retry_deferred,
                MIN({retry_deadline_expression}) next_task_retry_at,
                SUM(CASE WHEN status='pending' AND error IS NOT NULL THEN 1 ELSE 0 END) pending_with_error,
                SUM(
                    CASE
                        WHEN status='pending' AND error LIKE '%cooldown until%' THEN 1
                        ELSE 0
                    END
                ) pending_cooldown,
                SUM(
                    CASE
                        WHEN status='pending' AND error IS NOT NULL AND (
                            LOWER(error) LIKE '%cooldown until%'
                            OR LOWER(error) LIKE '%429%'
                            OR LOWER(error) LIKE '%limit reach%'
                            OR LOWER(error) LIKE '%rate limit%'
                            OR LOWER(error) LIKE '%too many requests%'
                            OR LOWER(error) LIKE '%daily limit%'
                            OR LOWER(error) LIKE '%quota%'
                        ) THEN 1 ELSE 0
                    END
                ) pending_rate_limited,
                SUM(CASE WHEN status='pending' AND attempts > 0 THEN 1 ELSE 0 END) pending_attempted,
                SUM(
                    CASE WHEN provider_outcomes_json!='{{}}' THEN 1 ELSE 0 END
                ) tasks_with_provider_outcomes,
                SUM(
                    CASE WHEN provider_outcomes_json LIKE '%:"empty"%' THEN 1
                    ELSE 0 END
                ) tasks_with_empty_provider_outcomes,
                SUM(
                    CASE WHEN provider_outcomes_json LIKE '%:"unavailable"%' THEN 1
                    ELSE 0 END
                ) tasks_with_unavailable_provider_outcomes,
                SUM(
                    CASE WHEN provider_outcomes_json LIKE '%:"permanent"%' THEN 1
                    ELSE 0 END
                ) tasks_with_permanent_provider_outcomes,
                SUM(
                    CASE
                        WHEN status='pending'
                          AND LOWER(error) LIKE '%provider concurrency capacity busy%'
                        THEN 1 ELSE 0
                    END
                ) provider_capacity_deferred,
                SUM(
                    CASE
                        WHEN status='pending' AND providers_json='["fmp"]' THEN 1
                        ELSE 0
                    END
                ) fmp_only_pending,
                SUM(
                    CASE
                        WHEN status='pending' AND LOWER(error) LIKE '%fmp:%' AND (
                            LOWER(error) LIKE '%cooldown until%'
                            OR LOWER(error) LIKE '%429%'
                            OR LOWER(error) LIKE '%limit reach%'
                            OR LOWER(error) LIKE '%rate limit%'
                            OR LOWER(error) LIKE '%too many requests%'
                            OR LOWER(error) LIKE '%daily limit%'
                            OR LOWER(error) LIKE '%quota%'
                        ) THEN 1 ELSE 0
                    END
                ) fmp_quota_deferred,
                SUM(
                    CASE
                        WHEN LOWER(error) LIKE '%fmp:%'
                          AND LOWER(error) LIKE '%limit reach%'
                        THEN 1 ELSE 0
                    END
                ) fmp_basic_limit_reach_evidence,
                SUM(
                    CASE
                        WHEN status='pending'
                          AND endpoint='economy.survey.bls_series'
                          AND LOWER(error) LIKE '%bls:%' AND (
                              LOWER(error) LIKE '%cooldown until%'
                              OR LOWER(error) LIKE '%429%'
                              OR LOWER(error) LIKE '%daily threshold%'
                              OR LOWER(error) LIKE '%daily limit%'
                              OR LOWER(error) LIKE '%quota%'
                        ) THEN 1 ELSE 0
                    END
                ) bls_quota_deferred,
                SUM(
                    CASE
                        WHEN status='pending' AND LOWER(error) LIKE '%sec:%' AND (
                            LOWER(error) LIKE '%cooldown until%'
                            OR LOWER(error) LIKE '%429%'
                            OR LOWER(error) LIKE '%rate limit%'
                            OR LOWER(error) LIKE '%too many requests%'
                        ) THEN 1 ELSE 0
                    END
                ) sec_rate_limit_deferred,
                SUM(
                    CASE
                        WHEN status='pending' AND endpoint='news.company'
                          AND LOWER(error) LIKE '%tiingo:%'
                          AND LOWER(error) LIKE '%permission to access the news api%'
                        THEN 1 ELSE 0
                    END
                ) tiingo_news_permission_pending,
                {runtime_cooldown_expression} runtime_provider_cooldown_deferred,
                SUM(CASE WHEN status='failed' AND ({actionable_provider}) THEN 1 ELSE 0 END) failed_retryable,
                SUM(
                    CASE
                        WHEN status='exhausted' THEN 1 ELSE 0
                    END
                ) exhausted,
                SUM(
                    CASE
                        WHEN status IN ('pending','failed') AND attempts >= ?
                        THEN 1 ELSE 0
                    END
                ) high_attempt,
                SUM({buffered_running_expression}) buffered_running,
                SUM({executing_expression}) executing,
                SUM({stale_buffered_expression}) stale_buffered,
                SUM({stale_executing_expression}) stale_executing,
                SUM(
                    CASE
                        WHEN status IN ('success','empty') AND updated_at >= ? THEN 1
                        ELSE 0
                    END
                ) recent_tasks,
                COALESCE(
                    SUM(
                        CASE
                            WHEN status IN ('success','empty') AND updated_at >= ? THEN rows
                            ELSE 0
                        END
                    ),
                    0
                ) recent_rows,
                MAX(updated_at) last_updated,
                MAX(
                    CASE WHEN status IN ('success','empty') THEN updated_at ELSE NULL END
                ) last_accepted_updated
            FROM tasks
            WHERE {active_where}
            GROUP BY status,endpoint,category,selected_provider
            """.format(
                    active_where=active_where,
                    actionable_provider=actionable_provider,
                    retry_eligible_expression=retry_eligible_expression,
                    retry_deferred_expression=retry_deferred_expression,
                    retry_deadline_expression=retry_deadline_expression,
                    buffered_running_expression=buffered_running_expression,
                    executing_expression=executing_expression,
                    stale_buffered_expression=stale_buffered_expression,
                    stale_executing_expression=stale_executing_expression,
                    runtime_cooldown_expression=runtime_cooldown_expression,
                ),
                (
                    *retry_aggregate_parameters,
                    *cooldown_provider_names,
                    max_total_attempts,
                    *stale_parameters,
                    recent_cutoff,
                    recent_cutoff,
                    *active_parameters,
                ),
            ).fetchall()
        status_progress.update(1)
        status_progress.set_postfix(stage="summarize statuses", refresh=False)
        status_counts: dict[str, int] = {}
        endpoint_names: set[str] = set()
        endpoint_status_counts: dict[str, dict[str, int]] = {}
        category_status_counts: dict[str, dict[str, int]] = {}
        category_success_rows: dict[str, int] = {}
        accepted_by_provider: dict[str, dict[str, int]] = {}
        endpoint_last_accepted: dict[str, str] = {}
        endpoint_blockers: dict[str, dict[str, int]] = {}
        active_endpoint_counts: dict[str, int] = {}
        recent_by_category: dict[str, dict[str, int]] = {}
        recent_by_provider: dict[str, dict[str, int]] = {}
        total_rows = 0
        pending_eligible = 0
        pending_retry_deferred = 0
        next_task_retry_at = None
        pending_with_error = 0
        pending_cooldown = 0
        pending_rate_limited = 0
        pending_attempted = 0
        tasks_with_provider_outcomes = 0
        tasks_with_empty_provider_outcomes = 0
        tasks_with_unavailable_provider_outcomes = 0
        tasks_with_permanent_provider_outcomes = 0
        permanent_provider_outcomes_by_endpoint: dict[str, int] = {}
        provider_capacity_deferred = 0
        fmp_only_pending = 0
        fmp_quota_deferred = 0
        fmp_basic_limit_reach_evidence = 0
        bls_quota_deferred = 0
        sec_rate_limit_deferred = 0
        tiingo_news_permission_pending = 0
        failed_retryable = 0
        exhausted = 0
        high_attempt = 0
        buffered_running = 0
        executing = 0
        stale_buffered = 0
        stale_executing = 0
        recent_tasks = 0
        recent_rows = 0
        last_updated = None
        last_accepted_updated = None
        for row in aggregate_rows:
            status_name = str(row["status"])
            endpoint_name = str(row["endpoint"])
            category_name = str(row["category"])
            selected_provider = row["selected_provider"]
            count = int(row["count"])
            status_counts[status_name] = status_counts.get(status_name, 0) + count
            category_counts_for_status = category_status_counts.setdefault(
                category_name, {}
            )
            category_counts_for_status[status_name] = (
                category_counts_for_status.get(status_name, 0) + count
            )
            category_success_rows[category_name] = category_success_rows.get(
                category_name, 0
            ) + int(row["success_rows"] or 0)
            if selected_provider and status_name in ACCEPTED_STATUSES:
                provider_accepted = accepted_by_provider.setdefault(
                    str(selected_provider),
                    {
                        "accepted_tasks": 0,
                        "success_tasks": 0,
                        "empty_tasks": 0,
                        "rows": 0,
                    },
                )
                provider_accepted["accepted_tasks"] += count
                provider_accepted[f"{status_name}_tasks"] += count
                provider_accepted["rows"] += int(row["success_rows"] or 0)
            endpoint_names.add(endpoint_name)
            endpoint_counts_for_status = endpoint_status_counts.setdefault(
                endpoint_name, {}
            )
            endpoint_counts_for_status[status_name] = (
                endpoint_counts_for_status.get(status_name, 0) + count
            )
            blockers = endpoint_blockers.setdefault(
                endpoint_name,
                {
                    "provider_capacity_deferred": 0,
                    "fmp_only_pending": 0,
                    "fmp_quota_deferred": 0,
                    "bls_quota_deferred": 0,
                    "sec_rate_limit_deferred": 0,
                    "tiingo_news_permission_pending": 0,
                    "runtime_provider_cooldown_deferred": 0,
                },
            )
            for blocker_name in blockers:
                blockers[blocker_name] += int(row[blocker_name] or 0)
            if status_name == "running":
                active_endpoint_counts[endpoint_name] = (
                    active_endpoint_counts.get(endpoint_name, 0) + count
                )
            total_rows += int(row["success_rows"] or 0)
            pending_eligible += int(row["pending_eligible"] or 0)
            pending_retry_deferred += int(row["pending_retry_deferred"] or 0)
            row_retry_at = row["next_task_retry_at"]
            if row_retry_at is not None and (
                next_task_retry_at is None or str(row_retry_at) < next_task_retry_at
            ):
                next_task_retry_at = str(row_retry_at)
            pending_with_error += int(row["pending_with_error"] or 0)
            pending_cooldown += int(row["pending_cooldown"] or 0)
            pending_rate_limited += int(row["pending_rate_limited"] or 0)
            pending_attempted += int(row["pending_attempted"] or 0)
            tasks_with_provider_outcomes += int(
                row["tasks_with_provider_outcomes"] or 0
            )
            tasks_with_empty_provider_outcomes += int(
                row["tasks_with_empty_provider_outcomes"] or 0
            )
            tasks_with_unavailable_provider_outcomes += int(
                row["tasks_with_unavailable_provider_outcomes"] or 0
            )
            row_permanent_outcomes = int(
                row["tasks_with_permanent_provider_outcomes"] or 0
            )
            tasks_with_permanent_provider_outcomes += row_permanent_outcomes
            if row_permanent_outcomes:
                permanent_provider_outcomes_by_endpoint[endpoint_name] = (
                    permanent_provider_outcomes_by_endpoint.get(endpoint_name, 0)
                    + row_permanent_outcomes
                )
            provider_capacity_deferred += int(row["provider_capacity_deferred"] or 0)
            fmp_only_pending += int(row["fmp_only_pending"] or 0)
            fmp_quota_deferred += int(row["fmp_quota_deferred"] or 0)
            fmp_basic_limit_reach_evidence += int(
                row["fmp_basic_limit_reach_evidence"] or 0
            )
            bls_quota_deferred += int(row["bls_quota_deferred"] or 0)
            sec_rate_limit_deferred += int(row["sec_rate_limit_deferred"] or 0)
            tiingo_news_permission_pending += int(
                row["tiingo_news_permission_pending"] or 0
            )
            failed_retryable += int(row["failed_retryable"] or 0)
            exhausted += int(row["exhausted"] or 0)
            high_attempt += int(row["high_attempt"] or 0)
            buffered_running += int(row["buffered_running"] or 0)
            executing += int(row["executing"] or 0)
            stale_buffered += int(row["stale_buffered"] or 0)
            stale_executing += int(row["stale_executing"] or 0)
            row_recent_tasks = int(row["recent_tasks"] or 0)
            row_recent_rows = int(row["recent_rows"] or 0)
            recent_tasks += row_recent_tasks
            recent_rows += row_recent_rows
            if row_recent_tasks:
                category_progress = recent_by_category.setdefault(
                    category_name, {"tasks": 0, "rows": 0}
                )
                category_progress["tasks"] += row_recent_tasks
                category_progress["rows"] += row_recent_rows
                if selected_provider:
                    provider_progress = recent_by_provider.setdefault(
                        str(selected_provider), {"tasks": 0, "rows": 0}
                    )
                    provider_progress["tasks"] += row_recent_tasks
                    provider_progress["rows"] += row_recent_rows
            row_last_updated = row["last_updated"]
            if row_last_updated is not None and (
                last_updated is None or str(row_last_updated) > str(last_updated)
            ):
                last_updated = str(row_last_updated)
            row_last_accepted_updated = row["last_accepted_updated"]
            if row_last_accepted_updated is not None and (
                endpoint_name not in endpoint_last_accepted
                or str(row_last_accepted_updated)
                > endpoint_last_accepted[endpoint_name]
            ):
                endpoint_last_accepted[endpoint_name] = str(row_last_accepted_updated)
            if row_last_accepted_updated is not None and (
                last_accepted_updated is None
                or str(row_last_accepted_updated) > str(last_accepted_updated)
            ):
                last_accepted_updated = str(row_last_accepted_updated)
        status_progress.update(1)
        total_tasks = sum(status_counts.values())
        endpoint_counts = len(endpoint_names)
        category_progress = []
        for category_name in sorted(category_status_counts):
            counts = category_status_counts[category_name]
            total = sum(counts.values())
            success = counts.get("success", 0)
            empty = counts.get("empty", 0)
            unavailable = counts.get("unavailable", 0)
            accepted = sum(counts.get(name, 0) for name in ACCEPTED_STATUSES)
            category_progress.append(
                {
                    "category": category_name,
                    "total_tasks": total,
                    "accepted_tasks": accepted,
                    "success_tasks": success,
                    "empty_tasks": empty,
                    "unavailable_tasks": unavailable,
                    "pending_tasks": counts.get("pending", 0),
                    "running_tasks": counts.get("running", 0),
                    "failed_tasks": counts.get("failed", 0),
                    "unresolved_tasks": total - accepted - unavailable,
                    "success_rows": category_success_rows.get(category_name, 0),
                    "completion_percent": round(
                        (accepted / total * 100.0) if total else 0.0, 6
                    ),
                }
            )
        zero_accepted_categories = [
            item
            for item in category_progress
            if int(item["accepted_tasks"]) == 0 and int(item["unresolved_tasks"]) > 0
        ]
        endpoint_progress = []
        for endpoint_name in sorted(endpoint_names):
            counts = endpoint_status_counts.get(endpoint_name, {})
            total = sum(counts.values())
            accepted = sum(counts.get(name, 0) for name in ACCEPTED_STATUSES)
            success = counts.get("success", 0)
            empty = counts.get("empty", 0)
            unavailable = counts.get("unavailable", 0)
            unresolved = total - accepted - unavailable
            blockers = endpoint_blockers.get(endpoint_name, {})
            endpoint_progress.append(
                {
                    "endpoint": endpoint_name,
                    "total_tasks": total,
                    "accepted_tasks": accepted,
                    "success_tasks": success,
                    "empty_tasks": empty,
                    "empty_ratio": (round(empty / accepted, 6) if accepted else None),
                    "unavailable_tasks": unavailable,
                    "unresolved_tasks": unresolved,
                    "running_tasks": counts.get("running", 0),
                    "failed_tasks": counts.get("failed", 0),
                    "last_accepted_update": endpoint_last_accepted.get(endpoint_name),
                    **blockers,
                }
            )
        zero_accepted_endpoints = [
            item for item in endpoint_progress if int(item["accepted_tasks"]) == 0
        ]
        actionable_zero_accepted_endpoints = [
            item
            for item in zero_accepted_endpoints
            if int(item["unresolved_tasks"]) > 0
        ]
        resolved_endpoints = sum(
            int(item["unresolved_tasks"]) == 0 for item in endpoint_progress
        )
        high_empty_endpoints = [
            item
            for item in endpoint_progress
            if int(item["accepted_tasks"]) >= 10
            and float(item["empty_ratio"] or 0.0) >= 0.9
        ]
        all_empty_endpoints = [
            item for item in high_empty_endpoints if int(item["success_tasks"]) == 0
        ]
        quota_blocker_fields = (
            "fmp_quota_deferred",
            "bls_quota_deferred",
            "sec_rate_limit_deferred",
            "runtime_provider_cooldown_deferred",
        )
        zero_quota_blocked = sum(
            any(int(item[field]) > 0 for field in quota_blocker_fields)
            for item in actionable_zero_accepted_endpoints
        )
        zero_capacity_blocked = sum(
            int(item["provider_capacity_deferred"]) > 0
            for item in actionable_zero_accepted_endpoints
        )
        zero_inflight = sum(
            int(item["running_tasks"]) > 0
            for item in actionable_zero_accepted_endpoints
        )
        zero_without_recorded_blocker = sum(
            int(item["running_tasks"]) == 0
            and not any(
                int(item[field]) > 0
                for field in (
                    *quota_blocker_fields,
                    "provider_capacity_deferred",
                    "tiingo_news_permission_pending",
                )
            )
            for item in actionable_zero_accepted_endpoints
        )
        accepted_tasks = sum(
            status_counts.get(status, 0) for status in ACCEPTED_STATUSES
        )
        unavailable_tasks = status_counts.get("unavailable", 0)
        resolved_tasks = accepted_tasks + unavailable_tasks
        unresolved_tasks = total_tasks - accepted_tasks
        actionable_unresolved_tasks = total_tasks - resolved_tasks
        runnable_retryable_tasks = (
            pending_eligible + failed_retryable + status_counts.get("running", 0)
        )
        retryable_tasks = runnable_retryable_tasks + pending_retry_deferred
        accepted_tasks_per_minute = recent_tasks / 15.0
        minutes_since_last_accepted = (
            max(
                0.0,
                (now - datetime.fromisoformat(last_accepted_updated)).total_seconds()
                / 60.0,
            )
            if last_accepted_updated is not None
            else None
        )
        accepted_progress_stalled = bool(
            runnable_retryable_tasks > 0
            and minutes_since_last_accepted is not None
            and minutes_since_last_accepted >= max(1, accepted_stall_minutes)
        )
        raw_estimated_hours = (
            actionable_unresolved_tasks / accepted_tasks_per_minute / 60.0
            if accepted_tasks_per_minute > 0
            else None
        )
        active_endpoints = [
            {"endpoint": endpoint, "tasks": count}
            for endpoint, count in sorted(
                active_endpoint_counts.items(), key=lambda item: (-item[1], item[0])
            )[:10]
        ]
        permanent_provider_outcome_samples: list[dict[str, Any]] = []
        if tasks_with_permanent_provider_outcomes:
            sample_rows = connection.execute(
                f"""
                SELECT endpoint,scope_key,status,selected_provider,attempts,
                       provider_outcomes_json,error,updated_at
                FROM tasks
                WHERE {active_where}
                  AND provider_outcomes_json LIKE '%:"permanent"%'
                ORDER BY updated_at DESC,task_id
                LIMIT 50
                """,
                active_parameters,
            ).fetchall()
            for row in sample_rows:
                try:
                    outcomes = json.loads(row["provider_outcomes_json"] or "{}")
                except (TypeError, ValueError, json.JSONDecodeError):
                    outcomes = {}
                permanent_provider_outcome_samples.append(
                    {
                        "endpoint": str(row["endpoint"]),
                        "scope_key": str(row["scope_key"]),
                        "status": str(row["status"]),
                        "providers": sorted(
                            str(provider)
                            for provider, outcome in outcomes.items()
                            if outcome == "permanent"
                        ),
                        "selected_provider": row["selected_provider"],
                        "attempts": int(row["attempts"] or 0),
                        "error": str(row["error"] or "")[:1000],
                        "updated_at": str(row["updated_at"]),
                    }
                )
        status_progress.set_postfix(stage="check processes and disk", refresh=False)
        downloader_pid, downloader_active = _read_pid(
            state_dir / "downloader.pid", "download_openbb_archive.py"
        )
        supervisor_pid, supervisor_active = _read_pid(
            state_dir / "supervisor.pid", "run_openbb_archive_supervisor.sh"
        )
        disk = shutil.disk_usage(output_dir)
        status_progress.update(1)
        status_progress.set_postfix(stage="check plan boundaries", refresh=False)
        with _sqlite_progress(
            connection,
            "monitor:check plan boundaries",
            enabled=show_progress,
        ):
            inactive_tasks = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM tasks WHERE NOT ({active_where})",
                    active_parameters,
                ).fetchone()[0]
            )
            active_other_plan_tasks = (
                int(
                    connection.execute(
                        "SELECT COUNT(*) FROM tasks WHERE active=1 AND plan_token!=?",
                        (plan_token,),
                    ).fetchone()[0]
                )
                if plan_token is not None
                else 0
            )
        status_progress.update(1)
        status_progress.set_postfix(stage="check manifest pagination", refresh=False)
        fred_pagination_gaps, fred_pagination_gap_samples = (
            _fred_release_pagination_gaps(
                connection,
                plan_token,
                show_progress=show_progress,
            )
        )
        fmp_pagination_gaps, fmp_pagination_gap_samples = _fmp_manifest_pagination_gaps(
            connection,
            plan_token,
            show_progress=show_progress,
        )
        pagination_gaps = fred_pagination_gaps + fmp_pagination_gaps
        failure_clusters = _retryable_failure_clusters(
            connection,
            plan_token,
            max_total_attempts=max_total_attempts,
            show_progress=show_progress,
        )
        non_authoritative_empty_clusters = _non_authoritative_empty_clusters(
            connection,
            plan_token,
            show_progress=show_progress,
        )
        non_authoritative_empty_tasks = sum(
            int(cluster["tasks"]) for cluster in non_authoritative_empty_clusters
        )
        non_authoritative_unavailable_clusters = (
            _non_authoritative_unavailable_clusters(
                connection,
                plan_token,
                show_progress=show_progress,
            )
        )
        non_authoritative_unavailable_tasks = sum(
            int(cluster["tasks"]) for cluster in non_authoritative_unavailable_clusters
        )
        (
            unproven_unavailable_clusters,
            unproven_provider_capability_constraints,
        ) = _unproven_unavailable_clusters(
            connection,
            plan_token,
            state_dir,
            show_progress=show_progress,
        )
        unproven_unavailable_tasks = sum(
            int(cluster["tasks"]) for cluster in unproven_unavailable_clusters
        )
        systematic_failure_clusters = [
            cluster for cluster in failure_clusters if int(cluster["tasks"]) >= 3
        ]
        source_cap_saturations = _source_cap_saturations(
            connection,
            plan_token,
            show_progress=show_progress,
        )
        raw_declared_limit_saturations = _declared_limit_saturations(
            connection,
            plan_token,
            show_progress=show_progress,
        )
        provider_runtime_limits = _provider_runtime_limits(state_dir)
        provider_scheduler = _provider_scheduler_state(state_dir)
        local_cooldown_bypass_endpoints = (
            _active_local_cooldown_bypass_endpoints(state_dir)
        )
        scheduler_invariant_violations: list[dict[str, Any]] = []
        for provider, pool in provider_scheduler.get("providers", {}).items():
            execution_limit = int(pool.get("execution_limit") or 0)
            queue_limit = int(pool.get("queue_limit") or 0)
            active = int(pool.get("active") or 0)
            reservations = int(pool.get("reservations") or 0)
            refill_threshold = int(pool.get("refill_threshold") or 0)
            if queue_limit < execution_limit:
                scheduler_invariant_violations.append(
                    {
                        "provider": provider,
                        "invariant": "queue_limit_gte_execution_limit",
                        "execution_limit": execution_limit,
                        "queue_limit": queue_limit,
                    }
                )
            if refill_threshold > queue_limit:
                scheduler_invariant_violations.append(
                    {
                        "provider": provider,
                        "invariant": "refill_threshold_lte_queue_limit",
                        "refill_threshold": refill_threshold,
                        "queue_limit": queue_limit,
                    }
                )
            if active > queue_limit or reservations > queue_limit:
                scheduler_invariant_violations.append(
                    {
                        "provider": provider,
                        "invariant": "live_reservations_lte_queue_limit",
                        "active": active,
                        "reservations": reservations,
                        "queue_limit": queue_limit,
                    }
                )
        global_queue_limit = int(provider_scheduler.get("global_queue_limit") or 0)
        live_handoff_total = sum(
            int(provider_scheduler.get(key) or 0)
            for key in (
                "active_total",
                "completed_pending_total",
                "buffered_total",
            )
        )
        if global_queue_limit and live_handoff_total > global_queue_limit:
            scheduler_invariant_violations.append(
                {
                    "provider": "__global__",
                    "invariant": "live_handoff_lte_global_queue_limit",
                    "live_handoff_total": live_handoff_total,
                    "global_queue_limit": global_queue_limit,
                }
            )
        request_checkpoint_summary = _request_checkpoint_summary(state_dir)
        provider_parameter_maximums = _provider_parameter_maximums(state_dir)
        provider_omitted_parameters = _provider_omitted_parameters(state_dir)
        (
            entitlement_limit_saturations,
            declared_limit_saturations,
        ) = _partition_entitlement_limit_saturations(
            raw_declared_limit_saturations,
            provider_parameter_maximums,
        )
        recent_provider_tasks = {
            provider: int(values["tasks"])
            for provider, values in recent_by_provider.items()
        }
        # Derive unresolved provider backlog from the fallback chain itself so
        # stall detection applies equally to every market/provider, rather
        # than silently watching only the historically troublesome trio.
        # A task held by its own durable deadline is recoverable backlog, but
        # it is not currently eligible provider demand and must not trigger a
        # false provider-stall or HTTP ETA projection.
        provider_retry_clause = (
            "AND (retry_not_before IS NULL OR retry_not_before<=?)"
            if retry_tracking_available
            else ""
        )
        provider_backlog_parameters: tuple[Any, ...] = (
            (*active_parameters, now_iso)
            if retry_tracking_available
            else active_parameters
        )
        with _sqlite_progress(
            connection,
            "monitor:aggregate provider backlog",
            enabled=show_progress,
        ):
            provider_backlog_rows = connection.execute(
                f"""
                SELECT provider.value AS provider,
                       tasks.category AS category,
                       tasks.endpoint AS endpoint,
                       COUNT(*) AS backlog,
                       SUM(
                           CASE WHEN NOT EXISTS (
                               SELECT 1
                               FROM json_each(tasks.providers_json) AS alternative
                               WHERE alternative.value != provider.value
                                 AND NOT EXISTS (
                                     SELECT 1
                                     FROM json_each(tasks.provider_outcomes_json) AS alternative_outcome
                                     WHERE alternative_outcome.key=alternative.value
                                 )
                           ) THEN 1 ELSE 0 END
                       ) AS exclusive_backlog
                FROM tasks, json_each(tasks.providers_json) AS provider
                WHERE {active_where}
                  AND status='pending'
                  {provider_retry_clause}
                  AND NOT EXISTS (
                      SELECT 1 FROM json_each(tasks.provider_outcomes_json) AS outcome
                      WHERE outcome.key=provider.value
                  )
                GROUP BY provider.value,tasks.category,tasks.endpoint
                """,
                provider_backlog_parameters,
            ).fetchall()
        provider_endpoint_backlogs = [
            {
                "provider": str(row["provider"]),
                "category": str(row["category"]),
                "endpoint": str(row["endpoint"]),
                "eligible_backlog_tasks": int(row["backlog"] or 0),
                "exclusive_backlog_tasks": int(row["exclusive_backlog"] or 0),
                "local_cooldown_bypass": (
                    (str(row["provider"]), str(row["endpoint"]))
                    in local_cooldown_bypass_endpoints
                ),
            }
            for row in provider_backlog_rows
        ]
        provider_non_bypass_backlogs: Counter[str] = Counter()
        for row in provider_endpoint_backlogs:
            if row["local_cooldown_bypass"]:
                continue
            provider_non_bypass_backlogs[str(row["provider"])] += int(
                row["eligible_backlog_tasks"]
            )
        # Convert task backlog into actual HTTP demand using the downloader's
        # transport-boundary observations.  Unseen endpoints default to one
        # request per task, explicitly marked as unobserved, so an adapter's
        # hidden fan-out can never remain invisible once it executes.
        for row in provider_endpoint_backlogs:
            runtime = provider_runtime_limits.get(str(row["provider"]), {})
            raw_costs = runtime.get("endpoint_request_costs", {})
            endpoint_cost = (
                raw_costs.get(str(row["endpoint"]), {})
                if isinstance(raw_costs, dict)
                else {}
            )
            observed = (
                isinstance(endpoint_cost, dict)
                and int(endpoint_cost.get("claiming_attempts") or 0) > 0
            )
            average_cost = max(
                1.0,
                float(
                    endpoint_cost.get("average_requests_per_claiming_attempt") or 1.0
                ),
            )
            row["http_request_cost_observed"] = observed
            row["observed_requests_per_claiming_attempt"] = round(average_cost, 6)
            row["observed_claiming_attempts"] = int(
                endpoint_cost.get("claiming_attempts") or 0
            )
            row["estimated_eligible_http_requests"] = math.ceil(
                int(row["eligible_backlog_tasks"]) * average_cost
            )
            row["estimated_exclusive_http_requests"] = math.ceil(
                int(row["exclusive_backlog_tasks"]) * average_cost
            )
        provider_category_totals: dict[tuple[str, str], dict[str, Any]] = {}
        for row in provider_endpoint_backlogs:
            key = (str(row["provider"]), str(row["category"]))
            aggregate = provider_category_totals.setdefault(
                key,
                {
                    "provider": key[0],
                    "category": key[1],
                    "eligible_backlog_tasks": 0,
                    "exclusive_backlog_tasks": 0,
                    "estimated_eligible_http_requests": 0,
                    "estimated_exclusive_http_requests": 0,
                    "unobserved_request_cost_tasks": 0,
                    "unobserved_exclusive_request_cost_tasks": 0,
                },
            )
            aggregate["eligible_backlog_tasks"] += int(row["eligible_backlog_tasks"])
            aggregate["exclusive_backlog_tasks"] += int(row["exclusive_backlog_tasks"])
            aggregate["estimated_eligible_http_requests"] += int(
                row["estimated_eligible_http_requests"]
            )
            aggregate["estimated_exclusive_http_requests"] += int(
                row["estimated_exclusive_http_requests"]
            )
            if not row["http_request_cost_observed"]:
                aggregate["unobserved_request_cost_tasks"] += int(
                    row["eligible_backlog_tasks"]
                )
                aggregate["unobserved_exclusive_request_cost_tasks"] += int(
                    row["exclusive_backlog_tasks"]
                )
        provider_category_backlogs = list(provider_category_totals.values())
        dynamic_provider_backlogs: dict[str, int] = {}
        exclusive_provider_backlogs: dict[str, int] = {}
        provider_estimated_http_requests: dict[str, int] = {}
        provider_estimated_exclusive_http_requests: dict[str, int] = {}
        provider_unobserved_request_cost_tasks: dict[str, int] = {}
        provider_unobserved_exclusive_request_cost_tasks: dict[str, int] = {}
        for row in provider_category_backlogs:
            provider = str(row["provider"])
            dynamic_provider_backlogs[provider] = dynamic_provider_backlogs.get(
                provider, 0
            ) + int(row["eligible_backlog_tasks"])
            exclusive_provider_backlogs[provider] = exclusive_provider_backlogs.get(
                provider, 0
            ) + int(row["exclusive_backlog_tasks"])
            provider_estimated_http_requests[provider] = (
                provider_estimated_http_requests.get(provider, 0)
                + int(row["estimated_eligible_http_requests"])
            )
            provider_estimated_exclusive_http_requests[provider] = (
                provider_estimated_exclusive_http_requests.get(provider, 0)
                + int(row["estimated_exclusive_http_requests"])
            )
            provider_unobserved_request_cost_tasks[provider] = (
                provider_unobserved_request_cost_tasks.get(provider, 0)
                + int(row["unobserved_request_cost_tasks"])
            )
            provider_unobserved_exclusive_request_cost_tasks[provider] = (
                provider_unobserved_exclusive_request_cost_tasks.get(provider, 0)
                + int(row["unobserved_exclusive_request_cost_tasks"])
            )
        provider_backlogs: dict[str, int] = {}
        # Stall detection must use the complete unresolved fallback-chain
        # backlog for every provider.  Legacy FMP/BLS/SEC counters describe
        # specific quota symptoms and remain in provider_constraints, but
        # substituting them here would hide unresolved mixed-provider tasks.
        provider_order = [
            provider
            for provider in ("fmp", "bls", "sec")
            if provider in dynamic_provider_backlogs
        ] + sorted(
            provider
            for provider in dynamic_provider_backlogs
            if provider not in {"fmp", "bls", "sec"}
        )
        for provider in provider_order:
            if (
                provider == "tiingo"
                and tiingo_news_permission_pending
                >= dynamic_provider_backlogs[provider]
            ):
                continue
            provider_backlogs[provider] = dynamic_provider_backlogs[provider]
        fully_local_bypass_providers = {
            provider
            for provider, backlog in provider_backlogs.items()
            if backlog > 0 and provider_non_bypass_backlogs.get(provider, 0) == 0
        }
        effective_cooldown_providers = set(active_provider_cooldowns).difference(
            fully_local_bypass_providers
        )
        provider_progress_stalls = [
            {
                "provider": provider,
                "backlog_tasks": backlog,
                "recent_accepted_tasks": recent_provider_tasks.get(provider, 0),
            }
            for provider, backlog in provider_backlogs.items()
            if backlog > 0
            and provider not in effective_cooldown_providers
            and recent_provider_tasks.get(provider, 0) == 0
        ]
        # A global recent-rate projection is valid only when every provider
        # that still owns unresolved work is currently producing evidence.
        # Otherwise fast Yahoo/FRED work makes a multi-year daily-quota backlog
        # look as if it will finish in days.  Fail closed on ETA just as the
        # archive fails closed on data completeness, and expose the exact
        # providers preventing a defensible projection.
        eta_blocked_by_providers = sorted(
            provider
            for provider, backlog in provider_backlogs.items()
            if backlog > 0
            and (
                provider in effective_cooldown_providers
                or recent_provider_tasks.get(provider, 0) == 0
            )
        )
        estimated_hours = None if eta_blocked_by_providers else raw_estimated_hours
        fmp_basic_projection = _daily_quota_projection(
            "fmp",
            "basic",
            fmp_only_pending,
            FMP_BASIC_DAILY_CALL_CAP,
            fmp_basic_limit_reach_evidence,
        )
        provider_eta_projections: list[dict[str, Any]] = []
        for provider in sorted(
            set(dynamic_provider_backlogs)
            | set(provider_runtime_limits)
            | set(provider_scheduler.get("providers", {}))
        ):
            eligible_backlog = int(dynamic_provider_backlogs.get(provider, 0))
            exclusive_backlog = int(exclusive_provider_backlogs.get(provider, 0))
            estimated_http_requests = int(
                provider_estimated_http_requests.get(provider, eligible_backlog)
            )
            estimated_exclusive_http_requests = int(
                provider_estimated_exclusive_http_requests.get(
                    provider, exclusive_backlog
                )
            )
            unobserved_cost_tasks = int(
                provider_unobserved_request_cost_tasks.get(provider, 0)
            )
            unobserved_exclusive_cost_tasks = int(
                provider_unobserved_exclusive_request_cost_tasks.get(provider, 0)
            )
            recent_accepted = int(recent_provider_tasks.get(provider, 0))
            provider_tasks_per_minute = recent_accepted / 15.0
            runtime = provider_runtime_limits.get(provider, {})
            requests_per_second = float(runtime.get("requests_per_second", 0.0))
            daily_cap = int(runtime.get("declared_daily_request_cap") or 0)
            daily_remaining = int(runtime.get("declared_daily_requests_remaining") or 0)
            hourly_cap = int(runtime.get("declared_hourly_request_cap") or 0)
            hourly_remaining = int(
                runtime.get("declared_hourly_requests_remaining") or 0
            )
            cooldown = active_provider_cooldowns.get(provider)
            local_cooldown_bypass = provider in fully_local_bypass_providers
            quota_daily_cap = 0 if local_cooldown_bypass else daily_cap
            quota_hourly_cap = 0 if local_cooldown_bypass else hourly_cap
            provider_eta_projections.append(
                {
                    "provider": provider,
                    # Eligible backlog includes overlapping fallback chains and
                    # therefore must never be summed across providers.
                    "eligible_backlog_tasks": eligible_backlog,
                    # Exclusive backlog has no remaining alternative provider;
                    # it is the defensible provider-specific completion floor.
                    "exclusive_backlog_tasks": exclusive_backlog,
                    "estimated_eligible_http_requests": estimated_http_requests,
                    "estimated_exclusive_http_requests": (
                        estimated_exclusive_http_requests
                    ),
                    "unobserved_request_cost_tasks": unobserved_cost_tasks,
                    "unobserved_exclusive_request_cost_tasks": (
                        unobserved_exclusive_cost_tasks
                    ),
                    "request_cost_model": (
                        "observed_http_boundary_mean_with_one_request_default"
                    ),
                    "recent_accepted_tasks_15m": recent_accepted,
                    "recent_tasks_per_minute": round(provider_tasks_per_minute, 6),
                    "exclusive_eta_hours_at_recent_rate": (
                        round(exclusive_backlog / provider_tasks_per_minute / 60.0, 2)
                        if exclusive_backlog > 0 and provider_tasks_per_minute > 0
                        else None
                    ),
                    "eligible_pressure_hours_at_recent_rate": (
                        round(eligible_backlog / provider_tasks_per_minute / 60.0, 2)
                        if eligible_backlog > 0 and provider_tasks_per_minute > 0
                        else None
                    ),
                    # One task can fan out to several HTTP requests, so RPS is
                    # only an optimistic physical lower bound.
                    "optimistic_exclusive_hours_at_configured_rps": (
                        round(
                            estimated_exclusive_http_requests
                            / requests_per_second
                            / 3600.0,
                            2,
                        )
                        if estimated_exclusive_http_requests > 0
                        and requests_per_second > 0
                        else None
                    ),
                    "requests_per_second": requests_per_second,
                    "configured_concurrency": int(runtime.get("concurrency", 0)),
                    "durable_claims_current_provider_day": int(
                        runtime.get("limiter_claims_current_provider_day", 0)
                    ),
                    "provider_day_key": runtime.get("current_provider_day_key"),
                    "declared_daily_request_cap": daily_cap or None,
                    "declared_daily_requests_remaining": (
                        daily_remaining if daily_cap else None
                    ),
                    "optimistic_daily_quota_windows_for_exclusive_backlog": (
                        (
                            estimated_exclusive_http_requests
                            + quota_daily_cap
                            - 1
                        )
                        // quota_daily_cap
                        if estimated_exclusive_http_requests > 0
                        and quota_daily_cap > 0
                        else None
                    ),
                    "optimistic_additional_daily_resets_required": (
                        (
                            max(
                                0,
                                estimated_exclusive_http_requests - daily_remaining,
                            )
                            + quota_daily_cap
                            - 1
                        )
                        // quota_daily_cap
                        if exclusive_backlog > 0 and quota_daily_cap > 0
                        else None
                    ),
                    "durable_claims_current_utc_hour": int(
                        runtime.get("limiter_claims_current_utc_hour", 0)
                    ),
                    "utc_hour_key": runtime.get("current_utc_hour_key"),
                    "declared_hourly_request_cap": hourly_cap or None,
                    "declared_hourly_requests_remaining": (
                        hourly_remaining if hourly_cap else None
                    ),
                    "optimistic_hourly_quota_windows_for_exclusive_backlog": (
                        (estimated_exclusive_http_requests + quota_hourly_cap - 1)
                        // quota_hourly_cap
                        if estimated_exclusive_http_requests > 0
                        and quota_hourly_cap > 0
                        else None
                    ),
                    "optimistic_additional_hourly_resets_required": (
                        (
                            max(
                                0,
                                estimated_exclusive_http_requests - hourly_remaining,
                            )
                            + quota_hourly_cap
                            - 1
                        )
                        // quota_hourly_cap
                        if exclusive_backlog > 0 and quota_hourly_cap > 0
                        else None
                    ),
                    "observed_quota_limit": runtime.get("observed_quota_limit"),
                    "cooldown_until": cooldown.get("until") if cooldown else None,
                    "local_cooldown_bypass": local_cooldown_bypass,
                    "local_cooldown_bypass_endpoints": sorted(
                        endpoint
                        for item_provider, endpoint in local_cooldown_bypass_endpoints
                        if item_provider == provider
                    ),
                    "state": (
                        "cooldown"
                        if cooldown and not local_cooldown_bypass
                        else "active"
                        if recent_accepted > 0
                        else "stalled"
                        if eligible_backlog > 0
                        else "idle"
                    ),
                    "daily_quota_lower_bound": (
                        fmp_basic_projection if provider == "fmp" else None
                    ),
                    "overlapping_fallback_backlog": True,
                }
            )

        category_backlogs: dict[str, list[dict[str, Any]]] = {}
        for row in provider_category_backlogs:
            category_backlogs.setdefault(str(row["category"]), []).append(row)
        category_eta_projections: list[dict[str, Any]] = []
        for item in category_progress:
            category = str(item["category"])
            unresolved = int(item["unresolved_tasks"])
            recent = int(recent_by_category.get(category, {}).get("tasks", 0))
            category_tasks_per_minute = recent / 15.0
            ownership = category_backlogs.get(category, [])
            exclusive_owners = {
                str(row["provider"]): int(row["exclusive_backlog_tasks"])
                for row in ownership
                if int(row["exclusive_backlog_tasks"]) > 0
            }
            blockers = sorted(
                provider
                for provider in exclusive_owners
                if provider in effective_cooldown_providers
                or recent_provider_tasks.get(provider, 0) == 0
            )
            raw_hours = (
                unresolved / category_tasks_per_minute / 60.0
                if unresolved > 0 and category_tasks_per_minute > 0
                else None
            )
            category_eta_projections.append(
                {
                    "category": category,
                    "unresolved_tasks": unresolved,
                    "recent_accepted_tasks_15m": recent,
                    "recent_tasks_per_minute": round(category_tasks_per_minute, 6),
                    "raw_eta_hours_at_recent_rate": (
                        round(raw_hours, 2) if raw_hours is not None else None
                    ),
                    "estimated_hours_at_recent_rate": (
                        round(raw_hours, 2)
                        if raw_hours is not None and not blockers
                        else None
                    ),
                    "eta_blocked_by_exclusive_providers": blockers,
                    "exclusive_backlog_by_provider": dict(
                        sorted(exclusive_owners.items())
                    ),
                }
            )
        provider_quota_feasibility: dict[str, list[dict[str, Any]]] = {}
        for projection in provider_eta_projections:
            provider = str(projection["provider"])
            if projection.get("local_cooldown_bypass"):
                continue
            exclusive_backlog = int(projection["exclusive_backlog_tasks"])
            estimated_exclusive_requests = int(
                projection.get("estimated_exclusive_http_requests") or exclusive_backlog
            )
            daily_cap = int(projection.get("declared_daily_request_cap") or 0)
            hourly_cap = int(projection.get("declared_hourly_request_cap") or 0)
            contracts: list[dict[str, Any]] = []
            if exclusive_backlog > 0 and daily_cap > 0:
                contracts.append(
                    {
                        "window": "provider_day",
                        "exclusive_backlog_tasks": exclusive_backlog,
                        "estimated_exclusive_http_requests": (
                            estimated_exclusive_requests
                        ),
                        "request_cap": daily_cap,
                        "requests_remaining_in_current_window": int(
                            projection.get("declared_daily_requests_remaining") or 0
                        ),
                        "minimum_quota_windows": int(
                            projection[
                                "optimistic_daily_quota_windows_for_exclusive_backlog"
                            ]
                        ),
                        "additional_reset_boundaries_required": int(
                            projection["optimistic_additional_daily_resets_required"]
                        ),
                        "lower_bound_only": bool(
                            projection.get("unobserved_exclusive_request_cost_tasks")
                        ),
                    }
                )
            if exclusive_backlog > 0 and hourly_cap > 0:
                contracts.append(
                    {
                        "window": "utc_hour",
                        "exclusive_backlog_tasks": exclusive_backlog,
                        "estimated_exclusive_http_requests": (
                            estimated_exclusive_requests
                        ),
                        "request_cap": hourly_cap,
                        "requests_remaining_in_current_window": int(
                            projection.get("declared_hourly_requests_remaining") or 0
                        ),
                        "minimum_quota_windows": int(
                            projection[
                                "optimistic_hourly_quota_windows_for_exclusive_backlog"
                            ]
                        ),
                        "additional_reset_boundaries_required": int(
                            projection["optimistic_additional_hourly_resets_required"]
                        ),
                        "lower_bound_only": bool(
                            projection.get("unobserved_exclusive_request_cost_tasks")
                        ),
                    }
                )
            if contracts:
                provider_quota_feasibility[provider] = contracts
        archive_end_date_path = state_dir / "archive_end_date.txt"
        archive_end_date = (
            archive_end_date_path.read_text(encoding="utf-8").strip()
            if archive_end_date_path.is_file()
            else None
        )
        status_progress.update(1)
        status_progress.set_postfix(stage="build status and audit", refresh=False)
        status: dict[str, Any] = {
            "schema_version": 14,
            "checked_at": now.isoformat(),
            "output_dir": str(output_dir.resolve()),
            "active_plan_token": plan_token,
            "inactive_tasks": inactive_tasks,
            "active_other_plan_tasks": active_other_plan_tasks,
            "archive_end_date": archive_end_date,
            "total_tasks": total_tasks,
            "accepted_tasks": accepted_tasks,
            "unavailable_tasks": unavailable_tasks,
            "resolved_tasks": resolved_tasks,
            "unresolved_tasks": unresolved_tasks,
            "actionable_unresolved_tasks": actionable_unresolved_tasks,
            "completion_percent": round(
                (accepted_tasks / total_tasks * 100.0) if total_tasks else 0.0, 6
            ),
            "success_rows": total_rows,
            "category_progress": category_progress,
            "zero_accepted_categories": zero_accepted_categories,
            "provider_progress": [
                {"provider": provider, **values}
                for provider, values in sorted(
                    accepted_by_provider.items(),
                    key=lambda item: (-item[1]["accepted_tasks"], item[0]),
                )
            ],
            "endpoint_count": endpoint_counts,
            "endpoint_progress_summary": {
                "endpoint_count": endpoint_counts,
                "started_endpoint_count": endpoint_counts
                - len(zero_accepted_endpoints),
                "zero_accepted_endpoint_count": len(zero_accepted_endpoints),
                "resolved_endpoint_count": resolved_endpoints,
                "unresolved_endpoint_count": endpoint_counts - resolved_endpoints,
                "zero_quota_blocked_endpoint_count": zero_quota_blocked,
                "zero_capacity_blocked_endpoint_count": zero_capacity_blocked,
                "zero_inflight_endpoint_count": zero_inflight,
                "zero_without_recorded_blocker_count": zero_without_recorded_blocker,
                "high_empty_endpoint_count": len(high_empty_endpoints),
                "all_empty_endpoint_count": len(all_empty_endpoints),
            },
            "zero_accepted_endpoints": sorted(
                zero_accepted_endpoints,
                key=lambda item: (-int(item["total_tasks"]), str(item["endpoint"])),
            )[:50],
            "high_empty_endpoints": sorted(
                high_empty_endpoints,
                key=lambda item: (
                    -float(item["empty_ratio"] or 0.0),
                    -int(item["accepted_tasks"]),
                    str(item["endpoint"]),
                ),
            )[:50],
            "status_counts": dict(sorted(status_counts.items())),
            "pending_eligible": pending_eligible,
            "pending_retry_deferred": pending_retry_deferred,
            "next_task_retry_at": next_task_retry_at,
            "pending_with_error": pending_with_error,
            "pending_cooldown": pending_cooldown,
            "pending_rate_limited": pending_rate_limited,
            "pending_attempted": pending_attempted,
            "provider_outcomes": {
                "tasks_with_any": tasks_with_provider_outcomes,
                "tasks_with_empty": tasks_with_empty_provider_outcomes,
                "tasks_with_unavailable": tasks_with_unavailable_provider_outcomes,
                "tasks_with_permanent": tasks_with_permanent_provider_outcomes,
                "permanent_endpoint_count": len(
                    permanent_provider_outcomes_by_endpoint
                ),
                "permanent_by_endpoint": [
                    {"endpoint": endpoint, "tasks": count}
                    for endpoint, count in sorted(
                        permanent_provider_outcomes_by_endpoint.items(),
                        key=lambda item: (-item[1], item[0]),
                    )
                ],
                "permanent_samples": permanent_provider_outcome_samples,
            },
            "provider_constraints": {
                "fmp_only_pending": fmp_only_pending,
                "provider_capacity_deferred": provider_capacity_deferred,
                "fmp_quota_deferred": fmp_quota_deferred,
                "bls_quota_deferred": bls_quota_deferred,
                "sec_rate_limit_deferred": sec_rate_limit_deferred,
                "tiingo_news_permission_pending": tiingo_news_permission_pending,
            },
            "provider_quota_feasibility": provider_quota_feasibility,
            "active_provider_cooldowns": active_provider_cooldowns,
            "local_cooldown_bypass_endpoints": [
                {"provider": provider, "endpoint": endpoint}
                for provider, endpoint in sorted(local_cooldown_bypass_endpoints)
            ],
            "fully_local_bypass_providers": sorted(fully_local_bypass_providers),
            "provider_runtime_limits": provider_runtime_limits,
            "provider_scheduler": provider_scheduler,
            "scheduler_invariant_violations": scheduler_invariant_violations,
            "request_checkpoints": request_checkpoint_summary,
            "provider_parameter_maximums": provider_parameter_maximums,
            "provider_omitted_parameters": provider_omitted_parameters,
            # Keep the complete unresolved fallback-chain backlog visible even
            # when a provider is making recent progress and therefore is not
            # classified as stalled.
            "dynamic_provider_backlogs": dict(
                sorted(dynamic_provider_backlogs.items())
            ),
            "exclusive_provider_backlogs": dict(
                sorted(exclusive_provider_backlogs.items())
            ),
            "provider_category_backlogs": sorted(
                provider_category_backlogs,
                key=lambda item: (item["provider"], item["category"]),
            ),
            "provider_endpoint_backlogs": sorted(
                provider_endpoint_backlogs,
                key=lambda item: (
                    item["provider"],
                    -int(item["exclusive_backlog_tasks"]),
                    item["endpoint"],
                ),
            ),
            "provider_eta_projections": provider_eta_projections,
            "category_eta_projections": category_eta_projections,
            "provider_progress_stalls": provider_progress_stalls,
            "failed_retryable": failed_retryable,
            "runnable_retryable_tasks": runnable_retryable_tasks,
            "retryable_tasks": retryable_tasks,
            "exhausted_tasks": exhausted,
            "high_attempt_tasks": high_attempt,
            "execution_tracking_available": execution_tracking_available,
            "retry_tracking_available": retry_tracking_available,
            "buffered_running_tasks": buffered_running,
            "executing_tasks": executing,
            "stale_buffered_tasks": stale_buffered,
            "stale_executing_tasks": stale_executing,
            "stale_running_tasks": stale_buffered + stale_executing,
            "fred_release_pagination_gaps": fred_pagination_gaps,
            "fred_release_pagination_gap_samples": fred_pagination_gap_samples,
            "fmp_manifest_pagination_gaps": fmp_pagination_gaps,
            "fmp_manifest_pagination_gap_samples": fmp_pagination_gap_samples,
            "pagination_gaps": pagination_gaps,
            "retryable_failure_clusters": failure_clusters,
            "systematic_failure_clusters": systematic_failure_clusters,
            "systematic_retryable_failure_tasks": sum(
                int(cluster["tasks"]) for cluster in systematic_failure_clusters
            ),
            "non_authoritative_empty_clusters": (non_authoritative_empty_clusters),
            "non_authoritative_empty_tasks": non_authoritative_empty_tasks,
            "non_authoritative_unavailable_clusters": (
                non_authoritative_unavailable_clusters
            ),
            "non_authoritative_unavailable_tasks": (
                non_authoritative_unavailable_tasks
            ),
            "unproven_unavailable_clusters": unproven_unavailable_clusters,
            "unproven_unavailable_tasks": unproven_unavailable_tasks,
            "unproven_provider_capability_constraints": (
                unproven_provider_capability_constraints
            ),
            "source_cap_saturations": source_cap_saturations,
            "source_cap_saturation_count": len(source_cap_saturations),
            "declared_limit_saturations": declared_limit_saturations,
            "declared_limit_saturation_count": len(declared_limit_saturations),
            "entitlement_limit_saturations": entitlement_limit_saturations,
            "entitlement_limit_saturation_count": len(entitlement_limit_saturations),
            "last_task_update": last_updated,
            "last_accepted_update": last_accepted_updated,
            "minutes_since_last_accepted": (
                round(minutes_since_last_accepted, 2)
                if minutes_since_last_accepted is not None
                else None
            ),
            "accepted_progress_stalled": accepted_progress_stalled,
            "accepted_tasks_last_15m": recent_tasks,
            "success_rows_last_15m": recent_rows,
            "recent_progress": {
                "by_category": [
                    {"category": category, **values}
                    for category, values in sorted(
                        recent_by_category.items(),
                        key=lambda item: (-item[1]["tasks"], item[0]),
                    )
                ],
                "by_provider": [
                    {"provider": provider, **values}
                    for provider, values in sorted(
                        recent_by_provider.items(),
                        key=lambda item: (-item[1]["tasks"], item[0]),
                    )
                ],
            },
            "tasks_per_minute_last_15m": round(accepted_tasks_per_minute, 4),
            "raw_estimated_hours_at_recent_rate": round(raw_estimated_hours, 2)
            if raw_estimated_hours is not None
            else None,
            "estimated_hours_at_recent_rate": round(estimated_hours, 2)
            if estimated_hours is not None
            else None,
            "eta_blocked_by_providers": eta_blocked_by_providers,
            "active_endpoints": active_endpoints,
            "disk_free_bytes": disk.free,
            "min_free_bytes": int(max(0.0, min_free_gib) * 2**30),
            "downloader_pid": downloader_pid,
            "downloader_active": downloader_active,
            "downloader_process": _proc_runtime_metrics(
                downloader_pid if downloader_active else None
            ),
            "supervisor_pid": supervisor_pid,
            "supervisor_active": supervisor_active,
            "coverage_counts": _coverage_counts(output_dir),
            "completeness_contract": _completeness_contract_summary(output_dir),
        }
        if audit_files:
            status["file_audit"] = _audit_success_files(
                connection,
                output_dir=output_dir,
                plan_token=plan_token,
                show_progress=show_progress,
            )
            status["catalog_followup_audit"] = _audit_catalog_followups(
                connection,
                plan_token=plan_token,
                show_progress=show_progress,
            )
        alerts: list[dict[str, str]] = []

        def add_alert(severity: str, code: str, message: str) -> None:
            alerts.append({"severity": severity, "code": code, "message": message})

        if retryable_tasks > 0 and not supervisor_active:
            add_alert(
                "critical",
                "supervisor_down",
                "Retryable tasks remain but the supervisor is not active.",
            )
        if retryable_tasks > 0 and not downloader_active:
            add_alert(
                "critical",
                "downloader_down",
                "Retryable tasks remain but the downloader is not active.",
            )
        stale_running = stale_buffered + stale_executing
        if stale_running:
            add_alert(
                "critical",
                "stale_running",
                f"{stale_running:,} running tasks exceeded the stale threshold "
                f"(buffered={stale_buffered:,}, executing={stale_executing:,}).",
            )
        if exhausted:
            add_alert(
                "critical",
                "exhausted_tasks",
                f"{exhausted:,} tasks exhausted their attempt budget.",
            )
        if high_attempt:
            add_alert(
                "warning",
                "high_attempt_tasks",
                f"{high_attempt:,} transient tasks crossed the diagnostic attempt "
                f"threshold; {pending_retry_deferred:,} pending tasks are currently "
                "isolated by durable retry deadlines.",
            )
        if fred_pagination_gaps:
            add_alert(
                "critical",
                "fred_pagination_gaps",
                f"{fred_pagination_gaps:,} required FRED continuation pages are absent.",
            )
        if fmp_pagination_gaps:
            add_alert(
                "critical",
                "fmp_pagination_gaps",
                f"{fmp_pagination_gaps:,} required FMP continuation pages are absent.",
            )
        if systematic_failure_clusters:
            add_alert(
                "warning",
                "systematic_retryable_failures",
                f"{sum(int(item['tasks']) for item in systematic_failure_clusters):,} "
                "retryable tasks share repeated endpoint/provider failure signatures; "
                "investigate the shared boundary or parser invariant.",
            )
        if non_authoritative_empty_tasks:
            add_alert(
                "critical",
                "non_authoritative_terminal_empty",
                f"{non_authoritative_empty_tasks:,} terminal empty tasks end in "
                "quota, network, parameter-boundary, or wrong-route evidence; "
                "they must be requeued before the archive can be complete.",
            )
        if non_authoritative_unavailable_tasks:
            add_alert(
                "critical",
                "non_authoritative_terminal_unavailable",
                f"{non_authoritative_unavailable_tasks:,} unavailable tasks end "
                "in quota, network, bounded-limit, or wrong-route evidence; "
                "they must be requeued before the archive can be complete.",
            )
        if unproven_unavailable_tasks or unproven_provider_capability_constraints:
            add_alert(
                "critical",
                "unproven_terminal_unavailable",
                f"{unproven_unavailable_tasks:,} unavailable tasks and "
                f"{len(unproven_provider_capability_constraints):,} persisted "
                "capability constraints lack positive subscription, credential, "
                "or market-namespace evidence.",
            )
        if tasks_with_permanent_provider_outcomes:
            add_alert(
                "warning",
                "permanent_provider_outcomes",
                f"{tasks_with_permanent_provider_outcomes:,} tasks contain a "
                "provider outcome classified as permanent; inspect the grouped "
                "endpoints and samples for shared adapter/schema failures.",
            )
        if scheduler_invariant_violations:
            add_alert(
                "critical",
                "provider_scheduler_invariant_failed",
                f"{len(scheduler_invariant_violations):,} provider/global queue "
                "invariants are violated; completed-result retention or a "
                "stale refill threshold can couple otherwise independent pools.",
            )
        if source_cap_saturations:
            add_alert(
                "critical",
                "source_cap_saturated",
                f"{len(source_cap_saturations):,} successful task partitions reached "
                "a provider's fixed row cap and cannot prove completeness.",
            )
        if declared_limit_saturations:
            add_alert(
                "critical",
                "declared_limit_saturated",
                f"{len(declared_limit_saturations):,} successful bounded result "
                "partitions reached their declared row limit and cannot prove "
                "completeness.",
            )
        if entitlement_limit_saturations:
            add_alert(
                "warning",
                "entitlement_history_capped",
                f"{len(entitlement_limit_saturations):,} successful non-pageable "
                "results contain every row exposed by the current entitlement, "
                "but older provider history requires a higher subscription.",
            )
        if zero_accepted_categories:
            add_alert(
                "warning",
                "categories_without_accepted_data",
                f"{len(zero_accepted_categories):,} active market categories with "
                "unresolved tasks have not yet produced a success or "
                "authoritative empty task: "
                + ", ".join(str(item["category"]) for item in zero_accepted_categories),
            )
        if actionable_zero_accepted_endpoints:
            add_alert(
                "warning",
                "endpoints_without_accepted_data",
                f"{len(actionable_zero_accepted_endpoints):,} active endpoints with "
                "actionable unresolved tasks have not yet "
                "produced a success or authoritative empty task.",
            )
        if all_empty_endpoints:
            add_alert(
                "critical",
                "endpoints_all_empty",
                f"{len(all_empty_endpoints):,} active endpoints have at least 10 "
                "accepted tasks but every accepted task is authoritative empty; "
                "verify the shared query dimensions and provider routing.",
            )
        if not status["completeness_contract"]["passed"]:
            add_alert(
                "critical",
                "completeness_contract_failed",
                "The all-market temporal/dimension/pagination contract has "
                f"{int(status['completeness_contract'].get('unresolved', 0)):,} "
                "unresolved obligations.",
            )
        if active_other_plan_tasks:
            add_alert(
                "critical",
                "other_active_plan",
                f"{active_other_plan_tasks:,} active tasks belong to another plan.",
            )
        if accepted_progress_stalled:
            add_alert(
                "warning",
                "accepted_progress_stalled",
                "No success or confirmed-empty task completed for "
                f"{minutes_since_last_accepted:.1f} minutes.",
            )
        if provider_progress_stalls:
            add_alert(
                "warning",
                "provider_progress_stalled",
                "Providers have unresolved backlog and no active cooldown, but "
                "produced no accepted task in the last 15 minutes: "
                + ", ".join(
                    f"{item['provider']}={int(item['backlog_tasks']):,}"
                    for item in provider_progress_stalls
                ),
            )
        quota_constraints = {
            "fmp": fmp_quota_deferred,
            "bls": bls_quota_deferred,
            "sec": sec_rate_limit_deferred,
        }
        active_quota_constraints = {
            provider: count
            for provider, count in quota_constraints.items()
            if count > 0 and provider not in fully_local_bypass_providers
        }
        if active_quota_constraints:
            add_alert(
                "warning",
                "provider_quota_deferred",
                "Provider quotas or rate limits currently defer pending tasks: "
                + ", ".join(
                    f"{provider}={count:,}"
                    for provider, count in active_quota_constraints.items()
                ),
            )
        for provider, contracts in provider_quota_feasibility.items():
            for contract in contracts:
                resets = int(contract["additional_reset_boundaries_required"])
                if resets <= 0:
                    continue
                window = str(contract["window"])
                severity = (
                    "critical"
                    if window == "provider_day"
                    and resets > QUOTA_FEASIBILITY_CRITICAL_DAYS
                    else "warning"
                )
                add_alert(
                    severity,
                    "provider_quota_completion_floor",
                    f"{provider} has {int(contract['exclusive_backlog_tasks']):,} "
                    "pending tasks with no remaining fallback and a declared "
                    f"{int(contract['request_cap']):,}-request {window} cap. "
                    f"Remaining demand is estimated at "
                    f"{int(contract['estimated_exclusive_http_requests']):,} "
                    "HTTP requests from endpoint-level boundary telemetry"
                    + (
                        " plus a one-request default for endpoints not observed yet; "
                        if contract["lower_bound_only"]
                        else "; "
                    )
                    + "completion crosses at least "
                    f"{resets:,} additional reset boundaries; concurrency cannot "
                    "remove this account-allocation floor.",
                )
        if int(request_checkpoint_summary.get("corrupt_file_count") or 0) > 0:
            add_alert(
                "warning",
                "request_checkpoint_corrupt",
                f"{int(request_checkpoint_summary['corrupt_file_count']):,} "
                "request-level resume checkpoints were quarantined as corrupt; "
                "their exact subrequests will be fetched again.",
            )
        if disk.free < status["min_free_bytes"]:
            add_alert(
                "critical",
                "low_disk",
                f"Free disk is {disk.free / 2**30:.1f} GiB, below the configured {max(0.0, min_free_gib):.1f} GiB floor.",
            )
        if audit_files and not status["file_audit"]["passed"]:
            add_alert(
                "critical",
                "file_audit_failed",
                "One or more successful Parquet shards failed the full audit.",
            )
        if audit_files and not status["catalog_followup_audit"]["passed"]:
            add_alert(
                "critical",
                "catalog_followup_audit_failed",
                "One or more successful catalog records lack their required follow-up task.",
            )
        status["alerts"] = alerts
        status["health"] = (
            "critical"
            if any(item["severity"] == "critical" for item in alerts)
            else "warning"
            if alerts
            else "ok"
        )
        complete = (
            actionable_unresolved_tasks == 0
            and exhausted == 0
            and active_other_plan_tasks == 0
            and pagination_gaps == 0
            and non_authoritative_empty_tasks == 0
            and non_authoritative_unavailable_tasks == 0
            and unproven_unavailable_tasks == 0
            and not unproven_provider_capability_constraints
            and not source_cap_saturations
            and not declared_limit_saturations
            and not all_empty_endpoints
            and bool(status["completeness_contract"]["passed"])
            and status_counts.get("running", 0) == 0
            and (not audit_files or bool(status["file_audit"]["passed"]))
            and (not audit_files or bool(status["catalog_followup_audit"]["passed"]))
            and not any(item["severity"] == "critical" for item in alerts)
        )
        status["complete"] = complete
        status_progress.update(1)
        return status
    finally:
        status_progress.close()
        connection.rollback()
        connection.close()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    """Publish machine-readable monitor projections without partial files."""
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        pq.write_table(
            pa.Table.from_pylist(rows),
            temporary,
            compression="zstd",
            use_dictionary=True,
        )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_json_object(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def _snapshot_delta(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
) -> dict[str, Any] | None:
    """Compare durable monitor snapshots without confusing new follow-ups with loss."""
    if not previous or previous.get("active_plan_token") != current.get(
        "active_plan_token"
    ):
        return None
    try:
        previous_at = datetime.fromisoformat(str(previous["checked_at"]))
        current_at = datetime.fromisoformat(str(current["checked_at"]))
    except (KeyError, TypeError, ValueError):
        return None
    elapsed_seconds = (current_at - previous_at).total_seconds()
    if elapsed_seconds <= 0:
        return None

    def scalar_delta(field: str) -> int:
        return int(current.get(field, 0) or 0) - int(previous.get(field, 0) or 0)

    def keyed_rows(
        payload: dict[str, Any], list_field: str, key_field: str
    ) -> dict[str, dict[str, Any]]:
        rows = payload.get(list_field, [])
        if not isinstance(rows, list):
            return {}
        return {
            str(row[key_field]): row
            for row in rows
            if isinstance(row, dict) and row.get(key_field) is not None
        }

    previous_categories = keyed_rows(previous, "category_progress", "category")
    current_categories = keyed_rows(current, "category_progress", "category")
    category_deltas = []
    for category in sorted(set(previous_categories) | set(current_categories)):
        old = previous_categories.get(category, {})
        new = current_categories.get(category, {})
        item = {
            "category": category,
            "total_tasks_delta": int(new.get("total_tasks", 0) or 0)
            - int(old.get("total_tasks", 0) or 0),
            "accepted_tasks_delta": int(new.get("accepted_tasks", 0) or 0)
            - int(old.get("accepted_tasks", 0) or 0),
            "rows_delta": int(new.get("success_rows", 0) or 0)
            - int(old.get("success_rows", 0) or 0),
        }
        if any(int(item[field]) != 0 for field in item if field != "category"):
            category_deltas.append(item)

    previous_providers = keyed_rows(previous, "provider_progress", "provider")
    current_providers = keyed_rows(current, "provider_progress", "provider")
    provider_deltas = []
    for provider in sorted(set(previous_providers) | set(current_providers)):
        old = previous_providers.get(provider, {})
        new = current_providers.get(provider, {})
        item = {
            "provider": provider,
            "accepted_tasks_delta": int(new.get("accepted_tasks", 0) or 0)
            - int(old.get("accepted_tasks", 0) or 0),
            "rows_delta": int(new.get("rows", 0) or 0) - int(old.get("rows", 0) or 0),
        }
        if any(int(item[field]) != 0 for field in item if field != "provider"):
            provider_deltas.append(item)

    previous_endpoint_summary = previous.get("endpoint_progress_summary", {})
    current_endpoint_summary = current.get("endpoint_progress_summary", {})
    return {
        "previous_checked_at": previous_at.isoformat(),
        "elapsed_seconds": round(elapsed_seconds, 3),
        "total_tasks_delta": scalar_delta("total_tasks"),
        "accepted_tasks_delta": scalar_delta("accepted_tasks"),
        "success_rows_delta": scalar_delta("success_rows"),
        "unavailable_tasks_delta": scalar_delta("unavailable_tasks"),
        "retryable_tasks_delta": scalar_delta("retryable_tasks"),
        "zero_accepted_endpoints_delta": int(
            current_endpoint_summary.get("zero_accepted_endpoint_count", 0) or 0
        )
        - int(previous_endpoint_summary.get("zero_accepted_endpoint_count", 0) or 0),
        "by_category": category_deltas,
        "by_provider": provider_deltas,
    }


def _active_provider_cooldowns(
    state_dir: Path, now: datetime
) -> dict[str, dict[str, Any]]:
    path = state_dir / "provider_cooldowns.json"
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        providers = payload.get("providers", {})
        if not isinstance(providers, dict):
            return {}
        now_timestamp = now.timestamp()
        active: dict[str, dict[str, Any]] = {}
        for provider, raw_deadline in providers.items():
            if isinstance(raw_deadline, dict):
                deadline = float(raw_deadline.get("blocked_until") or 0.0)
                kind = str(raw_deadline.get("kind") or "unknown")
                reason = str(raw_deadline.get("reason") or "")[:1000]
            else:
                deadline = float(raw_deadline)
                kind = "legacy"
                reason = ""
            if deadline <= now_timestamp:
                continue
            active[str(provider)] = {
                "until": datetime.fromtimestamp(deadline, tz=timezone.utc).isoformat(),
                "remaining_seconds": max(0, int(deadline - now_timestamp)),
                "kind": kind,
                "reason": reason,
            }
        return dict(sorted(active.items()))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return {}


def _provider_runtime_limits(state_dir: Path) -> dict[str, dict[str, Any]]:
    """Read the downloader's effective per-provider RPS/concurrency contract."""
    path = state_dir / "provider_cooldowns.json"
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        raw_rps = payload.get("rate_limits_rps", {})
        raw_concurrency = payload.get("concurrency", {})
        raw_activity = payload.get("rate_activity", {})
        raw_observed_limits = payload.get("observed_quota_limits", {})
        if not isinstance(raw_observed_limits, dict):
            raw_observed_limits = {}
        activity_providers = (
            raw_activity.get("providers", {}) if isinstance(raw_activity, dict) else {}
        )
        if not isinstance(activity_providers, dict):
            activity_providers = {}
        if not isinstance(raw_rps, dict) or not isinstance(raw_concurrency, dict):
            return {}
        providers = set(raw_rps) | set(raw_concurrency)
        result: dict[str, dict[str, Any]] = {}
        for provider in sorted(providers):
            activity = activity_providers.get(provider, {})
            if not isinstance(activity, dict):
                activity = {}
            result[str(provider)] = {
                "requests_per_second": float(raw_rps.get(provider, 0.0)),
                "concurrency": int(raw_concurrency.get(provider, 0)),
                "active_calls": int(activity.get("active_calls", 0)),
                "ticket_waiters": int(activity.get("ticket_waiters", 0)),
                "effective_concurrency": int(
                    activity.get(
                        "effective_concurrency", raw_concurrency.get(provider, 0)
                    )
                ),
                "adaptive_concurrency_cap": int(
                    activity.get(
                        "adaptive_concurrency_cap", raw_concurrency.get(provider, 0)
                    )
                ),
                "concurrency_expansions": int(
                    activity.get("concurrency_expansions", 0)
                ),
                "limiter_claims_total": int(activity.get("limiter_claims_total", 0)),
                "limiter_observed_claims_total": int(
                    activity.get(
                        "limiter_observed_claims_total",
                        activity.get("limiter_claims_total", 0),
                    )
                ),
                "pending_claim_observations": int(
                    activity.get("pending_claim_observations", 0)
                ),
                "limiter_claims_current_utc_hour": int(
                    activity.get("limiter_claims_current_utc_hour", 0)
                ),
                "current_utc_hour_key": activity.get("current_utc_hour_key"),
                "limiter_claims_current_provider_day": int(
                    activity.get("limiter_claims_current_provider_day", 0)
                ),
                "current_provider_day_key": activity.get("current_provider_day_key"),
                "provider_day_basis": activity.get("provider_day_basis"),
                "declared_hourly_request_cap": activity.get(
                    "declared_hourly_request_cap"
                ),
                "declared_hourly_requests_remaining": activity.get(
                    "declared_hourly_requests_remaining"
                ),
                "declared_daily_request_cap": activity.get(
                    "declared_daily_request_cap"
                ),
                "declared_daily_requests_remaining": activity.get(
                    "declared_daily_requests_remaining"
                ),
                "limiter_claims_last_60s": int(
                    activity.get("limiter_claims_last_60s", 0)
                ),
                "observed_claims_per_second": float(
                    activity.get("observed_claims_per_second", 0.0)
                ),
                "utilization_percent": float(activity.get("utilization_percent", 0.0)),
                "observed_quota_limit": (
                    dict(raw_observed_limits[provider])
                    if isinstance(raw_observed_limits.get(provider), dict)
                    else None
                ),
                "endpoint_request_costs": (
                    {
                        str(endpoint): dict(cost)
                        for endpoint, cost in activity.get(
                            "endpoint_request_costs", {}
                        ).items()
                        if isinstance(cost, dict)
                    }
                    if isinstance(activity.get("endpoint_request_costs"), dict)
                    else {}
                ),
            }
        return result
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return {}


def _provider_scheduler_state(state_dir: Path) -> dict[str, Any]:
    """Read the downloader's durable independent provider-pool snapshot."""
    payload = _read_json_object(state_dir / "provider_scheduler.json")
    if not payload:
        return {}
    providers = payload.get("providers", {})
    if not isinstance(providers, dict):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for provider, raw in providers.items():
        if not isinstance(raw, dict):
            continue
        normalized[str(provider)] = {
            "requests_per_second": (
                None
                if raw.get("requests_per_second") is None
                else float(raw.get("requests_per_second", 0.0))
            ),
            "execution_limit": int(raw.get("execution_limit", 0)),
            "queue_limit": int(raw.get("queue_limit", 0)),
            "active": int(raw.get("active", 0)),
            "buffered": int(raw.get("buffered", 0)),
            "reservations": int(raw.get("reservations", 0)),
            "refill_threshold": int(raw.get("refill_threshold", 0)),
            "seed_route_count": int(raw.get("seed_route_count", 0)),
            "cooldown": bool(raw.get("cooldown", False)),
            "unavailable": bool(raw.get("unavailable", False)),
        }
    return {
        "schema_version": int(payload.get("schema_version", 0)),
        "phase": str(payload.get("phase", "unknown")),
        "pid": int(payload.get("pid", 0)),
        "plan_token": payload.get("plan_token"),
        "wave": int(payload.get("wave", 0)),
        "attempted_this_run": int(payload.get("attempted_this_run", 0)),
        "global_worker_limit": int(payload.get("global_worker_limit", 0)),
        "global_queue_limit": int(payload.get("global_queue_limit", 0)),
        "preloaded_provider_queues": bool(
            payload.get("preloaded_provider_queues", False)
        ),
        "active_total": int(payload.get("active_total", 0)),
        "completed_pending_total": int(payload.get("completed_pending_total", 0)),
        "buffered_total": int(payload.get("buffered_total", 0)),
        "retry_deferred_total": int(payload.get("retry_deferred_total", 0)),
        "next_task_retry_at": payload.get("next_task_retry_at"),
        "completion_persistence_batch_size": int(
            payload.get("completion_persistence_batch_size", 0)
        ),
        "completion_backpressure_limit": int(
            payload.get("completion_backpressure_limit", 0)
        ),
        "completion_backpressure_active": bool(
            payload.get("completion_backpressure_active", False)
        ),
        "providers": dict(sorted(normalized.items())),
        "updated_at": payload.get("updated_at"),
    }


def _request_checkpoint_summary(state_dir: Path) -> dict[str, Any]:
    """Summarize incomplete request-level resumes without reading payloads."""
    root = state_dir / "request_checkpoints"
    summary: dict[str, Any] = {
        "task_count": 0,
        "file_count": 0,
        "bytes": 0,
        "corrupt_file_count": 0,
        "oldest_checkpoint_at": None,
        "newest_checkpoint_at": None,
        "by_provider": {},
    }
    if not root.is_dir():
        return summary
    oldest: float | None = None
    newest: float | None = None
    by_provider: dict[str, dict[str, int]] = {}
    try:
        provider_dirs = [path for path in root.iterdir() if path.is_dir()]
    except OSError:
        return summary
    for provider_dir in provider_dirs:
        provider = provider_dir.name
        aggregate = {"task_count": 0, "file_count": 0, "bytes": 0}
        try:
            task_dirs = [path for path in provider_dir.iterdir() if path.is_dir()]
        except OSError:
            continue
        for task_dir in task_dirs:
            try:
                files = [path for path in task_dir.iterdir() if path.is_file()]
            except OSError:
                continue
            if not files:
                continue
            aggregate["task_count"] += 1
            summary["task_count"] += 1
            for path in files:
                try:
                    stat = path.stat()
                except OSError:
                    continue
                aggregate["file_count"] += 1
                aggregate["bytes"] += int(stat.st_size)
                summary["file_count"] += 1
                summary["bytes"] += int(stat.st_size)
                if ".corrupt." in path.name:
                    summary["corrupt_file_count"] += 1
                oldest = stat.st_mtime if oldest is None else min(oldest, stat.st_mtime)
                newest = stat.st_mtime if newest is None else max(newest, stat.st_mtime)
        if aggregate["task_count"] > 0:
            by_provider[provider] = aggregate
    summary["by_provider"] = dict(sorted(by_provider.items()))
    if oldest is not None:
        summary["oldest_checkpoint_at"] = datetime.fromtimestamp(
            oldest, tz=timezone.utc
        ).isoformat()
    if newest is not None:
        summary["newest_checkpoint_at"] = datetime.fromtimestamp(
            newest, tz=timezone.utc
        ).isoformat()
    return summary


def _provider_parameter_maximums(state_dir: Path) -> list[dict[str, Any]]:
    """Read learned query-shape constraints used by every market task."""
    path = state_dir / "provider_cooldowns.json"
    if not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        raw = payload.get("parameter_maximums", [])
        if not isinstance(raw, list):
            return []
        constraints: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            provider = str(item.get("provider", "")).strip()
            endpoint = str(item.get("endpoint", "")).strip()
            parameter = str(item.get("parameter", "")).strip()
            maximum = int(item.get("maximum") or 0)
            if provider and endpoint and parameter and maximum > 0:
                constraints.append(
                    {
                        "provider": provider,
                        "endpoint": endpoint,
                        "parameter": parameter,
                        "maximum": maximum,
                    }
                )
        return sorted(
            constraints,
            key=lambda item: (
                item["provider"],
                item["endpoint"],
                item["parameter"],
            ),
        )
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return []


def _provider_omitted_parameters(state_dir: Path) -> list[dict[str, Any]]:
    """Read learned field omissions that preserve a legal provider query."""
    path = state_dir / "provider_cooldowns.json"
    if not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        raw = payload.get("omitted_parameters", [])
        if not isinstance(raw, list):
            return []
        constraints: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            provider = str(item.get("provider", "")).strip()
            endpoint = str(item.get("endpoint", "")).strip()
            parameters = sorted(
                {
                    str(value).strip()
                    for value in item.get("parameters", [])
                    if str(value).strip()
                }
            )
            if provider and endpoint and parameters:
                constraints.append(
                    {
                        "provider": provider,
                        "endpoint": endpoint,
                        "parameters": parameters,
                    }
                )
        return sorted(
            constraints,
            key=lambda item: (item["provider"], item["endpoint"]),
        )
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return []


def _print_human(status: dict[str, Any]) -> None:
    counts = status["status_counts"]
    process_state = (
        f"supervisor={status['supervisor_pid'] or '-'}:{'up' if status['supervisor_active'] else 'down'} "
        f"downloader={status['downloader_pid'] or '-'}:{'up' if status['downloader_active'] else 'down'}"
    )
    print(
        "[openbb-monitor] "
        f"accepted={status['accepted_tasks']:,}/{status['total_tasks']:,} "
        f"({status['completion_percent']:.4f}%) rows={status['success_rows']:,} "
        f"unavailable={status['unavailable_tasks']:,} "
        f"status={counts} retryable={status['retryable_tasks']:,} exhausted={status['exhausted_tasks']:,} "
        f"retry_deferred={status.get('pending_retry_deferred', 0):,} "
        f"next_retry={status.get('next_task_retry_at') or '-'} "
        f"pending_error={status['pending_with_error']:,} "
        f"rate_limited={status['pending_rate_limited']:,} "
        f"cooldown={status['pending_cooldown']:,} "
        f"other_plan_active={status['active_other_plan_tasks']:,} "
        f"pagination_gaps={status['pagination_gaps']:,} "
        f"execution_tracking={status.get('execution_tracking_available', False)} "
        f"buffered={status.get('buffered_running_tasks', 0):,} "
        f"executing={status.get('executing_tasks', 0):,} "
        f"{process_state} last_update={status['last_task_update']}",
        flush=True,
    )
    print(
        "[openbb-rate] "
        f"last15m={status['accepted_tasks_last_15m']:,} tasks "
        f"({status['tasks_per_minute_last_15m']:.2f}/min) "
        f"last_accepted={status['last_accepted_update']} "
        f"eta_hours={status['estimated_hours_at_recent_rate'] or '-'} "
        f"eta_blockers={status.get('eta_blocked_by_providers', [])} "
        f"active={status['active_endpoints']} disk_free={status['disk_free_bytes'] / 2**30:.1f}GiB",
        flush=True,
    )
    if status.get("snapshot_delta"):
        print(f"[openbb-delta] {status['snapshot_delta']}", flush=True)
    recent_progress = status.get("recent_progress", {})
    print(
        f"[openbb-categories] {status.get('category_progress', [])}",
        flush=True,
    )
    print(
        f"[openbb-providers] {status.get('provider_progress', [])}",
        flush=True,
    )
    print(
        "[openbb-recent] "
        f"categories={recent_progress.get('by_category', [])} "
        f"providers={recent_progress.get('by_provider', [])}",
        flush=True,
    )
    print(
        f"[openbb-constraints] {status['provider_constraints']}",
        flush=True,
    )
    if status.get("provider_runtime_limits"):
        print(
            f"[openbb-rate-limits] {status['provider_runtime_limits']}",
            flush=True,
        )
    if status.get("provider_scheduler"):
        print(
            f"[openbb-scheduler-pools] {status['provider_scheduler']}",
            flush=True,
        )
    request_checkpoints = status.get("request_checkpoints", {})
    if request_checkpoints.get("task_count") or request_checkpoints.get(
        "corrupt_file_count"
    ):
        print(
            f"[openbb-request-checkpoints] {request_checkpoints}",
            flush=True,
        )
    if status.get("provider_eta_projections"):
        print(
            f"[openbb-provider-eta] {status['provider_eta_projections']}",
            flush=True,
        )
    if status.get("category_eta_projections"):
        print(
            f"[openbb-category-eta] {status['category_eta_projections']}",
            flush=True,
        )
    if status.get("provider_parameter_maximums"):
        print(
            f"[openbb-parameter-limits] {status['provider_parameter_maximums']}",
            flush=True,
        )
    if status.get("provider_omitted_parameters"):
        print(
            f"[openbb-parameter-omissions] {status['provider_omitted_parameters']}",
            flush=True,
        )
    if status.get("active_provider_cooldowns"):
        print(
            f"[openbb-cooldowns] {status['active_provider_cooldowns']}",
            flush=True,
        )
    if status.get("provider_progress_stalls"):
        print(
            f"[openbb-provider-stalls] {status['provider_progress_stalls']}",
            flush=True,
        )
    provider_outcomes = status.get("provider_outcomes", {})
    print(
        "[openbb-provider-outcomes] "
        f"any={provider_outcomes.get('tasks_with_any', 0):,} "
        f"empty={provider_outcomes.get('tasks_with_empty', 0):,} "
        f"unavailable={provider_outcomes.get('tasks_with_unavailable', 0):,} "
        f"permanent={provider_outcomes.get('tasks_with_permanent', 0):,} "
        f"permanent_endpoints={provider_outcomes.get('permanent_endpoint_count', 0):,}",
        flush=True,
    )
    process = status.get("downloader_process", {})
    if process:
        print(
            "[openbb-process] "
            f"rss={process.get('rss_bytes', 0) / 2**30:.2f}GiB "
            f"rss_peak={process.get('rss_peak_bytes', 0) / 2**30:.2f}GiB "
            f"vms={process.get('virtual_bytes', 0) / 2**30:.2f}GiB "
            f"swap={process.get('swap_bytes', 0) / 2**30:.2f}GiB "
            f"threads={process.get('threads', 0):,} "
            f"fds={process.get('open_fds', 0):,}",
            flush=True,
        )
    endpoint_progress = status.get("endpoint_progress_summary", {})
    print(
        "[openbb-endpoints] "
        f"total={endpoint_progress.get('endpoint_count', 0):,} "
        f"started={endpoint_progress.get('started_endpoint_count', 0):,} "
        f"zero_accepted={endpoint_progress.get('zero_accepted_endpoint_count', 0):,} "
        f"zero_quota={endpoint_progress.get('zero_quota_blocked_endpoint_count', 0):,} "
        f"zero_capacity={endpoint_progress.get('zero_capacity_blocked_endpoint_count', 0):,} "
        f"zero_inflight={endpoint_progress.get('zero_inflight_endpoint_count', 0):,} "
        f"zero_unclassified={endpoint_progress.get('zero_without_recorded_blocker_count', 0):,} "
        f"high_empty={endpoint_progress.get('high_empty_endpoint_count', 0):,} "
        f"all_empty={endpoint_progress.get('all_empty_endpoint_count', 0):,} "
        f"resolved={endpoint_progress.get('resolved_endpoint_count', 0):,} "
        f"unresolved={endpoint_progress.get('unresolved_endpoint_count', 0):,}",
        flush=True,
    )
    if status.get("retryable_failure_clusters"):
        print(
            "[openbb-failures] "
            f"clusters={len(status['retryable_failure_clusters']):,} "
            f"systematic={len(status['systematic_failure_clusters']):,} "
            f"systematic_tasks={status['systematic_retryable_failure_tasks']:,} "
            f"top={status['retryable_failure_clusters'][:5]}",
            flush=True,
        )
    if status.get("non_authoritative_empty_clusters"):
        print(
            "[openbb-false-empty] "
            f"tasks={status['non_authoritative_empty_tasks']:,} "
            f"clusters={status['non_authoritative_empty_clusters'][:5]}",
            flush=True,
        )
    if status.get("non_authoritative_unavailable_clusters"):
        print(
            "[openbb-false-unavailable] "
            f"tasks={status['non_authoritative_unavailable_tasks']:,} "
            f"clusters={status['non_authoritative_unavailable_clusters'][:5]}",
            flush=True,
        )
    if status.get("unproven_unavailable_clusters") or status.get(
        "unproven_provider_capability_constraints"
    ):
        print(
            "[openbb-unproven-unavailable] "
            f"tasks={status.get('unproven_unavailable_tasks', 0):,} "
            f"clusters={status.get('unproven_unavailable_clusters', [])[:5]} "
            "invalid_constraints="
            f"{status.get('unproven_provider_capability_constraints', [])[:5]}",
            flush=True,
        )
    if status.get("source_cap_saturations"):
        print(
            "[openbb-cap-audit] "
            f"saturations={status['source_cap_saturation_count']:,} "
            f"samples={status['source_cap_saturations'][:5]}",
            flush=True,
        )
    if status.get("declared_limit_saturations"):
        print(
            "[openbb-limit-audit] "
            f"saturations={status['declared_limit_saturation_count']:,} "
            f"samples={status['declared_limit_saturations'][:5]}",
            flush=True,
        )
    if status.get("entitlement_limit_saturations"):
        print(
            "[openbb-entitlement-cap] "
            f"saturations={status['entitlement_limit_saturation_count']:,} "
            f"samples={status['entitlement_limit_saturations'][:5]}",
            flush=True,
        )
    contract = status.get("completeness_contract", {})
    print(
        "[openbb-contract] "
        f"present={contract.get('present', False)} "
        f"passed={contract.get('passed', False)} "
        f"unresolved={contract.get('unresolved', 0)} "
        f"by_axis={contract.get('unresolved_by_axis', {})}",
        flush=True,
    )
    print(
        f"[openbb-health] health={status['health']} alerts={status['alerts']}",
        flush=True,
    )
    audit = status.get("file_audit")
    if audit is not None:
        print(
            "[openbb-audit] "
            f"files={audit['checked_files']:,} rows={audit['checked_rows']:,} "
            f"missing={audit['missing_files']:,} unreadable={audit['unreadable_files']:,} "
            f"row_mismatch={audit['row_mismatch_files']:,} "
            f"metadata_mismatch={audit['metadata_mismatch_files']:,} "
            f"encoded_wrapper={audit['encoded_wrapper_files']:,} "
            f"zero_row_success={audit['success_zero_row_tasks']:,} "
            f"duplicate_paths={audit['duplicate_output_paths']:,} "
            f"passed={audit['passed']}",
            flush=True,
        )
    followups = status.get("catalog_followup_audit")
    if followups is not None:
        print(
            "[openbb-followup-audit] "
            f"parents={followups['checked_parent_files']:,} "
            f"required={followups['required_unique_followups']:,} "
            f"missing={followups['missing_followups']:,} "
            f"parent_read_failures={followups['parent_read_failures']:,} "
            f"passed={followups['passed']}",
            flush=True,
        )


def run(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    state_dir = args.output_dir / "_state"
    previous_snapshot = _read_json_object(state_dir / "monitor_latest.json")
    if (
        args.field
        and previous_snapshot
        and not args.audit_files
        and not args.write_snapshot
        and not args.append_history
        and not args.fail_on_incomplete
    ):
        try:
            snapshot_age = (
                _utc_now()
                - datetime.fromisoformat(str(previous_snapshot["checked_at"]))
            ).total_seconds()
        except (KeyError, TypeError, ValueError):
            snapshot_age = float("inf")
        if 0 <= snapshot_age <= 120:
            value = previous_snapshot.get(args.field)
            if isinstance(value, (dict, list)):
                print(json.dumps(value, ensure_ascii=False, separators=(",", ":")))
            elif isinstance(value, bool):
                print("true" if value else "false")
            elif value is None:
                print("")
            else:
                print(value)
            return 0
    status = collect_status(
        args.output_dir,
        max_total_attempts=max(1, args.max_total_attempts),
        stale_running_minutes=max(1, args.stale_running_minutes),
        accepted_stall_minutes=max(1, args.accepted_stall_minutes),
        min_free_gib=max(0.0, args.min_free_gib),
        audit_files=args.audit_files,
        show_progress=not args.no_progress,
    )
    status["snapshot_delta"] = _snapshot_delta(previous_snapshot, status)
    if args.write_snapshot:
        _atomic_write_json(state_dir / "monitor_latest.json", status)
        provider_eta_rows = []
        for row in status.get("provider_eta_projections", []):
            flat = dict(row)
            flat["daily_quota_lower_bound_json"] = json.dumps(
                flat.pop("daily_quota_lower_bound", None),
                ensure_ascii=False,
                separators=(",", ":"),
            )
            flat["observed_quota_limit_json"] = json.dumps(
                flat.pop("observed_quota_limit", None),
                ensure_ascii=False,
                separators=(",", ":"),
            )
            flat["checked_at"] = status["checked_at"]
            provider_eta_rows.append(flat)
        category_eta_rows = []
        for row in status.get("category_eta_projections", []):
            flat = dict(row)
            flat["eta_blocked_by_exclusive_providers_json"] = json.dumps(
                flat.pop("eta_blocked_by_exclusive_providers", []),
                ensure_ascii=False,
                separators=(",", ":"),
            )
            flat["exclusive_backlog_by_provider_json"] = json.dumps(
                flat.pop("exclusive_backlog_by_provider", {}),
                ensure_ascii=False,
                separators=(",", ":"),
            )
            flat["checked_at"] = status["checked_at"]
            category_eta_rows.append(flat)
        provider_endpoint_eta_rows = []
        for row in status.get("provider_endpoint_backlogs", []):
            flat = dict(row)
            flat["checked_at"] = status["checked_at"]
            provider_endpoint_eta_rows.append(flat)
        _atomic_write_parquet(
            args.output_dir / "catalog" / "provider_eta.parquet",
            provider_eta_rows,
        )
        _atomic_write_parquet(
            args.output_dir / "catalog" / "category_eta.parquet",
            category_eta_rows,
        )
        _atomic_write_parquet(
            args.output_dir / "catalog" / "provider_endpoint_eta.parquet",
            provider_endpoint_eta_rows,
        )
        if args.audit_files:
            # A lightweight minute monitor intentionally omits expensive file
            # and follow-up scans.  Preserve the most recent completed full
            # audit separately so the next lightweight snapshot cannot erase
            # the durable evidence used by the completion gate.
            _atomic_write_json(state_dir / "audit_latest.json", status)
    if args.append_history:
        history = state_dir / "monitor_history.jsonl"
        dashboard_history = state_dir / "monitor_dashboard_history.jsonl"
        history.parent.mkdir(parents=True, exist_ok=True)
        with history.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(status, ensure_ascii=False, separators=(",", ":")) + "\n"
            )
        dashboard_row = project_openbb_history_row(status)
        if dashboard_row:
            with dashboard_history.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        dashboard_row,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    + "\n"
                )
    if args.field:
        value = status.get(args.field)
        if isinstance(value, (dict, list)):
            print(json.dumps(value, ensure_ascii=False, separators=(",", ":")))
        elif isinstance(value, bool):
            print("true" if value else "false")
        elif value is None:
            print("")
        else:
            print(value)
    elif args.json:
        print(json.dumps(status, ensure_ascii=False, indent=2))
    else:
        _print_human(status)
    return 2 if args.fail_on_incomplete and not status["complete"] else 0


if __name__ == "__main__":
    raise SystemExit(run())

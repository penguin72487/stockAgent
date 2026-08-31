#!/usr/bin/env python3
"""Persist and repair the unattended TW day-trade operational contract.

This guardian does not download data, infer a model, create a signal, or own a
broker connection.  It checks the receipts produced by those canonical
components and only re-arms their existing systemd units.  Active Shioaji and
Discord processes are never restarted by this script because their clients are
process-local and a restart would discard warm contracts and subscriptions.
"""

from __future__ import annotations

import argparse
from datetime import datetime, time as datetime_time
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from typing import Any
from urllib.request import urlopen
import uuid
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.download_tw_public_data import DEFAULT_DATASETS  # noqa: E402


TAIPEI = ZoneInfo("Asia/Taipei")
EXPECTED_MARKETS = (
    "tw_day_trade_100m",
    "tw_day_trade_multi_basis",
    "tw_day_trade_multi_basis_projection_l1_gelu",
)
REQUIRED_SERVICES = (
    "stockagent-tw-day-trade-simulation.service",
    "stockagent-discord-bot.service",
    "stockagent-tw-public-source-events.service",
    "stockagent-public-dashboards.service",
)
REQUIRED_TIMERS = (
    "stockagent-time-sync-check.timer",
    "stockagent-tw-day-trade-eligibility.timer",
    "stockagent-tw-public-publication-sweep.timer",
    "stockagent-tw-public-0830-check.timer",
    "stockagent-tw-day-trade-preopen-gate.timer",
    "stockagent-discord-artifact-maintenance.timer",
    "stockagent-tw-day-trade-unattended-guardian.timer",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state-root",
        type=Path,
        default=Path("artifacts/operations/tw_day_trade_guardian"),
    )
    parser.add_argument("--no-repair", action="store_true")
    parser.add_argument("--action-cooldown-seconds", type=float, default=300.0)
    parser.add_argument("--minimum-free-gib", type=float, default=5.0)
    parser.add_argument("--minimum-free-percent", type=float, default=5.0)
    parser.add_argument("--observed-at", default=None)
    return parser.parse_args()


def _repo_path(path: Path) -> Path:
    return path if path.is_absolute() else REPO_ROOT / path


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _prune_managed_runs(
    directory: Path, *, max_files: int = 4096, max_age_days: int = 90
) -> None:
    """Bound only guardian-generated observation receipts."""

    try:
        rows = sorted(
            (path for path in directory.glob("*.json") if path.is_file()),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
    except OSError:
        return
    cutoff = time.time() - max_age_days * 86400
    for index, path in enumerate(rows):
        try:
            if index >= max_files or path.stat().st_mtime < cutoff:
                path.unlink(missing_ok=True)
        except OSError:
            continue


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _parse_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=TAIPEI)


def _age_seconds(value: Any, observed: datetime) -> float | None:
    parsed = _parse_time(value)
    if parsed is None:
        return None
    return max(0.0, (observed - parsed.astimezone(TAIPEI)).total_seconds())


def _systemctl_show(unit: str) -> dict[str, str]:
    completed = subprocess.run(
        [
            "systemctl",
            "show",
            unit,
            "--property=LoadState,ActiveState,SubState,UnitFileState,Result,NRestarts",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    rows: dict[str, str] = {}
    for line in completed.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            rows[key] = value
    rows["show_returncode"] = str(completed.returncode)
    return rows


def _run_systemctl(*arguments: str) -> dict[str, Any]:
    completed = subprocess.run(
        ["systemctl", *arguments],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return {
        "command": ["systemctl", *arguments],
        "returncode": int(completed.returncode),
        "stdout": completed.stdout[-1000:],
        "stderr": completed.stderr[-1000:],
    }


def _cooldown_ready(
    action_key: str,
    *,
    action_state: dict[str, Any],
    now_monotonic_wall: float,
    cooldown_seconds: float,
) -> bool:
    try:
        prior = float((action_state.get(action_key) or {}).get("attempted_at_epoch"))
    except (AttributeError, TypeError, ValueError):
        return True
    return now_monotonic_wall - prior >= cooldown_seconds


def _record_action(
    action_key: str,
    result: dict[str, Any],
    *,
    action_state: dict[str, Any],
    observed: datetime,
) -> None:
    action_state[action_key] = {
        "attempted_at_epoch": time.time(),
        "attempted_at_taipei": observed.isoformat(timespec="seconds"),
        "returncode": result.get("returncode"),
    }


def _repair_unit(
    unit: str,
    *,
    timer: bool,
    repair: bool,
    action_state: dict[str, Any],
    observed: datetime,
    cooldown_seconds: float,
) -> list[dict[str, Any]]:
    if not repair:
        return []
    key = f"enable-start:{unit}"
    if not _cooldown_ready(
        key,
        action_state=action_state,
        now_monotonic_wall=time.time(),
        cooldown_seconds=cooldown_seconds,
    ):
        return []
    actions: list[dict[str, Any]] = []
    reset = _run_systemctl("reset-failed", unit)
    reset["action_key"] = key
    actions.append(reset)
    start_args = ("enable", "--now", unit) if timer else (
        "enable",
        "--now",
        unit,
    )
    started = _run_systemctl(*start_args)
    started["action_key"] = key
    actions.append(started)
    _record_action(key, started, action_state=action_state, observed=observed)
    return actions


def _trigger_oneshot(
    unit: str,
    *,
    repair: bool,
    action_state: dict[str, Any],
    observed: datetime,
    cooldown_seconds: float,
) -> list[dict[str, Any]]:
    if not repair:
        return []
    key = f"start:{unit}"
    if not _cooldown_ready(
        key,
        action_state=action_state,
        now_monotonic_wall=time.time(),
        cooldown_seconds=cooldown_seconds,
    ):
        return []
    result = _run_systemctl("start", "--no-block", unit)
    result["action_key"] = key
    _record_action(key, result, action_state=action_state, observed=observed)
    return [result]


def _clear_receipted_oneshot_failure(
    unit: str,
    *,
    repair: bool,
    action_state: dict[str, Any],
    observed: datetime,
    cooldown_seconds: float,
) -> list[dict[str, Any]]:
    """Clear only systemd's latch; the immutable incident receipt is retained."""

    if not repair:
        return []
    key = f"reset-receipted:{unit}"
    if not _cooldown_ready(
        key,
        action_state=action_state,
        now_monotonic_wall=time.time(),
        cooldown_seconds=cooldown_seconds,
    ):
        return []
    result = _run_systemctl("reset-failed", unit)
    result["action_key"] = key
    result["incident_receipt_retained"] = True
    _record_action(key, result, action_state=action_state, observed=observed)
    return [result]


def _run_time_check(*, repair: bool) -> dict[str, Any]:
    command = [
        sys.executable,
        str(REPO_ROOT / "scripts/check_stockagent_time_sync.py"),
    ]
    if repair:
        command.append("--repair")
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    receipt = _json(REPO_ROOT / "artifacts/operations/time_sync/latest.json")
    return {
        "returncode": int(completed.returncode),
        "ready": completed.returncode == 0 and receipt.get("ready") is True,
        "receipt": receipt,
        "stderr": completed.stderr[-1000:],
    }


def _public_endpoint(path: str) -> dict[str, Any]:
    port = int(os.getenv("STOCKAGENT_PUBLIC_DASHBOARD_PORT", "8770"))
    url = f"http://127.0.0.1:{port}{path}"
    try:
        with urlopen(url, timeout=3) as response:  # noqa: S310 - localhost only
            value = json.loads(response.read())
        return {
            "ready": isinstance(value, dict),
            "status_code": 200,
            "url": url,
            "payload": value if isinstance(value, dict) else {},
        }
    except Exception as exc:
        return {
            "ready": False,
            "url": url,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _disk_health(path: Path, *, minimum_gib: float, minimum_percent: float) -> dict[str, Any]:
    try:
        usage = shutil.disk_usage(path)
    except OSError as exc:
        return {
            "path": str(path),
            "ready": False,
            "error": f"{type(exc).__name__}: {exc}",
            "policy": {
                "minimum_free_gib": minimum_gib,
                "minimum_free_percent": minimum_percent,
                "automatic_deletion": False,
            },
        }
    free_gib = usage.free / (1024**3)
    free_percent = (usage.free / usage.total * 100.0) if usage.total else 0.0
    return {
        "path": str(path),
        "free_gib": round(free_gib, 3),
        "free_percent": round(free_percent, 3),
        "ready": free_gib >= minimum_gib and free_percent >= minimum_percent,
        "policy": {
            "minimum_free_gib": minimum_gib,
            "minimum_free_percent": minimum_percent,
            "automatic_deletion": False,
        },
    }


def main() -> int:
    args = parse_args()
    observed = (
        datetime.fromisoformat(args.observed_at).astimezone(TAIPEI)
        if args.observed_at
        else datetime.now(TAIPEI)
    )
    state_root = _repo_path(args.state_root)
    state_root.mkdir(parents=True, exist_ok=True)
    lock_path = state_root / "guardian.lock"
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0

        repair = not bool(args.no_repair)
        action_path = state_root / "action_state.json"
        action_state = _json(action_path)
        actions: list[dict[str, Any]] = []
        failures: list[str] = []
        warnings: list[str] = []

        time_health = _run_time_check(repair=repair)
        if not time_health["ready"]:
            failures.append("schedule clock is not verified")

        services: dict[str, Any] = {}
        for unit in REQUIRED_SERVICES:
            row = _systemctl_show(unit)
            services[unit] = row
            if row.get("ActiveState") != "active":
                failures.append(f"required service is inactive: {unit}")
                actions.extend(
                    _repair_unit(
                        unit,
                        timer=False,
                        repair=repair,
                        action_state=action_state,
                        observed=observed,
                        cooldown_seconds=float(args.action_cooldown_seconds),
                    )
                )

        timers: dict[str, Any] = {}
        for unit in REQUIRED_TIMERS:
            row = _systemctl_show(unit)
            timers[unit] = row
            if row.get("ActiveState") != "active" or row.get("UnitFileState") not in {
                "enabled",
                "static",
            }:
                failures.append(f"required weekly timer is not armed: {unit}")
                actions.extend(
                    _repair_unit(
                        unit,
                        timer=True,
                        repair=repair,
                        action_state=action_state,
                        observed=observed,
                        cooldown_seconds=float(args.action_cooldown_seconds),
                    )
                )

        event_receipt = _json(
            REPO_ROOT / "artifacts/data_refresh/tw_public/events/latest.json"
        )
        event_age = _age_seconds(event_receipt.get("updated_at_taipei"), observed)
        event_blocking = int(
            (
                event_receipt.get("blocking_unapplied_event_count")
                if "blocking_unapplied_event_count" in event_receipt
                else event_receipt.get("unapplied_event_count")
            )
            or 0
        )
        event_ready = bool(
            event_receipt.get("coverage_complete") is True
            and int(event_receipt.get("registered_dataset_count") or -1)
            == len(DEFAULT_DATASETS)
            and int(event_receipt.get("observed_dataset_count") or -1)
            == len(DEFAULT_DATASETS)
            and int(event_receipt.get("failed_probe_count") or 0) == 0
            and event_blocking == 0
            and event_age is not None
            and event_age <= 180.0
        )
        if not event_ready:
            warnings.append("TW public source-event receipt is stale or degraded")

        weekday = observed.weekday() < 5
        wall = observed.timetz().replace(tzinfo=None)
        session_date = observed.date().isoformat()
        eligibility = _json(
            REPO_ROOT / "artifacts/data_refresh/tw_day_trade_eligibility/latest.json"
        )
        eligibility_ready = bool(
            eligibility.get("status") == "ok"
            and eligibility.get("trading_date") == session_date
        )
        if weekday and datetime_time(5, 30) <= wall <= datetime_time(10, 0):
            if not eligibility_ready:
                warnings.append("same-session TWSE/TPEx eligibility is not accepted")
                actions.extend(
                    _trigger_oneshot(
                        "stockagent-tw-day-trade-eligibility.service",
                        repair=repair,
                        action_state=action_state,
                        observed=observed,
                        cooldown_seconds=float(args.action_cooldown_seconds),
                    )
                )

        public_acceptance = _json(
            REPO_ROOT / "artifacts/data_refresh/tw_public/0830/latest.json"
        )
        public_started = str(public_acceptance.get("started_at_taipei") or "")[:10]
        public_ready = bool(
            public_acceptance.get("status") == "ok"
            and public_started == session_date
            and (public_acceptance.get("acceptance") or {}).get(
                "live_root_receipt_fresh"
            )
            is True
        )
        if weekday and datetime_time(8, 0) <= wall <= datetime_time(10, 0):
            if not public_ready:
                warnings.append("08:30 TW public-data acceptance is not ready")
                actions.extend(
                    _trigger_oneshot(
                        "stockagent-tw-public-0830-check.service",
                        repair=repair,
                        action_state=action_state,
                        observed=observed,
                        cooldown_seconds=float(args.action_cooldown_seconds),
                    )
                )

        # The gate intentionally exits nonzero when the 09:00 SLO was missed.
        # After its last catch-up point, retain that truth in the gate receipt
        # and dashboard but clear systemd's failed latch so boot-health checks
        # do not confuse a receipted historical incident with a dead daemon.
        preopen_unit = "stockagent-tw-day-trade-preopen-gate.service"
        preopen_systemd = _systemctl_show(preopen_unit)
        if wall >= datetime_time(10, 5) and preopen_systemd.get("ActiveState") == "failed":
            actions.extend(
                _clear_receipted_oneshot_failure(
                    preopen_unit,
                    repair=repair,
                    action_state=action_state,
                    observed=observed,
                    cooldown_seconds=float(args.action_cooldown_seconds),
                )
            )

        engine_sync_path = (
            REPO_ROOT / "artifacts/live/tw_day_trade_simulation/service_sync.json"
        )
        discord_path = REPO_ROOT / "artifacts/discord_bot/service_status.json"
        engine_sync: dict[str, Any] = {}
        discord_status: dict[str, Any] = {}
        revision_lag: int | None = None
        for attempt in range(3):
            engine_sync = _json(engine_sync_path)
            discord_status = _json(discord_path)
            revision_lag = int(engine_sync.get("state_revision") or 0) - int(
                discord_status.get("engine_state_revision") or 0
            )
            if revision_lag == 0:
                break
            if attempt < 2:
                time.sleep(1.0)
        engine_age = _age_seconds(engine_sync.get("published_at"), observed)
        discord_age = _age_seconds(discord_status.get("updated_at"), observed)
        modes = engine_sync.get("modes")
        modes = dict(modes) if isinstance(modes, dict) else {}
        runtime_ready = bool(
            engine_sync.get("simulation_only") is True
            and engine_sync.get("production_order_possible") is False
            and engine_sync.get("ledger_integrity_ready") is True
            and set(engine_sync.get("enabled_markets") or ()) == set(EXPECTED_MARKETS)
            and discord_status.get("simulation_only") is True
            and discord_status.get("production_order_possible") is False
            and discord_status.get("discord_connected") is True
            and set(discord_status.get("day_trade_markets") or ())
            == set(EXPECTED_MARKETS)
            and engine_age is not None
            and engine_age <= 20.0
            and discord_age is not None
            and discord_age <= 20.0
            and revision_lag == 0
        )
        if not runtime_ready:
            failures.append("paper engine and Discord revisions are not synchronized")
        maintenance = discord_status.get("background_maintenance")
        maintenance = dict(maintenance) if isinstance(maintenance, dict) else {}
        maintenance_degraded = bool(
            maintenance.get("status") == "degraded"
            or int(maintenance.get("failed_count") or 0) > 0
        )
        if maintenance_degraded:
            # Formal-history maintenance is intentionally a separate health
            # domain: make failure visible without declaring the Gateway or
            # the independent opening execution engine disconnected.
            warnings.append("post-close Discord artifact maintenance is degraded")

        if weekday and wall >= datetime_time(9, 0, 15):
            missing_signals = [
                market
                for market in EXPECTED_MARKETS
                if (modes.get(market) or {}).get("session_date") != session_date
                or not (modes.get(market) or {}).get("signal_id")
                or not (modes.get(market) or {}).get("entry_completed_at")
                or (modes.get(market) or {}).get("entry_fill_policy")
                != "causal_best_quote"
                or int(
                    (modes.get(market) or {}).get("entry_price_offset_ticks") or 0
                )
                != 0
            ]
            if missing_signals:
                failures.append(
                    "current-session signal commit missing: " + ",".join(missing_signals)
                )
        else:
            missing_signals = []

        if weekday and wall >= datetime_time(13, 30):
            open_markets = [
                market
                for market in EXPECTED_MARKETS
                if int((modes.get(market) or {}).get("open_position_count") or 0) != 0
            ]
            if open_markets:
                failures.append(
                    "post-close paper positions are not flat: " + ",".join(open_markets)
                )
        else:
            open_markets = []

        dashboard_status = _public_endpoint("/tw-day-trade/api/status")
        dashboard_revision = _public_endpoint("/tw-day-trade/api/revision")
        public_surface_ready = bool(
            dashboard_status.get("ready")
            and dashboard_revision.get("ready")
            and (dashboard_status.get("payload") or {}).get("simulation_only") is True
            and (dashboard_status.get("payload") or {}).get(
                "production_order_possible"
            )
            is False
            and (dashboard_status.get("payload") or {})
            .get("ledger_integrity", {})
            .get("ready")
            is True
        )
        if not public_surface_ready:
            failures.append("read-only TW day-trade dashboard endpoint is unavailable")

        disks = {
            "repository": _disk_health(
                REPO_ROOT,
                minimum_gib=float(args.minimum_free_gib),
                minimum_percent=float(args.minimum_free_percent),
            ),
            "tw_public_live": _disk_health(
                Path("/srv/stockagent-live/data_tw_public"),
                minimum_gib=float(args.minimum_free_gib),
                minimum_percent=float(args.minimum_free_percent),
            ),
        }
        if any(not row["ready"] for row in disks.values()):
            failures.append("disk free-space guard is below threshold; no data was deleted")

        if actions:
            status = "repairing"
        elif failures:
            status = "failed"
        elif warnings:
            status = "degraded"
        else:
            status = "ready"
        payload = {
            "schema_version": 1,
            "status": status,
            "ready": not failures and not warnings,
            "repair_enabled": repair,
            "simulation_only": True,
            "production_order_possible": False,
            "observed_at_taipei": observed.isoformat(timespec="milliseconds"),
            "session_date": session_date,
            "expected_markets": list(EXPECTED_MARKETS),
            "failures": failures,
            "warnings": warnings,
            "actions": actions,
            "components": {
                "time_sync": time_health,
                "services": services,
                "weekly_timers": timers,
                "source_events": {
                    "ready": event_ready,
                    "age_seconds": event_age,
                    "blocking_unapplied_event_count": event_blocking,
                    "registered_dataset_count": event_receipt.get(
                        "registered_dataset_count"
                    ),
                    "observed_dataset_count": event_receipt.get(
                        "observed_dataset_count"
                    ),
                    "failed_probe_count": event_receipt.get("failed_probe_count"),
                },
                "eligibility": {
                    "ready": eligibility_ready,
                    "status": eligibility.get("status"),
                    "trading_date": eligibility.get("trading_date"),
                },
                "public_0830": {
                    "ready": public_ready,
                    "status": public_acceptance.get("status"),
                    "started_at_taipei": public_acceptance.get(
                        "started_at_taipei"
                    ),
                },
                "runtime_sync": {
                    "ready": runtime_ready,
                    "engine_age_seconds": engine_age,
                    "discord_age_seconds": discord_age,
                    "revision_lag": revision_lag,
                    "engine_run_id": engine_sync.get("engine_run_id"),
                    "enabled_markets": engine_sync.get("enabled_markets"),
                },
                "post_close_artifact_maintenance": {
                    "ready": not maintenance_degraded,
                    **maintenance,
                },
                "session_signals": {
                    "ready": not missing_signals,
                    "missing_markets": missing_signals,
                    "modes": modes,
                },
                "post_close_flat": {
                    "ready": not open_markets,
                    "open_markets": open_markets,
                },
                "public_dashboard": {
                    "ready": public_surface_ready,
                    "status_endpoint": {
                        key: value
                        for key, value in dashboard_status.items()
                        if key != "payload"
                    },
                    "revision_endpoint": {
                        key: value
                        for key, value in dashboard_revision.items()
                        if key != "payload"
                    },
                },
                "disks": disks,
            },
        }
        _atomic_json(action_path, action_state)
        _atomic_json(state_root / "latest.json", payload)
        # A minute heartbeat belongs in ``latest``.  Preserve an immutable run
        # only on the hourly boundary or when it contains an incident/action.
        if observed.minute == 0 or status != "ready" or actions:
            run_id = observed.strftime("%Y%m%dT%H%M%S%f")
            runs_dir = state_root / "runs"
            _atomic_json(runs_dir / f"{run_id}.json", payload)
            _prune_managed_runs(runs_dir)
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

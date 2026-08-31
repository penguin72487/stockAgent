#!/usr/bin/env python3
"""Receipt the final 09:00 TW day-trade paper-simulation readiness gate."""

from __future__ import annotations

import argparse
from datetime import datetime, time as datetime_time
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid
from typing import Any, Mapping
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.config import load_config  # noqa: E402
from downloader.download_tw_public_data import DEFAULT_DATASETS  # noqa: E402
from stockagent.live.market_config import load_market_configs  # noqa: E402
from stockagent.live.market_status import verified_tw_stock_session_day  # noqa: E402


TAIPEI = ZoneInfo("Asia/Taipei")
REQUIRED_SERVICES = (
    "stockagent-discord-bot.service",
    "stockagent-tw-day-trade-simulation.service",
)
EVENT_MONITOR_SERVICE = "stockagent-tw-public-source-events.service"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--public-receipt",
        type=Path,
        default=Path("artifacts/data_refresh/tw_public/0830/latest.json"),
    )
    parser.add_argument(
        "--model-receipt",
        type=Path,
        default=Path("artifacts/discord_bot/preopen_readiness.json"),
    )
    parser.add_argument(
        "--simulation-receipt",
        type=Path,
        default=Path("artifacts/live/tw_day_trade_simulation/preopen_readiness.json"),
    )
    parser.add_argument(
        "--event-receipt",
        type=Path,
        default=Path("artifacts/data_refresh/tw_public/events/latest.json"),
    )
    parser.add_argument(
        "--engine-status",
        type=Path,
        default=Path("artifacts/live/tw_day_trade_simulation/status.json"),
    )
    parser.add_argument(
        "--engine-sync",
        type=Path,
        default=Path("artifacts/live/tw_day_trade_simulation/service_sync.json"),
    )
    parser.add_argument(
        "--discord-status",
        type=Path,
        default=Path("artifacts/discord_bot/service_status.json"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/data_refresh/tw_public/preopen_gate/latest.json"),
    )
    parser.add_argument("--strict-after", default="08:57:00")
    return parser.parse_args()


def _repo_path(path: Path) -> Path:
    return path if path.is_absolute() else REPO_ROOT / path


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _same_session(value: object, session_date: str) -> bool:
    return str(value or "")[:10] == session_date


def _parse_time(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=TAIPEI)


def _service_states() -> dict[str, str]:
    states: dict[str, str] = {}
    for service in (*REQUIRED_SERVICES, EVENT_MONITOR_SERVICE):
        completed = subprocess.run(
            ("systemctl", "is-active", service),
            check=False,
            capture_output=True,
            text=True,
            timeout=3.0,
        )
        states[service] = (completed.stdout or completed.stderr).strip()
    return states


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_synchronized_runtime_receipts(
    engine_path: Path,
    discord_path: Path,
    *,
    timeout_seconds: float = 3.0,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read one matching engine/Discord revision without racing heartbeats."""

    deadline = time.monotonic() + max(0.0, float(timeout_seconds))
    engine: dict[str, Any] = {}
    discord: dict[str, Any] = {}
    while True:
        engine = _read_json(engine_path)
        discord = _read_json(discord_path)
        if (
            engine.get("engine_run_id")
            and engine.get("engine_run_id") == discord.get("engine_run_id")
            and int(engine.get("state_revision") or 0)
            == int(discord.get("engine_state_revision") or -1)
        ):
            return engine, discord
        if time.monotonic() >= deadline:
            return engine, discord
        time.sleep(0.1)


def evaluate_readiness(
    *,
    observed: datetime,
    strict_after: datetime_time,
    market_names: tuple[str, ...],
    public_receipt: Mapping[str, Any],
    model_receipt: Mapping[str, Any],
    simulation_receipt: Mapping[str, Any],
    service_states: Mapping[str, str],
    event_receipt: Mapping[str, Any] | None = None,
    engine_status_receipt: Mapping[str, Any] | None = None,
    engine_sync_receipt: Mapping[str, Any] | None = None,
    discord_status_receipt: Mapping[str, Any] | None = None,
    opening_check_after: datetime_time = datetime_time(9, 0, 15),
    opening_commit_slo_seconds: float = 15.0,
) -> dict[str, Any]:
    session_date = observed.astimezone(TAIPEI).date().isoformat()
    failures: list[str] = []
    acceptance = public_receipt.get("acceptance")
    acceptance = dict(acceptance) if isinstance(acceptance, Mapping) else {}
    public_ready = bool(
        public_receipt.get("status") == "ok"
        and _same_session(public_receipt.get("started_at_taipei"), session_date)
        and acceptance.get("subprocess_ok") is True
        and acceptance.get("live_root_receipt_fresh") is True
        and acceptance.get("live_root_status_ok") is True
        and acceptance.get("live_root_verified") is True
        and acceptance.get("coverage_complete") is True
        and acceptance.get("same_session_eligibility") is True
        and acceptance.get("runtime_materialization_required") is False
        and acceptance.get("runtime_materialized_snapshot") is False
    )
    if not public_ready:
        failures.append("08:30 public-data acceptance receipt is not ready")

    event_monitor: dict[str, Any] | None = None
    if event_receipt is not None:
        event_updated = _parse_time(event_receipt.get("updated_at_taipei"))
        age_seconds = (
            (observed.astimezone(TAIPEI) - event_updated.astimezone(TAIPEI)).total_seconds()
            if event_updated is not None
            else None
        )
        expected = len(DEFAULT_DATASETS)
        blocking_unapplied = (
            event_receipt.get("blocking_unapplied_event_count")
            if "blocking_unapplied_event_count" in event_receipt
            else event_receipt.get("unapplied_event_count")
        )
        event_ready = bool(
            event_receipt.get("status") == "ok"
            and event_receipt.get("coverage_complete") is True
            and int(event_receipt.get("registered_dataset_count") or -1) == expected
            and int(event_receipt.get("monitored_dataset_count") or -1) == expected
            and int(event_receipt.get("observed_dataset_count") or -1) == expected
            and event_receipt.get("failed_probe_count") == 0
            and int(blocking_unapplied or 0) == 0
            and age_seconds is not None
            and -60.0 <= age_seconds <= 120.0
        )
        event_monitor = {
            "ready": event_ready,
            "status": event_receipt.get("status"),
            "coverage_complete": event_receipt.get("coverage_complete"),
            "registered_dataset_count": event_receipt.get(
                "registered_dataset_count"
            ),
            "observed_dataset_count": event_receipt.get("observed_dataset_count"),
            "failed_probe_count": event_receipt.get("failed_probe_count"),
            "unapplied_event_count": event_receipt.get("unapplied_event_count"),
            "blocking_unapplied_event_count": blocking_unapplied,
            "opening_apply_deferred": event_receipt.get(
                "opening_apply_deferred"
            ),
            "opening_apply_deferred_count": event_receipt.get(
                "opening_apply_deferred_count"
            ),
            "updated_at": event_receipt.get("updated_at_taipei"),
            "heartbeat_age_seconds": age_seconds,
        }
        if not event_ready:
            failures.append("156-dataset source-event monitor is not healthy")

    model_rows = model_receipt.get("markets")
    model_rows = dict(model_rows) if isinstance(model_rows, Mapping) else {}
    model_run_id = str(model_receipt.get("run_id") or "")
    model_results: dict[str, dict[str, Any]] = {}
    for market in market_names:
        row = model_rows.get(market)
        row = dict(row) if isinstance(row, Mapping) else {}
        final_arm = row.get("final_arm")
        final_arm = dict(final_arm) if isinstance(final_arm, Mapping) else {}
        latency = final_arm.get("live_latency")
        latency = dict(latency) if isinstance(latency, Mapping) else {}
        opening_prewarm = final_arm.get("opening_source_prewarm")
        opening_prewarm = (
            dict(opening_prewarm)
            if isinstance(opening_prewarm, Mapping)
            else {}
        )
        ready = bool(
            row.get("status") == "ready"
            and _same_session(row.get("completed_at"), session_date)
            and final_arm.get("status") == "ready"
            and model_run_id
            and final_arm.get("run_id") == model_run_id
            and _same_session(final_arm.get("completed_at"), session_date)
            and latency.get("panel_cache_hit") is True
            and latency.get("checkpoint_cache_hit") is True
            and latency.get("model_cache_hit") is True
            and opening_prewarm.get("ready") is True
            and opening_prewarm.get("run_id") == model_run_id
            and opening_prewarm.get("source") == "twse_tpex:mis"
        )
        model_results[market] = {
            "ready": ready,
            "status": row.get("status"),
            "completed_at": row.get("completed_at"),
            "final_arm_status": final_arm.get("status"),
            "run_id": model_run_id or None,
            "final_arm_run_id": final_arm.get("run_id"),
            "final_arm_completed_at": final_arm.get("completed_at"),
            "cache_proof": {
                key: latency.get(key)
                for key in (
                    "panel_cache_hit",
                    "checkpoint_cache_hit",
                    "model_cache_hit",
                )
            },
            "opening_source_prewarm": {
                "ready": opening_prewarm.get("ready"),
                "run_id": opening_prewarm.get("run_id"),
                "source": opening_prewarm.get("source"),
                "cache_hit": opening_prewarm.get("cache_hit"),
                "proof": opening_prewarm.get("proof"),
            },
            "tw_mis_fallback_prewarm": final_arm.get(
                "tw_mis_fallback_prewarm"
            ),
        }
        if not ready:
            failures.append(f"{market} model final-arm is not ready")

    components = simulation_receipt.get("components")
    components = dict(components) if isinstance(components, Mapping) else {}
    simulation_ready = bool(
        simulation_receipt.get("status") == "ready"
        and simulation_receipt.get("session_date") == session_date
        and all(
            isinstance(components.get(name), Mapping)
            and components[name].get("status") == "ready"
            and _same_session(components[name].get("checked_at"), session_date)
            for name in ("eligibility", "shioaji_quote")
        )
    )
    if not simulation_ready:
        failures.append("simulation eligibility/Shioaji usage probe is not ready")

    engine_runtime: dict[str, Any] | None = None
    if engine_status_receipt is not None:
        engine_status = dict(engine_status_receipt)
        updated_at = _parse_time(engine_status.get("updated_at"))
        age_seconds = (
            (observed.astimezone(TAIPEI) - updated_at.astimezone(TAIPEI)).total_seconds()
            if updated_at is not None
            else None
        )
        raw_modes = engine_status.get("modes")
        runtime_modes = dict(raw_modes) if isinstance(raw_modes, Mapping) else {}
        expected_markets = set(market_names)
        observed_markets = set(runtime_modes)
        mode_failures: dict[str, dict[str, Any]] = {}
        for market in market_names:
            raw_row = runtime_modes.get(market)
            row = dict(raw_row) if isinstance(raw_row, Mapping) else {}
            if (
                raw_row is None
                or row.get("checkpoint_ready") is not True
                or bool(row.get("readiness_error"))
                or str(row.get("engine_status") or "").startswith(
                    ("blocked", "critical")
                )
            ):
                mode_failures[market] = {
                    "engine_status": row.get("engine_status"),
                    "checkpoint_ready": row.get("checkpoint_ready"),
                    "readiness_error": row.get("readiness_error"),
                }
        ledger_integrity = engine_status.get("ledger_integrity")
        ledger_integrity = (
            dict(ledger_integrity) if isinstance(ledger_integrity, Mapping) else {}
        )
        runtime_ready = bool(
            engine_status.get("simulation_only") is True
            and engine_status.get("production_order_possible") is False
            and ledger_integrity.get("ready") is True
            and expected_markets == observed_markets
            and not mode_failures
            and age_seconds is not None
            and -5.0 <= age_seconds <= 30.0
        )
        engine_runtime = {
            "ready": runtime_ready,
            "health": engine_status.get("health"),
            "updated_at": engine_status.get("updated_at"),
            "heartbeat_age_seconds": age_seconds,
            "simulation_only": engine_status.get("simulation_only"),
            "production_order_possible": engine_status.get(
                "production_order_possible"
            ),
            "ledger_integrity": ledger_integrity,
            "expected_markets": sorted(expected_markets),
            "observed_markets": sorted(observed_markets),
            "mode_failures": mode_failures,
        }
        if not runtime_ready:
            failures.append("paper engine runtime/integrity receipt is not ready")

    runtime_sync: dict[str, Any] | None = None
    if engine_sync_receipt is not None or discord_status_receipt is not None:
        engine_sync = dict(engine_sync_receipt or {})
        discord_status = dict(discord_status_receipt or {})
        engine_updated = _parse_time(engine_sync.get("published_at"))
        discord_updated = _parse_time(discord_status.get("updated_at"))
        engine_age = (
            (observed.astimezone(TAIPEI) - engine_updated.astimezone(TAIPEI)).total_seconds()
            if engine_updated is not None
            else None
        )
        discord_age = (
            (observed.astimezone(TAIPEI) - discord_updated.astimezone(TAIPEI)).total_seconds()
            if discord_updated is not None
            else None
        )
        engine_revision = int(engine_sync.get("state_revision") or 0)
        discord_revision = int(discord_status.get("engine_state_revision") or -1)
        expected_markets = sorted(market_names)
        engine_markets = sorted(
            str(value) for value in (engine_sync.get("enabled_markets") or ())
        )
        discord_markets = sorted(
            str(value) for value in (discord_status.get("day_trade_markets") or ())
        )
        sync_ready = bool(
            engine_sync.get("simulation_only") is True
            and engine_sync.get("production_order_possible") is False
            and engine_sync.get("ledger_integrity_ready") is True
            and discord_status.get("simulation_only") is True
            and discord_status.get("production_order_possible") is False
            and discord_status.get("discord_connected") is True
            and engine_sync.get("engine_run_id")
            == discord_status.get("engine_run_id")
            and engine_revision > 0
            and engine_revision == discord_revision
            and engine_markets == expected_markets
            and discord_markets == expected_markets
            and engine_age is not None
            and -5.0 <= engine_age <= 30.0
            and discord_age is not None
            and -5.0 <= discord_age <= 10.0
        )
        runtime_sync = {
            "ready": sync_ready,
            "engine_run_id": engine_sync.get("engine_run_id"),
            "discord_engine_run_id": discord_status.get("engine_run_id"),
            "engine_revision": engine_revision,
            "discord_revision": discord_revision,
            "revision_lag": max(0, engine_revision - discord_revision),
            "engine_age_seconds": engine_age,
            "discord_age_seconds": discord_age,
            "discord_connected": discord_status.get("discord_connected"),
            "engine_markets": engine_markets,
            "discord_markets": discord_markets,
        }
        if not sync_ready:
            failures.append("paper engine/Discord revision is not synchronized")

    opening_execution: dict[str, Any] | None = None
    opening_check_due = (
        observed.astimezone(TAIPEI).timetz().replace(tzinfo=None)
        >= opening_check_after
    )
    if opening_check_due:
        execution_boundary = datetime.combine(
            observed.astimezone(TAIPEI).date(),
            datetime_time(9, 0),
            tzinfo=TAIPEI,
        )
        engine_sync = dict(engine_sync_receipt or {})
        raw_modes = engine_sync.get("modes")
        sync_modes = dict(raw_modes) if isinstance(raw_modes, Mapping) else {}
        mode_results: dict[str, dict[str, Any]] = {}
        for market in market_names:
            raw_row = sync_modes.get(market)
            row = dict(raw_row) if isinstance(raw_row, Mapping) else {}
            entry_completed = _parse_time(row.get("entry_completed_at"))
            entry_commit_delay_ms = (
                round(
                    (
                        entry_completed.astimezone(TAIPEI) - execution_boundary
                    ).total_seconds()
                    * 1000.0,
                    3,
                )
                if entry_completed is not None
                else None
            )
            slo_met = bool(
                entry_commit_delay_ms is not None
                and 0.0 <= entry_commit_delay_ms <= opening_commit_slo_seconds * 1000.0
            )
            accepted = bool(
                row.get("session_date") == session_date
                and _same_session(row.get("signal_at"), session_date)
                and _same_session(row.get("entry_completed_at"), session_date)
                and row.get("checkpoint_ready") is True
                and row.get("entry_fill_policy") == "causal_best_quote"
                and int(row.get("entry_price_offset_ticks") or 0) == 0
                and slo_met
                and not str(row.get("engine_status") or "").startswith(
                    ("blocked", "critical", "waiting")
                )
            )
            mode_results[market] = {
                "ready": accepted,
                "session_date": row.get("session_date"),
                "signal_id": row.get("signal_id"),
                "signal_at": row.get("signal_at"),
                "entry_completed_at": row.get("entry_completed_at"),
                "engine_status": row.get("engine_status"),
                "checkpoint_ready": row.get("checkpoint_ready"),
                "entry_fill_policy": row.get("entry_fill_policy"),
                "entry_price_offset_ticks": row.get("entry_price_offset_ticks"),
                "entry_commit_delay_ms": entry_commit_delay_ms,
                "commit_slo_seconds": opening_commit_slo_seconds,
                "commit_slo_met": slo_met,
            }
        opening_ready = bool(mode_results) and all(
            row["ready"] for row in mode_results.values()
        )
        opening_execution = {
            "ready": opening_ready,
            "checked_after": opening_check_after.isoformat(),
            "commit_slo_seconds": opening_commit_slo_seconds,
            "modes": mode_results,
        }
        if not opening_ready:
            failures.append(
                "09:00 live signals were not durably committed with causal best-quote execution for every paper mode by 09:00:15"
            )

    required_services = (
        (*REQUIRED_SERVICES, EVENT_MONITOR_SERVICE)
        if event_receipt is not None
        else REQUIRED_SERVICES
    )
    inactive = [
        service for service in required_services if service_states.get(service) != "active"
    ]
    failures.extend(f"required service is not active: {service}" for service in inactive)
    strict = observed.astimezone(TAIPEI).timetz().replace(tzinfo=None) >= strict_after
    ready = not failures
    status = "ready" if ready else "failed" if strict else "warming"
    return {
        "schema_version": 1,
        "status": status,
        "ready": ready,
        "strict": strict,
        "session_date": session_date,
        "observed_at_taipei": observed.astimezone(TAIPEI).isoformat(
            timespec="seconds"
        ),
        "deadline_taipei": (
            f"{session_date}T{opening_check_after.isoformat()}+08:00"
            if opening_check_due
            else f"{session_date}T09:00:00+08:00"
        ),
        "failures": failures,
        "public_data": {
            "ready": public_ready,
            "started_at": public_receipt.get("started_at_taipei"),
            "completed_at": public_receipt.get("completed_at_taipei"),
            "acceptance": acceptance,
        },
        "source_event_monitor": event_monitor,
        "model_final_arm": model_results,
        "simulation_executor": {
            "ready": simulation_ready,
            "status": simulation_receipt.get("status"),
            "updated_at": simulation_receipt.get("updated_at"),
            "components": components,
        },
        "engine_runtime": engine_runtime,
        "runtime_sync": runtime_sync,
        "opening_execution": opening_execution,
        "services": dict(service_states),
    }


def _session_contract(observed: datetime) -> tuple[str, str, tuple[str, ...]]:
    configs = load_market_configs(REPO_ROOT / "services/discord_bot/markets")
    selected = tuple(
        sorted(
            market
            for market, cfg in configs.items()
            if cfg.enabled and cfg.day_trade_simulation_enabled
        )
    )
    if not selected:
        return "unknown", "no enabled TW day-trade simulation markets", ()
    decisions: list[tuple[bool, str]] = []
    for market in selected:
        cfg = configs[market]
        experiment = load_config(_repo_path(Path(cfg.config_path)))
        parquet_root = _repo_path(Path(experiment.data.parquet_root))
        decisions.append(
            verified_tw_stock_session_day(
                observed.astimezone(TAIPEI).date(),
                tuple(cfg.holidays or ()),
                parquet_root=parquet_root,
            )
        )
    if all(opened for opened, _reason in decisions):
        return "open", "; ".join(reason for _opened, reason in decisions), selected
    if not any(opened for opened, _reason in decisions):
        reasons = [reason for _opened, reason in decisions]
        known_closed = all(
            "weekend" in reason
            or "configured market holiday" in reason
            or (
                "official TWSE schedule as-of" in reason
                and "ordinary weekday session" not in reason
            )
            for reason in reasons
        )
        return (
            "closed" if known_closed else "unknown",
            "; ".join(reasons),
            selected,
        )
    return "unknown", "market session decisions disagree: " + repr(decisions), selected


def main() -> int:
    args = parse_args()
    try:
        strict_after = datetime_time.fromisoformat(str(args.strict_after))
    except ValueError as exc:
        raise ValueError("--strict-after must be HH:MM[:SS]") from exc
    observed = datetime.now(TAIPEI)
    session_state, session_reason, markets = _session_contract(observed)
    output = _repo_path(args.output)
    if session_state == "closed":
        payload = {
            "schema_version": 1,
            "status": "skipped",
            "ready": False,
            "session_date": observed.date().isoformat(),
            "observed_at_taipei": observed.isoformat(timespec="seconds"),
            "session_reason": session_reason,
            "markets": list(markets),
        }
        _atomic_json(output, payload)
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)
        return 0

    if session_state != "open":
        payload = {
            "schema_version": 1,
            "status": "failed",
            "ready": False,
            "session_date": observed.date().isoformat(),
            "observed_at_taipei": observed.isoformat(timespec="seconds"),
            "session_reason": session_reason,
            "markets": list(markets),
            "failures": ["TWSE session calendar is not verified"],
        }
        _atomic_json(output, payload)
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)
        return 1

    engine_sync, discord_status = _read_synchronized_runtime_receipts(
        _repo_path(args.engine_sync),
        _repo_path(args.discord_status),
    )
    payload = evaluate_readiness(
        observed=observed,
        strict_after=strict_after,
        market_names=markets,
        public_receipt=_read_json(_repo_path(args.public_receipt)),
        model_receipt=_read_json(_repo_path(args.model_receipt)),
        simulation_receipt=_read_json(_repo_path(args.simulation_receipt)),
        service_states=_service_states(),
        event_receipt=_read_json(_repo_path(args.event_receipt)),
        engine_status_receipt=_read_json(_repo_path(args.engine_status)),
        engine_sync_receipt=engine_sync,
        discord_status_receipt=discord_status,
    )
    payload["session_reason"] = session_reason
    payload["sources"] = {
        "public_receipt": str(_repo_path(args.public_receipt)),
        "model_receipt": str(_repo_path(args.model_receipt)),
        "simulation_receipt": str(_repo_path(args.simulation_receipt)),
        "event_receipt": str(_repo_path(args.event_receipt)),
        "engine_status": str(_repo_path(args.engine_status)),
        "engine_sync": str(_repo_path(args.engine_sync)),
        "discord_status": str(_repo_path(args.discord_status)),
    }
    _atomic_json(output, payload)
    run_path = output.parent / "runs" / (
        observed.strftime("%Y%m%dT%H%M%S%f") + ".json"
    )
    _atomic_json(run_path, payload)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)
    return 0 if payload["ready"] or not payload["strict"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

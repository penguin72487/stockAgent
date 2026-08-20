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
) -> dict[str, Any]:
    session_date = observed.astimezone(TAIPEI).date().isoformat()
    failures: list[str] = []
    acceptance = public_receipt.get("acceptance")
    acceptance = dict(acceptance) if isinstance(acceptance, Mapping) else {}
    public_ready = bool(
        public_receipt.get("status") == "ok"
        and _same_session(public_receipt.get("started_at_taipei"), session_date)
        and acceptance.get("subprocess_ok") is True
        and acceptance.get("snapshot_receipt_fresh") is True
        and acceptance.get("snapshot_status_ok") is True
        and acceptance.get("coverage_complete") is True
        and acceptance.get("same_session_eligibility") is True
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
        event_ready = bool(
            event_receipt.get("status") == "ok"
            and event_receipt.get("coverage_complete") is True
            and int(event_receipt.get("registered_dataset_count") or -1) == expected
            and int(event_receipt.get("monitored_dataset_count") or -1) == expected
            and int(event_receipt.get("observed_dataset_count") or -1) == expected
            and event_receipt.get("failed_probe_count") == 0
            and event_receipt.get("unapplied_event_count") == 0
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
            "updated_at": event_receipt.get("updated_at_taipei"),
            "heartbeat_age_seconds": age_seconds,
        }
        if not event_ready:
            failures.append("156-dataset source-event monitor is not healthy")

    model_rows = model_receipt.get("markets")
    model_rows = dict(model_rows) if isinstance(model_rows, Mapping) else {}
    model_results: dict[str, dict[str, Any]] = {}
    for market in market_names:
        row = model_rows.get(market)
        row = dict(row) if isinstance(row, Mapping) else {}
        final_arm = row.get("final_arm")
        final_arm = dict(final_arm) if isinstance(final_arm, Mapping) else {}
        latency = final_arm.get("live_latency")
        latency = dict(latency) if isinstance(latency, Mapping) else {}
        ready = bool(
            row.get("status") == "ready"
            and _same_session(row.get("completed_at"), session_date)
            and final_arm.get("status") == "ready"
            and _same_session(final_arm.get("completed_at"), session_date)
            and latency.get("panel_cache_hit") is True
            and latency.get("checkpoint_cache_hit") is True
            and latency.get("model_cache_hit") is True
        )
        model_results[market] = {
            "ready": ready,
            "status": row.get("status"),
            "completed_at": row.get("completed_at"),
            "final_arm_status": final_arm.get("status"),
            "final_arm_completed_at": final_arm.get("completed_at"),
            "cache_proof": {
                key: latency.get(key)
                for key in (
                    "panel_cache_hit",
                    "checkpoint_cache_hit",
                    "model_cache_hit",
                )
            },
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
        "deadline_taipei": f"{session_date}T09:00:00+08:00",
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

    payload = evaluate_readiness(
        observed=observed,
        strict_after=strict_after,
        market_names=markets,
        public_receipt=_read_json(_repo_path(args.public_receipt)),
        model_receipt=_read_json(_repo_path(args.model_receipt)),
        simulation_receipt=_read_json(_repo_path(args.simulation_receipt)),
        service_states=_service_states(),
        event_receipt=_read_json(_repo_path(args.event_receipt)),
    )
    payload["session_reason"] = session_reason
    payload["sources"] = {
        "public_receipt": str(_repo_path(args.public_receipt)),
        "model_receipt": str(_repo_path(args.model_receipt)),
        "simulation_receipt": str(_repo_path(args.simulation_receipt)),
        "event_receipt": str(_repo_path(args.event_receipt)),
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

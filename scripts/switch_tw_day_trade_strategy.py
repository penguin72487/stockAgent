#!/usr/bin/env python3
"""Plan or atomically apply a TW day-trade strategy replacement.

The stable paper-market ID is the public identity.  This workflow replaces only
that market's historical signal decisions, keeps every other mode pinned to the
current source ledger, rebuilds all modes into an isolated candidate, requires
receipt-backed 09:01..13:30 minute curves, and promotes by atomic directory
exchange.  It never submits a production order.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta
import fcntl
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time as wall_time
from typing import Any, Mapping, Sequence
import urllib.parse
from zoneinfo import ZoneInfo

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.download_tw_public_data import _validated_taiex_session_dates  # noqa: E402
from scripts.deploy_tw_day_trade_multi_basis_22_history import (  # noqa: E402
    _atomic_json,
    _json_get,
    _object,
    _restart_and_wait_active,
    _run,
    _visible_history_dates,
    _wait_active,
)
from scripts.rebuild_tw_day_trade_open_price_replay import (  # noqa: E402
    _latest_valid_signal,
    _resolve_source_signal_pins,
    _sha256,
    _source_ledger_signal_ids,
)
from scripts.run_tw_day_trade_simulation import _mode_specs  # noqa: E402
from stockagent.config import load_config  # noqa: E402
from stockagent.live.market_config import (  # noqa: E402
    load_market_config,
    resolved_live_output_dir,
)
from stockagent.live.market_status import short_file_fingerprint  # noqa: E402


TAIPEI = ZoneInfo("Asia/Taipei")
DEFAULT_MARKET_CONFIG = Path(
    "services/discord_bot/markets/tw_day_trade_multi_basis_projection_l1_gelu.yaml"
)
DEFAULT_START_DATE = date(2026, 2, 25)
DEFAULT_LIVE_DIR = Path("artifacts/live/tw_day_trade_simulation")
DEFAULT_TW_PUBLIC_DIR = Path("/srv/stockagent-live/data_tw_public")
DEFAULT_OPERATIONS_ROOT = Path("artifacts/operations/tw_day_trade_strategy_switch")
SIMULATION_SERVICE = "stockagent-tw-day-trade-simulation.service"
DISCORD_SERVICE = "stockagent-discord-bot.service"
PUBLIC_SERVICE = "stockagent-public-dashboards.service"
DISCORD_STATUS_PATH = Path("artifacts/discord_bot/service_status.json")


def _resolved(path: Path) -> Path:
    return path if path.is_absolute() else (REPO_ROOT / path).resolve()


def _latest_completed_session(
    *, tw_public_dir: Path, start_date: date
) -> tuple[date, list[date], str]:
    calendar_path = tw_public_dir / "twse_taiex_ohlc.parquet"
    if not calendar_path.is_file():
        raise FileNotFoundError(calendar_path)
    frame = (
        pl.scan_parquet(calendar_path)
        .select(pl.col("date").cast(pl.Date, strict=False).max().alias("date"))
        .collect()
    )
    archive_end = frame.item()
    if archive_end is None:
        raise ValueError(f"official TAIEX calendar is empty: {calendar_path}")
    now = datetime.now(TAIPEI)
    completed_cap = now.date()
    if now.weekday() < 5 and now.timetz().replace(tzinfo=None) < time(13, 40):
        completed_cap -= timedelta(days=1)
    coverage_end = min(archive_end, completed_cap)
    sessions, calendar_sha256 = _validated_taiex_session_dates(
        tw_public_dir, start_date, coverage_end
    )
    completed = sorted(day for day in sessions if day <= completed_cap)
    if not completed:
        raise RuntimeError("no receipt-verified completed TAIEX sessions in range")
    return completed[-1], completed, calendar_sha256


def _active_specs(markets_dir: Path) -> tuple[list[Any], dict[str, Any], list[str]]:
    specs, configs, errors = _mode_specs(markets_dir, include_disabled=False)
    if errors:
        raise RuntimeError(f"mode configuration errors: {errors}")
    if not specs:
        raise RuntimeError("no enabled TW day-trade paper modes")
    markets = [spec.market for spec in specs]
    if len(markets) != len(set(markets)):
        raise RuntimeError("duplicate active paper market IDs")
    return specs, configs, markets


def _market_config_paths(markets_dir: Path) -> dict[str, Path]:
    resolved: dict[str, Path] = {}
    for path in sorted(markets_dir.glob("*.yaml")) + sorted(markets_dir.glob("*.yml")):
        config = load_market_config(path)
        if config.market in resolved:
            raise RuntimeError(f"duplicate market config for {config.market}")
        resolved[config.market] = path.resolve()
    return resolved


def _flat_live_state(live_dir: Path, expected_markets: set[str]) -> dict[str, Any]:
    state_path = live_dir / "state.json"
    state = _object(state_path)
    modes = state.get("modes") or {}
    if set(modes) != expected_markets:
        raise RuntimeError(
            "live mode set differs from enabled config: "
            f"live={sorted(modes)} expected={sorted(expected_markets)}"
        )
    active: list[str] = []
    for market, mode in modes.items():
        positions = mode.get("positions") or {}
        if int(mode.get("open_position_count") or 0) != 0 or any(
            int(row.get("signed_shares") or 0) != 0
            for row in positions.values()
            if isinstance(row, Mapping)
        ):
            active.append(market)
    if active:
        raise RuntimeError(
            "refusing strategy replacement while paper positions are open: "
            f"{sorted(active)}"
        )
    return {
        "path": str(state_path),
        "updated_at": state.get("updated_at"),
        "mode_count": len(modes),
        "flat_markets": sorted(modes),
    }


def _fold_lifecycle_evidence(
    *, market_config: Any, market_config_path: Path, checkpoint: Path
) -> dict[str, Any]:
    if market_config.fold_id is None:
        raise ValueError("strategy replacement requires an explicit fold_id")
    configured_output = _resolved(Path(str(market_config.output_dir or "")))
    expected_fold_dir = configured_output / f"fold_{int(market_config.fold_id):02d}"
    if checkpoint.parent.resolve() != expected_fold_dir.resolve():
        raise ValueError(
            "checkpoint is outside the configured immutable fold: "
            f"checkpoint={checkpoint} expected_fold={expected_fold_dir}"
        )
    complete_path = checkpoint.parent / "fold_complete.json"
    mode_contract_path = checkpoint.parent / "mode_artifact_contract.json"
    weights = _resolved(Path(str(market_config.weights_path or "")))
    for path in (complete_path, mode_contract_path, weights):
        if not path.is_file():
            raise FileNotFoundError(path)
    complete = _object(complete_path)
    mode_contract = _object(mode_contract_path)
    if (
        str(complete.get("status") or "") != "complete"
        or int(complete.get("fold_id") or -1) != int(market_config.fold_id)
        or str(complete.get("checkpoint_path") or "") != checkpoint.name
    ):
        raise ValueError(f"invalid fold completion contract: {complete_path}")
    if str(mode_contract.get("execution_mode") or "") != "tw_day_trade":
        raise ValueError(f"wrong mode artifact contract: {mode_contract_path}")
    selection = Path(str(market_config.model_selection_path or ""))
    if not selection.is_absolute():
        selection = (market_config_path.parent / selection).resolve()
    if not selection.is_file():
        raise FileNotFoundError(selection)
    return {
        "fold_complete_path": str(complete_path),
        "fold_complete_sha256": _sha256(complete_path),
        "fold_status": complete.get("status"),
        "fold_id": complete.get("fold_id"),
        "test_date_start": complete.get("test_date_start"),
        "test_date_end": complete.get("test_date_end"),
        "mode_artifact_contract_path": str(mode_contract_path),
        "mode_artifact_contract_sha256": _sha256(mode_contract_path),
        "execution_mode": mode_contract.get("execution_mode"),
        "decision_clock": mode_contract.get("decision_clock"),
        "execution_clock": mode_contract.get("execution_clock"),
        "weights_path": str(weights),
        "weights_sha256": _sha256(weights),
        "model_selection_path": str(selection),
        "model_selection_sha256": _sha256(selection),
    }


def _current_deployment_evidence(
    *,
    live_dir: Path,
    market: str,
    live_output: Path,
    checkpoint_fingerprint: str,
    initial_capital_twd: float,
    start_date: date,
    end_date: date,
    session_count: int,
    expected_minute_rows: int,
) -> dict[str, Any]:
    reasons: list[str] = []
    try:
        promotion = _object(live_dir / "promotion_receipt.json")
        rebuild = _object(live_dir / "rebuild_receipt.json")
        state = _object(live_dir / "state.json")
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        return {"matches": False, "reasons": [f"missing_receipt:{type(exc).__name__}"]}

    acceptance = promotion.get("acceptance") or {}
    minute = acceptance.get("minute_curve_validation") or {}
    session_dates = [str(value) for value in acceptance.get("session_dates") or ()]
    if (
        len(session_dates) != session_count
        or not session_dates
        or session_dates[0] != start_date.isoformat()
        or session_dates[-1] != end_date.isoformat()
    ):
        reasons.append("promoted_session_range_mismatch")
    if int(minute.get("validated_rows") or 0) != expected_minute_rows:
        reasons.append("promoted_minute_rows_mismatch")
    if int(minute.get("unverified_historical_interior_rows") or 0) != 0:
        reasons.append("unverified_minute_rows_present")
    source_ledger = rebuild.get("source_signal_ledger") or {}
    if market not in set(source_ledger.get("replacement_signal_markets") or ()):
        reasons.append("target_market_was_not_explicitly_replaced")
    mode = (state.get("modes") or {}).get(market) or {}
    if float(mode.get("initial_capital_twd") or 0.0) != initial_capital_twd:
        reasons.append("initial_capital_mismatch")

    summary_count = 0
    observed_fingerprints: set[str] = set()
    for session in rebuild.get("sessions") or ():
        target = next(
            (
                item
                for item in session.get("modes") or ()
                if str(item.get("market") or "") == market
            ),
            None,
        )
        if target is None:
            continue
        summary_path = Path(str(target.get("summary_path") or ""))
        try:
            summary_path = summary_path.resolve(strict=True)
            if not summary_path.is_relative_to(live_output.resolve()):
                reasons.append("target_summary_outside_model_scoped_output")
                break
            summary = _object(summary_path)
        except (FileNotFoundError, json.JSONDecodeError, ValueError, OSError):
            reasons.append("target_summary_unreadable")
            break
        summary_count += 1
        observed_fingerprints.add(str(summary.get("checkpoint_fingerprint") or ""))
    if summary_count != session_count:
        reasons.append("target_signal_session_count_mismatch")
    if observed_fingerprints != {checkpoint_fingerprint}:
        reasons.append("target_checkpoint_fingerprint_mismatch")
    return {
        "matches": not reasons,
        "reasons": sorted(set(reasons)),
        "promotion_receipt": str(live_dir / "promotion_receipt.json"),
        "rebuild_receipt": str(live_dir / "rebuild_receipt.json"),
        "session_count": len(session_dates),
        "minute_rows": int(minute.get("validated_rows") or 0),
        "target_signal_summaries": summary_count,
        "target_checkpoint_fingerprints": sorted(observed_fingerprints),
        "promoted_at": promotion.get("promoted_at"),
        "rollback_directory": promotion.get("rollback_directory"),
    }


def _build_plan(args: argparse.Namespace) -> dict[str, Any]:
    market_config_path = _resolved(args.market_config)
    market_config = load_market_config(market_config_path)
    experiment_path = _resolved(Path(market_config.config_path))
    experiment = load_config(experiment_path)
    if str(experiment.trading.execution_mode) != "tw_day_trade":
        raise ValueError("selected experiment is not execution_mode=tw_day_trade")
    if not market_config.enabled or not market_config.day_trade_simulation_enabled:
        raise ValueError("selected market is not an enabled paper day-trade mode")
    capital = float(
        market_config.current_capital or market_config.initial_capital or 0.0
    )
    if capital != float(args.expected_initial_capital):
        raise ValueError(
            f"initial capital mismatch: resolved={capital} "
            f"expected={args.expected_initial_capital}"
        )

    checkpoint = _resolved(Path(str(market_config.checkpoint_path or "")))
    if not checkpoint.is_file():
        raise FileNotFoundError(checkpoint)
    lifecycle = _fold_lifecycle_evidence(
        market_config=market_config,
        market_config_path=market_config_path,
        checkpoint=checkpoint,
    )
    live_output = _resolved(resolved_live_output_dir(market_config))
    live_dir = _resolved(args.live_dir)
    markets_dir = _resolved(args.markets_dir)
    tw_public_dir = _resolved(args.tw_public_dir)
    start = date.fromisoformat(args.start_date)
    latest, all_sessions, calendar_sha256 = _latest_completed_session(
        tw_public_dir=tw_public_dir, start_date=start
    )
    requested_end = (
        latest if args.end_date == "latest" else date.fromisoformat(args.end_date)
    )
    if requested_end > latest:
        raise ValueError(
            f"requested end {requested_end} exceeds latest completed session {latest}"
        )
    sessions = [day for day in all_sessions if day <= requested_end]
    if not sessions or sessions[0] != start or sessions[-1] != requested_end:
        raise ValueError(
            "start/end must be receipt-verified TAIEX sessions: "
            f"requested={start}..{requested_end}"
        )

    specs, _configs, active_markets = _active_specs(markets_dir)
    if market_config.market not in active_markets:
        raise ValueError(f"replacement market is not active: {market_config.market}")
    flat = _flat_live_state(live_dir, set(active_markets))
    source_ids, source_provenance = _source_ledger_signal_ids(
        live_dir, start_date=start, end_date=requested_end
    )
    expected_keys = {
        (day.isoformat(), market) for day in sessions for market in active_markets
    }
    missing_non_target_keys = sorted(
        key for key in expected_keys - set(source_ids) if key[1] != market_config.market
    )
    repair_markets = {key[1] for key in missing_non_target_keys}
    config_paths = _market_config_paths(markets_dir)
    missing_repair_configs = repair_markets - set(config_paths)
    if missing_repair_configs:
        raise RuntimeError(
            "missing market configs for source-ledger repair: "
            f"{sorted(missing_repair_configs)}"
        )
    _pins, pin_plan = _resolve_source_signal_pins(
        source_ids,
        expected_signal_keys=expected_keys,
        known_markets=set(active_markets),
        allowed_unpinned_markets=repair_markets,
        replacement_signal_markets={market_config.market},
    )
    disk = shutil.disk_usage(live_dir.parent)
    expected_minute_rows = len(sessions) * len(active_markets) * 270
    current_deployment = _current_deployment_evidence(
        live_dir=live_dir,
        market=market_config.market,
        live_output=live_output,
        checkpoint_fingerprint=str(short_file_fingerprint(checkpoint) or ""),
        initial_capital_twd=capital,
        start_date=start,
        end_date=requested_end,
        session_count=len(sessions),
        expected_minute_rows=expected_minute_rows,
    )
    return {
        "schema_version": 1,
        "status": (
            "already_current" if current_deployment["matches"] else "ready_to_apply"
        ),
        "simulation_only": True,
        "production_order_possible": False,
        "replacement": {
            "stable_market_id": market_config.market,
            "label": market_config.label,
            "market_config_path": str(market_config_path),
            "experiment_config_path": str(experiment_path),
            "checkpoint_path": str(checkpoint),
            "checkpoint_fingerprint": short_file_fingerprint(checkpoint),
            "live_signal_output_dir": str(live_output),
            "fold_id": market_config.fold_id,
            "initial_capital_twd": capital,
            "lifecycle": lifecycle,
        },
        "history": {
            "start_date": start.isoformat(),
            "end_date": requested_end.isoformat(),
            "latest_completed_session": latest.isoformat(),
            "session_count": len(sessions),
            "points_per_session_mode": 270,
            "expected_strategy_minute_rows": expected_minute_rows,
            "official_calendar_sha256": calendar_sha256,
        },
        "active_markets": active_markets,
        "live_state": flat,
        "source_ledger": {**source_provenance, **pin_plan},
        "source_ledger_repairs": [
            {
                "market": repair_market,
                "market_config_path": str(config_paths[repair_market]),
                "session_dates": [
                    day
                    for day, market in missing_non_target_keys
                    if market == repair_market
                ],
                "reason": "missing_existing_signal_registration_requires_counterfactual_repair",
            }
            for repair_market in sorted(repair_markets)
        ],
        "current_deployment": current_deployment,
        "disk": {
            "filesystem": str(live_dir.parent),
            "free_bytes": disk.free,
        },
    }


def _verify_generated_signals(
    plan: Mapping[str, Any], sessions: Sequence[date], markets_dir: Path
) -> dict[str, Any]:
    replacement = plan["replacement"]
    market_config = load_market_config(Path(str(replacement["market_config_path"])))
    specs, _configs, _markets = _active_specs(markets_dir)
    spec = next(
        item for item in specs if item.market == replacement["stable_market_id"]
    )
    expected_fingerprint = str(replacement["checkpoint_fingerprint"] or "")
    fingerprints: set[str] = set()
    signal_ids: set[str] = set()
    for day in sessions:
        _generated_at, _summary_path, _weights_path, summary, _rows = (
            _latest_valid_signal(spec, day)
        )
        if not bool(summary.get("counterfactual_signal_regeneration")):
            raise RuntimeError(f"{day}: replacement signal is not counterfactual")
        fingerprint = str(summary.get("checkpoint_fingerprint") or "")
        if fingerprint != expected_fingerprint:
            raise RuntimeError(
                f"{day}: checkpoint fingerprint {fingerprint!r} != "
                f"{expected_fingerprint!r}"
            )
        fingerprints.add(fingerprint)
        signal_ids.add(str(summary.get("signal_id") or ""))
    if len(signal_ids) != len(sessions):
        raise RuntimeError("replacement history does not have one signal per session")
    return {
        "market": market_config.market,
        "sessions": len(sessions),
        "checkpoint_fingerprints": sorted(fingerprints),
        "distinct_signal_ids": len(signal_ids),
    }


def _verify_repaired_signals(
    repairs: Sequence[Mapping[str, Any]], markets_dir: Path
) -> list[dict[str, Any]]:
    specs, _configs, _markets = _active_specs(markets_dir)
    specs_by_market = {spec.market: spec for spec in specs}
    accepted: list[dict[str, Any]] = []
    for repair in repairs:
        market = str(repair["market"])
        spec = specs_by_market[market]
        signal_ids: list[str] = []
        for raw_day in repair["session_dates"]:
            day = date.fromisoformat(str(raw_day))
            _generated_at, _summary_path, _weights_path, summary, _rows = (
                _latest_valid_signal(spec, day)
            )
            if not bool(summary.get("counterfactual_signal_regeneration")):
                raise RuntimeError(
                    f"{day}/{market}: repaired signal is not counterfactual"
                )
            signal_ids.append(str(summary.get("signal_id") or ""))
        accepted.append(
            {
                "market": market,
                "session_dates": list(repair["session_dates"]),
                "distinct_signal_ids": len(set(signal_ids)),
            }
        )
    return accepted


def _wait_engine_sync(
    live_dir: Path, expected_markets: set[str], *, timeout_seconds: float = 90.0
) -> dict[str, Any]:
    deadline = wall_time.monotonic() + timeout_seconds
    while wall_time.monotonic() < deadline:
        try:
            sync = _object(live_dir / "service_sync.json")
        except (FileNotFoundError, json.JSONDecodeError, ValueError):
            wall_time.sleep(1.0)
            continue
        if (
            set(sync.get("enabled_markets") or ()) == expected_markets
            and int(sync.get("mode_count") or 0) == len(expected_markets)
            and bool(sync.get("ledger_integrity_ready"))
        ):
            return sync
        wall_time.sleep(1.0)
    raise RuntimeError("paper engine did not acknowledge the promoted ledger")


def _wait_discord_ready(
    *,
    expected_engine_run_id: str,
    expected_markets: set[str],
    target_market: str,
    checkpoint_fingerprint: str,
    previous_run_id: str,
    timeout_seconds: float = 240.0,
) -> dict[str, Any]:
    status_path = _resolved(DISCORD_STATUS_PATH)
    deadline = wall_time.monotonic() + timeout_seconds
    while wall_time.monotonic() < deadline:
        try:
            status = _object(status_path)
        except (FileNotFoundError, json.JSONDecodeError, ValueError):
            wall_time.sleep(1.0)
            continue
        warmup = status.get("startup_inference_warmup") or {}
        warm_markets = {
            str(row.get("market") or ""): row
            for row in warmup.get("markets") or ()
            if isinstance(row, Mapping)
        }
        target_warm = warm_markets.get(target_market) or {}
        if (
            str(status.get("run_id") or "")
            and str(status.get("run_id") or "") != previous_run_id
            and bool(status.get("discord_connected"))
            and str(status.get("core_health") or "") == "ready"
            and set(status.get("scheduled_day_trade_markets") or ()) == expected_markets
            and str(status.get("engine_run_id") or "") == expected_engine_run_id
            and str(warmup.get("status") or "") == "ready"
            and int(warmup.get("ready_count") or 0) == len(expected_markets)
            and str(target_warm.get("checkpoint_fingerprint") or "")
            == checkpoint_fingerprint
        ):
            return status
        wall_time.sleep(1.0)
    raise RuntimeError("Discord did not acknowledge the promoted ledger and checkpoint")


def _verify_dashboard(
    *,
    market: str,
    start_date: str,
    end_date: str,
    expected_markets: set[str],
) -> dict[str, Any]:
    query = urllib.parse.urlencode(
        {"range": "all", "start_date": start_date, "end_date": end_date}
    )
    results: dict[str, Any] = {}
    for name, url in {
        "local": f"http://127.0.0.1:8766/api/history?{query}",
        "public_gateway": f"http://127.0.0.1:8770/tw-day-trade/api/history?{query}",
    }.items():
        payload = _json_get(url)
        dates = _visible_history_dates(payload, market)
        if not dates or dates[0] != start_date or dates[-1] != end_date:
            raise RuntimeError(
                f"{name} history range mismatch for {market}: "
                f"{dates[:1]}..{dates[-1:] if dates else []}"
            )
        results[name] = {
            "http_status": 200,
            "first_session_date": dates[0],
            "last_session_date": dates[-1],
            "session_count": len(dates),
        }
    signals = _json_get(
        "http://127.0.0.1:8770/tw-day-trade/api/signals?"
        + urllib.parse.urlencode(
            {"mode": market, "date": start_date, "offset": 0, "limit": 1}
        )
    )
    if int(signals.get("total") or 0) <= 0:
        raise RuntimeError("public dashboard exposes no replacement signal detail")
    results["public_signal_detail"] = {
        "http_status": 200,
        "session_date": start_date,
        "total": int(signals.get("total") or 0),
    }
    for name, url in {
        "local_status": "http://127.0.0.1:8766/api/status",
        "public_gateway_status": "http://127.0.0.1:8770/tw-day-trade/api/status",
    }.items():
        payload = _json_get(url)
        service_sync = payload.get("service_sync") or {}
        ledger = payload.get("ledger_integrity") or {}
        if (
            service_sync.get("synchronized") is not True
            or int(service_sync.get("revision_lag") or 0) != 0
            or set(service_sync.get("enabled_markets") or ()) != expected_markets
            or ledger.get("ready") is not True
            or int(ledger.get("divergence_count") or 0) != 0
        ):
            raise RuntimeError(f"{name} runtime synchronization is not accepted")
        results[name] = {
            "http_status": 200,
            "health": payload.get("health"),
            "operational_issues": payload.get("operational_issues") or [],
            "synchronized": True,
            "revision_lag": 0,
            "ledger_integrity_ready": True,
            "ledger_divergence_count": 0,
        }
    return results


def _apply(args: argparse.Namespace, plan: dict[str, Any]) -> dict[str, Any]:
    replacement = plan["replacement"]
    history = plan["history"]
    market = str(replacement["stable_market_id"])
    start = date.fromisoformat(str(history["start_date"]))
    end = date.fromisoformat(str(history["end_date"]))
    _latest, sessions, _sha = _latest_completed_session(
        tw_public_dir=_resolved(args.tw_public_dir), start_date=start
    )
    sessions = [day for day in sessions if day <= end]
    live_dir = _resolved(args.live_dir)
    operations_root = _resolved(args.operations_root) / market
    operations_root.mkdir(parents=True, exist_ok=True)
    status_path = operations_root / "status.json"
    run_id = datetime.now(TAIPEI).strftime("%Y%m%dT%H%M%S%z")
    run_dir = operations_root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    candidate = Path(
        tempfile.mkdtemp(prefix=f"{market}.replacement.", dir=live_dir.parent)
    )
    stages: list[dict[str, Any]] = []
    running = {
        "schema_version": 1,
        "status": "running",
        "started_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
        "plan": plan,
        "run_dir": str(run_dir),
        "candidate_dir": str(candidate),
        "stages": stages,
    }
    _atomic_json(status_path, running)
    python = sys.executable
    active_markets = [str(value) for value in plan["active_markets"]]
    expected_markets = set(active_markets)
    try:
        for index, repair in enumerate(plan["source_ledger_repairs"]):
            repair_dates = [
                date.fromisoformat(str(value)) for value in repair["session_dates"]
            ]
            stages.append(
                _run(
                    f"repair_signal_backfill_{index:02d}",
                    [
                        python,
                        str(
                            REPO_ROOT / "scripts/backfill_tw_day_trade_open_signals.py"
                        ),
                        "--market-config",
                        str(repair["market_config_path"]),
                        "--start-date",
                        min(repair_dates).isoformat(),
                        "--end-date",
                        max(repair_dates).isoformat(),
                    ],
                    run_dir=run_dir,
                )
            )
        repair_acceptance = _verify_repaired_signals(
            plan["source_ledger_repairs"], _resolved(args.markets_dir)
        )
        stages.append(
            {"stage": "verify_repaired_signals", "repairs": repair_acceptance}
        )
        stages.append(
            _run(
                "signal_backfill",
                [
                    python,
                    str(REPO_ROOT / "scripts/backfill_tw_day_trade_open_signals.py"),
                    "--market-config",
                    str(replacement["market_config_path"]),
                    "--start-date",
                    start.isoformat(),
                    "--end-date",
                    end.isoformat(),
                ],
                run_dir=run_dir,
            )
        )
        signal_acceptance = _verify_generated_signals(
            plan, sessions, _resolved(args.markets_dir)
        )
        stages.append({"stage": "verify_generated_signals", **signal_acceptance})

        replay_command = [
            python,
            str(REPO_ROOT / "scripts/rebuild_tw_day_trade_open_price_replay.py"),
            "--markets-dir",
            str(_resolved(args.markets_dir)),
            "--state-dir",
            str(candidate),
            "--start-date",
            start.isoformat(),
            "--end-date",
            end.isoformat(),
            "--benchmark-state-source",
            str(live_dir / "state.json"),
            "--source-ledger-dir",
            str(live_dir),
            "--replace-signal-market",
            market,
            "--reuse-retained-signal-open",
            "--replay-intraday-kbars",
        ]
        for repair in plan["source_ledger_repairs"]:
            replay_command.extend(("--allow-unpinned-market", str(repair["market"])))
        stages.append(_run("replay", replay_command, run_dir=run_dir))
        minute_output = run_dir / "minute_curves"
        stages.append(
            _run(
                "minute_curves",
                [
                    python,
                    str(REPO_ROOT / "scripts/rebuild_tw_day_trade_minute_curves.py"),
                    "--state-dir",
                    str(candidate),
                    "--start-date",
                    start.isoformat(),
                    "--end-date",
                    end.isoformat(),
                    "--output-dir",
                    str(minute_output),
                    "--simulation",
                    "--fetch-missing-kbars",
                    "--repair-unverified-strategy-marks",
                    "--publish",
                ],
                run_dir=run_dir,
            )
        )
        promote_command = [
            python,
            str(REPO_ROOT / "scripts/promote_tw_day_trade_replay.py"),
            "--live-dir",
            str(live_dir),
            "--candidate-dir",
            str(candidate),
        ]
        for active_market in active_markets:
            promote_command.extend(("--expected-market", active_market))
        stages.append(
            _run(
                "validate_promotion",
                [*promote_command, "--validate-only"],
                run_dir=run_dir,
            )
        )

        stopped = False
        try:
            # Recheck after the long isolated build and again while quiesced.
            _flat_live_state(live_dir, expected_markets)
            subprocess.run(["systemctl", "stop", SIMULATION_SERVICE], check=True)
            stopped = True
            _flat_live_state(live_dir, expected_markets)
            stages.append(_run("promote", promote_command, run_dir=run_dir))
        finally:
            if (
                stopped
                or subprocess.run(
                    ["systemctl", "is-active", "--quiet", SIMULATION_SERVICE],
                    check=False,
                ).returncode
                != 0
            ):
                subprocess.run(["systemctl", "start", SIMULATION_SERVICE], check=True)

        _wait_active(SIMULATION_SERVICE)
        sync = _wait_engine_sync(live_dir, expected_markets)
        try:
            previous_discord_run_id = str(
                _object(_resolved(DISCORD_STATUS_PATH)).get("run_id") or ""
            )
        except (FileNotFoundError, json.JSONDecodeError, ValueError):
            previous_discord_run_id = ""
        discord_restart = _restart_and_wait_active(DISCORD_SERVICE)
        discord = _wait_discord_ready(
            expected_engine_run_id=str(sync.get("engine_run_id") or ""),
            expected_markets=expected_markets,
            target_market=market,
            checkpoint_fingerprint=str(replacement["checkpoint_fingerprint"]),
            previous_run_id=previous_discord_run_id,
        )
        public_restart = _restart_and_wait_active(PUBLIC_SERVICE)
        dashboard = _verify_dashboard(
            market=market,
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            expected_markets=expected_markets,
        )
        promotion = _object(live_dir / "promotion_receipt.json")
        acceptance = promotion.get("acceptance") or {}
        minute = acceptance.get("minute_curve_validation") or {}
        expected_rows = int(history["expected_strategy_minute_rows"])
        if int(minute.get("validated_rows") or 0) != expected_rows:
            raise RuntimeError(
                "promoted minute-row count differs from the full active-mode contract"
            )
        runtime_issues = dashboard["local_status"]["operational_issues"]
        complete = {
            **running,
            "status": "complete_with_data_gaps" if runtime_issues else "complete",
            "completed_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
            "stages": stages,
            "signal_acceptance": signal_acceptance,
            "repair_acceptance": repair_acceptance,
            "promotion_receipt": str(live_dir / "promotion_receipt.json"),
            "rollback_directory": promotion.get("rollback_directory"),
            "engine_sync": sync,
            "discord": {
                "run_id": discord.get("run_id"),
                "restart": discord_restart,
                "warmup": discord.get("startup_inference_warmup"),
            },
            "public_restart": public_restart,
            "dashboard_acceptance": dashboard,
        }
        _atomic_json(status_path, complete)
        return complete
    except Exception as exc:
        failed = {
            **running,
            "status": "failed_retryable",
            "failed_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
            "error_type": type(exc).__name__,
            "error": str(exc),
            "stages": stages,
            "candidate_or_rollback_directory": str(candidate),
        }
        _atomic_json(status_path, failed)
        raise


def _verify_current(
    args: argparse.Namespace, plan: Mapping[str, Any]
) -> dict[str, Any]:
    current = plan.get("current_deployment") or {}
    if current.get("matches") is not True:
        raise RuntimeError(
            "current live deployment does not match the requested strategy/history: "
            f"{current.get('reasons') or []}"
        )
    replacement = plan["replacement"]
    history = plan["history"]
    expected_markets = {str(value) for value in plan["active_markets"]}
    live_dir = _resolved(args.live_dir)
    service_states = {
        service: subprocess.run(
            ["systemctl", "is-active", service],
            check=False,
            text=True,
            capture_output=True,
        ).stdout.strip()
        for service in (SIMULATION_SERVICE, DISCORD_SERVICE, PUBLIC_SERVICE)
    }
    inactive = [name for name, state in service_states.items() if state != "active"]
    if inactive:
        raise RuntimeError(f"required services are not active: {inactive}")
    sync = _wait_engine_sync(live_dir, expected_markets)
    discord = _wait_discord_ready(
        expected_engine_run_id=str(sync.get("engine_run_id") or ""),
        expected_markets=expected_markets,
        target_market=str(replacement["stable_market_id"]),
        checkpoint_fingerprint=str(replacement["checkpoint_fingerprint"]),
        previous_run_id="__no_current_run_has_this_id__",
        timeout_seconds=30.0,
    )
    dashboard = _verify_dashboard(
        market=str(replacement["stable_market_id"]),
        start_date=str(history["start_date"]),
        end_date=str(history["end_date"]),
        expected_markets=expected_markets,
    )
    promotion = _object(live_dir / "promotion_receipt.json")
    acceptance = promotion.get("acceptance") or {}
    runtime_issues = dashboard["local_status"]["operational_issues"]
    return {
        "schema_version": 1,
        "status": "verified_with_data_gaps" if runtime_issues else "verified",
        "verified_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
        "simulation_only": True,
        "production_order_possible": False,
        "replacement": replacement,
        "current_deployment": current,
        "services": service_states,
        "engine_sync": sync,
        "discord": {
            "run_id": discord.get("run_id"),
            "warmup": discord.get("startup_inference_warmup"),
        },
        "dashboard_acceptance": dashboard,
        "ending_equity_twd": (acceptance.get("ending_equity_twd") or {}).get(
            replacement["stable_market_id"]
        ),
        "final_open_positions": (acceptance.get("final_open_positions") or {}).get(
            replacement["stable_market_id"]
        ),
        "minute_curve_validation": acceptance.get("minute_curve_validation"),
        "operational_issues": runtime_issues,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "apply", "verify", "status"))
    parser.add_argument("--market-config", type=Path, default=DEFAULT_MARKET_CONFIG)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE.isoformat())
    parser.add_argument("--end-date", default="latest")
    parser.add_argument("--expected-initial-capital", type=float, default=10_000_000.0)
    parser.add_argument(
        "--markets-dir", type=Path, default=Path("services/discord_bot/markets")
    )
    parser.add_argument("--live-dir", type=Path, default=DEFAULT_LIVE_DIR)
    parser.add_argument("--tw-public-dir", type=Path, default=DEFAULT_TW_PUBLIC_DIR)
    parser.add_argument("--operations-root", type=Path, default=DEFAULT_OPERATIONS_ROOT)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.action == "status":
        market = load_market_config(_resolved(args.market_config)).market
        status_path = _resolved(args.operations_root) / market / "status.json"
        print(
            json.dumps(
                _object(status_path), ensure_ascii=False, indent=2, sort_keys=True
            )
        )
        return
    plan = _build_plan(args)
    if args.action == "plan":
        print(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True))
        return
    if args.action == "verify":
        print(
            json.dumps(
                _verify_current(args, plan),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return
    if plan["status"] == "already_current":
        print(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True))
        return
    operations_root = _resolved(args.operations_root) / str(
        plan["replacement"]["stable_market_id"]
    )
    operations_root.mkdir(parents=True, exist_ok=True)
    lock_path = operations_root / "switch.lock"
    with lock_path.open("a+", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("another strategy switch is active") from exc
        result = _apply(args, plan)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

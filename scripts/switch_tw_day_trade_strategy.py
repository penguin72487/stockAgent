#!/usr/bin/env python3
"""Plan or atomically apply a TW day-trade strategy replacement or addition.

The stable paper-market ID is the public identity.  This workflow replaces only
that market's historical signal decisions, keeps every other mode pinned to the
current source ledger, rebuilds all modes into an isolated candidate, requires
receipt-backed 09:01..13:30 minute curves, and promotes by atomic directory
exchange. An explicit --add-mode instead retains current account states and
decision/execution bytes, validates one independent replay and appends it to an
isolated union before the same promotion. It never submits a production order.
An explicit --history-extension accepts an already rebuilt candidate, retains
every existing signal decision, and permits only an earlier missing prefix.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from dataclasses import replace
from datetime import date, datetime, time, timedelta
import fcntl
import json
import os
from pathlib import Path
import shutil
import shlex
import subprocess
import sys
import tempfile
import time as wall_time
from typing import Any, Mapping, Sequence
import urllib.parse
from zoneinfo import ZoneInfo

import polars as pl
import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_text  # noqa: E402

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
    _source_ledger_signal_artifacts,
    _source_ledger_signal_ids,
)
from scripts.run_tw_day_trade_simulation import _mode_specs  # noqa: E402
from scripts.reload_tw_day_trade_services_if_flat import fresh as _require_fresh_receipt  # noqa: E402
from stockagent.config import load_config  # noqa: E402
from stockagent.data.tw_price_rules import TW_ORDER_PRICE_CONTRACT_VERSION  # noqa: E402
from stockagent.live.market_config import (  # noqa: E402
    load_market_config,
    resolved_live_output_dir,
)
from stockagent.live.market_status import short_file_fingerprint  # noqa: E402
from stockagent.live.feature_source_contract import (  # noqa: E402
    inspect_live_feature_source,
    require_live_feature_source,
)
from stockagent.live.tw_day_trade_simulation import (  # noqa: E402
    REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL,
    REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
)


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


def _require_requested_artifact(
    market_config: Any, expected_artifact_root: Path | None
) -> dict[str, Any] | None:
    """Bind an explicit user request before trusting a stable market alias.

    The selector can override the YAML's visible checkpoint/output fields.
    Similar model labels or architecture names are not artifact identities.
    Existing lifecycle and checkpoint checks still own artifact acceptance.
    """
    if expected_artifact_root is None:
        return None
    requested = _resolved(expected_artifact_root).resolve()
    if not requested.is_dir():
        raise FileNotFoundError(f"requested training artifact is unavailable: {requested}")
    selected = _resolved(Path(str(market_config.output_dir or ""))).resolve()
    if selected != requested:
        raise ValueError(
            "requested training artifact differs from resolved model selection: "
            f"requested={requested} selected={selected}"
        )
    if market_config.fold_id is None:
        raise ValueError("requested training artifact requires an explicit fold_id")
    expected_checkpoint = requested / f"fold_{int(market_config.fold_id):02d}" / "checkpoint_best.pt"
    selected_checkpoint = _resolved(Path(str(market_config.checkpoint_path or ""))).resolve()
    if selected_checkpoint != expected_checkpoint.resolve():
        raise ValueError(
            "selected checkpoint does not belong to the requested training artifact: "
            f"selected={selected_checkpoint} expected={expected_checkpoint}"
        )
    if not expected_checkpoint.is_file():
        raise FileNotFoundError(expected_checkpoint)
    return {
        "requested_artifact_root": str(requested),
        "selected_artifact_root": str(selected),
        "checkpoint_sha256": _sha256(expected_checkpoint),
        "identity_matches": True,
        "training_execution_parity_proven": False,
    }


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
    required_replay_entry_contract: str = REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
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
    from stockagent.live.day_trade_addition import CONTRACT as ADDITION_CONTRACT, evidence_path
    is_addition = rebuild.get("contract") == ADDITION_CONTRACT and rebuild.get("added_market") == market
    if is_addition:
        component = evidence_path(live_dir, rebuild["new_mode_component"])
        component_rebuild = _object(component / "rebuild_receipt.json")
    else:
        component_rebuild = rebuild
    source_ledger = component_rebuild.get("source_signal_ledger") or {}
    if not is_addition and market not in set(source_ledger.get("replacement_signal_markets") or ()):
        reasons.append("target_market_was_not_explicitly_replaced")
    replay_entry_contract = str(
        (component_rebuild.get("replay_contract") or {}).get("entry") or ""
    )
    if replay_entry_contract != required_replay_entry_contract:
        reasons.append("replay_entry_contract_version_mismatch")
    price_contract = (component_rebuild.get("replay_contract") or {}).get("order_price_contract_version")
    if type(price_contract) is not int or price_contract != TW_ORDER_PRICE_CONTRACT_VERSION:
        reasons.append("order_price_contract_version_mismatch")
    mode = (state.get("modes") or {}).get(market) or {}
    if float(mode.get("initial_capital_twd") or 0.0) != initial_capital_twd:
        reasons.append("initial_capital_mismatch")

    summary_count = 0
    observed_fingerprints: set[str] = set()
    for session in component_rebuild.get("sessions") or ():
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
        "replay_entry_contract": replay_entry_contract,
        "required_replay_entry_contract": required_replay_entry_contract,
        "order_price_contract_version": price_contract,
        "required_order_price_contract_version": TW_ORDER_PRICE_CONTRACT_VERSION,
        "promoted_at": promotion.get("promoted_at"),
        "rollback_directory": promotion.get("rollback_directory"),
    }


def _candidate_preflight(
    args: argparse.Namespace, *, market_config: Any,
    market_config_path: Path, experiment_path: Path, experiment: Any,
    requested_artifact: Any, lifecycle: Mapping[str, Any], capital: float,
) -> dict[str, Any]:
    """Inspect an unenabled, additive candidate, never an applicable switch.

    Adding an account needs a different cutover mode-set contract from replacing
    one. This receipt does not imply that replacement apply can promote it.
    """
    markets_dir = _resolved(args.markets_dir)
    _specs, _configs, active_markets = _active_specs(markets_dir)
    if market_config.enabled or market_config.market in active_markets:
        raise ValueError("candidate preflight requires a disabled independent market")
    live_dir = _resolved(args.live_dir)
    features = inspect_live_feature_source(experiment.data)
    blockers = list(features["blockers"])
    blockers.append({
        "code": "additive_cutover_not_verified",
        "detail": "This is a disabled candidate preflight, not replacement apply; additive account promotion and retained-history parity are not yet verified.",
    })
    dependencies = []
    for field, kind in (
        ("parquet_root", "directory"),
        ("factorized_feature_manifest", "file"),
        ("tw_public_feature_path", "file"),
        ("day_trade_physical_public_feature_path", "file"),
        ("day_trade_minute_execution_root", "directory"),
    ):
        value = str(getattr(experiment.data, field, "") or "")
        if not value:
            continue
        path = _resolved(Path(value))
        exists = path.is_file() if kind == "file" else path.is_dir()
        dependencies.append({"field": field, "path": str(path), "kind": kind, "exists": exists})
        if not exists:
            blockers.append({"code": "source_dependency_missing", "detail": f"{field}: {path}"})
    try:
        flat = {**_flat_live_state(live_dir, set(active_markets)), "ready": True}
    except (RuntimeError, ValueError, OSError) as exc:
        flat = {"path": str(live_dir / "state.json"), "ready": False, "error": str(exc)}
        blockers.append({"code": "live_flat_gate_not_ready", "detail": str(exc)})
    history: dict[str, Any] = {"requested_start_date": args.start_date, "requested_end_date": args.end_date}
    try:
        start = date.fromisoformat(args.start_date)
        latest, all_sessions, calendar_sha256 = _latest_completed_session(
            tw_public_dir=_resolved(args.tw_public_dir), start_date=start,
        )
        end = latest if args.end_date == "latest" else date.fromisoformat(args.end_date)
        sessions = [day for day in all_sessions if day <= end]
        if end > latest or not sessions or sessions[0] != start or sessions[-1] != end:
            raise ValueError("candidate start/end must be receipt-verified completed TAIEX sessions")
        history.update({
            "start_date": start.isoformat(), "end_date": end.isoformat(),
            "latest_completed_session": latest.isoformat(), "session_count": len(sessions),
            "official_calendar_sha256": calendar_sha256, "points_per_session_mode": 270,
            "expected_strategy_minute_rows_after_addition": len(sessions) * (len(active_markets) + 1) * 270,
        })
    except (RuntimeError, ValueError, OSError) as exc:
        blockers.append({"code": "official_completed_calendar_not_ready", "detail": str(exc)})
    checkpoint = _resolved(Path(str(market_config.checkpoint_path)))
    return {
        "schema_version": 1, "status": "blocked_preflight", "plan_complete": False,
        "read_only": True, "simulation_only": True, "production_order_possible": False,
        "intent": "add_independent_mode", "mutations": [],
        "candidate": {
            "stable_market_id": market_config.market, "label": market_config.label,
            "enabled": False, "market_config_path": str(market_config_path),
            "experiment_config_path": str(experiment_path),
            "experiment_config_sha256": _sha256(experiment_path),
            "checkpoint_path": str(checkpoint),
            "checkpoint_fingerprint": short_file_fingerprint(checkpoint),
            "requested_artifact": requested_artifact, "fold_id": market_config.fold_id,
            "initial_capital_twd": capital, "lifecycle": dict(lifecycle),
            "live_signal_output_dir": str(_resolved(resolved_live_output_dir(market_config))),
        },
        "active_markets": active_markets,
        "proposed_markets": sorted([*active_markets, market_config.market]),
        "existing_market_config_sha256": {
            market: _sha256(path) for market, path in _market_config_paths(markets_dir).items()
            if market in active_markets
        },
        "preserve_existing_signal_pins": True, "existing_history_parity_proven": False,
        "live_state": flat, "history": history,
        "live_features": features, "source_dependencies": dependencies, "blockers": blockers,
    }


def _build_plan(args: argparse.Namespace) -> dict[str, Any]:
    market_config_path = _resolved(args.market_config)
    market_config = load_market_config(market_config_path)
    requested_artifact = _require_requested_artifact(
        market_config, getattr(args, "expected_artifact_root", None)
    )
    experiment_path = _resolved(Path(market_config.config_path))
    experiment = load_config(experiment_path)
    if str(experiment.trading.execution_mode) != "tw_day_trade":
        raise ValueError("selected experiment is not execution_mode=tw_day_trade")
    candidate_preflight = bool(getattr(args, "candidate_preflight", False))
    if candidate_preflight and args.action != "plan":
        raise ValueError("candidate preflight is read-only and supports plan only")
    add_mode = bool(getattr(args, "add_mode", False))
    if add_mode and candidate_preflight:
        raise ValueError("independent apply and read-only candidate inspection are distinct operations")
    if not market_config.day_trade_simulation_enabled or (not candidate_preflight and not add_mode and not market_config.enabled):
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
    if getattr(args, "history_extension", False):
        if candidate_preflight or add_mode:
            raise ValueError("history extension cannot add or replace a strategy")
        require_live_feature_source(experiment.data)
        return _build_history_extension_plan(
            args, market_config=market_config, market_config_path=market_config_path,
            experiment_path=experiment_path, requested_artifact=requested_artifact,
            lifecycle=lifecycle, capital=capital,
        )
    if candidate_preflight:
        return _candidate_preflight(
            args, market_config=market_config, market_config_path=market_config_path,
            experiment_path=experiment_path, experiment=experiment,
            requested_artifact=requested_artifact, lifecycle=lifecycle, capital=capital,
        )
    if add_mode:
        return _build_addition_plan(args, market_config, market_config_path, experiment_path,
                                    experiment, requested_artifact, lifecycle, capital)
    # Do not make a training-only wide checkpoint applicable merely by changing
    # its label/enablement or by constructing the six-column executable base.
    require_live_feature_source(experiment.data)
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
        live_dir, start_date=start, end_date=requested_end,
        accepted_repair_manifests=tuple(getattr(args, "accepted_repair_manifest", ())),
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
            "requested_artifact": requested_artifact,
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


def _history_source_streams(live: Path) -> dict[str, Any]:
    """Bind decisions and execution streams, not changing status heartbeats."""
    return {
        name: {"bytes": (live / name).stat().st_size, "sha256": _sha256(live / name)}
        for name in ("events.jsonl", "signals.jsonl", "orders.jsonl", "fills.jsonl",
                     "execution_latency.jsonl") if (live / name).is_file()
    }


def _require_history_source_unchanged(live: Path, proof: Mapping[str, Any]) -> None:
    if _history_source_streams(live) != proof:
        raise RuntimeError("live decisions or execution changed after history-extension planning")


def _prepared_history_evidence(candidate: Path, live: Path, *, sessions: Sequence[date],
                               configs: Mapping[str, Any], manifests: Sequence[Path]) -> dict[str, Any]:
    """An earlier counterfactual prefix may not replace any retained decision."""
    candidate, live = candidate.resolve(strict=True), live.resolve(strict=True)
    if (candidate == live or candidate in live.parents or live in candidate.parents
            or candidate.stat().st_dev != live.stat().st_dev):
        raise ValueError("prepared history requires a separate same-filesystem candidate")
    receipt = _object(candidate / "rebuild_receipt.json")
    if receipt.get("simulation_only") is not True or receipt.get("production_order_possible") is not False:
        raise ValueError("prepared history must be simulation-only with production orders prohibited")
    source = receipt.get("source_signal_ledger") or {}
    if source.get("replacement_signal_markets") or source.get("replaced_pinned_signal_keys"):
        raise ValueError("history extension cannot replace existing signal decisions")
    if (Path(str(source.get("path") or "")).resolve() != live / "events.jsonl"
            or source.get("sha256") != _sha256(live / "events.jsonl")):
        raise ValueError("prepared history does not bind the current live decision journal")
    original, _ = _source_ledger_signal_ids(
        live, start_date=sessions[0], end_date=sessions[-1],
        accepted_repair_manifests=tuple(manifests),
    )
    retained_artifacts = _source_ledger_signal_artifacts(
        live, original, accepted_repair_manifests=tuple(manifests),
    )
    expected = {(day.isoformat(), market) for day in sessions for market in configs}
    if not original or not set(original) <= expected:
        raise ValueError("prepared history has no compatible retained decision scope")
    missing = expected - set(original)
    first_retained = min(day for day, _ in original)
    if not missing or any(day >= first_retained for day, _ in missing):
        raise ValueError("history extension permits only a missing earlier prefix")
    prefix_root = Path(str(receipt.get("missing_signal_root") or "")).resolve(strict=True)
    candidate_modes, live_modes = _object(candidate / "state.json").get("modes") or {}, _object(live / "state.json").get("modes") or {}
    if set(candidate_modes) != set(configs) or set(live_modes) != set(configs):
        raise ValueError("history extension cannot add or remove accounts")
    for market, config in configs.items():
        capital = float(config.current_capital or config.initial_capital or 0)
        if capital <= 0 or any(float(modes[market].get("initial_capital_twd") or 0) != capital
                               for modes in (candidate_modes, live_modes)):
            raise ValueError(f"history extension changed initial capital: {market}")
    seen: dict[tuple[str, str], str] = {}
    artifact_proofs: list[dict[str, Any]] = []
    for session in receipt.get("sessions") or ():
        day = str(session.get("session_date") or "")
        for row in session.get("modes") or ():
            market, signal_id = str(row.get("market") or ""), str(row.get("signal_id") or "")
            key = (day, market)
            if key in seen or key not in expected or not signal_id:
                raise ValueError(f"prepared history has an invalid or duplicate signal: {key}")
            if key in original and signal_id != original[key]:
                raise ValueError(f"prepared history changed a retained signal: {key}")
            paths = {kind: Path(str(row.get(f"{kind}_path") or "")).resolve(strict=True)
                     for kind in ("summary", "weights")}
            for kind, path in paths.items():
                if _sha256(path) != row.get(f"{kind}_sha256"):
                    raise ValueError(f"prepared history signal artifact changed: {key}/{kind}")
                if key in retained_artifacts and row.get(f"{kind}_sha256") != retained_artifacts[key][f"{kind}_sha256"]:
                    raise ValueError(f"prepared history changed retained artifact bytes: {key}/{kind}")
            summary = _object(paths["summary"])
            if summary.get("signal_id") != signal_id:
                raise ValueError(f"prepared history summary identity differs: {key}")
            if key in missing:
                config = configs[market]
                if (not all(path.is_relative_to(prefix_root / market) for path in paths.values())
                        or summary.get("counterfactual_signal_regeneration") is not True
                        or summary.get("simulation_only") is not True
                        or summary.get("production_order_possible") is not False
                        or summary.get("checkpoint_fingerprint") != short_file_fingerprint(
                            _resolved(Path(config.checkpoint_path)))
                        or str(summary.get("replay_effective_signal_at") or "")[:10] != day):
                    raise ValueError(f"missing-prefix signal is not the selected marked counterfactual: {key}")
            seen[key] = signal_id
            artifact_proofs.append({"session_date": day, "market": market, "signal_id": signal_id,
                                    **{f"{kind}_{field}": str(paths[kind]) if field == "path"
                                       else row[f"{kind}_sha256"]
                                       for kind in paths for field in ("path", "sha256")}})
    if set(seen) != expected:
        raise ValueError("prepared history does not cover every official session and configured mode")
    return {"candidate_dir": str(candidate), "rebuild_receipt_sha256": _sha256(candidate / "rebuild_receipt.json"),
            "retained_decision_count": len(original), "retained_artifact_pin_count": len(retained_artifacts),
            "counterfactual_prefix_count": len(missing),
            "prefix_last_session": max(day for day, _ in missing),
            "source_streams": _history_source_streams(live), "signal_artifacts": artifact_proofs}


def _build_history_extension_plan(args, *, market_config, market_config_path,
                                  experiment_path, requested_artifact, lifecycle, capital):
    live = _resolved(args.live_dir)
    specs, configs, markets = _active_specs(_resolved(args.markets_dir))
    expected = set(markets)
    if market_config.market not in expected:
        raise ValueError("history verification target is not a configured mode")
    flat = _flat_live_state(live, expected)
    start = date.fromisoformat(args.start_date)
    latest, calendar, digest = _latest_completed_session(tw_public_dir=_resolved(args.tw_public_dir), start_date=start)
    end = latest if args.end_date == "latest" else date.fromisoformat(args.end_date)
    sessions = [day for day in calendar if day <= end]
    if not sessions or sessions[0] != start or sessions[-1] != end or end > latest:
        raise ValueError("history extension requires the exact receipt-verified completed session range")
    target = {"stable_market_id": market_config.market, "label": market_config.label,
              "market_config_path": str(market_config_path), "experiment_config_path": str(experiment_path),
              "checkpoint_path": str(_resolved(Path(market_config.checkpoint_path))),
              "checkpoint_fingerprint": short_file_fingerprint(_resolved(Path(market_config.checkpoint_path))),
              "live_signal_output_dir": str(_resolved(resolved_live_output_dir(market_config))),
              "fold_id": market_config.fold_id, "initial_capital_twd": capital,
              "lifecycle": lifecycle, "requested_artifact": requested_artifact}
    history = {"start_date": str(start), "end_date": str(end), "latest_completed_session": str(latest),
               "session_count": len(sessions), "points_per_session_mode": 270,
               "expected_strategy_minute_rows": len(sessions) * len(expected) * 270,
               "official_calendar_sha256": digest}
    status_path = _resolved(args.operations_root) / market_config.market / "status.json"
    if args.action == "verify" or (live / "promotion_receipt.json").is_file() and status_path.is_file():
        previous = _object(status_path) if status_path.is_file() else {}
        prior = previous.get("plan") or {}
        promotion = _object(live / "promotion_receipt.json") if (live / "promotion_receipt.json").is_file() else {}
        prepared = prior.get("prepared_history") or {}
        matches = (prior.get("operation") == "extend_history" and prior.get("replacement") == target
                   and prior.get("history") == history and set(prior.get("active_markets") or ()) == expected
                   and (promotion.get("acceptance") or {}).get("rebuild_receipt_sha256") == prepared.get("rebuild_receipt_sha256")
                   and prepared.get("rebuild_receipt_sha256") == _sha256(live / "rebuild_receipt.json"))
        if matches:
            _require_history_source_unchanged(Path(str(promotion["rollback_directory"])), prepared["source_streams"])
            for proof in prepared["signal_artifacts"]:
                for kind in ("summary", "weights"):
                    if _sha256(Path(proof[f"{kind}_path"])) != proof[f"{kind}_sha256"]:
                        raise RuntimeError("promoted history signal artifacts changed")
            return {**prior, "status": "already_current", "current_deployment": {
                "matches": True, "reasons": [], "promotion_receipt": str(live / "promotion_receipt.json"),
                "rollback_directory": promotion.get("rollback_directory")}}
        if args.action == "verify":
            raise RuntimeError("current history is not the exact accepted history-extension operation")
    if args.prepared_candidate is None:
        raise ValueError("history extension plan/apply requires --prepared-candidate")
    proof = _prepared_history_evidence(_resolved(args.prepared_candidate), live, sessions=sessions,
                                      configs={spec.market: configs[spec.market] for spec in specs},
                                      manifests=tuple(args.accepted_repair_manifest))
    return {"schema_version": 1, "operation": "extend_history", "plan_complete": True,
            "status": "ready_to_apply", "simulation_only": True, "production_order_possible": False,
            "replacement": target, "history": history, "active_markets": markets,
            "live_state": flat, "prepared_history": proof, "source_ledger_repairs": [],
            "current_deployment": {"matches": False, "reasons": ["history_extension_not_promoted"]},
            "disk": {"filesystem": str(live.parent), "free_bytes": shutil.disk_usage(live.parent).free}}


def _build_addition_plan(args, config, config_path, experiment_path, experiment,
                         requested_artifact, lifecycle, capital):
    require_live_feature_source(experiment.data)
    start = date.fromisoformat(args.start_date)
    latest, calendar, digest = _latest_completed_session(
        tw_public_dir=_resolved(args.tw_public_dir), start_date=start)
    end = latest if args.end_date == "latest" else date.fromisoformat(args.end_date)
    sessions = [d for d in calendar if d <= end]
    if not sessions or sessions[0] != start or sessions[-1] != end or end > latest:
        raise ValueError("addition requires the exact receipt-verified completed session range")
    _, _, active = _active_specs(_resolved(args.markets_dir))
    expected = set(active) | {config.market}
    live = _resolved(args.live_dir)
    current = _current_deployment_evidence(
        live_dir=live, market=config.market, live_output=_resolved(resolved_live_output_dir(config)),
        checkpoint_fingerprint=str(short_file_fingerprint(_resolved(Path(config.checkpoint_path)))),
        initial_capital_twd=capital, start_date=start, end_date=end, session_count=len(sessions),
        expected_minute_rows=len(sessions)*len(expected)*270,
        required_replay_entry_contract=(REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL
            if getattr(args, "historical_full_fill_0901", False)
            else REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE))
    if config.market in active and not current["matches"]:
        raise ValueError("addition cannot replace an already active market")
    resumed = getattr(args, "resume_addition_candidate", None)
    if resumed and config.market in (_object(live / "state.json").get("modes") or {}) and config.market not in active:
        from stockagent.live.day_trade_addition import inspect_aborted_activation
        abort = inspect_aborted_activation(live, _resolved(resumed))
        if abort["added_market"] != config.market or set(abort["retained_markets"]) != set(active):
            raise ValueError("resumed additive candidate has a different market scope")
        flat = {"path": str(live / "state.json"), "flat_markets": sorted(active),
                "aborted_unfunded_activation": config.market,
                "recovery_requires_preserved_activation_evidence": True}
    else:
        flat = _flat_live_state(live, set(active))
    return {"schema_version": 1, "operation": "add_independent_mode", "plan_complete": True,
            "status": "already_current" if current["matches"] else "ready_to_apply",
            "simulation_only": True, "production_order_possible": False,
            "replacement": {"stable_market_id": config.market, "label": config.label,
                "market_config_path": str(config_path), "experiment_config_path": str(experiment_path),
                "checkpoint_path": str(_resolved(Path(config.checkpoint_path))),
                "checkpoint_fingerprint": short_file_fingerprint(_resolved(Path(config.checkpoint_path))),
                "live_signal_output_dir": str(_resolved(resolved_live_output_dir(config))),
                "fold_id": config.fold_id, "initial_capital_twd": capital,
                "lifecycle": lifecycle, "requested_artifact": requested_artifact},
            "active_markets": sorted(expected), "retained_markets": sorted(set(active)-{config.market}),
            "live_state": flat, "current_deployment": current,
            "history": {"start_date": str(start), "end_date": str(end),
                "latest_completed_session": str(latest), "session_count": len(sessions),
                "points_per_session_mode": 270, "expected_strategy_minute_rows": len(sessions)*len(expected)*270,
                "official_calendar_sha256": digest,
                **({"replay_entry_contract": REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL,
                    "historical_entry_time": "09:01:00 Asia/Taipei",
                    "liquidity_assumption": "full_legal_NAV_funded_target_no_liquidity_claim"}
                   if getattr(args, "historical_full_fill_0901", False) else {})},
            "source_ledger": {"policy": "retain_existing_decision_and_execution_bytes_and_account_states"},
            "source_ledger_repairs": []}


def _verify_generated_signals(
    plan: Mapping[str, Any], sessions: Sequence[date], markets_dir: Path
) -> dict[str, Any]:
    replacement = plan["replacement"]
    market_config = load_market_config(Path(str(replacement["market_config_path"])))
    if plan.get("operation") == "add_independent_mode":
        specs, _configs, errors = _mode_specs(markets_dir, include_disabled=True)
        if errors:
            raise RuntimeError(f"candidate mode configuration errors: {errors}")
    else:
        specs, _configs, _markets = _active_specs(markets_dir)
    spec = next(
        item for item in specs if item.market == replacement["stable_market_id"]
    )
    if plan.get("verification_signal_dir"):
        spec = replace(spec, live_output_dir=Path(plan["verification_signal_dir"]))
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
            _require_fresh_receipt(sync, "heartbeat_at" if "heartbeat_at" in sync else "published_at", datetime.now(TAIPEI))
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
            _require_fresh_receipt(status, "updated_at", datetime.now(TAIPEI))
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


def _promote_with_cutover(command, *, before_exchange, stage, run_dir):
    """Use the canonical promoter's late-drain hook, retaining ordinary logs."""
    from scripts import promote_tw_day_trade_replay as promoter
    stdout_path, stderr_path = run_dir / f"{stage}.stdout.log", run_dir / f"{stage}.stderr.log"
    started = datetime.now(TAIPEI)
    with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open("w", encoding="utf-8") as stderr:
        with redirect_stdout(stdout), redirect_stderr(stderr):
            try:
                promoter.main(argv=list(command[2:]), before_exchange=before_exchange)
            except Exception as exc:
                print(f"{type(exc).__name__}: {exc}", file=stderr)
                raise
    return {"stage": stage, "command": list(command), "returncode": 0,
            "started_at": started.isoformat(), "completed_at": datetime.now(TAIPEI).isoformat(),
            "stdout_path": str(stdout_path), "stderr_path": str(stderr_path),
            "writer_drain": "canonical_after_full_source_validation"}


def _ensure_discord_deployment_schedule(market: str, *, run_dir: Path, env_path: Path | None = None):
    """Extend only the selected allow-list; keep other schedules and secrets exact."""
    path = env_path or _resolved(Path(os.environ.get("STOCKAGENT_DISCORD_ENV_FILE") or "services/discord_bot/.env"))
    before = path.read_bytes()
    lines = before.decode().splitlines(keepends=True)
    key = "STOCKAGENT_SCHEDULED_MARKETS"
    indices = [i for i, line in enumerate(lines) if line.strip().removeprefix("export ").startswith(key+"=")]
    if len(indices) > 1:
        raise ValueError("ambiguous duplicate Discord schedule key")
    previous = None
    if indices:
        i = indices[0]
        previous = lines[i]
        value = shlex.split(lines[i].strip().removeprefix("export ").split("=", 1)[1], comments=True)
        if len(value) != 1:
            raise ValueError("Discord schedule is not a literal market allow-list")
        markets = [item.strip() for item in value[0].split(",") if item.strip()]
        if not any(item.casefold() in {"all", "*"} for item in markets) and market not in markets:
            markets.append(market)
            lines[i] = key+"="+",".join(markets)+"\n"
    deadline_key = "STOCKAGENT_DEPLOYMENT_WARMUP_UNTIL"
    deadline = (datetime.now(TAIPEI)+timedelta(minutes=10)).isoformat(timespec="seconds")
    deadline_indices = [i for i, line in enumerate(lines) if line.strip().removeprefix("export ").startswith(deadline_key+"=")]
    if len(deadline_indices) > 1:
        raise ValueError("ambiguous duplicate deployment warmup deadline")
    deadline_line = deadline_key+"="+deadline+"\n"
    if deadline_indices:
        lines[deadline_indices[0]] = deadline_line
    else:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] += "\n"
        lines.append(deadline_line)
    if path.read_bytes() != before:
        raise RuntimeError("Discord environment changed while planning the scoped update")
    atomic_write_text(path, "".join(lines), durable=True, mode=0o600)
    receipt = {"contract": "additive_schedule_and_bounded_no_publication_warmup_v1",
               "market": market, "env_path": str(path), "previous_schedule_line": previous,
               "warmup_deadline": deadline, "other_environment_values_unchanged": True,
               "simulation_only": True, "production_order_possible": False}
    _atomic_json(run_dir / "discord_schedule_update.json", receipt)
    return receipt


def _addition_replay_flags(args, *, market: str, live: Path) -> list[str]:
    """An explicit retrospective scenario, never a live execution setting."""
    if not getattr(args, "historical_full_fill_0901", False):
        return []
    return ["--historical-full-fill-0901", "--source-ledger-dir", str(live),
            "--allow-unpinned-market", market]


def _apply_addition(args: argparse.Namespace, plan: dict[str, Any]) -> dict[str, Any]:
    """Accept an independent replay, retain old accounts, then use the promoter."""
    from stockagent.live.day_trade_addition import merge_addition, preserve_aborted_activation, validate_aborted_activation
    from stockagent.live.day_trade_maintenance import control_lease
    target, history = plan["replacement"], plan["history"]
    market = target["stable_market_id"]
    live, expected = _resolved(args.live_dir), set(plan["active_markets"])
    retained = set(plan["retained_markets"])
    root = _resolved(args.operations_root) / market
    run = root / "runs" / datetime.now(TAIPEI).strftime("%Y%m%dT%H%M%S%z")
    run.mkdir(parents=True, exist_ok=False)
    resumed = getattr(args, "resume_addition_candidate", None)
    reuse = bool(resumed) and not getattr(args, "rebuild_resumed_addition", False)
    candidate = (_resolved(resumed) if reuse else Path(tempfile.mkdtemp(prefix=f"{market}.addition.", dir=live.parent)))
    component = run / "new_mode_replay"
    stages = []
    status_path = root / "status.json"
    if resumed:
        previous_operation = _object(status_path)
        if (previous_operation.get("status") != "failed_retryable"
                or previous_operation.get("promotion_completed") is not False
                or Path(previous_operation.get("candidate_dir", "")).resolve() != _resolved(resumed).resolve()
                or previous_operation.get("plan", {}).get("replacement") != target
                or previous_operation.get("plan", {}).get("history") != history):
            raise ValueError("resume requires this exact failed, unpromoted additive operation and scope")
    status = {"schema_version": 1, "status": "running", "plan": plan,
              "run_dir": str(run), "candidate_dir": str(candidate), "stages": stages}
    _atomic_json(status_path, status)
    config_path = Path(target["market_config_path"])
    config_bytes = config_path.read_bytes()
    raw = yaml.safe_load(config_bytes)
    if raw.get("enabled") is not False:
        raise ValueError("independent addition requires a genuinely disabled configuration")
    config = load_market_config(config_path)
    accepted_signals = _resolved(resolved_live_output_dir(config))
    signal_root = (_resolved(args.staged_signal_root) if args.staged_signal_root
                   else accepted_signals if resumed else run / "signal_root")
    # backfill --live-output-dir is an exact output directory, not a base
    # which receives another implicit model suffix.
    scoped = signal_root
    promoted = enabled = stopped = False
    maintenance = ExitStack()
    try:
        if not args.staged_signal_root and not resumed:
            stages.append(_run("signal_backfill", [sys.executable,
                str(REPO_ROOT / "scripts/backfill_tw_day_trade_open_signals.py"),
                "--market-config", str(config_path), "--start-date", history["start_date"],
                "--end-date", history["end_date"], "--live-output-dir", str(signal_root),
                "--artifact-root", str(_resolved(Path(config.output_dir)))], run_dir=run))
        # Reuse is never acceptance: inspect every summary/checkpoint first.
        isolated_configs = run / "markets"
        isolated_configs.mkdir()
        staged = dict(raw, enabled=True, live_output_dir=str(signal_root))
        if raw.get("model_selection_path"):
            staged["model_selection_path"] = str((config_path.parent / raw["model_selection_path"]).resolve())
        staged_path = isolated_configs / config_path.name
        atomic_write_text(staged_path, yaml.safe_dump(staged, allow_unicode=True, sort_keys=False))
        start = date.fromisoformat(history["start_date"])
        end = date.fromisoformat(history["end_date"])
        _, sessions, _ = _latest_completed_session(tw_public_dir=_resolved(args.tw_public_dir), start_date=start)
        sessions = [d for d in sessions if d <= end]
        # Preserve selector model scoping while making the isolated root exact.
        # The selector owns model fields; signal verification uses its ModeSpec
        # with only this explicit output path replaced.
        staged_plan = {**plan, "replacement": {**target, "market_config_path": str(staged_path)},
                       "verification_signal_dir": str(scoped)}
        signals = _verify_generated_signals(staged_plan, sessions, isolated_configs)
        stages.append({"stage": "verify_generated_signals", **signals})
        if accepted_signals.exists():
            # A previous failed operation may already have published these exact
            # immutable signals. Never replace a different accepted artifact.
            for source in scoped.rglob("*"):
                if source.is_file():
                    target_file = accepted_signals / source.relative_to(scoped)
                    if not target_file.is_file() or _sha256(source) != _sha256(target_file):
                        raise ValueError("accepted signal root differs from the independently verified candidate")
        else:
            accepted_signals.parent.mkdir(parents=True, exist_ok=True)
            pending = Path(tempfile.mkdtemp(prefix=".signals-addition.", dir=accepted_signals.parent))
            shutil.copytree(scoped, pending, dirs_exist_ok=True)
            os.replace(pending, accepted_signals)
        if not reuse:
            stages.append(_run("new_mode_replay", [sys.executable,
            str(REPO_ROOT / "scripts/rebuild_tw_day_trade_open_price_replay.py"),
            "--markets-dir", str(isolated_configs), "--only-market", market,
            "--single-market-signal-dir", str(accepted_signals), "--state-dir", str(component),
            "--start-date", history["start_date"], "--end-date", history["end_date"],
            "--benchmark-state-source", str(live / "state.json"),
            "--benchmark-history-source", str(live / "benchmark_history.json"),
                "--replay-intraday-kbars",
                *_addition_replay_flags(args, market=market, live=live)], run_dir=run))
        def curves(state_dir, stage):
            state = _object(state_dir / "state.json")
            carried = any(mode.get("margin_carry_contract") for mode in state["modes"].values())
            flags = ["--revalue-carried-marks"] if carried else ["--repair-unverified-strategy-marks"]
            command = [sys.executable,
                str(REPO_ROOT / "scripts/rebuild_tw_day_trade_minute_curves.py"),
                "--state-dir", str(state_dir), "--start-date", history["start_date"],
                "--end-date", history["end_date"], "--output-dir", str(run / stage),
                "--simulation", "--fetch-missing-kbars", *flags,
                "--revalue-opening-marks", "--publish"]
            for day in sessions:
                command.extend(("--expected-session-date", day.isoformat()))
            for item in state["modes"]:
                command.extend(("--expected-market", item))
            return _run(stage, command, run_dir=run)
        if not reuse:
            stages.append(curves(component, "new_mode_minute_curves"))
            aborted_candidate = None
            if resumed and market in (_object(live / "state.json").get("modes") or {}):
                aborted_candidate = _resolved(resumed)
            else:
                _flat_live_state(live, retained)
            addition = merge_addition(live, component, candidate, added_market=market,
                                      aborted_activation_candidate=aborted_candidate)
            stages.append({"stage": "retain_old_accounts_and_execution_bytes", "receipt": addition})
            stages.append(curves(candidate, "combined_minute_curves"))
        else:
            candidate_state = _object(candidate / "state.json")
            added_mode = candidate_state["modes"][market]
            if (set(candidate_state["modes"]) != expected
                    or added_mode.get("checkpoint_fingerprint") != target["checkpoint_fingerprint"]
                    or added_mode.get("initial_capital_twd") != target["initial_capital_twd"]):
                raise ValueError("resumed candidate model/capital/mode set differs")
            if market in (_object(live / "state.json").get("modes") or {}):
                stages.append({"stage": "preserve_aborted_unfunded_activation",
                               "receipt": preserve_aborted_activation(live, candidate)})
            stages.append({"stage": "reuse_candidate_not_acceptance", "candidate": str(candidate),
                           "previous_run_dir": previous_operation["run_dir"]})
        promote = [sys.executable, str(REPO_ROOT / "scripts/promote_tw_day_trade_replay.py"),
                   "--live-dir", str(live), "--candidate-dir", str(candidate), "--added-market", market]
        for item in sorted(expected):
            promote.extend(("--expected-market", item))
        stages.append(_run("validate_addition", [*promote, "--validate-only"], run_dir=run))
        # Slow source verification occurs while the existing engine stays up.
        # Flatness and configuration identity are fenced again at cutover.
        enabled_text = yaml.safe_dump(dict(raw, enabled=True), allow_unicode=True, sort_keys=False)
        def cutover():
            nonlocal stopped, enabled
            maintenance.enter_context(control_lease(owner=f"add-mode:{market}:{run.name}", timeout_seconds=90))
            if _object(candidate / "rebuild_receipt.json").get("aborted_activation"):
                validate_aborted_activation(candidate, live=live)
            else:
                _flat_live_state(live, retained)
            subprocess.run(["systemctl", "stop", SIMULATION_SERVICE], check=True)
            stopped = True
            if config_path.read_bytes() != config_bytes:
                raise RuntimeError("candidate configuration changed while staging; refusing to overwrite")
            _atomic_json(run / "configuration_rollback.json", {"path": str(config_path), "content": config_bytes.decode()})
            atomic_write_text(config_path, enabled_text)
            enabled = True
        _atomic_json(status_path, status)
        stages.append(_promote_with_cutover(promote, before_exchange=cutover,
                                            stage="promote_addition", run_dir=run))
        promoted = True
        subprocess.run(["systemctl", "start", SIMULATION_SERVICE], check=True)
        stopped = False
        _wait_active(SIMULATION_SERVICE)
        sync = _wait_engine_sync(live, expected)
        stages.append({"stage": "include_added_mode_in_discord_schedule",
                       "receipt": _ensure_discord_deployment_schedule(market, run_dir=run)})
        previous = _object(_resolved(DISCORD_STATUS_PATH)).get("run_id", "")
        discord_restart = _restart_and_wait_active(DISCORD_SERVICE)
        discord = _wait_discord_ready(expected_engine_run_id=str(sync.get("engine_run_id") or ""),
            expected_markets=expected, target_market=market,
            checkpoint_fingerprint=str(target["checkpoint_fingerprint"]), previous_run_id=str(previous))
        public_restart = _restart_and_wait_active(PUBLIC_SERVICE)
        dashboard = _verify_dashboard(market=market, start_date=history["start_date"],
            end_date=history["end_date"], expected_markets=expected)
        promotion = _object(live / "promotion_receipt.json")
        if (promotion["acceptance"]["minute_curve_validation"]["validated_rows"]
                != history["expected_strategy_minute_rows"]):
            raise RuntimeError("combined minute count does not meet the planned all-mode scope")
        result = {**status, "status": "complete_with_data_gaps" if dashboard["local_status"]["operational_issues"] else "complete",
                  "completed_at": datetime.now(TAIPEI).isoformat(), "signal_acceptance": signals,
                  "promotion_completed": True,
                  "promotion_receipt": str(live / "promotion_receipt.json"),
                  "rollback_directory": promotion["rollback_directory"], "engine_sync": sync,
                  "discord": {"run_id": discord.get("run_id"), "restart": discord_restart,
                              "warmup": discord.get("startup_inference_warmup")},
                  "public_restart": public_restart, "dashboard_acceptance": dashboard}
        _atomic_json(status_path, result)
        return result
    except Exception as exc:
        if enabled and not promoted and config_path.read_bytes() == enabled_text.encode():
            atomic_write_text(config_path, config_bytes.decode())
        _atomic_json(status_path, {**status, "status": "failed_retryable", "error_type": type(exc).__name__,
                                 "error": str(exc), "promotion_completed": promoted})
        raise
    finally:
        try:
            if stopped:
                subprocess.run(["systemctl", "start", SIMULATION_SERVICE], check=True)
        finally:
            maintenance.close()


def _prepare_replacement_candidate(args, plan, *, candidate, live_dir, sessions, run_dir, stages):
    if plan.get("operation") == "extend_history":
        proof = plan["prepared_history"]
        if _sha256(candidate / "rebuild_receipt.json") != proof["rebuild_receipt_sha256"]:
            raise RuntimeError("prepared history changed after planning")
        _require_history_source_unchanged(live_dir, proof["source_streams"])
        accepted = {"retained_decision_count": proof["retained_decision_count"],
                    "counterfactual_prefix_count": proof["counterfactual_prefix_count"],
                    "replacement_signal_markets": []}
        stages.append({"stage": "retain_decisions_and_accept_prepared_history", **accepted})
        return accepted, []
    python, replacement = sys.executable, plan["replacement"]
    start, end = str(plan["history"]["start_date"]), str(plan["history"]["end_date"])
    market = str(replacement["stable_market_id"])
    for index, repair in enumerate(plan["source_ledger_repairs"]):
        repair_dates = [date.fromisoformat(str(value)) for value in repair["session_dates"]]
        stages.append(_run(f"repair_signal_backfill_{index:02d}", [python,
            str(REPO_ROOT / "scripts/backfill_tw_day_trade_open_signals.py"),
            "--market-config", str(repair["market_config_path"]),
            "--start-date", min(repair_dates).isoformat(), "--end-date", max(repair_dates).isoformat()],
            run_dir=run_dir))
    repairs = _verify_repaired_signals(plan["source_ledger_repairs"], _resolved(args.markets_dir))
    stages.append({"stage": "verify_repaired_signals", "repairs": repairs})
    stages.append(_run("signal_backfill", [python,
        str(REPO_ROOT / "scripts/backfill_tw_day_trade_open_signals.py"),
        "--market-config", str(replacement["market_config_path"]),
        "--start-date", start, "--end-date", end], run_dir=run_dir))
    signals = _verify_generated_signals(plan, sessions, _resolved(args.markets_dir))
    stages.append({"stage": "verify_generated_signals", **signals})
    command = [python, str(REPO_ROOT / "scripts/rebuild_tw_day_trade_open_price_replay.py"),
               "--markets-dir", str(_resolved(args.markets_dir)), "--state-dir", str(candidate),
               "--start-date", start, "--end-date", end,
               "--benchmark-state-source", str(live_dir / "state.json"),
               "--source-ledger-dir", str(live_dir), "--replace-signal-market", market,
               "--reuse-retained-signal-open", "--replay-intraday-kbars"]
    for repair in plan["source_ledger_repairs"]:
        command.extend(("--allow-unpinned-market", str(repair["market"])))
    for manifest in getattr(args, "accepted_repair_manifest", ()):
        command.extend(("--accepted-repair-manifest", str(_resolved(manifest))))
    stages.append(_run("replay", command, run_dir=run_dir))
    stages.append(_run("minute_curves", [python,
        str(REPO_ROOT / "scripts/rebuild_tw_day_trade_minute_curves.py"),
        "--state-dir", str(candidate), "--start-date", start, "--end-date", end,
        "--output-dir", str(run_dir / "minute_curves"), "--simulation", "--fetch-missing-kbars",
        "--repair-unverified-strategy-marks", "--revalue-opening-marks", "--publish"], run_dir=run_dir))
    return signals, repairs


def _apply(args: argparse.Namespace, plan: dict[str, Any]) -> dict[str, Any]:
    if getattr(args, "candidate_preflight", False) or plan.get("plan_complete") is False:
        raise RuntimeError("a read-only candidate preflight cannot be applied")
    if plan.get("operation") == "add_independent_mode":
        return _apply_addition(args, plan)
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
    candidate = (Path(plan["prepared_history"]["candidate_dir"])
                 if plan.get("operation") == "extend_history" else Path(
                     tempfile.mkdtemp(prefix=f"{market}.replacement.", dir=live_dir.parent)))
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
    promoted = False
    try:
        signal_acceptance, repair_acceptance = _prepare_replacement_candidate(
            args, plan, candidate=candidate, live_dir=live_dir, sessions=sessions,
            run_dir=run_dir, stages=stages,
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
        maintenance = ExitStack()
        try:
            from stockagent.live.day_trade_maintenance import control_lease
            def cutover():
                nonlocal stopped
                maintenance.enter_context(control_lease(owner=f"replace-mode:{market}:{run_dir.name}", timeout_seconds=90))
                _flat_live_state(live_dir, expected_markets)
                if plan.get("operation") == "extend_history":
                    _require_history_source_unchanged(live_dir, plan["prepared_history"]["source_streams"])
                subprocess.run(["systemctl", "stop", SIMULATION_SERVICE], check=True)
                stopped = True
                _flat_live_state(live_dir, expected_markets)
                if plan.get("operation") == "extend_history":
                    _require_history_source_unchanged(live_dir, plan["prepared_history"]["source_streams"])
            stages.append(_promote_with_cutover(promote_command, before_exchange=cutover,
                                                stage="promote", run_dir=run_dir))
            promoted = True
            _atomic_json(status_path, {**running, "promotion_completed": True})
        finally:
            try:
                if (
                    stopped
                    or subprocess.run(
                        ["systemctl", "is-active", "--quiet", SIMULATION_SERVICE],
                        check=False,
                    ).returncode != 0
                ):
                    subprocess.run(["systemctl", "start", SIMULATION_SERVICE], check=True)
            finally:
                maintenance.close()

        _wait_active(SIMULATION_SERVICE)
        sync = _wait_engine_sync(live_dir, expected_markets)
        stages.append({"stage": "confirm_discord_deployment_schedule",
                       "receipt": _ensure_discord_deployment_schedule(market, run_dir=run_dir)})
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
            "promotion_completed": True,
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
            "promotion_completed": promoted,
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
    parser.add_argument("--accepted-repair-manifest", action="append", type=Path, default=[],
                        help="Receipt-backed original/repair signal lineage, validated before planning and replay.")
    parser.add_argument("--history-extension", action="store_true",
                        help="Extend the earlier missing history only; retain all existing signal decisions and strategy identities.")
    parser.add_argument("--prepared-candidate", type=Path,
                        help="Fully rebuilt and minute-validated isolated history candidate; requires --history-extension.")
    parser.add_argument("--add-mode", action="store_true", help="Add an independent paper mode; retain all current accounts and execution bytes.")
    parser.add_argument("--historical-full-fill-0901", action="store_true",
                        help="Explicit addition-only historical scenario: a full legal NAV-funded target at sourced 09:01 prices; no liquidity or broker-fill claim. Keep this flag for verify.")
    parser.add_argument("--staged-signal-root", type=Path, help="Reuse an isolated signal root only after full checkpoint/session validation; addition only.")
    parser.add_argument("--resume-addition-candidate", type=Path,
                        help="Revalidate an exact failed, unpromoted additive candidate; preserve any provably unfunded aborted activation.")
    parser.add_argument("--rebuild-resumed-addition", action="store_true",
                        help="Preserve the failed candidate and rebuild its independent replay/curves from fresh pinned source bytes.")
    parser.add_argument("--expected-initial-capital", type=float, default=10_000_000.0)
    parser.add_argument(
        "--candidate-preflight", action="store_true",
        help="Read-only plan for a disabled independent candidate; reports blockers and never authorizes replacement apply.",
    )
    parser.add_argument(
        "--expected-artifact-root", type=Path,
        help="Require this exact training artifact after model-selector resolution; never substitute a similarly named run.",
    )
    parser.add_argument(
        "--markets-dir", type=Path, default=Path("services/discord_bot/markets")
    )
    parser.add_argument("--live-dir", type=Path, default=DEFAULT_LIVE_DIR)
    parser.add_argument("--tw-public-dir", type=Path, default=DEFAULT_TW_PUBLIC_DIR)
    parser.add_argument("--operations-root", type=Path, default=DEFAULT_OPERATIONS_ROOT)
    return parser


def _recover_promoted_consumers(args, plan, previous_operation):
    """Finish consumers after an accepted exchange; never replay/promote twice."""
    from stockagent.live.day_trade_maintenance import control_lease
    target, history = plan["replacement"], plan["history"]
    market, expected = target["stable_market_id"], set(plan["active_markets"])
    if (plan.get("operation") not in {"add_independent_mode", "extend_history"}
            or previous_operation.get("status") != "failed_retryable"
            or previous_operation.get("promotion_completed") is not True
            or not plan["current_deployment"]["matches"]
            or previous_operation.get("plan", {}).get("replacement") != target
            or previous_operation.get("plan", {}).get("history") != history):
        raise ValueError("consumer recovery requires the exact already-promoted failed operation")
    live = _resolved(args.live_dir)
    run = Path(previous_operation["run_dir"])
    with control_lease(owner=f"deployed-consumer-recovery:{market}", timeout_seconds=90):
        _flat_live_state(live, expected)
        sync = _wait_engine_sync(live, expected)
        schedule = _ensure_discord_deployment_schedule(market, run_dir=run)
        previous_run = _object(_resolved(DISCORD_STATUS_PATH)).get("run_id", "")
        restart = _restart_and_wait_active(DISCORD_SERVICE)
        discord = _wait_discord_ready(expected_engine_run_id=sync["engine_run_id"],
            expected_markets=expected, target_market=market,
            checkpoint_fingerprint=target["checkpoint_fingerprint"], previous_run_id=str(previous_run))
        public = _restart_and_wait_active(PUBLIC_SERVICE)
        dashboard = _verify_dashboard(market=market, start_date=history["start_date"],
                                     end_date=history["end_date"], expected_markets=expected)
        promotion = _object(live / "promotion_receipt.json")
        if promotion["acceptance"]["minute_curve_validation"]["validated_rows"] != history["expected_strategy_minute_rows"]:
            raise RuntimeError("consumer recovery minute scope differs")
        result = {**previous_operation,
            "status": "complete_with_data_gaps" if dashboard["local_status"]["operational_issues"] else "complete",
            "previous_consumer_failure": previous_operation.get("error"), "error": None,
            "consumer_recovered_at": datetime.now(TAIPEI).isoformat(), "promotion_completed": True,
            "completed_at": datetime.now(TAIPEI).isoformat(), "schedule_update": schedule,
            "promotion_receipt": str(live / "promotion_receipt.json"),
            "rollback_directory": promotion["rollback_directory"], "engine_sync": sync,
            "discord": {"run_id": discord.get("run_id"), "restart": restart,
                        "warmup": discord.get("startup_inference_warmup")},
            "public_restart": public, "dashboard_acceptance": dashboard,
            "second_ledger_promotion_performed": False}
        _atomic_json(_resolved(args.operations_root) / market / "status.json", result)
        return result


def main() -> None:
    args = build_parser().parse_args()
    if args.historical_full_fill_0901 and not args.add_mode:
        raise ValueError("--historical-full-fill-0901 requires --add-mode; never changes live execution")
    if args.prepared_candidate and not args.history_extension:
        raise ValueError("--prepared-candidate requires --history-extension")
    if args.history_extension and (args.add_mode or args.candidate_preflight or args.staged_signal_root
                                   or args.resume_addition_candidate or args.rebuild_resumed_addition):
        raise ValueError("history extension cannot be combined with strategy addition/replacement inputs")
    if args.staged_signal_root and not args.add_mode:
        raise ValueError("--staged-signal-root requires --add-mode")
    if args.resume_addition_candidate and (not args.add_mode or args.action not in {"plan", "apply"}):
        raise ValueError("--resume-addition-candidate requires additive plan/apply")
    if args.rebuild_resumed_addition and not args.resume_addition_candidate:
        raise ValueError("--rebuild-resumed-addition requires --resume-addition-candidate")
    if args.candidate_preflight and args.action != "plan":
        raise ValueError("--candidate-preflight is read-only and supports plan only")
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
        status_path = _resolved(args.operations_root) / plan["replacement"]["stable_market_id"] / "status.json"
        prior = _object(status_path) if status_path.is_file() else {}
        if (args.add_mode or args.history_extension) and prior.get("status") == "failed_retryable" and prior.get("promotion_completed") is True:
            print(json.dumps(_recover_promoted_consumers(args, plan, prior), ensure_ascii=False, indent=2, sort_keys=True))
            return
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

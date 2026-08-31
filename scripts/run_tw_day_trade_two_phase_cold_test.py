#!/usr/bin/env python3
"""Run a hermetic full-session cold-start drill for TW day-trade simulation.

The drill deliberately starts with every registered TW public source stale,
no same-session opening observation, no signal pointer, and an unsynchronised
Discord receipt.  It then atomically applies repaired source receipts and
executes the same production pre-open readiness gate.  Only after that gate is
ready does it inject an opening observation, publish three atomic signal
pointers, and execute them immediately after 09:00 using a strictly later best
Ask/Bid.  A separate isolated phase proves that historical replay alone is
recorded at 09:01 using the observed official session open.

The final phase advances that same durable engine through intraday marking,
13:20 passive exits, 13:24 market-at-best force exits, the 13:30 close, and two
process restarts.  This proves recovery and once-only ledger semantics in
addition to the original two opening phases.

No network request, Shioaji login, broker order, systemd mutation, live-root
write, or production-ledger write is performed.  External source and quote
delivery are fixtures; readiness, signal-pointer consumption, live causal
best-quote execution, historical 09:01 replay, ledger durability, and post-open
acceptance use the production implementations.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import date, datetime, time as datetime_time, timedelta
import hashlib
import json
from pathlib import Path
import sys
import time
from typing import Any, Mapping
import uuid
from zoneinfo import ZoneInfo

import numpy as np


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.download_tw_public_data import DEFAULT_DATASETS  # noqa: E402
from scripts.check_tw_day_trade_preopen_readiness import (  # noqa: E402
    _atomic_json,
    evaluate_readiness,
)
from scripts.run_tw_day_trade_simulation import (  # noqa: E402
    _LATEST_SIGNAL_CACHE,
    _LEGACY_SIGNAL_SCAN_CACHE,
    _latest_signal,
    _mode_specs,
)
from stockagent.live.tw_day_trade_service_sync import load_service_sync  # noqa: E402
from stockagent.live.tw_day_trade_simulation import (  # noqa: E402
    ENTRY_FILL_POLICY_CAUSAL_BOOK,
    ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901,
    LiveEligibility,
    ModeSpec,
    TwDayTradeSimulationEngine,
)


TAIPEI = ZoneInfo("Asia/Taipei")
EXPECTED_MARKETS = (
    "tw_day_trade_100m",
    "tw_day_trade_multi_basis",
    "tw_day_trade_multi_basis_projection_l1_gelu",
)
TARGET_WEIGHTS = {
    "tw_day_trade_100m": 0.01,
    "tw_day_trade_multi_basis": -0.10,
    "tw_day_trade_multi_basis_projection_l1_gelu": 0.10,
}


class ColdTestFailure(RuntimeError):
    """One or more deterministic cold-start acceptance checks failed."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--session-date",
        type=date.fromisoformat,
        default=None,
        help="Synthetic weekday session in YYYY-MM-DD form (default: today/prior weekday).",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("artifacts/validation/tw_day_trade_two_phase_cold_test"),
    )
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--real-model-inference",
        action="store_true",
        help=(
            "also run cold+hot GPU inference for all three real configs/checkpoints; "
            "the opening observation remains an isolated fixture"
        ),
    )
    return parser.parse_args()


def _default_session_date() -> date:
    session = datetime.now(TAIPEI).date()
    while session.weekday() >= 5:
        session -= timedelta(days=1)
    return session


def _default_future_session_date() -> date:
    """Choose a synthetic session strictly after every completed daily panel."""

    session = datetime.now(TAIPEI).date() + timedelta(days=1)
    while session.weekday() >= 5:
        session += timedelta(days=1)
    return session


def _at(
    session: date,
    hour: int,
    minute: int,
    second: int = 0,
    microsecond: int = 0,
) -> datetime:
    return datetime(
        session.year,
        session.month,
        session.day,
        hour,
        minute,
        second,
        microsecond,
        tzinfo=TAIPEI,
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _assert_check(
    checks: list[dict[str, Any]],
    name: str,
    condition: bool,
    *,
    evidence: Any = None,
) -> None:
    row = {"name": name, "passed": bool(condition)}
    if evidence is not None:
        row["evidence"] = evidence
    checks.append(row)
    if not condition:
        raise ColdTestFailure(f"cold-test check failed: {name}; evidence={evidence!r}")


def _enabled_specs(sandbox: Path) -> list[ModeSpec]:
    specs, _configs, errors = _mode_specs(
        REPO_ROOT / "services/discord_bot/markets"
    )
    if errors:
        raise ColdTestFailure(f"active mode configuration errors: {errors}")
    by_market = {spec.market: spec for spec in specs}
    if set(by_market) != set(EXPECTED_MARKETS):
        raise ColdTestFailure(
            "active mode set mismatch: "
            f"expected={list(EXPECTED_MARKETS)} observed={sorted(by_market)}"
        )
    isolated: list[ModeSpec] = []
    for market in EXPECTED_MARKETS:
        spec = by_market[market]
        isolated.append(
            replace(
                spec,
                live_output_dir=sandbox / "signals" / market,
                entry_fill_policy=ENTRY_FILL_POLICY_CAUSAL_BOOK,
                entry_price_offset_ticks=0,
            )
        )
    return isolated


def _stale_public_receipt(session: date) -> dict[str, Any]:
    yesterday = session - timedelta(days=1)
    return {
        "schema_version": 3,
        "status": "waiting",
        "started_at_taipei": _at(yesterday, 8, 0).isoformat(),
        "acceptance": {
            "subprocess_ok": False,
            "live_root_receipt_fresh": False,
            "live_root_status_ok": False,
            "live_root_verified": False,
            "coverage_complete": False,
            "same_session_eligibility": False,
            "runtime_materialization_required": False,
            "runtime_materialized_snapshot": False,
        },
    }


def _ready_public_receipt(session: date) -> dict[str, Any]:
    return {
        "schema_version": 3,
        "status": "ok",
        "started_at_taipei": _at(session, 8, 0).isoformat(),
        "completed_at_taipei": _at(session, 8, 59, 10).isoformat(),
        "live_runtime": {
            "authority": "catalog_mutable_live_root",
            "atomic_file_replacement": True,
            "packed_release_required_for_opening": False,
            "materialization_required_for_opening": False,
        },
        "acceptance": {
            "subprocess_ok": True,
            "live_root_receipt_fresh": True,
            "live_root_status_ok": True,
            "live_root_verified": True,
            "coverage_complete": True,
            "same_session_eligibility": True,
            "runtime_materialization_required": False,
            "runtime_materialized_snapshot": False,
        },
    }


def _event_receipt(
    session: date,
    *,
    ready: bool,
    updated_at: datetime,
) -> dict[str, Any]:
    total = len(DEFAULT_DATASETS)
    return {
        "schema_version": 1,
        "status": "ok" if ready else "degraded",
        "coverage_complete": ready,
        "registered_dataset_count": total,
        "monitored_dataset_count": total,
        "observed_dataset_count": total if ready else total - 1,
        "failed_probe_count": 0 if ready else 1,
        "unapplied_event_count": 0 if ready else 1,
        "updated_at_taipei": updated_at.isoformat(timespec="milliseconds"),
        "session_date": session.isoformat(),
    }


def _model_receipt(
    specs: list[ModeSpec],
    session: date,
    *,
    ready: bool,
) -> dict[str, Any]:
    completed = _at(session, 8, 59, 20).isoformat(timespec="milliseconds")
    run_id = "two-phase-cold-test-model-process"
    rows: dict[str, Any] = {}
    for spec in specs:
        rows[spec.market] = {
            "status": "ready" if ready else "warming",
            "completed_at": completed if ready else None,
            "final_arm": {
                "status": "ready" if ready else "pending",
                "run_id": run_id,
                "completed_at": completed if ready else None,
                "live_latency": {
                    "panel_cache_hit": ready,
                    "checkpoint_cache_hit": ready,
                    "model_cache_hit": ready,
                },
                "opening_source_prewarm": {
                    "ready": ready,
                    "run_id": run_id,
                    "source": "twse_tpex:mis",
                    "proof": "fixture_same_session_opening_source",
                },
            },
        }
    return {
        "schema_version": 1,
        "run_id": run_id,
        "session_date": session.isoformat(),
        "markets": rows,
    }


def _simulation_receipt(session: date, *, ready: bool) -> dict[str, Any]:
    checked = _at(session, 8, 59, 25).isoformat(timespec="milliseconds")
    status = "ready" if ready else "pending"
    return {
        "schema_version": 1,
        "session_date": session.isoformat(),
        "status": "ready" if ready else "warming",
        "simulation_only": True,
        "production_order_possible": False,
        "updated_at": checked,
        "components": {
            "eligibility": {"status": status, "checked_at": checked},
            "shioaji_quote": {"status": status, "checked_at": checked},
        },
    }


def _discord_receipt(
    sync: Mapping[str, Any],
    specs: list[ModeSpec],
    *,
    updated_at: datetime,
    revision_lag: int = 0,
) -> dict[str, Any]:
    engine_revision = int(sync.get("state_revision") or 0)
    return {
        "schema_version": 1,
        "updated_at": updated_at.isoformat(timespec="milliseconds"),
        "engine_run_id": sync.get("engine_run_id"),
        "engine_state_revision": engine_revision - int(revision_lag),
        "simulation_only": True,
        "production_order_possible": False,
        "discord_connected": True,
        "day_trade_markets": [spec.market for spec in specs],
    }


def _service_states() -> dict[str, str]:
    return {
        "stockagent-discord-bot.service": "active",
        "stockagent-tw-day-trade-simulation.service": "active",
        "stockagent-tw-public-source-events.service": "active",
    }


def _source_matrix(session: date, *, ready: bool) -> dict[str, Any]:
    status = "ready" if ready else "stale_or_missing"
    rows = [
        {
            "dataset": name,
            "status": status,
            "gate_session": session.isoformat(),
            "fixture_revision": "current" if ready else "previous_or_absent",
            "atomic_replacement": ready,
        }
        for name in sorted(DEFAULT_DATASETS)
    ]
    return {
        "schema_version": 1,
        "session_date": session.isoformat(),
        "registered_dataset_count": len(rows),
        "ready_dataset_count": len(rows) if ready else 0,
        "stale_or_missing_dataset_count": 0 if ready else len(rows),
        "datasets": rows,
    }


def _publish_signal_fixture(
    spec: ModeSpec,
    *,
    session: date,
    signal_at: datetime,
    target_weight: float,
) -> None:
    signal_id = f"cold-{session.isoformat()}-{spec.market}"
    output = spec.live_output_dir / session.isoformat() / signal_id
    summary_path = output / "summary.json"
    execution_path = output / "execution_weights.json"
    weights_path = output / "target_weights.parquet"
    summary = {
        "schema_version": 1,
        "market": spec.market,
        "signal_id": signal_id,
        "asof_date": signal_at.isoformat(timespec="microseconds"),
        "generated_at": signal_at.isoformat(timespec="microseconds"),
        "signal_started_at": signal_at.isoformat(timespec="microseconds"),
        "signal_ready_at": signal_at.isoformat(timespec="microseconds"),
        "artifact_published_at": signal_at.isoformat(timespec="microseconds"),
        "execution_mode": "tw_day_trade",
        "live_session_open_feature_applied": True,
        "feature_cutoff_date": (
            _at(session - timedelta(days=1), 13, 30).isoformat(timespec="seconds")
        ),
        "checkpoint_fingerprint": "cold-test-real-checkpoint-present",
        "config_fingerprint": "cold-test-production-config-contract",
        "weights_path": str(weights_path),
        "positions_markdown_path": str(output / "target_positions.md"),
        "symbol_count": 1,
        "target_risk": {
            "gross": abs(target_weight),
            "long_gross": max(0.0, target_weight),
            "short_gross": max(0.0, -target_weight),
        },
        "simulation_only": True,
        "production_order_possible": False,
    }
    row = {
        "symbol": "2330",
        "name": "台積電",
        "target_weight": target_weight,
        "tradable": True,
        "can_buy": True,
        "can_sell": True,
        "score": 1.0 if target_weight > 0 else -1.0,
        "raw_score": 1.0 if target_weight > 0 else -1.0,
    }
    _atomic_json(summary_path, summary)
    _atomic_json(
        execution_path,
        {
            "schema_version": 1,
            "market": spec.market,
            "signal_id": signal_id,
            "rows": [row],
        },
    )
    _atomic_json(
        spec.live_output_dir / "latest_signal.json",
        {
            "schema_version": 1,
            "summary_path": str(summary_path),
            "weights_path": str(weights_path),
            "execution_weights_path": str(execution_path),
        },
    )


def _eligibility(session: date) -> dict[str, LiveEligibility]:
    return {
        "2330": LiveEligibility(
            symbol="2330",
            venue="twse",
            security_type="stock",
            eligible=True,
            short_open=True,
            covered=True,
            source_date=session.isoformat(),
        )
    }


def _opening_quote(session: date, observed_at: datetime) -> dict[str, Any]:
    return {
        "symbol": "2330",
        "open": 999.0,
        "last": 999.0,
        "bid": None,
        "ask": None,
        "bid_volume": 0.0,
        "ask_volume": 0.0,
        "minute_volume_lots": 0.0,
        "upper_limit": 1_095.0,
        "lower_limit": 900.0,
        "quote_at": observed_at.isoformat(timespec="microseconds"),
        "source": "cold_test_opening_fixture",
    }


def _best_book_quote(session: date, observed_at: datetime) -> dict[str, Any]:
    return {
        "symbol": "2330",
        "open": 999.0,
        "last": 999.0,
        "bid": 998.0,
        "ask": 1_000.0,
        # One displayed board lot proves that the live causal path respects the
        # executable top-of-book quantity instead of fabricating liquidity.
        "bid_volume": 1.0,
        "ask_volume": 1.0,
        "minute_volume_lots": 0.0,
        "upper_limit": 1_095.0,
        "lower_limit": 900.0,
        "quote_at": observed_at.isoformat(timespec="microseconds"),
        "source": "cold_test_causally_later_level_one_fixture",
    }


def _lifecycle_quote(
    observed_at: datetime,
    *,
    bid: float | None,
    ask: float | None,
    last: float,
    bid_volume: float,
    ask_volume: float,
    minute_volume_lots: float,
) -> dict[str, Any]:
    return {
        "symbol": "2330",
        "open": 999.0,
        "last": last,
        "bid": bid,
        "ask": ask,
        "bid_volume": bid_volume,
        "ask_volume": ask_volume,
        "minute_volume_lots": minute_volume_lots,
        "upper_limit": 1_095.0,
        "lower_limit": 900.0,
        "quote_at": observed_at.isoformat(timespec="microseconds"),
        "source": "cold_test_full_session_level_one_fixture",
    }


def _run_real_model_inference_shadow(
    specs: list[ModeSpec],
    *,
    session: date,
) -> dict[str, Any]:
    """Run real cold+hot inference while replacing only external opening I/O."""

    import torch

    from stockagent.live import signal_engine
    from stockagent.live.quote_provider import PriceSnapshot

    asof = _at(session, 9, 0, 0, 300_000)
    timestamp_ms = int(asof.timestamp() * 1_000)
    original_price_snapshot = signal_engine._price_snapshot

    def fixture_price_snapshot(
        *,
        source: str,
        symbols: list[str],
        fallback_prices: np.ndarray,
        parquet_root: str | Path,
        prices_csv: str | Path | None,
        yahoo_chunk_size: int,
        request_mask: np.ndarray | None = None,
        require_official_tw_session_open: bool = False,
    ) -> PriceSnapshot:
        del (
            source,
            parquet_root,
            prices_csv,
            yahoo_chunk_size,
            request_mask,
            require_official_tw_session_open,
        )
        closes = np.asarray(fallback_prices, dtype=np.float64)
        available = np.isfinite(closes) & (closes > 0.0)
        opens = np.where(available, closes * 1.001, np.nan)
        prices = np.where(available, opens, closes)
        return PriceSnapshot(
            prices=prices,
            source="cold_test:opening_fixture",
            timestamp=asof.isoformat(timespec="microseconds"),
            available_count=int(available.sum()),
            requested_count=len(symbols),
            available_mask=available,
            open_prices=opens,
            timestamps_ms=np.full((len(symbols),), timestamp_ms, dtype=np.int64),
        )

    results: dict[str, Any] = {}
    signal_engine.clear_live_inference_memory_cache()
    signal_engine._price_snapshot = fixture_price_snapshot
    try:
        for spec in specs:
            checkpoint = Path(str(spec.checkpoint_path or ""))
            if not checkpoint.is_file():
                raise ColdTestFailure(
                    f"real model shadow is missing checkpoint: {spec.market}: {checkpoint}"
                )
            output_dir = checkpoint.parent.parent
            common = {
                "market": spec.market,
                "market_label": spec.label,
                "config_path": spec.config_path,
                "output_dir": output_dir,
                "live_output_dir": spec.live_output_dir,
                "checkpoint_path": checkpoint,
                "panel_date": "latest",
                "asof_date": asof.isoformat(timespec="microseconds"),
                "price_source": "cold_test_fixture",
                "top_n": 2_400,
                "min_abs_delta": 0.0,
                "benchmark_window_days": 32,
                "write": False,
                "ensure_previous_signal": False,
                "previous_signal_backfill_limit": 0,
            }
            cold_started = time.perf_counter()
            cold = signal_engine.generate_live_signal(**common)
            cold_elapsed = time.perf_counter() - cold_started
            hot_started = time.perf_counter()
            hot = signal_engine.generate_live_signal(**common)
            hot_elapsed = time.perf_counter() - hot_started
            cold_latency = dict(cold.summary.get("live_latency") or {})
            hot_latency = dict(hot.summary.get("live_latency") or {})
            cache_keys = (
                "panel_cache_hit",
                "checkpoint_cache_hit",
                "model_cache_hit",
            )
            valid = bool(
                cold.summary.get("execution_mode") == "tw_day_trade"
                and cold.summary.get("live_session_open_feature_applied") is True
                and int(cold.summary.get("opening_price_available_count") or 0) > 0
                and all(hot_latency.get(key) is True for key in cache_keys)
            )
            if not valid:
                raise ColdTestFailure(
                    f"real model shadow contract failed: {spec.market}: "
                    f"cold={cold.summary.get('live_latency')} "
                    f"hot={hot.summary.get('live_latency')}"
                )
            results[spec.market] = {
                "status": "ready",
                "config_path": spec.config_path,
                "checkpoint_path": str(checkpoint),
                "panel_date": cold.summary.get("panel_date"),
                "feature_cutoff_date": cold.summary.get("feature_cutoff_date"),
                "opening_price_available_count": cold.summary.get(
                    "opening_price_available_count"
                ),
                "symbol_count": cold.summary.get("symbol_count"),
                "target_gross": cold.summary.get("target_gross"),
                "cold_elapsed_seconds": round(cold_elapsed, 6),
                "hot_elapsed_seconds": round(hot_elapsed, 6),
                "cold_latency": cold_latency,
                "hot_latency": hot_latency,
                "hot_cache_proof": {
                    key: hot_latency.get(key) for key in cache_keys
                },
                "weights_row_count": len(hot.weights_rows),
                "write_performed": False,
            }
    finally:
        signal_engine._price_snapshot = original_price_snapshot
        signal_engine.clear_live_inference_memory_cache()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
    return {
        "status": "ready",
        "external_opening_delivery": "deterministic_fixture",
        "model_compute": "real_config_checkpoint_panel_and_gpu",
        "write_performed": False,
        "markets": results,
    }


def run_two_phase_cold_test(
    *,
    session_date: date,
    output_root: Path,
    run_id: str | None = None,
    real_model_inference: bool = False,
) -> dict[str, Any]:
    if session_date.weekday() >= 5:
        raise ValueError("cold-test session-date must be a weekday")
    started = time.perf_counter()
    started_at = datetime.now(TAIPEI)
    resolved_run_id = run_id or started_at.strftime("%Y%m%dT%H%M%S%f")
    root = Path(output_root)
    if not root.is_absolute():
        root = REPO_ROOT / root
    run_dir = root / "runs" / resolved_run_id
    sandbox = run_dir / "sandbox"
    run_dir.mkdir(parents=True, exist_ok=False)
    checks: list[dict[str, Any]] = []
    _LATEST_SIGNAL_CACHE.clear()
    _LEGACY_SIGNAL_SCAN_CACHE.clear()

    specs = _enabled_specs(sandbox)
    markets = tuple(spec.market for spec in specs)
    engine = TwDayTradeSimulationEngine(sandbox / "paper_engine")
    strict_after = datetime_time(8, 57)

    # Phase 1A: worst-case cold state.  All public sources are stale/missing,
    # model/executor preparation is absent, and Discord is one revision behind.
    pre_fault_at = _at(session_date, 8, 57, 1)
    source_state_path = sandbox / "public_source_state.json"
    _atomic_json(source_state_path, _source_matrix(session_date, ready=False))
    stale_source_sha = _sha256(source_state_path)
    engine.update_readiness(specs, now=pre_fault_at)
    initial_sync = load_service_sync(engine.state_dir) or {}
    pre_fault = evaluate_readiness(
        observed=pre_fault_at,
        strict_after=strict_after,
        market_names=markets,
        public_receipt=_stale_public_receipt(session_date),
        model_receipt=_model_receipt(specs, session_date, ready=False),
        simulation_receipt=_simulation_receipt(session_date, ready=False),
        service_states=_service_states(),
        event_receipt=_event_receipt(
            session_date,
            ready=False,
            updated_at=pre_fault_at,
        ),
        engine_status_receipt=json.loads(
            engine.status_path.read_text(encoding="utf-8")
        ),
        engine_sync_receipt=initial_sync,
        discord_status_receipt=_discord_receipt(
            initial_sync,
            specs,
            updated_at=pre_fault_at,
            revision_lag=1,
        ),
    )
    _assert_check(
        checks,
        "phase1_missing_data_fails_closed",
        pre_fault.get("ready") is False and pre_fault.get("status") == "failed",
        evidence=pre_fault.get("failures"),
    )
    _assert_check(
        checks,
        "phase1_missing_data_produces_no_signal_or_position",
        not engine.signals_path.exists()
        and not engine.fills_path.exists()
        and all(
            not ((engine.state.get("modes") or {}).get(market) or {}).get("positions")
            for market in markets
        ),
    )
    _atomic_json(run_dir / "phase1_before_repair.json", pre_fault)

    # Phase 1B: source events arrive and atomically replace the stale state.
    # The readiness gate sees all registered sources, exact-session eligibility,
    # warm model/executor receipts, matching engine/Discord revision, and no
    # materialized runtime snapshot.
    pre_ready_at = _at(session_date, 8, 59, 30)
    _atomic_json(source_state_path, _source_matrix(session_date, ready=True))
    ready_source_sha = _sha256(source_state_path)
    public_receipt = _ready_public_receipt(session_date)
    event_receipt = _event_receipt(
        session_date,
        ready=True,
        updated_at=_at(session_date, 8, 59, 29),
    )
    model_receipt = _model_receipt(specs, session_date, ready=True)
    simulation_receipt = _simulation_receipt(session_date, ready=True)
    eligibility_coverage = {
        spec.market: {
            "status": "ok",
            "trading_date": session_date.isoformat(),
            "coverage_complete": True,
        }
        for spec in specs
    }
    engine.update_readiness(
        specs,
        now=_at(session_date, 8, 59, 29),
        current_eligibility_coverage=eligibility_coverage,
    )
    ready_sync = load_service_sync(engine.state_dir) or {}
    ready_discord = _discord_receipt(
        ready_sync,
        specs,
        updated_at=_at(session_date, 8, 59, 29),
    )
    pre_ready = evaluate_readiness(
        observed=pre_ready_at,
        strict_after=strict_after,
        market_names=markets,
        public_receipt=public_receipt,
        model_receipt=model_receipt,
        simulation_receipt=simulation_receipt,
        service_states=_service_states(),
        event_receipt=event_receipt,
        engine_status_receipt=json.loads(
            engine.status_path.read_text(encoding="utf-8")
        ),
        engine_sync_receipt=ready_sync,
        discord_status_receipt=ready_discord,
    )
    _assert_check(
        checks,
        "phase1_atomic_source_repair_changed_revision",
        stale_source_sha != ready_source_sha
        and not list(sandbox.glob("**/*.tmp.*")),
        evidence={"before_sha256": stale_source_sha, "after_sha256": ready_source_sha},
    )
    _assert_check(
        checks,
        "phase1_all_registered_sources_repaired",
        event_receipt["registered_dataset_count"] == len(DEFAULT_DATASETS)
        and event_receipt["observed_dataset_count"] == len(DEFAULT_DATASETS)
        and event_receipt["unapplied_event_count"] == 0,
        evidence={
            "registered": event_receipt["registered_dataset_count"],
            "observed": event_receipt["observed_dataset_count"],
            "unapplied": event_receipt["unapplied_event_count"],
        },
    )
    _assert_check(
        checks,
        "phase1_preopen_gate_ready_after_repair",
        pre_ready.get("ready") is True and pre_ready.get("status") == "ready",
        evidence=pre_ready.get("failures"),
    )
    _assert_check(
        checks,
        "phase1_runtime_does_not_materialize_snapshot",
        (public_receipt.get("acceptance") or {}).get(
            "runtime_materialized_snapshot"
        )
        is False
        and (public_receipt.get("live_runtime") or {}).get(
            "packed_release_required_for_opening"
        )
        is False,
    )
    _atomic_json(run_dir / "phase1_after_repair.json", pre_ready)

    model_shadow = (
        _run_real_model_inference_shadow(specs, session=session_date)
        if real_model_inference
        else {
            "status": "not_requested",
            "external_opening_delivery": "deterministic_fixture",
            "model_compute": "contract_fixture_only",
            "write_performed": False,
            "markets": {},
        }
    )
    if real_model_inference:
        _assert_check(
            checks,
            "phase1_real_model_cold_and_hot_shadow_ready",
            model_shadow.get("status") == "ready"
            and set(model_shadow.get("markets") or {}) == set(EXPECTED_MARKETS),
            evidence=model_shadow,
        )
    _atomic_json(run_dir / "phase1_model_shadow.json", model_shadow)

    # Phase 2A: 09:00 arrives with no same-session opening observation.  The
    # producer has not published a signal pointer, so the executor cannot act.
    before_open_at = _at(session_date, 9, 0, 0, 100_000)
    missing_pointers = {
        spec.market: _latest_signal(spec, before_open_at) for spec in specs
    }
    _assert_check(
        checks,
        "phase2_no_opening_observation_means_no_signal_pointer",
        all(value is None for value in missing_pointers.values()),
        evidence={market: value is None for market, value in missing_pointers.items()},
    )
    _assert_check(
        checks,
        "phase2_no_opening_observation_means_no_fill",
        not engine.fills_path.exists(),
    )

    # Phase 2B: the first opening observation unlocks atomic signal publication.
    # Merely publishing the pointers does not mutate the paper ledger; live
    # execution uses only a strictly later best Ask/Bid after 09:00.
    opening_at = _at(session_date, 9, 0, 0, 300_000)
    signal_at = _at(session_date, 9, 0, 0, 600_000)
    for spec in specs:
        _publish_signal_fixture(
            spec,
            session=session_date,
            signal_at=signal_at,
            target_weight=TARGET_WEIGHTS[spec.market],
        )
    pointer_results: dict[str, str] = {}
    for spec in specs:
        loaded = _latest_signal(spec, _at(session_date, 9, 0, 0, 700_000))
        if loaded is None:
            raise ColdTestFailure(f"atomic signal pointer unreadable: {spec.market}")
        pointer_results[spec.market] = "ready"
    _assert_check(
        checks,
        "phase2_signal_pointers_ready_before_execution",
        set(pointer_results.values()) == {"ready"} and not engine.fills_path.exists(),
        evidence=pointer_results,
    )

    book_at = _at(session_date, 9, 0, 0, 700_000)
    commit_at = _at(session_date, 9, 0, 0, 800_000)
    executable_book = {"2330": _best_book_quote(session_date, book_at)}
    registered_results: dict[str, str] = {}
    for spec in specs:
        loaded = _latest_signal(spec, commit_at)
        if loaded is None:
            raise ColdTestFailure(f"published signal disappeared: {spec.market}")
        summary, rows = loaded
        registered_results[spec.market] = engine.register_signal(
            spec=spec,
            summary=summary,
            signal_rows=rows,
            quotes=executable_book,
            eligibility=_eligibility(session_date),
            eligibility_coverage=eligibility_coverage[spec.market],
            now=commit_at,
        )

    mode_evidence: dict[str, Any] = {}
    for spec in specs:
        mode = (engine.state.get("modes") or {}).get(spec.market) or {}
        positions = list((mode.get("positions") or {}).values())
        position = positions[0] if positions else {}
        expected_price = 1_000.0 if position.get("side") == "long" else 998.0
        mode_evidence[spec.market] = {
            "result": registered_results.get(spec.market),
            "side": position.get("side"),
            "entry_price": position.get("entry_price"),
            "expected_entry_price": expected_price,
            "filled_shares": position.get("filled_shares"),
            "requested_shares": mode.get("entry_requested_shares"),
            "entry_unfilled_shares": mode.get("entry_unfilled_shares"),
            "paper_market_fill": position.get("paper_market_fill"),
            "counterfactual_open_price_fill": position.get(
                "counterfactual_open_price_fill"
            ),
            "displayed_best_volume_shares": 1_000,
            "entry_price_source": position.get("entry_price_source"),
            "synthetic_fill": position.get("entry_fill_is_synthetic"),
            "entry_completed_at": mode.get("entry_completed_at"),
        }
    _assert_check(
        checks,
        "phase2_all_three_modes_register_once",
        set(registered_results) == set(EXPECTED_MARKETS)
        and set(registered_results.values()) == {"registered"},
        evidence=registered_results,
    )
    _assert_check(
        checks,
        "phase2_live_0900_uses_causally_later_best_quote",
        all(
            row["entry_price"] == row["expected_entry_price"]
            and int(row["filled_shares"] or 0) == 1_000
            and int(row["requested_shares"] or 0) == 1_000
            and int(row["entry_unfilled_shares"] or 0) == 0
            and row["paper_market_fill"] is False
            and row["counterfactual_open_price_fill"] is False
            and row["synthetic_fill"] is False
            for row in mode_evidence.values()
        ),
        evidence=mode_evidence,
    )

    post_sync = load_service_sync(engine.state_dir) or {}
    post_event_receipt = dict(event_receipt)
    post_event_receipt["updated_at_taipei"] = _at(
        session_date, 9, 0, 14
    ).isoformat(timespec="milliseconds")
    post_discord = _discord_receipt(
        post_sync,
        specs,
        updated_at=_at(session_date, 9, 0, 14),
    )
    post_open = evaluate_readiness(
        observed=_at(session_date, 9, 0, 15),
        strict_after=strict_after,
        market_names=markets,
        public_receipt=public_receipt,
        model_receipt=model_receipt,
        simulation_receipt=simulation_receipt,
        service_states=_service_states(),
        event_receipt=post_event_receipt,
        engine_status_receipt=json.loads(
            engine.status_path.read_text(encoding="utf-8")
        ),
        engine_sync_receipt=post_sync,
        discord_status_receipt=post_discord,
    )
    _assert_check(
        checks,
        "phase2_post_open_gate_accepts_every_committed_mode_within_slo",
        post_open.get("ready") is True
        and bool((post_open.get("opening_execution") or {}).get("ready")),
        evidence=post_open.get("opening_execution"),
    )
    _atomic_json(
        run_dir / "phase2_before_opening.json",
        {
            "observed_at": before_open_at.isoformat(timespec="microseconds"),
            "opening_observation_present": False,
            "signal_pointer_present": {
                market: value is not None for market, value in missing_pointers.items()
            },
            "fill_ledger_present": False,
        },
    )
    _atomic_json(
        run_dir / "phase2_after_signal.json",
        {
            "opening_observed_at": opening_at.isoformat(timespec="microseconds"),
            "signal_ready_at": signal_at.isoformat(timespec="microseconds"),
            "signal_pointer_results": pointer_results,
            "causal_best_quote_observed_at": book_at.isoformat(timespec="microseconds"),
            "registered_results": registered_results,
            "modes": mode_evidence,
            "post_open_gate": post_open,
        },
    )

    # Phase 2C: replay is isolated from live state and remains fixed to the
    # official-open-at-09:01 counterfactual contract.
    replay_engine = TwDayTradeSimulationEngine(sandbox / "replay_engine")
    replay_at = _at(session_date, 9, 1)
    replay_results: dict[str, str] = {}
    replay_evidence: dict[str, Any] = {}
    for spec in specs:
        replay_spec = replace(
            spec,
            entry_fill_policy=ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901,
            entry_price_offset_ticks=0,
        )
        loaded = _latest_signal(spec, commit_at)
        if loaded is None:
            raise ColdTestFailure(f"published signal disappeared: {spec.market}")
        source_summary, rows = loaded
        replay_summary = {
            **source_summary,
            "signal_id": f"replay-{session_date.isoformat()}-{spec.market}",
            "simulation_replay": True,
            "replay_basis": "official_session_open_at_09_01_to_official_close",
            "entry_fill_contract": (
                "retrospective_official_session_open_at_09_01_counterfactual"
            ),
        }
        replay_results[spec.market] = replay_engine.register_signal(
            spec=replay_spec,
            summary=replay_summary,
            signal_rows=rows,
            quotes={"2330": _opening_quote(session_date, replay_at)},
            eligibility=_eligibility(session_date),
            eligibility_coverage=eligibility_coverage[spec.market],
            now=replay_at,
            counterfactual_open_replay=True,
        )
        replay_mode = (replay_engine.state.get("modes") or {}).get(spec.market) or {}
        replay_position = next(iter((replay_mode.get("positions") or {}).values()), {})
        replay_evidence[spec.market] = {
            "result": replay_results[spec.market],
            "entry_price": replay_position.get("entry_price"),
            "entry_at": replay_position.get("entry_at"),
            "entry_fill_policy": replay_position.get("entry_fill_policy"),
            "counterfactual_open_price_fill": replay_position.get(
                "counterfactual_open_price_fill"
            ),
            "synthetic_fallback_fill": replay_position.get(
                "synthetic_fallback_fill"
            ),
        }
    _assert_check(
        checks,
        "phase2_historical_replay_only_uses_0901_official_open",
        set(replay_results.values()) == {"registered"}
        and all(
            row["entry_price"] == 999.0
            and row["entry_at"] == replay_at.isoformat(timespec="seconds")
            and row["entry_fill_policy"]
            == ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901
            and row["counterfactual_open_price_fill"] is True
            and row["synthetic_fallback_fill"] is False
            for row in replay_evidence.values()
        ),
        evidence=replay_evidence,
    )
    _atomic_json(
        run_dir / "phase2_historical_replay.json",
        {"recorded_at": replay_at.isoformat(), "modes": replay_evidence},
    )

    # Phase 3: advance the production paper engine through the full session and
    # restart it twice.  The first restart proves active-position recovery; the
    # second proves a completed session cannot duplicate exit fills.
    intraday_at = _at(session_date, 10, 0)
    neutral_quote = {
        "2330": _lifecycle_quote(
            intraday_at,
            bid=998.0,
            ask=1_000.0,
            last=999.0,
            bid_volume=1.0,
            ask_volume=1.0,
            minute_volume_lots=2.0,
        )
    }
    engine.process_quotes(quotes=neutral_quote, now=intraday_at)
    active_before_restart = {
        market: sum(
            int(position.get("signed_shares") or 0) != 0
            for position in (
                ((engine.state.get("modes") or {}).get(market) or {}).get(
                    "positions", {}
                )
                or {}
            ).values()
        )
        for market in markets
    }
    fills_before_restart = engine.fills_path.read_text(encoding="utf-8").splitlines()
    first_run_id = str((load_service_sync(engine.state_dir) or {}).get("engine_run_id"))
    engine = TwDayTradeSimulationEngine(sandbox / "paper_engine")
    engine.update_readiness(
        specs,
        now=_at(session_date, 10, 0, 1),
        current_eligibility_coverage=eligibility_coverage,
    )
    restarted_run_id = str(
        (load_service_sync(engine.state_dir) or {}).get("engine_run_id")
    )
    _assert_check(
        checks,
        "phase3_restart_recovers_every_open_position",
        all(count == 1 for count in active_before_restart.values())
        and all(
            any(
                int(position.get("signed_shares") or 0) != 0
                for position in (
                    ((engine.state.get("modes") or {}).get(market) or {}).get(
                        "positions", {}
                    )
                    or {}
                ).values()
            )
            for market in markets
        )
        and restarted_run_id != first_run_id
        and engine.fills_path.read_text(encoding="utf-8").splitlines()
        == fills_before_restart,
        evidence=active_before_restart,
    )

    exit_limit_at = _at(session_date, 13, 20)
    passive_quote = {
        "2330": _lifecycle_quote(
            exit_limit_at,
            bid=998.0,
            ask=1_000.0,
            last=999.0,
            bid_volume=1.0,
            ask_volume=1.0,
            minute_volume_lots=2.0,
        )
    }
    engine.process_quotes(quotes=passive_quote, now=exit_limit_at)
    _assert_check(
        checks,
        "phase3_1320_passive_orders_do_not_cross_the_book",
        all(
            int(position.get("signed_shares") or 0) != 0
            and position.get("eod_limit_order_status") == "working"
            for market in markets
            for position in (
                ((engine.state.get("modes") or {}).get(market) or {}).get(
                    "positions", {}
                )
                or {}
            ).values()
        ),
    )

    force_exit_at = _at(session_date, 13, 24)
    force_quote = {
        "2330": _lifecycle_quote(
            force_exit_at,
            bid=998.0,
            ask=None,
            last=999.0,
            bid_volume=1.0,
            ask_volume=0.0,
            minute_volume_lots=2.0,
        )
    }
    engine.process_quotes(quotes=force_quote, now=force_exit_at)
    force_state = {
        market: int(
            next(
                iter(
                    (
                        ((engine.state.get("modes") or {}).get(market) or {}).get(
                            "positions", {}
                        )
                        or {}
                    ).values()
                )
            ).get("signed_shares")
            or 0
        )
        for market in markets
    }
    _assert_check(
        checks,
        "phase3_1324_uses_best_bid_and_preserves_unquoted_short",
        force_state["tw_day_trade_100m"] == 0
        and force_state["tw_day_trade_multi_basis_projection_l1_gelu"] == 0
        and force_state["tw_day_trade_multi_basis"] == -1_000,
        evidence=force_state,
    )

    close_at = _at(session_date, 13, 30)
    close_quote = {
        "2330": _lifecycle_quote(
            close_at,
            bid=None,
            ask=None,
            last=1_002.0,
            bid_volume=0.0,
            ask_volume=0.0,
            minute_volume_lots=0.0,
        )
    }
    engine.process_quotes(quotes=close_quote, now=close_at)
    fills_after_close = [
        json.loads(line)
        for line in engine.fills_path.read_text(encoding="utf-8").splitlines()
    ]
    terminal_fills = [
        row
        for row in fills_after_close
        if row.get("purpose") == "13_30_terminal_ledger_flatten"
    ]
    force_fills = [
        row
        for row in fills_after_close
        if row.get("purpose") == "13_24_market_force_exit"
    ]
    final_modes = engine.state.get("modes") or {}
    all_flat = all(
        not any(
            int(position.get("signed_shares") or 0) != 0
            for position in ((final_modes.get(market) or {}).get("positions") or {}).values()
        )
        and (final_modes.get(market) or {}).get("engine_status")
        == "session_flat_after_exit"
        for market in markets
    )
    _assert_check(
        checks,
        "phase3_1330_all_modes_are_terminal_and_flat",
        all_flat
        and len(force_fills) == 2
        and all(float(row.get("price") or 0.0) == 998.0 for row in force_fills)
        and len(terminal_fills) == 1
        and terminal_fills[0].get("market") == "tw_day_trade_multi_basis"
        and terminal_fills[0].get("fill_contract")
        == "simulation_terminal_ledger_not_exchange_fill",
        evidence={
            "force_exit_fill_count": len(force_fills),
            "terminal_fill_count": len(terminal_fills),
            "all_flat": all_flat,
        },
    )

    fill_lines_at_close = engine.fills_path.read_text(encoding="utf-8").splitlines()
    completed_run_id = str(
        (load_service_sync(engine.state_dir) or {}).get("engine_run_id")
    )
    engine = TwDayTradeSimulationEngine(sandbox / "paper_engine")
    engine.process_quotes(
        quotes=close_quote,
        now=_at(session_date, 13, 31),
    )
    _assert_check(
        checks,
        "phase3_completed_restart_is_idempotent",
        str((load_service_sync(engine.state_dir) or {}).get("engine_run_id"))
        != completed_run_id
        and engine.fills_path.read_text(encoding="utf-8").splitlines()
        == fill_lines_at_close
        and all(
            not any(
                int(position.get("signed_shares") or 0) != 0
                for position in (
                    ((engine.state.get("modes") or {}).get(market) or {}).get(
                        "positions", {}
                    )
                    or {}
                ).values()
            )
            for market in markets
        ),
    )
    mark_rows = [
        json.loads(line)
        for line in engine.marks_path.read_text(encoding="utf-8").splitlines()
    ]
    mark_minutes = {
        str(row.get("minute")) for row in mark_rows if row.get("minute")
    }
    required_mark_minutes = {
        value.isoformat(timespec="minutes")
        for value in (intraday_at, exit_limit_at, force_exit_at, close_at)
    }
    _assert_check(
        checks,
        "phase3_intraday_and_close_marks_are_durable",
        required_mark_minutes.issubset(mark_minutes),
        evidence={
            "required": sorted(required_mark_minutes),
            "observed": sorted(mark_minutes),
        },
    )
    phase3 = {
        "intraday_mark_at": intraday_at.isoformat(timespec="seconds"),
        "active_positions_before_restart": active_before_restart,
        "exit_limit_at": exit_limit_at.isoformat(timespec="seconds"),
        "force_exit_at": force_exit_at.isoformat(timespec="seconds"),
        "force_exit_signed_shares": force_state,
        "close_at": close_at.isoformat(timespec="seconds"),
        "all_modes_flat": all_flat,
        "force_exit_fill_count": len(force_fills),
        "terminal_ledger_fill_count": len(terminal_fills),
        "terminal_fill_contract": (
            terminal_fills[0].get("fill_contract") if terminal_fills else None
        ),
        "restart_count": 2,
        "duplicate_fill_count_after_completed_restart": (
            len(engine.fills_path.read_text(encoding="utf-8").splitlines())
            - len(fill_lines_at_close)
        ),
        "durable_mark_minutes": sorted(mark_minutes),
    }
    _atomic_json(run_dir / "phase3_intraday_postclose.json", phase3)

    elapsed = time.perf_counter() - started
    report = {
        "schema_version": 2,
        "status": "ok",
        "run_id": resolved_run_id,
        "session_date": session_date.isoformat(),
        "started_at_taipei": started_at.isoformat(timespec="milliseconds"),
        "completed_at_taipei": datetime.now(TAIPEI).isoformat(
            timespec="milliseconds"
        ),
        "elapsed_seconds": round(elapsed, 6),
        "simulation_only": True,
        "production_order_possible": False,
        "isolation": {
            "sandbox": str(sandbox),
            "live_root_modified": False,
            "live_services_modified": False,
            "production_ledger_modified": False,
            "network_access_performed": False,
            "shioaji_login_performed": False,
            "broker_order_api_called": False,
        },
        "scope": {
            "production_readiness_gate": True,
            "production_atomic_signal_pointer_consumer": True,
            "production_integer_paper_execution_engine": True,
            "production_0900_live_causal_best_quote_contract": True,
            "historical_0901_official_open_replay_contract": True,
            "production_intraday_mark_and_exit_state_machine": True,
            "production_restart_recovery_and_idempotency": True,
            "external_publication_delivery": "deterministic_fixture",
            "external_official_open_delivery": "deterministic_fixture",
            "model_inference": (
                "real_config_checkpoint_panel_and_gpu"
                if real_model_inference
                else "contract_fixture_with_real_active_config_and_checkpoint_presence"
            ),
        },
        "active_markets": list(markets),
        "registered_public_dataset_count": len(DEFAULT_DATASETS),
        "phases": {
            "preopen_missing": {
                "status": pre_fault.get("status"),
                "ready": pre_fault.get("ready"),
                "failures": pre_fault.get("failures"),
            },
            "preopen_repaired": {
                "status": pre_ready.get("status"),
                "ready": pre_ready.get("ready"),
                "source_revision_before": stale_source_sha,
                "source_revision_after": ready_source_sha,
            },
            "model_shadow": model_shadow,
            "opening_missing": {
                "signal_pointers_absent": all(
                    value is None for value in missing_pointers.values()
                ),
                "fill_ledger_absent": True,
            },
            "signal_ready_for_execution": pointer_results,
            "opening_executed": {
                "results": registered_results,
                "modes": mode_evidence,
                "post_open_gate_ready": post_open.get("ready"),
                "opening_execution": post_open.get("opening_execution"),
            },
            "historical_replay": {
                "recorded_at": replay_at.isoformat(timespec="seconds"),
                "results": replay_results,
                "modes": replay_evidence,
            },
            "intraday_postclose": phase3,
        },
        "checks": checks,
        "artifacts": {
            "phase1_before_repair": str(run_dir / "phase1_before_repair.json"),
            "phase1_after_repair": str(run_dir / "phase1_after_repair.json"),
            "phase1_model_shadow": str(run_dir / "phase1_model_shadow.json"),
            "phase2_before_opening": str(run_dir / "phase2_before_opening.json"),
            "phase2_after_signal": str(run_dir / "phase2_after_signal.json"),
            "phase2_historical_replay": str(
                run_dir / "phase2_historical_replay.json"
            ),
            "phase3_intraday_postclose": str(
                run_dir / "phase3_intraday_postclose.json"
            ),
            "paper_engine": str(engine.state_dir),
        },
    }
    report_path = run_dir / "report.json"
    _atomic_json(report_path, report)
    report_sha256 = _sha256(report_path)
    (run_dir / "report.sha256").write_text(
        f"{report_sha256}  report.json\n", encoding="utf-8"
    )
    _atomic_json(
        root / "latest.json",
        {
            "schema_version": 1,
            "status": "ok",
            "run_id": resolved_run_id,
            "session_date": session_date.isoformat(),
            "report_path": str(report_path),
            "report_sha256": report_sha256,
            "completed_at_taipei": report["completed_at_taipei"],
        },
    )
    _LATEST_SIGNAL_CACHE.clear()
    _LEGACY_SIGNAL_SCAN_CACHE.clear()
    return report


def main() -> None:
    args = parse_args()
    session_date = args.session_date
    if session_date is None:
        session_date = (
            _default_future_session_date()
            if args.real_model_inference
            else _default_session_date()
        )
    report = run_two_phase_cold_test(
        session_date=session_date,
        output_root=args.output_root,
        run_id=args.run_id,
        real_model_inference=bool(args.real_model_inference),
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

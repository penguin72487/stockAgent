#!/usr/bin/env python3
"""Repair source-blocked, never-filled paper sessions by scoped ledger append.

This is not a strategy switch or a directory promotion. Original journals remain
append-only, other markets retain their state, and canonical 09:01 minute replay
owns every new simulated fill. Prepare validates an isolated candidate first.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import asdict, replace
from datetime import date, datetime, time, timedelta
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace
import uuid
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import polars as pl

from scripts.rebuild_tw_day_trade_open_price_replay import (
    DEFAULT_MINUTE_DATA_ROOTS, DEFAULT_TPEX_DAILY_OHLCV_PATH,
    DEFAULT_TWSE_DAILY_OHLCV_PATH, _atomic_json, _canonicalize_signal_rows_for_replay,
    _close_quotes, _entry_quotes, _historical_minute_valuation_quotes,
    _latest_valid_signal, _load_price_limits, _load_retained_historical_entry_books,
    _materialize_exact_session_rules, _minute_bar_rows,
    _official_open_map, _replay_historical_intraday, _sha256,
)
from scripts.run_tw_day_trade_simulation import _acquire_engine_lock, _mode_specs, _rule_data_dir
from stockagent.live.quote_provider import (
    fetch_shioaji_current_stock_minute_bars, load_local_stock_0901_vwaps,
)
from stockagent.live.day_trade_maintenance import control_lease
from stockagent.live.market_status import short_file_fingerprint
from stockagent.live.paper_journal import journal_write_transaction, open_paper_journal, rebind_cloned_journals
from stockagent.live.tw_day_trade_simulation import (
    ENTRY_FILL_POLICY_0901_MINUTE_PRICE, REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
    SIMULATION_SCHEMA_VERSION, TwDayTradeSimulationEngine, _append_jsonl_many,
    PUBLISHED_REVISION_MINUTE_REPLAY_CONTRACT,
    load_live_eligibility, whole_lot_target_shares,
)
from stockagent.live.opening_signal_revision import (
    SIGNAL_UPDATE_PROGRESSIVE, revision_admission, revision_fields,
)

TAIPEI = ZoneInfo("Asia/Taipei")
SERVICE = "stockagent-tw-day-trade-simulation.service"
CONTRACT = "never_filled_missing_open_append_only_minute_repair_v1"
ORIGINAL_SIGNAL_CONTRACT = "never_filled_original_signal_append_only_minute_repair_v2"
MISSING_SESSION_CONTRACT = "missing_completed_session_append_only_minute_repair_v1"
PUBLISHED_SESSION_CONTRACT = "missing_session_published_revisions_append_only_minute_repair_v1"
ECONOMIC_KEYS = (
    "initial_capital_twd", "cumulative_realized_net_pnl_twd",
    "cumulative_commission_rebate_accrued_twd", "total_equity_twd",
    "session_sizing_nav_twd", "session_date", "signal_id", "processed_signal_ids",
    "positions", "pending_entry_orders", "pending_reduction_orders",
    "cumulative_corporate_action_net_twd", "cumulative_carry_cost_twd",
)
POSITION_ECONOMIC_KEYS = (
    "symbol", "side", "target_weight", "requested_shares",
    "filled_shares", "signed_shares", "entry_price", "closed_shares",
    "entry_fee_twd", "realized_gross_pnl_twd", "realized_exit_fee_twd",
    "realized_net_pnl_twd", "last_complete_net_pnl_twd", "total_net_pnl_twd",
    "last_mark_price", "exit_at", "exit_price",
    "take_profit_price", "stop_trigger_price", "entry_fill_policy",
)
REPLAY_SUMMARY_FIELDS = frozenset("""
    signal_id market execution_mode asof_date generated_at signal_started_at
    signal_ready_at artifact_published_at execution_pointer_written_at
    execution_pointer_observed_at checkpoint_fingerprint config_fingerprint
    feature_cutoff_date symbol_count target_risk summary_path weights_path
    positions_markdown_path day_trade_signal_update_policy
    day_trade_signal_update_policy_fingerprint opening_signal_revision
    day_trade_opening_reference_policy day_trade_opening_reference_policy_fingerprint
    opening_coverage_gate_basis opening_input_required_count opening_input_resolved_count
    opening_missing_symbols opening_model_inputs_ready opening_no_print_active_count
    opening_price_active_count opening_price_active_symbol_count opening_price_required_count
    opening_provisional_reference_active_count opening_source_complete
    live_session_open_feature_applied live_session_open_reference_applied
    opening_entry_clock_contract initial_opening_signal_id initial_opening_publication_proof
    entry_fill_contract entry_liquidity_assumption historical_minute_valuation
    replay_basis replay_open_evidence_contract replay_source simulation_only simulation_replay
    production_order_possible published_revision_replay
    missed_opening_original_policy_fingerprint missed_opening_original_signal_revision
    missed_opening_original_signal_update_policy missed_opening_recovery
""".split())


def _compact_replay_summary(summary: dict) -> dict:
    # Full originals stay byte-pinned. The executor does not consume the MB-sized
    # diagnostics/panel inventories; do not retain 163 copies of them in RAM.
    return {key: value for key, value in summary.items() if key in REPLAY_SUMMARY_FIELDS}


def _object(path: Path) -> dict:
    from stockagent.live.paper_mode_snapshot import read_paper_state
    value = read_paper_state(path)
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                    ensure_ascii=False).encode()).hexdigest()


def _spec_digest(spec) -> str:
    return _digest(json.dumps(asdict(spec), sort_keys=True, default=str))


def _anchor(mode: dict) -> dict:
    return {key: deepcopy(mode.get(key)) for key in ECONOMIC_KEYS}


def _outcome(mode: dict) -> dict:
    # Order/commit IDs are new audit identities on each real append. Compare
    # economic quantities/prices instead of requiring a reused candidate ID.
    result = {key: mode.get(key) for key in ECONOMIC_KEYS
              if key not in {"positions", "pending_entry_orders", "pending_reduction_orders"}}
    result["positions"] = {key: {name: value.get(name) for name in POSITION_ECONOMIC_KEYS}
                           for key, value in mode.get("positions", {}).items()}
    for name in ("pending_entry_orders", "pending_reduction_orders"):
        result[name] = {key: {field: value.get(field) for field in
                       ("symbol", "side", "requested_shares", "remaining_shares", "status")}
                       for key,value in (mode.get(name) or {}).items()}
    return deepcopy(result)


def _inventory_anchor(mode: dict) -> dict:
    """Prove a mark-only bridge has no unseen entry, exit or cash change."""
    keys = ("initial_capital_twd", "cumulative_realized_net_pnl_twd",
            "cumulative_commission_rebate_accrued_twd", "cumulative_carry_cost_twd",
            "cumulative_corporate_action_net_twd", "session_date", "signal_id")
    result = {key: mode.get(key) for key in keys}
    fields = set(POSITION_ECONOMIC_KEYS) - {"last_complete_net_pnl_twd", "total_net_pnl_twd", "last_mark_price"}
    result["positions"] = {key: {field: value.get(field) for field in fields}
                           for key, value in mode.get("positions", {}).items()}
    return result


def _handoff_gap_minutes(path: Path, manifest: dict) -> list[datetime]:
    cutoff = datetime.fromisoformat(manifest["current_capture"]["completed_through"])
    applied = datetime.fromisoformat(manifest["applied_at"])
    first_live = None
    with open_paper_journal(path) as handle:
        handle.seek(manifest["journal_prefix_bytes"]["marks.jsonl"])
        for line in handle:
            if not line.endswith(b"\n"):
                break
            row = json.loads(line)
            if row.get("market") != manifest["market"] or row.get("session_date") != cutoff.date().isoformat():
                continue
            recorded = datetime.fromisoformat(row["recorded_at"])
            if recorded >= applied:
                first_live = datetime.fromisoformat(row["minute"])
                break
    if first_live is None:
        raise RuntimeError("no resumed live mark yet; wait for the canonical service")
    return [cutoff + timedelta(minutes=offset) for offset in
            range(1, int((first_live-cutoff).total_seconds()//60))]


def _require_empty_target(mode: dict, *, preserve_original_signal: bool = False) -> None:
    # Even closed position history is disallowed: this path must never become
    # an implicit same-day repeated-entry or fill-replacement mechanism.
    if mode.get("positions") or mode.get("ledger_state_divergence"):
        raise RuntimeError("repair target has position history or ledger divergence")
    requested = int(mode.get("entry_requested_shares") or 0)
    if int(mode.get("entry_filled_shares") or 0) or (requested and not preserve_original_signal):
        raise RuntimeError("repair target already requested/filled inventory")
    if mode.get("pending_entry_orders") or mode.get("pending_reduction_orders"):
        raise RuntimeError("repair target has pending orders")
    if mode.get("engine_status") != "flat_no_executable_signal":
        raise RuntimeError("only source-blocked consumed no-order sessions are repairable")
    if preserve_original_signal:
        reasons = mode.get("signal_reason_counts") or {}
        if not (
            requested > 0 and int(mode.get("entry_unfilled_shares") or 0) == requested
            and mode.get("simulation_replay") is True
            and mode.get("counterfactual_open_replay") is True
            and mode.get("entry_fill_contract") == REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE
            and int(reasons.get("quote_not_after_signal") or 0) > 0
            and not (set(reasons) - {"quote_not_after_signal", "below_one_board_lot", "zero_target_weight"})
        ):
            raise RuntimeError("original-signal repair requires a never-filled minute-replay clock failure")


def _has_live_fill(state_dir: Path, market: str, session: str) -> bool:
    # The canonical guard only reads fills_path. Do not construct a live engine
    # during prepare: its constructor can recover/migrate persisted state.
    probe = SimpleNamespace(fills_path=state_dir / "fills.jsonl")
    return TwDayTradeSimulationEngine._session_has_fill(probe, market, session)


def _require_missing_session_target(mode: dict, day: date) -> None:
    """Append a new session, never rearm or replace a previously traded one."""
    previous = str(mode.get("session_date") or "")
    if not previous or date.fromisoformat(previous) >= day:
        raise RuntimeError("missing-session append requires a strictly later session")
    if mode.get("ledger_state_divergence") or any(
        int(position.get("signed_shares") or 0) for position in (mode.get("positions") or {}).values()
    ):
        raise RuntimeError("missing-session append requires flat, reconciled prior inventory")
    if any(not (order.get("status") == "filled" and type(order.get("remaining_shares")) is int
                and order["remaining_shares"] == 0 and order.get("sizing_pending") is False)
           for name in ("pending_entry_orders", "pending_reduction_orders")
           for order in (mode.get(name) or {}).values()):
        raise RuntimeError("missing-session append cannot bypass active or unknown pending orders")


def _missing_session_proof(spec, day: date, summary_path: Path) -> dict:
    summary = _object(summary_path)
    weights_path = summary_path.with_name("target_weights.parquet")
    proof = summary.get("counterfactual_artifact_proofs") or {}
    if (summary.get("counterfactual_signal_regeneration") is not True
            or summary.get("simulation_only") is not True
            or summary.get("production_order_possible") is not False
            or summary.get("live_session_open_feature_applied") is not True
            or str(summary.get("replay_effective_signal_at") or "")[:10] != str(day)
            or summary.get("config_fingerprint") != short_file_fingerprint(Path(spec.config_path))
            or summary.get("checkpoint_fingerprint") != short_file_fingerprint(Path(spec.checkpoint_path))
            or proof.get("weights_sha256") != _sha256(weights_path)):
        raise RuntimeError("missing-session append requires a pinned selected-model counterfactual")
    return {"session_date": str(day), "signal_id": summary["signal_id"],
            "summary_path": str(summary_path.resolve()), "summary_sha256": _sha256(summary_path),
            "weights_path": str(weights_path.resolve()), "weights_sha256": _sha256(weights_path),
            "config_fingerprint": summary["config_fingerprint"],
            "checkpoint_fingerprint": summary["checkpoint_fingerprint"],
            "signal_started_at": summary.get("signal_started_at") or summary.get("generated_at"),
            "generated_at": summary.get("generated_at"), "original_weights_preserved": False}


def _spec(market: str, markets_dir: Path, published_revisions: bool = False):
    specs, configs, errors = _mode_specs(markets_dir)
    if errors:
        raise RuntimeError(f"mode configuration errors: {errors}")
    spec = next(value for value in specs if value.market == market)
    return replace(spec, entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
                   entry_price_offset_ticks=0,
                   historical_progressive_revision_replay=published_revisions), configs[market]


def _published_revision_inventory(spec, day: date) -> dict:
    """Pin every intraday publication, including independently restarted roots.

    Original roots are audited separately. The repair's new lineage is explicit
    and never edits, relabels or claims registration of the source artifacts.
    """
    from stockagent.live.market_status import short_file_fingerprint
    paths = sorted(spec.live_output_dir.glob(f"{day}*/**/summary.json"))
    versions, excluded, seen, lineages = [], [], set(), {}
    for path in paths:
        before = _sha256(path)
        summary = _object(path)
        if _sha256(path) != before:
            raise RuntimeError(f"source changed during scan: {path}")
        revision = summary.get("opening_signal_revision")
        if not revision:
            # Completed-close caches are not intraday decisions. Do not quietly
            # classify malformed opening artifacts as an off-hours cache.
            if summary.get("live_session_open_reference_applied") or summary.get("live_session_open_feature_applied"):
                raise RuntimeError(f"unclassified opening artifact: {path}")
            excluded.append({"path": str(path.resolve()), "sha256": before})
            continue
        stamp = datetime.fromisoformat(str(summary["execution_pointer_written_at"]))
        artifact = datetime.fromisoformat(str(summary["artifact_published_at"]))
        generated = datetime.fromisoformat(str(summary["generated_at"]))
        started = datetime.fromisoformat(str(summary["signal_started_at"]))
        if (stamp.utcoffset() != timedelta(hours=8) or stamp.date() != day
                or not time(9) <= stamp.time() < time(13, 20)
                or generated > artifact
                or started > artifact
                or started >= generated + timedelta(seconds=1)):
            raise RuntimeError(f"invalid publication clocks: {path}")
        if (summary.get("market") != str(spec.signal_market or spec.market)
                or summary.get("execution_mode") != "tw_day_trade"
                or summary.get("asof_date", "")[:10] != str(day)
                or summary.get("config_fingerprint") != short_file_fingerprint(Path(spec.config_path))
                or summary.get("checkpoint_fingerprint") != short_file_fingerprint(Path(spec.checkpoint_path))):
            raise RuntimeError(f"published strategy identity differs: {path}")
        signal_id = str(summary["signal_id"])
        weights = path.with_name("target_weights.parquet")
        if signal_id in seen or not weights.is_file():
            raise RuntimeError(f"duplicate/incomplete publication: {path}")
        seen.add(signal_id)
        versions.append({"signal_id": signal_id, "summary_path": str(path.resolve()),
            "weights_path": str(weights.resolve()), "summary_sha256": before,
            "weights_sha256": _sha256(weights), "publication_upper_bound": max(stamp, artifact).isoformat(),
            "execution_pointer_written_at": stamp.isoformat(),
            "publication_clock_anomalies": ["artifact_after_pointer"] if artifact > stamp else [],
            "artifact_published_at": artifact.isoformat(), "signal_started_at": started.isoformat(),
            "opening_signal_revision": revision,
            **{key: summary[key] for key in ("config_fingerprint", "checkpoint_fingerprint",
                "day_trade_signal_update_policy", "day_trade_signal_update_policy_fingerprint")}})
    versions.sort(key=lambda row: (row["publication_upper_bound"], row["signal_id"]))
    previous_root = None
    for row in versions:
        summary = row
        root = row["opening_signal_revision"]["root_signal_id"]
        old = lineages.get(root, {})
        _, error = revision_admission(policy=SIGNAL_UPDATE_PROGRESSIVE, summary=summary,
                                      mode=old, session_date=str(day))
        revision = row["opening_signal_revision"]
        prior = old.get("opening_signal_revision")
        if (error or (not prior and revision["index"] != 0)
                or (prior and (revision["index"] != prior["index"] + 1
                    or revision["parent_signal_id"] != old["signal_id"]))
                or root != previous_root and root in lineages):
            raise RuntimeError(f"non-contiguous/interleaved original revision lineage: {row['signal_id']}: {error}")
        lineages[root] = {"session_date": str(day), "entry_completed_at": row["publication_upper_bound"],
            "signal_id": row["signal_id"], "opening_signal_revision": revision,
            "day_trade_signal_update_policy": SIGNAL_UPDATE_PROGRESSIVE,
            "config_fingerprint": summary["config_fingerprint"],
            "checkpoint_fingerprint": summary["checkpoint_fingerprint"]}
        previous_root = root
    if not versions or versions[0]["opening_signal_revision"]["index"] != 0:
        raise RuntimeError("no complete original published lineage")
    after = sorted(spec.live_output_dir.glob(f"{day}*/**/summary.json"))
    for changed in set(paths) ^ set(after):
        if changed in paths or _object(changed).get("opening_signal_revision"):
            raise RuntimeError("intraday source membership changed during scan")
    # Admit only the latest decision visible before a full completed interval.
    # Superseded versions remain inspectable; they are not fabricated fills.
    selected = {versions[0]["signal_id"]: datetime.combine(day, time(9, 1), tzinfo=TAIPEI).isoformat()}
    current, cursor = versions[0]["signal_id"], 0
    for offset in range(2, 260):  # last entry minute 13:19, exit priority 13:20
        end = datetime.combine(day, time(9), tzinfo=TAIPEI) + timedelta(minutes=offset)
        while cursor + 1 < len(versions) and datetime.fromisoformat(
                versions[cursor + 1]["publication_upper_bound"]) <= end - timedelta(minutes=1):
            cursor += 1
        if versions[cursor]["signal_id"] != current:
            current = versions[cursor]["signal_id"]
            selected[current] = end.isoformat()
    for row in versions:
        row["effective_entry_at"] = selected.get(row["signal_id"])
        row["disposition"] = ("completed_minute_target_revision" if row["effective_entry_at"]
                              else "retained_not_executed_superseded_or_exit_priority")
    result = {"contract": PUBLISHED_SESSION_CONTRACT, "session_date": str(day),
        "market": spec.market, "simulation_only": True, "production_order_possible": False,
        "original_weights_preserved": True, "original_publication_clocks_preserved": True,
        "first_entry_assumption": "explicit_counterfactual_09_01_not_intraminute_fill_evidence",
        "later_entry_assumption": "latest_published_target_before_full_minute_start",
        "publication_count": len(versions), "selected_revision_count": len(selected),
        "source_lineage_count": len(lineages), "versions": versions, "excluded_close_caches": excluded}
    result["plan_sha256"] = _digest({key: value for key, value in result.items() if key != "excluded_close_caches"})
    return result


def plan_published_revisions(args) -> dict:
    """Source/price inventory only; no account/journal mutation or API calls."""
    spec, _ = _spec(args.market, args.markets_dir, True)
    day = datetime.now(TAIPEI).date()
    mode = _object(args.live_dir / "state.json")["modes"][args.market]
    _require_missing_session_target(mode, day)
    if datetime.now(TAIPEI).time() < time(13, 31) or _has_live_fill(args.live_dir, args.market, str(day)):
        raise RuntimeError("published revision repair requires an unfilled completed session")
    plan = _published_revision_inventory(spec, day)
    first = plan["versions"][0]
    rows = pl.read_parquet(first["weights_path"]).to_dicts()
    opens, counts, no_trade = _official_open_map([spec], {spec.market: (
        datetime.fromisoformat(first["publication_upper_bound"]), Path(first["summary_path"]),
        Path(first["weights_path"]), _object(Path(first["summary_path"])), rows)}, day,
        twse_daily_ohlcv_path=DEFAULT_TWSE_DAILY_OHLCV_PATH,
        tpex_daily_ohlcv_path=DEFAULT_TPEX_DAILY_OHLCV_PATH)
    symbols, unavailable = set(), set()
    for version in plan["versions"]:
        for row in pl.read_parquet(version["weights_path"], columns=["symbol", "target_weight"]).to_dicts():
            symbol, weight = str(row["symbol"]), float(row["target_weight"] or 0)
            if not weight:
                continue
            if symbol not in opens:
                unavailable.add(symbol)
            elif whole_lot_target_shares(weight, float(mode["total_equity_twd"]), opens[symbol], spec.lot_size) > 0:
                symbols.add(symbol)
    roots = tuple((*getattr(args, "minute_data_root", []), *DEFAULT_MINUTE_DATA_ROOTS))
    bars, proof = _minute_bar_rows(roots, trading_date=day, symbols=symbols)
    missing = sorted(symbols - bars.keys())
    inventory = {"plan_sha256": plan["plan_sha256"], "session_date": str(day),
        "sizing_nav_twd": mode["total_equity_twd"], "official_open_source_counts": counts,
        "official_open_unavailable_symbols": sorted(unavailable),
        "official_no_trade_symbols": sorted(no_trade & unavailable), "minute_symbols": sorted(symbols),
        "missing_minute_symbols": missing, "local_source_proof": proof,
        "simulation_only": True, "production_order_possible": False, "ledger_modified": False}
    args.operation_root.mkdir(parents=True, exist_ok=True)
    _atomic_json(args.operation_root / "published_revision_plan.json", plan)
    _atomic_json(args.operation_root / "source_inventory.json", inventory)
    pl.DataFrame({"symbol": missing}, schema={"symbol": pl.String}).write_csv(args.operation_root / "missing_minute_symbols.csv")
    return {"publication_count": plan["publication_count"], "selected_revision_count": plan["selected_revision_count"],
        "source_lineage_count": plan["source_lineage_count"], "minute_symbols": len(symbols),
        "missing_minute_symbols": len(missing), "official_open_unavailable_symbols": sorted(unavailable),
        "plan_sha256": plan["plan_sha256"], "ledger_modified": False}


def _source_block_proof(spec, day: date, signal_id: str, *, preserve_original_signal: bool = False) -> dict:
    _stamp, summary_path, weights_path, summary, rows = _latest_valid_signal(
        spec, day, preferred_signal_id=signal_id,
    )
    raw = [row for row in rows if abs(float(row.get("model_weight") or 0)) > 0]
    if preserve_original_signal:
        selected = [row for row in rows if abs(float(row.get("target_weight") or 0)) > 0]
        if (str(summary.get("signal_id")) != signal_id or not selected
                or any(not (float(row.get("open_price") or 0) > 0) for row in selected)):
            raise RuntimeError(f"{day}: original executable signal identity/opening prices missing")
    else:
        if not raw or any(abs(float(row.get("target_weight") or 0)) > 0 for row in rows):
            raise RuntimeError(f"{day}: original signal is not a lost nonzero-model signal")
        if not any(not (float(row.get("open_price") or 0) > 0) for row in raw):
            raise RuntimeError(f"{day}: original signal does not prove selected opening prices missing")
    return {"session_date": day.isoformat(), "signal_id": signal_id,
            "summary_path": str(summary_path.resolve()), "summary_sha256": _sha256(summary_path),
            "weights_path": str(weights_path.resolve()), "weights_sha256": _sha256(weights_path),
            "config_fingerprint": summary.get("config_fingerprint"),
            "checkpoint_fingerprint": summary.get("checkpoint_fingerprint"),
            "signal_started_at": summary.get("signal_started_at") or summary.get("generated_at"),
            "generated_at": summary.get("generated_at"),
            "raw_nonzero_count": len(raw), "original_weights_preserved": preserve_original_signal}


def _assert_source_pins(pins: list[dict]) -> None:
    for pin in pins:
        if _sha256(Path(pin["path"])) != pin["sha256"]:
            raise RuntimeError(f"repair source changed: {pin['path']}")


def _prepared_sessions(manifest: dict, spec, live, *, fetch_current: bool) -> tuple[list[dict], list[dict]]:
    result, pins = [], []
    roots = tuple(Path(value) for value in manifest["minute_roots"])
    now = datetime.now(TAIPEI)
    preserve_original = manifest.get("preserve_original_signal") is True
    append_missing = manifest.get("append_missing_session") is True
    published = manifest.get("published_revisions") is True
    for source in manifest["sessions"]:
        day = date.fromisoformat(source["session_date"])
        summary_path = Path(source["summary_path"])
        weights_path = summary_path.with_name("target_weights.parquet")
        summary = _object(summary_path)
        rows = pl.read_parquet(weights_path).to_dicts()
        original = source["original"]
        revision_rows = []
        if published:
            plan = manifest["published_revision_plan"]
            for version in plan["versions"]:
                for key in ("summary", "weights"):
                    path = Path(version[f"{key}_path"])
                    if _sha256(path) != version[f"{key}_sha256"]:
                        raise RuntimeError(f"original published artifact changed: {path}")
                    pins.append({"path": str(path), "sha256": version[f"{key}_sha256"]})
                if version["effective_entry_at"]:
                    revision_rows.append((version, _compact_replay_summary(_object(Path(version["summary_path"]))),
                        pl.read_parquet(version["weights_path"]).to_dicts()))
            rows_for_prices = [row for _, _, version_rows in revision_rows for row in version_rows]
        else:
            rows_for_prices = rows
        if preserve_original:
            if _sha256(weights_path) != original["weights_sha256"] or _sha256(summary_path) != original["summary_sha256"]:
                raise RuntimeError(f"{day}: original signal changed")
            summary = {**summary, "signal_id": source["repair_signal_id"]}
        for key in ("config_fingerprint", "checkpoint_fingerprint"):
            if summary.get(key) != original[key]:
                raise RuntimeError(f"{day}: repair changed {key}")
        original_symbols = pl.read_parquet(original["weights_path"], columns=["symbol"])["symbol"].to_list()
        if [row["symbol"] for row in rows] != original_symbols:
            raise RuntimeError(f"{day}: repair changed model universe/order")
        symbols = {str(row["symbol"]) for row in rows_for_prices if abs(float(row.get("target_weight") or 0)) > 0}
        if not symbols:
            raise RuntimeError(f"{day}: regenerated signal still has no targets")
        if str(summary["signal_id"]) in set(manifest["anchor"].get("processed_signal_ids") or []):
            raise RuntimeError(f"{day}: replacement identity was already consumed")
        opens = {str(row["symbol"]): float(row["open_price"]) for row in rows
                 if row.get("open_price") is not None and float(row["open_price"]) > 0}
        if published:
            opens, open_counts, no_trade = _official_open_map([spec], {spec.market: (
                datetime.fromisoformat(plan["versions"][0]["publication_upper_bound"]), summary_path,
                weights_path, summary, rows)}, day,
                twse_daily_ohlcv_path=DEFAULT_TWSE_DAILY_OHLCV_PATH,
                tpex_daily_ohlcv_path=DEFAULT_TPEX_DAILY_OHLCV_PATH)
            source["official_open_unavailable_symbols"] = sorted(symbols - opens.keys())
            source["official_open_source_counts"] = open_counts
            source["official_no_trade_symbols"] = sorted(no_trade & (symbols - opens.keys()))
            symbols &= opens.keys()
            for path in (DEFAULT_TWSE_DAILY_OHLCV_PATH, DEFAULT_TPEX_DAILY_OHLCV_PATH):
                pins.append({"path": str(path.resolve()), "sha256": _sha256(path)})
        if not symbols <= opens.keys():
            raise RuntimeError(f"{day}: selected model opening prices still missing")
        limit_path = Path(manifest.get("price_limit_dir", ROOT / "artifacts/live/tw_price_limits")) / f"{day}.parquet"
        limits = _load_price_limits(limit_path)
        # Keep every original model row, but do not fetch prices for targets
        # which cannot buy even one lot at the frozen session NAV.
        if preserve_original or append_missing:
            nav = float(manifest["anchor"]["total_equity_twd"] if append_missing
                        else manifest["anchor"]["session_sizing_nav_twd"])
            symbols = {str(row["symbol"]) for row in rows_for_prices if str(row["symbol"]) in opens
                       and abs(float(row.get("target_weight") or 0)) > 0 and whole_lot_target_shares(
                float(row.get("target_weight") or 0), nav, opens[str(row["symbol"])], spec.lot_size
            ) > 0}
            if not symbols:
                raise RuntimeError(f"{day}: original targets have no whole-lot orders")
        if not symbols <= limits.keys():
            raise RuntimeError(f"{day}: executable target legal price limits missing: {sorted(symbols - limits.keys())}")
        completed = source.get("completed_through")
        if completed:
            cutoff = datetime.fromisoformat(completed)
        else:
            cutoff = datetime.combine(day, time(13, 30), tzinfo=TAIPEI)
            if day == now.date():
                cutoff = min(cutoff, now.replace(second=0, microsecond=0)-timedelta(minutes=1))
            source["completed_through"] = cutoff.isoformat()
        books, book_proof = load_local_stock_0901_vwaps(roots, sorted(symbols), trading_date=day)
        bars, bar_proof = _minute_bar_rows(roots, trading_date=day, symbols=symbols)
        first_key = datetime.combine(day, time(9, 1), tzinfo=TAIPEI).isoformat(timespec="minutes")
        last_key = cutoff.isoformat(timespec="minutes")
        missing = (symbols - bars.keys() if published else
                   {symbol for symbol in symbols if symbol not in books
                    or first_key not in bars.get(symbol, {}) or last_key not in bars.get(symbol, {})})
        if fetch_current:
            source["minute_source_before"] = {"requested_symbols": sorted(symbols), "missing_symbols": sorted(missing),
                                               "source_files": bar_proof.get("source_files", [])}
        if missing and (preserve_original or append_missing):
            raise RuntimeError(f"{day}: use canonical download_shioaji_tw_minute_kbars.py for missing local paths: {sorted(missing)}")
        if day == now.date() and fetch_current and missing:
            fetched, fetch_proof = fetch_shioaji_current_stock_minute_bars(
                sorted(missing), trading_date=day, completed_through=cutoff,
            )
            if fetch_proof["resolved_symbols"] != len(missing) or fetch_proof["error_counts"]:
                raise RuntimeError(f"current minute acquisition incomplete: {fetch_proof}")
            capture = Path(manifest["operation_root"]) / "minute_sources" / uuid.uuid4().hex
            path = capture / f"trade_date={day}" / "data.parquet"
            path.parent.mkdir(parents=True)
            pl.DataFrame(fetched).write_parquet(path)
            fetch_proof.update({"received_at": datetime.now(TAIPEI).isoformat(),
                                "path": str(path), "sha256": _sha256(path)})
            _atomic_json(path.with_suffix(".receipt.json"), fetch_proof)
            # A newer immutable capture wins; never overwrite earlier source evidence.
            roots = (capture, *roots)
            manifest["minute_roots"] = [str(root) for root in roots]
            books, book_proof = load_local_stock_0901_vwaps(roots, sorted(symbols), trading_date=day)
            bars, bar_proof = _minute_bar_rows(roots, trading_date=day, symbols=symbols)
            manifest["current_capture"] = fetch_proof
        if day == now.date() and fetch_current:
            manifest["current_capture"] = {**manifest.get("current_capture", {}),
                "completed_through": cutoff.isoformat(), "local_first": True,
                "queried_symbols": sorted(missing) if not preserve_original else [],
                "source_files": bar_proof.get("source_files", [])}
        if fetch_current:
            source["minute_source_after"] = {"resolved_symbols": sorted(bars), "source_files": bar_proof.get("source_files", [])}
        if (not published and set(books) != symbols) or set(bars) != symbols:
            raise RuntimeError(f"{day}: missing 09:01/minute path for {sorted(symbols-set(books))}")
        if preserve_original and any(first_key not in bars[symbol] or last_key not in bars[symbol] for symbol in symbols):
            raise RuntimeError(f"{day}: replay source does not cover both 09:01 and {cutoff:%H:%M}")
        end_at = cutoff.timetz().replace(tzinfo=None)
        if preserve_original:
            retained, retained_proof = _load_retained_historical_entry_books(
                historical_book_root=Path(manifest["live_dir"])/"missed_opening_0901_prices",
                trading_date=day, allow_missing=True,
            )
            # Retain the original source-backed first-minute VWAP when present;
            # the newly acquired KBars supply the rest of the session path.
            for symbol in symbols & retained.keys():
                entry = retained[symbol]
                stamp = datetime.fromisoformat(str(entry.get("quote_at") or ""))
                if (stamp.utcoffset() != timedelta(hours=8) or stamp != datetime.combine(day, time(9, 1), tzinfo=TAIPEI)
                        or entry.get("execution_price_0901_method") not in {"minute_vwap", "minute_close"}):
                    raise RuntimeError(f"{day}/{symbol}: invalid retained first-minute evidence clock/method")
                books[symbol] = entry
            book_proof["retained_entry_source"] = retained_proof
            if not retained_proof.get("source_missing"):
                pins.append({"path": retained_proof["source_path"], "sha256": retained_proof["source_sha256"]})
        rule_source = _rule_data_dir(live, spec)
        rule_capture = Path(manifest["operation_root"]) / "exact_session_rules" / str(day)
        rule_receipt = rule_capture / "source_receipt.json"
        if fetch_current:
            rule_proof = _materialize_exact_session_rules(
                source_root=rule_source, destination_root=rule_capture, trading_date=day)
            _atomic_json(rule_receipt, rule_proof)
        else:
            rule_proof = _object(rule_receipt)
        eligibility, coverage = load_live_eligibility(
            rule_data_dir=rule_capture, parquet_root=spec.parquet_root,
            symbols=[str(row["symbol"]) for row in rows], trading_date=day,
        )
        if any(not eligibility[symbol].covered for symbol in symbols):
            raise RuntimeError(f"{day}: exact-session selected eligibility unavailable")
        for proof in rule_proof.values():
            pins.extend([{"path": proof["raw_path"], "sha256": proof["raw_sha256"]},
                         {"path": proof["normalized_path"], "sha256": proof["normalized_sha256"]}])
        observed = datetime.combine(day, time(9, 1), tzinfo=TAIPEI)
        quotes, quote_proof = _entry_quotes(
            rows, limits, quote_at=observed, spec=spec,
            canonical_open_by_symbol=opens, canonical_open_source="repaired_source_backed_session_open",
            historical_books=books,
        )
        replay_summary = {**summary,
                          "signal_started_at": original["signal_started_at"],
                          "source_generated_at": summary.get("generated_at"),
                          "source_signal_started_at": summary.get("signal_started_at"),
                          "summary_path": str(summary_path), "weights_path": str(weights_path),
                          "simulation_replay": True, "historical_minute_valuation": True,
                          "entry_fill_contract": REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
                          "replay_basis": manifest.get("contract", CONTRACT), "replay_source": "source_pinned_completed_minute_ohlcv",
                          "replay_effective_signal_at": observed.isoformat(),
                          "opening_repair_of_signal_id": original["signal_id"],
                          "opening_repair_source_generated_at": original.get("generated_at"),
                          "counterfactual_signal_regeneration": not preserve_original,
                          "original_weights_preserved": preserve_original,
                          "repair_recorded_at": manifest["created_at"],
                          "simulation_only": True}
        revisions = []
        if published:
            prior = None
            for version, original_summary, version_rows in revision_rows:
                repaired_id = f"{version['signal_id']}:published-minute-repair:{source['repair_token']}"
                fields = revision_fields(policy=SIGNAL_UPDATE_PROGRESSIVE, signal_id=repaired_id,
                    input_fingerprint=_digest({"source_signal_id": version["signal_id"],
                        "original_input_fingerprint": version["opening_signal_revision"]["input_fingerprint"],
                        "weights_sha256": version["weights_sha256"]}), previous=prior)
                fields["opening_signal_revision"]["trigger"] = "source_pinned_published_target_minute_replay"
                derived = {**original_summary, **fields, "signal_id": repaired_id,
                    "summary_path": version["summary_path"], "weights_path": version["weights_path"],
                    "simulation_replay": True, "historical_minute_valuation": True,
                    "entry_fill_contract": REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
                    "replay_basis": PUBLISHED_SESSION_CONTRACT,
                    "replay_source": "source_pinned_completed_minute_ohlcv",
                    "replay_effective_signal_at": version["effective_entry_at"],
                    "opening_repair_of_signal_id": version["signal_id"],
                    "original_weights_preserved": True, "counterfactual_signal_regeneration": False,
                    "repair_recorded_at": manifest["created_at"], "simulation_only": True,
                    "production_order_possible": False,
                    "published_revision_replay": {"contract": PUBLISHED_REVISION_MINUTE_REPLAY_CONTRACT,
                        "plan_sha256": plan["plan_sha256"], "source_signal_id": version["signal_id"],
                        "publication_upper_bound": version["publication_upper_bound"],
                        "original_revision": version["opening_signal_revision"]}}
                prior = derived
                revisions.append({"summary": derived, "rows": _canonicalize_signal_rows_for_replay(version_rows, opens),
                                  "observed": datetime.fromisoformat(version["effective_entry_at"])})
            replay_summary, rows = revisions[0]["summary"], revisions[0]["rows"]
        for path in (summary_path, weights_path, limit_path,
                     Path(original["summary_path"]), Path(original["weights_path"])):
            pins.append({"path": str(path.resolve()), "sha256": _sha256(path)})
        for pin in bar_proof.get("source_files", []):
            pins.append({"path": pin["path"], "sha256": pin["sha256"]})
        result.append({"day": day, "observed": observed, "summary": replay_summary,
                       "rows": rows if (preserve_original or published) else _canonicalize_signal_rows_for_replay(rows, opens),
                       "quotes": quotes, "eligibility": eligibility, "coverage": coverage,
                       "bars": bars, "end_at": end_at, "book_proof": book_proof,
                       "bar_proof": bar_proof, "quote_proof": quote_proof,
                       "rule_data_dir": rule_source, "rule_proof": rule_proof})
        if published:
            result[-1]["revisions"] = revisions[1:]
    return result, pins


def _execute(engine: TwDayTradeSimulationEngine, spec, sessions: list[dict], *, reason: str) -> list[dict]:
    market = spec.market
    completed_replay = reason == ORIGINAL_SIGNAL_CONTRACT and all(item["end_at"] == time(13, 30) for item in sessions)
    if reason in {MISSING_SESSION_CONTRACT, PUBLISHED_SESSION_CONTRACT}:
        if len(sessions) != 1 or sessions[0]["end_at"] != time(13, 30):
            raise RuntimeError("missing-session append requires one completed session")
        _require_missing_session_target(engine.state["modes"][market], sessions[0]["day"])
        if engine._session_has_fill(market, str(sessions[0]["day"])):
            raise RuntimeError("missing-session append found existing target fills")
    else:
        engine.rearm_flat_session(market, now=datetime.now(TAIPEI), reason=reason,
                                 completed_session_replay=completed_replay)
    accepted = []
    for item in sessions:
        result = engine.register_signal(
            spec=spec, summary=item["summary"], signal_rows=item["rows"], quotes=item["quotes"],
            eligibility=item["eligibility"], eligibility_coverage=item["coverage"],
            now=item["observed"], counterfactual_open_replay=True,
        )
        if result != "registered":
            raise RuntimeError(f"{item['day']}: registration failed: {result}")
        engine.begin_deferred_ledger_writes()
        mode = engine.state["modes"][market]
        # This is an explicitly re-played historical session. An inherited
        # terminal cash-mark cache must not suppress this journal's 13:30 row.
        mode.pop("terminal_curve_mark", None)
        mode["opening_repair"] = {
            "contract": reason, "original_signal_id": item["summary"]["opening_repair_of_signal_id"],
            "original_signal_at": item["summary"]["signal_started_at"],
            "regenerated_artifact_at": item["summary"]["generated_at"],
            "recorded_at": datetime.now(TAIPEI).isoformat(),
            "effective_entry_at": item["observed"].isoformat(),
            "simulation_only": True, "live_opening_slo_claim": False,
            "original_weights_preserved": item["summary"].get("original_weights_preserved", False),
        }
        # The entry price is VWAP; the initial asset mark is that same completed
        # minute's Close, with net-liquidation fees, not merely the entry price.
        engine._mark_mode(market, item["observed"], _historical_minute_valuation_quotes(
            mode, item["bars"], observed=item["observed"]), append_history=True)
        def closing():
            quotes, proof = _close_quotes(engine, {market: spec}, trading_date=item["day"],
                quote_at=datetime.combine(item["day"], time(13, 30), tzinfo=TAIPEI),
                twse_daily_ohlcv_path=DEFAULT_TWSE_DAILY_OHLCV_PATH,
                tpex_daily_ohlcv_path=DEFAULT_TPEX_DAILY_OHLCV_PATH,
                official_raw_root=item["rule_data_dir"] / "raw")
            item["closing_proof"] = proof
            return quotes
        remaining_revisions = {revision["observed"]: revision for revision in item.get("revisions", [])}
        consumed_revisions = []
        def update_targets(observed):
            revision = remaining_revisions.pop(observed, None)
            if revision is None:
                return
            status = engine.register_signal(spec=spec, summary=revision["summary"],
                signal_rows=revision["rows"], quotes={}, eligibility=item["eligibility"],
                eligibility_coverage=item["coverage"], now=observed, counterfactual_open_replay=True)
            if status != "registered":
                raise RuntimeError(f"published revision {revision['summary']['signal_id']}: {status}")
            consumed_revisions.append(revision["summary"]["published_revision_replay"]["source_signal_id"])
        _replay_historical_intraday(engine, markets=[market], bars=item["bars"],
            trading_date=item["day"], end_at=item["end_at"],
            terminal_close_quote_provider=closing if (item["day"] < datetime.now(TAIPEI).date()
                or reason in {MISSING_SESSION_CONTRACT, PUBLISHED_SESSION_CONTRACT}) else None,
            **({"before_completed_minute": update_targets} if item.get("revisions") else {}))
        if remaining_revisions:
            raise RuntimeError("published revisions were not completely replayed")
        engine.flush_deferred_ledger_writes()
        engine._persist(datetime.combine(item["day"], item["end_at"], tzinfo=TAIPEI))
        mode = engine.state["modes"][market]
        if item["end_at"] == time(13, 30) and int(mode["open_position_count"]):
            raise RuntimeError(f"{item['day']}: completed replay not flat")
        accepted.append({"session_date": str(item["day"]), "signal_id": mode["signal_id"],
                         "entry_requested_shares": mode.get("entry_requested_shares"),
                         "entry_filled_shares": mode.get("entry_filled_shares"),
                         "total_equity_twd": mode["total_equity_twd"],
                         "cumulative_realized_net_pnl_twd": mode["cumulative_realized_net_pnl_twd"],
                         "open_position_count": mode["open_position_count"],
                         "completed_through": item["end_at"].isoformat(),
                         "closing_proof": item.get("closing_proof"),
                         "eligibility_source_proof": item.get("rule_proof"),
                         **({"later_revisions_applied": len(consumed_revisions),
                             "original_revision_signal_ids": consumed_revisions} if item.get("revisions") else {})})
    return accepted


def _curve_proof(path: Path, market: str, sessions: list[dict]) -> list[dict]:
    points = {str(item["day"]): {} for item in sessions}
    with open_paper_journal(path, "r", encoding="utf-8") as stream:
        lines = stream.read().splitlines()
    for line in lines:
        row = json.loads(line)
        stamp = str(row.get("minute") or "")
        if row.get("market") == market and stamp[:10] in points:
            points[stamp[:10]][stamp[:16]] = row
    proof = []
    for item in sessions:
        count = (item["end_at"].hour-9)*60+item["end_at"].minute
        expected = {(item["observed"]+timedelta(minutes=i)).isoformat(timespec="minutes")[:16]
                    for i in range(count)}
        actual = set(points[str(item["day"])])
        if actual != expected:
            raise RuntimeError(f"{item['day']}: minute curve mismatch {len(actual)}/{len(expected)}")
        proof.append({"session_date": str(item["day"]), "unique_minute_points": len(actual),
                      "expected_minute_points": len(expected), "start": "09:01",
                      "end": item["end_at"].strftime("%H:%M")})
    return proof


def prepare(args) -> dict:
    root = args.operation_root.resolve()
    manifest_path = root / "ledger_repair_manifest.json"
    if manifest_path.exists():
        raise FileExistsError("repair manifest exists; inspect/verify it before another operation")
    published = getattr(args, "published_revisions", False)
    spec, live = _spec(args.market, args.markets_dir, True) if published else _spec(args.market, args.markets_dir)
    mode = _object(args.live_dir / "state.json")["modes"][args.market]
    preserve_original = getattr(args, "preserve_original_signal", False)
    append_missing = getattr(args, "append_missing_session", False) or published
    if append_missing and preserve_original:
        raise ValueError("missing-session append cannot rearm an original signal")
    paths = sorted(args.signal_summary, key=lambda path: _object(path)["asof_date"])
    plan = None
    if published:
        if paths or preserve_original:
            raise ValueError("published revision repair cannot collapse to a supplied single signal")
        plan = _object(root / "published_revision_plan.json")
        if plan["plan_sha256"] != _published_revision_inventory(spec, datetime.now(TAIPEI).date())["plan_sha256"]:
            raise RuntimeError("published revision plan/source membership changed; re-inventory")
        paths = [Path(plan["versions"][0]["summary_path"])]
    if append_missing:
        if (len(paths) != 1 or str(_object(paths[0])["asof_date"])[:10] != str(datetime.now(TAIPEI).date())
                or datetime.now(TAIPEI).time() < time(13, 31)):
            raise RuntimeError("missing-session append is limited to today's completed session")
        _require_missing_session_target(mode, datetime.now(TAIPEI).date())
    else:
        _require_empty_target(mode, preserve_original_signal=preserve_original)
    contract = PUBLISHED_SESSION_CONTRACT if published else MISSING_SESSION_CONTRACT if append_missing else ORIGINAL_SIGNAL_CONTRACT if preserve_original else CONTRACT
    manifest = {"schema_version": 2 if preserve_original else 1, "contract": contract, "simulation_only": True,
                "preserve_original_signal": preserve_original,
                "append_missing_session": append_missing,
                "published_revisions": published,
                "spec_sha256": _spec_digest(spec),
                "production_order_possible": False, "market": args.market,
                "live_dir": str(args.live_dir.resolve()), "operation_root": str(root),
                "price_limit_dir": str(getattr(args, "price_limit_dir", ROOT / "artifacts/live/tw_price_limits").resolve()),
                "created_at": datetime.now(TAIPEI).isoformat(), "anchor": _anchor(mode),
                "minute_roots": [str(path.resolve()) for path in
                                 (*getattr(args, "minute_data_root", []), *DEFAULT_MINUTE_DATA_ROOTS)], "sessions": []}
    if published:
        manifest["published_revision_plan"] = plan
    if preserve_original and (len(paths) != 1 or str(mode["session_date"]) != str(datetime.now(TAIPEI).date())
                              or datetime.now(TAIPEI).time() < time(13, 31)):
        raise RuntimeError("original-signal repair is scoped to one completed current-session replay")
    for path in paths:
        summary = _object(path)
        day = date.fromisoformat(str(summary["asof_date"])[:10])
        if _has_live_fill(args.live_dir, args.market, str(day)):
            raise RuntimeError(f"{day}: live target has existing fills")
        if published:
            first = plan["versions"][0]
            original = {**first, "config_fingerprint": summary["config_fingerprint"],
                "checkpoint_fingerprint": summary["checkpoint_fingerprint"], "generated_at": summary["generated_at"]}
            original_id = original["signal_id"]
        elif append_missing:
            original = _missing_session_proof(spec, day, path)
            original_id = original["signal_id"]
        elif day == datetime.now(TAIPEI).date():
            original_id = str(mode["signal_id"])
        else:
            original_id = str(_object(args.live_dir/"position_history"/str(day)/f"{args.market}.json")["signal_id"])
        if not append_missing:
            original = _source_block_proof(spec, day, original_id, preserve_original_signal=preserve_original)
        if preserve_original and (day != datetime.now(TAIPEI).date() or path.resolve() != Path(original["summary_path"])):
            raise RuntimeError("original-signal repair must use the exact consumed original artifact")
        token = uuid.uuid4().hex[:12]
        manifest["sessions"].append({"session_date": str(day), "summary_path": str(path.resolve()),
                                     "repair_signal_id": (f"{original_id}:published-minute-repair:{token}" if published
                                                          else f"{original_id}:minute-repair:{token}"),
                                     "repair_token": token, "original": original})
    if not manifest["sessions"] or (not append_missing and manifest["sessions"][-1]["session_date"] != mode["session_date"]):
        raise ValueError("repair must end at the current empty session")
    if len({row["session_date"] for row in manifest["sessions"]}) != len(manifest["sessions"]):
        raise ValueError("duplicate repair session")
    sessions, pins = _prepared_sessions(manifest, spec, live, fetch_current=True)
    candidate = args.live_dir.with_name(args.live_dir.name+f".opening-repair-{uuid.uuid4().hex}.candidate")
    candidate.mkdir()
    _atomic_json(candidate/"state.json", {"schema_version": SIMULATION_SCHEMA_VERSION,
                 "modes": {args.market: deepcopy(mode)}, "enabled_markets": [args.market]})
    candidate_engine = TwDayTradeSimulationEngine(candidate, publication_clock=lambda: datetime.now(TAIPEI))
    accepted = _execute(candidate_engine, spec, sessions, reason=contract)
    if preserve_original and (accepted[0]["entry_requested_shares"] != int(mode["entry_requested_shares"])
                              or accepted[0]["entry_filled_shares"] != accepted[0]["entry_requested_shares"]):
        raise RuntimeError("original-signal replay did not complete the same requested whole-lot target")
    curves = _curve_proof(candidate/"marks.jsonl", args.market, sessions)
    if published:
        from scripts.verify_tw_day_trade_scoped_repair import (
            verify_published_fill_accounting, verify_minute_source_coverage,
        )
        with open_paper_journal(candidate / "fills.jsonl") as handle:
            fills = [json.loads(line) for line in handle if line.endswith(b"\n")]
        manifest["candidate_fill_accounting"] = verify_published_fill_accounting(
            candidate_engine.state["modes"][args.market], fills, day=str(sessions[0]["day"]),
            plan=plan, bars=sessions[0]["bars"],
            prior_realized=float(manifest["anchor"]["cumulative_realized_net_pnl_twd"]),
            terminal_close_sources=(accepted[0].get("closing_proof") or {}).get("terminal_close_sources", []))
        with open_paper_journal(candidate / "marks.jsonl") as handle:
            marks = [json.loads(line) for line in handle if line.endswith(b"\n")]
        manifest["candidate_minute_source_coverage"] = verify_minute_source_coverage(
            candidate_engine.state["modes"][args.market], marks, market=args.market,
            day=str(sessions[0]["day"]), source_files=sessions[0]["bar_proof"]["source_files"])
    for item in accepted:
        for source in (item.get("closing_proof") or {}).get("terminal_close_sources", []):
            pins.append({"path": source["path"], "sha256": source["sha256"]})
    for source in (args.markets_dir / f"{args.market}.yaml", Path(spec.config_path)):
        pins.append({"path": str(source.resolve()), "sha256": _sha256(source)})
    if preserve_original or append_missing:
        for source in (Path(__file__), ROOT/"scripts/rebuild_tw_day_trade_open_price_replay.py",
                       ROOT/"stockagent/live/tw_day_trade_simulation.py", Path(spec.checkpoint_path),
                       *((ROOT/"scripts/verify_tw_day_trade_scoped_repair.py",) if published else ())):
            pins.append({"path": str(source.resolve()), "sha256": _sha256(source)})
    _assert_source_pins(pins)
    manifest.update({"candidate_dir": str(candidate.resolve()), "source_pins": pins,
                     "candidate_sessions": accepted, "minute_curves": curves,
                     "candidate_final_outcome": _outcome(candidate_engine.state["modes"][args.market]),
                     "status": "candidate_validated_not_applied"})
    _atomic_json(manifest_path, manifest)
    return {key: manifest[key] for key in ("status", "market", "candidate_dir", "candidate_sessions", "minute_curves")}


def apply(args) -> dict:
    with control_lease(owner=f"scoped-minute-repair:{args.market}", timeout_seconds=0.0):
        return _apply_locked(args)


def _apply_locked(args) -> dict:
    path = args.operation_root.resolve()/"ledger_repair_manifest.json"
    manifest = _object(path)
    if manifest["status"] != "candidate_validated_not_applied":
        raise RuntimeError("only a newly validated candidate can be applied; inspect interrupted receipts")
    if manifest["market"] != args.market or manifest["live_dir"] != str(args.live_dir.resolve()):
        raise ValueError("repair target identity mismatch")
    spec, live = (_spec(args.market, args.markets_dir, True) if manifest.get("published_revisions")
                  else _spec(args.market, args.markets_dir))
    if manifest.get("spec_sha256") and _spec_digest(spec) != manifest["spec_sha256"]:
        raise RuntimeError("repair execution configuration changed after candidate preparation")
    _assert_source_pins(manifest["source_pins"])
    sessions, pins = _prepared_sessions(manifest, spec, live, fetch_current=False)
    _assert_source_pins(pins)
    cutoff = datetime.fromisoformat(manifest["current_capture"]["completed_through"])
    preserve_original = manifest.get("preserve_original_signal") is True
    append_missing = manifest.get("append_missing_session") is True
    completed_replay = (((preserve_original and manifest.get("contract") == ORIGINAL_SIGNAL_CONTRACT)
                         or (append_missing and manifest.get("contract") in {MISSING_SESSION_CONTRACT, PUBLISHED_SESSION_CONTRACT}))
                        and len(sessions) == 1 and sessions[0]["day"] == datetime.now(TAIPEI).date()
                        and cutoff == datetime.combine(sessions[0]["day"], time(13, 30), tzinfo=TAIPEI)
                        and sessions[0]["end_at"] == time(13, 30))
    if datetime.now(TAIPEI)-cutoff > timedelta(minutes=3) and not completed_replay:
        raise RuntimeError("current-session candidate is stale; refresh sources and revalidate before applying")
    active = subprocess.run(["systemctl", "is-active", "--quiet", SERVICE]).returncode == 0
    lock = None
    try:
        if active:
            subprocess.run(["systemctl", "stop", SERVICE], check=True)
        lock = _acquire_engine_lock(args.live_dir)
        engine = TwDayTradeSimulationEngine(args.live_dir, publication_clock=lambda: datetime.now(TAIPEI))
        mode = engine.state["modes"][args.market]
        if append_missing:
            _require_missing_session_target(mode, sessions[0]["day"])
        else:
            _require_empty_target(mode, preserve_original_signal=preserve_original)
        if _anchor(mode) != manifest["anchor"]:
            raise RuntimeError("target economic state advanced after candidate preparation")
        for item in sessions:
            if engine._session_has_fill(args.market, str(item["day"])):
                raise RuntimeError("target received fills after preparation")
        before = deepcopy(engine.state)
        others = {key: _digest(value) for key,value in before["modes"].items() if key != args.market}
        _atomic_json(args.operation_root/"state_before_apply.json", before)
        histories = {}
        for item in sessions:
            history = args.live_dir / "position_history" / str(item["day"]) / f"{args.market}.json"
            if history.is_file():
                histories[str(item["day"])] = _object(history)
        _atomic_json(args.operation_root/"position_history_before_apply.json", histories)
        manifest["journal_prefix_bytes"] = {name: (args.live_dir/name).stat().st_size for name in
                                            ("signals.jsonl", "orders.jsonl", "fills.jsonl", "marks.jsonl", "events.jsonl")}
        manifest.update({"status": "applying", "apply_started_at": datetime.now(TAIPEI).isoformat()})
        _atomic_json(path, manifest)
        accepted = _execute(engine, spec, sessions, reason=manifest.get("contract", CONTRACT))
        if _outcome(engine.state["modes"][args.market]) != manifest["candidate_final_outcome"]:
            raise RuntimeError("applied economics differ from validated candidate")
        if others != {key: _digest(value) for key,value in engine.state["modes"].items() if key != args.market}:
            raise RuntimeError("non-target market state changed during scoped repair")
        manifest.update({"status": "applied", "applied_at": datetime.now(TAIPEI).isoformat(),
                         "applied_sessions": accepted, "non_target_modes_unchanged": True,
                         "original_journals_retained": True})
        _atomic_json(path, manifest)
    except Exception as exc:
        if manifest.get("status") == "applying":
            manifest.update({"status": "apply_interrupted_requires_audit",
                             "failed_at": datetime.now(TAIPEI).isoformat(),
                             "error_type": type(exc).__name__, "error": str(exc)})
            _atomic_json(path, manifest)
        raise
    finally:
        if lock is not None:
            lock.close()
        if active:
            subprocess.run(["systemctl", "start", SERVICE], check=True)
    return {key: manifest[key] for key in ("status", "applied_at", "applied_sessions", "non_target_modes_unchanged")}


def recover_interrupted(args) -> dict:
    """Recover only this tool's interrupted append, via an isolated exchange.

    No live journal is truncated or edited. The entire interrupted directory
    survives as rollback evidence; pre-operation journal bytes are retained in
    the reconstructed directory. Existing unrelated account economics must
    still equal the saved pre-apply state.
    """
    from scripts.promote_tw_day_trade_replay import _exchange_directories
    from scripts.verify_tw_day_trade_scoped_repair import prefix_sha256

    root = args.operation_root.resolve()
    path = root / "ledger_repair_manifest.json"
    original = _object(path)
    if (original.get("status") != "apply_interrupted_requires_audit"
            or original.get("append_missing_session") is not True
            or original.get("market") != args.market
            or original.get("live_dir") != str(args.live_dir.resolve())
            or (root / "interrupted_manifest.json").exists()):
        raise RuntimeError("recovery requires one exact, previously unaudited interrupted missing-session append")
    before = _object(root / "state_before_apply.json")
    baseline = _object(root / "journal_prefix_baseline.json")
    revised = deepcopy(original)
    revisions = []
    allowed_code = {Path(__file__).resolve(), (ROOT / "scripts/rebuild_tw_day_trade_open_price_replay.py").resolve()}
    for pin in revised["source_pins"]:
        source = Path(pin["path"])
        current = _sha256(source)
        if current != pin["sha256"]:
            if source.resolve() not in allowed_code:
                raise RuntimeError(f"interrupted recovery source changed: {source}")
            revisions.append({"path": str(source), "previous_sha256": pin["sha256"], "sha256": current})
            pin["sha256"] = current
    _assert_source_pins(revised["source_pins"])
    spec, live = (_spec(args.market, args.markets_dir, True) if original.get("published_revisions")
                  else _spec(args.market, args.markets_dir))
    if _spec_digest(spec) != original["spec_sha256"]:
        raise RuntimeError("interrupted recovery changed execution configuration")
    sessions, pins = _prepared_sessions(revised, spec, live, fetch_current=False)
    _assert_source_pins(pins)
    candidate = args.live_dir.with_name(args.live_dir.name + f".recovery-{uuid.uuid4().hex}.candidate")
    active = subprocess.run(["systemctl", "is-active", "--quiet", SERVICE]).returncode == 0
    lock = None
    with control_lease(owner=f"interrupted-minute-repair:{args.market}", timeout_seconds=0.0):
        try:
            if active:
                subprocess.run(["systemctl", "stop", SERVICE], check=True)
            lock = _acquire_engine_lock(args.live_dir)
            state = _object(args.live_dir / "state.json")
            for market, mode in state["modes"].items():
                if mode.get("ledger_state_divergence") or any(int(p.get("signed_shares") or 0)
                        for p in (mode.get("positions") or {}).values()):
                    raise RuntimeError(f"recovery requires reconciled flat accounts: {market}")
                if market != args.market and _anchor(mode) != _anchor(before["modes"][market]):
                    raise RuntimeError(f"non-target account advanced after interrupted append: {market}")
            if state["modes"][args.market].get("signal_id") != sessions[0]["summary"]["signal_id"]:
                raise RuntimeError("interrupted target has a different accepted decision")
            for name, proof in baseline["journals"].items():
                if prefix_sha256(args.live_dir / name, proof["prefix_bytes"]) != proof["sha256"]:
                    raise RuntimeError(f"original journal prefix changed before recovery: {name}")
            needed = sum(p.stat().st_size for p in args.live_dir.rglob("*") if p.is_file())
            if shutil.disk_usage(args.live_dir).free < needed + 1024**3:
                raise RuntimeError("insufficient disk for a recoverable isolated ledger copy")
            shutil.copytree(args.live_dir, candidate, ignore=lambda _dir, names: {
                name for name in names if name.endswith(".lock") or name.endswith(".tmp")})
            rebind_cloned_journals(args.live_dir, candidate)
            for name, proof in baseline["journals"].items():
                # This is a new isolated projection, never a live source file.
                with journal_write_transaction(candidate / name, operation="replace"), (candidate / name).open("r+b") as handle:
                    handle.truncate(original["journal_prefix_bytes"][name])
                    handle.flush()
                    os.fsync(handle.fileno())
            # Root execution/recovery watermarks must match the same original
            # journal prefix, not point into the retained failed transaction.
            _atomic_json(candidate / "state.json", deepcopy(before))
            history_before = _object(root / "position_history_before_apply.json")
            for item in sessions:
                history = candidate / "position_history" / str(item["day"]) / f"{args.market}.json"
                if str(item["day"]) in history_before:
                    _atomic_json(history, history_before[str(item["day"])])
                elif history.exists():
                    history.unlink()  # Generated failed-transaction projection; live original remains retained.
            engine = TwDayTradeSimulationEngine(candidate, publication_clock=lambda: datetime.now(TAIPEI))
            accepted = _execute(engine, spec, sessions, reason=original.get("contract", MISSING_SESSION_CONTRACT))
            if _outcome(engine.state["modes"][args.market]) != original["candidate_final_outcome"]:
                raise RuntimeError("recovery differs from the original validated financial candidate")
            for market, mode in engine.state["modes"].items():
                if market != args.market and _anchor(mode) != _anchor(before["modes"][market]):
                    raise RuntimeError(f"recovery changed non-target account economics: {market}")
            _curve_proof(candidate / "marks.jsonl", args.market, sessions)
            for name, proof in baseline["journals"].items():
                if prefix_sha256(candidate / name, proof["prefix_bytes"]) != proof["sha256"]:
                    raise RuntimeError(f"recovery did not preserve original journal prefix: {name}")
            _assert_source_pins(revised["source_pins"])
            _atomic_json(root / "interrupted_manifest.json", original)
            _exchange_directories(args.live_dir, candidate)
            revised.update(status="applied", applied_at=datetime.now(TAIPEI).isoformat(),
                applied_sessions=accepted, non_target_modes_unchanged=True, original_journals_retained=True,
                atomic_recovery={"rollback_directory": str(candidate.resolve()),
                    "interrupted_manifest_path": str(root / "interrupted_manifest.json"),
                    "interrupted_manifest_sha256": _sha256(root / "interrupted_manifest.json"),
                    "implementation_revisions": revisions, "broker_orders": 0,
                    "original_validated_outcome_preserved": True})
            _atomic_json(path, revised)
        finally:
            if lock is not None:
                lock.close()
            if active:
                subprocess.run(["systemctl", "start", SERVICE], check=True)
    return {key: revised[key] for key in ("status", "applied_at", "applied_sessions", "atomic_recovery")}


def bridge_marks(args) -> dict:
    """Append source-backed valuation holes at replay/live handoff only.

    This never writes state, positions, fills or orders and never replaces a
    resumed live mark. An inventory/cash change requires a full audited replay,
    not this deliberately narrow mark-only fast path.
    """
    root = args.operation_root.resolve()
    receipt_path = root / "mark_bridge_receipt.json"
    if receipt_path.exists():
        return _object(receipt_path)
    manifest = _object(root / "ledger_repair_manifest.json")
    if (manifest.get("status") != "applied" or manifest["market"] != args.market
            or manifest["live_dir"] != str(args.live_dir.resolve())):
        raise RuntimeError("mark bridge requires the exact applied repair")
    minutes = _handoff_gap_minutes(args.live_dir / "marks.jsonl", manifest)
    if not minutes:
        receipt = {"status": "no_handoff_gap", "minutes": [], "orders_fills_state_untouched": True}
        _atomic_json(receipt_path, receipt)
        return receipt
    baseline = _object(Path(manifest["candidate_dir"]) / "state.json")["modes"][args.market]
    current = _object(args.live_dir / "state.json")["modes"][args.market]
    if _inventory_anchor(current) != _inventory_anchor(baseline):
        raise RuntimeError("inventory/cash changed; mark-only bridge requires a full audit")
    symbols = sorted({str(p["symbol"]) for p in baseline.get("positions", {}).values()
                      if int(p.get("signed_shares") or 0)})
    capture = root / "mark_bridge" / uuid.uuid4().hex
    capture.mkdir(parents=True)
    if symbols:
        rows, acquisition = fetch_shioaji_current_stock_minute_bars(
            symbols, trading_date=minutes[-1].date(), completed_through=minutes[-1])
        if acquisition["resolved_symbols"] != len(symbols) or acquisition["error_counts"]:
            raise RuntimeError("handoff minute acquisition incomplete")
        source = capture / f"trade_date={minutes[-1].date()}" / "data.parquet"
        source.parent.mkdir()
        pl.DataFrame(rows).write_parquet(source)
        bars, proof = _minute_bar_rows((capture,), trading_date=minutes[-1].date(), symbols=set(symbols))
    else:
        bars, acquisition, proof = {}, {}, {}
    staging = capture / "mark_projection"
    _atomic_json(staging / "state.json", {"schema_version": SIMULATION_SCHEMA_VERSION,
                 "modes": {args.market: deepcopy(baseline)}, "enabled_markets": [args.market]})
    engine = TwDayTradeSimulationEngine(staging)
    for minute in minutes:
        engine._mark_mode(args.market, minute, _historical_minute_valuation_quotes(
            engine.state["modes"][args.market], bars, observed=minute), append_history=True)
    with open_paper_journal(engine.marks_path, "r", encoding="utf-8") as stream:
        bridged = [json.loads(line) for line in stream.read().splitlines()]
    if len(bridged) != len(minutes):
        raise RuntimeError("handoff bridge did not produce every requested minute")
    if _inventory_anchor(_object(args.live_dir / "state.json")["modes"][args.market]) != _inventory_anchor(baseline):
        raise RuntimeError("inventory/cash advanced during mark acquisition")
    receipt = {"status": "marks_bridged", "recorded_at": datetime.now(TAIPEI).isoformat(),
               "market": args.market, "minutes": [stamp.isoformat() for stamp in minutes],
               "acquisition": acquisition, "source_proof": proof,
               "orders_fills_state_untouched": True, "simulation_only": True}
    _atomic_json(capture / "source_receipt.json", receipt)
    _append_jsonl_many(args.live_dir / "marks.jsonl", [
        {**row, "handoff_mark_repair": True, "handoff_repair_receipt": str(capture / "source_receipt.json")}
        for row in bridged])
    _atomic_json(receipt_path, receipt)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan-published-revisions", "prepare", "apply", "recover-interrupted", "bridge-marks", "status"))
    parser.add_argument("--market", default="tw_day_trade_multi_basis_22")
    parser.add_argument("--live-dir", type=Path, default=ROOT/"artifacts/live/tw_day_trade_simulation")
    parser.add_argument("--markets-dir", type=Path, default=ROOT/"services/discord_bot/markets")
    parser.add_argument("--operation-root", type=Path, required=True)
    parser.add_argument("--signal-summary", type=Path, action="append", default=[])
    parser.add_argument("--minute-data-root", type=Path, action="append", default=[],
                        help="Additional canonical collector roots, checked before default local sources.")
    parser.add_argument("--price-limit-dir", type=Path, default=ROOT / "artifacts/live/tw_price_limits",
                        help="Receipt-backed exact-session static limits; original snapshots stay untouched.")
    parser.add_argument("--preserve-original-signal", action="store_true",
                        help="Repair a never-filled completed minute replay without model inference or weight changes.")
    parser.add_argument("--append-missing-session", action="store_true",
                        help="Append today's missing completed session after flat prior history; never replace existing fills.")
    parser.add_argument("--published-revisions", action="store_true",
                        help="Use a pinned all-lineage published-revision plan for the missing completed session.")
    args = parser.parse_args()
    result = plan_published_revisions(args) if args.action == "plan-published-revisions" else prepare(args) if args.action == "prepare" else apply(args) if args.action == "apply" else recover_interrupted(args) if args.action == "recover-interrupted" else bridge_marks(args) if args.action == "bridge-marks" else _object(
        args.operation_root/"ledger_repair_manifest.json")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()

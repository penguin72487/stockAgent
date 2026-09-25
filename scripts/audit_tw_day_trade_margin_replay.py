#!/usr/bin/env python3
"""Read-only, fill-to-inventory-to-minute-NAV audit of a carried paper replay."""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, time
import json
import math
from pathlib import Path
import sys

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.rebuild_tw_day_trade_minute_curves import (
    _read_json, _read_jsonl, _sha256, _atomic_json, _is_entry_inventory_fill,
    validate_existing_strategy_marks,
)
from scripts.complete_tw_day_trade_paper_entry import CONTRACT as PAPER_COMPLETION_CONTRACT
from stockagent.live.tw_day_trade_simulation import (
    ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
    MARGIN_CARRY_CONTRACT,
    REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
)
from downloader.download_shioaji_tw_minute_kbars import minute_receipt_valid
from downloader.download_tw_public_data import _validated_taiex_session_dates
from stockagent.live.tw_share_replacement import ODD_LOT_BOARD_PRICE, load_share_replacements
from stockagent.data.panel import _CorporateActionReferencePaths, _load_corporate_action_reference


def _entry_source_path(source: str) -> Path:
    prefix, separator, name = source.partition(":")
    if (not separator or prefix not in {
            "local_minute_parquet_0901_minute_vwap",
            "local_minute_parquet_0901_minute_close"}
            or not Path(name).is_absolute()):
        raise ValueError(f"unsupported historical entry source identity: {source}")
    return Path(name)


def _retained_entry_book_source_days(
    state_dir: Path,
    session: dict,
    opening_fill_symbols: set[str],
    source_signatures: dict[str, tuple[int, int, int]],
    retained_opening_bars: list[dict] | None = None,
) -> dict[str, set[date]]:
    """Recover exact 09:01 source files from a hash-pinned retained entry book.

    A carried position can be fully reduced at 09:01 and therefore never enter
    the subsequent intraday inventory scan.  Its minute source is still present
    in the replay's immutable entry book, so audit that book rather than treating
    the missing intraday source-count entry as missing market data.
    """
    if not opening_fill_symbols:
        return {}
    proof = session.get("historical_entry_books") or {}
    raw_path, expected_sha = proof.get("path"), proof.get("sha256")
    if not raw_path or not expected_sha:
        return {}
    path = Path(str(raw_path))
    expected_dir = (state_dir / "replay_entry_books").resolve()
    if (not path.is_absolute() or path.resolve().parent != expected_dir
            or path.name != f"{session['session_date']}.parquet"):
        raise ValueError(f"retained entry book is outside candidate: {path}")
    stat = path.stat()
    signature = (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
    source_signatures[str(path)] = signature
    if _sha256(path) != expected_sha:
        raise ValueError(f"retained entry book hash mismatch: {path}")
    columns = [
        "symbol",
        "quote_at",
        "source",
        "execution_price_0901",
        "tick_volume_units_0901",
        "source_window_start",
        "source_window_end",
    ]
    schema = pl.read_parquet_schema(path)
    frame = pl.read_parquet(
        path, columns=[column for column in columns if column in schema]
    )
    missing_columns = [column for column in columns if column not in frame.columns]
    if missing_columns:
        frame = frame.with_columns(
            *(pl.lit(None).alias(column) for column in missing_columns)
        )
    frame = frame.filter(pl.col("symbol").is_in(sorted(opening_fill_symbols)))
    if frame.select(pl.col("symbol").is_duplicated().any()).item():
        raise ValueError(f"retained entry book has duplicate symbols: {path}")
    day = session["session_date"]
    result: dict[str, set[date]] = defaultdict(set)
    for row in frame.iter_rows(named=True):
        if str(row["quote_at"])[:16] != f"{day}T09:01":
            raise ValueError(
                f"retained entry book has non-09:01 quote: {row['symbol']}:{row['quote_at']}"
            )
        source_identity = str(row["source"])
        shioaji_volume_multiplier: float | None = None
        if source_identity == (
            "shioaji:historical_ticks_0900_090059_vwap_right_label_0901"
        ):
            shioaji_volume_multiplier = 1_000.0
        elif source_identity in {
            "shioaji:historical_kbar_0901_minute_vwap",
            "shioaji:historical_kbar_0901_minute_close",
        }:
            shioaji_volume_multiplier = 1.0
        if shioaji_volume_multiplier is not None:
            try:
                opening = datetime.fromisoformat(str(row["source_window_start"]))
                closing = datetime.fromisoformat(str(row["source_window_end"]))
                quote_at = datetime.fromisoformat(str(row["quote_at"]))
                price = float(row["execution_price_0901"])
                volume = float(row["tick_volume_units_0901"])
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"invalid retained Shioaji 09:01 evidence: {day}:{row['symbol']}"
                ) from exc
            window_start = datetime.combine(
                date.fromisoformat(day), time(9, 0), tzinfo=opening.tzinfo
            )
            window_end = datetime.combine(
                date.fromisoformat(day), time(9, 1), tzinfo=opening.tzinfo
            )
            if (
                not window_start <= opening < window_end
                or not opening <= closing <= window_end
                or quote_at != window_end
                or not math.isfinite(price)
                or price <= 0.0
                or not math.isfinite(volume)
                or volume <= 0.0
            ):
                raise ValueError(
                    f"invalid retained Shioaji 09:01 evidence: {day}:{row['symbol']}"
                )
            if retained_opening_bars is not None:
                retained_opening_bars.append(
                    {
                        "symbol": str(row["symbol"]),
                        "minute_key": f"{day}T09:01",
                        "high": price,
                        "low": price,
                        "volume_shares": volume * shioaji_volume_multiplier,
                    }
                )
            continue
        source = str(_entry_source_path(source_identity))
        source_path = Path(source)
        is_symbol_chunk = (
            "minute_chunks" in source_path.parts
            and source_path.parent.name == row["symbol"]
        )
        is_daily_research_partition = (
            "research_dataset" in source_path.parts
            and source_path.name == "data.parquet"
        )
        if not (is_symbol_chunk or is_daily_research_partition):
            raise ValueError(f"entry source symbol mismatch: {row['symbol']}:{source}")
        result[source].add(date.fromisoformat(day))
    return result


def _verify_calendar_coverage(rebuild: dict, dates: list[str], *, prefix: bool) -> dict:
    """An internally consistent truncated replay is not a full-range result."""
    proof = rebuild.get("official_session_calendar") or {}
    path = Path(str(proof.get("path") or ""))
    if not path.is_absolute() or proof.get("current_open_session_appended"):
        raise ValueError("completed-history audit requires a pinned completed-session calendar")
    start, end = date.fromisoformat(proof["start_date"]), date.fromisoformat(proof["end_date"])
    expected, digest = _validated_taiex_session_dates(path.parent, start, end)
    if (path.name != "twse_taiex_ohlc.parquet"
            or len(expected) != proof.get("session_count")):
        raise ValueError("official replay calendar identity or session count changed")
    expected_dates = sorted(day.isoformat() for day in expected)
    wanted = expected_dates[:len(dates)] if prefix else expected_dates
    if dates != wanted:
        raise ValueError("replay omitted or added official sessions within its requested range")
    # The canonical calendar is append-only in normal operation. A later
    # completed session changes the whole-file SHA even though the replay's
    # bounded session set is unchanged. Verify the exact requested semantic
    # prefix and retain both hashes instead of treating a valid later append as
    # historical corruption.
    return {
        "path": str(path),
        "sha256": digest,
        "replay_source_sha256": proof.get("sha256"),
        "current_source_sha256": digest,
        "source_file_unchanged": digest == proof.get("sha256"),
        "identity_contract": (
            "exact_file_sha256"
            if digest == proof.get("sha256")
            else "receipt_validated_exact_requested_session_set_after_append"
        ),
        "requested_sessions": len(expected_dates),
        "verified_sessions": len(dates),
    }


def _verified_action_sources(path: Path, start: str, end: str) -> tuple:
    """Audit source terms independently of quantities already in the ledger."""
    path = path.resolve(strict=True)
    ent = path.parent / "tw_corporate_action_entitlements.parquet"
    share = path.parent / "tw_share_replacement_reference.parquet"
    files = [p for source in (path, ent, share) for p in (source, source.with_suffix(".summary.json"))]
    hashes = {str(p): _sha256(p) for p in files}
    reference = _load_corporate_action_reference(_CorporateActionReferencePaths(
        path, path.with_suffix(".summary.json"), ent, ent.with_suffix(".summary.json")))
    first, last = np.datetime64(start), np.datetime64(end)
    if (reference.coverage_start > first or reference.coverage_end < last
            or reference.exact_coverage_start is None or reference.exact_coverage_end is None
            or reference.exact_coverage_start > first or reference.exact_coverage_end < last):
        raise ValueError("corporate-action source does not cover the audited history")
    replacements = load_share_replacements(path.parent, required_start=date.fromisoformat(start),
                                            required_end=date.fromisoformat(end))
    return reference, replacements, hashes


def _claim_matches_source(claim: dict, reference) -> bool:
    terms = (reference.exact_cash_terms_by_symbol or {}).get(claim["symbol"])
    if terms is None:
        return False
    matches = np.flatnonzero(terms[0] == np.datetime64(claim["ex_date"]))
    return bool(len(matches) == 1
                and float(terms[1][matches[0]]) == float(claim["cash_per_share"])
                and str(terms[2][matches[0]]) == claim["payment_date"])


def audit(state_dir: Path, *, verify_sources: bool = True, completed_prefix: bool = False) -> dict:
    names = ("state.json", "rebuild_receipt.json", "fills.jsonl", "marks.jsonl", "orders.jsonl")
    before = {name: _sha256(state_dir / name) for name in names}
    state, rebuild = _read_json(state_dir / "state.json"), _read_json(state_dir / "rebuild_receipt.json")
    full_target_counterfactual = (
        (rebuild.get("replay_contract") or {}).get("entry")
        == "retrospective_prior_paper_fill_else_09_01_minute_price_full_target_no_liquidity_claim_v1"
    )
    prior_manifest: dict[str, dict] = {}
    if full_target_counterfactual and verify_sources:
        from scripts.stage_tw_day_trade_prior_paper_source import verify
        prior_manifest = verify(state_dir, rebuild)
    sessions = rebuild["sessions"]
    incomplete = [i for i, row in enumerate(sessions)
                  if str((row.get("close") or {}).get("status", "")).startswith("blocked")]
    if incomplete:
        if not completed_prefix:
            raise ValueError("replay contains an incomplete session; full-range audit refused")
        sessions = sessions[:incomplete[0]]
    dates = [row["session_date"] for row in sessions]
    if dates != sorted(set(dates)) or not dates:
        raise ValueError("replay dates are absent or duplicated")
    calendar_check = (_verify_calendar_coverage(rebuild, dates, prefix=bool(incomplete))
                      if verify_sources else None)
    fills, marks = _read_jsonl(state_dir / "fills.jsonl"), _read_jsonl(state_dir / "marks.jsonl")
    order_sides = {}
    for order in _read_jsonl(state_dir / "orders.jsonl"):
        key, side = (order["market"], order["order_id"]), order.get("side")
        if side not in {"buy", "sell", "buy_to_cover", "sell_short"}:
            raise ValueError(f"order has no explicit transaction side: {key}")
        if key in order_sides and order_sides[key] != side:
            raise ValueError(f"conflicting transaction sides: {key}")
        order_sides[key] = side
    for fill in fills:
        side = order_sides[(fill["market"], fill["order_id"])]
        if fill.get("side", side) != side:
            raise ValueError("fill and order transaction sides disagree")
        fill["side"] = side
    if any(str(row.get("session_date", "")) > dates[-1] for row in [*fills, *marks]):
        raise ValueError("failed session has partial ledger writes; prefix audit requires reconciliation")
    _, minute_stats = validate_existing_strategy_marks(marks, start=date.fromisoformat(dates[0]), end=date.fromisoformat(dates[-1]))
    expected_pairs = {(market, day) for market in state["modes"] for day in dates}
    observed_pairs = {(row["market"], row["session_date"]) for row in marks}
    if observed_pairs != expected_pairs:
        raise ValueError(f"market/session minute coverage mismatch: missing={sorted(expected_pairs-observed_pairs)[:10]}")
    errors = []
    max_error = 0.
    def equal(a, b, message):
        nonlocal max_error
        diff = abs(float(a) - float(b))
        max_error = max(max_error, diff)
        if not math.isfinite(diff) or diff > 1e-5:
            errors.append(f"{message}: {a} != {b}")
    by_market = defaultdict(list)
    for fill in fills:
        by_market[fill["market"]].append(fill)
    accounts, action_sources, action_source_hashes = {}, {}, {}
    for market, mode in state["modes"].items():
        if mode.get("margin_carry_contract") != MARGIN_CARRY_CONTRACT or mode["session_date"] != dates[-1]:
            errors.append(f"{market}: wrong carry contract or unfinished session")
        actions = mode.get("share_replacement_ledger") or []
        if len({a["action_id"] for a in actions}) != len(actions):
            errors.append(f"{market}: duplicate share replacement")
        action_map = {a["action_id"]: a for a in actions}
        if verify_sources:
            source_path = mode.get("margin_corporate_action_reference_path")
            if not source_path:
                raise ValueError(f"{market}: carried-history audit requires corporate-action sources")
            if source_path not in action_sources:
                action_sources[source_path] = _verified_action_sources(Path(source_path), dates[0], dates[-1])
            reference, replacements, hashes = action_sources[source_path]
            action_source_hashes.update(hashes)
            for event in replacements:
                halt, resume = str(event["suspension_date"]), str(event["resume_date"])
                affected = [f for f in by_market[market] if f["symbol"] == event["symbol"]]
                if any(halt <= f["session_date"] < resume for f in affected):
                    errors.append(f"{market}:{event['symbol']}: fill during source-proven suspension")
                if not dates[0] <= resume <= dates[-1]:
                    continue
                held = defaultdict(int)
                prior_events = sorted([*affected, *(a | {"_share_action": True} for a in actions
                                                    if a["symbol"] == event["symbol"])],
                                      key=lambda row: row["recorded_at"])
                for item in prior_events:
                    if item["recorded_at"][:10] >= halt:
                        continue
                    key = item["position_id"]
                    if item.get("_share_action"):
                        held[key] = int(item["new_signed_shares"])
                    elif _is_entry_inventory_fill(item):
                        held[key] += int(item["quantity"]) * (1 if item["side"] in {"buy", "buy_to_cover"} else -1)
                    else:
                        held[key] -= int(item["quantity"]) * (1 if held[key] > 0 else -1)
                for key, quantity in held.items():
                    if quantity and f"{key}:{resume}:share_replacement" not in action_map:
                        errors.append(f"{market}:{key}: source-proven share replacement omitted")
            for action in actions:
                source = load_share_replacements(Path(action["source"]).parent)
                matched = [e for e in source if e["symbol"] == action["symbol"]
                           and str(e["resume_date"]) == action["effective_date"]]
                if len(matched) != 1:
                    errors.append(f"{market}: share replacement missing source")
                    continue
                equal(action["ratio"], matched[0]["new_shares_per_1000_old"] / 1000., "share replacement ratio")
                equal(action["cash_per_old_share"], matched[0]["cash_return_per_old_share"], "share replacement cash")
                claims_for_action = [c for c in mode.get("corporate_action_ledger", []) if c.get("share_action_id") == action["action_id"]]
                if action["cash_per_old_share"] and (len(claims_for_action) != 1 or claims_for_action[0]["payment_date"] != str(matched[0]["cash_payment_date"])):
                    errors.append(f"{market}: capital-return payment differs from source")
        events = sorted([*by_market[market], *(a | {"_share_action": True} for a in actions)],
                        key=lambda row: (row["recorded_at"][:16], not row.get("_share_action", False), row["recorded_at"]))
        entries, inventory = {}, defaultdict(int)
        market_marks = sorted((row for row in marks if row["market"] == market), key=lambda row: row["minute"])
        costs = mode.get("carry_cost_ledger") or []
        claims = mode.get("corporate_action_ledger") or []
        if len({x["cost_id"] for x in costs}) != len(costs) or len({x["claim_id"] for x in claims}) != len(claims):
            errors.append(f"{market}: duplicate cost or distribution claim")
        realized, index = 0., 0
        open_ids = set()
        cash_by_day = {day: (
            sum(float(c["amount_twd"]) for c in claims if c["ex_date"] <= day),
            sum(float(c["amount_twd"]) for c in claims if c["ex_date"] <= day and c["payment_date"] <= day)) for day in dates}
        costs_by_day = {day: (
            sum(float(c["amount_twd"]) for c in costs if c["date"] < day or
                (c["date"] == day and c["kind"] != "short_conversion_tax_and_handling")),
            sum(float(c["amount_twd"]) for c in costs if c["date"] == day and c["kind"] == "short_conversion_tax_and_handling")) for day in dates}
        for mark in market_marks:
            minute = mark["minute"]
            while index < len(events) and events[index]["recorded_at"][:16] <= minute[:16]:
                fill = events[index]
                if fill.get("_share_action"):
                    key = fill["position_id"]
                    equal(inventory[key], fill["old_signed_shares"], f"{key}: pre-replacement inventory")
                    equal(entries[key]["price"], fill["old_entry_price"], f"{key}: pre-replacement basis")
                    equal(fill["new_signed_shares"], fill["old_signed_shares"] * fill["ratio"], f"{key}: share conservation")
                    equal(fill["old_signed_shares"] * fill["old_entry_price"],
                          fill["new_signed_shares"] * fill["new_entry_price"], f"{key}: investment principal conservation")
                    inventory[key] = int(fill["new_signed_shares"])
                    entries[key] = entries[key] | {"price": fill["new_entry_price"]}
                    index += 1
                    continue
                key, quantity = fill["position_id"], int(fill["quantity"])
                if quantity <= 0 or fill["quantity"] != quantity or (quantity % 1000 and not (
                        fill.get("odd_lot_execution_policy") == ODD_LOT_BOARD_PRICE
                        and mode.get("odd_lot_execution_policy") == ODD_LOT_BOARD_PRICE)):
                    errors.append(f"{key}: invalid whole-lot or unauthorized odd-lot fill")
                if fill["purpose"] == "entry":
                    if key in entries:
                        errors.append(f"{key}: duplicate entry identity")
                    entries[key] = fill
                    inventory[key] += quantity if fill["side"] in {"buy", "buy_to_cover"} else -quantity
                elif fill["purpose"] == "entry_completion":
                    disclosure = fill.get("counterfactual_paper_completion") or {}
                    minute_sweep_completion = False
                    try:
                        fill_at = datetime.fromisoformat(str(fill.get("fill_at")))
                        minute_volume_lots = float(
                            fill.get("observed_minute_volume_lots")
                        )
                        minute_capacity = int(
                            math.floor(minute_volume_lots * 0.5)
                        ) * 1000
                        minute_sweep_completion = bool(
                            key in entries
                            and fill.get("fill_contract")
                            == REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE
                            and fill.get("entry_fill_policy")
                            == ENTRY_FILL_POLICY_0901_MINUTE_PRICE
                            and fill.get("counterfactual_minute_sweep_fill") is True
                            and fill.get("simulation_replay") is True
                            and math.isfinite(float(fill["price"]))
                            and float(fill["price"]) > 0.0
                            and math.isfinite(minute_volume_lots)
                            and minute_volume_lots > 0.0
                            and quantity <= minute_capacity
                            and time(9, 1) < fill_at.timetz().replace(tzinfo=None)
                            < time(13, 20)
                        )
                    except (TypeError, ValueError, OverflowError):
                        minute_sweep_completion = False
                    paper_completion = bool(
                        key in entries
                        and fill.get("fill_contract") == PAPER_COMPLETION_CONTRACT
                        and disclosure.get("contract") == PAPER_COMPLETION_CONTRACT
                        and disclosure.get("full_quantity_is_user_assumption") is True
                        and disclosure.get("same_price_as_original_entry") is True
                        and disclosure.get("broker_fill") is False
                        and fill.get("broker_fill") is False
                        and float(fill["price"])
                        == float(entries.get(key, {}).get("price") or 0.0)
                    )
                    if not (minute_sweep_completion or paper_completion):
                        errors.append(f"{key}: unverified paper entry completion")
                    else:
                        direction = 1 if inventory[key] > 0 else -1
                        if direction * (1 if fill["side"] in {"buy", "buy_to_cover"} else -1) < 0:
                            errors.append(f"{key}: entry completion changes inventory direction")
                        if minute_sweep_completion:
                            old_quantity = abs(inventory[key])
                            entries[key] = entries[key] | {
                                "price": (
                                    old_quantity * float(entries[key]["price"])
                                    + quantity * float(fill["price"])
                                )
                                / (old_quantity + quantity)
                            }
                        inventory[key] += direction * quantity
                else:
                    entry = entries[key]
                    direction = 1 if inventory[key] > 0 else -1
                    if abs(inventory[key]) < quantity:
                        errors.append(f"{key}: exit exceeds inventory")
                    equal(fill["gross_pnl_twd"], direction * quantity * (float(fill["price"]) - float(entry["price"])), f"{key}: gross PnL")
                    equal(fill["net_pnl_twd"], float(fill["gross_pnl_twd"]) - float(fill["entry_fee_allocated_twd"]) - float(fill["fee_and_tax_twd"]), f"{key}: net PnL")
                    inventory[key] -= direction * quantity
                    realized += float(fill["net_pnl_twd"])
                if fill.get("synthetic_terminal_ledger") or fill.get("synthetic_fallback_fill"):
                    errors.append(f"{key}: fabricated terminal/tick fill")
                if inventory[key]:
                    open_ids.add(key)
                else:
                    open_ids.discard(key)
                index += 1
            equal(mark["cumulative_realized_net_pnl_twd"], realized, f"{market}:{minute}: realized")
            equal(mark["open_position_count"], len(open_ids), f"{market}:{minute}: inventory count")
            day = mark["session_date"]
            earned, paid = cash_by_day[day]
            charges, close_charges = costs_by_day[day]
            if minute[11:16] == "13:30":
                charges += close_charges
            equal(mark.get("cumulative_carry_cost_twd", 0), charges, f"{market}:{minute}: financing costs")
            equal(mark.get("cumulative_corporate_action_net_twd", 0), earned, f"{market}:{minute}: earned distributions")
            equal(mark.get("corporate_action_cash_net_twd", 0), paid, f"{market}:{minute}: paid distributions")
        for claim in claims:
            key = claim["position_id"]
            if claim.get("share_action_id"):
                action = action_map[claim["share_action_id"]]
                equal(claim["entitled_signed_shares"], action["old_signed_shares"], f"{key}: capital-return entitlement")
                equal(claim["cash_per_share"], action["cash_per_old_share"], f"{key}: capital-return rate")
                equal(claim["amount_twd"], action["old_signed_shares"] * action["cash_per_old_share"], f"{key}: signed capital return")
                continue
            held = 0
            for fill in events:
                event_day = fill.get("effective_date") if fill.get("_share_action") else fill["session_date"]
                if fill["position_id"] != key or event_day >= claim["ex_date"]:
                    continue
                if fill.get("_share_action"):
                    held = int(fill["new_signed_shares"])
                    continue
                if _is_entry_inventory_fill(fill):
                    held += int(fill["quantity"]) * (1 if fill["side"] == "buy" else -1)
                else:
                    held -= int(fill["quantity"]) * (1 if held > 0 else -1)
            equal(claim["entitled_signed_shares"], held, f"{key}: ex-date entitlement quantity")
            equal(claim["amount_twd"], held * float(claim["cash_per_share"]), f"{key}: exact signed cash")
            if verify_sources and not _claim_matches_source(claim, reference):
                errors.append(f"{market}:{claim['claim_id']}: cash amount/payment differs from verified source")
        for position in mode["positions"].values():
            equal(position["signed_shares"], inventory[position["position_id"]], f"{market}: final inventory")
        equal(mode["total_equity_twd"], market_marks[-1]["total_equity_twd"], f"{market}: final NAV")
        accounts[market] = {"fills": len(by_market[market]), "claims": len(claims), "share_replacements": len(actions), "carry_costs": len(costs),
                            "ending_equity_twd": mode["total_equity_twd"], "open_positions": sum(q != 0 for q in inventory.values())}
    source_days = defaultdict(set)
    filled_symbol_days = {(row["symbol"], row["session_date"]) for row in fills}
    minute_source_signatures = {}
    retained_opening_bars: list[dict] = []
    opening_fill_symbols_by_day = defaultdict(set)
    for row in fills:
        if (not row.get("prior_paper_fill_reused")
                and str(row.get("recorded_at", ""))[11:16] == "09:01"):
            opening_fill_symbols_by_day[row["session_date"]].add(row["symbol"])
    for session in sessions:
        day = session["session_date"]
        for name in (session.get("intraday_replay") or {}).get("source_counts", {}):
            source_days[name].add(date.fromisoformat(day))
        # A carried position fully reduced at 09:01 is absent from the later
        # intraday inventory scan, but its exit still needs source validation.
        entry_local = ((session.get("historical_entry_books") or {}).get("local") or {})
        for source in entry_local.get("source_counts", {}):
            # Entry provenance includes the method tag before its absolute
            # file path. It is not itself a filesystem path.
            path = _entry_source_path(source)
            if (path.parent.name, day) in filled_symbol_days:
                source_days[str(path)].add(date.fromisoformat(day))
        retained_sources = _retained_entry_book_source_days(
            state_dir,
            session,
            opening_fill_symbols_by_day[day],
            minute_source_signatures,
            retained_opening_bars,
        )
        for name, source_dates in retained_sources.items():
            source_days[name].update(source_dates)
    if verify_sources and not full_target_counterfactual and any(
        row.get("prior_paper_fill_reused") for row in fills
    ):
        errors.append("prior paper-fill reuse outside explicit full-target contract")
    if verify_sources and full_target_counterfactual:
        for fill in fills:
            if not fill.get("prior_paper_fill_reused"):
                continue
            key_prior = "|".join((str(fill["session_date"]),
                                  str(fill["market"]), str(fill["symbol"])))
            prior = prior_manifest.get(key_prior)
            if (not isinstance(prior, dict)
                    or str(fill.get("fill_at")) != str(prior.get("fill_at"))
                    or not math.isclose(float(fill["price"]), float(prior.get("price") or 0),
                                        rel_tol=0.0, abs_tol=1e-8)):
                errors.append(f"prior paper fill has no matching source: {key_prior}")
    if verify_sources and any(not row.get("prior_paper_fill_reused") for row in fills) and not source_days:
        errors.append("filled replay has no retained minute source receipts")
    if verify_sources and fills:
        needs = pl.DataFrame(
            [{"symbol": row["symbol"], "minute_key": row["recorded_at"][:16]}
             for row in fills if not row.get("prior_paper_fill_reused")],
            schema={"symbol": pl.Utf8, "minute_key": pl.Utf8},
        ).unique()
        source_bars = []
        for name, days in source_days.items():
            path = Path(name)
            if "minute_chunks" not in path.parts:
                if "research_dataset" in path.parts and path.name == "data.parquet":
                    manifest_path = path.parent.parent / "manifest.json"
                    try:
                        for source in (path, manifest_path):
                            stat = source.stat()
                            signature = (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
                            if minute_source_signatures.setdefault(str(source), signature) != signature:
                                raise RuntimeError(f"minute source changed during audit: {source}")
                        manifest = _read_json(manifest_path)
                        relative = str(path.relative_to(path.parent.parent))
                        entry = next(row for row in manifest.get("partitions") or []
                                     if row.get("output") == relative)
                        if (entry.get("status") != "ok"
                                or entry.get("trade_date") not in {d.isoformat() for d in days}
                                or _sha256(path) != entry.get("output_sha256")):
                            raise ValueError("research partition hash/date mismatch")
                        schema = pl.read_parquet_schema(path)
                        volume = (pl.col("volume_shares") if "volume_shares" in schema else
                                  pl.col("Volume") * (pl.col("contract_unit") if "contract_unit" in schema else 1000))
                        source_bars.append(pl.scan_parquet(path)
                            .select(pl.col("symbol"), pl.col("ts").dt.strftime("%Y-%m-%dT%H:%M").alias("minute_key"),
                                    pl.col("High").cast(pl.Float64).alias("high"),
                                    pl.col("Low").cast(pl.Float64).alias("low"),
                                    volume.cast(pl.Float64).alias("volume_shares"))
                            .join(needs.lazy(), on=["symbol", "minute_key"], how="semi").collect())
                        continue
                    except (OSError, ValueError, StopIteration, KeyError) as exc:
                        errors.append(f"invalid research minute source {name}: {exc}")
                        continue
                errors.append(f"minute source has no checked collector receipt: {name}")
                continue
            start, end = map(date.fromisoformat, path.stem.split("_"))
            receipt_path = path.with_suffix(".receipt.json")
            proof = _read_json(receipt_path)
            inputs = [path, receipt_path, *[Path(row["path"]) for row in proof.get("raw_tick_sources", [])]]
            for source in inputs:
                stat = source.stat()
                signature = (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
                if minute_source_signatures.setdefault(str(source), signature) != signature:
                    raise RuntimeError(f"minute source changed during audit: {source}")
            if not minute_receipt_valid(receipt_path, symbol=path.parent.name, start=start, end=end, required_dates=days):
                errors.append(f"invalid minute source receipt: {name}")
                continue
            schema = pl.read_parquet_schema(path)
            volume = (pl.col("volume_shares") if "volume_shares" in schema else
                      pl.col("Volume") * (pl.col("contract_unit") if "contract_unit" in schema else 1000))
            source_bars.append(pl.scan_parquet(path).filter(pl.col("date").is_in(sorted(days)))
                .select(pl.col("symbol"), pl.col("ts").dt.strftime("%Y-%m-%dT%H:%M").alias("minute_key"),
                        pl.col("High").cast(pl.Float64).alias("high"), pl.col("Low").cast(pl.Float64).alias("low"),
                        volume.cast(pl.Float64).alias("volume_shares"))
                .join(needs.lazy(), on=["symbol", "minute_key"], how="semi").collect())
        if source_bars or retained_opening_bars:
            retained_by_key = {
                (row["symbol"], row["minute_key"]): row
                for row in retained_opening_bars
            }
            if len(retained_by_key) != len(retained_opening_bars):
                errors.append("retained opening evidence has duplicate symbol/minute")
            bars = {}
            if source_bars:
                frame = pl.concat(source_bars).unique()
                duplicate_keys = {
                    (row["symbol"], row["minute_key"])
                    for row in frame.filter(
                        pl.struct("symbol", "minute_key").is_duplicated()
                    ).select("symbol", "minute_key").iter_rows(named=True)
                }
                unresolved_duplicates = duplicate_keys - set(retained_by_key)
                if unresolved_duplicates:
                    errors.append("retained minute sources disagree on fill OHLCV")
                bars = {
                    (row["symbol"], row["minute_key"]): row
                    for row in frame.to_dicts()
                    if (row["symbol"], row["minute_key"])
                    not in retained_by_key
                }
            bars.update(retained_by_key)
            usage = defaultdict(int)
            for fill in fills:
                if fill.get("prior_paper_fill_reused"):
                    continue
                key = (fill["symbol"], fill["recorded_at"][:16])
                bar = bars.get(key)
                if bar is None:
                    errors.append(f"fill minute has no verified source: {key}")
                    continue
                price = float(fill["price"])
                if not bar["low"] - 1e-7 <= price <= bar["high"] + 1e-7:
                    errors.append(f"fill outside source OHLC range: {key}: {price}")
                usage[(fill["market"], *key)] += int(fill["quantity"])
            for (market, symbol, minute), quantity in usage.items():
                capacity = math.floor(bars[(symbol, minute)]["volume_shares"] * .5 / 1000) * 1000
                if not full_target_counterfactual and quantity > capacity:
                    errors.append(f"shared minute capacity exceeded: {market}:{symbol}:{minute}: {quantity}>{capacity}")
    if before != {name: _sha256(state_dir / name) for name in names}:
        raise RuntimeError("replay changed during audit; rerun on completed candidate")
    if calendar_check and _sha256(Path(calendar_check["path"])) != calendar_check["sha256"]:
        raise RuntimeError("calendar changed during replay audit")
    if any(_sha256(Path(name)) != digest for name, digest in action_source_hashes.items()):
        raise RuntimeError("corporate-action source changed during replay audit")
    for name, signature in minute_source_signatures.items():
        stat = Path(name).stat()
        if (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns) != signature:
            raise RuntimeError(f"minute source changed during audit: {name}")
    return {"contract": "carried_inventory_fill_cash_cost_minute_reconciliation_v1", "passed": not errors,
            "scope": "completed_prefix_only" if incomplete else "requested_range",
            "full_requested_range_passed": not errors and not incomplete and verify_sources,
            "sources_verified": bool(verify_sources),
            "incomplete_session": rebuild["sessions"][incomplete[0]]["session_date"] if incomplete else None,
            "session_count": len(dates), "start_date": dates[0], "end_date": dates[-1],
            "official_calendar_verification": calendar_check,
            "minute_stats": minute_stats, "accounts": accounts, "max_accounting_error_twd": max_error,
            "source_files_checked": len(source_days) if verify_sources else 0,
            "source_hashes": before, "corporate_action_source_hashes": action_source_hashes,
            "minute_source_signatures": minute_source_signatures,
            "errors": errors[:100], "error_count": len(errors)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--completed-prefix", action="store_true",
                        help="Audit only the untouched completed prefix; never a promotion receipt.")
    args = parser.parse_args()
    try:
        result = audit(args.state_dir, completed_prefix=args.completed_prefix)
    except (OSError, ValueError, KeyError, RuntimeError) as exc:
        result = {"passed": False, "full_requested_range_passed": False,
                  "error": f"{type(exc).__name__}: {exc}", "state_dir": str(args.state_dir)}
    _atomic_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result["full_requested_range_passed"] else 2)


if __name__ == "__main__":
    main()

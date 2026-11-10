#!/usr/bin/env python3
"""Read-only, fill-to-inventory-to-minute-NAV audit of a carried paper replay."""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, time, timedelta
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
    _replay_minute_source_hashes, _replay_minute_symbol_sources,
)
from scripts.complete_tw_day_trade_paper_entry import CONTRACT as PAPER_COMPLETION_CONTRACT
from stockagent.live.tw_day_trade_simulation import (
    ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
    HISTORICAL_BRACKET_FULL_CROSS_CONTRACT,
    HISTORICAL_OPEN_FALLBACK_CONTRACT,
    MARGIN_CARRY_CONTRACT,
    LEGACY_REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL,
    REPLAY_FULL_COUNTERFACTUAL_CONTRACTS,
    REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
)
from downloader.download_shioaji_tw_minute_kbars import minute_receipt_valid
from downloader.download_tw_public_data import _validated_taiex_session_dates
from downloader.stock_volume_units import with_stock_share_volume
from stockagent.live.tw_share_replacement import ODD_LOT_BOARD_PRICE, load_share_replacements
from stockagent.data.panel import _CorporateActionReferencePaths, _load_corporate_action_reference


def _verify_official_open_fallback_fill(fill: dict, session: dict, rebuild: dict, *, state_dir: Path) -> dict[str, str]:
    """Independently validate the disclosed daily-open paper assumption."""
    from scripts.settle_tw_day_trade_official_close import official_closes
    from stockagent.live.quote_provider import observed_0901_minute_volume_lots

    proof = fill.get("historical_open_fallback_evidence") or {}
    day, symbol = str(fill.get("session_date") or ""), str(fill.get("symbol") or "")
    mode_receipts = [row for row in session.get("modes", []) if row.get("market") == fill.get("market")]
    quality = (mode_receipts[0].get("entry_price_quality") or {}) if len(mode_receipts) == 1 else {}
    if (rebuild.get("provisional_minute_source_coverage") is not True
            or rebuild.get("historical_open_fallback_assumption") != HISTORICAL_OPEN_FALLBACK_CONTRACT
            or session.get("session_date") != day or fill.get("purpose") != "entry"
            or fill.get("simulation_replay") is not True or fill.get("simulation_only") is not True
            or fill.get("fill_contract") != HISTORICAL_OPEN_FALLBACK_CONTRACT
            or fill.get("historical_open_fallback_fill") is not True
            or fill.get("counterfactual_0901_price_fill") is not False
            or fill.get("quote_at") is not None
            or fill.get("historical_source_quote_at") is not None
            or proof.get("contract") != HISTORICAL_OPEN_FALLBACK_CONTRACT
            or proof.get("session_date") != day or proof.get("symbol") != symbol
            or proof.get("simulation_only") is not True or proof.get("broker_fill") is not False
            or proof.get("exchange_fill") is not False or proof.get("opening_time_known") is not False
            or proof.get("full_quantity_assumed") is not True
            or proof.get("proof_clock_basis") != "host_wall_clock"
            or fill.get("recorded_at_basis") != "counterfactual_effective_clock_not_original_publication"
            or fill.get("replay_generated_at") != proof.get("proof_created_at")
            or symbol not in quality.get("official_open_fallback_symbols", [])
            or symbol in (session.get("canonical_open") or {}).get("official_no_trade_print_symbols", [])
            or symbol in (session.get("historical_entry_books") or {}).get("verified_no_trade_through_0901_symbols", [])):
        raise ValueError(f"invalid official-open fallback disclosure: {day}/{symbol}")
    try:
        stamp = datetime.fromisoformat(str(fill["fill_at"]))
        created = datetime.fromisoformat(str(proof["proof_created_at"]))
        quantity = int(fill["quantity"])
        price = float(fill["price"])
        if (stamp.date().isoformat() != day or stamp.time() != time(9, 1)
                or stamp.utcoffset() != timedelta(hours=8) or quantity <= 0 or quantity % 1000
                or created.utcoffset() != timedelta(hours=8)
                or created < datetime.combine(stamp.date(), time(13, 30), tzinfo=stamp.tzinfo)
                or quantity != fill["quantity"] or not math.isfinite(price) or price <= 0
                or not math.isclose(price, float(proof["price"]), rel_tol=0, abs_tol=1e-8)):
            raise ValueError
    except (TypeError, ValueError, KeyError, OverflowError):
        raise ValueError(f"invalid official-open fallback values: {day}/{symbol}") from None
    book = session.get("historical_entry_books") or {}
    book_path = (state_dir / "replay_entry_books" / f"{day}.parquet").resolve(strict=True)
    if _sha256(book_path) != book.get("sha256"):
        raise ValueError("fallback retained entry book hash mismatch")
    frame = pl.read_parquet(book_path).filter(pl.col("symbol").cast(pl.String) == symbol)
    if frame.height > 1:
        raise ValueError("fallback retained entry book has duplicate symbols")
    if frame.height:
        observed = frame.row(0, named=True)
        volume = observed.get("tick_volume_units_0901")
        if volume is not None:
            try:
                valid_volume = math.isfinite(float(volume)) and float(volume) > 0
            except (TypeError, ValueError):
                valid_volume = False
            if not valid_volume:
                raise ValueError("fallback cannot overturn explicit zero or invalid volume")
        observed_price = observed.get("execution_price_0901")
        try:
            has_price = math.isfinite(float(observed_price)) and float(observed_price) > 0
        except (TypeError, ValueError):
            has_price = False
        if has_price and observed_0901_minute_volume_lots(observed) > 0:
            raise ValueError("fallback cannot override an observed 09:01 price and volume")
    path = Path(str(proof.get("source_path") or "")).resolve(strict=True)
    digest = str(proof.get("source_sha256") or "")
    pins = (session.get("canonical_open") or {}).get("official_open_fallback_sources") or []
    if not any(row.get("path") == str(path) and row.get("sha256") == digest for row in pins):
        raise ValueError("fallback source is not pinned by its session")
    exchange = path.parent.name.removesuffix("_daily_ohlcv")
    if (exchange not in {"twse", "tpex"} or path.name != f"{day}.json"
            or path.parent.parent.name != digest or path.parent.parent.parent.name != ".close_evidence"
            or _sha256(path) != digest):
        raise ValueError("fallback official source namespace or hash mismatch")
    official, _ = official_closes(path.parent.parent, date.fromisoformat(day), exchanges=(exchange,))
    row = official.get(symbol) or {}
    raw = row.get("official_row") or {}
    try:
        opening = float(str(raw.get("開盤價", raw.get("開盤"))).replace(",", ""))
        volume = float(str(raw.get("成交股數", raw.get("成交量"))).replace(",", ""))
    except (TypeError, ValueError):
        raise ValueError("fallback official row lacks valid open/volume") from None
    if (not math.isfinite(volume) or volume <= 0 or not volume.is_integer()
            or not math.isclose(opening, price, rel_tol=0, abs_tol=1e-8)
            or row.get("source") != proof.get("source")):
        raise ValueError("fallback differs from official traded open")
    return {str(path): digest, str(book_path): book["sha256"]}


def _verified_full_cross_assumption(fill: dict, bar: dict, entry: dict, rebuild: dict) -> bool:
    """Audit the opt-in exception against source OHLC and the original bracket."""
    evidence = fill.get("bracket_cross_evidence") or {}
    sessions = [row for row in rebuild.get("sessions", []) if row.get("session_date") == fill.get("session_date")]
    session_policy = (sessions[0].get("intraday_replay") or {}) if len(sessions) == 1 else {}
    if (rebuild.get("historical_bracket_cross_assumption") != HISTORICAL_BRACKET_FULL_CROSS_CONTRACT
            or session_policy.get("historical_bracket_cross_assumption") != HISTORICAL_BRACKET_FULL_CROSS_CONTRACT
            or session_policy.get("full_bracket_fill_is_assumed_not_broker_or_exchange_fill") is not True
            or fill.get("fill_contract") != HISTORICAL_BRACKET_FULL_CROSS_CONTRACT
            or evidence.get("contract") != HISTORICAL_BRACKET_FULL_CROSS_CONTRACT
            or evidence.get("simulation_only") is not True or evidence.get("exchange_fill") is not False
            or evidence.get("broker_fill") is not False or fill.get("simulation_replay") is not True
            or fill.get("quantity") != fill.get("requested_quantity") or fill.get("remaining_quantity") != 0
            or evidence.get("symbol") != fill.get("symbol")):
        return False
    side = "long" if fill.get("side") == "sell" else "short" if fill.get("side") == "buy_to_cover" else ""
    bracket = evidence.get("bracket")
    if not side or evidence.get("side") != side or bracket not in {"stop_loss", "take_profit"} or not str(fill.get("purpose") or "").startswith(bracket):
        return False
    try:
        trigger = float(entry["stop_trigger_price" if bracket == "stop_loss" else "take_profit_price"])
        low, high, price = float(bar["low"]), float(bar["high"]), float(fill["price"])
        volume = float(bar["volume_shares"])
        stamp = datetime.fromisoformat(str(fill["fill_at"]))
        if (not all(math.isfinite(x) for x in (trigger, low, high, price, volume))
                or volume <= 0 or not math.isclose(trigger, float(evidence["trigger_price"]), abs_tol=1e-9, rel_tol=0)
                or not time(9, 1) < stamp.time() <= time(13, 24)
                or (bracket == "take_profit" and stamp.time() >= time(13, 20))):
            return False
    except (KeyError, TypeError, ValueError):
        return False
    crossed = ((low < trigger if side == "long" else high > trigger) if bracket == "stop_loss"
               else (high > trigger if side == "long" else low < trigger))
    if not crossed:
        return False
    if bracket == "stop_loss":
        return math.isclose(price, low if side == "long" else high, abs_tol=1e-9, rel_tol=0)
    return low <= price <= high and (price >= trigger if side == "long" else price <= trigger)


def _verify_provisional_nontrade_event(event: dict, session: dict, *, mode: dict,
                                       entry: dict, verify_sources: bool,
                                       allow_daily_close_reference: bool = False) -> dict:
    """Independently reconcile a marked non-fill against its exact sources."""
    from scripts.settle_tw_day_trade_official_close import official_closes

    day, symbol = str(event.get("session_date") or ""), str(event.get("symbol") or "")
    settlement = (session.get("close") or {}).get("nontrade_settlement") or {}
    ref, official = event.get("reference") or {}, event.get("official_close_evidence") or {}
    daily = ref.get("source_clock_basis") == "official_daily_close_last_trade_time_unknown"
    if (not verify_sources or event.get("contract") != "last_observed_trade_nonexecution_book_settlement_v1"
            or event.get("simulation_only") is not True or event.get("simulated_order") is not False
            or event.get("simulated_fill") is not False or event.get("broker_fill") is not False
            or event.get("exchange_fill") is not False or event.get("source_completeness_verified") is not False
            or settlement.get("source_completeness_verified") is not False
            or symbol not in (settlement.get("symbols") or [])
            or ref.get("symbol") != symbol or ref.get("exact_trade_time_known") is not False
            or (not daily and ref.get("source_clock_basis") != "minute_interval_end_not_own_execution_or_exact_trade_time")
            or (daily and (not allow_daily_close_reference
                           or symbol not in settlement.get("official_daily_close_time_unknown_symbols", [])
                           or ref.get("reference_observed_at") is not None
                           or ref.get("reference_session_date") != day
                           or event.get("reference_before_simulated_entry") is not None))
            or not entry or event.get("quantity", 0) <= 0):
        raise ValueError(f"invalid provisional nontrade disclosure: {day}/{symbol}")
    try:
        actual = datetime.fromisoformat(str(event["recorded_at"]))
        booked = datetime.fromisoformat(str(event["accounting_at"]))
        observed = None if daily else datetime.fromisoformat(str(ref["reference_observed_at"]))
    except (ValueError, KeyError):
        raise ValueError("invalid provisional nontrade clock") from None
    if (booked.date().isoformat() != day or booked.time() != time(13, 30)
            or booked.utcoffset() != timedelta(hours=8)
            or (observed is not None and (observed.utcoffset() != timedelta(hours=8)
                                          or observed.date() != booked.date() or observed > booked))
            or actual.utcoffset() != timedelta(hours=8) or actual < booked):
        raise ValueError("provisional nontrade clocks conflict")
    verified_hashes = {}
    sources = [row for row in (session.get("intraday_replay") or {}).get("source_files", [])
               if symbol in row.get("symbols", [])]
    if not daily:
        sources = [row for row in sources if row.get("path") == ref.get("source_path")
                   and row.get("sha256") == ref.get("source_sha256")]
    if not daily and len(sources) != 1:
        raise ValueError("nontrade minute source does not match session ownership")
    for source in sources:
        path = Path(source["path"]).resolve(strict=True)
        if _sha256(path) != source["sha256"]:
            raise ValueError("nontrade minute source hash mismatch")
        verified_hashes[str(path)] = source["sha256"]
        schema = pl.read_parquet_schema(path)
        if not {"symbol", "ts", "Close", "volume_shares"}.issubset(schema.names()):
            raise ValueError("nontrade minute source lacks explicit shares")
        frame = (pl.scan_parquet(path).filter(pl.col("symbol").cast(pl.String) == symbol)
                 .filter(pl.col("ts").dt.date() == date.fromisoformat(day))
                 .filter(pl.col("ts").dt.time().is_between(time(9, 1), time(13, 30)))
                 .filter((pl.col("volume_shares") > 0) & (pl.col("Close") > 0))
                 .select("ts", "Close", "volume_shares").sort("ts").collect())
        if daily:
            if frame.height:
                raise ValueError("daily-close fallback cannot override a retained minute")
            continue
        if not frame.height or frame.select(pl.col("ts").is_duplicated().any()).item():
            raise ValueError("nontrade source has no unique last minute")
        last = frame.row(-1, named=True)
        stamp = last["ts"].replace(tzinfo=observed.tzinfo)
        if (stamp != observed or float(last["Close"]) != float(ref["reference_price"])
                or float(last["volume_shares"]) != float(ref["source_volume_shares"])
                or ref.get("reference_interval_start_exclusive") != (observed - timedelta(minutes=1)).isoformat(timespec="seconds")
                or ref.get("reference_interval_end_inclusive") != observed.isoformat(timespec="seconds")):
            raise ValueError("nontrade reference is not the source's last observed minute")
    close_path = Path(str(official.get("source_path") or "")).resolve(strict=True)
    close_pins = {(str(Path(row["path"]).resolve()), row["sha256"])
                  for row in (session.get("close") or {}).get("terminal_close_sources", [])}
    if (str(close_path), official.get("source_sha256")) not in close_pins or _sha256(close_path) != official.get("source_sha256"):
        raise ValueError("nontrade official close pin mismatch")
    exchange = close_path.parent.name.removesuffix("_daily_ohlcv")
    if exchange not in {"twse", "tpex"} or close_path.name != f"{day}.json":
        raise ValueError("nontrade official source namespace mismatch")
    official_rows, _ = official_closes(close_path.parent.parent, date.fromisoformat(day), exchanges=(exchange,))
    source_close = official_rows.get(symbol) or {}
    raw = source_close.get("official_row") or {}
    volume = float(str(raw.get("成交股數", raw.get("成交量", 0))).replace(",", ""))
    if (official.get("symbol") != symbol or official.get("session_date") != day
            or official.get("price_basis") != "official_session_close"
            or source_close.get("price") != official.get("price")
            or source_close.get("source") != official.get("source") or not math.isfinite(volume) or volume <= 0):
        raise ValueError("nontrade official close source disagrees")
    if daily and (ref.get("source_path") != str(close_path)
                  or ref.get("source_sha256") != official.get("source_sha256")
                  or ref.get("source") != official.get("source")
                  or ref.get("reference_price") != official.get("price")):
        raise ValueError("daily-close fallback reference disagrees with official source")
    verified_hashes[str(close_path)] = official["source_sha256"]
    expense = event.get("research_close_expense")
    if entry.get("side") == "sell_short" and mode.get("research_short_limit_up_close_penalty_rate"):
        if not isinstance(expense, dict):
            raise ValueError("nontrade research short lacks cost proof")
        hypothetical = {"session_date": day, "symbol": symbol, "quantity": event["quantity"],
                        "side": "buy_to_cover", "price": official["price"],
                        "fill_at": booked.isoformat(), "purpose": "13_30_unlimited_close_paper_settlement",
                        "research_close_expense": expense}
        amount, research_hashes = _verify_research_close_source(hypothetical, expense.get("source_context") or {},
            rate=float(mode["research_short_limit_up_close_penalty_rate"]), expected_used=0)
        if amount != 0.0:
            raise ValueError("nontrade replay cannot silently omit a positive research cost")
        verified_hashes.update(research_hashes)
    exit_side = "sell" if entry.get("side") == "buy" else "buy_to_cover"
    return {**event, "_nontrade_accounting_assumption": True, "_nontrade_verified_source_hashes": verified_hashes,
            "purpose": "nontrade_assumption",
            "side": exit_side, "price": ref["reference_price"],
            "fee_and_tax_twd": event["assumed_exit_fee_and_tax_twd"], "order_id": ""}


def _source_free_flat_sessions(sessions: list[dict], fills: list[dict], marks: list[dict],
                               nontrade_rows: list[dict]) -> frozenset[str]:
    """A flat day needs no held-position minute tape, but is not a source proof."""
    active_days = {str(row.get("session_date") or "") for row in [*fills, *nontrade_rows]}
    marked = defaultdict(list)
    for mark in marks:
        marked[str(mark.get("session_date") or "")].append(mark)
    flat = set()
    for session in sessions:
        day = str(session.get("session_date") or "")
        intraday, modes = session.get("intraday_replay") or {}, session.get("modes") or []
        rows = marked.get(day) or []
        if (day in active_days or not modes or intraday.get("source_files")
                or intraday.get("requested_symbols") != 0 or intraday.get("resolved_symbols") != 0
                or intraday.get("missing_symbols") != [] or len(rows) != 270 * len(modes)
                or any(int(row.get("open_position_count") or 0) != 0 for row in rows)
                or any(int((mode.get("entry") or {}).get("position_rows") or 0) != 0
                       or int((mode.get("after_close") or {}).get("open_position_rows") or 0) != 0
                       for mode in modes)):
            continue
        flat.add(day)
    return frozenset(flat)


def _source_free_provisional_sessions(sessions: list[dict], fills: list[dict], marks: list[dict],
                                      nontrade_rows: list[dict], rebuild: dict) -> frozenset[str]:
    """Prove a labelled assumption-only ledger, not absent market trading."""
    if (rebuild.get("provisional_minute_source_coverage") is not True
            or rebuild.get("official_daily_close_reference_when_minute_missing") is not True
            or rebuild.get("historical_open_fallback_assumption") != HISTORICAL_OPEN_FALLBACK_CONTRACT):
        return frozenset()
    result = set()
    for session in sessions:
        day = str(session.get("session_date") or "")
        intraday, modes = session.get("intraday_replay") or {}, session.get("modes") or []
        if (not modes or intraday.get("source_files") or intraday.get("resolved_symbols") != 0
                or intraday.get("provisional_minute_source_coverage") is not True):
            continue
        day_fills = [row for row in fills if row.get("session_date") == day]
        exits = [row for row in nontrade_rows if row.get("session_date") == day]
        rows = [row for row in marks if row.get("session_date") == day]
        if (not day_fills or len(exits) != len(day_fills) or len(rows) != 270 * len(modes)
                or not {row.get("symbol") for row in day_fills} <= set(intraday.get("missing_symbols") or [])
                or any(row.get("purpose") != "entry" or row.get("historical_open_fallback_fill") is not True
                       for row in day_fills)
                or any(row.get("_nontrade_accounting_assumption") is not True
                       or (row.get("reference") or {}).get("source_clock_basis")
                          != "official_daily_close_last_trade_time_unknown" for row in exits)
                or {(row.get("market"), row.get("position_id"), row.get("quantity")) for row in day_fills}
                   != {(row.get("market"), row.get("position_id"), row.get("quantity")) for row in exits}
                or any(int((mode.get("after_close") or {}).get("open_position_rows") or 0) for mode in modes)
                or any(row.get("provisional_minute_source_coverage") is not True
                       or (int(row.get("open_position_count") or 0) > 0
                           and (row.get("valuation_stale") is not True
                                or row.get("fresh_trade_position_count") != 0))
                       for row in rows)):
            continue
        result.add(day)
    return frozenset(result)


def _with_verified_share_volume(frame: pl.LazyFrame) -> pl.LazyFrame:
    """Keep canonical shares or prove the encoding of legacy raw stock KBars."""

    schema = frame.collect_schema()
    if "volume_shares" in schema:
        shares = pl.col("volume_shares").cast(pl.Float64, strict=False)
        return frame.with_columns(
            pl.when(shares.is_finite() & (shares >= 0)
                    & ((shares - shares.round(0)).abs() <= 1e-6))
            .then(shares.round(0)).otherwise(None).alias("volume_shares")
        )
    frame = frame.with_columns(*[
        (pl.col(name).cast(pl.Float64, strict=False) if name in schema
         else pl.lit(None, dtype=pl.Float64)).alias(name)
        for name in ("Volume", "Amount", "Low", "High", "contract_unit")
    ])
    return with_stock_share_volume(frame, tolerance=0.001)


def _entry_source_path(source: str) -> Path:
    prefix, separator, name = source.partition(":")
    if (not separator or prefix not in {
            "local_minute_parquet_0901_minute_vwap",
            "local_minute_parquet_0901_minute_close"}
            or not Path(name).is_absolute()):
        raise ValueError(f"unsupported historical entry source identity: {source}")
    return Path(name)


def _selected_replay_source_bars(
    frame: pl.DataFrame,
    source_pins: dict[tuple[str, str], frozenset[str]] | None,
) -> pl.DataFrame:
    """Audit the exact symbol/session owner, not an unused overlapping chunk."""
    if source_pins is not None:
        selected = pl.DataFrame(
            [(symbol, day, path) for (day, symbol), paths in source_pins.items()
             for path in paths],
            schema={"symbol": pl.String, "_session": pl.String, "_source_path": pl.String},
            orient="row",
        )
        frame = (frame.with_columns(pl.col("minute_key").str.slice(0, 10).alias("_session"))
                 .join(selected, on=["symbol", "_session", "_source_path"], how="semi")
                 .drop("_session"))
    return frame.drop("_source_path")


def _audit_source_bar_projection(frame: pl.LazyFrame) -> pl.LazyFrame:
    """Keep observed Close separate from OHLC/depth for price-only research."""
    close = pl.col("Close") if "Close" in frame.collect_schema() else pl.lit(None)
    return _with_verified_share_volume(frame).select(
        pl.col("symbol"),
        pl.col("ts").dt.strftime("%Y-%m-%dT%H:%M").alias("minute_key"),
        pl.col("High").cast(pl.Float64).alias("high"),
        pl.col("Low").cast(pl.Float64).alias("low"),
        pl.col("volume_shares"), close.cast(pl.Float64).alias("close"),
    )


def _full_0901_price_only_sources(frame: pl.DataFrame, retained: list[dict]) -> dict:
    """Verify price-only entry witnesses, never infer trade volume or capacity.

    ``frame`` contains only files already hash/receipt checked by the audit.
    The opening owner can differ from the positive-volume intraday tape. Keep
    its exact symbol/minute/path rather than borrowing that tape's source pin.
    """
    wanted = {}
    for row in retained:
        source = str(row["source"])
        if not source.startswith("local_minute_parquet_0901_minute_close:"):
            continue
        day = str(row["quote_at"])[:10]
        if (row["quote_at"] != f"{day}T09:01:00+08:00"
                or row.get("source_window_start") != f"{day}T09:00:00+08:00"
                or row.get("source_window_end") not in {
                    f"{day}T09:00:59+08:00", f"{day}T09:01:00+08:00"}
                or row.get("tick_volume_units_0901") != 0):
            continue
        price = float(row["execution_price_0901"])
        if not math.isfinite(price) or price <= 0:
            raise ValueError("invalid price-only retained 09:01 Close")
        key = (str(row["symbol"]), f"{day}T09:01", str(_entry_source_path(source)))
        if key in wanted:
            raise ValueError("duplicate price-only retained 09:01 source")
        wanted[key] = (source, price)
    if not wanted:
        return {}
    output = {}
    seen = set()
    for bar in frame.iter_rows(named=True):
        key = (bar["symbol"], bar["minute_key"], bar["_source_path"])
        if key not in wanted:
            continue
        if key in seen:
            raise ValueError("ambiguous price-only 09:01 source observations")
        seen.add(key)
        source, price = wanted[key]
        close, low, high = bar["close"], bar["low"], bar["high"]
        if (any(value is None or not math.isfinite(value) or value <= 0
                for value in (close, low, high)) or not low <= close <= high
                or not math.isclose(price, close, rel_tol=0, abs_tol=1e-8)):
            raise ValueError("retained 09:01 price differs from the source-published Close")
        volume = bar.get("volume_shares")
        if volume is not None and (not math.isfinite(volume) or volume != 0):
            continue
        output[(key[0], key[1], source)] = price
    return output


def _is_verified_full_0901_price_only_entry(fill: dict, prices: dict, *, full_target: bool) -> bool:
    day = str(fill.get("session_date") or "")
    price = prices.get((fill.get("symbol"), f"{day}T09:01", fill.get("entry_price_source")))
    return bool(
        full_target and price is not None
        and fill.get("purpose") == "entry"
        # Only the legacy v1 contract allowed a price-only, zero-volume fill.
        # The v2 contract requires a real positive-volume minute observation.
        and fill.get("fill_contract") == LEGACY_REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL
        and fill.get("fill_at") == f"{day}T09:01:00+08:00"
        and fill.get("recorded_at") == f"{day}T09:01:00+08:00"
        and fill.get("simulation_only") is True
        and fill.get("simulation_replay") is True
        and fill.get("counterfactual_0901_price_fill") is True
        and fill.get("synthetic_fill") is False
        and fill.get("entry_price_method") == "minute_close"
        and math.isclose(float(fill.get("price") or 0), price, rel_tol=0, abs_tol=1e-8)
    )


def _retained_entry_book_source_days(
    state_dir: Path,
    session: dict,
    opening_fill_symbols: set[str],
    source_signatures: dict[str, tuple[int, int, int]],
    retained_opening_bars: list[dict] | None = None,
    retained_local_opening_prices: list[dict] | None = None,
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
    recorded_path = Path(str(raw_path))
    expected_dir = (state_dir / "replay_entry_books").resolve()
    path = expected_dir / f"{session['session_date']}.parquet"
    # Atomic promotion moves directories. Retain the recorded path as
    # provenance, but read only the local canonical book with its exact SHA.
    if (not recorded_path.is_absolute() or recorded_path.parent.name != "replay_entry_books"
            or recorded_path.name != path.name or not path.is_file()):
        raise ValueError(f"retained entry book is outside candidate: {recorded_path}")
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
    finmind_projection_cache: dict = {}
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
        is_finmind_projection = (source_path.name == "data.parquet"
            and any((source_path.parent.parent / name).is_file() for name in (
                "finmind_replay_source.json", "finmind_replay_legacy_source.json")))
        if is_finmind_projection:
            pins = _verified_finmind_minute_projection(source_path, day=day,
                                                       cache=finmind_projection_cache)
            if not pins:
                raise ValueError(f"entry FinMind source evidence unavailable: {source}")
            for source_name in pins:
                stat = Path(source_name).stat()
                signature = (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
                if source_signatures.setdefault(source_name, signature) != signature:
                    raise RuntimeError(f"FinMind entry source changed during audit: {source_name}")
        if not (is_symbol_chunk or is_daily_research_partition or is_finmind_projection):
            raise ValueError(f"entry source symbol mismatch: {row['symbol']}:{source}")
        result[source].add(date.fromisoformat(day))
        if retained_local_opening_prices is not None:
            retained_local_opening_prices.append(row)
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


def _verify_terminal_close_fills(fills, rebuild, modes, *, verify_sources=True):
    """Verify the sole capacity exception against raw official close reports.

    A contract string alone never exempts an arbitrary exit from minute volume.
    Ordinary exits remain in the existing shared-capacity audit below.
    """
    from scripts.settle_tw_day_trade_official_close import official_closes
    from stockagent.live.tw_day_trade_simulation import TERMINAL_CLOSE_UNLIMITED_CONTRACT as contract
    from stockagent.data.tw_security import classify_tw_stock_or_etf
    from stockagent.data.tw_price_rules import price_on_tick_grid_numpy
    selected = [f for f in fills if f.get("fill_contract") == contract
                or f.get("terminal_close_receipt") is not None
                or f.get("purpose") == "13_30_unlimited_close_paper_settlement"]
    enabled = (rebuild.get("replay_contract") or {}).get("terminal_close_contract") == contract
    if selected and not enabled:
        raise ValueError("terminal close fills lack explicit replay authorization")
    if not enabled:
        return {}
    if any(m.get("terminal_close_contract") != contract for m in modes.values()):
        raise ValueError("terminal close state/replay policy mismatch")
    sessions = {s["session_date"]: s for s in rebuild["sessions"]}
    source_hashes, prices, counts, seen = {}, {}, defaultdict(int), set()
    for fill in selected:
        day, market, symbol = fill["session_date"], fill["market"], fill["symbol"]
        proof = fill.get("terminal_close_receipt") or {}
        evidence = proof.get("source_evidence") or {}
        stamp = f"{day}T13:30:00+08:00"
        kind = classify_tw_stock_or_etf(symbol)
        if not (enabled and fill.get("fill_contract") == contract
                and proof.get("contract") == contract
                and proof.get("broker_fill") is False
                and proof.get("full_quantity_is_user_assumption") is True
                and fill.get("simulation_only") is True
                and fill.get("purpose") == "13_30_unlimited_close_paper_settlement"
                and fill.get("fill_at") == proof.get("effective_at") == stamp
                and datetime.fromisoformat(fill["recorded_at"]) >= datetime.fromisoformat(stamp)
                and fill.get("quote_at") is None and fill.get("exchange_match_at") is None
                and fill["quantity"] == fill["requested_quantity"] > 0
                and fill.get("remaining_quantity") == 0
                and evidence.get("session_date") == day
                and evidence.get("price_basis") == "official_session_close"
                and evidence.get("price") == fill["price"]
                and kind is not None
                and price_on_tick_grid_numpy(np.array([fill["price"]]), np.array([date.fromisoformat(day)]),
                                              security_types=kind)[0]):
            raise ValueError(f"invalid terminal close receipt: {market}/{day}/{symbol}")
        identity = (market, day, fill["position_id"])
        if identity in seen:
            raise ValueError(f"duplicate terminal close: {identity}")
        seen.add(identity)
        counts[(market, day)] += 1
        if verify_sources:
            if day not in prices:
                prices[day] = {}
                for source in sessions[day]["close"]["terminal_close_sources"]:
                    path = Path(source["path"])
                    exchange = path.parent.name.removesuffix("_daily_ohlcv")
                    if exchange not in ("twse", "tpex") or path.name != f"{day}.json" or _sha256(path) != source["sha256"]:
                        raise ValueError(f"terminal close raw source mismatch: {path}")
                    rows, _ = official_closes(path.parent.parent, date.fromisoformat(day), exchanges=(exchange,))
                    if prices[day].keys() & rows.keys():
                        raise ValueError(f"duplicate terminal close source: {day}")
                    prices[day].update(rows)
                    source_hashes[str(path)] = source["sha256"]
            row = prices[day].get(symbol) or {}
            raw = row.get("official_row") or {}
            volume = float(str(raw.get("成交股數", raw.get("成交量", 0))).replace(",", ""))
            if (row.get("price") != fill["price"] or row.get("source") != evidence.get("source")
                    or row.get("source_sha256") != evidence.get("source_sha256") or not volume > 0):
                raise ValueError(f"terminal close differs from traded official source: {market}/{day}/{symbol}")
    for day, session in sessions.items():
        for row in session["modes"]:
            close = row.get("after_close") or {}
            if (close.get("terminal_close_contract") != contract
                    or close.get("open_position_rows") != 0
                    or int(close.get("terminal_flatten_count") or 0) != counts[(row["market"], day)]):
                raise ValueError(f"terminal close count/flat proof mismatch: {row['market']}/{day}")
    return source_hashes


def _verified_finmind_minute_projection(path: Path, *, day: str, cache: dict | None = None) -> dict[str, str] | None:
    """Independently rebuild one private FinMind projection, including its pins."""
    path = path.resolve(strict=True)
    manifests = [path.parent.parent / name for name in (
        "finmind_replay_source.json", "finmind_replay_legacy_source.json")
        if (path.parent.parent / name).is_file()]
    if path.name != "data.parquet" or not manifests:
        return None
    if len(manifests) != 1:
        raise ValueError("ambiguous FinMind projection manifest")
    manifest = manifests[0]
    cache = {} if cache is None else cache
    key = ("verified_finmind_minute_projection", str(manifest), day)
    if key not in cache:
        if manifest.name == "finmind_replay_legacy_source.json":
            from scripts.prepare_finmind_tw_day_trade_legacy_source import verify
        else:
            from scripts.prepare_finmind_tw_day_trade_repair_source import verify

        before = manifest.stat()
        manifest_sha = _sha256(manifest)
        proof = verify(manifest.parent)
        after = manifest.stat()
        if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                or proof.get("status") != "source_verified_not_ledger_applied"
                or proof.get("session_date") != day
                or proof.get("output", {}).get("path") != str(path)):
            raise ValueError("FinMind private minute projection identity changed")
        pins = {str(manifest): manifest_sha}
        for pin in [proof["output"], *proof["source_pins"]]:
            source_path, digest = str(pin["path"]), str(pin["sha256"])
            if source_path in pins and pins[source_path] != digest:
                raise ValueError("FinMind private projection has conflicting source pins")
            pins[source_path] = digest
        cache[key] = pins
    return dict(cache[key])


def _verify_research_close_source(fill, session, *, rate, expected_used, source_cache=None):
    """Independently read the pinned sources for one opt-in research charge.

    This verifies an evidence envelope; it does not turn a paper settlement
    into an exchange execution. The caller must also check inventory/FIFO and
    use the normal terminal-close and full-session source audits.
    """
    from scripts.settle_tw_day_trade_official_close import official_closes
    from stockagent.live.tw_day_trade_simulation import _research_close_expense_amount

    day, symbol = str(fill["session_date"]), str(fill["symbol"])
    envelope = fill.get("research_close_expense")
    if not isinstance(envelope, dict):
        raise ValueError("research short close lacks expense evidence")
    close = envelope.get("official_close_evidence") or {}
    limit = envelope.get("upper_limit_evidence") or {}
    capacity = envelope.get("closing_capacity_evidence")
    source_hashes = {}
    cache = {} if source_cache is None else source_cache

    def source_digest(path):
        key = ("digest", str(path))
        if key not in cache:
            if not path.is_file():
                raise ValueError(f"research close source missing: {path}")
            cache[key] = _sha256(path)
        return cache[key]

    def pinned(path_value, digest, allowed):
        path = Path(str(path_value or ""))
        if not path.is_absolute() or not path.is_file():
            raise ValueError("research close source path missing")
        path = path.resolve()
        if (str(path), digest) not in allowed or source_digest(path) != digest:
            raise ValueError(f"research close source pin mismatch: {path}")
        source_hashes[str(path)] = digest
        return path

    close_sources = {(str(Path(row["path"]).resolve()), row["sha256"])
                     for row in (session.get("close") or {}).get("terminal_close_sources") or []}
    close_path = pinned(close.get("source_path"), close.get("source_sha256"), close_sources)
    exchange = close_path.parent.name.removesuffix("_daily_ohlcv")
    if exchange not in {"twse", "tpex"} or close_path.name != f"{day}.json":
        raise ValueError("research close is not a dated official exchange report")
    official_key = ("official", str(close_path), close.get("source_sha256"), day)
    if official_key not in cache:
        cache[official_key], _ = official_closes(
            close_path.parent.parent, date.fromisoformat(day), exchanges=(exchange,))
    prices = cache[official_key]
    official = prices.get(symbol) or {}
    raw = official.get("official_row") or {}
    try:
        traded_volume = float(str(raw.get("成交股數", raw.get("成交量", 0))).replace(",", ""))
    except (TypeError, ValueError):
        traded_volume = 0.0
    if (official.get("price") != fill.get("price") or official.get("source") != close.get("source")
            or official.get("source_sha256") != close.get("source_sha256")
            or not math.isfinite(traded_volume) or traded_volume <= 0 or not traded_volume.is_integer()):
        raise ValueError("research close differs from official traded source")

    limit_path = Path(str(session.get("price_limit_path") or ""))
    if not limit_path.is_absolute():
        raise ValueError("research close lacks a pinned daily price-limit source")
    limit_path = pinned(limit.get("source_path"), limit.get("source_sha256"), {
        (str(limit_path.resolve()), session.get("price_limit_sha256"))})
    limit_key = ("limits", str(limit_path), limit.get("source_sha256"))
    if limit_key not in cache:
        grouped = defaultdict(list)
        for row in pl.read_parquet(limit_path, columns=["symbol", "upper_limit_price"]).to_dicts():
            grouped[str(row["symbol"])].append(row["upper_limit_price"])
        cache[limit_key] = grouped
    limits = cache[limit_key].get(symbol) or []
    if len(limits) != 1 or limits[0] != limit.get("price"):
        raise ValueError("research close upper limit differs from pinned source")

    if capacity is not None:
        if not isinstance(capacity, dict):
            raise ValueError("invalid research closing minute evidence")
        pins = {(str(Path(row["path"]).resolve()), row["sha256"])
                for row in (session.get("intraday_replay") or {}).get("source_files") or []
                if symbol in (row.get("symbols") or [])}
        minute_path = pinned(capacity.get("source_path"), capacity.get("source_sha256"), pins)
        receipt_key = ("minute_receipt", str(minute_path), capacity.get("source_sha256"), day)
        finmind_pins = _verified_finmind_minute_projection(minute_path, day=day, cache=cache)
        if finmind_pins is not None:
            source_hashes.update(finmind_pins)
        elif "minute_chunks" in minute_path.parts:
            try:
                start, end = map(date.fromisoformat, minute_path.stem.split("_"))
            except ValueError as exc:
                raise ValueError("invalid research closing minute chunk identity") from exc
            receipt_path = minute_path.with_suffix(".receipt.json")
            source_hashes[str(receipt_path)] = source_digest(receipt_path)
            if receipt_key not in cache:
                cache[receipt_key] = minute_receipt_valid(
                    receipt_path, symbol=symbol,
                    start=start, end=end, required_dates={date.fromisoformat(day)})
            if minute_path.parent.name != symbol or not cache[receipt_key]:
                raise ValueError("invalid research closing minute collector receipt")
            receipt = _read_json(receipt_path)
            output_receipt = receipt.get("output_receipt") or {}
            if (day not in (receipt.get("returned_dates") or [])
                    or day in (receipt.get("source_gap_dates") or [])
                    or Path(str(output_receipt.get("path") or "")).resolve() != minute_path
                    or output_receipt.get("sha256") != capacity.get("source_sha256")):
                raise ValueError("research closing minute receipt does not own the observed date/file")
            for raw_source in receipt.get("raw_tick_sources") or []:
                raw_path = Path(raw_source["path"]).resolve()
                digest = source_digest(raw_path)
                if digest != raw_source.get("sha256"):
                    raise ValueError("research close raw tick source changed")
                source_hashes[str(raw_path)] = digest
        elif "research_dataset" in minute_path.parts and minute_path.name == "data.parquet":
            manifest_path = minute_path.parent.parent / "manifest.json"
            source_hashes[str(manifest_path)] = source_digest(manifest_path)
            if receipt_key not in cache:
                manifest = _read_json(manifest_path)
                relative = str(minute_path.relative_to(minute_path.parent.parent))
                matching = [row for row in manifest.get("partitions") or []
                            if row.get("output") == relative and row.get("trade_date") == day]
                cache[receipt_key] = (len(matching) == 1 and matching[0].get("status") == "ok"
                    and matching[0].get("output_sha256") == capacity.get("source_sha256"))
            if not cache[receipt_key]:
                raise ValueError("invalid research closing minute partition receipt")
        else:
            raise ValueError("research closing minute source has no checked collector receipt")
        bar_key = ("minute_bars", str(minute_path), capacity.get("source_sha256"), day)
        if bar_key not in cache:
            grouped = defaultdict(list)
            bars = (_audit_source_bar_projection(pl.scan_parquet(minute_path))
                    .filter(pl.col("minute_key") == f"{day}T13:30").collect())
            for bar in bars.to_dicts():
                grouped[str(bar["symbol"])].append(bar)
            cache[bar_key] = grouped
        bars = cache[bar_key].get(symbol) or []
        if len(bars) != 1:
            raise ValueError("research close minute is missing or ambiguous")
        bar = bars[0]
        volume = bar.get("volume_shares")
        if (volume is None or not math.isfinite(volume) or not 0 <= volume < 2**53
                or volume != int(volume) or int(volume) != capacity.get("volume_shares")
                or capacity.get("used_capacity_shares") != expected_used):
            raise ValueError("research close capacity differs from the retained minute")
        if volume > 0:
            observed = [bar.get(field) for field in ("low", "close", "high")]
            if (any(value is None or not math.isfinite(value) or value <= 0 for value in observed)
                    or bar["close"] != fill.get("price")
                    or not bar["low"] <= bar["close"] <= bar["high"] <= limit["price"]):
                raise ValueError("research close minute is not the official closing observation")
    elif expected_used:
        raise ValueError("research close source capacity usage is unexplained")

    quantity = fill.get("quantity")
    if (type(quantity) is not int or quantity <= 0 or fill.get("side") != "buy_to_cover"
            or fill.get("purpose") != "13_30_unlimited_close_paper_settlement"):
        raise ValueError("research expense is not attached to a terminal short close")
    amount = _research_close_expense_amount(
        position={"symbol": symbol, "signed_shares": -quantity,
                  "upper_limit": limit.get("price"), "lot_size": 1000},
        mode={"session_date": day, "research_short_limit_up_close_penalty_rate": rate},
        price=fill["price"], now=datetime.fromisoformat(str(fill["fill_at"])),
        reason=fill["purpose"], order_type="PAPER_CLOSE_SETTLEMENT", quantity=quantity,
        evidence=envelope,
    )
    return amount, source_hashes


def _verify_research_close_costs(fills, rebuild, modes, *, verify_sources):
    """Admit research costs only after source and shared-FIFO verification.

    The explicitly versioned opt-in is per market. Existing markets cannot
    acquire this cost merely because another account uses it. Ambiguous mixed
    direction/13:30 executions stay blocked until their capacity ownership is
    independently reconstructible.
    """
    policy = (rebuild.get("replay_contract") or {}).get("research_close_penalty_by_market_v1", {})
    if not isinstance(policy, dict):
        raise ValueError("invalid research close market policy")
    for market, rate in policy.items():
        if (market not in modes or isinstance(rate, bool) or not isinstance(rate, (float, int))
                or not math.isfinite(rate) or not 0 < rate <= 1):
            raise ValueError("invalid research close market or rate")
    for market, mode in modes.items():
        rate = mode.get("research_short_limit_up_close_penalty_rate", 0.0)
        if (isinstance(rate, bool) or not isinstance(rate, (float, int))
                or not math.isfinite(rate) or not 0 <= rate <= 1
                or (rate > 0 and policy.get(market) != rate)
                or (market in policy and policy[market] != rate)):
            raise ValueError("research close state and replay rate mismatch")
    claimed = [fill for fill in fills if fill.get("research_close_expense") is not None]
    if any(fill.get("market") not in policy for fill in claimed):
        raise ValueError("research close expense lacks market authorization")
    if policy and not verify_sources:
        raise ValueError("research close accounting requires independent source validation")
    sessions = {str(row["session_date"]): row for row in rebuild.get("sessions") or []}
    for day, session in sessions.items():
        rows = {str(row["market"]): row for row in session.get("modes") or []}
        for market, rate in policy.items():
            close = (rows.get(market) or {}).get("after_close") or {}
            if close.get("research_short_limit_up_close_penalty_rate") != rate:
                raise ValueError(f"research close session rate mismatch: {market}/{day}")
    entries, groups = {}, defaultdict(list)
    for index, fill in enumerate(fills):
        market = str(fill.get("market") or "")
        key = (market, str(fill.get("position_id") or ""))
        if market in policy and fill.get("purpose") == "entry":
            if not key[1] or key in entries:
                raise ValueError("research close lacks unique entry identity")
            try:
                entry_at = datetime.fromisoformat(str(fill.get("fill_at") or ""))
            except ValueError as exc:
                raise ValueError("research entry has no valid execution clock") from exc
            if (entry_at.utcoffset() != timedelta(hours=8)
                    or entry_at.date().isoformat() != fill.get("session_date")
                    or not time(9, 0) <= entry_at.time() < time(13, 20)):
                raise ValueError("research entry is outside its session clock")
            entries[key] = (entry_at, index, key[1], str(fill.get("side") or ""),
                            str(fill.get("session_date") or ""), str(fill.get("symbol") or ""))
        if (market in policy
                and fill.get("purpose") == "13_30_unlimited_close_paper_settlement"):
            groups[(market, str(fill["session_date"]), str(fill["symbol"]))].append(fill)
        elif market in policy and fill.get("research_close_expense") is not None:
            raise ValueError("research close expense attached to nonterminal fill")
    result, hashes, totals, source_cache = {}, {}, defaultdict(float), {}
    for (market, day, symbol), rows in groups.items():
        if day not in sessions or any((market, str(row.get("position_id") or "")) not in entries for row in rows):
            raise ValueError("research close lacks source session or entry")
        if rows != sorted(rows, key=lambda row: entries[(market, str(row["position_id"]))]):
            raise ValueError("research close terminal rows violate source-capacity FIFO")
        if len({str(row["position_id"]) for row in rows}) != len(rows):
            raise ValueError("duplicate research terminal close")
        shorts = [row for row in rows if row.get("side") == "buy_to_cover"]
        if any(row.get("side") not in {"buy_to_cover", "sell"} for row in rows):
            raise ValueError("research terminal direction is unknown")
        if any((entries[(market, str(row["position_id"]))][3:]
                != (("sell_short" if row.get("side") == "buy_to_cover" else "buy"), day, symbol))
                for row in rows):
            raise ValueError("research terminal direction differs from original entry")
        if shorts and len(shorts) != len(rows):
            raise ValueError("mixed terminal directions need independent capacity reconciliation")
        if any(row.get("research_close_expense") is not None for row in rows if row not in shorts):
            raise ValueError("research short expense attached to a long close")
        if shorts and any(row.get("market") == market and row.get("session_date") == day
                and row.get("symbol") == symbol and str(row.get("fill_at") or "")[:16] == f"{day}T13:30"
                and row.get("purpose") != "13_30_unlimited_close_paper_settlement"
                for row in fills):
            raise ValueError("other 13:30 fills need independent capacity reconciliation")
        used, source_identity = 0, None
        for row in shorts:
            amount, checked = _verify_research_close_source(
                row, sessions[day], rate=policy[market], expected_used=used,
                source_cache=source_cache)
            envelope = row["research_close_expense"]
            capacity = envelope.get("closing_capacity_evidence")
            if capacity is not None:
                identity = (capacity["source_path"], capacity["source_sha256"], capacity["volume_shares"])
                if source_identity is not None and identity != source_identity:
                    raise ValueError("research close cohorts disagree on source capacity")
                source_identity = identity
                used += int(envelope["cost"]["source_covered_shares"])
            key = (market, str(row.get("order_id") or ""))
            if not key[1] or key in result:
                raise ValueError("duplicate or missing research close order identity")
            result[key] = amount
            totals[market] += amount
            hashes.update(checked)
    for market in policy:
        total = modes[market].get("cumulative_research_close_expense_twd") or 0.0
        if not math.isclose(float(total), totals[market], rel_tol=0.0, abs_tol=1e-5):
            raise ValueError(f"research close cumulative expense mismatch: {market}")
    return result, hashes


def _accounting_event_clock(event):
    """Use the disclosed 13:30 economic clock only for validated terminal fills."""
    if event.get("_nontrade_accounting_assumption"):
        return str(event.get("accounting_at") or "")
    if event.get("terminal_close_receipt") is not None:
        return str(event.get("fill_at") or "")
    return str(event.get("recorded_at") or "")


def _terminal_event_visible_to_mark(event, mark):
    """A retrospective endpoint cannot know a settlement before it was recorded."""
    if event.get("_nontrade_accounting_assumption"):
        try:
            recorded = datetime.fromisoformat(str(event.get("recorded_at") or ""))
            generated = datetime.fromisoformat(str(mark.get("replay_generated_at") or ""))
            return (mark.get("recorded_at_basis") == "counterfactual_effective_clock_not_original_publication"
                    and recorded.utcoffset() is not None and generated.utcoffset() is not None
                    and recorded <= generated)
        except (TypeError, ValueError):
            return False
    if event.get("terminal_close_receipt") is None:
        return True
    try:
        recorded = datetime.fromisoformat(str(event.get("recorded_at") or ""))
        mark_recorded = datetime.fromisoformat(str(mark.get("recorded_at") or ""))
        return (recorded.utcoffset() is not None and mark_recorded.utcoffset() is not None
                and recorded <= mark_recorded)
    except (TypeError, ValueError):
        return False


def audit(state_dir: Path, *, verify_sources: bool = True, completed_prefix: bool = False,
          require_margin_carry: bool = True) -> dict:
    names = ("state.json", "rebuild_receipt.json", "fills.jsonl", "marks.jsonl", "orders.jsonl") + (
        ("events.jsonl",) if (state_dir / "events.jsonl").is_file() else ())
    before = {name: _sha256(state_dir / name) for name in names}
    state, rebuild = _read_json(state_dir / "state.json"), _read_json(state_dir / "rebuild_receipt.json")
    full_target_counterfactual = (
        (rebuild.get("replay_contract") or {}).get("entry")
        in REPLAY_FULL_COUNTERFACTUAL_CONTRACTS
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
    terminal_source_hashes = _verify_terminal_close_fills(
        fills, rebuild, state["modes"], verify_sources=verify_sources)
    if not require_margin_carry:
        # Share the source, fee, inventory and minute-NAV audit without
        # authorizing overnight borrowing merely to validate terminal closes.
        for session in sessions:
            if any(int((row.get("after_close") or {}).get("open_position_rows") or 0)
                   for row in session["modes"]):
                raise ValueError("strict intraday audit requires every session flat")
        for mode in state["modes"].values():
            if any(mode.get(name) for name in ("carry_cost_ledger", "corporate_action_ledger",
                                              "share_replacement_ledger")):
                raise ValueError("strict intraday audit cannot admit carried-account events")
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
    nontrade_raw = ([row for row in _read_jsonl(state_dir / "events.jsonl")
                     if row.get("event") == "nontrade_last_observed_accounting_settlement"]
                    if (state_dir / "events.jsonl").is_file() else [])
    if nontrade_raw and (require_margin_carry or rebuild.get("provisional_minute_source_coverage") is not True):
        raise ValueError("nontrade accounting assumptions require an isolated provisional intraday replay")
    entry_by_position = {(row["market"], row["position_id"]): row for row in fills if row.get("purpose") == "entry"}
    session_by_date = {row["session_date"]: row for row in sessions}
    if len({(row.get("market"), row.get("position_id")) for row in nontrade_raw}) != len(nontrade_raw):
        raise ValueError("duplicate provisional nontrade settlement")
    nontrade_rows = []
    for row in nontrade_raw:
        market, day = row.get("market"), row.get("session_date")
        if market not in state["modes"] or day not in session_by_date:
            raise ValueError("nontrade assumption is outside the audited scope")
        session = session_by_date[day]
        mode_rows = [item for item in session.get("modes", []) if item.get("market") == market]
        if len(mode_rows) != 1 or (mode_rows[0].get("after_close") or {}).get("execution_evidence_complete") is not False:
            raise ValueError("nontrade settlement must disclose unverified execution evidence")
        nontrade_rows.append(_verify_provisional_nontrade_event(row, session, mode=state["modes"][market],
            entry=entry_by_position.get((market, row.get("position_id"))) or {}, verify_sources=verify_sources,
            allow_daily_close_reference=rebuild.get("official_daily_close_reference_when_minute_missing") is True))
    for day, session in session_by_date.items():
        declared = (session.get("close") or {}).get("nontrade_settlement") or {}
        count = sum(row.get("session_date") == day for row in nontrade_raw)
        if int(declared.get("position_count") or 0) != count:
            raise ValueError("nontrade event count differs from the session receipt")
    research_costs, research_source_hashes = _verify_research_close_costs(
        fills, rebuild, state["modes"], verify_sources=verify_sources)
    for fill in fills:
        if fill.get("fill_contract") != HISTORICAL_BRACKET_FULL_CROSS_CONTRACT:
            continue
        rows = [row for session in sessions if session.get("session_date") == fill.get("session_date")
                for row in session.get("modes", []) if row.get("market") == fill.get("market")]
        if len(rows) != 1 or (rows[0].get("after_close") or {}).get("execution_evidence_complete") is not False:
            raise ValueError("full-cross replay must disclose unverified execution evidence")
    if any(str(row.get("session_date", "")) > dates[-1] for row in [*fills, *marks]):
        raise ValueError("failed session has partial ledger writes; prefix audit requires reconciliation")
    _, minute_stats = validate_existing_strategy_marks(marks, start=date.fromisoformat(dates[0]), end=date.fromisoformat(dates[-1]))
    provisional = rebuild.get("provisional_minute_source_coverage") is True
    if provisional and any(row.get("provisional_minute_source_coverage") is not True for row in marks):
        raise ValueError("provisional replay contains an unlabelled minute mark")
    terminal_marks = {(row["market"], row["session_date"]): row for row in marks
                      if str(row.get("minute") or "")[11:16] == "13:30"}
    for fill in fills:
        if fill.get("terminal_close_receipt") is None:
            continue
        mark = terminal_marks.get((fill["market"], fill["session_date"])) or {}
        if not _terminal_event_visible_to_mark(fill, mark):
            raise ValueError("terminal close endpoint predates the recorded paper settlement")
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
    for assumption in nontrade_rows:
        by_market[assumption["market"]].append(assumption)
    accounts, action_sources, action_source_hashes = {}, {}, {}
    for market, mode in state["modes"].items():
        if (require_margin_carry and mode.get("margin_carry_contract") != MARGIN_CARRY_CONTRACT) or mode["session_date"] != dates[-1]:
            errors.append(f"{market}: wrong carry contract or unfinished session")
        actions = mode.get("share_replacement_ledger") or []
        if len({a["action_id"] for a in actions}) != len(actions):
            errors.append(f"{market}: duplicate share replacement")
        action_map = {a["action_id"]: a for a in actions}
        if verify_sources and require_margin_carry:
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
                        key=lambda row: (_accounting_event_clock(row)[:16],
                                         not row.get("_share_action", False), row["recorded_at"]))
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
            while (index < len(events) and _accounting_event_clock(events[index])[:16] <= minute[:16]
                   and _terminal_event_visible_to_mark(events[index], mark)):
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
                    equal(fill["net_pnl_twd"], float(fill["gross_pnl_twd"])
                          - float(fill["entry_fee_allocated_twd"]) - float(fill["fee_and_tax_twd"])
                          - research_costs.get((market, str(fill["order_id"])), 0.0), f"{key}: net PnL")
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
        accounts[market] = {"fills": sum(not row.get("_nontrade_accounting_assumption", False) for row in by_market[market]),
                            "nontrade_assumptions": sum(bool(row.get("_nontrade_accounting_assumption")) for row in by_market[market]),
                            "claims": len(claims), "share_replacements": len(actions), "carry_costs": len(costs),
                            "ending_equity_twd": mode["total_equity_twd"], "open_positions": sum(q != 0 for q in inventory.values())}
    source_days = defaultdict(set)
    filled_symbol_days = {(row["symbol"], row["session_date"]) for row in fills}
    minute_source_signatures = {}
    finmind_projection_hashes: dict[str, str] = {}
    finmind_projection_cache: dict = {}
    retained_opening_bars: list[dict] = []
    retained_local_opening_prices: list[dict] = []
    full_0901_price_only_entries = 0
    full_cross_assumptions = 0
    open_fallback_hashes: dict[str, str] = {}
    for fill in fills:
        if fill.get("historical_open_fallback_fill") or fill.get("fill_contract") == HISTORICAL_OPEN_FALLBACK_CONTRACT:
            if not verify_sources:
                raise ValueError("official-open fallback requires independent source verification")
            matches = [row for row in sessions if row.get("session_date") == fill.get("session_date")]
            if len(matches) != 1:
                raise ValueError("official-open fallback session missing or duplicated")
            open_fallback_hashes.update(_verify_official_open_fallback_fill(fill, matches[0], rebuild,
                                                                           state_dir=state_dir))
    bracket_entries = {(row["market"], row["position_id"]): row for row in fills
                       if row.get("purpose") == "entry"}
    if not verify_sources and any(row.get("fill_contract") == HISTORICAL_BRACKET_FULL_CROSS_CONTRACT for row in fills):
        raise ValueError("full-cross assumptions require independently verified minute sources")
    opening_fill_symbols_by_day = defaultdict(set)
    for row in fills:
        if (not row.get("prior_paper_fill_reused") and not row.get("historical_open_fallback_fill")
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
            retained_local_opening_prices if full_target_counterfactual else None,
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
    assumed_without_sources = _source_free_provisional_sessions(sessions, fills, marks, nontrade_rows, rebuild)
    flat_without_sources = _source_free_flat_sessions(sessions, fills, marks, nontrade_rows)
    if (verify_sources and any(not row.get("prior_paper_fill_reused") for row in fills) and not source_days
            and set(dates) - set(assumed_without_sources) - set(flat_without_sources)):
        errors.append("filled replay has no retained minute source receipts")
    if verify_sources and fills:
        replay_source_hashes = _replay_minute_source_hashes(
            state_dir, start=date.fromisoformat(dates[0]), end=date.fromisoformat(dates[-1]),
            proven_flat_sessions=flat_without_sources,
            provisional_no_source_sessions=assumed_without_sources)
        replay_symbol_sources = _replay_minute_symbol_sources(
            state_dir, start=date.fromisoformat(dates[0]), end=date.fromisoformat(dates[-1]))
        needs = pl.DataFrame(
            [{"symbol": row["symbol"], "minute_key": row["recorded_at"][:16]}
             for row in fills if not row.get("prior_paper_fill_reused") and not row.get("historical_open_fallback_fill")],
            schema={"symbol": pl.Utf8, "minute_key": pl.Utf8},
        ).unique()
        source_bars = []
        for name, days in source_days.items():
            path = Path(name)
            stat = path.stat()
            signature = (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
            if minute_source_signatures.setdefault(str(path), signature) != signature:
                raise RuntimeError(f"minute source changed during audit: {path}")
            expected_hashes = {replay_source_hashes[(day.isoformat(), str(path))]
                               for day in days if (day.isoformat(), str(path)) in replay_source_hashes}
            if expected_hashes and expected_hashes != {_sha256(path)}:
                errors.append(f"replay minute source hash mismatch: {path}")
                continue
            if "minute_chunks" not in path.parts:
                if any((path.parent.parent / member).is_file() for member in (
                        "finmind_replay_source.json", "finmind_replay_legacy_source.json")):
                    try:
                        if len(days) != 1:
                            raise ValueError("FinMind projection must own exactly one session")
                        pins = _verified_finmind_minute_projection(
                            path, day=next(iter(days)).isoformat(), cache=finmind_projection_cache)
                        if not pins:
                            raise ValueError("FinMind projection evidence is missing")
                        for source_name, digest in pins.items():
                            if (source_name in finmind_projection_hashes
                                    and finmind_projection_hashes[source_name] != digest):
                                raise ValueError("FinMind projection source pins conflict")
                            finmind_projection_hashes[source_name] = digest
                        source_bars.append(_audit_source_bar_projection(pl.scan_parquet(path))
                            .with_columns(pl.lit(str(path)).alias("_source_path"))
                            .join(needs.lazy(), on=["symbol", "minute_key"], how="semi").collect())
                        continue
                    except (OSError, ValueError, RuntimeError, KeyError) as exc:
                        errors.append(f"invalid FinMind minute projection {name}: {exc}")
                        continue
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
                        source_bars.append(_audit_source_bar_projection(pl.scan_parquet(path))
                            .with_columns(pl.lit(str(path)).alias("_source_path"))
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
            source_bars.append(_audit_source_bar_projection(
                pl.scan_parquet(path).filter(pl.col("date").is_in(sorted(days))))
                .with_columns(pl.lit(str(path)).alias("_source_path"))
                .join(needs.lazy(), on=["symbol", "minute_key"], how="semi").collect())
        if source_bars or retained_opening_bars:
            retained_by_key = {
                (row["symbol"], row["minute_key"]): row
                for row in retained_opening_bars
            }
            if len(retained_by_key) != len(retained_opening_bars):
                errors.append("retained opening evidence has duplicate symbol/minute")
            bars = {}
            price_only_sources = {}
            if source_bars:
                source_frame = pl.concat(source_bars)
                if full_target_counterfactual:
                    price_only_sources = _full_0901_price_only_sources(
                        source_frame, retained_local_opening_prices)
                frame = _selected_replay_source_bars(
                    source_frame, replay_symbol_sources).unique()
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
                if fill.get("prior_paper_fill_reused") or fill.get("historical_open_fallback_fill"):
                    continue
                if fill.get("terminal_close_receipt") is not None:
                    # Fully verified separately against the exact official raw
                    # close; never add user-assumed liquidity to minute volume.
                    continue
                key = (fill["symbol"], fill["recorded_at"][:16])
                if _is_verified_full_0901_price_only_entry(
                    fill, price_only_sources, full_target=full_target_counterfactual
                ):
                    full_0901_price_only_entries += 1
                    continue
                bar = bars.get(key)
                if bar is None:
                    errors.append(f"fill minute has no verified source: {key}")
                    continue
                price = float(fill["price"])
                if not bar["low"] - 1e-7 <= price <= bar["high"] + 1e-7:
                    errors.append(f"fill outside source OHLC range: {key}: {price}")
                volume = bar.get("volume_shares")
                if volume is None or not math.isfinite(volume) or volume <= 0:
                    errors.append(f"fill minute has no verified share volume: {key}")
                    continue
                if fill.get("fill_contract") == HISTORICAL_BRACKET_FULL_CROSS_CONTRACT:
                    entry = bracket_entries.get((fill["market"], fill["position_id"])) or {}
                    if not _verified_full_cross_assumption(fill, bar, entry, rebuild):
                        errors.append(f"invalid full-cross bracket assumption: {fill['market']}:{key}")
                    else:
                        full_cross_assumptions += 1
                    continue
                usage[(fill["market"], *key)] += int(fill["quantity"])
            for (market, symbol, minute), quantity in usage.items():
                capacity = math.floor(bars[(symbol, minute)]["volume_shares"] * .5 / 1000) * 1000
                if not full_target_counterfactual and quantity > capacity:
                    errors.append(f"shared minute capacity exceeded: {market}:{symbol}:{minute}: {quantity}>{capacity}")
    if before != {name: _sha256(state_dir / name) for name in names}:
        raise RuntimeError("replay changed during audit; rerun on completed candidate")
    if any(_sha256(Path(name)) != digest for name, digest in open_fallback_hashes.items()):
        raise RuntimeError("official-open fallback evidence changed during audit")
    if calendar_check and _sha256(Path(calendar_check["path"])) != calendar_check["sha256"]:
        raise RuntimeError("calendar changed during replay audit")
    if any(_sha256(Path(name)) != digest for name, digest in action_source_hashes.items()):
        raise RuntimeError("corporate-action source changed during replay audit")
    if any(_sha256(Path(name)) != digest for name, digest in terminal_source_hashes.items()):
        raise RuntimeError("terminal close source changed during replay audit")
    if any(_sha256(Path(name)) != digest for name, digest in research_source_hashes.items()):
        raise RuntimeError("research close source changed during replay audit")
    if any(_sha256(Path(name)) != digest for name, digest in finmind_projection_hashes.items()):
        raise RuntimeError("FinMind minute source changed during replay audit")
    for name, signature in minute_source_signatures.items():
        stat = Path(name).stat()
        if (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns) != signature:
            raise RuntimeError(f"minute source changed during audit: {name}")
    nontrade_hashes = {name: digest for row in nontrade_rows
                       for name, digest in row.get("_nontrade_verified_source_hashes", {}).items()}
    if any(_sha256(Path(name)) != digest for name, digest in nontrade_hashes.items()):
        raise RuntimeError("nontrade source changed during audit")
    return {"contract": ("carried_inventory_fill_cash_cost_minute_reconciliation_v1" if require_margin_carry
                         else "strict_intraday_fill_cash_cost_minute_reconciliation_v1"), "passed": not errors,
            "scope": "completed_prefix_only" if incomplete else "requested_range",
            "full_requested_range_passed": not errors and not incomplete and verify_sources,
            "sources_verified": bool(verify_sources),
            "incomplete_session": rebuild["sessions"][incomplete[0]]["session_date"] if incomplete else None,
            "session_count": len(dates), "start_date": dates[0], "end_date": dates[-1],
            "official_calendar_verification": calendar_check,
            "minute_stats": minute_stats, "accounts": accounts, "max_accounting_error_twd": max_error,
            "source_files_checked": len(source_days) if verify_sources else 0,
            "full_0901_source_close_without_trade_entries": full_0901_price_only_entries,
            "official_open_0901_assumption_entries": sum(bool(row.get("historical_open_fallback_fill")) for row in fills),
            "official_open_fallback_source_hashes": open_fallback_hashes,
            "provisional_full_cross_bracket_assumptions": full_cross_assumptions,
            "provisional_nontrade_accounting_assumptions": len(nontrade_rows),
            "flat_sessions_without_position_minute_source": len(
                _source_free_flat_sessions(sessions, fills, marks, nontrade_rows)),
            "provisional_assumption_only_sessions_without_minute_source": len(assumed_without_sources),
            "all_rejected_entry_prices_independently_verified": False,
            "price_only_0901_entries_claim_executable_liquidity": False,
            "source_hashes": before, "corporate_action_source_hashes": action_source_hashes,
            "terminal_close_source_hashes": terminal_source_hashes,
            "research_close_source_hashes": research_source_hashes,
            "finmind_projection_source_hashes": finmind_projection_hashes,
            "nontrade_source_hashes": nontrade_hashes,
            "minute_source_signatures": minute_source_signatures,
            **({"provisional_minute_source_coverage": True,
                "complete_minute_source_coverage_verified": False,
                "provisional_carried_mark_rows": sum(bool(row.get("valuation_stale")) for row in marks),
                "coverage_note": "Accounting/source identities checked; unknown minute gaps remain provisional."}
               if provisional else {}),
            "errors": errors[:100], "error_count": len(errors)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--completed-prefix", action="store_true",
                        help="Audit only the untouched completed prefix; never a promotion receipt.")
    parser.add_argument("--strict-intraday", action="store_true",
                        help="Audit a source-backed, flat intraday candidate without assuming margin carry.")
    args = parser.parse_args()
    try:
        result = audit(args.state_dir, completed_prefix=args.completed_prefix,
                       require_margin_carry=not args.strict_intraday)
    except (OSError, ValueError, KeyError, RuntimeError) as exc:
        result = {"passed": False, "full_requested_range_passed": False,
                  "error": f"{type(exc).__name__}: {exc}", "state_dir": str(args.state_dir)}
    _atomic_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result["full_requested_range_passed"] else 2)


if __name__ == "__main__":
    main()

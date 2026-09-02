#!/usr/bin/env python3
"""Rebuild day-trade paper sessions from official opens and 09:01 prices.

The active replay contract uses the official 09:00 session open only for model
inference and whole-lot sizing, then values paper execution at the observed
right-labelled 09:01 minute VWAP. Missing opens or VWAPs fail closed; no
Bid/Ask, last price, opening-price fill, or adverse-tick substitute is allowed.
This is explicitly counterfactual and is never labelled as a live exchange fill.

Legacy forensic modes remain available. With
``--paper-market-at-best`` it queries each actionable symbol's first
historical Shioaji level-one quote during the 09:00 minute.  Buys and covers use
the best ask; sells and shorts use the best bid, and the complete requested
paper quantity is filled at that observed price without claiming exchange
depth or queue priority. If the required side is unavailable, the row is
separately labelled and filled at the official session open moved one adverse
legal tick. The fallback is never represented as a received book or broker fill.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from functools import lru_cache
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.run_tw_day_trade_simulation import _mode_specs, _rule_data_dir  # noqa: E402
from downloader.download_tw_public_data import (  # noqa: E402
    DEFAULT_DATASETS,
    _parse_historical_response_content,
    _validated_taiex_session_dates,
)
from stockagent.live.tw_day_trade_simulation import (  # noqa: E402
    ENTRY_FILL_POLICY_0901_MINUTE_VWAP,
    ENTRY_FILL_POLICY_CAUSAL_BOOK,
    ENTRY_FILL_POLICY_CAUSAL_BOOK_ELSE_OPEN_TICK,
    ENTRY_FILL_POLICY_MARKET_AT_BEST_ELSE_OPEN_TICK,
    ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901,
    ModeSpec,
    TwDayTradeSimulationEngine,
    load_live_eligibility,
)
from stockagent.live.quote_provider import (  # noqa: E402
    fetch_shioaji_historical_stock_0901_vwaps,
    fetch_shioaji_historical_stock_entry_books,
    fetch_shioaji_stock_snapshots,
)
from stockagent.data.tw_price_rules import move_price_ticks_numpy  # noqa: E402


TAIPEI = ZoneInfo("Asia/Taipei")
PAPER_LIQUIDITY_LOTS = 1_000_000_000.0
DEFAULT_TWSE_DAILY_OHLCV_PATH = Path(
    "/srv/stockagent-live/data_tw_public/twse_daily_ohlcv.parquet"
)
DEFAULT_TPEX_DAILY_OHLCV_PATH = Path(
    "/srv/stockagent-live/data_tw_public/tpex_daily_ohlcv.parquet"
)
DEFAULT_MINUTE_DATA_ROOTS = (
    Path("data_tw_minute/shioaji_1m"),
    Path("data_tw_minute/research_dataset"),
)
MINUTE_VOLUME_PARTICIPATION = 0.50
HISTORICAL_KBAR_FILL_CONTRACT = (
    "historical_1m_ohlcv_counterfactual_not_executable_bid_ask"
)
HISTORICAL_OFFICIAL_CLOSE_FILL_CONTRACT = (
    "historical_official_close_auction_counterfactual_no_depth_claim"
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _finite(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0.0 else None


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _retain_benchmark_history(source_path: Path, state_dir: Path) -> dict[str, Any]:
    """Carry the read-only dashboard benchmark ledger into a replay candidate."""

    source_path = source_path.resolve()
    raw = source_path.read_bytes()
    payload = json.loads(raw)
    if not isinstance(payload, dict) or not isinstance(payload.get("marks"), list):
        raise ValueError(f"invalid benchmark history ledger: {source_path}")
    required_origins = {
        "benchmark_0050",
        "benchmark_2330",
        "benchmark_tx_continuous",
    }
    origins = payload.get("origins") or {}
    if not isinstance(origins, Mapping) or not required_origins.issubset(origins):
        raise ValueError(
            "benchmark history source is incomplete: "
            f"{sorted(required_origins - set(origins))}"
        )
    destination = state_dir / "benchmark_history.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_bytes(raw)
    temporary.replace(destination)
    return {
        "path": str(source_path),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "marks": len(payload["marks"]),
        "origins": sorted(origins),
        "destination": str(destination),
    }


def _retained_rule_response_kind(dataset: str, raw: bytes) -> str:
    """Identify the retained official schema without changing its contents."""

    if dataset != "twse_day_trade_eligibility":
        return "json"
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"retained {dataset} receipt is not valid JSON") from exc
    return "twse_day_trade_openapi_json" if isinstance(payload, list) else "json"


def _is_weekend(day: date) -> bool:
    return day.weekday() >= 5


def _require_received_entry_book(specs: list[ModeSpec]) -> None:
    causal_markets = sorted(
        spec.market
        for spec in specs
        if spec.entry_fill_policy == ENTRY_FILL_POLICY_CAUSAL_BOOK
    )
    if causal_markets:
        raise RuntimeError(
            "open-only replay is retired for causal best-quote execution; "
            "official open data has no received best bid/ask or displayed depth "
            f"for markets={causal_markets}"
        )


def _latest_valid_signal(
    spec: ModeSpec,
    trading_date: date,
    *,
    preferred_signal_id: str | None = None,
) -> tuple[datetime, Path, Path, dict[str, Any], list[dict[str, Any]]]:
    ranked: list[tuple[datetime, Path, Path, dict[str, Any]]] = []
    day = trading_date.isoformat()
    for summary_path in spec.live_output_dir.glob(f"{day}*/**/summary.json"):
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            generated_at = datetime.fromisoformat(str(summary.get("generated_at")))
        except (OSError, TypeError, ValueError):
            continue
        if generated_at.tzinfo is None:
            generated_at = generated_at.replace(tzinfo=TAIPEI)
        generated_at = generated_at.astimezone(TAIPEI)
        is_counterfactual = bool(summary.get("counterfactual_signal_regeneration"))
        if is_counterfactual:
            try:
                replay_effective_at = datetime.fromisoformat(
                    str(summary.get("replay_effective_signal_at"))
                )
            except (TypeError, ValueError):
                continue
            if replay_effective_at.tzinfo is None:
                replay_effective_at = replay_effective_at.replace(tzinfo=TAIPEI)
            replay_effective_at = replay_effective_at.astimezone(TAIPEI)
            provenance = summary.get("counterfactual_open_provenance")
            counterfactual_contract_valid = (
                replay_effective_at.date() == trading_date
                and bool(summary.get("simulation_only"))
                and summary.get("production_order_possible") is False
                and isinstance(provenance, dict)
                and str(provenance.get("source") or "") == "official_daily_session_open"
                and bool(provenance.get("input_path"))
                and bool(provenance.get("input_sha256"))
            )
        else:
            counterfactual_contract_valid = False
        weights_path = summary_path.with_name("target_weights.parquet")
        if (
            (preferred_signal_id and summary.get("signal_id") != preferred_signal_id)
            or str(summary.get("market") or "")
            != str(spec.signal_market or spec.market)
            or str(summary.get("execution_mode") or "") != "tw_day_trade"
            or not bool(summary.get("live_session_open_feature_applied"))
            or not (
                generated_at.date() == trading_date or counterfactual_contract_valid
            )
            or not weights_path.is_file()
        ):
            continue
        ranked.append((generated_at, summary_path, weights_path, summary))
    if not ranked:
        preferred = f" signal_id={preferred_signal_id!r}" if preferred_signal_id else ""
        raise FileNotFoundError(
            f"{spec.market}: no open-feature-applied live signal for {day}{preferred}"
        )
    generated_at, summary_path, weights_path, summary = max(
        ranked, key=lambda item: item[0]
    )
    rows = pl.read_parquet(weights_path).to_dicts()
    if not rows:
        raise ValueError(f"{weights_path} contains no target rows")
    return generated_at, summary_path, weights_path, summary, rows


def _source_ledger_signal_ids(
    source_ledger_dir: Path,
    *,
    start_date: date,
    end_date: date,
) -> tuple[dict[tuple[str, str], str], dict[str, Any]]:
    path = source_ledger_dir / "events.jsonl"
    if not path.is_file():
        raise FileNotFoundError(path)
    resolved: dict[tuple[str, str], str] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                row = json.loads(line)
                recorded_at = datetime.fromisoformat(str(row.get("recorded_at") or ""))
            except (json.JSONDecodeError, ValueError) as exc:
                raise ValueError(f"{path}:{line_number}: invalid event row") from exc
            if row.get("event") != "signal_registered":
                continue
            session_date = recorded_at.date()
            if session_date < start_date or session_date > end_date:
                continue
            market = str(row.get("market") or "")
            signal_id = str(row.get("signal_id") or "")
            if not market or not signal_id:
                raise ValueError(f"{path}:{line_number}: signal identity is incomplete")
            key = (session_date.isoformat(), market)
            previous = resolved.get(key)
            if previous is not None and previous != signal_id:
                raise ValueError(
                    f"{path}:{line_number}: conflicting source signal ids for {key}"
                )
            resolved[key] = signal_id
    return resolved, {
        "path": str(path.resolve()),
        "sha256": _sha256(path),
        "signal_registrations": len(resolved),
    }


def _load_retained_historical_entry_books(
    *,
    historical_book_root: Path,
    trading_date: date,
    allow_missing: bool = False,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    path = historical_book_root / f"{trading_date.isoformat()}.parquet"
    if not path.is_file():
        if allow_missing:
            return {}, {
                "source": "retained_historical_entry_book_missing",
                "source_path": str(path.resolve()),
                "source_rows": 0,
                "source_missing": True,
                "additional_shioaji_requests": 0,
                "fallback_authorized": True,
            }
        raise FileNotFoundError(path)
    frame = pl.read_parquet(path)
    if "symbol" not in frame.columns:
        raise ValueError(f"{path} missing symbol column")
    books = {
        str(row["symbol"]): row
        for row in frame.to_dicts()
        if str(row.get("symbol") or "")
    }
    return books, {
        "source": "retained_historical_entry_book",
        "source_path": str(path.resolve()),
        "source_sha256": _sha256(path),
        "source_rows": frame.height,
        "additional_shioaji_requests": 0,
    }


def _local_0901_vwap_rows(
    *,
    minute_roots: tuple[Path, ...],
    symbols: list[str],
    trading_date: date,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Resolve 09:01 VWAPs from canonical local minute data before Shioaji."""

    requested = set(symbols)
    resolved: dict[str, dict[str, Any]] = {}
    source_counts: dict[str, int] = {}

    def accept(frame: pl.DataFrame, source: Path) -> None:
        if not frame.height:
            return
        names = set(frame.columns)
        if "minutes_from_open" in names:
            frame = frame.filter(pl.col("minutes_from_open") == 1)
        elif "ts" in names:
            frame = frame.with_columns(pl.col("ts").cast(pl.Datetime("ns"))).filter(
                (pl.col("ts").dt.hour() == 9) & (pl.col("ts").dt.minute() == 1)
            )
        if "date" in frame.columns:
            frame = frame.filter(pl.col("date").cast(pl.Date) == trading_date)
        for row in frame.iter_rows(named=True):
            symbol = str(row.get("symbol") or "")
            if symbol not in requested or symbol in resolved:
                continue
            try:
                amount = float(row.get("Amount"))
            except (TypeError, ValueError):
                amount = math.nan
            try:
                volume_shares = float(row.get("volume_shares"))
            except (TypeError, ValueError):
                volume_shares = math.nan
            if not (math.isfinite(volume_shares) and volume_shares > 0.0):
                try:
                    raw_volume = float(row.get("Volume"))
                except (TypeError, ValueError):
                    raw_volume = math.nan
                candidates: list[float] = []
                try:
                    contract_unit = float(row.get("contract_unit"))
                    if math.isfinite(contract_unit) and contract_unit > 0.0:
                        candidates.append(contract_unit)
                except (TypeError, ValueError):
                    pass
                candidates.extend([1_000.0, 100.0, 10.0, 1.0])
                try:
                    low = float(row.get("Low"))
                    high = float(row.get("High"))
                except (TypeError, ValueError):
                    low, high = math.nan, math.nan
                for multiplier in dict.fromkeys(candidates):
                    candidate_shares = raw_volume * multiplier
                    candidate_price = (
                        amount / candidate_shares
                        if candidate_shares > 0.0
                        else math.nan
                    )
                    if (
                        math.isfinite(candidate_price)
                        and candidate_price > 0.0
                        and math.isfinite(low)
                        and math.isfinite(high)
                        and low * 0.999 <= candidate_price <= high * 1.001
                    ):
                        volume_shares = candidate_shares
                        break
            vwap = (
                amount / volume_shares
                if math.isfinite(amount)
                and amount > 0.0
                and math.isfinite(volume_shares)
                and volume_shares > 0.0
                else math.nan
            )
            if not math.isfinite(vwap) or vwap <= 0.0:
                continue
            row_source = Path(str(row.get("_source_path") or source))
            source_text = f"local_minute_parquet_0901_vwap:{row_source.resolve()}"
            resolved[symbol] = {
                "symbol": symbol,
                "execution_price_0901": float(vwap),
                "tick_volume_units_0901": float(volume_shares),
                "tick_count_0901": 0,
                "source_window_start": datetime.combine(
                    trading_date, time(9, 0), tzinfo=TAIPEI
                ).isoformat(timespec="seconds"),
                "source_window_end": datetime.combine(
                    trading_date, time(9, 0, 59), tzinfo=TAIPEI
                ).isoformat(timespec="seconds"),
                "quote_at": datetime.combine(
                    trading_date, time(9, 1), tzinfo=TAIPEI
                ).isoformat(timespec="seconds"),
                "source": source_text,
            }
            source_counts[source_text] = source_counts.get(source_text, 0) + 1

    for root in minute_roots:
        unresolved = requested - set(resolved)
        if not unresolved:
            break
        # Canonical receipt-backed source chunks are checked before the
        # materialized research partition, matching the storage contract.
        chunk_paths: list[Path] = []
        for symbol in sorted(unresolved):
            for path in sorted((root / "minute_chunks" / symbol).glob("*.parquet")):
                boundaries = path.stem.split("_")
                day_text = trading_date.isoformat()
                if len(boundaries) != 2 or not (
                    boundaries[0] <= day_text <= boundaries[1]
                ):
                    continue
                chunk_paths.append(path)
        schema_groups: dict[tuple[str, ...], list[Path]] = {}
        for path in chunk_paths:
            schema_groups.setdefault(
                tuple(pl.read_parquet_schema(path).names()), []
            ).append(path)
        for schema_tuple, paths in schema_groups.items():
            schema = set(schema_tuple)
            columns = [
                name
                for name in (
                    "symbol",
                    "date",
                    "ts",
                    "Amount",
                    "Volume",
                    "volume_shares",
                    "contract_unit",
                    "Low",
                    "High",
                )
                if name in schema
            ]
            lazy = pl.scan_parquet(
                sorted(paths), include_file_paths="_source_path"
            )
            if "date" in schema:
                lazy = lazy.filter(pl.col("date").cast(pl.Date) == trading_date)
            chunk_frame = lazy.select(*columns, "_source_path").collect(
                engine="streaming"
            )
            accept(chunk_frame, root)
        unresolved = requested - set(resolved)
        partition = root / f"trade_date={trading_date.isoformat()}" / "data.parquet"
        if unresolved and partition.is_file():
            schema = pl.scan_parquet(partition).collect_schema().names()
            columns = [
                name
                for name in (
                    "symbol",
                    "date",
                    "ts",
                    "minutes_from_open",
                    "Amount",
                    "Volume",
                    "volume_shares",
                    "contract_unit",
                    "Low",
                    "High",
                )
                if name in schema
            ]
            frame = (
                pl.scan_parquet(partition)
                .filter(pl.col("symbol").is_in(sorted(unresolved)))
                .select(columns)
                .collect()
            )
            accept(frame, partition)
    return resolved, {
        "source": "local_minute_data_0901_vwap",
        "requested_symbols": len(requested),
        "resolved_symbols": len(resolved),
        "source_counts": source_counts,
        "additional_shioaji_requests": 0,
    }


def _load_price_limits(path: Path) -> dict[str, dict[str, Any]]:
    required = {
        "symbol",
        "reference_price",
        "upper_limit_price",
        "lower_limit_price",
    }
    frame = pl.read_parquet(path)
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{path} missing price-limit columns: {sorted(missing)}")
    return {
        str(row["symbol"]): row for row in frame.select(sorted(required)).to_dicts()
    }


def _materialize_exact_session_rules(
    *,
    source_root: Path,
    destination_root: Path,
    trading_date: date,
) -> dict[str, dict[str, Any]]:
    """Restore exact-session eligibility parquet from retained official raw JSON."""

    provenance: dict[str, dict[str, Any]] = {}
    destination_root.mkdir(parents=True, exist_ok=True)
    for dataset in (
        "twse_day_trade_eligibility",
        "tpex_day_trade_eligibility",
    ):
        raw_path = source_root / "raw" / dataset / f"{trading_date.isoformat()}.json"
        if not raw_path.is_file():
            raise FileNotFoundError(
                f"retained official exact-session rule is missing: {raw_path}"
            )
        raw = raw_path.read_bytes()
        frame, _suffix = _parse_historical_response_content(
            DEFAULT_DATASETS[dataset],
            trading_date,
            raw,
            _retained_rule_response_kind(dataset, raw),
        )
        destination = destination_root / f"{dataset}.parquet"
        frame.write_parquet(destination)
        provenance[dataset] = {
            "raw_path": str(raw_path),
            "raw_sha256": hashlib.sha256(raw).hexdigest(),
            "normalized_path": str(destination),
            "normalized_sha256": _sha256(destination),
            "rows": frame.height,
        }
    return provenance


def _entry_quotes(
    rows: list[dict[str, Any]],
    limits: Mapping[str, Mapping[str, Any]],
    *,
    quote_at: datetime,
    spec: ModeSpec,
    canonical_open_by_symbol: Mapping[str, float],
    canonical_open_source: str,
    historical_books: Mapping[str, Mapping[str, Any]] | None = None,
    official_no_trade_symbols: set[str] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    if spec.entry_fill_policy == ENTRY_FILL_POLICY_CAUSAL_BOOK:
        raise RuntimeError(
            "open-only replay has no received best bid/ask or displayed depth; "
            "refusing to fabricate an execution book for causal_best_quote"
        )
    historical_books = historical_books or {}
    quotes: dict[str, dict[str, Any]] = {}
    potential_whole_lot_rows = 0
    official_open_covered_rows = 0
    exact_best_quote_rows = 0
    adverse_tick_fallback_rows = 0
    exact_best_quote_symbols: list[str] = []
    adverse_tick_fallback_symbols: list[str] = []
    observed_0901_vwap_symbols: list[str] = []
    missing_official_open_symbols: list[str] = []
    official_open_mismatches: list[dict[str, Any]] = []
    for row in rows:
        symbol = str(row.get("symbol") or "")
        if not symbol:
            continue
        try:
            recorded_signal_open = float(row.get("open_price"))
        except (TypeError, ValueError):
            recorded_signal_open = math.nan
        try:
            opening_price = float(canonical_open_by_symbol.get(symbol, math.nan))
        except (TypeError, ValueError):
            opening_price = math.nan
        target_weight = abs(float(row.get("target_weight") or 0.0))
        side = (
            "long"
            if float(row.get("target_weight") or 0.0) > 0.0
            else "short"
            if float(row.get("target_weight") or 0.0) < 0.0
            else "flat"
        )
        potentially_executable = (
            math.isfinite(opening_price)
            and opening_price > 0.0
            and target_weight * float(spec.initial_capital_twd) / opening_price
            >= int(spec.lot_size)
        )
        if potentially_executable:
            potential_whole_lot_rows += 1
        if potentially_executable:
            if not (math.isfinite(opening_price) and opening_price > 0.0):
                missing_official_open_symbols.append(symbol)
            else:
                official_open_covered_rows += 1
                if not math.isclose(
                    recorded_signal_open,
                    opening_price,
                    rel_tol=0.0,
                    abs_tol=1e-9,
                ):
                    official_open_mismatches.append(
                        {
                            "symbol": symbol,
                            "recorded_signal_open": recorded_signal_open,
                            "canonical_session_open": opening_price,
                        }
                    )
        evidence = limits.get(symbol) or {}
        serialized_open = (
            opening_price
            if math.isfinite(opening_price) and opening_price > 0.0
            else None
        )
        book = historical_books.get(symbol) or {}
        is_0901_vwap_policy = (
            spec.entry_fill_policy == ENTRY_FILL_POLICY_0901_MINUTE_VWAP
        )
        transaction_price_key = "ask" if side == "long" else "bid"
        transaction_volume_key = "ask_volume" if side == "long" else "bid_volume"
        source_quote_at_key = "ask_quote_at" if side == "long" else "bid_quote_at"
        try:
            transaction_price = float(book.get(transaction_price_key))
            transaction_volume = float(book.get(transaction_volume_key))
        except (TypeError, ValueError):
            transaction_price = math.nan
            transaction_volume = math.nan
        has_required_best_quote = bool(
            potentially_executable
            and math.isfinite(transaction_price)
            and transaction_price > 0.0
            and math.isfinite(transaction_volume)
            and transaction_volume > 0.0
        )
        try:
            execution_price_0901 = float(book.get("execution_price_0901"))
        except (TypeError, ValueError):
            execution_price_0901 = math.nan
        has_0901_vwap = bool(
            potentially_executable
            and math.isfinite(execution_price_0901)
            and execution_price_0901 > 0.0
        )
        use_adverse_tick_fallback = bool(
            potentially_executable
            and spec.entry_fill_policy
            in {
                ENTRY_FILL_POLICY_CAUSAL_BOOK_ELSE_OPEN_TICK,
                ENTRY_FILL_POLICY_MARKET_AT_BEST_ELSE_OPEN_TICK,
            }
            and not has_required_best_quote
        )
        if has_required_best_quote:
            exact_best_quote_rows += 1
            exact_best_quote_symbols.append(symbol)
        elif use_adverse_tick_fallback:
            adverse_tick_fallback_rows += 1
            adverse_tick_fallback_symbols.append(symbol)
        if is_0901_vwap_policy and has_0901_vwap:
            observed_0901_vwap_symbols.append(symbol)
        source_quote_at = book.get(source_quote_at_key) or book.get("quote_at")
        entry_price_source = (
            str(book.get("source") or "observed_right_labelled_09_01_minute_vwap")
            if is_0901_vwap_policy and has_0901_vwap
            else "missing_observed_09_01_minute_vwap"
            if is_0901_vwap_policy
            else
            f"shioaji:historical_stock_tick_best_{transaction_price_key}"
            if has_required_best_quote
            else "official_daily_session_open:adverse_one_legal_tick_fallback"
            if use_adverse_tick_fallback
            else canonical_open_source
        )
        quotes[symbol] = {
            "symbol": symbol,
            "open": serialized_open,
            "last": (
                execution_price_0901
                if is_0901_vwap_policy and has_0901_vwap
                else book.get("last") or serialized_open
            ),
            "bid": book.get("bid") if has_required_best_quote else None,
            "ask": book.get("ask") if has_required_best_quote else None,
            "bid_volume": (book.get("bid_volume") if has_required_best_quote else None),
            "ask_volume": (book.get("ask_volume") if has_required_best_quote else None),
            "minute_volume_lots": None,
            "execution_price_0901": (
                execution_price_0901 if has_0901_vwap else None
            ),
            "upper_limit": evidence.get("upper_limit_price"),
            "lower_limit": evidence.get("lower_limit_price"),
            "reference_price": evidence.get("reference_price"),
            "official_session_no_trade_print": bool(
                official_no_trade_symbols and symbol in official_no_trade_symbols
            ),
            "quote_at": quote_at.isoformat(timespec="seconds"),
            "historical_source_quote_at": (
                book.get("source_window_end")
                if is_0901_vwap_policy
                else source_quote_at
            ),
            "source": (
                str(book.get("source") or "observed_right_labelled_09_01_minute_vwap")
                if is_0901_vwap_policy and has_0901_vwap
                else "missing_observed_09_01_minute_vwap"
                if is_0901_vwap_policy
                else
                "shioaji:historical_stock_tick_best_quote"
                if has_required_best_quote
                else "synthetic:adverse_open_tick_fallback"
                if use_adverse_tick_fallback
                else canonical_open_source
            ),
            "entry_price_source": entry_price_source,
            "entry_price_is_synthetic_fallback": use_adverse_tick_fallback,
        }
    nonzero_target_symbols = {
        str(row.get("symbol") or "")
        for row in rows
        if str(row.get("symbol") or "")
        and abs(float(row.get("target_weight") or 0.0)) > 0.0
    }
    observed_0901_vwap_symbol_set = set(observed_0901_vwap_symbols)
    return quotes, {
        "potential_whole_lot_rows": potential_whole_lot_rows,
        "official_open_covered_rows": official_open_covered_rows,
        "missing_official_open_symbols": sorted(missing_official_open_symbols),
        "official_open_mismatches": official_open_mismatches,
        "canonical_open_source": canonical_open_source,
        "exact_best_quote_rows": exact_best_quote_rows,
        "exact_best_quote_symbols": sorted(set(exact_best_quote_symbols)),
        "adverse_tick_fallback_rows": adverse_tick_fallback_rows,
        "adverse_tick_fallback_symbols": sorted(set(adverse_tick_fallback_symbols)),
        "observed_0901_vwap_rows": len(observed_0901_vwap_symbols),
        "observed_0901_vwap_symbols": sorted(set(observed_0901_vwap_symbols)),
        "missing_0901_vwap_symbols": sorted(
            nonzero_target_symbols - observed_0901_vwap_symbol_set
        ),
    }


def _canonicalize_signal_rows_for_replay(
    rows: list[dict[str, Any]],
    canonical_open_by_symbol: Mapping[str, float],
) -> list[dict[str, Any]]:
    """Prevent the executor from falling back to a non-canonical signal open."""

    canonicalized: list[dict[str, Any]] = []
    for source_row in rows:
        row = dict(source_row)
        symbol = str(row.get("symbol") or "")
        row["source_signal_open_price"] = row.get("open_price")
        opening = canonical_open_by_symbol.get(symbol)
        row["open_price"] = (
            float(opening)
            if opening is not None
            and math.isfinite(float(opening))
            and float(opening) > 0.0
            else None
        )
        canonicalized.append(row)
    return canonicalized


def _historical_book_request_symbols(
    prepared_modes: list[
        tuple[
            ModeSpec,
            datetime,
            Path,
            Path,
            dict[str, Any],
            list[dict[str, Any]],
            Mapping[str, Any],
            Mapping[str, Any],
        ]
    ],
    canonical_open_by_symbol: Mapping[str, float],
) -> list[str]:
    """Return only rows that can reach an opening whole-lot order."""

    symbols: set[str] = set()
    for (
        spec,
        _generated,
        _summary_path,
        _weights_path,
        _summary,
        rows,
        eligibility,
        _coverage,
    ) in prepared_modes:
        for row in rows:
            symbol = str(row.get("symbol") or "")
            if not symbol:
                continue
            try:
                weight = float(row.get("target_weight") or 0.0)
                opening_price = float(canonical_open_by_symbol.get(symbol, math.nan))
            except (TypeError, ValueError):
                continue
            evidence = eligibility.get(symbol)
            if (
                weight == 0.0
                or not bool(row.get("tradable"))
                or (weight > 0.0 and not bool(row.get("can_buy")))
                or (weight < 0.0 and not bool(row.get("can_sell")))
                or evidence is None
                or not bool(evidence.covered)
                or not bool(evidence.eligible)
                or (weight < 0.0 and not bool(evidence.short_open))
                or not math.isfinite(opening_price)
                or opening_price <= 0.0
            ):
                continue
            requested_shares = int(
                math.floor(
                    abs(weight)
                    * float(spec.initial_capital_twd)
                    / opening_price
                    / int(spec.lot_size)
                )
            ) * int(spec.lot_size)
            if requested_shares > 0:
                symbols.add(symbol)
    return sorted(symbols)


def _persist_historical_entry_books(
    *,
    state_dir: Path,
    trading_date: date,
    books: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    path = state_dir / "replay_entry_books" / f"{trading_date.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = {
        "symbol": pl.String,
        "bid": pl.Float64,
        "ask": pl.Float64,
        "bid_volume": pl.Float64,
        "ask_volume": pl.Float64,
        "bid_quote_at": pl.String,
        "ask_quote_at": pl.String,
        "bid_timestamp_ns": pl.Int64,
        "ask_timestamp_ns": pl.Int64,
        "bid_source_row_index": pl.Int64,
        "ask_source_row_index": pl.Int64,
        "last": pl.Float64,
        "execution_price_0901": pl.Float64,
        "tick_volume_units_0901": pl.Float64,
        "tick_count_0901": pl.Int64,
        "source_window_start": pl.String,
        "source_window_end": pl.String,
        "quote_at": pl.String,
        "source": pl.String,
    }
    normalized = [
        {column: row.get(column) for column in schema}
        for _symbol, row in sorted(books.items())
    ]
    pl.DataFrame(normalized, schema=schema).write_parquet(path)
    return {
        "path": str(path),
        "sha256": _sha256(path),
        "rows": len(normalized),
    }


@lru_cache(maxsize=None)
def _official_aggregate_daily_rows(
    twse_path: Path,
    tpex_path: Path,
    trading_date: date,
) -> dict[str, dict[str, Any]]:
    """Load one official session from the venue aggregates as a lag fallback."""

    def _price(column: str, alias: str) -> pl.Expr:
        return (
            pl.col(column)
            .cast(pl.String)
            .str.strip_chars()
            .str.replace_all(",", "")
            .cast(pl.Float64, strict=False)
            .alias(alias)
        )

    frames: list[pl.DataFrame] = []
    sources = (
        (
            twse_path,
            "證券代號",
            {"open": "開盤價", "max": "最高價", "min": "最低價", "close": "收盤價"},
            "twse_daily_ohlcv",
        ),
        (
            tpex_path,
            "代號",
            {"open": "開盤", "max": "最高", "min": "最低", "close": "收盤"},
            "tpex_daily_ohlcv",
        ),
    )
    for path, symbol_column, price_columns, source in sources:
        if not path.is_file():
            continue
        frame = (
            pl.scan_parquet(path)
            .filter(pl.col("date").cast(pl.String) == trading_date.isoformat())
            .select(
                pl.col(symbol_column).cast(pl.String).str.strip_chars().alias("symbol"),
                *(
                    _price(source_column, target_column)
                    for target_column, source_column in price_columns.items()
                ),
                pl.lit(source).alias("_official_source"),
            )
            .collect(engine="streaming")
        )
        if not frame.is_empty():
            frames.append(frame)
    if not frames:
        return {}

    rows: dict[str, dict[str, Any]] = {}
    for row in pl.concat(frames, how="vertical").iter_rows(named=True):
        symbol = str(row.get("symbol") or "")
        if not symbol:
            continue
        if symbol in rows:
            raise ValueError(
                f"duplicate official aggregate daily row for {symbol} on {trading_date}"
            )
        rows[symbol] = {
            "date": trading_date,
            "open": row.get("open"),
            "max": row.get("max"),
            "min": row.get("min"),
            "close": row.get("close"),
            "_official_source": row.get("_official_source"),
        }
    return rows


@lru_cache(maxsize=None)
def _official_daily_row(
    parquet_root: Path,
    symbol: str,
    trading_date: date,
    twse_daily_ohlcv_path: Path = DEFAULT_TWSE_DAILY_OHLCV_PATH,
    tpex_daily_ohlcv_path: Path = DEFAULT_TPEX_DAILY_OHLCV_PATH,
) -> dict[str, Any] | None:
    path = parquet_root / f"{symbol}_features.parquet"
    if path.is_file():
        frame = (
            pl.scan_parquet(path)
            .filter(pl.col("date") == trading_date)
            .select("date", "open", "max", "min", "close")
            .collect()
        )
        if frame.height > 1:
            raise ValueError(
                f"duplicate per-symbol daily row for {symbol} on {trading_date}"
            )
        if frame.height == 1:
            return {
                **frame.row(0, named=True),
                "_official_source": "per_symbol_official_features",
            }
    return _official_aggregate_daily_rows(
        twse_daily_ohlcv_path,
        tpex_daily_ohlcv_path,
        trading_date,
    ).get(symbol)


def _official_open_map(
    specs: list[ModeSpec],
    selected: Mapping[
        str, tuple[datetime, Path, Path, dict[str, Any], list[dict[str, Any]]]
    ],
    trading_date: date,
    *,
    twse_daily_ohlcv_path: Path,
    tpex_daily_ohlcv_path: Path,
) -> tuple[dict[str, float], dict[str, int], set[str]]:
    opens: dict[str, float] = {}
    source_counts: dict[str, int] = {}
    no_trade_symbols: set[str] = set()
    # The venue aggregates are the direct official authority and already carry
    # the complete date slice.  Load each venue once instead of opening one
    # derived per-symbol parquet for every market row.  The per-symbol path is
    # retained only as a fallback for a symbol absent from both aggregates.
    aggregate_rows = _official_aggregate_daily_rows(
        twse_daily_ohlcv_path,
        tpex_daily_ohlcv_path,
        trading_date,
    )
    for spec in specs:
        rows = selected[spec.market][4]
        for row in rows:
            symbol = str(row.get("symbol") or "")
            if not symbol or symbol in opens:
                continue
            daily = aggregate_rows.get(symbol)
            if daily is None:
                daily = _official_daily_row(
                    spec.parquet_root,
                    symbol,
                    trading_date,
                    twse_daily_ohlcv_path,
                    tpex_daily_ohlcv_path,
                )
            if daily is None:
                continue
            opening = float(daily.get("open") or math.nan)
            if math.isfinite(opening) and opening > 0.0:
                opens[symbol] = opening
                source = str(daily.get("_official_source") or "unknown")
                source_counts[source] = source_counts.get(source, 0) + 1
            elif not any(
                value is not None and math.isfinite(float(value)) and float(value) > 0.0
                for value in (
                    daily.get("open"),
                    daily.get("max"),
                    daily.get("min"),
                    daily.get("close"),
                )
            ):
                # A retained venue row with every OHLC field empty is positive
                # evidence of no session trade print, not a downloader gap.
                no_trade_symbols.add(symbol)
    return opens, source_counts, no_trade_symbols


def _capture_current_open_map(
    *,
    specs: list[ModeSpec],
    selected: Mapping[
        str, tuple[datetime, Path, Path, dict[str, Any], list[dict[str, Any]]]
    ],
    state_dir: Path,
    trading_date: date,
) -> tuple[dict[str, float], dict[str, Any]]:
    fallback_by_symbol: dict[str, float] = {}
    for spec in specs:
        for row in selected[spec.market][4]:
            if abs(float(row.get("target_weight") or 0.0)) <= 0.0:
                continue
            symbol = str(row.get("symbol") or "")
            try:
                fallback = float(row.get("open_price"))
            except (TypeError, ValueError):
                fallback = math.nan
            if symbol and math.isfinite(fallback) and fallback > 0.0:
                fallback_by_symbol.setdefault(symbol, fallback)
    symbols = sorted(fallback_by_symbol)
    snapshot = fetch_shioaji_stock_snapshots(
        symbols,
        np.asarray([fallback_by_symbol[symbol] for symbol in symbols]),
        cache_ttl_seconds=0.0,
    )
    open_prices = np.asarray(snapshot.open_prices, dtype=np.float64)
    available = np.asarray(snapshot.available_mask, dtype=bool)
    timestamps = np.asarray(snapshot.timestamps_ms, dtype=np.int64)
    rows: list[dict[str, Any]] = []
    resolved: dict[str, float] = {}
    for index, symbol in enumerate(symbols):
        opening = float(open_prices[index])
        valid = bool(available[index]) and math.isfinite(opening) and opening > 0.0
        if valid:
            resolved[symbol] = opening
        rows.append(
            {
                "trading_date": trading_date,
                "symbol": symbol,
                "open_price": opening if valid else None,
                "snapshot_available": bool(available[index]),
                "timestamp_ms": int(timestamps[index]),
                "source": snapshot.source,
            }
        )
    path = state_dir / "replay_open_data" / f"{trading_date.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)
    return resolved, {
        "path": str(path),
        "sha256": _sha256(path),
        "requested_symbols": len(symbols),
        "snapshot_available_symbols": int(available.sum()),
        "valid_open_symbols": len(resolved),
        "source": snapshot.source,
    }


def _reuse_current_open_map(
    *,
    source_path: Path,
    state_dir: Path,
    trading_date: date,
) -> tuple[dict[str, float], dict[str, Any]]:
    """Reuse a retained same-session Shioaji open snapshot without more quota."""

    source_path = source_path.resolve()
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    frame = pl.read_parquet(source_path)
    required = {
        "trading_date",
        "symbol",
        "open_price",
        "snapshot_available",
        "timestamp_ms",
        "source",
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(
            f"{source_path} missing cached-open columns: {sorted(missing)}"
        )
    frame = frame.filter(pl.col("trading_date").cast(pl.Date) == trading_date)
    if frame.is_empty():
        raise ValueError(
            f"{source_path} has no rows for current session {trading_date}"
        )
    if frame.select(pl.col("symbol").n_unique()).item() != frame.height:
        raise ValueError(f"{source_path} has duplicate symbols")
    resolved = {
        str(row["symbol"]): float(row["open_price"])
        for row in frame.to_dicts()
        if bool(row.get("snapshot_available"))
        and row.get("open_price") is not None
        and math.isfinite(float(row["open_price"]))
        and float(row["open_price"]) > 0.0
    }
    if not resolved:
        raise ValueError(f"{source_path} has no valid session-open prices")
    source_values = sorted(
        {
            str(value or "").strip()
            for value in frame.get_column("source").to_list()
            if str(value or "").strip()
        }
    )
    if source_values and all("official" in value.lower() for value in source_values):
        canonical_source = "retained_same_session_official_open_snapshot"
    else:
        canonical_source = "retained_same_session_shioaji_snapshot_session_open"
    destination = state_dir / "replay_open_data" / f"{trading_date.isoformat()}.parquet"
    destination.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(destination)
    return resolved, {
        "path": str(destination),
        "sha256": _sha256(destination),
        "reused_from_path": str(source_path),
        "reused_from_sha256": _sha256(source_path),
        "requested_symbols": frame.height,
        "snapshot_available_symbols": int(
            frame.select(pl.col("snapshot_available").sum()).item()
        ),
        "valid_open_symbols": len(resolved),
        "source": canonical_source,
        "source_values": source_values,
        "canonical_source": canonical_source,
        "additional_shioaji_requests": 0,
    }


def _reuse_retained_signal_open_map(
    *,
    specs: list[ModeSpec],
    selected: Mapping[
        str,
        tuple[datetime, Path, Path, dict[str, Any], list[dict[str, Any]]],
    ],
    state_dir: Path,
    trading_date: date,
) -> tuple[dict[str, float], dict[str, Any]]:
    """Materialize already-observed same-session opens from retained signals.

    Every selected signal must explicitly attest that the live session-open
    feature was applied.  Official TWSE/TPEx MIS evidence is canonical when it
    exists. Same-tier observations must agree exactly; a disagreement fails the
    replay closed, while a lower-tier Shioaji mismatch remains visible in the
    receipt instead of being silently selected.
    """

    candidates: dict[str, list[dict[str, Any]]] = {}
    actionable_symbols: set[str] = set()
    evidence: list[dict[str, Any]] = []
    for spec in specs:
        generated_at, summary_path, weights_path, summary, rows = selected[spec.market]
        if not bool(summary.get("live_session_open_feature_applied")):
            raise ValueError(
                f"{spec.market}: retained signal does not attest an observed session open"
            )
        price_source = str(summary.get("price_source") or "").strip()
        if not price_source:
            raise ValueError(f"{spec.market}: retained signal has no price_source")
        evidence.append(
            {
                "market": spec.market,
                "generated_at": generated_at.isoformat(timespec="seconds"),
                "price_source": price_source,
                "summary_path": str(summary_path.resolve()),
                "summary_sha256": _sha256(summary_path),
                "weights_path": str(weights_path.resolve()),
                "weights_sha256": _sha256(weights_path),
            }
        )
        timestamp_ms = int(generated_at.timestamp() * 1_000)
        for row in rows:
            symbol = str(row.get("symbol") or "")
            if symbol and abs(float(row.get("target_weight") or 0.0)) > 0.0:
                actionable_symbols.add(symbol)
            try:
                opening = float(row.get("open_price"))
            except (TypeError, ValueError):
                opening = math.nan
            if not symbol or not (math.isfinite(opening) and opening > 0.0):
                continue
            candidates.setdefault(symbol, []).append(
                {
                    "market": spec.market,
                    "open_price": opening,
                    "price_source": price_source,
                    "timestamp_ms": timestamp_ms,
                }
            )

    observations: dict[str, dict[str, Any]] = {}
    conflicts: list[dict[str, Any]] = []
    noncanonical_mismatches: list[dict[str, Any]] = []
    for symbol in sorted(actionable_symbols):
        available = candidates.get(symbol, [])
        official = [
            item
            for item in available
            if (
                str(item["price_source"]).lower().startswith("twse_tpex:mis")
                or "official_mis_session_open" in str(item["price_source"]).lower()
            )
        ]
        preferred = official or available
        if not preferred:
            continue
        canonical = preferred[0]
        preferred_conflicts = [
            item
            for item in preferred[1:]
            if not math.isclose(
                float(item["open_price"]),
                float(canonical["open_price"]),
                rel_tol=0.0,
                abs_tol=1e-9,
            )
        ]
        if preferred_conflicts:
            conflicts.append(
                {
                    "symbol": symbol,
                    "canonical_market": canonical["market"],
                    "canonical_open": canonical["open_price"],
                    "canonical_source": canonical["price_source"],
                    "conflicts": preferred_conflicts,
                }
            )
            continue
        mismatches = [
            item
            for item in available
            if not math.isclose(
                float(item["open_price"]),
                float(canonical["open_price"]),
                rel_tol=0.0,
                abs_tol=1e-9,
            )
        ]
        for item in mismatches:
            noncanonical_mismatches.append(
                {
                    "symbol": symbol,
                    "canonical_market": canonical["market"],
                    "canonical_open": canonical["open_price"],
                    "canonical_source": canonical["price_source"],
                    "noncanonical_market": item["market"],
                    "noncanonical_open": item["open_price"],
                    "noncanonical_source": item["price_source"],
                }
            )
        matching = [
            item
            for item in available
            if math.isclose(
                float(item["open_price"]),
                float(canonical["open_price"]),
                rel_tol=0.0,
                abs_tol=1e-9,
            )
        ]
        observations[symbol] = {
            "trading_date": trading_date,
            "symbol": symbol,
            "open_price": float(canonical["open_price"]),
            # Kept for compatibility with the immutable-open parquet reader.
            # The source field accurately identifies retained signal evidence.
            "snapshot_available": True,
            "timestamp_ms": min(int(item["timestamp_ms"]) for item in matching),
            "source": (
                "retained_live_signal_open_feature:" + str(canonical["price_source"])
            ),
            "markets": sorted({str(item["market"]) for item in matching}),
            "price_sources": sorted({str(item["price_source"]) for item in matching}),
        }
    if conflicts:
        raise ValueError(
            "retained signal session-open conflict: "
            + json.dumps(conflicts[:20], ensure_ascii=False, sort_keys=True)
        )
    if not observations:
        raise ValueError("retained signals contain no valid actionable session opens")

    rows = [observations[symbol] for symbol in sorted(observations)]
    path = state_dir / "replay_open_data" / f"{trading_date.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)
    resolved = {
        symbol: float(row["open_price"]) for symbol, row in observations.items()
    }
    return resolved, {
        "path": str(path),
        "sha256": _sha256(path),
        "requested_symbols": len(actionable_symbols),
        "snapshot_available_symbols": len(observations),
        "valid_open_symbols": len(resolved),
        "source": "retained_live_signal_open_feature",
        "source_signal_evidence": evidence,
        "cross_mode_open_conflicts": 0,
        "official_mis_preferred_over_other_sources": True,
        "noncanonical_open_mismatch_count": len(noncanonical_mismatches),
        "noncanonical_open_mismatches": noncanonical_mismatches,
        "additional_shioaji_requests": 0,
    }


def _close_quotes(
    engine: TwDayTradeSimulationEngine,
    specs_by_market: Mapping[str, ModeSpec],
    *,
    trading_date: date,
    quote_at: datetime,
    twse_daily_ohlcv_path: Path,
    tpex_daily_ohlcv_path: Path,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    quotes: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    open_mismatches: list[dict[str, Any]] = []
    observed_symbols: set[str] = set()
    source_counts: dict[str, int] = {}
    for market, mode in (engine.state.get("modes") or {}).items():
        if str(mode.get("session_date") or "") != trading_date.isoformat():
            continue
        spec = specs_by_market[str(market)]
        for position in (mode.get("positions") or {}).values():
            if int(position.get("signed_shares") or 0) == 0:
                continue
            symbol = str(position.get("symbol") or "")
            if symbol in observed_symbols:
                continue
            observed_symbols.add(symbol)
            row = _official_daily_row(
                spec.parquet_root,
                symbol,
                trading_date,
                twse_daily_ohlcv_path,
                tpex_daily_ohlcv_path,
            )
            if row is None or not math.isfinite(float(row.get("close") or math.nan)):
                missing.append(symbol)
                continue
            entry_open = float(position.get("sizing_open_price") or math.nan)
            official_open = float(row.get("open") or math.nan)
            if not math.isclose(entry_open, official_open, rel_tol=0.0, abs_tol=1e-9):
                open_mismatches.append(
                    {
                        "symbol": symbol,
                        "signal_open": entry_open,
                        "official_open": official_open,
                    }
                )
            close_price = float(row["close"])
            official_source = str(row.get("_official_source") or "unknown")
            source_counts[official_source] = source_counts.get(official_source, 0) + 1
            quotes[symbol] = {
                "symbol": symbol,
                "open": official_open,
                "last": close_price,
                "bid": close_price,
                "ask": close_price,
                "minute_volume_lots": PAPER_LIQUIDITY_LOTS,
                "bid_volume": PAPER_LIQUIDITY_LOTS,
                "ask_volume": PAPER_LIQUIDITY_LOTS,
                "quote_at": quote_at.isoformat(timespec="seconds"),
                "source": f"{official_source}:session_close_replay",
                "fill_contract": HISTORICAL_OFFICIAL_CLOSE_FILL_CONTRACT,
                "depth_assumption": (
                    "full_residual_at_official_close_no_historical_auction_depth_claim"
                ),
            }
    if missing:
        raise ValueError(
            f"official close unavailable for {trading_date}: {sorted(missing)}"
        )
    return quotes, {
        "position_symbols": len(observed_symbols),
        "official_close_covered_symbols": len(quotes),
        "recorded_open_matches_official": not open_mismatches,
        "recorded_open_mismatches": open_mismatches,
        "official_source_counts": source_counts,
    }


def _position_stats(mode: Mapping[str, Any]) -> dict[str, Any]:
    positions = [
        row
        for row in (mode.get("positions") or {}).values()
        if isinstance(row, Mapping)
    ]
    return {
        "position_rows": len(positions),
        "open_position_rows": sum(
            int(row.get("signed_shares") or 0) != 0 for row in positions
        ),
        "long_shares": sum(
            max(int(row.get("signed_shares") or 0), 0) for row in positions
        ),
        "short_shares": sum(
            max(-int(row.get("signed_shares") or 0), 0) for row in positions
        ),
        "engine_status": mode.get("engine_status"),
        "entry_fill_policy": mode.get("entry_fill_policy"),
        "entry_fill_contract": mode.get("entry_fill_contract"),
        "entry_fill_is_synthetic": bool(mode.get("entry_fill_is_synthetic", False)),
        "signal_counts": mode.get("signal_counts") or {},
        "entry_fill_count": int(mode.get("entry_fill_count") or 0),
        "entry_best_quote_fill_count": int(
            mode.get("entry_best_quote_fill_count") or 0
        ),
        "entry_synthetic_fallback_fill_count": int(
            mode.get("entry_synthetic_fallback_fill_count") or 0
        ),
        "entry_official_open_fill_count": int(
            mode.get("entry_official_open_fill_count") or 0
        ),
        "entry_0901_vwap_fill_count": int(
            mode.get("entry_0901_vwap_fill_count") or 0
        ),
        "entry_requested_shares": int(mode.get("entry_requested_shares") or 0),
        "entry_filled_shares": int(mode.get("entry_filled_shares") or 0),
        "entry_unfilled_shares": int(mode.get("entry_unfilled_shares") or 0),
    }


def _minute_bar_rows(
    roots: tuple[Path, ...],
    *,
    trading_date: date,
    symbols: set[str],
) -> tuple[dict[str, dict[str, dict[str, float]]], dict[str, Any]]:
    """Load retained right-labelled minute OHLCV without fabricating books."""

    unresolved = set(symbols)
    output: dict[str, dict[str, dict[str, float]]] = {}
    source_counts: dict[str, int] = {}

    def accept(frame: pl.DataFrame, source: Path) -> None:
        nonlocal unresolved
        if not frame.height:
            return
        schema = set(frame.columns)
        volume_expr = (
            pl.col("volume_shares").cast(pl.Float64, strict=False)
            if "volume_shares" in schema
            else pl.col("Volume").cast(pl.Float64, strict=False)
            * (
                pl.col("contract_unit").cast(pl.Float64, strict=False)
                if "contract_unit" in schema
                else pl.lit(1_000.0)
            )
        )
        amount_expr = (
            pl.col("Amount").cast(pl.Float64, strict=False)
            if "Amount" in schema
            else pl.lit(None, dtype=pl.Float64)
        )
        normalized = frame.select(
            pl.col("symbol").cast(pl.String),
            pl.col("ts").cast(pl.Datetime("ns")),
            pl.col("Open").cast(pl.Float64, strict=False),
            pl.col("High").cast(pl.Float64, strict=False),
            pl.col("Low").cast(pl.Float64, strict=False),
            pl.col("Close").cast(pl.Float64, strict=False),
            volume_expr.alias("volume_shares"),
            amount_expr.alias("Amount"),
            (
                pl.col("_source_path")
                if "_source_path" in schema
                else pl.lit(str(source.resolve()))
            ).alias("_source_path"),
        )
        accepted_symbols: set[str] = set()
        accepted_sources: dict[str, str] = {}
        for row in normalized.iter_rows(named=True):
            symbol = str(row.get("symbol") or "")
            stamp = row.get("ts")
            close = _finite(row.get("Close"))
            if symbol not in unresolved or not isinstance(stamp, datetime) or close is None:
                continue
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=TAIPEI)
            else:
                stamp = stamp.astimezone(TAIPEI)
            if stamp.date() != trading_date:
                continue
            volume = max(0.0, float(row.get("volume_shares") or 0.0))
            amount = _finite(row.get("Amount"))
            vwap = amount / volume if amount is not None and volume > 0.0 else close
            output.setdefault(symbol, {})[stamp.isoformat(timespec="minutes")] = {
                "open": float(_finite(row.get("Open")) or close),
                "high": float(_finite(row.get("High")) or close),
                "low": float(_finite(row.get("Low")) or close),
                "close": float(close),
                "vwap": float(vwap),
                "volume_shares": volume,
            }
            accepted_symbols.add(symbol)
            accepted_sources.setdefault(
                symbol, str(row.get("_source_path") or source.resolve())
            )
        if accepted_symbols:
            for key in set(accepted_sources.values()):
                source_counts[key] = source_counts.get(key, 0) + sum(
                    value == key for value in accepted_sources.values()
                )
            unresolved -= accepted_symbols

    for root in roots:
        if not unresolved:
            break
        partition = root / f"trade_date={trading_date.isoformat()}" / "data.parquet"
        if partition.is_file():
            schema = set(pl.read_parquet_schema(partition).names())
            columns = [
                name
                for name in (
                    "symbol", "ts", "Open", "High", "Low", "Close", "Volume",
                    "Amount", "volume_shares", "contract_unit",
                )
                if name in schema
            ]
            frame = (
                pl.scan_parquet(partition)
                .filter(pl.col("symbol").is_in(sorted(unresolved)))
                .select(columns)
                .collect(engine="streaming")
            )
            accept(frame, partition)
        chunk_paths: list[Path] = []
        for symbol in sorted(unresolved):
            for path in sorted((root / "minute_chunks" / symbol).glob("*.parquet")):
                boundary = path.stem.split("_")
                if len(boundary) != 2 or not (
                    boundary[0] <= trading_date.isoformat() <= boundary[1]
                ):
                    continue
                chunk_paths.append(path)
        schema_groups: dict[tuple[str, ...], list[Path]] = {}
        for path in chunk_paths:
            schema_groups.setdefault(
                tuple(pl.read_parquet_schema(path).names()), []
            ).append(path)
        for schema_tuple, paths in schema_groups.items():
            schema = set(schema_tuple)
            columns = [
                name
                for name in (
                    "symbol", "date", "ts", "Open", "High", "Low", "Close",
                    "Volume", "Amount", "volume_shares", "contract_unit",
                )
                if name in schema
            ]
            lazy = pl.scan_parquet(
                sorted(paths), include_file_paths="_source_path"
            )
            if "date" in schema:
                lazy = lazy.filter(pl.col("date").cast(pl.Date) == trading_date)
            frame = lazy.select(*columns, "_source_path").collect(
                engine="streaming"
            )
            accept(frame, root)
    return output, {
        "requested_symbols": len(symbols),
        "resolved_symbols": len(output),
        "missing_symbols": sorted(unresolved),
        "source_counts": dict(sorted(source_counts.items())),
    }


def _bar_quote(
    position: Mapping[str, Any],
    bar: Mapping[str, float],
    *,
    observed: datetime,
    bid: float | None,
    ask: float | None,
    last: float | None = None,
) -> dict[str, Any]:
    lots = max(0.0, float(bar.get("volume_shares") or 0.0) / 1_000.0)
    return {
        "symbol": str(position.get("symbol") or ""),
        "last": float(last if last is not None else bar["close"]),
        "bid": bid,
        "ask": ask,
        "bid_volume": lots,
        "ask_volume": lots,
        "minute_volume_lots": lots,
        "quote_at": observed.isoformat(timespec="seconds"),
        "source": "retained_right_labelled_1m_ohlcv_counterfactual",
        "fill_contract": HISTORICAL_KBAR_FILL_CONTRACT,
        "depth_assumption": "50pct_observed_minute_volume_no_level_one_depth_claim",
    }


def _capacity_from_bar(position: Mapping[str, Any], bar: Mapping[str, float]) -> int:
    lot_size = int(position.get("lot_size") or 1_000)
    return (
        int(
            math.floor(
                max(0.0, float(bar.get("volume_shares") or 0.0))
                * MINUTE_VOLUME_PARTICIPATION
                / lot_size
            )
        )
        * lot_size
    )


def _apply_historical_kbar_brackets(
    engine: TwDayTradeSimulationEngine,
    *,
    market: str,
    bars: Mapping[str, Mapping[str, Mapping[str, float]]],
    observed: datetime,
) -> None:
    """Replay brackets conservatively from a completed one-minute range.

    If both stop and take-profit are touched inside one minute, the stop is
    processed first.  This avoids using unknowable intraminute ordering to
    improve the counterfactual result.
    """

    mode = engine.state["modes"][market]
    valuation_quotes: dict[str, dict[str, Any]] = {}
    minute_key = observed.isoformat(timespec="minutes")
    for position in (mode.get("positions") or {}).values():
        if int(position.get("signed_shares") or 0) == 0:
            continue
        symbol = str(position.get("symbol") or "")
        bar = (bars.get(symbol) or {}).get(minute_key)
        if bar is None:
            continue
        side = str(position.get("side") or "")
        take_profit = float(position["take_profit_price"])
        stop = float(position["stop_trigger_price"])
        stop_waiting = str(position.get("stop_order_status") or "").startswith(
            "triggered"
        )
        stop_hit = stop_waiting or (
            side == "long" and float(bar["low"]) <= stop
        ) or (side == "short" and float(bar["high"]) >= stop)
        take_profit_hit = (
            side == "long" and float(bar["high"]) > take_profit
        ) or (side == "short" and float(bar["low"]) < take_profit)
        capacity = _capacity_from_bar(position, bar)
        if stop_hit:
            position["stop_order_status"] = "triggered_waiting_liquidity"
            if capacity > 0:
                execution = float(bar["vwap"])
                quote = _bar_quote(
                    position,
                    bar,
                    observed=observed,
                    bid=execution if side == "long" else None,
                    ask=execution if side == "short" else None,
                    last=stop,
                )
                engine._close_position(  # noqa: SLF001 - canonical ledger writer
                    position,
                    mode,
                    price=execution,
                    quote=quote,
                    now=observed,
                    reason=(
                        "stop_loss_inside_daily_limit_"
                        f"{int(position.get('price_limit_offset_ticks') or 0)}_tick_trigger"
                    ),
                    order_type="MKT",
                    quantity=capacity,
                )
        elif take_profit_hit and capacity > 0:
            quote = _bar_quote(
                position,
                bar,
                observed=observed,
                bid=take_profit if side == "long" else None,
                ask=take_profit if side == "short" else None,
            )
            engine._close_position(  # noqa: SLF001 - canonical ledger writer
                position,
                mode,
                price=take_profit,
                quote=quote,
                now=observed,
                reason=(
                    "take_profit_inside_daily_limit_"
                    f"{int(position.get('price_limit_offset_ticks') or 0)}_tick"
                ),
                order_type="LMT",
                quantity=capacity,
            )
        if int(position.get("signed_shares") or 0) != 0:
            close = float(bar["close"])
            valuation_quotes[symbol] = _bar_quote(
                position,
                bar,
                observed=observed,
                bid=close if side == "long" else None,
                ask=close if side == "short" else None,
            )
    engine._mark_mode(  # noqa: SLF001 - one exact minute mark per mode
        market,
        observed,
        valuation_quotes,
        append_history=True,
    )


def _eod_kbar_quotes(
    mode: Mapping[str, Any],
    bars: Mapping[str, Mapping[str, Mapping[str, float]]],
    *,
    observed: datetime,
) -> dict[str, dict[str, Any]]:
    quotes: dict[str, dict[str, Any]] = {}
    minute_key = observed.isoformat(timespec="minutes")
    for position in (mode.get("positions") or {}).values():
        if int(position.get("signed_shares") or 0) == 0:
            continue
        symbol = str(position.get("symbol") or "")
        bar = (bars.get(symbol) or {}).get(minute_key)
        if bar is None:
            continue
        side = str(position.get("side") or "")
        close = float(bar["close"])
        if observed.time() == time(13, 20):
            day_array = np.asarray([np.datetime64(observed.date().isoformat(), "D")])
            if side == "long":
                bid = close
                ask = float(move_price_ticks_numpy(np.asarray([close]), 1, day_array)[0])
            else:
                bid = float(move_price_ticks_numpy(np.asarray([close]), -1, day_array)[0])
                ask = close
        elif time(13, 20) < observed.time() < time(13, 24):
            bid = float(bar["high"]) if side == "long" else close
            ask = float(bar["low"]) if side == "short" else close
        else:
            execution = float(bar["vwap"])
            bid = execution if side == "long" else None
            ask = execution if side == "short" else None
        quotes[symbol] = _bar_quote(
            position,
            bar,
            observed=observed,
            bid=bid,
            ask=ask,
        )
    return quotes


def _replay_historical_intraday(
    engine: TwDayTradeSimulationEngine,
    *,
    markets: list[str],
    bars: Mapping[str, Mapping[str, Mapping[str, float]]],
    trading_date: date,
) -> None:
    # 09:01 was already emitted atomically with the entry registration.  Every
    # following point is an exact right-labelled minute through 13:30.
    for offset in range(2, 271):
        observed = datetime.combine(trading_date, time(9, 0), tzinfo=TAIPEI) + timedelta(
            minutes=offset
        )
        if observed.time() < time(13, 20):
            for market in markets:
                _apply_historical_kbar_brackets(
                    engine,
                    market=market,
                    bars=bars,
                    observed=observed,
                )
            continue
        if observed.time() < time(13, 25):
            for market in markets:
                mode = engine.state["modes"][market]
                engine.process_quotes(
                    quotes=_eod_kbar_quotes(mode, bars, observed=observed),
                    now=observed,
                    markets=[market],
                    persist=False,
                )
            continue
        if observed.time() < time(13, 30):
            for market in markets:
                engine.process_quotes(
                    quotes={}, now=observed, markets=[market], persist=False
                )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--markets-dir", type=Path, default=Path("services/discord_bot/markets")
    )
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument(
        "--price-limit-dir",
        type=Path,
        default=Path("artifacts/live/tw_price_limits"),
    )
    parser.add_argument(
        "--twse-daily-ohlcv-path",
        type=Path,
        default=DEFAULT_TWSE_DAILY_OHLCV_PATH,
    )
    parser.add_argument(
        "--tw-public-dir",
        type=Path,
        default=Path("/srv/stockagent-live/data_tw_public"),
        help="Receipt-verified public-data root that owns the TAIEX session calendar.",
    )
    parser.add_argument(
        "--tpex-daily-ohlcv-path",
        type=Path,
        default=DEFAULT_TPEX_DAILY_OHLCV_PATH,
    )
    parser.add_argument(
        "--benchmark-state-source",
        type=Path,
        help=(
            "Retain the existing read-only 0050, 2330, and TX benchmark ledger "
            "so it can be rebased to the replay start without resetting live continuity."
        ),
    )
    parser.add_argument(
        "--source-ledger-dir",
        type=Path,
        help=(
            "Pin every replayed date/mode to the signal_id recorded in this source "
            "ledger, so an execution-only rebuild cannot silently switch signals."
        ),
    )
    parser.add_argument(
        "--benchmark-history-source",
        type=Path,
        help=(
            "Read-only benchmark_history.json to retain in the replay candidate. "
            "Defaults to SOURCE_LEDGER_DIR/benchmark_history.json when present."
        ),
    )
    parser.add_argument(
        "--historical-book-root",
        type=Path,
        help=(
            "Reuse retained per-date historical entry-book parquet files instead of "
            "starting any new Shioaji historical Tick requests."
        ),
    )
    parser.add_argument(
        "--minute-data-root",
        action="append",
        type=Path,
        default=[],
        help=(
            "Local canonical minute-data root checked before any 09:01 VWAP "
            "Shioaji query; repeat for multiple roots."
        ),
    )
    parser.add_argument(
        "--include-disabled",
        action="store_true",
        help=(
            "Include disabled day-trade modes for an explicit forensic rebuild. "
            "The default rebuilds enabled modes only."
        ),
    )
    parser.add_argument(
        "--replay-intraday-kbars",
        action="store_true",
        help=(
            "Replay the retained right-labelled 1m OHLCV path: conservative "
            "stop-before-profit bracket ordering through 13:19, a replay-only "
            "13:20 passive-price proxy, 13:24 minute VWAP market attempt, then "
            "the ordinary 13:30 auction/terminal flatten. The proxy is never "
            "labelled as an executable historical Bid/Ask."
        ),
    )
    entry_policy = parser.add_mutually_exclusive_group()
    entry_policy.add_argument(
        "--allow-adverse-tick-fallback",
        action="store_true",
        help=(
            "Explicitly authorize the historical hybrid contract: use the first "
            "Shioaji best ask/bid in the 09:00 minute when present, otherwise "
            "fill at the official open moved one adverse legal tick."
        ),
    )
    entry_policy.add_argument(
        "--paper-market-at-best",
        action="store_true",
        help=(
            "Use the active paper-market-order contract: fill every legal whole-lot "
            "request at the first retained 09:00 best ask/bid, ignoring displayed "
            "depth as a quantity cap; use one adverse open tick only when that side "
            "has no retained best quote. This is simulation-only and makes no "
            "exchange-fill claim."
        ),
    )
    parser.add_argument(
        "--max-shioaji-traffic-fraction",
        type=float,
        default=0.90,
        help=(
            "Stop historical Tick queries when Shioaji usage reaches this fraction; "
            "remaining actionable rows use the explicitly authorized fallback."
        ),
    )
    current_open = parser.add_mutually_exclusive_group()
    current_open.add_argument(
        "--fetch-current-open",
        action="store_true",
        help=(
            "Fetch today's actual session-open fields from Shioaji snapshots. "
            "Required when rebuilding an unclosed current session."
        ),
    )
    current_open.add_argument(
        "--current-open-path",
        type=Path,
        help=(
            "Reuse a retained same-session Shioaji open snapshot parquet instead "
            "of consuming more quote requests."
        ),
    )
    current_open.add_argument(
        "--reuse-retained-signal-open",
        action="store_true",
        help=(
            "Reuse the observed same-session open_price fields and provenance "
            "already retained in today's live signal artifacts."
        ),
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if start > end:
        raise ValueError("--start-date must not be after --end-date")
    state_dir = args.state_dir.resolve()
    if state_dir.exists() and any(state_dir.iterdir()):
        raise RuntimeError(f"refusing non-empty rebuild target: {state_dir}")
    minute_data_roots = tuple(
        path.resolve()
        for path in (args.minute_data_root or list(DEFAULT_MINUTE_DATA_ROOTS))
    )

    specs, live_configs, errors = _mode_specs(
        args.markets_dir,
        include_disabled=bool(args.include_disabled),
    )
    if errors:
        raise RuntimeError(f"mode configuration errors: {errors}")
    if not specs:
        raise RuntimeError("no enabled day-trade simulation modes")
    if args.paper_market_at_best:
        specs = [
            replace(
                spec,
                entry_fill_policy=ENTRY_FILL_POLICY_MARKET_AT_BEST_ELSE_OPEN_TICK,
                entry_price_offset_ticks=1,
            )
            for spec in specs
        ]
    elif args.allow_adverse_tick_fallback:
        specs = [
            replace(
                spec,
                entry_fill_policy=ENTRY_FILL_POLICY_CAUSAL_BOOK_ELSE_OPEN_TICK,
                entry_price_offset_ticks=1,
            )
            for spec in specs
        ]
    else:
        # Historical/missed-opening reconstruction is deliberately different
        # from the live runner. It sizes from the official 09:00 open and
        # executes from the observed right-labelled 09:01 minute VWAP.
        specs = [
            replace(
                spec,
                entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_VWAP,
                entry_price_offset_ticks=0,
            )
            for spec in specs
        ]
    specs_by_market = {spec.market: spec for spec in specs}
    twse_daily_ohlcv_path = args.twse_daily_ohlcv_path.resolve()
    tpex_daily_ohlcv_path = args.tpex_daily_ohlcv_path.resolve()
    for official_path in (twse_daily_ohlcv_path, tpex_daily_ohlcv_path):
        if not official_path.is_file():
            raise FileNotFoundError(official_path)
    official_sessions, official_calendar_sha256 = _validated_taiex_session_dates(
        args.tw_public_dir.resolve(), start, end
    )
    engine = TwDayTradeSimulationEngine(state_dir)
    source_signal_ids: dict[tuple[str, str], str] = {}
    source_ledger_provenance: dict[str, Any] | None = None
    if args.source_ledger_dir is not None:
        source_signal_ids, source_ledger_provenance = _source_ledger_signal_ids(
            args.source_ledger_dir.resolve(),
            start_date=start,
            end_date=end,
        )
        expected_signal_keys = {
            (day.isoformat(), spec.market)
            for day in official_sessions
            for spec in specs
        }
        missing_signal_keys = sorted(expected_signal_keys - set(source_signal_ids))
        if missing_signal_keys:
            raise ValueError(
                "source ledger is missing replay signal identities: "
                f"{missing_signal_keys[:20]}"
            )
    benchmark_state_provenance: dict[str, Any] | None = None
    if args.benchmark_state_source is not None:
        benchmark_state_path = args.benchmark_state_source.resolve()
        benchmark_state_raw = benchmark_state_path.read_bytes()
        benchmark_state_payload = json.loads(benchmark_state_raw)
        benchmarks = benchmark_state_payload.get("benchmarks") or {}
        required_benchmark_ids = {
            "benchmark_0050",
            "benchmark_2330",
            "benchmark_tx_continuous",
        }
        if not required_benchmark_ids.issubset(benchmarks):
            raise ValueError(
                "benchmark state source is incomplete: "
                f"{sorted(required_benchmark_ids - set(benchmarks))}"
            )
        engine.state["benchmarks"] = json.loads(json.dumps(benchmarks))
        benchmark_state_provenance = {
            "path": str(benchmark_state_path),
            "sha256": hashlib.sha256(benchmark_state_raw).hexdigest(),
            "benchmark_ids": sorted(benchmarks),
            "source_updated_at": benchmark_state_payload.get("updated_at"),
        }
    benchmark_history_path = args.benchmark_history_source
    if benchmark_history_path is None and args.source_ledger_dir is not None:
        inferred_history = args.source_ledger_dir.resolve() / "benchmark_history.json"
        if inferred_history.is_file():
            benchmark_history_path = inferred_history
    benchmark_history_provenance = (
        _retain_benchmark_history(benchmark_history_path, state_dir)
        if benchmark_history_path is not None
        else None
    )
    current = datetime.now(TAIPEI)
    paper_market_at_best = bool(args.paper_market_at_best)
    official_open_at_0901 = bool(specs) and all(
        spec.entry_fill_policy == ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901
        for spec in specs
    )
    minute_vwap_at_0901 = bool(specs) and all(
        spec.entry_fill_policy == ENTRY_FILL_POLICY_0901_MINUTE_VWAP
        for spec in specs
    )
    replay_entry_contract = (
        "retrospective_official_open_signal_at_09_00_observed_09_01_minute_vwap_counterfactual"
        if minute_vwap_at_0901
        else "retrospective_official_session_open_at_09_01_counterfactual"
        if official_open_at_0901
        else "retrospective_historical_best_quote_market_else_adverse_open_tick_counterfactual"
        if paper_market_at_best
        else "retrospective_historical_best_quote_else_adverse_open_tick_counterfactual"
        if bool(args.allow_adverse_tick_fallback)
        else "retrospective_observed_best_quote_counterfactual"
    )
    receipt: dict[str, Any] = {
        "schema_version": 2,
        "created_at": current.isoformat(timespec="seconds"),
        "simulation_only": True,
        "production_order_possible": False,
        "replay_contract": {
            "entry": replay_entry_contract,
            "entry_price": (
                "the official 09:00 open is used only for inference/sizing and every "
                "direction executes at the observed right-labelled 09:01 minute VWAP; "
                "missing opens or VWAPs fail closed without open-fill, Bid/Ask, last, "
                "or adverse-tick substitution"
                if minute_vwap_at_0901
                else "every direction uses the official session open and missing opens "
                "fail closed without Bid/Ask, last-price, or adverse-tick substitution"
                if official_open_at_0901
                else "market buy/cover uses first historical best ask in the 09:00 minute; "
                "market sell/short uses first historical best bid; missing required "
                "side uses official session open moved one adverse legal tick"
            ),
            "entry_liquidity": (
                "complete independently legal whole-lot paper quantity at the observed "
                "09:01 minute VWAP without any exchange-fill or queue claim"
                if minute_vwap_at_0901
                else
                "complete independently legal whole-lot paper quantity at the official "
                "open without any exchange-fill or queue claim"
                if official_open_at_0901
                else "complete requested paper quantity at the observed historical best "
                "quote without an exchange-depth claim; adverse one-tick fallback "
                "when that side is absent"
                if paper_market_at_best
                else "historical best quote consumes only its recorded level-one "
                "displayed lots; adverse one-tick fallback is counterfactual "
                "unbounded and makes no exchange or broker fill claim"
            ),
            "whole_lot_execution": (
                "each symbol is executed independently after eligibility, legal-price, "
                "whole-lot, and available-liquidity constraints; no cross-symbol "
                "direction balancing or fill reduction"
            ),
            "completed_session_exit": (
                "13:20 passive proxy, 13:24 minute VWAP, 13:30 official auction and terminal flatten"
                if args.replay_intraday_kbars
                else "official daily close"
            ),
            "intraday_path": (
                "retained right-labelled 1m OHLCV; stop-before-profit for same-minute ambiguity; 50pct minute-volume capacity; no executable Bid/Ask claim"
                if args.replay_intraday_kbars
                else "not reconstructed; daily-limit bracket ordering is not inferred from OHLC"
            ),
            "current_session": "left open for ordinary live quote service",
            "recorded_entry_time": "09:01:00 Asia/Taipei for every replayed session",
            "historical_quote_window": (
                "09:00:00-09:00:59 Asia/Taipei, right-labelled 09:01"
                if minute_vwap_at_0901
                else "not used by official-open execution"
                if official_open_at_0901
                else "09:00:00-09:00:59 Asia/Taipei"
            ),
            "source_signal_time": "retained separately for provenance",
        },
        "state_dir": str(state_dir),
        "source_signal_ledger": source_ledger_provenance,
        "retained_benchmark_state": benchmark_state_provenance,
        "retained_benchmark_history": benchmark_history_provenance,
        "official_daily_aggregate_sources": {
            "twse": {
                "path": str(twse_daily_ohlcv_path),
                "sha256": _sha256(twse_daily_ohlcv_path),
            },
            "tpex": {
                "path": str(tpex_daily_ohlcv_path),
                "sha256": _sha256(tpex_daily_ohlcv_path),
            },
        },
        "official_session_calendar": {
            "path": str(
                (args.tw_public_dir.resolve() / "twse_taiex_ohlc.parquet")
            ),
            "sha256": official_calendar_sha256,
            "session_count": len(official_sessions),
            "start_date": min(official_sessions).isoformat(),
            "end_date": max(official_sessions).isoformat(),
        },
        "skipped_sessions": [],
        "sessions": [],
    }

    day = start
    while day <= end:
        if day not in official_sessions:
            receipt["skipped_sessions"].append(
                {
                    "session_date": day.isoformat(),
                    "reason": "receipt_verified_taiex_non_session",
                }
            )
            day += timedelta(days=1)
            continue
        limit_path = (args.price_limit_dir / f"{day.isoformat()}.parquet").resolve()
        if not limit_path.is_file():
            raise FileNotFoundError(limit_path)
        limits = _load_price_limits(limit_path)
        session_receipt: dict[str, Any] = {
            "session_date": day.isoformat(),
            "price_limit_path": str(limit_path),
            "price_limit_sha256": _sha256(limit_path),
            "price_limit_rows": len(limits),
            "modes": [],
        }
        selected = {
            spec.market: _latest_valid_signal(
                spec,
                day,
                preferred_signal_id=source_signal_ids.get(
                    (day.isoformat(), spec.market)
                ),
            )
            for spec in specs
        }
        official_no_trade_symbols: set[str] = set()
        should_close = day < current.date() or (
            day == current.date()
            and current.timetz().replace(tzinfo=None) >= time(13, 30)
        )
        if should_close:
            (
                canonical_open_by_symbol,
                official_open_source_counts,
                official_no_trade_symbols,
            ) = _official_open_map(
                specs,
                selected,
                day,
                twse_daily_ohlcv_path=twse_daily_ohlcv_path,
                tpex_daily_ohlcv_path=tpex_daily_ohlcv_path,
            )
            canonical_open_source = "official_daily_session_open"
            session_receipt["canonical_open"] = {
                "source": canonical_open_source,
                "valid_open_symbols": len(canonical_open_by_symbol),
                "official_source_counts": official_open_source_counts,
                "official_no_trade_print_count": len(official_no_trade_symbols),
                "official_no_trade_print_symbols": sorted(official_no_trade_symbols),
            }
        else:
            if day != current.date() or not (
                bool(args.fetch_current_open)
                or args.current_open_path is not None
                or bool(args.reuse_retained_signal_open)
            ):
                raise RuntimeError(
                    "an unclosed current session requires --fetch-current-open "
                    "or --current-open-path or --reuse-retained-signal-open"
                )
            if args.current_open_path is not None:
                canonical_open_by_symbol, open_provenance = _reuse_current_open_map(
                    source_path=args.current_open_path,
                    state_dir=state_dir,
                    trading_date=day,
                )
                canonical_open_source = str(open_provenance["canonical_source"])
            elif args.reuse_retained_signal_open:
                canonical_open_by_symbol, open_provenance = (
                    _reuse_retained_signal_open_map(
                        specs=specs,
                        selected=selected,
                        state_dir=state_dir,
                        trading_date=day,
                    )
                )
                canonical_open_source = "retained_live_signal_session_open_feature"
            else:
                canonical_open_by_symbol, open_provenance = _capture_current_open_map(
                    specs=specs,
                    selected=selected,
                    state_dir=state_dir,
                    trading_date=day,
                )
                canonical_open_source = "fresh_shioaji_snapshot_session_open"
            session_receipt["canonical_open"] = {
                **open_provenance,
                "canonical_source": canonical_open_source,
            }
        configured_rule_roots = {
            _rule_data_dir(live_configs[spec.market], spec).resolve() for spec in specs
        }
        if len(configured_rule_roots) != 1:
            raise RuntimeError(
                "replay requires one shared exact-session rule source; got "
                f"{sorted(str(path) for path in configured_rule_roots)}"
            )
        configured_rule_root = next(iter(configured_rule_roots))
        replay_rule_root = state_dir / "replay_rule_data" / day.isoformat()
        session_receipt["eligibility_rules"] = _materialize_exact_session_rules(
            source_root=configured_rule_root,
            destination_root=replay_rule_root,
            trading_date=day,
        )
        prepared_modes = []
        for spec in specs:
            generated_at, summary_path, weights_path, summary, rows = selected[
                spec.market
            ]
            symbols = [str(row.get("symbol") or "") for row in rows]
            eligibility, coverage = load_live_eligibility(
                rule_data_dir=replay_rule_root,
                parquet_root=spec.parquet_root,
                symbols=symbols,
                trading_date=day,
            )
            prepared_modes.append(
                (
                    spec,
                    generated_at,
                    summary_path,
                    weights_path,
                    summary,
                    rows,
                    eligibility,
                    coverage,
                )
            )
        requested_book_symbols = (
            []
            if official_open_at_0901
            else _historical_book_request_symbols(
                prepared_modes,
                canonical_open_by_symbol,
            )
        )
        if official_open_at_0901:
            historical_books = {}
            historical_book_query = {
                "source": "not_required_for_official_open_at_09_01",
                "trading_date": day.isoformat(),
                "requested_symbols": 0,
                "resolved_book_symbols": 0,
                "additional_shioaji_requests": 0,
            }
        elif args.historical_book_root is not None:
            historical_books, historical_book_query = (
                _load_retained_historical_entry_books(
                    historical_book_root=args.historical_book_root.resolve(),
                    trading_date=day,
                    allow_missing=bool(
                        args.allow_adverse_tick_fallback
                        or args.paper_market_at_best
                    ),
                )
            )
            historical_book_query.update(
                {
                    "trading_date": day.isoformat(),
                    "requested_symbols": len(requested_book_symbols),
                    "resolved_book_symbols": len(
                        set(requested_book_symbols) & set(historical_books)
                    ),
                }
            )
        elif minute_vwap_at_0901:
            try:
                local_books, local_query = _local_0901_vwap_rows(
                    minute_roots=minute_data_roots,
                    symbols=requested_book_symbols,
                    trading_date=day,
                )
                unresolved = sorted(set(requested_book_symbols) - set(local_books))
                remote_books: dict[str, dict[str, Any]] = {}
                remote_query: dict[str, Any] = {
                    "source": "not_required_local_0901_vwap_complete",
                    "requested_symbols": 0,
                    "queried_symbols": 0,
                    "resolved_symbols": 0,
                    "unqueried_symbols": 0,
                    "error_counts": {},
                    "stopped_for_traffic": False,
                }
                if unresolved:
                    try:
                        remote_books, remote_query = (
                            fetch_shioaji_historical_stock_0901_vwaps(
                                unresolved,
                                trading_date=day,
                                max_traffic_fraction=float(
                                    args.max_shioaji_traffic_fraction
                                ),
                            )
                        )
                    except Exception as exc:
                        remote_books = {}
                        remote_query = {
                            "source": "shioaji:historical_ticks_0900_090059_vwap_right_label_0901",
                            "requested_symbols": len(unresolved),
                            "resolved_symbols": 0,
                            "unqueried_symbols": len(unresolved),
                            "query_failed": True,
                            "error_counts": {type(exc).__name__: 1},
                            "error": str(exc),
                            "stopped_for_traffic": False,
                        }
                historical_books = {**local_books, **remote_books}
                historical_book_query = {
                    "source": "local_first_then_shioaji_0901_vwap",
                    "trading_date": day.isoformat(),
                    "requested_symbols": len(requested_book_symbols),
                    "resolved_symbols": len(historical_books),
                    "local": local_query,
                    "remote": remote_query,
                    "unqueried_symbols": int(
                        remote_query.get("unqueried_symbols") or 0
                    ),
                    "error_counts": dict(remote_query.get("error_counts") or {}),
                    "query_failed": bool(remote_query.get("query_failed")),
                    "stopped_for_traffic": bool(
                        remote_query.get("stopped_for_traffic")
                    ),
                }
            except Exception as exc:
                historical_books = {}
                historical_book_query = {
                    "source": "shioaji:historical_ticks_0900_090059_vwap_right_label_0901",
                    "trading_date": day.isoformat(),
                    "requested_symbols": len(requested_book_symbols),
                    "resolved_symbols": 0,
                    "query_failed": True,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
        else:
            try:
                historical_books, historical_book_query = (
                    fetch_shioaji_historical_stock_entry_books(
                        requested_book_symbols,
                        trading_date=day,
                        max_traffic_fraction=float(args.max_shioaji_traffic_fraction),
                    )
                )
            except Exception as exc:
                historical_books = {}
                historical_book_query = {
                    "source": "shioaji:historical_stock_tick_best_quote",
                    "trading_date": day.isoformat(),
                    "requested_symbols": len(requested_book_symbols),
                    "resolved_book_symbols": 0,
                    "query_failed": True,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
        session_receipt["historical_entry_books"] = {
            **historical_book_query,
            **_persist_historical_entry_books(
                state_dir=state_dir,
                trading_date=day,
                books=historical_books,
            ),
        }
        if minute_vwap_at_0901:
            resolved_symbols = set(historical_books)
            missing_symbols = sorted(
                set(requested_book_symbols) - resolved_symbols
            )
            # The independent per-symbol execution contract must not turn one
            # illiquid/no-print stock into a portfolio-wide outage.  A row
            # without an observed 09:01 VWAP remains blocked by
            # _prepare_entry_plan; it is never replaced by the official open,
            # last price, a best quote, or an adverse tick.  Retain the gap in
            # the receipt so replay completeness cannot be confused with data
            # completeness.
            historical_book_query.update(
                {
                    "missing_actionable_symbols": missing_symbols,
                    "missing_actionable_symbol_count": len(missing_symbols),
                    "missing_symbol_policy": (
                        "per_symbol_fail_closed_no_price_substitution"
                    ),
                    "portfolio_wide_abort": False,
                }
            )
            session_receipt["historical_entry_books"].update(
                {
                    "missing_actionable_symbols": missing_symbols,
                    "missing_actionable_symbol_count": len(missing_symbols),
                    "missing_symbol_policy": (
                        "per_symbol_fail_closed_no_price_substitution"
                    ),
                    "portfolio_wide_abort": False,
                }
            )
        for (
            spec,
            generated_at,
            summary_path,
            weights_path,
            summary,
            rows,
            eligibility,
            coverage,
        ) in prepared_modes:
            observed = datetime.combine(day, time(9, 1), tzinfo=TAIPEI)
            replay_summary = dict(summary)
            replay_summary.update(
                {
                    # register_signal validates its event-time session through
                    # signal_started_at before generated_at. The immutable
                    # source times stay in the source_* fields below; only this
                    # isolated simulation event envelope is stamped at the
                    # replayed open.
                    "generated_at": observed.isoformat(timespec="seconds"),
                    "signal_started_at": observed.isoformat(timespec="seconds"),
                    "summary_path": str(summary_path.resolve()),
                    "weights_path": str(weights_path.resolve()),
                    "simulation_replay": True,
                    "replay_basis": (
                        "official_09_00_open_inference_to_observed_09_01_minute_vwap_to_intraday_kbar_schedule"
                        if minute_vwap_at_0901 and should_close and args.replay_intraday_kbars
                        else "official_09_00_open_inference_to_observed_09_01_minute_vwap_to_official_close"
                        if minute_vwap_at_0901 and should_close
                        else "official_09_00_open_inference_to_observed_09_01_minute_vwap_to_live_quotes"
                        if minute_vwap_at_0901
                        else "official_session_open_at_09_01_to_official_close"
                        if official_open_at_0901 and should_close
                        else "official_session_open_at_09_01_to_live_quotes"
                        if official_open_at_0901
                        else "historical_best_quote_else_adverse_open_tick_to_official_close"
                        if should_close
                        else "historical_best_quote_else_adverse_open_tick_to_live_quotes"
                    ),
                    "replay_source": (
                        "immutable_live_signal_official_daily_session_open_and_shioaji_09_01_minute_vwap"
                        if minute_vwap_at_0901
                        else "immutable_live_signal_and_official_daily_session_open"
                        if official_open_at_0901
                        else "immutable_live_signal_shioaji_historical_tick_and_official_market_data"
                    ),
                    "entry_fill_contract": replay_entry_contract,
                    "entry_liquidity_assumption": (
                        "observed_09_01_minute_vwap_full_requested_paper_quantity_no_exchange_fill_claim"
                        if minute_vwap_at_0901
                        else "official_open_full_requested_paper_quantity_no_exchange_fill_claim"
                        if official_open_at_0901
                        else "historical_best_quote_full_requested_paper_quantity_else_"
                        "adverse_open_tick_no_exchange_depth_claim"
                        if paper_market_at_best
                        else "historical_tick_level_one_depth_else_counterfactual_"
                        "unbounded_adverse_open_tick_fallback"
                    ),
                    "replay_effective_signal_at": observed.isoformat(
                        timespec="seconds"
                    ),
                    "source_signal_generated_at": generated_at.isoformat(
                        timespec="seconds"
                    ),
                    "source_signal_started_at": summary.get("signal_started_at"),
                    "source_signal_ready_at": summary.get("signal_ready_at"),
                }
            )
            entry_quotes, entry_price_quality = _entry_quotes(
                rows,
                limits,
                quote_at=observed,
                spec=spec,
                canonical_open_by_symbol=canonical_open_by_symbol,
                canonical_open_source=canonical_open_source,
                historical_books=historical_books,
                official_no_trade_symbols=official_no_trade_symbols,
            )
            replay_rows = _canonicalize_signal_rows_for_replay(
                rows,
                canonical_open_by_symbol,
            )
            result = engine.register_signal(
                spec=spec,
                summary=replay_summary,
                signal_rows=replay_rows,
                quotes=entry_quotes,
                eligibility=eligibility,
                eligibility_coverage=coverage,
                now=observed,
                counterfactual_open_replay=True,
            )
            mode = engine.state["modes"][spec.market]
            session_receipt["modes"].append(
                {
                    "market": spec.market,
                    "signal_id": summary.get("signal_id"),
                    "signal_generated_at": generated_at.isoformat(timespec="seconds"),
                    "replay_effective_signal_at": observed.isoformat(
                        timespec="seconds"
                    ),
                    "summary_path": str(summary_path.resolve()),
                    "summary_sha256": _sha256(summary_path),
                    "weights_path": str(weights_path.resolve()),
                    "weights_sha256": _sha256(weights_path),
                    "target_rows": len(rows),
                    "actionable_target_rows": sum(
                        abs(float(row.get("target_weight") or 0.0)) > 0.0
                        for row in rows
                    ),
                    "eligibility_coverage": coverage,
                    "register_result": result,
                    "entry_price_quality": entry_price_quality,
                    "entry": _position_stats(mode),
                }
            )

        if should_close and args.replay_intraday_kbars:
            position_symbols = {
                str(position.get("symbol") or "")
                for market in specs_by_market
                for position in (
                    engine.state.get("modes", {}).get(market, {}).get("positions", {})
                    or {}
                ).values()
                if int(position.get("signed_shares") or 0) != 0
            }
            minute_bars, minute_coverage = _minute_bar_rows(
                minute_data_roots,
                trading_date=day,
                symbols={symbol for symbol in position_symbols if symbol},
            )
            if minute_coverage["missing_symbols"]:
                raise RuntimeError(
                    f"missing retained minute path for {day}: "
                    f"{minute_coverage['missing_symbols'][:30]}"
                )
            session_receipt["intraday_replay"] = {
                **minute_coverage,
                "contract": HISTORICAL_KBAR_FILL_CONTRACT,
                "same_minute_bracket_order": "stop_before_take_profit",
                "minute_volume_participation": MINUTE_VOLUME_PARTICIPATION,
                "historical_best_bid_ask_claimed": False,
            }
            engine.begin_deferred_ledger_writes()
            _replay_historical_intraday(
                engine,
                markets=list(specs_by_market),
                bars=minute_bars,
                trading_date=day,
            )

        if should_close:
            close_at = datetime.combine(day, time(13, 30), tzinfo=TAIPEI)
            close_quotes, close_quality = _close_quotes(
                engine,
                specs_by_market,
                trading_date=day,
                quote_at=close_at,
                twse_daily_ohlcv_path=twse_daily_ohlcv_path,
                tpex_daily_ohlcv_path=tpex_daily_ohlcv_path,
            )
            engine.process_quotes(
                quotes=close_quotes,
                now=close_at,
                persist=not bool(args.replay_intraday_kbars),
            )
            if args.replay_intraday_kbars:
                # Commit all append-only ledgers before advancing state.json.
                # The live process still fsyncs each call; only this isolated
                # completed-session replay batches writes at the day boundary.
                engine.flush_deferred_ledger_writes()
                engine._persist(close_at)  # noqa: SLF001 - session transaction
            session_receipt["close"] = {
                "status": "settled_official_close",
                **close_quality,
            }
            for mode_receipt in session_receipt["modes"]:
                mode_receipt["after_close"] = _position_stats(
                    engine.state["modes"][mode_receipt["market"]]
                )
        else:
            session_receipt["close"] = {
                "status": "current_session_left_open_for_live_service"
            }
        receipt["sessions"].append(session_receipt)
        day += timedelta(days=1)

    _atomic_json(state_dir / "rebuild_receipt.json", receipt)
    print(
        json.dumps(
            {
                "state_dir": str(state_dir),
                "sessions": [
                    {
                        "session_date": session["session_date"],
                        "close_status": session["close"]["status"],
                        "modes": {
                            mode["market"]: mode["entry"] for mode in session["modes"]
                        },
                    }
                    for session in receipt["sessions"]
                ],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

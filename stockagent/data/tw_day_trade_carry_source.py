"""Receipt-bound lazy source for the physical TW day-trade FIFO executor.

This module converts immutable daily/minute sources into executor-only sessions.
It performs the expensive Parquet scan once, commits a content-addressed cache,
and then memory-maps only the dates requested by a chronological trainer batch.
No array produced here is a model feature or a broker fill receipt.
"""

from __future__ import annotations

from datetime import date
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any

import numpy as np
import torch

from stockagent.backtest.tw_day_trade_carry import DayTradeCarrySession
from stockagent.backtest.tw_day_trade_contract import BOARD_LOT_SHARES
from stockagent.data.tw_day_trade_schedule import (
    PAPER_MINUTE_SCHEDULE_ABI,
    paper_minute_opportunities,
)
from stockagent.data.panel import (
    _load_exact_cash_entitlements,
    _resolve_corporate_action_reference_paths,
)
from stockagent.data.tw_price_rules import (
    limit_price_numpy,
    price_on_tick_grid_numpy,
    tick_size_numpy,
)
from stockagent.data.tw_security import classify_tw_stock_or_etf
from stockagent.training.day_trade_carry_bridge import PreparedDayTradeCarrySource


PHYSICAL_SOURCE_CACHE_ABI = "tw_day_trade_physical_source_cache_v8_official_no_regular_execution"
PATH_CHANNELS = (
    "long_exit_price",
    "short_exit_price",
    "long_exit_capacity",
    "short_exit_capacity",
    "mark",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _hash_array(digest: Any, name: str, value: np.ndarray) -> None:
    array = np.ascontiguousarray(value)
    digest.update(name.encode("utf-8") + b"\0")
    digest.update(str(array.dtype).encode("ascii") + b"\0")
    digest.update(json.dumps(array.shape).encode("ascii") + b"\0")
    digest.update(array.view(np.uint8))


def _ordinal(day: np.datetime64) -> int:
    return int(day.astype("datetime64[D]").astype(np.int64)) + date(1970, 1, 1).toordinal()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent,
        prefix=f".{path.name}.", suffix=".tmp", delete=False,
    ) as handle:
        temporary = Path(handle.name)
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_npy(path: Path, value: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False,
    ) as handle:
        temporary = Path(handle.name)
        np.save(handle, value, allow_pickle=False)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_npz(path: Path, **payload: np.ndarray) -> None:
    """Write an uncompressed sparse archive; fast reads matter every epoch."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False,
    ) as handle:
        temporary = Path(handle.name)
        np.savez(handle, **payload)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _public_root(public_feature_path: str | Path) -> Path:
    path = Path(public_feature_path).resolve()
    if path.name != "tw_public_stock_daily.parquet" or path.parent.name != "features":
        raise ValueError(
            "physical day-trade source requires the canonical public feature path"
        )
    root = path.parent.parent
    required = (
        root / "twse_daily_ohlcv.parquet",
        root / "tpex_daily_ohlcv.parquet",
        root / "tw_corporate_action_reference.parquet",
        root / "tw_corporate_action_entitlements.parquet",
        root / "tw_corporate_action_entitlements.summary.json",
    )
    missing = [str(item) for item in required if not item.is_file()]
    if missing:
        raise FileNotFoundError(
            "physical day-trade source is missing public inputs: " + ", ".join(missing)
        )
    return root


def _source_identity(
    *, minute_root: Path, public_root: Path, dates: np.ndarray,
    symbols: tuple[str, ...], opens: np.ndarray, closes: np.ndarray,
    volumes: np.ndarray, action_mask: np.ndarray,
    unresolved_action_mask: np.ndarray, force_exit: np.ndarray,
    exact_action_mask: np.ndarray, exact_cash: np.ndarray,
    exact_payment_day: np.ndarray, no_regular_execution: np.ndarray,
) -> tuple[
    str,
    dict[str, dict[str, Any]],
    dict[np.datetime64, tuple[Path, str]],
    dict[str, Any],
]:
    manifest = minute_root / "manifest.json"
    if not manifest.is_file():
        raise FileNotFoundError(f"physical minute source has no manifest: {manifest}")
    try:
        minute_receipt = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid physical minute manifest: {manifest}") from exc
    if not (
        isinstance(minute_receipt, dict)
        and minute_receipt.get("source") == "shioaji_kbars_1m"
        and minute_receipt.get("research_ready") is True
        and minute_receipt.get("status") == "research_ready"
        and isinstance(minute_receipt.get("partitions"), list)
    ):
        raise RuntimeError("physical minute source manifest is not research_ready")
    partitions: dict[np.datetime64, tuple[Path, str]] = {}
    selected_days = set(dates.astype("datetime64[D]"))
    seen_manifest_days: set[np.datetime64] = set()
    partition_scope: dict[str, Any] = {
        "selected_panel_partitions": 0,
        "quarantined_non_panel_session_partitions": [],
        "unselected_outside_panel_horizon_partitions": [],
    }
    for item in minute_receipt["partitions"]:
        if not isinstance(item, dict):
            raise RuntimeError("physical minute manifest partition must be structured")
        trade_date = item.get("trade_date")
        relative = item.get("output")
        output_sha256 = item.get("output_sha256")
        if (
            item.get("status") != "ok"
            or not isinstance(trade_date, str)
            or not isinstance(relative, str)
            or relative != f"trade_date={trade_date}/data.parquet"
            or not isinstance(output_sha256, str)
            or len(output_sha256) != 64
        ):
            raise RuntimeError(
                f"physical minute manifest has an unaccepted partition: {trade_date}"
            )
        day = np.datetime64(trade_date, "D")
        output = (minute_root / relative).resolve()
        try:
            output.relative_to(minute_root.resolve())
        except ValueError as exc:
            raise RuntimeError("physical minute partition escapes its exact release") from exc
        if day in seen_manifest_days:
            raise RuntimeError(f"physical minute manifest repeats partition {trade_date}")
        seen_manifest_days.add(day)
        if day in selected_days:
            partitions[day] = (output, output_sha256)
            partition_scope["selected_panel_partitions"] += 1
            continue
        if not output.is_file():
            raise FileNotFoundError(
                f"unselected physical minute partition is missing: {output}"
            )
        actual_sha256 = _sha256(output)
        if actual_sha256 != output_sha256:
            raise RuntimeError(
                "unselected physical minute partition differs from its manifest: "
                f"date={trade_date} expected={output_sha256} actual={actual_sha256}"
            )
        receipt = {
            "trade_date": trade_date,
            "output": relative,
            "output_sha256": output_sha256,
            "rows": int(item.get("rows", -1)),
            "symbols": int(item.get("symbols", -1)),
        }
        if dates[0] < day < dates[-1]:
            partition_scope["quarantined_non_panel_session_partitions"].append(
                receipt
            )
        else:
            partition_scope["unselected_outside_panel_horizon_partitions"].append(
                receipt
            )
    if not partitions:
        raise RuntimeError("physical minute manifest declares no accepted partition")
    source_paths = {
        "minute_manifest": manifest,
        "twse_daily": public_root / "twse_daily_ohlcv.parquet",
        "tpex_daily": public_root / "tpex_daily_ohlcv.parquet",
        "corporate_reference": public_root / "tw_corporate_action_reference.parquet",
        "corporate_entitlements": public_root / "tw_corporate_action_entitlements.parquet",
        "corporate_entitlements_receipt": public_root / "tw_corporate_action_entitlements.summary.json",
    }
    receipts = {
        name: {"path": str(path), "bytes": path.stat().st_size, "sha256": _sha256(path)}
        for name, path in source_paths.items()
    }
    digest = hashlib.sha256()
    digest.update(PHYSICAL_SOURCE_CACHE_ABI.encode("ascii") + b"\0")
    digest.update(PAPER_MINUTE_SCHEDULE_ABI.encode("ascii") + b"\0")
    for name in sorted(receipts):
        digest.update(name.encode("utf-8") + b"\0")
        digest.update(receipts[name]["sha256"].encode("ascii") + b"\0")
    digest.update("\0".join(symbols).encode("utf-8"))
    for name, value in (
        ("dates", dates.astype("datetime64[D]").astype(np.int64)),
        ("official_open", opens), ("official_close", closes),
        ("daily_volume", volumes), ("corporate_avoidance", action_mask),
        ("unresolved_corporate_action", unresolved_action_mask),
        ("exact_cash_action", exact_action_mask),
        ("exact_cash_per_old_share", exact_cash),
        ("exact_cash_payment_day", exact_payment_day),
        ("official_no_regular_execution", no_regular_execution),
        ("force_exit", force_exit),
    ):
        _hash_array(digest, name, np.asarray(value))
    return digest.hexdigest(), receipts, partitions, partition_scope


def _exact_cash_action_arrays(
    *, public_feature_path: Path, dates: np.ndarray, symbols: tuple[str, ...],
) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict[str, Any]]:
    """Map the canonical exact-cash ledger to its first effective session.

    Avoid mode still asks the policy to flatten before every corporate action.
    This executor-only ledger is the physical fallback when ordinary 50%
    participation cannot fully liquidate an entitled residual.  It is not a
    model feature and does not relax an unresolved/complex action.
    """
    paths = _resolve_corporate_action_reference_paths(
        public_feature_path, include_rules=True
    )
    if paths is None:
        raise FileNotFoundError(
            "physical carry source requires the canonical corporate-action receipts"
        )
    terms, _short_terms, coverage_start, coverage_end = (
        _load_exact_cash_entitlements(paths)
    )
    if terms is None or coverage_start is None or coverage_end is None:
        raise ValueError(
            "physical carry source requires a complete exact-cash entitlement baseline"
        )
    if dates[0] < coverage_start or dates[-1] > coverage_end:
        raise ValueError(
            "physical carry horizon exceeds exact-cash entitlement coverage: "
            f"panel={dates[0]}..{dates[-1]} "
            f"archive={coverage_start}..{coverage_end}"
        )

    shape = (dates.size, len(symbols))
    event_mask = np.zeros(shape, dtype=np.bool_)
    cash = np.zeros(shape, dtype=np.float64)
    payment_day = np.zeros(shape, dtype=np.int64)
    symbol_index = {symbol: index for index, symbol in enumerate(symbols)}
    mapped = 0
    outside_universe = 0
    outside_horizon = 0
    mapped_after_closed_date = 0
    for symbol, (event_dates, cash_amounts, payment_dates) in terms.items():
        column = symbol_index.get(symbol)
        if column is None:
            outside_universe += int(len(event_dates))
            continue
        for event_date, amount, payment_date in zip(
            np.asarray(event_dates, dtype="datetime64[D]"),
            np.asarray(cash_amounts, dtype=np.float64),
            np.asarray(payment_dates, dtype="datetime64[D]"),
        ):
            row = int(np.searchsorted(dates, event_date, side="left"))
            if row >= dates.size or event_date < dates[0]:
                outside_horizon += 1
                continue
            effective_day = dates[row]
            if event_mask[row, column]:
                raise ValueError(
                    "multiple exact cash events map to one physical session: "
                    f"date={effective_day} symbol={symbol}"
                )
            if (
                not np.isfinite(amount)
                or amount <= 0.0
                or np.isnat(payment_date)
                or payment_date < effective_day
            ):
                raise ValueError(
                    "invalid exact cash action after session mapping: "
                    f"event={event_date} effective={effective_day} symbol={symbol}"
                )
            event_mask[row, column] = True
            cash[row, column] = float(amount)
            payment_day[row, column] = _ordinal(payment_date)
            mapped += 1
            mapped_after_closed_date += int(effective_day != event_date)
    return event_mask, cash, payment_day, {
        "mapped_events": mapped,
        "mapped_after_closed_date": mapped_after_closed_date,
        "outside_universe_events": outside_universe,
        "outside_horizon_events": outside_horizon,
        "coverage_start": str(coverage_start),
        "coverage_end": str(coverage_end),
        "policy": "exact_cash_on_first_exchange_session_on_or_after_declared_ex_date",
    }


def _official_no_regular_execution_mask(
    *, public_root: Path, dates: np.ndarray, symbols: tuple[str, ...],
) -> tuple[np.ndarray, dict[str, int]]:
    """Identify official symbol rows that contain no regular-market OHLC.

    Such a row is positive evidence of no executable board-lot price, not a
    missing download. Existing inventory may retain the previous observable
    regular-market mark, while entry/exit capacity remains exactly zero. A
    completely absent official row is deliberately NOT classified here.
    """
    import polars as pl

    lookup = pl.DataFrame(
        {"symbol": list(symbols), "_column": np.arange(len(symbols), dtype=np.int32)}
    )
    shape = (dates.size, len(symbols))
    result = np.zeros(shape, dtype=np.bool_)
    counts: dict[str, int] = {}
    specifications = (
        ("twse", public_root / "twse_daily_ohlcv.parquet", "證券代號", "開盤價", "收盤價"),
        ("tpex", public_root / "tpex_daily_ohlcv.parquet", "代號", "開盤", "收盤"),
    )
    start = date.fromisoformat(str(dates[0]))
    end = date.fromisoformat(str(dates[-1]))
    for market, path, symbol_column, open_column, close_column in specifications:
        schema = pl.scan_parquet(path).collect_schema()
        required = {"date", symbol_column, open_column, close_column}
        missing = required - set(schema.names())
        if missing:
            raise ValueError(
                f"official {market} daily source lacks no-execution fields: {sorted(missing)}"
            )
        frame = (
            pl.scan_parquet(path)
            .select(
                pl.col("date").cast(pl.String).str.to_date(strict=False).alias("date"),
                pl.col(symbol_column).cast(pl.String).str.strip_chars().alias("symbol"),
                pl.col(open_column).cast(pl.String).str.replace_all(",", "")
                .cast(pl.Float64, strict=False).alias("open"),
                pl.col(close_column).cast(pl.String).str.replace_all(",", "")
                .cast(pl.Float64, strict=False).alias("close"),
            )
            .filter(pl.col("date").is_between(start, end, closed="both"))
            .join(lookup.lazy(), on="symbol", how="inner", validate="m:1")
            .filter(pl.col("open").is_null() & pl.col("close").is_null())
            .select("date", "_column")
            .collect(engine="streaming")
        )
        market_count = 0
        seen: set[tuple[int, int]] = set()
        for item in frame.iter_rows(named=True):
            if item["date"] is None:
                continue
            day = np.datetime64(item["date"], "D")
            row = int(np.searchsorted(dates, day, side="left"))
            column = int(item["_column"])
            if row >= dates.size or dates[row] != day:
                continue
            key = (row, column)
            if key in seen:
                raise ValueError(
                    "official no-regular-execution source repeats symbol/date: "
                    f"market={market} date={day} symbol={symbols[column]}"
                )
            seen.add(key)
            result[row, column] = True
            market_count += 1
        counts[f"{market}_symbol_days"] = market_count
    counts["total_symbol_days"] = int(result.sum())
    return result, counts


def _price_limits(
    *, public_root: Path, dates: np.ndarray, symbols: tuple[str, ...],
    closes: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict[str, int]]:
    """Build only source-proven limits; unsupported TWSE ETFs remain missing."""
    import polars as pl

    rows, width = closes.shape
    kinds = np.asarray(
        [classify_tw_stock_or_etf(symbol) or "unsupported" for symbol in symbols]
    )
    if np.any(kinds == "unsupported"):
        bad = np.flatnonzero(kinds == "unsupported")
        raise ValueError(
            "physical day-trade universe contains unsupported securities: "
            + ", ".join(symbols[index] for index in bad[:8])
        )
    tpex_path = public_root / "tpex_daily_ohlcv.parquet"
    # The TPEx columns are next-session limits.  Keep enough pre-panel rows to
    # resolve the first panel session without pretending that same-day limits
    # were known one session earlier.
    tpex_start = dates[0].astype("datetime64[D]") - np.timedelta64(45, "D")
    tpex = (
        pl.scan_parquet(tpex_path)
        .select(
            pl.col("date").cast(pl.Date, strict=False).alias("date"),
            pl.col("代號").cast(pl.String).str.strip_chars().alias("symbol"),
            *[
                pl.col(column).cast(pl.String).str.strip_chars()
                .str.replace_all(",", "").cast(pl.Float64, strict=False).alias(alias)
                for column, alias in (
                    ("次日漲停價", "upper"), ("次日跌停價", "lower")
                )
            ],
        )
        .filter(
            pl.col("date").is_between(
                pl.lit(date.fromisoformat(str(tpex_start))),
                pl.lit(date.fromisoformat(str(dates[-1].astype("datetime64[D]")))),
                closed="both",
            )
        )
        .collect(engine="streaming")
    )
    tpex_symbols = set(tpex.get_column("symbol").drop_nulls().to_list())
    explicit: dict[tuple[np.datetime64, str], tuple[float, float]] = {}
    for item in tpex.iter_rows(named=True):
        upper, lower = item["upper"], item["lower"]
        if (
            item["date"] is not None and item["symbol"]
            and upper is not None and lower is not None
            and np.isfinite(upper) and np.isfinite(lower) and upper > lower > 0
        ):
            explicit[(np.datetime64(item["date"], "D"), str(item["symbol"]))] = (
                float(lower), float(upper)
            )
    tpex_days = np.asarray(
        sorted({source_day for source_day, _symbol in explicit}), dtype="datetime64[D]"
    )
    corporate = pl.read_parquet(
        public_root / "tw_corporate_action_reference.parquet",
        columns=["date", "symbol", "reference_price"],
    )
    references_by_day: dict[np.datetime64, list[tuple[str, float]]] = {}
    for item in corporate.iter_rows(named=True):
        if (
            item["date"] is not None and item["symbol"]
            and item["reference_price"] is not None
            and np.isfinite(item["reference_price"])
            and item["reference_price"] > 0
        ):
            references_by_day.setdefault(np.datetime64(item["date"], "D"), []).append(
                (str(item["symbol"]), float(item["reference_price"]))
            )
    lower = np.full((rows, width), np.nan, dtype=np.float64)
    upper = np.full_like(lower, np.nan)
    last_close = np.full(width, np.nan, dtype=np.float64)
    symbol_index = {symbol: index for index, symbol in enumerate(symbols)}
    twse_stock = np.asarray(
        [kind == "stock" and symbol not in tpex_symbols for symbol, kind in zip(symbols, kinds)],
        dtype=bool,
    )
    for row, day in enumerate(dates.astype("datetime64[D]")):
        reference = last_close.copy()
        for symbol, price in references_by_day.get(day, ()):
            if symbol in symbol_index:
                reference[symbol_index[symbol]] = price
        valid = twse_stock & np.isfinite(reference) & (reference > 0)
        if valid.any():
            day_grid = np.full(int(valid.sum()), day, dtype="datetime64[D]")
            lower[row, valid] = limit_price_numpy(reference[valid], 0.90, day_grid)
            upper[row, valid] = limit_price_numpy(reference[valid], 1.10, day_grid)
        previous_day: np.datetime64 | None = None
        if tpex_days.size:
            prior = int(np.searchsorted(tpex_days, day, side="left")) - 1
            if prior >= 0:
                previous_day = tpex_days[prior]
        if previous_day is not None:
            for symbol, index in symbol_index.items():
                item = explicit.get((previous_day, symbol))
                if item is not None:
                    lower[row, index], upper[row, index] = item
        quote = closes[row]
        last_close = np.where(np.isfinite(quote) & (quote > 0), quote, last_close)
    valid_limits = np.isfinite(lower) & np.isfinite(upper) & (lower > 0) & (upper > lower)
    return lower, upper, kinds, {
        "valid_symbol_days": int(valid_limits.sum()),
        "missing_symbol_days": int(valid_limits.size - valid_limits.sum()),
        "unsupported_twse_etf_symbol_days": int(
            ((kinds == "etf") & ~np.asarray([symbol in tpex_symbols for symbol in symbols]))
            .sum() * rows
        ),
    }


def _dense_bars(path: Path, symbols: tuple[str, ...]) -> tuple[np.ndarray, np.ndarray]:
    import polars as pl

    width = len(symbols)
    dense = np.full((width, 270, 6), np.nan, dtype=np.float64)
    dense[..., 5] = 0.0
    if not path.is_file():
        return dense, np.zeros(width, dtype=np.float64)
    lookup = pl.DataFrame(
        {"symbol": list(symbols), "_slot": np.arange(width, dtype=np.int32)}
    )
    frame = (
        pl.read_parquet(
            path,
            columns=[
                "symbol", "minutes_from_open", "Open", "High", "Low", "Close",
                "Amount", "volume_shares",
            ],
        )
        .with_columns(
            pl.col("symbol").cast(pl.String),
            pl.col("minutes_from_open").cast(pl.Int32, strict=False),
        )
        .filter(pl.col("minutes_from_open").is_between(1, 270, closed="both"))
        .join(lookup, on="symbol", how="inner", validate="m:1")
    )
    if frame.is_empty():
        return dense, np.zeros(width, dtype=np.float64)
    slots = frame.get_column("_slot").to_numpy().astype(np.int64, copy=False)
    minutes = (
        frame.get_column("minutes_from_open").to_numpy().astype(np.int64, copy=False)
        - 1
    )
    keys = slots * 270 + minutes
    if np.unique(keys).size != keys.size:
        counts = np.bincount(keys, minlength=width * 270)
        duplicate = int(np.flatnonzero(counts > 1)[0])
        slot, minute = divmod(duplicate, 270)
        raise ValueError(
            f"duplicate physical minute row: symbol={symbols[slot]} minute={minute + 1}"
        )
    columns = [
        frame.get_column(name).cast(pl.Float64, strict=False).to_numpy()
        for name in ("Open", "High", "Low", "Close", "Amount", "volume_shares")
    ]
    opens, highs, lows, closes, amounts, volumes = columns
    vwaps = np.where(
        np.isfinite(amounts) & np.isfinite(volumes) & (volumes > 0),
        amounts / np.maximum(volumes, np.finfo(np.float64).tiny),
        closes,
    )
    dense[slots, minutes] = np.column_stack(
        (opens, highs, lows, closes, vwaps, volumes)
    )
    valid_volume = np.where(np.isfinite(volumes) & (volumes > 0), volumes, 0.0)
    totals = np.bincount(slots, weights=valid_volume, minlength=width).astype(
        np.float64, copy=False
    )
    return dense, totals


def build_prepared_day_trade_carry_source(
    *, panel: Any, minute_root: str | Path, public_feature_path: str | Path,
    cache_dir: str | Path, allow_daily_proxy: bool,
    daily_proxy_price_policy: str, corporate_action_mode: str,
) -> PreparedDayTradeCarrySource:
    if corporate_action_mode != "avoid":
        raise ValueError("physical source v1 supports the requested avoid action policy only")
    if daily_proxy_price_policy != "official_open_close":
        raise ValueError("physical source requires the official OPEN/CLOSE proxy policy")
    minute_path = Path(minute_root).resolve()
    public_path = _public_root(public_feature_path)
    dates = np.asarray(panel.dates, dtype="datetime64[D]").reshape(-1)
    symbols = tuple(str(symbol) for symbol in panel.symbols)
    raw_opens = np.asarray(panel.open_prices)
    raw_closes = np.asarray(panel.close_prices)
    opens = np.asarray(raw_opens, dtype=np.float64).copy()
    closes = np.asarray(raw_closes, dtype=np.float64).copy()
    volumes = np.asarray(panel.daily_volumes, dtype=np.float64)
    shape = (dates.size, len(symbols))
    if (
        not dates.size or np.isnat(dates).any() or np.any(dates[1:] <= dates[:-1])
        or opens.shape != shape or closes.shape != shape or volumes.shape != shape
    ):
        raise ValueError("physical source requires an aligned ordered daily panel")
    action = np.asarray(panel.corporate_action_avoidance_mask, dtype=bool)
    unresolved_action = np.asarray(
        panel.unresolved_corporate_action_mask, dtype=bool
    )
    force_exit = np.asarray(panel.force_exit_mask, dtype=bool)
    if (
        action.shape != shape
        or unresolved_action.shape != shape
        or force_exit.shape != shape
    ):
        raise ValueError("physical source requires exact action/terminal masks")
    exact_action, exact_cash, exact_payment_day, exact_action_counts = (
        _exact_cash_action_arrays(
            public_feature_path=Path(public_feature_path).resolve(),
            dates=dates,
            symbols=symbols,
        )
    )
    no_regular_execution, no_execution_counts = (
        _official_no_regular_execution_mask(
            public_root=public_path, dates=dates, symbols=symbols
        )
    )
    digest, receipts, minute_partitions, partition_scope = _source_identity(
        minute_root=minute_path, public_root=public_path, dates=dates,
        symbols=symbols, opens=opens, closes=closes, volumes=volumes,
        action_mask=action, unresolved_action_mask=unresolved_action,
        force_exit=force_exit, exact_action_mask=exact_action,
        exact_cash=exact_cash, exact_payment_day=exact_payment_day,
        no_regular_execution=no_regular_execution,
    )
    cache_root = Path(cache_dir).resolve() / f"physical-{digest}"
    manifest_path = cache_root / "manifest.json"
    ready_path = cache_root / "READY.json"
    lock_path = cache_root.parent / f".{cache_root.name}.lock"
    cache_root.parent.mkdir(parents=True, exist_ok=True)
    partition_dates = sorted(minute_partitions)
    if not partition_dates:
        raise FileNotFoundError("physical minute source has no dated partitions")
    first_minute = partition_dates[0]
    if not allow_daily_proxy and bool((dates < first_minute).any()):
        raise ValueError("physical source forbids the required pre-minute daily proxy")

    lower, upper, security_types, limit_counts = _price_limits(
        public_root=public_path, dates=dates, symbols=symbols, closes=closes
    )
    # The daily panel intentionally stores float32. Preserve that source dtype
    # while validating the dated stock/ETF tick grid; converting first to
    # float64 loses the original ULP tolerance and turns a legal ETF quote such
    # as 35.77 into 35.770000457... off-grid. Once accepted, restore the exact
    # grid decimal for currency accounting rather than carrying storage noise.
    open_on_grid = np.zeros(shape, dtype=np.bool_)
    close_on_grid = np.zeros(shape, dtype=np.bool_)
    normalized_open_cells = 0
    normalized_close_cells = 0
    for row, day in enumerate(dates):
        open_on_grid[row] = price_on_tick_grid_numpy(
            raw_opens[row], day, security_types=security_types
        )
        close_on_grid[row] = price_on_tick_grid_numpy(
            raw_closes[row], day, security_types=security_types
        )
        for values, valid, is_open in (
            (opens[row], open_on_grid[row], True),
            (closes[row], close_on_grid[row], False),
        ):
            ticks = tick_size_numpy(values, day, security_types=security_types)
            canonical = np.rint(values / ticks) * ticks
            canonical = np.rint(canonical * 100.0) / 100.0
            changed = valid & (canonical != values)
            values[valid] = canonical[valid]
            if is_open:
                normalized_open_cells += int(changed.sum())
            else:
                normalized_close_cells += int(changed.sum())
    last_close = np.full(len(symbols), np.nan, dtype=np.float64)
    opening_marks = np.full(shape, np.nan, dtype=np.float64)
    for row in range(dates.size):
        opening_marks[row] = np.where(
            np.isfinite(opens[row]) & (opens[row] > 0), opens[row], last_close
        )
        last_close = np.where(
            np.isfinite(closes[row]) & (closes[row] > 0), closes[row], last_close
        )

    source_started = time.monotonic()
    print(
        "[physical source] validating content-addressed cache "
        f"abi={PHYSICAL_SOURCE_CACHE_ABI} release=tw-day-trade-carry:{digest} ",
        flush=True,
    )
    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        manifest: dict[str, Any] | None = None
        if manifest_path.is_file():
            try:
                candidate = json.loads(manifest_path.read_text(encoding="utf-8"))
                ready = json.loads(ready_path.read_text(encoding="utf-8"))
                if (
                    candidate.get("abi") == PHYSICAL_SOURCE_CACHE_ABI
                    and candidate.get("source_digest") == digest
                    and candidate.get("rows") == int(dates.size)
                    and candidate.get("symbols") == len(symbols)
                    and ready.get("abi") == PHYSICAL_SOURCE_CACHE_ABI
                    and ready.get("source_digest") == digest
                    and ready.get("manifest_sha256") == _sha256(manifest_path)
                    and ready.get("verified_files") == len(candidate.get("files", {}))
                ):
                    manifest = candidate
            except (OSError, json.JSONDecodeError, AttributeError):
                manifest = None
        gap_path = cache_root / "source_gaps.npy"
        action_path = cache_root / "exact_cash_actions.npz"
        if manifest is not None:
            expected = manifest.get("files", {})
            required = [gap_path, action_path]
            for day in dates:
                if day >= first_minute:
                    day_text = np.datetime_as_string(day, unit="D")
                    required.append(cache_root / f"session-{day_text}.npz")
            if (
                not isinstance(expected, dict)
                or set(expected) != {path.name for path in required}
                or any(
                    not path.is_file()
                    or expected.get(path.name, {}).get("bytes") != path.stat().st_size
                    or expected.get(path.name, {}).get("sha256") != _sha256(path)
                    for path in required
                )
            ):
                manifest = None
        if manifest is None:
            cache_root.mkdir(parents=True, exist_ok=True)
            print(
                "[physical source] rebuilding exact hybrid sessions "
                f"rows={dates.size} symbols={len(symbols)} "
                f"minute_sessions={int((dates >= first_minute).sum())}",
                flush=True,
            )
            gaps = np.zeros(shape, dtype=np.bool_)
            # If an unresolved/terminal interval ends without liquidation, the
            # next session is an exact account-source failure for that held
            # symbol. A receipt-verified exact cash event is instead applied to
            # the residual FIFO cohorts below; it must never be called missing.
            if dates.size > 1:
                action_ends = action[:-1] & ~action[1:]
                gaps[1:] |= action_ends & ~exact_action[1:]
                gaps[1:] |= (force_exit[:-1] & ~force_exit[1:])
            files: dict[str, dict[str, Any]] = {}
            mode_counts = {
                "minute_symbol_days": 0,
                "daily_proxy_pre_minute_symbol_days": 0,
                "daily_proxy_post_minute_symbol_days": 0,
                "post_minute_missing_or_empty_rows": 0,
                "post_minute_volume_exceeds_official_day": 0,
                "post_minute_missing_historical_limits": 0,
                "official_no_regular_execution_symbol_days": int(
                    no_regular_execution.sum()
                ),
            }
            total_minute_sessions = int((dates >= first_minute).sum())
            minute_sessions_built = 0
            for row, day in enumerate(dates):
                valid_limits = (
                    np.isfinite(lower[row]) & np.isfinite(upper[row])
                    & (lower[row] > 0) & (upper[row] > lower[row])
                )
                active_daily = (
                    (np.isfinite(volumes[row]) & (volumes[row] > 0))
                    | (np.isfinite(opens[row]) & (opens[row] > 0))
                    | (np.isfinite(closes[row]) & (closes[row] > 0))
                )
                valid_proxy = (
                    np.isfinite(opens[row]) & (opens[row] > 0)
                    & np.isfinite(closes[row]) & (closes[row] > 0)
                    & np.isfinite(volumes[row]) & (volumes[row] >= 0)
                    & open_on_grid[row] & close_on_grid[row]
                )
                if day < first_minute:
                    gaps[row] |= (
                        active_daily & ~valid_proxy & ~no_regular_execution[row]
                    )
                    usable_proxy = valid_proxy & ~gaps[row]
                    mode_counts["daily_proxy_pre_minute_symbol_days"] += int(
                        usable_proxy.sum()
                    )
                    continue
                day_text = np.datetime_as_string(day, unit="D")
                partition = minute_partitions.get(day)
                if partition is None:
                    dense = np.full((len(symbols), 270, 6), np.nan, dtype=np.float64)
                    dense[..., 5] = 0.0
                    minute_total = np.zeros(len(symbols), dtype=np.float64)
                else:
                    partition_path, expected_sha256 = partition
                    if not partition_path.is_file():
                        raise FileNotFoundError(
                            f"accepted physical minute partition is missing: {partition_path}"
                        )
                    actual_sha256 = _sha256(partition_path)
                    if actual_sha256 != expected_sha256:
                        raise RuntimeError(
                            "physical minute partition differs from its manifest: "
                            f"date={day_text} expected={expected_sha256} actual={actual_sha256}"
                        )
                    dense, minute_total = _dense_bars(partition_path, symbols)
                official_volume_valid = (
                    np.isfinite(volumes[row]) & (volumes[row] >= 0)
                )
                # The retained KBars cover regular-session observations while
                # the official daily total includes additional mechanisms.
                # Equality is therefore neither expected nor meaningful.  A
                # positive minute sum may not, however, exceed the independent
                # official whole-day bound; such a symbol-day uses the explicit
                # daily proxy instead of overstating executable liquidity.
                volume_exceeds_day = (
                    official_volume_valid
                    & (minute_total > volumes[row] + 1.0)
                )
                missing_or_empty_minute = (
                    (partition is None) | (minute_total <= 0)
                )
                minute_candidate = (
                    ~missing_or_empty_minute
                    & official_volume_valid
                    & ~volume_exceeds_day
                    & valid_limits
                    & ~no_regular_execution[row]
                )
                use_proxy = (
                    ~minute_candidate & valid_proxy & ~no_regular_execution[row]
                )
                gaps[row] |= (
                    active_daily
                    & ~(minute_candidate | use_proxy)
                    & ~no_regular_execution[row]
                )
                good = minute_candidate & ~gaps[row]
                proxy = use_proxy & ~gaps[row]
                mode_counts["minute_symbol_days"] += int(good.sum())
                mode_counts["daily_proxy_post_minute_symbol_days"] += int(
                    proxy.sum()
                )
                mode_counts["post_minute_missing_or_empty_rows"] += int(
                    (proxy & missing_or_empty_minute).sum()
                )
                mode_counts["post_minute_volume_exceeds_official_day"] += int(
                    (proxy & volume_exceeds_day).sum()
                )
                mode_counts["post_minute_missing_historical_limits"] += int(
                    (proxy & ~valid_limits).sum()
                )
                path_array = np.full((len(symbols), 270, len(PATH_CHANNELS)), np.nan, dtype=np.float64)
                path_array[..., 2:4] = 0.0
                # price, source volume, explicit daily-proxy bit
                entry_array = np.full((len(symbols), 3), np.nan, dtype=np.float64)
                entry_array[:, 1] = 0.0
                entry_array[:, 2] = 0.0
                if good.any():
                    opportunities = paper_minute_opportunities(
                        dense[good], trading_date=day,
                        lower_limit=lower[row, good], upper_limit=upper[row, good],
                        security_types=security_types[good],
                    )
                    path_array[good, :, :2] = opportunities.prices
                    path_array[good, :, 2:4] = opportunities.capacity_shares
                    marks = opportunities.marks
                    leading = ~np.isfinite(marks) | (marks <= 0)
                    marks = np.where(leading, opening_marks[row, good, None], marks)
                    path_array[good, :, 4] = marks
                    entry_array[good, 0] = dense[good, 0, 4]
                    entry_array[good, 1] = dense[good, 0, 5]
                if proxy.any():
                    path_array[proxy, :, 4] = opens[row, proxy, None]
                    path_array[proxy, -1, 4] = closes[row, proxy]
                    path_array[proxy, -1, :2] = closes[row, proxy, None]
                    proxy_capacity = (
                        np.floor(
                            volumes[row, proxy]
                            / 271.0
                            * 0.5
                            / BOARD_LOT_SHARES
                        )
                        * BOARD_LOT_SHARES
                    )
                    path_array[proxy, -1, 2:4] = proxy_capacity[:, None]
                    entry_array[proxy, 0] = opens[row, proxy]
                    entry_array[proxy, 1] = volumes[row, proxy] / 271.0
                    entry_array[proxy, 2] = 1.0
                carried_mark = (
                    no_regular_execution[row]
                    & (gaps[row] == 0)
                    & np.isfinite(opening_marks[row])
                    & (opening_marks[row] > 0)
                )
                if carried_mark.any():
                    path_array[carried_mark, :, 4] = opening_marks[
                        row, carried_mark, None
                    ]
                exit_view = path_array[..., :2].reshape(-1)
                exit_flat = np.flatnonzero(np.isfinite(exit_view)).astype(
                    np.int32, copy=False
                )
                mark_view = path_array[..., 4].reshape(-1)
                mark_flat = np.flatnonzero(np.isfinite(mark_view)).astype(
                    np.int32, copy=False
                )
                output = cache_root / f"session-{day_text}.npz"
                _atomic_npz(
                    output,
                    exit_flat=exit_flat,
                    exit_price=exit_view[exit_flat].astype(np.float64, copy=False),
                    exit_capacity=path_array[..., 2:4].reshape(-1)[exit_flat].astype(
                        np.float64, copy=False
                    ),
                    mark_flat=mark_flat,
                    mark=mark_view[mark_flat].astype(np.float64, copy=False),
                    entry=entry_array,
                )
                files[output.name] = {
                    "bytes": output.stat().st_size, "sha256": _sha256(output)
                }
                minute_sessions_built += 1
                if minute_sessions_built % 100 == 0:
                    print(
                        "[physical source] rebuild progress "
                        f"minute_sessions={minute_sessions_built}/"
                        f"{total_minute_sessions} "
                        f"elapsed={time.monotonic() - source_started:.1f}s",
                        flush=True,
                    )
            _atomic_npy(gap_path, gaps)
            files[gap_path.name] = {
                "bytes": gap_path.stat().st_size, "sha256": _sha256(gap_path)
            }
            action_flat = np.flatnonzero(exact_action.reshape(-1)).astype(
                np.int64, copy=False
            )
            _atomic_npz(
                action_path,
                action_flat=action_flat,
                cash_per_old_share=exact_cash.reshape(-1)[action_flat].astype(
                    np.float64, copy=False
                ),
                payment_day=exact_payment_day.reshape(-1)[action_flat].astype(
                    np.int64, copy=False
                ),
            )
            files[action_path.name] = {
                "bytes": action_path.stat().st_size,
                "sha256": _sha256(action_path),
            }
            manifest = {
                "abi": PHYSICAL_SOURCE_CACHE_ABI,
                "schedule_abi": PAPER_MINUTE_SCHEDULE_ABI,
                "source_digest": digest,
                "release_id": f"tw-day-trade-carry:{digest}",
                "rows": int(dates.size), "symbols": len(symbols),
                "date_start": str(dates[0]), "date_end": str(dates[-1]),
                "first_minute_date": str(first_minute),
                "daily_proxy": "official_open_close_without_adverse_tick",
                "daily_proxy_capacity": "floor(daily_volume/271*0.5/1000)*1000",
                "daily_proxy_intraday_marks": "official_open_carried_to_official_close_not_observed_minutes",
                "daily_proxy_source_precision": {
                    "validation": "preserve_panel_source_dtype_ulp_then_require_dated_tick_grid",
                    "accounting": "canonical_dated_tick_decimal_float64",
                    "normalized_open_cells": normalized_open_cells,
                    "normalized_close_cells": normalized_close_cells,
                },
                "minute_volume_reconciliation": "regular_session_positive_volume_must_not_exceed_official_whole_day_volume_else_daily_proxy",
                "session_encoding": "sorted_flat_sparse_exit_mark_and_exact_cash_uncompressed_npz",
                "corporate_action_policy": {
                    "policy_target": "avoid_all_announced_actions_before_event",
                    "residual_cash_entitlement": "receipt_verified_exact_amount_and_payment_date",
                    "unresolved_residual": "fail_closed_per_symbol_on_first_post_event_session",
                    **exact_action_counts,
                },
                "official_no_regular_execution": {
                    **no_execution_counts,
                    "valuation": "carry_previous_observable_regular_market_mark",
                    "entry_exit_capacity": "zero",
                    "absent_official_row": "not_classified_and_never_auto_filled",
                },
                "twse_etf_limit_policy": "official_daily_proxy_without_fabricated_limit_when_historical_limit_receipt_is_missing",
                "mode_counts": mode_counts,
                "minute_partition_scope": partition_scope,
                "limit_counts": limit_counts, "source_receipts": receipts,
                "source_gap_symbol_days": int(gaps.sum()),
                "files": files,
            }
            _atomic_json(manifest_path, manifest)
            _atomic_json(
                ready_path,
                {
                    "abi": PHYSICAL_SOURCE_CACHE_ABI,
                    "source_digest": digest,
                    "manifest_sha256": _sha256(manifest_path),
                    "verified_files": len(files),
                    "verified_bytes": int(
                        sum(int(item["bytes"]) for item in files.values())
                    ),
                    "verification": "sha256_each_file_at_atomic_build",
                },
            )
        gaps = np.load(gap_path, allow_pickle=False, mmap_mode="r")
        if gaps.shape != shape or gaps.dtype != np.dtype(bool):
            raise RuntimeError("invalid physical source gap cache")
        with np.load(action_path, allow_pickle=False) as packed_actions:
            if set(packed_actions.files) != {
                "action_flat", "cash_per_old_share", "payment_day"
            }:
                raise RuntimeError("invalid physical exact-cash cache fields")
            action_flat = np.array(packed_actions["action_flat"], copy=True)
            action_cash = np.array(packed_actions["cash_per_old_share"], copy=True)
            action_payment = np.array(packed_actions["payment_day"], copy=True)
        action_size = dates.size * len(symbols)
        if (
            action_flat.dtype != np.int64
            or action_cash.dtype != np.float64
            or action_payment.dtype != np.int64
            or action_flat.ndim != 1
            or action_cash.shape != action_flat.shape
            or action_payment.shape != action_flat.shape
            or (
                action_flat.size
                and (
                    action_flat[0] < 0
                    or action_flat[-1] >= action_size
                    or np.any(action_flat[1:] <= action_flat[:-1])
                    or not np.isfinite(action_cash).all()
                    or np.any(action_cash <= 0.0)
                    or np.any(action_payment <= 0)
                )
            )
        ):
            raise RuntimeError("invalid physical sparse exact-cash cache")
        print(
            "[physical source] accepted cache "
            f"manifest_sha256={_sha256(manifest_path)} "
            f"minute_symbol_days={manifest['mode_counts']['minute_symbol_days']} "
            "daily_proxy_symbol_days="
            f"{manifest['mode_counts']['daily_proxy_pre_minute_symbol_days'] + manifest['mode_counts']['daily_proxy_post_minute_symbol_days']} "
            f"source_gap_symbol_days={manifest['source_gap_symbol_days']} "
            f"elapsed={time.monotonic() - source_started:.1f}s",
            flush=True,
        )

    def load_session(row: int) -> DayTradeCarrySession:
        day = dates[row]
        day_text = np.datetime_as_string(day, unit="D")
        gap = np.asarray(gaps[row], dtype=np.float64)
        action_mask = np.zeros(len(symbols), dtype=np.float64)
        share_ratio = np.ones(len(symbols), dtype=np.float64)
        cash_per_old_share = np.zeros(len(symbols), dtype=np.float64)
        payment_day = np.zeros(len(symbols), dtype=np.float64)
        flat_start = row * len(symbols)
        event_start = int(np.searchsorted(action_flat, flat_start, side="left"))
        event_stop = int(
            np.searchsorted(
                action_flat, flat_start + len(symbols), side="left"
            )
        )
        if event_stop > event_start:
            event_columns = (
                action_flat[event_start:event_stop] - flat_start
            ).astype(np.int64, copy=False)
            action_mask[event_columns] = 1.0
            cash_per_old_share[event_columns] = action_cash[event_start:event_stop]
            payment_day[event_columns] = action_payment[event_start:event_stop]
        if day < first_minute:
            marks = np.full((len(symbols), 270), np.nan, dtype=np.float64)
            exit_prices = np.full((len(symbols), 270, 2), np.nan, dtype=np.float64)
            exit_capacity = np.zeros_like(exit_prices)
            executable = (
                np.isfinite(opens[row]) & (opens[row] > 0)
                & np.isfinite(closes[row]) & (closes[row] > 0)
                & (gap == 0)
            )
            exit_prices[executable, -1, :] = closes[row, executable, None]
            capacity = (
                np.floor(np.maximum(volumes[row], 0) / 271.0 * 0.5 / BOARD_LOT_SHARES)
                * BOARD_LOT_SHARES
            )
            exit_capacity[executable, -1, :] = capacity[executable, None]
            marks[executable] = opening_marks[row, executable, None]
            marks[executable, -1] = closes[row, executable]
            carried_mark = (
                no_regular_execution[row]
                & (gap == 0)
                & np.isfinite(opening_marks[row])
                & (opening_marks[row] > 0)
            )
            marks[carried_mark] = opening_marks[row, carried_mark, None]
            entry_price = np.where(executable, opens[row], np.nan)
            entry_volume = np.where(executable, np.maximum(volumes[row], 0) / 271.0, 0)
            daily_proxy_mask = executable.astype(np.float64, copy=False)
        else:
            with np.load(
                cache_root / f"session-{day_text}.npz", allow_pickle=False
            ) as packed:
                if set(packed.files) != {
                    "exit_flat", "exit_price", "exit_capacity", "mark_flat",
                    "mark", "entry",
                }:
                    raise RuntimeError(f"invalid physical session fields: {day_text}")
                exit_flat = np.array(packed["exit_flat"], copy=True)
                exit_value = np.array(packed["exit_price"], copy=True)
                exit_cap = np.array(packed["exit_capacity"], copy=True)
                mark_flat = np.array(packed["mark_flat"], copy=True)
                mark_value = np.array(packed["mark"], copy=True)
                entry = np.array(packed["entry"], copy=True)
            exit_size = len(symbols) * 270 * 2
            mark_size = len(symbols) * 270
            if (
                exit_flat.dtype != np.int32
                or mark_flat.dtype != np.int32
                or exit_value.dtype != np.float64
                or exit_cap.dtype != np.float64
                or mark_value.dtype != np.float64
                or entry.dtype != np.float64
                or exit_flat.ndim != 1
                or mark_flat.ndim != 1
                or exit_value.shape != exit_flat.shape
                or exit_cap.shape != exit_flat.shape
                or mark_value.shape != mark_flat.shape
                or entry.shape != (len(symbols), 3)
                or (exit_flat.size and (
                    exit_flat[0] < 0 or exit_flat[-1] >= exit_size
                    or np.any(exit_flat[1:] <= exit_flat[:-1])
                ))
                or (mark_flat.size and (
                    mark_flat[0] < 0 or mark_flat[-1] >= mark_size
                    or np.any(mark_flat[1:] <= mark_flat[:-1])
                ))
                or not np.isfinite(exit_value).all()
                or not np.isfinite(exit_cap).all()
                or not np.isfinite(mark_value).all()
            ):
                raise RuntimeError(f"invalid physical sparse session cache: {day_text}")
            exit_prices = np.full((len(symbols), 270, 2), np.nan, dtype=np.float64)
            exit_capacity = np.zeros_like(exit_prices)
            marks = np.full((len(symbols), 270), np.nan, dtype=np.float64)
            exit_prices.reshape(-1)[exit_flat] = exit_value
            exit_capacity.reshape(-1)[exit_flat] = exit_cap
            marks.reshape(-1)[mark_flat] = mark_value
            entry_price, entry_volume = entry[:, 0], entry[:, 1]
            daily_proxy_mask = entry[:, 2]
        tensor = lambda value: torch.from_numpy(np.asarray(value, dtype=np.float64))
        return DayTradeCarrySession(
            day=_ordinal(day), official_open=tensor(opens[row]),
            opening_marks=tensor(opening_marks[row]), entry_price=tensor(entry_price),
            entry_volume=tensor(entry_volume), lower_limit=tensor(lower[row]),
            upper_limit=tensor(upper[row]),
            halted=tensor(no_regular_execution[row]),
            exit_prices=tensor(exit_prices), exit_capacity=tensor(exit_capacity),
            marks=tensor(marks), action_mask=tensor(action_mask),
            share_ratio=tensor(share_ratio),
            cash_per_old_share=tensor(cash_per_old_share),
            payment_day=tensor(payment_day), source_gap_mask=tensor(gap),
            daily_proxy_mask=tensor(daily_proxy_mask),
        )

    return PreparedDayTradeCarrySource(
        (), symbols, str(manifest["release_id"]),
        session_days=tuple(_ordinal(day) for day in dates), session_loader=load_session,
        audit_receipt={
            "abi": manifest["abi"],
            "cache_manifest": str(manifest_path),
            "cache_manifest_sha256": _sha256(manifest_path),
            "ready_receipt": str(ready_path),
            "ready_receipt_sha256": _sha256(ready_path),
            "source_gap_symbol_days": int(manifest["source_gap_symbol_days"]),
            "mode_counts": manifest["mode_counts"],
            "corporate_action_policy": manifest["corporate_action_policy"],
            "official_no_regular_execution": manifest[
                "official_no_regular_execution"
            ],
            "daily_proxy_source_precision": manifest[
                "daily_proxy_source_precision"
            ],
            "minute_partition_scope": manifest["minute_partition_scope"],
            "limit_counts": manifest["limit_counts"],
        },
    )


__all__ = [
    "PHYSICAL_SOURCE_CACHE_ABI",
    "build_prepared_day_trade_carry_source",
]

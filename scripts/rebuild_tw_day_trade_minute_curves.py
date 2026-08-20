#!/usr/bin/env python3
"""Rebuild audited one-minute day-trade and stock-benchmark curves.

The script never changes orders, fills, positions, or final PnL.  It preserves
the accepted 09:00 and 13:30 ledger marks byte-for-byte at the JSON-object
level and inserts right-labelled historical one-minute last-trade valuations
between them.  Missing trade minutes carry the latest observed trade/open
price and are explicitly counted; prices are never linearly interpolated.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, time as datetime_time, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Any, Mapping, Sequence
import sys

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live.tw_day_trade_simulation import (
    TAIPEI,
    position_net_liquidation_pnl,
)


SESSION_OPEN = datetime_time(9, 0)
SESSION_CLOSE = datetime_time(13, 30)
MINUTE_CONTRACT = "right_labelled_historical_last_trade_mark_v1"
DEFAULT_LOCAL_MINUTE_ROOTS = (
    Path("artifacts/data_repair/tw_day_trade_minute_curve/kbars"),
    Path("data_tw_minute/shioaji_1m"),
    Path("data_tw_minute/research_dataset"),
)
DEFAULT_LOCAL_MINUTE_CACHE_ROOTS = (
    Path("artifacts/data_repair/tw_day_trade_minute_curve/rebuilt/tick_minutes"),
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return payload


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError(f"JSONL row {line_number} is not an object: {path}")
            rows.append(payload)
    return rows


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_text(
        path,
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )


def _atomic_jsonl(path: Path, rows: list[Mapping[str, Any]]) -> None:
    _atomic_text(
        path,
        "".join(
            json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n"
            for row in rows
        ),
    )


def build_tick_minute_manifest(root: Path, output_path: Path) -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    for path in sorted(root.glob("*/*.parquet")):
        metadata = pl.read_parquet_schema(path)
        if not {"date", "ts", "Close", "Volume", "symbol"}.issubset(metadata):
            raise RuntimeError(f"invalid tick-minute schema: {path}")
        frame = pl.read_parquet(path, columns=["date", "symbol"])
        dates = frame["date"].unique().to_list()
        symbols = frame["symbol"].unique().to_list()
        if len(dates) != 1 or len(symbols) != 1:
            raise RuntimeError(f"tick-minute partition identity mismatch: {path}")
        files.append(
            {
                "path": str(path.resolve()),
                "relative_path": str(path.relative_to(root)),
                "symbol": str(symbols[0]),
                "session_date": dates[0].isoformat(),
                "rows": frame.height,
                "size_bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
        )
    manifest = {
        "schema_version": 1,
        "source": "shioaji_historical_ticks_resampled_right_labelled_1m",
        "session_close_boundary": "13:30:00 tick belongs to the 13:30 bar",
        "linear_interpolation_used": False,
        "file_count": len(files),
        "rows": sum(int(row["rows"]) for row in files),
        "size_bytes": sum(int(row["size_bytes"]) for row in files),
        "files": files,
    }
    _atomic_json(output_path, manifest)
    return manifest


def _session_minutes(day: date) -> list[datetime]:
    start = datetime.combine(day, SESSION_OPEN, tzinfo=TAIPEI)
    end = datetime.combine(day, SESSION_CLOSE, tzinfo=TAIPEI)
    return [
        start + timedelta(minutes=index)
        for index in range(271)
        if start + timedelta(minutes=index) <= end
    ]


def _in_range(day: str, start: date, end: date) -> bool:
    try:
        parsed = date.fromisoformat(str(day)[:10])
    except ValueError:
        return False
    return start <= parsed <= end


def _filled_quantity(position: Mapping[str, Any]) -> int:
    return int(position.get("filled_shares") or position.get("last_exit_quantity") or 0)


def load_positions(
    state_dir: Path,
    *,
    start: date,
    end: date,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    by_day: dict[str, dict[str, dict[str, dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    for path in sorted((state_dir / "position_history").glob("*/*.json")):
        payload = _read_json(path)
        session_date = str(payload.get("session_date") or path.parent.name)
        market = str(payload.get("market") or path.stem)
        if not _in_range(session_date, start, end):
            continue
        for position in payload.get("positions") or ():
            if not isinstance(position, Mapping) or _filled_quantity(position) <= 0:
                continue
            key = str(position.get("position_id") or position.get("symbol") or "")
            if not key:
                raise ValueError(f"position without identity: {path}")
            by_day[session_date][market][key] = dict(position)

    state = _read_json(state_dir / "state.json")
    for market, mode in (state.get("modes") or {}).items():
        if not isinstance(mode, Mapping):
            continue
        session_date = str(mode.get("session_date") or "")
        if not _in_range(session_date, start, end):
            continue
        raw_positions = mode.get("positions") or {}
        values = (
            raw_positions.values()
            if isinstance(raw_positions, Mapping)
            else raw_positions
        )
        for position in values:
            if not isinstance(position, Mapping) or _filled_quantity(position) <= 0:
                continue
            key = str(position.get("position_id") or position.get("symbol") or "")
            if not key:
                raise ValueError(f"current position without identity: {market}")
            by_day[session_date][str(market)][key] = dict(position)

    return {
        day: {market: list(rows.values()) for market, rows in markets.items()}
        for day, markets in by_day.items()
    }


class MinutePriceStore:
    """Read ordered local minute sources before any network fallback.

    Supported local layouts are the receipt-backed per-symbol source chunks
    (``minute_chunks/<symbol>/<start>_<end>.parquet``), the materialized
    research partitions (``trade_date=<date>/data.parquet``), and the legacy
    per-symbol/date Tick-minute cache.  The first KBar source containing a
    symbol-date pair wins; lower-priority sources never overwrite it.
    """

    def __init__(
        self,
        kbar_roots: Path | Sequence[Path],
        tick_minute_roots: Path | Sequence[Path],
    ) -> None:
        def unique_paths(value: Path | Sequence[Path]) -> tuple[Path, ...]:
            candidates = (
                [Path(value)]
                if isinstance(value, (str, Path))
                else [Path(root) for root in value]
            )
            unique: list[Path] = []
            seen: set[str] = set()
            for root in candidates:
                identity = str(root.resolve())
                if identity in seen:
                    continue
                seen.add(identity)
                unique.append(root)
            return tuple(unique)

        self.kbar_roots = unique_paths(kbar_roots)
        self.kbar_root = self.kbar_roots[0] if self.kbar_roots else Path(".")
        self.tick_minute_roots = unique_paths(tick_minute_roots)
        self.tick_minute_root = (
            self.tick_minute_roots[0] if self.tick_minute_roots else Path(".")
        )
        self._cache: dict[tuple[str, str], dict[str, float]] = {}
        self._source_cache: dict[tuple[str, str], str | None] = {}
        self._root_cache: dict[tuple[str, str, str], dict[str, float]] = {}
        self._chunk_index: dict[tuple[str, str], tuple[Path, ...]] = {}

    @staticmethod
    def _frame_prices(frame: pl.DataFrame, session_date: str) -> dict[str, float]:
        if not frame.height:
            return {}
        day = date.fromisoformat(session_date)
        selected = (
            frame.filter(pl.col("date") == pl.lit(day))
            if "date" in frame.columns
            else frame
        )
        output: dict[str, float] = {}
        for row in selected.select("ts", "Close").iter_rows(named=True):
            stamp = row["ts"]
            price = float(row["Close"])
            if isinstance(stamp, datetime) and math.isfinite(price) and price > 0.0:
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=TAIPEI)
                else:
                    stamp = stamp.astimezone(TAIPEI)
                output[stamp.isoformat(timespec="minutes")] = price
        return output

    def _chunk_paths(
        self,
        root: Path,
        symbol: str,
        session_date: str,
    ) -> tuple[Path, ...]:
        index_key = (str(root.resolve()), symbol)
        indexed = self._chunk_index.get(index_key)
        if indexed is None:
            indexed = tuple(sorted((root / "minute_chunks" / symbol).glob("*.parquet")))
            self._chunk_index[index_key] = indexed
        selected: list[Path] = []
        for path in indexed:
            boundaries = path.stem.split("_")
            if len(boundaries) == 2 and boundaries[0] <= session_date <= boundaries[1]:
                selected.append(path)
        return tuple(selected)

    def _root_prices(
        self,
        root: Path,
        symbol: str,
        session_date: str,
    ) -> dict[str, float]:
        root_key = (str(root.resolve()), symbol, session_date)
        cached = self._root_cache.get(root_key)
        if cached is not None:
            return dict(cached)

        output: dict[str, float] = {}
        for path in self._chunk_paths(root, symbol, session_date):
            frame = pl.read_parquet(path, columns=["date", "ts", "Close"])
            output.update(self._frame_prices(frame, session_date))
        if not output:
            partition = root / f"trade_date={session_date}" / "data.parquet"
            if partition.is_file():
                frame = (
                    pl.scan_parquet(partition)
                    .filter(pl.col("symbol") == symbol)
                    .select("date", "ts", "Close")
                    .collect()
                )
                output = self._frame_prices(frame, session_date)
        self._root_cache[root_key] = dict(output)
        return output

    def prepare(self, required: Mapping[str, set[str]]) -> None:
        """Batch-read each relevant local parquet at most once."""

        pairs = [
            (str(symbol), str(session_date))
            for symbol, dates in required.items()
            for session_date in dates
        ]
        for root in self.kbar_roots:
            staged: dict[tuple[str, str], dict[str, float]] = {
                pair: {} for pair in pairs
            }
            path_pairs: dict[Path, set[tuple[str, str]]] = defaultdict(set)
            for symbol, session_date in pairs:
                for path in self._chunk_paths(root, symbol, session_date):
                    path_pairs[path].add((symbol, session_date))
            for path, selected_pairs in path_pairs.items():
                dates = sorted({date.fromisoformat(day) for _, day in selected_pairs})
                frame = (
                    pl.scan_parquet(path)
                    .filter(pl.col("date").is_in(dates))
                    .select("date", "ts", "Close")
                    .collect()
                )
                for symbol, session_date in selected_pairs:
                    staged[(symbol, session_date)].update(
                        self._frame_prices(frame, session_date)
                    )

            by_date: dict[str, set[str]] = defaultdict(set)
            for symbol, session_date in pairs:
                if not staged[(symbol, session_date)]:
                    by_date[session_date].add(symbol)
            for session_date, symbols in by_date.items():
                partition = root / f"trade_date={session_date}" / "data.parquet"
                if not partition.is_file():
                    continue
                frame = (
                    pl.scan_parquet(partition)
                    .filter(pl.col("symbol").is_in(sorted(symbols)))
                    .select("symbol", "date", "ts", "Close")
                    .collect()
                )
                for symbol in symbols:
                    selected = frame.filter(pl.col("symbol") == symbol).select(
                        "date", "ts", "Close"
                    )
                    staged[(symbol, session_date)] = self._frame_prices(
                        selected, session_date
                    )

            root_identity = str(root.resolve())
            for (symbol, session_date), prices in staged.items():
                self._root_cache[(root_identity, symbol, session_date)] = dict(prices)

    def prices(self, symbol: str, session_date: str) -> dict[str, float]:
        key = (str(symbol), str(session_date))
        cached = self._cache.get(key)
        if cached is not None:
            return dict(cached)
        output: dict[str, float] = {}
        source: str | None = None
        for root in self.kbar_roots:
            output = self._root_prices(root, key[0], key[1])
            if output:
                source = f"local_kbar:{root.resolve()}"
                break
        if not output:
            for tick_root in self.tick_minute_roots:
                tick_path = tick_root / key[0] / f"{key[1]}.parquet"
                if not tick_path.is_file():
                    continue
                frame = pl.read_parquet(tick_path, columns=["date", "ts", "Close"])
                output = self._frame_prices(frame, key[1])
                if output:
                    source = f"local_tick_cache:{tick_root.resolve()}"
                    break
        self._cache[key] = dict(output)
        self._source_cache[key] = source
        return output

    def invalidate(self, symbol: str, session_date: str) -> None:
        key = (str(symbol), str(session_date))
        self._cache.pop(key, None)
        self._source_cache.pop(key, None)
        for root in self.kbar_roots:
            root_identity = str(root.resolve())
            self._root_cache.pop((root_identity, key[0], key[1]), None)
            self._chunk_index.pop((root_identity, key[0]), None)

    def missing_pairs(self, required: Mapping[str, set[str]]) -> list[tuple[str, str]]:
        return sorted(
            (str(symbol), str(session_date))
            for symbol, dates in required.items()
            for session_date in dates
            if not self.prices(str(symbol), str(session_date))
        )

    def coverage(self, required: Mapping[str, set[str]]) -> dict[str, Any]:
        source_pairs: Counter[str] = Counter()
        source_minutes: Counter[str] = Counter()
        missing: list[tuple[str, str]] = []
        total = 0
        for symbol, dates in sorted(required.items()):
            for session_date in sorted(dates):
                total += 1
                prices = self.prices(str(symbol), str(session_date))
                source = self._source_cache.get((str(symbol), str(session_date)))
                if not prices or source is None:
                    missing.append((str(symbol), str(session_date)))
                    continue
                source_pairs[source] += 1
                source_minutes[source] += len(prices)
        return {
            "required_pairs": total,
            "available_pairs": total - len(missing),
            "missing_pairs": len(missing),
            "missing_pair_sample": [list(pair) for pair in missing[:100]],
            "source_pair_counts": dict(sorted(source_pairs.items())),
            "source_minute_counts": dict(sorted(source_minutes.items())),
        }


def required_symbol_dates(
    positions: Mapping[str, Mapping[str, list[dict[str, Any]]]],
    session_dates: list[str],
) -> dict[str, set[str]]:
    required: dict[str, set[str]] = defaultdict(set)
    for session_date in session_dates:
        required["0050"].add(session_date)
        required["2330"].add(session_date)
        for mode_positions in (positions.get(session_date) or {}).values():
            for position in mode_positions:
                required[str(position["symbol"])].add(session_date)
    return required


def _ticks_to_minute_frame(
    ticks: Any, *, symbol: str, session_date: str
) -> pl.DataFrame:
    frame = pl.DataFrame(
        {"ts": ticks.ts, "price": ticks.close, "volume": ticks.volume}
    ).with_columns(pl.col("ts").cast(pl.Datetime("ns")))
    if not frame.height:
        return pl.DataFrame(
            schema={
                "ts": pl.Datetime("ns"),
                "Close": pl.Float64,
                "Volume": pl.Float64,
                "date": pl.Date,
                "symbol": pl.String,
            }
        )
    session_open = datetime.fromisoformat(f"{session_date}T09:00:00")
    session_close = datetime.fromisoformat(f"{session_date}T13:30:00")
    return (
        frame.filter((pl.col("ts") >= session_open) & (pl.col("ts") <= session_close))
        .with_columns(
            pl.when(pl.col("ts") == session_close)
            .then(pl.col("ts") - pl.duration(microseconds=1))
            .otherwise(pl.col("ts"))
            .alias("bucket_input")
        )
        .with_columns(
            (pl.col("bucket_input").dt.truncate("1m") + pl.duration(minutes=1)).alias(
                "ts"
            )
        )
        .group_by("ts", maintain_order=True)
        .agg(
            pl.col("price").last().cast(pl.Float64).alias("Close"),
            pl.col("volume").sum().cast(pl.Float64).alias("Volume"),
        )
        .with_columns(
            pl.lit(date.fromisoformat(session_date)).alias("date"),
            pl.lit(symbol).alias("symbol"),
        )
        .sort("ts")
    )


def fetch_missing_kbars(
    store: MinutePriceStore,
    required: Mapping[str, set[str]],
    *,
    output_root: Path,
    simulation: bool,
    workers: int,
    requests_per_second: float,
    max_traffic_fraction: float,
    traffic_reserve_mb: float,
) -> dict[str, Any]:
    """Delegate true local misses to the canonical receipt-backed collector."""

    missing = store.missing_pairs(required)
    if not missing:
        return {
            "api_process_started": False,
            "reason": "all_required_symbol_dates_found_locally",
            "requested_symbols": 0,
            "missing_before": 0,
            "missing_after": 0,
            "api_requests_started": 0,
        }

    symbols = sorted({symbol for symbol, _ in missing})
    start = min(session_date for _, session_date in missing)
    end = max(session_date for _, session_date in missing)
    output_root.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        str(REPO_ROOT / "downloader" / "download_shioaji_tw_minute_kbars.py"),
        "--output-dir",
        str(output_root),
        "--start-date",
        start,
        "--end-date",
        end,
        "--symbols",
        ",".join(symbols),
        "--workers",
        str(workers),
        "--requests-per-second",
        str(requests_per_second),
        "--max-traffic-fraction",
        str(max_traffic_fraction),
        "--traffic-reserve-mb",
        str(traffic_reserve_mb),
    ]
    if simulation:
        command.append("--simulation")
    completed = subprocess.run(command, cwd=REPO_ROOT, check=False)

    summary: dict[str, Any] = {}
    for name in ("download_summary.json", "latest_run_summary.json"):
        path = output_root / name
        if path.is_file():
            summary = _read_json(path)
            break
    for symbol, session_date in missing:
        store.invalidate(symbol, session_date)
    missing_after = store.missing_pairs(required)
    result = {
        "api_process_started": True,
        "collector": "downloader/download_shioaji_tw_minute_kbars.py",
        "collector_returncode": completed.returncode,
        "requested_symbols": len(symbols),
        "requested_start_date": start,
        "requested_end_date": end,
        "missing_before": len(missing),
        "missing_after": len(missing_after),
        "api_requests_started": int(summary.get("api_requests_started_this_run") or 0),
        "stopped_for_traffic": bool(summary.get("stopped_for_traffic")),
        "stopped_for_market_hours": bool(summary.get("stopped_for_market_hours")),
        "summary_path": str((output_root / name).resolve()) if summary else None,
    }
    if completed.returncode != 0 or missing_after:
        raise RuntimeError(
            "canonical Shioaji minute KBar fallback did not close all local gaps: "
            + json.dumps(result, ensure_ascii=False, sort_keys=True)
        )
    return result


def rebuild_strategy_marks(
    source_rows: list[dict[str, Any]],
    positions: Mapping[str, Mapping[str, list[dict[str, Any]]]],
    store: MinutePriceStore,
    *,
    start: date,
    end: date,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    outside = [
        dict(row)
        for row in source_rows
        if not _in_range(str(row.get("session_date") or ""), start, end)
    ]
    selected = [
        row
        for row in source_rows
        if _in_range(str(row.get("session_date") or ""), start, end)
    ]
    endpoints = {
        (str(row["session_date"]), str(row["market"]), str(row["minute"])[11:16]): row
        for row in selected
        if str(row.get("minute") or "")[11:16] in {"09:00", "13:30"}
    }
    session_dates = sorted({key[0] for key in endpoints})
    markets = sorted({key[1] for key in endpoints})
    generated: list[dict[str, Any]] = []
    carried_rows = 0
    fresh_ratios: list[float] = []
    for session_date in session_dates:
        day = date.fromisoformat(session_date)
        for market in markets:
            opening = endpoints.get((session_date, market, "09:00"))
            closing = endpoints.get((session_date, market, "13:30"))
            if opening is None or closing is None:
                raise RuntimeError(
                    f"missing accepted endpoint for {session_date}:{market}"
                )
            mode_positions = list((positions.get(session_date) or {}).get(market) or ())
            expected_open = int(opening.get("open_position_count") or 0)
            if expected_open != len(mode_positions):
                raise RuntimeError(
                    f"position count mismatch for {session_date}:{market}: "
                    f"accepted={expected_open} reconstructed={len(mode_positions)}"
                )
            current_prices: dict[str, float] = {}
            minute_prices: dict[str, dict[str, float]] = {}
            for position in mode_positions:
                symbol = str(position["symbol"])
                initial = float(
                    position.get("sizing_open_price")
                    or position.get("last_mark_price")
                    or 0.0
                )
                if not math.isfinite(initial) or initial <= 0.0:
                    raise RuntimeError(
                        f"missing official opening price for {session_date}:{market}:{symbol}"
                    )
                current_prices[symbol] = initial
                minute_prices[symbol] = store.prices(symbol, session_date)

            for minute in _session_minutes(day):
                clock = minute.strftime("%H:%M")
                if clock == "09:00":
                    generated.append(dict(opening))
                    continue
                if clock == "13:30":
                    generated.append(dict(closing))
                    continue
                fresh_symbols: set[str] = set()
                for symbol, prices in minute_prices.items():
                    value = prices.get(minute.isoformat(timespec="minutes"))
                    if value is not None:
                        current_prices[symbol] = value
                        fresh_symbols.add(symbol)
                open_net = 0.0
                total_notional = 0.0
                fresh_notional = 0.0
                for position in mode_positions:
                    symbol = str(position["symbol"])
                    quantity = _filled_quantity(position)
                    signed = (
                        quantity if str(position.get("side")) == "long" else -quantity
                    )
                    price = current_prices[symbol]
                    open_net += position_net_liquidation_pnl(
                        position,
                        price,
                        signed_shares=signed,
                        remaining_entry_fee_twd=float(
                            position.get("entry_fee_twd") or 0.0
                        ),
                    )
                    notional = abs(quantity * price)
                    total_notional += notional
                    if symbol in fresh_symbols:
                        fresh_notional += notional
                carried = max(0, expected_open - len(fresh_symbols))
                coverage = (
                    fresh_notional / total_notional if total_notional > 0.0 else 1.0
                )
                carried_rows += int(carried > 0)
                fresh_ratios.append(coverage)
                cumulative = float(
                    opening.get("cumulative_realized_net_pnl_twd") or 0.0
                )
                initial_capital = float(opening.get("initial_capital_twd") or 0.0)
                generated.append(
                    {
                        "recorded_at": minute.isoformat(timespec="seconds"),
                        "minute": minute.isoformat(timespec="minutes"),
                        "session_date": session_date,
                        "market": market,
                        "initial_capital_twd": initial_capital,
                        "cumulative_realized_net_pnl_twd": cumulative,
                        "open_net_liquidation_pnl_twd": open_net,
                        "total_equity_twd": initial_capital + cumulative + open_net,
                        "open_position_count": expected_open,
                        "stale_position_count": carried,
                        "valuation_stale": carried > 0,
                        "fresh_trade_position_count": len(fresh_symbols),
                        "last_trade_carried_position_count": carried,
                        "missing_price_position_count": 0,
                        "fresh_trade_notional_coverage_ratio": coverage,
                        "historical_minute_replay": True,
                        "minute_valuation_contract": MINUTE_CONTRACT,
                        "valuation_source": "shioaji_historical_1m_close_with_last_trade_carry",
                        "valuation_executable": False,
                        "simulation_only": True,
                    }
                )
    rows = outside + generated
    rows.sort(
        key=lambda row: (str(row.get("minute") or ""), str(row.get("market") or ""))
    )
    expected = len(session_dates) * len(markets) * 271
    if len(generated) != expected:
        raise RuntimeError(
            f"strategy minute cardinality mismatch: {len(generated)} != {expected}"
        )
    return rows, {
        "session_dates": session_dates,
        "markets": markets,
        "generated_rows": len(generated),
        "rows_with_carried_prices": carried_rows,
        "minimum_fresh_trade_notional_coverage_ratio": min(fresh_ratios, default=1.0),
        "mean_fresh_trade_notional_coverage_ratio": (
            sum(fresh_ratios) / len(fresh_ratios) if fresh_ratios else 1.0
        ),
    }


def _benchmark_minute_row(
    template: Mapping[str, Any],
    *,
    minute: datetime,
    price: float,
    fresh: bool,
) -> dict[str, Any]:
    row = dict(template)
    quantity = float(row.get("adjusted_quantity") or row.get("quantity") or 0.0)
    entry_price = float(row["entry_price"])
    initial_capital = float(row["initial_capital_twd"])
    initial_fees = float(row.get("initial_fixed_fees_twd") or 0.0)
    template_price = float(template["last_mark_price"])
    template_cost = float(template.get("liquidation_cost_twd") or 0.0)
    liquidation_rate = (
        template_cost / (quantity * template_price) if quantity > 0.0 else 0.0
    )
    liquidation_cost = quantity * price * liquidation_rate
    net_pnl = quantity * (price - entry_price) - initial_fees - liquidation_cost
    total_equity = initial_capital + net_pnl
    row.update(
        {
            "recorded_at": minute.isoformat(timespec="seconds"),
            "minute": minute.isoformat(timespec="minutes"),
            "last_mark_at": minute.isoformat(timespec="seconds"),
            "last_quote_at": minute.isoformat(timespec="seconds"),
            "last_mark_price": price,
            "liquidation_cost_twd": liquidation_cost,
            "net_pnl_twd": net_pnl,
            "total_equity_twd": total_equity,
            "return_fraction": net_pnl / initial_capital,
            "return_pct": net_pnl / initial_capital * 100.0,
            "source": "shioaji_historical_1m_close",
            "valuation_source": "historical_last_trade_mark_after_tw_cash_costs_not_executable_bid",
            "valuation_stale": not fresh,
            "historical_minute_replay": True,
            "minute_valuation_contract": MINUTE_CONTRACT,
            "fresh_trade_position_count": int(fresh),
            "last_trade_carried_position_count": int(not fresh),
            "missing_price_position_count": 0,
            "fresh_trade_notional_coverage_ratio": float(fresh),
            "valuation_executable": False,
        }
    )
    return row


def rebuild_benchmark_history(
    source: dict[str, Any],
    store: MinutePriceStore,
    *,
    start: date,
    end: date,
) -> tuple[dict[str, Any], dict[str, Any]]:
    marks = [row for row in source.get("marks") or () if isinstance(row, Mapping)]
    stock_ids = {"benchmark_0050": "0050", "benchmark_2330": "2330"}
    outside_or_other = [
        dict(row)
        for row in marks
        if row.get("benchmark_id") not in stock_ids
        or not _in_range(str(row.get("session_date") or ""), start, end)
    ]
    endpoints = {
        (
            str(row["session_date"]),
            str(row["benchmark_id"]),
            str(row["minute"])[11:16],
        ): row
        for row in marks
        if row.get("benchmark_id") in stock_ids
        and _in_range(str(row.get("session_date") or ""), start, end)
        and str(row.get("minute") or "")[11:16] in {"09:00", "13:30"}
    }
    session_dates = sorted({key[0] for key in endpoints})
    generated: list[dict[str, Any]] = []
    carried_rows = 0
    for session_date in session_dates:
        day = date.fromisoformat(session_date)
        for benchmark_id, symbol in stock_ids.items():
            opening = endpoints.get((session_date, benchmark_id, "09:00"))
            closing = endpoints.get((session_date, benchmark_id, "13:30"))
            if opening is None or closing is None:
                raise RuntimeError(
                    f"missing stock benchmark endpoint: {session_date}:{benchmark_id}"
                )
            prices = store.prices(symbol, session_date)
            current = float(opening["last_mark_price"])
            for minute in _session_minutes(day):
                clock = minute.strftime("%H:%M")
                if clock == "09:00":
                    generated.append(dict(opening))
                    continue
                if clock == "13:30":
                    generated.append(dict(closing))
                    continue
                value = prices.get(minute.isoformat(timespec="minutes"))
                fresh = value is not None
                if fresh:
                    current = float(value)
                else:
                    carried_rows += 1
                generated.append(
                    _benchmark_minute_row(
                        opening, minute=minute, price=current, fresh=fresh
                    )
                )
    output = dict(source)
    output_marks = outside_or_other + generated
    output_marks.sort(
        key=lambda row: (
            str(row.get("minute") or ""),
            str(row.get("benchmark_id") or ""),
        )
    )
    output["marks"] = output_marks
    tx_count = sum(
        row.get("benchmark_id") == "benchmark_tx_continuous" for row in output_marks
    )
    stock_count = sum(row.get("benchmark_id") in stock_ids for row in output_marks)
    output["counts"] = {
        **(output.get("counts") or {}),
        "marks": len(output_marks),
        "stock_marks": stock_count,
        "tx_minute_marks": tx_count,
    }
    output["minute_curve_contract"] = {
        "schema_version": 1,
        "contract": MINUTE_CONTRACT,
        "stock_marks": "right-labelled Shioaji historical 1m close; no interpolation",
        "missing_trade_minutes": "carry the latest observed trade/open and disclose the carry",
        "accepted_endpoints": "09:00 and 13:30 marks are preserved",
        "historical_marks_are_executable_quotes": False,
    }
    expected = len(session_dates) * len(stock_ids) * 271
    if len(generated) != expected:
        raise RuntimeError(
            f"benchmark minute cardinality mismatch: {len(generated)} != {expected}"
        )
    return output, {
        "generated_stock_rows": len(generated),
        "tx_rows_preserved": tx_count,
        "stock_rows_with_carried_price": carried_rows,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path("artifacts/live/tw_day_trade_simulation"),
    )
    parser.add_argument(
        "--kbar-root",
        "--local-minute-root",
        dest="local_minute_roots",
        type=Path,
        action="append",
        help=(
            "Highest-priority local KBar/research root. Repeatable; canonical "
            "project roots are appended automatically."
        ),
    )
    parser.add_argument(
        "--local-minute-cache-root",
        dest="local_minute_cache_roots",
        type=Path,
        action="append",
        help=(
            "Retained per-symbol/date minute cache used only after every local "
            "KBar/research root misses. Repeatable."
        ),
    )
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--fetch-missing-kbars",
        "--fetch-missing-ticks",
        dest="fetch_missing_kbars",
        action="store_true",
        help=(
            "After exhausting every local minute source, delegate only true "
            "symbol-date gaps to the canonical Shioaji KBar downloader."
        ),
    )
    parser.add_argument("--simulation", action="store_true")
    parser.add_argument("--fetch-workers", type=int, default=1)
    parser.add_argument("--requests-per-second", type=float, default=5.0)
    parser.add_argument("--max-traffic-fraction", type=float, default=0.75)
    parser.add_argument("--traffic-reserve-mb", type=float, default=256.0)
    parser.add_argument("--publish", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if start > end:
        raise ValueError("start date must not be after end date")
    if not 0.0 < args.requests_per_second <= 10.0:
        raise ValueError("requests per second must be in (0, 10]")
    if not 0.0 < args.max_traffic_fraction < 1.0:
        raise ValueError("max traffic fraction must be in (0, 1)")
    if not 1 <= args.fetch_workers <= 5:
        raise ValueError("fetch workers must be between 1 and 5")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    tick_root = args.output_dir / "tick_minutes"
    fetched_kbar_root = args.output_dir / "fetched_kbars"
    local_roots = [
        *(args.local_minute_roots or ()),
        *DEFAULT_LOCAL_MINUTE_ROOTS,
        fetched_kbar_root,
    ]
    local_cache_roots = [
        *(args.local_minute_cache_roots or ()),
        tick_root,
        *DEFAULT_LOCAL_MINUTE_CACHE_ROOTS,
    ]
    store = MinutePriceStore(local_roots, local_cache_roots)
    positions = load_positions(args.state_dir, start=start, end=end)
    source_marks = _read_jsonl(args.state_dir / "marks.jsonl")
    session_dates = sorted(
        {
            str(row.get("session_date"))
            for row in source_marks
            if _in_range(str(row.get("session_date") or ""), start, end)
        }
    )
    required = required_symbol_dates(positions, session_dates)
    store.prepare(required)
    coverage_before = store.coverage(required)
    fetch = {
        "api_process_started": False,
        "reason": "api_fallback_disabled",
        "requested_symbols": 0,
        "missing_before": coverage_before["missing_pairs"],
        "missing_after": coverage_before["missing_pairs"],
        "api_requests_started": 0,
    }
    if args.fetch_missing_kbars:
        fetch = fetch_missing_kbars(
            store,
            required,
            output_root=fetched_kbar_root,
            simulation=bool(args.simulation),
            workers=int(args.fetch_workers),
            requests_per_second=float(args.requests_per_second),
            max_traffic_fraction=float(args.max_traffic_fraction),
            traffic_reserve_mb=float(args.traffic_reserve_mb),
        )
    coverage_after = store.coverage(required)
    missing = store.missing_pairs(required)
    if missing:
        gap_audit = {
            "schema_version": 1,
            "created_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
            "local_first_contract": True,
            "api_fallback_enabled": bool(args.fetch_missing_kbars),
            "local_minute_roots": [str(root.resolve()) for root in store.kbar_roots],
            "local_minute_cache_roots": [
                str(root.resolve()) for root in store.tick_minute_roots
            ],
            "coverage_before_fetch": coverage_before,
            "coverage_after_fetch": coverage_after,
            "fetch": fetch,
        }
        _atomic_json(args.output_dir / "minute_curve_gap_audit.json", gap_audit)
        raise RuntimeError(f"required minute prices are missing: {missing[:20]}")

    rebuilt_marks, strategy_stats = rebuild_strategy_marks(
        source_marks, positions, store, start=start, end=end
    )
    source_benchmarks = _read_json(args.state_dir / "benchmark_history.json")
    rebuilt_benchmarks, benchmark_stats = rebuild_benchmark_history(
        source_benchmarks, store, start=start, end=end
    )
    tick_manifest_path = args.output_dir / "tick_minute_manifest.json"
    tick_manifest = build_tick_minute_manifest(tick_root, tick_manifest_path)
    marks_path = args.output_dir / "marks.jsonl"
    benchmark_path = args.output_dir / "benchmark_history.json"
    _atomic_jsonl(marks_path, rebuilt_marks)
    _atomic_json(benchmark_path, rebuilt_benchmarks)
    receipt = {
        "schema_version": 2,
        "created_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
        "simulation_only": True,
        "production_order_possible": False,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "minute_contract": MINUTE_CONTRACT,
        "linear_interpolation_used": False,
        "accepted_09_00_and_13_30_endpoints_preserved": True,
        "historical_minute_marks_are_executable_quotes": False,
        "local_first_contract": (
            "ordered local KBar and research partitions first; canonical "
            "Shioaji KBar collector only for remaining symbol-date gaps"
        ),
        "local_minute_sources": [
            {
                "root": str(root.resolve()),
                "exists": root.exists(),
                "layout": (
                    "symbol_chunks"
                    if (root / "minute_chunks").is_dir()
                    else (
                        "trade_date_partitions"
                        if (root / "manifest.json").is_file()
                        else "unavailable"
                    )
                ),
                "receipt_path": str(
                    next(
                        (
                            path.resolve()
                            for path in (
                                root / "download_summary.json",
                                root / "manifest.json",
                            )
                            if path.is_file()
                        ),
                        "",
                    )
                ),
                "receipt_sha256": next(
                    (
                        _sha256(path)
                        for path in (
                            root / "download_summary.json",
                            root / "manifest.json",
                        )
                        if path.is_file()
                    ),
                    None,
                ),
            }
            for root in store.kbar_roots
        ],
        "local_minute_cache_sources": [
            {
                "root": str(root.resolve()),
                "exists": root.is_dir(),
                "layout": "symbol_date_parquet_cache",
            }
            for root in store.tick_minute_roots
        ],
        "coverage_before_fetch": coverage_before,
        "coverage_after_fetch": coverage_after,
        "fetch": fetch,
        "tick_minute_catalog": {
            "path": str(tick_manifest_path.resolve()),
            "sha256": _sha256(tick_manifest_path),
            "file_count": tick_manifest["file_count"],
            "rows": tick_manifest["rows"],
            "size_bytes": tick_manifest["size_bytes"],
        },
        "strategy": strategy_stats,
        "benchmarks": benchmark_stats,
        "outputs": {
            "marks": {
                "path": str(marks_path.resolve()),
                "rows": len(rebuilt_marks),
                "sha256": _sha256(marks_path),
            },
            "benchmark_history": {
                "path": str(benchmark_path.resolve()),
                "rows": len(rebuilt_benchmarks.get("marks") or ()),
                "sha256": _sha256(benchmark_path),
            },
        },
    }
    receipt_path = args.output_dir / "minute_curve_receipt.json"
    _atomic_json(receipt_path, receipt)
    if args.publish:
        _atomic_text(
            args.state_dir / "marks.jsonl", marks_path.read_text(encoding="utf-8")
        )
        _atomic_text(
            args.state_dir / "benchmark_history.json",
            benchmark_path.read_text(encoding="utf-8"),
        )
        _atomic_text(
            args.state_dir / "minute_curve_receipt.json",
            receipt_path.read_text(encoding="utf-8"),
        )
    print(json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

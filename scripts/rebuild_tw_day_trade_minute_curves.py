#!/usr/bin/env python3
"""Rebuild audited one-minute day-trade and stock-benchmark curves.

The script never changes orders, fills, positions, or final PnL. It preserves
the accepted 13:30 ledger marks and reconstructs right-labelled historical
minute valuations. Explicit --revalue-opening-marks repairs entry-fee-only
09:01 marks from the completed source Close. Missing later trade minutes carry
the latest observed trade and are counted; prices are never interpolated.
"""

from __future__ import annotations

import argparse
from contextlib import nullcontext
from collections import Counter, defaultdict
from datetime import date, datetime, time as datetime_time, timedelta
import hashlib
import fcntl
import json
import math
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Any, Iterable, Iterator, Mapping, Sequence
import sys

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live.tw_day_trade_simulation import (
    TAIPEI,
    position_net_liquidation_pnl,
    minute_curve_write_lock,
)
from stockagent.live.benchmark_accounting import previous_close_return
from stockagent.live.shioaji_schedule import HISTORICAL_MAX_TRAFFIC_FRACTION


SESSION_OPEN = datetime_time(9, 0)
STRATEGY_ENTRY = datetime_time(9, 1)
SESSION_CLOSE = datetime_time(13, 30)
MINUTE_CONTRACT = "right_labelled_historical_last_trade_mark_v1"
LAST_TRADE_SETTLEMENT_CONTRACT = (
    "user_authorized_last_traded_price_paper_settlement_v1"
)
DEFAULT_LOCAL_MINUTE_ROOTS = (
    Path("artifacts/data_repair/tw_day_trade_minute_curve/maintenance/current/fetched_kbars"),
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


def _iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError(f"JSONL row {line_number} is not an object: {path}")
            yield payload


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return list(_iter_jsonl(path))


def missing_accepted_endpoints(
    rows: Iterable[Mapping[str, Any]], *, start: date, end: date,
    expected_sessions: Iterable[str] | None = None,
    expected_markets: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    """Check irrecoverable ledger gaps before loading minute data or fetching.

    Retain only endpoint keys so maintenance can stream a large marks ledger.
    The completed-session/mode contract can also expose wholly absent pairs.
    """
    sessions: set[str] = set()
    markets: set[str] = set()
    present: set[tuple[str, str, str]] = set()
    for row in rows:
        session = str(row.get("session_date") or "")
        if not _in_range(session, start, end):
            continue
        market = str(row.get("market") or "")
        sessions.add(session)
        markets.add(market)
        minute = str(row.get("minute") or "")
        for clock in ("09:01", "13:30"):
            if minute == f"{session}T{clock}+08:00":
                present.add((session, market, clock))
    if expected_sessions is not None:
        sessions = set(expected_sessions)
    if expected_markets is not None:
        markets = set(expected_markets)
    missing = []
    for session in sorted(sessions):
        for market in sorted(markets):
            clocks = [clock for clock in ("09:01", "13:30")
                      if (session, market, clock) not in present]
            if clocks:
                missing.append({"session_date": session, "market": market, "missing_clocks": clocks})
    return missing


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


def _atomic_json(
    path: Path,
    payload: Mapping[str, Any],
    *,
    compact: bool = False,
) -> None:
    _atomic_text(
        path,
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=None if compact else 2,
            separators=(",", ":") if compact else None,
            sort_keys=True,
        )
        + "\n",
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
    fill_rows: Sequence[Mapping[str, Any]] | None = None,
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

    if fill_rows is not None:
        # Archives may retain positions from an older replay under the same
        # stable market ID. Only the current append-only entry ledger proves
        # that a position belongs to this replay; never resurrect stale trades.
        entries = {
            (str(row.get("session_date")), str(row.get("market")), str(row.get("position_id"))): row
            for row in fill_rows if row.get("purpose") == "entry"
            and _in_range(str(row.get("session_date") or ""), start, end)
        }
        carried_entries = {(market, key): row for (_, market, key), row in entries.items()}
        for day, markets in by_day.items():
            for market, positions in markets.items():
                for key in list(positions):
                    entry = entries.get((day, market, key))
                    if entry is None and positions[key].get("margin_carry_contract"):
                        entry = carried_entries.get((market, key))
                        if entry is not None and str(entry["session_date"]) > day:
                            raise RuntimeError("carried archive precedes its entry")
                    if entry is None:
                        del positions[key]
                        continue
                    position = positions[key]
                    if (_filled_quantity(position) != int(entry["quantity"])
                        or not math.isclose(float(position["entry_price"]), float(entry["price"]), abs_tol=1e-8)):
                        raise RuntimeError(f"position archive disagrees with entry ledger: {day}:{market}:{key}")
        retained = {(day, market, key) for day, markets in by_day.items()
                    for market, positions in markets.items() for key in positions}
        missing = set(entries) - retained
        if missing:
            raise RuntimeError(f"entry ledger has no reconstructable position: {sorted(missing)[:10]}")
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
        *, require_receipts: bool = False,
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
        self.require_receipts = require_receipts
        self._verified_chunk_dates: dict[Path, tuple[tuple, set[str]]] = {}
        self._verified_raw_tick_signatures: dict[Path, tuple[int, int, int]] = {}

    def _verified_chunk(self, path: Path, symbol: str, session_date: str) -> bool:
        if not self.require_receipts:
            return True
        receipt = path.with_suffix(".receipt.json")
        if not receipt.is_file():
            return False
        signature = tuple((p.stat().st_size, p.stat().st_mtime_ns, p.stat().st_ctime_ns) for p in (path, receipt))
        cached = self._verified_chunk_dates.get(path)
        if cached is None or cached[0] != signature:
            from downloader.download_shioaji_tw_minute_kbars import minute_receipt_valid
            first, last = map(date.fromisoformat, path.stem.split("_"))
            proof = _read_json(receipt)
            output = proof.get("output_receipt") or {}
            valid = (Path(str(output.get("path", ""))).resolve() == path.resolve()
                     and minute_receipt_valid(receipt, symbol=symbol, start=first, end=last))
            if valid:
                for source in proof.get("raw_tick_sources", []):
                    raw = Path(source["path"])
                    stat = raw.stat()
                    signature_now = (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
                    previous = self._verified_raw_tick_signatures.setdefault(raw, signature_now)
                    if previous != signature_now:
                        raise RuntimeError(f"verified raw tick source changed: {raw}")
            days = set(proof.get("returned_dates", [])) if valid else set()
            self._verified_chunk_dates[path] = (signature, days)
        return session_date in self._verified_chunk_dates[path][1]

    def assert_sources_unchanged(self) -> None:
        """Reject a price/receipt replacement between verification and publish."""
        for path, expected in self._verified_raw_tick_signatures.items():
            stat = path.stat()
            if (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns) != expected:
                raise RuntimeError(f"verified raw tick source changed: {path}")
        for path, (expected, _) in self._verified_chunk_dates.items():
            try:
                actual = tuple((p.stat().st_size, p.stat().st_mtime_ns, p.stat().st_ctime_ns)
                               for p in (path, path.with_suffix(".receipt.json")))
            except OSError as exc:
                raise RuntimeError(f"verified minute source disappeared: {path}") from exc
            if actual != expected:
                raise RuntimeError(f"verified minute source changed during reconstruction: {path}")

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
        if "Volume" in selected.columns:
            selected = selected.filter(pl.col("Volume").is_finite() & (pl.col("Volume") > 0))
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
            if (len(boundaries) == 2 and boundaries[0] <= session_date <= boundaries[1]
                    and self._verified_chunk(path, symbol, session_date)):
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
            frame = pl.read_parquet(path, columns=["date", "ts", "Close", *(["Volume"] if self.require_receipts else [])])
            output.update(self._frame_prices(frame, session_date))
        if not output and not self.require_receipts:
            partition = root / f"trade_date={session_date}" / "data.parquet"
            if partition.is_file():
                frame = (
                    pl.scan_parquet(partition)
                    .filter(pl.col("symbol") == symbol)
                    .select("date", "ts", "Close", *(["Volume"] if self.require_receipts else []))
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
                    .select("date", "ts", "Close", *(["Volume"] if self.require_receipts else []))
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
                if self.require_receipts:
                    continue
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
            # prices() uses the first nonempty source for a symbol/session.
            # Reading lower-priority duplicates cannot change that result and
            # needlessly loads the same multi-month tape twice into memory.
            pairs = [pair for pair in pairs if not staged[pair]]
            if not pairs:
                break

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
        if not output and not self.require_receipts:
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
    *,
    include_stock_benchmarks: bool = True,
) -> dict[str, set[str]]:
    required: dict[str, set[str]] = defaultdict(set)
    for session_date in session_dates:
        if include_stock_benchmarks:
            required["0050"].add(session_date)
            required["2330"].add(session_date)
        for mode_positions in (positions.get(session_date) or {}).values():
            for position in mode_positions:
                required[str(position["symbol"])].add(session_date)
    return required


def verified_manual_no_trade_pairs(
    state_dir: Path,
) -> dict[tuple[str, str], str]:
    """Load only hash-verified user-authorized no-print settlement evidence.

    These receipts do not manufacture a same-day price. They prove that the
    accepted paper endpoint intentionally used the last earlier official trade
    because the completed session had no daily print. Interior minute marks may
    therefore retain the prior observed trade and remain explicitly stale.
    """

    output: dict[tuple[str, str], str] = {}
    receipt_root = state_dir / "settlement_receipts"
    for path in sorted(receipt_root.glob("*.json")):
        try:
            payload = _read_json(path)
            session_date = str(payload.get("session_date") or "")
            raw_root = Path(str(payload.get("raw_root") or "")).resolve(strict=True)
            if (
                payload.get("status") != "applied"
                or payload.get("simulation_only") is not True
                or payload.get("contract") != LAST_TRADE_SETTLEMENT_CONTRACT
                or not session_date
            ):
                continue
            verified_hashes: set[str] = set()
            current_day_source = False
            sources = payload.get("sources") or []
            if not isinstance(sources, list) or not sources:
                continue
            for source in sources:
                source_path = Path(str(source["path"])).resolve(strict=True)
                expected_hash = str(source["sha256"])
                if (
                    not source_path.is_relative_to(raw_root)
                    or _sha256(source_path) != expected_hash
                ):
                    raise ValueError("manual settlement source receipt mismatch")
                verified_hashes.add(expected_hash)
                current_day_source = current_day_source or (
                    source_path.name == f"{session_date}.json"
                    and source_path.parent.name
                    in {"twse_daily_ohlcv", "tpex_daily_ohlcv"}
                )
            if not current_day_source:
                continue
            declared = {
                str(symbol) for symbol in payload.get("last_traded_price_for", [])
            }
            for entry in payload.get("entries") or []:
                symbol = str(entry.get("symbol") or "")
                price_date = str(entry.get("price_date") or "")
                if (
                    symbol in declared
                    and price_date
                    and price_date < session_date
                    and str(entry.get("official_date") or "") == price_date
                    and str(entry.get("price_basis") or "")
                    == "last_available_trade"
                    and str(entry.get("source_sha256") or "") in verified_hashes
                ):
                    output[(symbol, session_date)] = (
                        f"manual_last_trade_settlement:{path.name}"
                    )
        except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError):
            # Optional evidence is fail-closed: a damaged receipt cannot exempt
            # the pair from normal minute-source requirements.
            continue
    return output


def _ticks_to_minute_frame(
    ticks: Any, *, symbol: str, session_date: str
) -> pl.DataFrame:
    from downloader.download_shioaji_tw_minute_kbars import ticks_to_minute_kbars
    # This valuation-only interface does not consume Amount or contract_unit.
    return ticks_to_minute_kbars(ticks, symbol=symbol, market="", session_date=session_date,
                                contract_unit=1).select("ts", "Close", "Volume", "date", "symbol")


def fetch_missing_kbars(
    store: MinutePriceStore,
    required: Mapping[str, set[str]],
    *,
    output_root: Path,
    simulation: bool,
    workers: int,
    requests_per_second: float,
    max_traffic_fraction: float,
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
        "missing_after_sample": [
            {"symbol": symbol, "session_date": session_date}
            for symbol, session_date in missing_after[:20]
        ],
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


def historical_minute_mark_has_source(row: Mapping[str, Any]) -> bool:
    """Return whether an interior strategy mark has auditable minute pricing.

    Cardinality alone is insufficient: a live writer can emit every wall-clock
    minute while repeatedly carrying an unproved snapshot.  Completed-session
    replay rows therefore need the retained one-minute contract and explicit
    price-quality fields.  The accepted 09:01 entry and 13:30 endpoint are
    checked separately and intentionally do not use this predicate.
    """

    try:
        coverage = float(row.get("fresh_trade_notional_coverage_ratio"))
        fresh_positions = int(row.get("fresh_trade_position_count"))
        carried_positions = int(row.get("last_trade_carried_position_count"))
        missing_positions = int(row.get("missing_price_position_count"))
    except (TypeError, ValueError):
        return False
    return bool(
        row.get("historical_minute_replay") is True
        and str(row.get("minute_valuation_contract") or "") == MINUTE_CONTRACT
        and str(row.get("valuation_source") or "")
        and math.isfinite(coverage)
        and 0.0 <= coverage <= 1.0
        and fresh_positions >= 0
        and carried_positions >= 0
        and missing_positions == 0
        and row.get("valuation_executable") is False
    )


def rebuild_strategy_marks(
    source_rows: list[dict[str, Any]],
    positions: Mapping[str, Mapping[str, list[dict[str, Any]]]],
    store: MinutePriceStore,
    *,
    start: date,
    end: date,
    fill_rows: Sequence[Mapping[str, Any]] = (),
    repair_unverified_existing: bool = False,
    revalue_opening_marks: bool = False,
    recompute_existing_marks: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fill exact minute holes without replacing observed strategy marks.

    Existing audited replay marks are authoritative observations of the
    bracket/EOD-aware paper engine. Missing minutes, and optionally existing
    interior minutes without auditable price provenance, are reconstructed
    from the retained one-minute trade tape and append-only exit-fill ledger.
    The fill ledger preserves real stop, take-profit, and partial-exit state;
    Entry/exit fills and the accepted 13:30 endpoint remain untouched.
    Explicit opening repair values 09:01 at its completed source bar Close,
    using the same net-liquidation accounting as every following minute.
    """

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
    by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    duplicate_rows_removed = 0
    for raw in selected:
        row = dict(raw)
        session_date = str(row.get("session_date") or "")
        market = str(row.get("market") or "")
        minute = str(row.get("minute") or "")
        if not session_date or not market or not minute:
            raise RuntimeError(
                "strategy mark is missing session_date, market, or minute"
            )
        key = (session_date, market, minute)
        if key in by_key:
            duplicate_rows_removed += 1
        # Append-ledger order makes the latest observation inside a minute the
        # canonical live mark, including legacy rows without recorded_at.
        by_key[key] = row

    session_dates = sorted({key[0] for key in by_key})
    markets = sorted({key[1] for key in by_key})
    generated: list[dict[str, Any]] = []
    inserted_rows = 0
    replaced_unverified_rows = 0
    preserved_rows = 0
    carried_rows = 0
    fresh_ratios: list[float] = []
    opening_revaluations: list[dict[str, Any]] = []
    changed_equity_rows = 0
    maximum_equity_change_twd = 0.0
    fills_by_position: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(
        list
    )
    for raw_fill in fill_rows:
        if (
            not isinstance(raw_fill, Mapping)
            or str(raw_fill.get("purpose") or "") == "entry"
        ):
            continue
        session_date = str(raw_fill.get("session_date") or "")
        market = str(raw_fill.get("market") or "")
        position_id = str(raw_fill.get("position_id") or "")
        if not session_date or not market or not position_id:
            continue
        fills_by_position[(session_date, market, position_id)].append(dict(raw_fill))
    for rows in fills_by_position.values():
        rows.sort(
            key=lambda row: str(row.get("fill_at") or row.get("recorded_at") or "")
        )

    for session_date in session_dates:
        day = date.fromisoformat(session_date)
        for market in markets:
            opening_minute = datetime.combine(
                day, STRATEGY_ENTRY, tzinfo=TAIPEI
            ).isoformat(timespec="minutes")
            closing_minute = datetime.combine(
                day, SESSION_CLOSE, tzinfo=TAIPEI
            ).isoformat(timespec="minutes")
            opening = by_key.get((session_date, market, opening_minute))
            closing = by_key.get((session_date, market, closing_minute))
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
                    position.get("entry_price")
                    or position.get("sizing_open_price")
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
                    # The strategy has no position before its 09:01 paper entry.
                    # Do not fabricate a pre-entry equity point from the later fill.
                    continue
                minute_key = minute.isoformat(timespec="minutes")
                existing = by_key.get((session_date, market, minute_key))
                fresh_symbols: set[str] = set()
                for symbol, prices in minute_prices.items():
                    value = prices.get(minute_key)
                    if value is not None:
                        current_prices[symbol] = value
                        fresh_symbols.add(symbol)
                preserve_existing = bool(
                    existing is not None
                    and (
                        clock == "13:30"
                        or (clock == "09:01" and not revalue_opening_marks)
                        or (clock != "09:01" and not recompute_existing_marks and (
                            not repair_unverified_existing
                            or historical_minute_mark_has_source(existing)
                        ))
                    )
                )
                if preserve_existing:
                    generated.append(dict(existing))
                    preserved_rows += 1
                    continue
                if clock == "09:01" and revalue_opening_marks:
                    missing_open = set(minute_prices) - fresh_symbols
                    if missing_open:
                        raise RuntimeError(
                            f"missing completed 09:01 valuation bar for "
                            f"{session_date}:{market}:{sorted(missing_open)}"
                        )
                if existing is not None:
                    replaced_unverified_rows += 1
                open_net = 0.0
                total_notional = 0.0
                fresh_notional = 0.0
                realized_this_session = 0.0
                open_positions = 0
                fresh_open_positions = 0
                for position in mode_positions:
                    symbol = str(position["symbol"])
                    quantity = _filled_quantity(position)
                    position_id = str(position.get("position_id") or symbol)
                    exited_quantity = 0
                    allocated_entry_fee = 0.0
                    for fill in fills_by_position.get(
                        (session_date, market, position_id), ()
                    ):
                        fill_at = str(
                            fill.get("fill_at") or fill.get("recorded_at") or ""
                        )
                        if not fill_at or fill_at > minute.isoformat(
                            timespec="seconds"
                        ):
                            break
                        exited_quantity += int(fill.get("quantity") or 0)
                        allocated_entry_fee += float(
                            fill.get("entry_fee_allocated_twd") or 0.0
                        )
                        realized_this_session += float(fill.get("net_pnl_twd") or 0.0)
                    remaining = quantity - exited_quantity
                    if remaining < 0:
                        raise RuntimeError(
                            "exit quantity exceeds entry for "
                            f"{session_date}:{market}:{position_id}"
                        )
                    if remaining == 0:
                        continue
                    open_positions += 1
                    signed = (
                        remaining if str(position.get("side")) == "long" else -remaining
                    )
                    price = current_prices[symbol]
                    remaining_entry_fee = max(
                        0.0,
                        float(position.get("entry_fee_twd") or 0.0)
                        - allocated_entry_fee,
                    )
                    open_net += position_net_liquidation_pnl(
                        position,
                        price,
                        signed_shares=signed,
                        remaining_entry_fee_twd=remaining_entry_fee,
                    )
                    notional = abs(remaining * price)
                    total_notional += notional
                    if symbol in fresh_symbols:
                        fresh_notional += notional
                        fresh_open_positions += 1
                carried = max(0, open_positions - fresh_open_positions)
                coverage = (
                    fresh_notional / total_notional if total_notional > 0.0 else 1.0
                )
                carried_rows += int(carried > 0)
                fresh_ratios.append(coverage)
                cumulative = (
                    float(opening.get("cumulative_realized_net_pnl_twd") or 0.0)
                    + realized_this_session
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
                        "open_position_count": open_positions,
                        "stale_position_count": carried,
                        "valuation_stale": carried > 0,
                        "fresh_trade_position_count": fresh_open_positions,
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
                if clock == "09:01" and revalue_opening_marks:
                    opening_revaluations.append({
                        "session_date": session_date,
                        "market": market,
                        "before_equity_twd": float(opening["total_equity_twd"]),
                        "after_equity_twd": initial_capital + cumulative + open_net,
                    })
                if existing is not None:
                    delta = abs(float(existing.get("total_equity_twd") or 0.0)
                                - generated[-1]["total_equity_twd"])
                    changed_equity_rows += int(delta > 0.000001)
                    maximum_equity_change_twd = max(maximum_equity_change_twd, delta)
                inserted_rows += int(existing is None)
    rows = outside + generated
    rows.sort(
        key=lambda row: (str(row.get("minute") or ""), str(row.get("market") or ""))
    )
    expected = len(session_dates) * len(markets) * 270
    if len(generated) != expected:
        raise RuntimeError(
            f"strategy minute cardinality mismatch: {len(generated)} != {expected}"
        )
    return rows, {
        "session_dates": session_dates,
        "markets": markets,
        "generated_rows": len(generated),
        "preserved_observed_rows": preserved_rows,
        "inserted_missing_rows": inserted_rows,
        "replaced_unverified_rows": replaced_unverified_rows,
        "duplicate_rows_removed": duplicate_rows_removed,
        "opening_revaluations": opening_revaluations,
        "changed_equity_rows": changed_equity_rows,
        "maximum_equity_change_twd": maximum_equity_change_twd,
        "rows_with_carried_prices": carried_rows,
        "minimum_fresh_trade_notional_coverage_ratio": min(fresh_ratios, default=1.0),
        "mean_fresh_trade_notional_coverage_ratio": (
            sum(fresh_ratios) / len(fresh_ratios) if fresh_ratios else 1.0
        ),
    }


def _is_terminal_carry_cost(cost: Mapping[str, Any]) -> bool:
    """Return whether a carry-ledger row becomes effective at 13:30."""

    # Offline same-day close reconciliation appends an immutable reversal of
    # the 13:30 conversion debit.  Both sides belong to the same accounting
    # instant; applying only the reversal at 09:01 corrupts that endpoint.
    return (
        cost.get("kind") == "short_conversion_tax_and_handling"
        or bool(cost.get("reverses_cost_id"))
    )


def rebuild_carried_strategy_marks(source_rows, positions, store, *, state, fill_rows, start, end, preserve_sourced=True):
    """Revalue an immutable carried fill/action book, never re-execute trades.

    Unlike the flat-session path, inventory, cost basis, unallocated entry
    fees, cash claims and financing charges all survive the session boundary.
    Accepted endpoint NAV is preserved and its accounting is independently
    checked. Interior prices come only from retained completed-minute trades.
    """
    selected = {(r["market"], r["minute"]): r for r in source_rows
                if _in_range(r["session_date"], start, end)}
    days = sorted({r["session_date"] for r in selected.values()})
    output = [r for r in source_rows if not _in_range(r["session_date"], start, end)]
    for market, mode in state["modes"].items():
        metadata = {p["position_id"]: p for modes in positions.values() for p in modes.get(market, [])}
        fills = [f for f in fill_rows if f["market"] == market]
        if any(f["session_date"] < days[0] for f in fills):
            raise RuntimeError("carried minute reconstruction requires the full accepted ledger horizon")
        events = sorted([*(f | {"clock": str(f.get("fill_at") or f["recorded_at"])} for f in fills),
                         *(a | {"clock": a["recorded_at"], "share_action": True}
                           for a in mode.get("share_replacement_ledger", []))], key=lambda e: e["clock"])
        book, prices, price_times, seen_entries = {}, {}, {}, set()
        cursor, realized = 0, 0.
        for day in days:
            symbols = {p["symbol"] for p in positions.get(day, {}).get(market, [])}
            source = {symbol: store.prices(symbol, day) for symbol in symbols}
            claims = [c for c in mode.get("corporate_action_ledger", []) if c["ex_date"] <= day]
            earned = sum(float(c["amount_twd"]) for c in claims)
            paid = sum(float(c["amount_twd"]) for c in claims if c["payment_date"] <= day)
            costs = mode.get("carry_cost_ledger", [])
            charges = sum(float(c["amount_twd"]) for c in costs if c["date"] < day or
                          (c["date"] == day and not _is_terminal_carry_cost(c)))
            terminal_charges = sum(float(c["amount_twd"]) for c in costs
                                   if c["date"] == day and _is_terminal_carry_cost(c))
            for minute in _session_minutes(date.fromisoformat(day))[1:]:
                key, clock = minute.isoformat(timespec="minutes"), minute.isoformat(timespec="seconds")
                existing = selected.get((market, key))
                cutoff = clock
                if existing and key[11:16] in {"09:01", "13:30"}:
                    cutoff = max(clock, str(existing.get("recorded_at") or clock))
                while cursor < len(events) and events[cursor]["clock"] <= cutoff:
                    event = events[cursor]
                    identity = event["position_id"]
                    if event.get("share_action"):
                        p = book[identity]
                        if p["signed_shares"] != event["old_signed_shares"]:
                            raise RuntimeError("minute share replacement disagrees with retained inventory")
                        p["signed_shares"] = int(event["new_signed_shares"])
                        p["inventory_basis_price"] = float(event["new_entry_price"])
                        symbol = p["symbol"]
                        if price_times.get(symbol, "") < day:
                            prices[symbol] = (prices[symbol] - event["cash_per_old_share"]) / event["ratio"]
                            price_times[symbol] = event["clock"]
                    elif event["purpose"] == "entry":
                        if identity in seen_entries or identity not in metadata:
                            raise RuntimeError("minute entry identity is duplicate or has no accepted position")
                        seen_entries.add(identity)
                        p = dict(metadata[identity])
                        sign = 1 if p["side"] == "long" else -1
                        p.update(signed_shares=sign * int(event["quantity"]), inventory_basis_price=float(event["price"]),
                                 remaining_entry_fee_twd=float(event["fee_and_tax_twd"]))
                        book[identity] = p
                        symbol = p["symbol"]
                        if price_times.get(symbol, "") < event["clock"]:
                            prices[symbol], price_times[symbol] = float(event["price"]), event["clock"]
                    else:
                        p = book[identity]
                        quantity = int(event["quantity"])
                        if quantity > abs(p["signed_shares"]):
                            raise RuntimeError("minute exit exceeds retained carried inventory")
                        p["signed_shares"] -= quantity * (1 if p["signed_shares"] > 0 else -1)
                        p["remaining_entry_fee_twd"] -= float(event["entry_fee_allocated_twd"])
                        realized += float(event["net_pnl_twd"])
                        if not p["signed_shares"]:
                            del book[identity]
                    cursor += 1
                fresh = set()
                for symbol, values in source.items():
                    if key in values:
                        prices[symbol], price_times[symbol] = float(values[key]), clock
                        fresh.add(symbol)
                count, fresh_count, net, notional, fresh_notional = 0, 0, 0., 0., 0.
                for p in book.values():
                    if not p["signed_shares"]:
                        continue
                    symbol = p["symbol"]
                    if symbol not in prices:
                        raise RuntimeError("carried valuation has no prior observed price")
                    valuation = dict(p)
                    if str(p.get("margin_converted_at") or "9999") > clock:
                        valuation.pop("margin_carry_contract", None)
                    net += position_net_liquidation_pnl(valuation, prices[symbol])
                    count += 1
                    amount = abs(p["signed_shares"] * prices[symbol])
                    notional += amount
                    if symbol in fresh:
                        fresh_count += 1
                        fresh_notional += amount
                cost = charges + (terminal_charges if key[11:16] == "13:30" else 0.)
                initial = float(mode["initial_capital_twd"])
                row = {"recorded_at": clock, "minute": key, "session_date": day, "market": market,
                       "initial_capital_twd": initial, "cumulative_realized_net_pnl_twd": realized,
                       "open_net_liquidation_pnl_twd": net, "total_equity_twd": initial + realized + net + earned - cost,
                       "cumulative_carry_cost_twd": cost, "cumulative_corporate_action_net_twd": earned,
                       "corporate_action_cash_net_twd": paid,
                       "corporate_action_receivable_twd": sum(max(0., float(c["amount_twd"])) for c in claims if c["payment_date"] > day),
                       "corporate_action_payable_twd": sum(max(0., -float(c["amount_twd"])) for c in claims if c["payment_date"] > day),
                       "margin_carry_contract": mode["margin_carry_contract"],
                       "odd_lot_execution_policy": mode.get("odd_lot_execution_policy"),
                       "open_position_count": count, "stale_position_count": count - fresh_count,
                       "valuation_stale": count > fresh_count, "fresh_trade_position_count": fresh_count,
                       "last_trade_carried_position_count": count - fresh_count, "missing_price_position_count": 0,
                       "fresh_trade_notional_coverage_ratio": fresh_notional / notional if notional else 1.,
                       "historical_minute_replay": True, "minute_valuation_contract": MINUTE_CONTRACT,
                       "valuation_source": "retained_1m_close_from_immutable_carried_fill_and_action_book",
                       "valuation_executable": False, "simulation_only": True}
                if key[11:16] in {"09:01", "13:30"}:
                    if existing is None:
                        raise RuntimeError("carried reconstruction cannot manufacture accepted endpoints")
                    for field in ("cumulative_realized_net_pnl_twd", "open_position_count", "cumulative_carry_cost_twd", "cumulative_corporate_action_net_twd"):
                        if not math.isclose(float(existing.get(field) or 0), float(row[field]), rel_tol=1e-12, abs_tol=1e-6):
                            raise RuntimeError(f"carried endpoint accounting disagrees: {market}:{key}:{field}")
                    row = dict(existing)
                    row["accepted_endpoint_accounting_verified"] = True
                elif preserve_sourced and existing and historical_minute_mark_has_source(existing):
                    row = dict(existing)
                output.append(row)
    output.sort(key=lambda r: (r["minute"], r["market"]))
    _, stats = validate_existing_strategy_marks(output, start=start, end=end)
    stats["carried_fill_book_revalued_without_execution"] = True
    differences = [{"market": r["market"], "minute": r["minute"],
                    "original_equity_twd": float(selected[(r["market"], r["minute"])]["total_equity_twd"]),
                    "revalued_equity_twd": float(r["total_equity_twd"]),
                    "difference_twd": abs(float(r["total_equity_twd"]) - float(selected[(r["market"], r["minute"])]["total_equity_twd"]))}
                   for r in output if (r["market"], r["minute"]) in selected]
    stats["maximum_original_equity_difference_twd"] = max((d["difference_twd"] for d in differences), default=0.)
    changed = [d for d in differences if d["difference_twd"] > 1e-6]
    stats["differing_original_equity_points"] = len(changed)
    stats["equity_difference_counts_by_market_date"] = dict(Counter(
        f"{row['market']}:{row['minute'][:10]}" for row in changed))
    stats["equity_difference_samples"] = sorted(changed, key=lambda d: d["difference_twd"], reverse=True)[:20]
    return output, stats


def validate_existing_strategy_marks(
    source_rows: list[dict[str, Any]],
    *,
    start: date,
    end: date,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Validate bracket-aware replay marks without recomputing their PnL path."""

    selected = [
        row
        for row in source_rows
        if _in_range(str(row.get("session_date") or ""), start, end)
    ]
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in selected:
        session_date = str(row.get("session_date") or "")
        market = str(row.get("market") or "")
        if not session_date or not market:
            raise RuntimeError(
                "existing strategy mark is missing session_date or market"
            )
        grouped[(session_date, market)].append(row)
    session_dates = sorted({key[0] for key in grouped})
    markets = sorted({key[1] for key in grouped})
    if not session_dates or not markets:
        raise RuntimeError("no existing strategy minute marks in requested range")

    carried_rows = 0
    coverage_values: list[float] = []
    for session_date in session_dates:
        base = datetime.fromisoformat(f"{session_date}T09:01:00+08:00")
        expected_minutes = [
            (base + timedelta(minutes=offset)).isoformat(timespec="minutes")
            for offset in range(270)
        ]
        for market in markets:
            rows = grouped.get((session_date, market), [])
            actual_minutes = [str(row.get("minute") or "") for row in rows]
            if len(rows) != 270 or Counter(actual_minutes) != Counter(expected_minutes):
                raise RuntimeError(
                    f"existing strategy minute cardinality mismatch for "
                    f"{session_date}:{market}; expected exactly 09:01..13:30"
                )
            for row in rows:
                if row.get("margin_carry_contract"):
                    endpoint = str(row.get("minute", ""))[11:16] in {"09:01", "13:30"}
                    if not historical_minute_mark_has_source(row) and not (endpoint and row.get("accepted_endpoint_accounting_verified") is True):
                        raise RuntimeError(f"unsourced carried-account minute: {session_date}:{market}")
                    expected_equity = (float(row["initial_capital_twd"])
                        + float(row["cumulative_realized_net_pnl_twd"])
                        + float(row["open_net_liquidation_pnl_twd"])
                        - float(row.get("cumulative_carry_cost_twd") or 0)
                        + float(row.get("cumulative_corporate_action_net_twd") or 0))
                    if not math.isclose(expected_equity, float(row["total_equity_twd"]), rel_tol=1e-12, abs_tol=1e-6):
                        raise RuntimeError(f"carried-account NAV mismatch: {session_date}:{market}")
                stale = (
                    bool(row.get("valuation_stale"))
                    or int(row.get("stale_position_count") or 0) > 0
                )
                carried_rows += int(stale)
                value = row.get("fresh_trade_notional_coverage_ratio")
                try:
                    parsed = float(value)
                except (TypeError, ValueError):
                    parsed = math.nan
                if math.isfinite(parsed):
                    coverage_values.append(parsed)

    expected_rows = len(session_dates) * len(markets) * 270
    if len(selected) != expected_rows:
        raise RuntimeError(
            f"existing strategy minute rows include unexpected scope: "
            f"{len(selected)} != {expected_rows}"
        )
    return list(source_rows), {
        "session_dates": session_dates,
        "markets": markets,
        "generated_rows": len(selected),
        "rows_with_carried_prices": carried_rows,
        "minimum_fresh_trade_notional_coverage_ratio": (
            min(coverage_values) if coverage_values else None
        ),
        "mean_fresh_trade_notional_coverage_ratio": (
            sum(coverage_values) / len(coverage_values) if coverage_values else None
        ),
        "existing_bracket_aware_marks_preserved": True,
    }


def recover_terminal_marks(
    state_dir: Path, source_rows: list[dict[str, Any]], *, start: date, end: date,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Recover only flat 13:30 NAV from accepted fills, never from prices.

    Two intraday realized-PnL anchors, complete position lifecycles and the
    settlement event must agree. A later actual fill cannot be backdated.
    No orders, fills, source prices or strategy decisions are regenerated.
    """
    state = _read_json(state_dir / "state.json")
    selected = [r for r in source_rows if _in_range(str(r.get("session_date") or ""), start, end)]
    missing = missing_accepted_endpoints(selected, start=start, end=end,
                                         expected_markets=state["modes"])
    if any(r["missing_clocks"] != ["13:30"] for r in missing):
        raise RuntimeError("terminal recovery cannot synthesize an opening mark")
    fills = _read_jsonl(state_dir / "fills.jsonl")
    events = _read_jsonl(state_dir / "events.jsonl")
    recovered = []
    now = datetime.now(TAIPEI)

    def timestamp(value):
        parsed = datetime.fromisoformat(str(value))
        if parsed.tzinfo is None:
            raise RuntimeError("terminal evidence requires timezone-aware timestamps")
        return parsed.astimezone(TAIPEI)

    def number(value):
        result = float(value)
        if not math.isfinite(result):
            raise RuntimeError("nonfinite terminal accounting evidence")
        return result

    def close(a, b):
        if not math.isclose(number(a), number(b), rel_tol=0, abs_tol=1e-6):
            raise RuntimeError(f"terminal accounting evidence disagrees: {a} != {b}")

    for gap in missing:
        day, market = gap["session_date"], gap["market"]
        endpoint = datetime.fromisoformat(f"{day}T13:30:00+08:00")
        deadline = endpoint + timedelta(minutes=1)
        if now < deadline:
            raise RuntimeError("terminal recovery is only for completed sessions")
        valid = sorted((r for r in selected if r.get("session_date") == day and r.get("market") == market
                        and f"{day}T09:01+08:00" <= str(r.get("minute")) < f"{day}T13:30+08:00"),
                       key=lambda r: r["minute"])
        if len(valid) < 2:
            raise RuntimeError(f"missing independent realized-PnL anchors: {day}:{market}")
        first, anchor = valid[0], valid[-1]
        first_at, anchor_at = timestamp(first["recorded_at"]), timestamp(anchor["recorded_at"])
        if first_at.date().isoformat() != day or not first_at < anchor_at < endpoint:
            raise RuntimeError("invalid pre-close anchor timestamps")
        settled = [e for e in events if e.get("event") == "closing_auction_settled" and e.get("market") == market
                   and endpoint <= timestamp(e["recorded_at"]) < deadline]
        if not settled:
            raise RuntimeError(f"missing same-session closing settlement: {day}:{market}")
        current = state["modes"][market]
        if current.get("session_date") == day:
            positions = list(current["positions"].values())
            position_source = "state.json"
        else:
            position_source = f"position_history/{day}/{market}.json"
            archive = _read_json(state_dir / position_source)
            if archive.get("session_date") != day or archive.get("market") != market:
                raise RuntimeError("position archive identity mismatch")
            positions = archive["positions"]
        if not positions or any(int(p["signed_shares"]) for p in positions):
            raise RuntimeError("terminal recovery requires proven flat positions")
        session_fills = [f for f in fills if f.get("session_date") == day and f.get("market") == market]
        if not session_fills:
            raise RuntimeError("terminal recovery requires accepted fills")
        by_position = defaultdict(list)
        for fill in session_fills:
            recorded = timestamp(fill["recorded_at"])
            if recorded.date().isoformat() != day or recorded >= deadline or timestamp(fill["fill_at"]) >= deadline:
                raise RuntimeError("a later fill cannot be backdated to 13:30")
            by_position[fill["position_id"]].append(fill)
        if set(by_position) != {p["position_id"] for p in positions}:
            raise RuntimeError("fill and position identities disagree")
        for position in positions:
            pf = by_position[position["position_id"]]
            entries = sum(int(f["quantity"]) for f in pf if f["purpose"] == "entry")
            exits = sum(int(f["quantity"]) for f in pf if f["purpose"] != "entry")
            if entries <= 0 or entries != exits or entries != int(position["filled_shares"]):
                raise RuntimeError("incomplete or duplicated position fill lifecycle")
            close(math.fsum(number(f["net_pnl_twd"]) for f in pf if f["purpose"] != "entry"),
                  position["realized_net_pnl_twd"])
        exit_fills = [f for f in session_fills if f["purpose"] != "entry"]
        realized_between = math.fsum(number(f["net_pnl_twd"]) for f in exit_fills
                                     if first_at < timestamp(f["recorded_at"]) <= anchor_at)
        close(number(first["cumulative_realized_net_pnl_twd"]) + realized_between,
              anchor["cumulative_realized_net_pnl_twd"])
        for mark, at in ((first, first_at), (anchor, anchor_at)):
            remaining = defaultdict(int)
            for f in session_fills:
                if timestamp(f["recorded_at"]) <= at:
                    remaining[f["position_id"]] += int(f["quantity"]) * (1 if f["purpose"] == "entry" else -1)
            if any(q < 0 for q in remaining.values()) or sum(q > 0 for q in remaining.values()) != int(mark["open_position_count"]):
                raise RuntimeError("anchor position count disagrees with accepted fills")
        cumulative = number(anchor["cumulative_realized_net_pnl_twd"]) + math.fsum(
            number(f["net_pnl_twd"]) for f in exit_fills if timestamp(f["recorded_at"]) > anchor_at)
        initial = number(anchor["initial_capital_twd"])
        close(first["initial_capital_twd"], initial)
        equity = initial + cumulative
        if current.get("session_date") == day:
            close(current["total_equity_twd"], equity)
        recovered.append({
            "session_date": day, "market": market, "minute": endpoint.isoformat(timespec="minutes"),
            "recorded_at": now.isoformat(timespec="seconds"), "initial_capital_twd": initial,
            "cumulative_realized_net_pnl_twd": cumulative, "open_net_liquidation_pnl_twd": 0.0,
            "total_equity_twd": equity, "open_position_count": 0, "stale_position_count": 0,
            "valuation_stale": False, "valuation_source": "accepted_closed_fill_ledger",
            "terminal_recovery": {"contract": "flat_settlement_nav_v1", "position_source": position_source,
                                  "settled_at": settled[-1]["recorded_at"], "anchor_at": anchor["recorded_at"]},
        })
    # The old engine also emitted prior-session flat NAV at the next day's
    # 09:01. Remove only that proven duplicate, retaining a full before-image.
    endpoints = {(r["session_date"], r["market"]): r for r in [*selected, *recovered]
                 if r.get("minute") == f"{r['session_date']}T13:30+08:00"}
    rows, removed = [], []
    for row in source_rows:
        day = str(row.get("session_date") or "")
        if _in_range(day, start, end) and not str(row.get("minute", "")).startswith(day + "T"):
            endpoint_row = endpoints.get((day, row["market"]))
            if endpoint_row is None or int(row["open_position_count"]) or number(row["open_net_liquidation_pnl_twd"]) != 0:
                raise RuntimeError("unproven cross-session mark cannot be removed")
            close(row["total_equity_twd"], endpoint_row["total_equity_twd"])
            removed.append(row)
        else:
            rows.append(row)
    rows.extend(recovered)
    rows.sort(key=lambda r: (str(r.get("minute")), str(r.get("market"))))
    _, coverage = validate_existing_strategy_marks(rows, start=start, end=end)
    return rows, {"recovered_endpoints": recovered, "removed_cross_session_duplicates": removed, "coverage": coverage}


def repair_terminal_marks_only(args, *, start: date, end: date) -> None:
    """Stage and validate first; publish one ledger atomically under engine lock."""
    if args.output_dir.resolve().is_relative_to(args.state_dir.resolve()):
        raise ValueError("terminal candidate must be outside the live ledger directory")
    lock_path = args.state_dir / ".engine.lock"
    with (lock_path.open("a+") if args.publish else nullcontext()) as lock:
        if lock is not None:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        sources = ["state.json", "fills.jsonl", "orders.jsonl", "signals.jsonl", "events.jsonl", "marks.jsonl"]
        before = {name: _sha256(args.state_dir / name) for name in sources}
        source = _read_jsonl(args.state_dir / "marks.jsonl")
        rows, result = recover_terminal_marks(args.state_dir, source, start=start, end=end)
        args.output_dir.mkdir(parents=True, exist_ok=True)
        _atomic_text(args.output_dir / "marks.before.jsonl", (args.state_dir / "marks.jsonl").read_text())
        _atomic_jsonl(args.output_dir / "marks.jsonl", rows)
        if before != {name: _sha256(args.state_dir / name) for name in sources}:
            raise RuntimeError("ledger changed during terminal recovery; retry from a stable revision")
        result.update({"contract": "terminal_nav_only_no_replay_v1", "source_sha256": before,
                       "candidate_sha256": _sha256(args.output_dir / "marks.jsonl"), "published": bool(args.publish)})
        _atomic_json(args.output_dir / "terminal_curve_recovery_receipt.json", result)
        if args.publish:
            _atomic_text(args.state_dir / "marks.jsonl", (args.output_dir / "marks.jsonl").read_text())
            _atomic_json(args.state_dir / "terminal_curve_recovery_receipt.json", result)
        print(json.dumps({"published": bool(args.publish), "recovered_endpoints": len(result["recovered_endpoints"]),
                          "removed_cross_session_duplicates": len(result["removed_cross_session_duplicates"]),
                          "coverage": result["coverage"]}, ensure_ascii=False))


def _benchmark_minute_row(
    template: Mapping[str, Any],
    *,
    minute: datetime,
    price: float,
    fresh: bool,
) -> dict[str, Any]:
    row = dict(template)
    quantity = float(row.get("adjusted_quantity") or row.get("quantity") or 0.0)
    original_quantity = float(row.get("quantity") or 0.0)
    entry_price = float(row["entry_price"])
    initial_capital = float(row["initial_capital_twd"])
    initial_fees = float(
        row.get("estimated_entry_cost_twd")
        or row.get("estimated_initial_fixed_fees_twd")
        or row.get("initial_fixed_fees_twd")
        or 0.0
    )
    template_price = float(template["last_mark_price"])
    template_cost = float(
        template.get("estimated_liquidation_cost_twd")
        or template.get("liquidation_cost_twd")
        or 0.0
    )
    liquidation_rate = (
        template_cost / (quantity * template_price) if quantity > 0.0 else 0.0
    )
    liquidation_cost = quantity * price * liquidation_rate
    gross_value = quantity * price
    gross_pnl = gross_value - original_quantity * entry_price
    total_equity = initial_capital + gross_pnl
    daily_return_fraction, daily_return_pct = previous_close_return(
        price,
        row.get("daily_return_reference_price"),
    )
    row.update(
        {
            "recorded_at": minute.isoformat(timespec="seconds"),
            "minute": minute.isoformat(timespec="minutes"),
            "last_mark_at": minute.isoformat(timespec="seconds"),
            "last_quote_at": minute.isoformat(timespec="seconds"),
            "last_mark_price": price,
            "liquidation_cost_twd": 0.0,
            "estimated_entry_cost_twd": initial_fees,
            "estimated_liquidation_cost_twd": liquidation_cost,
            "estimated_tracking_cost_twd": initial_fees + liquidation_cost,
            "performance_pnl_twd": gross_pnl,
            "gross_pnl_twd": gross_pnl,
            "net_pnl_twd": gross_pnl,
            "total_equity_twd": total_equity,
            "return_fraction": gross_pnl / initial_capital,
            "return_pct": gross_pnl / initial_capital * 100.0,
            "buy_hold_wealth_index": total_equity / initial_capital,
            "daily_return_fraction": daily_return_fraction,
            "daily_return_pct": daily_return_pct,
            "source": "shioaji_historical_1m_close",
            "valuation_source": "gross_buy_hold_historical_last_trade_mark_not_executable_bid",
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
    parser.add_argument(
        "--validate-existing-strategy-marks",
        action="store_true",
        help=(
            "Validate and preserve an existing bracket/EOD-aware 270-point "
            "strategy curve instead of replacing it with endpoint-derived marks."
        ),
    )
    parser.add_argument(
        "--repair-unverified-strategy-marks",
        action="store_true",
        help=(
            "Replace completed-session interior marks that have no auditable "
            "one-minute price provenance; preserve the 09:01 and 13:30 endpoints."
        ),
    )
    parser.add_argument("--fetch-workers", type=int, default=1)
    parser.add_argument(
        "--revalue-opening-marks", action="store_true",
        help="Revalue 09:01 from the completed source bar Close; preserve all fills and 13:30.",
    )
    parser.add_argument("--recompute-existing-strategy-marks", action="store_true",
                        help="Reconcile all interior marks with entry/exit fills and source minute prices.")
    parser.add_argument("--revalue-carried-marks", action="store_true",
                        help="Revalue carried inventory from immutable fills/actions; preserve accepted endpoints and every execution.")
    parser.add_argument("--require-revaluation-parity", action="store_true",
                        help="Cross-check a new full replay against independent minute valuation; refuse publication on any discrepancy.")
    parser.add_argument("--requests-per-second", type=float, default=5.0)
    parser.add_argument(
        "--max-traffic-fraction",
        type=float,
        default=HISTORICAL_MAX_TRAFFIC_FRACTION,
    )
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--repair-terminal-only", action="store_true",
                        help="Recover missing flat 13:30 NAV from accepted fills only; no price fetch or trade replay.")
    return parser.parse_args()


def _carried_accounting_signature(state_dir: Path, end: date) -> str:
    state = _read_json(state_dir / "state.json")
    payload = {}
    for market, mode in state.get("modes", {}).items():
        payload[market] = {"initial_capital_twd": mode.get("initial_capital_twd")}
        for field, day_field in (("share_replacement_ledger", "effective_date"),
                                 ("corporate_action_ledger", "ex_date"), ("carry_cost_ledger", "date")):
            payload[market][field] = [row for row in mode.get(field, []) if str(row[day_field]) <= str(end)]
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def _assert_minute_publication_window(state_dir: Path, now: datetime) -> None:
    if not datetime_time(8, 30) <= now.time() < datetime_time(14, 31):
        return
    with (state_dir / ".engine.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("live engine is in its protected window; defer minute publication") from exc


def main() -> None:
    args = parse_args()
    args.revalue_carried_marks = bool(getattr(args, "revalue_carried_marks", False))
    args.require_revaluation_parity = bool(getattr(args, "require_revaluation_parity", False))
    if args.require_revaluation_parity and not (args.revalue_carried_marks and args.recompute_existing_strategy_marks):
        raise ValueError("parity verification requires full independent carried revaluation")
    if (args.revalue_opening_marks or args.recompute_existing_strategy_marks) and args.validate_existing_strategy_marks:
        raise ValueError("opening revaluation cannot be combined with preserve-only validation")
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if start > end:
        raise ValueError("start date must not be after end date")
    source_hashes = {name: _sha256(args.state_dir / name) for name in
                     ("marks.jsonl", "fills.jsonl", "orders.jsonl", "benchmark_history.json")
                     if (args.state_dir / name).exists()}
    source_marks = _read_jsonl(args.state_dir / "marks.jsonl")
    has_margin_carry = any(row.get("margin_carry_contract") for row in source_marks)
    accounting_signature = _carried_accounting_signature(args.state_dir, end) if args.revalue_carried_marks else None
    if args.revalue_carried_marks and (not has_margin_carry or args.validate_existing_strategy_marks
                                      or args.revalue_opening_marks):
        raise ValueError("carried revaluation requires its own explicit inventory contract")
    if has_margin_carry and (not (args.validate_existing_strategy_marks or args.revalue_carried_marks)
            or args.repair_terminal_only or args.repair_unverified_strategy_marks):
        raise RuntimeError(
            "margin-carry histories require the canonical stateful open-price replay; "
            "the flat-session minute rebuilder cannot reconstruct carried basis and interest"
        )
    if getattr(args, "repair_terminal_only", False):
        if args.fetch_missing_kbars or args.revalue_opening_marks or args.recompute_existing_strategy_marks or args.repair_unverified_strategy_marks:
            raise ValueError("terminal-only recovery cannot change prices or interior marks")
        repair_terminal_marks_only(args, start=start, end=end)
        return
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
    store = MinutePriceStore(local_roots, local_cache_roots, require_receipts=has_margin_carry)
    missing_endpoints = missing_accepted_endpoints(source_marks, start=start, end=end)
    if missing_endpoints:
        raise RuntimeError(f"missing accepted endpoints before minute-data preparation: {missing_endpoints[:20]}")
    source_fills = _read_jsonl(args.state_dir / "fills.jsonl")
    positions = load_positions(args.state_dir, start=start, end=end, fill_rows=source_fills)
    session_dates = sorted(
        {
            str(row.get("session_date"))
            for row in source_marks
            if _in_range(str(row.get("session_date") or ""), start, end)
        }
    )
    required = required_symbol_dates(
        positions,
        session_dates,
        include_stock_benchmarks=True,
    )
    official_no_trade_pairs = []
    if has_margin_carry:
        # A source-verified no-print session has no minute trades to download.
        # Preserve the marked stale holding; never invent a 270-bar price path.
        replay_path = args.state_dir / "rebuild_receipt.json"
        replay = _read_json(replay_path) if replay_path.exists() else {}
        for session in replay.get("sessions", ()):
            day = str(session.get("session_date") or "")
            evidence_sets = (
                (
                    "intraday_replay",
                    (session.get("intraday_replay") or {}).get(
                        "official_no_trade_carried_symbols", ()
                    ),
                ),
                (
                    "official_close",
                    (session.get("close") or {}).get(
                        "official_no_trade_carried_symbols", ()
                    ),
                ),
                (
                    "official_daily_absence",
                    (session.get("canonical_open") or {}).get(
                        "official_no_trade_print_symbols", ()
                    ),
                ),
            )
            for evidence, symbols in evidence_sets:
                for symbol in symbols:
                    if day in required.get(symbol, set()):
                        required[symbol].remove(day)
                        official_no_trade_pairs.append(
                            {
                                "symbol": symbol,
                                "session_date": day,
                                "evidence": evidence,
                            }
                        )
        for (symbol, day), evidence in verified_manual_no_trade_pairs(
            args.state_dir
        ).items():
            if day in required.get(symbol, set()):
                required[symbol].remove(day)
                official_no_trade_pairs.append(
                    {
                        "symbol": symbol,
                        "session_date": day,
                        "evidence": evidence,
                    }
                )
        from stockagent.live.tw_share_replacement import halted_symbols
        action_roots = {Path(m["margin_corporate_action_reference_path"]).parent
                        for m in _read_json(args.state_dir / "state.json")["modes"].values()
                        if m.get("margin_corporate_action_reference_path")}
        for day in session_dates:
            halted = set().union(*(halted_symbols(root, date.fromisoformat(day)) for root in action_roots))
            for symbol in halted:
                if day in required.get(symbol, set()):
                    required[symbol].remove(day)
                    official_no_trade_pairs.append(
                        {
                            "symbol": symbol,
                            "session_date": day,
                            "evidence": "official_halt",
                        }
                    )
        required = {symbol: days for symbol, days in required.items() if days}
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

    if args.revalue_carried_marks:
        rebuilt_marks, strategy_stats = rebuild_carried_strategy_marks(
            source_marks, positions, store, state=_read_json(args.state_dir / "state.json"),
            fill_rows=source_fills, start=start, end=end,
            preserve_sourced=not args.recompute_existing_strategy_marks)
    elif args.validate_existing_strategy_marks:
        rebuilt_marks, strategy_stats = validate_existing_strategy_marks(
            source_marks, start=start, end=end
        )
    else:
        rebuilt_marks, strategy_stats = rebuild_strategy_marks(
            source_marks,
            positions,
            store,
            start=start,
            end=end,
            fill_rows=source_fills,
            repair_unverified_existing=bool(
                args.repair_unverified_strategy_marks
            ),
            revalue_opening_marks=bool(args.revalue_opening_marks),
            recompute_existing_marks=bool(args.recompute_existing_strategy_marks),
        )
    source_benchmarks = _read_json(args.state_dir / "benchmark_history.json")
    if args.require_revaluation_parity and strategy_stats["differing_original_equity_points"]:
        _atomic_json(args.output_dir / "minute_revaluation_parity_failure.json", strategy_stats)
        _atomic_jsonl(args.output_dir / "unpromotable_revalued_marks.jsonl", rebuilt_marks)
        raise RuntimeError("independent carried minute valuation differs from replay; publication refused")
    rebuilt_benchmarks, benchmark_stats = rebuild_benchmark_history(
        source_benchmarks, store, start=start, end=end
    )
    benchmark_stats["strategy_marks_preserved_independently"] = bool(
        args.validate_existing_strategy_marks
    )
    tick_manifest_path = args.output_dir / "tick_minute_manifest.json"
    tick_manifest = build_tick_minute_manifest(tick_root, tick_manifest_path)
    marks_path = args.output_dir / "marks.jsonl"
    benchmark_path = args.output_dir / "benchmark_history.json"
    _atomic_jsonl(marks_path, rebuilt_marks)
    _atomic_json(benchmark_path, rebuilt_benchmarks, compact=True)
    receipt = {
        "schema_version": 2,
        "created_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
        "simulation_only": True,
        "production_order_possible": False,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "minute_contract": MINUTE_CONTRACT,
        "linear_interpolation_used": False,
        "carried_inventory_marks_preserved": has_margin_carry and not args.revalue_carried_marks,
        "carried_inventory_revalued_from_unchanged_executions": args.revalue_carried_marks,
        "independent_carried_valuation_parity_required": args.require_revaluation_parity,
        "independent_carried_valuation_parity_passed": bool(
            args.require_revaluation_parity
            and strategy_stats["differing_original_equity_points"] == 0
        ),
        "official_no_trade_carried_pairs": official_no_trade_pairs,
        "accepted_09_01_strategy_and_13_30_endpoints_preserved": not args.revalue_opening_marks,
        "accepted_13_30_endpoints_preserved": True,
        "opening_marks_revalued_at_completed_minute": bool(args.revalue_opening_marks),
        "unchanged_fills_sha256": _sha256(args.state_dir / "fills.jsonl"),
        "source_ledger_hashes": source_hashes,
        "source_carried_accounting_signature": accounting_signature,
        "existing_bracket_aware_strategy_marks_preserved": bool(
            args.validate_existing_strategy_marks
        ),
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
    store.assert_sources_unchanged()
    _atomic_json(receipt_path, receipt)
    if args.publish:
        _assert_minute_publication_window(args.state_dir, datetime.now(TAIPEI))
        with minute_curve_write_lock(args.state_dir):
            store.assert_sources_unchanged()
            if source_hashes != {name: _sha256(args.state_dir / name) for name in source_hashes}:
                raise RuntimeError("accepted ledger changed during minute reconstruction; publication refused")
            if accounting_signature is not None and accounting_signature != _carried_accounting_signature(args.state_dir, end):
                raise RuntimeError("carried accounting changed during minute reconstruction; publication refused")
            _atomic_text(args.state_dir / "marks.jsonl", marks_path.read_text(encoding="utf-8"))
            _atomic_text(args.state_dir / "benchmark_history.json", benchmark_path.read_text(encoding="utf-8"))
            _atomic_text(args.state_dir / "minute_curve_receipt.json", receipt_path.read_text(encoding="utf-8"))
    print(json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

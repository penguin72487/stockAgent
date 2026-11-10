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
from contextlib import ExitStack, nullcontext
from collections import Counter, defaultdict
from copy import copy, deepcopy
from datetime import date, datetime, time as datetime_time, timedelta
import hashlib
import fcntl
from itertools import chain
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from typing import Any, Iterable, Iterator, Mapping, Sequence
import sys

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live.tw_day_trade_simulation import (
    TAIPEI,
    LEGACY_REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL,
    position_net_liquidation_pnl,
    minute_curve_write_lock,
)
from stockagent.live.benchmark_accounting import previous_close_return
from stockagent.live.benchmark_history_projection import write_benchmark_projection
from stockagent.live.shioaji_schedule import HISTORICAL_MAX_TRAFFIC_FRACTION
from stockagent.live.minute_source_paths import (
    DEFAULT_LOCAL_MINUTE_ROOTS,
    DEFAULT_LOCAL_MINUTE_CACHE_ROOTS,
)


SESSION_OPEN = datetime_time(9, 0)
STRATEGY_ENTRY = datetime_time(9, 1)
SESSION_CLOSE = datetime_time(13, 30)
MINUTE_CONTRACT = "right_labelled_historical_last_trade_mark_v1"
MINUTE_SESSION_POINTS = 270
OPENING_COST_VALUATION_CONTRACT = "user_authorized_missing_0901_entry_cost_valuation_v1"
OPENING_FIRST_BAR_VALUATION_CONTRACT = "user_authorized_missing_0901_later_first_bar_valuation_v1"


def opening_assumption_policy(first_bar: bool = False) -> dict[str, str]:
    kind = "first_bar" if first_bar else "cost"
    return {"contract": OPENING_FIRST_BAR_VALUATION_CONTRACT if first_bar else OPENING_COST_VALUATION_CONTRACT,
            "key": f"provisional_opening_{kind}_valuation",
            "readback": f"provisional_opening_{kind}_independent_readback",
            "flag": f"opening_marks_revalued_with_disclosed_{kind}_exceptions",
            "scope": f"provisional_opening_{kind}_valuation_targets"}
LAST_TRADE_SETTLEMENT_CONTRACT = (
    "user_authorized_last_traded_price_paper_settlement_v1"
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    from stockagent.live.paper_mode_snapshot import read_paper_state
    payload = read_paper_state(path)
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return payload


def minute_curve_scope_failures(
    receipt: Mapping[str, Any],
    *,
    completed_session_dates: list[str],
    expected_markets: set[str],
) -> list[str]:
    """Share exact scope checks between the writer and publication acceptance.

    This metadata gate is not a substitute for source, NAV, or output-hash
    validation. It prevents an internally complete but wrongly scoped curve
    from being written to the live ledger in the first place.
    """
    failures: list[str] = []
    strategy = receipt.get("strategy")
    strategy = strategy if isinstance(strategy, Mapping) else {}
    if sorted(str(value) for value in strategy.get("session_dates") or ()) != completed_session_dates:
        failures.append("minute curve session dates do not match completed replay sessions")
    actual_markets = [str(value) for value in strategy.get("markets") or ()]
    if set(actual_markets) != expected_markets or len(actual_markets) != len(expected_markets):
        failures.append("minute curve markets do not match the promoted mode set")
    expected_rows = len(completed_session_dates) * len(expected_markets) * MINUTE_SESSION_POINTS
    if type(strategy.get("generated_rows")) is not int or strategy["generated_rows"] != expected_rows:
        failures.append(f"minute curve generated row count does not equal {MINUTE_SESSION_POINTS} points per completed session and mode")
    if not completed_session_dates or str(receipt.get("start_date") or "") != completed_session_dates[0]:
        failures.append("minute curve start date does not match the replay start")
    if not completed_session_dates or str(receipt.get("end_date") or "") != completed_session_dates[-1]:
        failures.append("minute curve end date does not match the latest completed replay")
    return failures


def _expected_minute_scope(
    args: argparse.Namespace, *, start: date, end: date,
) -> tuple[list[str], set[str]] | None:
    sessions = getattr(args, "expected_session_date", None)
    markets = getattr(args, "expected_market", None)
    if sessions is None and markets is None:
        return None
    if not sessions or not markets:
        raise ValueError("expected session dates and markets must be supplied together")
    canonical_sessions = sorted({date.fromisoformat(value).isoformat() for value in sessions})
    if (len(canonical_sessions) != len(sessions)
            or set(canonical_sessions) != set(sessions)
            or canonical_sessions[0] != start.isoformat()
            or canonical_sessions[-1] != end.isoformat()):
        raise ValueError("expected session dates must be unique ISO dates spanning start/end")
    if (len(set(markets)) != len(markets)
            or any(not isinstance(market, str) or not market.strip() for market in markets)):
        raise ValueError("expected markets must be unique nonempty strings")
    return canonical_sessions, set(markets)


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
    from stockagent.live.paper_journal import journal_write_transaction
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        with journal_write_transaction(path, operation="replace"):
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


def _is_entry_inventory_fill(fill: Mapping[str, Any]) -> bool:
    """A disclosed paper completion adds inventory, not realized exit P&L."""

    return str(fill.get("purpose") or "") in {"entry", "entry_completion"}


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
        entry_rows: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
        for row in fill_rows:
            day = str(row.get("session_date") or "")
            if _is_entry_inventory_fill(row) and _in_range(day, start, end):
                entry_rows[(day, str(row.get("market")), str(row.get("position_id")))].append(row)
        entries: dict[tuple[str, str, str], dict[str, Any]] = {}
        for identity, rows in entry_rows.items():
            if not any(row.get("purpose") == "entry" for row in rows):
                raise RuntimeError(f"entry completion has no original entry: {identity}")
            quantity = sum(int(row["quantity"]) for row in rows)
            if quantity <= 0:
                raise RuntimeError(f"invalid entry quantity: {identity}")
            entries[identity] = {
                "session_date": identity[0],
                "quantity": quantity,
                "price": math.fsum(float(row["price"]) * int(row["quantity"]) for row in rows) / quantity,
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
        replay_source_hashes: Mapping[tuple[str, str], str] | None = None,
        replay_symbol_sources: Mapping[
            tuple[str, str], Iterable[str | Path]
        ] | None = None,
        finmind_root: Path | None = None,
        complete_opening_prices: bool = False,
    ) -> None:
        self._resolved_source_paths: dict[Path, str] = {}

        def unique_paths(value: Path | Sequence[Path]) -> tuple[Path, ...]:
            candidates = (
                [Path(value)]
                if isinstance(value, (str, Path))
                else [Path(root) for root in value]
            )
            unique: list[Path] = []
            seen: set[str] = set()
            for root in candidates:
                identity = self._source_path_identity(root)
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
        self._replay_source_hashes = {
            (str(day), str(Path(path).resolve())): str(digest)
            for (day, path), digest in (replay_source_hashes or {}).items()
        }
        self._replay_symbol_sources = {
            (str(day), str(symbol)): frozenset(
                str(Path(path).resolve()) for path in paths
            )
            for (day, symbol), paths in (replay_symbol_sources or {}).items()
        }
        self._replay_pair_pins_enforced = replay_symbol_sources is not None
        self._verified_chunk_dates: dict[Path, tuple[tuple, set[str]]] = {}
        self._batch_chunk_dates: dict[tuple[Path, str], frozenset[str]] | None = None
        self.local_read_metrics = {"full_chunk_receipt_validations": 0,
                                   "batch_metadata_reuses": 0, "chunk_files_read": 0}
        self._source_progress = None
        self._verified_raw_tick_signatures: dict[Path, tuple[int, int, int]] = {}
        self._verified_replay_source_signatures: dict[
            Path, tuple[int, int, int, str]
        ] = {}
        self.finmind_root = Path(finmind_root).resolve() if finmind_root is not None else None
        self._finmind_prices: dict[tuple[str, str], dict[str, float]] = {}
        self._finmind_loaded: dict[str, set[str]] = defaultdict(set)
        self.finmind_source_records: dict[str, dict[str, Any]] = {}
        self._verified_finmind_signatures: dict[Path, tuple[int, int, int]] = {}
        self.finmind_read_metrics = {"full_source_validations": 0, "validated_generation_reuses": 0,
                                     "subset_reads": 0, "opening_backup_pairs_prepared": 0}
        if complete_opening_prices and (not require_receipts or self._replay_pair_pins_enforced):
            raise ValueError("opening backup requires verified valuation sources, not an execution-pinned replay")
        self.complete_opening_prices = complete_opening_prices
        self.opening_backup_records: dict[tuple[str, str], dict[str, Any]] = {}
        self._first_observed_bar_cache: dict[tuple[str, str], dict[str, Any]] = {}

    def _source_path_identity(self, path: Path) -> str:
        """Pin aliases once per reconstruction; fence every alias at readback.

        Re-resolving the same WSL/Windows path for every stock/session is not
        additional source evidence. The source/receipt generation checks and
        final alias fence still reject replacements before acceptance.
        """
        identity = self._resolved_source_paths.get(path)
        if identity is None:
            identity = str(path.resolve())
            self._resolved_source_paths[path] = identity
        return identity

    @staticmethod
    def _source_signature(path: Path) -> tuple[int, int, int]:
        info = path.stat()
        return info.st_size, info.st_mtime_ns, info.st_ctime_ns

    def _report_source_progress(self):
        if self._source_progress is not None:
            self._source_progress("prepare_receipt_backed_prices",
                                  local_read_metrics=dict(self.local_read_metrics),
                                  finmind_read_metrics=dict(self.finmind_read_metrics))

    def first_observed_bar(self, symbol: str, session_date: str) -> dict[str, Any]:
        """Return an actual positive-volume bar, never a fabricated 09:01 row.

        Explicitly retrospective valuation only. Compare receipt-verified local
        sources and the native FinMind backup; retain the real completed-bar
        clock and reject disagreement at the earliest available minute.
        """
        if not self.require_receipts or self._replay_pair_pins_enforced:
            raise ValueError("first-bar assumption requires verified valuation sources, not execution-pinned sources")
        key = (str(symbol), str(session_date))
        if key in self._first_observed_bar_cache:
            return deepcopy(self._first_observed_bar_cache[key])
        candidates = []

        def add(prices, source, pins):
            regular = {m: p for m, p in prices.items()
                       if f"{session_date}T09:01+08:00" <= m <= f"{session_date}T13:30+08:00"}
            if regular:
                minute = min(regular)
                candidates.append({"symbol": symbol, "session_date": session_date,
                    "source_minute": minute, "price": float(regular[minute]), "source": source,
                    "source_pins": pins, "price_method": "observed_completed_minute_close"})

        for root in self.kbar_roots:
            values = self._root_prices(root, symbol, session_date)
            if not values:
                continue
            paths = list(self._chunk_paths(root, symbol, session_date))
            if not paths:
                paths = [root / f"trade_date={session_date}" / "data.parquet"]
            pins = []
            for path in paths:
                pins.append({"path": str(path.resolve()), "sha256": _sha256(path)})
                receipt = path.with_suffix(".receipt.json")
                if receipt.is_file():
                    pins.append({"path": str(receipt.resolve()), "sha256": _sha256(receipt)})
            add(values, f"local_kbar:{root.resolve()}", pins)
        if self.finmind_root is not None:
            receipt_path = self.finmind_root / "receipts/TaiwanStockKBar/all" / f"{session_date}.json"
            if receipt_path.is_file():
                receipt = _read_json(receipt_path)
                if (receipt.get("dataset") != "TaiwanStockKBar" or receipt.get("data_id") != ""
                        or receipt.get("partition") != session_date):
                    raise RuntimeError("first-bar FinMind receipt identity mismatch")
                if receipt.get("status") == "complete":
                    self._prepare_finmind([key])
                    proof = self.finmind_source_records.get(session_date)
                    if proof:
                        add(self._finmind_prices.get(key, {}), "local_finmind_kbar", [
                            {"path": proof["path"], "sha256": proof["sha256"]},
                            {"path": proof["receipt"], "sha256": proof["receipt_sha256"]}])
                else:
                    info = receipt_path.stat()
                    signature = (info.st_size, info.st_mtime_ns, info.st_ctime_ns)
                    if self._verified_finmind_signatures.setdefault(receipt_path, signature) != signature:
                        raise RuntimeError("first-bar source receipt changed during reconstruction")
        if not candidates:
            raise RuntimeError(f"no verified observed first bar for {session_date}:{symbol}")
        earliest = min(item["source_minute"] for item in candidates)
        peers = [item for item in candidates if item["source_minute"] == earliest]
        if any(not math.isclose(item["price"], peers[0]["price"], rel_tol=1e-12, abs_tol=1e-8) for item in peers):
            raise RuntimeError(f"conflicting observed first bars for {session_date}:{symbol}:{earliest}")
        selected = peers[0]
        self._first_observed_bar_cache[key] = deepcopy(selected)
        return deepcopy(selected)

    def _complete_opening_price(self, key, output, primary_source):
        """Resolve a missing *minute*, not merely an entirely missing day.

        Never overwrite a primary observation or change later-minute source
        choice. These values are historical valuations, not entry capacity.
        """
        minute = f"{key[1]}T09:01+08:00"
        if not self.complete_opening_prices or minute in output:
            return
        for root in self.kbar_roots:
            backup = self._root_prices(root, key[0], key[1])
            if minute in backup:
                output[minute] = backup[minute]
                files = list(self._chunk_paths(root, key[0], key[1]))
                if not files:
                    files = [root / f"trade_date={key[1]}" / "data.parquet"]
                self.opening_backup_records[key] = {
                    "symbol": key[0], "session_date": key[1], "minute": minute,
                    "price": backup[minute], "source": f"local_kbar:{root.resolve()}",
                    "primary_source": primary_source, "valuation_only": True,
                    "source_pins": [{"path": str(p.resolve()), "sha256": _sha256(p)} for p in files],
                }
                return
        if self.finmind_root is None:
            return
        receipt_path = self.finmind_root / "receipts/TaiwanStockKBar/all" / f"{key[1]}.json"
        if not receipt_path.is_file():
            return
        receipt = _read_json(receipt_path)
        if (receipt.get("dataset") != "TaiwanStockKBar" or receipt.get("data_id") != ""
                or receipt.get("partition") != key[1]):
            raise RuntimeError("optional opening backup receipt identity mismatch")
        if receipt.get("status") != "complete":
            stat = receipt_path.stat()
            signature = (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
            if self._verified_finmind_signatures.setdefault(receipt_path, signature) != signature:
                raise RuntimeError("optional opening backup receipt changed during reconstruction")
            return
        self._prepare_finmind([key])
        backup = self._finmind_prices.get(key, {})
        if minute in backup:
            output[minute] = backup[minute]
            self.opening_backup_records[key] = {
                "symbol": key[0], "session_date": key[1], "minute": minute,
                "price": backup[minute], "source": "receipt_pinned_finmind_0901_close",
                "primary_source": primary_source, "valuation_only": True,
                "source_pin": self.finmind_source_records[key[1]],
            }

    def _prepare_finmind(self, pairs: Sequence[tuple[str, str]]) -> None:
        """Read only receipt-pinned local backup prices, never execution capacity.

        A current replay's exact symbol/source pins remain authoritative. The
        backup is for ordinary valuation and additive ledgers, which otherwise
        unnecessarily lose locally retained prices of now-delisted contracts.
        """
        if not pairs or self.finmind_root is None or self._replay_pair_pins_enforced:
            return
        from downloader.finmind_storage_objects import validate_parquet
        from stockagent.data.tw_minute_sources import normalize_finmind

        by_day: dict[str, set[str]] = defaultdict(set)
        for symbol, day in pairs:
            if symbol not in self._finmind_loaded[day]:
                by_day[day].add(symbol)
        for day, symbols in by_day.items():
            receipt_path = self.finmind_root / "receipts/TaiwanStockKBar/all" / f"{day}.json"
            if not receipt_path.is_file():
                self._finmind_loaded[day].update(symbols)
                continue
            def signature(path):
                stat = path.stat()
                return stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns
            receipt_signature = signature(receipt_path)
            receipt = _read_json(receipt_path)
            if (receipt.get("status") != "complete"
                    or receipt.get("dataset") != "TaiwanStockKBar"
                    or receipt.get("data_id") != ""
                    or receipt.get("partition") != day):
                raise RuntimeError(f"invalid FinMind minute receipt identity: {receipt_path}")
            relative = Path(str(receipt.get("parquet_path") or ""))
            path = (self.finmind_root / relative).resolve()
            if (relative.is_absolute() or ".." in relative.parts
                    or not path.is_relative_to(self.finmind_root) or not path.is_file()):
                raise RuntimeError(f"unavailable or unsafe FinMind minute object: {path}")
            source_signature = signature(path)
            for source, observed in ((receipt_path, receipt_signature), (path, source_signature)):
                prior_signature = self._verified_finmind_signatures.get(source)
                if prior_signature is not None and prior_signature != observed:
                    raise RuntimeError(f"FinMind minute source changed during reconstruction: {source}")
            prior_proof = self.finmind_source_records.get(day)
            verified_generation = bool(prior_proof
                and prior_proof.get("path") == str(path) and prior_proof.get("sha256") == receipt.get("sha256")
                and self._verified_finmind_signatures.get(path) == source_signature
                and self._verified_finmind_signatures.get(receipt_path) == receipt_signature)
            if verified_generation:
                self.finmind_read_metrics["validated_generation_reuses"] += 1
            else:
                if (source_signature[0] != receipt.get("parquet_size_bytes")
                        or _sha256(path) != receipt.get("sha256")):
                    raise RuntimeError(f"FinMind minute source hash mismatch: {path}")
                validation = validate_parquet(path, "TaiwanStockKBar", day)
                if validation["rows"] != receipt.get("rows"):
                    raise RuntimeError(f"FinMind minute row count mismatch: {path}")
                self.finmind_read_metrics["full_source_validations"] += 1
            raw = pl.scan_parquet(path).filter(pl.col("stock_id").is_in(sorted(symbols))).collect()
            self.finmind_read_metrics["subset_reads"] += 1
            if raw.select(pl.struct("stock_id", "date", "minute").is_duplicated().any()).item():
                raise RuntimeError(f"ambiguous duplicate FinMind minute observations: {path}")
            normalized = normalize_finmind(raw).with_columns(
                raw["volume"].cast(pl.Float64).alias("Volume"))
            normalized = normalized.filter(
                (pl.col("_invalid_reason") == "")
                & pl.col("phase").is_in(["regular", "closing_auction"])
            ).with_columns(pl.col("ts").dt.date().alias("date"))
            # A closing-auction observation supersedes the regular bar sharing
            # its right label. Delayed/after-hours auctions are not 13:30 marks.
            normalized = normalized.sort("ts", pl.col("phase") == "closing_auction")
            grouped = normalized.partition_by("symbol", as_dict=True, maintain_order=True)
            empty = normalized.head(0)
            for symbol in symbols:
                selected = grouped.get((symbol,), empty)
                self._finmind_prices[(symbol, day)] = self._frame_prices(selected, day)
            for source, expected in ((receipt_path, receipt_signature), (path, source_signature)):
                if signature(source) != expected:
                    raise RuntimeError(f"FinMind minute source changed during read: {source}")
                previous = self._verified_finmind_signatures.setdefault(source, expected)
                if previous != expected:
                    raise RuntimeError(f"FinMind minute source changed during reconstruction: {source}")
            self.finmind_source_records[day] = {
                "session_date": day, "path": str(path), "sha256": receipt["sha256"],
                "receipt": str(receipt_path), "receipt_sha256": _sha256(receipt_path),
                "clock_normalization": "tw_raw_minute_provider_union_v2_dated_units",
                "native_volume_use": "positive_trade_observation_only_not_shares_or_liquidity",
                "execution_capacity_claimed": False,
            }
            self._finmind_loaded[day].update(symbols)

    def _verified_replay_source(self, path: Path, session_date: str) -> bool:
        """Verify one exact source file pinned by the execution replay receipt."""

        resolved = Path(self._source_path_identity(path))
        expected = self._replay_source_hashes.get(
            (str(session_date), str(resolved))
        )
        if expected is None:
            return False
        stat = resolved.stat()
        signature = (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns, expected)
        previous = self._verified_replay_source_signatures.get(resolved)
        if previous is None:
            actual = _sha256(resolved)
            if actual != expected:
                raise RuntimeError(
                    f"replay-pinned minute source hash mismatch: {resolved}"
                )
            current = resolved.stat()
            if (current.st_size, current.st_mtime_ns, current.st_ctime_ns) != signature[:3]:
                raise RuntimeError(
                    f"replay-pinned minute source changed while hashing: {resolved}"
                )
            self._verified_replay_source_signatures[resolved] = signature
        elif previous != signature:
            raise RuntimeError(
                f"replay-pinned minute source changed during reconstruction: {resolved}"
            )
        return True

    def _verified_chunk(self, path: Path, symbol: str, session_date: str) -> bool:
        if not self.require_receipts:
            return True
        pair_sources = self._replay_symbol_sources.get(
            (str(session_date), str(symbol))
        )
        resolved = self._source_path_identity(path)
        if pair_sources is not None:
            return resolved in pair_sources and self._verified_replay_source(
                path, session_date
            )
        if (
            session_date,
            resolved,
        ) in self._replay_source_hashes:
            if self._replay_pair_pins_enforced:
                # This file is pinned for another symbol. A symbol absent from
                # the replay's source map may still use a receipt-backed chunk,
                # but it cannot borrow a mutable replay partition implicitly.
                pass
            else:
                return self._verified_replay_source(path, session_date)
        receipt = path.with_suffix(".receipt.json")
        batch_key = path, str(symbol)
        if self._batch_chunk_dates is not None and batch_key in self._batch_chunk_dates:
            self.local_read_metrics["batch_metadata_reuses"] += 1
            return session_date in self._batch_chunk_dates[batch_key]
        if not receipt.is_file():
            if self._batch_chunk_dates is not None:
                self._batch_chunk_dates[batch_key] = frozenset()
            return False
        signature = tuple(self._source_signature(p) for p in (path, receipt))
        cached = self._verified_chunk_dates.get(path)
        if cached is None or cached[0] != signature:
            from downloader.download_shioaji_tw_minute_kbars import minute_receipt_valid
            first, last = map(date.fromisoformat, path.stem.split("_"))
            proof = _read_json(receipt)
            output = proof.get("output_receipt") or {}
            valid = False
            if Path(str(output.get("path", ""))).resolve() == path.resolve():
                self.local_read_metrics["full_chunk_receipt_validations"] += 1
                valid = minute_receipt_valid(receipt, symbol=symbol, start=first, end=last)
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
            self._report_source_progress()
        if self._batch_chunk_dates is not None:
            self._batch_chunk_dates[batch_key] = frozenset(self._verified_chunk_dates[path][1])
        return session_date in self._verified_chunk_dates[path][1]

    def assert_sources_unchanged(self) -> None:
        """Reject a price/receipt replacement between verification and publish."""
        for path, identity in self._resolved_source_paths.items():
            if str(path.resolve()) != identity:
                raise RuntimeError(f"minute source path alias changed during reconstruction: {path}")
        for path, expected in self._verified_finmind_signatures.items():
            stat = path.stat()
            if (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns) != expected:
                raise RuntimeError(f"FinMind minute source changed during reconstruction: {path}")
        for path, expected in self._verified_replay_source_signatures.items():
            try:
                stat = path.stat()
            except OSError as exc:
                raise RuntimeError(
                    f"replay-pinned minute source disappeared: {path}"
                ) from exc
            if (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns) != expected[:3]:
                raise RuntimeError(
                    f"replay-pinned minute source changed during reconstruction: {path}"
                )
        for path, expected in self._verified_raw_tick_signatures.items():
            stat = path.stat()
            if (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns) != expected:
                raise RuntimeError(f"verified raw tick source changed: {path}")
        for path, (expected, _) in self._verified_chunk_dates.items():
            try:
                actual = tuple(self._source_signature(p)
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
        stamp_type = selected.schema.get("ts")
        price_type = selected.schema.get("Close")
        if (isinstance(stamp_type, pl.Datetime) and price_type is not None
                and price_type.is_numeric() and selected["Close"].null_count() == 0):
            stamp = pl.col("ts")
            stamp = (stamp.dt.replace_time_zone("Asia/Taipei") if stamp_type.time_zone is None
                     else stamp.dt.convert_time_zone("Asia/Taipei"))
            # Same minute keys, FP64 values and last-row-wins behavior as the
            # legacy loop, without constructing a Python datetime per trade bar.
            prices = selected.select(
                stamp.dt.strftime("%Y-%m-%dT%H:%M%:z").alias("minute"),
                pl.col("Close").cast(pl.Float64),
            ).filter(pl.col("minute").is_not_null() & pl.col("Close").is_finite()
                     & (pl.col("Close") > 0))
            return dict(prices.iter_rows())
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

    @classmethod
    def _frame_prices_by_session(
        cls, frame: pl.DataFrame, session_dates: Sequence[str],
    ) -> dict[str, dict[str, float]]:
        """Normalize a multi-session chunk once, preserving row-order ties.

        The scalar reader is the compatibility oracle. Batch only its typed
        fast path; unusual source schemas retain the original validation and
        errors rather than silently coercing dates or prices.
        """
        days = {day: date.fromisoformat(day) for day in session_dates}
        if not frame.height:
            return {day: {} for day in days}
        stamp_type, price_type = frame.schema.get("ts"), frame.schema.get("Close")
        if (frame.schema.get("date") != pl.Date
                or not isinstance(stamp_type, pl.Datetime) or price_type is None
                or not price_type.is_numeric() or frame["Close"].null_count()):
            return {day: cls._frame_prices(frame, day) for day in days}
        selected = frame.filter(pl.col("date").is_in(list(days.values())))
        if "Volume" in selected.columns:
            selected = selected.filter(pl.col("Volume").is_finite() & (pl.col("Volume") > 0))
        stamp = pl.col("ts")
        stamp = (stamp.dt.replace_time_zone("Asia/Taipei") if stamp_type.time_zone is None
                 else stamp.dt.convert_time_zone("Asia/Taipei"))
        prices = selected.select(
            pl.col("date").cast(pl.String).alias("session_date"),
            stamp.dt.strftime("%Y-%m-%dT%H:%M%:z").alias("minute"),
            pl.col("Close").cast(pl.Float64),
        ).filter(pl.col("minute").is_not_null() & pl.col("Close").is_finite()
                 & (pl.col("Close") > 0))
        output = {day: {} for day in days}
        for day, minute, price in prices.iter_rows():
            output[day][minute] = price
        return output

    def _chunk_paths(
        self,
        root: Path,
        symbol: str,
        session_date: str,
    ) -> tuple[Path, ...]:
        index_key = (self._source_path_identity(root), symbol)
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
        root_key = (self._source_path_identity(root), symbol, session_date)
        cached = self._root_cache.get(root_key)
        if cached is not None:
            return dict(cached)

        output: dict[str, float] = {}
        for path in self._chunk_paths(root, symbol, session_date):
            frame = pl.read_parquet(path, columns=["date", "ts", "Close", *(["Volume"] if self.require_receipts else [])])
            output.update(self._frame_prices(frame, session_date))
        if not output:
            partition = root / f"trade_date={session_date}" / "data.parquet"
            pair_sources = self._replay_symbol_sources.get(
                (str(session_date), str(symbol))
            )
            partition_allowed = (
                partition.is_file()
                and (
                    not self.require_receipts
                    or (
                        pair_sources is not None
                        and str(partition.resolve()) in pair_sources
                        and self._verified_replay_source(partition, session_date)
                    )
                    or (
                        not self._replay_pair_pins_enforced
                        and self._verified_replay_source(partition, session_date)
                    )
                )
            )
            if partition_allowed:
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
        if self._batch_chunk_dates is not None:
            raise RuntimeError("minute source batch preparation is not reentrant")
        self._batch_chunk_dates = {}
        try:
            self._prepare_source_snapshot(required)
            # The batch cache is only a read-planning optimization. Reject
            # every changed verified source/receipt/alias before returning;
            # the independent publication fence remains in place as well.
            self.assert_sources_unchanged()
        finally:
            self._batch_chunk_dates = None

    def _prepare_source_snapshot(self, required: Mapping[str, set[str]]) -> None:

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
                normalized = self._frame_prices_by_session(
                    frame, sorted({day for _, day in selected_pairs}))
                self.local_read_metrics["chunk_files_read"] += 1
                self._report_source_progress()
                for symbol, session_date in selected_pairs:
                    staged[(symbol, session_date)].update(
                        normalized[session_date]
                    )

            by_date: dict[str, set[str]] = defaultdict(set)
            for symbol, session_date in pairs:
                if not staged[(symbol, session_date)]:
                    by_date[session_date].add(symbol)
            for session_date, symbols in by_date.items():
                partition = root / f"trade_date={session_date}" / "data.parquet"
                if not partition.is_file():
                    continue
                allowed_symbols = set(symbols)
                if self.require_receipts:
                    if self._replay_pair_pins_enforced:
                        resolved_partition = str(partition.resolve())
                        allowed_symbols = {
                            symbol
                            for symbol in symbols
                            if resolved_partition
                            in self._replay_symbol_sources.get(
                                (str(session_date), str(symbol)), frozenset()
                            )
                        }
                    if not allowed_symbols or not self._verified_replay_source(
                        partition, session_date
                    ):
                        continue
                frame = (
                    pl.scan_parquet(partition)
                    .filter(pl.col("symbol").is_in(sorted(allowed_symbols)))
                    .select(
                        "symbol",
                        "date",
                        "ts",
                        "Close",
                        *(["Volume"] if self.require_receipts else []),
                    )
                    .collect()
                )
                grouped = frame.partition_by("symbol", as_dict=True, maintain_order=True)
                empty = frame.head(0)
                for symbol in allowed_symbols:
                    selected = grouped.get((symbol,), empty).select(
                        "date",
                        "ts",
                        "Close",
                        *(["Volume"] if self.require_receipts else []),
                    )
                    staged[(symbol, session_date)] = self._frame_prices(
                        selected, session_date
                    )

            root_identity = self._source_path_identity(root)
            for (symbol, session_date), prices in staged.items():
                self._root_cache[(root_identity, symbol, session_date)] = dict(prices)
            # prices() uses the first nonempty source for a symbol/session.
            # Reading lower-priority duplicates cannot change that result and
            # needlessly loads the same multi-month tape twice into memory.
            pairs = [pair for pair in pairs if not staged[pair]]
            if not pairs:
                break
        self._prepare_finmind(pairs)
        if self.complete_opening_prices and self.finmind_root is not None:
            # A whole-market native partition answers all missing opening
            # minutes together. Do not validate/decode the entire day again
            # for each symbol later in coverage()/prices(). Source priority
            # and real 09:01 observations remain unchanged.
            roots = [self._source_path_identity(root) for root in self.kbar_roots]
            by_day: dict[str, list[tuple[str, str]]] = defaultdict(list)
            for symbol, dates in required.items():
                for raw_day in dates:
                    key = str(symbol), str(raw_day)
                    primary = next((values for root in roots
                        if (values := self._root_cache.get((root, *key)))),
                        self._finmind_prices.get(key, {}))
                    if f"{key[1]}T09:01+08:00" not in primary:
                        by_day[key[1]].append(key)
            ready = []
            for day, selected in by_day.items():
                receipt = self.finmind_root / "receipts/TaiwanStockKBar/all" / f"{day}.json"
                if receipt.is_file() and _read_json(receipt).get("status") == "complete":
                    ready.extend(selected)
            self._prepare_finmind(ready)
            self.finmind_read_metrics["opening_backup_pairs_prepared"] += len(ready)

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
                source = f"local_kbar:{self._source_path_identity(root)}"
                break
        if not output and self.finmind_root is not None:
            self._prepare_finmind([key])
            output = dict(self._finmind_prices.get(key, {}))
            if output:
                source = f"local_finmind_kbar:{self.finmind_root}"
        if not output and not self.require_receipts:
            for tick_root in self.tick_minute_roots:
                tick_path = tick_root / key[0] / f"{key[1]}.parquet"
                if not tick_path.is_file():
                    continue
                frame = pl.read_parquet(tick_path, columns=["date", "ts", "Close"])
                output = self._frame_prices(frame, key[1])
                if output:
                    source = f"local_tick_cache:{self._source_path_identity(tick_root)}"
                    break
        self._complete_opening_price(key, output, source)
        self._cache[key] = dict(output)
        self._source_cache[key] = source
        return output

    def invalidate(self, symbol: str, session_date: str) -> None:
        key = (str(symbol), str(session_date))
        self._cache.pop(key, None)
        self._source_cache.pop(key, None)
        for root in self.kbar_roots:
            root_identity = self._source_path_identity(root)
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
    price-quality fields.  Completed 09:01 revaluation uses this predicate
    after separately checking its accepted accounting; the 13:30 execution
    endpoint is checked under its own immutable-ledger contract.
    """

    if any(row.get(opening_assumption_policy(first)["key"]) is not None for first in (False, True)):
        return False
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
    from the retained one-minute tape and append-only inventory fill ledger.
    Clocked entries, completions and exits determine the inventory and basis;
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
    opening_full_target_price_carries = 0
    fills_by_position: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(
        list
    )
    session_fills: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for raw_fill in fill_rows:
        if not isinstance(raw_fill, Mapping):
            continue
        session_date = str(raw_fill.get("session_date") or "")
        market = str(raw_fill.get("market") or "")
        position_id = str(raw_fill.get("position_id") or "")
        if not session_date or not market or not position_id:
            continue
        session_fills[(session_date, market)].append(dict(raw_fill))
        if _is_entry_inventory_fill(raw_fill):
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
            events = session_fills.get((session_date, market), [])
            inventory = None
            if any(_is_entry_inventory_fill(event) for event in events):
                inventory = _ImmutableMinuteInventory(
                    {str(p["position_id"]): p for p in mode_positions},
                    [dict(event, clock=str(event.get("fill_at") or event.get("recorded_at") or ""))
                     for event in events],
                    market=market,
                )
            # Exit-only legacy books have no entry clocks. Full append-only
            # books must prove the opening count from fills, not the final
            # all-day position archive (which may include later entries).
            if inventory is None and expected_open != len(mode_positions):
                raise RuntimeError(
                    f"position count mismatch for {session_date}:{market}: "
                    f"accepted={expected_open} reconstructed={len(mode_positions)}"
                )
            current_prices: dict[str, float] = {}
            price_times: dict[str, datetime] = {}
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
                if inventory is None:
                    current_prices[symbol] = initial
                minute_prices[symbol] = store.prices(symbol, session_date)

            realized_baseline = float(opening.get("cumulative_realized_net_pnl_twd") or 0.0)
            for minute in _session_minutes(day):
                clock = minute.strftime("%H:%M")
                if clock == "09:00":
                    # The strategy has no position before its 09:01 paper entry.
                    # Do not fabricate a pre-entry equity point from the later fill.
                    continue
                minute_key = minute.isoformat(timespec="minutes")
                existing = by_key.get((session_date, market, minute_key))
                if inventory is not None:
                    cutoff = _clock_instant(minute)
                    if existing is not None and clock in {"09:01", "13:30"}:
                        cutoff = max(cutoff, _clock_instant(existing.get("recorded_at") or cutoff))
                    inventory.advance(cutoff, current_prices, price_times)
                    if clock == "09:01":
                        actual_open = len(inventory.positions)
                        if expected_open != actual_open:
                            raise RuntimeError(
                                f"position count mismatch for {session_date}:{market}: "
                                f"accepted={expected_open} reconstructed={actual_open}"
                            )
                        # A subrange can begin after prior realized gains;
                        # exits already included in its opening endpoint must
                        # not be counted twice.
                        realized_baseline -= inventory.realized
                    if clock == "13:30":
                        if (len(inventory.positions) != int(closing.get("open_position_count") or 0)
                            or not math.isclose(realized_baseline + inventory.realized,
                                                float(closing.get("cumulative_realized_net_pnl_twd") or 0.0),
                                                rel_tol=1e-12, abs_tol=1e-6)):
                            raise RuntimeError(f"closing fill-book accounting disagrees: {session_date}:{market}")
                fresh_symbols: set[str] = set()
                for symbol, prices in minute_prices.items():
                    value = prices.get(minute_key)
                    if value is not None:
                        current_prices[symbol] = value
                        price_times[symbol] = minute
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
                    held_symbols = ({p["symbol"] for p in inventory.positions.values()}
                                    if inventory is not None else set(minute_prices))
                    missing_open = held_symbols - fresh_symbols
                    # The explicitly authorized no-liquidity-claim scenario
                    # can enter from a source-published 09:01 price with no new
                    # valuation print in the selected minute tape. Carry that
                    # cohort's immutable paper price, marked stale; never call
                    # it a fresh market print or relax ordinary replay/live
                    # source requirements. All cohorts of the symbol qualify.
                    authorized_carry = {
                        symbol for symbol in missing_open
                        if inventory is not None and all(
                            p["position_id"] in inventory.full_0901_price_entries
                            for p in inventory.positions.values() if p["symbol"] == symbol
                        )
                    }
                    if missing_open - authorized_carry:
                        raise RuntimeError(
                            f"missing completed 09:01 valuation bar for "
                            f"{session_date}:{market}:{sorted(missing_open - authorized_carry)}"
                        )
                    opening_full_target_price_carries += len(authorized_carry)
                if existing is not None:
                    replaced_unverified_rows += 1
                open_net = 0.0
                total_notional = 0.0
                fresh_notional = 0.0
                realized_this_session = inventory.realized if inventory is not None else 0.0
                open_positions = 0
                fresh_open_positions = 0
                valued_positions = inventory.positions.values() if inventory is not None else mode_positions
                for position in valued_positions:
                    symbol = str(position["symbol"])
                    if inventory is not None:
                        remaining = abs(int(position["signed_shares"]))
                        signed = int(position["signed_shares"])
                        remaining_entry_fee = float(position["remaining_entry_fee_twd"])
                        price = inventory.mark_price(position, current_prices, price_times)
                    else:
                        position_id = str(position.get("position_id") or symbol)
                        exited_quantity, allocated_entry_fee = 0, 0.0
                        for fill in fills_by_position.get((session_date, market, position_id), ()):
                            fill_at = str(fill.get("fill_at") or fill.get("recorded_at") or "")
                            if not fill_at or fill_at > minute.isoformat(timespec="seconds"):
                                break
                            exited_quantity += int(fill.get("quantity") or 0)
                            allocated_entry_fee += float(fill.get("entry_fee_allocated_twd") or 0.0)
                            realized_this_session += float(fill.get("net_pnl_twd") or 0.0)
                        remaining = _filled_quantity(position) - exited_quantity
                        if remaining < 0:
                            raise RuntimeError(
                                f"exit quantity exceeds entry for {session_date}:{market}:{position_id}"
                            )
                        if remaining == 0:
                            continue
                        signed = remaining if str(position.get("side")) == "long" else -remaining
                        price = current_prices[symbol]
                        remaining_entry_fee = max(0.0, float(position.get("entry_fee_twd") or 0.0)
                                                  - allocated_entry_fee)
                    open_positions += 1
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
                    realized_baseline + realized_this_session
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
        "opening_full_target_fill_price_carried_symbol_days": opening_full_target_price_carries,
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


def _validated_legacy_paper_entry_retries(
    events: Sequence[Mapping[str, Any]],
    metadata: Mapping[str, Mapping[str, Any]],
    *,
    market: str,
) -> frozenset[str]:
    """Recognize old paper retry labels without changing the immutable fills.

    A second positive paper retry extends an existing cohort, even when an old
    producer labelled it `entry`. Only our explicit retry order IDs and paper
    fill contracts qualify. This is not broker-event deduplication; arbitrary
    duplicate entries and repeated paper order IDs remain hard errors.
    Validate identities before reading all session prices or valuing minutes.
    """

    seen_positions: set[str] = set()
    seen_orders: set[str] = set()
    legacy_orders: set[str] = set()
    for event in events:
        if event.get("share_action") or not _is_entry_inventory_fill(event):
            continue
        identity = str(event.get("position_id") or "")
        order_id = str(event.get("order_id") or "")
        if identity not in metadata:
            raise RuntimeError(f"minute entry has no accepted position: {market}:{identity}")
        if int(event.get("quantity") or 0) <= 0:
            raise RuntimeError(f"minute entry has invalid quantity: {market}:{identity}")
        if order_id and order_id in seen_orders:
            raise RuntimeError(f"duplicate paper entry order: {market}:{order_id}")
        if event.get("purpose") == "entry" and identity in seen_positions:
            prefix = f"{identity}:entry_retry:"
            sequence = order_id[len(prefix):] if order_id.startswith(prefix) else ""
            if not (
                event.get("simulation_only") is True
                and event.get("fill_contract") in {
                    "causal_best_quote_remaining_order",
                    "paper_market_full_target_at_causal_shioaji_best_quote",
                }
                and event.get("symbol") == metadata[identity].get("symbol")
                and sequence and sequence[0] != "0"
                and sequence.isascii() and sequence.isdecimal()
            ):
                raise RuntimeError(f"duplicate minute entry identity: {market}:{identity}")
            legacy_orders.add(order_id)
        elif event.get("purpose") == "entry_completion" and identity not in seen_positions:
            raise RuntimeError(f"minute entry completion has no accepted original entry: {market}:{identity}")
        seen_positions.add(identity)
        if order_id:
            seen_orders.add(order_id)
    return frozenset(legacy_orders)


def _clock_instant(value: str | datetime) -> datetime:
    """Compare actual instants, not ISO spelling, without assuming a timezone."""
    if isinstance(value, datetime):
        stamp = value
    elif isinstance(value, str) and value:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError("minute inventory clock is missing or invalid")
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError("minute inventory clock requires a timezone")
    return stamp


def _is_legacy_0901_fill(event: Mapping[str, Any]) -> bool:
    """Accept equivalent representations of the exact session entry instant."""
    try:
        day = date.fromisoformat(str(event.get("session_date") or ""))
        return _clock_instant(event.get("fill_at")) == datetime.combine(
            day, STRATEGY_ENTRY, tzinfo=TAIPEI
        )
    except (TypeError, ValueError):
        return False


def _archived_terminal_fill_clock(event, metadata):
    """Resolve only a uniquely matched legacy second-truncated native exit.

    Original journal clocks are never changed. The sealed position's finer
    last_exit_at is usable only inside that original second and when identity,
    purpose, quantity, price and quote clock all match the terminal fill.
    """
    raw = event.get("clock")
    if (not isinstance(raw, str) or len(raw) < 20 or raw[19:20] not in {"+", "-", "Z"}
            or event.get("purpose") in {"entry", "entry_completion"}
            or event.get("share_action") or event.get("simulation_only") is not True
            or event.get("simulation_replay") is not False):
        return None
    position = metadata.get(event.get("position_id"))
    if not isinstance(position, Mapping):
        return None
    try:
        original = _clock_instant(raw)
        precise = _clock_instant(position.get("last_exit_at"))
        entered = _clock_instant(position.get("entry_at"))
        if (not original < precise < original + timedelta(seconds=1) or precise < entered
                or _clock_instant(event.get("fill_at")) != original
                or _clock_instant(event.get("recorded_at")) != original
                or _clock_instant(position.get("exit_at")) != original
                or position.get("position_id") != event.get("position_id")
                or position.get("market") != event.get("market")
                or position.get("symbol") != event.get("symbol")
                or position.get("session_date") != event.get("session_date")
                or position.get("signed_shares") != 0
                or position.get("exit_reason") != event.get("purpose")
                or type(event.get("quantity")) is not int or event["quantity"] <= 0
                or position.get("last_exit_quantity") != event["quantity"]
                or not math.isfinite(float(event.get("price")))
                or float(position.get("last_exit_price")) != float(event["price"])
                or not isinstance(event.get("quote_at"), str) or not event["quote_at"]
                or position.get("last_exit_quote_at") != event["quote_at"]):
            return None
    except (ValueError, TypeError):
        return None
    return precise


class _ImmutableMinuteInventory:
    """Replay retained inventory clocks for flat and carried valuation alike.

    This is a read model, not an execution engine: fills, quantities, fees and
    realized PnL are immutable inputs. Final archived average cost must never
    leak into minutes preceding a late entry or entry completion.
    """

    def __init__(self, metadata, events, *, market):
        self.metadata = metadata
        # Stable sorting preserves journal order when differently formatted
        # timestamps identify the same instant, such as +08:00 and UTC.
        resolutions = [(event, _archived_terminal_fill_clock(event, metadata)) for event in events]
        counts = Counter(event.get("position_id") for event, clock in resolutions if clock is not None)
        self.fill_clock_resolutions = []
        timed = []
        for event, precise in resolutions:
            clock = _clock_instant(event.get("clock"))
            if precise is not None and counts[event["position_id"]] == 1:
                self.fill_clock_resolutions.append({
                    "contract": "archived_unique_terminal_exit_precision_v1",
                    "position_id": event["position_id"], "order_id": event.get("order_id"),
                    "original_fill_clock": str(event["clock"]),
                    "derived_read_clock": precise.isoformat(timespec="microseconds"),
                    "original_journal_changed": False,
                })
                clock = precise
            timed.append((clock, event))
        timed.sort(key=lambda item: item[0])
        self._event_times = [item[0] for item in timed]
        self.events = [item[1] for item in timed]
        self.legacy_orders = _validated_legacy_paper_entry_retries(
            self.events, metadata, market=market
        )
        self.positions: dict[str, dict[str, Any]] = {}
        self.entry_fallbacks: dict[str, tuple[float, datetime]] = {}
        self.full_0901_price_entries = frozenset(
            event["position_id"] for event in self.events
            if event.get("purpose") == "entry"
            and event.get("fill_contract") == LEGACY_REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL
            and _is_legacy_0901_fill(event)
            and event.get("simulation_only") is True
            and event.get("simulation_replay") is True
            and event.get("counterfactual_0901_price_fill") is True
            and event.get("synthetic_fill") is False
            and event.get("entry_price_method") in {"minute_vwap", "minute_close"}
            and bool(event.get("entry_price_source"))
            and math.isfinite(float(event.get("price") or 0))
            and float(event.get("price") or 0) > 0
        )
        self.seen_entries: set[str] = set()
        self.cursor = 0
        self.realized = 0.0

    def advance(self, cutoff, prices, price_times, new_entries=None):
        cutoff_at = _clock_instant(cutoff)
        while self.cursor < len(self.events) and self._event_times[self.cursor] <= cutoff_at:
            event = self.events[self.cursor]
            event_at = self._event_times[self.cursor]
            identity = event["position_id"]
            if event.get("share_action"):
                p = self.positions[identity]
                if p["signed_shares"] != event["old_signed_shares"]:
                    raise RuntimeError("minute share replacement disagrees with retained inventory")
                p["signed_shares"] = int(event["new_signed_shares"])
                p["inventory_basis_price"] = float(event["new_entry_price"])
                symbol = p["symbol"]
                price_at = price_times.get(symbol)
                if symbol in prices and (not price_at or _clock_instant(price_at).astimezone(TAIPEI).date()
                                         < event_at.astimezone(TAIPEI).date()):
                    prices[symbol] = (prices[symbol] - event["cash_per_old_share"]) / event["ratio"]
                    price_times[symbol] = event_at
                if identity in self.entry_fallbacks:
                    fallback, started = self.entry_fallbacks[identity]
                    self.entry_fallbacks[identity] = (
                        (fallback - event["cash_per_old_share"]) / event["ratio"], started
                    )
            elif event["purpose"] == "entry" and event.get("order_id") not in self.legacy_orders:
                if identity in self.seen_entries or identity not in self.metadata:
                    raise RuntimeError("minute entry identity is duplicate or has no accepted position")
                self.seen_entries.add(identity)
                if new_entries is not None:
                    new_entries.add(identity)
                p = dict(self.metadata[identity])
                sign = 1 if p["side"] == "long" else -1
                p.update(signed_shares=sign * int(event["quantity"]),
                         inventory_basis_price=float(event["price"]),
                         remaining_entry_fee_twd=float(event["fee_and_tax_twd"]))
                self.positions[identity] = p
                self.entry_fallbacks[identity] = (float(event["price"]), event_at)
            elif event["purpose"] == "entry_completion" or event.get("order_id") in self.legacy_orders:
                if identity not in self.seen_entries or identity not in self.positions:
                    raise RuntimeError("minute entry completion has no accepted original entry")
                p = self.positions[identity]
                old_quantity = abs(int(p["signed_shares"]))
                quantity = int(event["quantity"])
                if quantity <= 0:
                    raise RuntimeError("minute entry completion has invalid quantity")
                sign = 1 if p["side"] == "long" else -1
                p["signed_shares"] += sign * quantity
                p["inventory_basis_price"] = (
                    float(p["inventory_basis_price"]) * old_quantity
                    + float(event["price"]) * quantity
                ) / (old_quantity + quantity)
                p["remaining_entry_fee_twd"] += float(event["fee_and_tax_twd"])
                self.entry_fallbacks[identity] = (p["inventory_basis_price"], event_at)
            else:
                if identity not in self.positions:
                    raise RuntimeError(f"minute exit precedes a proven entry or its retained inventory: {identity}")
                p = self.positions[identity]
                quantity = int(event["quantity"])
                if quantity <= 0 or quantity > abs(p["signed_shares"]):
                    raise RuntimeError("minute exit exceeds retained inventory or has invalid quantity")
                p["signed_shares"] -= quantity * (1 if p["signed_shares"] > 0 else -1)
                p["remaining_entry_fee_twd"] -= float(event["entry_fee_allocated_twd"])
                self.realized += float(event["net_pnl_twd"])
                if not p["signed_shares"]:
                    del self.positions[identity]
                    self.entry_fallbacks.pop(identity, None)
            self.cursor += 1

    def mark_price(self, position, prices, price_times):
        symbol = position["symbol"]
        fallback = self.entry_fallbacks.get(position["position_id"])
        price_at = price_times.get(symbol)
        if fallback and (not price_at or _clock_instant(price_at) < fallback[1]):
            return fallback[0]
        price = prices.get(symbol)
        if price is None:
            raise RuntimeError("inventory valuation has no prior observed price")
        return price


def _advance_endpoint_with_recorded_precision(
    inventory: _ImmutableMinuteInventory,
    cutoff: datetime,
    existing: Mapping[str, Any],
    prices: dict,
    price_times: dict,
    new_entries: set,
) -> bool:
    """Resolve a second-truncated mark only when its inventory is unique.

    The engine records the mark at second precision but can record a fill
    made during the same cycle at microsecond precision.  The truncated mark
    describes an interval, not its lower bound.  Try immutable event prefixes
    in that interval against the retained position count and realized PnL;
    do not guess if two different prefixes fit.  Never change the source
    timestamp or any execution, and never widen an explicitly fractional mark.
    """
    inventory.advance(cutoff, prices, price_times, new_entries)
    raw = existing.get("recorded_at")
    if not (existing.get("historical_minute_replay") is not True
            and isinstance(raw, str) and len(raw) >= 20
            and raw[16:17] == ":" and raw[19:20] in {"+", "-", "Z"}
            and _clock_instant(raw) == cutoff):
        return False
    end = cutoff + timedelta(seconds=1)
    if inventory.cursor >= len(inventory.events) or inventory._event_times[inventory.cursor] >= end:
        return False

    trial = copy(inventory)
    trial.positions = {identity: dict(position) for identity, position in inventory.positions.items()}
    trial.entry_fallbacks = dict(inventory.entry_fallbacks)
    trial.seen_entries = set(inventory.seen_entries)
    trial_prices, trial_times, trial_entries = dict(prices), dict(price_times), set(new_entries)
    expected_count = float(existing.get("open_position_count") or 0)
    expected_realized = float(existing.get("cumulative_realized_net_pnl_twd") or 0)

    def matches() -> bool:
        count = sum(bool(position.get("signed_shares")) for position in trial.positions.values())
        return (math.isclose(float(count), expected_count, rel_tol=1e-12, abs_tol=1e-6)
                and math.isclose(trial.realized, expected_realized, rel_tol=1e-12, abs_tol=1e-6))

    matches_at = [cutoff] if matches() else []
    while trial.cursor < len(trial.events) and trial._event_times[trial.cursor] < end:
        instant = trial._event_times[trial.cursor]
        # The streams do not record the relative position of a mark among
        # multiple same-instant events. Do not invent that ordering.
        if (trial.cursor + 1 < len(trial.events)
                and trial._event_times[trial.cursor + 1] == instant):
            raise RuntimeError("ambiguous second-precision endpoint: simultaneous paper events")
        trial.advance(instant, trial_prices, trial_times, trial_entries)
        if matches():
            matches_at.append(instant)
    if len(matches_at) > 1:
        raise RuntimeError("ambiguous second-precision endpoint: multiple inventory prefixes match")
    if len(matches_at) == 1 and matches_at[0] > cutoff:
        inventory.advance(matches_at[0], prices, price_times, new_entries)
        return True
    return False


def _eligible_replayed_opening_repair(
    existing: Mapping[str, Any], *, market: str, key: str,
    targets: set[tuple[str, str]], mismatched_fields: set[str],
) -> bool:
    """Admit only explicitly selected, source-labelled derived openings."""
    day = key[:10]
    fields = ("cumulative_realized_net_pnl_twd", "open_position_count",
              "cumulative_carry_cost_twd", "cumulative_corporate_action_net_twd")
    return bool(
        (market, day) in targets and key == f"{day}T09:01+08:00"
        and mismatched_fields and mismatched_fields <= {
            "cumulative_realized_net_pnl_twd", "open_position_count"}
        and existing.get("market") == market and existing.get("session_date") == day
        and existing.get("minute") == key
        and existing.get("recorded_at") == f"{day}T09:01:00+08:00"
        and existing.get("simulation_only") is True
        and existing.get("opening_mark_revalued_from_completed_bar") is True
        and all(type(existing.get(field)) in {int, float}
                and math.isfinite(existing[field]) for field in fields)
        and float(existing["open_position_count"]).is_integer()
        and existing["open_position_count"] >= 0
        and historical_minute_mark_has_source(existing)
    )


CASH_GAP_RECOVERY_CONTRACT = "proven_recorded_paper_cash_session_valuation_v1"


def recover_proven_cash_session_marks(
    source_rows, positions, *, state, fill_rows, activity_rows,
    completed_sessions: Sequence[str], expected_markets: set[str], reconstructed_at: datetime,
):
    """Derive missing cash-account valuations, never missing executions.

    Only entirely absent interior sessions qualify. The immutable fill book,
    flat preceding close, following opening and absence of orders/events must
    agree. No quote provider's missing rows are interpreted as zero trading.
    """
    days = list(completed_sessions)
    if (len(days) < 3 or days != sorted(set(days)) or not expected_markets
            or not expected_markets <= set(state.get("modes") or {})
            or reconstructed_at.tzinfo is None):
        raise ValueError("cash-session recovery requires an exact completed-session and mode scope")
    start, end = date.fromisoformat(days[0]), date.fromisoformat(days[-1])
    if any(date.fromisoformat(day) >= reconstructed_at.astimezone(TAIPEI).date() for day in days):
        raise ValueError("cash-session recovery is completed-session only")
    selected = {(row["market"], row["minute"]): row for row in source_rows
                if row.get("session_date") in days and row.get("market") in expected_markets}
    missing = missing_accepted_endpoints(source_rows, start=start, end=end,
        expected_sessions=days, expected_markets=expected_markets)
    gaps = {(row["market"], row["session_date"]) for row in missing}
    if not gaps:
        return list(source_rows), {"contract": CASH_GAP_RECOVERY_CONTRACT, "sessions": [], "generated_rows": 0}
    recorded_sessions = {(row.get("market"), row.get("session_date")) for row in source_rows}
    gap_days = {day for _market, day in gaps}
    for gap in missing:
        market, day = gap["market"], gap["session_date"]
        if (gap["missing_clocks"] != ["09:01", "13:30"] or day in {days[0], days[-1]}
                or (market, day) in recorded_sessions):
            raise RuntimeError(f"cash recovery requires a wholly absent interior session: {day}:{market}")
    # Both declared session and original local clock can expose a misdated
    # accounting row. Global events on a candidate day are not ignored.
    for row in chain(fill_rows, activity_rows):
        market = row.get("market")
        observed = {str(row.get("session_date") or "")}
        for field in ("fill_at", "recorded_at"):
            if row.get(field):
                observed.add(_clock_instant(row[field]).astimezone(TAIPEI).date().isoformat())
        if (any((market, day) in gaps for day in observed) if market else bool(observed & gap_days)):
            raise RuntimeError("cash recovery encountered recorded order, fill or account activity")
    recovered, proofs, fill_clock_resolutions = [], [], []
    for market in sorted({name for name, _day in gaps}):
        mode = state["modes"][market]
        if any(mode.get(key) for key in ("carry_cost_ledger", "corporate_action_ledger",
                                        "share_replacement_ledger", "research_close_expense_ledger")):
            raise RuntimeError("cash recovery does not infer corporate, carry or research adjustments")
        # Later, unrelated sessions have no metadata in this exact historical
        # projection. Do not require their positions or replay today's book.
        # Retain a misdated row whose actual clock falls in the selected past;
        # missing metadata/activity must still reject it rather than hide it.
        fills = [row for row in fill_rows if row.get("market") == market
                 and (date.fromisoformat(str(row["session_date"])) <= end
                      or _clock_instant(str(row.get("fill_at") or row["recorded_at"]))
                          .astimezone(TAIPEI).date() <= end)]
        if any(str(row.get("session_date") or "") < days[0] for row in fills):
            raise RuntimeError("cash recovery requires the full retained fill horizon")
        metadata = {p["position_id"]: p for modes in positions.values() for p in modes.get(market, [])}
        inventory = _ImmutableMinuteInventory(metadata,
            [row | {"clock": str(row.get("fill_at") or row["recorded_at"])} for row in fills], market=market)
        fill_clock_resolutions.extend(inventory.fill_clock_resolutions)
        prices, times, entries = {}, {}, set()
        for _name, day in sorted(pair for pair in gaps if pair[0] == market):
            index = days.index(day)
            previous, following = days[index - 1], days[index + 1]
            before = selected.get((market, f"{previous}T13:30+08:00"))
            after = selected.get((market, f"{following}T09:01+08:00"))
            if before is None or after is None:
                raise RuntimeError("cash recovery requires both adjacent accepted account anchors")
            initial = float(mode["initial_capital_twd"])
            def check_anchor(row, *, flat):
                if (not math.isfinite(initial) or initial <= 0
                        or not math.isclose(float(row["initial_capital_twd"]), initial, rel_tol=0, abs_tol=1e-6)
                        or any(float(row.get(key) or 0) != 0 for key in (
                            "cumulative_carry_cost_twd", "cumulative_corporate_action_net_twd",
                            "corporate_action_receivable_twd", "corporate_action_payable_twd"))):
                    raise RuntimeError("cash recovery anchor has unsupported account adjustments")
                cutoff = max(_clock_instant(row["minute"]), _clock_instant(row["recorded_at"]))
                if cutoff.astimezone(TAIPEI).date().isoformat() != row["session_date"]:
                    raise RuntimeError("cash recovery refuses cross-session account anchor clocks")
                _advance_endpoint_with_recorded_precision(inventory, cutoff, row, prices, times, entries)
                count = sum(bool(p["signed_shares"]) for p in inventory.positions.values())
                if (count != row["open_position_count"]
                        or not math.isfinite(float(row["cumulative_realized_net_pnl_twd"]))
                        or not math.isclose(inventory.realized, float(row["cumulative_realized_net_pnl_twd"]), rel_tol=1e-12, abs_tol=1e-6)
                        or flat and (count or float(row["open_net_liquidation_pnl_twd"]) != 0
                            or not math.isclose(initial + inventory.realized, float(row["total_equity_twd"]), rel_tol=1e-12, abs_tol=1e-6))):
                    raise RuntimeError("cash recovery anchor disagrees with the retained fill book")
            check_anchor(before, flat=True)
            realized = inventory.realized
            for clock in ("09:01", "13:30"):
                inventory.advance(f"{day}T{clock}:00+08:00", prices, times, entries)
                if inventory.positions or inventory.realized != realized:
                    raise RuntimeError("cash recovery found held inventory or changed realized accounting")
            check_anchor(after, flat=False)
            proof = {"market": market, "session_date": day,
                "prior_close": dict(before), "following_open": dict(after),
                "cumulative_realized_net_pnl_twd": realized, "total_equity_twd": initial + realized,
                "recorded_paper_activity_count": 0, "market_zero_volume_claimed": False,
                "missing_signal_replay_performed": False}
            proofs.append(proof)
            for minute in _session_minutes(date.fromisoformat(day))[1:]:
                recovered.append({"market": market, "session_date": day,
                    "minute": minute.isoformat(timespec="minutes"),
                    "recorded_at": minute.isoformat(timespec="seconds"),
                    "reconstructed_at": reconstructed_at.isoformat(timespec="microseconds"),
                    "timestamp_semantics": "derived_account_valuation_minute_not_historical_observation",
                    "initial_capital_twd": initial, "cumulative_realized_net_pnl_twd": realized,
                    "cumulative_carry_cost_twd": 0.0, "cumulative_corporate_action_net_twd": 0.0,
                    "corporate_action_cash_net_twd": 0.0, "corporate_action_receivable_twd": 0.0,
                    "corporate_action_payable_twd": 0.0, "open_net_liquidation_pnl_twd": 0.0,
                    "total_equity_twd": initial + realized, "open_position_count": 0,
                    "stale_position_count": 0, "valuation_stale": False,
                    "fresh_trade_position_count": 0, "last_trade_carried_position_count": 0,
                    "missing_price_position_count": 0, "fresh_trade_notional_coverage_ratio": 1.0,
                    "historical_minute_replay": True, "minute_valuation_contract": MINUTE_CONTRACT,
                    "valuation_source": CASH_GAP_RECOVERY_CONTRACT, "valuation_executable": False,
                    "cash_account_gap_recovery_contract": CASH_GAP_RECOVERY_CONTRACT,
                    "simulation_only": True, "margin_carry_contract": mode.get("margin_carry_contract"),
                    "odd_lot_execution_policy": mode.get("odd_lot_execution_policy")})
    return [*source_rows, *recovered], {"contract": CASH_GAP_RECOVERY_CONTRACT,
        "reconstructed_at": reconstructed_at.isoformat(timespec="microseconds"),
        "generated_rows": len(recovered), "sessions": proofs,
        "source_price_or_execution_invented": False,
        "archived_terminal_fill_clock_resolutions": fill_clock_resolutions}


def validate_carried_endpoint_accounting(
    source_rows, positions, *, state, fill_rows, start, end, collect_failures=False,
    repair_opening_targets: set[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    """Check retained endpoint clocks and inventory before loading prices.

    This lightweight source/accounting gate cannot certify minute valuation
    or source coverage. It prevents a late endpoint failure after an expensive
    full-year price reconstruction and uses the identical inventory rules.
    """
    selected = {(r["market"], r["minute"]): r for r in source_rows
                if _in_range(r["session_date"], start, end)}
    days = sorted({row["session_date"] for row in selected.values()})
    if not days:
        raise RuntimeError("carried endpoint validation requires recorded session dates")
    checked = reconciled = 0
    fill_clock_resolutions = []
    failures, repair_candidates = [], []
    targets = set(repair_opening_targets or ())
    for market, mode in state["modes"].items():
        metadata = {p["position_id"]: p for modes in positions.values()
                    for p in modes.get(market, [])}
        fills = [f for f in fill_rows if f["market"] == market
                 and date.fromisoformat(str(f["session_date"])) <= end]
        if any(f["session_date"] < days[0] for f in fills):
            raise RuntimeError("carried minute reconstruction requires the full accepted ledger horizon")
        events = [*(f | {"clock": str(f.get("fill_at") or f["recorded_at"])} for f in fills),
                  *(a | {"clock": a["recorded_at"], "share_action": True}
                    for a in mode.get("share_replacement_ledger", []))]
        inventory = _ImmutableMinuteInventory(metadata, events, market=market)
        fill_clock_resolutions.extend(inventory.fill_clock_resolutions)
        prices, price_times = {}, {}
        for day in days:
            new_entries = set()
            claims = [item for item in mode.get("corporate_action_ledger", [])
                      if item["ex_date"] <= day]
            earned = sum(float(item["amount_twd"]) for item in claims)
            costs = mode.get("carry_cost_ledger", [])
            charges = sum(float(item["amount_twd"]) for item in costs
                          if item["date"] < day or
                          (item["date"] == day and not _is_terminal_carry_cost(item)))
            terminal_charges = sum(float(item["amount_twd"]) for item in costs
                                   if item["date"] == day and _is_terminal_carry_cost(item))
            for label in ("09:01", "13:30"):
                key = f"{day}T{label}+08:00"
                existing = selected.get((market, key))
                if existing is None:
                    if not collect_failures:
                        raise RuntimeError(f"missing accepted carried endpoint: {market}:{key}")
                    failures.append({"market": market, "minute": key, "error": "missing_endpoint"})
                    continue
                cutoff = _clock_instant(f"{day}T{label}:00+08:00")
                cutoff = max(cutoff, _clock_instant(existing.get("recorded_at") or cutoff))
                try:
                    reconciled += _advance_endpoint_with_recorded_precision(
                        inventory, cutoff, existing, prices, price_times, new_entries)
                except RuntimeError as exc:
                    if not collect_failures or not str(exc).startswith("ambiguous second-precision endpoint"):
                        raise
                    failures.append({"market": market, "minute": key, "error": str(exc)})
                actual = {
                    "cumulative_realized_net_pnl_twd": inventory.realized,
                    "open_position_count": sum(bool(p.get("signed_shares"))
                                               for p in inventory.positions.values()),
                    "cumulative_carry_cost_twd": charges + (terminal_charges if label == "13:30" else 0.),
                    "cumulative_corporate_action_net_twd": earned,
                }
                differences = {field: value for field, value in actual.items()
                               if not math.isclose(float(existing.get(field) or 0), float(value),
                                                   rel_tol=1e-12, abs_tol=1e-6)}
                if _eligible_replayed_opening_repair(existing, market=market, key=key,
                                                       targets=targets, mismatched_fields=set(differences)):
                    repair_candidates.append({"market": market, "session_date": day,
                                              "minute": key, "differences": differences})
                else:
                    for field, value in differences.items():
                        if not collect_failures:
                            raise RuntimeError(f"carried endpoint accounting disagrees: {market}:{key}:{field}")
                        failures.append({
                            "market": market, "minute": key, "field": field,
                            "recorded_at": existing.get("recorded_at"),
                            "retained_value": existing.get(field), "journal_value": value,
                            "historical_minute_replay": existing.get("historical_minute_replay"),
                            "opening_mark_revalued_from_completed_bar": existing.get("opening_mark_revalued_from_completed_bar"),
                        })
                checked += 1
    result = {"status": "failed_component_check" if failures else "validated_component_only",
              "endpoints_checked": checked,
              "second_precision_endpoint_reconciliations": reconciled,
              "archived_terminal_fill_clock_resolutions": fill_clock_resolutions,
              "price_and_minute_coverage_validated": False}
    if collect_failures:
        result["failures"] = failures
    if repair_candidates:
        result["repair_candidates"] = repair_candidates
        if not failures:
            result["status"] = "repair_candidates_identified_not_applied"
    if targets != {(item["market"], item["session_date"]) for item in repair_candidates}:
        raise RuntimeError("scoped opening correction target was missing, unchanged, or ineligible")
    return result


def rebuild_carried_strategy_marks(
    source_rows,
    positions,
    store,
    *,
    state,
    fill_rows,
    start,
    end,
    preserve_sourced=True,
    revalue_opening_marks=False,
    opening_valuation_prices: Mapping[str, Mapping[str, float]] | None = None,
    repair_opening_targets: set[tuple[str, str]] | None = None,
    provisional_opening_cost_targets: set[tuple[str, str, str]] | None = None,
    provisional_opening_first_bar_targets: set[tuple[str, str, str]] | None = None,
    progress=None,
):
    """Revalue an immutable carried fill/action book, never re-execute trades.

    Unlike the flat-session path, inventory, cost basis, unallocated entry
    fees, cash claims and financing charges all survive the session boundary.
    Accepted endpoint accounting is independently checked. The 13:30 NAV is
    preserved; explicit opening repair revalues 09:01 from the completed bar.
    Interior prices come only from retained completed-minute trades.
    """
    selected = {(r["market"], r["minute"]): r for r in source_rows
                if _in_range(r["session_date"], start, end)}
    opening_valuation_prices = opening_valuation_prices or {}
    days = sorted({r["session_date"] for r in selected.values()})
    output = [r for r in source_rows if not _in_range(r["session_date"], start, end)]
    legacy_retry_rows = 0
    precision_reconciliations = 0
    fill_clock_resolutions = []
    repair_targets = set(repair_opening_targets or ())
    if repair_targets and not revalue_opening_marks:
        raise ValueError("scoped opening repair requires completed-bar opening revaluation")
    repaired_openings = []
    if provisional_opening_cost_targets and provisional_opening_first_bar_targets:
        raise ValueError("choose one explicit provisional opening reference policy")
    first_bar_policy = bool(provisional_opening_first_bar_targets)
    assumption = opening_assumption_policy(first_bar_policy)
    cost_targets = set(provisional_opening_first_bar_targets or provisional_opening_cost_targets or ())
    applied_cost_targets = set()
    cost_valuations = []
    if cost_targets and not revalue_opening_marks:
        raise ValueError("provisional opening cost valuation requires explicit opening revaluation")
    for market, mode in state["modes"].items():
        metadata = {p["position_id"]: p for modes in positions.values() for p in modes.get(market, [])}
        # Position metadata above is scoped to the requested end date.  A
        # newer, already recorded session is neither an orphan within this
        # valuation nor permission to include future inventory; retain its
        # original journal bytes and exclude it only from this read model.
        fills = [f for f in fill_rows if f["market"] == market
                 and date.fromisoformat(str(f["session_date"])) <= end]
        if any(f["session_date"] < days[0] for f in fills):
            raise RuntimeError("carried minute reconstruction requires the full accepted ledger horizon")
        events = [*(f | {"clock": str(f.get("fill_at") or f["recorded_at"])} for f in fills),
                  *(a | {"clock": a["recorded_at"], "share_action": True}
                    for a in mode.get("share_replacement_ledger", []))]
        inventory = _ImmutableMinuteInventory(metadata, events, market=market)
        fill_clock_resolutions.extend(inventory.fill_clock_resolutions)
        legacy_retry_rows += len(inventory.legacy_orders)
        book, prices, price_times = inventory.positions, {}, {}
        # A new paper entry is a price for that cohort, not a market-wide
        # trade. Until a sourced minute print arrives, older carried cohorts
        # retain their own last observed mark.
        for day in days:
            new_entries_this_day = set()
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
                cutoff = _clock_instant(clock)
                if existing and key[11:16] in {"09:01", "13:30"}:
                    cutoff = max(cutoff, _clock_instant(existing.get("recorded_at") or cutoff))
                    precision_reconciliations += _advance_endpoint_with_recorded_precision(
                        inventory, cutoff, existing, prices, price_times, new_entries_this_day)
                else:
                    inventory.advance(cutoff, prices, price_times, new_entries_this_day)
                fresh = set()
                if key[11:16] == "09:01" and not revalue_opening_marks:
                    # Mirror _mark_mode exactly: only an explicitly retained
                    # valuation_price_0901 refreshes every cohort of a symbol.
                    # When it is absent, a new cohort carries its own fill and
                    # an older cohort keeps its previous observed mark.
                    for symbol, price in opening_valuation_prices.get(
                        day, {}
                    ).items():
                        prices[symbol], price_times[symbol] = float(price), minute
                        fresh.add(symbol)
                else:
                    for symbol, values in source.items():
                        if key in values:
                            prices[symbol], price_times[symbol] = float(values[key]), minute
                            fresh.add(symbol)
                cost_cohorts = []
                cost_overrides = {}
                if (key[11:16] == "09:01" and existing is not None
                        and (first_bar_policy or not historical_minute_mark_has_source(existing))):
                    for identity in sorted(new_entries_this_day):
                        p = book.get(identity)
                        if (not p or not p.get("signed_shares") or p["symbol"] in fresh
                                or (market, day, p["symbol"]) not in cost_targets):
                            continue
                        reference = store.first_observed_bar(p["symbol"], day) if first_bar_policy else None
                        if reference and reference["source_minute"] <= key:
                            raise RuntimeError("first-bar assumption is unnecessary when a 09:01 source exists")
                        cost_price = float(reference["price"] if reference else p["inventory_basis_price"])
                        if not math.isfinite(cost_price) or cost_price <= 0:
                            raise RuntimeError("provisional opening valuation lacks a valid immutable entry cost")
                        cost_overrides[identity] = cost_price
                        cost_cohorts.append({"position_id": identity, "symbol": p["symbol"],
                            "signed_shares": int(p["signed_shares"]),
                            "entry_cost_price": float(p["inventory_basis_price"]),
                            "remaining_entry_fee_twd": float(p["remaining_entry_fee_twd"]),
                            "entry_at": inventory.entry_fallbacks[identity][1].isoformat(),
                            **({"reference_bar": reference, "valuation_price": cost_price} if reference else {})})
                        applied_cost_targets.add((market, day, p["symbol"]))
                count, fresh_count, net, notional, fresh_notional = 0, 0, 0., 0., 0.
                for p in book.values():
                    if not p["signed_shares"]:
                        continue
                    symbol = p["symbol"]
                    mark_price = (cost_overrides[p["position_id"]] if p["position_id"] in cost_overrides
                                  else inventory.mark_price(p, prices, price_times))
                    valuation = dict(p)
                    if str(p.get("margin_converted_at") or "9999") > clock:
                        valuation.pop("margin_carry_contract", None)
                    net += position_net_liquidation_pnl(valuation, mark_price)
                    count += 1
                    amount = abs(p["signed_shares"] * mark_price)
                    notional += amount
                    if symbol in fresh:
                        fresh_count += 1
                        fresh_notional += amount
                cost = charges + (terminal_charges if key[11:16] == "13:30" else 0.)
                initial = float(mode["initial_capital_twd"])
                row = {"recorded_at": clock, "minute": key, "session_date": day, "market": market,
                       "initial_capital_twd": initial, "cumulative_realized_net_pnl_twd": inventory.realized,
                       "open_net_liquidation_pnl_twd": net, "total_equity_twd": initial + inventory.realized + net + earned - cost,
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
                    fields = ("cumulative_realized_net_pnl_twd", "open_position_count", "cumulative_carry_cost_twd", "cumulative_corporate_action_net_twd")
                    differences = {field for field in fields
                                   if not math.isclose(float(existing.get(field) or 0), float(row[field]),
                                                       rel_tol=1e-12, abs_tol=1e-6)}
                    repair_opening = _eligible_replayed_opening_repair(
                        existing, market=market, key=key, targets=repair_targets,
                        mismatched_fields=differences)
                    if differences and not repair_opening:
                        for field in fields:
                            if field in differences:
                                raise RuntimeError(f"carried endpoint accounting disagrees: {market}:{key}:{field}")
                    if repair_opening:
                        missing_open = sorted({p["symbol"] for p in book.values()
                                               if p.get("signed_shares") and p["symbol"] not in fresh})
                        if missing_open:
                            raise RuntimeError(f"missing completed 09:01 valuation bar for {day}:{market}:{missing_open}")
                        prior = {field: existing.get(field) for field in sorted(differences)}
                        row["opening_mark_revalued_from_completed_bar"] = True
                        row["historical_opening_endpoint_repair"] = {
                            "contract": "scoped_replayed_opening_from_immutable_fills_v1",
                            "prior_recorded_at": existing.get("recorded_at"),
                            "prior_accounting": prior,
                            "book_cutoff_at": clock,
                            "all_open_positions_have_completed_minute_price": True,
                            "simulation_only": True, "broker_fill": False, "exchange_fill": False,
                        }
                        repaired_openings.append({"market": market, "session_date": day,
                                                  "prior_accounting": prior,
                                                  "reconstructed_accounting": {field: row[field] for field in sorted(differences)}})
                    elif (key[11:16] == "09:01" and revalue_opening_marks
                            and (cost_cohorts or not historical_minute_mark_has_source(existing))):
                        missing_open = sorted({book[identity]["symbol"] for identity in new_entries_this_day
                                               if identity in book and book[identity]["symbol"] not in fresh
                                               and identity not in cost_overrides})
                        if missing_open:
                            raise RuntimeError(
                                f"missing completed 09:01 valuation bar for {day}:{market}:{missing_open}"
                            )
                        row["opening_mark_revalued_from_completed_bar"] = not bool(cost_cohorts)
                        row["accepted_endpoint_accounting_verified"] = True
                    else:
                        row = dict(existing)
                        row["accepted_endpoint_accounting_verified"] = True
                elif preserve_sourced and existing and historical_minute_mark_has_source(existing):
                    row = dict(existing)
                if cost_cohorts:
                    disclosure = {"contract": assumption["contract"],
                        "market": market, "session_date": day, "minute": key,
                        "book_cutoff_at": cutoff.isoformat(), "cohorts": cost_cohorts,
                        "market_price_verified": False, "execution_price": False,
                        "broker_fill": False, "exchange_fill": False,
                        "source_gaps_may_change_results": True,
                        **({"uses_later_observation": True,
                            "causal_0901_market_observation": False} if first_bar_policy else {})}
                    # Keep the accepted endpoint's true clock. The minute key
                    # is its retained bucket, not permission to backdate fills.
                    row["recorded_at"] = existing["recorded_at"]
                    row[assumption["key"]] = disclosure
                    row["valuation_stale"] = True
                    row["provisional_minute_source_coverage"] = True
                    row["source_gaps_may_change_results"] = True
                    row["valuation_source"] = assumption["contract"]
                    cost_valuations.append(disclosure)
                output.append(row)
            if progress is not None:
                progress("retained_account_valuation", market=market, session_date=day,
                         generated_rows=len(output))
    if repair_targets != {(item["market"], item["session_date"]) for item in repaired_openings}:
        raise RuntimeError("scoped opening correction target was missing, unchanged, or ineligible")
    if cost_targets != applied_cost_targets:
        raise RuntimeError("provisional opening cost scope was missing, sourced, or ineligible")
    output.sort(key=lambda r: (r["minute"], r["market"]))
    _, stats = validate_existing_strategy_marks(output, start=start, end=end)
    stats["carried_fill_book_revalued_without_execution"] = True
    stats["second_precision_endpoint_reconciliations"] = precision_reconciliations
    stats["archived_terminal_fill_clock_resolutions"] = fill_clock_resolutions
    stats["repaired_opening_endpoints"] = repaired_openings
    stats["legacy_paper_entry_retry_rows_reclassified_for_valuation"] = legacy_retry_rows
    if cost_targets:
        cost_valuations.sort(key=lambda row: (row["minute"], row["market"]))
        stats[assumption["key"]] = {
            "contract": assumption["contract"],
            "authorized_targets": [list(item) for item in sorted(cost_targets)],
            "applied": cost_valuations, "financial_journals_changed": False,
            "source_gaps_may_change_results": True}
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


def validate_provisional_opening_cost_valuations(
    rows, positions, *, state, fill_rows, authorization, source_rows,
    first_bar: bool = False, store=None,
):
    """Recompute scoped assumed cost references from the original fill book.

    This certifies the disclosed reference basis and its original endpoint
    clock, not missing market prices or historical executability.
    """
    policy = opening_assumption_policy(first_bar)
    if first_bar and store is None:
        raise ValueError("first-bar readback requires an independent receipt-verified source store")
    targets = {tuple(item) for item in authorization.get("authorized_targets", [])}
    if (authorization.get("contract") != policy["contract"]
            or not targets or any(len(item) != 3 for item in targets)):
        raise ValueError("invalid provisional opening cost authorization")
    before = {(r["market"], r["minute"]): r for r in source_rows}
    affected = [r for r in rows if r.get(policy["key"]) is not None]
    expected_points = {(market, f"{day}T09:01+08:00") for market, day, _ in targets}
    if {(r["market"], r["minute"]) for r in affected} != expected_points or len(affected) != len(expected_points):
        raise ValueError("provisional opening cost rows differ from authorized scope")
    verified = set()
    for row in affected:
        market, minute = row["market"], row["minute"]
        original = before[(market, minute)]
        cutoff = max(_clock_instant(minute), _clock_instant(original["recorded_at"]))
        metadata = {p["position_id"]: p for modes in positions.values() for p in modes.get(market, [])}
        fills = [f | {"clock": str(f.get("fill_at") or f["recorded_at"])}
                 for f in fill_rows if f["market"] == market
                 and str(f["session_date"]) <= str(row["session_date"])]
        events = [*fills, *(a | {"clock": a["recorded_at"], "share_action": True}
                           for a in state["modes"][market].get("share_replacement_ledger", []))]
        inventory = _ImmutableMinuteInventory(metadata, events, market=market)
        _advance_endpoint_with_recorded_precision(inventory, cutoff, original, {}, {}, set())
        cohorts = []
        for identity, p in sorted(inventory.positions.items()):
            target = (market, row["session_date"], p["symbol"])
            if target not in targets or not p.get("signed_shares"):
                continue
            entered = inventory.entry_fallbacks[identity][1]
            if entered.astimezone(TAIPEI).date().isoformat() != row["session_date"]:
                raise ValueError("provisional cost valuation cannot reset an older carried cohort")
            reference = store.first_observed_bar(p["symbol"], row["session_date"]) if first_bar else None
            if reference and reference["source_minute"] <= minute:
                raise ValueError("a later first-bar assumption cannot replace a valid 09:01 observation")
            cohorts.append({"position_id": identity, "symbol": p["symbol"],
                "signed_shares": int(p["signed_shares"]),
                "entry_cost_price": float(p["inventory_basis_price"]),
                "remaining_entry_fee_twd": float(p["remaining_entry_fee_twd"]),
                "entry_at": entered.isoformat(),
                **({"reference_bar": reference, "valuation_price": reference["price"]} if reference else {})})
            verified.add(target)
        expected = {"contract": policy["contract"],
            "market": market, "session_date": row["session_date"], "minute": minute,
            "book_cutoff_at": cutoff.isoformat(), "cohorts": cohorts,
            "market_price_verified": False, "execution_price": False,
            "broker_fill": False, "exchange_fill": False,
            "source_gaps_may_change_results": True,
            **({"uses_later_observation": True, "causal_0901_market_observation": False} if first_bar else {})}
        if (not cohorts or row[policy["key"]] != expected
                or row.get("recorded_at") != original["recorded_at"]
                or row.get("valuation_source") != policy["contract"]
                or row.get("valuation_stale") is not True
                or row.get("valuation_executable") is not False
                or row.get("provisional_minute_source_coverage") is not True
                or row.get("source_gaps_may_change_results") is not True
                or row.get("opening_mark_revalued_from_completed_bar") is not False):
            raise ValueError("provisional opening valuation differs from its immutable cost, clock or disclosure")
    if targets != verified or authorization.get("applied") != [
            r[policy["key"]] for r in affected]:
        raise ValueError("provisional opening cost receipt differs from independently verified rows")
    if first_bar:
        store.assert_sources_unchanged()
    return {"contract": policy["contract"], "verified_symbol_points": len(verified),
            "market_observation_verified": False, "financial_journals_changed": False,
            **({"later_bar_source_verified": True} if first_bar else {})}


def first_bar_valuation_store(receipt: Mapping[str, Any]) -> MinutePriceStore:
    """Reopen actual native sources for independent assumption readback."""
    config = receipt.get("first_bar_valuation_sources") or {}
    if (config.get("contract") != "receipt_verified_local_first_bar_reference_v1"
            or not isinstance(config.get("kbar_roots"), list)
            or not isinstance(config.get("cache_roots"), list)):
        raise ValueError("first-bar independent readback lacks its source configuration")
    return MinutePriceStore([Path(p) for p in config["kbar_roots"]],
        [Path(p) for p in config["cache_roots"]], require_receipts=True,
        finmind_root=Path(config["finmind_root"]) if config.get("finmind_root") else None)


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
            entries = sum(int(f["quantity"]) for f in pf if _is_entry_inventory_fill(f))
            exits = sum(int(f["quantity"]) for f in pf if not _is_entry_inventory_fill(f))
            if entries <= 0 or entries != exits or entries != int(position["filled_shares"]):
                raise RuntimeError("incomplete or duplicated position fill lifecycle")
            close(math.fsum(number(f["net_pnl_twd"]) for f in pf if not _is_entry_inventory_fill(f)),
                  position["realized_net_pnl_twd"])
        exit_fills = [f for f in session_fills if not _is_entry_inventory_fill(f)]
        realized_between = math.fsum(number(f["net_pnl_twd"]) for f in exit_fills
                                     if first_at < timestamp(f["recorded_at"]) <= anchor_at)
        close(number(first["cumulative_realized_net_pnl_twd"]) + realized_between,
              anchor["cumulative_realized_net_pnl_twd"])
        for mark, at in ((first, first_at), (anchor, anchor_at)):
            remaining = defaultdict(int)
            for f in session_fills:
                if timestamp(f["recorded_at"]) <= at:
                    remaining[f["position_id"]] += int(f["quantity"]) * (1 if _is_entry_inventory_fill(f) else -1)
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
    parser.add_argument(
        "--finmind-minute-root", type=Path, default=Path("data_finmind/complement"),
        help="Receipt-verified local TaiwanStockKBar backup; valuation only, never executable capacity.",
    )
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument(
        "--expected-session-date", action="append",
        help="Exact completed session set from the maintenance owner; repeatable with --expected-market.",
    )
    parser.add_argument(
        "--expected-market", action="append",
        help="Exact active mode set; reject a differently scoped reconstruction before publication.",
    )
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
    parser.add_argument("--repair-replayed-opening-endpoint", action="append", default=[],
                        metavar="MARKET:YYYY-MM-DD",
                        help="Stage an explicitly selected inconsistent historical 09:01 projection; never publish directly.")
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
    parser.add_argument("--preserve-addition-component", action="store_true",
                        help="Revalue retained accounts while preserving the independently accepted added mode's distinct economics.")
    parser.add_argument("--recover-proven-cash-sessions", action="store_true",
                        help="Stage only ledger-proven missing cash valuations; never reconstruct a missing trade.")
    parser.add_argument("--provisional-opening-cost-valuation", action="append", default=[],
                        metavar="MARKET:YYYY-MM-DD:SYMBOL",
                        help="Explicitly authorized isolated 09:01 valuation at immutable entry cost; never a quote or fill.")
    parser.add_argument("--provisional-opening-first-bar-valuation", action="append", default=[],
                        metavar="MARKET:YYYY-MM-DD:SYMBOL",
                        help="Isolated retrospective 09:01 valuation at a later first observed bar; disclose its actual clock, never change executions.")
    parser.add_argument("--complete-opening-prices-from-local-backup", action="store_true",
                        help="Isolated valuation only: fill missing 09:01 from verified local roots/FinMind, never a proxy or execution.")
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


def _replay_minute_source_hashes(
    state_dir: Path,
    *,
    start: date,
    end: date,
    proven_flat_sessions: frozenset[str] = frozenset(),
    provisional_no_source_sessions: frozenset[str] = frozenset(),
) -> dict[tuple[str, str], str]:
    """Load the exact minute files used by the immutable execution replay.

    A receipt-backed collector chunk is sufficient for ordinary curve repair,
    but independent execution parity is stronger: it must read the same bytes
    that drove fills and bracket decisions.  New replay receipts therefore pin
    every selected partition/chunk by session and SHA-256.
    """

    receipt_path = state_dir / "rebuild_receipt.json"
    if not receipt_path.is_file():
        return {}
    receipt = _read_json(receipt_path)
    if receipt.get("contract") == "additive_independent_paper_mode_v1":
        # A union contains different original execution policies. Their own
        # component audits keep execution pins; this step independently values
        # unchanged accounts using verified collector receipts for all symbols.
        return {}
    output: dict[tuple[str, str], str] = {}
    replay_days: set[str] = set()
    for session in receipt.get("sessions") or ():
        day = str(session.get("session_date") or "")
        if not _in_range(day, start, end):
            continue
        intraday = session.get("intraday_replay") or {}
        if not intraday:
            continue
        files = intraday.get("source_files") or ()
        if not files:
            # An independent caller may prove from the complete fills and
            # 270 marked inventory rows that the entire session was flat and
            # had no pending work. No intraday position source exists to pin.
            # The default remains strict for ordinary history rebuilds.
            if day in proven_flat_sessions:
                continue
            # Only the independent auditor may supply these explicitly
            # validated assumption-only sessions. This is NOT a claim that
            # market trading or stops were absent.
            if (day in provisional_no_source_sessions
                    and receipt.get("provisional_minute_source_coverage") is True
                    and receipt.get("official_daily_close_reference_when_minute_missing") is True
                    and receipt.get("historical_open_fallback_assumption")):
                continue
            raise RuntimeError(
                f"execution replay has no hash-pinned minute sources: {day}"
            )
        replay_days.add(day)
        for item in files:
            path = Path(str(item.get("path") or "")).resolve()
            digest = str(item.get("sha256") or "")
            if not path.is_file() or len(digest) != 64:
                raise RuntimeError(
                    f"invalid execution replay minute-source pin: {day}:{path}"
                )
            key = (day, str(path))
            previous = output.setdefault(key, digest)
            if previous != digest:
                raise RuntimeError(
                    f"conflicting execution replay minute-source pin: {day}:{path}"
                )
    if replay_days and not output:
        raise RuntimeError("execution replay minute-source pins are empty")
    return output


def _replay_minute_symbol_sources(
    state_dir: Path,
    *,
    start: date,
    end: date,
) -> dict[tuple[str, str], frozenset[str]] | None:
    """Return exact per-symbol source pins from a current replay receipt.

    Older immutable replay receipts pinned file hashes but did not record which
    symbols selected each file. They remain reproducible with ordered-source
    resolution, but only a current receipt can forbid all lower-priority
    symbol/session fallbacks. Mixed old/new receipts are invalid.
    """

    receipt_path = state_dir / "rebuild_receipt.json"
    if not receipt_path.is_file():
        return None
    receipt = _read_json(receipt_path)
    if receipt.get("contract") == "additive_independent_paper_mode_v1":
        return None  # Do not restrict retained modes to the new mode's symbols.
    output: dict[tuple[str, str], set[str]] = defaultdict(set)
    saw_current = False
    saw_legacy = False
    for session in receipt.get("sessions") or ():
        day = str(session.get("session_date") or "")
        if not _in_range(day, start, end):
            continue
        intraday = session.get("intraday_replay") or {}
        files = intraday.get("source_files") or ()
        if not files:
            continue
        session_symbols: set[str] = set()
        session_current = False
        for item in files:
            symbols = item.get("symbols")
            if symbols is None:
                saw_legacy = True
                continue
            saw_current = True
            session_current = True
            path = str(Path(str(item.get("path") or "")).resolve())
            declared_count = int(item.get("symbol_count") or 0)
            normalized = [str(symbol).strip() for symbol in symbols]
            if (
                any(not symbol for symbol in normalized)
                or len(normalized) != len(set(normalized))
                or declared_count != len(normalized)
            ):
                raise RuntimeError(
                    f"invalid replay minute source symbol pin: {day}:{path}"
                )
            for symbol in normalized:
                if symbol in session_symbols:
                    raise RuntimeError(
                        f"replay symbol selected multiple minute sources: {day}:{symbol}"
                    )
                session_symbols.add(symbol)
                output[(day, symbol)].add(path)
        if session_current and int(intraday.get("resolved_symbols") or 0) != len(
            session_symbols
        ):
            raise RuntimeError(
                f"replay minute source symbol cardinality mismatch: {day}"
            )
    if saw_current and saw_legacy:
        raise RuntimeError("mixed legacy/current replay minute source pins")
    if not saw_current:
        return None
    return {key: frozenset(paths) for key, paths in output.items()}


def _replay_opening_valuation_prices(
    state_dir: Path,
    *,
    start: date,
    end: date,
) -> dict[str, dict[str, float]]:
    """Load the exact 09:01 valuation prices retained by the replay.

    Execution VWAP and the completed-minute valuation price are distinct. The
    replay intentionally leaves ``valuation_price_0901`` null when no accepted
    09:01 valuation print exists. In that case a new cohort keeps its fill price
    and older inventory keeps its preceding mark until the next positive-volume
    minute. Reading the immutable entry book preserves that distinction in the
    independent curve reconstruction.
    """

    receipt_path = state_dir / "rebuild_receipt.json"
    if not receipt_path.is_file():
        return {}
    receipt = _read_json(receipt_path)
    state_root = state_dir.resolve()
    if receipt.get("contract") == "additive_independent_paper_mode_v1":
        # The caller requests sourced opening revaluation, not a global price
        # book borrowed from one component's historical execution policy.
        return {}
    output: dict[str, dict[str, float]] = {}
    selected_days: set[str] = set()
    for session in receipt.get("sessions") or ():
        day = str(session.get("session_date") or "")
        if not _in_range(day, start, end):
            continue
        if day in selected_days:
            raise RuntimeError(f"duplicate replay session receipt: {day}")
        selected_days.add(day)
        book = session.get("historical_entry_books") or {}
        recorded_path = Path(str(book.get("path") or ""))
        expected = (
            state_root / "replay_entry_books" / f"{day}.parquet"
        ).resolve()
        digest = str(book.get("sha256") or "")
        # Atomic promotion/retained replay copies move the state directory.
        # The recorded location remains provenance; read only the canonical
        # local book and require its original content hash. Never follow an
        # external path from a receipt or fall back to an old source directory.
        path = expected
        if (
            recorded_path.name != f"{day}.parquet"
            or recorded_path.parent.name != "replay_entry_books"
            or not path.is_relative_to(state_root)
            or not path.is_file()
            or len(digest) != 64
        ):
            raise RuntimeError(
                f"invalid replay entry-book pin for opening valuation: {day}:{path}"
            )
        if _sha256(path) != digest:
            raise RuntimeError(
                f"replay entry book changed before opening valuation: {day}:{path}"
            )
        schema = pl.read_parquet_schema(path)
        required = {"symbol", "valuation_price_0901"}
        if not required.issubset(schema):
            raise RuntimeError(
                f"replay entry book lacks opening valuation columns: {day}:{path}"
            )
        frame = pl.read_parquet(path, columns=sorted(required))
        symbols = frame.get_column("symbol").cast(pl.String)
        if symbols.n_unique() != frame.height:
            raise RuntimeError(f"duplicate replay entry-book symbol: {day}:{path}")
        prices: dict[str, float] = {}
        for symbol, raw_price in frame.iter_rows():
            normalized_symbol = str(symbol or "").strip()
            if not normalized_symbol:
                raise RuntimeError(f"blank replay entry-book symbol: {day}:{path}")
            if raw_price is None:
                continue
            price = float(raw_price)
            if not math.isfinite(price) or price <= 0:
                raise RuntimeError(
                    f"invalid replay 09:01 valuation price: {day}:{normalized_symbol}"
                )
            prices[normalized_symbol] = price
        output[day] = prices
    return output


def _assert_minute_publication_window(state_dir: Path, now: datetime) -> None:
    if not datetime_time(8, 30) <= now.time() < datetime_time(14, 31):
        return
    with (state_dir / ".engine.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("live engine is in its protected window; defer minute publication") from exc


def accepted_addition_curve_component(state_dir, source_marks, *, start, end):
    """Keep the new component's separately audited execution/cost ABI intact."""
    from stockagent.live.day_trade_addition import CONTRACT, evidence_path
    from scripts.promote_tw_day_trade_replay import _validate_rebuild
    receipt = _read_json(state_dir / "rebuild_receipt.json")
    if receipt.get("contract") != CONTRACT:
        raise ValueError("component preservation requires an isolated additive ledger")
    market = receipt["added_market"]
    component = evidence_path(state_dir, receipt["new_mode_component"])
    authorization = receipt.get("provisional_research_publication") or {}
    if authorization and authorization.get("market") != market:
        raise ValueError("component authorization targets a different market")
    accepted = _validate_rebuild(component, expected_markets={market},
        allow_provisional_research_market=market if authorization else None)
    dates = accepted["session_dates"]
    if dates[0] != str(start) or dates[-1] != str(end):
        raise ValueError("preserved component range differs from minute reconstruction")
    state, component_state = _read_json(state_dir / "state.json"), _read_json(component / "state.json")
    if state["modes"].get(market) != component_state["modes"].get(market):
        raise ValueError("preserved component account changed")
    preserved = _read_jsonl(component / "marks.jsonl")
    candidate_rows = [row for row in source_marks if row.get("market") == market]
    order = lambda row: (str(row.get("minute")), str(row.get("market")))
    if sorted(candidate_rows, key=order) != sorted(preserved, key=order):
        raise ValueError("new mode minute rows differ from the independent component")
    proof = {"market": market, "component": receipt["new_mode_component"],
        "marks_sha256": _sha256(component / "marks.jsonl"),
        "rebuild_receipt_sha256": accepted["rebuild_receipt_sha256"],
        "state_sha256": accepted["state_sha256"], "preserved_rows": len(preserved)}
    return market, preserved, proof


def main() -> None:
    args = parse_args()
    args.revalue_carried_marks = bool(getattr(args, "revalue_carried_marks", False))
    args.require_revaluation_parity = bool(getattr(args, "require_revaluation_parity", False))
    if args.require_revaluation_parity and not (args.revalue_carried_marks and args.recompute_existing_strategy_marks):
        raise ValueError("parity verification requires full independent carried revaluation")
    if (args.revalue_opening_marks or args.recompute_existing_strategy_marks) and args.validate_existing_strategy_marks:
        raise ValueError("opening revaluation cannot be combined with preserve-only validation")
    repair_targets: set[tuple[str, str]] = set()
    for value in getattr(args, "repair_replayed_opening_endpoint", ()):
        try:
            market, day = value.rsplit(":", 1)
            if not market.strip() or date.fromisoformat(day).isoformat() != day:
                raise ValueError
        except (AttributeError, ValueError) as exc:
            raise ValueError("opening correction target must be MARKET:YYYY-MM-DD") from exc
        if (market, day) in repair_targets:
            raise ValueError("duplicate opening correction target")
        repair_targets.add((market, day))
    if repair_targets and (not args.revalue_carried_marks or not args.revalue_opening_marks
                           or not args.simulation or args.publish):
        raise ValueError("scoped opening correction requires isolated simulation and carried/opening revaluation; direct publication is forbidden")
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if start > end:
        raise ValueError("start date must not be after end date")
    expected_scope = _expected_minute_scope(args, start=start, end=end)
    complete_opening = bool(getattr(args, "complete_opening_prices_from_local_backup", False))
    if complete_opening and (not args.simulation or args.publish or not expected_scope
                             or not args.revalue_carried_marks or args.require_revaluation_parity):
        raise ValueError("opening price backup requires isolated exact-scope carried valuation, never direct publication/parity claims")
    cost_targets = set()
    for value in getattr(args, "provisional_opening_cost_valuation", ()):
        try:
            market, day, symbol = value.rsplit(":", 2)
            target = (market, day, symbol)
            if (not market.strip() or not symbol.strip() or date.fromisoformat(day).isoformat() != day
                    or not start <= date.fromisoformat(day) <= end or target in cost_targets):
                raise ValueError
        except (AttributeError, ValueError) as exc:
            raise ValueError("opening cost valuation target must be unique MARKET:YYYY-MM-DD:SYMBOL in range") from exc
        cost_targets.add(target)
    if cost_targets and (not args.simulation or args.publish or not expected_scope
                         or not args.revalue_carried_marks or not args.revalue_opening_marks
                         or args.require_revaluation_parity):
        raise ValueError("provisional opening cost requires isolated exact-scope carried revaluation, no publish or parity claim")
    first_bar_targets = set()
    for value in getattr(args, "provisional_opening_first_bar_valuation", ()):
        try:
            market, day, symbol = value.rsplit(":", 2)
            target = (market, day, symbol)
            if (not market.strip() or not symbol.strip() or date.fromisoformat(day).isoformat() != day
                    or not start <= date.fromisoformat(day) <= end or target in first_bar_targets):
                raise ValueError
        except (AttributeError, ValueError) as exc:
            raise ValueError("first-bar valuation target must be unique MARKET:YYYY-MM-DD:SYMBOL in range") from exc
        first_bar_targets.add(target)
    if first_bar_targets and (cost_targets or not args.simulation or args.publish or not expected_scope
                              or not args.revalue_carried_marks or not args.revalue_opening_marks
                              or args.require_revaluation_parity):
        raise ValueError("first-bar assumption requires isolated exact-scope valuation, no cost policy, publication or causal parity claim")
    preserve_component = bool(getattr(args, "preserve_addition_component", False))
    recover_cash = bool(getattr(args, "recover_proven_cash_sessions", False))
    if (preserve_component or recover_cash) and (
            not args.simulation or args.publish or not expected_scope or not args.revalue_carried_marks):
        raise ValueError("additive component/cash recovery requires isolated simulation, exact scope and no direct publication")
    if expected_scope is not None and getattr(args, "repair_terminal_only", False):
        raise ValueError("full minute scope cannot be validated by terminal-only repair")
    source_hashes = {name: _sha256(args.state_dir / name) for name in
                     ("marks.jsonl", "fills.jsonl", "orders.jsonl", "benchmark_history.json")
                     if (args.state_dir / name).exists()}
    if recover_cash:
        source_hashes.update({name: _sha256(args.state_dir / name)
                              for name in ("state.json", "events.jsonl")})
    source_marks = _read_jsonl(args.state_dir / "marks.jsonl")
    full_source_marks = source_marks
    work_state = _read_json(args.state_dir / "state.json") if (preserve_component or recover_cash) else None
    preserved_rows, component_proof = [], None
    if preserve_component:
        added, preserved_rows, component_proof = accepted_addition_curve_component(
            args.state_dir, source_marks, start=start, end=end)
        source_marks = [row for row in source_marks if row.get("market") != added]
        work_state = {**work_state, "modes": {m: row for m, row in work_state["modes"].items() if m != added}}
    has_margin_carry = any(row.get("margin_carry_contract") for row in source_marks)
    accounting_signature = _carried_accounting_signature(args.state_dir, end) if args.revalue_carried_marks else None
    if args.revalue_carried_marks and (not has_margin_carry or args.validate_existing_strategy_marks):
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
    progress_started = time.perf_counter()
    progress_state = {"phase": None, "written_at": 0.0}
    stage_seconds: dict[str, float] = {}

    def progress(phase, **facts):
        now = time.perf_counter()
        prior = progress_state["phase"]
        if phase != prior:
            if prior is not None:
                stage_seconds[prior] = stage_seconds.get(prior, 0.0) + now - progress_state["phase_started"]
            progress_state["phase"], progress_state["phase_started"] = phase, now
        elif now - progress_state["written_at"] < 5.0:
            return
        progress_state["written_at"] = now
        _atomic_json(args.output_dir / "minute_curve_progress.json", {
            "contract": "minute_curve_rebuild_phase_timing_v1", "phase": phase,
            "observed_at": datetime.now(TAIPEI).isoformat(), "elapsed_seconds": now - progress_started,
            "completed_stage_seconds": stage_seconds, **facts,
            "formal_promotion_completed": False, "progress_is_acceptance": False})

    progress("load_retained_inventory")
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
    replay_source_hashes = (
        _replay_minute_source_hashes(args.state_dir, start=start, end=end)
        if has_margin_carry and args.revalue_carried_marks
        else {}
    )
    replay_symbol_sources = (
        _replay_minute_symbol_sources(args.state_dir, start=start, end=end)
        if has_margin_carry and args.revalue_carried_marks
        else None
    )
    opening_valuation_prices = (
        _replay_opening_valuation_prices(args.state_dir, start=start, end=end)
        if has_margin_carry and args.revalue_carried_marks
        else {}
    )
    store = MinutePriceStore(
        local_roots,
        local_cache_roots,
        require_receipts=has_margin_carry,
        replay_source_hashes=replay_source_hashes,
        replay_symbol_sources=replay_symbol_sources,
        finmind_root=getattr(args, "finmind_minute_root", None),
        complete_opening_prices=complete_opening,
    )
    if not recover_cash:
        early_missing = missing_accepted_endpoints(source_marks, start=start, end=end,
            expected_sessions=expected_scope[0] if expected_scope else None,
            expected_markets=(set(work_state["modes"]) if work_state is not None else
                              expected_scope[1] if expected_scope else None))
        if early_missing:
            raise RuntimeError(f"missing accepted endpoints before minute-data preparation: {early_missing[:20]}")
    if work_state is None:
        work_state = _read_json(args.state_dir / "state.json")
    source_fills = _read_jsonl(args.state_dir / "fills.jsonl")
    positions = load_positions(args.state_dir, start=start, end=end, fill_rows=source_fills)
    if preserve_component:
        source_fills = [row for row in source_fills if row.get("market") != added]
        positions = {day: {m: values for m, values in modes.items() if m != added}
                     for day, modes in positions.items()}
    cash_recovery = None
    if recover_cash:
        progress("cash_session_proof")
        from stockagent.live.paper_journal import open_paper_journal
        with ExitStack() as stack:
            streams = [stack.enter_context(open_paper_journal(args.state_dir / name, "r"))
                       for name in ("orders.jsonl", "events.jsonl")]
            activity = (json.loads(line) for stream in streams for line in stream if line.strip())
            source_marks, cash_recovery = recover_proven_cash_session_marks(
                source_marks, positions, state=work_state, fill_rows=source_fills,
                activity_rows=activity, completed_sessions=expected_scope[0],
                expected_markets=set(work_state["modes"]), reconstructed_at=datetime.now(TAIPEI))
        with (args.state_dir / "marks.jsonl").open("rb") as original, (args.output_dir / "marks.before.jsonl").open("xb") as before:
            shutil.copyfileobj(original, before, 1024**2)
        if _sha256(args.output_dir / "marks.before.jsonl") != source_hashes["marks.jsonl"]:
            raise RuntimeError("cash recovery before-image differs from the accepted source")
    missing_endpoints = missing_accepted_endpoints(
        source_marks, start=start, end=end,
        expected_sessions=expected_scope[0] if expected_scope else None,
        expected_markets=set(work_state["modes"]) if expected_scope else None,
    )
    if missing_endpoints:
        raise RuntimeError(f"missing accepted endpoints before minute-data preparation: {missing_endpoints[:20]}")
    progress("endpoint_accounting_preflight")
    endpoint_preflight = (
        validate_carried_endpoint_accounting(
            source_marks, positions, state=work_state,
            fill_rows=source_fills, start=start, end=end, repair_opening_targets=repair_targets,
        ) if args.revalue_carried_marks else None
    )
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
    progress("prepare_receipt_backed_prices", required_symbol_days=sum(map(len, required.values())))
    store._source_progress = progress
    store.prepare(required)
    store._source_progress = None
    progress("price_coverage", finmind_read_metrics=getattr(store, "finmind_read_metrics", {}))
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
        progress("retained_account_valuation")
        rebuilt_marks, strategy_stats = rebuild_carried_strategy_marks(
            source_marks, positions, store, state=work_state,
            fill_rows=source_fills, start=start, end=end,
            preserve_sourced=not args.recompute_existing_strategy_marks,
            revalue_opening_marks=bool(args.revalue_opening_marks),
            opening_valuation_prices=opening_valuation_prices,
            repair_opening_targets=repair_targets,
            provisional_opening_cost_targets=cost_targets,
            provisional_opening_first_bar_targets=first_bar_targets,
            progress=progress,
        )
        if cost_targets:
            strategy_stats["provisional_opening_cost_independent_readback"] = (
                validate_provisional_opening_cost_valuations(
                    rebuilt_marks, positions, state=work_state, fill_rows=source_fills,
                    authorization=strategy_stats["provisional_opening_cost_valuation"],
                    source_rows=source_marks))
        if first_bar_targets:
            progress("independent_first_bar_readback")
            policy = opening_assumption_policy(True)
            checker = MinutePriceStore(store.kbar_roots, store.tick_minute_roots,
                require_receipts=True, finmind_root=store.finmind_root)
            strategy_stats[policy["readback"]] = validate_provisional_opening_cost_valuations(
                rebuilt_marks, positions, state=work_state, fill_rows=source_fills,
                authorization=strategy_stats[policy["key"]], source_rows=source_marks,
                first_bar=True, store=checker)
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
    if preserved_rows:
        rebuilt_marks.extend(preserved_rows)
        rebuilt_marks.sort(key=lambda row: (str(row.get("minute")), str(row.get("market"))))
        _rows, union_stats = validate_existing_strategy_marks(rebuilt_marks, start=start, end=end)
        strategy_stats = {**strategy_stats, **union_stats,
            "independently_preserved_addition_component": component_proof}
    source_benchmarks = _read_json(args.state_dir / "benchmark_history.json")
    if args.require_revaluation_parity and strategy_stats["differing_original_equity_points"]:
        _atomic_json(args.output_dir / "minute_revaluation_parity_failure.json", strategy_stats)
        _atomic_jsonl(args.output_dir / "unpromotable_revalued_marks.jsonl", rebuilt_marks)
        raise RuntimeError("independent carried minute valuation differs from replay; publication refused")
    progress("benchmark_valuation")
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
    progress("write_and_validate_outputs")
    _atomic_jsonl(marks_path, rebuilt_marks)
    _atomic_json(benchmark_path, rebuilt_benchmarks, compact=True)
    benchmark_sha256 = _sha256(benchmark_path)
    benchmark_projection = write_benchmark_projection(
        state_dir=args.output_dir,
        source_path=benchmark_path,
        source_sha256=benchmark_sha256,
        origins=rebuilt_benchmarks.get("origins") or {},
        marks=rebuilt_benchmarks.get("marks") or (),
        created_at=datetime.now(TAIPEI).isoformat(timespec="seconds"),
    )
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
        "opening_marks_revalued_at_completed_minute": bool(args.revalue_opening_marks and not (cost_targets or first_bar_targets)),
        "opening_marks_revalued_with_disclosed_cost_exceptions": bool(cost_targets),
        "opening_marks_revalued_with_disclosed_first_bar_exceptions": bool(first_bar_targets),
        **({"first_bar_valuation_sources": {
            "contract": "receipt_verified_local_first_bar_reference_v1",
            "kbar_roots": [str(p.resolve()) for p in store.kbar_roots],
            "cache_roots": [str(p.resolve()) for p in store.tick_minute_roots],
            "finmind_root": str(store.finmind_root) if store.finmind_root else None,
        }} if first_bar_targets else {}),
        "unchanged_fills_sha256": _sha256(args.state_dir / "fills.jsonl"),
        "source_ledger_hashes": source_hashes,
        "source_carried_accounting_signature": accounting_signature,
        "execution_replay_minute_source_pin_count": len(replay_source_hashes),
        "execution_replay_minute_symbol_source_pin_count": (
            len(replay_symbol_sources) if replay_symbol_sources is not None else 0
        ),
        "execution_replay_minute_symbol_sources_strict": (
            replay_symbol_sources is not None
        ),
        "execution_replay_opening_valuation_session_count": len(
            opening_valuation_prices
        ),
        "execution_replay_opening_valuation_symbol_count": sum(
            len(prices) for prices in opening_valuation_prices.values()
        ),
        "existing_bracket_aware_strategy_marks_preserved": bool(
            args.validate_existing_strategy_marks
        ),
        "historical_minute_marks_are_executable_quotes": False,
        "local_first_contract": (
            "ordered local KBar and research partitions first; canonical "
            "Shioaji KBar collector only for remaining symbol-date gaps"
        ),
        "finmind_valuation_sources": list(store.finmind_source_records.values()),
        "observed_opening_price_backups": list(store.opening_backup_records.values()),
        "finmind_read_metrics": dict(getattr(store, "finmind_read_metrics", {})),
        "local_read_metrics": dict(getattr(store, "local_read_metrics", {})),
        "rebuild_stage_seconds": dict(stage_seconds),
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
        "cash_account_gap_recovery": cash_recovery,
        "independently_preserved_addition_component": component_proof,
        "carried_endpoint_accounting_preflight": endpoint_preflight,
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
                "sha256": benchmark_sha256,
                **benchmark_projection,
            },
        },
    }
    receipt_path = args.output_dir / "minute_curve_receipt.json"
    if expected_scope is not None:
        scope_failures = minute_curve_scope_failures(
            receipt,
            completed_session_dates=expected_scope[0],
            expected_markets=expected_scope[1],
        )
        if scope_failures:
            raise RuntimeError("minute curve scope mismatch; publication refused: " + "; ".join(scope_failures))
    progress("verify_source_generations")
    store.assert_sources_unchanged()
    progress("finalize_receipt")
    receipt["rebuild_stage_seconds"] = dict(stage_seconds)
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
            write_benchmark_projection(
                state_dir=args.state_dir,
                source_path=args.state_dir / "benchmark_history.json",
                source_sha256=benchmark_sha256,
                origins=rebuilt_benchmarks.get("origins") or {},
                marks=rebuilt_benchmarks.get("marks") or (),
                created_at=str(receipt["created_at"]),
            )
            _atomic_text(args.state_dir / "minute_curve_receipt.json", receipt_path.read_text(encoding="utf-8"))
    progress("complete")
    print(json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

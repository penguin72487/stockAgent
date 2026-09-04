from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timezone
import hashlib
import itertools
import json
import os
from pathlib import Path
import queue
import re
import signal
import threading
import time
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from downloader.artifact_io import atomic_write_json, atomic_write_parquet
from stockagent.data.taifex_sessions import taifex_trading_date
from stockagent.live.shioaji_traffic_ledger import StreamingLedgerRecorder


TAIPEI = ZoneInfo("Asia/Taipei")
SOURCE_NAME = "shioaji_streaming_v1"


TICK_SCHEMA = {
    "event_seq": pl.Int64,
    "worker_index": pl.Int16,
    "exchange": pl.String,
    "code": pl.String,
    "trade_date": pl.Date,
    "exchange_ts_ns": pl.Int64,
    "receive_ts_ns": pl.Int64,
    "receive_monotonic_ns": pl.Int64,
    "open": pl.Float64,
    "avg_price": pl.Float64,
    "close": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "amount": pl.Float64,
    "total_amount": pl.Float64,
    "volume": pl.Int64,
    "total_volume": pl.Int64,
    "tick_type": pl.Int16,
    "chg_type": pl.Int16,
    "price_chg": pl.Float64,
    "pct_chg": pl.Float64,
    "bid_side_total_vol": pl.Int64,
    "ask_side_total_vol": pl.Int64,
    "bid_side_total_cnt": pl.Int64,
    "ask_side_total_cnt": pl.Int64,
    "closing_oddlot_shares": pl.Int64,
    "closing_oddlot_close": pl.Float64,
    "closing_oddlot_amount": pl.Float64,
    "closing_oddlot_bid_price": pl.Float64,
    "closing_oddlot_ask_price": pl.Float64,
    "fixed_trade_vol": pl.Int64,
    "fixed_trade_amount": pl.Float64,
    "suspend": pl.Boolean,
    "simtrade": pl.Boolean,
    "intraday_odd": pl.Boolean,
}


BOOK_BASE_SCHEMA = {
    "event_seq": pl.Int64,
    "worker_index": pl.Int16,
    "exchange": pl.String,
    "code": pl.String,
    "trade_date": pl.Date,
    "exchange_ts_ns": pl.Int64,
    "receive_ts_ns": pl.Int64,
    "receive_monotonic_ns": pl.Int64,
    "suspend": pl.Boolean,
    "simtrade": pl.Boolean,
    "intraday_odd": pl.Boolean,
}
BOOK_LEVEL_SCHEMA = {
    **{f"bid_price_{level}": pl.Float64 for level in range(1, 6)},
    **{f"bid_volume_{level}": pl.Int64 for level in range(1, 6)},
    **{f"diff_bid_vol_{level}": pl.Int64 for level in range(1, 6)},
    **{f"ask_price_{level}": pl.Float64 for level in range(1, 6)},
    **{f"ask_volume_{level}": pl.Int64 for level in range(1, 6)},
    **{f"diff_ask_vol_{level}": pl.Int64 for level in range(1, 6)},
}
BOOK_SCHEMA = {**BOOK_BASE_SCHEMA, **BOOK_LEVEL_SCHEMA}
BOOK_1S_SCHEMA = {
    "snapshot_ts_ns": pl.Int64,
    "worker_index": pl.Int16,
    "exchange": pl.String,
    "code": pl.String,
    "trade_date": pl.Date,
    "book_exchange_ts_ns": pl.Int64,
    "book_receive_ts_ns": pl.Int64,
    "book_age_ms": pl.Float64,
    "stale": pl.Boolean,
    "suspend": pl.Boolean,
    "simtrade": pl.Boolean,
    "intraday_odd": pl.Boolean,
    **BOOK_LEVEL_SCHEMA,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Capture Shioaji stock Tick and five-level BidAsk event streams. Raw "
            "events are preserved and a separate one-second as-of book is emitted."
        )
    )
    parser.add_argument(
        "--universe",
        type=Path,
        default=Path("data_tw_microstructure/universe/top_200.csv"),
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data_tw_microstructure/captures")
    )
    parser.add_argument("--worker-index", type=int, required=True)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--stop-time", default="13:35:00")
    parser.add_argument("--queue-size", type=int, default=250_000)
    parser.add_argument("--flush-rows", type=int, default=50_000)
    parser.add_argument("--flush-seconds", type=float, default=10.0)
    parser.add_argument("--stale-ms", type=float, default=5_000.0)
    parser.add_argument("--subscribe-interval", type=float, default=0.02)
    parser.add_argument("--simulation", action="store_true")
    parser.add_argument(
        "--capture-id",
        default="",
        help="Shared identifier for every worker in one capture attempt.",
    )
    return parser.parse_args()


def _scalar(value: Any) -> Any:
    return getattr(value, "value", value)


def _float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _exchange_ts_ns(value: Any) -> int:
    if not isinstance(value, datetime):
        raise ValueError(f"Shioaji event has invalid datetime: {value!r}")
    localized = value.replace(tzinfo=TAIPEI) if value.tzinfo is None else value
    return int(localized.timestamp() * 1_000_000_000)


def normalize_tick(
    exchange: Any,
    tick: Any,
    *,
    event_seq: int,
    worker_index: int,
    receive_ts_ns: int,
    receive_monotonic_ns: int,
) -> dict[str, Any]:
    values = tick.to_dict()
    row: dict[str, Any] = {
        "event_seq": event_seq,
        "worker_index": worker_index,
        "exchange": str(_scalar(exchange)),
        "code": str(values["code"]),
        "trade_date": values["date"],
        "exchange_ts_ns": _exchange_ts_ns(values["datetime"]),
        "receive_ts_ns": receive_ts_ns,
        "receive_monotonic_ns": receive_monotonic_ns,
    }
    for name in (
        "open",
        "avg_price",
        "close",
        "high",
        "low",
        "amount",
        "total_amount",
        "price_chg",
        "pct_chg",
        "closing_oddlot_close",
        "closing_oddlot_amount",
        "closing_oddlot_bid_price",
        "closing_oddlot_ask_price",
        "fixed_trade_amount",
    ):
        row[name] = _float(values.get(name))
    for name in (
        "volume",
        "total_volume",
        "tick_type",
        "chg_type",
        "bid_side_total_vol",
        "ask_side_total_vol",
        "bid_side_total_cnt",
        "ask_side_total_cnt",
        "closing_oddlot_shares",
        "fixed_trade_vol",
    ):
        row[name] = _int(values.get(name))
    for name in ("suspend", "simtrade", "intraday_odd"):
        row[name] = bool(values.get(name, False))
    return row


def _levels(values: Any, *, numeric: str) -> list[Any]:
    sequence = list(values or [])[:5]
    converter = _float if numeric == "float" else _int
    return [converter(value) for value in sequence] + [None] * (5 - len(sequence))


def normalize_book(
    exchange: Any,
    book: Any,
    *,
    event_seq: int,
    worker_index: int,
    receive_ts_ns: int,
    receive_monotonic_ns: int,
) -> dict[str, Any]:
    values = book.to_dict()
    row: dict[str, Any] = {
        "event_seq": event_seq,
        "worker_index": worker_index,
        "exchange": str(_scalar(exchange)),
        "code": str(values["code"]),
        "trade_date": values["date"],
        "exchange_ts_ns": _exchange_ts_ns(values["datetime"]),
        "receive_ts_ns": receive_ts_ns,
        "receive_monotonic_ns": receive_monotonic_ns,
        "suspend": bool(values.get("suspend", False)),
        "simtrade": bool(values.get("simtrade", False)),
        "intraday_odd": bool(values.get("intraday_odd", False)),
    }
    fields = {
        "bid_price": _levels(values.get("bid_price"), numeric="float"),
        "bid_volume": _levels(values.get("bid_volume"), numeric="int"),
        "diff_bid_vol": _levels(values.get("diff_bid_vol"), numeric="int"),
        "ask_price": _levels(values.get("ask_price"), numeric="float"),
        "ask_volume": _levels(values.get("ask_volume"), numeric="int"),
        "diff_ask_vol": _levels(values.get("diff_ask_vol"), numeric="int"),
    }
    for field, levels in fields.items():
        for level, value in enumerate(levels, start=1):
            row[f"{field}_{level}"] = value
    return row


def normalize_fop_tick(
    tick: Any,
    *,
    event_seq: int,
    worker_index: int,
    receive_ts_ns: int,
    receive_monotonic_ns: int,
) -> dict[str, Any]:
    """Normalize a futures/options tick into the shared immutable event schema."""

    values = tick.to_dict()
    event_datetime = values.get("datetime")
    exchange_ts_ns = _exchange_ts_ns(event_datetime)
    localized = (
        event_datetime.replace(tzinfo=TAIPEI)
        if event_datetime.tzinfo is None
        else event_datetime.astimezone(TAIPEI)
    )
    row: dict[str, Any] = {
        "event_seq": event_seq,
        "worker_index": worker_index,
        "exchange": "TAIFEX",
        "code": str(values["code"]),
        "trade_date": taifex_trading_date(localized),
        "exchange_ts_ns": exchange_ts_ns,
        "receive_ts_ns": receive_ts_ns,
        "receive_monotonic_ns": receive_monotonic_ns,
    }
    for name in TICK_SCHEMA:
        row.setdefault(name, None)
    for name in ("open", "close", "high", "low"):
        row[name] = _float(values.get(name))
    for name in (
        "volume",
        "total_volume",
        "bid_side_total_vol",
        "ask_side_total_vol",
    ):
        row[name] = _int(values.get(name))
    row["simtrade"] = bool(values.get("simtrade", False))
    row["suspend"] = False
    row["intraday_odd"] = False
    return row


def normalize_fop_book(
    book: Any,
    *,
    event_seq: int,
    worker_index: int,
    receive_ts_ns: int,
    receive_monotonic_ns: int,
) -> dict[str, Any]:
    """Normalize a TAIFEX five-level book while retaining receive time."""

    values = book.to_dict()
    event_datetime = values.get("datetime")
    exchange_ts_ns = _exchange_ts_ns(event_datetime)
    localized = (
        event_datetime.replace(tzinfo=TAIPEI)
        if event_datetime.tzinfo is None
        else event_datetime.astimezone(TAIPEI)
    )
    row: dict[str, Any] = {
        "event_seq": event_seq,
        "worker_index": worker_index,
        "exchange": "TAIFEX",
        "code": str(values["code"]),
        "trade_date": taifex_trading_date(localized),
        "exchange_ts_ns": exchange_ts_ns,
        "receive_ts_ns": receive_ts_ns,
        "receive_monotonic_ns": receive_monotonic_ns,
        "suspend": False,
        "simtrade": bool(values.get("simtrade", False)),
        "intraday_odd": False,
    }
    fields = {
        "bid_price": _levels(values.get("bid_price"), numeric="float"),
        "bid_volume": _levels(values.get("bid_volume"), numeric="int"),
        "diff_bid_vol": [None] * 5,
        "ask_price": _levels(values.get("ask_price"), numeric="float"),
        "ask_volume": _levels(values.get("ask_volume"), numeric="int"),
        "diff_ask_vol": [None] * 5,
    }
    for field, levels in fields.items():
        for level, value in enumerate(levels, start=1):
            row[f"{field}_{level}"] = value
    return row


class PartWriter:
    def __init__(
        self,
        root: Path,
        kind: str,
        schema: dict[str, pl.DataType],
        *,
        worker_index: int,
        capture_id: str | None = None,
        flush_rows: int,
        flush_seconds: float,
    ) -> None:
        self.root = root
        self.kind = kind
        self.schema = schema
        self.worker_index = worker_index
        self.capture_id = capture_id
        self.flush_rows = flush_rows
        self.flush_seconds = flush_seconds
        self.rows: list[dict[str, Any]] = []
        self.part_sequence = 0
        self.total_rows = 0
        self.total_parts = 0
        self.total_bytes = 0
        self.last_flush = time.monotonic()

    def append(self, row: dict[str, Any]) -> None:
        self.rows.append(row)

    def maybe_flush(self, *, force: bool = False) -> None:
        if not self.rows:
            return
        if (
            not force
            and len(self.rows) < self.flush_rows
            and (time.monotonic() - self.last_flush < self.flush_seconds)
        ):
            return
        by_partition: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in self.rows:
            event_ns = int(row.get("exchange_ts_ns") or row["snapshot_ts_ns"])
            event_dt = datetime.fromtimestamp(event_ns / 1e9, tz=TAIPEI)
            key = (str(row["trade_date"]), f"{event_dt.hour:02d}")
            by_partition.setdefault(key, []).append(row)
        self.rows = []
        for (trade_date, hour), rows in sorted(by_partition.items()):
            partition = (
                self.root / self.kind / f"trade_date={trade_date}" / f"hour={hour}"
            )
            partition.mkdir(parents=True, exist_ok=True)
            self.part_sequence += 1
            stamp = time.time_ns()
            capture_prefix = (
                f"capture={self.capture_id}-" if self.capture_id is not None else ""
            )
            filename = (
                f"{capture_prefix}worker={self.worker_index:02d}-"
                f"part={self.part_sequence:06d}-{stamp}.parquet"
            )
            path = partition / filename
            atomic_write_parquet(
                path,
                pl.from_dicts(rows, schema=self.schema, strict=False),
                compression="zstd",
                write_statistics=True,
                row_group_size=128_000,
            )
            self.total_rows += len(rows)
            self.total_parts += 1
            self.total_bytes += int(path.stat().st_size)
        self.last_flush = time.monotonic()


@dataclass(slots=True)
class CaptureStats:
    tick_events: int = 0
    book_events: int = 0
    book_1s_rows: int = 0
    dropped_events: int = 0
    queue_high_watermark: int = 0
    missed_snapshot_seconds: int = 0
    out_of_scope_events: int = 0


class EventSink:
    def __init__(
        self,
        output_dir: Path,
        *,
        worker_index: int,
        capture_id: str,
        queue_size: int,
        flush_rows: int,
        flush_seconds: float,
        stale_ms: float,
        accepted_trade_date: date | None = None,
    ) -> None:
        self.worker_index = worker_index
        self.queue: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue(queue_size)
        self.stop_event = threading.Event()
        self.fatal_event = threading.Event()
        self.stats = CaptureStats()
        self.stats_lock = threading.Lock()
        self.latest_books: dict[str, dict[str, Any]] = {}
        self.stale_ms = stale_ms
        self.accepted_trade_date = accepted_trade_date
        self.live_book_codes: set[str] = set()
        self.live_book_metadata: dict[str, dict[str, Any]] = {}
        self.live_books_path = (
            output_dir / "runtime" / f"worker_{worker_index:02d}.json"
        )
        options = {
            "worker_index": worker_index,
            "capture_id": capture_id,
            "flush_rows": flush_rows,
            "flush_seconds": flush_seconds,
        }
        self.tick_writer = PartWriter(output_dir, "ticks", TICK_SCHEMA, **options)
        self.book_writer = PartWriter(output_dir, "book_events", BOOK_SCHEMA, **options)
        self.snapshot_writer = PartWriter(
            output_dir, "book_1s", BOOK_1S_SCHEMA, **options
        )
        self.thread = threading.Thread(
            target=self._run, name="event-writer", daemon=False
        )

    def start(self) -> None:
        self.thread.start()

    def enqueue(self, kind: str, row: dict[str, Any]) -> None:
        if (
            self.accepted_trade_date is not None
            and row.get("trade_date") != self.accepted_trade_date
        ):
            # Shioaji may emit one cached quote from the preceding session as a
            # subscription is established.  It is useful provider behaviour,
            # but it is not an event from this capture's trading date and must
            # not leak into this capture's part counts or causal book stream.
            with self.stats_lock:
                self.stats.out_of_scope_events += 1
            return
        try:
            self.queue.put_nowait((kind, row))
            size = self.queue.qsize()
            with self.stats_lock:
                self.stats.queue_high_watermark = max(
                    self.stats.queue_high_watermark, size
                )
        except queue.Full:
            with self.stats_lock:
                self.stats.dropped_events += 1
            self.fatal_event.set()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join()

    def _emit_snapshots(self, second: int) -> None:
        snapshot_ns = second * 1_000_000_000
        snapshot_datetime = datetime.fromtimestamp(snapshot_ns / 1e9, tz=TAIPEI)
        live_books: dict[str, dict[str, Any]] = {}
        for book in self.latest_books.values():
            snapshot_date = (
                taifex_trading_date(snapshot_datetime)
                if book["exchange"] == "TAIFEX"
                else snapshot_datetime.date()
            )
            if book["trade_date"] != snapshot_date:
                continue
            age_ms = max(0.0, (snapshot_ns - int(book["receive_ts_ns"])) / 1e6)
            row = {
                "snapshot_ts_ns": snapshot_ns,
                "worker_index": self.worker_index,
                "exchange": book["exchange"],
                "code": book["code"],
                "trade_date": snapshot_date,
                "book_exchange_ts_ns": book["exchange_ts_ns"],
                "book_receive_ts_ns": book["receive_ts_ns"],
                "book_age_ms": age_ms,
                "stale": age_ms > self.stale_ms,
                "suspend": book["suspend"],
                "simtrade": book["simtrade"],
                "intraday_odd": book["intraday_odd"],
                **{name: book[name] for name in BOOK_LEVEL_SCHEMA},
            }
            self.snapshot_writer.append(row)
            self.stats.book_1s_rows += 1
            if str(book["code"]) in self.live_book_codes:
                live_books[str(book["code"])] = {
                    key: (str(value) if key == "trade_date" else value)
                    for key, value in row.items()
                    if key
                    in {
                        "snapshot_ts_ns",
                        "code",
                        "trade_date",
                        "book_exchange_ts_ns",
                        "book_receive_ts_ns",
                        "book_age_ms",
                        "stale",
                        "suspend",
                        "simtrade",
                        "bid_price_1",
                        "bid_volume_1",
                        "ask_price_1",
                        "ask_volume_1",
                    }
                }
        if self.live_book_codes:
            _atomic_json(
                self.live_books_path,
                {
                    "schema_version": 2,
                    "source": SOURCE_NAME,
                    "published_at": snapshot_datetime.astimezone(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z"),
                    "worker_index": self.worker_index,
                    "contract_metadata": {
                        code: self.live_book_metadata[code]
                        for code in sorted(live_books)
                        if code in self.live_book_metadata
                    },
                    "books": live_books,
                },
            )

    def _run(self) -> None:
        current_second = time.time_ns() // 1_000_000_000
        while not self.stop_event.is_set() or not self.queue.empty():
            try:
                kind, row = self.queue.get(timeout=0.05)
            except queue.Empty:
                kind = ""
                row = {}
            # Sample the last fully processed book at the second boundary before
            # applying an event received after that boundary. This avoids future
            # information leaking into the one-second as-of book.
            now_second = time.time_ns() // 1_000_000_000
            if now_second > current_second:
                if now_second > current_second + 1:
                    self.stats.missed_snapshot_seconds += (
                        now_second - current_second - 1
                    )
                current_second = now_second
                self._emit_snapshots(current_second)
            if kind == "tick":
                self.tick_writer.append(row)
                self.stats.tick_events += 1
            elif kind == "book":
                self.book_writer.append(row)
                self.latest_books[row["code"]] = row
                self.stats.book_events += 1
            self.tick_writer.maybe_flush()
            self.book_writer.maybe_flush()
            self.snapshot_writer.maybe_flush()
        self.tick_writer.maybe_flush(force=True)
        self.book_writer.maybe_flush(force=True)
        self.snapshot_writer.maybe_flush(force=True)


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_json(
        path,
        payload,
        durable=True,
        ensure_ascii=True,
        sort_keys=True,
    )


def _stop_datetime(value: str) -> datetime:
    parsed = datetime_time.fromisoformat(value)
    now = datetime.now(TAIPEI)
    return datetime.combine(now.date(), parsed, tzinfo=TAIPEI)


def main() -> None:
    args = parse_args()
    if not 0 <= args.worker_index < args.workers:
        raise ValueError("worker-index must satisfy 0 <= index < workers")
    if args.workers <= 0:
        raise ValueError("workers must be positive")
    capture_id = str(args.capture_id).strip()
    if args.workers > 1 and not capture_id:
        raise ValueError("--capture-id is required when workers > 1")
    if not capture_id:
        capture_id = f"single-{time.time_ns()}"
    if re.fullmatch(r"[A-Za-z0-9_.-]+", capture_id) is None:
        raise ValueError(
            "capture-id may contain only ASCII letters, digits, dot, underscore, or dash"
        )
    universe = (
        pl.read_csv(
            args.universe,
            schema_overrides={"symbol": pl.String},
            infer_schema_length=0,
        )
        .with_columns(
            pl.col("market_cap_rank").cast(pl.Int32, strict=True),
            pl.col("symbol").cast(pl.String, strict=True),
        )
        .sort("market_cap_rank")
    )
    universe_sha256 = hashlib.sha256(args.universe.read_bytes()).hexdigest()
    required = {"market_cap_rank", "symbol", "market"}
    if not required <= set(universe.columns):
        raise ValueError(
            f"universe lacks columns: {sorted(required - set(universe.columns))}"
        )
    selected = universe.filter(
        ((pl.col("market_cap_rank") - 1) % args.workers) == args.worker_index
    )
    if selected.is_empty() or selected.height * 2 > 200:
        raise ValueError(
            f"worker {args.worker_index} has {selected.height} symbols / "
            f"{selected.height * 2} subscriptions; limit is 200"
        )
    api_key = os.environ.get("SHIOAJI_API_KEY", "").strip()
    secret_key = os.environ.get("SHIOAJI_SECRET_KEY", "").strip()
    if not api_key or not secret_key:
        raise RuntimeError("SHIOAJI_API_KEY and SHIOAJI_SECRET_KEY are required")
    import shioaji as sj

    started_at = datetime.now(timezone.utc)
    stop_at = _stop_datetime(args.stop_time)
    sink = EventSink(
        args.output_dir,
        worker_index=args.worker_index,
        capture_id=capture_id,
        queue_size=args.queue_size,
        flush_rows=args.flush_rows,
        flush_seconds=args.flush_seconds,
        stale_ms=args.stale_ms,
    )
    event_sequence = itertools.count(1)
    shutdown = threading.Event()
    symbol_exchange = {
        str(row["symbol"]): (
            "TSE" if str(row["market"]).strip().lower() == "twse" else "OTC"
        )
        for row in selected.iter_rows(named=True)
    }

    def request_shutdown(_signum: int, _frame: Any) -> None:
        shutdown.set()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)
    api = sj.Shioaji(simulation=bool(args.simulation))
    api.set_event_callback(lambda *_args: None)
    api.login(api_key=api_key, secret_key=secret_key, subscribe_trade=False)

    @api.on_tick_stk_v1()
    def on_tick(tick: Any) -> None:
        receive_ts_ns = time.time_ns()
        receive_monotonic_ns = time.monotonic_ns()
        try:
            row = normalize_tick(
                symbol_exchange[str(tick.code)],
                tick,
                event_seq=next(event_sequence),
                worker_index=args.worker_index,
                receive_ts_ns=receive_ts_ns,
                receive_monotonic_ns=receive_monotonic_ns,
            )
            sink.enqueue("tick", row)
        except Exception:
            sink.fatal_event.set()

    @api.on_bidask_stk_v1()
    def on_book(book: Any) -> None:
        receive_ts_ns = time.time_ns()
        receive_monotonic_ns = time.monotonic_ns()
        try:
            row = normalize_book(
                symbol_exchange[str(book.code)],
                book,
                event_seq=next(event_sequence),
                worker_index=args.worker_index,
                receive_ts_ns=receive_ts_ns,
                receive_monotonic_ns=receive_monotonic_ns,
            )
            sink.enqueue("book", row)
        except Exception:
            sink.fatal_event.set()

    contracts: list[Any] = []
    missing: list[str] = []
    for symbol in selected["symbol"]:
        contract = api.contracts.get(str(symbol))
        if contract is None:
            missing.append(str(symbol))
        else:
            contracts.append(contract)
    if missing:
        api.logout()
        raise RuntimeError(
            f"top-market-cap symbols missing Shioaji contracts: {missing}"
        )
    sink.start()
    stream_ledger = StreamingLedgerRecorder(
        consumer="stock_top200_stream",
        asset_class="stock",
        details={
            "worker_index": int(args.worker_index),
            "trade_date": datetime.now(TAIPEI).date().isoformat(),
        },
    )
    last_ledger_observation = time.monotonic()

    def observe_stream_ledger() -> None:
        stats = sink.stats
        stream_ledger.observe(
            tick_events=stats.tick_events,
            book_events=stats.book_events,
            snapshot_rows=stats.book_1s_rows,
            dropped_events=stats.dropped_events,
            stored_bytes=(
                sink.tick_writer.total_bytes
                + sink.book_writer.total_bytes
                + sink.snapshot_writer.total_bytes
            ),
        )

    subscribed = 0
    status = "running"
    try:
        for contract in contracts:
            api.subscribe(
                contract,
                quote_type=sj.QuoteType.Tick,
                version=sj.QuoteVersion.v1,
            )
            subscribed += 1
            if args.subscribe_interval:
                time.sleep(args.subscribe_interval)
            api.subscribe(
                contract,
                quote_type=sj.QuoteType.BidAsk,
                version=sj.QuoteVersion.v1,
            )
            subscribed += 1
            if args.subscribe_interval:
                time.sleep(args.subscribe_interval)
        print(
            f"[shioaji-stream] worker={args.worker_index}/{args.workers} "
            f"symbols={len(contracts)} subscriptions={subscribed} stop_at={stop_at}",
            flush=True,
        )
        while datetime.now(TAIPEI) < stop_at and not shutdown.wait(0.5):
            if time.monotonic() - last_ledger_observation >= 60.0:
                observe_stream_ledger()
                last_ledger_observation = time.monotonic()
            if sink.fatal_event.is_set():
                status = "failed_event_loss_or_normalization"
                break
        if shutdown.is_set():
            status = "stopped_by_signal"
        elif status == "running":
            status = "complete"
    finally:
        sink.stop()
        observe_stream_ledger()
        try:
            api.logout()
        except Exception:
            status = "failed_logout" if status == "complete" else status
        finished_at = datetime.now(timezone.utc)
        stats = sink.stats
        manifest = {
            "schema_version": 3,
            "source": SOURCE_NAME,
            "status": status,
            "capture_id": capture_id,
            "simulation": bool(args.simulation),
            "worker_index": int(args.worker_index),
            "workers": int(args.workers),
            "symbols": selected["symbol"].to_list(),
            "universe_path": str(args.universe),
            "universe_sha256": universe_sha256,
            "symbol_count": selected.height,
            "subscriptions_requested": subscribed,
            "tick_events": stats.tick_events,
            "book_events": stats.book_events,
            "book_1s_rows": stats.book_1s_rows,
            "dropped_events": stats.dropped_events,
            "queue_high_watermark": stats.queue_high_watermark,
            "missed_snapshot_seconds": stats.missed_snapshot_seconds,
            "tick_rows_written": sink.tick_writer.total_rows,
            "book_rows_written": sink.book_writer.total_rows,
            "book_1s_rows_written": sink.snapshot_writer.total_rows,
            "tick_parts": sink.tick_writer.total_parts,
            "book_parts": sink.book_writer.total_parts,
            "book_1s_parts": sink.snapshot_writer.total_parts,
            "started_at_utc": started_at.replace(microsecond=0).isoformat(),
            "finished_at_utc": finished_at.replace(microsecond=0).isoformat(),
        }
        manifest_path = (
            args.output_dir
            / "manifests"
            / f"trade_date={datetime.now(TAIPEI).date()}"
            / f"worker={args.worker_index:02d}.json"
        )
        _atomic_json(manifest_path, manifest)
        print(
            f"[shioaji-stream] worker={args.worker_index} status={status} "
            f"ticks={stats.tick_events} books={stats.book_events} "
            f"book_1s={stats.book_1s_rows} dropped={stats.dropped_events} "
            f"manifest={manifest_path}",
            flush=True,
        )
    if status != "complete":
        raise SystemExit(1)


if __name__ == "__main__":
    main()

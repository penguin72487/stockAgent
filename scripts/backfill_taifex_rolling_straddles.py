#!/usr/bin/env python3
"""Replay the rolling TXO straddles from receipt-backed Shioaji five-level books.

The replay deliberately uses completed-second snapshots from market-data worker 0,
which is the same worker that owns the live strategy engine.  A signal may inspect
only books received by that completed second; every replacement leg must use a
strictly later complete five-level book.  Missing books remain missing.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, time as datetime_time, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
from typing import Any, Iterable, Mapping

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.shioaji_capture_parts import (  # noqa: E402
    read_capture_manifests,
    select_capture_part_paths,
)
from stockagent.data.taifex_sessions import TAIPEI  # noqa: E402
from stockagent.live.taifex_volatility_simulation import (  # noqa: E402
    EXECUTION_CONTRACT_VERSION,
    FuturesInstrument,
    TaifexVolatilitySimulation,
)
from stockagent.research.taifex_volatility_metadata import (  # noqa: E402
    ROLLING_STRADDLE_IDS,
    STRATEGY_MODE_INTRADAY_FUTURES,
)


REPLAY_CONTRACT_VERSION = 1
REPLAY_SOURCE = "shioaji_worker0_completed_second_bidask"
BOOK_COLUMNS = [
    "snapshot_ts_ns",
    "code",
    "book_receive_ts_ns",
    "suspend",
    "simtrade",
    "intraday_odd",
    *[f"bid_price_{level}" for level in range(1, 6)],
    *[f"bid_volume_{level}" for level in range(1, 6)],
    *[f"ask_price_{level}" for level in range(1, 6)],
    *[f"ask_volume_{level}" for level in range(1, 6)],
]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _atomic_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    count = 0
    with temporary.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(
                json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)
                + "\n"
            )
            count += 1
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    return count


def _manifest_paths(capture_root: Path, trade_date: date) -> list[Path]:
    return sorted(
        (capture_root / "manifests" / f"trade_date={trade_date.isoformat()}").glob(
            "worker=*.json"
        )
    )


def _validate_manifest_group(
    capture_root: Path,
    trade_date: date,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[Path]]:
    manifests = read_capture_manifests(capture_root, trade_date.isoformat())
    if not manifests:
        raise RuntimeError(f"no finalized capture manifests for {trade_date}")
    declared_workers = {int(row.get("workers", 0)) for row in manifests}
    if len(declared_workers) != 1 or len(manifests) != next(iter(declared_workers)):
        raise RuntimeError(
            f"incomplete worker manifest group for {trade_date}: "
            f"got={len(manifests)} declared={sorted(declared_workers)}"
        )
    capture_ids = {str(row.get("capture_id") or "") for row in manifests}
    if len(capture_ids) != 1 or "" in capture_ids:
        raise RuntimeError(f"worker capture identity mismatch for {trade_date}")
    for manifest in manifests:
        if manifest.get("source") != "shioaji_taifex_tick_bidask_v1":
            raise RuntimeError(f"unexpected capture source for {trade_date}")
        if manifest.get("status") != "complete":
            raise RuntimeError(f"capture is not complete for {trade_date}")
        if int(manifest.get("dropped_events", -1)) != 0:
            raise RuntimeError(f"capture dropped callback events for {trade_date}")
    worker_zero = [row for row in manifests if int(row.get("worker_index", -1)) == 0]
    if len(worker_zero) != 1:
        raise RuntimeError(f"capture has no unique strategy worker 0 for {trade_date}")
    parts = select_capture_part_paths(
        capture_root=capture_root,
        kind="book_1s",
        trade_date=trade_date.isoformat(),
        manifests=worker_zero,
    )
    if not parts:
        raise RuntimeError(f"capture has no worker-0 completed-second books: {trade_date}")
    return manifests, worker_zero[0], parts


def _option_info(metadata: Mapping[str, Any]) -> SimpleNamespace:
    base = SimpleNamespace(code=str(metadata["code"]))
    return SimpleNamespace(
        base=base,
        root=str(metadata["root"]),
        delivery_month=str(metadata["delivery_month"]),
        delivery_date=date.fromisoformat(str(metadata["delivery_date"])),
        strike_price=float(metadata["strike_price"]),
        option_right=str(metadata["option_right"]),
    )


def _instrument_metadata(
    manifests: Iterable[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for manifest in manifests:
        for raw in manifest.get("contract_metadata") or ():
            if not isinstance(raw, Mapping) or not raw.get("code"):
                continue
            code = str(raw["code"])
            row = dict(raw)
            existing = output.get(code)
            if existing is not None and existing != row:
                raise RuntimeError(f"contract metadata changed inside replay: {code}")
            output[code] = row
    return output


def _contract_identity(metadata: Mapping[str, Any]) -> tuple[object, ...]:
    return tuple(
        metadata.get(key)
        for key in (
            "code",
            "exchange",
            "security_type",
            "root",
            "logical_code",
            "delivery_month",
            "delivery_date",
            "last_trading_date",
            "strike_price",
            "option_right",
            "multiplier",
        )
    )


def _future_instrument(
    metadata: Iterable[Mapping[str, Any]],
    *,
    logical_code: str,
) -> FuturesInstrument:
    matches = [
        row
        for row in metadata
        if str(row.get("logical_code") or "").upper() == logical_code
    ]
    codes = {str(row.get("code") or "") for row in matches}
    if len(matches) != 1 or len(codes) != 1:
        raise RuntimeError(
            f"replay requires one stable {logical_code} contract, got={sorted(codes)}"
        )
    row = matches[0]
    return FuturesInstrument(
        logical_code=logical_code,
        code=str(row["code"]),
        contract=SimpleNamespace(code=str(row["code"])),
        last_trading_date=(
            date.fromisoformat(str(row["last_trading_date"]))
            if row.get("last_trading_date")
            else None
        ),
    )


def _iter_completed_seconds(
    paths: Iterable[Path],
) -> Iterable[tuple[int, list[dict[str, Any]]]]:
    pending_timestamp: int | None = None
    pending_rows: list[dict[str, Any]] = []
    for path in paths:
        frame = pl.read_parquet(path, columns=BOOK_COLUMNS).sort(
            ["snapshot_ts_ns", "code"]
        )
        for raw in frame.iter_rows(named=True):
            timestamp = int(raw["snapshot_ts_ns"])
            if pending_timestamp is not None and timestamp != pending_timestamp:
                yield pending_timestamp, pending_rows
                pending_rows = []
            pending_timestamp = timestamp
            row = dict(raw)
            row["receive_ts_ns"] = int(row.pop("book_receive_ts_ns"))
            pending_rows.append(row)
    if pending_timestamp is not None:
        yield pending_timestamp, pending_rows


def _copy_jsonl_with_replay_provenance(
    source: Path,
    destination: Path,
    *,
    replay_id: str,
) -> int:
    rows: list[dict[str, Any]] = []
    if source.is_file():
        with source.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        f"malformed replay JSONL {source}:{line_number}"
                    ) from exc
                row["history_source"] = REPLAY_SOURCE
                row["replay_contract_version"] = REPLAY_CONTRACT_VERSION
                row["replay_id"] = replay_id
                rows.append(row)
    return _atomic_jsonl(destination, rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 8, 13))
    parser.add_argument("--end-date", type=date.fromisoformat, default=None)
    parser.add_argument(
        "--capture-root",
        type=Path,
        default=Path("data_tw_index_derivatives_ticks/shioaji_fop_captures"),
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path("artifacts/live/shioaji_taifex_volatility_simulation"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
    )
    parser.add_argument(
        "--final-settlement-path",
        type=Path,
        default=Path(
            "data_tw_index_options_daily/txo_final_settlement_history.parquet"
        ),
    )
    args = parser.parse_args()
    end_date = args.end_date or datetime.now(TAIPEI).date()
    if end_date < args.start_date:
        raise ValueError("end date is before start date")
    output_dir = args.output_dir or (
        args.state_dir / "backfills" / "rolling_straddles_bidask_v1"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    lock_path = output_dir / "backfill.lock"
    lock_handle = lock_path.open("a+")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise RuntimeError("another rolling-straddle backfill is running") from exc

    replay_id = (
        f"rolling-straddles-{args.start_date.isoformat()}-{end_date.isoformat()}-"
        f"v{REPLAY_CONTRACT_VERSION}"
    )
    finalized_days: list[date] = []
    day_sources: dict[date, dict[str, Any]] = {}
    all_metadata: dict[str, dict[str, Any]] = {}
    manifest_receipts: list[dict[str, Any]] = []
    for manifest_dir in sorted((args.capture_root / "manifests").glob("trade_date=*")):
        try:
            trade_date = date.fromisoformat(manifest_dir.name.split("=", 1)[1])
        except (IndexError, ValueError):
            continue
        if not args.start_date <= trade_date <= end_date:
            continue
        manifests, worker_zero, parts = _validate_manifest_group(
            args.capture_root, trade_date
        )
        if str(worker_zero.get("capture_session") or "day") != "day":
            continue
        metadata = _instrument_metadata([worker_zero])
        for code, row in metadata.items():
            existing = all_metadata.get(code)
            if existing is not None and _contract_identity(
                existing
            ) != _contract_identity(row):
                raise RuntimeError(f"contract metadata changed across replay: {code}")
            all_metadata.setdefault(code, row)
        finalized_days.append(trade_date)
        day_sources[trade_date] = {
            "manifests": manifests,
            "worker_zero": worker_zero,
            "parts": parts,
            "metadata": metadata,
        }
        manifest_paths = _manifest_paths(args.capture_root, trade_date)
        manifest_receipts.append(
            {
                "trade_date": trade_date.isoformat(),
                "capture_id": str(worker_zero["capture_id"]),
                "capture_session": str(worker_zero.get("capture_session") or "day"),
                "worker_count": len(manifests),
                "worker_0_contract_count": int(worker_zero["contract_count"]),
                "worker_0_book_1s_rows": int(worker_zero["book_1s_rows_written"]),
                "worker_0_book_1s_parts": len(parts),
                "dropped_events": sum(int(row["dropped_events"]) for row in manifests),
                "manifest_sha256": {
                    str(path.relative_to(args.capture_root)): _sha256(path)
                    for path in manifest_paths
                },
                "part_inventory_sha256": hashlib.sha256(
                    "\n".join(
                        f"{path.relative_to(args.capture_root)}|{path.stat().st_size}|"
                        f"{path.stat().st_mtime_ns}"
                        for path in parts
                    ).encode("utf-8")
                ).hexdigest(),
            }
        )
    if not finalized_days:
        raise RuntimeError("no complete day-session captures are replayable")

    option_infos = [
        _option_info(row)
        for row in all_metadata.values()
        if str(row.get("security_type") or "") == "OPT"
    ]
    metadata_rows = list(all_metadata.values())
    underlying = _future_instrument(metadata_rows, logical_code="TXFR1")
    hedge = _future_instrument(metadata_rows, logical_code="TMFR1")
    marks: list[dict[str, Any]] = []
    replayed_seconds = 0
    first_snapshot_by_day: dict[str, int] = {}
    last_snapshot_by_day: dict[str, int] = {}
    uncaptured_held_codes: dict[str, list[str]] = {}
    with tempfile.TemporaryDirectory(prefix="taifex-rolling-backfill-") as temporary:
        replay_state_dir = Path(temporary) / "state"
        fake_api = SimpleNamespace(futopt_account=SimpleNamespace())
        engine = TaifexVolatilitySimulation(
            api=fake_api,
            shioaji_module=SimpleNamespace(),
            state_dir=replay_state_dir,
            option_infos=option_infos,
            underlying=underlying,
            hedge=hedge,
            final_settlement_path=args.final_settlement_path,
            bootstrap_after=args.start_date - timedelta(days=1),
            broker_orders_enabled=False,
            strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
            active_strategy_ids=ROLLING_STRADDLE_IDS,
        )
        option_by_code = dict(engine.options_by_code)
        last_mark_bucket: int | None = None
        last_snapshot_ns = 0
        for trade_date in finalized_days:
            source = day_sources[trade_date]
            subscribed_codes = set(source["metadata"])
            current_options = tuple(
                option_by_code[code]
                for code in subscribed_codes
                if code in option_by_code
            )
            engine.options = tuple(
                sorted(
                    current_options,
                    key=lambda item: (
                        item.expiry,
                        item.root,
                        item.strike,
                        item.right,
                        item.code,
                    ),
                )
            )
            held_codes = {
                str(code)
                for strategy_id in ROLLING_STRADDLE_IDS
                for code, quantity in (
                    engine.state["strategies"][strategy_id].get("option_positions")
                    or {}
                ).items()
                if int(quantity) != 0
            }
            missing_held = sorted(held_codes - subscribed_codes)
            if missing_held:
                uncaptured_held_codes[trade_date.isoformat()] = missing_held
            for snapshot_ns, rows in _iter_completed_seconds(source["parts"]):
                if snapshot_ns <= last_snapshot_ns:
                    raise RuntimeError("completed-second replay clock is not increasing")
                last_snapshot_ns = snapshot_ns
                first_snapshot_by_day.setdefault(trade_date.isoformat(), snapshot_ns)
                last_snapshot_by_day[trade_date.isoformat()] = snapshot_ns
                for row in rows:
                    engine.on_book(row)
                observed_now = datetime.fromtimestamp(snapshot_ns / 1e9, tz=TAIPEI)
                engine._maybe_settle_expired_cycle(observed_now, snapshot_ns)
                engine._maybe_open_cycle(observed_now, snapshot_ns)
                session_state = engine._intraday_session_state(observed_now)
                if bool(session_state["entry_allowed"]):
                    engine._maybe_enter_cycle_strategies(snapshot_ns)
                engine._maybe_roll_straddles(snapshot_ns)
                engine._maybe_flatten_expiry_hedges(observed_now, snapshot_ns)
                engine._maybe_enforce_strategy_margin(snapshot_ns)
                replayed_seconds += 1
                mark_bucket = snapshot_ns // 60_000_000_000
                if mark_bucket == last_mark_bucket:
                    continue
                last_mark_bucket = mark_bucket
                for strategy_id in ROLLING_STRADDLE_IDS:
                    mark = engine._strategy_mark(strategy_id, snapshot_ns)
                    mark.update(
                        {
                            "history_source": REPLAY_SOURCE,
                            "replay_contract_version": REPLAY_CONTRACT_VERSION,
                            "replay_id": replay_id,
                            "source_trade_date": trade_date.isoformat(),
                            "source_capture_id": str(
                                source["worker_zero"]["capture_id"]
                            ),
                        }
                    )
                    marks.append(mark)

        cycle = engine.state.get("active_cycle")
        if isinstance(cycle, Mapping):
            expiry = date.fromisoformat(str(cycle["expiry_date"]))
            if expiry <= max(finalized_days):
                settlement_at = datetime.combine(
                    expiry,
                    datetime_time(15, 0),
                    tzinfo=TAIPEI,
                )
                settlement_ns = int(settlement_at.timestamp() * 1e9)
                engine._maybe_settle_expired_cycle(settlement_at, settlement_ns)
                if engine.state.get("active_cycle") is not None:
                    raise RuntimeError(
                        f"official final settlement did not close replay cycle {expiry}"
                    )
                for strategy_id in ROLLING_STRADDLE_IDS:
                    mark = engine._strategy_mark(strategy_id, settlement_ns)
                    mark.update(
                        {
                            "history_source": REPLAY_SOURCE,
                            "replay_contract_version": REPLAY_CONTRACT_VERSION,
                            "replay_id": replay_id,
                            "source_trade_date": expiry.isoformat(),
                            "source_capture_id": None,
                            "history_event": "official_taifex_final_settlement",
                        }
                    )
                    marks.append(mark)

        terminal_state = dict(engine.state)
        engine.close()
        marks.sort(key=lambda row: (int(row["decision_ts_ns"]), row["strategy_id"]))
        seen_mark_keys: set[tuple[int, str]] = set()
        for row in marks:
            key = (int(row["decision_ts_ns"]), str(row["strategy_id"]))
            if key in seen_mark_keys:
                raise RuntimeError(f"duplicate replay mark key: {key}")
            seen_mark_keys.add(key)
        mark_count = _atomic_jsonl(output_dir / "marks.jsonl", marks)
        trade_count = _copy_jsonl_with_replay_provenance(
            replay_state_dir / "ideal_ledger.jsonl",
            output_dir / "ideal_ledger.jsonl",
            replay_id=replay_id,
        )
        event_count = _copy_jsonl_with_replay_provenance(
            replay_state_dir / "events.jsonl",
            output_dir / "events.jsonl",
            replay_id=replay_id,
        )
        _atomic_json(output_dir / "terminal_state.json", terminal_state)

    terminal_summary = {
        strategy_id: {
            "cumulative_pnl_twd": next(
                (
                    row.get("cumulative_pnl_twd")
                    for row in reversed(marks)
                    if row["strategy_id"] == strategy_id
                ),
                None,
            ),
            "initial_capital_twd": float(
                terminal_state["strategies"][strategy_id].get("initial_capital_twd")
                or 0.0
            ),
            "trade_sides": int(
                terminal_state["strategies"][strategy_id].get("trade_sides") or 0
            ),
            "option_roll_count": int(
                terminal_state["strategies"][strategy_id].get("option_roll_count")
                or 0
            ),
            "alive": bool(terminal_state["strategies"][strategy_id].get("alive")),
        }
        for strategy_id in ROLLING_STRADDLE_IDS
    }
    source_coverage = []
    for trade_date in finalized_days:
        key = trade_date.isoformat()
        source_coverage.append(
            {
                "trade_date": key,
                "capture_session": "day",
                "coverage_start_utc": datetime.fromtimestamp(
                    first_snapshot_by_day[key] / 1e9, tz=timezone.utc
                ).isoformat(),
                "coverage_end_utc": datetime.fromtimestamp(
                    last_snapshot_by_day[key] / 1e9, tz=timezone.utc
                ).isoformat(),
                "pre_capture_gap": True,
            }
        )
    pending_capture_dates = sorted(
        {
            path.parent.name.split("=", 1)[1]
            for path in (args.capture_root / "book_events").glob(
                "trade_date=*/hour=*"
            )
            if path.parent.name.split("=", 1)[1]
            not in {day.isoformat() for day in finalized_days}
            and args.start_date.isoformat()
            <= path.parent.name.split("=", 1)[1]
            <= end_date.isoformat()
        }
    )
    receipt = {
        "schema_version": 1,
        "status": "partial_receipt_backfill",
        "replay_id": replay_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "requested_start_date": args.start_date.isoformat(),
        "requested_end_date": end_date.isoformat(),
        "strategy_ids": list(ROLLING_STRADDLE_IDS),
        "execution_contract_version": EXECUTION_CONTRACT_VERSION,
        "replay_contract_version": REPLAY_CONTRACT_VERSION,
        "source": REPLAY_SOURCE,
        "causal_clock": {
            "decision": "completed local-receive second on worker 0",
            "entry": "first complete fresh five-level Call/Put and TX books",
            "roll_signal": "current completed-second TX midpoint and fixed held strikes",
            "roll_execution": "strictly later complete old/new five-level books; sell Bid and buy Ask; atomic prevalidation",
            "mark": "signed immediate five-level liquidation side; explicitly labelled carry only",
            "settlement": "official TAIFEX final settlement; never zero or synthetic",
        },
        "manifest_receipts": manifest_receipts,
        "source_coverage": source_coverage,
        "finalized_replayed_dates": [day.isoformat() for day in finalized_days],
        "pending_unfinalized_capture_dates": pending_capture_dates,
        "uncaptured_counterfactual_held_codes": uncaptured_held_codes,
        "limitations": [
            "Only finalized day-session worker-0 captures can be replayed.",
            "The 08:30-to-capture-start interval and all sessions without a complete manifest remain unavailable.",
            "No transaction print, midpoint fill, forward fill, cross-worker substitution, or fabricated depth is used.",
            "This is a historical ideal-ledger replay layer and does not mutate the forward live account state or broker simulation ledger.",
        ],
        "record_counts": {
            "completed_seconds": replayed_seconds,
            "marks": mark_count,
            "ideal_trades": trade_count,
            "events": event_count,
        },
        "terminal_strategies": terminal_summary,
        "output_sha256": {
            name: _sha256(output_dir / name)
            for name in (
                "marks.jsonl",
                "ideal_ledger.jsonl",
                "events.jsonl",
                "terminal_state.json",
            )
        },
    }
    _atomic_json(output_dir / "receipt.json", receipt)
    print(
        "[taifex-rolling-backfill] "
        f"status={receipt['status']} dates={len(finalized_days)} "
        f"seconds={replayed_seconds} marks={mark_count} trades={trade_count} "
        f"output={output_dir}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Keep every completed TW day-trade replay at audited one-minute grain."""

from __future__ import annotations

import argparse
from datetime import date, datetime, time as wall_time, timedelta
import fcntl
from http.client import HTTPConnection, HTTPException
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any, Mapping
import uuid

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.promote_tw_day_trade_replay import (  # noqa: E402
    MINUTE_CURVE_CONTRACT,
    MINUTE_CURVE_SESSION_POINTS,
    _validate_benchmarks,
    _validate_minute_curve_coverage,
)
from scripts.rebuild_tw_day_trade_benchmark_history import (  # noqa: E402
    _tx_complete_minute_books,
    _tx_day_books,
    _tx_front_contract_metadata,
    _tx_historical_day_books,
)
from scripts.rebuild_tw_day_trade_minute_curves import (  # noqa: E402
    _iter_jsonl,
    historical_minute_mark_has_source,
    missing_accepted_endpoints,
)
from stockagent.live.shioaji_schedule import (  # noqa: E402
    HISTORICAL_MAX_TRAFFIC_FRACTION,
    TAIPEI,
    historical_query_is_protected,
)
from stockagent.live.tw_day_trade_simulation import MARGIN_CARRY_CONTRACT
from downloader.download_shioaji_tx_futures_ticks import _valid_receipt  # noqa: E402
from downloader.download_tw_public_data import (  # noqa: E402
    _validated_taiex_session_dates,
)


def _object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return payload


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def _prewarm_public_history() -> dict[str, Any]:
    """Move the post-close all-minute projection cost off the first visitor.

    This is a best-effort read of the local sanitized gateway, not another
    source acceptance gate. The gateway's revision-keyed cache remains the
    authority for whether the response can be reused by a later visitor.
    """
    try:
        port = int(os.environ.get("STOCKAGENT_PUBLIC_DASHBOARD_PORT", "8770"))
        if not 1 <= port <= 65_535:
            raise ValueError("invalid local public-dashboard port")
    except ValueError:
        return {"status": "not_warmed", "reason": "invalid_local_port"}
    started = time.monotonic()
    connection = HTTPConnection("127.0.0.1", port, timeout=20)
    try:
        connection.request(
            "GET", "/tw-day-trade/api/history?range=all&resolution=1m&encoding=v2"
        )
        response = connection.getresponse()
        # Drain a bounded response so the gateway can finish writing cleanly.
        # The complete 2026-02-25 onward history is currently about 15 MiB.
        body = response.read(64 * 1024 * 1024 + 1)
        status = "warmed" if (
            response.status == 200
            and "application/json" in str(response.getheader("Content-Type") or "")
            and 0 < len(body) <= 64 * 1024 * 1024
        ) else "not_warmed"
        return {
            "status": status,
            "http_status": response.status,
            "response_bytes": len(body),
            "elapsed_seconds": round(time.monotonic() - started, 6),
        }
    except (OSError, TimeoutError, HTTPException) as error:
        return {
            "status": "not_warmed",
            "reason": type(error).__name__,
            "elapsed_seconds": round(time.monotonic() - started, 6),
        }
    finally:
        connection.close()


def _completed_scope(
    state_dir: Path,
) -> tuple[list[str], set[str]]:
    rebuild = _object(state_dir / "rebuild_receipt.json")
    completed = {
        str(session.get("session_date") or "")
        for session in rebuild.get("sessions") or ()
        if isinstance(session, dict)
        and isinstance(session.get("close"), dict)
        and session["close"].get("status") == "settled_official_close"
    }
    state = _object(state_dir / "state.json")
    modes = state.get("modes")
    if not isinstance(modes, dict) or not modes:
        raise RuntimeError("state has no active day-trade modes")
    markets = {str(market) for market in modes}
    # A one-time replay receipt stops at its deployment day; current state
    # retains only the latest session. Include intervening settled sessions
    # from the append-only engine ledger or their gaps disappear from audits.
    events_path = state_dir / "events.jsonl"
    if events_path.is_file():
        for event in _iter_jsonl(events_path):
            if event.get("event") != "closing_auction_settled" or event.get("market") not in markets:
                continue
            settled_at = datetime.fromisoformat(str(event["recorded_at"]))
            if settled_at.tzinfo is None:
                settled_at = settled_at.replace(tzinfo=TAIPEI)
            completed.add(settled_at.astimezone(TAIPEI).date().isoformat())
    session_dates = {
        str(mode.get("session_date") or "")
        for mode in modes.values()
        if isinstance(mode, dict)
    }
    current_is_terminal = bool(
        len(session_dates) == 1
        and all(
            isinstance(mode, dict)
            and not any(
                int(position.get("signed_shares") or 0) != 0 and not (
                    mode.get("margin_carry_contract") == MARGIN_CARRY_CONTRACT
                    and position.get("margin_carry_contract") == MARGIN_CARRY_CONTRACT)
                for position in (mode.get("positions") or {}).values()
                if isinstance(position, dict)
            )
            and (
                bool(mode.get("closing_auction_settled_at"))
                or str(mode.get("engine_status") or "") == "terminal"
            )
            for mode in modes.values()
        )
    )
    if current_is_terminal:
        completed.update(session_dates)
    return sorted(value for value in completed if value), markets


def _endpoint_source_signature(path: Path) -> list[int]:
    """Identify the exact append-only marks snapshot used by a prior preflight."""

    info = path.stat()
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]


def _missing_endpoints_with_cache(
    marks_path: Path,
    *,
    completed: list[str],
    markets: set[str],
    previous_status: Mapping[str, Any] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Reuse only a successful preflight of the same immutable ledger view."""

    before = _endpoint_source_signature(marks_path)
    cached = (previous_status or {}).get("accepted_endpoint_preflight")
    if (
        isinstance(cached, dict)
        and cached.get("contract_version") == 1
        and cached.get("source_signature") == before
        and cached.get("completed_session_dates") == completed
        and cached.get("markets") == sorted(markets)
        and cached.get("missing_endpoint_pairs") == 0
        and _endpoint_source_signature(marks_path) == before
    ):
        return [], {**cached, "reused": True}
    missing = missing_accepted_endpoints(
        _iter_jsonl(marks_path),
        start=date.fromisoformat(completed[0]),
        end=date.fromisoformat(completed[-1]),
        expected_sessions=completed,
        expected_markets=markets,
    )
    after = _endpoint_source_signature(marks_path)
    if before != after:
        raise RuntimeError("marks ledger changed during accepted endpoint preflight")
    return missing, {
        "contract_version": 1,
        "source_signature": after,
        "completed_session_dates": completed,
        "markets": sorted(markets),
        "missing_endpoint_pairs": len(missing),
        "reused": False,
    }


def _validate_current(
    state_dir: Path,
    *,
    completed_session_dates: list[str],
    expected_markets: set[str],
) -> dict[str, Any]:
    failures: list[str] = []
    result = _validate_minute_curve_coverage(
        state_dir,
        completed_session_dates=completed_session_dates,
        expected_markets=expected_markets,
        failures=failures,
    )
    if failures:
        raise RuntimeError("; ".join(failures))
    return result


def _tx_benchmark_source_state(
    tx_history_root: Path,
    completed_session: str,
    capture_root: Path | None = None,
) -> dict[str, Any]:
    """Validate the same retained-capture-first source used by the rebuild."""

    trading_date = date.fromisoformat(completed_session)
    capture_error: str | None = None
    if capture_root is not None:
        manifest_root = capture_root / "manifests" / f"trade_date={completed_session}"
        if any(manifest_root.glob("worker=*.json")):
            try:
                metadata, manifests = _tx_front_contract_metadata(
                    capture_root=capture_root, trading_date=trading_date
                )
                books = _tx_day_books(
                    capture_root=capture_root,
                    trading_date=trading_date,
                    contract_code=str(metadata["code"]),
                    end_at=datetime.combine(
                        trading_date, wall_time(13, 45), tzinfo=TAIPEI
                    ),
                )
            except (OSError, TypeError, ValueError, pl.exceptions.PolarsError) as exc:
                return {
                    "ready": False,
                    "trading_date": completed_session,
                    "status": "invalid_capture",
                    "reason": f"{type(exc).__name__}: {exc}",
                }
            try:
                minutes = _tx_complete_minute_books(
                    books,
                    trading_date=trading_date,
                    timestamp_column="snapshot_ts_ns",
                    epoch_utc=True,
                )
            except RuntimeError as exc:
                # Match the rebuild's fallback for incomplete minute coverage.
                capture_error = str(exc)
            except (OSError, TypeError, ValueError, pl.exceptions.PolarsError) as exc:
                return {
                    "ready": False,
                    "trading_date": completed_session,
                    "status": "invalid_capture",
                    "reason": f"{type(exc).__name__}: {exc}",
                }
            else:
                return {
                    "ready": True,
                    "trading_date": completed_session,
                    "status": "complete",
                    "source": "retained_shioaji_fop_book_1s",
                    "contract_code": metadata["code"],
                    "minutes": len(minutes),
                    "fresh_minutes": sum(fresh for _, _, fresh in minutes),
                    "manifest_receipts": manifests,
                }

    receipt_path = (
        tx_history_root
        / "receipts"
        / f"trading_date={completed_session}.json"
    )
    receipt = _valid_receipt(tx_history_root, trading_date)
    ready = bool(receipt is not None and receipt.get("status") == "complete")
    if ready:
        try:
            books, history_receipt = _tx_historical_day_books(
                history_root=tx_history_root, trading_date=trading_date
            )
            minutes = _tx_complete_minute_books(
                books,
                trading_date=trading_date,
                timestamp_column="event_ts",
                epoch_utc=False,
            )
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            return {
                "ready": False,
                "trading_date": completed_session,
                "status": "invalid_history",
                "reason": f"{type(exc).__name__}: {exc}",
                "receipt_path": str(receipt_path),
            }
        return {
            "ready": True,
            "trading_date": completed_session,
            "status": "complete",
            "source": "receipt_backed_shioaji_txfr1_historical_tick_l1",
            "minutes": len(minutes),
            "fresh_minutes": sum(fresh for _, _, fresh in minutes),
            "receipt": history_receipt,
            "capture_open_error": capture_error,
        }
    return {
        "ready": ready,
        "trading_date": completed_session,
        "receipt_path": str(receipt_path),
        "status": receipt.get("status") if receipt is not None else "missing_or_invalid",
        "capture_open_error": capture_error,
    }


def _stock_calendar_source_state(
    calendar_root: Path,
    completed_session: str,
) -> dict[str, Any]:
    """Require the same verified session calendar as the minute collector."""

    trading_date = date.fromisoformat(completed_session)
    try:
        sessions, digest = _validated_taiex_session_dates(
            calendar_root, trading_date, trading_date
        )
    except (OSError, RuntimeError, ValueError) as exc:
        return {
            "ready": False,
            "trading_date": completed_session,
            "status": "missing_or_invalid",
            "reason": f"{type(exc).__name__}: {exc}",
            "summary_path": str(
                (calendar_root / "twse_taiex_ohlc.summary.json").resolve()
            ),
        }
    return {
        "ready": trading_date in sessions,
        "trading_date": completed_session,
        "status": "complete" if trading_date in sessions else "not_a_verified_session",
        "sha256": digest,
        "summary_path": str(
            (calendar_root / "twse_taiex_ohlc.summary.json").resolve()
        ),
    }


def _inspect_strategy_price_provenance(
    state_dir: Path,
    *,
    completed_session_dates: list[str],
    expected_markets: set[str],
) -> dict[str, Any]:
    """Inspect completed-session opening and interior prices, not just timestamps."""

    expected_sessions = set(completed_session_dates)
    opening_by_session = {
        session_date: datetime.fromisoformat(f"{session_date}T09:01:00+08:00")
        for session_date in completed_session_dates
    }
    # The append-only ledger may contain multiple marks for one minute.  The
    # latest row is what the dashboard projects, so audit that exact row.
    source_by_key: dict[tuple[str, str, int], bool] = {}
    marks_path = state_dir / "marks.jsonl"
    with marks_path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            row = json.loads(line)
            if not isinstance(row, dict):
                continue
            session_date = str(row.get("session_date") or "")
            market = str(row.get("market") or "")
            if session_date not in expected_sessions or market not in expected_markets:
                continue
            try:
                minute = datetime.fromisoformat(str(row.get("minute") or ""))
            except ValueError:
                raise RuntimeError(f"marks.jsonl:{line_number}: invalid minute")
            try:
                opening = opening_by_session[session_date]
                offset = int((minute - opening).total_seconds() // 60)
            except TypeError:
                # A naive timestamp is not one of the +08:00 contract keys.
                continue
            if not (0 <= offset < 269 and minute == opening + timedelta(minutes=offset)):
                continue
            source_by_key[(session_date, market, offset)] = historical_minute_mark_has_source(row)
    audited_opening = sum(bool(source) for (_, _, offset), source in source_by_key.items() if offset == 0)
    audited_interior = sum(bool(source) for (_, _, offset), source in source_by_key.items() if offset > 0)
    expected_opening = len(completed_session_dates) * len(expected_markets)
    expected_interior = expected_opening * 268
    unverified_opening = expected_opening - audited_opening
    unverified_interior = expected_interior - audited_interior
    unverified_sample: list[str] = []
    if unverified_opening or unverified_interior:
        for session_date in sorted(completed_session_dates):
            for market in sorted(expected_markets):
                for offset in range(269):
                    if source_by_key.get((session_date, market, offset)):
                        continue
                    minute = opening_by_session[session_date] + timedelta(minutes=offset)
                    unverified_sample.append(
                        f"{session_date}:{market}:{minute.isoformat(timespec='minutes')}"
                    )
                    if len(unverified_sample) == 20:
                        break
                if len(unverified_sample) == 20:
                    break
            if len(unverified_sample) == 20:
                break
    return {
        "contract": "right_labelled_historical_last_trade_mark_v1",
        "expected_opening_rows": expected_opening,
        "audited_opening_rows": audited_opening,
        "unverified_opening_rows": unverified_opening,
        "expected_interior_rows": expected_interior,
        "audited_interior_rows": audited_interior,
        "unverified_interior_rows": unverified_interior,
        "unverified_sample": unverified_sample,
    }


def _proven_price_state_from_curve_validation(
    validation: Mapping[str, Any] | None,
    *,
    completed_session_dates: list[str],
    expected_markets: set[str],
) -> dict[str, Any] | None:
    """Reuse the stronger, hash-checked mark scan only when it covers 09:01."""

    expected_sessions = len(completed_session_dates)
    mode_count = len(expected_markets)
    if not (
        isinstance(validation, Mapping)
        and validation.get("required") is True
        and validation.get("contract") == MINUTE_CURVE_CONTRACT
        and validation.get("opening_marks_revalued_at_completed_minute") is True
        and validation.get("completed_session_dates") == completed_session_dates
        and validation.get("points_per_session_mode") == MINUTE_CURVE_SESSION_POINTS
        and validation.get("validated_rows")
        == expected_sessions * mode_count * MINUTE_CURVE_SESSION_POINTS
        and validation.get("unverified_historical_interior_rows") == 0
    ):
        return None
    opening = expected_sessions * mode_count
    interior = opening * (MINUTE_CURVE_SESSION_POINTS - 2)
    return {
        "contract": MINUTE_CURVE_CONTRACT,
        "basis": "hash_checked_complete_minute_curve_validation",
        "expected_opening_rows": opening,
        "audited_opening_rows": opening,
        "unverified_opening_rows": 0,
        "expected_interior_rows": interior,
        "audited_interior_rows": interior,
        "unverified_interior_rows": 0,
        "unverified_sample": [],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path("artifacts/live/tw_day_trade_simulation"),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path(
            "artifacts/data_repair/tw_day_trade_minute_curve/maintenance/current"
        ),
    )
    parser.add_argument(
        "--status-path",
        type=Path,
        default=Path("artifacts/operations/tw_day_trade_minute_curves/latest.json"),
    )
    parser.add_argument("--no-fetch", action="store_true")
    parser.add_argument(
        "--tx-history-root",
        type=Path,
        default=Path("data_tw_index_futures/shioaji_history/TXFR1"),
    )
    parser.add_argument(
        "--fop-capture-root",
        type=Path,
        default=Path("data_tw_index_derivatives_ticks/shioaji_fop_captures"),
    )
    parser.add_argument(
        "--calendar-root",
        type=Path,
        default=Path("data_tw_public"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    state_dir = args.state_dir.resolve()
    output_root = args.output_root.resolve()
    status_path = args.status_path.resolve()
    status_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = status_path.with_suffix(".lock")
    with lock_path.open("a+", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("[tw-day-trade-minute-curves] another maintenance run is active")
            return

        observed = datetime.now(TAIPEI)
        preflight_seconds: dict[str, float] = {}
        stage_started = time.monotonic()
        completed, markets = _completed_scope(state_dir)
        preflight_seconds["completed_scope"] = round(time.monotonic() - stage_started, 6)
        if not completed:
            payload = {
                "schema_version": 1,
                "status": "waiting_completed_session",
                "observed_at": observed.isoformat(timespec="seconds"),
                "simulation_only": True,
                "production_order_possible": False,
            }
            _atomic_json(status_path, payload)
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
            return

        try:
            previous_status = _object(status_path)
        except (OSError, ValueError):
            previous_status = None
        stage_started = time.monotonic()
        missing_endpoints, endpoint_preflight = _missing_endpoints_with_cache(
            state_dir / "marks.jsonl",
            completed=completed,
            markets=markets,
            previous_status=previous_status,
        )
        preflight_seconds["accepted_endpoints"] = round(time.monotonic() - stage_started, 6)
        if missing_endpoints:
            payload = {
                "schema_version": 1,
                "status": "waiting_accepted_endpoints",
                "observed_at": observed.isoformat(timespec="seconds"),
                "completed_session_dates": completed,
                "missing_endpoint_pairs": len(missing_endpoints),
                "missing_endpoints": missing_endpoints[:50],
                "accepted_endpoint_preflight": endpoint_preflight,
                "preflight_seconds": preflight_seconds,
                "reason": "Accepted entry/close ledger marks require canonical history repair; minute prices cannot reconstruct missed executions.",
                "simulation_only": True,
                "production_order_possible": False,
            }
            _atomic_json(status_path, payload)
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
            return

        # An attempted session is not a completed session: waiting_source also
        # records completed_session_dates. Always check the exact receipt before
        # starting the expensive benchmark rebuild, even on a retry.
        stage_started = time.monotonic()
        source_state = _tx_benchmark_source_state(
            getattr(
                args,
                "tx_history_root",
                Path("data_tw_index_futures/shioaji_history/TXFR1"),
            ).resolve(),
            completed[-1],
            getattr(
                args,
                "fop_capture_root",
                Path("data_tw_index_derivatives_ticks/shioaji_fop_captures"),
            ).resolve(),
        )
        preflight_seconds["benchmark_source"] = round(time.monotonic() - stage_started, 6)
        if not source_state["ready"]:
            payload = {
                "schema_version": 1,
                "status": "waiting_source",
                "failed_stage": "benchmark_source_preflight",
                "observed_at": observed.isoformat(timespec="seconds"),
                "completed_session_dates": completed,
                "source": source_state,
                "accepted_endpoint_preflight": endpoint_preflight,
                "preflight_seconds": preflight_seconds,
                "retry_contract": (
                    "A complete retained FOP capture or verified TXFR1 receipt "
                    "can supply the benchmark; retry on source change or the "
                    "post-close timer. No benchmark subprocess was started"
                ),
                "simulation_only": True,
                "production_order_possible": False,
            }
            _atomic_json(status_path, payload)
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
            return

        stage_started = time.monotonic()
        calendar_state = _stock_calendar_source_state(
            getattr(args, "calendar_root", Path("data_tw_public")).resolve(),
            completed[-1],
        )
        preflight_seconds["stock_calendar"] = round(time.monotonic() - stage_started, 6)
        if not calendar_state["ready"]:
            payload = {
                "schema_version": 1,
                "status": "waiting_source",
                "failed_stage": "stock_session_calendar_preflight",
                "observed_at": observed.isoformat(timespec="seconds"),
                "completed_session_dates": completed,
                "source": calendar_state,
                "accepted_endpoint_preflight": endpoint_preflight,
                "preflight_seconds": preflight_seconds,
                "retry_contract": (
                    "Retry on verified TAIEX calendar publication or the next "
                    "post-close timer; do not start price queries or a "
                    "benchmark rebuild without this source"
                ),
                "simulation_only": True,
                "production_order_possible": False,
            }
            _atomic_json(status_path, payload)
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
            return

        validation_clock = time.monotonic()
        check_clock = validation_clock
        try:
            strategy_validation = _validate_current(
                state_dir,
                completed_session_dates=completed,
                expected_markets=markets,
            )
        except (OSError, RuntimeError, TypeError, ValueError):
            strategy_validation = None
        preflight_seconds["strategy_validation"] = round(time.monotonic() - check_clock, 6)
        check_clock = time.monotonic()
        price_validation = _proven_price_state_from_curve_validation(
            strategy_validation,
            completed_session_dates=completed,
            expected_markets=markets,
        )
        if price_validation is None:
            try:
                price_validation = _inspect_strategy_price_provenance(
                    state_dir,
                    completed_session_dates=completed,
                    expected_markets=markets,
                )
            except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
                price_validation = None
        preflight_seconds["strategy_price_provenance"] = round(time.monotonic() - check_clock, 6)
        check_clock = time.monotonic()
        try:
            benchmark_validation = _validate_benchmarks(
                state_dir,
                completed_session_dates=completed,
            )
        except (OSError, RuntimeError, TypeError, ValueError):
            benchmark_validation = None
        preflight_seconds["benchmark_validation"] = round(time.monotonic() - check_clock, 6)
        preflight_seconds["existing_validation"] = round(time.monotonic() - validation_clock, 6)
        price_provenance_ready = bool(
            price_validation is not None
            and int(price_validation.get("unverified_opening_rows") or 0) == 0
            and int(price_validation.get("unverified_interior_rows") or 0) == 0
        )
        if (
            strategy_validation is not None
            and benchmark_validation is not None
            and price_provenance_ready
        ):
            payload = {
                "schema_version": 1,
                "status": "ready",
                "action": "no_op_already_complete",
                "observed_at": observed.isoformat(timespec="seconds"),
                "completed_session_dates": completed,
                "accepted_endpoint_preflight": endpoint_preflight,
                "stage_seconds": preflight_seconds,
                "validation": {
                    "strategy": strategy_validation,
                    "strategy_price_provenance": price_validation,
                    "benchmarks": benchmark_validation,
                },
                "simulation_only": True,
                "production_order_possible": False,
            }
            _atomic_json(status_path, payload)
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
            return

        # The worker always publishes rebuilt marks.  Skipping API fallback
        # does not make that write safe during the protected live window.
        if historical_query_is_protected(observed):
            payload = {
                "schema_version": 1,
                "status": "waiting_live_priority_window",
                "observed_at": observed.isoformat(timespec="seconds"),
                "completed_session_dates": completed,
                "strategy_price_provenance": price_validation,
                "retry_contract": (
                    "scheduled post-close attempts at 14:35, 15:30 and 18:00; "
                    "do not compete with live quote traffic before 14:31"
                ),
                "simulation_only": True,
                "production_order_possible": False,
            }
            _atomic_json(status_path, payload)
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
            return

        output_root.mkdir(parents=True, exist_ok=True)
        benchmark_command = [
            sys.executable,
            str(
                REPO_ROOT
                / "scripts"
                / "rebuild_tw_day_trade_benchmark_history.py"
            ),
            "--state-dir",
            str(state_dir),
            "--start-date",
            completed[0],
            "--end-date",
            completed[-1],
            "--fop-capture-root",
            str(getattr(
                args,
                "fop_capture_root",
                Path("data_tw_index_derivatives_ticks/shioaji_fop_captures"),
            ).resolve()),
            "--tx-history-root",
            str(getattr(
                args,
                "tx_history_root",
                Path("data_tw_index_futures/shioaji_history/TXFR1"),
            ).resolve()),
        ]
        benchmark_started = datetime.now(TAIPEI)
        benchmark_clock = time.monotonic()
        benchmark_process = subprocess.run(
            benchmark_command,
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        preflight_seconds["benchmark_rebuild"] = round(time.monotonic() - benchmark_clock, 6)
        if benchmark_process.returncode != 0:
            stderr_tail = benchmark_process.stderr.strip()[-4000:]
            stdout_tail = benchmark_process.stdout.strip()[-4000:]
            payload = {
                "schema_version": 1,
                "status": "failed",
                "failed_stage": "benchmark_history_rebuild",
                "observed_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
                "started_at": benchmark_started.isoformat(timespec="seconds"),
                "completed_session_dates": completed,
                "returncode": benchmark_process.returncode,
                "stage_seconds": preflight_seconds,
                "stderr_tail": stderr_tail,
                "stdout_tail": stdout_tail,
                "simulation_only": True,
                "production_order_possible": False,
            }
            _atomic_json(status_path, payload)
            if stdout_tail:
                print(stdout_tail, file=sys.stdout, flush=True)
            if stderr_tail:
                print(stderr_tail, file=sys.stderr, flush=True)
            raise RuntimeError(
                "benchmark-history rebuild failed with "
                f"{benchmark_process.returncode}"
            )
        if benchmark_process.stdout.strip():
            print(benchmark_process.stdout.strip(), flush=True)
        if benchmark_process.stderr.strip():
            print(benchmark_process.stderr.strip(), file=sys.stderr, flush=True)

        command = [
            sys.executable,
            str(REPO_ROOT / "scripts" / "rebuild_tw_day_trade_minute_curves.py"),
            "--state-dir",
            str(state_dir),
            "--start-date",
            completed[0],
            "--end-date",
            completed[-1],
            "--output-dir",
            str(output_root),
            "--simulation",
            "--max-traffic-fraction",
            str(HISTORICAL_MAX_TRAFFIC_FRACTION),
            "--publish",
        ]
        if not args.no_fetch:
            command.append("--fetch-missing-kbars")
        if price_validation is not None and int(price_validation.get("unverified_opening_rows") or 0) > 0:
            command.append("--revalue-opening-marks")
        has_margin_carry = any(m.get("margin_carry_contract") == MARGIN_CARRY_CONTRACT
                              for m in _object(state_dir / "state.json").get("modes", {}).values())
        if price_provenance_ready and (strategy_validation is not None or has_margin_carry):
            command.append("--validate-existing-strategy-marks")
        elif has_margin_carry:
            command.append("--revalue-carried-marks")
        elif price_validation is not None and not price_provenance_ready:
            command.append("--repair-unverified-strategy-marks")
        started = datetime.now(TAIPEI)
        minute_clock = time.monotonic()
        completed_process = subprocess.run(
            command,
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        preflight_seconds["minute_curve_rebuild"] = round(time.monotonic() - minute_clock, 6)
        if completed_process.returncode != 0:
            stderr_tail = completed_process.stderr.strip()[-4000:]
            stdout_tail = completed_process.stdout.strip()[-4000:]
            payload = {
                "schema_version": 1,
                "status": "failed",
                "observed_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
                "started_at": started.isoformat(timespec="seconds"),
                "completed_session_dates": completed,
                "returncode": completed_process.returncode,
                "stage_seconds": preflight_seconds,
                "stderr_tail": stderr_tail,
                "stdout_tail": stdout_tail,
                "simulation_only": True,
                "production_order_possible": False,
            }
            _atomic_json(status_path, payload)
            if stdout_tail:
                print(stdout_tail, file=sys.stdout, flush=True)
            if stderr_tail:
                print(stderr_tail, file=sys.stderr, flush=True)
            raise RuntimeError(
                f"minute-curve rebuild failed with {completed_process.returncode}"
            )
        if completed_process.stdout.strip():
            print(completed_process.stdout.strip(), flush=True)
        if completed_process.stderr.strip():
            print(completed_process.stderr.strip(), file=sys.stderr, flush=True)
        validation_clock = time.monotonic()
        strategy_validation = _validate_current(
            state_dir,
            completed_session_dates=completed,
            expected_markets=markets,
        )
        price_validation = _proven_price_state_from_curve_validation(
            strategy_validation,
            completed_session_dates=completed,
            expected_markets=markets,
        )
        if price_validation is None:
            price_validation = _inspect_strategy_price_provenance(
                state_dir,
                completed_session_dates=completed,
                expected_markets=markets,
            )
        if (int(price_validation.get("unverified_opening_rows") or 0) != 0
                or int(price_validation.get("unverified_interior_rows") or 0) != 0):
            raise RuntimeError(
                "minute-curve rebuild left unverified opening/interior strategy prices: "
                f"{price_validation['unverified_sample']}"
            )
        benchmark_validation = _validate_benchmarks(
            state_dir,
            completed_session_dates=completed,
        )
        preflight_seconds["post_validation"] = round(time.monotonic() - validation_clock, 6)
        public_history_prewarm = _prewarm_public_history()
        preflight_seconds["public_history_prewarm"] = public_history_prewarm.get(
            "elapsed_seconds", 0.0
        )
        payload = {
            "schema_version": 1,
            "status": "ready",
            "action": "benchmarks_and_minute_curves_rebuilt_and_published",
            "observed_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
            "started_at": started.isoformat(timespec="seconds"),
            "completed_session_dates": completed,
            "accepted_endpoint_preflight": endpoint_preflight,
            "stage_seconds": preflight_seconds,
            "public_history_prewarm": public_history_prewarm,
            "validation": {
                "strategy": strategy_validation,
                "strategy_price_provenance": price_validation,
                "benchmarks": benchmark_validation,
            },
            "simulation_only": True,
            "production_order_possible": False,
        }
        _atomic_json(status_path, payload)
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

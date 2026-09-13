#!/usr/bin/env python3
"""Validate and atomically promote a rebuilt TW day-trade paper ledger."""

from __future__ import annotations

import argparse
import ctypes
from datetime import date, datetime, timedelta
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import sys
from typing import Any, Callable, Mapping
import uuid
from zoneinfo import ZoneInfo

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_price_rules import move_price_ticks_numpy
from scripts.rebuild_tw_day_trade_minute_curves import (
    historical_minute_mark_has_source,
)


TAIPEI = ZoneInfo("Asia/Taipei")
AT_FDCWD = -100
RENAME_EXCHANGE = 2
HYBRID_ENTRY_POLICY = "causal_best_quote_else_adverse_open_tick"
HYBRID_REPLAY_CONTRACT = (
    "retrospective_historical_best_quote_else_adverse_open_tick_counterfactual"
)
PAPER_MARKET_ENTRY_POLICY = "market_at_best_quote_else_adverse_open_tick"
PAPER_MARKET_REPLAY_CONTRACT = (
    "retrospective_historical_best_quote_market_else_adverse_open_tick_counterfactual"
)
OFFICIAL_OPEN_ENTRY_POLICY = "official_open_at_09_01"
OFFICIAL_OPEN_REPLAY_CONTRACT = (
    "retrospective_official_session_open_at_09_01_counterfactual"
)
MINUTE_VWAP_0901_ENTRY_POLICY = "official_open_signal_0900_execute_0901_vwap"
MINUTE_VWAP_0901_REPLAY_CONTRACT = (
    "retrospective_official_open_signal_at_09_00_observed_09_01_minute_vwap_counterfactual"
)
MINUTE_PRICE_0901_REPLAY_CONTRACT = (
    "retrospective_official_open_signal_at_09_00_observed_09_01_"
    "minute_price_volume_capped_nav_counterfactual_v3"
)
MINUTE_PRICE_0901_REPLAY_CONTRACTS = {
    MINUTE_VWAP_0901_REPLAY_CONTRACT,
    MINUTE_PRICE_0901_REPLAY_CONTRACT,
    "retrospective_official_open_signal_at_09_00_observed_09_01_minute_price_counterfactual_v2",
}
MINUTE_CURVE_CONTRACT = "right_labelled_historical_last_trade_mark_v1"
MINUTE_CURVE_SESSION_POINTS = 270


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _load_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root is not an object: {path}")
    return payload


def _promotion_input_hashes(candidate: Path) -> dict[str, str]:
    """Bind the complete promoted history, not only its terminal state."""
    return {str(path.relative_to(candidate)): _sha256(path)
            for path in sorted(candidate.rglob("*"))
            if path.is_file() and not path.name.startswith(".")}


def _validate_minute_curve_coverage(
    candidate: Path,
    *,
    completed_session_dates: list[str],
    expected_markets: set[str],
    failures: list[str],
    require_carried_parity: bool = False,
) -> dict[str, Any]:
    """Require every completed replay session to ship a minute-grain curve."""

    if not completed_session_dates:
        return {
            "required": False,
            "completed_session_dates": [],
            "validated_rows": 0,
        }
    receipt_path = candidate / "minute_curve_receipt.json"
    marks_path = candidate / "marks.jsonl"
    benchmark_path = candidate / "benchmark_history.json"
    if not receipt_path.is_file():
        failures.append(
            "minute_curve_receipt.json is required for every completed replay session"
        )
        return {
            "required": True,
            "completed_session_dates": completed_session_dates,
            "validated_rows": 0,
        }
    try:
        receipt = _load_object(receipt_path)
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        failures.append(f"invalid minute_curve_receipt.json: {type(exc).__name__}:{exc}")
        return {
            "required": True,
            "completed_session_dates": completed_session_dates,
            "validated_rows": 0,
        }

    strategy = receipt.get("strategy")
    strategy = strategy if isinstance(strategy, dict) else {}
    coverage = receipt.get("coverage_after_fetch")
    coverage = coverage if isinstance(coverage, dict) else {}
    outputs = receipt.get("outputs")
    outputs = outputs if isinstance(outputs, dict) else {}
    if require_carried_parity:
        # Ledger arithmetic alone cannot detect using an execution High/Low
        # instead of the retained minute Close for inventory valuation.
        if not (
            receipt.get("carried_inventory_revalued_from_unchanged_executions") is True
            and receipt.get("independent_carried_valuation_parity_required") is True
            and receipt.get("independent_carried_valuation_parity_passed") is True
            and strategy.get("differing_original_equity_points") == 0
            and isinstance(strategy.get("maximum_original_equity_difference_twd"), (float, int))
            and 0 <= strategy["maximum_original_equity_difference_twd"] <= 1e-6
        ):
            failures.append("carried-history promotion requires independent minute valuation parity")
        fills_path = candidate / "fills.jsonl"
        if not fills_path.is_file() or receipt.get("unchanged_fills_sha256") != _sha256(fills_path):
            failures.append("carried minute valuation changed the accepted fills ledger")
    expected_rows = (
        len(completed_session_dates)
        * len(expected_markets)
        * MINUTE_CURVE_SESSION_POINTS
    )
    if receipt.get("simulation_only") is not True:
        failures.append("minute curve receipt is not simulation_only=true")
    if receipt.get("production_order_possible") is not False:
        failures.append("minute curve receipt does not prohibit production orders")
    if str(receipt.get("minute_contract") or "") != MINUTE_CURVE_CONTRACT:
        failures.append("minute curve receipt has the wrong one-minute contract")
    if receipt.get("linear_interpolation_used") is not False:
        failures.append("minute curve receipt permits linear interpolation")
    opening_revalued = receipt.get("opening_marks_revalued_at_completed_minute") is True
    if opening_revalued:
        if receipt.get("accepted_13_30_endpoints_preserved") is not True:
            failures.append("minute revaluation did not preserve accepted 13:30 endpoints")
        fills_path = marks_path.parent / "fills.jsonl"
        if not fills_path.is_file() or receipt.get("unchanged_fills_sha256") != _sha256(fills_path):
            failures.append("minute revaluation changed the accepted fills ledger")
    elif receipt.get("accepted_09_01_strategy_and_13_30_endpoints_preserved") is not True:
        failures.append("minute curve receipt did not preserve accepted endpoints")
    if int(coverage.get("missing_pairs") or 0) != 0:
        failures.append("minute curve receipt still has missing symbol-date pairs")
    if sorted(str(value) for value in strategy.get("session_dates") or ()) != completed_session_dates:
        failures.append("minute curve session dates do not match completed replay sessions")
    if {str(value) for value in strategy.get("markets") or ()} != expected_markets:
        failures.append("minute curve markets do not match the promoted mode set")
    if int(strategy.get("generated_rows") or 0) != expected_rows:
        failures.append(
            "minute curve generated row count does not equal "
            f"{MINUTE_CURVE_SESSION_POINTS} points per completed session and mode"
        )
    if str(receipt.get("start_date") or "") != completed_session_dates[0]:
        failures.append("minute curve start date does not match the replay start")
    if str(receipt.get("end_date") or "") != completed_session_dates[-1]:
        failures.append("minute curve end date does not match the latest completed replay")

    for path, key in (
        (marks_path, "marks"),
        (benchmark_path, "benchmark_history"),
    ):
        output = outputs.get(key)
        output = output if isinstance(output, dict) else {}
        if not path.is_file():
            failures.append(f"minute curve output is missing: {path.name}")
        elif str(output.get("sha256") or "") != _sha256(path):
            failures.append(f"minute curve output hash mismatch: {path.name}")

    observed: dict[tuple[str, str], set[str]] = {}
    unverified_interior_rows = 0
    unverified_interior_samples: list[str] = []
    if marks_path.is_file():
        with marks_path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                try:
                    row = json.loads(line)
                except (TypeError, ValueError, json.JSONDecodeError):
                    failures.append(f"marks.jsonl:{line_number}: invalid JSON")
                    continue
                if not isinstance(row, dict):
                    continue
                session_date = str(row.get("session_date") or "")
                market = str(row.get("market") or "")
                if session_date not in completed_session_dates or market not in expected_markets:
                    continue
                minute = str(row.get("minute") or "")
                observed.setdefault((session_date, market), set()).add(minute)
                if (
                    (minute[11:16] != "13:30" if opening_revalued
                     else minute[11:16] not in {"09:01", "13:30"})
                    and not historical_minute_mark_has_source(row)
                ):
                    unverified_interior_rows += 1
                    if len(unverified_interior_samples) < 20:
                        unverified_interior_samples.append(
                            f"{session_date}:{market}:{minute}"
                        )
    for session_date in completed_session_dates:
        base = datetime.fromisoformat(f"{session_date}T09:01:00+08:00")
        expected_minutes = {
            (base + timedelta(minutes=offset)).isoformat(timespec="minutes")
            for offset in range(MINUTE_CURVE_SESSION_POINTS)
        }
        for market in expected_markets:
            actual = observed.get((session_date, market), set())
            if actual != expected_minutes:
                failures.append(
                    f"{session_date}/{market}: minute curve does not contain exactly "
                    "09:01 through 13:30"
                )
    if unverified_interior_rows:
        failures.append(
            "minute curve contains interior rows without auditable historical "
            f"price provenance: {unverified_interior_rows}; "
            f"sample={unverified_interior_samples}"
        )
    return {
        "required": True,
        "contract": str(receipt.get("minute_contract") or ""),
        "completed_session_dates": completed_session_dates,
        "points_per_session_mode": MINUTE_CURVE_SESSION_POINTS,
        "opening_marks_revalued_at_completed_minute": opening_revalued,
        "validated_rows": sum(len(values) for values in observed.values()),
        "audited_historical_interior_rows": (
            len(completed_session_dates) * len(expected_markets) * 268
            - unverified_interior_rows
        ),
        "unverified_historical_interior_rows": unverified_interior_rows,
        "receipt_sha256": _sha256(receipt_path),
    }


def _validate_benchmarks(
    state_dir: Path, *, completed_session_dates: list[str],
) -> dict[str, Any]:
    """Shared maintenance/promotion gate: date coverage is not an envelope claim."""
    marks = _load_object(state_dir / "benchmark_history.json").get("marks")
    if not isinstance(marks, list):
        raise RuntimeError("benchmark history has no marks")
    expected_sessions = set(completed_session_dates)
    contracts = {
        "benchmark_0050": ("09:00", "13:30", 271),
        "benchmark_2330": ("09:00", "13:30", 271),
        "benchmark_tx_continuous": ("08:45", "13:44", 300),
    }
    counts = {}
    for benchmark_id, (first_clock, last_clock, expected_points) in contracts.items():
        by_session: dict[str, list[str]] = {}
        for row in marks:
            if (isinstance(row, dict) and row.get("benchmark_id") == benchmark_id
                    and str(row.get("session_date") or "") in expected_sessions):
                by_session.setdefault(str(row["session_date"]), []).append(str(row.get("minute") or ""))
        if set(by_session) != expected_sessions:
            raise RuntimeError(f"{benchmark_id} missing completed sessions: {sorted(expected_sessions - set(by_session))[:20]}")
        for session_date, minutes in by_session.items():
            if len(minutes) != expected_points or len(set(minutes)) != expected_points:
                raise RuntimeError(f"{benchmark_id}:{session_date} minute count {len(minutes)}/{len(set(minutes))} != {expected_points}")
            clocks = sorted(value[11:16] for value in minutes)
            if clocks[0] != first_clock or clocks[-1] != last_clock:
                raise RuntimeError(f"{benchmark_id}:{session_date} minute boundary {clocks[0]}..{clocks[-1]} != {first_clock}..{last_clock}")
            first = datetime.fromisoformat(f"{session_date}T{first_clock}:00+08:00")
            expected = {first + timedelta(minutes=i) for i in range(expected_points)}
            try:
                observed = {datetime.fromisoformat(value) for value in minutes}
            except ValueError as exc:
                raise RuntimeError(f"{benchmark_id}:{session_date} invalid minute timestamp") from exc
            if observed != expected:
                raise RuntimeError(f"{benchmark_id}:{session_date} incomplete exact one-minute grid")
        counts[benchmark_id] = sum(map(len, by_session.values()))
    return {"completed_session_dates": completed_session_dates,
            "points_per_session": {key: value[2] for key, value in contracts.items()}, "rows": counts}


def _validate_hybrid_signal_ledger(
    candidate: Path,
    *,
    expected_best_quote_fills: int,
    expected_synthetic_fallback_fills: int,
    failures: list[str],
) -> dict[str, int]:
    path = candidate / "signals.jsonl"
    if not path.is_file():
        failures.append("hybrid replay has no signals.jsonl audit ledger")
        return {"best_quote_fills": 0, "synthetic_fallback_fills": 0}
    exact_count = 0
    fallback_count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                failures.append(f"signals.jsonl:{line_number}: invalid JSON")
                continue
            policy = str(row.get("entry_fill_policy") or "")
            if policy not in {HYBRID_ENTRY_POLICY, PAPER_MARKET_ENTRY_POLICY}:
                continue
            filled_shares = int(row.get("filled_shares") or 0)
            if filled_shares <= 0:
                continue
            side = str(row.get("side") or "")
            execution_price = float(row.get("execution_price") or math.nan)
            is_fallback = row.get("synthetic_fallback_fill") is True
            if is_fallback:
                fallback_count += 1
                if (
                    row.get("synthetic_fill") is not True
                    or row.get("status") != "forced_synthetic_fill"
                    or int(row.get("entry_price_offset_ticks") or 0) != 1
                    or row.get("entry_price_source")
                    != "official_daily_session_open:adverse_one_legal_tick_fallback"
                ):
                    failures.append(
                        f"signals.jsonl:{line_number}: malformed adverse-tick fallback"
                    )
                    continue
                if (
                    policy == PAPER_MARKET_ENTRY_POLICY
                    and row.get("paper_market_fill") is not True
                ):
                    failures.append(
                        f"signals.jsonl:{line_number}: paper fallback is not labelled"
                    )
                sizing_price = float(row.get("sizing_open_price") or math.nan)
                session_date = str(row.get("session_date") or "")
                direction = 1 if side == "long" else -1 if side == "short" else 0
                if direction == 0 or not math.isfinite(sizing_price):
                    failures.append(
                        f"signals.jsonl:{line_number}: fallback has invalid side/open"
                    )
                    continue
                expected_price = float(
                    move_price_ticks_numpy(
                        np.asarray([sizing_price], dtype=np.float64),
                        direction,
                        np.asarray([session_date]),
                    )[0]
                )
                lower = float(row.get("lower_limit") or math.nan)
                upper = float(row.get("upper_limit") or math.nan)
                if math.isfinite(lower):
                    expected_price = max(expected_price, lower)
                if math.isfinite(upper):
                    expected_price = min(expected_price, upper)
                if not math.isclose(
                    execution_price, expected_price, rel_tol=0.0, abs_tol=1e-9
                ):
                    failures.append(
                        f"signals.jsonl:{line_number}: fallback price is not adverse one tick"
                    )
            else:
                exact_count += 1
                price_key = "ask" if side == "long" else "bid"
                quote_price = float(row.get(price_key) or math.nan)
                source_quote_at = str(row.get("historical_source_quote_at") or "")
                try:
                    source_time = datetime.fromisoformat(source_quote_at)
                except ValueError:
                    source_time = None
                common_invalid = bool(
                    row.get("synthetic_fill") is not False
                    or row.get("status") not in {"ready", "partial_depth"}
                    or not math.isclose(
                        execution_price, quote_price, rel_tol=0.0, abs_tol=1e-9
                    )
                    or source_time is None
                    or source_time.date().isoformat()
                    != str(row.get("session_date") or "")
                    or source_time.hour != 9
                    or source_time.minute != 0
                )
                depth_invalid = bool(
                    policy == HYBRID_ENTRY_POLICY
                    and int(row.get("top_book_capacity_shares") or 0) < filled_shares
                )
                paper_market_invalid = bool(
                    policy == PAPER_MARKET_ENTRY_POLICY
                    and (
                        row.get("paper_market_fill") is not True
                        or filled_shares != int(row.get("requested_shares") or 0)
                    )
                )
                if common_invalid or depth_invalid or paper_market_invalid:
                    failures.append(
                        f"signals.jsonl:{line_number}: malformed historical best-quote fill"
                    )
    if exact_count != expected_best_quote_fills:
        failures.append(
            f"signals ledger best-quote fills={exact_count} receipt={expected_best_quote_fills}"
        )
    if fallback_count != expected_synthetic_fallback_fills:
        failures.append(
            "signals ledger fallback fills="
            f"{fallback_count} receipt={expected_synthetic_fallback_fills}"
        )
    return {
        "best_quote_fills": exact_count,
        "synthetic_fallback_fills": fallback_count,
    }


def _validate_official_open_signal_ledger(
    candidate: Path,
    *,
    expected_fills: int,
    failures: list[str],
) -> dict[str, int]:
    path = candidate / "signals.jsonl"
    if not path.is_file():
        failures.append("official-open replay has no signals.jsonl audit ledger")
        return {"official_open_fills": 0}
    fill_count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                failures.append(f"signals.jsonl:{line_number}: invalid JSON")
                continue
            if str(row.get("entry_fill_policy") or "") != OFFICIAL_OPEN_ENTRY_POLICY:
                continue
            filled_shares = int(row.get("filled_shares") or 0)
            if filled_shares <= 0:
                continue
            fill_count += 1
            try:
                execution_price = float(row.get("execution_price"))
                sizing_open = float(row.get("sizing_open_price"))
                recorded_at = datetime.fromisoformat(str(row.get("recorded_at")))
            except (TypeError, ValueError):
                failures.append(
                    f"signals.jsonl:{line_number}: malformed official-open fill"
                )
                continue
            if (
                row.get("counterfactual_open_price_fill") is not True
                or row.get("synthetic_fill") is not False
                or row.get("synthetic_fallback_fill") is not False
                or row.get("paper_market_fill") is not False
                or int(row.get("entry_price_offset_ticks") or 0) != 0
                or not math.isclose(
                    execution_price, sizing_open, rel_tol=0.0, abs_tol=1e-9
                )
                or recorded_at.hour != 9
                or recorded_at.minute != 1
            ):
                failures.append(
                    f"signals.jsonl:{line_number}: official-open/09:01 contract mismatch"
                )
    if fill_count != expected_fills:
        failures.append(
            f"signals ledger official-open fills={fill_count} receipt={expected_fills}"
        )
    return {"official_open_fills": fill_count}


def _validate_official_open_fill_ledger(
    candidate: Path,
    *,
    expected_fills: int,
    failures: list[str],
) -> dict[str, int]:
    path = candidate / "fills.jsonl"
    if not path.is_file():
        failures.append("official-open replay has no fills.jsonl audit ledger")
        return {"fill_ledger_official_open_fills": 0}
    fill_count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                failures.append(f"fills.jsonl:{line_number}: invalid JSON")
                continue
            if (
                str(row.get("purpose") or "") != "entry"
                or str(row.get("fill_contract") or "")
                != OFFICIAL_OPEN_REPLAY_CONTRACT
            ):
                continue
            fill_count += 1
            try:
                price = float(row.get("price"))
                fill_at = datetime.fromisoformat(str(row.get("fill_at")))
            except (TypeError, ValueError):
                failures.append(
                    f"fills.jsonl:{line_number}: malformed official-open fill"
                )
                continue
            if (
                str(row.get("entry_fill_policy") or "")
                != OFFICIAL_OPEN_ENTRY_POLICY
                or int(row.get("entry_price_offset_ticks") or 0) != 0
                or row.get("counterfactual_open_price_fill") is not True
                or row.get("synthetic_fill") is not False
                or row.get("synthetic_fallback_fill") is not False
                or row.get("paper_market_fill") is not False
                or not math.isfinite(price)
                or price <= 0.0
                or fill_at.hour != 9
                or fill_at.minute != 1
            ):
                failures.append(
                    f"fills.jsonl:{line_number}: official-open/09:01 contract mismatch"
                )
    if fill_count != expected_fills:
        failures.append(
            f"fills ledger official-open fills={fill_count} receipt={expected_fills}"
        )
    return {"fill_ledger_official_open_fills": fill_count}


def _verify_absent_0901_prices(candidate: Path, pairs: set[tuple[str, str]]) -> dict[str, Any]:
    """No execution quote is not a download failure, but must have source proof.

    A returned, receipt-verified session may start trading after 09:01. Never
    backdate its later first trade. Missing session receipts still fail closed.
    """
    from collections import defaultdict
    import polars as pl
    from scripts.rebuild_tw_day_trade_minute_curves import MinutePriceStore
    receipt = _load_object(candidate / "minute_curve_receipt.json")
    roots = [Path(row["root"]) for row in receipt.get("local_minute_sources", [])]
    store = MinutePriceStore(roots, (), require_receipts=True)
    path_pairs: dict[Path, set[tuple[str, str]]] = defaultdict(set)
    found, executable = set(), set()
    for symbol, day in sorted(pairs):
        for root in store.kbar_roots:
            for path in store._chunk_paths(root, symbol, day):
                path_pairs[path].add((symbol, day))
                found.add((symbol, day))
    for path, selected in path_pairs.items():
        days = sorted({date.fromisoformat(day) for _, day in selected})
        schema = pl.read_parquet_schema(path)
        volume = "volume_shares" if "volume_shares" in schema else "Volume"
        observed = (pl.scan_parquet(path).filter(pl.col("date").is_in(days)
            & (pl.col("ts").dt.hour() == 9) & (pl.col("ts").dt.minute() == 1)
            & (pl.col(volume).is_null() | ~pl.col(volume).is_finite() | (pl.col(volume) != 0)))
            .select("date").unique().collect())
        # A positive-volume row with a corrupt price is also a repair, not an
        # accepted absence. Do not filter it out just because Close is invalid.
        executable.update((path.parent.name, str(day)) for day in observed["date"].to_list())
    store.assert_sources_unchanged()
    missing, contradicting = pairs - found, pairs & executable
    if missing or contradicting:
        raise RuntimeError(f"09:01 unavailable-price source proof failed: missing={len(missing)} {sorted(missing)[:10]}; positive_volume={len(contradicting)} {sorted(contradicting)[:10]}")
    return {"receipt_verified_absent_price_pairs": len(pairs), "source_files_checked": len(path_pairs),
            "contract": "receipt_verified_session_without_0901_trade_no_fill_v1",
            "raw_tick_source_signatures": {str(path): signature for path, signature in store._verified_raw_tick_signatures.items()},
            "source_signatures": {str(path): signature for path, (signature, _) in store._verified_chunk_dates.items()}}


def _validate_0901_minute_price_signal_ledger(
    candidate: Path,
    *,
    expected_fills: int,
    failures: list[str],
    require_capacity: bool = False,
    allow_receipted_absence: bool = False,
) -> dict[str, Any]:
    path = candidate / "signals.jsonl"
    if not path.is_file():
        failures.append("09:01 minute-price replay has no signals.jsonl audit ledger")
        return {"minute_price_0901_fills": 0, "minute_vwap_0901_fills": 0}
    fill_count = 0
    absent_pairs: set[tuple[str, str]] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                failures.append(f"signals.jsonl:{line_number}: invalid JSON")
                continue
            if str(row.get("entry_fill_policy") or "") != MINUTE_VWAP_0901_ENTRY_POLICY:
                continue
            if require_capacity and int(row.get("requested_shares") or 0) > 0 and row.get("reason") in {
                "observed_09_01_minute_price_unavailable", "observed_09_01_minute_liquidity_unavailable",
            }:
                # A verified zero-volume minute can legitimately have no fill;
                # absent price/volume evidence is a separate data-health gate.
                volume = row.get("minute_kbar_volume_lots")
                if volume is None or row.get("execution_price") is None:
                    if (allow_receipted_absence and int(row.get("filled_shares") or 0) == 0
                            and int(row.get("reduction_filled_shares") or 0) == 0):
                        absent_pairs.add((str(row.get("symbol") or ""), str(row.get("session_date") or "")))
                    else:
                        failures.append(f"signals.jsonl:{line_number}: unresolved v3 entry source")
            if int(row.get("filled_shares") or 0) <= 0:
                continue
            fill_count += 1
            if require_capacity:
                try:
                    volume = float(row.get("minute_kbar_volume_lots"))
                    nav = float(row.get("sizing_nav_twd"))
                    capacity = int(math.floor(volume * .5)) * 1000
                    valid = (math.isfinite(volume) and volume >= 0 and math.isfinite(nav) and nav > 0
                             and int(row["filled_shares"]) <= capacity
                             and row.get("filled_weight_basis") == "session_start_account_nav")
                except (TypeError, ValueError, OverflowError):
                    valid = False
                if not valid:
                    failures.append(f"signals.jsonl:{line_number}: missing or exceeded v3 NAV/liquidity proof")
            try:
                execution_price = float(row.get("execution_price"))
                sizing_open = float(row.get("sizing_open_price"))
                recorded_at = datetime.fromisoformat(str(row.get("recorded_at")))
            except (TypeError, ValueError):
                failures.append(
                    f"signals.jsonl:{line_number}: malformed 09:01 minute-price fill"
                )
                continue
            source = str(row.get("entry_price_source") or "")
            method = str(row.get("entry_price_method") or "minute_vwap")
            if (
                row.get("counterfactual_0901_price_fill") is not True
                or row.get("counterfactual_open_price_fill") is not False
                or row.get("synthetic_fill") is not False
                or row.get("synthetic_fallback_fill") is not False
                or row.get("paper_market_fill") is not False
                or int(row.get("entry_price_offset_ticks") or 0) != 0
                or not math.isfinite(execution_price)
                or execution_price <= 0.0
                or not math.isfinite(sizing_open)
                or sizing_open <= 0.0
                or "0901" not in source.replace(":", "").replace("_", "")
                or method not in {"minute_vwap", "minute_close"}
                or recorded_at.hour != 9
                or recorded_at.minute != 1
            ):
                failures.append(
                    f"signals.jsonl:{line_number}: 09:00-open/09:01-minute-price contract mismatch"
                )
    if fill_count != expected_fills:
        failures.append(
            f"signals ledger 09:01 minute-price fills={fill_count} receipt={expected_fills}"
        )
    absence = None
    if absent_pairs:
        try:
            absence = _verify_absent_0901_prices(candidate, absent_pairs)
        except (OSError, ValueError, RuntimeError) as exc:
            failures.append(str(exc))
    return {
        "minute_price_0901_fills": fill_count,
        "minute_vwap_0901_fills": fill_count,
        "absent_execution_price_validation": absence,
    }


def _validate_0901_minute_price_fill_ledger(
    candidate: Path,
    *,
    expected_fills: int,
    failures: list[str],
) -> dict[str, int]:
    path = candidate / "fills.jsonl"
    if not path.is_file():
        failures.append("09:01 minute-price replay has no fills.jsonl audit ledger")
        return {
            "fill_ledger_minute_price_0901_fills": 0,
            "fill_ledger_minute_vwap_0901_fills": 0,
        }
    fill_count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                failures.append(f"fills.jsonl:{line_number}: invalid JSON")
                continue
            if (
                str(row.get("purpose") or "") != "entry"
                or str(row.get("fill_contract") or "")
                not in MINUTE_PRICE_0901_REPLAY_CONTRACTS
            ):
                continue
            fill_count += 1
            try:
                price = float(row.get("price"))
                fill_at = datetime.fromisoformat(str(row.get("fill_at")))
            except (TypeError, ValueError):
                failures.append(
                    f"fills.jsonl:{line_number}: malformed 09:01 minute-price fill"
                )
                continue
            if (
                str(row.get("entry_fill_policy") or "")
                != MINUTE_VWAP_0901_ENTRY_POLICY
                or row.get("counterfactual_0901_price_fill") is not True
                or row.get("counterfactual_open_price_fill") is not False
                or row.get("synthetic_fill") is not False
                or row.get("synthetic_fallback_fill") is not False
                or row.get("paper_market_fill") is not False
                or str(row.get("entry_price_method") or "minute_vwap")
                not in {"minute_vwap", "minute_close"}
                or not math.isfinite(price)
                or price <= 0.0
                or fill_at.hour != 9
                or fill_at.minute != 1
            ):
                failures.append(
                    f"fills.jsonl:{line_number}: 09:01 minute-price contract mismatch"
                )
    if fill_count != expected_fills:
        failures.append(
            f"fills ledger 09:01 minute-price fills={fill_count} receipt={expected_fills}"
        )
    return {
        "fill_ledger_minute_price_0901_fills": fill_count,
        "fill_ledger_minute_vwap_0901_fills": fill_count,
    }


def _validate_rebuild(
    candidate: Path,
    *,
    expected_markets: set[str],
    allow_current_open_session: bool = False,
    allow_margin_carry: bool = False,
) -> dict[str, Any]:
    input_hashes = _promotion_input_hashes(candidate)
    receipt_path = candidate / "rebuild_receipt.json"
    state_path = candidate / "state.json"
    receipt = _load_object(receipt_path)
    state = _load_object(state_path)
    failures: list[str] = []
    if receipt.get("simulation_only") is not True:
        failures.append("rebuild receipt is not simulation_only=true")
    if receipt.get("production_order_possible") is not False:
        failures.append("rebuild receipt does not prohibit production orders")
    replay_contract = receipt.get("replay_contract")
    receipt_entry_contract = (
        str(replay_contract.get("entry") or "")
        if isinstance(replay_contract, dict)
        else ""
    )
    modes = state.get("modes")
    if not isinstance(modes, dict):
        failures.append("state.modes is not an object")
        modes = {}
    if set(modes) != expected_markets:
        failures.append(f"mode set={sorted(modes)} expected={sorted(expected_markets)}")
    margin_audit = None
    if allow_margin_carry:
        from scripts.audit_tw_day_trade_margin_replay import audit
        from stockagent.live.tw_day_trade_simulation import MARGIN_CARRY_CONTRACT
        from stockagent.live.tw_share_replacement import ODD_LOT_BOARD_PRICE
        if allow_current_open_session:
            failures.append("carried-history promotion requires completed sessions")
        if (not isinstance(replay_contract, dict) or replay_contract.get("residual") != MARGIN_CARRY_CONTRACT
                or replay_contract.get("odd_lot_execution_policy") != ODD_LOT_BOARD_PRICE
                or any(m.get("margin_carry_contract") != MARGIN_CARRY_CONTRACT
                       or m.get("odd_lot_execution_policy") != ODD_LOT_BOARD_PRICE for m in modes.values())):
            failures.append("carried-history promotion requires the explicit margin and odd-lot contracts")
        else:
            margin_audit = audit(candidate)
            if not margin_audit.get("full_requested_range_passed"):
                failures.append(f"carried-history full source/accounting audit failed: {margin_audit.get('errors')}")

    sessions = receipt.get("sessions")
    if not isinstance(sessions, list) or not sessions:
        failures.append("rebuild has no sessions")
        sessions = []
    session_dates: list[str] = []
    registrations = 0
    best_quote_fills = 0
    synthetic_fallback_fills = 0
    official_open_fills = 0
    minute_vwap_0901_fills = 0
    minute_price_0901_fills = 0
    current_date = datetime.now(TAIPEI).date().isoformat()
    current_open_session: str | None = None
    for session_index, session in enumerate(sessions):
        if not isinstance(session, dict):
            failures.append("malformed session receipt")
            continue
        session_date = str(session.get("session_date") or "")
        session_dates.append(session_date)
        close = session.get("close")
        close_status = close.get("status") if isinstance(close, dict) else None
        is_allowed_current_open = bool(
            allow_current_open_session
            and session_index == len(sessions) - 1
            and session_date == current_date
            and close_status == "current_session_left_open_for_live_service"
        )
        if is_allowed_current_open:
            current_open_session = session_date
        elif close_status != "settled_official_close" and not (
            allow_margin_carry and close_status == "assumed_margin_inventory_carried"
            and margin_audit and margin_audit.get("full_requested_range_passed")
        ):
            failures.append(f"{session_date}: session is not settled at official close")
        mode_rows = session.get("modes")
        if not isinstance(mode_rows, list):
            failures.append(f"{session_date}: modes is not a list")
            continue
        observed_markets = {
            str(row.get("market") or "") for row in mode_rows if isinstance(row, dict)
        }
        if observed_markets != expected_markets:
            failures.append(
                f"{session_date}: modes={sorted(observed_markets)} "
                f"expected={sorted(expected_markets)}"
            )
        for row in mode_rows:
            if not isinstance(row, dict):
                continue
            market = str(row.get("market") or "")
            if row.get("register_result") != "registered":
                failures.append(
                    f"{session_date}/{market}: register_result="
                    f"{row.get('register_result')!r}"
                )
            else:
                registrations += 1
            entry = row.get("entry")
            if isinstance(entry, dict):
                policy = str(entry.get("entry_fill_policy") or "")
                fill_count = int(entry.get("entry_fill_count") or 0)
                exact_count = int(entry.get("entry_best_quote_fill_count") or 0)
                fallback_count = int(
                    entry.get("entry_synthetic_fallback_fill_count") or 0
                )
                official_open_count = int(
                    entry.get("entry_official_open_fill_count") or 0
                )
                minute_vwap_0901_count = int(
                    entry.get("entry_0901_vwap_fill_count") or 0
                )
                minute_close_0901_count = int(
                    entry.get("entry_0901_close_fill_count") or 0
                )
                minute_price_0901_count = int(
                    entry.get("entry_0901_minute_price_fill_count")
                    or minute_vwap_0901_count
                    or 0
                )
                best_quote_fills += exact_count
                synthetic_fallback_fills += fallback_count
                official_open_fills += official_open_count
                minute_vwap_0901_fills += minute_vwap_0901_count
                minute_price_0901_fills += minute_price_0901_count
                if policy == MINUTE_VWAP_0901_ENTRY_POLICY:
                    if receipt_entry_contract not in MINUTE_PRICE_0901_REPLAY_CONTRACTS:
                        failures.append(
                            f"{session_date}/{market}: 09:01 minute-price replay contract is not explicit"
                        )
                    if minute_price_0901_count != fill_count:
                        failures.append(
                            f"{session_date}/{market}: 09:01 minute-price fill counts do not reconcile"
                        )
                    if (
                        receipt_entry_contract != MINUTE_VWAP_0901_REPLAY_CONTRACT
                        and minute_vwap_0901_count + minute_close_0901_count
                        != minute_price_0901_count
                    ):
                        failures.append(
                            f"{session_date}/{market}: 09:01 VWAP+Close method counts do not reconcile"
                        )
                    if (
                        entry.get("entry_fill_is_synthetic") is True
                        or fallback_count
                        or official_open_count
                    ):
                        failures.append(
                            f"{session_date}/{market}: 09:01 minute-price fill uses a forbidden synthetic fallback"
                        )
                if policy == OFFICIAL_OPEN_ENTRY_POLICY:
                    if receipt_entry_contract != OFFICIAL_OPEN_REPLAY_CONTRACT:
                        failures.append(
                            f"{session_date}/{market}: official-open replay contract is not explicit"
                        )
                    if official_open_count != fill_count:
                        failures.append(
                            f"{session_date}/{market}: official-open fill counts do not reconcile"
                        )
                    if entry.get("entry_fill_is_synthetic") is True or fallback_count:
                        failures.append(
                            f"{session_date}/{market}: official-open fill uses a synthetic tick"
                        )
                if policy in {HYBRID_ENTRY_POLICY, PAPER_MARKET_ENTRY_POLICY}:
                    required_contract = (
                        PAPER_MARKET_REPLAY_CONTRACT
                        if policy == PAPER_MARKET_ENTRY_POLICY
                        else HYBRID_REPLAY_CONTRACT
                    )
                    if receipt_entry_contract != required_contract:
                        failures.append(
                            f"{session_date}/{market}: replay contract is not explicit"
                        )
                    if exact_count + fallback_count != fill_count:
                        failures.append(
                            f"{session_date}/{market}: exact+fallback fill counts do not reconcile"
                        )
                elif fallback_count or entry.get("entry_fill_is_synthetic") is True:
                    failures.append(
                        f"{session_date}/{market}: synthetic fallback is not labelled hybrid"
                    )
            if is_allowed_current_open:
                if (
                    not isinstance(entry, dict)
                    or entry.get("engine_status") != "active"
                ):
                    failures.append(
                        f"{session_date}/{market}: current entry is not active"
                    )
            else:
                after_close = row.get("after_close")
                if (
                    not isinstance(after_close, dict)
                    or (int(after_close.get("open_position_rows") or 0) != 0 and not allow_margin_carry)
                ):
                    failures.append(f"{session_date}/{market}: not flat after close")
                if receipt_entry_contract == MINUTE_PRICE_0901_REPLAY_CONTRACT and isinstance(after_close, dict):
                    if int(after_close.get("terminal_flatten_count") or 0):
                        failures.append(f"{session_date}/{market}: v3 uses synthetic terminal flatten")

    if session_dates != sorted(set(session_dates)):
        failures.append("session dates are duplicated or not strictly increasing")

    final_open_positions: dict[str, int] = {}
    ending_equity: dict[str, float] = {}
    for market, mode_value in modes.items():
        mode = mode_value if isinstance(mode_value, dict) else {}
        mode_policy = str(mode.get("entry_fill_policy") or "")
        mode_is_hybrid = mode_policy == HYBRID_ENTRY_POLICY
        mode_is_paper_market = mode_policy == PAPER_MARKET_ENTRY_POLICY
        mode_is_official_open = mode_policy == OFFICIAL_OPEN_ENTRY_POLICY
        mode_is_0901_vwap = mode_policy == MINUTE_VWAP_0901_ENTRY_POLICY
        if mode_policy == "synthetic_open_tick" or (
            mode.get("entry_fill_is_synthetic") is True
            and not (mode_is_hybrid or mode_is_paper_market)
        ):
            failures.append(
                f"{market}: legacy synthetic open-tick replay cannot be promoted"
            )
        if mode_is_hybrid and receipt_entry_contract != HYBRID_REPLAY_CONTRACT:
            failures.append(f"{market}: hybrid final state has no matching receipt")
        if (
            mode_is_paper_market
            and receipt_entry_contract != PAPER_MARKET_REPLAY_CONTRACT
        ):
            failures.append(
                f"{market}: paper-market final state has no matching receipt"
            )
        if (
            mode_is_official_open
            and receipt_entry_contract != OFFICIAL_OPEN_REPLAY_CONTRACT
        ):
            failures.append(
                f"{market}: official-open final state has no matching receipt"
            )
        if (
            mode_is_0901_vwap
            and receipt_entry_contract not in MINUTE_PRICE_0901_REPLAY_CONTRACTS
        ):
            failures.append(
                f"{market}: 09:01 minute-price final state has no matching receipt"
            )
        positions = mode.get("positions")
        if not isinstance(positions, dict):
            positions = {}
        open_count = sum(
            int(position.get("signed_shares") or 0) != 0
            for position in positions.values()
            if isinstance(position, dict)
        )
        final_open_positions[str(market)] = open_count
        if current_open_session is None and open_count and not allow_margin_carry:
            failures.append(f"{market}: final open positions={open_count}")
        if current_open_session is not None:
            if str(mode.get("session_date") or "") != current_open_session:
                failures.append(
                    f"{market}: final session_date is not {current_open_session}"
                )
            if mode.get("engine_status") != "active":
                failures.append(f"{market}: final current-session engine is not active")
            if mode.get("counterfactual_open_replay") is not True:
                failures.append(
                    f"{market}: current session is not counterfactual replay"
                )
            allowed_contracts = {"retrospective_observed_best_quote_counterfactual"}
            if mode_is_hybrid:
                allowed_contracts.add(HYBRID_REPLAY_CONTRACT)
            if mode_is_paper_market:
                allowed_contracts.add(PAPER_MARKET_REPLAY_CONTRACT)
            if mode_is_official_open:
                allowed_contracts.add(OFFICIAL_OPEN_REPLAY_CONTRACT)
            if mode_is_0901_vwap:
                allowed_contracts.update(MINUTE_PRICE_0901_REPLAY_CONTRACTS)
            if mode.get("entry_fill_contract") not in allowed_contracts:
                failures.append(f"{market}: current entry fill contract is invalid")
            if mode.get("entry_fill_is_synthetic") is not False and not (
                mode_is_hybrid or mode_is_paper_market
            ):
                failures.append(
                    f"{market}: current entries are not received-book fills"
                )
            for symbol, position_value in positions.items():
                position = position_value if isinstance(position_value, dict) else {}
                if int(position.get("signed_shares") or 0) == 0:
                    continue
                for price_key in ("entry_price", "sizing_open_price"):
                    try:
                        value = float(position.get(price_key))
                    except (TypeError, ValueError):
                        value = float("nan")
                    if not (value > 0.0 and value < float("inf")):
                        failures.append(
                            f"{market}/{symbol}: invalid {price_key}={value!r}"
                        )
                if position.get("counterfactual_open_replay") is not True:
                    failures.append(
                        f"{market}/{symbol}: position is not counterfactual replay"
                    )
                if mode_is_0901_vwap and (
                    position.get("counterfactual_0901_price_fill") is not True
                    or position.get("counterfactual_open_price_fill") is not False
                ):
                    failures.append(
                        f"{market}/{symbol}: position is not an observed 09:01 minute-price fill"
                    )
                if (
                    position.get("entry_fill_is_synthetic") is not False
                    and not (mode_is_hybrid or mode_is_paper_market)
                ):
                    failures.append(
                        f"{market}/{symbol}: position is not a received-book fill"
                    )
        ending_equity[str(market)] = float(mode.get("total_equity_twd") or 0.0)

    signal_ledger_validation = {
        "best_quote_fills": best_quote_fills,
        "synthetic_fallback_fills": synthetic_fallback_fills,
    }
    if receipt_entry_contract in {
        HYBRID_REPLAY_CONTRACT,
        PAPER_MARKET_REPLAY_CONTRACT,
    }:
        signal_ledger_validation = _validate_hybrid_signal_ledger(
            candidate,
            expected_best_quote_fills=best_quote_fills,
            expected_synthetic_fallback_fills=synthetic_fallback_fills,
            failures=failures,
        )
    elif receipt_entry_contract == OFFICIAL_OPEN_REPLAY_CONTRACT:
        signal_ledger_validation = _validate_official_open_signal_ledger(
            candidate,
            expected_fills=official_open_fills,
            failures=failures,
        )
        signal_ledger_validation.update(
            _validate_official_open_fill_ledger(
                candidate,
                expected_fills=official_open_fills,
                failures=failures,
            )
        )
    elif receipt_entry_contract in MINUTE_PRICE_0901_REPLAY_CONTRACTS:
        expected_minute_price_fills = (
            minute_price_0901_fills
            if receipt_entry_contract != MINUTE_VWAP_0901_REPLAY_CONTRACT
            else minute_vwap_0901_fills
        )
        signal_ledger_validation = _validate_0901_minute_price_signal_ledger(
            candidate,
            expected_fills=expected_minute_price_fills,
            failures=failures,
            require_capacity=receipt_entry_contract == MINUTE_PRICE_0901_REPLAY_CONTRACT,
            allow_receipted_absence=allow_margin_carry,
        )
        signal_ledger_validation.update(
            _validate_0901_minute_price_fill_ledger(
                candidate,
                expected_fills=expected_minute_price_fills,
                failures=failures,
            )
        )

    completed_session_dates = [
        session_date
        for session_date in session_dates
        if session_date != current_open_session
    ]
    minute_curve_validation = _validate_minute_curve_coverage(
        candidate,
        completed_session_dates=completed_session_dates,
        expected_markets=expected_markets,
        failures=failures,
        require_carried_parity=allow_margin_carry,
    )
    benchmark_validation = None
    if allow_margin_carry:
        try:
            benchmark_validation = _validate_benchmarks(candidate, completed_session_dates=completed_session_dates)
        except (OSError, ValueError, RuntimeError) as exc:
            failures.append(f"carried-history benchmark minute coverage failed: {exc}")

    if input_hashes != _promotion_input_hashes(candidate):
        failures.append("candidate history changed during validation")
    if failures:
        raise RuntimeError(f"replay promotion validation failed ({len(failures)} failures): " + "; ".join(failures[:50]))
    return {
        "session_dates": session_dates,
        "promotion_input_hashes": input_hashes,
        "session_count": len(session_dates),
        "registrations": registrations,
        "best_quote_fills": best_quote_fills,
        "synthetic_fallback_fills": synthetic_fallback_fills,
        "official_open_fills": official_open_fills,
        "minute_vwap_0901_fills": minute_vwap_0901_fills,
        "minute_price_0901_fills": minute_price_0901_fills,
        "signal_ledger_validation": signal_ledger_validation,
        "minute_curve_validation": minute_curve_validation,
        "benchmark_minute_validation": benchmark_validation,
        "mode_set": sorted(expected_markets),
        "final_open_positions": final_open_positions,
        "margin_carry_audit": margin_audit,
        "ending_equity_twd": ending_equity,
        "allow_current_open_session": bool(allow_current_open_session),
        "current_open_session": current_open_session,
        "rebuild_receipt_sha256": _sha256(receipt_path),
        "state_sha256": _sha256(state_path),
    }


def _acquire_engine_lock(directory: Path):
    path = directory / ".engine.lock"
    path.touch(exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise RuntimeError(
            f"paper ledger still has a live writer: {directory}"
        ) from None
    return handle


def _exchange_directories(left: Path, right: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = libc.renameat2
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        AT_FDCWD,
        os.fsencode(left),
        AT_FDCWD,
        os.fsencode(right),
        RENAME_EXCHANGE,
    )
    if result != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-dir", type=Path, required=True)
    parser.add_argument("--candidate-dir", type=Path, required=True)
    parser.add_argument(
        "--expected-market",
        action="append",
        required=True,
        help="Repeat once for every and only intended active paper mode.",
    )
    parser.add_argument(
        "--allow-current-open-session",
        action="store_true",
        help=(
            "Permit only the final Taipei-today session to remain open after "
            "validating its explicitly recorded counterfactual fill contract."
        ),
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate the candidate and print acceptance without exchanging directories.",
    )
    parser.add_argument("--allow-margin-carry", action="store_true",
                        help="Explicit user-approved carried-history contract; requires full source/accounting audit and a flat old live account.")
    return parser


def _revalidate_margin_sources(candidate: Path, acceptance: dict[str, Any]) -> None:
    """Recheck the exact source evidence after expensive validation and drain."""
    audit = acceptance["margin_carry_audit"]
    for name, digest in audit["source_hashes"].items():
        if _sha256(candidate / name) != digest:
            raise RuntimeError(f"audited carried candidate changed: {name}")
    for name, digest in audit["corporate_action_source_hashes"].items():
        if _sha256(Path(name)) != digest:
            raise RuntimeError(f"audited corporate-action source changed: {name}")
    for name, expected in audit.get("minute_source_signatures", {}).items():
        stat = Path(name).stat()
        if [stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns] != list(expected):
            raise RuntimeError(f"audited fill-minute source changed: {name}")
    absence = acceptance["signal_ledger_validation"].get("absent_execution_price_validation") or {}
    for name, expected in absence.get("source_signatures", {}).items():
        path = Path(name)
        actual = [[p.stat().st_size, p.stat().st_mtime_ns, p.stat().st_ctime_ns]
                  for p in (path, path.with_suffix(".receipt.json"))]
        if actual != [list(value) for value in expected]:
            raise RuntimeError(f"audited absent-minute source changed: {name}")
    for name, expected in absence.get("raw_tick_source_signatures", {}).items():
        stat = Path(name).stat()
        if [stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns] != list(expected):
            raise RuntimeError(f"audited raw tick source changed: {name}")


def main(*, before_exchange: Callable[[], None] | None = None) -> None:
    args = build_parser().parse_args()
    live = args.live_dir.resolve(strict=True)
    candidate = args.candidate_dir.resolve(strict=True)
    if live == candidate or live in candidate.parents or candidate in live.parents:
        raise ValueError("live and candidate directories must be separate siblings")
    if live.stat().st_dev != candidate.stat().st_dev:
        raise RuntimeError("atomic directory exchange requires the same filesystem")
    expected_markets = {str(value).strip() for value in args.expected_market}
    if not expected_markets or "" in expected_markets:
        raise ValueError("--expected-market values must be non-empty")
    acceptance = _validate_rebuild(
        candidate,
        expected_markets=expected_markets,
        allow_current_open_session=bool(args.allow_current_open_session),
        allow_margin_carry=bool(args.allow_margin_carry),
    )
    if args.validate_only:
        print(
            json.dumps(
                {"status": "validated", "acceptance": acceptance},
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return

    # The service coordinator can drain writers only AFTER expensive source
    # validation, avoiding minutes of needless Gateway downtime. The callback
    # cannot bypass validation; the exact inputs are rechecked under locks.
    if before_exchange is not None:
        before_exchange()
    live_lock = _acquire_engine_lock(live)
    candidate_lock = _acquire_engine_lock(candidate)
    exchanged = False
    try:
        # Recheck under both writer locks. A source-backed carried candidate
        # must never erase a still-open pre-existing live account.
        if acceptance["promotion_input_hashes"] != _promotion_input_hashes(candidate):
            raise RuntimeError("candidate history changed after validation")
        old_state = _load_object(live / "state.json")
        if any(int(p.get("signed_shares") or 0) for m in old_state.get("modes", {}).values()
               for p in m.get("positions", {}).values()):
            raise RuntimeError("refusing promotion over open live paper inventory")
        if _sha256(candidate / "state.json") != acceptance["state_sha256"] or _sha256(candidate / "rebuild_receipt.json") != acceptance["rebuild_receipt_sha256"]:
            raise RuntimeError("candidate changed after validation")
        if args.allow_margin_carry:
            _revalidate_margin_sources(candidate, acceptance)
            from stockagent.live.market_config import load_market_config
            from stockagent.live.tw_share_replacement import ODD_LOT_BOARD_PRICE
            for market in expected_markets:
                config = load_market_config(REPO_ROOT / "services/discord_bot/markets" / f"{market}.yaml")
                if not config.day_trade_residual_margin_conversion or config.day_trade_odd_lot_execution_policy != ODD_LOT_BOARD_PRICE:
                    raise RuntimeError(f"runtime margin/odd-lot policy not enabled: {market}")
        _exchange_directories(live, candidate)
        exchanged = True
        promotion_receipt = {
            "schema_version": 3,
            "promoted_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
            "simulation_only": True,
            "production_order_possible": False,
            "promotion_method": "same_filesystem_atomic_directory_exchange",
            "live_directory": str(live),
            "rollback_directory": str(candidate),
            "acceptance": acceptance,
        }
        _atomic_json(live / "promotion_receipt.json", promotion_receipt)
    except Exception:
        if exchanged:
            _exchange_directories(live, candidate)
        raise
    finally:
        candidate_lock.close()
        live_lock.close()

    print(
        json.dumps(
            {
                "status": "promoted",
                "live_directory": str(live),
                "rollback_directory": str(candidate),
                "acceptance": acceptance,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

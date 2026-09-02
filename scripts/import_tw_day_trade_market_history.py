#!/usr/bin/env python3
"""Append one receipt-validated market history without replacing live state.

This is the safe bridge between an immutable replay candidate and an already
running multi-mode paper ledger.  Only completed historical sessions are
copied.  Current-session state, positions, signals, and marks remain owned by
the live engine.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any, BinaryIO, Iterable, Mapping
import urllib.request
import uuid
from zoneinfo import ZoneInfo


TAIPEI = ZoneInfo("Asia/Taipei")
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
LEDGERS = ("signals.jsonl", "orders.jsonl", "fills.jsonl", "marks.jsonl", "events.jsonl")
ENGINE_SERVICE = "stockagent-tw-day-trade-simulation.service"
DISCORD_SERVICE = "stockagent-discord-bot.service"
PUBLIC_SERVICE = "stockagent-public-dashboards.service"


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


def _session_date(row: Mapping[str, Any]) -> str:
    explicit = str(row.get("session_date") or "")[:10]
    if explicit:
        return explicit
    value = str(row.get("recorded_at") or row.get("fill_at") or "").strip()
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=TAIPEI)
    return parsed.astimezone(TAIPEI).date().isoformat()


def _selected_line(
    line: bytes, *, market_token: bytes, market: str, dates: frozenset[str]
) -> tuple[bool, str]:
    if market_token not in line:
        return False, ""
    row = json.loads(line)
    if not isinstance(row, dict) or str(row.get("market") or "") != market:
        return False, ""
    session_date = _session_date(row)
    return session_date in dates, session_date


def _filter_ledger(
    source: Path,
    destination: BinaryIO | None,
    *,
    market: str,
    dates: frozenset[str],
) -> dict[str, Any]:
    digest = hashlib.sha256()
    count = 0
    bytes_selected = 0
    commission_rebate_twd = 0.0
    dates_selected: Counter[str] = Counter()
    market_token = f'"{market}"'.encode("utf-8")
    with source.open("rb") as handle:
        for line in handle:
            if not line.strip():
                continue
            selected, session_date = _selected_line(
                line,
                market_token=market_token,
                market=market,
                dates=dates,
            )
            if not selected:
                continue
            if not line.endswith(b"\n"):
                line += b"\n"
            digest.update(line)
            count += 1
            bytes_selected += len(line)
            dates_selected[session_date] += 1
            row = json.loads(line)
            try:
                commission_rebate_twd += float(
                    row.get("commission_rebate_accrued_twd") or 0.0
                )
            except (TypeError, ValueError):
                raise ValueError(f"invalid commission rebate in {source}")
            if destination is not None:
                destination.write(line)
    return {
        "rows": count,
        "bytes": bytes_selected,
        "sha256": digest.hexdigest(),
        "rows_by_session": dict(sorted(dates_selected.items())),
        "commission_rebate_accrued_twd": commission_rebate_twd,
    }


def _source_dates(candidate: Path, *, start_date: str, end_date: str) -> list[str]:
    receipt = _object(candidate / "minute_curve_receipt.json")
    strategy = receipt.get("strategy") or {}
    dates = sorted(
        str(value)
        for value in (strategy.get("session_dates") or ())
        if start_date <= str(value) <= end_date
    )
    if not dates or dates[0] != start_date or dates[-1] != end_date:
        raise RuntimeError(
            f"candidate minute receipt does not cover {start_date}..{end_date}"
        )
    if bool(receipt.get("linear_interpolation_used")):
        raise RuntimeError("candidate minute receipt used linear interpolation")
    if not bool(receipt.get("accepted_09_01_strategy_and_13_30_endpoints_preserved")):
        raise RuntimeError("candidate minute endpoints were not accepted")
    return dates


def _stage_ledgers(
    candidate: Path,
    stage_root: Path,
    *,
    market: str,
    dates: frozenset[str],
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for filename in LEDGERS:
        source = candidate / filename
        if not source.is_file():
            raise FileNotFoundError(source)
        staged = stage_root / filename
        with staged.open("wb") as destination:
            stats = _filter_ledger(
                source,
                destination,
                market=market,
                dates=dates,
            )
            destination.flush()
            os.fsync(destination.fileno())
        output[filename] = {**stats, "stage_path": str(staged)}
    mark_counts = output["marks.jsonl"]["rows_by_session"]
    invalid = {date: mark_counts.get(date, 0) for date in dates if mark_counts.get(date, 0) != 270}
    if invalid:
        raise RuntimeError(f"candidate minute curve is not exactly 270 points per session: {invalid}")
    if output["signals.jsonl"]["rows"] <= 0:
        raise RuntimeError("candidate contains no historical signal rows")
    return output


def _target_stats(
    live: Path, *, market: str, dates: frozenset[str]
) -> dict[str, dict[str, Any]]:
    return {
        filename: _filter_ledger(
            live / filename,
            None,
            market=market,
            dates=dates,
        )
        for filename in LEDGERS
    }


def _append_file(source: Path, target: Path) -> None:
    descriptor = os.open(target, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        with source.open("rb") as handle:
            while chunk := handle.read(8 << 20):
                view = memoryview(chunk)
                while view:
                    written = os.write(descriptor, view)
                    if written <= 0:
                        raise OSError(f"short append to {target}")
                    view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _copy_position_history(
    candidate: Path,
    live: Path,
    *,
    market: str,
    dates: Iterable[str],
) -> dict[str, int]:
    copied = 0
    identical = 0
    for session_date in dates:
        source = candidate / "position_history" / session_date / f"{market}.json"
        if not source.is_file():
            continue
        target = live / "position_history" / session_date / source.name
        if target.is_file():
            if hashlib.sha256(target.read_bytes()).digest() != hashlib.sha256(source.read_bytes()).digest():
                raise RuntimeError(f"position history conflict: {target}")
            identical += 1
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(target.suffix + f".tmp.{uuid.uuid4().hex}")
        try:
            shutil.copyfile(source, temporary)
            with temporary.open("rb") as handle:
                os.fsync(handle.fileno())
            os.replace(temporary, target)
            directory = os.open(target.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            temporary.unlink(missing_ok=True)
        copied += 1
    return {"copied": copied, "already_identical": identical}


def _closing_mark(candidate: Path, *, market: str, session_date: str) -> dict[str, Any]:
    selected: dict[str, Any] | None = None
    source = candidate / "marks.jsonl"
    market_token = f'"{market}"'.encode("utf-8")
    with source.open("rb") as handle:
        for line in handle:
            if market_token not in line or session_date.encode("ascii") not in line:
                continue
            row = json.loads(line)
            if (
                isinstance(row, dict)
                and str(row.get("market") or "") == market
                and _session_date(row) == session_date
                and (
                    selected is None
                    or str(row.get("minute") or row.get("recorded_at") or "")
                    > str(selected.get("minute") or selected.get("recorded_at") or "")
                )
            ):
                selected = row
    if selected is None:
        raise RuntimeError(f"candidate has no closing mark for {market} {session_date}")
    if int(selected.get("open_position_count") or 0) != 0:
        raise RuntimeError(f"candidate closing mark is not flat for {market} {session_date}")
    open_net = float(selected.get("open_net_liquidation_pnl_twd") or 0.0)
    if abs(open_net) > 0.01:
        raise RuntimeError(f"candidate closing mark has non-zero open PnL: {open_net}")
    return selected


def _current_market_marks(
    live: Path, *, market: str, session_date: str
) -> dict[str, dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    source = live / "marks.jsonl"
    market_token = f'"{market}"'.encode("utf-8")
    date_token = session_date.encode("ascii")
    with source.open("rb") as handle:
        for line in handle:
            if market_token not in line or date_token not in line:
                continue
            row = json.loads(line)
            if (
                not isinstance(row, dict)
                or str(row.get("market") or "") != market
                or _session_date(row) != session_date
            ):
                continue
            minute = str(row.get("minute") or "")
            if minute:
                selected[minute] = row
    return selected


def _apply_equity_carry(
    *,
    candidate: Path,
    live: Path,
    market: str,
    end_date: str,
    historical_commission_rebate_twd: float,
) -> dict[str, Any]:
    """Seed a newly deployed live mode from its last completed replay close."""

    from stockagent.live.tw_day_trade_simulation import (
        TwDayTradeSimulationEngine,
        _append_jsonl_many,
    )

    closing = _closing_mark(candidate, market=market, session_date=end_date)
    carry_twd = float(closing.get("cumulative_realized_net_pnl_twd") or 0.0)
    closing_equity = float(closing.get("total_equity_twd") or 0.0)
    initial_capital = float(closing.get("initial_capital_twd") or 0.0)
    if abs((initial_capital + carry_twd) - closing_equity) > 0.01:
        raise RuntimeError("candidate close does not reconcile to initial capital plus realized PnL")

    engine = TwDayTradeSimulationEngine(live)
    mode = (engine.state.get("modes") or {}).get(market)
    if not isinstance(mode, dict):
        raise RuntimeError(f"live mode is absent: {market}")
    current_session = str(mode.get("session_date") or "")
    if not current_session or current_session <= end_date:
        return {
            "status": "not_required",
            "source_session_date": end_date,
            "current_session_date": current_session or None,
        }

    existing_source = str(mode.get("equity_carry_source_session") or "")
    if existing_source:
        if existing_source != end_date or abs(
            float(mode.get("equity_carry_in_twd") or 0.0) - carry_twd
        ) > 0.01:
            raise RuntimeError("live mode has a conflicting equity carry marker")
        return {
            "status": "already_applied",
            "source_session_date": end_date,
            "current_session_date": current_session,
            "carry_in_twd": carry_twd,
        }

    marks = _current_market_marks(live, market=market, session_date=current_session)
    if not marks:
        raise RuntimeError(f"live mode has no current-session marks: {market} {current_session}")
    ordered = [marks[key] for key in sorted(marks)]
    first_cumulative = float(ordered[0].get("cumulative_realized_net_pnl_twd") or 0.0)
    latest_marks_already_corrected = all(
        str(row.get("equity_carry_source_session") or "") == end_date
        for row in ordered
    )
    if latest_marks_already_corrected:
        mark_delta = 0.0
        state_delta = carry_twd
    elif abs(first_cumulative) <= 0.01:
        mark_delta = carry_twd
        state_delta = carry_twd
    elif abs(first_cumulative - carry_twd) <= 0.01:
        mark_delta = 0.0
        state_delta = 0.0
    else:
        raise RuntimeError(
            "cannot prove current-session equity carry baseline: "
            f"first_cumulative={first_cumulative} expected_zero_or={carry_twd}"
        )

    corrected: list[dict[str, Any]] = []
    if mark_delta:
        for row in ordered:
            adjusted = dict(row)
            adjusted["cumulative_realized_net_pnl_twd"] = (
                float(row.get("cumulative_realized_net_pnl_twd") or 0.0) + mark_delta
            )
            adjusted["total_equity_twd"] = (
                float(row.get("total_equity_twd") or 0.0) + mark_delta
            )
            adjusted["equity_carry_correction_twd"] = mark_delta
            adjusted["equity_carry_source_session"] = end_date
            adjusted["supersedes_same_market_minute"] = True
            corrected.append(adjusted)
        _append_jsonl_many(engine.marks_path, corrected)

    if state_delta:
        mode["cumulative_realized_net_pnl_twd"] = (
            float(mode.get("cumulative_realized_net_pnl_twd") or 0.0) + state_delta
        )
        mode["cumulative_commission_rebate_accrued_twd"] = (
            float(mode.get("cumulative_commission_rebate_accrued_twd") or 0.0)
            + float(historical_commission_rebate_twd)
        )
    mode["equity_carry_source_session"] = end_date
    mode["equity_carry_in_twd"] = carry_twd
    mode["equity_carry_closing_equity_twd"] = closing_equity
    mode["equity_carry_applied_at"] = datetime.now(TAIPEI).isoformat(timespec="seconds")
    mode["total_equity_twd"] = (
        float(mode.get("initial_capital_twd") or 0.0)
        + float(mode.get("cumulative_realized_net_pnl_twd") or 0.0)
        + float(mode.get("open_net_liquidation_pnl_twd") or 0.0)
    )
    engine._persist(datetime.now(TAIPEI))
    return {
        "status": "applied" if state_delta or mark_delta else "adopted_existing",
        "source_session_date": end_date,
        "current_session_date": current_session,
        "carry_in_twd": carry_twd,
        "source_closing_equity_twd": closing_equity,
        "corrected_current_session_minutes": len(corrected),
        "state_delta_twd": state_delta,
        "historical_commission_rebate_twd": (
            float(historical_commission_rebate_twd) if state_delta else 0.0
        ),
    }


def _wait_active(service: str, *, timeout_seconds: float = 90.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if subprocess.run(
            ["systemctl", "is-active", "--quiet", service], check=False
        ).returncode == 0:
            return
        time.sleep(1.0)
    raise RuntimeError(f"service did not become active: {service}")


def _json_get(url: str, *, timeout: float = 180.0) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        if int(response.status) != 200:
            raise RuntimeError(f"GET {url} returned HTTP {response.status}")
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise RuntimeError(f"GET {url} did not return an object")
    return payload


def _verify_visibility(
    *, market: str, start_date: str, end_date: str
) -> dict[str, Any]:
    endpoints: dict[str, Any] = {}
    query = f"range=all&start_date={start_date}&end_date={end_date}"
    for name, url in {
        "local": f"http://127.0.0.1:8766/api/history?{query}",
        "public_gateway": f"http://127.0.0.1:8770/tw-day-trade/api/history?{query}",
    }.items():
        payload = _json_get(url)
        rows = [
            row
            for row in (payload.get("history") or ())
            if isinstance(row, Mapping) and str(row.get("series_id") or "") == market
        ]
        observed_dates: list[str] = []
        for row in rows:
            session_date = str(row.get("session_date") or "")[:10]
            if not session_date and row.get("minute"):
                try:
                    minute = datetime.fromisoformat(str(row["minute"]))
                    if minute.tzinfo is None:
                        minute = minute.replace(tzinfo=TAIPEI)
                    session_date = minute.astimezone(TAIPEI).date().isoformat()
                except ValueError:
                    session_date = ""
            if session_date:
                observed_dates.append(session_date)
        observed_dates = sorted(set(observed_dates))
        if not observed_dates or observed_dates[0] != start_date or observed_dates[-1] != end_date:
            raise RuntimeError(
                f"{name} history does not expose {market} {start_date}..{end_date}: "
                f"{observed_dates[:1]}..{observed_dates[-1:] if observed_dates else []}"
            )
        endpoints[name] = {
            "status": 200,
            "returned_curve_points_after_bounded_downsampling": len(rows),
            "first_session_date": observed_dates[0],
            "last_session_date": observed_dates[-1],
        }
    signal_url = (
        "http://127.0.0.1:8770/tw-day-trade/api/signals?"
        f"mode={market}&date={start_date}&offset=0&limit=1"
    )
    signals = _json_get(signal_url)
    if int(signals.get("total") or 0) <= 0:
        raise RuntimeError(f"public signal detail does not expose {market} on {start_date}")
    endpoints["public_signal_detail"] = {
        "status": 200,
        "session_date": start_date,
        "total": int(signals.get("total") or 0),
    }
    return endpoints


def _verify_equity_continuity(
    *, candidate: Path, live: Path, market: str, end_date: str
) -> dict[str, Any]:
    state = _object(live / "state.json")
    mode = (state.get("modes") or {}).get(market) or {}
    current_session = str(mode.get("session_date") or "")
    if not current_session or current_session <= end_date:
        return {"status": "not_required", "current_session_date": current_session or None}
    closing = _closing_mark(candidate, market=market, session_date=end_date)
    prior_equity = float(closing.get("total_equity_twd") or 0.0)
    marks = _current_market_marks(live, market=market, session_date=current_session)
    if not marks:
        raise RuntimeError("cannot verify equity continuity without current marks")
    first = marks[sorted(marks)[0]]
    first_equity = float(first.get("total_equity_twd") or 0.0)
    first_open_net = float(first.get("open_net_liquidation_pnl_twd") or 0.0)
    continuity_residual = first_equity - (prior_equity + first_open_net)
    if abs(continuity_residual) > 0.01:
        raise RuntimeError(
            f"current first equity does not inherit prior close: residual={continuity_residual}"
        )
    state_residual = float(mode.get("total_equity_twd") or 0.0) - (
        float(mode.get("initial_capital_twd") or 0.0)
        + float(mode.get("cumulative_realized_net_pnl_twd") or 0.0)
        + float(mode.get("open_net_liquidation_pnl_twd") or 0.0)
    )
    if abs(state_residual) > 0.01:
        raise RuntimeError(f"live state equity does not reconcile: residual={state_residual}")
    return {
        "status": "passed",
        "source_session_date": end_date,
        "current_session_date": current_session,
        "prior_closing_equity_twd": prior_equity,
        "current_first_equity_twd": first_equity,
        "current_first_open_net_pnl_twd": first_open_net,
        "continuity_residual_twd": continuity_residual,
        "state_reconciliation_residual_twd": state_residual,
        "corrected_current_session_mark_count": sum(
            str(row.get("equity_carry_source_session") or "") == end_date
            for row in marks.values()
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-dir", type=Path, required=True)
    parser.add_argument("--live-dir", type=Path, required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()

    candidate = args.candidate_dir.resolve()
    live = args.live_dir.resolve()
    dates_list = _source_dates(
        candidate, start_date=args.start_date, end_date=args.end_date
    )
    dates = frozenset(dates_list)
    args.receipt.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(
        prefix=f"{args.market}-history-stage.", dir=args.receipt.parent
    ) as temporary:
        staged = _stage_ledgers(
            candidate,
            Path(temporary),
            market=args.market,
            dates=dates,
        )
        target_before = _target_stats(live, market=args.market, dates=dates)
        pending: list[str] = []
        for filename, source_stats in staged.items():
            target_stats = target_before[filename]
            if int(target_stats["rows"]) == 0:
                pending.append(filename)
                continue
            if (
                int(target_stats["rows"]) != int(source_stats["rows"])
                or str(target_stats["sha256"]) != str(source_stats["sha256"])
            ):
                raise RuntimeError(
                    f"live historical ledger conflicts with candidate: {filename}"
                )

        live_state = _object(live / "state.json")
        live_mode = (live_state.get("modes") or {}).get(args.market) or {}
        current_session = str(live_mode.get("session_date") or "")
        carry_needed = bool(
            current_session > args.end_date
            and not str(live_mode.get("equity_carry_source_session") or "")
        )

        stopped = False
        position_stats: dict[str, int] = {"copied": 0, "already_identical": 0}
        equity_carry: dict[str, Any] = {
            "status": "already_applied"
            if str(live_mode.get("equity_carry_source_session") or "") == args.end_date
            else "not_required",
            "source_session_date": args.end_date,
            "current_session_date": current_session or None,
            "carry_in_twd": live_mode.get("equity_carry_in_twd"),
        }
        try:
            if pending or carry_needed:
                subprocess.run(["systemctl", "stop", ENGINE_SERVICE], check=True)
                stopped = True
            if pending:
                target_stopped = _target_stats(live, market=args.market, dates=dates)
                for filename in pending:
                    if int(target_stopped[filename]["rows"]) != 0:
                        raise RuntimeError(
                            f"historical rows appeared during import preflight: {filename}"
                        )
                    _append_file(Path(staged[filename]["stage_path"]), live / filename)
            position_stats = _copy_position_history(
                candidate,
                live,
                market=args.market,
                dates=dates_list,
            )
            if carry_needed:
                equity_carry = _apply_equity_carry(
                    candidate=candidate,
                    live=live,
                    market=args.market,
                    end_date=args.end_date,
                    historical_commission_rebate_twd=float(
                        staged["fills.jsonl"]["commission_rebate_accrued_twd"]
                    ),
                )
        finally:
            if stopped or subprocess.run(
                ["systemctl", "is-active", "--quiet", ENGINE_SERVICE], check=False
            ).returncode != 0:
                subprocess.run(["systemctl", "start", ENGINE_SERVICE], check=True)

    _wait_active(ENGINE_SERVICE)
    subprocess.run(["systemctl", "restart", DISCORD_SERVICE], check=True)
    _wait_active(DISCORD_SERVICE)
    subprocess.run(["systemctl", "restart", PUBLIC_SERVICE], check=True)
    _wait_active(PUBLIC_SERVICE)

    target_after = _target_stats(live, market=args.market, dates=dates)
    for filename, source_stats in staged.items():
        target_stats = target_after[filename]
        if (
            int(target_stats["rows"]) != int(source_stats["rows"])
            or str(target_stats["sha256"]) != str(source_stats["sha256"])
        ):
            raise RuntimeError(f"post-import ledger verification failed: {filename}")
    visibility = _verify_visibility(
        market=args.market,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    equity_continuity = _verify_equity_continuity(
        candidate=candidate,
        live=live,
        market=args.market,
        end_date=args.end_date,
    )
    receipt = {
        "schema_version": 1,
        "status": "complete",
        "completed_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
        "market": args.market,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "session_count": len(dates),
        "candidate_dir": str(candidate),
        "live_dir": str(live),
        "ledgers": {
            filename: {
                key: value
                for key, value in stats.items()
                if key != "stage_path"
            }
            for filename, stats in staged.items()
        },
        "position_history": position_stats,
        "equity_carry": equity_carry,
        "equity_continuity": equity_continuity,
        "visibility": visibility,
        "live_state_replaced": False,
        "current_session_rows_replaced": False,
        "simulation_only": True,
        "production_order_possible": False,
    }
    _atomic_json(args.receipt, receipt)
    print(json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

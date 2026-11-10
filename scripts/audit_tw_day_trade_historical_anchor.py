#!/usr/bin/env python3
"""Read-only proof of a flat prior-session paper-account anchor.

The receipt is only an input to an isolated historical reconstruction.  It is
not proof that later sessions were replayed, that broker orders existed, or
that any candidate can be published.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime
import hashlib
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from downloader.download_tw_public_data import _validated_taiex_session_dates
from scripts.plan_tw_day_trade_historical_revisions import _write_new_plan
from scripts import repair_tw_day_trade_flat_opening as revision_owner
from scripts.repair_tw_day_trade_flat_opening import POSITION_ECONOMIC_KEYS, _require_missing_session_target
from stockagent.live import paper_journal
from stockagent.live.paper_journal import managed, open_paper_journal

FILL_FIELDS = (
    "market", "session_date", "symbol", "position_id", "order_id", "purpose", "quantity",
    "price", "fee_and_tax_twd", "gross_fee_and_tax_twd", "commission_rebate_accrued_twd",
    "fill_at", "recorded_at", "quote_at", "simulation_only", "simulation_replay", "replay_basis",
)
MARK_FIELDS = ("initial_capital_twd", "cumulative_realized_net_pnl_twd",
               "cumulative_corporate_action_net_twd", "cumulative_carry_cost_twd",
               "open_position_count", "total_equity_twd")


def _generation(path: Path) -> tuple[int, int, int, int, int]:
    s = path.stat()
    return s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns


def _sha(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _rows(path: Path, *, day: str, market: str):
    with open_paper_journal(path) as stream:
        for raw in stream:
            if not raw.endswith(b"\n"):
                raise RuntimeError(f"incomplete journal row: {path}")
            if day.encode() not in raw or market.encode() not in raw:
                continue
            row = json.loads(raw)
            if row.get("session_date") == day and row.get("market") == market:
                yield row


def _close_mark(path: Path, *, day: str, market: str) -> dict:
    rows = [row for row in _rows(path, day=day, market=market)
            if str(row.get("minute") or "")[:16] == f"{day}T13:30"]
    if not rows:
        raise RuntimeError("missing prior-session closing mark")
    try:
        timestamps = [datetime.fromisoformat(str(row.get("recorded_at") or "")) for row in rows]
    except ValueError as error:
        raise RuntimeError("invalid prior-session closing mark clock") from error
    if any(value.utcoffset() is None for value in timestamps) or timestamps != sorted(timestamps):
        raise RuntimeError("ambiguous prior-session closing mark order")
    return rows[-1]


def _positions(value) -> dict[str, dict]:
    if not isinstance(value, list):
        raise ValueError("historical position archive must be a list")
    result = {}
    for row in value:
        if not isinstance(row, dict) or not isinstance(row.get("position_id"), str):
            raise ValueError("historical position identity missing")
        key = row["position_id"]
        if key in result or type(row.get("signed_shares")) is not int or row["signed_shares"] != 0:
            raise ValueError("duplicate or nonflat historical position")
        result[key] = {field: row.get(field) for field in POSITION_ECONOMIC_KEYS}
    return result


def audit(*, anchor_dir: Path, live_dir: Path, market: str,
          anchor_day: date, target_day: date, calendar_root: Path) -> dict:
    if not market or Path(market).name != market or not anchor_day < target_day:
        raise ValueError("exact market and increasing sessions are required")
    anchor_dir, live_dir = anchor_dir.resolve(strict=True), live_dir.resolve(strict=True)
    if anchor_dir == live_dir or anchor_dir.is_relative_to(live_dir):
        raise ValueError("anchor must be a separate preserved paper candidate")
    sessions, calendar_sha = _validated_taiex_session_dates(calendar_root, anchor_day, target_day)
    if {str(value) for value in sessions} != {str(anchor_day), str(target_day)}:
        raise RuntimeError("anchor is not the immediately preceding official trading session")
    day = str(anchor_day)
    paths = {
        "anchor_state": anchor_dir / "state.json",
        "anchor_events": anchor_dir / "events.jsonl",
        "anchor_fills": anchor_dir / "fills.jsonl", "anchor_marks": anchor_dir / "marks.jsonl",
        "live_positions": live_dir / "position_history" / day / f"{market}.json",
        "live_fills": live_dir / "fills.jsonl", "live_marks": live_dir / "marks.jsonl",
    }
    if any(not path.is_file() or path.is_symlink() or path.resolve(strict=True) != path
           for path in paths.values()):
        raise ValueError("anchor source is missing or redirected")
    if any(managed(paths[name]) for name in ("anchor_events", "anchor_fills", "anchor_marks",
                                             "live_fills", "live_marks")):
        raise ValueError("managed journal segments need their own immutable source pins")
    code_paths = (Path(__file__).resolve(), Path(revision_owner.__file__).resolve(),
                  Path(paper_journal.__file__).resolve())
    code_before = {str(path): _sha(path) for path in code_paths}
    before = {name: _generation(path) for name, path in paths.items()}
    state = json.loads(paths["anchor_state"].read_text(encoding="utf-8"))
    mode = state.get("modes", {}).get(market)
    archive = json.loads(paths["live_positions"].read_text(encoding="utf-8"))
    if (not isinstance(mode, dict) or mode.get("session_date") != day
            or archive.get("market") != market or archive.get("session_date") != day
            or archive.get("simulation_only") is not True
            or archive.get("production_order_possible") is not False
            or mode.get("signal_id") != archive.get("signal_id")
            or any(mode.get(key) != archive.get(key) for key in ("config_fingerprint", "checkpoint_fingerprint"))):
        raise RuntimeError("prior mode and archived paper identity differ")
    _require_missing_session_target(mode, target_day, events_path=paths["anchor_events"])
    candidate_positions = _positions(list(mode.get("positions", {}).values()))
    if candidate_positions != _positions(archive.get("positions")):
        raise RuntimeError("prior candidate positions differ from the original archive")

    left, right = (_close_mark(paths[name], day=day, market=market)
                   for name in ("anchor_marks", "live_marks"))
    for field in MARK_FIELDS:
        a, b = left.get(field), right.get(field)
        if a is None and b is None:
            continue
        if (isinstance(a, bool) or isinstance(b, bool)
                or not isinstance(a, (int, float)) or not isinstance(b, (int, float))
                or not math.isfinite(float(a)) or not math.isfinite(float(b))
                or not math.isclose(float(a), float(b), rel_tol=1e-12, abs_tol=1e-6)):
            raise RuntimeError(f"prior closing marks differ: {field}")
    if (type(left.get("open_position_count")) is not int or left["open_position_count"] != 0
            or type(mode.get("open_position_count")) is not int or mode["open_position_count"] != 0):
        raise RuntimeError("prior close is not flat")
    for field in ("initial_capital_twd", "cumulative_realized_net_pnl_twd", "total_equity_twd"):
        if not math.isclose(float(left[field]), float(mode[field]), rel_tol=1e-12, abs_tol=1e-6):
            raise RuntimeError(f"prior state and closing mark differ: {field}")
    expected = (float(mode["initial_capital_twd"]) + float(mode["cumulative_realized_net_pnl_twd"])
                + float(mode.get("cumulative_corporate_action_net_twd") or 0)
                - float(mode.get("cumulative_carry_cost_twd") or 0))
    if not math.isclose(expected, float(mode["total_equity_twd"]), rel_tol=1e-12, abs_tol=1e-6):
        raise RuntimeError("prior flat account equity does not reconcile")

    def fill_counter(path: Path) -> Counter:
        return Counter(json.dumps({key: row.get(key) for key in FILL_FIELDS},
                                  sort_keys=True, ensure_ascii=False, allow_nan=False)
                       for row in _rows(path, day=day, market=market))
    candidate_fills, live_fills = fill_counter(paths["anchor_fills"]), fill_counter(paths["live_fills"])
    if candidate_fills != live_fills:
        raise RuntimeError("prior candidate fills or fees differ from the original journal")
    pins = {name: {"path": str(path), "sha256": _sha(path)} for name, path in paths.items()}
    if (any(_generation(path) != before[name] for name, path in paths.items())
            or any(_sha(path) != code_before[str(path)] for path in code_paths)):
        raise RuntimeError("prior account source changed during audit")
    return {
        "contract": "historical_flat_paper_anchor_component_v1",
        "status": "prior_flat_anchor_component_verified_not_replayed",
        "market": market, "anchor_session": day, "target_session": str(target_day),
        "official_calendar_sha256": calendar_sha, "position_count": len(candidate_positions),
        "matching_fill_count": sum(candidate_fills.values()),
        "total_equity_twd": float(mode["total_equity_twd"]), "source_pins": pins,
        "implementation_sha256": code_before,
        "simulation_only": True, "broker_orders_created": 0, "ledger_modified": False,
        "subsequent_sessions_validated": False, "publication_authorized": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--anchor-dir", type=Path, required=True)
    parser.add_argument("--live-dir", type=Path, required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--anchor-day", type=date.fromisoformat, required=True)
    parser.add_argument("--target-day", type=date.fromisoformat, required=True)
    parser.add_argument("--calendar-root", type=Path, default=ROOT / "data_tw_public")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists() or output.is_relative_to(args.live_dir.resolve()) or output.is_relative_to(args.anchor_dir.resolve()):
        raise ValueError("a new receipt outside both ledgers is required")
    result = audit(anchor_dir=args.anchor_dir, live_dir=args.live_dir, market=args.market,
                   anchor_day=args.anchor_day, target_day=args.target_day, calendar_root=args.calendar_root)
    _write_new_plan(output, result)
    print(json.dumps({key: value for key, value in result.items() if key != "source_pins"}, ensure_ascii=False))


if __name__ == "__main__":
    main()

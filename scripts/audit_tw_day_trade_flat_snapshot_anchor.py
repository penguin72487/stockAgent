#!/usr/bin/env python3
"""Audit flat historical accounts from an existing pre-apply state snapshot.

The result is a source/accounting component only, not a replay or permission
to publish. It never alters a journal, a position or the snapshot.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from downloader.download_tw_public_data import _validated_taiex_session_dates
from scripts import audit_tw_day_trade_historical_anchor as anchor
from scripts.plan_tw_day_trade_historical_revisions import _write_new_plan
from stockagent.live.paper_journal import managed, open_paper_journal
from stockagent.live.tw_day_trade_simulation import TAIPEI


def _near(actual, expected, name: str) -> None:
    if (isinstance(actual, bool) or not isinstance(actual, (int, float))
            or not math.isfinite(float(actual))
            or not math.isclose(float(actual), float(expected), rel_tol=1e-12, abs_tol=1e-5)):
        raise RuntimeError(f"paper account value disagrees: {name}")


def check_position(position: dict, fills: list[dict], *, market: str, day: str) -> dict:
    """Independently reduce same-session paper entries, exits and fees."""
    identity = position.get("position_id")
    if (not identity or position.get("market") != market or position.get("session_date") != day
            or str(position.get("entry_at") or "")[:10] != day or position.get("side") not in {"long", "short"}
            or position.get("signed_shares") != 0 or position.get("nontrade_settlement")):
        raise RuntimeError("position is not an ordinary flat same-session paper cohort")
    quantity = position.get("filled_shares")
    lot = position.get("lot_size")
    if type(quantity) is not int or type(lot) is not int or quantity <= 0 or lot <= 0 or quantity % lot:
        raise RuntimeError("invalid filled shares or lot size")
    totals = defaultdict(float)
    has_original_entry = False
    for fill in fills:
        if (fill.get("market") != market or fill.get("session_date") != day
                or fill.get("position_id") != identity or fill.get("symbol") != position.get("symbol")
                or fill.get("simulation_only") is not True):
            raise RuntimeError("unowned or non-paper fill")
        count, price = fill.get("quantity"), fill.get("price")
        if (type(count) is not int or count <= 0 or count % lot or isinstance(price, bool)
                or not isinstance(price, (int, float)) or not math.isfinite(price) or price <= 0):
            raise RuntimeError("invalid fill quantity or price")
        at = datetime.fromisoformat(str(fill.get("fill_at") or ""))
        if at.utcoffset() is None or at.astimezone(TAIPEI).date().isoformat() != day:
            raise RuntimeError("invalid paper fill clock")
        entry = fill.get("purpose") in {"entry", "entry_completion"}
        has_original_entry |= fill.get("purpose") == "entry"
        side = position["side"]
        rate_key = ("buy_fee_rate" if side == "long" else "sell_fee_rate") if entry else (
            "sell_fee_rate" if side == "long" else "buy_fee_rate")
        rate = float(position[rate_key]) - float(position.get("commission_rebate_rate") or 0)
        fee = count * float(price) * rate
        _near(fill.get("fee_and_tax_twd"), fee, f"{identity}:fill_fee")
        prefix = "entry" if entry else "exit"
        totals[prefix + "_shares"] += count
        totals[prefix + "_notional"] += count * float(price)
        totals[prefix + "_fees"] += fee
    if not has_original_entry or totals["entry_shares"] != quantity or totals["exit_shares"] != quantity:
        raise RuntimeError("entry and exit shares do not reconcile")
    gross = (1 if position["side"] == "long" else -1) * (
        totals["exit_notional"] - totals["entry_notional"])
    net = gross - totals["entry_fees"] - totals["exit_fees"]
    for field, expected in (("entry_price", totals["entry_notional"] / quantity),
                            ("entry_fee_twd", totals["entry_fees"]),
                            ("realized_exit_fee_twd", totals["exit_fees"]),
                            ("realized_gross_pnl_twd", gross), ("realized_net_pnl_twd", net)):
        _near(position.get(field), expected, f"{identity}:{field}")
    return {"shares": quantity, "fill_count": len(fills), "net_pnl_twd": net,
            "fees_twd": totals["entry_fees"] + totals["exit_fees"]}


def audit(*, snapshot: Path, live_dir: Path, markets: list[str], previous_day: date,
          anchor_day: date, target_day: date, calendar_root: Path) -> dict:
    if (not markets or len(markets) != len(set(markets)) or any(Path(m).name != m for m in markets)
            or not previous_day < anchor_day < target_day):
        raise ValueError("exact unique markets and increasing sessions are required")
    snapshot, live_dir = snapshot.resolve(strict=True), live_dir.resolve(strict=True)
    sessions, calendar_sha = _validated_taiex_session_dates(calendar_root, previous_day, target_day)
    if {str(day) for day in sessions} != {str(previous_day), str(anchor_day), str(target_day)}:
        raise RuntimeError("requested days are not consecutive official sessions")
    files = {"snapshot": snapshot, "events": live_dir / "events.jsonl", "fills": live_dir / "fills.jsonl",
             "marks": live_dir / "marks.jsonl"}
    files.update({f"archive:{market}": live_dir / "position_history" / str(anchor_day) / f"{market}.json"
                  for market in markets})
    files.update({"audit_code": Path(__file__).resolve(), "anchor_code": Path(anchor.__file__).resolve(),
                  "revision_owner_code": Path(anchor.revision_owner.__file__).resolve(),
                  "journal_reader_code": Path(anchor.paper_journal.__file__).resolve()})
    if any(not path.is_file() or path.is_symlink() or path.resolve(strict=True) != path for path in files.values()):
        raise ValueError("snapshot source missing or redirected")
    if any(managed(files[name]) for name in ("events", "fills", "marks")):
        raise ValueError("managed journal segments need immutable source pins")
    before = {name: anchor._generation(path) for name, path in files.items()}
    state = json.loads(snapshot.read_text(encoding="utf-8"))
    if state.get("simulation_only") is not True or state.get("production_order_possible") is not False:
        raise RuntimeError("snapshot is not a paper-only account")
    modes, positions = {}, {}
    for market in markets:
        mode = state.get("modes", {}).get(market)
        archive = json.loads(files[f"archive:{market}"].read_text(encoding="utf-8"))
        if (not isinstance(mode, dict) or mode.get("session_date") != str(anchor_day)
                or archive.get("market") != market or archive.get("session_date") != str(anchor_day)
                or archive.get("simulation_only") is not True or archive.get("production_order_possible") is not False
                or mode.get("signal_id") != archive.get("signal_id")
                or any(mode.get(key) != archive.get(key) for key in ("config_fingerprint", "checkpoint_fingerprint"))):
            raise RuntimeError(f"snapshot/archive identity differs: {market}")
        anchor._require_missing_session_target(mode, target_day, events_path=files["events"])
        archived = anchor._positions(archive.get("positions"))
        if anchor._positions(list(mode.get("positions", {}).values())) != archived:
            raise RuntimeError(f"snapshot/archive economics differ: {market}")
        modes[market] = mode
        positions[market] = mode.get("positions", {})

    fills = defaultdict(list)
    with open_paper_journal(files["fills"]) as stream:
        for raw in stream:
            if not raw.endswith(b"\n"):
                raise RuntimeError("incomplete fill journal")
            if str(anchor_day).encode() not in raw:
                continue
            row = json.loads(raw)
            if row.get("session_date") == str(anchor_day) and row.get("market") in modes:
                fills[(row["market"], row.get("position_id"))].append(row)
    marks = defaultdict(list)
    wanted_days = {str(previous_day), str(anchor_day)}
    with open_paper_journal(files["marks"]) as stream:
        for raw in stream:
            if not raw.endswith(b"\n"):
                raise RuntimeError("incomplete mark journal")
            if not any(day.encode() in raw for day in wanted_days):
                continue
            row = json.loads(raw)
            day, market = row.get("session_date"), row.get("market")
            if day in wanted_days and market in modes and str(row.get("minute") or "")[:16] == f"{day}T13:30":
                marks[(market, day)].append(row)
    results = []
    for market, mode in modes.items():
        position_map = positions[market]
        if {pid for m, pid in fills if m == market} != set(position_map):
            raise RuntimeError(f"fill/position identity set differs: {market}")
        checked = [check_position(position, fills[(market, identity)], market=market, day=str(anchor_day))
                   for identity, position in position_map.items()]
        close_rows, previous_rows = marks[(market, str(anchor_day))], marks[(market, str(previous_day))]
        if not close_rows or not previous_rows:
            raise RuntimeError(f"missing terminal mark: {market}")
        for group in (close_rows, previous_rows):
            clocks = [datetime.fromisoformat(str(row.get("recorded_at") or "")) for row in group]
            if any(clock.utcoffset() is None for clock in clocks) or clocks != sorted(clocks):
                raise RuntimeError(f"ambiguous terminal mark order: {market}")
        close, previous = close_rows[-1], previous_rows[-1]
        if type(close.get("open_position_count")) is not int or close["open_position_count"] != 0:
            raise RuntimeError(f"terminal mark is not flat: {market}")
        for field in anchor.MARK_FIELDS:
            if field in mode and field in close:
                _near(mode[field], close[field], f"{market}:{field}")
        realized = math.fsum(row["net_pnl_twd"] for row in checked)
        _near(float(mode["cumulative_realized_net_pnl_twd"]) - float(previous["cumulative_realized_net_pnl_twd"]),
              realized, f"{market}:daily_realized")
        expected_equity = (float(mode["initial_capital_twd"]) + float(mode["cumulative_realized_net_pnl_twd"])
                           + float(mode.get("cumulative_corporate_action_net_twd") or 0)
                           - float(mode.get("cumulative_carry_cost_twd") or 0))
        _near(mode["total_equity_twd"], expected_equity, f"{market}:equity")
        results.append({"market": market, "positions": len(checked),
                        "matching_paper_fills": sum(row["fill_count"] for row in checked),
                        "net_fees_and_tax_twd": math.fsum(row["fees_twd"] for row in checked),
                        "day_realized_net_pnl_twd": realized, "total_equity_twd": float(mode["total_equity_twd"])})
    pins = {name: {"path": str(path), "sha256": anchor._sha(path)} for name, path in files.items()}
    if any(anchor._generation(path) != before[name] for name, path in files.items()):
        raise RuntimeError("snapshot or source changed during audit")
    return {"contract": "historical_flat_snapshot_anchor_component_v1", "status": "snapshot_accounts_verified_not_replayed",
            "previous_session": str(previous_day), "anchor_session": str(anchor_day), "target_session": str(target_day),
            "official_calendar_sha256": calendar_sha, "accounts": results, "source_pins": pins,
            "simulation_only": True, "broker_orders_created": 0, "ledger_modified": False,
            "historical_prices_or_broker_fills_validated": False, "subsequent_sessions_validated": False,
            "publication_authorized": False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--live-dir", type=Path, required=True)
    parser.add_argument("--market", action="append", required=True)
    parser.add_argument("--previous-day", type=date.fromisoformat, required=True)
    parser.add_argument("--anchor-day", type=date.fromisoformat, required=True)
    parser.add_argument("--target-day", type=date.fromisoformat, required=True)
    parser.add_argument("--calendar-root", type=Path, default=ROOT / "data_tw_public")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists() or output.is_relative_to(args.live_dir.resolve()):
        raise ValueError("a new private receipt outside the live ledger is required")
    result = audit(snapshot=args.snapshot, live_dir=args.live_dir, markets=args.market,
                   previous_day=args.previous_day, anchor_day=args.anchor_day, target_day=args.target_day,
                   calendar_root=args.calendar_root)
    _write_new_plan(output, result)
    print(json.dumps({key: value for key, value in result.items() if key != "source_pins"}, ensure_ascii=False))


if __name__ == "__main__":
    main()

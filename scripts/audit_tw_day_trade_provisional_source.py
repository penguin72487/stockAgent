#!/usr/bin/env python3
"""Audit provisional replay marks and decisions against their pinned sources."""
from __future__ import annotations

import argparse
from bisect import bisect_left
from datetime import date, datetime, timedelta
import json
import math
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import rebuild_tw_day_trade_open_price_replay as replay
from scripts import repair_tw_day_trade_flat_opening as repair
from stockagent.live.paper_journal import open_paper_journal


def _instant(value: str) -> datetime:
    stamp = datetime.fromisoformat(value)
    if stamp.utcoffset() is None:
        raise RuntimeError("revision audit requires timezone-aware clocks")
    return stamp


def audit_published_decisions(candidate: Path, manifest: dict) -> dict:
    """Verify every replay target against the actual immutable publication.

    A decision calculated after 09:01 can be replayed at that time only as the
    explicitly labelled first counterfactual. Later decisions use a complete
    minute beginning strictly after their publication bound. This audit does
    not interpret a replay as an exchange or brokerage transaction.
    """
    market = manifest["market"]
    source = manifest["sessions"][0]
    day, token = source["session_date"], source["repair_token"]
    versions = [v for v in manifest["published_revision_plan"]["versions"]
                if v.get("effective_entry_at")]
    if not versions:
        raise RuntimeError("published revision plan contains no effective decision")
    expected = {}
    for index, version in enumerate(versions):
        original = str(version["signal_id"])
        replay_id = f"{original}:published-minute-repair:{token}"
        if replay_id in expected:
            raise RuntimeError("duplicate effective published decision")
        at = _instant(version["effective_entry_at"])
        published = _instant(version["publication_upper_bound"])
        if index == 0:
            if at != _instant(f"{day}T09:01:00+08:00"):
                raise RuntimeError("first counterfactual is not at 09:01")
        elif at - timedelta(minutes=1) <= published:
            raise RuntimeError("revision consumes a minute before publication")
        for name in ("summary", "weights"):
            if repair._sha256(Path(version[f"{name}_path"])) != version[f"{name}_sha256"]:
                raise RuntimeError(f"published decision {name} changed")
        frame = pl.read_parquet(version["weights_path"], columns=["symbol", "target_weight"])
        rows = frame.to_dicts()
        weights = {}
        for row in rows:
            symbol = str(row.get("symbol") or "")
            weight = float(row.get("target_weight") or 0.0)
            if not symbol or symbol in weights or not math.isfinite(weight):
                raise RuntimeError("published decision has invalid or duplicate symbols")
            weights[symbol] = weight
        expected[replay_id] = {"version": version, "index": index,
                               "weights": weights, "seen": set()}
    count = 0
    signals = candidate / "signals.jsonl"
    initial_hash = repair._sha256(signals)
    with open_paper_journal(signals) as stream:
        for line in stream:
            if not line.strip():
                continue
            if not line.endswith(b"\n"):
                raise RuntimeError("incomplete replay signal row")
            row = json.loads(line)
            decision = expected.get(row.get("signal_id"))
            if decision is None:
                raise RuntimeError("replay signal has no pinned published revision")
            version, symbol = decision["version"], str(row.get("symbol") or "")
            weight = row.get("target_weight")
            if (row.get("market") != market or row.get("session_date") != day
                    or row.get("simulation_replay") is not True
                    or row.get("counterfactual_open_replay") is not True
                    or row.get("replay_basis") != manifest["contract"]
                    or row.get("signal_source_path") != version["summary_path"]
                    or _instant(row["signal_at"]) != _instant(version["effective_entry_at"])
                    or _instant(row["recorded_at"]) != _instant(version["effective_entry_at"])
                    or _instant(row["source_signal_at"]) != _instant(version["signal_started_at"])
                    or (row.get("opening_signal_revision") or {}).get("index") != decision["index"]
                    or symbol not in decision["weights"] or symbol in decision["seen"]
                    or isinstance(weight, bool) or not isinstance(weight, (float, int))
                    or not math.isfinite(weight) or float(weight) != decision["weights"][symbol]):
                raise RuntimeError(f"replay signal differs from published decision: {row.get('signal_id')}/{symbol}")
            decision["seen"].add(symbol)
            count += 1
    if repair._sha256(signals) != initial_hash:
        raise RuntimeError("replay signals changed during audit")
    if any(item["seen"] != set(item["weights"]) for item in expected.values()):
        raise RuntimeError("replay is missing published target rows")

    symbols = set().union(*(set(item["weights"]) for item in expected.values()))
    orders = candidate / "orders.jsonl"
    orders_hash = repair._sha256(orders)
    order_ids, order_count = set(), 0
    with open_paper_journal(orders) as stream:
        for line in stream:
            if not line.strip():
                continue
            if not line.endswith(b"\n"):
                raise RuntimeError("incomplete replay order row")
            row = json.loads(line)
            quantity = row.get("quantity")
            if (row.get("market") != market or row.get("session_date") != day
                    or row.get("simulation_only") is not True
                    or row.get("symbol") not in symbols
                    or type(quantity) is not int or quantity < 0
                    or "nontrade" in str(row.get("purpose") or "")):
                raise RuntimeError("replay order has invalid scope or quantity")
            if row.get("order_id"):
                order_ids.add((row["order_id"], row["symbol"]))
            order_count += 1
    if repair._sha256(orders) != orders_hash:
        raise RuntimeError("replay orders changed during audit")
    fills = candidate / "fills.jsonl"
    fills_hash = repair._sha256(fills)
    fill_count = 0
    with open_paper_journal(fills) as stream:
        for line in stream:
            if not line.strip():
                continue
            if not line.endswith(b"\n"):
                raise RuntimeError("incomplete replay fill row")
            row = json.loads(line)
            if ((row.get("order_id"), row.get("symbol")) not in order_ids
                    or row.get("market") != market or row.get("session_date") != day
                    or row.get("simulation_only") is not True):
                raise RuntimeError("replay fill has no corresponding scoped paper order")
            fill_count += 1
    if repair._sha256(fills) != fills_hash:
        raise RuntimeError("replay fills changed during audit")
    for name in ("marks.jsonl", "events.jsonl"):
        with open_paper_journal(candidate / name) as stream:
            for line in stream:
                if not line.endswith(b"\n"):
                    raise RuntimeError(f"incomplete replay row: {name}")
                if not line.strip():
                    continue
                row = json.loads(line)
                event_day = row.get("session_date")
                if name == "events.jsonl" and event_day is None:
                    event_day = _instant(row["recorded_at"]).astimezone(repair.TAIPEI).date().isoformat()
                if row.get("market") != market or event_day != day:
                    raise RuntimeError(f"foreign replay account or session: {name}")
    return {"published_decisions": len(versions), "signal_rows": count,
            "paper_order_rows": order_count, "paper_fill_rows": fill_count,
            "original_targets_and_clocks_verified": True,
            "first_signal_later_than_counterfactual":
                _instant(versions[0]["publication_upper_bound"]) > _instant(versions[0]["effective_entry_at"]),
            "first_publication_upper_bound": versions[0]["publication_upper_bound"],
            "counterfactual_effective_at": versions[0]["effective_entry_at"],
            "candidate_signals_sha256": initial_hash, "candidate_orders_sha256": orders_hash,
            "candidate_fills_sha256": fills_hash}


def audit_marks(mode: dict, marks: list[dict], bars: dict, *, day: str) -> dict:
    start = datetime.fromisoformat(f"{day}T09:01:00+08:00")
    expected = {(start + timedelta(minutes=i)).isoformat()[:16] for i in range(270)}
    delayed = mode.get("delayed_paper_auction_settled_at")
    if delayed:
        if delayed != f"{day}T13:33:00+08:00":
            raise RuntimeError("invalid delayed settlement")
        expected.add(delayed[:16])
    selected = [row for row in marks if row.get("market") == mode["market"] and row.get("session_date") == day]
    by_minute = {str(row.get("minute") or "")[:16]: row for row in selected}
    if len(selected) != len(expected) or set(by_minute) != expected:
        raise RuntimeError("provisional candidate has missing or duplicate minute marks")
    positions = [p for p in mode["positions"].values() if p.get("session_date") == day]
    observed = {str(symbol): sorted({str(key)[:16] for key in rows}) for symbol, rows in bars.items()}
    observed_sets = {symbol: set(minutes) for symbol, minutes in observed.items()}
    gap_minutes: dict[str, list[str]] = {}
    for minute, row in sorted(by_minute.items()):
        if (row.get("historical_minute_replay") is not True
                or row.get("provisional_minute_source_coverage") is not True
                or int(row.get("missing_price_position_count") or 0) != 0):
            raise RuntimeError(f"provisional valuation lacks a valid disclosure: {minute}")
        held = [p for p in positions if str(p["entry_at"])[:16] <= minute
                < str((p.get("nontrade_settlement") or {}).get("accounting_at")
                      or p.get("exit_at") or f"{day}T23:59")[:16]]
        missing = [p for p in held if minute not in observed_sets.get(str(p["symbol"]), set())]
        count = len(missing)
        if (int(row.get("open_position_count") or 0) != len(held)
                or int(row.get("stale_position_count") or 0) != count
                or int(row.get("last_trade_carried_position_count") or 0) != count
                or int(row.get("fresh_trade_position_count") or 0) != len(held) - count
                or bool(row.get("valuation_stale")) != bool(count)):
            raise RuntimeError(f"mark falsely reports observed/fresh inventory: {minute}")
        for position in missing:
            symbol = str(position["symbol"])
            prior = observed.get(symbol, [])
            index = bisect_left(prior, minute) - 1
            if index < 0 or prior[index] < str(position["entry_at"])[:16]:
                raise RuntimeError(f"carried valuation lacks an observed earlier price: {symbol}/{minute}")
        if missing:
            gap_minutes[minute] = sorted({str(position["symbol"]) for position in missing})
    return {"regular_minute_points": 270, "delayed_event_points": int(bool(delayed)),
            "carried_valuation_minutes": len(gap_minutes), "carried_minute_symbols": gap_minutes,
            "freshness_and_prior_observation_verified": True}


def audit(root: Path, *, output_path: Path | None = None) -> dict:
    root = root.resolve(strict=True)
    manifest_path = root / "ledger_repair_manifest.json"
    manifest = repair._object(manifest_path)
    if (manifest.get("status") != "candidate_validated_not_applied"
            or manifest.get("provisional_minute_source_coverage") is not True
            or manifest.get("published_revisions") is not True):
        raise RuntimeError("only an explicitly provisional unpublished candidate can be audited")
    repair._assert_source_pins(manifest["source_pins"])
    candidate = Path(manifest["candidate_dir"]).resolve(strict=True)
    market, day = manifest["market"], manifest["sessions"][0]["session_date"]
    mode = repair._object(candidate / "state.json")["modes"][market]
    if mode.get("provisional_minute_source_coverage") is not True:
        raise RuntimeError("candidate does not persist the provisional replay marker")
    symbols = {str(p["symbol"]) for p in mode["positions"].values() if p.get("session_date") == day}
    bars, _ = replay._minute_bar_rows(tuple(Path(p) for p in manifest["minute_roots"]),
                                     trading_date=date.fromisoformat(day), symbols=symbols)
    with open_paper_journal(candidate / "marks.jsonl") as handle:
        marks = [json.loads(line) for line in handle if line.endswith(b"\n")]
    proof = audit_marks(mode, marks, bars, day=day)
    decisions = audit_published_decisions(candidate, manifest)
    evidence = {"status": "verified_provisional", "market": market, "session_date": day,
                "simulation_only": True, "production_order_possible": False,
                "manifest_sha256": repair._sha256(manifest_path),
                "auditor_sha256": repair._sha256(Path(__file__)),
                "candidate_files_sha256": {name: repair._sha256(candidate / name)
                    for name in ("state.json", "marks.jsonl", "fills.jsonl")},
                "published_decision_audit": decisions, **proof}
    repair._assert_source_pins(manifest["source_pins"])
    repair._atomic_json(output_path or root / "provisional_source_audit.json", evidence)
    return {key: value for key, value in evidence.items() if key != "carried_minute_symbols"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operation-root", required=True, type=Path)
    parser.add_argument("--output-path", type=Path,
                        help="Keep a new audit receipt instead of replacing the operation's prior receipt")
    args = parser.parse_args()
    print(json.dumps(audit(args.operation_root, output_path=args.output_path), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

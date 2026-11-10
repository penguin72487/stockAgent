#!/usr/bin/env python3
"""Independently verify original targets and observed clocks, without replay.

An inconsistent producer timestamp is preserved as evidence.  This audit can
establish when the paper executor observed matching targets; it does not
rewrite the summary, certify a fill, or make a replay plan publishable.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import sys
from typing import Any

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.plan_tw_day_trade_historical_revisions import _write_new_plan
from stockagent.live.paper_journal import open_paper_journal
from stockagent.live.tw_day_trade_dashboard import _ledger_session_index
from zoneinfo import ZoneInfo

TAIPEI = ZoneInfo("Asia/Taipei")


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _generation(path: Path) -> tuple[int, int, int, int, int]:
    value = path.stat()
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _instant(value: Any) -> datetime:
    stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if stamp.utcoffset() is None:
        raise RuntimeError("a timezone-aware source clock is required")
    return stamp


def conservative_clock_bounds(proof: dict) -> dict:
    """Transform a *freshly re-audited* receipt into conservative time bounds.

    This helper does not authenticate the receipt itself. The caller must run
    ``audit`` against the original journal and compare the complete result.
    None of the returned bounds are a rewritten publication or fill time.
    """
    if (proof.get("status") != "observed_signal_evidence_only"
            or proof.get("simulation_only") is not True
            or proof.get("production_order_possible") is not False
            or proof.get("execution_replay_validated") is not False
            or proof.get("overall_plan_ready") is not False):
        raise ValueError("source must be an observed-signal audit, not a replay")
    session = date.fromisoformat(str(proof.get("session_date") or ""))
    signals = proof.get("signals")
    if not isinstance(signals, list) or not signals:
        raise ValueError("observed-signal audit must contain signals")
    output, seen = [], set()
    for item in signals:
        if (not isinstance(item, dict) or not isinstance(item.get("signal_id"), str)
                or not item["signal_id"] or item["signal_id"] in seen
                or item.get("matching_target_weights") is not True
                or type(item.get("target_rows")) is not int or item["target_rows"] <= 0):
            raise ValueError("observed signal identity or target evidence is invalid")
        seen.add(item["signal_id"])
        clocks = item.get("original_clocks")
        required = ("generated_at", "signal_started_at", "signal_ready_at",
                    "artifact_published_at", "artifact_completed_at", "execution_pointer_written_at")
        if not isinstance(clocks, dict) or any(not clocks.get(key) for key in required):
            raise ValueError("complete original producer clocks are required")
        parsed = {key: _instant(clocks[key]) for key in required}
        observed = _instant(item.get("executor_observed_at"))
        registered = _instant(item.get("registered_at_second_precision"))
        if (registered.microsecond != 0
                or any(value.utcoffset() != timedelta(hours=8)
                       or value.astimezone(TAIPEI).date() != session
                       for value in (*parsed.values(), observed, registered))
                or observed >= registered + timedelta(seconds=1)):
            raise ValueError("observed registration or producer clock is invalid")
        # A zero fractional component can be a producer's second-precision
        # representation. The end of that second is a safe upper bound even
        # if the original clock happened to be exact.
        candidates = [value + timedelta(seconds=1) if value.microsecond == 0 else value
                      for value in parsed.values()]
        candidates.extend((observed, registered + timedelta(seconds=1)))
        anomalies = []
        if parsed["signal_started_at"] > parsed["artifact_published_at"]:
            anomalies.append("start_after_declared_publication")
        if parsed["generated_at"] > parsed["artifact_published_at"]:
            anomalies.append("generation_after_declared_publication")
        if parsed["artifact_published_at"] > parsed["execution_pointer_written_at"]:
            anomalies.append("declared_publication_after_pointer")
        output.append({"signal_id": item["signal_id"], "summary_path": item["summary_path"],
                       "summary_sha256": item["summary_sha256"], "weights_path": item["weights_path"],
                       "weights_sha256": item["weights_sha256"], "target_rows": item["target_rows"],
                       "original_clocks": dict(clocks), "executor_observed_at": observed.isoformat(),
                       "registered_at_second_precision": item["registered_at_second_precision"],
                       "conservative_not_before": max(candidates).isoformat(),
                       "producer_clock_anomalies": anomalies,
                       "bound_is_actual_publication_time": False,
                       "source_targets_exactly_match": True})
    return {"contract": "observed_target_conservative_clock_bound_v1",
            "status": "clock_evidence_only_not_replayed", "session_date": str(session),
            "market": proof.get("market"), "signals": output, "simulation_only": True,
            "production_order_possible": False, "broker_orders_created": 0,
            "ledger_modified": False, "publication_authorized": False,
            "fills_or_price_evidence_verified": False}


def audit(*, state_dir: Path, session: date, market: str, summaries: list[Path]) -> dict:
    if not market or not summaries:
        raise ValueError("market and source summaries are required")
    day = session.isoformat()
    expected: dict[str, dict[str, Any]] = {}
    for raw_path in summaries:
        path = raw_path.resolve(strict=True)
        before = _generation(path)
        summary_hash = _sha(path)
        summary = json.loads(path.read_text(encoding="utf-8"))
        weights_path = path.with_name("target_weights.parquet")
        weights_generation = _generation(weights_path)
        weights_hash = _sha(weights_path)
        signal_id = str(summary.get("signal_id") or "")
        if (not signal_id or signal_id in expected or summary.get("market") != market
                or str(summary.get("asof_date") or "")[:10] != day
                or not isinstance(summary.get("opening_signal_revision"), dict)):
            raise RuntimeError("source summary identity or session differs")
        target: dict[str, float] = {}
        for symbol, weight in pl.read_parquet(weights_path, columns=["symbol", "target_weight"]).iter_rows():
            symbol = str(symbol or "")
            if (not symbol or symbol in target or isinstance(weight, bool)
                    or not isinstance(weight, (int, float)) or not math.isfinite(float(weight))):
                raise RuntimeError("source target weights are invalid or duplicated")
            target[symbol] = float(weight)
        if not target or _generation(path) != before or _generation(weights_path) != weights_generation:
            raise RuntimeError("source changed during target read")
        expected[signal_id] = {"summary": path, "summary_sha256": summary_hash,
                               "weights": weights_path, "weights_sha256": weights_hash,
                               "summary_generation": before, "weights_generation": weights_generation,
                               "original_clocks": {name: summary.get(name) for name in (
                                   "generated_at", "signal_started_at", "signal_ready_at",
                                   "artifact_published_at", "artifact_completed_at", "execution_pointer_written_at")},
                               "target": target, "seen": set(), "observed": set()}

    events = state_dir / "events.jsonl"
    event_before, events_sha = _generation(events), _sha(events)
    registrations: dict[str, list[str]] = defaultdict(list)
    with open_paper_journal(events) as stream:
        for line in stream:
            if not line.endswith(b"\n"):
                raise RuntimeError("incomplete event journal")
            row = json.loads(line)
            if (row.get("market") == market and row.get("session_date") == day
                    and row.get("event") == "signal_registered" and row.get("signal_id") in expected):
                registrations[row["signal_id"]].append(str(row.get("recorded_at") or ""))
    if _generation(events) != event_before or _sha(events) != events_sha:
        raise RuntimeError("event journal changed during read")

    signals = state_dir / "signals.jsonl"
    before = _generation(signals)
    index = _ledger_session_index(signals, recorded_at_fallback=False)
    if index is None:
        raise RuntimeError("session signal index unavailable")
    spans = index.spans.get(day, ())
    if not spans:
        raise RuntimeError("no signal journal spans for the requested session")
    needle_market = tuple((json.dumps("market") + separator + json.dumps(market)).encode()
                          for separator in (":", ": "))
    digest = hashlib.sha256()
    with open_paper_journal(signals) as stream:
        for start, end in spans:
            stream.seek(start)
            remaining, carry = end - start, b""
            digest.update(f"{start}:{end}\n".encode())
            while remaining:
                block = stream.read(min(8 * 1024 * 1024, remaining))
                if not block:
                    raise RuntimeError("truncated indexed journal span")
                remaining -= len(block)
                digest.update(block)
                lines = (carry + block).split(b"\n")
                carry = lines.pop()
                for raw in lines:
                    if not any(needle in raw for needle in needle_market):
                        continue
                    row = json.loads(raw)
                    item = expected.get(row.get("signal_id"))
                    if item is None:
                        continue
                    symbol, weight = str(row.get("symbol") or ""), row.get("target_weight")
                    if (row.get("market") != market or row.get("session_date") != day
                            or row.get("simulation_replay") is not False
                            or Path(str(row.get("signal_source_path") or "")).resolve() != item["summary"]
                            or symbol not in item["target"] or symbol in item["seen"]
                            or isinstance(weight, bool) or not isinstance(weight, (int, float))
                            or not math.isfinite(float(weight)) or float(weight) != item["target"][symbol]):
                        raise RuntimeError(f"observed target differs or is duplicated: {row.get('signal_id')}/{symbol}")
                    item["seen"].add(symbol)
                    item["observed"].add(_instant(row.get("execution_pointer_observed_at")))
            if carry:
                raise RuntimeError("incomplete indexed signal row")
    if _generation(signals) != before:
        raise RuntimeError("signal journal changed during source audit")

    results = []
    for signal_id, item in expected.items():
        if (item["seen"] != set(item["target"]) or len(item["observed"]) != 1
                or len(registrations[signal_id]) != 1):
            raise RuntimeError(f"incomplete observed targets or registration: {signal_id}")
        observed = next(iter(item["observed"]))
        registered_text = registrations[signal_id][0]
        registered = _instant(registered_text)
        # The original engine stored registration at second precision. It is
        # an interval, not evidence that the event occurred at its lower edge.
        if (observed.date() != session or registered.date() != session
                or observed >= registered + timedelta(seconds=1)
                or _generation(item["summary"]) != item["summary_generation"]
                or _generation(item["weights"]) != item["weights_generation"]
                or _sha(item["summary"]) != item["summary_sha256"]
                or _sha(item["weights"]) != item["weights_sha256"]):
            raise RuntimeError(f"source/registration clock or artifact changed: {signal_id}")
        results.append({"signal_id": signal_id, "target_rows": len(item["target"]),
                        "summary_path": str(item["summary"]), "summary_sha256": item["summary_sha256"],
                        "weights_path": str(item["weights"]), "weights_sha256": item["weights_sha256"],
                        "original_clocks": item["original_clocks"],
                        "executor_observed_at": observed.isoformat(),
                        "registered_at_second_precision": registered_text,
                        "matching_target_weights": True})
    return {"schema_version": 1, "status": "observed_signal_evidence_only",
            "market": market, "session_date": day, "signals": results,
            "signal_span_sha256": digest.hexdigest(), "signal_spans": list(spans),
            "signal_journal_generation": list(before), "events_sha256": events_sha,
            "simulation_only": True, "production_order_possible": False,
            "execution_replay_validated": False, "prices_verified": False,
            "overall_plan_ready": False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--summary", type=Path, action="append", required=True)
    parser.add_argument("--index-cache-dir", type=Path,
                        help="Existing isolated dashboard-index directory; no source mutation.")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--conservative-clock-bounds", action="store_true",
                        help="Re-audit original records and emit conservative bounds; no replay or ledger write.")
    args = parser.parse_args()
    root, output = args.state_dir.resolve(strict=True), args.output.resolve()
    if output.is_relative_to(root) or output.exists():
        raise ValueError("a new evidence receipt outside the inspected ledger is required")
    if args.index_cache_dir is not None:
        if not args.index_cache_dir.is_dir():
            raise ValueError("index cache directory must already exist")
        os.environ["STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR"] = str(args.index_cache_dir.resolve())
    code = (Path(__file__).resolve(), ROOT / "stockagent/live/paper_journal.py",
            ROOT / "stockagent/live/tw_day_trade_dashboard.py")
    code_before = {str(path): _sha(path) for path in code}
    proof = audit(state_dir=root, session=date.fromisoformat(args.session_date),
                  market=args.market, summaries=args.summary)
    if any(_sha(path) != code_before[str(path)] for path in code):
        raise RuntimeError("observed-signal audit implementation changed during read")
    if args.conservative_clock_bounds:
        result = conservative_clock_bounds(proof)
        result["source_observed_signal_audit"] = proof
        result["audit_implementation_sha256"] = code_before
    else:
        result = proof
    _write_new_plan(output, result)
    print(f"{result['status']}: {output}")


if __name__ == "__main__":
    main()

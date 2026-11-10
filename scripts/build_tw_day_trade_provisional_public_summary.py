#!/usr/bin/env python3
"""Build an explicitly provisional, price-free summary from an audited replay.

This is a separate public research projection, never an active paper account or
ledger promotion. It refuses changed evidence and never reads a live ledger.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, time, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.live.market_config import load_market_config
from stockagent.live.tw_day_trade_research_candidates import research_session_is_completed
from stockagent.live.tw_day_trade_simulation import HISTORICAL_BRACKET_FULL_CROSS_CONTRACT

TAIPEI = ZoneInfo("Asia/Taipei")
CONTRACT = "public_audited_provisional_daily_replay_v1"


def _sha(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object: {path.name}")
    return value


def _unchanged(signatures: dict[str, list[int]]) -> None:
    for name, expected in signatures.items():
        stat = Path(name).stat()
        if [stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns] != expected:
            raise ValueError("a verified minute source changed after audit")


def build(*, state_dir: Path, audit_path: Path, candidate_config: Path,
          checkpoint_sha256: str, now: datetime, include_minutes: bool = False) -> dict:
    root, audit_path = state_dir.resolve(strict=True), audit_path.resolve(strict=True)
    if now.tzinfo is None or not audit_path.is_relative_to(root):
        raise ValueError("audit must belong to the candidate and clock must be aware")
    now = now.astimezone(TAIPEI)
    live = (ROOT / "artifacts/live/tw_day_trade_simulation").resolve()
    if root == live or root.is_relative_to(live) or live.is_relative_to(root):
        raise ValueError("a live ledger cannot be presented as an isolated replay")
    checked = _json(audit_path)
    if (checked.get("contract") != "strict_intraday_fill_cash_cost_minute_reconciliation_v1"
            or checked.get("passed") is not True or checked.get("full_requested_range_passed") is not True
            or checked.get("sources_verified") is not True
            or checked.get("provisional_minute_source_coverage") is not True
            or checked.get("complete_minute_source_coverage_verified") is not False
            or checked.get("error_count") != 0):
        raise ValueError("candidate lacks the full provisional accounting and source audit")
    hashes = checked.get("source_hashes") or {}
    required = {"state.json", "rebuild_receipt.json", "fills.jsonl", "marks.jsonl", "orders.jsonl", "events.jsonl"}
    if not required.issubset(hashes) or any(_sha(root / name) != hashes[name] for name in required):
        raise ValueError("candidate changed after its independent audit")
    signatures = checked.get("minute_source_signatures") or {}
    if not signatures:
        raise ValueError("minute source audit is absent")
    _unchanged(signatures)
    for group in ("terminal_close_source_hashes", "research_close_source_hashes", "nontrade_source_hashes"):
        if any(_sha(Path(name)) != digest for name, digest in (checked.get(group) or {}).items()):
            raise ValueError("a verified official or research source changed after audit")
    cfg = load_market_config(candidate_config.resolve(strict=True))
    checkpoint = Path(str(cfg.checkpoint_path or "")).resolve(strict=True)
    if (cfg.enabled or not cfg.day_trade_simulation_enabled or checkpoint.name != "checkpoint_best.pt"
            or len(checkpoint_sha256) != 64 or _sha(checkpoint) != checkpoint_sha256):
        raise ValueError("candidate checkpoint or disabled scope changed")
    state, receipt = _json(root / "state.json"), _json(root / "rebuild_receipt.json")
    if (receipt.get("simulation_only") is not True or receipt.get("production_order_possible") is not False
            or receipt.get("publication_authorized") is not False
            or receipt.get("historical_bracket_cross_assumption") != HISTORICAL_BRACKET_FULL_CROSS_CONTRACT
            or receipt.get("historical_decision_policy") != "single_counterfactual_open_not_published_revision_replay_v1"):
        raise ValueError("replay disclosure or execution contract is inconsistent")
    modes = state.get("modes") or {}
    if set(modes) != {cfg.market} or float(modes[cfg.market].get("initial_capital_twd") or 0) != float(cfg.initial_capital):
        raise ValueError("replay account or initial capital differs from candidate")
    sessions = receipt.get("sessions") or []
    dates = [str(row.get("session_date") or "") for row in sessions]
    if (not dates or dates != sorted(set(dates)) or len(dates) != checked.get("session_count")
            or dates[0] != checked.get("start_date") or dates[-1] != checked.get("end_date")
            or any(not research_session_is_completed(date.fromisoformat(day), now) for day in dates)
            or any((row.get("close") or {}).get("status") not in {
                "session_flat_no_broker_fill_claim", "session_flat_with_nontrade_book_assumptions"} for row in sessions)):
        raise ValueError("replay does not cover the audited completed current-year sessions")
    preflight = receipt.get("signal_preflight") or {}
    preflight_path = Path(str(preflight.get("path") or "")).resolve(strict=True)
    if not preflight_path.is_relative_to(root) or _sha(preflight_path) != preflight.get("sha256"):
        raise ValueError("signal preflight changed")
    signal = _json(preflight_path)
    artifacts = signal.get("artifacts") or []
    if (signal.get("status") != "accepted_sources_only" or signal.get("failures") != []
            or signal.get("expected_signals") != len(dates) or signal.get("accepted_signals") != len(dates)
            or [item.get("session_date") for item in artifacts] != dates):
        raise ValueError("signal source proof does not cover every requested day")
    for item in artifacts:
        if item.get("market") != cfg.market:
            raise ValueError("signal market differs from selected candidate")
        for field in ("open_input", "summary", "weights"):
            source = Path(str(item.get(f"{field}_path") or "")).resolve(strict=True)
            if _sha(source) != item.get(f"{field}_sha256"):
                raise ValueError("counterfactual signal input changed after preflight")
        summary = _json(Path(item["summary_path"]))
        if Path(str(summary.get("checkpoint_path") or "")).resolve(strict=True) != checkpoint:
            raise ValueError("counterfactual signal used another checkpoint")
    marks = defaultdict(list)
    with (root / "marks.jsonl").open("r", encoding="utf-8") as stream:
        for line in stream:
            if not line.endswith("\n"):
                raise ValueError("candidate mark journal is incomplete")
            row = json.loads(line)
            if (not isinstance(row, dict) or row.get("market") != cfg.market
                    or row.get("historical_minute_replay") is not True
                    or row.get("provisional_minute_source_coverage") is not True
                    or row.get("recorded_at_basis") != "counterfactual_effective_clock_not_original_publication"):
                raise ValueError("candidate minute is not an explicitly labelled provisional replay")
            actual = datetime.fromisoformat(str(row.get("replay_generated_at") or ""))
            minute = datetime.fromisoformat(str(row.get("minute") or ""))
            equity = float(row.get("total_equity_twd"))
            if (actual.utcoffset() is None or actual < minute or actual > now
                    or minute.utcoffset() != timedelta(hours=8)
                    or not math.isfinite(equity) or equity <= 0):
                raise ValueError("candidate minute clock or equity is invalid")
            marks[row["session_date"]].append((minute, equity, bool(row.get("valuation_stale"))))
    if set(marks) != set(dates):
        raise ValueError("candidate minute session set differs from the audited calendar")
    prior = float(cfg.initial_capital)
    capital = prior
    rows = []
    for day in dates:
        values = sorted(marks[day], key=lambda row: row[0])
        start = datetime.combine(date.fromisoformat(day), time(9, 1), tzinfo=TAIPEI)
        if len(values) != 270 or any(stamp != start + timedelta(minutes=index)
                                    for index, (stamp, _equity, _stale) in enumerate(values)):
            raise ValueError("candidate is missing or duplicates a canonical minute")
        end_equity = values[-1][1]
        row = {"date": day, "end_equity_twd": end_equity,
               "daily_return_pct": (end_equity / prior - 1) * 100,
               "cumulative_return_pct": (end_equity / capital - 1) * 100,
               "carried_mark_minutes": sum(stale for _stamp, _equity, stale in values)}
        if include_minutes:
            # The grid was independently checked above. Do not include any
            # underlying broker/exchange quote or fabricate missing minutes.
            row["minute_equity_twd"] = [equity for _stamp, equity, _stale in values]
            row["minute_valuation_stale"] = [stale for _stamp, _equity, stale in values]
        rows.append(row)
        prior = end_equity
    if not math.isclose(prior, float(modes[cfg.market].get("total_equity_twd")), rel_tol=0.0, abs_tol=1e-5):
        raise ValueError("final equity differs from the committed final minute")
    if any(_sha(root / name) != hashes[name] for name in required):
        raise ValueError("candidate changed while building the public summary")
    _unchanged(signatures)
    return {"contract": CONTRACT, "market": cfg.market, "year": now.year,
            "generated_at": now.isoformat(timespec="seconds"), "simulation_only": True,
            "broker_fill": False, "exchange_fill": False, "paper_execution_enabled": False,
            "provisional": True, "source_completeness_verified": False,
            "all_rejected_entry_prices_independently_verified": False,
            "historical_decision_policy": receipt["historical_decision_policy"],
            "historical_bracket_cross_assumption": receipt["historical_bracket_cross_assumption"],
            "start_date": dates[0], "end_date": dates[-1], "session_count": len(dates),
            "minute_count": len(dates) * 270,
            "full_cross_assumptions": int(checked.get("provisional_full_cross_bracket_assumptions") or 0),
            "nontrade_assumptions": int(checked.get("provisional_nontrade_accounting_assumptions") or 0),
            "carried_mark_minutes": sum(row["carried_mark_minutes"] for row in rows),
            "end_equity_twd": prior, "cumulative_return_pct": rows[-1]["cumulative_return_pct"],
            "daily": rows}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--audit", type=Path, required=True)
    parser.add_argument("--candidate-config", type=Path, required=True)
    parser.add_argument("--checkpoint-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    allowed = (ROOT / "artifacts/operations/public_research").resolve()
    if not output.is_relative_to(allowed) or output.exists():
        raise ValueError("new public summary must be a nonexisting file in the public research directory")
    result = build(state_dir=args.state_dir, audit_path=args.audit, candidate_config=args.candidate_config,
                   checkpoint_sha256=args.checkpoint_sha256, now=datetime.now(TAIPEI))
    output.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(result, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n").encode()
    fd = os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps({"path": str(output), "sha256": hashlib.sha256(encoded).hexdigest(),
                      "sessions": result["session_count"], "minutes": result["minute_count"],
                      "provisional": True, "live_account_enabled": False}, ensure_ascii=False))


if __name__ == "__main__":
    main()

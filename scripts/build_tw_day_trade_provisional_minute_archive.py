#!/usr/bin/env python3
"""Create a pinned, public-safe minute-NAV archive from an audited replay.

This is a read-only projection, not a ledger promotion or live account. It
contains account equity and stale flags, never source quotes, positions or
private paths. A new output is required; existing research artifacts survive.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import gzip
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

from scripts.build_tw_day_trade_provisional_public_summary import build

CONTRACT = "public_audited_provisional_minute_equity_v1"


def project(*, state_dir: Path, audit_path: Path, candidate_config: Path,
            checkpoint_sha256: str, summary_path: Path, summary_sha256: str,
            now: datetime) -> dict:
    summary_bytes = summary_path.read_bytes()
    if hashlib.sha256(summary_bytes).hexdigest() != summary_sha256:
        raise ValueError("published daily summary changed")
    previous = json.loads(summary_bytes)
    current = build(state_dir=state_dir, audit_path=audit_path, candidate_config=candidate_config,
                    checkpoint_sha256=checkpoint_sha256, now=now, include_minutes=True)
    keys = ("contract", "market", "year", "simulation_only", "broker_fill", "exchange_fill",
            "paper_execution_enabled", "provisional", "source_completeness_verified",
            "historical_decision_policy", "historical_bracket_cross_assumption", "start_date",
            "end_date", "session_count", "minute_count", "full_cross_assumptions",
            "nontrade_assumptions", "carried_mark_minutes")
    if any(previous.get(key) != current.get(key) for key in keys):
        raise ValueError("audited replay differs from the published daily summary")
    old_rows, rows = previous.get("daily"), current.get("daily")
    if not isinstance(old_rows, list) or len(old_rows) != len(rows):
        raise ValueError("daily summary coverage differs")
    sessions = []
    for old, row in zip(old_rows, rows, strict=True):
        for key in ("date", "carried_mark_minutes"):
            if old.get(key) != row.get(key):
                raise ValueError("daily source identity differs")
        for key in ("end_equity_twd", "daily_return_pct", "cumulative_return_pct"):
            if not math.isclose(float(old[key]), float(row[key]), rel_tol=0.0, abs_tol=1e-9):
                raise ValueError("daily account values differ")
        sessions.append({"date": row["date"], "equity_twd": row["minute_equity_twd"],
                         "valuation_stale": row["minute_valuation_stale"]})
    # A final pin catches a concurrently replaced daily publication.
    if hashlib.sha256(summary_path.read_bytes()).hexdigest() != summary_sha256:
        raise ValueError("published daily summary changed during projection")
    return {"contract": CONTRACT, "market": current["market"], "year": current["year"],
            "summary_sha256": summary_sha256, "simulation_only": True, "provisional": True,
            "broker_fill": False, "exchange_fill": False, "source_completeness_verified": False,
            "session_count": len(sessions), "minute_count": len(sessions) * 270,
            "sessions": sessions}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("state-dir", "audit", "candidate-config", "summary", "output"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--checkpoint-sha256", required=True)
    parser.add_argument("--summary-sha256", required=True)
    args = parser.parse_args()
    allowed = (ROOT / "artifacts/operations/public_research").resolve(strict=True)
    output, summary = args.output.resolve(), args.summary.resolve(strict=True)
    if (not output.is_relative_to(allowed) or not summary.is_relative_to(allowed)
            or output.exists() or output.suffixes[-2:] != [".json", ".gz"]):
        raise ValueError("output must be a new .json.gz in the public research namespace")
    result = project(state_dir=args.state_dir, audit_path=args.audit,
        candidate_config=args.candidate_config, checkpoint_sha256=args.checkpoint_sha256,
        summary_path=summary, summary_sha256=args.summary_sha256,
        now=datetime.now(ZoneInfo("Asia/Taipei")))
    raw = json.dumps(result, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
    encoded = gzip.compress(raw, compresslevel=6, mtime=0)
    if len(raw) > 16 * 1024 * 1024 or len(encoded) > 4 * 1024 * 1024:
        raise ValueError("minute archive exceeds its bounded read contract")
    fd = os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps({"path": str(output), "sha256": hashlib.sha256(encoded).hexdigest(),
        "sessions": result["session_count"], "minutes": result["minute_count"],
        "compressed_bytes": len(encoded), "simulation_only": True, "live_account_enabled": False},
        ensure_ascii=False))


if __name__ == "__main__":
    main()

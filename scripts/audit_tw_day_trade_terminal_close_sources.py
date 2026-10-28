#!/usr/bin/env python3
"""Audit pinned paper close receipts against retained and current official data.

This is a read-only diagnostic. Auction receipts are not independent raw-tick
proof and full-quantity settlement is a paper assumption, never a broker fill.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, time
import hashlib
import json
import math
from pathlib import Path
import re
import sys
from typing import Callable
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_tw_day_trade_restart import verify_prefix
from scripts.settle_tw_day_trade_official_close import official_closes
from stockagent.live.paper_journal import open_paper_journal
from stockagent.live.tw_day_trade_simulation import TERMINAL_CLOSE_UNLIMITED_CONTRACT

TAIPEI = ZoneInfo("Asia/Taipei")
PURPOSE = "13_30_unlimited_close_paper_settlement"
OFFICIAL_SOURCES = {"twse_official_daily_close": "twse", "tpex_official_daily_close": "tpex"}
AUCTION_SOURCES = {"exchange_non_trial_tick", "exchange_non_trial_quote_trade"}


def _price(value):
    try:
        number = float(value)
        return number if math.isfinite(number) and number > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def _when(value):
    try:
        parsed = datetime.fromisoformat(str(value))
        return parsed.astimezone(TAIPEI) if parsed.utcoffset() is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def _equal(left, right):
    a, b = _price(left), _price(right)
    return a is not None and b is not None and math.isclose(a, b, rel_tol=0, abs_tol=1e-8)


def _has_traded_volume(row):
    try:
        volume = float(str((row.get("official_row") or {}).get("成交股數")).replace(",", ""))
        return math.isfinite(volume) and volume > 0
    except (TypeError, ValueError):
        return False


def compare(fills: list[dict], day: date, current: dict, pinned: Callable[[str, str], dict]) -> dict:
    """Compare each evidence receipt without inferring exchange fills."""
    errors, counts = [], Counter()
    deadline = datetime.combine(day, time(13, 30), tzinfo=TAIPEI)
    for index, fill in enumerate(fills):
        if fill.get("purpose") != PURPOSE or fill.get("session_date") != str(day):
            continue
        symbol = str(fill.get("symbol") or "")
        proof = fill.get("terminal_close_receipt") or {}
        evidence = proof.get("source_evidence") or {}
        basis, source = evidence.get("price_basis"), evidence.get("source")
        counts[str(basis)] += 1
        errors_before = len(errors)

        def fail(message):
            errors.append({"fill_index": index, "symbol": symbol, "reason": message})

        qty = fill.get("quantity")
        if (fill.get("simulation_only") is not True or fill.get("fill_contract") != TERMINAL_CLOSE_UNLIMITED_CONTRACT
                or proof.get("contract") != TERMINAL_CLOSE_UNLIMITED_CONTRACT
                or proof.get("broker_fill") is not False or proof.get("full_quantity_is_user_assumption") is not True
                or type(qty) is not int or qty <= 0 or fill.get("requested_quantity") != qty
                or fill.get("remaining_quantity") != 0 or _when(fill.get("fill_at")) != deadline
                or _when(proof.get("effective_at")) != deadline or evidence.get("session_date") != str(day)
                or not _equal(fill.get("price"), evidence.get("price"))):
            fail("invalid paper settlement, quantity, clock, or receipt")
        ref = current.get(symbol) or {}
        if not _equal(fill.get("price"), ref.get("price")):
            fail("price differs from current official close or official close unavailable")
        if not _has_traded_volume(ref):
            fail("current official close lacks positive reported trade volume")
        if basis == "official_session_close" and source in OFFICIAL_SOURCES:
            digest = str(evidence.get("source_sha256") or "")
            if not re.fullmatch(r"[0-9a-f]{64}", digest):
                fail("invalid official source digest")
            else:
                try:
                    retained = pinned(OFFICIAL_SOURCES[source], digest).get(symbol) or {}
                    if (retained.get("source") != source or retained.get("source_sha256") != digest
                            or not _equal(fill.get("price"), retained.get("price"))
                            or not _has_traded_volume(retained)):
                        fail("retained official source does not establish receipt price")
                    else:
                        counts["official_retained_digest_verified"] += 1
                except (OSError, ValueError, KeyError):
                    fail("retained official source missing, invalid, or digest mismatch")
        elif basis == "observed_regular_closing_auction" and source in AUCTION_SOURCES:
            exchanged, received, recorded = (_when(evidence.get("exchange_quote_at")),
                                             _when(evidence.get("received_at")),
                                             _when(proof.get("recorded_at")))
            if (not exchanged or not received or not recorded or exchanged.date() != day
                    or not deadline <= exchanged < deadline.replace(minute=34)
                    or not exchanged <= received <= recorded
                    or _price(evidence.get("observed_trade_volume_lots")) is None):
                fail("auction receipt has invalid clock or observed volume")
            else:
                # This validates only retained metadata, not the original tick stream.
                counts["auction_metadata_only"] += 1
        else:
            fail("unknown close price basis or source")
        if len(errors) == errors_before:
            counts["rows_without_detected_error"] += 1
    return {"row_counts": dict(counts), "errors": errors}


def audit(day: date, state_dir: Path, restart_path: Path, raw_root: Path) -> dict:
    restart = json.loads(restart_path.read_bytes())
    if (restart.get("status") != "accepted" or restart.get("simulation_only") is not True
            or restart.get("production_order_possible") is not False
            or Path(restart.get("state_dir") or "").resolve() != state_dir.resolve()):
        raise ValueError("restart receipt does not pin this paper ledger")
    prefixes = verify_prefix(state_dir, restart)
    closes, sources = official_closes(raw_root, day)
    cache = {}

    def pinned(exchange, digest):
        key = (exchange, digest)
        if key not in cache:
            root = raw_root / ".close_evidence" / digest
            data, evidence = official_closes(root, day, exchanges=(exchange,))
            if len(evidence) != 1 or evidence[0]["sha256"] != digest:
                raise ValueError("retained raw official digest mismatch")
            cache[key] = (data, evidence[0])
        return cache[key][0]

    rows, size = [], restart["inputs"]["fills.jsonl"]["size_bytes"]
    with open_paper_journal(state_dir / "fills.jsonl", "rb") as stream:
        while stream.tell() < size:
            line = stream.readline()
            if not line or stream.tell() > size or not line.endswith(b"\n"):
                raise ValueError("pinned fill prefix is not a complete line boundary")
            if str(day).encode() in line and PURPOSE.encode() in line:
                rows.append(json.loads(line))
    result = compare(rows, day, closes, pinned)
    if verify_prefix(state_dir, restart) != prefixes or official_closes(raw_root, day)[1] != sources:
        raise RuntimeError("ledger or current official evidence changed during audit")
    for (_, digest), (_, evidence) in cache.items():
        if hashlib.sha256(Path(evidence["path"]).read_bytes()).hexdigest() != digest:
            raise RuntimeError("retained official evidence changed during audit")
    status = ("source_price_comparison_failed" if result["errors"] else
              "source_price_comparison_passed" if rows else "no_terminal_fills_to_compare")
    return {"contract": "independent_paper_terminal_close_source_audit_v1", "session_date": str(day),
            "status": status,
            "simulation_only": True, "production_order_possible": False, "production_state_modified": False,
            "promotable_ledger": False, "source_journal_prefixes": prefixes,
            "official_sources": sources, "retained_official_sources": [row[1] for row in cache.values()],
            "caveat": "Checks paper receipt prices against retained and current official reports. Auction tick provenance is receipt metadata only; depth, broker fills, model replay and PnL are not proven.",
            **result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--restart-audit", type=Path, required=True)
    parser.add_argument("--raw-root", type=Path, default=Path("data_tw_public/raw"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output, state = args.output.resolve(), args.state_dir.resolve()
    if output.is_relative_to(state) or output.exists():
        raise ValueError("output must be new and outside the production ledger")
    result = audit(args.date, state, args.restart_audit.resolve(), args.raw_root.resolve())
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    print(json.dumps({"status": result["status"], "counts": result["row_counts"],
                      "errors": len(result["errors"]), "output": str(output)}, ensure_ascii=False))
    if result["errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

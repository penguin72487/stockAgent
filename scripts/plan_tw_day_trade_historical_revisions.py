#!/usr/bin/env python3
"""Pin published intraday decisions for an isolated historical repair plan.

This only inventories the existing source artifacts through the canonical
revision owner.  It neither executes a paper order nor modifies a ledger.
The resulting plan is not an authorization to replace a later accounting
chain; that requires a separate replay and independent acceptance.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import repair_tw_day_trade_flat_opening as revision_owner
from scripts.repair_tw_day_trade_flat_opening import _published_revision_inventory, _spec
from stockagent.live.tw_day_trade_simulation import TAIPEI


def plan(*, session: date, markets: list[str], markets_dir: Path) -> dict:
    if not markets or len(markets) != len(set(markets)) or any(not m.strip() for m in markets):
        raise ValueError("unique nonempty market IDs are required")
    if session >= datetime.now(TAIPEI).date():
        raise ValueError("historical planning requires an earlier completed session")
    code_paths = (Path(__file__).resolve(), Path(revision_owner.__file__).resolve())
    def fingerprints():
        return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in code_paths}
    code_before = fingerprints()
    plans = []
    for market in markets:
        spec, _ = _spec(market, markets_dir, True)
        plans.append(_published_revision_inventory(spec, session))
    if fingerprints() != code_before:
        raise RuntimeError("historical planning implementation changed during source scan")
    return {
        "schema_version": 1,
        "status": "sources_pinned_not_replayed",
        "session_date": session.isoformat(),
        "created_at": datetime.now(TAIPEI).isoformat(),
        "simulation_only": True,
        "production_order_possible": False,
        "broker_orders_created": 0,
        "ledger_modified": False,
        "implementation_files_sha256_before_and_after": code_before,
        "later_session_accounting_replay_required": True,
        "source_plans": plans,
    }


def _write_new_plan(path: Path, payload: dict) -> None:
    """Publish a fully flushed private receipt without replacing another."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--market", action="append", required=True)
    parser.add_argument("--markets-dir", type=Path, default=ROOT / "services/discord_bot/markets")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    live = (ROOT / "service_data/live/tw_day_trade_simulation").resolve()
    if output.is_relative_to(live) or output.exists():
        raise ValueError("a new output outside the live ledger is required")
    result = plan(session=date.fromisoformat(args.session_date), markets=args.market,
                  markets_dir=args.markets_dir.resolve(strict=True))
    _write_new_plan(output, result)
    print(f"sources_pinned_not_replayed: {output}")


if __name__ == "__main__":
    main()

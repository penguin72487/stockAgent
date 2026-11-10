#!/usr/bin/env python3
"""Prepare isolated historical paper inputs using the existing repair owner.

This is diagnostic only. It does not create an engine, execute paper fills,
apply a ledger, contact a quote provider, or authorize publication. Unpriced
targets stay explicit and block a complete reconstruction claim.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import json
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import repair_tw_day_trade_flat_opening as repair
from scripts.plan_tw_day_trade_historical_revisions import _write_new_plan
from stockagent.data_sync import workspace_volume
from stockagent.live.tw_day_trade_simulation import TAIPEI


def _checked_anchor(receipt: dict, *, market: str, day: date) -> Path:
    if (receipt.get("contract") != "historical_flat_paper_anchor_component_v1"
            or receipt.get("status") != "prior_flat_anchor_component_verified_not_replayed"
            or receipt.get("market") != market or receipt.get("target_session") != str(day)
            or receipt.get("simulation_only") is not True or receipt.get("ledger_modified") is not False
            or receipt.get("publication_authorized") is not False):
        raise ValueError("matching, verified private paper anchor is required")
    pins = receipt.get("source_pins")
    code = receipt.get("implementation_sha256")
    if not isinstance(pins, dict) or not isinstance(code, dict) or not code:
        raise ValueError("anchor source and implementation pins are required")
    for pin in pins.values():
        path = Path(pin["path"])
        if not path.is_file() or repair._sha256(path) != pin["sha256"]:
            raise RuntimeError("prior paper anchor source changed")
    for path, digest in code.items():
        if repair._sha256(Path(path)) != digest:
            raise RuntimeError("prior paper anchor audit implementation changed")
    state = Path(pins["anchor_state"]["path"])
    if state.name != "state.json":
        raise ValueError("anchor state pin must identify the original state.json")
    return state.parent


def prepare(*, day: date, market: str, anchor_receipt: Path, scope_root: Path,
            minute_root: Path, price_limit_root: Path, output_root: Path,
            markets_dir: Path) -> dict:
    if day >= datetime.now(TAIPEI).date() or not market or Path(market).name != market:
        raise ValueError("an exact completed historical market/session is required")
    workspace = workspace_volume.configuration()
    root = output_root.resolve()
    build_root = Path(workspace["mount_point"]) / "build"
    if not root.is_relative_to(build_root) or root == build_root or root.exists():
        raise ValueError("a new isolated E build directory is required")
    workspace_volume.admit(required_bytes=128 * 1024**2, c=workspace)
    implementation = (Path(__file__).resolve(), Path(repair.__file__).resolve(),
                      ROOT / "scripts/rebuild_tw_day_trade_open_price_replay.py",
                      ROOT / "stockagent/live/quote_provider.py",
                      ROOT / "stockagent/live/tw_day_trade_simulation.py")
    implementation_before = {str(path): repair._sha256(path) for path in implementation}
    anchor_receipt_sha = repair._sha256(anchor_receipt)
    receipt = repair._object(anchor_receipt)
    anchor_dir = _checked_anchor(receipt, market=market, day=day)
    mode = repair._object(anchor_dir / "state.json")["modes"][market]
    repair._require_missing_session_target(mode, day, events_path=anchor_dir / "events.jsonl")
    spec, live_config = repair._spec(market, markets_dir, True)
    scope = scope_root / market
    plan, inventory = repair._object(scope / "published_revision_plan.json"), repair._object(scope / "source_inventory.json")
    if (plan.get("market") != market or plan.get("session_date") != str(day)
            or inventory.get("session_date") != str(day) or inventory.get("ledger_modified") is not False
            or plan.get("plan_sha256") != inventory.get("plan_sha256")
            or repair._published_revision_inventory(spec, day)["plan_sha256"] != plan.get("plan_sha256")):
        raise RuntimeError("historical signal scope changed or belongs to another account")
    first = plan["versions"][0]
    first_summary = repair._object(Path(first["summary_path"]))
    token = uuid.uuid4().hex[:12]
    manifest = {
        "contract": repair.PUBLISHED_SESSION_CONTRACT, "simulation_only": True,
        "production_order_possible": False, "preserve_original_signal": False,
        "append_missing_session": True, "published_revisions": True,
        "provisional_minute_source_coverage": True, "nontrade_tick_receipt": None,
        "market": market, "live_dir": str(anchor_dir), "operation_root": str(root),
        "price_limit_dir": str(price_limit_root), "finmind_price_limit_source_root": str(price_limit_root),
        "finmind_replay_source_roots": [str(minute_root)],
        "minute_roots": [str(minute_root), *map(str, repair.DEFAULT_MINUTE_DATA_ROOTS)],
        "created_at": datetime.now(TAIPEI).isoformat(), "anchor": repair._anchor(mode),
        "published_revision_plan": plan,
        "sessions": [{"session_date": str(day), "summary_path": first["summary_path"],
                      "repair_signal_id": f"{first['signal_id']}:diagnostic:{token}",
                      "repair_token": token,
                      "original": {**first, "config_fingerprint": first_summary["config_fingerprint"],
                                   "checkpoint_fingerprint": first_summary["checkpoint_fingerprint"],
                                   "generated_at": first_summary["generated_at"]}}],
    }
    workspace_volume.admit(required_bytes=128 * 1024**2, c=workspace)
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    sessions, source_pins = repair._prepared_sessions(manifest, spec, live_config, fetch_current=True)
    repair._assert_source_pins(source_pins)
    if len(sessions) != 1 or sessions[0]["day"] != day:
        raise RuntimeError("prepared input has wrong session scope")
    session = sessions[0]
    if (1 + len(session.get("revisions") or []) != plan.get("selected_revision_count")
            or repair._sha256(anchor_receipt) != anchor_receipt_sha
            or any(repair._sha256(path) != implementation_before[str(path)] for path in implementation)):
        raise RuntimeError("prepared revisions or anchor receipt changed")
    result = {
        "contract": "historical_paper_replay_input_diagnostic_v1",
        "status": "inputs_checked_not_replayed", "market": market, "session_date": str(day),
        "anchor_receipt_path": str(anchor_receipt.resolve()),
        "anchor_receipt_sha256": anchor_receipt_sha,
        "plan_sha256": plan["plan_sha256"],
        "selected_revision_count": 1 + len(session.get("revisions") or []),
        "price_source_symbols": len(session["bars"]),
        "first_minute_price_method_counts": (session.get("book_proof") or {}).get("price_method_counts", {}),
        "original_nonzero_target_count": inventory["nonzero_target_count"],
        "official_open_unavailable_symbols": inventory["official_open_unavailable_symbols"],
        "unresolved_official_open_symbols": inventory["unresolved_official_open_symbols"],
        "intraday_source_gaps_not_interpreted_as_no_trade": True,
        "source_pins": source_pins, "implementation_sha256": implementation_before,
        "simulation_only": True, "broker_orders_created": 0,
        "ledger_modified": False, "paper_fills_created": 0, "publication_authorized": False,
    }
    _write_new_plan(root / "historical_input_diagnostic.json", result)
    return {key: value for key, value in result.items() if key != "source_pins"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", type=date.fromisoformat, required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--anchor-receipt", type=Path, required=True)
    parser.add_argument("--scope-root", type=Path, required=True)
    parser.add_argument("--minute-root", type=Path, required=True)
    parser.add_argument("--price-limit-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--markets-dir", type=Path, default=ROOT / "services/discord_bot/markets")
    args = parser.parse_args()
    print(json.dumps(prepare(day=args.day, market=args.market,
                             anchor_receipt=args.anchor_receipt.resolve(strict=True),
                             scope_root=args.scope_root.resolve(strict=True),
                             minute_root=args.minute_root.resolve(strict=True),
                             price_limit_root=args.price_limit_root.resolve(strict=True),
                             output_root=args.output_root,
                             markets_dir=args.markets_dir.resolve(strict=True)), ensure_ascii=False))


if __name__ == "__main__":
    main()

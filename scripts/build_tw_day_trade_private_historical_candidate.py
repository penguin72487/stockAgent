#!/usr/bin/env python3
"""Build one explicitly provisional historical paper candidate, never apply it.

Only an audited flat source snapshot and fully priced target universe are
accepted. Missing minute observations are disclosed; they never authorize a
fill. The result must remain private and cannot authorize a ledger swap.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import date, datetime
import json
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import audit_tw_day_trade_flat_snapshot_anchor as snapshot_audit
from scripts import repair_tw_day_trade_flat_opening as owner
from scripts.plan_tw_day_trade_historical_revisions import _write_new_plan
from scripts.verify_tw_day_trade_scoped_repair import verify_minute_source_coverage, verify_published_fill_accounting
from stockagent.data_sync import workspace_volume
from stockagent.live.paper_journal import open_paper_journal
from stockagent.live.tw_day_trade_simulation import SIMULATION_SCHEMA_VERSION, TAIPEI, TwDayTradeSimulationEngine


def _audited_mode(receipt_path: Path, *, market: str, day: date, live_dir: Path) -> tuple[dict, dict]:
    receipt = owner._object(receipt_path)
    if (receipt.get("contract") != "historical_flat_snapshot_anchor_component_v1"
            or receipt.get("status") != "snapshot_accounts_verified_not_replayed"
            or receipt.get("target_session") != str(day)
            or receipt.get("simulation_only") is not True or receipt.get("broker_orders_created") != 0
            or receipt.get("ledger_modified") is not False or receipt.get("publication_authorized") is not False):
        raise ValueError("verified private snapshot receipt is required")
    accounts = [item for item in receipt.get("accounts", []) if item.get("market") == market]
    pins = receipt.get("source_pins") or {}
    if len(accounts) != 1 or any(name not in pins for name in ("snapshot", "events", "fills", "marks")):
        raise ValueError("exact audited account and its original source pins are required")
    # Re-run the canonical snapshot/account audit: a receipt is not trusted
    # merely because it names plausible source files.
    current = snapshot_audit.audit(snapshot=Path(pins["snapshot"]["path"]), live_dir=live_dir,
        markets=[market], previous_day=date.fromisoformat(receipt["previous_session"]),
        anchor_day=date.fromisoformat(receipt["anchor_session"]), target_day=day, calendar_root=ROOT / "data_tw_public")
    if (current["accounts"] != accounts or current["official_calendar_sha256"] != receipt["official_calendar_sha256"]
            or any(pins.get(key) != value for key, value in current["source_pins"].items())):
        raise RuntimeError("flat account proof or original source changed")
    state = owner._object(Path(pins["snapshot"]["path"]))
    return deepcopy(state["modes"][market]), current


def _check_plan(*, scope: Path, spec, market: str, day: date) -> tuple[dict, dict]:
    plan = owner._object(scope / "published_revision_plan.json")
    inventory = owner._object(scope / "source_inventory.json")
    if (plan.get("market") != market or plan.get("session_date") != str(day)
            or inventory.get("session_date") != str(day) or inventory.get("ledger_modified") is not False
            or inventory.get("official_open_unavailable_symbols")
            or inventory.get("unresolved_official_open_symbols")
            or plan.get("plan_sha256") != inventory.get("plan_sha256")
            or owner._published_revision_inventory(spec, day)["plan_sha256"] != plan.get("plan_sha256")):
        raise RuntimeError("decision scope changed or an opening reference remains unresolved")
    return plan, inventory


def _price_limit_source(price_limit_root: Path, *, day: date) -> str | None:
    """Use the existing official snapshot directly or a verified overlay."""
    root = price_limit_root.resolve(strict=True)
    if root == (ROOT / "artifacts/live/tw_price_limits").resolve(strict=True):
        if not (root / f"{day}.parquet").is_file():
            raise ValueError("exact-session canonical price limits are unavailable")
        return None
    if not (root / "finmind_price_limits_source.json").is_file():
        raise ValueError("noncanonical price limits require their private source manifest")
    return str(root)


def build(*, day: date, market: str, anchor_receipt: Path, scope_root: Path,
          minute_root: Path, price_limit_root: Path, output_root: Path, live_dir: Path,
          markets_dir: Path) -> dict:
    if day >= datetime.now(TAIPEI).date() or not market or Path(market).name != market:
        raise ValueError("one exact completed historical market/session is required")
    workspace = workspace_volume.configuration()
    root = output_root.resolve()
    build_root = Path(workspace["mount_point"]) / "build"
    if not root.is_relative_to(build_root) or root == build_root or root.exists():
        raise ValueError("a new isolated E build directory is required")
    workspace_volume.admit(required_bytes=512 * 1024**2, c=workspace)
    code = (Path(__file__).resolve(), ROOT / "scripts/repair_tw_day_trade_flat_opening.py",
            ROOT / "scripts/rebuild_tw_day_trade_open_price_replay.py",
            ROOT / "scripts/verify_tw_day_trade_scoped_repair.py",
            ROOT / "scripts/audit_tw_day_trade_flat_snapshot_anchor.py",
            ROOT / "stockagent/live/tw_day_trade_simulation.py")
    code_before = {str(path): owner._sha256(path) for path in code}
    receipt_sha = owner._sha256(anchor_receipt)
    mode, anchor_proof = _audited_mode(anchor_receipt, market=market, day=day, live_dir=live_dir)
    spec, live_config = owner._spec(market, markets_dir, True)
    scope = scope_root / market
    plan, inventory = _check_plan(scope=scope, spec=spec, market=market, day=day)
    price_limit_overlay = _price_limit_source(price_limit_root, day=day)
    first = plan["versions"][0]
    summary = owner._object(Path(first["summary_path"]))
    token = uuid.uuid4().hex[:12]
    manifest = {"contract": owner.PUBLISHED_SESSION_CONTRACT, "simulation_only": True,
        "production_order_possible": False, "preserve_original_signal": False,
        "append_missing_session": True, "published_revisions": True,
        "provisional_minute_source_coverage": True, "nontrade_tick_receipt": None,
        "market": market, "live_dir": str(live_dir), "operation_root": str(root),
        "price_limit_dir": str(price_limit_root), "finmind_price_limit_source_root": price_limit_overlay,
        "finmind_replay_source_roots": [str(minute_root)],
        "minute_roots": [str(minute_root), *map(str, owner.DEFAULT_MINUTE_DATA_ROOTS)],
        "created_at": datetime.now(TAIPEI).isoformat(), "anchor": owner._anchor(mode),
        "published_revision_plan": plan,
        "sessions": [{"session_date": str(day), "summary_path": first["summary_path"],
                      "repair_signal_id": f"{first['signal_id']}:private-candidate:{token}",
                      "repair_token": token,
                      "original": {**first, "config_fingerprint": summary["config_fingerprint"],
                                   "checkpoint_fingerprint": summary["checkpoint_fingerprint"],
                                   "generated_at": summary["generated_at"]}}]}
    workspace_volume.admit(required_bytes=512 * 1024**2, c=workspace)
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    sessions, pins = owner._prepared_sessions(manifest, spec, live_config, fetch_current=True)
    if len(sessions) != 1 or sessions[0]["day"] != day:
        raise RuntimeError("prepared session scope changed")
    candidate = root / "candidate"
    candidate.mkdir(mode=0o700)
    owner._atomic_json(candidate / "state.json", {"schema_version": SIMULATION_SCHEMA_VERSION,
                       "modes": {market: mode}, "enabled_markets": [market]})
    engine = TwDayTradeSimulationEngine(candidate, publication_clock=lambda: datetime.now(TAIPEI))
    accepted = owner._execute(engine, spec, sessions, reason=owner.PUBLISHED_SESSION_CONTRACT,
                              prior_events_path=live_dir / "events.jsonl")
    curves = owner._curve_proof(candidate / "marks.jsonl", market, sessions)
    with open_paper_journal(candidate / "fills.jsonl") as handle:
        fills = [json.loads(line) for line in handle if line.endswith(b"\n")]
    with open_paper_journal(candidate / "marks.jsonl") as handle:
        marks = [json.loads(line) for line in handle if line.endswith(b"\n")]
    item = sessions[0]
    accounting = verify_published_fill_accounting(engine.state["modes"][market], fills, day=str(day),
        plan=plan, bars=item["bars"], prior_realized=float(manifest["anchor"]["cumulative_realized_net_pnl_twd"]),
        terminal_close_sources=(accepted[0].get("closing_proof") or {}).get("terminal_close_sources", []),
        minute_source_files=item["bar_proof"]["source_files"], require_observed_terminal_minute=True)
    coverage = verify_minute_source_coverage(engine.state["modes"][market], marks, market=market,
        day=str(day), source_files=item["bar_proof"]["source_files"], allow_unverified_gaps=True, bars=item["bars"])
    owner._assert_source_pins(pins)
    if (owner._sha256(anchor_receipt) != receipt_sha
            or any(owner._sha256(path) != code_before[str(path)] for path in code)
            or owner._published_revision_inventory(spec, day)["plan_sha256"] != plan["plan_sha256"]):
        raise RuntimeError("candidate implementation, account proof or decision source changed")
    candidate_files = {path.name: owner._sha256(path) for path in candidate.iterdir() if path.is_file()}
    result = {"contract": "private_historical_progressive_candidate_v1",
        "status": "private_candidate_verified_not_applied", "session_date": str(day), "market": market,
        "candidate_path": str(candidate), "candidate_file_sha256": candidate_files,
        "anchor_receipt": str(anchor_receipt), "anchor_receipt_sha256": receipt_sha,
        "anchor_account": anchor_proof["accounts"][0], "original_plan_sha256": plan["plan_sha256"],
        "selected_revision_count": plan["selected_revision_count"], "scope_nonzero_targets": inventory["nonzero_target_count"],
        "session": accepted, "minute_curves": curves, "fill_accounting": accounting,
        "minute_source_coverage": coverage, "source_pins": pins, "implementation_sha256": code_before,
        "first_entry_is_counterfactual_09_01": True, "missing_minutes_are_unknown_not_zero": True,
        "simulation_only": True, "broker_orders_created": 0, "live_ledger_modified": False,
        "subsequent_sessions_replayed": False, "publication_authorized": False}
    _write_new_plan(root / "private_candidate_receipt.json", result)
    return {key: value for key, value in result.items() if key not in {"source_pins", "minute_source_coverage"}}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", type=date.fromisoformat, required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--anchor-receipt", type=Path, required=True)
    parser.add_argument("--scope-root", type=Path, required=True)
    parser.add_argument("--minute-root", type=Path, required=True)
    parser.add_argument("--price-limit-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--live-dir", type=Path, default=ROOT / "service_data/live/tw_day_trade_simulation")
    parser.add_argument("--markets-dir", type=Path, default=ROOT / "services/discord_bot/markets")
    args = parser.parse_args()
    result = build(day=args.day, market=args.market, anchor_receipt=args.anchor_receipt.resolve(strict=True),
        scope_root=args.scope_root.resolve(strict=True), minute_root=args.minute_root.resolve(strict=True),
        price_limit_root=args.price_limit_root.resolve(strict=True), output_root=args.output_root,
        live_dir=args.live_dir.resolve(strict=True), markets_dir=args.markets_dir.resolve(strict=True))
    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()

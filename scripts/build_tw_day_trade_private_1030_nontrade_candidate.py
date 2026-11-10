#!/usr/bin/env python3
"""Build the explicitly authorized 2026-10-30 Unit MGDA paper candidate.

This is an isolated historical replay adapter, not a live executor or a ledger
promotion tool. The execution and accounting implementations are shared with
the existing repair owner; its settlement hook is constrained in this process
to the one user-approved symbol and pinned observed tick.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, time
import json
from pathlib import Path
import sys
from unittest.mock import patch
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import audit_tw_day_trade_nontrade as nontrade_audit
from scripts import build_tw_day_trade_private_historical_candidate as base
from scripts import repair_tw_day_trade_flat_opening as owner
from scripts.plan_tw_day_trade_historical_revisions import _write_new_plan
from scripts.verify_tw_day_trade_scoped_repair import (
    verify_minute_source_coverage,
    verify_published_fill_accounting,
)
from stockagent.data_sync import workspace_volume
from stockagent.live.paper_journal import open_paper_journal
from stockagent.live.tw_day_trade_simulation import SIMULATION_SCHEMA_VERSION, TAIPEI, TwDayTradeSimulationEngine

DAY = date(2026, 10, 30)
MARKET = "tw_day_trade_unit_mgda"
SYMBOL = "2235"
OBSERVED_AT = "2026-10-30T11:37:57.688508+08:00"
OBSERVED_PRICE = 30.6
TICK_SHA256 = "4808e1f96e78f3b3b7496998e3a8867fae9eb5cb1620323bc925651f3bcf5570"


def _missing_close_symbols(mode: dict, item: dict) -> set[str]:
    key = datetime.combine(item["day"], time(13, 30), tzinfo=TAIPEI).isoformat(timespec="minutes")
    return {str(p.get("symbol") or "") for p in (mode.get("positions") or {}).values()
            if int(p.get("signed_shares") or 0)
            and str(p.get("symbol") or "") not in (item.get("delayed_close_quotes") or {})
            and key not in (item["bars"].get(str(p.get("symbol") or "")) or {})}


def _guarded_nontrade(delegate, engine, mode: dict, item: dict, quotes: dict) -> dict:
    """Fail before the shared owner books any non-approved residual."""
    if item.get("day") != DAY or mode.get("market") != MARKET or _missing_close_symbols(mode, item) != {SYMBOL}:
        raise RuntimeError("nontrade settlement is authorized only for 2026-10-30 Unit MGDA 2235")
    result = delegate(engine, mode, item, quotes)
    details = [p["nontrade_settlement"] for p in (mode.get("positions") or {}).values()
               if p.get("nontrade_settlement")]
    if (result.get("symbols") != [SYMBOL] or not details
            or any((ref := d.get("reference") or {}).get("symbol") != SYMBOL
                   or ref.get("reference_observed_at") != OBSERVED_AT
                   or ref.get("reference_price") != OBSERVED_PRICE
                   or ref.get("source_sha256") != TICK_SHA256
                   or any(d.get(k) is not False for k in ("simulated_order", "simulated_fill", "exchange_fill", "broker_fill"))
                   for d in details)):
        raise RuntimeError("settlement differs from the exact approved source observation")
    return result


def _rows(path: Path) -> list[dict]:
    with open_paper_journal(path) as stream:
        rows = []
        for line in stream:
            if not line.endswith(b"\n"):
                raise RuntimeError(f"torn private journal: {path.name}")
            row = json.loads(line)
            if not isinstance(row, dict):
                raise RuntimeError(f"invalid private journal row: {path.name}")
            rows.append(row)
        return rows


def build(*, anchor_receipt: Path, scope_root: Path, minute_root: Path,
          price_limit_root: Path, tick_receipt: Path, output_root: Path,
          live_dir: Path, markets_dir: Path) -> dict:
    workspace = workspace_volume.configuration()
    root = output_root.resolve()
    build_root = Path(workspace["mount_point"]) / "build"
    if not root.is_relative_to(build_root) or root == build_root or root.exists():
        raise ValueError("a new isolated E build directory is required")
    if DAY >= datetime.now(TAIPEI).date():
        raise ValueError("the authorized session must already have completed")
    workspace_volume.admit(required_bytes=512 * 1024**2, c=workspace)
    code = (Path(__file__).resolve(), Path(base.__file__).resolve(), Path(owner.__file__).resolve(),
            Path(nontrade_audit.__file__).resolve(), ROOT / "scripts/rebuild_tw_day_trade_open_price_replay.py",
            ROOT / "scripts/verify_tw_day_trade_scoped_repair.py", ROOT / "scripts/audit_tw_day_trade_flat_snapshot_anchor.py",
            ROOT / "stockagent/live/tw_day_trade_simulation.py", ROOT / "stockagent/live/tw_day_trade_nontrade.py")
    code_before = {str(path): owner._sha256(path) for path in code}
    anchor_sha = owner._sha256(anchor_receipt)
    mode, anchor_proof = base._audited_mode(anchor_receipt, market=MARKET, day=DAY, live_dir=live_dir)
    spec, live_config = owner._spec(MARKET, markets_dir, True)
    plan, inventory = base._check_plan(scope=scope_root / MARKET, spec=spec, market=MARKET, day=DAY)
    first = plan["versions"][0]
    summary = owner._object(Path(first["summary_path"]))
    token = uuid.uuid4().hex[:12]
    manifest = {"contract": owner.PUBLISHED_SESSION_CONTRACT, "simulation_only": True,
        "production_order_possible": False, "preserve_original_signal": False,
        "append_missing_session": True, "published_revisions": True,
        "provisional_minute_source_coverage": True, "nontrade_tick_receipt": str(tick_receipt),
        "market": MARKET, "live_dir": str(live_dir), "operation_root": str(root),
        "price_limit_dir": str(price_limit_root),
        "finmind_price_limit_source_root": base._price_limit_source(price_limit_root, day=DAY),
        "finmind_replay_source_roots": [str(minute_root)],
        "minute_roots": [str(minute_root), *map(str, owner.DEFAULT_MINUTE_DATA_ROOTS)],
        "created_at": datetime.now(TAIPEI).isoformat(), "anchor": owner._anchor(mode),
        "published_revision_plan": plan,
        "sessions": [{"session_date": str(DAY), "summary_path": first["summary_path"],
                      "repair_signal_id": f"{first['signal_id']}:private-candidate:{token}",
                      "repair_token": token,
                      "original": {**first, "config_fingerprint": summary["config_fingerprint"],
                                   "checkpoint_fingerprint": summary["checkpoint_fingerprint"],
                                   "generated_at": summary["generated_at"]}}]}
    workspace_volume.admit(required_bytes=512 * 1024**2, c=workspace)
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    sessions, pins = owner._prepared_sessions(manifest, spec, live_config, fetch_current=True)
    if len(sessions) != 1 or sessions[0]["day"] != DAY:
        raise RuntimeError("prepared session scope changed")
    candidate = root / "candidate"
    candidate.mkdir(mode=0o700)
    owner._atomic_json(candidate / "state.json", {"schema_version": SIMULATION_SCHEMA_VERSION,
                       "modes": {MARKET: mode}, "enabled_markets": [MARKET]})
    engine = TwDayTradeSimulationEngine(candidate, publication_clock=lambda: datetime.now(TAIPEI))
    original_settle = owner._settle_missing_close_nontrade
    # The hook is process-local and restored immediately; the live service and
    # canonical owner file are never changed. Its implementation is pinned.
    with patch.object(owner, "_settle_missing_close_nontrade",
                      lambda engine, mode, item, quotes: _guarded_nontrade(original_settle, engine, mode, item, quotes)):
        accepted = owner._execute(engine, spec, sessions, reason=owner.PUBLISHED_SESSION_CONTRACT,
                                  prior_events_path=live_dir / "events.jsonl")
    item = sessions[0]
    settlement = (accepted[0].get("closing_proof") or {}).get("nontrade_settlement") or {}
    if settlement.get("symbols") != [SYMBOL] or settlement.get("position_count", 0) < 1:
        raise RuntimeError("the approved settlement was not recorded")
    curves = owner._curve_proof(candidate / "marks.jsonl", MARKET, sessions)
    fills, events, marks = (_rows(candidate / name) for name in ("fills.jsonl", "events.jsonl", "marks.jsonl"))
    nontrade_events = [row for row in events if row.get("event") == "nontrade_last_observed_accounting_settlement"]
    if any(str(row.get("purpose") or "").startswith("nontrade_") for row in (*fills, *_rows(candidate / "orders.jsonl"))):
        raise RuntimeError("nontrade bookkeeping must not appear in fills or orders")
    accounting = verify_published_fill_accounting(engine.state["modes"][MARKET], fills, day=str(DAY),
        plan=plan, bars=item["bars"], prior_realized=float(manifest["anchor"]["cumulative_realized_net_pnl_twd"]),
        terminal_close_sources=(accepted[0].get("closing_proof") or {}).get("terminal_close_sources", []),
        minute_source_files=item["bar_proof"]["source_files"], nontrade_events=nontrade_events,
        require_observed_terminal_minute=True)
    coverage = verify_minute_source_coverage(engine.state["modes"][MARKET], marks, market=MARKET,
        day=str(DAY), source_files=item["bar_proof"]["source_files"], allow_unverified_gaps=True, bars=item["bars"])
    pins.extend(item.get("nontrade_source_pins") or [])
    pins.extend({"path": src["path"], "sha256": src["sha256"]}
                for src in (accepted[0].get("closing_proof") or {}).get("terminal_close_sources", []))
    for path in (markets_dir / f"{MARKET}.yaml", Path(spec.config_path), Path(spec.checkpoint_path)):
        pins.append({"path": str(path.resolve()), "sha256": owner._sha256(path)})
    owner._assert_source_pins(pins)
    # Give the independent arithmetic auditor an isolated, non-promotable
    # manifest; it is not a live ledger-repair or apply manifest.
    audit_root = root / "independent_audit_input"
    audit_root.mkdir(mode=0o700)
    audit_manifest = {"status": "candidate_validated_not_applied", "apply_prohibited": True,
        "purpose": "private_independent_nontrade_audit_only", "nontrade_tick_receipt": str(tick_receipt),
        "provisional_minute_source_coverage": True, "source_pins": pins,
        "candidate_dir": str(candidate), "market": MARKET, "sessions": [{"session_date": str(DAY)}],
        "anchor": manifest["anchor"]}
    owner._atomic_json(audit_root / "ledger_repair_manifest.json", audit_manifest)
    independent = nontrade_audit.audit(audit_root)
    if (owner._sha256(anchor_receipt) != anchor_sha
            or any(owner._sha256(path) != code_before[str(path)] for path in code)
            or owner._published_revision_inventory(spec, DAY)["plan_sha256"] != plan["plan_sha256"]):
        raise RuntimeError("candidate implementation, anchor or decision source changed")
    owner._assert_source_pins(pins)
    candidate_files = {path.name: owner._sha256(path) for path in candidate.iterdir() if path.is_file()}
    result = {"contract": "private_historical_progressive_candidate_v1",
        "status": "private_candidate_verified_not_applied", "session_date": str(DAY), "market": MARKET,
        "candidate_path": str(candidate), "candidate_file_sha256": candidate_files,
        "anchor_receipt": str(anchor_receipt), "anchor_receipt_sha256": anchor_sha,
        "anchor_account": anchor_proof["accounts"][0], "original_plan_sha256": plan["plan_sha256"],
        "selected_revision_count": plan["selected_revision_count"], "scope_nonzero_targets": inventory["nonzero_target_count"],
        "session": accepted, "minute_curves": curves, "fill_accounting": accounting,
        "minute_source_coverage": coverage, "source_pins": pins, "implementation_sha256": code_before,
        "nontrade_authorization_scope": {"session_date": str(DAY), "market": MARKET, "symbol": SYMBOL,
            "reference_observed_at": OBSERVED_AT, "reference_price": OBSERVED_PRICE, "source_sha256": TICK_SHA256},
        "independent_nontrade_audit": independent,
        "independent_nontrade_audit_sha256": owner._sha256(audit_root / "nontrade_accounting_audit.json"),
        "first_entry_is_counterfactual_09_01": True, "missing_minutes_are_unknown_not_zero": True,
        "simulation_only": True, "broker_orders_created": 0, "live_ledger_modified": False,
        "subsequent_sessions_replayed": False, "publication_authorized": False}
    _write_new_plan(root / "private_candidate_receipt.json", result)
    return {key: value for key, value in result.items() if key not in {"source_pins", "minute_source_coverage"}}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("anchor-receipt", "scope-root", "minute-root", "price-limit-root", "tick-receipt", "output-root", "live-dir"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--markets-dir", type=Path, default=ROOT / "services/discord_bot/markets")
    args = parser.parse_args()
    result = build(anchor_receipt=args.anchor_receipt.resolve(strict=True), scope_root=args.scope_root.resolve(strict=True),
        minute_root=args.minute_root.resolve(strict=True), price_limit_root=args.price_limit_root.resolve(strict=True),
        tick_receipt=args.tick_receipt.resolve(strict=True), output_root=args.output_root,
        live_dir=args.live_dir.resolve(strict=True), markets_dir=args.markets_dir.resolve(strict=True))
    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()

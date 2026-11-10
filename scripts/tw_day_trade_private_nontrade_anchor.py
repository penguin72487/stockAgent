#!/usr/bin/env python3
"""Carry the authorized 10/30 Unit MGDA nontrade settlement into a private chain.

The existing flat-anchor audit intentionally rejects nontrade positions. This
adapter checks the separately pinned non-execution audit and supplies only its
verified accounting reduction to that audit, in this process. It cannot apply
a ledger, create a fill, or change the live executor.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import date
import json
import math
from pathlib import Path
import shutil
import sys
import tempfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import audit_tw_day_trade_flat_snapshot_anchor as snapshot
from scripts import audit_tw_day_trade_nontrade as nontrade_audit
from scripts import audit_tw_day_trade_private_candidate_chain as chain_audit
from scripts import build_tw_day_trade_private_1030_nontrade_candidate as authorized
from scripts import build_tw_day_trade_private_historical_candidate as builder
from scripts import repair_tw_day_trade_flat_opening as owner
from scripts import stage_tw_day_trade_private_chain_anchor as chain
from scripts.plan_tw_day_trade_historical_revisions import _write_new_plan
from stockagent.data_sync import workspace_volume


def _checked_position(position: dict, fills: list[dict], *, market: str, day: str,
                      expected_position: dict, expected_fills: list[dict]) -> dict:
    """Reduce one already independently reaudited, exact-source position."""
    details = position.get("nontrade_settlement") or {}
    reference = details.get("reference") or {}
    if (market != authorized.MARKET or day != str(authorized.DAY)
            or position.get("market") != market or position.get("session_date") != day
            or position.get("symbol") != authorized.SYMBOL or position != expected_position
            or fills != expected_fills or position.get("signed_shares") != 0
            or reference.get("reference_observed_at") != authorized.OBSERVED_AT
            or reference.get("reference_price") != authorized.OBSERVED_PRICE
            or reference.get("source_sha256") != authorized.TICK_SHA256
            or any(details.get(key) is not False for key in
                   ("simulated_order", "simulated_fill", "exchange_fill", "broker_fill"))):
        raise RuntimeError("nontrade anchor differs from its independently audited source")
    quantity = position.get("filled_shares")
    if type(quantity) is not int or quantity <= 0:
        raise RuntimeError("invalid nontrade anchor shares")
    fees = math.fsum(float(row["fee_and_tax_twd"]) for row in fills) + float(details["assumed_exit_fee_and_tax_twd"])
    pnl = float(position["realized_net_pnl_twd"])
    if not math.isfinite(fees) or not math.isfinite(pnl):
        raise RuntimeError("invalid nontrade anchor economics")
    return {"shares": quantity, "fill_count": len(fills), "net_pnl_twd": pnl, "fees_twd": fees}


@contextmanager
def checked_nontrade_anchor(receipt_path: Path):
    """Verify candidate, re-run its independent audit, then scope the adapter."""
    receipt_path = receipt_path.resolve(strict=True)
    receipt_sha = owner._sha256(receipt_path)
    receipt = owner._object(receipt_path)
    chain._check_private_receipt(receipt, allow_observed_zero_proof=True)
    expected_scope = {"session_date": str(authorized.DAY), "market": authorized.MARKET,
        "symbol": authorized.SYMBOL, "reference_observed_at": authorized.OBSERVED_AT,
        "reference_price": authorized.OBSERVED_PRICE, "source_sha256": authorized.TICK_SHA256}
    if (receipt.get("market") != authorized.MARKET or receipt.get("session_date") != str(authorized.DAY)
            or receipt.get("nontrade_authorization_scope") != expected_scope):
        raise RuntimeError("private nontrade candidate has the wrong authorization scope")
    mount = Path(workspace_volume.configuration()["mount_point"]).resolve(strict=True)
    candidate = Path(receipt["candidate_path"]).resolve(strict=True)
    if (receipt_path.name != "private_candidate_receipt.json" or candidate.parent != receipt_path.parent
            or not candidate.is_relative_to(mount / "build")):
        raise RuntimeError("candidate and receipt do not have the same owner")
    files = receipt.get("candidate_file_sha256") or {}
    if (set(files) != {path.name for path in candidate.iterdir() if path.is_file()}
            or not {"state.json", "marks.jsonl", "fills.jsonl", "events.jsonl", "signals.jsonl"} <= set(files)):
        raise RuntimeError("private candidate file set changed")
    for name, digest in files.items():
        if (candidate / name).is_symlink() or owner._sha256(candidate / name) != digest:
            raise RuntimeError(f"private candidate changed: {name}")
    implementation = receipt.get("implementation_sha256") or {}
    required_code = {str(Path(module.__file__).resolve()) for module in (authorized, builder, owner, nontrade_audit, snapshot)}
    if not required_code <= set(implementation):
        raise RuntimeError("private candidate lacks required implementation pins")
    for name, digest in implementation.items():
        if owner._sha256(Path(name)) != digest:
            raise RuntimeError("private candidate implementation changed")
    owner._assert_source_pins(receipt["source_pins"])
    audit_input = candidate.parent / "independent_audit_input"
    audit_manifest = audit_input / "ledger_repair_manifest.json"
    audit_result = audit_input / "nontrade_accounting_audit.json"
    independent = receipt["independent_nontrade_audit"]
    audit_manifest_data = owner._object(audit_manifest)
    if (audit_manifest_data.get("apply_prohibited") is not True
            or audit_manifest_data.get("purpose") != "private_independent_nontrade_audit_only"
            or Path(audit_manifest_data.get("candidate_dir") or "").resolve() != candidate
            or owner._sha256(audit_manifest) != independent.get("manifest_sha256")
            or owner._sha256(audit_result) != receipt.get("independent_nontrade_audit_sha256")
            or owner._object(audit_result) != independent
            or independent.get("settlement_positions") != 1
            or independent.get("settlement_shares") != 2000):
        raise RuntimeError("independent nontrade accounting receipt changed")
    # Re-run the exact auditor in a fresh private directory: do not replace the
    # original immutable audit receipt, even with byte-identical content.
    with tempfile.TemporaryDirectory(prefix=".nontrade-reaudit-", dir=candidate.parent) as temporary:
        copy = Path(temporary) / "ledger_repair_manifest.json"
        shutil.copyfile(audit_manifest, copy)
        if owner._sha256(copy) != independent["manifest_sha256"]:
            raise RuntimeError("independent audit input copy changed")
        if nontrade_audit.audit(Path(temporary)) != independent:
            raise RuntimeError("independent nontrade account re-audit disagrees")
    state = owner._object(candidate / "state.json")
    mode = state.get("modes", {}).get(authorized.MARKET) or {}
    positions = {identity: position for identity, position in (mode.get("positions") or {}).items()
                 if position.get("nontrade_settlement")}
    if len(positions) != 1:
        raise RuntimeError("unexpected nontrade position set")
    identity, expected_position = next(iter(positions.items()))
    expected_fills = [row for row in authorized._rows(candidate / "fills.jsonl") if row.get("position_id") == identity]
    original = snapshot.check_position

    def checked(position, fills, *, market, day):
        if not position.get("nontrade_settlement"):
            return original(position, fills, market=market, day=day)
        return _checked_position(position, fills, market=market, day=day,
                                 expected_position=expected_position, expected_fills=expected_fills)

    adapter = Path(__file__).resolve()
    adapter_sha = owner._sha256(adapter)
    proof = {"contract": "private_scoped_nontrade_anchor_audit_v1",
             "candidate_receipt": str(receipt_path), "candidate_receipt_sha256": receipt_sha,
             "independent_audit_path": str(audit_result), "independent_audit_sha256": owner._sha256(audit_result),
             "independent_manifest_path": str(audit_manifest), "independent_manifest_sha256": owner._sha256(audit_manifest),
             "adapter_path": str(adapter), "adapter_sha256": adapter_sha,
             "scope": expected_scope, "simulated_fill": False, "exchange_fill": False,
             "broker_fill": False, "publication_authorized": False}
    with patch.object(snapshot, "check_position", checked):
        yield proof
    if (owner._sha256(receipt_path) != receipt_sha or owner._sha256(adapter) != adapter_sha
            or owner._sha256(audit_result) != proof["independent_audit_sha256"]):
        raise RuntimeError("nontrade audit source changed during private chain operation")
    if any(owner._sha256(Path(name)) != digest for name, digest in implementation.items()):
        raise RuntimeError("nontrade candidate implementation changed during chain operation")
    owner._assert_source_pins(receipt["source_pins"])


def _enrich_anchor(path: Path, payload: dict, *, proof: dict) -> None:
    payload["private_nontrade_anchor_audit"] = proof
    for key, name in (("nontrade_anchor_adapter", "adapter"), ("nontrade_accounting_audit", "independent_audit"),
                      ("nontrade_accounting_manifest", "independent_manifest")):
        payload["source_pins"][key] = {"path": proof[name + "_path"], "sha256": proof[name + "_sha256"]}
    _write_new_plan(path, payload)


def stage(*, candidate_receipt: Path, output_root: Path) -> dict:
    pending = []
    with checked_nontrade_anchor(candidate_receipt) as proof:
        with patch.object(chain, "_write_new_plan", lambda path, payload: pending.append((path, payload))):
            result = chain.stage(candidate_receipt=candidate_receipt.resolve(strict=True), target_day=date(2026, 11, 2),
                                 output_root=output_root, allow_unverified_private_gaps=True)
    if len(pending) != 1:
        raise RuntimeError("exactly one private anchor receipt was expected")
    _enrich_anchor(*pending[0], proof=proof)
    return result


def _enrich_candidate(path: Path, payload: dict, *, proof: dict) -> None:
    payload["private_nontrade_anchor_audit"] = proof
    payload["implementation_sha256"][proof["adapter_path"]] = proof["adapter_sha256"]
    _write_new_plan(path, payload)


def build_next(*, candidate_receipt: Path, anchor_receipt: Path, scope_root: Path, minute_root: Path,
               price_limit_root: Path, output_root: Path, markets_dir: Path) -> dict:
    anchor_receipt = anchor_receipt.resolve(strict=True)
    anchor = owner._object(anchor_receipt)
    if (anchor.get("private_candidate_receipt") != str(candidate_receipt.resolve(strict=True))
            or anchor.get("anchor_session") != str(authorized.DAY) or anchor.get("target_session") != "2026-11-02"):
        raise RuntimeError("next-day anchor is not linked to the authorized candidate")
    pending = []
    with checked_nontrade_anchor(candidate_receipt) as proof:
        if anchor.get("private_nontrade_anchor_audit") != proof:
            raise RuntimeError("next-day anchor adapter proof differs")
        live_dir = Path(anchor["source_pins"]["events"]["path"]).parent
        with patch.object(builder, "_write_new_plan", lambda path, payload: pending.append((path, payload))):
            result = builder.build(day=date(2026, 11, 2), market=authorized.MARKET, anchor_receipt=anchor_receipt,
                scope_root=scope_root.resolve(strict=True), minute_root=minute_root.resolve(strict=True),
                price_limit_root=price_limit_root.resolve(strict=True), output_root=output_root,
                live_dir=live_dir, markets_dir=markets_dir.resolve(strict=True))
    if len(pending) != 1:
        raise RuntimeError("exactly one private candidate receipt was expected")
    _enrich_candidate(*pending[0], proof=proof)
    return result


def audit(*, candidate_receipt: Path, latest_receipt: Path, output: Path, markets_dir: Path) -> dict:
    workspace = workspace_volume.configuration()
    mount = Path(workspace["mount_point"]).resolve(strict=True)
    target = output.resolve()
    if not target.is_relative_to(mount / "verify") or target.exists():
        raise ValueError("a new E-workspace verification receipt is required")
    with checked_nontrade_anchor(candidate_receipt) as proof:
        result = chain_audit.audit(latest_receipt=latest_receipt.resolve(strict=True), market=authorized.MARKET,
            expected_day=date(2026, 11, 4), build_root=mount / "build", markets_dir=markets_dir.resolve(strict=True),
            allow_unverified_private_gaps=True)
        result["private_nontrade_anchor_audit"] = proof
    _write_new_plan(target, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("stage", "build-next", "audit"))
    parser.add_argument("--candidate-receipt", type=Path, required=True)
    parser.add_argument("--anchor-receipt", type=Path)
    parser.add_argument("--scope-root", type=Path)
    parser.add_argument("--minute-root", type=Path)
    parser.add_argument("--price-limit-root", type=Path)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--latest-receipt", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--markets-dir", type=Path, default=ROOT / "services/discord_bot/markets")
    args = parser.parse_args()
    if args.action == "stage":
        if args.output_root is None:
            parser.error("stage requires --output-root")
        result = stage(candidate_receipt=args.candidate_receipt, output_root=args.output_root)
    elif args.action == "build-next":
        if any(value is None for value in (args.anchor_receipt, args.scope_root, args.minute_root,
                                           args.price_limit_root, args.output_root)):
            parser.error("build-next requires anchor, scope, minute, price-limit and output roots")
        result = build_next(candidate_receipt=args.candidate_receipt, anchor_receipt=args.anchor_receipt,
            scope_root=args.scope_root, minute_root=args.minute_root, price_limit_root=args.price_limit_root,
            output_root=args.output_root, markets_dir=args.markets_dir)
    else:
        if args.latest_receipt is None or args.output is None:
            parser.error("audit requires --latest-receipt and --output")
        result = audit(candidate_receipt=args.candidate_receipt, latest_receipt=args.latest_receipt,
                       output=args.output, markets_dir=args.markets_dir)
    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()

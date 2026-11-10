#!/usr/bin/env python3
"""Recheck an entire private paper-replay chain; never publish or modify it.

Every candidate remains a counterfactual paper replay.  This check is an
integrity/accounting component, not an authorization to replace a ledger.
"""
from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import build_tw_day_trade_private_historical_candidate as builder
from scripts import repair_tw_day_trade_flat_opening as owner
from scripts import stage_tw_day_trade_private_chain_anchor as chain
from scripts.plan_tw_day_trade_historical_revisions import _write_new_plan
from scripts.verify_tw_day_trade_scoped_repair import verify_minute_source_coverage
from stockagent.data_sync import workspace_volume


class _Pins:
    """Hash each exact file once, and fail if its filesystem generation changes."""

    def __init__(self) -> None:
        self.entries: dict[str, tuple[str, tuple]] = {}

    @staticmethod
    def _generation(path: Path) -> tuple:
        target = path.resolve(strict=True)
        if not target.is_file():
            raise ValueError(f"not a regular pinned file: {path}")
        st = target.stat()
        return (str(target), st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns)

    def check(self, path: Path, expected: str) -> None:
        if not isinstance(expected, str) or len(expected) != 64:
            raise ValueError("a SHA-256 pin is required")
        key = str(path.absolute())
        generation = self._generation(path)
        previous = self.entries.get(key)
        if previous:
            if previous != (expected, generation):
                raise RuntimeError(f"conflicting or changed source pin: {path}")
            return
        if owner._sha256(path) != expected or self._generation(path) != generation:
            raise RuntimeError(f"source pin changed: {path}")
        self.entries[key] = (expected, generation)

    def unchanged(self) -> None:
        for name, (_, before) in self.entries.items():
            if self._generation(Path(name)) != before:
                raise RuntimeError(f"source changed during chain audit: {name}")


def _candidate(receipt_path: Path, *, market: str, expected_day: date, build_root: Path,
               spec, pins: _Pins, provisional: bool = False) -> tuple[dict, Path, dict, dict, list[dict], dict]:
    if (receipt_path.name != "private_candidate_receipt.json"
            or not receipt_path.is_relative_to(build_root)):
        raise ValueError("candidate receipt must be inside the isolated build workspace")
    pins.check(receipt_path, owner._sha256(receipt_path))
    receipt = owner._object(receipt_path)
    chain._check_private_receipt(receipt, allow_observed_zero_proof=True)
    if (receipt.get("market") != market or receipt.get("session_date") != str(expected_day)
            or receipt.get("first_entry_is_counterfactual_09_01") is not True
            or receipt.get("missing_minutes_are_unknown_not_zero") is not True):
        raise ValueError("candidate identity or paper-replay semantics differ")
    candidate = Path(receipt["candidate_path"]).resolve(strict=True)
    if candidate.parent != receipt_path.parent or not candidate.is_relative_to(build_root):
        raise ValueError("candidate is not owned by its private receipt")
    expected = receipt.get("candidate_file_sha256")
    names = {path.name for path in candidate.iterdir() if path.is_file()}
    if (not isinstance(expected, dict) or names != set(expected)
            or not {"state.json", "marks.jsonl", "fills.jsonl", "events.jsonl"} <= names):
        raise RuntimeError("private candidate file set changed")
    for name, digest in expected.items():
        path = candidate / name
        if path.is_symlink():
            raise RuntimeError("candidate files cannot be symlinks")
        pins.check(path, digest)
    implementation = receipt.get("implementation_sha256")
    if not isinstance(implementation, dict) or not implementation:
        raise ValueError("candidate implementation pins are missing")
    for name, digest in implementation.items():
        pins.check(Path(name), digest)
    sources = receipt.get("source_pins")
    if not isinstance(sources, list) or not sources:
        raise ValueError("candidate source pins are missing")
    for item in sources:
        pins.check(Path(item["path"]), item["sha256"])
    plan = owner._published_revision_inventory(spec, expected_day)
    if (plan.get("plan_sha256") != receipt.get("original_plan_sha256")
            or plan.get("selected_revision_count") != receipt.get("selected_revision_count")):
        raise RuntimeError("original published signal inventory changed")
    decisions = _audit_published_decisions(receipt, candidate=candidate, market=market,
                                           day=expected_day, plan=plan, pins=pins)
    state = owner._object(candidate / "state.json")
    mode = state.get("modes", {}).get(market)
    if (not isinstance(mode, dict) or mode.get("session_date") != str(expected_day)
            or type(mode.get("open_position_count")) is not int or mode["open_position_count"] != 0):
        raise RuntimeError("candidate account is not flat")
    marks = chain._jsonl(candidate / "marks.jsonl")
    if provisional:
        if not receipt["minute_source_coverage"]["unknown_carried_minute_symbols"]:
            raise RuntimeError("provisional audit requires actually disclosed unknown minutes")
        coverage = chain._provisional_coverage(receipt, day=expected_day, mode=mode, marks=marks)
        extra_pins = []
    else:
        no_trade_sources, extra_pins = chain._verified_zero_trade_sources(receipt, day=expected_day)
        for item in extra_pins:
            pins.check(Path(item["path"]), item["sha256"])
        coverage = verify_minute_source_coverage(mode, marks, market=market, day=str(expected_day),
            source_files=no_trade_sources)
    return receipt, candidate, mode, coverage, extra_pins, decisions


def _audit_published_decisions(receipt: dict, *, candidate: Path, market: str, day: date,
                               plan: dict, pins: _Pins) -> dict:
    sessions = receipt.get("session")
    if not isinstance(sessions, list) or len(sessions) != 1 or sessions[0].get("session_date") != str(day):
        raise RuntimeError("one exact replay session is required")
    signal = str(sessions[0].get("signal_id") or "")
    marker = ":published-minute-repair:"
    if signal.count(marker) != 1:
        raise RuntimeError("invalid paper-replay signal identity")
    token = signal.rsplit(marker, 1)[-1]
    if re.fullmatch(r"[0-9a-f]{12}", token) is None:
        raise RuntimeError("invalid paper-replay token")
    auditor = Path(chain.provisional_audit.__file__).resolve()
    pins.check(auditor, owner._sha256(auditor))
    manifest = {"market": market, "contract": owner.PUBLISHED_SESSION_CONTRACT,
        "published_revision_plan": plan, "sessions": [{"session_date": str(day), "repair_token": token}]}
    return chain.provisional_audit.audit_published_decisions(candidate, manifest)


def _source_pins_retained(old: list[dict], current: list[dict]) -> bool:
    """An added independently rechecked source strengthens, not invalidates, proof."""
    if not isinstance(old, list) or not isinstance(current, list) or bool(old) != bool(current):
        return False
    def pairs(items):
        return {(item["path"], item["sha256"]) for item in items}
    try:
        return pairs(old) <= pairs(current)
    except (KeyError, TypeError):
        return False


def _check_projection(anchor: dict, *, prior_receipt_path: Path, prior_receipt: dict,
                      prior_candidate: Path, prior_mode: dict, coverage: dict,
                      extra_pins: list[dict]) -> None:
    """Connect the audited staged projection to the exact prior candidate."""
    unknown = bool(coverage.get("unknown_carried_minute_symbols"))
    expected_coverage = anchor.get("provisional_minute_coverage" if unknown else "strict_minute_coverage")
    if (anchor.get("private_chain_projection") is not True
            or anchor.get("private_candidate_receipt") != str(prior_receipt_path)
            or anchor.get("private_candidate_receipt_sha256") != owner._sha256(prior_receipt_path)
            or expected_coverage != coverage
            or not _source_pins_retained(anchor.get("observed_zero_trade_source_pins") or [], extra_pins)):
        raise RuntimeError("private anchor does not match its previous candidate")
    sources = anchor["source_pins"]
    snapshot = owner._object(Path(sources["snapshot"]["path"]))
    marker = snapshot.get("private_chain_projection") or {}
    if (snapshot.get("modes", {}).get(prior_receipt["market"]) != prior_mode
            or marker.get("candidate_receipt") != str(prior_receipt_path)
            or marker.get("candidate_receipt_sha256") != anchor["private_candidate_receipt_sha256"]
            or (marker.get("inherited_unverified_minute_gaps") or []) !=
               (anchor.get("inherited_unverified_minute_gaps") or [])
            or marker.get("publication_authorized") is not False):
        raise RuntimeError("private account projection does not match candidate state")
    for name in ("events", "fills"):
        if sources[name]["sha256"] != prior_receipt["candidate_file_sha256"][name + ".jsonl"]:
            raise RuntimeError(f"private {name} projection differs from candidate")
    staged_marks = chain._jsonl(Path(sources["marks"]["path"]))
    original_marks = chain._jsonl(prior_candidate / "marks.jsonl")
    if len(staged_marks) != len(original_marks) + 1 or staged_marks[1:] != original_marks:
        raise RuntimeError("private minute projection differs from candidate")


def _verify_inherited_gaps(sessions: list[dict], inherited_by_day: dict[str, list]) -> list[dict]:
    """Recompute each forward-propagated disclosure from actual prior receipts."""
    expected_gaps = []
    for item in reversed(sessions):
        if item["unknown_carried_minute_symbols"]:
            expected_gaps.append({"session_date": item["session_date"],
                "candidate_receipt": item["candidate_receipt"],
                "candidate_receipt_sha256": item["candidate_receipt_sha256"],
                "unknown_carried_minute_symbols": item["unknown_carried_minute_symbols"],
                "missing_is_not_zero_and_fills_are_not_invented": True})
        if item["session_date"] in inherited_by_day and inherited_by_day[item["session_date"]] != expected_gaps:
            raise RuntimeError("inherited private source gaps were lost or changed")
    return expected_gaps


def audit(*, latest_receipt: Path, market: str, expected_day: date, build_root: Path,
          markets_dir: Path, allow_unverified_private_gaps: bool = False) -> dict:
    if type(allow_unverified_private_gaps) is not bool:
        raise ValueError("private diagnostic option must be an explicit boolean")
    build_root = build_root.resolve(strict=True)
    current = latest_receipt.resolve(strict=True)
    spec, _ = owner._spec(market, markets_dir, True)
    pins, seen, sessions, inherited_by_day = _Pins(), set(), [], {}
    # The chain verifier calls the staging and provisional source auditors;
    # pin their actual implementations before any evidence is consumed.
    implementation_files = (Path(__file__).resolve(), Path(chain.__file__).resolve(),
                            Path(chain.provisional_audit.__file__).resolve())
    implementation_hashes = {str(path): owner._sha256(path) for path in implementation_files}
    for path in implementation_files:
        pins.check(path, implementation_hashes[str(path)])
    pending_anchor = None
    day = expected_day
    while True:
        if current in seen or len(seen) >= 366:
            raise RuntimeError("cyclic or excessive private candidate chain")
        seen.add(current)
        candidate_preview = owner._object(current)
        has_unknown = bool((candidate_preview.get("minute_source_coverage") or {})
                           .get("unknown_carried_minute_symbols"))
        provisional = bool(allow_unverified_private_gaps and has_unknown and
            (pending_anchor is None or pending_anchor.get("provisional_minute_coverage") is not None))
        receipt, candidate, mode, coverage, extra_pins, decisions = _candidate(
            current, market=market, expected_day=day, build_root=build_root, spec=spec, pins=pins,
            provisional=provisional)
        if pending_anchor is not None:
            _check_projection(pending_anchor, prior_receipt_path=current, prior_receipt=receipt,
                              prior_candidate=candidate, prior_mode=mode, coverage=coverage, extra_pins=extra_pins)
            inherited_by_day[str(day)] = pending_anchor.get("inherited_unverified_minute_gaps") or []
        sessions.append({"session_date": str(day), "candidate_receipt": str(current),
            "candidate_receipt_sha256": owner._sha256(current),
            "selected_revision_count": receipt["selected_revision_count"],
            "minute_points": coverage["minute_points"],
            "observed_zero_trade_carried_minutes": len(coverage["explicit_zero_trade_carried_minutes"]),
            "unknown_carried_minute_symbols": coverage.get("unknown_carried_minute_symbols") or {},
            "fill_accounting": receipt["fill_accounting"], "published_decisions": decisions})
        anchor_path = Path(receipt["anchor_receipt"]).resolve(strict=True)
        pins.check(anchor_path, receipt["anchor_receipt_sha256"])
        anchor = owner._object(anchor_path)
        if anchor.get("inherited_unverified_minute_gaps") and not allow_unverified_private_gaps:
            raise RuntimeError("upstream unknown minutes require explicit private diagnostic mode")
        source_pins = anchor.get("source_pins") or {}
        for item in source_pins.values():
            pins.check(Path(item["path"]), item["sha256"])
        for item in anchor.get("observed_zero_trade_source_pins") or []:
            pins.check(Path(item["path"]), item["sha256"])
        live_dir = Path(source_pins["events"]["path"]).parent
        audited_mode, _ = builder._audited_mode(anchor_path, market=market, day=day, live_dir=live_dir)
        if audited_mode.get("session_date") != anchor.get("anchor_session"):
            raise RuntimeError("audited anchor session differs")
        if not anchor.get("private_chain_projection"):
            origin = {"anchor_receipt": str(anchor_path),
                      "anchor_receipt_sha256": receipt["anchor_receipt_sha256"],
                      "anchor_session": anchor["anchor_session"]}
            break
        pending_anchor = anchor
        day = date.fromisoformat(anchor["anchor_session"])
        current = Path(anchor["private_candidate_receipt"]).resolve(strict=True)
    expected_gaps = _verify_inherited_gaps(sessions, inherited_by_day)
    pins.unchanged()
    return {"contract": "private_paper_candidate_chain_integrity_v1",
        "status": ("private_chain_verified_with_unknown_source_gaps_not_applied" if expected_gaps
                   else "private_chain_integrity_verified_not_applied"), "market": market,
        "anchor": origin, "sessions": list(reversed(sessions)), "pinned_files": len(pins.entries),
        "inherited_unverified_session_count": len(expected_gaps),
        "simulation_only": True, "broker_orders_created": 0, "live_ledger_modified": False,
        "publication_authorized": False, "all_modes_validated": False,
        "replay_prices_are_not_broker_fills": True,
        "audit_implementation_sha256": implementation_hashes[str(Path(__file__).resolve())],
        "audit_dependency_sha256": implementation_hashes}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--latest-receipt", type=Path, required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--day", type=date.fromisoformat, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--allow-unverified-private-gaps", action="store_true",
                        help="verify disclosed gaps as unknown in an unpublished diagnostic chain")
    parser.add_argument("--markets-dir", type=Path, default=ROOT / "services/discord_bot/markets")
    args = parser.parse_args()
    workspace = workspace_volume.configuration()
    mount = Path(workspace["mount_point"]).resolve(strict=True)
    output = args.output.resolve()
    if not output.is_relative_to(mount / "verify") or output.exists():
        raise ValueError("a new E-workspace verification receipt is required")
    result = audit(latest_receipt=args.latest_receipt, market=args.market, expected_day=args.day,
                   build_root=mount / "build", markets_dir=args.markets_dir.resolve(strict=True),
                   allow_unverified_private_gaps=args.allow_unverified_private_gaps)
    workspace_volume.admit(required_bytes=16 * 1024**2, c=workspace)
    _write_new_plan(output, result)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()

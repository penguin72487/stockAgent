#!/usr/bin/env python3
"""Build, but never promote, a recoverable batch of scoped paper repairs.

The scoped repair owner builds and independently audits each account first.
This utility composes those approved journals into a same-filesystem private
copy while the live writer continues running.  It is deliberately incapable
of restarting a service or exchanging directories.  A separate promotion must
recheck live generations, annual curves, flatness and all source pins.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from copy import deepcopy
from datetime import date, datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import repair_tw_day_trade_flat_opening as repair
from scripts.add_tw_day_trade_paper_account import _prefix_hash, _sync_tree
from stockagent.data_sync.cache_capacity import _admit_backing_volume, _control_directory
from stockagent.data_sync.completed_forward_capacity import reserve_capacity
from stockagent.live.paper_journal import journal_write_transaction, open_paper_journal, rebind_cloned_journals
from stockagent.live.paper_journal import managed as managed_journal
from stockagent.live.paper_mode_snapshot import read_paper_state
from stockagent.live.tw_day_trade_service_sync import PaperReadChanged, committed_paper_read_guard
from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine


JOURNALS = ("signals.jsonl", "orders.jsonl", "fills.jsonl", "marks.jsonl", "events.jsonl")
PROJECTIONS = {"state.json", "status.json", "positions.json", "service_sync.json",
               "service_sync_durable_ack.json", "preopen_readiness.json",
               ".engine.lock", ".minute_curve_write.lock"}
VOLATILE_DIRS = {"quote_broker"}
COPY_CHUNK = 4 * 1024**2
CHECK_INTERVAL = 2 * 1024**3


def _generation(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _ignored(relative: Path) -> bool:
    if relative.parts[0] in VOLATILE_DIRS:
        return True
    if len(relative.parts) == 1:
        return relative.name in PROJECTIONS or ".json.tmp." in relative.name
    return False


def _inventory(root: Path) -> dict[str, list[int]]:
    """Inventory exactly the copied source; never follow a redirected member."""
    result = {}
    for directory, dirs, files in os.walk(root, followlinks=False):
        parent = Path(directory)
        dirs[:] = [name for name in dirs if not _ignored((parent / name).relative_to(root))]
        for name in [*dirs, *files]:
            path = parent / name
            relative = path.relative_to(root)
            if _ignored(relative):
                continue
            if path.is_symlink():
                raise RuntimeError(f"ledger clone refuses symlink: {relative}")
            if path.is_file():
                result[str(relative)] = list(_generation(path.stat()))
            elif not path.is_dir():
                raise RuntimeError(f"ledger clone refuses non-regular member: {relative}")
    return result


def _read_committed_state(root: Path) -> dict:
    for attempt in range(30):
        try:
            with committed_paper_read_guard(root):
                return read_paper_state(root / "state.json", use_cache=False)
        except PaperReadChanged:
            if attempt == 29:
                raise
            time.sleep(0.1)
    raise AssertionError("unreachable")


def _require_flat(state: dict) -> None:
    if state.get("production_order_possible") is not False:
        raise RuntimeError("source is not explicitly paper-only")
    modes = state.get("modes")
    if not isinstance(modes, dict) or not modes:
        raise RuntimeError("source modes are missing")
    for market, mode in modes.items():
        positions = mode.get("positions") if isinstance(mode, dict) else None
        if (not isinstance(positions, dict) or mode.get("ledger_state_divergence")
                or mode.get("entry_retry_pending_commit") or mode.get("research_terminal_pending_commit")
                or any(not isinstance(p, dict) or type(p.get("signed_shares")) is not int
                       or p["signed_shares"] != 0 for p in positions.values())):
            raise RuntimeError(f"source account is nonflat or has an unresolved commit: {market}")


def _pinned_candidate(operation: Path, source_audit: Path, accounting_audit: Path,
                      live: Path, markets_dir: Path) -> dict:
    manifest_path = operation / "ledger_repair_manifest.json"
    manifest = repair._object(manifest_path)
    source_proof, accounting = repair._object(source_audit), repair._object(accounting_audit)
    manifest_sha = repair._sha256(manifest_path)
    if (manifest.get("status") != "candidate_validated_not_applied"
            or manifest.get("published_revisions") is not True
            or manifest.get("provisional_minute_source_coverage") is not True
            or manifest.get("simulation_only") is not True
            or manifest.get("production_order_possible") is not False
            or manifest.get("live_dir") != str(live)
            or len(manifest.get("sessions") or []) != 1
            or source_proof.get("status") != "verified_provisional"
            or accounting.get("status") != "verified_with_data_gaps"
            or source_proof.get("manifest_sha256") != manifest_sha
            or accounting.get("manifest_sha256") != manifest_sha):
        raise RuntimeError("repair candidate or independent receipts have the wrong identity")
    market, day = manifest["market"], manifest["sessions"][0]["session_date"]
    if any(proof.get("market") != market or proof.get("session_date") != day
           for proof in (source_proof, accounting)):
        raise RuntimeError("independent receipts belong to a different account/session")
    if (source_proof.get("auditor_sha256") != repair._sha256(ROOT / "scripts/audit_tw_day_trade_provisional_source.py")
            or accounting.get("auditor_sha256") != repair._sha256(ROOT / "scripts/audit_tw_day_trade_nontrade.py")):
        raise RuntimeError("independent audit implementation changed after the receipts")
    if (accounting.get("simulation_only") is not True
            or any(accounting.get(key) is not False for key in
                   ("simulated_order", "simulated_fill", "exchange_fill", "broker_fill"))):
        raise RuntimeError("nontrade settlement receipt misstates execution")
    decisions = source_proof.get("published_decision_audit") or {}
    if (decisions.get("original_targets_and_clocks_verified") is not True
            or decisions.get("published_decisions") != sum(bool(v.get("effective_entry_at"))
                for v in manifest["published_revision_plan"]["versions"])):
        raise RuntimeError("all effective published decisions must be independently verified")
    candidate = Path(manifest["candidate_dir"]).resolve(strict=True)
    if candidate == live or candidate.stat().st_dev != live.stat().st_dev:
        raise RuntimeError("candidate must be a distinct same-filesystem directory")
    expected = {
        "state.json": source_proof["candidate_files_sha256"]["state.json"],
        "marks.jsonl": source_proof["candidate_files_sha256"]["marks.jsonl"],
        "fills.jsonl": source_proof["candidate_files_sha256"]["fills.jsonl"],
        "signals.jsonl": decisions["candidate_signals_sha256"],
        "orders.jsonl": decisions["candidate_orders_sha256"],
        "events.jsonl": accounting["candidate_events_sha256"],
    }
    if (expected["state.json"] != accounting.get("candidate_state_sha256")
            or expected["fills.jsonl"] != accounting.get("candidate_fills_sha256")
            or expected["fills.jsonl"] != decisions.get("candidate_fills_sha256")):
        raise RuntimeError("independent candidate hashes disagree")
    for name, digest in expected.items():
        if repair._sha256(candidate / name) != digest:
            raise RuntimeError(f"candidate changed after independent audit: {market}/{name}")
    repair._assert_source_pins(manifest["source_pins"])
    spec, _ = repair._spec(market, markets_dir, True)
    if repair._spec_digest(spec) != manifest["spec_sha256"]:
        raise RuntimeError("selected account configuration changed")
    incoming = read_paper_state(candidate / "state.json", use_cache=False)
    mode = (incoming.get("modes") or {}).get(market)
    if (set(incoming.get("modes") or {}) != {market} or not isinstance(mode, dict)
            or repair._outcome(mode) != manifest["candidate_final_outcome"]
            or mode.get("execution_evidence_complete") is not False):
        raise RuntimeError("candidate economic state or execution disclosure differs")
    _require_flat({"production_order_possible": False, "modes": {market: mode}})
    return {"manifest": manifest, "manifest_sha256": manifest_sha, "candidate": candidate,
            "manifest_path": str(manifest_path.resolve()), "candidate_directory": str(candidate),
            "mode": mode, "expected_hashes": expected,
            "source_audit": str(source_audit.resolve()), "source_audit_sha256": repair._sha256(source_audit),
            "accounting_audit": str(accounting_audit.resolve()),
            "accounting_audit_sha256": repair._sha256(accounting_audit)}


def _append_pinned(source: Path, destination: Path, digest: str, capacity_check) -> int:
    """Append exact already-audited bytes only to the private candidate."""
    observed = hashlib.sha256()
    total = 0
    if destination.stat().st_size:
        with open_paper_journal(destination) as existing:
            existing.seek(-1, os.SEEK_END)
            if existing.read(1) != b"\n":
                raise RuntimeError(f"source ledger has torn final row: {destination.name}")
    with journal_write_transaction(destination, operation="append"), destination.open("ab") as target:
        with open_paper_journal(source) as stream:
            while block := stream.read(COPY_CHUNK):
                target.write(block)
                observed.update(block)
                total += len(block)
                capacity_check(len(block))
        target.flush()
        os.fsync(target.fileno())
    if observed.hexdigest() != digest:
        raise RuntimeError(f"candidate journal changed during append: {source}")
    return total


def compose(live: Path, merged: Path, candidates: list[dict], capacity_check) -> dict:
    """Construct a private successor; caller owns capacity and target lifetime."""
    live, merged = live.resolve(strict=True), merged.resolve(strict=True)
    if (live == merged or live.parent != merged.parent or live.stat().st_dev != merged.stat().st_dev
            or any(merged.iterdir())):
        raise RuntimeError("an empty same-parent private candidate is required")
    markets = [item["manifest"]["market"] for item in candidates]
    if not markets or len(set(markets)) != len(markets):
        raise RuntimeError("repair market list is empty or duplicated")
    inventory = _inventory(live)
    copied = {}

    def copy_file(src, dst):
        source, target = Path(src), Path(dst)
        relative = str(source.relative_to(live))
        before = _generation(source.stat())
        if list(before) != inventory[relative]:
            raise RuntimeError(f"live source changed before copy: {relative}")
        digest = hashlib.sha256()
        with source.open("rb") as reader, target.open("xb") as writer:
            while block := reader.read(COPY_CHUNK):
                writer.write(block)
                digest.update(block)
                capacity_check(len(block))
            writer.flush()
            os.fsync(writer.fileno())
        shutil.copystat(source, target, follow_symlinks=False)
        if _generation(source.stat()) != before or repair._sha256(target) != digest.hexdigest():
            raise RuntimeError(f"live source changed or clone readback differs: {relative}")
        copied[relative] = {"bytes": before[2], "sha256": digest.hexdigest()}
        return str(target)

    shutil.copytree(live, merged, dirs_exist_ok=True, copy_function=copy_file,
                    ignore=lambda directory, names: {name for name in names
                        if _ignored((Path(directory) / name).relative_to(live))})
    if _inventory(live) != inventory or set(copied) != set(inventory):
        raise RuntimeError("live ledger changed during private clone")
    clone_proof = rebind_cloned_journals(live, merged)
    state = _read_committed_state(live)
    _require_flat(state)
    original = deepcopy(state)
    for item in candidates:
        manifest, market = item["manifest"], item["manifest"]["market"]
        day = date.fromisoformat(manifest["sessions"][0]["session_date"])
        mode = state["modes"].get(market)
        if not isinstance(mode, dict) or repair._anchor(mode) != manifest["anchor"]:
            raise RuntimeError(f"source economic anchor changed: {market}")
        repair._require_missing_session_target(mode, day, events_path=live / "events.jsonl")
        if repair._has_live_fill(live, market, str(day)):
            raise RuntimeError(f"source gained a fill for the repair session: {market}")
        archive = merged / "position_history" / str(day) / f"{market}.json"
        if archive.exists():
            raise RuntimeError(f"source already has a position archive for repair: {market}")
    repair._atomic_json(merged / "state.json", state)
    # Construct against the original committed journals.  Constructing after
    # appending the repair suffix would present new commit events paired with
    # the old state and could trigger false interrupted-commit recovery.
    engine = TwDayTradeSimulationEngine(merged)
    engine.state = deepcopy(state)
    for item in candidates:
        for name in JOURNALS:
            _append_pinned(item["candidate"] / name, merged / name,
                           item["expected_hashes"][name], capacity_check)
    for item in candidates:
        market = item["manifest"]["market"]
        mode = deepcopy(item["mode"])
        mode["executed_positions_path"] = str(live / "positions.json")
        engine.state["modes"][market] = mode
        engine._archive_mode_positions(mode, archived_at=datetime.now(repair.TAIPEI))
    engine._persist(datetime.now(repair.TAIPEI))
    with committed_paper_read_guard(merged):
        after = read_paper_state(merged / "state.json", use_cache=False)
    for market, mode in original["modes"].items():
        if market not in markets and after["modes"][market] != mode:
            raise RuntimeError(f"unrelated account changed: {market}")
    for key in ("benchmarks", "minute_liquidity", "enabled_markets"):
        if after.get(key) != original.get(key):
            raise RuntimeError(f"shared observation changed: {key}")
    for item in candidates:
        market = item["manifest"]["market"]
        if repair._outcome(after["modes"][market]) != item["manifest"]["candidate_final_outcome"]:
            raise RuntimeError(f"composed account differs from validated outcome: {market}")
    for name in JOURNALS:
        proof = copied[name]
        if _prefix_hash(merged / name, proof["bytes"]) != proof["sha256"]:
            raise RuntimeError(f"original financial prefix changed: {name}")
    if _inventory(live) != inventory:
        raise RuntimeError("live ledger advanced while candidate was composed")
    return {"status": "candidate_built_not_promoted", "live_directory": str(live),
            "candidate_directory": str(merged), "markets": markets,
            "simulation_only": True, "production_order_possible": False,
            "source_inventory": inventory, "copied_source_proofs": copied,
            "immutable_clone_proof": clone_proof, "shared_observations_preserved": True,
            "source_repair_receipts": [{key: item[key] for key in
                ("manifest_path", "manifest_sha256", "candidate_directory", "source_audit", "source_audit_sha256",
                 "accounting_audit", "accounting_audit_sha256", "expected_hashes")}
                for item in candidates],
            "annual_minute_validation": "pending", "publication": "not_attempted"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-dir", type=Path, required=True)
    parser.add_argument("--markets-dir", type=Path, default=ROOT / "services/discord_bot/markets")
    parser.add_argument("--repair", nargs=3, action="append", required=True,
                        metavar=("OPERATION_ROOT", "SOURCE_AUDIT", "ACCOUNTING_AUDIT"))
    parser.add_argument("--output-receipt", type=Path, required=True)
    parser.add_argument("--authorize-zero-extra-reserve", action="store_true",
                        help="Use only for the explicitly authorized one-time repair; still reserve actual growth")
    args = parser.parse_args()
    if not args.authorize_zero_extra_reserve:
        parser.error("explicit one-time zero extra reserve authorization required")
    live = args.live_dir.resolve(strict=True)
    candidates = [_pinned_candidate(*(Path(value).resolve(strict=True) for value in entry),
                                    live, args.markets_dir.resolve(strict=True)) for entry in args.repair]
    total = sum(values[2] for values in _inventory(live).values())
    append_sizes = {name: sum((item["candidate"] / name).stat().st_size for item in candidates)
                    for name in JOURNALS}
    total += sum(append_sizes.values())
    # An enrolled journal also seals appended immutable chunks. Count their
    # maximum encoded size, not merely the visible raw JSONL growth.
    for name, size in append_sizes.items():
        if managed_journal(live / name):
            total += size + ((size + COPY_CHUNK - 1) // COPY_CHUNK) * 128 * 1024
    # Bounded state, archives, receipts and persistence temporaries are actual
    # transaction growth, not an added minimum-free policy for the host.
    total += 256 * 1024**2
    controls = _control_directory(f"filesystem-{live.stat().st_dev}")
    if controls.stat().st_dev != live.stat().st_dev:
        raise RuntimeError("capacity control and live ledger are on different filesystems")
    key = hashlib.sha256((str(live) + uuid.uuid4().hex).encode()).hexdigest()
    with ExitStack() as stack:
        reservation = stack.enter_context(reserve_capacity(controls, key=key, maximum_growth_bytes=total,
                                         capacity_bytes=2**63 - 1, minimum_free_bytes=0))
        extra = reservation["other_reserved_growth_bytes"] + reservation["control_bytes"]
        _admit_backing_volume(live, total + extra)
        candidate = Path(tempfile.mkdtemp(prefix="tw_day_trade_simulation.atomic-repair-", dir=live.parent))
        written, next_check = 0, CHECK_INTERVAL

        def capacity_check(count):
            nonlocal written, next_check
            written += count
            if written > total:
                raise RuntimeError("candidate exceeded reserved growth")
            if written >= next_check:
                backing = _admit_backing_volume(live, max(0, total - written) + extra)
                print(json.dumps({"phase": "copy_or_append", "written_bytes": written,
                                  "reserved_maximum_growth_bytes": total,
                                  "backing_state": backing.get("state")}), file=sys.stderr, flush=True)
                next_check = written + CHECK_INTERVAL

        try:
            print(json.dumps({"phase": "private_clone_started", "candidate": str(candidate),
                              "reserved_maximum_growth_bytes": total}), file=sys.stderr, flush=True)
            result = compose(live, candidate, candidates, capacity_check)
            print(json.dumps({"phase": "composition_verified", "candidate": str(candidate)}),
                  file=sys.stderr, flush=True)
            for item in candidates:
                if (repair._sha256(Path(item["manifest_path"])) != item["manifest_sha256"]
                        or repair._sha256(Path(item["source_audit"])) != item["source_audit_sha256"]
                        or repair._sha256(Path(item["accounting_audit"])) != item["accounting_audit_sha256"]):
                    raise RuntimeError("repair manifest or independent audit changed during composition")
                repair._assert_source_pins(item["manifest"]["source_pins"])
            result["capacity"] = {"reserved_maximum_growth_bytes": total,
                                  "bytes_written": written, "extra_minimum_free_bytes": 0}
            result["composer_sha256"] = repair._sha256(Path(__file__))
            repair._atomic_json(candidate / "scoped_repair_batch_candidate_receipt.json", result)
            _sync_tree(candidate)
            repair._atomic_json(args.output_receipt, result)
        except Exception as error:
            repair._atomic_json(args.output_receipt, {
                "status": "candidate_build_failed_not_promoted", "live_directory": str(live),
                "candidate_directory": str(candidate), "bytes_written": written,
                "reserved_maximum_growth_bytes": total, "error_type": type(error).__name__,
                "error": str(error), "live_ledger_replaced": False,
                "simulation_only": True, "production_order_possible": False,
            })
            raise
    print(json.dumps({key: result[key] for key in ("status", "candidate_directory", "markets",
                                                     "annual_minute_validation", "publication")}, ensure_ascii=False))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Recheck accepted paper-ledger prefixes in one scan per journal.

This is read-only with respect to the ledger. It does not promote, change a
trade, or claim that source pins and web endpoints have been revalidated.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import repair_tw_day_trade_flat_opening as repair
from scripts.prepare_tw_day_trade_scoped_repair_batch import _read_committed_state
from scripts.validate_tw_day_trade_repair_curves import validate_previously_accepted_mode
from stockagent.live.paper_journal import open_paper_journal


def multiple_prefix_hashes(path: Path, lengths: set[int]) -> dict[int, str]:
    """Hash each exact prefix without rereading the same 59-GiB journal."""
    if not lengths or any(type(value) is not int or value < 0 for value in lengths):
        raise ValueError("nonnegative exact prefix lengths are required")
    before = path.stat()
    if max(lengths) > before.st_size:
        raise RuntimeError(f"ledger shorter than an accepted prefix: {path}")
    digest, position, result = hashlib.sha256(), 0, {}
    with open_paper_journal(path) as stream:
        for limit in sorted(lengths):
            while position < limit:
                block = stream.read(min(8 * 1024**2, limit - position))
                if not block:
                    raise RuntimeError(f"ledger prefix was truncated: {path}")
                digest.update(block)
                position += len(block)
            result[limit] = digest.copy().hexdigest()
    after = path.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise RuntimeError(f"ledger changed during prefix validation: {path}")
    return result


def audit(live: Path, roots: list[Path], *, day: str) -> dict:
    live = live.resolve(strict=True)
    state = _read_committed_state(live)
    components, requested = [], {}
    for root in roots:
        root = root.resolve(strict=True)
        manifest_path = root / "ledger_repair_manifest.json"
        acceptance_path = root / "acceptance.json"
        baseline_path = root / "journal_prefix_baseline.json"
        manifest, acceptance, baseline = (repair._object(path) for path in
                                          (manifest_path, acceptance_path, baseline_path))
        market = manifest["market"]
        if baseline.get("market") != market or manifest.get("live_dir") != str(live):
            raise RuntimeError("accepted prefix belongs to another ledger or market")
        if set(baseline.get("journals") or {}) != {
                "signals.jsonl", "orders.jsonl", "fills.jsonl", "marks.jsonl", "events.jsonl"}:
            raise RuntimeError("accepted prefix inventory is incomplete")
        economic = validate_previously_accepted_mode(state["modes"][market], manifest, acceptance, day=day)
        pins = {str(path): repair._sha256(path) for path in (manifest_path, acceptance_path, baseline_path)}
        for name, proof in (baseline.get("journals") or {}).items():
            path = live / name
            current = path.stat()
            if (current.st_dev, current.st_ino) != (proof.get("device"), proof.get("inode")):
                raise RuntimeError(f"accepted journal identity changed: {market}/{name}")
            requested.setdefault(name, []).append({"market": market, **proof})
        components.append({"market": market, "economic": economic, "receipt_sha256": pins})
    if not components or len({row["market"] for row in components}) != len(components):
        raise RuntimeError("distinct previously accepted markets are required")
    evidence, generations = {}, {}
    for name, proofs in requested.items():
        current = (live / name).stat()
        generations[name] = (current.st_dev, current.st_ino, current.st_size,
                             current.st_mtime_ns, current.st_ctime_ns)
        values = multiple_prefix_hashes(live / name, {row["prefix_bytes"] for row in proofs})
        for row in proofs:
            if values[row["prefix_bytes"]] != row["sha256"]:
                raise RuntimeError(f"accepted original journal prefix differs: {row['market']}/{name}")
        evidence[name] = {"accepted_prefix_count": len(proofs), "distinct_lengths": len(values),
                          "max_bytes_read": max(values)}
    for component in components:
        for name, expected in component["receipt_sha256"].items():
            if repair._sha256(Path(name)) != expected:
                raise RuntimeError("accepted operation receipt changed during audit")
        component["economic"]["original_journal_prefix_recheck_required"] = False
    for name, expected in generations.items():
        current = (live / name).stat()
        if (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns,
                current.st_ctime_ns) != expected:
            raise RuntimeError(f"accepted journal changed after its prefix was checked: {name}")
    after = _read_committed_state(live)
    for root in roots:
        manifest, acceptance = (repair._object(root / name) for name in
                                ("ledger_repair_manifest.json", "acceptance.json"))
        validate_previously_accepted_mode(after["modes"][manifest["market"]], manifest, acceptance, day=day)
    return {"status": "accepted_prefixes_verified", "recorded_at": datetime.now(repair.TAIPEI).isoformat(),
            "live_directory": str(live), "session_date": day, "markets": components,
            "journals": evidence, "source_pin_recheck_required": True,
            "public_readback_recheck_required": True, "simulation_only": True,
            "production_order_possible": False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-dir", required=True, type=Path)
    parser.add_argument("--operation-root", action="append", required=True, type=Path)
    parser.add_argument("--date", required=True)
    parser.add_argument("--output-receipt", required=True, type=Path)
    args = parser.parse_args()
    if args.output_receipt.exists():
        raise FileExistsError("preserve the existing audit receipt")
    result = audit(args.live_dir, args.operation_root, day=args.date)
    repair._atomic_json(args.output_receipt, result)
    print(json.dumps({"status": result["status"], "markets": len(result["markets"]),
                      "journals": result["journals"], "source_pin_recheck_required": True}))


if __name__ == "__main__":
    main()

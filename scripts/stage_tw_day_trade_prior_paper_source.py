#!/usr/bin/env python3
"""Make a full-target replay's prior paper-fill source proof self-contained."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.live.tw_day_trade_simulation import REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL


SNAPSHOT_NAME = "prior_paper_source_records.jsonl"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: dict) -> None:
    temporary = path.with_name(f".{path.name}.tmp.{uuid.uuid4().hex}")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                         encoding="utf-8")
    os.replace(temporary, path)


def _line_hashes(path: Path, needed: set[str], *, exact: bool = False) -> set[str]:
    seen: set[str] = set()
    with path.open("rb") as handle:
        for line in handle:
            if not line.endswith(b"\n"):
                raise ValueError(f"incomplete paper-fill record in {path}")
            digest = hashlib.sha256(line).hexdigest()
            if exact and (digest not in needed or digest in seen):
                raise ValueError(f"extra or duplicate paper-fill snapshot record in {path}")
            if digest in needed:
                seen.add(digest)
    return seen


def verify(candidate: Path, receipt: dict, *, require_snapshot: bool = True) -> dict:
    """Verify selected immutable records; external original is checked while accessible."""
    candidate = candidate.resolve(strict=True)
    manifest_path = candidate / "prior_paper_fills.json"
    proof = receipt.get("prior_paper_fill_provenance") or {}
    if _sha256(manifest_path) != proof.get("candidate_manifest_sha256"):
        raise ValueError("prior paper-fill manifest missing or changed")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    fills = manifest.get("fills")
    if not isinstance(fills, dict):
        raise ValueError("prior paper-fill mapping invalid")
    needed = {str(digest) for row in fills.values()
              for digest in row.get("source_record_sha256") or []}
    snapshot_name = manifest.get("source_snapshot")
    if require_snapshot and snapshot_name != SNAPSHOT_NAME:
        raise ValueError("prior paper-fill immutable source snapshot not staged")
    if snapshot_name:
        if snapshot_name != SNAPSHOT_NAME:
            raise ValueError("invalid prior paper-fill snapshot name")
        snapshot = candidate / SNAPSHOT_NAME
        expected_sha = manifest.get("source_snapshot_sha256")
        if (_sha256(snapshot) != expected_sha
                or expected_sha != proof.get("candidate_source_snapshot_sha256")
                or manifest.get("source_snapshot_records") != len(needed)
                or proof.get("candidate_source_snapshot_records") != len(needed)
                or _line_hashes(snapshot, needed, exact=True) != needed):
            raise ValueError("prior paper-fill source snapshot invalid")
    source = Path(str(manifest.get("source") or ""))
    if source.is_file() and not source.resolve().is_relative_to(candidate):
        if _line_hashes(source, needed) != needed:
            raise ValueError("original prior paper-fill source records changed")
    elif not snapshot_name:
        raise ValueError("prior paper-fill source absent without immutable snapshot")
    return fills


def stage(candidate: Path) -> dict:
    candidate = candidate.resolve(strict=True)
    receipt_path = candidate / "rebuild_receipt.json"
    manifest_path = candidate / "prior_paper_fills.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if ((receipt.get("replay_contract") or {}).get("entry")
            != REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL):
        raise ValueError("candidate is not the historical full-target contract")
    proof = receipt.get("prior_paper_fill_provenance") or {}
    if _sha256(manifest_path) != proof.get("candidate_manifest_sha256"):
        raise ValueError("candidate prior-paper manifest changed")
    fills = manifest.get("fills") or {}
    needed = {str(digest) for row in fills.values()
              for digest in row.get("source_record_sha256") or []}
    source = Path(str(manifest.get("source") or "")).resolve(strict=True)
    if source.is_relative_to(candidate):
        raise ValueError("source paper ledger must be outside the candidate")
    snapshot = candidate / SNAPSHOT_NAME
    if snapshot.exists():
        if (_sha256(snapshot) == manifest.get("source_snapshot_sha256")
                and manifest.get("source_snapshot") == SNAPSHOT_NAME):
            verify(candidate, receipt)
            return {"status": "already_staged", "records": len(needed),
                    "snapshot": str(snapshot), "sha256": _sha256(snapshot)}
        raise ValueError("existing source snapshot differs; refusing overwrite")
    temporary = candidate / f".{SNAPSHOT_NAME}.tmp.{os.getpid()}"
    found: set[str] = set()
    digest = hashlib.sha256()
    bytes_written = 0
    try:
        with source.open("rb") as source_handle, temporary.open("xb") as target:
            for line in source_handle:
                if not line.endswith(b"\n"):
                    break
                line_sha = hashlib.sha256(line).hexdigest()
                if line_sha not in needed or line_sha in found:
                    continue
                target.write(line)
                digest.update(line)
                bytes_written += len(line)
                found.add(line_sha)
            target.flush()
            os.fsync(target.fileno())
        if found != needed:
            raise ValueError(f"source ledger lacks {len(needed - found)} prior paper records")
        temporary.replace(snapshot)
    finally:
        if temporary.exists():
            temporary.unlink()
    manifest["source_snapshot"] = SNAPSHOT_NAME
    manifest["source_snapshot_sha256"] = digest.hexdigest()
    manifest["source_snapshot_records"] = len(found)
    _atomic_json(manifest_path, manifest)
    proof.update({
        "candidate_manifest_sha256": _sha256(manifest_path),
        "candidate_source_snapshot": SNAPSHOT_NAME,
        "candidate_source_snapshot_sha256": digest.hexdigest(),
        "candidate_source_snapshot_records": len(found),
    })
    receipt["prior_paper_fill_provenance"] = proof
    _atomic_json(receipt_path, receipt)
    verify(candidate, receipt)
    return {"status": "staged", "records": len(found), "bytes": bytes_written,
            "snapshot": str(snapshot), "sha256": digest.hexdigest()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(stage(args.candidate_dir), ensure_ascii=False))


if __name__ == "__main__":
    main()

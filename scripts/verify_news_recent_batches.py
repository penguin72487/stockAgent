"""Verify a bounded, newest committed news batch snapshot without HTTP or a writer.

This is a recent-write acceptance check, not a replacement for the full immutable
receipt audit or a claim of complete source history. Use the canonical benchmark
batch verifier for every immutable file/raw/text version and every current
queue-head witness for its observed article IDs inside the batch cohort. A
prior version is not a current URL head; unrelated queue IDs are out of scope.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_queue import utc_now
from scripts.benchmark_cnyes_acquisition import verify_batches


VERIFICATION_CONTRACT_VERSION = 2


def verify_recent_batches(root: Path, maximum_batches: int = 128, *, batch_paths=None) -> dict:
    if not 1 <= maximum_batches <= 1024:
        raise ValueError("maximum_batches must be between 1 and 1024")
    db = sqlite3.connect(f"file:{(root / 'queue.sqlite3').resolve()}?mode=ro", uri=True)
    try:
        db.execute("BEGIN")
        # Pin the WAL generation before reading batch paths or current heads.
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
        snapshot_at = utc_now()
        if batch_paths is None:
            paths = [row[0] for row in db.execute(
                "SELECT path FROM accepted_batches ORDER BY path DESC LIMIT ?", (maximum_batches,))]
        else:
            if (not isinstance(batch_paths, list) or not 1 <= len(batch_paths) <= maximum_batches
                    or any(not isinstance(path, str) for path in batch_paths) or len(set(batch_paths)) != len(batch_paths)):
                raise ValueError('bounded_distinct_snapshot_batch_paths_required')
            paths = list(batch_paths)
        result = {
            "verification_contract_version": VERIFICATION_CONTRACT_VERSION,
            "database_snapshot_at_utc": snapshot_at,
            "owner_id": owner[0] if owner else None,
            "verification_scope": "newest committed batches in a pinned read-only queue snapshot; not the whole lake, future writes or source history",
            "batch_selection_basis": "latest committed paths" if batch_paths is None else "explicit previous batch-path cohort in a new pinned queue snapshot",
            "maximum_batches": maximum_batches, "batches_verified": len(paths),
            "batch_paths": paths, "body_versions_verified": 0, "errors": [],
            "accepted": False, "historical_completeness_verified": False,
            "publish": False, "remote_training_export": False,
        }
        if batch_paths is not None:
            result['verification_scope'] = 'explicit previous batch-path cohort in a new pinned read-only queue snapshot; not the old WAL generation, whole lake, future writes or source history'
        if not paths:
            result["errors"].append({"error": "no_committed_batches_to_verify"})
        else:
            try:
                proof = verify_batches(SimpleNamespace(root=root, db=db), paths, allow_superseded_heads=True)
                result.update(body_versions_verified=proof["body_versions"],
                              private_raw_bytes=proof["private_raw_bytes"],
                              verified_ids_sha256=proof["verified_ids_sha256"], accepted=proof["accepted"])
                result.update({key: proof[key] for key in ('current_heads_verified', 'current_heads_outside_cohort',
                    'superseded_body_versions_verified', 'distinct_article_ids_checked', 'head_verification_scope')})
            except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
                result["errors"].append({"error": type(error).__name__ + ":" + str(error)[:160]})
        result["verified_at_utc"] = utc_now()
        result['implementation_sha256'] = {str(path): sha256_file(path) for path in (
            Path(__file__), Path(__file__).with_name('benchmark_cnyes_acquisition.py'))}
        return result
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--maximum-batches", type=int, default=128)
    parser.add_argument('--batch-snapshot', type=Path, help='Reverify exactly a prior report\'s bounded batch paths; never skip their immutable versions or claim its old WAL generation')
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if not 1 <= args.maximum_batches <= 1024:
        parser.error("maximum-batches must be between 1 and 1024")
    config, _, repo = load_config(args.config)
    paths = None
    if args.batch_snapshot is not None:
        if args.batch_snapshot.stat().st_size > 1 << 20:
            parser.error('batch snapshot report exceeds one MiB')
        paths = json.loads(args.batch_snapshot.read_text())['batch_paths']
    result = verify_recent_batches(repo / config["root"], args.maximum_batches, batch_paths=paths)
    if args.batch_snapshot is not None:
        result['input_batch_snapshot'] = str(args.batch_snapshot)
        result['input_batch_snapshot_sha256'] = sha256_file(args.batch_snapshot)
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print({key: value for key, value in result.items() if key != "batch_paths"})
    return 0 if result["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

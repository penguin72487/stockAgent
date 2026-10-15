"""Read-only acceptance of every frozen scope, separate from missing bodies.

One WAL snapshot; no publisher requests, queue writes, text export or forecast.
Verify exact URL membership, immutable receipt and current raw/body/file hashes.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_cohorts import cohort_progress, membership_digest, validate_supplements, supplement_progress
from downloader.news_fulltext_queue import source_path, utc_now
from scripts.benchmark_cnyes_acquisition import verify_batches


def audit_cohorts(root: Path) -> dict:
    db = sqlite3.connect(f"file:{(root / 'queue.sqlite3').resolve()}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute("BEGIN")
        result = {"contract_version": 1, "observed_at_utc": utc_now(), "cohorts": [],
                  "scope": "frozen local URL cohorts, not all publisher history",
                  "publish": False, "all_history_complete": False}
        result["supplemental_scopes"] = validate_supplements(root, db)
        for item in result["supplemental_scopes"]:
            item["progress"] = supplement_progress(db, item["provider"])
        for header in db.execute("SELECT * FROM finite_cohorts ORDER BY provider").fetchall():
            provider = header["provider"]
            item = {"provider": provider, **cohort_progress(db, provider), "errors": [],
                    "scope_integrity_accepted": False, "stored_heads_integrity_accepted": False}
            try:
                path = source_path(root, header["receipt_path"])
                receipt = json.loads(path.read_text())
                count, digest = membership_digest(db, provider)
                if (sha256_file(path) != header["receipt_sha256"] or receipt.get("contract_version") != 1
                        or receipt.get("provider") != provider or count != header["urls"]
                        or count != receipt.get("urls") or digest != header["urls_sha256"]
                        or digest != receipt.get("urls_sha256")):
                    raise ValueError("cohort_receipt_or_membership_mismatch")
                dangling = db.execute("SELECT count(*) FROM finite_cohort_members m LEFT JOIN articles a "
                    "ON a.id=m.article_id WHERE m.provider=? AND (a.id IS NULL OR a.url<>m.url OR a.provider<>m.provider)",
                    (provider,)).fetchone()[0]
                if dangling:
                    raise ValueError("cohort_article_identity_mismatch")
                item["scope_integrity_accepted"] = True
                heads = db.execute("SELECT a.id,a.parquet_path FROM finite_cohort_members m JOIN articles a "
                    "ON a.id=m.article_id WHERE m.provider=? AND a.body_sha256 IS NOT NULL", (provider,)).fetchall()
                if heads:
                    paths = sorted({h["parquet_path"] for h in heads})
                    item["batch_proof"] = verify_batches(SimpleNamespace(root=root, db=db),
                        paths, head_ids={h["id"] for h in heads})
                    head_paths = {h['id'].hex(): h['parquet_path'] for h in heads}
                    clocks, attributed, licensed = [], 0, 0
                    for relative in paths:
                        rows = pq.read_table(source_path(root, relative), columns=['article_id', 'published_at_raw',
                            'authors_json', 'per_article_license_verified']).to_pylist()
                        for row in rows:
                            if head_paths.get(row['article_id']) != relative:
                                continue
                            if row['published_at_raw']:
                                clocks.append(row['published_at_raw'])
                            attributed += bool(json.loads(row['authors_json']))
                            licensed += row['per_article_license_verified'] is True
                    item['stored_heads_with_declared_clock'] = len(clocks)
                    item['stored_heads_with_byline'] = attributed
                    item['stored_heads_with_article_license'] = licensed
                    # Raw clocks may have different formats/timezones. Do not
                    # manufacture chronological bounds by sorting these strings.
                    item['publication_clock_examples_raw'] = list(dict.fromkeys(clocks))[:3]
                    aware_clocks = []
                    for clock in clocks:
                        try:
                            stamp = datetime.fromisoformat(clock.replace('Z', '+00:00'))
                            if stamp.tzinfo is not None:
                                aware_clocks.append(stamp.astimezone(timezone.utc))
                        except (ValueError, OverflowError):
                            pass
                    item['stored_heads_with_timezone_aware_iso_clock'] = len(aware_clocks)
                    item['first_declared_publication_utc'] = min(aware_clocks).isoformat() if aware_clocks else None
                    item['last_declared_publication_utc'] = max(aware_clocks).isoformat() if aware_clocks else None
                    item['publication_bounds_cover_all_stored_heads'] = len(aware_clocks) == len(heads)
                item["stored_heads_integrity_accepted"] = True  # Empty set is not body-complete.
            except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
                item["errors"].append({"type": type(error).__name__, "error": str(error)[:160]})
            item["accepted"] = (item["scope_integrity_accepted"] and item["stored_heads_integrity_accepted"]
                                and item["all_cohort_bodies_stored"])
            result["cohorts"].append(item)
        result["scope_integrity_accepted"] = bool(result["cohorts"]) and all(
            c["scope_integrity_accepted"] and c["stored_heads_integrity_accepted"] for c in result["cohorts"])
        result["all_cohort_bodies_stored"] = bool(result["cohorts"]) and all(c["accepted"] for c in result["cohorts"])
        result["verified_at_utc"] = utc_now()
        return result
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    config, _, repo = load_config(args.config)
    result = audit_cohorts(repo / config["root"])
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print(json.dumps({k: result[k] for k in ("scope_integrity_accepted", "all_cohort_bodies_stored")}, ensure_ascii=False))
    for c in result["cohorts"]:
        print(json.dumps({k: c[k] for k in ("provider", "urls", "stored_bodies", "states", "accepted", "errors")}, ensure_ascii=False))
    return 0 if result["scope_integrity_accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

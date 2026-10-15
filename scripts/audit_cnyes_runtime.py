"""Read-only Cnyes runtime acceptance; no source requests, news text or secrets.

Compare two coherent database snapshots from the same production owner. A
separate immutable-body verification proves storage; liveness alone does not.
Neither proves earliest-source coverage or completion of all historical news.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import subprocess

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_queue import utc_now


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--previous", type=Path)
    parser.add_argument("--body-verification", type=Path)
    args = parser.parse_args(argv)
    config, registry, repo = load_config(args.config)
    root = repo / config["root"]
    source = registry.sources["cnyes"]
    status = json.loads((root / "status.json").read_text())
    fields = subprocess.check_output(["systemctl", "show", "stockagent-news-fulltext.service",
                                     "-p", "MainPID", "-p", "ActiveState", "-p", "ExecMainStartTimestamp"], text=True)
    service = dict(line.split("=", 1) for line in fields.splitlines())
    db = sqlite3.connect(f"file:{root / 'queue.sqlite3'}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute("BEGIN")
        states = [dict(r) for r in db.execute("SELECT * FROM state_metrics WHERE provider='cnyes'")]
        owner_row = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
        owner_id = owner_row[0] if owner_row else None
        snapshot_at = utc_now()
        stored = db.execute("SELECT count(*) FROM articles INDEXED BY stored_bodies "
                            "WHERE provider='cnyes' AND body_sha256 IS NOT NULL").fetchone()[0]
        canonical = db.execute("SELECT count(*) FROM articles INDEXED BY stored_bodies WHERE provider='cnyes' "
                               "AND body_sha256 IS NOT NULL AND url GLOB 'https://news.cnyes.com/news/id/[0-9]*'").fetchone()[0]
        error_counts = [dict(r) for r in db.execute(
            "SELECT error_code,count(*) AS jobs FROM discovery WHERE provider='cnyes' "
            "AND superseded_by_windows_json IS NULL AND error_code IS NOT NULL GROUP BY error_code")]
        archived_errors = [dict(r) for r in db.execute(
            "SELECT error_code,count(*) AS jobs FROM discovery WHERE provider='cnyes' "
            "AND superseded_by_windows_json IS NOT NULL AND error_code IS NOT NULL GROUP BY error_code")]
        gaps = [dict(r) for r in db.execute("SELECT * FROM bulk_window_gaps WHERE provider='cnyes'")]
        split_count = db.execute("SELECT count(*) FROM bulk_window_splits WHERE provider='cnyes'").fetchone()[0]
        pending = db.execute("SELECT count(*) FROM discovery WHERE provider='cnyes' AND kind='cnyes_news' "
                             "AND superseded_by_windows_json IS NULL AND due<=strftime('%s','now')").fetchone()[0]
        item_errors = [dict(r) for r in db.execute("SELECT error_code,count(*) AS items FROM bulk_item_errors "
                                                 "WHERE provider='cnyes' GROUP BY error_code")]
    finally:
        db.close()
    result = {
        "observed_at_utc": snapshot_at, "service": service, "config_sha256": sha256_file(args.config),
        "owner_id": owner_id, "status_owner_id": status.get("owner_id"),
        "status_database_owner_consistent": bool(owner_id and owner_id == status.get("owner_id")),
        "data_fingerprint": source.data_fingerprint, "interval_seconds": source.interval_seconds,
        "parallelism": source.parallelism, "official_numeric_rate_limit": None,
        "stored_url_heads": stored, "canonical_news_id_url_heads": canonical, "states": states,
        "active_discovery_errors": error_counts, "archived_discovery_errors": archived_errors,
        "historical_windows_partitioned": split_count, "single_day_overflow_gaps": gaps,
        "bulk_ready_jobs": pending, "bulk_item_errors": item_errors,
        "status_snapshot_at_utc": status["updated_at_utc"], "status_activity": status["activity"],
        "performance": status.get("performance", {}).get("providers", {}).get("cnyes", {}),
        "all_history_complete": False, "publish": False, "remote_training_export": False,
        "training_injection_enabled": False,
        "scope": "production owner progress and independently verified captured bodies, not a source history completeness claim",
    }
    if args.previous:
        previous = json.loads(args.previous.read_text())
        elapsed = (datetime.fromisoformat(snapshot_at) - datetime.fromisoformat(previous["observed_at_utc"])).total_seconds()
        same_owner = (service["MainPID"] != "0" and service["MainPID"] == previous["service"]["MainPID"]
                      and owner_id is not None and owner_id == previous.get("owner_id"))
        result["production_progress"] = {
            "same_owner": same_owner, "wall_seconds": elapsed,
            "new_stored_url_heads": stored - previous["stored_url_heads"],
            "new_canonical_news_id_url_heads": canonical - previous["canonical_news_id_url_heads"],
            "stored_url_heads_per_second": (stored - previous["stored_url_heads"]) / elapsed if same_owner and elapsed > 0 else None,
        }
        before = previous.get("performance", {}).get("counts", {})
        after = result["performance"].get("counts", {})
        coherent = result["status_database_owner_consistent"] and previous.get("status_database_owner_consistent", False)
        result["production_progress"]["http_counts_delta"] = {key: value - before.get(key, 0) for key, value in after.items()} if same_owner and coherent else None
        counter_seconds = (datetime.fromisoformat(result["status_snapshot_at_utc"])
                           - datetime.fromisoformat(previous["status_snapshot_at_utc"])).total_seconds()
        result["production_progress"]["http_counter_window_seconds"] = counter_seconds if same_owner and coherent else None
        result["production_progress"]["http_requests_per_second"] = (
            (after.get("http_requests_started", 0) - before.get("http_requests_started", 0)) / counter_seconds
            if same_owner and coherent and counter_seconds > 0 else None)
        result["production_progress"]["http_counter_basis"] = "same-owner published status clocks; database head growth uses separate coherent database clocks"
    if args.body_verification:
        verification = json.loads(args.body_verification.read_text())
        verification_at = datetime.fromisoformat(verification["database_snapshot_at_utc"])
        verification_age = (datetime.now(timezone.utc) - verification_at).total_seconds()
        result["body_verification"] = {
            "path": str(args.body_verification), "sha256": sha256_file(args.body_verification),
            "accepted": verification["accepted"], "snapshot_at_utc": verification["database_snapshot_at_utc"],
            "body_versions_verified": verification["body_versions_verified"], "errors": verification["errors"],
        }
        progress = result.get("production_progress", {})
        checks = {
            "service_active": service["ActiveState"] == "active" and service["MainPID"] != "0",
            "same_owner_progress": progress.get("same_owner", False) and progress.get("new_stored_url_heads", 0) > 0,
            "status_database_owner_consistent": result["status_database_owner_consistent"] and previous.get("status_database_owner_consistent", False) if args.previous else False,
            "verified_bodies_recent": verification["accepted"] and 0 <= verification_age <= 300,
            "no_active_page_cap_422": not any(e["error_code"] == "discovery_http_422" for e in error_counts),
            "no_claim_of_all_history": result["all_history_complete"] is False,
        }
        result["engineering_checks"] = checks
        result["accepted"] = all(checks.values())
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("accepted", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())

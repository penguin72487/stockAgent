"""Bounded end-to-end Cnyes experiment using the single production frontier.

Stop only the canonical news service/timer before this command; the owner lock
rejects overlap. Accepted bodies remain in that frontier, never in a second lake.
Rates are measured client policies, NOT undocumented official ceilings. Refusal
or sustained source errors stops escalation; no proxies, identity rotation or
cooldown resets. Every phase includes durable acceptance and its verification.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import resource
import signal
import time

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config, run_cycle
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_metrics import NewsMetrics
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, source_path, utc_now, verify_bodies


def verify_batches(queue, paths, *, head_ids=None, allow_superseded_heads=False):
    """Verify immutable envelopes; optionally pin only the selected current heads.

    A batch may also contain unrelated historical versions. Those still need
    valid receipt/raw/body hashes, but need not remain their URL's latest head.
    The default preserves the existing all-rows-current benchmark contract.
    Recent-write audits can explicitly verify EVERY immutable version while
    requiring a matching current-head witness for the observed article IDs
    whose heads point inside their cohort, not unrelated queue article IDs.
    Heads outside that cohort must still exist and reference a committed batch;
    their bodies are outside this bounded audit, not silently called verified.
    """
    from downloader.news_fulltext_public_originals import verify_resolution_response
    if type(allow_superseded_heads) is not bool or allow_superseded_heads and head_ids is not None:
        raise ValueError('incompatible_batch_head_verification_scope')
    paths = tuple(paths)
    cohort_paths = set(paths)
    versions, raw_bytes, hashes = 0, 0, []
    matched_heads, required_cohort_heads = set(), set()
    heads, committed_outside_paths = {}, set()
    superseded_versions = 0
    for path in paths:
        receipt_path = queue.db.execute("SELECT receipt_path FROM accepted_batches WHERE path=?", (path,)).fetchone()[0]
        receipt = json.loads(source_path(queue.root, receipt_path).read_text())
        target = source_path(queue.root, path)
        if sha256_file(target) != receipt["sha256"]:
            raise ValueError("benchmark_durable_receipt_mismatch")
        rows = pq.read_table(target).to_pylist()
        if len(rows) != receipt["rows"]:
            raise ValueError("benchmark_durable_rows_mismatch")
        envelopes = {r["article_id"]: r["raw_response_body"] for r in rows if r["raw_response_body"] is not None}
        raw_bytes += sum(map(len, envelopes.values()))
        for row in rows:
            raw_bytes += verify_resolution_response(row)
            raw = envelopes[row["source_response_article_id"]]
            if hashlib.sha256(raw).hexdigest() != row["response_sha256"] or hashlib.sha256(row["body"].encode()).hexdigest() != row["body_sha256"]:
                raise ValueError("benchmark_raw_or_body_hash_mismatch")
            article_id = bytes.fromhex(row["article_id"])
            if allow_superseded_heads or head_ids is None or article_id in head_ids:
                if article_id not in heads:
                    heads[article_id] = queue.db.execute("SELECT body_sha256,parquet_path FROM articles WHERE id=?", (article_id,)).fetchone()
                head = heads[article_id]
                if head is None:
                    raise ValueError("benchmark_queue_head_mismatch")
                if allow_superseded_heads:
                    if head[1] in cohort_paths:
                        required_cohort_heads.add(article_id)
                    else:
                        if (not isinstance(head[0], str) or len(head[0]) != 64
                                or any(c not in '0123456789abcdef' for c in head[0])):
                            raise ValueError('recent_queue_head_references_uncommitted_batch')
                        if head[1] not in committed_outside_paths:
                            source_path(queue.root, head[1])  # Reject unsafe paths even outside the read cohort.
                            if not queue.db.execute('SELECT 1 FROM accepted_batches WHERE path=?', (head[1],)).fetchone():
                                raise ValueError('recent_queue_head_references_uncommitted_batch')
                            committed_outside_paths.add(head[1])
                    if tuple(head) == (row['body_sha256'], path):
                        matched_heads.add(article_id)
                    else:
                        superseded_versions += 1
                else:
                    if (head_ids is None or head[1] == path) and tuple(head) != (row["body_sha256"], path):
                        raise ValueError("benchmark_queue_head_mismatch")
                    if head[1] == path:
                        matched_heads.add(article_id)
            hashes.append(row["article_id"])
        versions += len(rows)
    if head_ids is not None and matched_heads != set(head_ids):
        raise ValueError("selected_queue_heads_missing_from_batches")
    if allow_superseded_heads and matched_heads != required_cohort_heads:
        raise ValueError('recent_queue_head_mismatch')
    return {"body_versions": versions, "private_raw_bytes": raw_bytes,
            "current_heads_verified": len(matched_heads),
            **({'superseded_body_versions_verified': superseded_versions,
                'current_heads_outside_cohort': len(heads) - len(required_cohort_heads),
                'distinct_article_ids_checked': len(heads),
                'head_verification_scope': 'all immutable versions and current-head witnesses for their observed article IDs pointing into this cohort; outside-cohort bodies and unrelated queue IDs not verified'}
               if allow_superseded_heads else {}),
            "verified_ids_sha256": hashlib.sha256(json.dumps(sorted(hashes)).encode()).hexdigest(), "accepted": True}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--html-articles", type=int, default=24)
    parser.add_argument("--bulk-requests", type=int, default=8)
    parser.add_argument("--faster-api-interval", type=float,
                        help="Additional bounded API-only 1s/faster ABBA; minimum 0.5s, never a quota claim")
    args = parser.parse_args(argv)
    if not 1 <= args.html_articles <= 60 or not 1 <= args.bulk_requests <= 12:
        parser.error("bounded workload required")
    if args.faster_api_interval is not None and not 0.5 <= args.faster_api_interval < 1:
        parser.error("additional API-only probe must use 0.5 <= interval < 1")
    config, registry, repo = load_config(args.config)
    source = registry.sources["cnyes"]
    if not source.cnyes_apis:
        parser.error("review and register the Cnyes bulk source first")
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    result = {"started_at_utc": utc_now(), "config_sha256": sha256_file(args.config), "phases": [],
              "method": "bounded API 1s/faster ABBA" if args.faster_api_interval else "HTML controls and API 2s/1s ABBA",
              "measurement_scope": "existing frontier, same validation/storage, phase-specific independent verification; different pages and duplicates retained",
              "official_numeric_rate_limit": None, "historical_completeness_verified": False, "publish": False}
    started, started_cpu = time.monotonic(), time.process_time()
    queue = NewsQueue(repo / config["root"], registry)
    http = NewsHTTP(registry, config, queue.root)
    handlers = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
    for s in handlers:
        signal.signal(s, lambda *_: http.stop.set())
    try:
        phases = [("html", 2.0, 2), ("bulk", 2.0, 2), ("bulk", 1.0, 3),
                  ("bulk", 1.0, 3), ("bulk", 2.0, 2), ("html", 2.0, 2)]
        if args.faster_api_interval is not None:
            phases = [("bulk", 1.0, 3), ("bulk", args.faster_api_interval, 3),
                      ("bulk", args.faster_api_interval, 3), ("bulk", 1.0, 3)]
        for kind, interval, parallelism in phases:
            if http.stop.is_set() or http.provider_not_before.get("cnyes", 0) > time.time():
                result["stopped_escalation"] = "source_cooldown_or_stop; persisted deadline preserved"
                break
            effective = deepcopy(config)
            next(p for p in effective["providers"] if p["id"] == "cnyes").update(interval_seconds=interval, parallelism=parallelism)
            registry.sources["cnyes"] = PolicyRegistry(effective, base=repo).sources["cnyes"]
            http.set_client_capacity("cnyes", interval, parallelism)
            metrics = NewsMetrics()
            queue.metrics = http.metrics = metrics
            before = {row[0] for row in queue.db.execute("SELECT path FROM accepted_batches")}
            before_stored = queue.db.execute("SELECT count(*) FROM articles INDEXED BY stored_bodies WHERE provider='cnyes' AND body_sha256 IS NOT NULL").fetchone()[0]
            phase_started, phase_cpu = time.monotonic(), time.process_time()
            status = run_cycle(queue, http, effective, max_articles=args.html_articles if kind == "html" else 0,
                               max_discovery_requests=args.bulk_requests if kind == "bulk" else 0,
                               provider_ids={"cnyes"}, max_run_seconds=180)
            after = {row[0] for row in queue.db.execute("SELECT path FROM accepted_batches")}
            with metrics.measure("independent_phase_verification"):
                acceptance = verify_batches(queue, sorted(after - before))
            after_stored = queue.db.execute("SELECT count(*) FROM articles INDEXED BY stored_bodies WHERE provider='cnyes' AND body_sha256 IS NOT NULL").fetchone()[0]
            wall = time.monotonic() - phase_started
            measurement = metrics.snapshot()
            counts = measurement["providers"].get("cnyes", {}).get("counts", {})
            attempted = counts.get("frontier_attempts_started", 0)
            errors = counts.get("http_transient_errors", 0) + counts.get("transport_errors", 0)
            requests = counts.get("http_requests_started", 0)
            phase = {"kind": kind, "interval_seconds": interval, "parallelism": parallelism, "wall_seconds": wall,
                     "cpu_seconds": time.process_time() - phase_cpu, "new_stored_url_heads": after_stored - before_stored,
                     "new_heads_per_second": (after_stored - before_stored) / wall,
                     "durable_versions_per_second": acceptance["body_versions"] / wall,
                     "body_versions": acceptance["body_versions"], "http_requests": requests,
                     "validated_api_ids": counts.get("bulk_article_ids_validated", 0),
                     "transient_errors": errors, "http_transient_success_fraction": 1 - errors / requests if requests else None,
                     "http_200_responses": counts.get("http_status_200", 0),
                     "metadata_only_pages": counts.get("restricted_pages_metadata_only", 0),
                     "html_attempt_success_fraction": acceptance["body_versions"] / attempted if attempted else None,
                     "acceptance": acceptance, "cycle": status["cycle"], "performance": measurement}
            result["phases"].append(phase)
            atomic_write_json(args.output, result)
            print(json.dumps({k: v for k, v in phase.items() if k not in {"performance", "cycle", "acceptance"}}, ensure_ascii=False), flush=True)
            if http.provider_not_before.get("cnyes", 0) > time.time() or requests and errors / requests > 0.1:
                result["stopped_escalation"] = "source health threshold; no faster probe"
                break
        with queue.metrics.measure("independent_final_verification"):
            result["final_acceptance"] = verify_bodies(queue.root)
        result.update(completed_at_utc=utc_now(), total_wall_seconds=time.monotonic() - started,
                      total_cpu_seconds=time.process_time() - started_cpu,
                      peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        result["accepted"] = result["final_acceptance"]["accepted"] and bool(result["phases"])
        atomic_write_json(args.output, result)
        print(json.dumps({k: v for k, v in result.items() if k != "phases"}, ensure_ascii=False), flush=True)
        return 0 if result["accepted"] else 1
    finally:
        for s, handler in handlers.items():
            signal.signal(s, handler)
        http.close()
        queue.close()


if __name__ == "__main__":
    raise SystemExit(main())

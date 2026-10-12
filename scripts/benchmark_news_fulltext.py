"""Offline, source-backed article parsing and durable-acceptance benchmark.

Only reads previously accepted private responses. Never contacts a publisher or
FinLab/FinMind, publishes full text, or claims network/all-history throughput.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import resource
import statistics
import tempfile
import time

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_queue import NewsQueue, source_path, verify_bodies


def samples(root, registry, per_provider):
    counts, rows = Counter(), []
    for receipt_path in sorted((root / "receipts").glob("*.json")):
        receipt = json.loads(receipt_path.read_text())
        path = source_path(root, receipt["parquet_path"])
        if sha256_file(path) != receipt["sha256"]:
            raise ValueError("benchmark_source_digest_mismatch")
        for record in pq.read_table(path).to_pylist():
            provider = record["provider"]
            if (record.get("raw_response_body") is not None and provider in registry.sources
                    and registry.sources[provider].allowed and counts[provider] < per_provider):
                rows.append(record)
                counts[provider] += 1
    if not rows:
        raise ValueError("no accepted private source responses for benchmark")
    return rows, dict(counts)


def extract(rows, registry, parser):
    outputs = []
    for row in rows:
        source = replace(registry.sources[row["provider"]], html_parser=parser)
        try:
            outputs.append(extract_article(row["raw_response_body"], source, page_url=row["final_url"]))
        except NewsError as error:
            outputs.append({"rejected": error.code})
    return outputs


def timed(operation):
    wall, cpu = time.perf_counter(), time.process_time()
    result = operation()
    return result, {"wall_seconds": time.perf_counter() - wall, "cpu_seconds": time.process_time() - cpu}


class CachedHTTP:
    """Replay immutable responses through the canonical article/queue contracts."""

    def __init__(self, registry, rows):
        self.registry = registry
        self.responses = {row["requested_url"]: row for row in rows}

    def get(self, url, *, headers=None):
        row = self.responses[url]
        return Response(row["final_url"], 200, {"content-type": "text/html"}, row["raw_response_body"])


def pipeline(rows, registry, config, parser, parent):
    with tempfile.TemporaryDirectory(prefix="news-replay-", dir=parent) as directory:
        root = Path(directory)
        registry.sources = {key: replace(source, html_parser=parser) for key, source in registry.sources.items()}
        http = CachedHTTP(registry, rows)
        accepted = []
        queue = NewsQueue(root, registry)
        try:
            with queue.db:
                queue.add_articles([{"url": row["requested_url"], "date": row.get("metadata_date_raw")} for row in rows])
            # Production freezes finite scopes after local metadata ingestion.
            # A fresh isolated replay must do the same before a normal claim;
            # never switch a finite source to continuous or bypass membership.
            queue.sync_finite_cohorts()
            for row in rows:
                claimed = queue.claim_url(row["requested_url"], config["lease_seconds"])
                if not claimed:
                    continue
                try:
                    accepted.append(fetch_article(http, claimed))
                except NewsError as error:
                    queue.failed(claimed, error, config["max_attempts"])
            for offset in range(0, len(accepted), config["batch_articles"]):
                queue.accept(accepted[offset:offset + config["batch_articles"]])
        finally:
            queue.close()
        verified = verify_bodies(root)
        return {"accepted_records": len(accepted), "verified": verified["accepted"],
                "verified_body_versions": verified["body_versions_verified"],
                "body_hashes": [row["body_sha256"] for row in accepted]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--root", type=Path)
    parser.add_argument("--per-provider", type=int, default=64)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.per_provider < 1 or args.repeat < 1:
        parser.error("sample/repeat counts must be positive")
    config, registry, repo = load_config(args.config)
    rows, counts = samples(args.root or repo / config["root"], registry, args.per_provider)
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    results = {"html.parser": {"parse": [], "pipeline": []}, "lxml": {"parse": [], "pipeline": []}}
    reference, pipeline_reference = None, None
    mismatches, pipeline_mismatches, stored_body_mismatches = set(), set(), set()
    for repetition in range(args.repeat):
        order = ("html.parser", "lxml") if repetition % 2 == 0 else ("lxml", "html.parser")
        for backend in order:
            outputs, measurement = timed(lambda: extract(rows, registry, backend))
            results[backend]["parse"].append(measurement)
            if reference is None:
                reference = outputs
            mismatches.update(row["article_id"] for row, expected, actual in zip(rows, reference, outputs) if expected != actual)
            stored_body_mismatches.update(row["article_id"] for row, actual in zip(rows, outputs)
                                          if actual.get("body_sha256") != row["body_sha256"])
            outcome, measurement = timed(lambda: pipeline(rows, registry, config, backend, args.output.parent))
            results[backend]["pipeline"].append({**measurement, **{k: v for k, v in outcome.items() if k != "body_hashes"}})
            if pipeline_reference is None:
                pipeline_reference = outcome
            if outcome != pipeline_reference:
                pipeline_mismatches.add(backend)
    for item in results.values():
        item["median_parse_wall_seconds"] = statistics.median(r["wall_seconds"] for r in item["parse"])
        item["median_pipeline_wall_seconds"] = statistics.median(r["wall_seconds"] for r in item["pipeline"])
    result = {"benchmark_version": 1, "scope": "offline original-response extraction + queue + durable Parquet + receipt + independent verify",
              "network_requests": 0, "sources": counts, "source_records": len(rows), "repeat": args.repeat,
              "raw_source_bytes": sum(len(row["raw_response_body"]) for row in rows),
              "source_identity_sha256": hashlib.sha256(json.dumps([(r["article_id"], r["response_sha256"]) for r in rows]).encode()).hexdigest(),
              "results": results, "extraction_mismatches": sorted(mismatches), "pipeline_mismatches": sorted(pipeline_mismatches),
              "stored_body_mismatches": sorted(stored_body_mismatches),
              "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
              "accepted": not mismatches and not pipeline_mismatches and not stored_body_mismatches and all(r["verified"] for item in results.values() for r in item["pipeline"]),
              "production_network_speedup_verified": False, "historical_completeness_verified": False, "publish": False}
    atomic_write_json(args.output, result)
    print(json.dumps({k: v for k, v in result.items() if k != "results"}, indent=2))
    return 0 if result["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

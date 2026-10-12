"""Exclusive live capacity search using the canonical queue and durable bodies.

Stop only the news service/timer and set automatic resume protection first.
Rate learning persists for the production owner; no discarded load-only traffic.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import resource
import signal
import time

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config, run_service
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_queue import NewsQueue, utc_now
from scripts.benchmark_cnyes_acquisition import verify_batches


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--duration-seconds", type=int, default=600)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if not 60 <= args.duration_seconds <= 3600:
        parser.error("calibration duration must be 60..3600 seconds")
    config, registry, repo = load_config(args.config)
    if not config.get("adaptive_rate", {}).get("enabled"):
        parser.error("enable reviewed adaptive_rate policy before calibration")
    started, cpu_started = time.monotonic(), time.process_time()
    result = {"started_at_utc": utc_now(), "config_sha256": sha256_file(args.config),
              "source_code_sha256": {str(p.relative_to(repo)): sha256_file(p)
                                     for p in sorted((repo / "downloader").glob("news_fulltext*.py"))},
              "duration_seconds": args.duration_seconds, "official_maximum_verified": False,
              "all_history_complete": False, "publish": False}
    queue = NewsQueue(repo / config["root"], registry)
    http = NewsHTTP(registry, config, queue.root, metrics=queue.metrics)
    previous_handlers = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
    for sig in previous_handlers:
        signal.signal(sig, lambda *_: http.stop.set())
    before = {r[0] for r in queue.db.execute("SELECT path FROM accepted_batches")}
    try:
        result["owner_id"] = queue.owner
        result["final_status"] = run_service(queue, http, config, max_articles=512, max_discovery_requests=512,
                                            duration_seconds=args.duration_seconds)
        result["calibration"] = http.capacity_controller.snapshot()
        after = {r[0] for r in queue.db.execute("SELECT path FROM accepted_batches")}
        with queue.metrics.measure("independent_calibration_batch_verification"):
            result["body_acceptance"] = verify_batches(queue, sorted(after - before))
        result.update(completed_at_utc=utc_now(), full_wall_seconds=time.monotonic() - started,
                      process_cpu_seconds=time.process_time() - cpu_started,
                      peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                      performance=queue.metrics.snapshot(),
                      accepted=result["body_acceptance"]["accepted"])
        atomic_write_json(args.output, result)
        print({"accepted": result["accepted"], "output": str(args.output),
               "body_versions": result["body_acceptance"]["body_versions"]}, flush=True)
        return 0 if result["accepted"] else 1
    finally:
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)
        http.close()
        queue.close()


if __name__ == "__main__":
    raise SystemExit(main())

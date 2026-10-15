"""Bounded parallel publisher experiment in the single production frontier.

Exclusive queue ownership, shared publisher pacing/robots/cooldowns, unchanged
validation and durable receipts. Independent publishers escalate only while
healthy. Fastest tested is not an undocumented official or sustainable maximum.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
from pathlib import Path
import resource
import signal
import time

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config, run_cycle
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_capacity import PublisherCapacity
from downloader.news_fulltext_metrics import NewsMetrics
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, utc_now, verify_bodies
from scripts.benchmark_cnyes_acquisition import verify_batches


def stored_counts(queue):
    return dict(queue.db.execute("SELECT provider,count(*) FROM articles INDEXED BY stored_bodies "
                                 "WHERE body_sha256 IS NOT NULL GROUP BY provider"))


def set_intervals(config, registry, http, intervals, parallelism=None):
    effective = deepcopy(config)
    # Fixed-rate trials must not instantiate the production adaptive controller:
    # otherwise its checkpoint silently replaces the candidate being measured.
    effective["adaptive_rate"] = {"enabled": False}
    for entry in effective["providers"]:
        if entry["id"] in intervals:
            entry["interval_seconds"] = intervals[entry["id"]]
        if parallelism and entry["id"] in parallelism:
            entry["parallelism"] = parallelism[entry["id"]]
    for provider in intervals:
        slots = parallelism.get(provider, registry.sources[provider].parallelism) if parallelism else registry.sources[provider].parallelism
        http.set_client_capacity(provider, intervals[provider], slots)
    return effective


def phase_provider_metrics(source, performance, before, after, wall, cooldown, adapter_error, control=None):
    counts = performance.get("counts", {})
    requests = counts.get("http_requests_started", 0)
    responses = counts.get("http_responses_received", 0)
    transient = counts.get("http_transient_errors", 0) + counts.get("transport_errors", 0)
    refusals = sum(counts.get("http_status_" + str(s), 0) for s in (401, 403, 429, 451)) + counts.get("content_access_refusals", 0)
    unusable = sum(value for key, value in counts.items() if key.startswith("http_status_")
                   and key.removeprefix("http_status_").isdigit()
                   and 400 <= int(key.removeprefix("http_status_")) < 500
                   and int(key.removeprefix("http_status_")) not in {401, 403, 404, 410, 429, 451})
    healthy = requests > 0 and not cooldown and not adapter_error and not refusals and (transient + unusable) / requests <= 0.05
    new_heads = after - before
    return {
        "client_interval_seconds": (control or {}).get("client_interval_seconds", source.interval_seconds),
        "effective_interval_seconds": (control or {}).get("effective_interval_seconds", source.interval_seconds),
        "parallelism": (control or {}).get("parallelism", source.parallelism),
        "http_requests": requests, "http_responses": responses,
        "http_200_responses": counts.get("http_status_200", 0),
        "http_304_responses": counts.get("http_status_304", 0),
        "transport_or_transient_errors": transient, "explicit_refusals": refusals,
        "unusable_http_responses": unusable, "ended_at_epoch": time.time(),
        "http_response_fraction": responses / requests if requests else None,
        "peak_overlapping_responses": counts.get("peak_http_inflight", 0),
        "minimum_request_start_gap_seconds": performance.get("minimum_request_start_gap_seconds"),
        "requests_per_full_workflow_second": requests / wall,
        "new_stored_url_heads": new_heads, "new_heads_per_full_workflow_second": new_heads / wall,
        "durable_body_versions": counts.get("durably_accepted_versions", 0),
        "validated_bulk_article_ids": counts.get("bulk_article_ids_validated", 0),
        "html_attempts": counts.get("frontier_attempts_started", 0),
        "healthy_for_next_bounded_phase": healthy, "provider_cooling": cooldown,
        "adapter_error": adapter_error,
        "status_counts": {key: value for key, value in counts.items() if key.startswith("http_status_")},
    }


def cnyes_retest_candidates(maximum_rps, maximum_slots):
    return [(rate, 0, maximum_slots, 0) for rate in (2, 5, 10, 20, 30, 50, 75, 100) if rate <= maximum_rps]


def repeated_candidate_is_healthy(item, minimum_requests=20):
    return bool(item["healthy_for_next_bounded_phase"] and item["http_responses"] >= minimum_requests
                and (item["transport_or_transient_errors"] + item.get("unusable_http_responses", 0))
                    / max(1, item["http_requests"]) <= 0.01
                and item["new_stored_url_heads"] > 0)


def publish_retest_status(queue, http, activity):
    queue.http_capacity_snapshot = http.capacity_snapshot
    return queue.status(activity=activity, requests_by_provider=http.requests_by_provider,
                        response_bytes=http.response_bytes, provider_cooldowns=http.provider_not_before)


def accept_retested_candidate(http, config, candidate, repeated_windows):
    """Promote only repeated, durable useful-body evidence, never a rate quota."""
    required = config.get("adaptive_rate", {}).get("stable_windows", 3)
    if (len(repeated_windows) < required
            or not all(repeated_candidate_is_healthy(row) for row in repeated_windows)
            or any(row["client_interval_seconds"] != candidate["client_interval_seconds"]
                   or row["parallelism"] != candidate["parallelism"] for row in repeated_windows)):
        return {"accepted": False, "reason": "candidate needs repeated healthy durable-body windows"}
    controller = PublisherCapacity(http, config)
    http.capacity_controller = controller
    def capacity_sample(row):
        return {
            "ended_at_epoch": row["ended_at_epoch"], "wall_seconds": row["wall_seconds"],
            "interval_seconds": row["effective_interval_seconds"], "parallelism": row["parallelism"],
            "new_heads_per_second": row["new_heads_per_full_workflow_second"],
            "comparable_new_heads_per_second": row["new_heads_per_full_workflow_second"],
            "actual_requests_per_second": row["requests_per_full_workflow_second"],
            "requests": row["http_requests"], "responses": row["http_responses"],
            "new_stored_url_heads": row["new_stored_url_heads"], "durable_body_versions": row["durable_body_versions"],
            "workload_family": "cnyes_bulk", "healthy": True,
            "measurement_basis": "fixed-rate full phase wall includes drain and independent receipt verification",
        }

    sample = capacity_sample(candidate)
    state = controller.states["cnyes"]
    state.update(best=sample, best_by_workload={**state["best_by_workload"], "cnyes_bulk": sample},
                 workload_family="cnyes_bulk", last_window=capacity_sample(repeated_windows[-1]))
    controller._set("cnyes", sample["interval_seconds"], sample["parallelism"], "stable_pipeline_capacity",
                    "fixed-rate candidate passed repeated durable-body windows; not a source quota")
    state.update(stable_samples=len(repeated_windows), next_search_epoch=time.time() + controller.policy["recheck_seconds"])
    atomic_write_json(controller.path, controller.snapshot())
    return {"accepted": True, "interval_seconds": http.effective_interval("cnyes"),
            "parallelism": http.parallelism["cnyes"], "stable_windows": len(repeated_windows), "source_maximum_verified": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--phase-seconds", type=int, default=20)
    parser.add_argument("--maximum-cnyes-rps", type=int, choices=(2, 3, 4, 5, 6, 8, 10, 20, 30, 50, 75, 100), default=4)
    parser.add_argument("--capacity-compare", action="store_true", help="Same 8/4 rps: reserved response slots 3/2, 5/3, 5/3, 3/2")
    parser.add_argument("--cnyes-retest", action="store_true", help="Cnyes bulk-only 2/5/10/20/30/50/75/100; repeat durable winner before promotion")
    args = parser.parse_args(argv)
    if not 10 <= args.phase_seconds <= 120:
        parser.error("bounded phase duration must be 10..120 seconds")
    if args.cnyes_retest and (args.capacity_compare or args.maximum_cnyes_rps not in (2, 5, 10, 20, 30, 50, 75, 100)):
        parser.error("cnyes-retest requires an explicit 2/5/10/20/30/50/75/100 maximum and no capacity-compare")
    config, registry, repo = load_config(args.config)
    if args.capacity_compare:
        config["workers"] = max(config["workers"], sum(s.parallelism for s in registry.sources.values() if s.allowed)
                                + 5 - registry.sources["cnyes"].parallelism + 3 - registry.sources["fed_board"].parallelism)
    allowed = {s.provider for s in registry.sources.values() if s.allowed}
    if args.cnyes_retest:
        if "cnyes" not in allowed:
            parser.error("Cnyes must be reviewed and enabled")
        allowed = {"cnyes"}
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    started, cpu_started = time.monotonic(), time.process_time()
    result = {"started_at_utc": utc_now(), "config_sha256": sha256_file(args.config), "phases": [],
              "method": "fixed Cnyes bulk-rate ladder, independent drain/receipt proof, repeated useful winner" if args.cnyes_retest else
                        "same-rate reserved-capacity ABBA 3/2,5/3,5/3,3/2" if args.capacity_compare else
                        "independent publisher ramp, repeated fastest candidate, initial-rate control",
              "bounds": {"phase_seconds": args.phase_seconds, "maximum_cnyes_probe_rps": args.maximum_cnyes_rps,
                         "maximum_fed_probe_rps": 4, "workers": config["workers"]},
              "official_rate_limits_verified": False, "sustainable_maximum_verified": False,
              "source_code_sha256": {str(p.relative_to(repo)): sha256_file(p) for p in sorted((repo / 'downloader').glob('news_fulltext*.py'))},
              "historical_completeness_verified": False, "publish": False,
              "clock_basis": "phase wall includes all providers, pacing, failures, full drain, compression/fsync and independent verification; provider stage times overlap"}
    queue = NewsQueue(repo / config["root"], registry)
    http = NewsHTTP(registry, config, queue.root)
    handlers = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
    for s in handlers:
        signal.signal(s, lambda *_: http.stop.set())
    blocked = {}
    try:
        candidates = [(2, 0.5, 3, 2), (3, 1, 3, 2), (4, 2, 3, 2), (6, 3, 3, 2), (8, 4, 3, 2)]
        candidates = [pair for pair in candidates if pair[0] <= args.maximum_cnyes_rps]
        candidates += [candidates[-1], candidates[0]]
        if args.capacity_compare:
            candidates = [(8, 4, 3, 2), (8, 4, 5, 3), (8, 4, 5, 3), (8, 4, 3, 2)]
        if args.cnyes_retest:
            candidates = cnyes_retest_candidates(args.maximum_cnyes_rps, http.maximum_parallelism["cnyes"])
        winner, repeated = None, []
        with ThreadPoolExecutor(max_workers=config["workers"], thread_name_prefix="news-provider-benchmark") as executor:
            for number, (cnyes_rps, fed_rps, cnyes_slots, fed_slots) in enumerate(candidates, start=1):
                if http.stop.is_set():
                    result["stopped"] = "signal; leased outcomes drained"
                    break
                if args.cnyes_retest:
                    while http.provider_not_before.get("cnyes", 0) > time.time() and not http.stop.is_set():
                        publish_retest_status(queue, http, "waiting_next_due")
                        http.stop.wait(min(5, http.provider_not_before["cnyes"] - time.time()))
                    if http.stop.is_set():
                        break
                deferred = {p for p in allowed if http.provider_not_before.get(p, 0) > time.time()}
                result.setdefault("initially_deferred_by_provider", {}).update(
                    {p: "persisted/current source deadline; no request before expiry" for p in deferred})
                selected = allowed - set(blocked) - deferred
                if not selected:
                    break
                intervals = {p: s.interval_seconds for p, s in registry.sources.items() if p in selected}
                if "cnyes" in selected:
                    intervals["cnyes"] = 1 / cnyes_rps
                if "fed_board" in selected:
                    intervals["fed_board"] = 1 / fed_rps
                effective = set_intervals(config, registry, http, intervals,
                                          {"cnyes": cnyes_slots, "fed_board": fed_slots})
                metrics = NewsMetrics()
                queue.metrics = http.metrics = metrics
                before_batches = {row[0] for row in queue.db.execute("SELECT path FROM accepted_batches")}
                before_heads = stored_counts(queue)
                phase_started_utc = utc_now()
                phase_started, phase_cpu = time.monotonic(), time.process_time()
                status = run_cycle(queue, http, effective, max_articles=0 if args.cnyes_retest else 512, max_discovery_requests=512,
                                   provider_ids=selected, executor=executor, max_run_seconds=args.phase_seconds,
                                   continuous_budgets=True)
                after_batches = {row[0] for row in queue.db.execute("SELECT path FROM accepted_batches")}
                with metrics.measure("independent_phase_verification"):
                    acceptance = verify_batches(queue, sorted(after_batches - before_batches))
                after_heads = stored_counts(queue)
                wall = time.monotonic() - phase_started
                performance = metrics.snapshot()
                adapter_errors = {r[0]: r[1] for r in queue.db.execute(
                    "SELECT provider,error_code FROM bulk_adapter_cooldowns WHERE not_before>?", (time.time(),))}
                providers = {p: phase_provider_metrics(registry.sources[p], performance["providers"].get(p, {}),
                             before_heads.get(p, 0), after_heads.get(p, 0), wall,
                             http.provider_not_before.get(p, 0) > time.time(), adapter_errors.get(p),
                             http.capacity_snapshot()["providers"][p]) for p in sorted(selected)}
                phase = {"phase": number, "started_at_utc": phase_started_utc, "wall_seconds": wall,
                         "cpu_seconds": time.process_time() - phase_cpu, "acceptance": acceptance,
                         "providers": providers, "cycle": status["cycle"], "performance": performance}
                result["phases"].append(phase)
                for p, item in providers.items():
                    if not item["healthy_for_next_bounded_phase"] or args.cnyes_retest and not repeated_candidate_is_healthy(item):
                        blocked[p] = "source health/cooldown/adapter threshold or no requests; no further escalation"
                if args.cnyes_retest:
                    item = providers.get("cnyes")
                    if item and repeated_candidate_is_healthy(item):
                        item["wall_seconds"] = wall
                        if winner is None or item["new_heads_per_full_workflow_second"] > winner["new_heads_per_full_workflow_second"]:
                            winner = dict(item)
                result["stopped_escalation_by_provider"] = dict(blocked)
                atomic_write_json(args.output, result)
                print(json.dumps({"phase": number, "wall_seconds": wall, "providers": providers,
                                  "verified_body_versions": acceptance["body_versions"]}, ensure_ascii=False), flush=True)
            if args.cnyes_retest and winner and not http.stop.is_set():
                # A failed higher-rate phase cannot be ignored: wait for its
                # actual shared source deadline before trying the lower winner.
                for repeat in range(config["adaptive_rate"].get("stable_windows", 3)):
                    while http.provider_not_before.get("cnyes", 0) > time.time() and not http.stop.is_set():
                        publish_retest_status(queue, http, "waiting_next_due")
                        http.stop.wait(min(5, http.provider_not_before["cnyes"] - time.time()))
                    if http.stop.is_set():
                        break
                    effective = set_intervals(config, registry, http, {"cnyes": winner["client_interval_seconds"]},
                                              {"cnyes": winner["parallelism"]})
                    metrics = NewsMetrics()
                    queue.metrics = http.metrics = metrics
                    before_batches = {r[0] for r in queue.db.execute("SELECT path FROM accepted_batches")}
                    before_heads = stored_counts(queue)
                    phase_started = time.monotonic()
                    status = run_cycle(queue, http, effective, max_articles=0, max_discovery_requests=512,
                                       provider_ids={"cnyes"}, executor=executor, max_run_seconds=args.phase_seconds, continuous_budgets=True)
                    after_batches = {r[0] for r in queue.db.execute("SELECT path FROM accepted_batches")}
                    acceptance = verify_batches(queue, sorted(after_batches - before_batches))
                    wall = time.monotonic() - phase_started
                    after_heads = stored_counts(queue)
                    row = phase_provider_metrics(registry.sources["cnyes"], metrics.snapshot()["providers"].get("cnyes", {}),
                        before_heads.get("cnyes", 0), after_heads.get("cnyes", 0), wall,
                        http.provider_not_before.get("cnyes", 0) > time.time(), None, http.capacity_snapshot()["providers"]["cnyes"])
                    row["wall_seconds"] = wall
                    repeated.append(row)
                    result.setdefault("winner_repeats", []).append({"repeat": repeat + 1, "metrics": row, "acceptance": acceptance})
                    atomic_write_json(args.output, result)
                    print(json.dumps({"repeat": repeat + 1, "winner": row}, ensure_ascii=False), flush=True)
                    if not repeated_candidate_is_healthy(row):
                        break
                result["candidate_promotion"] = accept_retested_candidate(http, config, winner, repeated)
                publish_retest_status(queue, http, "service_duration_complete")
        result["final_acceptance"] = ({"accepted": all(p["acceptance"]["accepted"] for p in result["phases"])
            and all(p["acceptance"]["accepted"] for p in result.get("winner_repeats", [])),
            "verification_scope": "each fixed-rate phase and repeated winner's newly accepted immutable batches; not a whole-lake audit"}
            if args.cnyes_retest else verify_bodies(queue.root))
        fastest = {}
        for phase in result["phases"]:
            for p, item in phase["providers"].items():
                if item["healthy_for_next_bounded_phase"] and (p not in fastest or item["client_interval_seconds"] < fastest[p]["client_interval_seconds"]):
                    fastest[p] = {**item, "phase": phase["phase"]}
        result.update(fastest_healthy_tested=fastest, completed_at_utc=utc_now(),
                      total_wall_seconds=time.monotonic() - started, total_cpu_seconds=time.process_time() - cpu_started,
                      peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        result["accepted"] = bool(result["phases"]) and result["final_acceptance"]["accepted"]
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

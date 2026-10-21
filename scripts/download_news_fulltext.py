"""Private source-compliant news full-text CLI. Existing metadata owners stay intact."""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
from pathlib import Path
import signal
import time
from stockagent.data.news_source_catalog import configuration_revision

from downloader.artifact_io import atomic_write_json, atomic_write_text
from downloader.news_fulltext import fetch_article, load_config, run_cycle, run_service
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_metrics import NewsMetrics
from downloader.news_fulltext_policy import NewsError, normalize_url
from downloader.news_fulltext_queue import NewsQueue, verify_bodies


def selected_urls(args):
    """Stream explicit URLs; the SQLite frontier owns global deduplication."""
    for url in args.url:
        yield normalize_url(url)
    if args.url_file:
        with args.url_file.open(encoding="utf-8-sig") as stream:
            for line in stream:
                line = line.strip()
                if line and not line.startswith("#"):
                    yield normalize_url(line)


def print_status(result, *, compact=False):
    """Keep daemon handoff logs bounded; the canonical status file stays full.

    Explicit one-shot CLI commands retain their detailed output. A continuous
    owner's terminal snapshot can contain every observed publisher and many
    timing samples, so pretty-printing it to journald repeats already stored
    evidence and can exhaust the journal's message budget during restarts.
    """
    if compact:
        providers = result.get("providers")
        value = {key: result.get(key) for key in (
            "activity", "owner_id", "updated_at_utc", "unique_articles",
            "articles_with_stored_body", "http_response_bytes_this_run",
        )}
        value.update(log_contract_version=1, full_status_snapshot="status.json",
                     configured_or_observed_provider_count=len(providers) if isinstance(providers, dict) else None)
    else:
        value = result
    print(json.dumps(value, ensure_ascii=False, indent=None if compact else 2), flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inventory", "run", "enqueue", "fetch", "repair", "status", "verify"))
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--root", type=Path)
    parser.add_argument("--max-articles", type=int, default=64)
    parser.add_argument("--max-discovery-requests", type=int, default=64)
    parser.add_argument("--skip-local-ingest", action="store_true", help="Diagnostic only; normal runs ingest changed accepted metadata")
    parser.add_argument("--output", type=Path, help="Private report for verification, selected fetch or reviewed source outcomes")
    parser.add_argument("--url", action="append", default=[], help="Explicit public article URL; repeat for multiple articles")
    parser.add_argument("--url-file", type=Path, help="UTF-8 file with one article URL per line; # comments allowed")
    parser.add_argument("--refresh", action="store_true", help="Recheck selected saved articles with conditional HTTP")
    parser.add_argument("--reason", help="Bounded review reason for a missing-body repair request; repair only, no HTTP or owner restart")
    parser.add_argument("--reviewed-supplement", type=Path, help="Retained public-source diagnostic report for an explicit finite supplement; requires source opt-in, at most16 direct --url arguments; original cohort unchanged")
    parser.add_argument('--reviewed-native-diagnostics', type=Path,
                        help='Explicit approved native diagnostic manifest; fetch only, 1-16 direct URLs, no HTTP or scope/retry reset')
    parser.add_argument("--materialize-reviewed-response", action="store_true",
                        help="Revalidate and durably accept the immutable reviewed response without a second HTTP request; fetch plus --reviewed-supplement only")
    parser.add_argument("--reviewed-source-outcomes", type=Path, help="Explicit non-body finite metadata correction through retained canonical probes; enqueue only, no HTTP or scope extension")
    parser.add_argument("--continuous", action="store_true", help="Keep one owner and warm bounded workers; sleep only when no source is ready")
    parser.add_argument("--duration-seconds", type=float, help="Bounded continuous diagnostic; normal service has no duration limit")
    args = parser.parse_args(argv)
    if (args.command == 'repair') != (args.reason is not None):
        parser.error('repair requires --reason; --reason belongs to repair')
    if args.command == 'repair' and (args.refresh or args.reviewed_supplement or args.materialize_reviewed_response
            or args.reviewed_native_diagnostics or args.reviewed_source_outcomes or args.skip_local_ingest):
        parser.error('repair submits known missing parser failures under the reviewed source contract only; no refresh, supplement or ingest flags')
    if args.reviewed_supplement and (args.command not in {"enqueue", "fetch"} or args.url_file is not None or not 1 <= len(args.url) <= 16):
        parser.error("reviewed supplement requires enqueue/fetch and one to sixteen direct --url arguments")
    if args.materialize_reviewed_response and (args.command != "fetch" or not args.reviewed_supplement or args.refresh):
        parser.error("retained materialization requires fetch plus explicit reviewed supplement, without refresh")
    if args.reviewed_native_diagnostics and (args.command != 'fetch' or args.url_file is not None
            or not 1 <= len(args.url) <= 16 or args.refresh or args.reviewed_supplement
            or args.materialize_reviewed_response or args.reviewed_source_outcomes):
        parser.error('reviewed native materialization requires fetch and 1-16 direct URLs, without other mutations')
    if args.reviewed_source_outcomes and (args.command != 'enqueue' or args.url_file is not None
            or not 1 <= len(args.url) <= 16 or args.reviewed_supplement or args.refresh):
        parser.error('source outcome review requires enqueue and one to sixteen direct URLs, without refresh or supplement')
    if args.max_articles < 0 or args.max_discovery_requests < 0:
        parser.error("work limits cannot be negative")
    if (args.continuous or args.duration_seconds is not None) and args.command != "run":
        parser.error("continuous service belongs to run")
    if args.duration_seconds is not None and (not args.continuous or not math.isfinite(args.duration_seconds) or args.duration_seconds <= 0):
        parser.error("duration must be positive and used with --continuous")
    if (args.url or args.url_file or args.refresh) and args.command not in {"enqueue", "fetch", "repair"}:
        parser.error("URL selection belongs to enqueue/fetch/repair")
    if args.command in {"enqueue", "fetch", "repair"} and not (args.url or args.url_file):
        parser.error("enqueue/fetch/repair requires --url or --url-file")
    config_revision = configuration_revision(args.config)
    config, registry, repo = load_config(args.config)
    if configuration_revision(args.config) != config_revision:
        raise ValueError("news configuration changed during startup; retry with a coherent snapshot")
    root = args.root or repo / config["root"]
    if args.command == 'repair':
        # Must stay before lifecycle/queue/HTTP construction. A live owner is
        # expected; this command only reads its heads and seals a local inbox.
        from downloader.news_fulltext_repairs import submit_repairs
        from downloader.artifact_io import atomic_write_bytes
        result = submit_repairs(root, registry, selected_urls(args), reason=args.reason)
        if args.output:
            atomic_write_bytes(args.output, json.dumps(result, ensure_ascii=False, indent=2).encode(), durable=True, mode=0o600)
        print_status(result)
        return 0
    if args.command == "status":
        print((root / "status.json").read_text())
        return 0
    if args.command == "verify":
        result = verify_bodies(root)
        if args.output:
            atomic_write_json(args.output, result)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result["accepted"] else 1
    metrics = NewsMetrics()
    from stockagent.data.news_worker_lifecycle import NewsWorkerLifecycle
    lifecycle = NewsWorkerLifecycle(root)
    try:
        with metrics.measure("owner_initialization"):
            queue = NewsQueue(root, registry, metrics=metrics, lifecycle=lifecycle,
                              defer_initial_finite_freeze=args.command in {'enqueue', 'fetch'})
    except BaseException:
        lifecycle.finish(failed=True)
        raise
    http = None
    old_handlers = {}
    configuration_changed = False
    failed = False

    def refresh_metadata():
        nonlocal configuration_changed
        if args.continuous and configuration_revision(args.config) != config_revision:
            configuration_changed = True
            # Receipt recovery can outlive a configuration edit, before any
            # transport exists. Hand off without constructing a stale client.
            if http is not None:
                http.stop.set()  # Between drained cycles; canonical owner restarts with the new policy.
            return
        if args.continuous and http is not None:
            for source in registry.sources.values():
                if source.allowed and source.authorization.get("valid_until"):
                    try:
                        registry.require("https://" + source.domains[0] + "/")
                    except NewsError as error:
                        if error.code == "authorization_expired":
                            configuration_changed = True
                            http.stop.set()  # Restart reevaluates allowed state for all pending URLs.
                            return
        previous = queue.db.execute("SELECT value FROM counters WHERE key='last_metadata_ingest_epoch'").fetchone()
        due = not previous or time.time() - previous[0] >= config["metadata_refresh_seconds"]
        pending = getattr(queue, 'metadata_inventory_pending', False)
        if not args.skip_local_ingest and (args.command == "inventory" or due or pending):
            if not pending:
                queue.status(activity="inventorying")
            with metrics.measure("metadata_inventory"):
                if args.continuous:
                    from downloader.news_fulltext_metadata_ingest import METADATA_SLICE_SECONDS
                    result = queue.ingest_local_slice(repo / config['finlab_root'], repo / config['finmind_root'],
                        maximum_wall_seconds=METADATA_SLICE_SECONDS, stop=http.stop if http else None)
                else:
                    result = {'imported': queue.ingest_local(repo / config['finlab_root'], repo / config['finmind_root']),
                              'complete': True}
            if result['complete']:
                with queue.db:
                    queue.gauge("last_metadata_ingest_epoch", int(time.time()))
            if result['imported'] or result['complete']:
                print(json.dumps({'changed_metadata_partitions_imported': result['imported'],
                                  'metadata_inventory_complete': result['complete']}, ensure_ascii=False), flush=True)
    try:
        from downloader.news_fulltext_failure_focus import configure_focus
        configure_focus(queue, config.get('failure_diagnostic_focus'),
                        metadata_identity_upgrade=config.get('failure_diagnostic_metadata_identity_upgrade', False))
        if args.reviewed_source_outcomes:
            from downloader.news_fulltext_cohorts import record_reviewed_source_outcomes
            result = record_reviewed_source_outcomes(queue, args.url, args.reviewed_source_outcomes)
            if args.output:
                atomic_write_json(args.output, result)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0
        if args.command in {"enqueue", "fetch"}:
            if args.reviewed_native_diagnostics:
                from downloader.news_fulltext_retained_native import materialize_reviewed_native
                replay = materialize_reviewed_native(queue, list(selected_urls(args)),
                    args.reviewed_native_diagnostics, lease_seconds=config['lease_seconds'])
                result = queue.status(accepted_this_run=replay['materialized_bodies'],
                                      activity='reviewed_retained_source_materialized')
                result['reviewed_native_materialization'] = replay
                if args.output:
                    atomic_write_json(args.output, result)
                print(json.dumps(result, ensure_ascii=False, indent=2))
                return 0
            # Explicit links do not trigger a scan of the metadata lake or
            # requests to FinLab/FinMind. They share the same identity/owner.
            urls, selected, submitted, buffer = [], set(), 0, []
            for url in selected_urls(args):
                submitted += 1
                buffer.append({"url": url})
                if len(urls) < args.max_articles and url not in selected:
                    urls.append(url)
                    selected.add(url)
                if len(buffer) >= 5000:
                    with queue.db:
                        queue.add_articles(buffer)
                    buffer = []
            if buffer:
                with queue.db:
                    queue.add_articles(buffer)
            queue.sync_finite_cohorts()
            supplements = []
            if args.reviewed_supplement:
                from downloader.news_fulltext_cohorts import add_reviewed_supplement
                supplements = add_reviewed_supplement(queue, args.url, args.reviewed_supplement)
            if args.materialize_reviewed_response:
                from downloader.news_fulltext_cohorts import materialize_reviewed_supplements
                replay = materialize_reviewed_supplements(queue, urls, lease_seconds=config["lease_seconds"])
                result = queue.status(accepted_this_run=replay["materialized_bodies"],
                                      activity="reviewed_retained_source_materialized")
                result.update(reviewed_retained_materialization=replay, reviewed_finite_supplements=supplements)
                if args.output:
                    atomic_write_json(args.output, result)
                print(json.dumps(result, ensure_ascii=False, indent=2))
                return 0
            attempted, captured, outcomes = 0, 0, []
            if args.command == "fetch":
                http = NewsHTTP(registry, config, root)
                http.metrics = metrics
                for url in urls:
                    row = None
                    try:
                        row = queue.claim_url(url, config["lease_seconds"], refresh=args.refresh)
                        if row is None:
                            continue
                        attempted += 1
                        article = fetch_article(http, row)
                        if article is None:
                            queue.unchanged(row)
                        else:
                            queue.accept([article])
                            captured += 1
                    except NewsError as error:
                        if row:
                            queue.failed(row, error, config["max_attempts"])
                        outcomes.append({"url": url, "outcome": error.code})
            result = queue.status(requests_by_provider=http.requests_by_provider if http else {},
                                  response_bytes=http.response_bytes if http else 0, activity="selected_urls_complete",
                                  provider_cooldowns=http.provider_not_before if http else {})
            result["selected_urls"] = {"submitted_url_lines": submitted, "selected_unique_urls": len(urls),
                                       "attempted": attempted, "captured": captured, "outcomes": outcomes,
                                       "unselected_url_lines": max(0, submitted - len(urls)),
                                       "selection_basis": "first bounded unique URLs fetched; all submitted links enter the shared deduplicated frontier"}
            result["reviewed_finite_supplements"] = supplements
            if args.output:
                atomic_write_json(args.output, result)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0
        refresh_metadata()
        if configuration_changed:
            result = queue.status(activity="configuration_reload_requested")
        elif args.command == "inventory":
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            writer.writerow(("provider", "host", "state", "articles", "permission_basis", "automatic_fulltext_allowed"))
            for provider, host, state, count in queue.db.execute(
                    "SELECT provider,host,state,count(*) FROM articles GROUP BY provider,host,state ORDER BY count(*) DESC"):
                source = registry.sources.get(provider)
                writer.writerow((provider, host, state, count, source.authorization.get("evidence_url", "") if source else "",
                                 bool(source and source.allowed)))
            atomic_write_text(root / "source_inventory.csv", buffer.getvalue())
            result = queue.status(activity="inventory_complete")
        else:
            http = NewsHTTP(registry, config, root)
            http.metrics = metrics
            for signum in (signal.SIGTERM, signal.SIGINT):
                old_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, lambda *_: http.stop.set())
            if args.continuous:
                result = run_service(queue, http, config, max_articles=args.max_articles,
                                     max_discovery_requests=args.max_discovery_requests,
                                     refresh_metadata=refresh_metadata, duration_seconds=args.duration_seconds)
            else:
                result = run_cycle(queue, http, config, max_articles=args.max_articles,
                                   max_discovery_requests=args.max_discovery_requests)
        if configuration_changed:
            result["activity"] = "configuration_reload_requested"
            atomic_write_json(root / "status.json", result, durable=False)
        print_status(result, compact=args.continuous)
        return 75 if configuration_changed else 0
    except BaseException:
        # Recovered in-flight leases remain visible, never forged complete.
        failed = True
        queue.status(activity="interrupted")
        raise
    finally:
        for signum, handler in old_handlers.items():
            signal.signal(signum, handler)
        try:
            if http:
                http.close()
        finally:
            try:
                queue.close()
            finally:
                lifecycle.finish(failed=failed)


if __name__ == "__main__":
    raise SystemExit(main())

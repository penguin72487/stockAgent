"""Pinned read-only coverage projection replay; never open an HTTP owner."""
from __future__ import annotations

import argparse
from collections import Counter
import cProfile
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import pstats
import sqlite3
import statistics
import time
from types import SimpleNamespace
from unittest.mock import patch

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_queue import NewsQueue, summarize_bulk_coverage, summarize_discovery_status, utc_now


def legacy_discovery_status(db, now):
    statements = (
        ('SELECT count(*) FROM discovery WHERE superseded_by_windows_json IS NULL AND due<=?', (now,)),
        ('SELECT count(*) FROM discovery WHERE superseded_by_windows_json IS NULL AND error_code IS NOT NULL', ()),
        ('SELECT count(*) FROM discovery WHERE superseded_by_windows_json IS NOT NULL', ()),
        ('SELECT count(*) FROM discovery WHERE superseded_by_windows_json IS NOT NULL AND error_code IS NOT NULL', ()),
        ("SELECT count(*) FROM discovery WHERE kind IN ('wordpress_posts','cnyes_news','rss_fulltext') "
         'AND superseded_by_windows_json IS NULL AND due<=?', (now,)),
    )
    return tuple(db.execute(statement, params).fetchone()[0] for statement, params in statements)


def profile_current_status(root, registry, repetitions=5, *, compare_discovery_summary=False):
    """Replay the canonical whole heartbeat without an owner or source writes.

    The only publisher is replaced with exact JSON serialization in this
    process. SQLite is read-only, one WAL snapshot and frozen clocks keep
    outputs comparable. No HTTP client or NewsQueue initializer is opened.
    This excludes live metrics locking and filesystem publication latency.
    """
    if not 3 <= repetitions <= 15:
        raise ValueError('bounded_repetition_count_required')
    payload = (root / 'status.json').read_bytes()
    published = json.loads(payload)
    db = sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
        if not owner or owner[0] != published.get('owner_id'):
            raise ValueError('status_database_owner_mismatch')
        view = NewsQueue.__new__(NewsQueue)
        view.root, view.db, view.registry, view.owner = root, db, registry, owner[0]
        view.metrics = SimpleNamespace(snapshot=lambda: deepcopy(published['performance']))
        view.http_capacity_snapshot = lambda: deepcopy(published['http_capacity'])
        view._original_url_started = time.monotonic() - 120
        view._original_url_start_count = 0
        serialized = []

        def serialize_without_publication(path, result, *, durable=False, compact=False):
            if path != root / 'status.json' or durable:
                raise ValueError('unexpected_status_publication')
            serialized.append((json.dumps(result, ensure_ascii=False,
                indent=None if compact else 2, separators=(',', ':') if compact else None) + '\n').encode())

        observed, epoch, monotonic = utc_now(), time.time(), time.monotonic()
        projectors = ({'legacy': legacy_discovery_status, 'summary': summarize_discovery_status}
                      if compare_discovery_summary else {'current': summarize_discovery_status})
        measurements, identities = {name: [] for name in projectors}, {name: set() for name in projectors}
        sizes = {}
        with (patch('downloader.news_fulltext_queue.atomic_write_json', serialize_without_publication),
              patch('downloader.news_fulltext_queue.utc_now', return_value=observed),
              patch('time.time', return_value=epoch), patch('time.monotonic', return_value=monotonic)):
            for number in range(repetitions):
                order = list(projectors) if number % 2 == 0 else list(reversed(projectors))
                for name in order:
                    with patch('downloader.news_fulltext_queue.summarize_discovery_status', projectors[name]):
                        started_wall, started_cpu = time.perf_counter(), time.process_time()
                        view.status(activity='read_only_projection_replay')
                        measurements[name].append({'wall_seconds': time.perf_counter() - started_wall,
                                                   'cpu_seconds': time.process_time() - started_cpu})
                    output = serialized.pop()
                    sizes[name] = len(output)
                    identities[name].add(hashlib.sha256(output).hexdigest())
            profiler = cProfile.Profile(timer=time.process_time)
            queries = {}

            class TimedReads:
                def execute(self, statement, parameters=()):
                    start_wall, start_cpu = time.perf_counter(), time.process_time()
                    cursor = db.execute(statement, parameters)
                    item = queries.setdefault(statement, {'calls': 0, 'wall_seconds': 0, 'cpu_seconds': 0})
                    item['calls'] += 1
                    item['wall_seconds'] += time.perf_counter() - start_wall
                    item['cpu_seconds'] += time.process_time() - start_cpu
                    return cursor

            view.db = TimedReads()
            profiler.runcall(view.status, activity='read_only_projection_replay')
        stats = pstats.Stats(profiler)
        functions = sorted(stats.stats.items(), key=lambda item: -item[1][3])[:20]
        medians = {name: {unit: statistics.median(m[unit] for m in samples)
                         for unit in ('wall_seconds', 'cpu_seconds')} for name, samples in measurements.items()}
        digests = set().union(*identities.values())
        return {'contract_version': 3, 'accepted': len(digests) == 1,
                'observed_at_utc': observed, 'owner_id': owner[0],
                'input_status_sha256': hashlib.sha256(payload).hexdigest(),
                'serialized_status_bytes': sizes,
                'deterministic_projection_sha256': {name: sorted(values) for name, values in identities.items()},
                'measurements': measurements, 'repetitions': repetitions,
                'medians': medians,
                'profiler_time_basis': 'explicit process CPU clock; profiled run excluded from timing medians',
                'speedups': {unit: medians['legacy'][unit] / medians['summary'][unit]
                             for unit in ('wall_seconds', 'cpu_seconds')} if compare_discovery_summary else None,
                'top_cumulative_functions': [{'file': key[0], 'line': key[1], 'function': key[2],
                    'primitive_calls': value[0], 'total_calls': value[1],
                    'self_cpu_seconds': value[2], 'cumulative_cpu_seconds': value[3]}
                    for key, value in functions],
                'top_query_execution_times': [{'statement': statement, **times}
                    for statement, times in sorted(queries.items(), key=lambda item: -item[1]['wall_seconds'])[:12]],
                'publisher_requests': 0, 'source_states_mutated': False,
                'status_published': False, 'all_history_complete': False,
                'scope': 'canonical complete status building and exact JSON serialization on one WAL snapshot; excludes live metrics locks and filesystem publication'}
    finally:
        db.close()


def legacy_projection(db, registry):
    splits = {row[0]: json.loads(row[1]) for row in db.execute("SELECT scope_url,child_urls_json FROM bulk_window_splits")}
    rows = []
    for row in db.execute("SELECT * FROM bulk_api_coverage"):
        item = dict(row)
        source = registry.sources.get(item["provider"])
        item["superseded_by_windows"] = splits.get(item["base_url"])
        item["enabled"] = bool(source and source.allowed and not item["superseded_by_windows"]
                               and item["base_url"].split("?", 1)[0] in (*source.wordpress_apis, *source.cnyes_apis))
        rows.append(item)
    return rows


def benchmark(root, registry, repetitions=5):
    db = sqlite3.connect(f"file:{(root / 'queue.sqlite3').resolve()}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute("BEGIN")
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
        observed = utc_now()
        oracle_rows = legacy_projection(db, registry)
        actual_rows = summarize_bulk_coverage(db, registry)
        expected = {}
        for row in oracle_rows:
            key = (row["provider"], row["base_url"].split("?", 1)[0])
            item = expected.setdefault(key, Counter(coverage_scopes=0, leaf_scopes=0, superseded_scopes=0, total_articles=0, total_pages=0, enabled=False))
            item["coverage_scopes"] += 1
            superseded = bool(row["superseded_by_windows"])
            item["superseded_scopes"] += superseded
            item["leaf_scopes"] += not superseded
            if not superseded:
                item["total_articles"] += row["total_articles"]
                item["total_pages"] += row["total_pages"]
            item["enabled"] = bool(item["enabled"] or row["enabled"])
            item["observed_at_utc"] = max(item.get("observed_at_utc", ""), row["observed_at_utc"])
        actual = {(row["provider"], row["base_url"]): {key: value for key, value in row.items() if key not in {"provider", "base_url"}}
                  for row in actual_rows}
        measurements = {"legacy": [], "summary": []}
        sizes, identities = {}, {}
        for number in range(repetitions):
            for name in (("legacy", "summary") if number % 2 == 0 else ("summary", "legacy")):
                wall, cpu = time.perf_counter(), time.process_time()
                rows = legacy_projection(db, registry) if name == "legacy" else summarize_bulk_coverage(db, registry)
                serialized = json.dumps(rows, ensure_ascii=False, indent=2).encode()
                measurements[name].append({"wall_seconds": time.perf_counter() - wall, "cpu_seconds": time.process_time() - cpu})
                sizes[name] = len(serialized)
                identities[name] = hashlib.sha256(serialized).hexdigest()
        medians = {name: {unit: statistics.median(row[unit] for row in samples) for unit in ("wall_seconds", "cpu_seconds")}
                   for name, samples in measurements.items()}
        return {"contract_version": 2, "accepted": dict(expected) == actual,
                "observed_at_utc": observed, "owner_id": owner[0] if owner else None,
                "coverage_scopes": len(oracle_rows), "source_endpoints": len(actual_rows),
                "all_scope_totals_preserved": dict(expected) == actual,
                "serialized_projection_bytes": sizes, "projection_sha256": identities,
                "medians": medians, "measurements": measurements, "repetitions": repetitions,
                "scope": "one WAL snapshot; coverage projection and JSON serialization only, not overall CPU or throughput",
                "publish": False, "all_history_complete": False}
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument('--profile-current-status', action='store_true')
    parser.add_argument('--compare-discovery-summary', action='store_true')
    args = parser.parse_args(argv)
    if not 3 <= args.repeat <= 15:
        parser.error("bounded repetition count required")
    if args.compare_discovery_summary and not args.profile_current_status:
        parser.error('--compare-discovery-summary requires --profile-current-status')
    config, registry, repo = load_config(args.config)
    result = (profile_current_status(repo / config['root'], registry, args.repeat,
                                    compare_discovery_summary=args.compare_discovery_summary)
              if args.profile_current_status else benchmark(repo / config["root"], registry, args.repeat))
    result["implementation_sha256"] = {str(path): sha256_file(path) for path in (Path(__file__), Path("downloader/news_fulltext_queue.py"))}
    atomic_write_json(args.output, result)
    print(json.dumps({key: result[key] for key in ("accepted", "coverage_scopes", "source_endpoints", "serialized_projection_bytes", "serialized_status_bytes", "medians", "scope") if key in result}))
    return 0 if result["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

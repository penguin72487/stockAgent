"""Same owned full acquisition with the pinned old and current work selectors.

Continuous ordinary rechecks are an explicit stress fixture, not production
cooldown resets. Only owned loopback is requested. Includes native transport,
parsing, source clocks/authors/raw hashes, durable batches and every receipt.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import statistics
import tempfile
import time
from urllib.parse import urlsplit

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, utc_now, verify_bodies
from scripts.benchmark_cnyes_acquisition import verify_batches
from scripts.benchmark_news_connection_recovery import (
    HOSTS, OwnedLimiter, OwnedScopedSession, owned_config, owned_servers,
)


STAGE = Path('artifacts/data_quality/news_goal_stage01_20261009/remaining_v83')
SIGNATURE_FIELDS = ('requested_url', 'final_url', 'title', 'body_sha256',
                    'response_sha256', 'published_at_raw', 'authors_json', 'data_fingerprint')


def pinned_reference():
    before = json.loads((STAGE / 'before.json').read_bytes())
    entry = before['reference_files']['downloader/news_fulltext_queue.py']
    path = Path(entry['reference'])
    if sha256_file(path) != entry['sha256']:
        raise ValueError('original_selector_snapshot_changed')
    spec = importlib.util.spec_from_file_location('owned_original_news_queue', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.NewsQueue, entry['sha256']


def phase(queue_class, *, reference, missing_burst, stored_burst):
    bound = (missing_burst + 1) * (stored_burst + 1)
    config = owned_config()
    with tempfile.TemporaryDirectory(prefix='news-owned-stored-fairness-') as directory, owned_servers() as servers:
        root = Path(directory)
        started, cpu_started = time.monotonic(), time.process_time()
        registry = PolicyRegistry(config, base=root)
        queue = queue_class(root, registry)

        def address_check(url):
            if urlsplit(url).hostname not in HOSTS:
                raise ValueError('external_fixture_address_forbidden')

        http = NewsHTTP(registry, config, root,
            session_factory=lambda: OwnedScopedSession(servers, observe_connections=True),
            limiter_factory=OwnedLimiter, address_check=address_check, metrics=queue.metrics)
        base = 'https://publisher.test'
        old, fresh = base + '/old', base + '/fresh'
        selected, records = [], []
        try:
            with queue.db:
                queue.add_articles([{'url': old, 'date': '2014-01-01'},
                                    {'url': fresh, 'date': '2026-10-11'}]
                    + [{'url': base + f'/missing/{i}', 'date': '2013-01-01'} for i in range(bound)])
            for url in (old, fresh):
                records.append(fetch_article(http, queue.claim_url(url, 120)))
            queue.accept(records)
            records = []
            # Already-due historical failure and ready ordinary refresh fixture.
            # Capture clocks still come from real owned HTTP, never backdated.
            with queue.db:
                queue.db.execute("UPDATE articles SET state='retry',error_code='transport_error',"
                    'due=? WHERE url=?', (time.time() - 3600, old))
                queue.db.execute("UPDATE articles SET state='retry',error_code=NULL,due=0 WHERE url=?", (fresh,))
            original = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (old,)).fetchone())
            for step in range(bound):
                kwargs = {'missing_body_burst': missing_burst}
                if not reference:
                    kwargs['stored_body_retry_burst'] = stored_burst
                row = queue.claim('publisher', 120, **kwargs)
                selected.append({'step': step + 1, 'url': row['url'], 'stored': row['body_sha256'] is not None})
                records.append(fetch_article(http, row))
                # Same batch boundaries in both phases; a recheck remains
                # inflight until its complete raw/body receipt is committed.
                if row['body_sha256'] is not None:
                    queue.accept(records)
                    records = []
                    if row['url'] == fresh:
                        with queue.db:
                            queue.db.execute("UPDATE articles SET state='retry',error_code=NULL,due=0 WHERE url=?", (fresh,))
            queue.accept(records)
            after = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (old,)).fetchone())
            paths = [row[0] for row in queue.db.execute('SELECT path FROM accepted_batches ORDER BY path')]
            all_versions = verify_batches(queue, paths, allow_superseded_heads=True)
            proof = verify_bodies(root)
            heads = {}
            for path in paths:
                for record in pq.read_table(root / path).to_pylist():
                    heads[record['requested_url']] = {key: record[key] for key in SIGNATURE_FIELDS}
            signature = hashlib.sha256(json.dumps(sorted(heads.values(), key=lambda r: r['requested_url']),
                sort_keys=True).encode()).hexdigest()
            stored_slots = [row['step'] for row in selected if row['stored']]
            old_steps = [row['step'] for row in selected if row['url'] == old]
            unchanged_failure = all(after[key] == original[key] for key in (
                'state', 'due', 'attempts', 'error_code', 'body_sha256', 'captured_at_utc', 'last_checked_at_utc'))
            recovered = (after['state'] == 'complete' and after['error_code'] is None
                and after['attempts'] == original['attempts'] + 1
                and after['captured_at_utc'] > original['captured_at_utc']
                and after['body_sha256'] == original['body_sha256'])
            accepted = (proof['accepted'] and all_versions['accepted']
                and all_versions['body_versions'] == bound + 2
                and stored_slots == list(range(missing_burst + 1, bound + 1, missing_burst + 1))
                and (not old_steps and unchanged_failure if reference else old_steps == [bound] and recovered))
            return {'reference': reference, 'accepted': bool(accepted),
                'wall_seconds': time.monotonic() - started, 'cpu_seconds': time.process_time() - cpu_started,
                'article_dispatches': bound, 'stored_slots': stored_slots, 'old_failure_selected_at': old_steps,
                'old_failure_recovered': recovered, 'original_failure_unchanged': unchanged_failure,
                'semantic_head_signature_sha256': signature, 'distinct_heads': len(heads),
                'requests': {host: len(server.paths) for host, server in servers.items()},
                'tcp_connections': {host: server.connections for host, server in servers.items()},
                'body_receipt_proof': proof, 'all_immutable_version_proof': all_versions,
                'dispatch_counts': {key: value for key, value in queue.metrics.snapshot()['providers']['publisher']['counts'].items()
                    if key.startswith('frontier_')},
                'captured_clock_is_actual_http_not_fixture_publication_clock': True}
        finally:
            http.close()
            queue.close()


def benchmark(*, missing_burst=8, stored_burst=8, cycles=2):
    if any(type(value) is not int or not 1 <= value <= 8 for value in (missing_burst, stored_burst)):
        raise ValueError('bounded_owned_bursts_required')
    if type(cycles) is not int or not 1 <= cycles <= 2:
        raise ValueError('bounded_owned_cycles_required')
    original, original_sha = pinned_reference()
    phases = [phase(original if reference else NewsQueue, reference=reference,
                    missing_burst=missing_burst, stored_burst=stored_burst)
              for _ in range(cycles) for reference in (True, False, False, True)]
    accepted = all(row['accepted'] and all(row[key] == phases[0][key] for key in (
        'requests', 'tcp_connections', 'semantic_head_signature_sha256', 'distinct_heads', 'stored_slots')) for row in phases)
    return {'contract_version': 1, 'accepted': bool(accepted), 'verified_at_utc': utc_now(),
        'scope': 'same_owned_source_ABBA_selector_only_full_native_capture_parser_fsync_receipts_and_verification',
        'external_source_requests': 0, 'production_queue_writes': False, 'production_rate_state_writes': False,
        'official_limit_proved': False, 'publisher_speedup_claimed': False,
        'same_source_fields_verified': list(SIGNATURE_FIELDS),
        'intentional_difference': 'old due stored failure receives an already awarded stored recheck slot; real capture/check clocks advance',
        'missing_body_burst': missing_burst, 'stored_body_retry_burst': stored_burst,
        'oldest_ready_failure_selection_bound_article_dispatches': (missing_burst + 1) * (stored_burst + 1),
        'bound_is_wall_clock_guarantee': False, 'reference_queue_sha256': original_sha,
        'phases': phases,
        'summary': {label: {'median_wall_seconds': statistics.median(r['wall_seconds'] for r in phases if r['reference'] == reference),
                           'median_cpu_seconds': statistics.median(r['cpu_seconds'] for r in phases if r['reference'] == reference),
                           'wall_samples': [r['wall_seconds'] for r in phases if r['reference'] == reference],
                           'cpu_samples': [r['cpu_seconds'] for r in phases if r['reference'] == reference]}
                    for label, reference in (('reference', True), ('candidate', False))},
        'implementation_sha256': {str(path): sha256_file(path) for path in (
            Path(__file__), *sorted(Path('downloader').glob('news_fulltext*.py')))}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cycles', type=int, default=2)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    result = benchmark(cycles=args.cycles)
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print(json.dumps({key: result[key] for key in ('accepted', 'summary', 'oldest_ready_failure_selection_bound_article_dispatches')}))
    return int(not result['accepted'])


if __name__ == '__main__':
    raise SystemExit(main())

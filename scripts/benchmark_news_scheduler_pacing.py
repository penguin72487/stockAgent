"""Private owned-loopback comparison; no publisher or production data access."""
from __future__ import annotations

import argparse
from contextlib import closing
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import statistics
import tempfile
import threading
import time
from urllib.parse import urlsplit

import pyarrow.parquet as pq
from protego import Protego

from downloader import news_fulltext as owner
from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, utc_now, verify_bodies
from scripts.benchmark_news_connection_recovery import (
    HOSTS, OwnedLimiter, OwnedScopedSession, owned_config, owned_servers,
)


def phase(servers, case, reference):
    config = owned_config()
    due_cases = {'future_due', 'provider_batch_due'}
    batch_cases = {'provider_batch_due', 'provider_batch_burst'}
    config.update(workers=2 if case in {'multi_idle_burst', 'future_due', 'future_idle_burst', *batch_cases} else 1,
                  batch_articles=100, batch_flush_seconds=30,
                  response_deadline_seconds=3)
    config['providers'][0].update(interval_seconds=(.6 if case == 'fairness' else .001 if case in due_cases else .05),
                                  parallelism=1)
    config['providers'][1].update(interval_seconds=.001, parallelism=1)
    if case in batch_cases:
        for number in range(56):
            idle = deepcopy(config['providers'][0])
            idle.update(id=f'idle_{number:03}', domains=[f'idle{number:03}.test'])
            config['providers'].append(idle)
    if case in {'fairness', *due_cases}:
        urls = ['https://publisher.test/history/0', 'https://other.test/history/0']
    else:
        if case == 'burst':
            config['providers'] = config['providers'][:1]
        urls = [f'https://publisher.test/history/{number}' for number in range(8)]
    with tempfile.TemporaryDirectory(prefix='owned-news-pacing-', dir='/var/tmp') as directory:
        root = Path(directory)
        registry = PolicyRegistry(config, base=root)
        starts, lock = [], threading.Lock()
        other_started, slow_saw_other, due_monotonic = threading.Event(), [], []

        class Session(OwnedScopedSession):
            def get(self, url, **kwargs):
                if urlsplit(url).hostname not in HOSTS:
                    raise ValueError('only_owned_fixture_requests_allowed')
                with lock:
                    starts.append((url, time.monotonic()))
                if case in due_cases:
                    if urlsplit(url).hostname == 'publisher.test':
                        slow_saw_other.append(other_started.wait(1.2))
                    else:
                        other_started.set()
                return super().get(url, **kwargs)

        def check_address(url):
            if urlsplit(url).hostname not in HOSTS:
                raise ValueError('only_owned_fixture_requests_allowed')

        with closing(NewsQueue(root, registry)) as queue, closing(NewsHTTP(
                registry, config, root, session_factory=lambda: Session(servers),
                limiter_factory=OwnedLimiter, address_check=check_address, metrics=queue.metrics)) as http:
            parser = Protego.parse('User-agent: *\nAllow: /')
            for source in config['providers']:
                for domain in source['domains']:
                    http.robots_parsers['https://' + domain] = (time.time(), parser)
            with queue.db:
                queue.add_articles([{'url': url} for url in urls])
                if case in due_cases:
                    queue.db.execute('UPDATE articles SET due=? WHERE url=?', (time.time() + 60, urls[1]))
            if case in due_cases:
                claim = queue.claim

                def on_claim(provider, *args, **kwargs):
                    row = claim(provider, *args, **kwargs)
                    if provider == 'publisher' and row and not due_monotonic:
                        due_monotonic.append(time.monotonic() + .25)
                        with queue.db:
                            queue.db.execute('UPDATE articles SET due=? WHERE url=?', (time.time() + .25, urls[1]))
                    return row

                queue.claim = on_claim
            if reference and case not in {'future_due', 'future_idle_burst', *batch_cases}:
                http.scheduling_cooldown_snapshot = http.cooldown_snapshot
            if reference and case in batch_cases:
                # Baseline the original three indexed statements per source;
                # all other scheduler, HTTP, parser and receipt paths match.
                def old_article_deadlines(providers, dispatch_after):
                    deadlines = {}
                    for provider in providers:
                        for state in ('pending', 'retry', 'retry_cooldown'):
                            after = ' AND due>?' if dispatch_after is not None else ''
                            binds = (provider, state, dispatch_after) if dispatch_after is not None else (provider, state)
                            row = queue.db.execute('SELECT due FROM articles INDEXED BY dispatch '
                                'WHERE provider=? AND state=?' + after + ' ORDER BY due LIMIT 1', binds).fetchone()
                            if row and (provider not in deadlines or row[0] < deadlines[provider]):
                                deadlines[provider] = row[0]
                    return deadlines
                queue._unscoped_article_wake_deadlines = old_article_deadlines
            accepts, status_count, wake_count, evaluated_wakes = [], [0], [0], [0]
            accept, status, next_wake = queue.accept, queue.status, queue.next_wake_delay

            def on_accept(records, **kwargs):
                result = accept(records, **kwargs)
                accepts.append(len(records))
                if sum(accepts) == len(urls):
                    http.stop.set()
                return result

            def on_status(**kwargs):
                status_count[0] += 1
                return status(**kwargs)

            def on_next_wake(*args, **kwargs):
                wake_count[0] += 1
                if reference and case in {'future_due', 'future_idle_burst'} and kwargs.get('dispatch_after') is not None:
                    # Compare the new in-flight deadline scan alone. The rest
                    # of the current scheduler and request policy is identical.
                    return args[1]
                evaluated_wakes[0] += 1
                return next_wake(*args, **kwargs)

            queue.accept, queue.status, queue.next_wake_delay = on_accept, on_status, on_next_wake
            previous_send = time.monotonic()
            if case == 'fairness':
                http.last_calls['publisher'] = previous_send
            started, cpu_start = time.monotonic(), time.process_time()
            owner.run_service(queue, http, config, max_articles=1, max_discovery_requests=0, duration_seconds=8)
            wall, cpu = time.monotonic() - started, time.process_time() - cpu_start
            verification = verify_bodies(root)
            records = []
            for path in sorted((root / 'bodies').rglob('*.parquet')):
                for record in pq.read_table(path).to_pylist():
                    records.append({key: record[key] for key in ('requested_url', 'final_url', 'title',
                        'body_sha256', 'response_sha256', 'published_at_raw', 'authors_json', 'data_fingerprint')})
            records.sort(key=lambda row: row['requested_url'])
            rows = [dict(row) for row in queue.db.execute('SELECT url,state,attempts,body_sha256 FROM articles ORDER BY url')]
            publisher_times = [stamp for url, stamp in starts if urlsplit(url).hostname == 'publisher.test']
            gaps = ([publisher_times[0] - previous_send] if case == 'fairness' else [])
            gaps += [later - earlier for earlier, later in zip(publisher_times, publisher_times[1:])]
            interval = config['providers'][0]['interval_seconds']
            accepted = (verification['accepted'] and len(records) == len(urls) and len(starts) == len(urls)
                        and all(row['state'] == 'complete' and row['body_sha256'] and row['attempts'] == 1 for row in rows)
                        and all(gap >= interval - .005 for gap in gaps) and sum(accepts) == len(urls))
            if case in due_cases:
                # Both variants may start the second source during a 1.2 s
                # response: the reference already has a one-second poll.
                # Treat timing as a measurement, not an acceptance premise.
                accepted &= len(slow_saw_other) == 1 and starts[1][1] >= due_monotonic[0] - .01
            return {'case': case, 'reference': reference, 'accepted': accepted,
                    'wall_seconds': wall, 'cpu_seconds': cpu, 'status_publications': status_count[0],
                    'wake_queries': wake_count[0], 'evaluated_wake_queries': evaluated_wakes[0],
                    'batch_sizes': accepts, 'requests': dict(http.requests_by_provider),
                    'body_receipts_verified': verification['accepted'],
                    'semantic_signature_sha256': hashlib.sha256(json.dumps(records, sort_keys=True).encode()).hexdigest(),
                    'start_order': [urlsplit(url).hostname for url, _ in starts],
                    'slow_saw_other': slow_saw_other,
                    'other_after_due_seconds': starts[1][1] - due_monotonic[0] if case in due_cases else None,
                    'starts_after_service_seconds': {url: stamp - started for url, stamp in starts},
                    'minimum_publisher_gap_seconds': min(gaps) if gaps else None}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    choices = ('fairness', 'burst', 'multi_idle_burst', 'future_due', 'future_idle_burst',
               'provider_batch_due', 'provider_batch_burst')
    parser.add_argument('--cases', nargs='+', choices=choices, default=choices)
    args = parser.parse_args(argv)
    # Only private synthetic fixtures: the real physical probe cannot run
    # inside an isolated network namespace and production admission is untouched.
    original_admission = owner.refresh_disk_admission
    try:
        owner.refresh_disk_admission = lambda *_: False
        with owned_servers() as servers:
            phases = [phase(servers, case, reference) for case in args.cases
                      for reference in (True, False, False, True, False, True, True, False)]
    finally:
        owner.refresh_disk_admission = original_admission
    summary = {}
    accepted = True
    for case in args.cases:
        group = [row for row in phases if row['case'] == case]
        accepted &= all(row['accepted'] and row['semantic_signature_sha256'] == group[0]['semantic_signature_sha256']
                        and row['batch_sizes'] == [2 if case in {'fairness', 'future_due', 'provider_batch_due'} else 8]
                        for row in group)
        if case == 'fairness':
            accepted &= all(row['start_order'] == (['publisher.test', 'other.test'] if row['reference']
                                                    else ['other.test', 'publisher.test']) for row in group)
        summary[case] = {}
        for label, reference in (('reference', True), ('candidate', False)):
            selected = [row for row in group if row['reference'] == reference]
            summary[case][label] = {
                'median_wall_seconds': statistics.median(row['wall_seconds'] for row in selected),
                'median_cpu_seconds': statistics.median(row['cpu_seconds'] for row in selected),
                'wall_samples': [row['wall_seconds'] for row in selected],
                'cpu_samples': [row['cpu_seconds'] for row in selected],
                'status_publications': [row['status_publications'] for row in selected],
                'wake_queries': [row['wake_queries'] for row in selected],
                'evaluated_wake_queries': [row['evaluated_wake_queries'] for row in selected],
                'other_after_due_samples': [row['other_after_due_seconds'] for row in selected],
                'other_start_samples': [row['starts_after_service_seconds'].get('https://other.test/history/0') for row in selected],
            }
    source_paths = [Path(__file__), *sorted(Path('downloader').glob('news_fulltext*.py')),
                    Path('scripts/benchmark_news_connection_recovery.py')]
    result = {'contract_version': 1, 'verified_at_utc': utc_now(), 'accepted': accepted,
              'scope': 'owned_loopback_resident_owner_parser_raw_receipts_ABBA_BAAB',
              'external_publisher_requests': 0, 'production_queue_writes': False,
              'synthetic_disk_admission': 'fixture_only_override',
              'official_limit_or_publisher_speed_proved': False, 'phases': phases, 'summary': summary,
              'implementation_sha256': {str(path): sha256_file(path) for path in source_paths}}
    output = args.output
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(output, result)
    print(json.dumps({'accepted': accepted, 'summary': summary, 'output': str(output)}, ensure_ascii=False))
    return 0 if accepted else 1


if __name__ == '__main__':
    raise SystemExit(main())


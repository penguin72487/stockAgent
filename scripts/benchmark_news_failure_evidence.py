"""Bounded owned-TCP full-chain comparison of private failure evidence budgets.

Same synthetic source responses, parser, failure states and healthy receipts.
No publisher requests, production writes or claimed publisher rate maximum.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from copy import deepcopy
import hashlib
from http.server import ThreadingHTTPServer
import importlib.util
import json
from pathlib import Path
import statistics
import sys
import tempfile
import threading
import time
from urllib.parse import urlsplit

import pyarrow.parquet as pq

from downloader import news_fulltext_diagnostics as diagnostics
from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, utc_now, verify_bodies
from scripts.benchmark_cnyes_acquisition import verify_batches
from scripts.benchmark_news_connection_recovery import Handler, OwnedLimiter, OwnedScopedSession, owned_config


STAGE = Path('artifacts/data_quality/news_goal_stage01_20261009/remaining_v84')
HOSTS = ('m.cnyes.com', 'other.test')
SIGNATURE_FIELDS = ('requested_url', 'final_url', 'title', 'body_sha256',
                    'response_sha256', 'published_at_raw', 'authors_json', 'data_fingerprint')


def fixtures():
    # Reuse the maintained source-contract generator. It is explicitly owned
    # synthetic data, not a saved publisher response or a publication clock.
    test_path = str(Path('test').resolve())
    sys.path.insert(0, test_path)
    try:
        from test_news_cnyes_canonical_alias import mobile_page
        visible = mobile_page(visible='Owned visible text differs from current data.')
        schema = mobile_page(schema_changes={'articleBody': 'Owned schema differs from the visible text.'})
        healthy = mobile_page()
    finally:
        sys.path.remove(test_path)
    def identity(payload, number):
        return payload.replace(b'/42', ('/' + str(number)).encode()).replace(
            b'"newsId": 42', ('"newsId": ' + str(number)).encode()).replace(
            b'newsId\\": 42', ('newsId\\": ' + str(number)).encode())
    return {'/news/id/42': visible, '/news/id/43': identity(schema, 43),
            **{f'/news/id/{i}': identity(healthy, i) for i in range(50, 58)}}


class EvidenceHandler(Handler):
    def do_GET(self):
        with self.server.observation_lock:
            self.server.paths.append(self.path)
        if self.path == '/robots.txt':
            payload, mime = b'User-agent: *\nAllow: /', 'text/plain'
        else:
            payload, mime = self.server.payloads.get(self.path, b'<html>Owned missing body</html>'), 'text/html'
        self.send_response(200)
        self.send_header('Content-Type', mime)
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


@contextmanager
def servers():
    values, threads = {}, []
    try:
        payloads = fixtures()
        for host in HOSTS:
            server = ThreadingHTTPServer(('127.0.0.1', 0), EvidenceHandler)
            server.daemon_threads = True
            server.observation_lock = threading.Lock()
            server.connections, server.paths = 0, []
            server.payloads = payloads if host == HOSTS[0] else {}
            thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .02}, daemon=True)
            thread.start()
            values[host] = server
            threads.append(thread)
        yield values
    finally:
        for server in values.values():
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join(timeout=2)


def pinned_reference(snapshot=None):
    before = json.loads((snapshot or STAGE / 'before.json').read_bytes())
    entry = before['reference_files']['downloader/news_fulltext_diagnostics.py']
    path = Path(entry['reference'])
    if sha256_file(path) != entry['sha256']:
        raise ValueError('original_diagnostic_snapshot_changed')
    spec = importlib.util.spec_from_file_location('owned_original_failure_diagnostics', path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module, entry['sha256']


def phase(original, *, reference):
    raw_config, _, _ = load_config(Path('configs/news_fulltext.json'))
    config = owned_config()
    native = deepcopy(next(p for p in raw_config['providers'] if p['id'] == 'cnyes'))
    native.update(feeds=[], sitemaps=[], cnyes_apis=[], wordpress_apis=[], index_pages=[],
                  interval_seconds=.001, domains=['news.cnyes.com', 'm.cnyes.com'])
    native.pop('cnyes_endpoint_inventory', None)
    config['providers'] = [native, config['providers'][1]]
    config['workers'] = 16
    with tempfile.TemporaryDirectory(prefix='news-owned-failure-evidence-') as directory, servers() as owned:
        root = Path(directory)
        started, cpu_started = time.monotonic(), time.process_time()
        registry = PolicyRegistry(config, base=root)
        queue = NewsQueue(root, registry)
        def address_check(url):
            if urlsplit(url).hostname not in HOSTS:
                raise ValueError('external_fixture_address_forbidden')
        http = NewsHTTP(registry, config, root, metrics=queue.metrics,
            session_factory=lambda: OwnedScopedSession(owned, observe_connections=True),
            address_check=address_check, limiter_factory=OwnedLimiter)
        saved_sink = diagnostics.retain_failure_response
        failures = []
        def source_observation(url, *, legacy=False):
            with queue.db:
                queue.add_articles([{'url': url}])
                # Repeated failure observations are a bounded owned fixture,
                # never a reset of a production retry or source cooldown.
                queue.db.execute('UPDATE articles SET due=0 WHERE url=?', (url,))
            row = queue.claim_url(url, 120, refresh=True)
            try:
                queue.accept([fetch_article(http, row)])
            except NewsError as error:
                if error.code != 'body_not_found':
                    raise
                sink = original if legacy or reference else diagnostics
                diagnostics.retain_failure_response = sink.retain_failure_response
                context = error.failure_response
                if sink is original:
                    error.failure_response = original.FailureResponse(context.response, context.provider,
                        context.source_fingerprint, context.requested_url, context.received_at_utc)
                try:
                    queue.failed(row, error, 6)
                finally:
                    diagnostics.retain_failure_response = saved_sink
                failures.append({'url': url, 'error': error.code,
                    'stage': getattr(error, 'cnyes_native_body_failure_stage', None)})
        try:
            visible, schema = (f'https://m.cnyes.com/news/id/{i}' for i in (42, 43))
            for _ in range(2):
                source_observation(visible, legacy=True)
            for number in range(3):
                source_observation(schema)
                source_observation(visible)
                source_observation(f'https://m.cnyes.com/news/id/{50 + number}')
                source_observation('https://other.test/missing')
            for number in range(53, 58):
                source_observation(f'https://m.cnyes.com/news/id/{number}')
            # Persisted evidence reservations, receipts and healthy heads must
            # survive owner reconstruction before another source observation.
            queue.close()
            queue = NewsQueue(root, registry)
            for _ in range(3):
                source_observation(schema)
            batch_paths = [r[0] for r in queue.db.execute('SELECT path FROM accepted_batches ORDER BY path')]
            all_versions = verify_batches(queue, batch_paths)
            bodies = verify_bodies(root)
            heads = [({key: record[key] for key in SIGNATURE_FIELDS}) for path in batch_paths
                     for record in pq.read_table(root / path).to_pylist()]
            signature = hashlib.sha256(json.dumps(sorted(heads, key=lambda r: r['requested_url']),
                sort_keys=True).encode()).hexdigest()
            evidence = []
            for path in sorted((root / 'diagnostics').rglob('*.parquet')):
                receipt = json.loads(path.with_suffix('.receipt.json').read_bytes())
                record = pq.read_table(path).to_pylist()[0]
                raw = record['raw_response_body']
                if (sha256_file(path) != receipt['sha256'] or not record['diagnostic_only']
                        or record['accepted_as_article'] or record['publish'] or record['remote_training_export']
                        or raw is not None and hashlib.sha256(raw).hexdigest() != record['response_sha256']):
                    raise ValueError('private_diagnostic_receipt_invalid')
                evidence.append({'provider': record['provider'], 'stage': record['native_body_failure_stage'],
                                 'raw_retained': raw is not None, 'budget_period_kind': record.get('budget_period_kind'),
                                 'budget_attempt_limit': record.get('budget_attempt_limit')})
            states = [dict(r) for r in queue.db.execute('SELECT url,state,error_code,attempts,body_sha256 '
                'FROM articles WHERE error_code IS NOT NULL ORDER BY url')]
            native_rows = [r for r in evidence if r['provider'] == 'cnyes']
            other_rows = [r for r in evidence if r['provider'] == 'other']
            module = original if reference else diagnostics
            version = getattr(module, 'FAILURE_DIAGNOSTIC_BUDGET_CONTRACT_VERSION', 1)
            expected = 2 if version < 2 else 4
            if 'cnyes' in getattr(module, 'HOURLY_EVIDENCE_PROVIDERS', ()):
                expected += 2 * module.HOURLY_BUCKET_ATTEMPTS
            accepted = (bodies['accepted'] and all_versions['accepted'] and len(heads) == 8
                and len(native_rows) == expected and all(r['raw_retained'] for r in native_rows)
                and len(other_rows) == 2 and not any(r['raw_retained'] for r in other_rows)
                and all(r['state'] in {'retry', 'retry_cooldown', 'body_not_found'}
                    and r['error_code'] == 'body_not_found' and r['body_sha256'] is None for r in states)
                and {r['stage'] for r in native_rows} == ({'visible_current_mismatch'} if version < 2 else
                    {'visible_current_mismatch', 'schema_mismatch_without_verified_figure_view'}))
            return {'reference': reference, 'accepted': bool(accepted),
                'wall_seconds': time.monotonic() - started, 'cpu_seconds': time.process_time() - cpu_started,
                'requests': {host: len(server.paths) for host, server in owned.items()},
                'failures': failures, 'source_failure_states': states, 'diagnostics': evidence,
                'healthy_signature_sha256': signature, 'healthy_body_versions': len(heads),
                'all_immutable_version_proof': all_versions, 'body_receipt_proof': bodies,
                'reservations_survive_reconstruction': True, 'source_failures_accepted_as_articles': False}
        finally:
            diagnostics.retain_failure_response = saved_sink
            http.close()
            queue.close()


def benchmark(snapshot=None):
    original, reference_sha = pinned_reference(snapshot)
    phases = [phase(original, reference=old) for old in (True, False, False, True)]
    accepted = all(row['accepted'] and all(row[key] == phases[0][key] for key in (
        'requests', 'failures', 'source_failure_states', 'healthy_signature_sha256', 'healthy_body_versions')) for row in phases)
    return {'contract_version': 2, 'accepted': bool(accepted), 'verified_at_utc': utc_now(),
        'scope': 'same_owned_TCP_full_capture_parser_failure_commit_private_evidence_fsync_healthy_receipts_recovery_and_verification',
        'external_source_requests': 0, 'production_queue_writes': False, 'official_limit_proved': False,
        'publisher_speedup_claimed': False, 'intentional_difference': 'previously starved known failure class obtains bounded private evidence',
        'reference_diagnostic_sha256': reference_sha,
        'reference_budget_contract_version': getattr(original, 'FAILURE_DIAGNOSTIC_BUDGET_CONTRACT_VERSION', 1),
        'candidate_budget_contract_version': diagnostics.FAILURE_DIAGNOSTIC_BUDGET_CONTRACT_VERSION, 'phases': phases,
        'summary': {name: {'median_wall_seconds': statistics.median(r['wall_seconds'] for r in phases if r['reference'] == old),
                           'median_cpu_seconds': statistics.median(r['cpu_seconds'] for r in phases if r['reference'] == old),
                           'wall_samples': [r['wall_seconds'] for r in phases if r['reference'] == old],
                           'cpu_samples': [r['cpu_seconds'] for r in phases if r['reference'] == old]}
                    for name, old in (('reference', True), ('candidate', False))},
        'implementation_sha256': {str(p): sha256_file(p) for p in (Path(__file__),
            Path('downloader/news_fulltext_diagnostics.py'), Path('downloader/news_fulltext_queue.py'),
            Path('downloader/news_fulltext_capacity.py'))}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--reference-snapshot', type=Path,
                        help='SHA-pinned source snapshot; default is the preserved v84 reference')
    args = parser.parse_args(argv)
    if args.output.exists():
        raise ValueError('benchmark_receipt_is_immutable_select_a_new_output')
    result = benchmark(args.reference_snapshot)
    atomic_write_json(args.output, result)
    print(json.dumps({key: result[key] for key in ('accepted', 'summary')}))
    return int(not result['accepted'])


if __name__ == '__main__':
    raise SystemExit(main())

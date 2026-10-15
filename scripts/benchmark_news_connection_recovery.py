"""Paired owned-loopback news recovery: real TCP, parser, receipts and raw parity.

Synthetic owned sources, not publisher throughput, TLS or official-limit proof.
No production queue/HTTP owner, external source requests or rate-state writes.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from copy import deepcopy
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import statistics
import tempfile
import threading
import time
from urllib.parse import urlsplit

import pyarrow.parquet as pq
import requests

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_connections import OriginConnectionSession
from downloader.news_fulltext_connection_observation import ConnectionObservingAdapter
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, utc_now, verify_bodies


HOSTS = ('publisher.test', 'm.publisher.test', 'other.test')


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def setup(self):
        super().setup()
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        with self.server.observation_lock:
            self.server.connections += 1

    def do_GET(self):
        with self.server.observation_lock:
            self.server.paths.append(self.path)
        if self.path.startswith('/fail/'):
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        if self.path == '/robots.txt':
            payload, mime = b'User-agent: *\nAllow: /', 'text/plain'
        else:
            body = 'This is owned synthetic news, retaining original signed body and clock. ' * 18
            article = {'@type': 'NewsArticle', 'headline': 'Owned fixture ' + self.path,
                       'articleBody': body, 'isAccessibleForFree': True,
                       'datePublished': '2026-10-01T00:00:00+00:00', 'author': {'name': 'Fixture author'}}
            payload = ('<html><script type="application/ld+json">' + json.dumps(article) + '</script>'
                       '<article class="story">' + body + '</article></html>').encode()
            mime = 'text/html; charset=utf-8'
        self.send_response(200)
        self.send_header('Content-Type', mime)
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


@contextmanager
def owned_servers():
    servers, threads = {}, []
    try:
        for host in HOSTS:
            server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
            server.daemon_threads = True
            server.observation_lock = threading.Lock()
            server.connections, server.paths = 0, []
            thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .02}, daemon=True)
            thread.start()
            servers[host] = server
            threads.append(thread)
        yield servers
    finally:
        for server in servers.values():
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join(timeout=2)


class _LoopbackRequest:
    def send(self, request, **kwargs):
        parts = urlsplit(request.url)
        if parts.scheme != 'https' or parts.hostname not in self.servers or parts.query:
            raise ValueError('only_owned_fixture_requests_allowed')
        local = request.copy()
        local.url = f'http://127.0.0.1:{self.servers[parts.hostname].server_port}{parts.path}'
        local.headers['Host'] = parts.hostname
        response = super().send(local, **kwargs)
        response.url, response.request = request.url, request
        return response


class LoopbackAdapter(_LoopbackRequest, requests.adapters.HTTPAdapter):
    def __init__(self, servers, *, scoped):
        super().__init__(pool_connections=1 if scoped else 10, pool_maxsize=1 if scoped else 10, max_retries=0)
        self.servers = servers



class ObservedLoopbackAdapter(_LoopbackRequest, ConnectionObservingAdapter):
    def __init__(self, servers):
        super().__init__(pool_connections=1, pool_maxsize=1, max_retries=0)
        self.servers = servers


class OwnedScopedSession(OriginConnectionSession):
    def __init__(self, servers, *, observe_connections=False):
        super().__init__()
        self.servers = servers
        self.observe_connections = observe_connections

    def _new_origin_adapter(self):
        if self.observe_connections:
            return ObservedLoopbackAdapter(self.servers)
        return LoopbackAdapter(self.servers, scoped=True)


class OwnedLimiter:
    """Only owned loopback data: no external provider pacing/quotas are borrowed."""
    def __init__(self, interval_seconds, **kwargs):
        self.interval_seconds = interval_seconds

    def wait(self, **kwargs):
        pass

    def defer(self, seconds):
        pass


def owned_config():
    config = json.loads(Path('configs/news_fulltext.json').read_text())
    config.pop('provider_catalog', None)
    source = deepcopy(config['providers'][0])
    source.update(id='publisher', domains=list(HOSTS[:2]), interval_seconds=.001,
                  feeds=[], sitemaps=[], wordpress_apis=[], fulltext_feeds=[], index_pages=[],
                  index_path_pattern='', article_path_pattern='', article_selectors=['.story'],
                  require_configured_container=False, require_author_attribution=False,
                  title_selector='', author_selectors=[], required_license_url_prefix='', exclude_selectors=[],
                  authorization={'kind': 'open_license', 'license': 'CC-BY-3.0',
                                 'evidence_url': 'https://publisher.test/terms',
                                 'scope': 'owned synthetic benchmark fixture only'})
    other = deepcopy(source)
    other.update(id='other', domains=[HOSTS[2]])
    config.update(providers=[source, other], adaptive_rate={'enabled': False},
                  exclusive_http_owner=False, min_free_disk_bytes=1)
    return config


def phase(*, reference, rounds, observe_connections=False):
    config = owned_config()
    with tempfile.TemporaryDirectory(prefix='news-owned-connection-recovery-') as directory, owned_servers() as servers:
        root = Path(directory)
        registry = PolicyRegistry(config, base=root)
        sessions = []

        def factory():
            session = requests.Session() if reference else OwnedScopedSession(servers, observe_connections=observe_connections)
            if reference:
                session.mount('https://', LoopbackAdapter(servers, scoped=False))
            sessions.append(session)
            return session

        def owned_address(url):
            if urlsplit(url).hostname not in HOSTS:
                raise ValueError('external_address_forbidden')

        queue = NewsQueue(root, registry)
        http = NewsHTTP(registry, config, root, session_factory=factory,
                        address_check=owned_address, limiter_factory=OwnedLimiter, metrics=queue.metrics)
        sequence = [f'https://{host}/warm' for host in HOSTS]
        for number in range(rounds):
            sequence.extend((f'https://m.publisher.test/ok/{number}', f'https://m.publisher.test/fail/{number}',
                             f'https://publisher.test/after/{number}', f'https://other.test/after/{number}'))
        # Failures use the source path, not a transparent local re-send.
        started, cpu_started = time.monotonic(), time.process_time()
        errors, records = [], []
        try:
            for url in sequence:
                with queue.db:
                    queue.add_articles([{'url': url}])
                row = queue.claim_url(url, 120)
                try:
                    records.append(fetch_article(http, row))
                except NewsError as error:
                    queue.failed(row, error, 6)
                    errors.append({'url': url, 'error': error.code, 'retry': error.retry, 'delay': error.delay})
            queue.accept(records)
            proof = verify_bodies(root)
            signatures = []
            for path in sorted((root / 'bodies').rglob('*.parquet')):
                for record in pq.read_table(path).to_pylist():
                    signatures.append({key: record[key] for key in ('requested_url', 'final_url', 'title',
                        'body_sha256', 'response_sha256', 'published_at_raw', 'authors_json', 'data_fingerprint')})
            signatures.sort(key=lambda row: row['requested_url'])
            failures = [dict(row) for row in queue.db.execute(
                "SELECT state,error_code,body_sha256,due FROM articles WHERE state!='complete'")]
            accepted = (proof['accepted'] and len(records) == 3 + 3 * rounds and len(errors) == rounds
                        and all(error['error'] == 'transport_error' and error['retry'] and error['delay'] == 30 for error in errors)
                        and len(failures) == rounds
                        and all(row['state'] == 'retry' and row['body_sha256'] is None and row['due'] > time.time() for row in failures))
            performance = http.metrics.snapshot()
            return {'reference': reference, 'observe_connections': observe_connections,
                    'accepted': accepted, 'wall_seconds': time.monotonic() - started,
                    'cpu_seconds': time.process_time() - cpu_started, 'stored_bodies': len(records), 'failures': errors,
                    'semantic_signature_sha256': hashlib.sha256(json.dumps(signatures, sort_keys=True).encode()).hexdigest(),
                    'requests': {host: len(server.paths) for host, server in servers.items()},
                    'tcp_connections': {host: server.connections for host, server in servers.items()},
                    'sessions_created': len(sessions), 'body_receipt_proof': proof,
                    'connection_checkout_counts': {provider: {key: value for key, value in data['counts'].items()
                        if key.startswith('connection_checkout_')} for provider, data in performance['providers'].items()},
                    'recovery_counts': {provider: {key: data['counts'].get(key, 0) for key in (
                        'transport_errors', 'failed_sessions_reset', 'failed_origin_pools_reset', 'http_requests_started')}
                        for provider, data in performance['providers'].items()}}
        finally:
            http.close()
            queue.close()


def observation_benchmark(*, rounds=8, cycles=2):
    """Only the connection observer differs, with full original body receipts."""
    if type(rounds) is not int or not 1 <= rounds <= 12 or type(cycles) is not int or not 1 <= cycles <= 3:
        raise ValueError('bounded_owned_rounds_and_cycles_required')
    phases = [phase(reference=False, rounds=rounds, observe_connections=observed)
              for _ in range(cycles) for observed in (False, True, True, False)]
    first = phases[0]
    accepted = all(row['accepted'] and all(row[key] == first[key] for key in (
        'semantic_signature_sha256', 'requests', 'tcp_connections', 'failures')) for row in phases)
    for row in phases:
        for provider, counts in row['connection_checkout_counts'].items():
            starts = row['recovery_counts'][provider]['http_requests_started']
            accepted &= sum(counts.values()) == starts
            accepted &= counts.get('connection_checkout_unobserved', 0) == (0 if row['observe_connections'] else starts)
    return {'contract_version': 1, 'accepted': accepted, 'verified_at_utc': utc_now(),
        'scope': 'same_owned_loopback_capture_parser_raw_receipts_recovery_ABBA_connection_observer_only',
        'publisher_requests': 0, 'production_queue_writes': False, 'production_rate_state_writes': False,
        'official_limit_proved': False, 'publisher_speedup_claimed': False, 'phases': phases,
        'summary': {label: {'median_wall_seconds': statistics.median(r['wall_seconds'] for r in phases if r['observe_connections'] == observed),
                           'median_cpu_seconds': statistics.median(r['cpu_seconds'] for r in phases if r['observe_connections'] == observed),
                           'wall_samples': [r['wall_seconds'] for r in phases if r['observe_connections'] == observed],
                           'cpu_samples': [r['cpu_seconds'] for r in phases if r['observe_connections'] == observed]}
                    for label, observed in (('reference', False), ('observed', True))},
        'implementation_sha256': {str(path): sha256_file(path) for path in (
            Path(__file__), *sorted(Path('downloader').glob('news_fulltext*.py')))}}


def benchmark(*, rounds=6, cycles=2):
    if type(rounds) is not int or not 1 <= rounds <= 12 or type(cycles) is not int or not 1 <= cycles <= 3:
        raise ValueError('bounded_owned_rounds_and_cycles_required')
    phases = [phase(reference=reference, rounds=rounds) for _ in range(cycles) for reference in (True, False, False, True)]
    signature = phases[0]['semantic_signature_sha256']
    accepted = (all(row['accepted'] and row['semantic_signature_sha256'] == signature for row in phases)
                and all(row['requests'] == phases[0]['requests'] and row['failures'] == phases[0]['failures'] for row in phases)
                and all(row['tcp_connections']['publisher.test'] == row['tcp_connections']['other.test'] == 1
                        for row in phases if not row['reference']))
    return {'contract_version': 1, 'verified_at_utc': utc_now(), 'accepted': accepted,
            'scope': 'owned_synthetic_loopback_full_capture_and_recovery_not_publisher_rate_or_tls',
            'external_source_requests': 0, 'production_queue_writes': False, 'production_rate_state_writes': False,
            'cycles': cycles, 'faults_per_phase': rounds, 'phases': phases,
            'summary': {label: {'median_wall_seconds': statistics.median(r['wall_seconds'] for r in phases if r['reference'] == reference),
                               'median_cpu_seconds': statistics.median(r['cpu_seconds'] for r in phases if r['reference'] == reference),
                               'tcp_connections': [r['tcp_connections'] for r in phases if r['reference'] == reference]}
                        for label, reference in (('reference', True), ('candidate', False))},
            'implementation_sha256': {str(path): sha256_file(path) for path in (
                Path(__file__), *sorted(Path('downloader').glob('news_fulltext*.py')))}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rounds', type=int, default=6)
    parser.add_argument('--cycles', type=int, default=2)
    parser.add_argument('--connection-observation', action='store_true',
                        help='Compare the same scoped transport with/without connection hooks')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    benchmark_function = observation_benchmark if args.connection_observation else benchmark
    result = benchmark_function(rounds=args.rounds, cycles=args.cycles)
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print(json.dumps({key: result[key] for key in ('accepted', 'scope', 'summary')}, ensure_ascii=False))
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

"""Paired offline owner benchmark with durable fake-publisher body receipts.

All HTTP responses are synthetic, the SQLite queue is under /var/tmp, and no
production service, publisher, original body or production queue is touched.
"""
from copy import deepcopy
import hashlib
import json
import statistics
import tempfile
import time
from pathlib import Path

import downloader.news_fulltext as news
from downloader.news_fulltext_bulk import cnyes_scope
from downloader.news_fulltext_cnyes_index import request_url
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies


class FakeResponse:
    def __init__(self, body):
        self.body, self.status_code = body, 200
        self.headers = {'Content-Type': 'text/html'}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def iter_content(self, **_):
        yield self.body


class FakeSession:
    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def get(self, url, **_):
        self.calls.append(url)
        return self.routes[url]

    def close(self):
        pass


class FakeLimiter:
    def __init__(self, interval_seconds, **_kwargs):
        self.interval_seconds = interval_seconds

    def wait(self, **_kwargs):
        pass

    def defer(self, *_args):
        pass


class DisableCache:
    disabled = True

    def hit(self, *_):
        return False

    def wake_deadline(self, *_):
        return None


def config():
    data = json.loads(Path('configs/news_fulltext.json').read_text())
    cnyes = deepcopy(next(entry for entry in data['providers'] if entry['id'] == 'cnyes'))
    cnyes.pop('cnyes_endpoint_inventory', None)
    cnyes.update(cnyes_apis=['https://api.cnyes.com/media/api/v1/newslist/category/headline'],
                 cnyes_history_partition_policy='monthly_v1', interval_seconds=.001)
    publisher = deepcopy(data['providers'][0])
    publisher.update(id='publisher', domains=['publisher.test'], interval_seconds=.001,
        feeds=[], sitemaps=[], wordpress_apis=[], fulltext_feeds=[], historical_fulltext_feeds=[],
        index_pages=[], index_path_pattern='', article_path_pattern='', article_selectors=['.story'],
        require_configured_container=False, require_author_attribution=False, title_selector='',
        author_selectors=[], required_license_url_prefix='', exclude_selectors=[])
    data.pop('provider_catalog', None)
    data.pop('failure_diagnostic_focus', None)
    data.update(providers=[cnyes, publisher], workers=2, batch_articles=1, min_free_disk_bytes=1,
                adaptive_rate={'enabled': False}, exclusive_http_owner=False,
                failure_diagnostic_metadata_identity_upgrade=False, retained_cnyes_failure_replay_enabled=False)
    return data


def run_one(cache_enabled):
    with tempfile.TemporaryDirectory(prefix='cnyes-owner-bench-', dir='/var/tmp') as directory:
        root, cfg = Path(directory), config()
        registry = PolicyRegistry(cfg, base=root)
        source = registry.sources['cnyes']
        # Keep the end-to-end I/O sample bounded on a shared workstation.
        urls = [f'https://publisher.test/article/{number}' for number in range(16)]
        body = '這是一篇保留原始內容的新聞報導，不是標題或摘要。' * 16
        schema = {'@type': 'NewsArticle', 'headline': '測試新聞', 'articleBody': body,
                  'isAccessibleForFree': True, 'datePublished': '2014-02-03', 'author': {'name': '作者'}}
        html = ('<html><title>News</title><script type="application/ld+json">' +
                json.dumps(schema, ensure_ascii=False) + '</script></html>').encode()
        routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /')}
        routes.update({url: FakeResponse(html) for url in urls})
        session = FakeSession(routes)
        http = NewsHTTP(registry, cfg, root, session_factory=lambda: session,
                        address_check=lambda _: None, limiter_factory=FakeLimiter)
        owner = NewsQueue(root, registry)
        try:
            days = range(16323, 16323+232)
            now = time.time()
            with owner.db:
                owner.add_articles([{'url': url} for url in urls])
                owner.db.executemany("INSERT INTO discovery(url,kind,provider,due) VALUES (?,'index','cnyes',?)",
                    [(request_url(source, day, page), now+600 if page == 1 else 0)
                     for day in days for page in range(1, 49)])
                owner.db.executemany('INSERT INTO cnyes_index_revision_rounds VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                    [(cnyes_scope(request_url(source, day)), source.cnyes_index_data_fingerprint,
                      source.authorization_fingerprint, f'{day:064x}', f'{day:064x}', 'anchor',
                      None, None, None, None, '2026-10-28T00:00:00+00:00') for day in days])
            if not cache_enabled:
                owner._cnyes_revision_empty_selection = DisableCache()
            wall, cpu = time.perf_counter(), time.process_time()
            result = news.run_cycle(owner, http, cfg, max_articles=len(urls), max_discovery_requests=1)
            wall, cpu = time.perf_counter()-wall, time.process_time()-cpu
            verified = verify_bodies(root)
            cnyes_counts = owner.metrics.snapshot()['providers']['cnyes']
            assert result['cycle']['durably_accepted_versions'] == len(urls)
            assert verified['accepted'] and verified['body_versions_verified'] == len(urls)
            assert all(url.startswith('https://publisher.test/') for url in session.calls)
            return {'cache_enabled': cache_enabled, 'wall_seconds': wall, 'cpu_seconds': cpu,
                    'accepted_bodies': len(urls), 'verified_body_versions': verified['body_versions_verified'],
                    'fake_http_calls': len(session.calls), 'real_publisher_calls': 0,
                    'cnyes_discovery_stage': cnyes_counts['stages']['discovery_dispatch'],
                    'cnyes_cache_hits': cnyes_counts['counts'].get('discovery_revision_empty_cache_hits', 0)}
        finally:
            http.close()
            owner.close()


original_disk, original_seed = news.refresh_disk_admission, news.seed_discovery
try:
    # Explicitly bypass only the real backing-space sampler for small temporary
    # synthetic fixtures; the production owner's admission remains untouched.
    news.refresh_disk_admission = lambda *_: False
    news.seed_discovery = lambda *_: None
    samples = []
    for enabled in (False, True, True, False):
        sample = run_one(enabled)
        samples.append(sample)
        print(json.dumps({'completed_sample': len(samples), **sample}), flush=True)
finally:
    news.refresh_disk_admission, news.seed_discovery = original_disk, original_seed

baseline = [row['wall_seconds'] for row in samples if not row['cache_enabled']]
optimized = [row['wall_seconds'] for row in samples if row['cache_enabled']]
files = ('downloader/news_fulltext_queue.py', 'downloader/news_fulltext_cnyes_dispatch_cache.py')
result = {'contract_version': 1, 'synthetic': True, 'order': 'ABBA', 'samples': samples,
          'days': 232, 'pages_per_day': 48, 'due_pages': 232*47, 'articles_per_sample': 16,
          'median_baseline_wall_seconds': statistics.median(baseline),
          'median_optimized_wall_seconds': statistics.median(optimized),
          'implementation_sha256': {path: hashlib.sha256(Path(path).read_bytes()).hexdigest() for path in files},
          'limits': 'Local fake publisher; real executor, parser, queue and durable receipt verification. No network or production files.'}
Path('/var/tmp/cnyes-revision-cache-owner-benchmark-20261028.json').write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps({key: result[key] for key in ('median_baseline_wall_seconds','median_optimized_wall_seconds','samples')}))

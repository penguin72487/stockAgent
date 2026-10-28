"""Compare bounded fake-host failures through the real offline news owner.

The reference removes only discovery's pre-dispatch host exclusion; the real
HTTP host gate remains active. Everything, including receipts, lives in a
temporary directory. No source, production queue, or service is contacted.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import statistics
import tempfile
import time
from unittest.mock import patch

import downloader.news_fulltext as owner_module
from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies


class Response:
    def __init__(self, body, content_type='text/html'):
        self.body, self.status_code, self.headers = body, 200, {'Content-Type': content_type}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def iter_content(self, **_):
        yield self.body


class Session:
    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def get(self, url, **_):
        self.calls.append(url)
        return self.routes[url]  # An unexpected route fails; never use network.

    def close(self):
        pass


class Limiter:
    def __init__(self, interval_seconds, **_):
        self.interval_seconds = interval_seconds

    def wait(self, **_):
        pass

    def defer(self, *_):
        pass


def config():
    cfg = json.loads(Path('configs/news_fulltext.json').read_text())
    source = deepcopy(cfg['providers'][0])
    source.update(id='publisher', domains=['publisher.test'], interval_seconds=.001,
        feeds=[], sitemaps=[], wordpress_apis=[], fulltext_feeds=[], historical_fulltext_feeds=[],
        index_pages=[], index_path_pattern='', article_path_pattern='', article_selectors=['.story'],
        require_configured_container=False, require_author_attribution=False, title_selector='',
        author_selectors=[], required_license_url_prefix='', exclude_selectors=[])
    cfg.pop('provider_catalog', None)
    cfg.pop('failure_diagnostic_focus', None)
    cfg.update(providers=[source], workers=1, batch_articles=1, min_free_disk_bytes=1,
               adaptive_rate={'enabled': False}, exclusive_http_owner=False,
               failure_diagnostic_metadata_identity_upgrade=False, retained_cnyes_failure_replay_enabled=False,
               cached_cnyes_replay_enabled=False, cached_cnyes_metadata_replay_enabled=False)
    return cfg


def run_one(exclude_paused_host, *, blocked_count=250):
    with tempfile.TemporaryDirectory(prefix='news-discovery-host-bench-', dir='/var/tmp') as directory:
        root, cfg = Path(directory), config()
        registry = PolicyRegistry(cfg, base=root)
        good_feed, good_article = 'https://good.publisher.test/feed', 'https://good.publisher.test/news/1'
        feed = (f'<rss version="2.0"><channel><title>fixture</title><item><title>news</title>'
                f'<link>{good_article}</link></item></channel></rss>').encode()
        body = '這是一篇供離線驗證的合成新聞內容。' * 25
        schema = {'@type': 'NewsArticle', 'headline': '測試新聞', 'articleBody': body,
                  'isAccessibleForFree': True, 'datePublished': '2014-02-03', 'author': {'name': '作者'}}
        html = ('<html><script type="application/ld+json">' + json.dumps(schema, ensure_ascii=False)
                + '</script></html>').encode()
        session = Session({'https://good.publisher.test/robots.txt': Response(b'User-agent: *\nAllow: /'),
                           good_feed: Response(feed, 'application/rss+xml'), good_article: Response(html)})
        http = NewsHTTP(registry, cfg, root, session_factory=lambda: session,
                        limiter_factory=Limiter, address_check=lambda _: None)
        queue = NewsQueue(root, registry)
        try:
            with queue.db:
                queue.db.executemany(
                    "INSERT INTO discovery(url,kind,provider,priority,due,attempts,error_code) "
                    "VALUES (?,'feed','publisher',0,0,3,'prior_failure')",
                    [(f'https://bad.publisher.test/feed/{number:05d}',) for number in range(blocked_count)])
                queue.db.execute("INSERT INTO discovery(url,kind,provider,priority) VALUES (?,'feed','publisher',100)", (good_feed,))
            http.host_circuits.paused['publisher'] = {'bad.publisher.test': time.time() + 600}
            if not exclude_paused_host:
                canonical = queue.discovery_candidates
                def reference(source, limit, *, bulk_allowed=True, excluded_origins=(), excluded_hosts=()):
                    return canonical(source, limit, bulk_allowed=bulk_allowed, excluded_origins=excluded_origins)
                queue.discovery_candidates = reference
            wall, cpu = time.perf_counter(), time.process_time()
            result = owner_module.run_cycle(queue, http, cfg, max_articles=1,
                                            max_discovery_requests=blocked_count + 2, max_run_seconds=30)
            wall, cpu = time.perf_counter() - wall, time.process_time() - cpu
            audit = verify_bodies(root)
            changed = queue.db.execute("SELECT count(*) FROM discovery WHERE url LIKE 'https://bad.publisher.test/%' "
                "AND (due!=0 OR attempts!=3 OR error_code!='prior_failure')").fetchone()[0]
            counts = queue.metrics.snapshot()['providers']['publisher']['counts']
            assert result['cycle']['durably_accepted_versions'] == 1
            assert audit['accepted'] and audit['body_versions_verified'] == 1
            assert good_feed in session.calls and good_article in session.calls
            assert all(not url.startswith('https://bad.publisher.test/') for url in session.calls)
            assert changed == (0 if exclude_paused_host else blocked_count)
            return {'exclude_paused_host': exclude_paused_host, 'wall_seconds': wall, 'cpu_seconds': cpu,
                    'paused_rows_changed': changed, 'discovery_dispatches': result['cycle']['discovery_attempts_started'],
                    'gate_deferrals': counts.get('discovery_source_gate_deferrals', 0),
                    'accepted_bodies': audit['body_versions_verified'], 'fake_http_calls': len(session.calls)}
        finally:
            http.close()
            queue.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    with patch.object(owner_module, 'refresh_disk_admission', return_value=False), \
            patch.object(owner_module, 'seed_discovery', return_value=None):
        samples = [run_one(enabled) for enabled in (False, True, True, False)]
    medians = {str(enabled).lower(): {metric: statistics.median(
        row[metric] for row in samples if row['exclude_paused_host'] == enabled)
        for metric in ('wall_seconds', 'cpu_seconds')} for enabled in (False, True)}
    files = (Path(__file__), Path('downloader/news_fulltext.py'), Path('downloader/news_fulltext_queue.py'),
             Path('downloader/news_fulltext_host_circuit.py'), Path('downloader/news_fulltext_cnyes_dispatch_cache.py'))
    result = {'contract_version': 1, 'synthetic': True, 'order': 'ABBA', 'blocked_jobs': 250,
              'samples': samples, 'medians': medians, 'real_publisher_calls': 0,
              'production_queue_written': False, 'source_hashes': {str(path): sha256_file(path) for path in files},
              'body_sha256': hashlib.sha256(('這是一篇供離線驗證的合成新聞內容。' * 25).encode()).hexdigest(),
              'limits': 'Synthetic queue and HTTP only; not live throughput, historical coverage or an official rate.'}
    atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()

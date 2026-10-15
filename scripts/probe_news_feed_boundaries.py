"""Bounded canonical RSS diagnostics; never retain mixed restricted envelopes.

Pause the formal HTTP owner first. These probes reuse its robots, pacing and
persisted cooldowns. Only hashes, public identities and parser outcomes are
saved; no article body, raw response, queue mutation or acceptance claim.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
from pathlib import Path
import json
import time

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import fetch_discovery, load_config
from downloader.news_fulltext_bulk import wordpress_base
from downloader.news_fulltext_feed import feed_page, feed_url
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import utc_now


def inspect(http, urls, *, output):
    if not 1 <= len(urls) <= 4 or len(set(urls)) != len(urls):
        raise ValueError('one_to_four_distinct_registered_feed_pages_required')
    for url in urls:
        source = http.registry.require(url)
        if wordpress_base(url) not in source.fulltext_feeds:
            raise ValueError('explicit_registered_fulltext_feed_required')
        feed_page(url)
    result = {'observed_at_utc': utc_now(), 'probes': [], 'queue_mutated': False,
              'raw_response_retained': False, 'body_retained': False,
              'article_durably_accepted': False, 'historical_completeness_verified': False}
    for url in urls:
        started = time.monotonic()
        source = http.registry.require(url)
        item = {'url': url, 'provider': source.provider}
        try:
            task = {'url': url, 'kind': 'rss_fulltext', 'etag': None,
                    'last_modified': None, 'parser_fingerprint': None}
            response, links, records, errors = fetch_discovery(http, task)
            item.update(status=response.status, response_sha256=hashlib.sha256(response.body).hexdigest(),
                        response_bytes=len(response.body), public_links=len(links),
                        permitted_body_candidates=[{k: r[k] for k in (
                            'requested_url', 'body_sha256', 'published_at_raw', 'authors_json')}
                            for r in records],
                        item_error_counts=dict(Counter(e['error'] for e in errors)),
                        item_errors=[{k: e[k] for k in ('url', 'error')} for e in errors],
                        next_page_url=feed_url(wordpress_base(url), feed_page(url) + 1)
                            if records or errors else None)
        except NewsError as error:
            item.update(error=error.code, retry=error.retry, delay=error.delay)
        item.update(wall_seconds=time.monotonic() - started, observed_at_utc=utc_now())
        result['probes'].append(item)
        atomic_write_json(output, result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--probe', action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    config, registry, repo = load_config(args.config)
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    http = NewsHTTP(registry, config, repo / config['root'])
    try:
        result = inspect(http, args.probe, output=args.output)
    finally:
        http.close()
    result['implementation_sha256'] = sha256_file(Path(__file__))
    atomic_write_json(args.output, result)
    print(json.dumps({'probes': [{k: p.get(k) for k in (
        'url', 'status', 'error', 'public_links', 'item_error_counts')} for p in result['probes']],
        'raw_response_retained': False, 'body_retained': False}))
    return int(any('error' in p for p in result['probes']))


if __name__ == '__main__':
    raise SystemExit(main())

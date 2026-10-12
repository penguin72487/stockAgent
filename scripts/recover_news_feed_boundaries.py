"""Recover up to four verified feed boundary jobs through the canonical owner.

The formal service/timer must be paused. No deadline, refusal or source policy
is reset here: queue initialization performs versioned reconciliation, and each
explicit job must be registered and already due. All bodies/receipts are written
only by accept_discovery; mixed restricted envelopes never leave memory.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import time

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import accept_discovery, fail_discovery, fetch_discovery, load_config
from downloader.news_fulltext_bulk import wordpress_base
from downloader.news_fulltext_feed import feed_page
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import NewsQueue, utc_now


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--diagnostics', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    diagnostic = json.loads(args.diagnostics.read_text())
    probes = diagnostic['probes']
    if (not 1 <= len(probes) <= 4 or len({p['url'] for p in probes}) != len(probes)
            or any(p.get('status') != 200 or p.get('error') for p in probes)):
        raise ValueError('bounded_successfully_observed_feed_pages_required')
    config, registry, repo = load_config(args.config)
    for probe in probes:
        source = registry.require(probe['url'])
        if wordpress_base(probe['url']) not in source.fulltext_feeds:
            raise ValueError('registered_feed_scope_required')
        feed_page(probe['url'])
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    queue = NewsQueue(repo / config['root'], registry)
    http = None
    result = {'observed_at_utc': utc_now(), 'diagnostic_sha256': sha256_file(args.diagnostics),
              'owner_id': queue.owner, 'jobs': [], 'source_deadlines_reset': False,
              'all_history_complete': False, 'publish': False}
    try:
        http = NewsHTTP(registry, config, queue.root, metrics=queue.metrics)
        for probe in probes:
            row = queue.db.execute('SELECT * FROM discovery WHERE url=?', (probe['url'],)).fetchone()
            if not row or row['kind'] != 'rss_fulltext' or row['due'] > time.time():
                raise ValueError('selected_feed_job_not_registered_or_not_due')
            task = dict(row)
            item = {'url': probe['url'], 'provider': task['provider'], 'observed_at_utc': utc_now()}
            try:
                response, links, records, errors = fetch_discovery(http, task)
                item.update(response_sha256=hashlib.sha256(response.body).hexdigest(),
                            response_bytes=len(response.body), source_status=response.status,
                            candidate_bodies=len(records), item_errors=errors)
                item['accepted_bodies'] = accept_discovery(queue, task, response, links, records, errors)
                item['checkpoint'] = dict(queue.db.execute(
                    'SELECT error_code,last_checked_at_utc,response_sha256,parser_fingerprint,due FROM discovery WHERE url=?',
                    (task['url'],)).fetchone())
            except NewsError as error:
                fail_discovery(queue, task, error)
                item.update(error=error.code, retry=error.retry, delay=error.delay)
            result['jobs'].append(item)
            atomic_write_json(args.output, result)
        queue.status(activity='idle')
        result.update(native_capture_completed=not any('error' in j for j in result['jobs']),
                      implementation_sha256=sha256_file(Path(__file__)), finished_at_utc=utc_now())
        atomic_write_json(args.output, result)
    finally:
        if http:
            http.close()
        queue.close()
    print(json.dumps({k: result[k] for k in ('native_capture_completed', 'jobs', 'all_history_complete')}, ensure_ascii=False))
    return int(not result['native_capture_completed'])


if __name__ == '__main__':
    raise SystemExit(main())

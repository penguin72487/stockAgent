"""Pinned read-only audit of dense news scopes and migration-owned retirement.

Public source URLs only; no bodies, publisher requests or SQLite writer. The
entire source gap stays visible even when its first 30 pages are obtainable.
"""
from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import sqlite3
from urllib.parse import parse_qs, urlsplit

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext_bulk import cnyes_scope
from downloader.news_fulltext_queue import utc_now


def audit(root):
    db = sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        scopes = {r['base_url']: dict(r) for r in db.execute(
            'SELECT b.* FROM bulk_api_coverage b LEFT JOIN bulk_window_splits s ON b.base_url=s.scope_url '
            'WHERE b.provider=\'cnyes\' AND b.total_pages>30 AND s.scope_url IS NULL')}
        counts, samples = Counter(), {}
        for scope, item in scopes.items():
            query = parse_qs(urlsplit(scope).query)
            span = int(query['endAt'][0]) - int(query['startAt'][0]) + 1 if 'startAt' in query else None
            kind = 'unbounded' if span is None else 'one_day_or_less' if span <= 86400 else 'splittable_multiday'
            counts[kind] += 1
            samples.setdefault(kind, item)
        owned = []
        for r in db.execute('SELECT url,due,error_code,superseded_by_windows_json FROM discovery '
                            'WHERE provider=\'cnyes\' AND kind=\'cnyes_news\' AND superseded_by_windows_json IS NOT NULL'):
            scope = cnyes_scope(r['url'])
            if scope not in scopes:
                continue
            query = parse_qs(urlsplit(r['url']).query)
            if 'startAt' not in query:
                continue
            span = int(query['endAt'][0]) - int(query['startAt'][0]) + 1
            page = int(query.get('page', ['1'])[0])
            if (span <= 86400 and 2 <= page <= 30 and
                    r['superseded_by_windows_json'] == json.dumps({'pending_partition_scope': scope})):
                owned.append({'url': r['url'], 'due': r['due'], 'error_code': r['error_code']})
        return {'contract_version': 1, 'observed_at_utc': utc_now(), 'unsplit_dense_scope_counts': dict(counts),
                'scope_examples': samples, 'migration_retired_permitted_day_jobs': len(owned),
                'retired_jobs_sample': owned[:5],
                'dispatch_migration_marker': dict(db.execute('SELECT key,value FROM queue_settings '
                                                            'WHERE key LIKE \'cnyes_window_dispatch_v2:%\'')),
                'migration_counters': dict(db.execute('SELECT key,value FROM counters WHERE key LIKE \'cnyes_window_v2_%\'')),
                'all_history_complete': False, 'publish': False,
                'scope': 'all currently observed Cnyes dense scopes; restoring a legal page is not body acceptance'}
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_news_fulltext'))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    result = audit(args.root)
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

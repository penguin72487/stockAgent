"""Bounded read-only alias candidates, not a proof of interchangeable pages.

An existing desktop body can explain a failed mobile view without a missing
story. Never accept aliases, reset failures, fetch publishers or copy bodies.
The sample is host/state index order, not random or the whole remaining queue.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
from pathlib import Path
import re
import sqlite3
import time
from urllib.parse import urlsplit

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_publishers import cnyes_supported_mobile_view
from downloader.news_fulltext_queue import utc_now


SAMPLE_SQL = ("SELECT id,url,title FROM articles INDEXED BY host_state "
              "WHERE host='m.cnyes.com' AND state='pending' "
              "AND provider='cnyes' AND body_sha256 IS NULL LIMIT ?")


def audit(root: Path, maximum_rows: int = 512) -> dict:
    if type(maximum_rows) is not int or not 1 <= maximum_rows <= 4096:
        raise ValueError('bounded_integer_sample_required')
    started, cpu = time.monotonic(), time.process_time()
    db = sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    counts, digest, selected = Counter(), hashlib.sha256(), []
    try:
        db.execute('BEGIN')
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
        observed = utc_now()
        plan = [r['detail'] for r in db.execute('EXPLAIN QUERY PLAN ' + SAMPLE_SQL, (maximum_rows,))]
        rows = db.execute(SAMPLE_SQL, (maximum_rows,)).fetchall()
        for row in rows:
            digest.update(row['id'])
            parts = urlsplit(row['url'])
            match = re.fullmatch(r'/news/id/([0-9]+)', parts.path)
            if parts.scheme != 'https' or parts.netloc != 'm.cnyes.com' or not match or parts.fragment:
                counts['unsupported_request_shape'] += 1
                continue
            story_id = match[1]
            target = db.execute('SELECT id,state,body_sha256,captured_at_utc,title FROM articles WHERE url=?',
                                ('https://news.cnyes.com/news/id/' + story_id,)).fetchone()
            candidate = bool(target and target['state'] == 'complete' and target['body_sha256'])
            counts['existing_canonical_body_candidate' if candidate else 'no_complete_canonical_body_candidate'] += 1
            supported = cnyes_supported_mobile_view(parts)
            counts['supported_current_mobile_view' if supported else 'unverified_query_view'] += 1
            title_match = None
            if candidate and row['title'] and target['title']:
                title_match = row['title'] == target['title']
                counts['exact_metadata_title_match' if title_match else 'different_metadata_titles'] += 1
            selected.append({'mobile_article_id': row['id'].hex(), 'publisher_story_id': story_id,
                'query_present': bool(parts.query), 'supported_current_mobile_view': supported,
                'existing_canonical_body_candidate': candidate,
                'canonical_article_id': target['id'].hex() if target else None,
                'canonical_captured_at_utc': target['captured_at_utc'] if candidate else None,
                'exact_metadata_title_match': title_match})
        return {'contract_version': 1, 'observed_at_utc': observed,
            'queue_owner_id': owner[0] if owner else None, 'maximum_rows': maximum_rows,
            'sampled_pending_mobile_heads': len(rows), 'sample_limit_reached': len(rows) == maximum_rows,
            'sample_basis': 'bounded pending mobile host/state index order; not random, latest-first or all remaining work',
            'selected_ids_sha256': digest.hexdigest(), 'counts': dict(counts), 'candidates': selected,
            'query_plan': plan, 'publisher_requests': 0, 'source_states_mutated': False,
            'canonical_receipts_verified': False, 'alias_source_equivalence_verified': False,
            'historical_completeness_verified': False, 'publish': False, 'remote_training_export': False,
            'inventory_cost': {'wall_seconds': time.monotonic() - started, 'cpu_seconds': time.process_time() - cpu},
            'next_action': 'verify source-declared identity, rights, grain and original receipts before any canonical reuse; retain distinct observations and unknown aliases'}
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--maximum-rows', type=int, default=512)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    config, registry, repo = load_config(args.config)
    result = audit(repo / config['root'], args.maximum_rows)
    result['current_source_fingerprint'] = registry.sources['cnyes'].data_fingerprint
    result['configuration_sha256'] = sha256_file(args.config)
    result['implementation_sha256'] = sha256_file(Path(__file__))
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print({k: result[k] for k in ('sampled_pending_mobile_heads', 'counts', 'inventory_cost',
                                  'publisher_requests', 'source_states_mutated', 'alias_source_equivalence_verified')})
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

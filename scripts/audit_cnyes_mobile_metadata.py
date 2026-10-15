"""Read-only current mobile-head metadata inventory, with no article HTTP.

Read selected columns once per distinct immutable batch. Never load response
envelopes or body text, acquire the writer lock, rewrite a head, or call a source.
The result proves a metadata join, not raw-response or full-history acceptance.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time
from urllib.parse import urlsplit

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_queue import source_path, utc_now


COLUMNS = ('article_id', 'body_sha256', 'published_at_raw', 'authors_json',
           'declared_canonical_url', 'final_url', 'extraction_method',
           'body_completeness', 'data_fingerprint')


def audit(root, *, maximum_heads=100000):
    started, cpu = time.monotonic(), time.process_time()
    db = sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        observed = utc_now()
        rows = db.execute("SELECT id,url,state,body_sha256,parquet_path,captured_at_utc FROM articles "
                          "WHERE host='m.cnyes.com' AND body_sha256 IS NOT NULL ORDER BY url "
                          "LIMIT ?", (maximum_heads + 1,)).fetchall()
    finally:
        db.close()
    if len(rows) > maximum_heads:
        raise ValueError('mobile_head_inventory_exceeds_explicit_bound')
    groups = defaultdict(list)
    for row in rows:
        groups[row['parquet_path']].append(row)
    counts, methods, states = Counter(), Counter(), Counter()
    issues, errors, clocks = [], [], []
    capture_clocks = [row['captured_at_utc'] for row in rows if row['captured_at_utc']]
    joined = 0
    for relative, heads in groups.items():
        try:
            batch = pq.ParquetFile(source_path(root, relative))
            available = set(batch.schema_arrow.names)
            if not {'article_id', 'body_sha256'} <= available:
                raise ValueError('mobile_head_identity_columns_missing')
            metadata = batch.read(columns=[key for key in COLUMNS if key in available], use_threads=False)
            selected = {row['id'].hex(): row for row in heads}
            # A repeated article ID is not a unique immutable head witness.
            found = Counter()
            for record in metadata.to_pylist():
                key = record['article_id']
                row = selected.get(key)
                if row is None:
                    continue
                found[key] += 1
                if record['body_sha256'] != row['body_sha256']:
                    raise ValueError('mobile_head_body_digest_join_mismatch')
                joined += 1
                states[row['state']] += 1
                methods[record.get('extraction_method') or 'unknown'] += 1
                flags = []
                raw_clock = record.get('published_at_raw')
                if not raw_clock:
                    counts['missing_publication_clock'] += 1
                    flags.append('missing_publication_clock')
                else:
                    try:
                        clock = datetime.fromisoformat(raw_clock.replace('Z', '+00:00'))
                        if clock.tzinfo is not None:
                            clocks.append(clock.astimezone(timezone.utc).isoformat())
                        else:
                            counts['source_clock_without_declared_timezone'] += 1
                    except (ValueError, TypeError, AttributeError):
                        counts['source_clock_not_iso_parseable'] += 1
                authors = json.loads(record.get('authors_json') or '[]')
                if not isinstance(authors, list) or any(not isinstance(name, str) for name in authors):
                    raise ValueError('mobile_head_author_metadata_invalid')
                if not any(name.strip() for name in authors):
                    counts['missing_source_byline'] += 1
                    flags.append('missing_source_byline')
                final = urlsplit(record.get('final_url') or '')
                canonical = urlsplit(record.get('declared_canonical_url') or '')
                same_id_alias = (final.hostname == 'm.cnyes.com' and canonical.netloc == 'news.cnyes.com'
                                 and canonical.scheme == 'https' and canonical.path == final.path
                                 and re.fullmatch(r'/news/id/[0-9]+', final.path) is not None
                                 and not canonical.query and not canonical.fragment)
                if same_id_alias:
                    counts['source_declared_same_id_mobile_alias'] += 1
                if record.get('body_completeness') == 'article_container_unverified':
                    counts['unverified_body_container'] += 1
                    flags.append('unverified_body_container')
                if same_id_alias and flags:
                    counts['canonical_alias_metadata_replay_candidates'] += 1
                if flags:
                    issues.append({'url': row['url'], 'article_id': key, 'state': row['state'],
                                   'body_sha256': row['body_sha256'], 'parquet_path': relative,
                                   'capture_source_fingerprint': record.get('data_fingerprint'),
                                   'flags': flags, 'same_id_declared_alias': same_id_alias})
            if any(found[key] != 1 for key in selected):
                raise ValueError('mobile_head_not_uniquely_present_in_batch')
        except (OSError, ValueError, KeyError, TypeError) as error:
            errors.append({'parquet_path': relative, 'error': str(error)})
    membership = '\n'.join('\t'.join(str(row[key]) for key in ('url', 'body_sha256', 'parquet_path')) for row in rows)
    return {'contract_version': 1, 'observed_at_utc': observed,
            'scope': 'frozen current m.cnyes.com stored URL heads; metadata-column join only',
            'current_mobile_url_heads': len(rows), 'metadata_rows_joined': joined,
            'distinct_batches_read': len(groups), 'states': dict(states), 'extraction_methods': dict(methods),
            'findings': dict(counts), 'issue_url_heads': len(issues), 'issues': issues, 'errors': errors,
            'source_aware_publication_min_utc': min(clocks) if clocks else None,
            'source_aware_publication_max_utc': max(clocks) if clocks else None,
            'capture_min_utc': min(capture_clocks) if capture_clocks else None,
            'capture_max_utc': max(capture_clocks) if capture_clocks else None,
            'membership_sha256': hashlib.sha256(membership.encode()).hexdigest(),
            'metadata_inventory_accepted': joined == len(rows) and not errors,
            'raw_response_integrity_verified': False, 'source_response_replay_complete': False,
            'required_metadata_present': not issues and not errors,
            'all_source_metadata_correct': False,  # No raw-response/clock replay in this inventory.
            'http_requests': 0, 'body_text_read': False, 'queue_modified': False,
            'publish': False, 'remote_training_export': False, 'all_history_complete': False,
            'wall_seconds': time.monotonic() - started, 'cpu_seconds': time.process_time() - cpu}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--maximum-heads', type=int, default=100000)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if not 1 <= args.maximum_heads <= 100000:
        parser.error('maximum-heads must be 1..100000; exceeding it is an explicit rejection, not truncation')
    config, _, repo = load_config(args.config)
    result = audit(repo / config['root'], maximum_heads=args.maximum_heads)
    result['implementation_sha256'] = sha256_file(Path(__file__).resolve())
    atomic_write_json(args.output, result)
    print(json.dumps({key: value for key, value in result.items() if key != 'issues'}, ensure_ascii=False))
    return 0 if result['metadata_inventory_accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

"""Sample clock formats from already-verified provider heads, without prose or HTTP.

This bounded diagnostic does not change published times or claim the sample is
representative. Reuse is limited to one unchanged source snapshot and unchanged
receipt/Parquet identities from an already-completed private audit checkpoint.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import closing
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json
from scripts.audit_news_provider_storage_readback import snapshot, source_file, stamp, state_snapshot
from stockagent.data.news_clock import combined_publication_update


def classify_clock(raw):
    if not isinstance(raw, str) or not raw.strip():
        return 'missing_or_nonstring'
    try:
        parsed = datetime.fromisoformat(raw.replace('Z', '+00:00'))
        if parsed.tzinfo is not None and parsed.utcoffset() is not None:
            parsed.astimezone(timezone.utc)
            return 'iso_aware'
        return 'iso_date_only' if re.fullmatch(r'\d{4}-\d{2}-\d{2}', raw) else 'iso_naive'
    except (ValueError, OverflowError):
        pass
    # Keep the same literal parser as corpus construction; it never assigns a timezone.
    pair_status, _ = combined_publication_update(raw)
    if pair_status:
        return pair_status
    try:
        parsed = parsedate_to_datetime(raw)
        if parsed.tzinfo is not None and parsed.utcoffset() is not None:
            parsed.astimezone(timezone.utc)
            return 'mail_date_aware'
        return 'mail_date_naive'
    except (ValueError, TypeError, OverflowError, IndexError):
        return 'other'


def audit(root, state_path, *, limit=128, skip_batches=0):
    if type(limit) is not int or not 1 <= limit <= 8192:
        raise ValueError('invalid_batch_limit')
    if type(skip_batches) is not int or skip_batches < 0:
        raise ValueError('invalid_batch_offset')
    root, state_path = root.resolve(), state_path.resolve()
    owner_fd = os.open(root / 'owner.lock', os.O_RDONLY | os.O_NOFOLLOW)
    try:
        fcntl.flock(owner_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        source_before, state_before = snapshot(root), state_snapshot(state_path)
        with closing(sqlite3.connect((root / 'queue.sqlite3').as_uri() + '?mode=ro&immutable=1', uri=True)) as source, \
             closing(sqlite3.connect(state_path.as_uri() + '?mode=ro&immutable=1', uri=True)) as state:
            meta = {key: json.loads(value) for key, value in state.execute('SELECT key,value FROM metadata')}
            if meta['snapshot'] != source_before:
                raise ValueError('source_snapshot_changed')
            job_count, head_count, failures = state.execute('SELECT count(*),coalesce(sum(selected_heads),0),'
                'coalesce(sum(error IS NOT NULL),0) FROM jobs').fetchone()
            if (job_count != meta['target_batches'] or head_count + meta['missing_batch_path_heads'] != meta['stored_heads']
                    or failures or meta['missing_batch_path_heads']):
                raise ValueError('checkpoint_not_fully_verified')
            eligible, eligible_heads, eligible_iso = state.execute('''SELECT count(*),coalesce(sum(selected_heads),0),
                coalesce(sum(json_extract(metadata_json,'$.aware_iso_publication_clocks')),0)
                FROM jobs WHERE error IS NULL
                AND json_extract(metadata_json,'$.aware_iso_publication_clocks') < selected_heads''').fetchone()
            if skip_batches > eligible:
                raise ValueError('batch_offset_outside_scope')
            jobs = list(state.execute('''SELECT path,receipt_path,selected_heads,selected_ids_sha256,
                body_stamp,receipt_stamp,metadata_json FROM jobs WHERE error IS NULL
                AND json_extract(metadata_json,'$.aware_iso_publication_clocks') < selected_heads
                ORDER BY path LIMIT ? OFFSET ?''', (limit, skip_batches)))
            by_path = defaultdict(list)
            for start in range(0, len(jobs), 500):
                chunk = jobs[start:start + 500]
                marks = ','.join('?' for _ in chunk)
                rows = source.execute('SELECT parquet_path,id FROM articles INDEXED BY stored_bodies '
                    'WHERE provider=? AND body_sha256 IS NOT NULL AND parquet_path IN (' + marks + ') '
                    'ORDER BY parquet_path,id', [meta['provider'], *(job[0] for job in chunk)])
                for path, article_id in rows:
                    by_path[path].append(article_id)
            totals = Counter({key: 0 for key in ('iso_aware', 'iso_naive', 'iso_date_only',
                'mail_date_aware', 'mail_date_naive', 'combined_naive_publication_and_update',
                'combined_naive_update_before_publication', 'combined_naive_invalid', 'other', 'missing_or_nonstring')})
            for path, receipt, selected, selected_sha, body_stamp, receipt_stamp, metadata_json in jobs:
                ids = by_path[path]
                if (len(ids) != selected or any(not isinstance(key, bytes) or len(key) != 32 for key in ids)
                        or hashlib.sha256(b''.join(sorted(ids))).hexdigest() != selected_sha):
                    raise ValueError('selected_head_identity_mismatch')
                body_path, receipt_path = source_file(root, path), source_file(root, receipt)
                if stamp(body_path) != json.loads(body_stamp) or stamp(receipt_path) != json.loads(receipt_stamp):
                    raise ValueError('verified_artifact_changed')
                expected, seen, local = {key.hex() for key in ids}, set(), Counter()
                table = pq.read_table(body_path, columns=['article_id', 'published_at_raw'])
                for row in table.to_pylist():
                    article_id = row['article_id']
                    if article_id in expected:
                        if article_id in seen:
                            raise ValueError('selected_head_ambiguous_in_batch')
                        seen.add(article_id)
                        local[classify_clock(row['published_at_raw'])] += 1
                if (seen != expected or local['iso_aware'] != json.loads(metadata_json)['aware_iso_publication_clocks']
                        or stamp(body_path) != json.loads(body_stamp) or stamp(receipt_path) != json.loads(receipt_stamp)):
                    raise ValueError('sample_or_artifact_identity_changed')
                totals.update(local)
            if snapshot(root) != source_before or state_snapshot(state_path) != state_before:
                raise ValueError('source_or_checkpoint_changed')
            return {'provider': meta['provider'], 'eligible_batches': eligible,
                    'eligible_heads': eligible_heads, 'eligible_non_iso_heads': eligible_heads - eligible_iso,
                    'skip_batches': skip_batches, 'sampled_batches': len(jobs),
                    'sampled_heads': sum(totals.values()), 'clock_formats': dict(totals),
                    'selection': 'first batches by path with at least one head not parsed as ISO-aware',
                    'representative_of_provider': False, 'changes_source_times': False,
                    'publisher_requests': 0, 'production_queue_writes': 0, 'source_snapshot': source_before}
    finally:
        os.close(owner_fd)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('state', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--limit', type=int, default=128)
    parser.add_argument('--skip-batches', type=int, default=0)
    args = parser.parse_args(argv)
    result = audit(args.root, args.state, limit=args.limit, skip_batches=args.skip_batches)
    atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == '__main__':
    main()

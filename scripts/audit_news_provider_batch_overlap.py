"""Find other-provider batches already covered by a completed local audit.

This is a planning hint only. Batch integrity was checked for all versions,
but this query does not validate the other provider's current head pointers.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import fcntl
import json
import os
from pathlib import Path
import sqlite3

from downloader.artifact_io import atomic_write_json
from scripts.audit_news_provider_storage_readback import snapshot, state_snapshot


def inspect(root, state_path, provider):
    root, state_path = root.resolve(), state_path.resolve()
    if not isinstance(provider, str) or not provider or provider != provider.strip():
        raise ValueError('invalid_provider')
    owner = os.open(root / 'owner.lock', os.O_RDONLY | os.O_NOFOLLOW)
    try:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        source_before, state_before = snapshot(root), state_snapshot(state_path)
        with closing(sqlite3.connect((root / 'queue.sqlite3').as_uri() + '?mode=ro&immutable=1', uri=True)) as db:
            db.execute('ATTACH DATABASE ? AS witness', (state_path.as_uri() + '?mode=ro&immutable=1',))
            meta = {key: json.loads(value) for key, value in db.execute('SELECT key,value FROM witness.metadata')}
            count, heads, failures = db.execute('SELECT count(*),coalesce(sum(selected_heads),0),'
                'coalesce(sum(error IS NOT NULL),0) FROM witness.jobs').fetchone()
            if (meta['snapshot'] != source_before or count != meta['target_batches']
                    or heads + meta['missing_batch_path_heads'] != meta['stored_heads']
                    or failures or meta['missing_batch_path_heads']):
                raise ValueError('completed_source_audit_required')
            total, missing = db.execute('SELECT count(*),coalesce(sum(parquet_path IS NULL OR parquet_path=\'\'),0) '
                'FROM articles INDEXED BY stored_bodies WHERE provider=? AND body_sha256 IS NOT NULL', (provider,)).fetchone()
            valid, paths, overlapping, overlapping_paths = db.execute('''SELECT count(*),count(DISTINCT a.parquet_path),
                coalesce(sum(j.path IS NOT NULL),0),count(DISTINCT CASE WHEN j.path IS NOT NULL THEN a.parquet_path END)
                FROM articles AS a INDEXED BY stored_bodies LEFT JOIN witness.jobs AS j
                ON j.path=a.parquet_path AND j.error IS NULL
                WHERE a.provider=? AND a.body_sha256 IS NOT NULL AND a.parquet_path IS NOT NULL AND a.parquet_path<>''
                ''', (provider,)).fetchone()
        if snapshot(root) != source_before or state_snapshot(state_path) != state_before:
            raise ValueError('source_or_checkpoint_changed')
        return {'provider': provider, 'reference_provider': meta['provider'], 'known_stored_heads': total,
                'heads_without_batch_path': missing, 'heads_with_batch_path': valid, 'distinct_batches': paths,
                'heads_in_previously_verified_batches': overlapping, 'previously_verified_batches': overlapping_paths,
                'additional_batches': paths - overlapping_paths,
                'scope': 'candidate batch overlap only; other-provider current head pointers not yet verified',
                'publisher_requests': 0, 'production_queue_writes': 0, 'source_snapshot': source_before}
    finally:
        os.close(owner)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('state', type=Path)
    parser.add_argument('provider')
    parser.add_argument('output', type=Path)
    args = parser.parse_args(argv)
    result = inspect(args.root, args.state, args.provider)
    atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == '__main__':
    main()

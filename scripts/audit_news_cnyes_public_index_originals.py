"""Read-only audit of CNYES public-index originals, receipts and queue rows.

This does not start the collector, fetch publishers, repair the queue or prove
that all historical news exists. It requires a stopped, checkpointed SQLite
database because immutable SQLite intentionally ignores a live WAL.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext import load_config
from downloader.news_fulltext_cnyes_index import compatible_fingerprints
from downloader.news_fulltext_cnyes_index_checkpoint import inspect_retained as _inspect_original
from downloader.news_fulltext_policy import NewsError


VERSION = 2
NAME = re.compile(r'([0-9a-f]{64})-([0-9a-f]{64})\.parquet')
FINGERPRINT = re.compile(r'[0-9a-f]{64}')


def _stat_value(value):
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns, value.st_mode


def _stat(path: Path):
    try:
        value = path.stat(follow_symlinks=False)
    except FileNotFoundError:
        return None
    return _stat_value(value)


def _changed(path, before):
    try:
        return _stat(path) != before
    except OSError:
        return True


def audit(root: Path, source):
    """Return aggregate results and all problematic paths; never mutate inputs."""
    started = time.monotonic()
    root = root.resolve(strict=True)
    db, wal = root / 'queue.sqlite3', root / 'queue.sqlite3-wal'
    journal, tree = root / 'queue.sqlite3-journal', root / 'list_metadata/cnyes'
    database_before, wal_before, journal_before = _stat(db), _stat(wal), _stat(journal)
    if database_before is None or db.is_symlink() or not db.is_file():
        raise ValueError('missing_or_unsafe_database')
    if wal_before is not None and wal_before[2] != 0:
        raise ValueError('nonempty_wal_requires_stopped_checkpointed_owner')
    if journal_before is not None and journal_before[2] != 0:
        raise ValueError('nonempty_journal_requires_stopped_checkpointed_owner')
    if tree.is_symlink() or not tree.is_dir():
        raise ValueError('missing_or_unsafe_public_index_directory')
    tree_before = _stat(tree)
    directories, originals, receipts, issues, file_stamps = [], {}, {}, [], {}
    unreadable_entries = 0
    with os.scandir(tree) as parents:
        for parent in parents:
            if not parent.is_dir(follow_symlinks=False) or not FINGERPRINT.fullmatch(parent.name):
                issues.append({'path': 'list_metadata/cnyes/' + parent.name, 'reason': 'unexpected_directory'})
                continue
            directory = Path(parent.path)
            directories.append((directory, _stat(directory)))
            with os.scandir(directory) as children:
                for child in children:
                    relative = f'list_metadata/cnyes/{parent.name}/{child.name}'
                    try:
                        stamp = _stat_value(child.stat(follow_symlinks=False))
                    except OSError:
                        issues.append({'path': relative, 'reason': 'entry_unreadable_during_scan'})
                        unreadable_entries += 1
                        continue
                    file_stamps[Path(child.path)] = stamp
                    if not stat.S_ISREG(stamp[5]):
                        issues.append({'path': relative, 'reason': 'not_a_regular_file'})
                    elif child.name.endswith('.parquet.receipt.json'):
                        receipts[relative[:-len('.receipt.json')]] = Path(child.path)
                    elif child.name.endswith('.parquet'):
                        originals[relative] = (Path(child.path), parent.name)
                    else:
                        issues.append({'path': relative, 'reason': 'unexpected_file'})
    connection = sqlite3.connect(db.as_uri() + '?mode=ro&immutable=1', uri=True)
    connection.row_factory = sqlite3.Row
    try:
        references = defaultdict(list)
        for row in connection.execute('SELECT * FROM cnyes_public_index_observations'):
            references[row['evidence_path']].append(dict(row))
        heads = connection.execute('SELECT count(*) FROM cnyes_public_index_heads').fetchone()[0]
        dangling_heads = connection.execute('SELECT count(*) FROM cnyes_public_index_heads h '
            'LEFT JOIN cnyes_public_index_observations o '
            'USING(scope_url,page,response_sha256,index_fingerprint,authorization_fingerprint) '
            'WHERE o.scope_url IS NULL').fetchone()[0]
    finally:
        connection.close()
    counts = Counter()
    for relative in sorted(originals):
        path, fingerprint = originals[relative]
        if relative not in receipts:
            issues.append({'path': relative, 'reason': 'missing_receipt'})
            continue
        try:
            outcome = _inspect_original(path, relative, fingerprint, receipts[relative], references.get(relative, []), source)
            counts[outcome] += 1
            if outcome == 'valid_unindexed_original':
                issues.append({'path': relative, 'reason': outcome})
        except (OSError, ValueError, TypeError, KeyError, OverflowError, NewsError) as error:
            reason = error.code if isinstance(error, NewsError) else str(error) if type(error) is ValueError else type(error).__name__
            issues.append({'path': relative, 'reason': reason})
    for relative in sorted(set(receipts) - set(originals)):
        issues.append({'path': relative, 'reason': 'receipt_without_original'})
    for relative in sorted(set(references) - set(originals)):
        issues.append({'path': relative, 'reason': 'observation_without_original'})
    changed_files = [path for path, before in file_stamps.items() if _changed(path, before)]
    for path in sorted(changed_files):
        issues.append({'path': path.relative_to(root).as_posix(), 'reason': 'file_changed_during_audit'})
    changed_directories = sum(_changed(path, before) for path, before in directories)
    database_stable = not any((_changed(db, database_before), _changed(wal, wal_before),
                               _changed(journal, journal_before)))
    stable = (not unreadable_entries and not changed_files and not changed_directories
              and database_stable and not _changed(tree, tree_before))
    return {'contract_version': VERSION, 'kind': 'cnyes_public_index_original_read_only_audit',
        'completed_at_utc': datetime.now(timezone.utc).isoformat(), 'elapsed_seconds': time.monotonic() - started,
        'fingerprint_directories': len(directories), 'originals': len(originals), 'receipts': len(receipts),
        'observation_rows': sum(map(len, references.values())), 'head_rows': heads, 'dangling_heads': dangling_heads,
        'file_metadata_checked': len(file_stamps), 'changed_files': len(changed_files),
        'changed_directories': changed_directories, 'database_metadata_stable': database_stable,
        'snapshot_has_writer_lock': False,
        'outcomes': dict(counts), 'issue_count': len(issues), 'issues': issues, 'input_snapshot_stable': stable,
        'historical_http_status_or_headers_proven': False, 'article_bodies_verified': False,
        'all_history_complete': False, 'publisher_requests': 0, 'production_queue_writes': 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_news_fulltext'))
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--output', type=Path, required=True, help='private report outside the source data root')
    args = parser.parse_args()
    root = args.root.resolve()
    # Atomic replacement targets the directory entry, not an existing output
    # symlink's destination. Check both the actual parent and the destination.
    if args.output.parent.resolve().is_relative_to(root) or args.output.resolve().is_relative_to(root):
        parser.error('output must be outside the input data root')
    _, registry, _ = load_config(args.config)
    result = audit(args.root, registry.sources['cnyes'])
    atomic_write_json(args.output, result, durable=True, sort_keys=True)
    print(json.dumps({key: value for key, value in result.items() if key != 'issues'}, ensure_ascii=False, sort_keys=True))
    return 0 if result['input_snapshot_stable'] and not result['issue_count'] and not result['dangling_heads'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

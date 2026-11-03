"""Read-only inventory of retained CNYES history-page source envelopes.

This is not the owner's repair scan: it never writes the queue, updates a
checkpoint, contacts a publisher, or verifies article bodies. A stopped and
checkpointed database is required because immutable SQLite ignores WAL data.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys
import time

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader import news_fulltext_cnyes_history as history_module
from downloader import news_fulltext_queue as queue_module
from downloader.news_fulltext import load_config
from downloader.news_fulltext_bulk import wordpress_base
from downloader.news_fulltext_cnyes_history import (
    _checked_page_observation, _OriginalEnvelopeAuditCache, scope_key,
)
from downloader.news_fulltext_policy import cnyes_bulk_fingerprint
from downloader.news_fulltext_queue import replay_source_path
from scripts.audit_news_cnyes_public_index_originals import _changed, _stat


def _artifact_stamp(root, relative):
    try:
        path = replay_source_path(root, relative)
        return (str(path.relative_to(root)), _stat(path))
    except (OSError, RuntimeError, ValueError, TypeError):
        return None


def _observation_status(row, source, root, verifier, observe_artifact):
    observation = _checked_page_observation(row)
    if not observation or observation['page'] != row['head_page']:
        return 'unclassifiable', None
    try:
        parsed_scope = scope_key(row['head_scope'])
    except (TypeError, ValueError, IndexError, AttributeError, OverflowError):
        parsed_scope = None
    if not parsed_scope:
        return 'unclassifiable', observation
    if (parsed_scope[0] not in source.cnyes_apis or observation['provider'] != source.provider
            or wordpress_base(observation['request_url']) not in source.cnyes_apis
            or observation['source_bulk_fingerprint'] != cnyes_bulk_fingerprint(source)
            or observation['authorization_fingerprint'] != source.authorization_fingerprint
            or observation['artifact'].get('kind') not in ('canonical_quarantine', 'cnyes_history_source_envelope')):
        return 'outside_contract', observation
    for name in ('parquet_path', 'receipt_path'):
        observe_artifact(observation['artifact'].get(name))
    try:
        status = verifier.check(root, observation['artifact'], observation['request_url'],
                                observation['response_sha256'], source.authorization_fingerprint)
    except (OSError, MemoryError):
        status = 'unavailable'
    return status, observation


def audit(root: Path, source):
    started = time.monotonic()
    root = root.resolve(strict=True)
    code_paths = (Path(history_module.__file__), Path(queue_module.__file__), Path(__file__))
    code_before = tuple(sha256_file(path) for path in code_paths)
    db, wal, journal = root / 'queue.sqlite3', root / 'queue.sqlite3-wal', root / 'queue.sqlite3-journal'
    db_before, wal_before, journal_before = _stat(db), _stat(wal), _stat(journal)
    if db_before is None or db.is_symlink() or not db.is_file():
        raise ValueError('missing_or_unsafe_database')
    if wal_before is not None and wal_before[2] != 0:
        raise ValueError('nonempty_wal_requires_stopped_checkpointed_owner')
    if journal_before is not None and journal_before[2] != 0:
        raise ValueError('nonempty_journal_requires_stopped_checkpointed_owner')
    if not source or not source.allowed or not source.cnyes_apis:
        raise ValueError('cnyes_source_disabled')

    paths, changed, unavailable = {}, set(), set()

    def observe_artifact(relative):
        if not isinstance(relative, str):
            unavailable.add('<invalid-path-field>')
            return
        stamp = _artifact_stamp(root, relative)
        if stamp is None:
            unavailable.add(relative)
        if relative not in paths:
            paths[relative] = stamp
        elif paths[relative] != stamp:
            changed.add(relative)

    counts, aliases = Counter(), Counter()
    verifier = _OriginalEnvelopeAuditCache()
    conn = sqlite3.connect(db.as_uri() + '?mode=ro&immutable=1', uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute('SELECT h.scope_url AS head_scope,h.page AS head_page,'
            'h.observation_sha256 AS head_sha,o.* FROM cnyes_history_page_heads h '
            'LEFT JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
            'ORDER BY h.scope_url,h.page')
        for index, row in enumerate(rows, 1):
            status, observation = _observation_status(row, source, root, verifier, observe_artifact)
            counts[status] += 1
            if observation and observation['scope_url'] != row['head_scope']:
                aliases[status] += 1
            if index % 5000 == 0:
                print(f'checked_history_heads={index}', file=sys.stderr, flush=True)
    finally:
        conn.close()
    for relative, first_stamp in paths.items():
        if _artifact_stamp(root, relative) != first_stamp:
            changed.add(relative)
    db_stable = not any((_changed(db, db_before), _changed(wal, wal_before), _changed(journal, journal_before)))
    code_stable = code_before == tuple(sha256_file(path) for path in code_paths)
    stable = db_stable and code_stable and not changed and not unavailable
    return {
        'contract': 'cnyes_history_original_read_only_inventory_v1',
        'completed_at_utc': datetime.now(timezone.utc).isoformat(),
        'elapsed_seconds': time.monotonic() - started,
        'head_rows': sum(counts.values()), 'original_envelope_outcomes': dict(counts),
        'alias_head_outcomes': dict(aliases), 'referenced_paths': len(paths),
        'uninspectable_paths': len(unavailable), 'changed_paths': len(changed),
        'database_metadata_stable': db_stable, 'code_fingerprints_stable': code_stable,
        'input_snapshot_stable': stable,
        'snapshot_has_writer_lock': False,
        'owner_repair_scan_completed': False, 'alias_request_identity_proven': False,
        'article_bodies_verified': False, 'all_history_complete': False,
        'publisher_requests': 0, 'production_queue_writes': 0,
        'owner_source_sha256': code_before[0], 'path_resolver_source_sha256': code_before[1],
        'inventory_source_sha256': code_before[2],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_news_fulltext'))
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--output', type=Path, required=True, help='private report outside the input root')
    args = parser.parse_args()
    root = args.root.resolve()
    if args.output.parent.resolve().is_relative_to(root) or args.output.resolve().is_relative_to(root):
        parser.error('output must be outside the input data root')
    _, registry, _ = load_config(args.config)
    result = audit(args.root, registry.sources['cnyes'])
    atomic_write_json(args.output, result, durable=True, sort_keys=True)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result['input_snapshot_stable'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

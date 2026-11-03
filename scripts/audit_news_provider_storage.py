"""Resumable, offline integrity audit of one provider's stored news heads.

The canonical immutable batch verifier remains responsible for receipt, raw
and body validation. This tool does not fetch, publish, alter source data or
claim publisher history is complete. Its private SQLite checkpoint is separate
from the news root and can only resume against the same quiescent source and
selected verifier code identities.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import fcntl
import hashlib
from importlib.metadata import version
from itertools import groupby, zip_longest
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys
from types import SimpleNamespace

import pyarrow
import pyarrow.parquet as parquet

from downloader.artifact_io import sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_queue import source_path, utc_now
from scripts.benchmark_cnyes_acquisition import verify_batches
from scripts.audit_news_provider_storage_readback import readback, state_snapshot


CONTRACT_VERSION = 4
REPO = Path(__file__).resolve().parent.parent
VERIFIER_FILES = (
    'scripts/audit_news_provider_storage.py',
    'scripts/audit_news_provider_storage_readback.py',
    'scripts/benchmark_cnyes_acquisition.py',
    'downloader/news_fulltext_queue.py',
    'downloader/news_fulltext_public_originals.py',
    'downloader/news_fulltext_policy.py',
    'downloader/news_receipt_io.py',
    'downloader/artifact_io.py',
    'stockagent/private_json.py',
)


def _stamp(path: Path):
    try:
        s = path.stat()
        return [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns]
    except FileNotFoundError:
        return None


def _snapshot(root: Path):
    paths = [root / 'queue.sqlite3', root / 'queue.sqlite3-wal', root / 'queue.sqlite3-journal']
    stamps = [_stamp(path) for path in paths]
    if stamps[0] is None or any(s is not None and s[2] for s in stamps[1:]):
        raise ValueError('immutable_snapshot_requires_empty_wal_and_journal')
    # SQLite may remove or recreate an empty sidecar without changing the DB.
    # Nonempty sidecars still reject the snapshot on every check.
    return [stamps[0], None, None]


def _identity():
    return {'files': {name: sha256_file(REPO / name) for name in VERIFIER_FILES},
            'python': list(sys.version_info[:3]), 'sqlite': sqlite3.sqlite_version,
            'pyarrow': pyarrow.__version__,
            'beautifulsoup4': version('beautifulsoup4'), 'lxml': version('lxml')}


def _digest_ids(ids):
    digest = hashlib.sha256()
    for key in sorted(ids):
        digest.update(key)
    return digest.hexdigest()


def _selected_metadata(records, heads, *, provider=None, body_digests=None):
    """Inspect only selected verified rows; never persist prose or raw bytes."""
    selected = {key.hex(): (url, size, captured) for key, url, size, captured in heads}
    found = set()
    totals = {'declared_publication_clocks': 0, 'aware_iso_publication_clocks': 0,
              'aware_mail_publication_clocks': 0,
              'nonempty_author_lists': 0, 'invalid_author_json': 0,
              'per_article_license_verified': 0, 'first_aware_utc': None, 'last_aware_utc': None}
    for record in records:
        key = record['article_id']
        if key not in selected:
            continue
        if key in found:
            raise ValueError('selected_head_ambiguous_in_batch')
        found.add(key)
        if provider is not None and record.get('provider') != provider:
            raise ValueError('selected_head_provider_mismatch')
        if body_digests is not None and record.get('body_sha256') != body_digests[key]:
            raise ValueError('selected_head_digest_mismatch')
        url, size, captured = selected[key]
        if (record.get('requested_url') != url or record.get('body_bytes') != size
                or record.get('captured_at_utc') != captured):
            raise ValueError('selected_head_metadata_mismatch')
        clock = record.get('published_at_raw')
        if isinstance(clock, str) and clock.strip():
            totals['declared_publication_clocks'] += 1
            try:
                parsed = datetime.fromisoformat(clock.replace('Z', '+00:00'))
            except (ValueError, OverflowError):
                try:
                    parsed = parsedate_to_datetime(clock)
                    kind = 'aware_mail_publication_clocks'
                except (ValueError, TypeError, OverflowError, IndexError):
                    parsed, kind = None, None
            else:
                kind = 'aware_iso_publication_clocks'
            try:
                stamp = (parsed.astimezone(timezone.utc).isoformat()
                         if parsed is not None and parsed.tzinfo is not None and parsed.utcoffset() is not None else None)
            except (ValueError, OverflowError):
                stamp = None
            if stamp is not None:
                totals[kind] += 1
                totals['first_aware_utc'] = min(totals['first_aware_utc'], stamp) if totals['first_aware_utc'] else stamp
                totals['last_aware_utc'] = max(totals['last_aware_utc'], stamp) if totals['last_aware_utc'] else stamp
        try:
            authors = json.loads(record.get('authors_json') or 'null')
            if isinstance(authors, list):
                totals['nonempty_author_lists'] += bool(authors)
            elif authors is not None:
                totals['invalid_author_json'] += 1
        except (ValueError, TypeError):
            totals['invalid_author_json'] += 1
        totals['per_article_license_verified'] += record.get('per_article_license_verified') is True
    if found != set(selected):
        raise ValueError('selected_head_missing_from_batch')
    return totals


def _load_meta(db):
    return {key: json.loads(value) for key, value in db.execute('SELECT key,value FROM metadata')}


def _save_meta(db, values):
    db.executemany('INSERT INTO metadata(key,value) VALUES(?,?)',
                   [(key, json.dumps(value, sort_keys=True)) for key, value in values.items()])


def _observed_stamp(root, relative):
    try:
        return _stamp(source_path(root, relative)) if relative else None
    except (OSError, ValueError, TypeError):
        return None


def _reference_identity(root, state_path, log_path, implementation_root):
    """Independently read back a finished audit before reusing its batch proof."""
    state_path, log_path = state_path.resolve(strict=True), log_path.resolve(strict=True)
    implementation_root = implementation_root.resolve(strict=True)
    before = state_snapshot(state_path)
    state_hash, log_hash = sha256_file(state_path), sha256_file(log_path)
    result = readback(root, state_path, log_path, implementation_root=implementation_root)
    if (not result['accepted'] or not result['provider_head_pointer_consistency']['accepted']
            or before != state_snapshot(state_path) or state_hash != sha256_file(state_path)
            or log_hash != sha256_file(log_path)):
        raise ValueError('reference_audit_not_accepted_or_changed')
    return {'state_path': str(state_path), 'log_path': str(log_path),
            'implementation_root': str(implementation_root), 'state_stamp': before,
            'state_sha256': state_hash, 'log_sha256': log_hash,
            'snapshot': result['snapshot'], 'scope_sha256': result['scope_sha256'],
            'provider': result['provider']}


def _check_reference_identity(reference):
    if reference is None:
        return
    state_path, log_path = Path(reference['state_path']), Path(reference['log_path'])
    if (state_snapshot(state_path) != reference['state_stamp']
            or sha256_file(state_path) != reference['state_sha256']
            or sha256_file(log_path) != reference['log_sha256']):
        raise ValueError('reference_audit_changed')


def _reused_records(root, path, receipt, witness):
    """Read only head/metadata columns after checking the previously proven files."""
    if (receipt != witness[0] or _observed_stamp(root, path) != json.loads(witness[1])
            or _observed_stamp(root, receipt) != json.loads(witness[2])):
        raise ValueError('reference_batch_artifact_changed')
    columns = ['article_id', 'provider', 'requested_url', 'body_sha256', 'body_bytes',
               'captured_at_utc', 'published_at_raw', 'authors_json', 'per_article_license_verified']
    records = parquet.ParquetFile(source_path(root, path)).read(columns=columns).to_pylist()
    if (_observed_stamp(root, path) != json.loads(witness[1])
            or _observed_stamp(root, receipt) != json.loads(witness[2])):
        raise ValueError('reference_batch_artifact_changed')
    return records


def _ensure_previous_files_unchanged(root, state):
    for path, receipt, body_stamp, receipt_stamp in state.execute(
            'SELECT path,receipt_path,body_stamp,receipt_stamp FROM jobs WHERE error IS NULL'):
        try:
            current_body = _stamp(source_path(root, path))
            current_receipt = _stamp(source_path(root, receipt))
        except (OSError, ValueError):
            raise ValueError('previously_verified_artifact_changed:' + path) from None
        if current_body != json.loads(body_stamp) or current_receipt != json.loads(receipt_stamp):
            raise ValueError('previously_verified_artifact_changed:' + path)


def _retry_paths(root, state):
    """Only recheck changed artifacts or a transient local I/O failure."""
    retry = set()
    for path, receipt, old_body, old_receipt, error in state.execute(
            'SELECT path,receipt_path,body_stamp,receipt_stamp,error FROM jobs'):
        # The pinned database identity has not changed; its receipt mapping
        # cannot change within this audit generation.
        stamps = [_observed_stamp(root, relative) for relative in (path, receipt)]
        if (stamps != [json.loads(old_body), json.loads(old_receipt)]
                or (error is not None and error.startswith('local_io:'))):
            retry.add(path)
    return retry


def _verify_complete_scope(source, state, provider):
    """Independently match every selected head to exactly one checkpoint job."""
    rows = source.execute("""SELECT parquet_path,id FROM articles INDEXED BY stored_bodies
        WHERE provider=? AND body_sha256 IS NOT NULL AND parquet_path IS NOT NULL AND parquet_path<>''
        ORDER BY parquet_path,id""", (provider,))
    groups = groupby(rows, key=lambda row: row[0])
    jobs = state.execute('SELECT path,selected_heads,selected_ids_sha256 FROM jobs ORDER BY path')
    for group, job in zip_longest(groups, jobs):
        if group is None or job is None:
            raise ValueError('audit_checkpoint_scope_mismatch')
        path, grouped = group
        ids = [row[1] for row in grouped]
        if (path != job[0] or len(ids) != job[1] or _digest_ids(ids) != job[2]):
            raise ValueError('audit_checkpoint_scope_mismatch')


def _result(state, meta, *, include_errors=True, include_metadata=True):
    row = state.execute('SELECT count(*),coalesce(sum(selected_heads),0),'
                        'coalesce(sum(CASE WHEN error IS NULL THEN selected_heads ELSE 0 END),0),'
                        'coalesce(sum(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END),0),'
                        'coalesce(sum(body_versions),0) FROM jobs').fetchone()
    processed, heads, verified, failed, versions = row
    completed = (processed == meta['target_batches'] and
                 heads + meta['missing_batch_path_heads'] == meta['stored_heads'])
    errors = ([{'path': path, 'selected_heads': count, 'error': error}
               for path, count, error in state.execute(
                   'SELECT path,selected_heads,error FROM jobs WHERE error IS NOT NULL ORDER BY path')]
              if include_errors else [])
    metadata = None
    if include_metadata:
        metadata = {'declared_publication_clocks': 0, 'aware_iso_publication_clocks': 0,
                    'aware_mail_publication_clocks': 0,
                    'nonempty_author_lists': 0, 'invalid_author_json': 0,
                    'per_article_license_verified': 0, 'first_aware_utc': None, 'last_aware_utc': None}
        for (raw,) in state.execute('SELECT metadata_json FROM jobs WHERE error IS NULL'):
            value = json.loads(raw)
            for key in ('declared_publication_clocks', 'aware_iso_publication_clocks', 'aware_mail_publication_clocks', 'nonempty_author_lists',
                        'invalid_author_json', 'per_article_license_verified'):
                metadata[key] += value[key]
            for key, function in (('first_aware_utc', min), ('last_aware_utc', max)):
                if value[key]:
                    metadata[key] = function(metadata[key], value[key]) if metadata[key] else value[key]
        metadata['bounds_cover_all_verified_heads'] = bool(verified) and (
            metadata['aware_iso_publication_clocks'] + metadata['aware_mail_publication_clocks'] == verified)
    return {'contract_version': CONTRACT_VERSION, 'provider': meta['provider'],
            'snapshot': meta['snapshot'], 'implementation': meta['implementation'],
            'stored_heads_in_snapshot': meta['stored_heads'], 'target_batches': meta['target_batches'],
            'missing_batch_path_heads': meta['missing_batch_path_heads'],
            'processed_batches': processed, 'processed_heads': heads,
            'verified_stored_heads': verified, 'failed_batches': failed,
            'body_versions_verified_in_successful_batches': versions, 'errors': errors,
            'verified_head_metadata': metadata,
            'completed_scope': completed,
            'accepted': bool(meta['stored_heads']) and completed and not failed and not meta['missing_batch_path_heads'],
            'verification_scope': 'stored heads of this provider in one quiescent local snapshot; all immutable versions in their batches',
            'current_parser_replay_verified': None, 'publisher_history_complete': None,
            'historical_completeness_verified': False,
            'publisher_requests': 0, 'production_queue_writes': 0, 'publish': False}


def audit(root: Path, provider: str, state_path: Path, *, max_batches=None, progress_every=64,
          reference_state=None, reference_log=None, reference_implementation_root=None):
    """Verify all eligible batches, or a bounded prefix that can be resumed.

    A successful batch is committed to the private checkpoint before it is
    skipped on a later run. Only unchanged source DB and verified file stamps
    permit reuse. Failed batches remain explicit and do not stop other batches.
    """
    if not isinstance(provider, str) or not provider or provider != provider.strip():
        raise ValueError('invalid_provider')
    if max_batches is not None and (type(max_batches) is not int or max_batches < 1):
        raise ValueError('invalid_max_batches')
    if type(progress_every) is not int or progress_every < 1:
        raise ValueError('invalid_progress_interval')
    if state_path.is_symlink():
        raise ValueError('audit_state_must_not_be_symlink')
    root, state_path = root.resolve(), state_path.resolve()
    if state_path.is_relative_to(root):
        raise ValueError('audit_state_must_be_outside_news_root')
    if any(item is not None for item in (reference_state, reference_log, reference_implementation_root)):
        if any(item is None for item in (reference_state, reference_log, reference_implementation_root)):
            raise ValueError('complete_reference_audit_required')
        if Path(reference_state).resolve() == state_path:
            raise ValueError('reference_and_output_state_must_differ')
        reference = _reference_identity(root, Path(reference_state), Path(reference_log),
                                        Path(reference_implementation_root))
    else:
        reference = None
    state_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    owner_fd = os.open(root / 'owner.lock', os.O_RDONLY | os.O_NOFOLLOW)
    audit_fd = None
    try:
        fcntl.flock(owner_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        audit_fd = os.open(str(state_path) + '.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        fcntl.flock(audit_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        snapshot, implementation = _snapshot(root), _identity()
        if reference is not None and reference['snapshot'] != snapshot:
            raise ValueError('reference_source_snapshot_changed')
        _check_reference_identity(reference)
        try:
            created = os.open(state_path, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        except FileExistsError:
            info = state_path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
                raise ValueError('audit_state_must_be_private_regular_file') from None
        else:
            os.close(created)
        with closing(sqlite3.connect((root / 'queue.sqlite3').as_uri() + '?mode=ro&immutable=1', uri=True)) as source:
            source.execute('PRAGMA query_only=ON')
            if reference is not None:
                source.execute('ATTACH DATABASE ? AS witness',
                               (Path(reference['state_path']).as_uri() + '?mode=ro&immutable=1',))
            with closing(sqlite3.connect(state_path)) as state:
                state.execute('PRAGMA journal_mode=WAL')
                state.execute('PRAGMA synchronous=FULL')
                state.execute('CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
                state.execute('''CREATE TABLE IF NOT EXISTS jobs(
                    path TEXT PRIMARY KEY, receipt_path TEXT, selected_heads INTEGER NOT NULL,
                    selected_ids_sha256 TEXT NOT NULL, body_stamp TEXT, receipt_stamp TEXT,
                    body_versions INTEGER NOT NULL, error TEXT, verified_at_utc TEXT NOT NULL,
                    metadata_json TEXT NOT NULL)''')
                meta = _load_meta(state)
                if not meta:
                    count, missing, batches = source.execute('''SELECT count(*),
                        coalesce(sum(parquet_path IS NULL OR parquet_path=''),0),
                        count(DISTINCT CASE WHEN parquet_path IS NOT NULL AND parquet_path<>'' THEN parquet_path END)
                        FROM articles INDEXED BY stored_bodies WHERE provider=? AND body_sha256 IS NOT NULL''',
                        (provider,)).fetchone()
                    meta = {'contract_version': CONTRACT_VERSION, 'provider': provider, 'snapshot': snapshot,
                            'implementation': implementation, 'stored_heads': count,
                            'missing_batch_path_heads': missing, 'target_batches': batches,
                            'created_at_utc': utc_now(), 'reference': reference}
                    with state:
                        _save_meta(state, meta)
                elif (meta.get('contract_version') != CONTRACT_VERSION or meta.get('provider') != provider
                      or meta.get('snapshot') != snapshot or meta.get('implementation') != implementation
                      or meta.get('reference') != reference):
                    raise ValueError('audit_checkpoint_identity_changed')
                last = state.execute('SELECT max(path) FROM jobs').fetchone()[0]
                retry = _retry_paths(root, state)
                existing = {row[0]: (row[1], row[2]) for row in state.execute(
                    'SELECT path,selected_heads,selected_ids_sha256 FROM jobs')}
                sql = """SELECT parquet_path,id,url,body_bytes,captured_at_utc,body_sha256 FROM articles INDEXED BY stored_bodies
                    WHERE provider=? AND body_sha256 IS NOT NULL AND parquet_path IS NOT NULL AND parquet_path<>''"""
                args = [provider]
                if retry:
                    sql += ' AND parquet_path>=?'
                    args.append(min(retry))
                elif last is not None:
                    sql += ' AND parquet_path>?'
                    args.append(last)
                sql += ' ORDER BY parquet_path,id'
                processed_now = 0
                rows = source.execute(sql, args)
                try:
                    for path, grouped in groupby(rows, key=lambda row: row[0]):
                        if not isinstance(path, str):
                            raise ValueError('invalid_batch_path_type')
                        if path in existing and path not in retry:
                            continue
                        selected_rows = list(grouped)
                        heads = [(row[1], row[2], row[3], row[4]) for row in selected_rows]
                        ids = [head[0] for head in heads]
                        if any(not isinstance(key, bytes) or len(key) != 32 for key in ids) or len(set(ids)) != len(ids):
                            raise ValueError('invalid_selected_article_ids')
                        digest = _digest_ids(ids)
                        body_digests = {row[1].hex(): row[5] for row in selected_rows}
                        if path in existing and existing[path] != (len(ids), digest):
                            raise ValueError('audit_checkpoint_group_changed')
                        receipt, body_stamp, receipt_stamp, versions, error, metadata = None, None, None, 0, None, {}
                        try:
                            registered = source.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (path,)).fetchone()
                            if registered is None:
                                raise ValueError('batch_not_committed')
                            receipt = registered[0]
                            body_stamp = _observed_stamp(root, path)
                            receipt_stamp = _observed_stamp(root, receipt)
                            witness = (source.execute('SELECT receipt_path,body_stamp,receipt_stamp,body_versions '
                                'FROM witness.jobs WHERE path=? AND error IS NULL', (path,)).fetchone()
                                if reference is not None else None)
                            if witness is None:
                                proof = verify_batches(SimpleNamespace(root=root, db=source), [path],
                                                       head_ids=set(ids), include_records=True)
                                if (not proof['accepted'] or body_stamp is None or receipt_stamp is None
                                        or body_stamp != _observed_stamp(root, path)
                                        or receipt_stamp != _observed_stamp(root, receipt)):
                                    raise ValueError('batch_artifact_changed_during_verification')
                                versions = proof['body_versions']
                                records = proof.pop('private_verified_records')
                            else:
                                records = _reused_records(root, path, receipt, witness)
                                versions = witness[3]
                            metadata = _selected_metadata(records, heads, provider=provider, body_digests=body_digests)
                        except (OSError, ValueError, KeyError, TypeError, IndexError, pyarrow.ArrowException) as exc:
                            error = ('local_io:' if isinstance(exc, OSError) else '') + type(exc).__name__ + ':' + str(exc)[:160]
                            versions, metadata = 0, {}
                        state.execute('''INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?,?)
                            ON CONFLICT(path) DO UPDATE SET receipt_path=excluded.receipt_path,
                            body_stamp=excluded.body_stamp,receipt_stamp=excluded.receipt_stamp,
                            body_versions=excluded.body_versions,error=excluded.error,
                            verified_at_utc=excluded.verified_at_utc,metadata_json=excluded.metadata_json''',
                            (path, receipt, len(ids), digest, json.dumps(body_stamp),
                             json.dumps(receipt_stamp), versions, error, utc_now(), json.dumps(metadata, sort_keys=True)))
                        processed_now += 1
                        if processed_now % 32 == 0:
                            state.commit()
                        if processed_now % progress_every == 0:
                            state.commit()
                            result = _result(state, meta, include_errors=False, include_metadata=False)
                            print(json.dumps({k: result[k] for k in ('provider', 'processed_batches',
                                'target_batches', 'verified_stored_heads', 'failed_batches')}, sort_keys=True), flush=True)
                        if max_batches is not None and processed_now >= max_batches:
                            break
                    state.commit()
                except BaseException:
                    state.rollback()
                    raise
                _check_reference_identity(reference)
                if _snapshot(root) != snapshot or _identity() != implementation:
                    raise ValueError('audit_input_changed_during_run')
                result = _result(state, meta)
                result['scope_membership_verified'] = False
                if result['completed_scope']:
                    _verify_complete_scope(source, state, provider)
                    if _snapshot(root) != snapshot or _identity() != implementation:
                        raise ValueError('audit_input_changed_during_scope_readback')
                    result['scope_membership_verified'] = True
                _ensure_previous_files_unchanged(root, state)
                if reference is not None:
                    reference_paths = {row[0] for row in source.execute('SELECT path FROM witness.jobs WHERE error IS NULL')}
                    successful_paths = [row[0] for row in state.execute('SELECT path FROM jobs WHERE error IS NULL')]
                    result['reference_audit'] = reference
                    result['reused_previously_verified_batches'] = sum(path in reference_paths for path in successful_paths)
                    result['newly_verified_batches'] = sum(path not in reference_paths for path in successful_paths)
                    result['verification_scope'] += '; reused batches carry an independently read-back earlier batch proof'
                _check_reference_identity(reference)
                result['verified_at_utc'] = utc_now()
                return result
    finally:
        if audit_fd is not None:
            os.close(audit_fd)
        os.close(owner_fd)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--provider', required=True)
    parser.add_argument('--state', type=Path, required=True, help='private checkpoint outside the news source root')
    parser.add_argument('--max-batches', type=int, help='optional bounded work for one invocation')
    parser.add_argument('--reference-state', type=Path)
    parser.add_argument('--reference-log', type=Path)
    parser.add_argument('--reference-implementation-root', type=Path)
    args = parser.parse_args(argv)
    config, _, repo = load_config(args.config)
    result = audit(repo / config['root'], args.provider, args.state, max_batches=args.max_batches,
                   reference_state=args.reference_state, reference_log=args.reference_log,
                   reference_implementation_root=args.reference_implementation_root)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    return 0 if result['accepted'] or not result['completed_scope'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

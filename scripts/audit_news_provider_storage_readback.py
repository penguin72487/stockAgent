"""Independently reconcile one finished provider audit without reading prose."""
from __future__ import annotations

from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
import argparse
import fcntl
import hashlib
from itertools import groupby, zip_longest
import json
import os
from pathlib import Path
import sqlite3

from downloader.artifact_io import atomic_write_json, sha256_file

REPO = Path(__file__).resolve().parent.parent


def stamp(path):
    try:
        item = path.stat()
        return [item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns, item.st_ctime_ns]
    except FileNotFoundError:
        return None


def snapshot(root):
    db = stamp(root / 'queue.sqlite3')
    sidecars = [stamp(root / name) for name in ('queue.sqlite3-wal', 'queue.sqlite3-journal')]
    if db is None or any(item and item[2] for item in sidecars):
        raise ValueError('source_is_not_quiescent')
    return [db, None, None]


def state_snapshot(state_path):
    state = stamp(state_path)
    sidecars = [stamp(Path(str(state_path) + suffix)) for suffix in ('-wal', '-journal')]
    if state is None or any(item and item[2] for item in sidecars):
        raise ValueError('audit_state_not_quiescent')
    return state


def source_file(root, relative):
    if not isinstance(relative, str):
        raise ValueError('missing_artifact_path')
    path = (root / relative).resolve(strict=True)
    if not path.is_relative_to(root) or not path.is_file():
        raise ValueError('unsafe_artifact')
    return path


def _check_reference(reference):
    state_path = Path(reference['state_path'])
    log_path = Path(reference['log_path'])
    if (state_snapshot(state_path) != reference['state_stamp']
            or sha256_file(state_path) != reference['state_sha256']
            or sha256_file(log_path) != reference['log_sha256']):
        raise ValueError('reference_audit_changed')


def readback(root, state_path, log_path, *, implementation_root=None, _ancestors=frozenset()):
    root, state_path = root.resolve(), state_path.resolve()
    if state_path in _ancestors:
        raise ValueError('cyclic_reference_audit')
    ancestors = _ancestors | {state_path}
    implementation_root = Path(implementation_root or REPO).resolve(strict=True)
    reports = []
    for line in log_path.read_text().splitlines():
        if line.startswith('{'):
            entry = json.loads(line)
            if 'scope_membership_verified' in entry:
                reports.append(entry)
    if len(reports) != 1:
        raise ValueError('one_finished_audit_report_required')
    report = reports[0]
    if not report['completed_scope'] or not report['scope_membership_verified']:
        raise ValueError('unfinished_audit')
    reference = report.get('reference_audit')
    if reference is not None:
        _check_reference(reference)
        previous = readback(root, Path(reference['state_path']), Path(reference['log_path']),
                            implementation_root=reference['implementation_root'], _ancestors=ancestors)
        if (not previous['accepted'] or not previous['provider_head_pointer_consistency']['accepted']
                or previous['provider'] != reference['provider']
                or previous['scope_sha256'] != reference['scope_sha256']
                or previous['snapshot'] != reference['snapshot']):
            raise ValueError('reference_audit_not_accepted')
        _check_reference(reference)
    fd = os.open(root / 'owner.lock', os.O_RDONLY | os.O_NOFOLLOW)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        source_before = snapshot(root)
        state_before = state_snapshot(state_path)
        with closing(sqlite3.connect((root / 'queue.sqlite3').as_uri() + '?mode=ro&immutable=1', uri=True)) as source, \
             closing(sqlite3.connect(state_path.as_uri() + '?mode=ro&immutable=1', uri=True)) as state:
            meta = {key: json.loads(value) for key, value in state.execute('SELECT key,value FROM metadata')}
            if (meta['snapshot'] != source_before or report['snapshot'] != source_before
                    or report['implementation'] != meta['implementation'] or report['provider'] != meta['provider']
                    or report['contract_version'] != meta['contract_version']
                    or meta.get('reference') != reference):
                raise ValueError('snapshot_or_implementation_mismatch')
            if reference is not None:
                if reference['snapshot'] != source_before:
                    raise ValueError('reference_source_snapshot_changed')
                _check_reference(reference)
                source.execute('ATTACH DATABASE ? AS witness',
                               (Path(reference['state_path']).resolve().as_uri() + '?mode=ro&immutable=1',))
                reference_paths = {row[0] for row in source.execute('SELECT path FROM witness.jobs WHERE error IS NULL')}
                reused, newly_verified = 0, 0
            for name, expected in meta['implementation']['files'].items():
                path = (implementation_root / name).resolve(strict=True)
                if not path.is_relative_to(implementation_root) or not path.is_file():
                    raise ValueError('unsafe_implementation_path:' + name)
                if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                    raise ValueError('implementation_changed:' + name)
            scope = source.execute("""SELECT parquet_path,id FROM articles INDEXED BY stored_bodies
                WHERE provider=? AND body_sha256 IS NOT NULL AND parquet_path IS NOT NULL AND parquet_path<>''
                ORDER BY parquet_path,id""", (meta['provider'],))
            jobs = state.execute('SELECT path,receipt_path,selected_heads,selected_ids_sha256,body_stamp,'
                                 'receipt_stamp,body_versions,error,metadata_json FROM jobs ORDER BY path')
            counts = Counter({key: 0 for key in ('processed_batches', 'processed_heads',
                'verified_stored_heads', 'failed_batches', 'body_versions_verified_in_successful_batches')})
            scope_hash = hashlib.sha256()
            summary = {'declared_publication_clocks': 0, 'aware_iso_publication_clocks': 0,
                       'nonempty_author_lists': 0, 'invalid_author_json': 0,
                       'per_article_license_verified': 0, 'first_aware_utc': None, 'last_aware_utc': None}
            if meta['contract_version'] >= 3:
                summary['aware_mail_publication_clocks'] = 0
            for group, job in zip_longest(groupby(scope, key=lambda row: row[0]), jobs):
                if group is None or job is None:
                    raise ValueError('scope_job_count_mismatch')
                path, grouped = group
                ids = [row[1] for row in grouped]
                ids_hash = hashlib.sha256(b''.join(sorted(ids))).hexdigest()
                if (not isinstance(path, str) or any(not isinstance(key, bytes) or len(key) != 32 for key in ids)
                        or path != job[0] or len(ids) != job[2] or ids_hash != job[3]):
                    raise ValueError('scope_group_identity_mismatch')
                path_bytes = path.encode()
                scope_hash.update(len(path_bytes).to_bytes(4, 'big'))
                scope_hash.update(path_bytes)
                for key in sorted(ids):
                    scope_hash.update(key)
                counts['processed_batches'] += 1
                counts['processed_heads'] += len(ids)
                if job[7] is not None:
                    counts['failed_batches'] += 1
                    continue
                receipt = source.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (path,)).fetchone()
                if not receipt or receipt[0] != job[1]:
                    raise ValueError('batch_receipt_registration_mismatch')
                if (stamp(source_file(root, path)) != json.loads(job[4]) or
                        stamp(source_file(root, receipt[0])) != json.loads(job[5])):
                    raise ValueError('verified_artifact_changed')
                counts['verified_stored_heads'] += len(ids)
                counts['body_versions_verified_in_successful_batches'] += job[6]
                if reference is not None:
                    reused += path in reference_paths
                    newly_verified += path not in reference_paths
                metadata = json.loads(job[8])
                for key in ('declared_publication_clocks', 'aware_iso_publication_clocks',
                            'nonempty_author_lists', 'invalid_author_json', 'per_article_license_verified',
                            *(['aware_mail_publication_clocks'] if meta['contract_version'] >= 3 else [])):
                    summary[key] += metadata[key]
                for key, function in (('first_aware_utc', min), ('last_aware_utc', max)):
                    if metadata[key]:
                        summary[key] = function(summary[key], metadata[key]) if summary[key] else metadata[key]
            summary['bounds_cover_all_verified_heads'] = bool(counts['verified_stored_heads']) and (
                summary['aware_iso_publication_clocks'] + summary.get('aware_mail_publication_clocks', 0)
                == counts['verified_stored_heads'])
            head_count, missing = source.execute("SELECT count(*),coalesce(sum(parquet_path IS NULL OR parquet_path=''),0) "
                "FROM articles INDEXED BY stored_bodies WHERE provider=? AND body_sha256 IS NOT NULL", (meta['provider'],)).fetchone()
            known, path_without_digest, complete_incomplete = source.execute("""SELECT count(*),
                coalesce(sum(CASE WHEN (body_sha256 IS NULL OR body_sha256='')
                    AND parquet_path IS NOT NULL AND parquet_path<>'' THEN 1 ELSE 0 END),0),
                coalesce(sum(CASE WHEN state='complete' AND (body_sha256 IS NULL OR body_sha256=''
                    OR parquet_path IS NULL OR parquet_path='') THEN 1 ELSE 0 END),0)
                FROM articles INDEXED BY dispatch WHERE provider=?""", (meta['provider'],)).fetchone()
            counts['stored_heads_in_snapshot'], counts['missing_batch_path_heads'] = head_count, missing
            counts['target_batches'] = meta['target_batches']
            for key in ('processed_batches', 'processed_heads', 'verified_stored_heads', 'failed_batches',
                        'body_versions_verified_in_successful_batches', 'stored_heads_in_snapshot',
                        'missing_batch_path_heads', 'target_batches'):
                if counts[key] != report[key]:
                    raise ValueError('reported_count_mismatch:' + key)
            expected_acceptance = bool(head_count) and counts['processed_batches'] == meta['target_batches'] and (
                counts['processed_heads'] + missing == head_count) and not counts['failed_batches'] and not missing
            if report['accepted'] != expected_acceptance or summary != report['verified_head_metadata']:
                raise ValueError('acceptance_or_metadata_mismatch')
            if reference is not None and (report['reused_previously_verified_batches'] != reused
                                           or report['newly_verified_batches'] != newly_verified):
                raise ValueError('reference_batch_count_mismatch')
            if snapshot(root) != source_before or state_snapshot(state_path) != state_before:
                raise ValueError('source_or_audit_state_changed_during_readback')
            if reference is not None:
                _check_reference(reference)
            return {'provider': meta['provider'], 'accepted': expected_acceptance, 'counts': dict(counts),
                    'verified_head_metadata': summary, 'scope_sha256': scope_hash.hexdigest(),
                    'provider_head_pointer_consistency': {
                        'known_heads': known, 'path_without_body_sha256': path_without_digest,
                        'complete_heads_missing_body_pointer': complete_incomplete,
                        'accepted': not (path_without_digest or complete_incomplete or missing)},
                    'snapshot': source_before, 'checked_at_utc': datetime.now(timezone.utc).isoformat(),
                    **({'reference_audit': {'provider': reference['provider'], 'scope_sha256': reference['scope_sha256'],
                        'reused_previously_verified_batches': reused, 'newly_verified_batches': newly_verified}}
                       if reference is not None else {}),
                    'publisher_requests': 0, 'production_queue_writes': 0,
                    'scope': 'independent local head membership, artifact stamps and aggregate readback; not a second full body or current-parser replay',
                    'publisher_history_complete': None}
    finally:
        os.close(fd)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('state', type=Path)
    parser.add_argument('log', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--implementation-root', type=Path,
                        help='optional archived copies of the exact recorded verifier files')
    args = parser.parse_args(argv)
    result = readback(args.root, args.state, args.log, implementation_root=args.implementation_root)
    atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result['accepted'] and result['provider_head_pointer_consistency']['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

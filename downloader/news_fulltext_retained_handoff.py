"""Hand independently witnessed local URL hints to the existing news owner.

The certificate is an input, not an article, a source permission, or a body
receipt.  Current source heads, retained-batch identities and registry policy
are rechecked while the canonical owner holds its lock.  Only its existing
content-addressed URL mailbox may mutate the queue.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time

from downloader.news_fulltext_repairs import MAX_PENDING, _directory, _pending, _read_private_request, _write_once
from downloader.news_fulltext_url_metadata import _load, _schema, _source_identity
from downloader.news_fulltext_policy import NewsError

_SHA = re.compile(r'[0-9a-f]{64}\Z')
_MAX_CERTIFICATE_BYTES = 2 << 20


def validate_spec(value, repo):
    if value is None:
        return None
    if (type(value) is not dict or set(value) != {'path', 'sha256'}
            or type(value['path']) is not str or not value['path']
            or Path(value['path']).is_absolute() or type(value['sha256']) is not str
            or not _SHA.fullmatch(value['sha256'])):
        raise ValueError('invalid_retained_link_handoff_spec')
    artifacts = (Path(repo) / 'artifacts').resolve()
    path = (Path(repo) / value['path']).resolve()
    if path == artifacts or not path.is_relative_to(artifacts):
        raise ValueError('retained_link_handoff_outside_artifacts')
    return {'path': str(path), 'sha256': value['sha256'], 'artifacts': str(artifacts)}


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate_handoff_json_key')
        result[key] = value
    return result


def _file_signature(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _read_private(path, *, maximum=None):
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
    try:
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_mode & 0o077
                or maximum is not None and before.st_size > maximum):
            raise ValueError('handoff_input_not_private_regular_file')
        digest, chunks, length = hashlib.sha256(), [], 0
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            while chunk := stream.read(1 << 20):
                length += len(chunk)
                if maximum is not None and length > maximum:
                    raise ValueError('handoff_input_too_large')
                digest.update(chunk)
                if maximum is not None:
                    chunks.append(chunk)
        if _file_signature(os.fstat(fd)) != _file_signature(before):
            raise ValueError('handoff_input_changed')
        return digest.hexdigest(), b''.join(chunks) if maximum is not None else None
    finally:
        os.close(fd)


def _artifact_path(value, artifacts):
    if type(value) is not str:
        raise ValueError('invalid_handoff_artifact_path')
    path = Path(value)
    resolved = path.resolve(strict=True)
    if not path.is_absolute() or resolved != path or not resolved.is_relative_to(artifacts):
        raise ValueError('unsafe_handoff_artifact_path')
    return path


def _source_stamp(root, name):
    if type(name) is not str:
        raise ValueError('invalid_handoff_source_path')
    path = (root / name).resolve(strict=True)
    if not path.is_relative_to(root) or not path.is_file():
        raise ValueError('unsafe_handoff_source_path')
    return list(_file_signature(path.stat()))


def _load_certificate(queue, spec):
    artifacts = Path(spec['artifacts'])
    path = _artifact_path(spec['path'], artifacts)
    digest, raw = _read_private(path, maximum=_MAX_CERTIFICATE_BYTES)
    if digest != spec['sha256']:
        raise ValueError('handoff_certificate_digest_changed')
    cert = json.loads(raw, object_pairs_hook=_unique)
    keys = {'contract', 'source_root', 'source_snapshot', 'reference_scope_sha256', 'stage_manifest',
            'checkpoint', 'independent_readback', 'source_identity', 'candidate_count', 'requests',
            'parents', 'batches', 'target_identity_or_body_verified'}
    if (type(cert) is not dict or set(cert) != keys or cert['contract'] != 'cmnews_retained_link_handoff_v1'
            or cert['source_root'] != str(queue.root.resolve()) or cert['target_identity_or_body_verified'] is not False
            or type(cert['candidate_count']) is not int or cert['candidate_count'] < 1
            or type(cert['requests']) is not list or not cert['requests']
            or type(cert['parents']) is not dict or not cert['parents']
            or type(cert['batches']) is not dict or not cert['batches']):
        raise ValueError('invalid_handoff_certificate')
    return cert


def _validate(queue, spec, cert=None):
    cert = cert if cert is not None else _load_certificate(queue, spec)
    artifacts = Path(spec['artifacts'])
    for key in ('stage_manifest', 'checkpoint', 'independent_readback'):
        item = cert[key]
        if (type(item) is not dict or set(item) != {'path', 'sha256'}
                or type(item['sha256']) is not str or not _SHA.fullmatch(item['sha256'])):
            raise ValueError('invalid_handoff_proof')
        proof_path = _artifact_path(item['path'], artifacts)
        if _read_private(proof_path)[0] != item['sha256']:
            raise ValueError('handoff_proof_digest_changed')
    requests, urls = [], set()
    for item in cert['requests']:
        if (type(item) is not dict or set(item) != {'path', 'sha256', 'targets'}
                or type(item['sha256']) is not str or not _SHA.fullmatch(item['sha256'])
                or type(item['targets']) is not int or not 1 <= item['targets'] <= 512):
            raise ValueError('invalid_handoff_request')
        request_path = _artifact_path(item['path'], artifacts)
        if request_path.name != item['sha256'] + '.json':
            raise ValueError('handoff_request_name_changed')
        request = _load(request_path)
        reread, payload = _read_private_request(request_path, include_payload=True)
        if request != reread or len(request['targets']) != item['targets']:
            raise ValueError('handoff_request_changed')
        for target in request['targets']:
            url = target['url']
            if (url in urls or target['source'] != cert['source_identity']
                    or target['source'] != _source_identity(queue.registry, url)
                    or not target['source']['allowed'] or target['source']['provider'] != 'cmnews'):
                raise ValueError('handoff_target_or_policy_changed')
            urls.add(url)
        requests.append((item, payload))
    if len(urls) != cert['candidate_count']:
        raise ValueError('handoff_target_count_changed')
    root = queue.root.resolve()
    for parent_id, expected in cert['parents'].items():
        if (type(parent_id) is not str or not _SHA.fullmatch(parent_id)
                or type(expected) is not list or len(expected) != 6):
            raise ValueError('invalid_handoff_parent')
        row = queue.db.execute('SELECT url,provider,body_sha256,body_bytes,captured_at_utc,parquet_path '
                               'FROM articles WHERE id=?', (bytes.fromhex(parent_id),)).fetchone()
        if row is None or list(row) != expected:
            raise ValueError('handoff_parent_head_changed')
    for name, item in cert['batches'].items():
        if type(item) is not dict or set(item) != {'receipt_path', 'body_stamp', 'receipt_stamp'}:
            raise ValueError('invalid_handoff_batch')
        row = queue.db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (name,)).fetchone()
        if (row is None or row[0] != item['receipt_path']
                or _source_stamp(root, name) != item['body_stamp']
                or _source_stamp(root, item['receipt_path']) != item['receipt_stamp']):
            raise ValueError('handoff_batch_changed')
    return cert, requests


def next_wake_delay(queue, fallback):
    status = getattr(queue, '_retained_link_handoff_status', None)
    if not status or status['state'] == 'owner_checked':
        return fallback
    retry_at = getattr(queue, '_retained_link_handoff_retry_at', 0.0)
    return min(fallback, max(.05, retry_at - time.monotonic())) if retry_at else fallback


def pump(queue, spec, *, now=None):
    """Retry a finite handoff without blocking the collector on bad evidence."""
    if spec is None:
        return None
    now = time.monotonic() if now is None else now
    previous = getattr(queue, '_retained_link_handoff_status', None)
    if previous and (previous['state'] == 'owner_checked' or now < getattr(queue, '_retained_link_handoff_retry_at', 0)):
        return previous
    status = {'contract_version': 1, 'provider': 'cmnews', 'state': 'validating',
              'plan_sha256': spec['sha256'],
              'candidates': None, 'requests': None, 'submitted_requests': 0,
              'reason': None, 'outcomes': None, 'body_verified': False}
    try:
        inbox = _directory(queue.root, 'url_metadata', 'pending')
        cert = _load_certificate(queue, spec)
        status.update(candidates=cert['candidate_count'], requests=len(cert['requests']))
        _schema(queue)
        submitted, completed = 0, 0
        for item in cert['requests']:
            if (type(item) is not dict or set(item) != {'path', 'sha256', 'targets'}
                    or type(item['sha256']) is not str or not _SHA.fullmatch(item['sha256'])
                    or type(item['targets']) is not int or not 1 <= item['targets'] <= 512):
                raise ValueError('invalid_handoff_request')
            job = queue.db.execute('SELECT targets,next_index,completed_at_utc FROM url_metadata_jobs WHERE request_id=?',
                                   (item['sha256'],)).fetchone()
            if job and (job[0] != item['targets'] or not 0 <= job[1] <= job[0]
                        or (job[2] is None) == (job[1] == job[0])):
                raise ValueError('handoff_job_cursor_inconsistent')
            if job and job[2] is not None:
                submitted += 1
                completed += 1
            elif (inbox / (item['sha256'] + '.json')).exists():
                submitted += 1
        if (sum(item['targets'] for item in cert['requests']) != cert['candidate_count']
                or len({item['sha256'] for item in cert['requests']}) != len(cert['requests'])):
            raise ValueError('handoff_request_membership_inconsistent')
        status['submitted_requests'] = submitted
        if completed == len(cert['requests']):
            outcomes = {}
            for item in cert['requests']:
                for name, count in queue.db.execute('SELECT outcome,count(*) FROM url_metadata_items '
                                                     'WHERE request_id=? GROUP BY outcome', (item['sha256'],)):
                    outcomes[name] = outcomes.get(name, 0) + count
            if sum(outcomes.values()) != cert['candidate_count']:
                raise ValueError('handoff_completed_outcomes_inconsistent')
            status.update(state='owner_checked', outcomes=outcomes)
        elif submitted == len(cert['requests']):
            status['state'] = 'submitted'
            queue._url_metadata_continuation_pending = True
            queue._retained_link_handoff_retry_at = now + 10
        elif len(_pending(inbox)) >= MAX_PENDING:
            status.update(state='waiting_inbox', reason='url_metadata_inbox_full')
            queue._retained_link_handoff_retry_at = now + 2
        else:
            cert, requests = _validate(queue, spec, cert)
            submitted = 0
            for item, payload in requests:
                job = queue.db.execute('SELECT completed_at_utc FROM url_metadata_jobs WHERE request_id=?',
                                       (item['sha256'],)).fetchone()
                destination = inbox / (item['sha256'] + '.json')
                if job and job[0] is not None:
                    submitted += 1
                    continue
                if not destination.exists() and len(_pending(inbox)) >= MAX_PENDING:
                    break
                _write_once(destination, payload)
                submitted += 1
            status['submitted_requests'] = submitted
            status['state'] = 'submitted' if submitted == len(requests) else 'waiting_inbox'
            status['reason'] = None if status['state'] == 'submitted' else 'url_metadata_inbox_full'
            if submitted > completed:
                queue._url_metadata_continuation_pending = True
            queue._retained_link_handoff_retry_at = now + (10 if status['state'] == 'submitted' else 2)
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError, OverflowError, RecursionError, NewsError) as error:
        # A malformed private artifact cannot kill the CNYES/other providers.
        # Log a fixed code only: URLs, local paths and credentials stay private.
        reason = str(error) if isinstance(error, ValueError) and re.fullmatch(r'[a-z_]{1,80}', str(error)) else 'local_input_unavailable'
        status.update(state='deferred', reason=reason)
        queue._retained_link_handoff_retry_at = now + (2 if isinstance(error, (OSError, sqlite3.Error)) else 60)
    queue._retained_link_handoff_status = status
    return status

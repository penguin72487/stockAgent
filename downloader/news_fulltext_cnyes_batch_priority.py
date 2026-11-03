"""Admit finite local batch hints to the existing Cnyes evidence owner.

Hints are not page evidence and never cause a publisher request. The normal
accepted-batch scanner and retained-response validator remain authoritative.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
import time

from downloader.news_fulltext_cnyes_priority import _MAX_BYTES, _MAX_TARGETS, _sha, _stamp, _unique
from downloader.news_fulltext_policy import cnyes_bulk_fingerprint

_CONTRACT = 'cnyes_accepted_batch_priority_v1'
_PREFIX = 'cnyes_accepted_batch_priority_v1:'


def _safe_batch_path(value):
    if type(value) is not str or not value or len(value) > 4096:
        return False
    path = PurePosixPath(value)
    return (not path.is_absolute() and str(path) == value and len(path.parts) >= 2
            and path.parts[0] == 'bodies' and all(p not in ('.', '..') for p in path.parts)
            and path.suffix == '.parquet')


def make_plan(report_bytes, source):
    """Turn a bounded inventory into paths to inspect, never accepted facts."""
    report = json.loads(report_bytes, object_pairs_hook=_unique)
    if (type(report) is not dict or report.get('contract') != 'cnyes_six_scope_current_head_accepted_batches_v1'
            or report.get('publisher_requests') != 0 or report.get('production_queue_writes') != 0
            or type(report.get('candidates')) is not list
            or type(report.get('exact_candidate_pages')) is not int
            or report['exact_candidate_pages'] != len(report['candidates'])):
        raise ValueError('invalid_accepted_batch_priority_source')
    paths = set()
    for candidate in report['candidates']:
        if type(candidate) is not dict or type(candidate.get('artifacts')) is not list or not candidate['artifacts']:
            raise ValueError('invalid_accepted_batch_priority_candidate')
        for artifact in candidate['artifacts']:
            if type(artifact) is not dict or not _safe_batch_path(artifact.get('parquet_path')):
                raise ValueError('invalid_accepted_batch_priority_path')
            paths.add(artifact['parquet_path'])
    if not 1 <= len(paths) <= _MAX_TARGETS:
        raise ValueError('invalid_accepted_batch_priority_count')
    return {'contract': _CONTRACT, 'source_bulk_fingerprint': cnyes_bulk_fingerprint(source),
            'authorization_fingerprint': source.authorization_fingerprint,
            'inventory_sha256': hashlib.sha256(report_bytes).hexdigest(), 'batches': sorted(paths)}


def _load(queue, spec):
    path = Path(spec['path'])
    path_stat = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(path_stat.st_mode) or path_stat.st_size > _MAX_BYTES:
        raise ValueError('unsafe_or_oversize_batch_priority_plan')
    signature = (str(path), spec['sha256'], _stamp(path_stat))
    cached = getattr(queue, '_cnyes_batch_priority_plan_cache', None)
    if cached and cached[0] == signature:
        return cached[1]
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or _stamp(before) != _stamp(path_stat):
            raise ValueError('batch_priority_plan_changed')
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            raw = stream.read(_MAX_BYTES + 1)
        if _stamp(os.fstat(fd)) != _stamp(before) or _stamp(path.stat(follow_symlinks=False)) != _stamp(before):
            raise ValueError('batch_priority_plan_changed')
    finally:
        os.close(fd)
    if len(raw) > _MAX_BYTES or hashlib.sha256(raw).hexdigest() != spec['sha256']:
        raise ValueError('batch_priority_plan_digest_mismatch')
    plan = json.loads(raw, object_pairs_hook=_unique)
    if (type(plan) is not dict or set(plan) != {'contract', 'source_bulk_fingerprint',
            'authorization_fingerprint', 'inventory_sha256', 'batches'} or plan['contract'] != _CONTRACT
            or not all(_sha(plan[k]) for k in ('source_bulk_fingerprint', 'authorization_fingerprint', 'inventory_sha256'))
            or type(plan['batches']) is not list or not 1 <= len(plan['batches']) <= _MAX_TARGETS
            or not all(_safe_batch_path(path) for path in plan['batches'])
            or plan['batches'] != sorted(set(plan['batches']))):
        raise ValueError('invalid_batch_priority_plan')
    queue._cnyes_batch_priority_plan_cache = (signature, plan)
    return plan


def seed(queue, spec, *, stop=None, limit=128, max_seconds=.01):
    """Queue registered batch paths for local scanner retry; preserve deadlines."""
    if spec is None:
        return None
    view = {'state': 'blocked', 'problem': None, 'slice_steps': 0, 'publisher_requests': 0}
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        view['problem'] = 'source_unavailable'
        return view
    try:
        plan = _load(queue, spec)
    except (OSError, ValueError, UnicodeError, TypeError, KeyError, RecursionError, OverflowError):
        view['problem'] = 'invalid_or_unavailable_plan'
        return view
    if (plan['source_bulk_fingerprint'] != cnyes_bulk_fingerprint(source)
            or plan['authorization_fingerprint'] != source.authorization_fingerprint):
        view['problem'] = 'source_contract_changed'
        return view
    from downloader.news_fulltext_cnyes_history import accepted_batch_scan_key, digest, encoded
    scan_key = accepted_batch_scan_key(source)
    marker = _PREFIX + digest((spec['sha256'], scan_key))
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
    try:
        state = json.loads(saved[0]) if saved else {'version': 1, 'offset': 0, 'enqueued': 0, 'existing': 0, 'stale': 0}
        if (type(state) is not dict or set(state) != {'version', 'offset', 'enqueued', 'existing', 'stale'}
                or type(state['version']) is not int or state['version'] != 1
                or any(type(state[k]) is not int or state[k] < 0 for k in ('offset', 'enqueued', 'existing', 'stale'))
                or state['offset'] > len(plan['batches'])
                or state['offset'] != state['enqueued'] + state['existing'] + state['stale']):
            raise ValueError('invalid_checkpoint')
    except (ValueError, TypeError, KeyError, OverflowError):
        view['problem'] = 'invalid_or_unknown_checkpoint'
        return view
    view.update(state='complete' if state['offset'] == len(plan['batches']) else 'scanning',
                targets=len(plan['batches']), processed_targets=state['offset'])
    if limit <= 0 or max_seconds <= 0 or stop is not None and stop.is_set() or view['state'] == 'complete':
        return view
    started = time.monotonic()
    with queue.db:
        if not queue.db.in_transaction:
            queue.db.execute('BEGIN')
        while state['offset'] < len(plan['batches']) and view['slice_steps'] < limit:
            if stop is not None and stop.is_set() or view['slice_steps'] and time.monotonic() - started >= max_seconds:
                break
            path = plan['batches'][state['offset']]
            row = queue.db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (path,)).fetchone()
            if row:
                inserted = queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_local_retries '
                    'VALUES (?,?,?,?,?)', (scan_key, path, row[0], 0, 0)).rowcount
                state['enqueued' if inserted else 'existing'] += 1
            else:
                state['stale'] += 1
            state['offset'] += 1
            view['slice_steps'] += 1
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (marker, encoded(state)))
    view.update(state='complete' if state['offset'] == len(plan['batches']) else 'scanning',
                processed_targets=state['offset'], enqueued=state['enqueued'],
                existing=state['existing'], stale=state['stale'])
    return view

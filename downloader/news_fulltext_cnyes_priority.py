"""Finite, local-only hints for the existing Cnyes original-file auditor.

An observation identifier is neither proof nor permission to fetch. The owner
still checks the current head, source, original bytes and receipt; source
requests retain their existing fences, clocks and publisher policy.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import time

from downloader.news_fulltext_policy import cnyes_bulk_fingerprint

_SHA = re.compile(r'[0-9a-f]{64}\Z')
_CONTRACT = 'cnyes_original_audit_priority_v1'
_MAX_BYTES = 2 << 20
_MAX_TARGETS = 20000
_PREFIX = 'cnyes_original_audit_priority_v1:'
_ACTIVE = 'cnyes_original_audit_active_plan_v1'


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate_json_key')
        result[key] = value
    return result


def _sha(value):
    return type(value) is str and _SHA.fullmatch(value) is not None


def validate_spec(value, repo):
    """Resolve only a finite, digest-pinned private artifact below artifacts/."""
    if value is None:
        return None
    if (type(value) is not dict or set(value) != {'path', 'sha256'}
            or type(value['path']) is not str or not value['path']
            or Path(value['path']).is_absolute() or not _sha(value['sha256'])):
        raise ValueError('invalid_cnyes_original_priority_spec')
    artifacts = (Path(repo) / 'artifacts').resolve()
    path = (Path(repo) / value['path']).resolve()
    if path == artifacts or not path.is_relative_to(artifacts):
        raise ValueError('cnyes_original_priority_not_in_artifacts')
    return {'path': str(path), 'sha256': value['sha256']}


def make_plan(report_bytes, source):
    """Convert a prior offline mismatch plan into hints, never outcomes."""
    report = json.loads(report_bytes, object_pairs_hook=_unique)
    if (type(report) is not dict or report.get('contract') != 'cnyes_unchanged_mismatch_plan_v2'
            or report.get('state') != 'unresolved_evidence_plan' or report.get('all_history_complete') is not False
            or report.get('source_fingerprint') != cnyes_bulk_fingerprint(source)
            or type(report.get('unchanged_mismatches')) is not list):
        raise ValueError('invalid_mismatch_hint_source')
    entries = report['unchanged_mismatches']
    if (type(report.get('unchanged_mismatch_envelopes')) is not int
            or report['unchanged_mismatch_envelopes'] != len(entries)):
        raise ValueError('mismatch_hint_source_count')
    observations = []
    for entry in entries:
        if (type(entry) is not dict or type(entry.get('observation_sha256')) is not list
                or not entry['observation_sha256'] or not all(_sha(x) for x in entry['observation_sha256'])):
            raise ValueError('invalid_mismatch_observation')
        observations.extend(entry['observation_sha256'])
    observations = sorted(set(observations))
    if not observations or len(observations) > _MAX_TARGETS:
        raise ValueError('invalid_mismatch_observation_count')
    return {'contract': _CONTRACT, 'source_bulk_fingerprint': cnyes_bulk_fingerprint(source),
            'authorization_fingerprint': source.authorization_fingerprint,
            'prior_plan_sha256': hashlib.sha256(report_bytes).hexdigest(), 'observations': observations}


def plan_bytes(plan):
    return (json.dumps(plan, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def _stamp(value):
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _load(queue, spec):
    path = Path(spec['path'])
    path_stat = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(path_stat.st_mode) or path_stat.st_size > _MAX_BYTES:
        raise ValueError('unsafe_or_oversize_priority_plan')
    signature = (str(path), spec['sha256'], _stamp(path_stat))
    cached = getattr(queue, '_cnyes_original_priority_plan_cache', None)
    if cached and cached[0] == signature:
        return cached[1]
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or _stamp(before) != _stamp(path_stat):
            raise ValueError('priority_plan_changed')
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            raw = stream.read(_MAX_BYTES + 1)
        if (_stamp(os.fstat(fd)) != _stamp(before) or _stamp(path.stat(follow_symlinks=False)) != _stamp(before)):
            raise ValueError('priority_plan_changed')
    finally:
        os.close(fd)
    if len(raw) > _MAX_BYTES or hashlib.sha256(raw).hexdigest() != spec['sha256']:
        raise ValueError('priority_plan_digest_mismatch')
    plan = json.loads(raw, object_pairs_hook=_unique)
    if (type(plan) is not dict or set(plan) != {'contract', 'source_bulk_fingerprint',
            'authorization_fingerprint', 'prior_plan_sha256', 'observations'} or plan['contract'] != _CONTRACT
            or not all(_sha(plan[k]) for k in ('source_bulk_fingerprint', 'authorization_fingerprint', 'prior_plan_sha256'))
            or type(plan['observations']) is not list or not 1 <= len(plan['observations']) <= _MAX_TARGETS
            or not all(_sha(x) for x in plan['observations'])
            or plan['observations'] != sorted(set(plan['observations']))):
        raise ValueError('invalid_priority_plan')
    queue._cnyes_original_priority_plan_cache = (signature, plan)
    return plan


def _valid_checkpoint_fields(state, total):
    keys = {'version', 'next_index', 'after_scope', 'after_page', 'heads_seen', 'added', 'existing', 'stale'}
    integers = ('next_index', 'after_page', 'heads_seen', 'added', 'existing', 'stale')
    return (type(state) is dict and set(state) == keys and type(state['version']) is int and state['version'] == 1
            and all(type(state[k]) is int and 0 <= state[k] <= 2**53 - 1 for k in integers)
            and type(state['after_scope']) is str and len(state['after_scope']) <= 8192
            and (bool(state['after_scope']) == bool(state['after_page']))
            and state['next_index'] <= total and state['stale'] <= state['next_index']
            and state['heads_seen'] == state['added'] + state['existing']
            and (state['next_index'] < total or not state['after_scope']))


def _valid_checkpoint(state, total):
    # Each completed non-stale observation, and each partial observation with
    # an after-cursor, must have admitted at least one head.
    return (_valid_checkpoint_fields(state, total)
            and state['heads_seen'] >= (state['next_index'] - state['stale']
                                       + int(bool(state['after_scope']))))


def _admit_page(queue, plan_sha, audit_key, scope, page, observation):
    """Add a page version without resetting an existing retry or result."""
    identity = (plan_sha, audit_key, scope, page, observation)
    prior = queue.db.execute('SELECT last_result FROM cnyes_history_original_priority_pages '
        'WHERE plan_sha256=? AND audit_key=? AND scope_url=? AND page=? AND observation_sha256=?',
        identity).fetchone()
    if prior and prior[0] not in ('awaiting', 'superseded'):
        inserted = 0
        if prior[0] in ('invalid', 'unavailable', 'skipped'):
            inserted = queue.db.execute('INSERT OR IGNORE INTO cnyes_history_original_file_retries '
                'VALUES (?,?,?,?,0,0)', (audit_key, scope, page, observation)).rowcount
        return inserted, 0, False
    reactivated = bool(prior and prior[0] == 'superseded')
    if reactivated:
        queue.db.execute("UPDATE cnyes_history_original_priority_pages SET last_result='awaiting' "
            'WHERE plan_sha256=? AND audit_key=? AND scope_url=? AND page=? AND observation_sha256=?',
            identity)
    inserted = queue.db.execute('INSERT OR IGNORE INTO cnyes_history_original_file_retries '
        'VALUES (?,?,?,?,0,0)', (audit_key, scope, page, observation)).rowcount
    tracked = queue.db.execute('INSERT OR IGNORE INTO cnyes_history_original_priority_pages '
        'VALUES (?,?,?,?,?,?)', (*identity, 'awaiting')).rowcount
    return inserted, tracked, reactivated


def notify_head(queue, scope, page, observation):
    """Wake the existing owner only for a durably journalled target head.

    The caller's transaction may still roll back. Invalidating a memory hint
    is conservative in that case; the journal itself rolls back with the head.
    """
    active = getattr(queue, '_cnyes_original_priority_tracking', None)
    watched = getattr(queue, '_cnyes_original_priority_observations', None)
    if active is None or watched is None or observation not in watched:
        return
    if queue.db.execute('SELECT 1 FROM cnyes_history_original_priority_pending '
        'WHERE plan_sha256=? AND audit_key=? AND scope_url=? AND page=? AND observation_sha256=?',
        (*active, scope, page, observation)).fetchone():
        queue._cnyes_original_priority_needs_wake = True
        queue._cnyes_original_priority_view = None
        queue._cnyes_original_priority_wait = None


def seed(queue, spec, *, stop=None, limit=128, max_seconds=.01):
    """Insert resumable local retry hints in the canonical writer transaction."""
    if spec is None:
        queue._cnyes_original_priority_view = None
        queue._cnyes_original_priority_tracking = None
        queue._cnyes_original_priority_observations = None
        queue._cnyes_original_priority_watch_validation = None
        if queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (_ACTIVE,)).fetchone():
            with queue.db:
                queue.db.execute('DELETE FROM queue_settings WHERE key=?', (_ACTIVE,))
        return None
    view = {'contract_version': 1, 'state': 'blocked', 'problem': None,
            'targets': None, 'processed_targets': None, 'stale_targets': None,
            'enqueued_pages': None, 'existing_pages': None,
            'slice_steps': 0, 'checkpoint_recovered': False,
            'publisher_requests': 0, 'accepted_as_originals': False, 'all_history_complete': False}
    queue._cnyes_original_priority_view = view
    queue._cnyes_original_priority_tracking = None
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        view['problem'] = 'source_unavailable'
        return view
    try:
        plan = _load(queue, spec)
    except (OSError, ValueError, UnicodeError, TypeError, RecursionError, OverflowError):
        view['problem'] = 'plan_unavailable_or_invalid'
        return view
    if (plan['source_bulk_fingerprint'] != cnyes_bulk_fingerprint(source)
            or plan['authorization_fingerprint'] != source.authorization_fingerprint):
        view['problem'] = 'source_identity_changed'
        return view
    if (type(limit) is not int or not 1 <= limit <= 512 or type(max_seconds) not in (int, float)
            or not math.isfinite(max_seconds) or max_seconds <= 0):
        raise ValueError('invalid_priority_slice')
    from downloader.news_fulltext_cnyes_history import original_envelope_audit_key
    marker = _PREFIX + spec['sha256']
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
    try:
        state = json.loads(saved[0], object_pairs_hook=_unique) if saved else {
            'version': 1, 'next_index': 0, 'after_scope': '', 'after_page': 0,
            'heads_seen': 0, 'added': 0, 'existing': 0, 'stale': 0}
    except (ValueError, TypeError):
        state = None
    observations = plan['observations']
    cached_observations = getattr(queue, '_cnyes_original_priority_observation_cache', None)
    if cached_observations is None or cached_observations[0] != spec['sha256']:
        cached_observations = (spec['sha256'], frozenset(observations))
        queue._cnyes_original_priority_observation_cache = cached_observations
    target_set = cached_observations[1]
    recover_raw = None
    if not _valid_checkpoint(state, len(observations)):
        if saved and type(saved[0]) is str and _valid_checkpoint_fields(state, len(observations)):
            # This known v1 counter contradiction can be reconstructed from
            # the pinned plan and per-page outcomes. Unknown/malformed cursor
            # versions remain blocked and are never overwritten.
            recover_raw = saved[0]
            state = {'version': 1, 'next_index': 0, 'after_scope': '', 'after_page': 0,
                     'heads_seen': 0, 'added': 0, 'existing': 0, 'stale': 0}
        else:
            view['problem'] = 'invalid_checkpoint'
            return view
    audit_key = original_envelope_audit_key(source)
    watch_marker = _PREFIX + 'watch_v1:' + spec['sha256'] + ':' + audit_key
    active_value = spec['sha256'] + ':' + audit_key
    active_saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (_ACTIVE,)).fetchone()
    watch_saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (watch_marker,)).fetchone()
    if (saved and type(saved[0]) is not str or watch_saved and watch_saved[0] != '1'
            or active_saved and type(active_saved[0]) is not str):
        view['problem'] = 'invalid_checkpoint'
        return view
    rebuild_watch = bool(saved and recover_raw is None and
                         (not watch_saved or not active_saved or active_saved[0] != active_value))
    validation = (spec['sha256'], audit_key)
    if getattr(queue, '_cnyes_original_priority_watch_validation', None) != validation:
        watched = {row[0] for row in queue.db.execute(
            'SELECT observation_sha256 FROM cnyes_history_original_priority_watch '
            'WHERE plan_sha256=? AND audit_key=?', validation)}
        required = set(observations[:state['next_index'] + int(bool(state['after_scope']))])
        if not watched <= target_set:
            view['problem'] = 'invalid_checkpoint'
            return view
        if saved and not required <= watched:
            rebuild_watch = True
    if queue.db.execute('SELECT 1 FROM cnyes_history_original_priority_pending p '
        'LEFT JOIN cnyes_history_original_priority_watch w ON w.plan_sha256=p.plan_sha256 '
        'AND w.audit_key=p.audit_key AND w.observation_sha256=p.observation_sha256 '
        'WHERE p.plan_sha256=? AND p.audit_key=? AND w.observation_sha256 IS NULL LIMIT 1',
        validation).fetchone():
        view['problem'] = 'invalid_checkpoint'
        return view
    steps, started, added_tracking, added_retry, reactivated = 0, time.monotonic(), False, False, False
    with queue.db:
        if stop is not None and stop.is_set():
            if recover_raw is not None:
                view['problem'] = 'invalid_checkpoint'
                return view
            view.update(state='scanning', targets=len(observations), processed_targets=state['next_index'],
                        stale_targets=state['stale'], enqueued_pages=state['added'], existing_pages=state['existing'])
            return view
        if recover_raw is not None:
            archive = (_PREFIX + 'invalid:' + spec['sha256'] + ':'
                       + hashlib.sha256(recover_raw.encode()).hexdigest())
            prior = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (archive,)).fetchone()
            if prior and (type(prior[0]) is not str or prior[0] != recover_raw):
                view['problem'] = 'checkpoint_archive_conflict'
                return view
            queue.db.execute('INSERT OR IGNORE INTO queue_settings VALUES (?,?)', (archive, recover_raw))
        if rebuild_watch:
            archive = (_PREFIX + 'watch_upgrade:' + spec['sha256'] + ':'
                       + hashlib.sha256(saved[0].encode()).hexdigest())
            prior = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (archive,)).fetchone()
            if prior and (type(prior[0]) is not str or prior[0] != saved[0]):
                view['problem'] = 'checkpoint_archive_conflict'
                return view
            queue.db.execute('INSERT OR IGNORE INTO queue_settings VALUES (?,?)', (archive, saved[0]))
        if rebuild_watch or recover_raw is not None:
            # Count retained versions whose head is no longer current before
            # replaying every current target. Their audit results are evidence,
            # and must remain in the page-version denominator.
            historical = queue.db.execute('SELECT count(*) FROM cnyes_history_original_priority_pages p '
                'LEFT JOIN cnyes_history_page_heads h ON h.scope_url=p.scope_url AND h.page=p.page '
                'WHERE p.plan_sha256=? AND p.audit_key=? AND h.observation_sha256 IS NOT p.observation_sha256',
                (spec['sha256'], audit_key)).fetchone()[0]
            state = {'version': 1, 'next_index': 0, 'after_scope': '', 'after_page': 0,
                     'heads_seen': historical, 'added': 0, 'existing': historical, 'stale': 0}
        queue.db.execute('INSERT OR IGNORE INTO queue_settings VALUES (?,?)', (watch_marker, '1'))
        if not active_saved or active_saved[0] != active_value:
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (_ACTIVE, active_value))
        while state['next_index'] < len(observations) and steps < limit:
            if stop is not None and stop.is_set() or steps and time.monotonic() - started >= max_seconds:
                break
            observation = observations[state['next_index']]
            queue.db.execute('INSERT OR IGNORE INTO cnyes_history_original_priority_watch VALUES (?,?,?)',
                             (spec['sha256'], audit_key, observation))
            rows = queue.db.execute('SELECT scope_url,page FROM cnyes_history_page_heads '
                'WHERE observation_sha256=? AND (scope_url,page)>(?,?) ORDER BY scope_url,page LIMIT ?',
                (observation, state['after_scope'], state['after_page'], limit - steps)).fetchall()
            if not rows:
                state['stale'] += int(not state['after_scope'])
                state['next_index'] += 1
                state['after_scope'], state['after_page'] = '', 0
                steps += 1
                continue
            for row in rows:
                inserted, tracked, reopened = _admit_page(queue, spec['sha256'], audit_key,
                                                         row[0], row[1], observation)
                added_tracking = added_tracking or bool(tracked)
                added_retry = added_retry or bool(inserted)
                reactivated = reactivated or reopened
                state['heads_seen'] += 1
                state['added'] += inserted
                state['existing'] += 1 - inserted
                state['after_scope'], state['after_page'] = row[0], row[1]
                steps += 1
                if stop is not None and stop.is_set() or time.monotonic() - started >= max_seconds:
                    break
        if state['next_index'] == len(observations) and steps < limit:
            pending = queue.db.execute('SELECT p.scope_url,p.page,p.observation_sha256,h.observation_sha256 '
                'FROM cnyes_history_original_priority_pending p LEFT JOIN cnyes_history_page_heads h '
                'ON h.scope_url=p.scope_url AND h.page=p.page WHERE p.plan_sha256=? AND p.audit_key=? '
                'ORDER BY p.scope_url,p.page,p.observation_sha256 LIMIT ?',
                (spec['sha256'], audit_key, limit - steps)).fetchall()
            for scope, page, observation, head in pending:
                if stop is not None and stop.is_set() or steps and time.monotonic() - started >= max_seconds:
                    break
                if head == observation:
                    inserted, tracked, reopened = _admit_page(queue, spec['sha256'], audit_key,
                                                             scope, page, observation)
                    added_tracking = added_tracking or bool(tracked)
                    added_retry = added_retry or bool(inserted)
                    reactivated = reactivated or reopened
                    if tracked:
                        state['heads_seen'] += 1
                        state['added'] += inserted
                        state['existing'] += 1 - inserted
                queue.db.execute('DELETE FROM cnyes_history_original_priority_pending '
                    'WHERE plan_sha256=? AND audit_key=? AND scope_url=? AND page=? AND observation_sha256=?',
                    (spec['sha256'], audit_key, scope, page, observation))
                steps += 1
        pending_more = bool(queue.db.execute('SELECT 1 FROM cnyes_history_original_priority_pending '
            'WHERE plan_sha256=? AND audit_key=? LIMIT 1', (spec['sha256'], audit_key)).fetchone())
        if steps or recover_raw is not None or rebuild_watch:
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)',
                             (marker, json.dumps(state, sort_keys=True, separators=(',', ':'))))
    view.update(state='complete' if state['next_index'] == len(observations) and not pending_more else 'scanning',
                targets=len(observations), processed_targets=state['next_index'], stale_targets=state['stale'],
                enqueued_pages=state['added'], existing_pages=state['existing'], slice_steps=steps,
                checkpoint_recovered=recover_raw is not None)
    if added_tracking:
        queue._cnyes_original_priority_has_records = True
    if added_tracking or added_retry or reactivated:
        queue._cnyes_original_priority_wait = None
    queue._cnyes_original_priority_tracking = (spec['sha256'], audit_key)
    queue._cnyes_original_priority_observations = target_set
    queue._cnyes_original_priority_watch_validation = validation
    return view


def record_audit_result(queue, audit_key, scope, page, observation, result):
    """Record exact results for all plans in the auditor's existing transaction."""
    has_records = getattr(queue, '_cnyes_original_priority_has_records', None)
    if has_records is None:
        has_records = bool(queue.db.execute('SELECT 1 FROM cnyes_history_original_priority_pages LIMIT 1').fetchone())
        queue._cnyes_original_priority_has_records = has_records
    if not has_records:
        return
    if result not in ('verified', 'request_mismatch', 'authorization_mismatch',
                      'invalid', 'unavailable', 'skipped', 'outside', 'superseded'):
        raise ValueError('invalid_priority_audit_result')
    changed = queue.db.execute('UPDATE cnyes_history_original_priority_pages SET last_result=? '
        'WHERE audit_key=? AND scope_url=? AND page=? AND observation_sha256=? AND last_result!=?',
        (result, audit_key, scope, page, observation, result)).rowcount
    if changed:
        # Conservative invalidation also covers a transaction that later rolls
        # back; it may cause another lookup but cannot hide due work.
        queue._cnyes_original_priority_wait = None


def select_priority_retries(queue, audit_key, epoch, limit):
    """Select due, first-attempt priority hints without resetting retry clocks.

    Retry slices alternate with the ordinary due-order selector. The caller
    persists the next selector only in the same transaction as a processed
    audit, so restarts and failed transactions do not discard a turn.
    """
    active = getattr(queue, '_cnyes_original_priority_tracking', None)
    if active is None or active[1] != audit_key:
        return None, None
    marker = audit_key + ':priority_retry_next:' + active[0]
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
    if saved and saved[0] == 'general':
        return None, (marker, 'priority')
    waiting = getattr(queue, '_cnyes_original_priority_wait', None)
    if waiting is not None and waiting[0] == active and epoch < waiting[1]:
        return None, None
    rows = queue.db.execute('SELECT r.scope_url AS head_scope,r.page AS head_page,'
        'r.observation_sha256 AS retry_sha,h.observation_sha256 AS head_sha,o.* '
        'FROM cnyes_history_original_priority_pages p '
        'INDEXED BY cnyes_history_original_priority_awaiting '
        'CROSS JOIN cnyes_history_original_file_retries r '
        'ON r.audit_key=p.audit_key AND r.scope_url=p.scope_url AND r.page=p.page '
        'AND r.observation_sha256=p.observation_sha256 '
        'LEFT JOIN cnyes_history_page_heads h ON h.scope_url=r.scope_url AND h.page=r.page '
        'LEFT JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
        "WHERE p.plan_sha256=? AND p.audit_key=? AND p.last_result='awaiting' AND r.not_before_epoch<=? "
        'ORDER BY p.scope_url,p.page,p.observation_sha256 LIMIT ?',
        (*active, epoch, limit)).fetchall()
    if rows:
        queue._cnyes_original_priority_wait = None
        return rows, (marker, 'general')
    # Within the single canonical owner, the only additions to this set are
    # seed() and audit results, both of which invalidate this advisory cache.
    # Never persist it: a restart or a newly admitted page performs a fresh
    # lookup, and the original retry deadlines remain authoritative.
    next_due = queue.db.execute('SELECT min(r.not_before_epoch) '
        'FROM cnyes_history_original_priority_pages p '
        'INDEXED BY cnyes_history_original_priority_awaiting '
        'CROSS JOIN cnyes_history_original_file_retries r '
        'ON r.audit_key=p.audit_key AND r.scope_url=p.scope_url AND r.page=p.page '
        'AND r.observation_sha256=p.observation_sha256 '
        "WHERE p.plan_sha256=? AND p.audit_key=? AND p.last_result='awaiting'", active).fetchone()[0]
    if next_due is not None and next_due > epoch:
        queue._cnyes_original_priority_wait = (active, next_due)
    return None, None


def progress(queue):
    """Summarize current-plan page versions, not source or body completion."""
    active = getattr(queue, '_cnyes_original_priority_tracking', None)
    view = getattr(queue, '_cnyes_original_priority_view', None)
    if active is None or not isinstance(view, dict) or view.get('state') not in ('scanning', 'complete'):
        return None
    if getattr(queue, '_cnyes_priority_digest_db', None) is not queue.db:
        def event_digest(value):
            # SQLite TEXT can contain invalid UTF-8. Receive raw bytes so the
            # Python driver never has to decode a corrupt event before we can
            # classify it as changed.
            if type(value) is not bytes:
                return None
            return hashlib.sha256(value).hexdigest()
        queue.db.create_function('cnyes_priority_event_digest', 1, event_digest, deterministic=True)
        queue._cnyes_priority_digest_db = queue.db
    counts = {'awaiting_pages': 0, 'verified_pages': 0, 'mismatched_pages': 0,
              'unverified_pages': 0, 'outside_pages': 0, 'changed_pages': 0,
              'recovered_id_pages': 0}
    mapping = {'awaiting': 'awaiting_pages', 'verified': 'verified_pages',
               'request_mismatch': 'mismatched_pages', 'authorization_mismatch': 'mismatched_pages',
               'invalid': 'unverified_pages', 'unavailable': 'unverified_pages', 'skipped': 'unverified_pages',
               'outside': 'outside_pages', 'superseded': 'changed_pages',
               'recovered_id': 'recovered_id_pages'}
    # Recovery events and ID references store encoded(dict(page_evidence)).
    # Generate that same canonical key order in SQLite, but never pass a
    # damaged BLOB to json_object: an unreadable receipt means changed, not
    # a failed dashboard snapshot.
    def evidence_json(alias):
        return (f"CASE WHEN typeof({alias}.scope_url)='text' AND typeof({alias}.page)='integer' "
                f"AND typeof({alias}.total)='integer' AND typeof({alias}.ids_sha256)='text' "
                f"AND typeof({alias}.observed_at_utc)='text' THEN json_object("
                f"'ids_sha256',{alias}.ids_sha256,'observed_at_utc',{alias}.observed_at_utc,"
                f"'page',{alias}.page,'scope_url',{alias}.scope_url,'total',{alias}.total) END")
    target_evidence, source_evidence = evidence_json('pe'), evidence_json('spe')
    rows = queue.db.execute('SELECT CASE WHEN h.observation_sha256 IS NOT p.observation_sha256 '
        "THEN CASE WHEN r.event_key IS NOT NULL AND ev.value IS NOT NULL AND om.scope_url IS NULL "
        "AND sm.scope_url IS NULL AND obs.scope_url=r.source_scope_url AND obs.page=p.page "
        "AND typeof(ev.value)='text' AND ev.key='cnyes_history_equal_day_original_recovery_v1:' || "
        "cnyes_priority_event_digest(CAST(ev.value AS BLOB)) "
        "AND CASE WHEN json_valid(ev.value) THEN "
        "json_extract(ev.value,'$.contract')='cnyes_equal_day_original_mismatch_v1' "
        "AND json_extract(ev.value,'$.actual_source_observation_sha256')=h.observation_sha256 "
        f"AND (p.scope_url=r.source_scope_url OR (ref.scope_url IS NOT NULL AND "
        f"ref.original_ID_evidence_json=({target_evidence}) AND "
        "json_extract(ev.value,'$.original_target_page_ID_evidence')=ref.original_ID_evidence_json)) "
        "AND (json_extract(ev.value,'$.reconstructed_source_from_retained_original') IS NOT 1 OR "
        f"json_extract(ev.value,'$.original_source_page_ID_evidence')=({source_evidence})) "
        "AND ((json_extract(ev.value,'$.original_target_head.scope_url')=p.scope_url "
        "AND json_extract(ev.value,'$.original_target_head.page')=p.page "
        "AND json_extract(ev.value,'$.original_target_head.observation_sha256')=p.observation_sha256) "
        "OR (json_extract(ev.value,'$.original_source_head.scope_url')=p.scope_url "
        "AND json_extract(ev.value,'$.original_source_head.page')=p.page "
        "AND json_extract(ev.value,'$.original_source_head.observation_sha256')=p.observation_sha256)) "
        "ELSE 0 END THEN 'recovered_id' ELSE 'superseded' END ELSE p.last_result END AS outcome, count(*) "
        'FROM cnyes_history_original_priority_pages p LEFT JOIN cnyes_history_page_heads h '
        'ON h.scope_url=p.scope_url AND h.page=p.page '
        'LEFT JOIN cnyes_history_equal_day_recovery_heads r ON r.scope_url=p.scope_url AND r.page=p.page '
        'AND r.old_observation_sha256=p.observation_sha256 AND r.recovered_observation_sha256=h.observation_sha256 '
        'LEFT JOIN queue_settings ev ON ev.key=r.event_key '
        'LEFT JOIN cnyes_history_unproven_pages om ON om.scope_url=p.scope_url AND om.page=p.page '
        'LEFT JOIN cnyes_history_unproven_pages sm ON sm.scope_url=r.source_scope_url AND sm.page=p.page '
        'LEFT JOIN cnyes_history_page_observations obs ON obs.observation_sha256=h.observation_sha256 '
        'LEFT JOIN cnyes_history_page_ID_references ref ON ref.scope_url=p.scope_url AND ref.page=p.page '
        'AND ref.observation_sha256=h.observation_sha256 AND ref.reference_scope=r.source_scope_url '
        'LEFT JOIN bulk_page_evidence pe ON pe.scope_url=p.scope_url AND pe.page=p.page '
        'LEFT JOIN bulk_page_evidence spe ON spe.scope_url=r.source_scope_url AND spe.page=p.page '
        'WHERE p.plan_sha256=? AND p.audit_key=? '
        'GROUP BY outcome', active).fetchall()
    for outcome, count in rows:
        if outcome not in mapping:
            return None
        counts[mapping[outcome]] += count
    total = sum(counts.values())
    return {'contract_version': 2, 'tracked_pages': total,
            'tracking_complete': total == view['enqueued_pages'] + view['existing_pages'], **counts,
            'local_raw_only': True, 'publisher_requests': 0, 'all_history_complete': False}

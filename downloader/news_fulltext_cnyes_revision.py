"""Reconcile changing CNYES list pages through the existing discovery owner.

These are checks of observed metadata pages, not an atomic publisher snapshot,
body coverage or a promise that the publisher will never change a page again.
Every round and check is committed with the corresponding index-head update.
"""
from __future__ import annotations

import hashlib
import json
import re
import secrets

from downloader.news_fulltext_bulk import cnyes_scope
from downloader.news_fulltext_policy import NewsError


def initialize(queue):
    queue.db.executescript('''
        CREATE TABLE IF NOT EXISTS cnyes_index_revision_rounds(
            scope_url TEXT NOT NULL,index_fingerprint TEXT NOT NULL,authorization_fingerprint TEXT NOT NULL,
            round_id TEXT NOT NULL,recheck_id TEXT NOT NULL,phase TEXT NOT NULL,anchor_total INTEGER,anchor_pages INTEGER,
            anchor_ids_sha256 TEXT,anchor_response_sha256 TEXT,started_at_utc TEXT NOT NULL,
            PRIMARY KEY(scope_url,index_fingerprint,authorization_fingerprint)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_index_revision_checks(
            round_id TEXT NOT NULL,page INTEGER NOT NULL,response_sha256 TEXT NOT NULL,
            index_fingerprint TEXT NOT NULL,checked_at_utc TEXT NOT NULL,
            PRIMARY KEY(round_id,page)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_index_revision_retired(
            scope_url TEXT NOT NULL,page INTEGER NOT NULL,url TEXT NOT NULL,
            index_fingerprint TEXT NOT NULL,authorization_fingerprint TEXT NOT NULL,
            round_id TEXT NOT NULL,heads_json TEXT NOT NULL,retired_at_utc TEXT NOT NULL,
            active INTEGER NOT NULL CHECK(active IN (0,1)),
            PRIMARY KEY(scope_url,page,index_fingerprint,authorization_fingerprint,round_id)
        ) WITHOUT ROWID;
        CREATE UNIQUE INDEX IF NOT EXISTS cnyes_index_revision_active_retired
            ON cnyes_index_revision_retired(url,index_fingerprint,authorization_fingerprint) WHERE active=1;
    ''')


def _identity(source, scope):
    return scope, source.cnyes_index_data_fingerprint, source.authorization_fingerprint


def _digest(value):
    return isinstance(value, str) and re.fullmatch(r'[a-f0-9]{64}', value) is not None


def _round(queue, source, scope):
    return queue.db.execute('SELECT * FROM cnyes_index_revision_rounds WHERE scope_url=? '
                            'AND index_fingerprint=? AND authorization_fingerprint=?',
                            _identity(source, scope)).fetchone()


def prepare_dispatch(queue, task):
    """Bind a request to the round *and phase* that existed before dispatch."""
    from downloader.news_fulltext_cnyes_index import is_index_task
    if not is_index_task(task):
        return
    source = queue.registry.sources[task['provider']]
    state = _round(queue, source, cnyes_scope(task['url']))
    task['_cnyes_revision_round'] = state['round_id'] if state else None
    task['_cnyes_revision_phase'] = state['phase'] if state else None


def stale_dispatch(queue, source, task):
    """A response from before a round/phase transition is evidence, not a head."""
    from downloader.news_fulltext_cnyes_index import validate_request
    _, page, _, _ = validate_request(task['url'], source)
    state = _round(queue, source, cnyes_scope(task['url']))
    if not state:
        return task.get('_cnyes_revision_round') is not None
    if not _digest(state['round_id']) or not _digest(state['recheck_id']):
        return True
    if (task.get('_cnyes_revision_round') != state['round_id']
            or task.get('_cnyes_revision_phase') != state['phase']):
        return True
    if state['phase'] == 'anchor':
        return page != 1
    if state['phase'] == 'pages':
        return (type(state['anchor_pages']) is not int or not 1 <= state['anchor_pages'] <= 100_000
                or type(state['anchor_total']) is not int or state['anchor_total'] < 0
                or not _digest(state['anchor_ids_sha256']) or not _digest(state['anchor_response_sha256'])
                or page < 2 or page > state['anchor_pages'])
    if state['phase'] == 'verify':
        return (type(state['anchor_pages']) is not int or not 1 <= state['anchor_pages'] <= 100_000
                or type(state['anchor_total']) is not int or state['anchor_total'] < 0
                or not _digest(state['anchor_ids_sha256']) or not _digest(state['anchor_response_sha256']) or page != 1)
    return True


def _signature(head):
    return head['total'], head['pages'], head['ids_sha256']


def _archive_round(queue, state, reason, *, final_head=None, scope_idle=None):
    from downloader.news_fulltext_cnyes_index import clock
    value = {'contract_version': 1, 'round': dict(state), 'outcome': reason, 'recorded_at_utc': clock(),
             'final_first_response_sha256': final_head['response_sha256'] if final_head else None,
             'scope_idle_at_final_check': scope_idle,
             'confirmed_pages': queue.db.execute('SELECT count(*) FROM cnyes_index_revision_checks WHERE round_id=?',
                                                  (state['round_id'],)).fetchone()[0]}
    queue.db.execute('INSERT OR IGNORE INTO queue_settings VALUES (?,?)',
                     ('cnyes_index_revision_outcome_v1:' + state['round_id'],
                      json.dumps(value, sort_keys=True, separators=(',', ':'))))


def _schedule_pages(queue, source, state):
    from downloader.news_fulltext_cnyes_index import _schedule_index_total_recheck, compatible_head_guard
    guard, parameters = compatible_head_guard(source)
    rows = queue.db.execute('SELECT o.* FROM cnyes_public_index_heads h JOIN cnyes_public_index_observations o '
        'USING(scope_url,page,response_sha256,index_fingerprint,authorization_fingerprint) '
        'WHERE h.scope_url=? AND h.page>1 AND h.page<=? AND ' + guard
        + ' AND h.authorization_fingerprint=? ORDER BY h.page',
        (state['scope_url'], state['anchor_pages'], *parameters, source.authorization_fingerprint)).fetchall()
    for head in rows:
        _schedule_index_total_recheck(queue, source, head, state['recheck_id'])


def _schedule_first(queue, source, state):
    from downloader.news_fulltext_cnyes_index import (
        _index_head, _schedule_index_total_recheck, discovery_priority, request_url, validate_request,
    )
    first = _index_head(queue, source, state['scope_url'], 1)
    if first:
        _schedule_index_total_recheck(queue, source, first, state['recheck_id'])
    else:
        # Older evidence can start at a later page. Seed the canonical first
        # request but do not reset an existing job, failure or source circuit.
        from urllib.parse import parse_qs, urlsplit
        try:
            day = (int(parse_qs(urlsplit(state['scope_url']).query)['startAt'][0]) + 28800) // 86400
            url = request_url(source, day)
            validate_request(url, source)
            if cnyes_scope(url) != state['scope_url']:
                return
        except (KeyError, IndexError, TypeError, ValueError, NewsError):
            return
        queue.db.execute("INSERT OR IGNORE INTO discovery(url,kind,provider,priority) VALUES (?,'index',?,?)",
                         (url, source.provider, discovery_priority(day)))


def _anchor(queue, source, state, head):
    queue.db.execute('UPDATE cnyes_index_revision_rounds SET phase=?,anchor_total=?,anchor_pages=?,anchor_ids_sha256=?,'
                     'anchor_response_sha256=? '
                     'WHERE scope_url=? AND index_fingerprint=? AND authorization_fingerprint=? AND round_id=?',
                     ('pages' if head['pages'] > 1 else 'verify', head['total'], head['pages'], head['ids_sha256'],
                      head['response_sha256'],
                      *_identity(source, head['scope_url']), state['round_id']))
    state = _round(queue, source, head['scope_url'])
    if state['phase'] == 'pages':
        _schedule_pages(queue, source, state)
    else:
        _schedule_first(queue, source, state)


def start_round(queue, source, scope, *, first=None, reason):
    """Start/restart without resetting any source failure or retry deadline."""
    from downloader.news_fulltext_cnyes_index import clock
    previous = _round(queue, source, scope)
    if previous:
        _archive_round(queue, previous, 'restarted:' + reason)
    round_id = hashlib.sha256(secrets.token_bytes(32)).hexdigest()
    # Conflicting pages can repeatedly restart the causal round without new
    # information. Keep their retry budget until that conflict is resolved.
    recheck_id = previous['recheck_id'] if previous else round_id
    queue.db.execute('INSERT OR REPLACE INTO cnyes_index_revision_rounds VALUES (?,?,?,?,?,?,?,?,?,?,?)',
        (*_identity(source, scope), round_id, recheck_id, 'anchor', None, None, None, None, clock()))
    state = _round(queue, source, scope)
    if first is not None:
        _anchor(queue, source, state, first)
    else:
        _schedule_first(queue, source, state)
    queue.metrics.count(source.provider, 'public_index_revision_rounds_started')


def _reactivate(queue, source, scope, pages, round_id):
    """A later first page can advertise pages retired by an older round."""
    from downloader.news_fulltext_cnyes_index import _schedule_index_total_recheck
    rows = queue.db.execute('SELECT * FROM cnyes_index_revision_retired WHERE scope_url=? '
        'AND index_fingerprint=? AND authorization_fingerprint=? AND active=1 AND page<=?',
        (*_identity(source, scope), pages)).fetchall()
    for retired in rows:
        queue.db.execute('UPDATE cnyes_index_revision_retired SET active=0 WHERE scope_url=? AND page=? '
            'AND index_fingerprint=? AND authorization_fingerprint=? AND round_id=?',
            (scope, retired['page'], source.cnyes_index_data_fingerprint,
             source.authorization_fingerprint, retired['round_id']))
        try:
            recorded = json.loads(retired['heads_json'])
        except (ValueError, TypeError):
            continue  # Keep the source job and its existing deadline intact.
        for item in recorded:
            if type(item) is not dict:
                continue
            original = queue.db.execute('SELECT * FROM cnyes_public_index_observations WHERE scope_url=? AND page=? '
                'AND response_sha256=? AND index_fingerprint=? AND authorization_fingerprint=?',
                (scope, retired['page'], item.get('response_sha256'), item.get('index_fingerprint'),
                 source.authorization_fingerprint)).fetchone()
            if original and _schedule_index_total_recheck(queue, source, original, round_id):
                break


def _retire_beyond_anchor(queue, source, state):
    """Retire only mutable head pointers; immutable observations remain."""
    from downloader.news_fulltext_cnyes_index import clock, compatible_fingerprints, request_url
    from urllib.parse import parse_qs, urlsplit
    versions = compatible_fingerprints(source)
    rows = queue.db.execute('SELECT h.* FROM cnyes_public_index_heads h WHERE h.scope_url=? AND h.page>? '
        'AND h.index_fingerprint IN (' + ','.join('?' for _ in versions) + ') AND h.authorization_fingerprint=? '
        'ORDER BY h.page,h.index_fingerprint',
        (state['scope_url'], state['anchor_pages'], *versions, source.authorization_fingerprint)).fetchall()
    by_page = {}
    for row in rows:
        by_page.setdefault(row['page'], []).append(dict(row))
    day = (int(parse_qs(urlsplit(state['scope_url']).query)['startAt'][0]) + 28800) // 86400
    for page, heads in by_page.items():
        url = request_url(source, day, page)
        queue.db.execute('UPDATE cnyes_index_revision_retired SET active=0 WHERE url=? '
            'AND index_fingerprint=? AND authorization_fingerprint=? AND active=1',
            (url, source.cnyes_index_data_fingerprint, source.authorization_fingerprint))
        queue.db.execute('INSERT INTO cnyes_index_revision_retired VALUES (?,?,?,?,?,?,?,?,1)',
            (state['scope_url'], page, url, source.cnyes_index_data_fingerprint,
             source.authorization_fingerprint, state['round_id'],
             json.dumps(heads, sort_keys=True, separators=(',', ':')), clock()))
        queue.db.executemany('DELETE FROM cnyes_public_index_heads WHERE scope_url=? AND page=? '
            'AND response_sha256=? AND index_fingerprint=? AND authorization_fingerprint=?',
            ((row['scope_url'], row['page'], row['response_sha256'], row['index_fingerprint'],
              row['authorization_fingerprint']) for row in heads))
    return len(by_page)


def _duplicate_pages(queue, source, scope, pages):
    from downloader.news_fulltext_cnyes_index import compatible_head_guard
    guard, parameters = compatible_head_guard(source)
    rows = queue.db.execute('SELECT o.ids_sha256,group_concat(o.page) FROM cnyes_public_index_heads h '
        'JOIN cnyes_public_index_observations o USING(scope_url,page,response_sha256,index_fingerprint,authorization_fingerprint) '
        'WHERE h.scope_url=? AND h.page<=? AND ' + guard + ' AND h.authorization_fingerprint=? AND o.metadata_rows>0 '
        'GROUP BY o.ids_sha256 HAVING count(*)>1',
        (scope, pages, *parameters, source.authorization_fingerprint)).fetchall()
    return {int(page) for row in rows for page in row[1].split(',') if int(page) > 1}


def _changed_confirmations(queue, source, state):
    """Fence every completed page against the currently selected head."""
    from downloader.news_fulltext_cnyes_index import compatible_head_guard
    guard, parameters = compatible_head_guard(source)
    return queue.db.execute('SELECT c.* FROM cnyes_index_revision_checks c WHERE c.round_id=? '
        'AND c.page>1 AND c.page<=? AND NOT EXISTS (SELECT 1 FROM cnyes_public_index_heads h '
        'JOIN cnyes_public_index_observations o USING(scope_url,page,response_sha256,index_fingerprint,authorization_fingerprint) '
        'WHERE h.scope_url=? AND h.page=c.page AND h.response_sha256=c.response_sha256 '
        'AND h.index_fingerprint=c.index_fingerprint AND ' + guard
        + ' AND h.authorization_fingerprint=? AND o.total=? AND o.pages=?)',
        (state['round_id'], state['anchor_pages'], state['scope_url'], *parameters,
         source.authorization_fingerprint, state['anchor_total'], state['anchor_pages'])).fetchall()


def accepted(queue, source, task, previous, head):
    """Advance only a response dispatched after the relevant round/phase."""
    from downloader.news_fulltext_cnyes_index import clock, _schedule_index_total_recheck, _index_head
    scope, page = head['scope_url'], head['page']
    state = _round(queue, source, scope)
    if not state:
        if previous and _signature(previous) != _signature(head):
            start_round(queue, source, scope, first=head if page == 1 else None, reason='page_changed')
            state = _round(queue, source, scope)
            if page == 1:
                _reactivate(queue, source, scope, head['pages'], state['recheck_id'])
        return
    if state['phase'] == 'anchor':
        _anchor(queue, source, state, head)
        _reactivate(queue, source, scope, head['pages'], state['recheck_id'])
        return
    anchor = (state['anchor_total'], state['anchor_pages'], state['anchor_ids_sha256'])
    if page == 1 and _signature(head) != anchor:
        start_round(queue, source, scope, first=head, reason='anchor_changed')
        _reactivate(queue, source, scope, head['pages'], _round(queue, source, scope)['recheck_id'])
        return
    if state['phase'] == 'pages':
        if page == 1:
            return
        if (head['total'], head['pages']) != anchor[:2]:
            start_round(queue, source, scope, reason='page_total_conflict')
            return
        queue.db.execute('INSERT OR REPLACE INTO cnyes_index_revision_checks VALUES (?,?,?,?,?)',
                         (state['round_id'], page, head['response_sha256'], head['index_fingerprint'], clock()))
        count = queue.db.execute('SELECT count(*) FROM cnyes_index_revision_checks '
                                 'WHERE round_id=? AND page>1 AND page<=?',
                                 (state['round_id'], state['anchor_pages'])).fetchone()[0]
        if count == state['anchor_pages'] - 1:
            queue.db.execute('UPDATE cnyes_index_revision_rounds SET phase=? WHERE scope_url=? '
                'AND index_fingerprint=? AND authorization_fingerprint=? AND round_id=?',
                ('verify', *_identity(source, scope), state['round_id']))
            _schedule_first(queue, source, _round(queue, source, scope))
        return
    if state['phase'] != 'verify':
        return
    # Even an out-of-range request sent in an older phase may still return.
    # The owner supplies this proof after removing the current future.
    if task.get('_cnyes_revision_scope_idle') is not True:
        _schedule_first(queue, source, state)
        return
    count = queue.db.execute('SELECT count(*) FROM cnyes_index_revision_checks '
        'WHERE round_id=? AND page>1 AND page<=?', (state['round_id'], state['anchor_pages'])).fetchone()[0]
    changed = _changed_confirmations(queue, source, state)
    if count != state['anchor_pages'] - 1 or changed:
        start_round(queue, source, scope, first=head, reason='confirmed_head_changed')
        renewed = _round(queue, source, scope)
        for check in changed:
            original = queue.db.execute('SELECT * FROM cnyes_public_index_observations WHERE scope_url=? AND page=? '
                'AND response_sha256=? AND index_fingerprint=? AND authorization_fingerprint=?',
                (scope, check['page'], check['response_sha256'], check['index_fingerprint'],
                 source.authorization_fingerprint)).fetchone()
            if original:
                _schedule_index_total_recheck(queue, source, original, renewed['recheck_id'])
        return
    duplicate_pages = _duplicate_pages(queue, source, scope, state['anchor_pages'])
    if duplicate_pages:
        queue.db.execute('UPDATE cnyes_index_revision_rounds SET phase=? WHERE scope_url=? '
            'AND index_fingerprint=? AND authorization_fingerprint=? AND round_id=?',
            ('pages', *_identity(source, scope), state['round_id']))
        for duplicate in duplicate_pages:
            queue.db.execute('DELETE FROM cnyes_index_revision_checks WHERE round_id=? AND page=?',
                             (state['round_id'], duplicate))
            prior = _index_head(queue, source, scope, duplicate)
            if prior:
                _schedule_index_total_recheck(queue, source, prior, state['recheck_id'])
        return
    retired = _retire_beyond_anchor(queue, source, state)
    _archive_round(queue, state, 'verified_pages_and_first:retired=' + str(retired),
                   final_head=head, scope_idle=True)
    queue.db.execute('DELETE FROM cnyes_index_revision_rounds WHERE scope_url=? AND index_fingerprint=? '
                     'AND authorization_fingerprint=? AND round_id=?', (*_identity(source, scope), state['round_id']))
    queue.metrics.count(source.provider, 'public_index_revision_rounds_completed')


def active_scopes(db, source):
    """Old read-only databases have no round table until the owner migrates."""
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='cnyes_index_revision_rounds'").fetchone():
        return set()
    return {row[0] for row in db.execute('SELECT scope_url FROM cnyes_index_revision_rounds '
        'WHERE index_fingerprint=? AND authorization_fingerprint=?',
        (source.cnyes_index_data_fingerprint, source.authorization_fingerprint))}


def migrate_historical_revisions(queue, source):
    """One indexed-history pass for revisions predating this round contract."""
    from downloader.news_fulltext_cnyes_index import clock, compatible_fingerprints, _index_head
    identity = (source.cnyes_index_data_fingerprint, source.authorization_fingerprint)
    marker = 'cnyes_index_revision_migration_v1:' + hashlib.sha256(
        json.dumps(identity, separators=(',', ':')).encode()).hexdigest()
    if queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
        return
    versions = compatible_fingerprints(source)
    scopes = queue.db.execute('SELECT DISTINCT scope_url FROM (SELECT scope_url,page '
        'FROM cnyes_public_index_observations WHERE index_fingerprint IN ('
        + ','.join('?' for _ in versions) + ') AND authorization_fingerprint=? '
        'GROUP BY scope_url,page HAVING count(DISTINCT ids_sha256)>1 OR count(DISTINCT total)>1 '
        'OR count(DISTINCT pages)>1)', (*versions, source.authorization_fingerprint)).fetchall()
    started = missing_first = 0
    for row in scopes:
        scope = row[0]
        if _round(queue, source, scope):
            continue
        if not _index_head(queue, source, scope, 1):
            missing_first += 1
        start_round(queue, source, scope, reason='historical_revision_without_round_proof')
        started += 1
    proof = {'contract_version': 1, 'completed_at_utc': clock(), 'changed_scopes': len(scopes),
             'rounds_started': started, 'scopes_without_first_head': missing_first,
             'source_requests_made': 0, 'original_observations_modified': False}
    queue.db.execute('INSERT INTO queue_settings VALUES (?,?)',
                     (marker, json.dumps(proof, sort_keys=True, separators=(',', ':'))))

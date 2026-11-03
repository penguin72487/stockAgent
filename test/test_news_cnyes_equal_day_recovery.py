"""Recover list IDs without inventing an original request or capture clock."""
from datetime import datetime, timezone
from contextlib import closing
import hashlib
import json

import pytest

from downloader import news_fulltext_cnyes_history as history
from downloader import news_fulltext_cnyes_priority as priority
from downloader.news_fulltext_bulk import cnyes_scope, cnyes_url
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import BASE, cfg, response  # noqa: F401
from test_news_cnyes_history_evidence import bounds, capture, head, queue, source_payload  # noqa: F401


def seed_legacy_collision(queue, monkeypatch, *, overflow=False):
    start = 1388534400
    original = cnyes_url(BASE, start=start, end=start + 100)
    target = cnyes_url(BASE, start=start + 101, end=start + 300)
    data = source_payload(range(1, 31) if overflow else [1, 2], epoch=start,
                          total=1000 if overflow else None)
    source = queue.registry.sources['cnyes']
    artifact = history.retain_response(queue, response(data, original), source, accepted=[], restricted=False)
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: artifact)
        capture(queue, original, data)
        capture(queue, target, data)
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (target,))
    return original, target, artifact


def seed_alias_cycle(queue, monkeypatch, *, overflow=False):
    """Old content-addressed raw bytes with an alias but no direct source row."""
    original, target, artifact = seed_legacy_collision(queue, monkeypatch, overflow=overflow)
    source_scope, target_scope = cnyes_scope(original), cnyes_scope(target)
    source_head = dict(queue.db.execute('SELECT * FROM cnyes_history_page_heads WHERE scope_url=?',
                                        (source_scope,)).fetchone())
    target_head = dict(queue.db.execute('SELECT * FROM cnyes_history_page_heads WHERE scope_url=?',
                                        (target_scope,)).fetchone())
    source_evidence = dict(queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=?',
                                            (source_scope,)).fetchone())
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_page_ID_references VALUES (?,?,?,?,?)',
                         (source_scope, 1, target_head['observation_sha256'],
                          history.encoded(source_evidence), target_scope))
        queue.db.execute('UPDATE cnyes_history_page_heads SET observation_sha256=?,last_confirmed_at_utc=? '
                         'WHERE scope_url=? AND page=1',
                         (target_head['observation_sha256'], target_head['last_confirmed_at_utc'], source_scope))
        queue.db.execute('DELETE FROM cnyes_history_page_observations WHERE observation_sha256=?',
                         (source_head['observation_sha256'],))
    return original, target, artifact


def current_audit_row(queue, url):
    return queue.db.execute('SELECT h.scope_url AS head_scope,h.page AS head_page,h.observation_sha256 AS head_sha,o.* '
        'FROM cnyes_history_page_heads h JOIN cnyes_history_page_observations o '
        'ON o.observation_sha256=h.observation_sha256 WHERE h.scope_url=? AND h.page=1',
        (cnyes_scope(url),)).fetchone()


def test_auditor_recovers_equal_day_IDs_without_relabelling_original(queue, monkeypatch):
    original, target, artifact = seed_legacy_collision(queue, monkeypatch)
    source = queue.registry.sources['cnyes']
    old = current_audit_row(queue, target)
    old_head = dict(queue.db.execute('SELECT * FROM cnyes_history_page_heads WHERE scope_url=?',
                                     (cnyes_scope(target),)).fetchone())
    old_observation = old['observation_json']
    evidence = dict(queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=?',
                                     (cnyes_scope(target),)).fetchone())
    job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (target,)).fetchone())
    source_files = [queue.root / artifact[name] for name in ('parquet_path', 'receipt_path')]
    original_bytes = [hashlib.sha256(path.read_bytes()).hexdigest() for path in source_files]
    assert history._original_envelope_state(queue.root, artifact, target, old['response_sha256'],
                                            source.authorization_fingerprint) == 'request_mismatch'
    result = history.audit_original_envelopes(queue, limit=100, max_seconds=30)
    assert result['equal_day_page_ID_recoveries'] == 1
    assert result['mismatched'] == 1  # The original mismatch remains an incident.
    new_head = dict(queue.db.execute('SELECT * FROM cnyes_history_page_heads WHERE scope_url=?',
                                     (cnyes_scope(target),)).fetchone())
    source_head = current_audit_row(queue, original)
    assert new_head['observation_sha256'] == source_head['head_sha'] != old['head_sha']
    assert new_head['last_confirmed_at_utc'] == old_head['last_confirmed_at_utc']
    assert queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
                            'WHERE observation_sha256=?', (old['head_sha'],)).fetchone()[0] == old_observation
    assert dict(queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=?',
                                 (cnyes_scope(target),)).fetchone()) == evidence
    ref = queue.db.execute('SELECT * FROM cnyes_history_page_ID_references WHERE scope_url=?',
                           (cnyes_scope(target),)).fetchone()
    assert ref['reference_scope'] == cnyes_scope(original)
    assert json.loads(ref['original_ID_evidence_json']) == evidence
    event = json.loads(queue.db.execute("SELECT value FROM queue_settings "
        "WHERE key LIKE 'cnyes_history_equal_day_original_recovery_v1:%'").fetchone()[0])
    assert event['original_target_head'] == old_head
    assert event['actual_source_request_url'] == original
    assert event['historical_point_in_time'] is event['body_completeness_verified'] is False
    assert event['publisher_requests'] == 0
    assert head(queue, target)['state'] == 'list_complete_by_page_ID_equivalence'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (target,)).fetchone()) == job
    assert [hashlib.sha256(path.read_bytes()).hexdigest() for path in source_files] == original_bytes


@pytest.mark.parametrize('damage', ['target_IDs', 'target_marker', 'source_marker', 'missing_source_head',
                                    'target_clock', 'receipt', 'source_inventory_fence'])
def test_unproven_or_changed_evidence_falls_back_without_alias(queue, monkeypatch, damage):
    original, target, artifact = seed_legacy_collision(queue, monkeypatch)
    scope, actual_scope = cnyes_scope(target), cnyes_scope(original)
    old = current_audit_row(queue, target)
    with queue.db:
        if damage == 'target_IDs':
            queue.db.execute("UPDATE bulk_page_evidence SET ids_sha256=? WHERE scope_url=?", ('0' * 64, scope))
        elif damage == 'target_marker':
            queue.db.execute('INSERT INTO cnyes_history_unproven_pages VALUES (?,?,?)',
                             (scope, 1, json.dumps({'reason': 'unrelated_integrity_failure'})))
        elif damage == 'source_marker':
            queue.db.execute('INSERT INTO cnyes_history_unproven_pages VALUES (?,?,?)',
                             (actual_scope, 1, json.dumps({'reason': 'unrelated_integrity_failure'})))
        elif damage == 'missing_source_head':
            queue.db.execute('DELETE FROM cnyes_history_page_heads WHERE scope_url=?', (actual_scope,))
        elif damage == 'target_clock':
            queue.db.execute('UPDATE cnyes_history_page_heads SET last_confirmed_at_utc=? WHERE scope_url=?',
                             (datetime(2000, 1, 1, tzinfo=timezone.utc).isoformat(), scope))
        elif damage == 'source_inventory_fence':
            source = queue.registry.sources['cnyes']
            queue.db.execute('INSERT INTO cnyes_history_local_inventory_scopes VALUES (?,?,?,?,?)',
                (scope, history.cnyes_bulk_fingerprint(source), source.authorization_fingerprint, 'scanning',
                 datetime.now(timezone.utc).isoformat()))
    if damage == 'receipt':
        (queue.root / artifact['receipt_path']).write_text('{}')
    if damage == 'target_clock':
        old = current_audit_row(queue, target)
    if damage == 'receipt':
        result = history.audit_original_envelopes(queue, limit=100, max_seconds=30)
        assert result['unverified'] >= 1 and result['equal_day_page_ID_recoveries'] == 0
    else:
        with queue.db:
            assert not history._recover_equal_day_original_mismatch(
                queue, queue.registry.sources['cnyes'], old, history._checked_page_observation(old))
    assert current_audit_row(queue, target)['head_sha'] == old['head_sha']
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_page_ID_references WHERE scope_url=?', (scope,)).fetchone()
    assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE "
                                "'cnyes_history_equal_day_original_recovery_v1:%'").fetchone()


def test_recovery_is_atomic_when_reconciliation_fails(queue, monkeypatch):
    _, target, _ = seed_legacy_collision(queue, monkeypatch)
    source, row = queue.registry.sources['cnyes'], current_audit_row(queue, target)
    scope = cnyes_scope(target)
    with monkeypatch.context() as patch:
        patch.setattr(history, 'reconcile_scope', lambda *a, **kw: (_ for _ in ()).throw(RuntimeError('test crash')))
        with pytest.raises(RuntimeError, match='test crash'):
            with queue.db:
                history._recover_equal_day_original_mismatch(queue, source, row, history._checked_page_observation(row))
    assert current_audit_row(queue, target)['head_sha'] == row['head_sha']
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_page_ID_references WHERE scope_url=?', (scope,)).fetchone()
    assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE "
                                "'cnyes_history_equal_day_original_recovery_v1:%'").fetchone()


def test_matching_mismatch_marker_is_archived_before_recovery(queue, monkeypatch):
    _, target, _ = seed_legacy_collision(queue, monkeypatch)
    scope, old = cnyes_scope(target), current_audit_row(queue, target)
    marker = json.dumps({'reason': 'original_envelope_request_mismatch',
                         'actual_observation_sha256': old['head_sha'], 'original_request_url': target})
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_unproven_pages VALUES (?,?,?)', (scope, 1, marker))
    result = history.audit_original_envelopes(queue, limit=100, max_seconds=30)
    assert result['equal_day_page_ID_recoveries'] == 1
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_unproven_pages WHERE scope_url=?', (scope,)).fetchone()
    event = json.loads(queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE "
        "'cnyes_history_equal_day_original_recovery_v1:%'").fetchone()[0])
    assert event['original_target_marker']['evidence_json'] == marker


def test_overflow_page_is_reused_without_claiming_day_complete(queue, monkeypatch):
    original, target, _ = seed_legacy_collision(queue, monkeypatch, overflow=True)
    scope = cnyes_scope(target)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (target,)).fetchone())
    result = history.audit_original_envelopes(queue, limit=100, max_seconds=30)
    assert result['equal_day_page_ID_recoveries'] == 1
    assert current_audit_row(queue, target)['head_sha'] == current_audit_row(queue, original)['head_sha']
    gap = queue.db.execute('SELECT error_code FROM bulk_window_gaps WHERE scope_url=?', (scope,)).fetchone()
    assert gap and gap['error_code'] == 'cnyes_single_day_page_cap'
    state = queue.db.execute('SELECT state FROM cnyes_history_scope_heads WHERE scope_url=?', (scope,)).fetchone()
    assert not state or state['state'] not in history.COMPLETE_STATES
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (target,)).fetchone()) == before


def test_recovered_reference_and_source_cooldown_survive_owner_restart(queue, monkeypatch):
    original, target, _ = seed_legacy_collision(queue, monkeypatch)
    source, scope = queue.registry.sources['cnyes'], cnyes_scope(target)
    old = current_audit_row(queue, target)
    assert history.audit_original_envelopes(queue, limit=100, max_seconds=30)['equal_day_page_ID_recoveries'] == 1
    expected_head = dict(queue.db.execute('SELECT * FROM cnyes_history_page_heads WHERE scope_url=?', (scope,)).fetchone())
    expected_job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (target,)).fetchone())
    root, registry = queue.root, queue.registry
    queue.close()
    with closing(NewsQueue(root, registry)) as resumed:
        with resumed.db:
            history.reconcile_scope(resumed, scope, source)
        assert dict(resumed.db.execute('SELECT * FROM cnyes_history_page_heads WHERE scope_url=?',
                                       (scope,)).fetchone()) == expected_head
        assert head(resumed, target)['state'] == 'list_complete_by_page_ID_equivalence'
        assert resumed.db.execute('SELECT 1 FROM cnyes_history_page_observations WHERE observation_sha256=?',
                                  (old['head_sha'],)).fetchone()
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (target,)).fetchone()) == expected_job
        assert current_audit_row(resumed, target)['head_sha'] == current_audit_row(resumed, original)['head_sha']


def test_same_IDs_do_not_hide_different_original_post_values(queue, monkeypatch):
    _, target, _ = seed_legacy_collision(queue, monkeypatch)
    row = current_audit_row(queue, target)
    observation = json.loads(row['observation_json'])
    observation['identities'][0]['source_post_sha256'] = 'f' * 64
    changed_sha = history.digest(observation)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_page_observations SET observation_sha256=?,observation_json=? '
                         'WHERE observation_sha256=?', (changed_sha, history.encoded(observation), row['head_sha']))
        queue.db.execute('UPDATE cnyes_history_page_heads SET observation_sha256=? WHERE scope_url=? AND page=1',
                         (changed_sha, cnyes_scope(target)))
    changed = current_audit_row(queue, target)
    assert history._checked_page_observation(changed)
    with queue.db:
        assert not history._recover_equal_day_original_mismatch(
            queue, queue.registry.sources['cnyes'], changed, history._checked_page_observation(changed))
    assert current_audit_row(queue, target)['head_sha'] == changed_sha
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_page_ID_references WHERE scope_url=?',
                                (cnyes_scope(target),)).fetchone()
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_equal_day_recovery_heads').fetchone()
    assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE "
                                "'cnyes_history_equal_day_original_recovery_v1:%'").fetchone()


def test_same_day_equivalence_rejects_other_query_semantics():
    start = 1388534400
    first = cnyes_url(BASE, start=start, end=start + 100)
    second = cnyes_url(BASE, start=start + 101, end=start + 300)
    assert history._same_day_page_requests(first, second)
    assert not history._same_day_page_requests(first, second + '&unexpected=1')
    assert not history._same_day_page_requests(first, second + '&page=1')
    assert not history._same_day_page_requests(first, cnyes_url(BASE, 2, start=start + 101, end=start + 300))
    assert not history._same_day_page_requests(first, cnyes_url(BASE, start=start + 86400, end=start + 86500))


def test_equal_day_alias_cycle_reconstructs_original_without_backdating_heads(queue, monkeypatch):
    original, target, artifact = seed_alias_cycle(queue, monkeypatch)
    source, source_scope, target_scope = queue.registry.sources['cnyes'], cnyes_scope(original), cnyes_scope(target)
    old = current_audit_row(queue, target)
    before_heads = {scope: dict(queue.db.execute('SELECT * FROM cnyes_history_page_heads WHERE scope_url=?',
                                                (scope,)).fetchone()) for scope in (source_scope, target_scope)}
    before_jobs = [dict(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    raw_capture = history._read_original_envelope_identity(
        queue.root / artifact['parquet_path'], queue.root / artifact['receipt_path'], old['response_sha256'],
        expected_parquet_path=artifact['parquet_path'], expected_kind=artifact['kind'], include_original=True)[-1]
    assert history._matching_shared_reference(queue, source_scope, 1, old['head_sha'],
                                              history._checked_page_observation(old))
    result = history.audit_original_envelopes(queue, limit=100, max_seconds=30)
    assert result['equal_day_page_ID_recoveries'] == 1
    assert result['unverified'] == 0  # The other selected row was verified by this same repair.
    assert not list(queue.db.execute('SELECT * FROM cnyes_history_original_file_retries WHERE scope_url IN (?,?)',
                                     (source_scope, target_scope)))
    direct = current_audit_row(queue, original)
    observation = history._checked_page_observation(direct, source)
    assert observation['request_url'] == original
    assert observation['source_observed_at_utc'] == raw_capture['captured_at_utc']
    assert observation['origin'] == 'verified_retained_source_response'
    assert observation['artifact'] == artifact
    for scope, before in before_heads.items():
        after = dict(queue.db.execute('SELECT * FROM cnyes_history_page_heads WHERE scope_url=?',
                                      (scope,)).fetchone())
        assert after['observation_sha256'] == direct['head_sha'] != before['observation_sha256']
        assert after['last_confirmed_at_utc'] == before['last_confirmed_at_utc']
    assert queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
                            'WHERE observation_sha256=?', (old['head_sha'],)).fetchone()[0] == old['observation_json']
    event = json.loads(queue.db.execute("SELECT value FROM queue_settings "
        "WHERE key LIKE 'cnyes_history_equal_day_original_recovery_v1:%'").fetchone()[0])
    assert event['reconstructed_source_from_retained_original'] is True
    assert event['original_source_head'] == before_heads[source_scope]
    assert event['original_source_reference']['observation_sha256'] == old['head_sha']
    assert head(queue, target)['state'] == 'list_complete_by_page_ID_equivalence'
    assert [dict(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before_jobs


@pytest.mark.parametrize('damage', ['source_reference_missing', 'source_reference_changed', 'source_coverage',
    'source_evidence_clock', 'source_head_clock', 'source_inventory_fence', 'occupied_source_primary_key'])
def test_alias_cycle_with_unproven_source_is_not_reconstructed(queue, monkeypatch, damage):
    original, target, _ = seed_alias_cycle(queue, monkeypatch)
    source, source_scope = queue.registry.sources['cnyes'], cnyes_scope(original)
    row = current_audit_row(queue, target)
    with queue.db:
        if damage == 'source_reference_missing':
            queue.db.execute('DELETE FROM cnyes_history_page_ID_references WHERE scope_url=?', (source_scope,))
        elif damage == 'source_reference_changed':
            queue.db.execute('UPDATE cnyes_history_page_ID_references SET original_ID_evidence_json=? WHERE scope_url=?',
                             ('{}', source_scope))
        elif damage == 'source_coverage':
            queue.db.execute('UPDATE bulk_api_coverage SET total_articles=total_articles+1 WHERE base_url=?', (source_scope,))
        elif damage == 'source_evidence_clock':
            queue.db.execute('UPDATE bulk_page_evidence SET observed_at_utc=? WHERE scope_url=?',
                             (datetime(2000, 1, 1, tzinfo=timezone.utc).isoformat(), source_scope))
        elif damage == 'source_head_clock':
            queue.db.execute('UPDATE cnyes_history_page_heads SET last_confirmed_at_utc=? WHERE scope_url=?',
                             (datetime(2000, 1, 1, tzinfo=timezone.utc).isoformat(), source_scope))
        elif damage == 'source_inventory_fence':
            queue.db.execute('INSERT INTO cnyes_history_local_inventory_scopes VALUES (?,?,?,?,?)',
                (source_scope, history.cnyes_bulk_fingerprint(source), source.authorization_fingerprint, 'scanning',
                 datetime.now(timezone.utc).isoformat()))
        elif damage == 'occupied_source_primary_key':
            queue.db.execute('INSERT INTO cnyes_history_page_observations VALUES (?,?,?,?,?,?,?,?)',
                (source_scope, row['page'], row['response_sha256'], row['evidence_contract_version'],
                 row['source_bulk_fingerprint'], row['authorization_fingerprint'], 'f' * 64, row['observation_json']))
        assert not history._recover_equal_day_original_mismatch(queue, source, row, history._checked_page_observation(row))
    assert current_audit_row(queue, target)['head_sha'] == row['head_sha']
    assert current_audit_row(queue, original)['head_sha'] == row['head_sha']
    assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE "
                                "'cnyes_history_equal_day_original_recovery_v1:%'").fetchone()


def test_alias_cycle_reconstruction_rolls_back_both_heads(queue, monkeypatch):
    original, target, _ = seed_alias_cycle(queue, monkeypatch)
    source, row = queue.registry.sources['cnyes'], current_audit_row(queue, target)
    before = [dict(r) for r in queue.db.execute('SELECT * FROM cnyes_history_page_heads ORDER BY scope_url')]
    with monkeypatch.context() as patch:
        patch.setattr(history, 'reconcile_scope', lambda *a, **kw: (_ for _ in ()).throw(RuntimeError('test crash')))
        with pytest.raises(RuntimeError, match='test crash'):
            with queue.db:
                history._recover_equal_day_original_mismatch(queue, source, row, history._checked_page_observation(row))
    assert [dict(r) for r in queue.db.execute('SELECT * FROM cnyes_history_page_heads ORDER BY scope_url')] == before
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_page_observations WHERE scope_url=?',
                                (cnyes_scope(original),)).fetchone()
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_page_ID_references WHERE scope_url=?',
                                (cnyes_scope(target),)).fetchone()
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_equal_day_recovery_heads').fetchone()
    assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE "
                                "'cnyes_history_equal_day_original_recovery_v1:%'").fetchone()


@pytest.mark.parametrize('marked', ['source', 'target', 'both'])
def test_alias_cycle_prior_matching_markers_are_archived(queue, monkeypatch, marked):
    original, target, _ = seed_alias_cycle(queue, monkeypatch)
    row = current_audit_row(queue, target)
    source_scope, target_scope = cnyes_scope(original), cnyes_scope(target)
    marker = history.encoded({'reason': 'original_envelope_request_mismatch',
                              'actual_observation_sha256': row['head_sha'], 'original_request_url': target})
    scopes = ([source_scope] if marked == 'source' else [target_scope] if marked == 'target'
              else [source_scope, target_scope])
    with queue.db:
        queue.db.executemany('INSERT INTO cnyes_history_unproven_pages VALUES (?,?,?)',
                             ((scope, 1, marker) for scope in scopes))
    result = history.audit_original_envelopes(queue, limit=100, max_seconds=30)
    assert result['equal_day_page_ID_recoveries'] == 1
    assert not list(queue.db.execute('SELECT * FROM cnyes_history_unproven_pages WHERE scope_url IN (?,?)',
                                     (source_scope, target_scope)))
    event = json.loads(queue.db.execute("SELECT value FROM queue_settings "
        "WHERE key LIKE 'cnyes_history_equal_day_original_recovery_v1:%'").fetchone()[0])
    if source_scope in scopes:
        assert event['original_source_marker']['evidence_json'] == marker
    if target_scope in scopes:
        assert event['original_target_marker']['evidence_json'] == marker


def test_alias_cycle_unrelated_source_marker_is_preserved(queue, monkeypatch):
    original, target, _ = seed_alias_cycle(queue, monkeypatch)
    source_scope = cnyes_scope(original)
    row, source = current_audit_row(queue, target), queue.registry.sources['cnyes']
    marker = history.encoded({'reason': 'some_other_integrity_failure', 'actual_observation_sha256': row['head_sha']})
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_unproven_pages VALUES (?,?,?)', (source_scope, 1, marker))
        assert not history._recover_equal_day_original_mismatch(queue, source, row, history._checked_page_observation(row))
    assert queue.db.execute('SELECT evidence_json FROM cnyes_history_unproven_pages WHERE scope_url=?',
                            (source_scope,)).fetchone()[0] == marker
    assert current_audit_row(queue, original)['head_sha'] == row['head_sha']


@pytest.mark.parametrize('marker_scope', ['source', 'target'])
def test_alias_cycle_marker_with_conflicting_request_is_preserved(queue, monkeypatch, marker_scope):
    original, target, _ = seed_alias_cycle(queue, monkeypatch)
    row, source = current_audit_row(queue, target), queue.registry.sources['cnyes']
    scope = cnyes_scope(original if marker_scope == 'source' else target)
    marker = history.encoded({'reason': 'original_envelope_request_mismatch',
        'actual_observation_sha256': row['head_sha'], 'original_request_url': 'https://invalid.example/other'})
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_unproven_pages VALUES (?,?,?)', (scope, 1, marker))
        assert not history._recover_equal_day_original_mismatch(queue, source, row, history._checked_page_observation(row))
    assert queue.db.execute('SELECT evidence_json FROM cnyes_history_unproven_pages WHERE scope_url=?',
                            (scope,)).fetchone()[0] == marker
    assert current_audit_row(queue, original)['head_sha'] == row['head_sha']


def test_alias_cycle_priority_retry_and_restart_preserve_original_evidence(queue, monkeypatch, tmp_path):
    original, target, artifact = seed_alias_cycle(queue, monkeypatch)
    source, old = queue.registry.sources['cnyes'], current_audit_row(queue, target)
    plan = {'contract': 'cnyes_original_audit_priority_v1',
            'source_bulk_fingerprint': history.cnyes_bulk_fingerprint(source),
            'authorization_fingerprint': source.authorization_fingerprint,
            'prior_plan_sha256': 'b' * 64, 'observations': [old['head_sha']]}
    raw = priority.plan_bytes(plan)
    path = tmp_path / 'priority.json'
    path.write_bytes(raw)
    spec = {'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest()}
    for _ in range(4):
        if priority.seed(queue, spec, limit=8, max_seconds=30)['state'] == 'complete':
            break
    assert priority.progress(queue)['tracked_pages'] == 2
    before_job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (target,)).fetchone())
    result = history.audit_original_envelopes(queue, limit=8, max_seconds=30)
    assert result['equal_day_page_ID_recoveries'] == 1 and result['unverified'] == 0
    progress = priority.progress(queue)
    assert progress['contract_version'] == 2 and progress['recovered_id_pages'] == 2
    assert progress['changed_pages'] == progress['awaiting_pages'] == 0
    assert not list(queue.db.execute('SELECT * FROM cnyes_history_original_file_retries'))
    source_scope, target_scope = cnyes_scope(original), cnyes_scope(target)
    heads = [dict(row) for row in queue.db.execute('SELECT * FROM cnyes_history_page_heads ORDER BY scope_url')]
    root, registry = queue.root, queue.registry
    queue.close()
    with closing(NewsQueue(root, registry)) as resumed:
        assert priority.seed(resumed, spec, limit=8, max_seconds=30)['state'] == 'complete'
        assert priority.progress(resumed)['recovered_id_pages'] == 2
        with resumed.db:
            history.reconcile_scope(resumed, source_scope, source)
            history.reconcile_scope(resumed, target_scope, source)
        assert [dict(row) for row in resumed.db.execute('SELECT * FROM cnyes_history_page_heads ORDER BY scope_url')] == heads
        original_row = current_audit_row(resumed, original)
        direct = history._checked_page_observation(original_row, source)
        assert direct['request_url'] == original and direct['artifact'] == artifact
        assert history._original_envelope_state(resumed.root, artifact, original, direct['response_sha256'],
                                                source.authorization_fingerprint) == 'verified'
        assert head(resumed, target)['state'] == 'list_complete_by_page_ID_equivalence'
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (target,)).fetchone()) == before_job
        assert resumed.db.execute('SELECT 1 FROM cnyes_history_page_observations WHERE observation_sha256=?',
                                  (old['head_sha'],)).fetchone()


def test_direct_original_recovery_has_one_priority_page(queue, monkeypatch, tmp_path):
    _, target, _ = seed_legacy_collision(queue, monkeypatch)
    source, old = queue.registry.sources['cnyes'], current_audit_row(queue, target)
    plan = {'contract': 'cnyes_original_audit_priority_v1',
            'source_bulk_fingerprint': history.cnyes_bulk_fingerprint(source),
            'authorization_fingerprint': source.authorization_fingerprint,
            'prior_plan_sha256': 'b' * 64, 'observations': [old['head_sha']]}
    raw = priority.plan_bytes(plan)
    path = tmp_path / 'priority.json'
    path.write_bytes(raw)
    spec = {'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest()}
    for _ in range(4):
        if priority.seed(queue, spec, limit=8, max_seconds=30)['state'] == 'complete':
            break
    assert history.audit_original_envelopes(queue, limit=8, max_seconds=30)['equal_day_page_ID_recoveries'] == 1
    progress = priority.progress(queue)
    assert progress['tracked_pages'] == progress['recovered_id_pages'] == 1
    assert progress['changed_pages'] == 0
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_page_ID_references SET original_ID_evidence_json='{}' WHERE scope_url=?",
                         (cnyes_scope(target),))
    assert priority.progress(queue)['recovered_id_pages'] == 0
    assert priority.progress(queue)['changed_pages'] == 1


@pytest.mark.parametrize('damage,expected_recovered', [
    ('missing_event', 0), ('invalid_event', 0), ('invalid_event_utf8', 0), ('changed_target_head', 1),
    ('missing_target_reference', 1), ('corrupt_target_reference', 1), ('utf8_target_reference', 1),
    ('changed_target_evidence', 1), ('changed_source_evidence', 0),
    ('blob_target_evidence', 1), ('blob_source_evidence', 0),
    ('utf8_target_evidence', 1), ('utf8_source_evidence', 0),
    ('event_target_evidence', 0), ('event_source_evidence', 0), ('event_other_field', 0),
    ('target_unproven', 1), ('source_unproven', 0),
])
def test_recovered_priority_status_requires_current_evidence(queue, monkeypatch, tmp_path, damage, expected_recovered):
    original, target, _ = seed_alias_cycle(queue, monkeypatch)
    source, old = queue.registry.sources['cnyes'], current_audit_row(queue, target)
    plan = {'contract': 'cnyes_original_audit_priority_v1',
            'source_bulk_fingerprint': history.cnyes_bulk_fingerprint(source),
            'authorization_fingerprint': source.authorization_fingerprint,
            'prior_plan_sha256': 'b' * 64, 'observations': [old['head_sha']]}
    raw = priority.plan_bytes(plan)
    path = tmp_path / 'priority.json'
    path.write_bytes(raw)
    spec = {'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest()}
    for _ in range(4):
        if priority.seed(queue, spec, limit=8, max_seconds=30)['state'] == 'complete':
            break
    assert history.audit_original_envelopes(queue, limit=8, max_seconds=30)['equal_day_page_ID_recoveries'] == 1
    assert priority.progress(queue)['recovered_id_pages'] == 2
    source_scope, target_scope = cnyes_scope(original), cnyes_scope(target)
    with queue.db:
        if damage == 'missing_event':
            queue.db.execute("DELETE FROM queue_settings WHERE key LIKE 'cnyes_history_equal_day_original_recovery_v1:%'")
        elif damage == 'invalid_event':
            queue.db.execute("UPDATE queue_settings SET value='{' WHERE key LIKE 'cnyes_history_equal_day_original_recovery_v1:%'")
        elif damage == 'invalid_event_utf8':
            queue.db.execute("UPDATE queue_settings SET value=CAST(? AS TEXT) WHERE key LIKE "
                             "'cnyes_history_equal_day_original_recovery_v1:%'", (b'{\"bad\":\"\xff\"}',))
        elif damage == 'changed_target_head':
            queue.db.execute('UPDATE cnyes_history_page_heads SET observation_sha256=? WHERE scope_url=?',
                             ('0' * 64, target_scope))
        elif damage == 'missing_target_reference':
            queue.db.execute('DELETE FROM cnyes_history_page_ID_references WHERE scope_url=?', (target_scope,))
        elif damage == 'corrupt_target_reference':
            queue.db.execute("UPDATE cnyes_history_page_ID_references SET original_ID_evidence_json='{}' "
                             'WHERE scope_url=?', (target_scope,))
        elif damage == 'utf8_target_reference':
            queue.db.execute('UPDATE cnyes_history_page_ID_references SET original_ID_evidence_json=CAST(? AS TEXT) '
                             'WHERE scope_url=?', (b'\xff', target_scope))
        elif damage in ('changed_target_evidence', 'changed_source_evidence', 'blob_target_evidence', 'blob_source_evidence'):
            scope = target_scope if 'target' in damage else source_scope
            value = b'\x00\xff' if damage.startswith('blob_') else '0' * 64
            queue.db.execute("UPDATE bulk_page_evidence SET ids_sha256=? WHERE scope_url=?", (value, scope))
        elif damage in ('utf8_target_evidence', 'utf8_source_evidence'):
            scope = target_scope if 'target' in damage else source_scope
            queue.db.execute('UPDATE bulk_page_evidence SET ids_sha256=CAST(? AS TEXT) WHERE scope_url=?',
                             (b'\xff', scope))
        elif damage in ('event_target_evidence', 'event_source_evidence', 'event_other_field'):
            key, raw_event = queue.db.execute("SELECT key,value FROM queue_settings WHERE key LIKE "
                "'cnyes_history_equal_day_original_recovery_v1:%'").fetchone()
            event = json.loads(raw_event)
            if damage == 'event_other_field':
                event['publisher_requests'] = 1
            else:
                field = 'original_target_page_ID_evidence' if 'target' in damage else 'original_source_page_ID_evidence'
                event[field]['ids_sha256'] = '0' * 64
            queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (history.encoded(event), key))
        elif damage in ('target_unproven', 'source_unproven'):
            scope = target_scope if damage == 'target_unproven' else source_scope
            queue.db.execute('INSERT INTO cnyes_history_unproven_pages VALUES (?,?,?)', (scope, 1, '{}'))
    progress = priority.progress(queue)
    assert progress['recovered_id_pages'] == expected_recovered
    assert progress['changed_pages'] == 2 - expected_recovered

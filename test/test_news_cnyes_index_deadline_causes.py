"""A request-local metadata delay cannot become a global transport delay."""
import json
import sqlite3
import time

import pytest

from downloader.news_fulltext import fail_discovery
from downloader.news_fulltext_cnyes_index import initialize, request_failure_scope_key
from downloader.news_fulltext_policy import NewsError
from test_news_cnyes_public_index import DAY, cfg, queue, register


def failed(queue, task, code, delay):
    fail_discovery(queue, task, NewsError(code, retry=True, delay=delay))
    return dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())


def circuit(queue):
    return dict(queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns').fetchone())


def journal(queue):
    return [json.loads(r[0]) for r in queue.db.execute("SELECT value FROM queue_settings "
        "WHERE key LIKE 'cnyes_index_deadline_reconciliation_v1:%'")]


def legacy(queue):
    metadata = register(queue)
    transport = register(queue, day=DAY+1)
    bad = failed(queue, metadata, 'cnyes_index_unverified_metadata', 3600)
    donor = circuit(queue)
    short = failed(queue, transport, 'transport_error', 60)
    # Reconstruct the previous canonical max() bug from actual persisted jobs,
    # not another request or an altered source error/deadline.
    with queue.db:
        queue.db.execute('UPDATE bulk_endpoint_cooldowns SET not_before=?', (donor['not_before'],))
        queue.db.execute("DELETE FROM queue_settings WHERE key LIKE 'cnyes_index_deadline_reconciliation_v1:%'")
    return metadata, transport, bad, short, donor


def test_transport_uses_own_global_deadline_not_metadata_hour(queue):
    metadata, transport = register(queue), register(queue, day=DAY+1)
    bad = failed(queue, metadata, 'cnyes_index_unverified_metadata', 3600)
    short = failed(queue, transport, 'transport_error', 60)
    assert short['due'] <= circuit(queue)['not_before'] <= short['due']+1
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (metadata['url'],)).fetchone()) == bad
    assert len(journal(queue)) == 1
    assert journal(queue)[0]['original_circuit']['not_before'] > short['due']+3000


def test_metadata_failure_during_real_global_cooldown_keeps_global_context(queue):
    transport, metadata = register(queue, day=DAY+1), register(queue)
    failed(queue, transport, 'transport_error', 600)
    global_original = circuit(queue)
    failed(queue, metadata, 'cnyes_index_unverified_metadata', 3600)
    assert circuit(queue) == global_original
    assert queue.discovery_candidates(queue.registry.sources['cnyes'], 10) == []


def test_exact_legacy_donation_reconciles_once_without_job_changes(queue):
    metadata, transport, bad, short, _ = legacy(queue)
    original = circuit(queue)
    jobs = [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    initialize(queue)
    assert short['due'] <= circuit(queue)['not_before'] <= short['due']+1
    assert [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == jobs
    assert len(journal(queue)) == 1 and journal(queue)[0]['original_circuit'] == original
    settings = [tuple(r) for r in queue.db.execute('SELECT * FROM queue_settings ORDER BY key')]
    initialize(queue)
    assert [tuple(r) for r in queue.db.execute('SELECT * FROM queue_settings ORDER BY key')] == settings
    assert not queue.db.execute('SELECT 1 FROM articles').fetchone()


@pytest.mark.parametrize('code', ['source_access_denied','http_403','http_429','http_503',
    'robots_unavailable','robots_disallowed','worker_ValueError','unknown_source_failure'])
def test_legacy_refusal_unknown_and_server_floors_never_reconciled(queue, code):
    legacy(queue)
    with queue.db:
        queue.db.execute('UPDATE bulk_endpoint_cooldowns SET error_code=?', (code,))
        queue.db.execute('UPDATE discovery SET error_code=? WHERE error_code=?', (code,'transport_error'))
    original = circuit(queue)
    initialize(queue)
    assert circuit(queue) == original and not journal(queue)


@pytest.mark.parametrize('change', ['missing','invalid_json','boolean_version','grant','fingerprint',
    'different_deadline','different_donor','recovered_donor','naive_clock','wrong_transport_error'])
def test_missing_corrupt_changed_or_recovered_donor_fails_closed(queue, change):
    metadata, transport, _, _, _ = legacy(queue)
    source = queue.registry.sources['cnyes']
    key = request_failure_scope_key(source)
    value = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0])
    with queue.db:
        if change == 'missing':
            queue.db.execute('DELETE FROM queue_settings WHERE key=?', (key,))
        elif change == 'invalid_json':
            queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', ('{',key))
        elif change == 'recovered_donor':
            queue.db.execute('UPDATE discovery SET error_code=NULL WHERE url=?', (metadata['url'],))
        elif change == 'naive_clock':
            value['recorded_at_utc'] = '2026-10-14T00:00:00'
        elif change == 'wrong_transport_error':
            queue.db.execute('UPDATE discovery SET error_code=? WHERE url=?', ('http_429',transport['url']))
        elif change == 'boolean_version': value['contract_version'] = True
        elif change == 'grant': value['permission_granted'] = True
        elif change == 'fingerprint': value['data_fingerprint'] = 'other'
        elif change == 'different_deadline': value['not_before'] += 10
        elif change == 'different_donor': value['request_url'] = transport['url']
        if change not in {'missing','invalid_json','recovered_donor','wrong_transport_error'}:
            queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps(value),key))
    original = circuit(queue)
    initialize(queue)
    assert circuit(queue) == original and not journal(queue)


def test_other_genuine_global_job_deadline_is_included(queue):
    _, _, _, short, _ = legacy(queue)
    third = register(queue, day=DAY+2)
    real = time.time()+1200
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=?,attempts=1,error_code=?,last_checked_at_utc=? WHERE url=?',
            (real,'http_429',short['last_checked_at_utc'],third['url']))
    initialize(queue)
    assert real <= circuit(queue)['not_before'] <= real+1
    assert queue.discovery_candidates(queue.registry.sources['cnyes'], 20) == []


def test_multiple_proved_metadata_hours_never_donate_to_transport(queue):
    first, second, transport = register(queue), register(queue, day=DAY+1), register(queue, day=DAY+2)
    one = failed(queue, first, 'cnyes_index_unverified_metadata', 3600)
    two = failed(queue, second, 'cnyes_index_unverified_metadata', 3600)
    short = failed(queue, transport, 'transport_error', 60)
    assert short['due'] <= circuit(queue)['not_before'] <= short['due']+1
    for task, original in ((first,one),(second,two)):
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()) == original


def test_scope_cannot_hide_an_inherited_global_floor_as_metadata(queue):
    task = register(queue)
    failed(queue, task, 'cnyes_index_unverified_metadata', 3600)
    with queue.db:
        queue.db.execute('UPDATE bulk_endpoint_cooldowns SET not_before=not_before+1000')
        queue.db.execute('DELETE FROM queue_settings WHERE key LIKE ?', ('cnyes_index_request_failure_scope_v1:%',))
    initialize(queue)
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?',
        (request_failure_scope_key(queue.registry.sources['cnyes']),)).fetchone()


def test_loaded_deadline_provenance_contract_is_explicit(queue):
    assert queue.status()['cnyes_index_failure_deadline_contract_version'] == 1


@pytest.mark.parametrize('bad', ['{', '{"contract_version":true}'])
def test_another_unproved_metadata_deadline_cannot_be_excluded(queue, bad):
    first, second, transport = register(queue), register(queue, day=DAY+1), register(queue, day=DAY+2)
    one = failed(queue, first, 'cnyes_index_unverified_metadata', 3600)
    key = request_failure_scope_key(queue.registry.sources['cnyes'], first['url'])
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (bad,key))
    failed(queue, second, 'cnyes_index_unverified_metadata', 3600)
    failed(queue, transport, 'transport_error', 60)
    assert circuit(queue)['not_before'] >= one['due']


def test_too_many_remaining_causes_fail_closed_instead_of_partial_maximum(queue):
    legacy(queue)
    original = circuit(queue)
    with queue.db:
        for offset in range(2,132):
            row = register(queue, day=DAY+offset)
            queue.db.execute('UPDATE discovery SET due=?,error_code=? WHERE url=?',
                             (time.time()+120,'unknown_source_failure',row['url']))
    initialize(queue)
    assert circuit(queue) == original and not journal(queue)


@pytest.mark.parametrize('change', ['unknown_request','nonfinite_deadline'])
def test_unknown_scope_or_invalid_remaining_global_deadline_fail_closed(queue, change):
    _, _, _, short, _ = legacy(queue)
    original = circuit(queue)
    row = register(queue, day=DAY+2)
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=?,attempts=1,error_code=?,last_checked_at_utc=? WHERE url=?',
            (float('inf') if change == 'nonfinite_deadline' else time.time()+600,
             'http_429',short['last_checked_at_utc'],row['url']))
        if change == 'unknown_request':
            queue.db.execute('UPDATE discovery SET url=url||? WHERE url=?', ('&unreviewed=1',row['url']))
    initialize(queue)
    assert circuit(queue) == original and not journal(queue)


def test_journal_failure_rolls_back_circuit_and_derived_settings(queue):
    legacy(queue)
    original = circuit(queue)
    settings = [tuple(r) for r in queue.db.execute('SELECT * FROM queue_settings ORDER BY key')]
    queue.db.execute("CREATE TRIGGER reject_journal BEFORE INSERT ON queue_settings "
        "WHEN NEW.key LIKE 'cnyes_index_deadline_reconciliation_v1:%' "
        "BEGIN SELECT RAISE(ABORT,'journal not durable'); END")
    with pytest.raises(sqlite3.IntegrityError, match='journal not durable'):
        initialize(queue)
    assert circuit(queue) == original and not journal(queue)
    assert [tuple(r) for r in queue.db.execute('SELECT * FROM queue_settings ORDER BY key')] == settings


def test_future_donor_recording_clock_is_not_causal_provenance(queue):
    _, _, _, short, _ = legacy(queue)
    key = request_failure_scope_key(queue.registry.sources['cnyes'])
    value = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0])
    value['recorded_at_utc'] = '9999-01-01T00:00:00+00:00'
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps(value),key))
    original = circuit(queue)
    initialize(queue)
    assert circuit(queue) == original and not journal(queue)


def test_other_index_origin_cannot_donate_to_this_API_endpoint(queue):
    _, _, _, short, _ = legacy(queue)
    with queue.db:
        queue.db.execute('INSERT INTO discovery(url,kind,provider,due,attempts,error_code) VALUES (?,?,?,?,?,?)',
            ('https://news.cnyes.com/news/cat/tw_stock','index','cnyes',time.time()+2400,1,'robots_unavailable'))
    initialize(queue)
    assert short['due'] <= circuit(queue)['not_before'] <= short['due']+1

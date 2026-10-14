"""A failed metadata page is not evidence that its entire endpoint is broken."""
import json
import time

import pytest

from downloader.news_fulltext import fail_discovery
from downloader.news_fulltext_cnyes_index import initialize
from downloader.news_fulltext_policy import NewsError
from test_news_cnyes_public_index import DAY, accept, cfg, index_payload, queue, register


def scope_key(source):
    return 'cnyes_index_request_failure_scope_v1:' + source.cnyes_index_data_fingerprint


def selection(queue):
    return [row['url'] for row in queue.discovery_candidates(queue.registry.sources['cnyes'], 20)]


def fail_page(queue, error='cnyes_index_unverified_metadata'):
    task = register(queue, page=1)
    same_day = register(queue, page=2)
    other_day = register(queue, day=DAY+1)
    fail_discovery(queue, task, NewsError(error, retry=True, delay=3600))
    return task, same_day, other_day


def test_one_unverified_metadata_page_does_not_block_strictly_validated_neighbors(queue):
    task, same_day, other_day = fail_page(queue)
    assert set(selection(queue)) == {same_day['url'], other_day['url']}
    row = queue.db.execute('SELECT due,attempts,error_code FROM discovery WHERE url=?', (task['url'],)).fetchone()
    assert row['due'] > time.time() + 3500 and row['attempts'] == 1
    assert row['error_code'] == 'cnyes_index_unverified_metadata'
    assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
    assert queue.db.execute('SELECT count(*) FROM cnyes_public_index_heads').fetchone()[0] == 0


def test_loaded_failure_scope_protocol_is_explicit_without_changing_metadata_capture(queue):
    assert queue.status()['cnyes_index_failure_scope_contract_version'] == 1
    source = queue.registry.sources['cnyes']
    assert source.cnyes_index_data_fingerprint


def test_original_circuit_error_deadline_and_failed_page_stay_guarded(queue):
    task, same_day, other_day = fail_page(queue)
    original = dict(queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns').fetchone())
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (task['url'],))
    assert set(selection(queue)) == {same_day['url'], other_day['url']}
    assert dict(queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns').fetchone()) == original
    assert queue.next_wake_delay({}, maximum=60) == 0


def test_successful_neighbor_cannot_retire_other_pages_original_failure(queue):
    task, _, _ = fail_page(queue)
    original = dict(queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns').fetchone())
    failed = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
    _, _, count = accept(queue, index_payload(ids=range(31, 61), page=2, total=61), page=2)
    assert count == 0
    assert dict(queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns').fetchone()) == original
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()) == failed
    assert queue.db.execute('SELECT count(*) FROM cnyes_public_index_heads').fetchone()[0] == 1
    assert queue.db.execute('SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL').fetchone()[0] == 0


def test_failure_retires_only_when_its_own_page_has_real_valid_recovery(queue, monkeypatch):
    task, _, _ = fail_page(queue)
    original = dict(queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns').fetchone())
    monkeypatch.setattr('time.time', lambda: original['not_before'] + 1)
    _, _, count = accept(queue, index_payload())
    assert count == 0
    assert queue.db.execute('SELECT count(*) FROM bulk_endpoint_cooldowns').fetchone()[0] == 0
    recovered = queue.db.execute('SELECT error_code,attempts FROM discovery WHERE url=?', (task['url'],)).fetchone()
    assert tuple(recovered) == (None, 0)


@pytest.mark.parametrize('error', [
    'source_access_denied', 'rate_limited', 'http_429', 'http_503', 'transport_error',
    'robots_unavailable', 'cnyes_index_unreviewed_metadata_fields',
    'cnyes_index_pagination_no_progress', 'bulk_pagination_totals_invalid',
    'cnyes_index_original_receipt_unverified',
])
def test_refusal_schema_pagination_and_unknown_failures_keep_endpoint_guard(queue, error):
    task, _, _ = fail_page(queue, error)
    assert selection(queue) == []
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?',
                               (scope_key(queue.registry.sources['cnyes']),)).fetchone()
    assert queue.db.execute('SELECT error_code FROM discovery WHERE url=?', (task['url'],)).fetchone()[0] == error


@pytest.mark.parametrize('change', ['deadline', 'url', 'error', 'fingerprint'])
def test_new_circuit_invalidates_old_exact_scope_annotation(queue, change):
    _, _, other_day = fail_page(queue)
    with queue.db:
        if change == 'deadline':
            queue.db.execute('UPDATE bulk_endpoint_cooldowns SET not_before=not_before+100')
        elif change == 'url':
            queue.db.execute('UPDATE bulk_endpoint_cooldowns SET request_url=?', (other_day['url'],))
        elif change == 'error':
            queue.db.execute("UPDATE bulk_endpoint_cooldowns SET error_code='source_access_denied'")
        else:
            row = queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
                (scope_key(queue.registry.sources['cnyes']),)).fetchone()
            assert row is not None
            value = json.loads(row[0])
            value['data_fingerprint'] = 'wrong'
            queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?',
                (json.dumps(value), scope_key(queue.registry.sources['cnyes'])))
    assert selection(queue) == []


@pytest.mark.parametrize('value', ['broken-json', '{}', '{"contract_version":true}',
                                 '{"contract_version":1,"permission_granted":true}'])
def test_missing_or_malformed_scope_proof_fails_closed_without_sql_crash(queue, value):
    fail_page(queue)
    with queue.db:
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)',
            (scope_key(queue.registry.sources['cnyes']), value))
    assert selection(queue) == []


def test_validated_legacy_annotation_does_not_rewrite_any_original_job_or_circuit(queue):
    _, same_day, other_day = fail_page(queue)
    key = scope_key(queue.registry.sources['cnyes'])
    with queue.db:
        queue.db.execute('DELETE FROM queue_settings WHERE key=?', (key,))
    before_jobs = [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    before_circuits = [tuple(r) for r in queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns')]
    assert selection(queue) == []  # Read-only pre-migration uses the old guard.
    initialize(queue)
    assert set(selection(queue)) == {same_day['url'], other_day['url']}
    assert [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before_jobs
    assert [tuple(r) for r in queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns')] == before_circuits
    recorded = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0]
    initialize(queue)
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0] == recorded


@pytest.mark.parametrize('field,value', [('contract_version', True), ('permission_granted', 0),
                                       ('preserves_source_validation', 1)])
def test_corrupt_boolean_integer_annotation_repairs_from_original_failure_only(queue, field, value):
    task, same_day, other_day = fail_page(queue)
    key = scope_key(queue.registry.sources['cnyes'])
    original_job = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
    original_circuit = tuple(queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns').fetchone())
    with queue.db:
        original = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0])
        original[field] = value
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps(original), key))
    assert selection(queue) == []
    initialize(queue)
    assert set(selection(queue)) == {same_day['url'], other_day['url']}
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()) == original_job
    assert tuple(queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns').fetchone()) == original_circuit


@pytest.mark.parametrize('invalid', ['request', 'missing_job', 'different_job_error', 'clock'])
def test_legacy_scope_requires_canonical_request_and_matching_actual_failure(queue, invalid):
    task, _, _ = fail_page(queue)
    key = scope_key(queue.registry.sources['cnyes'])
    with queue.db:
        queue.db.execute('DELETE FROM queue_settings WHERE key=?', (key,))
        if invalid == 'request':
            queue.db.execute("UPDATE bulk_endpoint_cooldowns SET request_url=request_url||'&secret=invalid'")
        elif invalid == 'missing_job':
            queue.db.execute('DELETE FROM discovery WHERE url=?', (task['url'],))
        elif invalid == 'different_job_error':
            queue.db.execute("UPDATE discovery SET error_code='http_429' WHERE url=?", (task['url'],))
        else:
            queue.db.execute("UPDATE discovery SET last_checked_at_utc='invalid' WHERE url=?", (task['url'],))
    initialize(queue)
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (key,)).fetchone()
    assert selection(queue) == []


def test_scope_annotation_and_source_failure_are_one_atomic_transaction(queue):
    task = register(queue)
    before = tuple(queue.db.execute('SELECT * FROM discovery').fetchone())
    key = scope_key(queue.registry.sources['cnyes'])
    queue.db.execute("CREATE TRIGGER reject_scope_annotation BEFORE INSERT ON queue_settings "
        "WHEN NEW.key LIKE 'cnyes_index_request_failure_scope_v1:%' "
        "BEGIN SELECT RAISE(ABORT,'scope-test-fault'); END")
    with pytest.raises(Exception, match='scope-test-fault'):
        fail_discovery(queue, task, NewsError('cnyes_index_unverified_metadata', retry=True, delay=3600))
    assert tuple(queue.db.execute('SELECT * FROM discovery').fetchone()) == before
    assert queue.db.execute('SELECT count(*) FROM bulk_endpoint_cooldowns').fetchone()[0] == 0
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (key,)).fetchone()

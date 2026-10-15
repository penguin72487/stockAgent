"""Typed field reasons survive spent raw quotas without new acceptance or keys."""
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json

import pytest

from downloader import news_fulltext_diagnostics as diagnostics
from downloader.news_fulltext import fail_discovery
from downloader.news_fulltext_cnyes_index import initialize, request_failure_scope_key
from test_news_cnyes_index_failure_diagnostics import failed_metadata, receipts, queue, cfg


def spent(queue,error):
    utc=datetime.fromisoformat(error.failure_response.received_at_utc).astimezone(timezone.utc)
    with queue.db:
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)',
            ('failure_diagnostic_attempts:v1:cnyes:'+utc.strftime('%Y%m%d'),'2'))
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)',
            ('failure_diagnostic_hourly_attempts:v1:cnyes:'+utc.strftime('%Y%m%dT%H')+':provider_general','1'))


def witness(queue,task):
    source=queue.registry.sources['cnyes']
    return json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
        (request_failure_scope_key(source,task['url']),)).fetchone()[0])


@pytest.mark.parametrize('changes',[
    {'categoryId':0}, {'categoryId':False}, {'payment':2}, {'payment':True},
    {'title':'','payment':None}, {'categoryId':0,'payment':None,'title':None},
])
def test_field_metadata_survives_spent_original_raw_budget(queue,changes):
    task,received,error=failed_metadata(queue,changes)
    spent(queue,error)
    before=dict(queue.db.execute("SELECT key,value FROM queue_settings WHERE key LIKE 'failure_diagnostic_%'"))
    fail_discovery(queue,task,error)
    annotation=witness(queue,task)
    diagnostic=annotation['metadata_failure_diagnostic']
    assert diagnostic['contract_version']==2
    assert diagnostic['failure_details']==error.cnyes_index_failure_details
    assert diagnostic['response_sha256']==hashlib.sha256(received.body).hexdigest()
    assert diagnostic['raw_response_retained'] is False and diagnostic['accepted_body_count']==0
    assert diagnostic['permission_granted'] is False and diagnostic['publish'] is False
    assert diagnostic['remote_training_export'] is False and len(json.dumps(diagnostic).encode())<=4096
    assert receipts(queue)==[] and annotation['scope']=='request' and annotation['permission_granted'] is False
    for key,value in before.items():
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?',(key,)).fetchone()[0]==value
    assert queue.db.execute('SELECT attempts,error_code FROM discovery WHERE url=?',(task['url'],)).fetchone()[:] == (1,error.code)
    assert not queue.db.execute('SELECT 1 FROM articles').fetchone()


def test_startup_preserves_exact_existing_field_observation_and_first_scope_clock(queue):
    task,received,error=failed_metadata(queue)
    spent(queue,error)
    fail_discovery(queue,task,error)
    original=[tuple(r) for r in queue.db.execute("SELECT key,value FROM queue_settings WHERE key LIKE 'cnyes_index_request_failure_scope_v1:%' ORDER BY key")]
    jobs=[tuple(r) for r in queue.db.execute('SELECT * FROM discovery')]
    initialize(queue)
    initialize(queue)
    assert [tuple(r) for r in queue.db.execute("SELECT key,value FROM queue_settings WHERE key LIKE 'cnyes_index_request_failure_scope_v1:%' ORDER BY key")]==original
    assert [tuple(r) for r in queue.db.execute('SELECT * FROM discovery')]==jobs


@pytest.mark.parametrize('change',['fingerprint','request','details','status','naive'])
def test_unproved_context_cannot_invent_field_metadata_in_a_valid_scope(queue,change):
    task,received,error=failed_metadata(queue)
    spent(queue,error)
    if change=='fingerprint':error.failure_response=replace(error.failure_response,source_fingerprint='bad')
    elif change=='request':error.failure_response=replace(error.failure_response,requested_url=task['url']+'&secret=1')
    elif change=='details':error.cnyes_index_failure_details={'violations':['bad']}
    elif change=='status':error.failure_response=replace(error.failure_response,response=replace(received,status=403))
    elif change=='naive':error.failure_response=replace(error.failure_response,received_at_utc='2026-10-14T01:01:00')
    fail_discovery(queue,task,error)
    assert 'metadata_failure_diagnostic' not in witness(queue,task)
    assert receipts(queue)==[]


def test_unreviewed_later_body_fields_are_not_copied_to_metadata_witness(queue):
    task,received,error=failed_metadata(queue,extra_post_field=True)
    spent(queue,error)
    fail_discovery(queue,task,error)
    encoded=json.dumps(witness(queue,task))
    assert 'metadata_failure_diagnostic' in encoded and 'Unreviewed body' not in encoded
    assert receipts(queue)==[]


def test_metadata_cause_does_not_change_endpoint_guard_source_delay_or_other_pages(queue):
    task,received,error=failed_metadata(queue)
    spent(queue,error)
    fail_discovery(queue,task,error)
    source=queue.registry.sources['cnyes']
    annotation=witness(queue,task)
    row=queue.db.execute('SELECT due FROM discovery WHERE url=?',(task['url'],)).fetchone()
    assert 0<=annotation['not_before']-row['due']<=1
    assert annotation['preserves_source_validation'] is True
    assert queue.db.execute('SELECT error_code FROM bulk_endpoint_cooldowns').fetchone()[0]==error.code
    assert set(dict(queue.db.execute("SELECT key,value FROM queue_settings WHERE key LIKE 'cnyes_index_request_failure_scope_v1:%'"))) == {
        request_failure_scope_key(source),request_failure_scope_key(source,task['url'])}


def test_large_broken_source_scalar_keeps_reason_and_exact_value_hash_in_fixed_bound(queue):
    huge=-int('1'*3800)
    task,received,error=failed_metadata(queue,{'categoryId':huge})
    spent(queue,error)
    fail_discovery(queue,task,error)
    diagnostic=witness(queue,task)['metadata_failure_diagnostic']
    assert len(json.dumps(diagnostic).encode())<=4096
    assert diagnostic['failure_details_complete'] is False
    fields=diagnostic['failure_details']['checked_fields']['categoryId']
    assert 'value' not in fields and fields['value_omitted_for_bound'] is True
    assert fields['value_sha256']==hashlib.sha256(json.dumps(huge).encode()).hexdigest()
    assert diagnostic['failure_details']['violations']==['categoryId_nonpositive']
    assert receipts(queue)==[]


def test_original_raw_sink_failure_cannot_erase_committed_field_witness(queue,monkeypatch):
    task,received,error=failed_metadata(queue)
    monkeypatch.setattr(diagnostics,'atomic_write_parquet',lambda *a,**k: (_ for _ in ()).throw(OSError('fixture disk error')))
    fail_discovery(queue,task,error)
    diagnostic=witness(queue,task)['metadata_failure_diagnostic']
    assert diagnostic['failure_details']==error.cnyes_index_failure_details and receipts(queue)==[]
    assert queue.db.execute('SELECT attempts,error_code FROM discovery WHERE url=?',(task['url'],)).fetchone()[:] == (1,error.code)

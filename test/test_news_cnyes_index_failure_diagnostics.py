"""Diagnose normal failed metadata responses without fetching or accepting text."""
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
import hashlib
import json
import time

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_diagnostics as diagnostics
from downloader.news_fulltext import fail_discovery, fetch_discovery
from downloader.news_fulltext_cnyes_index import parse_index
from downloader.news_fulltext_policy import NewsError
from test_news_cnyes_public_index import DAY, cfg, index_payload, queue, register
from test_cnyes_news_bulk import response


def failed_metadata(queue, changes=None, *, extra_post_field=False):
    task = register(queue)
    source = queue.registry.sources['cnyes']
    data = index_payload()
    data['items']['data'][0].update(changes or {'categoryId':0})
    if extra_post_field:
        data['items']['data'][1]['content'] = 'Unreviewed body must not be retained'
    received = response(data,task['url'])
    with pytest.raises(NewsError,match='cnyes_index_unverified_metadata') as caught:
        parse_index(received,source)
    return task,received,caught.value


def receipts(queue):
    return sorted(queue.root.glob('diagnostics/*/cnyes/index/*.receipt.json'))


@pytest.mark.parametrize('field,value,reason',[
    ('publishAt',DAY*86400-28800-1,'publishAt_outside_request'),
    ('publishAt',DAY*86400-28800+86400,'publishAt_outside_request'),
    ('categoryId',0,'categoryId_nonpositive'),
    ('categoryId',-1,'categoryId_nonpositive'),
    ('categoryId',True,'categoryId_type'),
    ('categoryId',1.0,'categoryId_type'),
    ('payment',True,'payment_type'),
    ('payment',0.0,'payment_type'),
    ('payment',2,'payment_outside_free_paid_flags'),
    ('payment',None,'payment_type'),
    ('title',[],'title_type'),
    ('title',None,'title_type'),
    ('title',12,'title_type'),
])
def test_exact_invalid_field_is_explained_without_weakening_validation(queue,field,value,reason):
    task,received,error = failed_metadata(queue,{field:value})
    detail = error.cnyes_index_failure_details
    assert detail['violations'] == [reason]
    assert detail['item_number'] == 0 and detail['news_id'] == 1
    assert detail['checked_fields'][field]['json_type'] == type(value).__name__
    assert error.failure_response.response is received
    assert error.failure_response.requested_url == task['url']
    assert error.failure_response.source_fingerprint == queue.registry.sources['cnyes'].cnyes_index_data_fingerprint
    assert datetime.fromisoformat(error.failure_response.received_at_utc).utcoffset() is not None
    assert error.code == 'cnyes_index_unverified_metadata' and error.retry and error.delay == 3600
    assert not queue.db.execute('SELECT 1 FROM articles').fetchone()


def test_owner_persists_exact_private_metadata_and_original_failure_before_diagnosis(queue):
    task,received,error = failed_metadata(queue,{'categoryId':0,'payment':True,'title':''})
    fail_discovery(queue,task,error)
    row = queue.db.execute('SELECT * FROM discovery WHERE url=?',(task['url'],)).fetchone()
    assert row['attempts'] == 1 and row['error_code'] == error.code and row['due'] > time.time()+3500
    path = receipts(queue)[0]
    receipt = json.loads(path.read_bytes())
    stored = pq.read_table(queue.root/receipt['path']).to_pylist()[0]
    assert receipt['contract_version'] == 2 and receipt['metadata_only'] is True
    assert receipt['accepted_as_article'] is False and receipt['accepted_body_count'] == 0
    assert receipt['publish'] is False and receipt['remote_training_export'] is False
    assert receipt['response_sha256'] == hashlib.sha256(received.body).hexdigest()
    assert stored['raw_response_body'] == received.body and receipt['raw_response_retained'] is True
    assert stored['request_url'] == task['url']
    assert json.loads(stored['failure_details_json'])['violations'] == [
        'categoryId_nonpositive','payment_type']
    assert path.stat().st_mode & 0o077 == 0
    assert not queue.db.execute('SELECT 1 FROM cnyes_public_index_heads').fetchone()
    assert not queue.db.execute('SELECT 1 FROM articles').fetchone()


def test_unknown_body_field_anywhere_in_page_keeps_only_field_diagnostics(queue):
    task,received,error = failed_metadata(queue,extra_post_field=True)
    fail_discovery(queue,task,error)
    receipt = json.loads(receipts(queue)[0].read_bytes())
    stored = pq.read_table(queue.root/receipt['path']).to_pylist()[0]
    assert receipt['raw_response_retained'] is False and stored['raw_response_body'] is None
    assert 'Unreviewed body' not in receipts(queue)[0].read_text()
    assert receipt['response_sha256'] == hashlib.sha256(received.body).hexdigest()


def test_index_uses_existing_general_daily_hourly_budget_no_scope_reset(queue):
    task,received,error = failed_metadata(queue)
    for _ in range(4):
        fail_discovery(queue,task,error)
    assert len(receipts(queue)) == 3  # Same two daily + one current-hour general reservations.
    day = datetime.fromisoformat(error.failure_response.received_at_utc).strftime('%Y%m%d')
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
        ('failure_diagnostic_attempts:v1:cnyes:'+day,)).fetchone()[0] == '2'
    assert {json.loads(p.read_bytes())['budget_bucket'] for p in receipts(queue)} == {'provider_general'}
    assert queue.db.execute('SELECT attempts FROM discovery WHERE url=?',(task['url'],)).fetchone()[0] == 1


@pytest.mark.parametrize('change',['wrong_fingerprint','wrong_url','wrong_details','wrong_error','status','too_large','naive'])
def test_unproved_context_never_reserves_budget_or_changes_original_outcome(queue,change):
    task,received,error = failed_metadata(queue)
    if change == 'wrong_fingerprint':
        error.failure_response = replace(error.failure_response,source_fingerprint='bad')
    elif change == 'wrong_url':
        error.failure_response = replace(error.failure_response,requested_url=task['url']+'&secret=1')
    elif change == 'wrong_details':
        error.cnyes_index_failure_details = {'violations':['categoryId_type']}
    elif change == 'wrong_error':
        error.code = 'source_access_denied'
    elif change in {'status','too_large'}:
        altered = replace(received,status=403) if change == 'status' else replace(received,body=b'x'*(8*1024*1024+1))
        error.failure_response = replace(error.failure_response,response=altered)
    elif change == 'naive':
        error.failure_response = replace(error.failure_response,received_at_utc='2026-10-14T01:02:03')
    fail_discovery(queue,task,error)
    assert receipts(queue) == []
    assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'failure_diagnostic_attempts:%'").fetchone()
    assert queue.db.execute('SELECT error_code FROM discovery WHERE url=?',(task['url'],)).fetchone()[0] == error.code


def test_diagnostic_storage_error_cannot_undo_failed_job_or_block_healthy_work(queue,monkeypatch):
    task,received,error = failed_metadata(queue)
    monkeypatch.setattr(diagnostics,'atomic_write_parquet',lambda *a,**k: (_ for _ in ()).throw(OSError('fixture disk error')))
    fail_discovery(queue,task,error)
    assert queue.db.execute('SELECT attempts,error_code FROM discovery WHERE url=?',(task['url'],)).fetchone()[:] == (1,error.code)
    assert receipts(queue) == []
    assert queue.metrics.snapshot()['providers']['cnyes']['counts']['public_index_failure_diagnostic_write_errors'] == 1
    assert queue.discovery_candidates(queue.registry.sources['cnyes'],20) == []


def test_normal_valid_metadata_has_no_diagnostic_context_hash_budget_or_files(queue):
    task = register(queue)
    parsed = parse_index(response(index_payload(),task['url']),queue.registry.sources['cnyes'])
    assert len(parsed[2]) == 2
    assert receipts(queue) == []
    assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'failure_diagnostic_attempts:%'").fetchone()


def test_loaded_private_index_failure_diagnostic_protocol_is_explicit(queue):
    assert queue.status()['cnyes_index_failure_diagnostic_contract_version'] == 2


def test_worker_transfers_only_the_already_received_response_to_queue_owner(queue):
    task,received,original = failed_metadata(queue)
    requests = []
    def get(url,**kwargs):
        requests.append(url)
        return received
    http = SimpleNamespace(registry=queue.registry,metrics=queue.metrics,get=get)
    with pytest.raises(NewsError,match=original.code) as caught:
        fetch_discovery(http,task)
    fail_discovery(queue,task,caught.value)
    assert requests == [task['url']] and len(receipts(queue)) == 1
    assert not queue.db.execute('SELECT 1 FROM articles').fetchone()


def test_non_utc_observation_uses_the_existing_utc_budget_and_path(queue):
    task,received,error = failed_metadata(queue)
    error.failure_response = replace(error.failure_response,received_at_utc='2026-10-14T00:01:00+08:00')
    fail_discovery(queue,task,error)
    path = receipts(queue)[0]
    receipt = json.loads(path.read_bytes())
    assert receipt['budget_period'] == '20261013'
    assert '/20261013/' in receipt['path']
    assert receipt['captured_at_utc'] == '2026-10-14T00:01:00+08:00'


def test_spent_existing_hourly_budget_never_reopens_for_an_index_scope(queue):
    task,received,error = failed_metadata(queue)
    observed = datetime.fromisoformat(error.failure_response.received_at_utc).astimezone(timezone.utc)
    day,hour = observed.strftime('%Y%m%d'),observed.strftime('%Y%m%dT%H')
    with queue.db:
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)',
            ('failure_diagnostic_attempts:v1:cnyes:'+day,'2'))
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)',
            ('failure_diagnostic_hourly_attempts:v1:cnyes:'+hour+':provider_general','1'))
    fail_discovery(queue,task,error)
    assert receipts(queue) == []
    assert queue.metrics.snapshot()['providers']['cnyes']['counts']['failure_diagnostic_budget_exhausted'] == 1


def test_a_different_registered_page_cannot_reuse_the_failed_pages_response(queue):
    task,received,error = failed_metadata(queue)
    other = register(queue,page=2)
    fail_discovery(queue,other,error)
    assert receipts(queue) == []
    assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'failure_diagnostic_attempts:%'").fetchone()

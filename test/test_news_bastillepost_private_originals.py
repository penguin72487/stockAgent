"""Synthetic short native originals; no publisher prose or network requests."""
from dataclasses import replace
import hashlib
import json
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as jobs
from downloader.news_fulltext_candidate_originals import (
    BASTILLE_SHORT_QUESTION, PendingOriginal, private_bastille_short_original_permit,
    BASTILLE_CONTINUATION_QUESTION, private_bastille_continuation_original_permit,
)
from downloader.news_fulltext_http import RedirectHop, Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from test_news_bastillepost_native import (
    settings, boundary_settings, boundary_failure, boundary_http, boundary_submit, page, URL, versioned,
)

LAYOUT = 'bastillepost_visible_article_v2'
TEXT = '本金 -12.50、股數 0、原始比率 0%（ms.）'


def short_page():
    return page(body='<p class="font-size-scaler-control">'+TEXT+'</p>')


@pytest.fixture
def queue(boundary_settings, tmp_path):
    value = NewsQueue(tmp_path, PolicyRegistry(boundary_settings, base=tmp_path))
    boundary_failure(value)
    yield value
    value.close()


def submit(queue, config):
    return jobs.submit_candidate(queue.root, config, queue.registry, URL, LAYOUT,
        reason='One source-bound private short original, not article acceptance', evidence_contract_version=9)


def claim(queue, config, http):
    request = submit(queue, config)
    with patch.object(jobs.time, 'monotonic', return_value=0.0):
        jobs.pump_candidates(queue)
    job = jobs.claim_candidate(queue, http, config, 'bastillepost')
    assert job['request_id'] == request['request_id']
    return request, job


def head(queue):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
def test_native_verified_private_witness_does_not_relax_short_body_or_replay_parser(
        boundary_settings, queue, engine, monkeypatch):
    from downloader import news_fulltext_candidates as candidates
    source = replace(queue.registry.sources['bastillepost'], html_parser=engine)
    raw = short_page().encode()
    assert private_bastille_short_original_permit(
        Response(URL, 200, {'content-type': 'text/html'}, raw), source, URL)['question'] == BASTILLE_SHORT_QUESTION
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(raw, source, page_url=URL)
    monkeypatch.setattr(candidates, 'extract_article', lambda *a, **k: pytest.fail('not its own candidate oracle'))
    before = head(queue); http = boundary_http(queue, short_page())
    request, job = claim(queue, boundary_settings, http)
    assert jobs.candidate_status(queue.root, request['request_id'])['raw_retention_state'] == 'unknown'
    pending = jobs.fetch_candidate(http, job)
    assert isinstance(pending, PendingOriginal) and pending.permit['question'] == BASTILLE_SHORT_QUESTION
    jobs.finish_candidate(queue, job, result=pending)
    with patch.object(jobs.time, 'monotonic', return_value=0.0):
        jobs.pump_candidates(queue)
    result = jobs.candidate_status(queue.root, request['request_id'])
    assert result['state'] == 'observed' and result['raw_retention_state'] == 'retained_verified'
    assert result['raw_or_names_or_body_retained'] is True and not result['accepted_as_article']
    facts = result['result_json']; ref = facts['private_original']
    restored = pq.ParquetFile(queue.root/ref['path']).read().to_pylist()
    assert len(restored) == 1 and restored[0]['raw_response_body'] == raw
    assert restored[0]['provider'] == 'bastillepost' and restored[0]['request_url'] == URL
    assert json.loads(restored[0]['transport_redirects_json']) == []
    assert ref['sha256'] == hashlib.sha256((queue.root/ref['path']).read_bytes()).hexdigest()
    receipt = json.loads((queue.root/ref['receipt_path']).read_bytes())
    assert receipt['permit'] == pending.permit and receipt['original'] == ref
    assert not facts['current_parser_replayed'] and not facts['identity_or_alias_accepted']
    assert TEXT not in json.dumps(facts, ensure_ascii=False)
    for path in (queue.root/ref['path'], queue.root/ref['receipt_path']):
        assert not path.stat().st_mode & 0o077
    assert head(queue) == before and http.calls == [URL]
    assert not submit(queue, boundary_settings)['submitted']


@pytest.mark.parametrize('edit', [
    lambda s: s.replace('/article/10123210/', '/article/9999/'),
    lambda s: s.replace('"巴士的報"', '"其他署名"'),
    lambda s: s.replace('2022年02月08日 11:56', '2022年02月09日 11:56'),
    lambda s: s.replace(TEXT, '（星島日報報道）'+TEXT),
    lambda s: s.replace(TEXT, '轉載自其他網站 '+TEXT),
    lambda s: s.replace('class="article-body"', 'hidden class="article-body"'),
    lambda s: s.replace(TEXT, TEXT+'<script src="//media.example.test/x"></script>'),
    lambda s: s.replace(TEXT, TEXT+'<iframe src="https://media.example.test/x"></iframe>'),
    lambda s: s.replace(TEXT, TEXT*10),
    lambda s: s.replace(TEXT, TEXT+'</p><p>另段</p><p>'),
    lambda s: '<title>Just a moment</title>'+s,
    lambda s: s.replace('<html>', '<html><div data-paywall="true">'),
])
def test_unverified_native_scope_or_non_short_shape_is_never_retained(boundary_settings, queue, edit):
    raw = edit(short_page()).encode(); source = queue.registry.sources['bastillepost']
    with pytest.raises(NewsError):
        private_bastille_short_original_permit(Response(URL, 200, {'content-type': 'text/html'}, raw), source, URL)


@pytest.mark.parametrize('mutation', [
    {'status': 403}, {'headers': {'content-type': 'application/json'}}, {'body': b'x'*(2*1024*1024+1)},
])
def test_response_policy_and_size_bound(boundary_settings, queue, mutation):
    response = replace(Response(URL, 200, {'content-type': 'text/html'}, short_page().encode()), **mutation)
    with pytest.raises(NewsError):
        private_bastille_short_original_permit(response, queue.registry.sources['bastillepost'], URL)


def test_actual_same_id_transport_is_retained_not_guessed(queue):
    requested = URL.replace('https:', 'http:')
    hop = RedirectHop(requested, 301, URL, URL, '2026-10-21T00:00:00Z', 'a'*64, 0)
    response = Response(URL, 200, {'content-type': 'text/html'}, short_page().encode(), (hop,))
    permit = private_bastille_short_original_permit(response, queue.registry.sources['bastillepost'], requested)
    assert permit['request_url'] == requested and permit['url'] == URL
    with pytest.raises(NewsError):
        private_bastille_short_original_permit(replace(response, redirects=()), queue.registry.sources['bastillepost'], requested)


def test_new_original_question_is_not_a_repeated_boundary_question(boundary_settings, queue):
    old = boundary_submit(queue, boundary_settings)
    with patch.object(jobs.time, 'monotonic', return_value=0.0):
        jobs.pump_candidates(queue)
    with pytest.raises(ValueError, match='prior_observation_not_terminal'):
        submit(queue, boundary_settings)
    client = boundary_http(queue, short_page())
    job = jobs.claim_candidate(queue, client, boundary_settings, 'bastillepost')
    jobs.finish_candidate(queue, job, result=jobs.fetch_candidate(client, job))
    new = submit(queue, boundary_settings)
    assert new['request_id'] != old['request_id']
    assert not boundary_submit(queue, boundary_settings)['submitted']
    target = jobs._head(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    assert jobs._key(target, 'a'*64, evidence_contract_version=9) == jobs._key(
        dict(target, attempts=99, due=0), 'b'*64, evidence_contract_version=9)


def test_refusals_remain_ready_and_unknown_owner_loss_does_not_retry(boundary_settings, queue):
    client = boundary_http(queue, short_page()); request = submit(queue, boundary_settings)
    with patch.object(jobs.time, 'monotonic', return_value=0.0):
        jobs.pump_candidates(queue)
    client.blocked_robots_origins = lambda p: ('https://www.bastillepost.com',)
    assert jobs.claim_candidate(queue, client, boundary_settings, 'bastillepost') is None
    client.blocked_robots_origins = lambda p: ()
    assert jobs.claim_candidate(queue, client, boundary_settings, 'bastillepost')
    queue.close()
    reopened = NewsQueue(queue.root, queue.registry)
    try:
        with patch.object(jobs.time, 'monotonic', return_value=0.0):
            jobs.pump_candidates(reopened)
        state = jobs.candidate_status(queue.root, request['request_id'])
        assert state['state'] == 'uncertain_outcome' and state['raw_retention_state'] == 'unknown'
        assert state['raw_or_names_or_body_retained'] is None
        assert not submit(reopened, boundary_settings)['submitted']
        assert jobs.claim_candidate(reopened, client, boundary_settings, 'bastillepost') is None
        assert client.calls == []
    finally:
        reopened.close()


def test_write_failure_is_terminal_without_accepting_body(boundary_settings, queue, monkeypatch):
    before = head(queue); client = boundary_http(queue, short_page())
    request, job = claim(queue, boundary_settings, client)
    pending = jobs.fetch_candidate(client, job)
    def fail(*a, **k):
        raise OSError('synthetic full disk')
    monkeypatch.setattr(jobs, 'retain_original', fail)
    jobs.finish_candidate(queue, job, result=pending)
    state = jobs.candidate_status(queue.root, request['request_id'])
    assert state['state'] == 'observed_error' and state['raw_retention_state'] == 'unknown'
    assert state['raw_or_names_or_body_retained'] is None and head(queue) == before
    assert not submit(queue, boundary_settings)['submitted']


def test_stale_completion_is_rejected_before_private_write(boundary_settings, queue):
    client = boundary_http(queue, short_page()); request, job = claim(queue, boundary_settings, client)
    pending = jobs.fetch_candidate(client, job)
    with queue.db:
        queue.db.execute("UPDATE native_candidate_jobs SET owner_id='other' WHERE request_id=?", (request['request_id'],))
    with pytest.raises(ValueError, match='candidate_completion_owner_mismatch'):
        jobs.finish_candidate(queue, job, result=pending)
    assert not (queue.root/'reviewed_repairs/candidate_diagnostics/originals').exists()


def test_plain_result_cannot_forge_a_private_original(boundary_settings, queue):
    request, job = claim(queue, boundary_settings, boundary_http(queue, short_page()))
    jobs.finish_candidate(queue, job, result={'accepted_as_article': False,
        'raw_or_names_or_body_retained': True, 'private_original': {'path': 'fake'}})
    result = jobs.candidate_status(queue.root, request['request_id'])
    assert result['state'] == 'observed_error' and not result['raw_or_names_or_body_retained']
    assert result['result_json']['error'] == 'invalid_candidate_result'


def test_racing_pending_questions_serialize_without_deadlock_or_repeating_http(boundary_settings, queue):
    boundary = boundary_submit(queue, boundary_settings)
    original = submit(queue, boundary_settings)  # Neither mailbox file is registered yet.
    with patch.object(jobs.time, 'monotonic', return_value=0.0):
        jobs.pump_candidates(queue)
    http = boundary_http(queue, short_page())
    first = jobs.claim_candidate(queue, http, boundary_settings, 'bastillepost')
    assert first['request_id'] == boundary['request_id']
    assert jobs.claim_candidate(queue, http, boundary_settings, 'bastillepost') is None
    jobs.finish_candidate(queue, first, result=jobs.fetch_candidate(http, first))
    second = jobs.claim_candidate(queue, http, boundary_settings, 'bastillepost')
    assert second['request_id'] == original['request_id']
    jobs.finish_candidate(queue, second, result=jobs.fetch_candidate(http, second))
    assert http.calls == [URL, URL]  # Two DIFFERENT fixed questions, once each.


@pytest.mark.parametrize('changes', [
    {'allowed': False}, {'provider': 'other'},
    {'embedded_article_layout': 'bastillepost_visible_article_v1'}, {'known_url_only': False},
    {'authorization': {'kind': 'unknown'}},
])
def test_private_scope_is_not_granted_by_guest_visibility_alone(queue, changes):
    source = replace(queue.registry.sources['bastillepost'], **changes)
    response = Response(URL, 200, {'content-type': 'text/html'}, short_page().encode())
    with pytest.raises(NewsError):
        private_bastille_short_original_permit(response, source, URL)


@pytest.mark.parametrize('change', [{'state': 'complete', 'body_sha256': 'a'*64},
    {'state': 'article_identity_not_verified', 'error_code': 'article_identity_not_verified'},
    {'due': 9999999999}, {'lease_owner': 'other'}])
def test_saved_nonshort_failure_future_or_leased_head_is_not_rearmed(boundary_settings, queue, change):
    boundary_failure(queue, **change)
    with pytest.raises(ValueError):
        submit(queue, boundary_settings)


def test_cli_submits_private_mailbox_not_http(boundary_settings, queue, tmp_path, monkeypatch):
    from scripts import probe_news_sources as cli
    monkeypatch.setattr(cli, 'load_config', lambda p: (boundary_settings, queue.registry, tmp_path))
    monkeypatch.setattr(cli, 'NewsHTTP', lambda *a, **k: pytest.fail('submitter must not own HTTP'))
    assert cli.main(['--probe', URL, '--candidate-embedded-layout', LAYOUT, '--enqueue-candidate',
        '--require-current-missing-body-failure', '--failure-code', 'body_not_found',
        '--candidate-evidence-contract', '9', '--output', str(tmp_path/'submission.json')]) == 0


AUDIO = '<iframe allowtransparency="true" attrmap="opaque" style="height:20px" src="https://www.bastillepost.com/hongkong/article-audio?opaque_key=1"></iframe>'


def continuation_page():
    return page(body=AUDIO+'<p class="font-size-scaler-control">第一容器未處理數值 -12.50、0%。</p>').replace(
        '<div class="fb-comments">', '<div class="article-body"><p class="font-size-scaler-control">'
        '第二容器的關係尚未證明，不可當摘要丟掉。</p></div><div class="fb-comments">')


@pytest.fixture
def continuation_config(boundary_settings):
    return versioned(boundary_settings,6)


@pytest.fixture
def continuation_queue(continuation_config,tmp_path):
    value=NewsQueue(tmp_path,PolicyRegistry(continuation_config,base=tmp_path))
    boundary_failure(value)
    yield value
    value.close()


def continuation_submit(queue,config):
    return jobs.submit_candidate(queue.root,config,queue.registry,URL,'bastillepost_visible_article_v6',
        reason='One exact private double-container witness; no accepted body relationship',evidence_contract_version=11)


def continuation_claim(queue,config,http):
    request=continuation_submit(queue,config)
    with patch.object(jobs.time,'monotonic',return_value=0.0):
        jobs.pump_candidates(queue)
    job=jobs.claim_candidate(queue,http,config,'bastillepost')
    assert job['request_id']==request['request_id'] and job['evidence_contract_version']==11
    return request,job


@pytest.mark.parametrize('engine',['lxml','html.parser'])
def test_two_container_original_is_private_exact_and_not_a_body_or_parser_oracle(
        continuation_config,continuation_queue,engine,monkeypatch):
    from downloader import news_fulltext_candidates as candidates
    queue=continuation_queue;source=replace(queue.registry.sources['bastillepost'],html_parser=engine)
    raw=continuation_page().encode();response=Response(URL,200,{'content-type':'text/html'},raw)
    assert private_bastille_continuation_original_permit(response,source,URL)['question']==BASTILLE_CONTINUATION_QUESTION
    with pytest.raises(NewsError,match='^body_not_found$'):
        extract_article(raw,source,page_url=URL)
    monkeypatch.setattr(candidates,'extract_article',lambda *a,**k:pytest.fail('not a body oracle'))
    before=head(queue);client=boundary_http(queue,continuation_page())
    request,job=continuation_claim(queue,continuation_config,client)
    assert jobs.candidate_status(queue.root,request['request_id'])['raw_retention_state']=='unknown'
    pending=jobs.fetch_candidate(client,job)
    assert isinstance(pending,PendingOriginal) and pending.permit['contract_version']==4
    jobs.finish_candidate(queue,job,result=pending)
    with patch.object(jobs.time,'monotonic',return_value=0.0):
        jobs.pump_candidates(queue)
    result=jobs.candidate_status(queue.root,request['request_id']);facts=result['result_json']
    assert result['state']=='observed' and result['raw_retention_state']=='retained_verified'
    assert facts['question']==BASTILLE_CONTINUATION_QUESTION and not facts['current_parser_replayed']
    assert not facts['accepted_as_article'] and not facts['article_heads_mutated']
    ref=facts['private_original'];rows=pq.ParquetFile(queue.root/ref['path']).read().to_pylist()
    assert len(rows)==1 and rows[0]['raw_response_body']==raw
    assert rows[0]['response_sha256']==hashlib.sha256(raw).hexdigest()
    assert json.loads(rows[0]['transport_redirects_json'])==[]
    assert rows[0]['source_document_url']==URL
    assert hashlib.sha256((queue.root/ref['path']).read_bytes()).hexdigest()==ref['sha256']
    receipt=json.loads((queue.root/ref['receipt_path']).read_bytes())
    assert receipt['question']==BASTILLE_CONTINUATION_QUESTION and receipt['permit']==pending.permit
    for flag in ('accepted_as_article','publish','historical_point_in_time','per_article_license_verified',
            'training_injection_enabled','remote_training_export'):
        assert rows[0][flag] is False and receipt[flag] is False
    assert '第一容器' not in json.dumps(facts,ensure_ascii=False) and '第二容器' not in json.dumps(facts,ensure_ascii=False)
    assert all(not (queue.root/ref[key]).stat().st_mode & 0o077 for key in ('path','receipt_path'))
    assert head(queue)==before and client.calls==[URL]
    assert not continuation_submit(queue,continuation_config)['submitted']
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0]==0
    with pytest.raises(NewsError,match='^body_not_found$'):
        extract_article(raw,source,page_url=URL)


@pytest.mark.parametrize('edit',[
    lambda s:s.replace('/article/10123210/', '/article/9999/'),
    lambda s:s.replace('"巴士的報"','"其他署名"'),
    lambda s:s.replace('2022年02月08日 11:56','2022年02月09日 11:56'),
    lambda s:s.replace('第一容器','（星島日報報道）第一容器'),
    lambda s:s.replace('第二容器','轉載自其他網站 第二容器'),
    lambda s:s.replace('class="article-body"','hidden class="article-body"'),
    lambda s:s.replace('class="article-body"','class="article-body d-none"'),
    lambda s:s.replace('第一容器','<script>unknown()</script>第一容器'),
    lambda s:s.replace('第二容器','<iframe src="https://other.test/v"></iframe>第二容器'),
    lambda s:s.replace('/hongkong/article-audio','/hongkong/other'),
    lambda s:s.replace('https://www.bastillepost.com/hongkong/article-audio','https://other.test/hongkong/article-audio'),
    lambda s:s.replace('</iframe>','fallback text must not be stripped</iframe>'),
    lambda s:s.replace(AUDIO,''),
    lambda s:s.replace(AUDIO,AUDIO*2),
    lambda s:s.replace('<div class="fb-comments">','<div class="article-body"><p>第三容器</p></div><div class="fb-comments">'),
    lambda s:s.replace('class="article-body"','class="not-native"',1),
    lambda s:'<title>Just a moment</title>'+s,
    lambda s:s.replace('<html>','<html><div data-paywall="true">'),
])
def test_two_container_witness_cannot_bypass_identity_credit_visibility_or_access(
        continuation_queue,edit):
    source=continuation_queue.registry.sources['bastillepost']
    response=Response(URL,200,{'content-type':'text/html'},edit(continuation_page()).encode())
    with pytest.raises(NewsError):
        private_bastille_continuation_original_permit(response,source,URL)


@pytest.mark.parametrize('changes',[
    {'allowed':False},{'provider':'other'},{'known_url_only':False},
    {'authorization':{'kind':'unknown'}},{'embedded_article_layout':'bastillepost_visible_article_v5'},
])
def test_continuation_original_requires_reviewed_current_native_scope(continuation_queue,changes):
    source=replace(continuation_queue.registry.sources['bastillepost'],**changes)
    with pytest.raises(NewsError):
        private_bastille_continuation_original_permit(
            Response(URL,200,{'content-type':'text/html'},continuation_page().encode()),source,URL)


@pytest.mark.parametrize('changes',[
    {'status':403},{'headers':{'content-type':'application/json'}},{'body':b'x'*((2<<20)+1)},
    {'url':URL.replace('10123210','99999')},
])
def test_continuation_original_requires_actual_public_same_id_response(continuation_queue,changes):
    response=replace(Response(URL,200,{'content-type':'text/html'},continuation_page().encode()),**changes)
    with pytest.raises(NewsError):
        private_bastille_continuation_original_permit(response,continuation_queue.registry.sources['bastillepost'],URL)


def test_continuation_same_id_query_redirect_witness_is_kept_separate(continuation_queue):
    requested=URL+'?current_cat=138496'
    redirect=RedirectHop(requested,301,URL,URL,'2026-10-21T00:00:00Z','b'*64,1)
    response=Response(URL,200,{'content-type':'text/html'},continuation_page().encode(),(redirect,))
    source=continuation_queue.registry.sources['bastillepost']
    permit=private_bastille_continuation_original_permit(response,source,requested)
    assert permit['request_url']==requested and permit['url']==permit['document_url']==URL
    with pytest.raises(NewsError):
        private_bastille_continuation_original_permit(replace(response,redirects=()),source,requested)


def test_original_question_does_not_reopen_or_conflict_with_old_resource_question(continuation_config,continuation_queue):
    queue=continuation_queue;layout=queue.registry.sources['bastillepost'].embedded_article_layout
    previous=jobs.submit_candidate(queue.root,continuation_config,queue.registry,URL,layout,
        reason='Original different resource question',evidence_contract_version=10)
    original=continuation_submit(queue,continuation_config);client=boundary_http(queue,continuation_page())
    with patch.object(jobs.time,'monotonic',return_value=0.0):
        jobs.pump_candidates(queue)
    first=jobs.claim_candidate(queue,client,continuation_config,'bastillepost')
    assert first['request_id']==previous['request_id']
    assert jobs.claim_candidate(queue,client,continuation_config,'bastillepost') is None
    jobs.finish_candidate(queue,first,result=jobs.fetch_candidate(client,first))
    second=jobs.claim_candidate(queue,client,continuation_config,'bastillepost')
    assert second['request_id']==original['request_id']
    jobs.finish_candidate(queue,second,result=jobs.fetch_candidate(client,second))
    assert client.calls==[URL,URL]  # Once each, two distinct fixed questions.
    target=jobs._head(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
    assert jobs._key(target,'a'*64,evidence_contract_version=11)==jobs._key(
        dict(target,attempts=999,due=0),'b'*64,evidence_contract_version=11)
    assert not continuation_submit(queue,continuation_config)['submitted']


def test_continuation_refusals_and_unknown_restart_do_not_issue_another_get(continuation_config,continuation_queue):
    queue=continuation_queue;client=boundary_http(queue,continuation_page())
    request=continuation_submit(queue,continuation_config)
    with patch.object(jobs.time,'monotonic',return_value=0.0):
        jobs.pump_candidates(queue)
    client.blocked_robots_origins=lambda p:('https://www.bastillepost.com',)
    assert jobs.claim_candidate(queue,client,continuation_config,'bastillepost') is None
    client.blocked_robots_origins=lambda p:()
    assert jobs.claim_candidate(queue,client,continuation_config,'bastillepost')
    queue.close();reopened=NewsQueue(queue.root,queue.registry)
    try:
        with patch.object(jobs.time,'monotonic',return_value=0.0):
            jobs.pump_candidates(reopened)
        result=jobs.candidate_status(queue.root,request['request_id'])
        assert result['state']=='uncertain_outcome' and result['raw_retention_state']=='unknown'
        assert result['raw_or_names_or_body_retained'] is None
        assert not continuation_submit(reopened,continuation_config)['submitted']
        assert jobs.claim_candidate(reopened,client,continuation_config,'bastillepost') is None
        assert client.calls==[]
    finally:
        reopened.close()


@pytest.mark.parametrize('changes',[
    {'state':'complete','body_sha256':'a'*64},{'due':9999999999},{'lease_owner':'other'},
    {'state':'article_identity_not_verified','error_code':'article_identity_not_verified'},
])
def test_continuation_request_cannot_rearm_saved_future_leased_or_other_failure(
        continuation_config,continuation_queue,changes):
    boundary_failure(continuation_queue,**changes)
    with pytest.raises(ValueError):
        continuation_submit(continuation_queue,continuation_config)


def test_continuation_stale_completion_and_partial_write_are_not_new_retries(continuation_config,continuation_queue,monkeypatch):
    queue=continuation_queue;client=boundary_http(queue,continuation_page());before=head(queue)
    request,job=continuation_claim(queue,continuation_config,client)
    pending=jobs.fetch_candidate(client,job)
    with queue.db:
        queue.db.execute("UPDATE native_candidate_jobs SET owner_id='other' WHERE request_id=?",(request['request_id'],))
    with pytest.raises(ValueError,match='candidate_completion_owner_mismatch'):
        jobs.finish_candidate(queue,job,result=pending)
    assert not (queue.root/'reviewed_repairs/candidate_diagnostics/originals').exists()
    with queue.db:
        queue.db.execute('UPDATE native_candidate_jobs SET owner_id=? WHERE request_id=?',(queue.owner,request['request_id']))
    def fail(*a,**k):
        raise OSError('synthetic full disk')
    monkeypatch.setattr(jobs,'retain_original',fail)
    jobs.finish_candidate(queue,job,result=pending)
    result=jobs.candidate_status(queue.root,request['request_id'])
    assert result['state']=='observed_error' and result['raw_retention_state']=='unknown'
    assert result['raw_or_names_or_body_retained'] is None and head(queue)==before
    assert not continuation_submit(queue,continuation_config)['submitted']


def test_cli_continuation_is_resident_only_and_future_evidence_is_closed(
        continuation_config,continuation_queue,tmp_path,monkeypatch):
    from scripts import probe_news_sources as cli
    queue=continuation_queue
    monkeypatch.setattr(cli,'load_config',lambda p:(continuation_config,queue.registry,tmp_path))
    monkeypatch.setattr(cli,'NewsHTTP',lambda *a,**k:pytest.fail('no submitter HTTP'))
    args=['--probe',URL,'--candidate-embedded-layout','bastillepost_visible_article_v6',
        '--require-current-missing-body-failure','--failure-code','body_not_found',
        '--candidate-evidence-contract','11','--output',str(tmp_path/'submission.json')]
    with pytest.raises(SystemExit):
        cli.main(args)
    assert cli.main(args+['--enqueue-candidate'])==0
    target=jobs._head(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
    with pytest.raises(ValueError,match='reviewed_candidate_evidence_contract_required'):
        jobs._key(target,'a'*64,evidence_contract_version=12)

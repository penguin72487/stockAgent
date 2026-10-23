"""Once-only exact original after a positive current ETtoday credit lead."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from types import SimpleNamespace
import time

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import (
    ETTODAY_CREDIT_ORIGINAL_QUESTION, PendingOriginal, private_ettoday_credit_original_permit, retain_original,
    ETTODAY_INLINE_ORIGINAL_QUESTION, private_ettoday_inline_credit_original_permit,
    ETTODAY_VISIBLE_ORIGINAL_QUESTION, private_ettoday_visible_credit_original_permit,
    ETTODAY_IMAGE_ORIGINAL_QUESTION, private_ettoday_image_credit_original_permit,
    ETTODAY_NONEMPTY_ORIGINAL_QUESTION, private_ettoday_nonempty_credit_original_permit,
    ETTODAY_CANONICAL_ORIGINAL_QUESTION, private_ettoday_canonical_credit_original_permit,
)
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
import test_news_ettoday as native

URL, LAYOUT = native.URLS[1], 'ettoday_visible_article_v39'
SIGNATURE = '記者研究記者／台北報導'


def page():
    soup = native.page('www')
    body = soup.select_one('.story')
    body.clear()
    first = soup.new_tag('p')
    first.append(SIGNATURE)
    first.append(soup.new_tag('br'))
    first.append(soup.new_tag('br'))
    first.append(native.PROSE)
    body.append(first)
    more = soup.new_tag('p'); more.string = native.NOTICE
    body.append(more)
    return soup


def response(soup=None, **changes):
    return replace(Response(URL,200,{'content-type':'text/html; charset=utf-8'},
        str(soup if soup is not None else page()).encode()), **changes)


@pytest.fixture
def settings(tmp_path):
    entry = native.recipe(); entry['embedded_article_layout'] = LAYOUT
    return {'contract_version':1,'root':str(tmp_path),'collection_purpose':'personal_research',
        'publish':False,'remote_training_export':False,'providers':[entry],
        'min_free_disk_bytes':1,'workers':2,'exclusive_http_owner':False,'adaptive_rate':{'enabled':False}}


@pytest.fixture
def queue(settings,tmp_path):
    owner = NewsQueue(tmp_path,PolicyRegistry(settings,base=tmp_path))
    owner.add_articles([{'url':URL}])
    with owner.db:
        owner.db.execute('UPDATE articles SET state=?,error_code=?,attempts=7,due=?,last_checked_at_utc=?',
            ('article_attribution_not_verified','article_attribution_not_verified',time.time()-60,
             '2026-01-01T00:00:00+00:00'))
    yield owner
    owner.close()


def head(queue):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())


def submit(queue,settings,version=28,**kwargs):
    return diagnostic.submit_candidate(queue.root,settings,queue.registry,URL,LAYOUT,
        reason=kwargs.pop('reason','Exact private leading-reporter original after completed geometry'),
        evidence_contract_version=version,**kwargs)


def http_stub(queue,envelope=None):
    calls = []
    def get(url):
        calls.append(url)
        return envelope or response()
    return SimpleNamespace(registry=queue.registry,get=get,calls=calls,provider_not_before={},
        blocked_gateway_hosts=lambda p:(),blocked_robots_origins=lambda p:())


def complete_boundary(queue,settings):
    request = submit(queue,settings,27)
    diagnostic.pump_candidates(queue)
    http = http_stub(queue)
    job = diagnostic.claim_candidate(queue,http,settings,'ettoday')
    diagnostic.finish_candidate(queue,job,result=diagnostic.fetch_candidate(http,job))
    diagnostic.pump_candidates(queue)
    return request


def test_actual_signature_and_independent_person_prove_private_scope_not_article(queue,monkeypatch):
    monkeypatch.setattr('downloader.news_fulltext_ettoday.ettoday_visible_article',
        lambda *a,**k:pytest.fail('A diagnostic is not an accepting parse'))
    permit = private_ettoday_credit_original_permit(response(),queue.registry.sources['ettoday'],URL)
    assert permit['question'] == ETTODAY_CREDIT_ORIGINAL_QUESTION
    assert permit['printed_signature_sha256'] == hashlib.sha256(SIGNATURE.encode()).hexdigest()
    assert SIGNATURE not in json.dumps(permit,ensure_ascii=False)


@pytest.mark.parametrize('change',[
    {'status':403},{'status':429},{'status':500},{'body':b''},{'body':b'x'*((2<<20)+1)},
    {'url':URL+'0'},{'headers':{'content-type':'application/json'}},
    {'body':b'<html><title>Just a moment</title></html>'},
    {'body':b'<html><div class="paywall">Login required</div></html>'},
])
def test_transport_refusals_do_not_grant_original(queue,change):
    with pytest.raises(NewsError):
        private_ettoday_credit_original_permit(response(**change),queue.registry.sources['ettoday'],URL)


@pytest.mark.parametrize('mutation',[
    'canonical','schema_identity','person','organization','clock','paid','hidden','missing_br',
    'embedded_signature','unreviewed_markup','no_prose','reprint','iframe','duplicate_story',
])
def test_native_scope_must_be_positive_and_same_article(queue,mutation):
    soup = page(); first = soup.select_one('.story > p')
    if mutation == 'canonical': soup.select_one('link[rel=canonical]')['href'] = URL+'0'
    elif mutation == 'schema_identity': native.update_schema(soup,url=URL+'0')
    elif mutation == 'person': native.update_schema(soup,author={'@type':'Person','name':'另一作者'})
    elif mutation == 'organization': native.update_schema(soup,author={
        '@type':'Organization','name':'ETtoday新聞雲','url':'https://www.ettoday.net/'})
    elif mutation == 'clock': soup.select_one('meta[name=pubdate]')['content'] = native.MOD
    elif mutation == 'paid': native.update_schema(soup,isAccessibleForFree=False)
    elif mutation == 'hidden': first['hidden'] = ''
    elif mutation == 'missing_br':
        for br in first.find_all('br'): br.decompose()
    elif mutation == 'embedded_signature': first.contents[0].replace_with('未知前言'+SIGNATURE)
    elif mutation == 'unreviewed_markup': first.contents[0].wrap(soup.new_tag('span'))
    elif mutation == 'no_prose': first.clear(); first.append(SIGNATURE); first.append(soup.new_tag('br'))
    elif mutation == 'reprint': soup.select_one('.story > p:last-child').string = '本文轉載自未驗證來源'
    elif mutation == 'iframe': soup.select_one('.story').append(soup.new_tag('iframe'))
    else: soup.select_one('.subject_article').append(deepcopy(soup.select_one('.story')))
    with pytest.raises(NewsError):
        private_ettoday_credit_original_permit(response(soup),queue.registry.sources['ettoday'],URL)


def test_missing_completed_boundary_cannot_authorize_original(queue,settings):
    before = head(queue)
    with pytest.raises(ValueError,match='completed_ettoday_leading_reporter_break_proof_required'):
        submit(queue,settings)
    assert head(queue) == before


@pytest.mark.parametrize('change',['ready','reserved','uncertain_outcome','head','person','clock','request_hash'])
def test_submission_requires_unchanged_positive_predecessor(queue,settings,change):
    prior = complete_boundary(queue,settings)
    with queue.db:
        if change in {'ready','reserved','uncertain_outcome'}:
            queue.db.execute('UPDATE native_candidate_jobs SET state=? WHERE request_id=?',(change,prior['request_id']))
        elif change == 'head': queue.db.execute('UPDATE articles SET attempts=attempts+1')
        elif change == 'clock': queue.db.execute("UPDATE native_candidate_jobs SET completed_at_utc='2099-01-01T00:00:00+00:00'")
        elif change == 'request_hash':
            queue.db.execute("UPDATE native_candidate_jobs SET request_json=request_json||' '")
        else:
            row = queue.db.execute('SELECT result_json FROM native_candidate_jobs').fetchone()
            facts = json.loads(row[0]); facts['ettoday_native_boundary']['declared_author_types'] = [['Organization']]
            queue.db.execute('UPDATE native_candidate_jobs SET result_json=?',(json.dumps(facts),))
    with pytest.raises(ValueError): submit(queue,settings)


def test_owner_retains_private_original_without_replaying_boundary_or_mutating_heads(queue,settings,monkeypatch):
    old = complete_boundary(queue,settings)
    before = head(queue)
    prior = dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(old['request_id'],)).fetchone())
    budgets = [tuple(r) for r in queue.db.execute('SELECT * FROM queue_settings')]
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',lambda *a,**k:pytest.fail('No second HTTP owner'))
    submitted = submit(queue,settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(queue)
    job = diagnostic.claim_candidate(queue,http,settings,'ettoday')
    result = diagnostic.fetch_candidate(http,job)
    assert isinstance(result,PendingOriginal)
    assert not list(queue.root.glob('reviewed_repairs/candidate_diagnostics/originals/*'))
    diagnostic.finish_candidate(queue,job,result=result)
    diagnostic.pump_candidates(queue)
    status = diagnostic.candidate_status(queue.root,submitted['request_id'])
    assert status['state'] == 'observed' and status['raw_retention_state'] == 'retained_verified'
    reference = status['result_json']['private_original']
    path = queue.root/reference['path']
    record = pq.ParquetFile(path).read().to_pylist()[0]
    assert record['raw_response_body'] == result.response.body and path.stat().st_mode & 0o777 == 0o600
    assert record['source_document_url'] == URL and record['transport_redirects_json'] == '[]'
    assert not record['accepted_as_article'] and not record['publish'] and not record['remote_training_export']
    assert head(queue) == before and http.calls == [URL]
    assert dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(old['request_id'],)).fetchone()) == prior
    assert [tuple(r) for r in queue.db.execute('SELECT * FROM queue_settings')] == budgets
    assert not submit(queue,settings,reason='Changed reason cannot rearm the question')['submitted']
    assert SIGNATURE not in json.dumps(status['result_json'],ensure_ascii=False)


@pytest.mark.parametrize('change',['lease','due','prior'])
def test_claim_rechecks_gate_after_submission(queue,settings,change):
    old = complete_boundary(queue,settings); submit(queue,settings); diagnostic.pump_candidates(queue)
    with queue.db:
        if change == 'lease': queue.db.execute("UPDATE articles SET lease_owner='other',lease_until=?",(time.time()+3600,))
        elif change == 'due': queue.db.execute('UPDATE articles SET due=?',(time.time()+3600,))
        else: queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(old['request_id'],))
    http = http_stub(queue)
    assert diagnostic.claim_candidate(queue,http,settings,'ettoday') is None and not http.calls


def test_writer_rechecks_actual_raw_signature(queue,settings):
    complete_boundary(queue,settings); submit(queue,settings); diagnostic.pump_candidates(queue)
    job = diagnostic.claim_candidate(queue,http_stub(queue),settings,'ettoday')
    permit = private_ettoday_credit_original_permit(response(),job['source'],URL)
    changed = replace(response(),body=response().body.replace(SIGNATURE.encode(),b'unknown'))
    with pytest.raises((NewsError,ValueError)):
        retain_original(queue,job,PendingOriginal(changed,{},permit))
    assert not list(queue.root.glob('reviewed_repairs/candidate_diagnostics/originals/*'))


def test_cli_routes_private_question_to_current_source(queue,settings,monkeypatch,tmp_path):
    from scripts import probe_news_sources as cli
    monkeypatch.setattr(cli,'load_config',lambda p:(settings,queue.registry,tmp_path))
    monkeypatch.setattr(cli,'NewsHTTP',lambda *a,**k:pytest.fail('Mailbox only'))
    complete_boundary(queue,settings)
    output = tmp_path/'submitted.json'
    assert cli.main(['--output',str(output),'--probe',URL,'--require-current-missing-body-failure',
        '--failure-code','article_attribution_not_verified','--candidate-embedded-layout',LAYOUT,
        '--candidate-evidence-contract','28','--enqueue-candidate']) == 0
    assert json.loads(output.read_text())['submitted']


def test_original_key_is_distinct_stable_and_provider_bound():
    target = {'provider':'ettoday','id':'a'*64}
    old = diagnostic._key(target,'b'*64,evidence_contract_version=27)
    new = diagnostic._key(target,'b'*64,evidence_contract_version=28)
    assert new != old and new == diagnostic._key(target,'c'*64,evidence_contract_version=28)
    with pytest.raises(ValueError):
        diagnostic._key(dict(target,provider='cmnews'),'b'*64,evidence_contract_version=28)


def test_unknown_original_outcome_is_not_retried_or_reported_raw_free(queue,settings):
    from downloader.news_fulltext_candidate_originals import retention_flags
    complete_boundary(queue,settings); request = submit(queue,settings); diagnostic.pump_candidates(queue)
    assert diagnostic.claim_candidate(queue,http_stub(queue),settings,'ettoday')
    with queue.db:
        queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(request['request_id'],))
    assert not submit(queue,settings)['submitted']
    http = http_stub(queue)
    assert diagnostic.claim_candidate(queue,http,settings,'ettoday') is None and not http.calls
    flags = retention_flags({'evidence_contract_version':28},None,'uncertain_outcome')
    assert flags == {'raw_or_names_or_body_retained':None,'raw_retention_state':'unknown'}


def inline_page(tag='strong',nested_break=False):
    soup = page(); first = soup.select_one('.story > p')
    wrapper = soup.new_tag(tag)
    first.contents[0].wrap(wrapper)
    if nested_break: wrapper.append(first.find('br').extract())
    return soup


def complete_plain_refusal(queue,settings):
    complete_boundary(queue,settings)
    request = submit(queue,settings); diagnostic.pump_candidates(queue)
    http = http_stub(queue,response(inline_page()))
    job = diagnostic.claim_candidate(queue,http,settings,'ettoday')
    try:
        diagnostic.fetch_candidate(http,job)
    except NewsError as error:
        assert error.code == 'private_ettoday_credit_original_scope_not_verified'
        diagnostic.finish_candidate(queue,job,error=error)
    else: pytest.fail('The plain-line question cannot accept an inline credit')
    diagnostic.pump_candidates(queue)
    return request


@pytest.mark.parametrize('tag,nested_break',[(tag,flag) for tag in ('strong','span','b','em','a') for flag in (False,True)])
def test_inline_original_requires_real_literal_signature_and_matching_person(queue,tag,nested_break):
    permit = private_ettoday_inline_credit_original_permit(
        response(inline_page(tag,nested_break)),queue.registry.sources['ettoday'],URL)
    assert permit['question'] == ETTODAY_INLINE_ORIGINAL_QUESTION
    assert permit['printed_signature_sha256'] == hashlib.sha256(SIGNATURE.encode()).hexdigest()
    assert SIGNATURE not in json.dumps(permit,ensure_ascii=False)


@pytest.mark.parametrize('change',['foreign_person','organization','hidden','unknown_tag','paid','split_place','no_prose'])
def test_inline_scope_does_not_infer_credit_or_ignore_rights(queue,change):
    soup = inline_page()
    if change == 'foreign_person': native.update_schema(soup,author={'@type':'Person','name':'另一作者'})
    elif change == 'organization': native.update_schema(soup,author={
        '@type':'Organization','name':'ETtoday新聞雲','url':'https://www.ettoday.net/'})
    elif change == 'hidden': soup.select_one('.story strong')['hidden'] = ''
    elif change == 'unknown_tag': soup.select_one('.story strong').name = 'arbitrary-credit-wrapper'
    elif change == 'paid': native.update_schema(soup,isAccessibleForFree=False)
    elif change == 'split_place': soup.select_one('.story strong').string = '記者研究記者／未知'
    else:
        first = soup.select_one('.story > p'); first.clear()
        node = soup.new_tag('strong'); node.string = SIGNATURE
        first.append(node); first.append(soup.new_tag('br'))
    with pytest.raises(NewsError):
        private_ettoday_inline_credit_original_permit(response(soup),queue.registry.sources['ettoday'],URL)


def test_inline_question_requires_completed_plain_scope_refusal(queue,settings):
    complete_boundary(queue,settings)
    with pytest.raises(ValueError,match='completed_ettoday_plain_credit_original_scope_refusal_required'):
        submit(queue,settings,29)


@pytest.mark.parametrize('mutation',['uncertain_outcome','error','raw','site','future_clock','request_hash'])
def test_inline_question_rechecks_actual_previous_outcome(queue,settings,mutation):
    request = complete_plain_refusal(queue,settings)
    with queue.db:
        if mutation == 'uncertain_outcome':
            queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(request['request_id'],))
        elif mutation == 'future_clock':
            queue.db.execute("UPDATE native_candidate_jobs SET completed_at_utc='2099-01-01T00:00:00+00:00' WHERE request_id=?",(request['request_id'],))
        elif mutation == 'request_hash':
            queue.db.execute("UPDATE native_candidate_jobs SET request_json=request_json||' ' WHERE request_id=?",(request['request_id'],))
        else:
            raw = queue.db.execute('SELECT result_json FROM native_candidate_jobs WHERE request_id=?',(request['request_id'],)).fetchone()[0]
            result = json.loads(raw)
            if mutation == 'error': result['error'] = 'paywall_or_login'
            elif mutation == 'raw': result['raw_or_names_or_body_retained'] = True
            else: result['failure_location']['sites'] = []
            queue.db.execute('UPDATE native_candidate_jobs SET result_json=? WHERE request_id=?',(json.dumps(result),request['request_id']))
    with pytest.raises(ValueError): submit(queue,settings,29)


def test_inline_original_new_question_preserves_closed_plain_and_boundary_results(queue,settings):
    plain = complete_plain_refusal(queue,settings)
    previous = {r['request_id']:dict(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')}
    before = head(queue)
    request = submit(queue,settings,29); diagnostic.pump_candidates(queue)
    http = http_stub(queue,response(inline_page('strong',True)))
    job = diagnostic.claim_candidate(queue,http,settings,'ettoday')
    result = diagnostic.fetch_candidate(http,job)
    assert isinstance(result,PendingOriginal) and result.permit['question'] == ETTODAY_INLINE_ORIGINAL_QUESTION
    diagnostic.finish_candidate(queue,job,result=result); diagnostic.pump_candidates(queue)
    stored = diagnostic.candidate_status(queue.root,request['request_id'])
    assert stored['state'] == 'observed' and stored['raw_retention_state'] == 'retained_verified'
    assert head(queue) == before and http.calls == [URL]
    assert all(dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(key,)).fetchone()) == value
        for key,value in previous.items())
    assert not submit(queue,settings,29)['submitted']
    assert diagnostic.candidate_status(queue.root,plain['request_id'])['state'] == 'observed_error'


def test_inline_cli_routes_to_current_owner_after_actual_plain_refusal(queue,settings,monkeypatch,tmp_path):
    from scripts import probe_news_sources as cli
    complete_plain_refusal(queue,settings)
    monkeypatch.setattr(cli,'load_config',lambda p:(settings,queue.registry,tmp_path))
    monkeypatch.setattr(cli,'NewsHTTP',lambda *a,**k:pytest.fail('Mailbox only'))
    output = tmp_path/'inline-submitted.json'
    assert cli.main(['--output',str(output),'--probe',URL,'--require-current-missing-body-failure',
        '--failure-code','article_attribution_not_verified','--candidate-embedded-layout',LAYOUT,
        '--candidate-evidence-contract','29','--enqueue-candidate']) == 0
    assert json.loads(output.read_text())['submitted']


def complete_inline_refusal(queue,settings):
    complete_plain_refusal(queue,settings)
    request = submit(queue,settings,29); diagnostic.pump_candidates(queue)
    http = http_stub(queue,response(inline_page('font')))
    job = diagnostic.claim_candidate(queue,http,settings,'ettoday')
    with pytest.raises(NewsError) as failed:
        diagnostic.fetch_candidate(http,job)
    assert failed.value.code == 'private_ettoday_credit_original_scope_not_verified'
    diagnostic.finish_candidate(queue,job,error=failed.value); diagnostic.pump_candidates(queue)
    return request


@pytest.mark.parametrize('tag',['font','mark','small','strong','custom-format'])
def test_visible_private_scope_is_independent_of_formatting_vocabulary(queue,tag):
    from bs4 import Comment
    soup = inline_page(tag,True)
    soup.select_one('.story > p').insert(0,Comment('not a rendered signature'))
    permit = private_ettoday_visible_credit_original_permit(response(soup),queue.registry.sources['ettoday'],URL)
    assert permit['question'] == ETTODAY_VISIBLE_ORIGINAL_QUESTION
    assert permit['printed_signature_sha256'] == hashlib.sha256(SIGNATURE.encode()).hexdigest()
    assert SIGNATURE not in json.dumps(permit,ensure_ascii=False)


@pytest.mark.parametrize('mutation',[
    'comment_only','active','hidden','no_prose','no_separator','person','organization','paid','clock',
    'prefix_limit','split_signature','reprint',
])
def test_visible_scope_keeps_literal_person_clock_access_guards(queue,mutation):
    from bs4 import Comment
    soup = inline_page('font'); first = soup.select_one('.story > p')
    if mutation == 'comment_only': first.contents[0].replace_with(Comment(SIGNATURE))
    elif mutation == 'active': first.contents[0].name = 'template'
    elif mutation == 'hidden': first.contents[0]['aria-hidden'] = 'true'
    elif mutation == 'no_prose':
        first.clear(); first.append(SIGNATURE); first.append(soup.new_tag('br'))
    elif mutation == 'no_separator':
        for br in first.find_all('br'): br.decompose()
    elif mutation == 'person': native.update_schema(soup,author={'@type':'Person','name':'另一作者'})
    elif mutation == 'organization': native.update_schema(soup,author={'@type':'Organization','name':'ETtoday新聞雲'})
    elif mutation == 'paid': native.update_schema(soup,isAccessibleForFree=False)
    elif mutation == 'clock': soup.select_one('meta[name=pubdate]')['content'] = native.MOD
    elif mutation == 'prefix_limit':
        for _ in range(129): first.insert(0,Comment(''))
    elif mutation == 'split_signature': first.insert(0,'未知前言')
    else: soup.select_one('.story > p:last-child').string = '本文轉載自未驗證來源'
    with pytest.raises(NewsError):
        private_ettoday_visible_credit_original_permit(response(soup),queue.registry.sources['ettoday'],URL)


@pytest.mark.parametrize('mutation',['uncertain_outcome','error','raw','site','future_clock','request_hash'])
def test_visible_question_requires_real_unchanged_closed_inline_refusal(queue,settings,mutation):
    request = complete_inline_refusal(queue,settings)
    with queue.db:
        if mutation == 'uncertain_outcome':
            queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(request['request_id'],))
        elif mutation == 'future_clock':
            queue.db.execute("UPDATE native_candidate_jobs SET completed_at_utc='2099-01-01T00:00:00+00:00' WHERE request_id=?",(request['request_id'],))
        elif mutation == 'request_hash':
            queue.db.execute("UPDATE native_candidate_jobs SET request_json=request_json||' ' WHERE request_id=?",(request['request_id'],))
        else:
            value = json.loads(queue.db.execute('SELECT result_json FROM native_candidate_jobs WHERE request_id=?',(request['request_id'],)).fetchone()[0])
            if mutation == 'error': value['error'] = 'paywall_or_login'
            elif mutation == 'raw': value['raw_or_names_or_body_retained'] = True
            else: value['failure_location']['sites'] = []
            queue.db.execute('UPDATE native_candidate_jobs SET result_json=? WHERE request_id=?',(json.dumps(value),request['request_id']))
    with pytest.raises(ValueError): submit(queue,settings,30)


def test_visible_scope_once_only_writer_independently_rechecks_and_preserves_closed_jobs(queue,settings):
    complete_inline_refusal(queue,settings)
    previous = {r['request_id']:dict(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')}
    before = head(queue)
    request = submit(queue,settings,30); diagnostic.pump_candidates(queue)
    http = http_stub(queue,response(inline_page('font',True)))
    job = diagnostic.claim_candidate(queue,http,settings,'ettoday')
    result = diagnostic.fetch_candidate(http,job)
    assert isinstance(result,PendingOriginal)
    assert result.permit['question'] == ETTODAY_VISIBLE_ORIGINAL_QUESTION
    broken = inline_page('font'); native.update_schema(broken,author={'@type':'Person','name':'另一作者'})
    with pytest.raises(NewsError):
        retain_original(queue,job,PendingOriginal(response(broken),{},result.permit))
    diagnostic.finish_candidate(queue,job,result=result); diagnostic.pump_candidates(queue)
    stored = diagnostic.candidate_status(queue.root,request['request_id'])
    assert stored['state'] == 'observed' and stored['raw_retention_state'] == 'retained_verified'
    ref = stored['result_json']['private_original']
    path = queue.root/ref['path']
    record, = pq.ParquetFile(path).read().to_pylist()
    assert path.stat().st_mode&0o777 == 0o600 and record['raw_response_body'] == result.response.body
    assert not record['accepted_as_article'] and not record['publish'] and not record['remote_training_export']
    assert head(queue) == before and http.calls == [URL]
    assert all(dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(key,)).fetchone()) == value
        for key,value in previous.items())
    assert not submit(queue,settings,30,reason='Another reason must not buy another request')['submitted']


@pytest.mark.parametrize('stage',['visible_prefix','printed_reporter','source_clocks','same_article_person','reprint_guard'])
def test_visible_refusal_returns_finite_stage_without_raw_or_another_fetch(queue,settings,stage):
    complete_inline_refusal(queue,settings)
    request = submit(queue,settings,30); diagnostic.pump_candidates(queue)
    soup = inline_page('font')
    if stage == 'visible_prefix': soup.select_one('.story font').name = 'template'
    elif stage == 'printed_reporter': soup.select_one('.story font').string = '沒有署名'
    elif stage == 'source_clocks': soup.select_one('meta[name=pubdate]')['content'] = native.MOD
    elif stage == 'same_article_person': native.update_schema(soup,author={'@type':'Person','name':'另一作者'})
    else: soup.select_one('.story > p:last-child').string = '本文轉載自未驗證來源'
    http = http_stub(queue,response(soup))
    job = diagnostic.claim_candidate(queue,http,settings,'ettoday')
    result = diagnostic.fetch_candidate(http,job)
    assert not isinstance(result,PendingOriginal)
    assert result['visible_credit_scope']['verification_stage'] == stage
    assert not result['raw_or_names_or_body_retained'] and not result['accepted_as_article']
    assert SIGNATURE not in json.dumps(result,ensure_ascii=False)
    diagnostic.finish_candidate(queue,job,result=result); diagnostic.pump_candidates(queue)
    assert diagnostic.candidate_status(queue.root,request['request_id'])['state'] == 'observed'
    assert http.calls == [URL] and not submit(queue,settings,30)['submitted']


def test_visible_cli_uses_resident_mailbox_not_a_new_http_owner(queue,settings,monkeypatch,tmp_path):
    from scripts import probe_news_sources as cli
    complete_inline_refusal(queue,settings)
    monkeypatch.setattr(cli,'load_config',lambda p:(settings,queue.registry,tmp_path))
    monkeypatch.setattr(cli,'NewsHTTP',lambda *a,**k:pytest.fail('Mailbox only'))
    output = tmp_path/'visible-submitted.json'
    assert cli.main(['--output',str(output),'--probe',URL,'--require-current-missing-body-failure',
        '--failure-code','article_attribution_not_verified','--candidate-embedded-layout',LAYOUT,
        '--candidate-evidence-contract','30','--enqueue-candidate']) == 0
    assert json.loads(output.read_text())['submitted']


def image_page():
    soup = inline_page('font')
    soup.select_one('.story > p').insert(0,soup.new_tag('img',src='https://example.test/photo.jpg'))
    return soup


def complete_image_refusal(queue,settings):
    complete_inline_refusal(queue,settings)
    request = submit(queue,settings,30); diagnostic.pump_candidates(queue)
    http = http_stub(queue,response(image_page()))
    job = diagnostic.claim_candidate(queue,http,settings,'ettoday')
    facts = diagnostic.fetch_candidate(http,job)
    assert facts['visible_credit_scope'] == {'verification_stage':'visible_prefix','prefix_nodes':1,
        'comment_nodes':0,'tag_counts':{'img':1},'separator_found':False}
    diagnostic.finish_candidate(queue,job,result=facts); diagnostic.pump_candidates(queue)
    return request


def test_image_is_not_a_reporter_and_does_not_hide_real_printed_reporter(queue):
    soup = image_page(); soup.select_one('.story img')['alt'] = '這不是作者'
    permit = private_ettoday_image_credit_original_permit(response(soup),queue.registry.sources['ettoday'],URL)
    assert permit['question'] == ETTODAY_IMAGE_ORIGINAL_QUESTION
    assert permit['printed_signature_sha256'] == hashlib.sha256(SIGNATURE.encode()).hexdigest()
    soup.select_one('.story font').decompose()
    soup.select_one('.story img')['alt'] = SIGNATURE
    with pytest.raises(NewsError):
        private_ettoday_image_credit_original_permit(response(soup),queue.registry.sources['ettoday'],URL)


@pytest.mark.parametrize('mutation',['hidden_reporter','person','organization','paid','clock','active','empty_image_only'])
def test_replaced_image_scope_does_not_change_literal_source_guards(queue,mutation):
    soup = image_page()
    if mutation == 'hidden_reporter': soup.select_one('.story font')['hidden'] = ''
    elif mutation == 'person': native.update_schema(soup,author={'@type':'Person','name':'另一作者'})
    elif mutation == 'organization': native.update_schema(soup,author={'@type':'Organization','name':'ETtoday新聞雲'})
    elif mutation == 'paid': native.update_schema(soup,isAccessibleForFree=False)
    elif mutation == 'clock': soup.select_one('meta[name=pubdate]')['content'] = native.MOD
    elif mutation == 'active': soup.select_one('.story font').name = 'template'
    else:
        soup.select_one('.story > p').clear()
        soup.select_one('.story > p').append(soup.new_tag('img',alt=SIGNATURE))
    with pytest.raises(NewsError):
        private_ettoday_image_credit_original_permit(response(soup),queue.registry.sources['ettoday'],URL)


@pytest.mark.parametrize('mutation',['uncertain_outcome','foreign_stage','raw','future_clock','request_hash'])
def test_image_question_requires_actual_closed_same_head_image_evidence(queue,settings,mutation):
    request = complete_image_refusal(queue,settings)
    with queue.db:
        if mutation == 'uncertain_outcome':
            queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(request['request_id'],))
        elif mutation == 'future_clock':
            queue.db.execute("UPDATE native_candidate_jobs SET completed_at_utc='2099-01-01T00:00:00+00:00' WHERE request_id=?",(request['request_id'],))
        elif mutation == 'request_hash':
            queue.db.execute("UPDATE native_candidate_jobs SET request_json=request_json||' ' WHERE request_id=?",(request['request_id'],))
        else:
            value = json.loads(queue.db.execute('SELECT result_json FROM native_candidate_jobs WHERE request_id=?',(request['request_id'],)).fetchone()[0])
            if mutation == 'raw': value['raw_or_names_or_body_retained'] = True
            else: value['visible_credit_scope']['verification_stage'] = 'source_clocks'
            queue.db.execute('UPDATE native_candidate_jobs SET result_json=? WHERE request_id=?',(json.dumps(value),request['request_id']))
    with pytest.raises(ValueError): submit(queue,settings,31)


def test_image_original_once_only_same_owner_durable_readback_no_body_admission(queue,settings):
    complete_image_refusal(queue,settings)
    previous = {r['request_id']:dict(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')}
    before = head(queue)
    request = submit(queue,settings,31); diagnostic.pump_candidates(queue)
    http = http_stub(queue,response(image_page()))
    job = diagnostic.claim_candidate(queue,http,settings,'ettoday')
    result = diagnostic.fetch_candidate(http,job)
    assert isinstance(result,PendingOriginal) and result.permit['question'] == ETTODAY_IMAGE_ORIGINAL_QUESTION
    broken = image_page(); native.update_schema(broken,author={'@type':'Person','name':'另一作者'})
    with pytest.raises(NewsError):
        retain_original(queue,job,PendingOriginal(response(broken),{},result.permit))
    diagnostic.finish_candidate(queue,job,result=result); diagnostic.pump_candidates(queue)
    stored = diagnostic.candidate_status(queue.root,request['request_id'])
    assert stored['state'] == 'observed' and stored['raw_retention_state'] == 'retained_verified'
    ref = stored['result_json']['private_original']; path = queue.root/ref['path']
    record, = pq.ParquetFile(path).read().to_pylist()
    assert record['raw_response_body'] == result.response.body and path.stat().st_mode&0o777 == 0o600
    assert head(queue) == before and http.calls == [URL]
    assert all(dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(key,)).fetchone()) == value
        for key,value in previous.items())
    assert not submit(queue,settings,31)['submitted']


def test_image_cli_preserves_resident_owner_and_closed_visible_question(queue,settings,monkeypatch,tmp_path):
    from scripts import probe_news_sources as cli
    complete_image_refusal(queue,settings)
    monkeypatch.setattr(cli,'load_config',lambda p:(settings,queue.registry,tmp_path))
    monkeypatch.setattr(cli,'NewsHTTP',lambda *a,**k:pytest.fail('Mailbox only'))
    output = tmp_path/'image-submitted.json'
    assert cli.main(['--output',str(output),'--probe',URL,'--require-current-missing-body-failure',
        '--failure-code','article_attribution_not_verified','--candidate-embedded-layout',LAYOUT,
        '--candidate-evidence-contract','31','--enqueue-candidate']) == 0
    assert json.loads(output.read_text())['submitted']


def nonempty_page(blank_lines=1):
    soup = image_page(); first = soup.select_one('.story > p')
    for _ in range(blank_lines):
        first.insert(1,soup.new_tag('br'))
    return soup


def complete_empty_line_refusal(queue,settings):
    complete_image_refusal(queue,settings)
    request = submit(queue,settings,31); diagnostic.pump_candidates(queue)
    http = http_stub(queue,response(nonempty_page()))
    job = diagnostic.claim_candidate(queue,http,settings,'ettoday')
    facts = diagnostic.fetch_candidate(http,job)
    assert facts['visible_credit_scope']['verification_stage'] == 'printed_reporter'
    assert facts['visible_credit_scope']['prefix_characters'] == 0
    diagnostic.finish_candidate(queue,job,result=facts); diagnostic.pump_candidates(queue)
    return request


@pytest.mark.parametrize('blank_lines',[1,2,4])
def test_first_nonempty_reporter_line_is_literal_not_an_empty_photo_line(queue,blank_lines):
    from bs4 import Comment
    soup = nonempty_page(blank_lines); first = soup.select_one('.story > p')
    first.insert(0,' \n\t'); first.insert(0,Comment('not a printed author'))
    permit = private_ettoday_nonempty_credit_original_permit(response(soup),queue.registry.sources['ettoday'],URL)
    assert permit['question'] == ETTODAY_NONEMPTY_ORIGINAL_QUESTION
    assert permit['printed_signature_sha256'] == hashlib.sha256(SIGNATURE.encode()).hexdigest()
    assert SIGNATURE not in json.dumps(permit,ensure_ascii=False)


@pytest.mark.parametrize('mutation',[
    'caption_before_reporter','zero_width_first_line','comment_only','alt_only','hidden_reporter',
    'foreign_person','organization','clock','paid','reprint','no_prose','no_signature_separator','too_many_nodes',
])
def test_nonempty_scope_does_not_search_past_nonempty_prose_or_infer_names(queue,mutation):
    from bs4 import Comment
    soup = nonempty_page(); first = soup.select_one('.story > p')
    if mutation in {'caption_before_reporter','zero_width_first_line'}:
        first.insert(0,'照片說明' if mutation == 'caption_before_reporter' else '\u200b')
    elif mutation == 'comment_only': soup.select_one('.story font').replace_with(Comment(SIGNATURE))
    elif mutation == 'alt_only':
        soup.select_one('.story font').decompose(); soup.select_one('.story img')['alt'] = SIGNATURE
    elif mutation == 'hidden_reporter': soup.select_one('.story font')['style'] = 'display: none'
    elif mutation == 'foreign_person': native.update_schema(soup,author={'@type':'Person','name':'另一作者'})
    elif mutation == 'organization': native.update_schema(soup,author={'@type':'Organization','name':'ETtoday新聞雲'})
    elif mutation == 'clock': soup.select_one('meta[name=pubdate]')['content'] = native.MOD
    elif mutation == 'paid': native.update_schema(soup,isAccessibleForFree=False)
    elif mutation == 'reprint': soup.select_one('.story > p:last-child').string = '本文轉載自未驗證來源'
    elif mutation == 'no_prose':
        first.clear(); first.append(soup.new_tag('br')); first.append(SIGNATURE); first.append(soup.new_tag('br'))
    elif mutation == 'no_signature_separator':
        for br in list(first.find_all('br'))[1:]: br.decompose()
    else:
        for _ in range(129): first.insert(0,soup.new_tag('br'))
    with pytest.raises(NewsError):
        private_ettoday_nonempty_credit_original_permit(response(soup),queue.registry.sources['ettoday'],URL)


@pytest.mark.parametrize('mutation',['uncertain_outcome','nonempty','raw','future_clock','request_hash','head'])
def test_nonempty_question_requires_exact_actual_closed_empty_first_line(queue,settings,mutation):
    request = complete_empty_line_refusal(queue,settings)
    with queue.db:
        if mutation == 'uncertain_outcome':
            queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(request['request_id'],))
        elif mutation == 'future_clock':
            queue.db.execute("UPDATE native_candidate_jobs SET completed_at_utc='2099-01-01T00:00:00+00:00' WHERE request_id=?",(request['request_id'],))
        elif mutation == 'request_hash':
            queue.db.execute("UPDATE native_candidate_jobs SET request_json=request_json||' ' WHERE request_id=?",(request['request_id'],))
        elif mutation == 'head': queue.db.execute('UPDATE articles SET attempts=attempts+1')
        else:
            value = json.loads(queue.db.execute('SELECT result_json FROM native_candidate_jobs WHERE request_id=?',(request['request_id'],)).fetchone()[0])
            if mutation == 'raw': value['raw_or_names_or_body_retained'] = True
            else: value['visible_credit_scope']['prefix_characters'] = 10
            queue.db.execute('UPDATE native_candidate_jobs SET result_json=? WHERE request_id=?',(json.dumps(value),request['request_id']))
    with pytest.raises(ValueError): submit(queue,settings,32)


def test_nonempty_original_same_owner_once_only_writer_readback_preserves_old_results(queue,settings):
    complete_empty_line_refusal(queue,settings)
    previous = {r['request_id']:dict(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')}
    before = head(queue)
    request = submit(queue,settings,32); diagnostic.pump_candidates(queue)
    http = http_stub(queue,response(nonempty_page()))
    job = diagnostic.claim_candidate(queue,http,settings,'ettoday')
    result = diagnostic.fetch_candidate(http,job)
    assert isinstance(result,PendingOriginal) and result.permit['question'] == ETTODAY_NONEMPTY_ORIGINAL_QUESTION
    broken = nonempty_page(); native.update_schema(broken,author={'@type':'Person','name':'另一作者'})
    with pytest.raises(NewsError):
        retain_original(queue,job,PendingOriginal(response(broken),{},result.permit))
    diagnostic.finish_candidate(queue,job,result=result); diagnostic.pump_candidates(queue)
    stored = diagnostic.candidate_status(queue.root,request['request_id'])
    assert stored['state'] == 'observed' and stored['raw_retention_state'] == 'retained_verified'
    ref = stored['result_json']['private_original']; path = queue.root/ref['path']
    record, = pq.ParquetFile(path).read().to_pylist()
    assert record['raw_response_body'] == result.response.body and path.stat().st_mode&0o777 == 0o600
    assert head(queue) == before and http.calls == [URL]
    assert all(dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(key,)).fetchone()) == value
        for key,value in previous.items())
    assert not submit(queue,settings,32,reason='A new reason is not a new observation')['submitted']


def test_nonempty_cli_never_creates_another_http_owner(queue,settings,monkeypatch,tmp_path):
    from scripts import probe_news_sources as cli
    complete_empty_line_refusal(queue,settings)
    monkeypatch.setattr(cli,'load_config',lambda p:(settings,queue.registry,tmp_path))
    monkeypatch.setattr(cli,'NewsHTTP',lambda *a,**k:pytest.fail('Mailbox only'))
    output = tmp_path/'nonempty-submitted.json'
    assert cli.main(['--output',str(output),'--probe',URL,'--require-current-missing-body-failure',
        '--failure-code','article_attribution_not_verified','--candidate-embedded-layout',LAYOUT,
        '--candidate-evidence-contract','32','--enqueue-candidate']) == 0
    assert json.loads(output.read_text())['submitted']


def canonical_author_page(relationship='subset'):
    soup = nonempty_page()
    names = ['研究記者','共同記者','另一記者'] if relationship == 'subset' else ['研究記者','共同記者']
    signature = '記者'+'、'.join(names)+'／台北報導'
    soup.select_one('.story font').string = signature
    author = [{'@type':'Person','name':name} for name in names]
    if relationship == 'subset': author = [author[0],author[-1]]
    elif relationship == 'last_of_two': author = author[-1]
    elif relationship == 'organization': author = {
        '@type':'Organization','name':'ETtoday新聞雲','url':'https://www.ettoday.net/'}
    native.update_schema(soup,author=author)
    # Reparse serialized synthetic bytes so a later mutation sees a genuine
    # Script node, not BeautifulSoup's programmatically assigned text class.
    return native.BeautifulSoup(str(soup),'lxml'), signature


@pytest.mark.parametrize('relationship,notice',[
    ('exact',None),('subset','schema_declares_ordered_subset_of_native_reporters'),
    ('last_of_two','schema_declares_last_of_two_native_reporters'),
    ('organization','schema_organization_author_not_reporter'),
])
def test_canonical_original_reuses_existing_relationship_and_whole_printed_credit(queue,monkeypatch,relationship,notice):
    monkeypatch.setattr('downloader.news_fulltext_ettoday.ettoday_visible_article',
        lambda *a,**k:pytest.fail('Private evidence is not a body parse'))
    soup, signature = canonical_author_page(relationship)
    permit = private_ettoday_canonical_credit_original_permit(response(soup),queue.registry.sources['ettoday'],URL)
    assert permit['question'] == ETTODAY_CANONICAL_ORIGINAL_QUESTION
    assert permit['printed_signature_sha256'] == hashlib.sha256(signature.encode()).hexdigest()
    assert permit['author_relationship_contract'] == LAYOUT
    assert permit['author_relationship_notices'] == ([notice] if notice else [])
    assert permit['reporters_taken_from_whole_visible_signature'] and not permit['source_values_rewritten']
    assert not any(name in json.dumps(permit,ensure_ascii=False) for name in ('研究記者','共同記者','另一記者'))
    if relationship != 'exact':
        with pytest.raises(NewsError,match='article_attribution_not_verified'):
            private_ettoday_nonempty_credit_original_permit(response(soup),queue.registry.sources['ettoday'],URL)


@pytest.mark.parametrize('mutation',[
    'foreign_person','reverse_subset','duplicate_schema_name','duplicate_visible_name','foreign_organization',
    'foreign_publisher','foreign_identity','paid','clock','hidden','nonempty_prelude','alt_only','reprint',
])
def test_canonical_scope_does_not_guess_repair_or_accept_unsafe_source_values(queue,mutation):
    soup, _ = canonical_author_page()
    if mutation == 'foreign_person': native.update_schema(soup,author={'@type':'Person','name':'來源外作者'})
    elif mutation == 'reverse_subset': native.update_schema(soup,author=[
        {'@type':'Person','name':'另一記者'},{'@type':'Person','name':'研究記者'}])
    elif mutation == 'duplicate_schema_name': native.update_schema(soup,author=[
        {'@type':'Person','name':'研究記者'},{'@type':'Person','name':'研究記者'}])
    elif mutation == 'duplicate_visible_name': soup.select_one('.story font').string = '記者研究記者、研究記者／台北報導'
    elif mutation == 'foreign_organization': native.update_schema(soup,author={'@type':'Organization','name':'其他出版單位'})
    elif mutation == 'foreign_publisher': native.update_schema(soup,publisher={'@type':'Organization','name':'其他出版單位'})
    elif mutation == 'foreign_identity': native.update_schema(soup,url=URL+'0')
    elif mutation == 'paid': native.update_schema(soup,isAccessibleForFree=False)
    elif mutation == 'clock': soup.select_one('meta[name=pubdate]')['content'] = native.MOD
    elif mutation == 'hidden': soup.select_one('.story font')['hidden'] = ''
    elif mutation == 'nonempty_prelude': soup.select_one('.story > p').insert(0,'不能略過的文字')
    elif mutation == 'alt_only':
        soup.select_one('.story img')['alt'] = SIGNATURE; soup.select_one('.story font').decompose()
    else: soup.select_one('.story > p:last-child').string = '本文轉載自其他媒體'
    with pytest.raises(NewsError):
        private_ettoday_canonical_credit_original_permit(response(soup),queue.registry.sources['ettoday'],URL)


def complete_named_author_refusal(queue,settings):
    complete_empty_line_refusal(queue,settings)
    request = submit(queue,settings,32); diagnostic.pump_candidates(queue)
    http = http_stub(queue,response(canonical_author_page()[0]))
    job = diagnostic.claim_candidate(queue,http,settings,'ettoday')
    facts = diagnostic.fetch_candidate(http,job)
    assert facts['private_original_scope_error'] == 'article_attribution_not_verified'
    assert facts['visible_credit_scope']['verification_stage'] == 'same_article_person'
    diagnostic.finish_candidate(queue,job,result=facts); diagnostic.pump_candidates(queue)
    return request


@pytest.mark.parametrize('mutation',[
    'uncertain_outcome','future_clock','request_hash','head','foreign_stage','empty_credit','raw','permit','question','status',
])
def test_canonical_question_requires_closed_same_head_actual_named_scope_refusal(queue,settings,mutation):
    request = complete_named_author_refusal(queue,settings)
    with queue.db:
        if mutation == 'uncertain_outcome':
            queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",(request['request_id'],))
        elif mutation == 'future_clock':
            queue.db.execute("UPDATE native_candidate_jobs SET completed_at_utc='2099-01-01T00:00:00+00:00' WHERE request_id=?",(request['request_id'],))
        elif mutation == 'request_hash':
            queue.db.execute("UPDATE native_candidate_jobs SET request_json=request_json||' ' WHERE request_id=?",(request['request_id'],))
        elif mutation == 'head': queue.db.execute('UPDATE articles SET attempts=attempts+1')
        else:
            value = json.loads(queue.db.execute('SELECT result_json FROM native_candidate_jobs WHERE request_id=?',(request['request_id'],)).fetchone()[0])
            if mutation == 'foreign_stage': value['visible_credit_scope']['verification_stage'] = 'source_clocks'
            elif mutation == 'empty_credit': value['visible_credit_scope']['prefix_characters'] = 0
            elif mutation == 'raw': value['raw_or_names_or_body_retained'] = True
            elif mutation == 'permit': value['private_original_permit'] = {}
            elif mutation == 'question': value['question'] = ETTODAY_IMAGE_ORIGINAL_QUESTION
            else: value['status'] = 403
            queue.db.execute('UPDATE native_candidate_jobs SET result_json=? WHERE request_id=?',(json.dumps(value),request['request_id']))
    with pytest.raises(ValueError): submit(queue,settings,33)


def test_canonical_original_same_owner_actual_bytes_recheck_readback_and_no_replay(queue,settings):
    complete_named_author_refusal(queue,settings)
    previous = {r['request_id']:dict(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')}
    before = head(queue)
    request = submit(queue,settings,33); diagnostic.pump_candidates(queue)
    http = http_stub(queue,response(canonical_author_page()[0]))
    job = diagnostic.claim_candidate(queue,http,settings,'ettoday')
    result = diagnostic.fetch_candidate(http,job)
    assert isinstance(result,PendingOriginal) and result.permit['question'] == ETTODAY_CANONICAL_ORIGINAL_QUESTION
    broken, _ = canonical_author_page(); native.update_schema(broken,author={'@type':'Person','name':'來源外作者'})
    with pytest.raises(NewsError): retain_original(queue,job,PendingOriginal(response(broken),{},result.permit))
    diagnostic.finish_candidate(queue,job,result=result); diagnostic.pump_candidates(queue)
    status = diagnostic.candidate_status(queue.root,request['request_id'])
    assert status['state'] == 'observed' and status['raw_retention_state'] == 'retained_verified'
    ref = status['result_json']['private_original']; path = queue.root/ref['path']
    record, = pq.ParquetFile(path).read().to_pylist()
    assert record['raw_response_body'] == result.response.body and path.stat().st_mode&0o777 == 0o600
    assert not record['accepted_as_article'] and not record['publish'] and not record['remote_training_export']
    assert head(queue) == before and http.calls == [URL]
    assert all(dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(key,)).fetchone()) == value
        for key,value in previous.items())
    assert not submit(queue,settings,33,reason='Changing reason must not issue another GET')['submitted']


def test_canonical_refusal_records_safe_failure_site_without_names_or_retry(queue,settings):
    complete_named_author_refusal(queue,settings)
    request = submit(queue,settings,33); diagnostic.pump_candidates(queue)
    soup, _ = canonical_author_page(); native.update_schema(soup,author={'@type':'Person','name':'來源外作者'})
    http = http_stub(queue,response(soup)); job = diagnostic.claim_candidate(queue,http,settings,'ettoday')
    facts = diagnostic.fetch_candidate(http,job)
    assert facts['private_original_scope_error'] == 'article_rights_or_scope_excluded'
    assert facts['visible_credit_scope']['verification_stage'] == 'same_article_author'
    assert any(site['function'] == '_schemas' for site in facts['failure_location']['sites'])
    assert not facts['failure_location']['source_text_or_locals_exported']
    assert '來源外作者' not in json.dumps(facts,ensure_ascii=False)
    assert 'private_original_permit' not in facts
    diagnostic.finish_candidate(queue,job,result=facts); diagnostic.pump_candidates(queue)
    assert diagnostic.candidate_status(queue.root,request['request_id'])['state'] == 'observed'
    assert not submit(queue,settings,33)['submitted'] and http.calls == [URL]


def test_canonical_cli_uses_only_resident_mailbox(queue,settings,monkeypatch,tmp_path):
    from scripts import probe_news_sources as cli
    complete_named_author_refusal(queue,settings)
    monkeypatch.setattr(cli,'load_config',lambda p:(settings,queue.registry,tmp_path))
    monkeypatch.setattr(cli,'NewsHTTP',lambda *a,**k:pytest.fail('Mailbox only'))
    output = tmp_path/'canonical-submitted.json'
    assert cli.main(['--output',str(output),'--probe',URL,'--require-current-missing-body-failure',
        '--failure-code','article_attribution_not_verified','--candidate-embedded-layout',LAYOUT,
        '--candidate-evidence-contract','33','--enqueue-candidate']) == 0
    assert json.loads(output.read_text())['submitted']

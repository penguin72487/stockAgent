"""A once-only v8 body-length question, distinct from credit/layout repairs."""
from dataclasses import replace
import json
import time

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import (
    INEWS_BODY_ORIGINAL_QUESTION, PendingOriginal, private_inews_body_original_permit,
    INEWS_SHORT_CREDIT_ORIGINAL_QUESTION, private_inews_short_credit_original_permit, retain_original,
)
from downloader.news_fulltext_candidates import candidate_evidence_allowed, candidate_response_metadata
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_inews_resident_diagnostics import head, http_stub
from test_news_setn_inews import URL, NAME, TITLE, TEXT, CLOCK, page, policy, v3_entry, v8_entry

LAYOUT = 'setn_inews_native_article_v8'
SHORT = '合成短新聞：交易量1234股，負值-5，比率2.3%。'


def short_page(text=SHORT):
    soup = page()
    body = soup.select_one('[itemprop=articleBody]')
    body.clear()
    if text:
        paragraph = soup.new_tag('p'); paragraph.string = text; body.append(paragraph)
    return soup


def response(soup=None, **changes):
    return replace(Response(URL, 200, {'content-type': 'text/html; charset=utf-8'},
        str(soup if soup is not None else short_page()).encode()), **changes)


@pytest.fixture
def owner(tmp_path):
    config = {'contract_version': 1, 'root': str(tmp_path), 'collection_purpose': 'personal_research',
        'publish': False, 'remote_training_export': False, 'providers': [v8_entry()],
        'min_free_disk_bytes': 1, 'workers': 2, 'exclusive_http_owner': False,
        'adaptive_rate': {'enabled': False}}
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    queue.add_articles([{'url': URL}])
    with queue.db:
        queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=2,due=?,last_checked_at_utc=? WHERE url=?',
            ('body_not_found', 'body_not_found', time.time()-60, '2026-01-01T00:00:00+00:00', URL))
    yield queue, config
    queue.close()


def submit(queue, config, reason='Native body length versus an empty editor; preserve source scope'):
    return diagnostic.submit_candidate(queue.root, config, queue.registry, URL, LAYOUT,
        reason=reason, evidence_contract_version=38)


@pytest.mark.parametrize('text', [SHORT, 'x'*99, ''])
def test_positive_original_scope_preserves_short_or_empty_native_editor_without_admission(text):
    permit = private_inews_body_original_permit(response(short_page(text)), policy(v8_entry()), URL)
    assert permit['question'] == INEWS_BODY_ORIGINAL_QUESTION
    assert permit['native_editor_geometry_verified'] and not permit['body_completeness_verified']
    assert permit['current_v8_editor_error'] == 'body_not_found'
    assert all(value not in json.dumps(permit, ensure_ascii=False) for value in (SHORT, NAME, TITLE, CLOCK))


@pytest.mark.parametrize('mutation', ['long', 'unknown_subtree', 'canonical', 'credit', 'clock', 'paywall', 'hidden', 'reprint'])
def test_raw_original_needs_public_literal_identity_credit_clock_and_only_the_length_failure(mutation):
    soup = short_page()
    if mutation == 'long':
        soup.select_one('p').string = TEXT
    elif mutation == 'unknown_subtree':
        soup.select_one('[itemprop=articleBody]').append(soup.new_tag('table'))
    elif mutation == 'canonical':
        soup.select_one('link')['href'] = '/news/999999'
    elif mutation == 'credit':
        soup.select_one('.author_wrap').clear()
    elif mutation == 'clock':
        soup.select_one('time').string = '2026/02/30 25:00'
    elif mutation == 'paywall':
        soup.body.append(soup.new_tag('div', attrs={'class': 'paywall'}))
    elif mutation == 'hidden':
        soup.select_one('[itemprop=articleBody]')['hidden'] = ''
    else:
        soup.select_one('p').string = '本文轉載自未驗證來源'
    with pytest.raises(NewsError):
        private_inews_body_original_permit(response(soup), policy(v8_entry()), URL)


@pytest.mark.parametrize('change', [{'status': 403}, {'status': 429}, {'body': b''},
    {'url': URL+'?view=other'}, {'headers': {'content-type': 'application/json'}}])
def test_source_refusals_and_changed_representations_cannot_authorize_raw(change):
    with pytest.raises(NewsError):
        private_inews_body_original_permit(response(**change), policy(v8_entry()), URL)


def test_question_is_v8_body_failure_only_and_its_key_cannot_be_rearmed_by_candidate_hash(owner):
    queue, config = owner
    source = queue.registry.sources['setn_inews_scope']
    assert candidate_evidence_allowed(source, source, 38)
    older = policy(v3_entry())
    assert not candidate_evidence_allowed(older, older, 38)
    assert diagnostic._failure_states(LAYOUT, 38) == {'body_not_found'}
    assert diagnostic._failure_states(v3_entry()['embedded_article_layout'], 38) == set()
    target = diagnostic._head(head(queue))
    assert diagnostic._key(target, 'a'*64, evidence_contract_version=38) == diagnostic._key(target, 'b'*64, evidence_contract_version=38)
    assert diagnostic._key(target, 'a'*64, evidence_contract_version=38) != diagnostic._key(target, 'a'*64, evidence_contract_version=24)
    with queue.db:
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified' WHERE url=?", (URL,))
    with pytest.raises(ValueError):
        submit(queue, config)


def test_same_owner_once_only_original_has_durable_receipt_without_accepting_or_requeueing(owner, monkeypatch):
    queue, config = owner
    before, owner_id = head(queue), queue.owner
    budgets = [tuple(r) for r in queue.db.execute('SELECT * FROM queue_settings')]
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('no second HTTP owner'))
    submitted = submit(queue, config)
    diagnostic.pump_candidates(queue)
    http = http_stub(queue, response())
    job = diagnostic.claim_candidate(queue, http, config, 'setn_inews_scope')
    result = diagnostic.fetch_candidate(http, job)
    assert isinstance(result, PendingOriginal) and http.calls == [URL]
    assert result.facts['native_short_body_scope_verified'] and not result.facts['accepted_as_article']
    diagnostic.finish_candidate(queue, job, result=result)
    diagnostic.pump_candidates(queue)
    state = diagnostic.candidate_status(queue.root, submitted['request_id'])
    assert state['state'] == 'observed' and state['raw_or_names_or_body_retained'], state
    reference = state['result_json']['private_original']
    stored = pq.read_table(queue.root/reference['path']).to_pylist()[0]
    assert stored['raw_response_body'] == result.response.body
    assert (queue.root/reference['receipt_path']).is_file()
    assert head(queue) == before and queue.owner == owner_id
    assert [tuple(r) for r in queue.db.execute('SELECT * FROM queue_settings')] == budgets
    assert submit(queue, config, reason='Different words cannot buy a second observation')['submitted'] is False
    assert diagnostic.claim_candidate(queue, http, config, 'setn_inews_scope') is None


def test_negative_scope_keeps_bounded_geometry_without_prose_or_automatic_second_get(owner):
    queue, config = owner
    submitted = submit(queue, config)
    diagnostic.pump_candidates(queue)
    soup = short_page(); soup.select_one('.author_wrap').clear()
    http = http_stub(queue, response(soup))
    job = diagnostic.claim_candidate(queue, http, config, 'setn_inews_scope')
    facts = diagnostic.fetch_candidate(http, job)
    assert not isinstance(facts, PendingOriginal)
    assert facts['native_short_body_scope_verified'] is False
    assert facts['question'] == INEWS_BODY_ORIGINAL_QUESTION and 'inews_native_boundary' in facts
    assert all(value not in json.dumps(facts, ensure_ascii=False) for value in (SHORT, NAME, TITLE, CLOCK))
    diagnostic.finish_candidate(queue, job, result=facts)
    assert diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'observed'
    assert submit(queue, config)['submitted'] is False and http.calls == [URL]


def test_original_writer_rechecks_changed_bytes_before_any_raw_file(owner):
    queue, config = owner
    submit(queue, config); diagnostic.pump_candidates(queue)
    http = http_stub(queue, response())
    job = diagnostic.claim_candidate(queue, http, config, 'setn_inews_scope')
    original = diagnostic.fetch_candidate(http, job)
    soup = short_page(); soup.select_one('link')['href'] = '/news/999999'
    changed = PendingOriginal(response(soup), original.facts, original.permit)
    with pytest.raises(NewsError):
        retain_original(queue, job, changed)
    assert not list(queue.root.glob('reviewed_repairs/candidate_diagnostics/originals/*'))


def credit_page(kind):
    soup = short_page()
    if kind == 'editor':
        soup.select_one('.author_wrap').contents[0].replace_with('編輯 ')
    else:
        soup.select_one('.author_wrap').clear()
        soup.select_one('[itemprop=articleBody] > p').string = '財經中心／合成人報導'
        paragraph = soup.new_tag('p'); paragraph.string = SHORT
        soup.select_one('[itemprop=articleBody]').append(paragraph)
    return soup


def submit_credit(queue, config):
    return diagnostic.submit_candidate(queue.root,config,queue.registry,URL,LAYOUT,
        reason='Actual short editor/printed credit after closed v38 reporter-only gap',evidence_contract_version=39)


def complete_short_question(queue, config, soup):
    submitted = submit(queue,config); diagnostic.pump_candidates(queue)
    http = http_stub(queue,response(soup))
    job = diagnostic.claim_candidate(queue,http,config,'setn_inews_scope')
    diagnostic.finish_candidate(queue,job,result=diagnostic.fetch_candidate(http,job))
    diagnostic.pump_candidates(queue)
    return submitted


@pytest.mark.parametrize('kind,location', [('editor','native_header_editor'),('printed','native_first_printed_signature')])
def test_short_credit_original_requires_actual_named_editor_or_printed_signature(kind,location):
    permit = private_inews_short_credit_original_permit(response(credit_page(kind)),policy(v8_entry()),URL)
    assert permit['question']==INEWS_SHORT_CREDIT_ORIGINAL_QUESTION
    assert permit['source_credit_location']==location and permit['native_editor_geometry_verified']
    assert not permit['body_completeness_verified']
    assert all(value not in json.dumps(permit,ensure_ascii=False) for value in (SHORT,NAME,TITLE,CLOCK))


@pytest.mark.parametrize('kind', ['editor','printed'])
def test_short_credit_question_follows_closed_shapes_once_without_replaying_v38(owner,kind):
    queue,config = owner; soup = credit_page(kind)
    with pytest.raises(ValueError,match='completed_inews_short_editor_or_printed_credit_proof_required'):
        submit_credit(queue,config)
    prior = complete_short_question(queue,config,soup)
    old = dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(prior['request_id'],)).fetchone())
    before = head(queue); new = submit_credit(queue,config); diagnostic.pump_candidates(queue)
    http = http_stub(queue,response(soup))
    job = diagnostic.claim_candidate(queue,http,config,'setn_inews_scope')
    result = diagnostic.fetch_candidate(http,job)
    assert isinstance(result,PendingOriginal) and result.permit['question']==INEWS_SHORT_CREDIT_ORIGINAL_QUESTION
    diagnostic.finish_candidate(queue,job,result=result)
    state = diagnostic.candidate_status(queue.root,new['request_id'])
    assert state['state']=='observed' and state['raw_or_names_or_body_retained'],state
    assert dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(prior['request_id'],)).fetchone())==old
    assert head(queue)==before and http.calls==[URL] and submit_credit(queue,config)['submitted'] is False


@pytest.mark.parametrize('change', ['ready','reserved','uncertain_outcome','changed_head','already_retained','long','empty'])
def test_short_credit_original_cannot_reset_unknown_prior_or_repeat_already_available_original(owner,change):
    queue,config = owner
    soup = short_page() if change=='already_retained' else short_page(TEXT) if change=='long' else short_page('') if change=='empty' else credit_page('editor')
    prior = complete_short_question(queue,config,soup)
    with queue.db:
        if change in {'ready','reserved','uncertain_outcome'}:
            queue.db.execute('UPDATE native_candidate_jobs SET state=? WHERE request_id=?',(change,prior['request_id']))
        if change=='changed_head':
            queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?',(URL,))
    with pytest.raises(ValueError):
        submit_credit(queue,config)

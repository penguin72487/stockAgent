"""Printed-credit originals are finite private evidence, never accepted news."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import (
    INEWS_ORIGINAL_QUESTION, INEWS_HEADER_ORIGINAL_QUESTION, PendingOriginal,
    private_inews_original_permit, private_inews_header_original_permit, retain_original,
)
from downloader.news_fulltext_candidates import candidate_evidence_allowed
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError
from test_news_inews_resident_diagnostics import LAYOUT, head, http_stub, queue, settings, submit as submit_boundary
from test_news_setn_inews import URL, INEWS_PROVIDER, TEXT, page, policy, v3_entry


SIGNATURE = '財經中心／合成人報導'


def printed_page():
    soup = page()
    soup.select_one('.author_wrap').clear()
    soup.select_one('[itemprop=articleBody] > p').string = SIGNATURE
    return soup


def response(soup=None, **changes):
    return replace(Response(URL, 200, {'content-type': 'text/html; charset=utf-8'},
        str(soup if soup is not None else printed_page()).encode()), **changes)


def complete_boundary(queue, settings, *, envelope=None):
    submitted = submit_boundary(queue, settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(queue, envelope or response())
    job = diagnostic.claim_candidate(queue, http, settings, INEWS_PROVIDER)
    facts = diagnostic.fetch_candidate(http, job)
    diagnostic.finish_candidate(queue, job, result=facts)
    diagnostic.pump_candidates(queue)
    return submitted


def submit_original(queue, settings, *, reason='Still-unanswered printed credit and editor original after completed empty-header shapes'):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, URL, LAYOUT,
        reason=reason, evidence_contract_version=25)


def test_original_scope_proves_native_identity_and_printed_signature_without_accepting_parser(monkeypatch):
    monkeypatch.setattr('downloader.news_fulltext_setn.setn_inews_native_article',
        lambda *a, **k: pytest.fail('original scope must not accept or retry failed body parser'))
    permit = private_inews_original_permit(response(), policy(v3_entry()), URL)
    assert permit['question'] == INEWS_ORIGINAL_QUESTION
    assert permit['printed_signature_sha256'] == hashlib.sha256(SIGNATURE.encode()).hexdigest()
    assert SIGNATURE not in json.dumps(permit, ensure_ascii=False)
    assert candidate_evidence_allowed(policy(v3_entry()), policy(v3_entry()), 25)


@pytest.mark.parametrize('change', [
    {'status': 403}, {'status': 429}, {'status': 500}, {'body': b''},
    {'body': b'x' * ((2 << 20)+1)}, {'url': URL+'0'},
    {'headers': {'content-type': 'application/json'}},
    {'body': b'<html><title>Just a moment</title></html>'},
    {'body': b'<html><div class="paywall">Login required</div></html>'},
])
def test_transport_and_access_refusals_cannot_authorize_originals(change):
    with pytest.raises(NewsError):
        private_inews_original_permit(response(**change), policy(v3_entry()), URL)


@pytest.mark.parametrize('selector,attribute,value', [
    ('link[rel=canonical]', 'href', 'https://www.setn.com/news/123456'),
    ('meta[property="og:url"]', 'content', URL+'0'),
    ('meta[name=author]', 'content', 'unreviewed publisher'),
    ('.leftCon', 'itemtype', 'https://schema.org/VideoObject'),
    ('[itemprop=articleBody]', 'hidden', ''),
])
def test_native_source_identity_is_positive_not_just_http200(selector, attribute, value):
    soup = printed_page(); soup.select_one(selector)[attribute] = value
    with pytest.raises(NewsError):
        private_inews_original_permit(response(soup), policy(v3_entry()), URL)


@pytest.mark.parametrize('signature', ['', '中央社', '合成人報導', '未知文章內容',
    '財經中心／', '財經中心／合成人報導 trailing prose'])
def test_a_missing_or_unrecognised_printed_credit_does_not_get_raw_retention(signature):
    soup = printed_page(); soup.select_one('[itemprop=articleBody] > p').string = signature
    with pytest.raises(NewsError):
        private_inews_original_permit(response(soup), policy(v3_entry()), URL)


@pytest.mark.parametrize('mutation', ['header_credit', 'invalid_clock', 'iframe', 'reprint'])
def test_new_question_scope_does_not_expand_to_unreviewed_or_mixed_source(mutation):
    soup = printed_page()
    if mutation == 'header_credit':
        soup.select_one('.author_wrap').string = 'another header credit'
    elif mutation == 'invalid_clock':
        soup.select_one('time').string = '2026/02/30 25:00'
    elif mutation == 'iframe':
        soup.select_one('[itemprop=articleBody]').append(soup.new_tag('iframe'))
    else:
        node = soup.new_tag('p'); node.string = '本文轉載自未驗證來源'
        soup.select_one('[itemprop=articleBody]').append(node)
    with pytest.raises(NewsError):
        private_inews_original_permit(response(soup), policy(v3_entry()), URL)


def test_completed_empty_header_shape_proof_is_required_before_submission(queue, settings):
    before = head(queue)
    with pytest.raises(ValueError, match='completed_inews_empty_header_printed_paragraph_proof_required'):
        submit_original(queue, settings)
    assert head(queue) == before and not (queue.root/'reviewed_repairs').exists()


@pytest.mark.parametrize('prior', ['ready', 'reserved', 'uncertain_outcome', 'nonempty_header', 'changed_head'])
def test_new_question_cannot_replay_inflight_unknown_or_mismatched_prior_proof(queue, settings, prior):
    completed = complete_boundary(queue, settings,
        envelope=response(page()) if prior == 'nonempty_header' else None)
    with queue.db:
        if prior in {'ready','reserved','uncertain_outcome'}:
            queue.db.execute('UPDATE native_candidate_jobs SET state=? WHERE request_id=?',
                (prior, completed['request_id']))
        elif prior == 'changed_head':
            queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (URL,))
    before = head(queue)
    with pytest.raises(ValueError):
        submit_original(queue, settings)
    assert head(queue) == before


def test_same_owner_durably_keeps_original_but_not_article_or_refilled_budget(queue, settings, monkeypatch):
    previous = complete_boundary(queue, settings)
    old_job = dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',
        (previous['request_id'],)).fetchone())
    before, owner = head(queue), queue.owner
    budgets = [tuple(r) for r in queue.db.execute('SELECT * FROM queue_settings')]
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('no second HTTP owner'))
    submitted = submit_original(queue, settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(queue, response())
    job = diagnostic.claim_candidate(queue, http, settings, INEWS_PROVIDER)
    result = diagnostic.fetch_candidate(http, job)
    assert isinstance(result, PendingOriginal) and not result.facts['accepted_as_article']
    assert result.permit['question'] == INEWS_ORIGINAL_QUESTION
    assert not list(queue.root.glob('reviewed_repairs/candidate_diagnostics/originals/*'))
    diagnostic.finish_candidate(queue, job, result=result)
    diagnostic.pump_candidates(queue)
    status = diagnostic.candidate_status(queue.root, submitted['request_id'])
    assert status['state'] == 'observed' and status['raw_or_names_or_body_retained'] is True
    reference = status['result_json']['private_original']
    path = queue.root/reference['path']
    assert path.stat().st_mode & 0o777 == 0o600
    stored = pq.read_table(path).to_pylist()[0]
    assert stored['raw_response_body'] == result.response.body
    assert stored['source_document_url'] == URL and stored['transport_redirects_json'] == '[]'
    assert stored['response_sha256'] == hashlib.sha256(stored['raw_response_body']).hexdigest()
    assert not stored['accepted_as_article'] and not stored['publish'] and not stored['remote_training_export']
    assert SIGNATURE not in json.dumps(status['result_json'], ensure_ascii=False)
    assert queue.owner == owner and head(queue) == before and http.calls == [URL]
    assert [tuple(r) for r in queue.db.execute('SELECT * FROM queue_settings')] == budgets
    assert dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',
        (previous['request_id'],)).fetchone()) == old_job
    assert submit_original(queue, settings, reason='Another reason cannot grant another original')['submitted'] is False
    assert not list(queue.root.glob('bodies/**/*.parquet'))


def test_writer_rechecks_actual_raw_scope_independently(queue, settings):
    complete_boundary(queue, settings)
    submit_original(queue, settings); diagnostic.pump_candidates(queue)
    job = diagnostic.claim_candidate(queue, http_stub(queue), settings, INEWS_PROVIDER)
    permit = private_inews_original_permit(response(), job['source'], URL)
    changed = replace(response(), body=response().body.replace(SIGNATURE.encode(), b'unknown'))
    with pytest.raises((NewsError, ValueError)):
        retain_original(queue, job, PendingOriginal(changed,
            {'accepted_as_article':False, 'raw_or_names_or_body_retained':False}, permit))
    assert not list(queue.root.glob('reviewed_repairs/candidate_diagnostics/originals/*'))


def test_original_question_key_is_distinct_and_stable_across_parser_or_reason():
    target = {'provider': INEWS_PROVIDER, 'id': 'a'*64}
    old_key = diagnostic._key(target, 'b'*64, evidence_contract_version=24)
    new_key = diagnostic._key(target, 'b'*64, evidence_contract_version=25)
    assert old_key != new_key
    assert diagnostic._key(target, 'c'*64, evidence_contract_version=25) == new_key


def header_response():
    soup = page()
    # Unknown editorial structure is private evidence, not accepted prose.
    node = soup.new_tag('div', **{'class': 'unknown-written-block'})
    node.string = TEXT
    soup.select_one('[itemprop=articleBody]').append(node)
    return response(soup)


def submit_header(queue, settings, *, reason='Named header and unresolved editor structure after completed geometry'):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, URL, LAYOUT,
        reason=reason, evidence_contract_version=26)


@pytest.mark.parametrize('mutation', ['empty', 'unnamed', 'foreign_profile', 'hidden', 'nested', 'reprint', 'paid', 'invalid_clock'])
def test_named_header_original_requires_positive_source_credit(mutation):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(header_response().body, 'lxml')
    byline, link = soup.select_one('.author_wrap'), soup.select_one('a[itemprop=author]')
    if mutation == 'empty':
        byline.clear()
    elif mutation == 'unnamed':
        link.string = '綜合'
    elif mutation == 'foreign_profile':
        link['href'] = 'https://example.test/author/person'
    elif mutation == 'hidden':
        link['hidden'] = ''
    elif mutation == 'nested':
        link.append(soup.new_tag('span'))
    elif mutation == 'reprint':
        soup.select_one('[itemprop=articleBody] > p').string = '本文轉載自未驗證來源'
    elif mutation == 'paid':
        soup.select_one('[itemprop=articleBody]')['data-paywall'] = 'true'
    else:
        soup.select_one('time').string = '2026/02/30 25:00'
    with pytest.raises(NewsError):
        private_inews_header_original_permit(response(soup), policy(v3_entry()), URL)


def test_named_header_original_reuses_owner_preserves_head_and_closed_geometry(queue, settings):
    prior = complete_boundary(queue, settings, envelope=header_response())
    old_job = dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',
        (prior['request_id'],)).fetchone())
    before, owner = head(queue), queue.owner
    submitted = submit_header(queue, settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(queue, header_response())
    job = diagnostic.claim_candidate(queue, http, settings, INEWS_PROVIDER)
    result = diagnostic.fetch_candidate(http, job)
    assert isinstance(result, PendingOriginal) and result.permit['question'] == INEWS_HEADER_ORIGINAL_QUESTION
    diagnostic.finish_candidate(queue, job, result=result)
    status = diagnostic.candidate_status(queue.root, submitted['request_id'])
    assert status['state'] == 'observed' and status['raw_or_names_or_body_retained']
    raw = pq.read_table(queue.root/status['result_json']['private_original']['path']).to_pylist()[0]
    assert raw['raw_response_body'] == header_response().body
    assert not raw['accepted_as_article'] and not raw['publish'] and not raw['remote_training_export']
    assert head(queue) == before and queue.owner == owner and http.calls == [URL]
    assert dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',
        (prior['request_id'],)).fetchone()) == old_job
    assert not submit_header(queue, settings, reason='A changed reason cannot replay this question')['submitted']
    target = {'provider': INEWS_PROVIDER, 'id': 'a'*64}
    assert diagnostic._key(target, 'b'*64, evidence_contract_version=26) == diagnostic._key(target, 'c'*64, evidence_contract_version=26)
    assert len({diagnostic._key(target, 'b'*64, evidence_contract_version=v) for v in (24,25,26)}) == 3


@pytest.mark.parametrize('prior', ['missing', 'ready', 'reserved', 'uncertain_outcome', 'empty_credit', 'changed_head'])
def test_named_header_question_cannot_replay_unknown_or_inapplicable_geometry(queue, settings, prior):
    if prior != 'missing':
        completed = complete_boundary(queue, settings,
            envelope=response() if prior == 'empty_credit' else header_response())
        with queue.db:
            if prior in {'ready','reserved','uncertain_outcome'}:
                queue.db.execute('UPDATE native_candidate_jobs SET state=? WHERE request_id=?',
                    (prior, completed['request_id']))
            elif prior == 'changed_head':
                queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (URL,))
    before = head(queue)
    with pytest.raises(ValueError, match='completed_inews_named_header_editor_proof_required'):
        submit_header(queue, settings)
    assert head(queue) == before


@pytest.mark.parametrize('corruption', [None, 'data_fingerprint', 'authorization_fingerprint'])
def test_completed_v4_geometry_is_not_repeated_after_exact_v5_credit_promotion(queue, settings, corruption):
    from downloader.news_fulltext_setn import INEWS_V4_HTTP_BINDING, INEWS_V5_HTTP_BINDING
    completed = complete_boundary(queue, settings, envelope=header_response())
    old_job = dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',
        (completed['request_id'],)).fetchone())
    request = json.loads(old_job['request_json'])
    request['source'].update(data_fingerprint=INEWS_V4_HTTP_BINDING[0],
        authorization_fingerprint=INEWS_V4_HTTP_BINDING[1])
    if corruption:
        request['source'][corruption] = 'a'*64
    encoded = diagnostic._bytes(request)
    with queue.db:
        queue.db.execute('UPDATE native_candidate_jobs SET request_json=?,request_id=? WHERE request_id=?',
            (encoded.decode(), hashlib.sha256(encoded).hexdigest(), completed['request_id']))
    source = SimpleNamespace(provider=INEWS_PROVIDER, data_fingerprint=INEWS_V5_HTTP_BINDING[0],
        authorization_fingerprint=INEWS_V5_HTTP_BINDING[1],
        http_capacity_source_fingerprint=INEWS_V5_HTTP_BINDING[2],
        http_capacity_authorization_fingerprint=INEWS_V5_HTTP_BINDING[3])
    identity = dict(request['source'], data_fingerprint=source.data_fingerprint,
        authorization_fingerprint=source.authorization_fingerprint, parser_fingerprint='b'*64)
    if corruption:
        with pytest.raises(ValueError):
            diagnostic._prior_inews_printed_credit(queue.db, source, identity, URL, header_credit=True)
    else:
        diagnostic._prior_inews_printed_credit(queue.db, source, identity, URL, header_credit=True)

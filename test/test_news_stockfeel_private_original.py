"""A missing title/byline witness is private evidence, not body acceptance."""
from dataclasses import replace
import hashlib
import json
import stat

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import (
    PendingOriginal, STOCKFEEL_ORIGINAL_QUESTION, private_stockfeel_original_permit,
)
from downloader.news_fulltext_candidates import candidate_response_metadata
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from test_news_candidate_diagnostics import http_stub
from test_news_fulltext import config
from test_news_stockfeel_original import AUTHOR, CANONICAL, PROFILE, TITLE, URL, html, settings
from test_news_stockfeel_boundary import LAYOUT, queue


def response(html, **changes):
    return replace(Response(URL, 200, {'content-type': 'text/html'}, html.encode()), **changes)


def source(settings):
    return PolicyRegistry(settings, base='.').sources['stockfeel']


@pytest.mark.parametrize('changed', ['title', 'type', 'byline'])
def test_independent_original_scope_does_not_require_the_failing_equality(settings, html, changed):
    if changed == 'title':
        html = html.replace('<title>'+TITLE+' - StockFeel 股感</title>', '<title>不同原始標題</title>')
    elif changed == 'type':
        html = html.replace('content="article"', 'content="website"')
    else:
        html = html.replace('name="author"', 'name="unknown-author"')
    current = source(settings)
    facts = candidate_response_metadata(response(html), current, current,
        evidence_contract_version=15, requested_url=URL)
    assert facts['current_native_error'] in {'article_identity_not_verified', 'article_attribution_not_verified'}
    permit = facts['private_original_permit']
    assert permit['question'] == STOCKFEEL_ORIGINAL_QUESTION
    assert permit['response_sha256'] == hashlib.sha256(html.encode()).hexdigest()
    assert not facts['accepted_as_article'] and not facts['raw_or_names_or_body_retained']
    assert AUTHOR not in json.dumps(facts, ensure_ascii=False)


@pytest.mark.parametrize('mutation', [
    {'status': 403}, {'status': 429}, {'headers': {'content-type': 'application/json'}},
    {'url': URL + 'other/'}, {'body': b'x' * ((2 << 20) + 1)},
])
def test_private_witness_is_not_a_transport_bypass(settings, html, mutation):
    with pytest.raises(NewsError):
        private_stockfeel_original_permit(response(html, **mutation), source(settings), URL)


@pytest.mark.parametrize('old,new', [
    ('<title>'+TITLE+' - StockFeel 股感</title>', '<title>Access denied</title>'),
    ('rel="canonical"', 'rel="other"'),
    ('<h1>', '<h1 hidden>'),
    ('<span>文章來源</span>', '<span>未知來源</span>'),
    (PROFILE, 'https://example.invalid/author/news'),
    ('data-aid="427"', 'data-aid="999"'),
    ('visibility: hidden;', 'display: none;'),
    ('</table>', '</table><iframe src="/not-fetched"></iframe>'),
    ('</table>', '</table><form></form>'),
    ('</table>', '</table><div data-paywall="true"></div>'),
    ('</table>', '</table><p>本文轉載自未核實來源</p>'),
    ('2026-10-15T07:07:51+00:00', '2026-10-15T07:07:51'),
    ('2026-10-15T07:07:55+00:00', '2026-10-14T07:07:55+00:00'),
    ('© Stockfeel. All rights reserved', 'unreviewed notice'),
])
def test_independent_native_scope_still_fails_closed(settings, html, old, new):
    with pytest.raises(NewsError):
        private_stockfeel_original_permit(response(html.replace(old, new)), source(settings), URL)


def eligible(queue):
    with queue.db:
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',"
            "error_code='article_identity_not_verified' WHERE url=?", (URL,))


def submit(queue, settings, **changes):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, URL, LAYOUT,
        reason=changes.pop('reason', 'Retain the unaccepted native title/byline original once for offline diagnosis'),
        evidence_contract_version=15, **changes)


def test_owner_only_durable_original_does_not_mutate_heads_or_refill_budgets(queue, settings, html, monkeypatch):
    eligible(queue)
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    budgets = [tuple(row) for row in queue.db.execute('SELECT * FROM queue_settings')]
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('reuse the canonical HTTP owner'))
    submitted = submit(queue, settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(queue, response=response(html.replace('content="article"', 'content="website"')))
    job = diagnostic.claim_candidate(queue, http, settings, 'stockfeel')
    assert job and diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'reserved'
    result = diagnostic.fetch_candidate(http, job)
    assert isinstance(result, PendingOriginal) and not result.facts['accepted_as_article']
    assert not list(queue.root.glob('candidate_diagnostics/originals/*'))
    diagnostic.finish_candidate(queue, job, result=result)
    status = diagnostic.candidate_status(queue.root, submitted['request_id'])
    assert status['state'] == 'observed' and status['raw_or_names_or_body_retained'] is True
    row = queue.db.execute('SELECT result_json FROM native_candidate_jobs WHERE request_id=?',
        (submitted['request_id'],)).fetchone()
    reference = json.loads(row[0])['private_original']
    path = queue.root / reference['path']
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    stored = pq.read_table(path).to_pylist()[0]
    assert stored['raw_response_body'] == result.response.body
    assert not stored['accepted_as_article'] and not stored['publish'] and not stored['remote_training_export']
    assert stored['response_sha256'] == hashlib.sha256(stored['raw_response_body']).hexdigest()
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM queue_settings')] == budgets
    assert not list(queue.root.glob('bodies/**/*.parquet'))
    assert not submit(queue, settings, reason='A new reason cannot buy another request')['submitted']
    assert diagnostic.claim_candidate(queue, http, settings, 'stockfeel') is None and http.calls == [URL]


@pytest.mark.parametrize('state', ['complete', 'pending', 'paywall_or_login', 'awaiting_authorization',
    'http_403', 'http_429', 'robots_denied', 'body_not_found', 'article_rights_or_scope_excluded'])
def test_only_unfinished_identity_or_byline_heads_are_eligible(queue, settings, state):
    with queue.db:
        queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?', (state, state, URL))
    with pytest.raises(ValueError):
        submit(queue, settings)


def test_old_metadata_key_is_preserved_and_new_original_key_never_depends_on_capture_generation():
    target = {'provider': 'stockfeel', 'id': 'a' * 64}
    old = diagnostic._key(target, 'b' * 64, evidence_contract_version=14)
    new = diagnostic._key(target, 'b' * 64, evidence_contract_version=15)
    assert new != old
    assert new == diagnostic._key(target, 'c' * 64, evidence_contract_version=15)
    assert old == diagnostic._key(target, 'c' * 64, evidence_contract_version=14)


def test_a_reserved_or_unknown_prior_question_prevents_a_new_original(queue, settings):
    eligible(queue)
    from test_news_stockfeel_boundary import submit as submit_old
    submit_old(queue, settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(queue)
    assert diagnostic.claim_candidate(queue, http, settings, 'stockfeel')
    with pytest.raises(ValueError, match='prior_observation_not_terminal'):
        submit(queue, settings)


def test_worker_parses_one_dom_not_one_per_equality(settings, html, monkeypatch):
    from downloader import news_fulltext_policy as policy
    original, calls = policy.BeautifulSoup, []
    def counted(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(policy, 'BeautifulSoup', counted)
    current = source(settings)
    candidate_response_metadata(response(html), current, current, evidence_contract_version=15, requested_url=URL)
    assert calls == [1]


def test_refused_private_original_keeps_bounded_gate_location_without_raw_or_second_dom(queue, settings, html, monkeypatch):
    from downloader import news_fulltext_policy as policy
    eligible(queue)
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    submitted = submit(queue, settings)
    diagnostic.pump_candidates(queue)
    doc = html.replace('</table>', '</table><iframe src="https://unrequested.invalid/private-secret"></iframe>')
    http = http_stub(queue, response=response(doc))
    job = diagnostic.claim_candidate(queue, http, settings, 'stockfeel')
    original, calls = policy.BeautifulSoup, []
    def counted(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(policy, 'BeautifulSoup', counted)
    result = diagnostic.fetch_candidate(http, job)
    assert isinstance(result, dict) and not isinstance(result, PendingOriginal)
    diagnostic.finish_candidate(queue, job, result=result)
    status = diagnostic.candidate_status(queue.root, submitted['request_id'])
    facts = status['result_json']
    assert status['state'] == 'observed' and status['raw_or_names_or_body_retained'] is False
    assert facts['private_original_scope_error'] == 'private_stockfeel_original_scope_not_verified'
    assert facts['current_native_error'] == 'paywall_or_login'
    assert facts['diagnostic_representation_contract_version'] == 2
    assert facts['stockfeel_native_boundary']['unreviewed_tag_counts'] == {'iframe': 1}
    assert 'private_original_permit' not in facts and 'private_original' not in facts
    location = facts['private_original_scope_failure_location']
    assert location['contract_version'] == 2 and not location['source_text_or_locals_exported']
    assert any(site['module'] == 'news_fulltext_candidate_originals.py'
        and site['function'] == '_stockfeel_original_permit' and site['line'] > 0 for site in location['sites'])
    assert all(set(site) == {'module', 'function', 'line'} for site in location['sites'])
    assert len(location['sites']) <= 8
    encoded = json.dumps(facts, ensure_ascii=False)
    assert all(value not in encoded for value in (AUTHOR, TITLE, 'unrequested.invalid', 'private-secret', str(queue.root)))
    assert calls == [1] and http.calls == [URL]
    assert not list(queue.root.glob('reviewed_repairs/candidate_diagnostics/originals/*'))
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
    assert not submit(queue, settings, reason='No repeat after the improved error receipt')['submitted']


def test_untrusted_exception_message_and_attributes_do_not_enter_error_receipt(queue, settings):
    eligible(queue)
    submitted = submit(queue, settings)
    diagnostic.pump_candidates(queue)
    job = diagnostic.claim_candidate(queue, http_stub(queue), settings, 'stockfeel')
    error = NewsError('unsafe/error/token-secret')
    error.response_body = 'SOURCE BODY SECRET'
    error.url = 'https://secret.invalid/?api_key=do-not-copy'
    diagnostic.finish_candidate(queue, job, error=error)
    facts = diagnostic.candidate_status(queue.root, submitted['request_id'])['result_json']
    assert facts['error'] == 'unclassified_worker_error'
    assert facts['failure_location']['sites'] == []
    assert facts['failure_location']['source_text_or_locals_exported'] is False
    assert all(word not in json.dumps(facts) for word in ('token-secret', 'SOURCE BODY', 'secret.invalid', 'api_key'))


@pytest.mark.parametrize('mutation,expected', [
    ('canonical', 'article_identity_not_verified'),
    ('unknown-source', 'article_rights_or_scope_excluded'),
])
def test_public_refusal_metadata_never_becomes_original_permission(settings, html, monkeypatch, mutation, expected):
    from downloader import news_fulltext_policy as policy
    original, calls = policy.BeautifulSoup, []
    def counted(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(policy, 'BeautifulSoup', counted)
    changed = (html.replace(CANONICAL, 'https://www.stockfeel.com.tw/current-slug/')
        if mutation == 'canonical' else html.replace('data-aid="427"', 'data-aid="368"'))
    current = source(settings)
    facts = candidate_response_metadata(response(changed), current, current,
        evidence_contract_version=15, requested_url=URL)
    assert calls == [1] and facts['current_native_error'] == expected
    assert facts['private_original_scope_error'] == 'private_stockfeel_original_scope_not_verified'
    assert 'private_original_permit' not in facts and not facts['raw_or_names_or_body_retained']
    assert not facts['accepted_as_article'] and not facts['identity_or_alias_accepted']
    if mutation == 'canonical':
        identity = facts['stockfeel_native_boundary']['url_identity']
        assert identity['canonical']['declarations'][0]['literal_same_origin_article_url'].endswith('/current-slug/')
        assert not identity['canonical']['declarations'][0]['matches_requested_page_key']
    else:
        assert all(row['data_aid'] == '368' for row in facts['stockfeel_native_boundary']['credits'])
    with pytest.raises(NewsError):
        private_stockfeel_original_permit(response(changed), current, URL)


@pytest.mark.parametrize('mutation', ['challenge', '403', '429'])
def test_original_metadata_fallback_cannot_cross_source_refusal(settings, html, mutation):
    current = source(settings)
    reply = (response(html.replace('<title>'+TITLE+' - StockFeel 股感</title>', '<title>Access denied</title>'))
        if mutation == 'challenge' else response(html, status=int(mutation)))
    with pytest.raises(NewsError):
        candidate_response_metadata(reply, current, current, evidence_contract_version=15, requested_url=URL)


def test_owner_rechecks_actual_raw_even_with_forged_worker_permit(queue, settings, html):
    eligible(queue)
    request = submit(queue, settings)
    diagnostic.pump_candidates(queue)
    job = diagnostic.claim_candidate(queue, http_stub(queue), settings, 'stockfeel')
    current = source(settings)
    valid = response(html)
    permit = private_stockfeel_original_permit(valid, current, URL)
    invalid = response(html.replace('</table>', '</table><iframe src="/never-fetched"></iframe>'))
    forged_permit = dict(permit, response_sha256=hashlib.sha256(invalid.body).hexdigest())
    facts = {'accepted_as_article': False, 'raw_or_names_or_body_retained': False}
    diagnostic.finish_candidate(queue, job, result=PendingOriginal(invalid, facts, forged_permit))
    status = diagnostic.candidate_status(queue.root, request['request_id'])
    assert status['state'] == 'observed_error'
    assert status['result_json']['error'] == 'private_stockfeel_original_scope_not_verified'
    assert not list(queue.root.glob('reviewed_repairs/candidate_diagnostics/originals/*'))
    assert not list(queue.root.glob('bodies/**/*.parquet'))
    assert not submit(queue, settings)['submitted']


def test_unknown_retention_does_not_allow_a_restart_retry(queue, settings, html, monkeypatch):
    from downloader import news_fulltext_candidate_originals as originals
    eligible(queue)
    submitted = submit(queue, settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(queue, response=response(html))
    job = diagnostic.claim_candidate(queue, http, settings, 'stockfeel')
    result = diagnostic.fetch_candidate(http, job)
    def failed_write(*args, **kwargs):
        raise OSError('synthetic disk failure')
    monkeypatch.setattr(originals, 'retain_original', failed_write)
    monkeypatch.setattr(diagnostic, 'retain_original', failed_write)
    diagnostic.finish_candidate(queue, job, result=result)
    status = diagnostic.candidate_status(queue.root, submitted['request_id'])
    assert status['state'] == 'observed_error' and status['raw_or_names_or_body_retained'] is None
    assert not submit(queue, settings)['submitted'] and http.calls == [URL]


def test_completed_old_question_remains_terminal_and_is_not_rearmed(queue, settings, html):
    eligible(queue)
    from test_news_stockfeel_boundary import submit as submit_old
    old = submit_old(queue, settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(queue, response=response(html))
    job = diagnostic.claim_candidate(queue, http, settings, 'stockfeel')
    diagnostic.finish_candidate(queue, job, result=diagnostic.fetch_candidate(http, job))
    before = tuple(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',
        (old['request_id'],)).fetchone())
    new = submit(queue, settings)
    diagnostic.pump_candidates(queue)
    job = diagnostic.claim_candidate(queue, http, settings, 'stockfeel')
    diagnostic.finish_candidate(queue, job, result=diagnostic.fetch_candidate(http, job))
    assert new['request_id'] != old['request_id'] and http.calls == [URL, URL]
    assert not submit_old(queue, settings)['submitted']
    assert tuple(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',
        (old['request_id'],)).fetchone()) == before


@pytest.mark.parametrize('changes', [
    {'allowed': False}, {'acquisition_mode': 'metadata_only'}, {'known_url_only': False},
    {'authorization': {'kind': 'api'}}, {'domains': ('other.example',)},
])
def test_no_new_authorization_or_source_scope(settings, html, changes):
    with pytest.raises(NewsError):
        private_stockfeel_original_permit(response(html), replace(source(settings), **changes), URL)

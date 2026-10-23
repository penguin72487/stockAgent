"""Unfinished native failures: finite evidence, not another article downloader."""
from dataclasses import replace
import hashlib
import json
import stat
import time

from bs4 import BeautifulSoup
import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import PendingOriginal
from downloader.news_fulltext_candidates import candidate_evidence_allowed, candidate_response_metadata
from downloader.news_fulltext_http import RedirectHop, Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_taiwannews import (
    TAIWANNEWS_ORIGINAL_QUESTION, TAIWANNEWS_V2_LAYOUT,
    private_taiwannews_original_permit, taiwannews_evidence_source,
    private_taiwannews_inline_original_permit, TAIWANNEWS_INLINE_ORIGINAL_QUESTION,
)
from test_news_candidate_diagnostics import http_stub
from test_news_taiwannews_original import CLOCK, PARAGRAPHS, TITLE, URL, page, production_settings


@pytest.fixture
def settings(tmp_path):
    result, _ = production_settings(2)
    result.update(root=str(tmp_path), min_free_disk_bytes=1, exclusive_http_owner=False)
    result['adaptive_rate'] = {'enabled': False}
    return result


@pytest.fixture
def queue(settings, tmp_path):
    owner = NewsQueue(tmp_path, PolicyRegistry(settings, base='.'))
    yield owner
    owner.close()


def current(settings):
    return PolicyRegistry(settings, base='.').sources['taiwannews']


def response(raw=None, **changes):
    return replace(Response(URL, 200, {'content-type': 'text/html'}, page() if raw is None else raw), **changes)


def inspect(settings, raw=None, **changes):
    source = current(settings)
    return candidate_response_metadata(response(raw, **changes), source, source,
        evidence_contract_version=16, requested_url=URL)


def failed(queue, url=URL, **changes):
    values = dict(state='article_title_not_verified', error_code='article_title_not_verified',
        attempts=4, due=time.time()-60, last_checked_at_utc='2026-10-01T00:00:00+00:00')
    values.update(changes)
    with queue.db:
        queue.add_articles([{'url': url}])
        queue.db.execute('UPDATE articles SET '+','.join(key+'=?' for key in values)+' WHERE url=?',
            (*values.values(), url))


def submit(queue, settings, url=URL, **changes):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, url, TAIWANNEWS_V2_LAYOUT,
        reason=changes.pop('reason', 'Inspect only an unresolved native failure; no accepted body or alias'),
        evidence_contract_version=16, **changes)


def test_only_exact_current_scope_and_fixed_question(settings):
    source = current(settings)
    assert taiwannews_evidence_source(source) and candidate_evidence_allowed(source, source, 16)
    assert not candidate_evidence_allowed(source, source, 15)
    for change in ({'data_fingerprint': 'f'*64}, {'authorization_fingerprint': 'f'*64},
                   {'http_capacity_source_fingerprint': 'f'*64}, {'known_url_only': False}):
        changed = replace(source, **change)
        assert not candidate_evidence_allowed(changed, changed, 16)
    target = {'provider': 'taiwannews', 'id': hashlib.sha256(URL.encode()).hexdigest()}
    assert diagnostic._key(target, 'old', evidence_contract_version=16) == diagnostic._key(
        target, 'new-capture-cannot-rearm', evidence_contract_version=16)


def test_title_equality_failure_is_private_evidence_not_article_acceptance(settings):
    raw = page().replace(('<title>'+TITLE).encode(), '<title>不同的原始標題'.encode())
    result = inspect(settings, raw)
    assert result['current_native_error'] == 'article_title_not_verified'
    permit = result['private_original_permit']
    assert permit['question'] == TAIWANNEWS_ORIGINAL_QUESTION
    assert permit['response_sha256'] == hashlib.sha256(raw).hexdigest()
    assert not result['accepted_as_article'] and not result['raw_or_names_or_body_retained']
    with pytest.raises(NewsError, match='article_title_not_verified'):
        extract_article(raw, current(settings), page_url=URL)
    serialized = json.dumps(result, ensure_ascii=False)
    assert '合成編輯組' not in serialized and PARAGRAPHS[0] not in serialized
    assert result['taiwannews_native_boundary']['body_text_exported'] is False
    assert len(serialized.encode()) < 60000


@pytest.mark.parametrize('mutation', [
    {'status': 403}, {'status': 429}, {'headers': {'content-type': 'application/json'}},
    {'url': URL+'?other=1'}, {'url': URL.replace('1234567', '1234568')},
    {'body': b'x'*((2 << 20)+1)}, {'body': b'\x00'+page()},
])
def test_transport_and_public_document_are_not_bypassed(settings, mutation):
    with pytest.raises(NewsError):
        private_taiwannews_original_permit(response(**mutation), current(settings), URL)


@pytest.mark.parametrize('old,new', [
    (b'/zh/news/1234567', b'/zh/news/7654321'),
    ('台灣英文新聞－編輯'.encode(), '中央社'.encode()),
    (b'/zh/journalist/20', b'/zh/not-a-credit/20'),
    (b'<h1 ', b'<h1 hidden '),
    (b'<div class="editor app">', b'<div class="editor app"><form></form>'),
    (b'<div class="editor app">', b'<div class="editor app"><iframe></iframe>'),
    (b'<div class="editor app">', '<div class="editor app"><p>中央社提供</p>'.encode()),
    ((CLOCK+'</p>').encode(), b'unknown-clock</p>'),
    (b'<title>', b'<title>Access denied '),
])
def test_independent_writer_scope_still_refuses_unverified_originals(settings, old, new):
    with pytest.raises(NewsError):
        private_taiwannews_original_permit(response(page().replace(old, new)), current(settings), URL)


@pytest.mark.parametrize('status,location,hop_target', [
    (302, URL, URL), (301, URL.replace('1234567', '7654321'), URL),
    (301, URL, URL.replace('1234567', '7654321')),
])
def test_redirect_must_be_one_actual_same_ID_301(settings, status, location, hop_target):
    requested = URL.replace('/zh/', '/ch/')
    hop = RedirectHop(requested, status, location, hop_target, '2026-10-01T00:00:00+00:00', 'a'*64, 1)
    with pytest.raises(NewsError):
        private_taiwannews_original_permit(response(redirects=(hop,)), current(settings), requested)
    with pytest.raises(NewsError):
        private_taiwannews_original_permit(response(), current(settings), requested)


@pytest.mark.parametrize('state', ['complete', 'pending', 'http_403', 'http_429', 'paywall_or_login'])
def test_finished_or_unrelated_heads_cannot_be_rearmed(queue, settings, state):
    failed(queue, state=state, error_code=state)
    with pytest.raises(ValueError):
        submit(queue, settings)
    assert queue.db.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='native_candidate_jobs'").fetchone()[0] == 0


@pytest.mark.parametrize('changes', [{'due': time.time()+3600}, {'last_checked_at_utc': None}])
def test_future_or_unchecked_heads_are_not_eligible(queue, settings, changes):
    failed(queue, **changes)
    with pytest.raises(ValueError):
        submit(queue, settings)


@pytest.mark.parametrize('legacy', [False, True])
def test_owner_retains_once_without_head_budget_alias_or_body_mutation(queue, settings, monkeypatch, legacy):
    requested = URL.replace('/zh/', '/ch/') if legacy else URL
    failed(queue, requested)
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (requested,)).fetchone())
    budgets = [tuple(row) for row in queue.db.execute('SELECT * FROM queue_settings')]
    submitted = submit(queue, settings, requested)
    diagnostic.pump_candidates(queue)
    hops = (RedirectHop(requested, 301, '/zh/news/1234567', URL,
        '2026-10-01T00:00:00+00:00', 'a'*64, 1),) if legacy else ()
    raw = page().replace(('<title>'+TITLE).encode(), '<title>不同的原始標題'.encode())
    http = http_stub(queue, response=response(raw, redirects=hops))
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('must reuse canonical resident HTTP owner'))
    job = diagnostic.claim_candidate(queue, http, settings, 'taiwannews')
    assert job is not None
    result = diagnostic.fetch_candidate(http, job)
    assert isinstance(result, PendingOriginal) and 'private_original_permit' not in result.facts
    diagnostic.finish_candidate(queue, job, result=result)
    status = diagnostic.candidate_status(queue.root, submitted['request_id'])
    assert status['state'] == 'observed' and status['raw_or_names_or_body_retained'] is True
    record = json.loads(queue.db.execute('SELECT result_json FROM native_candidate_jobs WHERE request_id=?',
        (submitted['request_id'],)).fetchone()[0])
    path = queue.root / record['private_original']['path']
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    stored, = pq.read_table(path).to_pylist()
    assert stored['raw_response_body'] == raw and stored['response_sha256'] == hashlib.sha256(raw).hexdigest()
    assert stored['request_url'] == requested and stored['source_document_url'] == URL
    assert not stored['accepted_as_article'] and not stored['publish'] and not stored['remote_training_export']
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (requested,)).fetchone()) == before
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM queue_settings')] == budgets
    assert not list(queue.root.glob('bodies/**/*.parquet'))
    assert not submit(queue, settings, requested, reason='changed reason cannot repeat')['submitted']
    assert diagnostic.claim_candidate(queue, http, settings, 'taiwannews') is None
    assert http.calls == [requested]


def test_scope_refusal_keeps_finite_metadata_without_names_or_body(queue, settings):
    failed(queue)
    submitted = submit(queue, settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(queue, response=response(page(label='中央社')))
    job = diagnostic.claim_candidate(queue, http, settings, 'taiwannews')
    result = diagnostic.fetch_candidate(http, job)
    assert not isinstance(result, PendingOriginal)
    assert result['private_original_scope_error'] == 'article_rights_or_scope_excluded'
    assert result['private_original_scope_failure_location']['sites']
    diagnostic.finish_candidate(queue, job, result=result)
    status = diagnostic.candidate_status(queue.root, submitted['request_id'])
    assert status['state'] == 'observed' and not status['raw_or_names_or_body_retained']
    assert not list(queue.root.glob('candidate_diagnostics/originals/**/*.parquet'))
    assert '合成編輯組' not in json.dumps(result, ensure_ascii=False)


def test_bounded_declared_metadata_omits_unbounded_text(settings):
    raw = page().replace(('<title>'+TITLE).encode(), ('<title>'+'x'*100000).encode())
    result = inspect(settings, raw)
    title = result['taiwannews_native_boundary']['declarations']['document_title']['values'][0]
    assert title['value_omitted'] and len(title['sha256']) == 64 and 'value' not in title
    assert len(json.dumps(result, ensure_ascii=False).encode()) < 60000


def test_forged_worker_permit_is_rechecked_before_raw_write(queue, settings):
    failed(queue)
    submitted = submit(queue, settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(queue, response=response())
    job = diagnostic.claim_candidate(queue, http, settings, 'taiwannews')
    result = diagnostic.fetch_candidate(http, job)
    assert isinstance(result, PendingOriginal)
    forged = replace(result, response=response(page(label='中央社')))
    diagnostic.finish_candidate(queue, job, result=forged)
    status = diagnostic.candidate_status(queue.root, submitted['request_id'])
    assert status['state'] == 'observed_error' and not status.get('private_original')
    assert not list(queue.root.glob('candidate_diagnostics/originals/**/*.parquet'))


def inline_page(prefix='台灣英文新聞－編輯'):
    document = BeautifulSoup(page(), 'lxml')
    document.select_one('h1 ~ p.text-right.text-subtle-regular').string = prefix+' '+CLOCK
    document.select_one('div.flex-1').parent.extract()
    return document.encode()


def submit_inline(queue, settings, requested=URL, **changes):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, requested, TAIWANNEWS_V2_LAYOUT,
        reason=changes.pop('reason', 'New literal legacy inline-editor scope question; reuse completed v16 metadata'),
        evidence_contract_version=17, **changes)


def previous_inline_observation(queue, settings, *, prefix='台灣英文新聞－編輯', requested=URL):
    failed(queue, requested)
    submitted = submit(queue, settings, requested)
    diagnostic.pump_candidates(queue)
    hops = () if requested == URL else (RedirectHop(requested, 301, URL, URL,
        '2026-10-01T00:00:00+00:00', 'a'*64, 1),)
    http = http_stub(queue, response=response(inline_page(prefix), redirects=hops))
    job = diagnostic.claim_candidate(queue, http, settings, 'taiwannews')
    result = diagnostic.fetch_candidate(http, job)
    assert not isinstance(result, PendingOriginal) and result['current_native_error'] == 'article_title_not_verified'
    diagnostic.finish_candidate(queue, job, result=result)
    return submitted


def test_legacy_scope_is_a_new_fixed_question_not_a_rearmed_title_observation(settings):
    source = current(settings)
    assert candidate_evidence_allowed(source, source, 17)
    target = {'provider': 'taiwannews', 'id': hashlib.sha256(URL.encode()).hexdigest()}
    old = diagnostic._key(target, source.data_fingerprint, evidence_contract_version=16)
    new = diagnostic._key(target, source.data_fingerprint, evidence_contract_version=17)
    assert old != new == diagnostic._key(target, 'changed-hash-cannot-rearm', evidence_contract_version=17)


@pytest.mark.parametrize('prefix', ['台灣英文新聞', '中央社', '台灣英文新聞－記者', '未知來源'])
def test_bare_publisher_or_other_role_is_not_the_observed_editor_question(queue, settings, prefix):
    previous_inline_observation(queue, settings, prefix=prefix)
    with pytest.raises(ValueError, match='prior_literal_inline_editor_evidence_required'):
        submit_inline(queue, settings)
    with pytest.raises(NewsError):
        private_taiwannews_inline_original_permit(response(inline_page(prefix)), current(settings), URL)


def test_legacy_scope_requires_actual_completed_prior_metadata(queue, settings):
    failed(queue)
    with pytest.raises(ValueError, match='prior_literal_inline_editor_evidence_required'):
        submit_inline(queue, settings)
    submit(queue, settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(queue, response=response(inline_page()))
    job = diagnostic.claim_candidate(queue, http, settings, 'taiwannews')
    assert job is not None
    with pytest.raises(ValueError, match='prior_literal_inline_editor_evidence_required'):
        submit_inline(queue, settings)
    assert not http.calls


@pytest.mark.parametrize('legacy', [False, True])
def test_legacy_editor_original_is_private_once_and_never_upgrades_v2_body(queue, settings, legacy):
    requested = URL.replace('/zh/', '/ch/') if legacy else URL
    old = previous_inline_observation(queue, settings, requested=requested)
    old_row = tuple(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (old['request_id'],)).fetchone())
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (requested,)).fetchone())
    budgets = [tuple(row) for row in queue.db.execute('SELECT * FROM queue_settings')]
    raw = inline_page()
    with pytest.raises(NewsError, match='article_title_not_verified'):
        extract_article(raw, current(settings), page_url=URL)
    new = submit_inline(queue, settings, requested)
    diagnostic.pump_candidates(queue)
    hops = () if not legacy else (RedirectHop(requested, 301, URL, URL,
        '2026-10-01T00:00:00+00:00', 'a'*64, 1),)
    http = http_stub(queue, response=response(raw, redirects=hops))
    job = diagnostic.claim_candidate(queue, http, settings, 'taiwannews')
    result = diagnostic.fetch_candidate(http, job)
    assert isinstance(result, PendingOriginal) and result.permit['question'] == TAIWANNEWS_INLINE_ORIGINAL_QUESTION
    diagnostic.finish_candidate(queue, job, result=result)
    status = diagnostic.candidate_status(queue.root, new['request_id'])
    assert status['state'] == 'observed' and status['raw_or_names_or_body_retained'] is True
    assert tuple(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (old['request_id'],)).fetchone()) == old_row
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (requested,)).fetchone()) == before
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM queue_settings')] == budgets
    assert not submit_inline(queue, settings, requested, reason='No repeated scope GET')['submitted']
    assert not submit(queue, settings, requested, reason='Do not repeat completed v16')['submitted']
    assert http.calls == [requested]
    record = status['result_json']; original = queue.root / record['private_original']['path']
    stored, = pq.read_table(original).to_pylist()
    assert stored['raw_response_body'] == raw and stat.S_IMODE(original.stat().st_mode) == 0o600
    assert not stored['accepted_as_article'] and not stored['publish'] and not stored['remote_training_export']


@pytest.mark.parametrize('old,new', [
    ('台灣英文新聞－編輯 ', '台灣英文新聞－編輯 其他文字 '),
    ('台灣英文新聞－編輯 ', '<span hidden>台灣英文新聞－編輯 </span>'),
    ('台灣英文新聞－編輯 ', '<span onclick="not-run()">台灣英文新聞－編輯 </span>'),
    ('<div class="editor app">', '<div class="editor app"><p>中央社提供</p>'),
    ('<div class="editor app">', '<div class="editor app"><iframe></iframe>'),
    ('<div class="editor app">', '<div class="editor app"><p hidden>hidden</p>'),
])
def test_new_inline_scope_does_not_remove_visibility_wire_or_active_markup_guards(settings, old, new):
    raw = inline_page().replace(old.encode(), new.encode())
    with pytest.raises(NewsError):
        private_taiwannews_inline_original_permit(response(raw), current(settings), URL)


def test_literal_legacy_word_break_bytes_are_evidence_not_rewritten_body(settings):
    raw = inline_page().replace(b'10.8', b'10.<wbr/>8')
    permit = private_taiwannews_inline_original_permit(response(raw), current(settings), URL)
    assert permit['response_sha256'] == hashlib.sha256(raw).hexdigest()
    with pytest.raises(NewsError):
        extract_article(raw, current(settings), page_url=URL)

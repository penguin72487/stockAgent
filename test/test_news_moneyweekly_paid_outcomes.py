"""Paid-preview classification reuses sealed native evidence without new HTTP."""
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import time
from unittest.mock import patch

from bs4 import BeautifulSoup
import pytest

from downloader import news_fulltext_candidate_diagnostics as candidates
from downloader import news_fulltext_moneyweekly_outcomes as paid
from downloader import news_fulltext_repairs as repairs
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from test_news_moneyweekly_body_geometry import claim, submit as submit_geometry, transport
from test_news_moneyweekly_native_routes import MAGAZINE, magazine_page, native_config


def paid_page():
    payload, _ = magazine_page()
    soup = BeautifulSoup(payload, 'lxml')
    container = soup.select_one('.div_Article_Info[itemprop=articleBody]')
    container.clear()
    fragment = BeautifulSoup('<div>公開預覽只包含這一段，不能當作完整文章。</div>'
        '<div>本文為付費文章，欲閱覽請至商城作數位訂閱<br>'
        '<a href="/Shop/Subscribe">訂閱服務</a></div>', 'lxml')
    for node in list(fragment.body.contents):
        container.append(node)
    return soup.encode()


def head(queue):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (MAGAZINE,)).fetchone())


def consume(queue):
    # Production keeps its bounded slice; the transaction tests use a fixed clock.
    with patch.object(repairs.time, 'monotonic', return_value=0.0):
        return repairs.consume_repairs(queue)


@contextmanager
def retained(tmp_path, monkeypatch, *, body=None):
    settings = native_config()
    settings.update(root=str(tmp_path), exclusive_http_owner=False, min_free_disk_bytes=1)
    with monkeypatch.context() as predecessor:
        predecessor.setattr(paid, 'CONTRACT_VERSION', 0)
        old_registry = PolicyRegistry(settings, base=Path.cwd())
    previous = NewsQueue(tmp_path, old_registry)
    try:
        previous.add_articles([{'url': MAGAZINE}])
        with previous.db:
            previous.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found',"
                "attempts=2,due=0,last_checked_at_utc='2026-01-01T00:00:00+00:00' WHERE url=?", (MAGAZINE,))
        request = submit_geometry(previous, settings, MAGAZINE)
        client = transport(previous, response=Response(MAGAZINE, 200, {'content-type': 'text/html'}, body or paid_page()))
        with patch.object(candidates.time, 'monotonic', return_value=0.0):
            job = claim(previous, settings, client)
        result = candidates.fetch_candidate(client, job)
        candidates.finish_candidate(previous, job, result=result)
        assert client.calls == [(MAGAZINE, False)]
        closed = dict(previous.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (request['request_id'],)).fetchone())
        assert closed['state'] == 'observed'
        ref = dict(request_id=request['request_id'], job_sha256=hashlib.sha256(repairs._bytes(closed)).hexdigest())
        before = head(previous)
    finally:
        previous.close()
    current = NewsQueue(tmp_path, PolicyRegistry(settings, base=Path.cwd()))
    try:
        assert head(current) == before
        source = current.registry.sources['moneyweekly']
        old = old_registry.sources['moneyweekly']
        assert source.data_fingerprint != old.data_fingerprint
        assert source.parser_repair_fingerprint == old.parser_repair_fingerprint
        assert source.authorization_fingerprint == old.authorization_fingerprint
        assert source.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
        assert old.data_fingerprint in source.parser_repair_capture_history
        monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
            lambda *a, **k: pytest.fail('retained outcome must not instantiate an HTTP client'))
        yield current, ref, before, closed
    finally:
        current.close()


def submit(queue, ref):
    return paid.submit(queue.root, queue.registry, [MAGAZINE],
        reason='Classify an exact native paid notice using an immutable prior observation', witnesses={MAGAZINE: ref})


def receipt(queue, request):
    return json.loads((queue.root/'reviewed_repairs/processed'/(request['request_id']+'.outcome.json')).read_bytes())


def test_native_paid_footer_precedes_schema_and_body_fallback():
    source = PolicyRegistry(native_config(), base=Path.cwd()).sources['moneyweekly']
    with pytest.raises(NewsError, match='^paywall_or_login$'):
        extract_article(paid_page(), source, page_url=MAGAZINE)
    original, text = magazine_page()
    assert extract_article(original, source, page_url=MAGAZINE)['body'] == text


@pytest.mark.parametrize('change', ['quoted_paragraph', 'navigation', 'hidden', 'extra_prose', 'wrong_notice', 'second_link', 'wrong_route'])
def test_footer_recognizer_does_not_expand_to_quoted_or_hidden_notices(change):
    source = PolicyRegistry(native_config(), base=Path.cwd()).sources['moneyweekly']
    soup = BeautifulSoup(paid_page(), source.html_parser)
    container = soup.select_one(source.selectors[0])
    footer = container.find_all('div', recursive=False)[-1]
    url = MAGAZINE
    if change == 'quoted_paragraph':
        footer.name = 'p'
    elif change == 'navigation':
        container.clear()
        container.append(soup.new_tag('p'))
        soup.body.append(footer)
    elif change == 'hidden':
        footer['hidden'] = ''
    elif change == 'extra_prose':
        footer.append('其餘正文')
    elif change == 'wrong_notice':
        footer.contents[0].replace_with('本文提到付費文章這個現象')
    elif change == 'second_link':
        footer.append(soup.new_tag('a', href='/another'))
    else:
        url = 'https://www.moneyweekly.com.tw/ArticleData/Info/Article/257127'
    assert not paid.native_paid_preview(container, source, page_url=url)


def test_retained_notice_changes_only_classification_and_keeps_closed_evidence(tmp_path, monkeypatch):
    with retained(tmp_path, monkeypatch) as (queue, ref, before, closed):
        request = submit(queue, ref)
        assert head(queue) == before and request['publisher_requests'] == 0 and not request['queue_mutations']
        sealed = repairs._load(Path(request['request_path']))
        assert sealed['contract_version'] == paid.VERSION
        assert consume(queue)['outcomes'] == {paid.OUTCOME: 1}
        assert head(queue) == dict(before, state='paywall_or_login', error_code='paywall_or_login')
        assert dict(queue.db.execute('SELECT * FROM native_candidate_jobs').fetchone()) == closed
        outcome = receipt(queue, request)
        assert outcome['reclassified_targets'] == 1 and outcome['admitted_targets'] == 0
        assert outcome['publisher_requests'] == outcome['body_acceptances'] == 0
        assert not outcome['source_check_and_publication_clocks_changed'] and not outcome['source_freshness_verified']
        assert outcome['immutable_bodies_preserved'] and outcome['witnesses'] == sealed['witnesses']
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert not (queue.root/'bodies').exists()
        assert not submit(queue, ref)['submitted'] and consume(queue)['processed'] == 0


@pytest.mark.parametrize('change', ['unknown_job', 'wrong_sha', 'changed_observation', 'clock', 'not_paid'])
def test_no_missing_or_changed_or_nonpaid_evidence_can_classify(tmp_path, monkeypatch, change):
    body = magazine_page()[0] if change == 'not_paid' else None
    with retained(tmp_path, monkeypatch, body=body) as (queue, ref, before, closed):
        if change == 'unknown_job':
            ref = dict(ref, request_id='0'*64)
        elif change == 'wrong_sha':
            ref = dict(ref, job_sha256='0'*64)
        elif change == 'changed_observation':
            with queue.db:
                queue.db.execute("UPDATE native_candidate_jobs SET result_json='{}'")
        elif change == 'clock':
            with queue.db:
                queue.db.execute("UPDATE articles SET last_checked_at_utc='2026-01-02T00:00:00+00:00' WHERE url=?", (MAGAZINE,))
        with pytest.raises(ValueError, match='closed_paid_preview_evidence_required'):
            submit(queue, ref)
        assert head(queue)['state'] == before['state']
        assert not (queue.root/'reviewed_repairs/pending').exists()


@pytest.mark.parametrize('changes,expected', [
    ({'attempts': 3}, 'head_changed_since_review'),
    ({'lease_owner': 'another-owner'}, 'leased_head_protected'),
    ({'body_sha256': 'a'*64}, 'stored_head_protected'),
    ({'due': 4102444800.0}, 'future_or_invalid_deadline'),
])
def test_newer_heads_and_leases_are_never_overwritten(tmp_path, monkeypatch, changes, expected):
    with retained(tmp_path, monkeypatch) as (queue, ref, before, closed):
        request = submit(queue, ref)
        fields = ','.join(key+'=?' for key in changes)
        with queue.db:
            queue.db.execute('UPDATE articles SET '+fields+' WHERE url=?', (*changes.values(), MAGAZINE))
        assert consume(queue)['outcomes'] == {expected: 1}
        assert head(queue) == dict(before, **changes)
        assert receipt(queue, request)['reclassified_targets'] == 0


def test_source_change_after_review_is_not_a_new_download_authorization(tmp_path, monkeypatch):
    with retained(tmp_path, monkeypatch) as (queue, ref, before, closed):
        request = submit(queue, ref)
        from dataclasses import replace
        source = queue.registry.sources['moneyweekly']
        queue.registry.sources['moneyweekly'] = replace(source, authorization_fingerprint='f'*64)
        assert consume(queue)['outcomes'] == {'source_identity_changed': 1}
        assert head(queue) == before and receipt(queue, request)['publisher_requests'] == 0


def test_archive_failure_is_idempotent_without_revalidating_or_refetching(tmp_path, monkeypatch):
    with retained(tmp_path, monkeypatch) as (queue, ref, before, closed):
        request = submit(queue, ref)
        with monkeypatch.context() as crash:
            crash.setattr(repairs, '_archive', lambda *a, **k: (_ for _ in ()).throw(OSError('archive unavailable')))
            assert consume(queue)['outcomes'] == {paid.OUTCOME: 1}
        classified = head(queue)
        monkeypatch.setattr(paid, 'admit', lambda *a, **k: pytest.fail('committed request cannot be applied twice'))
        assert consume(queue)['processed'] == 0 and head(queue) == classified
        assert receipt(queue, request)['reclassified_targets'] == 1
        assert queue.db.execute('SELECT count(*) FROM reviewed_repair_items').fetchone()[0] == 1


@pytest.mark.parametrize('change', ['version_bool', 'extra', 'future', 'nan_due', 'attempt_bool', 'reference_path', 'wrong_provider'])
def test_closed_request_schema_rejects_unbounded_or_ambiguous_values(tmp_path, monkeypatch, change):
    with retained(tmp_path, monkeypatch) as (queue, ref, before, closed):
        submitted = submit(queue, ref)
        request = deepcopy(repairs._load(Path(submitted['request_path'])))
        if change == 'version_bool': request['contract_version'] = True
        elif change == 'extra': request['unexpected'] = 'not in ABI'
        elif change == 'future': request['created_at_utc'] = (datetime.now(timezone.utc)+timedelta(hours=1)).isoformat()
        elif change == 'nan_due': request['targets'][0]['due'] = float('nan')
        elif change == 'attempt_bool': request['targets'][0]['attempts'] = True
        elif change == 'reference_path': next(iter(request['witnesses'].values()))['path'] = '../raw.html'
        else: request['targets'][0]['provider'] = 'another-provider'
        with pytest.raises(ValueError):
            repairs._validate_request(request)
        assert head(queue) == before


def test_expired_slice_defers_then_resumes_through_the_same_owner(tmp_path, monkeypatch):
    with retained(tmp_path, monkeypatch) as (queue, ref, before, closed):
        request = submit(queue, ref)
        started = time.monotonic()
        with patch.object(repairs.time, 'monotonic', side_effect=[started, started+2]):
            result = repairs.consume_repairs(queue)
        assert result['processed'] == 0 and head(queue) == before
        assert consume(queue)['outcomes'] == {paid.OUTCOME: 1}
        assert receipt(queue, request)['reclassified_targets'] == 1

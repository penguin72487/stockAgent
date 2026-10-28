"""Missing Knews originals are observed once by the existing HTTP/queue owner."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import time

import pyarrow.parquet as pq
import pytest
from bs4 import BeautifulSoup

from downloader import news_fulltext_candidate_diagnostics as diagnostics
from downloader.news_fulltext_candidates import candidate_evidence_allowed, candidate_response_metadata
from downloader.news_fulltext_http import Response
from downloader.news_fulltext import load_config
from downloader.news_fulltext_knews import KNEWS_SPARSE_NATIVE_LAYOUT, private_knews_failure_original_permit
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config
from test_news_knews_known_urls import URL, authored
from test_news_knews_native import schema
from test_news_knews_written_blocks import sparse_entry


def source():
    settings, _, repo = load_config(Path('configs/news_fulltext.json'))
    settings['providers'] = [sparse_entry()]
    return PolicyRegistry(settings, base=repo).sources['knews']


def response(*, printed_failure=False):
    soup = authored()
    if printed_failure:
        soup.select_one('.post-intro').string = '合成報導：這個尚未辨識的署名仍不能自動當成記者。'
    else:
        soup.select_one('.article-content-inner').append(soup.new_tag('aside'))
    return Response(URL, 200, {'content-type': 'text/html', 'set-cookie': 'SECRET'}, str(soup).encode())


def facts(raw):
    current = source()
    return candidate_response_metadata(raw, current, current, evidence_contract_version=56, requested_url=URL)


@pytest.mark.parametrize('printed_failure', [False, True])
def test_raw_permission_keeps_a_failed_body_failed(printed_failure):
    raw = response(printed_failure=printed_failure)
    current = source()
    with pytest.raises(NewsError, match='article_attribution_not_verified' if printed_failure else 'article_identity_not_verified'):
        extract_article(raw.body, current, page_url=URL)
    result = facts(raw)
    assert not result['accepted_as_article'] and not result['body_completeness_verified']
    assert not result['raw_or_names_or_body_retained']
    permit = result['private_original_permit']
    assert permit['response_sha256'] == hashlib.sha256(raw.body).hexdigest()
    assert not permit['body_geometry_accepted'] and not permit['printed_signature_accepted']
    assert '合成' not in json.dumps(result, ensure_ascii=False) and 'SECRET' not in json.dumps(result)


@pytest.mark.parametrize('change', [dict(allowed=False), dict(known_url_only=False),
    dict(domains=('example.org',)), dict(embedded_article_layout='knews_public_native_article_v6'),
    dict(selectors=('body',)), dict(acquisition_mode='finite')])
def test_question_preserves_current_source_boundary(change):
    current = source()
    assert candidate_evidence_allowed(current, current, 56)
    changed = replace(current, **change)
    assert not candidate_evidence_allowed(changed, changed, 56)
    assert not candidate_evidence_allowed(current, replace(current, data_fingerprint='a'*64), 56)
    assert not candidate_evidence_allowed(current, current, 55)


@pytest.mark.parametrize('mutation', ['alias', 'author', 'external', 'clock', 'paywall', 'hidden', 'oversize'])
def test_unverified_headers_and_access_do_not_allow_raw_retention(mutation):
    soup = BeautifulSoup(str(authored()), 'lxml')
    if mutation == 'alias':
        soup.select_one('link[rel=canonical]')['href'] = URL + '?different=1'
    elif mutation == 'author':
        soup.select_one('meta[name=author]')['content'] = '不同合成署名'
    elif mutation == 'external':
        soup.select_one('.post-intro').string = '【中央社台北報導】合成轉載不可入庫。'
    elif mutation == 'clock':
        schema(soup, lambda article: article.update(datePublished='2026-06-17'))
    elif mutation == 'paywall':
        schema(soup, lambda article: article.update(isAccessibleForFree=False))
    elif mutation == 'hidden':
        soup.select_one('.article-content-inner')['hidden'] = ''
    body = b'x' * ((2 << 20) + 1) if mutation == 'oversize' else str(soup).encode()
    raw = Response(URL, 200, {'content-type': 'text/html'}, body)
    with pytest.raises(NewsError):
        private_knews_failure_original_permit(raw, source(), URL)


@pytest.fixture
def owner(config, tmp_path):
    settings = deepcopy(config)
    settings.update(root=str(tmp_path), providers=[sparse_entry()])
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    with queue.db:
        queue.add_articles([{'url': URL}])
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
            "attempts=6,due=0,last_checked_at_utc='2026-10-24T10:00:00+00:00' WHERE url=?", (URL,))
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)',
            ('failure_diagnostic_attempts:v1:knews:20261026', '2'))
    try:
        yield queue, settings
    finally:
        queue.close()


def submit(queue, settings, reason='Diagnose a previously unobserved failed native boundary'):
    return diagnostics.submit_candidate(queue.root, settings, queue.registry, URL, KNEWS_SPARSE_NATIVE_LAYOUT,
        reason=reason, evidence_contract_version=56)


def http_stub():
    calls = []
    def get(url, *, follow_redirects):
        calls.append((url, follow_redirects))
        return response()
    return SimpleNamespace(get=get, calls=calls, provider_not_before={},
        blocked_gateway_hosts=lambda _: (), blocked_robots_origins=lambda _: ())


def test_once_owner_retains_exact_raw_and_receipt_without_admission_or_budget_reset(owner, monkeypatch):
    queue, settings = owner
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    budgets = list(map(tuple, queue.db.execute('SELECT * FROM queue_settings')))
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *args, **kwargs: pytest.fail('no second HTTP owner'))
    request = submit(queue, settings)
    diagnostics.pump_candidates(queue)
    http = http_stub()
    job = diagnostics.claim_candidate(queue, http, settings, 'knews')
    assert job is not None and not http.calls
    diagnostics.finish_candidate(queue, job, result=diagnostics.fetch_candidate(http, job))
    completed = diagnostics.candidate_status(queue.root, request['request_id'])
    assert completed['state'] == 'observed' and http.calls == [(URL, False)]
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
    assert list(map(tuple, queue.db.execute('SELECT * FROM queue_settings'))) == budgets
    assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()
    result = completed['result_json']
    assert result['raw_or_names_or_body_retained'] and not result['accepted_as_article']
    reference = result['private_original']
    path = queue.root / reference['path']
    record, = pq.ParquetFile(path).read().to_pylist()
    assert record['raw_response_body'] == response().body
    assert 'SECRET' not in record['headers_json']
    assert path.stat().st_mode & 0o077 == 0
    receipt = json.loads((queue.root / reference['receipt_path']).read_bytes())
    assert receipt['original'] == reference and not receipt['publish'] and not receipt['remote_training_export']
    duplicate = submit(queue, settings, reason='Different reason cannot repeat the completed original')
    assert not duplicate['submitted'] and duplicate['request_id'] == request['request_id']
    target = json.loads(queue.db.execute('SELECT request_json FROM native_candidate_jobs').fetchone()[0])['target']
    assert diagnostics._key(target, 'a'*64, evidence_contract_version=56) == diagnostics._key(
        target, 'b'*64, evidence_contract_version=56)


@pytest.mark.parametrize('blocked', ['deadline', 'gateway', 'robots', 'uncertain', 'stored', 'lease'])
def test_cooldown_refusal_unknown_and_protected_heads_do_not_issue_requests(owner, blocked):
    queue, settings = owner
    request = submit(queue, settings)
    diagnostics.pump_candidates(queue)
    http = http_stub()
    if blocked == 'deadline':
        http.provider_not_before['knews'] = time.time() + 600
    elif blocked == 'gateway':
        http.blocked_gateway_hosts = lambda _: ('www.knews.com.tw',)
    elif blocked == 'robots':
        http.blocked_robots_origins = lambda _: ('https://www.knews.com.tw',)
    else:
        with queue.db:
            if blocked == 'uncertain':
                queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?", (request['request_id'],))
            elif blocked == 'stored':
                queue.db.execute("UPDATE articles SET body_sha256=? WHERE url=?", ('f'*64, URL))
            else:
                queue.db.execute("UPDATE articles SET lease_owner='other',lease_until=? WHERE url=?", (time.time()+60, URL))
        diagnostics.pump_candidates(queue)
    assert diagnostics.claim_candidate(queue, http, settings, 'knews') is None and not http.calls


def test_write_failure_stays_uncertain_and_does_not_repeat_http(owner, monkeypatch):
    from downloader import artifact_io
    queue, settings = owner
    requested = submit(queue, settings)
    diagnostics.pump_candidates(queue)
    http = http_stub()
    job = diagnostics.claim_candidate(queue, http, settings, 'knews')
    result = diagnostics.fetch_candidate(http, job)
    def broken(*args, **kwargs):
        raise OSError('SECRET storage detail')
    monkeypatch.setattr(artifact_io, 'atomic_write_parquet', broken)
    diagnostics.finish_candidate(queue, job, result=result)
    status = diagnostics.candidate_status(queue.root, requested['request_id'])
    assert status['state'] == 'observed_error'
    assert 'SECRET' not in json.dumps(status)
    assert not submit(queue, settings)['submitted']
    assert len(http.calls) == 1 and not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()


def missing_header_response():
    soup = authored()
    link = soup.select_one('.abc-byline > a')
    link.name = 'span'
    del link['href']
    return Response(URL, 200, {'content-type': 'text/html'}, str(soup).encode())


@pytest.fixture
def header_owner(config, tmp_path):
    from test_news_knews_grid_media import grid_entry
    settings = deepcopy(config)
    settings.update(root=str(tmp_path), providers=[grid_entry()])
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        with queue.db:
            queue.add_articles([{'url': URL}])
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
                "attempts=6,due=0,last_checked_at_utc='2026-10-24T10:00:00+00:00' WHERE url=?", (URL,))
        request = diagnostics.submit_candidate(queue.root, settings, queue.registry, URL,
            grid_entry()['embedded_article_layout'], reason='First exact original boundary', evidence_contract_version=56)
        http = http_stub()
        http.get = lambda url, *, follow_redirects: missing_header_response()
        job, deadline = None, time.monotonic() + 5
        while job is None and time.monotonic() < deadline:
            diagnostics.pump_candidates(queue, max_seconds=.1)
            job = diagnostics.claim_candidate(queue, http, settings, 'knews')
        assert job is not None, diagnostics.candidate_status(queue.root, request['request_id'])
        diagnostics.finish_candidate(queue, job, result=diagnostics.fetch_candidate(http, job))
        prior = diagnostics.candidate_status(queue.root, request['request_id'])
        assert prior['state'] == 'observed' and 'private_original' not in prior['result_json']
        yield queue, settings, request['request_id']
    finally:
        queue.close()


def submit_header(queue, settings):
    return diagnostics.submit_candidate(queue.root, settings, queue.registry, URL,
        queue.registry.sources['knews'].embedded_article_layout,
        reason='Unanswered native byline structure only; never repeat original56', evidence_contract_version=57)


def test_missing_header_structure_uses_one_resident_request_and_retains_no_prose(header_owner):
    from downloader.news_fulltext_candidate_originals import ORIGINAL_EVIDENCE_VERSIONS
    queue, settings, prior_id = header_owner
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    old = dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (prior_id,)).fetchone())
    budget = list(map(tuple, queue.db.execute('SELECT * FROM queue_settings')))
    request = submit_header(queue, settings)
    assert request['submitted'] and 57 not in ORIGINAL_EVIDENCE_VERSIONS
    diagnostics.pump_candidates(queue)
    http = http_stub()
    def get(url, *, follow_redirects):
        http.calls.append((url, follow_redirects))
        return missing_header_response()
    http.get = get
    job = diagnostics.claim_candidate(queue, http, settings, 'knews')
    diagnostics.finish_candidate(queue, job, result=diagnostics.fetch_candidate(http, job))
    completed = diagnostics.candidate_status(queue.root, request['request_id'])
    assert completed['state'] == 'observed' and http.calls == [(URL, False)]
    facts = completed['result_json']; header = facts['native_header_structure']
    assert header['native_anchor_count'] == 0 and header['native_byline_count'] == 1
    assert header['schema_person_count'] == 1 and header['meta_author_equals_one_declared_name']
    assert any(n['tag'] == 'span' and n['equals_meta_author'] and n['equals_declared_author']
        for n in header['native_panel_shape'])
    assert not facts['accepted_as_article'] and not facts['raw_or_names_or_body_retained']
    assert '合成' not in json.dumps(facts, ensure_ascii=False)
    assert not list(queue.root.glob('reviewed_repairs/candidate_diagnostics/originals/*'))
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
    assert dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (prior_id,)).fetchone()) == old
    assert list(map(tuple, queue.db.execute('SELECT * FROM queue_settings'))) == budget
    assert not submit_header(queue, settings)['submitted']


@pytest.mark.parametrize('change', ['missing', 'uncertain', 'raw', 'author_error', 'clock', 'head', 'source', 'site'])
def test_new_header_question_requires_the_exact_closed_unretained_refusal(header_owner, change):
    queue, settings, prior_id = header_owner
    row = queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (prior_id,)).fetchone()
    result = json.loads(row['result_json'])
    request = json.loads(row['request_json'])
    with queue.db:
        if change == 'missing': queue.db.execute('DELETE FROM native_candidate_jobs WHERE request_id=?', (prior_id,))
        elif change == 'uncertain': queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?", (prior_id,))
        elif change == 'head': queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (URL,))
        elif change == 'clock': queue.db.execute("UPDATE native_candidate_jobs SET completed_at_utc='2099-01-01T00:00:00+00:00' WHERE request_id=?", (prior_id,))
        else:
            if change == 'raw': result['private_original'] = {'path': 'existing'}
            elif change == 'author_error': result['private_original_scope_error'] = 'article_attribution_not_verified'
            elif change == 'source': request['source']['authorization_fingerprint'] = 'f'*64
            else: result['private_original_scope_failure_location']['sites'][-1]['function'] = 'different_boundary'
            queue.db.execute('UPDATE native_candidate_jobs SET result_json=?,request_json=? WHERE request_id=?',
                (json.dumps(result), json.dumps(request), prior_id))
    with pytest.raises(ValueError, match='completed_knews_unretained_native_header_refusal_required'):
        submit_header(queue, settings)


@pytest.mark.parametrize('blocked', ['deadline', 'gateway', 'robots', 'uncertain', 'stored', 'lease', 'prior_changed'])
def test_header_diagnosis_preserves_resident_cooldowns_and_revalidates_prior(header_owner, blocked):
    queue, settings, prior_id = header_owner
    request = submit_header(queue, settings)
    diagnostics.pump_candidates(queue)
    http = http_stub()
    if blocked == 'deadline': http.provider_not_before['knews'] = time.time() + 600
    elif blocked == 'gateway': http.blocked_gateway_hosts = lambda _: ('www.knews.com.tw',)
    elif blocked == 'robots': http.blocked_robots_origins = lambda _: ('https://www.knews.com.tw',)
    else:
        with queue.db:
            if blocked == 'uncertain': queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?", (request['request_id'],))
            elif blocked == 'stored': queue.db.execute('UPDATE articles SET body_sha256=? WHERE url=?', ('e'*64, URL))
            elif blocked == 'lease': queue.db.execute("UPDATE articles SET lease_owner='other',lease_until=? WHERE url=?", (time.time()+60, URL))
            else: queue.db.execute("UPDATE native_candidate_jobs SET state='observed_error' WHERE request_id=?", (prior_id,))
    assert diagnostics.claim_candidate(queue, http, settings, 'knews') is None and not http.calls


@pytest.mark.parametrize('mutation', ['identity', 'clock', 'paywall', 'redirect', 'hidden'])
def test_header_metadata_cannot_bypass_public_identity_access_and_clock(mutation):
    from test_news_knews_grid_media import grid_entry
    from test_news_knews_tables import source as grid_source
    raw = missing_header_response()
    soup = BeautifulSoup(raw.body, 'lxml')
    if mutation == 'identity': soup.select_one('link[rel=canonical]')['href'] = URL + '?different'
    elif mutation == 'clock': schema(soup, lambda obj: obj.update(datePublished='2026-06-17'))
    elif mutation == 'paywall': schema(soup, lambda obj: obj.update(isAccessibleForFree=False))
    elif mutation == 'hidden': soup.select_one('article')['hidden'] = ''
    raw = Response(URL + '/wrong' if mutation == 'redirect' else URL, 200, {'content-type':'text/html'}, str(soup).encode())
    with pytest.raises(NewsError):
        candidate_response_metadata(raw, grid_source(grid_entry()), grid_source(grid_entry()),
            evidence_contract_version=57, requested_url=URL)

"""Metadata only, synthetic documents; normal article permission is unchanged."""
from copy import deepcopy
import json
from types import SimpleNamespace
from unittest.mock import patch

from bs4 import BeautifulSoup
import pytest

from downloader import news_fulltext_candidate_diagnostics as jobs
from downloader.news_fulltext_candidates import candidate_response_metadata
from downloader.news_fulltext_http import RedirectHop, Response
from downloader.news_fulltext_pocket_scope import (
    MAX_BYTES, QUESTION, non_school_metadata, non_school_route, require_non_school_response)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_pocket_original import settings as original_settings
from test_news_pocket_opaque_routes import opaque_routes


ETF = 'https://www.pocket.tw/etf/tw/00410A/fundholding/'
VIDEO = 'https://www.pocket.tw/school/videos/1188/'
LAYOUT = 'pocket_visible_article_v12'


@pytest.fixture
def config(tmp_path):
    result = opaque_routes(original_settings.__wrapped__())
    result['providers'][0]['embedded_article_layout'] = LAYOUT
    result.update(root=str(tmp_path), min_free_disk_bytes=1, exclusive_http_owner=False,
        workers=2, adaptive_rate={'enabled': False})
    return result


def markup(url=ETF, types=('WebPage', 'FinancialProduct')):
    schema = {'@type': 'WebPage', 'name': 'DO_NOT_EXPORT_SECRET_TITLE',
        'author': {'name': 'DO_NOT_EXPORT_SECRET_AUTHOR'},
        '@graph': [{'@type': kind, 'description': 'DO_NOT_EXPORT_SECRET_BODY'} for kind in types]}
    return '<html><head><link rel="canonical" href="'+url+'"><meta property="og:url" content="'+url+'">'+\
        '<script type="application/ld+json">'+json.dumps(schema)+'</script></head><body>'+\
        '<main><h1>DO_NOT_EXPORT_SECRET_TITLE</h1><a>持股明細</a><h2>配息資訊</h2>'+\
        '<table><tr><td>DO_NOT_EXPORT_SECRET_VALUE</td></tr></table>'+\
        '<iframe src="https://media.example/DO_NOT_REQUEST"></iframe></main></body></html>'


def response(url=ETF, *, final=None, raw=None, hops=(), status=200, mime='text/html'):
    return Response(final or url, status, {'content-type': mime},
        (markup(final or url).encode() if raw is None else raw), hops)


def hop(before, after, status=301):
    return RedirectHop(from_url=before, status=status, location=after, to_url=after,
        observed_at_utc='2026-01-01T00:00:00+00:00', response_sha256='a'*64, response_bytes=0)


def failed_head(queue, url=ETF, **overrides):
    queue.add_articles([{'url': url}])
    values = {'state': 'unsupported_article_path', 'error_code': 'unsupported_article_path',
        'due': 0, 'attempts': 1, 'last_checked_at_utc': '2026-01-01T00:00:00+00:00', **overrides}
    with queue.db:
        queue.db.execute('UPDATE articles SET '+','.join(k+'=?' for k in values)+' WHERE url=?',
            (*values.values(), url))
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())


def submit(queue, config, url=ETF, reason='Observe one unknown document boundary, no article admission'):
    return jobs.submit_candidate(queue.root, config, queue.registry, url, LAYOUT,
        reason=reason, evidence_contract_version=7)


def client(queue, result=None):
    calls = []
    return SimpleNamespace(registry=queue.registry, calls=calls,
        get=lambda url: (calls.append(url), result or response(url))[1], provider_not_before={},
        blocked_gateway_hosts=lambda p: (), blocked_robots_origins=lambda p: ())


def pump(queue):
    with patch.object(jobs.time, 'monotonic', return_value=0):
        jobs.pump_candidates(queue)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('url,types', [(ETF, ('WebPage', 'FinancialProduct')),
    (VIDEO, ('VideoObject',)), (ETF, ('Article', 'NewsArticle', 'BlogPosting'))])
def test_metadata_preserves_types_without_exporting_values_names_or_text(parser, url, types):
    facts = non_school_metadata(BeautifulSoup(markup(url, types), parser), page_url=url)
    assert facts['route_kind'] == ('video_document' if url == VIDEO else 'etf_information')
    assert facts['canonical']['literal_page_URL_agrees'] and facts['og_url']['literal_page_URL_agrees']
    assert facts['native_school_body_count'] == 0 and facts['heading_count'] == 1
    assert facts['known_navigation_labels'] == ['持股明細', '配息資訊']
    assert all(facts['known_schema_type_counts'][kind] >= 1 for kind in types)
    assert facts['main_element_counts']['iframe'] == facts['main_element_counts']['table'] == 1
    encoded = json.dumps(facts)
    assert 'DO_NOT_' not in encoded and 'media.example' not in encoded
    assert all(facts[k] is False for k in ('accepted_as_article', 'authorization_or_identity_expanded',
        'raw_names_or_body_retained', 'financial_values_exported', 'embedded_media_fetched'))


@pytest.mark.parametrize('url', [
    'http://www.pocket.tw/etf/tw/00410A/', 'https://evil.example/etf/tw/00410A/',
    'https://www.pocket.tw:443/etf/tw/00410A/', ETF+'#body', ETF+'?api_key=secret',
    'https://www.pocket.tw/api/etf/tw/00410A/', 'https://www.pocket.tw/school/articles/1188/',
    'https://www.pocket.tw/etf/tw/00410A/raw/', VIDEO+'?page=2',
    'https://www.pocket.tw/etf/tw/00410A/../00411A/', ETF+'?source='+'x'*513,
])
def test_non_school_route_does_not_create_a_general_scope(url):
    assert non_school_route(url) is None


@pytest.mark.parametrize('url', [ETF, VIDEO, ETF.rstrip('/'),
    'https://www.pocket.tw/etf//tw/00984D/fundholding',
    'https://www.pocket.tw/etf/tw/00410A/cashdividend?page&parent=&source=APP'])
def test_exact_reviewed_document_routes(url):
    assert non_school_route(url) is not None
    require_non_school_response(response(url), url)


def test_only_witnessed_slash_redirect_for_same_document_section():
    original = 'https://www.pocket.tw/etf//tw/00410A/fundholding'
    require_non_school_response(response(original, final=ETF, hops=(hop(original, ETF),)), original)
    with pytest.raises(NewsError):
        require_non_school_response(response(original, final=ETF), original)


@pytest.mark.parametrize('final,status', [(ETF.replace('00410A', '00411A'), 301),
    (ETF.replace('fundholding', 'cashdividend'), 301), (ETF, 302),
    ('https://other.example/etf/tw/00410A/fundholding/', 301), (ETF+'?source=changed', 301)])
def test_cross_scope_redirects_are_not_document_evidence(final, status):
    with pytest.raises(NewsError):
        require_non_school_response(response(ETF, final=final, hops=(hop(ETF, final, status),)), ETF)


@pytest.mark.parametrize('change', [{'status': 403}, {'status': 404}, {'status': 429},
    {'mime': 'application/json'}, {'raw': b''}, {'raw': b'x'*(MAX_BYTES+1)}])
def test_invalid_source_envelopes_do_not_get_parsed(change):
    with pytest.raises(NewsError):
        require_non_school_response(response(**change), ETF)


def test_schema_limits_and_parse_errors_are_visible_not_silent_negative_evidence():
    raw = markup().replace('</head>', '<script type="application/ld+json">{broken</script></head>')
    facts = non_school_metadata(BeautifulSoup(raw, 'lxml'), page_url=ETF)
    assert facts['schema_parse_errors'] == 1
    for declaration in ([{'@type': 'Article'}]*2050, {'@type': ['WebPage']*40}, {'large': 'x'*MAX_BYTES}):
        raw = '<script type="application/ld+json">'+json.dumps(declaration)+'</script>'
        facts = non_school_metadata(BeautifulSoup(raw, 'lxml'), page_url=ETF)
        assert facts['schema_bound_exceeded']


def test_resident_same_source_once_only_no_body_parser_no_mutations_and_restart(config, tmp_path, monkeypatch):
    import downloader.news_fulltext_candidates as candidates
    monkeypatch.setattr(candidates, 'extract_article', lambda *a, **k: pytest.fail('not an article parse'))
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        before = failed_head(queue)
        request = submit(queue, config)
        pump(queue)
        http = client(queue)
        job = jobs.claim_candidate(queue, http, config, 'pocket')
        assert job and job['request_id'] == request['request_id']
        facts = jobs.fetch_candidate(http, job)
        assert facts['question'] == QUESTION and facts['current_parser_replayed'] is False
        jobs.finish_candidate(queue, job, result=facts)
        pump(queue)
        status = jobs.candidate_status(tmp_path, request['request_id'])
        assert status['state'] == 'observed' and not status['raw_or_names_or_body_retained']
        assert http.calls == [ETF]
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (ETF,)).fetchone()) == before
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert submit(queue, config, reason='Changing reason is not another GET')['submitted'] is False
        target = json.loads(queue.db.execute('SELECT request_json FROM native_candidate_jobs').fetchone()[0])['target']
        assert jobs._key(target, 'a'*64, evidence_contract_version=7) == jobs._key(target, 'b'*64, evidence_contract_version=7)
    finally:
        queue.close()
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        assert submit(queue, config)['state'] == 'observed'
    finally:
        queue.close()


@pytest.mark.parametrize('state', ['ready', 'reserved', 'uncertain_outcome'])
def test_prior_pending_operation_blocks_a_different_question(config, tmp_path, state):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        failed_head(queue)
        request = submit(queue, config)
        pump(queue)
        row = dict(queue.db.execute('SELECT * FROM native_candidate_jobs').fetchone())
        with queue.db:
            queue.db.execute('UPDATE native_candidate_jobs SET request_id=?,observation_key=?,state=?',
                ('f'*64, 'e'*64, state))
        with pytest.raises(ValueError, match='prior_observation_not_terminal'):
            submit(queue, config)
        assert client(queue).calls == []
    finally:
        queue.close()


@pytest.mark.parametrize('overrides', [{'state': 'complete'}, {'body_sha256': 'a'*64}, {'due': 1e12},
    {'state': 'paywall_or_login', 'error_code': 'paywall_or_login'},
    {'state': 'article_identity_not_verified', 'error_code': 'article_identity_not_verified'},
    {'lease_owner': 'other', 'lease_until': 1e12}])
def test_saved_denied_leased_and_not_due_are_never_rearmed(config, tmp_path, overrides):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        failed_head(queue, **overrides)
        with pytest.raises(ValueError):
            submit(queue, config)
    finally:
        queue.close()


@pytest.mark.parametrize('pause', ['origin', 'provider', 'gateway'])
def test_original_source_pauses_are_preserved(config, tmp_path, pause):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        failed_head(queue)
        submit(queue, config)
        pump(queue)
        http = client(queue)
        if pause == 'origin': http.blocked_robots_origins = lambda p: {'https://www.pocket.tw'}
        if pause == 'gateway': http.blocked_gateway_hosts = lambda p: {'www.pocket.tw'}
        if pause == 'provider': http.provider_not_before['pocket'] = 1e12
        assert jobs.claim_candidate(queue, http, config, 'pocket') is None
        assert http.calls == []
    finally:
        queue.close()


def test_unknown_reserved_result_after_restart_does_not_send_again(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        failed_head(queue)
        submit(queue, config)
        pump(queue)
        assert jobs.claim_candidate(queue, client(queue), config, 'pocket')
    finally:
        queue.close()
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        pump(queue)
        assert submit(queue, config)['state'] == 'uncertain_outcome'
        assert jobs.claim_candidate(queue, client(queue), config, 'pocket') is None
    finally:
        queue.close()


def test_mailbox_cli_submits_without_constructing_an_http_owner(config, tmp_path, monkeypatch):
    from scripts import probe_news_sources as cli
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        before = failed_head(queue, VIDEO)
        monkeypatch.setattr(cli, 'load_config', lambda path: (config, queue.registry, tmp_path))
        monkeypatch.setattr(cli, 'NewsHTTP', lambda *a, **k: pytest.fail('not a second owner'))
        target = tmp_path/'submission.json'
        assert cli.main(['--probe', VIDEO, '--candidate-embedded-layout', LAYOUT,
            '--require-current-missing-body-failure', '--failure-code', 'unsupported_article_path',
            '--enqueue-candidate', '--candidate-evidence-contract', '7', '--output', str(target)]) == 0
        assert json.loads(target.read_text())['submitted']
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (VIDEO,)).fetchone()) == before
    finally:
        queue.close()

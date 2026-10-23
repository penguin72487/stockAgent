"""Current iNEWS failure questions share one resident owner and fixed budget."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from types import SimpleNamespace
import time

from bs4 import BeautifulSoup
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidates import candidate_evidence_allowed, native_candidate_source
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_setn import (
    INEWS_BOUNDARY_QUESTION, INEWS_LAYOUT_VERSIONS, INEWS_PROVIDER,
    inews_boundary_source, inews_literal_article_url, inews_resident_boundary_metadata,
)
from test_news_setn_inews import URL, TITLE, NAME, TEXT, CAPTION, CLOCK, page, v3_entry, v4_entry, v8_entry, policy


LAYOUT = 'setn_inews_native_article_v3'


@pytest.fixture
def settings(tmp_path):
    return {'contract_version': 1, 'root': str(tmp_path), 'collection_purpose': 'personal_research',
        'publish': False, 'remote_training_export': False, 'providers': [v3_entry()],
        'min_free_disk_bytes': 1, 'workers': 2, 'exclusive_http_owner': False,
        'adaptive_rate': {'enabled': False}}


def head(queue):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())


@pytest.fixture
def queue(settings, tmp_path):
    owner = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    owner.add_articles([{'url': URL}])
    with owner.db:
        owner.db.execute('UPDATE articles SET state=?,error_code=?,attempts=7,due=?,last_checked_at_utc=? WHERE url=?',
            ('article_identity_not_verified', 'article_identity_not_verified', time.time()-60,
             '2026-01-01T00:00:00+00:00', URL))
    yield owner
    owner.close()


def submit(queue, settings, **kwargs):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, URL, LAYOUT,
        reason=kwargs.pop('reason', 'Current native editor/credit shapes; unanswered original question'),
        evidence_contract_version=24, **kwargs)


def http_stub(queue, response=None):
    calls = []
    def get(url):
        calls.append(url)
        return response or Response(url, 200, {'content-type': 'text/html'}, str(page()).encode())
    return SimpleNamespace(registry=queue.registry, get=get, calls=calls, provider_not_before={},
        blocked_gateway_hosts=lambda p: (), blocked_robots_origins=lambda p: ())


@pytest.mark.parametrize('layout', INEWS_LAYOUT_VERSIONS)
def test_current_layout_map_covers_all_native_versions_without_capture_or_permission_change(settings, tmp_path, layout):
    settings['providers'][0]['embedded_article_layout'] = layout
    if INEWS_LAYOUT_VERSIONS[layout] >= 4:
        settings['providers'][0]['author_selectors'] = v4_entry()['author_selectors']
    if INEWS_LAYOUT_VERSIONS[layout] >= 8:
        settings['providers'][0]['article_selectors'] = v8_entry()['article_selectors']
    registry = PolicyRegistry(settings, base=tmp_path)
    source = registry.sources[INEWS_PROVIDER]
    assert inews_boundary_source(source)
    assert native_candidate_source(settings, registry, tmp_path, layout) is source
    assert candidate_evidence_allowed(source, source, 24)
    assert all(not candidate_evidence_allowed(source, source, v) for v in range(1, 24))
    assert not candidate_evidence_allowed(source, source, True)
    assert diagnostic._failure_states(layout, 24) == {
        'article_identity_not_verified', 'article_attribution_not_verified'}
    assert diagnostic._failure_states(layout, 1) == set()


@pytest.mark.parametrize('change', [
    {'allowed': False}, {'request_enabled': False}, {'domains': ('www.setn.com',)},
    {'known_url_only': False}, {'html_parser': 'html.parser'}, {'selectors': ('body',)},
    {'author_selectors': ()}, {'title_selector': 'h1'}, {'require_author_attribution': False},
    {'require_configured_container': False}, {'authorization': {'kind': 'licensed_api'}},
    {'embedded_article_layout': 'unreviewed_layout'}, {'article_path_pattern': '.*'},
])
def test_scope_is_fixed_to_registered_native_policy(change):
    source = policy(v3_entry())
    # request_enabled is derived; known_url_only is a policy field.
    if 'request_enabled' in change:
        change = {'acquisition_mode': 'local_only'}
    source = replace(source, **change)
    assert not inews_boundary_source(source)
    assert not candidate_evidence_allowed(source, source, 24)


@pytest.mark.parametrize('url', [
    'https://www.setn.com/news/123456', 'http://inews.setn.com/news/123456',
    URL+'?x=1', URL+'#fragment', 'https://inews.setn.com/news/0123456',
    'https://inews.setn.com/News.aspx?NewsID=123456', 'https://inews.setn.com:443/news/123456',
    'https://inews.setn.com/news/123456?', 'https://inews.setn.com/news/123456#', None,
])
def test_no_alias_query_legacy_or_normalized_route_is_admitted(url):
    assert not inews_literal_article_url(url)
    assert inews_literal_article_url(URL)


def test_once_only_current_head_work_uses_same_owner_without_body_names_or_ledger_changes(queue, settings, monkeypatch):
    owner, before = queue.owner, head(queue)
    config_before = deepcopy(settings)
    budgets = list(queue.db.execute('SELECT * FROM queue_settings'))
    changes = queue.db.total_changes
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('must not create another HTTP owner'))
    monkeypatch.setattr('downloader.news_fulltext_policy.extract_article',
        lambda *a, **k: pytest.fail('diagnostic must not run accepting parser'))
    submitted = submit(queue, settings)
    assert submitted['submitted'] and submitted['publisher_requests'] == 0
    assert head(queue) == before and queue.db.total_changes == changes
    assert submit(queue, settings, reason='A changed reason cannot buy another observation')['request_id'] == submitted['request_id']
    diagnostic.pump_candidates(queue)
    http = http_stub(queue)
    job = diagnostic.claim_candidate(queue, http, settings, INEWS_PROVIDER)
    assert job and diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'reserved'
    facts = diagnostic.fetch_candidate(http, job)
    assert http.calls == [URL] and facts['question'] == INEWS_BOUNDARY_QUESTION
    assert facts['accepted_as_article'] is False and facts['raw_or_names_or_body_retained'] is False
    assert facts['current_source_not_candidate'] and not facts['body_completeness_verified']
    private = json.dumps(facts, ensure_ascii=False)
    assert all(text not in private for text in (TITLE, NAME, TEXT, CAPTION, CLOCK, 'test-reporter', 'test.jpg'))
    assert facts['response_sha256'] == hashlib.sha256(str(page()).encode()).hexdigest()
    assert len(private.encode()) < 60000
    diagnostic.finish_candidate(queue, job, result=facts)
    diagnostic.pump_candidates(queue)
    state = diagnostic.candidate_status(queue.root, submitted['request_id'])
    assert state['state'] == 'observed'
    assert queue.db.execute('SELECT owner_id FROM native_candidate_jobs WHERE request_id=?',
        (submitted['request_id'],)).fetchone()[0] == owner
    assert queue.owner == owner and head(queue) == before and settings == config_before
    assert list(queue.db.execute('SELECT * FROM queue_settings')) == budgets
    assert submit(queue, settings)['submitted'] is False
    assert diagnostic.claim_candidate(queue, http, settings, INEWS_PROVIDER) is None
    assert not (queue.root/'batches').exists() and not (queue.root/'candidate_originals').exists()


def test_missing_links_and_unsupported_body_children_are_facts_not_repaired_authors_or_prose():
    soup = page()
    soup.select_one('a[itemprop=author]').unwrap()
    body = soup.select_one('[itemprop=articleBody]')
    extra = BeautifulSoup('<ul><li>合成未知清單正文</li></ul>', 'lxml').ul
    body.append(extra)
    context = inews_resident_boundary_metadata(soup, page_url=URL)
    assert context['credits'][0]['links'] == []
    assert context['credits'][0]['verified_as_credit'] is False
    assert context['credits'][0]['prefix_candidate'] == '記者'
    assert any(n['tag'] == 'ul' and n['child_tags'] == ['li'] for n in context['editors'][0]['bodies'][0]['children'])
    assert '合成未知清單正文' not in json.dumps(context, ensure_ascii=False)
    assert not context['accepted_as_article'] and not context['publication_offset_or_release_role_inferred']


def test_shape_output_stays_finite_and_marks_truncation():
    soup = page()
    body = soup.select_one('[itemprop=articleBody]')
    for i in range(180):
        node = soup.new_tag('p')
        node['arbitrary-'+str(i)+'x'*100] = 'PRIVATE ATTRIBUTE VALUE'
        node.string = 'SYNTHETIC PRIVATE PROSE' * 100
        body.append(node)
    context = inews_resident_boundary_metadata(soup, page_url=URL)
    assert context['global_shape_limit_reached'] and context['global_shape_limit'] == 48
    assert context['editors'][0]['bodies'][0]['children_truncated']
    private = json.dumps(context, ensure_ascii=False)
    assert len(private.encode()) < 60000 and 'PRIVATE ATTRIBUTE VALUE' not in private
    assert 'SYNTHETIC PRIVATE PROSE' not in private


@pytest.mark.parametrize('change', [
    {'body_sha256': 'a'*64}, {'parquet_path': 'existing.parquet'}, {'body_bytes': 1},
    {'captured_at_utc': '2026-01-01T00:00:00Z'}, {'lease_owner': 'another'},
    {'due': time.time()+86400}, {'state': 'http_403', 'error_code': 'http_403'},
    {'state': 'robots_denied', 'error_code': 'robots_denied'},
    {'state': 'body_not_found', 'error_code': 'body_not_found'},
])
def test_protected_or_out_of_question_heads_never_submit(queue, settings, change):
    with queue.db:
        queue.db.execute('UPDATE articles SET '+','.join(k+'=?' for k in change)+' WHERE url=?',
            (*change.values(), URL))
    before = head(queue)
    with pytest.raises(ValueError):
        submit(queue, settings)
    assert head(queue) == before and not (queue.root/'reviewed_repairs').exists()


@pytest.mark.parametrize('kind', ['redirect', 'mime', '403', '429', 'empty', 'oversize', 'challenge', 'paywall'])
def test_response_and_access_gates_do_not_add_another_request(queue, settings, kind):
    response = Response(URL, 200, {'content-type': 'text/html'}, str(page()).encode())
    if kind == 'redirect':
        response = replace(response, url=URL+'0')
    elif kind == 'mime':
        response = replace(response, headers={'content-type': 'application/json'})
    elif kind in {'403', '429'}:
        response = replace(response, status=int(kind))
    elif kind == 'empty':
        response = replace(response, body=b'')
    elif kind == 'oversize':
        response = replace(response, body=b'x' * ((2 << 20)+1))
    elif kind == 'challenge':
        response = replace(response, body=b'<title>Just a moment</title>')
    else:
        response = replace(response, body=b'<html><div class="paywall">Login required</div></html>')
    submitted = submit(queue, settings)
    before = head(queue)
    diagnostic.pump_candidates(queue)
    http = http_stub(queue, response)
    job = diagnostic.claim_candidate(queue, http, settings, INEWS_PROVIDER)
    with pytest.raises(NewsError) as error:
        diagnostic.fetch_candidate(http, job)
    diagnostic.finish_candidate(queue, job, error=error.value)
    assert http.calls == [URL] and head(queue) == before
    assert diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'observed_error'
    assert submit(queue, settings)['submitted'] is False


@pytest.mark.parametrize('boundary', ['provider', 'robots', 'gateway'])
def test_live_pause_keeps_job_ready_without_reservation_or_http(queue, settings, boundary):
    submitted = submit(queue, settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(queue)
    if boundary == 'provider':
        http.provider_not_before[INEWS_PROVIDER] = time.time()+3600
    elif boundary == 'robots':
        http.blocked_robots_origins = lambda p: ('https://inews.setn.com',)
    else:
        http.blocked_gateway_hosts = lambda p: ('inews.setn.com',)
    assert diagnostic.claim_candidate(queue, http, settings, INEWS_PROVIDER) is None
    assert not http.calls and diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'ready'


def test_question_key_cannot_be_rearmed_by_layout_fingerprint_or_failure_counter(queue, settings):
    target = diagnostic._head(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    key = diagnostic._key(target, 'a'*64, evidence_contract_version=24)
    assert diagnostic._key({**target, 'attempts': 999}, 'b'*64, evidence_contract_version=24) == key
    assert all(diagnostic._key(target, layout, evidence_contract_version=24) == key for layout in INEWS_LAYOUT_VERSIONS)
    with pytest.raises(ValueError):
        diagnostic._key({**target, 'provider': 'videoland'}, 'a'*64, evidence_contract_version=24)


def test_owner_rechecks_exact_head_before_any_http(queue, settings):
    submitted = submit(queue, settings)
    diagnostic.pump_candidates(queue)
    with queue.db:
        queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (URL,))
    http = http_stub(queue)
    assert diagnostic.claim_candidate(queue, http, settings, INEWS_PROVIDER) is None
    assert not http.calls and diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'skipped'


def test_crashed_reserved_request_is_uncertain_and_never_automatically_retried(settings, tmp_path):
    owner = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        owner.add_articles([{'url': URL}])
        with owner.db:
            owner.db.execute('UPDATE articles SET state=?,error_code=?,due=?,last_checked_at_utc=? WHERE url=?',
                ('article_identity_not_verified', 'article_identity_not_verified', time.time()-60,
                 '2026-01-01T00:00:00Z', URL))
        submitted = submit(owner, settings)
        diagnostic.pump_candidates(owner)
        assert diagnostic.claim_candidate(owner, http_stub(owner), settings, INEWS_PROVIDER)
    finally:
        owner.close()
    recovered = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        diagnostic.pump_candidates(recovered)
        http = http_stub(recovered)
        assert diagnostic.claim_candidate(recovered, http, settings, INEWS_PROVIDER) is None
        assert diagnostic.candidate_status(tmp_path, submitted['request_id'])['state'] == 'uncertain_outcome'
        assert submit(recovered, settings)['submitted'] is False and not http.calls
    finally:
        recovered.close()


def test_cli_submit_and_status_use_mailbox_only(queue, settings, monkeypatch, tmp_path):
    from scripts import probe_news_sources as cli
    monkeypatch.setattr(cli, 'load_config', lambda p: (settings, queue.registry, tmp_path))
    monkeypatch.setattr(cli, 'NewsHTTP', lambda *a, **k: pytest.fail('no CLI HTTP owner'))
    output = tmp_path/'private/submitted.json'
    args = ['--config', str(tmp_path/'config.json'), '--output', str(output), '--probe', URL,
        '--require-current-missing-body-failure', '--failure-code', 'article_identity_not_verified',
        '--candidate-embedded-layout', LAYOUT, '--enqueue-candidate', '--candidate-evidence-contract', '24']
    before = head(queue)
    assert cli.main(args) == 0
    result = json.loads(output.read_bytes())
    assert result['submitted'] and result['publisher_requests'] == 0 and head(queue) == before
    assert cli.main(['--config', str(tmp_path/'config.json'), '--output', str(output),
        '--candidate-request-status', result['request_id']]) == 0
    assert json.loads(output.read_bytes())['state'] == 'pending' and head(queue) == before


@pytest.mark.parametrize('layout,evidence,enqueue', [(LAYOUT, '1', True), (LAYOUT, '24', False),
    ('videoland_visible_article_v4', '24', True)])
def test_cli_does_not_fall_back_to_standalone_or_wrong_provider_http(queue, settings, monkeypatch, tmp_path, layout, evidence, enqueue):
    from scripts import probe_news_sources as cli
    monkeypatch.setattr(cli, 'load_config', lambda p: (settings, queue.registry, tmp_path))
    monkeypatch.setattr(cli, 'NewsHTTP', lambda *a, **k: pytest.fail('no invalid-scope HTTP'))
    args = ['--output', str(tmp_path/'private/error.json'), '--probe', URL,
        '--require-current-missing-body-failure', '--failure-code', 'article_identity_not_verified',
        '--candidate-embedded-layout', layout, '--candidate-evidence-contract', evidence]
    if enqueue:
        args.append('--enqueue-candidate')
    with pytest.raises(SystemExit):
        cli.main(args)

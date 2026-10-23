"""Synthetic scoped-credit questions; no source bodies or network fixtures."""
from copy import deepcopy
import json
from types import SimpleNamespace
import time

from bs4 import BeautifulSoup
import pytest

from downloader import news_fulltext_candidate_diagnostics as jobs
from downloader.news_fulltext_candidates import candidate_response_metadata, native_candidate_source
from downloader.news_fulltext_http import RedirectHop, Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_watchmedia import watchmedia_credit_boundary_metadata
from test_news_watchmedia_original import URL, PROFILE, page, settings as original_settings, versioned


LAYOUT = 'watchmedia_visible_article_v3'


@pytest.fixture
def settings(tmp_path):
    result = versioned(original_settings.__wrapped__(), 3)
    result.update(root=str(tmp_path), min_free_disk_bytes=1, exclusive_http_owner=False, workers=2)
    result['adaptive_rate'] = {'enabled': False}
    return result


def missing_own_credit():
    soup = BeautifulSoup(page(), 'lxml')
    heading = soup.select_one('h1')
    heading.parent.parent.parent.select_one('[itemprop=author]').decompose()
    related = soup.select_one('.e-loop-item')
    related.clear()
    for i in range(9):
        wrapper = soup.new_tag('div', attrs={'class': 'elementor-widget-post-info'})
        credit = soup.new_tag('span', attrs={'itemprop': 'author'})
        credit.string = f'不應輸出的推薦署名{i}'
        wrapper.append(credit)
        related.append(wrapper)
    return soup


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_nine_recommendation_credits_never_become_own_author(parser):
    html = str(missing_own_credit())
    facts = watchmedia_credit_boundary_metadata(BeautifulSoup(html, parser), page_url=URL)
    assert facts['all_post_info_credit_count'] == facts['non_header_credit_count'] == 9
    assert facts['own_header_credit_count'] == 0
    assert facts['own_header_credit_sha256'] == []
    assert not facts['source_or_author_acceptance_granted']
    assert not facts['body_or_names_exported'] and facts['additional_requests'] == 0
    exported = json.dumps(facts, ensure_ascii=False)
    for private in ['推薦署名', '林合成', '負值零值', PROFILE]:
        assert private not in exported


def test_own_credit_and_person_are_separate_unaccepted_observations():
    soup = BeautifulSoup(page(), 'lxml')
    facts = watchmedia_credit_boundary_metadata(soup, page_url=URL)
    assert facts['own_header_credit_count'] == 1
    assert len(facts['own_header_credit_sha256'][0]) == 64
    author = facts['article_author_declarations'][0]
    assert author['matching_person_count'] == 1
    assert author['matching_person_name_present'] == [True]
    assert author['matching_person_works_for_publisher'] == [True]
    assert not facts['source_or_author_acceptance_granted']
    own = soup.select_one('h1').parent.parent.parent.select_one('[itemprop=author]')
    own.insert_after(deepcopy(own))
    assert watchmedia_credit_boundary_metadata(soup, page_url=URL)['own_header_credit_count'] == 2


def test_bad_or_unbounded_schema_does_not_export_original_error(monkeypatch):
    from downloader import news_fulltext_watchmedia as native
    soup = BeautifulSoup(page(), 'lxml')
    soup.select_one('script[type="application/ld+json"]').string = '{"private": broken'
    facts = watchmedia_credit_boundary_metadata(soup, page_url=URL)
    assert facts['schema_error'] and facts['article_author_declarations'] == []
    assert 'private' not in json.dumps(facts)
    monkeypatch.setattr(native, '_declarations', lambda doc: [{'@type': 'Person'}] * 2049)
    facts = watchmedia_credit_boundary_metadata(BeautifulSoup(page(), 'lxml'), page_url=URL)
    assert facts['schema_error'] and facts['person_schema_count'] == 0


def test_current_source_only_no_policy_change_or_self_comparison(settings, tmp_path, monkeypatch):
    from downloader import news_fulltext_candidates as candidate
    registry = PolicyRegistry(settings, base=tmp_path)
    source = registry.sources['watchmedia']
    before = deepcopy(settings)
    assert native_candidate_source(settings, registry, tmp_path, LAYOUT) is source
    assert settings == before
    monkeypatch.setattr(candidate, 'extract_article', lambda *a, **k: pytest.fail('must not replay current parser as its own candidate'))
    response = Response(URL, 200, {'content-type': 'text/html'}, str(missing_own_credit()).encode())
    facts = candidate_response_metadata(response, source, source)
    assert not facts['current_parser_replayed']
    assert facts['watchmedia_credit_boundary']['own_header_credit_count'] == 0
    assert not facts['accepted_as_article'] and not facts['raw_or_names_or_body_retained']
    assert 'body' not in facts and 'authors_json' not in facts
    denied = Response(URL, 403, {'content-type': 'text/html'}, b'no')
    with pytest.raises(NewsError):
        candidate_response_metadata(denied, source, source)
    huge = Response(URL, 200, {'content-type': 'text/html'}, b'x' * ((2 << 20) + 1))
    with pytest.raises(NewsError, match='candidate_response_exceeds_bound'):
        candidate_response_metadata(huge, source, source)


@pytest.mark.parametrize('change', ['disabled', 'old_layout', 'missing'])
def test_current_registration_is_required(settings, tmp_path, change):
    if change == 'disabled':
        settings['providers'][0]['enabled'] = False
    elif change == 'old_layout':
        settings['providers'][0]['embedded_article_layout'] = 'watchmedia_visible_article_v2'
    else:
        settings['providers'] = []
    with pytest.raises(ValueError, match='reviewed_current_watchmedia_source_required'):
        native_candidate_source(settings, PolicyRegistry(settings, base=tmp_path), tmp_path, LAYOUT)


def fail_head(queue, *, url=URL, **overrides):
    values = {'state': 'article_attribution_not_verified', 'error_code': 'article_attribution_not_verified',
        'attempts': 7, 'due': time.time()-30, 'last_checked_at_utc': '2026-01-01T00:00:00+00:00'}
    values.update(overrides)
    with queue.db:
        queue.add_articles([{'url': url}])
        queue.db.execute('UPDATE articles SET '+','.join(k+'=?' for k in values)+' WHERE url=?',
                         (*values.values(), url))
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())


def submit(queue, settings, reason='Review own-credit boundary only', *, url=URL, evidence=1):
    return jobs.submit_candidate(queue.root, settings, queue.registry, url, LAYOUT, reason=reason,
        evidence_contract_version=evidence)


def fake_http(queue):
    calls = []
    def get(url):
        calls.append(url)
        return Response(url, 200, {'content-type': 'text/html'}, str(missing_own_credit()).encode())
    return SimpleNamespace(registry=queue.registry, get=get, calls=calls, provider_not_before={},
        blocked_gateway_hosts=lambda p: (), blocked_robots_origins=lambda p: ())


def test_resident_once_only_observation_no_article_changes_after_restart(settings, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        before = fail_head(queue)
        request = submit(queue, settings)
        assert request['submitted'] and request['publisher_requests'] == 0
        jobs.pump_candidates(queue)
        http = fake_http(queue)
        job = jobs.claim_candidate(queue, http, settings, 'watchmedia')
        result = jobs.fetch_candidate(http, job)
        jobs.finish_candidate(queue, job, result=result)
        assert http.calls == [URL]
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert submit(queue, settings, reason='Changed reason does not buy a second GET')['state'] == 'observed'
    finally:
        queue.close()
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        assert submit(queue, settings)['state'] == 'observed'
        assert not submit(queue, settings)['submitted']
        jobs.pump_candidates(queue)
        assert jobs.claim_candidate(queue, fake_http(queue), settings, 'watchmedia') is None
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
    finally:
        queue.close()


def test_unknown_dispatch_outcome_is_not_automatically_retried(settings, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        fail_head(queue)
        submit(queue, settings)
        jobs.pump_candidates(queue)
        http = fake_http(queue)
        assert jobs.claim_candidate(queue, http, settings, 'watchmedia') is not None
    finally:
        queue.close()  # Crash after reservation, before result: deliberately unknown.
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        jobs.pump_candidates(queue)
        assert submit(queue, settings)['state'] == 'uncertain_outcome'
        assert jobs.claim_candidate(queue, fake_http(queue), settings, 'watchmedia') is None
        assert http.calls == []
    finally:
        queue.close()


@pytest.mark.parametrize('override', [
    {'due': time.time()+3600}, {'state': 'complete'}, {'lease_owner': 'another', 'lease_until': time.time()+3600},
    {'body_sha256': 'a'*64},
    {'state': 'paywall_or_login', 'error_code': 'paywall_or_login'},
])
def test_saved_leased_not_due_and_refusal_are_not_rearmed(settings, tmp_path, override):
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        fail_head(queue, **override)
        with pytest.raises(ValueError):
            submit(queue, settings)
        assert not (tmp_path/'reviewed_repairs/candidate_diagnostics/pending').exists()
    finally:
        queue.close()


@pytest.mark.parametrize('pause', ['origin', 'provider'])
def test_existing_policy_pause_prevents_diagnostic_dispatch(settings, tmp_path, pause):
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        fail_head(queue)
        submit(queue, settings)
        jobs.pump_candidates(queue)
        http = fake_http(queue)
        if pause == 'origin':
            http.blocked_robots_origins = lambda provider: {'https://www.watchmedia01.com'}
        else:
            http.provider_not_before['watchmedia'] = time.time()+300
        assert jobs.claim_candidate(queue, http, settings, 'watchmedia') is None
        assert http.calls == []
        assert queue.db.execute('SELECT state FROM native_candidate_jobs').fetchone()[0] == 'ready'
    finally:
        queue.close()


def test_cli_mailbox_submission_does_not_construct_http(settings, tmp_path, monkeypatch):
    from scripts import probe_news_sources as cli
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        before = fail_head(queue)
        monkeypatch.setattr(cli, 'load_config', lambda path: (settings, queue.registry, tmp_path))
        monkeypatch.setattr(cli, 'NewsHTTP', lambda *a, **k: pytest.fail('submitter must not own transport'))
        output = tmp_path/'submitted.json'
        assert cli.main(['--probe', URL, '--candidate-embedded-layout', LAYOUT,
            '--require-current-missing-body-failure', '--failure-code', 'article_attribution_not_verified',
            '--enqueue-candidate', '--output', str(output)]) == 0
        assert json.loads(output.read_text())['submitted']
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
    finally:
        queue.close()


LEGACY = 'https://www.watchmedia01.com/bnews-20210908043901.html'


def test_literal_legacy_fixed_question_dispatch_and_no_rearm_after_restart(settings, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        before = fail_head(queue, url=LEGACY)
        with pytest.raises(ValueError, match='fixed_evidence6'):
            submit(queue, settings, url=LEGACY)
        request = submit(queue, settings, url=LEGACY, evidence=6)
        jobs.pump_candidates(queue)
        http = fake_http(queue)
        calls = []
        def get(url):
            calls.append(url)
            return Response(URL, 200, {'content-type': 'text/html'}, str(missing_own_credit()).encode(),
                redirects=(RedirectHop(from_url=LEGACY, status=301, location=URL, to_url=URL,
                    observed_at_utc='2026-01-01T00:00:00+00:00', response_sha256='a'*64, response_bytes=0),))
        http.get = get
        job = jobs.claim_candidate(queue, http, settings, 'watchmedia')
        facts = jobs.fetch_candidate(http, job)
        assert facts['question'] == 'watchmedia_literal_legacy_credit_boundary_v1'
        assert facts['legacy_transport'] == {'requested_url': LEGACY, 'final_url': URL,
            'redirect_count': 1, 'identity_or_alias_accepted': False}
        assert facts['watchmedia_credit_boundary']['own_header_credit_count'] == 0
        jobs.finish_candidate(queue, job, result=facts)
        assert calls == [LEGACY]
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (LEGACY,)).fetchone()) == before
        assert request['request_id'] == submit(queue, settings, url=LEGACY, evidence=6)['request_id']
    finally:
        queue.close()
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        assert submit(queue, settings, url=LEGACY, evidence=6)['state'] == 'observed'
        assert not submit(queue, settings, url=LEGACY, evidence=6)['submitted']
    finally:
        queue.close()


@pytest.mark.parametrize('case', ['direct', '302', 'external', 'extra_hop', 'wrong_from', 'wrong_location'])
def test_legacy_question_never_relaxes_other_transport(settings, tmp_path, case):
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        fail_head(queue, url=LEGACY)
        submit(queue, settings, url=LEGACY, evidence=6)
        jobs.pump_candidates(queue)
        http = fake_http(queue)
        job = jobs.claim_candidate(queue, http, settings, 'watchmedia')
        final = 'https://external.invalid/archives/12345' if case == 'external' else URL
        hops = (RedirectHop(from_url=LEGACY if case != 'wrong_from' else URL, to_url=final,
            status=302 if case == '302' else 301, location=final if case != 'wrong_location' else LEGACY,
            observed_at_utc='2026-01-01T00:00:00+00:00', response_sha256='a'*64, response_bytes=0),)
        if case == 'direct':
            hops = ()
        if case == 'extra_hop':
            hops = hops + hops
        http.get = lambda url: Response(final, 200, {'content-type': 'text/html'}, page().encode(), redirects=hops)
        with pytest.raises(NewsError, match='candidate_literal_legacy_response_not_verified'):
            jobs.fetch_candidate(http, job)
    finally:
        queue.close()


def test_fixed_legacy_question_key_cannot_be_rearmed_by_parser_fingerprint():
    target = {'provider': 'watchmedia', 'id': 'a'*64}
    assert jobs._key(target, 'b'*64, evidence_contract_version=6) == jobs._key(target, 'c'*64, evidence_contract_version=6)

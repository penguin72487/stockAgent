"""Native attribution and frontier growth, independent of the old two samples."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import time
from types import SimpleNamespace

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_gvm import GVM_KNOWN_URL_LAYOUT
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config
from test_news_gvm_native import URL, ORIGIN, TEXT, entry, page, schema, policy


def current_source():
    return PolicyRegistry({'contract_version': 1, 'collection_purpose': 'personal_research',
        'publish': False, 'remote_training_export': False, 'providers': [known_entry()]},
        base=Path('.')).sources['gvm']


def test_current_boundary_metadata_locates_refusal_without_retaining_article():
    from downloader.news_fulltext_candidates import candidate_response_metadata
    from downloader.news_fulltext_http import Response
    source = current_source()
    soup = authored('PRIVATE_AUTHOR_SENTINEL')
    soup.select_one('.article-content').append(soup.new_tag('table'))
    facts = candidate_response_metadata(Response(URL, 200, {'content-type': 'text/html'}, str(soup).encode()),
        source, source, evidence_contract_version=34, requested_url=URL)
    boundary = facts['gvm_native_boundary']
    assert boundary['current_native_error'] == 'article_identity_not_verified'
    assert any(site['module'] == 'news_fulltext_gvm.py' and site['function'] == '_body_structure'
        for site in boundary['failure_location']['sites'])
    assert boundary['editor']['tag_counts']['table'] == 1
    assert not facts['accepted_as_article'] and not facts['raw_or_names_or_body_retained']
    assert facts['current_parser_replayed'] and facts['current_source_not_candidate']
    encoded = json.dumps(facts, ensure_ascii=False)
    assert TEXT not in encoded and 'PRIVATE_AUTHOR_SENTINEL' not in encoded and '合成測試標題' not in encoded


def test_editor_question_observes_rejecting_nodes_not_valid_ad_mounts():
    from downloader.news_fulltext_candidates import candidate_response_metadata
    from downloader.news_fulltext_http import Response
    source = current_source()
    soup = authored('PRIVATE_AUTHOR_SENTINEL')
    body = soup.select_one('.article-content')
    body.append(soup.new_tag('div', id='PRIVATE_ID_SENTINEL'))
    body.append(soup.new_tag('iframe', title='PRIVATE_TITLE_SENTINEL',
        src='https://www.youtube.com/embed/PRIVATE_PATH_SENTINEL?key=PRIVATE_QUERY_SENTINEL'))
    raw = str(soup).encode()
    facts = candidate_response_metadata(Response(URL,200,{'content-type':'text/html'},raw),
        source,source,evidence_contract_version=35,requested_url=URL)
    grammar = facts['gvm_editor_grammar']
    assert grammar['observed'] and grammar['classifier_shared_with_production']
    assert grammar['current_native_error']=='article_identity_not_verified'
    assert [n['tag'] for n in grammar['unsupported_nodes']]==['div','iframe']
    mount, frame = grammar['unsupported_nodes']
    assert mount['empty_static_leaf'] and mount['direct_body_child']
    assert frame['src_https_origin']=='https://www.youtube.com' and frame['src_embed_path']
    assert frame['src_has_query'] and frame['empty_static_leaf']
    assert not facts['accepted_as_article'] and not facts['raw_or_names_or_body_retained']
    encoded = json.dumps(facts,ensure_ascii=False)
    assert TEXT not in encoded and 'PRIVATE_' not in encoded and '合成測試標題' not in encoded
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract(soup)  # Observation is never an accepting parser repair.


def test_editor_observer_bounds_unknown_nodes_without_hiding_nonempty_text():
    from downloader.news_fulltext_gvm import gvm_editor_grammar_metadata
    soup = page()
    body = soup.select_one('.article-content')
    for _ in range(20):
        node = soup.new_tag('div')
        node.string = 'PRIVATE_BODY_SENTINEL'
        body.append(node)
    facts = gvm_editor_grammar_metadata(soup,page_url=URL)
    assert facts['observed'] and facts['unsupported_truncated']
    assert len(facts['unsupported_nodes'])==16
    assert all(n['text_characters']>0 and not n['empty_static_leaf'] for n in facts['unsupported_nodes'])
    assert 'PRIVATE_BODY_SENTINEL' not in json.dumps(facts)


@pytest.mark.parametrize('mutate,error',[
    (lambda s:s.select_one('.article-head').__setitem__('data-is-accessible','0'),'paywall_or_login'),
    (lambda s:s.select_one('.article-head').__setitem__('article_type','第三方供稿文章'),'article_rights_or_scope_excluded'),
    (lambda s:s.select_one('link[rel=canonical]').__setitem__('href',URL+'9'),'article_identity_not_verified'),
])
def test_editor_observer_does_not_inspect_prior_gate_failures(mutate,error,monkeypatch):
    from downloader.news_fulltext_gvm import gvm_editor_grammar_metadata
    soup=page()
    mutate(soup)
    monkeypatch.setattr('downloader.news_fulltext_gvm._unsupported_editor_nodes',
        lambda *a:pytest.fail('prior identity/access/credit gates must remain earlier'))
    facts=gvm_editor_grammar_metadata(soup,page_url=URL)
    assert not facts['observed'] and facts['current_native_error']==error
    assert 'unsupported_nodes' not in facts


def test_editor_observer_preserves_script_failure_without_exporting_script():
    from downloader.news_fulltext_gvm import gvm_editor_grammar_metadata
    soup=page()
    soup.select_one('.article-content script').string='PRIVATE_SCRIPT_SENTINEL'
    facts=gvm_editor_grammar_metadata(soup,page_url=URL)
    assert not facts['observed'] and facts['classification_error']=='article_identity_not_verified'
    assert 'PRIVATE_SCRIPT_SENTINEL' not in json.dumps(facts)


@pytest.mark.parametrize('changed', [
    {'known_url_only': False}, {'allowed': False}, {'domains': ('esg.gvm.com.tw',)},
    {'feeds': (ORIGIN + '/feed',)}, {'require_author_attribution': False},
    {'reviewed_article_subsites': ('esg.gvm.com.tw',)}, {'selectors': ('body',)},
])
def test_current_boundary_cannot_grant_a_changed_source(changed):
    from downloader.news_fulltext_candidates import candidate_evidence_allowed
    source = replace(current_source(), **changed)
    assert not candidate_evidence_allowed(source, source, 34)


@pytest.mark.parametrize('version', [1, 2, 27, 33, True, 36])
def test_gvm_current_boundary_has_one_fixed_question(version):
    from downloader.news_fulltext_candidates import candidate_evidence_allowed
    source = current_source()
    assert not candidate_evidence_allowed(source, source, version)


@pytest.mark.parametrize('url', [URL + '?x=1', URL + '#fragment', URL.replace('www.', 'esg.'),
                              URL.replace('https:', 'http:')])
def test_boundary_rejects_literal_route_changes(url):
    from downloader.news_fulltext_candidates import candidate_response_metadata
    from downloader.news_fulltext_http import Response
    source = current_source()
    with pytest.raises(NewsError, match='candidate_literal_response_not_verified'):
        candidate_response_metadata(Response(url, 200, {'content-type': 'text/html'}, str(page()).encode()),
            source, source, evidence_contract_version=34, requested_url=url)


@pytest.mark.parametrize('state,evidence', [('article_identity_not_verified',34),
    ('article_rights_or_scope_excluded',34),('article_identity_not_verified',35)])
def test_resident_gvm_question_uses_one_owner_request_and_preserves_failed_head(config, tmp_path, monkeypatch, state,evidence):
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    from downloader.news_fulltext_http import Response
    settings = deepcopy(config)
    settings['providers'] = [known_entry()]
    registry = PolicyRegistry(settings, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        queue.add_articles([{'url': URL}])
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=?,last_checked_at_utc=? WHERE url=?',
                (state, state, time.time()-60, '2026-01-01T00:00:00+00:00', URL))
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
            lambda *a, **k: pytest.fail('no second HTTP owner'))
        submitted = diagnostic.submit_candidate(tmp_path, settings, registry, URL, GVM_KNOWN_URL_LAYOUT,
            reason='Current native refusal question', evidence_contract_version=evidence)
        diagnostic.pump_candidates(queue)
        calls = []
        def get(url):
            calls.append(url)
            return Response(url, 200, {'content-type': 'text/html'}, str(page()).encode())
        http = SimpleNamespace(get=get, registry=registry, provider_not_before={},
            blocked_gateway_hosts=lambda _: (), blocked_robots_origins=lambda _: ())
        job = diagnostic.claim_candidate(queue, http, settings, 'gvm')
        assert job and job['source'] == job['candidate']
        result = diagnostic.fetch_candidate(http, job)
        diagnostic.finish_candidate(queue, job, result=result)
        assert calls == [URL]
        assert result['gvm_native_boundary' if evidence==34 else 'gvm_editor_grammar']['current_native_error'] is None
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
        again = diagnostic.submit_candidate(tmp_path, settings, registry, URL, GVM_KNOWN_URL_LAYOUT,
            reason='Another reason cannot retry', evidence_contract_version=evidence)
        assert not again['submitted'] and again['request_id'] == submitted['request_id']
        assert diagnostic.candidate_status(tmp_path, submitted['request_id'])['state'] == 'observed'
    finally:
        queue.close()


@pytest.mark.parametrize('evidence',[34,35])
def test_gvm_cli_enqueues_current_question_without_http(config, tmp_path, monkeypatch,evidence):
    from scripts import probe_news_sources as cli
    settings = deepcopy(config)
    settings.update(root='news', providers=[known_entry()])
    registry = PolicyRegistry(settings, base=tmp_path)
    monkeypatch.setattr(cli, 'load_config', lambda _: (settings, registry, tmp_path))
    monkeypatch.setattr(cli, 'require_current_missing_body_failures', lambda *a, **k: {})
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('the CLI must not start another HTTP owner'))
    calls = []
    def submit(*args, **kwargs):
        calls.append((args, kwargs))
        return {'submitted': True, 'request_id': 'a'*64}
    monkeypatch.setattr('downloader.news_fulltext_candidate_diagnostics.submit_candidate', submit)
    args = ['--probe', URL, '--candidate-evidence-contract', str(evidence), '--candidate-embedded-layout',
        GVM_KNOWN_URL_LAYOUT, '--require-current-missing-body-failure', '--failure-code',
        'article_identity_not_verified', '--output', str(tmp_path/'result.json')]
    with pytest.raises(SystemExit) as error:
        cli.main(args)
    assert error.value.code == 2 and not calls
    assert cli.main([*args, '--enqueue-candidate']) == 0
    assert len(calls) == 1 and calls[0][1]['evidence_contract_version'] == evidence


@pytest.mark.parametrize('minimum,idle,expected_required,expected_idle', [(25,60,25,60),(25.1,60.1,26,61)])
def test_disk_admission_sample_survives_inventory_status_without_extra_filesystem_work(
        config, tmp_path, monkeypatch, minimum, idle, expected_required, expected_idle):
    from downloader.news_fulltext import refresh_disk_admission
    registry = PolicyRegistry({**config, 'providers': [known_entry()]}, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    calls = []
    def disk_usage(path):
        calls.append(path)
        return SimpleNamespace(free=19)
    monkeypatch.setattr('downloader.news_fulltext.shutil.disk_usage', disk_usage)
    try:
        settings = {'min_free_disk_bytes': minimum, 'service_idle_max_seconds': idle}
        assert refresh_disk_admission(queue, settings)
        first = queue.status(activity='waiting_disk')
        second = queue.status(activity='inventorying')
        assert len(calls) == 1 and first['disk_admission'] == second['disk_admission']
        assert second['activity'] == 'inventorying' and not second['disk_admission']['fetch_allowed']
        assert second['disk_admission']['required_free_bytes'] == expected_required
        assert second['disk_admission']['automatic_recheck_seconds'] == expected_idle
        assert second['disk_admission']['shortfall_bytes'] == expected_required-19
        second['disk_admission']['free_bytes'] = 999
        assert queue.disk_admission['free_bytes'] == 19  # Status cannot mutate the owner's admission.
        monkeypatch.setattr('downloader.news_fulltext.shutil.disk_usage', lambda _: SimpleNamespace(free=expected_required-1))
        assert refresh_disk_admission(queue, settings)
        monkeypatch.setattr('downloader.news_fulltext.shutil.disk_usage', lambda _: SimpleNamespace(free=expected_required))
        assert not refresh_disk_admission(queue, settings)
        assert queue.disk_admission['fetch_allowed'] and queue.disk_admission['shortfall_bytes'] == 0
    finally:
        queue.close()


def known_entry():
    value = deepcopy(entry())
    value.update(acquisition_mode='continuous', embedded_article_layout=GVM_KNOWN_URL_LAYOUT,
                 private_known_url_scope='personal_noncommercial_originals_v1')
    value.pop('finite_article_urls')
    value.pop('permitted_author_names')
    value['authorization']['scope'] = 'Locally known public originals for private reading; no discovery or training/export.'
    return value


def authored(name):
    soup = page()
    soup.select_one('.article-head')['author'] = name
    soup.select_one('.article-head_author').string = name
    schema(soup, lambda value: value['author'][0].update(name=name))
    return BeautifulSoup(str(soup), 'lxml')


def extract(soup, source=None):
    return extract_article(str(soup).encode(), source or policy(known_entry()), page_url=URL)


@pytest.mark.parametrize('name', ['合成甲', 'Synthetic Writer', '合成編輯部'])
def test_visible_profile_header_schema_agreement_replaces_sample_name_whitelist(name):
    soup = authored(name)
    result = extract(soup)
    detail = json.loads(result['author_attribution_detail_json'])
    assert result['authors_json'] == json.dumps([name], ensure_ascii=False) and TEXT in result['body']
    assert detail['contract_version'] == 2 and detail['source_visible_credit'] == name
    assert detail['source_credit_role'] == 'publisher_declared_reporter'
    assert not detail['human_reporter_identity_verified'] and not detail['original_ownership_verified']
    assert not result['per_article_license_verified'] and not result['historical_point_in_time']
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(soup, policy(entry()))


@pytest.mark.parametrize('mutate', [
    lambda soup: soup.select_one('.article-head').__setitem__('author', '別個署名'),
    lambda soup: schema(soup, lambda value: value['author'][0].update(name='別個署名')),
    lambda soup: soup.select_one('.article-head_author').__setitem__('href', ORIGIN + '/author/999'),
    lambda soup: soup.select_one('.article-head').__setitem__('article_type', '轉載'),
])
def test_name_growth_does_not_relax_native_credit_agreement(mutate):
    soup = authored('合成作者')
    mutate(soup)
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(soup)


@pytest.mark.parametrize('name', ['中央社', '中央通訊社', '聯合新聞網', '經濟日報', 'Reuters', 'AFP'])
def test_matching_profile_and_schema_are_not_a_third_party_original_grant(name):
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(authored(name))


@pytest.mark.parametrize('notice', ['本文轉載自別家媒體', '全文轉載，原始作者另列', '（中央社記者合成測試）'])
def test_reprint_notice_is_not_silently_removed_to_accept_article(notice):
    soup = authored('合成作者')
    soup.select_one('.article-content > p').append(notice)
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(soup)


def test_advertorial_role_stays_distinct_and_unreviewed_sponsor_is_not_a_reporter():
    result = extract(page(advertorial=True))
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['source_credit_role'] == 'publisher_declared_advertorial'
    assert not detail['sponsored_content_is_independent_editorial']
    soup = authored('任意品牌')
    soup.select_one('.article-head')['article_type'] = '廣編'
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(soup)


@pytest.mark.parametrize('field,value', [
    ('feeds', [ORIGIN + '/feed']), ('sitemaps', [ORIGIN + '/sitemap.xml']),
    ('wordpress_apis', [ORIGIN + '/api']), ('finite_article_urls', [URL]),
    ('permitted_author_names', ['任意署名']), ('allow_reviewed_supplements', True),
    ('private_known_url_scope', ''), ('domains', ['esg.gvm.com.tw']),
    ('require_author_attribution', False), ('article_selectors', ['body']),
])
def test_known_url_mode_cannot_add_discovery_subsites_or_weaken_article_checks(field, value):
    candidate = known_entry()
    candidate[field] = value
    with pytest.raises(ValueError):
        policy(candidate)


@pytest.mark.parametrize('state,error', [
    ('retry', 'http_429'), ('paywall_or_login', 'paywall_or_login'),
    ('article_rights_or_scope_excluded', 'article_rights_or_scope_excluded'),
    ('body_not_found', 'body_not_found'),
    ('article_attribution_not_verified', 'article_attribution_not_verified'),
    ('article_title_not_verified', 'article_title_not_verified'),
])
def test_known_url_transition_keeps_complete_heads_refusals_due_and_old_cohort(config, tmp_path, monkeypatch, state, error):
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
                        lambda *args, **kwargs: pytest.fail('Queue reconciliation cannot fetch HTTP'))
    old = NewsQueue(tmp_path, PolicyRegistry({**config, 'providers': [entry()]}, base=tmp_path))
    with old.db:
        old.add_articles([{'url': URL}])
    old.sync_finite_cohorts()
    urls = [ORIGIN + '/article/' + str(i) for i in (12346, 12347, 12348)]
    with old.db:
        old.add_articles([{'url': url, 'title': '保留原始標題', 'date': '2020-01-02'} for url in urls])
        old.db.execute("UPDATE articles SET state='complete',body_sha256=?,body_bytes=100 WHERE url=?", ('a' * 64, URL))
        old.db.execute("UPDATE articles SET state='awaiting_authorization' WHERE url=?", (urls[1],))
        old.db.execute("UPDATE articles SET state=?,attempts=3,error_code=?,due=? WHERE url=?", (state, error, time.time() + 3600, urls[2]))
    before = {row['url']: dict(row) for row in old.db.execute('SELECT * FROM articles')}
    cohort = dict(old.db.execute('SELECT * FROM finite_cohorts').fetchone())
    old.close()
    current = NewsQueue(tmp_path, PolicyRegistry({**config, 'providers': [known_entry()]}, base=tmp_path))
    try:
        after = {row['url']: dict(row) for row in current.db.execute('SELECT * FROM articles')}
        assert after[URL] == before[URL] and after[urls[2]] == before[urls[2]]
        assert after[urls[0]] == {**before[urls[0]], 'state': 'pending'}
        assert after[urls[1]] == {**before[urls[1]], 'state': 'pending'}
        assert dict(current.db.execute('SELECT * FROM finite_cohorts').fetchone()) == cohort
    finally:
        current.close()

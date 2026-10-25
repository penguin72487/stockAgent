"""Use an actual refusal once; preserve completed heads and spent budgets."""
from copy import deepcopy
from contextlib import closing
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader import news_fulltext_gvm as native
from downloader.news_fulltext_candidates import candidate_evidence_allowed, candidate_response_metadata
from downloader.news_fulltext_diagnostics import FailureResponse, retain_identity_metadata
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_failure_identity_metadata import evidence
from test_news_fulltext import config
from test_news_gvm_known_urls import current_source
from test_news_gvm_media import media_page
from test_news_gvm_native import TEXT, URL
from test_news_gvm_remaining import media_source, prepared
from test_news_gvm_tables import table_entry


def table_source():
    return PolicyRegistry({'contract_version': 1, 'collection_purpose': 'personal_research',
        'publish': False, 'remote_training_export': False, 'providers': [table_entry()]}, base=Path('.')).sources['gvm']


def refused(*, early=False):
    soup = media_page()
    node = soup.new_tag('hr', id='PRIVATE_NODE_VALUE')
    soup.select_one('.article-content').append(node)
    if early:
        soup.select_one('link[rel=canonical]')['href'] = URL + '?PRIVATE_QUERY_VALUE'
    raw = Response(URL, 200, {'content-type': 'text/html'}, str(soup).encode())
    with pytest.raises(NewsError) as caught:
        native.gvm_public_native_article(soup, page_url=URL, version=4)
    return soup, raw, caught.value


@pytest.mark.parametrize('early', [False, True])
def test_received_context_observes_the_actual_frame_without_parser_or_classifier_replay(monkeypatch, early):
    _, _, error = refused(early=early)
    monkeypatch.setattr(native, '_unsupported_editor_nodes', lambda *a, **k: pytest.fail('no replay'))
    result = native.gvm_failure_context(error)
    assert result['native_frame_observed'] and result['native_parser_version'] == 4
    assert result['prior_header_identity_access_credit_clocks_passed'] == (not early)
    node = result['first_refusing_editor_node']
    assert node is None if early else node['tag'] == 'hr' and node['direct_body_child']
    assert not result['parser_replayed'] and result['additional_requests'] == 0
    assert not result['raw_or_names_or_body_retained'] and not result['accepted_as_article']
    encoded = json.dumps(result, ensure_ascii=False)
    assert 'PRIVATE_' not in encoded and TEXT not in encoded and '合成作者' not in encoded


def test_context_without_native_trace_does_not_invent_a_stage():
    result = native.gvm_failure_context(NewsError('article_identity_not_verified'))
    assert not result['native_frame_observed']
    assert not result['prior_header_identity_access_credit_clocks_passed']
    assert result['first_refusing_editor_node'] is None


@pytest.mark.parametrize('version', [1, 34, 35, 36, 37, 38, True])
def test_each_question_keeps_its_exact_parser_binding(version):
    for source, allowed in [(current_source(), {34, 35}), (media_source(), {36}), (table_source(), {37})]:
        assert candidate_evidence_allowed(source, source, version) == (type(version) is int and version in allowed)
        assert not candidate_evidence_allowed(source, replace(source, allowed=False), version)


def test_new_question_parses_once_and_keeps_only_received_failure_shapes(monkeypatch):
    _, raw, _ = refused()
    source, calls = table_source(), []
    original = native._unsupported_editor_nodes
    def once(*args, **kwargs):
        calls.append(kwargs['version'])
        return original(*args, **kwargs)
    monkeypatch.setattr(native, '_unsupported_editor_nodes', once)
    result = candidate_response_metadata(raw, source, source,
        evidence_contract_version=37, requested_url=URL)
    assert calls == [4] and result['question'] == native.GVM_V4_FAILURE_QUESTION
    assert result['gvm_received_failure_context']['native_frame_observed']
    assert result['gvm_received_failure_context']['first_refusing_editor_node']['tag'] == 'hr'
    assert result['failure_location']['sites'][-1]['function'] == '_body_structure'
    assert not result['accepted_as_article'] and not result['body_completeness_verified']
    assert 'PRIVATE_' not in json.dumps(result) and TEXT not in json.dumps(result, ensure_ascii=False)


@pytest.mark.parametrize('url', [URL + '?x=1', URL + '#fragment', URL.replace('www.', 'esg.'), URL.replace('https:', 'http:')])
def test_new_question_rejects_nonliteral_responses(url):
    source = table_source()
    with pytest.raises(NewsError, match='candidate_literal_response_not_verified'):
        candidate_response_metadata(Response(url, 200, {'content-type': 'text/html'}, str(media_page()).encode()),
            source, source, evidence_contract_version=37, requested_url=url)


def test_received_failure_uses_the_existing_daily_budget_and_never_reparses(tmp_path, monkeypatch):
    _, raw, error = refused()
    settings = {'contract_version': 1, 'collection_purpose': 'personal_research',
        'publish': False, 'remote_training_export': False, 'providers': [table_entry()]}
    registry = PolicyRegistry(settings, base=tmp_path)
    with closing(NewsQueue(tmp_path / 'store', registry)) as queue:
        queue.add_articles([{'url': URL}])
        row = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        source = registry.sources['gvm']
        context = FailureResponse(raw, source.provider, source.data_fingerprint, URL, '2026-10-23T17:00:00+00:00')
        monkeypatch.setattr(native, '_unsupported_editor_nodes', lambda *a, **k: pytest.fail('no replay'))
        for _ in range(3):
            retain_identity_metadata(queue, row, error, context)
        paths, rows = evidence(queue.root)
        assert len(rows) == 2 and all(r['contract_version'] == 2 for r in rows)
        assert all(r['facts']['gvm_received_failure_context']['first_refusing_editor_node']['tag'] == 'hr' for r in rows)
        assert all(r['facts']['failure_location']['sites'][-1]['function'] == '_body_structure' for r in rows)
        key = 'failure_identity_metadata_attempts:v1:gvm:20261023'
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0] == '2'
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == row
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert all('PRIVATE_' not in path.read_text() for path in paths)
    with closing(NewsQueue(tmp_path / 'store', registry)) as queue:
        retain_identity_metadata(queue, row, error, context)
        assert len(evidence(queue.root)[0]) == 2


def test_v4_resident_question_gets_once_and_preserves_entire_article_head(config, tmp_path, monkeypatch):
    settings = deepcopy(config)
    settings['providers'] = [table_entry()]
    registry = PolicyRegistry(settings, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        queue.add_articles([{'url': URL}])
        with queue.db:
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified', error_code='article_identity_not_verified',"
                "attempts=3,due=0,last_checked_at_utc='2026-01-01T00:00:00+00:00'")
        before = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a, **k: pytest.fail('no second HTTP owner'))
        request = diagnostic.submit_candidate(tmp_path, settings, registry, URL, native.GVM_TABLE_LAYOUT,
            reason='Actual V4 failure context', evidence_contract_version=37)
        diagnostic.pump_candidates(queue)
        calls = []
        def get(url):
            calls.append(url)
            return refused()[1]
        http = SimpleNamespace(get=get, registry=registry, provider_not_before={},
            blocked_gateway_hosts=lambda _: (), blocked_robots_origins=lambda _: ())
        job = diagnostic.claim_candidate(queue, http, settings, 'gvm')
        assert job and job['source'] == job['candidate']
        result = diagnostic.fetch_candidate(http, job)
        diagnostic.finish_candidate(queue, job, result=result)
        repeat = diagnostic.submit_candidate(tmp_path, settings, registry, URL, native.GVM_TABLE_LAYOUT,
            reason='Different words cannot replay the same question', evidence_contract_version=37)
        assert calls == [URL] and not repeat['submitted'] and repeat['request_id'] == request['request_id']
        assert diagnostic.candidate_status(tmp_path, request['request_id'])['state'] == 'observed'
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == before
    finally:
        queue.close()


@pytest.mark.parametrize('state', ['complete', 'article_rights_or_scope_excluded', 'paywall_or_login', 'body_not_found'])
def test_v4_question_cannot_probe_completed_or_unrelated_failures(config, tmp_path, state):
    settings, _, queue = prepared(config, tmp_path)
    queue.close()
    settings['providers'] = [table_entry()]
    registry = PolicyRegistry(settings, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?', (state, state, URL))
        with pytest.raises(ValueError):
            diagnostic.submit_candidate(tmp_path, settings, registry, URL, native.GVM_TABLE_LAYOUT,
                reason='Only an existing V4 identity refusal', evidence_contract_version=37)


def test_v4_cli_enqueues_without_another_http_owner(config, tmp_path, monkeypatch):
    from scripts import probe_news_sources as cli
    settings = {**config, 'root': 'news', 'providers': [table_entry()]}
    registry = PolicyRegistry(settings, base=tmp_path)
    monkeypatch.setattr(cli, 'load_config', lambda _: (settings, registry, tmp_path))
    monkeypatch.setattr(cli, 'require_current_missing_body_failures', lambda *a, **k: {})
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a, **k: pytest.fail('no HTTP'))
    calls = []
    def submit(*args, **kwargs):
        calls.append((args, kwargs))
        return {'submitted': True, 'request_id': 'a' * 64}
    monkeypatch.setattr(diagnostic, 'submit_candidate', submit)
    args = ['--probe', URL, '--candidate-evidence-contract', '37', '--candidate-embedded-layout',
        native.GVM_TABLE_LAYOUT, '--require-current-missing-body-failure', '--failure-code',
        'article_identity_not_verified', '--output', str(tmp_path / 'result.json')]
    with pytest.raises(SystemExit) as error:
        cli.main(args)
    assert error.value.code == 2 and not calls
    assert cli.main([*args, '--enqueue-candidate']) == 0
    assert len(calls) == 1 and calls[0][1]['evidence_contract_version'] == 37

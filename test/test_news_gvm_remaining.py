"""The remaining V3 failure is a new finite question, never an article retry."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import time
from types import SimpleNamespace

import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidates import candidate_evidence_allowed, candidate_response_metadata
from downloader.news_fulltext_gvm import GVM_MEDIA_LAYOUT, GVM_REMAINING_QUESTION
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config
from test_news_gvm_known_urls import current_source
from test_news_gvm_media import media_entry, media_page
from test_news_gvm_native import TEXT, URL


def media_source():
    return PolicyRegistry({'contract_version': 1, 'collection_purpose': 'personal_research',
        'publish': False, 'remote_training_export': False, 'providers': [media_entry()]},
        base=Path('.')).sources['gvm']


@pytest.mark.parametrize('evidence', [1, 2, 27, 33, 34, 35, 36, 37, True])
def test_question_versions_bind_the_exact_registered_parser(evidence):
    old, current = current_source(), media_source()
    assert candidate_evidence_allowed(old, old, evidence) == (type(evidence) is int and evidence in {34, 35})
    assert candidate_evidence_allowed(current, current, evidence) == (type(evidence) is int and evidence == 36)
    assert not candidate_evidence_allowed(old, current, evidence)


@pytest.mark.parametrize('change', [
    {'known_url_only': False}, {'allowed': False}, {'acquisition_mode': 'finite'},
    {'domains': ('esg.gvm.com.tw',)}, {'selectors': ('body',)},
    {'require_author_attribution': False}, {'feeds': ('https://www.gvm.com.tw/feed',)},
])
def test_question_cannot_change_the_private_source_scope(change):
    source = replace(media_source(), **change)
    assert not candidate_evidence_allowed(source, source, 36)


def test_remaining_metadata_uses_current_media_classifier_without_private_text():
    source = media_source()
    soup = media_page()
    table = soup.new_tag('table', id='PRIVATE_ID_SENTINEL')
    table.string = 'PRIVATE_TEXT_SENTINEL'
    soup.select_one('.article-content').append(table)
    result = candidate_response_metadata(Response(URL, 200, {'content-type': 'text/html'}, str(soup).encode()),
        source, source, evidence_contract_version=36, requested_url=URL)
    assert result['question'] == GVM_REMAINING_QUESTION
    assert not result['accepted_as_article'] and not result['raw_or_names_or_body_retained']
    assert result['current_parser_replayed'] and not result['identity_or_alias_accepted']
    for field in ('gvm_native_boundary', 'gvm_editor_grammar'):
        assert result[field]['native_parser_version'] == 3 and result[field]['contract_version'] == 2
        assert result[field]['current_native_error'] == 'article_identity_not_verified'
    grammar = result['gvm_editor_grammar']
    assert grammar['observed'] and [n['tag'] for n in grammar['unsupported_nodes']] == ['table']
    assert grammar['classifier_shared_with_production']
    encoded = json.dumps(result, ensure_ascii=False)
    assert TEXT not in encoded and 'PRIVATE_' not in encoded and '合成作者' not in encoded


def test_successful_current_replay_is_still_only_metadata():
    source = media_source()
    result = candidate_response_metadata(Response(URL, 200, {'content-type': 'text/html'}, str(media_page()).encode()),
        source, source, evidence_contract_version=36, requested_url=URL)
    assert result['gvm_editor_grammar']['current_native_error'] is None
    assert not result['gvm_editor_grammar']['observed']
    assert not result['accepted_as_article'] and not result['body_completeness_verified']


@pytest.mark.parametrize('url', [URL + '?x=1', URL + '#fragment', URL.replace('www.', 'esg.'),
    URL.replace('https:', 'http:')])
def test_remaining_question_requires_the_literal_original_response(url):
    source = media_source()
    with pytest.raises(NewsError, match='candidate_literal_response_not_verified'):
        candidate_response_metadata(Response(url, 200, {'content-type': 'text/html'}, str(media_page()).encode()),
            source, source, evidence_contract_version=36, requested_url=url)


def prepared(config, tmp_path):
    settings = deepcopy(config)
    settings['providers'] = [media_entry()]
    registry = PolicyRegistry(settings, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    queue.add_articles([{'url': URL}])
    with queue.db:
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified', "
            "error_code='article_identity_not_verified', attempts=2, due=?, last_checked_at_utc=? WHERE url=?",
            (time.time() - 60, '2026-01-01T00:00:00+00:00', URL))
    return settings, registry, queue


def test_resident_remaining_question_gets_once_and_keeps_the_entire_failed_head(config, tmp_path, monkeypatch):
    settings, registry, queue = prepared(config, tmp_path)
    try:
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
            lambda *a, **k: pytest.fail('no second HTTP owner'))
        request = diagnostic.submit_candidate(tmp_path, settings, registry, URL, GVM_MEDIA_LAYOUT,
            reason='Remaining V3 native refusal', evidence_contract_version=36)
        diagnostic.pump_candidates(queue)
        calls = []
        def get(url):
            calls.append(url)
            return Response(url, 200, {'content-type': 'text/html'}, str(media_page()).encode())
        http = SimpleNamespace(get=get, registry=registry, provider_not_before={},
            blocked_gateway_hosts=lambda _: (), blocked_robots_origins=lambda _: ())
        job = diagnostic.claim_candidate(queue, http, settings, 'gvm')
        assert job and job['source'] == job['candidate']
        result = diagnostic.fetch_candidate(http, job)
        diagnostic.finish_candidate(queue, job, result=result)
        again = diagnostic.submit_candidate(tmp_path, settings, registry, URL, GVM_MEDIA_LAYOUT,
            reason='A different reason cannot replay it', evidence_contract_version=36)
        assert calls == [URL] and not again['submitted'] and again['request_id'] == request['request_id']
        assert diagnostic.candidate_status(tmp_path, request['request_id'])['state'] == 'observed'
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
    finally:
        queue.close()


@pytest.mark.parametrize('state', ['complete', 'article_rights_or_scope_excluded', 'paywall_or_login', 'body_not_found'])
def test_remaining_question_rejects_completed_or_different_failures(config, tmp_path, state):
    settings, registry, queue = prepared(config, tmp_path)
    try:
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?', (state, state, URL))
        with pytest.raises(ValueError):
            diagnostic.submit_candidate(tmp_path, settings, registry, URL, GVM_MEDIA_LAYOUT,
                reason='Remaining V3 native refusal', evidence_contract_version=36)
    finally:
        queue.close()


def test_remaining_cli_only_enqueues_the_current_question(config, tmp_path, monkeypatch):
    from scripts import probe_news_sources as cli
    settings = deepcopy(config)
    settings.update(root='news', providers=[media_entry()])
    registry = PolicyRegistry(settings, base=tmp_path)
    monkeypatch.setattr(cli, 'load_config', lambda _: (settings, registry, tmp_path))
    monkeypatch.setattr(cli, 'require_current_missing_body_failures', lambda *a, **k: {})
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('CLI cannot start an HTTP owner'))
    calls = []
    def submit(*args, **kwargs):
        calls.append((args, kwargs))
        return {'submitted': True, 'request_id': 'a' * 64}
    monkeypatch.setattr(diagnostic, 'submit_candidate', submit)
    args = ['--probe', URL, '--candidate-evidence-contract', '36', '--candidate-embedded-layout',
        GVM_MEDIA_LAYOUT, '--require-current-missing-body-failure', '--failure-code',
        'article_identity_not_verified', '--output', str(tmp_path / 'result.json')]
    with pytest.raises(SystemExit) as error:
        cli.main(args)
    assert error.value.code == 2 and not calls
    assert cli.main([*args, '--enqueue-candidate']) == 0
    assert len(calls) == 1 and calls[0][1]['evidence_contract_version'] == 36

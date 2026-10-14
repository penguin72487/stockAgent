"""A stale raw failure never licenses another request for a saved body."""
from datetime import datetime, timezone
from dataclasses import replace
import json
import time

import pytest

from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from scripts.probe_news_sources import require_current_missing_body_failures, select_current_failure_urls, main
from test_news_fulltext import config


URL = 'https://publisher.test/article/123'


@pytest.fixture
def queue(config, tmp_path):
    value = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    with value.db:
        value.add_articles([{'url': URL}])
        value.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found',last_checked_at_utc=? WHERE url=?",
                         (datetime.now(timezone.utc).isoformat(), URL))
    yield value
    value.close()


def test_due_failed_missing_body_gets_pinned_read_only_grant(queue):
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    proof = require_current_missing_body_failures(queue.root, [URL], queue.registry)
    assert len(proof) == 1 and proof[0]['body_sha256'] is None
    assert proof[0]['last_checked_at_utc'] == before['last_checked_at_utc']
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before


@pytest.mark.parametrize('code', ['article_attribution_not_verified', 'article_title_not_verified', 'article_identity_not_verified'])
def test_attribution_failure_requires_explicit_exact_code_and_keeps_original_head(queue, code):
    with queue.db:
        queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?', (code, code, URL))
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    with pytest.raises(ValueError, match='probe_no_longer_a_due_failed_missing_body'):
        require_current_missing_body_failures(queue.root, [URL], queue.registry)
    assert select_current_failure_urls(queue.root, 'publisher', queue.registry,
                                       maximum=1, failure_code=code) == [URL]
    assert require_current_missing_body_failures(queue.root, [URL], queue.registry,
                                                 failure_code=code) == [{k: before[k] for k in (
        'provider', 'url', 'state', 'error_code', 'body_sha256', 'due', 'last_checked_at_utc', 'lease_owner', 'lease_until')}]
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before


@pytest.mark.parametrize('code', ['article_attribution_not_verified', 'article_title_not_verified', 'article_identity_not_verified'])
@pytest.mark.parametrize('changes', [
    {'state': 'complete'}, {'body_sha256': 'a'*64}, {'state': 'pending'}, {'state': 'inflight'},
    {'due': 1e12}, {'lease_owner': 'other'}, {'lease_until': 1e12},
    {'error_code': 'http_403'}, {'error_code': 'http_429'}, {'error_code': 'paywall_or_login'},
    {'last_checked_at_utc': None},
])
def test_attribution_failure_diagnosis_never_grants_saved_refused_future_or_leased_work(queue, changes, code):
    with queue.db:
        queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?', (code, code, URL))
        for field, value in changes.items():
            queue.db.execute(f'UPDATE articles SET {field}=? WHERE url=?', (value, URL))
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    with pytest.raises(ValueError, match='probe_no_longer_a_due_failed_missing_body'):
        require_current_missing_body_failures(queue.root, [URL], queue.registry,
                                             failure_code=code)
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before


@pytest.mark.parametrize('code', ['http_403', 'http_429', 'paywall_or_login', 'transport_error', None])
def test_failure_probe_code_cannot_become_an_arbitrary_retry_grant(queue, code):
    with pytest.raises(ValueError, match='unsupported_missing_article_failure_probe'):
        require_current_missing_body_failures(queue.root, [URL], queue.registry, failure_code=code)
    with pytest.raises(ValueError, match='unsupported_missing_article_failure_probe'):
        select_current_failure_urls(queue.root, 'publisher', queue.registry, failure_code=code)


@pytest.mark.parametrize('code', ['body_not_found', 'article_attribution_not_verified', 'article_title_not_verified', 'article_identity_not_verified'])
def test_future_failure_observation_cannot_grant_a_request_even_with_due_zero(queue, code):
    future = datetime.fromtimestamp(time.time()+600, timezone.utc).isoformat()
    with queue.db:
        queue.db.execute('UPDATE articles SET state=?,error_code=?,last_checked_at_utc=?,due=0 WHERE url=?',
                         (code, code, future, URL))
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    with pytest.raises(ValueError, match='probe_failure_observation_clock_not_verified'):
        require_current_missing_body_failures(queue.root, [URL], queue.registry, failure_code=code)
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before


@pytest.mark.parametrize('code', ['article_attribution_not_verified', 'article_title_not_verified', 'article_identity_not_verified'])
@pytest.mark.parametrize('flags', [[], ['--retain-reviewed-response', '--require-current-missing-body-failure']])
def test_attribution_cli_requires_pinned_current_head_and_metadata_only_before_any_http(queue, monkeypatch, flags, code):
    from scripts import probe_news_sources as probes
    monkeypatch.setattr(probes, 'NewsHTTP', lambda *_a, **_k: pytest.fail('invalid grant created HTTP owner'))
    with pytest.raises(SystemExit):
        main(['--probe', URL, '--failure-code', code,
              '--output', str(queue.root/'invalid-attribution.json'), *flags])
    assert not (queue.root/'invalid-attribution.json').exists()


@pytest.mark.parametrize('code', ['article_attribution_not_verified', 'article_title_not_verified', 'article_identity_not_verified'])
def test_explicit_author_diagnosis_uses_one_request_and_never_accepts_or_retains_body(queue, monkeypatch, capsys, code):
    from types import SimpleNamespace
    from downloader.news_fulltext_http import Response
    from downloader.news_fulltext_policy import NewsError
    from scripts import probe_news_sources as probes
    with queue.db:
        queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?', (code, code, URL))
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    calls, closed = [], []
    def get(url):
        calls.append(url)
        return Response(url, 200, {'content-type': 'text/html'},
                        b'<html><body><article><h1>Public headline</h1>'
                        b'<p>BODY SHOULD NEVER BE RETAINED OR PRINTED</p></article></body></html>')
    def reject_author(*_args, **_kwargs):
        raise NewsError(code)
    fake = SimpleNamespace(registry=queue.registry, provider_not_before={}, robot_delays={},
        metrics=SimpleNamespace(snapshot=lambda: {}), get=get, close=lambda: closed.append(True))
    monkeypatch.setattr(probes, 'load_config', lambda _p: ({'root': '.', 'workers': 1}, queue.registry, queue.root))
    monkeypatch.setattr(probes, 'NewsHTTP', lambda *_a, **_k: fake)
    monkeypatch.setattr(probes, 'extract_article', reject_author)
    output = queue.root/'author-metadata.json'
    assert main(['--probe', URL, '--failure-code', code,
                 '--require-current-missing-body-failure', '--output', str(output)]) == 0
    result = json.loads(output.read_text())
    assert calls == [URL] and closed == [True]
    assert result['failure_probe_code'] == code
    assert result['probes'][0]['extraction_error'] == code
    assert result['current_failed_missing_body_preflight'][0]['last_checked_at_utc'] == before['last_checked_at_utc']
    assert 'BODY SHOULD' not in output.read_text() and 'BODY SHOULD' not in capsys.readouterr().out
    assert not list(queue.root.rglob('*.parquet')) and not (queue.root/'private-source-diagnostics').exists()
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before


def test_parser_location_never_exports_traceback_message_locals_or_untrusted_paths():
    from downloader.news_fulltext_policy import NewsError
    from scripts.probe_news_sources import extraction_failure_location
    private_value = 'untrusted secret query and body text'
    try:
        raise NewsError(private_value)
    except NewsError as error:
        result = extraction_failure_location(error)
    assert result['sites'] == []
    assert not result['source_text_or_locals_exported']
    assert private_value not in json.dumps(result)
    assert str(__file__) not in json.dumps(result)


@pytest.mark.parametrize('change', ['wrong_canonical', 'wrong_id', 'wrong_og'])
def test_actual_native_identity_error_has_code_sites_not_source_prose(tmp_path, change):
    from test_news_cnyes_canonical_alias import MOBILE, TITLE, AUTHOR, mobile_page
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_policy import NewsError, extract_article
    from scripts.probe_news_sources import extraction_failure_location
    # Use the production registry rather than a synthetic filename/traceback.
    from downloader.news_fulltext import load_config
    from pathlib import Path
    _cfg, registry, _repo = load_config(Path('configs/news_fulltext.json'))
    source = registry.sources['cnyes']
    payload = mobile_page(changes={'newsId': 43}) if change == 'wrong_id' else mobile_page()
    if change != 'wrong_id':
        soup = BeautifulSoup(payload, 'lxml')
        selector, attribute = ('link[rel="canonical"]', 'href') if change == 'wrong_canonical' else (
            'meta[property="og:url"]', 'content')
        soup.select_one(selector)[attribute] = MOBILE + '0'
        payload = str(soup).encode()
    with pytest.raises(NewsError, match='article_identity_not_verified') as caught:
        extract_article(payload, source, page_url=MOBILE)
    result = extraction_failure_location(caught.value)
    assert result['sites'][-1]['module'] == 'news_fulltext_publishers.py'
    assert result['sites'][-1]['function'] == '_cnyes_native_article_identity'
    assert result['sites'][-1]['line'] > 0 and len(result['sites']) <= 8
    assert result['line_numbers_are_code_generation_local'] and not result['source_text_or_locals_exported']
    encoded = json.dumps(result, ensure_ascii=False)
    assert TITLE not in encoded and AUTHOR not in encoded and MOBILE not in encoded
    assert str(tmp_path) not in encoded and '/root/' not in encoded


@pytest.mark.parametrize('kind', ['paragraph_hidden', 'container_hidden', 'registered_ad_hidden'])
def test_trusted_visibility_failure_exports_flags_not_hidden_text_attrs_or_links(kind):
    from bs4 import BeautifulSoup
    from downloader.news_fulltext import load_config
    from downloader.news_fulltext_policy import NewsError, extract_article
    from pathlib import Path
    from scripts.probe_news_sources import extraction_failure_location
    from test_news_cnyes_canonical_alias import MOBILE, mobile_page
    _cfg, registry, _repo = load_config(Path('configs/news_fulltext.json'))
    soup = BeautifulSoup(mobile_page(), 'lxml')
    container = soup.select_one('#article-container')
    private_text = 'PRIVATE HIDDEN AD TEXT'
    if kind == 'registered_ad_hidden':
        ad = BeautifulSoup('<div id="div-gpt-ad-cnyes_news_article_42" hidden><p>'
                           + private_text + '</p></div>', 'lxml').div
        container.append(ad)
    elif kind == 'container_hidden':
        container['hidden'] = ''
    else:
        container.p['hidden'] = ''
    with pytest.raises(NewsError, match='article_identity_not_verified') as caught:
        extract_article(str(soup).encode(), registry.sources['cnyes'], page_url=MOBILE)
    value = extraction_failure_location(caught.value)
    flags = value['visibility']
    assert value['contract_version'] == 2 and flags['refusing_hidden']
    assert flags['within_registered_cnyes_ad_slot'] is (kind == 'registered_ad_hidden')
    assert flags['candidate_tag'] == ('div' if kind == 'container_hidden' else 'p')
    assert flags['candidate_text_characters'] > 0
    encoded = json.dumps(value, ensure_ascii=False)
    assert private_text not in encoded and 'div-gpt-ad-cnyes_news_article_42' not in encoded
    assert MOBILE not in encoded and 'visibility:hidden' not in encoded


@pytest.mark.parametrize('column,value', [('body_sha256', 'a' * 64), ('state', 'complete'),
    ('state', 'pending'), ('state', 'inflight'), ('state', 'waiting_cohort'),
    ('state', 'source_article_empty_template'), ('error_code', 'http_403'),
    ('error_code', 'http_429'), ('error_code', 'paywall_or_login'),
    ('last_checked_at_utc', None), ('lease_owner', 'other'), ('lease_until', 1e12), ('provider', 'other')])
def test_saved_refused_leased_or_unobserved_heads_cancel_probe(queue, column, value):
    with queue.db:
        queue.db.execute(f'UPDATE articles SET {column}=? WHERE url=?', (value, URL))
    with pytest.raises(ValueError, match='probe_no_longer_a_due_failed_missing_body'):
        require_current_missing_body_failures(queue.root, [URL], queue.registry)


@pytest.mark.parametrize('clock', ['invalid', '2026-10-10', '2026-10-10T00:00:00'])
def test_probe_cannot_invent_the_previous_failure_clock(queue, clock):
    with queue.db:
        queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', (clock, URL))
    with pytest.raises(ValueError, match='probe_failure_observation_clock_not_verified'):
        require_current_missing_body_failures(queue.root, [URL], queue.registry)


def test_future_per_url_deadline_is_not_overridden_by_failure_probe(queue):
    deadline = time.time() + 600
    with queue.db:
        queue.db.execute("UPDATE articles SET state='retry_cooldown',due=? WHERE url=?", (deadline, URL))
    with pytest.raises(ValueError, match='probe_no_longer_a_due_failed_missing_body'):
        require_current_missing_body_failures(queue.root, [URL], queue.registry)
    assert queue.db.execute('SELECT due FROM articles WHERE url=?', (URL,)).fetchone()[0] == deadline


def test_missing_or_oversized_selection_cannot_make_source_requests(queue):
    for urls in ([], [URL] * 17, [URL, URL], ['https://publisher.test/not-registered']):
        with pytest.raises(ValueError):
            require_current_missing_body_failures(queue.root, urls, queue.registry)


def test_dynamic_selection_reads_all_failure_lanes_without_changing_queue(queue):
    clock = datetime.now(timezone.utc).isoformat()
    with queue.db:
        for lane in ('retry', 'retry_cooldown', 'body_not_found'):
            for number in range(3):
                url = f'https://publisher.test/article/{lane}-{number}'
                queue.add_articles([{'url': url}])
                queue.db.execute('UPDATE articles SET state=?,error_code=?,last_checked_at_utc=?,due=? WHERE url=?',
                                 (lane, 'body_not_found', clock, 1 + number, url))
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    selected = select_current_failure_urls(queue.root, 'publisher', queue.registry, maximum=16)
    proof = require_current_missing_body_failures(queue.root, selected, queue.registry)
    assert len(selected) == 6 and len(set(selected)) == 6
    assert all(sum(row['state'] == lane for row in proof) == 2
               for lane in ('retry', 'retry_cooldown', 'body_not_found'))
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')] == before


@pytest.mark.parametrize('maximum', [1, 2, 4])
def test_dynamic_selection_honors_its_bound(queue, maximum):
    selected = select_current_failure_urls(queue.root, 'publisher', queue.registry, maximum=maximum)
    assert 1 <= len(selected) <= maximum and selected == [URL]


@pytest.mark.parametrize('maximum', [0, 17, 1.0, True, None])
def test_invalid_dynamic_probe_bounds_cancel_before_http(queue, maximum):
    with pytest.raises(ValueError, match='bounded_enabled_ongoing_failure_provider_required'):
        select_current_failure_urls(queue.root, 'publisher', queue.registry, maximum=maximum)


@pytest.mark.parametrize('change', [{'allowed': False}, {'acquisition_mode': 'finite'}])
def test_dynamic_selection_cannot_expand_disabled_or_finite_scope(queue, change):
    queue.registry.sources['publisher'] = replace(queue.registry.sources['publisher'], **change)
    with pytest.raises(ValueError, match='bounded_enabled_ongoing_failure_provider_required'):
        select_current_failure_urls(queue.root, 'publisher', queue.registry)


@pytest.mark.parametrize('changes', [{'state': 'complete'}, {'body_sha256': 'a' * 64},
    {'due': 1e12}, {'lease_owner': 'other'}, {'error_code': 'http_403'}, {'error_code': 'http_429'}])
def test_dynamic_selection_excludes_saved_refused_not_due_or_leased_work(queue, changes):
    with queue.db:
        for field, value in changes.items():
            queue.db.execute(f'UPDATE articles SET {field}=? WHERE url=?', (value, URL))
    with pytest.raises(ValueError, match='no_due_failed_missing_body_to_probe'):
        select_current_failure_urls(queue.root, 'publisher', queue.registry)


def test_dynamic_index_seek_does_not_scan_the_saved_or_pending_frontier(queue):
    plan = ' '.join(row[3] for row in queue.db.execute(
        "EXPLAIN QUERY PLAN SELECT url FROM articles INDEXED BY dispatch WHERE provider=? AND state=? "
        "AND error_code='body_not_found' AND body_sha256 IS NULL AND due<=? AND last_checked_at_utc IS NOT NULL "
        "AND lease_owner IS NULL AND lease_until IS NULL ORDER BY due LIMIT ?",
        ('publisher', 'retry', time.time(), 2)))
    assert 'SEARCH articles USING INDEX dispatch' in plan
    assert 'SCAN articles' not in plan and 'TEMP B-TREE' not in plan


@pytest.mark.parametrize('changes', [{'state': 'complete'}, {'due': 1e12},
    {'error_code': 'http_403'}, {'error_code': 'http_429'}, {'lease_owner': 'other'}])
def test_no_due_dynamic_cli_saves_explicit_skip_without_http_or_mutation(queue, monkeypatch, changes):
    from scripts import probe_news_sources as probes
    with queue.db:
        for field, value in changes.items():
            queue.db.execute(f'UPDATE articles SET {field}=? WHERE url=?', (value, URL))
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    monkeypatch.setattr(probes, 'load_config', lambda _path: ({'root': '.', 'workers': 1}, queue.registry, queue.root))
    monkeypatch.setattr(probes, 'NewsHTTP', lambda *_a, **_k: pytest.fail('empty selection created HTTP owner'))
    output = queue.root / 'probe-result.json'
    assert main(['--current-failure-provider', 'publisher', '--output', str(output)]) == 0
    result = json.loads(output.read_text())
    assert result['diagnostic_status_contract_version'] == 1
    assert result['state'] == 'skipped_no_due_failed_missing_body' and result['probes'] == []
    assert result['publisher_requests'] == 0 and not result['source_states_mutated']
    assert not result['all_history_complete'] and not result['publish']
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before


@pytest.mark.parametrize('maximum', ['0', '17'])
def test_no_due_cli_does_not_mask_invalid_scope_or_bounds(queue, monkeypatch, maximum):
    from scripts import probe_news_sources as probes
    monkeypatch.setattr(probes, 'load_config', lambda _path: ({'root': '.', 'workers': 1}, queue.registry, queue.root))
    monkeypatch.setattr(probes, 'NewsHTTP', lambda *_a, **_k: pytest.fail('invalid selection created HTTP owner'))
    with pytest.raises(ValueError, match='bounded_enabled_ongoing_failure_provider_required'):
        main(['--current-failure-provider', 'publisher', '--maximum-failure-probes', maximum,
              '--output', str(queue.root / 'invalid-probe.json')])
    assert not (queue.root / 'invalid-probe.json').exists()


@pytest.mark.parametrize('changes', [{'state': 'complete'}, {'due': 1e12},
    {'error_code': 'http_403'}, {'error_code': 'http_429'}, {'lease_owner': 'other'}])
def test_explicit_handoff_race_records_skip_without_request_or_reset(queue, monkeypatch, changes):
    from scripts import probe_news_sources as probes
    with queue.db:
        for field, value in changes.items():
            queue.db.execute(f'UPDATE articles SET {field}=? WHERE url=?', (value, URL))
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    monkeypatch.setattr(probes, 'load_config', lambda _path: ({'root': '.', 'workers': 1}, queue.registry, queue.root))
    monkeypatch.setattr(probes, 'NewsHTTP', lambda *_a, **_k: pytest.fail('canceled explicit grant created HTTP owner'))
    output = queue.root / 'explicit-skip.json'
    assert main(['--probe', URL, '--require-current-missing-body-failure', '--output', str(output)]) == 0
    result = json.loads(output.read_text())
    assert result['state'] == 'skipped_no_longer_due_failed_missing_body'
    assert result['explicit_selected_urls'] == [URL] and result['probes'] == []
    assert result['publisher_requests'] == 0 and not result['source_states_mutated']
    assert not result['all_history_complete']
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before


def test_explicit_skip_does_not_mask_bad_clocks_or_duplicate_requests(queue, monkeypatch):
    from scripts import probe_news_sources as probes
    monkeypatch.setattr(probes, 'load_config', lambda _path: ({'root': '.', 'workers': 1}, queue.registry, queue.root))
    monkeypatch.setattr(probes, 'NewsHTTP', lambda *_a, **_k: pytest.fail('invalid grant created HTTP owner'))
    output = queue.root / 'invalid-explicit-skip.json'
    with pytest.raises(ValueError, match='bounded_failure_probe_selection_required'):
        main(['--probe', URL, '--probe', URL, '--require-current-missing-body-failure', '--output', str(output)])
    with queue.db:
        queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', ('invalid', URL))
    with pytest.raises(ValueError, match='probe_failure_observation_clock_not_verified'):
        main(['--probe', URL, '--require-current-missing-body-failure', '--output', str(output)])
    assert not output.exists()

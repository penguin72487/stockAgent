"""Failed responses are private diagnostics, never successful body receipts."""
from __future__ import annotations

from dataclasses import replace
from contextlib import closing
import html
import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_diagnostics as diagnostics
from downloader.artifact_io import sha256_file
from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, utc_now
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import URL, short_page
from test_news_cnyes_canonical_alias import MOBILE, FIZZ_COMPLETION, mobile_page, streamed_mobile_page
from test_news_fulltext import config as cfg
from test_news_globalvoices_small_body import URL as GV_URL, page as gv_page


def fail(queue, raw, *, code='body_not_found', stage=None, received_at_utc=None):
    with queue.db:
        queue.add_articles([{'url': raw.url}])
        # Synthetic repeated observations test the diagnostic daily budget;
        # production rechecks must still respect the queue's new body delay.
        queue.db.execute('UPDATE articles SET due=0 WHERE url=?', (raw.url,))
    row = queue.claim_url(raw.url, 300, refresh=True)
    source = queue.registry.require(raw.url)
    error = NewsError(code)
    if stage is not None:
        error.cnyes_native_body_failure_stage = stage
    error.failure_response = diagnostics.FailureResponse(raw, source.provider, source.data_fingerprint, raw.url,
                                                        received_at_utc or utc_now())
    queue.failed(row, error, 6)
    return row


def reports(root):
    paths = sorted((root / 'diagnostics').rglob('*.parquet'))
    return paths, [pq.read_table(path).to_pylist()[0] for path in paths]


def test_free_public_cnyes_failed_raw_has_durable_private_receipt_and_original_failure(cnyes_config, tmp_path):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    raw = Response(URL, 200, {'content-type': 'text/html', 'set-cookie': 'private-session', 'authorization': 'private-key'},
                   short_page(visible='公開內容與宣告不一致，需要查核。'))
    with closing(NewsQueue(tmp_path / 'store', registry)) as queue:
        fail(queue, raw)
        paths, rows = reports(queue.root)
        assert len(rows) == 1 and rows[0]['raw_response_body'] == raw.body
        assert rows[0]['raw_response_retained'] and not rows[0]['accepted_as_article']
        assert not rows[0]['publish'] and not rows[0]['remote_training_export']
        receipt = json.loads(paths[0].with_suffix('.receipt.json').read_text())
        assert receipt['sha256'] == sha256_file(paths[0]) and receipt['diagnostic_only']
        assert 'private-' not in receipt['headers_json']
        state = queue.db.execute('SELECT state,error_code,body_sha256 FROM articles WHERE url=?', (URL,)).fetchone()
        assert tuple(state) == ('retry', 'body_not_found', None)
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0


@pytest.mark.parametrize('change', [{'payment': 1}, {'payment': False}, {'publishAt': 1}, {'content': '$3f'}])
def test_unverified_public_scope_keeps_only_metadata(cnyes_config, tmp_path, change):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path / 'store', registry)) as queue:
        fail(queue, Response(URL, 200, {'content-type': 'text/html'}, short_page(change=change)))
        _, rows = reports(queue.root)
        assert len(rows) == 1 and rows[0]['raw_response_body'] is None
        assert not rows[0]['raw_response_retained']


def test_provider_daily_limit_survives_restart_and_parser_fingerprint_change(cnyes_config, tmp_path):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    root = tmp_path / 'store'
    raw = Response(URL, 200, {'content-type': 'text/html'}, short_page())
    with closing(NewsQueue(root, registry)) as queue:
        for _ in range(3):
            fail(queue, raw)
        assert len(reports(root)[0]) == 3
    registry.sources['cnyes'] = replace(registry.sources['cnyes'], data_fingerprint='new-parser-fingerprint')
    with closing(NewsQueue(root, registry)) as queue:
        fail(queue, raw)
        assert len(reports(root)[0]) == 3


@pytest.mark.parametrize('storage_error', [OSError, RuntimeError])
def test_storage_failure_consumes_budget_and_never_changes_original_outcome(cnyes_config, tmp_path, monkeypatch, storage_error):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    calls = []
    def broken(*args, **kwargs):
        calls.append(args)
        raise storage_error('diagnostic storage unavailable')
    monkeypatch.setattr(diagnostics, 'atomic_write_parquet', broken)
    with closing(NewsQueue(tmp_path / 'store', registry)) as queue:
        for _ in range(4):
            fail(queue, Response(URL, 200, {'content-type': 'text/html'}, short_page()))
        assert len(calls) == 3 and not reports(queue.root)[0]
        assert queue.db.execute('SELECT state,error_code FROM articles WHERE url=?', (URL,)).fetchone()[:] == ('retry', 'body_not_found')


@pytest.mark.parametrize('code', ['access_challenge', 'paywall_or_login', 'article_attribution_not_verified', 'http_403', 'source_article_empty_template'])
def test_restrictions_and_source_outcomes_do_not_capture_raw(cnyes_config, tmp_path, code):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path / 'store', registry)) as queue:
        fail(queue, Response(URL, 200, {'content-type': 'text/html'}, short_page()), code=code)
        assert not reports(queue.root)[0]
        assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'failure_diagnostic_attempts:%'").fetchone()


def test_other_provider_diagnostic_removes_query_and_auth_headers(cfg, tmp_path):
    registry = PolicyRegistry(cfg, base=tmp_path)
    url = 'https://publisher.test/article/42?token=private-query'
    with closing(NewsQueue(tmp_path / 'store', registry)) as queue:
        fail(queue, Response(url, 200, {'content-type': 'text/html', 'set-cookie': 'private-cookie',
                                     'authorization': 'private-key', 'location': url}, b'<html>private-body</html>'))
        paths, rows = reports(queue.root)
        assert len(rows) == 1 and rows[0]['raw_response_body'] is None
        assert rows[0]['request_url'] == rows[0]['final_url'] == 'https://publisher.test/article/42'
        assert 'private-' not in paths[0].with_suffix('.receipt.json').read_text()


def test_fetch_worker_transfers_original_response_without_diagnostic_io(cnyes_config, tmp_path, monkeypatch):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    raw = Response(URL, 200, {'content-type': 'text/html'}, short_page(visible='不是宣告正文'))
    calls = []
    http = SimpleNamespace(registry=registry, get=lambda *_a, **_k: calls.append(URL) or raw)
    monkeypatch.setattr(diagnostics, 'atomic_write_parquet', lambda *_a, **_k: pytest.fail('worker wrote diagnostics'))
    with pytest.raises(NewsError, match='body_not_found') as caught:
        fetch_article(http, {'url': URL, 'id': b'a' * 32})
    assert calls == [URL] and caught.value.failure_response.response is raw
    assert not (tmp_path / 'diagnostics').exists()


def test_healthy_article_neither_attaches_failure_context_nor_parses_diagnostics(cnyes_config, tmp_path, monkeypatch):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    raw = Response(URL, 200, {'content-type': 'text/html'}, short_page())
    monkeypatch.setattr(diagnostics, '_public_cnyes_scope', lambda *_a: pytest.fail('healthy article did diagnostic parsing'))
    result = fetch_article(SimpleNamespace(registry=registry, get=lambda *_a, **_k: raw), {'url': URL, 'id': b'a' * 32})
    assert result['body'] and not (tmp_path / 'diagnostics').exists()


@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=a', MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_received_native_body_failure_reuses_public_identity_and_preserves_exact_variant(cnyes_config, tmp_path, url, streamed):
    page = streamed_mobile_page if streamed else mobile_page
    raw = Response(url, 200, {'content-type': 'text/html', 'authorization': 'private-key'},
                   page(visible='可見全文不一致；這是待診斷的解析失敗，不是可接受正文。'))
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path / 'store', registry)) as queue:
        fail(queue, raw)
        paths, rows = reports(queue.root)
        assert len(rows) == 1 and rows[0]['raw_response_body'] == raw.body
        assert rows[0]['contract_version'] == diagnostics.FAILURE_DIAGNOSTIC_CONTRACT_VERSION and rows[0]['raw_response_retained']
        assert rows[0]['request_url'] == rows[0]['final_url'] == url
        assert rows[0]['diagnostic_only'] and not rows[0]['accepted_as_article']
        assert not rows[0]['publish'] and not rows[0]['remote_training_export']
        assert 'private-key' not in paths[0].with_suffix('.receipt.json').read_text()
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert queue.db.execute('SELECT body_sha256 FROM articles WHERE url=?', (url,)).fetchone()[0] is None


@pytest.mark.parametrize('change', [
    'payment', 'boolean_payment', 'missing_author', 'different_author', 'wrong_id',
    'wrong_canonical', 'wrong_og', 'wrong_schema_identity', 'wrong_clock', 'naive_clock',
    'hidden_author', 'hidden_container', 'unfinished_stream', 'paid_schema',
    'paywall', 'challenge', 'unknown_query', 'secret_query', 'wrong_mime', 'different_response_url',
])
def test_native_diagnostic_does_not_waive_identity_clocks_visibility_access_or_query_secrecy(cnyes_config, tmp_path, change):
    url, mime, final = URL + '?exp=a', 'text/html', URL + '?exp=a'
    changes = {'payment': {'payment': 1}, 'boolean_payment': {'payment': False},
               'missing_author': {'author': None}, 'different_author': {'author': '別篇作者'},
               'wrong_id': {'newsId': 43}}
    raw = streamed_mobile_page(visible='不是完整宣告正文', changes=changes.get(change))
    mutations = {
        'wrong_canonical': (f'href="{URL}"', f'href="{URL}0"'),
        'wrong_og': (f'content="{URL}"', f'content="{URL}0"'),
        'wrong_schema_identity': ('"@id": "' + URL + '"', '"@id": "' + URL + '0"'),
        'wrong_clock': ('datetime="2014-01-01T00:00:00.000Z"', 'datetime="2014-01-02T00:00:00.000Z"'),
        'naive_clock': ('2014-01-01T00:00:00.000Z', '2014-01-01T00:00:00.000'),
        'hidden_author': ('<span class="signature">', '<span class="signature" hidden>'),
        'hidden_container': ('<div id="article-container">', '<div id="article-container" hidden>'),
        'unfinished_stream': (FIZZ_COMPLETION, ''),
        'paid_schema': ('"headline":', '"isAccessibleForFree": false, "headline":'),
        'paywall': ('<article>', '<article class="paywall">'),
        'challenge': ('</head>', '<title>Just a moment</title></head>'),
    }
    if change in mutations:
        before, after = mutations[change]
        raw = raw.replace(before.encode(), after.encode())
    if change in {'unknown_query', 'secret_query'}:
        url = final = URL + ('?exp=c' if change == 'unknown_query' else '?exp=a&token=private-query')
    elif change == 'wrong_mime':
        mime = 'application/json'
    elif change == 'different_response_url':
        final = URL + '0?exp=a'
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path / 'store', registry)) as queue:
        response = Response(final, 200, {'content-type': mime}, raw)
        # Keep the requested URL distinct from final URL without changing the
        # failure attachment's matching queue-head precondition.
        with queue.db:
            queue.add_articles([{'url': url}])
        row = queue.claim_url(url, 300)
        source = registry.require(url)
        error = NewsError('body_not_found')
        error.failure_response = diagnostics.FailureResponse(response, source.provider, source.data_fingerprint, url, utc_now())
        queue.failed(row, error, 6)
        paths, rows = reports(queue.root)
        assert len(rows) == 1 and rows[0]['raw_response_body'] is None
        assert not rows[0]['raw_response_retained']
        assert '?' not in rows[0]['request_url'] and '?' not in rows[0]['final_url']
        assert 'private-query' not in paths[0].with_suffix('.receipt.json').read_text()


def test_healthy_native_article_uses_identity_once_and_never_runs_failure_scope(cnyes_config, tmp_path, monkeypatch):
    from downloader import news_fulltext_publishers as publishers
    witness, calls = publishers._cnyes_native_article_identity, []
    def observed(*args, **kwargs):
        calls.append(kwargs['page_url'])
        return witness(*args, **kwargs)
    monkeypatch.setattr(publishers, '_cnyes_native_article_identity', observed)
    monkeypatch.setattr(diagnostics, '_public_cnyes_scope', lambda *_a: pytest.fail('healthy native article repeated diagnostic parsing'))
    url = URL + '?exp=a'
    raw = Response(url, 200, {'content-type': 'text/html'}, streamed_mobile_page())
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    result = fetch_article(SimpleNamespace(registry=registry, get=lambda *_a, **_k: raw), {'url': url, 'id': b'a' * 32})
    assert result['body'] and calls == [url] and not (tmp_path / 'diagnostics').exists()


@pytest.mark.parametrize('stage', sorted(diagnostics.CNYES_NATIVE_BODY_FAILURE_STAGES))
def test_native_stage_is_bounded_counted_and_private_without_changing_original_failure(cnyes_config, tmp_path, stage):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    url = URL + '?exp=a'
    with closing(NewsQueue(tmp_path / 'store', registry)) as queue:
        for _ in range(3):
            fail(queue, Response(url, 200, {'content-type': 'text/html'}, mobile_page()), stage=stage)
        paths, rows = reports(queue.root)
        assert len(rows) == 3  # Two daily and one hourly; stage counts remain independent.
        assert all(r['native_body_failure_stage'] == stage
                   and r['native_body_failure_observation_contract_version'] == 1 for r in rows)
        snapshot = queue.metrics.snapshot()
        assert snapshot['cnyes_native_body_failure_observation_contract_version'] == 1
        assert snapshot['providers']['cnyes']['counts']['native_body_failure_stage_' + stage] == 3
        assert queue.db.execute('SELECT body_sha256 FROM articles WHERE url=?', (url,)).fetchone()[0] is None


@pytest.mark.parametrize('stage', ['private-arbitrary-body-words', ['unhashable'], 1, True])
def test_unrecognized_failure_stage_never_creates_unbounded_metrics_or_private_text(cnyes_config, tmp_path, stage):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path / 'store', registry)) as queue:
        fail(queue, Response(URL, 200, {'content-type': 'text/html'}, short_page()), stage=stage)
        _, rows = reports(queue.root)
        assert rows[0]['native_body_failure_stage'] is None
        assert not any(k.startswith('native_body_failure_stage_') for k in queue.metrics.snapshot()['providers']['cnyes']['counts'])


@pytest.mark.parametrize('kind,expected_stage', [
    ('empty', 'missing_body_representation'),
    ('visible', 'visible_current_mismatch'),
    ('schema', 'schema_mismatch_without_verified_figure_view'),
    ('caption', 'schema_comparison_mismatch'),
])
def test_real_native_failed_branch_is_carried_from_extractor_to_owner(cnyes_config, tmp_path, kind, expected_stage):
    from test_news_cnyes_captions import CAPTION, caption_page
    from downloader.news_fulltext_policy import extract_article
    payload = (mobile_page(body='') if kind == 'empty' else
               mobile_page(visible='不是宣告正文') if kind == 'visible' else
               mobile_page(schema_changes={'articleBody': '另一份不一致的全文'}) if kind == 'schema' else
               caption_page(schema_caption=CAPTION + ' 未確認的額外文字'))
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    source = registry.sources['cnyes']
    url = URL + '?exp=a'
    with pytest.raises(NewsError, match='^body_not_found$') as error:
        extract_article(payload, source, page_url=url)
    assert error.value.cnyes_native_body_failure_stage == expected_stage


def test_native_stage_preserves_financial_table_boundaries_even_when_compact_text_matches(cnyes_config, tmp_path):
    from downloader.news_fulltext_policy import extract_article
    from test_news_cnyes_html import BODY
    prefix = '<p>' + BODY + '</p>'
    visible = prefix + '<table><tr><td>數值12</td><td>345</td></tr></table>'
    current = prefix + '<table><tr><td>數值1</td><td>2345</td></tr></table>'
    payload = mobile_page(body=BODY, changes={'content': html.escape(current)},
                          schema_changes={'articleBody': BODY + '數值12345'})
    payload = payload.replace(prefix.encode(), visible.encode())
    source = PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']
    with pytest.raises(NewsError, match='^body_not_found$') as error:
        extract_article(payload, source, page_url=URL + '?exp=a')
    assert error.value.cnyes_native_body_failure_stage == 'table_grid_mismatch'


def test_native_stage_is_never_attributed_to_another_provider(cfg, tmp_path):
    registry = PolicyRegistry(cfg, base=tmp_path)
    with closing(NewsQueue(tmp_path / 'store', registry)) as queue:
        fail(queue, Response('https://publisher.test/news/42', 200, {'content-type': 'text/html'}, b'<p>synthetic</p>'),
             stage='visible_current_mismatch')
        _, rows = reports(queue.root)
        assert rows[0]['native_body_failure_stage'] is None
        assert not any(k.startswith('native_body_failure_stage_')
                       for k in queue.metrics.snapshot()['providers']['publisher']['counts'])


def globalvoices_registry(tmp_path):
    config = load_config(Path('configs/news_fulltext.json'))[0]
    config = {**config, 'providers': [p for p in config['providers'] if p['id'] == 'globalvoices']}
    return PolicyRegistry(config, base=tmp_path)


def test_current_licensed_globalvoices_failure_retains_received_bytes_without_body_acceptance(tmp_path):
    raw = Response(GV_URL, 200, {'content-type': 'text/html', 'set-cookie': 'private-cookie'}, gv_page(''))
    with closing(NewsQueue(tmp_path / 'store', globalvoices_registry(tmp_path))) as queue:
        fail(queue, raw)
        paths, rows = reports(queue.root)
        assert len(rows) == 1 and rows[0]['raw_response_body'] == raw.body
        assert rows[0]['raw_response_retained'] and not rows[0]['accepted_as_article']
        assert rows[0]['contract_version'] == diagnostics.FAILURE_DIAGNOSTIC_CONTRACT_VERSION
        assert rows[0]['retention_basis'] == 'matching_current_native_article_CC_license_authors_and_aware_clock'
        receipt = json.loads(paths[0].with_suffix('.receipt.json').read_text())
        assert receipt['sha256'] == sha256_file(paths[0])
        assert 'private-cookie' not in receipt['headers_json']
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert tuple(queue.db.execute('SELECT state,error_code,body_sha256 FROM articles WHERE url=?',
                                     (GV_URL,)).fetchone()) == ('body_not_found', 'body_not_found', None)


@pytest.mark.parametrize('change', [
    'paywall', 'challenge', 'different_canonical', 'missing_license',
    'different_footer_author', 'naive_clock', 'hidden_credit', 'paid_declaration', 'wrong_mime',
])
def test_unverified_globalvoices_failure_keeps_only_metadata(tmp_path, change):
    raw, mime = gv_page(''), 'text/html'
    if change == 'paywall':
        raw = raw.replace(b'class="entry"', b'class="entry paywall"')
    elif change == 'challenge':
        raw = raw.replace(b'</head>', b'<title>Just a moment</title></head>')
    elif change == 'different_canonical':
        raw = raw.replace(GV_URL.encode(), b'https://globalvoices.org/2007/02/13/another/')
    elif change == 'missing_license':
        raw = raw.replace(b'https://creativecommons.org/licenses/by/3.0/', b'https://unknown.test/license')
    elif change == 'different_footer_author':
        raw = raw.replace(b'<span class="contributor-name"><a class="user-link" href="/author/writer/">Writer',
                          b'<span class="contributor-name"><a class="user-link" href="/author/other/">Other')
    elif change == 'naive_clock':
        raw = raw.replace(b'2007-02-13T12:17:00+0000', b'2007-02-13T12:17:00')
    elif change == 'hidden_credit':
        raw = raw.replace(b'id="post-header-credit"', b'id="post-header-credit" hidden')
    elif change == 'paid_declaration':
        raw = raw.replace(b'</head>', b'<script type="application/ld+json">'
                          b'{"@type":"NewsArticle","isAccessibleForFree":false}</script></head>')
    elif change == 'wrong_mime':
        mime = 'application/json'
    with closing(NewsQueue(tmp_path / 'store', globalvoices_registry(tmp_path))) as queue:
        fail(queue, Response(GV_URL, 200, {'content-type': mime}, raw))
        _, rows = reports(queue.root)
        assert len(rows) == 1 and rows[0]['raw_response_body'] is None
        assert not rows[0]['raw_response_retained']


def test_old_metadata_budget_is_not_reset_by_new_licensed_raw_contract(tmp_path):
    registry = globalvoices_registry(tmp_path)
    raw = Response(GV_URL, 200, {'content-type': 'text/html'}, gv_page(''))
    with closing(NewsQueue(tmp_path / 'store', registry)) as queue:
        day = utc_now()[:10].replace('-', '')
        key = 'failure_diagnostic_attempts:v1:globalvoices:' + day
        with queue.db:
            queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (key, '2'))
        fail(queue, raw)
        _, rows = reports(queue.root)
        assert len(rows) == 1 and rows[0]['budget_period_kind'] == 'utc_hour'
        assert rows[0]['budget_attempt_limit'] == 1 and not rows[0]['accepted_as_article']
        fail(queue, raw)
        assert len(reports(queue.root)[0]) == 1
        assert [tuple(row) for row in queue.db.execute(
            "SELECT key,value FROM queue_settings WHERE key LIKE 'failure_diagnostic_attempts:%'")] == [(key, '2')]

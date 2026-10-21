"""Synthetic native DOM; source article bodies are never committed fixtures."""
from copy import deepcopy
from contextlib import closing
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import _require_article_response, extract_article_response, fetch_article, load_config
from downloader.news_fulltext_http import RedirectHop, Response
from downloader.news_fulltext_pinview import PINVIEW_BODY_SELECTOR, PINVIEW_ORIGIN, PINVIEW_PATH_PATTERN
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, native_text_parser_repair, utc_now


URL = PINVIEW_ORIGIN + '/News/12345.html'
TITLE = '合成數據 -12.50 與 0% 原值不變'
CLOCK = '2025/07/01 06:28:00'
COPYRIGHT = '品觀點 版權所有 ©2022 品觀點 All Rights Reserved.'


@pytest.fixture
def settings():
    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    config = deepcopy(config)
    config['providers'] = [{
        'id': 'pinview', 'domains': ['www.pinview.com.tw'], 'enabled': True,
        'acquisition_mode': 'continuous', 'private_known_url_scope': 'personal_noncommercial_originals_v1',
        'interval_seconds': 2, 'parallelism': 1, 'html_parser': 'lxml',
        'embedded_article_layout': 'pinview_visible_article_v1',
        'article_path_pattern': PINVIEW_PATH_PATTERN, 'article_selectors': [PINVIEW_BODY_SELECTOR],
        'exclude_selectors': [], 'title_selector': 'h1',
        'require_configured_container': True, 'require_author_attribution': True,
        'authorization': {'kind': 'personal_research', 'reviewed_on': '2026-10-21',
            'evidence_url': PINVIEW_ORIGIN + '/',
            'scope': 'Private known reporter originals only. No license, media, discovery or export grant.',
            'automated_access_review': 'no_explicit_prohibition_identified'},
    }]
    return config


def page():
    prose = ''.join(f'<p>第{i}段合成原始值。' + '保留股數負值零值及比率。' * 5 + '</p>' for i in range(3))
    prose += '<p><img src="/Upload/image/synthetic.jpg"><br>圖片旁的文字仍是正文。</p>'
    prose += '<table><caption>合成表格</caption><tr><th>股數</th><td>-12.50</td><td>0%</td></tr></table>'
    return f'''<html><head><title>{TITLE}   </title>
      <meta property="og:title" content="{TITLE}   "><meta property="og:url" content="{URL}">
      <meta property="og:type" content="article"><meta property="og:site_name" content="品觀點">
      </head><body><div class="web-layout"><div class="main-container">
        <div class="article"><div class="article-title"><h1>{TITLE}   </h1>
          <span>{CLOCK}</span><span>文／資深記者 李錦奇</span></div>
          <picture><img src="/hero.jpg"></picture><div class="fontSize-bar">導覽不是正文</div>
          <div id="contentText">{prose}</div><div class="tag-list">標籤不是正文</div></div>
        <div class="news-box-h"><h3>推薦另一篇</h3><p>不得混入正文 記者別人</p></div>
        <div class="fb-comments" data-href="{URL}"></div>
      </div></div><footer><p>{COPYRIGHT}</p></footer></body></html>'''


def parse(settings, html=None, *, url=URL):
    source = PolicyRegistry(settings, base=Path.cwd()).sources['pinview']
    return extract_article((page() if html is None else html).encode(), source, page_url=url)


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_whole_native_prose_values_and_naive_clock_retained(settings, parser):
    settings['providers'][0]['html_parser'] = parser
    result = parse(settings)
    assert result['title'] == TITLE and json.loads(result['authors_json']) == ['李錦奇']
    assert result['published_at_raw'] == CLOCK and result['modified_at_raw'] is None
    assert 'timezone_and_release_revision_label_undeclared' in result['published_time_basis']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    for i in range(3):
        assert result['body'].count(f'第{i}段') == 1
    for value in ('合成表格', '股數', '-12.50', '0%', '圖片旁的文字仍是正文。'):
        assert value in result['body']
    for value in ('導覽', '標籤', '推薦', '記者別人', '版權所有'):
        assert value not in result['body']
    notices = json.loads(result['verified_article_notices_json'])
    assert notices[0]['text'] == CLOCK and not notices[0]['timezone_declared']
    assert notices[1]['text'] == COPYRIGHT
    assert not notices[1]['open_license_or_bulk_permission_inferred']
    assert notices[2]['kind'] == 'unfetched_inline_image' and not notices[2]['media_requested']
    assert not json.loads(result['author_attribution_detail_json'])['reporter_real_identity_independently_verified']


@pytest.mark.parametrize('old,new,code', [
    ('content="article"', 'content="website"', 'article_identity_not_verified'),
    ('content="品觀點"', 'content="其他媒體"', 'article_identity_not_verified'),
    ('data-href="' + URL + '"', 'data-href="' + URL + '9"', 'article_identity_not_verified'),
    ('property="og:url" content="' + URL + '"', 'property="og:url" content="' + URL + '?other=1"', 'article_identity_not_verified'),
    ('<title>' + TITLE, '<title>其他文章', 'article_identity_not_verified'),
    ('<h1>', '<h1 hidden>', 'article_identity_not_verified'),
    ('id="contentText"', 'id="contentText" style="display:none"', 'article_identity_not_verified'),
    ('第1段', '<span aria-hidden="true">第1段</span>', 'article_identity_not_verified'),
    ('第1段', '<span class="sr-only">第1段</span>', 'article_identity_not_verified'),
    ('第1段', '<iframe src="/unknown"></iframe>第1段', 'article_identity_not_verified'),
    ('第1段', '<script>unknown()</script>第1段', 'article_identity_not_verified'),
    ('第1段', '<form action="/other"></form>第1段', 'article_identity_not_verified'),
    ('第0段合成原始值。', '新聞來源：其他媒體', 'article_rights_or_scope_excluded'),
    ('文／資深記者 李錦奇', '文／資深記者 中央社', 'article_rights_or_scope_excluded'),
    ('文／資深記者 李錦奇', '文／編輯 合成人', 'article_rights_or_scope_excluded'),
    ('文／資深記者 李錦奇', '', 'article_attribution_not_verified'),
    (CLOCK, '2025/02/30 06:28:00', 'invalid_bulk_publication_clock'),
    (CLOCK, '2025/07/01', 'invalid_bulk_publication_clock'),
    (CLOCK, '2025/07/01 06:28:00+08:00', 'invalid_bulk_publication_clock'),
    (COPYRIGHT, '全部免費開放授權', 'article_rights_not_verified'),
    ('<div id="contentText">', '<div id="contentText"></div><div id="contentText">', 'article_identity_not_verified'),
])
@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_reject_unwitnessed_identity_scope_hidden_body_and_clocks(settings, old, new, code, parser):
    settings['providers'][0]['html_parser'] = parser
    assert old in page()
    with pytest.raises(NewsError) as error:
        parse(settings, page().replace(old, new))
    assert error.value.code == code


def test_optional_canonical_is_checked_not_required(settings):
    html = page().replace('</head>', f'<link rel="canonical" href="{URL}"></head>')
    assert parse(settings, html)['title'] == TITLE
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, html.replace('rel="canonical" href="' + URL, 'rel="canonical" href="' + URL + '9'))


def test_native_footer_preserves_direct_copyright_not_contacts_or_privacy_label(settings):
    html = page().replace('<p>' + COPYRIGHT + '</p>',
        '<p>合成聯絡方式<br>\n' + COPYRIGHT + '<a href="/privacy">隱私權聲明</a></p>')
    result = parse(settings, html)
    assert json.loads(result['verified_article_notices_json'])[1]['text'] == COPYRIGHT
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, html.replace('<footer>', '<footer hidden>'))


@pytest.mark.parametrize('update', [
    {'domains': ['pinview.com.tw', 'www.pinview.com.tw']}, {'article_selectors': ['#contentText']},
    {'article_path_pattern': '^/.*'}, {'require_author_attribution': False},
    {'require_configured_container': False}, {'exclude_selectors': ['p']},
    {'title_selector': 'h2'}, {'feeds': [PINVIEW_ORIGIN + '/rss']},
])
def test_layout_scope_cannot_be_broadened(settings, update):
    settings['providers'][0].update(update)
    with pytest.raises(ValueError):
        PolicyRegistry(settings, base=Path.cwd())


@pytest.mark.parametrize('url', [URL + '?other=1', URL + '#part',
    URL.replace('https:', 'http:'), URL.replace('www.', ''), URL.replace('/12345.', '/012345.')])
def test_unreviewed_routes_rejected_before_http(settings, url):
    def forbidden(*args, **kwargs):
        raise AssertionError('must not make an unreviewed request')
    with pytest.raises(NewsError):
        fetch_article(SimpleNamespace(registry=PolicyRegistry(settings, base=Path.cwd()), get=forbidden), {'url': url})


def test_no_redirect_or_alias_is_synthesized_and_raw_retained(settings):
    registry = PolicyRegistry(settings, base=Path.cwd())
    response = Response(URL, 200, {'content-type': 'text/html'}, page().encode())
    http = SimpleNamespace(registry=registry, get=lambda *a, **kw: response)
    result = fetch_article(http, {'url': URL, 'id': b'\x01' * 16})
    assert result['requested_url'] == result['final_url'] == URL
    assert result['url_resolution_basis'] == 'requested_public_url'
    assert result['raw_response_body'] == response.body
    other = URL.replace('/12345.', '/54321.')
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        fetch_article(http, {'url': other, 'id': b'\x02' * 16})
    hop = RedirectHop(URL, 302, URL, URL, '2026-10-20T19:40:00+00:00', 'd' * 64, 0)
    http.get = lambda *a, **kw: Response(URL, 200, response.headers, response.body, (hop,))
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        fetch_article(http, {'url': URL, 'id': b'\x01' * 16})


def test_actual_zero_byte_http200_is_recoverable_not_a_terminal_identity_verdict(settings, tmp_path):
    registry = PolicyRegistry(settings, base=Path.cwd())
    response = Response(URL, 200, {'content-type': 'text/html'}, b'')
    http = SimpleNamespace(registry=registry, get=lambda *a, **kw: response)
    with pytest.raises(NewsError) as caught:
        fetch_article(http, {'url': URL, 'id': b'\x01' * 16})
    error = caught.value
    assert error.code == 'empty_http_response' and error.retry and error.delay == 30
    with pytest.raises(NewsError, match='empty_http_response'):
        extract_article_response(response, registry.sources['pinview'])
    queue = NewsQueue(tmp_path, registry)
    try:
        with queue.db:
            queue.add_articles([{'url': URL}])
        row = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        queue.failed(row, error, max_attempts=6)
        first = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        assert first['state'] == 'retry' and first['error_code'] == 'empty_http_response'
        queue.failed({**first, 'attempts': 6}, error, max_attempts=6)
        exhausted = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        assert exhausted['state'] == 'retry_cooldown' and exhausted['body_sha256'] is None
        assert exhausted['captured_at_utc'] is None and exhausted['parquet_path'] is None
    finally:
        queue.close()


@pytest.mark.parametrize('status,mime,path,code', [
    (404, 'text/html', URL, 'source_not_found'),
    (403, 'text/html', URL, 'http_403'),
    (429, 'text/html', URL, 'http_429'),
    (200, 'application/json', URL, 'unsupported_content_type'),
    (200, 'text/html', PINVIEW_ORIGIN + '/', 'redirect_not_article'),
])
def test_empty_envelope_retry_does_not_override_refusal_mime_or_actual_redirect(settings, status, mime, path, code):
    source = PolicyRegistry(settings, base=Path.cwd()).sources['pinview']
    with pytest.raises(NewsError) as error:
        _require_article_response(Response(path, status, {'content-type':mime}, b''), source)
    assert error.value.code == code


def test_empty_envelope_change_does_not_reclassify_other_source_contracts():
    source = SimpleNamespace(source_not_found_paths=(), article_path_pattern=None,
        pdf_path_pattern=None, embedded_article_layout='other_source_v1')
    assert _require_article_response(Response(URL, 200, {'content-type':'text/html'}, b''), source) is False


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_validation_metadata_observes_unreviewed_credit_without_acceptance_or_dom_changes(settings, parser):
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_pinview import pinview_validation_metadata
    html = page().replace('文／資深記者 李錦奇', '文／未審核署名 合成人')
    soup = BeautifulSoup(html, parser)
    before = str(soup)
    observed = pinview_validation_metadata(soup, page_url=URL)
    assert str(soup) == before
    assert observed['current_native_url_recognized']
    assert observed['article_scope_count'] == observed['sampled_native_body_count'] == 1
    assert observed['header_labels'][0][0]['value'] == CLOCK
    assert observed['header_labels'][0][1]['value'] == '文／未審核署名 合成人'
    assert observed['headings'][0]['value'] == TITLE
    assert observed['copyright_declaration_count'] == 1
    assert observed['og_urls'][0]['display_url'] == URL
    assert observed['comment_urls'][0]['display_url'] == URL
    assert not observed['body_text_retained'] and not observed['raw_response_retained']
    assert not observed['article_identity_or_scope_granted'] and observed['additional_requests'] == 0
    for forbidden in ('第0段', '圖片旁的文字', '合成表格', '不得混入正文'):
        assert forbidden not in json.dumps(observed, ensure_ascii=False)
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        parse(settings, html)


def test_validation_metadata_records_structure_and_reprint_flag_not_prose():
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_pinview import pinview_validation_metadata
    html = page().replace('第0段', '本文轉載不可保留的逐字正文<iframe>保密文字</iframe>第0段')
    observed = pinview_validation_metadata(BeautifulSoup(html, 'lxml'), page_url=URL)
    structure = observed['native_body_structures'][0]
    assert structure['sampled_reprint_prefix_present']
    assert structure['unrecognized_tag_names'] == ['iframe']
    assert structure['tag_counts']['iframe'] == 1
    assert '不可保留' not in json.dumps(observed, ensure_ascii=False)
    assert '保密文字' not in json.dumps(observed, ensure_ascii=False)


def test_validation_metadata_bounds_values_topology_and_redacts_url_secrets():
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_pinview import pinview_validation_metadata
    soup = BeautifulSoup(page().replace('第0段', '<span>不留存</span>' * 5000 + '第0段'), 'lxml')
    soup.h1.string = '長' * 5000
    soup.select_one('meta[property="og:url"]')['content'] = URL + '?secret=hidden-token#private'
    soup.select_one('.fb-comments')['data-href'] = 'https://user:hidden-password@www.pinview.com.tw/News/12345.html'
    observed = pinview_validation_metadata(soup, page_url=URL)
    assert observed['headings'][0]['truncated'] and len(observed['headings'][0]['value']) == 512
    assert observed['native_body_structures'][0]['elements_truncated']
    assert observed['native_body_structures'][0]['sampled_element_count'] == 4096
    assert observed['og_urls'][0]['display_url'] == URL
    assert observed['comment_urls'][0]['display_url'] is None
    encoded = json.dumps(observed, ensure_ascii=False)
    for forbidden in ('hidden-token', 'hidden-password', '不留存'):
        assert forbidden not in encoded
    assert len(encoded.encode()) < 65536


def test_validation_metadata_does_not_rewrite_valid_native_values(settings):
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_pinview import pinview_validation_metadata
    from downloader.news_fulltext_policy import extract_article
    source = PolicyRegistry(settings, base=Path.cwd()).sources['pinview']
    before = parse(settings)
    soup = BeautifulSoup(page(), source.html_parser)
    pinview_validation_metadata(soup, page_url=URL)
    assert extract_article(str(soup).encode(), source, page_url=URL) == before


def test_canonical_validation_sink_reuses_budget_and_never_retains_unreviewed_body(settings, tmp_path):
    from test_news_failure_diagnostics import fail, reports
    from test_news_failure_identity_metadata import evidence
    registry = PolicyRegistry(settings, base=Path.cwd())
    response = Response(URL, 200, {'content-type': 'text/html', 'set-cookie': 'private-cookie'},
        page().replace('文／資深記者 李錦奇', '文／未審核署名 合成人').encode())
    queue = NewsQueue(tmp_path, registry)
    try:
        for _ in range(3):
            fail(queue, response, code='article_rights_or_scope_excluded',
                 received_at_utc='2026-10-20T19:00:00+00:00')
        paths, rows = evidence(tmp_path)
        assert len(rows) == 2
        assert all(row['facts']['pinview_native_validation']['header_labels'][0][1]['value']
                   == '文／未審核署名 合成人' for row in rows)
        assert all(not row['accepted_as_article'] and not row['raw_response_retained'] for row in rows)
        assert all('第0段' not in path.read_text() and 'private-cookie' not in path.read_text() for path in paths)
        assert not reports(tmp_path)[0]
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'failure_validation_metadata_attempts:%'").fetchone()[0] == '2'
    finally:
        queue.close()


def test_operator_probe_emits_pinview_metadata_without_keeping_body(settings, tmp_path, capsys):
    from scripts.probe_news_sources import probe_provider
    registry = PolicyRegistry(settings, base=Path.cwd())
    requests = []
    def get(url):
        requests.append(url)
        return Response(url, 200, {'content-type': 'text/html'},
            page().replace('文／資深記者 李錦奇', '文／未審核署名 合成人').encode())
    http = SimpleNamespace(registry=registry, provider_not_before={}, robot_delays={}, get=get)
    result = probe_provider(http, [URL], output_root=tmp_path)
    assert requests == [URL]
    assert result[0]['extraction_error'] == 'article_rights_or_scope_excluded'
    assert result[0]['pinview_native_validation']['header_labels'][0][1]['value'] == '文／未審核署名 合成人'
    assert '第0段' not in json.dumps(result, ensure_ascii=False) and '第0段' not in capsys.readouterr().out
    assert not list(tmp_path.iterdir())


def versioned(settings, version):
    result = deepcopy(settings)
    result['providers'][0]['embedded_article_layout'] = f'pinview_visible_article_v{version}'
    return result


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_v2_admits_only_the_second_observed_reporter_without_rewriting_v1_data(settings, parser):
    settings['providers'][0]['html_parser'] = parser
    updated = versioned(settings, 2)
    assert parse(settings) == parse(updated)
    html = page().replace('文／資深記者 李錦奇', '文／資深記者 林憲祥')
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        parse(settings, html)
    result = parse(updated, html)
    assert json.loads(result['authors_json']) == ['林憲祥']
    assert result['body'] == parse(settings)['body'] and result['published_at_raw'] == CLOCK
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['native_credit'] == '文／資深記者 林憲祥'
    assert not detail['reporter_real_identity_independently_verified']


@pytest.mark.parametrize('credit', ['文／品觀點編輯中心', '文／中央社', '文／資深記者 其他人',
    '文／資深記者林憲祥', '文／資深記者 林憲祥、他人', '文／讀者投書 林憲祥'])
def test_v2_does_not_turn_new_reporter_into_an_author_role_pattern_or_editorial_grant(settings, credit):
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        parse(versioned(settings, 2), page().replace('文／資深記者 李錦奇', credit))


def test_v2_keeps_transport_identity_and_requires_exact_v1_predecessor(settings):
    old = PolicyRegistry(settings, base=Path.cwd()).sources['pinview']
    new = PolicyRegistry(versioned(settings, 2), base=Path.cwd()).sources['pinview']
    assert new.data_fingerprint != old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.http_capacity_source_fingerprint == (old.http_capacity_source_fingerprint or old.data_fingerprint)
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert native_text_parser_repair(new, old.data_fingerprint)
    assert not native_text_parser_repair(new, new.data_fingerprint)
    changed = versioned(settings, 2)
    changed['providers'][0]['authorization']['scope'] += '; changed scope'
    other = PolicyRegistry(changed, base=Path.cwd()).sources['pinview']
    assert not native_text_parser_repair(other, old.data_fingerprint)


@pytest.mark.parametrize('obstruction', [None, 'body_sha256', 'parquet_path', 'lease_owner', 'lease_until',
    'missing_observation', 'future_observation', 'naive_observation', 'different_error',
    'unsupported_url', 'unknown_prior'])
def test_v2_rechecks_only_observed_missing_credit_failures_once_preserving_old_evidence(settings, tmp_path, obstruction):
    old = PolicyRegistry(settings, base=tmp_path)
    new = PolicyRegistry(versioned(settings, 2), base=tmp_path)
    states = ['article_rights_or_scope_excluded', 'article_identity_not_verified', 'invalid_bulk_publication_clock',
        'complete', 'body_not_found', 'article_attribution_not_verified', 'http_403', 'http_429',
        'unsupported_article_path', 'source_not_found', 'paywall_or_login', 'retry']
    before = {}
    with closing(NewsQueue(tmp_path, old)) as queue:
        for number, state in enumerate(states, 1):
            url = PINVIEW_ORIGIN + f'/News/{number}.html'
            queue.add_articles([{'url': url}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=9000000000,'
                'last_checked_at_utc=? WHERE url=?', (state, state, utc_now(), url))
            if obstruction in {'body_sha256', 'parquet_path', 'lease_owner', 'lease_until'}:
                queue.db.execute(f'UPDATE articles SET {obstruction}=? WHERE url=?',
                    (9000000001 if obstruction == 'lease_until' else 'saved-or-owned', url))
            elif obstruction in {'missing_observation', 'future_observation', 'naive_observation'}:
                observed = {'missing_observation': None, 'future_observation': '2099-01-01T00:00:00+00:00',
                            'naive_observation': '2026-01-01T00:00:00'}[obstruction]
                queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', (observed, url))
            elif obstruction == 'different_error':
                queue.db.execute("UPDATE articles SET error_code='different' WHERE url=?", (url,))
            elif obstruction == 'unsupported_url':
                queue.db.execute("UPDATE articles SET url=url||'?other=1' WHERE url=?", (url,))
            row = dict(queue.db.execute('SELECT * FROM articles ORDER BY rowid DESC LIMIT 1').fetchone())
            before[row['url']] = row
        if obstruction == 'unknown_prior':
            queue.db.execute("UPDATE queue_settings SET value=? WHERE key='source_capture_fingerprint:pinview'", ('f' * 64,))
        queue.db.commit()
    marker = 'pinview_native_parser_recheck_v2:' + new.sources['pinview'].data_fingerprint
    with closing(NewsQueue(tmp_path, new)) as queue:
        for url, row in before.items():
            expected = {**row, 'state': 'retry'} if obstruction is None and row['state'] == states[0] else row
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == expected
        record = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
        if obstruction == 'unknown_prior':
            assert record is None
        else:
            evidence = json.loads(record[0])
            assert len(evidence['rows']) == (1 if obstruction is None else 0)
            assert evidence['attempts_due_error_and_observation_preserved'] and not evidence['accepted_as_bodies']
        if obstruction is None:
            queue.db.execute("UPDATE articles SET state='article_rights_or_scope_excluded' WHERE error_code=?", (states[0],))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (next(iter(before)),)).fetchone()) == before[next(iter(before))]


@pytest.mark.parametrize('version', [1, 2])
def test_candidate_obeys_installed_source_contract(settings, monkeypatch, version):
    import test_news_fulltext as shared
    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(config)
    candidate['providers'] = [entry for entry in config['providers'] if entry['id'] != 'pinview'] + versioned(settings, version)['providers']
    registry = PolicyRegistry(candidate, base=repo)
    monkeypatch.setattr(shared, 'load_config', lambda path: (candidate, registry, repo))
    shared.test_default_configuration_separates_personal_capture_from_licenses()

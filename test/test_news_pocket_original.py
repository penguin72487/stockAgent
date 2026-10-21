"""Synthetic native Pocket identity/body/clock tests; no site requests or prose."""
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import json
from contextlib import closing

import pytest
from bs4 import BeautifulSoup

from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_http import RedirectHop, Response
from downloader.news_fulltext_pocket import POCKET_AUTHOR, POCKET_BODY_SELECTOR, POCKET_ORIGIN, POCKET_PATH_PATTERN, pocket_article_id
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, pocket_native_parser_repair, utc_now


URL = POCKET_ORIGIN + '/school/articles/12345/'
OLD = POCKET_ORIGIN + '/school/report/SCHOOL/12345/'
RELEASE = '2023-02-14T23:01:02.000Z'
REVISION = '2026-10-19T03:04:05.678Z'


@pytest.fixture
def settings():
    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    config = deepcopy(config)
    config['providers'] = [{
        'id': 'pocket', 'name': 'Synthetic own School text', 'domains': ['www.pocket.tw'],
        'enabled': True, 'acquisition_mode': 'continuous',
        'private_known_url_scope': 'personal_noncommercial_originals_v1',
        'interval_seconds': 2, 'parallelism': 1, 'html_parser': 'lxml',
        'embedded_article_layout': 'pocket_visible_article_v1',
        'article_path_pattern': POCKET_PATH_PATTERN, 'article_selectors': [POCKET_BODY_SELECTOR],
        'exclude_selectors': [], 'title_selector': 'h1',
        'require_configured_container': True, 'require_author_attribution': True,
        'authorization': {'kind': 'personal_research', 'reviewed_on': '2026-10-20',
            'evidence_url': POCKET_ORIGIN + '/main/disclaimer',
            'scope': 'Private own known text only; no discovery, media, redistribution or training grant.',
            'automated_access_review': 'no_explicit_prohibition_identified'},
    }]
    return config


def page(*, nested=True):
    title = '合成資料的原生研究'
    article = {'@type': 'Article', 'url': URL, 'headline': title,
        'mainEntityOfPage': {'@type': 'WebPage', '@id': URL},
        'publisher': {'@type': 'Organization', 'name': '口袋學堂'},
        'author': {'@type': 'Organization', 'name': POCKET_AUTHOR, 'url': POCKET_ORIGIN + '/school/'},
        'datePublished': RELEASE, 'dateModified': REVISION, 'isAccessibleForFree': True}
    prose = ''.join(f'<p>第{i}段原始研究內容' + '保留負值、比率及零值。' * 8 + '</p>' for i in range(4))
    prose += '<table><thead><tr><th>原始欄</th><th>比率</th></tr></thead><tbody><tr><td>-12.50</td><td>0</td></tr></tbody></table>'
    prose += '<section class="faq-section"><h2>常見問題</h2><p>合成 FAQ 正文。</p></section>'
    prose += '<p><img src="https://images.example.org/chart.png" alt="不是圖中文字"/></p>'
    if nested:
        prose = f'''<style>@scope (.cms-article-scope) {{ p {{color:red}} }}</style><article class="pocket-article">
          <div class="article-meta"><p>更新於 2023-02-15 ｜ {POCKET_AUTHOR}</p></div>{prose}
          <h2 id="section-cta">怎麼買 合成股票？</h2><div><p>口袋證券廣告不是研究正文</p>
          <a href="https://pse.im/78wp56">開戶連結不是研究正文</a></div></article>'''
    return f'''<html><head><title>{title} | 口袋學堂</title>
      <link rel="canonical" href="{URL}"><meta property="og:url" content="{URL}">
      <meta property="og:type" content="article"><meta property="og:title" content="{title}">
      <script type="application/ld+json">{json.dumps(article, ensure_ascii=False)}</script></head>
      <body><nav>外部導航不入正文</nav><main><div><h1>{title}</h1><div class="border-b">
      <img alt="{POCKET_AUTHOR}"><div><span class="font-bold text-slate-600 mr-2">{POCKET_AUTHOR}</span>
      <span>市場分析師</span>更新於 2023/02/15</div></div><div>AI速讀不是正文</div>
      <article class="cms-body"><div class="cms-article-scope">{prose}</div></article>
      <div>作者簡介不是正文</div><aside>推薦閱讀不是正文</aside></div></main></body></html>'''


def parse(settings, html=None, *, url=URL):
    source = PolicyRegistry(settings, base=Path.cwd()).sources['pocket']
    return extract_article((html if html is not None else page()).encode(), source, page_url=url)


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('nested', [True, False])
def test_native_body_once_raw_table_faq_and_separate_current_clocks(settings, parser, nested):
    settings['providers'][0]['html_parser'] = parser
    result = parse(settings, page(nested=nested))
    assert result['published_at_raw'] == RELEASE and result['modified_at_raw'] == REVISION
    assert json.loads(result['authors_json']) == [POCKET_AUTHOR]
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    for i in range(4):
        assert result['body'].count(f'第{i}段') == 1
    assert result['body'].count('保留負值、比率及零值') == 32
    for value in ['-12.50', '0', '原始欄', '比率', '常見問題', '合成 FAQ 正文']:
        assert value in result['body']
    for value in ['不是正文', '不是研究正文', '更新於', POCKET_AUTHOR, '不是圖中文字']:
        assert value not in result['body']
    notices = json.loads(result['verified_article_notices_json'])
    assert notices[0]['visible_date_raw'] == '2023/02/15'
    assert notices[0]['matches_machine_release_Taipei_day']
    assert not notices[0]['matches_machine_revision_Taipei_day']
    assert notices[0]['label_is_not_promoted_to_release_or_revision_clock']
    image = next(n for n in notices if n['kind'].startswith('unfetched_inline'))
    assert not image['media_requested'] and not image['image_data_or_text_inferred']
    if nested:
        assert any('不是研究正文' in n.get('text', '') for n in notices if n['kind'].startswith('separate_publisher'))


@pytest.mark.parametrize('old,new,code', [
    ('rel="canonical"', 'rel="alternate"', 'article_identity_not_verified'),
    ('property="og:type" content="article"', 'property="og:type" content="website"', 'article_identity_not_verified'),
    ('<h1>', '<h1 hidden>', 'article_identity_not_verified'),
    ('class="cms-body"', 'class="cms-body hidden"', 'article_identity_not_verified'),
    ('第2段', '<span aria-hidden="true">第2段</span>', 'article_identity_not_verified'),
    ('第2段', '<span class="sr-only">第2段</span>', 'article_identity_not_verified'),
    ('"name": "口袋學堂編輯團隊"', '"name": "外部供稿"', 'article_rights_or_scope_excluded'),
    ('口袋學堂編輯團隊</span>', '外部供稿</span>', 'article_rights_or_scope_excluded'),
    ('"name": "口袋學堂"', '"name": "其他出版商"', 'article_identity_not_verified'),
    ('"isAccessibleForFree": true', '"isAccessibleForFree": false', 'paywall_or_login'),
    (REVISION, '2022-10-19T03:04:05.678Z', 'invalid_bulk_publication_clock'),
    (REVISION, '2026-02-30T03:04:05.678Z', 'invalid_bulk_publication_clock'),
    (RELEASE, '2023-02-14T23:01:02', 'invalid_bulk_publication_clock'),
    ('更新於 2023/02/15', '更新於 2023/02/30', 'invalid_bulk_publication_clock'),
    ('更新於 2023-02-15', '更新於 2023-02-16', 'invalid_bulk_publication_clock'),
    ('https://pse.im/78wp56', 'https://unknown.example.org/', 'article_identity_not_verified'),
    ('<table>', '<iframe src="/not-reviewed"></iframe><table>', 'article_identity_not_verified'),
    ('"@type": "Article"', '"@type": "Article", "headline": "competing"', 'article_identity_not_verified'),
])
def test_drift_access_and_clock_fail_closed(settings, old, new, code):
    with pytest.raises(NewsError, match=code):
        parse(settings, page().replace(old, new))


@pytest.mark.parametrize('selector', ['link[rel=canonical]', 'main h1', 'article.cms-body', 'script[type="application/ld+json"]'])
def test_ambiguous_native_identity_is_not_first_match(settings, selector):
    soup = BeautifulSoup(page(), 'lxml')
    node = soup.select_one(selector)
    node.insert_after(deepcopy(node))
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, str(soup))


@pytest.mark.parametrize('url', [OLD, URL + '?other=1', URL + '#other', URL.replace('https:', 'http:'),
    URL.replace('www.pocket.tw', 'pocket.tw'), 'https://[bad', URL.replace('12345', '012345')])
def test_native_parser_does_not_guess_aliases_or_scope(settings, url):
    assert pocket_article_id(url) is None
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, url=url)


@pytest.mark.parametrize('field,value', [
    ('domains', ['other.example.org']), ('acquisition_mode', 'finite'),
    ('article_path_pattern', '^/.*$'), ('article_selectors', ['article']),
    ('exclude_selectors', ['p']), ('require_author_attribution', False),
    ('require_configured_container', False), ('title_selector', 'h2'),
    ('feeds', [POCKET_ORIGIN + '/feed/']), ('sitemaps', [POCKET_ORIGIN + '/sitemap.xml']),
])
def test_scope_cannot_expand_with_native_adapter(settings, field, value):
    settings['providers'][0][field] = value
    with pytest.raises(ValueError, match='unreviewed|requires exact|cannot configure'):
        PolicyRegistry(settings, base=Path.cwd())


@pytest.mark.parametrize('final,with_witness,accepted', [(URL, True, True), (URL, False, False),
    (URL.replace('12345', '12346'), True, False)])
def test_fetch_keeps_literal_request_and_requires_actual_same_id_redirect(settings, final, with_witness, accepted):
    registry = PolicyRegistry(settings, base=Path.cwd())
    hop = RedirectHop(OLD, 308, '/school/articles/12345/', final, '2026-10-20T00:00:00Z', 'a' * 64, 22)
    response = Response(final, 200, {'content-type': 'text/html'}, page().encode(), (hop,) if with_witness else ())
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        return response
    http = SimpleNamespace(registry=registry, get=get, metrics=None)
    row = {'url': OLD, 'id': b'a' * 16}
    if accepted:
        result = fetch_article(http, row)
        assert result['requested_url'] == OLD and result['final_url'] == URL
        assert result['url_resolution_basis'].startswith('verified_pocket_literal_same_id_source_redirect_v1')
        assert len(json.loads(result['transport_redirects_json'])) == 1
    else:
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            fetch_article(http, row)
    assert calls == [OLD]


def test_candidate_obeys_installed_source_oracle(settings, monkeypatch):
    import test_news_fulltext as shared
    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(config)
    candidate['providers'] = [p for p in config['providers'] if p['id'] != 'pocket'] + settings['providers']
    registry = PolicyRegistry(candidate, base=repo)
    monkeypatch.setattr(shared, 'load_config', lambda path: (candidate, registry, repo))
    shared.test_default_configuration_separates_personal_capture_from_licenses()


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_exact_native_accessible_table_caption_is_context_not_hidden_article(settings, parser):
    settings['providers'][0]['html_parser'] = parser
    html = page().replace('<table>', '<table><caption class="sr-only">合成表格的年份及單位</caption>')
    result = parse(settings, html)
    assert result['body'].count('合成表格的年份及單位') == 1
    assert any(n['kind'] == 'native_accessible_table_caption_preserved_in_table_text'
               for n in json.loads(result['verified_article_notices_json']))
    for replacement in ['<caption class="hidden">', '<caption class="sr-only" hidden>',
                        '<caption class="sr-only" style="display:none">']:
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            parse(settings, html.replace('<caption class="sr-only">', replacement))


def versioned(settings, version):
    result = deepcopy(settings)
    result['providers'][0]['embedded_article_layout'] = f'pocket_visible_article_v{version}'
    return result


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('nested', [False, True])
def test_v2_preserves_all_previously_accepted_raw_outputs(settings, parser, nested):
    settings['providers'][0]['html_parser'] = parser
    assert parse(settings, page(nested=nested)) == parse(versioned(settings, 2), page(nested=nested))


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_v2_current_source_clock_order_is_recorded_not_rewritten_or_backdated(settings, parser):
    settings['providers'][0]['html_parser'] = parser
    modified = '2023-02-14T22:01:02.345Z'
    html = page().replace(REVISION, modified)
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        parse(settings, html)
    result = parse(versioned(settings, 2), html)
    assert result['published_at_raw'] == RELEASE and result['modified_at_raw'] == modified
    assert not result['historical_point_in_time']
    notices = json.loads(result['verified_article_notices_json'])
    assert next(n for n in notices if n['kind'] == 'source_modification_precedes_publication') == {
        'kind': 'source_modification_precedes_publication', 'declared_publication': RELEASE,
        'declared_modification': modified, 'availability_backdated_to_modification': False,
        'source_reason_inferred': False}


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['repeated_heading', 'native_toc', 'current_cta_and_no_repeated_meta'])
def test_v2_verified_cms_variants_keep_body_once_and_auxiliary_text_separate(settings, parser, change):
    settings['providers'][0]['html_parser'] = parser
    soup = BeautifulSoup(page(), parser)
    native = soup.select_one('article.pocket-article')
    if change == 'repeated_heading':
        native.select_one('.article-meta').insert_after(deepcopy(soup.select_one('main h1')))
    elif change == 'native_toc':
        native.select_one('h2').attrs['id'] = 'faq'
        toc = BeautifulSoup('<nav class="toc" aria-label="文章目錄"><strong>文章目錄</strong>'
            '<ol><li><a href="#faq">常見問題</a></li></ol></nav>', parser).select_one('nav')
        native.insert(1, toc)
    else:
        native.select_one('.article-meta').decompose()
        cta = native.select_one('#section-cta')
        cta.name = 'p'
        cta.attrs['class'] = ['cta-section-title']
        cta.string = '怎麼買合成股票？'
        native.select_one('a[href="https://pse.im/78wp56"]')['href'] = POCKET_ORIGIN + '/openaccountonline/oa/login/new?scope=synthetic'
    html = str(soup)
    with pytest.raises(NewsError):
        parse(settings, html)
    result = parse(versioned(settings, 2), html)
    assert result['body'] == parse(settings)['body']
    assert '不是研究正文' not in result['body'] and '文章目錄' not in result['body']
    assert result['body'].count('常見問題') == 1


@pytest.mark.parametrize('change', ['competing_heading', 'hidden_heading', 'nested_heading', 'external_toc',
    'missing_target', 'duplicate_target', 'hidden_toc', 'unreviewed_toc', 'foreign_cta', 'invalid_clock'])
def test_v2_never_promotes_unverified_body_variants(settings, change):
    soup = BeautifulSoup(page(), 'lxml')
    native = soup.select_one('article.pocket-article')
    if change.endswith('heading'):
        title = deepcopy(soup.select_one('main h1'))
        if change == 'competing_heading': title.string = '另一篇原始文章'
        elif change == 'hidden_heading': title['hidden'] = ''
        if change == 'nested_heading': native.select_one('section').append(title)
        else: native.insert(1, title)
    elif change.endswith('toc') or change.endswith('target'):
        native.select_one('h2')['id'] = 'faq'
        toc = BeautifulSoup('<nav class="toc" aria-label="文章目錄"><strong>文章目錄</strong>'
            '<ol><li><a href="#faq">常見問題</a></li></ol></nav>', 'lxml').select_one('nav')
        if change == 'external_toc': toc.select_one('a')['href'] = 'https://other.example.org/'
        elif change == 'missing_target': toc.select_one('a')['href'] = '#missing'
        elif change == 'duplicate_target': native.append(deepcopy(native.select_one('h2')))
        elif change == 'hidden_toc': toc.select_one('strong')['hidden'] = ''
        else: toc.append(BeautifulSoup('<p>不是已驗證的目錄</p>', 'lxml').select_one('p'))
        native.insert(1, toc)
    elif change == 'foreign_cta':
        native.select_one('a[href="https://pse.im/78wp56"]')['href'] = 'https://www.pocket.tw.evil.example.org/openaccountonline/oa/login/new'
    else:
        soup.select_one('script').string = soup.select_one('script').get_text().replace(REVISION, '2026-02-30T03:04:05.678Z')
    with pytest.raises(NewsError):
        parse(versioned(settings, 2), str(soup))


def test_v2_retry_and_http_identity_are_pinned_to_exact_v1(settings):
    old = PolicyRegistry(settings, base=Path.cwd()).sources['pocket']
    new = PolicyRegistry(versioned(settings, 2), base=Path.cwd()).sources['pocket']
    assert new.data_fingerprint != old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert pocket_native_parser_repair(new, old.data_fingerprint)
    assert not pocket_native_parser_repair(new, new.data_fingerprint)
    changed = versioned(settings, 2)
    changed['providers'][0]['authorization']['scope'] += '; other scope'
    assert not pocket_native_parser_repair(PolicyRegistry(changed, base=Path.cwd()).sources['pocket'], old.data_fingerprint)


@pytest.mark.parametrize('obstruction', [None, 'body_sha256', 'parquet_path', 'lease_owner', 'lease_until',
    'missing_observation', 'future_observation', 'naive_observation', 'different_error', 'unsupported_url',
    'unknown_prior'])
def test_v2_only_missing_observed_unleased_failures_retry_once_with_evidence(settings, tmp_path, obstruction):
    old = PolicyRegistry(settings, base=tmp_path)
    new = PolicyRegistry(versioned(settings, 2), base=tmp_path)
    states = ['article_identity_not_verified', 'invalid_bulk_publication_clock', 'complete',
        'body_not_found', 'article_attribution_not_verified', 'http_403', 'http_429',
        'unsupported_article_path', 'source_not_found', 'paywall_or_login', 'article_rights_or_scope_excluded']
    previous = {}
    with closing(NewsQueue(tmp_path, old)) as queue:
        for number, state in enumerate(states, 1):
            url = POCKET_ORIGIN + f'/school/articles/{number}/'
            queue.add_articles([{'url': url}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=9000000000,'
                'last_checked_at_utc=? WHERE url=?', (state, state, utc_now(), url))
            if obstruction in {'body_sha256', 'parquet_path', 'lease_owner', 'lease_until'}:
                queue.db.execute(f'UPDATE articles SET {obstruction}=? WHERE url=?',
                    (9000000001 if obstruction == 'lease_until' else 'saved-or-owned', url))
            elif obstruction in {'missing_observation', 'future_observation', 'naive_observation'}:
                value = {'missing_observation': None, 'future_observation': '2099-01-01T00:00:00+00:00',
                         'naive_observation': '2026-01-01T00:00:00'}[obstruction]
                queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', (value, url))
            elif obstruction == 'different_error':
                queue.db.execute("UPDATE articles SET error_code='different' WHERE url=?", (url,))
            elif obstruction == 'unsupported_url':
                queue.db.execute('UPDATE articles SET url=url||\'?other=1\' WHERE url=?', (url,))
            row = dict(queue.db.execute('SELECT * FROM articles WHERE id=(SELECT id FROM articles ORDER BY rowid DESC LIMIT 1)').fetchone())
            previous[row['url']] = row
        if obstruction == 'unknown_prior':
            queue.db.execute("UPDATE queue_settings SET value=? WHERE key='source_capture_fingerprint:pocket'", ('f' * 64,))
        queue.db.commit()
    marker = 'pocket_native_parser_recheck_v2:' + new.sources['pocket'].data_fingerprint
    with closing(NewsQueue(tmp_path, new)) as queue:
        for url, row in previous.items():
            eligible = obstruction is None and row['state'] in states[:2]
            expected = {**row, 'state': 'retry'} if eligible else row
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == expected
        record = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
        if obstruction == 'unknown_prior':
            assert record is None
        else:
            evidence = json.loads(record[0])
            assert len(evidence['rows']) == (2 if obstruction is None else 0)
            assert evidence['attempts_due_error_and_observation_preserved'] and not evidence['accepted_as_bodies']
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified' WHERE state='retry'")
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert queue.db.execute("SELECT count(*) FROM articles WHERE state='retry'").fetchone()[0] == 0

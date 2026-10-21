"""Synthetic publisher-native structure only; no copyrighted body fixtures."""
from copy import deepcopy
from contextlib import closing
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_http import RedirectHop, Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, native_text_parser_repair, utc_now
from downloader.news_fulltext_watchmedia import WATCHMEDIA_BODY_SELECTOR, WATCHMEDIA_ORIGIN, WATCHMEDIA_PATH_PATTERN


URL = WATCHMEDIA_ORIGIN + '/archives/12345'
PROFILE = WATCHMEDIA_ORIGIN + '/author/synthetic-reporter'
ORG = WATCHMEDIA_ORIGIN + '/#organization'
TITLE = '合成原始觀測資料'
RELEASE = '2025-08-09T19:32:34+08:00'
REVISION = '2025-08-10T01:02:03+08:00'


@pytest.fixture
def settings():
    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    config = deepcopy(config)
    config['providers'] = [{
        'id': 'watchmedia', 'name': 'Synthetic native source', 'domains': ['www.watchmedia01.com'],
        'enabled': True, 'acquisition_mode': 'continuous',
        'private_known_url_scope': 'personal_noncommercial_originals_v1',
        'interval_seconds': 2, 'parallelism': 1, 'html_parser': 'lxml',
        'embedded_article_layout': 'watchmedia_visible_article_v1',
        'article_path_pattern': WATCHMEDIA_PATH_PATTERN, 'article_selectors': [WATCHMEDIA_BODY_SELECTOR],
        'exclude_selectors': [], 'title_selector': 'h1',
        'require_configured_container': True, 'require_author_attribution': True,
        'authorization': {'kind': 'personal_research', 'reviewed_on': '2026-10-21',
            'evidence_url': WATCHMEDIA_ORIGIN + '/copyright',
            'scope': 'Private known own text only. No media, discovery, redistribution or training grant.',
            'automated_access_review': 'no_explicit_prohibition_identified'},
    }]
    return config


def page(*, name='林合成', head_revision=True):
    seo = TITLE + ' - 觀傳媒'
    graph = [
        {'@type': 'Organization', '@id': ORG, 'name': '觀傳媒'},
        {'@type': 'Person', '@id': PROFILE, 'url': PROFILE, 'name': name, 'worksFor': {'@id': ORG}},
        {'@type': 'WebPage', '@id': URL + '#webpage', 'url': URL, 'name': seo,
         'datePublished': RELEASE, 'dateModified': REVISION},
        {'@type': 'BreadcrumbList', '@id': URL + '#breadcrumb', 'itemListElement': [
            {'item': {'@id': WATCHMEDIA_ORIGIN, 'name': 'Home'}},
            {'item': {'@id': WATCHMEDIA_ORIGIN + '/category/life', 'name': '產經'}},
            {'item': {'@id': URL, 'name': TITLE}}]},
        {'@type': 'NewsArticle', '@id': URL + '#richSnippet', 'headline': seo, 'name': seo,
         'mainEntityOfPage': {'@id': URL + '#webpage'}, 'isPartOf': {'@id': URL + '#webpage'},
         'publisher': {'@id': ORG}, 'author': {'@id': PROFILE, 'name': name},
         'datePublished': RELEASE, 'dateModified': REVISION, 'isAccessibleForFree': True},
    ]
    prose = ''.join(f'<p>第{i}段的合成原始數據。' + '負值零值與比率均保留。' * 5 + '</p>' for i in range(3))
    prose += '<table><caption>合成數據</caption><tr><th>股數</th><th>比率</th></tr><tr><td>-12.50</td><td>0</td></tr></table>'
    prose += '<figure class="wp-block-image"><img src="https://media.invalid/photo.png"><figcaption>另存圖片說明，不是正文</figcaption></figure>'
    revision = f'<meta property="article:modified_time" content="{REVISION}">' if head_revision else ''
    return f'''<html><head><title>{seo}</title><link rel="canonical" href="{URL}">
      <meta property="og:url" content="{URL}"><meta property="og:type" content="article">
      <meta property="og:site_name" content="觀傳媒"><meta property="og:title" content="{seo}">
      <meta property="article:published_time" content="{RELEASE}">{revision}
      <script type="application/ld+json">{json.dumps({'@graph': graph}, ensure_ascii=False)}</script></head>
      <body><header>首頁雜訊與不相關博彩文字不是正文</header>
      <div class="elementor-location-single post-12345" data-elementor-type="single-page">
        <div class="elementor-element"><div class="e-con-inner">
          <div class="elementor-widget-theme-post-title"><div class="elementor-widget-container"><h1>{TITLE}</h1></div></div>
          <div class="elementor-widget-post-info"><ul><li itemprop="author"><a href="{PROFILE}">
          <span class="elementor-post-info__item--type-author"><span class="elementor-post-info__item-prefix">記者 / </span>{name}</span>
          </a></li><li itemprop="datePublished"><time>2025-08-09</time></li></ul></div>
        </div></div>
        <div class="elementor-element"><div class="e-con-inner"><article>
          <div class="elementor-widget-theme-post-content"><div class="elementor-widget-container">{prose}</div></div>
          <aside>相關報導不是正文，記者別人 2026-01-01</aside>
          <div class="e-loop-item"><h3>另一篇完整文章</h3><p>不得混入正文</p><span itemprop="author">其他人</span></div>
        </article></div></div>
      </div><footer>版權與導覽不是正文</footer></body></html>'''


def parse(settings, html=None, *, url=URL):
    source = PolicyRegistry(settings, base=Path.cwd()).sources['watchmedia']
    return extract_article((page() if html is None else html).encode(), source, page_url=url)


def versioned(settings, version):
    result = deepcopy(settings)
    result['providers'][0]['embedded_article_layout'] = f'watchmedia_visible_article_v{version}'
    return result


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('head_revision', [True, False])
def test_exact_native_prose_table_credit_and_separate_clocks(settings, parser, head_revision):
    settings['providers'][0]['html_parser'] = parser
    result = parse(settings, page(head_revision=head_revision))
    assert result['title'] == TITLE and json.loads(result['authors_json']) == ['林合成']
    assert result['published_at_raw'] == RELEASE and result['modified_at_raw'] == REVISION
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    for i in range(3):
        assert result['body'].count(f'第{i}段') == 1
    for value in ['-12.50', '股數', '比率', '0', '合成數據']:
        assert value in result['body']
    for value in ['不是正文', '博彩', '另一篇', '不得混入', '其他人', '圖片說明']:
        assert value not in result['body']
    assert json.loads(result['verified_image_captions_json']) == ['另存圖片說明，不是正文']
    assert 'date_only' == json.loads(result['verified_article_notices_json'])[0]['visible_precision']
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['role'] == 'publisher_declared_reporter'
    assert detail['reporter_real_identity_independently_verified'] is False


@pytest.mark.parametrize('old,new,code', [
    ('rel="canonical"', 'rel="alternate"', 'article_identity_not_verified'),
    ('post-12345', 'post-54321', 'article_identity_not_verified'),
    ('content="article"', 'content="website"', 'article_identity_not_verified'),
    ('content="觀傳媒"', 'content="別的媒體"', 'article_identity_not_verified'),
    ('<h1>', '<h1 hidden>', 'article_identity_not_verified'),
    ('class="elementor-widget-theme-post-content"', 'hidden class="elementor-widget-theme-post-content"', 'article_identity_not_verified'),
    ('第1段', '<span aria-hidden="true">第1段</span>', 'article_identity_not_verified'),
    ('第1段', '<span class="screen-reader-text">第1段</span>', 'article_identity_not_verified'),
    ('第1段', '<iframe src="/unreviewed"></iframe>第1段', 'article_identity_not_verified'),
    ('第1段', '<script>unreviewed()</script>第1段', 'article_identity_not_verified'),
    ('記者 / </span>', '編輯 / </span>', 'article_attribution_not_verified'),
    ('"worksFor": {"@id": "' + ORG + '"}', '"worksFor": {"@id": "https://other.invalid/#org"}', 'article_attribution_not_verified'),
    ('<time>2025-08-09</time>', '<time>2025-08-10</time>', 'invalid_bulk_publication_clock'),
    (RELEASE, '2025-02-30T19:32:34+08:00', 'invalid_bulk_publication_clock'),
    (RELEASE, '2025-08-09T19:32:34', 'invalid_bulk_publication_clock'),
    (REVISION, '2025-08-08T01:02:03+08:00', 'invalid_bulk_publication_clock'),
    ('"isAccessibleForFree": true', '"isAccessibleForFree": false', 'paywall_or_login'),
    ('第0段的合成原始數據。', '新聞來源：合成第三方', 'article_rights_or_scope_excluded'),
    ('<figure class="wp-block-image">', '<figure class="wp-block-image"><p>不得捨棄這段正文</p>', 'article_identity_not_verified'),
])
@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('version', [1, 2, 3])
def test_reject_ambiguous_identity_hidden_content_media_and_clock(settings, old, new, code, parser, version):
    settings = versioned(settings, version)
    settings['providers'][0]['html_parser'] = parser
    html = page()
    assert old in html
    with pytest.raises(NewsError) as error:
        parse(settings, html.replace(old, new))
    assert error.value.code == code


@pytest.mark.parametrize('name', ['樂聯網', '享新聞', '警政時報', '中央社', '觀傳媒編輯團隊'])
@pytest.mark.parametrize('version', [1, 2, 3])
def test_cms_reporter_prefix_does_not_turn_partner_org_into_own_reporter(settings, name, version):
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        parse(versioned(settings, version), page(name=name))


def test_absent_native_byline_is_not_reconstructed_from_photo_or_related_story(settings):
    html = page().replace('itemprop="author"', 'itemprop="photoAuthor"')
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        parse(settings, html)


@pytest.mark.parametrize('update', [
    {'domains': ['another.invalid']}, {'article_selectors': ['article']},
    {'exclude_selectors': ['p']}, {'require_author_attribution': False},
    {'feeds': [WATCHMEDIA_ORIGIN + '/feed/']}, {'article_path_pattern': '^/.*$'},
])
@pytest.mark.parametrize('version', [1, 2, 3])
def test_recipe_cannot_expand_host_body_or_discovery_scope(settings, update, version):
    settings = versioned(settings, version)
    settings['providers'][0].update(update)
    with pytest.raises(ValueError):
        PolicyRegistry(settings, base=Path.cwd())


@pytest.mark.parametrize('version', [1, 2, 3])
def test_actual_legacy_redirect_and_original_title_are_both_required(settings, version):
    settings = versioned(settings, version)
    registry = PolicyRegistry(settings, base=Path.cwd())
    legacy = WATCHMEDIA_ORIGIN + '/bnews-20210908043901.html'
    hop = RedirectHop(legacy, 301, URL, URL, '2026-10-20T18:22:10+00:00', 'e' * 64, 0)
    response = Response(URL, 200, {'content-type': 'text/html'}, page().encode(), (hop,))
    http = SimpleNamespace(registry=registry, get=lambda *a, **kw: response)
    row = {'url': legacy, 'id': b'\x01' * 16, 'title': TITLE + ' - watchmedia01.com'}
    result = fetch_article(http, row)
    assert result['requested_url'] == legacy and result['final_url'] == URL
    assert 'source301_and_metadata_title_v1' in result['url_resolution_basis']
    assert result['published_at_raw'] == RELEASE  # Never derive from the legacy URL.
    assert json.loads(result['transport_redirects_json'])[0]['from_url'] == legacy
    for bad in [None, '同日別篇新聞']:
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            fetch_article(http, {**row, 'title': bad})


@pytest.mark.parametrize('version', [1, 2, 3])
def test_native_numeric_story_must_not_redirect_to_a_different_story(settings, version):
    settings = versioned(settings, version)
    registry = PolicyRegistry(settings, base=Path.cwd())
    response = Response(URL, 200, {'content-type': 'text/html'}, page().encode())
    http = SimpleNamespace(registry=registry, get=lambda *a, **kw: response)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        fetch_article(http, {'url': WATCHMEDIA_ORIGIN + '/archives/99999', 'id': b'\x02' * 16})


@pytest.mark.parametrize('version', [1, 2, 3])
def test_unknown_query_is_rejected_before_http(settings, version):
    settings = versioned(settings, version)
    registry = PolicyRegistry(settings, base=Path.cwd())
    def forbidden(*args, **kwargs):
        raise AssertionError('unreviewed request must not be sent')
    with pytest.raises(NewsError, match='unsupported_article_path'):
        fetch_article(SimpleNamespace(registry=registry, get=forbidden), {'url': URL + '?other=1'})


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_validation_metadata_describes_identity_without_prose_or_acceptance(parser):
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_watchmedia import watchmedia_validation_metadata
    observed = watchmedia_validation_metadata(BeautifulSoup(page(), parser), page_url=URL)
    assert observed['article_schema_count'] == observed['webpage_schema_count'] == 1
    assert observed['breadcrumbs'][0]['item_count'] == 3
    assert observed['headings'][0]['value'] == TITLE
    assert observed['articles'][0]['headline']['value'] == TITLE + ' - 觀傳媒'
    assert observed['articles'][0]['published_raw'] == RELEASE
    assert observed['visible_credits'][0]['value'] == '記者 / 林合成'
    assert observed['body_container_count'] == 1
    assert observed['additional_requests'] == 0
    assert not observed['article_identity_or_scope_granted']
    assert not observed['body_text_retained'] and not observed['raw_response_retained']
    serialized = json.dumps(observed, ensure_ascii=False)
    for forbidden in ('第0段', '負值零值', '圖片說明', '博彩', '另一篇完整文章'):
        assert forbidden not in serialized


@pytest.mark.parametrize('change', ['different_schema_title', 'extra_breadcrumb', 'invalid_json', 'query', 'body_schema'])
def test_validation_metadata_exposes_bounded_missing_evidence_without_filling_it(change):
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_watchmedia import watchmedia_validation_metadata
    soup = BeautifulSoup(page(), 'lxml')
    script = soup.select_one('script[type="application/ld+json"]')
    payload = json.loads(script.string)
    article = payload['@graph'][-1]
    if change == 'different_schema_title':
        article['headline'] = '另一個原生宣告標題'
    elif change == 'extra_breadcrumb':
        payload['@graph'][-2]['itemListElement'].insert(1, {'item': {'name': '多一層分類'}})
    elif change == 'query':
        payload['@graph'][2]['url'] = URL + '?secret=must-not-leak#fragment'
    elif change == 'body_schema':
        article['articleBody'] = '不得存出的逐字正文'
    script.string = '{bad' if change == 'invalid_json' else json.dumps(payload, ensure_ascii=False)
    # Assigning .string creates a generic NavigableString, not the Script
    # subclass produced by an actual HTTP document. Reparse that document.
    soup = BeautifulSoup(str(soup), 'lxml')
    observed = watchmedia_validation_metadata(soup, page_url=URL)
    if change == 'different_schema_title':
        assert observed['articles'][0]['headline']['value'] != observed['headings'][0]['value'] + ' - 觀傳媒'
    elif change == 'extra_breadcrumb':
        assert observed['breadcrumbs'][0]['item_count'] == 4
    elif change == 'invalid_json':
        assert observed['schema_error'] and not observed['articles']
    elif change == 'query':
        assert observed['webpages'][0]['url']['display_url'] == URL
    elif change == 'body_schema':
        assert observed['articles'][0]['body_declared']
    serialized = json.dumps(observed, ensure_ascii=False)
    assert 'must-not-leak' not in serialized and '不得存出的逐字正文' not in serialized
    assert not observed['article_identity_or_scope_granted']


def test_validation_metadata_caps_display_values_and_nodes():
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_watchmedia import watchmedia_validation_metadata
    soup = BeautifulSoup(page(), 'lxml')
    soup.h1.string = '長' * 5000
    for _ in range(20):
        node = soup.new_tag('meta', attrs={'property': 'og:title', 'content': '長' * 5000})
        soup.head.append(node)
    observed = watchmedia_validation_metadata(soup, page_url=URL)
    assert len(observed['headings'][0]['value']) == 512
    assert observed['headings'][0]['truncated']
    assert len(observed['og_titles']) == 8
    assert len(json.dumps(observed, ensure_ascii=False).encode()) < 65536


@pytest.mark.parametrize('tag', ['iframe', 'font', 'unknown-native-layout'])
def test_validation_metadata_preserves_only_bounded_body_structure_not_element_text(tag):
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_watchmedia import watchmedia_validation_metadata
    html = page().replace('第0段', f'<{tag}>不得留存的逐字正文</{tag}>第0段')
    observed = watchmedia_validation_metadata(BeautifulSoup(html, 'lxml'), page_url=URL)
    assert observed['contract_version'] == 2
    structure = observed['native_body_structures'][0]
    assert tag in structure['unrecognized_tag_names'] and structure['tag_counts'][tag] == 1
    assert structure['diagnostic_only_not_body_acceptance']
    assert '不得留存' not in json.dumps(observed, ensure_ascii=False)


def test_validation_metadata_bounds_body_topology_without_acceptance():
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_watchmedia import watchmedia_validation_metadata
    html = page().replace('第0段', '<span>不得留存</span>' * 5000 + '第0段')
    observed = watchmedia_validation_metadata(BeautifulSoup(html, 'lxml'), page_url=URL)
    structure = observed['native_body_structures'][0]
    assert structure['elements_truncated'] and structure['sampled_element_count'] == 4096
    assert not observed['article_identity_or_scope_granted'] and not observed['body_text_retained']
    assert len(json.dumps(observed, ensure_ascii=False).encode()) < 65536


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('head_revision', [True, False])
def test_v2_does_not_rewrite_already_valid_native_values(settings, parser, head_revision):
    settings['providers'][0]['html_parser'] = parser
    html = page(head_revision=head_revision)
    assert parse(versioned(settings, 2), html) == parse(settings, html)


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('visible,declared', [
    ('合成  原始資料 -12.50', '合成 原始資料 -12.50'),
    ('合成\u3000\u00a0原始資料 -12.50', '合成 原始資料 -12.50'),
    ('合成\t\n原始資料 -12.50', '合成 原始資料 -12.50'),
    ('合成 原始資料 -12.50', '合成 原始資料 -12.50\u3000'),
])
def test_v2_compares_only_unicode_whitespace_and_retains_originals(settings, parser, visible, declared):
    settings['providers'][0]['html_parser'] = parser
    html = page().replace(TITLE, declared).replace(f'<h1>{declared}</h1>', f'<h1>{visible}</h1>')
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, html)
    result = parse(versioned(settings, 2), html)
    original = parse(settings)
    for field in ('body', 'authors_json', 'published_at_raw', 'modified_at_raw', 'verified_image_captions_json'):
        assert result[field] == original[field]
    assert result['title'] == visible
    notices = [item for item in json.loads(result['verified_article_notices_json'])
               if item['kind'] == 'native_title_whitespace_equivalence']
    assert len(notices) == 5
    for item in notices:
        expected = declared if item['field'] == 'BreadcrumbList.last.name' else declared + ' - 觀傳媒'
        assert item['source_raw'] == expected
        assert item['comparison_only_original_values_retained']


@pytest.mark.parametrize('visible,declared', [
    ('合成原始資料', '合成 原始資料'),
    ('合成 原始資料', '合成原始資料'),
    ('合成 -12.50', '合成 12.50'),
    ('合成 -12.50', '合成 -12.51'),
    ('合成 USD', '合成 usd'),
    ('合成：資料', '合成:資料'),
    ('合成\u200b資料', '合成資料'),
])
def test_v2_still_rejects_changed_tokens_numbers_punctuation_or_hidden_characters(settings, visible, declared):
    html = page().replace(TITLE, declared).replace(f'<h1>{declared}</h1>', f'<h1>{visible}</h1>')
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(versioned(settings, 2), html)


def test_v2_retry_and_http_identity_require_exact_v1_without_rights_changes(settings):
    old = PolicyRegistry(settings, base=Path.cwd()).sources['watchmedia']
    new = PolicyRegistry(versioned(settings, 2), base=Path.cwd()).sources['watchmedia']
    assert new.data_fingerprint != old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert native_text_parser_repair(new, old.data_fingerprint)
    assert not native_text_parser_repair(new, new.data_fingerprint)
    changed = versioned(settings, 2)
    changed['providers'][0]['authorization']['scope'] += '; another scope'
    assert not native_text_parser_repair(
        PolicyRegistry(changed, base=Path.cwd()).sources['watchmedia'], old.data_fingerprint)


@pytest.mark.parametrize('version', [1, 2, 3])
def test_candidate_obeys_installed_source_oracle(settings, monkeypatch, version):
    import test_news_fulltext as shared
    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(config)
    candidate['providers'] = [p for p in config['providers'] if p['id'] != 'watchmedia'] + versioned(settings, version)['providers']
    registry = PolicyRegistry(candidate, base=repo)
    monkeypatch.setattr(shared, 'load_config', lambda path: (candidate, registry, repo))
    shared.test_default_configuration_separates_personal_capture_from_licenses()


@pytest.mark.parametrize('obstruction', [None, 'body_sha256', 'parquet_path', 'lease_owner', 'lease_until',
    'missing_observation', 'future_observation', 'naive_observation', 'different_error', 'unsupported_url',
    'unknown_prior'])
@pytest.mark.parametrize('version', [2, 3])
def test_versioned_only_requeues_observed_missing_identity_once_without_resetting_evidence(settings, tmp_path, obstruction, version):
    old = PolicyRegistry(versioned(settings, version - 1), base=tmp_path)
    new = PolicyRegistry(versioned(settings, version), base=tmp_path)
    states = ['article_identity_not_verified', 'invalid_bulk_publication_clock', 'complete',
        'body_not_found', 'article_attribution_not_verified', 'http_403', 'http_429',
        'unsupported_article_path', 'source_not_found', 'paywall_or_login', 'article_rights_or_scope_excluded']
    before = {}
    with closing(NewsQueue(tmp_path, old)) as queue:
        for number, state in enumerate(states, 1):
            url = WATCHMEDIA_ORIGIN + f'/archives/{number}'
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
            queue.db.execute("UPDATE queue_settings SET value=? WHERE key='source_capture_fingerprint:watchmedia'", ('f' * 64,))
        queue.db.commit()
    marker = f'watchmedia_native_parser_recheck_v{version}:' + new.sources['watchmedia'].data_fingerprint
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
            assert evidence['attempts_due_error_and_observation_preserved']
            assert not evidence['accepted_as_bodies']
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified' WHERE state='retry'")
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert queue.db.execute("SELECT count(*) FROM articles WHERE state='retry'").fetchone()[0] == 0


def ellipsis_page(visible, seo, breadcrumb):
    from bs4 import BeautifulSoup
    html = page().replace(TITLE, seo).replace(f'<h1>{seo}</h1>', f'<h1>{visible}</h1>')
    soup = BeautifulSoup(html, 'lxml')
    script = soup.select_one('script[type="application/ld+json"]')
    payload = json.loads(script.string)
    payload['@graph'][-2]['itemListElement'][-1]['item']['name'] = breadcrumb
    script.string = json.dumps(payload, ensure_ascii=False)
    return str(soup)


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('parts', [('…', '...', '&#8230;'), ('...', '…', '…'), ('…', '...', '...')])
def test_v3_only_compares_observed_ellipsis_forms_preserving_all_original_values(settings, parser, parts):
    settings['providers'][0]['html_parser'] = parser
    visible, seo, breadcrumb = [f'合成{part}009816漲幅16.8% 費用0.01% -12.50' for part in parts]
    html = ellipsis_page(visible, seo, breadcrumb)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(versioned(settings, 2), html)
    result = parse(versioned(settings, 3), html)
    assert result['title'] == visible
    original = parse(settings)
    for field in ('body', 'authors_json', 'published_at_raw', 'modified_at_raw', 'verified_image_captions_json'):
        assert result[field] == original[field]
    notices = [item for item in json.loads(result['verified_article_notices_json'])
               if item['kind'] == 'native_title_ellipsis_representation_equivalence']
    assert notices
    for item in notices:
        assert item['source_raw'] == (breadcrumb if item['field'] == 'BreadcrumbList.last.name' else seo + ' - 觀傳媒')
        assert item['comparison_only_original_values_retained']


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('title', ['合成 原始資料 -12.50', '合成\u3000\u00a0原始資料 -12.50'])
def test_v3_keeps_all_v2_valid_outputs_and_notices_exactly_unchanged(settings, parser, title):
    settings['providers'][0]['html_parser'] = parser
    html = page().replace(TITLE, '合成 原始資料 -12.50').replace('<h1>合成 原始資料 -12.50</h1>', f'<h1>{title}</h1>')
    assert parse(versioned(settings, 3), html) == parse(versioned(settings, 2), html)


@pytest.mark.parametrize('other', ['....', '.....', '.', '..', '&#x2026;', '&hellip;', '&#038;#8230;',
    '−12.50', '12.51', 'USD', '：', '\u200b'])
def test_v3_does_not_generally_decode_entities_punctuation_numbers_or_other_dot_counts(settings, other):
    visible = '合成…原始資料 -12.50 usd:'
    html = ellipsis_page(visible, visible, '合成' + other + '原始資料 -12.50 usd:')
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(versioned(settings, 3), html)


def test_v3_needs_exact_v2_and_keeps_the_original_v1_http_identity(settings):
    sources = [PolicyRegistry(versioned(settings, version), base=Path.cwd()).sources['watchmedia'] for version in (1, 2, 3)]
    first, previous, current = sources
    assert len({source.data_fingerprint for source in sources}) == 3
    assert current.parser_repair_prior_capture_fingerprint == previous.data_fingerprint
    assert previous.http_capacity_source_fingerprint == current.http_capacity_source_fingerprint == first.data_fingerprint
    assert len({source.authorization_fingerprint for source in sources}) == 1
    assert native_text_parser_repair(current, previous.data_fingerprint)
    assert not native_text_parser_repair(current, first.data_fingerprint)
    assert not native_text_parser_repair(current, current.data_fingerprint)
    changed = versioned(settings, 3)
    changed['providers'][0]['authorization']['scope'] += '; another scope'
    assert not native_text_parser_repair(PolicyRegistry(changed, base=Path.cwd()).sources['watchmedia'], previous.data_fingerprint)

"""Mirror TV native reports use the shared collector; fixtures contain no source prose."""
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import load_config, run_cycle
from downloader.news_fulltext_mnews import MNEWS_BODY_SELECTOR, MNEWS_PATH_PATTERN, mnews_visible_article
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_news_fulltext import FakeResponse, http_fixture


URL = 'https://www.mnews.tw/story/20260725nm005'
TITLE = '原始台股報導：股數123456、負值-12.5%'
LEAD = '這是原頁可見導言，不以JSON摘要取代。' * 4
PROSE = '實際報導原值為123456股、-12.5%與0.75，不能做log或以摘要代替。' * 5
NOTICE = '★《鏡新聞》提醒您：原始風險提示須保留。'


def recipe():
    return {'id': 'mnews', 'name': 'Mirror TV fixture', 'domains': ['www.mnews.tw'], 'enabled': True,
        'acquisition_mode': 'continuous', 'private_known_url_scope': 'personal_noncommercial_originals_v1',
        'interval_seconds': 2, 'parallelism': 1, 'capacity_max_parallelism': 2,
        'authorization': {'kind': 'personal_research', 'reviewed_on': '2026-10-15',
            'evidence_url': 'https://www.mnews.tw/story/webauthorization',
            'automated_access_review': 'no_explicit_prohibition_identified',
            'scope': 'Known public own reports for private reading; not a license, redistribution or training grant'},
        'html_parser': 'lxml', 'embedded_article_layout': 'mnews_visible_article_v1',
        'article_path_pattern': MNEWS_PATH_PATTERN, 'article_selectors': [MNEWS_BODY_SELECTOR],
        'exclude_selectors': [], 'title_selector': 'h1', 'require_configured_container': True,
        'require_author_attribution': True, 'feeds': [], 'sitemaps': [], 'wordpress_apis': [], 'fulltext_feeds': []}


def policy_config(parser='lxml'):
    cfg, _, _ = load_config(Path('configs/news_fulltext.json'))
    # Synthetic transport/receipt tests must not depend on the host's actual
    # production disk reserve. The explicit low-disk test below keeps that
    # guard and automatic recovery independently exercised.
    return {**cfg, 'min_free_disk_bytes': 1, 'providers': [{**recipe(), 'html_parser': parser}]}


def policy(parser='lxml'):
    return PolicyRegistry(policy_config(parser), base=Path.cwd()).sources['mnews']


def page(parser='lxml', url=URL, name='甲作者'):
    declared = {'@context': 'https://schema.org', '@type': 'NewsArticle', 'headline': TITLE,
        'url': url, 'mainEntityOfPage': {'@type': 'WebPage', '@id': url},
        'datePublished': '2026-07-25T16:56:24+08:00', 'dateModified': '2026-07-25T16:57:22+08:00',
        'author': {'@type': 'Person', 'name': name}, 'description': '摘要絕不可升格為全文',
        'publisher': {'@type': 'Organization', 'name': '鏡新聞'}}
    crumbs = {'@type': 'BreadcrumbList', 'itemListElement': [{'@type': 'ListItem', 'position': 3,
        'name': TITLE, 'item': url}]}
    person = {'@type': 'Person', 'name': name, 'brand': {'@type': 'Brand', 'name': '鏡新聞', 'url': 'https://www.mnews.tw'}}
    return BeautifulSoup(f'''<html><head><meta charset="utf-8"><title>{TITLE}</title>
        <link rel="canonical" href="{url}"><meta property="og:url" content="{url}">
        <meta property="og:title" content="{TITLE}"><meta property="article:author" content="{name}">
        <meta property="article:published_time" content="{declared['datePublished']}">
        <meta property="article:modified_time" content="{declared['dateModified']}"></head><body>
        <section class="story_story__fixture"><main class="story_article__fixture">
        <script type="application/ld+json">{json.dumps(declared, ensure_ascii=False)}</script>
        <script type="application/ld+json">{json.dumps(crumbs, ensure_ascii=False)}</script>
        <script type="application/ld+json">{json.dumps(person, ensure_ascii=False)}</script>
        <section class="story_article__fixture">
        <figure><img src="https://media.invalid/not-requested.jpg"><figcaption>主圖說明／外部攝影</figcaption></figure>
        <section class="article-info_categoryAndPublishTime__fixture"><time class="amp-pub-cat-publishTime"
          datetime="{declared['datePublished']}">2026.07.25 16:56 臺北時間</time></section>
        <h1 class="article-info_title__fixture">{TITLE}</h1>
        <ul class="article-info_credit__fixture"><li>記者<!-- -->｜<!-- -->{name}</li><li></li></ul>
        <div class="article-brief_briefWrapper__fixture"><article class="api-data-renderer_apiDataArticle__fixture"><p>{LEAD}</p></article></div>
        <section class="story_contentWrapper__fixture"><article class="api-data-renderer_apiDataArticle__fixture">
        <p>{PROSE}</p><div class="gpt-ad_reserver__fixture"><div class="gpt-ad_ad__fixture"></div></div>
        <figure><img src="https://media.invalid/not-requested-again.jpg"><figcaption>內文圖說另存</figcaption></figure>
        <section class="embed-code-block_container__fixture"><div class="embed"></div></section><p>{NOTICE}</p>
        </article><div class="article-update-time_updateTime__fixture"><time datetime="{declared['dateModified']}">2026.07.25 16:57 臺北時間</time></div></section>
        <section class="story_socialAndRelatedWrapper__fixture"><h1>無關標題</h1><p>無關報導不可混入正文</p></section>
        </section></main><aside><article>旁欄完整文章不可混入</article></aside></section></body></html>''', parser)


def extract(soup, parser='lxml', url=URL):
    return extract_article(str(soup).encode(), policy(parser), page_url=url)


def schema(soup, index, **changes):
    node = soup.select('script[type="application/ld+json"]')[index]
    value = json.loads(node.string)
    value.update(changes)
    node.string = json.dumps(value, ensure_ascii=False)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_complete_lead_and_continuation_preserve_values_and_independent_clocks(parser):
    soup = page(parser)
    before = str(soup)
    result = extract(soup, parser)
    assert result['body'] == LEAD + '\n\n' + PROSE + '\n' + NOTICE
    assert result['authors_json'] == '["甲作者"]'
    assert result['published_at_raw'] == '2026-07-25T16:56:24+08:00'
    assert result['modified_at_raw'] == '2026-07-25T16:57:22+08:00'
    assert result['declared_canonical_url'] == URL
    assert result['historical_point_in_time'] is False
    assert result['per_article_license_verified'] is False
    assert json.loads(result['verified_image_captions_json']) == ['主圖說明／外部攝影', '內文圖說另存']
    assert 'whole_native_report_lead_and_continuation' in result['body_completeness']
    assert json.loads(result['author_attribution_detail_json'])['publisher_used_as_named_reporter'] is False
    assert not any(v in result['body'] for v in ('摘要絕不可', '記者', '內文圖說', '無關報導', '旁欄完整'))
    assert str(soup) == before


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('url', [URL.replace('https:', 'http:'), URL + '?amp=1', URL + '#x',
    URL.replace('www.mnews.tw', 'mnews.tw'), URL.replace('www.mnews.tw', 'other.mnews.tw'),
    URL.replace('/story/', '/story/amp/'), URL.replace('/story/', '/external/'),
    URL.replace('20260725nm005', 'mm-20260725edi005')])
def test_foreign_or_unverified_views_are_not_normalized_to_originals(parser, url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        mnews_visible_article(page(parser, url), page_url=url)


@pytest.mark.parametrize('field,value', [('headline', '別篇標題'), ('url', URL + '1'),
    ('mainEntityOfPage', {'@type': 'WebPage', '@id': URL + '1'}), ('publisher', {'@type': 'Organization', 'name': '別家媒體'}),
    ('articleBody', '不允許schema暗換全文'), ('mainEntityOfPage', URL)])
def test_declared_current_article_identity_is_required(field, value):
    soup = page(); schema(soup, 0, **{field: value})
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup)


@pytest.mark.parametrize('selector,attr,value', [('link[rel=canonical]', 'href', URL + '2'),
    ('meta[property="og:url"]', 'content', URL + '2'), ('meta[property="og:title"]', 'content', '別篇')])
def test_independent_url_and_title_declarations_must_agree(selector, attr, value):
    soup = page(); soup.select_one(selector)[attr] = value
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup)


@pytest.mark.parametrize('field', ['datePublished', 'dateModified'])
@pytest.mark.parametrize('value', ['2026-07-25', '2026-07-25T16:56:24', '2026-07-25T08:56:24Z',
    '2026-13-25T16:56:24+08:00', '1970-01-01T08:00:00+08:00', None])
def test_missing_unknown_or_conflicting_clocks_never_use_metadata_or_capture_time(field, value):
    soup = page(); schema(soup, 0, **{field: value})
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        extract(soup)


@pytest.mark.parametrize('selector,mutation', [
    ('time.amp-pub-cat-publishTime', 'text'), ('[class^="article-update-time_"] time', 'text'),
    ('time.amp-pub-cat-publishTime', 'datetime'), ('meta[property="article:modified_time"]', 'content')])
def test_native_visible_and_machine_clocks_are_independent_proof(selector, mutation):
    soup = page(); node = soup.select_one(selector)
    if mutation == 'text': node.string = '2026.07.25 16:59 臺北時間'
    else: node[mutation] = '2026-07-25T16:59:24+08:00'
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        extract(soup)


@pytest.mark.parametrize('change', ['native', 'meta', 'schema', 'brand', 'hidden', 'duplicate'])
def test_current_reporter_requires_visible_credit_meta_schema_and_own_brand(change):
    soup = page()
    if change == 'native': soup.select_one('[class^="article-info_credit__"] li').string = '記者｜別人'
    if change == 'meta': soup.select_one('meta[property="article:author"]')['content'] = '別人'
    if change == 'schema': schema(soup, 0, author={'@type': 'Organization', 'name': '甲作者'})
    if change == 'brand': schema(soup, 2, brand={'name': '鏡報', 'url': 'https://www.mnews.tw'})
    if change == 'hidden': soup.select_one('[class^="article-info_credit__"] li')['hidden'] = ''
    if change == 'duplicate': soup.select_one('[class^="article-info_credit__"]').append(deepcopy(soup.select_one('[class^="article-info_credit__"] li')))
    with pytest.raises(NewsError, match='article_attribution_not_verified|article_identity_not_verified'):
        extract(soup)


def test_publisher_name_is_not_a_named_reporter():
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract(page(name='鏡新聞'))


def test_external_credit_is_not_promoted_even_with_a_matching_schema():
    soup = page(); soup.select_one('[class^="article-info_credit__"] li').string = '來源｜鏡報'
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(soup)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_video_only_body_does_not_turn_lead_or_publisher_name_into_full_report(parser):
    soup = page(parser, name='鏡新聞')
    report = soup.select('[class^="api-data-renderer_apiDataArticle__"]')[1]
    report.clear()
    report.append(BeautifulSoup('<div class="youtube-block_youtubeContainer__fixture"><iframe src="https://www.youtube.com/embed/aBcD_eF-123?si=原標題" title="原標題"></iframe></div>', parser).div)
    with pytest.raises(NewsError, match='source_body_embedded_only'):
        extract(soup, parser)


@pytest.mark.parametrize('mutation', ['no_lead', 'no_report', 'extra_report', 'text_in_ad', 'text_in_embed',
    'hidden_prose', 'script_in_report', 'license_override', 'hidden_lead'])
def test_whole_prose_and_visibility_cannot_be_relaxed_to_improve_success_count(mutation):
    soup = page(); brief, report = soup.select('[class^="api-data-renderer_apiDataArticle__"]')
    if mutation == 'no_lead': brief.clear()
    if mutation == 'no_report': report.clear()
    if mutation == 'extra_report': report.parent.append(deepcopy(report))
    if mutation == 'text_in_ad': report.select_one('[class^="gpt-ad_reserver__"]').string = '不是已驗空白廣告'
    if mutation == 'text_in_embed': report.select_one('[class^="embed-code-block_"]').string = '外部作者文字不可暗中丟掉'
    if mutation == 'hidden_prose': report.p['style'] = 'display: none'
    if mutation == 'hidden_lead': brief['aria-hidden'] = 'true'
    if mutation == 'script_in_report': report.append(soup.new_tag('script'))
    if mutation == 'license_override':
        n = soup.new_tag('a', rel='license', href='https://rights.invalid'); report.append(n)
    with pytest.raises(NewsError): extract(soup)


@pytest.mark.parametrize('mutation', ['duplicate_schema', 'malformed_schema', 'duplicate_json_key', 'paid'])
def test_schema_and_access_fail_closed(mutation):
    soup = page(); node = soup.select('script[type="application/ld+json"]')[0]
    if mutation == 'duplicate_schema': node.parent.append(deepcopy(node))
    if mutation == 'malformed_schema': node.string = '{not-json}'
    if mutation == 'duplicate_json_key': node.string = node.string.replace('"headline":', '"headline":"competing", "headline":', 1)
    if mutation == 'paid': schema(soup, 0, isAccessibleForFree=False)
    with pytest.raises(NewsError): extract(soup)


@pytest.mark.parametrize('change', [{'domains': ['mnews.tw']}, {'acquisition_mode': 'finite'},
    {'private_known_url_scope': 'unknown'}, {'article_selectors': ['article']}, {'exclude_selectors': ['p']},
    {'article_path_pattern': '^/story/'}, {'require_author_attribution': False}, {'require_configured_container': False},
    {'authorization': {'kind': 'personal_research', 'evidence_url': 'https://elsewhere.invalid',
                       'automated_access_review': 'no_explicit_prohibition_identified'}}])
def test_native_recipe_cannot_grant_other_hosts_endpoints_or_parse_scope(change):
    cfg = policy_config(); cfg['providers'][0].update(change)
    with pytest.raises(ValueError): PolicyRegistry(cfg, base=Path.cwd())


def test_native_written_report_round_trip_uses_shared_queue_http_and_durable_receipts(tmp_path):
    cfg = policy_config()
    http, _ = http_fixture(cfg, tmp_path, {'https://www.mnews.tw/robots.txt': FakeResponse(body=b'User-agent: *\nAllow: /'),
        URL: FakeResponse(body=str(page()).encode())})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{'url': URL}])
        result = run_cycle(queue, http, cfg, max_articles=1, max_discovery_requests=0)
        assert result['states'] == {'complete': 1}
        assert verify_bodies(tmp_path)['accepted']
        assert len(list((tmp_path/'receipts').glob('*.json'))) == 1
        assert queue.db.execute('SELECT url FROM articles').fetchone()[0] == URL
    finally:
        http.close(); queue.close()


def test_low_disk_preserves_work_without_http_and_same_owner_recovers(monkeypatch, tmp_path):
    cfg = policy_config()
    cfg['min_free_disk_bytes'] = 1024
    http, session = http_fixture(cfg, tmp_path, {
        'https://www.mnews.tw/robots.txt': FakeResponse(body=b'User-agent: *\nAllow: /'),
        URL: FakeResponse(body=str(page()).encode())})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{'url': URL}])
        original = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        monkeypatch.setattr('downloader.news_fulltext.shutil.disk_usage', lambda path: SimpleNamespace(free=1023))
        paused = run_cycle(queue, http, cfg, max_articles=1, max_discovery_requests=0)
        assert paused['activity'] == 'waiting_disk'
        assert session.calls == []
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == original
        # Equality is sufficient for the existing reserve; no threshold,
        # deadline, source contract or owner reset is used to resume.
        monkeypatch.setattr('downloader.news_fulltext.shutil.disk_usage', lambda path: SimpleNamespace(free=1024))
        resumed = run_cycle(queue, http, cfg, max_articles=1, max_discovery_requests=0)
        assert resumed['states'] == {'complete': 1}
        assert resumed['owner_id'] == paused['owner_id']
        assert len(session.calls) == 2  # Actual fixture robots plus report.
        assert verify_bodies(tmp_path)['accepted']
    finally:
        http.close(); queue.close()


def test_installed_source_cutover_is_explicit_and_keeps_the_closed_default_set(monkeypatch):
    import test_news_fulltext as shared
    cfg, _, repo = load_config(Path('configs/news_fulltext.json'))
    cfg = deepcopy(cfg)
    cfg['providers'] = [entry for entry in cfg['providers'] if entry['id'] != 'mnews'] + [recipe()]
    registry = PolicyRegistry(cfg, base=repo)
    monkeypatch.setattr(shared, 'load_config', lambda path: (cfg, registry, repo))
    shared.test_default_configuration_separates_personal_capture_from_licenses()

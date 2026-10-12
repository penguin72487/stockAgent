"""Source-backed CMNews SSR scope: no related/schema/hydration text fallback."""
import json
from pathlib import Path
from bs4 import BeautifulSoup

import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article

UUID = "82e568db-c3a9-11f1-b154-4965150594c4"
SLUG = "forumnews-" + UUID
URL = "https://cmnews.com.tw/article/" + SLUG
AUTHOR = "財經焦點情報站"
TEXT = "來源原始營收 -12.5，成交量 12345 股，這是本篇正文。" * 10


@pytest.fixture
def source():
    return load_config(Path("configs/news_fulltext.json"))[1].sources["cmnews"]


def page(*, canonical=URL, slug=SLUG, author_id=SLUG, container_id="ArticleContainer-" + UUID,
         author_href="/forumnews", clock="2026-10-09 14:20", schema_clock="2026-10-09T14:20:23+08:00",
         schema_author=AUTHOR, schema_url=None, paid=False, body=TEXT):
    schema = {"@type": "NewsArticle", "headline": "本篇標題", "datePublished": schema_clock,
              "author": [{"name": schema_author, "url": "https://cmnews.com.tw/forumnews"}],
              "articleBody": "隱藏或其他 schema 正文，不得使用" * 20}
    if schema_url:
        schema["url"] = schema_url
    if paid:
        schema["isAccessibleForFree"] = False
    payload = f'''<html><head><meta charset="utf-8"><link rel="canonical" href="{canonical}">
    <script type="application/ld+json">{json.dumps(schema, ensure_ascii=False)}</script></head><body>
    <div class="articleContent" id="{container_id}" data-article-path="/article/{slug}"
      data-author-id-article-id="{author_id}"><h1 class="articleContent__title">本篇標題</h1>
    <div class="articleContent__content--main"><div><div class="flex items-center">
    <figure><a href="/elsewhere">作者簡介不屬於 byline</a></figure><a href="{author_href}">{AUTHOR}</a></div>
    <div class="articleContent__time">{clock}</div></div>
    <div class="articleContent__textContent"><p>{body}</p></div></div></div>
    <aside><h1>推薦標題</h1><div class="articleContent__textContent">推薦正文{TEXT}</div>
    <div class="articleContent__time">2099-01-01 00:00</div><a rel="author">別篇作者</a></aside>
    <script id="__NUXT_DATA__">會員或推薦資料不可取代可見正文</script></body></html>'''
    return payload.encode()


def test_current_public_ssr_scope_and_matching_precise_source_clock(source):
    result = extract_article(page(), source, page_url=URL)
    assert result["body"] == TEXT
    assert result["title"] == "本篇標題"
    assert json.loads(result["authors_json"]) == [AUTHOR]
    assert result["published_at_raw"] == "2026-10-09T14:20:23+08:00"
    assert result["extraction_method"] == "css:.articleContent__textContent"
    assert result["historical_point_in_time"] is False


def explicit_empty_byline_page(**options):
    soup = BeautifulSoup(page(**options), 'lxml')
    for script in soup.find_all('script', type='application/ld+json'):
        script.decompose()
    byline = soup.select_one('.articleContent__content--main > div:first-child > .flex.items-center > a[href]')
    byline.clear()
    byline.append(soup.new_tag('p'))
    return soup


def test_actual_empty_visible_profile_name_preserves_body_and_unknown_attribution(source):
    soup = explicit_empty_byline_page()
    soup.select_one('aside a')['rel'] = 'author'
    result = extract_article(str(soup).encode(), source, page_url=URL)
    assert result['body'] == TEXT and result['title'] == '本篇標題'
    assert json.loads(result['authors_json']) == []
    assert result['attribution_status'] == 'source_visible_byline_name_empty'
    assert result['published_at_raw'] == '2026-10-09 14:20'
    assert result['published_time_basis'] == 'publisher_visible_minute_clock; no_seconds_or_timezone_inferred'
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['source_profile_url'] == 'https://cmnews.com.tw/forumnews'
    assert detail['visible_byline_raw'] == '' and detail['named_author_invented'] is False


@pytest.mark.parametrize('selector,attribute,value', [
    ('.articleContent', 'hidden', ''), ('.articleContent__textContent', 'aria-hidden', 'true'),
    ('.articleContent__time', 'style', 'display: none'),
    ('.articleContent__title', 'style', 'visibility: hidden'),
    ('.articleContent__content--main', 'class', ['hidden']),
    ('.flex.items-center > a', 'title', 'declared author elsewhere'),
    ('.flex.items-center > a > p', 'aria-label', 'declared author elsewhere'),
])
def test_empty_byline_requires_actually_visible_unambiguous_source_gap(source, selector, attribute, value):
    soup = explicit_empty_byline_page()
    soup.select_one(selector)[attribute] = value
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract_article(str(soup).encode(), source, page_url=URL)


@pytest.mark.parametrize('clock', ['', '2026-02-30 09:00', '2099-01-01', '2026-10-09 14:20:23'])
def test_empty_visible_name_cannot_borrow_missing_or_invalid_clock(source, clock):
    soup = explicit_empty_byline_page(clock=clock)
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract_article(str(soup).encode(), source, page_url=URL)


@pytest.mark.parametrize('kind', ['schema', 'missing_p', 'extra_node', 'different_profile'])
def test_empty_name_does_not_accept_other_ambiguous_attribution_shapes(source, kind):
    soup = explicit_empty_byline_page()
    byline = soup.select_one('.flex.items-center > a')
    if kind == 'schema':
        script = soup.new_tag('script', type='application/ld+json')
        script.string = json.dumps({'@type': 'NewsArticle', 'author': {'name': 'Unverified'}})
        soup.head.append(script)
    elif kind == 'missing_p':
        byline.clear()
    elif kind == 'extra_node':
        byline.append(soup.new_tag('span'))
    else:
        byline['href'] = '/someone-else'
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract_article(str(soup).encode(), source, page_url=URL)


def test_explicit_empty_name_is_not_permission_to_take_vip_preview(source):
    soup = explicit_empty_byline_page()
    soup.select_one('.articleContent__textContent').append(BeautifulSoup(VIP_MASK, 'lxml'))
    with pytest.raises(NewsError, match='paywall_or_login'):
        extract_article(str(soup).encode(), source, page_url=URL)


@pytest.mark.parametrize('empty', [True, False])
def test_source_published_and_updated_minute_labels_are_distinct(source, empty):
    clock = '2025-09-18 16:16 (更新：2025-09-18 16:33)'
    soup = explicit_empty_byline_page(clock=clock) if empty else BeautifulSoup(page(clock=clock), 'lxml')
    breadcrumb = soup.new_tag('script', type='application/ld+json')
    breadcrumb.string = json.dumps({'@type': 'BreadcrumbList', 'itemListElement': [{'@type': 'ListItem', 'name': 'Navigation, not author'}]})
    soup.head.append(breadcrumb)
    result = extract_article(str(soup).encode(), source, page_url=URL)
    assert result['published_at_raw'] == '2025-09-18 16:16'
    assert result['modified_at_raw'] == '2025-09-18 16:33'
    if empty:
        assert json.loads(result['authors_json']) == []
        assert json.loads(result['author_attribution_detail_json'])['visible_clock_raw'] == clock


def test_source_breadcrumb_name_is_not_the_empty_article_author(source):
    soup = explicit_empty_byline_page()
    script = soup.new_tag('script', type='application/ld+json')
    script.string = json.dumps({'@type': 'BreadcrumbList', 'itemListElement': [{'@type': 'ListItem', 'name': AUTHOR}]})
    soup.head.append(script)
    assert json.loads(extract_article(str(soup).encode(), source, page_url=URL)['authors_json']) == []


@pytest.mark.parametrize("overrides", [
    {"canonical": "https://elsewhere.test/article/" + SLUG},
    {"canonical": "https://cmnews.com.tw/"}, {"slug": "other-" + UUID},
    {"author_id": "other-" + UUID}, {"container_id": "ArticleContainer-wrong"},
])
def test_unverified_current_article_identity_cannot_take_visible_or_schema_body(source, overrides):
    with pytest.raises(NewsError, match="article_identity_not_verified"):
        extract_article(page(**overrides), source, page_url=URL)


@pytest.mark.parametrize("href", ["/another-author", "https://elsewhere.test/forumnews", ""])
def test_current_author_profile_must_match_slug(source, href):
    with pytest.raises(NewsError, match="article_attribution_not_verified"):
        extract_article(page(author_href=href), source, page_url=URL)


@pytest.mark.parametrize("overrides", [
    {"schema_author": "相關文章作者"}, {"schema_url": "https://cmnews.com.tw/article/elsewhere"},
    {"schema_clock": "2099-01-01T00:00:00+08:00"},
])
def test_unmatched_schema_does_not_override_visible_current_clock(source, overrides):
    result = extract_article(page(**overrides), source, page_url=URL)
    assert result["body"] == TEXT
    assert result["published_at_raw"] == "2026-10-09 14:20"
    assert json.loads(result["authors_json"]) == [AUTHOR]


def test_missing_current_clock_does_not_borrow_sidebar_or_unmatched_schema(source):
    result = extract_article(page(clock=""), source, page_url=URL)
    assert result["published_at_raw"] is None


def test_empty_visible_body_cannot_be_replaced_by_schema_or_hydration_text(source):
    with pytest.raises(NewsError, match="body_not_found"):
        extract_article(page(body=""), source, page_url=URL)


def test_exact_matching_paid_marker_is_not_bypassed_by_visible_ssr(source):
    with pytest.raises(NewsError, match="paywall_or_login"):
        extract_article(page(paid=True), source, page_url=URL)


VIP_MASK = ('<vip-content class="simple-box vipWrapper"><div class="simple-box-description vipContent">'
            '<div class="utilsMask"><p class="utilsMask__title">請登入以查看完整文章</p>'
            '<p class="utilsMask__desc utilsMask__desc--lg">閱讀VIP文章請先登入理財寶會員</p>'
            '</div></div></vip-content>')


@pytest.mark.parametrize("preview", ["", TEXT])
def test_current_visible_vip_mask_without_schema_flag_is_not_a_body_parser_failure(source, preview):
    with pytest.raises(NewsError, match="paywall_or_login"):
        extract_article(page(body=preview + VIP_MASK), source, page_url=URL)


def test_related_story_vip_mask_does_not_block_current_public_article(source):
    payload = page().replace(b"<aside>", ("<aside>" + VIP_MASK).encode())
    assert extract_article(payload, source, page_url=URL)["body"] == TEXT


def test_quoted_login_sentence_is_not_a_vip_gate(source):
    text = TEXT + "本篇在說明網站設計，例句：請登入以查看完整文章。"
    assert extract_article(page(body=text), source, page_url=URL)["body"] == text


def test_unmatched_article_identity_still_precedes_mask_classification(source):
    with pytest.raises(NewsError, match="article_identity_not_verified"):
        extract_article(page(author_id="other-" + UUID, body=VIP_MASK), source, page_url=URL)


def test_current_editor_table_figure_retains_raw_text_without_image_captions(source):
    markup = (f'<figure class="table"><table><tbody><tr><td><p>{TEXT}</p></td></tr></tbody></table></figure>'
              '<figure class="image"><figcaption>照片說明不是本篇正文</figcaption></figure>')
    result = extract_article(page(body=markup), source, page_url=URL)
    assert result["body"] == TEXT
    assert result["extraction_method"] == "css:.articleContent__textContent"


def test_current_diagrams_with_short_cta_are_not_missing_text_or_complete_article(source):
    markup = ('<figure class="image"><img src="a.png"></figure>'
              '<figure class="image"><img src="b.png"></figure><p>短宣傳不是完整圖表內容。</p>')
    with pytest.raises(NewsError, match='source_body_image_only'):
        extract_article(page(body=markup), source, page_url=URL)
    with pytest.raises(NewsError, match='paywall_or_login'):
        extract_article(page(body=markup + VIP_MASK), source, page_url=URL)
    result = extract_article(page(body=markup + TEXT), source, page_url=URL)
    assert TEXT in result['body']


@pytest.mark.parametrize("markup", [
    '<figure class="table"><figcaption>' + TEXT + '</figcaption></figure>',
    '<figure class="image"><table><tr><td>' + TEXT + '</td></tr></table></figure>',
])
def test_unreviewed_figure_shape_does_not_replace_missing_public_body(source, markup):
    with pytest.raises(NewsError, match="body_not_found"):
        extract_article(page(body=markup), source, page_url=URL)


def test_data_table_cannot_bypass_the_current_vip_mask(source):
    markup = f'<figure class="table"><table><tr><td>{TEXT}</td></tr></table></figure>' + VIP_MASK
    with pytest.raises(NewsError, match="paywall_or_login"):
        extract_article(page(body=markup), source, page_url=URL)


def test_relative_source_canonical_and_source_offset_are_preserved(source):
    result = extract_article(page(canonical="/article/" + SLUG), source, page_url=URL)
    assert result["body"] == TEXT
    assert result["declared_canonical_url"] == "/article/" + SLUG


def test_new_layout_cannot_be_enabled_for_another_publisher():
    config, _, repo = load_config(Path("configs/news_fulltext.json"))
    next(p for p in config["providers"] if p["id"] == "moneyweekly")["embedded_article_layout"] = "cmnews_visible_article_v1"
    with pytest.raises(ValueError, match="unreviewed embedded article parser"):
        PolicyRegistry(config, base=repo)

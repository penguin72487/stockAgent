"""Actual public-layout contracts, distinct from source permission or throughput."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_public_originals import ftv_declared_public_canonical, verify_resolution_response
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from scripts.audit_news_selected_articles import audit
from scripts.verify_news_recent_batches import verify_recent_batches
from test_news_fulltext import FakeResponse, http_fixture

CONFIG = Path("configs/news/probe_public_originals_20261009.json")
NOW_URL = "https://www.nownews.com/news/6881792"
AMP_URL = "https://www.nownews.com/amp/news/6881792"
FTV_URL = "https://www.ftvnews.com.tw/news/detail/2026317W0044"
TITLE = "市場公開原文與原始數值"
TEXT = "原始值 -12.5、12345 股、EPS 2.12、比率 3.25% 及 2020 年均應原樣保留。" * 5
CLOCK = "2026-10-08T17:23:43+08:00"


@pytest.fixture
def registry():
    return load_config(CONFIG)[1]


def schema(url, *, name="吳紹瑜", free=True, title=TITLE, clock=CLOCK, modified=None, body="隱藏的 schema 正文不可取代可見原文"):
    return '<script type="application/ld+json">' + json.dumps({
        "@type": "NewsArticle", "url": url, "mainEntityOfPage": url,
        "headline": title, "isAccessibleForFree": free,
        "author": {"@type": "Person", "name": name},
        "datePublished": clock, "dateModified": modified or clock, "articleBody": body,
    }, ensure_ascii=False) + '</script>'


def nownews_page(*, amp=False, canonical=NOW_URL, og=NOW_URL, schema_url=NOW_URL,
                 title=TITLE, clock=CLOCK, visible="2026-10-08 17:23:43",
                 credit="記者吳紹瑜／台北報導", schema_name=None, meta_credit=None,
                 profile="/reporter/吳紹瑜/", free=True, extra="", attrs="", extra_schema="",
                 plain_credit=False, updated=None, modified=None):
    name = schema_name if schema_name is not None else credit if amp or plain_credit else "吳紹瑜"
    meta = f'<meta itemprop="author" content="{meta_credit or credit}">'
    clocks = f'<meta property="article:published_time" content="{clock}"><meta itemprop="datePublished" content="{clock}">'
    if modified is not None:
        clocks += f'<meta property="article:modified_time" content="{modified}">'
    if amp:
        scope = f'''<div class="newDetailContainer"><div class="newsContainer"><h1 class="newsTitle">{title}</h1>
        <div class="newsBox"><div class="newsContainerLeft"><div class="newsInfo">{credit}-{visible}</div>
        <div class="newsMsg" {attrs}>{TEXT}<br><b>原文小標題</b><p>風險提醒不能刪除</p>
        <ul class="related"><li>相關新聞不可作正文</li></ul></div></div></div></div></div>'''
    else:
        # The visible profile is this article's credit, not the author biography.
        visible_credit = credit if plain_credit else credit.replace("吳紹瑜", f'<a href="{profile}">吳紹瑜</a>')
        if updated is not None:
            visible = f'<span aria-label="出版時間">{visible}</span><span aria-label="更新時間">{updated}</span>'
        scope = f'''<div class="mainBlk"><div class="wrapper"><div class="titleBlk"><div class="title"><h1>{title}</h1></div></div>
        <div class="infoBlk"><div class="info"><p>{visible_credit}</p><time class="time">{visible}</time></div></div>
        <article class="article-body" itemprop="articleBody"><aside>分享控制</aside><div class="article-content">
        <div id="articleContent" class="article-content-edtor" {attrs}>{TEXT}<br><b>原文小標題</b>
        <div class="ad-blk1">我是廣告 請繼續往下閱讀</div><p>風險提醒不能刪除</p></div>
        <div class="author-card">作者個人介紹不能作正文</div><div class="related-item">推薦新聞不能作正文</div>
        </div></article></div></div>'''
    og_meta = f'<meta property="og:url" content="{og}">' if og is not None else ""
    return f'''<html amp><head><meta charset="utf-8"><title>{title}</title><link rel="canonical" href="{canonical}">
    {og_meta}{meta}{clocks}{schema(schema_url, name=name, free=free, title=title, clock=clock, modified=modified)}{extra_schema}</head>
    <body>{scope}<footer><a rel="author">頁尾其他作者</a><time>2099-01-01</time></footer>{extra}</body></html>'''.encode()


def ftv_page(*, canonical=FTV_URL, og=FTV_URL, schema_url=FTV_URL,
             name="梁博超（即時中心） - 民視新聞網", credit="即時中心／綜合報導",
             clock=CLOCK, visible="2026/10/08 17:23", title=TITLE,
             free=True, attrs="", extra="", extra_schema=""):
    return f'''<html><head><meta charset="utf-8"><title>{title}</title><link rel="canonical" href="{canonical}">
    <meta property="og:url" content="{og}"><meta property="article:published_time" content="{clock}">
    {schema(schema_url, name=name, free=free, title=title, clock=clock)}{extra_schema}</head><body>
    <div class="col-article position-relative"><div class="scroll-header"><h1>{title}</h1></div>
    <ul class="news-status"><li>發佈時間：{visible} 更新時間：{visible}</li></ul>
    <div class="article-body"><article id="contentarea" itemprop="articleBody" {attrs}><figure>圖片說明不可作正文</figure>
    <div><p>{credit}</p><p>{TEXT}</p></div><strong>更多新聞：<a href="/news/detail/2026X01W001">其他文章</a></strong>
    <strong>原文數值小標題</strong><div><p class="aphorism">風險提醒不能刪除</p></div></article>
    <div class="exread-list">其他文章摘要</div></div></div>{extra}</body></html>'''.encode()


@pytest.mark.parametrize("amp", [False, True])
def test_nownews_uses_visible_editor_text_not_schema_biography_or_recommendations(registry, amp):
    url = AMP_URL if amp else NOW_URL
    result = extract_article(nownews_page(amp=amp, og=None if amp else NOW_URL), registry.sources["nownews"], page_url=url)
    assert TEXT in result["body"]
    assert "原文小標題" in result["body"] and "風險提醒不能刪除" in result["body"]
    for wrong in ("作者個人介紹", "推薦新聞", "相關新聞", "我是廣告", "隱藏的 schema", "頁尾其他作者"):
        assert wrong not in result["body"]
    assert result["body_completeness"] == "verified_current_article_container"
    assert result["published_at_raw"] == CLOCK
    assert json.loads(result["authors_json"]) == ["吳紹瑜"]
    detail = json.loads(result["author_attribution_detail_json"])
    assert detail["visible_byline_raw"] == ("記者吳紹瑜／台北報導" if amp else "記者 吳紹瑜 ／台北報導")
    assert detail["source_meta_author_raw"] == "記者吳紹瑜／台北報導"


@pytest.mark.parametrize("provider,url,page", [("nownews", NOW_URL, nownews_page), ("ftvnews", FTV_URL, ftv_page)])
@pytest.mark.parametrize("field,value", [
    ("canonical", "https://unreviewed.test/news/6881792"),
    ("canonical", "https://www.nownews.com/news/6881793"),
    ("schema_url", "https://www.nownews.com/news/6881793"),
    ("og", "https://unreviewed.test/news/6881792"),
])
def test_current_identity_never_borrows_another_story(registry, provider, url, page, field, value):
    with pytest.raises(NewsError, match="article_identity_not_verified"):
        extract_article(page(**{field: value}), registry.sources[provider], page_url=url)


@pytest.mark.parametrize("page_url", [
    "https://www.nownews.com/news/6881792?other=1", "https://www.nownews.com/news/688179%32",
    "https://www.nownews.com/amp%2fnews/6881792", "https://partner.nownews.com/news/6881792",
])
def test_nownews_alias_identity_does_not_strip_real_queries_or_escape_boundaries(registry, page_url):
    with pytest.raises(NewsError, match="article_identity_not_verified"):
        extract_article(nownews_page(), registry.sources["nownews"], page_url=page_url)


@pytest.mark.parametrize("amp", [False, True])
@pytest.mark.parametrize("extra", ['<div class="paywall">需付費</div>', '<form action="/login"><input type="password"></form>'])
def test_nownews_actual_paywall_and_login_are_not_exempted(registry, amp, extra):
    with pytest.raises(NewsError, match="paywall_or_login"):
        extract_article(nownews_page(amp=amp, extra=extra), registry.sources["nownews"], page_url=AMP_URL if amp else NOW_URL)


@pytest.mark.parametrize("provider,url,page", [("nownews", NOW_URL, nownews_page), ("ftvnews", FTV_URL, ftv_page)])
def test_matching_paid_schema_stays_binding(registry, provider, url, page):
    with pytest.raises(NewsError, match="paywall_or_login"):
        extract_article(page(free=False), registry.sources[provider], page_url=url)


@pytest.mark.parametrize("provider,url,page", [("nownews", NOW_URL, nownews_page), ("ftvnews", FTV_URL, ftv_page)])
@pytest.mark.parametrize("attrs", ['hidden', 'aria-hidden="true"', 'style="display: none"'])
def test_hidden_body_is_not_a_public_original(registry, provider, url, page, attrs):
    with pytest.raises(NewsError, match="article_identity_not_verified"):
        extract_article(page(attrs=attrs), registry.sources[provider], page_url=url)


@pytest.mark.parametrize("kwargs", [
    {"visible": "2026-10-08 17:23:42"}, {"clock": "2026-10-08T17:23:43"},
    {"clock": "not-a-clock"}, {"visible": "2099-01-01 00:00:00"},
])
def test_nownews_publication_is_source_time_not_metadata_or_footer(registry, kwargs):
    with pytest.raises(NewsError, match="invalid_bulk_publication_clock"):
        extract_article(nownews_page(**kwargs), registry.sources["nownews"], page_url=NOW_URL)


def test_nownews_legacy_visible_reporter_does_not_need_an_invented_profile(registry):
    result = extract_article(nownews_page(plain_credit=True), registry.sources['nownews'], page_url=NOW_URL)
    assert json.loads(result['authors_json']) == ['吳紹瑜']
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['schema_authors_raw'] == ['記者吳紹瑜／台北報導']
    assert detail['source_meta_author_raw'] == detail['visible_byline_raw'] == '記者吳紹瑜／台北報導'
    assert result['published_at_raw'] == CLOCK


@pytest.mark.parametrize('kwargs', [{'schema_name': '吳紹瑜'}, {'schema_name': '另一篇作者'},
                                   {'meta_credit': '記者另一人／台北報導'}])
def test_plain_reporter_still_requires_all_current_full_credits_to_match(registry, kwargs):
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract_article(nownews_page(plain_credit=True, **kwargs), registry.sources['nownews'], page_url=NOW_URL)


@pytest.mark.parametrize('plain_credit', [False, True])
def test_nownews_separately_labelled_revision_is_not_the_publication_clock(registry, plain_credit):
    modified = '2026-10-09T10:58:38+08:00'
    result = extract_article(nownews_page(plain_credit=plain_credit, updated='2026-10-09 10:58:38', modified=modified),
                             registry.sources['nownews'], page_url=NOW_URL)
    assert result['published_at_raw'] == CLOCK
    assert result['modified_at_raw'] == modified
    assert result['historical_point_in_time'] is False  # Captured revision is not the old publication text.


@pytest.mark.parametrize('updated,modified', [
    ('2026-10-09 10:58:39', '2026-10-09T10:58:38+08:00'),
    ('2026-10-09 10:58:38', None), ('2026-10-09 10:58:38', '2026-10-09T10:58:38'),
    ('2026-10-07 10:58:38', '2026-10-07T10:58:38+08:00'),
])
def test_nownews_conflicting_or_backdated_revision_is_not_silently_accepted(registry, updated, modified):
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        extract_article(nownews_page(updated=updated, modified=modified), registry.sources['nownews'], page_url=NOW_URL)


def test_duplicate_hidden_or_unlabelled_two_clocks_are_not_promoted_to_one_publication(registry):
    for visible in [
        '2026-10-08 17:23:43 2026-10-09 10:58:38',
        '<span aria-label="出版時間">2026-10-08 17:23:43</span>' * 2,
        '<span aria-label="出版時間" hidden>2026-10-08 17:23:43</span>',
    ]:
        with pytest.raises(NewsError, match='invalid_bulk_publication_clock|article_identity_not_verified'):
            extract_article(nownews_page(visible=visible), registry.sources['nownews'], page_url=NOW_URL)


@pytest.mark.parametrize('pinned', [False, True])
def test_clock_repair_is_pinned_once_missing_body_only_and_preserves_refusals(registry, tmp_path, pinned):
    from downloader.news_fulltext_public_originals import NOWNEWS_V1_CAPTURE_FINGERPRINT
    queue = NewsQueue(tmp_path, registry)
    try:
        queue.add_articles([{'url': NOW_URL}, {'url': 'https://www.nownews.com/news/6881793'},
                            {'url': 'https://www.nownews.com/news/6881794'},
                            {'url': 'https://www.nownews.com/news/6881795'}])
        queue.reconcile_policy()
        queue.db.execute("UPDATE articles SET state='invalid_bulk_publication_clock',error_code='invalid_bulk_publication_clock',attempts=1,due=1234")
        queue.db.execute("UPDATE articles SET body_sha256='retained-stored-head' WHERE url=?", ('https://www.nownews.com/news/6881793',))
        queue.db.execute("UPDATE articles SET state='source_access_denied',error_code='source_access_denied' WHERE url=?", ('https://www.nownews.com/news/6881794',))
        queue.db.execute("UPDATE articles SET state='paywall_or_login',error_code='paywall_or_login' WHERE url=?", ('https://www.nownews.com/news/6881795',))
        queue.db.execute("UPDATE queue_settings SET value=? WHERE key='source_capture_fingerprint:nownews'",
                         (NOWNEWS_V1_CAPTURE_FINGERPRINT if pinned else 'another-parser-version',))
        queue.db.execute("UPDATE queue_settings SET value='changed-policy-marker' WHERE key='policy_fingerprint'")
        queue.db.commit()
        queue.reconcile_policy()
        rows = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        assert rows[NOW_URL]['state'] == ('pending' if pinned else 'invalid_bulk_publication_clock')
        assert rows[NOW_URL]['error_code'] == 'invalid_bulk_publication_clock'
        assert rows['https://www.nownews.com/news/6881793']['body_sha256'] == 'retained-stored-head'
        assert rows['https://www.nownews.com/news/6881793']['state'] == 'invalid_bulk_publication_clock'
        for url, expected in [('https://www.nownews.com/news/6881794', 'source_access_denied'),
                              ('https://www.nownews.com/news/6881795', 'paywall_or_login')]:
            assert rows[url]['state'] == expected and rows[url]['attempts'] == 1 and rows[url]['due'] == 1234
        if pinned:
            proof = json.loads(queue.db.execute("SELECT value FROM queue_settings WHERE key='nownews_v1_published_updated_clock_repair'").fetchone()[0])
            assert proof['url_count'] == 1 and proof['source_refusals_or_stored_heads_reset'] is False
        queue.db.execute("UPDATE articles SET state='invalid_bulk_publication_clock',due=1234 WHERE url=?", (NOW_URL,))
        queue.db.commit()
        queue.reconcile_policy()
        assert queue.db.execute('SELECT state,due FROM articles WHERE url=?', (NOW_URL,)).fetchone()['due'] == 1234
    finally:
        queue.close()


@pytest.mark.parametrize("kwargs,code", [
    ({"schema_name": "另一篇作者"}, "article_attribution_not_verified"),
    ({"meta_credit": "記者另一篇作者／台北報導"}, "article_attribution_not_verified"),
    ({"profile": "https://unreviewed.test/reporter/吳紹瑜/"}, "article_attribution_not_verified"),
    ({"credit": "中央社／綜合報導"}, "article_rights_or_scope_excluded"),
    ({"credit": "周刊王CTWANT／李蕙璇"}, "article_rights_or_scope_excluded"),
])
def test_nownews_original_credit_is_not_a_third_party_or_unmatched_person(registry, kwargs, code):
    with pytest.raises(NewsError, match=code):
        extract_article(nownews_page(**kwargs), registry.sources["nownews"], page_url=NOW_URL)


def test_ftv_visible_body_keeps_values_and_disclaimer_not_schema_or_related_news(registry):
    result = extract_article(ftv_page(), registry.sources["ftvnews"], page_url=FTV_URL)
    assert TEXT in result["body"] and "原文數值小標題" in result["body"]
    assert "風險提醒不能刪除" in result["body"]
    for wrong in ("圖片說明", "更多新聞", "其他文章", "隱藏的 schema"):
        assert wrong not in result["body"]
    assert json.loads(result["authors_json"]) == ["梁博超（即時中心） - 民視新聞網"]
    assert result["published_at_raw"] == CLOCK
    assert result["body_completeness"] == "verified_current_article_container"


@pytest.mark.parametrize('credit', ['財經中心／綜合報導', '即時中心／<a>綜合</a>報導'])
def test_ftv_credit_and_long_body_in_one_paragraph_use_the_actual_line_break(registry, credit):
    page = ftv_page().replace('<p>即時中心／綜合報導</p><p>'.encode(),
                              ('<p>' + credit + '<br><br>').encode())
    result = extract_article(page, registry.sources['ftvnews'], page_url=FTV_URL)
    assert TEXT in result['body'] and '風險提醒不能刪除' in result['body']
    assert json.loads(result['author_attribution_detail_json'])['visible_newsroom_credit_raw'] == credit.replace('<a>', '').replace('</a>', '')


def test_ftv_line_boundary_never_hides_a_third_party_credit(registry):
    page = ftv_page().replace('<p>即時中心／綜合報導</p><p>'.encode(), '<p>圖、文／品觀點<br><br>'.encode())
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract_article(page, registry.sources['ftvnews'], page_url=FTV_URL)


@pytest.mark.parametrize('credit', ['一段普通正文<br>財經中心／綜合報導',
                                   '<br>財經中心／綜合報導',
                                   '<span hidden>財經中心／綜合報導</span><br><br>'])
def test_ftv_does_not_borrow_a_later_line_or_hidden_inline_credit(registry, credit):
    page = ftv_page(credit=credit)
    with pytest.raises(NewsError, match='article_attribution_not_verified|article_identity_not_verified'):
        extract_article(page, registry.sources['ftvnews'], page_url=FTV_URL)


@pytest.mark.parametrize("host", ["finance", "sport", "health"])
def test_ftv_reviewed_subdomain_same_article_id_retains_actual_request(registry, host):
    url = FTV_URL.replace("www.", host + ".")
    result = extract_article(ftv_page(), registry.sources["ftvnews"], page_url=url)
    assert result["declared_canonical_url"] == FTV_URL
    assert result["body_completeness"] == "verified_current_article_container"


@pytest.mark.parametrize("kwargs,code", [
    ({"credit": "CNEWS匯流新聞網記者／台北報導"}, "article_rights_or_scope_excluded"),
    ({"credit": "圖、文／CTWANT"}, "article_rights_or_scope_excluded"),
    ({"name": "第三方內容提供者"}, "article_rights_or_scope_excluded"),
    ({"visible": "2026/10/08 17:24"}, "invalid_bulk_publication_clock"),
])
def test_ftv_does_not_promote_syndication_or_wrong_clock_to_original(registry, kwargs, code):
    with pytest.raises(NewsError, match=code):
        extract_article(ftv_page(**kwargs), registry.sources["ftvnews"], page_url=FTV_URL)


@pytest.mark.parametrize("credit", ["論壇中心／綜合報導", "（民視新聞／徐紹芸、鍾淑惠 桃園報導）"])
def test_ftv_old_tv_end_credit_and_leading_caption_are_not_source_exclusions(registry, credit):
    page = ftv_page(credit=credit).decode().replace(f"<p>{credit}</p>", "").replace(
        '<p class="aphorism">', f"<p>{credit}</p><p class=\"aphorism\">")
    result = extract_article(page.encode(), registry.sources["ftvnews"], page_url=FTV_URL)
    assert credit in result["body"]
    assert json.loads(result["author_attribution_detail_json"])["visible_newsroom_credit_raw"] == credit


@pytest.mark.parametrize('credit,name', [
    ('（民視新聞網／綜合報導）', 'qaza6565 - 民視新聞網'),
    ('（民視新聞網報導）', '陳思瑀 - 民視新聞網'),
    ('南部中心／綜合報導', '曾虹雯（記者－南部） - 民視新聞網'),
])
def test_ftv_actual_legacy_newsroom_and_southern_credit_keep_editor_roles(registry, credit, name):
    page = ftv_page(credit=credit, name=name)
    if credit.startswith('南部中心'):
        page = page.replace(f'<p>{credit}</p><p>'.encode(),
                            f'<p>{credit}<br><br>'.encode())
    result = extract_article(page, registry.sources['ftvnews'], page_url=FTV_URL)
    assert TEXT in result['body']
    assert json.loads(result['authors_json']) == [name]
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['visible_newsroom_credit_raw'] == credit
    assert detail['schema_authors_raw'] == [name]
    assert detail['byline_basis'] == 'declared_current_article_author_and_visible_own_newsroom'


@pytest.mark.parametrize('credit', ['（民視新聞網協助轉載）', '民視新聞網提醒投資風險',
                                   '撰文‧陳唯泰', '社群編輯姓名', '（其他新聞網報導）'])
def test_ftv_newsroom_signature_is_not_inferred_from_brand_or_schema_editor(registry, credit):
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract_article(ftv_page(credit=credit), registry.sources['ftvnews'], page_url=FTV_URL)


@pytest.mark.parametrize('credit', ['（民視新聞網報導）', '南部中心／綜合報導'])
def test_ftv_new_signatures_still_require_visible_current_original_not_third_party(registry, credit):
    hidden = ftv_page(credit=f'<span hidden>{credit}</span>')
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(hidden, registry.sources['ftvnews'], page_url=FTV_URL)
    external = ftv_page(credit='圖、文／品觀點').replace(
        f'<p>{TEXT}</p>'.encode(), f'<p>{credit}</p><p>{TEXT}</p>'.encode())
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract_article(external, registry.sources['ftvnews'], page_url=FTV_URL)


def ftv_subsite_page(url, *, canonical=FTV_URL, credit="論壇中心／綜合報導", clock="2026-10-08T17:23:43+00:00", extra="", free=True):
    return f'''<html><head><title>{TITLE}</title><link rel="canonical" href="{canonical}">
    <meta property="og:url" content="{url}"><meta property="article:published_time" content="{clock}">
    {schema(canonical, name="民視財經網", free=free, clock=clock)}</head><body><div id="__nuxt">
    <article><h1>{TITLE}</h1><figure><p>當前圖片說明不是記者署名</p></figure>
    <p>{credit}</p><p>{TEXT}</p></article></div>{extra}</body></html>'''.encode()


def test_ftv_subsite_clock_is_preserved_and_never_blindly_corrected(registry):
    url = FTV_URL.replace("www.", "finance.")
    proof = ftv_declared_public_canonical(ftv_subsite_page(url), registry.sources["ftvnews"], page_url=url)
    assert proof["declared_canonical_url"] == FTV_URL
    assert proof["subsite_published_at_raw"] == "2026-10-08T17:23:43+00:00"
    assert proof["clock_not_accepted_or_rewritten"] is True


@pytest.mark.parametrize("change,code", [
    ({"canonical": FTV_URL.replace("0044", "0045")}, "article_identity_not_verified"),
    ({"canonical": "https://unreviewed.test/news/detail/2026317W0044"}, "article_identity_not_verified"),
    ({"credit": "圖、文／品觀點"}, "article_rights_or_scope_excluded"),
    ({"free": False}, "paywall_or_login"),
    ({"extra": '<form action="/login"><input type="password"></form>'}, "paywall_or_login"),
])
def test_ftv_canonical_resolution_is_not_an_access_or_syndication_workaround(registry, change, code):
    url = FTV_URL.replace("www.", "finance.")
    with pytest.raises(NewsError, match=code):
        ftv_declared_public_canonical(ftv_subsite_page(url, **change), registry.sources["ftvnews"], page_url=url)


def ftv_http(tmp_path, *, subsite=None, original=None):
    config = json.loads(CONFIG.read_text())
    config["providers"] = [p for p in config["providers"] if p["id"] == "ftvnews"]
    config["providers"][0]["interval_seconds"] = 0.001  # Synthetic transport, no live rate claim.
    config["exclusive_http_owner"] = False
    url = FTV_URL.replace("www.", "finance.")
    http, session = http_fixture(config, tmp_path, {
        "https://finance.ftvnews.com.tw/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
        "https://www.ftvnews.com.tw/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
        url: subsite or FakeResponse(ftv_subsite_page(url)),
        FTV_URL: original or FakeResponse(ftv_page(credit="論壇中心／綜合報導")),
    })
    return http, session, {"url": url, "id": bytes.fromhex("11" * 32)}


def test_ftv_full_fetch_preserves_frontier_and_both_proofs_not_wrong_offset(tmp_path):
    http, session, row = ftv_http(tmp_path)
    record = fetch_article(http, row)
    assert record["requested_url"] == row["url"] and record["final_url"] == FTV_URL
    assert record["published_at_raw"] == CLOCK
    evidence = json.loads(record["url_resolution_evidence_json"])
    assert evidence["declared_clock_conflict"] is True
    assert evidence["subsite_published_at_raw"].endswith("+00:00")
    assert verify_resolution_response(record) == len(record["url_resolution_response_body"])
    assert [url for url, _ in session.calls] == [
        "https://finance.ftvnews.com.tw/robots.txt", row["url"], "https://www.ftvnews.com.tw/robots.txt", FTV_URL]
    http.close()


@pytest.mark.parametrize("response,code", [
    (FakeResponse(status=403), "access_denied"), (FakeResponse(status=429), "http_429"),
    (FakeResponse(b"<title>Just a moment</title>"), "access_challenge"),
    (FakeResponse(ftv_subsite_page(FTV_URL.replace("www.", "finance."), credit="圖、文／品觀點")), "article_rights_or_scope_excluded"),
])
def test_ftv_denial_or_third_party_never_triggers_canonical_request(tmp_path, response, code):
    http, session, row = ftv_http(tmp_path, subsite=response)
    with pytest.raises(NewsError, match=code):
        fetch_article(http, row)
    assert FTV_URL not in [url for url, _ in session.calls]
    http.close()


@pytest.mark.parametrize("key,value", [
    ("url_resolution_response_body", b"corrupt"),
    ("http_total_response_bytes", 0),
    ("url_resolution_evidence_json", "[]"),
    ("published_at_raw", "2099-01-01"),
])
def test_ftv_resolution_receipt_tampering_is_detected(tmp_path, key, value):
    http, _, row = ftv_http(tmp_path)
    record = fetch_article(http, row)
    record[key] = value
    with pytest.raises(ValueError, match="canonical_resolution"):
        verify_resolution_response(record)
    http.close()


def test_ftv_mixed_batch_retains_late_resolution_fields_and_full_byte_receipts(tmp_path):
    http, _, row = ftv_http(tmp_path)
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{"url": FTV_URL}, {"url": row["url"]}])
        records = [fetch_article(http, dict(queue.db.execute("SELECT * FROM articles WHERE url=?", (url,)).fetchone()))
                   for url in (FTV_URL, row["url"])]
        queue.accept(records)
        full = verify_bodies(tmp_path)
        expected = sum(len(r["raw_response_body"]) + len(r.get("url_resolution_response_body") or b"") for r in records)
        assert full["accepted"] and full["raw_response_bytes"] == expected
        recent = verify_recent_batches(tmp_path, 1)
        assert recent["accepted"] and recent["private_raw_bytes"] == expected
        selected = audit(tmp_path, [row["url"]], registry=http.registry)
        assert selected["accepted"]
        assert selected["articles"][0]["canonical_resolution_source_replay_verified"]
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize("provider", ["nownews", "ftvnews"])
@pytest.mark.parametrize("change", ["authorization", "domain", "finite", "attribution"])
def test_source_adapter_cannot_be_enabled_under_different_permission_contract(tmp_path, provider, change):
    config = json.loads(CONFIG.read_text())
    entry = deepcopy(next(p for p in config["providers"] if p["id"] == provider))
    if change == "authorization":
        entry["authorization"]["evidence_url"] = "https://unreviewed.test/terms"
    elif change == "domain":
        entry["domains"] = ["unreviewed.test"]
    elif change == "finite":
        entry["acquisition_mode"] = "continuous"
    else:
        entry["require_author_attribution"] = False
    config["providers"] = [entry]
    with pytest.raises(ValueError, match="unreviewed embedded"):
        PolicyRegistry(config, base=tmp_path)

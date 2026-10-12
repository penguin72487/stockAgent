"""Verified source query repair and current DOM identity, no hidden-body use."""
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article, normalize_url
from downloader.news_fulltext_taiwan_pages import tainan_news_request_url

CONFIG = Path("configs/news/probe_government_sources_20261009.json")
TAINAN_URL = "https://www.tainan.gov.tw/News_Content.aspx?n=13370&s=7868304"
TAINAN_ESCAPED_URL = "https://www.tainan.gov.tw/News_Content.aspx?n=13370&amp;s=7868304"
KMDN_URL = "https://www.kmdn.gov.tw/1117/1271/1272/306415/"
TEXT = "本篇可見新聞的原始數值 -12.5 與 12345 股，資料與來源身份不可改變。" * 6


@pytest.fixture
def registry():
    return load_config(CONFIG)[1]


def tainan_page(*, identity=TAINAN_URL + "&Create=1", title="官方本篇新聞", unit="新聞及國際關係處",
                clock="111年1月20日11:50(12:24更新)", body=TEXT, extra=""):
    return f'''<html><head><meta charset="utf-8"><meta property="og:url" content="{identity}">
    <meta property="og:title" content="{title}"></head><body><aside class="area-editor">導覽與頁尾{TEXT}</aside>
    <div id="CCMS_Content"><div class="simple-text title"><h3>{title}</h3></div>
    <div class="area-essay page-caption-p"><div class="essay"><div class="p">
    <p><span><p>發稿單位：{unit}</p><p>發稿時間：{clock}</p><p>發稿人：本篇發稿人</p>
    <p>{body}</p></span></p></div></div></div></div>{extra}
    <script type="application/ld+json">{{"@type":"NewsArticle","articleBody":"隱藏正文不能使用{TEXT}"}}</script>
    <footer><time>2099-12-31 23:59</time><a rel="author">頁尾作者</a></footer></body></html>'''.encode()


def kmdn_page(*, facebook=KMDN_URL, twitter=KMDN_URL, byline="本篇記者/綜合報導。", label="記者：",
              clock="2019/05/26", body=TEXT, extra="", og_title="-官方本篇新聞",
              og_url="https://www.kmdn.gov.tw/1117/1271/1272/262893/"):
    return f'''<html><head><meta charset="utf-8"><meta property="og:title" content="{og_title}">
    <meta property="og:url" content="{og_url}"></head><body><div id="center"><div class="container">
    <h2 class="title"><span>官方</span>本篇新聞</h2><div class="function_panel">
    <div class="publish_time"><span>發布日期：</span><time>{clock}</time></div>
    <div class="click_rate"><span>{label}</span><span class="times">{byline}</span></div>
    <div class="click_rate"><span>點閱率：</span><span class="times">12345</span></div>
    <div class="share"><a href="http://www.facebook.com/share.php?u={facebook}">分享</a>
    <a href="https://twitter.com/share?text=官方本篇新聞&amp;url={twitter}">分享</a></div></div>
    <section class="cp"><div class="pic pic_block"><img src="/a.jpg"><p>照片不是文章正文</p></div><p>{body}</p></section>
    <div class="simple_news_block"><h2 class="title">相關新聞</h2><p>{TEXT}</p><time>2099/01/01</time></div>
    </div></div>{extra}<script type="application/ld+json">{{"@type":"NewsArticle","articleBody":"隱藏文章{TEXT}"}}</script>
    <footer><a rel="author">頁尾作者</a></footer></body></html>'''.encode()


def test_verified_separator_repair_keeps_global_url_normalization_unchanged():
    assert tainan_news_request_url(TAINAN_ESCAPED_URL) == TAINAN_URL
    assert normalize_url(TAINAN_ESCAPED_URL) == TAINAN_ESCAPED_URL
    assert tainan_news_request_url(TAINAN_URL) == TAINAN_URL


@pytest.mark.parametrize("url", [
    "https://elsewhere.test/News_Content.aspx?n=13370&amp;s=7868304",
    "https://origin-www.tainan.gov.tw/News_Content.aspx?n=13370&amp;s=7868304",
    "http://www.tainan.gov.tw/News_Content.aspx?n=13370&amp;s=7868304",
    "https://www.tainan.gov.tw/other.aspx?n=13370&amp;s=7868304",
    "https://www.tainan.gov.tw/News_Content.aspx?n=99&amp;s=7868304",
    "https://www.tainan.gov.tw/News_Content.aspx?n=13370&amp;s=7868304&s=123",
    "https://www.tainan.gov.tw/News_Content.aspx?n=13370&amp;s=7868304&signature=exact%26amp%3B",
    "https://www.tainan.gov.tw/News_Content.aspx?amp;s=7868304&n=13370",
    "https://www.tainan.gov.tw/News_Content.aspx?n=13370&amp;s=0",
])
def test_unreviewed_ambiguous_or_signed_url_is_not_rewritten(url):
    assert tainan_news_request_url(url) == url


def test_tainan_visible_identity_original_clock_credits_and_body_only(registry):
    article = extract_article(tainan_page(), registry.sources["tainan_gov"], page_url=TAINAN_URL)
    assert TEXT in article["body"]
    assert "導覽與頁尾" not in article["body"] and "隱藏正文" not in article["body"]
    assert json.loads(article["authors_json"]) == ["新聞及國際關係處", "本篇發稿人"]
    assert article["published_at_raw"] == "111年1月20日11:50(12:24更新)"
    assert "no_timezone" in article["published_time_basis"]
    assert article["modified_at_raw"] is None
    assert article["body_completeness"] == "verified_current_article_container"
    assert article["historical_point_in_time"] is False


@pytest.mark.parametrize("path", ["news_content.aspx", "News_Content.aspx", "NEWS_CONTENT.ASPX"])
def test_tainan_actual_namespace_case_preserves_request_and_declared_identity(registry, path):
    url = TAINAN_URL.replace("News_Content.aspx", path)
    article = extract_article(tainan_page(), registry.sources["tainan_gov"], page_url=url)
    assert article["body"] and TEXT in article["body"]
    assert article["published_at_raw"] == "111年1月20日11:50(12:24更新)"
    assert normalize_url(url) == url  # No global URL/path rewrite.
    calls = []
    def get(request_url, **kwargs):
        calls.append(request_url)
        return SimpleNamespace(status=200, url=request_url, headers={"content-type": "text/html"}, body=tainan_page())
    record = fetch_article(SimpleNamespace(registry=registry, get=get), {"url": url, "id": b"c" * 32})
    assert calls == [url]
    assert record["requested_url"] == record["source_request_url"] == url
    escaped = url.replace("&s=", "&amp;s=")
    assert tainan_news_request_url(escaped) == url


@pytest.mark.parametrize("path", ["newſ_content.aspx", "news_content.aſpx", "%6Eews_content.aspx", "other.aspx"])
def test_tainan_namespace_case_proof_does_not_accept_unobserved_unicode_or_encoded_aliases(registry, path):
    url = TAINAN_URL.replace("News_Content.aspx", path)
    assert tainan_news_request_url(url.replace("&s=", "&amp;s=")) == url.replace("&s=", "&amp;s=")
    with pytest.raises(NewsError, match="article_identity_not_verified"):
        extract_article(tainan_page(), registry.sources["tainan_gov"], page_url=url)


@pytest.mark.parametrize("clock", ["110年10月3日 12：40", "111年02月23日 09:20", "111年9月16日15:55"])
def test_tainan_observed_clock_layout_keeps_exact_source_text(registry, clock):
    record = extract_article(tainan_page(clock=clock), registry.sources["tainan_gov"], page_url=TAINAN_URL)
    assert record["published_at_raw"] == clock
    assert record["modified_at_raw"] is None
    assert "no_timezone_or_missing_seconds_inferred" in record["published_time_basis"]
    assert TEXT in record["body"]


def test_tainan_same_paragraph_labels_bind_the_actual_reviewed_agency_and_clock(registry):
    page = tainan_page().replace(
        "<p>發稿單位：新聞及國際關係處</p><p>發稿時間：111年1月20日11:50(12:24更新)</p>".encode(),
        "<p>發稿單位：社會局 發稿時間：111年9月16日15:55</p>".encode())
    record = extract_article(page, registry.sources["tainan_gov"], page_url=TAINAN_URL)
    assert record["published_at_raw"] == "111年9月16日15:55"
    assert json.loads(record["authors_json"]) == ["社會局", "本篇發稿人"]
    assert "發稿單位：社會局 發稿時間：111年9月16日15:55" in record["body"]
    assert TEXT in record["body"]


@pytest.mark.parametrize("attrs", ["hidden", 'aria-hidden="true"', 'style="display: none"', 'style="visibility:hidden"'])
@pytest.mark.parametrize("element", ["header", "body", "title"])
def test_tainan_hidden_source_identity_credit_or_clock_cannot_become_evidence(registry, attrs, element):
    page = tainan_page()
    if element == "header":
        page = page.replace("<p>發稿時間：".encode(), ("<p " + attrs + ">發稿時間：").encode())
    elif element == "body":
        page = page.replace(b'<div class="p">', ('<div class="p" ' + attrs + '>').encode())
    else:
        page = page.replace(b"<h3>", ("<h3 " + attrs + ">").encode())
    with pytest.raises(NewsError, match="article_identity_not_verified"):
        extract_article(page, registry.sources["tainan_gov"], page_url=TAINAN_URL)


def test_tainan_caption_fields_do_not_create_a_false_agency_or_publication(registry):
    page = tainan_page().replace(b'<div class="p">',
        '<div class="p"><figure><figcaption><p>發稿單位：第三方出版者 發稿時間：199年1月1日00:00</p></figcaption></figure>'.encode())
    record = extract_article(page, registry.sources["tainan_gov"], page_url=TAINAN_URL)
    assert record["published_at_raw"] == "111年1月20日11:50(12:24更新)"
    assert json.loads(record["authors_json"]) == ["新聞及國際關係處", "本篇發稿人"]
    assert TEXT in record["body"]


@pytest.mark.parametrize("unit", ["第三方出版者", "社會局 發稿單位：第三方出版者", "新聞及國際關係處附屬外部稿件"])
def test_tainan_new_inline_layout_never_grants_an_unknown_or_conflicting_unit(registry, unit):
    page = tainan_page().replace(
        "<p>發稿單位：新聞及國際關係處</p><p>發稿時間：111年1月20日11:50(12:24更新)</p>".encode(),
        ("<p>發稿單位：" + unit + " 發稿時間：111年9月16日15:55</p>").encode())
    with pytest.raises(NewsError, match="article_rights_or_scope_excluded"):
        extract_article(page, registry.sources["tainan_gov"], page_url=TAINAN_URL)


@pytest.mark.parametrize("clock", ["111年02月30日 09：20", "111年02月23日 25：20", "111年02月23日 09：60"])
def test_tainan_spacing_and_fullwidth_colon_do_not_relax_calendar_validity(registry, clock):
    with pytest.raises(NewsError, match="invalid_bulk_publication_clock"):
        extract_article(tainan_page(clock=clock), registry.sources["tainan_gov"], page_url=TAINAN_URL)


@pytest.mark.parametrize("identity", [
    TAINAN_URL.replace("7868304", "123"), TAINAN_URL + "&Create=2",
    TAINAN_URL + "&s=7868304", TAINAN_URL.replace("www.", "origin-www."),
])
def test_tainan_wrong_or_ambiguous_page_identity_is_rejected(registry, identity):
    with pytest.raises(NewsError, match="article_identity_not_verified"):
        extract_article(tainan_page(identity=identity), registry.sources["tainan_gov"], page_url=TAINAN_URL)


def test_tainan_other_unit_does_not_inherit_reviewed_release_scope(registry):
    with pytest.raises(NewsError, match="article_rights_or_scope_excluded"):
        extract_article(tainan_page(unit="第三方出版者"), registry.sources["tainan_gov"], page_url=TAINAN_URL)


def test_tainan_release_headers_do_not_replace_an_empty_body(registry):
    with pytest.raises(NewsError, match="body_not_found"):
        extract_article(tainan_page(body=""), registry.sources["tainan_gov"], page_url=TAINAN_URL)


@pytest.mark.parametrize("clock", ["", "111年13月20日11:50", "111年2月30日11:50", "111年1月20日11:50(25:00更新)"])
def test_tainan_missing_or_invalid_clock_never_borrows_footer_time(registry, clock):
    with pytest.raises(NewsError, match="(?:invalid_bulk_publication_clock|article_attribution_not_verified)"):
        extract_article(tainan_page(clock=clock), registry.sources["tainan_gov"], page_url=TAINAN_URL)


def test_tainan_only_official_request_alias_and_original_frontier_provenance(registry):
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        return SimpleNamespace(status=200, url=url, headers={"content-type": "text/html"}, body=tainan_page())
    http = SimpleNamespace(registry=registry, get=get)
    article = fetch_article(http, {"url": TAINAN_ESCAPED_URL, "id": b"a" * 32, "metadata_date_raw": "2022-01-20 04:26:26"})
    assert calls == [TAINAN_URL]
    assert article["requested_url"] == TAINAN_ESCAPED_URL
    assert article["source_request_url"] == article["final_url"] == TAINAN_URL
    assert article["url_resolution_basis"].startswith("verified_tainan_html_query_separator_v1")
    assert article["metadata_date_raw"] != article["published_at_raw"]
    assert article["publish"] is False and article["remote_training_export"] is False


def test_alias_does_not_retry_source_refusal_under_another_url(registry):
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        raise NewsError("http_403", retry=True, delay=900)
    with pytest.raises(NewsError, match="http_403"):
        fetch_article(SimpleNamespace(registry=registry, get=get), {"url": TAINAN_ESCAPED_URL, "id": b"b" * 32})
    assert calls == [TAINAN_URL]


@pytest.mark.parametrize("url", [KMDN_URL, KMDN_URL.rstrip("/")])
def test_kmdn_current_shares_override_stale_og_identity_without_fetching_social_sites(registry, url):
    article = extract_article(kmdn_page(), registry.sources["kmdn"], page_url=url)
    assert article["title"] == "官方本篇新聞" and article["body"] == TEXT
    assert json.loads(article["authors_json"]) == ["本篇記者/綜合報導。"]
    assert article["published_at_raw"] == "2019/05/26"
    assert "date_only" in article["published_time_basis"]
    assert article["declared_canonical_url"] is None
    assert "source_og_url_untrusted" in article["article_identity_basis"]
    assert article["body_completeness"] == "verified_current_article_container"


@pytest.mark.parametrize("overrides", [
    {"facebook": KMDN_URL.replace("306415", "123")},
    {"twitter": "https://elsewhere.test/1117/1271/1272/306415/"},
    {"facebook": KMDN_URL + "?signed=1"}, {"twitter": KMDN_URL.replace("https:", "http:")},
    {"og_title": "另一篇新聞"},
])
def test_kmdn_unmatched_current_identity_not_replaced_by_stale_og_or_related_text(registry, overrides):
    with pytest.raises(NewsError, match="article_identity_not_verified"):
        extract_article(kmdn_page(**overrides), registry.sources["kmdn"], page_url=KMDN_URL)


def test_kmdn_current_author_label_preserved_without_view_counter(registry):
    article = extract_article(kmdn_page(label="作者："), registry.sources["kmdn"], page_url=KMDN_URL)
    assert json.loads(article["authors_json"]) == ["本篇記者/綜合報導。"]


@pytest.mark.parametrize("overrides", [{"byline": ""}, {"label": "點閱率："}])
def test_kmdn_missing_byline_never_borrows_footer_author(registry, overrides):
    with pytest.raises(NewsError, match="article_attribution_not_verified"):
        extract_article(kmdn_page(**overrides), registry.sources["kmdn"], page_url=KMDN_URL)


@pytest.mark.parametrize("clock", ["", "2021/02/30", "2099-01-01 00:00"])
def test_kmdn_missing_or_invalid_declared_date_never_borrows_other_clocks(registry, clock):
    with pytest.raises(NewsError, match="invalid_bulk_publication_clock"):
        extract_article(kmdn_page(clock=clock), registry.sources["kmdn"], page_url=KMDN_URL)


def test_kmdn_empty_body_never_uses_summary_schema_or_related_story(registry):
    with pytest.raises(NewsError, match="body_not_found"):
        extract_article(kmdn_page(body=""), registry.sources["kmdn"], page_url=KMDN_URL)


@pytest.mark.parametrize("provider,layout", [("tainan_gov", "tainan_visible_news_v1"), ("kmdn", "kmdn_visible_article_v1")])
def test_source_layout_cannot_enable_another_domain_or_continuous_crawl(provider, layout):
    original, _, repo = load_config(CONFIG)
    for changed in ("domain", "mode"):
        config = copy.deepcopy(original)
        entry = next(p for p in config["providers"] if p["id"] == provider)
        entry["embedded_article_layout"] = layout
        if changed == "domain":
            entry["domains"] = ["other.test"]
        else:
            entry["acquisition_mode"] = "continuous"
        with pytest.raises(ValueError, match="unreviewed embedded article parser"):
            PolicyRegistry(config, base=repo)

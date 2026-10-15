"""Reviewed government-origin text; no hidden bodies or inferred timezone."""
import copy
import json
from pathlib import Path

import pytest

from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article


CONFIG = Path("configs/news/probe_macau_government_20261011.json")
URL = "https://www.gov.mo/zh-hans/news/607199/"
AGENCY = "劳工事务局（DSAL）"
AGENCY_URL = "https://www.gov.mo/zh-hans/entity-page/entity-301/"
TITLE = "本篇政府勞動市場數據公告"
TEXT = "原始僱員人數 2,515，金額 -12.5，比例 0.75；這是本篇完整可見正文。" * 5


@pytest.fixture
def registry():
    return load_config(CONFIG)[1]


def page(*, canonical=URL, og_url=URL, headline=TITLE, title=TITLE, agency=AGENCY, agency_url=AGENCY_URL,
         clock="2022-04-29 10:17:00", visible_clock="2022年4月29日 10:17", body=TEXT,
         schema_changes=None, panel_attributes="", heading_attributes="", body_attributes="", extra="",
         source_label="来源：", date_label="发布日期："):
    schema = {"@type": "NewsArticle", "mainEntityOfPage": {"@type": "WebPage", "@id": URL},
              "headline": TITLE, "author": {"@type": "Organization", "name": AGENCY},
              "datePublished": "2022-04-29T10:17:00+00:00", "dateModified": "2026-10-11T12:59:00+00:00",
              "articleBody": "隱藏 schema 全文不得作為正文" * 20}
    schema.update(schema_changes or {})
    return f'''<html><head><meta charset="utf-8"><link rel="canonical" href="{canonical}">
    <meta property="og:url" content="{og_url}"><meta property="og:title" content="{title}"></head><body>
    <main id="content"><div class="mce-content-body">
    <div class="news-title"><h1 {heading_attributes}>{headline}</h1></div><div>
    <aside class="metadata-list" {panel_attributes}><dl><dt>{source_label}</dt><dd><a href="{agency_url}">{agency}</a></dd>
    <dt>{date_label}</dt><dd><time class="date" datetime="{clock}">{visible_clock}</time></dd></dl></aside></div>
    <article {body_attributes}><a class="caption" href="javascript:viewGallery('gallery-1');">原始可見圖片說明</a>
    <p>{body}</p><table><tr><th>原始統計</th><td>-25</td><td>12345</td></tr></table>
    <img class="gcs-counter" src="https://www.gcs.gov.mo/count/read/abc">
    <div class="gallery" id="gallery-1" hidden><span>媒體圖庫不是正文</span><img src="/photo.jpg"></div>
    <a class="btn btn-primary" href="javascript:viewGallery('gallery-1');">查看图库</a></article>
    <script type="application/ld+json">{json.dumps(schema, ensure_ascii=False)}</script>
    <nav><time datetime="2026-10-11 12:59:00">旁邊的最新日期</time><a rel="author">旁邊的作者</a></nav>
    </div><aside><article><p>另一篇新聞{TEXT}</p></article><time>2026年10月11日 12:59</time></aside></main>{extra}
    </body></html>'''.encode()


def test_current_native_body_department_clock_and_caption(registry):
    result = extract_article(page(), registry.require(URL), page_url=URL)
    assert result["title"] == TITLE
    assert json.loads(result["authors_json"]) == [AGENCY]
    assert result["published_at_raw"] == "2022-04-29 10:17:00"
    assert "no_timezone_inferred" in result["published_time_basis"]
    assert "reviewed_department_v1" in result["article_identity_basis"]
    assert "原始可見圖片說明" in result["body"]
    for value in ("2,515", "-12.5", "0.75", "-25", "12345"):
        assert value in result["body"]
    for value in ("隱藏 schema", "另一篇新聞", "媒體圖庫", "查看图库", "旁邊的作者", "2026年10月11日"):
        assert value not in result["body"]


@pytest.mark.parametrize("changes", [
    {"canonical": "https://www.gov.mo/zh-hans/news/607200/"},
    {"og_url": "https://www.gov.mo/zh-hans/news/607200/"},
    {"headline": "另一篇標題"}, {"title": "另一篇標題"},
    {"schema_changes": {"headline": "另一篇標題"}},
    {"schema_changes": {"mainEntityOfPage": {"@id": "https://www.gov.mo/zh-hans/news/607200/"}}},
    {"schema_changes": {"author": {"@type": "Person", "name": AGENCY}}},
    {"schema_changes": {"author": {"@type": "Organization", "name": "其他機構"}}},
    {"heading_attributes": "hidden"}, {"panel_attributes": "aria-hidden='true'"},
    {"body_attributes": "style='display: none'"},
    {"headline": "<span hidden>" + TITLE + "</span>"},
    {"agency": "<span hidden>" + AGENCY + "</span>"},
    {"body": "<span hidden>" + TEXT + "</span>"},
])
def test_wrong_or_hidden_article_not_accepted(registry, changes):
    with pytest.raises(NewsError, match="article_identity_not_verified"):
        extract_article(page(**changes), registry.require(URL), page_url=URL)


@pytest.mark.parametrize("changes", [
    {"agency": "其他部門"}, {"agency_url": "https://www.gov.mo/zh-hans/entity-page/entity-51/"},
    {"agency_url": "https://www.gov.mo.evil.test/zh-hans/entity-page/entity-301/"},
])
def test_host_and_publisher_schema_not_blanket_third_party_permission(registry, changes):
    with pytest.raises(NewsError, match="article_rights_or_scope_excluded"):
        extract_article(page(**changes), registry.require(URL), page_url=URL)


@pytest.mark.parametrize("changes", [
    {"clock": "2022-02-30 10:17:00"}, {"clock": "2022-04-29T10:17:00+08:00"},
    {"visible_clock": "2022年4月29日 10:18"},
    {"schema_changes": {"datePublished": "2022-04-29T02:17:00+00:00"}},
    {"schema_changes": {"datePublished": "2022-04-29T10:17:00"}},
    {"schema_changes": {"datePublished": None}},
])
def test_unknown_or_conflicting_release_clocks_not_corrected(registry, changes):
    with pytest.raises(NewsError, match="invalid_bulk_publication_clock"):
        extract_article(page(**changes), registry.require(URL), page_url=URL)


@pytest.mark.parametrize("changes", [{"source_label": "作者："}, {"date_label": "更新日期："}])
def test_borrowed_or_updated_metadata_not_publication_proof(registry, changes):
    with pytest.raises(NewsError, match="article_attribution_not_verified"):
        extract_article(page(**changes), registry.require(URL), page_url=URL)


def test_paid_source_flag_retained(registry):
    with pytest.raises(NewsError, match="paywall_or_login"):
        extract_article(page(schema_changes={"isAccessibleForFree": False}), registry.require(URL), page_url=URL)


@pytest.mark.parametrize("url", [URL + "?secret=1", URL + "#part", URL.replace("https:", "http:"),
    URL.replace("www.gov.mo", "elsewhere.test"), URL.replace("zh-hans", "zh-hant"),
    URL.replace("607199", "0607199")])
def test_unreviewed_representation_or_alias_not_invented(registry, url):
    with pytest.raises(NewsError, match="article_identity_not_verified"):
        extract_article(page(), registry.sources["macau_gov"], page_url=url)


@pytest.mark.parametrize("changes", [
    {"acquisition_mode": "continuous"}, {"require_author_attribution": False},
    {"require_configured_container": False}, {"domains": ["gov.mo"]},
    {"authorization": {"kind": "open_license", "license": "other", "scope": "test",
                       "evidence_url": "https://www.gov.mo/zh-hant/terms-of-use/"}},
])
def test_native_adapter_requires_exact_reviewed_policy(changes):
    config = copy.deepcopy(load_config(CONFIG)[0])
    config["providers"][0].update(changes)
    with pytest.raises(ValueError, match="unreviewed embedded article parser"):
        PolicyRegistry(config, base=Path.cwd())


def test_fetch_retains_actual_release_and_original_response_not_metadata_clock(registry):
    payload = page()

    class HTTP:
        def __init__(self):
            self.registry, self.calls = registry, []

        def get(self, url, **kwargs):
            self.calls.append(url)
            return Response(URL, 200, {"content-type": "text/html"}, payload)

    http = HTTP()
    result = fetch_article(http, {"url": URL, "id": b"id", "metadata_date_raw": "2014-01-01"})
    assert http.calls == [URL]
    assert result["published_at_raw"] == "2022-04-29 10:17:00"
    assert result["metadata_date_raw"] == "2014-01-01"
    assert result["raw_response_body"] == payload
    assert result["requested_url"] == result["final_url"] == URL
    assert result["publish"] is result["remote_training_export"] is False


def test_redirect_to_another_valid_release_does_not_fill_original_head(registry):
    other = "https://www.gov.mo/zh-hans/news/607200/"
    # Both releases are valid article paths in the production policy. The
    # single-URL diagnostic fixture would reject the redirect earlier, before
    # exercising the original-identity guard we want to verify here.
    registry = load_config(Path("configs/news_fulltext.json"))[1]

    class HTTP:
        def __init__(self):
            self.registry = registry

        def get(self, url, **kwargs):
            return Response(other, 200, {"content-type": "text/html"}, page(
                canonical=other, og_url=other, schema_changes={"mainEntityOfPage": {"@id": other}}))

    with pytest.raises(NewsError, match="article_identity_not_verified"):
        fetch_article(HTTP(), {"url": URL, "id": b"id"})

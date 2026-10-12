"""Reviewed public layouts must not relax other sources or fabricate credits."""
import copy
import json
from pathlib import Path

import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article

CONFIG = Path("configs/news/probe_cnews_cmmedia_sources_20261009.json")
CNEWS_URL = "https://cnews.com.tw/204260806a07/"
CMMEDIA_URL = "https://www.cmmedia.com.tw/home/articles/63155"
TEXT = "本篇公開正文的原始值是 -12.5、12345 股及比率 3.25%，不應擷取相關新聞或改寫數值。" * 5


@pytest.fixture
def registry():
    return load_config(CONFIG)[1]


def cnews_page(*, canonical=CNEWS_URL, og_url=CNEWS_URL, title="本篇匯流新聞", visible_title=None,
               author="謝 東明", profile="https://cnews.com.tw/author/cnews204/", credit="CNEWS匯流新聞網記者謝東明／台北報導",
               date="2026-08-06", clock="2026-08-06T18:53:44+08:00", body=TEXT,
               navigation=None, body_access="", schema="", extra="", body_attributes=""):
    if navigation is None:
        navigation = '<div id="modalLogin"><form id="login" action="/wp-login.php"><input type="password"></form></div>'
    clock_meta = f'<meta property="article:published_time" content="{clock}">' if clock is not None else ""
    return f'''<html><head><meta charset="utf-8"><title>{title} - 匯流新聞網</title>
    <link rel="canonical" href="{canonical}"><meta property="og:url" content="{og_url}">
    <meta property="og:title" content="{title} - 匯流新聞網">{clock_meta}</head><body>{navigation}
    <h1>匯流新聞網</h1><section id="articleTitle"><h2>FINANCIAL</h2><div class="special-format"><div class="_line"><strong>{visible_title or title}</strong>
    <ul class="info"><li class="date">{date}</li><li class="user"><a rel="author" href="{profile}">{author}</a></li></ul></div></div></section>
    <div class="theme-article-content"><article id="post-703306" class="post status-publish" {body_attributes}>
    <p>{credit}</p><p>{body}</p>{body_access}</article><div class="extend-wrapper"><h2>延伸閱讀</h2>
    <a rel="author">相關新聞作者</a><p>相關正文{TEXT}</p><li class="date">2099-01-01</li></div></div>
    {schema}{extra}</body></html>'''.encode()


def cmmedia_page(*, canonical=CMMEDIA_URL, og_url=CMMEDIA_URL, title="本篇信傳媒", visible_title=None,
                 author="曾智能", profile="/home/author/100283", visible_clock="2026-10-05 08:04",
                 clock="2026-10-05T08:04:39+08:00", body=TEXT, member="", schema="", extra="", body_attributes=""):
    clock_meta = f'<meta property="article:published_time" content="{clock}">' if clock is not None else ""
    return f'''<html><head><meta charset="utf-8"><title>{title}</title><link rel="canonical" href="{canonical}">
    <meta property="og:url" content="{og_url}"><meta property="og:title" content="{title}">{clock_meta}</head>
    <body><div id="main"><h1 class="article_title">{visible_title or title}</h1><h2>圖片摘要不是正文</h2>
    <div class="article_author-bar" itemprop="author"><div class="article_author_bar_author_div">
    <div class="article_author"><a class="author" href="{profile}"><span class="author_name" itemprop="name">{author}</span></a>
    <span>{visible_clock}</span></div><button>小 中 大</button></div><div class="article_remark">圖片來源／第三方</div></div>
    <div class="article_content" itemprop="articleBody" {body_attributes}><p>{body}</p></div>
    <div id="memberOnly-container">{member}</div><div id="sideBar-container"><p>相關新聞{TEXT}</p></div></div>
    <footer><a rel="author">頁尾作者</a><time>2099-01-01</time></footer>{schema}{extra}</body></html>'''.encode()


def schema(url, *, free=True, headline="", body="隱藏內容不可使用"):
    return '<script type="application/ld+json">' + json.dumps({
        "@type": "NewsArticle", "url": url, "headline": headline,
        "isAccessibleForFree": free, "articleBody": body, "author": {"name": "錯誤 schema 作者"},
    }, ensure_ascii=False) + '</script>'


def test_cnews_actual_navigation_login_is_not_article_access(registry):
    result = extract_article(cnews_page(schema=schema(CNEWS_URL)), registry.sources["cnews"], page_url=CNEWS_URL)
    assert result["title"] == "本篇匯流新聞"
    assert json.loads(result["authors_json"]) == ["謝 東明"]
    assert result["published_at_raw"] == "2026-08-06T18:53:44+08:00"
    assert result["body"] == "CNEWS匯流新聞網記者謝東明／台北報導\n" + TEXT
    assert "隱藏內容" not in result["body"] and "相關正文" not in result["body"]
    assert result["body_completeness"] == "verified_current_article_container"
    assert result["historical_point_in_time"] is False


def test_cnews_source_coauthors_are_preserved_not_lost_to_single_header_credit(registry):
    result = extract_article(cnews_page(credit="CNEWS匯流新聞網記者謝東明、李衣綸／台北報導"), registry.sources["cnews"], page_url=CNEWS_URL)
    assert json.loads(result["authors_json"]) == ["謝 東明", "李衣綸"]


@pytest.mark.parametrize("extra", ['<div class="paywall">付費</div>', '<div data-paywall="true">付費</div>'])
def test_cnews_explicit_paywall_anywhere_stays_excluded(registry, extra):
    with pytest.raises(NewsError, match="paywall_or_login"):
        extract_article(cnews_page(extra=extra), registry.sources["cnews"], page_url=CNEWS_URL)


@pytest.mark.parametrize("navigation,body_access", [
    ('<form id="login" action="/wp-login.php"></form>', ""),
    ('<div id="modalLogin"><form id="different" action="/wp-login.php"></form></div>', ""),
    ("", '<form id="login" action="/login"></form>'),
    ("", '<div id="modalLogin"><form id="login" action="/login"></form></div>'),
    ('<form action="/LOGIN"></form>', ""),
])
def test_cnews_only_exact_external_modal_is_exempted(registry, navigation, body_access):
    with pytest.raises(NewsError, match="paywall_or_login"):
        extract_article(cnews_page(navigation=navigation, body_access=body_access), registry.sources["cnews"], page_url=CNEWS_URL)


@pytest.mark.parametrize("provider,url,page", [("cnews", CNEWS_URL, cnews_page), ("cmmedia", CMMEDIA_URL, cmmedia_page)])
def test_matching_paid_schema_remains_excluded_and_related_schema_is_not_this_article(registry, provider, url, page):
    with pytest.raises(NewsError, match="paywall_or_login"):
        extract_article(page(schema=schema(url, free=False)), registry.sources[provider], page_url=url)
    result = extract_article(page(schema=schema(url + "different", free=False)), registry.sources[provider], page_url=url)
    assert TEXT in result["body"]


@pytest.mark.parametrize("overrides", [
    {"canonical": CNEWS_URL.replace("a07", "a06")}, {"og_url": CNEWS_URL.replace("a07", "a06")},
    {"visible_title": "另一篇新聞"}, {"canonical": "https://other.test/204260806a07/"},
    {"body_attributes": "hidden"}, {"body_attributes": 'style="display: none"'},
])
def test_cnews_unknown_or_hidden_identity_not_accepted(registry, overrides):
    with pytest.raises(NewsError, match="article_identity_not_verified"):
        extract_article(cnews_page(**overrides), registry.sources["cnews"], page_url=CNEWS_URL)


@pytest.mark.parametrize("overrides,code", [
    ({"profile": "https://other.test/author/cnews204/"}, "article_rights_or_scope_excluded"),
    ({"credit": "第三方媒體記者謝東明"}, "article_rights_or_scope_excluded"),
    ({"author": "另一位作者"}, "article_attribution_not_verified"),
])
def test_cnews_other_publisher_or_unmatched_reporter_not_given_default_rights(registry, overrides, code):
    with pytest.raises(NewsError, match=code):
        extract_article(cnews_page(**overrides), registry.sources["cnews"], page_url=CNEWS_URL)


def test_cnews_verified_editorial_account_and_reporter_are_distinct_credits(registry):
    result = extract_article(cnews_page(author="編輯部", profile="https://cnews.com.tw/author/cnews001/",
                                       credit="匯流新聞網記者胡照鑫 / 台北報導"), registry.sources["cnews"], page_url=CNEWS_URL)
    assert json.loads(result["authors_json"]) == ["編輯部", "胡照鑫"]
    details = json.loads(result["author_attribution_detail_json"])
    assert details["header_byline_raw"] == "編輯部" and details["body_reporter_names"] == ["胡照鑫"]
    assert details["byline_relationship"] == "publisher_editorial_account_and_original_reporters"


def test_editorial_label_at_another_profile_is_not_the_reviewed_account(registry):
    with pytest.raises(NewsError, match="article_attribution_not_verified"):
        extract_article(cnews_page(author="編輯部", profile="https://cnews.com.tw/author/cnews999/"), registry.sources["cnews"], page_url=CNEWS_URL)


def test_cnews_reporter_after_role_separator_is_not_lost_or_given_a_role_as_name(registry):
    result = extract_article(cnews_page(author="編輯部", profile="https://cnews.com.tw/author/cnews001/",
                                       credit="匯流新聞網記者/王少筠綜合報導"), registry.sources["cnews"], page_url=CNEWS_URL)
    assert json.loads(result["authors_json"]) == ["編輯部", "王少筠"]
    detail = json.loads(result["author_attribution_detail_json"])
    assert detail["reporter_credit_raw"] == "匯流新聞網記者/王少筠綜合報導"
    assert detail["body_reporter_names"] == ["王少筠"]


@pytest.mark.parametrize("credit", ["匯流新聞網記者/綜合報導", "匯流新聞網記者/報導"])
def test_reporting_role_without_a_person_is_not_fabricated_into_an_author(registry, credit):
    with pytest.raises(NewsError, match="article_attribution_not_verified"):
        extract_article(cnews_page(author="編輯部", profile="https://cnews.com.tw/author/cnews001/",
                                  credit=credit), registry.sources["cnews"], page_url=CNEWS_URL)


def test_cnews_legacy_publishing_account_keeps_the_actual_named_reporter(registry):
    result = extract_article(cnews_page(author="nongdesign", profile="https://cnews.com.tw/author/nongdesign/",
                                       credit="匯流新聞網 陳啟仁/報導"), registry.sources["cnews"], page_url=CNEWS_URL)
    assert json.loads(result["authors_json"]) == ["nongdesign", "陳啟仁"]
    detail = json.loads(result["author_attribution_detail_json"])
    assert detail["byline_relationship"] == "publisher_legacy_account_and_original_reporters"
    assert detail["body_reporter_names"] == ["陳啟仁"]


def test_cnews_unnamed_original_newsroom_does_not_fabricate_a_reporter(registry):
    result = extract_article(cnews_page(credit="CNEWS匯流新聞網／台北報導"), registry.sources["cnews"], page_url=CNEWS_URL)
    assert json.loads(result["authors_json"]) == ["謝 東明"]
    detail = json.loads(result["author_attribution_detail_json"])
    assert detail["body_reporter_names"] == []
    assert detail["byline_relationship"] == "header_byline_and_unnamed_original_newsroom"


@pytest.mark.parametrize("overrides,code", [
    ({"profile": "https://other.test/author/nongdesign/"}, "article_rights_or_scope_excluded"),
    ({"author": "別人", "profile": "https://cnews.com.tw/author/nongdesign/"}, "article_attribution_not_verified"),
    ({"author": "nongdesign", "profile": "https://cnews.com.tw/author/other/"}, "article_rights_or_scope_excluded"),
    ({"author": "nongdesign", "profile": "https://cnews.com.tw/author/nongdesign/", "credit": "第三方記者陳啟仁/報導"}, "article_rights_or_scope_excluded"),
])
def test_legacy_credit_repair_is_not_a_third_party_or_unknown_account_grant(registry, overrides, code):
    with pytest.raises(NewsError, match=code):
        extract_article(cnews_page(**overrides), registry.sources["cnews"], page_url=CNEWS_URL)


def test_invalid_percent_utf8_does_not_conflate_different_original_url_identities(registry):
    payload = cnews_page(canonical="https://cnews.com.tw/%FF/", og_url="https://cnews.com.tw/%FE/")
    with pytest.raises(NewsError, match="article_identity_not_verified"):
        extract_article(payload, registry.sources["cnews"], page_url="https://cnews.com.tw/%FF/")


def test_cmmedia_author_span_not_clock_caption_controls_or_schema(registry):
    result = extract_article(cmmedia_page(schema=schema(CMMEDIA_URL)), registry.sources["cmmedia"], page_url=CMMEDIA_URL)
    assert json.loads(result["authors_json"]) == ["曾智能"]
    assert result["published_at_raw"] == "2026-10-05T08:04:39+08:00"
    assert result["body"] == TEXT and result["body_completeness"] == "verified_current_article_container"
    assert "圖片摘要不是正文" not in result["body"] and "相關新聞" + TEXT not in result["body"]


@pytest.mark.parametrize("member", ["登入才能閱讀全文", '<form action="/login"></form>', '<input type="password">', '<iframe src="/member"></iframe>'])
def test_cmmedia_real_membership_notice_not_a_complete_public_body(registry, member):
    with pytest.raises(NewsError, match="paywall_or_login"):
        extract_article(cmmedia_page(member=member), registry.sources["cmmedia"], page_url=CMMEDIA_URL)


@pytest.mark.parametrize("overrides", [
    {"canonical": CMMEDIA_URL.replace("63155", "63146")}, {"og_url": CMMEDIA_URL.replace("63155", "63146")},
    {"visible_title": "另一篇新聞"}, {"body_attributes": 'aria-hidden="true"'},
])
def test_cmmedia_current_page_and_visible_body_required(registry, overrides):
    with pytest.raises(NewsError, match="article_identity_not_verified"):
        extract_article(cmmedia_page(**overrides), registry.sources["cmmedia"], page_url=CMMEDIA_URL)


@pytest.mark.parametrize("overrides", [{"author": ""}, {"profile": "https://other.test/home/author/1"}])
def test_cmmedia_missing_or_external_author_not_borrowed_from_footer(registry, overrides):
    with pytest.raises(NewsError, match="article_attribution_not_verified"):
        extract_article(cmmedia_page(**overrides), registry.sources["cmmedia"], page_url=CMMEDIA_URL)


@pytest.mark.parametrize("notice", ["本文轉載自《第三方書籍》，非經同意請勿轉載。", "內容來源：第三方授權轉載", "未經授權不得轉載"])
def test_cmmedia_third_party_rights_notice_stays_excluded(registry, notice):
    with pytest.raises(NewsError, match="article_rights_or_scope_excluded"):
        extract_article(cmmedia_page(body=TEXT + "</p><blockquote><p>" + notice + "</p></blockquote><p>"), registry.sources["cmmedia"], page_url=CMMEDIA_URL)


@pytest.mark.parametrize("provider,url,page,overrides", [
    ("cnews", CNEWS_URL, cnews_page, {"date": "2026-02-30"}),
    ("cnews", CNEWS_URL, cnews_page, {"clock": "2026-08-07T18:53:44+08:00"}),
    ("cnews", CNEWS_URL, cnews_page, {"clock": "not-a-clock"}),
    ("cmmedia", CMMEDIA_URL, cmmedia_page, {"visible_clock": "2026-10-05 25:04"}),
    ("cmmedia", CMMEDIA_URL, cmmedia_page, {"clock": "2026-10-05T08:05:39+08:00"}),
])
def test_missing_or_conflicting_source_clocks_never_borrow_scrape_metadata(registry, provider, url, page, overrides):
    with pytest.raises(NewsError, match="invalid_bulk_publication_clock"):
        extract_article(page(**overrides), registry.sources[provider], page_url=url)


@pytest.mark.parametrize("provider,url,page,visible", [
    ("cnews", CNEWS_URL, cnews_page, "2026-08-06"),
    ("cmmedia", CMMEDIA_URL, cmmedia_page, "2026-10-05 08:04"),
])
def test_only_visible_clock_keeps_its_actual_precision(registry, provider, url, page, visible):
    result = extract_article(page(clock=None), registry.sources[provider], page_url=url)
    assert result["published_at_raw"] == visible
    assert "no_timezone_or_missing_precision_inferred" in result["published_time_basis"]


@pytest.mark.parametrize("provider", ["cnews", "cmmedia"])
@pytest.mark.parametrize("change", ["domains", "acquisition_mode", "evidence"])
def test_layout_cannot_enable_another_domain_or_unreviewed_continuous_crawl(provider, change):
    config, _, repo = load_config(CONFIG)
    config = copy.deepcopy(config)
    entry = next(p for p in config["providers"] if p["id"] == provider)
    if change == "domains":
        entry["domains"] = ["other.test"]
    elif change == "acquisition_mode":
        entry[change] = "continuous"
    else:
        entry["authorization"]["evidence_url"] = "https://other.test/terms"
    with pytest.raises(ValueError, match="unreviewed embedded article parser"):
        PolicyRegistry(config, base=repo)


def test_general_whole_page_login_gate_is_unchanged(registry):
    from dataclasses import replace
    source = replace(registry.sources["cnews"], embedded_article_layout="")
    with pytest.raises(NewsError, match="paywall_or_login"):
        extract_article(cnews_page(), source, page_url=CNEWS_URL)


@pytest.mark.parametrize("pinned", [True, False])
def test_only_exact_first_production_attribution_outcomes_are_rechecked(registry, tmp_path, pinned):
    from dataclasses import replace
    from downloader.news_fulltext_cnews_cmmedia import CNEWS_V1_SCOPE_CLASSIFICATION_FINGERPRINT
    from downloader.news_fulltext_queue import NewsQueue

    old_registry = copy.copy(registry)
    old_registry.sources = dict(registry.sources)
    old_registry.sources["cnews"] = replace(registry.sources["cnews"], data_fingerprint=(
        CNEWS_V1_SCOPE_CLASSIFICATION_FINGERPRINT if pinned else "some_other_source_contract"))
    states = ["article_rights_or_scope_excluded", "http_403", "paywall_or_login", "source_not_found", "complete"]
    urls = [f"https://cnews.com.tw/204260806a{number:02d}/" for number in range(1, 6)]
    root = tmp_path / "queue"
    queue = NewsQueue(root, old_registry)
    try:
        queue.add_articles([{"url": url} for url in urls] + [{"url": CMMEDIA_URL}])
        with queue.db:
            for url, state in zip(urls, states):
                queue.db.execute("UPDATE articles SET state=?,error_code=?,attempts=4 WHERE url=?", (state, state, url))
            queue.db.execute("UPDATE articles SET state='article_rights_or_scope_excluded',error_code='article_rights_or_scope_excluded' WHERE url=?", (CMMEDIA_URL,))
    finally:
        queue.close()
    queue = NewsQueue(root, registry)
    try:
        outcomes = {r["url"]: dict(r) for r in queue.db.execute("SELECT url,state,error_code,attempts FROM articles")}
        assert outcomes[urls[0]]["state"] == ("pending" if pinned else states[0])
        assert outcomes[urls[0]]["error_code"] == states[0]  # Original evidence retained until a real result.
        for url, state in zip(urls[1:], states[1:]):
            assert outcomes[url]["state"] == state and outcomes[url]["attempts"] == 4
        assert outcomes[CMMEDIA_URL]["state"] == "article_rights_or_scope_excluded"
        marker = queue.db.execute("SELECT value FROM queue_settings WHERE key='cnews_editorial_byline_v2_repair'").fetchone()
        assert bool(marker) is pinned
        if marker:
            receipt = json.loads(marker[0])
            assert receipt["url_count"] == 1 and receipt["source_refusals_or_stored_heads_reset"] is False
            assert receipt["old_source_fingerprint"] == CNEWS_V1_SCOPE_CLASSIFICATION_FINGERPRINT
        queue.reconcile_policy()
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key='cnews_editorial_byline_v2_repair'").fetchone() == marker
    finally:
        queue.close()


@pytest.mark.parametrize("pinned", [True, False])
@pytest.mark.parametrize("state", ["article_rights_or_scope_excluded", "http_403", "paywall_or_login", "source_not_found", "complete"])
def test_v3_rechecks_only_three_live_reviewed_originals_from_pinned_v2(registry, tmp_path, pinned, state):
    from dataclasses import replace
    from downloader.news_fulltext_cnews_cmmedia import CNEWS_V2_CREDIT_CLASSIFICATION_FINGERPRINT
    from downloader.news_fulltext_queue import NewsQueue

    reviewed = "https://cnews.com.tw/204210911a02/"
    unreviewed = "https://cnews.com.tw/unreviewed-guest/"
    old_registry = copy.copy(registry)
    old_registry.sources = dict(registry.sources)
    old_registry.sources["cnews"] = replace(registry.sources["cnews"], data_fingerprint=(
        CNEWS_V2_CREDIT_CLASSIFICATION_FINGERPRINT if pinned else "some_other_source_contract"))
    root = tmp_path / "queue"
    queue = NewsQueue(root, old_registry)
    try:
        queue.add_articles([{"url": url} for url in (reviewed, unreviewed, CMMEDIA_URL)])
        with queue.db:
            queue.db.execute("UPDATE articles SET state=?,error_code=?,attempts=4 WHERE url=?", (state, state, reviewed))
            queue.db.execute("UPDATE articles SET state='article_rights_or_scope_excluded',error_code='article_rights_or_scope_excluded' WHERE url<>?", (reviewed,))
    finally:
        queue.close()
    queue = NewsQueue(root, registry)
    try:
        outcomes = {r["url"]: dict(r) for r in queue.db.execute("SELECT url,state,error_code,attempts FROM articles")}
        retry = pinned and state == "article_rights_or_scope_excluded"
        assert outcomes[reviewed]["state"] == ("pending" if retry else state)
        assert outcomes[reviewed]["error_code"] == state
        assert outcomes[reviewed]["attempts"] == (0 if retry else 4)
        assert outcomes[unreviewed]["state"] == "article_rights_or_scope_excluded"
        assert outcomes[CMMEDIA_URL]["state"] == "article_rights_or_scope_excluded"
        marker = queue.db.execute("SELECT value FROM queue_settings WHERE key='cnews_original_credit_v3_repair'").fetchone()
        assert bool(marker) is pinned
        if marker:
            receipt = json.loads(marker[0])
            assert receipt["url_count"] == int(retry)
            assert receipt["source_refusals_or_stored_heads_reset"] is False
        queue.reconcile_policy()
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key='cnews_original_credit_v3_repair'").fetchone() == marker
    finally:
        queue.close()

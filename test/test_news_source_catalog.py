from copy import deepcopy
import json
from pathlib import Path

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from stockagent.data.news_source_catalog import configuration_revision, load_news_configuration, source_role
from stockagent.live.news_dashboard import news_snapshot_revision
from test_news_fulltext import config


def test_catalog_is_bounded_local_and_has_no_duplicate_authorities(config, tmp_path):
    path = tmp_path / "news.json"
    config["provider_catalog"] = "sources.json"
    atomic_write_json(path, config)
    atomic_write_json(tmp_path / "sources.json", {"contract_version": 1, "providers": [config["providers"][0]]})
    with pytest.raises(ValueError, match="duplicate"):
        load_news_configuration(path)
    config["provider_catalog"] = "../outside.json"
    atomic_write_json(path, config)
    with pytest.raises(ValueError, match="bounded local"):
        load_news_configuration(path)


def test_catalog_change_invalidate_owner_and_readonly_panel(tmp_path):
    directory = tmp_path / "configs"
    directory.mkdir()
    config = directory / "news_fulltext.json"
    catalog = directory / "sources.json"
    atomic_write_json(config, {"provider_catalog": "sources.json", "providers": []})
    atomic_write_json(catalog, {"contract_version": 1, "providers": []})
    old = configuration_revision(config), news_snapshot_revision(tmp_path)
    atomic_write_json(catalog, {"contract_version": 1, "providers": [], "note": "updated"})
    assert old[0] != configuration_revision(config)
    assert old[1] != news_snapshot_revision(tmp_path)


def test_every_registered_candidate_has_canonical_adapter_but_no_inferred_permission():
    config, registry, _ = load_config(Path("configs/news_fulltext.json"))
    assert len(registry.sources) == len(config["providers"])
    for provider, source in registry.sources.items():
        assert source.selectors or source.cnyes_apis
        if source.authorization.get("kind") == "awaiting_authorization":
            assert not source.allowed
            with pytest.raises(NewsError, match="awaiting_authorization"):
                registry.require("https://" + source.domains[0] + "/")
    assert registry.match("https://ec.ltn.com.tw/news/1").provider == "ltn"
    assert registry.match("https://stock.ltn.com.tw/article/2").provider == "ltn"
    assert registry.match("https://finance.ettoday.net/news/1").provider == "ettoday"
    assert registry.match("https://autos.yahoo.com.tw/a").provider == "yahoo_tw"


def test_redirector_is_not_counted_as_original_publisher():
    assert source_role("news.google.com") == "redirector"
    assert source_role("evilnews.google.com") != "redirector"
    assert source_role("www.msn.com") == "aggregator"
    assert source_role("www.federalreserve.gov") == "government_origin"
    assert source_role("www.gov.mo") == "government_origin"
    assert source_role("www.shiyan.gov.cn") == "government_origin"
    assert source_role("www.gov.mo.attacker.test") == "publisher_or_other_unverified"
    assert source_role("www.gov.moe") == "publisher_or_other_unverified"


def test_macau_government_role_is_not_blanket_capture_permission():
    config, registry, _ = load_config(Path("configs/news_fulltext.json"))
    source = registry.sources["macau_gov"]
    assert source.allowed and source.acquisition_mode == "finite"
    assert source.embedded_article_layout == "macau_visible_news_v1"
    assert source_role("www.gov.mo") == "government_origin"
    recipe = next(p for p in config["providers"] if p["id"] == "macau_gov")
    disabled = deepcopy(recipe)
    disabled["authorization"] = {"kind": "awaiting_authorization"}
    disabled["embedded_article_layout"] = ""
    registry = PolicyRegistry({**config, "providers": [disabled]}, base=Path.cwd())
    assert not registry.sources["macau_gov"].allowed


def einfo_payload(**updates):
    post = {"__typename": "Post", "id": "242961", "title": "原始新聞", "state": "published", "isNewsletter": False,
            "sources": [], "writers": [], "otherByline": "", "reporters": [{"name": "作者"}],
            "publishTime": "2026-01-30T02:58:41.000Z", "updatedAt": "2026-01-31T01:00:00Z"}
    post.update(updates)
    return ('<html><h1>原始新聞</h1><ul class="post-credit__CreditList-x"><li>作者 環境資訊中心 報導</li></ul>'
            '<article id="post"><article class="post-content__Content-x">' + "原始文字報導。" * 30 + '</article></article>'
            '<script id="__NEXT_DATA__">' + json.dumps({"props": {"pageProps": {"postData": post}}}, ensure_ascii=False) + '</script></html>').encode()


def test_einfo_public_editorial_preserves_declared_metadata_not_fake_per_article_license():
    _, registry, _ = load_config(Path("configs/news_fulltext.json"))
    source = registry.sources["einfo"]
    result = extract_article(einfo_payload(), source, page_url="https://e-info.org.tw/node/242961")
    assert result["authors_json"] == '["作者"]'
    assert result["published_at_raw"] == "2026-01-30T02:58:41.000Z"
    assert not result["per_article_license_verified"] and result["declared_license_urls_json"] == "[]"
    assert result["license_verification_basis"] == "publisher_default_policy_verified_original_editorial"
    assert "環境資訊中心" not in result["body"]


@pytest.mark.parametrize("update", [{"id": "99"}, {"state": "draft"}, {"isNewsletter": True},
                                    {"sources": [{"name": "第三方"}]}, {"writers": [{"name": "外稿"}]}, {"reporters": []}])
def test_einfo_rejects_related_draft_external_and_missing_byline(update):
    _, registry, _ = load_config(Path("configs/news_fulltext.json"))
    with pytest.raises(NewsError):
        extract_article(einfo_payload(**update), registry.sources["einfo"], page_url="https://e-info.org.tw/node/242961")


def test_fed_v4_release_font_is_not_footer_update_or_url_date():
    _, registry, _ = load_config(Path("configs/news_fulltext.json"))
    source = registry.sources["fed_board"]
    payload = ('<html><title>Official release</title><body><table width="564">navigation</table>'
               '<font>FOR IMMEDIATE RELEASE<br>September 16, 1999</font>'
               '<table width="600"><tr><td>' + "Original source text with exact numbers and paragraphs. " * 6 + '</td></tr></table>'
               '<p>1999 Other announcements</p><font size="-1">Last update: October 18, 1999</font></body></html>').encode()
    result = extract_article(payload, source, page_url="https://www.federalreserve.gov/boarddocs/press/general/1999/19990915/default.htm")
    assert result["published_at_raw"] == "September 16, 1999"
    assert "navigation" not in result["body"] and "Last update" not in result["body"]


@pytest.mark.parametrize('nested', [False, True])
def test_fed_v6_adjacent_release_cells_bound_body_and_source_date(nested):
    _, reg, _ = load_config(Path('configs/news_fulltext.json'))
    header = '<table width="600"><tr><td>For immediate release</td><td>December 12, 2001</td></tr></table>'
    content = ('<div align="center">Source headline</div><p>' + 'Original monetary policy statement. ' * 8
               + '</p><ul><li>Source point 12.5%</li></ul><table><tr><td>Media contact 1234</td></tr></table>')
    footer = '<p><a href="/boarddocs/press/bcreg/2001">2001 Regulatory archive</a></p><hr><font size="-1">Last update: December 15, 2001</font>'
    if nested:
        html = '<table width="600"><tr><td>Agency navigation</td></tr><tr><td>' + header + content + footer + '</td></tr></table>'
    else:
        header = header.replace('width="600"', 'width="564"')
        html = header + '<table width="600"><tr><td>' + content + '</td></tr></table>' + footer
    result = extract_article(('<html><body>' + html + '</body></html>').encode(), reg.sources['fed_board'],
                             page_url='https://www.federalreserve.gov/boarddocs/press/general/2001/200112110/default.htm')
    assert result['published_at_raw'] == 'December 12, 2001'
    assert result['title'] == 'Source headline'
    assert result['body'].count('Media contact 1234') == 1
    assert 'Source point 12.5%' in result['body']
    assert all(x not in result['body'] for x in ['Agency navigation', 'Last update:', 'Regulatory archive', 'For immediate release'])


def test_fed_v6_never_uses_unrelated_date_cell():
    _, reg, _ = load_config(Path('configs/news_fulltext.json'))
    html = ('<body><table width="600"><tr><td>For immediate release</td><td>Not a publication date</td></tr></table>'
            '<table width="600"><tr><td><p>' + 'Original source text. ' * 20 + '</p></td></tr></table></body>').encode()
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(html, reg.sources['fed_board'], page_url='https://www.federalreserve.gov/boarddocs/press/general/2001/200112110/default.htm')


def test_fed_v7_joint_release_only_uses_rows_after_header():
    _, reg, _ = load_config(Path('configs/news_fulltext.json'))
    html = ('<body><table width="600"><tr><td>Agency navigation</td></tr>'
            '<tr><td>For Immediate Release</td><td>June 5, 2002</td></tr><tr><td colspan="2">'
            '<div align="center">Original source title</div><p>' + 'Original regulatory statement. ' * 12
            + '</p><ul><li>Exact source number 12.5%</li></ul></td></tr></table><font size="-1">Last update: June 8, 2002</font></body>').encode()
    result = extract_article(html, reg.sources['fed_board'],
        page_url='https://www.federalreserve.gov/boarddocs/press/bcreg/2002/20020605/default.htm')
    assert result['published_at_raw'] == 'June 5, 2002' and result['title'] == 'Original source title'
    assert result['body'].count('Exact source number 12.5%') == 1
    assert all(x not in result['body'] for x in ['Agency navigation', 'For Immediate Release', 'Last update:'])


def test_fed_minutes_keeps_publication_unknown_not_meeting_or_footer_date():
    _, reg, _ = load_config(Path('configs/news_fulltext.json'))
    html = ('<body><div id="content"><div id="printThis"><h1>Minutes of the Federal Open Market Committee</h1>'
            '<p>October 30-31 2007</p><div id="leftText">A meeting of the Federal Open Market Committee '
            + 'Original meeting minutes and numbers. ' * 15
            + '<a href="#pagetop">Return to top</a></div></div><div id="lastUpdate">Last update: January 17, 2008</div></div></body>').encode()
    result = extract_article(html, reg.sources['fed_board'], page_url='https://www.federalreserve.gov/monetarypolicy/fomcminutes20071031.htm')
    assert result['published_at_raw'] is None
    assert result['extraction_method'] == 'fed_bounded_document_v1'
    assert all(x not in result['body'] for x in ['Last update:', 'Return to top', 'October 30-31 2007'])


@pytest.mark.parametrize('stamp', ['September 16, 2026', 'Invalid clock'])
def test_fed_implementation_note_requires_declared_clock_and_bounded_body(stamp):
    _, reg, _ = load_config(Path('configs/news_fulltext.json'))
    html = ('<body><h1>Agency masthead</h1><div id="content"><div class="row"><div class="heading col-xs-12">'
            '<p class="article__time">' + stamp + '</p><h3>Implementation Note issued ' + stamp + '</h3></div></div>'
            '<div class="row"><div class="col-xs-12"><p>Decisions Regarding Monetary Policy Implementation</p><ul><li>'
            + 'Source instruction with exact values 3.90 percent. ' * 10 + '</li></ul></div></div>'
            '<div id="lastUpdate">Last Update: October 1, 2026</div></div></body>').encode()
    if stamp == 'Invalid clock':
        with pytest.raises(NewsError, match='body_not_found'):
            extract_article(html, reg.sources['fed_board'], page_url='https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a1.htm')
    else:
        result = extract_article(html, reg.sources['fed_board'], page_url='https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a1.htm')
        assert result['published_at_raw'] == stamp and result['title'] == 'Implementation Note issued ' + stamp
        assert '3.90 percent' in result['body'] and 'Last Update:' not in result['body']


def test_einfo_old_visible_reporter_role_must_match_current_metadata():
    _, registry, _ = load_config(Path("configs/news_fulltext.json"))
    raw = einfo_payload().replace('作者 環境資訊中心 報導'.encode(), '記者 — 作者'.encode())
    accepted = extract_article(raw, registry.sources['einfo'], page_url='https://e-info.org.tw/node/242961')
    assert accepted['authors_json'] == '["作者"]'
    assert accepted['license_verification_basis'] == 'publisher_default_policy_verified_original_editorial'
    for invalid in ('第三方 — 作者', '記者 — 其他人'):
        with pytest.raises(NewsError, match='article_rights_not_verified'):
            extract_article(raw.replace('記者 — 作者'.encode(), invalid.encode()), registry.sources['einfo'], page_url='https://e-info.org.tw/node/242961')


def test_fed_v5_transcript_uses_agreeing_release_clock_not_last_update():
    from dataclasses import replace
    _, registry, _ = load_config(Path('configs/news_fulltext.json'))
    raw = ('<html><body><h2>January 4, 2000</h2><h3>Official transcript</h3><pre>THE WHITE HOUSE\n'
           'Office of the Press Secretary\nFor Immediate Release   January 4, 2000\n' + 'Original speech text. ' * 20 +
           '</pre><p>2000 Other announcements</p><font size="-1">Last update: January 5, 2000</font></body></html>').encode()
    source = replace(registry.sources['fed_board'], legacy_release_layout='fed_table_release_v5')
    result = extract_article(raw, source, page_url='https://www.federalreserve.gov/boarddocs/press/general/2000/20000104/default.htm')
    assert result['published_at_raw'] == 'January 4, 2000' and result['title'] == 'Official transcript'
    assert result['extraction_method'] == 'fed_table_release_v5' and 'Last update' not in result['body']
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(raw.replace(b'<h2>January 4, 2000', b'<h2>January 6, 2000'), source,
                        page_url='https://www.federalreserve.gov/boarddocs/press/general/2000/20000104/default.htm')


def test_fed_v5_interagency_publication_requires_release_header_and_lead_dateline():
    _, registry, _ = load_config(Path('configs/news_fulltext.json'))
    raw = ('<body><table width="560">Navigation</table><table width="560">For immediate release</table>'
           '<table width="555"><p>WASHINGTON, D.C., March 30,1999 - ' + 'Actual source announcement. ' * 20 +
           '</p></table><hr><font size="-1">Last update: April 15, 1999</font></body>').encode()
    source = registry.sources['fed_board']
    url = 'https://www.federalreserve.gov/boarddocs/press/enforcement/1999/19990330/'
    result = extract_article(raw, source, page_url=url)
    assert result['published_at_raw'] == 'March 30,1999' and 'Navigation' not in result['body']
    assert 'Last update' not in result['body']
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(raw.replace(b'For immediate release', b'Other dates in archive'), source, page_url=url)


def test_gv_deliberate_security_removal_is_not_a_parser_gap():
    _, registry, _ = load_config(Path('configs/news_fulltext.json'))
    raw = b'<html><div class="post-header"><div class="post-tagline">This post was taken down for security reasons</div></div><article>Only recommendations</article></html>'
    with pytest.raises(NewsError, match='source_body_removed'):
        extract_article(raw, registry.sources['globalvoices'], page_url='https://globalvoices.org/2025/01/14/removed/')


def gv_access_denied_article():
    url = 'https://globalvoices.org/2021/07/01/access-denied-the-social-and-economic-costs-of-nigerias-twitter-blackout/'
    title = 'Access denied: the social and economic costs of Nigeria’s Twitter blackout'
    raw = ('<html><head><title>' + title + ' · Global Voices</title><link rel="canonical" href="' + url + '"></head>'
           '<body><div class="post-container"><div class="post-header"><h1 class="post-title">' + title + '</h1>'
           '<div id="post-header-credit"><a class="user-link">Olusegun Akinfenwa</a></div></div>'
           '<div class="entry"><p>' + 'Original public news body. ' * 20 + '</p></div>'
           '<div class="post-credit-container"><a rel="license" href="https://creativecommons.org/licenses/by/3.0/">CC</a></div>'
           '</div></body></html>').encode()
    return url, raw


def test_gv_news_title_is_not_a_provider_wide_access_challenge():
    _, registry, _ = load_config(Path('configs/news_fulltext.json'))
    url, raw = gv_access_denied_article()
    result = extract_article(raw, registry.sources['globalvoices'], page_url=url)
    assert result['title'].startswith('Access denied:')
    assert json.loads(result['authors_json']) == ['Olusegun Akinfenwa']
    assert result['body'].startswith('Original public news body.')
    assert result['per_article_license_verified']


@pytest.mark.parametrize('change', ['no_request_url', 'wrong_canonical', 'no_author', 'no_body', 'no_license',
                                  'challenge_form', 'additional_challenge_marker', 'unrelated_provider'])
def test_gv_headline_exception_requires_current_original_identity_and_no_challenge(change):
    _, registry, _ = load_config(Path('configs/news_fulltext.json'))
    url, raw = gv_access_denied_article()
    source = registry.sources['globalvoices']
    if change == 'no_request_url':
        url = None
    elif change == 'wrong_canonical':
        raw = raw.replace(url.encode(), b'https://globalvoices.org/2021/07/01/another-article/')
    elif change == 'no_author':
        raw = raw.replace(b'Olusegun Akinfenwa', b'')
    elif change == 'no_body':
        raw = raw.replace(b'Original public news body. ', b'')
    elif change == 'no_license':
        raw = raw.replace(b'https://creativecommons.org/licenses/by/3.0/', b'https://example.test/license')
    elif change == 'challenge_form':
        raw = raw.replace(b'</body>', b'<form id="challenge-form"></form></body>')
    elif change == 'additional_challenge_marker':
        raw = raw.replace(b'<title>Access denied:', b'<title>Verify you are human Access denied:')
    elif change == 'unrelated_provider':
        from dataclasses import replace
        source = replace(source, provider='another')
    with pytest.raises(NewsError, match='access_challenge'):
        extract_article(raw, source, page_url=url)


def test_gv_real_member_gate_is_not_exempted_by_a_public_headline():
    _, registry, _ = load_config(Path('configs/news_fulltext.json'))
    url, raw = gv_access_denied_article()
    with pytest.raises(NewsError, match='paywall_or_login'):
        extract_article(raw.replace(b'<div class="entry">', b'<div class="entry paywall">'),
                        registry.sources['globalvoices'], page_url=url)


@pytest.mark.parametrize('notice,removed', [
    ('This article was taken down for security reasons on January 21, 2026.', True),
    ('This article was taken down for security reasons on February 30, 2026.', False),
    ('Reporter discusses a post taken down for security reasons on January 21, 2026.', False),
])
def test_gv_dated_security_notice_is_source_removal_not_missing_parser(notice, removed):
    _, registry, _ = load_config(Path('configs/news_fulltext.json'))
    raw = ('<html><div class="post-header"><div class="post-tagline">' + notice + '</div></div>'
           '<div class="post-container"><div class="entry"><blockquote>' + notice + '</blockquote></div></div></html>').encode()
    with pytest.raises(NewsError, match='source_body_removed' if removed else 'article_license_not_verified'):
        extract_article(raw, registry.sources['globalvoices'], page_url='https://globalvoices.org/2019/02/09/removed/')


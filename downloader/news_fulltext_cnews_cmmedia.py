"""Current public article identity/credits, not another acquisition pipeline.

CNEWS has a site-navigation login dialog outside the public article. CMmedia's
author itemprop wraps a clock, controls and a picture caption. Neither element
is evidence of an article paywall or of a compound author name. These adapters
verify the actual original page before narrowing those two source layouts.
"""
from __future__ import annotations

from datetime import datetime
import json
import re
from urllib.parse import unquote, urljoin, urlsplit

from downloader.news_fulltext_policy import NewsError, normalize_url

CNEWS_V1_SCOPE_CLASSIFICATION_FINGERPRINT = "dc1665d80e46d8812d6602e13550c62a4aee5af25bc6a7df70f8869e0ef2a16b"
# This exact first production contract confused the source's 編輯部 account
# with its separately named original reporters. Its scope failures are not an
# access/rights prohibition. Other versions/sources retain protected outcomes.

CNEWS_V2_CREDIT_CLASSIFICATION_FINGERPRINT = "9c96b4bf6791462f0e0e272317b9c786a80ad6a4c35a8db8d0f2f5bcf6cc460d"
CNEWS_V2_REVIEWED_ORIGINAL_URL_SHA256 = frozenset({
    # Three actual original pages in cnews_remaining_scope_review.json, not a
    # blanket reset of v2 exclusions, third-party works or source refusals.
    "abf0cb93a918cc222365bc38b1503c5c1989bbee97bf6ecfc1bf9d0d803c71f0",
    "a15813782f9d5f0523408c8bf27bef89380cdb8e3c0126afe108304aa81ef2da",
    "c36414a6d608717985bfae24246ee8bbd9d0859537bb7cd576dd156e5c276c5c",
})

def _identity(url, host, path_pattern):
    try:
        parts = urlsplit(normalize_url(url))
    except (NewsError, TypeError, ValueError):
        return None
    if (parts.scheme != "https" or parts.netloc != host or parts.query
            or re.search(r"%(?:2f|5c)", parts.path, re.I)
            or not re.fullmatch(path_pattern, parts.path)):
        return None
    # URI-escaped versus literal Chinese is a representation comparison only.
    # Do not rewrite requests, remove real query parameters or change hosts.
    try:
        return unquote(parts.path, errors="strict").rstrip("/")
    except UnicodeError:
        return None


def _page_identity(soup, page_url, host, path_pattern):
    expected = _identity(page_url, host, path_pattern)
    canonical = soup.select('link[rel~="canonical"][href]')
    og_urls = soup.select('meta[property="og:url"][content]')
    if (expected is None or len(canonical) != 1 or len(og_urls) != 1
            or _identity(canonical[0].get("href"), host, path_pattern) != expected
            or _identity(og_urls[0].get("content"), host, path_pattern) != expected):
        raise NewsError("article_identity_not_verified")


def _text_key(value):
    return re.sub(r"\s+", "", value) if isinstance(value, str) else ""


def _publication_clock(soup, visible, format):
    try:
        datetime.strptime(visible, format)
    except (TypeError, ValueError):
        raise NewsError("invalid_bulk_publication_clock") from None
    declared = soup.select('meta[property="article:published_time"][content]')
    if not declared:
        return visible, "publisher_visible_clock; no_timezone_or_missing_precision_inferred"
    if len(declared) != 1:
        raise NewsError("invalid_bulk_publication_clock")
    clock = declared[0].get("content", "")
    try:
        parsed = datetime.fromisoformat(clock.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        raise NewsError("invalid_bulk_publication_clock") from None
    if parsed.strftime(format) != visible:
        raise NewsError("invalid_bulk_publication_clock")
    return clock, "matched_current_article_visible_and_declared_clock; source_precision_and_offset_preserved"


def _visible_body(node):
    for parent in [node, *node.parents]:
        if not getattr(parent, "attrs", None):
            continue
        style = re.sub(r"\s+", "", str(parent.get("style", ""))).lower()
        if (parent.has_attr("hidden") or parent.get("aria-hidden") == "true"
                or "display:none" in style or "visibility:hidden" in style):
            raise NewsError("article_identity_not_verified")


def cnews_public_access_guard(soup):
    """Exclude access markers before identity failures can retain diagnostics."""
    if soup.select_one(".paywall, [data-paywall]") is not None:
        raise NewsError("paywall_or_login")
    for form in soup.find_all("form", action=re.compile("login", re.I)):
        modal = form.find_parent(id="modalLogin")
        if (form.get("id") != "login" or modal is None
                or modal.find_parent(id="articleTitle") is not None
                or modal.find_parent(class_="theme-article-content") is not None):
            raise NewsError("paywall_or_login")


def _declared_access_guard(soup, page_url, headline):
    from downloader.news_fulltext_policy import _objects

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            objects = _objects(json.loads(script.get_text()))
            for obj in objects:
                types = obj.get("@type", [])
                types = [types] if isinstance(types, str) else types
                if not isinstance(types, list) or not any(str(t).rsplit("/", 1)[-1] in {"Article", "NewsArticle", "BlogPosting"} for t in types):
                    continue
                identity = obj.get("mainEntityOfPage") or obj.get("url") or obj.get("@id")
                if isinstance(identity, dict):
                    identity = identity.get("@id") or identity.get("url")
                if identity:
                    try:
                        if normalize_url(urljoin(page_url, identity)) != normalize_url(page_url):
                            continue
                    except (NewsError, TypeError, ValueError):
                        continue
                elif _text_key(obj.get("headline")) != _text_key(headline):
                    continue
                if str(obj.get("isAccessibleForFree", "true")).lower() == "false":
                    raise NewsError("paywall_or_login")
        except (json.JSONDecodeError, TypeError):
            continue


def _cnews_original_credit(paragraphs):
    """Separate actual newsroom/reporting formats from unverified guest text.

    The observed legacy agency line does not name a reporter; keep its names
    empty, rather than deriving a person from a location or a reporting role.
    """
    for text in paragraphs[:4]:
        key = _text_key(text)
        if not key.startswith(("CNEWS匯流新聞網", "匯流新聞網")):
            continue
        if key == "CNEWS匯流新聞網／台北報導":
            return text, [], "original_newsroom_without_named_reporter"
        trailing_names = re.fullmatch(r"(?:CNEWS\s*)?匯流新聞網(?:記者|編輯)\s*[／/]\s*(.*?)(?:綜合)?報導", text)
        legacy_names = re.fullmatch(r"(?:CNEWS\s*)?匯流新聞網\s+([^／/\n]+)[／/]\s*報導", text)
        named = trailing_names or legacy_names or re.match(
            r"^(?:CNEWS\s*)?匯流新聞網(?:記者|編輯)\s*([^／/\n]+)(?:[／/]|$)", text)
        if named:
            names = [name.strip() for name in re.split(r"[、，,]", named[1]) if name.strip()]
            if not names:
                raise NewsError("article_attribution_not_verified")
            if names and len(names) <= 10:
                return text, names, "original_named_reporters"
    raise NewsError("article_rights_or_scope_excluded")


def cnews_visible_article(soup, *, page_url):
    """Verify the current original; exempt only the observed navigation form."""
    cnews_public_access_guard(soup)
    _page_identity(soup, page_url, "cnews.com.tw", r"/[^/?#]+/?")
    headers = soup.select("#articleTitle")
    scopes = soup.select(".theme-article-content")
    bodies = soup.select(".theme-article-content > article[id^='post-']")
    og_titles = soup.select('meta[property="og:title"][content]')
    if (len(headers) != 1 or len(scopes) != 1 or len(bodies) != 1 or len(og_titles) != 1
            or not re.fullmatch(r"post-[1-9][0-9]*", bodies[0].get("id", ""))
            or "status-publish" not in bodies[0].get("class", [])):
        raise NewsError("article_identity_not_verified")
    headline = og_titles[0].get("content", "").removesuffix(" - 匯流新聞網")
    visible_heads = [n for n in headers[0].select(".special-format > ._line > strong")
                     if _text_key(n.get_text(" ", strip=True)) == _text_key(headline)]
    if not headline or len(visible_heads) != 1:
        raise NewsError("article_identity_not_verified")
    _declared_access_guard(soup, page_url, headline)
    _visible_body(bodies[0])
    # Do not weaken the general paywall detector. The only source-specific
    # exception is the observed #modalLogin/#login navigation form, outside
    # this verified public original. Every other login/paywall stays excluded.
    bylines = headers[0].select("li.user > a[rel=author][href]")
    clocks = headers[0].select("li.date")
    if len(bylines) != 1 or len(clocks) != 1:
        raise NewsError("article_attribution_not_verified")
    author = bylines[0].get_text(" ", strip=True)
    profile = _identity(urljoin(page_url, bylines[0].get("href", "")), "cnews.com.tw", r"/author/(?:cnews[0-9]+|nongdesign)/?")
    if not author or profile is None:
        raise NewsError("article_rights_or_scope_excluded")
    paragraphs = [p.get_text(" ", strip=True) for p in bodies[0].find_all("p") if not p.find("p")]
    credit, reporter_names, credit_basis = _cnews_original_credit(paragraphs)
    editorial_account = author == "編輯部" and profile == "/author/cnews001"
    legacy_account = author == "nongdesign" and profile == "/author/nongdesign"
    if (profile == "/author/nongdesign" and not legacy_account
            or author == "編輯部" and not editorial_account
            or reporter_names and not (editorial_account or legacy_account)
               and _text_key(author) not in {_text_key(name) for name in reporter_names}):
        raise NewsError("article_attribution_not_verified")
    relationship = ("header_byline_and_unnamed_original_newsroom" if not reporter_names else
                    "publisher_editorial_account_and_original_reporters" if editorial_account else
                    "publisher_legacy_account_and_original_reporters" if legacy_account else
                    "header_matches_original_reporter")
    clock, clock_basis = _publication_clock(soup, clocks[0].get_text(" ", strip=True), "%Y-%m-%d")
    return {
        "headline": visible_heads[0].get_text(" ", strip=True),
        "author": [{"name": author}, *[{"name": name} for name in reporter_names if _text_key(name) != _text_key(author)]],
        "datePublished": clock, "published_time_basis": clock_basis,
        "article_identity_basis": "matched_source_url_and_visible_cnews_original_v4; navigation_login_is_not_article_access",
        "author_attribution_detail_json": json.dumps({"header_byline_raw": author,
            "reporter_credit_raw": credit, "body_reporter_names": reporter_names,
            "reporter_credit_basis": credit_basis, "byline_relationship": relationship}, ensure_ascii=False),
        "body_completeness": "verified_current_article_container",
    }, scopes[0]


def cmmedia_visible_article(soup, *, page_url):
    """The byline is the current author_name span, not its surrounding bar."""
    _page_identity(soup, page_url, "www.cmmedia.com.tw", r"/home/articles/[1-9][0-9]*/?")
    scopes = soup.select("#main")
    og_titles = soup.select('meta[property="og:title"][content]')
    if len(scopes) != 1 or len(og_titles) != 1:
        raise NewsError("article_identity_not_verified")
    scope = scopes[0]
    heads = scope.select("h1.article_title")
    bodies = scope.select(".article_content[itemprop=articleBody]")
    if (len(heads) != 1 or len(bodies) != 1 or not heads[0].get_text(" ", strip=True)
            or _text_key(heads[0].get_text(" ", strip=True)) != _text_key(og_titles[0].get("content", ""))):
        raise NewsError("article_identity_not_verified")
    _declared_access_guard(soup, page_url, heads[0].get_text(" ", strip=True))
    _visible_body(bodies[0])
    member_notices = scope.select("#memberOnly-container")
    if len(member_notices) > 1 or any(n.get_text(" ", strip=True) or n.find(["form", "input", "iframe"]) for n in member_notices):
        raise NewsError("paywall_or_login")
    bylines = scope.select(".article_author > a.author[href] > .author_name[itemprop=name]")
    clocks = scope.select(".article_author > span")
    if len(bylines) != 1 or len(clocks) != 1 or not bylines[0].get_text(" ", strip=True):
        raise NewsError("article_attribution_not_verified")
    if _identity(urljoin(page_url, bylines[0].parent.get("href", "")), "www.cmmedia.com.tw", r"/home/author/[1-9][0-9]*") is None:
        raise NewsError("article_attribution_not_verified")
    # Preserve third-party notices as exclusions instead of using the website's
    # default personal-use declaration to override another author's rights.
    for node in bodies[0].find_all(["p", "blockquote"]):
        if node.find("p"):
            continue
        notice = node.get_text(" ", strip=True)
        if re.match(r"^(?:本文.{0,24}(?:轉載|摘自|摘錄)|內容來源[：:]|(?:非經|未經)(?:同意|授權).{0,24}(?:請勿|不得)轉載)", notice):
            raise NewsError("article_rights_or_scope_excluded")
    clock, clock_basis = _publication_clock(soup, clocks[0].get_text(" ", strip=True), "%Y-%m-%d %H:%M")
    return {
        "headline": heads[0].get_text(" ", strip=True), "author": [{"name": bylines[0].get_text(" ", strip=True)}],
        "datePublished": clock, "published_time_basis": clock_basis,
        "article_identity_basis": "matched_source_url_title_and_visible_cmmedia_author_name_v1",
        "body_completeness": "verified_current_article_container",
    }, scope

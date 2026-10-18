"""Visible NOWnews / FTV / TaiwanHot / reviewed VOA private source pages.

These are pure identity/metadata adapters, not HTTP clients. Related stories,
author biographies, AMP recommendations and third-party bylines are not the
current article. Schema clocks retain their actual precision and UTC offset.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import re
from urllib.parse import unquote, urljoin, urlsplit

from downloader.news_fulltext_policy import NewsError, _objects, normalize_url, public_article_document


# Actual production v1, before the independently reviewed plain reporter
# credits and separately labelled published/updated clocks were supported.
# Used only for a one-time missing-body clock repair, never source refusals.
NOWNEWS_V1_CAPTURE_FINGERPRINT = "6fda2860fad29ef4496ee5ef7cdd04b33848aed532b68320d4ee3b0628c181d1"


def _key(text):
    return re.sub(r"\s+", "", text) if isinstance(text, str) else ""


def _id(url, provider):
    try:
        parts = urlsplit(normalize_url(url))
    except (NewsError, TypeError, ValueError):
        return None
    if parts.scheme != "https" or parts.query or re.search(r"%(?:2f|5c)", parts.path, re.I):
        return None
    if provider == "nownews" and parts.netloc == "www.nownews.com":
        match = re.fullmatch(r"/(?:amp/)?news/([1-9][0-9]*)/?", parts.path)
    elif provider == "ftvnews" and parts.netloc in {
            "www.ftvnews.com.tw", "finance.ftvnews.com.tw", "sport.ftvnews.com.tw", "health.ftvnews.com.tw"}:
        match = re.fullmatch(r"/news/detail/([A-Za-z0-9]+)(?:/)?", parts.path)
    else:
        return None
    return match[1] if match else None


def _one(root, selector, error="article_identity_not_verified"):
    nodes = root.select(selector)
    if len(nodes) != 1:
        raise NewsError(error)
    return nodes[0]


def _visible(node):
    for parent in [node, *node.parents]:
        if not getattr(parent, "attrs", None):
            continue
        style = re.sub(r"\s+", "", str(parent.get("style", ""))).lower()
        if (parent.has_attr("hidden") or str(parent.get("aria-hidden", "")).lower() == "true"
                or "display:none" in style or "visibility:hidden" in style):
            raise NewsError("article_identity_not_verified")


TAIWANHOT_V1_NATIVE_VIEW_REPAIR_IDENTITY = (
    '7c462aa61ebff890e661f6486ce904c867d03df8dd10c730e4d2a9a979a3ac0c',
    'a69d12dd73e9ad3f6270a9e2a1bbc3ff7df632e550cedf946fb98c645f3e4a85',
)
TAIWANHOT_V2_ROUTE_REPAIR_IDENTITY = (
    'ea6f9212563164712548312d0abaaee3e0f7fe020d378927f6edbdcc3293edf9',
    '8eb0fa981bcf663c65bd839c04110bd074bd8ca3e6ed5b2a192a98bdb6460c83',
)
TAIWANHOT_V3_SHORT_PROSE_REPAIR_IDENTITY = (
    'b03681566468f0a131475f5afd49c5a4f37a9b74d5b67efa882fbf63c4036417',
    'cc4e4719eb62b183e7d26eea72e72795c47560e94d6c8fd27cf790aa9185d6c7',
)


def taiwanhot_article_id(url, *, requested=False, native_view=False, route_views=False):
    """Literal legacy requests are not native originals or generated aliases.

    The ordinary transport owns robots checks and each redirect. Only its
    observed HTTPS native result is a reviewed final representation. v1 stays
    www/queryless. v2 also admits literal bare-host/query views, but only the
    complete source page can verify them; no query is removed or host replaced.
    """
    try:
        parts = urlsplit(normalize_url(url))
    except (NewsError, TypeError, ValueError):
        return None
    hosts = ({'www.taiwanhot.net', 'taiwanhot.net', 'web.taiwanhot.net'} if requested else
             {'www.taiwanhot.net', 'taiwanhot.net'} if native_view else {'www.taiwanhot.net'})
    if (parts.netloc not in hosts or not requested and parts.scheme != 'https'
            or re.search(r'%(?:2f|5c)', parts.path, re.I)):
        return None
    if requested and parts.path == '/':
        match = re.fullmatch(r'p=([1-9][0-9]*)', parts.query)
    elif route_views and (not parts.query or native_view) and re.fullmatch(r'/news/[1-9][0-9]*/?', parts.path):
        match = re.fullmatch(r'/news/([1-9][0-9]*)/?', parts.path)
    elif not parts.query or native_view:
        match = re.fullmatch(r'/news/(?:focus/)?([1-9][0-9]*)/[^/]+(?:/[1-9][0-9]*/[^/]+)?/?', parts.path)
    else:
        return None
    return match[1] if match else None


def taiwanhot_request_ids(url):
    """Admission candidates, never a chosen article or an alternate request.

    Old focus/category/article and current focus/article/category paths share
    the same shape. Do not guess from ID magnitude or take the last number.
    Only the observed OG identity + title can bind an alternate legacy slot.
    """
    primary = taiwanhot_article_id(url, requested=True, native_view=True, route_views=True)
    if primary is None:
        return frozenset()
    parts = urlsplit(normalize_url(url))
    legacy = re.fullmatch(r'/news/focus/[1-9][0-9]*/[^/]+/([1-9][0-9]*)/[^/]+/?', parts.path)
    return frozenset([primary, *([legacy[1]] if legacy else [])])


def taiwanhot_route_matches(url, identity, title):
    """A newly admitted legacy slot needs the literal article-title proof."""
    primary = taiwanhot_article_id(url, requested=True, native_view=True, route_views=True)
    if primary == identity:
        return True
    if primary is None:
        return False
    parts = urlsplit(normalize_url(url))
    legacy = re.fullmatch(r'/news/focus/[1-9][0-9]*/[^/]+/([1-9][0-9]*)/([^/]+)/?', parts.path)
    return bool(legacy and legacy[1] == identity and _key(unquote(legacy[2]).replace('+', ' ')) == _key(title))


def taiwanhot_visible_article(soup, *, page_url, native_views=False, route_views=False, require_body=True):
    """One native content wrapper, not sidebar prose or a syndicated-owner claim."""
    from downloader.news_fulltext_cnews_cmmedia import _declared_access_guard, _publication_clock

    identity = taiwanhot_article_id(page_url, native_view=native_views, route_views=route_views)
    def declared_id(url):
        try:
            if urlsplit(normalize_url(url)).query:
                return None
        except (NewsError, TypeError, ValueError):
            return None
        return taiwanhot_article_id(url, native_view=native_views, route_views=route_views)
    og = _one(soup, 'meta[name="og:url"][content],meta[property="og:url"][content]')
    if route_views:
        # The source declaration is the anchor, not an ambiguous focus slot.
        # Matching this anchor to a legacy route is delayed until visible H2.
        identity = declared_id(og.get('content'))
    if identity is None or declared_id(og.get('content')) != identity:
        raise NewsError('article_identity_not_verified')
    canonical = soup.select('link[rel~="canonical"][href]')
    if len(canonical) > 1 or any(declared_id(node.get('href')) != identity for node in canonical):
        raise NewsError('article_identity_not_verified')
    scope = _one(soup, '.main_content > .content_wrapper')
    headline = _one(scope, '.top_title > h2.news_title')
    bodies = scope.select('article > .news_content')
    if len(bodies) != 1 and (require_body or bodies):
        raise NewsError('body_not_found')
    body = bodies[0] if bodies else None
    _visible(headline)
    if body is not None:
        _visible(body)
    title = headline.get_text(' ', strip=True)
    titles = soup.select('meta[name="og:title"][content],meta[property="og:title"][content]')
    if not title or not titles or len(titles) > 2 or any(_key(node.get('content')) != _key(title) for node in titles):
        raise NewsError('article_identity_not_verified')
    if route_views and (urlsplit(normalize_url(page_url)).scheme != 'https'
            or urlsplit(normalize_url(page_url)).netloc not in {'www.taiwanhot.net', 'taiwanhot.net'}
            or not taiwanhot_route_matches(page_url, identity, title)):
        raise NewsError('article_identity_not_verified')
    if native_views:
        # An actual same-ID bare/query declaration cannot hide a current paid
        # schema from exact-URL guards. Read declarations, never generate an
        # alternate request/route or use related-article schemas as this body.
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                for obj in _objects(json.loads(script.get_text())):
                    types = obj.get('@type', [])
                    types = [types] if isinstance(types, str) else types
                    if not isinstance(types, list) or not any(str(t).rsplit('/', 1)[-1] in {'Article', 'NewsArticle', 'BlogPosting'} for t in types):
                        continue
                    declared = obj.get('mainEntityOfPage') or obj.get('url') or obj.get('@id')
                    if isinstance(declared, dict):
                        declared = declared.get('@id') or declared.get('url')
                    if isinstance(declared, str):
                        url = urljoin(page_url, declared)
                        if ((taiwanhot_route_matches(url, identity, title) if route_views else
                                taiwanhot_article_id(url, native_view=True) == identity)
                                and str(obj.get('isAccessibleForFree', 'true')).lower() == 'false'):
                            raise NewsError('paywall_or_login')
            except (json.JSONDecodeError, TypeError):
                continue
    for url in {page_url, og.get('content')}:
        _declared_access_guard(soup, url, title)
    byline = _one(scope, '.top_title > h5.txt_gray2 > span.reporter_name', 'article_attribution_not_verified')
    clock_node = _one(scope, '.top_title > h5.txt_gray2 > span.post_time', 'invalid_bulk_publication_clock')
    _visible(byline)
    _visible(clock_node)
    author, visible_clock = byline.get_text(' ', strip=True), clock_node.get_text(' ', strip=True)
    if not author:
        raise NewsError('article_attribution_not_verified')
    if not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}', visible_clock):
        raise NewsError('invalid_bulk_publication_clock')
    clock, clock_basis = _publication_clock(soup, visible_clock, '%Y-%m-%d %H:%M')
    for node in (body if body is not None else scope).find_all(['p', 'blockquote']):
        text = node.get_text(' ', strip=True)
        if re.match(r'^(?:本文(?:禁止|不得)轉載|(?:非經|未經)(?:同意|授權).{0,24}(?:請勿|不得)轉載)', text):
            raise NewsError('article_rights_or_scope_excluded')
    # The actual rich-text markup wraps block paragraphs in a <p>. lxml closes
    # it; html.parser retains one direct wrapping <p>. Verify the same final
    # heading/link-list suffix in either tree, never delete that prose wrapper.
    # Unfamiliar tails stay intact. Mutate this request-local DOM once; the
    # shared plaintext walker still owns body extraction and hashing.
    markers = [n for n in body.find_all('h3') if n.get_text(' ', strip=True) == '【延伸閱讀】'] if body is not None else []
    if len(markers) == 1:
        marker = markers[0]
        wrapped = marker.parent.name == 'p' and marker.parent.parent is body
        tail = list(marker.next_siblings)
        lists = [n for n in tail if getattr(n, 'name', None) == 'ul']
        suffix_ok = all(getattr(n, 'name', None) in {'ul', 'br'} if getattr(n, 'name', None)
                        else not str(n).strip() for n in tail)
        wrapper_ok = not wrapped or all(getattr(n, 'name', None) == 'br' if getattr(n, 'name', None)
                                       else not str(n).strip() for n in marker.parent.next_siblings)
        if ((marker.parent is body or wrapped) and suffix_ok and wrapper_ok
                and len(lists) == 1 and lists[0].select('li > a[href]')):
            for node in [marker, *[n for n in tail if getattr(n, 'name', None)]]:
                node.decompose()
    return {
        'headline': title, 'author': [{'name': author}], 'datePublished': clock,
        'published_time_basis': clock_basis,
        'article_identity_basis': ('same_native_news_id_visible_h2_and_queryless_declared_og_title_url_v2'
                                   if native_views else 'same_native_news_id_visible_h2_and_declared_og_title_url_v1'),
        **({'source_native_article_id': identity,
            'article_identity_basis': 'source_og_id_and_visible_h2_bind_literal_native_or_legacy_focus_route_v3'}
           if route_views else {}),
        'author_attribution_detail_json': json.dumps({'byline_raw': author,
            'byline_relationship': 'source_visible_credit_not_inferred_person_or_owner',
            'rights_owner_inferred': False}, ensure_ascii=False),
        'body_completeness': 'verified_current_article_container',
    }, scope


def taiwanhot_verified_short_prose(container, text, source):
    """Native metadata must already bind the whole container before calling.

    Heading/graphic-caption text is not prose. Leaf paragraph subtrees are
    disjoint, so malformed nesting cannot create repeated quadratic text walks.
    A paragraph must contribute to the final configured text, not an excluded
    photo/caption region. This does not infer a publication clock or rights.
    """
    from downloader.news_fulltext_policy import _plain_text_node

    for paragraph in container.find_all('p'):
        if (paragraph.find('p')
                or paragraph.find(['h1','h2','h3','h4','figure','figcaption','table'])
                or paragraph.find_parent(['figure','figcaption'])):
            continue
        prose = _plain_text_node(paragraph, source.exclude_selectors, parser=source.html_parser)
        if prose and prose in text and (re.search(r'[\u3400-\u9fff]', prose) or len(prose.split()) >= 8):
            return True
    return False


def _ftv_paired_quote_title(soup, page_url, headline, declared, *, quote_edges=False):
    """One observed unescaped label quote pair, not fuzzy matching.

    FTV's current document title preserves the four ASCII apostrophes, its
    schema omits them, and its malformed OG title ends at the opening pair.
    Every other character and the independent full document title must agree.
    This comparison never edits a retained title, value, body or source clock.
    """
    if urlsplit(page_url).hostname != 'www.ftvnews.com.tw':
        raise NewsError('article_identity_not_verified')
    match = re.fullmatch(r"([^']+)''([\u3400-\u9fff]{2,20})''([^']+)", headline)
    version = 2
    if match is None and quote_edges:
        # Source-proved v3: a one-character CJK label or a pair at the
        # beginning. A nonempty tail is still required. Missing OG is not an
        # empty OG declaration, and no other character may disappear.
        match = re.fullmatch(r"([^']*)''([\u3400-\u9fff]{1,20})''([^']+)", headline)
        version = 3
    if (match is None or _key(declared) != _key(''.join(match.groups()))):
        raise NewsError('article_identity_not_verified')
    document = _one(soup, 'head > title').get_text(' ', strip=True)
    og = _one(soup, 'meta[property="og:title"][content]').get('content')
    if (_key(document) != _key(headline + ' - 民視新聞網')
            or _key(og) != _key(match[1])):
        raise NewsError('article_identity_not_verified')
    return {'kind': ('ftv_unescaped_paired_company_quotes' if version == 2
                     else 'ftv_unescaped_paired_label_quotes'), 'contract_version': version,
        'visible_headline_raw': headline, 'schema_headline_raw': declared,
        'document_title_raw': document, 'og_title_raw': og,
        'identity_comparison_only': True, 'original_values_modified': False}


def _ftv_schema_json(text):
    """Decode the observed literal value TAB without changing any JSON value.

    The strict fast path is unchanged. Only v4 calls this fallback: a bounded
    lexical pass proves every literal string control is a TAB, then the stdlib
    decoder preserves it. Controls in keys, broken escapes, comments and other
    invalid grammar still fail. Never strip controls or rewrite source bytes.
    """
    if len(text) > 262144:
        raise ValueError('bounded FTV schema required')
    try:
        return json.loads(text), 0
    except json.JSONDecodeError:
        pass
    quoted = escaped = False
    tabs = 0
    for char in text:
        if quoted:
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == '"':
                quoted = False
            elif ord(char) < 32:
                if char != '\t':
                    raise ValueError('unreviewed FTV string control')
                tabs += 1
        elif char == '"':
            quoted = True
    if not tabs:
        raise ValueError('unreviewed FTV JSON syntax')

    def keys_without_controls(pairs):
        if any(any(ord(char) < 32 for char in key) for key, _ in pairs):
            raise ValueError('FTV JSON key control')
        return dict(pairs)

    return json.loads(text, strict=False, object_pairs_hook=keys_without_controls), tabs


def _identity_and_schemas(soup, page_url, provider, headline, *, amp=False,
                          ftv_paired_quotes=False, ftv_quote_edges=False,
                          ftv_literal_tabs=False, syntax_notices=None):
    identity = _id(page_url, provider)
    canonical = _one(soup, 'link[rel~="canonical"][href]')
    if identity is None or _id(canonical.get("href"), provider) != identity:
        raise NewsError("article_identity_not_verified")
    og = soup.select('meta[property="og:url"][content]')
    if ((not amp or og) and (len(og) != 1 or _id(og[0].get("content"), provider) != identity)):
        raise NewsError("article_identity_not_verified")
    matches = []
    scripts = soup.find_all("script", type="application/ld+json")
    if ftv_literal_tabs and (provider != 'ftvnews' or len(scripts) > 16):
        raise NewsError('article_identity_not_verified')
    for script in scripts:
        try:
            value, tabs = (_ftv_schema_json(script.get_text()) if ftv_literal_tabs
                           else (json.loads(script.get_text()), 0))
            candidates = _objects(value)
            for candidate in candidates:
                types = candidate.get("@type", [])
                types = [types] if isinstance(types, str) else types
                if not isinstance(types, list) or not any(t in {"Article", "NewsArticle"} for t in types if isinstance(t, str)):
                    continue
                declared = candidate.get("mainEntityOfPage") or candidate.get("url") or candidate.get("@id")
                if isinstance(declared, dict):
                    declared = declared.get("@id") or declared.get("url")
                if _id(declared, provider) != identity:
                    continue  # Related/infinite-scroll schemas never supply this article.
                if str(candidate.get("isAccessibleForFree", "true")).lower() == "false":
                    raise NewsError("paywall_or_login")
                if _key(candidate.get("headline")) != _key(headline):
                    if provider != 'ftvnews' or not ftv_paired_quotes:
                        raise NewsError("article_identity_not_verified")
                    _ftv_paired_quote_title(soup, page_url, headline, candidate.get('headline'),
                        quote_edges=ftv_quote_edges)
                matches.append(candidate)
                if tabs and syntax_notices is not None:
                    syntax_notices.append({'kind': 'ftv_literal_json_value_tabs', 'contract_version': 4,
                        'literal_tab_count': tabs, 'decoded_values_modified': False,
                        'raw_evidence': 'shared_original_response_receipt'})
        except (json.JSONDecodeError, TypeError):
            if ftv_literal_tabs:
                raise NewsError('article_identity_not_verified') from None
            continue
        except (ValueError, RecursionError):
            if ftv_literal_tabs:
                raise NewsError('article_identity_not_verified') from None
            raise
    if not matches:
        raise NewsError("article_identity_not_verified")
    return matches


def _clock(soup, schemas, visible, format):
    try:
        datetime.strptime(visible, format)
        raw = _one(soup, 'meta[property="article:published_time"][content]',
                   "invalid_bulk_publication_clock").get("content")
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if (parsed.tzinfo is None or parsed.strftime(format) != visible
                or any(schema.get("datePublished") != raw for schema in schemas)):
            raise ValueError("current visible and declared clock disagree")
        for meta in soup.select('meta[itemprop="datePublished"][content]'):
            if meta.get("content") != raw:
                raise ValueError("publication metadata disagree")
    except (TypeError, ValueError, AttributeError):
        raise NewsError("invalid_bulk_publication_clock") from None
    return raw


def _schema_authors(schemas):
    values = []
    for schema in schemas:
        authors = schema.get("author")
        authors = authors if isinstance(authors, list) else [authors]
        names = [author.get("name", "").strip() for author in authors
                 if isinstance(author, dict) and isinstance(author.get("name"), str)]
        if not names or any(not name for name in names):
            raise NewsError("article_attribution_not_verified")
        values.append(names)
    if any(names != values[0] for names in values):
        raise NewsError("article_attribution_not_verified")
    return values[0]


def _nownews_regular_clock(soup, schemas, info):
    """One explicitly labelled publication, not publication + later revision."""
    time = _one(info, 'time.time', 'invalid_bulk_publication_clock')
    _visible(time)
    published_nodes = time.select('span[aria-label="出版時間"]')
    if published_nodes:
        published_node = _one(time, 'span[aria-label="出版時間"]', 'invalid_bulk_publication_clock')
        _visible(published_node)
        visible = published_node.get_text(' ', strip=True)
    else:
        # Older pages/fixtures can declare a single unlabelled clock. Exact
        # parsing below rejects two dates, unknown labels and footer dates.
        visible = time.get_text(' ', strip=True)
    published = _clock(soup, schemas, visible, '%Y-%m-%d %H:%M:%S')
    updated_nodes = time.select('span[aria-label="更新時間"]')
    modified = [schema.get('dateModified') for schema in schemas]
    if any(value != modified[0] for value in modified):
        raise NewsError('invalid_bulk_publication_clock')
    if updated_nodes:
        updated = _one(time, 'span[aria-label="更新時間"]', 'invalid_bulk_publication_clock')
        _visible(updated)
        try:
            parsed = datetime.fromisoformat(modified[0].replace('Z', '+00:00'))
            if (parsed.tzinfo is None or parsed.strftime('%Y-%m-%d %H:%M:%S') != updated.get_text(' ', strip=True)
                    or parsed < datetime.fromisoformat(published.replace('Z', '+00:00'))):
                raise ValueError('revision clock disagrees with its visible label')
            raw = _one(soup, 'meta[property="article:modified_time"][content]', 'invalid_bulk_publication_clock').get('content')
            if raw != modified[0]:
                raise ValueError('source revision declarations disagree')
        except (TypeError, AttributeError, ValueError):
            raise NewsError('invalid_bulk_publication_clock') from None
    return published


def nownews_visible_article(soup, *, page_url):
    """Verify both real public layouts; never fetch an alternative after refusal."""
    amp = urlsplit(page_url or "").path.startswith("/amp/")
    if amp:
        scope = _one(soup, ".newDetailContainer > .newsContainer")
        headline_node = _one(scope, "h1.newsTitle")
        body = _one(scope, ".newsBox > .newsContainerLeft > .newsMsg", "body_not_found")
        byline = _one(scope, ".newsBox > .newsContainerLeft > .newsInfo", "article_attribution_not_verified")
        match = re.fullmatch(r"(.+)-([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})",
                             byline.get_text(" ", strip=True))
        if not match:
            raise NewsError("article_attribution_not_verified")
        credit, visible_clock = match.groups()
    else:
        headline_node = _one(soup, ".mainBlk .titleBlk .title h1")
        scope = _one(soup, "article.article-body[itemprop=articleBody]")
        body = _one(scope, ".article-content > #articleContent.article-content-edtor", "body_not_found")
        info = _one(soup, ".mainBlk .infoBlk > .info", "article_attribution_not_verified")
        byline = _one(info, "p", "article_attribution_not_verified")
        credit = byline.get_text(" ", strip=True)
    for node in (headline_node, body, byline):
        _visible(node)
    headline = headline_node.get_text(" ", strip=True)
    schemas = _identity_and_schemas(soup, page_url, "nownews", headline, amp=amp)
    named = re.fullmatch(r"(?:記者|編輯)\s*([^／/]+)[／/]\s*[^／/]+報導", credit)
    if not named:
        raise NewsError("article_rights_or_scope_excluded")
    names = [name.strip() for name in re.split(r"[、，,]", named[1]) if name.strip()]
    if not names:
        raise NewsError("article_attribution_not_verified")
    declared_names = _schema_authors(schemas)
    byline_basis = 'current_visible_reporter_and_matching_source_metadata'
    profiles = [] if amp else byline.select("a[href]")
    if amp or not profiles:
        # Actual legacy regular pages declare the *whole* visible reporter
        # credit as schema author and do not have a profile link. The current
        # visible named byline + current schema + meta author still agree;
        # never invent a profile or use a related/footer author as evidence.
        if not all(_key(name) == _key(credit) for name in declared_names):
            raise NewsError("article_attribution_not_verified")
        byline_basis = 'current_visible_named_reporter_and_matching_full_credit_schema_and_meta'
    else:
        if [_key(name) for name in declared_names] != [_key(name) for name in names]:
            raise NewsError("article_attribution_not_verified")
        if len(profiles) != len(names):
            raise NewsError("article_attribution_not_verified")
        for link, name in zip(profiles, names):
            target = urlsplit(urljoin(page_url, link.get("href")))
            try:
                path = unquote(target.path, errors="strict")
            except UnicodeError:
                raise NewsError("article_attribution_not_verified") from None
            if (target.scheme != "https" or target.netloc != "www.nownews.com" or target.query
                    or path.rstrip("/") != "/reporter/" + name or _key(link.get_text(" ", strip=True)) != _key(name)):
                raise NewsError("article_attribution_not_verified")
    meta_author = _one(soup, 'meta[itemprop="author"][content]', "article_attribution_not_verified")
    if _key(meta_author.get("content")) != _key(credit):
        raise NewsError("article_attribution_not_verified")
    published = (_clock(soup, schemas, visible_clock, "%Y-%m-%d %H:%M:%S") if amp
                 else _nownews_regular_clock(soup, schemas, info))
    return {
        "headline": headline, "author": [{"name": name} for name in names],
        "datePublished": published, "dateModified": schemas[0].get("dateModified"),
        "published_time_basis": "matched_visible_and_declared_clock; source_seconds_and_offset_preserved",
        "article_identity_basis": "nownews_same_article_id_canonical_and_schema_v2; amp_representation_not_replacement",
        "body_completeness": "verified_current_article_container",
        "author_attribution_detail_json": json.dumps({"visible_byline_raw": credit,
            "source_meta_author_raw": meta_author.get("content"), "schema_authors_raw": declared_names,
            "byline_basis": byline_basis}, ensure_ascii=False, sort_keys=True),
    }, scope


def voa_original_article(soup, source, *, page_url):
    """Match the actual Pangea original, not its bio or cross-language link.

    The Cantonese response declares a different Chinese story as canonical.
    Preserve that declaration but do not replace the requested observation,
    follow it, or authorize arbitrary cross-domain aliases. Only the actual
    independently retained pair below is reviewed; other pairs stay gaps.
    """
    private_scope = source.embedded_article_layout == "voa_reviewed_private_article_v2"
    host = {"voa_chinese": "www.voachinese.com", "voa_cantonese": "www.voacantonese.com"}.get(source.provider)
    try:
        expected = normalize_url(page_url)
    except (NewsError, TypeError):
        raise NewsError("article_identity_not_verified") from None
    parts = urlsplit(expected)
    if (parts.scheme != "https" or parts.netloc != host or parts.query
            or not re.fullmatch(r"/a/[^/]+/[0-9]+[.]html", parts.path)):
        raise NewsError("article_identity_not_verified")
    scope = _one(soup, "main.container")
    headline_node = _one(scope, "h1.title.pg-title")
    if private_scope:
        _visible(headline_node)
    headline = headline_node.get_text(" ", strip=True)
    if not headline or _one(soup, 'meta[property="og:title"]').get("content") != headline:
        raise NewsError("article_identity_not_verified")
    canonical = _one(soup, 'link[rel="canonical"]').get("href")
    og_url = _one(soup, 'meta[property="og:url"]').get("content")
    translation_pair = {
        "https://www.voacantonese.com/a/TC-former-us-official-calls-for-including-taiwan-in-a-new-pandemic-response-initiative-with-asia-20200403-ry/5358735.html":
        "https://www.voachinese.com/a/former-us-official-calls-for-including-taiwan-in-a-new-pandemic-response-initiative-with-asia-20200402/5357662.html"
    }
    if private_scope:
        # Each pair was observed in the same retained public response whose
        # og/schema/request identity and byline are independently checked.
        # This is not a guessed same-ID alias or authorization to follow it.
        translation_pair.update({
            "https://www.voacantonese.com/a/attention-turns-to-presidential-poll-after-taiwan-ruling-party-thrashing-20221127/6851862.html":
            "https://www.voachinese.com/a/attention-turns-to-presidential-poll-after-taiwan-ruling-party-thrashing-20221127/6851803.html",
            "https://www.voacantonese.com/a/beijing-critic-nikki-haley-to-announce-candidacy-for-president-20230201/6942904.html":
            "https://www.voachinese.com/a/beijing-critic-nikki-haley-to-announce-candidacy-for-president-20230201/6942832.html",
            "https://www.voacantonese.com/a/biden-unveils-6-8-trillion-2024-budget-plan-20230309/6998804.html":
            "https://www.voachinese.com/a/biden-unveils-6-8-trillion-2024-budget-plan-20230309/6998231.html",
            "https://www.voacantonese.com/a/experts-predict-2024-taiwan-presidential-election-and-beijings-likely-reaction-20221208/6869142.html":
            "https://www.voachinese.com/a/experts-predict-2024-taiwan-presidential-election-and-beijings-likely-reaction-20221208/6868920.html",
            "https://www.voacantonese.com/a/haley-announces-2024-bid-20230214/6963826.html":
            "https://www.voachinese.com/a/haley-announces-2024-bid-20230214/6962936.html",
            "https://www.voacantonese.com/a/pentagon-budget-readies-us-for-possible-china-confrontation-03242023/7020040.html":
            "https://www.voachinese.com/a/pentagon-budget-readies-us-for-possible-china-confrontation-03242023/7019997.html",
        })
    try:
        canonical = normalize_url(canonical)
        identities_match = (normalize_url(og_url) == expected
                            and canonical in {expected, translation_pair.get(expected)})
    except (NewsError, TypeError):
        identities_match = False
    if not identities_match:
        raise NewsError("article_identity_not_verified")
    schemas = []
    for node in soup.select('script[type="application/ld+json"]'):
        try:
            for item in _objects(json.loads(node.get_text())):
                kinds = item.get("@type")
                if kinds == "NewsArticle" or isinstance(kinds, list) and "NewsArticle" in kinds:
                    schemas.append(item)
        except (TypeError, ValueError):
            raise NewsError("article_identity_not_verified") from None
    if (len(schemas) != 1 or schemas[0].get("url") != expected
            or schemas[0].get("mainEntityOfPage") != expected or schemas[0].get("headline") != headline):
        raise NewsError("article_identity_not_verified")
    schema = schemas[0]
    if private_scope and schema.get("isAccessibleForFree") is not True:
        raise NewsError("paywall_or_login" if schema.get("isAccessibleForFree") is False else "article_rights_not_verified")
    names = [n.get_text(" ", strip=True) for n in scope.select(source.author_selectors[0])]
    if not names or any(name not in source.permitted_author_names for name in names):
        raise NewsError("article_rights_or_scope_excluded")
    authors = schema.get("author")
    authors = authors if isinstance(authors, list) else [authors]
    if (any(not isinstance(item, dict) for item in authors)
            or [item.get("name") for item in authors] != names):
        raise NewsError("article_attribution_not_verified")
    body = _one(scope, "#article-content > .wsw", "body_not_found")
    if private_scope:
        for node in [scope, body, *scope.select(source.author_selectors[0])]:
            _visible(node)
    if not body.find("p"):
        raise NewsError("body_not_found")
    stamp = _one(scope, ".publishing-details > .published > .date > time", "invalid_bulk_publication_clock")
    if private_scope:
        _visible(stamp)
    visible, machine = stamp.get_text(" ", strip=True), stamp.get("datetime")
    published = schema.get("datePublished")
    try:
        machine_clock = datetime.fromisoformat(machine.replace("Z", "+00:00"))
        declared_clock = datetime.fromisoformat(published.replace("Z", "+00:00"))
        visible_match = re.fullmatch(r"([0-9]{4})年([0-9]{1,2})月([0-9]{1,2})日(?: ([0-9]{2}):([0-9]{2}))?", visible)
        valid = (machine_clock.tzinfo is not None and declared_clock.tzinfo is not None
                 and machine_clock == declared_clock and visible_match is not None
                 and (machine_clock.year, machine_clock.month, machine_clock.day)
                 == tuple(int(visible_match[i]) for i in (1, 2, 3)))
        if valid and visible_match[4] is not None:
            valid = (machine_clock.hour, machine_clock.minute) == (int(visible_match[4]), int(visible_match[5]))
        modified = schema.get("dateModified")
        if modified is not None:
            modified_clock = datetime.fromisoformat(modified.replace("Z", "+00:00"))
            valid = valid and modified_clock.tzinfo is not None and modified_clock >= declared_clock
    except (ValueError, AttributeError, TypeError, OverflowError):
        valid = False
    if not valid:
        raise NewsError("invalid_bulk_publication_clock")
    return {
        "headline": headline, "author": [{"name": name} for name in names],
        "datePublished": published, "dateModified": modified,
        "published_time_basis": "declared_schema_clock_matched_to_time_datetime; visible_precision_preserved_not_inferred",
        "article_identity_basis": ("voa_current_og_schema_visible_article_v2; reviewed_translation_canonical_not_followed"
                                   if canonical != expected else "voa_current_canonical_og_schema_visible_article_v2") if private_scope else (
                                      "voa_current_og_schema_visible_article_v1; reviewed_translation_canonical_not_followed"
                                      if canonical != expected else "voa_current_canonical_og_schema_visible_article_v1"),
        "body_completeness": ("verified_source_text_container; media_embeds_not_captured" if private_scope
                              else "verified_original_text_container; media_embeds_not_captured"),
        "author_attribution_detail_json": json.dumps({
            "visible_author_names": names, "source_visible_publication_text": visible,
            "source_time_datetime_raw": machine, "source_schema_publication_raw": published,
            "source_declared_canonical_url": canonical, "canonical_followed": False,
            **({"capture_scope": "personal_noncommercial_storage_only",
                "public_domain_status_verified": False, "downstream_reuse_cleared": False,
                "training_export_cleared": False} if private_scope else {}),
        }, ensure_ascii=False, sort_keys=True),
    }, scope


def _ftv_parenthesized_credit(text):
    return (len(text) <= 200 and text[:1] + text[-1:] in {'（）', '()'}
        and re.fullmatch(r'民視新聞(?:網)?[ \t　]*[／/][ \t　]*[^()（）<>]{1,160}報導', text[1:-1]) is not None)


def _ftv_newsroom_credit(body, *, standalone_credits=False, syntax_notices=None):
    """Credit lines are not whole paragraphs; preserve actual source breaks."""
    credits, legacy_credits, standalone = [], [], []
    ignored_roles = ['figure', 'figcaption', 'nav', 'aside']
    if standalone_credits:
        ignored_roles += ['blockquote', 'footer', 'header', 'template', 'noscript']
    for node in body.find_all("p"):
        if node.find("p") or node.find_parent(ignored_roles):
            continue
        # Actual originals put the credit and hundreds of body characters in
        # one <p>, separated by <br><br>. Flattening that whole paragraph loses
        # the credit boundary and rejects an original as an overlong byline.
        # Inline author markup remains part of this first line; captions and
        # nested blocks do not become credit evidence.
        parts, inline_nodes = [], []
        for child in node.children:
            if getattr(child, 'name', None) in {'br', 'p', 'div', 'figure', 'figcaption', 'nav', 'aside', 'ul', 'ol'}:
                break
            if getattr(child, 'name', None):
                inline_nodes.append(child)
                parts.append(child.get_text(' ', strip=True))
            else:
                parts.append(str(child))
        text = ''.join(parts).strip()
        native_credit = None
        if standalone_credits:
            # The observed older source has a byline-only <p><br/>credit</p>
            # or one horizontal space before '/'. Never search prose for a
            # reporter or flatten a whole mixed body paragraph into a byline.
            full_text = node.get_text(' ', strip=True) if not text else text
            if (_ftv_parenthesized_credit(full_text)
                    and node.get_text(' ', strip=True) == full_text
                    and not node.find(['p', 'div', 'figure', 'figcaption', 'img', 'iframe', 'video', 'audio', 'ul', 'ol'])
                    and not node.find_parent(['blockquote', 'footer', 'header', 'template', 'noscript'])):
                native_credit = full_text
            if (not text and len(full_text) <= 200
                    and not node.find(['img', 'iframe', 'video', 'audio'])
                    and not node.find_parent(['blockquote', 'footer', 'header', 'template', 'noscript'])
                    and (full_text in {'（中央社）', '(中央社)'} or re.match(
                        r'^(?:圖[、,，]?文|文|圖)[／/]\s*(?!民視)(\S+)|^(?:CNEWS|匯流新聞網|CTWANT|周刊王)', full_text))):
                # A leading break cannot conceal a competing source credit.
                text = full_text
        if len(text) > 200:
            continue
        # Actual older finance articles end with a standalone （中央社）.
        # This is explicit syndicated provenance, not a missing newsroom
        # reporter. Do not infer it from a mention, a first line followed by
        # prose, a photo, a quotation or a page/related-story footer. Inspect
        # the full paragraph only for this rare, exact marker; the common
        # credit path and original text are unchanged.
        if (text in {'（中央社）', '(中央社)'}
                and node.get_text(' ', strip=True) == text
                and not node.find(['img', 'iframe', 'video', 'audio'])
                and not node.find_parent(['blockquote', 'footer', 'header', 'template', 'noscript'])):
            _visible(node)
            for inline in inline_nodes:
                _visible(inline)
            error = NewsError('article_rights_or_scope_excluded')
            # Static, non-prose witness for the normal owner's telemetry.
            # No retained third-party response or extra source request.
            error.ftv_native_scope_witness = 'standalone_visible_CNA_credit_v1'
            raise error
        # Explicit third-party provenance stays binding even if another
        # paragraph mentions this publisher's newsroom.
        if re.match(r"^(?:圖[、,，]?文|文|圖)[／/]\s*(?!民視)(\S+)|^(?:CNEWS|匯流新聞網|CTWANT|周刊王)", text):
            raise NewsError("article_rights_or_scope_excluded")
        # Actual legacy originals sign as 民視新聞網 (including an unnamed
        # newsroom 報導), and current TV originals use 南部中心. These are
        # visible own-source credits, not a schema editor guessed to be the
        # reporter. Keep the raw credit and declared authors separate.
        if (re.fullmatch(r"(?:(?:即時|財經|政治|社會|生活|娛樂|體育|國際|論壇|南部)中心|民視新聞(?:網)?)[／/].+", text)
                or re.fullmatch(r"[（(]民視新聞(?:網)?(?:[／/].+)?報導[）)]", text)):
            _visible(node)
            for inline in inline_nodes:
                _visible(inline)
            credits.append(text)
            legacy_credits.append(text)
        elif native_credit is not None:
            _visible(node)
            for inline in node.find_all():
                _visible(inline)
            leading_breaks = 0
            for child in node.children:
                if getattr(child, 'name', None) == 'br':
                    leading_breaks += 1
                elif getattr(child, 'name', None) or str(child).strip():
                    break
            credits.append(native_credit)
            standalone.append({'contract_version': 7, 'visible_newsroom_credit_raw': native_credit,
                'paragraph_leading_br_count': leading_breaks, 'paragraph_text_only_credit': True,
                'basis': 'standalone_parenthesized_native_newsroom_credit', 'original_values_modified': False})
    credits = list(dict.fromkeys(credits))
    if len(credits) != 1:
        raise NewsError("article_attribution_not_verified")
    if syntax_notices is not None and credits[0] not in legacy_credits:
        syntax_notices.extend(standalone)
    return credits[0]


_FTV_SUBSITE_V1_BASIS = 'actual_public_subsite_canonical_same_article_id_headline_and_own_newsroom_v1'
_FTV_SUBSITE_V5_BASIS = 'actual_public_subsite_canonical_same_article_id_scoped_headline_and_own_newsroom_v5'


def _ftv_subsite_headline(main, *, scoped=False):
    headings = main.select('h1')
    if len(headings) == 1:
        return headings[0], None  # Preserve the complete v1-v4 single-header proof.
    if not scoped:
        raise NewsError('article_identity_not_verified')
    article = _one(main, 'article')
    content = _one(article, '#newscontent')
    if content.name != 'div':
        raise NewsError('article_identity_not_verified')
    _visible(content)
    body_headings = {id(node) for node in content.find_all('h1')}
    candidates = [node for node in headings if id(node) not in body_headings]
    if len(candidates) != 1 or not any(parent is article for parent in candidates[0].parents):
        raise NewsError('article_identity_not_verified')
    # All other H1s must actually belong to this unique native body. Never
    # select the first match, borrow a related headline, or drop body headings.
    return candidates[0], {'contract_version': 5, 'native_body_id': 'newscontent',
        'total_h1_count': len(headings), 'body_h1_count': len(body_headings),
        'basis': 'single_current_article_h1_outside_unique_native_newscontent'}


def ftv_declared_public_canonical(payload, source, *, page_url):
    """Resolve an actual allowed public subsite response, never a refused path.

    The destination is read from this exact page, not generated from an ID.
    No subsite clock is corrected or accepted here. The canonical page must
    independently verify the original article, byline, body and source clock.
    """
    parts = urlsplit(page_url)
    if parts.netloc == "www.ftvnews.com.tw":
        return None
    if (source.provider != "ftvnews" or source.embedded_article_layout not in {"ftv_visible_article_v1", "ftv_visible_article_v2", "ftv_visible_article_v3", "ftv_visible_article_v4", "ftv_visible_article_v5", "ftv_visible_article_v6", "ftv_visible_article_v7"}
            or _id(page_url, "ftvnews") is None):
        raise NewsError("article_identity_not_verified")
    soup = public_article_document(payload, source)
    return _ftv_subsite_canonical_proof(soup, page_url=page_url,
        scoped=source.embedded_article_layout in {'ftv_visible_article_v5', 'ftv_visible_article_v6', 'ftv_visible_article_v7'})


def _ftv_subsite_canonical_proof(soup, *, page_url, scoped=False):
    """Same native identity/byline/clock proof for capture and retained evidence.

    HTTP access/authorization is decided by the normal public-response gate,
    not by independently checking the retained receipt's DOM here.
    """
    if _id(page_url, 'ftvnews') is None or urlsplit(page_url).netloc == 'www.ftvnews.com.tw':
        raise NewsError('article_identity_not_verified')
    main = _one(soup, "#__nuxt")
    headline_node, selection = _ftv_subsite_headline(main, scoped=scoped)
    _visible(headline_node)
    headline = headline_node.get_text(" ", strip=True)
    schemas = _identity_and_schemas(soup, page_url, "ftvnews", headline)
    canonical = _one(soup, 'link[rel~="canonical"][href]').get("href")
    if urlsplit(canonical).netloc != "www.ftvnews.com.tw":
        raise NewsError("article_identity_not_verified")
    # The current visible article is distinct from related-story cards. Never
    # follow a canonical to turn syndicated text into publisher-owned work.
    body = _one(main, "article")
    _visible(body)
    credit = _ftv_newsroom_credit(body)
    authors = _schema_authors(schemas)
    if authors not in (["民視財經網"], ["民視運動網"], ["民視健康長照網"]):
        raise NewsError("article_attribution_not_verified")
    raw_clock = _one(soup, 'meta[property="article:published_time"][content]',
                     "invalid_bulk_publication_clock").get("content")
    if any(schema.get("datePublished") != raw_clock for schema in schemas):
        raise NewsError("invalid_bulk_publication_clock")
    proof = {"declared_canonical_url": canonical, "subsite_url": page_url, "headline": headline,
            "visible_newsroom_credit_raw": credit, "schema_authors_raw": authors,
            "subsite_published_at_raw": raw_clock,
            "clock_not_accepted_or_rewritten": True,
            "basis": _FTV_SUBSITE_V1_BASIS}
    if selection:
        proof.update(basis=_FTV_SUBSITE_V5_BASIS, headline_selection=selection)
    return proof


def verify_resolution_response(record):
    """Backwards-compatible integrity of the separately retained permalink proof."""
    fields = ("url_resolution_response_body", "url_resolution_response_sha256", "url_resolution_evidence_json")
    if not any(record.get(key) is not None for key in fields):
        return 0
    raw = record.get("url_resolution_response_body")
    if not isinstance(raw, bytes) or hashlib.sha256(raw).hexdigest() != record.get("url_resolution_response_sha256"):
        raise ValueError("canonical_resolution_response_digest_mismatch")
    evidence = json.loads(record["url_resolution_evidence_json"])
    if not isinstance(evidence, dict):
        raise ValueError("canonical_resolution_evidence_not_an_object")
    identity = _id(evidence.get("subsite_url"), "ftvnews")
    if (record.get("provider") != "ftvnews" or identity is None
            or identity != _id(record.get("requested_url"), "ftvnews")
            or identity != _id(evidence.get("declared_canonical_url"), "ftvnews")
            or identity != _id(record.get("final_url"), "ftvnews")
            or normalize_url(record["final_url"]) != normalize_url(evidence["declared_canonical_url"])
            or urlsplit(record["final_url"]).netloc != "www.ftvnews.com.tw"
            or evidence.get("basis") != record.get("url_resolution_basis")
            or evidence.get("basis") not in {_FTV_SUBSITE_V1_BASIS, _FTV_SUBSITE_V5_BASIS}
            or evidence.get("clock_not_accepted_or_rewritten") is not True
            or evidence.get("canonical_published_at_raw") != record.get("published_at_raw")
            or evidence.get("declared_clock_conflict") is not (evidence.get("subsite_published_at_raw") != record.get("published_at_raw"))
            or record.get("http_total_response_bytes") != len(raw) + record["http_response_bytes"]):
        raise ValueError("canonical_resolution_source_identity_or_byte_count_mismatch")
    if evidence['basis'] == _FTV_SUBSITE_V5_BASIS:
        from bs4 import BeautifulSoup
        try:
            observed = _ftv_subsite_canonical_proof(BeautifulSoup(raw, 'lxml'),
                page_url=evidence['subsite_url'], scoped=True)
        except (NewsError, ValueError, TypeError, RecursionError):
            raise ValueError('canonical_resolution_native_header_not_verified') from None
        actual = {key: evidence.get(key) for key in observed}
        if (observed['basis'] != _FTV_SUBSITE_V5_BASIS
                or json.dumps(actual, sort_keys=True) != json.dumps(observed, sort_keys=True)):
            raise ValueError('canonical_resolution_native_header_evidence_mismatch')
    return len(raw)


def _ftv_reject_explicit_external_authors(names):
    # A publisher suffix describes the hosting page, not original authorship.
    # The actual canonical author can explicitly say 外稿｜引新聞 even when
    # its subsite labels the prose 財經中心. Never let that generic newsroom
    # line override declared syndicated provenance. Do not rewrite names or
    # infer a different reporter from the source label.
    if any(re.match(r'^(?:外稿|轉載)\s*[｜|／/：:]\s*\S', name) for name in names):
        raise NewsError('article_rights_or_scope_excluded')


def ftv_visible_article(soup, *, page_url, paired_quotes=False, quote_edges=False, literal_tabs=False,
                        strict_author_provenance=False, standalone_credits=False):
    """Own-newsroom text only, not FTV-hosted CTWANT/CNEWS syndication."""
    scope = _one(soup, ".col-article")
    headline_node = _one(scope, ".scroll-header h1")
    body = _one(scope, "#contentarea[itemprop=articleBody]", "body_not_found")
    for node in (headline_node, body):
        _visible(node)
    headline = headline_node.get_text(" ", strip=True)
    syntax_notices = []
    schemas = _identity_and_schemas(soup, page_url, "ftvnews", headline,
        ftv_paired_quotes=paired_quotes, ftv_quote_edges=quote_edges,
        ftv_literal_tabs=literal_tabs, syntax_notices=syntax_notices)
    title_notices = [_ftv_paired_quote_title(soup, page_url, headline, schema.get('headline'),
        quote_edges=quote_edges)
        for schema in schemas if _key(schema.get('headline')) != _key(headline)]
    if strict_author_provenance:
        names = _schema_authors(schemas)
        _ftv_reject_explicit_external_authors(names)
    credit_notices = []
    credit = _ftv_newsroom_credit(body, standalone_credits=standalone_credits, syntax_notices=credit_notices)
    if not strict_author_provenance:
        names = _schema_authors(schemas)
    if any(not re.fullmatch(r".+ - 民視新聞網", name) for name in names):
        raise NewsError("article_rights_or_scope_excluded")
    if credit_notices:
        for name in names:
            reporter = re.fullmatch(r'(.+?)(?:（[^（）]+）)? - 民視新聞網', name)
            if not reporter or not re.search(r'[／/ \t　]' + re.escape(reporter[1]) + r'(?=[ \t　]|報導)', credit):
                raise NewsError('article_attribution_not_verified')
        for notice in credit_notices:
            notice['schema_authors_raw'] = names
            notice['declared_reporter_names_matched'] = True
    stamps = [n.get_text(" ", strip=True) for n in scope.select(":scope > ul.news-status > li")
              if n.get_text(" ", strip=True).startswith("發佈時間：")]
    if len(stamps) != 1:
        raise NewsError("invalid_bulk_publication_clock")
    stamp = re.fullmatch(r"發佈時間：([0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2})\s+更新時間：.*", stamps[0])
    if not stamp:
        raise NewsError("invalid_bulk_publication_clock")
    published = _clock(soup, schemas, stamp[1], "%Y/%m/%d %H:%M")
    # The real source inserts unrelated recommendations directly inside the
    # article. Remove only its verified labelled article links; bold financial
    # headings, original paragraphs and the publisher's risk notice remain.
    for node in body.find_all("strong", recursive=False):
        if (node.get_text(" ", strip=True).startswith("更多新聞：")
                and node.find("a", href=True)
                and all(_id(urljoin(page_url, a["href"]), "ftvnews") for a in node.select("a[href]"))):
            node.decompose()
    article = {
        "headline": headline, "author": [{"name": name} for name in names],
        "datePublished": published, "dateModified": schemas[0].get("dateModified"),
        "published_time_basis": "matched_visible_minutes_and_declared_clock; source_seconds_and_offset_preserved",
        "article_identity_basis": "ftv_current_news_id_canonical_og_schema_and_visible_newsroom_v1",
        "body_completeness": "verified_current_article_container",
        "author_attribution_detail_json": json.dumps({"visible_newsroom_credit_raw": credit,
            "schema_authors_raw": names, "byline_basis": "declared_current_article_author_and_visible_own_newsroom"},
            ensure_ascii=False, sort_keys=True),
    }
    if title_notices:
        article['verified_article_notices_json'] = json.dumps(title_notices, ensure_ascii=False, sort_keys=True)
        version = max(notice['contract_version'] for notice in title_notices)
        article['article_identity_basis'] = f'ftv_current_news_id_canonical_og_schema_visible_newsroom_and_paired_quotes_v{version}'
    if syntax_notices:
        article['verified_article_notices_json'] = json.dumps(title_notices + syntax_notices,
            ensure_ascii=False, sort_keys=True)
        article['article_identity_basis'] = 'ftv_current_news_id_canonical_og_schema_visible_newsroom_and_lossless_json_tabs_v4'
    if credit_notices:
        article['verified_article_notices_json'] = json.dumps(title_notices + syntax_notices + credit_notices,
            ensure_ascii=False, sort_keys=True)
        article['article_identity_basis'] = 'ftv_current_news_id_canonical_og_schema_and_standalone_visible_reporter_v7'
    return article, scope

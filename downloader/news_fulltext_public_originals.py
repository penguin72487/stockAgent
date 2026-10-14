"""Visible NOWnews / FTV / reviewed VOA originals in the private collector.

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


def _identity_and_schemas(soup, page_url, provider, headline, *, amp=False):
    identity = _id(page_url, provider)
    canonical = _one(soup, 'link[rel~="canonical"][href]')
    if identity is None or _id(canonical.get("href"), provider) != identity:
        raise NewsError("article_identity_not_verified")
    og = soup.select('meta[property="og:url"][content]')
    if ((not amp or og) and (len(og) != 1 or _id(og[0].get("content"), provider) != identity)):
        raise NewsError("article_identity_not_verified")
    matches = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            candidates = _objects(json.loads(script.get_text()))
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
                    raise NewsError("article_identity_not_verified")
                matches.append(candidate)
        except (json.JSONDecodeError, TypeError):
            continue
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


def _ftv_newsroom_credit(body):
    """Credit lines are not whole paragraphs; preserve actual source breaks."""
    credits = []
    for node in body.find_all("p"):
        if node.find("p") or node.find_parent(["figure", "figcaption", "nav", "aside"]):
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
    credits = list(dict.fromkeys(credits))
    if len(credits) != 1:
        raise NewsError("article_attribution_not_verified")
    return credits[0]


def ftv_declared_public_canonical(payload, source, *, page_url):
    """Resolve an actual allowed public subsite response, never a refused path.

    The destination is read from this exact page, not generated from an ID.
    No subsite clock is corrected or accepted here. The canonical page must
    independently verify the original article, byline, body and source clock.
    """
    parts = urlsplit(page_url)
    if parts.netloc == "www.ftvnews.com.tw":
        return None
    if (source.provider != "ftvnews" or source.embedded_article_layout != "ftv_visible_article_v1"
            or _id(page_url, "ftvnews") is None):
        raise NewsError("article_identity_not_verified")
    soup = public_article_document(payload, source)
    main = _one(soup, "#__nuxt")
    headline_node = _one(main, "h1")
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
    return {"declared_canonical_url": canonical, "subsite_url": page_url, "headline": headline,
            "visible_newsroom_credit_raw": credit, "schema_authors_raw": authors,
            "subsite_published_at_raw": raw_clock,
            "clock_not_accepted_or_rewritten": True,
            "basis": "actual_public_subsite_canonical_same_article_id_headline_and_own_newsroom_v1"}


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
            or evidence.get("basis") != "actual_public_subsite_canonical_same_article_id_headline_and_own_newsroom_v1"
            or evidence.get("clock_not_accepted_or_rewritten") is not True
            or evidence.get("canonical_published_at_raw") != record.get("published_at_raw")
            or evidence.get("declared_clock_conflict") is not (evidence.get("subsite_published_at_raw") != record.get("published_at_raw"))
            or record.get("http_total_response_bytes") != len(raw) + record["http_response_bytes"]):
        raise ValueError("canonical_resolution_source_identity_or_byte_count_mismatch")
    return len(raw)


def ftv_visible_article(soup, *, page_url):
    """Own-newsroom text only, not FTV-hosted CTWANT/CNEWS syndication."""
    scope = _one(soup, ".col-article")
    headline_node = _one(scope, ".scroll-header h1")
    body = _one(scope, "#contentarea[itemprop=articleBody]", "body_not_found")
    for node in (headline_node, body):
        _visible(node)
    headline = headline_node.get_text(" ", strip=True)
    schemas = _identity_and_schemas(soup, page_url, "ftvnews", headline)
    credit = _ftv_newsroom_credit(body)
    names = _schema_authors(schemas)
    if any(not re.fullmatch(r".+ - 民視新聞網", name) for name in names):
        raise NewsError("article_rights_or_scope_excluded")
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
    return {
        "headline": headline, "author": [{"name": name} for name in names],
        "datePublished": published, "dateModified": schemas[0].get("dateModified"),
        "published_time_basis": "matched_visible_minutes_and_declared_clock; source_seconds_and_offset_preserved",
        "article_identity_basis": "ftv_current_news_id_canonical_og_schema_and_visible_newsroom_v1",
        "body_completeness": "verified_current_article_container",
        "author_attribution_detail_json": json.dumps({"visible_newsroom_credit_raw": credit,
            "schema_authors_raw": names, "byline_basis": "declared_current_article_author_and_visible_own_newsroom"},
            ensure_ascii=False, sort_keys=True),
    }, scope

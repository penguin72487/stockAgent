"""Verified public WordPress posts: up to 100 full bodies per source request.

Only explicitly registered/authorized endpoints use this adapter. A summary,
private post, missing byline or stated rights override is not a complete body.
Raw JSON is shared within an acceptance batch, not copied once per article.
"""

from __future__ import annotations

import hashlib
import html
import json
import math
from datetime import datetime, timezone
import re
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

from downloader.news_fulltext_policy import CONTRACT_VERSION, NewsError, _plain_text, capture_usage, normalize_url
from downloader.news_fulltext_queue import utc_now


BULK_ENVELOPE_CONTINUATION_CONTRACT_VERSION = 2
BULK_RESTRICTED_ITEM_ERRORS = frozenset({
    'article_rights_override', 'bulk_post_not_public',
    'bulk_foreign_article_url', 'paywall_or_login',
})


def cnyes_url(base: str, page: int = 1, *, start: int | None = None, end: int | None = None) -> str:
    parts = urlsplit(normalize_url(base))
    if parts.query or not 1 <= page <= 30 or (start is None) != (end is None) or start is not None and end < start:
        raise ValueError("invalid bounded Cnyes page")
    query = {"limit": 30, "page": page}  # Requested page size; source may append identical join duplicates.
    if start is not None:
        query.update(startAt=int(start), endAt=int(end))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))


def cnyes_scope(url: str) -> str:
    parts = urlsplit(url)
    query = parse_qs(parts.query)
    query.pop("page", None)
    return urlunsplit((parts.scheme, parts.netloc, parts.path,
                       urlencode(sorted((k, v[0]) for k, v in query.items())), ""))


def cnyes_history_priority(url: str) -> int:
    """Latest first; broad financial categories before overlapping leaf views.

    No category is excluded. Membership overlap is not a history completeness
    proof, so complements keep their own jobs and source evidence.
    """
    query = parse_qs(urlsplit(url).query)
    date_priority = -int(query["endAt"][0]) // 86400 if "endAt" in query else -1_000_000
    broad = ("headline", "tw_stock", "wd_stock", "us_stock", "cn_stock", "hk_stock", "forex", "future", "fund", "bc")
    category = urlsplit(url).path.rsplit("/", 1)[-1]
    return date_priority * 100 + (broad.index(category) if category in broad else 50)


def cnyes_unseeded_intervals(low: int, high: int, covered, *, max_days: int = 730) -> list[tuple[int, int]]:
    """Largest contiguous unplanned bounds, reusing old jobs without dropping gaps.

    The publisher explicitly rejects date spans longer than two years. We
    bound integral-day roots to 730 days, then split dense totals recursively.
    `covered` means already scheduled, NOT downloaded or complete. Error and
    superseded checkpoints remain owned by the canonical repair/child queue.
    """
    if type(max_days) is not int or not 1 <= max_days <= 730:
        raise ValueError("Cnyes date windows must respect the observed two-year bound")
    cursor, remaining = low, []
    for start, end in sorted((max(low, a), min(high, b)) for a, b in covered if b >= low and a <= high):
        if start > cursor:
            remaining.append((cursor, start - 1))
        cursor = max(cursor, end + 1)
    if cursor <= high:
        remaining.append((cursor, high))
    maximum = max_days * 86400
    return [(start, min(start + maximum - 1, end)) for low, end in remaining for start in range(low, end + 1, maximum)]


def cnyes_window_partitions(url: str, pages: int) -> list[str]:
    """Respect the measured page <= 30 rule without assuming uniform density.

    Start with the minimum *estimated* number of day windows. Each child's
    actual total is checked again and dense children subdivide recursively.
    Exact inclusive request bounds are covered without holes; the publisher
    can expand to day boundaries, so cross-window article IDs are deduplicated.
    A single publication day cannot safely be split into hours without proof
    that the endpoint respects intraday bounds; that overflow remains a gap.
    """
    query = parse_qs(urlsplit(url).query)
    if pages <= 30 or "startAt" not in query:
        return []
    low, high = int(query["startAt"][0]), int(query["endAt"][0])
    days = math.ceil((high - low + 1) / 86400)
    if days <= 1:
        return []
    pieces = min(days, math.ceil(pages / 30))
    boundaries = [low + (i * days // pieces) * 86400 for i in range(pieces)] + [high + 1]
    return [cnyes_url(wordpress_base(url), start=a, end=b - 1) for a, b in zip(boundaries, boundaries[1:])]


def cnyes_pagination(response) -> tuple[dict, list[dict]]:
    """Reject wrong/repeated/truncated pages before accepting their text."""
    if "application/json" not in response.headers.get("content-type", ""):
        raise NewsError("bulk_api_not_json")
    try:
        payload = json.loads(response.body)
        items = payload["items"]
        query = parse_qs(urlsplit(response.url).query)
        if payload.get("statusCode") != 200 or not isinstance(items, dict):
            raise ValueError("invalid publisher result")
        total, size, page, pages = (items[k] for k in ("total", "per_page", "current_page", "last_page"))
        if any(type(v) is not int for v in (total, size, page, pages)):
            raise ValueError("noninteger pagination")
        requested_page = int(query.get("page", ["1"])[0])
        # A live publisher empty window reports last_page=0, page=1, data=[].
        # Accept only this exact empty form (or last_page=1), never a short
        # nonempty page. Retain the reported count instead of retrying absence.
        empty_zero_pages = total == 0 and page == 1 and pages == 0 and items.get("data") == []
        if (not 0 <= total <= 10_000_000 or not 1 <= size <= 30 or not 1 <= page <= 30 or page != requested_page
                or not empty_zero_pages and (pages != max(1, math.ceil(total / size)) or page > pages)):
            raise ValueError("inconsistent pagination")
        posts = items["data"]
        expected = min(size, max(0, total - (page - 1) * size))
        if (not isinstance(posts, list) or total == 0 and posts
                or not expected <= len(posts) <= max(expected, 2 * size)
                or any(not isinstance(p, dict) for p in posts)):
            raise ValueError("truncated page")
        ids = [p["newsId"] for p in posts]
        if any(type(i) is not int or i <= 0 for i in ids):
            raise ValueError("invalid article IDs")
        unique = {}
        for post in posts:
            prior = unique.setdefault(post["newsId"], post)
            if (prior is not post and json.dumps(prior, sort_keys=True, allow_nan=False)
                    != json.dumps(post, sort_keys=True, allow_nan=False)):
                # Python equates False with 0 and 1.0 with 1. Source JSON
                # types are part of the permission/clock proof, so do not
                # deduplicate an unproven flag or epoch as an identical row.
                raise ValueError("conflicting duplicate article ID")
        duplicates = len(posts) - len(unique)
        if len(unique) > expected:
            raise ValueError('unexplained excess unique article IDs')
        # Live 2022 pages declare from=571,to=602,per_page=30 and return
        # 32 rows with four byte-equivalent objects for one ID. Their join
        # duplication is not a truncated body or permission to discard a
        # whole page. Require the publisher's exact row span, bounded excess
        # and identical objects; retain the anomaly and deduplicate acceptance.
        # Conflicts, missing rows, ignored dates and unexplained excess still
        # fail closed. Source totals are not a unique-history denominator.
        if duplicates or len(posts) != expected:
            first, last = items.get("from"), items.get("to")
            if (not duplicates or type(first) is not int or type(last) is not int
                    or first != (page - 1) * size + 1 or last != first + len(posts) - 1):
                raise ValueError("unverified duplicate row span")
        # The endpoint normalizes its boundaries to publication days. Keep
        # exact request bounds and source epochs; allow one boundary day only,
        # never silently accept an ignored historical filter returning today.
        if "startAt" in query:
            low, high = int(query["startAt"][0]) - 86400, int(query["endAt"][0]) + 86400
            if any(type(p.get("publishAt")) is not int or not low <= p["publishAt"] <= high for p in posts):
                raise ValueError("ignored historical filter")
        return {"total": total, "pages": max(1, pages), "publisher_last_page_raw": pages, "page": page, "scope": cnyes_scope(response.url),
                "returned_rows": len(posts), "unique_rows": len(unique), "duplicate_rows": duplicates,
                "ids_sha256": hashlib.sha256(json.dumps(sorted(unique)).encode()).hexdigest()}, posts
    except (ValueError, TypeError, KeyError) as error:
        raise NewsError("bulk_pagination_totals_invalid", retry=True, delay=60) from error


def cnyes_records(response, http, source) -> tuple[list[dict], list[dict]]:
    if wordpress_base(response.url) not in source.cnyes_apis:
        raise NewsError("unregistered_bulk_api")
    page_info, posts = cnyes_pagination(response)
    records, errors = [], []
    seen = set()
    captured, digest = utc_now(), hashlib.sha256(response.body).hexdigest()
    for number, post in enumerate(posts):
        url = f"https://news.cnyes.com/news/id/{post['newsId']}"
        if post["newsId"] in seen:
            errors.append({"item_number": number, "url": url, "error": "bulk_exact_duplicate_row"})
            continue
        seen.add(post["newsId"])
        title, published = "", ""
        try:
            if http.registry.require(url).provider != source.provider:
                raise NewsError("bulk_foreign_article_url")
            if type(post.get("payment")) is not int or post["payment"] != 0:
                raise NewsError("bulk_post_not_public")
            if type(post.get("publishAt")) is not int:
                raise NewsError("invalid_bulk_post_fields")
            if not isinstance(post.get("title"), str):
                raise NewsError("invalid_bulk_post_fields")
            title = _plain_text(post["title"], parser=source.html_parser)
            published = datetime.fromtimestamp(post["publishAt"], timezone.utc).isoformat()
            if not title:
                raise NewsError("invalid_bulk_post_fields")
            fragment = post.get("content")
            if fragment is None:
                raise NewsError("bulk_body_not_provided")  # Valid public metadata; use the canonical article HTML.
            if not isinstance(fragment, str):
                raise NewsError("invalid_bulk_post_fields")
            encoded_html = "<" not in fragment and bool(re.match(r"\s*&lt;[a-zA-Z][^;]*?(?:&gt;|[\s&])", fragment))
            body = _plain_text(html.unescape(fragment) if encoded_html else fragment, parser=source.html_parser)
            if not body or body.lower() == 'null':
                # Actual announcement pages contain encoded <p>null</p>.
                # This is a missing API body with valid metadata, not a whole
                # category parser outage or a four-character full article.
                raise NewsError('bulk_body_not_provided')
            # Completeness comes from the public content field, not its
            # character count. Actual short flash reports have 59--89 chars.
            # Never replace missing content with summary/title or accept a
            # headline echoed as the entire body.
            if not body or len(body) < 100 and (body == title or not re.search(r"[\u3400-\u9fff]", body) and len(body.split()) < 8):
                raise NewsError("body_not_found")
            if source.require_author_attribution:
                raise NewsError("article_attribution_not_verified")  # API does not expose a verified byline.
            key = hashlib.sha256(url.encode()).hexdigest()
            encoded_body = body.encode()
            records.append({**capture_usage(source), "article_id": key, "title": title, "body": body,
                "body_sha256": hashlib.sha256(encoded_body).hexdigest(), "authors_json": "[]",
                "attribution_status": "source_byline_not_found", "publisher_source_raw": str(post.get("source") or ""),
                "publisher_columnists_json": json.dumps(post.get("columnists"), ensure_ascii=False),
                "published_at_raw": published, "published_at_epoch_raw": post["publishAt"], "modified_at_raw": None,
                "published_time_basis": "publisher_API_publishAt_epoch_UTC; original_integer_preserved",
                "declared_canonical_url": url, "declared_license_urls_json": "[]",
                "extraction_method": "cnyes_api_content_v1", "body_completeness": "publisher_full_content_field",
                "source_page_returned_rows": page_info["returned_rows"],
                "source_page_duplicate_rows": page_info["duplicate_rows"],
                "source_page_total_raw": page_info["total"], "short_publisher_content": len(body) < 100,
                "content_encoding": "entity_encoded_html" if encoded_html else "publisher_html_or_text",
                "historical_point_in_time": False, "requested_url": url, "final_url": url,
                "source_request_url": response.url, "provider": source.provider, "metadata_date_raw": published,
                "captured_at_utc": captured, "body_bytes": len(encoded_body), "response_sha256": digest,
                "http_response_bytes": len(response.body), "raw_response_body": None, "source_response_article_id": None,
                "duration_seconds": None, "etag": None, "last_modified": None,
                "authorization_fingerprint": source.authorization_fingerprint,
                "authorization_json": json.dumps(source.authorization, ensure_ascii=False, sort_keys=True),
                "data_fingerprint": source.data_fingerprint, "bulk_permission_basis": source.bulk_permission_basis,
                "contract_version": CONTRACT_VERSION, "publish": False, "remote_training_export": False})
        except (NewsError, KeyError, TypeError, OverflowError, ValueError) as error:
            errors.append({"item_number": number, "url": url,
                           "error": error.code if isinstance(error, NewsError) else "invalid_bulk_post_fields",
                           "title": title, "metadata_date_raw": published})
    return records, errors


def wordpress_url(base: str, page: int = 1) -> str:
    parts = urlsplit(normalize_url(base))
    if parts.query or page < 1:
        raise ValueError("WordPress base must not contain query; page must be positive")
    query = urlencode({"per_page": 100, "page": page, "_embed": "author", "orderby": "date", "order": "desc"})
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, ""))


def wordpress_base(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def wordpress_page(url: str) -> int:
    return int(parse_qs(urlsplit(url).query).get("page", ["1"])[0])


def wordpress_records(response, http, source) -> tuple[list[dict], list[dict]]:
    if wordpress_base(response.url) not in source.wordpress_apis:
        raise NewsError("unregistered_bulk_api")
    if "application/json" not in response.headers.get("content-type", ""):
        raise NewsError("bulk_api_not_json")
    try:
        posts = json.loads(response.body)
    except ValueError as error:
        raise NewsError("invalid_bulk_json") from error
    if not isinstance(posts, list) or len(posts) > 100:
        raise NewsError("invalid_bulk_posts")
    records, errors = [], []
    captured = utc_now()
    response_sha = hashlib.sha256(response.body).hexdigest()
    for item_number, post in enumerate(posts):
        url = None
        try:
            if not isinstance(post, dict) or post.get("status") != "publish" or post.get("type") != "post":
                raise NewsError("bulk_post_not_public")
            url = normalize_url(post["link"])
            if http.registry.require(url).provider != source.provider:
                raise NewsError("bulk_foreign_article_url")
            content = post["content"]
            if content.get("protected"):
                raise NewsError("paywall_or_login")
            body = _plain_text(content["rendered"], source.exclude_selectors)
            title = _plain_text(post["title"]["rendered"])
            embedded_authors = post.get("_embedded", {}).get("author", [])
            if isinstance(embedded_authors, dict):
                embedded_authors = [embedded_authors]
            authors = [a["name"].strip() for a in embedded_authors if isinstance(a, dict)
                       and isinstance(a.get("name"), str) and a["name"].strip()]
            if len(body) < 100 or not title:
                raise NewsError("body_not_found")
            if source.require_author_attribution and not authors:
                raise NewsError("article_attribution_not_verified")
            rights = post.get("meta", {}).get("license") if isinstance(post.get("meta"), dict) else None
            if rights and str(rights).lower() not in {"cc-by-3.0", "https://creativecommons.org/licenses/by/3.0/"}:
                raise NewsError("article_rights_override")
            if "all rights reserved" in body.lower():
                raise NewsError("article_rights_override")
            key = hashlib.sha256(url.encode()).hexdigest()
            records.append({
                **capture_usage(source),
                "title": title, "body": body, "body_sha256": hashlib.sha256(body.encode()).hexdigest(),
                "authors_json": json.dumps(authors, ensure_ascii=False),
                "attribution_status": "source_byline_present" if authors else "source_byline_not_found",
                "published_at_raw": post.get("date_gmt"), "modified_at_raw": post.get("modified_gmt"),
                "published_time_basis": "publisher_API_date_gmt_field_UTC; original_string_preserved",
                "declared_canonical_url": post["link"], "declared_license_urls_json": "[]",
                "license_verification_basis": "publisher_default_license_api; explicit_overrides_quarantined",
                "per_article_license_verified": False,
                "extraction_method": "wordpress_api_content_rendered", "body_completeness": "publisher_full_content_field",
                "historical_point_in_time": False, "article_id": key, "requested_url": url, "final_url": url,
                "source_request_url": response.url, "provider": source.provider,
                "metadata_date_raw": post.get("date_gmt"), "captured_at_utc": captured,
                "body_bytes": len(body.encode()), "response_sha256": response_sha,
                "http_response_bytes": len(response.body), "raw_response_body": None,
                "source_response_article_id": None, "duration_seconds": None,
                "etag": None, "last_modified": None,
                "authorization_fingerprint": source.authorization_fingerprint,
                "authorization_json": json.dumps(source.authorization, ensure_ascii=False, sort_keys=True),
                "data_fingerprint": source.data_fingerprint,
                "bulk_permission_basis": source.bulk_permission_basis,
                "contract_version": CONTRACT_VERSION, "publish": False, "remote_training_export": False,
            })
        except (NewsError, KeyError, TypeError, AttributeError) as error:
            errors.append({"item_number": item_number, "url": url,
                           "error": str(error) if isinstance(error, NewsError) else "invalid_bulk_post_fields"})
    # The acceptance owner may filter already stored articles. It assigns the
    # shared raw envelope to the first genuinely accepted record, not row one.
    return records, errors

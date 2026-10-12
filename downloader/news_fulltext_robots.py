"""Robots representation handling, distinct from source access authorization.

RFC 9309 section 2.3.1.5 requires using parseable lines, not treating every
malformed representation as an explicit prohibition. We retain valid rules,
including crawl delays, and still reject actual access/login challenges. An
empty HTML redirect can represent a missing policy only at a registered,
ordinary publisher home with its own public article links; no arbitrary HTML
error page, alternate identity or guessed endpoint becomes permission.
"""

from __future__ import annotations

import hashlib
import math
import re
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup

from downloader.news_fulltext_policy import NewsError, SourcePolicy, normalize_url
from downloader.news_fulltext_metrics import ROBOTS_ORIGIN_POLICY_CONTRACT_VERSION


REP_LINE = re.compile(r"(?im)^[ \t]*(?:user-agent|allow|disallow|crawl-delay|request-rate|sitemap)[ \t]*:[^\r\n]*")
AGENT_LINE = re.compile(r"(?im)^[ \t]*user-agent[ \t]*:")
REPRESENTATION_VERSION = 2
MAX_ROBOTS_ORIGINS_PER_PROVIDER = 32


def policy_origin(url: str) -> str:
    """REP is scoped to scheme/authority, not a publisher's shared quota."""
    parts = urlsplit(normalize_url(url))
    return f"{parts.scheme}://{parts.netloc}"


def robots_policy_retry(cache, origin, now):
    """Validate negative state separately from a previous successful policy.

    An expired retry permits a real policy probe, never reuse of old Allow
    rules. Parser/config changes cannot clear an intact origin deadline.
    """
    if not isinstance(cache, dict) or cache.get("origin") != origin:
        return None
    row = cache.get("policy_retry")
    if not isinstance(row, dict):
        return None
    expected = {"contract_version", "observed_at_epoch", "not_before_epoch", "delay_seconds", "generation",
                "error_code", "source_data_fingerprint", "authorization_fingerprint"}
    finite = lambda value: type(value) in (int, float) and math.isfinite(value) and value >= 0
    if (set(row) != expected or type(row["contract_version"]) is not int
            or row["contract_version"] != ROBOTS_ORIGIN_POLICY_CONTRACT_VERSION
            or not all(finite(row[k]) for k in ("observed_at_epoch", "not_before_epoch", "delay_seconds"))
            or not 30 <= row["delay_seconds"] <= 300 or row["observed_at_epoch"] > now + 5
            or abs(row["not_before_epoch"] - row["observed_at_epoch"] - row["delay_seconds"]) > 1e-5
            or type(row["generation"]) is not int or not 1 <= row["generation"] <= 5
            or not isinstance(row["error_code"], str) or not re.fullmatch(r"[a-z0-9_]{1,96}", row["error_code"])
            or any(not isinstance(row[k], str) or not re.fullmatch(r"[0-9a-f]{64}", row[k])
                   for k in ("source_data_fingerprint", "authorization_fingerprint"))):
        return None  # No permission: the caller still requires a real REP fetch.
    return dict(row)


def next_robots_policy_retry(source, error, previous, now):
    generation = min(5, (previous["generation"] if previous else 0) + 1)
    delay = min(300, max(30 * (1 << (generation - 1)), error.delay))
    return {"contract_version": ROBOTS_ORIGIN_POLICY_CONTRACT_VERSION, "observed_at_epoch": now,
        "not_before_epoch": now + delay, "delay_seconds": delay, "generation": generation,
        "error_code": error.code, "source_data_fingerprint": source.data_fingerprint,
        "authorization_fingerprint": source.authorization_fingerprint}


def robots_representation(response, source: SourcePolicy, origin: str) -> dict:
    # A source's explicit mitigation signal remains a challenge even when its
    # response happens to be plain text or contains otherwise valid REP lines.
    if response.headers.get("cf-mitigated", "").lower() == "challenge":
        raise NewsError("robots_access_challenge", retry=True, delay=900)
    text = response.body.decode("utf-8-sig", "replace")
    result = {
        "representation_version": REPRESENTATION_VERSION,
        "fetched_url": response.url, "content_type": response.headers.get("content-type", ""),
        "raw_response_sha256": hashlib.sha256(response.body).hexdigest(),
        "raw_response_bytes": len(response.body), "representation_kind": "plain_utf8", "body": text,
    }
    if not text.lstrip().startswith("<"):
        return result
    soup = BeautifulSoup(response.body, "xml" if text.lstrip().lower().startswith("<?xml") else "lxml")
    title = soup.title.get_text(" ", strip=True).lower() if soup.title else ""
    if (any(t in title for t in ("just a moment", "access denied", "captcha", "verify you are human"))
            or soup.select_one('#challenge-form, #cf-challenge-running, [data-sitekey]')):
        raise NewsError("robots_access_challenge", retry=True, delay=900)
    if soup.select_one('form[action*="login"], input[type="password"], meta[http-equiv="refresh"]'):
        raise NewsError("robots_not_plain_rules", retry=True, delay=300)
    # Keep rule order and every group. HTML wrappers are invalid lines, not a
    # reason to discard a valid Disallow. Never store the unrelated home text.
    lines = REP_LINE.findall(soup.get_text("\n"))
    rules = "\n".join(lines)
    if AGENT_LINE.search(rules):
        result.update(body=rules, representation_kind="html_valid_rule_lines")
        return result
    parts = urlsplit(response.url)
    original = urlsplit(origin)
    root = f"{parts.scheme}://{parts.netloc}/"
    # Follow the actual same-host HTTP -> HTTPS redirect, not a guessed URL.
    # Bind the resulting policy to the initial origin in the caller's cache.
    # Only an independently registered ordinary home may supply this narrow
    # no-REP representation; a host change or HTTPS downgrade cannot qualify.
    supported_origin = (parts.netloc == original.netloc
                        and (parts.scheme == original.scheme
                             or original.scheme == "http" and parts.scheme == "https"))
    if (not supported_origin or response.url != root or parts.query or root not in source.index_pages
            or not title or not source.article_path_pattern):
        raise NewsError("robots_not_plain_rules", retry=True, delay=300)
    canonical = soup.select_one('link[rel~="canonical"][href]')
    og_url = soup.select_one('meta[property="og:url"][content]')
    for node, attr in ((canonical, "href"), (og_url, "content")):
        if node is not None:
            try:
                identity = normalize_url(urljoin(root, node.get(attr, "")))
            except NewsError:
                raise NewsError("robots_not_plain_rules", retry=True, delay=300) from None
            if identity != root:
                raise NewsError("robots_not_plain_rules", retry=True, delay=300)
    pattern = re.compile(source.article_path_pattern)
    own_article_links = set()
    for node in soup.select('a[href]'):
        try:
            target = normalize_url(urljoin(root, node.get("href", "")))
            target_parts = urlsplit(target)
        except (NewsError, ValueError):
            continue
        if target_parts.netloc == parts.netloc and pattern.search(target_parts.path):
            own_article_links.add(target)
    if len(own_article_links) < 2 or any(marker in title for marker in ("error", "unavailable", "forbidden", "系統異常", "存取遭拒")):
        raise NewsError("robots_not_plain_rules", retry=True, delay=300)
    result.update(body=rules, representation_kind="registered_home_without_rep_groups",
                  home_scope_fingerprint=source.data_fingerprint,
                  initial_origin=origin, registered_home_url=root)
    return result

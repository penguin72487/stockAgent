"""Parse witnessed source clock syntax without assigning a timezone."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from urllib.parse import urlsplit


_COMBINED_NAIVE = re.compile(
    r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\s*\(更新[：:]\s*'
    r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\)'
)
_GLOBALVOICES_MONTHLY_FEED = re.compile(r'/([0-9]{4})/(0[1-9]|1[0-2])/feed/')
_GLOBALVOICES_MONTHLY_ARTICLE = re.compile(r'/([0-9]{4})/(0[1-9]|1[0-2])/[0-9]{2}/[^/]+/?')
_GLOBALVOICES_FEED_METHODS = frozenset(f'rss_content_encoded_v{version}' for version in (1, 2, 3))
GLOBALVOICES_RETAINED_META_BASIS = (
    'recovered_from_exact_retained_html_article_published_time; '
    'source_declaration_only_not_historical_availability'
)
CMNEWS_RETAINED_DECLARATION_BASIS = (
    'exact_retained_cmnews_NewsArticle_matches_visible_publication_and_update; '
    'source_declaration_only_not_historical_availability'
)
_CMNEWS_DECLARATION_BASIS = (
    'publisher_NewsArticle_matches_visible_minute; '
    'source_declared_clock_preserved_without_timezone_inference'
)


def legacy_globalvoices_basis_candidate(record):
    """Check the inexpensive identity and clock fields before reading HTML."""
    if (record.get('provider') != 'globalvoices'
            or record.get('extraction_method') != 'css:.post-container .entry'
            or record.get('published_time_basis') is not None):
        return False
    requested = record.get('requested_url')
    if (not isinstance(requested, str) or requested != record.get('final_url')
            or record.get('source_request_url') not in (None, requested)
            or record.get('source_response_article_id') not in (None, record.get('article_id'))):
        return False
    try:
        url = urlsplit(requested)
        if (url.scheme != 'https' or url.username is not None or url.password is not None
                or url.port not in (None, 443) or not url.hostname
                or not (url.hostname == 'globalvoices.org' or url.hostname.endswith('.globalvoices.org'))):
            return False
    except ValueError:
        return False
    expected, published = record.get('response_sha256'), record.get('published_at_raw')
    if (not isinstance(expected, str) or re.fullmatch(r'[0-9a-f]{64}', expected) is None
            or not isinstance(published, str)):
        return False
    try:
        clock = datetime.fromisoformat(published.replace('Z', '+00:00'))
        if clock.tzinfo is None or clock.utcoffset() is None:
            return False
    except (ValueError, OverflowError):
        return False
    return True


def legacy_globalvoices_publication_basis(record):
    """Recognize a clock declaration in the exact retained HTML response.

    The caller must first verify the immutable batch receipt and bind this row
    to the frozen head. This only describes a source declaration: it never
    changes the clock, availability, rights, body, or the original record.
    Incomplete or contradictory evidence is left unknown.
    """
    if not legacy_globalvoices_basis_candidate(record):
        return None
    raw, expected, published = (record.get('raw_response_body'), record.get('response_sha256'),
                                record.get('published_at_raw'))
    if not isinstance(raw, bytes) or not raw or hashlib.sha256(raw).hexdigest() != expected:
        return None
    # Parse only metadata nodes: the body and all author/rights fields remain
    # untouched. Count every matching tag, including conflicting duplicates.
    from bs4 import BeautifulSoup, SoupStrainer
    try:
        soup = BeautifulSoup(raw, 'lxml', parse_only=SoupStrainer('meta'))
        nodes = soup.find_all('meta', attrs={'property': 'article:published_time'})
    except (ValueError, TypeError, LookupError):
        return None
    if len(nodes) != 1 or nodes[0].get('content') != published:
        return None
    return GLOBALVOICES_RETAINED_META_BASIS


def combined_publication_update(raw: str):
    """Classify a literal publication/update pair; retain both as naive clocks.

    No match means this is another format. Invalid and reversed pairs carry an
    explicit status and no timestamps so callers cannot silently repair them.
    """
    match = _COMBINED_NAIVE.fullmatch(raw.strip())
    if match is None:
        return None, None
    try:
        published, updated = (datetime.fromisoformat(value) for value in match.groups())
    except ValueError:
        return 'combined_naive_invalid', None
    if updated < published:
        return 'combined_naive_update_before_publication', None
    return 'combined_naive_publication_and_update', (published, updated)


def legacy_cmnews_clock_candidate(record):
    """Cheaply select only the old combined-clock, single-response layout."""
    if (record.get('provider') != 'cmnews'
            or record.get('extraction_method') != 'css:.articleContent__textContent'
            or record.get('published_time_basis') not in (None, 'publisher_declared_clock_or_unknown')):
        return False
    requested = record.get('requested_url')
    if (not isinstance(requested, str) or requested != record.get('final_url')
            or record.get('source_request_url') not in (None, requested)
            or record.get('source_response_article_id') not in (None, record.get('article_id'))):
        return False
    try:
        url = urlsplit(requested)
        if (url.scheme != 'https' or url.netloc != 'cmnews.com.tw'
                or not re.fullmatch(r'/article/.+-[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}',
                                    url.path, re.I)
                or url.query or url.fragment):
            return False
    except ValueError:
        return False
    digest, raw_clock = record.get('response_sha256'), record.get('published_at_raw')
    return (isinstance(digest, str) and re.fullmatch(r'[0-9a-f]{64}', digest) is not None
            and isinstance(raw_clock, str)
            and combined_publication_update(raw_clock)[0] == 'combined_naive_publication_and_update')


def legacy_cmnews_publication_declaration(record):
    """Return witnessed publisher publication and optional update declarations.

    The caller must verify the immutable batch receipt and bind the row to its
    frozen head. This does not alter the old source, body, capture time, rights,
    or historical availability. Failed and ambiguous evidence remains unknown.
    """
    if not legacy_cmnews_clock_candidate(record):
        return None
    raw = record.get('raw_response_body')
    if (not isinstance(raw, bytes) or not raw
            or hashlib.sha256(raw).hexdigest() != record['response_sha256']):
        return None
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_policy import NewsError
    from downloader.news_fulltext_publishers import cmnews_visible_article

    try:
        result, scope = cmnews_visible_article(BeautifulSoup(raw, 'lxml'), page_url=record['requested_url'])
        visible = {n.get_text(' ', strip=True) for n in scope.select('.articleContent__time')} - {''}
        authors = json.loads(record.get('authors_json') or 'null')
        declared_authors = [a['name'] for a in result['author']]
        published = result['datePublished']
        exact = datetime.fromisoformat(published.replace('Z', '+00:00'))
        pair = combined_publication_update(record['published_at_raw'])[1]
        modified_raw = result['dateModified']
        modified = datetime.fromisoformat(modified_raw.replace('Z', '+00:00'))
    except (NewsError, ValueError, TypeError, KeyError, AttributeError, OverflowError):
        return None
    if (result.get('published_time_basis') != _CMNEWS_DECLARATION_BASIS
            or result.get('headline') != record.get('title') or authors != declared_authors
            or visible != {record['published_at_raw']} or pair is None
            or exact.tzinfo is None or exact.utcoffset() is None
            or exact.replace(tzinfo=None, second=0, microsecond=0) != pair[0]
            or modified.replace(tzinfo=None, second=0, microsecond=0) != pair[1]):
        return None
    # The visible fallback has minute precision and no timezone. An aware
    # update can only have come from the matched NewsArticle declaration.
    return published, modified_raw if modified.tzinfo is not None and modified.utcoffset() is not None else None


def globalvoices_monthly_feed_clock_mismatch(*, provider, article_url, request_url, method, basis, published_utc):
    """Flag a scoped feed/clock inconsistency without deriving a new date.

    Only the publisher's dated monthly feed and an article in that same path
    scope provide the evidence here. A day on either side avoids asserting the
    website's unstated timezone at month boundaries. The reported timestamp
    remains a source claim, and a match does not establish its correctness.
    """
    if (provider != 'globalvoices' or method not in _GLOBALVOICES_FEED_METHODS
            or basis != 'publisher_RSS_pubDate_with_timezone; original_string_preserved'
            or published_utc is None or published_utc.tzinfo is None or published_utc.utcoffset() is None
            or not isinstance(article_url, str) or not isinstance(request_url, str)):
        return False
    try:
        article, feed = urlsplit(article_url), urlsplit(request_url)
        if (article.scheme != 'https' or feed.scheme != 'https'
                or article.netloc != 'globalvoices.org' or feed.netloc != 'globalvoices.org'):
            return False
        article_match = _GLOBALVOICES_MONTHLY_ARTICLE.fullmatch(article.path)
        feed_match = _GLOBALVOICES_MONTHLY_FEED.fullmatch(feed.path)
        if not article_match or not feed_match or article_match.groups() != feed_match.groups():
            return False
        year, month = map(int, feed_match.groups())
        start = datetime(year, month, 1, tzinfo=timezone.utc)
        end = datetime(year + (month == 12), month % 12 + 1, 1, tzinfo=timezone.utc)
        published = published_utc.astimezone(timezone.utc)
        return published < start - timedelta(days=1) or published >= end + timedelta(days=1)
    except (ValueError, OverflowError):
        return False

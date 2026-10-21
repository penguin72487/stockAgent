"""Publisher-declared full RSS content; summaries are never promoted to bodies."""

from __future__ import annotations

from email.utils import parsedate_to_datetime
import hashlib
import json
import re
from urllib.parse import parse_qs, urlsplit, urlunsplit

from bs4 import BeautifulSoup
from lxml import etree

from downloader.news_fulltext_bulk import wordpress_base
from downloader.news_fulltext_policy import (CONTRACT_VERSION, GLOBALVOICES_FEED_BODY_CONTRACT_VERSION, NewsError, _has_paywall, _objects,
    _plain_text, _plain_text_node, capture_usage, normalize_url)
from downloader.news_fulltext_queue import utc_now


XML_BODY_METADATA_FALLBACK = 'rss_invalid_xml_body_requires_html'
MONTHLY_FULLTEXT_FEED_CONTRACT_VERSION = 1
_XML_INVALID_ASCII_CONTROL = re.compile(rb'[\x00-\x08\x0b\x0c\x0e-\x1f]')
FEED_BODY_PROVENANCE_FIELDS = ('fulltext_feed_body_contract_version', 'fulltext_feed_data_fingerprint',
                               'feed_envelope_metadata_json', 'verified_short_feed_body')


def replay_feed_body_version(record, source):
    """Select an immutable capture's parser, not today's default for old bodies."""
    if record.get('extraction_method') == 'rss_content_encoded_v1':
        if any(record.get(k) is not None for k in FEED_BODY_PROVENANCE_FIELDS):
            raise NewsError('invalid_feed_body_contract')
        return 1
    version = record.get('fulltext_feed_body_contract_version')
    fingerprint = dict(source.fulltext_feed_body_fingerprints).get(version) if type(version) is int else None
    if (type(version) is int and version in {2, 3} and source.provider == 'globalvoices'
            and record.get('extraction_method') == f'rss_content_encoded_v{version}'
            and fingerprint and record.get('fulltext_feed_data_fingerprint') == fingerprint):
        return version
    raise NewsError('invalid_feed_body_contract')


def _check_rights(fragment, encoded, item):
    restricted = _has_paywall(fragment) or any(
        str(node.get('content', '')).casefold() == 'false'
        for node in fragment.select('meta[itemprop="isAccessibleForFree"]'))
    for script in fragment.find_all('script', type='application/ld+json'):
        try:
            restricted = restricted or any(str(obj.get('isAccessibleForFree', 'true')).casefold() == 'false'
                for obj in _objects(json.loads(script.get_text())))
        except (ValueError, TypeError):
            raise NewsError('invalid_bulk_post_fields') from None
    if 'all rights reserved' in encoded.lower() or restricted:
        raise NewsError('article_rights_override')
    rights = item.findtext('{http://purl.org/dc/elements/1.1/}rights')
    if rights and rights.strip().lower() not in {'cc-by-3.0', 'https://creativecommons.org/licenses/by/3.0/'}:
        raise NewsError('article_rights_override')


def feed_document(body):
    """Strict XML, with metadata-only repair of controls inside body CDATA.

    Never use recover=True or promote a changed body to original full text.
    Only actual forbidden ASCII controls inside content:encoded CDATA may be
    removed in a temporary metadata view. URLs/clocks/structure remain strict;
    all bodies on this page must be fetched separately from their original URLs.
    """
    try:
        return etree.fromstring(body, etree.XMLParser(resolve_entities=False, no_network=True)), False
    except etree.XMLSyntaxError:
        pass
    controls = len(_XML_INVALID_ASCII_CONTROL.findall(body))
    removed = 0

    def metadata_view(match):
        nonlocal removed
        fragment = match.group(2)
        removed += len(_XML_INVALID_ASCII_CONTROL.findall(fragment))
        return match.group(1) + _XML_INVALID_ASCII_CONTROL.sub(b'', fragment) + match.group(3)

    filtered = re.sub(rb'(<content:encoded>\s*<!\[CDATA\[)(.*?)(\]\]>\s*</content:encoded>)',
                      metadata_view, body, flags=re.S)
    if not controls or removed != controls or b'<!DOCTYPE' in body:
        raise NewsError('invalid_feed')
    try:
        tree = etree.fromstring(filtered, etree.XMLParser(resolve_entities=False, no_network=True))
    except etree.XMLSyntaxError:
        raise NewsError('invalid_feed') from None
    return tree, True


def feed_page(url: str) -> int:
    query = parse_qs(urlsplit(url).query)
    if set(query) - {"paged"} or len(query.get("paged", ["1"])) != 1:
        raise NewsError("invalid_fulltext_feed_page")
    try:
        page = int(query.get("paged", ["1"])[0])
    except ValueError:
        raise NewsError("invalid_fulltext_feed_page") from None
    if not 1 <= page <= 100000:
        raise NewsError("invalid_fulltext_feed_page")
    return page


def feed_url(base: str, page: int = 1) -> str:
    if not 1 <= page <= 100000 or urlsplit(base).query:
        raise ValueError("invalid bounded feed page")
    p = urlsplit(base)
    return urlunsplit((p.scheme, p.netloc, p.path, "paged=" + str(page) if page > 1 else "", ""))


def feed_records(response, http, source, *, body_contract_version=None):
    # Explicit old versions are only for replaying already-sealed envelopes.
    # There is no configuration switch that downgrades new production capture.
    version = (GLOBALVOICES_FEED_BODY_CONTRACT_VERSION if source.provider == 'globalvoices' else 1
               ) if body_contract_version is None else body_contract_version
    if type(version) is not int or version not in {1, 2, 3} or version >= 2 and source.provider != 'globalvoices':
        raise NewsError('invalid_feed_body_contract')
    if version >= 2 and version not in dict(source.fulltext_feed_body_fingerprints):
        raise NewsError('invalid_feed_body_contract')
    scope = wordpress_base(response.url)
    if scope not in source.fulltext_feeds:
        raise NewsError("unregistered_bulk_feed")
    historical_month = (urlsplit(scope).path[:-len('feed/')]
                        if scope in source.historical_fulltext_feeds else None)
    feed_page(response.url)
    tree, metadata_only = feed_document(response.body)
    if tree.tag != "rss":
        raise NewsError("invalid_fulltext_feed")
    items = tree.findall("./channel/item")
    if len(items) > 100:
        raise NewsError("unbounded_fulltext_feed")
    historical_urls = None
    if historical_month:
        historical_urls = [normalize_url(item.findtext('link')) for item in items]
        if any(urlsplit(url).netloc != urlsplit(scope).netloc
               or not urlsplit(url).path.startswith(historical_month) for url in historical_urls):
            # Do not paginate a feed that ignored its date filter: that would
            # rescan unrelated healthy history once for every missing month.
            raise NewsError('historical_feed_scope_mismatch')
    records, errors, identities = [], [], set()
    digest, captured = hashlib.sha256(response.body).hexdigest(), utc_now()
    for number, item in enumerate(items):
        url = None
        try:
            url = historical_urls[number] if historical_urls is not None else normalize_url(item.findtext("link"))
            matched = http.registry.match(url)
            if not matched or matched.provider != source.provider or source.article_path_pattern and not re.search(source.article_path_pattern, urlsplit(url).path):
                raise NewsError("bulk_foreign_article_url")
            http.registry.require(url)
            if url in identities:
                raise NewsError("bulk_pagination_no_progress", retry=True, delay=3600)
            identities.add(url)
            if metadata_only:
                # Publication strings are copied only from unmodified fields,
                # and used for queue ordering, not a training availability claim.
                errors.append({'item_number': number, 'url': url, 'title': item.findtext('title') or '',
                    'metadata_date_raw': item.findtext('pubDate') or '', 'error': XML_BODY_METADATA_FALLBACK})
                continue
            # Only the RSS content module's explicit body, NOT <description>,
            # media content or an unknown namespaced 'content' field.
            encoded = item.find("{http://purl.org/rss/1.0/modules/content/}encoded")
            if encoded is None or not encoded.text:
                raise NewsError("body_not_found")
            fragment = BeautifulSoup(encoded.text, source.html_parser)
            title = _plain_text(item.findtext("title") or "", parser=source.html_parser)
            authors = [n.text.strip() for n in item.findall("{http://purl.org/dc/elements/1.1/}creator") if n.text and n.text.strip()]
            envelope, short_body = None, False
            if version >= 2:
                # Check access/rights before removing any envelope or handling
                # short/photo-only content. A protected short item must still
                # refuse retention of its entire mixed shared raw response.
                _check_rights(fragment, encoded.text, item)
                from downloader.news_fulltext_globalvoices_feed import globalvoices_feed_body
                try:
                    body, envelope, short_body = globalvoices_feed_body(fragment, source, url=url, authors=authors,
                                                                         body_contract_version=version)
                except NewsError as error:
                    if error.code in {'article_identity_not_verified', 'invalid_url'}:
                        # Retain the known article identity for its ordinary
                        # HTML fallback, not a lost/accepted feed body.
                        raise NewsError('invalid_bulk_post_fields') from None
                    raise
            else:
                body = _plain_text_node(fragment, source.exclude_selectors, parser=source.html_parser)
            if not title or len(body) < 100 and not short_body:
                raise NewsError("body_not_found")
            if source.require_author_attribution and not authors:
                raise NewsError("article_attribution_not_verified")
            # A report *about* a paywall is not itself paid content. Check
            # publisher access controls/declarations, not words in prose or
            # an external reference URL. Parse the body once for both duties.
            if version == 1:
                _check_rights(fragment, encoded.text, item)
            published = item.findtext("pubDate")
            if not published or parsedate_to_datetime(published).tzinfo is None:
                raise NewsError("invalid_bulk_publication_clock")
            key = hashlib.sha256(url.encode()).hexdigest()
            records.append({**capture_usage(source), "title": title, "body": body,
                "body_sha256": hashlib.sha256(body.encode()).hexdigest(), "authors_json": json.dumps(authors, ensure_ascii=False),
                "attribution_status": "source_feed_creator_present", "published_at_raw": published,
                "published_time_basis": "publisher_RSS_pubDate_with_timezone; original_string_preserved", "modified_at_raw": None,
                "declared_canonical_url": url, "declared_license_urls_json": "[]",
                "license_verification_basis": "publisher_default_license_feed; explicit_overrides_refused",
                "per_article_license_verified": False,
                "extraction_method": f"rss_content_encoded_v{version}", "body_completeness": "publisher_full_content_field",
                "historical_point_in_time": False, "article_id": key, "requested_url": url, "final_url": url,
                "source_request_url": response.url, "provider": source.provider, "metadata_date_raw": published,
                "captured_at_utc": captured, "body_bytes": len(body.encode()), "response_sha256": digest,
                "http_response_bytes": len(response.body), "raw_response_body": None, "source_response_article_id": None,
                "duration_seconds": None, "etag": None, "last_modified": None,
                "authorization_fingerprint": source.authorization_fingerprint,
                "authorization_json": json.dumps(source.authorization, ensure_ascii=False, sort_keys=True),
                "data_fingerprint": source.data_fingerprint, "bulk_permission_basis": source.bulk_permission_basis,
                "contract_version": CONTRACT_VERSION, "publish": False, "remote_training_export": False})
            if version >= 2:
                records[-1].update(fulltext_feed_body_contract_version=version,
                    fulltext_feed_data_fingerprint=dict(source.fulltext_feed_body_fingerprints)[version],
                    feed_envelope_metadata_json=envelope, verified_short_feed_body=short_body)
            if historical_month:
                records[-1].update(fulltext_feed_scope_contract_version=MONTHLY_FULLTEXT_FEED_CONTRACT_VERSION,
                                   fulltext_feed_scope=scope)
        except (NewsError, ValueError, TypeError) as error:
            errors.append({"item_number": number, "url": url, "error": error.code if isinstance(error, NewsError) else "invalid_bulk_post_fields"})
    return records, errors

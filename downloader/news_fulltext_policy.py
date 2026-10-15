"""Source permission, URL identity and article extraction for news enrichment.

Metadata access, robots permission, text reuse and training permission are
different contracts. This module does not infer one from another.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import ipaddress
import json
import math
from pathlib import Path
import re
import socket
from typing import Any
from urllib.parse import unquote_plus, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup, CData, NavigableString, Tag
from lxml import etree, html as lxml_html
import soupsieve


CONTRACT_VERSION = 1
_GLOBALVOICES_HTML_BODY_CONTRACT_VERSION = 10
CAPTURE_POLICY_VERSION = 2
TRACKING_KEYS = frozenset({"fbclid", "gclid", "mc_cid", "mc_eid"})
# Reviewed source revisions, not an unbounded parser or automatic upgrade.
# Keep old capture identities stable and parse multi-digit versions correctly.
_FED_RELEASE_LAYOUT_VERSIONS = {f"fed_table_release_v{version}": version for version in range(1, 11)}

# A source gate postpones the current request, not a completed download failure.
# Earlier redirect/policy requests remain independently counted by NewsHTTP.
REQUEST_DEFERRAL_CONTRACT_VERSION = 1
REQUEST_DEFERRAL_CODES = frozenset({
    'provider_cooldown', 'host_gateway_cooldown', 'robots_origin_policy_cooldown', 'stopping',
})


class NewsError(Exception):
    """A persistable source outcome; never an invented empty article."""

    def __init__(self, code: str, *, retry: bool = False, delay: float = 0, request_deferred: bool = False):
        if type(request_deferred) is not bool or request_deferred and (code not in REQUEST_DEFERRAL_CODES or retry is not True):
            raise ValueError('verified_request_deferral_required')
        super().__init__(code)
        self.code, self.retry, self.delay = code, retry, delay
        self.request_deferred = request_deferred


def normalize_url(value: Any) -> str:
    """Remove only known tracking parameters; preserve identity and query order."""
    if not isinstance(value, str) or not value.strip():
        raise NewsError("invalid_url")
    if len(value) > 8192 or any(char in value for char in "\r\n\t\\"):
        raise NewsError("invalid_url")
    value = value.strip()
    try:
        parts = urlsplit(value)
        hostname = parts.hostname or ""
        # The identity rule is unchanged. ASCII publisher names do not need
        # the relatively expensive Unicode/IDNA codec on every queued URL.
        host = (hostname if hostname.isascii() else hostname.encode("idna").decode("ascii")).lower().rstrip(".")
        port = parts.port
    except (ValueError, UnicodeError) as exc:
        raise NewsError("invalid_url") from exc
    if parts.scheme.lower() not in {"http", "https"} or not host or parts.username or parts.password:
        raise NewsError("invalid_url")
    if port not in {None, 80 if parts.scheme.lower() == "http" else 443}:
        raise NewsError("invalid_port")
    if ":" in host:  # IPv6 literals are not a configured publisher hostname.
        raise NewsError("invalid_host")
    query = []
    for parameter in parts.query.split("&") if parts.query else []:
        key = unquote_plus(parameter.split("=", 1)[0]).lower()
        if key not in TRACKING_KEYS and not key.startswith("utm_"):
            query.append(parameter)
    return urlunsplit((parts.scheme.lower(), host, parts.path or "/", "&".join(query), ""))


def public_address_check(url: str) -> None:
    """Check every redirect separately; configured publisher domains are mandatory."""
    parts = urlsplit(normalize_url(url))
    try:
        addresses = socket.getaddrinfo(parts.hostname, parts.port or (443 if parts.scheme == "https" else 80), type=socket.SOCK_STREAM)
    except OSError as exc:
        raise NewsError("dns_error", retry=True, delay=30) from exc
    if not addresses or any(not ipaddress.ip_address(item[4][0]).is_global for item in addresses):
        raise NewsError("non_public_address")


@dataclass(frozen=True)
class SourcePolicy:
    provider: str
    name: str
    domains: tuple[str, ...]
    allowed: bool
    authorization_fingerprint: str
    authorization: dict[str, Any]
    interval_seconds: float
    parallelism: int
    selectors: tuple[str, ...]
    feeds: tuple[str, ...]
    sitemaps: tuple[str, ...]
    discovery_interval_seconds: int
    article_path_pattern: str
    require_configured_container: bool
    require_author_attribution: bool
    title_selector: str
    author_selectors: tuple[str, ...]
    exclude_selectors: tuple[str, ...]
    required_license_url_prefix: str
    license_selectors: tuple[str, ...]
    wordpress_apis: tuple[str, ...]
    bulk_permission_basis: str
    data_fingerprint: str
    html_parser: str = "html.parser"
    cnyes_apis: tuple[str, ...] = ()
    cnyes_history_start: str = ""
    fulltext_feeds: tuple[str, ...] = ()
    index_pages: tuple[str, ...] = ()
    index_path_pattern: str = ""
    legacy_release_layout: str = ""
    official_min_interval_seconds: float = 0.0
    pdf_path_pattern: str = ""
    pdf_max_response_bytes: int = 0
    cnyes_history_partition_policy: str = "monthly_v1"
    excluded_article_title_pattern: str = ""
    embedded_article_layout: str = ""
    acquisition_mode: str = "continuous"
    publication_selectors: tuple[str, ...] = ()
    source_not_found_paths: tuple[str, ...] = ()
    allow_short_configured_body: bool = False
    permitted_author_names: tuple[str, ...] = ()
    allow_reviewed_supplements: bool = False
    cnyes_bulk_data_fingerprint: str = ""
    fulltext_feed_data_fingerprint: str = ""
    cnyes_index_api: str = ""
    cnyes_index_data_fingerprint: str = ""
    incremental_index_pages: tuple[str, ...] = ()
    metadata_feeds: tuple[str, ...] = ()
    metadata_feed_authorization: dict[str, Any] | None = None
    metadata_feed_fingerprint: str = ''
    metadata_feed_interval_seconds: int = 0
    cnyes_index_compatibility: tuple[str, str] = ()
    http_capacity_source_fingerprint: str = ''
    parser_repair_fingerprint: str = ''
    parser_repair_capture_fingerprint: str = ''
    known_url_only: bool = False

    @property
    def request_enabled(self) -> bool:
        return self.allowed or bool(self.metadata_feeds)


def parser_repair_fingerprint(payload: dict[str, Any]) -> str:
    """Separate parser repairs from known-URL scheduling and review prose.

    This identity only controls replay of failed parsing. It never replaces
    the complete capture/authorization identity or grants access. Retain all
    machine-enforced scope, attribution, selectors, paths and parser versions.
    Copy the nested mappings so the original capture payload stays identical.
    """
    normalized = dict(payload)
    # URL admission/discovery scheduling is not a body-parser repair. Old
    # refusals and immutable bodies must not be replayed after a scope-only edit.
    normalized.pop('known_url_only_schedule_contract_version', None)
    entry = dict(payload['entry'])
    for field in ('acquisition_mode', 'private_known_url_scope', 'note',
                  'interval_seconds', 'parallelism', 'discovery_interval_seconds',
                  'official_min_interval_seconds', 'official_rate_limit_evidence_url'):
        entry.pop(field, None)
    for field in ('entry', 'authorization'):
        authorization = dict(entry.get('authorization', {}) if field == 'entry' else payload['authorization'])
        authorization.pop('scope', None)
        authorization.pop('reviewed_on', None)
        if field == 'entry':
            entry['authorization'] = authorization
        else:
            normalized['authorization'] = authorization
    normalized['entry'] = entry
    return hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest()


def _known_url_only_scope(entry, auth):
    """Local metadata growth only, not an authorization to discover a site.

    This explicit scope reuses the existing personal-reading, article-level
    license, or reviewed native publisher-original checks. A linked site notice
    is not a per-article license or permission for syndicated works. Reject
    endpoint configuration, including dormant discovery jobs, rather than
    silently expanding the scope on a later edit.
    """
    scope = entry.get('private_known_url_scope', '')
    if scope == '':
        return False
    if not isinstance(scope, str) or scope not in {
            'personal_noncommercial_originals_v1', 'per_article_licensed_text_v1',
            'publisher_original_open_license_v1'}:
        raise ValueError('unreviewed known-URL-only scope')
    if entry.get('acquisition_mode') == 'finite':
        return False  # A retained annotation never extends an immutable cohort.
    personal = scope == 'personal_noncommercial_originals_v1' and auth.get('kind') == 'personal_research'
    licensed = (scope == 'per_article_licensed_text_v1' and auth.get('kind') == 'open_license'
                and bool(entry.get('required_license_url_prefix')) and bool(entry.get('license_selectors'))
                and entry.get('require_configured_container') is True)
    publisher_original = (
        scope == 'publisher_original_open_license_v1' and entry.get('id') == 'coolloud'
        and entry.get('domains') == ['www.coolloud.org.tw']
        and entry.get('embedded_article_layout') == 'coolloud_visible_original_v1'
        and auth.get('kind') == 'open_license' and auth.get('license') == 'CC-BY-NC-SA-2.0-TW'
        and auth.get('evidence_url') == 'https://www.coolloud.org.tw/node/4524'
        and entry.get('require_configured_container') is True
        and entry.get('article_selectors') == [':scope > .field-name-body > .field-items > .field-item'])
    if entry.get('require_author_attribution') is not True:
        raise ValueError('known-URL-only scope or reviewed author scope requires exact current-article attribution')
    if (not (personal or licensed or publisher_original) or entry.get('acquisition_mode') != 'continuous'
            or not entry.get('article_selectors') or not entry.get('article_path_pattern')):
        raise ValueError('unreviewed known-URL-only scope')
    if any(entry.get(field) for field in (
            'feeds', 'sitemaps', 'wordpress_apis', 'cnyes_apis', 'fulltext_feeds',
            'index_pages', 'incremental_index_pages', 'metadata_feeds', 'cnyes_public_index')):
        raise ValueError('known-URL-only scope cannot configure discovery endpoints')
    return True


def cnyes_bulk_fingerprint(source: SourcePolicy) -> str:
    """Keep API/cache acceptance independent of the verified short-HTML ABI.

    The registry removes ONLY that HTML-only version. Endpoint configuration,
    authorization and every API parser/capture rule remain in this fingerprint.
    """
    return source.cnyes_bulk_data_fingerprint or source.data_fingerprint


class PolicyRegistry:
    def __init__(self, config: dict[str, Any], *, base: Path):
        if config.get("contract_version") != CONTRACT_VERSION:
            raise ValueError("unsupported news fulltext contract")
        if config.get("publish") is not False or config.get("remote_training_export") is not False:
            raise ValueError("This collector is private; publication/training export needs its own contract")
        self.sources: dict[str, SourcePolicy] = {}
        occupied: set[str] = set()
        for entry in config["providers"]:
            provider = entry["id"]
            if not re.fullmatch(r"[a-z0-9_]+", provider) or provider in self.sources:
                raise ValueError("invalid or duplicate provider ID")
            domains = tuple(d.lower().rstrip(".") for d in entry["domains"])
            for domain in domains:
                if not re.fullmatch(r"[a-z0-9.-]+\.[a-z]{2,}", domain) or domain in occupied:
                    raise ValueError("invalid or duplicate publisher domain")
                occupied.add(domain)
            auth = dict(entry.get("authorization", {}))
            allowed = bool(entry.get("enabled"))
            if auth.get("kind") in {"open_license", "public_domain"}:
                allowed &= bool(auth.get("license") and auth.get("scope") and auth.get("evidence_url", "").startswith("https://"))
            elif auth.get("kind") == "personal_research":
                # A collection purpose is NOT a publisher license or a legal
                # fair-use verdict. Public, reviewed sources may be captured
                # privately without claiming redistribution/training rights.
                allowed &= bool(
                    config.get("collection_purpose") == "personal_research"
                    and auth.get("scope")
                    and auth.get("evidence_url", "").startswith("https://")
                    and auth.get("reviewed_on")
                    and auth.get("automated_access_review") == "no_explicit_prohibition_identified"
                )
            elif auth.get("kind") == "written_authorization":
                evidence = base / str(auth.get("evidence_file", ""))
                allowed &= bool(auth.get("scope") and auth.get("full_text_download") is True and evidence.is_file())
                if evidence.is_file():
                    auth["evidence_sha256"] = hashlib.sha256(evidence.read_bytes()).hexdigest()
            else:
                allowed = False
            if auth.get("valid_until"):
                expiry = datetime.fromisoformat(str(auth["valid_until"]).replace("Z", "+00:00"))
                if expiry.tzinfo is None:
                    raise ValueError("authorization expiry must include timezone")
                allowed &= expiry > datetime.now(timezone.utc)
            interval = float(entry.get("interval_seconds", 2))
            parallelism = int(entry.get("parallelism", 1))
            if not math.isfinite(interval) or interval <= 0 or parallelism < 1 or parallelism > 8:
                raise ValueError("invalid publisher resource limits")
            official_floor = entry.get("official_min_interval_seconds", 0.0)
            if (type(official_floor) not in {float, int} or not math.isfinite(official_floor) or official_floor < 0
                    or official_floor and not str(entry.get("official_rate_limit_evidence_url", "")).startswith("https://")):
                raise ValueError("official news rate limit requires a numeric floor and primary evidence")
            html_parser = str(entry.get("html_parser", "html.parser"))
            if html_parser not in {"html.parser", "lxml"}:
                raise ValueError("unsupported article parser")
            pattern = str(entry.get("article_path_pattern", ""))
            if pattern:
                re.compile(pattern)
            excluded_title = str(entry.get("excluded_article_title_pattern", ""))
            if excluded_title:
                re.compile(excluded_title)
            fingerprint = hashlib.sha256(json.dumps(auth, sort_keys=True).encode()).hexdigest()
            bulk_basis = str(entry.get("bulk_text_permission_basis", ""))
            if entry.get("wordpress_apis") and not bulk_basis:
                raise ValueError("bulk text endpoint requires an explicit permission basis")
            fulltext_feeds = tuple(entry.get("fulltext_feeds", []))
            if fulltext_feeds and (auth.get("kind") != "open_license" or not bulk_basis or any(
                    not url.startswith("https://") or urlsplit(url).query or not any(
                        urlsplit(url).hostname == d or (urlsplit(url).hostname or '').endswith('.' + d) for d in domains)
                    for url in fulltext_feeds)):
                raise ValueError("fulltext feeds require reviewed open-license endpoints")
            legacy = str(entry.get("legacy_release_layout", ""))
            if legacy and (legacy not in _FED_RELEASE_LAYOUT_VERSIONS or provider != "fed_board" or auth.get("kind") != "public_domain"):
                raise ValueError("unreviewed legacy release parser")
            known_url_only = _known_url_only_scope(entry, auth)
            embedded = str(entry.get("embedded_article_layout", ""))
            einfo_layout = (embedded == "einfo_next_post_v1" and provider == "einfo" and auth.get("kind") == "open_license"
                            and auth.get("license") == "CC-BY-NC-ND-4.0" and auth.get("evidence_url") == "https://e-info.org.tw/copyright")
            cmnews_layout = (embedded == "cmnews_visible_article_v1" and provider == "cmnews"
                             and domains == ("cmnews.com.tw",) and auth.get("kind") == "personal_research"
                             and entry.get("require_configured_container") is True
                             and entry.get("require_author_attribution") is True)
            tainan_layout = (embedded == "tainan_visible_news_v1" and provider == "tainan_gov"
                             and domains == ("www.tainan.gov.tw",) and auth.get("kind") == "open_license"
                             and auth.get("evidence_url") == "https://www.tainan.gov.tw/cp.aspx?n=13612"
                             and entry.get("acquisition_mode") == "finite"
                             and entry.get("require_configured_container") is True
                             and entry.get("require_author_attribution") is True)
            macau_layout = (embedded == "macau_visible_news_v1" and provider == "macau_gov"
                            and domains == ("www.gov.mo",) and auth.get("kind") == "open_license"
                            and auth.get("license") == "Macau-Government-Portal-Noncommercial-Unmodified"
                            and auth.get("evidence_url") == "https://www.gov.mo/zh-hant/terms-of-use/"
                            and entry.get("acquisition_mode") == "finite"
                            and entry.get("require_configured_container") is True
                            and entry.get("require_author_attribution") is True)
            kmdn_layout = (embedded in {"kmdn_visible_article_v1", "kmdn_visible_article_v2"} and provider == "kmdn"
                           and domains == ("www.kmdn.gov.tw",) and auth.get("kind") == "personal_research"
                           and (embedded == "kmdn_visible_article_v1"
                                or auth.get("evidence_url") == "https://www.kmdn.gov.tw/1119/1121/1367/")
                           and (entry.get("acquisition_mode") == "finite"
                                or known_url_only and entry.get('private_known_url_scope')
                                == 'personal_noncommercial_originals_v1')
                           and entry.get("require_configured_container") is True
                           and entry.get("require_author_attribution") is True)
            coolloud_layout = (embedded == 'coolloud_visible_original_v1' and provider == 'coolloud'
                               and domains == ('www.coolloud.org.tw',) and auth.get('kind') == 'open_license'
                               and auth.get('license') == 'CC-BY-NC-SA-2.0-TW'
                               and auth.get('evidence_url') == 'https://www.coolloud.org.tw/node/4524'
                               and (entry.get('acquisition_mode') == 'finite'
                                    or known_url_only and entry.get('private_known_url_scope')
                                    == 'publisher_original_open_license_v1')
                               and entry.get('require_configured_container') is True
                               and entry.get('require_author_attribution') is True
                               and entry.get('article_selectors') == [':scope > .field-name-body > .field-items > .field-item'])
            cnews_layout = (embedded == "cnews_visible_article_v1" and provider == "cnews"
                            and domains == ("cnews.com.tw",) and auth.get("kind") == "personal_research"
                            and auth.get("evidence_url") == "https://cnews.com.tw/%E7%89%88%E6%AC%8A%E8%81%B2%E6%98%8E/"
                            and (entry.get("acquisition_mode") == "finite"
                                 or known_url_only and entry.get('private_known_url_scope')
                                 == 'personal_noncommercial_originals_v1')
                            and entry.get("require_configured_container") is True
                            and entry.get("require_author_attribution") is True)
            cmmedia_layout = (embedded == "cmmedia_visible_article_v1" and provider == "cmmedia"
                              and domains == ("www.cmmedia.com.tw",) and auth.get("kind") == "personal_research"
                              and auth.get("evidence_url") == "https://www.cmmedia.com.tw/home/copyright"
                              and (entry.get("acquisition_mode") == "finite"
                                   or known_url_only and entry.get('private_known_url_scope')
                                   == 'personal_noncommercial_originals_v1')
                              and entry.get("require_configured_container") is True
                              and entry.get("require_author_attribution") is True)
            nownews_layout = (embedded == "nownews_visible_article_v1" and provider == "nownews"
                              and domains in {("nownews.com",), ("www.nownews.com",)}
                              and auth.get("kind") == "personal_research"
                              and auth.get("evidence_url") == "https://www.nownews.com/copyright"
                              and (entry.get("acquisition_mode") == "finite"
                                   or entry.get("acquisition_mode") == "continuous"
                                   and entry.get("private_known_url_scope") == "personal_noncommercial_originals_v1")
                              and entry.get("require_configured_container") is True
                              and entry.get("require_author_attribution") is True)
            ftv_layout = (embedded == "ftv_visible_article_v1" and provider == "ftvnews"
                          and domains in {("ftvnews.com.tw",), ("www.ftvnews.com.tw",),
                              ("www.ftvnews.com.tw", "finance.ftvnews.com.tw", "sport.ftvnews.com.tw", "health.ftvnews.com.tw")}
                          and auth.get("kind") == "personal_research"
                          and auth.get("evidence_url") == "https://www.ftvnews.com.tw/news/detail/2026317W0044"
                          and (entry.get("acquisition_mode") == "finite"
                               or entry.get("acquisition_mode") == "continuous"
                               and entry.get("private_known_url_scope") == "personal_noncommercial_originals_v1")
                          and entry.get("require_configured_container") is True
                          and entry.get("require_author_attribution") is True)
            taiwanhot_layout = (embedded in {'taiwanhot_visible_article_v1', 'taiwanhot_visible_article_v2', 'taiwanhot_visible_article_v3'} and provider == 'taiwanhot'
                                and domains == ('taiwanhot.net',) and auth.get('kind') == 'personal_research'
                                and auth.get('evidence_url') == 'https://www.taiwanhot.net/'
                                and known_url_only and entry.get('private_known_url_scope') == 'personal_noncommercial_originals_v1'
                                and entry.get('require_configured_container') is True
                                and entry.get('require_author_attribution') is True
                                and entry.get('article_path_pattern') == (
                                    '^(?:/news/(?:focus/)?[1-9][0-9]*(?:/|$)|/$)'
                                    if embedded == 'taiwanhot_visible_article_v3' else
                                    '^(?:/news/(?:focus/)?[1-9][0-9]*/|/$)')
                                and entry.get('article_selectors') == ['article > .news_content']
                                and entry.get('title_selector') == '.top_title > h2.news_title'
                                and entry.get('author_selectors') == ['.top_title > h5.txt_gray2 > span.reporter_name']
                                and entry.get('publication_selectors') == ['.top_title > h5.txt_gray2 > span.post_time']
                                and entry.get('exclude_selectors') == ['.photo_wrap'])
            voa_layout = (embedded in {"voa_original_article_v1", "voa_reviewed_private_article_v2"}
                          and (provider, domains, auth.get("evidence_url")) in {
                              ("voa_chinese", ("www.voachinese.com",), "https://www.voachinese.com/p/3874.html"),
                              ("voa_cantonese", ("www.voacantonese.com",), "https://www.voacantonese.com/p/3889.html")}
                          and (embedded == "voa_original_article_v1" and auth.get("kind") == "public_domain"
                               and auth.get("license") == "VOA-produced-original-text-public-property-with-attribution"
                               or embedded == "voa_reviewed_private_article_v2" and provider == "voa_cantonese"
                               and auth.get("kind") == "personal_research"
                               and auth.get("license") == "VOA-reviewed-text-personal-noncommercial-storage-only")
                          and bool(entry.get("permitted_author_names"))
                          and entry.get("acquisition_mode") == "finite"
                          and entry.get("article_selectors") == ["#article-content > .wsw"]
                          and entry.get("author_selectors") == [".publishing-details > .links > .links__list > .links__item"]
                          and ".wsw__embed" in entry.get("exclude_selectors", [])
                          and entry.get("require_configured_container") is True
                          and entry.get("require_author_attribution") is True)
            tfc_layout = (embedded == 'tfc_visible_fact_check_v1' and provider == 'tfc_taiwan'
                          and domains == ('tfc-taiwan.org.tw',) and auth.get('kind') == 'open_license'
                          and auth.get('license') == 'TFC-Publisher-Nonprofit-Unmodified-Attribution'
                          and auth.get('evidence_url') == 'https://tfc-taiwan.org.tw/copyright-notice/'
                          and entry.get('acquisition_mode') == 'finite'
                          and entry.get('require_configured_container') is True
                          and entry.get('require_author_attribution') is True)
            if tfc_layout:
                from downloader.news_fulltext_fact_checks import TFC_BODY_SELECTOR
                tfc_layout = (entry.get('article_selectors') == [TFC_BODY_SELECTOR]
                              and entry.get('exclude_selectors') == ['.wp-block-kadence-image'])
            nccu_layout = (embedded == 'nccu_visible_news_v1' and provider == 'nccu'
                           and domains == ('www.nccu.edu.tw',) and auth.get('kind') == 'open_license'
                           and auth.get('license') == 'CC-BY-NC-ND-2.5-TW'
                           and auth.get('evidence_url') == 'https://www.nccu.edu.tw/p/426-1000-30.php?Lang=zh-tw'
                           and entry.get('acquisition_mode') == 'finite'
                           and entry.get('require_configured_container') is True
                           and entry.get('require_author_attribution') is True)
            if nccu_layout:
                from downloader.news_fulltext_nccu import NCCU_BODY_SELECTOR
                nccu_layout = (entry.get('article_selectors') == [NCCU_BODY_SELECTOR]
                               and entry.get('exclude_selectors') == []
                               and entry.get('article_path_pattern') == '^/p/406-1000-9212,r17[.]php$')
            taichung_layout = (embedded == 'taichung_visible_news_v1' and provider == 'taichung_gov'
                               and domains == ('www.taichung.gov.tw',) and auth.get('kind') == 'open_license'
                               and auth.get('evidence_url') == 'https://www.taichung.gov.tw/10179/10185/13599'
                               and entry.get('acquisition_mode') == 'finite'
                               and entry.get('require_configured_container') is True
                               and entry.get('require_author_attribution') is True
                               and entry.get('article_selectors') == ['#cpArticle'])
            changhua_layout = (embedded == 'changhua_visible_news_v1' and provider == 'changhua_gov'
                               and domains == ('www.chcg.gov.tw',) and auth.get('kind') == 'open_license'
                               and auth.get('evidence_url') == 'https://www.chcg.gov.tw/ch2/copyright.aspx'
                               and entry.get('acquisition_mode') == 'finite'
                               and entry.get('require_configured_container') is True
                               and entry.get('require_author_attribution') is True
                               and entry.get('article_selectors') == [':scope > article.content'])
            events_layout = (embedded == 'eventsinfocus_visible_news_v1' and provider == 'eventsinfocus'
                             and domains == ('eventsinfocus.org',) and auth.get('kind') == 'personal_research'
                             and auth.get('evidence_url') == 'https://eventsinfocus.org/pages/55'
                             and entry.get('acquisition_mode') == 'finite'
                             and entry.get('require_configured_container') is True
                             and entry.get('require_author_attribution') is True
                             and entry.get('article_path_pattern') == '^/news/7145971$')
            if events_layout:
                from downloader.news_fulltext_independent import EVENTS_BODY_SELECTOR
                events_layout = (entry.get('article_selectors') == [EVENTS_BODY_SELECTOR]
                                 and entry.get('exclude_selectors') == [])
            ftnn_layout = (embedded == 'ftnn_visible_article_v1' and provider == 'ftnn'
                           and domains == ('ftnn.com.tw',) and auth.get('kind') == 'personal_research'
                           and auth.get('evidence_url') == 'https://www.ftnn.com.tw/copyright'
                           and known_url_only and entry.get('private_known_url_scope') == 'personal_noncommercial_originals_v1'
                           and entry.get('require_configured_container') is True
                           and entry.get('require_author_attribution') is True
                           and entry.get('article_path_pattern') == '^/news/[1-9][0-9]*$'
                           and entry.get('article_selectors') == [':scope > .news-body']
                           and entry.get('exclude_selectors') == [])
            tvbs_layout = (embedded in {'tvbs_visible_article_v1', 'tvbs_visible_article_v2'} and provider == 'tvbs'
                           and domains == ('news.tvbs.com.tw', 'cars.tvbs.com.tw', 'supertaste.tvbs.com.tw')
                           and auth.get('kind') == 'personal_research'
                           and auth.get('evidence_url') == 'https://www.tvbs.com.tw/policies/copyright'
                           and known_url_only and entry.get('private_known_url_scope') == 'personal_noncommercial_originals_v1'
                           and entry.get('require_configured_container') is True
                           and entry.get('require_author_attribution') is True
                           and entry.get('article_path_pattern') == '^(?:/[a-z][a-z-]*/[1-9][0-9]*|/exhibition/ai-trends-2026/article_(?:1|3)-3[.]html)$'
                           and entry.get('article_selectors') == [':scope > .tvbs-native-body']
                           and entry.get('exclude_selectors') == [])
            ettoday_layout = (embedded == 'ettoday_visible_article_v1' and provider == 'ettoday'
                              and domains == ('ettoday.net',) and auth.get('kind') == 'personal_research'
                              and auth.get('evidence_url') == 'https://member.ettoday.net/clause_copyright.php7'
                              and known_url_only and entry.get('private_known_url_scope') == 'personal_noncommercial_originals_v1'
                              and entry.get('require_configured_container') is True
                              and entry.get('require_author_attribution') is True
                              and entry.get('exclude_selectors') == [])
            if ettoday_layout:
                from downloader.news_fulltext_ettoday import ETTODAY_BODY_SELECTOR, ETTODAY_PATH_PATTERN
                ettoday_layout = (entry.get('article_selectors') == [ETTODAY_BODY_SELECTOR]
                                  and entry.get('article_path_pattern') == ETTODAY_PATH_PATTERN)
            if embedded and not (einfo_layout or cmnews_layout or tainan_layout or macau_layout or kmdn_layout or coolloud_layout or cnews_layout or cmmedia_layout or nownews_layout or ftv_layout or taiwanhot_layout or voa_layout or tfc_layout or nccu_layout or taichung_layout or changhua_layout or events_layout or ftnn_layout or tvbs_layout or ettoday_layout):
                raise ValueError("unreviewed embedded article parser")
            acquisition_mode = entry.get("acquisition_mode", "continuous")
            if not isinstance(acquisition_mode, str) or acquisition_mode not in {"continuous", "finite"}:
                raise ValueError("invalid news acquisition mode")
            incremental_indexes = entry.get('incremental_index_pages', [])
            if (not isinstance(incremental_indexes, list) or len(incremental_indexes) > 32
                    or any(not isinstance(url, str) or not url or len(url) > 2048 for url in incremental_indexes)
                    or len(set(incremental_indexes)) != len(incremental_indexes)):
                raise ValueError('invalid incremental index scope')
            if incremental_indexes:
                declared_interval = entry.get('discovery_interval_seconds', 3600)
                if (acquisition_mode != 'continuous' or type(declared_interval) is not int
                        or not 60 <= declared_interval <= 365*86400):
                    raise ValueError('incremental index requires continuous mode and a valid interval')
                for url in incremental_indexes:
                    try:
                        parts = urlsplit(url)
                        valid = (url in entry.get('index_pages', []) and normalize_url(url) == url
                            and not parts.query and not parts.fragment
                            and any(parts.hostname == d or (parts.hostname or '').endswith('.'+d) for d in domains))
                    except NewsError:
                        valid = False
                    if not valid:
                        raise ValueError('incremental index must be an exact registered public index URL')
            publication_selectors = entry.get('publication_selectors', [])
            if (not isinstance(publication_selectors, list) or len(publication_selectors) > 8
                    or any(not isinstance(s, str) or not s or len(s) > 512 for s in publication_selectors)):
                raise ValueError('invalid declared publication selectors')
            permitted_authors = entry.get('permitted_author_names', [])
            if (not isinstance(permitted_authors, list) or len(permitted_authors) > 128
                    or any(not isinstance(name, str) or not name or name != name.strip() or len(name) > 160
                           for name in permitted_authors)
                    or len(set(permitted_authors)) != len(permitted_authors)):
                raise ValueError('invalid reviewed author scope')
            if permitted_authors:
                evidence_urls = auth.get('original_author_scope_evidence_urls', [])
                if (entry.get('require_author_attribution') is not True or not entry.get('author_selectors')
                        or not isinstance(evidence_urls, list) or not 1 <= len(evidence_urls) <= 16
                        or any(not isinstance(url, str) or len(url) > 2048 or not url.startswith('https://')
                               or not any(urlsplit(url).hostname == domain
                                          or (urlsplit(url).hostname or '').endswith('.' + domain) for domain in domains)
                               for url in evidence_urls)):
                    raise ValueError('reviewed author scope requires exact byline selectors and primary evidence')
            absent_paths = entry.get('source_not_found_paths', [])
            if (not isinstance(absent_paths, list) or len(absent_paths) > 8
                    or any(not isinstance(p, str) or not p.startswith('/') or p == '/'
                           or len(p) > 512 or '?' in p or '#' in p for p in absent_paths)):
                raise ValueError('invalid source absence paths')
            short_body = entry.get('allow_short_configured_body', False)
            if type(short_body) is not bool or short_body and not (
                    entry.get('require_configured_container') is True and entry.get('require_author_attribution') is True
                    and entry.get('article_selectors') and entry.get('title_selector') and publication_selectors):
                raise ValueError('short-body acceptance requires reviewed exact container, title, author and clock selectors')
            pdf_pattern = str(entry.get("pdf_path_pattern", ""))
            pdf_maximum = entry.get("pdf_max_response_bytes", 0)
            if pdf_pattern:
                if provider != "fed_board" or auth.get("kind") != "public_domain" or not pdf_pattern.startswith("^"):
                    raise ValueError("PDF text requires a reviewed Board document namespace")
                re.compile(pdf_pattern)
            if (type(pdf_maximum) is not int or pdf_maximum < 0 or pdf_maximum > 64 << 20
                    or pdf_maximum and not pdf_pattern):
                raise ValueError("invalid registered PDF response budget")
            index_pattern = str(entry.get("index_path_pattern", ""))
            if index_pattern:
                re.compile(index_pattern)
            cnyes_apis = tuple(entry.get("cnyes_apis", []))
            cnyes_start = str(entry.get("cnyes_history_start", ""))
            cnyes_partition = str(entry.get("cnyes_history_partition_policy", "monthly_v1"))
            if cnyes_partition not in {"monthly_v1", "adaptive_days_v1"}:
                raise ValueError("unreviewed Cnyes history partition policy")
            cnyes_inventory = None
            if cnyes_apis:
                reviewed = {"headline", "stock_report"}
                manifest = entry.get("cnyes_endpoint_inventory")
                if manifest:
                    manifest_path = (base / manifest).resolve()
                    if not manifest_path.is_relative_to(base.resolve()) or manifest_path.stat().st_size > 1 << 20:
                        raise ValueError("invalid Cnyes endpoint inventory path/size")
                    cnyes_inventory = json.loads(manifest_path.read_text())
                    if not isinstance(cnyes_inventory, dict):
                        raise ValueError("Cnyes endpoint inventory must be an object")
                    navigation = cnyes_inventory.get("publisher_navigation", {})
                    endpoints = cnyes_inventory.get("endpoints", [])
                    if (cnyes_inventory.get("contract_version") != 1 or not isinstance(navigation, dict)
                            or not re.fullmatch(r"https://news[.]cnyes[.]com/_next/static/chunks/[A-Za-z0-9._-]+[.]js", navigation.get("url", ""))
                            or not re.fullmatch(r"[0-9a-f]{64}", navigation.get("response_sha256", ""))
                            or not isinstance(endpoints, list) or not 1 <= len(endpoints) <= 128):
                        raise ValueError("unreviewed Cnyes endpoint inventory")
                    for endpoint in endpoints:
                        if (not isinstance(endpoint, dict) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", endpoint.get("slug", ""))
                                or not re.fullmatch(r"[0-9a-f]{64}", endpoint.get("response_sha256", ""))
                                or type(endpoint.get("returned_rows")) is not int or not 0 <= endpoint["returned_rows"] <= 30
                                or type(endpoint.get("validated_body_rows")) is not int
                                or not 0 <= endpoint["validated_body_rows"] <= endpoint["returned_rows"]):
                            raise ValueError("invalid Cnyes reviewed endpoint evidence")
                        reviewed.add(endpoint["slug"])
                    if len({row["slug"] for row in endpoints}) != len(endpoints):
                        raise ValueError("duplicate Cnyes reviewed category")
                if provider != "cnyes" or not bulk_basis or any(
                        not re.fullmatch(r"https://api[.]cnyes[.]com/media/api/v1/newslist/category/[a-z][a-z0-9_]{0,63}", url)
                        or url.rsplit("/", 1)[-1] not in reviewed
                        for url in cnyes_apis):
                    raise ValueError("Cnyes bulk requires reviewed, explicitly registered publisher endpoints")
                history_start = datetime.strptime(cnyes_start, "%Y-%m-%d")
                if not 1999 <= history_start.year <= datetime.now(timezone.utc).year or history_start.date() > datetime.now(timezone.utc).date():
                    raise ValueError("invalid Cnyes history start")
            from downloader.news_fulltext_cnyes_index import reviewed_index
            cnyes_index_api, index_fingerprint = reviewed_index(entry, provider, auth)
            index_compatibility = ((index_fingerprint, reviewed_index(entry, provider, auth, data_version=1)[1])
                                   if cnyes_index_api else ())
            from downloader.news_fulltext_metadata_feeds import reviewed_policy
            metadata_urls, metadata_auth, metadata_fingerprint, metadata_interval = reviewed_policy(entry, config)
            # Public list metadata has its own ABI. Enabling that list must
            # not invalidate millions of verified bodies/category pages.
            # Live-index cadence has its own scheduling ABI. It grants no new
            # body/endpoint scope and must not invalidate historical captures
            # or cause otherwise unchanged indexes/feeds to be reparsed.
            capture_entry = {key:value for key,value in entry.items()
                             if key not in {'cnyes_public_index', 'incremental_index_pages', 'metadata_feed_policy'}}
            fingerprint_payload = {"version": CONTRACT_VERSION, "capture_policy_version": CAPTURE_POLICY_VERSION,
                                                        "collection_purpose": config.get("collection_purpose", "personal_research"),
                                                        "html_parser": html_parser,
                                                        "entry": capture_entry, "authorization": auth}
            if known_url_only:
                fingerprint_payload['known_url_only_schedule_contract_version'] = 1
            if cnyes_apis:
                fingerprint_payload["cnyes_adapter_contract_version"] = 8
                fingerprint_payload["reviewed_cnyes_endpoint_inventory"] = cnyes_inventory
            if provider == 'cnyes':
                fingerprint_payload['visible_headline_body_ownership_contract_version'] = 1
                fingerprint_payload['verified_short_html_body_contract_version'] = 5
                fingerprint_payload['verified_native_prose_spacing_contract_version'] = 1
                fingerprint_payload['mobile_canonical_html_contract_version'] = 43
                fingerprint_payload['verified_empty_html_outcome_contract_version'] = 8
                fingerprint_payload['verified_office_comment_empty_outcome_contract_version'] = 1
                fingerprint_payload['verified_native_quote_only_outcome_contract_version'] = 7
                fingerprint_payload['verified_native_image_only_outcome_contract_version'] = 4
                fingerprint_payload['verified_source_test_article_outcome_contract_version'] = 1
                fingerprint_payload['verified_untitled_no_events_bulletin_contract_version'] = 1
                fingerprint_payload['verified_source_signature_image_outcome_contract_version'] = 1
            if fulltext_feeds:
                fingerprint_payload['fulltext_feed_rights_contract_version'] = 2
            if pdf_pattern:
                fingerprint_payload["pdf_request_contract_version"] = 2
            if legacy:
                fingerprint_payload["fed_legacy_adapter_contract_version"] = max(3, _FED_RELEASE_LAYOUT_VERSIONS[legacy])
            if embedded:
                fingerprint_payload["embedded_article_adapter_contract_version"] = (
                    {"cmnews_visible_article_v1": 6, "einfo_next_post_v1": 2, "cnews_visible_article_v1": 4,
                     "ftv_visible_article_v1": 5, "nownews_visible_article_v1": 2,
                     "ftnn_visible_article_v1": 2, "tvbs_visible_article_v2": 2,
                     "tainan_visible_news_v1": 2, "voa_reviewed_private_article_v2": 3,
                     "taichung_visible_news_v1": 2}.get(embedded, 1))
            if tainan_layout:
                fingerprint_payload["verified_html_separator_request_alias_contract_version"] = 2
            if provider == "globalvoices":
                fingerprint_payload["publisher_removed_body_notice_contract_version"] = 2
                fingerprint_payload["publisher_challenge_headline_identity_contract_version"] = 1
                fingerprint_payload['verified_small_html_body_contract_version'] = _GLOBALVOICES_HTML_BODY_CONTRACT_VERSION
                fingerprint_payload['mixed_nontext_native_embed_classification_version'] = 1
            if provider == 'genetinfo':
                fingerprint_payload['image_only_body_classification_version'] = 1
            if short_body:
                fingerprint_payload['short_configured_body_contract_version'] = 1
            if provider == 'pansci':
                fingerprint_payload['first_post_identity_and_clock_contract_version'] = 1
            if permitted_authors:
                fingerprint_payload['reviewed_author_scope_contract_version'] = 1
            if provider == 'moneyweekly':
                fingerprint_payload['moneyweekly_source_identity_contract_version'] = 2
            data_fingerprint = hashlib.sha256(json.dumps(fingerprint_payload, sort_keys=True).encode()).hexdigest()
            capacity_fingerprint = ''
            if provider == 'cnyes':
                # Word spacing and verified source-test outcomes are parser
                # ABIs, not HTTP policy.
                # Keep the exact previous pacing/refusal identity, including
                # every original scope, authorization, endpoint and config
                # field. Other HTML ABI/policy changes still invalidate it.
                capacity_payload = dict(fingerprint_payload)
                capacity_payload.pop('verified_native_prose_spacing_contract_version')
                capacity_payload.pop('verified_source_test_article_outcome_contract_version')
                capacity_payload.pop('visible_headline_body_ownership_contract_version')
                capacity_payload.pop('verified_office_comment_empty_outcome_contract_version')
                capacity_payload.pop('verified_untitled_no_events_bulletin_contract_version')
                capacity_payload.pop('verified_source_signature_image_outcome_contract_version')
                capacity_fingerprint = hashlib.sha256(json.dumps(capacity_payload, sort_keys=True).encode()).hexdigest()
            elif ftnn_layout:
                # The dual native release/revision proof is an HTML-only ABI.
                # Normalize only that adapter version to retain the exact
                # previous HTTP identity, including scope/auth/entry fields.
                # Its old capture identity also pins the one-time repair of
                # prior parser clock failures, never source refusals.
                capacity_payload = dict(fingerprint_payload)
                capacity_payload['embedded_article_adapter_contract_version'] = 1
                capacity_fingerprint = hashlib.sha256(json.dumps(capacity_payload, sort_keys=True).encode()).hexdigest()
            elif tvbs_layout and embedded == 'tvbs_visible_article_v2':
                # Native clock/credit interpretation alone changed. Normalize
                # only this explicit adapter ABI to its exact prior capture;
                # all original source/auth/scope/HTTP fields remain binding.
                capacity_payload = dict(fingerprint_payload)
                capacity_payload['entry'] = {**capture_entry, 'embedded_article_layout': 'tvbs_visible_article_v1'}
                capacity_payload['embedded_article_adapter_contract_version'] = 1
                capacity_fingerprint = hashlib.sha256(json.dumps(capacity_payload, sort_keys=True).encode()).hexdigest()
            bulk_fingerprint = ""
            if cnyes_apis:
                bulk_payload = dict(fingerprint_payload)
                bulk_payload.pop('verified_short_html_body_contract_version', None)
                bulk_payload.pop('verified_native_prose_spacing_contract_version', None)
                bulk_payload.pop('mobile_canonical_html_contract_version', None)
                bulk_payload.pop('verified_empty_html_outcome_contract_version', None)
                bulk_payload.pop('verified_office_comment_empty_outcome_contract_version', None)
                bulk_payload.pop('verified_native_quote_only_outcome_contract_version', None)
                bulk_payload.pop('verified_native_image_only_outcome_contract_version', None)
                bulk_payload.pop('verified_source_test_article_outcome_contract_version', None)
                bulk_payload.pop('verified_untitled_no_events_bulletin_contract_version', None)
                bulk_payload.pop('verified_source_signature_image_outcome_contract_version', None)
                bulk_payload.pop('visible_headline_body_ownership_contract_version', None)
                bulk_fingerprint = hashlib.sha256(json.dumps(bulk_payload, sort_keys=True).encode()).hexdigest()
            feed_fingerprint = ''
            if fulltext_feeds:
                feed_payload = dict(fingerprint_payload)
                # This version changes only HTML micro-post extraction. Keep
                # every prior RSS rights/config/parser field, so a local HTML
                # repair does not re-fetch healthy historical feed pages.
                feed_payload.pop('verified_small_html_body_contract_version', None)
                feed_fingerprint = hashlib.sha256(json.dumps(feed_payload, sort_keys=True).encode()).hexdigest()
            supplements = entry.get("allow_reviewed_supplements", False)
            # These known-URL scopes may retain their immutable historical
            # supplement flag/receipts. Every new supplement admission still
            # independently requires finite mode in news_fulltext_cohorts;
            # retaining the flag must not freeze normal private URL dispatch.
            retained_private_supplements = (
                acquisition_mode == "continuous"
                and entry.get("private_known_url_scope") == "personal_noncommercial_originals_v1"
                and auth.get("kind") == "personal_research"
                and (ftv_layout or provider == "genetinfo" and domains == ("genetinfo.com",)
                     and auth.get("evidence_url") == "https://www.genetinfo.com/"
                     and entry.get("require_configured_container") is True
                     and entry.get("require_author_attribution") is True))
            if (type(supplements) is not bool
                    or supplements and acquisition_mode != "finite" and not retained_private_supplements):
                raise ValueError("reviewed supplements require an explicit finite scope")
            self.sources[provider] = SourcePolicy(
                provider, entry.get("name", provider), domains, allowed, fingerprint,
                auth, interval, parallelism, tuple(entry.get("article_selectors", [])),
                tuple(entry.get("feeds", [])), tuple(entry.get("sitemaps", [])),
                max(60, int(entry.get("discovery_interval_seconds", 3600))), pattern,
                bool(entry.get("require_configured_container", False)), bool(entry.get("require_author_attribution", False)),
                str(entry.get("title_selector", "")),
                tuple(entry.get("author_selectors", [])), tuple(entry.get("exclude_selectors", [])),
                str(entry.get("required_license_url_prefix", "")),
                tuple(entry.get("license_selectors", ['[rel="license"]'])),
                tuple(entry.get("wordpress_apis", [])),
                bulk_basis, data_fingerprint, html_parser, cnyes_apis, cnyes_start,
                fulltext_feeds, tuple(entry.get("index_pages", [])), index_pattern,
                legacy, float(official_floor), pdf_pattern, pdf_maximum, cnyes_partition, excluded_title, embedded, acquisition_mode,
                tuple(publication_selectors),
                tuple(absent_paths),
                short_body,
                tuple(permitted_authors),
                supplements,
                bulk_fingerprint,
                feed_fingerprint,
                cnyes_index_api,
                index_fingerprint,
                tuple(incremental_indexes),
                metadata_urls, metadata_auth, metadata_fingerprint, metadata_interval,
                cnyes_index_compatibility=index_compatibility,
                http_capacity_source_fingerprint=capacity_fingerprint,
                parser_repair_fingerprint=parser_repair_fingerprint(fingerprint_payload),
                parser_repair_capture_fingerprint=data_fingerprint,
                known_url_only=known_url_only,
            )

    def match(self, url: str) -> SourcePolicy | None:
        host = urlsplit(normalize_url(url)).hostname or ""
        if not hasattr(self, "_domain_providers"):
            self._domain_providers = {domain: source.provider for source in self.sources.values() for domain in source.domains}
        # Longest domain first, exactly as before, in O(host labels) instead
        # of O(all providers). Store IDs, not stale SourcePolicy instances.
        labels = host.split(".")
        for offset in range(len(labels)):
            provider = self._domain_providers.get(".".join(labels[offset:]))
            if provider is not None:
                return self.sources[provider]
        return None

    def require(self, url: str) -> SourcePolicy:
        source = self.match(url)
        if source is None or not source.allowed:
            raise NewsError("awaiting_authorization")
        expiry = source.authorization.get("valid_until")
        if expiry and datetime.fromisoformat(str(expiry).replace("Z", "+00:00")) <= datetime.now(timezone.utc):
            raise NewsError("authorization_expired")
        return source


def capture_usage(source: SourcePolicy) -> dict[str, Any]:
    """Describe this private capture, never certify all downstream uses."""
    kind = source.authorization.get("kind")
    return {
        "capture_policy_version": CAPTURE_POLICY_VERSION,
        "collection_purpose": "personal_research",
        "collection_basis": kind,
        "rights_status": "not_cleared_for_reuse" if kind == "personal_research" else "source_license_scope_only",
        "fair_use_legally_determined": False,
        "commercial_use_enabled": False,
        "training_injection_enabled": False,
    }


def _objects(value: Any):
    if isinstance(value, list):
        for item in value:
            yield from _objects(item)
    elif isinstance(value, dict):
        yield value
        if "@graph" in value:
            yield from _objects(value["@graph"])


_NON_BODY_TAGS = frozenset({"script", "style", "nav", "header", "footer", "aside", "form", "figure", "iframe", "noscript", "svg"})
_SPECIAL_STRING_TAGS = frozenset({"rt", "rp", "template"})  # BS get_text excludes these special string containers.


@lru_cache(maxsize=128)
def _native_exclusions(selectors: tuple[str, ...]):
    """Exact class, ID and nonempty quoted ASCII ID-prefix exclusions only.

    Cnyes's ad-slot prefix used to force a serialization and second DOM parse
    for every existing article tree. CSS's default case-sensitive ID-prefix
    comparison is a string prefix, so it can share the nonmutating walk.
    Escapes, empty prefixes, flags, groups and combinators keep Soup Sieve.
    """
    classes, ids, prefixes = set(), set(), set()
    for selector in selectors:
        simple = re.fullmatch(r"([.#])([A-Za-z_][A-Za-z0-9_-]*)", selector)
        if simple:
            (classes if simple[1] == '.' else ids).add(simple[2])
            continue
        prefix = re.fullmatch(r"\[id\^=([\"'])([A-Za-z_][A-Za-z0-9_-]*)\1\]", selector)
        if not prefix:
            return None
        prefixes.add(prefix[2])
    return frozenset(classes), frozenset(ids), tuple(sorted(prefixes))


def _native_plain_text(fragment: str, exclude_selectors: tuple[str, ...] = ()) -> str:
    """lxml text nodes, with the same node/line rule as BeautifulSoup(lxml).

    Do not drop_tree(): it merges adjacent text around excluded elements and
    changes the original text. Walk once and retain each unblocked tail,
    including comment tails. This is not a regex HTML stripper.
    """
    try:
        root = lxml_html.document_fromstring(fragment)
    except (etree.ParserError, ValueError):
        # An empty/encoding-declared fragment has no useful native document;
        # retain the established parser behavior for these uncommon cases.
        soup = BeautifulSoup(fragment, "lxml")
        for node in soup.find_all(_NON_BODY_TAGS):
            node.decompose()
        for selector in exclude_selectors:
            for node in soup.select(selector):
                node.decompose()
        return "\n".join(line.strip() for line in soup.get_text("\n", strip=True).splitlines() if line.strip())
    classes, ids, prefixes = _native_exclusions(exclude_selectors)
    text, blocked, stack = [], 0, []
    for event, node in etree.iterwalk(root, events=("start", "end", "comment", "pi")):
        if event == "start":
            node_id = node.get("id")
            excluded = (node.tag in _NON_BODY_TAGS or node.tag in _SPECIAL_STRING_TAGS or node_id in ids
                        or bool(prefixes and isinstance(node_id, str) and node_id.startswith(prefixes))
                        or bool(classes and classes.intersection(node.get("class", "").split())))
            stack.append(excluded)
            if excluded:
                blocked += 1
            value = node.text if not blocked else None
        else:
            if event == "end" and stack.pop():
                blocked -= 1
            value = node.tail if not blocked else None
        if value and value.strip():
            text.append(value.strip())
    return "\n".join(line.strip() for line in "\n".join(text).splitlines() if line.strip())


def _plain_text(fragment: str, exclude_selectors: tuple[str, ...] = (), *, parser: str = "html.parser") -> str:
    if "<" not in fragment and "&" not in fragment:
        # No DOM/entity work is necessary for declared plain text. The exact
        # existing line/whitespace rule is preserved; markup still uses a DOM.
        return "\n".join(line.strip() for line in fragment.splitlines() if line.strip())
    if parser == "lxml" and _native_exclusions(exclude_selectors) is not None:
        return _native_plain_text(fragment, exclude_selectors)
    soup = BeautifulSoup(fragment, parser)
    for node in soup.find_all(_NON_BODY_TAGS):
        node.decompose()
    for selector in exclude_selectors:
        for node in soup.select(selector):
            node.decompose()
    text = soup.get_text("\n", strip=True)
    return "\n".join(line.strip() for line in text.splitlines() if line.strip())


def _plain_text_node(node: Tag, exclude_selectors: tuple[str, ...] = (), *, parser: str = "html.parser") -> str:
    """Read an existing article tree once, without serialization/reparsing.

    Leave the document intact for subsequent title, byline and date extraction.
    Only exact simple exclusions take this fast path; context-sensitive CSS retains
    the fragment parser and its original selector semantics.
    """
    exclusions = _native_exclusions(exclude_selectors)
    if exclusions is None:
        return _plain_text(str(node), exclude_selectors, parser=parser)
    classes, ids, prefixes = exclusions

    def excluded(tag):
        node_id = tag.get("id")
        return (tag.name in _NON_BODY_TAGS or node_id in ids
                or bool(prefixes and isinstance(node_id, str) and node_id.startswith(prefixes))
                or bool(classes and classes.intersection(tag.get("class", []))))

    if excluded(node):
        return ""
    texts, stack = [], [iter(node.children)]
    while stack:
        try:
            child = next(stack[-1])
        except StopIteration:
            stack.pop()
            continue
        if isinstance(child, Tag):
            if not excluded(child):
                stack.append(iter(child.children))
        elif type(child) in (NavigableString, CData) and child.strip():
            texts.append(child.strip())
    return "\n".join(line.strip() for line in "\n".join(texts).splitlines() if line.strip())


_SELECTOR_IDENT = r'[A-Za-z_][A-Za-z0-9_-]*'
_SELECTOR_TAG = r'[a-z][a-z0-9-]*'
_SELECTOR_ATTRIBUTE = r'\[[A-Za-z_][A-Za-z0-9_-]*[^\[\]\\]*\]'
_SELECTOR_QUALIFIER = rf'(?:[.#]{_SELECTOR_IDENT}|{_SELECTOR_ATTRIBUTE})'
_SELECTOR_COMPOUND = rf'(?:{_SELECTOR_TAG}(?:{_SELECTOR_QUALIFIER})*|(?:{_SELECTOR_QUALIFIER})+)'
_SELECTOR_CHAIN = re.compile(
    rf'(?:{_SELECTOR_COMPOUND}(?:\s+|\s*[>+~]\s*))*'
    rf'(?P<last>{_SELECTOR_COMPOUND})')


@lru_cache(maxsize=1024)
def _selector_candidate_hint(selector: str):
    """A static prefilter, never cached DOM results or a replacement CSS match.

    Only bounded ASCII compounds/chains without escapes or pseudo-classes are
    recognized. The last compound narrows the leaf set; Soup Sieve still checks
    every ancestor/sibling, attribute operator and HTML case rule. Unknown
    syntax (including groups, :scope and namespaces) keeps the original engine.
    """
    if len(selector) > 4096 or '\\' in selector:
        return None
    chain = _SELECTOR_CHAIN.fullmatch(selector)
    if chain is None:
        return None
    leaf = chain['last']
    tag = re.match(_SELECTOR_TAG, leaf)
    name = tag[0] if tag else None
    # Inspect qualifiers only outside quoted attribute values. A literal
    # '.class' or '#id' inside an href must never become a candidate filter.
    suffix = leaf[tag.end():] if tag else leaf
    qualifier = re.match(rf'([.#])({_SELECTOR_IDENT})', suffix)
    if qualifier:
        return name, 'id' if qualifier[1] == '#' else 'class', qualifier[2]
    attribute = re.match(r'\[([A-Za-z_][A-Za-z0-9_-]*)', suffix)
    if attribute:
        return name, attribute[1].lower(), True
    return name, None, None


def _select_nodes(root: Tag, selector: str, *, limit: int = 0) -> list[Tag]:
    """Narrow terminal candidates, retaining exact full-selector CSS semantics.

    Common article/title/license descendant chains need not run a CSS match on
    every page node. There is no document cache, mutation or approximate body
    fallback; duplicates and document order remain observable.
    """
    # XML/XHTML names and namespaces have distinct case rules. This is an
    # HTML optimization; leave those document grammars to the native engine.
    hint = None if root._is_xml else _selector_candidate_hint(selector)
    if hint is None:
        return root.select(selector, limit=limit)
    matcher = soupsieve.compile(selector)
    name, attribute, value = hint
    candidates = root.find_all(name, attrs={attribute: value} if attribute else {})
    # Stop after the first exact match when the caller only needs existence.
    result = []
    for node in candidates:
        if matcher.match(node):
            result.append(node)
            if limit and len(result) >= limit:
                break
    return result


def _select_first(root: Tag, selector: str) -> Tag | None:
    return next(iter(_select_nodes(root, selector, limit=1)), None)


def _has_paywall(root: Tag) -> bool:
    def marker(node):
        classes = node.get('class', [])
        classes = classes.split() if isinstance(classes, str) else classes
        return ('paywall' in classes or 'data-paywall' in node.attrs
                or node.name == 'form' and isinstance(node.get('action'), str) and 'login' in node['action'])
    return root.find(marker) is not None


def _globalvoices_declared_news_headline(soup: BeautifulSoup, source: SourcePolicy, page_url: str | None) -> bool:
    """A verified news title is not an access-denial response.

    Native news uses this phrase in both prefix and suffix headlines. Its word
    position is not access evidence. Require the same current canonical/body,
    visible header/footer author profiles, aware clock and per-article license
    used by the publisher's exceptional-body identity contract. Other title
    markers, explicit challenges and HTTP/robots refusals remain binding.
    """
    if source.provider != "globalvoices" or not page_url or not source.title_selector:
        return False
    if soup.select_one('#challenge-form, #cf-challenge-running, .cf-challenge') is not None:
        return False
    headlines = _select_nodes(soup, source.title_selector)
    if len(headlines) != 1:
        return False
    headline = re.sub(r'\s+', ' ', headlines[0].get_text(' ', strip=True)).casefold()
    title = re.sub(r'\s+', ' ', soup.title.get_text(' ', strip=True)).casefold() if soup.title else ''
    if not re.search(r'\baccess denied\b', headline) or title != headline + ' · global voices':
        return False
    bodies = [node for selector in source.selectors for node in _select_nodes(soup, selector)]
    if len(bodies) != 1 or len(_plain_text_node(bodies[0], source.exclude_selectors, parser=source.html_parser)) < 100:
        return False
    from downloader.news_fulltext_publishers import _globalvoices_visible_article_identity

    identity = _globalvoices_visible_article_identity(soup, source, page_url=page_url)
    if identity is None or identity[0] is not bodies[0]:
        return False
    sitekeys = soup.select('[data-sitekey]')
    if sitekeys:
        # Seven retained native public templates have this exact comment-only
        # widget, outside the declared article. It verifies posting a comment,
        # not reading the already-visible licensed body. Never solve/submit it
        # or exempt an article gate, an unknown widget or ambiguous ownership.
        widgets = soup.select('#respond > .respond-content > form#commentform > h-captcha.h-captcha[data-sitekey]')
        scopes, responds, forms = soup.select('#page-content'), soup.select('#respond'), soup.select('form#commentform')
        if (len(sitekeys) != 1 or len(widgets) != 1 or sitekeys[0] is not widgets[0]
                or len(scopes) != 1 or len(responds) != 1 or len(forms) != 1
                or bodies[0].find_parent(id='page-content') is not scopes[0]
                or responds[0].find_parent(id='page-content') is not scopes[0]
                or responds[0].find_parent(class_='post-container') is not None):
            return False
    return True


def public_article_document(payload: bytes, source: SourcePolicy, *, page_url: str | None = None) -> BeautifulSoup:
    """Shared public-response gate, also before source-declared canonical resolution."""
    soup = BeautifulSoup(payload, source.html_parser)
    page_title = soup.title.get_text(" ", strip=True).lower() if soup.title else ""
    challenge_markers = {marker for marker in ("just a moment", "access denied", "captcha", "verify you are human") if marker in page_title}
    if challenge_markers and not (challenge_markers == {'access denied'}
                                  and _globalvoices_declared_news_headline(soup, source, page_url)):
        raise NewsError("access_challenge")
    if source.provider == "cnyes" and page_title == "404 not found":
        # Verified HTTP-200 soft-404 response, not a transient missing selector.
        # Do not classify a real article merely mentioning 404 as absent.
        visible = re.sub(r"[\s\u200b-\u200d\ufeff]+", " ", soup.get_text(" ", strip=True)).strip().lower()
        if (visible in {"404 not found", "404 not found 404 not found"}
                and not soup.find("script", type="application/ld+json")
                and not _select_first(soup, '#article-container')):
            raise NewsError("source_not_found")
    # CNEWS's reviewed adapter checks every access marker before identity/body
    # parsing, exempting only its external navigation modal. Other sources
    # retain the original whole-page gate; do not scan the CNEWS page twice.
    if source.embedded_article_layout != "cnews_visible_article_v1" and _has_paywall(soup):
        raise NewsError("paywall_or_login")
    return soup


def extract_article(payload: bytes, source: SourcePolicy, *, page_url: str | None = None) -> dict[str, Any]:
    """Prefer declared body over summaries. No boilerplate/page-wide fallback."""
    soup = public_article_document(payload, source, page_url=page_url)
    fed_layout_version = _FED_RELEASE_LAYOUT_VERSIONS.get(source.legacy_release_layout, 0)
    if source.provider == "globalvoices":
        notice = _select_first(soup, '.post-header .post-tagline')
        text = notice.get_text(' ', strip=True) if notice else ''
        if text == "This post was taken down for security reasons":
            raise NewsError("source_body_removed")
        removed = re.fullmatch(r'This article was taken down for security reasons on ([A-Za-z]+ [0-9]{1,2}, [0-9]{4})[.]', text)
        if removed:
            try:
                datetime.strptime(removed[1], '%B %d, %Y')
            except ValueError:
                pass  # An unrecognized notice remains an explicit parser gap.
            else:
                raise NewsError("source_body_removed")
    licenses = [str(tag.get("href", "")) for selector in source.license_selectors for tag in _select_nodes(soup, selector)]
    article: dict[str, Any] = {}
    license_basis = None
    article_scope = None
    if source.embedded_article_layout == "einfo_next_post_v1":
        from downloader.news_fulltext_publishers import einfo_metadata
        article, license_basis = einfo_metadata(soup, page_url=page_url)
    elif source.embedded_article_layout == "cmnews_visible_article_v1":
        from downloader.news_fulltext_publishers import cmnews_visible_article
        article, article_scope = cmnews_visible_article(soup, page_url=page_url)
    elif source.embedded_article_layout == "tainan_visible_news_v1":
        from downloader.news_fulltext_taiwan_pages import tainan_visible_news
        article, article_scope = tainan_visible_news(soup, page_url=page_url)
    elif source.embedded_article_layout == "macau_visible_news_v1":
        from downloader.news_fulltext_taiwan_pages import macau_visible_news
        article, article_scope = macau_visible_news(soup, page_url=page_url)
    elif source.embedded_article_layout in {"kmdn_visible_article_v1", "kmdn_visible_article_v2"}:
        from downloader.news_fulltext_taiwan_pages import kmdn_visible_article
        article, article_scope = kmdn_visible_article(soup, page_url=page_url,
            allow_student_essay=source.embedded_article_layout == "kmdn_visible_article_v2")
    elif source.embedded_article_layout == 'coolloud_visible_original_v1':
        from downloader.news_fulltext_taiwan_pages import coolloud_visible_original
        article, article_scope, license_basis = coolloud_visible_original(soup, page_url=page_url)
    elif source.embedded_article_layout == "cnews_visible_article_v1":
        from downloader.news_fulltext_cnews_cmmedia import cnews_visible_article
        article, article_scope = cnews_visible_article(soup, page_url=page_url)
    elif source.embedded_article_layout == "cmmedia_visible_article_v1":
        from downloader.news_fulltext_cnews_cmmedia import cmmedia_visible_article
        article, article_scope = cmmedia_visible_article(soup, page_url=page_url)
    elif source.embedded_article_layout == "nownews_visible_article_v1":
        from downloader.news_fulltext_public_originals import nownews_visible_article
        article, article_scope = nownews_visible_article(soup, page_url=page_url)
    elif source.embedded_article_layout == "ftv_visible_article_v1":
        from downloader.news_fulltext_public_originals import ftv_visible_article
        article, article_scope = ftv_visible_article(soup, page_url=page_url)
    elif source.embedded_article_layout in {'taiwanhot_visible_article_v1', 'taiwanhot_visible_article_v2', 'taiwanhot_visible_article_v3'}:
        from downloader.news_fulltext_public_originals import taiwanhot_visible_article
        article, article_scope = taiwanhot_visible_article(soup, page_url=page_url,
            native_views=source.embedded_article_layout != 'taiwanhot_visible_article_v1',
            route_views=source.embedded_article_layout == 'taiwanhot_visible_article_v3')
    elif source.embedded_article_layout in {"voa_original_article_v1", "voa_reviewed_private_article_v2"}:
        from downloader.news_fulltext_public_originals import voa_original_article
        article, article_scope = voa_original_article(soup, source, page_url=page_url)
    elif source.embedded_article_layout == 'tfc_visible_fact_check_v1':
        from downloader.news_fulltext_fact_checks import tfc_visible_fact_check
        article, article_scope = tfc_visible_fact_check(soup, page_url=page_url)
    elif source.embedded_article_layout == 'nccu_visible_news_v1':
        from downloader.news_fulltext_nccu import nccu_visible_news
        article, article_scope = nccu_visible_news(soup, page_url=page_url)
    elif source.embedded_article_layout == 'taichung_visible_news_v1':
        from downloader.news_fulltext_taiwan_pages import taichung_visible_news
        article, article_scope = taichung_visible_news(soup, page_url=page_url)
    elif source.embedded_article_layout == 'changhua_visible_news_v1':
        from downloader.news_fulltext_taiwan_pages import changhua_visible_news
        article, article_scope = changhua_visible_news(soup, page_url=page_url)
    elif source.embedded_article_layout == 'eventsinfocus_visible_news_v1':
        from downloader.news_fulltext_independent import eventsinfocus_visible_news
        article, article_scope = eventsinfocus_visible_news(soup, page_url=page_url)
    elif source.embedded_article_layout == 'ftnn_visible_article_v1':
        from downloader.news_fulltext_ftnn import ftnn_visible_article
        article, article_scope = ftnn_visible_article(soup, page_url=page_url)
    elif source.embedded_article_layout in {'tvbs_visible_article_v1', 'tvbs_visible_article_v2'}:
        from downloader.news_fulltext_tvbs import tvbs_visible_article
        article, article_scope = tvbs_visible_article(soup, page_url=page_url,
            adapter_contract_version=2 if source.embedded_article_layout.endswith('_v2') else 1)
    elif source.embedded_article_layout == 'ettoday_visible_article_v1':
        from downloader.news_fulltext_ettoday import ettoday_visible_article
        article, article_scope = ettoday_visible_article(soup, page_url=page_url)
    if source.required_license_url_prefix and not license_basis and not any(url.startswith(source.required_license_url_prefix) for url in licenses):
        raise NewsError("article_license_not_verified")
    canonical = _select_first(soup, 'link[rel="canonical"]')
    expected_url = page_url or (canonical.get("href") if canonical else None)
    native_canonical_article = None
    native_parts = urlsplit(expected_url or '')
    if source.provider == 'cnyes' and (native_parts.hostname == 'm.cnyes.com'
            or native_parts.hostname == 'news.cnyes.com' and native_parts.query == 'exp=a'):
        from downloader.news_fulltext_publishers import cnyes_native_canonical_article
        native_canonical_article = cnyes_native_canonical_article(soup, source, page_url=expected_url)
        if native_canonical_article is not None:
            article = native_canonical_article
    if article_scope is not None:
        # Metadata was matched against this visible article and canonical.
        # Body/title/byline/date fallback must not inspect neighboring stories.
        soup = article_scope
    if source.provider == 'moneyweekly':
        from downloader.news_fulltext_publishers import moneyweekly_article_identity
        moneyweekly_article_identity(soup, page_url=expected_url)
    if source.provider == 'pansci':
        # Infinite-scroll pages embed several complete stories. Establish the
        # first story's source identity before any body/byline/date extraction.
        scope = soup.select_one('#load_post_1[data-type="post"]')
        match = re.fullmatch(r'/archives/([0-9]+)/?', urlsplit(expected_url or '').path)
        if scope is None or match is None or str(scope.get('data-post_id')) != match[1]:
            raise NewsError('article_identity_not_verified')
        try:
            identity_matches = normalize_url(str(scope.get('data-url', ''))) == normalize_url(expected_url)
        except NewsError:
            identity_matches = False
        if not identity_matches:
            raise NewsError('article_identity_not_verified')
    for script in ([] if source.embedded_article_layout or native_canonical_article is not None else _select_nodes(soup, 'script[type="application/ld+json"]')):
        try:
            candidates = _objects(json.loads(script.get_text()))
            for candidate in candidates:
                types = candidate.get("@type", [])
                if isinstance(types, str):
                    types = [types]
                if not any(str(t).rsplit("/", 1)[-1] in {"Article", "NewsArticle", "BlogPosting", "ReportageNewsArticle"} for t in types):
                    continue
                identity = candidate.get("mainEntityOfPage") or candidate.get("url") or candidate.get("@id")
                if isinstance(identity, dict):
                    identity = identity.get("@id") or identity.get("url")
                if expected_url and isinstance(identity, str):
                    try:
                        if normalize_url(urljoin(expected_url, identity)) != normalize_url(expected_url):
                            continue  # Related-story schema is not this page's body.
                    except NewsError:
                        continue
                if str(candidate.get("isAccessibleForFree", "true")).lower() == "false":
                    raise NewsError("paywall_or_login")
                if candidate.get("articleBody"):
                    article = candidate
                    break
                if not article:
                    article = candidate
        except (json.JSONDecodeError, TypeError):
            continue
        if article.get("articleBody"):
            break
    body = _plain_text(str(article.get("articleBody", "")), parser=source.html_parser)
    method = "jsonld_article_body"
    if not body and source.provider == 'cnyes':
        from downloader.news_fulltext_publishers import cnyes_verified_image_only_body
        if cnyes_verified_image_only_body(soup, source, article, page_url=expected_url):
            raise NewsError('source_body_image_only')
    image_only_container = False
    short_configured_body = False
    verified_untitled_html_body = False
    if native_canonical_article is not None:
        method = 'cnyes_visible_canonical_alias_v1'
        short_configured_body = 0 < len(body) < 100
    if not body:
        selectors = source.selectors if source.require_configured_container else (*source.selectors, '[itemprop="articleBody"]', "article")
        for selector in selectors:
            nodes = _select_nodes(soup, selector)
            text = "\n\n".join(_plain_text_node(node, source.exclude_selectors, parser=source.html_parser) for node in nodes)
            if nodes and not text and any(node.find('img') is not None for node in nodes):
                image_only_container = True
            if (source.provider == 'cmnews' and source.embedded_article_layout == 'cmnews_visible_article_v1'
                    and len(nodes) == 1 and len(text) < 100
                    and len(nodes[0].select('figure.image img')) >= 2):
                # Verified public editor body is diagrams plus a short product
                # CTA. The canonical text-only collector must not call that
                # CTA a complete article or repeatedly retry a valid image page.
                image_only_container = True
            if source.provider == 'globalvoices' and 0 < len(text) < 100:
                from downloader.news_fulltext_publishers import (
                    globalvoices_verified_small_body, globalvoices_verified_untitled_body,
                )
                verified = globalvoices_verified_small_body(soup, source, text, page_url=page_url)
                if not verified:
                    verified = globalvoices_verified_untitled_body(soup, source, text, page_url=page_url)
                    verified_untitled_html_body = bool(verified)
                if verified:
                    article = verified
                    body, method, short_configured_body = text, 'css:' + selector, True
                    break
            if len(text) >= 100 or (source.allow_short_configured_body and len(nodes) == 1 and text):
                if (source.provider == 'taiwanhot' and source.embedded_article_layout == 'taiwanhot_visible_article_v3'
                        and len(text) < 100):
                    from downloader.news_fulltext_public_originals import taiwanhot_verified_short_prose
                    if not taiwanhot_verified_short_prose(nodes[0], text, source):
                        continue
                body, method = text, "css:" + selector
                short_configured_body = len(text) < 100
                break
    legacy_publication = None
    if not body and fed_layout_version >= 1 and page_url and re.fullmatch(
            r"/boarddocs/press/(?:[a-z]+/)?[0-9]{4}/[0-9a-z]+/(?:[^/]+[.]htm)?", urlsplit(page_url).path):
        # Verified old Board release layout: a declared Release Date, then a
        # single 600-pixel article table. Never select all <body>/<p> text or
        # mistake the archive/navigation/last-update footer for the release.
        clock = soup.find(string=re.compile(r"^\s*Release Date:\s*", re.I))
        if not clock and fed_layout_version >= 4 and soup.body:
            # Verified 1999/2000 releases declare their date in a direct font
            # header, without 'Release Date:'. Never use the footer's update
            # date, the URL date, or a date mentioned in the announcement.
            date_header = re.compile(r"^(?:FOR IMMEDIATE RELEASE|Washington,\s*D[.]C[.])\s+"
                                     r"((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+[0-9]{1,2},\s+[0-9]{4})$", re.I)
            for node in soup.body.find_all("font", recursive=False):
                if date_header.fullmatch(node.get_text(" ", strip=True)):
                    clock = node
                    break
        containers = soup.select('body > table[width="600"]')
        if fed_layout_version == 2:
            # Live 1996 releases split the declaration and its regulatory
            # annexes across several 555/600-pixel direct article tables.
            # Do not duplicate nested tables or capture 560-pixel release
            # headers, archive links, footer update dates, or whole-page text.
            containers = soup.select('body > table[width="555"], body > table[width="600"]')
        valid_tables = bool(containers and all(node.find("p") for node in containers))
        if fed_layout_version >= 3 and clock and soup.body:
            # Source-backed 1996/1997 releases have list-only and statistical
            # annex tables. Requiring a <p> in *every* table discarded the whole
            # valid release. Bound direct source tables by the declared release
            # header and actual archive/footer marker, not page-wide text.
            containers, after_clock = [], False
            for node in soup.body.children:
                if not getattr(node, "name", None):
                    continue
                if node is clock or clock in node.descendants:
                    after_clock = True
                    continue
                if not after_clock:
                    continue
                text = node.get_text(" ", strip=True)
                if (node.name == "p" and re.fullmatch(r"(?:[0-9]{4}\s+)?Other announcements", text, re.I)
                        or node.name == "font" and str(node.get("size")) == "-1" and "Last update:" in text):
                    break
                if node.name == "table" and str(node.get("width")) in {"555", "600", "650"} and text:
                    containers.append(node)
            valid_tables = bool(containers)
        if clock and valid_tables and (fed_layout_version != 1 or len(containers) == 1):
            if isinstance(clock, Tag):
                legacy_publication = date_header.fullmatch(clock.get_text(" ", strip=True)).group(1)
            else:
                legacy_publication = re.sub(r"^\s*Release Date:\s*", "", str(clock), flags=re.I).strip()
            for container in containers:
                for link in container.select('a[href="#pagetop"]'):
                    link.decompose()
            body = "\n\n".join(_plain_text_node(node, source.exclude_selectors, parser=source.html_parser) for node in containers)
            method = source.legacy_release_layout
        if not body and fed_layout_version >= 5:
            from downloader.news_fulltext_publishers import fed_additional_legacy_release
            extra = fed_additional_legacy_release(soup, table_headers=fed_layout_version >= 6, table_rows=fed_layout_version >= 7)
            if extra:
                legacy_publication, containers, legacy_title = extra
                body = "\n\n".join(_plain_text_node(node, source.exclude_selectors, parser=source.html_parser) for node in containers)
                method = source.legacy_release_layout
                if legacy_title:
                    article['headline'] = legacy_title
    # v10 delegates implementation notes to the reviewed main-column adapter
    # for one semantic date check, rather than keeping two competing clocks.
    implementation_note = bool(page_url and re.fullmatch(
        r'/newsevents/pressreleases/monetary[0-9]{8}[a-z][0-9]+[.]htm', urlsplit(page_url).path))
    if not body and fed_layout_version >= 7 and not (fed_layout_version >= 10 and implementation_note):
        from downloader.news_fulltext_publishers import fed_bounded_document
        extra = fed_bounded_document(soup, page_url=page_url)
        if extra:
            legacy_publication, containers, article['headline'] = extra
            body = "\n\n".join(_plain_text_node(node, source.exclude_selectors, parser=source.html_parser) for node in containers)
            method = 'fed_bounded_document_v1'
    if not body and fed_layout_version >= 8:
        from downloader.news_fulltext_publishers import fed_extended_document
        extra = fed_extended_document(soup, page_url=page_url, additional_layouts=fed_layout_version >= 9,
                                      historical_variants=fed_layout_version >= 10)
        if extra:
            legacy_publication = extra['published_at_raw']
            article.update(headline=extra['title'], published_time_basis=extra['published_time_basis'],
                           body_completeness='publisher_document_text_with_tables; images_not_OCRed')
            body = "\n\n".join(_plain_text_node(node, source.exclude_selectors, parser=source.html_parser)
                               for node in extra['containers'])
            method = 'fed_bounded_document_v4' if fed_layout_version >= 10 else 'fed_bounded_document_v3' if fed_layout_version == 9 else 'fed_bounded_document_v2'
    # A description, headline or teaser is not accepted as a complete body.
    if not body and source.provider == 'globalvoices':
        from downloader.news_fulltext_publishers import globalvoices_verified_nontext_body
        outcome = globalvoices_verified_nontext_body(soup, source, page_url=page_url)
        if outcome:
            raise NewsError(outcome)
    if not body and image_only_container and source.provider in {'genetinfo', 'cmnews'}:
        raise NewsError('source_body_image_only')  # Text-only scope; no fake body, media capture or silent OCR.
    if source.provider == 'cnyes' and body in ('', 'null'):
        from downloader.news_fulltext_publishers import cnyes_verified_empty_body
        if cnyes_verified_empty_body(soup, source, article, page_url=expected_url):
            raise NewsError('source_article_empty_template')
    verified_globalvoices_micro_post = (source.provider == 'globalvoices' and short_configured_body
        and str(article.get('body_completeness', '')).startswith('verified_publisher_short_body;'))
    # Word count is a heuristic, not evidence that a publisher's independently
    # verified complete micro-post lacks text. Other short/unknown bodies keep
    # the existing guard; navigation, credit and media-only posts never qualify.
    nontrivial_text_missing = (len(body.split()) < 8 and not re.search(r"[\u3400-\u9fff]", body)
                              and not verified_globalvoices_micro_post)
    if source.provider == 'cnyes' and method == 'jsonld_article_body' and 0 < len(body) < 100:
        from downloader.news_fulltext_publishers import cnyes_verified_test_article
        if cnyes_verified_test_article(soup, source, article, body, page_url=expected_url):
            raise NewsError('source_article_test_only')
    if (source.provider == 'cnyes' and method == 'jsonld_article_body'
            and 0 < len(body) < 100 and nontrivial_text_missing):
        from downloader.news_fulltext_publishers import cnyes_verified_quote_only_body, cnyes_verified_short_prose_spacing
        if cnyes_verified_quote_only_body(soup, source, article, body, page_url=expected_url):
            raise NewsError('source_body_quote_only')
        spaced_body = cnyes_verified_short_prose_spacing(soup, source, article, body, page_url=expected_url)
        if spaced_body is not None:
            body, method = spaced_body, 'cnyes_visible_short_prose_spacing_v1'
            short_configured_body = True
            nontrivial_text_missing = False  # At least eight actually observed words, not a word-count exemption.
            article['article_identity_basis'] = 'canonical_url_newsId_free_payment0; declared_clock_and_whole_visible_container_match_full_content'
            article['body_completeness'] = ('verified_publisher_short_body; '
                                          'whole_visible_container_matches_current_full_prose_spacing')
    if (source.provider == 'cnyes' and method == 'jsonld_article_body'
            and len(body) >= 100 and nontrivial_text_missing):
        from downloader.news_fulltext_publishers import cnyes_verified_prose_spacing
        spaced_body = cnyes_verified_prose_spacing(soup, source, article, body, page_url=expected_url)
        if spaced_body is not None:
            body, method = spaced_body, 'cnyes_visible_prose_spacing_v1'
            nontrivial_text_missing = False  # The whole observed prose has at least eight words.
            article['article_identity_basis'] = 'canonical_url_newsId_free_payment0; declared_clock_and_whole_visible_container_match_full_content'
            article['body_completeness'] = ('verified_publisher_body; '
                                          'whole_visible_container_matches_current_full_prose_spacing')
    if (source.provider == 'cnyes' and method == 'jsonld_article_body'
            and 0 < len(body) < 100 and not nontrivial_text_missing):
        from downloader.news_fulltext_publishers import (
            cnyes_verified_short_body, cnyes_verified_untitled_no_events_bulletin,
        )
        verified_body = cnyes_verified_short_body(soup, source, article, body, page_url=expected_url)
        if verified_body is None and article.get('headline') == '':
            verified_body = cnyes_verified_untitled_no_events_bulletin(soup, source, article, body, page_url=expected_url)
            verified_untitled_html_body = verified_body is not None
        if verified_body:
            short_configured_body = True
            if verified_body != body:
                body, method = verified_body, ('cnyes_visible_untitled_no_events_bulletin_v1'
                                               if verified_untitled_html_body else 'cnyes_visible_short_table_v1')
            article['article_identity_basis'] = 'canonical_url_newsId_free_payment0; declared_clock_and_whole_visible_container_match_full_content'
            article['body_completeness'] = ('verified_publisher_short_body; '
                                          'whole_visible_container_and_table_cells_match_declared_full_content')
            if verified_untitled_html_body:
                article['body_completeness'] += '; source_visible_title_empty; source_explicit_no_events_bulletin_not_missing_data'
    body_too_short = len(body) < 100 and not short_configured_body
    if not body or body_too_short or nontrivial_text_missing:
        raise NewsError("body_not_found")
    h1 = _select_first(soup, source.title_selector) if source.title_selector else soup.find("h1")
    title = ('' if source.provider == 'cnyes' and verified_untitled_html_body else
             str(article.get("headline") or (h1.get_text(" ", strip=True) if h1 else "")))
    if not title and not (source.provider == 'cnyes' and verified_untitled_html_body):
        title_meta = _select_first(soup, 'meta[property="og:title"]')
        title = str(title_meta.get("content", "")) if title_meta else ""
    if not title and method in _FED_RELEASE_LAYOUT_VERSIONS and soup.title:
        title = soup.title.get_text(" ", strip=True)
    if source.provider == 'globalvoices' and not title and not verified_untitled_html_body:
        from downloader.news_fulltext_publishers import globalvoices_verified_untitled_body
        verified = globalvoices_verified_untitled_body(soup, source, body, page_url=page_url)
        if verified:
            article.update(verified)
            verified_untitled_html_body = True
    if source.excluded_article_title_pattern and re.search(source.excluded_article_title_pattern, title):
        raise NewsError("article_rights_or_scope_excluded")
    authors = article.get("author") or []
    if not isinstance(authors, list):
        authors = [authors]
    author_names = [name.strip() for a in authors
                    if isinstance(name := a.get("name") if isinstance(a, dict) else a, str) and name.strip()]
    source_visible_name_empty = (source.provider == 'cmnews' and source.embedded_article_layout == 'cmnews_visible_article_v1'
                                 and article.get('attribution_status') == 'source_visible_byline_name_empty')
    if not any(author_names) and not source_visible_name_empty:
        author_selectors = source.author_selectors or ('[rel="author"]', '[itemprop="author"]')
        author_names = [n.get_text(" ", strip=True) for selector in author_selectors for n in _select_nodes(soup, selector)]
    author_names = list(dict.fromkeys(name.strip() for name in author_names if name.strip()))
    if source.permitted_author_names:
        # A publisher's private-use policy need not cover syndicated writers.
        # This gate uses the reviewed current-article byline, never schema from
        # a related story or a matching name in the page-wide/footer text.
        author_names = list(dict.fromkeys(name
                            for selector in source.author_selectors for n in _select_nodes(soup, selector)
                            if (name := n.get_text(' ', strip=True))))
        if not author_names or any(name not in source.permitted_author_names for name in author_names):
            raise NewsError('article_rights_or_scope_excluded')
    if (source.require_configured_container and not title and _FED_RELEASE_LAYOUT_VERSIONS.get(method, 0) < 3
            and not verified_untitled_html_body):
        raise NewsError('article_title_not_verified')
    if source.require_author_attribution and not any(author_names) and not source_visible_name_empty:
        raise NewsError("article_attribution_not_verified")
    published = article.get("datePublished") or legacy_publication
    if not published and source.provider != 'pansci':
        meta = _select_first(soup, 'meta[property="article:published_time"]')
        published = meta.get("content") if meta else None
    if not published and source.publication_selectors:
        clocks = {str(n.get('datetime') or n.get('content') or n.get_text(' ', strip=True)).strip()
                  for selector in source.publication_selectors for n in _select_nodes(soup, selector)} - {''}
        if len(clocks) == 1:
            published = clocks.pop()  # Exact declared text; never URL, scrape or related-story date.
    return {
        "title": title, "title_basis": ('source_visible_title_empty_verified_numeric_article'
                                        if verified_untitled_html_body else
                                        "publisher_declared_title" if title else "unknown_source_title_not_found"),
        "author_attribution_detail_json": article.get("author_attribution_detail_json"),
        "verified_image_captions_json": article.get("verified_image_captions_json"),
        "verified_article_notices_json": article.get("verified_article_notices_json"),
        "body": body, "body_sha256": hashlib.sha256(body.encode()).hexdigest(),
        "authors_json": json.dumps(author_names, ensure_ascii=False),
        "attribution_status": ("source_declared_author_visible_signature_empty"
                               if source.provider == 'cnyes' and any(author_names)
                               and article.get('attribution_status') == 'source_declared_author_visible_signature_empty' else
                               "source_byline_present" if any(author_names) else
                               "source_visible_byline_name_empty" if source_visible_name_empty else "source_byline_not_found"),
        "published_at_raw": str(published) if published else None,
        "published_time_basis": article.get("published_time_basis") or (
            "publisher_declared_date_only; no_intraday_time_inferred" if legacy_publication else "publisher_declared_clock_or_unknown"),
        "article_identity_basis": article.get("article_identity_basis"),
        **({"source_native_article_id": article["source_native_article_id"]}
           if source.embedded_article_layout == 'taiwanhot_visible_article_v3' else {}),
        "modified_at_raw": str(article["dateModified"]) if article.get("dateModified") else None,
        "declared_canonical_url": canonical.get("href") if canonical else None,
        "declared_license_urls_json": json.dumps(licenses, ensure_ascii=False),
        "license_verification_basis": license_basis or "article_declared_license_or_source_policy",
        "per_article_license_verified": bool(source.required_license_url_prefix and not license_basis),
        "extraction_method": method,
        "body_completeness": article.get("body_completeness") or (
            "declared_article_body" if method == "jsonld_article_body" else "article_container_unverified"),
        "historical_point_in_time": False,
    }

"""Full-text enrichment downstream of the existing metadata owners.

No FinLab/FinMind API calls, browser automation, paywall bypass or public body
gateway. Different publishers can run concurrently under independent budgets.
"""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from collections import deque
from contextlib import contextmanager, nullcontext
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import re
import shutil
import sqlite3
import time
from urllib.parse import urlsplit, urlunsplit

from bs4 import BeautifulSoup
from stockagent.data.news_source_catalog import load_news_configuration

from downloader.artifact_io import atomic_write_parquet, atomic_write_json, table_from_records
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_bulk import (cnyes_history_priority, cnyes_pagination, cnyes_records, cnyes_scope, cnyes_url,
                                          BULK_ENVELOPE_CONTINUATION_CONTRACT_VERSION, BULK_RESTRICTED_ITEM_ERRORS,
                                          cnyes_unseeded_intervals,
                                          wordpress_base, wordpress_page, wordpress_records, wordpress_url)
from downloader.news_fulltext_policy import (CONTRACT_VERSION, NewsError, PolicyRegistry, capture_usage,
                                             cnyes_bulk_fingerprint, extract_article, normalize_url,
                                             ETTODAY_LAYOUT_VERSIONS, POCKET_LAYOUT_VERSIONS, BASTILLE_LAYOUT_VERSIONS,
                                             REQUEST_DEFERRAL_CODES)
from downloader.news_fulltext_feed import (XML_BODY_METADATA_FALLBACK, XML_BODY_NO_USABLE_IDENTITY,
    feed_page, feed_records, feed_url)
from downloader.news_fulltext_queue import NewsQueue, article_claim_witness, unleased_article_head_witness, utc_now


# Small recovery journals can share the registration commit. For larger
# envelopes, compress outside the writer transaction so another provider does
# not wait on compression of an entire source response.
_BULK_INLINE_JOURNAL_MAX_BYTES = 1 << 20
_RETAINED_FAILURE_IDLE_POLL_SECONDS = 10
# A durable checkpoint or local retry removes the attempted item from the
# ready set; the existing per-slice bounds and provider dispatch provide
# fairness without an extra fixed pause between ready slices.
_RETAINED_FAILURE_CONTINUATION_MIN_SECONDS = 0.0
# Bounded local queue/worker setup before a send. The HTTP reservation still
# enforces the real interval; retain another free worker for other providers.
_SHORT_PACING_PREFETCH_SECONDS = .01


def validated_provider_priority(config, registry):
    """Validate order only; a named provider gains no request permission."""
    priority = config.get('provider_priority', [])
    if (type(priority) is not list
            or any(type(provider) is not str or provider not in registry.sources for provider in priority)
            or len(priority) != len(set(priority))):
        raise ValueError('invalid news provider_priority')
    return list(priority)


def load_config(path: Path, *, repo: Path | None = None):
    config = load_news_configuration(path)
    for key, value in {"batch_raw_response_bytes": 16 << 20, "batch_flush_seconds": 30,
                       "service_cycle_seconds": 180, "service_idle_max_seconds": 60,
                       "transport_breaker_failures": 3, "sitemap_article_ratio": 8,
                       "missing_body_recheck_burst": 8, "dated_body_undated_burst": 8,
                       "pending_body_retry_burst": 8, "stored_body_retry_burst": 8,
                       "bulk_article_burst": 16, "cnyes_primary_body_burst": 8}.items():
        config.setdefault(key, value)
    config.setdefault('original_url_resolution_batch', 256)
    config.setdefault('original_url_resolution_slice_seconds', 0.04)
    config.setdefault('original_url_resolution_interval_seconds', 0.25)
    if (type(config['original_url_resolution_batch']) is not int or not 1 <= config['original_url_resolution_batch'] <= 2048
            or type(config['original_url_resolution_slice_seconds']) not in (int, float)
            or not math.isfinite(config['original_url_resolution_slice_seconds']) or not 0.001 <= config['original_url_resolution_slice_seconds'] <= 0.1
            or type(config['original_url_resolution_interval_seconds']) not in (int, float)
            or not math.isfinite(config['original_url_resolution_interval_seconds']) or not 0.05 <= config['original_url_resolution_interval_seconds'] <= 60):
        raise ValueError('invalid bounded original URL resolution settings')
    repo = repo or Path(__file__).resolve().parents[1]
    from downloader.news_fulltext_cnyes_priority import validate_spec as validate_cnyes_original_priority_spec
    config['cnyes_original_audit_priority_plan'] = validate_cnyes_original_priority_spec(
        config.get('cnyes_original_audit_priority_plan'), repo)
    config['cnyes_accepted_batch_priority_plan'] = validate_cnyes_original_priority_spec(
        config.get('cnyes_accepted_batch_priority_plan'), repo)
    from downloader.news_fulltext_retained_handoff import validate_spec as validate_retained_link_handoff_spec
    config['retained_link_handoff'] = validate_retained_link_handoff_spec(config.get('retained_link_handoff'), repo)
    if config.get("collection_purpose", "personal_research") != "personal_research":
        raise ValueError("This collector supports local personal research only")
    for key in ("workers", "request_timeout_seconds", "response_deadline_seconds", "max_response_bytes", "max_attempts",
                "lease_seconds", "min_free_disk_bytes", "batch_articles", "batch_text_bytes", "robots_ttl_seconds",
                "metadata_refresh_seconds", "batch_raw_response_bytes", "batch_flush_seconds",
                "service_cycle_seconds", "service_idle_max_seconds", "transport_breaker_failures", "sitemap_article_ratio",
                "missing_body_recheck_burst", "dated_body_undated_burst", "pending_body_retry_burst", "stored_body_retry_burst", "bulk_article_burst"):
        if isinstance(config.get(key), bool) or not isinstance(config.get(key), (int, float)) or not math.isfinite(config[key]) or config[key] <= 0:
            raise ValueError("invalid news fulltext setting: " + key)
    for key in ("workers", "max_attempts", "lease_seconds", "max_response_bytes", "batch_articles", "batch_raw_response_bytes", "transport_breaker_failures", "sitemap_article_ratio", "missing_body_recheck_burst", "dated_body_undated_burst", "pending_body_retry_burst", "stored_body_retry_burst", "bulk_article_burst"):
        if not isinstance(config[key], int):
            raise ValueError("news count/byte/worker limits must be integers: " + key)
    if config["workers"] > 16 or config["lease_seconds"] < config["response_deadline_seconds"] * 6:
        raise ValueError("unbounded workers or lease shorter than redirect deadline")
    if max(config["missing_body_recheck_burst"], config["dated_body_undated_burst"],
           config["pending_body_retry_burst"], config["stored_body_retry_burst"], config["bulk_article_burst"]) > 128:
        raise ValueError("unbounded article dispatch fairness burst")
    if type(config['cnyes_primary_body_burst']) is not int or not 1 <= config['cnyes_primary_body_burst'] <= 128:
        raise ValueError('invalid bounded cnyes primary-body candidate burst')
    if not config.get("user_agent") or config.get("unknown_source_policy") != "awaiting_authorization":
        raise ValueError("missing crawler identity or unsafe unknown-source policy")
    for key in ("recent_article_recheck_seconds", "recent_article_window_days"):
        if not isinstance(config.get(key), int) or config[key] <= 0:
            raise ValueError("invalid article refresh window")
    registry = PolicyRegistry(config, base=repo)
    config['provider_priority'] = validated_provider_priority(config, registry)
    if type(config.get('cached_cnyes_replay_enabled', False)) is not bool:
        raise ValueError('cached_cnyes_replay_enabled must be a boolean')
    if type(config.get('cached_cnyes_metadata_replay_enabled', False)) is not bool:
        raise ValueError('cached_cnyes_metadata_replay_enabled must be a boolean')
    if type(config.get('retained_cnyes_failure_replay_enabled', False)) is not bool:
        raise ValueError('retained_cnyes_failure_replay_enabled must be a boolean')
    if type(config.get("exclusive_http_owner", False)) is not bool:
        raise ValueError("exclusive_http_owner must be a boolean")
    from downloader.news_fulltext_capacity import capacity_config
    capacity_config(config, registry)
    from downloader.news_fulltext_failure_focus import validate_focus
    config['failure_diagnostic_focus'] = validate_focus(config.get('failure_diagnostic_focus'), registry)
    config.setdefault('failure_diagnostic_metadata_identity_upgrade', False)
    if (type(config['failure_diagnostic_metadata_identity_upgrade']) is not bool
            or config['failure_diagnostic_metadata_identity_upgrade'] and config['failure_diagnostic_focus'] is None):
        raise ValueError('metadata identity upgrade requires explicit finite diagnostic focus')
    if config.get('retained_cnyes_failure_replay_enabled', False) and config['failure_diagnostic_focus'] is None:
        raise ValueError('retained failure replay requires explicit finite diagnostic focus')
    if config.get("require_provider_worker_reservations", False):
        if sum(p.parallelism for p in registry.sources.values() if p.request_enabled) > config["workers"]:
            raise ValueError("worker budget cannot reserve configured publisher parallelism independently")
    return config, registry, repo


def _require_article_response(response, source):
    """Use the same source/status/MIME gate for requested and declared originals."""
    if response.status in {404, 410} or response.status == 200 and urlsplit(response.url).path in source.source_not_found_paths:
        raise NewsError("source_not_found")
    if response.status != 200:
        raise NewsError("http_" + str(response.status))
    if source.article_path_pattern and not re.search(source.article_path_pattern, urlsplit(response.url).path):
        raise NewsError("redirect_not_article")
    mime = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    pdf = mime == "application/pdf" and source.pdf_path_pattern
    if mime not in {"text/html", "application/xhtml+xml"} and not pdf:
        raise NewsError("unsupported_content_type")
    if (response.body == b'' and (source.provider == 'cnyes'
            or source.embedded_article_layout in {'pinview_visible_article_v1', 'pinview_visible_article_v2'})):
        # Observed real HTTP200/zero-byte envelopes contain no article or
        # identity evidence. Treat this as a bounded recoverable response,
        # not a permanent parser/rights verdict. This also applies to a
        # revalidation with a stored body; it must retain that body and retry.
        # Only the two independently observed source scopes change (ABI2).
        raise NewsError('empty_http_response', retry=True, delay=30)
    return pdf


def extract_article_response(response, source):
    """One status/MIME/path and body validator for live and retained originals."""
    if _require_article_response(response, source):
        from downloader.news_fulltext_pdf import extract_board_pdf
        return extract_board_pdf(response.body, source, page_url=response.url)
    return extract_article(response.body, source, page_url=response.url)


def article_response_record(source, row, response, article, *, captured_at_utc,
                            http_duration_seconds, parse_duration_seconds, duration_seconds,
                            resolution_basis=None, resolution=None, resolution_response=None):
    """Assemble already-validated source evidence; callers supply the real clocks.

    This deliberately performs no request and grants no source/body permission.
    Retained replay must not label its processing time as an HTTP observation.
    """
    redirects = getattr(response, 'redirects', ())
    return {
        **article, **capture_usage(source), "article_id": row["id"].hex(), "requested_url": row["url"], "final_url": response.url,
        "provider": source.provider, "metadata_date_raw": row.get("metadata_date_raw"),
        "captured_at_utc": captured_at_utc, "body_bytes": len(article["body"].encode()),
        "response_sha256": hashlib.sha256(response.body).hexdigest(), "http_response_bytes": len(response.body),
        "raw_response_body": response.body,
        "source_response_article_id": row["id"].hex(), "source_request_url": response.url,
        "data_fingerprint": source.data_fingerprint, "parser_backend": source.html_parser,
        "url_resolution_basis": resolution_basis,
        **({"transport_redirect_contract_version": 1,
            "transport_redirects_json": json.dumps([asdict(hop) for hop in redirects], ensure_ascii=False, sort_keys=True),
            "http_total_response_bytes": len(response.body) + sum(hop.response_bytes for hop in redirects)}
           if redirects else {}),
        **({"url_resolution_evidence_json": json.dumps(resolution, ensure_ascii=False, sort_keys=True),
            "url_resolution_response_body": resolution_response.body,
            "url_resolution_response_sha256": hashlib.sha256(resolution_response.body).hexdigest(),
            "http_total_response_bytes": len(resolution_response.body) + len(response.body)} if resolution else {}),
        "http_duration_seconds": http_duration_seconds, "parse_duration_seconds": parse_duration_seconds,
        "duration_seconds": duration_seconds, "etag": response.headers.get("etag"),
        "last_modified": response.headers.get("last-modified"),
        "authorization_fingerprint": source.authorization_fingerprint,
        "authorization_json": json.dumps(source.authorization, ensure_ascii=False, sort_keys=True),
        "contract_version": CONTRACT_VERSION, "publish": False, "remote_training_export": False,
    }


def fetch_article(http: NewsHTTP, row: dict) -> dict | None:
    source = http.registry.require(row["url"])
    if source.article_path_pattern and not re.search(source.article_path_pattern, urlsplit(row["url"]).path):
        raise NewsError("unsupported_article_path")
    headers = {}
    request_url = row["url"]
    resolution_basis = "requested_public_url"
    if source.provider == 'moneyweekly':
        from downloader.news_fulltext_moneyweekly import moneyweekly_article_id, moneyweekly_route_version
        if moneyweekly_route_version(source) >= 4 and moneyweekly_article_id(request_url, version=moneyweekly_route_version(source), requested=True) is None:
            raise NewsError('unsupported_article_path')
    if source.provider == 'idn':
        from downloader.news_fulltext_idn import idn_article_id
        if idn_article_id(request_url, requested=True) is None:
            raise NewsError('unsupported_article_path')
    if source.provider == 'videoland':
        from downloader.news_fulltext_videoland import videoland_article_id
        if videoland_article_id(request_url) is None:
            raise NewsError('unsupported_article_path')
    ettoday_version = ETTODAY_LAYOUT_VERSIONS.get(source.embedded_article_layout, 0)
    # Verified publisher permalink alias, not another observation/URL identity.
    # Keep the original print URL in the frontier and acceptance receipt.
    if source.provider == "cnyes" and urlsplit(request_url).hostname in {"news.cnyes.com", "m.cnyes.com"}:
        parts = urlsplit(request_url)
        match = re.fullmatch(r"/news/print/([0-9]+)", parts.path)
        if match:
            request_url = urlunsplit(("https", "news.cnyes.com", "/news/id/" + match[1], parts.query, ""))
            resolution_basis = "verified_cnyes_print_permalink_alias_v1"
    elif source.embedded_article_layout == "tainan_visible_news_v1":
        from downloader.news_fulltext_taiwan_pages import tainan_news_request_url
        request_url = tainan_news_request_url(request_url)
        if request_url != row["url"]:
            resolution_basis = "verified_tainan_html_query_separator_v1; original_frontier_url_retained"
    elif source.embedded_article_layout in {'taiwanhot_visible_article_v1', 'taiwanhot_visible_article_v2', 'taiwanhot_visible_article_v3'}:
        from downloader.news_fulltext_public_originals import taiwanhot_article_id, taiwanhot_request_ids, taiwanhot_route_matches
        native_view = source.embedded_article_layout != 'taiwanhot_visible_article_v1'
        route_views = source.embedded_article_layout == 'taiwanhot_visible_article_v3'
        if (not taiwanhot_request_ids(request_url) if route_views else
                taiwanhot_article_id(request_url, requested=True, native_view=native_view) is None):
            raise NewsError('unsupported_article_path')
    elif source.embedded_article_layout in {'tvbs_visible_article_v1', 'tvbs_visible_article_v2'}:
        from downloader.news_fulltext_tvbs import tvbs_request_identity
        if tvbs_request_identity(request_url, requested=True) is None:
            raise NewsError('unsupported_article_path')
    elif ettoday_version:
        from downloader.news_fulltext_ettoday import ettoday_request_identity
        if ettoday_request_identity(request_url, requested=True,
                allow_finance_redirect_query=ettoday_version >= 16,
                allow_house=ettoday_version >= 24 and 'house.ettoday.net' in source.reviewed_article_subsites) is None:
            raise NewsError('unsupported_article_path')
    elif source.embedded_article_layout in POCKET_LAYOUT_VERSIONS:
        from downloader.news_fulltext_pocket import pocket_article_id
        if pocket_article_id(request_url, requested=True,
                adapter_contract_version=POCKET_LAYOUT_VERSIONS[source.embedded_article_layout]) is None:
            raise NewsError('unsupported_article_path')
    elif source.embedded_article_layout in {'watchmedia_visible_article_v1', 'watchmedia_visible_article_v2', 'watchmedia_visible_article_v3'}:
        from downloader.news_fulltext_watchmedia import watchmedia_article_id
        if watchmedia_article_id(request_url, requested=True) is None:
            raise NewsError('unsupported_article_path')
    elif source.embedded_article_layout in {'pinview_visible_article_v1', 'pinview_visible_article_v2'}:
        from downloader.news_fulltext_pinview import pinview_article_id
        if pinview_article_id(request_url) is None:
            raise NewsError('unsupported_article_path')
    elif source.embedded_article_layout in {'peoplemedia_visible_article_v1', 'peoplemedia_visible_article_v2', 'peoplemedia_visible_article_v3'}:
        from downloader.news_fulltext_peoplemedia import peoplemedia_article_id
        if peoplemedia_article_id(request_url) is None:
            raise NewsError('unsupported_article_path')
    elif source.embedded_article_layout in BASTILLE_LAYOUT_VERSIONS:
        from downloader.news_fulltext_bastillepost import bastillepost_article_id
        if bastillepost_article_id(request_url, requested=True,
                adapter_contract_version=BASTILLE_LAYOUT_VERSIONS[source.embedded_article_layout]) is None:
            raise NewsError('unsupported_article_path')
    elif source.embedded_article_layout in {'mitrade_visible_article_v1', 'mitrade_visible_article_v2', 'mitrade_visible_article_v3', 'mitrade_visible_article_v4', 'mitrade_visible_article_v5', 'mitrade_visible_article_v6'}:
        from downloader.news_fulltext_mitrade import mitrade_article_id
        if mitrade_article_id(request_url) is None:
            raise NewsError('unsupported_article_path')
    elif source.embedded_article_layout == 'nstock_finite_announcement_v1':
        from downloader.news_fulltext_nstock import nstock_article_id
        if nstock_article_id(request_url) is None:
            raise NewsError('unsupported_article_path')
    validator_url = row.get('validator_url')
    if row.get("body_sha256") and validator_url:
        if row.get("etag"):
            headers["If-None-Match"] = row["etag"]
        if row.get("last_modified"):
            headers["If-Modified-Since"] = row["last_modified"]
    started = time.monotonic()
    response = http.get(request_url, headers=headers,
                        **({'conditional_url': validator_url} if headers else {}),
                        **({'follow_redirects': False} if source.embedded_article_layout == 'nstock_finite_announcement_v1' else {}))
    if (source.embedded_article_layout == 'nstock_finite_announcement_v1'
            and (response.url != request_url or getattr(response, 'redirects', ()))):
        raise NewsError('article_identity_not_verified')
    if response.status != 304:
        _require_article_response(response, source)
    if source.provider == 'moneyweekly' and response.status != 304:
        from downloader.news_fulltext_moneyweekly import validate_moneyweekly_native_response
        try:
            native_resolution = validate_moneyweekly_native_response(source, request_url, response)
        except NewsError as error:
            from downloader.news_fulltext_diagnostics import FailureResponse
            error.failure_response = FailureResponse(response, source.provider, source.data_fingerprint, row['url'], utc_now())
            raise
        if native_resolution:
            resolution_basis = native_resolution
    if ettoday_version or source.embedded_article_layout in {'tvbs_visible_article_v1', 'tvbs_visible_article_v2'}:
        # Transport may upgrade a literal HTTP link; it cannot replace its
        # article/category/host with another currently available report.
        identity_options = {}
        if ettoday_version:
            from downloader.news_fulltext_ettoday import ettoday_request_identity as request_identity
            identity_options['allow_finance_redirect_query'] = ettoday_version >= 16
            identity_options['allow_house'] = ettoday_version >= 24 and 'house.ettoday.net' in source.reviewed_article_subsites
        else:
            from downloader.news_fulltext_tvbs import tvbs_request_identity as request_identity
        different_identity = (request_identity(response.url, **identity_options)
                              != request_identity(request_url, requested=True, **identity_options))
        source_alias = False
        if different_identity and ettoday_version >= 17:
            from downloader.news_fulltext_ettoday import ettoday_source_redirect_verified
            source_alias = ettoday_source_redirect_verified(request_url, response,
                allow_house=identity_options['allow_house'])
        if different_identity and not source_alias:
            from downloader.news_fulltext_diagnostics import FailureResponse
            error = NewsError('article_identity_not_verified')
            error.failure_response = FailureResponse(response, source.provider, source.data_fingerprint, row['url'], utc_now())
            raise error
        if source_alias:
            resolution_basis = 'verified_ettoday_literal_source_redirect_v17; original_frontier_url_retained'
        elif response.url != request_url:
            resolution_basis = f'verified_{source.provider}_same_host_path_source_redirect_v1; original_frontier_url_retained'
        if not source_alias and ettoday_version >= 16 and (urlsplit(request_url).query or urlsplit(response.url).query):
            resolution_basis = 'verified_ettoday_finance_redirect_parameter_v16; literal_request_retained'
    if source.embedded_article_layout in {'taiwanhot_visible_article_v1', 'taiwanhot_visible_article_v2', 'taiwanhot_visible_article_v3'}:
        mismatch = (not taiwanhot_request_ids(response.url).intersection(taiwanhot_request_ids(request_url))
                    if route_views else taiwanhot_article_id(response.url, native_view=native_view)
                    != taiwanhot_article_id(request_url, requested=True, native_view=native_view))
        if mismatch:
            from downloader.news_fulltext_diagnostics import FailureResponse
            error = NewsError('article_identity_not_verified')
            error.failure_response = FailureResponse(response, source.provider, source.data_fingerprint, row['url'], utc_now())
            raise error
        if response.url != request_url:
            version = 3 if route_views else 2 if native_view else 1
            resolution_basis = f'verified_taiwanhot_same_id_source_redirect_v{version}; original_frontier_url_retained'
    if source.embedded_article_layout == "macau_visible_news_v1" and response.url != request_url:
        # The reviewed portal has no verified article alias contract. A
        # different current release is not the originally requested ID.
        raise NewsError("article_identity_not_verified")
    if source.provider in {'idn', 'videoland'} and (
            response.url != request_url or getattr(response, 'redirects', ())):
        raise NewsError('article_identity_not_verified')
    if source.embedded_article_layout in POCKET_LAYOUT_VERSIONS:
        from downloader.news_fulltext_pocket import pocket_article_id, pocket_article_transport_verified
        hops = getattr(response, 'redirects', ())
        pocket_version = POCKET_LAYOUT_VERSIONS[source.embedded_article_layout]
        if (pocket_article_id(response.url, adapter_contract_version=pocket_version)
                != pocket_article_id(request_url, requested=True, adapter_contract_version=pocket_version)
                or response.url != request_url and (not hops or hops[0].from_url != request_url
                    or hops[-1].to_url != response.url)
                or pocket_version >= 10 and not pocket_article_transport_verified(
                    response, request_url, adapter_contract_version=pocket_version)):
            from downloader.news_fulltext_diagnostics import FailureResponse
            error = NewsError('article_identity_not_verified')
            error.failure_response = FailureResponse(response, source.provider, source.data_fingerprint, row['url'], utc_now())
            raise error
        if response.url != request_url:
            resolution_basis = 'verified_pocket_literal_same_id_source_redirect_v1; original_frontier_url_retained'
    if source.embedded_article_layout in {'pinview_visible_article_v1', 'pinview_visible_article_v2',
                                         'peoplemedia_visible_article_v1', 'peoplemedia_visible_article_v2', 'peoplemedia_visible_article_v3',
                                         'mitrade_visible_article_v1', 'mitrade_visible_article_v2', 'mitrade_visible_article_v3', 'mitrade_visible_article_v4', 'mitrade_visible_article_v5', 'mitrade_visible_article_v6'} and (
            response.url != request_url or getattr(response, 'redirects', ())):
        # No host/ID alias for these layouts has a verified transport witness. A title,
        # declared OG or a different story's HTTP 200 cannot create one.
        raise NewsError('article_identity_not_verified')
    if source.embedded_article_layout in BASTILLE_LAYOUT_VERSIONS:
        from downloader.news_fulltext_bastillepost import bastillepost_transport_verified
        version = BASTILLE_LAYOUT_VERSIONS[source.embedded_article_layout]
        if not bastillepost_transport_verified(response, request_url, adapter_contract_version=version):
            from downloader.news_fulltext_diagnostics import FailureResponse
            error = NewsError('article_identity_not_verified')
            error.failure_response = FailureResponse(response, source.provider, source.data_fingerprint, row['url'], utc_now())
            raise error
        if response.url != request_url:
            resolution_basis = 'verified_bastillepost_literal_same_id_source_redirect_v1; original_frontier_url_retained'
    if response.status == 304:
        # A 304 has no article body to parse. It proves freshness only for the
        # exact saved representation whose validators were actually requested,
        # never a different final URL or an unsolicited not-modified response.
        if (not headers or not row.get('parquet_path') or response.body
                or normalize_url(response.url) != normalize_url(validator_url)):
            raise NewsError('unverifiable_not_modified', retry=True, delay=60)
        return None
    resolution, resolution_response = None, None
    if source.embedded_article_layout in {"ftv_visible_article_v1", "ftv_visible_article_v2", "ftv_visible_article_v3", "ftv_visible_article_v4", "ftv_visible_article_v5", "ftv_visible_article_v6", "ftv_visible_article_v7"} and urlsplit(response.url).hostname != "www.ftvnews.com.tw":
        from downloader.news_fulltext_public_originals import ftv_declared_public_canonical
        try:
            resolution = ftv_declared_public_canonical(response.body, source, page_url=response.url)
        except NewsError as error:
            if error.code == "access_challenge":
                http.defer(source.provider, 900)
                raise NewsError("access_challenge", retry=True, delay=900) from None
            raise
        target = resolution["declared_canonical_url"]
        if http.registry.require(target).provider != source.provider:
            raise NewsError("article_identity_not_verified")
        resolution_response = response
        # No validators from a different representation, generated host alias,
        # alternate route after refusal, or backdated/relabelled subsite clock.
        response = http.get(target)
        _require_article_response(response, source)
        resolution_basis = resolution["basis"]
    received = time.monotonic()
    received_at_utc = utc_now()
    metrics = getattr(http, "metrics", None)
    with metrics.measure("article_extraction", source.provider) if metrics else nullcontext():
        try:
            article = extract_article_response(response, source)
            if source.embedded_article_layout in {'watchmedia_visible_article_v1', 'watchmedia_visible_article_v2', 'watchmedia_visible_article_v3'}:
                from downloader.news_fulltext_watchmedia import watchmedia_response_identity
                resolution_basis = watchmedia_response_identity(request_url, response, article['title'], row.get('title'))
            if (source.embedded_article_layout == 'taiwanhot_visible_article_v3'
                    and not taiwanhot_route_matches(request_url, article['source_native_article_id'], article['title'])):
                # A candidate intersection is admission only. The actual OG
                # ID and visible title must still bind the ORIGINAL route.
                raise NewsError('article_identity_not_verified')
            if resolution:
                if (re.sub(r"\s+", "", article["title"]) != re.sub(r"\s+", "", resolution["headline"])
                        or normalize_url(article.get("declared_canonical_url", "")) != normalize_url(resolution["declared_canonical_url"])):
                    raise NewsError("article_identity_not_verified")
                resolution.update(canonical_published_at_raw=article["published_at_raw"],
                                  declared_clock_conflict=resolution["subsite_published_at_raw"] != article["published_at_raw"],
                                  accepted_clock_basis="independently_verified_canonical_original; subsite_clock_preserved_not_corrected")
        except NewsError as error:
            if error.code == "access_challenge":
                # HTTP 200 can still be an explicit access refusal. Pause the
                # publisher, don't burn every pending URL on the same challenge.
                http.defer(source.provider, 900)
                if metrics:
                    metrics.count(source.provider, "content_access_refusals")
                raise NewsError("access_challenge", retry=True, delay=900) from None
            from downloader.news_fulltext_diagnostics import FailureResponse, VALIDATION_METADATA_CODES
            if error.code in {'body_not_found', 'article_identity_not_verified', *VALIDATION_METADATA_CODES}:
                # The worker transfers the already-fetched response only.
                # Bounded storage and SQLite budget belong to the queue owner.
                error.failure_response = FailureResponse(response, source.provider, source.data_fingerprint,
                                                         row['url'], received_at_utc)
            raise
    record = article_response_record(source, row, response, article, captured_at_utc=utc_now(),
        http_duration_seconds=received - started, parse_duration_seconds=time.monotonic() - received,
        duration_seconds=time.monotonic() - started, resolution_basis=resolution_basis,
        resolution=resolution, resolution_response=resolution_response)
    witness = article_claim_witness(row)
    if witness is not None:
        record['article_claim_witness_json'] = witness
    return record


def discovery_links(payload: bytes, kind: str, source, *, page_url: str | None = None) -> list[tuple[str, str, str]]:
    """RSS/Atom and sitemap indexes are discovery, never evidence of body completeness."""
    if kind == "index":
        from urllib.parse import urljoin
        soup = BeautifulSoup(payload, source.html_parser)
        links = []
        for node in soup.select("a[href]"):
            try:
                url = normalize_url(urljoin(page_url or "", node["href"]))
            except NewsError:
                continue
            host = urlsplit(url).hostname or ""
            if not any(host == d or host.endswith("." + d) for d in source.domains):
                continue
            if url == page_url:
                continue
            path = urlsplit(url).path
            if source.index_path_pattern and re.search(source.index_path_pattern, path):
                links.append((url, "publisher_index", ""))
            elif source.article_path_pattern and re.search(source.article_path_pattern, path):
                links.append((url, node.get_text(" ", strip=True), ""))
        if not links:
            raise NewsError("publisher_index_no_matching_links")
        return list(dict.fromkeys(links))
    if kind == 'sitemap' and payload.startswith(b'\x1f\x8b'):
        from downloader.news_fulltext_sitemaps import gzip_sitemap_links
        links = gzip_sitemap_links(payload)
    else:
        soup = BeautifulSoup(payload, "xml")
        if soup.find("parsererror"):
            raise NewsError("invalid_discovery_xml")
        links = []
        if kind == "feed":
            if not soup.find(["rss", "feed", "RDF"]):
                raise NewsError("invalid_feed")
            for item in soup.find_all(["item", "entry"]):
                link = item.find("link", attrs={"rel": "alternate"}) or item.find("link")
                if link:
                    url = link.get("href") or link.get_text(strip=True)
                    title = item.find("title")
                    date = item.find(["pubDate", "published", "updated"])
                    links.append((url, title.get_text() if title else "", date.get_text() if date else ""))
        elif soup.find("sitemapindex"):
            links = [(node.get_text(strip=True), "sitemap", "") for node in soup.select("sitemap > loc")]
        elif soup.find("urlset"):
            for item in soup.find_all("url"):
                location = item.find("loc")
                modified = item.find("lastmod")
                if location:
                    links.append((location.get_text(strip=True), "", modified.get_text() if modified else ""))
        else:
            raise NewsError("invalid_sitemap")
    accepted = []
    for url, title, date in links:
        try:
            url = normalize_url(url)
        except NewsError:
            continue
        host = urlsplit(url).hostname or ""
        if not any(host == domain or host.endswith("." + domain) for domain in source.domains):
            continue
        if (source.embedded_article_layout in {'max_visible_post_v1', 'max_visible_post_v2', 'max_visible_post_v3', 'max_visible_post_v4'} and title == 'sitemap'
                and not re.fullmatch(r'https://maxfinanciallife[.]com/post-sitemap[1-9][0-9]*[.]xml', url)):
            continue  # Native post leaves only; not pages, taxonomy or theme blocks.
        if title != "sitemap" and source.article_path_pattern and not re.search(source.article_path_pattern, urlsplit(url).path):
            continue
        accepted.append((url, title, date))
    return accepted


def seed_discovery(queue: NewsQueue):
    with queue.metrics.measure('discovery_seed_maintenance'):
        _seed_discovery(queue)


def _seed_discovery(queue: NewsQueue):
    queue.sync_finite_cohorts()
    with queue.db:
        for source in queue.registry.sources.values():
            if source.known_url_only:
                continue  # Only locally observed URLs; dormant discovery is not permission.
            if source.metadata_feeds:
                from downloader.news_fulltext_metadata_feeds import compatible_fingerprints as metadata_fingerprints
                fingerprints = metadata_fingerprints(source)
                queue.db.executemany("INSERT INTO discovery(url,kind,provider,priority) VALUES (?,'metadata_feed',?,0) "
                    "ON CONFLICT(url) DO UPDATE SET kind='metadata_feed',priority=0,"
                    "due=CASE WHEN error_code IS NULL THEN 0 ELSE due END "
                    "WHERE kind!='metadata_feed' OR (parser_fingerprint IS NOT NULL AND parser_fingerprint NOT IN ("
                    + ','.join('?' for _ in fingerprints) + '))',
                    ((url, source.provider, *fingerprints) for url in source.metadata_feeds))
            if source.allowed and source.acquisition_mode != "finite":
                queue.db.executemany("INSERT OR IGNORE INTO discovery(url,kind,provider) VALUES (?,?,?)",
                                     ((url, kind, source.provider) for kind, urls in (("feed", source.feeds), ("sitemap", source.sitemaps))
                                      for url in urls if url not in source.fulltext_feeds))
                for url in source.fulltext_feeds:
                    fingerprints = fulltext_feed_discovery_fingerprints(source)
                    queue.db.execute("INSERT INTO discovery(url,kind,provider,priority) VALUES (?,'rss_fulltext',?,0) "
                                     "ON CONFLICT(url) DO UPDATE SET kind='rss_fulltext',priority=0,"
                                     "due=CASE WHEN error_code IS NULL THEN 0 ELSE due END "
                                     "WHERE kind!='rss_fulltext' OR (parser_fingerprint IS NOT NULL AND parser_fingerprint NOT IN ("
                                     + ','.join('?' for _ in fingerprints) + '))',
                                     (url, source.provider, *fingerprints))
                queue.db.executemany("INSERT OR IGNORE INTO discovery(url,kind,provider) VALUES (?,'index',?)",
                                     ((url, source.provider) for url in source.index_pages))
                # Previously accepted indexes need one reparse when their
                # path/layout contract changes; otherwise yesterday's filtered
                # links can hide old years until the next daily scan. Refusal
                # and transient retry deadlines are never reset here.
                index_where = "provider=? AND kind='index' AND error_code IS NULL AND parser_fingerprint IS NOT NULL"
                index_parameters = (source.provider, discovery_fingerprint(source))
                generic_index_filter = ""
                if source.provider == 'cnyes':
                    from downloader.news_fulltext_cnyes_index import ENDPOINT
                    # The public API has its own metadata ABI. Comparing it
                    # with the body/index-HTML ABI makes every maintenance
                    # requeue successful pages and starves unseen history.
                    # Disabled metadata work retains its prior deadline too.
                    generic_index_filter = " AND substr(url,1,instr(url||'?','?')-1)!=?"
                    index_parameters += (ENDPOINT,)
                    if getattr(source, 'cnyes_index_api', ''):
                        from downloader.news_fulltext_cnyes_index import compatible_fingerprints
                        versions = compatible_fingerprints(source)
                        queue.db.execute('UPDATE discovery SET due=0 WHERE ' + index_where
                                         + " AND parser_fingerprint NOT IN (" + ','.join('?' for _ in versions)
                                         + ") AND substr(url,1,instr(url||'?','?')-1)=?",
                                         (source.provider, *versions, ENDPOINT))
                queue.db.execute('UPDATE discovery SET due=0 WHERE ' + index_where
                                 + ' AND parser_fingerprint!=?' + generic_index_filter, index_parameters)
                from downloader.news_fulltext_schedule import reconcile_incremental_indexes
                reconcile_incremental_indexes(queue, source, discovery_fingerprint(source))
                queue.db.executemany("INSERT OR IGNORE INTO discovery(url,kind,provider,priority) VALUES (?,'wordpress_posts',?,1)",
                                     ((wordpress_url(base), source.provider) for base in source.wordpress_apis))
                if source.cnyes_apis:
                    from downloader.news_fulltext_cnyes_index import seed_gap_dates
                    seed_gap_dates(queue,source)
                    from datetime import datetime, timedelta, timezone
                    from downloader.news_fulltext_cnyes_seed import CONTRACT_VERSION as seed_version, planned_intervals
                    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                    signature = (today.date(), cnyes_bulk_fingerprint(source), seed_version)
                    seeds = getattr(queue, "_cnyes_seed_signatures", {})
                    if seeds.get(source.provider) == signature:
                        continue  # No daily scope change: don't rebuild thousands of roots each owner cycle.
                    start = datetime.strptime(source.cnyes_history_start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    history_low = int(start.timestamp())
                    month_start = today.replace(day=1)
                    priority_key = 'cnyes_seed_priority_contract:' + source.provider
                    priority_contract = json.dumps({'version': seed_version, 'endpoints': sorted(source.cnyes_apis)}, sort_keys=True)
                    prior = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (priority_key,)).fetchone()
                    migrate_priorities = not prior or prior[0] != priority_contract
                    priority_examined = priority_updated = 0
                    for base in source.cnyes_apis:
                        windows = []
                        if source.cnyes_history_partition_policy == "adaptive_days_v1":
                            # New categories start with the largest historical
                            # interval, then partition only from returned totals.
                            # Existing monthly/day jobs are reused, not re-fetched.
                            with queue.metrics.measure('cnyes_planned_scope_read'):
                                covered = planned_intervals(queue.db, base)
                            windows.extend(cnyes_unseeded_intervals(history_low, int(month_start.timestamp()) - 1, covered))
                            current = max(start, month_start)
                        else:
                            current = start
                        while current < today:
                            if current.year == today.year and current.month == today.month:
                                end = min(current + timedelta(days=1), today)
                            else:
                                end = min(current.replace(year=current.year + (current.month == 12), month=current.month % 12 + 1, day=1), today)
                            windows.append((int(current.timestamp()), int(end.timestamp()) - 1))
                            current = end
                        queue.db.execute("INSERT INTO discovery(url,kind,provider,priority) VALUES (?,'cnyes_news',?,?) "
                                         "ON CONFLICT(url) DO UPDATE SET priority=excluded.priority WHERE priority!=excluded.priority",
                                         (cnyes_url(base), source.provider, cnyes_history_priority(cnyes_url(base))))
                        # Immutable closed bounds; do not eagerly enumerate all
                        # pages, guess article IDs, or alter existing retry times.
                        queue.db.executemany("INSERT INTO discovery(url,kind,provider,priority) VALUES (?,'cnyes_news',?,?) "
                                             "ON CONFLICT(url) DO UPDATE SET priority=excluded.priority WHERE priority!=excluded.priority",
                                             ((cnyes_url(base, start=low, end=high), source.provider,
                                               cnyes_history_priority(cnyes_url(base, start=low, end=high)))
                                              for low, high in windows))
                        # Existing jobs need a priority migration only when this
                        # contract/endpoint set changes, not each UTC day/owner.
                        # New roots/children already receive the same priority at
                        # their canonical insertion sites. Retry clocks stay put.
                        if migrate_priorities:
                            with queue.metrics.measure('cnyes_seed_priority_migration'):
                                rows = queue.db.execute("SELECT url,priority FROM discovery WHERE url>=? AND url<? AND kind='cnyes_news'",
                                                        (base + "?", base + "@"))
                                changes = []
                                for row in rows:
                                    priority_examined += 1
                                    priority = cnyes_history_priority(row[0])
                                    if row[1] != priority:
                                        changes.append((priority, row[0]))
                                queue.db.executemany('UPDATE discovery SET priority=? WHERE url=?', changes)
                                priority_updated += len(changes)
                    if migrate_priorities:
                        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (priority_key, priority_contract))
                        if priority_examined:
                            queue.counter('cnyes_priority_jobs_examined', priority_examined)
                        if priority_updated:
                            queue.counter('cnyes_priority_jobs_updated', priority_updated)
                    seeds[source.provider] = signature
                    queue._cnyes_seed_signatures = seeds


def discovery_fingerprint(source, *, kind=None):
    fingerprint = (cnyes_bulk_fingerprint(source) if source.cnyes_apis
                   else source.fulltext_feed_data_fingerprint or source.data_fingerprint)
    result = _discovery_fingerprint(source, fingerprint)
    if kind == 'sitemap':
        from downloader.news_fulltext_sitemaps import GZIP_SITEMAP_CONTRACT_VERSION
        result = hashlib.sha256((result + ':gzip-sitemap-v' + str(GZIP_SITEMAP_CONTRACT_VERSION)).encode()).hexdigest()
    return result


def sitemap_discovery_fingerprints(source):
    """Keep successful plain-XML validators and original schedules usable.

    The old parser could not accept gzip; its successful receipts therefore
    describe supported plain XML. Adding gzip does not require replaying them.
    Body/feed/index fingerprints and their deadlines are unchanged.
    """
    return discovery_fingerprint(source, kind='sitemap'), discovery_fingerprint(source)


def _discovery_fingerprint(source, fingerprint):
    rss_contract = ':rss-controls-metadata-only-v1' if source.fulltext_feeds else ''
    envelope_contract = (':mixed-bulk-continuation-v' + str(BULK_ENVELOPE_CONTINUATION_CONTRACT_VERSION)
                         if source.fulltext_feeds or source.wordpress_apis or source.cnyes_apis else '')
    return hashlib.sha256((fingerprint + ":news-discovery-parser-v2" + rss_contract + envelope_contract).encode()).hexdigest()


def fulltext_feed_discovery_fingerprints(source):
    """A body-only GV revision does not replay healthy history or refusals.

    Old v1/v2 receipts retain their own parser. Conditional requests may use their
    validators at the original due time; a 304 never claims a new body parse.
    """
    current = discovery_fingerprint(source)
    prior = source.prior_fulltext_feed_data_fingerprint
    fingerprints = [current]
    fingerprints.extend(_discovery_fingerprint(source, fingerprint)
                        for _, fingerprint in reversed(source.fulltext_feed_body_fingerprints))
    if prior:
        fingerprints.append(_discovery_fingerprint(source, prior))
    return tuple(dict.fromkeys(fingerprints))


def fetch_discovery(http: NewsHTTP, task: dict):
    metadata = task['kind'] == 'metadata_feed'
    if metadata:
        from downloader.news_fulltext_metadata_feeds import require_request
        source = require_request(http.registry, task['url'], task['url'])
        if task['provider'] != source.provider:
            raise NewsError('metadata_feed_provider_mismatch')
    else:
        source = http.registry.require(task["url"])
    from downloader.news_fulltext_cnyes_index import is_index_task, parse_index, validate_request
    index_task = is_index_task(task)
    if index_task:
        validate_request(task['url'],source)  # Reject before the HTTP owner.
    registered = {"wordpress_posts": source.wordpress_apis, "cnyes_news": source.cnyes_apis,
                  "rss_fulltext": source.fulltext_feeds}
    if task["kind"] in registered and wordpress_base(task["url"]) not in registered[task["kind"]]:
        raise NewsError("unregistered_bulk_api")  # reject before a network request
    headers = {}
    fingerprint = (source.metadata_feed_fingerprint if metadata else
                   source.cnyes_index_data_fingerprint if index_task else discovery_fingerprint(source, kind=task['kind']))
    compatible = task.get("parser_fingerprint") == fingerprint
    if metadata:
        from downloader.news_fulltext_metadata_feeds import compatible_fingerprints as metadata_fingerprints
        compatible = task.get('parser_fingerprint') in metadata_fingerprints(source)
    if index_task:
        from downloader.news_fulltext_cnyes_index import compatible_fingerprints
        compatible = task.get('parser_fingerprint') in compatible_fingerprints(source)
    if task['kind'] == 'rss_fulltext':
        compatible = task.get('parser_fingerprint') in fulltext_feed_discovery_fingerprints(source)
    if task['kind'] == 'sitemap':
        compatible = task.get('parser_fingerprint') in sitemap_discovery_fingerprints(source)
    needs_history_witness = (task['kind'] == 'cnyes_news'
                             and task.get('_cnyes_missing_restricted_history_evidence') is True)
    if task["etag"] and compatible and not needs_history_witness:
        headers["If-None-Match"] = task["etag"]
    if task["last_modified"] and compatible and not needs_history_witness:
        headers["If-Modified-Since"] = task["last_modified"]
    # A response can be in flight while another article request completes.
    # Only the earlier request-start clock can fence those intervening writes.
    # This is a local process clock, not a publisher or release timestamp.
    request_started_at_utc = utc_now()
    response = http.get(task["url"], headers=headers, **({'metadata_feed': task['url']} if metadata else {}))
    if response.status not in {200, 304}:
        raise NewsError("discovery_http_" + str(response.status))
    if response.status == 304 and (not compatible or not headers):
        raise NewsError("unverifiable_discovery_not_modified", retry=True, delay=60)
    # Keep the later worker observation distinct; it cannot order writes that
    # happened while the source response was in flight.
    response_observed_at_utc = utc_now()
    task['_discovery_response_observed_at_utc'] = response_observed_at_utc
    task['_discovery_request_started_at_utc'] = (
        request_started_at_utc if datetime.fromisoformat(request_started_at_utc)
        <= datetime.fromisoformat(response_observed_at_utc) else None)
    if metadata:
        return response, [], [], []  # Parse once at scoped durable acceptance; never HTML fallback.
    if index_task:
        with http.metrics.measure('public_index_metadata_extraction',source.provider):
            links = parse_index(response,source)[2] if response.status == 200 else []
        return response, links, [], []
    if task["kind"] == "rss_fulltext":
        with http.metrics.measure("bulk_extraction", source.provider):
            records, errors = feed_records(response, http, source) if response.status == 200 else ([], [])
        links = [(r["requested_url"], r["title"], r["metadata_date_raw"]) for r in records]
        links.extend((e["url"], e.get('title', ''), e.get('metadata_date_raw', '')) for e in errors if e["url"] and e["error"] in {
            "body_not_found", "article_attribution_not_verified", "invalid_bulk_publication_clock", "invalid_bulk_post_fields",
            XML_BODY_METADATA_FALLBACK})
        return response, links, records, errors
    if task["kind"] == "wordpress_posts":
        with http.metrics.measure("bulk_extraction", source.provider):
            records, errors = wordpress_records(response, http, source) if response.status == 200 else ([], [])
        links = [(r["requested_url"], r["title"], r["metadata_date_raw"]) for r in records]
        return response, links, records, errors
    if task["kind"] == "cnyes_news":
        # This observation belongs to the live worker, not the immutable
        # parser contract used to recheck historical source envelopes.
        observed_at_utc = utc_now()
        with http.metrics.measure("bulk_extraction", source.provider):
            records, errors = cnyes_records(response, http, source) if response.status == 200 else ([], [])
        for error in errors:
            error['captured_at_utc'] = observed_at_utc
        links = [(r["requested_url"], r["title"], r["metadata_date_raw"]) for r in records]
        # Invalid/short content can use the canonical HTML fallback. Payment
        # refusals are not automatically followed into a paywall.
        links.extend((e["url"], e.get("title", ""), e.get("metadata_date_raw", "")) for e in errors
                     if e["url"] and e["error"] not in {"bulk_post_not_public", "bulk_exact_duplicate_row"})
        return response, links, records, errors
    if response.status == 200 and compatible and task.get("response_sha256") == hashlib.sha256(response.body).hexdigest():
        http.metrics.count(source.provider, "unchanged_discovery_responses")
        return response, [], [], []
    with http.metrics.measure("discovery_extraction", source.provider):
        links = discovery_links(response.body, task["kind"], source, page_url=response.url) if response.status == 200 else []
    return response, links, [], []


def apply_cnyes_page_metadata(queue, task, response, source, info, posts, artifact, *, observed_at=None):
    """Commit one validated page's coverage using the existing queue owner."""
    from urllib.parse import parse_qs
    stamp = observed_at or utc_now()
    base, total, pages, page_number = info['scope'], info['total'], info['pages'], info['page']
    queue.observe_bulk_coverage(base, source.provider, total, pages, stamp)
    queue.metrics.count(source.provider, 'bulk_exact_duplicate_rows_observed', info['duplicate_rows'])
    query = parse_qs(urlsplit(task['url']).query)
    partitions = queue.partition_cnyes_window(task['url'], pages, source)
    if partitions:
        queue.metrics.count(source.provider, 'historical_windows_partitioned')
    elif 'startAt' in query and pages > 30:
        queue.db.execute('INSERT OR REPLACE INTO bulk_window_gaps VALUES (?,?,?,?)',
                         (base, source.provider, 'cnyes_single_day_page_cap', stamp))
    if 'startAt' in query and not partitions and page_number < min(pages, 30):
        next_url = cnyes_url(wordpress_base(task['url']), page_number + 1,
                             start=int(query['startAt'][0]), end=int(query['endAt'][0]))
        queue.db.execute("INSERT OR IGNORE INTO discovery(url,kind,provider,priority) VALUES (?,'cnyes_news',?,?)",
                         (next_url, source.provider, cnyes_history_priority(next_url)))
    if total:
        queue.db.execute('INSERT OR REPLACE INTO bulk_page_evidence VALUES (?,?,?,?,?)',
                         (base, page_number, total, info['ids_sha256'], stamp))
    if artifact is not None:
        from downloader.news_fulltext_cnyes_history import record_page
        record_page(queue, response, info, posts, source, artifact, observed_at=observed_at,
                    origin='retained_interrupted_page' if observed_at else 'normal_source_response')
    queue.metrics.count(source.provider, 'bulk_pages_validated')
    queue.metrics.count(source.provider, 'bulk_article_ids_validated', len(posts))


def _locked_discovery_head_matches(queue, task, fields):
    """Hold the writer lock while comparing the dispatched source snapshot."""
    if not fields:
        return True
    if not queue.db.in_transaction:
        queue.db.execute('BEGIN IMMEDIATE')
    current = queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()
    return current is not None and all(current[key] == task[key] for key in fields)


def _discovery_head_fields(task):
    from downloader.news_fulltext_cnyes_checkpoint import HEAD_FIELDS
    return HEAD_FIELDS if all(key in task for key in HEAD_FIELDS) else ()


def _cnyes_discovery_head_fields(task):
    return _discovery_head_fields(task) if task.get('kind') == 'cnyes_news' else ()


def _missing_article_links(queue, links):
    """Select only new identities under the caller's discovery write lock."""
    normalized = []
    for item in links:
        try:
            normalized.append((item, normalize_url(item[0])))
        except NewsError:
            normalized.append((item, None))  # Let add_articles count invalid input.
    urls = list(dict.fromkeys(url for _, url in normalized if url is not None))
    existing = set()
    for offset in range(0, len(urls), 500):
        batch = urls[offset:offset + 500]
        existing.update(row[0] for row in queue.db.execute(
            'SELECT url FROM articles WHERE url IN (' + ','.join('?' for _ in batch) + ')', batch))
    return [item for item, url in normalized if url is None or url not in existing]


def _article_links_started_after_heads(queue, links, request_started_at_utc):
    """Keep only new identities or requests started after article heads.

    The caller holds the queue write transaction. Unknown/malformed or naive
    clocks never authorize an overwrite; original articles and receipts remain
    available. URL normalization matches add_articles and queries are bounded.
    """
    if not links:
        return links
    try:
        observed = datetime.fromisoformat(request_started_at_utc.replace('Z', '+00:00'))
        if observed.tzinfo is None or observed.utcoffset() is None:
            raise ValueError('naive observation clock')
    except (ValueError, TypeError, AttributeError, OverflowError):
        return _missing_article_links(queue, links)
    normalized = []
    for item in links:
        try:
            normalized.append((item, normalize_url(item[0])))
        except NewsError:
            normalized.append((item, None))
    urls = list(dict.fromkeys(url for _, url in normalized if url is not None))
    existing = {}
    for offset in range(0, len(urls), 500):
        batch = urls[offset:offset + 500]
        rows = queue.db.execute('SELECT url,last_seen_utc,last_checked_at_utc,captured_at_utc,body_sha256 '
                                'FROM articles WHERE url IN (' + ','.join('?' for _ in batch) + ')', batch)
        existing.update((row['url'], row) for row in rows)

    def eligible(url):
        head = existing.get(url)
        if head is None:
            return True
        try:
            if head['body_sha256'] and not head['captured_at_utc']:
                return False
            for value in (head['last_seen_utc'], head['last_checked_at_utc'], head['captured_at_utc']):
                if value is not None:
                    checked = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    if checked.tzinfo is None or checked.utcoffset() is None or checked >= observed:
                        return False
            return True
        except (ValueError, TypeError, AttributeError, OverflowError):
            return False

    return [item for item, url in normalized if url is None or eligible(url)]


def _cnyes_fresh_article_links(queue, task, fields, links, records, errors):
    """Keep newer article metadata even while an independent page is in flight.

    Call inside the discovery-registration write transaction. CNYES pages
    contain at most 30 distinct IDs (60 raw rows including verified repeats).
    Only the local request-start clock orders an article update made while the
    response is in flight; it is not a publisher or release time.
    """
    if not links or task['kind'] != 'cnyes_news':
        return links
    if not queue.db.in_transaction:
        queue.db.execute('BEGIN IMMEDIATE')
    if not _locked_discovery_head_matches(queue, task, fields):
        return _missing_article_links(queue, links)
    return _article_links_started_after_heads(queue, links, task.get('_discovery_request_started_at_utc'))


def _fresh_article_links(queue, task, fields, links, records, errors):
    if task['kind'] == 'cnyes_news':
        return _cnyes_fresh_article_links(queue, task, fields, links, records, errors)
    if not links or not fields:
        return links
    if not _locked_discovery_head_matches(queue, task, fields):
        return _missing_article_links(queue, links)
    return _article_links_started_after_heads(queue, links, task.get('_discovery_request_started_at_utc'))


def _select_discovery_records(queue, task, records, *, defer_unchanged=False):
    """Select eligible bodies and optionally defer independent revalidations."""
    accepted, unchanged = [], []
    for record in records:
        if task["kind"] == "cnyes_news":
            # The current ID and both known print aliases are independent
            # heads. Read them in one indexed statement, then process aliases
            # in their original order; every accepted head keeps its own fence.
            identifier = bytes.fromhex(record["article_id"])
            alias_urls = tuple(record["requested_url"].replace("news.cnyes.com/news/id/", host + "/news/print/", 1)
                               for host in ("news.cnyes.com", "m.cnyes.com"))
            rows = queue.db.execute('SELECT * FROM articles WHERE id=? OR url=? OR url=?',
                                    (identifier, *alias_urls)).fetchall()
            aliases = {row['url']: row for row in rows}
            for alias_url in alias_urls:
                alias = aliases.get(alias_url)
                if (alias and not alias['body_sha256'] and alias['state'] in {"pending", "retry", "unsupported_article_path", "body_not_found", "article_attribution_not_verified"}
                        and alias['due'] <= time.time() and alias['lease_owner'] is None
                        and alias['lease_until'] is None and alias['lease_token'] is None):
                    accepted.append({**record, "article_id": alias['id'].hex(), "requested_url": alias_url,
                                     "url_resolution_basis": "verified_cnyes_print_permalink_alias_v1",
                                     "unleased_article_head_witness_json": unleased_article_head_witness(
                                         alias, request_started_at_utc=task.get('_discovery_request_started_at_utc'))})
            current = next((row for row in rows if row['id'] == identifier), None)
        else:
            current = queue.db.execute("SELECT * FROM articles WHERE id=?", (bytes.fromhex(record["article_id"]),)).fetchone()
        if current is None:
            continue  # No queue head exists to bind a durable article version to.
        if (current["state"] == "inflight" or current['lease_owner'] is not None
                or current['lease_until'] is not None or current['lease_token'] is not None):
            continue  # Never overwrite/release another worker's article lease.
        if current["state"] in {"retry", "retry_cooldown"} and current["due"] > time.time():
            continue  # A bulk response is not permission to bypass a retry deadline.
        if current["body_sha256"] == record["body_sha256"]:
            if current["state"] != "complete":
                if defer_unchanged:
                    unchanged.append((dict(current), record['captured_at_utc']))
                else:
                    queue.unchanged(dict(current), observed_at_utc=record['captured_at_utc'],
                                    request_started_at_utc=task.get('_discovery_request_started_at_utc'))
            continue
        if task["kind"] in {"cnyes_news", "rss_fulltext"} and current["body_sha256"] and current["state"] == "complete":
            # Existing verified HTML (with byline) is already current under the
            # configured recheck policy. Do not rewrite it merely because the
            # API uses different whitespace or omits author metadata.
            continue
        record['unleased_article_head_witness_json'] = unleased_article_head_witness(
            current, request_started_at_utc=task.get('_discovery_request_started_at_utc'))
        accepted.append(record)
    return accepted, unchanged


def accept_discovery(queue: NewsQueue, task: dict, response, links, records, errors):
    if task['kind'] == 'metadata_feed':
        from downloader.news_fulltext_metadata_feeds import accept
        return accept(queue, task, response, links, records, errors)
    source = queue.registry.sources[task["provider"]]
    from downloader.news_fulltext_cnyes_index import accept_index, is_index_task
    if is_index_task(task):
        if records or errors:
            raise NewsError('cnyes_index_cannot_accept_article_bodies')
        return accept_index(queue,task,response,links,source)
    head_fields = _discovery_head_fields(task)
    bulk = task["kind"] in {"wordpress_posts", "cnyes_news", "rss_fulltext"}
    cnyes_page_info = None
    no_body_checkpoint = None
    rss_page_info = None
    # A 304 confirms unchanged mixed-page metadata, not permission to accept
    # its paid bodies. Preserve the visible gap and its shorter recheck timer.
    restricted_page = response.status == 304 and task.get("error_code") == "bulk_response_contains_restricted_text"
    if response.status == 304 and bulk and not restricted_page:
        # A transient failure can replace the page-level error while the last
        # successful mixed response and its item errors remain authoritative.
        marks = ','.join('?' for _ in BULK_RESTRICTED_ITEM_ERRORS)
        restricted_page = bool(queue.db.execute(
            'SELECT 1 FROM bulk_item_errors WHERE request_url=? AND provider=? '
            f'AND error_code IN ({marks}) LIMIT 1',
            (task['url'], source.provider, *sorted(BULK_RESTRICTED_ITEM_ERRORS))).fetchone())
    rss_metadata_only = response.status == 304 and task.get('error_code') == XML_BODY_METADATA_FALLBACK
    total, pages = None, None
    if bulk and response.status == 200:
        try:
            if task["kind"] == "cnyes_news":
                cnyes_page_info, posts = cnyes_pagination(response)
                total, pages, page_number = (cnyes_page_info[k] for k in ("total", "pages", "page"))
                if len(posts) != len(records) + len(errors):
                    raise ValueError("unaccounted Cnyes records")
                previous = queue.db.execute("SELECT page FROM bulk_page_evidence WHERE scope_url=? AND total=? AND ids_sha256=? AND page!=?",
                                            (cnyes_page_info["scope"], total, cnyes_page_info["ids_sha256"], page_number)).fetchone()
                if posts and previous:
                    raise NewsError("bulk_pagination_no_progress", retry=True, delay=3600)
            elif task["kind"] == "rss_fulltext":
                page_number = feed_page(task["url"])
                identities = sorted(r["requested_url"] for r in records) + sorted(e["url"] or "" for e in errors)
                rss_page_info = {"scope": wordpress_base(task["url"]), "page": page_number,
                                 "rows": len(identities), "ids_sha256": hashlib.sha256(json.dumps(identities).encode()).hexdigest()}
                previous = queue.db.execute("SELECT page FROM bulk_page_evidence WHERE scope_url=? AND ids_sha256=? AND page!=?",
                                            (rss_page_info["scope"], rss_page_info["ids_sha256"], page_number)).fetchone()
                if identities and previous:
                    raise NewsError("bulk_pagination_no_progress", retry=True, delay=3600)
            else:
                total = int(response.headers["x-wp-total"])
                pages = int(response.headers["x-wp-totalpages"])
                page_number = wordpress_page(task["url"])
                if total < 0 or not 0 <= pages <= 100000 or (total + 99) // 100 != pages:
                    raise ValueError("invalid WordPress page totals")
                expected = min(100, max(0, total - (page_number - 1) * 100))
                if page_number > max(1, pages) or len(records) + len(errors) != expected:
                    raise ValueError("truncated WordPress page")
            if len({r["article_id"] for r in records}) != len(records):
                raise ValueError("duplicate WordPress article identifiers")
        except (ValueError, KeyError):
            raise NewsError("bulk_pagination_totals_invalid", retry=True, delay=60) from None
        if (cnyes_page_info and 'startAt=' in task['url'] and not records
                and all(e['error'] in {'bulk_body_not_provided', 'bulk_exact_duplicate_row'} for e in errors)):
            # Empty and metadata-only pages can be identified before any raw
            # envelope or fallback URL is written. Fence the source check now
            # so recovery can finish even if later registration is interrupted.
            from downloader.news_fulltext_cnyes_checkpoint import register_no_body
            no_body_checkpoint = register_no_body(
                queue, task, response, source, utc_now(),
                discovery_fingerprint(source, kind='cnyes_news'))
        if errors:
            with queue.db:
                if _locked_discovery_head_matches(queue, task, head_fields):
                    queue.db.execute("DELETE FROM bulk_item_errors WHERE request_url=?", (task["url"],))
                    queue.db.executemany("INSERT INTO bulk_item_errors VALUES (?,?,?,?,?,?)",
                                         ((task["url"], e["item_number"], source.provider, e["url"], e["error"], utc_now()) for e in errors))
            if any(e["error"] in BULK_RESTRICTED_ITEM_ERRORS for e in errors):
                # Do not persist a shared response containing explicitly
                # restricted text, including a protected WordPress row. One
                # refusal must not cut off later RSS/WordPress/Cnyes pages.
                # Pagination was validated above; only eligible identities
                # advance. Free bodies use canonical article HTML instead of
                # saving any part of this mixed raw envelope.
                restricted_page = True
                restricted_urls = {e["url"] for e in errors if e["error"] in BULK_RESTRICTED_ITEM_ERRORS}
                links = [link for link in links if link[0] not in restricted_urls]
                records = []
                queue.metrics.count(source.provider, "restricted_pages_metadata_only")
            # Retain one private compressed source envelope for re-analysis
            # instead of paying another source request to diagnose lost fields.
            digest = hashlib.sha256(response.body).hexdigest()
            relative = "quarantine/" + digest + ".parquet"
            target = queue.root / relative
            # Metadata-only is a property of the XML response, not a claim
            # that every item's identity could be parsed. Never retain the
            # shared body merely because a different item also failed.
            rss_metadata_only = bool(rss_page_info and any(
                e.get('xml_body_metadata_only') is True or e['error'] == XML_BODY_METADATA_FALLBACK
                for e in errors))
            if rss_metadata_only and records:
                raise NewsError('invalid_feed_metadata_only_records')
            if not restricted_page and not rss_metadata_only and not target.is_file():
                atomic_write_parquet(target, table_from_records([{
                    "request_url": response.url, "response_sha256": digest, "raw_response_body": response.body,
                    "captured_at_utc": utc_now(), "authorization_fingerprint": source.authorization_fingerprint,
                    "authorization_json": json.dumps(source.authorization, ensure_ascii=False, sort_keys=True),
                    "errors_json": json.dumps(errors, ensure_ascii=False), "publish": False,
                }]), compression="zstd", compression_level=9, durable=True)
                atomic_write_json(queue.root / (relative + ".receipt.json"), {
                    "parquet_path": relative, "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
                    "response_sha256": digest, "article_count": len(records) + len(errors),
                    "accepted_body_count": len(records), "publish": False,
                })
            from downloader.news_fulltext_metrics import bulk_body_page_classification
            page_metric = bulk_body_page_classification(records, errors, cnyes=bool(cnyes_page_info),
                                                       rss_metadata_only=rss_metadata_only)
            metadata_only_page = (page_metric == 'bulk_metadata_only_html_fallback_pages'
                                  or page_metric == 'rss_xml_controls_metadata_only_pages'
                                  and any(e['error'] == XML_BODY_METADATA_FALLBACK and e.get('url') for e in errors))
            if page_metric:
                queue.metrics.count(source.provider, page_metric)
            if not restricted_page and not records and not metadata_only_page and (total or rss_page_info and rss_page_info["rows"]):
                # A systematic parser/byline failure must not consume every
                # historical page. Keep eligible HTML fallback links first;
                # this circuit affects only the bulk adapter, not article work.
                with queue.db:
                    fallback_links = _fresh_article_links(queue, task, head_fields, links, records, errors)
                    queue.add_articles([{"url": url, "title": title, "date": date} for url, title, date in fallback_links])
                if rss_metadata_only:
                    # A single malformed XML body with no usable article URL
                    # proves only that this page cannot advance. Preserve the
                    # per-page retry; do not trip the provider-wide parser
                    # circuit and strand other independently registered feeds.
                    raise NewsError(XML_BODY_NO_USABLE_IDENTITY, retry=True, delay=3600)
                raise NewsError("bulk_all_items_unusable", retry=True, delay=3600)
    # Queue article identities first, but do not mark this source page complete
    # until the full bodies and their acceptance receipts are durably committed.
    journal_eligible = (task['kind'] in {'rss_fulltext', 'wordpress_posts'} and response.status == 200
                        and not restricted_page and not rss_metadata_only and (records or not errors))
    inline_journal_eligible = (journal_eligible and isinstance(response.body, bytes)
                               and len(response.body) <= _BULK_INLINE_JOURNAL_MAX_BYTES)
    accepted, unchanged = None, []
    bulk_no_body_checkpoint = None
    journal_attempted_in_registration = False
    with queue.metrics.measure("discovery_registration"), queue.db:
        article_links = [(url, title, date) for url, title, date in links if title not in {"sitemap", "publisher_index"}]
        article_links = _fresh_article_links(queue, task, head_fields, article_links, records, errors)
        if article_links:
            queue.add_articles([{"url": url, "title": title, "date": date} for url, title, date in article_links])
        queue.db.executemany("INSERT OR IGNORE INTO discovery(url,kind,provider) VALUES (?,'sitemap',?)",
                             ((url, source.provider) for url, title, _ in links if title == "sitemap"))
        queue.db.executemany("INSERT OR IGNORE INTO discovery(url,kind,provider) VALUES (?,'index',?)",
                             ((url, source.provider) for url, title, _ in links if title == "publisher_index"))
        if inline_journal_eligible:
            # Combining the identity writes and journal is safe only if no
            # article requires its own independent revalidation or receipt.
            accepted, unchanged = _select_discovery_records(queue, task, records, defer_unchanged=True)
            if not accepted and not unchanged:
                from downloader.news_fulltext_bulk_checkpoint import register_no_body
                journal_attempted_in_registration = True
                if not queue.db.in_transaction:
                    queue.db.execute('BEGIN IMMEDIATE')
                bulk_no_body_checkpoint = register_no_body(
                    queue, task, response, source,
                    task.get('_discovery_response_observed_at_utc') or (records[0]['captured_at_utc'] if records else utc_now()),
                    discovery_fingerprint(source, kind=task['kind']), in_registration_transaction=True)
    if accepted is None:
        accepted, _ = _select_discovery_records(queue, task, records)
    else:
        for current, observed_at_utc in unchanged:
            queue.unchanged(current, observed_at_utc=observed_at_utc,
                            request_started_at_utc=task.get('_discovery_request_started_at_utc'))
    accepted_batch = None
    bulk_checkpoint_batch = None
    if accepted:
        accepted[0]["raw_response_body"] = response.body
        for record in accepted:
            record["source_response_article_id"] = accepted[0]["article_id"]
        if cnyes_page_info and 'startAt=' in task['url']:
            from downloader.news_fulltext_cnyes_checkpoint import witness
            checkpoint = witness(task, response, source, accepted[0]['captured_at_utc'],
                                 discovery_fingerprint(source, kind='cnyes_news'))
            if checkpoint is not None:
                accepted[0]['cnyes_page_checkpoint_witness_json'] = checkpoint
            accepted_batch = queue.accept(accepted, return_batch=True)
        elif task['kind'] in {'rss_fulltext', 'wordpress_posts'}:
            from downloader.news_fulltext_bulk_checkpoint import witness
            checkpoint = witness(task, response, source, accepted[0]['captured_at_utc'],
                                 discovery_fingerprint(source, kind=task['kind']))
            if checkpoint is not None:
                accepted[0]['bulk_page_checkpoint_witness_json'] = checkpoint
            bulk_checkpoint_batch = queue.accept(accepted, return_batch=True)
        else:
            queue.accept(accepted)
    elif journal_eligible and not journal_attempted_in_registration:
        # Journal only an eligible, successfully parsed page that did not
        # produce a new accepted batch. Restricted or metadata-only envelopes
        # are never retained by this path.
        from downloader.news_fulltext_bulk_checkpoint import register_no_body
        bulk_no_body_checkpoint = register_no_body(
            queue, task, response, source,
            task.get('_discovery_response_observed_at_utc') or (records[0]['captured_at_utc'] if records else utc_now()),
            discovery_fingerprint(source, kind=task['kind']))
    history_artifact = None
    if cnyes_page_info and 'startAt=' in task['url']:
        if no_body_checkpoint is None and not accepted and not restricted_page:
            from downloader.news_fulltext_cnyes_checkpoint import register_no_body
            no_body_checkpoint = register_no_body(
                queue, task, response, source,
                records[0]['captured_at_utc'] if records else utc_now(),
                discovery_fingerprint(source, kind='cnyes_news'))
        from downloader.news_fulltext_cnyes_history import retain_response
        history_artifact = retain_response(queue, response, source, accepted=accepted,
                                          restricted=restricted_page, accepted_batch=accepted_batch)
    with queue.db:
        if head_fields:
            # A different completed request or a partition can change this
            # URL while its older response is in flight. Fence the entire
            # coverage/page/head transaction, not just recovery after a crash.
            if not _locked_discovery_head_matches(queue, task, head_fields):
                for path in (accepted_batch['parquet_path'] if accepted_batch else None, no_body_checkpoint):
                    if path is not None:
                        queue.db.execute("UPDATE cnyes_interrupted_page_checkpoints SET state='skipped',"
                                         "outcome='head_superseded' WHERE parquet_path=? AND state='pending'", (path,))
                if bulk_checkpoint_batch:
                    queue.db.execute("UPDATE bulk_interrupted_page_checkpoints SET state='skipped',"
                                     "outcome='head_superseded' WHERE parquet_path=? AND state='pending'",
                                     (bulk_checkpoint_batch['parquet_path'],))
                if bulk_no_body_checkpoint:
                    queue.db.execute("UPDATE bulk_interrupted_page_checkpoints SET state='skipped',"
                                     "outcome='head_superseded' WHERE parquet_path=? AND state='pending'",
                                     (bulk_no_body_checkpoint,))
                queue.counter('discovery_live_page_head_superseded', 1)
                queue.metrics.count(source.provider, 'live_page_head_superseded')
                if cnyes_page_info or task['kind'] == 'cnyes_news':
                    queue.counter('cnyes_live_page_head_superseded', 1)
                return len(accepted)  # Keep independently accepted bodies and their original receipts.
        if rss_page_info:
            # RSS publishes no history total. Observe one actual page at a time,
            # reject repeating IDs and never invent an all-history denominator.
            queue.db.execute("INSERT OR REPLACE INTO bulk_page_evidence VALUES (?,?,?,?,?)",
                             (rss_page_info["scope"], page_number, -1, rss_page_info["ids_sha256"], utc_now()))
            if rss_page_info["rows"]:
                next_url = feed_url(rss_page_info["scope"], page_number + 1)
                queue.db.execute("INSERT OR IGNORE INTO discovery(url,kind,provider,priority) VALUES (?,'rss_fulltext',?,?)",
                                 (next_url, source.provider, page_number + 1))
            queue.metrics.count(source.provider, "bulk_pages_validated")
            queue.metrics.count(source.provider, "bulk_article_ids_validated", rss_page_info["rows"])
            if not source.cnyes_apis:
                queue.db.execute("DELETE FROM bulk_adapter_cooldowns WHERE provider=?", (source.provider,))
        if total is not None:
            base = cnyes_scope(task["url"]) if cnyes_page_info else wordpress_base(task["url"])
            if cnyes_page_info:
                apply_cnyes_page_metadata(queue, task, response, source, cnyes_page_info, posts, history_artifact)
            else:
                queue.observe_bulk_coverage(base, source.provider, total, pages, utc_now())
                queue.db.executemany("INSERT OR IGNORE INTO discovery(url,kind,provider,priority) VALUES (?,'wordpress_posts',?,?)",
                                     ((wordpress_url(base, page), source.provider, page) for page in range(2, pages + 1)))
            if not source.cnyes_apis:
                queue.db.execute("DELETE FROM bulk_adapter_cooldowns WHERE provider=?", (source.provider,))
        if bulk and response.status == 200:
            # RSS has no 'total' field. Its latest successful parse must still
            # replace the previous per-item error projection. A conditional
            # 304 is not new parsing evidence and cannot erase those errors.
            queue.db.execute("DELETE FROM bulk_item_errors WHERE request_url=?", (task["url"],))
            queue.db.executemany("INSERT INTO bulk_item_errors VALUES (?,?,?,?,?,?)",
                                 ((task["url"], e["item_number"], source.provider, e["url"], e["error"], utc_now()) for e in errors))
            queue.counter("bulk_item_errors_observed", len(errors))
        page = feed_page(task["url"]) if task["kind"] == "rss_fulltext" else wordpress_page(task["url"])
        live_index = task['kind'] == 'index' and task['url'] in source.incremental_index_pages
        delay = source.discovery_interval_seconds if task["kind"] == "feed" or bulk and page == 1 or live_index else 86400
        if task["kind"] == "rss_fulltext" and (page > 1
                or wordpress_base(task['url']) in source.historical_fulltext_feeds):
            delay = 365 * 86400
        if bulk and (wordpress_page(task["url"]) > 1 or task["kind"] == "cnyes_news" and "startAt=" in task["url"]):
            delay = 365 * 86400
        if restricted_page:
            delay = 86400  # Visible metadata-only gap, not an accepted body page.
        if rss_metadata_only:
            delay = 86400  # HTML fallback proceeds; source XML repair stays visible.
        queue.db.execute("UPDATE discovery SET due=?,etag=coalesce(?,etag),last_modified=coalesce(?,last_modified),"
                         "response_sha256=coalesce(?,response_sha256),parser_fingerprint=coalesce(?,parser_fingerprint),last_checked_at_utc=?,"
                         "attempts=0,error_code=? WHERE url=?",
                         (time.time() + delay,
                          response.headers.get("etag"), response.headers.get("last-modified"),
                          hashlib.sha256(response.body).hexdigest() if response.status == 200 else None,
                          discovery_fingerprint(source, kind=task['kind']) if response.status == 200 else None,
                          utc_now(), "bulk_response_contains_restricted_text" if restricted_page else
                          XML_BODY_METADATA_FALLBACK if rss_metadata_only else None, task["url"]))
        if accepted_batch is not None:
            from downloader.news_fulltext_cnyes_checkpoint import finish as finish_cnyes_checkpoint
            finish_cnyes_checkpoint(queue, accepted_batch['parquet_path'])
        if no_body_checkpoint is not None:
            from downloader.news_fulltext_cnyes_checkpoint import finish as finish_cnyes_checkpoint
            finish_cnyes_checkpoint(queue, no_body_checkpoint)
        if bulk_checkpoint_batch is not None:
            from downloader.news_fulltext_bulk_checkpoint import finish as finish_bulk_checkpoint
            finish_bulk_checkpoint(queue, bulk_checkpoint_batch['parquet_path'])
        if bulk_no_body_checkpoint is not None:
            from downloader.news_fulltext_bulk_checkpoint import finish as finish_bulk_checkpoint
            finish_bulk_checkpoint(queue, bulk_no_body_checkpoint)
        if (cnyes_page_info and not restricted_page and not records and errors
                and all(e['error'] in {'bulk_body_not_provided', 'bulk_exact_duplicate_row'} for e in errors)):
            # A successful live parse already proved this response cannot add
            # API bodies under this contract. Do not immediately read/parse it
            # again from quarantine; pending original HTML work is unchanged.
            from downloader.news_fulltext_replay import checkpoint_no_body_cnyes_replay
            checkpoint_no_body_cnyes_replay(queue, source, hashlib.sha256(response.body).hexdigest())
    if no_body_checkpoint is not None:
        from downloader.news_fulltext_cnyes_checkpoint import cleanup_no_body_intent
        cleanup_no_body_intent(queue, no_body_checkpoint)
    return len(accepted)


def fail_discovery(queue: NewsQueue, task: dict, error: NewsError, *, expected_head_fields=()):
    deferred = (getattr(error, 'request_deferred', False) is True and error.retry is True
                and error.code in REQUEST_DEFERRAL_CODES)
    if deferred:
        # This hop was held before it was sent. Earlier redirect or robots
        # requests, if any, remain separately counted by NewsHTTP. Keep the
        # prior failure burst, source outcome/check clock and any newer row
        # deadline. The old outcome may be required to interpret a later 304;
        # a scheduler gate is not evidence for an endpoint-wide failure.
        with queue.db:
            guard = ''.join(' AND ' + key + ' IS ?' for key in expected_head_fields)
            changed = queue.db.execute('UPDATE discovery SET due=max(due,?) WHERE url=?' + guard,
                (time.time() + max(0, error.delay), task['url'],
                 *(task[key] for key in expected_head_fields))).rowcount
            if expected_head_fields and not changed:
                queue.counter('discovery_failure_superseded', 1)
                queue.metrics.count(task['provider'], 'discovery_failure_superseded')
                return
        queue.metrics.count(task['provider'], 'discovery_source_gate_deferrals')
        queue.metrics.count(task['provider'], 'discovery_deferred_' + error.code)
        return
    attempts = task["attempts"] + 1
    # Persistent refusals remain visible and do not hammer every cycle.
    delay = max(error.delay, min(3600, 30 * 2 ** min(attempts, 7))) if error.retry else 86400
    with queue.db:
        guard = ''.join(' AND ' + key + ' IS ?' for key in expected_head_fields)
        changed = queue.db.execute("UPDATE discovery SET due=?,attempts=?,error_code=?,last_checked_at_utc=? WHERE url=?" + guard,
                                   (time.time() + delay, attempts, error.code, utc_now(), task["url"],
                                    *(task[key] for key in expected_head_fields))).rowcount
        if expected_head_fields and not changed:
            queue.counter('discovery_failure_superseded', 1)
            queue.metrics.count(task['provider'], 'discovery_failure_superseded')
            return
        from downloader.news_fulltext_cnyes_index import ENDPOINT, LOCAL_EVIDENCE_FAILURES, is_index_task
        local_index_evidence = is_index_task(task) and error.code in LOCAL_EVIDENCE_FAILURES
        local_feed_evidence = (task['kind'] == 'metadata_feed' and error.code in {
            'metadata_feed_receipt_mismatch', 'unverifiable_metadata_feed_not_modified'})
        if local_index_evidence or local_feed_evidence:
            # A conditional 304 cannot repair missing evidence. The next
            # source request must obtain a complete response, while the
            # evidence failure and normal bounded backoff remain visible.
            queue.db.execute('UPDATE discovery SET etag=NULL,last_modified=NULL WHERE url=?', (task['url'],))
        if task['kind'] == 'metadata_feed':
            from downloader.news_fulltext_metadata_feeds import record_failure_observation
            record_failure_observation(queue, task, error)
        if is_index_task(task) and not local_index_evidence:
            source = queue.registry.sources[task['provider']]
            fingerprint = getattr(source,'cnyes_index_data_fingerprint','')
            if fingerprint:
                from downloader.news_fulltext_cnyes_index import (
                    record_request_failure_scope, reconcile_inherited_metadata_deadline,
                )
                existing = (queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns WHERE provider=? AND endpoint=?',
                    (source.provider, ENDPOINT)).fetchone() if error.code == 'cnyes_index_unverified_metadata' else None)
                deadline = time.time() + delay
                keep_global = (error.code == 'cnyes_index_unverified_metadata' and existing
                    and existing['error_code'] != 'cnyes_index_unverified_metadata'
                    and existing['not_before'] > time.time())
                if not keep_global:
                    queue.db.execute('INSERT INTO bulk_endpoint_cooldowns VALUES (?,?,?,?,?,?) '
                    'ON CONFLICT(provider,endpoint) DO UPDATE SET data_fingerprint=excluded.data_fingerprint,'
                    'not_before=max(not_before,excluded.not_before),error_code=excluded.error_code,request_url=excluded.request_url',
                    (source.provider,ENDPOINT,fingerprint,deadline,error.code,task['url']))
                if error.code == 'cnyes_index_unverified_metadata':
                    from downloader.news_fulltext_diagnostics import index_failure_metadata
                    record_request_failure_scope(queue, source, {'provider':source.provider, 'endpoint':ENDPOINT,
                        'data_fingerprint':fingerprint, 'not_before':deadline,
                        'error_code':error.code, 'request_url':task['url']},
                        metadata_diagnostic=index_failure_metadata(queue, task, error))
                else:
                    reconcile_inherited_metadata_deadline(queue, source)
        elif not is_index_task(task) and error.code in {"bulk_all_items_unusable", "bulk_pagination_no_progress", "bulk_pagination_totals_invalid"}:
            source = queue.registry.sources[task["provider"]]
            if task["kind"] == "cnyes_news" and source.cnyes_apis:
                queue.db.execute("INSERT INTO bulk_endpoint_cooldowns VALUES (?,?,?,?,?,?) "
                                 "ON CONFLICT(provider,endpoint) DO UPDATE SET data_fingerprint=excluded.data_fingerprint,"
                                 "not_before=max(not_before,excluded.not_before),error_code=excluded.error_code,request_url=excluded.request_url",
                                 (source.provider, task["url"].split("?", 1)[0], source.data_fingerprint, time.time() + delay, error.code, task["url"]))
                queue.metrics.count(source.provider, "bulk_endpoint_parser_circuits")
            else:
                queue.db.execute("INSERT OR REPLACE INTO bulk_adapter_cooldowns VALUES (?,?,?,?)",
                                 (source.provider, source.data_fingerprint, time.time() + delay, error.code))
    if getattr(error, 'failure_response', None) is not None:
        from downloader.news_fulltext_diagnostics import retain_index_failure_response
        retain_index_failure_response(queue, task, error)


def refresh_disk_admission(queue, config):
    """Use both actual backing and filesystem space; never block the owner on WSL."""
    from downloader.news_fulltext_disk import BackingDiskSampler
    sampler = getattr(queue, '_backing_disk_sampler', None)
    if sampler is None:
        sampler = queue._backing_disk_sampler = BackingDiskSampler(queue.root)
    sample = sampler.sample(shutil.disk_usage(queue.root).free)
    free = sample['free_bytes']
    required = math.ceil(config['min_free_disk_bytes'])  # Integer free bytes must meet the original threshold.
    checked = utc_now()
    if sample['state'] == 'available':
        from datetime import datetime, timedelta
        checked = (datetime.fromisoformat(checked) - timedelta(seconds=sample['backing_age_seconds'])).isoformat()
    queue.disk_admission = {'contract_version': 1, 'checked_at_utc': checked,
        'free_bytes': free, 'required_free_bytes': required,
        'shortfall_bytes': None if free is None else max(0, required-free),
        'fetch_allowed': free is not None and free >= required,
        'filesystem_free_bytes': sample['filesystem_free_bytes'],
        'backing_free_bytes': sample['backing_free_bytes'],
        'backing_age_seconds': sample.get('backing_age_seconds'),
        'physical_backing_state': sample['state'],
        'physical_probe_inflight': sample['inflight'],
        'automatic_recheck_seconds': math.ceil(max(10, config.get('service_idle_max_seconds', 60)))}
    return not queue.disk_admission['fetch_allowed']


def _metadata_legacy_delay(result):
    """Run productive local slices promptly; back off when nothing advances."""
    if result and result['ready'] and (result.get('scanned', 0) or result.get('processed', 0)):
        return 0.0
    return .05


def run_cycle(queue: NewsQueue, http: NewsHTTP, config: dict, *, max_articles: int, max_discovery_requests: int = 64,
              executor=None, max_run_seconds: float | None = None, provider_ids: set[str] | None = None,
              continuous_budgets: bool = False, refresh_metadata=None, defer_final_status: bool = False,
              idle_wake=None):
    if provider_ids is not None and (not provider_ids or any(
            p not in queue.registry.sources or not queue.registry.sources[p].request_enabled for p in provider_ids)):
        raise ValueError("cycle provider selection must contain authorized, configured providers")
    started, accepted, claimed = time.monotonic(), 0, 0

    def cycle_stopping():
        # The duration bounds new admissions, not a hard I/O timeout. A local
        # operation or an already-claimed request may overrun; let it finish
        # and drain, but do not start another once the boundary is observed.
        return (http.stop.is_set() or max_run_seconds is not None
                and time.monotonic() - started >= max_run_seconds)

    http.metrics = queue.metrics
    if continuous_budgets and config.get("adaptive_rate", {}).get("enabled") and http.capacity_controller is None:
        from downloader.news_fulltext_capacity import PublisherCapacity
        http.capacity_controller = PublisherCapacity(http, config)
    queue.http_capacity_snapshot = http.capacity_snapshot
    queue.provider_cooldown_snapshot = http.cooldown_snapshot
    queue.http_traffic_snapshot = http.traffic_snapshot
    requests_at_start = dict(http.requests_by_provider)
    initial_schedule_pending = True
    discovery_claimed, discovery_inflight = 0, set()
    article_budget_used, discovery_budget_used, budget_rollovers = 0, 0, 0
    last_maintenance = started
    from downloader.news_fulltext_metadata_ingest import METADATA_SLICE_INTERVAL_SECONDS
    last_metadata_inventory = started
    last_cached_replay = started - 10
    next_failure_replay = started
    failure_replay_continuation = False
    last_metadata_replay = started
    next_discovery_error_rollup = started
    initial_metadata_replay_pending = True
    from downloader.news_fulltext_repairs import PUMP_INTERVAL_SECONDS, CONTINUATION_INTERVAL_SECONDS
    from downloader.news_fulltext_url_metadata import local_retry_delay, local_retry_ready
    from downloader.news_fulltext_wake import mailbox_scan_retry_delay, mailbox_scan_retry_due, mailbox_scan_retry_ready
    last_reviewed_repair = started - PUMP_INTERVAL_SECONDS
    next_repair_continuation = started
    next_cnyes_work_backfill = started
    cnyes_work_pending = False
    next_cnyes_history_backfill = started
    cnyes_history_pending = False
    cnyes_history_retry_epoch = None
    next_cnyes_checkpoint_recovery = started
    cnyes_checkpoint_ready = False
    cnyes_checkpoint_retry_epoch = None
    next_metadata_checkpoint_recovery = started
    metadata_checkpoint_ready = False
    metadata_checkpoint_retry_epoch = None
    next_bulk_checkpoint_recovery = started
    bulk_checkpoint_ready = False
    bulk_checkpoint_retry_epoch = None
    next_metadata_legacy_backfill = started
    metadata_legacy_retry_epoch = None
    last_original_resolution = started - config['original_url_resolution_interval_seconds']
    pending_records, futures = [], {}
    pending_bytes, pending_raw_bytes, last_flush, last_status = 0, 0, time.monotonic(), 0
    active = {provider: 0 for provider, source in queue.registry.sources.items()
              if source.request_enabled and (provider_ids is None or provider in provider_ids)}
    priority_providers = tuple(provider for provider in config.get('provider_priority', ()) if provider in active)
    priority_set = frozenset(priority_providers)
    metadata_checkpoint_enabled = any(queue.registry.sources[provider].metadata_feeds for provider in active)
    bulk_checkpoint_enabled = any(queue.registry.sources[provider].fulltext_feeds
                                  or queue.registry.sources[provider].wordpress_apis for provider in active)
    metadata_legacy_providers = deque(provider for provider in active if queue.registry.sources[provider].metadata_feeds)
    metadata_legacy_due = {provider: 0.0 for provider in metadata_legacy_providers}
    metadata_legacy_ready = continuous_budgets and bool(metadata_legacy_providers)
    provider_order = deque(active)
    # Sample before requeueing complete articles or seeding discovery. A
    # blocked cycle cannot dispatch either, so repeating those writes while
    # waiting for backing space only consumes the reserve and changes status.
    low_disk, last_disk_check = False, started - 10
    # A one-shot diagnostic cycle must not consume attempts on these old
    # unsupported heads either. A bounded, resumable sweep precedes CMoney
    # dispatch in both service and one-shot modes.
    cmoney_namespace_done = 'cmoney' not in active
    next_cmoney_namespace_slice = started
    pool = ThreadPoolExecutor(max_workers=config["workers"], thread_name_prefix="news-fulltext") if executor is None else nullcontext(executor)
    with pool as executor:
        while True:
            now = time.monotonic()
            if http.capacity_controller:
                http.capacity_controller.tick()
            # With multiple publishers a resident owner keeps short client
            # waits out of shared workers. A single publisher has nobody to
            # block, so its worker can wait directly without extra queue scans.
            # One-shot diagnostics retain their bounded cycle semantics.
            provider_deadlines = (http.scheduling_cooldown_snapshot() if continuous_budgets and len(active) > 1
                                  else http.cooldown_snapshot())
            provider_deadlines_at = time.time()
            # Work limits are diagnostic bounds, not source quotas. A resident
            # owner renews its scheduling quantum without waiting for unrelated
            # publishers' slow responses. One queue writer and HTTP pacing stay
            # authoritative; stop/duration still drain all leased outcomes.
            if continuous_budgets and (max_articles > 0 and article_budget_used >= max_articles
                                       or max_discovery_requests > 0 and discovery_budget_used >= max_discovery_requests):
                article_budget_used = discovery_budget_used = 0
                budget_rollovers += 1
            maintenance_due = now - last_maintenance >= config.get('service_cycle_seconds', 180)
            # A committed metadata batch has more work behind it. If the
            # dispatch round has already run and no completed HTTP response
            # needs its durable handoff, offer the next original batch now.
            # Requests can run in parallel; recheck all source dispatch and
            # completed responses between slices.
            metadata_fast_due = (getattr(queue, 'metadata_inventory_made_progress', False)
                                 and not initial_schedule_pending and not low_disk
                                 and not pending_records and not any(f.done() for f in futures))
            if (continuous_budgets and refresh_metadata and not http.stop.is_set()
                    and (max_run_seconds is None or now - started < max_run_seconds) and (
                    maintenance_due or getattr(queue, 'metadata_inventory_pending', False)
                    and (metadata_fast_due or now - last_metadata_inventory >= METADATA_SLICE_INTERVAL_SECONDS))):
                # One original 4k-row commit can overrun the soft local budget,
                # but never ingest the whole large file before HTTP dispatch.
                queue.metadata_inventory_made_progress = False
                refresh_metadata()
                last_metadata_inventory = time.monotonic()
                now = last_metadata_inventory
            if now - last_disk_check >= 10:
                low_disk = refresh_disk_admission(queue, config)
                last_disk_check = now
            if (not low_disk and not cycle_stopping()
                    and (initial_schedule_pending or continuous_budgets and maintenance_due)):
                queue.schedule_recent_rechecks(config["recent_article_recheck_seconds"], config["recent_article_window_days"])
                if not cycle_stopping():
                    seed_discovery(queue)
                    initial_schedule_pending = False
                    last_maintenance = time.monotonic()
            bulk_rollover = False
            if (not cycle_stopping() and not continuous_budgets and max_discovery_requests > 0
                    and discovery_budget_used >= max_discovery_requests):
                for provider in provider_order:
                    source = queue.registry.sources[provider]
                    if not (source.wordpress_apis or source.cnyes_apis or source.fulltext_feeds) or provider_deadlines.get(provider, 0) > time.time():
                        continue
                    if not queue.bulk_allowed(source):
                        continue
                    dispatch_filter, parameters = queue.discovery_dispatch_filter(source)
                    if queue.db.execute("SELECT 1 FROM discovery WHERE provider=? AND superseded_by_windows_json IS NULL "
                                        "AND kind IN ('wordpress_posts','cnyes_news','rss_fulltext') AND due<=? AND " + dispatch_filter + " LIMIT 1",
                                        (provider, time.time(), *parameters)).fetchone():
                        bulk_rollover = True
                        break
            draining = (cycle_stopping()
                        or not continuous_budgets and max_articles > 0 and article_budget_used >= max_articles or bulk_rollover)
            regular_repair_poll = now - last_reviewed_repair >= PUMP_INTERVAL_SECONDS
            repair_continuation_due = (getattr(queue, 'reviewed_repair_continuation_pending', False)
                                       and now >= next_repair_continuation)
            url_ready = local_retry_ready(queue)
            url_continuation_due = url_ready and getattr(queue, '_url_metadata_continuation_pending', False)
            url_retry_due = url_ready and bool(getattr(queue, '_url_metadata_retry_at', 0.0))
            url_poll_due = (regular_repair_poll and url_ready) or url_continuation_due or url_retry_due
            repair_scan_due = mailbox_scan_retry_due(queue, 'repair')
            candidate_scan_due = mailbox_scan_retry_due(queue, 'candidate')
            if (continuous_budgets and not draining and not low_disk and provider_ids is None
                    and (regular_repair_poll or repair_continuation_due or url_poll_due
                         or repair_scan_due or candidate_scan_due)):
                from downloader.news_fulltext_repairs import consume_repairs
                with queue.metrics.measure('reviewed_repair_admission'):
                    # A body already received with no in-flight work must be
                    # durably handed off before another continuation slice.
                    defer_url_for_handoff = (not regular_repair_poll and (url_continuation_due or url_retry_due)
                                             and pending_records and not futures)
                    if url_poll_due and not defer_url_for_handoff:
                        from downloader.news_fulltext_url_metadata import consume_urls
                        consume_urls(queue, stop=http.stop)
                    if ((regular_repair_poll or repair_continuation_due or repair_scan_due)
                            and mailbox_scan_retry_ready(queue, 'repair') and not cycle_stopping()):
                        consume_repairs(queue, stop=http.stop)
                        next_repair_continuation = time.monotonic() + CONTINUATION_INTERVAL_SECONDS
                    if ((regular_repair_poll or candidate_scan_due)
                            and mailbox_scan_retry_ready(queue, 'candidate') and not cycle_stopping()):
                        from downloader.news_fulltext_candidate_diagnostics import pump_candidates
                        pump_candidates(queue, stop=http.stop)
                    if (regular_repair_poll and config.get('retained_link_handoff') and not cycle_stopping()
                            and not (pending_records and not futures)):
                        from downloader.news_fulltext_retained_handoff import pump as pump_retained_link_handoff
                        with queue.metrics.measure('retained_link_handoff'):
                            pump_retained_link_handoff(queue, config['retained_link_handoff'])
                if regular_repair_poll:
                    last_reviewed_repair = time.monotonic()
                draining = draining or cycle_stopping()
            # Give the dispatcher a chance to batch more HTTP responses; if
            # no request remains, hand off buffered bodies before a new local
            # lookup slice can delay their receipt.
            if (continuous_budgets and not draining and not low_disk
                    and 'cnyes' in active and now >= next_cnyes_work_backfill):
                if pending_records and not futures:
                    # A due periodic scan may have no journal yet. Preserve
                    # its wake so the cycle does not exit after the handoff.
                    cnyes_work_pending = True
                else:
                    from downloader.news_fulltext_cnyes_work import backfill as backfill_cnyes_work, maintenance_delay
                    result = backfill_cnyes_work(queue, stop=http.stop)
                    next_cnyes_work_backfill = time.monotonic() + maintenance_delay(result)
                    cnyes_work_pending = bool(result['checked_heads'] or result.get('seek_continuation'))
                    draining = draining or cycle_stopping()
            if (continuous_budgets and not draining and not low_disk and 'cnyes' in active
                    and (now >= next_cnyes_history_backfill
                         or getattr(queue, '_cnyes_original_priority_needs_wake', False))):
                from downloader.news_fulltext_cnyes_history import (
                    backfill as backfill_cnyes_history, maintenance_delay as cnyes_history_maintenance_delay,
                )
                from downloader.news_fulltext_cnyes_priority import seed as seed_cnyes_original_priority
                from downloader.news_fulltext_cnyes_batch_priority import seed as seed_cnyes_batch_priority
                # A head can join a watched observation after its scan cursor
                # passed. Its durable journal wakes this owner, not a second
                # downloader; rollback only causes a harmless extra poll.
                queue._cnyes_original_priority_needs_wake = False
                priority = seed_cnyes_original_priority(queue, config.get('cnyes_original_audit_priority_plan'),
                                                        stop=http.stop, limit=128, max_seconds=.01)
                batch_priority = seed_cnyes_batch_priority(queue, config.get('cnyes_accepted_batch_priority_plan'),
                                                            stop=http.stop, limit=128, max_seconds=.01)
                # Bound writer time, not fast metadata rows to four per second.
                # This is local evidence maintenance; HTTP pacing and source
                # refusal deadlines remain governed by the existing owner.
                initial_priority_scan = bool(priority and priority['state'] == 'scanning'
                    and priority['processed_targets'] < priority['targets'])
                result = backfill_cnyes_history(queue, stop=http.stop, limit=128, max_seconds=.05,
                                                defer_original_audit=initial_priority_scan)
                priorities = [item for item in (priority, batch_priority) if item and item['state'] == 'scanning']
                if priorities:
                    # Do not let an otherwise idle history scan postpone the
                    # next bounded priority slice for its 300-second sleep.
                    result['exhausted'] = False
                cnyes_history_pending = not result['exhausted']
                cnyes_history_retry_epoch = result.get('next_local_retry_epoch')
                next_cnyes_history_backfill = time.monotonic() + (
                    0 if any(item['slice_steps'] for item in priorities)
                    else cnyes_history_maintenance_delay(result))
                draining = draining or cycle_stopping()
            # If a known recovery backlog and completed bodies coexist, give
            # the dispatcher a chance to fill the batch first. When no online
            # work is ready, the corresponding *_checkpoint_only path flushes
            # the bodies before another local recovery slice starts.
            if (not draining and not low_disk and 'cnyes' in active
                    and now >= next_cnyes_checkpoint_recovery
                    and not (cnyes_checkpoint_ready and pending_records and not futures)):
                from downloader.news_fulltext_cnyes_checkpoint import recover as recover_cnyes_checkpoint
                result = recover_cnyes_checkpoint(queue, stop=http.stop)
                cnyes_checkpoint_ready = result['ready']
                cnyes_checkpoint_retry_epoch = result['next_local_retry_epoch']
                # Do not poll an empty indexed frontier twenty times a second.
                # Continue a real backlog promptly; a new interrupted page is
                # already excluded from network dispatch while waiting.
                next_cnyes_checkpoint_recovery = time.monotonic() + (.05 if result['ready'] else 1)
                draining = draining or cycle_stopping()
            if (not draining and not low_disk and metadata_checkpoint_enabled
                    and now >= next_metadata_checkpoint_recovery
                    and not (metadata_checkpoint_ready and pending_records and not futures)):
                from downloader.news_fulltext_metadata_checkpoint import recover as recover_metadata_checkpoint
                result = recover_metadata_checkpoint(queue, stop=http.stop)
                metadata_checkpoint_ready = result['ready']
                metadata_checkpoint_retry_epoch = result['next_local_retry_epoch']
                next_metadata_checkpoint_recovery = time.monotonic() + (.05 if result['ready'] else 1)
                draining = draining or cycle_stopping()
            if (not draining and not low_disk and bulk_checkpoint_enabled
                    and now >= next_bulk_checkpoint_recovery
                    and not (bulk_checkpoint_ready and pending_records and not futures)):
                from downloader.news_fulltext_bulk_checkpoint import recover as recover_bulk_checkpoint
                result = recover_bulk_checkpoint(queue, providers=active, stop=http.stop)
                bulk_checkpoint_ready = result['ready']
                bulk_checkpoint_retry_epoch = result['next_local_retry_epoch']
                next_bulk_checkpoint_recovery = time.monotonic() + (.05 if result['ready'] else 1)
                draining = draining or cycle_stopping()
            # A bounded local slice can observe stop or consume the remaining
            # cycle time. Do not dispatch new work using the older snapshot.
            draining = draining or cycle_stopping()
            if not draining and not low_disk and not cmoney_namespace_done and now >= next_cmoney_namespace_slice:
                with queue.metrics.measure('cmoney_note_detail_path_admission'):
                    result = queue.backfill_cmoney_note_detail_exclusions(stop=http.stop)
                cmoney_namespace_done = result['completed']
                next_cmoney_namespace_slice = time.monotonic() + .1
                draining = draining or cycle_stopping()
            if (continuous_budgets and not draining and not low_disk
                    and now - last_original_resolution >= config['original_url_resolution_interval_seconds']):
                from downloader.news_fulltext_urls import backfill_original_urls, sync_original_url_routes
                options = {'limit': config['original_url_resolution_batch'],
                           'max_seconds': config['original_url_resolution_slice_seconds'], 'stop': http.stop}
                sync_original_url_routes(queue, **options)
                if not cycle_stopping():
                    backfill_original_urls(queue, **options)
                last_original_resolution = time.monotonic()
                draining = draining or cycle_stopping()
            if (continuous_budgets and not draining and not low_disk and config.get('cached_cnyes_replay_enabled', False)
                    and 'cnyes' in active and now - last_cached_replay >= 10):
                from downloader.news_fulltext_replay import replay_cached_cnyes
                recovered = replay_cached_cnyes(queue, stop=http.stop)
                accepted += recovered.get('accepted_heads', 0)
                last_cached_replay = time.monotonic()
                draining = draining or cycle_stopping()
            if (continuous_budgets and not draining and not low_disk
                    and config.get('retained_cnyes_failure_replay_enabled', False)
                    and 'cnyes' in active and now >= next_failure_replay):
                if pending_records and not futures:
                    # Preserve the due local wake, but durably hand off the
                    # last received online bodies before another replay slice.
                    failure_replay_continuation = True
                else:
                    from downloader.news_fulltext_replay import (replay_retained_cnyes_failures,
                                                                 retained_failure_next_wake_delay)
                    recovered = replay_retained_cnyes_failures(queue, stop=http.stop)
                    accepted += recovered.get('accepted_heads', 0)
                    replay_delay = _RETAINED_FAILURE_IDLE_POLL_SECONDS
                    failure_replay_continuation = False
                    if (recovered.get('local_checkpoints', 0) or recovered.get('local_io_deferrals', 0)
                            or recovered.get('local_claim_deferrals', 0)
                            or recovered.get('local_commit_deferrals', 0)):
                        # Only a durable checkpoint or retry permits an expedited
                        # slice. A claim race without progress keeps the ordinary
                        # polling bound, so it cannot turn into a busy loop.
                        replay_delay = retained_failure_next_wake_delay(queue, replay_delay)
                        failure_replay_continuation = replay_delay < _RETAINED_FAILURE_IDLE_POLL_SECONDS
                    next_failure_replay = time.monotonic() + max(_RETAINED_FAILURE_CONTINUATION_MIN_SECONDS, replay_delay)
                    draining = draining or cycle_stopping()
            if (continuous_budgets and not draining and not low_disk
                    and config.get('cached_cnyes_metadata_replay_enabled', False)
                    and 'cnyes' in active and now - last_metadata_replay >= (
                        1 if getattr(queue, '_large_metadata_job', None) else 10)):
                from downloader.news_fulltext_metadata_replay import replay_retained_cnyes_metadata
                # Revisions are not new URL heads or HTTP attempts. The shared
                # metrics label retained work separately from online capacity.
                replay_retained_cnyes_metadata(queue, stop=http.stop)
                last_metadata_replay = time.monotonic()
                initial_metadata_replay_pending = False
                draining = draining or cycle_stopping()
            submitted_in_round, priority_submitted = 0, False
            priority_candidate_offered = set()
            dispatch_order = (priority_providers + tuple(provider for provider in provider_order if provider not in priority_set)
                              if priority_providers else provider_order)
            for provider in () if draining else dispatch_order:
                if cycle_stopping():
                    draining = True
                    break
                source = queue.registry.sources[provider]
                if low_disk or len(futures) >= config["workers"] or active[provider] >= http.parallelism[provider]:
                    continue
                # Avoid leasing a legacy excluded path while its local,
                # resumable classification is in progress. Other providers
                # retain their own slots and continue normally.
                if provider == 'cmoney' and not cmoney_namespace_done:
                    continue
                if provider_deadlines.get(provider, 0) > time.time():
                    spare_for_other = len(active) == 1 or len(futures) + 1 < config['workers']
                    if not (continuous_budgets and spare_for_other
                            and http.may_prefetch_short_local_pacing(provider, _SHORT_PACING_PREFETCH_SECONDS)):
                        continue
                submitted_before_provider = submitted_in_round
                excluded_origins = tuple(http.blocked_robots_origins(provider))
                excluded_hosts = http.blocked_gateway_hosts(provider)
                task = None
                if max_discovery_requests > 0 and (continuous_budgets or discovery_budget_used < max_discovery_requests):
                    bulk_allowed = queue.bulk_allowed(source)
                    candidates = queue.discovery_candidates(source, config['workers'] + 1,
                        bulk_allowed=bulk_allowed, excluded_origins=excluded_origins, excluded_hosts=excluded_hosts)
                    task = next((dict(r) for r in candidates if r["url"] not in discovery_inflight), None)
                    # The read itself may block. No article lease or discovery
                    # preparation has happened yet, so it is safe to defer all
                    # new work and drain other providers' existing futures.
                    if cycle_stopping():
                        draining = True
                        break
                row = None
                historical_bulk = bool(task and (
                    task["kind"] == "rss_fulltext" and (feed_page(task["url"]) > 1
                        or wordpress_base(task['url']) in source.historical_fulltext_feeds)
                    or task["kind"] == "wordpress_posts" and wordpress_page(task["url"]) > 1
                    or task["kind"] == "cnyes_news" and "startAt=" in task["url"]
                    or task['kind'] == 'index' and task['url'].startswith('https://api.cnyes.com/media/api/v1/newslist/all?')))
                if (historical_bulk and max_articles > 0 and (continuous_budgets or article_budget_used < max_articles)
                        and http.bulk_since_article.get(provider, 0) >= config.get("bulk_article_burst", 16)):
                    # Efficient bulk remains the main history lane. A bounded
                    # article slot serves HTML-only gaps and due failures even
                    # while many history pages remain. Current-release feeds
                    # retain priority; an empty lane never idles a bulk slot.
                    row = queue.claim(provider, config["lease_seconds"],
                                      excluded_hosts=excluded_hosts, excluded_origins=excluded_origins,
                                      missing_body_burst=config.get("missing_body_recheck_burst", 8),
                                      dated_body_burst=config.get("dated_body_undated_burst", 8),
                                      pending_body_burst=config.get("pending_body_retry_burst", 8),
                                      stored_body_retry_burst=config.get('stored_body_retry_burst', 8),
                                      cnyes_primary_body_burst=config.get('cnyes_primary_body_burst', 8))
                    if row:
                        task = None
                        queue.metrics.count(provider, "frontier_bulk_fairness_article_dispatches")
                if task and task["kind"] in {"sitemap", "index"} and max_articles > 0 and (continuous_budgets or article_budget_used < max_articles):
                    # Rolling RSS/new-release and useful bulk bodies keep their
                    # priority. Historical link discovery must not starve the
                    # already-known, missing article bodies for hours.
                    if http.articles_since_sitemap.get(provider, 0) < config.get("sitemap_article_ratio", 8):
                        row = queue.claim(provider, config["lease_seconds"],
                                          excluded_hosts=excluded_hosts, excluded_origins=excluded_origins,
                                          missing_body_burst=config.get("missing_body_recheck_burst", 8),
                                          dated_body_burst=config.get("dated_body_undated_burst", 8),
                                          pending_body_burst=config.get("pending_body_retry_burst", 8),
                                          stored_body_retry_burst=config.get('stored_body_retry_burst', 8),
                                          cnyes_primary_body_burst=config.get('cnyes_primary_body_burst', 8))
                        if row:
                            task = None
                if row is None and cycle_stopping():
                    draining = True
                    break
                if task:
                    from downloader.news_fulltext_cnyes_revision import prepare_dispatch
                    prepare_dispatch(queue, task)
                    if cycle_stopping():
                        draining = True
                        break
                    if task['kind'] == 'cnyes_news':
                        from downloader.news_fulltext_cnyes_history import prepare_restricted_history_dispatch
                        prepare_restricted_history_dispatch(queue, task, source,
                            compatible_fingerprint=discovery_fingerprint(source, kind=task['kind']))
                    # These preparations only read evidence and annotate the
                    # task; no request or durable lease has been issued yet.
                    if cycle_stopping():
                        draining = True
                        break
                    futures[executor.submit(fetch_discovery, http, task)] = ("discovery", task)
                    active[provider] += 1
                    discovery_inflight.add(task["url"])
                    discovery_claimed += 1
                    discovery_budget_used += 1
                    submitted_in_round += 1
                    if task["kind"] in {"sitemap", "index"}:
                        http.articles_since_sitemap[provider] = 0
                    if historical_bulk:
                        http.bulk_since_article[provider] = min(config.get("bulk_article_burst", 16),
                                                              http.bulk_since_article.get(provider, 0) + 1)
                elif source.allowed and max_articles > 0 and (continuous_budgets or article_budget_used < max_articles):
                    row = row or queue.claim(provider, config["lease_seconds"],
                                             excluded_hosts=excluded_hosts, excluded_origins=excluded_origins,
                                             missing_body_burst=config.get("missing_body_recheck_burst", 8),
                                             dated_body_burst=config.get("dated_body_undated_burst", 8),
                                             pending_body_burst=config.get("pending_body_retry_burst", 8),
                                             stored_body_retry_burst=config.get('stored_body_retry_burst', 8),
                                             cnyes_primary_body_burst=config.get('cnyes_primary_body_burst', 8))
                    if row:
                        futures[executor.submit(fetch_article, http, row)] = ("article", row)
                        active[provider] += 1
                        claimed += 1
                        article_budget_used += 1
                        submitted_in_round += 1
                        http.articles_since_sitemap[provider] = http.articles_since_sitemap.get(provider, 0) + 1
                        http.bulk_since_article[provider] = 0
                if (provider in priority_set and submitted_in_round == submitted_before_provider
                        and continuous_budgets and provider_ids is None and not cycle_stopping()
                        and provider in getattr(queue, 'native_candidate_ready_providers', ())):
                    # The priority source's ordinary work keeps first choice
                    # within that source. Its ready reviewed candidate gets
                    # the next shared slot before lower-priority history.
                    from downloader.news_fulltext_candidate_diagnostics import claim_candidate, fetch_candidate
                    priority_candidate_offered.add(provider)
                    job = claim_candidate(queue, http, config, provider)
                    if job:
                        futures[executor.submit(fetch_candidate, http, job)] = ('candidate_metadata', job)
                        active[provider] += 1
                        submitted_in_round += 1
                if provider in priority_set and submitted_in_round > submitted_before_provider:
                    # Re-sample real pacing and capacity before another slot.
                    # If the priority source is cooling or has no work, the
                    # next round admits independent lower-priority providers.
                    priority_submitted = True
                    break
            # Candidate metadata uses the SAME HTTP owner, pacer and executor.
            # After priority sources have had their chance, ordinary work
            # precedes candidates from the remaining independent providers.
            draining = draining or cycle_stopping()
            if (continuous_budgets and not priority_submitted and not draining and not low_disk and provider_ids is None
                    and getattr(queue, 'native_candidate_ready_providers', None)):
                from downloader.news_fulltext_candidate_diagnostics import claim_candidate, fetch_candidate
                for provider in dispatch_order:
                    if cycle_stopping():
                        draining = True
                        break
                    if provider == 'cmoney' and not cmoney_namespace_done:
                        continue
                    if provider in priority_candidate_offered:
                        continue
                    if (len(futures) >= config['workers'] or active[provider] >= http.parallelism[provider]
                            or provider_deadlines.get(provider, 0) > time.time()):
                        continue
                    job = claim_candidate(queue, http, config, provider)
                    if job:
                        futures[executor.submit(fetch_candidate, http, job)] = ('candidate_metadata', job)
                        active[provider] += 1
                        submitted_in_round += 1
            provider_order.rotate(-1)
            if metadata_legacy_retry_epoch is not None and metadata_legacy_retry_epoch <= time.time():
                metadata_legacy_ready = True
            if (continuous_budgets and not draining and not low_disk and metadata_legacy_ready
                    and not http.stop.is_set() and time.monotonic() >= next_metadata_legacy_backfill
                    and not (pending_records and not futures)
                    and (max_run_seconds is None or time.monotonic() - started < max_run_seconds)):
                # All online providers get their slots first. Historical RSS
                # recovery remains local, bounded and owned by this writer.
                # With no HTTP work left, keep the legacy lane ready and hand
                # received bodies to the durable batch before its next slice.
                from downloader.news_fulltext_metadata_legacy import backfill as backfill_metadata_legacy
                wall_now = time.time()
                provider = next((p for p in metadata_legacy_providers
                                 if metadata_legacy_due[p] is not None and metadata_legacy_due[p] <= wall_now), None)
                result = None
                if provider is not None:
                    with queue.metrics.measure('metadata_legacy_backfill', provider):
                        result = backfill_metadata_legacy(queue, queue.registry.sources[provider], stop=http.stop)
                    metadata_legacy_due[provider] = result['next_retry_epoch']
                    if not result['ready']:
                        metadata_legacy_providers.rotate(-1)
                remaining_due = [value for value in metadata_legacy_due.values() if value is not None]
                metadata_legacy_retry_epoch = min(remaining_due, default=None)
                metadata_legacy_ready = metadata_legacy_retry_epoch is not None and metadata_legacy_retry_epoch <= time.time()
                next_metadata_legacy_backfill = time.monotonic() + _metadata_legacy_delay(result)
                draining = draining or cycle_stopping()
            if (not draining and not low_disk and not http.stop.is_set()
                    and getattr(queue, '_discovery_error_rollup_pending', False)
                    and time.monotonic() >= next_discovery_error_rollup):
                # Online requests take their slots first. This is a resumable
                # local slice; its time budget is soft for individual I/O.
                queue.advance_discovery_error_rollup()
                next_discovery_error_rollup = time.monotonic() + 1
                draining = draining or cycle_stopping()
            if (not futures and initial_metadata_replay_pending and continuous_budgets and not draining and not low_disk
                    and config.get('cached_cnyes_metadata_replay_enabled', False) and 'cnyes' in active):
                # An idle cycle can end before the periodic ten-second tick.
                # Give online work first dispatch, then perform one bounded
                # local slice rather than resetting its timer on every cycle.
                from downloader.news_fulltext_metadata_replay import replay_retained_cnyes_metadata
                replay_retained_cnyes_metadata(queue, stop=http.stop)
                last_metadata_replay = time.monotonic()
                initial_metadata_replay_pending = False
                draining = draining or cycle_stopping()
            metadata_only = False
            repair_only = False
            url_inbox_only = False
            namespace_only = False
            history_only = False
            cnyes_work_only = False
            checkpoint_only = False
            metadata_checkpoint_only = False
            bulk_checkpoint_only = False
            metadata_legacy_only = False
            failure_replay_only = False
            metadata_inventory_only = False
            if not futures:
                if continuous_budgets and not draining and not low_disk:
                    from downloader.news_fulltext_urls import has_original_url_work
                    metadata_only = has_original_url_work(queue)
                    draining = draining or cycle_stopping()
                    if draining:
                        metadata_only = False
                    else:
                        repair_only = (provider_ids is None and mailbox_scan_retry_ready(queue, 'repair')
                                       and getattr(queue, 'reviewed_repair_continuation_pending', False))
                        url_inbox_only = (provider_ids is None and local_retry_ready(queue)
                                          and (getattr(queue, '_url_metadata_continuation_pending', False)
                                               or getattr(queue, '_url_metadata_retry_at', 0.0)))
                        history_only = 'cnyes' in active and (cnyes_history_pending
                            or getattr(queue, '_cnyes_original_priority_needs_wake', False))
                        if 'cnyes' in active:
                            from downloader.news_fulltext_cnyes_work import has_lookup_rechecks
                            lookup_rechecks = has_lookup_rechecks(queue)
                            cnyes_work_only = cnyes_work_pending or lookup_rechecks
                            if lookup_rechecks:
                                # A transition may commit after the last slice.
                                # Do not let an otherwise idle owner sleep over it.
                                next_cnyes_work_backfill = min(next_cnyes_work_backfill, time.monotonic())
                        metadata_legacy_only = metadata_legacy_ready
                        # Future retries return to the resident service's
                        # global wake calculation so another provider or a
                        # mailbox can become ready first. Only already-due
                        # replay work justifies retaining this inner cycle.
                        failure_replay_only = (failure_replay_continuation
                                               and next_failure_replay <= time.monotonic())
                        metadata_inventory_only = (bool(refresh_metadata) and not pending_records
                                                   and getattr(queue, 'metadata_inventory_pending', False)
                                                   and getattr(queue, 'metadata_inventory_made_progress', False))
                if not draining and not low_disk:
                    namespace_only = not cmoney_namespace_done
                    checkpoint_only = 'cnyes' in active and cnyes_checkpoint_ready
                    metadata_checkpoint_only = metadata_checkpoint_enabled and metadata_checkpoint_ready
                    bulk_checkpoint_only = bulk_checkpoint_enabled and bulk_checkpoint_ready
            # One predicate governs idle continuation, completion waits and
            # durable handoff. Adding a local lane must not accidentally keep
            # already received article bodies in memory during its slices.
            local_only = (metadata_only or repair_only or url_inbox_only or namespace_only or history_only or cnyes_work_only
                          or checkpoint_only or metadata_checkpoint_only or bulk_checkpoint_only or metadata_legacy_only
                          or failure_replay_only or metadata_inventory_only)
            if not futures and not local_only:
                if continuous_budgets and not draining and not low_disk and pending_records:
                    # A short local send gap must neither hold a worker nor
                    # fragment an in-memory batch into one receipt per article.
                    # Continue only when indexed work really wakes within such
                    # a gap. Mail/stop and the existing flush deadline retain
                    # precedence; no source token or article lease is claimed.
                    wall_now = time.time()
                    visible_cooldowns = http.cooldown_snapshot()
                    short_gaps = [deadline - wall_now for provider, deadline in provider_deadlines.items()
                                  if deadline > wall_now
                                  and http.effective_interval(provider) <= config['response_deadline_seconds']
                                  and deadline > visible_cooldowns.get(provider, 0) + .001]
                    if short_gaps:
                        maximum_gap = max(short_gaps)
                        due_delay = queue.next_wake_delay(
                            provider_deadlines, maximum_gap + 1,
                            request_scope_cooldowns=http.request_scope_cooldown_snapshot())
                        if 0 < due_delay < maximum_gap + .1:
                            wait_calculated_at = time.monotonic()
                            # Wake just before a local gap only when another
                            # provider retains a worker, or this is the only
                            # provider. Requests still wait for their real slot.
                            spare_for_other = len(active) == 1 or config['workers'] > 1
                            prefetch = (spare_for_other and any(
                                gap <= due_delay + .005 for gap in short_gaps))
                            until_dispatch = max(0, due_delay - _SHORT_PACING_PREFETCH_SECONDS) if prefetch else due_delay
                            if prefetch and until_dispatch == 0:
                                until_dispatch = min(due_delay, .001)  # Avoid spinning on another future-due task.
                            wait_for_due = min(until_dispatch, 1, max(0, config.get('batch_flush_seconds', 30)
                                                                  - (time.monotonic() - last_flush)))
                            if max_run_seconds is not None:
                                wait_for_due = min(wait_for_due, max(0, max_run_seconds - (time.monotonic() - started)))
                            if wait_for_due > 0:
                                now = time.monotonic()
                                if now - last_status >= 15:
                                    with queue.metrics.measure('status_write'):
                                        queue.status(requests_by_provider=http.requests_by_provider,
                                                     response_bytes=http.response_bytes,
                                                     run_seconds=now - started, accepted_this_run=accepted,
                                                     activity='running', provider_cooldowns=http.cooldown_snapshot())
                                    last_status = time.monotonic()
                                # Publishing itself can take longer than the
                                # remaining gap or service/flush deadline.
                                wait_for_due = max(0, wait_for_due - (time.monotonic() - wait_calculated_at))
                                if wait_for_due <= 0:
                                    continue
                                with queue.metrics.measure('idle_wait'):
                                    if idle_wake is not None:
                                        if idle_wake.wait(wait_for_due):
                                            break  # Durable handoff before newly arrived mailbox work.
                                    else:
                                        http.stop.wait(wait_for_due)
                                continue
                break
            # Fill every independently reserved response slot before sleeping.
            # One submission per publisher per round preserves fairness; when
            # no new job was submitted, block normally instead of CPU polling.
            wait_seconds = 0 if submitted_in_round else 1
            if continuous_budgets and futures and len(futures) < config['workers']:
                # A different publisher's slow response must not hide the
                # instant a paced source can use an otherwise idle worker.
                # Only deadlines that were future in this snapshot can wake
                # this wait; stale/expired source entries cannot cause a spin.
                visible_cooldowns = http.cooldown_snapshot()
                spare_for_other = len(active) == 1 or len(futures) + 1 < config['workers']
                readiness = [deadline - (_SHORT_PACING_PREFETCH_SECONDS if spare_for_other
                              and http.effective_interval(provider) <= config['response_deadline_seconds']
                              and deadline > visible_cooldowns.get(provider, 0) + .001 else 0)
                             for provider, deadline in provider_deadlines.items()
                             if provider in active and active[provider] < http.parallelism[provider]
                             and deadline > provider_deadlines_at]
                if readiness:
                    wait_seconds = min(wait_seconds, max(.001, min(readiness) - time.time()))
                if not submitted_in_round and not draining and not low_disk:
                    # Another provider's row can become due while an HTTP
                    # response is in flight. A free worker must wake for that
                    # queue deadline, not wait for the slow response. Ignore
                    # providers without a slot and work already offered in
                    # this round; those cannot justify repeated empty polls.
                    spare_providers = {provider for provider in active
                                       if active[provider] < http.parallelism[provider]
                                       and (provider != 'cmoney' or cmoney_namespace_done)}
                    request_scopes = http.request_scope_cooldown_snapshot()
                    try:
                        if spare_providers and (max_articles > 0 or max_discovery_requests > 0):
                            delay = queue.next_wake_delay(
                                provider_deadlines, wait_seconds, request_scope_cooldowns=request_scopes,
                                provider_ids=spare_providers, include_articles=max_articles > 0,
                                include_discovery=max_discovery_requests > 0, include_local=False,
                                dispatch_after=provider_deadlines_at)
                            wait_seconds = min(wait_seconds, max(.001, delay))
                        if (provider_ids is None and spare_providers
                                and getattr(queue, 'native_candidate_ready_providers', None)):
                            from downloader.news_fulltext_candidate_diagnostics import candidate_next_wake_delay
                            delay = candidate_next_wake_delay(
                                queue, provider_deadlines, request_scopes, wait_seconds,
                                provider_ids=spare_providers, dispatch_after=provider_deadlines_at)
                            wait_seconds = min(wait_seconds, max(.001, delay))
                    except (OSError, sqlite3.Error):
                        # A transient local read failure must not cancel an
                        # already-running request. Retry at the ordinary
                        # bounded completion wait; dispatch still rechecks
                        # every lease, scope and publisher limit.
                        queue.metrics.count('local_repairs', 'dispatch_wake_scan_deferred')
            if (continuous_budgets and provider_ids is None and not draining and not low_disk
                    and local_retry_ready(queue) and getattr(queue, '_url_metadata_continuation_pending', False)):
                wait_seconds = 0
            if continuous_budgets and provider_ids is None and not draining and not low_disk:
                wait_seconds = local_retry_delay(queue, wait_seconds)
                wait_seconds = mailbox_scan_retry_delay(queue, wait_seconds)
            if (getattr(queue, 'reviewed_repair_continuation_pending', False)
                    and mailbox_scan_retry_ready(queue, 'repair') and not draining and not low_disk):
                wait_seconds = min(wait_seconds, max(0, next_repair_continuation - time.monotonic()))
            if not cmoney_namespace_done and not draining and not low_disk:
                wait_seconds = min(wait_seconds, max(0, next_cmoney_namespace_slice - time.monotonic()))
            if cnyes_history_pending and not draining and not low_disk:
                wait_seconds = min(wait_seconds, max(0, next_cnyes_history_backfill - time.monotonic()))
            if cnyes_work_only and not draining and not low_disk:
                wait_seconds = min(wait_seconds, max(0, next_cnyes_work_backfill - time.monotonic()))
            if failure_replay_continuation and not draining and not low_disk:
                wait_seconds = min(wait_seconds, max(0, next_failure_replay - time.monotonic()))
            if (continuous_budgets and 'cnyes' in active
                    and getattr(queue, '_cnyes_original_priority_needs_wake', False)
                    and not draining and not low_disk):
                wait_seconds = 0
            if cnyes_checkpoint_ready and not draining and not low_disk:
                wait_seconds = min(wait_seconds, max(0, next_cnyes_checkpoint_recovery - time.monotonic()))
            if metadata_checkpoint_ready and not draining and not low_disk:
                wait_seconds = min(wait_seconds, max(0, next_metadata_checkpoint_recovery - time.monotonic()))
            if bulk_checkpoint_ready and not draining and not low_disk:
                wait_seconds = min(wait_seconds, max(0, next_bulk_checkpoint_recovery - time.monotonic()))
            if metadata_legacy_ready and not draining and not low_disk:
                wait_seconds = min(wait_seconds, max(0, next_metadata_legacy_backfill - time.monotonic()))
            elif metadata_legacy_retry_epoch is not None and not draining and not low_disk:
                wait_seconds = min(wait_seconds, max(.05, metadata_legacy_retry_epoch - time.time()))
            if (continuous_budgets and refresh_metadata and futures and not draining and not low_disk
                    and not pending_records and getattr(queue, 'metadata_inventory_pending', False)
                    and getattr(queue, 'metadata_inventory_made_progress', False)):
                # Drain an already-finished future before the next batch;
                # otherwise immediately offer a slice alongside the request.
                wait_seconds = 0
            done = () if local_only else wait(
                futures, timeout=wait_seconds, return_when=FIRST_COMPLETED)[0]
            for future in done:
                kind, row = futures.pop(future)
                active[row["provider"]] -= 1
                if kind == 'candidate_metadata':
                    from downloader.news_fulltext_candidate_diagnostics import finish_candidate, pump_candidates
                    try:
                        candidate_result = future.result()
                    except Exception as error:
                        finish_candidate(queue, row, error=error)
                    else:
                        finish_candidate(queue, row, result=candidate_result)
                    # Finishing the reserved observation is mandatory even
                    # while draining. Admitting other inbox items can wait
                    # for the next cycle after this one's stop/deadline.
                    if not cycle_stopping():
                        pump_candidates(queue, stop=http.stop)
                    continue
                if kind == "discovery":
                    discovery_inflight.remove(row["url"])
                    from downloader.news_fulltext_cnyes_index import is_index_task
                    if is_index_task(row):
                        from downloader.news_fulltext_bulk import cnyes_scope
                        scope = cnyes_scope(row['url'])
                        row['_cnyes_revision_scope_idle'] = not any(
                            url.split('?', 1)[0] == row['url'].split('?', 1)[0]
                            and cnyes_scope(url) == scope for url in discovery_inflight)
                    discovery_result = None
                    try:
                        discovery_result = future.result()
                        accepted += accept_discovery(queue, row, *discovery_result)
                    except NewsError as error:
                        fail_discovery(queue, row, error, expected_head_fields=_discovery_head_fields(row))
                    except (OSError, sqlite3.Error) as error:
                        # A committed CNYES batch can recover its page locally.
                        # Do not turn a local commit failure into a new source
                        # observation that would invalidate its exact head.
                        from downloader.news_fulltext_cnyes_checkpoint import defer_live_io_failure
                        from downloader.news_fulltext_metadata_checkpoint import defer_live_failure
                        from downloader.news_fulltext_bulk_checkpoint import defer_live_failure as defer_bulk_failure
                        deferred = (discovery_result is not None and (
                            defer_live_failure(queue, row, discovery_result[0], error)
                            or defer_bulk_failure(queue, row, discovery_result[0], error)
                            or (isinstance(error, OSError) or row['kind'] == 'index')
                               and defer_live_io_failure(queue, row, discovery_result[0], error)))
                        if deferred and row['kind'] == 'metadata_feed':
                            # The intent was created after this round's local
                            # snapshot. Wake the owner now, not at the next
                            # idle cycle or another publisher request.
                            metadata_checkpoint_ready = True
                            metadata_checkpoint_retry_epoch = 0
                            next_metadata_checkpoint_recovery = time.monotonic()
                        elif deferred and row['kind'] in {'rss_fulltext', 'wordpress_posts'}:
                            bulk_checkpoint_ready = True
                            bulk_checkpoint_retry_epoch = 0
                            next_bulk_checkpoint_recovery = time.monotonic()
                        elif deferred and row['provider'] == 'cnyes' and row['kind'] == 'index':
                            # Preserve the established body-page lifecycle; the
                            # new metadata index intent needs its own immediate
                            # wake because it can otherwise block this exact URL.
                            cnyes_checkpoint_ready = True
                            cnyes_checkpoint_retry_epoch = 0
                            next_cnyes_checkpoint_recovery = time.monotonic()
                        if not deferred:
                            fields = _discovery_head_fields(row)
                            fail_discovery(queue, row, NewsError("worker_" + type(error).__name__, retry=True, delay=60),
                                           expected_head_fields=fields)
                    except Exception as error:
                        fail_discovery(queue, row, NewsError("worker_" + type(error).__name__, retry=True, delay=60),
                                       expected_head_fields=_discovery_head_fields(row))
                    continue
                try:
                    record = future.result()
                    if record is None:
                        queue.unchanged(row)
                    else:
                        pending_records.append(record)
                        pending_bytes += record["body_bytes"]
                        pending_raw_bytes += len(record["raw_response_body"] or b"")
                        pending_raw_bytes += len(record.get("url_resolution_response_body") or b"")
                except NewsError as error:
                    queue.failed(row, error, config["max_attempts"])
                except Exception as error:
                    # Unexpected failures keep their type and finite retry, not
                    # a forged empty success. KeyboardInterrupt/SystemExit propagate.
                    queue.failed(row, NewsError("worker_" + type(error).__name__, retry=True, delay=60), config["max_attempts"])
            now = time.monotonic()
            if pending_records and (local_only or len(pending_records) >= config["batch_articles"]
                                    or pending_bytes >= config["batch_text_bytes"]
                                    or pending_raw_bytes >= config.get("batch_raw_response_bytes", 16 << 20)
                                    or now - last_flush >= config.get("batch_flush_seconds", 30)):
                # A cycle deadline closes admission, but an already received
                # batch must still drain through transient local lock retries.
                # Only an explicit owner stop interrupts that durable handoff.
                queue.accept_live_article_batch(pending_records, stop=http.stop, should_stop=http.stop.is_set)
                accepted += len(pending_records)
                pending_records, pending_bytes, pending_raw_bytes, last_flush = [], 0, 0, now
            if now - last_status >= 15:
                with queue.metrics.measure("status_write"):
                    queue.status(requests_by_provider=http.requests_by_provider, response_bytes=http.response_bytes,
                                 run_seconds=now - started, accepted_this_run=accepted, activity="draining" if draining else "running",
                                 provider_cooldowns=http.cooldown_snapshot())
                last_status = now
            if local_only:
                # A local metadata backlog is work even with every publisher
                # cooling down. Keep its bounded slices moving, flush bodies
                # above, and sleep interruptibly instead of an empty CPU loop.
                delay = config['original_url_resolution_interval_seconds']
                if repair_only:
                    delay = min(delay, max(0, next_repair_continuation - time.monotonic()))
                if url_inbox_only:
                    delay = 0
                if namespace_only:
                    delay = min(delay, max(0, next_cmoney_namespace_slice - time.monotonic()))
                if history_only:
                    if getattr(queue, '_cnyes_original_priority_needs_wake', False):
                        # A page journalled after the last priority slice is
                        # runnable now even when the history scan chose a
                        # future maintenance time and no HTTP future exists.
                        delay = 0
                    else:
                        delay = min(delay, max(0, next_cnyes_history_backfill - time.monotonic()))
                if cnyes_work_only:
                    delay = min(delay, max(0, next_cnyes_work_backfill - time.monotonic()))
                if checkpoint_only:
                    delay = min(delay, max(0, next_cnyes_checkpoint_recovery - time.monotonic()))
                if metadata_checkpoint_only:
                    delay = min(delay, max(0, next_metadata_checkpoint_recovery - time.monotonic()))
                if bulk_checkpoint_only:
                    delay = min(delay, max(0, next_bulk_checkpoint_recovery - time.monotonic()))
                if metadata_legacy_only:
                    delay = min(delay, max(0, next_metadata_legacy_backfill - time.monotonic()))
                if failure_replay_continuation:
                    delay = min(delay, max(0, next_failure_replay - time.monotonic()))
                http.stop.wait(delay)
    if pending_records:
        queue.accept_live_article_batch(pending_records, stop=http.stop, should_stop=http.stop.is_set)
        accepted += len(pending_records)
    activity = "waiting_disk" if low_disk else "stopped" if http.stop.is_set() else "cycle_complete"
    cycle = {"wall_seconds": time.monotonic() - started, "article_attempts_started": claimed,
             "durably_accepted_versions": accepted, "discovery_attempts_started": discovery_claimed,
             "http_requests": {p: count - requests_at_start.get(p, 0) for p, count in http.requests_by_provider.items()},
             "final_flush_records": len(pending_records), "max_run_seconds": max_run_seconds,
             "bulk_backlog_rollover": bulk_rollover,
             "continuous_budgets": continuous_budgets, "budget_rollovers": budget_rollovers}
    if defer_final_status:
        # The resident owner decides whether the next state is waiting or ready
        # before projecting the same multi-megabyte status. Accepted records and
        # source deadlines have already been committed above.
        return {"activity": activity, "cycle": cycle,
                "cnyes_history_retry_epoch": cnyes_history_retry_epoch,
                "cnyes_checkpoint_retry_epoch": cnyes_checkpoint_retry_epoch,
                "metadata_checkpoint_retry_epoch": metadata_checkpoint_retry_epoch,
                "bulk_checkpoint_retry_epoch": bulk_checkpoint_retry_epoch,
                "metadata_legacy_retry_epoch": metadata_legacy_retry_epoch}
    return queue.status(requests_by_provider=http.requests_by_provider, response_bytes=http.response_bytes,
                        run_seconds=cycle['wall_seconds'], accepted_this_run=accepted, activity=activity,
                        provider_cooldowns=http.cooldown_snapshot(), cycle=cycle)


DISK_WAIT_HEARTBEAT_SECONDS = 60


@contextmanager
def _coalesce_inventory_heartbeats(queue):
    """Let the blocked owner's heartbeat publish local inventory progress.

    The inventory callback stays synchronous and keeps committing its normal
    slices. Restore any outer state even when the callback raises or stops.
    """
    missing = object()
    previous = getattr(queue, '_coalesce_inventory_heartbeats', missing)
    queue._coalesce_inventory_heartbeats = True
    try:
        yield
    finally:
        if previous is missing:
            del queue._coalesce_inventory_heartbeats
        else:
            queue._coalesce_inventory_heartbeats = previous


def _wait_for_disk_capacity(queue, http, mailbox, delay, refresh_metadata, result=None):
    """Keep local inventory moving without rebuilding a blocked HTTP cycle.

    Disk admission keeps its configured wake; pending metadata keeps its
    registered slice interval. A blocked owner uses a one-minute heartbeat
    instead of rewriting the complete status every fifteen seconds.
    The capacity wake itself is unchanged, and ordinary idle/running updates
    keep their existing cadence.
    Neither local work nor a durable mailbox can bypass disk admission.
    """
    from downloader.news_fulltext_metadata_ingest import METADATA_SLICE_INTERVAL_SECONDS
    until = time.monotonic() + delay
    next_status = time.monotonic() + DISK_WAIT_HEARTBEAT_SECONDS
    next_metadata = time.monotonic() + METADATA_SLICE_INTERVAL_SECONDS
    with queue.metrics.measure('idle_wait'):
        while not http.stop.is_set() and time.monotonic() < until:
            sampler = getattr(queue, '_backing_disk_sampler', None)
            pending_backing = bool(sampler and getattr(queue, 'disk_admission', {}).get('physical_probe_inflight'))
            if pending_backing and sampler.wait(0):
                return  # Recheck this same owner as soon as the first sample finishes.
            now = time.monotonic()
            if (refresh_metadata and getattr(queue, 'metadata_inventory_pending', False)
                    and now >= next_metadata):
                with _coalesce_inventory_heartbeats(queue):
                    queue.metadata_inventory_made_progress = False
                    refresh_metadata()
                next_metadata = time.monotonic() + METADATA_SLICE_INTERVAL_SECONDS
                if http.stop.is_set():
                    break
                now = time.monotonic()
            if now >= next_status:
                # Preserve the last completed cycle across heartbeats, just as
                # the ordinary idle path does. These are cycle deltas, not a
                # claim that source requests continued while disk was blocked.
                _publish_service_cycle(queue, http, result if result is not None else {}, 'waiting_disk')
                next_status = time.monotonic() + DISK_WAIT_HEARTBEAT_SECONDS
            deadline = min(until, next_status)
            if refresh_metadata and getattr(queue, 'metadata_inventory_pending', False):
                deadline = min(deadline, next_metadata)
            wait_seconds = max(0, deadline - time.monotonic())
            if pending_backing:
                sampler.wait(min(1, wait_seconds))  # No source or mailbox work while unadmitted.
            else:
                mailbox.wait(wait_seconds, enabled=False)


def _publish_service_cycle(queue, http, result, activity, *, include_cycle_rate=False):
    """Project and publish the cycle once in the owner's actual next state."""
    cycle = result.get('cycle')
    options = {}
    if cycle is not None:
        options['cycle'] = cycle
        if include_cycle_rate:
            options['run_seconds'] = cycle['wall_seconds']
            options['accepted_this_run'] = cycle['durably_accepted_versions']
    return queue.status(requests_by_provider=http.requests_by_provider, response_bytes=http.response_bytes,
                        activity=activity, provider_cooldowns=http.cooldown_snapshot(), **options)


@contextmanager
def _service_cycle_snapshots(queue):
    """Scope inventory's last completed cycle to this resident owner only."""
    queue._service_last_completed_cycle = None
    try:
        yield
    finally:
        del queue._service_last_completed_cycle


def run_service(queue: NewsQueue, http: NewsHTTP, config: dict, *, max_articles: int, max_discovery_requests: int,
                refresh_metadata=None, duration_seconds: float | None = None):
    """Keep one owner, bounded executor and warm HTTP sessions across cycles.

    Ready history continues immediately. If every available publisher is cooling
    down, sleep until the nearest persisted deadline; no fast empty timer loop.
    """
    started = time.monotonic()
    http.metrics = queue.metrics
    # Metadata inventory publishes status before the first cycle. Give it the
    # same loaded source deadlines/capacity as every later progress heartbeat.
    queue.http_capacity_snapshot = http.capacity_snapshot
    queue.provider_cooldown_snapshot = http.cooldown_snapshot
    queue.http_traffic_snapshot = http.traffic_snapshot
    from downloader.news_fulltext_wake import OwnerMailboxWake
    latest_cycle = None
    with _service_cycle_snapshots(queue), \
            ThreadPoolExecutor(max_workers=config["workers"], thread_name_prefix="news-fulltext") as executor, \
            OwnerMailboxWake(queue, http.stop) as mailbox:
        while not http.stop.is_set():
            remaining = duration_seconds - (time.monotonic() - started) if duration_seconds is not None else None
            if remaining is not None and remaining <= 0:
                break
            if refresh_metadata:
                queue.metadata_inventory_made_progress = False
                refresh_metadata()
            if http.stop.is_set():
                break
            if duration_seconds is not None:
                remaining = duration_seconds - (time.monotonic() - started)
                if remaining <= 0:
                    break
            result = run_cycle(queue, http, config, max_articles=max_articles, max_discovery_requests=max_discovery_requests,
                               executor=executor, max_run_seconds=remaining,
                               continuous_budgets=True, refresh_metadata=refresh_metadata, defer_final_status=True,
                               idle_wake=mailbox)
            latest_cycle = result.get('cycle')
            if latest_cycle is not None:
                queue._service_last_completed_cycle = {'cycle': deepcopy(latest_cycle), 'completed_at_utc': utc_now()}
            if http.stop.is_set():
                break
            try:
                if result['activity'] == 'waiting_disk':
                    delay = config.get('service_idle_max_seconds', 60)
                else:
                    # Use the same dispatch-only readiness for the idle wake;
                    # the public status retains the actual source/long pauses.
                    provider_cooldowns = http.scheduling_cooldown_snapshot()
                    request_scopes = http.request_scope_cooldown_snapshot()
                    delay = queue.next_wake_delay(provider_cooldowns, config.get('service_idle_max_seconds', 60),
                                                  request_scope_cooldowns=request_scopes,
                                                  include_articles=max_articles > 0,
                                                  include_discovery=max_discovery_requests > 0)
                    # Suppressed ordinary work must not cause an idle spin.
                    if max_articles == 0 and max_discovery_requests == 0:
                        delay = max(1, delay)
                    history_retry = result.get('cnyes_history_retry_epoch')
                    if history_retry is not None:
                        from downloader.news_fulltext_cnyes_history import maintenance_delay as history_delay
                        delay = min(delay, history_delay({'exhausted': True,
                                                        'next_local_retry_epoch': history_retry}))
                    metadata_retry = result.get('metadata_checkpoint_retry_epoch')
                    if metadata_retry is not None:
                        delay = min(delay, max(.05, metadata_retry - time.time()))
                    bulk_retry = result.get('bulk_checkpoint_retry_epoch')
                    if bulk_retry is not None:
                        delay = min(delay, max(.05, bulk_retry - time.time()))
                    metadata_legacy_retry = result.get('metadata_legacy_retry_epoch')
                    if metadata_legacy_retry is not None:
                        delay = min(delay, max(.05, metadata_legacy_retry - time.time()))
                    cnyes_checkpoint_retry = result.get('cnyes_checkpoint_retry_epoch')
                    if cnyes_checkpoint_retry is not None:
                        delay = min(delay, max(.05, cnyes_checkpoint_retry - time.time()))
                    if getattr(queue, 'native_candidate_ready_providers', None):
                        from downloader.news_fulltext_candidate_diagnostics import candidate_next_wake_delay
                        delay = min(delay, candidate_next_wake_delay(queue, provider_cooldowns, request_scopes, delay))
                    from downloader.news_fulltext_candidate_diagnostics import candidate_local_retry_delay
                    delay = candidate_local_retry_delay(queue, delay)
                    from downloader.news_fulltext_url_metadata import local_retry_delay
                    delay = local_retry_delay(queue, delay)
                    if config.get('retained_link_handoff'):
                        from downloader.news_fulltext_retained_handoff import next_wake_delay as retained_handoff_next_wake_delay
                        delay = retained_handoff_next_wake_delay(queue, delay)
                    from downloader.news_fulltext_wake import mailbox_scan_retry_delay
                    delay = mailbox_scan_retry_delay(queue, delay)
                    if config.get('retained_cnyes_failure_replay_enabled', False):
                        from downloader.news_fulltext_replay import retained_failure_next_wake_delay
                        delay = min(delay, retained_failure_next_wake_delay(queue, delay))
                    if config.get('cached_cnyes_replay_enabled', False):
                        from downloader.news_fulltext_replay import cached_bulk_local_retry_next_wake_delay
                        delay = min(delay, cached_bulk_local_retry_next_wake_delay(queue, delay))
                    if config.get('cached_cnyes_metadata_replay_enabled', False):
                        from downloader.news_fulltext_metadata_replay import retained_metadata_next_wake_delay
                        delay = min(delay, retained_metadata_next_wake_delay(queue, delay))
                if result['activity'] != 'waiting_disk' and getattr(queue, 'metadata_inventory_pending', False):
                    from downloader.news_fulltext_metadata_ingest import METADATA_SLICE_INTERVAL_SECONDS
                    # A cycle may have yielded solely to durably hand off an
                    # HTTP response. Resume its productive inventory now;
                    # an unsuccessful slice retains the ordinary retry delay.
                    delay = min(delay, 0 if getattr(queue, 'metadata_inventory_made_progress', False)
                                else METADATA_SLICE_INTERVAL_SECONDS)
                if result['activity'] != 'waiting_disk' and getattr(queue, 'reviewed_repair_pending', False):
                    from downloader.news_fulltext_repairs import PUMP_INTERVAL_SECONDS
                    delay = min(delay, PUMP_INTERVAL_SECONDS)
                if remaining is not None:
                    delay = min(delay, max(0, duration_seconds - (time.monotonic() - started)))
            except Exception:
                # The durable cycle still gets its final view if scheduling
                # fails before the ordinary waiting transition is published.
                _publish_service_cycle(queue, http, result, result['activity'], include_cycle_rate=True)
                raise
            if delay and result['activity'] == 'waiting_disk':
                _publish_service_cycle(queue, http, result, 'waiting_disk', include_cycle_rate=True)
                _wait_for_disk_capacity(queue, http, mailbox, delay, refresh_metadata, result)
                continue
            if delay:
                idle_until = time.monotonic() + delay
                published = False
                next_idle_heartbeat = 0.0
                next_idle_rollup = 0.0
                with queue.metrics.measure("idle_wait"):
                    while not http.stop.is_set() and idle_until > time.monotonic():
                        try:
                            pending_mail = result['activity'] != 'waiting_disk' and mailbox.has_pending()
                        except Exception:
                            if not published:
                                _publish_service_cycle(queue, http, result, result['activity'], include_cycle_rate=True)
                            raise
                        if pending_mail:
                            break
                        now = time.monotonic()
                        heartbeat_due = now >= next_idle_heartbeat
                        if heartbeat_due:
                            # Refresh the physical capacity at the ordinary
                            # heartbeat, independently of cheap local slices.
                            try:
                                low_disk = refresh_disk_admission(queue, config)
                            except Exception:
                                if not published:
                                    _publish_service_cycle(queue, http, result, result['activity'], include_cycle_rate=True)
                                raise
                            next_idle_heartbeat = time.monotonic() + 15
                            if low_disk:
                                _publish_service_cycle(queue, http, result, 'waiting_disk', include_cycle_rate=True)
                                published = True
                                break
                        if (getattr(queue, '_discovery_error_rollup_pending', False)
                                and now >= next_idle_rollup):
                            queue.advance_discovery_error_rollup()
                            next_idle_rollup = time.monotonic() + 1
                        if heartbeat_due:
                            _publish_service_cycle(queue, http, result, 'waiting_next_due')
                            published = True
                        next_wake = min(idle_until, next_idle_heartbeat)
                        if getattr(queue, '_discovery_error_rollup_pending', False):
                            next_wake = min(next_wake, next_idle_rollup)
                        if mailbox.wait(max(0, next_wake - time.monotonic()),
                                enabled=result['activity'] != 'waiting_disk'):
                            break
                if not published and not http.stop.is_set():
                    # Includes a mailbox that was already ready before the
                    # first idle heartbeat and an expired service deadline.
                    _publish_service_cycle(queue, http, result, result['activity'], include_cycle_rate=True)
            else:
                _publish_service_cycle(queue, http, result, result['activity'], include_cycle_rate=True)
    return queue.status(requests_by_provider=http.requests_by_provider, response_bytes=http.response_bytes,
                        activity="stopped" if http.stop.is_set() else "service_duration_complete",
                        provider_cooldowns=http.cooldown_snapshot(), cycle=latest_cycle)

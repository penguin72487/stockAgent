"""Full-text enrichment downstream of the existing metadata owners.

No FinLab/FinMind API calls, browser automation, paywall bypass or public body
gateway. Different publishers can run concurrently under independent budgets.
"""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from collections import deque
from contextlib import nullcontext
import hashlib
import json
import math
from pathlib import Path
import re
import shutil
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
                                             cnyes_bulk_fingerprint, extract_article, normalize_url)
from downloader.news_fulltext_feed import XML_BODY_METADATA_FALLBACK, feed_page, feed_records, feed_url
from downloader.news_fulltext_queue import NewsQueue, utc_now


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
    if type(config.get('cached_cnyes_replay_enabled', False)) is not bool:
        raise ValueError('cached_cnyes_replay_enabled must be a boolean')
    if type(config.get('cached_cnyes_metadata_replay_enabled', False)) is not bool:
        raise ValueError('cached_cnyes_metadata_replay_enabled must be a boolean')
    if type(config.get("exclusive_http_owner", False)) is not bool:
        raise ValueError("exclusive_http_owner must be a boolean")
    from downloader.news_fulltext_capacity import capacity_config
    capacity_config(config, registry)
    from downloader.news_fulltext_failure_focus import validate_focus
    config['failure_diagnostic_focus'] = validate_focus(config.get('failure_diagnostic_focus'), registry)
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
    return {
        **article, **capture_usage(source), "article_id": row["id"].hex(), "requested_url": row["url"], "final_url": response.url,
        "provider": source.provider, "metadata_date_raw": row.get("metadata_date_raw"),
        "captured_at_utc": captured_at_utc, "body_bytes": len(article["body"].encode()),
        "response_sha256": hashlib.sha256(response.body).hexdigest(), "http_response_bytes": len(response.body),
        "raw_response_body": response.body,
        "source_response_article_id": row["id"].hex(), "source_request_url": response.url,
        "data_fingerprint": source.data_fingerprint, "parser_backend": source.html_parser,
        "url_resolution_basis": resolution_basis,
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
    if row.get("body_sha256"):
        if row.get("etag"):
            headers["If-None-Match"] = row["etag"]
        if row.get("last_modified"):
            headers["If-Modified-Since"] = row["last_modified"]
    started = time.monotonic()
    response = http.get(request_url, headers=headers)
    if response.status == 304:
        return None
    _require_article_response(response, source)
    if source.embedded_article_layout == "macau_visible_news_v1" and response.url != request_url:
        # The reviewed portal has no verified article alias contract. A
        # different current release is not the originally requested ID.
        raise NewsError("article_identity_not_verified")
    resolution, resolution_response = None, None
    if source.embedded_article_layout == "ftv_visible_article_v1" and urlsplit(response.url).hostname != "www.ftvnews.com.tw":
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
            if error.code == 'body_not_found':
                from downloader.news_fulltext_diagnostics import FailureResponse
                # The worker transfers the already-fetched response only.
                # Bounded storage and SQLite budget belong to the queue owner.
                error.failure_response = FailureResponse(response, source.provider, source.data_fingerprint,
                                                         row['url'], received_at_utc)
            raise
    return article_response_record(source, row, response, article, captured_at_utc=utc_now(),
        http_duration_seconds=received - started, parse_duration_seconds=time.monotonic() - received,
        duration_seconds=time.monotonic() - started, resolution_basis=resolution_basis,
        resolution=resolution, resolution_response=resolution_response)


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
        if title != "sitemap" and source.article_path_pattern and not re.search(source.article_path_pattern, urlsplit(url).path):
            continue
        accepted.append((url, title, date))
    return accepted


def seed_discovery(queue: NewsQueue):
    queue.sync_finite_cohorts()
    with queue.db:
        for source in queue.registry.sources.values():
            if source.metadata_feeds:
                queue.db.executemany("INSERT INTO discovery(url,kind,provider,priority) VALUES (?,'metadata_feed',?,0) "
                    "ON CONFLICT(url) DO UPDATE SET kind='metadata_feed',priority=0,"
                    "due=CASE WHEN error_code IS NULL THEN 0 ELSE due END "
                    "WHERE kind!='metadata_feed' OR (parser_fingerprint IS NOT NULL AND parser_fingerprint!=?)",
                    ((url, source.provider, source.metadata_feed_fingerprint) for url in source.metadata_feeds))
            if source.allowed and source.acquisition_mode != "finite":
                queue.db.executemany("INSERT OR IGNORE INTO discovery(url,kind,provider) VALUES (?,?,?)",
                                     ((url, kind, source.provider) for kind, urls in (("feed", source.feeds), ("sitemap", source.sitemaps))
                                      for url in urls if url not in source.fulltext_feeds))
                for url in source.fulltext_feeds:
                    queue.db.execute("INSERT INTO discovery(url,kind,provider,priority) VALUES (?,'rss_fulltext',?,0) "
                                     "ON CONFLICT(url) DO UPDATE SET kind='rss_fulltext',priority=0,"
                                     "due=CASE WHEN error_code IS NULL THEN 0 ELSE due END "
                                     "WHERE kind!='rss_fulltext' OR (parser_fingerprint IS NOT NULL AND parser_fingerprint!=?)",
                                     (url, source.provider, discovery_fingerprint(source)))
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


def discovery_fingerprint(source):
    rss_contract = ':rss-controls-metadata-only-v1' if source.fulltext_feeds else ''
    envelope_contract = (':mixed-bulk-continuation-v' + str(BULK_ENVELOPE_CONTINUATION_CONTRACT_VERSION)
                         if source.fulltext_feeds or source.wordpress_apis or source.cnyes_apis else '')
    fingerprint = (cnyes_bulk_fingerprint(source) if source.cnyes_apis
                   else source.fulltext_feed_data_fingerprint or source.data_fingerprint)
    return hashlib.sha256((fingerprint + ":news-discovery-parser-v2" + rss_contract + envelope_contract).encode()).hexdigest()


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
                   source.cnyes_index_data_fingerprint if index_task else discovery_fingerprint(source))
    compatible = task.get("parser_fingerprint") == fingerprint
    if index_task:
        from downloader.news_fulltext_cnyes_index import compatible_fingerprints
        compatible = task.get('parser_fingerprint') in compatible_fingerprints(source)
    if task["etag"] and compatible:
        headers["If-None-Match"] = task["etag"]
    if task["last_modified"] and compatible:
        headers["If-Modified-Since"] = task["last_modified"]
    response = http.get(task["url"], headers=headers, **({'metadata_feed': task['url']} if metadata else {}))
    if response.status not in {200, 304}:
        raise NewsError("discovery_http_" + str(response.status))
    if response.status == 304 and (not compatible or not headers):
        raise NewsError("unverifiable_discovery_not_modified", retry=True, delay=60)
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
        with http.metrics.measure("bulk_extraction", source.provider):
            records, errors = cnyes_records(response, http, source) if response.status == 200 else ([], [])
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
    bulk = task["kind"] in {"wordpress_posts", "cnyes_news", "rss_fulltext"}
    cnyes_page_info = None
    rss_page_info = None
    # A 304 confirms unchanged mixed-page metadata, not permission to accept
    # its paid bodies. Preserve the visible gap and its shorter recheck timer.
    restricted_page = response.status == 304 and task.get("error_code") == "bulk_response_contains_restricted_text"
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
        if errors:
            with queue.db:
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
            rss_metadata_only = bool(rss_page_info and errors and all(e['error'] == XML_BODY_METADATA_FALLBACK for e in errors))
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
            metadata_only_page = page_metric in {'rss_xml_controls_metadata_only_pages',
                                                'bulk_metadata_only_html_fallback_pages'}
            if page_metric:
                queue.metrics.count(source.provider, page_metric)
            if not restricted_page and not records and not metadata_only_page and (total or rss_page_info and rss_page_info["rows"]):
                # A systematic parser/byline failure must not consume every
                # historical page. Keep eligible HTML fallback links first;
                # this circuit affects only the bulk adapter, not article work.
                with queue.db:
                    queue.add_articles([{"url": url, "title": title, "date": date} for url, title, date in links])
                raise NewsError("bulk_all_items_unusable", retry=True, delay=3600)
    # Queue article identities first, but do not mark this source page complete
    # until the full bodies and their acceptance receipts are durably committed.
    with queue.metrics.measure("discovery_registration"), queue.db:
        queue.add_articles([{"url": url, "title": title, "date": date} for url, title, date in links if title not in {"sitemap", "publisher_index"}])
        queue.db.executemany("INSERT OR IGNORE INTO discovery(url,kind,provider) VALUES (?,'sitemap',?)",
                             ((url, source.provider) for url, title, _ in links if title == "sitemap"))
        queue.db.executemany("INSERT OR IGNORE INTO discovery(url,kind,provider) VALUES (?,'index',?)",
                             ((url, source.provider) for url, title, _ in links if title == "publisher_index"))
    accepted = []
    for record in records:
        if task["kind"] == "cnyes_news":
            for host in ("news.cnyes.com", "m.cnyes.com"):
                alias_url = record["requested_url"].replace("news.cnyes.com/news/id/", host + "/news/print/", 1)
                alias = queue.db.execute("SELECT id,state,body_sha256,due FROM articles WHERE url=?", (alias_url,)).fetchone()
                if (alias and not alias[2] and alias[1] in {"pending", "retry", "unsupported_article_path", "body_not_found", "article_attribution_not_verified"}
                        and alias[3] <= time.time()):
                    accepted.append({**record, "article_id": alias[0].hex(), "requested_url": alias_url,
                                     "url_resolution_basis": "verified_cnyes_print_permalink_alias_v1"})
        current = queue.db.execute("SELECT * FROM articles WHERE id=?", (bytes.fromhex(record["article_id"]),)).fetchone()
        if current and current["state"] == "inflight":
            continue  # Never overwrite/release another worker's article lease.
        if current and current["state"] in {"retry", "retry_cooldown"} and current["due"] > time.time():
            continue  # A bulk response is not permission to bypass a retry deadline.
        if current and current["body_sha256"] == record["body_sha256"]:
            if current["state"] != "complete":
                queue.unchanged(dict(current))
            continue
        if task["kind"] in {"cnyes_news", "rss_fulltext"} and current and current["body_sha256"] and current["state"] == "complete":
            # Existing verified HTML (with byline) is already current under the
            # configured recheck policy. Do not rewrite it merely because the
            # API uses different whitespace or omits author metadata.
            continue
        accepted.append(record)
    if accepted:
        accepted[0]["raw_response_body"] = response.body
        for record in accepted:
            record["source_response_article_id"] = accepted[0]["article_id"]
        queue.accept(accepted)
    history_artifact = None
    if cnyes_page_info and 'startAt=' in task['url']:
        from downloader.news_fulltext_cnyes_history import retain_response
        history_artifact = retain_response(queue, response, source, accepted=accepted, restricted=restricted_page)
    with queue.db:
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
            queue.db.execute("INSERT OR REPLACE INTO bulk_api_coverage VALUES (?,?,?,?,?)",
                             (base, source.provider, total, pages, utc_now()))
            if cnyes_page_info:
                queue.metrics.count(source.provider, "bulk_exact_duplicate_rows_observed", cnyes_page_info["duplicate_rows"])
                from urllib.parse import parse_qs
                query = parse_qs(urlsplit(task["url"]).query)
                partitions = queue.partition_cnyes_window(task["url"], pages, source)
                if partitions:
                    queue.metrics.count(source.provider, "historical_windows_partitioned")
                elif "startAt" in query and pages > 30:
                    queue.db.execute("INSERT OR REPLACE INTO bulk_window_gaps VALUES (?,?,?,?)",
                                     (base, source.provider, "cnyes_single_day_page_cap", utc_now()))
                # First-page success or queued children is not full list
                # coverage. Only the durable ID/date proof retires a gap.
                if "startAt" in query and not partitions and page_number < min(pages, 30):
                    next_url = cnyes_url(wordpress_base(task["url"]), page_number + 1,
                                         start=int(query["startAt"][0]), end=int(query["endAt"][0]))
                    queue.db.execute("INSERT OR IGNORE INTO discovery(url,kind,provider,priority) VALUES (?,'cnyes_news',?,?)",
                                     (next_url, source.provider, cnyes_history_priority(next_url)))
                if cnyes_page_info["total"]:
                    queue.db.execute("INSERT OR REPLACE INTO bulk_page_evidence VALUES (?,?,?,?,?)",
                                     (base, page_number, total, cnyes_page_info["ids_sha256"], utc_now()))
                if history_artifact is not None:
                    from downloader.news_fulltext_cnyes_history import record_page
                    record_page(queue, response, cnyes_page_info, posts, source, history_artifact)
                queue.metrics.count(source.provider, "bulk_pages_validated")
                queue.metrics.count(source.provider, "bulk_article_ids_validated", len(posts))
            else:
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
        if task["kind"] == "rss_fulltext" and page > 1:
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
                          discovery_fingerprint(source) if response.status == 200 else None,
                          utc_now(), "bulk_response_contains_restricted_text" if restricted_page else
                          XML_BODY_METADATA_FALLBACK if rss_metadata_only else None, task["url"]))
        if (cnyes_page_info and not restricted_page and not records and errors
                and all(e['error'] in {'bulk_body_not_provided', 'bulk_exact_duplicate_row'} for e in errors)):
            # A successful live parse already proved this response cannot add
            # API bodies under this contract. Do not immediately read/parse it
            # again from quarantine; pending original HTML work is unchanged.
            from downloader.news_fulltext_replay import checkpoint_no_body_cnyes_replay
            checkpoint_no_body_cnyes_replay(queue, source, hashlib.sha256(response.body).hexdigest())
    return len(accepted)


def fail_discovery(queue: NewsQueue, task: dict, error: NewsError):
    attempts = task["attempts"] + 1
    # Persistent refusals remain visible and do not hammer every cycle.
    delay = max(error.delay, min(3600, 30 * 2 ** min(attempts, 7))) if error.retry else 86400
    with queue.db:
        queue.db.execute("UPDATE discovery SET due=?,attempts=?,error_code=?,last_checked_at_utc=? WHERE url=?",
                         (time.time() + delay, attempts, error.code, utc_now(), task["url"]))
        if task['kind'] == 'metadata_feed':
            from downloader.news_fulltext_metadata_feeds import record_failure_observation
            record_failure_observation(queue, task, error)
        from downloader.news_fulltext_cnyes_index import ENDPOINT, is_index_task
        if is_index_task(task):
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
        elif error.code in {"bulk_all_items_unusable", "bulk_pagination_no_progress", "bulk_pagination_totals_invalid"}:
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


def run_cycle(queue: NewsQueue, http: NewsHTTP, config: dict, *, max_articles: int, max_discovery_requests: int = 64,
              executor=None, max_run_seconds: float | None = None, provider_ids: set[str] | None = None,
              continuous_budgets: bool = False, refresh_metadata=None):
    if provider_ids is not None and (not provider_ids or any(
            p not in queue.registry.sources or not queue.registry.sources[p].request_enabled for p in provider_ids)):
        raise ValueError("cycle provider selection must contain authorized, configured providers")
    started, accepted, claimed = time.monotonic(), 0, 0
    http.metrics = queue.metrics
    if continuous_budgets and config.get("adaptive_rate", {}).get("enabled") and http.capacity_controller is None:
        from downloader.news_fulltext_capacity import PublisherCapacity
        http.capacity_controller = PublisherCapacity(http, config)
    queue.http_capacity_snapshot = http.capacity_snapshot
    queue.provider_cooldown_snapshot = http.cooldown_snapshot
    requests_at_start = dict(http.requests_by_provider)
    queue.schedule_recent_rechecks(config["recent_article_recheck_seconds"], config["recent_article_window_days"])
    seed_discovery(queue)
    discovery_claimed, discovery_inflight = 0, set()
    article_budget_used, discovery_budget_used, budget_rollovers = 0, 0, 0
    last_maintenance = started
    from downloader.news_fulltext_metadata_ingest import METADATA_SLICE_INTERVAL_SECONDS
    last_metadata_inventory = started
    last_cached_replay = started - 10
    last_metadata_replay = started
    next_cnyes_work_backfill = started
    next_cnyes_history_backfill = started
    last_original_resolution = started - config['original_url_resolution_interval_seconds']
    pending_records, futures = [], {}
    pending_bytes, pending_raw_bytes, last_flush, last_status = 0, 0, time.monotonic(), 0
    active = {provider: 0 for provider, source in queue.registry.sources.items()
              if source.request_enabled and (provider_ids is None or provider in provider_ids)}
    provider_order = deque(active)
    low_disk, last_disk_check = False, 0
    pool = ThreadPoolExecutor(max_workers=config["workers"], thread_name_prefix="news-fulltext") if executor is None else nullcontext(executor)
    with pool as executor:
        while True:
            now = time.monotonic()
            if http.capacity_controller:
                http.capacity_controller.tick()
            # Work limits are diagnostic bounds, not source quotas. A resident
            # owner renews its scheduling quantum without waiting for unrelated
            # publishers' slow responses. One queue writer and HTTP pacing stay
            # authoritative; stop/duration still drain all leased outcomes.
            if continuous_budgets and (max_articles > 0 and article_budget_used >= max_articles
                                       or max_discovery_requests > 0 and discovery_budget_used >= max_discovery_requests):
                article_budget_used = discovery_budget_used = 0
                budget_rollovers += 1
            maintenance_due = now - last_maintenance >= config.get('service_cycle_seconds', 180)
            if (continuous_budgets and refresh_metadata and not http.stop.is_set()
                    and (max_run_seconds is None or now - started < max_run_seconds) and (
                    maintenance_due or getattr(queue, 'metadata_inventory_pending', False)
                    and now - last_metadata_inventory >= METADATA_SLICE_INTERVAL_SECONDS)):
                # One original 4k-row commit can overrun the soft local budget,
                # but never ingest the whole large file before HTTP dispatch.
                refresh_metadata()
                last_metadata_inventory = time.monotonic()
                now = last_metadata_inventory
            if continuous_budgets and maintenance_due and not http.stop.is_set():
                queue.schedule_recent_rechecks(config["recent_article_recheck_seconds"], config["recent_article_window_days"])
                seed_discovery(queue)
                last_maintenance = time.monotonic()
            bulk_rollover = False
            if not continuous_budgets and max_discovery_requests > 0 and discovery_budget_used >= max_discovery_requests:
                for provider in provider_order:
                    source = queue.registry.sources[provider]
                    if not (source.wordpress_apis or source.cnyes_apis or source.fulltext_feeds) or http.provider_not_before.get(provider, 0) > time.time():
                        continue
                    if not queue.bulk_allowed(source):
                        continue
                    dispatch_filter, parameters = queue.discovery_dispatch_filter(source)
                    if queue.db.execute("SELECT 1 FROM discovery WHERE provider=? AND superseded_by_windows_json IS NULL "
                                        "AND kind IN ('wordpress_posts','cnyes_news','rss_fulltext') AND due<=? AND " + dispatch_filter + " LIMIT 1",
                                        (provider, time.time(), *parameters)).fetchone():
                        bulk_rollover = True
                        break
            draining = (http.stop.is_set() or max_run_seconds is not None and now - started >= max_run_seconds
                        or not continuous_budgets and max_articles > 0 and article_budget_used >= max_articles or bulk_rollover)
            if now - last_disk_check >= 10:
                low_disk = shutil.disk_usage(queue.root).free < config["min_free_disk_bytes"]
                last_disk_check = now
            if (continuous_budgets and not draining and not low_disk
                    and 'cnyes' in active and now >= next_cnyes_work_backfill):
                from downloader.news_fulltext_cnyes_work import backfill as backfill_cnyes_work, maintenance_delay
                result = backfill_cnyes_work(queue, stop=http.stop)
                next_cnyes_work_backfill = time.monotonic() + maintenance_delay(result)
            if (continuous_budgets and not draining and not low_disk
                    and 'cnyes' in active and now >= next_cnyes_history_backfill):
                from downloader.news_fulltext_cnyes_history import backfill as backfill_cnyes_history
                # Bound writer time, not fast metadata rows to four per second.
                # This is local evidence maintenance; HTTP pacing and source
                # refusal deadlines remain governed by the existing owner.
                result = backfill_cnyes_history(queue, stop=http.stop, limit=128, max_seconds=.05)
                next_cnyes_history_backfill = time.monotonic() + (300 if result['exhausted'] else 2)
            if (continuous_budgets and not draining and not low_disk
                    and now - last_original_resolution >= config['original_url_resolution_interval_seconds']):
                from downloader.news_fulltext_urls import backfill_original_urls, sync_original_url_routes
                options = {'limit': config['original_url_resolution_batch'],
                           'max_seconds': config['original_url_resolution_slice_seconds'], 'stop': http.stop}
                sync_original_url_routes(queue, **options)
                backfill_original_urls(queue, **options)
                last_original_resolution = time.monotonic()
            if (continuous_budgets and not draining and not low_disk and config.get('cached_cnyes_replay_enabled', False)
                    and 'cnyes' in active and now - last_cached_replay >= 10):
                from downloader.news_fulltext_replay import replay_cached_cnyes
                recovered = replay_cached_cnyes(queue, stop=http.stop)
                accepted += recovered.get('accepted_heads', 0)
                last_cached_replay = time.monotonic()
            if (continuous_budgets and not draining and not low_disk
                    and config.get('cached_cnyes_metadata_replay_enabled', False)
                    and 'cnyes' in active and now - last_metadata_replay >= 10):
                from downloader.news_fulltext_metadata_replay import replay_retained_cnyes_metadata
                # Revisions are not new URL heads or HTTP attempts. The shared
                # metrics label retained work separately from online capacity.
                replay_retained_cnyes_metadata(queue, stop=http.stop)
                last_metadata_replay = time.monotonic()
            submitted_in_round = 0
            for provider in () if draining else provider_order:
                source = queue.registry.sources[provider]
                if low_disk or len(futures) >= config["workers"] or active[provider] >= http.parallelism[provider]:
                    continue
                if http.provider_not_before.get(provider, 0) > time.time():
                    continue
                excluded_origins = tuple(http.blocked_robots_origins(provider))
                task = None
                if max_discovery_requests > 0 and (continuous_budgets or discovery_budget_used < max_discovery_requests):
                    bulk_allowed = queue.bulk_allowed(source)
                    candidates = queue.discovery_candidates(source, config['workers'] + 1,
                        bulk_allowed=bulk_allowed, excluded_origins=excluded_origins)
                    task = next((dict(r) for r in candidates if r["url"] not in discovery_inflight), None)
                row = None
                excluded_hosts = http.blocked_gateway_hosts(provider)
                historical_bulk = bool(task and (
                    task["kind"] == "rss_fulltext" and feed_page(task["url"]) > 1
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
                if task:
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
            provider_order.rotate(-1)
            metadata_only = False
            if not futures:
                if continuous_budgets and not draining and not low_disk:
                    from downloader.news_fulltext_urls import original_url_progress
                    metadata_only = original_url_progress(queue)['remaining_urls'] > 0
                if not metadata_only:
                    break
            # Fill every independently reserved response slot before sleeping.
            # One submission per publisher per round preserves fairness; when
            # no new job was submitted, block normally instead of CPU polling.
            done = () if metadata_only else wait(
                futures, timeout=0 if submitted_in_round else 1, return_when=FIRST_COMPLETED)[0]
            for future in done:
                kind, row = futures.pop(future)
                active[row["provider"]] -= 1
                if kind == "discovery":
                    discovery_inflight.remove(row["url"])
                    try:
                        accepted += accept_discovery(queue, row, *future.result())
                    except NewsError as error:
                        fail_discovery(queue, row, error)
                    except Exception as error:
                        fail_discovery(queue, row, NewsError("worker_" + type(error).__name__, retry=True, delay=60))
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
            if pending_records and (metadata_only or len(pending_records) >= config["batch_articles"]
                                    or pending_bytes >= config["batch_text_bytes"]
                                    or pending_raw_bytes >= config.get("batch_raw_response_bytes", 16 << 20)
                                    or now - last_flush >= config.get("batch_flush_seconds", 30)):
                queue.accept(pending_records)
                accepted += len(pending_records)
                pending_records, pending_bytes, pending_raw_bytes, last_flush = [], 0, 0, now
            if now - last_status >= 15:
                with queue.metrics.measure("status_write"):
                    queue.status(requests_by_provider=http.requests_by_provider, response_bytes=http.response_bytes,
                                 run_seconds=now - started, accepted_this_run=accepted, activity="draining" if draining else "running",
                                 provider_cooldowns=http.provider_not_before)
                last_status = now
            if metadata_only:
                # A local metadata backlog is work even with every publisher
                # cooling down. Keep its bounded slices moving, flush bodies
                # above, and sleep interruptibly instead of an empty CPU loop.
                http.stop.wait(config['original_url_resolution_interval_seconds'])
    if pending_records:
        queue.accept(pending_records)
        accepted += len(pending_records)
    result = queue.status(requests_by_provider=http.requests_by_provider, response_bytes=http.response_bytes,
                          run_seconds=time.monotonic() - started, accepted_this_run=accepted,
                          activity="waiting_disk" if low_disk else "stopped" if http.stop.is_set() else "cycle_complete",
                          provider_cooldowns=http.provider_not_before)
    result["cycle"] = {"wall_seconds": time.monotonic() - started, "article_attempts_started": claimed,
                       "durably_accepted_versions": accepted, "discovery_attempts_started": discovery_claimed,
                       "http_requests": {p: count - requests_at_start.get(p, 0) for p, count in http.requests_by_provider.items()},
                       "final_flush_records": len(pending_records), "max_run_seconds": max_run_seconds,
                       "bulk_backlog_rollover": bulk_rollover,
                       "continuous_budgets": continuous_budgets, "budget_rollovers": budget_rollovers}
    result["http_counters_basis"] = "since current owner started; cycle contains separate deltas"
    atomic_write_json(queue.root / "status.json", result, durable=False, compact=True)
    return result


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
    with ThreadPoolExecutor(max_workers=config["workers"], thread_name_prefix="news-fulltext") as executor:
        while not http.stop.is_set():
            remaining = duration_seconds - (time.monotonic() - started) if duration_seconds is not None else None
            if remaining is not None and remaining <= 0:
                break
            if refresh_metadata:
                refresh_metadata()
            if http.stop.is_set():
                break
            if duration_seconds is not None:
                remaining = duration_seconds - (time.monotonic() - started)
                if remaining <= 0:
                    break
            result = run_cycle(queue, http, config, max_articles=max_articles, max_discovery_requests=max_discovery_requests,
                               executor=executor, max_run_seconds=remaining,
                               continuous_budgets=True, refresh_metadata=refresh_metadata)
            if http.stop.is_set():
                break
            delay = (config.get("service_idle_max_seconds", 60) if result["activity"] == "waiting_disk"
                     else queue.next_wake_delay(http.provider_not_before, config.get("service_idle_max_seconds", 60),
                                               request_scope_cooldowns=http.request_scope_cooldown_snapshot()))
            # Even diagnostic work limits of zero must not spin continuously.
            if max_articles == 0 and max_discovery_requests == 0:
                delay = max(1, delay)
            if getattr(queue, 'metadata_inventory_pending', False):
                from downloader.news_fulltext_metadata_ingest import METADATA_SLICE_INTERVAL_SECONDS
                delay = min(delay, METADATA_SLICE_INTERVAL_SECONDS)
            if remaining is not None:
                delay = min(delay, max(0, duration_seconds - (time.monotonic() - started)))
            if delay:
                idle_until = time.monotonic() + delay
                with queue.metrics.measure("idle_wait"):
                    while not http.stop.is_set() and idle_until > time.monotonic():
                        queue.status(requests_by_provider=http.requests_by_provider, response_bytes=http.response_bytes,
                                     activity="waiting_disk" if result["activity"] == "waiting_disk" else "waiting_next_due",
                                     provider_cooldowns=http.provider_not_before)
                        http.stop.wait(min(15, max(0, idle_until - time.monotonic())))
    return queue.status(requests_by_provider=http.requests_by_provider, response_bytes=http.response_bytes,
                        activity="stopped" if http.stop.is_set() else "service_duration_complete",
                        provider_cooldowns=http.provider_not_before)

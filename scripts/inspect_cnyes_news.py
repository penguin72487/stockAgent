"""Private, bounded diagnosis of the canonical Cnyes news transport.

Read-only inventory by default. Probes share the production limiter, robots and
cooldowns; they never create another frontier owner or invent API permissions.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import html
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import time
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_http import NewsHTTP, ACCESS_DIAGNOSTIC_HEADERS
from downloader.news_fulltext_policy import NewsError, _objects, _plain_text, _plain_text_node, extract_article, normalize_url
from downloader.news_fulltext_queue import utc_now


def headline_comparison_metadata(visible, current):
    """Explain a title mismatch without persisting either title or body.

    These are diagnostic comparisons, not new acceptance/retention rules.
    In particular, a whitespace/entity comparison never authorizes raw bytes
    when the existing exact public-scope proof fails.
    """
    if not isinstance(visible, str) or not isinstance(current, str):
        return {'string_pair': False}
    compact = lambda value: re.sub(r'\s+', '', value)
    first = next((i for i, pair in enumerate(zip(visible, current)) if pair[0] != pair[1]),
                 min(len(visible), len(current)) if visible != current else None)
    return {'string_pair': True,
            'visible_characters': len(visible), 'current_characters': len(current),
            'visible_sha256': hashlib.sha256(visible.encode()).hexdigest(),
            'current_sha256': hashlib.sha256(current.encode()).hexdigest(),
            'exact_match': visible == current,
            'whitespace_equivalent': compact(visible) == compact(current),
            'entity_decoded_equivalent': visible == html.unescape(current),
            'first_different_character_offset': first,
            'first_different_codepoints': {
                key: f'U+{ord(value[first]):04X}' if first is not None and first < len(value) else None
                for key, value in (('visible', visible), ('current', current))}}


def cnyes_html_structure(soup, source, *, page_url):
    """Bounded identity/clock/body comparison metadata, never body acceptance.

    Free current-ID declarations can permit private diagnosis even when the
    three body representations disagree. Missing/conflicting/hidden/paid
    evidence permits metadata only. No script execution, alternate requests,
    raw author/body text, credentials or inferred publication timestamps.
    """
    from downloader.news_fulltext_publishers import (
        _cnyes_completed_stream_wrapper, _cnyes_current_article_data,
        _cnyes_visible_after_completed_stream, _cnyes_visible_article_scope,
        cnyes_visible_headline_matches,
    )

    result = {"contract_version": 2, "raw_public_scope": False,
              "article_containers": len(soup.select('#article-container'))}
    parts = urlsplit(page_url)
    match = re.fullmatch(r'/news/id/([0-9]+)', parts.path)
    if (source.provider != 'cnyes' or not match or parts.scheme != 'https'
            or parts.netloc not in {'news.cnyes.com', 'm.cnyes.com'}):
        return result
    canonical = soup.select('link[rel="canonical"][href]')
    result['canonical_count'] = len(canonical)
    scope = _cnyes_visible_article_scope(soup)
    result['unambiguous_native_scope'] = scope is not None
    if len(canonical) != 1 or scope is None:
        return result
    try:
        declared_url = normalize_url(canonical[0]['href'])
        declared_parts = urlsplit(declared_url)
        if (declared_parts.scheme != 'https' or declared_parts.netloc not in {'news.cnyes.com', 'm.cnyes.com'}
                or declared_parts.path != parts.path or declared_parts.query or declared_parts.fragment):
            return result
        header, container = scope
        reveal = _cnyes_completed_stream_wrapper(soup, container.find_parent('article'))
        result['completed_native_stream'] = reveal is not None
        try:
            for node in (header, container):
                _cnyes_visible_after_completed_stream(node, reveal)
            result['native_scope_visible'] = True
        except NewsError:
            result['native_scope_visible'] = False
        data = _cnyes_current_article_data(soup, int(match[1]))
        result['current_data_present'] = data is not None
        result['current_author_unambiguous'] = _cnyes_current_article_data(
            soup, int(match[1]), verify_author=True) is not None
        if data is None:
            return result
        result['current_field_types'] = {key: type(data.get(key)).__name__
                                         for key in ('title', 'content', 'payment', 'publishAt', 'url', 'author')}
        result['current_payment_zero'] = type(data.get('payment')) is int and data['payment'] == 0
        result['current_url_matches_canonical'] = normalize_url(data.get('url', '')) == declared_url
        visible_title = header.get_text(' ', strip=True)
        result['visible_headline_matches_current'] = cnyes_visible_headline_matches(visible_title, data.get('title'))
        result['headline_comparison'] = headline_comparison_metadata(visible_title, data.get('title'))
        declarations = []
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                declarations.extend(node for node in _objects(json.loads(script.get_text()))
                                    if node.get('@type') == 'NewsArticle' and node.get('url') == declared_url)
            except (ValueError, TypeError, RecursionError):
                result['invalid_schema_declaration'] = True
                return result
        result['current_schema_count'] = len(declarations)
        if len(declarations) != 1:
            return result
        declared = declarations[0]
        clock = datetime.fromisoformat(declared.get('datePublished', '').replace('Z', '+00:00'))
        result['declared_clock_raw'] = declared.get('datePublished')
        result['declared_clock_matches_current'] = (clock.tzinfo is not None
            and type(data.get('publishAt')) is int and clock.timestamp() == data['publishAt'])
        result['declared_headline_matches_current'] = declared.get('headline') == data.get('title')
        result['schema_declares_free'] = str(declared.get('isAccessibleForFree', 'true')).lower() == 'true'
        result['raw_public_scope'] = all(result.get(key, False) for key in (
            'native_scope_visible', 'current_payment_zero', 'current_url_matches_canonical',
            'visible_headline_matches_current', 'declared_clock_matches_current',
            'declared_headline_matches_current', 'schema_declares_free')) and bool(data.get('title'))
        if not result['raw_public_scope']:
            return result
        texts = {'visible': _plain_text_node(container,
            (*source.exclude_selectors, '[id^="div-gpt-ad-cnyes_news_article_"]'), parser=source.html_parser)}
        for label, value in (('current', data.get('content')), ('schema', declared.get('articleBody'))):
            if isinstance(value, str):
                texts[label] = _plain_text(html.unescape(value), parser=source.html_parser)
        result['body_representations'] = {label: {'characters': len(text),
            'whitespace_normalized_sha256': hashlib.sha256(re.sub(r'\s+', '', text).encode()).hexdigest()}
            for label, text in texts.items()}
    except (NewsError, ValueError, TypeError, AttributeError, OverflowError, RecursionError):
        result['raw_public_scope'] = False
        result['identity_or_clock_malformed'] = True
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--probe", action="append", default=[])
    parser.add_argument("--canonical-fetch", action="store_true", help="Probe the actual article entrypoint with its existing queue row; retain its received response, no queue writes")
    args = parser.parse_args(argv)
    if len(args.probe) > 8:
        parser.error("at most eight explicitly selected probes; no saturation test")
    config, registry, repo = load_config(args.config)
    root = repo / config["root"]
    args.output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    status = json.loads((root / "status.json").read_text())
    db = sqlite3.connect(f"file:{root / 'queue.sqlite3'}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        states = [dict(r) for r in db.execute("SELECT * FROM state_metrics WHERE provider='cnyes'")]
        failures = {state: [dict(r) for r in db.execute(
            "SELECT url,attempts,due,error_code,metadata_date_raw FROM articles WHERE provider='cnyes' AND state=? LIMIT 12", (state,))]
            for state in ("unsupported_article_path", "body_not_found", "retry", "retry_cooldown", "article_attribution_not_verified")}
        probe_rows = {url: dict(row) for url in args.probe
                      if (row := db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())}
    finally:
        db.close()
    units = subprocess.check_output(["systemctl", "list-units", "--all", "--no-legend", "--plain", "stockagent-*.service"], text=True)
    services = {}
    for line in units.splitlines():
        unit = line.split()[0]
        fields = subprocess.check_output(["systemctl", "show", unit, "-p", "MainPID", "-p", "ActiveState", "-p", "ExecMainStartTimestampMonotonic"], text=True)
        services[unit] = dict(line.split("=", 1) for line in fields.splitlines())
    result = {"observed_at_utc": utc_now(), "services": services, "status": status, "states": states,
              "failure_samples": failures, "probes": [], "official_numeric_rate_limit": None, "all_history_complete": False,
              "canonical_fetch_entrypoint_used": args.canonical_fetch}
    atomic_write_json(args.output, result)
    if not args.probe:
        # Inventory is genuinely read-only: no HTTP owner, persisted limiter,
        # robots refresh or publisher cooldown rewrite while production runs.
        print(json.dumps({"output": str(args.output), "probes": 0, "states": states}, ensure_ascii=False))
        return 0
    for url in args.probe:
        if registry.require(url).provider != 'cnyes':
            raise ValueError('only explicitly authorized cnyes URLs')
        if args.canonical_fetch and url not in probe_rows:
            raise ValueError('canonical_fetch_probe_requires_existing_queue_row')
    try:
        http = NewsHTTP(registry, config, root)
    except NewsError as error:
        if error.code != 'news_http_owner_already_running':
            raise
        # The live canonical owner keeps fetching under the shared limiter.
        # A diagnostic must not steal its lock, turn exclusivity off, stop it
        # or send duplicate requests. Preserve the inspection snapshot and
        # explicitly defer this optional probe instead of crashing or calling
        # the publisher unavailable. No requested URL is requeued here.
        result.update(diagnostic_status_contract_version=1,
                      state='deferred_active_canonical_http_owner',
                      reason=error.code, explicit_selected_urls=list(args.probe),
                      publisher_requests=0, source_states_mutated=False,
                      raw_response_retained=False, original_source_integrity_verified=False)
        atomic_write_json(args.output, result)
        print(json.dumps({'output': str(args.output), 'state': result['state'],
                          'probes': 0, 'publisher_requests': 0}, ensure_ascii=False))
        return 0
    try:
        for number, url in enumerate(args.probe):
            source = registry.require(url)
            if source.provider != "cnyes":
                raise ValueError("only explicitly authorized cnyes URLs")
            started = time.monotonic()
            probe = {"url": url}
            try:
                response, article, extraction_error = None, None, None
                if args.canonical_fetch:
                    if url not in probe_rows:
                        raise ValueError('canonical_fetch_probe_requires_existing_queue_row')
                    original_get = http.get

                    def capture_get(*get_args, **get_kwargs):
                        nonlocal response
                        response = original_get(*get_args, **get_kwargs)
                        return response

                    http.get = capture_get
                    try:
                        article = fetch_article(http, probe_rows[url])
                    except NewsError as error:
                        if response is None:
                            raise
                        extraction_error = error.code
                    finally:
                        http.get = original_get
                else:
                    response = http.get(url)
                probe.update(status=response.status, bytes=len(response.body),
                             headers={k: v for k, v in response.headers.items() if k in ACCESS_DIAGNOSTIC_HEADERS},
                             response_sha256=hashlib.sha256(response.body).hexdigest())
                retain_public_response = False
                if "json" in response.headers.get("content-type", ""):
                    from downloader.news_fulltext_bulk import cnyes_pagination, wordpress_base
                    payload = json.loads(response.body)
                    items = payload.get("items", {})
                    rows = items.get("data", []) if isinstance(items, dict) else []
                    probe["json_structure"] = {"keys": sorted(payload), "rows": len(rows),
                        "item_keys": sorted(rows[0]) if rows else [],
                        "content_lengths": [len(str(r.get("content", ""))) for r in rows[:8]],
                        "total": items.get("total") if isinstance(items, dict) else None}
                    if response.status == 200 and wordpress_base(response.url) in source.cnyes_apis:
                        _, posts = cnyes_pagination(response)
                        retain_public_response = bool(posts) and all(
                            type(post.get('payment')) is int and post['payment'] == 0 for post in posts)
                elif response.status == 200 and "html" in response.headers.get("content-type", ""):
                    soup = BeautifulSoup(response.body, "lxml")
                    probe['cnyes_structure'] = cnyes_html_structure(soup, source, page_url=response.url)
                    canonical = soup.select_one('link[rel="canonical"]')
                    probe["canonical_url"] = canonical.get("href") if canonical else None
                    probe["script_sources"] = [s.get("src") for s in soup.select("script[src]")][:30]
                    try:
                        if args.canonical_fetch:
                            if extraction_error:
                                raise NewsError(extraction_error)
                            if article is None:
                                raise NewsError('diagnostic_not_modified_without_body')
                        else:
                            article = extract_article(response.body, source, page_url=response.url)
                        probe["article"] = {k: article.get(k) for k in ("body_sha256", "extraction_method", "body_completeness", "attribution_status", "published_at_raw")}
                        probe["body_characters"] = len(article["body"])
                        retain_public_response = True
                    except NewsError as error:
                        probe["extraction_error"] = error.code
                        # Failed public text/identity diagnosis is not article
                        # acceptance. Restriction, access, rights and unknown
                        # failures never permit retaining a response body.
                        retain_public_response = probe['cnyes_structure']['raw_public_scope'] and error.code in {
                            'body_not_found', 'source_article_empty_template',
                            'source_not_found', 'source_body_image_only', 'source_body_quote_only', 'source_body_removed'}
                if retain_public_response:
                    raw_path = args.output.parent / (args.output.stem + f"-probe-{number}.raw")
                    atomic_write_bytes(raw_path, response.body, durable=True)
                    probe['private_raw_path'] = str(raw_path)
                probe['raw_response_retained'] = retain_public_response
            except NewsError as error:
                probe.update(error=error.code, retry=error.retry, delay=error.delay)
            probe["wall_seconds"] = time.monotonic() - started
            result["probes"].append(probe)
            result["probe_performance"] = http.metrics.snapshot()
            atomic_write_json(args.output, result)
            print(json.dumps({k: v for k, v in probe.items() if k not in {"headers", "script_sources"}}, ensure_ascii=False), flush=True)
            if http.provider_not_before.get("cnyes", 0) > time.time():
                break  # Do not probe through a source refusal/outage.
    finally:
        http.close()
    print(json.dumps({"output": str(args.output), "probes": len(result["probes"]), "states": states}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

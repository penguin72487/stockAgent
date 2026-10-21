"""Private, bounded diagnosis of the canonical Cnyes news transport.

Read-only inventory by default. Probes share the production limiter, robots and
cooldowns; they never create another frontier owner or invent API permissions.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime
import hashlib
import html
import json
from itertools import islice
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


def cnyes_literal_route_hint(url):
    """Classify literal URL syntax, never article identity or source absence.

    Do not decode, normalize or generate a replacement URL. In particular,
    an old shtml path is still an unresolved article *candidate*, while a
    twstock route is a market-endpoint *candidate*, not downloaded news.
    """
    if not isinstance(url, str) or len(url) > 8192:
        return 'invalid_or_oversized_url'
    try:
        parts = urlsplit(url)
    except ValueError:
        return 'invalid_or_oversized_url'
    if parts.scheme not in {'http', 'https'} or not parts.netloc or parts.username or parts.password:
        return 'invalid_or_oversized_url'
    # Exact netlocs intentionally reject ports, lookalikes and transformed
    # identities. These hints confer no request, redirect or alias permission.
    if parts.netloc == 'www.cnyes.com' and parts.path.startswith('/twstock/'):
        return 'market_endpoint_route_candidate'
    if parts.netloc in {'news.cnyes.com', 'm.cnyes.com', 'amp-news.cnyes.com'}:
        if re.fullmatch(r'/news/id/[0-9]+/?', parts.path):
            return 'current_article_route_candidate'
        if re.fullmatch(r'(?:/(?:Content|content))?/[0-9]{8}/[A-Za-z0-9]+\.shtml', parts.path):
            return 'legacy_shtml_article_route_candidate'
        return 'other_news_host_route_unreviewed'
    if parts.netloc in {'www.cnyes.com', 'hao.cnyes.com', 'fund.cnyes.com', 'invest.cnyes.com'}:
        return 'other_cnyes_route_unreviewed'
    return 'foreign_origin_unreviewed'


def cnyes_unsupported_path_inventory(db, *, head_limit=4096):
    """One indexed, bounded read of unfinished heads; no completed-body scan.

    Keep URL-head counts and body presence separate. A syntax hint must not
    remove a head from any denominator, change a queue outcome or turn source
    content/rights/identity into a verified fact. Overflow remains explicit.
    """
    if type(head_limit) is not int or not 1 <= head_limit <= 4096:
        raise ValueError('unsupported path head limit must be 1..4096')
    rows = db.execute(
        "SELECT url,body_sha256 FROM articles INDEXED BY dispatch "
        "WHERE provider='cnyes' AND state='unsupported_article_path' "
        "ORDER BY due LIMIT ?", (head_limit + 1,)).fetchall()
    overflow = len(rows) > head_limit
    groups = defaultdict(lambda: {'url_heads': 0, 'missing_body_heads': 0,
                                  'stored_body_heads': 0, 'literal_url_samples': []})
    for url, body_sha256 in rows[:head_limit]:
        hint = cnyes_literal_route_hint(url)
        group = groups[hint]
        group['url_heads'] += 1
        group['stored_body_heads' if body_sha256 is not None else 'missing_body_heads'] += 1
        if len(group['literal_url_samples']) < 3:
            # This is the existing private inspection output, not a browser
            # projection. Do not retain oversized input or source prose.
            group['literal_url_samples'].append(url if hint not in {
                'invalid_or_oversized_url', 'foreign_origin_unreviewed'} else None)
    return {
        'contract_version': 1, 'provider': 'cnyes',
        'state': 'unsupported_article_path', 'scope': 'literal_unfinished_url_head_syntax_only',
        'head_limit': head_limit, 'heads_examined': min(len(rows), head_limit),
        'head_limit_reached': overflow, 'all_current_unsupported_heads_included': not overflow,
        'total_unsupported_url_heads': len(rows) if not overflow else None,
        'route_hint_counts': dict(sorted(groups.items())),
        'article_count_inferred': False, 'body_acceptance_granted': False,
        'source_absence_or_permission_inferred': False,
        'completion_denominator_changed': False, 'publisher_requests': 0,
        'production_queue_writes': 0, 'completed_body_rows_scanned': 0,
        'raw_or_body_payloads_read': 0,
    }


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


def cnyes_document_representation_shape(soup, source, *, article_id):
    """Explain missing native containers without exporting source prose.

    This is a bounded diagnostic, not an identity, visibility or permission
    witness. Field types/lengths are useful even when no body can be retained;
    never include values, bylines, URLs, scripts or schema excerpts here.
    """
    from downloader.news_fulltext_publishers import _cnyes_current_article_data

    def shape(value):
        result = {'type': type(value).__name__}
        if isinstance(value, str):
            result['characters'] = len(value)
        return result

    data_error = False
    try:
        data = _cnyes_current_article_data(soup, article_id)
    except (NewsError, ValueError, TypeError, AttributeError, OverflowError, RecursionError):
        data, data_error = None, True
    result = {'contract_version': 2, 'current_data_present': data is not None,
              'current_shape_malformed': data_error,
              'current_field_shapes': {key: shape(data.get(key)) for key in
                  ('title', 'content', 'payment', 'publishAt', 'url', 'author')} if data is not None else {},
              'current_payment_zero': bool(data is not None and type(data.get('payment')) is int
                                           and data['payment'] == 0),
              'matching_schema_shapes': [], 'schema_documents_examined': 0,
              'schema_objects_examined': 0, 'schema_invalid_documents': 0,
              'bounded_schema_scan': True, 'source_text_exported': False,
              'raw_scope_or_article_acceptance_granted': False}
    containers = soup.find_all(id='article-container', limit=5)
    result['native_container_limit_reached'] = len(containers) > 4
    result['native_container_shapes'] = []
    for container in containers[:4]:
        parent = container.find_parent('article')
        headers = parent.find_all('h1', limit=5) if parent is not None else []
        result['native_container_shapes'].append({
            'tag': container.name, 'has_article_parent': parent is not None,
            'article_header_count_capped_at_five': len(headers),
            'article_header_limit_reached': len(headers) > 4,
            'article_header_shapes': [{'tag': header.name, 'characters': len(header.get_text(' ', strip=True)),
                'matches_current_headline': bool(data is not None and isinstance(data.get('title'), str)
                    and headline_comparison_metadata(header.get_text(' ', strip=True), data['title'])['whitespace_equivalent'])}
                for header in headers[:4]],
            'ancestor_tags': [node.name for node in islice(container.parents, 8) if node.name],
        })
    scripts = soup.find_all('script', type='application/ld+json', limit=17)
    result['schema_document_limit_reached'] = len(scripts) > 16
    for script in scripts[:16]:
        result['schema_documents_examined'] += 1
        raw = script.get_text()
        if len(raw) > 1 << 20:
            result['schema_invalid_documents'] += 1
            continue
        try:
            nodes = list(islice(_objects(json.loads(raw)), 65))
        except (ValueError, TypeError, RecursionError):
            result['schema_invalid_documents'] += 1
            continue
        result['schema_objects_examined'] += min(len(nodes), 64)
        if len(nodes) > 64:
            result['schema_object_limit_reached'] = True
        for node in nodes[:64]:
            if node.get('@type') != 'NewsArticle':
                continue
            # A declared same-ID URL is diagnostic association only. The
            # canonical/native/current/free/author/clock proof below remains
            # wholly separate and still controls raw retention.
            try:
                parts = urlsplit(node.get('url', '')) if isinstance(node.get('url'), str) else None
            except ValueError:
                continue
            if (parts is None or parts.scheme != 'https' or parts.netloc not in
                    {'news.cnyes.com', 'm.cnyes.com'} or parts.path != f'/news/id/{article_id}'):
                continue
            if len(result['matching_schema_shapes']) == 4:
                result['schema_shape_limit_reached'] = True
                continue
            result['matching_schema_shapes'].append({key: shape(node.get(key)) for key in
                ('headline', 'articleBody', 'author', 'datePublished', 'isAccessibleForFree')})
    return result


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

    result = {"contract_version": 5, "raw_public_scope": False,
              "article_containers": len(soup.select('#article-container'))}
    parts = urlsplit(page_url)
    match = re.fullmatch(r'/news/id/([0-9]+)', parts.path)
    if (source.provider != 'cnyes' or not match or parts.scheme != 'https'
            or parts.netloc not in {'news.cnyes.com', 'm.cnyes.com'}):
        return result
    result['document_representation_shape'] = cnyes_document_representation_shape(
        soup, source, article_id=int(match[1]))
    canonical = soup.select('link[rel="canonical"][href]')
    result['canonical_count'] = len(canonical)
    scope = _cnyes_visible_article_scope(soup)
    result['unambiguous_native_scope'] = scope is not None
    if len(canonical) != 1 or scope is None:
        return result
    try:
        declared_url = normalize_url(canonical[0]['href'])
        declared_parts = urlsplit(declared_url)
        result['canonical_path_matches_request'] = declared_parts.path == parts.path
        result['canonical_public_origin'] = declared_parts.scheme == 'https' and declared_parts.netloc in {
            'news.cnyes.com', 'm.cnyes.com'}
        result['canonical_origin_kind'] = ({'news.cnyes.com': 'desktop_news', 'm.cnyes.com': 'mobile_news'}
                                           .get(declared_parts.netloc, 'other'))
        og = soup.select('meta[property="og:url"][content]')
        result['og_url_count'] = len(og)
        result['og_url_matches_canonical'] = len(og) == 1 and normalize_url(og[0]['content']) == declared_url
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
        declarations = []
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                declarations.extend(node for node in _objects(json.loads(script.get_text()))
                                    if node.get('@type') == 'NewsArticle' and node.get('url') == declared_url)
            except (ValueError, TypeError, RecursionError):
                result['invalid_schema_declaration'] = True
                return result
        result['current_schema_count'] = len(declarations)
        if len(declarations) == 1:
            entity = declarations[0].get('mainEntityOfPage')
            entity = entity.get('@id') if isinstance(entity, dict) else entity
            result['schema_main_entity_matches_canonical'] = entity == declared_url
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
    parser.add_argument("--unsupported-paths-only", action="store_true", help="Read only bounded unfinished Cnyes URL syntax; no status/service scan, HTTP or source-content determination")
    parser.add_argument("--unsupported-head-limit", type=int, default=4096, help="Maximum unfinished route heads to inspect (1..4096); overflow is unknown, not complete")
    args = parser.parse_args(argv)
    if len(args.probe) > 8:
        parser.error("at most eight explicitly selected probes; no saturation test")
    if not 1 <= args.unsupported_head_limit <= 4096:
        parser.error("unsupported head limit must be 1..4096")
    if args.unsupported_paths_only and (args.probe or args.canonical_fetch):
        parser.error("unsupported paths only cannot request publisher probes")
    config, registry, repo = load_config(args.config)
    root = repo / config["root"]
    args.output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if args.unsupported_paths_only:
        db = sqlite3.connect(f"file:{root / 'queue.sqlite3'}?mode=ro", uri=True)
        try:
            result = cnyes_unsupported_path_inventory(db, head_limit=args.unsupported_head_limit)
        finally:
            db.close()
        result['observed_at_utc'] = utc_now()
        atomic_write_json(args.output, result)
        print(json.dumps({'output': str(args.output), 'heads_examined': result['heads_examined'],
                          'head_limit_reached': result['head_limit_reached'], 'publisher_requests': 0}))
        return 0
    status = json.loads((root / "status.json").read_text())
    db = sqlite3.connect(f"file:{root / 'queue.sqlite3'}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        states = [dict(r) for r in db.execute("SELECT * FROM state_metrics WHERE provider='cnyes'")]
        unsupported_paths = cnyes_unsupported_path_inventory(db, head_limit=args.unsupported_head_limit)
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
              "unsupported_path_inventory": unsupported_paths,
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

"""Bounded public-source diagnosis through canonical robots/rate/circuit checks.

This is not a second collector or a rate saturation test. Independent publishers
can run concurrently; each publisher's selected probes are sequential. Never
persist paid/member content, credentials or denial response bodies.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import hashlib
from itertools import islice
import json
from pathlib import Path
import re
import sqlite3
import time
from urllib.parse import urlsplit

from bs4 import BeautifulSoup
from lxml import etree

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, table_from_records, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import NewsError, _objects, _plain_text_node, extract_article
from downloader.news_fulltext_queue import utc_now


def require_diagnostic_owner_scope(config, registry, canonical_config, canonical_registry, repo):
    """A different directory cannot grant another live host its own HTTP budget.

    New, disjoint hosts may use a bounded private diagnostic without stopping
    unrelated providers. Once registered/enabled, that host must use the real
    collector root and its exclusive lock, robots, cooldowns and pacing state.
    This guard neither claims the lock nor changes either queue or deadline.
    """
    root = (repo / config['root']).resolve()
    canonical_root = (repo / canonical_config['root']).resolve()
    if root == canonical_root:
        return
    active_domains = [(source.provider, domain) for source in canonical_registry.sources.values()
                      if source.allowed for domain in source.domains]
    overlap = {provider for source in registry.sources.values() for domain in source.domains
               for provider, live_domain in active_domains
               if domain == live_domain or domain.endswith('.' + live_domain) or live_domain.endswith('.' + domain)}
    if overlap:
        raise ValueError('isolated_diagnostic_overlaps_enabled_production_host; use canonical collector config/root')


def require_current_missing_body_failures(root, urls, registry, *, now=None):
    """Grant only due, genuinely missing failed heads, never stale diagnostics.

    Read before obtaining the sole HTTP lock. A live repair completed while
    draining the owner cancels this probe instead of downloading it again.
    The grant is evidence, not a queue mutation or deadline override.
    """
    if not 1 <= len(urls) <= 16 or len(set(urls)) != len(urls):
        raise ValueError('bounded_failure_probe_selection_required')
    now = time.time() if now is None else now
    db = sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        proof = []
        for url in urls:
            source = registry.require(url)
            row = db.execute('SELECT provider,url,state,error_code,body_sha256,due,last_checked_at_utc,lease_owner,lease_until '
                             'FROM articles WHERE url=?', (url,)).fetchone()
            if (not source.allowed or row is None or row['provider'] != source.provider
                    or row['state'] not in {'body_not_found', 'retry', 'retry_cooldown'}
                    or row['error_code'] != 'body_not_found' or row['body_sha256'] is not None
                    or row['due'] > now or not row['last_checked_at_utc']
                    or row['lease_owner'] is not None or row['lease_until'] is not None):
                raise ValueError('probe_no_longer_a_due_failed_missing_body')
            try:
                clock = datetime.fromisoformat(row['last_checked_at_utc'].replace('Z', '+00:00'))
                if clock.tzinfo is None:
                    raise ValueError('unaware failure clock')
            except (ValueError, TypeError):
                raise ValueError('probe_failure_observation_clock_not_verified') from None
            proof.append(dict(row))
        return proof
    finally:
        db.close()


def select_current_failure_urls(root, provider, registry, *, maximum=4, now=None):
    """Select bounded due failures after the producer has drained, not before.

    Two per retry/terminal lane lets diagnosis inspect different failure
    representations instead of repeatedly reading only the oldest head.
    Explicit finite cohorts still require direct reviewed URL selection.
    """
    source = registry.sources.get(provider)
    if (type(maximum) is not int or not 1 <= maximum <= 16 or source is None
            or not source.allowed or source.acquisition_mode == 'finite'):
        raise ValueError('bounded_enabled_ongoing_failure_provider_required')
    now = time.time() if now is None else now
    db = sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True)
    try:
        db.execute('BEGIN')
        urls = []
        for state in ('retry', 'retry_cooldown', 'body_not_found'):
            if len(urls) >= maximum:
                break
            urls.extend(row[0] for row in db.execute(
                "SELECT url FROM articles INDEXED BY dispatch WHERE provider=? AND state=? AND error_code='body_not_found' "
                "AND body_sha256 IS NULL AND due<=? AND last_checked_at_utc IS NOT NULL "
                "AND lease_owner IS NULL AND lease_until IS NULL ORDER BY due LIMIT ?",
                (provider, state, now, min(2, maximum - len(urls)))))
        if not urls:
            raise ValueError('no_due_failed_missing_body_to_probe')
        return urls
    finally:
        db.close()


def wordpress_public_metadata_shape(data, headers):
    """Bounded source declarations, never bodies, credentials or guessed names.

    `_embed=author` can return error objects or omit the relation. Describe
    that evidence without assuming WordPress core's schema is this site's
    actual schema, following profile links, or accepting any article. Sample
    four posts but count the whole bounded page; reject oversized envelopes.
    """
    if not isinstance(data, list) or len(data) > 100:
        raise NewsError('bulk_api_shape_invalid')

    def keys(value):
        return sorted(str(key)[:80] for key in islice(value, 64))[:32] if isinstance(value, dict) else []

    def author_rows(post):
        embedded = post.get('_embedded')
        authors = embedded.get('author') if isinstance(embedded, dict) else None
        return authors if isinstance(authors, list) else [authors] if isinstance(authors, dict) else []

    def public_credit(value):
        if not isinstance(value, dict):
            return {'type': type(value).__name__}
        return {'keys': keys(value),
            'id': value.get('id') if type(value.get('id')) is int else None,
            'name': value.get('name')[:200] if isinstance(value.get('name'), str) else None,
            'error_code': value.get('code')[:100] if isinstance(value.get('code'), str) else None}

    def post_shape(post):
        if not isinstance(post, dict):
            return {'type': type(post).__name__}
        content, title = post.get('content'), post.get('title')
        embedded, links = post.get('_embedded'), post.get('_links')
        link_authors = links.get('author') if isinstance(links, dict) else None
        link_authors = link_authors if isinstance(link_authors, list) else []
        head = post.get('yoast_head_json')
        meta = post.get('meta')
        return {'id': post.get('id') if type(post.get('id')) is int else None,
            'keys': keys(post), 'author_type': type(post.get('author')).__name__,
            'author_id': post.get('author') if type(post.get('author')) is int else None,
            'title_characters': len(title.get('rendered', '')) if isinstance(title, dict)
                and isinstance(title.get('rendered', ''), str) else None,
            'content_characters': len(content.get('rendered', '')) if isinstance(content, dict)
                and isinstance(content.get('rendered', ''), str) else None,
            'content_protected': content.get('protected') is True if isinstance(content, dict) else None,
            'embedded_relations': keys(embedded), 'link_relations': keys(links),
            'embedded_authors': [public_credit(author) for author in author_rows(post)[:8]],
            'author_link_embeddable': [link.get('embeddable') is True for link in link_authors[:8]
                                       if isinstance(link, dict)],
            'yoast_head_json_keys': keys(head), 'meta_keys': keys(meta),
            'yoast_declared_author': head.get('author')[:200] if isinstance(head, dict)
                and isinstance(head.get('author'), str) else None,
            'custom_credit_field_shapes': {key: {'type': type(value).__name__, 'keys': keys(value)}
                for key, value in islice(post.items(), 64)
                if re.search(r'author|creator|coauthor|credit', key, re.I)
                and key not in {'author', '_embedded', '_links'}}}

    def total(name):
        value = headers.get(name)
        return (int(value) if isinstance(value, str) and value.isascii() and value.isdecimal()
                and len(value) <= 8 and int(value) <= 10_000_000 else None)

    posts = [post for post in data if isinstance(post, dict)]
    return {'contract_version': 1, 'rows': len(data), 'object_rows': len(posts),
        'declared_total_posts': total('x-wp-total'), 'declared_total_pages': total('x-wp-totalpages'),
        'posts_with_embedded_author_name': sum(any(isinstance(author, dict)
            and isinstance(author.get('name'), str) and bool(author['name'].strip())
            for author in author_rows(post)) for post in posts),
        'posts_with_embedded_author_error': sum(any(isinstance(author, dict) and author.get('code')
            for author in author_rows(post)) for post in posts),
        'sampled_posts': [post_shape(post) for post in data[:4]],
        'samples_are_not_all_post_byline_proof': True, 'automatic_profile_requests': 0,
        'raw_response_retained': False, 'article_body_accepted': False}


def probe_provider(http, urls, *, retain=False, output_root=None):
    results = []
    for url in urls:
        source = http.registry.require(url)
        if http.provider_not_before.get(source.provider, 0) > time.time():
            results.append({"url": url, "provider": source.provider, "error": "persisted_provider_cooldown", "sent": False})
            break
        start = time.monotonic()
        item = {"url": url, "provider": source.provider}
        try:
            response = http.get(url)
            item.update(status=response.status, final_url=response.url, bytes=len(response.body),
                        response_sha256=hashlib.sha256(response.body).hexdigest())
            if response.status == 200 and "html" in response.headers.get("content-type", ""):
                soup = BeautifulSoup(response.body, "lxml")
                if source.provider == 'cnyes':
                    from scripts.inspect_cnyes_news import cnyes_html_structure
                    item['cnyes_structure'] = cnyes_html_structure(soup, source, page_url=response.url)
                item["containers"] = {s: [{"characters": len(n.get_text(' ', strip=True)), "tag": n.name,
                                          "classes": n.get('class')} for n in soup.select(s)] for s in source.selectors}
                if source.provider in {"cnews", "cmmedia"}:
                    # Diagnose the actual current article, not a global login
                    # dialog or a guessed CSS recipe. Structure and public
                    # credits only; no article/login body or form values here.
                    scopes = soup.select(".theme-article-content" if source.provider == "cnews" else "#main")
                    item["publisher_article_structure"] = [{
                        "id": scope.get("id"), "classes": scope.get("class"),
                        "headlines": [{"tag": n.name, "classes": n.get("class"), "text": n.get_text(" ", strip=True)[:200]}
                                      for n in scope.select("h1, h2, h3")[:8]],
                        "sections": [{"tag": n.name, "id": n.get("id"), "classes": n.get("class"),
                                      "itemprop": n.get("itemprop"), "characters": len(n.get_text(" ", strip=True))}
                                     for n in scope.select("header, section, div[class], div[id]")[:32]],
                        "credits": [{"tag": n.name, "classes": n.get("class"), "text": n.get_text(" ", strip=True)[:160],
                                     "datetime": n.get("datetime"), "href": n.get("href")}
                                    for n in scope.select("[rel=author], [itemprop=author], [itemprop=datePublished], time, .post-author, .post-date")[:16]],
                    } for scope in scopes[:4]]
                    item["article_access_markers"] = [{"tag": n.name, "classes": n.get("class"),
                        "id": n.get("id"), "has_data_paywall": n.has_attr("data-paywall"),
                        "login_form": n.name == "form", "nearest_article_id": n.find_parent("article").get("id") if n.find_parent("article") else None,
                        "ancestors": [{"tag": p.name, "id": p.get("id"), "classes": p.get("class")}
                                      for p in list(n.parents)[:8] if p.name]}
                        for n in soup.select(".paywall, [data-paywall], form[action*='login']")[:16]]
                    item["declared_article_metadata"] = [{"tag": n.name, "field": n.get("property") or n.get("name"),
                        "content": n.get("content", "")[:240], "href": n.get("href")}
                        for n in soup.select("meta[property='og:url'], meta[property='og:title'], meta[property='article:published_time'], link[rel=canonical]")[:12]]
                    if source.provider == "cnews":
                        item["original_credit_candidates"] = [n.get_text(" ", strip=True)[:180]
                            for n in soup.select(".theme-article-content > article[id^='post-'] p")
                            if not n.find("p") and re.match(r"^(?:CNEWS|匯流新聞網|本文|內容來源|來源[：:])", n.get_text(" ", strip=True))][:8]
                        item["publisher_header_elements"] = [{"tag": n.name, "classes": n.get("class"), "id": n.get("id"),
                            "text": n.get_text(" ", strip=True)[:200], "datetime": n.get("datetime"), "href": n.get("href"),
                            "ancestors": [{"tag": p.name, "classes": p.get("class"), "id": p.get("id")}
                                          for p in list(n.parents)[:4] if p.name]}
                            for n in soup.select("h1, h2, [rel=author], time, .date")[:24]]
                if source.provider == "cmnews":
                    # Structure only: distinguish a real article access notice
                    # from short public text without retaining a member page or
                    # borrowing a notice from an adjacent infinite-scroll story.
                    notices = {
                        "請登入以查看完整文章": "full_article_requires_login",
                        "閱讀VIP文章請先登入理財寶會員": "vip_requires_login",
                    }
                    item["article_access_structure"] = []
                    for scope in soup.select(".articleContent[data-article-path]")[:8]:
                        markers = []
                        for node in scope.find_all(string=True):
                            kind = notices.get(str(node).strip())
                            if not kind:
                                continue
                            markers.append({"kind": kind,
                                "ancestors": [{"tag": p.name, "id": p.get("id"), "classes": p.get("class")}
                                              for p in list(node.parents)[:6] if p.name]})
                        item["article_access_structure"].append({
                            "article_path": scope.get("data-article-path"),
                            "container_id": scope.get("id"),
                            "author_article_id": scope.get("data-author-id-article-id"),
                            "access_notices": markers[:8],
                            "body_nodes": [{"characters": len(body.get_text(" ", strip=True)),
                                "normalized_characters": len(_plain_text_node(body, source.exclude_selectors,
                                                                              parser=source.html_parser)),
                                "element_spans": [{"tag": node.name, "classes": node.get("class"),
                                    "characters": len(node.get_text(" ", strip=True)),
                                    "parent_tag": node.parent.name,
                                    "parent_classes": node.parent.get("class")}
                                    for node in body.select("figure, figcaption, nav, aside, p, div, a")[:40]]}
                                for body in scope.select(".articleContent__textContent")[:4]],
                        })
                item["title_matches"] = len(soup.select(source.title_selector)) if source.title_selector else 0
                item["title_text"] = [n.get_text(" ", strip=True) for n in soup.select(source.title_selector)][:4] if source.title_selector else []
                item["configured_authors"] = [n.get_text(" ", strip=True) for selector in source.author_selectors for n in soup.select(selector)][:16]
                if source.provider in {"voa_chinese", "voa_cantonese"}:
                    # Diagnose the declared article, not the navigation clock,
                    # related-story credits or embedded audio controls. Only
                    # bounded public header text and DOM shapes leave the raw
                    # private response; article prose is never printed here.
                    item["publisher_header_elements"] = [{
                        "tag": n.name, "id": n.get("id"), "classes": n.get("class"),
                        "text": n.get_text(" ", strip=True)[:180],
                        "datetime": n.get("datetime"), "content": n.get("content"),
                        "ancestors": [{"tag": p.name, "id": p.get("id"), "classes": p.get("class")}
                                      for p in list(n.parents)[:4] if p.name]}
                        for n in soup.select('h1, time, .authors li, .author__name, .date, '
                                             'meta[property="article:published_time"]')[:24]]
                    item["publisher_body_element_shapes"] = [{
                        "tag": n.name, "id": n.get("id"), "classes": n.get("class"),
                        "characters": len(n.get_text(" ", strip=True))}
                        for scope in soup.select('.wsw, [itemprop="articleBody"]')[:4]
                        for n in list(scope.find_all(recursive=False))[:32]]
                    item["reviewed_byline_candidates"] = [{
                        "name": str(n).strip(),
                        "ancestors": [{"tag": p.name, "id": p.get("id"), "classes": p.get("class")}
                                      for p in list(n.parents)[:6] if p.name]}
                        for n in soup.find_all(string=lambda value: bool(value)
                                               and value.strip() in source.permitted_author_names)[:8]]
                    item["declared_article_metadata"] = [{"tag": n.name,
                        "field": n.get("property") or n.get("name"),
                        "content": n.get("content", "")[:240], "href": n.get("href")}
                        for n in soup.select("meta[property='og:url'], meta[property='og:title'], link[rel=canonical]")[:12]]
                    scopes = soup.select("main.container")
                    names = [n.get_text(" ", strip=True) for n in scopes[0].select(source.author_selectors[0])] if len(scopes) == 1 else []
                    # Identity parsing may fail before the byline rights gate.
                    # Such a failure must not accidentally retain an excluded
                    # publisher/third-party body as a private diagnostic.
                    item["reviewed_current_byline_scope"] = bool(names) and all(
                        name in source.permitted_author_names for name in names)
                    if item["reviewed_current_byline_scope"]:
                        from downloader.news_fulltext_public_originals import _visible
                        bodies = scopes[0].select("#article-content > .wsw")
                        item["reviewed_current_byline_scope"] = len(bodies) == 1
                        try:
                            for node in [scopes[0], *bodies, *scopes[0].select(source.author_selectors[0])]:
                                _visible(node)
                        except NewsError:
                            item["reviewed_current_byline_scope"] = False
                if source.provider in {"nownews", "ftvnews", "voa_chinese", "voa_cantonese"}:
                    # Public credits/clocks only, including failed original-
                    # scope decisions. Never retain a third-party/paid body to
                    # diagnose an old newsroom label or subsite DOM layout.
                    item["publisher_public_credits"] = [{"tag": n.name,
                        "classes": n.get("class"), "content": n.get("content"),
                        "text": n.get_text(" ", strip=True)[:200]}
                        for n in soup.select('meta[itemprop="author"][content], .mainBlk .infoBlk .info > p, '
                                             '.newsInfo, .news-status > li, .articleInfo, .news__author')[:10]]
                    item["publisher_declared_articles"] = []
                    for node in soup.find_all("script", type="application/ld+json"):
                        try:
                            for declared in _objects(json.loads(node.get_text())):
                                if declared.get("@type") in ("Article", "NewsArticle"):
                                    item["publisher_declared_articles"].append({key: declared.get(key)
                                        for key in ("url", "mainEntityOfPage", "headline", "author", "datePublished", "dateModified", "isAccessibleForFree")})
                        except (ValueError, TypeError):
                            continue
                    item["publisher_declared_articles"] = item["publisher_declared_articles"][:8]
                    if source.provider in {"voa_chinese", "voa_cantonese"}:
                        declarations = item["publisher_declared_articles"]
                        declaration = declarations[0] if len(declarations) == 1 else {}
                        authors = declaration.get("author")
                        authors = authors if isinstance(authors, list) else [authors]
                        declared_names = [author.get("name") for author in authors if isinstance(author, dict)]
                        item["reviewed_current_byline_scope"] &= (
                            declaration.get("url") == response.url
                            and declaration.get("mainEntityOfPage") == response.url
                            and declaration.get("isAccessibleForFree") is True
                            and len(declared_names) == len(authors) and declared_names == names)
                    if source.provider == "ftvnews":
                        body = soup.select_one("#contentarea[itemprop=articleBody]")
                        item["publisher_article_credit_candidates"] = [n.get_text(" ", strip=True)
                            for n in body.find_all("p") if not n.find("p")
                            and len(n.get_text(" ", strip=True)) <= 200
                            and re.match(r"^[（(]?(?:民視|即時|財經|政治|社會|生活|娛樂|體育|國際|論壇|CNEWS|匯流|圖[、,，]?文[／/]|圖[／/]|文[／/]|CTWANT|周刊王)", n.get_text(" ", strip=True))][:12] if body else []
                item["top_level_body_elements"] = [{"tag": n.name, "id": n.get("id"), "classes": n.get("class"),
                    "width": n.get("width"), "align": n.get("align"),
                    "characters": len(n.get_text(" ", strip=True))} for n in soup.body.find_all(recursive=False) if n.name][:30] if soup.body else []
                item["body_container_candidates"] = [{"tag": n.name, "id": n.get("id"), "classes": n.get("class"),
                    "characters": len(n.get_text(" ", strip=True))} for n in soup.select("article, main, div[id], div[class]")
                    if len(n.get_text(" ", strip=True)) > 300][:40]
                item["license_links"] = [a.get("href") for a in soup.select("a[href*='creativecommons.org']")][:20]
                item["license_link_contexts"] = [{"href": a.get("href"),
                    "ancestors": [{"tag": n.name, "id": n.get("id"), "classes": n.get("class")}
                                  for n in list(a.parents)[:12] if n.name],
                    "notice": a.parent.get_text(" ", strip=True)[:240]}
                    for a in soup.select("a[href*='creativecommons.org']")][:20]
                if source.provider == 'peopo':
                    item['publisher_clock_elements'] = [{"tag": n.name, "classes": n.get('class'),
                        "datetime": n.get('datetime'), "text": n.get_text(' ', strip=True)[:100]}
                        for n in soup.select('#block-peopo-content time, #block-peopo-content .field--name-created')]
                    item['current_article_authors'] = [{"name": n.get_text(' ', strip=True),
                        "ancestors": [{"tag": p.name, "id": p.get('id'), "classes": p.get('class')}
                                      for p in list(n.parents)[:8] if p.name]}
                        for n in soup.select('#block-peopo-content article.node--view-mode-full .username')][:8]
                script = soup.find("script", id="__NEXT_DATA__")
                if script:
                    try:
                        data = json.loads(script.get_text())
                        shapes = []
                        def visit(value, path="", depth=0):
                            if depth > 5 or len(shapes) >= 120:
                                return
                            if isinstance(value, dict):
                                shapes.append({"path": path, "keys": list(value)[:40]})
                                for key, child in value.items():
                                    visit(child, path + "." + key, depth + 1)
                            elif isinstance(value, list):
                                shapes.append({"path": path, "rows": len(value)})
                                if value:
                                    visit(value[0], path + "[0]", depth + 1)
                            elif isinstance(value, str):
                                shapes.append({"path": path, "characters": len(value)})
                        visit(data.get("props", {}), "props")
                        item["hydration_structure"] = shapes
                    except (ValueError, TypeError):
                        item["hydration_error"] = "invalid_json"
                item["byline_elements"] = [{"tag": node.name, "classes": node.get("class"), "id": node.get("id"),
                                            "text": node.get_text(" ", strip=True)[:150],
                                            "author_links": [{"text": a.get_text(' ', strip=True), "classes": a.get('class'),
                                                              "href": a.get('href')} for a in node.select('a[href]')]}
                                           for node in soup.select("#post-header-credit, [rel=author], .contributor-name")][:10]
                item["links"] = [{"href": a.get("href"), "title": a.get_text(" ", strip=True)[:120]}
                                  for a in soup.select("#article a[href], #content a[href]")][:200]
                try:
                    article = extract_article(response.body, source, page_url=response.url)
                    item["extracted"] = {k: article.get(k) for k in ("title", "authors_json", "published_at_raw", "body_sha256", "body_completeness", "extraction_method", "declared_license_urls_json")}
                    item["body_characters"] = len(article["body"])
                except NewsError as error:
                    item["extraction_error"] = error.code
                if retain and item.get("extraction_error") not in {"paywall_or_login", "access_challenge", "article_rights_or_scope_excluded",
                                                                "article_license_not_verified", "article_rights_not_verified"} and (
                        source.provider != 'cnyes' or item.get('cnyes_structure', {}).get('raw_public_scope', False)) and (
                        source.provider not in {"voa_chinese", "voa_cantonese"} or item.get("reviewed_current_byline_scope") is True):
                    # Private diagnosis is not article acceptance. Keep the raw
                    # fetched public response and its source policy, never a
                    # refused/paid/member response or a body in the browser.
                    # Identical soft-404 bodies can belong to different
                    # requests. Body-only naming overwrote their provenance.
                    capture_key = hashlib.sha256(json.dumps([source.provider, url, response.url,
                                                           item['response_sha256']], ensure_ascii=False).encode()).hexdigest()
                    path = output_root / (capture_key + ".parquet")
                    atomic_write_parquet(path, table_from_records([{"request_url": url, "final_url": response.url,
                        "response_sha256": item["response_sha256"], "raw_response_body": response.body,
                        "headers_json": json.dumps(response.headers), "provider": source.provider,
                        "authorization_json": json.dumps(source.authorization, ensure_ascii=False),
                        "captured_at_utc": utc_now(), "diagnostic_only": True, "accepted_as_article": False, "publish": False}]),
                        compression="zstd", compression_level=9, durable=True)
                    atomic_write_json(path.with_suffix('.receipt.json'), {"sha256": sha256_file(path), "path": str(path),
                        "response_sha256": item["response_sha256"], "diagnostic_only": True, "accepted_as_article": False, "publish": False})
                    item["private_diagnostic_path"] = str(path)
            elif response.status == 200 and ("xml" in response.headers.get("content-type", "") or response.body.lstrip().startswith(b"<?xml")):
                try:
                    tree = etree.fromstring(response.body, etree.XMLParser(resolve_entities=False, no_network=True))
                except etree.XMLSyntaxError as error:
                    # A broken source document must not abort diagnosis of
                    # other endpoints. Retain locations/types, not rejected
                    # body text or a forgiving parse promoted to completeness.
                    item['xml_parse_errors'] = [{'type': e.type_name, 'domain': e.domain_name,
                        'line': e.line, 'column': e.column} for e in error.error_log[-8:]]
                    controls = re.findall(rb'[\x00-\x08\x0b\x0c\x0e-\x1f]', response.body)
                    item['xml_invalid_ascii_controls'] = {f'U+{code[0]:04X}': controls.count(code)
                        for code in sorted(set(controls))}
                    if controls:
                        try:
                            metadata_tree = etree.fromstring(re.sub(rb'[\x00-\x08\x0b\x0c\x0e-\x1f]', b'', response.body),
                                etree.XMLParser(resolve_entities=False, no_network=True))
                            item['xml_control_filtered_metadata_only'] = {
                                'root': metadata_tree.tag,
                                'items': len(metadata_tree.findall('./channel/item')),
                                'article_links': [n.text for n in metadata_tree.xpath("./channel/item/link")][:6],
                                'accepted_as_article': False,
                            }
                        except etree.XMLSyntaxError:
                            item['xml_control_filtered_still_invalid'] = True
                    raise NewsError('invalid_feed') from None
                entries = tree.xpath("//*[local-name()='item']")
                item["xml_items"] = len(entries)
                item["xml_item_fields"] = [etree.QName(n).localname for n in entries[0]] if entries else []
                item["xml_article_links"] = [n.text for n in tree.xpath("//*[local-name()='item']/*[local-name()='link']")][:6]
                item["xml_description_characters"] = [len(n.text or "") for n in tree.xpath("//*[local-name()='item']/*[local-name()='description']")][:6]
                item["xml_body_characters"] = [len(n.text or "") for n in tree.xpath("//*[local-name()='item']/*[local-name()='encoded']")][:6]
                item["xml_creator_samples"] = [n.text for n in tree.xpath("//*[local-name()='item']/*[local-name()='creator']")][:6]
                if response.url.split('?', 1)[0] in source.fulltext_feeds:
                    from downloader.news_fulltext_feed import feed_records
                    records, errors = feed_records(response, http, source)
                    item['feed_valid_body_rows'] = len(records)
                    item['feed_item_errors'] = errors
                    # Diagnose the location of rights flags without retaining
                    # an unreviewed/mixed raw feed or logging article bodies.
                    item['feed_rights_markers'] = []
                    for number, entry in enumerate(entries):
                        encoded = entry.find('{http://purl.org/rss/1.0/modules/content/}encoded')
                        fragment = BeautifulSoup(encoded.text or '', source.html_parser) if encoded is not None else None
                        flags = []
                        if fragment is not None:
                            for node in fragment.find_all(string=re.compile(r'all rights reserved|paywall', re.I))[:6]:
                                flags.append({'kind': 'rights_text_or_paywall_word', 'text': str(node).strip()[:160],
                                    'parent_tag': node.parent.name, 'parent_classes': node.parent.get('class'),
                                    'parent_href': node.parent.get('href')})
                            for node in fragment.select('.paywall, [data-paywall], [rel=license]')[:8]:
                                flags.append({'kind': 'explicit_marker_or_license', 'tag': node.name,
                                    'classes': node.get('class'), 'href': node.get('href')})
                        if flags or entry.find('{http://purl.org/dc/elements/1.1/}rights') is not None:
                            item['feed_rights_markers'].append({'item_number': number, 'url': entry.findtext('link'),
                                'rights': entry.findtext('{http://purl.org/dc/elements/1.1/}rights'), 'flags': flags})
            elif response.status == 200 and "json" in response.headers.get("content-type", ""):
                try:
                    data = json.loads(response.body)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    # A bad publisher envelope must not abort diagnosis of
                    # subsequent explicit endpoints. Keep only its hash and
                    # error class, never decoder excerpts of rejected text.
                    raise NewsError('bulk_api_invalid_json') from None
                item["json_rows"] = len(data) if isinstance(data, list) else None
                first = data[0] if isinstance(data, list) and data and isinstance(data[0], dict) else {}
                item["json_first_keys"] = sorted(islice(first, 64))[:32]
                embedded = first.get('_embedded')
                item["embedded_author_keys"] = sorted(islice(embedded, 64))[:32] if isinstance(embedded, dict) else []
                if urlsplit(response.url).path == '/wp-json/wp/v2/posts':
                    item['wordpress_public_metadata'] = wordpress_public_metadata_shape(data, response.headers)
                if source.provider == "cnyes" and response.url.split("?", 1)[0] in source.cnyes_apis:
                    from downloader.news_fulltext_bulk import cnyes_pagination, cnyes_records
                    envelope = data.get("items") if isinstance(data, dict) else None
                    posts = envelope.get("data") if isinstance(envelope, dict) else None
                    item["publisher_pagination"] = {
                        key: envelope.get(key) for key in ("total", "per_page", "current_page", "last_page")
                    } if isinstance(envelope, dict) else None
                    item["publisher_rows"] = len(posts) if isinstance(posts, list) else None
                    # Only public metadata and field lengths, never article
                    # text or a mixed paid/free envelope in diagnosis output.
                    item["publisher_post_shapes"] = [{
                        "newsId": post.get("newsId"), "publishAt": post.get("publishAt"),
                        "payment": post.get("payment"), "keys": sorted(post),
                        "content_characters": len(post["content"]) if isinstance(post.get("content"), str) else None,
                        "content_has_markup": "<" in post["content"] if isinstance(post.get("content"), str) else None,
                        "source": str(post.get("source") or "")[:160],
                    } for post in posts[:30] if isinstance(post, dict)] if isinstance(posts, list) else []
                    try:
                        cnyes_pagination(response)
                        rows, errors = cnyes_records(response, http, source)
                        item["bulk_validated_body_rows"] = len(rows)
                        item["bulk_item_errors"] = errors
                    except NewsError as error:
                        item["bulk_validation_error"] = error.code
                        item["bulk_validation_reason"] = str(error.__cause__)[:200] if isinstance(error.__cause__, ValueError) else None
                    if retain and isinstance(posts, list) and posts and all(
                            isinstance(post, dict) and type(post.get("payment")) is int and post["payment"] == 0 for post in posts):
                        capture_key = hashlib.sha256(json.dumps([source.provider, url, response.url,
                                                               item["response_sha256"]]).encode()).hexdigest()
                        path = output_root / (capture_key + ".parquet")
                        atomic_write_parquet(path, table_from_records([{
                            "request_url": url, "final_url": response.url, "provider": source.provider,
                            "raw_response_body": response.body, "response_sha256": item["response_sha256"],
                            "captured_at_utc": utc_now(), "diagnostic_only": True,
                            "accepted_as_article": False, "publish": False,
                        }]), compression="zstd", compression_level=9, durable=True)
                        atomic_write_json(path.with_suffix(".receipt.json"), {
                            "path": str(path), "sha256": sha256_file(path),
                            "response_sha256": item["response_sha256"], "url": url,
                            "diagnostic_only": True, "accepted_as_article": False, "publish": False,
                        }, durable=True)
                        item["private_diagnostic_path"] = str(path)
        except NewsError as error:
            item.update(error=error.code, retry=error.retry, delay=error.delay)
            chain, seen = error.__cause__, set()
            item["cause_chain"] = []
            while chain is not None and id(chain) not in seen:
                seen.add(id(chain))
                item["cause_chain"].append({"type": type(chain).__name__, "errno": getattr(chain, "errno", None),
                                            "message": str(chain)[:500]})
                chain = chain.__cause__ or chain.__context__
        item.update(wall_seconds=time.monotonic() - start,
                    robot_delay_seconds=http.robot_delays.get(source.provider, 0))
        results.append(item)
        print(json.dumps({k: v for k, v in item.items() if k not in {"links", "byline_elements"}}, ensure_ascii=False), flush=True)
        if http.provider_not_before.get(source.provider, 0) > time.time():
            break
    return results


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--probe", action="append", default=[])
    selection.add_argument('--current-failure-provider', help='Select current due missing-body failures after draining the canonical producer')
    parser.add_argument('--maximum-failure-probes', type=int, default=4)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--retain-reviewed-response", action="store_true", help="Retain private public diagnostics, not accepted bodies or denied/member responses")
    parser.add_argument("--trace-access", action="store_true", help="Record private bounded redirect/header/robots structure through the same HTTP path")
    parser.add_argument('--require-current-missing-body-failure', action='store_true',
                        help='Read-only exact-head preflight; reject saved, leased, denied, undated or not-yet-due selections')
    args = parser.parse_args(argv)
    if len(args.probe) > 16:
        parser.error("at most sixteen explicitly selected probes")
    config, registry, repo = load_config(args.config)
    canonical_config, canonical_registry, _ = load_config(repo / 'configs/news_fulltext.json')
    require_diagnostic_owner_scope(config, registry, canonical_config, canonical_registry, repo)
    if args.current_failure_provider:
        try:
            args.probe = select_current_failure_urls(repo / config['root'], args.current_failure_provider, registry,
                                                    maximum=args.maximum_failure_probes)
        except ValueError as error:
            if str(error) != 'no_due_failed_missing_body_to_probe':
                raise  # Invalid scope/bounds/storage still fail explicitly.
            # Draining the producer can finish/retry our previous candidates.
            # No current work is an expected read-only decision, not permission
            # to clear a deadline, fetch a stale URL or misreport completion.
            args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            atomic_write_json(args.output, {
                'diagnostic_status_contract_version': 1, 'observed_at_utc': utc_now(),
                'state': 'skipped_no_due_failed_missing_body', 'provider': args.current_failure_provider,
                'maximum_failure_probes': args.maximum_failure_probes, 'probes': [],
                'publisher_requests': 0, 'source_states_mutated': False, 'publish': False,
                'all_history_complete': False, 'official_rate_limit': None,
            })
            return 0
        args.require_current_missing_body_failure = True
    groups = defaultdict(list)
    for url in args.probe:
        groups[registry.require(url).provider].append(url)
    failure_grant = None
    if args.require_current_missing_body_failure:
        try:
            failure_grant = require_current_missing_body_failures(repo / config['root'], args.probe, registry)
        except ValueError as error:
            if str(error) != 'probe_no_longer_a_due_failed_missing_body':
                raise  # Bad bounds, source clocks and storage still fail.
            # An explicitly selected URL can be retried or completed by the
            # ordinary owner during drain, just like a dynamic selection.
            # Preserve the new deadline/state; no second HTTP owner/request,
            # stale grant or claim that the article has been recovered.
            args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            atomic_write_json(args.output, {
                'diagnostic_status_contract_version': 1, 'observed_at_utc': utc_now(),
                'state': 'skipped_no_longer_due_failed_missing_body',
                'explicit_selected_urls': args.probe, 'probes': [],
                'publisher_requests': 0, 'source_states_mutated': False, 'publish': False,
                'all_history_complete': False, 'official_rate_limit': None,
            })
            return 0
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    diagnostics = args.output.parent / "private-source-diagnostics"
    if args.retain_reviewed_response:
        diagnostics.mkdir(parents=True, exist_ok=True, mode=0o700)
    trace = None
    if args.trace_access:
        from scripts.diagnose_news_access import AccessTrace
        trace = AccessTrace(registry)
    http = NewsHTTP(registry, config, repo / config["root"], access_observer=trace.observe if trace else None)
    try:
        with ThreadPoolExecutor(max_workers=min(len(groups), config["workers"])) as pool:
            results = list(pool.map(lambda urls: probe_provider(http, urls, retain=args.retain_reviewed_response, output_root=diagnostics), groups.values()))
        result = {"observed_at_utc": utc_now(), "probes": [r for group in results for r in group],
                  "performance": http.metrics.snapshot(), "official_rate_limit": None}
        if failure_grant is not None:
            result['current_failed_missing_body_preflight'] = failure_grant
        if trace:
            result["http_path_by_provider"] = {provider: trace.snapshot(provider) for provider in groups}
        atomic_write_json(args.output, result)
        return 0
    finally:
        http.close()


if __name__ == "__main__":
    raise SystemExit(main())

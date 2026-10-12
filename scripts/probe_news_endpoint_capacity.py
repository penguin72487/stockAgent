"""Finite canonical public endpoint/capability discovery, not a second crawler.

All source restrictions and the shared publisher bucket remain authoritative.
Candidates come from actual publisher category links, never guessed news IDs.
Only hashes, pagination and safe metadata are retained in this private report.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import re
import time
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup
from lxml import etree

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext import load_config
from downloader.news_fulltext_bulk import cnyes_pagination, cnyes_records
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import utc_now


def probe_provider(http, source):
    results, categories = [], []
    endpoints = [(u, "feed") for u in source.feeds] + [(u, "sitemap") for u in source.sitemaps]
    endpoints += [(u, "index") for u in source.index_pages]
    if source.provider == "cnyes":
        endpoints = [("https://news.cnyes.com/news/cat/headline", "category_directory"),
                     ("https://news.cnyes.com/", "category_directory")]
    for url, kind in endpoints:
        item = {"url": url, "kind": kind, "provider": source.provider}
        try:
            response = http.get(url)
            item.update(status=response.status, bytes=len(response.body), response_sha256=hashlib.sha256(response.body).hexdigest())
            if kind == "category_directory" and response.status == 200:
                soup = BeautifulSoup(response.body, "lxml")
                for anchor in soup.select("a[href]"):
                    target = urljoin(response.url, anchor["href"])
                    parts = urlsplit(target)
                    match = re.fullmatch(r"/news/cat/([a-z0-9_]+)", parts.path)
                    if parts.hostname == "news.cnyes.com" and not parts.query and match:
                        slug = match[1]
                        if slug not in categories:
                            categories.append(slug)
                item["observed_category_slugs"] = categories[:40]
                item["directory_candidate_count"] = len(categories)
                node = soup.select_one("script#__NEXT_DATA__")
                if node and node.string:
                    try:
                        state = json.loads(node.string)
                        item["next_data_page_props_keys"] = sorted(state.get("props", {}).get("pageProps", {}))
                        def collect(value, path=""):
                            if isinstance(value, dict):
                                if "slug" in value and "categoryId" in value and isinstance(value["slug"], str) and re.fullmatch(r"[a-z0-9_]+", value["slug"]):
                                    slug = value["slug"]
                                    if slug not in categories and len(categories) < 40:
                                        categories.append(slug)
                                for key, child in value.items():
                                    collect(child, path + "/" + key)
                            elif isinstance(value, list):
                                for child in value[:1000]:
                                    collect(child, path)
                        collect(state)
                        item["observed_category_slugs"] = categories[:40]
                    except (ValueError, TypeError):
                        item["directory_state_error"] = "invalid_publisher_state_json"
            elif kind in {"feed", "sitemap"} and response.status == 200:
                tree = etree.fromstring(response.body, etree.XMLParser(resolve_entities=False, no_network=True))
                entries = tree.xpath("//*[local-name()='item' or local-name()='url' or local-name()='sitemap']")
                item["rows"] = len(entries)
                item["fulltext_fields"] = len(tree.xpath("//*[local-name()='item']/*[local-name()='encoded']"))
            item["official_interval_floor_seconds"] = http.hard_interval_floor(source.provider)
        except (NewsError, etree.XMLSyntaxError) as error:
            item["error"] = error.code if isinstance(error, NewsError) else "invalid_source_xml"
        results.append(item)
        if http.provider_not_before.get(source.provider, 0) > time.time():
            break
    if source.provider == "cnyes":
        # The existing endpoint is a control. New slugs must be observed on the
        # publisher's own directory before trying the same public API template.
        candidates = ["headline"] + [c for c in categories[:40] if c != "headline"]
        for slug in candidates:
            base = "https://api.cnyes.com/media/api/v1/newslist/category/" + slug
            limits = [30, 100, 300] if slug == "headline" else [100]
            for limit in limits:
                url = f"{base}?limit={limit}&page=1"
                item = {"url": url, "provider": source.provider, "kind": "cnyes_category", "requested_limit": limit,
                        "basis": "existing registered control" if slug == "headline" else "observed publisher category link plus verified public API template"}
                try:
                    response = http.get(url)
                    item.update(status=response.status, bytes=len(response.body), response_sha256=hashlib.sha256(response.body).hexdigest())
                    try:
                        payload = json.loads(response.body)
                        pagination = payload.get("items")
                        item["publisher_status_code"] = payload.get("statusCode")
                        item["pagination_fields"] = {k: pagination.get(k) for k in ("total", "per_page", "current_page", "last_page")} if isinstance(pagination, dict) else None
                    except (ValueError, AttributeError):
                        pass
                    page, posts = cnyes_pagination(response)
                    records, errors = cnyes_records(response, http, replace(source, cnyes_apis=(base,)))
                    item.update(status=response.status, page=page, returned_rows=len(posts),
                                validated_body_rows=len(records), item_errors=errors,
                                response_sha256=hashlib.sha256(response.body).hexdigest(), bytes=len(response.body),
                                observed_article_ids=[post["newsId"] for post in posts],
                                endpoint_candidate=base, parser_accepted=True)
                except NewsError as error:
                    item.update(error=error.code, parser_accepted=False)
                results.append(item)
                if http.provider_not_before.get(source.provider, 0) > time.time():
                    return results
    return results


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    config, registry, repo = load_config(args.config)
    http = NewsHTTP(registry, config, repo / config["root"])
    try:
        sources = [s for s in registry.sources.values() if s.allowed]
        with ThreadPoolExecutor(max_workers=len(sources)) as pool:
            groups = list(pool.map(lambda s: probe_provider(http, s), sources))
        result = {"observed_at_utc": utc_now(), "probes": [p for group in groups for p in group],
                  "not_probed": [{"provider": s.provider, "reason": "source_not_authorized_or_enabled"}
                                 for s in registry.sources.values() if not s.allowed],
                  "performance": http.metrics.snapshot(), "all_endpoints_claimed": False, "publish": False}
        atomic_write_json(args.output, result)
        print(json.dumps({"probes": len(result["probes"]), "output": str(args.output)}, ensure_ascii=False))
        return 0
    finally:
        http.close()


if __name__ == "__main__":
    raise SystemExit(main())

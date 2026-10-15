"""Inspect actually linked publisher bundles for omitted public category links.

Finite metadata discovery only: no execution of JavaScript, cookies, secrets,
guessed article IDs, or unbounded module crawling. All requests use NewsHTTP.
"""
from __future__ import annotations

import argparse
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import re
import time
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext import load_config
from downloader.news_fulltext_bulk import cnyes_pagination, cnyes_records
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import utc_now


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    config, registry, repo = load_config(Path("configs/news_fulltext.json"))
    http = NewsHTTP(registry, config, repo / config["root"])
    results, candidates = [], {}
    try:
        directory = "https://news.cnyes.com/news/cat/headline"
        response = http.get(directory)
        if response.status != 200:
            raise NewsError("navigation_http_" + str(response.status))
        soup = BeautifulSoup(response.body, "lxml")
        targets = list(dict.fromkeys(urljoin(response.url, n["src"]) for n in soup.select("script[src]")
            if urlsplit(urljoin(response.url, n["src"])).hostname == "news.cnyes.com"))[:40]
        # Static navigation strings are source evidence, not instructions.
        for url, text in [(directory, response.body.decode("utf-8", errors="replace"))]:
            for slug in re.findall(r"/news/cat/([a-z0-9_]+)", text):
                candidates.setdefault(slug, url)
        for url in targets:
            item = {"url": url}
            try:
                response = http.get(url)
                item.update(status=response.status, bytes=len(response.body), response_sha256=hashlib.sha256(response.body).hexdigest())
                if response.status == 200:
                    text = response.body.decode("utf-8", errors="replace")
                    slugs = sorted(set(re.findall(r"/news/cat/([a-z0-9_]+)", text)))
                    item["observed_slugs"] = slugs
                    item["public_api_route_fragments"] = sorted(set(re.findall(r"/media/api/v[0-9]+/[a-zA-Z0-9_/-]+", text)))[:40]
                    for slug in slugs:
                        candidates.setdefault(slug, url)
            except NewsError as error:
                item["error"] = error.code
            results.append(item)
            if http.provider_not_before.get("cnyes", 0) > time.time():
                break
        for slug, evidence_url in list(candidates.items())[:128]:
            base = "https://api.cnyes.com/media/api/v1/newslist/category/" + slug
            item = {"kind": "category_candidate", "url": base, "slug": slug, "evidence_url": evidence_url}
            try:
                response = http.get(base + "?limit=30&page=1")
                item.update(status=response.status, response_sha256=hashlib.sha256(response.body).hexdigest())
                page, posts = cnyes_pagination(response)
                records, errors = cnyes_records(response, http, replace(registry.sources["cnyes"], cnyes_apis=(base,)))
                item.update(page=page, returned_rows=len(posts), validated_body_rows=len(records), item_errors=errors,
                            observed_article_ids=[p["newsId"] for p in posts], parser_accepted=True)
            except NewsError as error:
                item.update(error=error.code, parser_accepted=False)
            results.append(item)
            if http.provider_not_before.get("cnyes", 0) > time.time():
                break
        atomic_write_json(args.output, {"observed_at_utc": utc_now(), "probes": results,
            "performance": http.metrics.snapshot(), "all_categories_claimed": False, "all_history_complete": False})
        print(json.dumps({"output": str(args.output), "observed_candidates": len(candidates)}))
    finally:
        http.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

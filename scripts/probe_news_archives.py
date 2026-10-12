"""Finite source-backed layout and historical batch probes, not a crawler.

Candidates are actual archive links or already inventoried failed releases.
Never bypass a refusal; retain response hashes and explicit extraction outcomes.
"""
from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext import load_config
from downloader.news_fulltext_bulk import cnyes_pagination, cnyes_records, cnyes_url
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_queue import utc_now


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--url", action="append", default=[])
    parser.add_argument("--skip-category-probes", action="store_true")
    args = parser.parse_args(argv)
    config, registry, repo = load_config(Path("configs/news_fulltext.json"))
    http = NewsHTTP(registry, config, repo / config["root"])
    results = []
    urls = args.url or ["https://www.federalreserve.gov/boarddocs/press/general/1996/19961224/",
            "https://www.federalreserve.gov/boarddocs/press/boardacts/1996/19961226/",
            "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
            "https://www.federalreserve.gov/monetarypolicy/fomc_historical.htm",
            "https://www.federalreserve.gov/monetarypolicy/fomc_historical_year.htm",
            "https://www.federalreserve.gov/monetarypolicy/fomcminutes20260916.htm"]
    try:
        for url in urls:
            item = {"url": url}
            try:
                response = http.get(url)
                item.update(status=response.status, response_sha256=hashlib.sha256(response.body).hexdigest(), bytes=len(response.body))
                if response.status != 200:
                    results.append(item)
                    continue
                soup = BeautifulSoup(response.body, "lxml")
                item["layout"] = {"tables": [{"attrs": t.attrs, "parent": t.parent.name,
                                              "direct_paragraphs": len(t.select("p"))} for t in soup.select("table")][:12],
                                  "date_nodes": [str(n.parent)[:400] for n in soup.find_all(string=re.compile("Release Date:", re.I))][:3],
                                  "content_ids": [n.get("id") for n in soup.select("[id]")][:80]}
                if "/boarddocs/" in url:
                    item["layout"]["body_children"] = [{"tag": n.name, "attrs": n.attrs,
                        "text_prefix": n.get_text(" ", strip=True)[:100]} for n in soup.body.children if getattr(n, "name", None)][:60]
                links = [urljoin(response.url, a["href"]) for a in soup.select("a[href]")]
                item["official_archive_links"] = list(dict.fromkeys(u for u in links if urlsplit(u).hostname == "www.federalreserve.gov"
                    and ("fomc" in urlsplit(u).path.lower() or "/monetarypolicy/files/" in u)))[:100]
                if "/boarddocs/" in url:
                    try:
                        article = extract_article(response.body, registry.sources["fed_board"], page_url=response.url)
                        item["extraction"] = {k: article[k] for k in ("extraction_method", "published_at_raw")}
                        item["body_characters"] = len(article["body"])
                    except NewsError as error:
                        item["extraction_error"] = error.code
                elif "/fomcminutes" in url:
                    try:
                        article = extract_article(response.body, registry.sources["fed_board"], page_url=response.url)
                        item["extraction"] = {k: article[k] for k in ("extraction_method", "published_at_raw")}
                        item["body_characters"] = len(article["body"])
                    except NewsError as error:
                        item["extraction_error"] = error.code
            except NewsError as error:
                item["error"] = error.code
            results.append(item)
        base = "https://api.cnyes.com/media/api/v1/newslist/category/stock_report"
        source = replace(registry.sources["cnyes"], cnyes_apis=(base,))
        for year in (() if args.skip_category_probes else (2010, 2014, 2020, 2026)):
            start = int(datetime(year, 1, 1, tzinfo=timezone.utc).timestamp())
            end = int(datetime(year, 2, 1, tzinfo=timezone.utc).timestamp()) - 1
            url = cnyes_url(base, start=start, end=end)
            item = {"url": url, "kind": "stock_report_closed_history", "year": year}
            try:
                response = http.get(url)
                item.update(status=response.status, response_sha256=hashlib.sha256(response.body).hexdigest())
                raw = json.loads(response.body)
                fields = raw.get("items", {})
                item["pagination_fields"] = {k: fields.get(k) for k in ("total", "per_page", "current_page", "last_page")} if isinstance(fields, dict) else None
                item["data_type"] = type(fields.get("data")).__name__ if isinstance(fields, dict) else None
                page, posts = cnyes_pagination(response)
                records, errors = cnyes_records(response, http, source)
                item.update(page=page, returned_rows=len(posts), validated_body_rows=len(records), item_errors=errors)
                item["published_epoch_range"] = [min(p["publishAt"] for p in posts), max(p["publishAt"] for p in posts)] if posts else None
            except NewsError as error:
                item["error"] = error.code
            results.append(item)
        atomic_write_json(args.output, {"observed_at_utc": utc_now(), "probes": results, "performance": http.metrics.snapshot(),
                                      "all_history_complete": False})
        print(json.dumps({"output": str(args.output), "probes": len(results)}))
    finally:
        http.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

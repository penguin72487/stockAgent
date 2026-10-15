"""Finite Board PDF negotiation/resource probes under the shared publisher limit.

No user-agent rotation, cookies, source-denial bypass or second article queue.
Only hashes, declared response metadata and extraction counters are recorded.
"""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext import load_config
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_pdf import extract_board_pdf
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import utc_now


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    config, registry, repo = load_config(Path("configs/news_fulltext.json"))
    http = NewsHTTP(registry, config, repo / config["root"])
    results = []
    try:
        for filename in ("fomcminutes20260128.pdf", "FOMChistmin19360318.pdf", "FOMC20200129tealbooka20200117.pdf"):
            url = "https://www.federalreserve.gov/monetarypolicy/files/" + filename
            item = {"url": url, "request_accept": "application/pdf"}
            try:
                response = http.get(url, headers={"Accept": "application/pdf"})
                item.update(status=response.status, content_type=response.headers.get("content-type"),
                            bytes=len(response.body), response_sha256=hashlib.sha256(response.body).hexdigest())
                if response.status == 200:
                    article = extract_board_pdf(response.body, registry.sources["fed_board"], page_url=response.url)
                    item.update(pdf_pages=article["pdf_pages"], body_characters=len(article["body"]),
                                body_sha256=article["body_sha256"], published_at_raw=article["published_at_raw"],
                                extraction_method=article["extraction_method"])
            except NewsError as error:
                item["error"] = error.code
            results.append(item)
        atomic_write_json(args.output, {"observed_at_utc": utc_now(), "probes": results,
            "performance": http.metrics.snapshot(), "all_history_complete": False})
        print({"output": str(args.output), "probes": len(results)})
    finally:
        http.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

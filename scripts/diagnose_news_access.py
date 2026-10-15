"""Diagnose the actual canonical access path, not just its terminal retry code.

Pause the sole news service before running this bounded probe. Its existing
HTTP lock, source cooldowns, honest identity, DNS/robots/redirect checks and
pacing remain in force. No proxies, cookies, alternate identities or denial
bodies are used. The private report contains headers/hashes/structure only;
successful extraction is not a durable article receipt or history acceptance.
"""

from __future__ import annotations

import argparse
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import re
import threading
import time
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_http import ACCESS_DIAGNOSTIC_HEADERS, NewsHTTP, Response
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_queue import utc_now


HEADER_NAMES = ACCESS_DIAGNOSTIC_HEADERS
ROBOTS_LINE = re.compile(r"(?im)^[ \t]*(?:user-agent|allow|disallow|crawl-delay|request-rate|sitemap)[ \t]*:[^\r\n]*")


class AccessTrace:
    """At most 32 response summaries per explicitly selected provider."""

    def __init__(self, registry):
        self.registry = registry
        self.lock = threading.Lock()
        self.rows = defaultdict(lambda: deque(maxlen=32))

    def observe(self, phase: str, response: Response):
        provider = self.registry.require(response.url).provider
        with self.lock:
            if phase == "headers":
                self.rows[provider].append({
                    "requested_url": response.url, "status": response.status,
                    "headers": {k: v[:2048] for k, v in response.headers.items() if k in HEADER_NAMES},
                    "observed_at_utc": utc_now(), "body_read": False,
                })
                return
            if phase != "complete" or not self.rows[provider]:
                raise ValueError("unpaired access trace phase")
            row = self.rows[provider][-1]
            if row["requested_url"] != response.url or row["status"] != response.status:
                raise ValueError("response trace identity mismatch")
            row.update(body_read=True, response_bytes=len(response.body),
                       response_sha256=hashlib.sha256(response.body).hexdigest())
            # Only policy/index responses need another parse. Ordinary article
            # extraction below already parses its body; never duplicate it here.
            parts = urlsplit(response.url)
            if parts.path not in {"/", "/robots.txt"}:
                return
            text = response.body.decode("utf-8-sig", "replace")
            row["robots_directive_lines"] = ROBOTS_LINE.findall(text)[:128]
            if response.status != 200 or "html" not in response.headers.get("content-type", ""):
                return
            soup = BeautifulSoup(response.body, "lxml")
            title = soup.title.get_text(" ", strip=True)[:160] if soup.title else ""
            canonical = soup.select_one('link[rel~="canonical"][href]')
            source = self.registry.sources[provider]
            pattern = re.compile(source.article_path_pattern) if source.article_path_pattern else None
            article_links, malformed_links = 0, 0
            for a in soup.select('a[href]'):
                try:
                    target = urlsplit(a.get("href", ""))
                except ValueError:
                    malformed_links += 1
                    continue
                if pattern and pattern.search(target.path):
                    article_links += 1
            row["html_structure"] = {
                "title": title, "canonical_url": canonical.get("href") if canonical else None,
                "known_article_links": article_links, "h1_count": len(soup.select("h1")),
                "malformed_links": malformed_links,
                "challenge_title": any(marker in title.lower() for marker in
                    ("just a moment", "access denied", "captcha", "verify you are human")),
                "challenge_elements": bool(soup.select_one('#challenge-form, #cf-challenge-running, [data-sitekey]')),
            }

    def snapshot(self, provider):
        with self.lock:
            return list(self.rows[provider])


def diagnose_provider(http, trace, urls, *, resolve_redirect_only=False):
    observations = []
    for url in urls:
        source = http.registry.require(url)
        start = time.monotonic()
        item = {"provider": source.provider, "url": url, "article_durably_accepted": False}
        if http.provider_not_before.get(source.provider, 0) > time.time():
            item.update(error="persisted_provider_cooldown", sent=False,
                        not_before_epoch=http.provider_not_before[source.provider])
            observations.append(item)
            break
        try:
            response = http.get(url, follow_redirects=False) if resolve_redirect_only else http.get(url)
            item.update(status=response.status, final_url=response.url,
                        response_bytes=len(response.body),
                        response_sha256=hashlib.sha256(response.body).hexdigest())
            if resolve_redirect_only:
                item.update(redirect_location=response.headers.get("location"), destination_requested=False,
                            redirected_article_identity_verified=False)
            elif response.status == 200 and "html" in response.headers.get("content-type", ""):
                try:
                    article = extract_article(response.body, source, page_url=response.url)
                    item["parser_result"] = {k: article.get(k) for k in
                        ("body_sha256", "body_completeness", "published_at_raw", "authors_json", "extraction_method")}
                except NewsError as error:
                    item["parser_error"] = error.code
                    if error.code == "access_challenge":
                        http.defer(source.provider, 900)
        except NewsError as error:
            item.update(error=error.code, retry=error.retry, delay=error.delay)
            if error.__cause__:
                item["cause_type"] = type(error.__cause__).__name__
        item["wall_seconds"] = time.monotonic() - start
        observations.append(item)
        if http.provider_not_before.get(source.provider, 0) > time.time():
            break
    provider = http.registry.require(urls[0]).provider
    return {"provider": provider, "probes": observations, "http_path": trace.snapshot(provider)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--probe", action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--resolve-redirect-only", action="store_true",
                        help="Inspect a reviewed public link's response headers; never follow its destination")
    args = parser.parse_args(argv)
    if len(args.probe) > 8:
        parser.error("at most eight explicitly selected access probes")
    config, registry, repo = load_config(args.config)
    groups = defaultdict(list)
    for url in args.probe:
        groups[registry.require(url).provider].append(url)
    trace = AccessTrace(registry)
    http = NewsHTTP(registry, config, repo / config["root"], access_observer=trace.observe)
    try:
        with ThreadPoolExecutor(max_workers=min(len(groups), config["workers"])) as pool:
            providers = list(pool.map(lambda urls: diagnose_provider(http, trace, urls,
                resolve_redirect_only=args.resolve_redirect_only), groups.values()))
        result = {
            "contract_version": 1, "observed_at_utc": utc_now(), "diagnosis_completed": True,
            "providers": providers, "performance": http.metrics.snapshot(),
            "exclusive_canonical_http_path": True, "cooldowns_preserved": True,
            "diagnostic_only": True, "article_receipts_created": 0,
            "redirect_headers_only": args.resolve_redirect_only,
            "all_sites_downloaded": False, "publish": False, "training_injection_enabled": False,
            "implementation_sha256": {str(p): sha256_file(p) for p in (
                Path(__file__), repo / "downloader/news_fulltext_http.py")},
        }
        args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        atomic_write_json(args.output, result)
        print(json.dumps({k: v for k, v in result.items() if k not in {"performance", "implementation_sha256"}}, ensure_ascii=False))
        return 0
    finally:
        http.close()


if __name__ == "__main__":
    raise SystemExit(main())

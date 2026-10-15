"""Finite registered Anue history/date-precision probes, not a crawler.

This tests useful history/bounds, not a lowest publication date or completeness.
Requests are serialized through the canonical publisher bucket and refusals
stop the experiment; no guessed article IDs or restricted bodies are stored.
An explicit plan retains only ID/clock/payment metadata, not raw bodies.  It
must run after the sole normal HTTP owner has drained; the same owner lock,
robots, provider/host refusals and limiter remain authoritative.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
from urllib.parse import parse_qs, urlsplit

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_bulk import cnyes_pagination, cnyes_records, cnyes_url, wordpress_base
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import utc_now


MAX_EXPLICIT_QUERIES = 40


def validated_queries(payload, source, *, now=None):
    """Only reviewed endpoints and canonical existing parameters reach HTTP."""
    now = time.time() if now is None else now
    if (not isinstance(payload, dict) or set(payload) != {'contract_version', 'provider', 'queries'}
            or type(payload['contract_version']) is not int or payload['contract_version'] != 1
            or payload['provider'] != 'cnyes' or source.provider != 'cnyes'
            or not isinstance(payload['queries'], list) or not 1 <= len(payload['queries']) <= MAX_EXPLICIT_QUERIES):
        raise ValueError('finite_registered_Anue_query_plan_required')
    result, seen = [], set()
    for item in payload['queries']:
        if (not isinstance(item, dict) or set(item) != {'url', 'purpose'}
                or not isinstance(item['url'], str) or not isinstance(item['purpose'], str)
                or not 1 <= len(item['purpose']) <= 160):
            raise ValueError('explicit_url_and_bounded_purpose_required')
        url = item['url']
        base = wordpress_base(url)
        query = parse_qs(urlsplit(url).query, strict_parsing=True)
        if (base not in source.cnyes_apis or set(query) != {'limit', 'page', 'startAt', 'endAt'}
                or any(len(value) != 1 for value in query.values())
                or query['limit'] != ['30'] or query['page'] != ['1']):
            raise ValueError('exact_registered_first_page_and_existing_date_parameters_required')
        low, high = int(query['startAt'][0]), int(query['endAt'][0])
        if not 0 <= low <= high <= now or high - low + 1 > 730 * 86400:
            raise ValueError('observed_two_year_bound_and_no_future_query_required')
        if url != cnyes_url(base, start=low, end=high) or url in seen:
            raise ValueError('canonical_unique_query_required')
        seen.add(url)
        result.append(dict(item))
    return result


def probe_query(http, source, query, *, validate_bodies=False):
    """A real page result is diagnostic metadata, never article acceptance."""
    url = query['url']
    item = {**query, 'category': wordpress_base(url).rsplit('/', 1)[-1], 'observed_at_utc': utc_now()}
    started = time.monotonic()
    if http.provider_not_before.get(source.provider, 0) > time.time():
        return {**item, 'error': 'persisted_provider_cooldown', 'sent': False,
                'not_before_epoch': http.provider_not_before[source.provider], 'parser_accepted': False}
    try:
        response = http.get(url)
        item.update(status=response.status, response_sha256=hashlib.sha256(response.body).hexdigest(),
                    response_bytes=len(response.body), sent=True)
        payload = json.loads(response.body)
        if response.status != 200:
            item['error_response_meta'] = {key: payload.get(key) for key in ('statusCode', 'message', 'errors')} if isinstance(payload, dict) else None
        page, posts = cnyes_pagination(response)
        item.update(page=page, returned_rows=len(posts),
                    publisher_pagination_fields={key: payload['items'].get(key) for key in
                        ('total', 'per_page', 'current_page', 'last_page', 'from', 'to', 'next_page_url', 'prev_page_url')},
                    source_id_clock_payment_metadata=[{key: post.get(key) for key in ('newsId', 'publishAt', 'payment')} for post in posts],
                    published_epoch_range=[min(p['publishAt'] for p in posts), max(p['publishAt'] for p in posts)] if posts else None,
                    parser_accepted=True, body_validation_performed=validate_bodies,
                    raw_response_retained=False, diagnostic_only=True, article_receipts_created=0)
        low, high = (int(parse_qs(urlsplit(url).query)[key][0]) for key in ('startAt', 'endAt'))
        item['rows_before_request_start'] = sum(post['publishAt'] < low for post in posts)
        item['rows_after_request_end'] = sum(post['publishAt'] > high for post in posts)
        if validate_bodies:
            records, errors = cnyes_records(response, http, source)
            item.update(validated_body_rows=len(records), item_errors=errors)
    except NewsError as error:
        item.update(error=error.code, retry=error.retry, delay=error.delay, parser_accepted=False)
    except (ValueError, TypeError, KeyError) as error:
        item.update(error='invalid_source_query_response', cause_type=type(error).__name__, parser_accepted=False)
    item['wall_seconds'] = time.monotonic() - started
    item['completed_at_utc'] = utc_now()
    return item


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--start", default="1999-01-01")
    parser.add_argument("--end", default="2009-12-31")
    parser.add_argument("--max-categories", type=int, default=3)
    parser.add_argument('--query-plan', type=Path,
                        help='At most 40 canonical registered first-page date queries; metadata only')
    args = parser.parse_args(argv)
    config, registry, repo = load_config(Path("configs/news_fulltext.json"))
    source = registry.sources['cnyes']
    try:
        if args.query_plan:
            queries = validated_queries(json.loads(args.query_plan.read_bytes()), source)
        else:
            start = int(datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
            end = int(datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) + 86399
            if not 1 <= args.max_categories <= 128 or not 0 <= start <= end <= time.time():
                raise ValueError('invalid finite category/date bounds')
            queries = [{'url': cnyes_url(base, start=start, end=end), 'purpose': 'legacy_broad_closed_history_control'}
                       for base in source.cnyes_apis[:args.max_categories]]
    except (ValueError, TypeError, KeyError, OSError) as error:
        parser.error(str(error))
    # Invalid input must not acquire the shared owner or leak its lock/session.
    http = NewsHTTP(registry, config, repo / config["root"])
    results = []
    try:
        for query in queries:
            item = probe_query(http, source, query, validate_bodies=not bool(args.query_plan))
            results.append(item)
            if (item.get('status') in {400, 403, 422, 429, 503} or item.get('error')
                    or http.provider_not_before.get('cnyes', 0) > time.time()):
                break
        atomic_write_json(args.output, {"observed_at_utc": utc_now(), "probes": results,
            "performance": http.metrics.snapshot(), "all_history_complete": False,
            'planned_queries': len(queries), 'queries_observed': len(results), 'all_planned_queries_observed': len(results) == len(queries),
            'query_plan_sha256': sha256_file(args.query_plan) if args.query_plan else None,
            'query_plan_path': str(args.query_plan) if args.query_plan else None,
            'implementation_sha256': sha256_file(Path(__file__)), 'exclusive_canonical_http_path': True,
            'source_deadlines_reset': False, 'production_queue_writes': False, 'article_receipts_created': 0,
            'diagnostic_only': True, 'raw_bodies_in_report': False, 'publish': False})
        print({"output": str(args.output), "probes": len(results)})
    finally:
        http.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

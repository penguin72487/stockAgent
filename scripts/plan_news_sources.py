"""Plan every observed news source from an immutable read-only inventory.

No publisher requests or queue writes. The small-cohort threshold chooses an
implementation strategy, never permission or a claim of publisher coverage.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
import io
import json
from pathlib import Path
import sys

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from downloader.artifact_io import atomic_write_json, atomic_write_text, sha256_file
from stockagent.data.news_source_catalog import classify_news_outcomes, NEWS_OUTCOME_CLASSIFICATION_VERSION


FINANCIAL_PROVIDERS = frozenset({
    "cnyes", "moneydj", "money_link", "technews", "finlab", "uanalyze",
    "genetinfo", "gbimonthly", "businesstoday", "wealth", "moneyweekly",
    "businessweekly", "bnext", "abmedia", "sinotrade_research", "ltn",
    "cmoney", "cmnews", "udn", "ettoday", "cnews", "housefun",
})

SOURCE_TOPICS = {
    "abmedia": "crypto_market", "genetinfo": "biomed_industry", "gbimonthly": "biomed_industry",
    "technews": "technology_supply_chain", "bnext": "technology_business", "einfo": "environment_energy",
    "sinotrade_research": "equity_research", "fed_board": "monetary_policy",
    "inside": "technology_business", "pansci": "science_energy_health",
    "unregistered:www.inside.com.tw": "technology_business", "unregistered:pansci.asia": "science_energy_health",
    "unregistered:buzzorange.com": "technology_business", "unregistered:www.wantgoo.com": "financial_markets",
    "unregistered:news.wearn.com": "financial_markets", "unregistered:www.investor.com.tw": "financial_markets",
    "unregistered:www.businessinsider.tw": "technology_business",
}

ERROR_REPAIRS = {
    "source_access_denied": "preserve provider refusal deadline; verify access policy, never rotate identity",
    "robots_not_plain_rules": "trace source redirect and representation; parse valid REP lines including HTML wrappers; verify registered ordinary home fallback, never treat a login/challenge or arbitrary error page as an allow-all policy",
    "robots_access_challenge": "preserve actual challenge and publisher deadline; verify a documented source-supported public endpoint, no identity/cookie/proxy bypass",
    "robots_disallowed": "do not fetch denied namespace; request authorization or use permitted original source",
    "body_not_found": "verify source article container or declared full-content field; never promote summary",
    "article_attribution_not_verified": "scope source byline to current article; preserve absence until verified",
    "article_license_not_verified": "verify current article license, not related-story or global-footer marker",
    "paywall_or_login": "public content only; no login/session or hidden protected body extraction",
    "http_429": "honor Retry-After and provider budget; independent healthy sources continue",
    "http_404": "record actual source absence, no invented replacement article",
    "source_not_found": "record explicit HTTP or reviewed source soft-404 absence; never substitute page boilerplate",
    "source_body_image_only": "image-only source body requires separate media/OCR scope; no headline promoted to full text",
    "source_body_embedded_only": "verified external-only representation with no native article text; preserve source identity and clocks, do not execute/fetch external widgets or infer their availability/rights",
    "source_body_quote_only": "verified native ticker/price stub, not authored news prose; preserve raw evidence and metadata, do not invent narrative or endlessly retry a parser",
    "source_article_empty_template": "preserve current empty headline/body template and source canonical ID; seek an authorized actual original, never invent text/date or treat HTTP200 as completion",
    "redirect_not_article": "verify actual same-provider destination namespace and article identity before targeted migration",
    "unsupported_article_path": "verify legitimate article namespace, requeue only scoped path failures after changed capture contract",
    "article_identity_not_verified": "validate source article ID and canonical/embedded URL; never merge infinite-scroll articles",
}


def implementation_plan(item, provider, entry, review):
    """One blueprint for every key, not permission inferred from hostname.

    Endpoint URLs come only from existing declared configuration. Unknown
    sites use the same adapter family after review, not guessed API paths.
    """
    endpoints = {kind: list(entry.get(key, [])) for kind, key in (
        ("bulk_json", "cnyes_apis"), ("wordpress_json", "wordpress_apis"),
        ("fulltext_feed", "fulltext_feeds"), ("feed", "feeds"),
        ("sitemap", "sitemaps"), ("index", "index_pages")) if entry.get(key)}
    if entry.get('cnyes_public_index') is not None:
        from downloader.news_fulltext_cnyes_index import reviewed_index
        endpoint, _ = reviewed_index(entry, provider, entry.get('authorization', {}))
        endpoints['public_metadata_index'] = [endpoint]
    family = ("declared_bulk_json" if entry.get("cnyes_apis") else
              "declared_wordpress_full_content" if entry.get("wordpress_apis") else
              "declared_fulltext_feed" if entry.get("fulltext_feeds") else "source_scoped_html")
    role = item["source_role"]
    if role in {"redirector", "aggregator"}:
        action, family = "resolve_original_url_without_duplicate_body_pipeline", "original_publisher_identity"
    elif role == "social_or_video":
        action, family = "supported_export_or_original_text_link", "platform_supported_access"
    elif provider == "appledaily_archive":
        action, family = "locate_authorized_actual_archive", "unavailable_archive"
    elif item.get("enabled"):
        action = "repair_existing_pipeline" if item.get("errors") else "reuse_existing_pipeline"
    elif review.get("capture_decision") == "authorized_api_configuration_required":
        action, family = "configure_and_verify_authorized_api_contract", "authorized_provider_api"
    elif review.get("capture_decision") == "permission_required":
        action = "permission_required_before_automatic_body_capture"
    else:
        action = "review_then_extend_shared_source_adapter"
    errors = sorted({error["error_code"] for error in item.get("errors", []) if error.get("error_code")})
    repairs = [ERROR_REPAIRS.get(error, "preserve source outcome; classify representation, schema and per-item failures")
               for error in errors]
    return {
        "outcome_progress_json": json.dumps(classify_news_outcomes(item['states'],
            registered=item.get('configured') is True, body_enabled=item.get('enabled') is True)
            if isinstance(item.get('states'), dict) else None, ensure_ascii=False, sort_keys=True),
        "research_topic": SOURCE_TOPICS.get(provider, "financial_markets" if provider in FINANCIAL_PROVIDERS
                                             else "public_sector_unclassified" if role == "government_origin" else "general_events"),
        "value_basis": "topic relevance heuristic; not measured predictive alpha; no source excluded by ranking",
        "implementation_action": action, "collector_family": family,
        "known_endpoints_json": json.dumps(endpoints, ensure_ascii=False, sort_keys=True),
        "endpoint_listing_contract_version": 2,
        "article_path_pattern": entry.get("article_path_pattern", ""),
        "body_selectors_json": json.dumps(entry.get("article_selectors", []), ensure_ascii=False),
        "requires_separate_service": False,
        "source_policy_review": review.get("capture_decision", "active_existing_policy" if item.get("enabled") else "not_reviewed"),
        "review_evidence_urls_json": json.dumps(review.get("evidence_urls", []), ensure_ascii=False),
        "source_policy_observation": review.get("observation", "No new access permission inferred"),
        "reviewed_at_utc": review.get("reviewed_at_utc"),
        "observed_error_codes_json": json.dumps(errors),
        "remediation_actions_json": json.dumps(list(dict.fromkeys(repairs)), ensure_ascii=False),
        "cpu_strategy": "one body parse; indexed URL dedup and dispatch; receipt-bounded batch writes; no completed finite rescan",
        "http_strategy": "verified bulk full-content first, source-scoped HTML fallback; independent bounded source pacing",
        "body_acceptance": "article identity + exact body + attribution/clock provenance + source policy + raw/body/file SHA",
        "refresh_strategy": "existing continuous schedule" if item.get("enabled") and item.get("acquisition_mode") != "finite"
                            else "freeze known missing URL scope; new URLs separate until refresh decision",
        "source_maximum_rate_verified": False,
    }


def apply_work_order(rows, policy):
    """Explicit engineering order, not a publisher quota or HTTP permission.

    Keep the legacy topic/stage fields as source facts. The selected user order
    overrides them for implementation, without stopping independent collectors
    or inflating URL counts with redirect associations. CNA and any other
    explicitly deferred sources remain inventoried, never silently omitted.
    """
    required = {'contract_version', 'focus_provider', 'provider_order',
                'deferred_providers', 'retain_running_collectors'}
    parallel_fields = {'background_provider_order', 'background_execution', 'focus_preemption'}
    if not isinstance(policy, dict) or type(policy.get('contract_version')) is not int:
        raise ValueError('explicit complete work-order policy required')
    version = policy['contract_version']
    if version not in {1, 2} or set(policy) != (required | parallel_fields if version == 2 else required):
        raise ValueError('explicit complete work-order policy required')
    focus, deferred = policy['focus_provider'], policy['deferred_providers']
    if (not isinstance(focus, str) or not focus
            or policy['provider_order'] != 'known_urls_desc'
            or not isinstance(deferred, list) or any(not isinstance(p, str) or not p for p in deferred)
            or len(set(deferred)) != len(deferred) or focus in deferred
            or policy['retain_running_collectors'] is not True):
        raise ValueError('invalid selected work-order policy')
    if focus not in {row['provider'] for row in rows}:
        raise ValueError('focus provider absent from complete inventory')
    background = policy.get('background_provider_order', [])
    if version == 2:
        providers = {row['provider'] for row in rows}
        if (not isinstance(background, list) or not background
                or any(not isinstance(p, str) or not p or p not in providers for p in background)
                or len(set(background)) != len(background)
                or focus in background or set(background).intersection(deferred)
                or policy['background_execution'] != 'parallel_while_focus_waits'
                or policy['focus_preemption'] != 'return_on_verified_operational_blocker'):
            raise ValueError('invalid selected background work-order policy')
    background_ranks = {provider: rank for rank, provider in enumerate(background)}
    for row in rows:
        provider = row['provider']
        if version == 2:
            row['work_phase'] = (4 if provider in deferred else 0 if provider == focus
                                 else 1 if provider in background_ranks
                                 else 2 if row['known_urls'] or row['literal_metadata_links'] else 3)
            # This is engineering attention, not a global HTTP gate. Running
            # collectors stay independently paced, even while focus is healthy.
            row['engineering_execution'] = ('deferred_by_user' if provider in deferred else
                'priority_focus' if provider == focus else
                'parallel_while_focus_waits' if provider in background_ranks else
                'after_selected_background_order')
            row['background_attention_rank'] = (background_ranks[provider] + 1
                                                 if provider in background_ranks else None)
        else:
            row['work_phase'] = (3 if provider in deferred else 0 if provider == focus
                                 else 1 if row['known_urls'] or row['literal_metadata_links'] else 2)
        row['implementation_state'] = (
            'deferred_by_user' if provider in deferred else
            'repair_or_backfill_known_urls' if row['missing_body_urls'] else
            'review_literal_original_links' if row['unqueued_literal_link_work'] else
            'maintenance_only' if row['known_urls'] else 'review_discovery_after_known_urls')
        row['implementation_order_basis'] = ('selected_focus_with_ordered_parallel_background'
            if version == 2 else 'selected_focus_first_then_known_url_count_descending')
    rows.sort(key=lambda row: (row['work_phase'], background_ranks.get(row['provider'], 0),
                              -row['known_urls'], row['provider']))
    work = [row for row in rows if row['implementation_state'] not in {'deferred_by_user', 'maintenance_only'}]
    ranks = {row['provider']: rank for rank, row in enumerate(work, 1)}
    for row in rows:
        row['implementation_rank'] = ranks.get(row['provider'])
        row['body_downloader_candidate'] = (
            row['implementation_state'] == 'repair_or_backfill_known_urls'
            and row['source_role'] not in {'redirector', 'aggregator', 'social_or_video'})
    parallel = ({
        'background_attention_order': list(background),
        'background_waits_for_focus_completion': False,
        'focus_preemption': policy['focus_preemption'],
        'dispatch_scope': 'engineering_only_existing_collectors_remain_independent',
        'background_access_is_not_granted_by_priority': True,
    } if version == 2 else {})
    return {
        'work_order_contract_version': version, 'selected_work_order': policy, **parallel,
        'implementation_order': [row['provider'] for row in work],
        'body_downloader_implementation_order': [row['provider'] for row in work if row['body_downloader_candidate']],
        'deferred_source_keys': sorted(row['provider'] for row in rows if row['provider'] in deferred),
        'running_collectors_stopped': False, 'publisher_requests': 0, 'queue_writes': False,
        'work_order_grants_source_access': False,
    }


def plan_sources(inventory: dict, *, small_threshold: int = 300, entries=None, reviews=None,
                 work_order=None) -> dict:
    if type(small_threshold) is not int or not 1 <= small_threshold <= 10000:
        raise ValueError("small cohort threshold must be 1..10000")
    entries, reviews = entries or {}, reviews or {}
    rows = []
    for provider, item in inventory["providers"].items():
        known, stored = item.get("known_urls", 0), item.get("stored_bodies", 0)
        if type(known) is not int or type(stored) is not int or not 0 <= stored <= known:
            raise ValueError("invalid inventory URL/body counts")
        original_metadata = item.get("original_metadata", {})
        if not isinstance(original_metadata, dict):
            raise ValueError("invalid original metadata link counts")
        literal_links = original_metadata.get("literal_metadata_links", 0)
        if type(literal_links) is not int or literal_links < 0:
            raise ValueError("invalid original metadata link counts")
        role = item["source_role"]
        # A .gov hostname proves neither monetary/economic relevance nor an
        # open license. Prioritize known monetary releases, then financial and
        # industry sources; keep unknown public-sector content for review.
        priority = 1 if SOURCE_TOPICS.get(provider) == "monetary_policy" else 2 if (provider in FINANCIAL_PROVIDERS
                    or SOURCE_TOPICS.get(provider) in {'technology_business', 'technology_supply_chain', 'biomed_industry',
                                                      'equity_research', 'financial_markets', 'crypto_market'}) else 3
        basis = ("known_monetary_policy_origin_not_blanket_reuse_permission" if priority == 1 else
                 "financial_or_industry_source_heuristic_not_measured_article_value" if priority == 2 else
                 "official_domain_only_relevance_unverified" if role == "government_origin" else
                 "other_public_news_pending_article_value_review")
        small = 0 < known <= small_threshold
        small_metadata = known == 0 and 0 < literal_links <= small_threshold
        if role in {"redirector", "aggregator"}:
            strategy, stage = "resolve_original_publisher_and_deduplicate", 4
            gate = "syndication identity and original rights; no aggregator body fallback"
        elif role == "social_or_video":
            strategy, stage = "platform_supported_export_or_public_original_link", 5
            gate = "platform terms and API scope; no video-to-text or login bypass"
        elif provider == "appledaily_archive":
            strategy, stage = "closed_source_archive_and_rights_review", 5
            gate = "actual authorized archive; absence is not a transport retry"
        elif item.get("enabled"):
            strategy, stage = "finite_existing_metadata" if item.get("acquisition_mode") == "finite" else "reuse_active_canonical_collector", 0
            gate = "preserve source/robots cooldowns and body acceptance receipts"
        elif reviews.get(provider, {}).get("capture_decision") == "authorized_api_configuration_required":
            strategy, stage = "authorized_api_configuration_and_capability_check", 1
            gate = "existing user authorization accepted; actual licensed API product/key, tool schema, history and quota need verification"
        elif known == 0 and literal_links:
            # A literal URL recovered from local metadata is reviewable work,
            # not an empty source. These are alias associations, however, not
            # distinct articles, publication proof or an authorized frontier.
            # Reuse the canonical resolver/router; never add them twice or
            # invent a request count before deduplication and source review.
            strategy, stage = "review_then_route_existing_literal_original_links", 1 if small_metadata else 2
            gate = "source policy, literal-target dedup and native identity/time/byline; canonical original router, no duplicate collector"
        elif known == 0:
            strategy, stage = "no_local_urls_review_discovery_only_if_useful", 3
            gate = "source policy and actual history endpoint; do not guess article IDs"
        elif small:
            strategy, stage = "review_then_finite_existing_metadata", 1
            gate = "review terms, robots, bounded article namespace, exact body/time/byline"
        else:
            strategy, stage = "review_then_shared_incremental_and_history_collector", 2
            gate = "same review plus verified feed/API/index history and shared source budget"
        rows.append({
            "provider": provider, "name": item.get("name", provider), "source_role": role,
            "stage": stage, "priority_tier": priority, "priority_basis": basis,
            "strategy": strategy, "small_cohort_candidate": small,
            "small_metadata_link_candidate": small_metadata,
            "configured": item.get("configured", False), "enabled": item.get("enabled", False),
            "known_urls": known, "stored_bodies": stored, "missing_body_urls": known - stored,
            "literal_metadata_links": literal_links,
            "literal_metadata_link_grain": "local original-resolution alias associations, not unique articles or queued heads",
            "unqueued_literal_link_work": known == 0 and literal_links > 0,
            "first_metadata_day": item.get("earliest_metadata_day"),
            "last_metadata_day": item.get("latest_metadata_day"),
            "metadata_dates_are_publisher_clocks": False,
            "source_evidence_url": item.get("authorization", {}).get("evidence_url"),
            "required_before_requests": gate,
            "minimum_html_article_requests_if_no_bulk": None if known == 0 and literal_links else known - stored,
            "bulk_request_reduction_verified": provider in {"cnyes", "globalvoices"},
            "finish_eta_utc": None,
            "eta_basis": "unknown until accepted-body throughput and source barriers are measured",
            "dedicated_refresh_service": False,
            "growth_action": "new metadata URLs visible; review cohort growth before adding refresh",
            "all_publisher_history_complete": False,
            **implementation_plan(item, provider, entries.get(provider, {}), reviews.get(provider, {})),
        })
    rows.sort(key=lambda r: (r["stage"], r["priority_tier"], r["known_urls"], r["provider"]))
    counts = Counter(row["strategy"] for row in rows)
    ordering = apply_work_order(rows, work_order) if work_order is not None else {}
    return {
        "contract_version": (7 if work_order is not None and work_order['contract_version'] == 2
                             else 6 if work_order is not None else 5),
        "news_outcome_classification_contract_version": NEWS_OUTCOME_CLASSIFICATION_VERSION,
        **ordering, "input_observed_at_utc": inventory["observed_at_utc"],
        "queue_owner_id": inventory.get("queue_owner_id"),
        "small_threshold_urls": small_threshold,
        "threshold_basis": "planning heuristic only; not source quota, permission or history cap",
        "grain": "same provider/hostname keys as inventory, not unique publishers",
        "sources": rows, "source_count": len(rows), "strategy_counts": dict(counts),
        "known_urls": sum(row["known_urls"] for row in rows),
        "stored_bodies": sum(row["stored_bodies"] for row in rows),
        "literal_metadata_links": sum(row["literal_metadata_links"] for row in rows),
        "unqueued_literal_link_source_keys": sum(row["unqueued_literal_link_work"] for row in rows),
        "plan_is_acquisition_acceptance": False, "all_history_complete": False,
        "source_policy_review_counts": dict(Counter(row["source_policy_review"] for row in rows)),
        "implementation_action_counts": dict(Counter(row["implementation_action"] for row in rows)),
        "all_source_keys_planned": len(rows) == len(inventory["providers"]),
        "all_sites_live_reviewed": all(row["source_policy_review"] != "not_reviewed" for row in rows),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--small-threshold", type=int, default=300)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--config", type=Path, help="Existing canonical source recipes, no publisher requests")
    parser.add_argument("--reviews", type=Path, help="Explicit primary-source review observations, never runtime authorization")
    parser.add_argument("--work-order", type=Path, help="Selected engineering focus and optional parallel background order; no HTTP or queue mutation")
    args = parser.parse_args(argv)
    before = sha256_file(args.inventory)
    entries, reviews, inputs = {}, {}, {}
    if args.config:
        from stockagent.data.news_source_catalog import load_news_configuration, configuration_paths
        config = load_news_configuration(args.config)
        entries = {entry["id"]: entry for entry in config["providers"]}
        inputs.update({str(path): sha256_file(path) for path in configuration_paths(args.config)})
    if args.reviews:
        from stockagent.data.news_source_catalog import load_source_review_ledger
        inputs[str(args.reviews)] = sha256_file(args.reviews)
        reviews = load_source_review_ledger(args.reviews)
    work_order = None
    if args.work_order:
        inputs[str(args.work_order)] = sha256_file(args.work_order)
        work_order = json.loads(args.work_order.read_bytes())
    result = plan_sources(json.loads(args.inventory.read_text()), small_threshold=args.small_threshold,
                          entries=entries, reviews=reviews, work_order=work_order)
    if sha256_file(args.inventory) != before:
        raise ValueError("inventory changed during planning")
    if any(sha256_file(Path(path)) != digest for path, digest in inputs.items()):
        raise ValueError("source recipes or review ledger changed during planning")
    result["input_sha256"] = before
    result["planning_inputs_sha256"] = inputs
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=list(result["sources"][0]) if result["sources"] else ["provider"])
    writer.writeheader()
    writer.writerows(result["sources"])
    atomic_write_text(args.output.with_suffix(".csv"), buffer.getvalue(), durable=True)
    print(json.dumps({k: result[k] for k in ("source_count", "strategy_counts", "known_urls", "stored_bodies")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

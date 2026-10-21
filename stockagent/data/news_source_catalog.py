"""Pure local news-source configuration and inventory classification.

No collector, SQLite writer, publisher calls or permission inference. Candidate
adapters remain disabled until their own reviewed capture policy is supplied.
"""
from __future__ import annotations

import json
from pathlib import Path


SOURCE_REVIEW_DECISIONS = frozenset({
    "private_public_candidate", "permission_required", "reviewed_existing_scope",
    "review_incomplete", "authorized_api_configuration_required",
})

# A queue outcome describes an observed URL head, not every possible archive
# or original publisher. In particular, 404, a denied endpoint and a missing
# parser are three different things. Keep this contract shared by the planner
# and the read-only page; it never changes an article state or retries.
NEWS_OUTCOME_CLASSIFICATION_VERSION = 3
NEWS_OUTCOME_GROUPS = {
    "queued_or_retry": frozenset({"pending", "inflight", "retry", "retry_cooldown"}),
    "waiting_cohort": frozenset({"waiting_cohort"}),
    "scope_or_access_waiting": frozenset({
        "awaiting_authorization", "source_not_allowed", "authorization_expired",
        "article_license_not_verified", "article_rights_not_verified",
        "article_rights_or_scope_excluded", "robots_disallowed",
        "paywall", "paywall_or_login", "access_challenge", "robots_access_challenge",
    }),
    "repair_required": frozenset({
        "unsupported_article_path", "redirect_not_article", "body_not_found", "article_attribution_not_verified",
        "article_title_not_verified", "article_identity_not_verified", "response_too_large",
        "robots_not_plain_rules", "robots_redirect_refused",
        "invalid_bulk_publication_clock",
    }),
    "nontext_representation": frozenset({
        "source_body_image_only", "source_body_embedded_only", "pdf_image_only_or_body_not_found",
    }),
    "nonarticle_response": frozenset({
        "source_body_quote_only", "source_article_test_only", "source_document_landing_page",
    }),
    "source_currently_empty": frozenset({"source_article_empty_template"}),
    "origin_absent": frozenset({"not_found", "gone", "source_body_removed", "source_not_found"}),
}
_NEWS_OUTCOME_GROUP_BY_STATE = {
    state: group for group, states in NEWS_OUTCOME_GROUPS.items() for state in states
}
# Public allowlisting and outcome grouping must use the same finite vocabulary.
# Unknown/private error names still stay out of the public snapshot; adding a
# known validation outcome must not silently turn it into an unknown error.
NEWS_OUTCOME_STATES = frozenset({"complete"}).union(*NEWS_OUTCOME_GROUPS.values())


def classify_news_outcomes(states: dict, *, registered: bool, body_enabled: bool) -> dict:
    """One bounded, mutually exclusive classification of retained queue counts.

    The complete *state* remains distinct from stored-body counters: an old
    body can still exist during a recheck or after removal. No current queue
    code certifies exhaustion of all authorized alternatives, so leave final
    unavailability/closure unknown instead of inventing a completed workload.
    Unknown outcomes remain visible and actionable, never silently dropped.
    """
    if (not isinstance(states, dict) or any(not isinstance(state, str)
            or type(count) is not int or count < 0 for state, count in states.items())):
        raise ValueError("invalid news outcome counts")
    categories = dict.fromkeys(("complete_state", "queued_or_retry", "registration_required",
        "scope_or_access_waiting", "waiting_cohort", "repair_required",
        "nontext_representation", "nonarticle_response", "source_currently_empty",
        "origin_absent", "unknown_outcome"), 0)
    for state, count in states.items():
        group = "complete_state" if state == "complete" else _NEWS_OUTCOME_GROUP_BY_STATE.get(state, "unknown_outcome")
        if group in {"queued_or_retry", "scope_or_access_waiting", "waiting_cohort"}:
            if not registered:
                group = "registration_required"
            elif group == "queued_or_retry" and not body_enabled:
                group = "scope_or_access_waiting"
        categories[group] += count
    return {"contract_version": NEWS_OUTCOME_CLASSIFICATION_VERSION,
            "grain": "retained queue URL-head states; not unique stories or stored bodies",
            "categories": categories, "known_urls": sum(categories.values()),
            "state_counts_preserved": True, "queue_mutation_possible": False,
            "origin_absence_is_not_all_paths_exhausted": True,
            "definitively_unavailable_urls": None, "closure_progress_ratio": None,
            "all_history_complete": False}


def planning_review_path(path: Path) -> Path | None:
    """An explicit catalog annotation, never a collector permission source.

    Keep this separate from configuration_paths/revision: changing an
    observation must refresh a read-only page, not restart an HTTP owner or
    change article retries, fingerprints or source authorization.
    """
    paths = configuration_paths(path)
    if len(paths) < 2:
        return None
    catalog = json.loads(paths[1].read_text())
    if not isinstance(catalog, dict):
        raise ValueError("invalid news source catalog")
    relative = catalog.get("planning_review_ledger")
    if relative is None:
        return None
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise ValueError("invalid news planning review path")
    target = (paths[1].parent / relative).resolve()
    if (not target.is_relative_to(paths[1].parent) or target.suffix != ".json"
            or not target.is_file() or target.stat().st_size > 2 << 20):
        raise ValueError("news planning reviews must be bounded catalog-local JSON")
    return target


def load_source_review_ledger(path: Path) -> dict:
    """Shared private planning observations; no access or reuse permission.

    Preserve evidence/text for the existing planner. Public callers must
    project only enumerations and verified review clocks, never this ledger.
    """
    if path.stat().st_size > 2 << 20:
        raise ValueError("oversized source review ledger")
    with path.open("rb") as stream:
        raw = stream.read((2 << 20) + 1)
    if len(raw) > 2 << 20:
        raise ValueError("oversized source review ledger")
    value = json.loads(raw)
    if (not isinstance(value, dict) or type(value.get("contract_version")) is not int
            or value["contract_version"] != 1 or not isinstance(value.get("providers"), dict)
            or len(value["providers"]) > 2048):
        raise ValueError("invalid source review ledger")
    reviews = value["providers"]
    for provider, review in reviews.items():
        if (not isinstance(provider, str) or not provider or len(provider) > 300
                or any(ord(char) < 32 for char in provider) or not isinstance(review, dict)
                or not isinstance(review.get("capture_decision"), str)
                or review.get("capture_decision") not in SOURCE_REVIEW_DECISIONS
                or not isinstance(review.get("evidence_urls"), list)
                or any(not isinstance(url, str) or not url.startswith("https://") for url in review["evidence_urls"])):
            raise ValueError("invalid source review entry")
    aliases = value.get("provider_aliases", {})
    if (not isinstance(aliases, dict) or len(aliases) > 2048
            or any(not isinstance(key, str) or not key or len(key) > 300
                   or any(ord(char) < 32 for char in key) or not isinstance(target, str)
                   or target not in reviews for key, target in aliases.items())):
        raise ValueError("invalid source review aliases")
    return {**reviews, **{key: reviews[target] for key, target in aliases.items() if key not in reviews}}


def configuration_paths(path: Path) -> tuple[Path, ...]:
    path = path.resolve()
    config = json.loads(path.read_text())
    if not isinstance(config, dict):
        raise ValueError("invalid news configuration")
    relative = config.get("provider_catalog")
    if not relative:
        return (path,)
    if not isinstance(relative, str):
        raise ValueError("invalid news provider catalog path")
    target = (path.parent / relative).resolve()
    if not target.is_relative_to(path.parent) or not target.is_file() or target.stat().st_size > 2 << 20:
        raise ValueError("news provider catalog must be a bounded local configuration")
    return path, target


def configuration_revision(path: Path):
    return tuple((str(p), p.stat().st_mtime_ns, p.stat().st_size) for p in configuration_paths(path))


def load_news_configuration(path: Path) -> dict:
    paths = configuration_paths(path)
    config = json.loads(paths[0].read_text())
    if len(paths) == 1:
        return config
    catalog = json.loads(paths[1].read_text())
    if (not isinstance(catalog, dict) or catalog.get("contract_version") != 1
            or not isinstance(catalog.get("providers"), list) or len(catalog["providers"]) > 512
            or not isinstance(catalog.get("defaults", {}), dict)):
        raise ValueError("invalid news source catalog")
    providers = list(config["providers"])
    existing = {entry["id"] for entry in providers}
    for recipe in catalog["providers"]:
        if not isinstance(recipe, dict) or recipe.get("id") in existing:
            raise ValueError("duplicate/invalid news catalog provider")
        existing.add(recipe.get("id"))
        entry = {**catalog.get("defaults", {}), **recipe}
        entry.setdefault("authorization", {"kind": "awaiting_authorization", "evidence_url": recipe.get("review_url", "")})
        providers.append(entry)
    config["providers"] = providers
    return config


def source_role(host: str) -> str:
    """A host role is not a copyright or publisher-identity determination."""
    groups = {
        "redirector": ("news.google.com", "reurl.cc", "bit.ly", "t.co", "goo.gl", "tinyurl.com"),
        "social_or_video": ("facebook.com", "youtube.com", "youtu.be", "instagram.com", "threads.net", "twitter.com", "x.com"),
        "aggregator": ("msn.com", "yahoo.com", "yahoo.com.tw", "today.line.me", "pchome.com.tw", "hinet.net", "yam.com"),
    }
    host = host.lower().rstrip('.')
    for role, domains in groups.items():
        if any(host == d or host.endswith('.' + d) for d in domains):
            return role
    if any(host.endswith(suffix) for suffix in ('.gov', '.gov.tw', '.gov.mo', '.gov.cn')):
        return "government_origin"
    return "publisher_or_other_unverified"


def adapter_summary(source, *, host: str = "") -> dict:
    """Describe implemented capability separately from observed acceptance."""
    role = source_role(host)
    if source is None:
        return {"source_role": role, "adapter": "declared_jsonld_article_candidate",
                "adapter_registered": False, "adapter_verified": False,
                "next_action": "resolve_original_publisher_url" if role == "redirector" else "review_source_and_register_adapter"}
    if source.cnyes_apis:
        adapter = "cnyes_public_json_and_html"
    elif source.pdf_path_pattern:
        adapter = "board_html_and_native_pdf"
    elif source.fulltext_feeds:
        adapter = "licensed_fulltext_rss_and_html"
    else:
        adapter = "declared_jsonld_and_source_css"
    return {"source_role": role, "adapter": adapter, "adapter_registered": True,
            "adapter_verified": False,
            "next_action": "capture_and_verify_original_bodies" if source.allowed else "review_capture_permission_and_article_fixture"}

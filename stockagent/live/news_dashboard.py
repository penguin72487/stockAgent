"""Bounded, read-only public projection of the canonical news owner's heartbeat.

Never open the writer queue, scan article files, import a collector or call a
publisher. Counts describe the discovered URL frontier, not all news history.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
import json
import math
from pathlib import Path
import re
from typing import Any, Mapping
from urllib.parse import urlsplit
from stockagent.data.news_worker_lifecycle import read_news_worker_lifecycle
from stockagent.data.news_source_catalog import (
    configuration_paths, load_news_configuration, load_source_review_ledger,
    planning_review_path, source_role, classify_news_outcomes,
    NEWS_OUTCOME_CLASSIFICATION_VERSION, NEWS_OUTCOME_STATES,
)


MAX_SNAPSHOT_BYTES = 4 * 1024 * 1024
MAX_AGE_SECONDS = 180
_ID = re.compile(r"^(?:[a-z][a-z0-9_]{0,63}|unregistered:[a-z0-9.-]{1,253})$")
_STATES = NEWS_OUTCOME_STATES
_READY_STATES = {"pending", "inflight", "retry", "retry_cooldown"}
# Match the canonical owner's actual cycle/daemon/CLI states. A completed
# cycle in a warm daemon is not a stopped download service. Conversely,
# draining, low disk and terminated CLI jobs must not retain a live forecast.
_ACTIVITIES = {
    "running": "running", "idle": "idle", "waiting": "waiting",
    "inventorying": "inventorying", "cycle_complete": "waiting",
    "waiting_next_due": "waiting", "draining": "draining",
    "waiting_disk": "waiting_disk", "stopped": "stopped",
    "interrupted": "stopped", "service_duration_complete": "stopped",
    "selected_urls_complete": "stopped", "inventory_complete": "stopped",
    "configuration_reload_requested": "waiting_restart",
}
_AUTHORIZATION = {"personal_research", "open_license", "public_domain", "unreviewed"}
_CAPACITY_STATES = {"warming", "ramping", "verifying", "waiting_source", "insufficient_samples",
                    "recovering_transport", "endpoint_validation_failed",
                    "insufficient_useful_data", "official_or_learned_floor", "stable_source_backoff",
                    "stable_pipeline_capacity", "engineering_ceiling_unverified", "metadata_polling"}


def _metadata_feed_progress(value, *, now):
    raw = _object(value)
    fields = ('registered_feeds', 'verified_feeds', 'latest_feed_item_observations',
              'latest_feed_response_bytes', 'feeds_with_errors', 'poll_interval_seconds')
    numbers = {key: _count(raw.get(key)) for key in fields}
    if (type(raw.get('contract_version')) is not int or raw.get('contract_version') not in {1, 2}
            or raw.get('enabled') is not True
            or any(item is None for item in numbers.values())
            or not 1 <= numbers['registered_feeds'] <= 14
            or numbers['verified_feeds'] > numbers['registered_feeds']
            or numbers['feeds_with_errors'] > numbers['registered_feeds']
            or not 60 <= numbers['poll_interval_seconds'] <= 86400
            or type(raw.get('article_bodies_accepted')) is not int or raw.get('article_bodies_accepted') != 0
            or raw.get('overlapping_feed_items_not_unique_articles') is not True
            or raw.get('all_history_complete') is not False or raw.get('finish_eta_utc') is not None):
        return None
    next_check = _stamp(raw.get('next_check_at_utc'))
    last_check = _stamp(raw.get('last_check_at_utc'))
    # The canonical queue's due=0 means ready, not a January 1970 appointment.
    return {'contract_version': raw['contract_version'], 'enabled': True, **numbers,
        'next_check_ready': bool(next_check and next_check <= now),
        'next_check_at_utc': next_check.isoformat() if next_check and next_check.timestamp() > 0 else None,
        'last_check_at_utc': last_check.isoformat() if last_check else None,
        'article_bodies_accepted': 0, 'overlapping_feed_items_not_unique_articles': True,
        'all_history_complete': False, 'finish_eta_utc': None}


def _object(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _body_work_progress(value: Any, ready: int | None) -> dict[str, Any] | None:
    """Safe optional owner projection; legacy/unreconciled snapshots are unknown."""
    raw = _object(value)
    if (raw.get('contract_version') != 1 or ready is None
            or raw.get('candidate_views_accepted_by_hint') is not False
            or raw.get('candidate_body_equivalence_verified') is not False):
        return None
    fields = ('active_missing_body_urls', 'existing_primary_body_candidates', 'no_known_primary_body_urls')
    counts = {key: _count(raw.get(key)) for key in fields}
    if (any(value is None for value in counts.values())
            or counts['active_missing_body_urls'] != counts['existing_primary_body_candidates'] + counts['no_known_primary_body_urls']
            or counts['active_missing_body_urls'] > ready):
        return None
    return {'contract_version': 1, **counts,
            'stored_body_recheck_urls': ready - counts['active_missing_body_urls'],
            'candidate_views_accepted_by_hint': False, 'candidate_body_equivalence_verified': False,
            'no_known_primary_includes_unclassified': True}


def _cnyes_public_index_progress(value: Any) -> dict[str, Any] | None:
    """Project receipted metadata counts, never a body/history denominator.

    Dates with every currently reported page are only the dates already
    observed by this endpoint. No queue scan, inferred total or saved ETA.
    """
    raw = _object(value)
    if (type(raw.get('contract_version')) is not int or raw['contract_version'] != 1
            or raw.get('enabled') is not True
            or type(raw.get('article_bodies_accepted_by_index')) is not int
            or raw['article_bodies_accepted_by_index'] != 0
            or raw.get('all_history_complete') is not False
            or raw.get('finish_eta_utc') is not None
            or raw.get('source_total_is_not_unique_story_count') is not True
            or raw.get('category_gaps_preserved') is not True):
        return None
    fields = ('source_dates_observed', 'verified_pages', 'metadata_rows',
              'public_ID_row_observations', 'dates_all_reported_pages_observed',
              'dates_with_changed_source_total')
    counts = {key: _count(raw.get(key)) for key in fields}
    if any(value is None for value in counts.values()):
        return None
    dates, pages, rows = (counts[key] for key in fields[:3])
    if (pages < dates or (not dates and pages)
            or rows > pages * 30 or counts['public_ID_row_observations'] > rows
            or counts['dates_all_reported_pages_observed'] + counts['dates_with_changed_source_total'] > dates):
        return None
    return {'contract_version': 1, 'enabled': True, **counts,
            'article_bodies_accepted_by_index': 0, 'all_history_complete': False,
            'finish_eta_utc': None, 'progress_ratio': None,
            'source_total_is_not_unique_story_count': True, 'category_gaps_preserved': True}


def _cnyes_category_gap_count(value: Any) -> int | None:
    """Read the full owner projection, not its truncated private URL sample."""
    raw = _object(value)
    groups = raw.get('groups')
    total = _count(raw.get('total_gaps'))
    if (type(raw.get('contract_version')) is not int or raw['contract_version'] != 1
            or total is None or not isinstance(groups, list) or len(groups) > 4096
            or raw.get('all_gap_details_preserved') is not True
            or raw.get('historical_completeness_verified') is not False):
        return None
    seen, counted, cnyes = set(), 0, 0
    for item in groups:
        group = _object(item)
        provider, reason, scopes = group.get('provider'), group.get('error_code'), _count(group.get('scopes'))
        if (not isinstance(provider, str) or not _ID.fullmatch(provider)
                or not isinstance(reason, str) or not 0 < len(reason) <= 160
                or scopes is None or (provider, reason) in seen):
            return None
        seen.add((provider, reason))
        counted += scopes
        if provider == 'cnyes':
            cnyes += scopes
    return cnyes if counted == total else None


def _cnyes_manifest_phase_progress(value: Any, metrics: Mapping[str, Any],
                                   now: datetime, live: bool, current_owner: Any) -> dict[str, Any] | None:
    """A source-reported finite metadata phase, separate from all-history ETA.

    Page views only validate/project the existing owner snapshot. Recompute
    ratios and scenario times from typed counts; never open the queue, read
    publisher originals, extrapolate unseen dates, or expose scope URLs.
    """
    index = _cnyes_public_index_progress(value)
    raw = _object(_object(value).get('manifest_phase'))
    if (index is None or type(raw.get('contract_version')) is not int or raw['contract_version'] != 1
            or raw.get('scope') != 'actual_category_overflow_source_dates'
            or not isinstance(raw.get('scope_fingerprint'), str)
            or re.fullmatch(r'[0-9a-f]{64}', raw['scope_fingerprint']) is None
            or raw.get('metadata_only') is not True or raw.get('body_completion_claimed') is not False
            or raw.get('all_history_complete') is not False
            or type(raw.get('totals_known')) is not bool or type(raw.get('phase_complete')) is not bool):
        return None
    fields = ('planned_source_dates', 'first_pages_observed', 'first_pages_remaining',
              'verified_pages', 'observed_metadata_rows', 'dates_with_changed_source_total')
    counts = {key: _count(raw.get(key)) for key in fields}
    if any(item is None for item in counts.values()):
        return None
    planned, first, missing, pages, rows, changed = (counts[key] for key in fields)
    if (not planned or first + missing != planned or first > index['source_dates_observed']
            or pages < first or pages > index['verified_pages'] or rows > min(pages * 30, index['metadata_rows'])
            or changed > min(planned, index['dates_with_changed_source_total'])):
        return None
    total_pages = _count(raw.get('source_reported_total_pages'))
    total_rows = _count(raw.get('source_reported_total_metadata_rows'))
    remaining = _count(raw.get('remaining_pages'))
    ratio = _number(raw.get('progress_ratio'))
    known = raw['totals_known']
    complete = raw['phase_complete']
    if known:
        if (missing or changed or total_pages is None or total_rows is None or remaining is None
                or total_pages < max(planned, pages) or total_rows < rows or total_rows > total_pages * 30
                or remaining != total_pages - pages or ratio is None
                or abs(ratio - pages / total_pages) > 1e-12
                or complete != (not remaining and rows == total_rows)):
            return None
    elif (complete or any(raw.get(key) is not None for key in (
            'source_reported_total_pages', 'source_reported_total_metadata_rows', 'remaining_pages', 'progress_ratio'))):
        return None
    forecast = {'contract_version': 1, 'state': 'unavailable', 'sample_seconds': None,
        'sample_count': None, 'verified_pages': None, 'pages_per_second': None,
        'fastest_seconds': None, 'middle_seconds': None, 'slowest_seconds': None,
        'middle_finish_at_utc': None, 'scenario_not_confidence_interval': True,
        'metadata_only': True, 'all_history_complete': False}
    if not live:
        forecast['state'] = 'inactive'
    elif not known:
        forecast['state'] = 'unknown_denominator'
    elif complete:
        forecast['state'] = 'phase_complete'
    else:
        saved = _object(raw.get('forecast'))
        clock, metric_clock = _stamp(saved.get('observed_at_utc')), _stamp(metrics.get('snapshot_at_utc'))
        elapsed = _number(metrics.get('owner_elapsed_seconds'))
        owner = metrics.get('owner_id')
        bound = (type(metrics.get('snapshot_clock_contract_version')) is int
            and metrics['snapshot_clock_contract_version'] == 1
            and clock is not None and metric_clock is not None and elapsed is not None
            and isinstance(owner, str) and re.fullmatch(r'[0-9a-f]{32}', owner) is not None
            and owner == current_owner and abs((clock - metric_clock).total_seconds()) <= .001
            and 0 <= (now - clock).total_seconds() <= MAX_AGE_SECONDS)
        if (type(saved.get('contract_version')) is int and saved['contract_version'] == 1
                and saved.get('scenario_not_confidence_interval') is True
                and saved.get('metadata_only') is True and saved.get('all_history_complete') is False and bound):
            sample_count, seconds, measured = (_count(saved.get(key)) for key in
                ('sample_count', 'sample_seconds', 'verified_pages'))
            bins = saved.get('window_page_counts')
            valid_bins = (sample_count is not None and 3 <= sample_count <= 5
                and seconds == sample_count * 60 and seconds <= elapsed + .001
                and measured is not None and measured <= pages and isinstance(bins, list)
                and len(bins) == sample_count and all(_count(count) is not None for count in bins)
                and sum(bins) == measured)
            if saved.get('state') == 'warming' and (not valid_bins or measured < 10):
                forecast['state'] = 'warming'
            elif valid_bins and saved.get('state') == 'no_recent_progress' and bins[-1] == 0:
                forecast.update(state='no_recent_progress', sample_count=sample_count,
                    sample_seconds=seconds, verified_pages=measured)
            elif valid_bins and bins[-1] > 0 and measured >= 10 and saved.get('state') == 'recent_measured' and remaining:
                rate = measured / seconds
                fast, middle = remaining * 60 / max(bins), remaining / rate
                slow = remaining * 60 / min(bins) if min(bins) else None
                finish = clock + timedelta(seconds=middle)
                supplied = {key: _number(saved.get(key)) for key in
                    ('pages_per_second', 'fastest_seconds', 'middle_seconds', 'slowest_seconds')}
                expected = {'pages_per_second': rate, 'fastest_seconds': fast,
                            'middle_seconds': middle, 'slowest_seconds': slow}
                saved_finish = _stamp(saved.get('middle_finish_at_utc'))
                if (all(supplied[key] is None if val is None else supplied[key] is not None
                        and math.isclose(supplied[key], val, rel_tol=1e-10, abs_tol=1e-9)
                        for key, val in expected.items()) and saved_finish is not None
                        and abs((saved_finish - finish).total_seconds()) <= .001):
                    forecast.update(state='recent_measured', sample_count=sample_count,
                        sample_seconds=seconds, verified_pages=measured, **expected,
                        middle_finish_at_utc=finish.isoformat())
    return {'contract_version': 1, 'scope': 'actual_category_overflow_source_dates', **counts,
        'totals_known': known, 'source_reported_total_pages': total_pages,
        'source_reported_total_metadata_rows': total_rows, 'remaining_pages': remaining,
        'progress_ratio': pages / total_pages if known else None, 'phase_complete': complete,
        'metadata_only': True, 'body_completion_claimed': False, 'all_history_complete': False,
        'forecast': forecast}


def _cnyes_history_list_progress(value: Any, gaps: Any) -> dict[str, Any] | None:
    """Only current-contract scope counts; scopes can overlap across dates."""
    raw = _object(value)
    states = raw.get('current_source_contract_scope_states')
    complete = ('list_complete', 'list_complete_by_page_ID_equivalence', 'partition_list_complete')
    waiting = ('waiting_page_evidence', 'waiting_original_page_recheck', 'waiting_child_evidence')
    inconsistent = ('inconsistent_source_snapshot', 'inconsistent_original_page_ID_reference',
                    'inconsistent_source_identity_or_date', 'inconsistent_partition_evidence')
    # These are actual canonical producer outcomes, not unknown states that
    # may be silently coerced into complete. A source conflict must remain
    # visible without hiding all otherwise verified historical scopes.
    allowed = complete + waiting + inconsistent + ('source_page_cap_exceeded',)
    retired = _count(raw.get('retired_gap_receipts'))
    if (type(raw.get('contract_version')) is not int or raw['contract_version'] != 1
            or not isinstance(states, Mapping) or set(states) - set(allowed)
            or retired is None or raw.get('list_coverage_is_not_body_coverage') is not True
            or raw.get('all_history_complete') is not False or raw.get('finish_eta_utc') is not None):
        return None
    counts = {key: _count(states.get(key, 0)) for key in allowed}
    if any(value is None for value in counts.values()) or sum(counts.values()) > 2**53 - 1:
        return None
    return {'contract_version': 1, 'current_source_contract_scope_states': counts,
            'scope_outcome_projection_contract_version': 1,
            'verified_list_scopes': sum(counts[key] for key in complete),
            'waiting_list_scopes': sum(counts[key] for key in waiting),
            'inconsistent_source_scopes': sum(counts[key] for key in inconsistent),
            'page_cap_exceeded_scopes': counts['source_page_cap_exceeded'],
            'retired_gap_receipts': retired, 'unresolved_category_scopes': _cnyes_category_gap_count(gaps),
            'list_coverage_is_not_body_coverage': True, 'all_history_complete': False,
            'finish_eta_utc': None, 'progress_ratio': None}


def _read(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as stream:
            raw = stream.read(MAX_SNAPSHOT_BYTES + 1)
        if len(raw) > MAX_SNAPSHOT_BYTES:
            return {}
        value = json.loads(raw)
    except (OSError, UnicodeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _count(value: Any) -> int | None:
    return value if type(value) is int and 0 <= value <= 2**53 - 1 else None


def _number(value: Any) -> float | None:
    if type(value) not in {int, float}:
        return None
    return float(value) if 0 <= value <= 1e18 and math.isfinite(value) else None


def _stamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or len(value) > 64:
        return None
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return stamp.astimezone(UTC) if stamp.tzinfo is not None else None


def _completion_forecast(metrics: Mapping[str, Any], calibration: Mapping[str, Any],
                         heads: int | None, observed: datetime | None,
                         now: datetime, live: bool, *, current_owner: Any = None) -> dict[str, Any] | None:
    """Recompute ETA from consecutive completed current-owner windows.

    Saved capacity history is useful for restart recovery but is not this
    owner's throughput. Include unhealthy/zero/gap time, never select the
    fastest healthy window. No source URL or private reason is projected.
    """
    clock_version = metrics.get('snapshot_clock_contract_version')
    version = calibration.get('measurement_contract_version')
    owner_version = calibration.get('completion_window_owner_contract_version')
    if (type(clock_version) is not int or clock_version != 1
            or type(version) is not int or version not in {10, 11}
            or type(owner_version) is not int or owner_version != 1):
        return None  # Explicit legacy owner-rate fallback, not fake windows.
    result = {'contract_version': 1, 'state': 'unavailable',
              'basis': 'recent_independent_new_url_heads',
              'new_url_heads_per_second': None, 'sample_count': None,
              'new_url_heads': None, 'sample_seconds': None,
              'started_at_utc': None, 'ended_at_utc': None,
              'includes_failures_waits_and_gaps': True, 'cached_heads_excluded': True,
              'current_owner_clock_bound': False, 'all_history_complete': False}
    clock = _stamp(metrics.get('snapshot_at_utc'))
    elapsed = _number(metrics.get('owner_elapsed_seconds'))
    owner = metrics.get('owner_id')
    if (not live or observed is None or clock is None or elapsed is None or heads is None
            or not isinstance(owner, str) or re.fullmatch(r'[0-9a-f]{32}', owner) is None
            or owner != current_owner
            or not 0 <= (observed - clock).total_seconds() <= MAX_AGE_SECONDS
            or not 0 <= (now - clock).total_seconds() <= MAX_AGE_SECONDS):
        return result
    end_limit = clock.timestamp()
    owner_start = end_limit - elapsed
    if owner_start < 0:
        return result
    history = calibration.get('history')
    latest = calibration.get('last_window')
    if not isinstance(history, list) or len(history) > 128:
        return result
    result.update(state='collecting_recent_samples', sample_count=0,
                  current_owner_clock_bound=True)
    if not history:
        return result if latest is None else {**result, 'state': 'unavailable', 'sample_count': None}
    if not isinstance(latest, Mapping) or history[-1] != latest:
        return {**result, 'state': 'unavailable', 'sample_count': None}
    selected = []
    for raw in reversed(history):
        sample = _object(raw)
        if sample.get('owner_id') != owner:
            break  # Legacy/other-owner evidence stays capacity-only, not ETA.
        end, wall, count = (_number(sample.get('ended_at_epoch')),
                            _number(sample.get('wall_seconds')), _count(sample.get('new_stored_url_heads')))
        if (end is None or wall is None or count is None or not 0 < wall <= 600
                or end > end_limit or end - wall < 0):
            return {**result, 'state': 'unavailable', 'sample_count': None}
        start = end - wall
        if not selected and end_limit - end > 120:
            return {**result, 'state': 'stale_recent_measurement', 'sample_count': None}
        if start < owner_start:
            break  # Cannot reuse a genuine but earlier owner's window.
        if selected:
            gap = selected[-1][0] - end
            if gap < -0.01:
                return {**result, 'state': 'unavailable', 'sample_count': None}
            if gap > 5 or selected[0][1] - start > 600:
                break  # Do not bridge a reset/long unobserved interval.
        selected.append((start, end, wall, count))
        if selected[0][1] - start >= 300:
            break  # Whole windows only; never prorate measured counts.
    if not selected:
        return result
    total_heads = sum(row[3] for row in selected)
    wall = max(sum(row[2] for row in selected), selected[0][1] - selected[-1][0])
    if total_heads > heads or not math.isfinite(wall):
        return {**result, 'state': 'unavailable', 'sample_count': None}
    result.update(sample_count=len(selected), new_url_heads=total_heads, sample_seconds=wall,
                  started_at_utc=datetime.fromtimestamp(selected[-1][0], UTC).isoformat(),
                  ended_at_utc=datetime.fromtimestamp(selected[0][1], UTC).isoformat())
    if len(selected) >= 2 and wall >= 90 and (total_heads >= 10 or total_heads == 0):
        result.update(state='recent_measured' if total_heads else 'no_recent_new_heads',
                      new_url_heads_per_second=total_heads / wall)
    return result


def _robots_origin_pause(control: Mapping[str, Any], metrics: Mapping[str, Any],
                         now: datetime, live: bool) -> dict[str, Any] | None:
    """Bounded private deadlines become counts/clocks, never policy permission.

    Missing, legacy, stale or malformed observations remain unknown. An expired
    negative policy only permits the owner's next policy probe; it is not an
    article allow-rule or evidence that the whole provider has recovered.
    """
    version = metrics.get('robots_origin_policy_contract_version')
    origins = control.get('temporarily_paused_robots_origins')
    if not live or type(version) is not int or version != 1 or not isinstance(origins, Mapping) or len(origins) > 32:
        return None
    deadlines = []
    for origin, value in origins.items():
        if not isinstance(origin, str) or len(origin) > 320:
            return None
        try:
            parts = urlsplit(origin)
            if (parts.scheme not in {'http', 'https'} or not parts.hostname or parts.username is not None
                    or parts.password is not None or parts.path or parts.query or parts.fragment
                    or parts.geturl() != origin or origin != origin.lower()
                    or not re.fullmatch(r'[a-z0-9.-]+', parts.hostname)
                    or parts.port is not None and not 1 <= parts.port <= 65535):
                return None
        except ValueError:
            return None
        deadline = _number(value)
        if deadline is None or deadline > now.timestamp() + 305:
            return None
        if deadline > now.timestamp():
            deadlines.append(deadline)
    return {'contract_version': 1, 'scope': 'initial_origin_policy',
            'active_origin_count': len(deadlines),
            'next_policy_probe_at_utc': datetime.fromtimestamp(min(deadlines), UTC).isoformat() if deadlines else None,
            'permission_granted': False, 'provider_recovery_verified': False}


def _states(value: Any) -> dict[str, int]:
    result: dict[str, int] = {}
    for key, count in _object(value).items():
        number = _count(count)
        if number is not None:
            name = key if key in _STATES else "other"
            result[name] = result.get(name, 0) + number
    return result


def news_snapshot_revision(repo_root: Path) -> str:
    """Cheap generation identity; no persistent cache or queue access."""
    parts = []
    paths = [repo_root / "data_news_fulltext/status.json", repo_root / "configs/news_fulltext.json"]
    lifecycle_path = repo_root / 'data_news_fulltext/owner_lifecycle.json'
    if lifecycle_path.is_file():
        paths.append(lifecycle_path)
    cna_path = repo_root / "configs/news/cna_authorized_api.json"
    if cna_path.is_file():
        paths.append(cna_path)
    try:
        paths.extend(configuration_paths(paths[1])[1:])
        review_path = planning_review_path(paths[1])
        if review_path is not None:
            paths.append(review_path)
    except (OSError, ValueError, KeyError, TypeError):
        pass
    for path in paths:
        try:
            stat = path.stat()
            parts.append(f"{stat.st_ino}:{stat.st_mtime_ns}:{stat.st_size}")
        except OSError:
            parts.append("missing")
    return ":".join(parts)


def _source_reviews(repo_root: Path) -> tuple[dict, str]:
    """Bounded local observations, independent of collector authorization.

    Malformed or missing reviews must not hide successful capture telemetry.
    Free-form text and evidence URLs stay in the private planning ledger.
    """
    try:
        path = planning_review_path(repo_root / "configs/news_fulltext.json")
        if path is None:
            return {}, "not_configured"
        return load_source_review_ledger(path), "loaded"
    except (OSError, UnicodeError, ValueError, KeyError, TypeError):
        return {}, "unavailable"


def _original_sources(status, raw_providers):
    """Validate only typed summary counts; never expose destination URLs.

    Missing legacy evidence is unknown. Alias incidence is not a new article
    denominator and cannot establish a source's access/identity/rights.
    """
    raw = _object(status.get('original_source_inventory'))
    summary = {'contract_version': 1, 'available': False, 'source_keys': None,
               'literal_metadata_links': None, 'original_hosts': None,
               'body_completion_claimed': False, 'metadata_identity_verified': False,
               'publisher_requests': 0}
    sources = {}
    for identifier, provider in raw_providers.items():
        evidence = _object(_object(provider).get('original_metadata'))
        links, hosts = (_count(evidence.get(key)) for key in ('literal_metadata_links', 'original_hosts'))
        if (isinstance(identifier, str) and _ID.fullmatch(identifier)
                and type(evidence.get('contract_version')) is int and evidence['contract_version'] == 1
                and links is not None and hosts is not None and 0 < hosts <= links):
            sources[identifier] = {'contract_version': 1, 'literal_metadata_links': links,
                                   'original_hosts': hosts, 'body_completion_claimed': False}
    keys, links, hosts = (_count(raw.get(key)) for key in ('source_keys', 'literal_metadata_links', 'original_hosts'))
    if (type(raw.get('contract_version')) is int and raw['contract_version'] == 1
            and type(raw.get('resolution_contract_version')) is int and raw['resolution_contract_version'] == 1
            and raw.get('available') is True and keys == len(sources)
            and links == sum(item['literal_metadata_links'] for item in sources.values())
            and hosts == sum(item['original_hosts'] for item in sources.values())):
        summary.update(available=True, source_keys=keys, literal_metadata_links=links, original_hosts=hosts)
        return summary, sources
    return summary, {}


def build_news_public_status(repo_root: Path, *, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        raise ValueError("news dashboard clock requires a timezone")
    now = now.astimezone(UTC)
    status = _read(repo_root / "data_news_fulltext/status.json")
    try:
        config = load_news_configuration(repo_root / "configs/news_fulltext.json")
    except (OSError, ValueError, KeyError, TypeError):
        config = {}
    source_reviews, source_reviews_state = _source_reviews(repo_root)
    # A declared licensed API and the disabled legacy HTML adapter are
    # different states. Configuration-only projection: no .env, API client,
    # arbitrary config text, keys, tools/schema or entitlement inference.
    cna_setup = _read(repo_root / "configs/news/cna_authorized_api.json")
    authorized_cna_setup = bool(
        type(cna_setup.get('contract_version')) is int and cna_setup['contract_version'] == 1
        and cna_setup.get('provider') == 'cna'
        and cna_setup.get('endpoint') == 'https://ask.cna.com.tw/mcp/connect'
        and cna_setup.get('key_env_name') == 'CNA_API_KEY'
        and cna_setup.get('user_authorization_declared') is True
        and cna_setup.get('phase') == 'capability_discovery_only'
        and cna_setup.get('article_tool_calls_enabled') is False
        and cna_setup.get('fulltext_pipeline_enabled') is False)
    observed = _stamp(status.get("updated_at_utc"))
    age = (now - observed).total_seconds() if observed else None
    fresh = bool(age is not None and -5 <= age <= MAX_AGE_SECONDS)
    raw_activity = status.get("activity")
    source_activity = raw_activity if isinstance(raw_activity, str) and raw_activity in _ACTIVITIES else "unavailable"
    activity = _ACTIVITIES.get(source_activity, "unavailable")
    valid = type(status.get("contract_version")) is int and status["contract_version"] == 1
    lifecycle = read_news_worker_lifecycle(repo_root / 'data_news_fulltext',
        snapshot_owner_id=status.get('owner_id'), snapshot_at=observed, now=now)
    initializing = False
    lifecycle_stopped = False
    if lifecycle.get('available') and (lifecycle['snapshot_owner_matches'] or lifecycle['supersedes_snapshot']):
        lifecycle_stopped = not lifecycle['process_alive'] or lifecycle['state'] in {'closed', 'failed'}
        initializing = not lifecycle_stopped and (not lifecycle['snapshot_owner_matches']
            or lifecycle['state'] == 'initializing')
        if initializing:
            activity = 'initializing' if lifecycle['age_seconds'] <= MAX_AGE_SECONDS else 'initializing_stale'
        elif lifecycle_stopped and activity not in {'stopped', 'waiting_restart'}:
            activity = 'stopped'
    live = valid and fresh and activity in {"running", "idle", "waiting", "inventorying"}
    health = ("unavailable" if not status else "invalid" if not valid else "stale" if not fresh
              else "stopped" if activity == "stopped" else activity)
    if initializing or lifecycle_stopped:
        health = activity
    configured: dict[str, Mapping[str, Any]] = {}
    for source in config.get("providers", []) if isinstance(config.get("providers"), list) else []:
        source = _object(source)
        identifier = source.get("id")
        if isinstance(identifier, str) and _ID.fullmatch(identifier):
            configured[identifier] = source
    raw_providers = _object(status.get("providers")) if valid else {}
    original_inventory, original_sources = _original_sources(status if valid else {}, raw_providers)
    # Persisted article counts are reusable. Previous-process timings, requests
    # and capacity windows are not observations of the initializing new owner.
    metrics = _object(status.get("performance")) if valid and not initializing else {}
    metric_providers = _object(metrics.get("providers"))
    elapsed = _number(metrics.get("owner_elapsed_seconds"))
    cooldowns = _object(status.get("provider_next_attempt_utc"))
    capacity = _object(status.get("http_capacity")) if valid and fresh and not initializing else {}
    runtime_controls = _object(capacity.get("providers"))
    calibration = _object(_object(capacity.get("calibration")).get("providers"))
    calibration_version = _object(capacity.get('calibration')).get('measurement_contract_version')
    completion_owner_version = _object(capacity.get('calibration')).get('completion_window_owner_contract_version')
    endpoint_circuits = {}
    for circuit in status.get("bulk_endpoint_cooldowns", []) if isinstance(status.get("bulk_endpoint_cooldowns"), list) else []:
        circuit = _object(circuit)
        identifier = circuit.get("provider")
        deadline = _number(circuit.get("not_before"))
        if isinstance(identifier, str) and identifier in configured and deadline is not None and deadline > now.timestamp():
            endpoint_circuits[identifier] = endpoint_circuits.get(identifier, 0) + 1
    providers = []
    stored_counter_declared = 'provider_stored_body_counter_contract_version' in status
    stored_counter_version = status.get('provider_stored_body_counter_contract_version')
    stored_counter_supported = type(stored_counter_version) is int and stored_counter_version == 1
    for identifier in sorted(set(configured) | set(raw_providers)):
        if not isinstance(identifier, str) or not _ID.fullmatch(identifier):
            continue
        raw, settings = _object(raw_providers.get(identifier)), configured.get(identifier, {})
        counts = _states(raw.get("states"))
        known = sum(counts.values()) if valid else None
        ready = sum(counts.get(key, 0) for key in _READY_STATES) if valid else None
        complete = counts.get("complete", 0) if valid else None
        stored_urls = (_count(raw.get('stored_body_urls')) if raw else 0) if valid and stored_counter_supported else None
        if stored_urls is not None and (known is None or stored_urls > known):
            stored_urls = None
        stored_ratio = stored_urls / known if known and stored_urls is not None else None
        growth_waiting = counts.get('waiting_cohort', 0) if valid else None
        blocked = known - (ready or 0) - (complete or 0) - (growth_waiting or 0) if known is not None else None
        raw_mode = raw.get('acquisition_mode')
        mode = raw_mode if isinstance(raw_mode, str) and raw_mode in {'finite', 'continuous'} else 'continuous'
        cohort_raw = _object(raw.get('finite_cohort')) if mode == 'finite' else {}
        cohort_urls, cohort_bodies = _count(cohort_raw.get('urls')), _count(cohort_raw.get('stored_bodies'))
        cohort = None
        if (cohort_urls is not None and cohort_bodies is not None and known is not None
                and 0 < cohort_urls <= known and cohort_bodies <= cohort_urls):
            cohort = {'urls': cohort_urls, 'stored_bodies': cohort_bodies,
                      'remaining_missing_bodies': cohort_urls - cohort_bodies,
                      'progress_ratio': cohort_bodies / cohort_urls,
                      'new_urls_outside_cohort': known - cohort_urls,
                      'all_cohort_bodies_stored': cohort_bodies == cohort_urls}
        supplement_raw = _object(raw.get('finite_supplements')) if cohort else {}
        supplement_urls = _count(supplement_raw.get('urls'))
        supplement_bodies = _count(supplement_raw.get('stored_bodies'))
        supplement_scopes = _count(supplement_raw.get('scopes'))
        supplement = None
        if (cohort and supplement_urls is not None and supplement_bodies is not None
                and supplement_scopes is not None and 0 < supplement_scopes <= supplement_urls
                and supplement_urls <= known - cohort_urls and supplement_bodies <= supplement_urls
                and supplement_raw.get('original_cohort_unchanged') is True):
            # Separate disjoint, receipted scopes, never enlarge the original
            # denominator or expose the private diagnostics/receipt paths.
            supplement = {'scopes': supplement_scopes, 'urls': supplement_urls,
                          'stored_bodies': supplement_bodies,
                          'remaining_missing_bodies': supplement_urls - supplement_bodies,
                          'progress_ratio': supplement_bodies / supplement_urls,
                          'all_supplement_bodies_stored': supplement_bodies == supplement_urls}
        if cohort:
            outside = known - cohort_urls - (supplement_urls if supplement else 0)
            outcome_raw = _object(cohort_raw.get('reviewed_nonbody_outside_cohort'))
            outcome_count = _count(outcome_raw.get('urls'))
            outcome_states = _states(outcome_raw.get('states'))
            verified_outcomes = None
            if (type(outcome_raw.get('contract_version')) is int and outcome_raw['contract_version'] == 1
                    and outcome_raw.get('accepted_article_bodies') == 0
                    and type(outcome_raw.get('accepted_article_bodies')) is int
                    and outcome_count is not None and outcome_count <= outside
                    and sum(outcome_states.values()) == outcome_count
                    and set(outcome_states) <= {'source_not_found', 'unsupported_article_path'}
                    and all(value <= counts.get(key, 0) for key, value in outcome_states.items())):
                verified_outcomes = {'contract_version': 1, 'urls': outcome_count, 'states': outcome_states,
                                     'accepted_article_bodies': 0}
            cohort['reviewed_nonbody_outside_cohort'] = verified_outcomes
            cohort['unreviewed_new_urls'] = outside - (outcome_count if verified_outcomes else 0)
        enabled = raw.get("automatic_collection_enabled") is True
        metadata_feed = _metadata_feed_progress(raw.get('metadata_feed_progress'), now=now) if valid else None
        body_enabled = enabled and raw.get('automatic_fulltext_allowed', True) is True
        outcome_progress = classify_news_outcomes(dict(counts), registered=identifier in configured,
                                                  body_enabled=body_enabled) if valid else None
        detail = _object(metric_providers.get(identifier))
        http_counts = _object(detail.get("counts"))
        accepted = _count(http_counts.get("durably_accepted_versions"))
        heads = _count(http_counts.get("durably_accepted_new_url_heads"))
        # Forecast URL-head completion from newly stored URL heads, not repeated
        # body revisions. Older snapshots retain their explicitly labeled proxy.
        forecast_count = heads if heads is not None else accepted
        rate = forecast_count / elapsed if live and forecast_count is not None and forecast_count >= 10 and elapsed and elapsed >= 60 else None
        measured_capacity = _object(calibration.get(identifier))
        forecast = (_completion_forecast(metrics, {**measured_capacity, 'measurement_contract_version': calibration_version,
                                         'completion_window_owner_contract_version': completion_owner_version},
                                         heads, observed, now, live, current_owner=status.get('owner_id')) if body_enabled else None)
        eta_rate = forecast['new_url_heads_per_second'] if forecast is not None else rate
        cooldown = _stamp(cooldowns.get(identifier))
        cooling = bool(cooldown and cooldown > now)
        eta = (ready / eta_rate if body_enabled and ready and eta_rate and not cooling else
               0.0 if body_enabled and ready == 0 and live else None)
        if mode == 'finite' and ready == 0 and (
                not cohort or not cohort['all_cohort_bodies_stored']
                or supplement and not supplement['all_supplement_bodies_stored']
                or supplement_raw and supplement is None):
            eta = None  # No pending request is not proof all cohort bodies exist.
        if ready == 0 and (blocked or growth_waiting):
            eta = None  # A completed old batch is not a finished known frontier.
        # A finite conditional projection is not a guaranteed completion clock.
        finish = None
        if eta is not None and observed and eta <= 3650 * 86400:
            finish = (observed + timedelta(seconds=eta)).isoformat()
            if ready and observed + timedelta(seconds=eta) <= now:
                eta = finish = None  # Old projection elapsed without proof of completion.
        state = (activity if initializing and enabled else "unavailable" if not valid else "not_enabled" if not enabled else
                 "stale" if not fresh else activity if not live else
                 "cooldown" if cooling else
                 ("attention" if metadata_feed['feeds_with_errors'] else "metadata_polling")
                     if metadata_feed and not body_enabled else
                 "running" if counts.get("inflight", 0) else
                 "backfilling" if ready else "attention" if blocked else
                 "waiting_cohort" if growth_waiting else
                 "finite_complete" if cohort and cohort['all_cohort_bodies_stored'] else "known_queue_clear")
        api_setup = None
        if identifier == 'cna' and authorized_cna_setup and not enabled:
            api_setup = {'phase': 'capability_discovery_only', 'user_authorization_declared': True,
                         'key_env_name': 'CNA_API_KEY', 'connection_verified': False,
                         'quota_verified': False, 'fulltext_pipeline_enabled': False}
            if valid:
                state = 'authorized_api_pending'
        name = settings.get("name")
        # Display names are configuration labels, never free-form source errors.
        if not isinstance(name, str) or len(name) > 160 or any(ord(char) < 32 for char in name):
            name = identifier.removeprefix("unregistered:")
        control = _object(runtime_controls.get(identifier))
        sample = _object(measured_capacity.get("last_window"))
        capacity_state = measured_capacity.get("state")
        capacity_state = capacity_state if isinstance(capacity_state, str) and capacity_state in _CAPACITY_STATES else None
        interval = _number(control.get("effective_interval_seconds"))
        if interval is None or interval <= 0:
            interval = _number(settings.get("interval_seconds"))
        auth = raw.get("authorization_kind")
        auth = auth if auth in _AUTHORIZATION else "unreviewed"
        requests = _count(http_counts.get("http_requests_started"))
        responses = _count(http_counts.get("http_responses_received"))
        http_errors = {str(code): value for code in (400, 401, 403, 404, 406, 408, 410, 415, 422, 429, 451, 500, 502, 503, 504)
                       if (value := _count(http_counts.get(f"http_status_{code}"))) is not None and value > 0}
        review = source_reviews.get(identifier)
        review_at = _stamp(review.get("reviewed_at_utc")) if review else None
        review_valid = review_at is not None and review_at <= now + timedelta(seconds=5)
        review_decision = (review["capture_decision"] if review_valid else "review_unverified" if review
                          else "active_existing_policy" if enabled else "not_reviewed")
        providers.append({
            "id": identifier, "name": name, "configured": identifier in configured,
            "enabled": enabled, "state": state, "authorization_kind": auth,
            "automatic_fulltext_allowed": body_enabled, "metadata_feed_progress": metadata_feed,
            "authorized_api_setup": api_setup,
            "source_policy_review": review_decision,
            "source_reviewed_at_utc": review_at.isoformat() if review_valid else None,
            "adapter_registered": identifier in configured,
            "adapter_acceptance": "committed_body_heads_observed" if stored_urls or complete else "no_body_acceptance_observed",
            "source_role": source_role(identifier.removeprefix("unregistered:")) if identifier.startswith("unregistered:") else
                source_role(settings.get("domains", [""])[0]) if isinstance(settings.get("domains"), list) and settings["domains"] else "publisher_or_other_unverified",
            "known_urls": known, "complete_urls": complete, "remaining_urls": ready,
            "stored_body_urls": stored_urls, "stored_body_progress_ratio": stored_ratio,
            "stored_body_counter_available": stored_counter_supported,
            "outcome_progress": outcome_progress,
            "original_metadata": original_sources.get(identifier),
            "body_work_progress": _body_work_progress(raw.get('body_work_progress'), ready) if identifier == 'cnyes' else None,
            "public_index_progress": _cnyes_public_index_progress(raw.get('public_index_progress')) if identifier == 'cnyes' else None,
            "manifest_phase_progress": _cnyes_manifest_phase_progress(raw.get('public_index_progress'), metrics,
                now, live and raw.get('automatic_collection_enabled') is True, status.get('owner_id')) if identifier == 'cnyes' else None,
            "history_list_progress": _cnyes_history_list_progress(raw.get('history_list_progress'), status.get('bulk_gap_projection')) if identifier == 'cnyes' else None,
            "blocked_urls": blocked, "states": counts, 'growth_waiting_urls': growth_waiting,
            'acquisition_mode': mode, 'finite_cohort': cohort, 'finite_supplements': supplement,
            "progress_ratio": cohort['progress_ratio'] if cohort else complete / known if known else None,
            "text_bytes": _count(raw.get("text_bytes")),
            "owner_requests": requests, "owner_responses": responses,
            "owner_response_bytes": _count(http_counts.get("http_response_bytes")),
            "owner_accepted_versions": accepted, "owner_articles_per_second": rate,
            "owner_new_stored_url_heads": heads,
            "forecast_rate_basis": "new_stored_url_heads" if heads is not None else "legacy_version_proxy",
            "completion_forecast": forecast,
            "completion_forecast_rate_basis": (forecast['basis'] if forecast is not None else
                                               'owner_new_stored_url_heads' if heads is not None else 'legacy_owner_version_proxy'),
            "conditional_eta_seconds": eta, "conditional_finish_at_utc": finish,
            "next_attempt_at_utc": cooldown.isoformat() if cooldown and cooling else None,
            "robots_origin_policy_pause": _robots_origin_pause(control, metrics, now, live),
            "client_min_interval_seconds": interval,
            "parallelism": _count(control.get("parallelism")) or _count(settings.get("parallelism")),
            "capacity_state": capacity_state,
            "target_requests_per_second": 1 / interval if interval else None,
            "last_window_requests_per_second": _number(sample.get("actual_requests_per_second")),
            "last_window_new_heads_per_second": _number(sample.get("new_heads_per_second")),
            "last_window_seconds": _number(sample.get("wall_seconds")),
            "last_window_ended_at_utc": (datetime.fromtimestamp(stamp_value, UTC).isoformat()
                                        if (stamp_value := _number(sample.get("ended_at_epoch"))) is not None and stamp_value <= 253402214400 else None),
            "capacity_stable_samples": _count(measured_capacity.get("stable_samples")),
            "runtime_capacity_observed": bool(control), "source_maximum_verified": False,
            "registered_bulk_endpoints": len(settings.get("cnyes_apis", [])) if isinstance(settings.get("cnyes_apis"), list) else 0,
            "temporarily_blocked_bulk_endpoints": endpoint_circuits.get(identifier, 0),
            "observed_bulk_rows_per_request_cap": 30 if identifier == "cnyes" and settings.get("cnyes_apis") else None,
            "capacity_workload_family": sample.get("workload_family") if isinstance(sample.get("workload_family"), str) and sample.get("workload_family") in {
                "cnyes_bulk", "rss_fulltext", "wordpress_posts", "article_pdf", "article_html", "mixed", "metadata", "unclassified"} else None,
            "minimum_observed_request_gap_seconds": _number(detail.get("minimum_request_start_gap_seconds")),
            "peak_http_inflight": _count(http_counts.get("peak_http_inflight")),
            "transport_errors": _count(http_counts.get("transport_errors")), "http_errors": http_errors,
            "official_hourly_quota": None, "official_daily_quota": None,
        })
    rank = {"running": 0, "backfilling": 1, "known_queue_clear": 2, 'finite_complete': 2, 'waiting_cohort': 2,
            "cooldown": 3, "attention": 4}
    providers.sort(key=lambda source: (not source["configured"], rank.get(source["state"], 5), -int(source["known_urls"] or 0), source["id"]))
    counts = _states(status.get("states")) if valid else {}
    known = _count(status.get("unique_articles")) if valid else None
    stored = _count(status.get("articles_with_stored_body")) if valid else None
    ratio = stored / known if known and stored is not None and stored <= known else None
    consistency = (known is not None and known == sum(counts.values())
                   and known == sum(source["known_urls"] or 0 for source in providers)
                   and stored is not None and stored <= known)
    if stored_counter_declared:
        consistency = consistency and stored_counter_supported and all(
            source['stored_body_urls'] is not None for source in providers) and stored == sum(
                source['stored_body_urls'] or 0 for source in providers)
    if valid and not consistency:
        health = "degraded"
        ratio = None
        for source in providers:
            source["conditional_eta_seconds"] = source["conditional_finish_at_utc"] = None
            source['stored_body_progress_ratio'] = None
    eligible = [source for source in providers if source["enabled"] and source["remaining_urls"]]
    etas = [source["conditional_eta_seconds"] for source in eligible]
    eligible_eta = max(etas) if consistency and live and etas and all(eta is not None for eta in etas) else None
    outcome_totals = Counter()
    if valid:
        for source in providers:
            outcome_totals.update(source['outcome_progress']['categories'])
    outcome_progress = ({'contract_version': NEWS_OUTCOME_CLASSIFICATION_VERSION,
        'categories': dict(outcome_totals), 'known_urls': known,
        'snapshot_consistent': consistency, 'grain': 'retained queue URL-head states',
        'origin_absence_is_not_all_paths_exhausted': True,
        'definitively_unavailable_urls': None, 'closure_progress_ratio': None,
        'all_history_complete': False} if valid else None)
    owner_requests = _object(status.get("http_requests_this_run"))
    requests = [_count(number) for number in owner_requests.values()]
    stages = []
    for key in ("owner_initialization", "policy_reconciliation", "metadata_inventory", "original_url_resolution", "original_url_routing", "article_dispatch",
                "discovery_registration", "batch_build", "parquet_compress_fsync",
                "receipt_hash_fsync", "acceptance_sqlite_commit", "status_write", "idle_wait"):
        stage = _object(_object(metrics.get("stages")).get(key))
        if stage:
            stages.append({"id": key, "count": _count(stage.get("count")),
                           "wall_seconds": _number(stage.get("wall_seconds")),
                           "cpu_seconds": _number(stage.get("cpu_seconds"))})
    for key in ("address_validation", "request_slot_wait", "transport", "article_extraction", "discovery_extraction"):
        measured = [_object(_object(_object(source).get("stages")).get(key)) for source in metric_providers.values()]
        measured = [stage for stage in measured if stage]
        if measured:
            stages.append({"id": key, **{
                field: sum(values) if all(value is not None for value in values) else None
                for field in ("count", "wall_seconds", "cpu_seconds")
                for values in [[(_count if field == "count" else _number)(stage.get(field)) for stage in measured]]
            }})
    resolution = None
    raw_resolution = _object(status.get('original_url_resolution')) if valid else {}
    if raw_resolution.get('contract_version') == 1:
        total, examined = (_count(raw_resolution.get(key)) for key in ('known_redirect_urls', 'examined_urls'))
        outcome_counts = {key: _count(_object(raw_resolution.get('counts')).get(key, 0))
                          for key in ('literal_original', 'opaque_original', 'unresolved')}
        linked = _count(raw_resolution.get('literal_links_to_queued_targets'))
        reconciled = (total is not None and examined is not None and examined <= total
                      and all(v is not None for v in outcome_counts.values()) and sum(outcome_counts.values()) == examined
                      and linked is not None and linked <= outcome_counts['literal_original'])
        owner_examined, owner_elapsed = _count(raw_resolution.get('owner_examined_urls')), _number(raw_resolution.get('owner_elapsed_seconds'))
        rate = (owner_examined / owner_elapsed if live and consistency and reconciled and owner_examined is not None
                and 256 <= owner_examined <= examined and owner_elapsed is not None and owner_elapsed >= 60 else None)
        remaining = total - examined if reconciled else None
        resolution = {'contract_version': 1, 'snapshot_consistent': reconciled,
                      'known_redirect_urls': total, 'examined_urls': examined, 'remaining_urls': remaining,
                      'literal_original_links': outcome_counts['literal_original'], 'opaque_links': outcome_counts['opaque_original'],
                      'unresolved_links': outcome_counts['unresolved'], 'links_to_queued_targets': linked,
                      'progress_ratio': examined / total if reconciled and total else None,
                      'owner_urls_per_second': rate, 'conditional_eta_seconds': remaining / rate if rate else None,
                      'body_completion_claimed': False, 'metadata_identity_verified': False,
                      'publisher_requests': 0}
    return {
        "schema_version": 1, "nonbody_source_outcome_progress_contract_version": 1,
        "worker_lifecycle_projection_contract_version": 1,
        "news_outcome_classification_contract_version": NEWS_OUTCOME_CLASSIFICATION_VERSION,
        "outcome_progress": outcome_progress,
        "provider_stored_body_counter_contract_version": 1 if stored_counter_supported else None,
        "cnyes_list_progress_projection_contract_version": 1,
        "cnyes_manifest_phase_projection_contract_version": 1,
        "original_source_inventory_contract_version": 1,
        "original_source_inventory": original_inventory,
        "source_policy_review_contract_version": 1,
        "source_reviews_state": source_reviews_state,
        "source_policy_review_counts": dict(Counter(source["source_policy_review"] for source in providers)),
        "generated_at_utc": now.isoformat(), "read_only": True,
        "production_control_possible": False, "health": health,
        "observed_at_utc": observed.isoformat() if observed else None,
        "status_age_seconds": max(0.0, age) if age is not None else None,
        "snapshot_consistent": consistency,
        "worker": {"activity": activity, "source_activity": source_activity,
                   "fresh": fresh and not initializing and not lifecycle_stopped,
                   "lifecycle": lifecycle,
                   "owner_elapsed_seconds": lifecycle['elapsed_seconds'] if initializing else elapsed,
                   "owner_cpu_seconds": None if initializing else _number(metrics.get("owner_cpu_seconds")),
                   "requests": sum(requests) if not initializing and requests and all(value is not None for value in requests) else None,
                   "response_bytes": _count(status.get("http_response_bytes_this_run")) if valid and not initializing else None},
        "acquisition": {"known_urls": known, "stored_body_articles": stored,
                        "complete_state_urls": counts.get("complete", 0) if valid else None,
                        "remaining_urls": sum(source["remaining_urls"] or 0 for source in providers if source["enabled"]) if valid else None,
                        "awaiting_authorization_urls": counts.get("awaiting_authorization", 0) if valid else None,
                        "inflight_urls": counts.get("inflight", 0) if valid else None,
                        "retry_urls": sum(counts.get(key, 0) for key in ("retry", "retry_cooldown")) if valid else None,
                        'growth_waiting_urls': counts.get('waiting_cohort', 0) if valid else None,
                        "blocked_urls": sum(count for key, count in counts.items() if key not in _READY_STATES | {"complete", 'waiting_cohort'}) if valid else None,
                        "progress_ratio": ratio, "text_bytes": _count(status.get("accepted_text_bytes")) if valid else None,
                        "eligible_eta_seconds": eligible_eta, "all_sources_eta_seconds": None,
                        "discovery_due": _count(status.get("discovery_pending")) if valid else None,
                        "discovery_errors": _count(status.get("discovery_errors")) if valid else None,
                        "metadata_partitions": _count(status.get("metadata_partitions")) if valid else None,
                        "metadata_association_rows": _count(status.get("metadata_association_rows")) if valid else None,
                        "metadata_errors": _count(status.get("metadata_partition_errors")) if valid else None,
                        "disabled_bulk_pages": _count(status.get("bulk_api_pages_disabled")) if valid else None,
                        "temporarily_blocked_bulk_pages": _count(status.get("bulk_api_pages_temporarily_blocked")) if valid else None},
        "states": counts, "providers": providers, "stages": stages,
        "original_url_resolution": resolution,
        "scope": {"historical_completeness_verified": False, "body_exported": False,
                  "source_reviews_grant_capture_permission": False,
                  "publisher_calls_on_page_view": False, "estimated_total_download_bytes": None,
                  "progress_basis": "stored bodies / discovered unique URLs; not all publisher history",
                  "rate_basis": "owner rate: new stored URL heads / owner wall time; completion ETA: consecutive current-owner completed windows including failures/waits when supported; explicit legacy owner fallback otherwise",
                  "eta_basis": "conditional per-source remaining frontier / owner rate; future discovery and blocked URLs excluded",
                  "stage_wall_time_basis": "worker durations can overlap; not additive end-to-end wall time",
                  "traffic_basis": "HTTP requests and response bytes since current owner start; not rolling-hour usage",
                  "metadata_basis": "accepted local FinLab/FinMind metadata plus publisher discovery; stock associations are not unique articles"},
    }

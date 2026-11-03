"""Bounded, read-only public projection of the canonical news owner's heartbeat.

Never open the writer queue, scan article files, import a collector or call a
publisher. Counts describe the discovered URL frontier, not all news history.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
from typing import Any, Mapping
from urllib.parse import urlsplit
from stockagent.data.news_disk_contract import BACKING_SAMPLE_MAX_AGE_SECONDS
from stockagent.data.news_worker_lifecycle import (
    LOCAL_SQLITE_WAIT_MAX_AGE_SECONDS, read_news_worker_lifecycle,
)
from stockagent.live.news_corpus_quality import (
    public_corpus_quality_statuses, public_sort_day_quality_status, reference_paths,
)
from stockagent.data.news_source_catalog import (
    configuration_paths, load_news_configuration, load_source_review_ledger,
    planning_review_path, source_role, classify_news_outcomes, recorded_review_questions,
    NEWS_OUTCOME_CLASSIFICATION_VERSION, NEWS_OUTCOME_STATES,
)


MAX_SNAPSHOT_BYTES = 4 * 1024 * 1024
# The 937-source canonical heartbeat already exceeds 4 MiB with valid bounded
# per-endpoint telemetry. Keep a finite input ceiling without dropping every
# discovered source when the owner crosses the old auxiliary-file limit.
MAX_STATUS_SNAPSHOT_BYTES = 16 * 1024 * 1024
MAX_AGE_SECONDS = 180


def _url_metadata_inbox(value):
    """Only publish bounded aggregate work, never private URLs or requests."""
    if (not isinstance(value, dict) or type(value.get('contract_version')) is not int
            or value['contract_version'] != 1 or value.get('available') is not True):
        return None
    names = ('request_files', 'pending_requests', 'targets_unexamined',
             'completed_unarchived', 'invalid_or_unreadable')
    if any(type(value.get(name)) is not int or value[name] < 0 for name in names):
        return None
    files, pending, targets, archived, invalid = (value[name] for name in names)
    if (files > 64 or files != pending + archived + invalid
            or not pending <= targets <= pending * 512):
        return None
    return {'contract_version': 1, **{name: value[name] for name in names}}


def _finlab_cnyes_priority(value, observed=None):
    """Bounded owner-receipt progress; never publish request IDs or URLs."""
    if (not isinstance(value, dict) or type(value.get('contract_version')) is not int
            or value['contract_version'] != 1 or value.get('body_completion_proven') is not False
            or value.get('association_completion_proven') is not False):
        return None
    recorded = _stamp(value.get('recorded_at_utc'))
    if not recorded or (observed is not None and recorded > observed + timedelta(seconds=5)):
        return None
    delta, examined, targets = (value.get(name) for name in ('raw_unique_delta', 'raw_hints_examined', 'targets'))
    if (any(type(item) is not int or not 0 <= item < 2**53 for item in (delta, examined, targets))
            or examined != min(delta, 4096) or targets > min(examined, 512)):
        return None
    state, checked, outcomes = (value.get(name) for name in ('request_state', 'owner_checked', 'outcomes'))
    if not isinstance(state, str):
        return None
    if state in {'unknown', 'unobserved'}:
        if not targets or checked is not None or outcomes is not None:
            return None
    elif state in {'not_applicable', 'processing', 'checked'}:
        names = ('metadata_registered', 'existing_head_preserved', 'source_contract_changed', 'other')
        if (type(checked) is not int or not 0 <= checked <= targets or not isinstance(outcomes, dict)
                or set(outcomes) != set(names)
                or any(type(outcomes.get(name)) is not int or outcomes[name] < 0 for name in names)
                or sum(outcomes[name] for name in names) != checked
                or (state == 'not_applicable' and (targets != 0 or checked != 0))
                or (state == 'processing' and (targets == 0 or checked >= targets))
                or (state == 'checked' and (targets == 0 or checked != targets))):
            return None
        outcomes = {name: outcomes[name] for name in names}
    else:
        return None
    # Optional in older snapshots. It is a subset of "other", not an
    # additional outcome or a claim that an article body was retrieved.
    unverified_origin = value.get('unverified_origin')
    if (unverified_origin is not None
            and (type(unverified_origin) is not int or unverified_origin < 0 or outcomes is None
                 or unverified_origin > outcomes['other'])):
        return None
    return {'contract_version': 1, 'raw_unique_delta': delta, 'raw_hints_examined': examined,
            'targets': targets, 'request_state': state, 'owner_checked': checked, 'outcomes': outcomes,
            'unverified_origin': unverified_origin,
            'recorded_at_utc': recorded.isoformat(),
            'body_completion_proven': False, 'association_completion_proven': False}


def _retained_handoff_plan_path(repo_root, config):
    spec = config.get('retained_link_handoff')
    if (not isinstance(spec, dict) or set(spec) != {'path', 'sha256'}
            or not isinstance(spec['path'], str) or Path(spec['path']).is_absolute()
            or not isinstance(spec['sha256'], str) or not re.fullmatch(r'[0-9a-f]{64}', spec['sha256'])):
        return None
    artifacts = (repo_root / 'artifacts').resolve()
    path = (repo_root / spec['path']).resolve()
    return path if path != artifacts and path.is_relative_to(artifacts) else None


def _retained_handoff_projection(repo_root, config, runtime):
    """Expose only aggregate local hints; never paths, URLs or body claims."""
    plan = None
    path = _retained_handoff_plan_path(repo_root, config)
    if path is not None:
        try:
            fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
            try:
                info = os.fstat(fd)
                if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_size > 2 << 20:
                    raise ValueError('invalid private handoff artifact')
                with os.fdopen(fd, 'rb', closefd=False) as stream:
                    raw = stream.read((2 << 20) + 1)
                if len(raw) > 2 << 20 or hashlib.sha256(raw).hexdigest() != config['retained_link_handoff']['sha256']:
                    raise ValueError('handoff digest mismatch')
            finally:
                os.close(fd)
            cert = json.loads(raw)
            count, requests = cert.get('candidate_count'), cert.get('requests')
            if (cert.get('contract') == 'cmnews_retained_link_handoff_v1'
                    and cert.get('source_root') == str((repo_root / config.get('root', '')).resolve())
                    and cert.get('target_identity_or_body_verified') is False
                    and type(count) is int and 0 < count < 2**53 and type(requests) is list
                    and 0 < len(requests) <= count):
                plan = {'provider': 'cmnews', 'candidates': count, 'requests': len(requests),
                        'source_and_policy_revalidation_required': True, 'body_verified': False}
        except (OSError, ValueError, TypeError, KeyError, RecursionError):
            pass
    projected = None
    if (isinstance(runtime, dict) and type(runtime.get('contract_version')) is int
            and runtime['contract_version'] == 1 and runtime.get('provider') == 'cmnews'
            and runtime.get('state') in {'validating', 'waiting_inbox', 'submitted', 'owner_checked', 'deferred'}
            and isinstance(runtime.get('plan_sha256'), str)
            and re.fullmatch(r'[0-9a-f]{64}', runtime['plan_sha256'])
            and runtime['plan_sha256'] == _object(config.get('retained_link_handoff')).get('sha256')
            and runtime.get('body_verified') is False):
        candidates, requests, submitted = (runtime.get(k) for k in ('candidates', 'requests', 'submitted_requests'))
        if (type(submitted) is int and submitted >= 0 and
                ((candidates is None and requests is None and submitted == 0)
                 or (type(candidates) is int and type(requests) is int and 0 < requests <= candidates < 2**53
                     and submitted <= requests))):
            reason = runtime.get('reason')
            reason = reason if reason in {'url_metadata_inbox_full', 'handoff_parent_head_changed',
                'handoff_batch_changed', 'handoff_target_or_policy_changed', 'handoff_proof_digest_changed',
                'handoff_certificate_digest_changed', 'local_input_unavailable'} else ('other' if reason else None)
            projected = {'provider': 'cmnews', 'state': runtime['state'], 'candidates': candidates,
                         'requests': requests, 'submitted_requests': submitted, 'reason': reason,
                         'body_verified': False}
            if runtime['state'] == 'owner_checked':
                outcomes = runtime.get('outcomes')
                names = ('metadata_registered', 'existing_head_preserved', 'source_contract_changed')
                if (not isinstance(outcomes, dict) or any(type(v) is not int or v < 0 for v in outcomes.values())
                        or sum(outcomes.values()) != candidates):
                    projected = None
                else:
                    projected['outcomes'] = {name: outcomes.get(name, 0) for name in names}
                    projected['outcomes']['other'] = sum(v for name, v in outcomes.items() if name not in names)
    return {'plan': plan, 'runtime': projected}


def _disk_admission(value, observed, now):
    """Fixed public byte/clock fields only; never storage paths or raw extras."""
    if not isinstance(value, dict) or type(value.get('contract_version')) is not int or value['contract_version'] != 1:
        return None
    checked = _stamp(value.get('checked_at_utc'))
    backing_state = value.get('physical_backing_state')
    if backing_state not in (None, 'available', 'not_required'):
        return None
    max_age = BACKING_SAMPLE_MAX_AGE_SECONDS if backing_state == 'available' else MAX_AGE_SECONDS
    fields = ('free_bytes', 'required_free_bytes', 'shortfall_bytes', 'automatic_recheck_seconds')
    if (not checked or not observed or not -5 <= (observed-checked).total_seconds() <= max_age
            or not -5 <= (now-checked).total_seconds() <= max_age
            or any(type(value.get(key)) is not int or not 0 <= value[key] <= 2**63-1 for key in fields)
            or value['automatic_recheck_seconds'] < 10
            or type(value.get('fetch_allowed')) is not bool
            or value['fetch_allowed'] != (value['free_bytes'] >= value['required_free_bytes'])
            or value['shortfall_bytes'] != max(0,value['required_free_bytes']-value['free_bytes'])):
        return None
    result = {'contract_version': 1, 'available': True, 'checked_at_utc': checked.isoformat(),
        **{key:value[key] for key in fields}, 'fetch_allowed':value['fetch_allowed']}
    if backing_state == 'available':
        result['valid_until_utc'] = (checked + timedelta(seconds=max_age)).isoformat()
    return result


def _disk_verification(value, observed, now):
    """Project an explicit unknown backing sample without inventing free bytes.

    Old or malformed v1 samples remain unavailable. This sideband cannot
    authorize a request or supply a completion estimate.
    """
    if not isinstance(value, dict) or type(value.get('contract_version')) is not int or value['contract_version'] != 1:
        return None
    checked = _stamp(value.get('checked_at_utc'))
    state = value.get('physical_backing_state')
    inflight = value.get('physical_probe_inflight')
    fields = ('free_bytes', 'shortfall_bytes', 'backing_free_bytes')
    required, interval = value.get('required_free_bytes'), value.get('automatic_recheck_seconds')
    if (not checked or not observed or not -5 <= (observed-checked).total_seconds() <= MAX_AGE_SECONDS
            or not -5 <= (now-checked).total_seconds() <= MAX_AGE_SECONDS
            or not isinstance(state, str) or state not in {'pending', 'unavailable', 'stale'}
            or type(inflight) is not bool or state == 'pending' and not inflight
            or any(key not in value or value[key] is not None for key in fields)
            or value.get('fetch_allowed') is not False
            or type(required) is not int or not 0 <= required <= 2**63-1
            or type(interval) is not int or not 10 <= interval <= 2**63-1):
        return None
    return {'contract_version': 1, 'state': state, 'probe_inflight': inflight,
            'checked_at_utc': checked.isoformat(), 'required_free_bytes': required,
            'automatic_recheck_seconds': interval}


_ID = re.compile(r"^(?:[a-z][a-z0-9_]{0,63}|unregistered:[a-z0-9.-]{1,253})$")
_DISCOVERY_ERROR_KINDS = frozenset({'feed', 'metadata_feed', 'index', 'sitemap', 'rss_fulltext',
                                    'wordpress_posts', 'cnyes_news', 'other'})
_DISCOVERY_ERROR_REASONS = frozenset({'http_401', 'http_403', 'http_404', 'http_410', 'http_429',
    'http_451', 'http_other', 'transport', 'unregistered_endpoint', 'invalid_feed',
    'invalid_feed_body', 'invalid_feed_identity', 'restricted_content', 'unusable_items',
    'pagination_no_progress', 'pagination_invalid', 'other'})
_CANDIDATE_WORK_KEYS = ('pending_unadmitted', 'ready', 'reserved', 'orphaned_reservation', 'uncertain_outcome')


def _candidate_diagnostic_progress(value, known_providers):
    """Publish validated counts only; private candidate requests stay local."""
    if (not isinstance(value, dict) or type(value.get('contract_version')) is not int
            or value['contract_version'] != 1 or value.get('available') is not True):
        return None
    names = ('request_files', 'pending_unadmitted', 'already_registered', 'invalid_or_unreadable')
    if any(type(value.get(key)) is not int or not 0 <= value[key] <= 16 for key in names):
        return None
    if value['request_files'] != sum(value[key] for key in names[1:]):
        return None
    states = _CANDIDATE_WORK_KEYS[1:]
    jobs, raw_providers, raw_unregistered = value.get('jobs'), value.get('providers'), value.get('unregistered')
    if not isinstance(jobs, dict) or not isinstance(raw_providers, dict) or len(raw_providers) > 4096:
        return None
    if any(type(jobs.get(key)) is not int or not 0 <= jobs[key] <= 2**63-1 for key in states):
        return None

    def clean_bucket(bucket):
        if (not isinstance(bucket, dict) or any(type(bucket.get(key)) is not int
                or not 0 <= bucket[key] <= 2**63-1 for key in _CANDIDATE_WORK_KEYS)):
            return None
        return {key: bucket[key] for key in _CANDIDATE_WORK_KEYS}

    unregistered = clean_bucket(raw_unregistered)
    if unregistered is None:
        return None
    providers = {}
    for identifier, raw in raw_providers.items():
        if (not isinstance(identifier, str) or not _ID.fullmatch(identifier)
                or identifier not in known_providers or (cleaned := clean_bucket(raw)) is None):
            return None
        providers[identifier] = cleaned
    totals = {key: unregistered[key] + sum(bucket[key] for bucket in providers.values())
              for key in _CANDIDATE_WORK_KEYS}
    if totals['pending_unadmitted'] != value['pending_unadmitted'] or any(totals[key] != jobs[key] for key in states):
        return None
    raw_local_retry = value.get('local_retry')
    local_retry = None
    retry_keys = ('request_io', 'quarantine', 'archive')
    if (isinstance(raw_local_retry, dict)
            and all(type(raw_local_retry.get(key)) is int and 0 <= raw_local_retry[key] <= value['request_files']
                    for key in retry_keys)
            and sum(raw_local_retry[key] for key in retry_keys) <= value['request_files']):
        local_retry = {key: raw_local_retry[key] for key in retry_keys}
    return {'contract_version': 1, **{key: value[key] for key in names},
            'jobs': {key: jobs[key] for key in states}, 'providers': providers, 'unregistered': unregistered,
            'local_retry': local_retry}


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
    recovery = None
    legacy_index = None
    legacy = _object(raw.get('legacy_original_index'))
    legacy_fields = ('expected_directories', 'registered_directories', 'examined_directories',
                     'candidate_originals', 'verified_originals', 'pending_originals', 'retry_originals',
                     'invalid_originals', 'other_originals')
    legacy_numbers = {key: _count(legacy.get(key)) for key in legacy_fields}
    if (type(legacy.get('contract_version')) is int and legacy['contract_version'] == 1
            and all(value is not None for value in legacy_numbers.values())
            and type(legacy.get('discovery_complete')) is bool
            and 0 < legacy_numbers['expected_directories'] <= 56
            and legacy_numbers['examined_directories'] <= legacy_numbers['registered_directories'] <= legacy_numbers['expected_directories']
            and legacy_numbers['candidate_originals'] == sum(legacy_numbers[key] for key in (
                'verified_originals', 'pending_originals', 'retry_originals', 'invalid_originals', 'other_originals'))
            and legacy['discovery_complete'] == (legacy_numbers['registered_directories'] == legacy_numbers['expected_directories']
                                                   and legacy_numbers['examined_directories'] == legacy_numbers['expected_directories'])):
        legacy_index = {'contract_version': 1, **legacy_numbers, 'discovery_complete': legacy['discovery_complete']}
    if type(raw.get('checkpoint_contract_version')) is int and raw['checkpoint_contract_version'] == 1:
        pending_feeds = _count(raw.get('pending_recovery_feeds'))
        pending_originals = _count(raw.get('pending_recovery_originals'))
        recovery_at = _stamp(raw.get('next_local_recovery_at_utc'))
        ready = raw.get('local_recovery_ready')
        if (pending_feeds is not None and pending_originals is not None and type(ready) is bool
                and pending_feeds <= numbers['registered_feeds'] and pending_originals >= pending_feeds
                and ((pending_feeds == 0 and pending_originals == 0 and not ready and raw.get('next_local_recovery_at_utc') is None)
                     or (pending_feeds > 0 and recovery_at is not None))):
            recovery = {'pending_feeds': pending_feeds, 'pending_originals': pending_originals,
                        'next_retry_ready': bool(recovery_at and recovery_at <= now),
                        'next_retry_at_utc': recovery_at.isoformat() if recovery_at and recovery_at.timestamp() > 0 else None}
    # The canonical queue's due=0 means ready, not a January 1970 appointment.
    return {'contract_version': raw['contract_version'], 'enabled': True, **numbers,
        'next_check_ready': bool(next_check and next_check <= now),
        'next_check_at_utc': next_check.isoformat() if next_check and next_check.timestamp() > 0 else None,
        'last_check_at_utc': last_check.isoformat() if last_check else None,
        'local_recovery': recovery,
        'legacy_original_index': legacy_index,
        'article_bodies_accepted': 0, 'overlapping_feed_items_not_unique_articles': True,
        'all_history_complete': False, 'finish_eta_utc': None}


def _object(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _fulltext_feed_inventory(settings: Mapping[str, Any], counts: Mapping[str, Any]) -> dict[str, Any] | None:
    """Registered feed scopes and current-owner counters, not history coverage.

    Read only the existing configuration/heartbeat. Response pages can overlap
    or fail validation; only the durable RSS workload counter proves new bodies.
    No endpoint URL, article, XML or arbitrary configuration text is exported.
    """
    def scopes(key):
        values = settings.get(key)
        return {value for value in values if isinstance(value, str) and value} if isinstance(values, list) else set()

    historical = scopes('historical_fulltext_feeds')
    roots = scopes('fulltext_feeds') - historical
    if not roots and not historical:
        return None
    months = []
    for scope in historical:
        try:
            match = re.fullmatch(r'/([1-9][0-9]{3})/(0[1-9]|1[0-2])/feed/?', urlsplit(scope).path)
        except ValueError:
            match = None
        if match:
            months.append('-'.join(match.groups()))
    return {
        'contract_version': 1,
        'registered_root_feeds': len(roots),
        'registered_historical_feeds': len(historical),
        'registered_monthly_feeds': len(months),
        'registered_other_historical_feeds': len(historical) - len(months),
        'first_registered_month': min(months) if months else None,
        'last_registered_month': max(months) if months else None,
        'owner_terminal_requests': _count(counts.get('http_endpoint_rss_fulltext_terminal_requests')),
        'owner_200_pages': _count(counts.get('http_endpoint_rss_fulltext_terminal_status_200')),
        'owner_304_pages': _count(counts.get('http_endpoint_rss_fulltext_terminal_status_304')),
        'owner_new_stored_url_heads': _count(counts.get('durable_workload_rss_fulltext_new_heads')),
        'registration_proves_history_complete': False,
        'response_pages_prove_accepted_bodies': False,
    }


def _body_state_coverage(value: Any, counts: Mapping[str, int], stored: int | None,
                         *, registered: bool, body_enabled: bool) -> dict[str, Any] | None:
    """Validate the joint state/body grain, never infer it from marginal totals.

    A retained body can have any subsequent check state. Legacy snapshots lack
    the joint counts; two independent totals cannot reconstruct that table.
    Unknown/private state names use the same finite public vocabulary as the
    original state projection, without leaking source errors or article data.
    """
    if (not isinstance(value, Mapping) or set(value) != {'missing', 'stored'} or stored is None):
        return None
    parts = {}
    for key in ('missing', 'stored'):
        raw = value[key]
        if (not isinstance(raw, Mapping) or any(not isinstance(state, str) or len(state) > 160
                or _count(number) is None for state, number in raw.items())):
            return None
        parts[key] = {state: number for state, number in _states(raw).items() if number}
    combined = Counter(parts['missing'])
    combined.update(parts['stored'])
    if (dict(combined) != {state: number for state, number in counts.items() if number}
            or sum(parts['stored'].values()) != stored):
        return None
    missing = sum(parts['missing'].values())
    outcomes = {name: classify_news_outcomes(parts[part], registered=registered, body_enabled=body_enabled)
                for name, part in (('missing_body_outcomes', 'missing'), ('retained_body_outcomes', 'stored'))}
    return {'contract_version': 1, 'missing_body_urls': missing, 'stored_body_urls': stored,
        'queued_missing_body_urls': sum(parts['missing'].get(state, 0) for state in _READY_STATES),
        'queued_stored_body_recheck_urls': sum(parts['stored'].get(state, 0) for state in _READY_STATES),
        'retained_body_noncomplete_urls': stored - parts['stored'].get('complete', 0),
        'missing_body_states': parts['missing'], 'retained_body_states': parts['stored'],
        **{key: {'categories': item['categories'], 'known_urls': item['known_urls']} for key, item in outcomes.items()},
        'grain': 'disjoint URL heads by retained body presence and latest check state',
        'retained_bodies_are_not_missing': True, 'all_history_complete': False}


def _publisher_reference_coverage(value: Any, known: int | None, stored: int | None,
                                  coverage: dict | None) -> dict[str, Any] | None:
    """Metadata links are a disjoint URL resolution, never stored body heads."""
    raw = _object(value)
    if (known is None or stored is None or coverage is None
            or type(raw.get('contract_version')) is not int or raw['contract_version'] != 1
            or type(raw.get('new_article_bodies')) is not int or raw['new_article_bodies'] != 0
            or raw.get('raw_alias_response_retained') is not False or raw.get('all_history_complete') is not False):
        return None
    fields = ('registered_urls', 'verified_reference_urls', 'inactive_reference_urls')
    counts = {key: _count(raw.get(key)) for key in fields}
    if (any(number is None for number in counts.values())
            or counts['registered_urls'] > known
            or counts['verified_reference_urls'] + counts['inactive_reference_urls'] != counts['registered_urls']
            or counts['verified_reference_urls'] > coverage['missing_body_states'].get('unsupported_article_path', 0)):
        return None
    resolved = stored + counts['verified_reference_urls']
    if resolved > known:
        return None
    return {'contract_version': 1, **counts, 'stored_body_urls': stored,
        'resolved_url_heads': resolved, 'unresolved_url_heads': known - resolved,
        'resolved_url_progress_ratio': resolved / known if known else None,
        'new_article_bodies': 0, 'raw_alias_response_retained': False, 'all_history_complete': False}


def _native_lookup_recheck_progress(value: Any) -> dict[str, Any] | None:
    """Allow only the bounded local journal count, never URLs or completion claims."""
    raw = _object(value)
    pending = raw.get('pending_records_lower_bound')
    exact = raw.get('count_exact')
    if (type(raw.get('contract_version')) is not int or raw['contract_version'] != 1
            or type(pending) is not int or type(exact) is not bool
            or not 0 <= pending <= 2049 or exact != (pending < 2049)
            or raw.get('records_are_article_completion') is not False
            or raw.get('source_lookup_performed') is not False):
        return None
    return {'contract_version': 1, 'pending_records_lower_bound': pending, 'count_exact': exact,
            'records_are_article_completion': False, 'source_lookup_performed': False}


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


def _pending_source_rechecks(value: Any, total: int, observed: datetime | None) -> dict[str, Any] | None:
    """Project unresolved source evidence without exposing origins or URLs."""
    raw = _object(value)
    if (type(raw.get('contract_version')) is not int or raw['contract_version'] != 1
            or type(raw.get('available')) is not bool):
        return None
    fields = ('unverified_missing_body_urls', 'due_missing_body_urls')
    if not raw['available']:
        if any(raw.get(key) is not None for key in (*fields, 'next_probe_at_utc')):
            return None
        return {'contract_version': 1, 'available': False, **dict.fromkeys(fields),
                'next_probe_at_utc': None}
    counts = {key: _count(raw.get(key)) for key in fields}
    if (any(value is None for value in counts.values())
            or not 0 <= counts['due_missing_body_urls'] <= counts['unverified_missing_body_urls'] <= total):
        return None
    next_probe = _stamp(raw.get('next_probe_at_utc'))
    if counts['unverified_missing_body_urls']:
        if observed is None or next_probe is None or (next_probe - observed).total_seconds() > 86400:
            return None
        if counts['due_missing_body_urls'] and (next_probe - observed).total_seconds() > 5:
            return None
        if not counts['due_missing_body_urls'] and (next_probe - observed).total_seconds() < -5:
            return None
    elif raw.get('next_probe_at_utc') is not None:
        return None
    return {'contract_version': 1, 'available': True, **counts,
            'next_probe_at_utc': next_probe.isoformat() if next_probe else None}


def _body_request_scope_progress(value: Any, work: Mapping[str, Any] | None,
                                 observed: datetime | None, now: datetime) -> dict[str, Any] | None:
    """Counts only: no private URL/host, gate permission or recovery inference."""
    raw = _object(value)
    if (type(raw.get('contract_version')) is not int or raw['contract_version'] != 1
            or type(raw.get('available')) is not bool or work is None
            or raw.get('source_recovery_verified') is not False):
        return None
    keys = ('active_missing_body_urls', 'source_paused_missing_body_urls', 'other_missing_body_urls',
            'policy_paused_missing_body_urls', 'gateway_paused_missing_body_urls')
    if not raw['available']:
        if any(raw.get(key) is not None for key in (*keys, 'next_source_probe_at_utc')):
            return None
        return {'contract_version': 1, 'available': False, **dict.fromkeys(keys),
                'next_source_probe_at_utc': None, 'source_probe_ready': None, 'source_recovery_verified': False}
    counts = {key: _count(raw.get(key)) for key in keys}
    if any(value is None for value in counts.values()):
        return None
    total, paused, other, policy, gateway = (counts[key] for key in keys)
    if (total != work['active_missing_body_urls'] or total > 2048 or paused + other != total
            or not max(policy, gateway) <= paused <= policy + gateway):
        return None
    deadline = _stamp(raw.get('next_source_probe_at_utc'))
    if paused:
        if observed is None or deadline is None or not -5 <= (deadline - observed).total_seconds() <= 86400:
            return None
    elif raw.get('next_source_probe_at_utc') is not None:
        return None
    return {'contract_version': 1, 'available': True, **counts,
            'next_source_probe_at_utc': deadline.isoformat() if deadline else None,
            'source_probe_ready': deadline <= now if deadline else False,
            'pending_source_rechecks': _pending_source_rechecks(
                raw.get('pending_source_rechecks'), total, observed),
            'source_recovery_verified': False}


def _cnyes_public_index_progress(value: Any, *, now: datetime) -> dict[str, Any] | None:
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
    reconciling = None
    if 'dates_reconciling_page_membership' in raw:
        reconciling = _count(raw['dates_reconciling_page_membership'])
        if (reconciling is None or reconciling > dates
                or counts['dates_all_reported_pages_observed'] + reconciling > dates):
            return None
    recovery = None
    if type(raw.get('checkpoint_contract_version')) is int and raw['checkpoint_contract_version'] == 1:
        pending = _count(raw.get('pending_local_originals'))
        retry_at = _stamp(raw.get('next_local_recovery_at_utc'))
        ready = raw.get('local_recovery_ready')
        if (pending is not None and type(ready) is bool
                and ((pending == 0 and not ready and raw.get('next_local_recovery_at_utc') is None)
                     or (pending > 0 and retry_at is not None))):
            recovery = {'pending_originals': pending,
                        'next_retry_ready': bool(retry_at and retry_at <= now),
                        'next_retry_at_utc': retry_at.isoformat() if retry_at and retry_at.timestamp() > 0 else None}
    return {'contract_version': 1, 'enabled': True, **counts,
            'dates_reconciling_page_membership': reconciling,
            'local_recovery': recovery,
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


def _cnyes_index_source_gate(value: Any, now: datetime) -> dict[str, Any] | None:
    """Project only the owner's bounded index-scope gate, not private URLs."""
    parent = _object(value)
    if 'source_gate' not in parent:
        return None  # Legacy snapshots did not contain this observation.
    result = {'state': 'unknown', 'next_probe_at_utc': None,
              'source_recovery_requires_success': True}
    raw = _object(parent['source_gate'])
    state = raw.get('state')
    if (type(raw.get('contract_version')) is not int or raw['contract_version'] != 1
            or state not in {'waiting', 'rechecking', 'clear', 'unknown'}
            or raw.get('source_recovery_requires_success') is not True):
        return result
    probe = _stamp(raw.get('next_probe_at_utc'))
    if state == 'waiting':
        if probe is None:
            return result
        # The deadline permits a source probe; it does not prove recovery.
        return {**result, 'state': 'waiting' if probe > now else 'rechecking',
                'next_probe_at_utc': probe.isoformat() if probe > now else None}
    if raw.get('next_probe_at_utc') is not None:
        return result
    return {**result, 'state': state}


def _cnyes_manifest_last_page_gate(value: Any, now: datetime) -> dict[str, Any]:
    """Project only the final page's own bounded job state, never its URL."""
    raw = _object(_object(value).get('last_page_gate'))
    result = {'state': 'unknown', 'next_probe_at_utc': None, 'recovery_requires_success': True}
    state = raw.get('state')
    if (type(raw.get('contract_version')) is not int or raw['contract_version'] != 1
            or state not in {'waiting', 'rechecking', 'ready', 'unknown'}
            or raw.get('recovery_requires_success') is not True):
        return result
    if state == 'waiting':
        probe = _stamp(raw.get('next_probe_at_utc'))
        if probe is None:
            return result
        return {**result, 'state': 'waiting' if probe > now else 'rechecking',
                'next_probe_at_utc': probe.isoformat() if probe > now else None}
    if raw.get('next_probe_at_utc') is not None:
        return result
    return {**result, 'state': state}


def _cnyes_manifest_partial_page_gate(value: Any, now: datetime, remaining: int) -> dict[str, Any]:
    """Project bounded request-local failure evidence, without source URLs."""
    raw = _object(_object(value).get('partial_page_gate'))
    result = {'state': 'unknown', 'failed_pages': None, 'next_probe_at_utc': None,
              'recovery_requires_success': True, 'scope': 'observed_request_local_failures_only'}
    state, failed = raw.get('state'), _count(raw.get('failed_pages'))
    if (type(raw.get('contract_version')) is not int or raw['contract_version'] != 1
            or state not in {'waiting', 'rechecking', 'none_observed', 'unknown'}
            or raw.get('recovery_requires_success') is not True
            or raw.get('scope') != 'observed_request_local_failures_only'):
        return result
    if state == 'unknown':
        return result
    if failed is None or failed > remaining or (state == 'none_observed') != (failed == 0):
        return result
    if state == 'waiting':
        probe = _stamp(raw.get('next_probe_at_utc'))
        if probe is None:
            return result
        return {**result, 'state': 'waiting' if probe > now else 'rechecking',
                'failed_pages': failed, 'next_probe_at_utc': probe.isoformat() if probe > now else None}
    if raw.get('next_probe_at_utc') is not None:
        return result
    return {**result, 'state': state, 'failed_pages': failed}


def _cnyes_manifest_phase_progress(value: Any, metrics: Mapping[str, Any],
                                   now: datetime, live: bool, current_owner: Any,
                                   *, provider_waiting: bool = False) -> dict[str, Any] | None:
    """A source-reported finite metadata phase, separate from all-history ETA.

    Page views only validate/project the existing owner snapshot. Recompute
    ratios and scenario times from typed counts; never open the queue, read
    publisher originals, extrapolate unseen dates, or expose scope URLs.
    """
    index = _cnyes_public_index_progress(value, now=now)
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
    revisions = None
    if 'dates_reconciling_page_membership' in raw:
        revisions = _count(raw['dates_reconciling_page_membership'])
        if (revisions is None or revisions > planned
                or index['dates_reconciling_page_membership'] is None
                or revisions > index['dates_reconciling_page_membership'] or (known and revisions)):
            return None
    elif index['dates_reconciling_page_membership'] not in (None, 0):
        return None
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
    source_gate = _cnyes_index_source_gate(value, now)
    last_page_gate = _cnyes_manifest_last_page_gate(raw, now) if remaining == 1 else None
    partial_page_gate = _cnyes_manifest_partial_page_gate(raw, now, remaining) if remaining is not None and remaining > 1 else None
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
    elif provider_waiting or source_gate is not None and source_gate['state'] == 'waiting':
        # A current provider-wide scheduler gate makes an old measured rate
        # or an index-specific gate makes an old measured rate unsuitable as
        # an active completion clock. Keep verified counts.
        forecast['state'] = 'provider_waiting'
    elif source_gate is not None and source_gate['state'] == 'rechecking':
        forecast['state'] = 'source_rechecking'
    elif source_gate is None or source_gate['state'] == 'unknown':
        # Legacy snapshots have no index-scope gate proof. Keep their counts,
        # but do not treat the missing observation as a recovered source.
        forecast['state'] = 'source_scope_unknown'
    elif last_page_gate is not None and last_page_gate['state'] != 'ready':
        forecast['state'] = {'waiting': 'last_page_waiting', 'rechecking': 'last_page_rechecking',
                             'unknown': 'last_page_scope_unknown'}[last_page_gate['state']]
    elif partial_page_gate is not None and partial_page_gate['state'] != 'none_observed':
        forecast['state'] = {'waiting': 'partial_page_waiting', 'rechecking': 'partial_page_rechecking',
                             'unknown': 'partial_page_scope_unknown'}[partial_page_gate['state']]
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
        'dates_reconciling_page_membership': revisions,
        'totals_known': known, 'source_reported_total_pages': total_pages,
        'source_reported_total_metadata_rows': total_rows, 'remaining_pages': remaining,
        'progress_ratio': pages / total_pages if known else None, 'phase_complete': complete,
        'metadata_only': True, 'body_completion_claimed': False, 'all_history_complete': False,
        'source_gate': source_gate, 'last_page_gate': last_page_gate,
        'partial_page_gate': partial_page_gate, 'forecast': forecast}


def _cnyes_history_list_progress(value: Any, gaps: Any) -> dict[str, Any] | None:
    """Only current-contract scope counts; scopes can overlap across dates."""
    raw = _object(value)
    states = raw.get('current_source_contract_scope_states')
    complete = ('list_complete', 'list_complete_by_page_ID_equivalence', 'partition_list_complete')
    waiting = ('waiting_page_evidence', 'waiting_original_page_recheck', 'waiting_child_evidence')
    inconsistent = ('inconsistent_source_snapshot', 'inconsistent_source_identity_or_date',
                    'inconsistent_partition_evidence')
    # These are actual canonical producer outcomes, not unknown states that
    # may be silently coerced into complete. A source conflict must remain
    # visible without hiding all otherwise verified historical scopes.
    allowed = complete + waiting + inconsistent + ('inconsistent_original_page_ID_reference',
                                                    'source_page_cap_exceeded', 'invalid_page_evidence',
                                                    'invalid_scope_evidence')
    retired = _count(raw.get('retired_gap_receipts'))
    if (type(raw.get('contract_version')) is not int or raw['contract_version'] != 1
            or not isinstance(states, Mapping) or set(states) - set(allowed)
            or retired is None or raw.get('list_coverage_is_not_body_coverage') is not True
            or raw.get('all_history_complete') is not False or raw.get('finish_eta_utc') is not None):
        return None
    counts = {key: _count(states.get(key, 0)) for key in allowed}
    if any(value is None for value in counts.values()) or sum(counts.values()) > 2**53 - 1:
        return None
    legacy = raw.get('legacy_metadata_scans')
    local_legacy = None
    if isinstance(legacy, Mapping):
        local_legacy = {}
        for kind in ('mismatch', 'unavailable', 'empty'):
            item = legacy.get(kind)
            if not isinstance(item, Mapping):
                item = {}
            scan_state = item.get('scan_state')
            if type(scan_state) is not str or scan_state not in ('not_started', 'scanning', 'complete', 'unknown'):
                scan_state = 'unknown'
            problem = item.get('checkpoint_problem')
            if (scan_state != 'unknown' or type(problem) is not str
                    or problem not in ('invalid_checkpoint', 'unsupported_version', 'audit_conflict')):
                problem = None
            local_legacy[kind] = {'scan_state': scan_state, 'checkpoint_problem': problem}
    recovery = raw.get('overflow_retained_recovery')
    local_recovery = None
    if isinstance(recovery, Mapping):
        pending = _count(recovery.get('pending_retained_pages'))
        retries = _count(recovery.get('local_retry_pages')) if 'local_retry_pages' in recovery else None
        scan_state = recovery.get('scan_state')
        if (type(recovery.get('contract_version')) is int and recovery['contract_version'] == 1
                and scan_state in ('not_started', 'scanning', 'complete', 'unknown')
                and pending is not None and ('local_retry_pages' not in recovery or retries is not None)
                and recovery.get('local_raw_only') is True
                and recovery.get('category_gap_completion_claimed') is False
                and type(recovery.get('publisher_requests')) is int and recovery['publisher_requests'] == 0):
            problem = recovery.get('checkpoint_problem')
            if (scan_state != 'unknown' or type(problem) is not str
                    or problem not in ('invalid_checkpoint', 'unsupported_version', 'audit_conflict')):
                problem = None
            local_recovery = {'contract_version': 1, 'scan_state': scan_state,
                'pending_retained_pages': pending, 'local_retry_pages': retries, 'checkpoint_problem': problem,
                'local_raw_only': True,
                'category_gap_completion_claimed': False, 'publisher_requests': 0}
    nonzero = raw.get('unobserved_nonzero_retained_scan')
    local_nonzero = None
    if isinstance(nonzero, Mapping):
        scan_state = nonzero.get('scan_state')
        checked = _count(nonzero.get('checked_steps'))
        queued = _count(nonzero.get('queued_candidate_pages'))
        counts_valid = (checked is not None and queued is not None and queued <= checked
                        or scan_state == 'unknown' and nonzero.get('checked_steps') is None
                        and nonzero.get('queued_candidate_pages') is None)
        if (type(nonzero.get('contract_version')) is int and nonzero['contract_version'] == 1
                and scan_state in ('not_started', 'scanning', 'complete', 'unknown') and counts_valid
                and nonzero.get('local_raw_only') is True and nonzero.get('all_history_complete') is False
                and type(nonzero.get('publisher_requests')) is int and nonzero['publisher_requests'] == 0):
            problem = nonzero.get('checkpoint_problem')
            if (scan_state != 'unknown' or type(problem) is not str
                    or problem not in ('invalid_checkpoint', 'unsupported_version', 'audit_conflict')):
                problem = None
            local_nonzero = {'contract_version': 1, 'scan_state': scan_state, 'checked_steps': checked,
                'queued_candidate_pages': queued, 'checkpoint_problem': problem, 'local_raw_only': True,
                'all_history_complete': False, 'publisher_requests': 0}
    accepted = raw.get('accepted_body_batch_scan')
    local_accepted = None
    if isinstance(accepted, Mapping):
        scan_state = accepted.get('scan_state')
        scanned = _count(accepted.get('scanned_batches'))
        indexed = _count(accepted.get('indexed_candidates'))
        local_retries = _count(accepted.get('local_retry_batches'))
        local_missing = _count(accepted.get('local_missing_retry_batches'))
        local_invalid = _count(accepted.get('local_invalid_retry_batches'))
        remaining_frontier = _count(accepted.get('remaining_frontier_batches'))
        estimated_frontier = _count(accepted.get('estimated_frontier_batches'))
        raw_migration = accepted.get('local_invalid_migration_state')
        migration = (None if raw_migration is None else raw_migration
                     if type(raw_migration) is str and raw_migration in ('not_started', 'scanning', 'complete', 'unknown')
                     else 'unknown')
        retries_valid = ((accepted.get('local_retry_batches') is None or local_retries is not None)
                         and (accepted.get('local_missing_retry_batches') is None or local_missing is not None)
                         and (accepted.get('local_invalid_retry_batches') is None or local_invalid is not None))
        counts_valid = ((scanned is not None and indexed is not None)
                        or (scan_state == 'unknown' and accepted.get('scanned_batches') is None
                            and accepted.get('indexed_candidates') is None))
        if (type(accepted.get('contract_version')) is int and accepted['contract_version'] == 1
                and scan_state in ('not_started', 'scanning', 'complete', 'local_retry_wait', 'unknown')
                and counts_valid and retries_valid and accepted.get('local_raw_only') is True
                and accepted.get('all_history_complete') is False
                and type(accepted.get('publisher_requests')) is int and accepted['publisher_requests'] == 0):
            problem = accepted.get('checkpoint_problem')
            if scan_state != 'unknown' or problem not in ('invalid_checkpoint', 'unsupported_version', 'audit_conflict'):
                problem = None
            if scan_state == 'complete':
                if local_retries or local_missing:
                    scan_state = 'local_retry_wait'
                elif migration in ('not_started', 'scanning'):
                    scan_state = 'scanning'
                elif migration == 'unknown':
                    scan_state = 'unknown'
                elif local_invalid:
                    scan_state = 'local_retry_wait'
                elif migration is None:
                    scan_state = 'unknown'
            if (scan_state == 'unknown' or scanned is None or remaining_frontier is None
                    or estimated_frontier is None or scanned + remaining_frontier != estimated_frontier
                    or scan_state == 'not_started' and scanned != 0):
                remaining_frontier = estimated_frontier = None
            local_accepted = {'contract_version': 1, 'scan_state': scan_state, 'checkpoint_problem': problem, 'scanned_batches': scanned,
                'indexed_candidates': indexed, 'local_retry_batches': local_retries,
                'local_missing_retry_batches': local_missing, 'local_invalid_retry_batches': local_invalid,
                'local_invalid_migration_state': migration,
                'remaining_frontier_batches': remaining_frontier,
                'estimated_frontier_batches': estimated_frontier,
                'local_raw_only': True,
                'all_history_complete': False, 'publisher_requests': 0}
    integrity = raw.get('completed_integrity_scan')
    local_integrity = None
    if isinstance(integrity, Mapping):
        scan_state = integrity.get('scan_state')
        checked = _count(integrity.get('checked_scopes'))
        affected = _count(integrity.get('affected_scopes'))
        counts_valid = (checked is not None and affected is not None
                        or scan_state == 'unknown' and integrity.get('checked_scopes') is None
                        and integrity.get('affected_scopes') is None)
        if (type(integrity.get('contract_version')) is int and integrity['contract_version'] == 1
                and scan_state in ('not_started', 'scanning', 'complete', 'unknown') and counts_valid
                and integrity.get('local_metadata_only') is True and integrity.get('all_history_complete') is False
                and type(integrity.get('publisher_requests')) is int and integrity['publisher_requests'] == 0):
            local_integrity = {'contract_version': 1, 'scan_state': scan_state, 'checked_scopes': checked,
                'affected_scopes': affected, 'local_metadata_only': True,
                'all_history_complete': False, 'publisher_requests': 0}
    original = raw.get('original_envelope_audit')
    local_original = None
    if isinstance(original, Mapping):
        scan_state = original.get('scan_state')
        checked = _count(original.get('checked_pages'))
        verified = _count(original.get('verified_pages'))
        out_of_scope = _count(original.get('out_of_scope_pages'))
        mismatched = _count(original.get('mismatched_pages'))
        unverified = _count(original.get('unverified_pages'))
        counts_valid = (all(item is not None for item in (checked, verified, out_of_scope, mismatched, unverified))
                        and verified + out_of_scope + mismatched + unverified == checked
                        or scan_state == 'unknown' and original.get('checked_pages') is None
                        and all(original.get(k) is None for k in ('verified_pages', 'out_of_scope_pages',
                                                                   'mismatched_pages', 'unverified_pages')))
        if (type(original.get('contract_version')) is int and original['contract_version'] == 1
                and scan_state in ('not_started', 'scanning', 'complete', 'unknown') and counts_valid
                and original.get('local_raw_only') is True and original.get('all_history_complete') is False
                and type(original.get('publisher_requests')) is int and original['publisher_requests'] == 0):
            problem = original.get('checkpoint_problem')
            if scan_state != 'unknown' or problem not in ('invalid_checkpoint', 'unsupported_version', 'audit_conflict'):
                problem = None
            local_original = {'contract_version': 1, 'scan_state': scan_state, 'checked_pages': checked,
                'verified_pages': verified, 'out_of_scope_pages': out_of_scope,
                'mismatched_pages': mismatched, 'unverified_pages': unverified, 'local_raw_only': True,
                'all_history_complete': False, 'publisher_requests': 0, 'checkpoint_problem': problem}
            local_original['retry_pending_pages'] = _count(original.get('retry_pending_pages'))
            next_audit = _stamp(original.get('next_audit_at_utc')) if scan_state == 'complete' else None
            local_original['next_audit_at_utc'] = next_audit.isoformat() if next_audit else None
    priority = raw.get('original_priority_admission')
    local_priority = None
    if isinstance(priority, Mapping):
        priority_state = priority.get('state')
        target = _count(priority.get('targets'))
        processed = _count(priority.get('processed_targets'))
        stale = _count(priority.get('stale_targets'))
        added = _count(priority.get('enqueued_pages'))
        existing = _count(priority.get('existing_pages'))
        counts_valid = (all(item is not None for item in (target, processed, stale, added, existing))
                        and 0 < target and stale <= processed <= target
                        and (priority_state != 'complete' or processed == target)
                        or priority_state == 'blocked' and all(priority.get(key) is None for key in
                            ('targets', 'processed_targets', 'stale_targets', 'enqueued_pages', 'existing_pages')))
        problem = priority.get('problem')
        if problem not in ('source_unavailable', 'plan_unavailable_or_invalid', 'source_identity_changed',
                           'invalid_checkpoint', 'checkpoint_archive_conflict'):
            problem = None
        if (type(priority.get('contract_version')) is int and priority['contract_version'] == 1
                and priority_state in ('blocked', 'scanning', 'complete') and counts_valid
                and type(priority.get('publisher_requests')) is int and priority['publisher_requests'] == 0
                and priority.get('accepted_as_originals') is False and priority.get('all_history_complete') is False
                and (priority_state == 'blocked' and problem is not None
                     or priority_state != 'blocked' and priority.get('problem') is None)):
            local_priority = {'contract_version': 1, 'state': priority_state, 'problem': problem,
                'targets': target, 'processed_targets': processed, 'stale_targets': stale,
                'enqueued_pages': added, 'existing_pages': existing, 'publisher_requests': 0,
                'accepted_as_originals': False, 'all_history_complete': False}
    priority_audit = raw.get('original_priority_audit')
    local_priority_audit = None
    if isinstance(priority_audit, Mapping) and local_priority and local_priority['state'] != 'blocked':
        keys = ('awaiting_pages', 'verified_pages', 'mismatched_pages', 'unverified_pages',
                'outside_pages', 'changed_pages')
        version = priority_audit.get('contract_version')
        if type(version) is int and version == 2:
            keys += ('recovered_id_pages',)
        audit_counts = {key: _count(priority_audit.get(key)) for key in keys}
        tracked = _count(priority_audit.get('tracked_pages'))
        tracking_complete = priority_audit.get('tracking_complete')
        expected = local_priority['enqueued_pages'] + local_priority['existing_pages']
        if (type(version) is int and version in (1, 2)
                and tracked is not None and all(value is not None for value in audit_counts.values())
                and sum(audit_counts.values()) == tracked and type(tracking_complete) is bool
                and tracking_complete == (tracked == expected)
                and priority_audit.get('local_raw_only') is True
                and type(priority_audit.get('publisher_requests')) is int and priority_audit['publisher_requests'] == 0
                and priority_audit.get('all_history_complete') is False):
            local_priority_audit = {'contract_version': version, 'tracked_pages': tracked,
                'tracking_complete': tracking_complete, **audit_counts, 'local_raw_only': True,
                'publisher_requests': 0, 'all_history_complete': False}
    return {'contract_version': 1, 'current_source_contract_scope_states': counts,
            'scope_outcome_projection_contract_version': 1,
            'verified_list_scopes': sum(counts[key] for key in complete),
            'waiting_list_scopes': sum(counts[key] for key in waiting),
            'inconsistent_source_scopes': sum(counts[key] for key in inconsistent),
            'invalid_page_evidence_scopes': counts['invalid_page_evidence'],
            'invalid_scope_evidence_scopes': counts['invalid_scope_evidence'],
            'original_page_reference_scopes': counts['inconsistent_original_page_ID_reference'],
            'page_cap_exceeded_scopes': counts['source_page_cap_exceeded'],
            'retired_gap_receipts': retired, 'unresolved_category_scopes': _cnyes_category_gap_count(gaps),
            'legacy_metadata_scans': local_legacy,
            'overflow_retained_recovery': local_recovery,
            'unobserved_nonzero_retained_scan': local_nonzero,
            'accepted_body_batch_scan': local_accepted,
            'completed_integrity_scan': local_integrity,
            'original_envelope_audit': local_original,
            'original_priority_admission': local_priority,
            'original_priority_audit': local_priority_audit,
            'list_coverage_is_not_body_coverage': True, 'all_history_complete': False,
            'finish_eta_utc': None, 'progress_ratio': None}


def _read(path: Path, *, max_bytes: int = MAX_SNAPSHOT_BYTES) -> dict[str, Any]:
    from stockagent.live.dashboard_updates import read_json_snapshot
    value = read_json_snapshot(path, max_bytes=max_bytes)
    return value if isinstance(value, dict) else {}


def _read_news_status(path: Path) -> tuple[dict[str, Any], int]:
    """Retry one proven file-generation race, not malformed/stale telemetry.

    The shared reader correctly refuses bytes whose descriptor/path metadata
    changed during decode. A canonical atomic heartbeat replacement can cause
    that refusal even though both generations were complete. Retry the latest
    generation once only when our own metadata fences prove a change. Never
    return the previously cached generation or weaken size/clock validation.
    Auxiliary reads and other dashboards retain the shared reader contract.
    """
    from stockagent.live.dashboard_updates import metadata_signature
    try:
        before = metadata_signature(path.stat())
    except OSError:
        return _read(path, max_bytes=MAX_STATUS_SNAPSHOT_BYTES), 1
    value = _read(path, max_bytes=MAX_STATUS_SNAPSHOT_BYTES)
    if value:
        return value, 1
    try:
        changed = before != metadata_signature(path.stat())
    except OSError:
        changed = False
    if changed:
        return _read(path, max_bytes=MAX_STATUS_SNAPSHOT_BYTES), 2
    return value, 1


def _count(value: Any) -> int | None:
    return value if type(value) is int and 0 <= value <= 2**53 - 1 else None


def _source_catalog_errors(status: Mapping[str, Any], source: str) -> int | None:
    """Old or inconsistent snapshots do not establish a healthy catalog."""
    inventory = _object(status.get('metadata_inventory'))
    version = inventory.get('contract_version')
    if type(version) is not int or version not in ({5, 6, 7} if source == 'finmind' else {6, 7}):
        return None
    count = _count(inventory.get(source + '_catalog_errors'))
    total = _count(status.get('metadata_partition_errors'))
    other = _count(inventory.get(('finlab' if source == 'finmind' else 'finmind') + '_catalog_errors'))
    if (version == 6 and count is not None and other is not None
            and total is not None and count + other > total):
        return None
    return count if count is not None and total is not None and count <= total else None


def _discovery_error_rollup(value, expected_total):
    """Validate aggregate observations only; never expose URLs or raw errors."""
    raw = _object(value)
    state = raw.get('state')
    if type(raw.get('contract_version')) is not int or raw['contract_version'] != 1:
        return None
    if state == 'unavailable':
        if any(raw.get(key) is not None for key in ('scanned_rows', 'active_errors', 'providers')):
            return None
        return {'state': state, 'scanned_rows': None, 'active_errors': None, 'providers': None}
    scanned = _count(raw.get('scanned_rows'))
    if scanned is None:
        return None
    if state == 'building':
        if raw.get('active_errors') is not None or raw.get('providers') is not None:
            return None
        return {'state': state, 'scanned_rows': scanned, 'active_errors': None, 'providers': None}
    total = _count(raw.get('active_errors'))
    entries = raw.get('providers')
    if state != 'ready' or expected_total is None or total != expected_total or not isinstance(entries, dict):
        return None
    providers = {}
    for identifier, value in entries.items():
        if not isinstance(identifier, str) or not _ID.fullmatch(identifier) or not isinstance(value, dict):
            return None
        count = _count(value.get('active_errors'))
        kinds, reasons = value.get('kinds'), value.get('reasons')
        if (count is None or count == 0 or not isinstance(kinds, dict) or not isinstance(reasons, dict)
                or set(kinds) - _DISCOVERY_ERROR_KINDS or set(reasons) - _DISCOVERY_ERROR_REASONS
                or any(_count(v) in (None, 0) for v in (*kinds.values(), *reasons.values()))
                or sum(kinds.values()) != count or sum(reasons.values()) != count):
            return None
        providers[identifier] = {'active_errors': count, 'kinds': dict(kinds), 'reasons': dict(reasons)}
    if sum(item['active_errors'] for item in providers.values()) != total:
        return None
    return {'state': state, 'scanned_rows': scanned, 'active_errors': total, 'providers': providers}


def _cnyes_local_replay_retries(value):
    """Publish only typed local retry aggregates from the owner's snapshot."""
    raw = _object(value)
    if (type(raw.get('contract_version')) is not int or raw['contract_version'] != 1
            or raw.get('current_source_contract_only') is not True
            or raw.get('retry_records_are_article_counts') is not False
            or not isinstance(raw.get('entries'), dict)):
        return None
    entries = {}
    for name in ('api_pages', 'metadata_batches', 'diagnostic_originals'):
        if name not in raw['entries']:
            return None
        item = raw['entries'][name]
        if item is None:
            entries[name] = None
            continue
        if not isinstance(item, dict) or 'earliest_not_before_utc' not in item:
            return None
        records = _count(item.get('records'))
        try:
            deadline = _stamp(item.get('earliest_not_before_utc'))
        except (OverflowError, ValueError):
            return None
        if records is None or (records == 0 and item.get('earliest_not_before_utc') is not None) or (records > 0 and deadline is None):
            return None
        entries[name] = {'records': records, 'earliest_not_before_utc': deadline.isoformat() if deadline else None}
    return {'contract_version': 1, 'current_source_contract_only': True,
            'retry_records_are_article_counts': False, 'entries': entries}


def _cnyes_metadata_resource_waits(value):
    """Expose only typed batch-size observations from the owner snapshot."""
    raw = _object(value)
    if (type(raw.get('contract_version')) is not int or raw['contract_version'] != 1
            or raw.get('current_source_contract_only') is not True
            or raw.get('records_are_article_counts') is not False):
        return None
    records = _count(raw.get('records'))
    above = _count(raw.get('above_current_bound_records'))
    bound = _count(raw.get('current_bound_bytes'))
    maximum = _count(raw.get('max_observed_required_bytes'))
    if (records is None or above is None or bound is None or bound == 0 or above > records
            or (records == 0 and raw.get('max_observed_required_bytes') is not None)
            or (records > 0 and (maximum is None or maximum == 0 or (above > 0) != (maximum > bound)))):
        return None
    return {'contract_version': 1, 'current_source_contract_only': True,
            'records_are_article_counts': False, 'records': records,
            'above_current_bound_records': above, 'current_bound_bytes': bound,
            'max_observed_required_bytes': maximum}


def _cnyes_metadata_large_replay(value):
    raw = _object(value)
    if (type(raw.get('contract_version')) is not int or raw['contract_version'] != 1
            or raw.get('current_source_contract_only') is not True
            or raw.get('retry_records_are_article_completion') is not False
            or type(raw.get('assigned')) is not bool or 'earliest_not_before_utc' not in raw):
        return None
    records = _count(raw.get('retry_records'))
    try:
        deadline = _stamp(raw.get('earliest_not_before_utc'))
    except (OverflowError, ValueError):
        return None
    if (records is None or (records == 0 and raw.get('earliest_not_before_utc') is not None)
            or (records > 0 and deadline is None)):
        return None
    result = {'contract_version': 1, 'current_source_contract_only': True,
              'retry_records_are_article_completion': False, 'assigned': raw['assigned'],
              'retry_records': records, 'earliest_not_before_utc': deadline.isoformat() if deadline else None}
    # Cause counts were added independently of the existing public snapshot
    # contract. An old or malformed breakdown remains unknown, never zero.
    causes = raw.get('retry_cause_counts')
    names = ('progress_unknown_timeout', 'counters_unchanged_timeout', 'other')
    if (type(raw.get('retry_causes_contract_version')) is int and raw['retry_causes_contract_version'] == 1
            and isinstance(causes, dict) and set(causes) == set(names)):
        counts = {key: _count(causes[key]) for key in names}
        if all(value is not None for value in counts.values()) and sum(counts.values()) == records:
            result.update(retry_causes_contract_version=1, retry_cause_counts=counts)
    return result


def _cnyes_metadata_index_progress(value):
    """Publish baseline scan counters without a fabricated completion ratio."""
    raw = _object(value)
    if (type(raw.get('contract_version')) is not int or raw['contract_version'] != 1
            or raw.get('current_source_contract_only') is not True
            or raw.get('counts_are_article_completion') is not False
            or raw.get('state') not in ('scanning', 'draining', 'complete')):
        return None
    names = ('rows_seen', 'registered_heads', 'dirty_processed', 'cursor_resets')
    values = {key: _count(raw.get(key)) for key in names}
    if any(value is None for value in values.values()):
        return None
    invalid = _count(raw.get('invalid_heads_observed'))
    if 'invalid_heads_observed' in raw and invalid is None:
        return None
    try:
        updated = _stamp(raw.get('updated_at_utc'))
    except (OverflowError, ValueError):
        return None
    if updated is None:
        return None
    return {'contract_version': 1, 'current_source_contract_only': True,
            'counts_are_article_completion': False, 'state': raw['state'], **values,
            'invalid_heads_observed': invalid, 'updated_at_utc': updated.isoformat()}


def _routing_work(value: Any) -> dict[str, Any] | None:
    """Host-plan progress is not metadata decoding, link or body coverage."""
    raw = _object(value)
    if type(raw.get('contract_version')) is not int or raw['contract_version'] != 1:
        return None
    result = {'contract_version': 1, 'snapshot_consistent': False, 'state': 'unavailable',
              'known_hosts': None, 'completed_hosts': None, 'pending_hosts': None,
              'host_progress_ratio': None, 'completion_proven': False,
              'conditional_eta_seconds': None, 'publisher_requests': 0, 'body_completion_claimed': False}
    total, completed, pending = (_count(raw.get(k)) for k in ('known_hosts', 'completed_hosts', 'pending_hosts'))
    if (raw.get('snapshot_consistent') is not True or any(v is None for v in (total, completed, pending))
            or completed + pending != total or type(raw.get('publisher_requests')) is not int
            or raw['publisher_requests'] != 0
            or raw.get('body_completion_claimed') is not False):
        return result
    done = pending == 0
    expected = ('empty' if not total else 'complete') if done else 'pending'
    if raw.get('state') != expected or raw.get('completion_proven') is not done:
        return result
    result.update(snapshot_consistent=True, state=expected, known_hosts=total, completed_hosts=completed,
                  pending_hosts=pending, host_progress_ratio=completed / total if total else None,
                  completion_proven=done)
    return result


def _number(value: Any) -> float | None:
    if type(value) not in {int, float}:
        return None
    return float(value) if 0 <= value <= 1e18 and math.isfinite(value) else None


def _stamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or len(value) > 64:
        return None
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return stamp.astimezone(UTC) if stamp.tzinfo is not None else None
    except (ValueError, OverflowError):
        return None


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
    observation_version = calibration.get('completion_observation_contract_version')
    if observation_version is None:
        observation = calibration  # Older owners: retain explicitly bounded legacy evidence.
    elif type(observation_version) is int and observation_version == 1:
        observation = _object(calibration.get('completion_observation'))
    else:
        return result  # A malformed new contract cannot fall back to old fast samples.
    history = observation.get('history')
    latest = observation.get('last_window')
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


_PROVIDER_SOURCE_GATE_MAPS = ('temporarily_paused_robots_origins', 'temporarily_paused_gateway_hosts',
                              'unverified_robots_origins', 'unverified_gateway_hosts')


def _provider_has_unscoped_source_gate(control: Mapping[str, Any]) -> bool:
    """A provider gate without a URL-scope proof makes a whole-queue ETA unknown.

    The maps are bounded owner telemetry. Expired rechecks are deliberately
    retained until an actual source response proves recovery. Missing fields
    remain compatible with older snapshots; a present malformed field is not
    evidence of an empty gate. Do not enumerate or publish private host names.
    """
    for key in _PROVIDER_SOURCE_GATE_MAPS:
        if key in control:
            value = control[key]
            if not isinstance(value, Mapping) or value:
                return True
    return ('unverified_robots_origins_overflow' in control
            and control['unverified_robots_origins_overflow'] is not False)


def _provider_precise_gate_evidence_invalid(control: Mapping[str, Any]) -> bool:
    """Check present control evidence before trusting a saved precise scope.

    This is used only for the provider with an explicit URL-scope projection.
    Match the owner's bounded gate contract without exposing its private keys.
    Missing legacy fields stay compatible; invalid or overflowed evidence is
    not a proof that the queue is unaffected.
    """
    for key in _PROVIDER_SOURCE_GATE_MAPS:
        if key not in control:
            continue
        value = control[key]
        if not isinstance(value, Mapping) or len(value) > 32:
            return True
        for origin, deadline in value.items():
            if (not isinstance(origin, str) or not 0 < len(origin) <= 320
                    or type(deadline) not in {int, float}
                    or not 0 <= deadline <= 253402214400 or not math.isfinite(deadline)):
                return True
    return ('unverified_robots_origins_overflow' in control
            and control['unverified_robots_origins_overflow'] is not False)


def _states(value: Any) -> dict[str, int]:
    result: dict[str, int] = {}
    for key, count in _object(value).items():
        number = _count(count)
        if number is not None:
            name = key if key in _STATES else "other"
            result[name] = result.get(name, 0) + number
    return result


def _provider_states_reconcile(global_states: Any, raw_providers: Any) -> bool:
    """Check the owner's exact state partition before public redaction.

    Unknown names are intentionally published as ``other``, but comparing
    those redacted buckets could hide different unknown states. Missing or
    malformed counts must not silently become zero.
    """
    def valid_states(value: Any) -> bool:
        return isinstance(value, dict) and all(
            isinstance(key, str) and _count(count) is not None for key, count in value.items())

    if not valid_states(global_states) or not isinstance(raw_providers, dict):
        return False
    totals: Counter[str] = Counter()
    for identifier, provider in raw_providers.items():
        if (not isinstance(identifier, str) or not _ID.fullmatch(identifier)
                or not isinstance(provider, dict) or not valid_states(provider.get('states'))):
            return False
        totals.update(provider['states'])
    return ({key: value for key, value in totals.items() if value}
            == {key: value for key, value in global_states.items() if value})


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
        handoff_path = _retained_handoff_plan_path(repo_root, load_news_configuration(paths[1]))
        if handoff_path is not None:
            paths.append(handoff_path)
        review_path = planning_review_path(paths[1])
        if review_path is not None:
            paths.append(review_path)
    except (OSError, ValueError, KeyError, TypeError):
        pass
    paths.extend(reference_paths(repo_root))
    for path in paths:
        try:
            stat = path.stat()
            parts.append(f"{stat.st_dev}:{stat.st_ino}:{stat.st_mtime_ns}:{stat.st_ctime_ns}:{stat.st_size}")
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


def _source_review_followup(review, *, review_valid: bool, now: datetime) -> str:
    """Project only a finite observation state; never private question text.

    An empty or absent legacy inventory is unknown, not evidence of complete
    source review or body history. A malformed entry affects this projection
    alone, not successful capture telemetry or other providers.
    """
    if not review_valid or not isinstance(review, dict):
        return 'unknown'
    scope = review.get('current_scope_review')
    if not isinstance(scope, dict) or 'remaining_questions' not in scope:
        return 'unknown'
    if 'recorded_at_utc' in scope:
        recorded = _stamp(scope['recorded_at_utc'])
        if recorded is None or recorded > now + timedelta(seconds=5):
            return 'unknown'
    try:
        completed, remaining = recorded_review_questions(review)
    except ValueError:
        return 'unknown'
    return 'open' if remaining else 'none_recorded' if completed else 'unknown'


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
    status_path = repo_root / "data_news_fulltext/status.json"
    status, snapshot_read_attempts = _read_news_status(status_path)
    snapshot_read_state = 'loaded' if status else 'unavailable_or_invalid'
    if not status:
        try:
            if status_path.stat().st_size > MAX_STATUS_SNAPSHOT_BYTES:
                snapshot_read_state = 'too_large'
        except OSError:
            pass
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
    disk_admission = _disk_admission(status.get('disk_admission'), observed, now)
    disk_verification = _disk_verification(status.get('disk_admission'), observed, now)
    valid = type(status.get("contract_version")) is int and status["contract_version"] == 1
    lifecycle = read_news_worker_lifecycle(repo_root / 'data_news_fulltext',
        snapshot_owner_id=status.get('owner_id'), snapshot_at=observed, now=now)
    initializing = False
    lifecycle_stopped = False
    local_sqlite_phase = False
    if lifecycle.get('available') and (lifecycle['snapshot_owner_matches'] or lifecycle['supersedes_snapshot']):
        lifecycle_stopped = not lifecycle['process_alive'] or lifecycle['state'] in {'closed', 'failed'}
        initializing = not lifecycle_stopped and (not lifecycle['snapshot_owner_matches']
            or lifecycle['state'] == 'initializing')
        if initializing:
            activity = 'initializing' if lifecycle['age_seconds'] <= MAX_AGE_SECONDS else 'initializing_stale'
        elif lifecycle_stopped and activity not in {'stopped', 'waiting_restart'}:
            activity = 'stopped'
        lifecycle_at = _stamp(lifecycle.get('observed_at_utc'))
        local_sqlite_phase = bool(not lifecycle_stopped and lifecycle_at
            and (observed is None or lifecycle_at >= observed)
            and lifecycle['phase'] in {'waiting_local_sqlite', 'local_sqlite_retry_finished'})
        if local_sqlite_phase:
            if lifecycle['phase'] == 'local_sqlite_retry_finished':
                activity = 'waiting_local_snapshot'
            else:
                activity = ('waiting_local_database' if lifecycle['age_seconds'] <= LOCAL_SQLITE_WAIT_MAX_AGE_SECONDS
                            else 'waiting_local_database_stale')
    if not initializing and not lifecycle_stopped and not local_sqlite_phase and activity in {'running', 'idle', 'waiting', 'inventorying', 'waiting_disk'}:
        if 'disk_admission' in status and disk_admission is None and disk_verification is None:
            activity = 'unavailable'  # A current heartbeat cannot freshen an old admission sample.
        elif disk_verification or disk_admission and not disk_admission['fetch_allowed']:
            activity = 'waiting_disk'  # Inventory activity does not remove the write constraint.
    live = valid and fresh and activity in {"running", "idle", "waiting", "inventorying"}
    health = ("unavailable" if not status else "invalid" if not valid else "stale" if not fresh
              else "stopped" if activity == "stopped" else activity)
    if initializing or lifecycle_stopped or local_sqlite_phase:
        health = activity
    if snapshot_read_state == 'too_large':
        # An alive owner plus unreadable telemetry is not a new worker starting
        # up, zero known URLs, or evidence that acquisition has stopped.
        health = 'unavailable'
    configured: dict[str, Mapping[str, Any]] = {}
    for source in config.get("providers", []) if isinstance(config.get("providers"), list) else []:
        source = _object(source)
        identifier = source.get("id")
        if isinstance(identifier, str) and _ID.fullmatch(identifier):
            configured[identifier] = source
    raw_providers = _object(status.get("providers")) if valid else {}
    discovery_rollup = (_discovery_error_rollup(status.get('discovery_error_rollup'),
                                               _count(status.get('discovery_errors'))) if valid else None)
    candidate_diagnostics = (_candidate_diagnostic_progress(status.get('candidate_diagnostic_progress'),
        set(configured) | set(raw_providers)) if valid else None)
    local_replay_retries = _cnyes_local_replay_retries(status.get('cnyes_local_replay_retries')) if valid else None
    metadata_resource_waits = _cnyes_metadata_resource_waits(status.get('cnyes_metadata_resource_waits')) if valid else None
    metadata_large_replay = _cnyes_metadata_large_replay(status.get('cnyes_metadata_large_replay')) if valid else None
    metadata_index_progress = _cnyes_metadata_index_progress(status.get('cnyes_metadata_index_progress')) if valid else None
    original_inventory, original_sources = _original_sources(status if valid else {}, raw_providers)
    # Persisted article counts are reusable. Previous-process timings, requests
    # and capacity windows are not observations of the initializing new owner.
    metrics = _object(status.get("performance")) if valid and not initializing else {}
    metric_providers = _object(metrics.get("providers"))
    elapsed = _number(metrics.get("owner_elapsed_seconds"))
    cooldowns = _object(status.get("provider_next_attempt_utc"))
    capacity_in_snapshot = valid and fresh and not initializing
    capacity_raw = status.get("http_capacity")
    capacity = _object(capacity_raw) if capacity_in_snapshot else {}
    runtime_controls_raw = capacity.get("providers")
    runtime_controls = _object(runtime_controls_raw)
    malformed_runtime_controls = capacity_in_snapshot and (
        'http_capacity' in status and not isinstance(capacity_raw, Mapping)
        or 'providers' in capacity and not isinstance(runtime_controls_raw, Mapping))
    calibration = _object(_object(capacity.get("calibration")).get("providers"))
    calibration_version = _object(capacity.get('calibration')).get('measurement_contract_version')
    completion_owner_version = _object(capacity.get('calibration')).get('completion_window_owner_contract_version')
    completion_observation_version = _object(capacity.get('calibration')).get('completion_observation_contract_version')
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
    body_state_declared = 'provider_body_state_counter_contract_version' in status
    body_state_version = status.get('provider_body_state_counter_contract_version')
    body_state_supported = type(body_state_version) is int and body_state_version == 1 and stored_counter_supported
    discovery_provider_ids = set(discovery_rollup['providers']) if discovery_rollup and discovery_rollup['state'] == 'ready' else set()
    for identifier in sorted(set(configured) | set(raw_providers) | discovery_provider_ids):
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
        body_coverage = (_body_state_coverage(raw.get('body_state_counts') if raw else {'missing': {}, 'stored': {}},
            counts, stored_urls, registered=identifier in configured, body_enabled=body_enabled)
            if valid and body_state_supported else None)
        work_raw = _object(raw.get('body_work_progress')) if identifier == 'cnyes' else {}
        body_work = _body_work_progress(work_raw, ready) if identifier == 'cnyes' else None
        request_scope_declared = 'request_scope_progress' in work_raw
        request_scope = _body_request_scope_progress(work_raw.get('request_scope_progress'), body_work, observed, now)
        rechecks = request_scope.get('pending_source_rechecks') if request_scope else None
        scope_eta_unknown = request_scope_declared and (request_scope is None or not request_scope['available']
            or request_scope['source_paused_missing_body_urls'] > 0
            or rechecks is None or not rechecks['available'] or rechecks['unverified_missing_body_urls'] > 0)
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
                                         'completion_window_owner_contract_version': completion_owner_version,
                                         'completion_observation_contract_version': completion_observation_version},
                                         heads, observed, now, live, current_owner=status.get('owner_id')) if body_enabled else None)
        eta_rate = forecast['new_url_heads_per_second'] if forecast is not None else rate
        cooldown = _stamp(cooldowns.get(identifier))
        cooling = bool(cooldown and cooldown > now)
        eta_remaining = body_coverage['queued_missing_body_urls'] if body_coverage else ready
        control_raw = runtime_controls.get(identifier)
        control = _object(control_raw)
        malformed_control = malformed_runtime_controls or (
            identifier in runtime_controls and not isinstance(control_raw, Mapping)) or (
            request_scope_declared and _provider_precise_gate_evidence_invalid(control))
        scope_eta_unknown = scope_eta_unknown or bool(
            live and body_enabled and eta_remaining and (
                malformed_control or not request_scope_declared and _provider_has_unscoped_source_gate(control)))
        eta = (eta_remaining / eta_rate if body_enabled and eta_remaining and eta_rate and not cooling and not scope_eta_unknown else
               0.0 if body_enabled and eta_remaining == 0 and live else None)
        if body_coverage and eta_remaining == 0 and body_coverage['missing_body_urls']:
            eta = None  # Missing but blocked/unreviewed bodies are not completed work.
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
            if eta_remaining and observed + timedelta(seconds=eta) <= now:
                eta = finish = None  # Old projection elapsed without proof of completion.
        state = (activity if initializing and enabled else "unavailable" if not valid else "not_enabled" if not enabled else
                 activity if lifecycle_stopped else "stale" if not fresh else activity if not live else
                 "cooldown" if cooling else
                 ("attention" if metadata_feed['feeds_with_errors'] else "metadata_polling")
                     if metadata_feed and not body_enabled else
                 "running" if counts.get("inflight", 0) else
                 "rechecking" if ready and body_coverage and not eta_remaining else
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
            "discovery_errors": (discovery_rollup['providers'].get(identifier,
                {'active_errors': 0, 'kinds': {}, 'reasons': {}})
                if discovery_rollup and discovery_rollup['state'] == 'ready' else None),
            "automatic_fulltext_allowed": body_enabled, "metadata_feed_progress": metadata_feed,
            "authorized_api_setup": api_setup,
            "source_policy_review": review_decision,
            "source_reviewed_at_utc": review_at.isoformat() if review_valid else None,
            "source_review_followup": _source_review_followup(review, review_valid=review_valid, now=now),
            "candidate_diagnostic_work": (candidate_diagnostics['providers'].get(identifier,
                {key: 0 for key in _CANDIDATE_WORK_KEYS}) if candidate_diagnostics else None),
            "adapter_registered": identifier in configured,
            "adapter_acceptance": "committed_body_heads_observed" if stored_urls or complete else "no_body_acceptance_observed",
            "source_role": source_role(identifier.removeprefix("unregistered:")) if identifier.startswith("unregistered:") else
                source_role(settings.get("domains", [""])[0]) if isinstance(settings.get("domains"), list) and settings["domains"] else "publisher_or_other_unverified",
            "known_urls": known, "complete_urls": complete, "remaining_urls": ready,
            "stored_body_urls": stored_urls, "stored_body_progress_ratio": stored_ratio,
            "stored_body_counter_available": stored_counter_supported,
            "body_coverage": body_coverage,
            "publisher_reference_coverage": _publisher_reference_coverage(
                raw.get('publisher_body_references'), known, stored_urls, body_coverage) if valid else None,
            "outcome_progress": outcome_progress,
            "original_metadata": original_sources.get(identifier),
            "body_work_progress": body_work,
            "native_lookup_recheck_progress": _native_lookup_recheck_progress(
                raw.get('native_lookup_recheck_progress')) if identifier == 'cnyes' and valid else None,
            "local_replay_retries": local_replay_retries if identifier == 'cnyes' else None,
            "metadata_resource_waits": metadata_resource_waits if identifier == 'cnyes' else None,
            "metadata_large_replay": metadata_large_replay if identifier == 'cnyes' else None,
            "metadata_index_progress": metadata_index_progress if identifier == 'cnyes' else None,
            "body_request_scope_progress": request_scope,
            "public_index_progress": _cnyes_public_index_progress(raw.get('public_index_progress'), now=now) if identifier == 'cnyes' else None,
            "manifest_phase_progress": _cnyes_manifest_phase_progress(raw.get('public_index_progress'), metrics,
                now, live and raw.get('automatic_collection_enabled') is True, status.get('owner_id'),
                provider_waiting=cooling) if identifier == 'cnyes' else None,
            "history_list_progress": _cnyes_history_list_progress(raw.get('history_list_progress'), status.get('bulk_gap_projection')) if identifier == 'cnyes' else None,
            "blocked_urls": blocked, "states": counts, 'growth_waiting_urls': growth_waiting,
            'acquisition_mode': mode, 'finite_cohort': cohort, 'finite_supplements': supplement,
            "progress_ratio": cohort['progress_ratio'] if cohort else complete / known if known else None,
            "text_bytes": _count(raw.get("text_bytes")),
            "owner_requests": requests, "owner_responses": responses,
            "owner_response_bytes": _count(http_counts.get("http_response_bytes")),
            "owner_accepted_versions": accepted, "owner_articles_per_second": rate,
            "owner_local_sqlite_busy_commit_events": _count(http_counts.get("local_sqlite_busy_commit_events")),
            "owner_new_stored_url_heads": heads,
            "forecast_rate_basis": "new_stored_url_heads" if heads is not None else "legacy_version_proxy",
            "completion_forecast": forecast,
            "completion_forecast_rate_basis": (forecast['basis'] if forecast is not None else
                                               'owner_new_stored_url_heads' if heads is not None else 'legacy_owner_version_proxy'),
            "conditional_eta_seconds": eta, "conditional_finish_at_utc": finish,
            "conditional_eta_unavailable_reason": ('source_request_scope_unknown'
                if malformed_control or request_scope is None or not request_scope['available']
                    or (not request_scope['source_paused_missing_body_urls'] and (rechecks is None or not rechecks['available']))
                else 'source_request_scope_paused') if scope_eta_unknown else None,
            "conditional_eta_scope": 'queued_missing_body_urls' if body_coverage else 'legacy_active_url_states',
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
            "fulltext_feed_inventory": _fulltext_feed_inventory(settings, http_counts),
            "temporarily_blocked_bulk_endpoints": endpoint_circuits.get(identifier, 0),
            "observed_bulk_rows_per_request_cap": 30 if identifier == "cnyes" and settings.get("cnyes_apis") else None,
            "capacity_workload_family": sample.get("workload_family") if isinstance(sample.get("workload_family"), str) and sample.get("workload_family") in {
                "cnyes_bulk", "rss_fulltext", "wordpress_posts", "article_pdf", "article_html", "mixed", "metadata", "unclassified"} else None,
            "minimum_observed_request_gap_seconds": _number(detail.get("minimum_request_start_gap_seconds")),
            "peak_http_inflight": _count(http_counts.get("peak_http_inflight")),
            "transport_errors": _count(http_counts.get("transport_errors")), "http_errors": http_errors,
            "official_hourly_quota": None, "official_daily_quota": None,
        })
    counts = _states(status.get("states")) if valid else {}
    known = _count(status.get("unique_articles")) if valid else None
    stored = _count(status.get("articles_with_stored_body")) if valid else None
    ratio = stored / known if known and stored is not None and stored <= known else None
    state_totals_reconciled = (valid and _provider_states_reconcile(status.get('states'), status.get('providers')))
    consistency = (known is not None and known == sum(counts.values())
                   and known == sum(source["known_urls"] or 0 for source in providers)
                   and stored is not None and stored <= known and state_totals_reconciled)
    if stored_counter_declared:
        consistency = consistency and stored_counter_supported and all(
            source['stored_body_urls'] is not None for source in providers) and stored == sum(
                source['stored_body_urls'] or 0 for source in providers)
    if body_state_declared:
        consistency = consistency and body_state_supported and all(source['body_coverage'] is not None for source in providers)
    body_coverage = None
    if valid and body_state_supported and all(source['body_coverage'] is not None for source in providers):
        # Counts are disjoint across providers and body-presence partitions.
        # Classification stays provider-specific (registered/enabled scope).
        fields = ('missing_body_urls', 'stored_body_urls', 'queued_missing_body_urls',
                  'queued_stored_body_recheck_urls', 'retained_body_noncomplete_urls')
        body_coverage = {'contract_version': 1, **{key: sum(source['body_coverage'][key] for source in providers) for key in fields},
            'retained_bodies_are_not_missing': True, 'all_history_complete': False,
            'grain': 'disjoint URL heads by retained body presence and latest check state'}
        for key in ('missing_body_outcomes', 'retained_body_outcomes'):
            categories = Counter()
            for source in providers:
                categories.update(source['body_coverage'][key]['categories'])
            body_coverage[key] = {'categories': dict(categories), 'known_urls': sum(categories.values())}
        for key in ('missing_body_states', 'retained_body_states'):
            states = Counter()
            for source in providers:
                states.update(source['body_coverage'][key])
            body_coverage[key] = dict(states)
    if valid and not consistency:
        health = "degraded"
        ratio = None
        for source in providers:
            source["conditional_eta_seconds"] = source["conditional_finish_at_utc"] = None
            source['progress_ratio'] = source['stored_body_progress_ratio'] = None
            source['completion_forecast'] = None
            source['finite_cohort'] = source['finite_supplements'] = None
            # The two partitions disagree, so neither side can establish
            # which provider's apparent completed/remaining counts are true.
            # Keep the original heartbeat untouched for diagnosis.
            for field in ('known_urls', 'complete_urls', 'remaining_urls', 'stored_body_urls',
                          'blocked_urls', 'growth_waiting_urls', 'text_bytes', 'adapter_acceptance',
                          'body_coverage', 'publisher_reference_coverage', 'outcome_progress',
                          'body_work_progress', 'native_lookup_recheck_progress', 'body_request_scope_progress'):
                source[field] = None
            source['states'] = {}
            if source['state'] in {'running', 'backfilling', 'rechecking', 'known_queue_clear',
                                    'finite_complete', 'waiting_cohort', 'cooldown', 'attention',
                                    'metadata_polling'}:
                source['state'] = 'unavailable'
        body_coverage = None
    rank = {"running": 0, "backfilling": 1, 'rechecking': 2, "known_queue_clear": 2, 'finite_complete': 2, 'waiting_cohort': 2,
            "cooldown": 3, "attention": 4}
    providers.sort(key=lambda source: (not source["configured"], rank.get(source["state"], 5), -int(source["known_urls"] or 0), source["id"]))
    eligible = [source for source in providers if source['automatic_fulltext_allowed'] and (
        source['body_coverage']['queued_missing_body_urls'] if source['body_coverage'] else source['remaining_urls'])]
    etas = [source["conditional_eta_seconds"] for source in eligible]
    eligible_eta = max(etas) if consistency and live and etas and all(eta is not None for eta in etas) else None
    outcome_totals = Counter()
    if valid and consistency:
        for source in providers:
            outcome_totals.update(source['outcome_progress']['categories'])
    outcome_progress = ({'contract_version': NEWS_OUTCOME_CLASSIFICATION_VERSION,
        'categories': dict(outcome_totals), 'known_urls': known,
        'snapshot_consistent': consistency, 'grain': 'retained queue URL-head states',
        'origin_absence_is_not_all_paths_exhausted': True,
        'definitively_unavailable_urls': None, 'closure_progress_ratio': None,
        'all_history_complete': False} if valid and consistency else None)
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
                      'routing_work': _routing_work(raw_resolution.get('routing_work')),
                      'known_redirect_urls': total, 'examined_urls': examined, 'remaining_urls': remaining,
                      'literal_original_links': outcome_counts['literal_original'], 'opaque_links': outcome_counts['opaque_original'],
                      'unresolved_links': outcome_counts['unresolved'], 'links_to_queued_targets': linked,
                      'progress_ratio': examined / total if reconciled and total else None,
                      'owner_urls_per_second': rate, 'conditional_eta_seconds': remaining / rate if rate else None,
                      'body_completion_claimed': False, 'metadata_identity_verified': False,
                      'publisher_requests': 0}
    corpus_quality, corpus_headline_quality = public_corpus_quality_statuses(repo_root, now=now)
    metadata_inventory = _object(status.get('metadata_inventory'))
    metadata_version = metadata_inventory.get('contract_version')
    alias_pending = (_count(metadata_inventory.get('receipt_alias_retargets_pending'))
                     if valid and type(metadata_version) is int and metadata_version == 7 else None)
    metadata_selection_valid = (valid and ('contract_version' not in metadata_inventory
        or (type(metadata_version) is int and (metadata_version < 7
            or (metadata_version == 7 and alias_pending == 0)))))
    return {
        "schema_version": 1, "nonbody_source_outcome_progress_contract_version": 1,
        "snapshot_reader_contract_version": 3, "snapshot_read_state": snapshot_read_state,
        "snapshot_read_attempts": snapshot_read_attempts,
        "worker_lifecycle_projection_contract_version": 3,
        "provider_state_owner_lifecycle_contract_version": 1,
        "disk_admission_projection_contract_version": 1,
        "news_outcome_classification_contract_version": NEWS_OUTCOME_CLASSIFICATION_VERSION,
        "outcome_progress": outcome_progress,
        "provider_stored_body_counter_contract_version": 1 if stored_counter_supported else None,
        "provider_body_state_counter_contract_version": 1 if body_state_supported else None,
        "discovery_error_rollup": ({key: discovery_rollup[key] for key in ('state', 'scanned_rows', 'active_errors')}
                                   if discovery_rollup else None),
        "body_coverage": body_coverage,
        "cnyes_list_progress_projection_contract_version": 1,
        "cnyes_manifest_phase_projection_contract_version": 1,
        "original_source_inventory_contract_version": 1,
        "original_source_inventory": original_inventory,
        "url_metadata_inbox": _url_metadata_inbox(status.get('url_metadata_inbox')) if valid else None,
        "finlab_cnyes_priority": _finlab_cnyes_priority(status.get('finlab_cnyes_priority'), observed) if valid else None,
        "retained_link_handoff": _retained_handoff_projection(repo_root, config, status.get('retained_link_handoff') if valid else None),
        "candidate_diagnostic_progress": candidate_diagnostics,
        "source_policy_review_contract_version": 1,
        "source_reviews_state": source_reviews_state,
        "source_policy_review_counts": dict(Counter(source["source_policy_review"] for source in providers)),
        "source_review_followup_contract_version": 1,
        "source_review_followup_state_counts": dict(Counter(source["source_review_followup"] for source in providers)),
        "corpus_quality": corpus_quality,
        "corpus_headline_quality": corpus_headline_quality,
        "sort_day_quality": public_sort_day_quality_status(repo_root, now=now),
        "generated_at_utc": now.isoformat(), "read_only": True,
        "production_control_possible": False, "health": health,
        "observed_at_utc": observed.isoformat() if observed else None,
        "status_age_seconds": max(0.0, age) if age is not None else None,
        "snapshot_consistent": consistency,
        "provider_state_totals_reconciled": state_totals_reconciled if valid else None,
        "worker": {"activity": activity, "source_activity": source_activity,
                   "disk_admission": disk_admission,
                   "disk_verification": disk_verification,
                   "fresh": fresh and not initializing and not lifecycle_stopped and not local_sqlite_phase,
                   "local_sqlite_wait_max_age_seconds": LOCAL_SQLITE_WAIT_MAX_AGE_SECONDS,
                   "lifecycle": lifecycle,
                   "owner_elapsed_seconds": lifecycle['elapsed_seconds'] if initializing else elapsed,
                   "owner_cpu_seconds": None if initializing else _number(metrics.get("owner_cpu_seconds")),
                   "requests": sum(requests) if not initializing and requests and all(value is not None for value in requests) else None,
                   "response_bytes": _count(status.get("http_response_bytes_this_run")) if valid and not initializing else None},
        "acquisition": {"known_urls": known if consistency else None,
                        "stored_body_articles": stored if consistency else None,
                        "complete_state_urls": counts.get("complete", 0) if consistency else None,
                        "remaining_urls": sum(source["remaining_urls"] or 0 for source in providers if source["enabled"]) if consistency else None,
                        "awaiting_authorization_urls": counts.get("awaiting_authorization", 0) if consistency else None,
                        "inflight_urls": counts.get("inflight", 0) if consistency else None,
                        "retry_urls": sum(counts.get(key, 0) for key in ("retry", "retry_cooldown")) if consistency else None,
                        'growth_waiting_urls': counts.get('waiting_cohort', 0) if consistency else None,
                        "blocked_urls": sum(count for key, count in counts.items() if key not in _READY_STATES | {"complete", 'waiting_cohort'}) if consistency else None,
                        "progress_ratio": ratio, "text_bytes": _count(status.get("accepted_text_bytes")) if consistency else None,
                        "eligible_eta_seconds": eligible_eta, "all_sources_eta_seconds": None,
                        "discovery_due": _count(status.get("discovery_pending")) if valid else None,
                        "discovery_errors": _count(status.get("discovery_errors")) if valid else None,
                        "metadata_partitions": _count(status.get("metadata_partitions")) if metadata_selection_valid else None,
                        "metadata_association_rows": _count(status.get("metadata_association_rows")) if metadata_selection_valid else None,
                        "metadata_errors": _count(status.get("metadata_partition_errors")) if valid else None,
                        "metadata_selection_pending": alias_pending,
                        "finmind_catalog_errors": _source_catalog_errors(status, 'finmind') if valid else None,
                        "finlab_catalog_errors": _source_catalog_errors(status, 'finlab') if valid else None,
                        "disabled_bulk_pages": _count(status.get("bulk_api_pages_disabled")) if valid else None,
                        "temporarily_blocked_bulk_pages": _count(status.get("bulk_api_pages_temporarily_blocked")) if valid else None},
        "states": counts if consistency else {}, "providers": providers, "stages": stages,
        "original_url_resolution": resolution,
        "scope": {"historical_completeness_verified": False, "body_exported": False,
                  "source_reviews_grant_capture_permission": False,
                  "publisher_calls_on_page_view": False, "estimated_total_download_bytes": None,
                  "progress_basis": "stored bodies / discovered unique URLs; not all publisher history",
                  "rate_basis": "owner rate: new stored URL heads / owner wall time; completion ETA: consecutive current-owner completed windows including failures/waits when supported; explicit legacy owner fallback otherwise",
                  "eta_basis": "queued missing bodies / current-owner new URL-head rate when joint body/state counts are available; explicitly labeled legacy active-state fallback otherwise; future discovery, blocked URLs and retained-body rechecks excluded from the new estimate",
                  "stage_wall_time_basis": "worker durations can overlap; not additive end-to-end wall time",
                  "traffic_basis": "HTTP requests and response bytes since current owner start; not rolling-hour usage",
                  "metadata_basis": "accepted local FinLab/FinMind metadata plus publisher discovery; stock associations are not unique articles"},
    }

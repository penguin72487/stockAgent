"""Read-only multi-publisher runtime acceptance, separate from history coverage.

Coherent WAL heads and published HTTP counters have different clocks. Refused,
unreviewed and unreachable publishers remain visible, never counted complete.
No publisher requests, queue owner, raw text or credentials are used here.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import subprocess
import time

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_queue import NewsQueue, utc_now, ARTICLE_DISPATCH_CONTRACT_VERSION
from downloader.news_fulltext_host_circuit import host_filtered_query
from downloader.news_fulltext_metrics import ROBOTS_ORIGIN_POLICY_CONTRACT_VERSION, metrics_snapshot_clock
from stockagent.data.news_source_catalog import configuration_paths


RUNTIME_ACCEPTANCE_CONTRACT_VERSION = 10


def runtime_source_paths(repo):
    """Include the out-of-prefix receipt reader used by owner recovery."""
    return sorted((repo / 'downloader').glob('news_fulltext*.py')) + [
        repo / 'downloader/news_receipt_io.py',
        repo / 'stockagent/data/news_source_catalog.py', repo / 'stockagent/data/news_source_urls.py',
        repo / 'stockagent/private_json.py',
        repo / 'scripts/download_news_fulltext.py']


def service_snapshot():
    fields = subprocess.check_output(["systemctl", "show", "stockagent-news-fulltext.service",
        "-p", "MainPID", "-p", "ActiveState", "-p", "ExecMainStartTimestamp", "-p", "NRestarts",
        "-p", "CPUUsageNSec", "-p", "MemoryCurrent", "-p", "MemoryPeak"], text=True)
    return dict(line.split('=', 1) for line in fields.splitlines())


def heartbeat_clock(status, read_at):
    """Freshness belongs to the instant this immutable heartbeat was read.

    Expensive historical reconciliation must not age its own input and call a
    healthy owner stale. A separate end-of-audit read still rejects a genuinely
    stale heartbeat, stopped service or owner restart; the 45-second bound stays.
    """
    stamp = datetime.fromisoformat(status['updated_at_utc'])
    observed = datetime.fromisoformat(read_at)
    if stamp.utcoffset() is None or observed.utcoffset() is None:
        raise ValueError('timezone_aware_heartbeat_clock_required')
    age = (observed - stamp).total_seconds()
    return {'read_at_utc': read_at, 'snapshot_at_utc': stamp.isoformat(),
            'age_seconds': age, 'fresh': -5 <= age <= 45}


def completion_owner_check(owner, service, status, completed_service, completed_status, read_at):
    clock = heartbeat_clock(completed_status, read_at)
    same_process = (service['MainPID'] != '0' and service['MainPID'] == completed_service['MainPID']
                    and completed_service['ActiveState'] == 'active'
                    and service['NRestarts'] == completed_service['NRestarts'])
    same_owner = bool(owner and status.get('owner_id') == owner == completed_status.get('owner_id'))
    process_clock = owner_process_clock_check(completed_service, completed_status)
    return {**clock, 'same_process': same_process, 'same_owner': same_owner,
            'process_clock': process_clock,
            'accepted': clock['fresh'] and same_process and same_owner and process_clock['accepted']}


def frontier_snapshot_counts(db, settings):
    """Exact canonical head counts and indexed rechecks in one read-only WAL.

    The transactional stored-head triggers already count bodies in every state,
    not just `complete`. Do not scan every saved head twice for each audit.
    Legacy queues without the v1 table keep the explicit full-index oracle.
    This is a metric read, not independent whole-lake integrity verification.
    """
    table = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='stored_body_metrics'").fetchone()
    cached = table and settings.get('stored_body_metrics_version') == '1'
    if cached:
        heads = dict(db.execute('SELECT provider,heads FROM stored_body_metrics WHERE heads>0'))
        basis = 'transactional_stored_body_metrics_v1; exact stored URL heads, not state or versions'
    else:
        heads = dict(db.execute("SELECT provider,count(*) FROM articles INDEXED BY stored_bodies "
                                "WHERE body_sha256 IS NOT NULL GROUP BY provider"))
        basis = 'legacy_full_stored_index; canonical v1 metric unavailable'
    states = [dict(r) for r in db.execute('SELECT provider,state,articles FROM state_metrics WHERE articles>0')]
    rechecks = []
    for item in states:
        if item['state'] == 'complete' or not heads.get(item['provider'], 0):
            continue
        count = db.execute('SELECT count(*) FROM articles INDEXED BY dispatch WHERE provider=? AND state=? '
                           'AND body_sha256 IS NOT NULL', (item['provider'], item['state'])).fetchone()[0]
        if count:
            rechecks.append({'provider': item['provider'], 'state': item['state'], 'articles': count})
    return heads, states, rechecks, basis


def owner_process_clock_check(service, status):
    """Reject the old owner's still-fresh heartbeat during a systemd restart.

    The canonical exclusive writer plus compatible database UUID are separate
    checks. Here process start ticks bind the metric epoch to the actual PID,
    rather than trusting ActiveState immediately after systemd forks a child.
    """
    pid = service.get('MainPID', '0')
    result = {'accepted': False, 'basis': 'current /proc PID start and owner metric epoch; two-second clock tolerance'}
    try:
        if not pid.isdigit() or int(pid) <= 0:
            return result
        stat = Path('/proc', pid, 'stat').read_text().rsplit(')', 1)[1].split()
        process_start = time.time() - time.clock_gettime(time.CLOCK_BOOTTIME) + int(stat[19]) / os.sysconf('SC_CLK_TCK')
        metric_stamp, _ = metrics_snapshot_clock(status)
        owner_start = metric_stamp.timestamp() - float(status['performance']['owner_elapsed_seconds'])
        result.update(process_started_at_epoch=process_start, owner_metric_started_at_epoch=owner_start,
                      accepted=owner_start >= process_start - 2)
    except (OSError, KeyError, ValueError, IndexError, TypeError, AttributeError):
        pass
    return result


def baseline_progress_requirements(previous):
    """Only a ready missing-body queue must gain new heads in this window.

    An idle source or a persisted refusal is not a stalled download. Keep both
    visible as separate scopes; never skip a failure that arose inside a ready
    source's measurement window or call the excluded backlog complete.
    """
    observed = datetime.fromisoformat(previous['observed_at_utc'])
    required, excluded = [], {}
    for provider, item in previous['providers'].items():
        if not item['enabled']:
            excluded[provider] = 'not_enabled'
        elif not item['missing_body_work']:
            excluded[provider] = 'no_ready_missing_body_work; terminal_errors_are_not_complete'
        elif item.get('next_attempt_at_utc') and datetime.fromisoformat(item['next_attempt_at_utc']) > observed:
            excluded[provider] = 'source_cooldown_at_baseline; backlog_not_complete'
        elif not item.get('has_dispatchable_missing_body_work', False):
            scope = item.get('request_scope_readiness', {})
            excluded[provider] = ('due_missing_bodies_wait_for_request_scope_policy; backlog_not_complete'
                if scope.get('has_unfiltered_due_work') and
                    (scope.get('active_gateway_hosts') or scope.get('active_robots_origins')) else
                'article_retry_or_lease_not_ready_or_readiness_unknown; backlog_not_complete')
        else:
            required.append(provider)
    return {'required_provider_ids': required, 'excluded_provider_reasons': excluded,
            'basis': 'baseline enabled sources with due missing-body work outside active host/origin pauses, '
                'or a current-owner active lease, and no persisted future provider cooldown; '
                'legacy readiness unknown stays excluded; not all-provider acceptance'}


def active_request_scope_pauses(control, now):
    """Snapshot the same expiring host/origin gates used by the HTTP owner.

    One paused origin never suspends a whole multi-origin publisher. Expiry
    permits a policy probe, not body availability. Invalid telemetry must fail
    this audit rather than silently make a blocked frontier appear healthy.
    """
    result = {}
    for field, output in (('temporarily_paused_gateway_hosts', 'active_gateway_hosts'),
                          ('temporarily_paused_robots_origins', 'active_robots_origins')):
        pauses = control.get(field, {})
        if not isinstance(pauses, dict) or any(type(until) not in (int, float)
                or not math.isfinite(until) for until in pauses.values()):
            raise ValueError('invalid_request_scope_pause_snapshot')
        result[output] = {key: until for key, until in sorted(pauses.items()) if until > now}
    # Validate exact origins and bounded host sets through the actual scheduler
    # SQL builder. This does not execute SQL, acquire an HTTP root or mutate it.
    host_filtered_query('SELECT 1 FROM articles WHERE provider=? LIMIT 1', ('scope-check',),
        tuple(result['active_gateway_hosts']), excluded_origins=tuple(result['active_robots_origins']))
    return result


def has_dispatchable_missing_body_work(db, source, owner, now, *, excluded_hosts=(), excluded_origins=()):
    """Read the scheduler clock, not just membership in a retry state.

    Provider cooldown expiry does not erase a later per-article retry deadline.
    A current owner's active missing-body lease may finish during the window;
    old/expired leases, stored rechecks and unopened finite cohorts cannot prove
    a stalled source. Apply the owner's exact host/origin SQL before LIMIT,
    without suppressing an already dispatched current-owner lease. Indexed
    existence checks perform no queue mutation or publisher requests.
    """
    if not source.allowed:
        return False
    if (source.acquisition_mode == 'finite'
            and not db.execute('SELECT 1 FROM finite_cohorts WHERE provider=?', (source.provider,)).fetchone()):
        return False
    statement, parameters = host_filtered_query(
                  "SELECT 1 FROM articles INDEXED BY dispatch WHERE provider=? "
                  "AND state IN ('pending','retry','retry_cooldown') AND due<=? "
                  "AND body_sha256 IS NULL LIMIT 1", (source.provider, now), excluded_hosts,
                  excluded_origins=excluded_origins)
    if db.execute(statement, parameters).fetchone():
        return True
    return bool(db.execute("SELECT 1 FROM articles INDEXED BY dispatch WHERE provider=? AND state='inflight' "
                           "AND body_sha256 IS NULL AND lease_owner=? AND lease_until>? LIMIT 1",
                           (source.provider, owner, now)).fetchone())


def source_readiness_snapshot(db, source, owner, control, *, queue_observed_at_utc, metric_snapshot_at_utc):
    """Do not invent a policy-probe result between two observation clocks.

    An origin paused in the HTTP snapshot is not proved usable merely because
    its deadline expired before SQLite was read. A policy probe may already
    have failed in that gap. Evaluate each pause at its own snapshot clock;
    keep article due/lease checks at the SQLite clock. Other origins and actual
    current-owner leases retain their existing independent readiness rules.
    """
    queue_clock, metric_clock = (datetime.fromisoformat(value)
        for value in (queue_observed_at_utc, metric_snapshot_at_utc))
    if queue_clock.utcoffset() is None or metric_clock.utcoffset() is None:
        raise ValueError('timezone_aware_readiness_clocks_required')
    scopes = active_request_scope_pauses(control, metric_clock.timestamp())
    unfiltered = has_dispatchable_missing_body_work(db, source, owner, queue_clock.timestamp())
    ready = unfiltered and (not any(scopes.values()) or has_dispatchable_missing_body_work(
        db, source, owner, queue_clock.timestamp(), excluded_hosts=tuple(scopes['active_gateway_hosts']),
        excluded_origins=tuple(scopes['active_robots_origins'])))
    return ready, {'contract_version': 2, 'evaluated_at_utc': queue_observed_at_utc,
        'scope_snapshot_at_utc': metric_snapshot_at_utc, 'pause_evaluated_at_utc': metric_snapshot_at_utc,
        'has_unfiltered_due_work': unfiltered, **scopes,
        'scope_filter_clock_basis': 'HTTP metric snapshot; queue deadlines and leases use SQLite observation',
        'expired_after_metric_snapshot': {key: {scope: until for scope, until in pauses.items()
            if until <= queue_clock.timestamp()} for key, pauses in scopes.items()},
        'permission_after_snapshot_inferred': False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--previous", type=Path)
    parser.add_argument("--body-verification", type=Path)
    args = parser.parse_args(argv)
    audit_started = time.perf_counter()
    config, registry, repo = load_config(args.config)
    root = repo / config["root"]
    status = json.loads((root / "status.json").read_text())
    status_read_at = utc_now()
    start_clock = heartbeat_clock(status, status_read_at)
    metric_stamp, metrics_clock_verified = metrics_snapshot_clock(status)
    service = service_snapshot()
    db = sqlite3.connect(f"file:{root / 'queue.sqlite3'}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        settings = dict(db.execute('SELECT key,value FROM queue_settings'))
        owner = settings.get('current_owner_id')
        observed = utc_now()
        heads, state_rows, stored_not_complete, head_count_basis = frontier_snapshot_counts(db, settings)
        index_stats = [dict(r) for r in db.execute("SELECT provider,kind,count(*) AS jobs,min(last_checked_at_utc) AS first_check,"
            "max(last_checked_at_utc) AS latest_check FROM discovery GROUP BY provider,kind")]
        disabled_jobs = {}
        readiness, scope_readiness = {}, {}
        for p, source in registry.sources.items():
            clause, params = NewsQueue.discovery_dispatch_filter(source)
            disabled_jobs[p] = db.execute("SELECT count(*) FROM discovery WHERE provider=? AND kind IN "
                "('wordpress_posts','cnyes_news','rss_fulltext') AND NOT " + clause, (p, *params)).fetchone()[0]
            control = status.get('http_capacity', {}).get('providers', {}).get(p, {})
            readiness[p], scope_readiness[p] = source_readiness_snapshot(db, source, owner, control,
                queue_observed_at_utc=observed, metric_snapshot_at_utc=metric_stamp.isoformat())
    finally:
        db.close()
    expected_policy = hashlib.sha256(json.dumps([
        (s.provider, s.allowed, s.domains, s.data_fingerprint) for s in registry.sources.values()
    ], sort_keys=True).encode()).hexdigest()
    result = {"runtime_acceptance_contract_version": RUNTIME_ACCEPTANCE_CONTRACT_VERSION,
        "audit_implementation_sha256": sha256_file(Path(__file__)),
        "metrics_snapshot_at_utc": metric_stamp.isoformat(), "metrics_snapshot_clock_verified": metrics_clock_verified,
        "observed_at_utc": observed, "service": service, "config_sha256": sha256_file(args.config),
        "owner_id": owner, "status_owner_id": status.get('owner_id'), "status_snapshot_at_utc": status['updated_at_utc'],
        "status_activity": status['activity'], "status_database_owner_consistent": owner == status.get('owner_id'),
        "loaded_provider_policy_matches_config": settings.get('policy_fingerprint') == expected_policy,
        "status_age_seconds": start_clock['age_seconds'], "status_read_at_utc": status_read_at,
        "stored_head_count_basis": head_count_basis, "providers": {}, "discovery": index_stats,
        "stored_body_rechecks": stored_not_complete, "inactive_bulk_jobs_retained": disabled_jobs,
        "official_numeric_rate_limits_verified": False, "all_history_complete": False,
        "all_configured_providers_successful": False, "publish": False, "remote_training_export": False}
    result['owner_process_clock_check'] = owner_process_clock_check(service, status)
    source_files = runtime_source_paths(repo)
    process_started = result['owner_process_clock_check'].get('process_started_at_epoch', 0)
    result['source_code_sha256'] = {str(path.relative_to(repo)): sha256_file(path) for path in source_files}
    result['source_files_unchanged_since_owner_start'] = bool(process_started and all(path.stat().st_mtime <= process_started for path in source_files))
    result['configuration_files_sha256'] = {str(path.relative_to(repo)): sha256_file(path)
                                            for path in configuration_paths(args.config)}
    capacity = status.get('http_capacity') or {}
    calibration = capacity.get('calibration') or {}
    for p, source in registry.sources.items():
        states = {r['state']: r['articles'] for r in state_rows if r['provider'] == p}
        ready_states = {'pending', 'retry', 'retry_cooldown', 'inflight'}
        bodied_ready = sum(r['articles'] for r in stored_not_complete if r['provider'] == p and r['state'] in ready_states)
        control = capacity.get('providers', {}).get(p, {})
        result['providers'][p] = {"enabled": source.allowed, "interval_seconds": control.get('effective_interval_seconds', source.interval_seconds),
            "parallelism": control.get('parallelism', source.parallelism), "configured_baseline_interval_seconds": source.interval_seconds,
            "runtime_capacity": control, "calibration": calibration.get('providers', {}).get(p),
            "known_urls": sum(states.values()), "stored_url_heads": heads.get(p, 0),
            "missing_body_work": sum(states.get(s, 0) for s in ready_states) - bodied_ready,
            "has_dispatchable_missing_body_work": readiness[p],
            "request_scope_readiness": scope_readiness[p],
            "stored_body_recheck_work": bodied_ready, "states": states,
            "next_attempt_at_utc": status.get('provider_next_attempt_utc', {}).get(p),
            "performance": status.get('performance', {}).get('providers', {}).get(p, {})}
    if args.previous:
        previous = json.loads(args.previous.read_text())
        elapsed = (datetime.fromisoformat(observed) - datetime.fromisoformat(previous['observed_at_utc'])).total_seconds()
        counter_elapsed = (metric_stamp - datetime.fromisoformat(
            previous.get('metrics_snapshot_at_utc', previous['status_snapshot_at_utc']))).total_seconds()
        same_owner = (owner == previous['owner_id'] and service['MainPID'] != '0'
                      and service['MainPID'] == previous['service']['MainPID']
                      and result['status_database_owner_consistent'] and previous['status_database_owner_consistent']
                      and result['owner_process_clock_check']['accepted'] and previous.get('owner_process_clock_check', {}).get('accepted', False))
        progress = {"same_owner": same_owner, "database_window_seconds": elapsed,
                   "http_counter_window_seconds": counter_elapsed, "providers": {}}
        for p, item in result['providers'].items():
            old = previous['providers'][p]
            before, after = old['performance'].get('counts', {}), item['performance'].get('counts', {})
            counts = {key: value - before.get(key, 0) for key, value in after.items()}
            delta = item['stored_url_heads'] - old['stored_url_heads']
            progress['providers'][p] = {"new_stored_url_heads": delta,
                "new_heads_per_second": delta / elapsed if same_owner and elapsed > 0 else None,
                "http_counts_delta": counts if same_owner else None,
                "http_requests_per_second": counts.get('http_requests_started', 0) / counter_elapsed
                    if same_owner and counter_elapsed > 0 else None}
        result['production_progress'] = progress
        progress['progress_requirements'] = baseline_progress_requirements(previous)
    if args.body_verification:
        verification = json.loads(args.body_verification.read_text())
        if 'final_acceptance' in verification:
            verification = verification['final_acceptance']
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(verification['database_snapshot_at_utc'])).total_seconds()
        result['body_verification'] = {"path": str(args.body_verification), "sha256": sha256_file(args.body_verification),
            "accepted": verification['accepted'], "snapshot_at_utc": verification['database_snapshot_at_utc'],
            "body_versions_verified": verification['body_versions_verified'], "errors": verification['errors'],
            "verification_scope": verification.get('verification_scope', 'unspecified; not a history-completeness proof')}
        progress = result.get('production_progress', {})
        completed_status = json.loads((root / 'status.json').read_text())
        completed_read_at = utc_now()
        completed_service = service_snapshot()
        completion = completion_owner_check(owner, service, status, completed_service,
                                             completed_status, completed_read_at)
        result['completion_owner_check'] = completion
        result['completion_service'] = completed_service
        checks = {"service_active": service['ActiveState'] == 'active' and service['MainPID'] != '0',
            "same_owner_progress": progress.get('same_owner', False),
            "healthy_sources_make_independent_body_progress": bool(progress.get('progress_requirements', {}).get('required_provider_ids')) and
                all(progress.get('providers', {}).get(p, {}).get('new_stored_url_heads', 0) > 0
                    for p in progress.get('progress_requirements', {}).get('required_provider_ids', [])),
            "status_owner_coherent": result['status_database_owner_consistent'] and start_clock['fresh'] and completion['accepted'],
            "provider_policy_coherent": result['loaded_provider_policy_matches_config'],
            "source_code_not_edited_after_owner_start": result['source_files_unchanged_since_owner_start'],
            "body_receipts_verified_recently": verification['accepted'] and not verification['errors'] and 0 <= age <= 300,
            "shared_dispatch_clock_contract": status.get('performance', {}).get('request_dispatch_clock_contract_version') == 2,
            "loaded_robots_origin_policy_contract": (
                type(status.get('performance', {}).get('robots_origin_policy_contract_version')) is int
                and status['performance']['robots_origin_policy_contract_version'] == ROBOTS_ORIGIN_POLICY_CONTRACT_VERSION),
            "loaded_oldest_due_retry_fairness_contract": (
                settings.get('article_dispatch_contract_version') == str(ARTICLE_DISPATCH_CONTRACT_VERSION)
                and status.get('article_dispatch_contract_version') == ARTICLE_DISPATCH_CONTRACT_VERSION),
            "locked_metrics_snapshot_clock_verified": metrics_clock_verified,
            "no_observed_gap_faster_than_runtime_policy": all((item['performance'].get('minimum_request_start_gap_seconds') or item['interval_seconds'])
                >= (item['runtime_capacity'].get('minimum_permitted_interval_this_owner') or item['interval_seconds']) - 0.001
                for item in result['providers'].values() if item['enabled']),
            "whole_history_not_falsely_claimed": result['all_history_complete'] is False}
        result['engineering_checks'] = checks
        result['request_spacing_scope'] = 'publisher-serialized dispatch commitments share the pacer timestamp; not kernel socket sends or WAN arrival gaps'
        result['accepted'] = all(checks.values())
    result['audit_completed_at_utc'] = utc_now()
    result['audit_runtime_seconds'] = time.perf_counter() - audit_started
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print(json.dumps({k: v for k, v in result.items() if k not in {'providers', 'discovery'}}, ensure_ascii=False))
    return 0 if result.get('accepted', True) else 1


if __name__ == '__main__':
    raise SystemExit(main())

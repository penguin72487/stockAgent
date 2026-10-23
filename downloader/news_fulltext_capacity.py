"""Publisher-wide, persistent capacity search using useful acquisition work.

Cadence and response concurrency are separate controls. All observations include
validation and durable acceptance. A pipeline plateau is never a source quota.
This controller owns no HTTP sessions, source data, threads, or second queue.
"""

from __future__ import annotations

from copy import deepcopy
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
from statistics import median
import time

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext_diagnostics import FAILURE_DIAGNOSTIC_BUDGET_CONTRACT_VERSION, HOURLY_EVIDENCE_PROVIDERS
from downloader.news_fulltext_metrics import endpoint_workload
from downloader.news_fulltext_metadata_feeds import access_policy_fingerprint
from downloader.news_fulltext_cnyes_work import CONTRACT_VERSION as CNYES_WORK_DISPATCH_VERSION
from downloader.news_fulltext_cnyes_seed import CONTRACT_VERSION as CNYES_SCOPE_SEED_VERSION
from downloader.news_fulltext_queue import (ARTICLE_RECHECK_DAY_CONTRACT_VERSION, ARTICLE_DISPATCH_CONTRACT_VERSION,
                                           CNYES_UNKNOWN_BODY_RECHECK_CONTRACT_VERSION,
                                           STORED_BODY_RETRY_FAIRNESS_CONTRACT_VERSION)


DEFAULTS = {
    "enabled": False,
    "ladder_rps": [0.1, 0.2, 0.5, 1, 2, 4, 5, 8, 10, 20, 30, 50, 75, 100],
    "window_seconds": 45,
    "minimum_requests": 20,
    "maximum_window_seconds": 600,
    "stable_windows": 3,
    "minimum_gain_fraction": 0.05,
    "maximum_error_fraction": 0.01,
    "recheck_seconds": 3600,
    "probe_through_rps": 30,
}


WORKLOADS = frozenset({"cnyes_bulk", "rss_fulltext", "wordpress_posts", "article_pdf", "article_html", "unclassified", "mixed", "metadata"})
MEASUREMENT_CONTRACT_VERSION = 11
TRANSPORT_RESUME_SELECTION_CONTRACT_VERSION = 3
COMPLETION_OBSERVATION_CONTRACT_VERSION = 1
COMPLETION_OBSERVATION_SECONDS = 45
ACCESS_REFUSAL_RECOVERY_CONTRACT_VERSION = 1


def _valid_access_refusal_recovery(value):
    """A local retry target, never an inferred publisher rate limit."""
    return bool(isinstance(value, dict)
        and type(value.get('contract_version')) is int
        and value['contract_version'] == ACCESS_REFUSAL_RECOVERY_CONTRACT_VERSION
        and all(type(value.get(key)) in {int, float} and math.isfinite(value[key]) and value[key] >= 0
                for key in ('established_at_epoch', 'source_not_before_epoch', 'hold_until_epoch'))
        and all(type(value.get(key)) in {int, float} and math.isfinite(value[key])
                and 0.01 <= value[key] <= 3600
                for key in ('interval_seconds', 'refused_interval_seconds'))
        and type(value.get('refused_parallelism')) is int and value['refused_parallelism'] >= 1
        and type(value.get('parallelism')) is int and value['parallelism'] == 1
        and type(value.get('legacy_checkpoint_adopted')) is bool
        and value.get('basis') == 'conservative_client_recovery_not_source_rate_limit')


def _valid_candidate(candidate, maximum):
    return bool(isinstance(candidate, dict)
                and all(type(candidate.get(k)) in {int, float} and math.isfinite(candidate[k])
                        for k in ("new_heads_per_second", "interval_seconds"))
                and candidate["new_heads_per_second"] >= 0
                and 0.01 <= candidate["interval_seconds"] <= 3600
                and type(candidate.get("parallelism")) is int and 1 <= candidate["parallelism"] <= maximum
                and ("comparable_new_heads_per_second" not in candidate
                     or type(candidate["comparable_new_heads_per_second"]) in {int, float}
                     and math.isfinite(candidate["comparable_new_heads_per_second"]) and candidate["comparable_new_heads_per_second"] >= 0))


def _completed_resume_pressure(sample):
    """Reconcile the same terminal failure domain used by live window health.

    A proven isolated host/REP error remains a raw failure, not a publisher-wide
    pressure result. Require the exact recorded denominators: never infer a
    missing isolation proof, accept a partial projection, or erase provisional
    errors. Older windows without isolation retain their conservative raw scope.
    """
    keys = {'pressure_transport_or_server_errors', 'pressure_request_results', 'pressure_response_results'}
    present = keys & sample.keys()
    isolated_keys = ('isolated_gateway_host_errors', 'isolated_robots_origin_errors',
                     'isolated_robots_origin_transport_errors', 'provisional_gateway_host_errors')
    isolated = {key: sample.get(key, 0) for key in isolated_keys}
    if any(type(value) is not int or value < 0 for value in isolated.values()):
        return None
    raw = (sample['requests'], sample['responses'], sample['transport_or_server_errors'])
    if not present:
        return raw if not any(isolated.values()) else None
    if present != keys or any(type(sample[key]) is not int or sample[key] < 0 for key in keys):
        return None
    removed = isolated['isolated_gateway_host_errors'] + isolated['isolated_robots_origin_errors']
    transport = isolated['isolated_robots_origin_transport_errors']
    if (isolated['provisional_gateway_host_errors'] or removed > min(raw) or raw[1] > raw[0] or raw[2] > raw[0]
            or transport > raw[0] - raw[1] or removed + transport > raw[2]):
        return None
    expected = (raw[0] - removed - transport, raw[1] - removed, raw[2] - removed - transport)
    actual = (sample['pressure_request_results'], sample['pressure_response_results'], sample['pressure_transport_or_server_errors'])
    return actual if actual == expected else None


def recent_transport_resume(item, policy, *, workload, maximum, floor, now,
                            provider_cooling=False, capacity_fingerprint=None):
    """Select repeated recent pressure, not the all-time peak or a new quota.

    This read-only selection shares the unchanged measurement ABI. A stale or
    singly observed peak must not hide another repeatedly verified candidate.
    Group the bounded history by exact workload/cadence/slots, require distinct
    non-overlapping body windows and rank their median durable-head rates. The
    caller still requires a fresh recovery streak before applying the pressure;
    every resumed setting starts another live verification, not a new ETA.
    """
    if workload not in WORKLOADS - {'mixed', 'metadata', 'unclassified'} or provider_cooling:
        return None
    groups = {}
    for sample in item.get('history', [])[-128:]:
        if (sample.get('healthy') is not True or sample.get('workload_family') != workload
                or not _valid_candidate(sample, maximum) or sample['interval_seconds'] + 1e-9 < floor):
            continue
        # Reusing the recovery setting itself cannot restore prior capacity.
        if (sample['interval_seconds'] >= item['interval_seconds'] - 1e-9
                and sample['parallelism'] <= item['parallelism']):
            continue
        ended, wall = sample.get('ended_at_epoch'), sample.get('wall_seconds')
        if (any(type(v) not in {int, float} or not math.isfinite(v) for v in (ended, wall))
                or not 0 <= now - ended <= policy['recheck_seconds'] or wall < policy['window_seconds']):
            continue
        numeric = {k: sample.get(k) for k in ('requests', 'responses', 'new_stored_url_heads',
                   'durable_body_versions', 'transport_or_server_errors', 'unusable_http_responses')}
        if (any(type(v) is not int or v < 0 for v in numeric.values())
                or not numeric['new_stored_url_heads'] or not numeric['durable_body_versions']
                or numeric['unusable_http_responses']):
            continue
        pressure = _completed_resume_pressure(sample)
        if (pressure is None or min(pressure[:2]) < policy['minimum_requests']
                or pressure[2] / pressure[0] > policy['maximum_error_fraction']):
            continue
        statuses = sample.get('http_status_counts')
        if (not isinstance(statuses, dict) or any(type(v) is not int or v < 0 for v in statuses.values())
                or any(statuses.get(str(code), 0) for code in (401, 403, 429, 451))):
            continue
        groups.setdefault((sample['interval_seconds'], sample['parallelism']), []).append(sample)
    ranked = []
    for pressure, samples in groups.items():
        independent, previous_end = [], -math.inf
        for sample in sorted(samples, key=lambda s: (s['ended_at_epoch'], s['wall_seconds'])):
            if sample['ended_at_epoch'] - sample['wall_seconds'] >= previous_end - 1e-6:
                independent.append(sample)
                previous_end = sample['ended_at_epoch']
        if len(independent) < policy['stable_windows']:
            continue
        rate = median(s.get('comparable_new_heads_per_second', s['new_heads_per_second']) for s in independent)
        ranked.append((rate, independent[-1]['ended_at_epoch'], pressure, independent))
    if not ranked:
        return None
    rate, latest, _, samples = max(ranked, key=lambda row: row[:3])
    return deepcopy(samples[-1]), {
        'contract_version': TRANSPORT_RESUME_SELECTION_CONTRACT_VERSION, 'workload_family': workload,
        'selection_basis': 'recent_repeated_exact_capacity_ranked_by_median_durable_heads',
        'eligible_repeated_capacity_groups': len(ranked),
        'recent_healthy_windows_at_exact_capacity': len(samples), 'latest_evidence_epoch': latest,
        'ranking_median_new_heads_per_second': rate, 'independent_nonoverlapping_windows': True,
        'maximum_evidence_age_seconds': policy['recheck_seconds'], 'capacity_fingerprint': capacity_fingerprint,
        'health_basis': 'reconciled_completed_pressure_counts; conservative_raw_counts_only_without_isolation',
        'isolated_events_retained_as_failures': True,
        'throughput_estimate_reused': False, 'provider_permission_changed': False,
    }


def capacity_config(config, registry):
    raw = config.get("adaptive_rate", {})
    if not isinstance(raw, dict):
        raise ValueError("adaptive_rate must be an object")
    result = {**deepcopy(DEFAULTS), **raw}
    if type(result["enabled"]) is not bool or set(raw) - set(DEFAULTS):
        raise ValueError("unknown/invalid adaptive news capacity setting")
    ladder = result["ladder_rps"]
    if (not isinstance(ladder, list) or not 2 <= len(ladder) <= 32
            or any(type(x) not in {float, int} or not math.isfinite(x) or not 0 < x <= 100 for x in ladder)
            or ladder != sorted(set(ladder))):
        raise ValueError("news rate ladder must be strictly increasing and bounded at 100 req/s")
    through = result["probe_through_rps"]
    if type(through) not in {float, int} or not math.isfinite(through) or not 0 < through <= ladder[-1]:
        raise ValueError("invalid minimum cadence search range")
    for key in ("window_seconds", "minimum_requests", "maximum_window_seconds", "stable_windows", "recheck_seconds"):
        if type(result[key]) is not int or result[key] <= 0:
            raise ValueError("invalid adaptive news capacity count: " + key)
    if (not 10 <= result["window_seconds"] <= 600 or result["maximum_window_seconds"] < result["window_seconds"]
            or result["maximum_window_seconds"] > 3600 or result["minimum_requests"] < 2
            or not 2 <= result["stable_windows"] <= 20 or not 300 <= result["recheck_seconds"] <= 604800):
        raise ValueError("adaptive news capacity observation bounds invalid")
    for key in ("minimum_gain_fraction", "maximum_error_fraction"):
        if type(result[key]) not in {float, int} or not math.isfinite(result[key]) or not 0 <= result[key] < 0.5:
            raise ValueError("invalid adaptive news capacity fraction")
    providers = {entry["id"]: entry for entry in config["providers"]}
    for provider, source in registry.sources.items():
        maximum = providers[provider].get("capacity_max_parallelism", source.parallelism)
        if type(maximum) is not int or not source.parallelism <= maximum <= 8:
            raise ValueError("adaptive publisher concurrency must be baseline..8")
        recovery_ceiling = providers[provider].get("capacity_transient_backoff_max_seconds", max(2.0, source.interval_seconds))
        if (type(recovery_ceiling) not in {int, float} or not math.isfinite(recovery_ceiling)
                or not source.interval_seconds <= recovery_ceiling <= 3600):
            raise ValueError("invalid bounded transient capacity backoff")
    if result["enabled"] and config.get("require_provider_worker_reservations", False) and sum(providers[p].get("capacity_max_parallelism", s.parallelism)
                                 for p, s in registry.sources.items() if s.request_enabled) > config["workers"]:
        raise ValueError("worker budget cannot reserve maximum adaptive publisher concurrency")
    return result


class PublisherCapacity:
    def __init__(self, http, config, *, clock=time.monotonic, epoch=time.time):
        self.http, self.clock, self.epoch = http, clock, epoch
        self.policy = capacity_config(config, http.registry)
        entries = {entry["id"]: entry for entry in config["providers"]}
        self.transient_backoff_limits = {p: entries[p].get("capacity_transient_backoff_max_seconds", max(2.0, s.interval_seconds))
                                        for p, s in http.registry.sources.items() if s.request_enabled}
        self.path = Path(http.root) / "rate_calibration.json"
        focus_identity = {}
        if config.get('failure_diagnostic_focus') is not None:
            from downloader.news_fulltext_failure_focus import fingerprint, CONTRACT_VERSION as focus_version
            focus_identity = {'private_failure_evidence_focus_contract': focus_version,
                              'private_failure_evidence_focus_manifest_sha256': fingerprint(config['failure_diagnostic_focus'])}
        self.states, self.baselines = {}, {}
        # ETA observes completed work on a fixed clock, independently of rate
        # experiments. A slow source/error-budget trial can take ten minutes;
        # that must not suppress current progress or discard failure/wait time.
        # Never restore these counters/windows into a different owner.
        self.completion_baselines, self.completion_observations = {}, {}
        self.last_tick = -math.inf
        self.fingerprint = hashlib.sha256(json.dumps({
            "version": 1, "measurement_contract_version": MEASUREMENT_CONTRACT_VERSION, "policy": self.policy,
            "cnyes_body_value_dispatch_contract": CNYES_WORK_DISPATCH_VERSION,
            "cnyes_scope_seed_maintenance_contract": CNYES_SCOPE_SEED_VERSION,
            "private_failure_evidence_budget_contract": FAILURE_DIAGNOSTIC_BUDGET_CONTRACT_VERSION,
            **focus_identity,
            "article_recheck_day_contract_version": ARTICLE_RECHECK_DAY_CONTRACT_VERSION,
            "article_dispatch_contract_version": ARTICLE_DISPATCH_CONTRACT_VERSION,
            "stored_body_retry_fairness_contract_version": STORED_BODY_RETRY_FAIRNESS_CONTRACT_VERSION,
            "stored_body_retry_burst": config.get('stored_body_retry_burst', 8),
            "cnyes_unknown_body_recheck_contract_version": CNYES_UNKNOWN_BODY_RECHECK_CONTRACT_VERSION,
            "providers": {p: {"source": s.http_capacity_source_fingerprint or s.data_fingerprint,
                               "authorization": s.http_capacity_authorization_fingerprint or s.authorization_fingerprint,
                               "maximum_parallelism": http.maximum_parallelism[p]}
                          for p, s in http.registry.sources.items() if s.request_enabled},
        }, sort_keys=True).encode()).hexdigest()
        self.provider_fingerprints = {p: hashlib.sha256(json.dumps({"measurement_contract_version": MEASUREMENT_CONTRACT_VERSION,
            "policy": self.policy, "source": s.http_capacity_source_fingerprint or s.data_fingerprint,
            "authorization": s.http_capacity_authorization_fingerprint or s.authorization_fingerprint,
            "article_recheck_day_contract_version": ARTICLE_RECHECK_DAY_CONTRACT_VERSION,
            "article_dispatch_contract_version": ARTICLE_DISPATCH_CONTRACT_VERSION,
            "stored_body_retry_fairness_contract_version": STORED_BODY_RETRY_FAIRNESS_CONTRACT_VERSION,
            "stored_body_retry_burst": config.get('stored_body_retry_burst', 8),
            "maximum_parallelism": http.maximum_parallelism[p],
            **({'metadata_feed_fingerprint': access_policy_fingerprint(s)}
               if not s.allowed and s.metadata_feeds else {}),
            **({'cnyes_body_value_dispatch_contract': CNYES_WORK_DISPATCH_VERSION,
                'cnyes_unknown_body_recheck_contract_version': CNYES_UNKNOWN_BODY_RECHECK_CONTRACT_VERSION,
                'cnyes_scope_seed_maintenance_contract': CNYES_SCOPE_SEED_VERSION,
                **focus_identity,
                'cnyes_primary_body_burst': config.get('cnyes_primary_body_burst', 8)} if p == 'cnyes' else {}),
            **({'private_failure_evidence_budget_contract': FAILURE_DIAGNOSTIC_BUDGET_CONTRACT_VERSION}
               if p in HOURLY_EVIDENCE_PROVIDERS else {})}, sort_keys=True).encode()).hexdigest()
            for p, s in http.registry.sources.items() if s.request_enabled}
        try:
            saved = json.loads(self.path.read_text())
            if isinstance(saved, dict) and saved.get("version") == 1 and isinstance(saved.get("providers"), dict):
                self.states = {p: row for p, row in saved["providers"].items() if isinstance(row, dict)
                    and (row.get("capacity_fingerprint") == self.provider_fingerprints.get(p)
                         or saved.get("fingerprint") == self.fingerprint)}
        except (OSError, ValueError, KeyError, TypeError):
            pass
        self.states = {p: self.states[p] for p in http.limiters if p in self.states}
        for provider in http.limiters:
            state = self.states.get(provider, {})
            if not isinstance(state, dict):
                state = {}
            source = http.registry.sources[provider]
            interval, slots = state.get("interval_seconds"), state.get("parallelism")
            if (type(interval) not in {int, float} or not math.isfinite(interval)
                    or not 1 / self.policy["ladder_rps"][-1] <= interval <= 3600
                    or type(slots) is not int or not 1 <= slots <= http.maximum_parallelism[provider]
                    or not isinstance(state.get("history"), list) or len(state["history"]) > 128
                    or any(not isinstance(row, dict) for row in state["history"])
                    or any(type(state.get(k)) is not int or state[k] < 0 for k in ("plateaus", "stable_samples"))
                    or type(state.get("transport_recovery_active", False)) is not bool
                    or type(state.get("next_search_epoch")) not in {float, int}
                    or not math.isfinite(state["next_search_epoch"])
                    or state.get("best") is not None and not _valid_candidate(state["best"], http.maximum_parallelism[provider])):
                state = {"interval_seconds": source.interval_seconds, "parallelism": source.parallelism,
                         "state": "warming", "history": [], "best": None, "plateaus": 0,
                         "stable_samples": 0, "next_search_epoch": 0}
            self.states[provider] = state
            state.pop('completion_observation', None)  # Reporting windows are owner-local, never resumed.
            # A new owner has a new observation baseline. Never carry an
            # unverified partial recovery streak across that discontinuity.
            state["transport_stable_samples"] = 0
            state.setdefault("transport_recovery_active", False)
            state.setdefault("best_by_workload", {})
            if (not isinstance(state["best_by_workload"], dict) or len(state["best_by_workload"]) > len(WORKLOADS)
                    or any(k not in WORKLOADS or not _valid_candidate(v, http.maximum_parallelism[provider])
                           for k, v in state["best_by_workload"].items())):
                state["best_by_workload"] = {}
            state["capacity_fingerprint"] = self.provider_fingerprints[provider]
            recovery = state.get('access_refusal_recovery')
            if recovery is not None and not _valid_access_refusal_recovery(recovery):
                state.pop('access_refusal_recovery')
            if (not state.get('access_refusal_recovery')
                    and state.get('state') == 'waiting_source' and state.get('reason') == 'http_access_refusal'
                    and max(state['next_search_epoch'], http.provider_not_before.get(provider, 0)) > self.epoch()):
                # Adopt an already refused checkpoint once, without extending
                # its source deadline/search hold or discarding old samples.
                self._record_access_refusal(provider, legacy=True)
            recovery = self._active_access_refusal_recovery(provider)
            if recovery:
                state['interval_seconds'] = max(state['interval_seconds'], recovery['interval_seconds'])
                state['parallelism'] = 1
            http.set_client_capacity(provider, state["interval_seconds"], state["parallelism"])
            state["interval_seconds"] = http.effective_interval(provider)
        self.tick(force=True)

    def snapshot(self, *, for_status=False):
        # The durable calibration keeps all 128 windows for recovery/ranking.
        # A heartbeat only needs the latest 64: at the validated >=10-second
        # observation cadence they cover the whole <=600-second ETA horizon,
        # including bad/zero windows. Never select just healthy/fast samples.
        # Slice BEFORE copying; avoid copying discarded diagnostic subtrees.
        from downloader.news_fulltext_status import CAPACITY_STATUS_HISTORY_LIMIT
        providers = ({provider: {key: deepcopy(value[-CAPACITY_STATUS_HISTORY_LIMIT:]
                         if key == 'history' else value) for key, value in state.items()}
                      for provider, state in self.states.items()}
                     if for_status else deepcopy(self.states))
        result = {"version": 1, "measurement_contract_version": MEASUREMENT_CONTRACT_VERSION, "fingerprint": self.fingerprint,
                "completion_window_owner_contract_version": 1,
                "completion_observation_contract_version": COMPLETION_OBSERVATION_CONTRACT_VERSION,
                "transport_resume_selection_contract_version": TRANSPORT_RESUME_SELECTION_CONTRACT_VERSION,
                "access_refusal_recovery_contract_version": ACCESS_REFUSAL_RECOVERY_CONTRACT_VERSION,
                "stored_body_retry_fairness_contract_version": STORED_BODY_RETRY_FAIRNESS_CONTRACT_VERSION,
                "clock_basis": "independent publisher windows, including validation/fsync and waits; no fabricated source quota",
                "providers": providers, "official_maximum_claimed": False}
        for provider, state in providers.items():
            observation = self.completion_observations.get(provider, {'history': [], 'last_window': None})
            state['completion_observation'] = {
                'history': deepcopy(observation['history'][-CAPACITY_STATUS_HISTORY_LIMIT:]
                                    if for_status else observation['history']),
                'last_window': deepcopy(observation['last_window']),
            }
        if for_status:
            result['heartbeat_projection'] = {
                'contract_version': 1, 'history_limit': CAPACITY_STATUS_HISTORY_LIMIT,
                'selection': 'latest consecutive windows, including errors and zero progress',
                'full_history_preserved': True, 'detail_authority': 'rate_calibration.json',
                'omitted_history_windows': {provider: max(0, len(state['history']) - CAPACITY_STATUS_HISTORY_LIMIT)
                                           for provider, state in self.states.items()},
            }
        return result

    def _observe_completion(self, observations, now, owner):
        """Fixed, consecutive owner-local counter deltas, not health samples.

        Only independent durable URL heads count. Cached replay, revisions and
        in-flight requests are not acquired heads; zero progress and all waits
        remain in the denominator. Calibration resets never reset this clock.
        """
        ended = self.epoch()
        for provider in self.states:
            counts = observations.get(provider, {}).get('counts', {})
            heads = max(0, counts.get('durably_accepted_new_url_heads', 0)
                        - counts.get('durably_accepted_cached_new_url_heads', 0))
            baseline = self.completion_baselines.get(provider)
            if baseline is None or baseline[0] != owner or heads < baseline[2]:
                self.completion_baselines[provider] = (owner, now, heads)
                self.completion_observations[provider] = {'history': [], 'last_window': None}
                continue
            wall = now - baseline[1]
            if wall < COMPLETION_OBSERVATION_SECONDS:
                continue
            sample = {'owner_id': owner, 'ended_at_epoch': ended, 'wall_seconds': wall,
                      'new_stored_url_heads': heads - baseline[2]}
            observation = self.completion_observations[provider]
            observation['last_window'] = sample
            observation['history'] = (observation['history'] + [sample])[-128:]
            self.completion_baselines[provider] = (owner, now, heads)

    def _reset(self, provider, metrics, now):
        self.baselines[provider] = (now, deepcopy(metrics.get("counts", {})))

    def _set(self, provider, interval, slots, state, reason):
        recovery = self._active_access_refusal_recovery(provider)
        if recovery:
            interval, slots = max(interval, recovery['interval_seconds']), 1
        self.http.set_client_capacity(provider, interval, slots)
        item = self.states[provider]
        item.pop('pending_observation', None)
        item.update(interval_seconds=self.http.effective_interval(provider), parallelism=self.http.parallelism[provider],
                    state=state, reason=reason, stable_samples=0, transport_stable_samples=0)
        if state in {"warming", "verifying", "ramping"}:
            item["transport_recovery_active"] = False

    def _active_access_refusal_recovery(self, provider):
        recovery = self.states[provider].get('access_refusal_recovery')
        return recovery if recovery and recovery['hold_until_epoch'] > self.epoch() else None

    def _record_access_refusal(self, provider, *, legacy=False):
        """Coalesce a refused in-flight burst, then retry at lower client pressure.

        401/403/451/challenges do not establish a numeric rate limit. Preserve
        the independent HTTP refusal, REP and 429 floors. The existing bounded
        search hold keeps old fast samples from immediately undoing recovery.
        """
        item, now = self.states[provider], self.epoch()
        deadline = self.http.provider_not_before.get(provider, 0)
        previous = item.get('access_refusal_recovery')
        if previous and previous['source_not_before_epoch'] > now:
            previous['source_not_before_epoch'] = max(previous['source_not_before_epoch'], deadline)
            previous['hold_until_epoch'] = max(previous['hold_until_epoch'], deadline)
            return
        source = self.http.registry.sources[provider]
        refused = item['interval_seconds']
        item['access_refusal_recovery'] = dict(
            contract_version=ACCESS_REFUSAL_RECOVERY_CONTRACT_VERSION,
            established_at_epoch=now, source_not_before_epoch=deadline,
            hold_until_epoch=max(deadline, item.get('next_search_epoch', 0),
                                 now if legacy else now + self.policy['recheck_seconds']),
            refused_interval_seconds=refused, refused_parallelism=item['parallelism'],
            interval_seconds=min(3600.0, max(source.interval_seconds, 2 * refused)), parallelism=1,
            legacy_checkpoint_adopted=legacy,
            basis='conservative_client_recovery_not_source_rate_limit')
        self.http.metrics.count(provider, 'access_refusal_conservative_recoveries')

    def _transport_resume_candidate(self, provider, workload):
        """Reuse pressure, not an old throughput estimate or permission grant.

        The enclosing state has the current source/authorization/measurement
        fingerprint. Require repeated recent online body windows at the exact
        same workload/cadence/slots; mixed, metadata and cached work cannot
        establish this proof. Current HTTP floors/circuits remain authoritative.
        """
        if self._active_access_refusal_recovery(provider):
            return None  # Old fast-body samples cannot undo a current refusal recovery.
        now = self.epoch()
        return recent_transport_resume(self.states[provider], self.policy, workload=workload,
            maximum=self.http.maximum_parallelism[provider], floor=self.http.hard_interval_floor(provider), now=now,
            provider_cooling=self.http.provider_not_before.get(provider, 0) > now,
            capacity_fingerprint=self.provider_fingerprints[provider])

    def tick(self, *, force=False):
        now = self.clock()
        if not force and now - self.last_tick < 1:
            return
        self.last_tick = now
        measured = self.http.metrics.snapshot()
        observations = measured.get("providers", {})
        self._observe_completion(observations, now, measured.get('owner_id'))
        # Reporting is already persisted by the normal status heartbeat. It
        # does not require an additional whole calibration-file write/fsync.
        changed = False
        for provider, item in self.states.items():
            observed = observations.get(provider, {})
            source = self.http.registry.sources[provider]
            if not source.allowed and source.metadata_feeds:
                # A rolling information feed has no body-throughput target.
                # Poll its declared jobs; never manufacture extra requests to
                # populate a rate ladder or claim an article capacity ceiling.
                item['state'] = 'metadata_polling'
                item['reason'] = 'registered RSS information cadence, not body capacity calibration'
                self._reset(provider, observed, now)
                continue
            if provider not in self.baselines:
                self._reset(provider, observed, now)
                continue
            start, previous = self.baselines[provider]
            wall = now - start
            counts = observed.get("counts", {})
            delta = {key: max(0, value - previous.get(key, 0)) for key, value in counts.items()}
            refusals = sum(delta.get("http_status_" + str(code), 0) for code in (401, 403, 451)) + delta.get("content_access_refusals", 0)
            throttled = delta.get("http_status_429", 0) > 0
            terminal_results = "http_terminal_requests" in counts
            errors = (delta.get("http_terminal_server_errors", 0) + delta.get("http_terminal_transport_errors", 0)
                      + delta.get("http_terminal_deadline_errors", 0) if terminal_results else
                      delta.get("http_transient_errors", 0) + delta.get("transport_errors", 0)
                      + delta.get("response_deadline_errors", 0))
            isolated_errors = min(errors, delta.get('http_terminal_isolated_gateway_host_errors', 0)) if terminal_results else 0
            isolated_robots_errors = min(errors - isolated_errors,
                delta.get('http_terminal_isolated_robots_origin_errors', 0)) if terminal_results else 0
            isolated_response_errors = isolated_errors + isolated_robots_errors
            isolated_robots_transport = min(errors - isolated_response_errors,
                delta.get('http_terminal_isolated_robots_origin_transport_errors', 0)) if terminal_results else 0
            all_isolated_errors = isolated_response_errors + isolated_robots_transport
            provisional_errors = min(errors - all_isolated_errors,
                delta.get('http_terminal_provisional_gateway_host_errors', 0)) if terminal_results else 0
            pressure_errors = errors - all_isolated_errors
            # Negotiation/schema/path failures are not healthy source capacity.
            # They need diagnosis, not a faster cadence, even when other work
            # produced bodies in the same publisher-wide window.
            status_prefix = "http_terminal_status_" if terminal_results else "http_status_"
            unusable_responses = delta.get("http_terminal_unusable_bodies", 0) + sum(v for k, v in delta.items() if k.startswith(status_prefix)
                                     and k.removeprefix(status_prefix).isdigit()
                                     and 400 <= int(k.removeprefix(status_prefix)) < 500
                                     and int(k.removeprefix(status_prefix)) not in {404, 410})
            if throttled or refusals or self.http.provider_not_before.get(provider, 0) > self.epoch():
                if item["state"] != "waiting_source" or throttled or refusals:
                    if throttled or refusals:
                        item["transport_recovery_active"] = False
                    if refusals:
                        self._record_access_refusal(provider)
                    item["refused_interval_seconds"] = item["interval_seconds"] if throttled else None
                    selected = item.get("best") if not refusals else None
                    interval = max(self.http.effective_interval(provider), selected["interval_seconds"]) if selected else self.http.effective_interval(provider)
                    slots = min(item["parallelism"], selected["parallelism"]) if selected else item["parallelism"]
                    self._set(provider, interval, slots, "waiting_source",
                              "http_429" if throttled else "http_access_refusal" if refusals else "source_circuit")
                    item["next_search_epoch"] = self.epoch() + self.policy["recheck_seconds"]
                    item["plateaus"] = 0
                    changed = True
                self._reset(provider, observed, now)
                continue
            if item["state"] == "waiting_source":
                transient = item.get("reason") == "source_circuit"
                interval = min(self.http.client_intervals[provider], self.transient_backoff_limits[provider]) if transient else self.http.effective_interval(provider)
                self._set(provider, interval, item["parallelism"], "recovering_transport" if transient else "verifying",
                          "cooldown_elapsed; bounded transport recovery" if transient else "cooldown_elapsed; verify source pacing before any new search")
                self._reset(provider, observed, now)
                changed = True
                continue
            effective = self.http.effective_interval(provider)
            if effective > item["interval_seconds"] + 1e-9:
                self._set(provider, effective, item["parallelism"], "warming", "new official/robots/learned floor")
                self._reset(provider, observed, now)
                changed = True
                continue
            if wall < self.policy["window_seconds"]:
                continue
            started_requests = delta.get("http_requests_started", 0)
            requests = delta.get("http_terminal_requests", 0) if terminal_results else started_requests
            responses = delta.get("http_terminal_responses", 0) if terminal_results else delta.get("http_responses_received", 0)
            pressure_requests, pressure_responses = requests - all_isolated_errors, responses - isolated_response_errors
            if min(pressure_requests, pressure_responses) < self.policy["minimum_requests"] and wall < self.policy["maximum_window_seconds"]:
                continue
            heads = max(0, delta.get('durably_accepted_new_url_heads', 0) - delta.get('durably_accepted_cached_new_url_heads', 0))
            versions = max(0, delta.get('durably_accepted_versions', 0) - delta.get('durably_accepted_cached_versions', 0))
            # A single completed failure cannot estimate a 1% error budget
            # from a 20--99-result window. Keep the SAME pressure (no ramp)
            # until there is one error-budget denominator, a second error or
            # the bounded observation deadline. The HTTP layer still honors
            # refusals, Retry-After and consecutive-error circuits immediately.
            fraction = self.policy["maximum_error_fraction"]
            result_target = math.ceil(1 / fraction) if fraction else 0
            if (pressure_errors == 1 and not unusable_responses and fraction > 0
                    and pressure_requests >= max(20, self.policy["minimum_requests"])
                    and (pressure_requests if terminal_results else responses + delta.get("transport_errors", 0)) < result_target
                    and (heads and versions or item['state'] == 'recovering_transport' or item['transport_recovery_active'])
                    and wall < self.policy["maximum_window_seconds"]):
                if 'pending_observation' not in item:
                    item.update(state='recovering_transport' if item['state'] == 'recovering_transport' or item['transport_recovery_active'] else 'verifying_transient_error',
                                reason="one isolated failure; retain capacity and extend independent sample, never ramp",
                                pending_observation={"requests": requests, "responses": responses,
                                                     "errors": pressure_errors, "required_completed_results": result_target,
                                                     "maximum_wall_seconds": self.policy["maximum_window_seconds"]})
                    changed = True
                continue
            item.pop("pending_observation", None)
            endpoints = {k.removeprefix("endpoint_").removesuffix("_requests"): v
                         for k, v in delta.items() if k.startswith("endpoint_") and k.endswith("_requests")}
            completed_endpoints = {k.removeprefix("http_endpoint_").removesuffix("_terminal_requests"): v
                                   for k, v in delta.items() if k.startswith("http_endpoint_") and k.endswith("_terminal_requests")}
            workloads = Counter()
            for endpoint, count in (completed_endpoints if terminal_results else endpoints).items():
                workloads[endpoint_workload(endpoint)] += count
            workload = workloads.most_common(1)[0][0] if workloads else "unclassified"
            if workloads and workloads[workload] < 0.8 * max(1, sum(workloads.values())):
                workload = "mixed"
            if item.get("workload_family") != workload:
                item.update(workload_family=workload, best=item["best_by_workload"].get(workload), plateaus=0)
                if item["state"] != "recovering_transport":
                    item["stable_samples"] = 0
            workload_heads = {k.removeprefix("durable_workload_").removesuffix("_new_heads"): v
                              for k, v in delta.items() if k.startswith("durable_workload_") and k.endswith("_new_heads")}
            comparable_heads = workload_heads.get(workload, 0) if workload != "unclassified" else heads
            sample = {"ended_at_epoch": self.epoch(), "wall_seconds": wall, "interval_seconds": effective,
                      "owner_id": self.http.metrics.owner_id,
                      "parallelism": item["parallelism"], "requests": requests, "responses": responses,
                      "started_requests": started_requests,
                      "request_measurement_basis": "terminal_results" if terminal_results else "legacy_started_requests",
                      "new_stored_url_heads": heads, "durable_body_versions": versions,
                      "actual_requests_per_second": started_requests / wall, "new_heads_per_second": heads / wall,
                      "durable_versions_per_request": versions / responses if responses else None,
                      "validated_bulk_ids": delta.get("bulk_article_ids_validated", 0), "transport_or_server_errors": errors,
                      "isolated_gateway_host_errors": isolated_errors,
                      "isolated_robots_origin_errors": isolated_robots_errors,
                      "isolated_robots_origin_transport_errors": isolated_robots_transport,
                      "provisional_gateway_host_errors": provisional_errors,
                      "pressure_transport_or_server_errors": pressure_errors,
                      "pressure_request_results": pressure_requests, "pressure_response_results": pressure_responses,
                      "all_observed_hosts_healthy": not errors and not unusable_responses,
                      "health_scope": "non-isolated completed results; only fresh completed other-host proof isolates REP502/504 or certificate-failure pressure; certificate failures remove no HTTP response; provisional gateway errors retain pressure; all isolated failures remain raw errors, not bodies or permission",
                      "opening_http_inflight": previous.get("http_inflight", 0),
                      "closing_http_inflight": counts.get("http_inflight", 0),
                      "http_status_counts": {k.removeprefix(status_prefix): v for k, v in delta.items() if k.startswith(status_prefix)},
                      "unusable_http_responses": unusable_responses,
                      "endpoint_request_counts": endpoints, "workload_family": workload,
                      "endpoint_completed_request_counts": completed_endpoints,
                      "workload_head_counts": workload_heads, "comparable_new_heads_per_second": comparable_heads / wall,
                      "healthy": bool(pressure_responses >= self.policy["minimum_requests"]
                                      and not provisional_errors
                                      and (pressure_errors + unusable_responses) / max(1, pressure_requests) <= self.policy["maximum_error_fraction"])}
            item["last_window"] = sample
            item["history"] = (item.get("history", []) + [sample])[-128:]
            changed = True
            self._reset(provider, observed, now)
            if not sample["healthy"]:
                item["transport_stable_samples"] = 0
                if pressure_errors:
                    # A timeout/gateway outage is not evidence of a permanent
                    # requests-per-second quota. Circuit deadlines already stop
                    # requests; never multiply cadence until recovery samples
                    # become impossible (256/512 seconds per request).
                    if item["transport_recovery_active"]:
                        # Repeated errors at the already-reduced pressure do
                        # not demonstrate that fewer response slots help.
                        # Ratcheting 4 -> 3 -> 2 -> 1 merely serializes each
                        # stalled socket. Hold the ONE bounded recovery step;
                        # do not ramp, and keep resetting the health streak.
                        # Consecutive failures, overload, Retry-After and
                        # refusals still open the independent HTTP circuit.
                        self._set(provider, effective, item["parallelism"], "recovering_transport",
                                  "hold bounded transport recovery pressure; repeated errors are not a measured rate limit")
                        self.http.metrics.count(provider, "transient_recovery_pressure_held")
                    else:
                        self._set(provider, min(self.transient_backoff_limits[provider], effective * 2),
                                  max(1, item["parallelism"] - 1), "recovering_transport",
                                  "bounded transient recovery; not proof of rate limit")
                        item["transport_recovery_active"] = True
                        self.http.metrics.count(provider, "transient_recovery_pressure_reduced_once")
                    item["next_search_epoch"] = self.epoch() + self.policy["recheck_seconds"]
                elif unusable_responses:
                    item.update(state="endpoint_validation_failed", reason="request/representation error; slower cadence cannot repair endpoint semantics")
                else:
                    item.update(state="insufficient_samples", reason="no adequate independent request/response sample; do not escalate")
                continue
            if item["state"] == "recovering_transport" or item["transport_recovery_active"]:
                # Healthy 304s may prove transport recovery, not new-history
                # throughput. Restore the reviewed baseline after repeated
                # transport windows; ordinary capacity search still needs bodies.
                # HTTP recovery is publisher-wide; a healthy HTML/RSS/API mix
                # at the SAME pressure must not erase it. Body-capacity proof
                # below remains isolated to the current workload family.
                item["transport_stable_samples"] += 1
                item["stable_samples"] = item["transport_stable_samples"]
                item["state"] = "recovering_transport"
                if item["transport_stable_samples"] >= self.policy["stable_windows"]:
                    source = self.http.registry.sources[provider]
                    resume = self._transport_resume_candidate(provider, workload) if heads and versions and comparable_heads else None
                    if resume:
                        candidate, proof = resume
                        self._set(provider, candidate['interval_seconds'], candidate['parallelism'], 'verifying',
                                  'healthy transport restored recent same-workload stable capacity; reverify current throughput')
                        item['transport_resume_evidence'] = proof
                        item['next_search_epoch'] = self.epoch() + self.policy['recheck_seconds']
                        self.http.metrics.count(provider, 'transient_recovery_prior_capacity_restored')
                    else:
                        self._set(provider, source.interval_seconds, source.parallelism, "warming",
                                  "healthy transport restored reviewed baseline; useful-body search still required")
                        item["next_search_epoch"] = 0
                continue
            # An idle/duplicate/metadata-only page must not be promoted because
            # a cheap HTTP 200 happened to arrive quickly.
            if not versions or not heads:
                item.update(state="insufficient_useful_data", reason="no new durable versions; no higher-rate proof")
                continue
            if workload in {"mixed", "metadata"} or not comparable_heads:
                item.update(state="insufficient_useful_data", reason="mixed/metadata requests cannot establish a comparable body-capacity window")
                continue
            best = item.get("best")
            best_rate = best.get("comparable_new_heads_per_second", best["new_heads_per_second"]) if best else None
            if best is None or sample["comparable_new_heads_per_second"] > best_rate:
                item["best"] = deepcopy(sample)
                item["best_by_workload"][workload] = deepcopy(sample)
            if item.get("next_search_epoch", 0) > self.epoch():
                item["stable_samples"] = item.get("stable_samples", 0) + 1
                if item["stable_samples"] >= self.policy["stable_windows"]:
                    item.update(state="stable_source_backoff" if item.get("refused_interval_seconds") else "stable_pipeline_capacity",
                                reason="repeated healthy full-pipeline windows; source maximum not established")
                continue
            floor = self.http.hard_interval_floor(provider)
            if floor > 0 and effective <= floor + 1e-9:
                item.update(state="official_or_learned_floor", reason="respect robots/official/refusal pacing; no higher-rate probes")
                item["next_search_epoch"] = self.epoch() + self.policy["recheck_seconds"]
                continue
            # When the client cannot fill its requested cadence, measure extra
            # response slots before raising an already-underfilled target.
            underfilled = sample["actual_requests_per_second"] < 0.8 / effective
            if best and sample["comparable_new_heads_per_second"] <= best_rate * (1 + self.policy["minimum_gain_fraction"]):
                item["plateaus"] = item.get("plateaus", 0) + 1
            else:
                item["plateaus"] = 0
            if item["plateaus"] >= 2 and underfilled and 1 / effective >= self.policy["probe_through_rps"] - 1e-6:
                selected = item["best"]
                self._set(provider, selected["interval_seconds"], selected["parallelism"], "verifying",
                          "additional cadence/slots did not improve durable throughput; verify best observed pipeline candidate")
                item["next_search_epoch"] = self.epoch() + self.policy["recheck_seconds"]
            elif underfilled and item["parallelism"] < self.http.maximum_parallelism[provider]:
                self._set(provider, effective, item["parallelism"] + 1, "ramping", "underfilled cadence; measure one additional response slot")
            else:
                candidates = [rate for rate in self.policy["ladder_rps"] if rate > 1 / effective + 1e-6]
                if candidates:
                    self._set(provider, 1 / candidates[0], item["parallelism"], "ramping", "healthy useful window; next cadence step")
                else:
                    item.update(state="engineering_ceiling_unverified", reason="finite client resource ceiling reached, not a provider limit")
                    item["next_search_epoch"] = self.epoch() + self.policy["recheck_seconds"]
        if changed or force:
            atomic_write_json(self.path, self.snapshot())

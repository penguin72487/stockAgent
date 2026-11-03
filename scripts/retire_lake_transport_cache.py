#!/usr/bin/env python3
"""One bounded recovery-gated temporary wave; primary D/NAS are preserved."""
from datetime import datetime, timezone
import fcntl
import json
import math
from pathlib import Path
import statistics
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.control.lakehouse import (
    acceptance, catalog_enrollment, configuration, guard, retire_accepted_transport,
)
from stockagent.data_sync.backup_transport_cache import retirement_failure_reason
from stockagent.data_sync.immutable_transport_cache import VALIDATION_CONTRACT
from stockagent.data_sync.work_progress import observe_work

RETRY_SECONDS = 600
MAX_RETRY_SECONDS = 3600
HISTORY_CONTRACT = 'stockagent.transport_gc_timing.v1'
GC_TIMER_INTERVAL_SECONDS = 30  # Original OnUnitInactiveSec, not an API limit.


def workload(row, journal=None):
    """Reuse the owner's existing sealed metadata, never scan a payload for ETA."""
    files = journal.get('files') if isinstance(journal, dict) else None
    if isinstance(files, dict):
        sizes = [v.get('bytes') for v in files.values() if isinstance(v, dict)]
        if len(sizes) == len(files) and all(type(v) is int and v >= 0 for v in sizes):
            return {'items': len(files), 'bytes': sum(sizes)}
    keys, size = row.get('file_keys'), row.get('bytes')
    if isinstance(keys, list) and type(size) is int and size >= 0:
        return {'items': len(keys)+2, 'bytes': size}
    return None


def duration_model(samples, kind, work):
    """Whole-workflow timings include admission, both gates, hashing and unlink.

    File count and bytes are distinct costs. Use the larger workload ratio,
    keeping fixed admission time; one sample is explicitly low confidence.
    These are empirical estimates, never a successful-recovery prediction.
    """
    if not work or not work['items']:
        return None
    estimates = []
    for row in samples:
        if (row.get('kind') != kind or row.get('validation_contract') != VALIDATION_CONTRACT
                or not all(type(row.get(k)) in (int, float) and math.isfinite(row[k]) and row[k] > 0
                           for k in ('items', 'seconds'))
                or type(row.get('bytes')) is not int or row['bytes'] < 0):
            continue
        ratios = [work['items']/row['items']]
        if row['bytes']:
            ratios.append(work['bytes']/row['bytes'])
        elif work['bytes']:
            continue
        fixed = row.get('admission_seconds', 0)
        fixed = fixed if type(fixed) in (int, float) and math.isfinite(fixed) and 0 <= fixed <= row['seconds'] else 0
        estimates.append(fixed+(row['seconds']-fixed)*max(ratios))
    if not estimates:
        return None
    return {'typical_seconds': statistics.median(estimates),
            'fastest_seconds': min(estimates)*0.5, 'slowest_seconds': max(estimates)*2,
            'sample_count': len(estimates), 'confidence': 'low' if len(estimates) < 3 else 'medium'}


class ProgressWriter:
    """Small telemetry owned by this exact GC invocation; not a new worker."""
    def __init__(self, base, status, candidates, works, *, started_at):
        self.base, self.status, self.started_at = base, status, started_at
        self.candidates, self.works = candidates, works
        self.phase, self.iteration, self.last_write = None, 0, float('-inf')
        self.latest = {}
        self.history_path = base/'transport-gc-timing.json'
        self.samples = []
        self.history_writable = True
        try:
            if self.history_path.is_symlink() or (self.history_path.exists() and self.history_path.stat().st_size > 65536):
                self.history_writable = False
            elif self.history_path.exists():
                old = json.loads(self.history_path.read_bytes())
                if old.get('contract') == HISTORY_CONTRACT and isinstance(old.get('samples'), list):
                    self.samples = [r for r in old['samples'][-48:] if isinstance(r, dict)]
                else:
                    self.history_writable = False
        except (OSError, ValueError, TypeError):
            self.history_writable = False

    def remember(self, result, work):
        identity, seconds = result.get('delivery_identity_sha256'), result.get('complete_workflow_seconds')
        if (result.get('state') != 'retired' or not work or not identity
                or type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds <= 0
                or any(r.get('delivery_identity_sha256') == identity for r in self.samples)):
            return
        self.samples = (self.samples+[{'delivery_identity_sha256': identity,
            'validation_contract': VALIDATION_CONTRACT, 'kind': result.get('delivery_kind'),
            **work, 'seconds': seconds, 'admission_seconds': result.get('nas_admission_seconds', 0)}])[-48:]
        if self.history_writable:
            try:
                atomic_write_json(self.history_path, {'contract': HISTORY_CONTRACT, 'samples': self.samples})
            except OSError:
                self.history_writable = False

    def forecasts(self, now):
        result, predictions, unknown = {}, [], 0
        for identity, kind in self.candidates:
            model = duration_model(self.samples, kind, self.works.get(identity))
            if model is None:
                unknown += 1
                continue
            predictions.append(model)
            if identity == self.status.get('delivery_identity_sha256'):
                result['completion_forecast'] = self._forecast(model, self.started_at, now, 'current_delivery')
        if not predictions:
            result['queue_forecast'] = {'state': 'sampling' if self.candidates else 'idle',
                'scope': 'ready_queue', 'estimated_jobs': 0, 'unknown_jobs': unknown,
                'retry_jobs': self.status.get('retry_wait_count', 0)}
        else:
            # This owner is serial. Add complete future workflows; never sum
            # phases or rates from independent machines as if they were serial.
            total = {field: sum(p[field] for p in predictions)
                     for field in ('typical_seconds', 'fastest_seconds', 'slowest_seconds')}
            dispatch_wait = GC_TIMER_INTERVAL_SECONDS * max(0, len(predictions) -
                (1 if self.status.get('state') == 'retiring_exact_accepted_transport' else 0))
            total = {field: seconds+dispatch_wait for field, seconds in total.items()}
            total.update(sample_count=min(p['sample_count'] for p in predictions),
                         confidence='low' if any(p['confidence'] == 'low' for p in predictions) else 'medium')
            result['queue_forecast'] = {**self._forecast(total, self.started_at, now, 'ready_queue'),
                'estimated_jobs': len(predictions), 'unknown_jobs': unknown,
                'retry_jobs': self.status.get('retry_wait_count', 0),
                'dispatch_wait_seconds': dispatch_wait}
            if unknown:
                result['queue_forecast']['state'] = 'partial'
                for field in ('completion_at_utc', 'fastest_completion_at_utc', 'slowest_completion_at_utc'):
                    result['queue_forecast'].pop(field, None)
        return result

    @staticmethod
    def _forecast(model, anchor, now, scope):
        return {'state': 'estimated' if anchor+model['typical_seconds'] > now else 'overdue',
                'scope': scope, 'basis': 'recent_complete_workflows',
                'generated_at_utc': datetime.fromtimestamp(now, timezone.utc).isoformat(),
                'completion_at_utc': datetime.fromtimestamp(anchor+model['typical_seconds'], timezone.utc).isoformat(),
                'fastest_completion_at_utc': datetime.fromtimestamp(anchor+model['fastest_seconds'], timezone.utc).isoformat(),
                'slowest_completion_at_utc': datetime.fromtimestamp(anchor+model['slowest_seconds'], timezone.utc).isoformat(),
                'sample_count': model['sample_count'], 'confidence': model['confidence']}

    def __call__(self, event):
        now, monotonic = time.time(), time.perf_counter()
        phase = (event.get('work_pass'), event['phase'])
        changed = phase != self.phase
        if changed:
            self.phase = phase
            self.iteration += 1
            self.phase_started = now
        self.latest = {'phase': event['phase'], 'phase_iteration': self.iteration,
            'work_pass': event.get('work_pass'), 'work_progress': event,
            'phase_started_at_utc': datetime.fromtimestamp(self.phase_started, timezone.utc).isoformat()}
        complete = event.get('total_items') is not None and event.get('completed_items') == event['total_items']
        # Each file advances in memory; durable status is bounded to one write
        # per two seconds or a phase boundary. No extra payload reads/fsyncs.
        if changed or monotonic-self.last_write >= 2 or complete:
            snapshot = {**self.status, **self.latest, **self.forecasts(now),
                        'workflow_started_at_utc': datetime.fromtimestamp(self.started_at, timezone.utc).isoformat(),
                        'observed_at_utc': datetime.fromtimestamp(now, timezone.utc).isoformat()}
            try:
                atomic_write_json(self.base/'transport-gc-status.json', snapshot)
                self.last_write = monotonic
            except OSError:
                pass  # Telemetry failure cannot replace any recovery gate.


def retire_one(c, base, *, now=None):
    """Fairly retry one wave; a rejected recovery gate cannot starve others."""
    started = time.perf_counter()
    now = time.time() if now is None else now
    ledger_path = base / 'source-replication-ledger.json'
    ledger = json.loads(ledger_path.read_bytes()) if ledger_path.exists() else {'deliveries': {}}
    schedule_path = base / 'transport-gc-schedule.json'
    if schedule_path.is_symlink():
        raise ValueError('transport GC schedule is redirected')
    schedule = json.loads(schedule_path.read_bytes()) if schedule_path.exists() else {
        'contract': 'immutable_transport_gc_schedule_v1', 'attempts': {}}
    if schedule.get('contract') != 'immutable_transport_gc_schedule_v1' or not isinstance(schedule.get('attempts'), dict):
        raise ValueError('transport GC schedule contract differs')
    candidates = [(identity, 'source') for identity, row in ledger['deliveries'].items()
                  if row.get('nas_acceptance', {}).get('state') == 'nas_archive_file_recovery_verified']
    for path in sorted(base.glob('catalog-acceptance-*.json')):
        if path.is_symlink():
            raise ValueError('catalog acceptance checkpoint is redirected')
        value = json.loads(path.read_bytes())
        identity = value['delivery_identity_sha256']
        if path.name != 'catalog-acceptance-' + identity + '.json' or value['state'] != 'nas_archive_file_recovery_verified':
            raise ValueError('catalog acceptance checkpoint identity differs')
        candidates.append((identity, 'catalog'))
    status_path = base / 'transport-gc-status.json'
    previous_status = json.loads(status_path.read_bytes()) if status_path.exists() else {}
    previous_kind = previous_status.get('delivery_kind')
    source_status = base / 'source-replication-status.json'
    source = json.loads(source_status.read_bytes()) if source_status.exists() else {}
    policy = c.get('source_replication', {})
    used, quota, wave = (source.get('retained_transport_bytes'),
                         policy.get('maximum_retained_transport_bytes'), policy.get('maximum_wave_bytes'))
    pressure = (source.get('state') == 'waiting_transport_capacity'
                or all(type(v) is int for v in (used, quota, wave)) and used + wave > quota)
    ready, waiting, works = [], [], {}
    for identity, kind in candidates:
        journal = base / ('cache-retirement-' + identity + '.json')
        recorded = json.loads(journal.read_bytes()) if journal.exists() else {}
        if recorded.get('state') == 'retired':
            if identity == previous_status.get('delivery_identity_sha256'):
                works[identity] = workload(ledger['deliveries'].get(identity, {}), recorded)
            continue
        works[identity] = workload(ledger['deliveries'].get(identity, {}), recorded)
        attempt = schedule['attempts'].get(identity, {})
        if (attempt.get('retry_at_epoch', 0) > now
                and attempt.get('validation_contract') == VALIDATION_CONTRACT):
            waiting.append((attempt['retry_at_epoch'], identity))
        else:
            ready.append((identity, kind))
    # Give a repaired validator one prompt ordinary attempt. Otherwise
    # untouched waves precede retried waves. Under source-capacity pressure,
    # larger raw waves free headroom before costly small-file catalog copies;
    # ordinary capacity keeps the original kind alternation. All recovery gates
    # and individual cooldowns still apply to the selected wave.
    def validator_repaired(identity):
        prior = schedule['attempts'].get(identity, {})
        return bool(prior) and prior.get('validation_contract') != VALIDATION_CONTRACT
    ready.sort(key=lambda item: (not validator_repaired(item[0]),
                                 schedule['attempts'].get(item[0], {}).get('last_attempt_epoch', 0),
                                 (item[1] != 'source' if pressure else item[1] == previous_kind),
                                 -ledger['deliveries'][item[0]].get('bytes', 0) if pressure and item[1] == 'source' else 0))
    result = {'contract': 'stockagent.lake_transport_gc_status.v1',
              'state': 'no_accepted_transport_candidates', 'retry_wait_count': len(waiting),
              'ready_candidate_count': len(ready), 'source_capacity_pressure': pressure}
    progress = ProgressWriter(base, result, ready, works, started_at=now)
    # Upgrade from the last original owner's full workflow timing, if present.
    # The retirement journal supplies workload only, never inferred completion.
    progress.remember(previous_status, works.get(previous_status.get('delivery_identity_sha256')))
    result.update(progress.forecasts(time.time()))
    if not ready:
        if waiting:
            retry_at, retry_identity = min(waiting)
            error = schedule['attempts'][retry_identity].get('error')
            # Polling the timer must not erase the last failed gate's cause.
            # Detailed exceptions remain in the original private schedule.
            reason = error.get('reason_code') if isinstance(error, dict) else None
            result.update(state='waiting_transport_retry', phase='waiting_retirement_retry',
                          next_retry_at_epoch=retry_at, reason_code=reason or 'retirement_retry')
        return result
    identity, kind = ready[0]
    attempt = schedule['attempts'].setdefault(identity, {})
    changed_validator = bool(attempt) and attempt.get('validation_contract') != VALIDATION_CONTRACT
    if changed_validator:
        # A repaired validator gets one ordinary attempt without inheriting
        # an obsolete failure's hour-long wait. Preserve that failure first;
        # the same validator keeps the original cooldown on every later poll.
        attempt.setdefault('validator_transitions', []).append({
            'previous_contract': attempt.get('validation_contract'),
            'previous_retry_at_epoch': attempt.get('retry_at_epoch'),
            'previous_error': attempt.get('error'),
            'previous_attempt_count': attempt.get('attempt_count', 0),
            'new_contract': VALIDATION_CONTRACT,
        })
    count = attempt.get('attempt_count', 0) + 1
    # Persist the attempt before expensive gates so an interrupted owner also
    # yields to other waves on its next invocation. This is not a recovery ACK.
    attempt.update(state='checking', attempt_count=count, last_attempt_epoch=now,
                   retry_at_epoch=now + RETRY_SECONDS, validation_contract=VALIDATION_CONTRACT)
    atomic_write_json(schedule_path, schedule, durable=True)
    result.update(state='retiring_exact_accepted_transport', delivery_identity_sha256=identity,
                  delivery_kind=kind, repaired_validator_retry=changed_validator,
                  ready_candidate_count=len(ready)-1)
    atomic_write_json(status_path, {**result, 'observed_at_utc': datetime.now(timezone.utc).isoformat()})
    try:
        # Admission authenticates the exact paired NAS restore against the
        # original sealed producer enrollment. It need not hash the sender's
        # whole HDD tree again: the canonical retirement below independently
        # checks the complete transport and primary bytes before any unlink.
        options = {}
        if kind == 'source':
            enrolled = ledger['deliveries'][identity]
            if 'file_keys' in enrolled:
                options['enrolled_source'] = enrolled
        else:
            enrolled = catalog_enrollment(c, identity)
            if enrolled is not None:
                options['enrolled_catalog'] = enrolled
        with observe_work(progress):
            phase_started = time.perf_counter()
            progress({'phase': 'admit_paired_nas_recovery', 'total_items': 1, 'completed_items': 0})
            proof = acceptance(c, identity, **options)
            admission_seconds = time.perf_counter() - phase_started
            result['nas_admission_seconds'] = admission_seconds
            progress({'phase': 'verify_and_retire_transport', 'total_items': None, 'completed_items': 0})
            retirement = retire_accepted_transport(c, identity, proof)
        result['nas_admission_seconds'] = admission_seconds
    except (OSError, ValueError, KeyError, TypeError) as error:
        retirement = {'state': 'retirement_deferred', 'error_type': type(error).__name__,
                      'reason_code': retirement_failure_reason(error), 'detail': str(error)[:512]}
        if isinstance(error, OSError):
            retirement.update(errno=error.errno, error_path=str(error.filename) if error.filename else None)
    # The exact prepared/renamed/retired journal remains authoritative in its
    # original file. Minute observers need a bounded summary, not another copy
    # of thousands of source signatures and the complete embedded manifest.
    summary_fields = ('contract', 'state', 'delivery_identity_sha256', 'reclaimed_bytes',
                      'primary_source_files_deleted', 'nas_archive_files_deleted',
                      'inventory_and_recovery_verified', 'error_type', 'reason_code',
                      'detail', 'errno', 'error_path')
    result.update(state=retirement['state'],
                  retirement={key: retirement[key] for key in summary_fields if key in retirement})
    if retirement['state'] != 'retired':
        result['reason_code'] = retirement.get('reason_code', 'retirement_validation')
    attempt.update(state=retirement['state'])
    if retirement['state'] == 'retired':
        attempt['retry_at_epoch'] = 0
        attempt.pop('error', None)
    else:
        attempt['retry_at_epoch'] = time.time() + min(MAX_RETRY_SECONDS, RETRY_SECONDS * 2 ** min(count - 1, 3))
        attempt['error'] = retirement
        result['next_retry_at_epoch'] = attempt['retry_at_epoch']
        result['retry_wait_count'] = len(waiting)+1
    atomic_write_json(schedule_path, schedule, durable=True)
    result.update(progress.latest, complete_workflow_seconds=time.perf_counter()-started)
    if retirement['state'] == 'retired':
        journal = base/('cache-retirement-'+identity+'.json')
        work = works.get(identity)
        if work is None and journal.exists():
            work = workload({}, json.loads(journal.read_bytes()))
        progress.remember(result, work)
        progress.candidates = ready[1:]
        progress.started_at = time.time()
        result.pop('completion_forecast', None)
    else:
        result.pop('completion_forecast', None)
        # A failed gate is not an unusually slow success sample. Retry time
        # remains visible, but successful completion is conditional/unbounded.
        progress.candidates = []
    result.update(progress.forecasts(time.time()))
    return result


def main():
    c = configuration();guard(c)
    base = Path(c['state_root'])
    with (base / 'transport-gc-owner.lock').open('a') as owner:
        try:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 75
        started = time.perf_counter()
        result = retire_one(c, base)
        result.update(observed_at_utc=datetime.now(timezone.utc).isoformat(),
                      complete_workflow_seconds=time.perf_counter() - started,
                      source_files_deleted=0, nas_archive_files_deleted=0)
        atomic_write_json(base / 'transport-gc-status.json', result)
        print(json.dumps(result))
        return 75 if result['state'] in ('retirement_deferred', 'waiting_transport_retry') else 0


if __name__ == '__main__':
    raise SystemExit(main())

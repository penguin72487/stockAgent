#!/usr/bin/env python3
"""One bounded recovery-gated temporary wave; primary D/NAS are preserved."""
from datetime import datetime, timezone
import fcntl
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.control.lakehouse import acceptance, configuration, guard, retire_accepted_transport

RETRY_SECONDS = 600
MAX_RETRY_SECONDS = 3600


def retire_one(c, base, *, now=None):
    """Fairly retry one wave; a rejected recovery gate cannot starve others."""
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
    previous_kind = json.loads(status_path.read_bytes()).get('delivery_kind') if status_path.exists() else None
    source_status = base / 'source-replication-status.json'
    source = json.loads(source_status.read_bytes()) if source_status.exists() else {}
    policy = c.get('source_replication', {})
    used, quota, wave = (source.get('retained_transport_bytes'),
                         policy.get('maximum_retained_transport_bytes'), policy.get('maximum_wave_bytes'))
    pressure = (source.get('state') == 'waiting_transport_capacity'
                or all(type(v) is int for v in (used, quota, wave)) and used + wave > quota)
    ready, waiting = [], []
    for identity, kind in candidates:
        journal = base / ('cache-retirement-' + identity + '.json')
        if journal.exists() and json.loads(journal.read_bytes()).get('state') == 'retired':
            continue
        attempt = schedule['attempts'].get(identity, {})
        if attempt.get('retry_at_epoch', 0) > now:
            waiting.append(attempt['retry_at_epoch'])
        else:
            ready.append((identity, kind))
    # Untouched waves precede retried waves. Under source-capacity pressure,
    # larger raw waves free headroom before costly small-file catalog copies;
    # ordinary capacity keeps the original kind alternation. All recovery gates
    # and individual cooldowns still apply to the selected wave.
    ready.sort(key=lambda item: (schedule['attempts'].get(item[0], {}).get('last_attempt_epoch', 0),
                                 (item[1] != 'source' if pressure else item[1] == previous_kind),
                                 -ledger['deliveries'][item[0]].get('bytes', 0) if pressure and item[1] == 'source' else 0))
    result = {'state': 'no_accepted_transport_candidates', 'retry_wait_count': len(waiting),
              'ready_candidate_count': len(ready), 'source_capacity_pressure': pressure}
    if not ready:
        if waiting:
            result.update(state='waiting_transport_retry', next_retry_at_epoch=min(waiting))
        return result
    identity, kind = ready[0]
    attempt = schedule['attempts'].setdefault(identity, {})
    count = attempt.get('attempt_count', 0) + 1
    # Persist the attempt before expensive gates so an interrupted owner also
    # yields to other waves on its next invocation. This is not a recovery ACK.
    attempt.update(state='checking', attempt_count=count, last_attempt_epoch=now,
                   retry_at_epoch=now + RETRY_SECONDS)
    atomic_write_json(schedule_path, schedule, durable=True)
    result.update(state='retiring_exact_accepted_transport', delivery_identity_sha256=identity,
                  delivery_kind=kind)
    atomic_write_json(status_path, {**result, 'observed_at_utc': datetime.now(timezone.utc).isoformat()})
    try:
        proof = acceptance(c, identity)
        retirement = retire_accepted_transport(c, identity, proof)
    except (OSError, ValueError, KeyError, TypeError) as error:
        retirement = {'state': 'retirement_deferred', 'error_type': type(error).__name__}
        if isinstance(error, OSError):
            retirement.update(errno=error.errno, error_path=str(error.filename) if error.filename else None)
    result.update(state=retirement['state'], retirement=retirement)
    attempt.update(state=retirement['state'])
    if retirement['state'] == 'retired':
        attempt['retry_at_epoch'] = 0
        attempt.pop('error', None)
    else:
        attempt['retry_at_epoch'] = time.time() + min(MAX_RETRY_SECONDS, RETRY_SECONDS * 2 ** min(count - 1, 3))
        attempt['error'] = retirement
        result['next_retry_at_epoch'] = attempt['retry_at_epoch']
    atomic_write_json(schedule_path, schedule, durable=True)
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

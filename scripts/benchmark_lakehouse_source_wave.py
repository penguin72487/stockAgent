"""Replay a complete no-new-object source cycle from fixed native inputs.

Use the locked lakehouse role. Real mount guards and READ_ONLY registry queries
remain; all state/status publications target private copies. This measures the
whole source-monitoring cycle, not catalog capture, payload copy or NAS restore.
"""
from __future__ import annotations

import argparse
import ast
from contextlib import ExitStack
from datetime import UTC, datetime
import gc
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import time
from unittest.mock import patch

from downloader.artifact_io import atomic_write_json, atomic_write_text
from scripts.benchmark_lakehouse_source_signature import limits, module
from scripts.verify_lakehouse_workflow_history import owner_identity
from stockagent.control import lakehouse
from stockagent.data_sync import nas_coverage
from stockagent.runtime_identity import identity_sha256

SOURCE_SQL = ("SELECT DISTINCT relative, sha256, bytes FROM lake.source_objects "
              "WHERE role IN ('packed_object','unreferenced_cold_object') ORDER BY bytes,relative,sha256")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def validate_code_scope(reference, candidate):
    """Only the selected source-cycle body may differ from the pinned reference."""
    def remainder(source):
        tree = ast.parse(source)
        selected = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                    and node.name == 'stage_source_wave']
        if len(selected) != 1:
            raise ValueError('one canonical source-cycle function required')
        tree.body.remove(selected[0])
        return ast.dump(tree, include_attributes=False)
    if remainder(reference) != remainder(candidate):
        raise ValueError('unrelated source changes invalidate this comparator')


def match_native_limits():
    """Adjust only this exact supervised engineering cgroup, never production."""
    own = Path('/proc/self/cgroup').read_text().split('0::', 1)[1].strip()
    unit = Path(own).name
    if not unit.startswith('stockagent-agent-lakehouse-source-wave-') or not unit.endswith('.service'):
        raise ValueError('resource matching requires the dedicated supervised engineering run')
    owner = owner_identity()
    pid = int(owner['MainPID'])
    native = Path('/sys/fs/cgroup/system.slice/stockagent-storage-lifecycle.service')
    quota, period = (native / 'cpu.max').read_text().split()
    if quota == 'max':
        raise ValueError('native resource comparator requires its bounded CPU quota')
    subprocess.run(['systemctl', 'set-property', '--runtime', unit,
        f'CPUQuota={int(quota) / int(period) * 100:g}%',
        'MemoryHigh=' + (native / 'memory.high').read_text().strip(),
        'MemoryMax=' + (native / 'memory.max').read_text().strip()], check=True, timeout=10)
    os.sched_setaffinity(0, os.sched_getaffinity(pid))
    os.setpriority(os.PRIO_PROCESS, 0, os.getpriority(os.PRIO_PROCESS, pid))
    return limits()


def capture_inputs(c, root):
    state = Path(c['state_root'])
    template = root / 'state-template'
    template.mkdir(mode=0o700)
    copied = {}

    def copy(path, destination):
        before = path.stat()
        raw = path.read_bytes()
        after = path.stat()
        if (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise ValueError('native input changed while making private fixture')
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        destination.write_bytes(raw)
        destination.chmod(0o600)
        copied[str(path)] = {'sha256': sha(raw), 'bytes': len(raw)}

    for name in ('source-replication-ledger.json', 'source-replication-status.json',
                 'source-catalog-refresh.json', 'source-wave-current.json', 'pending-transport-scan.json'):
        path = state / name
        if path.exists():
            copy(path, template / name)
    ledger = json.loads((template / 'source-replication-ledger.json').read_bytes())
    if not ledger['deliveries']:
        raise ValueError('a real source ledger is required')
    receipts = Path(c['receipt_root'])
    missing_receipts = pending_receipt_absences(ledger, receipts)
    # An outstanding delivery remains outstanding. Freeze only the observed
    # absence, never manufacture a NAS ACK or consume a newly arriving proof.
    (template / 'receipts').mkdir(mode=0o700)
    if (receipts / 'relay-status.json').exists():
        copy(receipts / 'relay-status.json', template / 'receipts/relay-status.json')
    copied['pending_receipt_absences'] = missing_receipts
    for identity in ledger['deliveries']:
        path = state / ('cache-retirement-' + identity + '.json')
        if path.exists():
            copy(path, template / path.name)
    scan = template / 'pending-transport-scan.json'
    if scan.exists() and json.loads(scan.read_bytes()).get('state') == 'retry_scan':
        raise ValueError('active transport scan is outside a no-op replay')
    policy = lakehouse.source_wave_policy(c)
    restic = root / 'restic'
    copy(Path(policy['restic_state_root']) / 'ledger.json', restic / 'ledger.json')
    with_connection = lakehouse.connect(c, read_only=True)
    try:
        rows = with_connection.execute(SOURCE_SQL).fetchall()
    finally:
        with_connection.close()
    published = {key for row in ledger['deliveries'].values() for key in row['file_keys']}
    if not rows or any(row[0] + '@' + row[1] not in published for row in rows):
        raise ValueError('new source objects need publication, not this monitoring benchmark')
    return template, restic, rows, copied


def pending_receipt_absences(ledger, receipts):
    missing = []
    for identity, row in ledger['deliveries'].items():
        proof = row.get('nas_acceptance')
        if proof:
            if proof.get('state') != 'nas_archive_file_recovery_verified':
                raise ValueError('an unverified proof cannot become a NAS ACK')
            continue
        path = receipts / ('lake-' + identity + '.json')
        if path.exists():
            raise ValueError('let the native owner consume its arriving NAS receipt first')
        missing.append({'path': str(path), 'exists': False})
    return missing


def validate_ledger_transition(root, original, persisted):
    """Permit only the original cycle's exact receipt-backed retirement update."""
    expected = json.loads(json.dumps(original))
    for identity, row in expected['deliveries'].items():
        path = root / ('cache-retirement-' + identity + '.json')
        if row.get('nas_acceptance', {}).get('state') == 'nas_archive_file_recovery_verified' and path.exists():
            receipt = json.loads(path.read_bytes())
            row['transport_retirement'] = {k: receipt[k] for k in
                ('state', 'reclaimed_bytes', 'primary_source_files_deleted', 'nas_archive_files_deleted')}
    if persisted != expected:
        raise ValueError('monitoring changed source membership, NAS proofs or unreceipted retirement state')


def monitoring_window_ready(refresh, status, now):
    """A real fresh no-new-object cycle; pending NAS work is not a new source."""
    phase = status.get('state')
    pending = status.get('pending_delivery_ids')
    phase_consistent = (isinstance(pending, list) and
        ((phase == 'monitoring_increments' and pending == []) or
         (phase == 'waiting_nas_archive_acceptance' and bool(pending))))
    return (refresh.get('state') == 'registered'
        and refresh.get('retry_at_epoch', 0) - now.timestamp() > 120
        and phase_consistent
        and status.get('unpublished_source_object_count') == 0
        and datetime.fromisoformat(status['observed_at_utc'])
            >= datetime.fromisoformat(refresh['observed_at_utc']))


def wait_for_native_window(c, owner):
    deadline = time.monotonic() + 180
    state = Path(c['state_root'])
    while True:
        if owner_identity() != owner or not Path('/proc/' + owner['MainPID']).is_dir():
            raise ValueError('native owner changed or exited while observing its publication boundary')
        refresh = json.loads((state / 'source-catalog-refresh.json').read_bytes())
        status = json.loads((state / 'source-replication-status.json').read_bytes())
        if monitoring_window_ready(refresh, status, datetime.now(UTC)):
            return
        if time.monotonic() >= deadline:
            raise ValueError('native no-new-object window not observed; do not manufacture eligibility')
        print(json.dumps({'state': 'verified_wait_native_monitoring_window',
                          'native_owner_unchanged': True, 'source_phase': status.get('state')}), flush=True)
        time.sleep(5)


def run_trial(active, c, root, template, restic, rows, clock):
    shutil.copytree(template, root)
    private = {**c, 'state_root': str(root), 'receipt_root': str(root / 'receipts'), 'source_replication': {
        **c.get('source_replication', {}), 'restic_state_root': str(restic)}}
    original_files = {p.relative_to(root).as_posix(): sha(p.read_bytes())
                      for p in root.rglob('*') if p.is_file()}
    original_ledger = json.loads((root / 'source-replication-ledger.json').read_bytes())
    connections, reads = [], []
    real_connect, real_read = lakehouse.connect, nas_coverage.read_ledger

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock.astimezone(tz)

    class Connection:
        def __init__(self):
            self.native = real_connect(c, read_only=True)

        def execute(self, sql):
            if sql != SOURCE_SQL:
                raise ValueError('unexpected registry action in monitoring replay')
            self.native.execute(sql)
            return self

        def fetchall(self):
            actual = self.native.fetchall()
            if actual != rows:
                raise ValueError('native readonly registry changed; reject comparison')
            return actual

        def close(self):
            self.native.close()

    def connect(config, **options):
        if config != private or options != {'read_only': True}:
            raise ValueError('engineering replay cannot open a writable registry')
        connections.append(True)
        return Connection()

    def read(path):
        if path != restic / 'ledger.json':
            raise ValueError('unexpected priority ledger path')
        reads.append(path.stat().st_size)
        return real_read(path)

    def forbidden(*args, **kwargs):
        raise ValueError('not a no-new-object source cycle; no live publication is allowed')

    gc.collect()
    resources = limits()
    start, cpu = time.perf_counter(), time.process_time()
    with ExitStack() as stack:
        for name, value in (('datetime', Clock), ('connect', connect), ('register', forbidden),
                            ('notify_transport', forbidden), ('_publish_source_wave', forbidden),
                            ('acceptance', forbidden)):
            stack.enter_context(patch.object(active, name, value))
        stack.enter_context(patch.object(nas_coverage, 'read_ledger', read))
        stack.enter_context(patch.object(active.time, 'time', return_value=clock.timestamp()))
        result = active.stage_source_wave(private)
        persisted = json.loads((root / 'source-replication-status.json').read_bytes())
        if persisted != result or connections != [True] or result['unpublished_source_object_count'] != 0:
            raise ValueError('complete source workflow or native publication readback differs')
        atomic_write_json(root / 'durable-readback.json', persisted, durable=True)
        if json.loads((root / 'durable-readback.json').read_bytes()) != result:
            raise ValueError('durable complete result differs')
        persisted_ledger_raw = (root / 'source-replication-ledger.json').read_bytes()
        persisted_ledger = json.loads(persisted_ledger_raw)
    measured = {'wall_seconds': time.perf_counter() - start,
                'cpu_seconds': time.process_time() - cpu}
    if limits() != resources:
        raise ValueError('resource comparator changed during replay')
    preserved = all(sha((root / name).read_bytes()) == digest for name, digest in original_files.items()
                    if name not in ('source-replication-status.json', 'source-replication-ledger.json'))
    if not preserved:
        raise ValueError('monitoring changed a recovery journal, priority, relay or retirement receipt')
    validate_ledger_transition(root, original_ledger, persisted_ledger)
    return {**measured, 'semantic_output_sha256': identity_sha256(result),
            'complete_output': result, 'pinned_nonledger_inputs_preserved': preserved,
            'source_membership_and_nas_proofs_preserved': True,
            'persisted_ledger_sha256': sha(persisted_ledger_raw),
            'ledger_advanced_by_original_retirement_receipts': original_ledger != persisted_ledger,
            'restic_reads': len(reads), 'restic_read_bytes': sum(reads),
            'resource_limits': resources, 'readonly_connections': len(connections)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-root', required=True, type=Path)
    parser.add_argument('--reference-revision', required=True)
    args = parser.parse_args(argv)
    root = args.output_root.absolute()
    if root.exists() or not root.resolve().is_relative_to(Path('artifacts/benchmarks').resolve()):
        parser.error('fresh private root under artifacts/benchmarks required')
    root.mkdir(mode=0o700, parents=True)
    resource_limits = match_native_limits()
    owner = owner_identity()
    c = lakehouse.configuration()
    lakehouse.guard(c)
    wait_for_native_window(c, owner)
    original = subprocess.check_output(['git', 'show', args.reference_revision + ':stockagent/control/lakehouse.py'], text=True)
    candidate = Path('stockagent/control/lakehouse.py').read_text()
    validate_code_scope(original, candidate)
    for label, source in (('baseline', original), ('candidate', candidate)):
        atomic_write_text(root / (label + '.py'), source, mode=0o600, durable=True)
    template, restic, rows, inputs = capture_inputs(c, root)
    clock = datetime.now(UTC)
    policy = lakehouse.source_wave_policy(c)
    refresh = template / 'source-catalog-refresh.json'
    if policy['refresh_source_catalog'] and (not refresh.exists()
            or json.loads(refresh.read_bytes()).get('retry_at_epoch', 0) <= clock.timestamp()):
        raise ValueError('native catalog refresh is due; do not manufacture a fresh checkpoint')
    variants = {label: module(source, label + '_wave') for label, source in (('baseline', original), ('candidate', candidate))}
    fixture = {'clock': clock.isoformat(), 'native_owner': owner, 'registry_rows': len(rows),
               'registry_rows_sha256': identity_sha256(rows), 'native_inputs': inputs,
               'source_code_sha256': {'baseline': sha(original.encode()), 'candidate': sha(candidate.encode())},
               'resource_limits': resource_limits, 'free_fixture_disk_bytes': shutil.disk_usage(root).free}
    atomic_write_json(root / 'fixture.json', fixture, durable=True)
    for label, active in variants.items():
        warm = run_trial(active, c, root / ('warmup-' + label), template, restic, rows, clock)
        atomic_write_json(root / ('warmup-' + label + '.json'), warm, durable=True)
    trials = []
    for index, label in enumerate(('baseline', 'candidate', 'candidate', 'baseline', 'candidate', 'baseline', 'baseline', 'candidate')):
        trial = run_trial(variants[label], c, root / f'trial-{index}-{label}', template, restic, rows, clock)
        trials.append({'label': label, **trial})
        atomic_write_json(root / f'trial-{index}.json', trials[-1], durable=True)
        print(json.dumps({'label': label, 'wall_seconds': trial['wall_seconds'], 'cpu_seconds': trial['cpu_seconds']}), flush=True)
    means = {label: {key: statistics.mean(t[key] for t in trials if t['label'] == label)
                     for key in ('wall_seconds', 'cpu_seconds', 'restic_read_bytes')} for label in variants}
    equal = len({(t['semantic_output_sha256'], t['persisted_ledger_sha256']) for t in trials}) == 1
    stable = owner_identity() == owner and Path('stockagent/control/lakehouse.py').read_text() == candidate
    accepted = equal and stable and all(t['restic_reads'] == int(t['label'] == 'baseline') for t in trials)
    result = {'state': 'accepted_complete_source_monitoring_replay' if accepted else 'rejected_source_monitoring_replay',
              'observed_at_utc': datetime.now(UTC).isoformat(), 'complete_output_parity': equal,
              'native_owner_and_source_code_unchanged': stable, 'registry_rows': len(rows),
              'means': means, 'trials': trials, 'fixture_identity_sha256': identity_sha256(fixture),
              'whole_source_cycle_improved': all(means['candidate'][key] < means['baseline'][key]
                                                 for key in ('wall_seconds', 'cpu_seconds')),
              'source_writes': 0, 'production_catalog_writes': 0, 'provider_calls': 0,
              'production_service_controls': 0,
              'caveats': ['Fixed native state and clock, real mount guards and full live READ_ONLY registry queries.',
                          'Pending NAS deliveries preserve observed missing receipts; they never acquire synthetic acceptance.',
                          'Original receipt-backed retirement ledger updates are allowed and compared in full; no payload is removed.',
                          'No-op source cycle, not payload transfer, full catalog capture, NAS restore or all-service throughput.',
                          'A reused benchmark process does not establish fresh-process or native RAM reduction.']}
    atomic_write_json(root / 'measurement.json', result, durable=True)
    if not accepted:
        raise ValueError('complete source cycle parity/stability not accepted')
    print(json.dumps({k: v for k, v in result.items() if k != 'trials'}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

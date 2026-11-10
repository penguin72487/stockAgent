#!/usr/bin/env python3
"""Build an E-only dedup candidate for an explicitly held fixed D head.

The canonical dataset writer lock and existing stream source pin are shared
with the original owner. This worker cannot publish or retire any source.
"""
from __future__ import annotations

from contextlib import ExitStack, closing
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.run_bounded_storage_maintenance import TradingPriorityGuard
from scripts.run_source_local_repack_queue import load_generation_holds, prior_published
from stockagent.data_sync import dedup_stage_handoff, dedup_stream_candidate as candidate, packed_snapshots as p, source_repack_bloat, stream_repack as s
from stockagent.data_sync import workspace_volume
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.completed_forward_capacity import reserve_capacity
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

CONTRACT = 'stockagent.dedup_candidate_queue.v1'
COLD = Path('/srv/stockagent-packed')


def process_identity() -> dict:
    fields = Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()
    return {'pid': os.getpid(), 'start_ticks': int(fields[19]),
            'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}


def retry_delay(failures: int, error: BaseException) -> int:
    """Bound retries, leaving at least one timer turn for another dataset."""
    if type(failures) is not int or failures < 1:
        raise ValueError('dedup candidate failure count is invalid')
    if isinstance(error, p.PackedPublicationBusy):
        # The systemd timer runs at one-minute intervals. A busy owner is a
        # transient contention event, not a failed payload; retry on the next
        # timer turn while allowing the rest of this queue to run first.
        return 60
    return min(600, 120 * 2**min(failures-1, 3))


def plan_summary(index: Path, dataset: str, expected: str) -> dict:
    """Read only a bounded planning identity before capacity admission."""
    if (index.is_symlink() or not index.is_file() or index.resolve(strict=True) != index
            or index.stat().st_nlink != 1):
        raise SnapshotError('dedup planning index is unavailable')
    with closing(sqlite3.connect(index.as_uri()+'?mode=ro', uri=True)) as db:
        row = db.execute("SELECT value FROM meta WHERE key='summary'").fetchone()
        totals = db.execute('SELECT COUNT(*),COALESCE(SUM(size),0),COALESCE(SUM(occurrences),0),'
                            'COALESCE(SUM(size*occurrences),0) FROM contents').fetchone()
    if row is None or len(row[0]) > 1024*1024:
        raise SnapshotError('dedup planning summary is missing or oversized')
    summary = json.loads(row[0])
    if (not isinstance(summary, dict) or summary.get('dataset') != dataset
            or summary.get('old_manifest_sha256') != expected
            or summary.get('contract') != 'stockagent.packed_dedup_plan.v1'
            or type(summary.get('unique_content_logical_bytes')) is not int
            or summary['unique_content_logical_bytes'] < 0
            or totals != (summary.get('unique_contents'), summary['unique_content_logical_bytes'],
                          summary.get('files'), summary.get('logical_bytes'))):
        raise SnapshotError('dedup planning summary identity differs')
    return summary


def growth_budget(summary: dict, manifest: dict, *, completed_unique_bytes=0) -> int:
    """Include representative bytes, both metadata indexes and one read buffer."""
    largest = max((int(item['bytes']) for item in manifest['archive']['objects']
                   if item['kind'] == 'pack'), default=0)
    if (type(completed_unique_bytes) is not int or not 0 <= completed_unique_bytes <=
            summary['unique_content_logical_bytes']):
        raise SnapshotError('dedup candidate checkpoint exceeds its unique source bytes')
    return (summary['unique_content_logical_bytes'] - completed_unique_bytes + int(manifest['source']['files'])*2048
            + 4*int(manifest['archive']['inventory']['bytes']) + largest + 2*1024**3)


def completed_unique_bytes(stage: Path, dataset: str, expected: str) -> int:
    """Observe sealed checkpoint metadata without mutating a possibly live DB."""
    path = stage/'checkpoint.sqlite'
    if not os.path.lexists(path):
        return 0
    path = s._safe_stage_file(stage, 'checkpoint.sqlite')
    intent_path = stage/'intent.json'
    if (intent_path.is_symlink() or not intent_path.is_file()
            or json.loads(intent_path.read_text()) != {'contract': candidate.CONTRACT+'.intent',
                'dataset': dataset, 'old_manifest_sha256': expected}):
        raise SnapshotError('dedup candidate checkpoint intent differs')
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True, timeout=0)) as db:
        meta = dict(db.execute('SELECT key,value FROM meta'))
        if meta != {'contract': s.CONTRACT+'.checkpoint', 'dataset': dataset, 'old_manifest_sha256': expected}:
            raise SnapshotError('dedup candidate checkpoint identity differs')
        packages = [json.loads(row[0]) for row in db.execute('SELECT body FROM packages ORDER BY number')]
        count, total = db.execute('SELECT COUNT(*),COALESCE(SUM(size),0) FROM members').fetchone()
    if (len({item['sha256'] for item in packages}) != len(packages)
            or count != sum(item['file_count'] for item in packages)
            or total != sum(item['logical_bytes'] for item in packages)):
        raise SnapshotError('dedup candidate checkpoint counts differ')
    for item in packages:
        if not s._signature_matches(s._safe_stage_file(stage, item['name']), item['E_signature']):
            raise SnapshotError('dedup candidate checkpoint pack changed')
    return total


def activate_private_candidate(cold: Path, stage: Path, dataset: str, expected: str, *, guard=lambda: None):
    """Enable a host-private encoding using its existing local v3 reader.

    This grants no remote export or reader acceptance. Public candidates keep
    their separately enrolled remote capability gate.
    """
    if cold.name != 'private-history' or cold.parent.name != '.local-state':
        raise SnapshotError('private dedup activation authority differs')
    with p._try_publication_lock(cold/'.local-state/locks'/f'publish-{dataset}.lock'):
        guard()
        current = p.resolve_latest_packed(cold, dataset, require_objects=False)
        prepared = s._read_prepared(stage, dataset, expected)
        sealed = candidate.read_sealed(stage, dataset, expected)
        if (current.manifest_sha256 != expected
                or not s.private_repack_source_allowed(dataset, current.manifest)
                or prepared.get('packed_format') != 'content-alias-v3'
                or prepared.get('candidate_identity_sha256') != sealed['identity_sha256']
                or prepared.get('old_inventory_sha256') != current.manifest['archive']['inventory']['sha256']
                or prepared.get('files') != current.manifest['source']['files']
                or prepared.get('logical_bytes') != current.manifest['source']['logical_bytes']):
            raise SnapshotError('private dedup activation fixed source differs')
        return s.activate_content_alias_install(stage, dataset, expected,
                                               reader_contract='packed-v3-content-alias')


def main() -> int:
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    workspace = Path(config['mount_point'])
    _check_d_primary_mount(COLD)
    root = workspace_volume.path('migration', create=True)/'source-local-dedup-candidate-v1'
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('dedup candidate queue root is redirected')
    guard = TradingPriorityGuard()
    owner_identity = process_identity()
    auto_holds = source_repack_bloat.auto_holds(workspace)
    auto_rows = {(row['dataset'], row['expected_manifest_sha256']): row
                for row in source_repack_bloat.auto_hold_rows(workspace)}
    holds = sorted(load_generation_holds() | auto_holds)
    for dataset, expected in holds:
        if (dataset, expected) in auto_holds and (dataset, expected) not in auto_rows:
            print(f'{dataset}: invalid auto dedup enrollment retained', file=sys.stderr)
            continue
        origin = auto_rows.get((dataset, expected), {}).get('origin', 'cold')
        private = origin == 'private_cold'
        source_cold = COLD/'.local-state/private-history' if private else COLD
        if (dataset, expected) in auto_holds:
            if prior_published(workspace/'migration/source-local-exact-repack',
                               {'dataset': dataset, 'expected_manifest_sha256': expected, 'origin': origin}):
                continue
            try:
                source_repack_bloat.ensure_stream_intent(workspace, dataset, expected)
            except (OSError, ValueError, TypeError, KeyError, SnapshotError) as error:
                print(f'{dataset}: auto dedup intent deferred: {type(error).__name__}', file=sys.stderr)
                continue
        stage = root/dataset/expected
        status = root/(dataset+'.status.json')
        prior = {}
        if status.is_file() and not status.is_symlink() and status.stat().st_size <= 1024*1024:
            try:
                value = json.loads(status.read_text())
                if (isinstance(value, dict) and value.get('contract') == CONTRACT
                        and value.get('dataset') == dataset and value.get('expected_manifest_sha256') == expected):
                    prior = value
            except (OSError, ValueError):
                pass
        retry_after = prior.get('retry_after_epoch')
        if (prior.get('state') == 'waiting_adoption_retry' and type(retry_after) in (int, float)
                and math.isfinite(retry_after) and retry_after > time.time()):
            continue
        stream_stage = workspace/'migration/stream-source-repack-v1'/dataset/expected
        # The original candidate name becomes the preserved old checkpoint
        # after an atomic directory exchange. Never restart candidate creation
        # merely because its payloads are now with the canonical stream owner.
        if (stream_stage/'candidate.json').is_file():
            try:
                sealed = candidate.read_sealed(stream_stage, dataset, expected)
                prepared = s._read_prepared(stream_stage, dataset, expected)
                pruned = None
                if (stage/'checkpoint.sqlite').is_file():
                    def prune_progress(event):
                        guard.check()
                        atomic_write_json(status, {'contract': CONTRACT, 'dataset': dataset,
                            'expected_manifest_sha256': expected, 'origin': origin, 'state': 'pruning_legacy_E_checkpoint',
                            'unique_contents': sealed['unique_contents'], 'files': sealed['files'],
                            'logical_bytes': sealed['logical_bytes'], 'prepared_sha256': prepared['identity_sha256'],
                            'owner_identity': owner_identity, 'observed_at_epoch': time.time(),
                            'progress': {'stage': 'pruning_legacy_E_checkpoint', **event},
                            'publishable': False, 'retirement_authorized': False})
                    pruned = dedup_stage_handoff.prune_legacy(source_cold, stage, stream_stage, dataset, expected,
                        materialized=Path('/srv/stockagent-packed-materialized'),
                        guard=guard.check, progress=prune_progress)
                if private:
                    activate_private_candidate(source_cold, stream_stage, dataset, expected, guard=guard.check)
                atomic_write_json(status, {'contract': CONTRACT, 'dataset': dataset,
                    'expected_manifest_sha256': expected, 'origin': origin,
                    'state': 'v3_handed_off_local_install_ready' if private else 'v3_handed_off_waiting_activation',
                    'unique_contents': sealed['unique_contents'], 'files': sealed['files'],
                    'logical_bytes': sealed['logical_bytes'], 'prepared_sha256': prepared['identity_sha256'],
                    'observed_at_epoch': time.time(), 'publishable': False, 'retirement_authorized': False,
                    'destination_independent_readback': False,
                    **({'legacy_E_reclaimed_allocated_bytes': pruned['reclaimed_allocated_bytes'],
                        'legacy_E_files_removed': len(pruned['removed'])} if pruned is not None else {})})
            except (OSError, ValueError, RuntimeError, KeyError, sqlite3.Error) as error:
                delay = retry_delay(1, error)
                atomic_write_json(status, {'contract': CONTRACT, 'dataset': dataset,
                    'expected_manifest_sha256': expected, 'origin': origin, 'state': 'waiting_adoption_retry',
                    'observed_at_epoch': time.time(), 'retry_after_epoch': time.time()+delay,
                    'error_type': type(error).__name__, 'error': str(error)[:512],
                    'publishable': False, 'retirement_authorized': False})
            continue
        if (stage/'candidate.json').is_file():
            sealed = candidate.read_sealed(stage, dataset, expected)
            retry_after = prior.get('retry_after_epoch')
            if (prior.get('state') == 'waiting_adoption_retry' and type(retry_after) in (int, float)
                    and math.isfinite(retry_after) and retry_after > time.time()):
                continue
            base = {'contract': CONTRACT, 'dataset': dataset, 'expected_manifest_sha256': expected,
                    'origin': origin,
                    'unique_contents': sealed['unique_contents'], 'files': sealed['files'],
                    'logical_bytes': sealed['logical_bytes'], 'fallback_count': prior.get('fallback_count', 0),
                    'publishable': False, 'retirement_authorized': False}
            last_report = 0.0
            def adoption_progress(event):
                nonlocal last_report
                guard.check()
                if time.monotonic()-last_report < 3 and event.get('completed_items') != event.get('total_items'):
                    return
                last_report = time.monotonic()
                atomic_write_json(status, {**base, 'state': 'validating_v3_metadata',
                    'owner_identity': owner_identity, 'observed_at_epoch': time.time(), 'progress': event})
            try:
                prepared = candidate.prepare_stream_adoption(source_cold, stage, dataset, expected,
                                                              progress=adoption_progress, guard=guard.check)
                state = 'v3_metadata_prepared_waiting_handoff'
                if stream_stage.is_dir() and not stream_stage.is_symlink():
                    dedup_stage_handoff.handoff(source_cold, stage, stream_stage, dataset, expected,
                        materialized=Path('/srv/stockagent-packed-materialized'), guard=guard.check)
                    # Retire the replaced encoding before advertising activation;
                    # activation removes the auto hold from the next queue turn.
                    if (stage/'checkpoint.sqlite').is_file():
                        dedup_stage_handoff.prune_legacy(source_cold, stage, stream_stage, dataset, expected,
                            materialized=Path('/srv/stockagent-packed-materialized'), guard=guard.check)
                    state = 'v3_handed_off_waiting_activation'
                    if private:
                        activate_private_candidate(source_cold, stream_stage, dataset, expected, guard=guard.check)
                        state = 'v3_handed_off_local_install_ready'
                atomic_write_json(status, {**base, 'state': state,
                    'origin': origin,
                    'prepared_sha256': prepared['identity_sha256'],
                    'observed_at_epoch': time.time(), 'destination_independent_readback': False})
            except (OSError, ValueError, RuntimeError, KeyError, sqlite3.Error) as error:
                failures = prior.get('failure_count', 0)
                failures = failures+1 if type(failures) is int and 0 <= failures < 1_000_000 else 1
                now = time.time()
                atomic_write_json(status, {**base, 'state': 'waiting_adoption_retry',
                    'observed_at_epoch': now, 'retry_after_epoch': now+retry_delay(failures, error),
                    'failure_count': failures, 'error_type': type(error).__name__, 'error': str(error)[:512]})
            continue
        retry_after = prior.get('retry_after_epoch')
        if (prior.get('state') == 'waiting_retry' and type(retry_after) in (int, float)
                and math.isfinite(retry_after) and retry_after > time.time()):
            continue
        fallback_count = prior.get('fallback_count', 0)
        if type(fallback_count) is not int or not 0 <= fallback_count < 1_000_000:
            fallback_count = 0
        index = (workspace/'migration/source-local-exact-repack/dedup-plans'/
                 (dataset+'-'+expected+'.sqlite'))
        try:
            summary = plan_summary(index, dataset, expected)
            latest = p.resolve_latest_packed(source_cold, dataset, require_objects=False)
            if latest.manifest_sha256 != expected:
                raise SnapshotError('dedup candidate original head changed')
            if private and not s.private_repack_source_allowed(dataset, latest.manifest):
                raise SnapshotError('private dedup source is excluded from migration')
            if (summary.get('files') != latest.manifest['source']['files']
                    or summary.get('logical_bytes') != latest.manifest['source']['logical_bytes']
                    or summary.get('inventory_sha256') != latest.manifest['archive']['inventory']['sha256']):
                raise SnapshotError('dedup planning counts differ from the fixed head')
            completed = completed_unique_bytes(stage, dataset, expected)
            budget = growth_budget(summary, latest.manifest, completed_unique_bytes=completed)
            controls = workspace/'migration/provider-state-temporary-20261103/capacity-leases'
            last = 0.0
            def report(event, *, force=False):
                nonlocal last, fallback_count
                guard.check()
                is_fallback = event.get('stage') == 'selective_native_fallback'
                if is_fallback:
                    fallback_count += 1
                if not force and not is_fallback and time.monotonic()-last < 3:
                    return
                last = time.monotonic()
                atomic_write_json(status, {'contract': CONTRACT, 'dataset': dataset,
                    'expected_manifest_sha256': expected, 'origin': origin, 'state': 'running',
                    'observed_at_epoch': time.time(), 'files': summary['files'],
                    'logical_bytes': summary['logical_bytes'], 'unique_contents': summary['unique_contents'],
                    'unique_bytes': summary['unique_content_logical_bytes'], 'progress': event,
                    'owner_identity': owner_identity,
                    'fallback_count': fallback_count,
                    'publishable': False, 'retirement_authorized': False})
            with reserve_capacity(controls, key=expected, maximum_growth_bytes=budget,
                    capacity_bytes=2**63-1, minimum_free_bytes=config['minimum_free_bytes']) as lease:
                workspace_volume.admit(budget+lease['other_reserved_growth_bytes'], c=config)
                locks = source_cold/'.local-state/locks'
                with ExitStack() as owners:
                    global_owner = ExitStack()
                    owners.callback(global_owner.close)
                    global_owner.enter_context(p._try_publication_lock(locks/'publish-retention-global.lock', shared=True))
                    owners.enter_context(p._try_publication_lock(locks/f'publish-{dataset}.lock'))
                    current = p.resolve_latest_packed(source_cold, dataset, require_objects=False)
                    if current.manifest_sha256 != expected:
                        raise SnapshotError('dedup candidate original head changed before pin')
                    old_stage = workspace/'migration/stream-source-repack-v1'/dataset/expected
                    pin = s._repack_pin(Path('/srv/stockagent-packed-materialized'), dataset,
                                        expected, current.manifest['snapshot_id'], old_stage)
                    global_owner.close()
                    report({'stage': 'starting'}, force=True)
                    sealed = candidate.prepare(source_cold, current, stage, index, native=True,
                                               progress=report, guard=guard.check)
                    global_owner.enter_context(p._try_publication_lock(
                        locks/'publish-retention-global.lock', shared=True))
                    guard.check()
                    if p.resolve_latest_packed(source_cold, dataset, require_objects=False).manifest_sha256 != expected:
                        raise SnapshotError('dedup candidate original head changed after build')
                    s._check_repack_pin(pin)
            atomic_write_json(status, {'contract': CONTRACT, 'dataset': dataset,
                'expected_manifest_sha256': expected, 'origin': origin,
                'state': 'sealed_waiting_local_adoption' if private else 'sealed_waiting_reader',
                'unique_contents': sealed['unique_contents'], 'files': sealed['files'],
                'logical_bytes': sealed['logical_bytes'], 'observed_at_epoch': time.time(),
                'fallback_count': fallback_count,
                'publishable': False, 'retirement_authorized': False})
        except (OSError, ValueError, RuntimeError, KeyError, sqlite3.Error) as error:
            failures = prior.get('failure_count', 0)
            failures = failures+1 if type(failures) is int and 0 <= failures < 1_000_000 else 1
            now = time.time()
            atomic_write_json(status, {'contract': CONTRACT, 'dataset': dataset,
                'expected_manifest_sha256': expected, 'origin': origin, 'state': 'waiting_retry',
                'observed_at_epoch': now, 'retry_after_epoch': now+retry_delay(failures, error),
                'failure_count': failures, 'error_type': type(error).__name__,
                'fallback_count': fallback_count,
                'error': str(error)[:512], 'publishable': False, 'retirement_authorized': False})
            print(f'{dataset}: {type(error).__name__}: {error}', file=sys.stderr)
        return 0  # one dataset per invocation; timer retries without blocking other owners
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, KeyError, sqlite3.Error) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)

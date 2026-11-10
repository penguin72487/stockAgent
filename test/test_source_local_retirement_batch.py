import fcntl
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import run_source_local_retirement_queue as queue
from scripts import run_source_local_repack_queue as repack_queue
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync import retirement_turn, packed_snapshots

run_batch = queue.run_batch


@pytest.fixture(autouse=True)
def isolate_ordinary_history_owner(monkeypatch):
    # This file exercises queue scheduling, not the node's live data-root GC.
    # Its entrypoint must not discover the production ordinary-history owner.
    from stockagent.data_sync import ordinary_archive
    monkeypatch.setattr(ordinary_archive, 'step', lambda *_a, **_kw: None)


def test_pressure_turn_waits_for_existing_writer_and_excludes_new_install(tmp_path, monkeypatch):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending_generations', lambda _: set())
    monkeypatch.setattr(queue, '_retirement_work_pending', lambda _: True)
    checks, sleeps, calls = [], [], []
    def busy():
        checks.append(True)
        return len(checks) <= 2
    def do_run(path, publication_busy):
        calls.append(path)
        with pytest.raises(packed_snapshots.PackedPublicationBusy):
            with retirement_turn.turn(root, retirement=False):
                pass
        return 4
    assert queue.run_pressure_turn(root, tmp_path/'cold', tmp_path, free_bytes=20,
            run=do_run, busy=busy, other_busy=lambda: False, sleep=sleeps.append, monotonic=lambda: 0) == 4
    assert sleeps == [1, 1] and calls == [root]
    with retirement_turn.turn(root, retirement=False):
        pass


def test_pressure_turn_has_bounded_wait_and_releases_lock(tmp_path, monkeypatch):
    root = tmp_path/'owner'; root.mkdir()
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending_generations', lambda _: set())
    monkeypatch.setattr(queue, '_retirement_work_pending', lambda _: True)
    times = iter([0, 0, 1, 2])
    assert queue.run_pressure_turn(root, tmp_path/'cold', tmp_path, free_bytes=20, wait_seconds=2,
        busy=lambda: True, other_busy=lambda: False, monotonic=lambda: next(times), sleep=lambda _: None,
        run=lambda *_a, **_k: pytest.fail('busy writer must finish first')) == 0
    with retirement_turn.turn(root, retirement=False):
        pass


def test_e_only_recovery_and_abundant_space_never_reserve_retirement_turn(tmp_path, monkeypatch):
    root = tmp_path/'owner'; root.mkdir()
    calls = []
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending_generations', lambda _: {('x', 'a')})
    monkeypatch.setattr(queue, '_retirement_work_pending', lambda _: True)
    def do_run(_root, publication_busy):
        with retirement_turn.turn(root, retirement=False):
            calls.append('install-not-blocked')
        return 1
    assert queue.run_pressure_turn(root, tmp_path/'cold', tmp_path, free_bytes=1, run=do_run) == 1
    assert queue.run_pressure_turn(root, tmp_path/'cold', tmp_path,
        free_bytes=retirement_turn.PRESSURE_BYTES, run=do_run) == 1
    assert calls == ['install-not-blocked', 'install-not-blocked']


@pytest.mark.parametrize('e_only,free', [(True, 1), (False, retirement_turn.PRESSURE_BYTES)])
def test_unreserved_turn_keeps_publication_contention_probe(tmp_path, monkeypatch, e_only, free):
    """An E-only D writer keeps priority without causing a retry storm."""
    root = tmp_path/'owner'; root.mkdir()
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending_generations',
                        lambda _: {('x', 'a')} if e_only else set())
    monkeypatch.setattr(queue, '_retirement_work_pending', lambda _: True)
    monkeypatch.setattr(queue, '_publication_owner_busy', lambda _: True)
    attempts, waits = [], []
    def selected(path):
        attempts.append(True)
        (path/'retirement-queue-status.json').write_text(json.dumps({
            'contract': 'stockagent.source_local_repack_queue.v1.retirement-status',
            'last_attempt': {'state': 'waiting_retry', 'reason': 'd_e_owner_busy',
                             'owner_lock': 'publish_retention_global'}}))
        return True
    def batch(path, publication_busy):
        return run_batch(path, run=selected, priority_pending=lambda _: False,
                         monotonic=lambda: 0, publication_busy=publication_busy, sleep=waits.append)
    assert queue.run_pressure_turn(root, tmp_path/'cold', tmp_path, free_bytes=free,
                                   other_busy=lambda: False, run=batch) == 1
    assert len(attempts) == 1 and waits == [1]*5
    with retirement_turn.turn(root, retirement=False):
        pass  # No retirement reservation delays the sole E recovery.


def test_retirement_turn_never_waits_for_other_turn_owner(tmp_path):
    with retirement_turn.turn(tmp_path, retirement=True):
        with pytest.raises(packed_snapshots.PackedPublicationBusy):
            with retirement_turn.turn(tmp_path, retirement=True):
                pass


def test_retirement_reserves_next_turn_while_existing_writer_finishes(tmp_path, monkeypatch):
    import threading
    import time
    root = tmp_path/'owner'; root.mkdir()
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending_generations', lambda _: set())
    monkeypatch.setattr(queue, '_retirement_work_pending', lambda _: True)
    waiting = threading.Event()
    results, observations = [], []
    def nap(_seconds):
        waiting.set()
        time.sleep(.01)
    def retire():
        results.append(queue.run_pressure_turn(root, tmp_path/'cold', tmp_path,
            free_bytes=20, busy=lambda: False, other_busy=lambda: False, sleep=nap, wait_seconds=3,
            run=lambda _root, publication_busy: 7, observe=observations.append))
    with retirement_turn.turn(root, retirement=False):
        worker = threading.Thread(target=retire)
        worker.start()
        assert waiting.wait(2), 'retirement did not wait for the existing writer'
        with pytest.raises(packed_snapshots.PackedPublicationBusy, match='retirement-intent'):
            with retirement_turn.turn(root, retirement=False):
                pass
        assert worker.is_alive()  # The already admitted writer was not interrupted.
    worker.join(3)
    assert not worker.is_alive() and results == [7]
    assert observations[0] == 'waiting_writer' and observations[-2:] == ['retiring', 'released']
    with retirement_turn.turn(root, retirement=False):
        pass


def test_reservation_is_released_when_inflight_writer_outlasts_timeout(tmp_path, monkeypatch):
    root = tmp_path/'owner'; root.mkdir()
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending_generations', lambda _: set())
    monkeypatch.setattr(queue, '_retirement_work_pending', lambda _: True)
    with retirement_turn.turn(root, retirement=False):
        assert queue.run_pressure_turn(root, tmp_path/'cold', tmp_path, free_bytes=1,
            other_busy=lambda: False, busy=lambda: False, wait_seconds=0,
            run=lambda *_a, **_kw: pytest.fail('writer is still active')) == 0
        # Its bounded reservation is gone; another shared writer may enter.
        with retirement_turn.turn(root, retirement=False):
            pass


def test_reservation_is_released_when_trading_priority_interrupts_wait(tmp_path, monkeypatch):
    root = tmp_path/'owner'; root.mkdir()
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending_generations', lambda _: set())
    monkeypatch.setattr(queue, '_retirement_work_pending', lambda _: True)
    calls, observations = [], []
    def guard():
        calls.append(True)
        if len(calls) == 2:
            raise SnapshotError('Taiwan day-trade priority window; maintenance deferred')
    with retirement_turn.turn(root, retirement=False):
        with pytest.raises(SnapshotError, match='day-trade priority'):
            queue.run_pressure_turn(root, tmp_path/'cold', tmp_path, free_bytes=1, other_busy=lambda: False,
                busy=lambda: False, guard=guard, sleep=lambda _: None, wait_seconds=3,
                run=lambda *_a, **_kw: pytest.fail('must yield to day trading'), observe=observations.append)
        with retirement_turn.turn(root, retirement=False):
            pass
    assert observations == ['waiting_writer', 'released']


def test_priority_hint_requires_a_fresh_actionable_queue(tmp_path):
    status = {'contract': 'stockagent.source_local_repack_queue.v1.retirement-status',
              'observed_at_epoch': 1000, 'entries': [{'state': 'waiting'}]}
    path = tmp_path/'retirement-queue-status.json'
    path.write_text(json.dumps(status))
    assert queue._retirement_work_pending(tmp_path, now=lambda: 1001)
    assert not queue._retirement_work_pending(tmp_path, now=lambda: 1181)
    status['entries'] = [{'state': 'waiting_retry', 'reason': 'retirement_pinned', 'retry_after_epoch': 1001}]
    path.write_text(json.dumps(status))
    assert not queue._retirement_work_pending(tmp_path, now=lambda: 1001)
    status['entries'][0].update(reason='d_e_owner_busy', retry_after_epoch=1030)
    path.write_text(json.dumps(status))
    assert queue._retirement_work_pending(tmp_path, now=lambda: 1001)


def test_external_owner_contention_stops_batch_after_one_attempt(tmp_path):
    status = {'contract': 'stockagent.source_local_repack_queue.v1.retirement-status',
              'last_attempt': {'state': 'waiting_retry', 'reason': 'retirement_source_replication_busy'}}
    attempts = []
    def run(root):
        attempts.append(True)
        (root/'retirement-queue-status.json').write_text(json.dumps(status))
        return True
    assert run_batch(tmp_path, run=run, monotonic=lambda: 0, priority_pending=lambda _: False) == 1
    assert len(attempts) == 1


def test_retirement_batch_drains_only_bounded_ready_work(tmp_path):
    calls = []
    assert run_batch(tmp_path, run=lambda root: calls.append(root) or True,
                     monotonic=lambda: 0, max_attempts=4) == 4
    assert calls == [tmp_path]*4
    calls.clear()
    assert run_batch(tmp_path, run=lambda root: calls.append(root) or False,
                     monotonic=lambda: 0) == 0
    assert calls == [tmp_path]


def test_default_batch_can_skip_many_fast_blockers_without_an_idle_minute(tmp_path):
    calls = []
    assert run_batch(tmp_path, run=lambda root: calls.append(root) or True,
                     monotonic=lambda: 0) == 16
    assert len(calls) == 16


def test_retirement_batch_stops_starting_work_after_the_time_budget(tmp_path):
    clock = iter((0, 0, 181))
    calls = []
    assert run_batch(tmp_path, run=lambda root: calls.append(root) or True,
                     monotonic=lambda: next(clock), max_seconds=180) == 2
    assert len(calls) == 2


def test_shared_publication_contention_waits_then_resumes(tmp_path):
    attempts, waits = [], []
    status = {'contract': 'stockagent.source_local_repack_queue.v1.retirement-status',
              'last_attempt': {'state': 'waiting_retry', 'reason': 'd_e_owner_busy',
                               'owner_lock': 'publish_retention_global'}}
    def run(root):
        attempts.append(True)
        (root/'retirement-queue-status.json').write_text(json.dumps(status))
        return len(attempts) == 1
    busy = iter([True, True, False])
    count = run_batch(tmp_path, run=run, monotonic=lambda: 0, priority_pending=lambda _: False,
                      publication_busy=lambda: next(busy), sleep=waits.append)
    assert count == 1 and len(attempts) == 2 and waits == [1, 1]


def test_shared_publication_contention_yields_after_bounded_wait(tmp_path):
    attempts, waits = [], []
    (tmp_path/'retirement-queue-status.json').write_text(json.dumps({
        'contract': 'stockagent.source_local_repack_queue.v1.retirement-status',
        'last_attempt': {'state': 'waiting_retry', 'reason': 'd_e_owner_busy',
                         'owner_lock': 'publish_retention_global'}}))
    count = run_batch(tmp_path, run=lambda _: attempts.append(True) or True, monotonic=lambda: 0,
                      priority_pending=lambda _: False, publication_busy=lambda: True, sleep=waits.append)
    assert count == 1 and len(attempts) == 1 and waits == [1]*5


def test_shared_publication_wait_yields_immediately_to_priority_handoff(tmp_path):
    attempts, checks, waits = [], [], []
    (tmp_path/'retirement-queue-status.json').write_text(json.dumps({
        'contract': 'stockagent.source_local_repack_queue.v1.retirement-status',
        'last_attempt': {'state': 'waiting_retry', 'reason': 'd_e_owner_busy',
                         'owner_lock': 'publish_retention_global'}}))
    def priority(_root):
        checks.append(True)
        return len(checks) > 1
    count = run_batch(tmp_path, run=lambda _: attempts.append(True) or True, monotonic=lambda: 0,
                      priority_pending=priority, publication_busy=lambda: True, sleep=waits.append)
    assert count == 1 and len(attempts) == 1 and waits == []


def test_publication_probe_only_uses_the_existing_lock(tmp_path):
    lock = tmp_path/'.local-state/locks/publish-retention-global.lock'
    assert queue._publication_owner_busy(tmp_path)  # Never creates a missing authority.
    lock.parent.mkdir(parents=True)
    lock.touch()
    with lock.open('r+') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert queue._publication_owner_busy(tmp_path)
    assert not queue._publication_owner_busy(tmp_path)
    lock.unlink()
    lock.symlink_to(tmp_path/'other')
    assert queue._publication_owner_busy(tmp_path)


def test_retirement_yields_only_to_a_fresh_sealed_handoff_waiting_for_global_lock(tmp_path):
    root = tmp_path/'source-local-exact-repack'
    root.mkdir()
    handoff = tmp_path/'handoff-index-archive-v1'
    handoff.mkdir()
    (handoff/'tw-minute-train-workspace.status.json').write_text(json.dumps({
        'contract': 'stockagent.handoff_index_archive.v1', 'dataset': 'tw-minute-train-workspace',
        'C_payload_deleted': True, 'D_E_stage_cleanup_complete': False}))
    row = {'dataset': 'tw-minute-train-workspace', 'state': 'retrying',
           'error': 'owner_busy', 'owner_lock': 'publish_retention_global'}
    status = {'contract': 'stockagent.handoff_index_private_D_queue.v1',
              'observed_at_epoch': 1000, 'entries': [row]}
    path = handoff/'D-queue-status.json'
    path.write_text(json.dumps(status))
    assert queue._priority_handoff_pending(root, now=lambda: 1001)
    assert not queue._priority_handoff_pending(root, now=lambda: 1181)
    row['owner_lock'] = 'cold_install'
    path.write_text(json.dumps(status))
    assert not queue._priority_handoff_pending(root, now=lambda: 1001)
    row['owner_lock'] = 'publish_retention_global'
    row['state'] = 'waiting_priority'
    path.write_text(json.dumps(status))
    assert not queue._priority_handoff_pending(root, now=lambda: 1001)
    calls = []
    assert run_batch(root, run=lambda _: calls.append(True) or True,
                     priority_pending=lambda _: True) == 0
    assert not calls


def test_retirement_yields_for_bound_local_head_recovery_only(tmp_path):
    root = tmp_path/'source-local-exact-repack'
    root.mkdir()
    digest = 'a'*64
    journal = tmp_path/'authoritative-head-handoffs'/'fixture'/digest/'journal.json'
    journal.parent.mkdir(parents=True)
    value = {'contract': 'stockagent.authoritative_head_handoff.v1', 'state': 'planned',
             'request': {'dataset': 'fixture', 'old_manifest_sha256': digest}}
    value['identity_sha256'] = queue.stream_repack._identity(value)
    journal.write_text(json.dumps(value))
    assert queue._foreign_head_recovery_pending(root)
    value['state'] = 'moved'  # A pending remote confirmation has its own pin.
    value['identity_sha256'] = queue.stream_repack._identity(value)
    journal.write_text(json.dumps(value))
    assert not queue._foreign_head_recovery_pending(root)
    value['state'] = 'peer_confirmed'
    value['identity_sha256'] = '0'*64
    journal.write_text(json.dumps(value))
    assert not queue._foreign_head_recovery_pending(root)


def test_batch_cache_build_and_reuse_are_limited_to_prepared_public_sources(tmp_path, monkeypatch):
    root, workspace, cold = tmp_path/'queue', tmp_path/'workspace', tmp_path/'cold'
    root.mkdir()
    cold.mkdir()
    base = workspace/'migration/d-cold-object-staging-v1'
    base.mkdir(parents=True)
    rows = [dict(mode='direct', dataset=name, old_manifest_sha256=digest)
            for name, digest in [('a', 'a'*64), ('b', 'b'*64)]]
    rows += [dict(mode='direct', dataset='private', old_manifest_sha256='c'*64, private=True)]
    for row in rows:
        (workspace/'migration/stream-source-repack-v1'/row['dataset']/row['old_manifest_sha256']).mkdir(parents=True)
    monkeypatch.setattr(queue, 'discover_pending_retirements', lambda _root, **_kw: rows)
    monkeypatch.setattr(queue.stream_repack, '_prepared_metadata',
                        lambda _stage, name, _sha: {'old_snapshot_id': name})
    monkeypatch.setattr(queue.packed_snapshots, 'resolve_packed_snapshot_id',
        lambda _cold, name, _snapshot, **_kwargs: SimpleNamespace(manifest_sha256=name*64, manifest=name))
    monkeypatch.setattr(queue.repack_retirement, '_require_not_current_head', lambda *_args: None)
    monkeypatch.setattr(queue.repack_retirement, '_refs', lambda manifest: {manifest+'-object'})
    monkeypatch.setattr(queue.retire_staged_cold_objects, 'catalog_index',
                        lambda *_args, **_kwargs: ({}, {}, 'catalog-identity'))
    monkeypatch.setattr(queue, '_private_catalog_generation', lambda _path: [1])
    monkeypatch.setattr(queue.repack_retirement, 'cached_catalog_references', lambda *_args: None)
    admitted, built = [], []
    monkeypatch.setattr(queue.workspace_volume, 'admit', lambda size, **kw: admitted.append((size, kw)))
    monkeypatch.setattr(queue.repack_retirement, 'build_catalog_reference_cache',
        lambda _catalog, _cold, _metadata, targets, _generation, _identity, _path:
            built.append(targets) or {'targets': len(targets)})
    config = {'marker': 'test'}
    assert queue.prepare_batch_reference_cache(root, workspace, cold, config) == {
        'state': 'built', 'candidate_count': 2, 'targets': 2}
    assert built == [{'a-object', 'b-object'}]
    assert admitted == [(2*queue.repack_retirement.MAX_TARGET_CACHE_BYTES, {'c': config})]
    monkeypatch.setattr(queue.repack_retirement, 'cached_catalog_references', lambda *_args: ({}, set()))
    assert queue.prepare_batch_reference_cache(root, workspace, cold, config)['state'] == 'reused'
    assert len(admitted) == len(built) == 1


def test_batch_cache_skips_only_live_matching_cooldowns(tmp_path, monkeypatch):
    rows = [dict(mode='direct', dataset=name, old_manifest_sha256=digest)
            for name, digest in [('a', 'a'*64), ('b', 'b'*64), ('c', 'c'*64), ('d', 'd'*64), ('e', 'e'*64)]]
    rows.extend([dict(mode='history', dataset='history', old_manifest_sha256='e'*64),
                 dict(mode='direct', dataset='private', old_manifest_sha256='f'*64, private=True)])
    monkeypatch.setattr(queue, 'discover_pending_retirements', lambda _root, **_kw: rows)
    def prior(row, retry, digest=repack_queue.RETIRE_IMPLEMENTATION_SHA256):
        return {**row, 'state': 'waiting_retry', 'retry_after_epoch': retry, 'execution_sha256': digest}
    path = tmp_path/'retirement-queue-status.json'
    path.write_text(json.dumps({'contract': 'stockagent.source_local_repack_queue.v1.retirement-status',
        'entries': [prior(rows[0], 1100), prior(rows[1], 999), prior(rows[2], 1100, 'other'),
                    prior(rows[3], float('inf')), prior(rows[4], 10**400)]}))
    assert [x['dataset'] for x in queue._ready_public_direct_rows(tmp_path, now=1000)] == ['b', 'c', 'd', 'e']
    path.write_text('[]')  # An invalid hint must never suppress the owner.
    assert [x['dataset'] for x in queue._ready_public_direct_rows(tmp_path, now=1000)] == ['a', 'b', 'c', 'd', 'e']


def test_batch_cache_does_not_scan_when_only_one_item_is_ready(tmp_path, monkeypatch):
    rows = [dict(mode='direct', dataset=name, old_manifest_sha256=digest)
            for name, digest in [('a', 'a'*64), ('b', 'b'*64)]]
    monkeypatch.setattr(queue, 'discover_pending_retirements', lambda _root, **_kw: rows)
    (tmp_path/'retirement-queue-status.json').write_text(json.dumps({
        'contract': 'stockagent.source_local_repack_queue.v1.retirement-status',
        'entries': [{**rows[0], 'retry_after_epoch': queue.time.time()+3600,
                     'execution_sha256': repack_queue.RETIRE_IMPLEMENTATION_SHA256}]}))
    monkeypatch.setattr(queue.stream_repack, '_prepared_metadata',
                        lambda *_a: (_ for _ in ()).throw(AssertionError('blocked source should not be scanned')))
    assert queue.prepare_batch_reference_cache(tmp_path, tmp_path, tmp_path, {}) == {'state': 'not_needed'}


def test_batch_pin_prefetch_uses_only_exact_positive_evidence_and_observes_unpin(tmp_path, monkeypatch):
    workspace = tmp_path/'workspace'
    root = workspace/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    materialized, retention = tmp_path/'materialized', tmp_path/'retention'
    materialized.mkdir()
    rows = []
    for name, digest in [('a', 'a'*64), ('b', 'b'*64)]:
        stage = workspace/'migration/stream-source-repack-v1'/name/digest
        stage.mkdir(parents=True)
        prepared = {'contract': queue.stream_repack.CONTRACT+'.prepared', 'dataset': name,
                    'old_manifest_sha256': digest, 'old_snapshot_id': name+'-old',
                    'destination_independent_readback': False, 'raw_extraction_tree_created': False,
                    'source_member_sha_streamed': True, 'packages': []}
        prepared['identity_sha256'] = queue.stream_repack._identity(prepared)
        (stage/'prepared.json').write_text(json.dumps(prepared))
        rows.append({'dataset': name, 'old_manifest_sha256': digest, 'mode': 'direct',
                     'state': 'waiting_retry', 'reason': 'retirement_pinned', 'retry_after_epoch': 999,
                     'execution_sha256': repack_queue.RETIRE_IMPLEMENTATION_SHA256})
    (root/'retirement-queue-status.json').write_text(json.dumps({
        'contract': repack_queue.CONTRACT+'.retirement-status', 'entries': rows}))
    pin = materialized/'research.pin.json'
    pin.write_text(json.dumps({'snapshot_id': 'a-old'}))
    (materialized/'unreadable.pin.json').write_text('{')  # Other pin errors cannot create a negative hint.
    hints = queue.prefetch_positive_pins(root, workspace, materialized=materialized,
                                         retention_state=retention, now=lambda: 1000)
    assert hints == {'observed_at_epoch': 1000, 'keys': frozenset({('a', 'a'*64)})}
    pin.unlink()
    assert queue.prefetch_positive_pins(root, workspace, materialized=materialized,
                                        retention_state=retention, now=lambda: 1001)['keys'] == frozenset()
    monkeypatch.setattr(queue.packed_retention, '_local_protected_snapshot_ids',
                        lambda *_a, **_kw: (_ for _ in ()).throw(OSError('pin census unavailable')))
    assert queue.prefetch_positive_pins(root, workspace, materialized=materialized,
                                         retention_state=retention, now=lambda: 1001) is None
    (root/'retirement-queue-status.json').write_text('[]')
    assert queue.prefetch_positive_pins(root, workspace, materialized=materialized,
                                         retention_state=retention, now=lambda: 1001) is None


def test_batch_pin_prefetch_bounds_fixed_metadata_work(tmp_path, monkeypatch):
    workspace = tmp_path/'workspace'
    root = workspace/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    rows = []
    for index in range(queue.MAX_PIN_PREFETCH_CANDIDATES+1):
        name, digest = f'dataset-{index}', f'{index:064x}'
        stage = workspace/'migration/stream-source-repack-v1'/name/digest
        stage.mkdir(parents=True)
        (stage/'prepared.json').write_text('{}')
        rows.append({'dataset': name, 'old_manifest_sha256': digest, 'mode': 'direct',
                     'state': 'waiting_retry', 'reason': 'retirement_pinned', 'retry_after_epoch': 0,
                     'execution_sha256': repack_queue.RETIRE_IMPLEMENTATION_SHA256})
    (root/'retirement-queue-status.json').write_text(json.dumps({
        'contract': repack_queue.CONTRACT+'.retirement-status', 'entries': rows}))
    monkeypatch.setattr(queue.stream_repack, '_prepared_metadata',
                        lambda _stage, name, _digest: {'old_snapshot_id': name+'-old'})
    monkeypatch.setattr(queue.packed_retention, '_local_protected_snapshot_ids',
                        lambda _root, known, **_kw: (known, [], []))
    hints = queue.prefetch_positive_pins(root, workspace, materialized=tmp_path,
                                         retention_state=tmp_path, now=lambda: 1000)
    assert len(hints['keys']) == queue.MAX_PIN_PREFETCH_CANDIDATES
    assert ('dataset-128', f'{128:064x}') not in hints['keys']


def test_batch_catalog_cache_skips_a_fresh_positive_pin_hint(tmp_path, monkeypatch):
    rows = [dict(mode='direct', dataset=name, old_manifest_sha256=digest)
            for name, digest in [('a', 'a'*64), ('b', 'b'*64)]]
    monkeypatch.setattr(queue, '_ready_public_direct_rows', lambda *_a, **_kw: rows)
    hints = {'observed_at_epoch': queue.time.time(), 'keys': frozenset({('a', 'a'*64)})}
    assert queue.prepare_batch_reference_cache(tmp_path, tmp_path, tmp_path, {},
                                               positive_pin_hints=hints) == {'state': 'not_needed'}


def test_current_foreign_heads_do_not_trigger_an_expensive_cache_scan(tmp_path, monkeypatch):
    root, workspace, cold = tmp_path/'queue', tmp_path/'workspace', tmp_path/'cold'
    root.mkdir()
    rows = [dict(mode='direct', dataset=name, old_manifest_sha256=name*64) for name in ('a', 'b')]
    for row in rows:
        (workspace/'migration/stream-source-repack-v1'/row['dataset']/row['old_manifest_sha256']).mkdir(parents=True)
    monkeypatch.setattr(queue, 'discover_pending_retirements', lambda _root, **_kw: rows)
    monkeypatch.setattr(queue.stream_repack, '_prepared_metadata',
                        lambda _stage, name, _sha: {'old_snapshot_id': name})
    monkeypatch.setattr(queue.packed_snapshots, 'resolve_packed_snapshot_id',
        lambda _cold, name, _snapshot, **_kwargs: SimpleNamespace(manifest_sha256=name*64, manifest=name))
    def current_head(*_args):
        raise SnapshotError('old release is still a current head')
    monkeypatch.setattr(queue.repack_retirement, '_require_not_current_head', current_head)
    monkeypatch.setattr(queue.retire_staged_cold_objects, 'catalog_index',
                        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError('catalog must not be read')))
    assert queue.prepare_batch_reference_cache(root, workspace, cold, {}) == {'state': 'insufficient_candidates'}


def test_cache_preparation_failure_does_not_block_retirement(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(queue.workspace_volume, 'configuration',
                        lambda: {'mount_point': str(tmp_path), 'cold_root': str(tmp_path)})
    monkeypatch.setattr(queue.workspace_volume, 'check', lambda **_kw: None)
    monkeypatch.setattr(queue.workspace_volume, 'path', lambda *_a, **_kw: tmp_path)
    monkeypatch.setattr(queue, '_publication_owner_busy', lambda *_a: False)
    def failed(*_a, **_kw):
        raise SnapshotError('cache unavailable')
    monkeypatch.setattr(queue, 'prepare_batch_reference_cache', failed)
    attempts = []
    monkeypatch.setattr(queue, 'run_batch', lambda root, **_kw: attempts.append(root) or 1)
    queue.main()
    assert attempts == [tmp_path/'source-local-exact-repack']
    assert 'fallback_full_scan' in capsys.readouterr().out


def test_public_busy_skips_public_hints_but_keeps_private_queue(tmp_path, monkeypatch, capsys):
    cold = tmp_path/'cold'
    monkeypatch.setattr(queue.workspace_volume, 'configuration',
                        lambda: {'mount_point': str(tmp_path), 'cold_root': str(cold)})
    monkeypatch.setattr(queue.workspace_volume, 'check', lambda **_kw: None)
    monkeypatch.setattr(queue.workspace_volume, 'path', lambda *_a, **_kw: tmp_path)
    monkeypatch.setattr(queue, '_priority_handoff_pending', lambda *_a: False)
    monkeypatch.setattr(queue, '_publication_owner_busy', lambda path: Path(path) == cold)
    def should_not_prepare(*_a, **_kw):
        raise AssertionError('public-only hints must not run while the owner is busy')
    monkeypatch.setattr(queue, 'prefetch_positive_pins', should_not_prepare)
    monkeypatch.setattr(queue, 'prepare_batch_reference_cache', should_not_prepare)
    observed = []
    monkeypatch.setattr(queue, 'run_one_retirement',
                        lambda _root, **kw: observed.append(kw) or 1)
    monkeypatch.setattr(queue, 'run_batch', lambda root, **kw: kw['run'](root))
    monkeypatch.setattr(queue, 'run_pressure_turn', lambda root, *_a, **kw: kw['run'](root, False))
    queue.main()
    assert observed[0]['busy_publication_scopes'] == {'public'}
    assert observed[0]['positive_pin_hints'] is None
    assert 'deferred_public_owner_busy' in capsys.readouterr().out


def test_each_fair_turn_attempts_at_most_one_ordinary_inventory(tmp_path,monkeypatch):
    from stockagent.data_sync import ordinary_archive
    monkeypatch.setattr(queue.workspace_volume,'configuration',
        lambda:{'mount_point':str(tmp_path),'cold_root':str(tmp_path/'cold')})
    monkeypatch.setattr(queue.workspace_volume,'check',lambda **_kw:None)
    monkeypatch.setattr(queue.workspace_volume,'path',lambda *_a,**_kw:tmp_path)
    monkeypatch.setattr(queue,'_priority_handoff_pending',lambda *_a:False)
    monkeypatch.setattr(queue,'_publication_owner_busy',lambda *_a:False)
    monkeypatch.setattr(queue,'prefetch_positive_pins',lambda *_a:None)
    monkeypatch.setattr(queue,'prepare_batch_reference_cache',lambda *_a,**_kw:{'state':'not_needed'})
    ordinary,original=[],[]
    monkeypatch.setattr(ordinary_archive,'step',lambda *_a,**_kw:ordinary.append('one') or True)
    monkeypatch.setattr(queue,'run_one_retirement',lambda *_a,**_kw:original.append('one') or True)
    monkeypatch.setattr(queue,'run_batch',lambda root,**kw:sum(kw['run'](root) for _ in range(3)))
    monkeypatch.setattr(queue,'run_pressure_turn',lambda root,*_a,**kw:kw['run'](root,False))
    queue.main()
    assert ordinary==['one'] and original==['one','one']


def test_history_lookahead_uses_the_same_batch_and_witness_limits(monkeypatch):
    monkeypatch.setattr(queue.superseded_repack_history, 'MAX_REFERENCE_OCCURRENCES', 4)
    monkeypatch.setattr(queue.superseded_repack_history, 'MAX_WITNESS_CANDIDATES', 2)
    paths = [Path(f'/manifests/source/v{i}.json') for i in range(5)]
    values = {
        paths[0]: ({'common', 'a'}, 2), paths[1]: ({'common', 'b'}, 2),
        paths[2]: ({'common', 'c'}, 2), paths[3]: ({'common', 'd'}, 2),
        paths[4]: ({'common', 'protected'}, 2),
    }
    groups = queue._history_lookahead_groups(paths, {'v4'}, values.__getitem__)
    assert groups == [{'a', 'b'}, {'c', 'd'}]
    assert all('common' not in group and 'protected' not in group for group in groups)


def test_history_name_probe_skips_only_the_optional_cache(tmp_path):
    row = {'dataset': 'history', 'mode': 'history'}
    directory = tmp_path/'manifests/history'
    directory.mkdir(parents=True)
    (directory/'current.json').write_text('{}')
    assert not queue._history_may_need_catalog(tmp_path, row)
    (directory/'previous.json').write_text('{}')
    assert queue._history_may_need_catalog(tmp_path, row)
    assert not queue._history_may_need_catalog(tmp_path, {'dataset': '../invalid'})


def test_one_ready_history_builds_future_cache_but_reuses_immediate_coverage(tmp_path, monkeypatch):
    root, workspace, cold = tmp_path/'queue', tmp_path/'workspace', tmp_path/'cold'
    root.mkdir()
    cold.mkdir()
    (workspace/'migration/d-cold-object-staging-v1').mkdir(parents=True)
    history = dict(mode='history', dataset='history', old_manifest_sha256='a'*64)
    monkeypatch.setattr(queue, '_ready_public_direct_rows', lambda _root: [])
    monkeypatch.setattr(queue, '_history_may_need_catalog', lambda *_a: True)
    monkeypatch.setattr(queue, '_ready_public_rows', lambda _root, mode: [history] if mode == 'history' else [])
    monkeypatch.setattr(queue.retire_staged_cold_objects, 'catalog_index',
                        lambda *_a, **_kw: ({}, {}, 'fixed-catalog'))
    monkeypatch.setattr(queue, '_private_catalog_generation', lambda _path: [1])
    monkeypatch.setattr(queue, '_history_prefetch_groups',
                        lambda *_a, **_kw: [{'now'}, {'later-1'}, {'later-2'}])
    checks, builds = [], []
    def cached(_path, _catalog, _metadata, targets, _generation, _identity):
        checks.append(targets.copy())
        return None if len(checks) == 1 else ({}, set())
    monkeypatch.setattr(queue.repack_retirement, 'cached_catalog_references', cached)
    monkeypatch.setattr(queue.workspace_volume, 'admit', lambda *_a, **_kw: None)
    monkeypatch.setattr(queue.repack_retirement, 'build_catalog_reference_cache',
                        lambda _catalog, _cold, _metadata, targets, *_a:
                        builds.append(targets.copy()) or {'targets': len(targets)})
    assert queue.prepare_batch_reference_cache(root, workspace, cold, {}, selected_history=history)['state'] == 'built'
    assert checks == [{'now'}] and builds == [{'now', 'later-1', 'later-2'}]
    assert queue.prepare_batch_reference_cache(root, workspace, cold, {}, selected_history=history)['state'] == 'reused'
    assert checks == [{'now'}, {'now'}] and len(builds) == 1


def test_single_history_group_never_replaces_a_multi_batch_cache(tmp_path, monkeypatch):
    root, workspace, cold = tmp_path/'queue', tmp_path/'workspace', tmp_path/'cold'
    root.mkdir()
    cold.mkdir()
    (workspace/'migration/d-cold-object-staging-v1').mkdir(parents=True)
    row = dict(mode='history', dataset='small', old_manifest_sha256='a'*64)
    monkeypatch.setattr(queue, '_ready_public_direct_rows', lambda _root: [])
    monkeypatch.setattr(queue, '_history_may_need_catalog', lambda *_a: True)
    monkeypatch.setattr(queue.retire_staged_cold_objects, 'catalog_index',
                        lambda *_a, **_kw: ({}, {}, 'fixed-catalog'))
    monkeypatch.setattr(queue, '_private_catalog_generation', lambda _path: [1])
    monkeypatch.setattr(queue, '_history_prefetch_groups', lambda *_a, **_kw: [{'one-batch'}])
    monkeypatch.setattr(queue.repack_retirement, 'build_catalog_reference_cache',
                        lambda *_a, **_kw: pytest.fail('single batch must not overwrite shared cache'))
    assert queue.prepare_batch_reference_cache(root, workspace, cold, {}, selected_history=row) == {
        'state': 'not_needed'}

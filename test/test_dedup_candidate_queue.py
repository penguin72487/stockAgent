import json
import sqlite3
from contextlib import contextmanager

import pytest

from scripts import run_dedup_candidate_queue as queue
from stockagent.data_sync import packed_dedup_plan, packed_snapshots
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_plan_admission_checks_index_totals_and_fixed_identity(tmp_path):
    index = tmp_path/'plan.sqlite'
    summary = {'contract': 'stockagent.packed_dedup_plan.v1', 'dataset': 'fixture',
               'old_manifest_sha256': 'a'*64, 'unique_contents': 2,
               'unique_content_logical_bytes': 9, 'files': 3, 'logical_bytes': 13}
    with sqlite3.connect(index) as db:
        db.execute('CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        db.execute('CREATE TABLE contents(size INTEGER,occurrences INTEGER)')
        db.execute('INSERT INTO meta VALUES (?,?)', ('summary', json.dumps(summary)))
        db.executemany('INSERT INTO contents VALUES (?,?)', [(4, 2), (5, 1)])
    assert queue.plan_summary(index, 'fixture', 'a'*64) == summary
    with pytest.raises(SnapshotError, match='identity differs'):
        queue.plan_summary(index, 'fixture', 'b'*64)
    with sqlite3.connect(index) as db:
        db.execute('UPDATE contents SET occurrences=3 WHERE size=4')
    with pytest.raises(SnapshotError, match='identity differs'):
        queue.plan_summary(index, 'fixture', 'a'*64)


def test_resumed_candidate_reserves_only_unfinished_content(tmp_path):
    summary = {'unique_content_logical_bytes': 100}
    manifest = {'source': {'files': 3}, 'archive': {'inventory': {'bytes': 5},
                'objects': [{'kind': 'pack', 'bytes': 40}, {'kind': 'blob', 'bytes': 80}]}}
    full = queue.growth_budget(summary, manifest)
    assert queue.growth_budget(summary, manifest, completed_unique_bytes=30) == full-30
    with pytest.raises(SnapshotError, match='exceeds'):
        queue.growth_budget(summary, manifest, completed_unique_bytes=101)


def configured_queue(tmp_path, monkeypatch):
    cold, workspace, source = (tmp_path/name for name in ('cold', 'workspace', 'source'))
    workspace.mkdir()
    source.mkdir()
    (source/'a').write_bytes(b'duplicate')
    (source/'b').write_bytes(b'duplicate')
    monkeypatch.setattr(packed_snapshots, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = packed_snapshots.publish_packed_snapshot(cold, 'fixture', source, node_id='test', pack_buckets=4)
    plan_root = workspace/'migration/source-local-exact-repack/dedup-plans'
    plan_root.mkdir(parents=True)
    packed_dedup_plan.build_plan(cold, 'fixture', old.manifest_sha256, plan_root)
    monkeypatch.setattr(queue, 'COLD', cold)
    monkeypatch.setattr(queue, '_check_d_primary_mount', lambda *_a: None)
    monkeypatch.setattr(queue, 'load_generation_holds', lambda: {('fixture', old.manifest_sha256)})
    monkeypatch.setattr(queue.workspace_volume, 'configuration',
                        lambda: {'mount_point': str(workspace), 'minimum_free_bytes': 0})
    monkeypatch.setattr(queue.workspace_volume, 'check', lambda **_kw: None)
    monkeypatch.setattr(queue.workspace_volume, 'admit', lambda *_a, **_kw: None)
    monkeypatch.setattr(queue.workspace_volume, 'path', lambda *_a, **_kw: workspace/'migration')
    monkeypatch.setattr(queue.s, '_repack_pin', lambda *_a: 'test-pin')
    monkeypatch.setattr(queue.s, '_check_repack_pin', lambda pin: pin == 'test-pin')
    @contextmanager
    def capacity(*_a, **_kw):
        yield {'other_reserved_growth_bytes': 0}
    monkeypatch.setattr(queue, 'reserve_capacity', capacity)
    return cold, workspace, source, old


def test_queue_prepares_once_under_canonical_owner_and_never_publishes(tmp_path, monkeypatch):
    cold, workspace, source, old = configured_queue(tmp_path, monkeypatch)
    assert queue.main() == 0
    root = workspace/'migration/source-local-dedup-candidate-v1'
    status = json.loads((root/'fixture.status.json').read_text())
    assert status['state'] == 'sealed_waiting_reader'
    assert not status['publishable'] and not status['retirement_authorized']
    assert packed_snapshots.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    assert (source/'a').read_bytes() == (source/'b').read_bytes() == b'duplicate'
    monkeypatch.setattr(queue.candidate, 'prepare', lambda *_a, **_kw: pytest.fail('sealed payload rebuilt'))
    assert queue.main() == 0
    status = json.loads((root/'fixture.status.json').read_text())
    assert status['state'] == 'v3_metadata_prepared_waiting_handoff'
    assert not status['publishable'] and not status['retirement_authorized']
    assert packed_snapshots.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256


def test_busy_original_dataset_owner_defers_without_deleting(tmp_path, monkeypatch):
    cold, workspace, source, old = configured_queue(tmp_path, monkeypatch)
    with packed_snapshots._try_publication_lock(cold/'.local-state/locks/publish-fixture.lock'):
        assert queue.main() == 0
    status = json.loads((workspace/'migration/source-local-dedup-candidate-v1/fixture.status.json').read_text())
    assert status['state'] == 'waiting_retry' and status['error_type'] == 'PackedPublicationBusy'
    assert status['failure_count'] == 1 and status['retry_after_epoch']-status['observed_at_epoch'] == 60
    assert packed_snapshots.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    assert (source/'a').is_file()


def test_queue_hands_completed_candidate_to_original_stream_owner_once(tmp_path, monkeypatch):
    cold, workspace, _source, old = configured_queue(tmp_path, monkeypatch)
    assert queue.main() == 0
    stage = workspace/'migration/source-local-dedup-candidate-v1/fixture'/old.manifest_sha256
    stream = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    stream.mkdir(parents=True, mode=0o700)
    (stream/'intent.json').write_text(json.dumps({'contract': queue.s.CONTRACT+'.intent',
        'dataset': 'fixture', 'old_manifest_sha256': old.manifest_sha256}))
    (stream/'old-checkpoint').write_text('old unfinished checkpoint')
    monkeypatch.setattr(queue.dedup_stage_handoff, 'process_references_many', lambda *_a, **_kw: [])
    assert queue.main() == 0
    assert (stream/'candidate.json').is_file() and (stage/'old-checkpoint').is_file()
    assert packed_snapshots.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    status_path = workspace/'migration/source-local-dedup-candidate-v1/fixture.status.json'
    assert json.loads(status_path.read_text())['state'] == 'v3_handed_off_waiting_activation'
    monkeypatch.setattr(queue.candidate, 'prepare', lambda *_a, **_kw: pytest.fail('candidate rebuilt'))
    assert queue.main() == 0
    assert json.loads(status_path.read_text())['state'] == 'v3_handed_off_waiting_activation'


def test_retry_delay_is_bounded_and_keeps_other_candidates_a_turn():
    assert queue.retry_delay(1, OSError('temporary')) == 120
    assert queue.retry_delay(20, OSError('temporary')) == 600
    assert queue.retry_delay(2, packed_snapshots.PackedPublicationBusy('busy')) == 60

"""The minute fast path reuses an exact published D generation, never a guess."""
from __future__ import annotations

import hashlib
import json
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from stockagent.data_sync import minute_raw_existing_d as minute
from stockagent.data_sync import packed_snapshots as packed, source_stream_staging
from stockagent.data_sync.desync_snapshots import SnapshotError


def fixture(tmp_path, monkeypatch, *, mismatch=False):
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_k: None)
    source = tmp_path/'C'/'shioaji_1m'
    (source/'nested').mkdir(parents=True)
    (source/'nested/a.parquet').write_bytes(b'one original source')
    (source/'nested/b.parquet').write_bytes(b'another source')
    cold = tmp_path/'D'
    packed.publish_packed_snapshot(cold, minute.COVERING_DATASET, source, node_id='test')
    if mismatch:
        (source/'nested/a.parquet').write_bytes(b'one changed source!')
    destination = tmp_path/'E'/'copy'
    evidence = tmp_path/'E'/'evidence'
    destination.parent.mkdir()
    receipt = source_stream_staging.copy(source, destination, evidence, workers=1)
    (source/'.download.lock').touch()
    stage = tmp_path/'E'/'migration'/'handoff-index-archive-v1'/minute.DATASET/('a'*64)
    stage.mkdir(parents=True)
    materialized = tmp_path/'materialized'
    materialized.mkdir()
    receipt_path = evidence/'copy-result.json'
    options = {'identity':'a'*64, 'copy_result_sha256':hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
               'original_inode':source.stat().st_ino, 'original_lock_inode':(source/'.download.lock').stat().st_ino,
               'files':receipt['files'], 'logical_bytes':receipt['logical_bytes'], 'materialized':materialized}
    return source, destination, cold, stage, evidence/'files.sqlite', receipt_path, options


def test_existing_d_matches_every_streamed_source_member_without_reading_payload_again(tmp_path, monkeypatch):
    source, destination, cold, stage, index, receipt, options = fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(packed, 'fetch_packed_snapshot', lambda *_a, **_k: pytest.fail('no independent readback'))
    original_open = type(source).open
    def reject_payload(path, *args, **kwargs):
        if path.is_relative_to(source) or path.is_relative_to(destination):
            pytest.fail('source and destination payloads must not be reopened')
        return original_open(path, *args, **kwargs)
    monkeypatch.setattr(type(source), 'open', reject_payload)
    proof = minute.prepare(cold, stage, index, receipt, **options)
    assert proof['files'] == 2 and proof['matched_items'] == 3
    assert proof['source_sha_already_streamed'] is True
    assert proof['source_payload_read_again'] is False
    assert proof['independent_D_readback'] is False and proof['NAS_accepted'] is False
    verify_options = {key:value for key, value in options.items() if key not in {'files','logical_bytes'}}
    assert minute.validate(cold, stage, index, receipt, **verify_options) == proof
    assert (source/'nested/a.parquet').exists() and (destination/'nested/b.parquet').exists()


def test_changed_source_cannot_reuse_the_existing_d_generation(tmp_path, monkeypatch):
    source, _destination, cold, stage, index, receipt, options = fixture(tmp_path, monkeypatch, mismatch=True)
    with pytest.raises(SnapshotError, match='content or link scope differs'):
        minute.prepare(cold, stage, index, receipt, **options)
    assert not (stage/minute.PROOF_NAME).exists()
    assert (source/'nested/a.parquet').exists()


def test_missing_pin_or_d_object_blocks_retirement_preparation(tmp_path, monkeypatch):
    source, _destination, cold, stage, index, receipt, options = fixture(tmp_path, monkeypatch)
    proof = minute.prepare(cold, stage, index, receipt, **options)
    verify_options = {key:value for key, value in options.items() if key not in {'files','logical_bytes'}}
    pin, value = minute._pin(proof, stage, options['materialized'])
    pin.unlink()
    with pytest.raises(SnapshotError, match='pin changed'):
        minute.validate(cold, stage, index, receipt, **verify_options)
    pin.write_text(json.dumps(value))
    snapshot = packed.resolve_packed_snapshot_id(cold, minute.COVERING_DATASET, proof['D_snapshot_id'])
    (cold/snapshot.manifest['archive']['objects'][0]['relpath']).unlink()
    with pytest.raises(SnapshotError):
        minute.validate(cold, stage, index, receipt, **verify_options)
    assert (source/'nested/a.parquet').exists()


def test_stream_receipt_or_index_change_is_rejected(tmp_path, monkeypatch):
    source, _destination, cold, stage, index, receipt, options = fixture(tmp_path, monkeypatch)
    proof = minute.prepare(cold, stage, index, receipt, **options)
    verify_options = {key:value for key, value in options.items() if key not in {'files','logical_bytes'}}
    receipt.write_text('{}')
    with pytest.raises(SnapshotError, match='receipt changed'):
        minute.validate(cold, stage, index, receipt, **verify_options)
    assert (source/'nested/a.parquet').exists()


def test_existing_d_rejects_uncheckpointed_index_wal(tmp_path, monkeypatch):
    source, _destination, cold, stage, index, receipt, options = fixture(tmp_path, monkeypatch)
    index.with_name(index.name+'-wal').write_bytes(b'pending')
    with pytest.raises(SnapshotError, match='uncheckpointed WAL'):
        minute.prepare(cold, stage, index, receipt, **options)
    assert (source/'nested/a.parquet').exists()


def test_publication_receipt_binds_pinned_public_d_without_fictitious_packages(tmp_path, monkeypatch):
    from stockagent.data_sync import index_archive_existing
    _source, _destination, cold, stage, index, receipt, options = fixture(tmp_path, monkeypatch)
    proof = minute.prepare(cold, stage, index, receipt, **options)
    monkeypatch.setattr(index_archive_existing, '_materialized_root', lambda: options['materialized'])
    result = minute.publication_receipt(proof, stage)
    assert index_archive_existing.receipt_matches(result, proof['identity_sha256'])
    assert result['new_object_count'] == 0 and result['independent_D_readback'] is False
    assert not list(stage.glob('pack-*'))
    minute._pin(proof, stage, options['materialized'])[0].unlink()
    assert not index_archive_existing.receipt_matches(result, proof['identity_sha256'])


def test_existing_d_retirement_resumes_and_refuses_to_continue_without_its_pin(tmp_path, monkeypatch):
    from stockagent.data_sync import handoff_source_repack, provider_workspace, workspace_source_handoff as h, workspace_volume
    source, destination, cold, stage, index, receipt, options = fixture(tmp_path, monkeypatch)
    proof = minute.prepare(cold, stage, index, receipt, **options)
    journal = {'state':'active', 'evidence':str(index.parent), 'destination':str(destination),
               'copy_result_sha256':options['copy_result_sha256'], 'original_inode':options['original_inode'],
               'original_lock_inode':options['original_lock_inode']}
    monkeypatch.setattr(minute, 'COLD', cold)
    monkeypatch.setattr(minute, 'MATERIALIZED', options['materialized'])
    monkeypatch.setattr(h, 'selected_source', lambda _name:nullcontext())
    monkeypatch.setattr(h, 'read_journal', lambda:journal)
    monkeypatch.setattr(h, '_scope', lambda:SimpleNamespace(source=tmp_path/'service-alias', backup=source))
    monkeypatch.setattr(h, '_assert_retained', lambda _value:destination)
    monkeypatch.setattr(workspace_volume, 'configuration', lambda:{'mount_point':str(tmp_path/'E')})
    monkeypatch.setattr(provider_workspace, 'exact_mount', lambda *_args:True)
    args = (source, stage, minute.DATASET, options['identity'], index)
    kwargs = {'original_inode':options['original_inode'], 'original_lock_inode':options['original_lock_inode'],
              'process_check':lambda _paths:[], 'reuse_existing_minute_D':True}
    original_unlink = type(source).unlink
    hit = []
    def interrupted(path, *args, **kw):
        result = original_unlink(path, *args, **kw)
        if path == source/'nested/a.parquet' and not hit:
            hit.append(path)
            raise OSError('interrupted after one unlink')
        return result
    monkeypatch.setattr(type(source), 'unlink', interrupted)
    with pytest.raises(OSError, match='interrupted after one unlink'):
        handoff_source_repack.retire(*args, **kwargs)
    monkeypatch.setattr(type(source), 'unlink', original_unlink)
    pin, pin_body = minute._pin(proof, stage, options['materialized'])
    pin.unlink()
    with pytest.raises(SnapshotError, match='pin changed'):
        handoff_source_repack.retire(*args, **kwargs)
    assert (source/'nested/b.parquet').exists()
    pin.write_text(json.dumps(pin_body))
    completed = handoff_source_repack.retire(*args, **kwargs)
    assert completed['state'] == 'C_payload_retired_root_and_lock_preserved'
    assert (source/'.download.lock').exists() and not (source/'nested/b.parquet').exists()
    assert (destination/'nested/a.parquet').exists() and (destination/'nested/b.parquet').exists()


def test_owner_workflow_recovers_after_interruption_with_honest_receipts(tmp_path, monkeypatch):
    from scripts import prepare_handoff_index_archive as cli
    from stockagent.data_sync import (cold_primary, handoff_source_repack, index_archive_existing,
                                     provider_workspace, workspace_source_handoff as h, workspace_volume)
    source, destination, cold, stage, index, receipt, options = fixture(tmp_path, monkeypatch)
    value = {'state':'active', 'evidence':str(index.parent), 'destination':str(destination),
             'copy_result_sha256':options['copy_result_sha256'], 'original_inode':options['original_inode'],
             'original_lock_inode':options['original_lock_inode'], 'files':options['files'],
             'logical_bytes':options['logical_bytes']}
    scope = SimpleNamespace(source=tmp_path/'service-alias', backup=source)
    controls = tmp_path/'controls'; controls.mkdir(); (controls/'owner.lock').touch()
    monkeypatch.setattr(minute, 'COLD', cold)
    monkeypatch.setattr(minute, 'MATERIALIZED', options['materialized'])
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', lambda _cold:None)
    monkeypatch.setattr(index_archive_existing, '_materialized_root', lambda:options['materialized'])
    monkeypatch.setattr(h, 'CONTROL', controls)
    monkeypatch.setattr(h, 'selected_source', lambda _name:nullcontext())
    monkeypatch.setattr(h, 'read_journal', lambda:value)
    monkeypatch.setattr(h, '_scope', lambda:scope)
    monkeypatch.setattr(h, '_assert_retained', lambda _value:destination)
    monkeypatch.setattr(workspace_volume, 'configuration', lambda:{'mount_point':str(tmp_path/'E')})
    monkeypatch.setattr(provider_workspace, 'exact_mount', lambda *_args:True)
    original_retire = handoff_source_repack.retire
    def retire(*args, **kwargs):
        return original_retire(*args, **kwargs, process_check=lambda _paths:[])
    monkeypatch.setattr(handoff_source_repack, 'retire', retire)
    def source_locks(stack):
        stack.enter_context(h._lock_supplemental_file(source/'.download.lock'))
    status = tmp_path/'E'/'minute.status.json'
    args = dict(value=value, scope=scope, destination=destination, index=index,
                identity=options['identity'], stage=stage, status=status, source_locks=source_locks,
                guard=lambda:None, report=lambda _event:None, retire=True)
    original_unlink = type(source).unlink
    hit = []
    def interrupted(path, *args, **kwargs):
        result = original_unlink(path, *args, **kwargs)
        if path == source/'nested/a.parquet' and not hit:
            hit.append(path)
            raise OSError('interrupted owner')
        return result
    monkeypatch.setattr(type(source), 'unlink', interrupted)
    with pytest.raises(OSError, match='interrupted owner'):
        cli.prepare_existing_minute(**args)
    pending = json.loads(status.read_text())
    assert pending['state'] == 'C_retirement_waiting' and pending['C_payload_deleted'] is False
    assert pending['D_archive_committed'] is True and pending['NAS_accepted'] is False
    monkeypatch.setattr(type(source), 'unlink', original_unlink)
    result = cli.prepare_existing_minute(**args)
    assert result['state'] == 'C_payload_retired_root_and_locks_preserved'
    assert result['C_payload_deleted'] is True and result['E_packages'] == 0
    assert result['D_publication']['independent_D_readback'] is False
    assert index_archive_existing.receipt_matches(result['D_publication'], result['prepared_sha256'])
    assert (source/'.download.lock').exists() and (destination/'nested/a.parquet').exists()

from __future__ import annotations

from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import shutil
import threading
import time
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.legacy_artifact_archive import (
    LegacyArchiveSpec, LINK_ARCHIVE_SCHEMA, MANUAL_WSL_CAPTURE_CONTRACT,
    OPAQUE_SYMLINK_POLICY, prepare_archive, publish_archive, restore_archive,
    verify_archive_directory, verify_cold_archive, recover_complete_archive_manifest,
)
import stockagent.data_sync.legacy_artifact_archive as archive_module
import stockagent.data_sync.benchmark_fixtures as benchmark_fixtures
from stockagent.data_sync.artifact_retirement import _reclaimable_file_bytes


def _case(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    root = tmp_path / 'artifacts'
    source = root / 'benchmarks' / 'fixture' / 'case'
    (source / 'data').mkdir(parents=True)
    (source / 'data' / 'report.json').write_bytes(b'original source bytes')
    (source / 'directory-link').symlink_to('data', target_is_directory=True)
    (source / 'file-link').symlink_to(source / 'data' / 'report.json')
    (source / 'dangling').symlink_to(source / 'absent')
    os.symlink(b'data/\xff', os.fsencode(source / 'non-utf8-target'))
    old_ns = time.time_ns() - 2 * 86400 * 10**9
    for path in (source / 'data' / 'report.json', source / 'directory-link',
                 source / 'file-link', source / 'dangling', source / 'non-utf8-target'):
        os.utime(path, ns=(old_ns, old_ns), follow_symlinks=False)
    spec = LegacyArchiveSpec('legacy-symlink-test', 'benchmarks/fixture/case', 7,
                             tmp_path / 'stage', 12, True, capture_contract=MANUAL_WSL_CAPTURE_CONTRACT,
                             symlink_policy=OPAQUE_SYMLINK_POLICY)
    monkeypatch.setattr(benchmark_fixtures, 'completed_benchmark_evidence', lambda *a, **kw: {})
    monkeypatch.setattr(archive_module, '_manual_capture_usage_gate', lambda *a, **kw: None)
    monkeypatch.setattr(archive_module, 'artifact_process_references', lambda *a, **kw: [])
    return spec, root, source


def test_symlinks_roundtrip_from_fixed_cold_release_without_following(tmp_path, monkeypatch):
    spec, root, source = _case(tmp_path, monkeypatch)
    before = {p.name: (os.readlink(os.fsencode(p)), p.lstat().st_mtime_ns)
              for p in source.iterdir() if p.is_symlink()}
    prepared = prepare_archive(spec, root, manual_capture=True, repo_root=tmp_path)
    assert prepared['schema_version'] == LINK_ARCHIVE_SCHEMA
    assert len(prepared['symlinks']) == 4
    archive = spec.stage_root / spec.dataset / 'archive'
    assert verify_archive_directory(archive, source, spec=spec, manual_capture=True,
                                    artifact_root=root, repo_root=tmp_path)['symlinks'] == 4
    packed = tmp_path / 'packed'
    release = publish_archive(spec, root, packed, repo_root=tmp_path, manual_capture=True)
    assert verify_cold_archive(spec, packed, reuse_encoded_archive=archive)['decoded_originals_verified']
    saved = source.with_name('original-in-test')
    source.rename(saved)
    restored = restore_archive(spec, packed, source, materialized_root=tmp_path / 'materialized',
                               snapshot_id=release['snapshot_id'])
    assert restored['files'] == 1 and restored['symlinks'] == 4
    assert (source / 'data' / 'report.json').read_bytes() == b'original source bytes'
    assert {p.name: (os.readlink(os.fsencode(p)), p.lstat().st_mtime_ns)
            for p in source.iterdir() if p.is_symlink()} == before
    assert (source / 'directory-link' / 'report.json').read_bytes() == b'original source bytes'
    assert (source / 'file-link').read_bytes() == b'original source bytes'
    assert (source / 'dangling').is_symlink() and not (source / 'dangling').exists()
    selected = restore_archive(spec, packed, tmp_path / 'selected', materialized_root=tmp_path / 'materialized',
                               include_paths=['dangling'], snapshot_id=release['snapshot_id'])
    assert selected['files'] == 0 and selected['symlinks'] == 1
    assert (tmp_path / 'selected' / 'dangling').is_symlink()


def test_symlinks_are_opt_in_internal_and_frozen(tmp_path, monkeypatch):
    spec, root, source = _case(tmp_path, monkeypatch)
    with pytest.raises(SnapshotError, match='symlink'):
        prepare_archive(replace(spec, symlink_policy=None), root, manual_capture=True)
    external = source / 'external'
    external.symlink_to('/etc/passwd')
    old_ns = time.time_ns() - 2 * 86400 * 10**9
    os.utime(external, ns=(old_ns, old_ns), follow_symlinks=False)
    with pytest.raises(SnapshotError, match='outside'):
        prepare_archive(spec, root, manual_capture=True)
    external.unlink()
    traversal = source / 'traversal'
    traversal.symlink_to('data/../data/report.json')
    os.utime(traversal, ns=(old_ns, old_ns), follow_symlinks=False)
    with pytest.raises(SnapshotError, match='parent traversal'):
        prepare_archive(spec, root, manual_capture=True)
    traversal.unlink()
    prepare_archive(spec, root, manual_capture=True)
    archive = spec.stage_root / spec.dataset / 'archive'
    link = source / 'directory-link'
    link.unlink()
    link.symlink_to('missing-dir', target_is_directory=True)
    os.utime(link, ns=(old_ns, old_ns), follow_symlinks=False)
    with pytest.raises(SnapshotError, match='symlinks differ'):
        verify_archive_directory(archive, source, spec=spec, manual_capture=True, artifact_root=root)
    assert link.is_symlink()


def test_symlink_manifest_cannot_replace_or_parent_a_real_member(tmp_path, monkeypatch):
    spec, root, source = _case(tmp_path, monkeypatch)
    prepare_archive(spec, root, manual_capture=True)
    archive = spec.stage_root / spec.dataset / 'archive'
    path = archive / 'legacy_archive_manifest.json'
    manifest = json.loads(path.read_text())
    row = manifest['symlinks'][0]
    row['path'] = 'data'
    manifest['symlinks'].sort(key=lambda item: item['path'])
    manifest['source_stability']['symlink_plan_sha256'] = hashlib.sha256(
        json.dumps(manifest['symlinks'], sort_keys=True).encode()).hexdigest()
    path.write_text(json.dumps(manifest))
    with pytest.raises(SnapshotError, match='overlap'):
        verify_archive_directory(archive, spec=spec)
    assert (source / 'data' / 'report.json').read_bytes() == b'original source bytes'


def test_interrupted_manifest_requires_exact_frozen_link_plan(tmp_path, monkeypatch):
    spec, root, _source = _case(tmp_path, monkeypatch)
    prepared = prepare_archive(spec, root, manual_capture=True)
    stage = spec.stage_root / spec.dataset
    (stage / 'archive' / 'legacy_archive_manifest.json').unlink()
    rebuilt = recover_complete_archive_manifest(spec, root, manual_capture=True)
    assert rebuilt == prepared
    (stage / 'archive' / 'legacy_archive_manifest.json').unlink()
    (stage / 'symlink_plan.json').unlink()
    with pytest.raises(SnapshotError, match='frozen symlink plan'):
        recover_complete_archive_manifest(spec, root, manual_capture=True)


@pytest.mark.parametrize('incomplete', [False, True])
def test_device_renumber_resumes_frozen_d_capture_without_reencoding_existing_files(
        tmp_path, monkeypatch, incomplete):
    import stockagent.data_sync.cold_primary as cold_primary

    spec, root, source = _case(tmp_path, monkeypatch)
    second = source / 'data' / 'second.json'
    second.write_bytes(b'additional original')
    old_ns = time.time_ns() - 2 * 86400 * 10**9
    os.utime(second, ns=(old_ns, old_ns))
    monkeypatch.setattr(archive_module, 'D_BENCHMARK_STAGE_ROOT', spec.stage_root)
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', lambda _root: None)
    signature = archive_module._source_signature
    with monkeypatch.context() as old_boot:
        old_boot.setattr(archive_module, '_source_signature',
                         lambda info: {**signature(info), 'device': info.st_dev + 77})
        original = prepare_archive(spec, root, manual_capture=True)
    stage = spec.stage_root / spec.dataset
    plan_before = (stage / 'source_plan.json').read_bytes()
    links_before = (stage / 'symlink_plan.json').read_bytes()
    manifest_path = stage / 'archive' / 'legacy_archive_manifest.json'
    manifest_path.unlink()
    missing = next(row for row in original['files'] if row['path'].endswith('second.json'))
    if incomplete:
        key = hashlib.sha256(missing['path'].encode()).hexdigest()
        (stage / 'receipts' / f'{key}.json').unlink()
        (stage / 'archive' / missing['encoded_path']).unlink()
        assert recover_complete_archive_manifest(spec, root, manual_capture=True) is None
        encoded = []
        encoder = archive_module._encode_one
        def track(path, *args, **kwargs):
            encoded.append(path.name)
            return encoder(path, *args, **kwargs)
        monkeypatch.setattr(archive_module, '_encode_one', track)
        resumed = prepare_archive(spec, root, manual_capture=True)
        assert encoded == ['second.json']
    else:
        monkeypatch.setattr(archive_module, '_encode_one', lambda *a, **kw: pytest.fail('reencoded'))
        resumed = recover_complete_archive_manifest(spec, root, manual_capture=True)
    assert resumed == original
    assert (stage / 'source_plan.json').read_bytes() == plan_before
    assert (stage / 'symlink_plan.json').read_bytes() == links_before
    proof = verify_archive_directory(stage / 'archive', source, spec=spec,
                                     manual_capture=True, artifact_root=root)
    assert proof['files'] == 2 and proof['symlinks'] == 4


def test_inventory_renumber_requires_exact_link_targets_and_observations(tmp_path, monkeypatch):
    from copy import deepcopy
    from scripts import retire_local_offline_artifacts as cleanup

    spec, root, _source = _case(tmp_path, monkeypatch)
    monkeypatch.setattr(cleanup, 'benchmark_evidence', lambda *a, **kw: {})
    current = cleanup.observe(root, spec.relative_root, service_refs=[], configuration_refs=[],
                              process_refs=[], repo_root=tmp_path, preserve_symlinks=True)
    old = deepcopy(current)
    for row in old['observation_rows']:
        row['signature'][0] += 77
    for link in old['symlinks']:
        link['source']['device'] += 77
    result = cleanup.uniform_device_renumber_drift(old, current)
    assert result and result['source_bytes_verified'] is False
    for field in ('target_b64', 'device', 'inode'):
        damaged = deepcopy(current)
        link = damaged['symlinks'][0]
        if field == 'target_b64':
            link[field] = link[field][::-1]
        else:
            link['source'][field] += 1
        assert cleanup.uniform_device_renumber_drift(old, damaged) is None


@pytest.mark.parametrize('defect', ['partial', 'mixed', 'inode', 'boolean', 'path'])
def test_interrupted_plan_renumber_rejects_partial_or_other_changes(defect):
    from copy import deepcopy

    source = {'device': 12, 'inode': 1, 'size': 2, 'mtime_ns': 3, 'ctime_ns': 4, 'mode': 0o600}
    old = [{'path': name, 'source': {**source, 'inode': index}}
           for index, name in enumerate(('a', 'b'), start=1)]
    current = deepcopy(old)
    for row in current:
        row['source']['device'] = 13
    assert archive_module._uniform_source_plan_device_renumber(old, current) == (12, 13)
    if defect == 'partial':
        current[0]['source']['device'] = 12
    elif defect == 'mixed':
        current[0]['source']['device'] = 14
    elif defect == 'inode':
        current[0]['source']['inode'] += 1
    elif defect == 'boolean':
        old[0]['source']['inode'] = True
    else:
        current[0]['path'] = 'different'
    assert archive_module._uniform_source_plan_device_renumber(old, current) is None


def test_reclaim_count_never_follows_links_or_counts_external_targets(tmp_path):
    tree = tmp_path / 'candidate'
    tree.mkdir()
    (tree / 'real').write_bytes(b'x' * 8192)
    outside = tmp_path / 'outside'
    outside.write_bytes(b'y' * 8192)
    (tree / 'link').symlink_to(outside)
    (tree / 'dirlink').symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(SnapshotError, match='non-regular'):
        _reclaimable_file_bytes(tree)
    assert _reclaimable_file_bytes(tree, allow_symlinks=True) == (tree / 'real').stat().st_blocks * 512
    assert outside.read_bytes() == b'y' * 8192


def test_parallel_d_capture_matches_serial_and_resumes_after_failure(tmp_path, monkeypatch):
    import stockagent.data_sync.cold_primary as cold_primary

    spec, root, source = _case(tmp_path, monkeypatch)
    old_ns = time.time_ns() - 2 * 86400 * 10**9
    for index in range(70):
        path = source / 'data' / f'{index:03}.json'
        path.write_text(f'payload-{index}')
        os.utime(path, ns=(old_ns, old_ns))
    serial = prepare_archive(spec, root, manual_capture=True)
    stage_root = tmp_path / 'guarded-d-stage'
    d_spec = replace(spec, stage_root=stage_root)
    monkeypatch.setattr(archive_module, 'D_BENCHMARK_STAGE_ROOT', stage_root)
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', lambda _root: None)
    original = archive_module._encode_one
    barrier = threading.Barrier(4)
    threads = set()
    threads_lock = threading.Lock()
    def observe(*args, **kwargs):
        with threads_lock:
            name = threading.current_thread().name
            first = name not in threads
            threads.add(name)
        if first:
            barrier.wait(timeout=10)
        return original(*args, **kwargs)
    monkeypatch.setattr(archive_module, '_encode_one', observe)
    parallel = prepare_archive(d_spec, root, manual_capture=True)
    assert len(threads) == 4 and parallel == serial
    assert verify_archive_directory(stage_root / spec.dataset / 'archive', source, spec=d_spec,
                                    manual_capture=True, artifact_root=root)['symlinks'] == 4

    interrupted_root = tmp_path / 'interrupted-d-stage'
    interrupted = replace(spec, stage_root=interrupted_root)
    monkeypatch.setattr(archive_module, 'D_BENCHMARK_STAGE_ROOT', interrupted_root)
    def fail_one(src, *args, **kwargs):
        if src.name == '010.json':
            raise OSError('injected encoding failure')
        return original(src, *args, **kwargs)
    monkeypatch.setattr(archive_module, '_encode_one', fail_one)
    with pytest.raises(OSError, match='injected encoding failure'):
        prepare_archive(interrupted, root, manual_capture=True)
    assert not (interrupted_root / spec.dataset / 'archive' / 'legacy_archive_manifest.json').exists()
    assert (source / 'data' / '010.json').read_text() == 'payload-10'
    monkeypatch.setattr(archive_module, '_encode_one', original)
    assert prepare_archive(interrupted, root, manual_capture=True) == serial


def test_inventory_explicitly_enrolls_only_frozen_in_scope_links(tmp_path, monkeypatch):
    from scripts import retire_local_offline_artifacts as cleanup

    spec, root, source = _case(tmp_path, monkeypatch)
    monkeypatch.setattr(cleanup, 'benchmark_evidence', lambda *a, **kw: {})
    kwargs = dict(service_refs=[], configuration_refs=[], process_refs=[], repo_root=tmp_path)
    blocked = cleanup.observe(root, spec.relative_root, **kwargs)
    assert 'non-regular-or-cross-filesystem' in blocked['blockers']
    observed = cleanup.observe(root, spec.relative_root, preserve_symlinks=True, **kwargs)
    assert observed['blockers'] == []
    assert observed['symlink_policy'] == OPAQUE_SYMLINK_POLICY
    assert len(observed['symlinks']) == 4
    assert {row['path'] for row in observed['symlinks']} == {
        'directory-link', 'file-link', 'dangling', 'non-utf8-target'}
    bad = source / 'outside'
    bad.symlink_to('/etc/passwd')
    old_ns = time.time_ns() - 2 * 86400 * 10**9
    os.utime(bad, ns=(old_ns, old_ns), follow_symlinks=False)
    with pytest.raises(SnapshotError, match='outside'):
        cleanup.observe(root, spec.relative_root, preserve_symlinks=True, **kwargs)


@pytest.mark.parametrize('change_link_after_plan', [False, True])
def test_symlink_retirement_and_exact_recovery_keep_final_gates(tmp_path, monkeypatch, change_link_after_plan):
    import stockagent.data_sync.artifact_retirement as common
    import stockagent.data_sync.cold_primary as cold_primary
    import stockagent.data_sync.legacy_artifact_retirement as retirement
    import stockagent.data_sync.materialized_cache as cache
    import stockagent.data_sync.remote_legacy_return as remote
    from stockagent.data_sync.packed_snapshots import initialize_packed_layout

    spec, root, source = _case(tmp_path, monkeypatch)
    packed, backup, hot, state = (tmp_path / name for name in ('packed', 'backup', 'hot', 'state'))
    hot.mkdir()
    initialize_packed_layout(packed, node_id='penguin')
    published = publish_archive(spec, root, packed, repo_root=tmp_path, manual_capture=True)
    shutil.copytree(packed, backup)
    monkeypatch.setattr(cold_primary.BackupConfig, 'load', lambda _p: SimpleNamespace(source=packed, destination=backup))
    monkeypatch.setattr(cold_primary.VolumeGuard, 'check', lambda self: None)
    monkeypatch.setattr(retirement, 'artifact_process_references', lambda *a: [])
    monkeypatch.setattr(retirement, 'process_references', lambda *a: [])
    monkeypatch.setattr(retirement, '_active_service_references', lambda *a: [])
    monkeypatch.setattr(common, 'artifact_process_references', lambda *a: [])
    monkeypatch.setattr(common, 'process_references', lambda *a: [])
    monkeypatch.setattr(common, 'artifact_service_references', lambda paths, *_: {str(p): [] for p in paths})
    monkeypatch.setattr(common, '_running_hot_bridge', lambda *_: False)
    monkeypatch.setattr(remote, 'active_configuration_references', lambda *a: [])
    options = dict(repo_root=tmp_path, artifact_root=root, hot_root=hot, sync_root=packed,
                   materialized_root=tmp_path/'materialized', state_root=state,
                   activation_root=tmp_path/'activations', backup_config=tmp_path/'backup.json',
                   peer_proof={'ok': True, 'checked_at': datetime.now(timezone.utc).isoformat(), 'peers': []},
                   bridge_inactive=True, manual_immediate=True, manual_capture=True)
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert plan['apply_ready']
    if change_link_after_plan:
        link = source/'dangling'
        link.unlink()
        link.symlink_to(source/'different')
        old_ns = time.time_ns() - 2 * 86400 * 10**9
        os.utime(link, ns=(old_ns, old_ns), follow_symlinks=False)
        with pytest.raises(SnapshotError, match='symlinks differ'):
            retirement.apply_legacy_retirement(spec, expected_fingerprint=plan['plan_fingerprint'], **options)
        assert (source/'data'/'report.json').read_bytes() == b'original source bytes'
        return
    retired = retirement.apply_legacy_retirement(spec, expected_fingerprint=plan['plan_fingerprint'], **options)
    assert retired['deleted'] and not source.exists()
    monkeypatch.setattr(archive_module, 'D_BENCHMARK_STAGE_ROOT', spec.stage_root)
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', lambda _root: None)
    monkeypatch.setattr(cache, '_pinned_snapshot_ids', lambda _root: set())
    monkeypatch.setattr(cache, 'process_references', lambda *_args: [])
    stage = spec.stage_root / spec.dataset
    link_plan = stage / 'symlink_plan.json'
    frozen = link_plan.read_bytes()
    link_plan.write_text('[]')
    with pytest.raises(SnapshotError, match='symlink-plan'):
        archive_module.plan_archive_stage_prune(spec, packed, artifact_root=root, state_root=state)
    assert stage.is_dir()
    link_plan.write_bytes(frozen)
    stage_plan = archive_module.plan_archive_stage_prune(spec, packed, artifact_root=root, state_root=state)
    pruned = archive_module.apply_archive_stage_prune(
        spec, packed, artifact_root=root, state_root=state,
        expected_fingerprint=stage_plan['stage_fingerprint'], owned_verified_plan=stage_plan)
    assert pruned['pruned'] and not stage.exists() and pruned['cold_objects_deleted'] == 0
    restored = restore_archive(spec, packed, source, materialized_root=tmp_path/'restored-cache',
                               snapshot_id=published['snapshot_id'])
    assert restored['symlinks'] == 4
    assert (source/'file-link').read_bytes() == b'original source bytes'
    assert (source/'dangling').is_symlink() and not (source/'dangling').exists()


@pytest.mark.parametrize('change_link_after_quarantine', [False, True])
def test_owned_split_quarantine_audits_symlinks_before_unlink(
        tmp_path, monkeypatch, change_link_after_quarantine):
    """The resumed audit uses the original root for opaque absolute targets."""
    import stockagent.data_sync.artifact_retirement as common
    import stockagent.data_sync.cold_primary as cold_primary
    import stockagent.data_sync.legacy_artifact_retirement as retirement
    import stockagent.data_sync.remote_legacy_return as remote
    from scripts import retire_local_offline_artifacts as cleanup
    from stockagent.data_sync.packed_snapshots import initialize_packed_layout

    spec, root, source = _case(tmp_path, monkeypatch)
    packed, backup, hot, state = (tmp_path / name for name in ('packed', 'backup', 'hot', 'state'))
    hot.mkdir()
    initialize_packed_layout(packed, node_id='penguin')
    release = publish_archive(spec, root, packed, repo_root=tmp_path, manual_capture=True)
    shutil.copytree(packed, backup)
    monkeypatch.setattr(cold_primary.BackupConfig, 'load',
                        lambda _p: SimpleNamespace(source=packed, destination=backup))
    monkeypatch.setattr(cold_primary.VolumeGuard, 'check', lambda self: None)
    monkeypatch.setattr(retirement, 'artifact_process_references', lambda *a: [])
    monkeypatch.setattr(retirement, 'process_references', lambda *a: [])
    monkeypatch.setattr(retirement, '_active_service_references', lambda *a: [])
    monkeypatch.setattr(common, 'artifact_process_references', lambda *a: [])
    monkeypatch.setattr(common, 'process_references', lambda *a: [])
    monkeypatch.setattr(common, 'artifact_service_references', lambda paths, *_: {str(p): [] for p in paths})
    monkeypatch.setattr(common, '_running_hot_bridge', lambda *_: False)
    monkeypatch.setattr(remote, 'active_configuration_references', lambda *a: [])
    options = dict(repo_root=tmp_path, artifact_root=root, hot_root=hot, sync_root=packed,
                   materialized_root=tmp_path/'materialized', state_root=state,
                   activation_root=tmp_path/'activations', backup_config=tmp_path/'backup.json',
                   peer_proof={'ok': True, 'checked_at': datetime.now(timezone.utc).isoformat(), 'peers': []},
                   bridge_inactive=True, manual_immediate=True, manual_capture=True)
    plan = retirement.plan_legacy_retirement(spec, **options)
    receipt = tmp_path / 'receipt'
    receipt.mkdir()
    monkeypatch.setattr(cleanup, 'COMMON_OWNER', tmp_path / 'common.lock')
    monkeypatch.setattr(cleanup, '_wait_for_convergence', lambda *a: {'ok': True})
    monkeypatch.setattr(cleanup, 'verified_retained_benchmark', lambda *a: {})
    apply = cleanup.apply_legacy_retirement
    quarantine = []

    def after_rename(current_spec, **kwargs):
        try:
            return apply(current_spec, **kwargs)
        except retirement.LegacyQuarantineAuditRequired:
            record = json.loads(retirement._state_path(state, spec.dataset).read_bytes())
            path = Path(record['quarantine'])
            quarantine.append(path)
            assert (path/'source'/'dangling').is_symlink()
            if change_link_after_quarantine:
                link = path/'source'/'dangling'
                link.unlink()
                link.symlink_to(source/'different')
                old_ns = time.time_ns() - 2 * 86400 * 10**9
                os.utime(link, ns=(old_ns, old_ns), follow_symlinks=False)
            raise

    monkeypatch.setattr(cleanup, 'apply_legacy_retirement', after_rename)
    if change_link_after_quarantine:
        with pytest.raises(SnapshotError):
            cleanup.retire_after_convergence(spec, None, receipt, plan, options,
                                            separate_quarantine_audit=True)
        assert quarantine and (quarantine[0]/'source'/'dangling').is_symlink()
        assert (quarantine[0]/'source'/'data'/'report.json').read_bytes() == b'original source bytes'
        assert json.loads(retirement._state_path(state, spec.dataset).read_bytes())['state'] == 'retiring'
    else:
        result = cleanup.retire_after_convergence(spec, None, receipt, plan, options,
                                                 separate_quarantine_audit=True)
        assert result['deleted'] and result['resumed_quarantine'] and not source.exists()
        restored = restore_archive(spec, packed, source, materialized_root=tmp_path/'restored-cache',
                                   snapshot_id=release['snapshot_id'])
        assert restored['symlinks'] == 4
        assert (source/'file-link').read_bytes() == b'original source bytes'
        assert (source/'dangling').is_symlink() and not (source/'dangling').exists()

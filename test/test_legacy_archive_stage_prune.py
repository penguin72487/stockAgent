from dataclasses import replace
import json
import hashlib
import os
from pathlib import Path
import time

import pytest

from stockagent.data_sync.desync_snapshots import SnapshotError
import stockagent.data_sync.legacy_artifact_archive as module
from stockagent.data_sync.legacy_artifact_archive import (
    apply_archive_stage_prune, plan_archive_stage_prune, verify_cold_archive,
)
from stockagent.data_sync.packed_snapshots import resolve_latest_packed


@pytest.mark.parametrize('workers', [1, 8])
@pytest.mark.parametrize('mutation', ['replace-same-size-mtime', 'mode', 'mtime',
    'add', 'delete', 'symlink-file', 'symlink-directory', 'fifo'])
def test_stage_fingerprint_observes_each_new_generation_and_rejects_redirects(tmp_path, mutation, workers):
    stage = tmp_path / 'stage'
    (stage / 'nested').mkdir(parents=True)
    payload = stage / 'nested/行情.json'
    payload.write_bytes(b'old')
    plan = {'dataset': 'engineering-fixture', 'snapshot_id': 'fixed', 'manifest_sha256': 'a' * 64}
    before = module._stage_prune_fingerprint(stage, plan, file_workers=workers)
    info = payload.stat()
    if mutation == 'replace-same-size-mtime':
        replacement = tmp_path / 'replacement'
        replacement.write_bytes(b'new')
        os.utime(replacement, ns=(info.st_atime_ns, info.st_mtime_ns))
        replacement.replace(payload)
    elif mutation == 'mode':
        payload.chmod(0o400)
    elif mutation == 'mtime':
        os.utime(payload, ns=(info.st_atime_ns, info.st_mtime_ns + 1))
    elif mutation == 'add':
        (stage / 'new').write_bytes(b'new observation')
    elif mutation == 'delete':
        payload.unlink()
    elif mutation == 'symlink-file':
        payload.unlink()
        payload.symlink_to(tmp_path / 'outside-source')
    elif mutation == 'symlink-directory':
        (stage / 'redirected').symlink_to(tmp_path / 'outside-root', target_is_directory=True)
        # os.walk classifies a dangling directory symlink as a file. Create
        # the external directory so this exercises the directory guard too.
        (tmp_path / 'outside-root').mkdir()
    else:
        payload.unlink()
        os.mkfifo(payload)
    if mutation in {'symlink-file', 'symlink-directory', 'fifo'}:
        with pytest.raises(SnapshotError, match='symlink directory|non-regular file'):
            module._stage_prune_fingerprint(stage, plan, file_workers=workers)
    else:
        assert module._stage_prune_fingerprint(stage, plan, file_workers=workers) != before


def test_parallel_stage_fingerprint_preserves_full_generation_and_fails_closed(tmp_path, monkeypatch):
    stage = tmp_path / 'stage'
    (stage / 'nested').mkdir(parents=True)
    for index in range(140):
        (stage / 'nested' / str(index)).write_text(str(index))
    plan = {'dataset': 'fixture', 'snapshot_id': 'fixed', 'manifest_sha256': 'a' * 64}
    before = module._stage_prune_fingerprint(stage, plan)
    assert module._stage_prune_fingerprint(stage, plan, file_workers=4) == before
    assert module._stage_prune_fingerprint(stage, plan, file_workers=8) == before
    (stage / 'nested' / '1').write_text('different')
    assert module._stage_prune_fingerprint(stage, plan, file_workers=4) != before
    original_scandir = os.scandir
    def denied(path):
        if Path(path) == stage / 'nested':
            raise PermissionError('unreadable stage')
        return original_scandir(path)
    monkeypatch.setattr(os, 'scandir', denied)
    with pytest.raises(PermissionError, match='unreadable stage'):
        module._stage_prune_fingerprint(stage, plan, file_workers=4)
    with pytest.raises(PermissionError, match='unreadable stage'):
        module._stage_prune_fingerprint(stage, plan, file_workers=8)


def test_stage_allocation_matches_independent_hardlink_aware_audit(tmp_path):
    from stockagent.data_sync.artifact_retirement import _reclaimable_file_bytes
    stage = tmp_path / 'stage'
    stage.mkdir()
    (stage / 'single').write_bytes(b'one')
    (stage / 'two-links').write_bytes(b'two')
    os.link(stage / 'two-links', stage / 'same-stage-link')
    (stage / 'external').write_bytes(b'three')
    os.link(stage / 'external', tmp_path / 'outside-link')
    reference = _reclaimable_file_bytes(stage)
    for workers in (1, 4, 8):
        rows, allocated = module._stage_prune_signatures(stage, file_workers=workers, collect_allocation=True)
        assert allocated == reference
        assert rows == module._stage_prune_signatures(stage, file_workers=workers)


@pytest.mark.parametrize('soft_limit, expected', [(1024, 8), (63, 4), (module.resource.RLIM_INFINITY, 8)])
def test_stage_metadata_parallelism_is_bounded_to_exact_approved_d_stage(tmp_path, monkeypatch, soft_limit, expected):
    spec, _packed, _options = fixture(tmp_path, monkeypatch, benchmark_d=True)
    stage = spec.stage_root / spec.dataset
    monkeypatch.setattr(module.resource, 'getrlimit', lambda _which: (soft_limit, soft_limit))
    assert module._stage_prune_metadata_workers(spec, stage) == expected
    assert module._stage_prune_metadata_workers(spec, stage.with_name('different')) == 1
    quarantine = stage.with_name(f'.{spec.dataset}.pruning-{os.getpid()}')
    assert module._stage_prune_metadata_workers(spec, quarantine) == expected
    other = stage.with_name(f'.{spec.dataset}.pruning-{os.getpid() + 1}')
    assert module._stage_prune_metadata_workers(spec, other) == 1
    assert module._stage_prune_metadata_workers(replace(spec, capture_contract=None), stage) == 4


@pytest.mark.parametrize('fd_observation', ['busy', 'unreadable'])
def test_stage_metadata_parallelism_preserves_live_fd_headroom(tmp_path, monkeypatch, fd_observation):
    spec, _packed, _options = fixture(tmp_path, monkeypatch, benchmark_d=True)
    stage = spec.stage_root / spec.dataset
    monkeypatch.setattr(module.resource, 'getrlimit', lambda _which: (64, 64))
    listdir = module.os.listdir
    def observed(path):
        if path == '/proc/self/fd':
            if fd_observation == 'unreadable':
                raise OSError('descriptor inventory unavailable')
            return [str(i) for i in range(50)]
        return listdir(path)
    monkeypatch.setattr(module.os, 'listdir', observed)
    assert module._stage_prune_metadata_workers(spec, stage) == 4


def test_parallel_d_stage_prune_rechecks_and_recovers_exact_fixed_source(tmp_path, monkeypatch):
    spec, packed, options = fixture(tmp_path, monkeypatch, benchmark_d=True)
    monkeypatch.setattr(module.resource, 'getrlimit', lambda _which: (1024, 1024))
    original_proof = verify_cold_archive(spec, packed)
    signatures = module._stage_prune_signatures
    workers_seen = []
    def observe(*args, **kwargs):
        workers_seen.append(kwargs.get('file_workers', 1))
        return signatures(*args, **kwargs)
    monkeypatch.setattr(module, '_stage_prune_signatures', observe)
    plan = plan_archive_stage_prune(spec, packed, **options)
    result = apply_archive_stage_prune(spec, packed, expected_fingerprint=plan['stage_fingerprint'],
                                       owned_verified_plan=plan, **options)
    assert len(workers_seen) >= 5 and all(workers == 8 for workers in workers_seen)
    assert result['pruned'] and result['cold_objects_deleted'] == 0
    proof = verify_cold_archive(spec, packed)
    assert proof['cold_verified'] and proof['decoded_originals_verified']
    assert all(proof[key] == original_proof[key]
               for key in ('files', 'original_bytes', 'snapshot_id', 'manifest_sha256'))


def fixture(tmp_path, monkeypatch, *, benchmark_d=False, offline_d=False):
    from test_legacy_manual_capture import fixture as source_fixture
    import stockagent.data_sync.materialized_cache as cache
    import stockagent.data_sync.artifact_maintenance as maintenance
    # These private source/stage fixtures have their own process inventory.
    # Retain the real fd/maps/ancestor parsers without walking unrelated host
    # processes and Windows paths for each synthetic publication.
    processes = tmp_path / 'proc'
    processes.mkdir()
    def process_directories():
        return sorted(path for path in processes.iterdir() if path.name.isdecimal())
    monkeypatch.setattr(cache, '_process_directories', process_directories)
    monkeypatch.setattr(maintenance, '_process_directories', process_directories)
    # This unit fixture owns a private materialization tree. Exercise the real
    # pin reader there, not a multi-million-file production cache per case.
    # Individual fault tests may still override the reader after setup.
    pin_root = tmp_path / 'materialized'
    pin_root.mkdir()
    read_pins = cache._pinned_snapshot_ids
    monkeypatch.setattr(cache, '_pinned_snapshot_ids', lambda root: read_pins(pin_root))
    spec, artifacts, source = source_fixture(tmp_path)
    if benchmark_d:
        from test_local_offline_artifact_retirement import _completed_benchmark
        target = artifacts / 'benchmarks/suite/completed-case'
        target.parent.mkdir(parents=True)
        source.rename(target)
        source = target
        _completed_benchmark(source)
        spec = replace(spec, relative_root='benchmarks/suite/completed-case',
                       capture_contract=module.MANUAL_WSL_CAPTURE_CONTRACT)
    elif offline_d:
        target = artifacts / 'operations/closed-audit'
        target.parent.mkdir(parents=True)
        source.rename(target)
        source = target
        spec = replace(spec, relative_root='operations/closed-audit',
                       capture_contract=module.MANUAL_WSL_CAPTURE_CONTRACT)
    packed = tmp_path / "packed"
    release = module.publish_archive(spec, artifacts, packed, repo_root=artifacts.parent,
                                     manual_capture=True)
    source.rename(source.with_name("original-byte-proof-kept-by-test"))
    states = tmp_path / "states"
    state = states / "retirements" / f"{spec.dataset}.json"
    state.parent.mkdir(parents=True)
    state.write_text(json.dumps({"state": "cold-only", "dataset": spec.dataset,
                                "relative_root": spec.relative_root, "snapshot_id": release["snapshot_id"]}))
    stage_constant = ('D_BENCHMARK_STAGE_ROOT' if benchmark_d else
                      'D_OFFLINE_STAGE_ROOT' if offline_d else 'C_TEMP_STAGE_ROOT')
    monkeypatch.setattr(module, stage_constant, spec.stage_root)
    import stockagent.data_sync.cold_primary as primary
    monkeypatch.setattr(primary, "_check_d_primary_mount", lambda root: None)
    options = {"artifact_root": artifacts, "state_root": states}
    return spec, packed, options


def reviewed_legacy_fixture(tmp_path, monkeypatch):
    spec, packed, options = fixture(tmp_path, monkeypatch)
    # Remove the C scratch exemption: this test must admit only the new exact
    # dataset/release/retirement binding, while keeping real full recovery.
    monkeypatch.setattr(module, 'C_TEMP_STAGE_ROOT', tmp_path / 'other-c-scratch')
    monkeypatch.setattr(module, 'LEGACY_D_STAGE_ROOT', spec.stage_root)
    selected = resolve_latest_packed(packed, spec.dataset)
    retired = options['state_root'] / 'retirements' / (spec.dataset + '.json')
    binding = {'relative_root': spec.relative_root, 'snapshot_id': selected.manifest['snapshot_id'],
               'manifest_sha256': selected.manifest_sha256,
               'retirement_receipt_sha256': hashlib.sha256(retired.read_bytes()).hexdigest()}
    policy = tmp_path / 'reviewed-stage.json'
    policy.write_text(json.dumps({'schema_version': 1, 'contract': 'reviewed_retired_legacy_d_encoding_v1',
        'authority_node_id': 'penguin', 'stage_root': str(spec.stage_root), 'datasets': {spec.dataset: binding}}))
    monkeypatch.setattr(module, 'REVIEWED_STAGE_POLICY', policy)
    return spec, packed, options, policy, binding


def test_reviewed_old_d_encoding_reuses_complete_recovery_and_preserves_cold(tmp_path, monkeypatch):
    spec, packed, options, _policy, _binding = reviewed_legacy_fixture(tmp_path, monkeypatch)
    before = {path.relative_to(packed).as_posix(): path.read_bytes()
              for path in packed.rglob('*') if path.is_file()}
    plan = plan_archive_stage_prune(spec, packed, **options)
    assert module._d_stage_file_workers(spec, spec.stage_root / spec.dataset / 'archive') == 4
    result = apply_archive_stage_prune(spec, packed, expected_fingerprint=plan['stage_fingerprint'],
                                      owned_verified_plan=plan, **options)
    assert result['pruned'] and result['reclaimed_allocated_file_bytes'] > 0
    assert {path.relative_to(packed).as_posix(): path.read_bytes()
            for path in packed.rglob('*') if path.is_file()} == before
    assert verify_cold_archive(spec, packed)['decoded_originals_verified']


def test_reviewed_stage_fuses_exact_commitments_and_reuses_decode_only_for_same_generation(tmp_path, monkeypatch):
    spec, packed, options, _policy, _binding = reviewed_legacy_fixture(tmp_path, monkeypatch)
    decode, reconstruct = module.verify_archive_directory, module.verify_packed_snapshot
    calls, cold_calls = [], []
    def decoded(*args, **kwargs):
        calls.append(args[0])
        return decode(*args, **kwargs)
    def recovered(*args, **kwargs):
        cold_calls.append(kwargs)
        return reconstruct(*args, **kwargs)
    monkeypatch.setattr(module, 'verify_archive_directory', decoded)
    monkeypatch.setattr(module, 'verify_packed_snapshot', recovered)
    plan = plan_archive_stage_prune(spec, packed, **options)
    proof_time = plan['full_verification']['verified_at_epoch']
    result = apply_archive_stage_prune(spec, packed, expected_fingerprint=plan['stage_fingerprint'],
                                      owned_verified_plan=plan, **options)
    assert len(calls) == 1 and len(cold_calls) == 1
    assert cold_calls[0]['reconstruct_all'] is True and 'materialized_path' not in cold_calls[0]
    assert result['pruned'] and result['full_verification']['verified_at_epoch'] == proof_time
    assert result['encoding_decode_reuse']['verification_time_extended'] is False
    assert verify_cold_archive(spec, packed)['decoded_originals_verified']


@pytest.mark.parametrize('mutation', ['cold-commitment', 'portable-tree', 'decode-generation', 'cold-counts', 'expired'])
def test_reviewed_fused_proof_never_replaces_complete_binding_or_fresh_generation(tmp_path, monkeypatch, mutation):
    spec, packed, options, _policy, _binding = reviewed_legacy_fixture(tmp_path, monkeypatch)
    if mutation == 'cold-commitment':
        load = module._load_inventory
        def wrong(*args):
            rows = [dict(row) for row in load(*args)]
            for row in rows:
                if row['kind'] == 'file':
                    row['sha256'] = '0'*64
                    break
            return rows
        monkeypatch.setattr(module, '_load_inventory', wrong)
    elif mutation == 'portable-tree':
        monkeypatch.setattr(module, 'scan_tree', lambda *a, **kw: {'portable_fingerprint_sha256': '0'*64})
    if mutation in {'cold-commitment', 'portable-tree'}:
        with pytest.raises(SnapshotError, match='commitments|portable metadata'):
            plan_archive_stage_prune(spec, packed, **options)
    else:
        plan = plan_archive_stage_prune(spec, packed, **options)
        if mutation == 'decode-generation':
            plan['full_verification']['encoding_stage_fingerprint'] = '0'*64
        elif mutation == 'cold-counts':
            plan['full_verification']['complete_cold_reconstruction']['independently_reconstructed_files'] = 0
        else:
            plan['full_verification']['verified_at_epoch'] = time.time()-1501
        with pytest.raises(SnapshotError):
            apply_archive_stage_prune(spec, packed, expected_fingerprint=plan['stage_fingerprint'],
                                      owned_verified_plan=plan, **options)
    assert (spec.stage_root / spec.dataset).is_dir()


def test_reviewed_stage_uses_enrolled_vast_policy_only_with_native_d_authority(tmp_path, monkeypatch):
    from stockagent.data_sync import packed_verification
    spec, packed, options, _policy, _binding = reviewed_legacy_fixture(tmp_path, monkeypatch)
    calls = []
    monkeypatch.setattr(module, '_native_read_options', lambda root: {'d_primary_native_blob_reads': True})
    monkeypatch.setattr(packed_verification, 'configuration', lambda dataset: calls.append(dataset) or {'enabled': True})
    recovery = module._reviewed_stage_recovery_options(spec, packed, options['state_root'])
    assert calls == [spec.dataset] and recovery['d_primary_native_original_hashes'] is True
    assert recovery['pack_verification']['evidence_directory'].startswith(str(options['state_root']/'stage-pruning'/spec.dataset))
    monkeypatch.setattr(module, '_native_read_options', lambda root: {})
    assert module._reviewed_stage_recovery_options(spec, packed, options['state_root']) == {}
    assert calls == [spec.dataset]


def test_reviewed_native_encoding_path_cannot_admit_unlisted_or_other_owners(tmp_path, monkeypatch):
    spec, _packed, _options, _policy, _binding = reviewed_legacy_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(module, 'load_legacy_specs', lambda _path: {spec.dataset: spec})
    source = spec.stage_root / spec.dataset / 'archive/payload/test.gz'
    assert module.reviewed_retired_stage_path(source)
    assert module.reviewed_retired_stage_path(spec.stage_root/f'.{spec.dataset}.pruning-{os.getpid()}'/'archive/payload/test.gz')
    for other in (spec.stage_root/'other/archive/payload/test.gz',
                  spec.stage_root/spec.dataset/'receipts/test.gz',
                  spec.stage_root/f'.{spec.dataset}.pruning-{os.getpid()+1}'/'archive/payload/test.gz'):
        assert not module.reviewed_retired_stage_path(other)


@pytest.mark.parametrize('mutation', ['other-dataset', 'manifest', 'retirement', 'redirect-policy',
                                      'reappeared-source', 'binding-changed-after-plan'])
def test_reviewed_old_d_encoding_rejects_unbound_or_changed_inputs(tmp_path, monkeypatch, mutation):
    spec, packed, options, policy, _binding = reviewed_legacy_fixture(tmp_path, monkeypatch)
    plan = plan_archive_stage_prune(spec, packed, **options) if mutation == 'binding-changed-after-plan' else None
    data = json.loads(policy.read_text())
    if mutation == 'other-dataset':
        data['datasets']['unregistered'] = data['datasets'].pop(spec.dataset)
    elif mutation in {'manifest', 'binding-changed-after-plan'}:
        data['datasets'][spec.dataset]['manifest_sha256'] = '0' * 64
    elif mutation == 'retirement':
        retired = options['state_root'] / 'retirements' / (spec.dataset + '.json')
        retired.write_bytes(retired.read_bytes() + b' ')
    elif mutation == 'redirect-policy':
        real = policy.with_suffix('.real')
        policy.rename(real)
        policy.symlink_to(real)
    else:
        (options['artifact_root'] / spec.relative_root).mkdir(parents=True)
    if mutation not in {'retirement', 'redirect-policy', 'reappeared-source'}:
        policy.write_text(json.dumps(data))
    with pytest.raises(SnapshotError):
        if plan is not None:
            apply_archive_stage_prune(spec, packed, expected_fingerprint=plan['stage_fingerprint'],
                                      owned_verified_plan=plan, **options)
        else:
            plan_archive_stage_prune(spec, packed, **options)
    assert (spec.stage_root / spec.dataset).is_dir()


@pytest.mark.parametrize('after_audit', [False, True])
def test_stage_prune_keeps_actual_fd_parser_and_fresh_use_gate_in_isolated_proc(
    tmp_path, monkeypatch, after_audit,
):
    spec, packed, options = fixture(tmp_path, monkeypatch)
    stage = spec.stage_root / spec.dataset
    original_files = {path.relative_to(stage).as_posix(): path.read_bytes()
                      for path in stage.rglob('*') if path.is_file()}
    plan = plan_archive_stage_prune(spec, packed, **options) if after_audit else None
    process = tmp_path / 'proc/999999999'
    (process / 'fd').mkdir(parents=True)
    for name in ('cwd', 'root', 'exe'):
        (process / name).symlink_to(tmp_path, target_is_directory=True)
    (process / 'maps').write_text('')
    (process / 'cmdline').write_bytes(b'isolated-fixture-worker\0')
    descriptor = process / 'fd/3'
    descriptor.symlink_to(stage / 'archive/legacy_archive_manifest.json')
    expected_error = 'final gates changed' if after_audit else 'live process references'
    with pytest.raises(SnapshotError, match=expected_error):
        if after_audit:
            apply_archive_stage_prune(spec, packed, expected_fingerprint=plan['stage_fingerprint'],
                owned_verified_plan=plan, **options)
        else:
            plan_archive_stage_prune(spec, packed, **options)
    protected = next(spec.stage_root.glob(f'.{spec.dataset}.pruning-*')) if after_audit else stage
    assert {path.relative_to(protected).as_posix(): path.read_bytes()
            for path in protected.rglob('*') if path.is_file()} == original_files
    descriptor.unlink()
    if not after_audit:
        assert plan_archive_stage_prune(spec, packed, **options)['apply_ready'] is True
    else:
        assert not stage.exists() and protected.is_dir()


@pytest.mark.parametrize('benchmark_d', [False, True])
def test_stage_prune_reads_real_pins_from_isolated_fixture(tmp_path, monkeypatch, benchmark_d):
    spec, packed, options = fixture(tmp_path, monkeypatch, benchmark_d=benchmark_d)
    release = resolve_latest_packed(packed, spec.dataset)
    pin = tmp_path / 'materialized' / 'selected.pin.json'
    pin.write_text(json.dumps({'manifest': {'snapshot_id': release.manifest['snapshot_id']}}))
    with pytest.raises(SnapshotError, match='release is pinned'):
        plan_archive_stage_prune(spec, packed, **options)
    pin.write_text(json.dumps({'manifest': {}}))
    with pytest.raises(SnapshotError, match='unknown protection schema'):
        plan_archive_stage_prune(spec, packed, **options)
    pin.unlink()
    assert plan_archive_stage_prune(spec, packed, **options)['snapshot_id'] == release.manifest['snapshot_id']
    assert (spec.stage_root / spec.dataset).is_dir()


def test_c_encoding_scratch_is_pruned_only_after_cold_recovery_and_hot_retirement(tmp_path, monkeypatch):
    spec, packed, options = fixture(tmp_path, monkeypatch)
    plan = plan_archive_stage_prune(spec, packed, **options)
    result = apply_archive_stage_prune(spec, packed, expected_fingerprint=plan["stage_fingerprint"], **options)
    assert result["pruned"] is True and result["cold_objects_deleted"] == 0
    assert not (spec.stage_root / spec.dataset).exists()
    verified = verify_cold_archive(spec, packed)
    assert verified["cold_verified"] and verified["decoded_originals_verified"]
    assert verified["verification_scratch_removed"]
    assert verified["files"] == 2


def test_completed_benchmark_d_scratch_uses_the_same_full_recovery_and_quarantine_gates(tmp_path, monkeypatch):
    spec, packed, options = fixture(tmp_path, monkeypatch, benchmark_d=True)
    plan = plan_archive_stage_prune(spec, packed, **options)
    result = apply_archive_stage_prune(spec, packed, expected_fingerprint=plan['stage_fingerprint'],
                                     owned_verified_plan=plan, **options)
    assert result['pruned'] and result['cold_objects_deleted'] == 0
    assert not (spec.stage_root / spec.dataset).exists()
    proof = verify_cold_archive(spec, packed)
    assert proof['cold_verified'] and proof['decoded_originals_verified'] and proof['files'] == 4


def test_d_stage_parallel_receipt_verification_rejects_corruption(tmp_path, monkeypatch):
    spec, packed, options = fixture(tmp_path, monkeypatch, benchmark_d=True)
    stage = spec.stage_root / spec.dataset
    assert module._d_stage_file_workers(spec, stage / 'archive') == 4
    quarantine = stage.with_name(f'.{spec.dataset}.pruning-{os.getpid()}')
    assert module._d_stage_file_workers(spec, quarantine / 'archive') == 4
    assert module._d_stage_file_workers(spec, stage.with_name('.other.pruning-1') / 'archive') == 1
    receipt = next((stage / 'receipts').iterdir())
    receipt.write_text('{}')
    with pytest.raises(SnapshotError, match='per-file receipt differs'):
        plan_archive_stage_prune(spec, packed, **options)
    assert stage.is_dir()


def test_stage_prune_progress_observes_full_work_but_is_not_a_receipt(tmp_path, monkeypatch):
    spec, packed, options = fixture(tmp_path, monkeypatch, offline_d=True)
    observations = []
    plan = plan_archive_stage_prune(spec, packed, progress=observations.append, **options)
    planned_steps = {row['prune_step'] for row in observations}
    assert {'plan-initial-stage-fingerprint', 'plan-verify-archive', 'plan-reconstruct-cold',
            'plan-check-receipts', 'plan-final-stage-fingerprint'} <= planned_steps
    receipt_counts = [row for row in observations if row['prune_step'] == 'plan-check-receipts']
    assert receipt_counts[0]['completed_items'] == 0
    assert receipt_counts[-1]['completed_items'] == receipt_counts[-1]['total_items'] == 2
    assert all(row['acceptance_proof'] is False for row in observations)
    assert all(row['total_items'] is None for row in observations
               if row['stage'] == 'scan-stage-metadata')
    result = apply_archive_stage_prune(spec, packed, expected_fingerprint=plan['stage_fingerprint'],
                                      owned_verified_plan=plan, progress=observations.append, **options)
    assert result['pruned'] is True
    assert {'apply-before-rename-fingerprint', 'apply-after-rename-fingerprint',
            'apply-verify-quarantine-archive', 'apply-final-stage-fingerprint',
            'apply-unlink-quarantine'} <= {row['prune_step'] for row in observations}
    assert all(row['acceptance_proof'] is False for row in observations)
    assert verify_cold_archive(spec, packed)['decoded_originals_verified']


def test_stage_prune_progress_does_not_turn_corruption_into_a_receipt(tmp_path, monkeypatch):
    spec, packed, options = fixture(tmp_path, monkeypatch, offline_d=True)
    stage = spec.stage_root / spec.dataset
    next((stage / 'receipts').iterdir()).write_text('{}')
    observations = []
    with pytest.raises(SnapshotError, match='per-file receipt differs'):
        plan_archive_stage_prune(spec, packed, progress=observations.append, **options)
    assert observations and all(row['acceptance_proof'] is False for row in observations)
    assert stage.is_dir() and not list(stage.parent.glob(f'.{spec.dataset}.pruning-*'))


def test_offline_d_scratch_uses_exact_cold_recovery_and_quarantine_before_prune(tmp_path, monkeypatch):
    spec, packed, options = fixture(tmp_path, monkeypatch, offline_d=True)
    plan = plan_archive_stage_prune(spec, packed, **options)
    result = apply_archive_stage_prune(spec, packed, expected_fingerprint=plan['stage_fingerprint'],
                                     owned_verified_plan=plan, **options)
    assert result['pruned'] and result['cold_objects_deleted'] == 0
    assert not (spec.stage_root / spec.dataset).exists()
    proof = verify_cold_archive(spec, packed)
    assert proof['cold_verified'] and proof['decoded_originals_verified'] and proof['files'] == 2


@pytest.mark.parametrize('failure', [None, 'different-bytes', 'unknown-receipt', 'payload-temp', 'changed-after-audit'])
def test_syncthing_control_temp_requires_exact_fixed_receipt_duplicate(tmp_path, monkeypatch, failure):
    spec, packed, options = fixture(tmp_path, monkeypatch)
    stage = spec.stage_root/spec.dataset
    canonical = next((stage/'receipts').iterdir())
    name = '.syncthing.'+canonical.stem+'.json.123.'+'a'*32+'.tmp'
    extra = stage/'receipts'/name
    extra.write_bytes(canonical.read_bytes())
    if failure=='different-bytes':
        extra.write_bytes(b'X'*extra.stat().st_size)
    elif failure=='unknown-receipt':
        extra.rename(extra.with_name('.syncthing.'+'f'*64+'.json.123.'+'a'*32+'.tmp'))
    elif failure=='payload-temp':
        extra.rename(stage/'archive'/'payload'/name)
    if failure in {'different-bytes','unknown-receipt','payload-temp'}:
        with pytest.raises(SnapshotError):
            plan_archive_stage_prune(spec,packed,**options)
        assert stage.exists()
        return
    plan=plan_archive_stage_prune(spec,packed,**options)
    assert plan['verified_duplicate_control_files']==[{
        'path':extra.relative_to(stage).as_posix(),
        'canonical_receipt':canonical.relative_to(stage).as_posix(),
        'sha256':module.sha256_file(canonical)}]
    if failure=='changed-after-audit':
        info=extra.stat();extra.write_bytes(b'X'*info.st_size)
        os.utime(extra,ns=(info.st_atime_ns,info.st_mtime_ns))
        with pytest.raises(SnapshotError,match='changed after dry run'):
            apply_archive_stage_prune(spec,packed,expected_fingerprint=plan['stage_fingerprint'],
                                      owned_verified_plan=plan,**options)
        assert stage.is_dir()
    else:
        result=apply_archive_stage_prune(spec,packed,expected_fingerprint=plan['stage_fingerprint'],
                                         owned_verified_plan=plan,**options)
        assert result['pruned'] and not stage.exists()
        proof=verify_cold_archive(spec,packed)
        assert proof['decoded_originals_verified'] and proof['files']==2


@pytest.mark.parametrize('benchmark_d', [False, True])
@pytest.mark.parametrize("gate", ["source", "state", "object", "unknown", "receipt", "pin", "process", "d-mount"])
def test_stage_prune_preserves_bytes_when_any_gate_fails(tmp_path, monkeypatch, gate, benchmark_d):
    spec, packed, options = fixture(tmp_path, monkeypatch, benchmark_d=benchmark_d)
    stage = spec.stage_root / spec.dataset
    if gate == "source":
        (options["artifact_root"] / spec.relative_root).mkdir()
    elif gate == "state":
        path = options["state_root"] / "retirements" / f"{spec.dataset}.json"
        state = json.loads(path.read_text())
        state["state"] = "hot-enrolled"
        path.write_text(json.dumps(state))
    elif gate == "object":
        release = resolve_latest_packed(packed, spec.dataset)
        (packed / release.manifest["archive"]["objects"][0]["relpath"]).write_bytes(b"corruption")
    elif gate == "unknown":
        (stage / "unique-unclassified-data").write_bytes(b"do not drop")
    elif gate == "receipt":
        next((stage / "receipts").iterdir()).write_text("{}")
    elif gate in {"pin", "process"}:
        import stockagent.data_sync.materialized_cache as cache
        if gate == "pin":
            release = resolve_latest_packed(packed, spec.dataset)
            monkeypatch.setattr(cache, "_pinned_snapshot_ids", lambda root: {release.manifest["snapshot_id"]})
        else:
            monkeypatch.setattr(cache, "process_references", lambda root: ["pid=123"])
    else:
        import stockagent.data_sync.cold_primary as primary
        def missing(root):
            raise SnapshotError("missing D")
        monkeypatch.setattr(primary, "_check_d_primary_mount", missing)
    with pytest.raises((SnapshotError, ValueError)):
        plan_archive_stage_prune(spec, packed, **options)
    assert stage.is_dir() and (stage / "archive/legacy_archive_manifest.json").is_file()


def test_stage_prune_rejects_a_changed_dry_run(tmp_path, monkeypatch):
    spec, packed, options = fixture(tmp_path, monkeypatch)
    plan = plan_archive_stage_prune(spec, packed, **options)
    receipt = next((spec.stage_root / spec.dataset / "receipts").iterdir())
    receipt.touch()
    with pytest.raises(SnapshotError, match="changed after dry run"):
        apply_archive_stage_prune(spec, packed, expected_fingerprint=plan["stage_fingerprint"], **options)
    assert (spec.stage_root / spec.dataset).is_dir()


def test_stage_prune_rejects_d_staging_and_non_catalog_roots(tmp_path, monkeypatch):
    spec, packed, options = fixture(tmp_path, monkeypatch)
    with pytest.raises(SnapshotError, match="restricted"):
        plan_archive_stage_prune(replace(spec, stage_root=tmp_path / "not-approved"), packed, **options)


def test_cold_only_cli_recovers_directly_when_stage_has_been_pruned(tmp_path, monkeypatch, capsys):
    import sys
    from scripts import manage_legacy_artifact_archives as cli
    spec, packed, options = fixture(tmp_path, monkeypatch)
    plan = plan_archive_stage_prune(spec, packed, **options)
    apply_archive_stage_prune(spec, packed, expected_fingerprint=plan["stage_fingerprint"], **options)
    monkeypatch.setattr(cli, "load_legacy_specs", lambda path: {spec.dataset: spec})
    monkeypatch.setattr(sys, "argv", ["manage_legacy_artifact_archives.py", "verify", spec.dataset,
                                     "--cold-only", "--sync-root", str(packed)])
    assert cli.main() == 0
    proof = json.loads(capsys.readouterr().out)
    assert proof["verification_scratch_removed"] and proof["cold_verified"]
    assert proof["source_comparison"] == "not_requested_cold_only"


@pytest.mark.parametrize('benchmark_d', [False, True])
@pytest.mark.parametrize("mutation", ["unknown-file", "cold-object"])
def test_post_rename_change_keeps_stage_quarantine(tmp_path, monkeypatch, mutation, benchmark_d):
    spec, packed, options = fixture(tmp_path, monkeypatch, benchmark_d=benchmark_d)
    plan = plan_archive_stage_prune(spec, packed, **options)
    import stockagent.data_sync.cold_primary as primary
    checks = []
    def mutate_after_rename(root):
        checks.append(root)
        if len(checks) == 2:
            quarantine = next(spec.stage_root.glob(f".{spec.dataset}.pruning-*"))
            if mutation == "unknown-file":
                (quarantine / "new-unique-data").write_bytes(b"keep")
            else:
                release = resolve_latest_packed(packed, spec.dataset)
                item = packed / release.manifest["archive"]["objects"][0]["relpath"]
                info = item.stat()
                item.write_bytes(b"x" * info.st_size)
    monkeypatch.setattr(primary, "_check_d_primary_mount", mutate_after_rename)
    with pytest.raises(SnapshotError):
        apply_archive_stage_prune(spec, packed, expected_fingerprint=plan["stage_fingerprint"], **options)
    quarantine = next(spec.stage_root.glob(f".{spec.dataset}.pruning-*"))
    assert (quarantine / "archive/legacy_archive_manifest.json").is_file()


def test_owned_stage_plan_reuses_full_d_proof_and_still_decodes_the_quarantine(tmp_path, monkeypatch):
    spec, packed, options = fixture(tmp_path, monkeypatch)
    calls = []
    verify = module.verify_packed_snapshot
    def counted(*a, **k):
        calls.append(k)
        return verify(*a, **k)
    monkeypatch.setattr(module, "verify_packed_snapshot", counted)
    plan = plan_archive_stage_prune(spec, packed, **options)
    assert len(calls) == 1
    result = apply_archive_stage_prune(spec, packed, expected_fingerprint=plan["stage_fingerprint"],
                                     owned_verified_plan=plan, **options)
    assert result["pruned"] and len(calls) == 1
    assert verify_cold_archive(spec, packed)["decoded_originals_verified"]


@pytest.mark.parametrize('failure', [None, 'cold-bytes', 'cold-generation', 'source', 'clock-backwards'])
def test_long_quarantine_decode_requires_new_complete_d_recovery(tmp_path, monkeypatch, failure):
    spec, packed, options = fixture(tmp_path, monkeypatch, benchmark_d=True)
    plan = plan_archive_stage_prune(spec, packed, **options)
    old_proof = dict(plan['full_verification'])
    clock = [old_proof['verified_at_epoch'] + 1]
    monkeypatch.setattr(module.time, 'time', lambda: clock[0])
    decode = module.verify_archive_directory
    calls = []
    def slow_decode(*args, **kwargs):
        proof = decode(*args, **kwargs)
        clock[0] += -100 if failure == 'clock-backwards' else 1600
        if failure in {'cold-bytes', 'cold-generation'}:
            release = resolve_latest_packed(packed, spec.dataset)
            payload = packed / release.manifest['archive']['objects'][0]['relpath']
            saved = payload.stat()
            payload.write_bytes(b'X' * saved.st_size)
            os.utime(payload, ns=(saved.st_atime_ns, saved.st_mtime_ns))
        if failure == 'source':
            (options['artifact_root'] / spec.relative_root).mkdir()
        return proof
    verify = module.verify_packed_snapshot
    def full_recovery(*args, **kwargs):
        calls.append(kwargs)
        assert kwargs.get('reconstruct_all') is True
        return verify(*args, **kwargs)
    monkeypatch.setattr(module, 'verify_archive_directory', slow_decode)
    monkeypatch.setattr(module, 'verify_packed_snapshot', full_recovery)
    if failure is None:
        result = apply_archive_stage_prune(spec, packed, expected_fingerprint=plan['stage_fingerprint'],
                                           owned_verified_plan=plan, **options)
        assert result['pruned'] and len(calls) == 1
        assert result['prior_full_verification'] == old_proof
        assert result['full_verification']['verified_at_epoch'] > old_proof['verified_at_epoch']
        assert plan['full_verification'] == old_proof
    else:
        with pytest.raises(SnapshotError):
            apply_archive_stage_prune(spec, packed, expected_fingerprint=plan['stage_fingerprint'],
                                      owned_verified_plan=plan, **options)
        assert list(spec.stage_root.glob(f'.{spec.dataset}.pruning-*'))


@pytest.mark.parametrize('failure', [None, 'modified', 'source', 'active-owner', 'multiple', 'cold'])
def test_interrupted_stage_quarantine_restores_only_exact_identity_for_new_audit(tmp_path, monkeypatch, failure):
    spec, packed, options = fixture(tmp_path, monkeypatch, benchmark_d=True)
    plan = plan_archive_stage_prune(spec, packed, **options)
    stage = spec.stage_root / spec.dataset
    pid = os.getpid() if failure == 'active-owner' else 999999999
    quarantine = stage.with_name(f'.{spec.dataset}.pruning-{pid}')
    stage.rename(quarantine)
    plan['full_verification']['verified_at_epoch'] -= 2000
    if failure == 'modified':
        (quarantine / 'unique').write_text('preserve')
    elif failure == 'source':
        (options['artifact_root'] / spec.relative_root).mkdir()
    elif failure == 'multiple':
        quarantine.with_name(f'.{spec.dataset}.pruning-999999998').mkdir()
    elif failure == 'cold':
        release = resolve_latest_packed(packed, spec.dataset)
        payload = packed / release.manifest['archive']['objects'][0]['relpath']
        payload.write_bytes(b'corrupt')
    journal = tmp_path / 'recovery.json'
    def recover():
        return module.recover_archive_stage_quarantine(spec, packed, owned_failed_plan=plan,
                                                       journal=journal, **options)
    if failure is None:
        result = recover()
        assert not result['deletion_authorized'] and not result['proof_time_extended']
        assert result['state'] == 'stage_restored_requires_full_audit'
        assert stage.is_dir() and not quarantine.exists()
        with pytest.raises(SnapshotError, match='expired'):
            apply_archive_stage_prune(spec, packed, expected_fingerprint=plan['stage_fingerprint'],
                                      owned_verified_plan=plan, **options)
        fresh = plan_archive_stage_prune(spec, packed, **options)
        assert apply_archive_stage_prune(spec, packed, expected_fingerprint=fresh['stage_fingerprint'],
                                          owned_verified_plan=fresh, **options)['pruned']
    else:
        with pytest.raises(SnapshotError):
            recover()
        assert quarantine.is_dir() and not stage.exists()


def test_canonical_finisher_resumes_expired_stage_quarantine_without_recreating_sources(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scripts import retire_local_offline_artifacts as cleanup
    spec, packed, options = fixture(tmp_path, monkeypatch, offline_d=True)
    target = tmp_path / 'retirement-receipts'
    target.mkdir()
    plan = plan_archive_stage_prune(spec, packed, **options)
    (target / 'stage-prune-plan.json').write_text(json.dumps(plan))
    stage = spec.stage_root / spec.dataset
    stage.rename(stage.with_name(f'.{spec.dataset}.pruning-999999999'))
    monkeypatch.setattr(cleanup, 'source_layout', lambda _: (
        options['artifact_root'], tmp_path / 'mirror', options['state_root'], tmp_path / 'activations'))
    retired = {'deleted': True, 'snapshot_id': plan['snapshot_id'],
               'manifest_sha256': plan['manifest_sha256'], 'reclaimed_allocated_file_bytes': 1234}
    accepted = cleanup.finish_retirement(spec, SimpleNamespace(sync_root=packed), target, retired,
        fixed_d_stage_only=True, expected_fixed_manifest_sha256=plan['manifest_sha256'])
    assert accepted['retired'] is True and accepted['cold_objects_deleted'] == 0
    progress = json.loads((target / 'stage-prune-progress.json').read_text())
    assert progress['state'] == 'completed' and progress['prune_step'] == 'stage-prune-receipt-written'
    assert progress['acceptance_proof'] is False and (target / 'stage-pruned.json').is_file()
    phases = json.loads((target / 'phase-history.json').read_text())['events']
    assert {'plan-encoding-stage-prune', 'apply-encoding-stage-prune'} <= {row['phase'] for row in phases}
    assert not stage.exists() and not list(spec.stage_root.glob(f'.{spec.dataset}.pruning-*'))
    assert list(target.glob('stage-quarantine-recovery-*.json'))
    assert verify_cold_archive(spec, packed)['decoded_originals_verified']


def test_canonical_finisher_reports_prune_failure_without_acceptance(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scripts import retire_local_offline_artifacts as cleanup
    spec, packed, options = fixture(tmp_path, monkeypatch, offline_d=True)
    target = tmp_path / 'retirement-receipts'
    target.mkdir()
    release = resolve_latest_packed(packed, spec.dataset)
    stage = spec.stage_root / spec.dataset
    next((stage / 'receipts').iterdir()).write_text('{}')
    monkeypatch.setattr(cleanup, 'source_layout', lambda _: (
        options['artifact_root'], tmp_path / 'mirror', options['state_root'], tmp_path / 'activations'))
    retired = {'deleted': True, 'snapshot_id': release.manifest['snapshot_id'],
               'manifest_sha256': release.manifest_sha256, 'reclaimed_allocated_file_bytes': 1234}
    with pytest.raises(SnapshotError, match='per-file receipt differs'):
        cleanup.finish_retirement(spec, SimpleNamespace(sync_root=packed), target, retired,
            fixed_d_stage_only=True, expected_fixed_manifest_sha256=release.manifest_sha256)
    progress = json.loads((target / 'stage-prune-progress.json').read_text())
    assert progress['state'] == 'failed' and progress['acceptance_proof'] is False
    assert stage.is_dir() and not (target / 'stage-pruned.json').exists()
    assert not (target / 'acceptance.json').exists()


@pytest.mark.parametrize('benchmark_d', [False, True])
@pytest.mark.parametrize("gate", ["expired", "future", "proof-contract", "cold-object", "source",
                                   "state", "pin", "process", "redirected", "unknown-file", "local-bytes"])
def test_owned_stage_proof_never_bypasses_fresh_retirement_or_content_gates(tmp_path, monkeypatch, gate, benchmark_d):
    spec, packed, options = fixture(tmp_path, monkeypatch, benchmark_d=benchmark_d)
    plan = plan_archive_stage_prune(spec, packed, **options)
    stage = spec.stage_root / spec.dataset
    if gate in {"expired", "future"}:
        plan["full_verification"]["verified_at_epoch"] = time.time() + (10 if gate == "future" else -1501)
    elif gate == "proof-contract":
        plan["full_verification"]["contract"] = "stat-only"
    elif gate == "cold-object":
        release = resolve_latest_packed(packed, spec.dataset)
        item = packed / release.manifest["archive"]["objects"][0]["relpath"]
        observed = item.stat()
        item.write_bytes(b"X" * observed.st_size)
        os.utime(item, ns=(observed.st_atime_ns, observed.st_mtime_ns))
    elif gate == "source":
        (options["artifact_root"] / spec.relative_root).mkdir()
    elif gate == "state":
        path = options["state_root"] / "retirements" / f"{spec.dataset}.json"
        value = json.loads(path.read_text()); value["state"] = "retiring"
        path.write_text(json.dumps(value))
    elif gate in {"pin", "process"}:
        import stockagent.data_sync.materialized_cache as cache
        if gate == "pin":
            monkeypatch.setattr(cache, "_pinned_snapshot_ids", lambda root: {plan["snapshot_id"]})
        else:
            monkeypatch.setattr(cache, "process_references", lambda root: ["pid=123"])
    elif gate == "redirected":
        saved = stage.with_name("redirected-stage-kept")
        stage.rename(saved); stage.symlink_to(saved, target_is_directory=True)
    elif gate == "unknown-file":
        (stage / "unknown-source").write_bytes(b"preserve")
    else:
        # Change bytes only after the before-rename signature check. A fresh
        # full decode must still catch them, even with the original mtime.
        rename = module.os.replace
        def mutate_after_rename(source, target):
            rename(source, target)
            if source == stage:
                file = next((Path(target) / "archive/payload").rglob("*.raw"))
                observed = file.stat(); file.write_bytes(b"X" * observed.st_size)
                os.utime(file, ns=(observed.st_atime_ns, observed.st_mtime_ns))
        monkeypatch.setattr(module.os, "replace", mutate_after_rename)
    with pytest.raises((SnapshotError, ValueError)):
        apply_archive_stage_prune(spec, packed, expected_fingerprint=plan["stage_fingerprint"],
                                 owned_verified_plan=plan, **options)
    assert stage.exists() or list(spec.stage_root.glob(f".{spec.dataset}.pruning-*"))

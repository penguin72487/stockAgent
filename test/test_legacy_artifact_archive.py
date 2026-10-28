from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
import json
import os
import time
from pathlib import Path

import pytest

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.legacy_artifact_archive import (
    LegacyArchiveSpec,
    prepare_archive,
    publish_archive,
    restore_archive,
    verify_archive_directory,
)
from stockagent.data_sync.packed_snapshots import resolve_latest_packed
import stockagent.data_sync.legacy_artifact_archive as archive_module


def _fixture(tmp_path: Path) -> tuple[LegacyArchiveSpec, Path, Path]:
    artifact_root = tmp_path / "artifacts"
    source = artifact_root / "markets" / "crypto"
    (source / "fold_01").mkdir(parents=True)
    (source / "fold_01" / "holdings.csv").write_bytes(b"date,symbol,weight\n" * 500_000)
    (source / "fold_01" / "checkpoint_best.pt").write_bytes(b"legacy incomplete checkpoint")
    old = time.time() - 10 * 86_400
    for path in source.rglob("*"):
        if path.is_file():
            os.utime(path, (old, old))
    spec = LegacyArchiveSpec("legacy-artifact-markets-crypto", "markets/crypto", 7, tmp_path / "stage")
    return spec, artifact_root, source


def test_encoded_tree_census_is_complete_with_bounded_parallel_metadata(tmp_path, monkeypatch):
    root = tmp_path / 'archive'
    (root / 'nested').mkdir(parents=True)
    for index in range(140):
        (root / 'nested' / str(index)).write_text(str(index))
    expected = {p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file()}
    assert archive_module._archive_encoded_census(root, file_workers=1) == expected
    assert archive_module._archive_encoded_census(root, file_workers=4) == expected
    assert archive_module._archive_encoded_census(root, file_workers=8) == expected
    (root / 'unexpected-link').symlink_to(root / 'nested', target_is_directory=True)
    with pytest.raises(SnapshotError, match='unsupported entry'):
        archive_module._archive_encoded_census(root, file_workers=8)
    (root / 'unexpected-link').unlink()
    original_scandir = os.scandir
    def denied(path):
        if Path(path) == root / 'nested':
            raise PermissionError('unreadable archive')
        return original_scandir(path)
    monkeypatch.setattr(os, 'scandir', denied)
    with pytest.raises(PermissionError, match='unreadable archive'):
        archive_module._archive_encoded_census(root, file_workers=4)


def test_encoded_tree_census_progress_is_observation_not_acceptance(tmp_path):
    root = tmp_path / 'archive'
    root.mkdir()
    (root / 'a').write_bytes(b'a')
    (root / 'b').write_bytes(b'b')
    events = []
    observed = archive_module._archive_encoded_census(
        root, file_workers=1, progress=events.append, expected_items=1)
    assert observed == {'a', 'b'}
    assert events[0]['completed_items'] == 0 and events[0]['total_items'] == 1
    assert events[-1]['completed_items'] == 2 and events[-1]['total_items'] is None
    assert all(event['stage'] == 'scan-encoded-tree' and event['metadata_only']
               and not event['acceptance_proof'] for event in events)


@pytest.mark.parametrize(('soft_limit', 'expected'), [
    (archive_module.resource.RLIM_INFINITY, 256),
    (1024, 252),
    (128, 28),
    (16, 2),
])
def test_parallel_archive_directory_budget_preserves_fd_headroom(monkeypatch, soft_limit, expected):
    monkeypatch.setattr(archive_module.resource, 'getrlimit', lambda _which: (soft_limit, soft_limit))
    assert archive_module._archive_directory_budget(4) == expected
    assert archive_module._archive_directory_budget(1) == 64


def test_legacy_archive_roundtrip_is_non_deployable_and_exact(tmp_path: Path) -> None:
    spec, artifact_root, source = _fixture(tmp_path)
    archive = spec.stage_root / spec.dataset / "archive"
    prepared = prepare_archive(spec, artifact_root)
    assert prepared["deployable"] is False
    assert sorted(row["codec"] for row in prepared["files"]) == ["gzip", "raw"]
    assert prepare_archive(spec, artifact_root) == prepared
    assert verify_archive_directory(archive, source)["files"] == 2
    sync_root = tmp_path / "packed"
    release = publish_archive(spec, artifact_root, sync_root, repo_root=tmp_path)
    repeated = publish_archive(spec, artifact_root, sync_root, repo_root=tmp_path)
    assert repeated["snapshot_id"] == release["snapshot_id"]
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    assert resolved.manifest["metadata"]["transport_role"] == "legacy-quarantine-archive"
    assert resolved.manifest["metadata"]["deployable"] == "false"
    assert release["source_files"] == 2
    destination = tmp_path / "restored"
    restore_archive(spec, sync_root, destination, materialized_root=tmp_path / "restore-cache")
    for original in source.rglob("*"):
        if original.is_file():
            assert (destination / original.relative_to(source)).read_bytes() == original.read_bytes()
    assert json.loads((destination / ".LEGACY_RESTORED.json").read_text())["deployable"] is False
    with pytest.raises(SnapshotError, match="overwrite"):
        restore_archive(spec, sync_root, destination, materialized_root=tmp_path / "restore-cache")


@pytest.mark.parametrize('manual_capture', [False, True])
def test_publication_uses_the_admitted_verifier_for_both_source_fences(tmp_path, monkeypatch, manual_capture):
    spec, artifact_root, source = _fixture(tmp_path)
    if manual_capture:
        spec = replace(spec, manual_capture_min_stable_hours=12)
    # Stand in for the enrolled D scratch without accessing the real drive.
    monkeypatch.setattr(archive_module, '_d_stage_file_workers', lambda *_: 4)
    verifier = archive_module.verify_archive_directory
    fences = []
    def record_fence(*args, **kwargs):
        fences.append(kwargs.get('file_workers', 1))
        return verifier(*args, **kwargs)
    monkeypatch.setattr(archive_module, 'verify_archive_directory', record_fence)
    release = publish_archive(spec, artifact_root, tmp_path / 'packed', repo_root=tmp_path,
                              manual_capture=manual_capture)
    assert fences == ([4, 4] if manual_capture else [1, 1])
    assert release['source_files'] == 2
    assert (source / 'fold_01/checkpoint_best.pt').read_bytes() == b'legacy incomplete checkpoint'


def test_parallel_publication_rejects_source_change_after_cold_commit(tmp_path, monkeypatch):
    spec, artifact_root, source = _fixture(tmp_path)
    spec = replace(spec, manual_capture_min_stable_hours=12)
    monkeypatch.setattr(archive_module, '_d_stage_file_workers', lambda *_: 4)
    publish = archive_module.publish_packed_snapshot
    def mutate_after_commit(*args, **kwargs):
        resolved = publish(*args, **kwargs)
        (source / 'fold_01/checkpoint_best.pt').write_bytes(b'changed after commit')
        return resolved
    monkeypatch.setattr(archive_module, 'publish_packed_snapshot', mutate_after_commit)
    with pytest.raises(SnapshotError, match='changed|stable|signature|mismatch'):
        publish_archive(spec, artifact_root, tmp_path / 'packed', repo_root=tmp_path,
                        manual_capture=True)
    assert (source / 'fold_01/checkpoint_best.pt').read_bytes() == b'changed after commit'


def test_parallel_archive_verification_keeps_full_proof_and_rejects_corruption(tmp_path, monkeypatch):
    spec, artifact_root, source = _fixture(tmp_path)
    manifest = prepare_archive(spec, artifact_root)
    archive = spec.stage_root / spec.dataset / 'archive'
    census_workers = []
    census = archive_module._archive_encoded_census
    def record_census(*args, **kwargs):
        census_workers.append(kwargs['file_workers'])
        return census(*args, **kwargs)
    monkeypatch.setattr(archive_module, '_archive_encoded_census', record_census)
    serial = verify_archive_directory(archive, source, spec=spec, file_workers=1)
    # The real four-worker path is reserved for the exact enrolled D stage;
    # use a private test stage to exercise its semantics without touching D.
    monkeypatch.setattr(archive_module, '_d_stage_file_workers', lambda *_: 4)
    parallel = verify_archive_directory(archive, source, spec=spec, file_workers=4)
    assert parallel == serial
    assert census_workers == [1, 8]
    encoded = archive / manifest['files'][0]['encoded_path']
    encoded.write_bytes(encoded.read_bytes() + b'corruption')
    with pytest.raises(SnapshotError, match='encoded file differs'):
        verify_archive_directory(archive, source, spec=spec, file_workers=4)
    assert (source / 'fold_01/checkpoint_best.pt').read_bytes() == b'legacy incomplete checkpoint'


@pytest.mark.parametrize('change', ['manifest', 'encoded', 'directory', 'root'])
def test_archive_verification_rejects_named_generation_change_after_full_hash(tmp_path, monkeypatch, change):
    spec, artifact_root, source = _fixture(tmp_path)
    prepare_archive(spec, artifact_root)
    archive = spec.stage_root / spec.dataset / 'archive'
    full_hash = archive_module._encoded_original_hashes
    changed = False

    def hash_then_change(path, codec):
        nonlocal changed
        proof = full_hash(path, codec)
        if not changed:
            changed = True
            if change == 'manifest':
                manifest = archive / 'legacy_archive_manifest.json'
                manifest.write_bytes(b'\n' + manifest.read_bytes())
            elif change == 'encoded':
                replacement = path.with_name(path.name + '.replacement')
                replacement.write_bytes(path.read_bytes())
                replacement.replace(path)
            else:
                directory = path.parent if change == 'directory' else archive
                moved = tmp_path / 'preserved-original-directory'
                directory.rename(moved)
                directory.symlink_to(moved, target_is_directory=True)
        return proof

    monkeypatch.setattr(archive_module, '_encoded_original_hashes', hash_then_change)
    reason = {'manifest': 'manifest changed', 'encoded': 'encoded file changed',
              'directory': 'directory changed|encoded tree', 'root': 'directory changed'}[change]
    with pytest.raises(SnapshotError, match=reason):
        verify_archive_directory(archive, source)
    assert changed
    assert (source / 'fold_01/checkpoint_best.pt').read_bytes() == b'legacy incomplete checkpoint'


def test_archive_verification_does_not_follow_a_redirected_encoding_parent(tmp_path):
    spec, artifact_root, source = _fixture(tmp_path)
    manifest = prepare_archive(spec, artifact_root)
    archive = spec.stage_root / spec.dataset / 'archive'
    parent = (archive / manifest['files'][0]['encoded_path']).parent
    moved = tmp_path / 'preserved-encodings'
    parent.rename(moved)
    parent.symlink_to(moved, target_is_directory=True)
    with pytest.raises((SnapshotError, OSError)):
        verify_archive_directory(archive, source)
    assert (source / 'fold_01/checkpoint_best.pt').exists()


@pytest.mark.parametrize('mode', ['reference', 'candidate'])
def test_verifier_comparison_keeps_full_publish_recovery_and_restore(tmp_path, mode):
    from types import SimpleNamespace
    from scripts import benchmark_legacy_archive_workflow as benchmark
    spec, root, source = _fixture(tmp_path)
    implementation = archive_module.verify_archive_directory
    calls = []
    def reference(*args, **kwargs):
        calls.append(True)
        return implementation(*args, **kwargs)
    expected = {p.relative_to(source).as_posix(): {
        'sha256': benchmark.sha256_file(p), 'size': p.stat().st_size,
        'mode': p.stat().st_mode & 0o7777, 'mtime_ns': p.stat().st_mtime_ns,
    } for p in source.rglob('*') if p.is_file()}
    result = benchmark.complete_trial(spec, root, tmp_path/'cold', tmp_path/'restored',
        tmp_path/'materialized', expected, verifier_mode=mode,
        verifier_reference=SimpleNamespace(verify_archive_directory=reference))
    assert result['all_original_sha_mode_mtime_verified'] and result['cold_verified']
    assert result['files'] == 2
    assert (len(calls) >= 4) is (mode == 'reference')
    assert archive_module.verify_archive_directory is implementation


@pytest.mark.parametrize('mode', ['reference', 'candidate'])
def test_materialized_comparison_retains_full_owner_publish_recovery_restore_and_readback(tmp_path, mode):
    from types import SimpleNamespace
    from scripts import benchmark_legacy_archive_workflow as benchmark
    import stockagent.data_sync.packed_snapshots as packed
    spec, root, source = _fixture(tmp_path)
    implementation = packed._verify_materialized
    calls = []

    def reference(*args, **kwargs):
        calls.append(True)
        return implementation(*args, **kwargs)

    expected = {p.relative_to(source).as_posix(): {
        'sha256': benchmark.sha256_file(p), 'size': p.stat().st_size,
        'mode': p.stat().st_mode & 0o7777, 'mtime_ns': p.stat().st_mtime_ns,
    } for p in source.rglob('*') if p.is_file()}
    result = benchmark.complete_trial(spec, root, tmp_path / 'cold', tmp_path / 'restored',
        tmp_path / 'materialized', expected, materialized_mode=mode,
        materialized_reference=SimpleNamespace(_verify_materialized=reference))
    assert result['all_original_sha_mode_mtime_verified'] and result['cold_verified']
    assert result['files'] == 2
    assert bool(calls) is (mode == 'reference')
    assert packed._verify_materialized is implementation
    assert archive_module._verify_materialized is implementation


@pytest.mark.parametrize('other', ['--single-pass-decode', '--verifier-reference',
                                 '--materialized-verifier-reference', '--encoding-on-d'])
def test_staging_benchmark_requires_an_isolated_comparison(tmp_path, monkeypatch, other):
    from scripts import benchmark_legacy_archive_workflow as benchmark
    argv = ['benchmark', '--source', str(tmp_path), '--file', 'raw', '--compare-staging',
            '--output', str(tmp_path / 'proof.json'), other]
    if other.endswith('reference'):
        argv.append(str(tmp_path / 'old.py'))
    monkeypatch.setattr(benchmark.sys, 'argv', argv)
    monkeypatch.setattr(benchmark, '_check_d_primary_mount',
                        lambda _: pytest.fail('invalid comparison reached real storage'))
    with pytest.raises(SystemExit) as error:
        benchmark.main()
    assert error.value.code == 2


def test_staging_comparison_keeps_both_full_roundtrips_and_removes_only_trial_scratch(tmp_path, monkeypatch):
    import tempfile
    from scripts import benchmark_legacy_archive_workflow as benchmark
    _spec, _root, source = _fixture(tmp_path)
    output = tmp_path / 'proof.json'
    native_parent, d_parent = tmp_path / 'native', tmp_path / 'd'
    native_parent.mkdir()
    d_parent.mkdir()
    original_mkdtemp, original_usage = tempfile.mkdtemp, benchmark.shutil.disk_usage

    def isolated_temp(*, prefix, dir):
        parent = native_parent if str(dir) == '/var/lib' else d_parent
        return original_mkdtemp(prefix=prefix, dir=parent)

    def capacity(path):
        if str(path) == '/srv/stockagent-d-volume':
            return type(original_usage(tmp_path))(256 * 1024**3, 0, 256 * 1024**3)
        return original_usage(path)

    argv = ['benchmark', '--source', str(source), '--rounds', '1', '--compare-staging', '--output', str(output)]
    for path in sorted(source.rglob('*')):
        if path.is_file():
            argv.extend(['--file', path.relative_to(source).as_posix()])
    monkeypatch.setattr(benchmark.sys, 'argv', argv)
    monkeypatch.setattr(benchmark.tempfile, 'mkdtemp', isolated_temp)
    monkeypatch.setattr(benchmark.shutil, 'disk_usage', capacity)
    monkeypatch.setattr(benchmark, '_check_d_primary_mount', lambda _: None)
    monkeypatch.setattr(benchmark, 'admit_workspace', lambda *_: None)
    monkeypatch.setattr(benchmark, 'observe_node', lambda *_: {'scope': 'native-fixture-semantics-only'})
    benchmark.main()
    proof = json.loads(output.read_bytes())
    assert proof['comparison'] == 'encoding-stage-native-ssd-vs-D-v1'
    assert [(row['profile'], row['encoding_on_d']) for row in proof['runs']] == [
        ('d-stage', True), ('native-ssd-stage', False)]
    for row in proof['runs']:
        assert row['all_original_sha_mode_mtime_verified'] and row['cold_verified']
        assert row['files'] == 2 and row['owned_native_trial_scratch_removed'] and row['owned_d_scratch_removed']
        assert row['complete_workflow_including_all_trial_scratch_retirement_seconds'] >= row['complete_workflow_including_d_scratch_retirement_seconds']
    assert not list(native_parent.iterdir()) and not list(d_parent.iterdir())
    assert (source / 'fold_01/checkpoint_best.pt').read_bytes() == b'legacy incomplete checkpoint'


def test_real_publisher_holds_owner_only_for_canonical_commit(tmp_path, monkeypatch):
    spec, artifact_root, source = _fixture(tmp_path)
    prepare_archive(spec, artifact_root)
    events = []
    progress_events = []
    held = False
    verify_original = archive_module.verify_archive_directory
    publish_original = archive_module.publish_packed_snapshot
    verify_packed_original = archive_module.verify_packed_snapshot

    @contextmanager
    def owner():
        nonlocal held
        assert not held
        held = True
        events.append("acquire")
        try:
            yield
        finally:
            held = False
            events.append("release")

    def verify(*args, **kwargs):
        assert not held
        events.append("verify-originals")
        return verify_original(*args, **kwargs)

    def publish(*args, **kwargs):
        assert held
        events.append("publish")
        return publish_original(*args, **kwargs)

    def verify_packed(*args, **kwargs):
        assert not held
        events.append("verify-cold")
        return verify_packed_original(*args, **kwargs)

    monkeypatch.setattr(archive_module, "verify_archive_directory", verify)
    monkeypatch.setattr(archive_module, "publish_packed_snapshot", publish)
    monkeypatch.setattr(archive_module, "verify_packed_snapshot", verify_packed)
    result = publish_archive(spec, artifact_root, tmp_path / "packed", repo_root=tmp_path,
                             publication_owner=owner, publication_progress=progress_events.append)
    assert events == ["verify-originals", "acquire", "publish", "release",
                      "verify-cold", "verify-originals"]
    assert not held
    assert result["source_files"] == 2
    assert progress_events and all(event['acceptance'] is False for event in progress_events)
    assert any(event['stage'] == 'packing_changed_small_files' for event in progress_events)
    assert (source / "fold_01/checkpoint_best.pt").read_bytes() == b"legacy incomplete checkpoint"


def test_selected_restore_keeps_full_archive_verification_and_exact_members(tmp_path):
    spec, artifact_root, source = _fixture(tmp_path)
    sync_root = tmp_path / "packed"
    publish_archive(spec, artifact_root, sync_root, repo_root=tmp_path)
    destination = tmp_path / "selected"
    result = restore_archive(spec, sync_root, destination,
                             materialized_root=tmp_path / "restore-cache",
                             include_paths=["fold_01/checkpoint_best.pt"])
    assert result["archive_files_verified"] == 2
    assert result["files"] == 1
    assert result["restore_scope"] == "selected_exact_members"
    assert (destination / "fold_01/checkpoint_best.pt").read_bytes() == (source / "fold_01/checkpoint_best.pt").read_bytes()
    assert not (destination / "fold_01/holdings.csv").exists()
    for paths in ([], ["absent"], ["../escape"]):
        with pytest.raises(SnapshotError):
            restore_archive(spec, sync_root, tmp_path / "invalid",
                            materialized_root=tmp_path / "restore-cache", include_paths=paths)


def test_restore_can_pin_exact_snapshot_without_reading_current_head(tmp_path, monkeypatch):
    spec, artifact_root, source = _fixture(tmp_path)
    sync_root = tmp_path / "packed"
    release = publish_archive(spec, artifact_root, sync_root, repo_root=tmp_path)

    def moving_head(*args, **kwargs):
        raise AssertionError("a fixed recovery must not resolve the moving head")

    monkeypatch.setattr(archive_module, "resolve_latest_packed", moving_head)
    destination = tmp_path / "fixed-recovery"
    result = restore_archive(spec, sync_root, destination,
                             materialized_root=tmp_path / "restore-cache",
                             snapshot_id=release["snapshot_id"])
    assert result["snapshot_id"] == release["snapshot_id"]
    assert result["archive_files_verified"] == 2
    assert (destination / "fold_01/checkpoint_best.pt").read_bytes() == (source / "fold_01/checkpoint_best.pt").read_bytes()
    with pytest.raises((OSError, SnapshotError)):
        restore_archive(spec, sync_root, tmp_path / "absent-recovery",
                        materialized_root=tmp_path / "restore-cache", snapshot_id="absent-release")
    assert not (tmp_path / "absent-recovery").exists()


def test_adaptive_large_binary_compression_keeps_exact_recovery(tmp_path):
    spec, artifact_root, source = _fixture(tmp_path)
    data = b"uncompressed training tensor\0" * 400_000
    (source / "backtest.npz").write_bytes(data)
    os.utime(source / "backtest.npz", (1_000_000_000, 1_000_000_000))
    spec = replace(spec, compression_profile=archive_module.ADAPTIVE_COMPRESSION_PROFILE)
    result = publish_archive(spec, artifact_root, tmp_path / "cold", repo_root=tmp_path,
                             batch_directory_fsync=True)
    proof = archive_module.verify_cold_archive(spec, tmp_path / "cold")
    row = next(r for r in proof["manifest"]["files"] if r["path"] == "backtest.npz")
    assert row["codec"] == "gzip" and row["encoded_size"] < len(data) // 10
    restored = tmp_path / "restored"
    restore_archive(spec, tmp_path / "cold", restored, materialized_root=tmp_path / "restore-cache")
    assert (restored / "backtest.npz").read_bytes() == data
    assert result["cold_bytes"] < result["source_bytes"] // 10


def test_adaptive_incompressible_binary_falls_back_without_extra_trial(tmp_path):
    spec, artifact_root, source = _fixture(tmp_path)
    data = os.urandom(archive_module.COMPRESS_MIN_BYTES)
    (source / "already-compressed.parquet").write_bytes(data)
    os.utime(source / "already-compressed.parquet", (1_000_000_000, 1_000_000_000))
    spec = replace(spec, compression_profile=archive_module.ADAPTIVE_COMPRESSION_PROFILE)
    manifest = prepare_archive(spec, artifact_root)
    row = next(r for r in manifest["files"] if r["path"] == "already-compressed.parquet")
    assert row["codec"] == "raw" and row["encoded_size"] == len(data)
    assert not list((spec.stage_root / spec.dataset / "codec-trials").iterdir())
    assert verify_archive_directory(spec.stage_root / spec.dataset / "archive", source)["files"] == 3


def test_new_compression_profile_preserves_verified_previous_stage(tmp_path):
    spec, artifact_root, source = _fixture(tmp_path)
    (source / "old.npz").write_bytes(b"0" * archive_module.COMPRESS_MIN_BYTES)
    os.utime(source / "old.npz", (1_000_000_000, 1_000_000_000))
    old = prepare_archive(spec, artifact_root)
    prior = next(r for r in old["files"] if r["path"] == "old.npz")
    assert prior["codec"] == "raw"
    spec = replace(spec, compression_profile=archive_module.ADAPTIVE_COMPRESSION_PROFILE)
    new = prepare_archive(spec, artifact_root)
    assert new["files"] == old["files"]
    assert verify_archive_directory(spec.stage_root / spec.dataset / "archive", source)["files"] == 3


def test_adaptive_trial_source_mutation_keeps_source_and_trial(tmp_path, monkeypatch):
    spec, artifact_root, source = _fixture(tmp_path)
    path = source / "data.npz"
    path.write_bytes(b"0" * archive_module.COMPRESS_MIN_BYTES)
    os.utime(path, (1_000_000_000, 1_000_000_000))
    spec = replace(spec, compression_profile=archive_module.ADAPTIVE_COMPRESSION_PROFILE)
    encode = archive_module._encode_one

    def mutate(original, target, codec, **kwargs):
        digest = encode(original, target, codec, **kwargs)
        if original == path:
            path.write_bytes(b"changed during trial")
        return digest

    monkeypatch.setattr(archive_module, "_encode_one", mutate)
    with pytest.raises(SnapshotError, match="trial retained"):
        prepare_archive(spec, artifact_root)
    assert path.exists() and list((spec.stage_root / spec.dataset / "codec-trials").glob("*.gz"))
    assert not (spec.stage_root / spec.dataset / "archive/legacy_archive_manifest.json").exists()


def test_catalog_compression_profile_is_explicit_and_fail_closed(tmp_path):
    row = {"dataset": "legacy-test", "relative_root": "markets/example", "minimum_stable_days": 7,
           "archive_only": True, "compression": archive_module.ADAPTIVE_COMPRESSION_PROFILE,
           "stage_root": str(tmp_path / "stage")}
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"schema_version": 1, "authority_node_id": "penguin", "archives": [row]}))
    assert archive_module.load_legacy_specs(catalog)["legacy-test"].compression_profile == row["compression"]
    row["compression"] = "unknown-fast-no-verification"
    catalog.write_text(json.dumps({"schema_version": 1, "authority_node_id": "penguin", "archives": [row]}))
    with pytest.raises(SnapshotError, match="safety contract"):
        archive_module.load_legacy_specs(catalog)


def test_deferred_legacy_notification_keeps_durable_intent_and_exact_cold_recovery(tmp_path, monkeypatch):
    import stockagent.data_sync.syncthing_scan as scan
    spec, artifact_root, source = _fixture(tmp_path)
    packed = tmp_path / "packed"
    packed.mkdir()
    (packed / scan.D_PRIMARY_MARKER).write_text("{}")
    monkeypatch.setattr(scan, "CANONICAL_ROOT", packed)
    monkeypatch.setattr(scan, "_scan_pending", lambda *a, **kw: pytest.fail("publication did a network scan"))
    result = publish_archive(spec, artifact_root, packed, repo_root=tmp_path, defer_scan=True)
    pending = packed / ".local-state/scan-pending" / (spec.dataset + ".json")
    intent = json.loads(pending.read_text())
    assert intent["dataset"] == spec.dataset and intent["new_object_paths"]
    proof = archive_module.verify_cold_archive(spec, packed)
    assert proof["snapshot_id"] == result["snapshot_id"]
    assert proof["cold_verified"] and proof["decoded_originals_verified"]
    assert pending.exists() and (source / "fold_01/checkpoint_best.pt").exists()


def test_explicit_verification_workspace_recovers_when_default_tmp_is_too_small(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import stockagent.data_sync.training_return as training_return
    spec, artifact_root, source = _fixture(tmp_path)
    packed = tmp_path / "packed"
    publish_archive(spec, artifact_root, packed, repo_root=tmp_path)
    scratch = tmp_path / "native-verification"
    observed = []
    monkeypatch.setattr(archive_module.shutil, "disk_usage", lambda path: SimpleNamespace(free=0))
    with pytest.raises(SnapshotError, match="verification scratch lacks"):
        archive_module.verify_cold_archive(spec, packed)

    def admit(path, required):
        observed.append((path, required))
        return {"filesystem": "ext4", "physical_free_bytes": 280 * 1024**3,
                "required_bytes": required}

    monkeypatch.setattr(training_return, "admit_workspace", admit)
    proof = archive_module.verify_cold_archive(spec, packed, verification_root=scratch)
    assert proof["cold_verified"] and proof["decoded_originals_verified"]
    assert proof["files"] == 2 and proof["verification_scratch_removed"]
    assert observed[0][0] == scratch and observed[0][1] > 32 * 1024**3
    assert proof["verification_workspace"]["parent"] == str(scratch)
    assert list(scratch.iterdir()) == []
    assert (source / "fold_01/checkpoint_best.pt").exists()


def test_default_verification_scratch_checks_physical_backing_before_allocating(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import stockagent.data_sync.training_return as training_return

    spec, artifact_root, source = _fixture(tmp_path)
    packed = tmp_path / "packed"
    publish_archive(spec, artifact_root, packed, repo_root=tmp_path)
    # Simulate ext4 regardless of whether pytest's fixture lives on tmpfs.
    monkeypatch.setattr(archive_module.tempfile, "gettempdir", lambda: str(tmp_path))
    monkeypatch.setattr(archive_module, "_verification_filesystem", lambda path: "ext4")
    monkeypatch.setattr(archive_module.shutil, "disk_usage",
                        lambda path: SimpleNamespace(free=1024 * 1024**3))
    observed = []
    def reject(path, required):
        observed.append((path, required))
        raise SnapshotError("physical backing volume lacks capacity")
    monkeypatch.setattr(training_return, "admit_workspace", reject)
    monkeypatch.setattr(archive_module.tempfile, "mkdtemp",
                        lambda *a, **kw: pytest.fail("scratch allocated before physical admission"))
    with pytest.raises(SnapshotError, match="physical backing volume lacks capacity"):
        archive_module.verify_cold_archive(spec, packed)
    assert len(observed) == 1 and observed[0][1] > 32 * 1024**3
    assert (source / "fold_01/checkpoint_best.pt").exists()


def test_default_tmpfs_remains_available_without_claiming_disk_backing(tmp_path, monkeypatch):
    import stockagent.data_sync.training_return as training_return

    spec, artifact_root, source = _fixture(tmp_path)
    packed = tmp_path / "packed"
    publish_archive(spec, artifact_root, packed, repo_root=tmp_path)
    monkeypatch.setattr(archive_module, "_verification_filesystem", lambda path: "tmpfs")
    monkeypatch.setattr(training_return, "admit_workspace",
                        lambda *a: pytest.fail("tmpfs is not an ext4 workspace"))
    proof = archive_module.verify_cold_archive(spec, packed)
    assert proof["cold_verified"] and proof["decoded_originals_verified"]
    assert proof["verification_workspace"]["filesystem"] == "tmpfs"
    assert proof["verification_workspace"]["physical_backing_verified"] is False
    assert (source / "fold_01/checkpoint_best.pt").exists()


def test_explicit_verification_workspace_preserves_failed_reconstruction(tmp_path, monkeypatch):
    import stockagent.data_sync.training_return as training_return
    spec, artifact_root, source = _fixture(tmp_path)
    packed = tmp_path / "packed"
    publish_archive(spec, artifact_root, packed, repo_root=tmp_path)
    monkeypatch.setattr(training_return, "admit_workspace", lambda *a: {})
    original_fetch = archive_module.fetch_packed_snapshot

    def corrupt_reconstructed_manifest(cold, destination, resolved, **options):
        archive = original_fetch(cold, destination, resolved, **options)
        (archive / "legacy_archive_manifest.json").write_bytes(b"corrupted reconstruction")
        return archive

    monkeypatch.setattr(archive_module, "fetch_packed_snapshot", corrupt_reconstructed_manifest)
    scratch = tmp_path / "native-verification"
    with pytest.raises(SnapshotError, match="scratch retained"):
        archive_module.verify_cold_archive(spec, packed, verification_root=scratch)
    assert list(scratch.glob("stockagent-legacy-verify-*"))
    assert (source / "fold_01/checkpoint_best.pt").exists()


def test_full_cold_recovery_progress_preserves_all_three_independent_steps(tmp_path):
    spec, artifact_root, source = _fixture(tmp_path)
    packed = tmp_path / 'packed'
    published = publish_archive(spec, artifact_root, packed, repo_root=tmp_path)
    events = []
    proof = archive_module.verify_cold_archive(spec, packed, progress=events.append)
    assert proof['cold_verified'] and proof['decoded_originals_verified']
    assert proof['verification_scratch_removed']
    assert proof['snapshot_id'] == published['snapshot_id']
    assert proof['manifest_sha256'] == published['manifest_sha256']
    assert {event['recovery_step'] for event in events} == {
        'fetch-encoded-archive', 'decode-originals', 'independent-cold-reconstruction'}
    decoded = [event for event in events if event['stage'] == 'decode-originals']
    assert decoded[0]['completed_items'] == decoded[0]['completed_bytes'] == 0
    assert decoded[-1]['completed_items'] == decoded[-1]['total_items'] == proof['files']
    assert decoded[-1]['completed_bytes'] == decoded[-1]['total_bytes'] == proof['original_bytes']
    last = events[-1]
    assert last['recovery_step'] == 'independent-cold-reconstruction'
    assert last['completed_items'] == last['total_items']
    assert last['completed_bytes'] == last['total_bytes']
    assert (source / 'fold_01/checkpoint_best.pt').exists()


def test_existing_encoding_recovery_checks_cold_and_originals_without_second_tree(tmp_path, monkeypatch):
    spec, artifact_root, source = _fixture(tmp_path)
    packed = tmp_path / "packed"
    released = publish_archive(spec, artifact_root, packed, repo_root=tmp_path)
    encoding = spec.stage_root / spec.dataset / "archive"
    monkeypatch.setattr(archive_module, "fetch_packed_snapshot",
                        lambda *a, **kw: pytest.fail("allocated another complete recovery tree"))
    events = []
    proof = archive_module.verify_cold_archive(
        spec, packed, reuse_encoded_archive=encoding, progress=events.append)
    assert proof["snapshot_id"] == released["snapshot_id"]
    assert proof["cold_verified"] and proof["decoded_originals_verified"]
    assert proof["verification_scratch_created"] is False
    assert proof["cold_reconstruction"]["independently_reconstructed_files"] == 3
    assert proof["files"] == 2
    assert {row["recovery_step"] for row in events} == {
        "independent-cold-reconstruction", "decode-originals"}
    phases = [(row['recovery_step'], row['stage']) for row in events]
    pre_hash = ('independent-cold-reconstruction', 'verify-materialized-files')
    pre_tree = ('independent-cold-reconstruction', 'verify-materialized-tree')
    decode = ('decode-originals', 'decode-originals')
    census = ('decode-originals', 'scan-encoded-tree')
    post_hash = ('decode-originals', 'verify-materialized-post-decode-files')
    post_tree = ('decode-originals', 'verify-materialized-post-decode-tree')
    assert phases.index(pre_hash) < phases.index(pre_tree) < phases.index(decode) < phases.index(census) < phases.index(post_hash) < phases.index(post_tree)
    fixed = resolve_latest_packed(packed, spec.dataset).manifest['source']
    census_events = [row for row in events if (row['recovery_step'], row['stage']) == census]
    assert census_events[-1]['completed_items'] == census_events[-1]['total_items'] == fixed['files']
    assert census_events[-1]['metadata_only'] and not census_events[-1]['acceptance_proof']
    for name in (pre_hash, post_hash):
        hashes = [row for row in events if (row['recovery_step'], row['stage']) == name]
        assert hashes[0]['completed_items'] == 0
        assert hashes[-1]['completed_items'] == hashes[-1]['total_items'] == fixed['files']
        assert hashes[-1]['completed_bytes'] == hashes[-1]['total_bytes'] == fixed['logical_bytes']
    for name in (pre_tree, post_tree):
        trees = [row for row in events if (row['recovery_step'], row['stage']) == name]
        assert trees[0]['completed_items'] == 0 and not trees[0]['fingerprint_matched']
        assert trees[-1]['fingerprint_matched'] and not trees[-1]['acceptance_proof']
    assert encoding.is_dir() and source.is_dir()


def test_post_decode_changed_encoding_reports_partial_failure_without_retirement(tmp_path, monkeypatch):
    spec, artifact_root, source = _fixture(tmp_path)
    packed = tmp_path / 'packed'
    publish_archive(spec, artifact_root, packed, repo_root=tmp_path)
    encoding = spec.stage_root / spec.dataset / 'archive'
    payload = next((encoding / 'payload').rglob('*.raw'))
    original_decode = archive_module.verify_archive_directory

    def decode_then_change(*args, **kwargs):
        proof = original_decode(*args, **kwargs)
        raw = payload.read_bytes()
        payload.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
        return proof

    monkeypatch.setattr(archive_module, 'verify_archive_directory', decode_then_change)
    events = []
    with pytest.raises(SnapshotError, match='checksum mismatch'):
        archive_module.verify_cold_archive(spec, packed, reuse_encoded_archive=encoding, progress=events.append)
    assert any(e['stage'] == 'verify-materialized-tree' and e['fingerprint_matched'] for e in events)
    assert any(e['stage'] == 'verify-materialized-post-decode-files' for e in events)
    assert not any(e['stage'] == 'verify-materialized-post-decode-tree' for e in events)
    assert source.is_dir() and encoding.is_dir()


@pytest.mark.parametrize("damage", ["cold", "encoding", "changed_during_decode", "foreign_path"])
def test_existing_encoding_never_substitutes_for_exact_recovery(tmp_path, monkeypatch, damage):
    spec, artifact_root, source = _fixture(tmp_path)
    packed = tmp_path / "packed"
    publish_archive(spec, artifact_root, packed, repo_root=tmp_path)
    encoding = spec.stage_root / spec.dataset / "archive"
    resolved = resolve_latest_packed(packed, spec.dataset)
    if damage == "cold":
        item = resolved.manifest["archive"]["objects"][0]
        payload = packed / item["relpath"]
        raw = payload.read_bytes()
        payload.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
    elif damage == "foreign_path":
        foreign = tmp_path / "foreign"
        import shutil
        shutil.copytree(encoding, foreign)
        encoding = foreign
    else:
        payload = next((encoding / "payload").rglob("*.raw"))
        def mutate():
            raw = payload.read_bytes()
            payload.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
        if damage == "encoding":
            mutate()
        else:
            original = archive_module.verify_archive_directory
            def decode_then_mutate(*a, **kw):
                result = original(*a, **kw)
                mutate()
                return result
            monkeypatch.setattr(archive_module, "verify_archive_directory", decode_then_mutate)
    with pytest.raises(SnapshotError):
        archive_module.verify_cold_archive(spec, packed, reuse_encoded_archive=encoding)
    assert source.is_dir() and encoding.is_dir()


@pytest.mark.parametrize("redirect", [False, True])
def test_verification_workspace_rejects_cold_overlap_or_symlinks(tmp_path, redirect):
    spec, artifact_root, source = _fixture(tmp_path)
    packed = tmp_path / "packed"
    publish_archive(spec, artifact_root, packed, repo_root=tmp_path)
    scratch = packed / "verification"
    if redirect:
        scratch = tmp_path / "redirect"
        scratch.symlink_to(tmp_path / "unrelated")
    with pytest.raises(SnapshotError, match="redirected or overlaps"):
        archive_module.verify_cold_archive(spec, packed, verification_root=scratch)


def test_archive_restores_empty_directories_and_portable_metadata(tmp_path):
    spec, artifact_root, source = _fixture(tmp_path)
    empty = source / "unused" / "nested"
    empty.mkdir(parents=True)
    empty.chmod(0o750)
    os.utime(empty, ns=(1_000_000_000, 2_000_000_000))
    publish_archive(spec, artifact_root, tmp_path / "packed", repo_root=tmp_path)
    restored = tmp_path / "restored"
    restore_archive(spec, tmp_path / "packed", restored, materialized_root=tmp_path / "restore-cache")
    actual = restored / "unused/nested"
    assert actual.is_dir() and list(actual.iterdir()) == []
    assert actual.stat().st_mode & 0o7777 == 0o750
    assert actual.stat().st_mtime_ns == 2_000_000_000


def test_archive_accepts_allowlisted_ablation_scope_and_rejects_cache_scope(tmp_path):
    path = tmp_path / "catalog.json"
    row = {"dataset": "legacy-ablation", "relative_root": "ablations/example",
           "minimum_stable_days": 7, "archive_only": True, "compression": "gzip-1-csv-over-8m",
           "stage_root": str(tmp_path / "stage")}
    catalog = {"schema_version": 1, "authority_node_id": "penguin", "archives": [row]}
    path.write_text(json.dumps(catalog))
    assert archive_module.load_legacy_specs(path)["legacy-ablation"].relative_root == "ablations/example"
    row["relative_root"] = "cache/example"
    path.write_text(json.dumps(catalog))
    with pytest.raises(SnapshotError, match="scoped"):
        archive_module.load_legacy_specs(path)


def test_empty_directory_change_after_encoding_is_rejected(tmp_path):
    spec, artifact_root, source = _fixture(tmp_path)
    (source / "empty").mkdir()
    prepare_archive(spec, artifact_root)
    (source / "empty").rmdir()
    with pytest.raises(SnapshotError, match="directory inventory"):
        verify_archive_directory(spec.stage_root / spec.dataset / "archive", source)


def test_legacy_archive_rejects_source_change_after_staging(tmp_path: Path) -> None:
    spec, artifact_root, source = _fixture(tmp_path)
    prepare_archive(spec, artifact_root)
    (source / "fold_01" / "checkpoint_best.pt").write_bytes(b"changed")
    with pytest.raises(SnapshotError, match="stable|changed"):
        prepare_archive(spec, artifact_root)


def test_cli_cold_only_verifies_retired_archive_and_still_rejects_corrupt_objects(tmp_path, monkeypatch, capsys):
    import sys
    from scripts import manage_legacy_artifact_archives as cli

    spec, artifact_root, source = _fixture(tmp_path)
    sync_root = tmp_path / "packed"
    release = publish_archive(spec, artifact_root, sync_root, repo_root=tmp_path)
    source.rename(source.with_name("retired-test-source"))
    monkeypatch.setattr(cli, "load_legacy_specs", lambda _path: {spec.dataset: spec})
    args = ["manage_legacy_artifact_archives.py", "verify", spec.dataset,
            "--artifact-root", str(artifact_root), "--sync-root", str(sync_root)]
    monkeypatch.setattr(sys, "argv", args)
    assert cli.main() == 2
    assert "source file missing" in capsys.readouterr().err
    monkeypatch.setattr(sys, "argv", args + ["--cold-only"])
    assert cli.main() == 0
    proof = json.loads(capsys.readouterr().out)
    assert proof["snapshot_id"] == release["snapshot_id"]
    assert proof["cold_verified"] is proof["decoded_originals_verified"] is True
    assert proof["source_comparison"] == "not_requested_cold_only"
    assert proof["deployable"] is False
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    object_path = sync_root / resolved.manifest["archive"]["objects"][0]["relpath"]
    object_path.write_bytes(b"corrupt cold object")
    assert cli.main() == 2
    assert "mismatch" in capsys.readouterr().err


def test_cli_refuses_cold_only_option_for_publication(monkeypatch, capsys):
    import sys
    from scripts import manage_legacy_artifact_archives as cli

    monkeypatch.setattr(sys, "argv", ["manage_legacy_artifact_archives.py", "publish", "example", "--cold-only"])
    assert cli.main() == 2
    assert "requires verify" in capsys.readouterr().err


def test_guarded_stage_refuses_missing_d_mount_before_writes(tmp_path: Path, monkeypatch):
    import stockagent.data_sync.cold_primary as cold_primary

    spec, artifact_root, source = _fixture(tmp_path)
    spec = replace(spec, stage_root=Path("/srv/stockagent-d-volume/test-archive-stage"))
    def missing_mount(root):
        assert root == Path("/srv/stockagent-packed")
        raise SnapshotError("D cold primary mount check failed")
    monkeypatch.setattr(cold_primary, "_check_d_primary_mount", missing_mount)
    with pytest.raises(SnapshotError, match="mount check failed"):
        prepare_archive(spec, artifact_root)
    assert (source / "fold_01/checkpoint_best.pt").read_bytes() == b"legacy incomplete checkpoint"


def test_benchmark_d_scratch_keeps_the_full_source_budget_and_32_gib_reserve(tmp_path, monkeypatch):
    from test_local_offline_artifact_retirement import _completed_benchmark
    import stockagent.data_sync.cold_primary as primary
    spec, artifacts, original = _fixture(tmp_path)
    source = artifacts / 'benchmarks/suite/case'
    source.parent.mkdir(parents=True)
    original.rename(source)
    _completed_benchmark(source)
    stage = tmp_path / 'guarded-d-scratch'
    monkeypatch.setattr(archive_module, 'D_BENCHMARK_STAGE_ROOT', stage)
    spec = replace(spec, relative_root='benchmarks/suite/case', stage_root=stage,
                   capture_contract=archive_module.MANUAL_WSL_CAPTURE_CONTRACT,
                   manual_capture_min_stable_hours=12)
    guards = []
    monkeypatch.setattr(primary, '_check_d_primary_mount', lambda root: guards.append(root))
    monkeypatch.setattr(archive_module.shutil, 'disk_usage', lambda root:
                        type('Usage', (), {'free': 32 * 1024**3})())
    with pytest.raises(SnapshotError, match='source-size budget plus 32 GiB reserve'):
        prepare_archive(spec, artifacts, manual_capture=True)
    assert guards == [Path('/srv/stockagent-packed')]
    assert not stage.exists() and source.is_dir()


def test_benchmark_scratch_cannot_capture_an_arbitrary_legacy_d_source(tmp_path, monkeypatch):
    spec, artifacts, source = _fixture(tmp_path)
    stage = tmp_path / 'benchmark-only-scratch'
    monkeypatch.setattr(archive_module, 'D_BENCHMARK_STAGE_ROOT', stage)
    with pytest.raises(SnapshotError, match='completed-case scope'):
        prepare_archive(replace(spec, stage_root=stage), artifacts)
    assert not stage.exists() and source.is_dir()


def test_offline_d_scratch_retains_full_source_budget_and_32_gib(tmp_path, monkeypatch):
    import stockagent.data_sync.cold_primary as primary
    spec, artifacts, original = _fixture(tmp_path)
    source = artifacts / 'operations/closed-audit'
    source.parent.mkdir(parents=True)
    original.rename(source)
    stage = tmp_path / 'guarded-offline-d-scratch'
    monkeypatch.setattr(archive_module, 'D_OFFLINE_STAGE_ROOT', stage)
    spec = replace(spec, relative_root='operations/closed-audit', stage_root=stage,
                   capture_contract=archive_module.MANUAL_WSL_CAPTURE_CONTRACT,
                   manual_capture_min_stable_hours=12)
    guards = []
    monkeypatch.setattr(primary, '_check_d_primary_mount', lambda root: guards.append(root))
    monkeypatch.setattr(archive_module.shutil, 'disk_usage', lambda root:
                        type('Usage', (), {'free': 32 * 1024**3})())
    with pytest.raises(SnapshotError, match='source-size budget plus 32 GiB reserve'):
        prepare_archive(spec, artifacts, manual_capture=True)
    assert guards == [Path('/srv/stockagent-packed')]
    assert not stage.exists() and source.is_dir()


@pytest.mark.parametrize('problem', ['raw-source', 'automatic', 'short-stability'])
def test_offline_d_scratch_is_manual_reviewed_and_stable_only(tmp_path, monkeypatch, problem):
    spec, artifacts, original = _fixture(tmp_path)
    source = artifacts / 'operations/closed-audit'
    source.parent.mkdir(parents=True)
    original.rename(source)
    stage = tmp_path / 'guarded-offline-d-scratch'
    monkeypatch.setattr(archive_module, 'D_OFFLINE_STAGE_ROOT', stage)
    spec = replace(spec, relative_root='operations/closed-audit', stage_root=stage,
                   capture_contract=archive_module.MANUAL_WSL_CAPTURE_CONTRACT,
                   manual_capture_min_stable_hours=12)
    if problem == 'raw-source':
        spec = replace(spec, relative_root='data_finlab/parquet')
    elif problem == 'short-stability':
        spec = replace(spec, manual_capture_min_stable_hours=1)
    with pytest.raises(SnapshotError):
        prepare_archive(spec, artifacts, manual_capture=problem != 'automatic')
    assert not stage.exists() and source.is_dir()


def test_legacy_exact_recovery_accepts_hardlink_ctime_drift(tmp_path):
    spec, artifact_root, source = _fixture(tmp_path)
    payload = source / "fold_01/checkpoint_best.pt"
    alias = tmp_path / "verified-alias"
    os.link(payload, alias)
    prepared = prepare_archive(spec, artifact_root)
    before = payload.stat().st_ctime_ns
    alias.unlink()
    assert payload.stat().st_ctime_ns != before
    proof = verify_archive_directory(spec.stage_root / spec.dataset / "archive", source)
    drift = next(row for row in proof["source_metadata_drift"] if row["path"] == "fold_01/checkpoint_best.pt")
    assert drift["changed_observations"] == ["ctime_ns"]
    assert proof["manifest"] == prepared


def test_legacy_source_mutating_while_hashing_is_still_rejected(tmp_path, monkeypatch):
    spec, artifact_root, source = _fixture(tmp_path)
    prepare_archive(spec, artifact_root)
    payload = source / "fold_01/checkpoint_best.pt"
    sha = archive_module.sha256_file

    def mutate(path):
        digest = sha(path)
        if path == payload:
            info = path.stat()
            os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns))
        return digest

    monkeypatch.setattr(archive_module, "sha256_file", mutate)
    with pytest.raises(SnapshotError, match="source differs"):
        verify_archive_directory(spec.stage_root / spec.dataset / "archive", source)


@pytest.mark.parametrize("change", ["bytes", "mtime", "mode"])
def test_legacy_ctime_fix_never_ignores_recoverable_metadata_or_bytes(tmp_path, change):
    spec, artifact_root, source = _fixture(tmp_path)
    prepare_archive(spec, artifact_root)
    payload = source / "fold_01/checkpoint_best.pt"
    info = payload.stat()
    if change == "bytes":
        payload.write_bytes(b"x" * info.st_size)
        os.utime(payload, ns=(info.st_atime_ns, info.st_mtime_ns))
    elif change == "mtime":
        os.utime(payload, ns=(info.st_atime_ns, info.st_mtime_ns - 1))
    else:
        payload.chmod(0o400 if info.st_mode & 0o7777 == 0o600 else 0o600)
    with pytest.raises(SnapshotError, match="source differs"):
        verify_archive_directory(spec.stage_root / spec.dataset / "archive", source)


@pytest.mark.parametrize("codec", ["raw", "gzip", "zstd"])
@pytest.mark.parametrize("repeats", [0, 1_200_000], ids=["empty", "multi-block"])
def test_single_input_pass_preserves_both_complete_hashes(tmp_path, codec, repeats, monkeypatch):
    import hashlib
    payload = b"archive-proof\0" * repeats
    source, encoding = tmp_path / "source", tmp_path / "encoded"
    source.write_bytes(payload)
    archive_module._encode_one(source, encoding, codec)
    encoded_bytes = encoding.read_bytes()
    observed = []
    read = archive_module._HashingReader.read

    def observe(self, size=archive_module.COPY_CHUNK):
        block = read(self, size)
        observed.append(len(block))
        return block

    monkeypatch.setattr(archive_module._HashingReader, "read", observe)
    actual = archive_module._encoded_original_hashes(encoding, codec)
    assert actual == (hashlib.sha256(encoded_bytes).hexdigest(), len(encoded_bytes),
                      hashlib.sha256(payload).hexdigest(), len(payload))
    assert sum(observed) == len(encoded_bytes)  # Not two reads of the encoding.
    assert max(observed) <= archive_module.COPY_CHUNK
    assert archive_module._decode_hash(encoding, codec) == actual[2:]


@pytest.mark.parametrize("codec", ["gzip", "zstd"])
@pytest.mark.parametrize("damage", ["truncated", "corrupt", "trailing"])
def test_single_pass_rejects_invalid_complete_stream(tmp_path, codec, damage):
    source, encoding = tmp_path / "source", tmp_path / "encoded"
    source.write_bytes(b"must decode completely\0" * 100_000)
    archive_module._encode_one(source, encoding, codec)
    payload = encoding.read_bytes()
    if damage == "truncated":
        payload = payload[:-3]
    elif damage == "corrupt":
        payload = b"invalid compression header" + payload[20:]
    else:
        payload += b"not another valid compressed frame"
    encoding.write_bytes(payload)
    with pytest.raises(SnapshotError, match="decode failed"):
        archive_module._encoded_original_hashes(encoding, codec)


@pytest.mark.parametrize("codec", ["raw", "gzip", "zstd"])
@pytest.mark.parametrize("damage", ["replace", "mtime", "inplace"])
def test_single_pass_keeps_fd_and_path_generation_fences(tmp_path, codec, damage, monkeypatch):
    source, encoding = tmp_path / "source", tmp_path / "encoded"
    source.write_bytes(b"unchanged original" * 1000)
    archive_module._encode_one(source, encoding, codec)
    read = archive_module._HashingReader.read
    changed = False

    def mutate(self, size=archive_module.COPY_CHUNK):
        nonlocal changed
        block = read(self, size)
        if not changed and block:
            changed = True
            if damage == "replace":
                replacement = encoding.with_suffix(".replacement")
                replacement.write_bytes(encoding.read_bytes())
                os.replace(replacement, encoding)
            elif damage == "mtime":
                info = encoding.stat()
                os.utime(encoding, ns=(info.st_atime_ns, info.st_mtime_ns + 1))
            else:
                with encoding.open("ab") as output:
                    output.write(b"\0")
        return block

    monkeypatch.setattr(archive_module._HashingReader, "read", mutate)
    with pytest.raises(SnapshotError, match="changed during verification|decode failed"):
        archive_module._encoded_original_hashes(encoding, codec)


@pytest.mark.parametrize("kind", ["fifo", "directory", "symlink"])
def test_single_pass_never_blocks_on_nonregular_or_redirected_payload(tmp_path, kind):
    path = tmp_path / "encoded"
    if kind == "fifo":
        os.mkfifo(path)
    elif kind == "directory":
        path.mkdir()
    else:
        target = tmp_path / "foreign"
        target.write_bytes(b"foreign")
        path.symlink_to(target)
    with pytest.raises((SnapshotError, OSError)):
        archive_module._encoded_original_hashes(path, "raw")


def test_single_pass_zstd_failure_closes_feeder_and_all_owned_descriptors(tmp_path, monkeypatch):
    import threading
    source, encoding = tmp_path / "source", tmp_path / "encoded"
    source.write_bytes(os.urandom(2 * 1024**2))
    archive_module._encode_one(source, encoding, "zstd")
    before = {thread.ident for thread in threading.enumerate()}
    original_sha256 = archive_module.hashlib.sha256
    original_open = archive_module.os.open
    opened = []

    def observe_open(path, flags, *args, **kwargs):
        fd = original_open(path, flags, *args, **kwargs)
        if Path(path) == encoding:
            opened.append(fd)
        return fd

    class BrokenHash:
        def update(self, _block):
            raise ValueError("injected hash consumer failure")

    calls = 0

    def fail_original():
        nonlocal calls
        calls += 1
        return BrokenHash() if calls == 2 else original_sha256()

    monkeypatch.setattr(archive_module.hashlib, "sha256", fail_original)
    monkeypatch.setattr(archive_module.os, "open", observe_open)
    with pytest.raises(ValueError, match="injected hash consumer failure"):
        archive_module._encoded_original_hashes(encoding, "zstd")
    assert {thread.ident for thread in threading.enumerate()} == before
    assert len(opened) == 1
    with pytest.raises(OSError):
        os.fstat(opened[0])


@pytest.mark.parametrize("codec", ["gzip", "zstd"])
def test_single_pass_hashes_all_concatenated_members(tmp_path, codec):
    import hashlib
    parts = [b"first complete member\0" * 1024, b"second complete member\0" * 4096]
    encodings = []
    for index, payload in enumerate(parts):
        source, encoded = tmp_path / f"source-{index}", tmp_path / f"encoded-{index}"
        source.write_bytes(payload)
        archive_module._encode_one(source, encoded, codec)
        encodings.append(encoded.read_bytes())
    path = tmp_path / "concatenated"
    path.write_bytes(b"".join(encodings))
    result = archive_module._encoded_original_hashes(path, codec)
    assert result == (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_size,
                      hashlib.sha256(b"".join(parts)).hexdigest(), sum(map(len, parts)))


def test_single_pass_zstd_rejects_input_failure_even_after_complete_frame(tmp_path, monkeypatch):
    source, encoding = tmp_path / "source", tmp_path / "encoded"
    source.write_bytes(b"complete frame before IO failure" * 1024)
    archive_module._encode_one(source, encoding, "zstd")
    read = archive_module._HashingReader.read

    def fail_at_end(self, size=archive_module.COPY_CHUNK):
        block = read(self, size)
        if not block:
            raise OSError("injected encoded input failure")
        return block

    monkeypatch.setattr(archive_module._HashingReader, "read", fail_at_end)
    with pytest.raises(OSError, match="injected encoded input failure"):
        archive_module._encoded_original_hashes(encoding, "zstd")


@pytest.mark.parametrize('codec', ['raw', 'gzip', 'zstd'])
@pytest.mark.parametrize('failure', ['none', 'native-exit', 'input', 'replace-on-close', 'append-on-close'])
def test_native_completed_encoding_retains_hashes_exit_and_fd_fences(tmp_path, monkeypatch, codec, failure):
    from contextlib import contextmanager
    import hashlib
    import io
    from stockagent.data_sync import windows_cold_io as native
    stage = tmp_path/'completed-stage';stage.mkdir()
    source, encoding = tmp_path/'source', stage/'encoding'
    payload = b'original values and null bytes\0' * 4097
    source.write_bytes(payload)
    archive_module._encode_one(source, encoding, codec)
    encoded = encoding.read_bytes()
    monkeypatch.setattr(archive_module, 'D_BENCHMARK_STAGE_ROOT', stage)
    monkeypatch.setattr(archive_module, 'NATIVE_ENCODED_READ_MIN_BYTES', 1)
    calls, streams = [], []

    class Input(io.BytesIO):
        def read(self, size=-1):
            value = super().read(size)
            if not value and failure == 'input':
                raise OSError('native pipe input failed')
            return value

    @contextmanager
    def reader(path, *, expected_bytes):
        calls.append((path, expected_bytes))
        stream = Input(encoded);streams.append(stream)
        try:
            yield stream
            if failure == 'native-exit':
                raise SnapshotError('native process failed after full EOF')
            if failure == 'replace-on-close':
                other = stage/'replacement';other.write_bytes(encoded);os.replace(other, encoding)
            if failure == 'append-on-close':
                with encoding.open('ab') as output:output.write(b'\0')
        finally:
            stream.close()

    monkeypatch.setattr(native, 'binary_reader', reader)
    if failure == 'none':
        assert archive_module._encoded_original_hashes(encoding, codec) == (
            hashlib.sha256(encoded).hexdigest(), len(encoded),
            hashlib.sha256(payload).hexdigest(), len(payload))
    else:
        with pytest.raises((SnapshotError, OSError), match='native|changed during verification'):
            archive_module._encoded_original_hashes(encoding, codec)
    assert calls == [(encoding, len(encoded))]
    assert streams and all(stream.closed for stream in streams)


@pytest.mark.parametrize('scope', ['small-stage', 'large-unregistered'])
def test_native_encoding_does_not_start_for_small_or_unregistered_paths(tmp_path, monkeypatch, scope):
    from stockagent.data_sync import windows_cold_io as native
    path = tmp_path/'encoding';payload=b'keep local byte I/O';path.write_bytes(payload)
    monkeypatch.setattr(archive_module, 'D_BENCHMARK_STAGE_ROOT', tmp_path if scope=='small-stage' else tmp_path/'other')
    monkeypatch.setattr(archive_module, 'D_OFFLINE_STAGE_ROOT', tmp_path/'offline')
    monkeypatch.setattr(archive_module, 'NATIVE_ENCODED_READ_MIN_BYTES', len(payload)+1 if scope=='small-stage' else 1)
    def forbidden(*args, **kwargs):pytest.fail('this source does not select native stage input')
    monkeypatch.setattr(native, 'binary_reader', forbidden)
    assert archive_module._encoded_original_hashes(path, 'raw')[1::2] == (len(payload), len(payload))


def test_oversize_stage_encoding_keeps_unbounded_original_local_reader(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from stockagent.data_sync import windows_cold_io as native
    path=tmp_path/'encoding';payload=b'keep the original streaming reader';path.write_bytes(payload)
    monkeypatch.setattr(archive_module, 'D_BENCHMARK_STAGE_ROOT', tmp_path)
    def forbidden(*args, **kwargs):pytest.fail('native exact-member bound must not reject large legacy members')
    monkeypatch.setattr(native, 'binary_reader', forbidden)
    fd=os.open(path, os.O_RDONLY)
    try:
        with archive_module._encoded_input(path, fd, SimpleNamespace(st_size=8*1024**3+1)) as stream:
            assert stream.read()==payload
        os.fstat(fd)  # The caller still owns its generation-fence descriptor.
    finally:
        os.close(fd)

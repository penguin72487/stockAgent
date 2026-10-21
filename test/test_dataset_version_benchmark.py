import hashlib
import os
import time

import pytest

from scripts import benchmark_dataset_version_pipeline as benchmark


@pytest.mark.parametrize('variant', ['two-pass', 'single-pass'])
@pytest.mark.parametrize('codec', ['gzip-1-csv-over-8m', 'gzip-1-adaptive-over-8m', 'zstd-1-adaptive-over-8m'])
def test_legacy_decode_comparison_retains_whole_publication_and_recovery(tmp_path, variant, codec):
    from scripts import benchmark_legacy_archive_workflow as comparison
    from stockagent.data_sync import legacy_artifact_archive as archive
    root = tmp_path / 'artifacts'
    source = root / 'markets/measurement'
    source.mkdir(parents=True)
    payload = source / 'original.csv'
    payload.write_bytes(b'date,symbol,value\n' * 520_000)
    stamp = time.time() - 10 * 86400
    os.utime(payload, (stamp, stamp))
    info = payload.stat()
    expected = {'original.csv': {'size': info.st_size, 'sha256': comparison.sha256_file(payload),
                'mode': info.st_mode & 0o7777, 'mtime_ns': info.st_mtime_ns}}
    spec = archive.LegacyArchiveSpec('legacy-io-measurement', 'markets/measurement', 7,
                                    tmp_path / 'encoding', compression_profile=codec)
    implementation = archive._encoded_original_hashes
    result = comparison.complete_trial(spec, root, tmp_path / 'cold', tmp_path / 'restored',
                                       tmp_path / 'cache', expected, variant=variant)
    assert archive._encoded_original_hashes is implementation  # No lasting production monkeypatch.
    assert result['all_original_sha_mode_mtime_verified'] and result['cold_verified']
    assert result['files'] == 1 and result['logical_bytes'] == info.st_size
    assert result['complete_workflow_seconds'] >= sum(result[name] for name in
        ('publish_seconds', 'cold_decode_verify_seconds', 'restore_and_original_sha_seconds'))
    assert result['complete_workflow_cpu_seconds_including_children'] > 0
    assert comparison.sha256_file(payload) == expected['original.csv']['sha256']


@pytest.mark.parametrize('inline', [False, True])
def test_inline_benchmark_preserves_every_version_and_removes_only_own_fixture(tmp_path, inline):
    result = benchmark.inline_control_trial(inline=inline, count=4, cold_parent=tmp_path)
    assert result['full_originals_and_old_version_verified']
    assert result['owned_generated_fixture_removed']
    assert result['loose_control_copies_created'] == (0 if inline else 12)
    assert [row['stage'] for row in result['phases']] == [
        'baseline', 'semantic_noop', 'incremental', 'old_version_full_restore_readback']
    assert not list(tmp_path.iterdir())


def test_inline_benchmark_requires_a_fresh_output_and_cannot_mix_modes():
    for argv in (['--native-inline-controls'],
                 ['--native-inline-controls', '--native-reused-chunks']):
        with pytest.raises(SystemExit) as error:
            benchmark.main(argv)
        assert error.value.code == 2


@pytest.mark.parametrize('variant', ['retained', 'bounded'])
def test_descriptor_comparison_keeps_all_originals_and_old_versions(tmp_path, monkeypatch, variant):
    from scripts import benchmark_packed_metadata as comparison
    from stockagent.data_sync import packed_snapshots as packed
    # The CLI isolates variants in child processes. Retain that isolation here.
    monkeypatch.setattr(packed, '_open_source_entry', packed._open_source_entry)
    monkeypatch.setattr(packed, 'METADATA_MEMORY_ROWS', packed.METADATA_MEMORY_ROWS)
    monkeypatch.setattr(packed, 'MATERIALIZE_FSYNC_WORKERS', packed.MATERIALIZE_FSYNC_WORKERS)
    result = comparison.trial(10, variant, 8, source_descriptor_only=True)
    assert result['all_originals_and_old_version_verified']
    assert [phase['stage'] for phase in result['phases']] == [
        'baseline', 'semantic_noop', 'incremental', 'old_version_full_restore_and_readback']
    assert result['metadata_memory_rows'] == 8
    assert result['fsync_workers'] == 4


@pytest.mark.parametrize('reference', [True, False])
def test_benchmark_retains_complete_roundtrip_and_does_not_mutate_inputs(tmp_path, reference):
    paths, proofs = [], []
    for index in range(2):
        path = tmp_path / f'revision-{index}.parquet'
        payload = b'a' * (8 * 1024**2) + bytes([index]) * 1024
        path.write_bytes(payload)
        paths.append(path)
        proofs.append(benchmark.source_proof(path))
    result = benchmark.run_trial(paths, proofs, reference=reference)
    assert result['full_roundtrip_verified'] is True
    assert [p['stage'] for p in result['phases']] == ['baseline', 'incremental', 'semantic_noop']
    assert result['source_pre_hash_passes'] == (3 if reference else 0)
    for path, proof in zip(paths, proofs, strict=True):
        assert hashlib.sha256(path.read_bytes()).hexdigest() == proof['sha256']
        assert list(benchmark.signature(path)) == proof['signature']


@pytest.mark.parametrize('fused', [False, True])
def test_chunk_recovery_benchmark_retains_new_and_old_originals(tmp_path, fused):
    paths, proofs = [], []
    for index in range(2):
        path = tmp_path / f'revision-{index}.parquet'
        path.write_bytes(b'a' * (8 * 1024**2) + bytes([index]) * 1024)
        paths.append(path)
        proofs.append(benchmark.source_proof(path))
    result = benchmark.run_trial(paths, proofs, reference=False, fused_recovery=fused)
    assert result['variant'] == ('fused_chunk_recovery' if fused else 'two_pass_chunk_recovery')
    assert result['full_roundtrip_verified'] is True
    assert result['source_pre_hash_passes'] == 0
    assert all(bool(row['chunk_object_hash_fusion']) is fused
               for row in result['phases'] if 'full_verify_seconds' in row)
    for path, proof in zip(paths, proofs, strict=True):
        assert benchmark.sha256_file(path) == proof['sha256']
        assert list(benchmark.signature(path)) == proof['signature']


def test_chunk_recovery_benchmark_cannot_mix_incompatible_modes():
    with pytest.raises(SystemExit) as error:
        benchmark.main(['--chunk-recovery-fusion', '--native-inline-controls'])
    assert error.value.code == 2


@pytest.mark.parametrize('mode', ['--native-reused-chunks', '--native-inline-controls',
                                 '--chunk-recovery-fusion', '--evaluate-cdc'])
def test_native_stream_benchmark_cannot_mix_modes(mode):
    with pytest.raises(SystemExit) as error:
        benchmark.main(['--native-stream-batch', mode])
    assert error.value.code == 2


@pytest.mark.parametrize('batched', [False, True])
@pytest.mark.parametrize('portable_reference', [False, True])
def test_native_stream_benchmark_retains_originals_and_all_version_lifecycle(tmp_path, monkeypatch, batched, portable_reference):
    from stockagent.data_sync import cold_primary, windows_cold_io as native
    paths, proofs = [], []
    for i in range(2):
        path = tmp_path / f'input-{i}.parquet'
        path.write_bytes(b'a' * (8 * 1024**2) + bytes([i]) * 1024)
        paths.append(path)
        proofs.append(benchmark.source_proof(path))
    parent = tmp_path / 'isolated-cold'
    parent.mkdir()
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', lambda root: None)
    monkeypatch.setattr(native, 'hash_many', lambda paths: {path: benchmark.sha256_file(path) for path in paths})
    result = benchmark.run_trial(paths, proofs, reference=False, fused_recovery=True,
                                 native_stream_batch=batched, cold_parent=parent,
                                 portable_stream_reference=portable_reference)
    assert result['full_roundtrip_verified']
    assert result['variant'] == ('batched_native_stream' if batched else
                                 'portable_chunk_stream' if portable_reference else 'per_extent_native_stream')
    assert [phase['stage'] for phase in result['phases']] == ['baseline', 'incremental', 'semantic_noop']
    assert not list(parent.iterdir())
    assert all(benchmark.sha256_file(path) == proof['sha256'] for path, proof in zip(paths, proofs, strict=True))


def test_benchmark_rejects_tiny_or_redirected_sources(tmp_path):
    small = tmp_path / 'tiny.parquet'
    small.write_bytes(b'not a throughput benchmark')
    redirect = tmp_path / 'redirect.parquet'
    redirect.symlink_to(small)
    for path in (small, redirect):
        with pytest.raises(ValueError, match='real closed'):
            benchmark.source_proof(path)


@pytest.mark.parametrize('missing', ['candidate', 'candidate-sha256', 'reference', 'reference-sha256'])
def test_native_benchmark_requires_both_fixed_writer_identities(tmp_path, missing):
    options = {'reference': str(tmp_path / 'old.py'), 'reference-sha256': '0' * 64,
               'candidate': str(tmp_path / 'new.py'), 'candidate-sha256': '1' * 64}
    options.pop(missing)
    argv = ['--native-reused-chunks', '--native-trial', 'native']
    for name, value in options.items():
        argv.extend(['--' + name, value])
    with pytest.raises(SystemExit) as error:
        benchmark.main(argv)
    assert error.value.code == 2

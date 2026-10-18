import hashlib

import pytest

from scripts import benchmark_dataset_version_pipeline as benchmark


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


def test_benchmark_rejects_tiny_or_redirected_sources(tmp_path):
    small = tmp_path / 'tiny.parquet'
    small.write_bytes(b'not a throughput benchmark')
    redirect = tmp_path / 'redirect.parquet'
    redirect.symlink_to(small)
    for path in (small, redirect):
        with pytest.raises(ValueError, match='real closed'):
            benchmark.source_proof(path)

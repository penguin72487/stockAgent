import hashlib
import os
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader import parquet_integrity as integrity


def seed(root):
    path = root / 'sample.parquet'
    pq.write_table(pa.table({'value': [1., None, 0.]}), path)
    return path, {'parquet_path': path.name, 'rows': 3, 'parquet_size_bytes': path.stat().st_size,
                  'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def test_persisted_proof_avoids_rehash_in_a_fresh_cache(tmp_path, monkeypatch):
    _, receipt = seed(tmp_path)
    cache = {}
    assert integrity.parquet_receipt_error(tmp_path, receipt, verification_cache=cache) is None
    target = tmp_path / 'proof.json'
    assert integrity.write_verification_cache(target, cache)
    integrity._checked.cache_clear()  # No process-local result survives.
    def unexpected_hash(path):
        raise AssertionError('unchanged file was rehashed')
    monkeypatch.setattr(integrity, 'sha256_file', unexpected_hash)
    assert integrity.parquet_receipt_error(
        tmp_path, receipt, verification_cache=integrity.read_verification_cache(target)) is None


@pytest.mark.parametrize('damage', ['same_size', 'empty', 'receipt_rows', 'receipt_hash', 'symlink'])
def test_file_or_receipt_change_invalidates_persisted_proof(tmp_path, damage):
    path, receipt = seed(tmp_path)
    cache = {}
    assert integrity.parquet_receipt_error(tmp_path, receipt, verification_cache=cache) is None
    if damage == 'same_size':
        original = path.stat()
        raw = bytearray(path.read_bytes())
        raw[16] ^= 1
        path.write_bytes(raw)
        os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns))
    elif damage == 'empty':
        path.write_bytes(b'')
    elif damage == 'receipt_rows':
        receipt['rows'] += 1
    elif damage == 'receipt_hash':
        receipt['sha256'] = '0' * 64
    else:
        renamed = tmp_path / 'renamed.parquet'
        path.rename(renamed)
        path.symlink_to(renamed)
    assert integrity.parquet_receipt_error(tmp_path, receipt, verification_cache=cache) is not None


def test_daily_recheck_does_not_reuse_in_process_or_disk_hash_cache(tmp_path, monkeypatch):
    _, receipt = seed(tmp_path)
    cache = {}
    assert integrity.parquet_receipt_error(tmp_path, receipt, verification_cache=cache) is None
    now = time.time()
    monkeypatch.setattr(integrity.time, 'time', lambda: now + 86400)
    calls = []
    original = integrity.sha256_file
    monkeypatch.setattr(integrity, 'sha256_file', lambda path: (calls.append(path), original(path))[1])
    assert integrity.parquet_receipt_error(tmp_path, receipt, verification_cache=cache) is None
    assert len(calls) == 1


def test_footer_only_check_cannot_mint_hash_proof(tmp_path):
    _, receipt = seed(tmp_path)
    receipt['sha256'] = '0' * 64
    cache = {}
    assert integrity.parquet_receipt_error(tmp_path, receipt, verify_hash=False,
                                           verification_cache=cache) is None
    assert cache == {}
    assert integrity.parquet_receipt_error(tmp_path, receipt, verification_cache=cache) == 'parquet_hash_mismatch'
    assert cache == {}


def test_invalid_or_wrong_contract_cache_falls_back_to_verification(tmp_path):
    path = tmp_path / 'proof.json'
    for content in [b'broken', b'[]', b'{"contract_version": 0, "files": {"fake": true}}']:
        path.write_bytes(content)
        assert integrity.read_verification_cache(path) == {}


def test_read_only_finlab_check_does_not_persist_metadata(tmp_path, monkeypatch):
    from scripts import download_finlab_history as finlab
    from finlab import data
    import pandas as pd

    monkeypatch.setattr(data, 'get', lambda *a, **kw: pd.DataFrame({'2330': [1.]}))
    finlab.fetch_one('test:field', tmp_path)
    assert finlab.has_local_download('test:field', tmp_path)
    assert not (tmp_path / 'local_integrity_cache.json').exists()
    assert finlab.persist_local_integrity_cache(tmp_path)
    assert (tmp_path / 'local_integrity_cache.json').is_file()

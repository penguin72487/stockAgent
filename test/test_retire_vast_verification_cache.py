import io
import json
import os
from pathlib import Path

import pytest

from scripts import retire_vast_verification_cache as retirement
from stockagent.data_sync import pack_compute_cache, packed_verification
from test_packed_verification import pack_bytes


def make_cache(tmp_path):
    cache = pack_compute_cache.ComputeCache(tmp_path/'cache', packed_verification, 1024**3)
    body, item, _rows = pack_bytes({'source': b'exact original'})
    cache.store(io.BytesIO(body), item)
    return cache, item


def test_withdraws_only_exact_closed_cache_pair_and_preserves_unknown(tmp_path, monkeypatch):
    cache, item = make_cache(tmp_path)
    (cache.root/'unknown').write_bytes(b'keep')
    monkeypatch.setattr(packed_verification, 'inspect_pack', lambda *_a: pytest.fail('unexpected payload readback'))
    planned = retirement.retire(cache, tmp_path/'receipts')
    assert planned['planned_pairs'] == 1 and planned['apply'] is False
    result = retirement.retire(cache, tmp_path/'receipts', apply=True, references=lambda _paths: [])
    assert result['state'] == 'completed' and result['removed_pairs'] == 1
    assert result['source_retirement'] is False and result['training_data_removed'] is False
    assert not (cache.root/(item['sha256']+'.zip')).exists()
    assert not (cache.root/(item['sha256']+'.json')).exists()
    assert (cache.root/(item['sha256']+'.lock')).exists()
    assert (cache.root/'unknown').read_bytes() == b'keep'
    assert json.loads(next((tmp_path/'receipts').glob('*.json')).read_text())['state'] == 'completed'


def test_busy_key_and_live_reader_do_not_lose_cache_data(tmp_path):
    cache, item = make_cache(tmp_path)
    with cache.lock(item):
        result = retirement.retire(cache, tmp_path/'receipts', apply=True, references=lambda _paths: [])
    assert result['state'] == 'partial' and result['busy_or_changed'] == 1
    assert (cache.root/(item['sha256']+'.zip')).exists()
    with pytest.raises(RuntimeError, match='running process'):
        retirement.retire(cache, tmp_path/'receipts', apply=True, references=lambda _paths: ['reader'])
    assert (cache.root/(item['sha256']+'.zip')).exists()


def test_shared_payload_or_redirected_proof_is_preserved(tmp_path):
    cache, item = make_cache(tmp_path)
    payload = cache.root/(item['sha256']+'.zip')
    other = tmp_path/'other-name'
    os.link(payload, other)
    result = retirement.retire(cache, tmp_path/'receipts', apply=True, references=lambda _paths: [])
    assert result['state'] == 'partial' and result['rejected_entries'] == 1
    assert payload.exists() and other.exists()
    other.unlink()
    proof = cache.root/(item['sha256']+'.json')
    proof.rename(tmp_path/'proof')
    proof.symlink_to(tmp_path/'proof')
    result = retirement.retire(cache, tmp_path/'receipts', apply=True, references=lambda _paths: [])
    assert result['state'] == 'partial' and payload.exists() and proof.is_symlink()


def test_interrupted_pair_unlink_recovers_orphan_proof(tmp_path, monkeypatch):
    cache, item = make_cache(tmp_path)
    proof = cache.root/(item['sha256']+'.json')
    unlink = Path.unlink
    failed = False
    def interrupt(path, *args, **kwargs):
        nonlocal failed
        if path == proof and not failed:
            failed = True
            raise OSError('injected proof unlink failure')
        return unlink(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'unlink', interrupt)
    first = retirement.retire(cache, tmp_path/'receipts', apply=True, references=lambda _paths: [])
    assert first['state'] == 'partial' and proof.exists()
    assert not (cache.root/(item['sha256']+'.zip')).exists()
    second = retirement.retire(cache, tmp_path/'receipts', apply=True, references=lambda _paths: [])
    assert second['state'] == 'completed' and second['removed_orphan_proofs'] == 1
    assert not proof.exists()

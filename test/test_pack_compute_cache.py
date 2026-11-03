"""Speculative remote work must preserve the canonical inventory boundary."""
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid
import zipfile

import pytest

from stockagent.data_sync import packed_verification as worker
from stockagent.data_sync import pack_compute_cache as cache
from test_packed_verification import pack_bytes


def fixture(tmp_path):
    body, item, rows = pack_bytes({'empty': b'', '數值': b'-10,NaN,1.25\n'})
    return cache.ComputeCache(tmp_path/'cache', worker, 1024**3), body, item, rows


def test_unbound_originals_only_become_inventory_bound_after_exact_match(tmp_path):
    owner, body, item, rows = fixture(tmp_path)
    early = owner.store(io.BytesIO(body), item)
    assert early['inventory_bound'] is False
    assert early['originals_sha256'] == worker.commitment(rows)
    assert owner.lookup(item, rows)['inventory_bound'] is True
    wrong = [row[:] for row in rows]; wrong[1][2] = '0'*64
    with pytest.raises(ValueError, match='original pack SHA'):
        owner.lookup(item, wrong)


def test_matching_commitment_avoids_redecode_but_still_hashes_all_encoded_bytes(tmp_path, monkeypatch):
    owner, body, item, rows = fixture(tmp_path)
    owner.store(io.BytesIO(body), item)
    monkeypatch.setattr(worker, 'inspect_pack', lambda *a: pytest.fail('matching pack decoded twice'))
    assert owner.lookup(item, rows)['cache_hit'] is True
    payload = owner.root/(item['sha256']+'.zip')
    payload.write_bytes(body[:-1]+b'x')
    with pytest.raises(ValueError, match='full SHA'):
        owner.lookup(item, rows)


def test_subset_inventory_keeps_all_stored_crcs_and_exact_originals(tmp_path):
    owner, body, item, rows = fixture(tmp_path)
    owner.store(io.BytesIO(body), item)
    subset = {**item, 'member_selection': 'subset', 'stored_file_count': 2, 'file_count': 1}
    result = owner.lookup(subset, rows[:1])
    assert result['files'] == 1 and result['stored_members_crc_verified'] == 2


@pytest.mark.parametrize('defect', ['redirect_root', 'redirect_payload', 'redirect_proof', 'changed_implementation', 'capacity', 'truncated'])
def test_cache_refuses_unsafe_or_incomplete_reuse(tmp_path, monkeypatch, defect):
    owner, body, item, rows = fixture(tmp_path)
    if defect == 'capacity':
        from types import SimpleNamespace
        monkeypatch.setattr(cache.shutil, 'disk_usage', lambda _: SimpleNamespace(free=0))
        with pytest.raises(ValueError, match='physical capacity'):
            owner.store(io.BytesIO(body), item)
        return
    if defect == 'truncated':
        with pytest.raises(EOFError): owner.store(io.BytesIO(body[:-1]), item)
        assert not list(owner.root.glob('*.partial'))
        assert not list(owner.root.glob('*.zip'))
        return
    owner.store(io.BytesIO(body), item)
    if defect == 'redirect_root':
        moved = tmp_path/'moved'; owner.root.rename(moved); owner.root.symlink_to(moved, target_is_directory=True)
    elif defect in {'redirect_payload', 'redirect_proof'}:
        path = owner.root/(item['sha256']+('.zip' if defect.endswith('payload') else '.json'))
        target = tmp_path/'outside'; path.rename(target); path.symlink_to(target)
    else:
        control = owner.root/(item['sha256']+'.json'); proof = json.loads(control.read_text())
        proof['worker_sha256'] = '0'*64; control.write_text(json.dumps(proof))
        assert owner.lookup(item, rows) is None
        return
    with pytest.raises((ValueError, OSError)): owner.lookup(item, rows)


def test_cache_gc_only_retires_expired_owned_pairs_and_preserves_busy_unknown(tmp_path):
    owner, body, item, rows = fixture(tmp_path)
    owner.store(io.BytesIO(body), item)
    unknown = owner.root/'unknown'; unknown.write_bytes(b'keep')
    assert owner.gc(now=0)['removed_cache_pairs'] == 0
    with owner.lock(item):
        assert owner.gc(now=10**12)['removed_cache_pairs'] == 0
    assert owner.gc(now=10**12)['removed_cache_pairs'] == 1
    assert unknown.read_bytes() == b'keep'


def test_fixed_remote_rpc_stages_then_binds_without_uploading_again(tmp_path):
    code = cache.bootstrap(Path(worker.__file__).read_bytes(), str(tmp_path), 1024**3)
    session = worker.StreamSession([sys.executable, '-c', code], 10)
    body, item, rows = pack_bytes({'a': b'original'})
    args = {'manifest_sha256': 'a'*64, 'item': item, 'rows': rows, 'job_id': uuid.uuid4().hex,
            'boot_id': session.identity['boot_id'], 'worker_sha256': worker.SOURCE_SHA256, 'timeout_seconds': 10}
    try:
        assert session.request({**args, 'mode': 'cached_verify'})['state'] == 'cache_miss'
        early = session.request({**args, 'mode': 'stage'}, io.BytesIO(body))
        assert early['inventory_bound'] is False and early['stored_members_crc_verified'] == 1
        fixed = session.request({**args, 'mode': 'cached_verify'})
        assert fixed['inventory_bound'] is True and fixed['originals_sha256'] == worker.commitment(rows)
        assert fixed['cache_hit'] is True
    finally:
        session.close()


def test_native_eof_or_generation_failure_cannot_accept_a_remote_reply(tmp_path):
    code = cache.bootstrap(Path(worker.__file__).read_bytes(), str(tmp_path), 1024**3)
    session = worker.StreamSession([sys.executable, '-c', code], 10)
    body, item, rows = pack_bytes({'a': b'original'})
    args = {'mode': 'stage', 'manifest_sha256': 'a'*64, 'item': item, 'job_id': uuid.uuid4().hex,
            'boot_id': session.identity['boot_id'], 'worker_sha256': worker.SOURCE_SHA256, 'timeout_seconds': 10}
    def invalid_native_child():
        raise ValueError('native EOF/generation gate failed')
    try:
        with pytest.raises(ValueError, match='native EOF/generation'):
            session.request(args, io.BytesIO(body), on_payload_sent=invalid_native_child)
    finally:
        session.abort()

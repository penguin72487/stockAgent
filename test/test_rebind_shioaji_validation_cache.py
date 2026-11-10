from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import time

import pytest

from scripts import rebind_shioaji_validation_cache as rebind
from stockagent.data_sync import workspace_relocation
from stockagent.data_sync import source_validation
from stockagent.data_sync.desync_snapshots import SnapshotError, sha256_file
from stockagent.data_sync.source_validation import VerificationCache


def _fixed_copies(tmp_path):
    source = tmp_path/'source'
    source.mkdir()
    paths = [source/name for name in ('a.parquet', 'b.parquet', 'c.parquet')]
    for index, path in enumerate(paths):
        path.write_bytes(bytes([index])*32)
    cache = tmp_path/'cache.sqlite3'
    owner = VerificationCache(cache, 'validator-v1')
    for path in paths:
        owner.check(path, sha256_file(path), lambda: {'validated': True})
    owner.close()
    new_device = paths[0].stat().st_dev
    old_device = new_device + 1
    index = tmp_path/'old-index.sqlite'
    with sqlite3.connect(index) as db:
        db.execute('CREATE TABLE files (path TEXT PRIMARY KEY,kind TEXT,sig TEXT,sha TEXT,bytes INTEGER,state TEXT)')
        for path in paths:
            signature = workspace_relocation.signature(path)
            signature[0] = old_device
            db.execute('INSERT INTO files VALUES (?,?,?,?,?,?)',
                       (path.name, 'file', json.dumps(signature), sha256_file(path), path.stat().st_size, 'verified'))
    with sqlite3.connect(cache) as db:
        for path in paths:
            signature = json.loads(db.execute('SELECT signature FROM verified WHERE path=?', (str(path),)).fetchone()[0])
            signature[0] = old_device
            db.execute('UPDATE verified SET signature=? WHERE path=?', (json.dumps(signature), str(path)))
    return source, paths, cache, index, old_device, new_device


def test_rebind_reuses_only_prior_sha_and_exact_native_file_generation(tmp_path):
    source, paths, cache, index, old, new = _fixed_copies(tmp_path)
    with sqlite3.connect(index) as db:
        db.execute('UPDATE files SET sha=? WHERE path=?', ('0'*64, paths[2].name))
    expected = sha256_file(paths[1])
    paths[1].write_bytes(b'changed since the original copy')
    result = rebind._rebind_rows(cache, source, index, 'validator-v1', old, new, lambda: None,
                                  deadline=time.monotonic()+10, batch_size=2)
    assert result == {'state': 'completed', 'matched': 2, 'rebound': 2, 'raced': 0}
    owner = VerificationCache(cache, 'validator-v1')
    assert owner.check(paths[0], sha256_file(paths[0]), lambda: pytest.fail('previous proof should be reused'))
    with pytest.raises(SnapshotError, match='fingerprint mismatch'):
        owner.check(paths[1], expected, lambda: {})
    assert owner.check(paths[2], sha256_file(paths[2]), lambda: {'new_validation': True}) == {'new_validation': True}
    assert (owner.reused, owner.checked) == (1, 1)
    owner.close()


def test_live_sqlite_owner_is_not_forced_and_later_rebind_resumes(tmp_path):
    source, paths, cache, index, old, new = _fixed_copies(tmp_path)
    owner = sqlite3.connect(cache, timeout=0)
    owner.execute('BEGIN IMMEDIATE')
    try:
        result = rebind._rebind_rows(cache, source, index, 'validator-v1', old, new, lambda: None,
                                    deadline=time.monotonic()+2, batch_size=2)
        assert result['state'] == 'busy' and result['rebound'] == 0
    finally:
        owner.rollback()
        owner.close()
    result = rebind._rebind_rows(cache, source, index, 'validator-v1', old, new, lambda: None,
                                deadline=time.monotonic()+10, batch_size=2)
    assert result['state'] == 'completed' and result['rebound'] == len(paths)


def test_identity_guard_failure_rolls_back_the_current_batch(tmp_path):
    source, paths, cache, index, old, new = _fixed_copies(tmp_path)
    calls = 0
    def guard():
        nonlocal calls
        calls += 1
        if calls == 3:
            raise SnapshotError('donor changed')
    with pytest.raises(SnapshotError, match='donor changed'):
        rebind._rebind_rows(cache, source, index, 'validator-v1', old, new, guard,
                            deadline=time.monotonic()+10, batch_size=2)
    with sqlite3.connect(cache) as db:
        assert all(json.loads(row[0])[0] == old for row in db.execute('SELECT signature FROM verified'))


def test_scheduler_reuses_completed_generation_without_repeating_inventory(tmp_path, monkeypatch):
    source, paths, cache, index, old, new = _fixed_copies(tmp_path)
    recorded = workspace_relocation.signature(source)
    fixed_index = workspace_relocation.signature(index)
    monkeypatch.setattr(rebind, 'SOURCE', source)
    monkeypatch.setattr(rebind, 'CACHE', cache)
    monkeypatch.setattr(rebind, 'STATUS', tmp_path/'status.json')
    monkeypatch.setattr(rebind, '_fixed_binding', lambda: (index, recorded, (old, new), fixed_index))
    monkeypatch.setattr(rebind, '_source_mapping', lambda *_args: (old, new))
    monkeypatch.setattr(rebind.handoff, 'binance_handoff_window_allowed', lambda **_kwargs: True)
    monkeypatch.setattr(source_validation, 'validator_identity', lambda *_args: 'validator-v1')
    first = rebind.rebind(max_seconds=10)
    assert first['state'] == 'completed' and first['rebound'] == len(paths)
    saved = json.loads(rebind.STATUS.read_text())
    assert saved['cumulative_rebound'] == len(paths) and saved['payloads_read'] is False
    monkeypatch.setattr(rebind, '_rebind_rows', lambda *_a, **_k: pytest.fail('completed generation rescanned'))
    assert rebind.rebind(max_seconds=10)['state'] == 'already_completed'


def test_scheduler_yields_to_day_trade_window_before_touching_cache(tmp_path, monkeypatch):
    source, _, cache, index, old, new = _fixed_copies(tmp_path)
    recorded, fixed_index = workspace_relocation.signature(source), workspace_relocation.signature(index)
    monkeypatch.setattr(rebind, 'SOURCE', source)
    monkeypatch.setattr(rebind, 'CACHE', cache)
    monkeypatch.setattr(rebind, 'STATUS', tmp_path/'status.json')
    monkeypatch.setattr(rebind, '_fixed_binding', lambda: (index, recorded, (old, new), fixed_index))
    monkeypatch.setattr(rebind, '_source_mapping', lambda *_args: (old, new))
    monkeypatch.setattr(rebind.handoff, 'binance_handoff_window_allowed', lambda **_kwargs: False)
    with pytest.raises(BlockingIOError, match='tw_day_trade_priority_window'):
        rebind.rebind(max_seconds=10)
    with sqlite3.connect(cache) as db:
        assert all(json.loads(row[0])[0] == old for row in db.execute('SELECT signature FROM verified'))


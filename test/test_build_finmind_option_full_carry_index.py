from datetime import datetime
import hashlib
import json
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts import build_finmind_option_full_carry_index as builder


def fixture(tmp_path, monkeypatch):
    root = tmp_path / 'data_finmind/complement'
    root.mkdir(parents=True)
    index = tmp_path / 'data_finmind/research/source_events'
    index.mkdir(parents=True)
    output = tmp_path / 'data_finmind/research/full_carry'
    status = tmp_path / 'artifacts/markets/status.json'
    windows = []
    for day in ('2011-01-03', '2011-01-10'):
        pins = [{'date': day, 'planned': True}]
        window = {'start': day, 'end': day, 'source_days': 1, 'source_receipt_pins': pins,
                  'source_group_fingerprint': hashlib.sha256(json.dumps(
                      pins, sort_keys=True, separators=(',', ':')).encode()).hexdigest()}
        windows.append(window)
        parent = index / f'{day}_{day}'
        parent.mkdir()
        payload = parent / 'observed_events.parquet'
        pl.DataFrame([{'event_ts': datetime.fromisoformat(day + 'T08:40:00').replace(
                           tzinfo=ZoneInfo('Asia/Taipei')), 'product_raw': 'TXO',
                       'series_raw': '201103', 'strike_raw': '8000', 'right_raw': 'C',
                       'price_raw': '10', 'source_sha256': 'a' * 64,
                       'source_date': datetime.fromisoformat(day).date()}]).write_parquet(payload)
        (parent / 'manifest.json').write_text(json.dumps({'data_path': str(payload)}))
    plan = tmp_path / 'plan.json'
    plan.write_text(json.dumps({'windows': windows, 'not_planned_days': [{'date': '2011-01-05'}]}))
    monkeypatch.setattr(builder, 'identity', lambda path, value: {'contract': 'fixture', 'plan': sha256_file(path)})
    class Capacity:
        def __init__(self, _root):
            pass
        def initial_sample(self, timeout_seconds):
            return {'state': 'admitted'}
    monkeypatch.setattr(builder, 'AcquisitionCapacitySampler', Capacity)
    def verify(parent, _plan_path, _plan, _window, _root):
        manifest = parent / 'manifest.json'
        payload = Path(json.loads(manifest.read_text())['data_path'])
        return {'manifest_sha256': sha256_file(manifest), 'data_sha256': sha256_file(payload)}
    monkeypatch.setattr(builder, 'verify', verify)
    return root, plan, index, output, status, verify


def test_index_resumes_after_window_failure_and_rechecks_every_published_row(tmp_path, monkeypatch):
    root, plan, index, output, status, verify = fixture(tmp_path, monkeypatch)
    def failure(parent, *args):
        if parent.name.startswith('2011-01-10'):
            raise ValueError('simulated missing source')
        return verify(parent, *args)
    monkeypatch.setattr(builder, 'verify', failure)
    with pytest.raises(ValueError, match='simulated missing source'):
        builder.run(root, plan, index, output, status, max_wait_seconds=5)
    with sqlite3.connect(output / 'events.sqlite3') as conn:
        assert conn.execute('SELECT count(*) FROM windows').fetchone() == (1,)
        assert conn.execute('SELECT count(*) FROM events').fetchone() == (1,)
    monkeypatch.setattr(builder, 'verify', verify)
    result = builder.run(root, plan, index, output, status, max_wait_seconds=5)
    assert result['state'] == 'source_index_prefix_complete'
    assert result['verified_prefix_windows'] == 2 and result['events'] == 2
    assert result['training_ready'] is False and result['known_missing_source_days'] == [{'date': '2011-01-05'}]
    with sqlite3.connect(output / 'events.sqlite3') as conn:
        conn.execute("UPDATE events SET price='999' WHERE source_window=0")
    with pytest.raises(ValueError, match='checkpoint payload or readback changed'):
        builder.run(root, plan, index, output, status, max_wait_seconds=5)


def test_index_rejects_mutated_parent_on_resume(tmp_path, monkeypatch):
    root, plan, index, output, status, _ = fixture(tmp_path, monkeypatch)
    builder.run(root, plan, index, output, status, max_wait_seconds=5)
    parent = index / '2011-01-03_2011-01-03' / 'manifest.json'
    parent.write_text(parent.read_text() + ' ')
    with pytest.raises(ValueError, match='checkpoint parent changed'):
        builder.run(root, plan, index, output, status, max_wait_seconds=5)

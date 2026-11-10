from __future__ import annotations

import json
import os

import pytest

from stockagent.data_sync import source_stream_staging as stage
from stockagent.data_sync import workspace_relocation as relocation


def _roots(tmp_path):
    source = tmp_path/'source'
    destination = tmp_path/'workspace'/'copy'
    evidence = tmp_path/'workspace'/'evidence'
    (source/'day').mkdir(parents=True)
    destination.parent.mkdir()
    (source/'manifest.json').write_text('{"generation": 1}')
    (source/'day'/'data.parquet').write_bytes(b'original minute data'*1000)
    (source/'day'/'summary.json').write_text('{"rows": 1000}')
    return source, destination, evidence


def test_source_stream_copy_is_durable_and_does_not_claim_destination_readback(tmp_path, monkeypatch):
    source, destination, evidence = _roots(tmp_path)
    monkeypatch.setattr(relocation, 'digest', lambda *_a, **_k: pytest.fail('no payload readback'))
    result = stage.copy(source, destination, evidence, workers=3)
    assert result['files'] == 3
    assert result['source_sha256_streamed'] is True
    assert result['destination_independent_readback'] is False
    assert result['all_source_sha256_and_destination_readback_verified'] is False
    assert (destination/'day/data.parquet').read_bytes() == (source/'day/data.parquet').read_bytes()
    assert json.loads((evidence/'copy-result.json').read_text()) == result
    assert stage.copy(source, destination, evidence, workers=2)['files'] == 3


def test_source_stream_reports_final_namespace_phases_without_payload_readback(tmp_path):
    source, destination, evidence = _roots(tmp_path)
    observed = []
    result = stage.copy(source, destination, evidence, workers=2, progress=observed.append)
    states = [event.get('state') for event in observed]
    assert states.index('source_stream_copy') < states.index('verifying_source_namespace')
    assert states.index('verifying_source_namespace') < states.index('verifying_destination_namespace')
    assert states.index('verifying_destination_namespace') < states.index('applying_destination_directory_metadata')
    assert states.index('applying_destination_directory_metadata') < states.index('flushing_destination')
    for event in observed:
        if event.get('state') in {'preparing_destination_directories', 'verifying_source_namespace',
                                  'verifying_destination_namespace', 'applying_destination_directory_metadata'}:
            assert 0 <= event['entries_completed'] <= event['entries_total']
            assert event['destination_payload_readback'] is False
    assert result['destination_independent_readback'] is False


def test_interrupted_owned_partial_is_retried_without_removing_source(tmp_path, monkeypatch):
    source, destination, evidence = _roots(tmp_path)
    real_link = os.link
    def interrupted(first, second, *args, **kwargs):
        if str(second).endswith('data.parquet'):
            raise OSError('simulated interrupted publication')
        return real_link(first, second, *args, **kwargs)
    with monkeypatch.context() as scoped:
        scoped.setattr(stage.os, 'link', interrupted)
        with pytest.raises(OSError, match='simulated interrupted'):
            stage.copy(source, destination, evidence, workers=1)
    assert (source/'day/data.parquet').is_file()
    assert list((destination/'day').glob('.source-stream-*.partial'))
    result = stage.copy(source, destination, evidence, workers=2)
    assert result['files'] == 3
    assert not list((destination/'day').glob('.source-stream-*.partial'))
    assert (destination/'day/data.parquet').read_bytes() == (source/'day/data.parquet').read_bytes()


def test_changed_destination_or_source_generation_never_counts_as_complete(tmp_path):
    source, destination, evidence = _roots(tmp_path)
    stage.copy(source, destination, evidence)
    (destination/'day/data.parquet').write_bytes(b'changed')
    with pytest.raises(ValueError, match='generation changed'):
        stage.copy(source, destination, evidence)
    assert (source/'day/data.parquet').read_bytes().startswith(b'original')


def test_empty_nested_directories_are_preserved_without_claiming_readback(tmp_path):
    source, destination, evidence = _roots(tmp_path)
    (source/'empty'/'nested').mkdir(parents=True)
    result = stage.copy(source, destination, evidence)
    assert (destination/'empty'/'nested').is_dir()
    assert list((destination/'empty'/'nested').iterdir()) == []
    assert result['destination_independent_readback'] is False
    assert stage.copy(source, destination, evidence)['files'] == result['files']


def test_failed_batch_barrier_never_commits_and_replays_bound_names(tmp_path, monkeypatch):
    source, destination, evidence = _roots(tmp_path)
    with monkeypatch.context() as scoped:
        scoped.setattr(relocation, 'sync_filesystem',
                       lambda _path: (_ for _ in ()).throw(OSError('barrier failed')))
        with pytest.raises(OSError, match='barrier failed'):
            stage.copy(source, destination, evidence, workers=3)
    assert not (evidence/'copy-result.json').exists()
    assert (source/'day/data.parquet').is_file()
    assert stage.copy(source, destination, evidence, workers=3)['files'] == 3
    assert (destination/'day/data.parquet').read_bytes() == (source/'day/data.parquet').read_bytes()


def test_batch_limits_amortize_small_files_and_bound_large_file_transactions():
    rows = [(str(index), '{}', size) for index, size in enumerate([2, 3, 6, 20, 1])]
    batches = list(stage._batches(iter(rows), max_files=2, max_bytes=10))
    assert batches == [rows[:2], rows[2:3], rows[3:4], rows[4:]]
    assert list(stage._batches(iter(()))) == []


def test_later_failed_batch_replays_only_uncommitted_files(tmp_path, monkeypatch):
    import sqlite3

    source, destination, evidence = _roots(tmp_path)
    for index in range(4):
        (source / 'day' / f'{index}.txt').write_text(str(index))
    real_batches = stage._batches
    real_sync = relocation.sync_filesystem
    calls = 0

    def interrupt_second_barrier(path):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError('second batch barrier failed')
        real_sync(path)

    with monkeypatch.context() as scoped:
        scoped.setattr(stage, '_batches', lambda rows: real_batches(rows, max_files=2))
        scoped.setattr(relocation, 'sync_filesystem', interrupt_second_barrier)
        with pytest.raises(OSError, match='second batch barrier failed'):
            stage.copy(source, destination, evidence, workers=2)
    with sqlite3.connect(evidence / 'files.sqlite') as db:
        assert db.execute("SELECT count(*) FROM files WHERE kind='file' AND state='verified'").fetchone()[0] == 2
    result = stage.copy(source, destination, evidence, workers=2)
    assert result['files'] == 7
    assert result['source_files_deleted'] == 0
    assert result['destination_independent_readback'] is False
    assert all((destination / 'day' / f'{index}.txt').read_text() == str(index) for index in range(4))


def test_budget_is_checked_after_inventory_before_payload_copy(tmp_path):
    source, destination, evidence = _roots(tmp_path)
    with pytest.raises(ValueError, match='reserved E budget'):
        stage.copy(source, destination, evidence, budget_bytes=1)
    assert (source/'day/data.parquet').is_file()
    assert not (destination/'day/data.parquet').exists()


def test_trading_priority_interrupts_and_resumes_without_deleting_source(tmp_path):
    source, destination, evidence = _roots(tmp_path)
    calls = 0
    def guard():
        nonlocal calls
        calls += 1
        if calls >= 3:
            raise BlockingIOError('tw_day_trade_priority_window')
    with pytest.raises(BlockingIOError, match='tw_day_trade_priority_window'):
        stage.copy(source, destination, evidence, workers=1, guard=guard)
    assert not (evidence/'copy-result.json').exists()
    assert (source/'day/data.parquet').is_file()
    assert stage.copy(source, destination, evidence, workers=2)['files'] == 3


def test_unowned_destination_name_is_preserved_and_rejected(tmp_path):
    source, destination, evidence = _roots(tmp_path)
    db = relocation.index(source, destination, evidence)
    db.close()
    # A pre-existing standard relocation cannot be silently adopted as the
    # weaker source-stream contract, even before it has copied any payload.
    (destination/'unrelated').write_text('do not remove')
    with pytest.raises(ValueError, match='another copy transaction'):
        stage.copy(source, destination, evidence)
    assert (destination/'unrelated').read_text() == 'do not remove'

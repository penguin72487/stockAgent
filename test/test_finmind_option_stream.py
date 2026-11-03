from __future__ import annotations

from datetime import UTC, date, datetime
import json
from pathlib import Path
import runpy
import sqlite3

import pyarrow.parquet as pq
import pytest

from downloader import download_finmind_complement as owner
from downloader import finmind_option_stream as streaming
from downloader.finmind_updates import row_fingerprint
from test_finmind_response_bounds import Limiter, Response, Session


NOW = datetime(2026, 10, 29, tzinfo=UTC)
TASK = owner.Task('TaiwanOptionTick', 'TXO', '2026-10-21', 'id_day', 0, 'failed')


def row(n=1, **extra):
    return {'date': '2026-10-21 08:45:00', 'option_id': 'TXO', 'price': 12.5,
            'ExercisePrice': 25000.0, 'PutCall': 'C', 'volume': n, **extra}


def request(tmp_path: Path, monkeypatch, raw: bytes, *, batch_rows=2):
    monkeypatch.setattr(owner, 'MIN_FREE_BYTES', 0)
    # Includes split UTF-8 characters and empty transport chunks.
    chunks = [raw[i:i+7] for i in range(0, len(raw), 7)]
    chunks.insert(1, b'')
    response = Response(chunks)
    limiter = Limiter()
    session = Session(response)
    result = streaming.fetch(session, limiter, tmp_path / 'complement', TASK, 'private-test-token',
                             today=NOW.date(), batch_rows=batch_rows)
    assert limiter.waits == 1 and len(session.calls) == 1 and response.closes == 1
    assert session.calls[0][1]['params'] == {'dataset': 'TaiwanOptionTick', 'data_id': 'TXO',
                                            'start_date': '2026-10-21'}
    return result, limiter


def test_stream_preserves_rows_late_fields_duplicates_and_normal_owner_receipt(tmp_path, monkeypatch):
    rows = [row(1, name='選擇權 "測試"'), row(1, name='選擇權 "測試"'),
            row(3, late='\\extra'), row(4, late=None)]
    raw = json.dumps({'unknown': {'nested': [1, 2]}, 'data': rows, 'msg': 'success', 'status': 200},
                     ensure_ascii=False).encode()
    (value, metadata), _ = request(tmp_path, monkeypatch, raw)
    assert isinstance(value, streaming.StreamedOptionRows)
    assert value.row_fingerprint == row_fingerprint(rows)
    assert value.observation['response_rows'] == len(rows)
    receipt = owner._store(tmp_path / 'complement', TASK, value, NOW, request_metadata=metadata)
    assert not value.path.exists()
    assert pq.read_table(tmp_path / 'complement' / receipt['parquet_path']).to_pylist() == [
        {**{'name': None, 'late': None}, **r} for r in rows]
    assert receipt['rows'] == 4 and receipt['source_first_date'] == '2026-10-21'
    assert receipt['update_observation']['fingerprint'] == row_fingerprint(rows)
    assert receipt['update_observation']['fingerprint_basis'] == 'canonical_row_multiset_v1'
    assert receipt['request']['response_processing'] == 'bounded_json_batches_preserve_order_duplicates_v1'


def test_stream_avoids_old_decoded_size_limit_without_requery(tmp_path, monkeypatch):
    rows = [row(i) for i in range(500)]
    raw = json.dumps({'status': 200, 'msg': 'success', 'data': rows}).encode()
    monkeypatch.setattr(owner, 'BULK_MAX_RESPONSE_BYTES', 64)
    legacy = Response([raw])
    (tmp_path / 'legacy').mkdir()
    with pytest.raises(owner.SourceError, match='response_size_limit'):
        owner._fetch_rows(Session(legacy), Limiter(), tmp_path / 'legacy', TASK.dataset, 'x', {},
                          max_response_bytes=64)
    (value, metadata), _ = request(tmp_path, monkeypatch, raw, batch_rows=31)
    receipt = owner._store(tmp_path / 'complement', TASK, value, NOW, request_metadata=metadata)
    assert pq.read_table(tmp_path / 'complement' / receipt['parquet_path']).to_pylist() == rows


@pytest.mark.parametrize('raw,code', [
    (b'{"status":200,"msg":"success","data":[{"date":', 'invalid_json'),
    (b'{"status":200,"msg":"success","data":[1]}', 'invalid_rows'),
    (b'{"status":200,"msg":"success","data":[],"data":[]}', 'invalid_json'),
    (b'{"status":200,"msg":"success"}', 'invalid_rows'),
    (b'{"status":429,"msg":"too many","data":[]}', 'rate_limited'),
    (b'{"status":200,"msg":"success","data":[]} garbage', 'invalid_json'),
])
def test_stream_fails_closed_and_cleans_staging(tmp_path, monkeypatch, raw, code):
    with pytest.raises(owner.SourceError) as error:
        request(tmp_path, monkeypatch, raw)
    assert error.value.code == code
    assert not list((tmp_path / 'complement' / '.option-json-stream').iterdir())
    assert not (tmp_path / 'complement' / 'receipts').exists()


@pytest.mark.parametrize('wrong', [row(option_id='OTHER'), row(date='2026-10-20 23:59:59')])
def test_stream_rejects_wrong_source_identity_before_publication(tmp_path, monkeypatch, wrong):
    raw = json.dumps({'status': 200, 'msg': 'success', 'data': [wrong]}).encode()
    with pytest.raises(ValueError):
        request(tmp_path, monkeypatch, raw)
    assert not list((tmp_path / 'complement' / '.option-json-stream').iterdir())
    assert not (tmp_path / 'complement' / 'receipts').exists()


def test_empty_keeps_existing_empty_guard_and_normal_transport(tmp_path, monkeypatch):
    raw = json.dumps({'status': 200, 'msg': 'success', 'data': []}).encode()
    (value, metadata), _ = request(tmp_path, monkeypatch, raw)
    assert value == [] and value.observation['response_rows'] == 0
    receipt = owner._store(tmp_path / 'complement', TASK, value, NOW, request_metadata=metadata)
    assert receipt['status'] == 'observed_empty'


def test_physical_capacity_gate_precedes_request(tmp_path, monkeypatch):
    class Denied:
        def sample(self):
            return {'state': 'physical_backing_reserve'}
    monkeypatch.setattr(owner, 'MIN_FREE_BYTES', 0)
    session, limiter = Session(Response([])), Limiter()
    with pytest.raises(owner.SourceError, match='object_disk_guard'):
        streaming.fetch(session, limiter, tmp_path / 'complement', TASK, 'x', today=NOW.date(),
                        capacity_sampler=Denied())
    assert not session.calls and limiter.waits == 0


def test_stream_waits_for_a_fresh_capacity_probe_without_bypassing_the_gate(tmp_path):
    class Sampler:
        def __init__(self, final):
            self.final = final
            self.waited = []
        def sample(self):
            return {'state': 'deferred', 'reason': 'capacity_sample_expired'}
        def initial_sample(self, timeout_seconds):
            self.waited.append(timeout_seconds)
            return self.final

    admitted = Sampler({'state': 'admitted'})
    streaming._admit(tmp_path, admitted, 0)
    assert admitted.waited == [29]
    denied = Sampler({'state': 'deferred', 'reason': 'physical_backing_reserve'})
    with pytest.raises(owner.SourceError, match='object_disk_guard'):
        streaming._admit(tmp_path, denied, 0)
    assert denied.waited == [29]


def test_stream_requirement_survives_a_later_failure_and_owner_restart(tmp_path):
    database = tmp_path / 'queue.sqlite3'
    with sqlite3.connect(database) as conn:
        assert streaming.requires_stream(conn, TASK, 'response_size_limit', NOW)
    with sqlite3.connect(database) as conn:
        assert streaming.requires_stream(conn, TASK, 'object_disk_guard', NOW)
        other = owner.Task('TaiwanOptionTick', 'TXO', '2026-10-20', 'id_day', 0, 'failed')
        assert not streaming.requires_stream(conn, other, 'object_disk_guard', NOW)
        non_option = owner.Task('TaiwanFuturesTick', 'TX', '2026-10-21', 'id_day', 0, 'failed')
        assert not streaming.requires_stream(conn, non_option, 'response_size_limit', NOW)


def test_source_error_identity_is_shared_when_owner_is_executed_as_another_module():
    with pytest.warns(RuntimeWarning, match='found in sys.modules'):
        executed = runpy.run_module('downloader.download_finmind_complement', run_name='isolated_owner', alter_sys=True)
    assert executed['SourceError'] is owner.SourceError is streaming.SourceError

"""Full receipt I/O retains original bytes, errors and recovery boundaries."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pytest

from downloader import news_fulltext_queue as queue_module
from downloader.news_fulltext import fetch_article
from scripts import benchmark_news_acceptance_recovery as benchmark_module
from stockagent.private_json import loads
from test_news_acceptance_recovery import URL, accepted_queue, config
from test_news_fulltext import FakeResponse, article_html, http_fixture


@pytest.mark.parametrize('raw', [
    b'{"value":-0.0,"big":1844674407370955161600000000000000000000}',
    b'{\r\n"values":[null,true,false,1e3],"duplicate":1,"duplicate":2\r}',
    b'{"legacy":[NaN,Infinity,-Infinity],"surrogate":"\\ud800"}',
    '{"original":"台灣原值","body":"'.encode() + b'x' * ((2 << 20) + 1) + b'"}',
])
def test_complete_input_preserves_all_values_and_never_uses_prefix_budget(tmp_path, raw):
    path = tmp_path / 'receipt.json'
    path.write_bytes(raw)
    reference = loads(benchmark_module._buffered_complete_receipt_input(path))
    actual = loads(benchmark_module._unbuffered_complete_receipt_input(path))
    assert json.dumps(actual, ensure_ascii=True, sort_keys=True) == json.dumps(
        reference, ensure_ascii=True, sort_keys=True)


@pytest.mark.parametrize('raw', [b'{invalid}', b'\xff{}', b'\xef\xbb\xbf{}',
    '{}'.encode('utf-16'), b'{"control":"\x00"}', b'{\r\n invalid}'])
def test_original_encoding_and_json_error_position_are_preserved(tmp_path, raw):
    path = tmp_path / 'receipt.json'
    path.write_bytes(raw)
    def outcome(reader):
        with pytest.raises((ValueError, UnicodeError)) as caught:
            loads(reader(path))
        return type(caught.value), str(caught.value)
    assert outcome(benchmark_module._unbuffered_complete_receipt_input) == outcome(
        benchmark_module._buffered_complete_receipt_input)


@pytest.mark.parametrize('kind', ['missing', 'directory', 'symlink_loop'])
def test_filesystem_error_type_number_and_filename_stay_original(tmp_path, kind):
    path = tmp_path / 'bad.json'
    if kind == 'directory':
        path.mkdir()
    elif kind == 'symlink_loop':
        path.symlink_to(path)
    def outcome(reader):
        with pytest.raises(OSError) as caught:
            reader(path)
        return type(caught.value), caught.value.errno, caught.value.filename
    assert outcome(benchmark_module._unbuffered_complete_receipt_input) == outcome(
        benchmark_module._buffered_complete_receipt_input)


@pytest.mark.parametrize('failure', ['read', 'close'])
def test_no_input_is_returned_after_read_or_close_failure(failure):
    observed = []
    @contextmanager
    def opening(*args, **kwargs):
        observed.append((args, kwargs))
        def read():
            if failure == 'read':
                raise OSError('late input read failed')
            return b'{"original":true}'
        try:
            yield SimpleNamespace(read=read)
        finally:
            observed.append('closed')
            if failure == 'close':
                raise OSError('input close failed')
    with pytest.raises(OSError, match='failed'):
        benchmark_module._unbuffered_complete_receipt_input(SimpleNamespace(open=opening))
    assert observed[-1] == 'closed'


def test_readall_keeps_later_short_pipe_writes_until_real_eof():
    reading, writing = os.pipe()
    blocks, errors = [b'{"original":', b'true}'], []
    def producer():
        try:
            for block in blocks:
                os.write(writing, block)
                time.sleep(.005)
        except BaseException as error:
            errors.append(error)
        finally:
            os.close(writing)
    thread = threading.Thread(target=producer)
    thread.start()
    try:
        path = SimpleNamespace(open=lambda *args, **kwargs: os.fdopen(reading, *args, **kwargs))
        assert benchmark_module._unbuffered_complete_receipt_input(path) == b''.join(blocks)
    finally:
        thread.join(timeout=2)
    assert not thread.is_alive() and not errors


@pytest.mark.parametrize('corruption', ['', 'digest', 'rows', 'contract', 'json'])
def test_candidate_retains_canonical_orphan_admission(config, tmp_path, monkeypatch, corruption):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html())})
    queue = queue_module.NewsQueue(tmp_path, http.registry)
    try:
        monkeypatch.setattr(queue_module, 'read_acceptance_receipt_input',
            benchmark_module._unbuffered_complete_receipt_input)
        queue.add_articles([{'url': URL}])
        record = fetch_article(http, queue.claim_url(URL, 300))
        # Preserve the real pre-commit lease witness instead of editing a
        # previously committed article into an impossible restart state.
        with monkeypatch.context() as patch:
            patch.setattr(queue, '_accept_records', lambda *a, **k: (
                _ for _ in ()).throw(OSError('simulated interruption')))
            with pytest.raises(OSError, match='simulated interruption'):
                queue.accept([record])
        path = next((tmp_path / 'receipts').glob('*.json'))
        receipt = json.loads(path.read_text())
        if corruption == 'digest':
            receipt['sha256'] = '0' * 64
        elif corruption == 'rows':
            receipt['rows'] += 1
        elif corruption == 'contract':
            receipt['contract_version'] = -1
        path.write_text('{invalid}' if corruption == 'json' else json.dumps(receipt))
        with queue.db:
            queue.db.execute(queue_module.ABANDONED_WORK_RECOVERY_SQL)
        if corruption:
            with pytest.raises(ValueError):
                queue.recover_acceptance(read_ahead_workers=0)
            assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        else:
            queue.recover_acceptance(read_ahead_workers=0)
            assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
            assert queue.db.execute('SELECT state FROM articles').fetchone()[0] == 'complete'
    finally:
        queue.close()
        http.close()


def test_unbuffered_orphan_replay_does_not_overwrite_superseded_head(config, tmp_path, monkeypatch):
    queue, http = accepted_queue(config, tmp_path)
    try:
        monkeypatch.setattr(queue_module, 'read_acceptance_receipt_input',
            benchmark_module._unbuffered_complete_receipt_input)
        with queue.db:
            queue.db.execute('DELETE FROM accepted_batches')
            queue.db.execute("UPDATE articles SET state='pending',body_sha256=NULL,body_bytes=NULL,parquet_path=NULL")
        before = tuple(queue.db.execute('SELECT state,captured_at_utc,last_checked_at_utc,body_sha256,parquet_path FROM articles').fetchone())
        queue.recover_acceptance(read_ahead_workers=0)
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
        assert tuple(queue.db.execute('SELECT state,captured_at_utc,last_checked_at_utc,body_sha256,parquet_path FROM articles').fetchone()) == before
        assert queue.db.execute("SELECT value FROM counters WHERE key='article_capture_superseded_at_commit'").fetchone()[0] == 1
    finally:
        queue.close()
        http.close()


def test_benchmark_measures_complete_canonical_recovery_and_restores_input(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    original = queue_module.read_acceptance_receipt_input
    try:
        report = benchmark_module.benchmark(tmp_path, maximum_receipts=1, cycles=1,
            unbuffered_input_only=True)
        assert report['accepted'] and report['contract_version'] == 12
        assert report['decode_parity']['all_fields_equal'] and report['source_receipts_unchanged']
        assert report['publisher_requests'] == 0 and report['production_database_written'] is False
        assert {r['mode'] for r in report['results']} == {
            'buffered_complete_input', 'unbuffered_complete_input'}
        assert all(r['sqlite_changes'] == 0 and r['receipt_io']['completed'] for r in report['results'])
        assert queue_module.read_acceptance_receipt_input is original
    finally:
        queue.close()
        http.close()


def test_benchmark_failure_restores_original_input(config, tmp_path, monkeypatch):
    queue, http = accepted_queue(config, tmp_path)
    original = queue_module.read_acceptance_receipt_input
    def fail(*args, **kwargs):
        raise OSError('receipt stage failed')
    monkeypatch.setattr(queue_module.NewsQueue, 'recover_acceptance', fail)
    try:
        with pytest.raises(OSError, match='receipt stage failed'):
            benchmark_module.benchmark(tmp_path, maximum_receipts=1, cycles=1,
                unbuffered_input_only=True)
        assert queue_module.read_acceptance_receipt_input is original
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('invalid', [0, 1, None, 'true'])
def test_comparison_is_an_explicit_boolean(tmp_path, invalid):
    with pytest.raises(ValueError, match='explicit_complete_receipt_input_comparison_required'):
        benchmark_module.benchmark(tmp_path, unbuffered_input_only=invalid)


def test_comparison_cannot_mix_unrelated_benchmark_axes(tmp_path):
    with pytest.raises(ValueError, match='separate_receipt_io_and_decode_comparisons_required'):
        benchmark_module.benchmark(tmp_path, unbuffered_input_only=True, prefix_io_only=True)

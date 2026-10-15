"""Receipt I/O cannot expand accepted encodings or weaken orphan replay."""
import json
from pathlib import Path

import pytest

from downloader import news_fulltext_queue as queue_module
from scripts.benchmark_news_acceptance_recovery import _binary_receipt_input, _binary_recovery, benchmark
from stockagent.private_json import loads
from test_news_acceptance_recovery import accepted_queue, config


@pytest.mark.parametrize('raw', [
    b'{"parquet_path":"bodies/a.parquet","unknown":[null,true,false,1e3,-0.0]}',
    b'{"integer":1844674407370955161600000000000000000000000}',
    b'{\r\n "array":[1,2,3]\r}', b'{"escaped":"\\r\\n"}', b'{"duplicate":1,"duplicate":2}',
    b'{"infinity":Infinity,"nan":NaN}', b'{"surrogate":"\\ud800"}',
    '{"台灣":"原始值\r\n不應改寫","null":null}'.replace('\r\n', '\\r\\n').encode(),
])
def test_binary_input_has_full_default_text_value_parity(tmp_path, raw):
    path = tmp_path / 'receipt.json'
    path.write_bytes(raw)
    reference = loads(path.read_text())
    actual = loads(_binary_receipt_input(path))
    assert json.dumps(reference, ensure_ascii=True, sort_keys=True) == json.dumps(actual, ensure_ascii=True, sort_keys=True)


@pytest.mark.parametrize('raw', [b'{invalid}', b'{\r\n invalid}', b'{\r bad}', b'{"control":"\x00"}',
    b'\xef\xbb\xbf{}', b'\xff{}', b'{"surrogate":"\xed\xa0\x80"}',
    '{}'.encode('utf-16'), '{}'.encode('utf-16-le'), '{}'.encode('utf-32-le'),
])
def test_encoding_or_json_failure_has_original_exception_type_and_position(tmp_path, raw):
    path = tmp_path / 'receipt.json'
    path.write_bytes(raw)
    def outcome(reader):
        try:
            loads(reader())
        except (ValueError, TypeError, UnicodeError) as error:
            return type(error), str(error)
        pytest.fail('source-invalid receipt was admitted')
    assert outcome(lambda: path.read_text()) == outcome(lambda: _binary_receipt_input(path))


def test_only_one_original_read_and_non_utf8_locale_uses_same_captured_text_bytes(tmp_path, monkeypatch):
    from scripts import benchmark_news_acceptance_recovery as module
    path = tmp_path / 'receipt.json'
    path.write_bytes(b'{"original":true}')
    seen, original = [], Path.read_bytes
    def read(path):
        seen.append(path)
        return original(path)
    monkeypatch.setattr(Path, 'read_bytes', read)
    monkeypatch.setattr(module.locale, 'getpreferredencoding', lambda _flag: 'cp1252')
    actual = _binary_receipt_input(path)
    assert isinstance(actual, str) and loads(actual) == {'original': True}
    assert seen == [path]


@pytest.mark.parametrize('corruption', ['', 'digest', 'rows', 'contract', 'json'])
def test_binary_prototype_preserves_canonical_orphan_validation_and_order(config, tmp_path, corruption):
    queue, http = accepted_queue(config, tmp_path)
    try:
        relative, receipt_relative = queue.db.execute('SELECT path,receipt_path FROM accepted_batches').fetchone()
        path = tmp_path / receipt_relative
        receipt = json.loads(path.read_text())
        if corruption == 'digest':
            receipt['sha256'] = '0' * 64
        elif corruption == 'rows':
            receipt['rows'] += 1
        elif corruption == 'contract':
            receipt['contract_version'] = -1
        path.write_text('{invalid}' if corruption == 'json' else json.dumps(receipt))
        with queue.db:
            queue.db.execute('DELETE FROM accepted_batches WHERE path=?', (relative,))
            queue.db.execute("UPDATE articles SET state='pending',body_sha256=NULL,body_bytes=NULL,parquet_path=NULL")
        if corruption:
            with pytest.raises(ValueError):
                _binary_recovery(queue, sorted((tmp_path / 'receipts').glob('*.json')))
            assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        else:
            _binary_recovery(queue, sorted((tmp_path / 'receipts').glob('*.json')))
            assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
            before = queue.db.total_changes
            _binary_recovery(queue, sorted((tmp_path / 'receipts').glob('*.json')))
            assert queue.db.total_changes == before
    finally:
        queue.close()
        http.close()


def test_binary_benchmark_uses_no_production_writes_and_restores_same_recovery_method(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    try:
        method = queue._recover_acceptance_receipt
        report = benchmark(tmp_path, maximum_receipts=1, cycles=1, binary_read_only=True)
        assert report['accepted'] and report['contract_version'] == 4
        assert report['decode_parity']['all_fields_equal']
        assert {row['mode'] for row in report['results']} == {'text_read', 'binary_read'}
        assert all(row['sqlite_changes'] == 0 for row in report['results'])
        assert report['source_receipts_unchanged'] and report['publisher_requests'] == 0
        assert queue._recover_acceptance_receipt == method
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('kwargs', [{'json_decode_only': True}, {'read_ahead_workers': 2}, {'binary_read_only': 1}])
def test_binary_benchmark_rejects_mixed_axes_or_wrong_boolean(tmp_path, kwargs):
    with pytest.raises(ValueError):
        benchmark(tmp_path, binary_read_only=kwargs.pop('binary_read_only', True), **kwargs)

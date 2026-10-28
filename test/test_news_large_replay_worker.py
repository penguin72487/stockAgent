"""The large-file reader is a proof-only subprocess, not a second owner."""
from contextlib import closing
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import sha256_file
from downloader.news_fulltext_policy import CONTRACT_VERSION, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_cnyes_news_bulk import cfg
from test_news_retained_metadata_replay import old_head


def run_worker(request, *, timeout=30):
    process = subprocess.run([sys.executable, '-m', 'downloader.news_fulltext_large_replay_worker'],
        input=json.dumps(request).encode(), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        cwd=Path(__file__).resolve().parents[1], timeout=timeout, check=False)
    assert process.returncode == 0, process.stderr.decode(errors='replace')
    assert not process.stderr
    return json.loads(process.stdout)


def request_for(root, head, receipt, *, extra=256 << 20, output=32 << 20):
    return {'version': 1, 'root': str(root.resolve()), 'source_path': head['parquet_path'],
            'receipt_path': receipt, 'receipt_sha256': sha256_file(root / receipt),
            'article_id': head['id'].hex(), 'body_sha256': head['body_sha256'],
            'extra_address_bytes': extra, 'max_output_bytes': output, 'parent_pid': os.getpid()}


def batch_request_for(root, heads, receipt, *, extra=384 << 20, output=32 << 20):
    request = request_for(root, heads[0], receipt, extra=extra, output=output)
    request['version'] = 2
    request['heads'] = [{'article_id': head['id'].hex(), 'body_sha256': head['body_sha256']} for head in heads]
    del request['article_id'], request['body_sha256']
    return request


def test_batch_worker_returns_same_proofs_as_single_heads(cfg, tmp_path):
    from test_news_retained_metadata_checkpoint_batch import native_heads

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = native_heads(queue, 2, repair_count=2)
        heads = list(original.values())
        receipt = queue.db.execute('SELECT receipt_path FROM accepted_batches').fetchone()[0]
        singles = [run_worker(request_for(tmp_path, head, receipt)) for head in heads]
        result = run_worker(batch_request_for(tmp_path, heads, receipt))
        assert result['state'] == 'ok' and result['version'] == 2
        assert result['items'] == [{'article_id': head['id'].hex(), 'result': proof}
                                   for head, proof in zip(heads, singles)]
        assert {row['id']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == original
        assert verify_bodies(tmp_path)['accepted']


def test_batch_bad_head_is_isolated_and_duplicate_request_is_rejected(cfg, tmp_path):
    from test_news_retained_metadata_checkpoint_batch import native_heads

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        heads = list(native_heads(queue, 2, repair_count=2).values())
        receipt = queue.db.execute('SELECT receipt_path FROM accepted_batches').fetchone()[0]
        request = batch_request_for(tmp_path, heads, receipt)
        request['heads'][0]['body_sha256'] = '0' * 64
        items = run_worker(request)['items']
        assert items[0]['result'] == {'version': 1, 'state': 'proof_failure', 'error': 'head_identity_mismatch'}
        assert items[1]['result']['state'] == 'ok' and items[1]['result']['outcome'] == 'requires_parent_validation'
        request['heads'][1]['article_id'] = request['heads'][0]['article_id']
        assert run_worker(request) == {'version': 1, 'state': 'proof_failure', 'error': 'invalid_request'}


def test_batch_output_bound_never_returns_partial_proofs(cfg, tmp_path):
    from test_news_retained_metadata_checkpoint_batch import native_heads

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        heads = list(native_heads(queue, 2, repair_count=2).values())
        receipt = queue.db.execute('SELECT receipt_path FROM accepted_batches').fetchone()[0]
        result = run_worker(batch_request_for(tmp_path, heads, receipt, output=256))
        assert result == {'version': 2, 'state': 'resource_wait', 'error': 'output_bound_exceeded'}


@pytest.mark.parametrize('dictionary', [False, True])
@pytest.mark.parametrize('row_group_size', [2, 16])
def test_batch_stream_preserves_last_nonnull_shared_envelopes(tmp_path, dictionary, row_group_size):
    from downloader.news_fulltext_large_replay_worker import _stream_selected_records

    first, second, shared, other = (f'{number:064x}' for number in range(1, 5))
    rows = [
        {'article_id': shared, 'source_response_article_id': shared, 'raw_response_body': b'old', 'body': 'source'},
        {'article_id': first, 'source_response_article_id': shared, 'raw_response_body': None, 'body': 'first'},
        {'article_id': other, 'source_response_article_id': other, 'raw_response_body': b'other', 'body': 'other'},
        {'article_id': second, 'source_response_article_id': shared, 'raw_response_body': None, 'body': 'second'},
        {'article_id': shared, 'source_response_article_id': shared, 'raw_response_body': b'last', 'body': 'source'},
        {'article_id': shared, 'source_response_article_id': shared, 'raw_response_body': None, 'body': 'source'},
    ]
    path = tmp_path / 'batch-stream.parquet'
    pq.write_table(pa.Table.from_pylist(rows), path, use_dictionary=dictionary, row_group_size=row_group_size)
    batch = pq.ParquetFile(path)
    meta = batch.read(columns=['article_id', 'source_response_article_id']).to_pylist()
    indexed, envelopes = _stream_selected_records(batch, meta, [{'article_id': first}, {'article_id': second}])
    assert indexed == {first: rows[1], second: rows[3]} and envelopes == {shared: b'last'}


def test_worker_reads_only_existing_proof_and_does_not_accept_or_touch_queue(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = old_head(queue)
        receipt = queue.db.execute('SELECT receipt_path FROM accepted_batches').fetchone()[0]
        request = request_for(tmp_path, original, receipt)
        before_file = sha256_file(tmp_path / original['parquet_path'])
        response = run_worker(request)
        assert response['state'] == 'ok' and response['outcome'] == 'requires_parent_validation'
        assert response['source_sha256'] == before_file and response['receipt_sha256'] == request['receipt_sha256']
        assert response['metadata']['article_id'] == original['id'].hex()
        payload = base64.b64decode(response['envelope_base64'], validate=True)
        assert hashlib.sha256(payload).hexdigest() == response['record']['response_sha256']
        assert response['record']['raw_response_body'] == {'same_as_envelope': True}
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        assert sha256_file(tmp_path / original['parquet_path']) == before_file
        assert verify_bodies(tmp_path)['accepted']


@pytest.mark.parametrize('change,expected', [
    ({'receipt_sha256': '0' * 64}, 'receipt_changed_or_digest_mismatch'),
    ({'body_sha256': '0' * 64}, 'head_identity_mismatch'),
    ({'article_id': '0' * 64}, 'head_identity_mismatch'),
    ({'source_path': '../outside'}, 'invalid_receipt_contract'),
    ({'version': 2}, 'invalid_request'),
    ({'parent_pid': 2}, 'worker_parent_changed'),
])
def test_worker_refuses_wrong_proof_without_returning_body(cfg, tmp_path, change, expected):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        head = old_head(queue)
        receipt = queue.db.execute('SELECT receipt_path FROM accepted_batches').fetchone()[0]
        request = request_for(tmp_path, head, receipt)
        request.update(change)
        response = run_worker(request)
        assert response == {'version': 1, 'state': 'proof_failure', 'error': expected}
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == head
        assert verify_bodies(tmp_path)['accepted']


def test_matching_forged_receipt_cannot_escape_source_root(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        head = old_head(queue)
        original_receipt = queue.db.execute('SELECT receipt_path FROM accepted_batches').fetchone()[0]
        request = request_for(tmp_path, head, original_receipt)
        forged = {'contract_version': CONTRACT_VERSION, 'parquet_path': '../outside',
                  'publish': False, 'rows': 1, 'sha256': sha256_file(tmp_path / head['parquet_path'])}
        receipt = tmp_path / 'forged.json'
        receipt.write_text(json.dumps(forged))
        request.update(source_path='../outside', receipt_path=receipt.name, receipt_sha256=sha256_file(receipt))
        assert run_worker(request) == {'version': 1, 'state': 'proof_failure',
                                       'error': 'worker_source_or_proof_invalid'}
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == head


def test_worker_local_io_failure_is_not_a_source_failure(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        head = old_head(queue)
        receipt = queue.db.execute('SELECT receipt_path FROM accepted_batches').fetchone()[0]
        request = request_for(tmp_path, head, receipt)
        request['receipt_path'] = 'receipts/not-present.json'
        response = run_worker(request)
        assert response == {'version': 1, 'state': 'local_io_retry', 'error': 'worker_local_io_unavailable'}
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == head


def test_imported_module_cannot_set_process_wide_memory_limit():
    import resource
    from downloader.news_fulltext_large_replay_worker import execute

    before = resource.getrlimit(resource.RLIMIT_AS)
    assert execute({}) == {'version': 1, 'state': 'proof_failure', 'error': 'worker_process_required'}
    assert resource.getrlimit(resource.RLIMIT_AS) == before


def test_primitive_record_preserves_other_binary_source_fields():
    from downloader.news_fulltext_large_replay_worker import _primitive_record

    record = _primitive_record({'raw_response_body': b'shared', 'url_resolution_response_body': b'other'}, b'shared')
    assert record['raw_response_body'] == {'same_as_envelope': True}
    assert base64.b64decode(record['url_resolution_response_body']['bytes_base64'], validate=True) == b'other'


@pytest.fixture(scope='module')
def large_original(tmp_path_factory):
    root = tmp_path_factory.mktemp('large-retained-source')
    path = root / 'original.parquet'
    rows = []
    for i in range(34):
        raw = os.urandom(1 << 20)
        body = f'source body {i}'
        ident = f'{i:064x}'
        rows.append({'article_id': ident, 'body_sha256': hashlib.sha256(body.encode()).hexdigest(),
            'published_at_raw': None, 'authors_json': '[]', 'body_completeness': 'article_container_unverified',
            'final_url': f'https://m.cnyes.com/news/id/{i}', 'source_response_article_id': ident,
            'raw_response_body': raw, 'body': body, 'body_bytes': len(body.encode()),
            'response_sha256': hashlib.sha256(raw).hexdigest(), 'http_response_bytes': len(raw)})
    pq.write_table(pa.Table.from_pylist(rows), path, use_dictionary=False, compression='zstd',
                   row_group_size=len(rows), data_page_size=1 << 20, write_batch_size=1)
    assert path.stat().st_size > 32 << 20 and pq.ParquetFile(path).metadata.num_row_groups == 1
    receipt_path = root / 'receipt.json'
    receipt_path.write_text(json.dumps({'contract_version': CONTRACT_VERSION, 'parquet_path': path.name,
        'publish': False, 'rows': len(rows), 'sha256': sha256_file(path)}))
    last = rows[-1]
    head = {'parquet_path': path.name, 'id': bytes.fromhex(last['article_id']), 'body_sha256': last['body_sha256']}
    return root, head, receipt_path.name, last['response_sha256']


def test_over_32_mib_source_is_read_in_child_and_original_is_unchanged(large_original):
    root, head, receipt, raw_sha = large_original
    before = sha256_file(root / head['parquet_path'])
    response = run_worker(request_for(root, head, receipt), timeout=60)
    assert response['state'] == 'ok' and response['outcome'] == 'requires_parent_validation'
    payload = base64.b64decode(response['envelope_base64'], validate=True)
    assert hashlib.sha256(payload).hexdigest() == raw_sha
    assert response['source_sha256'] == before == sha256_file(root / head['parquet_path'])


def test_memory_exhaustion_in_child_is_retryable_and_leaves_no_partial_result(large_original):
    root, head, receipt, _ = large_original
    before = sha256_file(root / head['parquet_path'])
    response = run_worker(request_for(root, head, receipt, extra=8 << 20), timeout=60)
    assert response == {'version': 1, 'state': 'resource_wait', 'error': 'worker_memory_exhausted'}
    assert sha256_file(root / head['parquet_path']) == before


def test_output_limit_does_not_publish_partial_record(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        head = old_head(queue)
        receipt = queue.db.execute('SELECT receipt_path FROM accepted_batches').fetchone()[0]
        response = run_worker(request_for(tmp_path, head, receipt, output=1))
        assert response == {'version': 1, 'state': 'resource_wait', 'error': 'output_bound_exceeded'}
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == head


def test_successful_worker_result_is_serialized_once(cfg, tmp_path):
    """The memory-limited proof and its wire payload must share one encoding."""
    script = '''import json,runpy,sys
from pathlib import Path
original = json.dumps
calls = []
def tracked(value, *args, **kwargs):
    if isinstance(value, dict) and value.get("outcome") == "requires_parent_validation":
        calls.append(1)
    return original(value, *args, **kwargs)
json.dumps = tracked
try:
    runpy.run_module("downloader.news_fulltext_large_replay_worker", run_name="__main__")
finally:
    Path(sys.argv[1]).write_text(str(len(calls)))
'''
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        head = old_head(queue)
        receipt = queue.db.execute('SELECT receipt_path FROM accepted_batches').fetchone()[0]
        request = request_for(tmp_path, head, receipt)
        count_path = tmp_path/'serialization-count.txt'
        process = subprocess.run([sys.executable, '-c', script, str(count_path)],
            input=json.dumps(request).encode(), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            cwd=Path(__file__).resolve().parents[1], timeout=30, check=False)
        assert process.returncode == 0 and not process.stderr
        result = json.loads(process.stdout)
        assert result['state'] == 'ok' and result['outcome'] == 'requires_parent_validation'
        assert count_path.read_text() == '1'
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == head


def test_output_limit_is_exact_utf8_wire_size_without_partial_payload(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        head = old_head(queue)
        receipt = queue.db.execute('SELECT receipt_path FROM accepted_batches').fetchone()[0]
        request = request_for(tmp_path, head, receipt)
        result = run_worker(request)
        wire = json.dumps(result, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        assert any(byte >= 128 for byte in wire)  # Test multibyte UTF-8, not only ASCII.
        request['max_output_bytes'] = len(wire)
        assert run_worker(request) == result
        request['max_output_bytes'] = len(wire) - 1
        assert run_worker(request) == {'version': 1, 'state': 'resource_wait', 'error': 'output_bound_exceeded'}
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == head


def test_wire_encoding_memory_error_returns_small_retry_without_source_data(cfg, tmp_path):
    script = '''import json,runpy
original = json.dumps
def exhaust(value, *args, **kwargs):
    if isinstance(value, dict) and value.get("outcome") == "requires_parent_validation":
        raise MemoryError()
    return original(value, *args, **kwargs)
json.dumps = exhaust
runpy.run_module("downloader.news_fulltext_large_replay_worker", run_name="__main__")
'''
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        head = old_head(queue)
        receipt = queue.db.execute('SELECT receipt_path FROM accepted_batches').fetchone()[0]
        request = request_for(tmp_path, head, receipt)
        process = subprocess.run([sys.executable, '-c', script], input=json.dumps(request).encode(),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=Path(__file__).resolve().parents[1],
            timeout=30, check=False)
        assert process.returncode == 0 and not process.stderr
        assert json.loads(process.stdout) == {
            'version': 1, 'state': 'resource_wait', 'error': 'worker_memory_exhausted'}
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == head


def test_completed_metadata_is_proved_without_exporting_raw_content(cfg, tmp_path):
    from test_news_retained_metadata_checkpoint_batch import native_heads

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        heads = native_heads(queue, 1)
        head = next(iter(heads.values()))
        receipt = queue.db.execute('SELECT receipt_path FROM accepted_batches').fetchone()[0]
        response = run_worker(request_for(tmp_path, head, receipt))
        assert response['state'] == 'ok' and response['outcome'] == 'current_metadata_complete'
        assert 'record' not in response and 'envelope_base64' not in response
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == head


def test_shared_envelope_uses_last_nonnull_original(tmp_path):
    selected, shared = ('1' * 64, '2' * 64)
    payload = b'last source response'
    body = 'source body'
    common = {'published_at_raw': None, 'authors_json': '[]', 'body_completeness': 'article_container_unverified',
              'final_url': 'https://m.cnyes.com/news/id/1', 'body': body, 'body_bytes': len(body),
              'body_sha256': hashlib.sha256(body.encode()).hexdigest(),
              'response_sha256': hashlib.sha256(payload).hexdigest(), 'http_response_bytes': len(payload)}
    rows = [
        {**common, 'article_id': shared, 'source_response_article_id': shared, 'raw_response_body': b'old'},
        {**common, 'article_id': selected, 'source_response_article_id': shared, 'raw_response_body': None},
        {**common, 'article_id': shared, 'source_response_article_id': shared, 'raw_response_body': payload},
        {**common, 'article_id': shared, 'source_response_article_id': shared, 'raw_response_body': None},
    ]
    source = tmp_path / 'original.parquet'
    pq.write_table(pa.Table.from_pylist(rows), source, row_group_size=2)
    receipt = tmp_path / 'receipt.json'
    receipt.write_text(json.dumps({'contract_version': CONTRACT_VERSION, 'parquet_path': source.name,
        'publish': False, 'rows': len(rows), 'sha256': sha256_file(source)}))
    head = {'parquet_path': source.name, 'id': bytes.fromhex(selected), 'body_sha256': common['body_sha256']}
    response = run_worker(request_for(tmp_path, head, receipt.name))
    assert response['state'] == 'ok' and response['outcome'] == 'requires_parent_validation'
    assert base64.b64decode(response['envelope_base64'], validate=True) == payload
    assert response['record']['raw_response_body'] is None

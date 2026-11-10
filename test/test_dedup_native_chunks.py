from contextlib import contextmanager
import hashlib
import io

import pytest

from stockagent.data_sync import dedup_native_chunks as chunks
from stockagent.data_sync.desync_snapshots import SnapshotError


class Entries:
    def __init__(self, rows):
        self.rows = rows

    def chunked_rows(self):
        yield from self.rows


def test_multiple_files_share_native_batches_and_keep_exact_boundaries(tmp_path):
    objects, rows, payloads = [], [], {}
    for file_number in range(10):
        extents, content = [], b''
        for chunk_number in range(2):
            data = f'{file_number}:{chunk_number}'.encode()
            sha = hashlib.sha256(data).hexdigest()
            name = f'blob-{file_number}-{chunk_number}'
            payloads[str(tmp_path/name)] = data
            objects.append({'sha256': sha, 'kind': 'blob', 'bytes': len(data), 'relpath': name})
            extents.append({'object_sha256': sha, 'bytes': len(data)})
            content += data
        rows.append({'path': f'file-{file_number}', 'size': len(content),
                     'sha256': hashlib.sha256(content).hexdigest(),
                     'storage': {'kind': 'chunks', 'chunks': extents}})
    batches, read = [], {}
    @contextmanager
    def native(members):
        batches.append(len(members))
        yield io.BytesIO(b''.join(payloads[str(path)] for path, _ in members))
    def consume(stream, row):
        read[row['path']] = b''.join(iter(lambda: stream.read(3), b''))
    result = chunks.stream(tmp_path, {'archive': {'objects': objects}}, Entries(rows),
                           selected_paths={row['path'] for row in rows}, completed_paths=set(),
                           consume=consume, batch_reader=native)
    assert len(result) == 10 and batches == [16, 4]
    assert all(hashlib.sha256(read[row['path']]).hexdigest() == row['sha256'] for row in rows)


def test_failed_chunk_never_exposes_an_earlier_file_from_the_same_batch(tmp_path):
    good = b'good'
    bad = b'bad'
    shas = [hashlib.sha256(good).hexdigest(), hashlib.sha256(b'expected').hexdigest()]
    objects = [{'sha256': sha, 'kind': 'blob', 'bytes': 4 if i == 0 else 3, 'relpath': str(i)}
               for i, sha in enumerate(shas)]
    rows = [{'path': str(i), 'size': item['bytes'], 'storage': {'kind': 'chunks',
             'chunks': [{'object_sha256': item['sha256'], 'bytes': item['bytes']}]}}
            for i, item in enumerate(objects)]
    calls = []
    @contextmanager
    def native(_members):
        yield io.BytesIO(good+bad)
    def consume(source, row):
        source.read(row['size'])
        calls.append(row['path'])
    with pytest.raises(SnapshotError, match='checksum mismatch'):
        chunks.stream(tmp_path, {'archive': {'objects': objects}}, Entries(rows),
                      selected_paths={'0', '1'}, completed_paths=set(),
                      consume=consume, batch_reader=native)
    assert not calls


def test_finished_representatives_are_never_read_again(tmp_path):
    payload = b'abc'
    digest = hashlib.sha256(payload).hexdigest()
    obj = {'sha256': digest, 'kind': 'blob', 'bytes': len(payload), 'relpath': 'blob'}
    rows = [{'path': name, 'size': len(payload), 'storage': {'kind': 'chunks',
             'chunks': [{'object_sha256': digest, 'bytes': len(payload)}]}} for name in ('a', 'b', 'c')]
    calls = []
    @contextmanager
    def native(members):
        calls.append(len(members))
        yield io.BytesIO(payload*len(members))
    observed = []
    result = chunks.stream(tmp_path, {'archive': {'objects': [obj]}}, Entries(rows),
                           selected_paths={'a', 'b'}, completed_paths={'a'}, batch_reader=native,
                           consume=lambda source, row: observed.append((row['path'], source.read(row['size']))))
    assert result == {'b'} and calls == [1] and observed == [('b', payload)]

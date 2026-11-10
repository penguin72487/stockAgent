from contextlib import contextmanager
import hashlib
import io
import zipfile

import pytest

from stockagent.data_sync import dedup_native_packs as packs
from stockagent.data_sync.desync_snapshots import SnapshotError


class Entries:
    def __init__(self, rows):
        self.rows = rows

    def object_rows(self, digest):
        return (row for row in self.rows if row['storage']['object_sha256'] == digest)


def fixture(tmp_path):
    path = tmp_path/'old.zip'
    contents = {f'folder/{i:03d}': bytes([i])*2000 for i in range(80)}
    with zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_STORED) as archive:
        for name, data in contents.items():
            archive.writestr(name, data)
    object_sha = hashlib.sha256(path.read_bytes()).hexdigest()
    item = {'sha256': object_sha, 'kind': 'pack', 'bytes': path.stat().st_size, 'relpath': path.name}
    rows = [{'path': name, 'size': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
             'storage': {'kind': 'pack', 'member': name, 'object_sha256': object_sha}}
            for name, data in contents.items()]
    return path, contents, {'archive': {'objects': [item]}}, Entries(rows)


def test_selective_reader_streams_only_needed_members_and_skips_checkpoints(tmp_path):
    path, contents, manifest, entries = fixture(tmp_path)
    groups_seen, observed = [], {}
    @contextmanager
    def read(groups):
        groups_seen.extend(groups)
        with zipfile.ZipFile(path) as archive:
            yield io.BytesIO(b''.join(archive.read(member['name']) for group in groups for member in group['members']))
    selected = {'folder/001', 'folder/010', 'folder/040'}
    result = packs.stream(tmp_path, manifest, entries, selected_paths=selected,
                          completed_paths={'folder/010'}, batch_reader=read,
                          consume=lambda source, row: observed.update({row['path']: source.read()}))
    assert result == {'folder/001', 'folder/040'}
    assert observed == {name: contents[name] for name in result}
    assert sum(member['size'] for group in groups_seen for member in group['members']) == 4000
    assert path.stat().st_size > 30*4000


def test_failed_member_digest_does_not_expose_an_earlier_member(tmp_path):
    path, contents, manifest, entries = fixture(tmp_path)
    @contextmanager
    def corrupt(groups):
        values = [contents[member['name']] for group in groups for member in group['members']]
        yield io.BytesIO(values[0]+b'x'*len(values[1]))
    seen = []
    with pytest.raises(SnapshotError, match='checksum mismatch'):
        packs.stream(tmp_path, manifest, entries, selected_paths={'folder/001', 'folder/040'},
                     completed_paths=set(), batch_reader=corrupt,
                     consume=lambda _source, row: seen.append(row['path']))
    assert not seen


def test_changed_zip_generation_or_failed_native_context_never_reaches_consumer(tmp_path):
    path, contents, manifest, entries = fixture(tmp_path)
    seen = []
    @contextmanager
    def changed(groups):
        data = b''.join(contents[member['name']] for group in groups for member in group['members'])
        yield io.BytesIO(data)
        path.touch()
    with pytest.raises(SnapshotError, match='changed after'):
        packs.stream(tmp_path, manifest, entries, selected_paths={'folder/001'},
                     completed_paths=set(), batch_reader=changed,
                     consume=lambda _source, row: seen.append(row['path']))
    assert not seen


def test_native_compatibility_failure_falls_back_without_exposing_the_failed_batch(tmp_path):
    _path, _contents, manifest, entries = fixture(tmp_path)
    seen, fallback = [], []
    @contextmanager
    def unavailable(_groups):
        raise OSError('native ZIP reader unavailable')
        yield  # pragma: no cover
    done = packs.stream(tmp_path, manifest, entries, selected_paths={'folder/001'}, completed_paths=set(),
                        batch_reader=unavailable, fallback_on_error=True, on_fallback=fallback.append,
                        consume=lambda _source, row: seen.append(row['path']))
    assert done == set() and not seen and fallback == ['OSError']


def test_fallback_returns_only_prior_fully_accepted_batches(tmp_path, monkeypatch):
    path, contents, manifest, entries = fixture(tmp_path)
    monkeypatch.setattr(packs, 'MAX_BATCH_MEMBERS', 1)
    calls, observed = [], {}
    @contextmanager
    def reader(groups):
        calls.append(groups)
        if len(calls) == 2:
            raise OSError('later batch failed')
        yield io.BytesIO(contents[groups[0]['members'][0]['name']])
    selected = {'folder/001', 'folder/040'}
    done = packs.stream(tmp_path, manifest, entries, selected_paths=selected, completed_paths=set(),
                        batch_reader=reader, fallback_on_error=True,
                        consume=lambda source, row: observed.update({row['path']: source.read()}))
    assert done == {'folder/001'} and observed == {'folder/001': contents['folder/001']}

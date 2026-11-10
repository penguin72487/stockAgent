from pathlib import Path
import os

import pytest

from stockagent.data_sync import packed_member_stream as stream, packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def release(tmp_path, monkeypatch):
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    source, root = tmp_path/'source', tmp_path/'cold'
    source.mkdir()
    (source/'a').write_bytes(b'alpha')
    (source/'b').write_bytes(b'bravo')
    fixed = packed.publish_packed_snapshot(root, 'source', source, node_id='test',
        pack_layout='source-dataset-v1', pack_target_bytes=1024, pack_buckets=1)
    return root, fixed


def test_stream_consumes_exact_selected_originals_and_scopes_proof(release):
    root, fixed = release
    values = {}
    result = stream.stream_members(root, fixed, names=['b', 'a'],
        consume=lambda path, value: values.update({path: value}), maximum_file_bytes=64,
        maximum_total_bytes=64)
    assert values == {'a': b'alpha', 'b': b'bravo'}
    assert result == {'files': 2, 'logical_bytes': 10, 'original_member_sha256_verified': True,
                      'whole_encoded_object_sha256_verified': False, 'independent_recovery_verified': False}


@pytest.mark.parametrize('names,limit', [(['missing'], 64), (['a', 'a'], 64), (['a'], 4)])
def test_stream_rejects_missing_duplicate_or_oversized_selection(release, names, limit):
    root, fixed = release
    with pytest.raises(SnapshotError):
        stream.stream_members(root, fixed, names=names, consume=lambda *_: None,
                              maximum_file_bytes=limit, maximum_total_bytes=64)


def test_stream_rejects_object_changed_during_consumer(release):
    root, fixed = release
    obj = root/fixed.manifest['archive']['objects'][0]['relpath']
    def change(_path, _value):
        info = obj.stat()
        os.utime(obj, ns=(info.st_atime_ns, info.st_mtime_ns+1_000_000))
    with pytest.raises(SnapshotError, match='identity changed'):
        stream.stream_members(root, fixed, names=['a'], consume=change,
                              maximum_file_bytes=64, maximum_total_bytes=64)

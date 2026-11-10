import hashlib
import json
import os
from pathlib import Path

import pytest

from stockagent.data_sync import sealed_workspace_copy as operation
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def sealed(tmp_path):
    source, stage = tmp_path/'source', tmp_path/'stage'
    (source/'nested').mkdir(parents=True, mode=0o700)
    (source/'empty').mkdir(mode=0o700)
    source.chmod(0o700)
    (source/'nested/a').write_bytes(b'hello')
    (source/'b').write_bytes(b'world')
    files = [{'relative': name, 'bytes': 5, 'sha256': hashlib.sha256(data).hexdigest()}
             for name, data in [('nested/a', b'hello'), ('b', b'world')]]
    raw = json.dumps({'files': files}, sort_keys=True).encode()
    (source/'capture-files.json').write_bytes(raw)
    digest = hashlib.sha256(raw).hexdigest()
    (source/'SOURCE-CAPTURE.READY').write_text(digest+'\n')
    return source, stage, digest


def test_sealed_copy_preserves_paths_and_resumes_closed_files(sealed):
    source, stage, digest = sealed
    events = []
    result = operation.copy(source, stage, digest, progress=events.append)
    assert result['state'] == 'copied' and result['files_completed'] == 4
    assert not result['source_deleted'] and not result['destination_readback']
    assert (stage/'data/nested/a').read_bytes() == b'hello'
    assert (stage/'data/b').read_bytes() == b'world'
    assert (stage/'data/empty').is_dir()
    assert (stage/'data/nested').stat().st_mode & 0o777 == (source/'nested').stat().st_mode & 0o777
    assert events[-1]['bytes_completed'] == events[-1]['bytes_total']
    again = operation.copy(source, stage, digest)
    assert again['files'] == result['files']
    assert (source/'nested/a').read_bytes() == b'hello'


def test_sealed_copy_resumes_after_interrupted_uncommitted_file(sealed):
    source, stage, digest = sealed
    def stop(event):
        if event['files_completed'] == 0:
            raise InterruptedError('synthetic stop')
    with pytest.raises(InterruptedError):
        operation.copy(source, stage, digest, progress=stop)
    assert list(stage.glob('.copy-*.partial'))
    result = operation.copy(source, stage, digest)
    assert result['state'] == 'copied' and not list(stage.glob('.copy-*.partial'))


def test_sealed_copy_resumes_after_rename_before_checkpoint(sealed, monkeypatch):
    source, stage, digest = sealed
    write = operation.atomic_write_json
    failed = False
    def interrupted(path, value, **kwargs):
        nonlocal failed
        if not failed and any(row.get('state') == 'completed' for row in value.get('files', {}).values()):
            failed = True
            raise InterruptedError('synthetic rename gap')
        return write(path, value, **kwargs)
    monkeypatch.setattr(operation, 'atomic_write_json', interrupted)
    with pytest.raises(InterruptedError, match='rename gap'):
        operation.copy(source, stage, digest)
    monkeypatch.setattr(operation, 'atomic_write_json', write)
    result = operation.copy(source, stage, digest)
    assert result['state'] == 'copied' and result['files_completed'] == 4


def test_sealed_copy_refuses_mutated_source_or_workspace(sealed):
    source, stage, digest = sealed
    operation.copy(source, stage, digest)
    (stage/'data/b').write_bytes(b'alter')
    with pytest.raises(SnapshotError, match='destination changed'):
        operation.copy(source, stage, digest)
    assert (source/'b').read_bytes() == b'world'


def test_sealed_copy_rejects_unlisted_file_and_preserves_source(sealed):
    source, stage, digest = sealed
    (source/'extra').write_bytes(b'keep')
    with pytest.raises(SnapshotError, match='namespace differs'):
        operation.copy(source, stage, digest)
    assert (source/'extra').read_bytes() == b'keep' and not stage.exists()


def test_sealed_copy_rejects_symlink_source_member(sealed):
    source, stage, digest = sealed
    (source/'b').unlink()
    (source/'b').symlink_to('nested/a')
    with pytest.raises(SnapshotError, match='redirected'):
        operation.copy(source, stage, digest)
    assert (source/'b').is_symlink() and not stage.exists()

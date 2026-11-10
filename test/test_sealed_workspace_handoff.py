import hashlib
import json

import pytest

from stockagent.data_sync import sealed_workspace_copy as copied, sealed_workspace_handoff as handoff
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def prepared(tmp_path):
    source, stage = tmp_path/'source', tmp_path/'e/stage'
    (source/'nested').mkdir(parents=True, mode=0o700)
    (source/'empty').mkdir(mode=0o700)
    source.chmod(0o700)
    (source/'nested/a').write_bytes(b'hello')
    (source/'b').write_bytes(b'world')
    files = [{'relative': name, 'bytes': 5, 'sha256': hashlib.sha256(data).hexdigest()}
             for name, data in [('nested/a', b'hello'), ('b', b'world')]]
    raw = json.dumps({'files': files}, sort_keys=True).encode()
    digest = hashlib.sha256(raw).hexdigest()
    (source/'capture-files.json').write_bytes(raw)
    (source/'SOURCE-CAPTURE.READY').write_text(digest+'\n')
    copied.copy(source, stage, digest)
    return source, stage, digest


class FakeMount:
    def __init__(self):
        self.bound = False

    def ensure(self, _data, _source):
        self.bound = True

    def matches(self, _data, _source):
        return self.bound


def run(prepared, mount, **kwargs):
    source, stage, digest = prepared
    return handoff.handoff(source, stage, digest, mount_ensure=mount.ensure,
                           mount_matches=mount.matches, references=lambda *_a, **_kw: [], **kwargs)


def test_handoff_retires_only_journalled_names_and_resumes_idempotently(prepared):
    source, stage, _ = prepared
    mount = FakeMount()
    result = run(prepared, mount)
    assert result['state'] == 'completed' and result['source_deleted']
    assert result['mount_handoff_completed'] and not result['destination_readback']
    assert not result['NAS_acceptance_claimed']
    assert (stage/'data/nested/a').read_bytes() == b'hello'
    assert (stage/'data/empty').is_dir()
    assert not source.with_name('.source.pre-e-'+prepared[2][:16]).exists()
    assert run(prepared, mount) == result


def test_handoff_waits_for_process_reference_without_renaming(prepared):
    source, stage, digest = prepared
    mount = FakeMount()
    with pytest.raises(SnapshotError, match='referenced'):
        handoff.handoff(source, stage, digest, mount_ensure=mount.ensure, mount_matches=mount.matches,
                        references=lambda *_a, **_kw: ['busy'])
    assert (source/'b').read_bytes() == b'world' and not mount.bound


def test_handoff_resumes_after_interrupted_retirement(prepared):
    source, stage, digest = prepared
    mount = FakeMount()
    def interrupt(event):
        if event['files_completed'] == 1:
            raise InterruptedError('stop after first removal')
    with pytest.raises(InterruptedError):
        run(prepared, mount, progress=interrupt)
    old = source.with_name('.source.pre-e-'+digest[:16])
    assert old.exists() and mount.bound
    result = run(prepared, mount)
    assert result['state'] == 'completed' and not old.exists()
    assert (stage/'data/b').read_bytes() == b'world'


def test_failed_mount_restores_original_path_and_can_retry(prepared):
    source, _, _ = prepared
    mount = FakeMount()
    def fail(*_):
        raise RuntimeError('mount failed')
    with pytest.raises(RuntimeError, match='mount failed'):
        handoff.handoff(*prepared[:2], prepared[2], mount_ensure=fail, mount_matches=mount.matches,
                        references=lambda *_a, **_kw: [])
    assert (source/'b').read_bytes() == b'world'
    assert run(prepared, mount)['state'] == 'completed'


def test_changed_e_destination_refuses_c_retirement(prepared):
    source, stage, _ = prepared
    (stage/'data/b').write_bytes(b'alter')
    with pytest.raises(SnapshotError, match='destination generation changed'):
        run(prepared, FakeMount())
    assert (source/'b').read_bytes() == b'world'


def test_handoff_resumes_after_source_rename_before_journal(prepared, monkeypatch):
    source, stage, digest = prepared
    mount = FakeMount()
    rename = handoff.rename_without_overwrite
    def interrupt(first, second):
        rename(first, second)
        if first == source:
            raise InterruptedError('stop after root rename')
    monkeypatch.setattr(handoff, 'rename_without_overwrite', interrupt)
    with pytest.raises(InterruptedError):
        run(prepared, mount)
    monkeypatch.setattr(handoff, 'rename_without_overwrite', rename)
    assert not source.exists() and source.with_name('.source.pre-e-'+digest[:16]).exists()
    assert handoff.recover_mount(source, stage, digest, mount_ensure=mount.ensure, mount_matches=mount.matches)
    assert source.with_name('.source.pre-e-'+digest[:16]).exists()
    assert (source.with_name('.source.pre-e-'+digest[:16])/'b').read_bytes() == b'world'
    assert run(prepared, mount)['state'] == 'completed'
    assert (stage/'data/b').read_bytes() == b'world'


def test_handoff_resumes_after_old_root_removed_before_final_journal(prepared, monkeypatch):
    mount = FakeMount()
    save = handoff.atomic_write_json
    def interrupt(path, value, **kwargs):
        if value.get('state') == 'completed':
            raise InterruptedError('stop after old root removal')
        return save(path, value, **kwargs)
    monkeypatch.setattr(handoff, 'atomic_write_json', interrupt)
    with pytest.raises(InterruptedError):
        run(prepared, mount)
    monkeypatch.setattr(handoff, 'atomic_write_json', save)
    assert run(prepared, mount)['state'] == 'completed'


def test_boot_recovery_does_not_move_or_delete_unstarted_source(prepared):
    source, stage, digest = prepared
    mount = FakeMount()
    assert not handoff.recover_mount(source, stage, digest, mount_ensure=mount.ensure, mount_matches=mount.matches)
    assert (source/'b').read_bytes() == b'world' and not mount.bound

from contextlib import ExitStack
import fcntl
import json
import os
from types import SimpleNamespace

import pytest

from stockagent.data_sync import workspace_relocation as relocation, workspace_source_handoff as handoff
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_bybit_registration_pins_every_reviewed_writer_and_keeps_recovery(tmp_path, monkeypatch):
    root, control = tmp_path / 'repo', tmp_path / 'control'
    root.mkdir(); control.mkdir()
    (root / 'writer.py').write_text('first')
    monkeypatch.setattr(handoff, 'ROOT', root)
    monkeypatch.setattr(handoff, 'CONTROL', control)
    monkeypatch.setattr(handoff, 'BYBIT_WRITER_GUARD_FILES', ('writer.py',))
    assert handoff.BYBIT_NAME not in handoff.registered_names()
    marker = control / 'bybit-enrollment.json'
    marker.write_text(json.dumps(handoff.bybit_enrollment_value())); marker.chmod(0o600)
    assert handoff.BYBIT_NAME in handoff.registered_names()
    handoff._validate_bybit_enrollment()
    (root / 'writer.py').write_text('changed')
    with pytest.raises(SnapshotError, match='writer guard differs'):
        handoff._validate_bybit_enrollment()
    marker.unlink()
    (control / 'bybit-market.json').write_text('{}')
    assert handoff.BYBIT_NAME in handoff.registered_names()


def test_bybit_handoff_fences_original_owner_and_existing_grains(tmp_path, monkeypatch):
    monkeypatch.setattr(handoff, 'ROOT', tmp_path)
    monkeypatch.setattr(handoff, '_scope', lambda: SimpleNamespace(name=handoff.BYBIT_NAME))
    global_lock = tmp_path / 'artifacts/daily_downloader/bybit_source_publish.lock'
    global_lock.parent.mkdir(parents=True)
    global_lock.touch()
    roots = [tmp_path / 'C', tmp_path / 'E']
    for root in roots:
        for relative in ('1m/.download.lock', 'daily/.download.lock'):
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
    with ExitStack() as stack:
        handoff._lock_writer_roots(stack, *roots)
        for path in [global_lock, *(root / part for root in roots for part in
                                  ('.download.lock', '1m/.download.lock', 'daily/.download.lock'))]:
            with path.open('a') as other, pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert all(not (root / '1m/.quality_priority.lock').exists() for root in roots)
    (roots[0] / '1m/.quality_priority.lock').touch()
    with ExitStack() as stack, pytest.raises(SnapshotError, match='grain lock namespace differs'):
        handoff._lock_writer_roots(stack, *roots)


def test_active_bybit_is_queued_for_sealing_without_claiming_d_or_nas(tmp_path):
    from scripts import run_handoff_index_archive_queue as queue

    def runner(command, **_kwargs):
        assert command[-2:] == [handoff.BYBIT_NAME, '--retire']
        result = {'contract': queue.cli.CONTRACT, 'dataset': 'bybit',
                  'state': 'C_payload_retired_root_and_locks_preserved', 'C_payload_deleted': True,
                  'prepared_sha256': 'a' * 64, 'C_retirement': {
                      'state': 'C_payload_retired_root_and_lock_preserved', 'prepared_sha256': 'a' * 64}}
        (tmp_path / 'bybit.status.json').write_text(json.dumps(result))
        return SimpleNamespace(returncode=0, stderr='')
    rows = queue.run_once(tmp_path, now=lambda: 1000, runner=runner, code_sha='test',
                          ready=lambda name: name == handoff.BYBIT_NAME)
    row = next(row for row in rows if row['source'] == handoff.BYBIT_NAME)
    assert row['state'] == 'completed'
    status = json.loads((tmp_path / 'bybit.status.json').read_text())
    assert not status.get('D_archive_committed', False) and not status.get('NAS_accepted', False)


def test_bybit_namespace_requires_its_exact_physical_root_and_marker(tmp_path, monkeypatch):
    from scripts import workspace_source_handoff_namespace as namespace

    root = tmp_path / 'repo'
    target = root / 'service_data/stockagent-live/data_bybit'
    target.mkdir(parents=True)
    marker = target / 'download_summary.json'
    marker.write_text('{"generation":1}')
    monkeypatch.setattr(namespace.common, 'mounts', lambda: {str(target): {}})
    tree, proc = os.open(target, os.O_RDONLY | os.O_DIRECTORY), os.open('/proc', os.O_RDONLY | os.O_DIRECTORY)
    try:
        request = {'repository_root': str(root), 'target': str(target),
                   'repository_identity': namespace.common.identity(root), 'proc_fd': proc,
                   'tree_fd': tree, 'destination_identity': namespace.common.identity(target),
                   'isolated_probe': True, 'marker_relative': 'download_summary.json',
                   'marker_sha256': relocation.digest(marker, no_follow=True)}
        assert namespace.attach(request)['verified']
        with pytest.raises(RuntimeError, match='marker scope differs'):
            namespace.attach({**request, 'marker_relative': '../elsewhere'})
        marker.write_text('{"generation":2}')
        with pytest.raises(RuntimeError, match='marker readback differs'):
            namespace.attach(request)
    finally:
        os.close(tree); os.close(proc)

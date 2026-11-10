"""Fenced OKX handoff setup; no network, live mounts, or source deletion."""
from contextlib import ExitStack
import fcntl
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from stockagent.data_sync import workspace_relocation as relocation
from stockagent.data_sync import workspace_source_handoff as handoff
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_okx_registration_requires_pinned_operator_enrollment_and_preserves_recovery(tmp_path, monkeypatch):
    monkeypatch.setattr(handoff, 'CONTROL', tmp_path)
    assert handoff.OKX_NAME not in handoff.registered_names()
    body = {'contract': 'stockagent.okx_source_handoff_enrollment.v1', 'name': handoff.OKX_NAME,
            'source': 'data_okx', 'marker': '1m/download_summary.json', 'preserve_c_original': True}
    body['identity_sha256'] = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    marker = tmp_path / 'okx-enrollment.json'
    marker.write_text(json.dumps(body)); marker.chmod(0o600)
    assert handoff.OKX_NAME in handoff.registered_names()
    handoff._validate_okx_enrollment()
    body['source'] = 'another-source'
    marker.write_text(json.dumps(body))
    assert handoff.OKX_NAME in handoff.registered_names()  # retain guard, isolate this failure
    with pytest.raises(SnapshotError, match='identity differs'):
        handoff._validate_okx_enrollment()
    marker.unlink()
    (tmp_path / 'okx.json').write_text('{}')
    assert handoff.OKX_NAME in handoff.registered_names()  # previously active aliases remain guarded


def test_okx_handoff_fences_all_writer_and_quality_locks(tmp_path, monkeypatch):
    monkeypatch.setattr(handoff, '_scope', lambda: SimpleNamespace(name=handoff.OKX_NAME))
    roots = [tmp_path / 'C', tmp_path / 'E']
    relatives = ('1m/.download.lock', 'daily/.download.lock', '1m/.quality_priority.lock')
    for root in roots:
        for relative in relatives:
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
    with ExitStack() as stack:
        handoff._lock_writer_roots(stack, *roots)
        for root in roots:
            for relative in ('.download.lock', *relatives):
                with (root / relative).open('a') as competitor:
                    with pytest.raises(BlockingIOError):
                        fcntl.flock(competitor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    with (roots[1] / '1m/.quality_priority.lock').open('a') as competitor:
        fcntl.flock(competitor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with ExitStack() as stack, pytest.raises(BlockingIOError):
            handoff._lock_writer_roots(stack, *roots)


def test_quality_owner_rejects_a_changed_lock_inode(tmp_path, monkeypatch):
    monkeypatch.setattr(handoff, 'guard_path', lambda _: None)
    path = tmp_path / '.quality_priority.lock'
    path.touch()
    with path.open('a') as handle:
        handoff.guard_locked_path(tmp_path, handle, lock_name='.quality_priority.lock')
        path.rename(tmp_path / 'old-lock')
        path.touch()
        with pytest.raises(SnapshotError, match='lock inode changed'):
            handoff.guard_locked_path(tmp_path, handle, lock_name='.quality_priority.lock')


def test_okx_collector_checks_workspace_before_creating_any_output(tmp_path, monkeypatch):
    from downloader import download_okx_perp_daily as collector
    output = tmp_path / 'missing-okx' / '1m'
    monkeypatch.setattr(collector, 'parse_args', lambda: SimpleNamespace(
        output_dir=output, tail_only=False, refresh=False, mode='incremental', lock_timeout_seconds=0))
    def unavailable(_path):
        raise SnapshotError('E workspace unavailable')
    monkeypatch.setattr(handoff, 'guard_path', unavailable)
    with pytest.raises(SnapshotError, match='E workspace unavailable'):
        collector.main()
    assert not output.exists()


def test_okx_namespace_binds_only_its_nested_fixed_marker(tmp_path, monkeypatch):
    from scripts import workspace_source_handoff_namespace as namespace
    root = tmp_path / 'repo'; root.mkdir()
    target = root / 'data_okx'; (target / '1m').mkdir(parents=True)
    marker = target / '1m/download_summary.json'; marker.write_text('{"snapshot":1}')
    monkeypatch.setattr(namespace.common, 'mounts', lambda: {str(target): {}})
    fd = os.open(target, os.O_RDONLY | os.O_DIRECTORY)
    proc = os.open('/proc', os.O_RDONLY | os.O_DIRECTORY)
    try:
        request = {'repository_root': str(root), 'target': str(target),
                   'repository_identity': namespace.common.identity(root), 'proc_fd': proc,
                   'tree_fd': fd, 'destination_identity': namespace.common.identity(target),
                   'isolated_probe': True, 'marker_relative': '1m/download_summary.json',
                   'marker_sha256': relocation.digest(marker, no_follow=True)}
        assert namespace.attach(request)['verified']
        with pytest.raises(RuntimeError, match='marker scope differs'):
            namespace.attach({**request, 'marker_relative': '../elsewhere'})
        marker.write_text('{"snapshot":2}')
        with pytest.raises(RuntimeError, match='marker readback differs'):
            namespace.attach(request)
    finally:
        os.close(fd); os.close(proc)

from __future__ import annotations

import fcntl
from pathlib import Path

import pytest

from downloader.dataset_lock import DatasetLockTimeout
from scripts import publish_data_releases as publisher
from stockagent.data_sync import taifex_workspace_guard as guard, workspace_volume


@pytest.fixture
def roots(tmp_path, monkeypatch):
    source = tmp_path/'data_taifex_public_history'
    source.mkdir()
    monkeypatch.setattr(guard, 'SOURCE', source)
    monkeypatch.setattr(guard, 'WORKSPACE_ROOT', tmp_path/'E')
    monkeypatch.setattr(publisher, 'REPO_ROOT', tmp_path)
    monkeypatch.setattr(workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path/'E')})
    return source


def test_unrelated_output_does_not_create_root_lock(roots, tmp_path):
    with guard.writer_guard(tmp_path/'elsewhere', timeout_seconds=0):
        pass
    assert not (roots/'.download.lock').exists()


def test_both_collectors_share_the_lock_and_handoff_is_exclusive(roots):
    with guard.writer_guard(roots, timeout_seconds=0):
        with guard.writer_guard(roots/'rules', timeout_seconds=0):
            with (roots/'.download.lock').open('r+') as handoff:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(handoff.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    with (roots/'.download.lock').open('r+') as handoff:
        fcntl.flock(handoff.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(DatasetLockTimeout):
            with guard.writer_guard(roots/'rules', timeout_seconds=0):
                pass


@pytest.mark.parametrize('dataset,tail', [('taifex-public-history', ''), ('taifex-rule-history', 'rules')])
def test_publication_waits_for_active_collector(roots, dataset, tail):
    source = roots/tail if tail else roots
    source.mkdir(exist_ok=True)
    entry = {'dataset': dataset, 'source': str(source)}
    with guard.writer_guard(source, timeout_seconds=0):
        with pytest.raises(publisher.SourceCoordinationLockTimeout):
            with publisher._publication_source_locks(entry, wait_seconds=0):
                pass


def test_direct_e_destination_uses_the_same_guard(roots, tmp_path):
    destination = tmp_path/'E/downloads'/guard.WORKSPACE_NAME/roots.name/'rules'
    assert guard._uses_source(destination)
    with guard.writer_guard(destination, timeout_seconds=0):
        assert (roots/'.download.lock').is_file()


def test_alias_to_the_source_cannot_bypass_the_writer_lock(roots, tmp_path):
    alias = tmp_path/'alias'
    alias.symlink_to(roots, target_is_directory=True)
    assert guard._uses_source(alias/'rules')
    with guard.writer_guard(alias/'rules', timeout_seconds=0):
        with (roots/'.download.lock').open('r+') as handoff:
            with pytest.raises(BlockingIOError):
                fcntl.flock(handoff.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_cli_guard_recognizes_output_flag_without_abbreviation(roots):
    with guard.cli_writer_guard(['--output-dir', str(roots/'rules')], default_output='unrelated'):
        assert (roots/'.download.lock').is_file()

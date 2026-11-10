from __future__ import annotations

from argparse import Namespace
from datetime import date
from pathlib import Path

import pytest

from scripts import build_shioaji_tw_minute_dataset as builder
from scripts import publish_data_releases as publisher
from scripts import reconcile_tw_stock_minute_day as reconcile
from stockagent.data_sync import workspace_source_handoff as handoff


def test_builder_and_repair_share_root_owner(tmp_path, monkeypatch):
    root = tmp_path / 'research_dataset'
    monkeypatch.setattr(builder, 'parse_args', lambda: Namespace(output_root=root))
    calls = []
    monkeypatch.setattr(builder, '_build_with_owner', lambda args: calls.append(args))
    monkeypatch.setattr(reconcile, '_reconcile_with_owner',
                        lambda *args, **kwargs: {'repair': kwargs['repair']})
    with handoff.guarded_source_writer(root):
        with pytest.raises(BlockingIOError):
            builder.main()
        with pytest.raises(BlockingIOError):
            reconcile.reconcile(tmp_path, root, date(2020, 3, 2), repair=True)
        assert reconcile.reconcile(tmp_path, root, date(2020, 3, 2)) == {'repair': False}
    builder.main()
    assert len(calls) == 1
    assert reconcile.reconcile(tmp_path, root, date(2020, 3, 2), repair=True) == {'repair': True}


def test_failed_handoff_guard_releases_minute_writer_lock(tmp_path, monkeypatch):
    root = tmp_path / 'research_dataset'
    def reject(_root, _handle):
        raise RuntimeError('source generation changed')
    with monkeypatch.context() as scoped:
        scoped.setattr(handoff, 'guard_locked_path', reject)
        with pytest.raises(RuntimeError, match='generation changed'):
            with handoff.guarded_source_writer(root):
                pytest.fail('a changed source must not be written')
    with handoff.guarded_source_writer(root):
        assert (root / '.download.lock').is_file()


def test_minute_publisher_and_direct_writer_share_owner(tmp_path, monkeypatch):
    monkeypatch.setattr(publisher, 'REPO_ROOT', tmp_path)
    root = tmp_path / 'data_tw_minute/research_dataset'
    root.mkdir(parents=True)
    entry = {'dataset': 'tw-minute-train', 'source': 'data_tw_minute/research_dataset',
             'source_coordination_lock': 'artifacts/data_repair/shioaji_minute_full/runner.lock'}
    with publisher._publication_source_locks(entry, wait_seconds=0):
        with pytest.raises(BlockingIOError):
            with handoff.guarded_source_writer(root):
                pytest.fail('a publisher owns this generation')
    with handoff.guarded_source_writer(root):
        with pytest.raises(publisher.SourceCoordinationLockTimeout):
            with publisher._publication_source_locks(entry, wait_seconds=0):
                pytest.fail('a direct writer owns this generation')
    assert publisher._native_source_lock_entries({**entry, 'source': str(tmp_path / 'other')}) == []

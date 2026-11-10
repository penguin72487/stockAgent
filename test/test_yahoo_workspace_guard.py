from __future__ import annotations

import fcntl
import os
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pytest

from downloader.dataset_lock import DatasetLockPathChanged, DatasetLockTimeout
from stockagent.data_sync import yahoo_workspace_guard as guard


@pytest.fixture
def yahoo(tmp_path, monkeypatch):
    root = tmp_path/'data_yahoo'
    root.mkdir()
    monkeypatch.setattr(guard, 'YAHOO_ROOT', root)
    return root


@pytest.fixture
def pepperstone(tmp_path, monkeypatch):
    root = tmp_path/'data_peperstone'
    root.mkdir()
    monkeypatch.setattr(guard, 'PEPPERSTONE_ROOT', root)
    return root


def test_unrelated_workspace_does_not_create_a_yahoo_lock(yahoo, tmp_path):
    with guard.yahoo_writer_guard(tmp_path/'other'):
        pass
    assert not (yahoo/'.download.lock').exists()


def test_pepperstone_and_yahoo_paths_share_independent_handoff_locks(yahoo, pepperstone):
    with guard.yahoo_writer_guard(yahoo/'forex', pepperstone/'fores'):
        for root in (yahoo, pepperstone):
            with (root/'.download.lock').open('r+') as handoff:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(handoff.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_pepperstone_publisher_waits_for_the_existing_writer(pepperstone, tmp_path, monkeypatch):
    from scripts import publish_data_releases as publisher
    monkeypatch.setattr(publisher, 'REPO_ROOT', tmp_path)
    entry = {'dataset':'forex-pepperstone', 'source':str(pepperstone)}
    with guard.yahoo_writer_guard(pepperstone/'fores'):
        with pytest.raises(publisher.SourceCoordinationLockTimeout):
            with publisher._publication_source_locks(entry, wait_seconds=0):
                pytest.fail('publisher must wait for an active source writer')
    with publisher._publication_source_locks(entry, wait_seconds=0):
        with pytest.raises(DatasetLockTimeout):
            with guard.yahoo_writer_guard(pepperstone/'fores', timeout_seconds=0):
                pytest.fail('writer must wait for the source publisher')
    assert publisher._native_source_lock_entries({**entry, 'source':str(tmp_path/'other')}) == []


def test_pepperstone_entrypoint_waits_before_group_writes(pepperstone, monkeypatch):
    from downloader import download_pepperstone as module
    monkeypatch.setattr(module, 'parse_args', lambda: SimpleNamespace(output_root=pepperstone))
    entered, errors = threading.Event(), []
    monkeypatch.setattr(module, '_run_with_workspace_guard', lambda *_a: entered.set())
    with (pepperstone/'.download.lock').open('a+') as handoff:
        fcntl.flock(handoff.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        def run():
            try:
                module.main()
            except BaseException as error:
                errors.append(error)
        thread = threading.Thread(target=run)
        thread.start()
        assert not entered.wait(0.4)
        fcntl.flock(handoff.fileno(), fcntl.LOCK_UN)
        thread.join(5)
    assert not thread.is_alive() and not errors and entered.is_set()


def test_parallel_writers_share_lock_but_handoff_is_exclusive(yahoo):
    with guard.yahoo_writer_guard(yahoo/'us_stocks'):
        with guard.yahoo_writer_guard(yahoo/'forex'):
            with (yahoo/'.download.lock').open('r+') as handoff:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(handoff.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    with (yahoo/'.download.lock').open('r+') as handoff:
        fcntl.flock(handoff.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_writer_waits_for_handoff_instead_of_using_unlocked_path(yahoo):
    path = yahoo/'.download.lock'
    with path.open('a+') as handoff:
        fcntl.flock(handoff.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(DatasetLockTimeout):
            with guard.yahoo_writer_guard(yahoo/'us_stocks', timeout_seconds=0):
                pytest.fail('must not enter while handoff owns the lock')


def test_waiting_writer_rejects_a_replaced_workspace_inode(yahoo, tmp_path):
    path = yahoo/'.download.lock'
    errors = []
    with path.open('a+') as handoff:
        fcntl.flock(handoff.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        original = os.fstat(handoff.fileno())
        def writer():
            try:
                with guard.yahoo_writer_guard(yahoo/'us_stocks', timeout_seconds=5):
                    pytest.fail('a lock on the old root must not authorize the new root')
            except BaseException as error:
                errors.append(error)
        thread = threading.Thread(target=writer)
        thread.start()
        deadline = time.monotonic()+3
        while time.monotonic() < deadline:
            matches = 0
            for fd in Path('/proc/self/fd').iterdir():
                try:
                    info = fd.stat()
                except OSError:
                    continue
                matches += (info.st_dev, info.st_ino) == (original.st_dev, original.st_ino)
            if matches >= 2:
                break
            time.sleep(0.01)
        else:
            pytest.fail('writer did not open the original locked inode')
        yahoo.rename(tmp_path/'old-yahoo')
        yahoo.mkdir()
        (yahoo/'.download.lock').touch()
        fcntl.flock(handoff.fileno(), fcntl.LOCK_UN)
        thread.join(5)
    assert not thread.is_alive()
    assert len(errors) == 1 and isinstance(errors[0], DatasetLockPathChanged)


def test_redirected_lock_is_rejected(yahoo, tmp_path):
    (yahoo/'.download.lock').symlink_to(tmp_path/'other')
    with pytest.raises(OSError):
        with guard.yahoo_writer_guard(yahoo/'forex'):
            pytest.fail('must not follow redirected lock')


@pytest.mark.parametrize('which', ['yahoo', 'alpaca', 'backfill', 'materialize', 'frankfurter'])
def test_real_entrypoints_wait_for_handoff_before_work(yahoo, monkeypatch, which):
    if which == 'yahoo':
        from downloader import download_yahoo_ohlcv as module
        args = SimpleNamespace(output_root=str(yahoo), output_dir=None)
        work = '_run_with_workspace_guard'
    elif which == 'alpaca':
        from downloader import download_alpaca_us_ohlcv as module
        args = SimpleNamespace(output_root=yahoo)
        work = '_run_with_workspace_guard'
    elif which == 'backfill':
        from downloader import backfill_tw_public_to_yahoo as module
        args = SimpleNamespace(symbols_root=str(yahoo/'tw_stocks'), summary_path=None, report_path=None)
        work = '_run_with_workspace_guard'
    elif which == 'materialize':
        from downloader import materialize_ohlcv_daily as module
        args = SimpleNamespace(input_dir=str(yahoo/'crypto/1m'), output_dir=str(yahoo/'crypto/daily'),
                               lock_timeout_seconds=5)
        work = '_run_with_workspace_guard'
    else:
        from downloader import download_forex_frankfurter as module
        args = SimpleNamespace(output_dir=str(yahoo/'forex'), symbols_file=None, lock_timeout_seconds=5)
        work = '_run_download'
    monkeypatch.setattr(module, 'parse_args', lambda: args)
    entered, errors = threading.Event(), []
    monkeypatch.setattr(module, work, lambda *_a, **_k: entered.set())
    with (yahoo/'.download.lock').open('a+') as handoff:
        fcntl.flock(handoff.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        def run():
            try:
                module.main()
            except BaseException as error:
                errors.append(error)
        thread = threading.Thread(target=run)
        thread.start()
        assert not entered.wait(0.4)
        fcntl.flock(handoff.fileno(), fcntl.LOCK_UN)
        thread.join(5)
    assert not thread.is_alive() and not errors and entered.is_set()

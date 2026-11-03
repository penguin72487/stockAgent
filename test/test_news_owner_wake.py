"""Synthetic local kernel events; no publisher requests or retained bodies."""
from collections import Counter
import errno
import os
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_bytes
from downloader.news_fulltext_wake import OwnerMailboxWake, OwnerStopEvent
from test_news_fulltext import config


class Metrics:
    def __init__(self):
        self.counts = Counter()

    def count(self, provider, event):
        self.counts[provider, event] += 1


@pytest.fixture
def wake(tmp_path):
    queue = SimpleNamespace(root=tmp_path, metrics=Metrics(), native_candidate_schema_ready=False)
    with OwnerMailboxWake(queue, OwnerStopEvent()) as value:
        if value.backend != 'linux_inotify':
            pytest.skip('Linux inotify is not available in this environment')
        yield value


def inbox(wake, kind='candidate'):
    prefix = wake.queue.root/'reviewed_repairs'
    return prefix/('candidate_diagnostics/pending' if kind == 'candidate' else 'pending')


def seal(path):
    atomic_write_bytes(path, b'{"synthetic":true}', durable=True, mode=0o600)


def test_stop_event_retains_event_behavior_and_unsubscribes():
    stop = OwnerStopEvent(); calls = []
    remove = stop.subscribe(lambda: calls.append(1))
    stop.set()
    assert stop.is_set() and stop.wait(0) and calls == [1]
    remove(); stop.clear(); stop.set()
    assert calls == [1] and stop.is_set()
    remove()


@pytest.mark.parametrize('kind', ['candidate', 'repair'])
def test_atomic_private_input_wakes_without_another_thread_or_owner(wake, kind):
    threads = threading.active_count()
    path = inbox(wake, kind)/('a'*64+'.json')
    seal(path)
    started = time.monotonic()
    assert wake.wait(2)
    assert time.monotonic()-started < 1
    assert threading.active_count() == threads
    assert wake.queue.metrics.counts['local_repairs', 'owner_mailbox_input_wakeups'] == 1
    assert path.read_bytes() == b'{"synthetic":true}'  # A hint neither consumes nor validates.


def test_non_json_temporary_write_is_not_a_complete_request(wake):
    complete = threading.Event()
    def writer():
        seal(inbox(wake)/'private.tmp')
        time.sleep(.05)
        seal(inbox(wake)/('b'*64+'.json'))
        complete.set()
    thread = threading.Thread(target=writer); thread.start()
    try:
        assert wake.wait(2) and complete.wait(1)
    finally:
        thread.join(2)
    assert not thread.is_alive()


def test_consumed_hint_does_not_restart_idle_cycle_or_spin(wake):
    path = inbox(wake)/('a'*64+'.json')
    seal(path); path.unlink()
    started = time.monotonic(); cpu = time.process_time()
    assert not wake.wait(.08)
    assert time.monotonic()-started >= .07
    assert time.process_time()-cpu < .05
    assert not wake.queue.metrics.counts['local_repairs', 'owner_mailbox_input_wakeups']


def test_unarchived_repair_is_not_a_level_triggered_busy_loop(wake):
    seal(inbox(wake, 'repair')/('c'*64+'.json'))
    assert wake.wait(.5)
    assert not wake.has_pending()  # A completed-but-unarchived job is not new work.
    assert not wake.wait(.04)
    assert wake.queue.metrics.counts['local_repairs', 'owner_mailbox_input_wakeups'] == 1


@pytest.mark.parametrize('kind', ['candidate', 'repair'])
def test_temporary_discovery_error_after_event_does_not_lose_wake(wake, monkeypatch, kind):
    from downloader import news_fulltext_candidate_diagnostics as candidates
    from downloader import news_fulltext_repairs as repairs
    module = candidates if kind == 'candidate' else repairs
    original = module._pending
    failed = []

    def transient(directory):
        if not failed:
            failed.append(True)
            raise OSError(errno.EIO, 'temporary inbox scan failure')
        return original(directory)

    seal(inbox(wake, kind)/('d'*64+'.json'))
    monkeypatch.setattr(module, '_pending', transient)
    assert wake.wait(.15)
    assert failed


def test_mailbox_scan_retries_back_off_per_inbox_and_clear(wake):
    from downloader.news_fulltext_wake import (clear_mailbox_scan_retry, defer_mailbox_scan_retry,
        mailbox_scan_retry_delay, mailbox_scan_retry_ready)
    delays = []
    for _ in range(8):
        defer_mailbox_scan_retry(wake.queue, 'candidate')
        delays.append(mailbox_scan_retry_delay(wake.queue, 10))
    assert delays[0] == pytest.approx(.25, abs=.03)
    assert delays[1] == pytest.approx(.5, abs=.03)
    assert delays[-1] == pytest.approx(5, abs=.03)
    assert not mailbox_scan_retry_ready(wake.queue, 'candidate')
    assert mailbox_scan_retry_ready(wake.queue, 'repair')
    defer_mailbox_scan_retry(wake.queue, 'repair')
    clear_mailbox_scan_retry(wake.queue, 'candidate')
    assert mailbox_scan_retry_delay(wake.queue, 10) == pytest.approx(.25, abs=.03)
    clear_mailbox_scan_retry(wake.queue, 'repair')
    assert mailbox_scan_retry_delay(wake.queue, 10) == 10


@pytest.mark.parametrize('kind', ['candidate', 'repair'])
@pytest.mark.parametrize('pending', [False, True])
def test_mailbox_hint_does_not_reset_failed_pump_backoff(wake, kind, pending):
    from downloader.news_fulltext_wake import defer_mailbox_scan_retry
    if pending:
        seal(inbox(wake, kind)/('e'*64+'.json'))
    defer_mailbox_scan_retry(wake.queue, kind)
    defer_mailbox_scan_retry(wake.queue, kind)
    wake.queue._mailbox_scan_retries[kind] = (time.monotonic()-1, 2)
    assert wake.has_pending(include_repairs=True) is pending
    assert wake.queue._mailbox_scan_retries[kind][1] == 2
    defer_mailbox_scan_retry(wake.queue, kind)
    assert wake.queue._mailbox_scan_retries[kind][1] == 3


@pytest.mark.parametrize('kind', ['candidate', 'repair'])
def test_service_retries_mailbox_after_event_and_pump_errors(config, tmp_path, monkeypatch, kind):
    from downloader import news_fulltext as main
    from downloader import news_fulltext_candidate_diagnostics as candidates
    from downloader import news_fulltext_repairs as repairs
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_fulltext import http_fixture

    module = candidates if kind == 'candidate' else repairs
    pump_name = 'pump_candidates' if kind == 'candidate' else 'consume_repairs'
    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    waiting = threading.Event()
    drained, checking, failures, errors = [False], [False], [], []
    original_wait, original_drain = OwnerMailboxWake.wait, OwnerMailboxWake._drain
    original_has, original_pump, original_pending = OwnerMailboxWake.has_pending, getattr(module, pump_name), module._pending

    def wait(self, seconds, *, enabled=True):
        if enabled:
            waiting.set()
        return original_wait(self, seconds, enabled=enabled)

    def drain(self):
        relevant = original_drain(self)
        if relevant:
            drained[0] = True
        return relevant

    def scoped(function):
        def call(*args, **kwargs):
            checking[0] = True
            try:
                return function(*args, **kwargs)
            finally:
                checking[0] = False
        return call

    def pending(directory):
        if drained[0] and checking[0] and len(failures) < 2:
            failures.append(time.monotonic())
            raise OSError(errno.EIO, 'temporary mailbox discovery failure')
        return original_pending(directory)

    def writer():
        try:
            assert waiting.wait(2)
            seal(inbox(SimpleNamespace(queue=queue), kind)/('e'*64+'.json'))
        except BaseException as error:
            errors.append(error)
            http.stop.set()

    monkeypatch.setattr(OwnerMailboxWake, 'wait', wait)
    monkeypatch.setattr(OwnerMailboxWake, '_drain', drain)
    monkeypatch.setattr(OwnerMailboxWake, 'has_pending', scoped(original_has))
    monkeypatch.setattr(module, pump_name, scoped(original_pump))
    monkeypatch.setattr(module, '_pending', pending)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    thread = threading.Thread(target=writer)
    thread.start()
    try:
        main.run_service(queue, http, config, max_articles=0, max_discovery_requests=0, duration_seconds=2)
        thread.join(2)
        assert not thread.is_alive() and not errors and len(failures) == 2
        assert failures[1] - failures[0] >= .1
        rejected = list((tmp_path/'reviewed_repairs'/'rejected').glob('*.input'))
        assert len(rejected) == 1 and rejected[0].read_bytes() == b'{"synthetic":true}'
        assert not session.calls and not getattr(queue, '_mailbox_scan_retries', {})
    finally:
        http.stop.set()
        thread.join(2)
        http.close()
        queue.close()


def test_already_registered_paused_candidate_does_not_wake_again(wake, monkeypatch):
    from downloader import news_fulltext_candidate_diagnostics as jobs
    monkeypatch.setattr(jobs, 'has_unadmitted_candidates', lambda queue, **kwargs: False)
    seal(inbox(wake)/('c'*64+'.json'))
    assert not wake.wait(.04)
    assert not wake.queue.metrics.counts['local_repairs', 'owner_mailbox_input_wakeups']


def test_waiting_disk_keeps_original_wait_and_does_not_dispatch(wake):
    path = inbox(wake)/('a'*64+'.json')
    seal(path)
    started = time.monotonic()
    assert not wake.wait(.04, enabled=False)
    assert time.monotonic()-started >= .03
    assert wake.wait(.5) and path.exists()


def test_stop_immediately_interrupts_long_kernel_wait(wake):
    thread = threading.Timer(.03, wake.stop.set); thread.start()
    started = time.monotonic()
    try:
        assert wake.wait(15)
    finally:
        thread.join(2)
    assert time.monotonic()-started < 1


def test_descriptor_lifecycle_does_not_leak_or_notify_reused_fd(wake):
    descriptors = list(wake._fds)
    wake.close(); wake.close(); wake.stop.set()
    assert not wake.stop._wakers and wake._stop_write is None
    for fd in descriptors:
        with pytest.raises(OSError):
            os.fstat(fd)


def test_unsupported_event_retains_old_interruptible_wait(tmp_path):
    stop = threading.Event()
    queue = SimpleNamespace(root=tmp_path, metrics=Metrics())
    with OwnerMailboxWake(queue, stop) as wake:
        assert wake.backend == 'legacy_stop_wait'
        assert not wake.wait(.001)
        stop.set()
        assert wake.wait(30)


def test_unavailable_kernel_watch_falls_back_without_losing_stop(tmp_path, monkeypatch):
    from downloader import news_fulltext_wake as module
    def unavailable(*a, **k):
        raise OSError(errno.ENOSPC, 'synthetic watch limit')
    monkeypatch.setattr(module.ctypes, 'CDLL', unavailable)
    stop = OwnerStopEvent(); queue = SimpleNamespace(root=tmp_path, metrics=Metrics())
    with OwnerMailboxWake(queue, stop) as wake:
        assert wake.backend == 'legacy_stop_wait' and not wake._fds
        stop.set(); assert wake.wait(30)


def test_unsafe_private_parent_is_not_followed(tmp_path):
    external = tmp_path/'external'; external.mkdir()
    root = tmp_path/'root'; root.mkdir()
    (root/'reviewed_repairs').symlink_to(external, target_is_directory=True)
    queue = SimpleNamespace(root=root, metrics=Metrics())
    with OwnerMailboxWake(queue, OwnerStopEvent()) as wake:
        assert wake.backend == 'legacy_stop_wait' and not wake.has_pending()
        assert not list(external.iterdir())


def test_watch_loss_keeps_the_original_wait_instead_of_spinning(wake):
    inbox(wake, 'repair').rmdir()
    assert not wake.wait(.02)
    assert wake.backend == 'legacy_stop_wait'
    assert wake.queue.metrics.counts['local_repairs', 'owner_mailbox_wake_fallback'] == 1


def test_overflow_is_only_a_rescan_hint_not_body_or_request_admission(wake, monkeypatch):
    payload = wake._EVENT.pack(-1, wake._OVERFLOW, 0, 0)
    outputs = iter((payload,))
    def read(*args):
        try:
            return next(outputs)
        except StopIteration:
            raise BlockingIOError()
    monkeypatch.setattr(os, 'read', read)
    assert wake._drain()
    assert wake.queue.metrics.counts['local_repairs', 'owner_mailbox_wake_overflow'] == 1
    assert not wake.has_pending()


@pytest.mark.parametrize('seconds', [-1, float('inf'), float('nan'), True])
def test_invalid_wait_is_rejected(wake, seconds):
    with pytest.raises(ValueError):
        wake.wait(seconds)


def test_stop_already_set_is_not_lost_during_watch_registration(tmp_path):
    stop = OwnerStopEvent(); stop.set()
    queue = SimpleNamespace(root=tmp_path, metrics=Metrics())
    with OwnerMailboxWake(queue, stop) as wake:
        assert wake.wait(10)


def test_idle_owner_admits_new_input_before_old_fifteen_second_poll(tmp_path, monkeypatch):
    from downloader import news_fulltext_candidate_diagnostics as jobs
    from downloader.news_fulltext import run_service
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_candidate_diagnostics import settings as fixture, fail_head, submit, URL, description_defect_page
    from test_news_fulltext import http_fixture, FakeResponse
    config = fixture.__wrapped__(tmp_path)
    http, session = http_fixture(config, tmp_path, {
        'https://news.videoland.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(description_defect_page()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    idle = threading.Event(); errors = []; request = []
    original_status, original_archive = queue.status, jobs._archive
    def status(**kwargs):
        result = original_status(**kwargs)
        if kwargs.get('activity') == 'waiting_next_due':
            idle.set()
        return result
    def archive(*args, **kwargs):
        result = original_archive(*args, **kwargs)
        http.stop.set()
        return result
    def writer():
        try:
            assert idle.wait(2)
            time.sleep(.02)
            request.append(submit(queue, config))
        except BaseException as error:
            errors.append(error); http.stop.set()
    monkeypatch.setattr(queue, 'status', status)
    monkeypatch.setattr(jobs, '_archive', archive)
    thread = threading.Thread(target=writer)
    try:
        before = fail_head(queue)
        thread.start()
        started = time.monotonic()
        run_service(queue, http, config, max_articles=1, max_discovery_requests=0, duration_seconds=3)
        assert not errors and request and time.monotonic()-started < 2.5
        result = jobs.candidate_status(tmp_path, request[0]['request_id'])
        assert result['state'] == 'observed'
        assert queue.metrics.snapshot()['providers']['local_repairs']['counts']['owner_mailbox_input_wakeups'] >= 1
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
        assert (tmp_path/'reviewed_repairs/candidate_diagnostics/processed'/(request[0]['request_id']+'.outcome.json')).is_file()
        assert [url for url, _ in session.calls].count(URL) == 1
    finally:
        http.stop.set(); thread.join(2); queue.close(); http.close()

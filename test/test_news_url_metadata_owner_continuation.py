"""URL inbox work must keep moving without delaying other providers."""
from collections import Counter
from copy import deepcopy
import errno
from pathlib import Path
import sqlite3
from threading import Event, Thread
import time

import pytest

from downloader import news_fulltext as main
from downloader import news_fulltext_candidate_diagnostics as candidates
from downloader import news_fulltext_original_declarations as declarations
from downloader import news_fulltext_repairs as repairs
from downloader import news_fulltext_url_metadata as urls
from downloader.artifact_io import atomic_write_bytes
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from downloader.news_fulltext_wake import OwnerMailboxWake
from test_news_fulltext import FakeResponse, article_html, config, http_fixture


def test_url_inbox_continues_while_another_provider_is_inflight(config, tmp_path, monkeypatch):
    staged = deepcopy(config['providers'][0])
    staged.update(id='staged', domains=['staged.test'], enabled=False)
    config['providers'].append(staged)
    config.update(workers=1, batch_articles=1)
    all_admitted, priority_started = Event(), Event()
    timed_out = []

    class PriorityResponse(FakeResponse):
        def iter_content(self, **kwargs):
            priority_started.set()
            timed_out.append(not all_admitted.wait(1.0))
            yield self.body

    priority_url = 'https://publisher.test/story'
    pending_urls = [f'https://staged.test/story/{number}' for number in range(6)]
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              priority_url: PriorityResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    calls = Counter()
    original_urls, original_repair, original_candidates = urls.consume_urls, repairs.consume_repairs, candidates.pump_candidates

    def one_url(*args, **kwargs):
        calls['urls'] += 1
        result = original_urls(*args, limit=1, **kwargs)
        if queue.db.execute("SELECT count(*) FROM articles WHERE provider='staged'").fetchone()[0] == len(pending_urls):
            all_admitted.set()
        return result

    def repair(*args, **kwargs):
        calls['repairs'] += 1
        return original_repair(*args, **kwargs)

    def candidate(*args, **kwargs):
        calls['candidates'] += 1
        return original_candidates(*args, **kwargs)

    monkeypatch.setattr(urls, 'consume_urls', one_url)
    monkeypatch.setattr(repairs, 'consume_repairs', repair)
    monkeypatch.setattr(candidates, 'pump_candidates', candidate)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    try:
        queue.add_articles([{'url': priority_url}])
        urls.submit_urls(queue.root, queue.registry, pending_urls)
        result = main.run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                                continuous_budgets=True, max_run_seconds=3, defer_final_status=True)
        assert priority_started.is_set() and timed_out == [False]
        assert all_admitted.is_set() and calls == {'urls': 6, 'repairs': 1, 'candidates': 1}
        assert queue.db.execute("SELECT count(*) FROM articles WHERE provider='staged' AND state='awaiting_authorization'").fetchone()[0] == 6
        assert result['cycle']['durably_accepted_versions'] == 1 and verify_bodies(tmp_path)['accepted']
        assert {url for url, _ in session.calls} == set(routes)
    finally:
        all_admitted.set()
        http.close()
        queue.close()


@pytest.mark.parametrize('failure', [OSError(errno.EIO, 'temporary inbox directory failure'),
                                     ValueError('temporary inbox directory validation failure'),
                                     sqlite3.OperationalError('temporary inbox index failure')])
def test_continuation_discovery_failure_retries_while_other_provider_is_inflight(config, tmp_path, monkeypatch, failure):
    staged = deepcopy(config['providers'][0])
    staged.update(id='staged', domains=['staged.test'], enabled=False)
    config['providers'].append(staged)
    config.update(workers=1, batch_articles=1)
    admitted, priority_started = Event(), Event()
    timed_out, injected, checking = [], [False], [False]

    class PriorityResponse(FakeResponse):
        def iter_content(self, **kwargs):
            priority_started.set()
            timed_out.append(not admitted.wait(1.5))
            yield self.body

    priority_url = 'https://publisher.test/story'
    pending_urls = [f'https://staged.test/story/continuation-{number}' for number in range(6)]
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              priority_url: PriorityResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    original_consume, original_has, original_pending = urls.consume_urls, urls.has_pending, urls._pending
    attempts = []

    def pending(path):
        if checking[0] and not injected[0]:
            injected[0] = True
            raise failure
        return original_pending(path)

    def has_pending(*args, **kwargs):
        checking[0] = True
        try:
            return original_has(*args, **kwargs)
        finally:
            checking[0] = False

    def one_url(*args, **kwargs):
        attempts.append(time.monotonic())
        result = original_consume(*args, limit=1, **kwargs)
        if queue.db.execute("SELECT count(*) FROM articles WHERE provider='staged'").fetchone()[0] == len(pending_urls):
            admitted.set()
        return result

    monkeypatch.setattr(urls, '_pending', pending)
    monkeypatch.setattr(urls, 'has_pending', has_pending)
    monkeypatch.setattr(urls, 'consume_urls', one_url)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    try:
        queue.add_articles([{'url': priority_url}])
        urls.submit_urls(queue.root, queue.registry, pending_urls)
        result = main.run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                                continuous_budgets=True, max_run_seconds=3, defer_final_status=True)
        assert injected[0] and priority_started.is_set() and admitted.is_set() and timed_out == [False]
        assert len(attempts) == len(pending_urls) and attempts[1] - attempts[0] >= .1
        assert queue.db.execute("SELECT count(*) FROM url_metadata_items WHERE outcome='metadata_registered'").fetchone()[0] == len(pending_urls)
        assert result['cycle']['durably_accepted_versions'] == 1 and verify_bodies(tmp_path)['accepted']
        assert {url for url, _ in session.calls} == set(routes)
    finally:
        admitted.set()
        http.close()
        queue.close()


@pytest.mark.parametrize('failure', [OSError(errno.EIO, 'temporary mailbox scan failure'),
                                     ValueError('temporary mailbox scan validation failure'),
                                     sqlite3.OperationalError('temporary mailbox index failure')])
def test_mailbox_scan_failure_after_event_wakes_owner_and_recovers(config, tmp_path, monkeypatch, failure):
    staged = deepcopy(config['providers'][0])
    staged.update(id='staged', domains=['staged.test'], enabled=False)
    config['providers'].append(staged)
    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    requested = 'https://staged.test/story/mailbox-wake'
    waiting, drained, injected, checking = Event(), [False], [False], [False]
    send_errors = []
    original_wait, original_drain = OwnerMailboxWake.wait, OwnerMailboxWake._drain
    original_has, original_pending = urls.has_pending, urls._pending

    def wait(self, seconds, *, enabled=True):
        if enabled:
            waiting.set()
        return original_wait(self, seconds, enabled=enabled)

    def drain(self):
        relevant = original_drain(self)
        if relevant:
            drained[0] = True
        return relevant

    def has_pending(*args, **kwargs):
        checking[0] = True
        try:
            return original_has(*args, **kwargs)
        finally:
            checking[0] = False

    def pending(path):
        if drained[0] and checking[0] and not injected[0]:
            injected[0] = True
            raise failure
        return original_pending(path)

    def sender():
        try:
            if not waiting.wait(2):
                raise AssertionError('owner did not enter mailbox wait')
            urls.submit_urls(queue.root, queue.registry, [requested])
        except Exception as error:
            send_errors.append(error)

    monkeypatch.setattr(OwnerMailboxWake, 'wait', wait)
    monkeypatch.setattr(OwnerMailboxWake, '_drain', drain)
    monkeypatch.setattr(urls, 'has_pending', has_pending)
    monkeypatch.setattr(urls, '_pending', pending)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    worker = Thread(target=sender)
    worker.start()
    try:
        main.run_service(queue, http, config, max_articles=0, max_discovery_requests=0, duration_seconds=1.5)
        worker.join(2)
        assert not worker.is_alive() and not send_errors and injected[0]
        head = queue.db.execute('SELECT state FROM articles WHERE url=?', (requested,)).fetchone()
        assert head is not None and head[0] == 'awaiting_authorization'
        assert not urls.has_pending(queue) and not session.calls
    finally:
        http.stop.set()
        worker.join(2)
        http.close()
        queue.close()


def test_transient_inbox_read_does_not_spin_and_recovers(config, tmp_path, monkeypatch):
    staged = deepcopy(config['providers'][0])
    staged.update(id='staged', domains=['staged.test'], enabled=False)
    config['providers'].append(staged)
    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    requested = 'https://staged.test/story/retry'
    original_load = urls._load
    original_consume = urls._consume_url_inbox
    attempts = []
    in_inbox = [False]

    def transient(path):
        if in_inbox[0]:
            attempts.append(time.monotonic())
            if len(attempts) == 1:
                raise OSError(errno.EIO, 'transient private inbox read failure')
        return original_load(path)

    def consume(*args, **kwargs):
        in_inbox[0] = True
        try:
            return original_consume(*args, **kwargs)
        finally:
            in_inbox[0] = False

    monkeypatch.setattr(urls, '_load', transient)
    monkeypatch.setattr(urls, '_consume_url_inbox', consume)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    try:
        urls.submit_urls(queue.root, queue.registry, [requested])
        main.run_service(queue, http, config, max_articles=0, max_discovery_requests=0, duration_seconds=1)
        assert len(attempts) == 2 and attempts[1] - attempts[0] >= .1
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (requested,)).fetchone()[0] == 'awaiting_authorization'
        assert not urls.has_pending(queue) and not session.calls
    finally:
        http.close()
        queue.close()


@pytest.mark.parametrize('kind', ['candidate', 'repair'])
def test_persistent_mailbox_scan_failure_does_not_block_url_admission(config, tmp_path, monkeypatch, kind):
    staged = deepcopy(config['providers'][0])
    staged.update(id='staged', domains=['staged.test'], enabled=False)
    config['providers'].append(staged)
    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    requested = 'https://staged.test/story/independent-mailbox'
    module = candidates if kind == 'candidate' else repairs
    pump_name = 'pump_candidates' if kind == 'candidate' else 'consume_repairs'
    parts = ('candidate_diagnostics', 'pending') if kind == 'candidate' else ('pending',)
    request_path = repairs._directory(tmp_path, *parts)/('f'*64+'.json')
    atomic_write_bytes(request_path, b'{"synthetic":true}', durable=True, mode=0o600)
    original_pump = getattr(module, pump_name)
    in_pump, attempts = [False], []

    def pump(*args, **kwargs):
        in_pump[0] = True
        try:
            return original_pump(*args, **kwargs)
        finally:
            in_pump[0] = False

    def unavailable(_):
        if in_pump[0]:
            attempts.append(time.monotonic())
        raise OSError(errno.EIO, 'persistent mailbox failure')

    monkeypatch.setattr(module, pump_name, pump)
    monkeypatch.setattr(module, '_pending', unavailable)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    try:
        urls.submit_urls(queue.root, queue.registry, [requested])
        main.run_service(queue, http, config, max_articles=0, max_discovery_requests=0, duration_seconds=1.2)
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (requested,)).fetchone()[0] == 'awaiting_authorization'
        assert request_path.exists() and 2 <= len(attempts) <= 4
        assert min(b-a for a, b in zip(attempts, attempts[1:])) >= .1
        assert not session.calls
    finally:
        http.stop.set()
        http.close()
        queue.close()


@pytest.mark.parametrize('kind', ['candidate', 'repair'])
def test_mailbox_retry_advances_while_other_provider_response_is_inflight(config, tmp_path, monkeypatch, kind):
    config.update(workers=1, batch_articles=1)
    module = candidates if kind == 'candidate' else repairs
    pump_name = 'pump_candidates' if kind == 'candidate' else 'consume_repairs'
    parts = ('candidate_diagnostics', 'pending') if kind == 'candidate' else ('pending',)
    request_path = repairs._directory(tmp_path, *parts)/('a'*64+'.json')
    atomic_write_bytes(request_path, b'{"synthetic":true}', durable=True, mode=0o600)
    finished, started, failures, timed_out = Event(), Event(), [], []

    class PriorityResponse(FakeResponse):
        def iter_content(self, **kwargs):
            started.set()
            timed_out.append(not finished.wait(1.5))
            yield self.body

    priority_url = 'https://publisher.test/story'
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              priority_url: PriorityResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    original_pump, original_pending = getattr(module, pump_name), module._pending
    in_pump, attempts = [False], []

    def pump(*args, **kwargs):
        in_pump[0] = True
        attempts.append(time.monotonic())
        try:
            result = original_pump(*args, **kwargs)
            if not request_path.exists():
                finished.set()
            return result
        finally:
            in_pump[0] = False

    def pending(directory):
        if in_pump[0] and not failures:
            failures.append(True)
            raise OSError(errno.EIO, 'temporary mailbox failure')
        return original_pending(directory)

    monkeypatch.setattr(module, pump_name, pump)
    monkeypatch.setattr(module, '_pending', pending)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    try:
        queue.add_articles([{'url': priority_url}])
        result = main.run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                                continuous_budgets=True, max_run_seconds=3, defer_final_status=True)
        assert failures and started.is_set() and finished.is_set() and timed_out == [False]
        assert len(attempts) >= 2 and attempts[1]-attempts[0] >= .1
        assert result['cycle']['durably_accepted_versions'] == 1 and verify_bodies(tmp_path)['accepted']
        assert {url for url, _ in session.calls} == set(routes)
    finally:
        finished.set()
        http.close()
        queue.close()


def test_url_continuation_flushes_received_body_before_more_inbox_work(config, tmp_path, monkeypatch):
    staged = deepcopy(config['providers'][0])
    staged.update(id='staged', domains=['staged.test'], enabled=False)
    config['providers'].append(staged)
    config.update(workers=1, batch_articles=100)
    priority_url = 'https://publisher.test/story'
    pending_urls = [f'https://staged.test/story/handoff-{number}' for number in range(6)]
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              priority_url: FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    calls = Counter()
    original_urls, original_wait, original_accept = urls.consume_urls, main.wait, queue.accept
    counts_at_commit = []

    def one_url(*args, **kwargs):
        calls['urls'] += 1
        return original_urls(*args, limit=1, **kwargs)

    def wait_for_priority(futures, *, timeout, return_when):
        # Force the received response to be handled before the third slice.
        return original_wait(futures, timeout=1 if calls['urls'] >= 2 else timeout, return_when=return_when)

    def accept(records):
        counts_at_commit.append(queue.db.execute("SELECT count(*) FROM articles WHERE provider='staged'").fetchone()[0])
        return original_accept(records)

    monkeypatch.setattr(urls, 'consume_urls', one_url)
    monkeypatch.setattr(main, 'wait', wait_for_priority)
    monkeypatch.setattr(queue, 'accept', accept)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    try:
        queue.add_articles([{'url': priority_url}])
        urls.submit_urls(queue.root, queue.registry, pending_urls)
        result = main.run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                                continuous_budgets=True, max_run_seconds=3, defer_final_status=True)
        assert counts_at_commit and counts_at_commit[0] < len(pending_urls)
        assert calls['urls'] == len(pending_urls) and result['cycle']['durably_accepted_versions'] == 1
        assert verify_bodies(tmp_path)['accepted'] and {url for url, _ in session.calls} == set(routes)
    finally:
        http.close()
        queue.close()


def test_inbox_retry_runs_while_other_provider_is_inflight(config, tmp_path, monkeypatch):
    staged = deepcopy(config['providers'][0])
    staged.update(id='staged', domains=['staged.test'], enabled=False)
    config['providers'].append(staged)
    config.update(workers=1, batch_articles=1)
    admitted = Event()
    timed_out, attempts = [], []

    class PriorityResponse(FakeResponse):
        def iter_content(self, **kwargs):
            timed_out.append(not admitted.wait(1.5))
            yield self.body

    priority_url = 'https://publisher.test/story'
    requested = 'https://staged.test/story/retry-inflight'
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              priority_url: PriorityResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    original_load, original_inbox, original_consume = urls._load, urls._consume_url_inbox, urls.consume_urls
    in_inbox = [False]

    def transient(path):
        if in_inbox[0]:
            attempts.append(time.monotonic())
            if len(attempts) == 1:
                raise OSError(errno.EIO, 'transient private inbox read failure')
        return original_load(path)

    def inbox(*args, **kwargs):
        in_inbox[0] = True
        try:
            return original_inbox(*args, **kwargs)
        finally:
            in_inbox[0] = False

    def consume(*args, **kwargs):
        result = original_consume(*args, **kwargs)
        if queue.db.execute('SELECT 1 FROM articles WHERE url=?', (requested,)).fetchone():
            admitted.set()
        return result

    monkeypatch.setattr(urls, '_load', transient)
    monkeypatch.setattr(urls, '_consume_url_inbox', inbox)
    monkeypatch.setattr(urls, 'consume_urls', consume)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    try:
        queue.add_articles([{'url': priority_url}])
        urls.submit_urls(queue.root, queue.registry, [requested])
        result = main.run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                                continuous_budgets=True, max_run_seconds=3, defer_final_status=True)
        assert len(attempts) == 2 and attempts[1] - attempts[0] >= .1
        assert timed_out == [False] and admitted.is_set()
        assert result['cycle']['durably_accepted_versions'] == 1 and verify_bodies(tmp_path)['accepted']
        assert {url for url, _ in session.calls} == set(routes)
    finally:
        admitted.set()
        http.close()
        queue.close()


def test_persistent_inbox_io_failure_is_bounded_and_keeps_request(config, tmp_path, monkeypatch):
    staged = deepcopy(config['providers'][0])
    staged.update(id='staged', domains=['staged.test'], enabled=False)
    config['providers'].append(staged)
    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    original_load, original_inbox = urls._load, urls._consume_url_inbox
    in_inbox, attempts = [False], []

    def broken(path):
        if in_inbox[0]:
            attempts.append(time.monotonic())
            raise OSError(errno.EIO, 'private inbox read remains unavailable')
        return original_load(path)

    def inbox(*args, **kwargs):
        in_inbox[0] = True
        try:
            return original_inbox(*args, **kwargs)
        finally:
            in_inbox[0] = False

    monkeypatch.setattr(urls, '_load', broken)
    monkeypatch.setattr(urls, '_consume_url_inbox', inbox)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    try:
        submitted = urls.submit_urls(queue.root, queue.registry, ['https://staged.test/story/persistent'])
        main.run_service(queue, http, config, max_articles=0, max_discovery_requests=0, duration_seconds=.8)
        assert 2 <= len(attempts) <= 5
        assert all(right - left >= .1 for left, right in zip(attempts, attempts[1:]))
        assert urls.has_pending(queue) and Path(submitted['pending_path']).exists() and not session.calls
    finally:
        http.close()
        queue.close()


def test_archive_failure_retries_completed_inbox_without_readmitting(config, tmp_path, monkeypatch):
    staged = deepcopy(config['providers'][0])
    staged.update(id='staged', domains=['staged.test'], enabled=False)
    config['providers'].append(staged)
    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    requested = 'https://staged.test/story/archive-retry'
    original_archive, attempts = urls._archive, []

    def transient(*args):
        attempts.append(time.monotonic())
        if len(attempts) == 1:
            raise OSError(errno.EIO, 'temporary archive failure')
        return original_archive(*args)

    monkeypatch.setattr(urls, '_archive', transient)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    try:
        submitted = urls.submit_urls(queue.root, queue.registry, [requested])
        main.run_service(queue, http, config, max_articles=0, max_discovery_requests=0, duration_seconds=1)
        assert len(attempts) == 2 and attempts[1] - attempts[0] >= .1
        assert not Path(submitted['pending_path']).exists() and not urls.has_pending(queue)
        assert queue.db.execute('SELECT count(*) FROM articles WHERE url=?', (requested,)).fetchone()[0] == 1
        assert queue.db.execute('SELECT count(*) FROM url_metadata_items').fetchone()[0] == 1
        assert not session.calls
    finally:
        http.close()
        queue.close()


def test_url_retry_does_not_hide_other_candidate_mail(config, tmp_path, monkeypatch):
    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    monkeypatch.setattr(urls, 'has_pending', lambda _, **kwargs: True)
    ready = [False]
    monkeypatch.setattr(candidates, 'has_unadmitted_candidates', lambda _, **kwargs: ready[0])
    queue._url_metadata_retry_at = time.monotonic() + 10
    try:
        with OwnerMailboxWake(queue, http.stop) as wake:
            assert not wake.has_pending()
            ready[0] = True
            assert wake.has_pending()
        assert not session.calls
    finally:
        http.close()
        queue.close()


def test_original_declaration_slices_continue_while_other_provider_is_inflight(config, tmp_path, monkeypatch):
    config.update(workers=1, batch_articles=1)
    remaining, done, timed_out = [6], Event(), []

    class PriorityResponse(FakeResponse):
        def iter_content(self, **kwargs):
            timed_out.append(not done.wait(1.0))
            yield self.body

    priority_url = 'https://publisher.test/story'
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              priority_url: PriorityResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)

    def consume(_queue, **_kwargs):
        if not remaining[0]:
            return {'processed': 0, 'outcomes': {}}
        remaining[0] -= 1
        if not remaining[0]:
            done.set()
        return {'processed': 1, 'outcomes': {'declared_original_witness_rejected': 1}}

    monkeypatch.setattr(declarations, 'consume', consume)
    monkeypatch.setattr(declarations, 'has_pending', lambda _: bool(remaining[0]), raising=False)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    try:
        queue.add_articles([{'url': priority_url}])
        result = main.run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                                continuous_budgets=True, max_run_seconds=3, defer_final_status=True)
        assert remaining[0] == 0 and timed_out == [False]
        assert result['cycle']['durably_accepted_versions'] == 1 and verify_bodies(tmp_path)['accepted']
        assert {url for url, _ in session.calls} == set(routes)
    finally:
        done.set()
        http.close()
        queue.close()


def test_declaration_storage_retry_runs_while_other_provider_is_inflight(config, tmp_path, monkeypatch):
    config.update(workers=1, batch_articles=1)
    attempts, done, timed_out = [], Event(), []

    class PriorityResponse(FakeResponse):
        def iter_content(self, **kwargs):
            timed_out.append(not done.wait(1.5))
            yield self.body

    priority_url = 'https://publisher.test/story'
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              priority_url: PriorityResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)

    def consume(_queue, **_kwargs):
        attempts.append(time.monotonic())
        if len(attempts) == 1:
            return {'processed': 0, 'outcomes': {'original_declaration_storage_deferred': 1}}
        done.set()
        return {'processed': 1, 'outcomes': {'declared_original_witness_rejected': 1}}

    monkeypatch.setattr(declarations, 'consume', consume)
    monkeypatch.setattr(declarations, 'has_pending', lambda _: not done.is_set())
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    try:
        queue.add_articles([{'url': priority_url}])
        result = main.run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                                continuous_budgets=True, max_run_seconds=3, defer_final_status=True)
        assert len(attempts) == 2 and attempts[1] - attempts[0] >= .1
        assert timed_out == [False] and done.is_set()
        assert result['cycle']['durably_accepted_versions'] == 1 and verify_bodies(tmp_path)['accepted']
        assert {url for url, _ in session.calls} == set(routes)
    finally:
        done.set()
        http.close()
        queue.close()

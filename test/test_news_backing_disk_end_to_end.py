"""A backing-volume shortage preserves the same durable news work for retry."""
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import run_cycle, run_service
from downloader.news_fulltext_disk import BackingDiskSampler
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_news_fulltext import FakeResponse, article_html, config, http_fixture
from test_news_mnews import URL, page, policy_config


@pytest.mark.parametrize('continuous_budgets', [False, True])
def test_disk_wait_defers_recent_rechecks_and_seed_until_same_owner_recovers(
        config, tmp_path, monkeypatch, continuous_budgets):
    """A durable body is not requeued when the disk gate prevents its recheck."""
    url = 'https://publisher.test/story'
    feed = 'https://publisher.test/recent.xml'
    config['providers'][0]['feeds'] = [feed]
    config['recent_article_recheck_seconds'] = 3600
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              url: FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    blocked = [False]
    monkeypatch.setattr('downloader.news_fulltext.refresh_disk_admission', lambda *_: blocked[0])
    try:
        with queue.db:
            queue.add_articles([{'url': url}])
        first = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
        assert first['states'] == {'complete': 1} and verify_bodies(tmp_path)['accepted']
        # Make an already accepted article due for its real six-hour-style
        # source recheck; the content and receipt remain exactly as accepted.
        old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        with queue.db:
            queue.db.execute('UPDATE articles SET metadata_order_day=?,last_checked_at_utc=? WHERE url=?',
                             (datetime.now(timezone.utc).date().isoformat(), old, url))
            queue.db.execute('DELETE FROM discovery WHERE url=?', (feed,))
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        calls = len(session.calls)
        blocked[0] = True
        for _ in range(2):
            waiting = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                                continuous_budgets=continuous_budgets)
            assert waiting['activity'] == 'waiting_disk' and len(session.calls) == calls
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == before
            assert not queue.db.execute('SELECT 1 FROM discovery WHERE url=?', (feed,)).fetchone()
        blocked[0] = False
        resumed = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                            continuous_budgets=continuous_budgets)
        assert resumed['owner_id'] == waiting['owner_id'] == first['owner_id']
        assert resumed['states'] == {'complete': 1} and len(session.calls) == calls + 1
        assert queue.db.execute('SELECT 1 FROM discovery WHERE url=?', (feed,)).fetchone()
        assert verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        queue.close()


def test_resident_owner_rechecks_due_body_after_backing_space_returns(config, tmp_path, monkeypatch):
    """The live service, not a manual second run, wakes and saves the due body."""
    url = 'https://publisher.test/story'
    config.update(min_free_disk_bytes=1024, recent_article_recheck_seconds=3600,
                  service_idle_max_seconds=.05)
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              url: FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    monkeypatch.setattr('downloader.news_fulltext.shutil.disk_usage', lambda _: SimpleNamespace(free=10_000))
    # The first real capacity sample admits the initial body; afterward the
    # same owner's sampler observes shortage then the real threshold again.
    try:
        queue._backing_disk_sampler = BackingDiskSampler(tmp_path, wsl=False)
        with queue.db:
            queue.add_articles([{'url': url}])
        first = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
        old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        with queue.db:
            queue.db.execute('UPDATE articles SET metadata_order_day=?,last_checked_at_utc=? WHERE url=?',
                             (datetime.now(timezone.utc).date().isoformat(), old, url))
        samples = []

        def probe(_root):
            samples.append(True)
            return 1023 if len(samples) == 1 else 1024

        sampler = BackingDiskSampler(tmp_path, wsl=True, probe=probe, interval_seconds=.03)
        queue._backing_disk_sampler = sampler
        assert sampler.sample(10_000)['state'] == 'pending' and sampler.wait(2)
        calls = len(session.calls)
        real_accept = queue.accept

        def accept_and_stop(records):
            real_accept(records)
            http.stop.set()

        monkeypatch.setattr(queue, 'accept', accept_and_stop)
        result = run_service(queue, http, config, max_articles=1, max_discovery_requests=0,
                             duration_seconds=5)
        assert result['activity'] == 'stopped' and result['owner_id'] == first['owner_id']
        assert len(samples) >= 2 and len(session.calls) == calls + 1
        assert result['states'] == {'complete': 1} and verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        queue.close()


def test_wsl_backing_shortage_then_recovery_reuses_owner_and_work(tmp_path, monkeypatch):
    config = policy_config()
    config['min_free_disk_bytes'] = 1024
    http, session = http_fixture(config, tmp_path, {
        'https://www.mnews.tw/robots.txt': FakeResponse(body=b'User-agent: *\nAllow: /'),
        URL: FakeResponse(body=str(page()).encode())})
    queue = NewsQueue(tmp_path, http.registry)
    clock, free = [0.0], [1023]
    sampler = BackingDiskSampler(tmp_path, wsl=True, clock=lambda: clock[0], probe=lambda _: free[0])
    queue._backing_disk_sampler = sampler
    monkeypatch.setattr('downloader.news_fulltext.shutil.disk_usage', lambda _: SimpleNamespace(free=10_000))
    try:
        with queue.db:
            queue.add_articles([{'url': URL}])
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        sampler.sample(10_000)
        assert sampler.wait(2)
        blocked = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
        assert blocked['activity'] == 'waiting_disk' and not session.calls
        assert blocked['disk_admission']['free_bytes'] == 1023
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
        clock[0], free[0] = 61.0, 1024
        assert sampler.sample(10_000)['inflight'] and sampler.wait(2)
        resumed = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
        assert resumed['states'] == {'complete': 1} and resumed['owner_id'] == blocked['owner_id']
        assert len(session.calls) == 2 and verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        queue.close()


def test_wedged_capacity_check_is_killed_then_same_owner_resumes(tmp_path, monkeypatch):
    config = policy_config()
    config['min_free_disk_bytes'] = 1024
    http, session = http_fixture(config, tmp_path, {
        'https://www.mnews.tw/robots.txt': FakeResponse(body=b'User-agent: *\nAllow: /'),
        URL: FakeResponse(body=str(page()).encode())})
    queue = NewsQueue(tmp_path, http.registry)
    clock, children = [0.0], []
    real_popen = subprocess.Popen

    def launch(_args, **kwargs):
        program = 'import time; time.sleep(60)' if not children else 'print(1024)'
        child = real_popen([sys.executable, '-c', program], **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr('downloader.news_fulltext_disk.subprocess.Popen', launch)
    monkeypatch.setattr('downloader.news_fulltext.shutil.disk_usage', lambda _: SimpleNamespace(free=10_000))
    sampler = BackingDiskSampler(tmp_path, wsl=True, clock=lambda: clock[0], probe_timeout_seconds=0.75)
    queue._backing_disk_sampler = sampler
    try:
        with queue.db:
            queue.add_articles([{'url': URL}])
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        assert sampler.sample(10_000)['state'] == 'pending'
        assert sampler.wait(5) and children[0].poll() is not None
        blocked = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
        assert blocked['activity'] == 'waiting_disk'
        assert blocked['disk_admission']['physical_backing_state'] == 'unavailable'
        assert blocked['disk_admission']['free_bytes'] is None and not session.calls
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
        clock[0] = 61.0
        assert sampler.sample(10_000)['inflight'] and sampler.wait(5)
        resumed = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
        assert len(children) == 2 and resumed['owner_id'] == blocked['owner_id']
        assert resumed['states'] == {'complete': 1}
        assert len(session.calls) == 2 and verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        queue.close()

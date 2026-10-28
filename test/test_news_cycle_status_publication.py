"""A cycle publishes its complete progress once, after durable acceptance."""
import json
from types import SimpleNamespace

import pytest

from downloader import news_fulltext, news_fulltext_queue
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_news_fulltext import config, http_fixture, FakeResponse, article_html


def observe_publications(monkeypatch):
    publications = []
    original = news_fulltext_queue.atomic_write_json

    def write(path, value, **kwargs):
        if path.name == 'status.json':
            publications.append(json.loads(json.dumps(value)))
        return original(path, value, **kwargs)

    monkeypatch.setattr(news_fulltext_queue, 'atomic_write_json', write)
    monkeypatch.setattr(news_fulltext, 'atomic_write_json', write)
    return publications


@pytest.mark.parametrize('outcome', ['accepted', 'refused', 'empty'])
def test_cycle_has_one_final_publication_with_complete_work_and_unchanged_receipts(config, tmp_path,
                                                                               monkeypatch, outcome):
    url = 'https://publisher.test/news/new'
    response = (FakeResponse(article_html()) if outcome == 'accepted' else
                FakeResponse(status=429, headers={'Retry-After': '120'}))
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'), url: response})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        if outcome != 'empty':
            queue.add_articles([{'url': url}])
        publications = observe_publications(monkeypatch)
        result = news_fulltext.run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
        final = [row for row in publications if row['activity'] == 'cycle_complete']
        assert len(final) == 1
        assert final[0] == result == json.loads((tmp_path / 'status.json').read_bytes())
        assert result['cycle']['article_attempts_started'] == (outcome != 'empty')
        assert result['cycle']['durably_accepted_versions'] == (outcome == 'accepted')
        assert result['http_counters_basis'] == 'since current owner started; cycle contains separate deltas'
        if outcome == 'accepted':
            assert verify_bodies(tmp_path)['complete_queue_heads_verified'] == 1
            assert queue.db.execute('SELECT state FROM articles WHERE url=?', (url,)).fetchone()[0] == 'complete'
        elif outcome == 'refused':
            row = queue.db.execute('SELECT state,error_code,due FROM articles WHERE url=?', (url,)).fetchone()
            assert row['state'] == 'retry' and row['error_code'] == 'http_429'
            assert row['due'] > 0 and result['provider_next_attempt_utc']['publisher']
        else:
            assert not session.calls and result['cycle']['http_requests'] == {}
    finally:
        queue.close()
        http.close()


def test_unpublished_projection_preserves_previous_file_and_lifecycle(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        queue.status(activity='prior')
        before = (tmp_path / 'status.json').read_bytes()
        acknowledgments = []
        queue.lifecycle = SimpleNamespace(snapshot_written=lambda: acknowledgments.append(True))
        projected = queue.status(activity='building_cycle', publish=False)
        assert (tmp_path / 'status.json').read_bytes() == before
        assert not acknowledgments
        projected['cycle'] = {'article_attempts_started': 0, 'durably_accepted_versions': 0}
        queue.publish_status(projected)
        assert json.loads((tmp_path / 'status.json').read_bytes()) == projected
        assert acknowledgments == [True]
    finally:
        queue.close()
        http.close()


def test_failed_publication_does_not_acknowledge_a_new_snapshot(config, tmp_path, monkeypatch):
    http, _ = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        queue.status(activity='prior')
        before = (tmp_path / 'status.json').read_bytes()
        acknowledgments = []
        queue.lifecycle = SimpleNamespace(snapshot_written=lambda: acknowledgments.append(True))
        value = queue.status(activity='building_cycle', publish=False)

        def failed(*args, **kwargs):
            raise OSError('fixture publication failure')

        monkeypatch.setattr(news_fulltext_queue, 'atomic_write_json', failed)
        with pytest.raises(OSError, match='fixture publication failure'):
            queue.publish_status(value)
        assert (tmp_path / 'status.json').read_bytes() == before and not acknowledgments
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('interrupt', [False, True])
def test_inventory_keeps_only_the_current_owner_last_completed_cycle(config, tmp_path, monkeypatch, interrupt):
    """Local inventory must not erase or present an earlier cycle as active."""
    http, _ = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    cycle = {'wall_seconds': 2, 'article_attempts_started': 4,
             'durably_accepted_versions': 3, 'http_requests': {'publisher': 4}}
    inventories = []
    try:
        def refresh():
            inventories.append(queue.status(activity='inventorying'))

        def wait_for_disk(_queue, _http, _mailbox, _delay, refresh_metadata, _result):
            refresh_metadata()
            if interrupt:
                raise RuntimeError('fixture interruption')
            http.stop.set()

        monkeypatch.setattr(news_fulltext, 'run_cycle',
                            lambda *args, **kwargs: {'activity': 'waiting_disk', 'cycle': cycle})
        monkeypatch.setattr(news_fulltext, '_wait_for_disk_capacity', wait_for_disk)
        if interrupt:
            with pytest.raises(RuntimeError, match='fixture interruption'):
                news_fulltext.run_service(queue, http, config, max_articles=1, max_discovery_requests=0,
                                          duration_seconds=5, refresh_metadata=refresh)
        else:
            news_fulltext.run_service(queue, http, config, max_articles=1, max_discovery_requests=0,
                                      duration_seconds=5, refresh_metadata=refresh)

        assert len(inventories) == 2
        assert 'cycle' not in inventories[0]  # No previous completed cycle on this owner.
        assert inventories[1]['cycle'] == cycle
        assert inventories[1]['cycle_snapshot_role'] == 'last_completed_before_inventory'
        assert inventories[1]['cycle_completed_at_utc']
        assert inventories[1]['observed_accepted_articles_per_second'] is None
        assert 'cycle' not in queue.status(activity='inventorying')  # No leak into a later owner/CLI.
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('outcome', ['accepted', 'refused', 'empty'])
def test_service_publishes_one_fresh_waiting_transition_with_durable_cycle(config, tmp_path, monkeypatch, outcome):
    """The resident transition must not build both cycle and idle projections."""
    url = 'https://publisher.test/news/new'
    response = (FakeResponse(article_html()) if outcome == 'accepted' else
                FakeResponse(status=429, headers={'Retry-After': '120'}))
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'), url: response})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        if outcome != 'empty':
            queue.add_articles([{'url': url}])
        publications = observe_publications(monkeypatch)
        checks = []
        original_status = queue.status

        def refresh(target, _config):
            checks.append(True)
            target.disk_admission = {'contract_version': 1, 'fetch_allowed': True,
                                     'checked_at_utc': '2026-10-28T00:00:00+00:00'}
            return False

        def status(**kwargs):
            result = original_status(**kwargs)
            if kwargs.get('activity') == 'waiting_next_due':
                http.stop.set()
            return result

        monkeypatch.setattr(news_fulltext, 'refresh_disk_admission', refresh)
        monkeypatch.setattr(queue, 'next_wake_delay', lambda *args, **kwargs: 60)
        monkeypatch.setattr(queue, 'status', status)
        final = news_fulltext.run_service(queue, http, config, max_articles=1, max_discovery_requests=0,
                                         duration_seconds=5)
        waiting = [row for row in publications if row['activity'] == 'waiting_next_due']
        assert len(waiting) == 1
        assert not [row for row in publications if row['activity'] == 'cycle_complete']
        assert len(checks) >= 2  # Initial cycle admission and fresh idle admission.
        assert waiting[0]['disk_admission']['checked_at_utc'] == '2026-10-28T00:00:00+00:00'
        cycle = waiting[0]['cycle']
        assert cycle['article_attempts_started'] == (outcome != 'empty')
        assert cycle['durably_accepted_versions'] == (outcome == 'accepted')
        assert final['activity'] == 'stopped' and final['cycle'] == cycle
        if outcome == 'accepted':
            assert verify_bodies(tmp_path)['complete_queue_heads_verified'] == 1
        elif outcome == 'refused':
            row = queue.db.execute('SELECT state,error_code,due FROM articles WHERE url=?', (url,)).fetchone()
            assert row['state'] == 'retry' and row['error_code'] == 'http_429' and row['due'] > 0
            assert waiting[0]['provider_next_attempt_utc']['publisher']
        else:
            assert not session.calls
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('failure_point', ['deadline', 'disk_refresh', 'mailbox'])
def test_service_publishes_completed_cycle_if_transition_fails(config, tmp_path, monkeypatch, failure_point):
    url = 'https://publisher.test/news/new'
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(article_html())})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        queue.add_articles([{'url': url}])
        publications = observe_publications(monkeypatch)
        checks = []

        def refresh(*args):
            checks.append(True)
            if failure_point == 'disk_refresh' and len(checks) > 1:
                raise RuntimeError('fixture transition failure')
            return False

        monkeypatch.setattr(news_fulltext, 'refresh_disk_admission', refresh)

        def failed(*args, **kwargs):
            raise RuntimeError('fixture transition failure')

        monkeypatch.setattr(queue, 'next_wake_delay', failed if failure_point == 'deadline' else lambda *args, **kwargs: 60)
        if failure_point == 'mailbox':
            monkeypatch.setattr('downloader.news_fulltext_wake.OwnerMailboxWake.has_pending', failed)
        with pytest.raises(RuntimeError, match='fixture transition failure'):
            news_fulltext.run_service(queue, http, config, max_articles=1, max_discovery_requests=0,
                                      duration_seconds=5)
        final = [row for row in publications if row['activity'] == 'cycle_complete']
        assert len(final) == 1 and final[0]['cycle']['durably_accepted_versions'] == 1
        assert verify_bodies(tmp_path)['complete_queue_heads_verified'] == 1
    finally:
        queue.close()
        http.close()

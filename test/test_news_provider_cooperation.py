"""Independent publisher work must not wait for another publisher's response.

Exercise the real shared queue/HTTP/executor/parser/fsync/receipt path with
synthetic source responses. No production owner, publisher request or policy
relaxation; the engineering order must not become a global downloader gate.
"""
from copy import deepcopy
from threading import Event
import time

import pytest

from downloader.news_fulltext import run_cycle
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_news_fulltext import FakeResponse, article_html, config, http_fixture


def cooperative_config(config):
    foreground = config['providers'][0]
    foreground.update(parallelism=1)
    background = deepcopy(foreground)
    background.update(id='background', domains=['background.test'])
    disabled = deepcopy(foreground)
    disabled.update(id='disabled', domains=['disabled.test'], enabled=False)
    # Background first deliberately tests independence, not lucky priority order.
    config['providers'] = [background, foreground, disabled]
    config.update(workers=2, batch_articles=1)
    return config


@pytest.mark.parametrize('continuous_budgets', [False, True])
def test_cmoney_pristine_path_sweep_waits_for_disk_and_other_provider_continues(
        config, tmp_path, monkeypatch, continuous_budgets):
    cmoney = config['providers'][0]
    cmoney.update(id='cmoney', domains=['cmoney.tw'], parallelism=1,
                  article_path_pattern=r'^/(forum/article/[0-9]+|notes/note[.]aspx)')
    other = deepcopy(cmoney)
    other.update(id='publisher', domains=['publisher.test'], article_path_pattern=r'^/story$')
    config['providers'] = [cmoney, other]
    config.update(workers=2, batch_articles=1)
    forum = 'https://www.cmoney.tw/forum/article/55'
    focus = 'https://publisher.test/story'
    notes = [f'https://www.cmoney.tw/notes/note-detail.aspx?nid={i}' for i in range(3)]
    routes = {'https://www.cmoney.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              forum: FakeResponse(article_html()), focus: FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    slices = []
    original = queue.backfill_cmoney_note_detail_exclusions

    def slice_one(*, stop=None):
        result = original(limit=1, stop=stop)
        slices.append((result, tuple(url for url, _ in session.calls)))
        return result

    monkeypatch.setattr(queue, 'backfill_cmoney_note_detail_exclusions', slice_one)
    blocked = [True]
    monkeypatch.setattr('downloader.news_fulltext.refresh_disk_admission', lambda *_: blocked[0])
    try:
        with queue.db:
            queue.add_articles([{'url': url} for url in notes + [forum, focus]])
            queue.db.executemany("UPDATE articles SET state='pending',error_code=NULL WHERE url=?",
                                 ((url,) for url in notes))
        before = [dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) for url in notes]
        result = run_cycle(queue, http, config, max_articles=4, max_discovery_requests=0,
                           continuous_budgets=continuous_budgets)
        assert result['activity'] == 'waiting_disk' and not slices and not session.calls
        assert [dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) for url in notes] == before
        blocked[0] = False
        result = run_cycle(queue, http, config, max_articles=4, max_discovery_requests=0,
                           continuous_budgets=continuous_budgets)
        assert slices and slices[-1][0]['completed']
        assert any(focus in calls and forum not in calls and not item['completed'] for item, calls in slices)
        assert all(queue.db.execute('SELECT state,attempts FROM articles WHERE url=?', (url,)).fetchone()[:]
                   == ('unsupported_article_path', 0) for url in notes)
        assert all(queue.db.execute('SELECT state FROM articles WHERE url=?', (url,)).fetchone()[0] == 'complete'
                   for url in (forum, focus))
        assert not any('note-detail.aspx' in url for url, _ in session.calls)
        assert result['cycle']['durably_accepted_versions'] == 2
        assert verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        queue.close()


def test_priority_body_is_durable_while_background_response_still_waits(config, tmp_path, monkeypatch):
    config = cooperative_config(config)
    background_started, focus_committed = Event(), Event()

    class WaitingBackground(FakeResponse):
        def iter_content(self, **kwargs):
            background_started.set()
            assert focus_committed.wait(5), 'background must not occupy every available worker'
            yield self.body

    class FocusResponse(FakeResponse):
        def iter_content(self, **kwargs):
            assert background_started.wait(5), 'both publishers must actually overlap'
            yield self.body

    routes = {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        'https://background.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        'https://publisher.test/story': FocusResponse(article_html()),
        'https://background.test/story': WaitingBackground(article_html()),
    }
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    original_accept = queue.accept
    commits = []

    def accept(records):
        original_accept(records)
        commits.extend(record['provider'] for record in records)
        if 'publisher' in commits:
            focus_committed.set()  # Only after the real durable writer returns.

    monkeypatch.setattr(queue, 'accept', accept)
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://' + host + '/story'} for host in
                                ('publisher.test', 'background.test', 'disabled.test')])
        status = run_cycle(queue, http, config, max_articles=4, max_discovery_requests=0)
        assert background_started.is_set() and focus_committed.is_set()
        assert commits == ['publisher', 'background']
        assert status['states'] == {'complete': 2, 'awaiting_authorization': 1}
        proof = verify_bodies(tmp_path)
        assert proof['accepted'] and proof['body_versions_verified'] == 2
        assert not any('disabled.test' in url for url, _ in session.calls)
        assert http.parallelism == {'background': 1, 'publisher': 1}
        assert 'disabled' not in http.limiters
    finally:
        focus_committed.set()
        http.close()
        queue.close()


@pytest.mark.parametrize('refused', ['publisher', 'background'])
def test_source_refusal_pauses_only_its_provider_while_other_body_commits(config, tmp_path, refused):
    config = cooperative_config(config)
    sources = {'publisher': 'publisher.test', 'background': 'background.test'}
    routes = {f'https://{host}/robots.txt': FakeResponse(b'User-agent: *\nAllow: /')
              for host in sources.values()}
    routes.update({f'https://{host}/story': FakeResponse(status=403) if provider == refused
                   else FakeResponse(article_html()) for provider, host in sources.items()})
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{'url': f'https://{host}/story'} for host in sources.values()])
        status = run_cycle(queue, http, config, max_articles=4, max_discovery_requests=0)
        assert status['states'] == {'complete': 1, 'retry': 1}
        refused_head = queue.db.execute('SELECT state,error_code,due FROM articles WHERE provider=?',
                                       (refused,)).fetchone()
        assert refused_head['state'] == 'retry' and refused_head['error_code'] == 'source_access_denied'
        assert refused_head['due'] > time.time()
        assert http.provider_not_before[refused] > time.time()
        healthy = next(provider for provider in sources if provider != refused)
        assert healthy not in http.provider_not_before
        assert sum(url == f'https://{sources[refused]}/story' for url, _ in session.calls) == 1
        proof = verify_bodies(tmp_path)
        assert proof['accepted'] and proof['body_versions_verified'] == 1
    finally:
        http.close()
        queue.close()

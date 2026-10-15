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

from datetime import datetime, timedelta, timezone

import pytest

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, accepted_publication_sort_day, verify_bodies
from test_news_fulltext import config, FakeResponse, article_html, http_fixture


@pytest.mark.parametrize('value', [None, '', True, 1791627600, '2026-10-10', '2026-10-10T09:00:00',
                                  '2026-99-10T09:00:00+08:00', 'unknown', 'Fri, 09 Oct 2026 09:00:00 -0000'])
def test_unknown_or_naive_source_clock_is_never_invented(value):
    assert accepted_publication_sort_day(value) == ''


@pytest.mark.parametrize('value,expected', [('2026-10-10T00:30:00+08:00', '2026-10-09'),
    ('2026-10-10T09:30:00Z', '2026-10-10'), ('Fri, 09 Oct 2026 09:00:00 GMT', '2026-10-09')])
def test_accepted_source_clock_sort_hint_has_utc_basis(value, expected):
    assert accepted_publication_sort_day(value) == expected


def test_newly_discovered_undated_article_uses_its_own_source_clock_not_capture_day(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    recent = (datetime.now(timezone.utc)-timedelta(hours=2)).isoformat()
    clocks = {'recent': recent, 'old': '2014-02-03T09:00:00+08:00', 'unknown': '2014-02-03', 'metadata': recent}
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nDisallow:\n')}
    for label, clock in clocks.items():
        routes[f'https://publisher.test/{label}'] = FakeResponse(article_html().replace(b'2014-02-03', clock.encode()))
    http, _ = http_fixture(config, tmp_path, routes)
    try:
        with queue.db:
            queue.add_articles([{'url': f'https://publisher.test/{label}', 'date': '2026-10-01' if label == 'metadata' else ''}
                                for label in clocks])
        for label in clocks:
            row = queue.claim_url(f'https://publisher.test/{label}', 300)
            queue.accept([fetch_article(http, row)])
        assert verify_bodies(tmp_path)['accepted']
        rows = {r['url'].rsplit('/', 1)[-1]: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        assert rows['recent']['metadata_order_day'] == accepted_publication_sort_day(recent)
        assert rows['old']['metadata_order_day'] == '2014-02-03'
        assert rows['unknown']['metadata_order_day'] == ''
        assert rows['metadata']['metadata_order_day'] == rows['metadata']['metadata_date_raw'] == '2026-10-01'
        assert all(rows[label]['metadata_date_raw'] == '' for label in ('recent', 'old', 'unknown'))
        with queue.db:
            queue.db.execute('UPDATE articles SET last_checked_at_utc=?', ((datetime.now(timezone.utc)-timedelta(hours=3)).isoformat(),))
        queue.schedule_recent_rechecks(3600, 3)
        states = {r['url'].rsplit('/', 1)[-1]: r['state'] for r in queue.db.execute('SELECT url,state FROM articles')}
        assert states == {'recent': 'retry', 'old': 'complete', 'unknown': 'complete', 'metadata': 'complete'}
        assert queue.status()['article_recheck_day_contract_version'] == 1
    finally:
        http.close()
        queue.close()

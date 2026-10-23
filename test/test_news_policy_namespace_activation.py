"""Enabling metadata is not permission to dispatch every same-host link."""
from copy import deepcopy
from dataclasses import replace

import pytest

from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config


def heads(queue):
    return {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')}


def test_enable_classifies_never_requested_paths_before_they_become_work(config, tmp_path, monkeypatch):
    config['providers'][0]['article_path_pattern'] = r'^/story/[0-9]+$'
    disabled = deepcopy(config); disabled['providers'][0]['enabled'] = False
    urls = ['https://publisher.test/story/1', 'https://publisher.test/news_info.php?n=1',
            'https://publisher.test/index/story/1', 'https://unknown.test/story/1']
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
                        lambda *a, **k: pytest.fail('Policy reconciliation cannot create HTTP'))
    queue = NewsQueue(tmp_path, PolicyRegistry(disabled, base=tmp_path))
    with queue.db: queue.add_articles([{'url': url, 'title': 'Raw metadata', 'date': '2020-03-02'} for url in urls])
    before = heads(queue); queue.close()
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        after = heads(queue)
        assert after[urls[0]]['state'] == 'pending'
        assert after[urls[3]] == before[urls[3]]
        for url in urls[1:3]:
            assert after[url] == {**before[url], 'state': 'unsupported_article_path',
                                  'error_code': 'unsupported_article_path'}
        assert queue.claim('publisher', 300)['url'] == urls[0]
        assert queue.claim('publisher', 300) is None
    finally:
        queue.close()
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        reopened = heads(queue)
        for url in urls[1:]: assert reopened[url] == after[url]
    finally: queue.close()


@pytest.mark.parametrize('field,value', [
    ('attempts', 1), ('due', 123.0), ('error_code', 'source_access_denied'),
    ('body_sha256', 'retained'), ('body_bytes', 1), ('parquet_path', 'retained.parquet'),
    ('captured_at_utc', '2020-01-01T00:00:00+00:00'),
    ('last_checked_at_utc', '2020-01-01T00:00:00+00:00'),
    ('authorization_fingerprint', 'previous'), ('etag', 'previous'), ('last_modified', 'previous'),
    ('validator_url', 'https://publisher.test/old'), ('lease_owner', 'previous'), ('lease_until', 123.0),
])
def test_excluded_observations_are_not_pristine_metadata(config, tmp_path, field, value):
    config['providers'][0]['article_path_pattern'] = r'^/story/[0-9]+$'
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/old'}])
            queue.db.execute("UPDATE articles SET state='awaiting_authorization',error_code=NULL")
            queue.db.execute('UPDATE articles SET '+field+'=?', (value,))
        before = heads(queue)
        with queue.db: queue._activate_authorized_metadata('publisher.test', queue.registry.sources['publisher'])
        assert heads(queue) == before
    finally: queue.close()


@pytest.mark.parametrize('finite', [False, True])
def test_finite_selection_and_no_pattern_fast_path_preserve_admission(config, tmp_path, finite):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        urls = ['https://publisher.test/story/1', 'https://publisher.test/story/2', 'https://publisher.test/old']
        with queue.db:
            queue.add_articles([{'url': url} for url in urls])
            queue.db.execute("UPDATE articles SET state='awaiting_authorization'")
        source = queue.registry.sources['publisher']
        if finite:
            source = replace(source, article_path_pattern=r'^/story/[0-9]+$', finite_article_urls=(urls[0],))
        with queue.db: queue._activate_authorized_metadata('publisher.test', source)
        after = heads(queue)
        assert after[urls[0]]['state'] == 'pending'
        assert after[urls[1]]['state'] == ('awaiting_authorization' if finite else 'pending')
        assert after[urls[2]]['state'] == ('unsupported_article_path' if finite else 'pending')
    finally: queue.close()

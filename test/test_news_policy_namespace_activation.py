"""Enabling metadata is not permission to dispatch every same-host link."""
from copy import deepcopy
from dataclasses import replace
import json

import pytest

from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config


def heads(queue):
    return {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')}


def test_readmo_legacy_pending_paths_are_classified_without_http(config, tmp_path, monkeypatch):
    config['providers'][0].update(id='cmoney', domains=['cmoney.tw'],
        article_path_pattern=r'^/(forum/article/[0-9]+|notes/note[.]aspx)')
    registry = PolicyRegistry(config, base=tmp_path)
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
                        lambda *a, **k: pytest.fail('local path admission cannot create HTTP'))
    q = NewsQueue(tmp_path, registry)
    urls = ['https://readmo.cmoney.tw/article/first', 'https://readmo.cmoney.tw/article/observed',
            'https://readmo.cmoney.tw/article/leased', 'https://readmo.cmoney.tw/forum/article/1']
    with q.db:
        q.add_articles([{'url': url} for url in urls])
        q.db.execute("DELETE FROM queue_settings WHERE key='readmo_pristine_path_admission_v1'")
        q.db.execute("UPDATE articles SET state='pending',error_code=NULL WHERE host='readmo.cmoney.tw'")
        q.db.execute('UPDATE articles SET attempts=1,last_checked_at_utc=? WHERE url=?',
                     ('2026-01-01T00:00:00+00:00', urls[1]))
        q.db.execute('UPDATE articles SET lease_owner=? WHERE url=?', ('owner', urls[2]))
    before = heads(q)
    q.close()
    q = NewsQueue(tmp_path, registry)
    after = heads(q)
    assert after[urls[0]] == {**before[urls[0]], 'state': 'unsupported_article_path',
                              'error_code': 'unsupported_article_path'}
    for url in urls[1:]:
        assert after[url] == before[url]
    marker = json.loads(q.db.execute("SELECT value FROM queue_settings WHERE key=?",
                                     ('readmo_pristine_path_admission_v1',)).fetchone()[0])
    assert marker['excluded_heads'] == 1 and marker['source_requests'] == 0
    q.migrate_readmo_pristine_namespace()
    assert heads(q) == after
    q.close()
    q = NewsQueue(tmp_path, registry)
    try:
        assert heads(q) == after
    finally:
        q.close()


def test_cmoney_note_detail_sweep_is_bounded_resumable_and_non_destructive(config, tmp_path, monkeypatch):
    config['providers'][0].update(id='cmoney', domains=['cmoney.tw'],
        article_path_pattern=r'^/(forum/article/[0-9]+|notes/note[.]aspx)')
    registry = PolicyRegistry(config, base=tmp_path)
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
                        lambda *a, **k: pytest.fail('local path admission cannot create HTTP'))
    q = NewsQueue(tmp_path, registry)
    notes = [f'https://www.cmoney.tw/notes/note-detail.aspx?nid={i}' for i in range(1, 7)]
    notes += ['http://www.cmoney.tw/notes/note-detail.aspx?nid=7']
    other = ['https://www.cmoney.tw/notes/note.aspx?nid=8',
             'https://other.cmoney.tw/unreviewed?nid=9']
    with q.db:
        q.add_articles([{'url': url, 'title': 'Existing metadata', 'date': '2020-01-01'}
                        for url in notes + other])
        for url in notes:
            q.db.execute("UPDATE articles SET state='pending',error_code=NULL WHERE url=?", (url,))
        q.db.execute('UPDATE articles SET attempts=1 WHERE url=?', (notes[3],))
        q.db.execute('UPDATE articles SET due=123 WHERE url=?', (notes[4],))
        q.db.execute('UPDATE articles SET body_sha256=?,body_bytes=1 WHERE url=?', ('kept', notes[5]))
    before = heads(q)
    first = q.backfill_cmoney_note_detail_exclusions(limit=2)
    assert first == {'completed': False, 'classified': 2, 'publisher_requests': 0}
    q.close()
    q = NewsQueue(tmp_path, registry)
    try:
        for _ in range(10):
            result = q.backfill_cmoney_note_detail_exclusions(limit=2)
            if result['completed']:
                break
        assert result['completed']
        after = heads(q)
        for url in notes[:3] + [notes[6]]:
            assert after[url] == {**before[url], 'state': 'unsupported_article_path',
                                  'error_code': 'unsupported_article_path'}
        for url in notes[3:6] + other:
            assert after[url] == before[url]
        source = registry.sources['cmoney']
        marker = 'cmoney_note_detail_pristine_path_v1:' + source.data_fingerprint
        progress = json.loads(q.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0])
        assert progress == {'contract_version': 1, 'phase': 2, 'last_url': '', 'classified': 4}
        assert q.backfill_cmoney_note_detail_exclusions(limit=2)['classified'] == 0
        assert heads(q) == after
        # A later reviewed path expansion is applied by a real queue restart.
        expanded_config = deepcopy(config)
        expanded_config['providers'][0]['article_path_pattern'] = r'^/(forum/article/[0-9]+|notes/note(?:-detail)?[.]aspx)'
        q.close()
        q = NewsQueue(tmp_path, PolicyRegistry(expanded_config, base=tmp_path))
        reactivated = heads(q)
        for url in notes[:3] + [notes[6]]:
            assert reactivated[url] == before[url]
        for url in notes[3:6] + other:
            assert reactivated[url] == before[url]
    finally:
        q.close()


def test_cmoney_note_detail_sweep_recovers_corrupt_cursor_without_reopening_bodies(config, tmp_path):
    config['providers'][0].update(id='cmoney', domains=['cmoney.tw'],
        article_path_pattern=r'^/(forum/article/[0-9]+|notes/note[.]aspx)')
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    url = 'https://www.cmoney.tw/notes/note-detail.aspx?nid=1'
    try:
        with q.db:
            q.add_articles([{'url': url}])
            q.db.execute("UPDATE articles SET state='pending',error_code=NULL WHERE url=?", (url,))
            marker = 'cmoney_note_detail_pristine_path_v1:' + q.registry.sources['cmoney'].data_fingerprint
            q.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, '{broken'))
        q.backfill_cmoney_note_detail_exclusions(limit=2)
        assert heads(q)[url]['state'] == 'unsupported_article_path'
        recovery = q.db.execute('SELECT reason,original_value FROM original_url_checkpoint_recoveries '
                                'WHERE setting_key=?', (marker,)).fetchone()
        assert tuple(recovery) == ('invalid_json', '{broken')
    finally:
        q.close()


@pytest.mark.parametrize('finite', [False, True])
def test_expanded_namespace_reactivates_only_pristine_local_exclusions(config, tmp_path, finite):
    config['providers'][0]['article_path_pattern'] = r'^/old/[0-9]+$'
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    urls = ['https://publisher.test/new/1', 'https://publisher.test/new/2',
            'https://publisher.test/new/3', 'https://publisher.test/other/4']
    try:
        with q.db:
            q.add_articles([{'url': url} for url in urls])
            q.db.execute('UPDATE articles SET attempts=1,last_checked_at_utc=? WHERE url=?',
                         ('2026-01-01T00:00:00+00:00', urls[1]))
        before = heads(q)
        source = replace(q.registry.sources['publisher'], article_path_pattern=r'^/new/[0-9]+$',
                         finite_article_urls=(urls[0],) if finite else ())
        with q.db:
            q._activate_authorized_metadata('publisher.test', source)
        after = heads(q)
        assert after[urls[0]] == {**before[urls[0]], 'state': 'pending', 'error_code': None}
        assert after[urls[1]] == before[urls[1]]
        assert after[urls[2]] == (before[urls[2]] if finite else
                                  {**before[urls[2]], 'state': 'pending', 'error_code': None})
        assert after[urls[3]] == before[urls[3]]
    finally:
        q.close()


def test_policy_rollout_reactivates_newly_supported_pristine_metadata(config, tmp_path):
    config['providers'][0]['article_path_pattern'] = r'^/old/[0-9]+$'
    old = PolicyRegistry(config, base=tmp_path)
    url = 'https://publisher.test/new/1'
    q = NewsQueue(tmp_path, old)
    with q.db:
        q.add_articles([{'url': url}])
    before = heads(q)[url]
    assert before['state'] == 'unsupported_article_path' and before['attempts'] == 0
    q.close()
    updated = deepcopy(config)
    updated['providers'][0]['article_path_pattern'] = r'^/new/[0-9]+$'
    new = PolicyRegistry(updated, base=tmp_path)
    q = NewsQueue(tmp_path, new)
    try:
        assert heads(q)[url] == {**before, 'state': 'pending', 'error_code': None}
    finally:
        q.close()
    q = NewsQueue(tmp_path, new)
    try:
        assert heads(q)[url]['state'] == 'pending'
    finally:
        q.close()


def test_namespace_rollout_preserves_frozen_finite_membership(config, tmp_path):
    """A new path rule does not expand an already frozen finite source."""
    config['providers'][0].update(acquisition_mode='finite', article_path_pattern=r'^/old/[0-9]+$')
    first = 'https://publisher.test/old/1'
    included = 'https://publisher.test/new/2'
    outside = 'https://publisher.test/new/3'
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    with q.db:
        q.add_articles([{'url': first}, {'url': included}])
    q.sync_finite_cohorts()
    receipt = dict(q.db.execute('SELECT * FROM finite_cohorts WHERE provider=?', ('publisher',)).fetchone())
    with q.db:
        q.add_articles([{'url': outside}])
    before = heads(q)
    q.close()

    updated = deepcopy(config)
    updated['providers'][0]['article_path_pattern'] = r'^/(old|new)/[0-9]+$'
    q = NewsQueue(tmp_path, PolicyRegistry(updated, base=tmp_path))
    try:
        assert dict(q.db.execute('SELECT * FROM finite_cohorts WHERE provider=?', ('publisher',)).fetchone()) == receipt
        after = heads(q)
        assert after[included] == {**before[included], 'state': 'pending', 'error_code': None}
        assert after[outside] == {**before[outside], 'state': 'waiting_cohort', 'error_code': None}
        assert {q.claim('publisher', 300)['url'], q.claim('publisher', 300)['url']} == {first, included}
        assert q.claim('publisher', 300) is None
        assert q.claim_url(outside, 300) is None
    finally:
        q.close()


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

"""A finite historical feed schedule must not replay healthy captures or refusals."""
from copy import deepcopy
import json
from pathlib import Path
import time

import pytest

from downloader.news_fulltext import (accept_discovery, discovery_fingerprint, fetch_discovery,
    load_config, seed_discovery)
from downloader.news_fulltext_feed import feed_records, feed_url
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_news_fulltext import config as config, http_fixture
from test_news_provider_efficiency import feed_item, rss


ROOT = 'https://publisher.test/feed/'
MONTH = 'https://publisher.test/2005/01/feed/'


def configured(config):
    entry = config['providers'][0]
    entry.update(fulltext_feeds=[ROOT], historical_fulltext_feeds=[MONTH], require_author_attribution=True)
    return config


def dated_item(number=1, *, month='2005/01', **kwargs):
    return feed_item(number, **kwargs).replace(f'https://publisher.test/story/{number}',
        f'https://publisher.test/{month}/03/story-{number}/').replace(
            'Thu, 08 Oct 2026 13:20:00 -0400', 'Mon, 03 Jan 2005 13:20:00 -0400')


def test_historical_schedule_preserves_all_existing_source_and_parser_identities(config, tmp_path):
    configured(config)
    old = deepcopy(config)
    old['providers'][0].pop('historical_fulltext_feeds')
    before, after = (PolicyRegistry(c, base=tmp_path).sources['publisher'] for c in (old, config))
    for field in ('data_fingerprint', 'authorization_fingerprint', 'http_capacity_source_fingerprint',
                  'parser_repair_fingerprint', 'fulltext_feed_data_fingerprint'):
        assert getattr(before, field) == getattr(after, field), field
    assert discovery_fingerprint(before) == discovery_fingerprint(after)
    assert after.fulltext_feeds == (ROOT, MONTH)
    assert after.historical_fulltext_feeds == (MONTH,)


@pytest.mark.parametrize('scopes', [None, MONTH, [MONTH, MONTH], ['https://other.test/2005/01/feed/'],
    ['http://publisher.test/2005/01/feed/'], ['https://publisher.test/2005/13/feed/'],
    ['https://publisher.test/2005/00/feed/'], [MONTH + '?paged=2'], [MONTH + '#fragment'],
    ['https://publisher.test/2005/01/'], ['https://publisher.test/wp-json/wp/v2/posts'], [{}]])
def test_rejects_unreviewed_or_unbounded_history_scope(config, tmp_path, scopes):
    configured(config)['providers'][0]['historical_fulltext_feeds'] = scopes
    with pytest.raises(ValueError, match='historical fulltext feeds'):
        PolicyRegistry(config, base=tmp_path)


def test_monthly_scopes_require_a_registered_native_root_and_open_license(config, tmp_path):
    configured(config)['providers'][0]['fulltext_feeds'] = []
    with pytest.raises(ValueError, match='historical fulltext feeds'):
        PolicyRegistry(config, base=tmp_path)


def test_seeding_only_adds_missing_months_and_preserves_existing_failure_deadlines(config, tmp_path):
    configured(config)
    original = deepcopy(config)
    original['providers'][0].pop('historical_fulltext_feeds')
    old = NewsQueue(tmp_path, PolicyRegistry(original, base=tmp_path))
    try:
        seed_discovery(old)
        old.db.execute("UPDATE discovery SET due=?,attempts=6,error_code='invalid_feed',parser_fingerprint=? WHERE url=?",
            (time.time() + 7200, discovery_fingerprint(old.registry.sources['publisher']), ROOT))
        old.db.commit()
        old_row = dict(old.db.execute('SELECT * FROM discovery WHERE url=?', (ROOT,)).fetchone())
    finally:
        old.close()
    current = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        seed_discovery(current)
        seed_discovery(current)
        assert dict(current.db.execute('SELECT * FROM discovery WHERE url=?', (ROOT,)).fetchone()) == old_row
        scopes = current.db.execute('SELECT url FROM discovery ORDER BY url').fetchall()
        assert {r[0] for r in scopes} == {ROOT, MONTH}
        assert current.db.execute('SELECT due FROM discovery WHERE url=?', (MONTH,)).fetchone()[0] == 0
    finally:
        current.close()


def test_monthly_fulltext_batches_share_raw_and_do_not_rewrite_existing_bodies(config, tmp_path):
    configured(config)
    http, _ = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        seed_discovery(queue)
        task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (MONTH,)).fetchone())
        response = Response(MONTH, 200, {}, rss([dated_item(1), dated_item(2)]))
        records, errors = feed_records(response, http, http.registry.sources['publisher'])
        assert len(records) == 2 and not errors
        assert json.loads(records[0]['authors_json']) == ['Real Byline']
        assert records[0]['fulltext_feed_scope_contract_version'] == 1
        assert records[0]['fulltext_feed_scope'] == MONTH
        assert records[0]['published_at_raw'] == 'Mon, 03 Jan 2005 13:20:00 -0400'
        links = [(r['requested_url'], r['title'], r['metadata_date_raw']) for r in records]
        assert accept_discovery(queue, task, response, links, records, errors) == 2
        assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (MONTH,)).fetchone()[0] > time.time() + 364 * 86400
        assert queue.db.execute('SELECT 1 FROM discovery WHERE url=?', (feed_url(MONTH, 2),)).fetchone()
        first = [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY url')]
        changed = [dict(r, body='A different RSS spacing view', body_sha256='a' * 64) for r in records]
        assert accept_discovery(queue, task, response, links, changed, []) == 0
        stable = lambda r: {k: v for k, v in dict(r).items() if k != 'last_seen_utc'}
        assert [stable(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY url')] == [stable(r) for r in first]
        assert verify_bodies(tmp_path)['accepted']
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('foreign', ['2005/02', '2026/10'])
def test_ignored_date_filter_cannot_accept_foreign_month_text(config, tmp_path, foreign):
    configured(config)
    http, _ = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        seed_discovery(queue)
        task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (MONTH,)).fetchone())
        response = Response(MONTH, 200, {}, rss([dated_item(1), dated_item(2, month=foreign)]))
        with pytest.raises(NewsError, match='historical_feed_scope_mismatch'):
            feed_records(response, http, http.registry.sources['publisher'])
        assert not queue.db.execute('SELECT 1 FROM discovery WHERE url=?', (feed_url(MONTH, 2),)).fetchone()
        assert not list(tmp_path.glob('**/*.parquet'))
    finally:
        queue.close()
        http.close()


def test_undeclared_month_refused_before_transport(config, tmp_path):
    configured(config)
    http, session = http_fixture(config, tmp_path, {})
    try:
        task = dict(url='https://publisher.test/2005/02/feed/', provider='publisher', kind='rss_fulltext')
        with pytest.raises(NewsError, match='unregistered_bulk_api'):
            fetch_discovery(http, task)
        assert not session.calls
    finally:
        http.close()


def test_live_gv_schedule_changes_no_other_source_or_original_feed_fingerprints():
    cfg, registry, repo = load_config(Path('configs/news_fulltext.json'))
    prior = deepcopy(cfg)
    entry = next(p for p in prior['providers'] if p['id'] == 'globalvoices')
    assert len(entry.pop('historical_fulltext_feeds')) == 24
    original = PolicyRegistry(prior, base=repo)
    assert set(registry.sources) == set(original.sources) == {p['id'] for p in cfg['providers']}
    for key, current in registry.sources.items():
        old = original.sources[key]
        for field in ('data_fingerprint', 'authorization_fingerprint', 'http_capacity_source_fingerprint',
                      'parser_repair_fingerprint', 'fulltext_feed_data_fingerprint'):
            assert getattr(old, field) == getattr(current, field), (key, field)
        assert discovery_fingerprint(old) == discovery_fingerprint(current)


def test_owned_loopback_fixture_does_not_inherit_remote_monthly_feeds(tmp_path):
    from scripts.benchmark_news_connection_recovery import owned_config

    cfg = owned_config()
    registry = PolicyRegistry(cfg, base=tmp_path)
    assert all(not p.historical_fulltext_feeds and not p.fulltext_feeds for p in registry.sources.values())

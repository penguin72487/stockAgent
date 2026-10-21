"""A finite source's first cohort cannot be a prefix of global alias routing."""
from copy import deepcopy
import json
import sqlite3
import time

import pytest

from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_urls import backfill_original_urls, seed_unfrozen_finite_originals
from test_news_fulltext import config
from test_news_original_queue import alias


def retained_literals(config, root, urls):
    disabled = deepcopy(config)
    disabled['providers'][0]['enabled'] = False
    queue = NewsQueue(root, PolicyRegistry(disabled, base=root))
    try:
        with queue.db:
            queue.add_articles([alias(url) for url in urls])
        while backfill_original_urls(queue)['examined']:
            pass
    finally:
        queue.close()


def finite(config, root):
    settings = deepcopy(config)
    settings['providers'][0].update(acquisition_mode='finite', article_path_pattern='^/approved$')
    return PolicyRegistry(settings, base=root)


def test_complete_indexed_source_snapshot_precedes_first_freeze(config, tmp_path, monkeypatch):
    urls = ['https://publisher.test/approved', 'https://publisher.test/outside']
    retained_literals(config, tmp_path, urls + ['https://unrelated.test/ignored'])
    monkeypatch.setattr('downloader.news_fulltext_urls.resolve_google_literal_url',
                        lambda _: pytest.fail('already verified URL envelope decoded again'))
    queue = NewsQueue(tmp_path, finite(config, tmp_path))
    try:
        rows = queue.db.execute("SELECT url,state,body_sha256 FROM articles WHERE provider='publisher' ORDER BY url").fetchall()
        assert [r[:] for r in rows] == [(urls[0], 'pending', None), (urls[1], 'unsupported_article_path', None)]
        header = queue.db.execute("SELECT urls FROM finite_cohorts WHERE provider='publisher'").fetchone()
        assert header[0] == 2
        assert queue.db.execute("SELECT count(*) FROM finite_cohort_members WHERE provider='publisher'").fetchone()[0] == 2
        proof = json.loads(queue.db.execute("SELECT value FROM queue_settings WHERE key='finite_original_seed_v1:publisher'").fetchone()[0])
        assert proof['literal_aliases'] == proof['distinct_target_urls'] == 2
        assert proof['publisher_requests'] == 0 and proof['global_routing_cursor_changed'] is False
        assert queue.db.execute("SELECT count(*) FROM original_url_resolutions WHERE target_queued=1").fetchone()[0] == 2
        assert queue.db.execute("SELECT count(*) FROM source_links").fetchone()[0] == 0
        checkpoint = json.loads(queue.db.execute("SELECT value FROM queue_settings WHERE key='google_literal_url_routing_v1'").fetchone()[0])
        assert checkpoint['after'] == ''
        assert queue.db.execute("SELECT count(*) FROM articles WHERE host='unrelated.test'").fetchone()[0] == 0
    finally:
        queue.close()


def test_new_literal_cannot_extend_an_existing_immutable_cohort(config, tmp_path):
    retained_literals(config, tmp_path, ['https://publisher.test/approved'])
    registry = finite(config, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        header = queue.db.execute('SELECT * FROM finite_cohorts').fetchone()[:]
        with queue.db:
            queue.add_articles([alias('https://publisher.test/later')])
        backfill_original_urls(queue)
        queue.sync_finite_cohorts()
        assert queue.db.execute('SELECT * FROM finite_cohorts').fetchone()[:] == header
        assert queue.db.execute("SELECT count(*) FROM finite_cohort_members WHERE provider='publisher'").fetchone()[0] == 1
        # The local literal survives; a namespace exclusion is not a body.
        assert queue.db.execute("SELECT state FROM articles WHERE url='https://publisher.test/later'").fetchone()[0] == 'unsupported_article_path'
    finally:
        queue.close()


def test_failed_second_admission_rolls_back_all_seed_updates(config, tmp_path, monkeypatch):
    retained_literals(config, tmp_path, ['https://publisher.test/approved', 'https://publisher.test/outside'])
    from downloader import news_fulltext_urls as module
    original = module._route_literal
    calls = []

    def fail_second(*args):
        calls.append(1)
        if len(calls) == 2:
            raise RuntimeError('synthetic interrupted finite seed')
        return original(*args)

    monkeypatch.setattr(module, '_route_literal', fail_second)
    with pytest.raises(RuntimeError, match='synthetic interrupted finite seed'):
        NewsQueue(tmp_path, finite(config, tmp_path))
    db = sqlite3.connect((tmp_path / 'queue.sqlite3').as_uri() + '?mode=ro', uri=True)
    try:
        assert db.execute("SELECT count(*) FROM articles WHERE provider='publisher'").fetchone()[0] == 0
        assert db.execute('SELECT count(*) FROM finite_cohorts').fetchone()[0] == 0
        assert db.execute('SELECT sum(target_queued) FROM original_url_resolutions').fetchone()[0] == 0
    finally:
        db.close()


@pytest.mark.parametrize('state', ['complete', 'retry_cooldown', 'inflight', 'paywall'])
def test_seed_preserves_saved_heads_denials_deadlines_and_leases(config, tmp_path, state):
    retained_literals(config, tmp_path, ['https://publisher.test/approved'])
    # A failed/saved target is protected even when its source is newly allowed.
    disabled = deepcopy(config)
    disabled['providers'][0]['enabled'] = False
    queue = NewsQueue(tmp_path, PolicyRegistry(disabled, base=tmp_path))
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/approved'}])
            queue.db.execute("UPDATE articles SET state=?,due=?,error_code='retained_source_fact',body_sha256=?,body_bytes=9 WHERE provider='publisher'",
                             (state, time.time() + 3600, 'b' * 64))
        before = queue.db.execute("SELECT state,due,error_code,body_sha256,body_bytes FROM articles WHERE provider='publisher'").fetchone()[:]
        queue.registry = finite(config, tmp_path)
        # Calling within this owner avoids intentionally abandoned-lease recovery
        # during a restart, which has a different, already maintained contract.
        with queue.db:
            queue.db.execute("UPDATE articles SET lease_owner='retained',lease_until=? WHERE provider='publisher'", (time.time() + 3600,))
        lease = queue.db.execute("SELECT lease_owner,lease_until FROM articles WHERE provider='publisher'").fetchone()[:]
        with queue.db:
            assert seed_unfrozen_finite_originals(queue, queue.registry.sources['publisher'])
        assert queue.db.execute("SELECT state,due,error_code,body_sha256,body_bytes FROM articles WHERE provider='publisher'").fetchone()[:] == before
        assert queue.db.execute("SELECT lease_owner,lease_until FROM articles WHERE provider='publisher'").fetchone()[:] == lease
    finally:
        queue.close()


def test_large_snapshot_does_not_admit_or_freeze_a_prefix(config, tmp_path):
    retained_literals(config, tmp_path, ['https://publisher.test/approved', 'https://publisher.test/outside'])
    disabled = deepcopy(config)
    disabled['providers'][0]['enabled'] = False
    queue = NewsQueue(tmp_path, PolicyRegistry(disabled, base=tmp_path))
    try:
        queue.registry = finite(config, tmp_path)
        with queue.db:
            assert seed_unfrozen_finite_originals(queue, queue.registry.sources['publisher'], maximum=1) is False
        assert queue.db.execute("SELECT count(*) FROM articles WHERE provider='publisher'").fetchone()[0] == 0
        assert queue.db.execute('SELECT sum(target_queued) FROM original_url_resolutions').fetchone()[0] == 0
    finally:
        queue.close()


@pytest.mark.parametrize('maximum', [False, 0, 2049, 2.5])
def test_seed_limits_are_not_silently_relaxed(config, tmp_path, maximum):
    queue = NewsQueue(tmp_path, finite(config, tmp_path))
    try:
        with pytest.raises(ValueError, match='bounded_finite_original_seed_required'):
            seed_unfrozen_finite_originals(queue, queue.registry.sources['publisher'], maximum=maximum)
    finally:
        queue.close()


def test_large_scope_freezes_only_after_the_matching_router_finishes(config, tmp_path, monkeypatch):
    retained_literals(config, tmp_path, ['https://publisher.test/approved', 'https://publisher.test/outside'])
    disabled = deepcopy(config)
    disabled['providers'][0]['enabled'] = False
    queue = NewsQueue(tmp_path, PolicyRegistry(disabled, base=tmp_path))
    from downloader import news_fulltext_urls as module
    original_seed = module.seed_unfrozen_finite_originals
    monkeypatch.setattr(module, 'seed_unfrozen_finite_originals', lambda q, s: original_seed(q, s, maximum=1))
    try:
        queue.registry = finite(config, tmp_path)
        module.initialize_original_urls(queue)
        queue.sync_finite_cohorts()
        assert queue.db.execute('SELECT count(*) FROM finite_cohorts').fetchone()[0] == 0
        while module.sync_original_url_routes(queue):
            pass
        queue.sync_finite_cohorts()
        assert queue.db.execute("SELECT urls FROM finite_cohorts WHERE provider='publisher'").fetchone()[0] == 2
        assert queue.db.execute('SELECT sum(target_queued) FROM original_url_resolutions').fetchone()[0] == 2
    finally:
        queue.close()


def test_inconsistent_source_summary_never_commits_a_partial_cohort(config, tmp_path):
    retained_literals(config, tmp_path, ['https://publisher.test/approved', 'https://publisher.test/outside'])
    disabled = deepcopy(config)
    disabled['providers'][0]['enabled'] = False
    queue = NewsQueue(tmp_path, PolicyRegistry(disabled, base=tmp_path))
    try:
        queue.registry = finite(config, tmp_path)
        with queue.db:
            queue.db.execute("UPDATE original_url_host_metrics SET metadata_links=1 WHERE original_host='publisher.test'")
        with pytest.raises(ValueError, match='finite_original_seed_summary_mismatch'):
            queue.sync_finite_cohorts()
        assert queue.db.execute('SELECT count(*) FROM finite_cohorts').fetchone()[0] == 0
        assert queue.db.execute("SELECT count(*) FROM articles WHERE provider='publisher'").fetchone()[0] == 0
        assert queue.db.execute('SELECT sum(target_queued) FROM original_url_resolutions').fetchone()[0] == 0
    finally:
        queue.close()


def test_restart_repairs_derived_summary_before_first_seed(config, tmp_path):
    retained_literals(config, tmp_path, ['https://publisher.test/approved', 'https://publisher.test/outside'])
    # Derived counters can be rebuilt; original literal proofs remain intact.
    db = sqlite3.connect(tmp_path / 'queue.sqlite3')
    with db:
        db.execute("UPDATE original_url_host_metrics SET metadata_links=1 WHERE original_host='publisher.test'")
    db.close()
    queue = NewsQueue(tmp_path, finite(config, tmp_path))
    try:
        assert queue.db.execute("SELECT metadata_links FROM original_url_host_metrics WHERE original_host='publisher.test'").fetchone()[0] == 2
        assert queue.db.execute("SELECT urls FROM finite_cohorts WHERE provider='publisher'").fetchone()[0] == 2
        assert queue.db.execute('SELECT sum(target_queued) FROM original_url_resolutions').fetchone()[0] == 2
    finally:
        queue.close()

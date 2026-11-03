"""Interrupted full-text list pages recover from their own source receipt."""

from concurrent.futures import Future
from contextlib import closing, contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import sqlite3
import time

import pytest

from downloader.news_fulltext import accept_discovery, fetch_discovery, seed_discovery
from downloader.news_fulltext_feed import feed_records, feed_url
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_queue import NewsQueue, utc_now
from test_news_fulltext import bulk_fixture, config
from test_news_provider_efficiency import feed_fixture, feed_item, rss


def interrupted_page(config, tmp_path, monkeypatch, kind):
    if kind == 'rss_fulltext':
        http, queue = feed_fixture(config, tmp_path)
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery WHERE kind='rss_fulltext'").fetchone())
        task['_discovery_request_started_at_utc'] = utc_now()
        response = Response(task['url'], 200, {'content-type': 'application/rss+xml'},
                            rss([feed_item(1), feed_item(2)]))
        task['_discovery_response_observed_at_utc'] = utc_now()
        records, errors = feed_records(response, http, http.registry.sources['publisher'])
        links = [(record['requested_url'], record['title'], record['metadata_date_raw']) for record in records]
        next_url = feed_url(task['url'], 2)
    else:
        queue, http, _, task, _ = bulk_fixture(config, tmp_path, count=100, total=101)
        response, links, records, errors = fetch_discovery(http, task)
        from downloader.news_fulltext_bulk import wordpress_base, wordpress_url
        next_url = wordpress_url(wordpress_base(task['url']), 2)
    registry = http.registry
    assert records and not errors
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
    original_accept = queue.accept

    def crash_after_durable_batch(*args, **kwargs):
        original_accept(*args, **kwargs)
        raise OSError('injected interruption after accepted body batch')

    monkeypatch.setattr(queue, 'accept', crash_after_durable_batch)
    try:
        with pytest.raises(OSError, match='injected interruption'):
            accept_discovery(queue, task, response, links, records, errors)
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()) == before
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
        assert not queue.db.execute('SELECT 1 FROM discovery WHERE url=?', (next_url,)).fetchone()
    finally:
        queue.close()
        http.close()

    return registry, task, next_url, before


@pytest.mark.parametrize('kind', ['rss_fulltext', 'wordpress_posts'])
def test_interrupted_accepted_page_restarts_without_another_source_request(config, tmp_path, monkeypatch, kind):
    registry, task, next_url, before = interrupted_page(config, tmp_path, monkeypatch, kind)
    from downloader.news_fulltext_bulk_checkpoint import recover
    with closing(NewsQueue(tmp_path, registry)) as resumed:
        result = recover(resumed, providers={'publisher'}, max_seconds=10)
        if result['outcomes'] == ['invalid_original']:
            from downloader.news_fulltext_bulk_checkpoint import _recover_one
            _recover_one(resumed, resumed.db.execute('SELECT * FROM bulk_interrupted_page_checkpoints').fetchone())
        assert result['outcomes'] == ['recovered']
        current = resumed.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()
        assert current['response_sha256'] and current['last_checked_at_utc']
        assert current['due'] > time.time()
        assert resumed.db.execute('SELECT 1 FROM discovery WHERE url=?', (next_url,)).fetchone()
        assert resumed.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
        assert recover(resumed, providers={'publisher'}, max_seconds=10)['outcomes'] == []


@pytest.mark.parametrize('kind', ['rss_fulltext', 'wordpress_posts'])
@pytest.mark.parametrize('change', ['newer_head', 'bad_receipt', 'other_provider'])
def test_checkpoint_fences_newer_heads_invalid_receipts_and_inactive_providers(config, tmp_path, monkeypatch, kind, change):
    registry, task, next_url, before = interrupted_page(config, tmp_path, monkeypatch, kind)
    from downloader.news_fulltext_bulk_checkpoint import recover
    with closing(NewsQueue(tmp_path, registry)) as resumed:
        row = resumed.db.execute('SELECT * FROM bulk_interrupted_page_checkpoints').fetchone()
        if change == 'newer_head':
            with resumed.db:
                resumed.db.execute("UPDATE discovery SET error_code='http_429',due=? WHERE url=?", (time.time() + 3600, task['url']))
            expected = dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
        elif change == 'bad_receipt':
            receipt_path = tmp_path / row['receipt_path']
            receipt_path.write_text('{}')
            expected = before
        else:
            expected = before
        providers = {'another_provider'} if change == 'other_provider' else {'publisher'}
        result = recover(resumed, providers=providers, max_seconds=10)
        assert result['outcomes'] == ([] if change == 'other_provider' else
                                      ['head_superseded'] if change == 'newer_head' else ['invalid_original'])
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()) == expected
        assert not resumed.db.execute('SELECT 1 FROM discovery WHERE url=?', (next_url,)).fetchone()
        state = resumed.db.execute('SELECT state FROM bulk_interrupted_page_checkpoints').fetchone()[0]
        assert state == ('pending' if change == 'other_provider' else 'skipped')


@pytest.mark.parametrize('kind', ['rss_fulltext', 'wordpress_posts'])
def test_pending_checkpoint_suppresses_only_its_exact_page(config, tmp_path, monkeypatch, kind):
    registry, task, _, _ = interrupted_page(config, tmp_path, monkeypatch, kind)
    with closing(NewsQueue(tmp_path, registry)) as resumed:
        source = registry.sources['publisher']
        clause, params = resumed.discovery_dispatch_filter(source)
        before = resumed.db.execute('SELECT url FROM discovery WHERE provider=? AND ' + clause,
                                    (source.provider, *params)).fetchall()
        assert task['url'] not in {row[0] for row in before}
        with resumed.db:
            resumed.db.execute("UPDATE bulk_interrupted_page_checkpoints SET state='skipped',outcome='invalid_original'")
        after = resumed.db.execute('SELECT url FROM discovery WHERE provider=? AND ' + clause,
                                   (source.provider, *params)).fetchall()
        assert task['url'] in {row[0] for row in after}


@pytest.mark.parametrize('kind', ['rss_fulltext', 'wordpress_posts'])
def test_orphan_durable_batch_registers_checkpoint_during_owner_restart(config, tmp_path, monkeypatch, kind):
    if kind == 'rss_fulltext':
        http, queue = feed_fixture(config, tmp_path)
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery WHERE kind='rss_fulltext'").fetchone())
        task['_discovery_request_started_at_utc'] = utc_now()
        response = Response(task['url'], 200, {}, rss([feed_item(1)]))
        records, errors = feed_records(response, http, http.registry.sources['publisher'])
        links = [(r['requested_url'], r['title'], r['metadata_date_raw']) for r in records]
    else:
        queue, http, _, task, _ = bulk_fixture(config, tmp_path, count=2)
        response, links, records, errors = fetch_discovery(http, task)
    registry = http.registry

    class PowerLoss(BaseException):
        pass

    def interrupted_before_sqlite(*args, **kwargs):
        raise PowerLoss()

    monkeypatch.setattr(queue, '_accept_records', interrupted_before_sqlite)
    try:
        with pytest.raises(PowerLoss):
            accept_discovery(queue, task, response, links, records, errors)
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert len(list((tmp_path / 'receipts').glob('*.json'))) == 1
    finally:
        queue.close()
        http.close()
    from downloader.news_fulltext_bulk_checkpoint import recover
    with closing(NewsQueue(tmp_path, registry)) as resumed:
        assert resumed.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
        assert resumed.db.execute("SELECT count(*) FROM bulk_interrupted_page_checkpoints WHERE state='pending'").fetchone()[0] == 1
        assert recover(resumed, providers={'publisher'}, max_seconds=10)['outcomes'] == ['recovered']
        assert resumed.db.execute('SELECT response_sha256 FROM discovery WHERE url=?', (task['url'],)).fetchone()[0]


@pytest.mark.parametrize('phase', ['after_accept', 'sqlite_before_accept'])
def test_running_owner_recovers_local_failure_without_refetching(config, tmp_path, monkeypatch, phase):
    from downloader.news_fulltext import run_cycle
    from test_news_fulltext import FakeResponse, http_fixture

    url = 'https://publisher.test/feed/'
    config['providers'][0]['fulltext_feeds'] = [url]
    config['providers'][0]['require_author_attribution'] = True
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(rss([feed_item(1)])),
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        original_accept = queue.accept
        calls = 0

        def fail_after_accept(*args, **kwargs):
            nonlocal calls
            result = original_accept(*args, **kwargs)
            calls += 1
            if calls == 1:
                raise OSError('injected transient page commit failure')
            return result

        if phase == 'after_accept':
            monkeypatch.setattr(queue, 'accept', fail_after_accept)
        else:
            original_records = queue._accept_records

            def fail_sqlite_once(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise sqlite3.OperationalError('injected transient sqlite failure')
                return original_records(*args, **kwargs)

            monkeypatch.setattr(queue, '_accept_records', fail_sqlite_once)
        run_cycle(queue, http, config, max_articles=0, max_discovery_requests=1)
        assert [called for called, _ in session.calls].count(url) == 1
        head = queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()
        assert head['error_code'] is None and head['response_sha256'] and head['due'] > time.time()
        assert queue.db.execute('SELECT 1 FROM discovery WHERE url=?', (feed_url(url, 2),)).fetchone()
        assert queue.db.execute("SELECT count(*) FROM bulk_interrupted_page_checkpoints WHERE state='pending'").fetchone()[0] == 0
        assert queue.db.execute("SELECT value FROM counters WHERE key='bulk_interrupted_page_recovered'").fetchone()[0] == 1


@pytest.mark.parametrize('lane,article_count,batch_size,expected_accepted', [
    (lane, count, batch, expected)
    for lane in ('bulk', 'metadata', 'cnyes')
    for count, batch, expected in ((1, 128, 1), (2, 2, 2), (3, 2, 2))
] + [('metadata_legacy', 1, 128, 1), ('metadata_legacy', 2, 2, 2)])
def test_completed_articles_are_durable_before_next_checkpoint_slice(
        config, tmp_path, monkeypatch, lane, article_count, batch_size, expected_accepted):
    """Local recovery must not defer ready bodies or prevent useful batches."""
    import downloader.news_fulltext as owner
    from downloader.news_fulltext_queue import verify_bodies
    from test_news_fulltext import FakeResponse, article_html, http_fixture

    urls = [f'https://publisher.test/article/{number}' for number in range(1, article_count + 1)]
    if lane == 'bulk':
        import downloader.news_fulltext_bulk_checkpoint as checkpoints
        config['providers'][0]['fulltext_feeds'] = ['https://publisher.test/feed/']
    elif lane in {'metadata', 'metadata_legacy'}:
        import downloader.news_fulltext_metadata_checkpoint as checkpoints
        import downloader.news_fulltext_metadata_legacy as legacy
        if lane == 'metadata':
            monkeypatch.setattr(legacy, 'backfill', lambda *args, **kwargs: {'ready': False, 'next_retry_epoch': None})
    else:
        import downloader.news_fulltext_cnyes_checkpoint as checkpoints
        import downloader.news_fulltext_cnyes_work as work
        import downloader.news_fulltext_cnyes_history as history
        import downloader.news_fulltext_cnyes_priority as priority
        import downloader.news_fulltext_cnyes_batch_priority as batch_priority
        config['providers'].append({**config['providers'][0], 'id': 'cnyes', 'domains': ['cnyes.test']})
        monkeypatch.setattr(work, 'backfill', lambda *args, **kwargs: {'checked_heads': 0})
        monkeypatch.setattr(history, 'backfill', lambda *args, **kwargs: {'exhausted': True})
        monkeypatch.setattr(priority, 'seed', lambda *args, **kwargs: None)
        monkeypatch.setattr(batch_priority, 'seed', lambda *args, **kwargs: None)
    config['batch_articles'] = batch_size
    config['batch_text_bytes'] = 1 << 24
    config['batch_raw_response_bytes'] = 1 << 24
    config['batch_flush_seconds'] = 3600
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        **{url: FakeResponse(article_html()) for url in urls},
    })
    if lane in {'metadata', 'metadata_legacy'}:
        source = http.registry.sources['publisher']
        http.registry.sources['publisher'] = replace(source, metadata_feeds=('https://publisher.test/feed/',))

    class ImmediateExecutor:
        def submit(self, function, *args):
            future = Future()
            try:
                future.set_result(function(*args))
            except Exception as error:
                future.set_exception(error)
            return future

    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        with queue.db:
            queue.add_articles([{'url': url} for url in urls])
        calls, observed = [], []
        real_status = queue.status

        def slow_status(*args, **kwargs):
            result = real_status(*args, **kwargs)
            time.sleep(.06)  # A status projection longer than the recovery poll interval.
            return result

        def recover(q, *args, **kwargs):
            calls.append(1)
            if len(calls) >= 2:
                if (lane == 'metadata_legacy' and article_count == 2
                        and q.db.execute("SELECT count(*) FROM articles WHERE state='complete'").fetchone()[0] < 2):
                    # The second response may already be in flight. Let the
                    # owner collect it and preserve the two-article batch.
                    return {'ready': True, 'next_retry_epoch': time.time() - 1}
                observed.append((q.db.execute("SELECT count(*) FROM articles WHERE state='complete'").fetchone()[0],
                                 q.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0],
                                 verify_bodies(tmp_path)['body_versions_verified']))
                http.stop.set()
                return {'ready': False, 'next_local_retry_epoch': None, 'next_retry_epoch': None}
            return {'ready': True, 'next_local_retry_epoch': None, 'next_retry_epoch': time.time() - 1}

        if lane == 'metadata_legacy':
            monkeypatch.setattr(legacy, 'backfill', recover)
        else:
            monkeypatch.setattr(checkpoints, 'recover', recover)
        monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *args: False)
        monkeypatch.setattr(owner, 'seed_discovery', lambda q: None)
        monkeypatch.setattr(queue, 'status', slow_status)
        owner.run_cycle(queue, http, config, max_articles=article_count, max_discovery_requests=0,
                        executor=ImmediateExecutor(), provider_ids=set(http.registry.sources),
                        continuous_budgets=True, max_run_seconds=10)
        assert observed == [(expected_accepted, 1, expected_accepted)]
        assert sum(called in urls for called, _ in session.calls) == expected_accepted
        assert verify_bodies(tmp_path)['body_versions_verified'] == expected_accepted


@pytest.mark.parametrize('kind', ['rss_fulltext', 'wordpress_posts'])
def test_local_recovery_does_not_clear_an_unordered_provider_cooldown(config, tmp_path, monkeypatch, kind):
    registry, task, _, _ = interrupted_page(config, tmp_path, monkeypatch, kind)
    from downloader.news_fulltext_bulk_checkpoint import recover
    with closing(NewsQueue(tmp_path, registry)) as resumed:
        cooldown = ('publisher', registry.sources['publisher'].data_fingerprint, time.time() + 1800, 'bulk_all_items_unusable')
        with resumed.db:
            resumed.db.execute('INSERT OR REPLACE INTO bulk_adapter_cooldowns VALUES (?,?,?,?)', cooldown)
        assert recover(resumed, providers={'publisher'}, max_seconds=10)['outcomes'] == ['recovered']
        assert tuple(resumed.db.execute('SELECT * FROM bulk_adapter_cooldowns WHERE provider=?', ('publisher',)).fetchone()) == cooldown
        assert resumed.db.execute('SELECT response_sha256 FROM discovery WHERE url=?', (task['url'],)).fetchone()[0]


@pytest.mark.parametrize('kind', ['rss_fulltext', 'wordpress_posts'])
def test_local_recovery_preserves_newer_scope_observation(config, tmp_path, monkeypatch, kind):
    registry, task, next_url, before = interrupted_page(config, tmp_path, monkeypatch, kind)
    from downloader.news_fulltext_bulk import wordpress_base
    from downloader.news_fulltext_bulk_checkpoint import recover
    base = wordpress_base(task['url'])
    newer = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    with closing(NewsQueue(tmp_path, registry)) as resumed:
        with resumed.db:
            if kind == 'rss_fulltext':
                resumed.db.execute('INSERT INTO bulk_page_evidence VALUES (?,?,?,?,?)', (base, 1, -1, 'other', newer))
            else:
                resumed.observe_bulk_coverage(base, 'publisher', 1, 1, newer)
        assert recover(resumed, providers={'publisher'}, max_seconds=10)['outcomes'] == ['newer_scope_observation']
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()) == before
        assert not resumed.db.execute('SELECT 1 FROM discovery WHERE url=?', (next_url,)).fetchone()


def test_transient_local_read_failure_retries_without_new_source_request(config, tmp_path, monkeypatch):
    registry, task, _, before = interrupted_page(config, tmp_path, monkeypatch, 'rss_fulltext')
    import downloader.news_fulltext_bulk_checkpoint as module
    original_read = module._read_original
    attempts = 0

    def transient(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError('injected local read failure')
        return original_read(*args, **kwargs)

    monkeypatch.setattr(module, '_read_original', transient)
    with closing(NewsQueue(tmp_path, registry)) as resumed:
        assert module.recover(resumed, providers={'publisher'}, max_seconds=10)['outcomes'] == ['local_io_retry']
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()) == before
        row = resumed.db.execute('SELECT * FROM bulk_interrupted_page_checkpoints').fetchone()
        assert row['state'] == 'pending' and row['attempts'] == 1 and row['not_before'] > time.time()
        with resumed.db:
            resumed.db.execute('UPDATE bulk_interrupted_page_checkpoints SET not_before=0')
        assert module.recover(resumed, providers={'publisher'}, max_seconds=10)['outcomes'] == ['recovered']
        assert attempts == 2


@pytest.mark.parametrize('kind', ['rss_fulltext', 'wordpress_posts'])
@pytest.mark.parametrize('page_content', ['already_saved', 'empty'])
def test_interrupted_no_new_body_page_recovers_from_exact_source_original(config, tmp_path, kind, page_content):
    if kind == 'rss_fulltext':
        http, queue = feed_fixture(config, tmp_path)
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery WHERE kind='rss_fulltext'").fetchone())
        response = Response(task['url'], 200, {}, rss([] if page_content == 'empty' else [feed_item(1)]))
        records, errors = feed_records(response, http, http.registry.sources['publisher'])
        links = [(r['requested_url'], r['title'], r['metadata_date_raw']) for r in records]
    else:
        queue, http, _, task, _ = bulk_fixture(config, tmp_path, count=0 if page_content == 'empty' else 2)
        response, links, records, errors = fetch_discovery(http, task)
    registry = http.registry
    try:
        if page_content == 'already_saved':
            assert accept_discovery(queue, task, response, links, records, errors) == len(records)
            with queue.db:
                queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (task['url'],))
            task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
        task['_discovery_request_started_at_utc'] = utc_now()
        task['_discovery_response_observed_at_utc'] = utc_now()
        if kind == 'rss_fulltext':
            records, errors = feed_records(response, http, registry.sources['publisher'])
        else:
            from downloader.news_fulltext_bulk import wordpress_records
            records, errors = wordpress_records(response, http, registry.sources['publisher'])
        links = [(r['requested_url'], r['title'], r['metadata_date_raw']) for r in records]
        before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
        old_batch_count = queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0]
        queue.db.execute("CREATE TEMP TRIGGER fail_page_commit BEFORE UPDATE ON discovery "
                         "WHEN NEW.response_sha256 IS NOT NULL BEGIN "
                         "SELECT RAISE(ABORT,'injected page transaction failure'); END")
        with pytest.raises(sqlite3.IntegrityError, match='injected page transaction failure'):
            accept_discovery(queue, task, response, links, records, errors)
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()) == before
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == old_batch_count
        queue.db.execute('DROP TRIGGER fail_page_commit')
    finally:
        queue.close()
        http.close()
    from downloader.news_fulltext_bulk_checkpoint import recover
    with closing(NewsQueue(tmp_path, registry)) as resumed:
        assert recover(resumed, providers={'publisher'}, max_seconds=10)['outcomes'] == ['recovered']
        head = resumed.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()
        assert head['response_sha256'] and head['last_checked_at_utc'] and head['due'] > time.time()
        assert resumed.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == old_batch_count
        assert resumed.db.execute('SELECT count(*) FROM bulk_interrupted_page_payloads').fetchone()[0] == 0


@pytest.mark.parametrize('kind', ['rss_fulltext', 'wordpress_posts'])
@pytest.mark.parametrize('change', ['bad_payload', 'bad_identity', 'newer_head'])
def test_no_body_journal_never_overwrites_newer_or_unverified_evidence(config, tmp_path, kind, change):
    if kind == 'rss_fulltext':
        http, queue = feed_fixture(config, tmp_path)
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery WHERE kind='rss_fulltext'").fetchone())
        response = Response(task['url'], 200, {}, rss([]))
        records, errors = feed_records(response, http, http.registry.sources['publisher'])
        links = []
    else:
        queue, http, _, task, _ = bulk_fixture(config, tmp_path, count=0)
        response, links, records, errors = fetch_discovery(http, task)
    registry = http.registry
    try:
        task['_discovery_request_started_at_utc'] = utc_now()
        task['_discovery_response_observed_at_utc'] = utc_now()
        queue.db.execute("CREATE TEMP TRIGGER fail_page_commit BEFORE UPDATE ON discovery "
                         "WHEN NEW.response_sha256 IS NOT NULL BEGIN "
                         "SELECT RAISE(ABORT,'injected page transaction failure'); END")
        with pytest.raises(sqlite3.IntegrityError):
            accept_discovery(queue, task, response, links, records, errors)
        queue.db.execute('DROP TRIGGER fail_page_commit')
        with queue.db:
            if change == 'bad_payload':
                queue.db.execute("UPDATE bulk_interrupted_page_payloads SET payload=x'00'")
            elif change == 'bad_identity':
                queue.db.execute("UPDATE bulk_interrupted_page_checkpoints SET response_sha256=?", ('a' * 64,))
            else:
                queue.db.execute("UPDATE discovery SET error_code='http_429',due=? WHERE url=?",
                                 (time.time() + 3600, task['url']))
        before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
    finally:
        queue.close()
        http.close()
    from downloader.news_fulltext_bulk_checkpoint import recover
    with closing(NewsQueue(tmp_path, registry)) as resumed:
        expected = 'head_superseded' if change == 'newer_head' else 'invalid_original'
        assert recover(resumed, providers={'publisher'}, max_seconds=10)['outcomes'] == [expected]
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()) == before
        assert resumed.db.execute('SELECT state FROM bulk_interrupted_page_checkpoints').fetchone()[0] == 'skipped'
        assert resumed.db.execute('SELECT count(*) FROM bulk_interrupted_page_payloads').fetchone()[0] == 1


@pytest.mark.parametrize('kind', ['rss_fulltext', 'wordpress_posts'])
def test_successful_empty_page_leaves_no_recovery_payload(config, tmp_path, kind):
    if kind == 'rss_fulltext':
        http, queue = feed_fixture(config, tmp_path)
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery WHERE kind='rss_fulltext'").fetchone())
        response = Response(task['url'], 200, {}, rss([]))
        records, errors = feed_records(response, http, http.registry.sources['publisher'])
        links = []
    else:
        queue, http, _, task, _ = bulk_fixture(config, tmp_path, count=0)
        response, links, records, errors = fetch_discovery(http, task)
    try:
        task['_discovery_request_started_at_utc'] = utc_now()
        task['_discovery_response_observed_at_utc'] = utc_now()
        assert accept_discovery(queue, task, response, links, records, errors) == 0
        assert queue.db.execute('SELECT count(*) FROM bulk_interrupted_page_payloads').fetchone()[0] == 0
        assert queue.db.execute('SELECT count(*) FROM bulk_interrupted_page_checkpoints').fetchone()[0] == 0
    finally:
        queue.close()
        http.close()


def test_running_owner_recovers_empty_page_journal_without_refetch(config, tmp_path, monkeypatch):
    from downloader.news_fulltext import run_cycle
    from test_news_fulltext import FakeResponse, http_fixture

    url = 'https://publisher.test/feed/'
    config['providers'][0]['fulltext_feeds'] = [url]
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(rss([])),
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        original_measure = queue.metrics.measure
        calls = 0

        @contextmanager
        def fail_once_after_registration(stage, provider=None):
            nonlocal calls
            with original_measure(stage, provider):
                yield
            if stage == 'discovery_registration' and not calls:
                calls += 1
                assert not queue.db.in_transaction
                assert queue.db.execute("SELECT 1 FROM bulk_interrupted_page_checkpoints WHERE state='pending'").fetchone()
                raise OSError('injected interruption after journal commit')

        monkeypatch.setattr(queue.metrics, 'measure', fail_once_after_registration)
        run_cycle(queue, http, config, max_articles=0, max_discovery_requests=1)
        assert [called for called, _ in session.calls].count(url) == 1
        head = queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()
        assert head['error_code'] is None and head['response_sha256'] and head['due'] > time.time()
        assert queue.db.execute('SELECT count(*) FROM bulk_interrupted_page_payloads').fetchone()[0] == 0
        assert queue.db.execute("SELECT value FROM counters WHERE key='bulk_interrupted_page_recovered'").fetchone()[0] == 1


def test_no_body_journal_and_article_registration_rollback_together(config, tmp_path, monkeypatch):
    import downloader.news_fulltext_bulk_checkpoint as checkpoints

    http, queue = feed_fixture(config, tmp_path)
    try:
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery WHERE kind='rss_fulltext'").fetchone())
        task['_discovery_request_started_at_utc'] = utc_now()
        task['_discovery_response_observed_at_utc'] = utc_now()
        response = Response(task['url'], 200, {}, rss([]))
        records, errors = feed_records(response, http, http.registry.sources['publisher'])
        before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
        original = checkpoints.register_no_body

        def fail_before_registration_commit(*args, **kwargs):
            assert original(*args, **kwargs)
            assert queue.db.in_transaction
            raise OSError('injected interruption before registration commit')

        monkeypatch.setattr(checkpoints, 'register_no_body', fail_before_registration_commit)
        with pytest.raises(OSError, match='before registration commit'):
            accept_discovery(queue, task, response, [('https://publisher.test/77', 'article', None)], records, errors)
        assert not queue.db.in_transaction
        assert not queue.db.execute('SELECT 1 FROM articles WHERE url=?', ('https://publisher.test/77',)).fetchone()
        assert not queue.db.execute('SELECT 1 FROM bulk_interrupted_page_checkpoints').fetchone()
        assert not queue.db.execute('SELECT 1 FROM bulk_interrupted_page_payloads').fetchone()
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()) == before
        assert not checkpoints.defer_live_failure(queue, task, response, OSError('interrupted'))
    finally:
        queue.close()
        http.close()


def test_oversize_journal_preserves_normal_page_processing(config, tmp_path, monkeypatch):
    import downloader.news_fulltext_bulk_checkpoint as checkpoints
    http, queue = feed_fixture(config, tmp_path)
    try:
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery WHERE kind='rss_fulltext'").fetchone())
        task['_discovery_request_started_at_utc'] = utc_now()
        task['_discovery_response_observed_at_utc'] = utc_now()
        response = Response(task['url'], 200, {}, rss([]))
        records, errors = feed_records(response, http, http.registry.sources['publisher'])
        monkeypatch.setattr(checkpoints, 'MAX_JOURNAL_BODY_BYTES', 1)
        original = checkpoints.register_no_body
        attempts = []

        def count_attempts(*args, **kwargs):
            attempts.append(True)
            return original(*args, **kwargs)

        monkeypatch.setattr(checkpoints, 'register_no_body', count_attempts)
        assert accept_discovery(queue, task, response, [], records, errors) == 0
        assert attempts == [True]
        assert queue.db.execute('SELECT count(*) FROM bulk_interrupted_page_payloads').fetchone()[0] == 0
        assert queue.db.execute('SELECT response_sha256 FROM discovery WHERE url=?', (task['url'],)).fetchone()[0]
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('interrupt', [False, True])
def test_large_journal_compression_stays_outside_registration_transaction(config, tmp_path, monkeypatch, interrupt):
    import downloader.news_fulltext_bulk_checkpoint as checkpoints

    http, queue = feed_fixture(config, tmp_path)
    try:
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery WHERE kind='rss_fulltext'").fetchone())
        task['_discovery_request_started_at_utc'] = utc_now()
        task['_discovery_response_observed_at_utc'] = utc_now()
        response = Response(task['url'], 200, {}, rss([]) + b' ' * (1 << 20))
        records, errors = feed_records(response, http, http.registry.sources['publisher'])
        original_compress = checkpoints.zlib.compress
        original_register = checkpoints.register_no_body
        transaction_states = []

        def observe_compress(*args, **kwargs):
            transaction_states.append(queue.db.in_transaction)
            return original_compress(*args, **kwargs)

        def optionally_interrupt(*args, **kwargs):
            path = original_register(*args, **kwargs)
            assert path and not queue.db.in_transaction
            if interrupt:
                raise OSError('injected interruption after large journal commit')
            return path

        monkeypatch.setattr(checkpoints.zlib, 'compress', observe_compress)
        monkeypatch.setattr(checkpoints, 'register_no_body', optionally_interrupt)
        if interrupt:
            with pytest.raises(OSError, match='after large journal commit'):
                accept_discovery(queue, task, response, [], records, errors)
            assert checkpoints.defer_live_failure(queue, task, response, OSError('interrupted'))
            assert checkpoints.recover(queue, providers={'publisher'}, max_seconds=10)['outcomes'] == ['recovered']
        else:
            assert accept_discovery(queue, task, response, [], records, errors) == 0
        assert transaction_states == [False]
        assert not queue.db.execute('SELECT 1 FROM bulk_interrupted_page_payloads').fetchone()
        assert queue.db.execute('SELECT response_sha256 FROM discovery WHERE url=?', (task['url'],)).fetchone()[0]
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('kind', ['rss_fulltext', 'wordpress_posts'])
@pytest.mark.parametrize('page_content', ['empty', 'already_saved'])
def test_eligible_no_body_journal_shares_article_registration_commit(config, tmp_path, kind, page_content):
    if kind == 'rss_fulltext':
        http, queue = feed_fixture(config, tmp_path)
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery WHERE kind='rss_fulltext'").fetchone())
        response = Response(task['url'], 200, {}, rss([] if page_content == 'empty' else [feed_item(1)]))
        records, errors = feed_records(response, http, http.registry.sources['publisher'])
        links = [(r['requested_url'], r['title'], r['metadata_date_raw']) for r in records]
    else:
        queue, http, _, task, _ = bulk_fixture(config, tmp_path, count=0 if page_content == 'empty' else 2)
        response, links, records, errors = fetch_discovery(http, task)
    try:
        if page_content == 'already_saved':
            assert accept_discovery(queue, task, response, links, records, errors) == len(records)
            with queue.db:
                queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (task['url'],))
            task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
        task['_discovery_request_started_at_utc'] = utc_now()
        task['_discovery_response_observed_at_utc'] = utc_now()
        statements = []
        queue.db.set_trace_callback(statements.append)
        assert accept_discovery(queue, task, response, links, records, errors) == 0
        queue.db.set_trace_callback(None)
        assert sum(sql == 'COMMIT' for sql in statements) == 2
        assert queue.db.execute('SELECT count(*) FROM bulk_interrupted_page_payloads').fetchone()[0] == 0
        assert queue.db.execute('SELECT response_sha256 FROM discovery WHERE url=?', (task['url'],)).fetchone()[0]
    finally:
        queue.close()
        http.close()


def test_pending_same_body_revalidation_finishes_before_no_body_journal(config, tmp_path, monkeypatch):
    http, queue = feed_fixture(config, tmp_path)
    try:
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery WHERE kind='rss_fulltext'").fetchone())
        response = Response(task['url'], 200, {}, rss([feed_item(1)]))
        records, errors = feed_records(response, http, http.registry.sources['publisher'])
        links = [(r['requested_url'], r['title'], r['metadata_date_raw']) for r in records]
        assert accept_discovery(queue, task, response, links, records, errors) == 1
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',due=0 WHERE url=?", (records[0]['requested_url'],))
            queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (task['url'],))
        task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
        task['_discovery_request_started_at_utc'] = utc_now()
        task['_discovery_response_observed_at_utc'] = utc_now()
        records, errors = feed_records(response, http, http.registry.sources['publisher'])
        original = queue.unchanged
        called = []

        def observe_unchanged(*args, **kwargs):
            assert not queue.db.execute("SELECT 1 FROM bulk_interrupted_page_checkpoints WHERE state='pending'").fetchone()
            called.append(True)
            return original(*args, **kwargs)

        monkeypatch.setattr(queue, 'unchanged', observe_unchanged)
        assert accept_discovery(queue, task, response, links, records, errors) == 0
        assert called == [True]
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (records[0]['requested_url'],)).fetchone()[0] == 'complete'
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('kind', ['rss_fulltext', 'wordpress_posts'])
def test_article_advance_after_registration_is_not_overwritten(config, tmp_path, monkeypatch, kind):
    if kind == 'rss_fulltext':
        http, queue = feed_fixture(config, tmp_path)
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery WHERE kind='rss_fulltext'").fetchone())
        task['_discovery_request_started_at_utc'] = utc_now()
        response = Response(task['url'], 200, {}, rss([feed_item(1)]))
        records, errors = feed_records(response, http, http.registry.sources['publisher'])
        links = [(record['requested_url'], record['title'], record['metadata_date_raw']) for record in records]
    else:
        queue, http, _, task, _ = bulk_fixture(config, tmp_path, count=1)
        task['_discovery_request_started_at_utc'] = utc_now()
        response, links, records, errors = fetch_discovery(http, task)
    try:
        task['_discovery_response_observed_at_utc'] = utc_now()
        original_accept = queue.accept
        changed = []

        def advance_then_accept(batch, **kwargs):
            assert not queue.db.in_transaction
            with queue.db:
                queue.db.execute("UPDATE articles SET state='retry',error_code='http_429',due=?,last_checked_at_utc=? WHERE url=?",
                                 (time.time() + 3600, utc_now(), records[0]['requested_url']))
            changed.append(True)
            return original_accept(batch, **kwargs)

        monkeypatch.setattr(queue, 'accept', advance_then_accept)
        assert accept_discovery(queue, task, response, links, records, errors) == 1
        article = queue.db.execute('SELECT state,error_code,body_sha256 FROM articles WHERE url=?',
                                   (records[0]['requested_url'],)).fetchone()
        assert changed == [True]
        assert (article['state'], article['error_code'], article['body_sha256']) == ('retry', 'http_429', None)
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
    finally:
        queue.close()
        http.close()

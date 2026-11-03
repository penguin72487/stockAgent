"""Synthetic HTTP/cache contracts; never downloads publisher articles."""
from contextlib import closing
import json
import sqlite3
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import ETTODAY_LAYOUT_VERSIONS, NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, unleased_article_head_witness, verify_bodies
from test_news_fulltext import config, FakeResponse, article_html, http_fixture
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks


URL = 'https://publisher.test/a'
TAG = '"saved-representation"'


def saved_head(queue, http, *, source_url=None):
    queue.add_articles([{'url': URL}])
    record = fetch_article(http, queue.claim_url(URL, 300))
    if source_url:
        record['source_request_url'] = source_url
    queue.accept([record])
    return record


def test_older_unleased_live_response_keeps_newer_article_failure(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html()),
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        queue.add_articles([{'url': URL}])
        record = fetch_article(http, dict(queue.db.execute('SELECT * FROM articles').fetchone()))
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',error_code='http_429',attempts=3,"
                             "last_checked_at_utc='2099-01-01T00:00:00+00:00' WHERE url=?", (URL,))
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        record['unleased_article_head_witness_json'] = unleased_article_head_witness(before)
        _, adopted = queue.accept([record], return_adopted=True)
        current = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        assert bytes.fromhex(record['article_id']) in adopted
        for key in ('state', 'error_code', 'attempts', 'last_checked_at_utc', 'due'):
            assert current[key] == before[key]
        assert current['body_sha256'] == record['body_sha256']
        assert verify_bodies(tmp_path)['accepted']


def test_older_unleased_live_response_never_replaces_a_newer_body(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: [FakeResponse(article_html(body='這是較早版本的完整新聞內容。' * 20)),
              FakeResponse(article_html(body='這是較新版本的完整新聞內容。' * 20))],
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        queue.add_articles([{'url': URL}])
        row = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        older, newer = fetch_article(http, row), fetch_article(http, row)
        assert older['captured_at_utc'] < newer['captured_at_utc']
        queue.accept([newer])
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        older['unleased_article_head_witness_json'] = unleased_article_head_witness(before)
        _, adopted = queue.accept([older], return_adopted=True)
        assert not adopted
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 2
        assert verify_bodies(tmp_path)['accepted']


def test_newer_unleased_live_response_updates_an_older_body(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: [FakeResponse(article_html(body='這是較早版本的完整新聞內容。' * 20)),
              FakeResponse(article_html(body='這是較新版本的完整新聞內容。' * 20))],
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        queue.add_articles([{'url': URL}])
        row = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        older, newer = fetch_article(http, row), fetch_article(http, row)
        assert older['captured_at_utc'] < newer['captured_at_utc']
        queue.accept([older])
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        newer['unleased_article_head_witness_json'] = unleased_article_head_witness(before)
        _, adopted = queue.accept([newer], return_adopted=True)
        current = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        assert bytes.fromhex(newer['article_id']) in adopted
        assert current['body_sha256'] == newer['body_sha256']
        assert current['last_checked_at_utc'] == newer['captured_at_utc']
        assert verify_bodies(tmp_path)['accepted']


@pytest.mark.parametrize(('started', 'may_clear'), [
    ('2026-01-01T00:00:00+00:00', False),
    ('2026-01-03T00:00:00+00:00', True),
    (None, False), ('bad clock', False), ('2026-01-05T00:00:00+00:00', False),
])
def test_unleased_request_witness_keeps_unordered_failure_but_stores_missing_body(config, tmp_path, started, may_clear):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html()),
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        queue.add_articles([{'url': URL}])
        record = fetch_article(http, dict(queue.db.execute('SELECT * FROM articles').fetchone()))
        record['captured_at_utc'] = '2026-01-04T00:00:00+00:00'
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',error_code='http_429',attempts=3,due=0,"
                             "last_checked_at_utc='2026-01-02T00:00:00+00:00' WHERE url=?", (URL,))
        head = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        record['unleased_article_head_witness_json'] = unleased_article_head_witness(head, request_started_at_utc=started)
        witness = json.loads(record['unleased_article_head_witness_json'])
        assert witness['contract_version'] == 2
        if started in (None, 'bad clock'):
            assert witness['request_started_at_utc'] is None
        queue.accept([record])
        current = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        assert current['body_sha256'] == record['body_sha256']
        if may_clear:
            assert current['state'] == 'complete' and current['error_code'] is None
            assert current['last_checked_at_utc'] == record['captured_at_utc']
        else:
            assert (current['state'], current['error_code'], current['attempts'], current['last_checked_at_utc']) == (
                head['state'], head['error_code'], head['attempts'], head['last_checked_at_utc'])
        assert verify_bodies(tmp_path)['accepted']


def test_recovery_keeps_newer_body_when_unleased_receipt_started_earlier(config, tmp_path, monkeypatch):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: [FakeResponse(article_html(body='這是較新的完整新聞版本。' * 20)),
              FakeResponse(article_html(body='這是較舊的完整新聞版本。' * 20))],
    })
    with closing(http):
        queue = NewsQueue(tmp_path, http.registry)
        try:
            saved_head(queue, http)
            older = fetch_article(http, dict(queue.db.execute('SELECT * FROM articles').fetchone()))
            older['captured_at_utc'] = '2026-01-03T00:00:00+00:00'
            with queue.db:
                queue.db.execute("UPDATE articles SET state='retry',error_code='http_429',attempts=3,due=0,"
                                 "captured_at_utc='2026-01-02T00:00:00+00:00',"
                                 "last_checked_at_utc='2026-01-02T00:00:00+00:00' WHERE url=?", (URL,))
            before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
            older['unleased_article_head_witness_json'] = unleased_article_head_witness(
                before, request_started_at_utc='2026-01-01T00:00:00+00:00')
            monkeypatch.setattr(queue, '_accept_records',
                                lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError('simulated commit interruption')))
            with pytest.raises(OSError, match='simulated commit interruption'):
                queue.accept([older])
            assert len(list((tmp_path / 'receipts').glob('*.json'))) == 2
        finally:
            queue.close()
        with closing(NewsQueue(tmp_path, http.registry)) as restored:
            after = dict(restored.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
            for key in ('body_sha256', 'parquet_path', 'captured_at_utc', 'state', 'error_code', 'attempts', 'last_checked_at_utc'):
                assert after[key] == before[key]
            assert restored.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 2
            assert verify_bodies(tmp_path)['accepted']


@pytest.mark.parametrize('head_kind', ['pristine', 'later_failure', 'existing_body', 'error_without_clock'])
def test_legacy_unleased_receipt_recovery_never_invents_request_order(config, tmp_path, monkeypatch, head_kind):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: [FakeResponse(article_html(body='這是已儲存的完整新聞版本。' * 20)),
              FakeResponse(article_html(body='這是中斷收據的完整新聞版本。' * 20))],
    })
    with closing(http):
        queue = NewsQueue(tmp_path, http.registry)
        try:
            if head_kind == 'existing_body':
                saved_head(queue, http)
            else:
                with queue.db:
                    queue.add_articles([{'url': URL}])
            incoming = fetch_article(http, dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()))
            incoming['captured_at_utc'] = '2026-01-03T00:00:00+00:00'
            if head_kind != 'pristine':
                with queue.db:
                    queue.db.execute("UPDATE articles SET state='retry',error_code='http_429',attempts=3,due=0,"
                                     "captured_at_utc=CASE WHEN body_sha256 IS NOT NULL THEN ? ELSE captured_at_utc END,"
                                     "last_checked_at_utc=? WHERE url=?",
                                     ('2026-01-02T00:00:00+00:00',
                                      None if head_kind == 'error_without_clock' else '2026-01-02T00:00:00+00:00', URL))
            before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
            incoming['unleased_article_head_witness_json'] = unleased_article_head_witness(before)
            assert json.loads(incoming['unleased_article_head_witness_json'])['contract_version'] == 1
            monkeypatch.setattr(queue, '_accept_records',
                                lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError('simulated commit interruption')))
            with pytest.raises(OSError, match='simulated commit interruption'):
                queue.accept([incoming])
        finally:
            queue.close()
        with closing(NewsQueue(tmp_path, http.registry)) as restored:
            after = dict(restored.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
            if head_kind == 'existing_body':
                for key in ('body_sha256', 'parquet_path', 'captured_at_utc', 'state', 'error_code', 'attempts', 'last_checked_at_utc'):
                    assert after[key] == before[key]
            else:
                assert after['body_sha256'] == incoming['body_sha256']
                if head_kind == 'pristine':
                    assert after['state'] == 'complete' and after['error_code'] is None
                else:
                    for key in ('state', 'error_code', 'attempts', 'last_checked_at_utc', 'due'):
                        assert after[key] == before[key]
            assert restored.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == (
                2 if head_kind == 'existing_body' else 1)
            assert verify_bodies(tmp_path)['accepted']


@pytest.mark.parametrize(('started', 'may_replace'), [
    (None, False), ('2026-01-01T00:00:00', False),
    ('2026-01-05T00:00:00+00:00', False), ('2026-01-02T12:00:00+00:00', True),
])
def test_unleased_request_witness_only_replaces_body_with_ordered_request(config, tmp_path, started, may_replace):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: [FakeResponse(article_html(body='原本已儲存的完整新聞版本。' * 20)),
              FakeResponse(article_html(body='新回應中的不同完整新聞版本。' * 20))],
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        saved_head(queue, http)
        incoming = fetch_article(http, dict(queue.db.execute('SELECT * FROM articles').fetchone()))
        incoming['captured_at_utc'] = '2026-01-03T00:00:00+00:00'
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',error_code='http_429',attempts=3,due=0,"
                             "captured_at_utc='2026-01-02T00:00:00+00:00',"
                             "last_checked_at_utc='2026-01-02T00:00:00+00:00' WHERE url=?", (URL,))
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        incoming['unleased_article_head_witness_json'] = unleased_article_head_witness(before, request_started_at_utc=started)
        queue.accept([incoming])
        after = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        if may_replace:
            assert after['body_sha256'] == incoming['body_sha256']
            assert (after['state'], after['error_code'], after['last_checked_at_utc']) == (
                'complete', None, incoming['captured_at_utc'])
        else:
            for key in ('body_sha256', 'parquet_path', 'captured_at_utc', 'state', 'error_code', 'attempts', 'last_checked_at_utc'):
                assert after[key] == before[key]
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 2
        assert verify_bodies(tmp_path)['accepted']


def test_request_started_without_observation_cannot_clear_retry(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html()),
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        saved_head(queue, http)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',error_code='http_429',due=0 WHERE url=?", (URL,))
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        queue.unchanged(before, request_started_at_utc='2099-01-01T00:00:00+00:00')
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before


@pytest.mark.parametrize('clock_kind', ['equal', 'bad_observation', 'bad_previous',
                                         'newer_body', 'missing_body_clock', 'bad_body_clock'])
def test_bulk_revalidation_rejects_unordered_clocks_without_reading_body(config, tmp_path, monkeypatch, clock_kind):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html()),
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        record = saved_head(queue, http)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',error_code='http_429',due=0,last_checked_at_utc=? WHERE url=?",
                             ('invalid-clock' if clock_kind == 'bad_previous' else
                              '2000-01-01T00:00:00+00:00' if clock_kind in {'newer_body', 'missing_body_clock', 'bad_body_clock'}
                              else record['captured_at_utc'], URL))
            if clock_kind in {'newer_body', 'missing_body_clock', 'bad_body_clock'}:
                body_clock = {'newer_body': '2099-01-01T00:00:00+00:00',
                              'missing_body_clock': None, 'bad_body_clock': 'invalid-clock'}[clock_kind]
                queue.db.execute('UPDATE articles SET captured_at_utc=? WHERE url=?', (body_clock, URL))
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        monkeypatch.setattr(queue, '_saved_body_record', lambda *a, **k: (_ for _ in ()).throw(AssertionError('unnecessary body read')))
        queue.unchanged(before, observed_at_utc=('invalid-clock' if clock_kind == 'bad_observation' else record['captured_at_utc']))
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before


@pytest.mark.parametrize('version', sorted(ETTODAY_LAYOUT_VERSIONS.values()))
def test_304_cannot_bypass_any_registered_ettoday_transport_identity(version):
    url = native.URLS[0]
    http = SimpleNamespace(registry=clocks.registry(version), get=lambda *a, **kw:
        Response(url + '1', 304, {}, b''))
    with pytest.raises(NewsError, match='article_identity_not_verified') as caught:
        fetch_article(http, {'url': url, 'id': b'1' * 16, 'body_sha256': 'a' * 64,
                            'parquet_path': 'saved.parquet', 'etag': TAG, 'validator_url': url})
    assert caught.value.failure_response.response.status == 304


@pytest.mark.parametrize('missing', ['body_sha256', 'parquet_path', 'validator_url', 'etag'])
def test_unsolicited_or_unbound_304_never_confirms_freshness(config, tmp_path, missing):
    registry = PolicyRegistry(config, base=tmp_path)
    row = {'url': URL, 'id': b'1' * 16, 'body_sha256': 'a' * 64,
           'parquet_path': 'saved.parquet', 'etag': TAG, 'validator_url': URL}
    row.pop(missing)
    calls = []
    def get(url, **kwargs):
        calls.append(kwargs)
        return Response(url, 304, {}, b'')
    with pytest.raises(NewsError, match='unverifiable_not_modified'):
        fetch_article(SimpleNamespace(registry=registry, get=get), row)
    if missing in {'body_sha256', 'validator_url', 'etag'}:
        assert calls == [{'headers': {}}]


@pytest.mark.parametrize('final,body', [(URL + '1', b''), (URL + '?other=1', b''),
                                      (URL, b'not a valid 304 body')])
def test_304_requires_exact_representation_url_and_empty_content(config, tmp_path, final, body):
    registry = PolicyRegistry(config, base=tmp_path)
    with pytest.raises(NewsError, match='unverifiable_not_modified'):
        fetch_article(SimpleNamespace(registry=registry, get=lambda *a, **kw:
            Response(final, 304, {}, body)), {'url': URL, 'id': b'1' * 16,
                'body_sha256': 'a' * 64, 'parquet_path': 'saved.parquet',
                'etag': TAG, 'validator_url': URL})


@pytest.mark.parametrize('bound', [URL, URL + '-target', URL + '-unreached', None])
def test_redirects_send_only_bound_validators_and_never_forward_other_headers(config, tmp_path, bound):
    final = URL + '-target'
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(status=302, headers={'Location': final}),
        final: FakeResponse(article_html()),
    })
    with closing(http):
        kwargs = {'conditional_url': bound} if bound else {}
        assert http.get(URL, headers={'If-None-Match': TAG, 'If-Modified-Since': 'old-clock',
                                     'X-Initial-Only': 'original'}, **kwargs).status == 200
    calls = {url: request['headers'] for url, request in session.calls if url in {URL, final}}
    for url in (URL, final):
        expected = url == (bound or URL)
        assert (calls[url].get('If-None-Match') == TAG) == expected
        assert (calls[url].get('If-Modified-Since') == 'old-clock') == expected
        assert (calls[url].get('X-Initial-Only') == 'original') == (url == URL)


def test_bound_redirect_still_checks_destination_robots(config, tmp_path):
    final = URL + '-private'
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nDisallow: /a-private'),
        URL: FakeResponse(status=302, headers={'Location': final}),
    })
    with closing(http), pytest.raises(NewsError, match='robots_disallowed'):
        http.get(URL, headers={'If-None-Match': TAG}, conditional_url=final)
    assert [url for url, _ in session.calls] == ['https://publisher.test/robots.txt', URL]


@pytest.mark.parametrize('legacy,explicit', [(False, False), (True, False), (True, True)])
def test_verified_304_reuses_sealed_body_without_new_capture_or_batch(config, tmp_path, monkeypatch, legacy, explicit):
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: [FakeResponse(article_html(), headers={'Content-Type': 'text/html', 'ETag': TAG}),
              FakeResponse(status=304, headers={'ETag': TAG})],
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        record = saved_head(queue, http)
        if legacy:
            with queue.db:
                queue.db.execute('UPDATE articles SET validator_url=NULL')
        paths = list((tmp_path / 'bodies').rglob('*.parquet'))
        old = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        # Binding/revalidation can read metadata and hash sealed compressed
        # bytes; they must not decode the article/raw response payloads.
        from downloader import news_fulltext_queue as module
        parquet_file = module.pq.ParquetFile
        reads = []
        def metadata_file(path):
            batch = parquet_file(path)
            def read(*, columns):
                reads.append(columns)
                assert not {'body', 'raw_response_body'}.intersection(columns)
                return batch.read(columns=columns)
            return SimpleNamespace(schema_arrow=batch.schema_arrow, metadata=batch.metadata, read=read)
        monkeypatch.setattr(module.pq, 'ParquetFile', metadata_file)
        if explicit:
            row = queue.claim_url(URL, 300, refresh=True)
        else:
            with queue.db:
                queue.db.execute("UPDATE articles SET state='retry'")
            row = queue.claim('publisher', 300)
        assert row['validator_url'] == URL
        assert fetch_article(http, row) is None
        queue.unchanged(row)
        head = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        assert head['state'] == 'complete' and head['last_checked_at_utc'] >= old['last_checked_at_utc']
        assert head['lease_token'] is None
        assert all(head[key] == old[key] for key in ('parquet_path', 'body_sha256', 'body_bytes', 'captured_at_utc'))
        assert list((tmp_path / 'bodies').rglob('*.parquet')) == paths and reads
        assert session.calls[-1][1]['headers']['If-None-Match'] == TAG


@pytest.mark.parametrize('legacy', [False, True])
def test_api_envelope_validator_is_not_an_article_validator(config, tmp_path, legacy):
    api = 'https://publisher.test/api/posts'
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html(), headers={'Content-Type': 'text/html', 'ETag': TAG}),
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        saved_head(queue, http, source_url=api)
        if legacy:
            with queue.db:
                queue.db.execute('UPDATE articles SET validator_url=NULL')
        row = queue.claim_url(URL, 300, refresh=True)
        assert row['validator_url'] == api
        assert fetch_article(http, row)['body']
        assert 'If-None-Match' not in session.calls[-1][1]['headers']
        assert api not in [url for url, _ in session.calls]


@pytest.mark.parametrize('broken', ['missing', 'tampered', 'wrong_head', 'wrong_body_hash'])
def test_304_never_credits_unavailable_or_unrelated_sealed_body(config, tmp_path, broken):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html(), headers={'Content-Type': 'text/html', 'ETag': TAG}),
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        record = saved_head(queue, http)
        row = queue.claim_url(URL, 300, refresh=True)
        if broken == 'missing':
            (tmp_path / row['parquet_path']).unlink()
        elif broken == 'tampered':
            with queue.db:
                queue.db.execute("UPDATE accepted_batches SET receipt_path='missing.json'")
        elif broken == 'wrong_head': row['id'] = b'2' * 16
        else:
            with queue.db:
                queue.db.execute("UPDATE articles SET body_sha256=? WHERE id=?", ('a' * 64, row['id']))
            row['body_sha256'] = 'a' * 64
        queue.unchanged(row)
        head = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        assert head['captured_at_utc'] == record['captured_at_utc']
        if broken != 'wrong_head':
            assert head['state'] == 'retry' and head['validator_url'] is None and head['etag'] is None
            # last_checked is the actual failed attempt clock, not a capture
            # or freshness claim. Preserve that existing diagnostic contract.
            assert head['error_code'] == 'saved_body_unavailable'
        assert not queue.db.execute("SELECT value FROM counters WHERE key='not_modified'").fetchone()


def test_legacy_binding_failure_keeps_existing_body_and_never_guesses_article_url(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html(), headers={'Content-Type': 'text/html', 'ETag': TAG}),
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        record = saved_head(queue, http)
        with queue.db:
            queue.db.execute("UPDATE articles SET validator_url=NULL,etag='not-in-sealed-evidence'")
        row = queue.claim_url(URL, 300, refresh=True)
        assert row['validator_url'] is None and row['body_sha256'] == record['body_sha256']
        fetch_article(http, row)
        assert 'If-None-Match' not in session.calls[-1][1]['headers']


@pytest.mark.parametrize('damage', [
    'receipt_path', 'receipt_contract', 'receipt_rows', 'record_url',
    'record_contract', 'head_bytes', 'head_url',
])
def test_304_requires_committed_receipt_and_exact_saved_head_identity(config, tmp_path, damage):
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html(), headers={'Content-Type': 'text/html', 'ETag': TAG}),
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        queue.add_articles([{'url': URL}])
        record = fetch_article(http, queue.claim_url(URL, 300))
        if damage == 'record_url':
            record['requested_url'] = URL + '-different'
            # A legacy malformed batch had no claimed-head witness. New
            # claimed records reject this before head adoption.
            record.pop('article_claim_witness_json', None)
        elif damage == 'record_contract':
            record['contract_version'] = -1
        queue.accept([record])
        if damage.startswith('receipt_'):
            receipt_path = next((tmp_path / 'receipts').glob('*.json'))
            receipt = json.loads(receipt_path.read_text())
            if damage == 'receipt_path':
                receipt['parquet_path'] = 'bodies/different.parquet'
            elif damage == 'receipt_contract':
                receipt['contract_version'] = -1
            else:
                receipt['rows'] += 1
            receipt_path.write_text(json.dumps(receipt))
        if damage == 'head_bytes':
            with queue.db:
                queue.db.execute('UPDATE articles SET body_bytes=body_bytes+1')
        row = queue.claim_url(URL, 300, refresh=True)
        if damage == 'head_url':
            with queue.db:
                queue.db.execute('UPDATE articles SET url=? WHERE id=?', (URL + '-different', row['id']))
            row['url'] = URL + '-different'
        before = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        calls_before = len(session.calls)
        queue.unchanged(row)
        head = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        assert head['state'] == 'retry' and head['error_code'] == 'saved_body_unavailable'
        assert head['validator_url'] is None and head['etag'] is None and head['last_modified'] is None
        assert all(head[key] == before[key] for key in
                   ('parquet_path', 'body_sha256', 'body_bytes', 'captured_at_utc'))
        assert not queue.db.execute("SELECT value FROM counters WHERE key='not_modified'").fetchone()
        assert len(session.calls) == calls_before


def test_legacy_validator_binding_rejects_wrong_committed_receipt_path(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: [FakeResponse(article_html(), headers={'Content-Type': 'text/html', 'ETag': TAG}),
              FakeResponse(article_html())],
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        saved_head(queue, http)
        receipt_path = next((tmp_path / 'receipts').glob('*.json'))
        receipt = json.loads(receipt_path.read_text())
        receipt['parquet_path'] = 'bodies/different.parquet'
        receipt_path.write_text(json.dumps(receipt))
        with queue.db:
            queue.db.execute('UPDATE articles SET validator_url=NULL')
        row = queue.claim_url(URL, 300, refresh=True)
        assert row['validator_url'] is None
        fetch_article(http, row)
        assert 'If-None-Match' not in session.calls[-1][1]['headers']


@pytest.mark.parametrize('field,value', [
    ('etag', '"new-tag"'), ('last_modified', 'a-new-clock'),
    ('authorization_fingerprint', 'new-authorization'),
    ('body_sha256', 'a' * 64), ('parquet_path', 'bodies/new.parquet'),
    ('body_bytes', 9999), ('url', URL + '-different'),
    ('validator_url', 'https://publisher.test/new-representation'),
])
def test_legacy_validator_binding_requires_head_unchanged_during_verification(
        config, tmp_path, monkeypatch, field, value):
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: [FakeResponse(article_html(), headers={'Content-Type': 'text/html', 'ETag': TAG}),
              FakeResponse(article_html())],
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        saved_head(queue, http)
        with queue.db:
            queue.db.execute('UPDATE articles SET validator_url=NULL')
        original = queue._saved_body_record
        def concurrent_change(row, *, validators=False):
            record = original(row, validators=validators)
            with queue.db:
                queue.db.execute(f'UPDATE articles SET {field}=? WHERE id=?', (value, row['id']))
            return record
        monkeypatch.setattr(queue, '_saved_body_record', concurrent_change)
        row = queue.claim_url(URL, 300, refresh=True)
        assert row['validator_url'] is None
        assert queue.db.execute(f'SELECT {field} FROM articles').fetchone()[0] == value
        fetch_article(http, row)
        assert 'If-None-Match' not in session.calls[-1][1]['headers']


@pytest.mark.parametrize('old_receipt_invalid', [False, True])
def test_late_304_never_replaces_a_newer_body_or_failure(config, tmp_path, old_receipt_invalid):
    newer_body = '這是來源更新後的新正文，不應由舊的 304 覆寫。' * 20
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: [FakeResponse(article_html(), headers={'Content-Type': 'text/html', 'ETag': TAG}),
              FakeResponse(status=304, headers={'ETag': TAG}),
              FakeResponse(article_html(body=newer_body), headers={'Content-Type': 'text/html', 'ETag': '"new"'})],
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        old = saved_head(queue, http)
        old_receipt = next((tmp_path / 'receipts').glob('*.json'))
        stale_row = queue.claim_url(URL, 300, refresh=True)
        assert fetch_article(http, stale_row) is None
        newer = fetch_article(http, stale_row)
        assert newer['body_sha256'] != old['body_sha256']
        queue.accept([newer])
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',error_code='newer_source_failure',due=12345")
        if old_receipt_invalid:
            receipt = json.loads(old_receipt.read_text())
            receipt['sha256'] = '0' * 64
            old_receipt.write_text(json.dumps(receipt))
        before = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        queue.unchanged(stale_row)
        after = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        assert after == before
        assert not queue.db.execute("SELECT value FROM counters WHERE key='not_modified'").fetchone()


@pytest.mark.parametrize('bulk', [False, True])
def test_late_revalidation_never_releases_a_newer_article_lease(config, tmp_path, bulk):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html(), headers={'Content-Type': 'text/html', 'ETag': TAG}),
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        saved_head(queue, http)
        if bulk:
            with queue.db:
                queue.db.execute("UPDATE articles SET state='retry',error_code='prior',due=0")
            stale_row = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        else:
            stale_row = queue.claim_url(URL, 300, refresh=True)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='inflight',lease_owner='new-owner',lease_until=9876543210")
        before = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        queue.unchanged(stale_row)
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == before
        assert not queue.db.execute("SELECT value FROM counters WHERE key='not_modified'").fetchone()


def test_late_304_cannot_match_reissued_same_owner_deadline_and_attempt(config, tmp_path, monkeypatch):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html(), headers={'Content-Type': 'text/html', 'ETag': TAG}),
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        saved_head(queue, http)
        from downloader import news_fulltext_queue as module
        monkeypatch.setattr(module.time, 'time', lambda: 2000000000.0)
        stale_row = queue.claim_url(URL, 300, refresh=True)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',due=0,attempts=?,lease_owner=NULL,lease_until=NULL",
                             (stale_row['attempts'],))
        second = queue.claim_url(URL, 300, refresh=True)
        assert second is not None
        assert stale_row['_article_claim_token'] != second['_article_claim_token']
        before = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        assert before['lease_owner'] == queue.owner and before['lease_until'] == 2000000300.0
        assert before['attempts'] == stale_row['attempts'] + 1
        queue.unchanged(stale_row)
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == before


def test_pre_token_database_migrates_and_requeues_abandoned_lease(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html(), headers={'Content-Type': 'text/html', 'ETag': TAG}),
    })
    with closing(http):
        queue = NewsQueue(tmp_path, http.registry)
        record = saved_head(queue, http)
        claimed = queue.claim_url(URL, 300, refresh=True)
        before = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        assert before['lease_token'] == claimed['_article_claim_token']
        queue.close()
        # Simulate an existing database created before the additive column.
        with sqlite3.connect(tmp_path / 'queue.sqlite3') as legacy:
            legacy.execute('ALTER TABLE articles DROP COLUMN lease_token')
            legacy.execute('ALTER TABLE articles DROP COLUMN recovery_lease_token')
        with closing(NewsQueue(tmp_path, http.registry)) as restored:
            head = dict(restored.db.execute('SELECT * FROM articles').fetchone())
            assert head['state'] == 'retry' and head['error_code'] == 'owner_restart'
            assert head['lease_owner'] is None and head['lease_until'] is None and head['lease_token'] is None
            assert head['recovery_lease_token'] is None
            assert head['body_sha256'] == record['body_sha256']
            assert head['parquet_path'] == before['parquet_path']
            assert head['captured_at_utc'] == before['captured_at_utc']
            assert restored.db.execute("SELECT value FROM queue_settings WHERE key='article_revalidation_lease_contract_version'").fetchone()[0] == '2'
            assert restored.db.execute("SELECT value FROM queue_settings WHERE key='article_capture_commit_contract_version'").fetchone()[0] == '7'


@pytest.mark.parametrize('code,retry,deferred', [
    ('source_not_found', False, False), ('http_429', True, False),
    ('provider_cooldown', True, True),
])
def test_late_article_failure_never_clears_newer_lease(config, tmp_path, code, retry, deferred):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html(), headers={'Content-Type': 'text/html', 'ETag': TAG}),
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        saved_head(queue, http)
        stale_row = queue.claim_url(URL, 300, refresh=True)
        with queue.db:
            queue.db.execute("UPDATE articles SET lease_owner='new-owner',lease_token='new-token',lease_until=9876543210")
        before = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        queue.failed(stale_row, NewsError(code, retry=retry, delay=120, request_deferred=deferred), 6)
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == before
        assert not queue.db.execute("SELECT value FROM counters WHERE key='article_failures'").fetchone()


def test_late_article_success_never_replaces_a_newer_committed_body(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: [FakeResponse(article_html(), headers={'Content-Type': 'text/html', 'ETag': TAG}),
              FakeResponse(article_html(body='舊工作者取得的正文。' * 30), headers={'Content-Type': 'text/html'}),
              FakeResponse(article_html(body='新工作者取得的正文。' * 30), headers={'Content-Type': 'text/html'})],
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        saved_head(queue, http)
        old_row = queue.claim_url(URL, 300, refresh=True)
        old_record = fetch_article(http, old_row)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',due=0,lease_owner=NULL,lease_until=NULL,lease_token=NULL")
        new_row = queue.claim_url(URL, 300, refresh=True)
        new_record = fetch_article(http, new_row)
        queue.accept([new_record])
        before = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        assert before['body_sha256'] == new_record['body_sha256'] != old_record['body_sha256']
        queue.accept([old_record])
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == before


@pytest.mark.parametrize('legacy', [False, True])
@pytest.mark.parametrize('newer_capture', [False, True])
def test_orphan_article_receipt_recovers_only_without_a_newer_head(
        config, tmp_path, monkeypatch, newer_capture, legacy):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: [FakeResponse(article_html(), headers={'Content-Type': 'text/html', 'ETag': TAG}),
              FakeResponse(article_html(body='先送出並留下收據的正文。' * 30), headers={'Content-Type': 'text/html'}),
              FakeResponse(article_html(body='後來成功寫入的新正文。' * 30), headers={'Content-Type': 'text/html'})],
    })
    with closing(http):
        queue = NewsQueue(tmp_path, http.registry)
        saved_head(queue, http)
        old_row = queue.claim_url(URL, 300, refresh=True)
        old_record = fetch_article(http, old_row)
        if legacy:
            old_record.pop('article_claim_witness_json')
        original_accept = queue._accept_records
        def fail_before_commit(*_args, **_kwargs):
            raise OSError('simulated SQLite commit interruption')
        monkeypatch.setattr(queue, '_accept_records', fail_before_commit)
        with pytest.raises(OSError, match='simulated SQLite'):
            queue.accept([old_record])
        monkeypatch.setattr(queue, '_accept_records', original_accept)
        if newer_capture:
            with queue.db:
                queue.db.execute("UPDATE articles SET state='retry',due=0,lease_owner=NULL,lease_until=NULL,lease_token=NULL")
            new_row = queue.claim_url(URL, 300, refresh=True)
            new_record = fetch_article(http, new_row)
            queue.accept([new_record])
        elif legacy:
            # A legacy receipt came from a pre-token owner, whose abandoned
            # inflight row could not contain a modern claim token either.
            with queue.db:
                queue.db.execute('UPDATE articles SET lease_token=NULL')
        before = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        queue.close()
        with closing(NewsQueue(tmp_path, http.registry)) as restored:
            after = dict(restored.db.execute('SELECT * FROM articles').fetchone())
            if newer_capture:
                assert after == before and after['body_sha256'] == new_record['body_sha256']
            else:
                assert after['state'] == 'complete' and after['body_sha256'] == old_record['body_sha256']


def test_superseded_missing_body_capture_is_not_counted_as_a_new_head(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html()),
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        queue.add_articles([{'url': URL}])
        old_row = queue.claim_url(URL, 300)
        old_record = fetch_article(http, old_row)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',due=0,lease_owner=NULL,lease_until=NULL,lease_token=NULL")
        new_row = queue.claim_url(URL, 300)
        assert new_row['_article_claim_token'] != old_row['_article_claim_token']
        before = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        queue.accept([old_record])
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == before
        counts = queue.metrics.snapshot()['providers']['publisher']['counts']
        assert counts.get('durably_accepted_new_url_heads', 0) == 0
        assert queue.db.execute("SELECT value FROM counters WHERE key='article_capture_superseded_at_commit'").fetchone()[0] == 1


def test_legacy_orphan_cannot_erase_future_source_cooldown(config, tmp_path, monkeypatch):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: [FakeResponse(article_html(), headers={'Content-Type': 'text/html', 'ETag': TAG}),
              FakeResponse(article_html(body='等待恢復的原文。' * 40), headers={'Content-Type': 'text/html'})],
    })
    with closing(http):
        queue = NewsQueue(tmp_path, http.registry)
        saved_head(queue, http)
        row = queue.claim_url(URL, 300, refresh=True)
        record = fetch_article(http, row)
        record.pop('article_claim_witness_json')
        original_accept = queue._accept_records
        monkeypatch.setattr(queue, '_accept_records', lambda *_a, **_k: (_ for _ in ()).throw(OSError('crash')))
        with pytest.raises(OSError, match='crash'):
            queue.accept([record])
        monkeypatch.setattr(queue, '_accept_records', original_accept)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry_cooldown',due=4102444800,error_code='http_429',"
                             "lease_owner=NULL,lease_until=NULL,lease_token=NULL")
        before = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        queue.close()
        with closing(NewsQueue(tmp_path, http.registry)) as restored:
            assert dict(restored.db.execute('SELECT * FROM articles').fetchone()) == before


def test_restart_replays_the_abandoned_lease_not_an_older_identical_lease(config, tmp_path, monkeypatch):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: [FakeResponse(article_html(), headers={'Content-Type': 'text/html', 'ETag': TAG}),
              FakeResponse(article_html(body='前一個租約的正文。' * 40), headers={'Content-Type': 'text/html'}),
              FakeResponse(article_html(body='實際中斷租約的正文。' * 40), headers={'Content-Type': 'text/html'})],
    })
    with closing(http):
        queue = NewsQueue(tmp_path, http.registry)
        saved_head(queue, http)
        from downloader import news_fulltext_queue as module
        monkeypatch.setattr(module.time, 'time', lambda: 2000000000.0)
        original_accept = queue._accept_records
        monkeypatch.setattr(queue, '_accept_records', lambda *_a, **_k: (_ for _ in ()).throw(OSError('crash')))
        old_row = queue.claim_url(URL, 300, refresh=True)
        old_record = fetch_article(http, old_row)
        with pytest.raises(OSError, match='crash'):
            queue.accept([old_record])
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',due=0,attempts=?,lease_owner=NULL,lease_until=NULL,lease_token=NULL",
                             (old_row['attempts'],))
        new_row = queue.claim_url(URL, 300, refresh=True)
        new_record = fetch_article(http, new_row)
        assert old_row['_article_claim_token'] != new_row['_article_claim_token']
        with pytest.raises(OSError, match='crash'):
            queue.accept([new_record])
        monkeypatch.setattr(queue, '_accept_records', original_accept)
        queue.close()
        with closing(NewsQueue(tmp_path, http.registry)) as restored:
            head = dict(restored.db.execute('SELECT * FROM articles').fetchone())
            assert head['state'] == 'complete' and head['body_sha256'] == new_record['body_sha256']
            assert head['body_sha256'] != old_record['body_sha256']

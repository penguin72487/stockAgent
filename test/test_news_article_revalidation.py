"""Synthetic HTTP/cache contracts; never downloads publisher articles."""
from contextlib import closing
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import ETTODAY_LAYOUT_VERSIONS, NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
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
            return SimpleNamespace(schema_arrow=batch.schema_arrow, read=read)
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
        if broken == 'missing': row['parquet_path'] = 'missing.parquet'
        elif broken == 'tampered':
            with queue.db:
                queue.db.execute("UPDATE accepted_batches SET receipt_path='missing.json'")
        elif broken == 'wrong_head': row['id'] = b'2' * 16
        else: row['body_sha256'] = 'a' * 64
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

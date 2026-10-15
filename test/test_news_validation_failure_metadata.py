"""Native validation failures retain bounded facts, not more publisher calls."""
from contextlib import closing
from dataclasses import replace
import json
from types import SimpleNamespace

import pytest

from downloader import news_fulltext_diagnostics as diagnostics
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_failure_diagnostics import fail, reports
from test_news_failure_identity_metadata import evidence
from test_news_ftnn import URL, page, policy_config, update_schema
from test_news_fulltext import config as cfg


@pytest.mark.parametrize('code', sorted(diagnostics.VALIDATION_METADATA_CODES))
def test_static_validation_budgets_are_metadata_only_and_never_refill_identity_raw_or_retry(cfg, tmp_path, code):
    policy = PolicyRegistry(cfg, base=tmp_path)
    root = tmp_path / 'store'
    raw = Response('https://publisher.test/article/42?token=private-query', 200,
        {'content-type': 'text/html', 'set-cookie': 'private-cookie'},
        b'<h1>private-headline</h1><article>private-body</article>')
    with closing(NewsQueue(root, policy)) as queue:
        for _ in range(3):
            fail(queue, raw, code='article_identity_not_verified', received_at_utc='2026-10-14T15:00:00+00:00')
        for _ in range(4):
            fail(queue, raw, code=code, received_at_utc='2026-10-14T15:00:00+00:00')
        paths, rows = evidence(root)
        assert len(paths) == 4
        validated = [row for row in rows if row['error_code'] == code]
        assert len(validated) == 2 and all(row['budget_key'].startswith('failure_validation_metadata_attempts:v1:') for row in validated)
        assert not any('private-' in p.read_text() for p in paths)
        assert all(not row['raw_response_retained'] and not row['accepted_as_article'] for row in rows)
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert not reports(root)[0]
        assert queue.db.execute('SELECT state,error_code,body_sha256 FROM articles').fetchone()[:] == (code, code, None)
    provider = policy.match(raw.url).provider
    policy.sources[provider] = replace(policy.sources[provider], data_fingerprint='upgraded-parser')
    with closing(NewsQueue(root, policy)) as queue:
        fail(queue, raw, code=code, received_at_utc='2026-10-14T15:00:00+00:00')
        assert len(evidence(root)[0]) == 4


@pytest.mark.parametrize('code', ['http_403', 'http_429', 'access_challenge', 'paywall_or_login', 'source_not_found'])
def test_refusals_do_not_gain_a_metadata_budget_or_recovery_request(cfg, tmp_path, code):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        fail(queue, Response('https://publisher.test/article/42', 200, {'content-type': 'text/html'}, b'<h1>x</h1>'),
             code=code, received_at_utc='2026-10-14T15:00:00+00:00')
        assert not evidence(tmp_path)[0]
        assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'failure_validation_metadata_attempts:%'").fetchone()


def test_failed_validation_storage_consumes_reservation_and_does_not_refetch(cfg, tmp_path, monkeypatch):
    calls = []
    def broken(*args, **kwargs):
        calls.append(args)
        raise OSError('owned storage failure')
    monkeypatch.setattr(diagnostics, 'atomic_write_json', broken)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        for _ in range(4):
            fail(queue, Response('https://publisher.test/article/42', 200, {'content-type': 'text/html'}, b'<h1>x</h1>'),
                code='invalid_bulk_publication_clock', received_at_utc='2026-10-14T15:00:00+00:00')
        assert len(calls) == 2 and not evidence(tmp_path)[0]
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'failure_validation_metadata_attempts:%'").fetchone()[0] == '2'


@pytest.mark.parametrize('code', sorted(diagnostics.VALIDATION_METADATA_CODES))
def test_worker_passes_already_received_validation_response_to_owner_without_io(tmp_path, monkeypatch, code):
    import downloader.news_fulltext as fulltext
    registry = PolicyRegistry(policy_config(), base=tmp_path)
    raw = Response(URL, 200, {'content-type': 'text/html'}, str(page()).encode())
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        return raw
    def rejected(*args):
        raise NewsError(code)
    monkeypatch.setattr(fulltext, 'extract_article_response', rejected)
    monkeypatch.setattr(diagnostics, 'atomic_write_json', lambda *a, **k: pytest.fail('worker wrote'))
    with pytest.raises(NewsError, match='^' + code + '$') as caught:
        fetch_article(SimpleNamespace(registry=registry, get=get), {'url': URL, 'id': b'z' * 32})
    assert calls == [URL] and caught.value.failure_response.response is raw
    assert not (tmp_path / 'identity_metadata').exists()


def test_ftnn_native_clock_and_identity_facts_preserve_source_defect_without_text_or_acceptance(tmp_path):
    soup = page()
    update_schema(soup, 'dateModified', '2026-10-14T21:00:00+08:00')
    raw = Response(URL, 200, {'content-type': 'text/html'}, str(soup).encode())
    with closing(NewsQueue(tmp_path, PolicyRegistry(policy_config(), base=tmp_path))) as queue:
        fail(queue, raw, code='invalid_bulk_publication_clock', received_at_utc='2026-10-14T15:00:00+00:00')
        paths, rows = evidence(tmp_path)
        assert len(paths) == 1
        facts = rows[0]['facts']['ftnn_native_validation']
        assert facts['declared_publication'] == '2026-10-14T22:00:15+08:00'
        assert facts['declared_modification'] == '2026-10-14T21:00:00+08:00'
        assert facts['visible_release'] == '發布時間：2026/10/14 22:00'
        assert facts['declared_author_in_visible_byline'] and facts['native_reporter_byline_shape']
        assert facts['heading_compact_sha256'] == [facts['schema_headline_compact_sha256']]
        assert facts['schema_headline_compact_sha256'] == facts['breadcrumb_headline_compact_sha256']
        assert facts['og_title_compact_sha256'] == [facts['schema_expected_og_title_compact_sha256']]
        assert not facts['body_text_retained'] and not rows[0]['raw_response_retained']
        assert '黃詩雯' not in paths[0].read_text() and '原始報導' not in paths[0].read_text()
        assert not rows[0]['accepted_as_article'] and not rows[0]['publish'] and not rows[0]['remote_training_export']


@pytest.mark.parametrize('bad_schema', ['broken', 'oversized', 'too_many', 'nested_type'])
def test_invalid_native_metadata_is_bounded_and_cannot_accept_or_emit_private_text(tmp_path, bad_schema):
    soup = page()
    scripts = soup.select('script[type="application/ld+json"]')
    if bad_schema == 'broken':
        scripts[0].string = 'private-broken-json'
    elif bad_schema == 'oversized':
        scripts[0].string = 'private-' * 10000
    elif bad_schema == 'too_many':
        for _ in range(17):
            extra = soup.new_tag('script', type='application/ld+json')
            extra.string = '{}'
            soup.head.append(extra)
    else:
        data = json.loads(scripts[0].string)
        data['author'] = ['private-author']
        scripts[0].string = json.dumps(data)
    with closing(NewsQueue(tmp_path, PolicyRegistry(policy_config(), base=tmp_path))) as queue:
        fail(queue, Response(URL, 200, {'content-type': 'text/html'}, str(soup).encode()),
             code='article_attribution_not_verified', received_at_utc='2026-10-14T15:00:00+00:00')
        paths, rows = evidence(tmp_path)
        assert len(paths) == 1 and paths[0].stat().st_size < 65536
        assert 'private-' not in paths[0].read_text() and not rows[0]['accepted_as_article']

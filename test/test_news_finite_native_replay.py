"""Literal finite admission reuses original bytes, not site-wide permission."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import time

import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_retained_native import materialize_reviewed_native
from test_news_gvm_native import URL, entry, page
from test_news_retained_native_replay import approved, head


URLS = (URL, URL.replace('12345', '12346'))
OUTSIDE = URL.replace('12345', '98765')


def settings():
    provider = entry()
    provider['finite_article_urls'] = list(URLS)
    provider['authorization']['original_author_scope_evidence_urls'] = list(URLS)
    return {'contract_version': 1, 'publish': False, 'remote_training_export': False,
        'collection_purpose': 'personal_research', 'providers': [provider]}


@pytest.fixture
def finite_approval(tmp_path):
    config = settings()
    original_auth = deepcopy(config['providers'][0]['authorization'])
    original_auth.pop('original_author_scope_evidence_urls')
    original_auth['scope'] = 'Two original diagnostics; no body acceptance.'
    queue, registry, path, manifest = approved(tmp_path, config, urls=URLS, provider='gvm',
        original_auth=original_auth,
        raw_factory=lambda url: str(page()).replace('12345', url.rsplit('/', 1)[1]).encode())
    queue.add_articles([{'url': OUTSIDE}])
    queue.sync_finite_cohorts()
    directory = path.parent/'private-source-diagnostics'
    directory.chmod(0o700)
    for member in directory.iterdir():
        member.chmod(0o600)
    manifest.update(contract_version=3, scope_kind='reviewed_finite_native_materialization_v3',
        original_author_scope_evidence_addition_reviewed=True,
        diagnostic_directory=directory.name)
    atomic_write_json(path, manifest, durable=True)
    yield queue, registry, path, manifest
    queue.close()


def replay(fixture):
    queue, _, path, _ = fixture
    return materialize_reviewed_native(queue, URLS, path, lease_seconds=300)


def test_exact_finite_originals_accept_once_zero_http_and_preserve_original_clock(finite_approval, monkeypatch):
    queue, registry, path, manifest = finite_approval
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('retained originals must not create an HTTP owner'))
    outside = head(queue, OUTSIDE)
    cohort = dict(queue.db.execute("SELECT * FROM finite_cohorts WHERE provider='gvm'").fetchone())
    result = replay(finite_approval)
    assert result['materialized_bodies'] == 2 and result['publisher_requests_this_command'] == 0
    assert not result['all_publisher_history_complete'] and not result['remote_training_export']
    for item in manifest['entries']:
        current = head(queue, item['url'])
        saved, = [row for row in pq.ParquetFile(queue.root/current['parquet_path']).read().to_pylist()
            if row['requested_url'] == item['url']]
        assert current['state'] == 'complete' and current['attempts'] == 1
        assert saved['reviewed_native_replay_contract_version'] == 3
        assert saved['captured_at_utc'] == saved['source_observed_at_utc'] == item['captured_at_utc']
        assert saved['http_duration_seconds'] is None and not saved['historical_point_in_time']
        assert json.loads(saved['original_diagnostic_authorization_json']) == item['original_authorization']
        assert json.loads(saved['authorization_json']) == registry.sources['gvm'].authorization
        assert sha256_file(queue.root/saved['replay_source_parquet']) == item['diagnostic_sha256']
    before = [head(queue, url) for url in URLS]
    assert replay(finite_approval)['materialized_bodies'] == 0
    assert before == [head(queue, url) for url in URLS]
    assert outside == head(queue, OUTSIDE) and outside['state'] == 'waiting_cohort'
    assert dict(queue.db.execute('SELECT * FROM finite_cohorts').fetchone()) == cohort
    assert queue.claim_url(OUTSIDE, 300) is None


@pytest.mark.parametrize('change', [
    {'state': 'http_403', 'error_code': 'http_403'}, {'state': 'http_429', 'error_code': 'http_429'},
    {'due': time.time()+3600}, {'lease_owner': 'other', 'lease_until': time.time()+3600},
    {'last_checked_at_utc': '2026-10-22T00:00:00+00:00'},
])
def test_refusal_deadline_lease_or_newer_observation_is_not_reset(finite_approval, change):
    queue, *_ = finite_approval
    # Use a clock definitely newer than this fixture's twenty-second-old original.
    if 'last_checked_at_utc' in change:
        from downloader.news_fulltext_queue import utc_now
        change = {'last_checked_at_utc': utc_now()}
    with queue.db:
        queue.db.execute('UPDATE articles SET '+','.join(k+'=?' for k in change)+' WHERE url=?',
            (*change.values(), URL))
    before = head(queue, URL)
    result = replay(finite_approval)
    assert result['materialized_bodies'] == 1 and result['skipped_urls'] == [URL]
    assert head(queue, URL) == before


@pytest.mark.parametrize('failure', ['v1', 'missing_review', 'directory_escape', 'less_selection',
    'cohort_changed', 'source_scope_changed', 'authorization_changed', 'public_original'])
def test_finite_preflight_does_not_write_on_unreviewed_scope(finite_approval, failure):
    queue, registry, path, manifest = finite_approval
    selected = URLS
    if failure == 'v1':
        manifest.update(contract_version=1, scope_kind='reviewed_native_diagnostic_materialization_v1')
    elif failure == 'missing_review':
        manifest['original_author_scope_evidence_addition_reviewed'] = False
    elif failure == 'directory_escape':
        manifest['diagnostic_directory'] = '..'
    elif failure == 'less_selection':
        selected = (URL,)
        manifest['entries'] = manifest['entries'][:1]
    elif failure == 'cohort_changed':
        with queue.db:
            queue.db.execute('DELETE FROM finite_cohort_members WHERE url=?', (URL,))
    elif failure == 'source_scope_changed':
        registry.sources['gvm'] = replace(registry.sources['gvm'], finite_article_urls=(URL,))
    elif failure == 'authorization_changed':
        auth = dict(registry.sources['gvm'].authorization, kind='explicit_permission')
        registry.sources['gvm'] = replace(registry.sources['gvm'], authorization=auth)
    else:
        (path.parent/manifest['entries'][0]['diagnostic_path']).chmod(0o644)
    atomic_write_json(path, manifest, durable=True)
    before = [head(queue, url) for url in URLS]
    with pytest.raises(ValueError):
        materialize_reviewed_native(queue, selected, path, lease_seconds=300)
    assert before == [head(queue, url) for url in URLS]
    assert not (queue.root/'reviewed_native_sources').exists()


def test_activation_freezes_only_complete_literal_selection_and_preserves_other_metadata(tmp_path):
    config = settings()
    config['providers'][0]['enabled'] = False
    registry = PolicyRegistry(config, base=tmp_path)
    old = NewsQueue(tmp_path/'news', registry)
    with old.db:
        old.add_articles([{'url': url} for url in (URL, OUTSIDE)])
    outside = head(old, OUTSIDE)
    old.close()
    config['providers'][0]['enabled'] = True
    current = NewsQueue(tmp_path/'news', PolicyRegistry(config, base=tmp_path))
    try:
        assert not current.db.execute('SELECT 1 FROM finite_cohorts').fetchone()
        assert current.claim('gvm', 300) is None
        assert head(current, OUTSIDE) == outside
        with current.db:
            current.add_articles([{'url': URLS[1]}])
        current.sync_finite_cohorts()
        members = tuple(r[0] for r in current.db.execute('SELECT url FROM finite_cohort_members ORDER BY url'))
        assert members == URLS and head(current, OUTSIDE) == outside
    finally:
        current.close()
    altered = deepcopy(config)
    altered['providers'][0]['finite_article_urls'].append(OUTSIDE)
    altered['providers'][0]['finite_article_urls'].sort()
    with pytest.raises(ValueError, match='differs from immutable cohort'):
        NewsQueue(tmp_path/'news', PolicyRegistry(altered, base=tmp_path))
    resumed = NewsQueue(tmp_path/'news', PolicyRegistry(config, base=tmp_path))
    resumed.close()


def test_durable_finite_batch_recovers_after_owner_loss_without_new_request(finite_approval, monkeypatch):
    queue, registry, _, manifest = finite_approval
    def crash(*args):
        raise RuntimeError('after durable batch before head commit')
    monkeypatch.setattr(queue, '_accept_records', crash)
    with pytest.raises(RuntimeError, match='after durable batch'):
        replay(finite_approval)
    root = queue.root
    queue.close()
    recovered = NewsQueue(root, registry)
    try:
        for item in manifest['entries']:
            assert head(recovered, item['url'])['state'] == 'complete'
            assert head(recovered, item['url'])['body_sha256'] == item['body_sha256']
        assert head(recovered, OUTSIDE)['state'] == 'waiting_cohort'
    finally:
        recovered.close()


@pytest.mark.parametrize('field,value', [
    ('finite_article_urls', [URL, URL]), ('finite_article_urls', list(reversed(URLS))),
    ('finite_article_urls', ['https://esg.gvm.com.tw/article/12345']),
    ('finite_article_urls', [URL+'?unreviewed=1']), ('finite_article_urls', ['http://www.gvm.com.tw/article/12345']),
    ('finite_article_urls', [URL.replace('/article/', '/search/')]),
    ('feeds', ['https://www.gvm.com.tw/feed']), ('allow_reviewed_supplements', True),
])
def test_explicit_finite_scope_rejects_expansion_at_registry_boundary(field, value):
    config = settings()
    config['providers'][0][field] = value
    with pytest.raises(ValueError):
        PolicyRegistry(config, base='.')


def test_direct_transport_cannot_expand_literal_scope_before_connecting(tmp_path):
    from downloader.news_fulltext import load_config
    from downloader.news_fulltext_http import NewsHTTP
    from downloader.news_fulltext_policy import NewsError
    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    config.pop('provider_catalog', None)
    config.update(settings(), exclusive_http_owner=False)
    http = NewsHTTP(PolicyRegistry(config, base=tmp_path), config, tmp_path)
    try:
        for url in (*URLS, 'https://www.gvm.com.tw/robots.txt'):
            assert http._require(url).provider == 'gvm'
        for url in (OUTSIDE, 'https://www.gvm.com.tw/', URL+'?unreviewed=1',
                    'https://www.gvm.com.tw/robots.txt?unreviewed=1'):
            with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
                http._require(url)
        assert http.requests_by_provider == {}
    finally:
        http.close()


@pytest.mark.parametrize('rep_status,rep_body,error,expected_requests', [
    (200, b'User-agent: *\nAllow: /\n', None, 2),
    (200, b'User-agent: *\nDisallow: /article/\n', 'robots_disallowed', 1),
    (403, b'', 'source_access_denied', 1),
    (429, b'', 'http_429', 1),
])
def test_finite_article_get_checks_actual_rep_without_requiring_home_scope(
        tmp_path, rep_status, rep_body, error, expected_requests):
    from downloader.news_fulltext import load_config
    from downloader.news_fulltext_policy import NewsError
    from test_news_fulltext import FakeResponse, http_fixture

    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    config.pop('provider_catalog', None)
    config.update(settings(), exclusive_http_owner=False, adaptive_rate={'enabled': False})
    robots = 'https://www.gvm.com.tw/robots.txt'
    http, session = http_fixture(config, tmp_path, {
        robots: FakeResponse(rep_body, rep_status), URL: FakeResponse(b'original bytes')})
    try:
        if error:
            with pytest.raises(NewsError, match=error):
                http.get(URL)
        else:
            assert http.get(URL).body == b'original bytes'
        assert [url for url, _ in session.calls] == [robots, URL][:expected_requests]
        for outside in (OUTSIDE, 'https://www.gvm.com.tw/', URL+'?unreviewed=1'):
            with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
                http.get(outside)
        assert len(session.calls) == expected_requests
    finally:
        http.close()


def test_finite_rep_redirect_cannot_expand_literal_scope(tmp_path):
    from downloader.news_fulltext import load_config
    from downloader.news_fulltext_policy import NewsError
    from test_news_fulltext import FakeResponse, http_fixture

    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    config.pop('provider_catalog', None)
    config.update(settings(), exclusive_http_owner=False, adaptive_rate={'enabled': False})
    robots = 'https://www.gvm.com.tw/robots.txt'
    http, session = http_fixture(config, tmp_path, {
        robots: FakeResponse(status=302, headers={'Location': 'https://www.gvm.com.tw/'})})
    try:
        with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
            http.get(URL)
        assert [url for url, _ in session.calls] == [robots]
    finally:
        http.close()

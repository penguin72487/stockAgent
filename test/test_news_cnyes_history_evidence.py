"""List completeness needs actual IDs/dates, not a queued child or one page."""
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import time
from urllib.parse import parse_qs

import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext import accept_discovery
from downloader.news_fulltext_bulk import cnyes_pagination, cnyes_records, cnyes_scope, cnyes_url, cnyes_window_partitions
from downloader.news_fulltext_cnyes_history import (
    CONTRACT_VERSION, backfill, index_children, record_page, progress, repair_incomplete_scopes,
    reconcile_legacy_request_mismatches, reconcile_legacy_unavailable, seed_overflow_retained,
)
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import BASE, cfg, payload, registry_http, response


@pytest.fixture
def queue(cfg, tmp_path):
    registry, _ = registry_http(cfg, tmp_path)
    value = NewsQueue(tmp_path, registry)
    yield value
    value.close()


def bounds(url):
    q = parse_qs(url.split('?', 1)[1])
    return int(q['startAt'][0]), int(q['endAt'][0])


def source_payload(ids, *, epoch, page=1, total=None):
    data = payload(ids=ids, page=page, total=total)
    for post in data['items']['data']:
        post['publishAt'] = epoch
    return data


def capture(queue, url, data):
    source = queue.registry.sources['cnyes']
    with queue.db:
        queue.db.execute("INSERT OR IGNORE INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", (url,))
    r = response(data, url)
    records, errors = cnyes_records(r, type('HTTP', (), {'registry': queue.registry})(), source)
    links = [(row['requested_url'], row['title'], row['metadata_date_raw']) for row in records]
    links.extend((row['url'], row.get('title', ''), row.get('metadata_date_raw', '')) for row in errors
                 if row['error'] not in {'bulk_post_not_public', 'bulk_exact_duplicate_row'})
    task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    accept_discovery(queue, task, r, links, records, errors)
    return r


def head(queue, url):
    return dict(queue.db.execute('SELECT * FROM cnyes_history_scope_heads WHERE scope_url=?', (cnyes_scope(url),)).fetchone())


def seed_partition(queue, total=4):
    parent = cnyes_url(BASE, start=1388534400, end=1388620799)
    children = cnyes_window_partitions(parent, 31)
    scope = cnyes_scope(parent)
    with queue.db:
        queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',?,31,'original-parent-source-clock')", (scope, total))
        queue.db.execute("INSERT INTO bulk_window_gaps VALUES (?,'cnyes','cnyes_single_day_page_cap','original-gap-clock')", (scope,))
        queue.db.execute("INSERT INTO bulk_window_splits VALUES (?,'cnyes',?,'original-split-clock')", (scope, json.dumps(children)))
        index_children(queue, scope, 'cnyes', children)
    return parent, children


def test_actual_all_pages_then_parent_ID_day_proof_and_preserved_original_gap(queue):
    parent, children = seed_partition(queue, total=33)
    left, right = children
    capture(queue, left, source_payload(range(1, 31), epoch=bounds(left)[0], total=31))
    assert head(queue, left)['state'] == 'waiting_page_evidence'
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(parent),)).fetchone()
    capture(queue, cnyes_url(BASE, 2, start=bounds(left)[0], end=bounds(left)[1]),
            source_payload([31], epoch=bounds(left)[0], page=2, total=31))
    assert head(queue, left)['state'] == 'list_complete'
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(parent),)).fetchone()
    capture(queue, right, source_payload([32, 33], epoch=bounds(right)[0]))
    assert head(queue, parent)['state'] == 'partition_list_complete'
    retirement = queue.db.execute('SELECT * FROM cnyes_history_gap_retirements WHERE scope_url=?', (cnyes_scope(parent),)).fetchone()
    assert json.loads(retirement['gap_json'])['observed_at_utc'] == 'original-gap-clock'
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(parent),)).fetchone() is None
    proof = json.loads(queue.db.execute('SELECT proof_json FROM cnyes_history_scope_proofs WHERE proof_sha256=?',
                                       (retirement['proof_sha256'],)).fetchone()[0])
    assert proof['unique_article_IDs'] == 33 and proof['body_completeness_verified'] is False
    assert proof['historical_point_in_time'] is False


@pytest.mark.parametrize('conflict', ['parent_total', 'overlapping_ID', 'source_date'])
def test_real_partition_conflict_keeps_source_gap(queue, conflict):
    parent, children = seed_partition(queue, total=5 if conflict == 'parent_total' else 4)
    capture(queue, children[0], source_payload([1, 2], epoch=bounds(children[0])[0]))
    ids = [2, 3] if conflict == 'overlapping_ID' else [3, 4]
    epoch = bounds(children[0])[0] if conflict == 'source_date' else bounds(children[1])[0]
    capture(queue, children[1], source_payload(ids, epoch=epoch))
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(parent),)).fetchone()
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_gap_retirements').fetchone()[0] == 0


def test_completed_parent_reopens_on_forward_changed_child_source(queue):
    parent, children = seed_partition(queue)
    capture(queue, children[0], source_payload([1, 2], epoch=bounds(children[0])[0]))
    capture(queue, children[1], source_payload([3, 4], epoch=bounds(children[1])[0]))
    original_proof = head(queue, parent)['proof_sha256']
    capture(queue, children[1], source_payload([3, 4, 5], epoch=bounds(children[1])[0]))
    assert head(queue, parent)['state'] == 'inconsistent_partition_evidence'
    assert queue.db.execute('SELECT 1 FROM cnyes_history_scope_proofs WHERE proof_sha256=?', (original_proof,)).fetchone()
    assert queue.db.execute('SELECT error_code FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(parent),)).fetchone()[0] == 'cnyes_source_list_evidence_changed'


def test_forward_source_totals_and_post_conflicts_cannot_certify_multi_page_list(queue):
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    capture(queue, url, source_payload(range(1, 31), epoch=low, total=31))
    second = cnyes_url(BASE, 2, start=low, end=low + 100)
    capture(queue, second, source_payload([31, 32], epoch=low, page=2, total=32))
    assert head(queue, url)['state'] == 'inconsistent_source_snapshot'
    assert progress(queue.db)['all_history_complete'] is False


def test_empty_source_window_has_its_own_exact_metadata_evidence(queue):
    url = cnyes_url(BASE, start=1388534400, end=1388534401)
    data = source_payload([], epoch=1388534400)
    data['items']['last_page'] = 0
    capture(queue, url, data)
    assert head(queue, url)['state'] == 'list_complete'
    assert json.loads(head(queue, url)['member_ids_json']) == []
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 1


def test_single_true_source_day_overflow_never_becomes_complete(queue):
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    capture(queue, url, source_payload(range(1, 31), epoch=low, total=1000))
    assert queue.db.execute('SELECT error_code FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(url),)).fetchone()[0] == 'cnyes_single_day_page_cap'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_scope_heads').fetchone()[0] == 0


def seed_equivalent(queue, *, change_page2=False):
    low = 1388534400
    exemplar = cnyes_url(BASE, start=low, end=low + 100)
    alias = cnyes_url(BASE, start=low + 101, end=low + 300)
    with queue.db:
        from downloader.news_fulltext_cnyes_history import index_scope
        index_scope(queue, cnyes_scope(alias), 'cnyes')
        queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',31,2,'old-alias-clock')", (cnyes_scope(alias),))
        for page, ids in [(1, range(1, 31)), (2, [32] if change_page2 else [31])]:
            page_url = cnyes_url(BASE, page, start=low + 101, end=low + 300)
            info, _ = cnyes_pagination(response(source_payload(ids, epoch=low, page=page, total=31),
                page_url))
            queue.db.execute('INSERT INTO bulk_page_evidence VALUES (?,?,?,?,?)',
                             (cnyes_scope(alias), page, 31, info['ids_sha256'], 'real-alias-page-clock'))
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due) VALUES (?,'cnyes_news','cnyes',9000000000)", (page_url,))
    return exemplar, alias


@pytest.mark.parametrize('change_page2', [False, True])
def test_effective_dates_first_page_not_enough_all_ID_hashes_required_no_clock_clone(queue, change_page2):
    exemplar, alias = seed_equivalent(queue, change_page2=change_page2)
    capture(queue, exemplar, source_payload(range(1, 31), epoch=bounds(exemplar)[0], total=31))
    assert head(queue, alias)['state'] == 'waiting_page_evidence'
    capture(queue, cnyes_url(BASE, 2, start=bounds(exemplar)[0], end=bounds(exemplar)[1]),
            source_payload([31], epoch=bounds(exemplar)[0], page=2, total=31))
    if change_page2:
        assert head(queue, alias)['state'] == 'waiting_original_page_recheck'
        direct = cnyes_url(BASE, 2, start=bounds(alias)[0], end=bounds(alias)[1])
        assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (direct,)).fetchone()[0] < 9000000000
    else:
        assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
        assert json.loads(head(queue, alias)['member_ids_json']) == list(range(1, 32))
        assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_ID_references WHERE scope_url=?', (cnyes_scope(alias),)).fetchone()[0] == 2
    assert queue.db.execute('SELECT observed_at_utc FROM bulk_api_coverage WHERE base_url=?', (cnyes_scope(alias),)).fetchone()[0] == 'old-alias-clock'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations WHERE scope_url=?', (cnyes_scope(alias),)).fetchone()[0] == 0


def test_repeated_source_bytes_keep_first_observation_and_old_retained_cannot_replace_forward(queue):
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    r = capture(queue, url, source_payload([1, 2], epoch=low))
    source = queue.registry.sources['cnyes']
    info, posts = cnyes_pagination(r)
    before = tuple(queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone())
    with queue.db:
        record_page(queue, r, info, posts, source, {'kind': 'actual-retained-reference'},
                    observed_at='2019-01-01T00:00:00+00:00', origin='verified_retained_source_response')
    assert tuple(queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone()) == before
    assert queue.db.execute('SELECT last_confirmed_at_utc FROM cnyes_history_page_heads').fetchone()[0].startswith('2026-')


def test_alias_proof_never_conceals_known_source_date_contradiction(queue):
    exemplar, alias = seed_equivalent(queue)
    # Metadata disagrees with the measured source date while preserving IDs.
    capture(queue, alias, source_payload(range(1, 31), epoch=bounds(alias)[0] - 86400, total=31))
    capture(queue, exemplar, source_payload(range(1, 31), epoch=bounds(exemplar)[0], total=31))
    capture(queue, cnyes_url(BASE, 2, start=bounds(exemplar)[0], end=bounds(exemplar)[1]),
            source_payload([31], epoch=bounds(exemplar)[0], page=2, total=31))
    assert head(queue, alias)['state'] == 'inconsistent_source_identity_or_date'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations WHERE scope_url=?',
                            (cnyes_scope(alias),)).fetchone()[0] == 1


def test_duplicate_source_SQL_rows_retained_but_not_unique_article_denominator(queue):
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 1)
    data = source_payload([1, 1, 2, 2], epoch=low, total=4)
    data['items'].update({'from': 1, 'to': 4})
    capture(queue, url, data)
    proof = json.loads(queue.db.execute('SELECT proof_json FROM cnyes_history_scope_proofs WHERE proof_sha256=?',
                                       (head(queue, url)['proof_sha256'],)).fetchone()[0])
    assert proof['unique_article_IDs'] == 2
    assert proof['facts']['total_articles_raw'] == 4
    assert proof['facts']['duplicate_rows_observed'] == 2
    assert proof['facts']['declared_totals_are_not_unique_article_denominators']


def test_same_response_bytes_reuse_canonical_source_without_body_or_capture_rewrite(queue):
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 1)
    data = source_payload([1, 2], epoch=low)
    capture(queue, url, data)
    original = [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')]
    capture(queue, url, data)
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
    assert [{k: v for k, v in dict(row).items() if k != 'last_seen_utc'} for row in queue.db.execute('SELECT * FROM articles ORDER BY url')] == [
        {k: v for k, v in row.items() if k != 'last_seen_utc'} for row in original]
    # The first source is already durable inside its body batch. Repeated
    # identical responses use that exact reference, not another raw copy.
    capture(queue, url, data)
    assert len(list(queue.root.glob('discovery_responses/*.parquet'))) == 0
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 1


def test_unrecorded_legacy_list_retains_exactly_one_source_without_recopied_body(queue):
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 1)
    data = source_payload([1, 2], epoch=low)
    capture(queue, url, data)
    with queue.db:  # Isolated fixture representing the old hash-only scheduler.
        queue.db.execute('DELETE FROM cnyes_history_page_observations')
        queue.db.execute('DELETE FROM cnyes_history_page_heads')
    capture(queue, url, data)
    capture(queue, url, data)
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
    assert len(list(queue.root.glob('discovery_responses/*.parquet'))) == 1
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 1


def test_restricted_response_keeps_public_ID_metadata_not_protected_text_or_body_receipt(queue):
    url = cnyes_url(BASE, start=1388534400, end=1388534401)
    data = source_payload([1, 2], epoch=1388534400)
    data['items']['data'][1]['payment'] = 1
    data['items']['data'][1]['content'] = 'SECRET PROTECTED CONTENT NOT TO PERSIST'
    capture(queue, url, data)
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
    raw = queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0]
    assert 'SECRET PROTECTED' not in raw and 'restricted_public_metadata_only' in raw
    assert not list(queue.root.glob('discovery_responses/*.parquet')) and not list(queue.root.glob('quarantine/*.parquet'))
    assert queue.db.execute('SELECT error_code FROM discovery WHERE url=?', (url,)).fetchone()[0] == 'bulk_response_contains_restricted_text'


def test_due_restricted_page_without_history_head_recovers_on_its_existing_request(cfg, tmp_path, monkeypatch):
    """An old mixed page can recover IDs without retaining its restricted envelope."""
    from downloader import news_fulltext as owner
    from test_news_fulltext import FakeResponse, http_fixture

    monkeypatch.setattr(owner, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *_: False)
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    data = source_payload([1, 2], epoch=low)
    data['items']['data'][1]['payment'] = 1
    data['items']['data'][1]['content'] = 'SECRET PROTECTED CONTENT NOT TO PERSIST'
    r = response(data, url)
    info, _ = cnyes_pagination(r)
    digest = hashlib.sha256(r.body).hexdigest()
    http, session = http_fixture(cfg, tmp_path, {
        'https://api.cnyes.com/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(r.body, headers={'Content-Type': 'application/json', 'ETag': 'same', 'Last-Modified': 'old-date'}),
    })
    queue = NewsQueue(tmp_path, http.registry)
    original_get = session.get
    def conditional_source(request_url, **kwargs):
        if request_url == url:
            headers = kwargs.get('headers', {})
            session.routes[url] = (FakeResponse(status=304, headers={'ETag': 'same'})
                                   if headers.get('If-None-Match') else
                                   FakeResponse(r.body, headers={'Content-Type': 'application/json', 'ETag': 'same', 'Last-Modified': 'old-date'}))
        return original_get(request_url, **kwargs)
    session.get = conditional_source
    try:
        source = queue.registry.sources['cnyes']
        with queue.db:
            queue.db.execute('INSERT INTO discovery(url,kind,provider,due,error_code,attempts,etag,last_modified,response_sha256,'
                'parser_fingerprint,last_checked_at_utc) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                (url, 'cnyes_news', 'cnyes', 0, 'bulk_response_contains_restricted_text', 0, 'same',
                 'old-date', digest, owner.discovery_fingerprint(source), '2020-01-01T00:00:00+00:00'))
            queue.db.execute('INSERT INTO bulk_page_evidence VALUES (?,?,?,?,?)',
                             (info['scope'], 1, info['total'], info['ids_sha256'], '2020-01-01T00:00:00+00:00'))
            queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',?,?,'2020-01-01T00:00:00+00:00')",
                             (info['scope'], info['total'], info['pages']))
            queue.db.execute("INSERT INTO bulk_item_errors VALUES (?,1,'cnyes',?,'bulk_post_not_public','2020-01-01T00:00:00+00:00')",
                             (url, 'https://news.cnyes.com/news/id/2'))
        owner.run_cycle(queue, http, cfg, max_articles=0, max_discovery_requests=1)
        requests = [kwargs for requested, kwargs in session.calls if requested == url]
        assert len(requests) == 1 and 'If-None-Match' not in requests[0]['headers']
        assert 'If-Modified-Since' not in requests[0]['headers']
        row = queue.db.execute('SELECT o.observation_json FROM cnyes_history_page_heads h '
            'JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
            'WHERE h.scope_url=? AND h.page=1', (info['scope'],)).fetchone()
        assert row is not None
        observation = json.loads(row[0])
        assert observation['artifact']['kind'] == 'restricted_public_metadata_only'
        assert [i['newsId'] for i in observation['identities']] == [1, 2]
        assert observation['body_completeness_verified'] is False
        assert 'SECRET PROTECTED' not in row[0]
        assert not list(tmp_path.glob('**/*.parquet'))
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert queue.db.execute('SELECT error_code FROM discovery WHERE url=?', (url,)).fetchone()[0] == 'bulk_response_contains_restricted_text'
        assert queue.db.execute('SELECT url FROM articles').fetchone()[0] == 'https://news.cnyes.com/news/id/1'

        # A new owner can reuse the durable witness. A 304 after a prior
        # transport failure must restore, rather than erase, the paid gap.
        queue.close()
        queue = NewsQueue(tmp_path, http.registry)
        with queue.db:
            queue.db.execute("UPDATE discovery SET due=0,error_code='http_429',attempts=1 WHERE url=?", (url,))
        owner.run_cycle(queue, http, cfg, max_articles=0, max_discovery_requests=1)
        requests = [kwargs for requested, kwargs in session.calls if requested == url]
        assert len(requests) == 2 and requests[-1]['headers']['If-None-Match'] == 'same'
        assert requests[-1]['headers']['If-Modified-Since'] == 'old-date'
        assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 1
        assert queue.db.execute('SELECT error_code FROM discovery WHERE url=?', (url,)).fetchone()[0] == 'bulk_response_contains_restricted_text'
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('failure', ['http_429', 'source_access_denied', 'worker_TimeoutError', None])
def test_missing_restricted_history_recovers_after_prior_failure_without_resetting_deadline(queue, failure):
    from downloader.news_fulltext import discovery_fingerprint
    from downloader.news_fulltext_cnyes_history import prepare_restricted_history_dispatch

    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    source = queue.registry.sources['cnyes']
    fingerprint = discovery_fingerprint(source, kind='cnyes_news')
    with queue.db:
        queue.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code,attempts,etag,"
                         "last_modified,response_sha256,parser_fingerprint) "
                         "VALUES (?,'cnyes_news','cnyes',9000000000,?,1,'old','old-date',?,?)",
                         (url, failure, 'a' * 64, fingerprint))
        queue.db.execute("INSERT INTO bulk_item_errors VALUES (?,1,'cnyes',?,'bulk_post_not_public','old-clock')",
                         (url, 'https://news.cnyes.com/news/id/2'))
    original = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    assert not queue.discovery_candidates(source, 1)  # The existing future retry remains binding.
    task = dict(original)
    prepare_restricted_history_dispatch(queue, task, source, compatible_fingerprint=fingerprint)
    assert task['_cnyes_missing_restricted_history_evidence'] is True
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == original
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 0


def test_restricted_history_dispatch_reuses_only_verified_matching_page_evidence(queue):
    from downloader.news_fulltext import discovery_fingerprint
    from downloader.news_fulltext_cnyes_history import prepare_restricted_history_dispatch

    exemplar, alias = seed_equivalent(queue)
    capture(queue, exemplar, source_payload(range(1, 31), epoch=bounds(exemplar)[0], total=31))
    source = queue.registry.sources['cnyes']
    fingerprint = discovery_fingerprint(source, kind='cnyes_news')
    with queue.db:
        queue.db.execute("UPDATE discovery SET etag='same',response_sha256=?,parser_fingerprint=?,"
                         "error_code='bulk_response_contains_restricted_text' WHERE url=?",
                         ('a' * 64, fingerprint, alias))
    original = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone())
    task = dict(original)
    prepare_restricted_history_dispatch(queue, task, source, compatible_fingerprint=fingerprint)
    assert '_cnyes_missing_restricted_history_evidence' not in task  # Proven shared IDs.
    with queue.db:
        queue.db.execute('UPDATE bulk_page_evidence SET ids_sha256=? WHERE scope_url=? AND page=1',
                         ('b' * 64, cnyes_scope(alias)))
    task = dict(original)
    prepare_restricted_history_dispatch(queue, task, source, compatible_fingerprint=fingerprint)
    assert task['_cnyes_missing_restricted_history_evidence'] is True  # No proof for changed IDs.
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone()) == original


def test_restricted_history_dispatch_rejects_stale_direct_response_witness(queue):
    from downloader.news_fulltext import discovery_fingerprint
    from downloader.news_fulltext_cnyes_history import prepare_restricted_history_dispatch

    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    data = source_payload([1, 2], epoch=low)
    data['items']['data'][1]['payment'] = 1
    capture(queue, url, data)
    source = queue.registry.sources['cnyes']
    fingerprint = discovery_fingerprint(source, kind='cnyes_news')
    with queue.db:
        queue.db.execute("UPDATE discovery SET etag='old',response_sha256=? WHERE url=?", ('a' * 64, url))
    task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    prepare_restricted_history_dispatch(queue, task, source, compatible_fingerprint=fingerprint)
    assert task['_cnyes_missing_restricted_history_evidence'] is True


def test_restricted_history_unconditional_304_is_not_accepted(cfg, tmp_path):
    from downloader.news_fulltext import discovery_fingerprint, fetch_discovery
    from downloader.news_fulltext_policy import NewsError
    from test_news_fulltext import FakeResponse, http_fixture

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    http, session = http_fixture(cfg, tmp_path, {
        'https://api.cnyes.com/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(status=304),
    })
    source = http.registry.sources['cnyes']
    task = {'url': url, 'kind': 'cnyes_news', 'provider': 'cnyes', 'etag': 'old', 'last_modified': 'old-date',
            'parser_fingerprint': discovery_fingerprint(source, kind='cnyes_news'),
            '_cnyes_missing_restricted_history_evidence': True}
    try:
        with pytest.raises(NewsError, match='unverifiable_discovery_not_modified') as error:
            fetch_discovery(http, task)
        assert error.value.retry and error.value.delay == 60
        headers = [kwargs['headers'] for requested, kwargs in session.calls if requested == url][0]
        assert 'If-None-Match' not in headers and 'If-Modified-Since' not in headers
    finally:
        http.close()


def test_restricted_history_429_preserves_provider_and_page_cooldowns(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as owner
    from downloader.news_fulltext_cnyes_history import prepare_restricted_history_dispatch
    from test_news_fulltext import FakeResponse, http_fixture

    monkeypatch.setattr(owner, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *_: False)
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    http, session = http_fixture(cfg, tmp_path, {
        'https://api.cnyes.com/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(status=429, headers={'Retry-After': '120'}),
    })
    queue = NewsQueue(tmp_path, http.registry)
    source = http.registry.sources['cnyes']
    fingerprint = owner.discovery_fingerprint(source, kind='cnyes_news')
    try:
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code,attempts,etag,response_sha256,parser_fingerprint) "
                             "VALUES (?,'cnyes_news','cnyes',0,'bulk_response_contains_restricted_text',0,'old',?,?)",
                             (url, 'a' * 64, fingerprint))
            queue.db.execute("INSERT INTO bulk_item_errors VALUES (?,1,'cnyes',?,'bulk_post_not_public','old-clock')",
                             (url, 'https://news.cnyes.com/news/id/2'))
        owner.run_cycle(queue, http, cfg, max_articles=0, max_discovery_requests=1)
        row = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
        assert row['error_code'] == 'http_429' and row['attempts'] == 1 and row['due'] > time.time() + 100
        assert row['etag'] == 'old' and http.provider_not_before['cnyes'] > time.time() + 100
        assert len([x for x in session.calls if x[0] == url]) == 1
        owner.run_cycle(queue, http, cfg, max_articles=0, max_discovery_requests=1)
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == row
        assert len([x for x in session.calls if x[0] == url]) == 1
        task = dict(row)  # Inspect future dispatch, without making the retry due.
        prepare_restricted_history_dispatch(queue, task, source, compatible_fingerprint=fingerprint)
        assert task['_cnyes_missing_restricted_history_evidence'] is True
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == row
    finally:
        queue.close()
        http.close()


def test_restricted_history_dispatch_ignores_nonhistorical_unregistered_or_unrelated_pages(queue):
    from downloader.news_fulltext import discovery_fingerprint
    from downloader.news_fulltext_cnyes_history import prepare_restricted_history_dispatch

    source = queue.registry.sources['cnyes']
    fingerprint = discovery_fingerprint(source, kind='cnyes_news')
    historical = cnyes_url(BASE, start=1388534400, end=1388534500)
    task = {'url': historical, 'kind': 'cnyes_news', 'provider': 'cnyes', 'etag': 'same',
            'last_modified': None, 'response_sha256': 'a' * 64, 'parser_fingerprint': fingerprint,
            'error_code': None}
    cases = [task, dict(task, url=cnyes_url(BASE), error_code='bulk_response_contains_restricted_text'),
             dict(task, url=cnyes_url('https://api.cnyes.com/not-registered', start=1388534400, end=1388534500),
                  error_code='bulk_response_contains_restricted_text'),
             dict(task, parser_fingerprint='old', error_code='bulk_response_contains_restricted_text'),
             dict(task, etag=None, error_code='bulk_response_contains_restricted_text'),
             dict(task, kind='sitemap', error_code='bulk_response_contains_restricted_text')]
    for candidate in cases:
        before = dict(candidate)
        prepare_restricted_history_dispatch(queue, candidate, source, compatible_fingerprint=fingerprint)
        assert candidate == before


def seed_retained_work(queue, *, failure=None, valid_raw=True, low=1388534400):
    url = cnyes_url(BASE, start=low, end=low + 100)
    r = response(source_payload([1, 2], epoch=low), url)
    info, _ = cnyes_pagination(r)
    sha = hashlib.sha256(r.body).hexdigest()
    source = queue.registry.sources['cnyes']
    with queue.db:
        queue.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code,attempts,response_sha256,last_checked_at_utc) "
            "VALUES (?,'cnyes_news','cnyes',9e9,?,?,?,'2020-01-01T00:00:00+00:00')", (url, failure, int(failure is not None), sha))
        queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',2,1,'2020-source-clock')", (info['scope'],))
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
            "VALUES (?,?,1,?,2,?,'2020-01-01T00:00:00+00:00')", (url, info['scope'], sha, info['ids_sha256']))
    if valid_raw:
        target = queue.root / 'quarantine' / (sha + '.parquet')
        atomic_write_parquet(target, table_from_records([{'request_url': url, 'raw_response_body': r.body,
            'response_sha256': sha, 'captured_at_utc': '2020-01-01T00:00:00+00:00',
            'authorization_fingerprint': source.authorization_fingerprint, 'publish': False}]), durable=True)
        atomic_write_json(target.with_suffix('.parquet.receipt.json'), {
            'sha256': sha256_file(target), 'response_sha256': sha, 'publish': False}, durable=True)
    return url, r


def move_retained_to_discovery(queue, response):
    sha = hashlib.sha256(response.body).hexdigest()
    old = queue.root / 'quarantine' / (sha + '.parquet')
    new = queue.root / 'discovery_responses' / (sha + '.parquet')
    new.parent.mkdir(parents=True, exist_ok=True)
    old.replace(new)
    old.with_suffix('.parquet.receipt.json').replace(new.with_suffix('.parquet.receipt.json'))
    return new


@pytest.mark.parametrize('failure', [None, 'http_429', 'source_access_denied'])
def test_discovery_envelope_recovery_preserves_job_and_source_deadline(queue, failure):
    url, r = seed_retained_work(queue, failure=failure)
    path = move_retained_to_discovery(queue, r)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert head(queue, url)['state'] == 'list_complete'
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert observation['artifact']['kind'] == 'cnyes_history_source_envelope'
    assert observation['artifact']['parquet_path'] == str(path.relative_to(queue.root))
    assert observation['source_observed_at_utc'] == '2020-01-01T00:00:00+00:00'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0


def test_discovery_envelope_same_bytes_different_request_is_not_relabelled(queue):
    import pyarrow.parquet as pq
    url, r = seed_retained_work(queue, failure='http_429')
    path = move_retained_to_discovery(queue, r)
    record = pq.read_table(path).to_pylist()[0]
    record['request_url'] = cnyes_url(BASE, start=bounds(url)[0] + 1, end=bounds(url)[1] + 1)
    atomic_write_parquet(path, table_from_records([record]), durable=True)
    atomic_write_json(path.with_suffix('.parquet.receipt.json'), {
        'sha256': sha256_file(path), 'response_sha256': hashlib.sha256(r.body).hexdigest(), 'publish': False}, durable=True)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    backfill(queue, max_seconds=10)
    row = queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work').fetchone()
    assert row['state'] == 'retained_request_mismatch'
    assert json.loads(row['outcome_json'])['error'] == 'retained_original_request_differs_not_relabelled'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 0
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_valid_discovery_envelope_wins_over_wrong_request_in_quarantine(queue):
    import pyarrow.parquet as pq
    url, r = seed_retained_work(queue)
    discovery_path = move_retained_to_discovery(queue, r)
    quarantine = queue.root / 'quarantine' / discovery_path.name
    record = pq.read_table(discovery_path).to_pylist()[0]
    record['request_url'] = cnyes_url(BASE, start=bounds(url)[0] + 1, end=bounds(url)[1] + 1)
    atomic_write_parquet(quarantine, table_from_records([record]), durable=True)
    atomic_write_json(quarantine.with_suffix('.parquet.receipt.json'), {
        'sha256': sha256_file(quarantine), 'response_sha256': hashlib.sha256(r.body).hexdigest(), 'publish': False}, durable=True)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    backfill(queue, max_seconds=10)
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work').fetchone()[0] == 'restored'
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert observation['artifact']['parquet_path'] == str(discovery_path.relative_to(queue.root))
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_invalid_discovery_envelope_does_not_bypass_source_failure(queue):
    url, r = seed_retained_work(queue, failure='http_429')
    path = move_retained_to_discovery(queue, r)
    receipt = path.with_suffix('.parquet.receipt.json')
    value = json.loads(receipt.read_bytes())
    value['sha256'] = '0' * 64
    atomic_write_json(receipt, value, durable=True)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    backfill(queue, max_seconds=10)
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work').fetchone()[0] == 'retained_invalid'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 0
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_discovery_envelope_io_error_retries_locally_without_resetting_job(queue, monkeypatch):
    from pathlib import Path
    url, r = seed_retained_work(queue)
    path = move_retained_to_discovery(queue, r)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    original = Path.read_bytes
    def unavailable(candidate):
        if candidate == path.with_suffix('.parquet.receipt.json'):
            raise OSError(5, 'temporary local error')
        return original(candidate)
    with monkeypatch.context() as patch:
        patch.setattr(Path, 'read_bytes', unavailable)
        assert backfill(queue, max_seconds=10)['local_io_deferrals'] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work').fetchone()[0] == 'local_retry'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 WHERE request_url=?', (url,))
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert head(queue, url)['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


@pytest.mark.parametrize('failure', [None, 'discovery_http_429', 'discovery_http_403', 'transport_retry', 'bulk_pagination_totals_invalid'])
def test_retained_backfill_exact_source_clock_and_no_source_calls_or_failure_reset(queue, failure):
    url, _ = seed_retained_work(queue, failure=failure)
    before = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    result = backfill(queue, max_seconds=10)
    assert result['processed'] == 1 and result['exhausted']
    assert head(queue, url)['state'] == 'list_complete'
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert observation['source_observed_at_utc'] == '2020-01-01T00:00:00+00:00'
    assert observation['evidence_recorded_at_utc'] > observation['source_observed_at_utc']
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0


@pytest.mark.parametrize('failure', [None, 'discovery_http_429', 'discovery_http_403', 'transport_retry'])
def test_missing_raw_rechecks_only_successful_page_once_not_failed_deadline(queue, failure):
    url, _ = seed_retained_work(queue, failure=failure, valid_raw=False)
    original = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    backfill(queue, max_seconds=10)
    current = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    if failure:
        assert current == original
    else:
        assert current['due'] < original['due']
        assert {k: v for k, v in current.items() if k != 'due'} == {k: v for k, v in original.items() if k != 'due'}
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 0
    second = deepcopy(current)
    backfill(queue, max_seconds=10)
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == second


def test_stop_and_zero_time_slice_do_not_consume_or_defer_work(queue):
    seed_retained_work(queue)
    import threading
    stop = threading.Event()
    stop.set()
    assert backfill(queue, stop=stop)['processed'] == 0
    result = backfill(queue, max_seconds=0)
    assert result['processed'] == 0 and not result['exhausted']
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work').fetchone()[0] == 'pending'


def test_old_reference_wrong_request_is_not_source_corruption_or_backdated_capture(queue):
    url, r = seed_retained_work(queue)
    sha = hashlib.sha256(r.body).hexdigest()
    target = queue.root / 'quarantine' / (sha + '.parquet')
    import pyarrow.parquet as pq
    row = pq.read_table(target).to_pylist()[0]
    row['request_url'] = cnyes_url(BASE, start=bounds(url)[0] + 1, end=bounds(url)[1] + 1)
    atomic_write_parquet(target, table_from_records([row]), durable=True)
    atomic_write_json(target.with_suffix('.parquet.receipt.json'), {
        'sha256': sha256_file(target), 'response_sha256': sha, 'publish': False}, durable=True)
    backfill(queue, max_seconds=10)
    work = queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work').fetchone()
    assert work['state'] == 'retained_request_mismatch'
    assert 'retained_original_request_differs_not_relabelled' in work['outcome_json']
    assert json.loads(work['outcome_json'])['one_successful_page_evidence_recheck_scheduled']
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 0


def test_legacy_request_mismatches_reconcile_only_with_verified_direct_current_evidence(queue):
    first, _ = seed_retained_work(queue)
    second, _ = seed_retained_work(queue, low=1388620800)
    third, _ = seed_retained_work(queue, low=1388707200)
    capture(queue, first, source_payload([1, 2], epoch=bounds(first)[0]))
    previous = json.dumps({'error': 'retained_original_request_differs_not_relabelled',
                           'one_successful_page_evidence_recheck_scheduled': True})
    other = json.dumps({'error': 'invalid_original_envelope_receipt'})
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_retained_work SET state='retained_invalid',outcome_json=? "
                         'WHERE request_url IN (?,?)', (previous, first, second))
        queue.db.execute("UPDATE cnyes_history_retained_work SET state='retained_invalid',outcome_json=? "
                         'WHERE request_url=?', (other, third))
    discovery = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    first_slice = reconcile_legacy_request_mismatches(queue, limit=1, max_seconds=10)
    assert not first_slice['complete'] and first_slice['checked'] == 1
    for _ in range(5):
        result = reconcile_legacy_request_mismatches(queue, limit=1, max_seconds=10)
        if result['complete']:
            break
    assert result['complete']
    states = dict(queue.db.execute('SELECT request_url,state FROM cnyes_history_retained_work'))
    assert states == {first: 'forward_evidence_available', second: 'retained_request_mismatch', third: 'retained_invalid'}
    resolved = {row['request_url']: dict(row) for row in queue.db.execute('SELECT * FROM cnyes_history_retained_outcome_resolutions')}
    assert resolved[first]['original_outcome_json'] == resolved[second]['original_outcome_json'] == previous
    assert resolved[first]['observation_sha256'] and resolved[second]['observation_sha256'] is None
    assert queue.db.execute('SELECT outcome_json FROM cnyes_history_retained_work WHERE request_url=?', (first,)).fetchone()[0] == previous
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == discovery
    assert reconcile_legacy_request_mismatches(queue, max_seconds=10)['reclassified'] == 0
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_retained_outcome_resolutions').fetchone()[0] == 2


@pytest.mark.parametrize('corruption', ['digest', 'source_contract'])
def test_legacy_reconciliation_does_not_trust_invalid_or_old_direct_evidence(queue, corruption):
    url, _ = seed_retained_work(queue)
    capture(queue, url, source_payload([1, 2], epoch=bounds(url)[0]))
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_retained_work SET state='retained_invalid',outcome_json=? WHERE request_url=?",
                         (json.dumps({'error': 'retained_original_request_differs_not_relabelled'}), url))
        if corruption == 'digest':
            queue.db.execute("UPDATE cnyes_history_page_observations SET observation_json='{}'")
        else:
            queue.db.execute("UPDATE cnyes_history_page_observations SET source_bulk_fingerprint='old-contract'")
    result = reconcile_legacy_request_mismatches(queue, max_seconds=10)
    assert result['complete'] and result['reclassified'] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work').fetchone()[0] == 'retained_request_mismatch'
    assert queue.db.execute('SELECT observation_sha256 FROM cnyes_history_retained_outcome_resolutions').fetchone()[0] is None


@pytest.mark.parametrize('fenced', [False, True])
def test_later_direct_page_resolves_unavailable_even_after_scan_completed(queue, fenced):
    url, _ = seed_retained_work(queue, valid_raw=False)
    previous = json.dumps({'publisher_requests': 0, 'old_state': 'no_local_original'})
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_retained_work SET state='retained_unavailable',outcome_json=? WHERE request_url=?",
                         (previous, url))
        if fenced:
            from downloader.news_fulltext_policy import cnyes_bulk_fingerprint
            source = queue.registry.sources['cnyes']
            queue.db.execute('INSERT INTO cnyes_history_local_inventory_scopes VALUES (?,?,?,?,?)',
                (cnyes_scope(url), cnyes_bulk_fingerprint(source), source.authorization_fingerprint,
                 'ready', '2020-01-01T00:00:00+00:00'))
    assert reconcile_legacy_unavailable(queue, max_seconds=10)['resolved'] == 0
    capture(queue, url, source_payload([1, 2], epoch=bounds(url)[0]))
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()[0] == 'forward_evidence_available'
    receipt = queue.db.execute('SELECT * FROM cnyes_history_retained_outcome_resolutions '
                               "WHERE request_url=? AND evidence_kind='direct'", (url,)).fetchone()
    assert receipt['original_state'] == 'retained_unavailable' and receipt['original_outcome_json'] == previous
    assert receipt['evidence_kind'] == 'direct' and receipt['observation_sha256']
    candidate = queue.db.execute('SELECT original_state,original_outcome_json,resolution_state '
        "FROM cnyes_history_retained_outcome_resolutions WHERE request_url=? "
        "AND evidence_kind='new_accepted_body_batch_candidate'", (url,)).fetchone()
    assert tuple(candidate) == ('retained_unavailable', previous, 'pending')
    assert queue.db.execute('SELECT 1 FROM cnyes_history_local_inventory_scopes WHERE scope_url=?',
                            (cnyes_scope(url),)).fetchone() is None


def test_later_direct_page_resolves_candidate_waiting_on_local_retry(queue):
    from downloader.news_fulltext_cnyes_history import seed_accepted_body_batch_candidates
    urls = seed_unindexed_nonzero_pages(queue)
    seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    page = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone()
    job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    old_outcome = json.dumps({'error': 'previous_missing_local_original'})
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,'
            'source_checked_at_utc,state,outcome_json) VALUES (?,?,?,?,?,?,?,?,?)',
            (urls[2], cnyes_scope(urls[0]), 3, job['response_sha256'], page['total'], page['ids_sha256'],
             page['observed_at_utc'], 'retained_unavailable', old_outcome))
    assert seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['complete']
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_retained_work SET state='local_retry' WHERE request_url=?", (urls[2],))
        queue.db.execute('INSERT INTO cnyes_history_retained_local_retries VALUES (?,?,?,?,?,?)',
            (urls[2], 'pending', 9e9, 1, 'accepted_batch_scan_pending', '2020-01-01T00:00:00+00:00'))
    capture(queue, urls[2], source_payload([61], epoch=1388534400, page=3, total=61))
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                            (urls[2],)).fetchone()[0] == 'forward_evidence_available'
    assert queue.db.execute('SELECT 1 FROM cnyes_history_retained_local_retries WHERE request_url=?',
                            (urls[2],)).fetchone() is None
    event = queue.db.execute('SELECT original_state,original_outcome_json,observation_sha256 '
        "FROM cnyes_history_retained_outcome_resolutions WHERE request_url=? AND evidence_kind='direct'",
        (urls[2],)).fetchone()
    assert event['original_state'] == 'retained_unavailable' and event['original_outcome_json'] == old_outcome
    assert event['observation_sha256']


def test_legacy_unavailable_scan_checkpoint_survives_restart(cfg, tmp_path):
    registry, _ = registry_http(cfg, tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as owner:
        urls = [seed_retained_work(owner, low=low)[0] for low in (1388534400, 1388620800)]
        for url in urls:
            capture(owner, url, source_payload([1, 2], epoch=bounds(url)[0]))
        with owner.db:
            owner.db.execute("UPDATE cnyes_history_retained_work SET state='retained_unavailable',outcome_json='{}'")
        result = reconcile_legacy_unavailable(owner, limit=1, max_seconds=10)
        assert not result['complete'] and result['resolved'] == 1
    with closing(NewsQueue(tmp_path, registry)) as resumed:
        for _ in range(3):
            result = reconcile_legacy_unavailable(resumed, limit=1, max_seconds=10)
            if result['complete']:
                break
        assert result['complete']
        assert dict(resumed.db.execute('SELECT state,count(*) FROM cnyes_history_retained_work GROUP BY state')) == {'forward_evidence_available': 2}
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_retained_outcome_resolutions').fetchone()[0] == 2


@pytest.mark.parametrize('tamper', [None, 'prior_ids', 'reference_receipt'])
def test_legacy_unavailable_shared_page_requires_original_identity_receipt(queue, tamper):
    from downloader.news_fulltext_cnyes_history import index_scope
    urls, _ = seed_legacy_overflow(queue)
    source = queue.registry.sources['cnyes']
    r = response(source_payload(range(1, 31), epoch=bounds(urls[0])[0], total=931), urls[0])
    info, posts = cnyes_pagination(r)
    sha = hashlib.sha256(r.body).hexdigest()
    with queue.db:
        for url in urls:
            index_scope(queue, cnyes_scope(url), source.provider)
        record_page(queue, r, info, posts, source, {'kind': 'fixture'}, observed_at='2020-01-01T00:00:00+00:00')
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc,state,outcome_json) '
            "VALUES (?,?,1,?,931,?,'old-clock','retained_unavailable','{}')",
            (urls[1], cnyes_scope(urls[1]), sha, info['ids_sha256']))
        if tamper == 'prior_ids':
            queue.db.execute("UPDATE cnyes_history_retained_work SET expected_ids_sha256='wrong'")
        elif tamper == 'reference_receipt':
            queue.db.execute("UPDATE cnyes_history_page_ID_references SET original_ID_evidence_json='{}'")
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    result = reconcile_legacy_unavailable(queue, max_seconds=10)
    assert result['complete']
    state = queue.db.execute('SELECT state FROM cnyes_history_retained_work').fetchone()[0]
    if tamper is None:
        assert result['resolved'] == 1 and state == 'forward_evidence_available'
        receipt = queue.db.execute('SELECT evidence_kind,observation_sha256 FROM cnyes_history_retained_outcome_resolutions').fetchone()
        assert receipt['evidence_kind'] == 'shared_page_id_reference' and receipt['observation_sha256']
    else:
        assert result['resolved'] == 0 and state == 'retained_unavailable'
        assert queue.db.execute('SELECT count(*) FROM cnyes_history_retained_outcome_resolutions').fetchone()[0] == 0
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before


def test_changed_bulk_contract_cannot_reuse_old_child_proof_for_parent(queue, cfg, tmp_path):
    from downloader.news_fulltext_cnyes_history import reconcile_parents
    parent, children = seed_partition(queue)
    capture(queue, children[0], source_payload([1, 2], epoch=bounds(children[0])[0]))
    capture(queue, children[1], source_payload([3, 4], epoch=bounds(children[1])[0]))
    changed_cfg = deepcopy(cfg)
    changed_cfg['providers'][0]['cnyes_apis'].append(BASE.replace('headline', 'stock_report'))
    changed_registry, _ = registry_http(changed_cfg, tmp_path)
    changed = changed_registry.sources['cnyes']
    assert changed.cnyes_bulk_data_fingerprint != queue.registry.sources['cnyes'].cnyes_bulk_data_fingerprint
    with queue.db:
        reconcile_parents(queue, cnyes_scope(children[0]), changed)
    assert head(queue, parent)['state'] == 'waiting_child_evidence'
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(parent),)).fetchone()


def test_source_body_and_discovery_contracts_do_not_change_with_metadata_ABI(monkeypatch):
    from downloader.news_fulltext import discovery_fingerprint, load_config
    from pathlib import Path
    _, before, _ = load_config(Path('configs/news_fulltext.json'))
    monkeypatch.setattr('downloader.news_fulltext_cnyes_history.CONTRACT_VERSION', CONTRACT_VERSION + 1)
    _, after, _ = load_config(Path('configs/news_fulltext.json'))
    for provider, source in before.sources.items():
        assert source.data_fingerprint == after.sources[provider].data_fingerprint
        assert discovery_fingerprint(source) == discovery_fingerprint(after.sources[provider])


def test_page_evidence_can_be_distributed_across_two_original_queries(queue):
    exemplar, alias = seed_equivalent(queue)
    low, high = bounds(exemplar)
    exemplar_second = cnyes_url(BASE, 2, start=low, end=high)
    info, _ = cnyes_pagination(response(source_payload([31], epoch=low, page=2, total=31), exemplar_second))
    with queue.db:
        queue.db.execute('INSERT INTO bulk_page_evidence VALUES (?,?,?,?,?)',
                         (cnyes_scope(exemplar), 2, 31, info['ids_sha256'], 'real-exemplar-page-clock'))
    capture(queue, exemplar, source_payload(range(1, 31), epoch=low, total=31))
    alias_second = cnyes_url(BASE, 2, start=bounds(alias)[0], end=bounds(alias)[1])
    capture(queue, alias_second, source_payload([31], epoch=low, page=2, total=31))
    for url in (exemplar, alias):
        assert head(queue, url)['state'] == 'list_complete_by_page_ID_equivalence'
        assert json.loads(head(queue, url)['member_ids_json']) == list(range(1, 32))
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 2
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_ID_references').fetchone()[0] == 2


@pytest.mark.parametrize('failure', [None, 'http_403', 'http_429', 'transport_error'])
def test_unmatched_page_falls_back_once_without_resetting_source_failures(queue, failure):
    exemplar, alias = seed_equivalent(queue, change_page2=True)
    direct = cnyes_url(BASE, 2, start=bounds(alias)[0], end=bounds(alias)[1])
    with queue.db:
        queue.db.execute('UPDATE discovery SET error_code=?,attempts=? WHERE url=?', (failure, int(failure is not None), direct))
    prior = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (direct,)).fetchone())
    capture(queue, exemplar, source_payload(range(1, 31), epoch=bounds(exemplar)[0], total=31))
    second = cnyes_url(BASE, 2, start=bounds(exemplar)[0], end=bounds(exemplar)[1])
    capture(queue, second, source_payload([31], epoch=bounds(exemplar)[0], page=2, total=31))
    current = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (direct,)).fetchone())
    assert head(queue, alias)['state'] == 'waiting_original_page_recheck'
    if failure:
        assert current == prior
    else:
        assert current['due'] < prior['due']
        assert {k: v for k, v in current.items() if k != 'due'} == {k: v for k, v in prior.items() if k != 'due'}
    capture(queue, second, source_payload([31], epoch=bounds(exemplar)[0], page=2, total=31))
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (direct,)).fetchone()) == current


def test_changed_shared_page_reopens_completed_alias_until_original_source_check(queue):
    exemplar, alias = seed_equivalent(queue)
    low, high = bounds(exemplar)
    capture(queue, exemplar, source_payload(range(1, 31), epoch=low, total=31))
    second = cnyes_url(BASE, 2, start=low, end=high)
    capture(queue, second, source_payload([31], epoch=low, page=2, total=31))
    assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
    original_proof = head(queue, alias)['proof_sha256']
    capture(queue, second, source_payload([32], epoch=low, page=2, total=31))
    assert head(queue, alias)['state'] == 'waiting_original_page_recheck'
    assert queue.db.execute('SELECT 1 FROM cnyes_history_scope_proofs WHERE proof_sha256=?', (original_proof,)).fetchone()
    direct = cnyes_url(BASE, 2, start=bounds(alias)[0], end=bounds(alias)[1])
    capture(queue, direct, source_payload([32], epoch=low, page=2, total=31))
    assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
    assert json.loads(head(queue, alias)['member_ids_json']) == [*range(1, 31), 32]


def test_revised_source_total_ignores_obsolete_pages_but_preserves_observations(queue):
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    capture(queue, url, source_payload(range(1, 31), epoch=low, total=31))
    capture(queue, cnyes_url(BASE, 2, start=low, end=low + 100), source_payload([31], epoch=low, page=2, total=31))
    capture(queue, url, source_payload([1], epoch=low))
    assert head(queue, url)['state'] == 'list_complete'
    assert json.loads(head(queue, url)['member_ids_json']) == [1]
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 3


def test_forward_page_cap_growth_invalidates_old_complete_head(queue):
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    capture(queue, url, source_payload([1], epoch=low))
    assert head(queue, url)['state'] == 'list_complete'
    capture(queue, url, source_payload(range(1, 31), epoch=low, total=1000))
    assert head(queue, url)['state'] == 'source_page_cap_exceeded'
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(url),)).fetchone()


def test_legacy_zero_total_reuses_its_exact_retained_empty_envelope(queue):
    from downloader.news_fulltext_cnyes_history import initialize
    parent, children = seed_partition(queue, total=1)
    capture(queue, children[0], source_payload([], epoch=bounds(children[0])[0]))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (children[0],)).fetchone())
    with queue.db:
        for table in ('cnyes_history_page_heads', 'cnyes_history_page_observations', 'cnyes_history_scope_heads'):
            queue.db.execute('DELETE FROM ' + table)
        queue.db.execute("DELETE FROM queue_settings WHERE key='cnyes_history_evidence_initialization_v1'")
    initialize(queue)
    row = queue.db.execute('SELECT * FROM cnyes_history_retained_work').fetchone()
    assert row['expected_total'] == 0 and row['state'] == 'pending'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 0
    backfill(queue, max_seconds=10)
    assert head(queue, children[0])['state'] == 'list_complete'
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert observation['request_url'] == children[0]
    assert observation['artifact']['kind'] == 'cnyes_history_source_envelope'
    assert observation['origin'] == 'verified_retained_source_response'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (children[0],)).fetchone()) == before
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(parent),)).fetchone()


def test_legacy_zero_total_without_retained_envelope_never_creates_a_page(queue):
    from downloader.news_fulltext_cnyes_history import initialize
    parent, children = seed_partition(queue, total=1)
    r = capture(queue, children[0], source_payload([], epoch=bounds(children[0])[0]))
    path = queue.root / 'discovery_responses' / (hashlib.sha256(r.body).hexdigest() + '.parquet')
    path.unlink()
    path.with_suffix('.parquet.receipt.json').unlink()
    with queue.db:
        for table in ('cnyes_history_page_heads', 'cnyes_history_page_observations', 'cnyes_history_scope_heads'):
            queue.db.execute('DELETE FROM ' + table)
        queue.db.execute("DELETE FROM queue_settings WHERE key='cnyes_history_evidence_initialization_v1'")
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=1,due=9e9 WHERE url=?", (children[0],))
    initialize(queue)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (children[0],)).fetchone())
    backfill(queue, max_seconds=10)
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work').fetchone()[0] == 'retained_unavailable'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 0
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 0
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(parent),)).fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (children[0],)).fetchone()) == before


def test_incomplete_empty_scope_checks_its_existing_discovery_envelope_first(queue):
    from downloader.news_fulltext_cnyes_history import reconcile_scope
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    with queue.db:
        for table in ('cnyes_history_page_heads', 'cnyes_history_page_observations', 'cnyes_history_scope_heads'):
            queue.db.execute('DELETE FROM ' + table)
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    with queue.db:
        reconcile_scope(queue, cnyes_scope(url), queue.registry.sources['cnyes'])
    assert head(queue, url)['state'] == 'waiting_page_evidence'
    work = queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
    assert work is not None and work['expected_total'] == 0 and work['state'] == 'pending'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert head(queue, url)['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_owner_discovers_unindexed_legacy_empty_scope_without_rechecking_source(queue):
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    complete = cnyes_url(BASE, start=1388534600, end=1388534700)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    capture(queue, complete, source_payload([1], epoch=bounds(complete)[0]))
    complete_before = head(queue, complete)
    with queue.db:
        for table in ('cnyes_history_page_heads', 'cnyes_history_page_observations', 'cnyes_history_scope_heads'):
            queue.db.execute('DELETE FROM ' + table + ' WHERE scope_url=?', (cnyes_scope(url),))
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    result = backfill(queue, max_seconds=10)
    assert result['processed'] == 1
    assert head(queue, url)['state'] == 'list_complete'
    assert head(queue, complete) == complete_before
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1


def test_unindexed_empty_same_response_from_other_request_requires_own_source_check(queue):
    original = cnyes_url(BASE, start=1388534400, end=1388534500)
    missing = cnyes_url(BASE, start=1388534600, end=1388534700)
    capture(queue, original, source_payload([], epoch=bounds(original)[0]))
    capture(queue, missing, source_payload([], epoch=bounds(missing)[0]))
    with queue.db:
        for table in ('cnyes_history_page_heads', 'cnyes_history_page_observations', 'cnyes_history_scope_heads'):
            queue.db.execute('DELETE FROM ' + table + ' WHERE scope_url=?', (cnyes_scope(missing),))
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (missing,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (missing,)).fetchone())
    result = backfill(queue, max_seconds=10)
    assert result['processed'] == 1
    work = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (missing,)).fetchone()
    assert work['state'] == 'retained_request_mismatch'
    assert head(queue, missing)['state'] == 'waiting_page_evidence'
    assert head(queue, original)['state'] == 'list_complete'
    after = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (missing,)).fetchone())
    assert after['due'] < before['due'] and after['error_code'] is None
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0


def test_unindexed_empty_scan_resumes_in_bounded_slices_and_preserves_failed_job(queue):
    from downloader.news_fulltext_cnyes_history import seed_unobserved_empty_scopes
    urls = [cnyes_url(BASE, start=1388534400 + i * 100, end=1388534450 + i * 100) for i in range(5)]
    with queue.db:
        for index, url in enumerate(urls):
            queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',0,1,'old-source-clock')", (cnyes_scope(url),))
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code,attempts) "
                             "VALUES (?,'cnyes_news','cnyes',9e9,?,?)",
                             (url, 'http_429' if index == 3 else None, int(index == 3)))
    failed_before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[3],)).fetchone())
    first = seed_unobserved_empty_scopes(queue, limit=2, max_seconds=10)
    assert first['scanned'] == 2 and not first['complete']
    queue.close()  # Release the owner before reopening the same test queue.
    resumed = NewsQueue(queue.root, queue.registry)
    try:
        second = seed_unobserved_empty_scopes(resumed, limit=2, max_seconds=10)
        last = seed_unobserved_empty_scopes(resumed, limit=2, max_seconds=10)
        assert second['scanned'] == 2 and not second['complete']
        assert last['scanned'] == 1 and last['complete']
        assert first['reconciled'] + second['reconciled'] + last['reconciled'] == 4
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (urls[3],)).fetchone()) == failed_before
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_scope_heads').fetchone()[0] == 4
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 0
        assert seed_unobserved_empty_scopes(resumed, limit=2, max_seconds=10)['scanned'] == 0
    finally:
        resumed.close()


def seed_unindexed_nonzero_pages(queue, *, failed_last=False):
    low = 1388534400
    urls = [cnyes_url(BASE, page, start=low, end=low + 100) for page in (1, 2, 3)]
    source = queue.registry.sources['cnyes']
    payloads = [source_payload(range(1, 31), epoch=low, page=1, total=61),
                source_payload(range(31, 61), epoch=low, page=2, total=61),
                source_payload([61], epoch=low, page=3, total=61)]
    with queue.db:
        queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',61,3,'2020-01-01T00:00:00+00:00')",
                         (cnyes_scope(urls[0]),))
        for page, (url, data) in enumerate(zip(urls, payloads), 1):
            r = response(data, url)
            info, _ = cnyes_pagination(r)
            sha = hashlib.sha256(r.body).hexdigest()
            failure = 'http_429' if failed_last and page == 3 else None
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due,etag,error_code,attempts,response_sha256,last_checked_at_utc) "
                "VALUES (?,'cnyes_news','cnyes',9e9,'old-etag',?,?,?,'2020-01-01T00:00:00+00:00')",
                (url, failure, int(failure is not None), sha))
            queue.db.execute('INSERT INTO bulk_page_evidence VALUES (?,?,?,?,?)',
                (cnyes_scope(url), page, 61, info['ids_sha256'], '2020-01-01T00:00:00+00:00'))
            if page < 3:
                target = queue.root / ('quarantine' if page == 1 else 'discovery_responses') / (sha + '.parquet')
                atomic_write_parquet(target, table_from_records([{'request_url': url, 'raw_response_body': r.body,
                    'response_sha256': sha, 'captured_at_utc': '2020-01-01T00:00:00+00:00',
                    'authorization_fingerprint': source.authorization_fingerprint, 'publish': False}]), durable=True)
                atomic_write_json(target.with_suffix('.parquet.receipt.json'), {
                    'sha256': sha256_file(target), 'response_sha256': sha, 'publish': False}, durable=True)
    return urls


def test_nonzero_local_inventory_recovers_all_available_pages_before_source_recheck(queue):
    from downloader.news_fulltext_cnyes_history import seed_unobserved_nonzero_pages
    urls = seed_unindexed_nonzero_pages(queue)
    with queue.db:
        queue.add_articles([{'url': 'https://news.cnyes.com/news/id/987654321', 'title': 'pending', 'date': None}])
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    first = seed_unobserved_nonzero_pages(queue, limit=1, max_seconds=10)
    assert first['queued'] == 1 and not first['complete']
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        for _ in range(10):
            scan = seed_unobserved_nonzero_pages(resumed, limit=1, max_seconds=10)
            if scan['complete']:
                break
        assert scan['complete']
        report = progress(resumed.db, resumed.registry.sources['cnyes'])['unobserved_nonzero_retained_scan']
        assert report['scan_state'] == 'complete' and report['queued_candidate_pages'] == 2
        assert report['checked_steps'] >= 3 and report['all_history_complete'] is False
        assert urls[0] not in json.dumps(report)
        backfill(resumed, limit=1, max_seconds=10)
        assert resumed.db.execute("SELECT count(*) FROM cnyes_history_retained_work WHERE state='pending'").fetchone()[0] == 1
        assert [dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before
        for _ in range(10):
            result = backfill(resumed, limit=8, max_seconds=10)
            if resumed.db.execute("SELECT count(*) FROM cnyes_history_retained_work WHERE state='pending'").fetchone()[0] == 0:
                break
        assert head(resumed, urls[0])['state'] == 'waiting_page_evidence'
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 2
        # A continuous body backlog cannot starve the last missing page.
        repair_incomplete_scopes(resumed, max_seconds=10)
        after = [dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
        assert after[:2] == before[:2]
        assert after[2]['due'] < before[2]['due'] and after[2]['etag'] is None
        assert resumed.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0


def test_nonzero_local_inventory_never_resets_failed_source_job(queue):
    from downloader.news_fulltext_cnyes_history import seed_unobserved_nonzero_pages
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    for _ in range(10):
        scan = seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)
        if scan['complete']:
            break
    assert scan['complete']
    for _ in range(10):
        backfill(queue, max_seconds=10)
        if queue.db.execute("SELECT count(*) FROM cnyes_history_retained_work WHERE state='pending'").fetchone()[0] == 0:
            break
    repair_incomplete_scopes(queue, max_seconds=10)
    assert head(queue, urls[0])['state'] == 'waiting_page_evidence'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


def test_nonzero_inventory_without_any_local_original_still_tracks_missing_pages(queue):
    from downloader.news_fulltext_cnyes_history import _retained_envelopes, seed_unobserved_nonzero_pages

    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    for url in urls:
        sha = queue.db.execute('SELECT response_sha256 FROM discovery WHERE url=?', (url,)).fetchone()[0]
        for path, _ in _retained_envelopes(queue.root, sha):
            path.unlink(missing_ok=True)
            path.with_suffix('.parquet.receipt.json').unlink(missing_ok=True)
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    assert not seed_unobserved_nonzero_pages(queue, limit=1, max_seconds=10)['complete']
    assert queue.db.execute('SELECT 1 FROM cnyes_history_scope_heads').fetchone() is None
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        for _ in range(10):
            result = seed_unobserved_nonzero_pages(resumed, limit=1, max_seconds=10)
            if result['complete']:
                break
        assert result['complete'] and head(resumed, urls[0])['state'] == 'waiting_page_evidence'
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 0
        after = [dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
                 for url in urls]
        assert all(after[i]['due'] < before[i]['due'] and after[i]['etag'] is None for i in (0, 1))
        assert after[2] == before[2]  # A source refusal retains its own deadline and evidence.
        result = repair_incomplete_scopes(resumed, limit=8, max_seconds=10)
        assert result['page_checks_scheduled'] == 0  # Already scheduled once, not again.
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_retained_work').fetchone()[0] == 0


def test_incomplete_scope_sweep_continues_without_a_five_minute_pause(queue):
    from downloader.news_fulltext_cnyes_history import (
        REPAIR_CURSOR, maintenance_delay, save_proof, seed_unobserved_nonzero_pages,
    )

    urls = []
    with queue.db:
        for i in range(4):
            low = 1388534400 + i * 200
            url = cnyes_url(BASE, start=low, end=low + 100)
            urls.append(url)
            r = response(source_payload([i + 1], epoch=low, total=1), url)
            info, _ = cnyes_pagination(r)
            queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',1,1,'2020-01-01T00:00:00+00:00')",
                             (cnyes_scope(url),))
            queue.db.execute("INSERT INTO bulk_page_evidence VALUES (?,1,1,?,'2020-01-01T00:00:00+00:00')",
                             (cnyes_scope(url), info['ids_sha256']))
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due,etag,error_code,attempts,response_sha256,last_checked_at_utc) "
                "VALUES (?,'cnyes_news','cnyes',9e9,'old-etag',?,?,?,'2020-01-01T00:00:00+00:00')",
                (url, 'http_429' if i == 3 else None, int(i == 3), hashlib.sha256(r.body).hexdigest()))
            save_proof(queue, cnyes_scope(url), queue.registry.sources['cnyes'],
                       'waiting_page_evidence' if i % 2 == 0 else 'inconsistent_source_snapshot', {}, set())
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    assert seed_unobserved_nonzero_pages(queue, limit=32, max_seconds=10)['complete']
    first = backfill(queue, limit=1, max_seconds=10)
    assert first['evidence_repair']['scopes_checked'] == 1
    assert not first['exhausted'] and maintenance_delay(first) == 0
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        for _ in range(10):
            result = backfill(resumed, limit=1, max_seconds=10)
            if result['exhausted']:
                break
        assert result['exhausted']
        assert resumed.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (REPAIR_CURSOR,)).fetchone() is None
        after = [dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
        assert all(after[i]['due'] < before[i]['due'] for i in range(3))
        assert after[3] == before[3]


def test_nonzero_complete_local_inventory_retires_its_fence_without_recheck(queue):
    from downloader.news_fulltext_cnyes_history import seed_unobserved_nonzero_pages
    urls = seed_unindexed_nonzero_pages(queue)
    source = queue.registry.sources['cnyes']
    r = response(source_payload([61], epoch=1388534400, page=3, total=61), urls[2])
    sha = hashlib.sha256(r.body).hexdigest()
    path = queue.root / 'quarantine' / (sha + '.parquet')
    atomic_write_parquet(path, table_from_records([{'request_url': urls[2], 'raw_response_body': r.body,
        'response_sha256': sha, 'captured_at_utc': '2020-01-01T00:00:00+00:00',
        'authorization_fingerprint': source.authorization_fingerprint, 'publish': False}]), durable=True)
    atomic_write_json(path.with_suffix('.parquet.receipt.json'), {
        'sha256': sha256_file(path), 'response_sha256': sha, 'publish': False}, durable=True)
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    assert seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)['complete']
    backfill(queue, limit=8, max_seconds=10)
    assert head(queue, urls[0])['state'] == 'list_complete'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_local_inventory_scopes').fetchone()[0] == 0
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 3
    assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before


def seed_accepted_batch_for_history_page(queue, url, data, *, original_url=None, authorization=None,
                                         batch_tag='accepted'):
    r = response(data, url)
    sha = hashlib.sha256(r.body).hexdigest()
    relative = f'bodies/20261028/20261028T120000000000Z-{batch_tag}.parquet'
    receipt_relative = f'receipts/20261028T120000000000Z-{batch_tag}.json'
    target = queue.root / relative
    row = {'provider': 'cnyes', 'source_request_url': original_url or url,
        'raw_response_body': r.body, 'response_sha256': sha, 'captured_at_utc': '2020-01-01T00:00:00+00:00',
        'authorization_fingerprint': authorization or queue.registry.sources['cnyes'].authorization_fingerprint,
        'publish': False}
    atomic_write_parquet(target, table_from_records([row]), durable=True)
    atomic_write_json(queue.root / receipt_relative, {'contract_version': 1, 'parquet_path': relative,
        'sha256': sha256_file(target), 'rows': 1, 'publish': False}, durable=True)
    with queue.db:
        queue.db.execute('INSERT INTO accepted_batches VALUES (?,?)', (relative, receipt_relative))
    return relative, receipt_relative


def test_accepted_batch_index_recovers_exact_missing_history_page_without_copy_or_request(queue):
    from downloader.news_fulltext_cnyes_history import seed_accepted_body_batch_candidates, seed_unobserved_nonzero_pages
    urls = seed_unindexed_nonzero_pages(queue)
    relative, receipt = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    first = seed_accepted_body_batch_candidates(queue, limit=1, max_seconds=10)
    assert first['indexed'] == 1 and not first['complete']
    report = progress(queue.db, queue.registry.sources['cnyes'])['accepted_body_batch_scan']
    assert report['scan_state'] == 'scanning' and report['scanned_batches'] == report['indexed_candidates'] == 1
    assert relative not in json.dumps(report) and report['all_history_complete'] is False
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        for _ in range(10):
            scan = seed_accepted_body_batch_candidates(resumed, limit=1, max_seconds=10)
            if scan['complete']:
                break
        assert scan['complete']
        assert seed_unobserved_nonzero_pages(resumed, limit=8, max_seconds=10)['queued'] == 3
        backfill(resumed, limit=8, max_seconds=10)
        assert head(resumed, urls[0])['state'] == 'list_complete'
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 3
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_local_inventory_scopes').fetchone()[0] == 0
        observation = json.loads(resumed.db.execute('SELECT o.observation_json FROM cnyes_history_page_heads h '
            'JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
            'WHERE h.scope_url=? AND h.page=3', (cnyes_scope(urls[0]),)).fetchone()[0])
        assert observation['source_observed_at_utc'] == '2020-01-01T00:00:00+00:00'
        assert observation['artifact'] == {'kind': 'accepted_body_batch', 'parquet_path': relative, 'receipt_path': receipt}
        assert [dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before
        assert resumed.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1


def test_existing_scope_waits_for_unscanned_body_batch_before_source_recheck(queue):
    from downloader.news_fulltext_cnyes_history import reconcile_scope, seed_accepted_body_batch_candidates
    urls = seed_unindexed_nonzero_pages(queue)
    seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    with queue.db:
        reconcile_scope(queue, cnyes_scope(urls[0]), queue.registry.sources['cnyes'])
    assert head(queue, urls[0])['state'] == 'waiting_page_evidence'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before
    assert seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['complete']
    for _ in range(4):
        backfill(queue, limit=8, max_seconds=10)
        if head(queue, urls[0])['state'] == 'list_complete':
            break
    assert head(queue, urls[0])['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


def test_pending_retained_work_waits_when_batch_index_temporarily_unreadable(queue, monkeypatch):
    import downloader.news_fulltext_cnyes_history as history
    urls = seed_unindexed_nonzero_pages(queue)
    seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    page = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone()
    job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
            'VALUES (?,?,?,?,?,?,?)', (urls[2], cnyes_scope(urls[0]), 3, job['response_sha256'],
                                       page['total'], page['ids_sha256'], page['observed_at_utc']))
    original = history._accepted_batch_metadata_pairs
    def unavailable(*args, **kwargs):
        raise OSError('temporary local disk read failure')
    monkeypatch.setattr(history, '_accepted_batch_metadata_pairs', unavailable)
    result = backfill(queue, limit=8, max_seconds=10)
    assert result['accepted_body_batch_scan']['blocked'] == 'local_retry_wait'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == job
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                            (urls[2],)).fetchone()[0] in ('pending', 'local_retry')
    monkeypatch.setattr(history, '_accepted_batch_metadata_pairs', original)
    marker = history.accepted_batch_scan_key(queue.registry.sources['cnyes'])
    with queue.db:
        checkpoint = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0])
        checkpoint['retry_after'] = 0
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps(checkpoint), marker))
    for _ in range(5):
        backfill(queue, limit=8, max_seconds=10)
        if queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone():
            break
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == job


@pytest.mark.parametrize('previous', ['retained_unavailable', 'retained_invalid', 'retained_request_mismatch'])
def test_new_batch_candidate_revisits_old_failed_local_work_without_losing_outcome(queue, previous):
    from downloader.news_fulltext_cnyes_history import seed_accepted_body_batch_candidates
    urls = seed_unindexed_nonzero_pages(queue)
    seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    page = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone()
    job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    old_outcome = json.dumps({'error': 'preserve_original_failed_candidate'})
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,'
            'source_checked_at_utc,state,outcome_json) VALUES (?,?,?,?,?,?,?,?,?)',
            (urls[2], cnyes_scope(urls[0]), 3, job['response_sha256'], page['total'],
             page['ids_sha256'], page['observed_at_utc'], previous, old_outcome))
    assert seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['complete']
    work = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (urls[2],)).fetchone()
    assert work['state'] == 'pending'
    audit = queue.db.execute('SELECT original_state,original_outcome_json,resolution_state,evidence_kind '
        'FROM cnyes_history_retained_outcome_resolutions WHERE request_url=?', (urls[2],)).fetchone()
    assert tuple(audit) == (previous, old_outcome, 'pending', 'new_accepted_body_batch_candidate')
    for _ in range(5):
        backfill(queue, limit=8, max_seconds=10)
        if queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone():
            break
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == job


def test_absent_batch_after_inventory_allows_normal_source_recheck(queue):
    from downloader.news_fulltext_cnyes_history import reconcile_scope, seed_accepted_body_batch_candidates
    urls = seed_unindexed_nonzero_pages(queue)
    # The only accepted batch belongs to page 2, whose content is already
    # available in a separate original envelope; page 3 remains unproven.
    seed_accepted_batch_for_history_page(queue, urls[1],
        source_payload(range(31, 61), epoch=1388534400, page=2, total=61))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    with queue.db:
        reconcile_scope(queue, cnyes_scope(urls[0]), queue.registry.sources['cnyes'])
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before
    assert seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['complete']
    with queue.db:
        reconcile_scope(queue, cnyes_scope(urls[0]), queue.registry.sources['cnyes'])
    after = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    assert after['due'] < before['due'] and after['etag'] is None
    assert after['error_code'] is None and after['attempts'] == 0
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone() is None


def test_live_accepted_cnyes_raw_registers_reference_in_same_commit(queue):
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    r = capture(queue, url, source_payload([1], epoch=1388534400))
    row = queue.db.execute('SELECT * FROM cnyes_history_accepted_batch_candidates WHERE request_url=?', (url,)).fetchone()
    assert row and row['response_sha256'] == hashlib.sha256(r.body).hexdigest()
    assert queue.db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (row['parquet_path'],)).fetchone()[0] == row['receipt_path']
    assert head(queue, url)['state'] == 'list_complete'


@pytest.mark.parametrize('history_absent', [False, True])
def test_startup_recovers_orphan_cnyes_batch_before_legacy_index_exists(queue, history_absent):
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    r = capture(queue, url, source_payload([1], epoch=1388534400))
    accepted = tuple(queue.db.execute('SELECT path,receipt_path FROM accepted_batches').fetchone())
    # Simulate an old schema and a crash after durable files but before the
    # SQLite batch commit. Startup must replay the receipt and index its raw
    # request in the same commit, even on the first boot after an upgrade.
    with queue.db:
        queue.db.execute('DELETE FROM accepted_batches')
        queue.db.execute('DROP TABLE cnyes_history_accepted_batch_candidates')
        if history_absent:
            queue.db.execute('DROP TABLE cnyes_history_retained_work')
            queue.db.execute('DROP TABLE cnyes_history_retained_outcome_resolutions')
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        assert tuple(resumed.db.execute('SELECT path,receipt_path FROM accepted_batches').fetchone()) == accepted
        candidate = resumed.db.execute('SELECT * FROM cnyes_history_accepted_batch_candidates WHERE request_url=?',
                                       (url,)).fetchone()
        assert candidate and candidate['response_sha256'] == hashlib.sha256(r.body).hexdigest()
        assert (candidate['parquet_path'], candidate['receipt_path']) == accepted


def test_startup_orphan_batch_revisits_old_failed_work_in_same_recovery_commit(queue):
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([1], epoch=1388534400))
    scope = cnyes_scope(url)
    legacy = dict(queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=1', (scope,)).fetchone())
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (url,))
        for table in ('cnyes_history_page_heads', 'cnyes_history_page_observations', 'cnyes_history_scope_heads'):
            queue.db.execute('DELETE FROM ' + table + ' WHERE scope_url=?', (scope,))
        queue.db.execute('DELETE FROM cnyes_history_accepted_batch_candidates')
        queue.db.execute('DELETE FROM accepted_batches')
        job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
        old_outcome = json.dumps({'error': 'previous_missing_local_original'})
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,'
            'source_checked_at_utc,state,outcome_json) VALUES (?,?,?,?,?,?,?,?,?)',
            (url, scope, 1, job['response_sha256'], legacy['total'], legacy['ids_sha256'],
             legacy['observed_at_utc'], 'retained_unavailable', old_outcome))
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        assert resumed.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                                  (url,)).fetchone()[0] == 'pending'
        event = resumed.db.execute('SELECT original_state,original_outcome_json,resolution_state,evidence_kind '
            'FROM cnyes_history_retained_outcome_resolutions WHERE request_url=?', (url,)).fetchone()
        assert tuple(event) == ('retained_unavailable', old_outcome, 'pending', 'new_accepted_body_batch_candidate')
        for _ in range(5):
            backfill(resumed, limit=8, max_seconds=10)
            if resumed.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=1',
                                  (scope,)).fetchone():
                break
        assert resumed.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=1',
                                  (scope,)).fetchone()
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == job


def test_failed_candidate_wake_rolls_back_batch_commit_then_recovers_receipt(queue, monkeypatch):
    import downloader.news_fulltext_cnyes_history as history
    urls = seed_unindexed_nonzero_pages(queue)
    page = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone()
    job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    old_outcome = json.dumps({'error': 'previous_missing_local_original'})
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,'
            'source_checked_at_utc,state,outcome_json) VALUES (?,?,?,?,?,?,?,?,?)',
            (urls[2], cnyes_scope(urls[0]), 3, job['response_sha256'], page['total'], page['ids_sha256'],
             page['observed_at_utc'], 'retained_unavailable', old_outcome))
    def interrupted(*args, **kwargs):
        raise RuntimeError('simulated candidate wake interruption')
    with monkeypatch.context() as patch:
        patch.setattr(history, '_save_retained_resolution', interrupted)
        with pytest.raises(RuntimeError, match='candidate wake interruption'):
            capture(queue, urls[2], source_payload([61], epoch=1388534400, page=3, total=61))
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_accepted_batch_candidates').fetchone()[0] == 0
    assert queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work WHERE request_url=?',
                            (urls[2],)).fetchone()[:] == ('retained_unavailable', old_outcome)
    queue.recover_acceptance()
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                            (urls[2],)).fetchone()[0] == 'pending'
    assert queue.db.execute('SELECT original_outcome_json FROM cnyes_history_retained_outcome_resolutions '
                            'WHERE request_url=?', (urls[2],)).fetchone()[0] == old_outcome
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == job


@pytest.mark.parametrize('invalid', ['receipt', 'authorization', 'capture_clock'])
def test_accepted_batch_candidate_never_creates_proof_when_validation_fails(queue, invalid):
    import pyarrow.parquet as pq
    from downloader.news_fulltext_cnyes_history import seed_accepted_body_batch_candidates, seed_unobserved_nonzero_pages
    urls = seed_unindexed_nonzero_pages(queue)
    relative, receipt_relative = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61),
        authorization='different-authority' if invalid == 'authorization' else None)
    if invalid == 'capture_clock':
        path = queue.root / relative
        record = pq.read_table(path).to_pylist()[0]
        record['captured_at_utc'] = '2020-01-01T00:00:00'
        atomic_write_parquet(path, table_from_records([record]), durable=True)
        receipt = json.loads((queue.root / receipt_relative).read_text())
        receipt['sha256'] = sha256_file(path)
        atomic_write_json(queue.root / receipt_relative, receipt, durable=True)
    if invalid == 'receipt':
        receipt = json.loads((queue.root / receipt_relative).read_text())
        receipt['sha256'] = '0' * 64
        atomic_write_json(queue.root / receipt_relative, receipt, durable=True)
    assert seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['indexed'] == 1
    assert seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)['queued'] == 3
    backfill(queue, limit=8, max_seconds=10)
    outcome = queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work WHERE request_url=?',
                               (urls[2],)).fetchone()
    assert outcome['state'] == 'retained_invalid'
    assert json.loads(outcome['outcome_json'])['error'] == {
        'receipt': 'invalid_accepted_batch_receipt', 'authorization': 'retained_original_authorization_mismatch',
        'capture_clock': 'retained_original_capture_clock_invalid'}[invalid]
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone() is None
    assert head(queue, urls[0])['state'] == 'waiting_page_evidence'


@pytest.mark.parametrize('invalid', ['request', 'raw_bytes', 'restricted', 'page_identity', 'provider'])
def test_accepted_batch_validates_exact_source_identity_and_public_page(queue, invalid):
    import pyarrow.parquet as pq
    from downloader.news_fulltext_cnyes_history import seed_accepted_body_batch_candidates, seed_unobserved_nonzero_pages
    urls = seed_unindexed_nonzero_pages(queue)
    data = source_payload([62] if invalid == 'page_identity' else [61], epoch=1388534400, page=3, total=61)
    if invalid == 'restricted':
        data['items']['data'][0]['payment'] = 1
    relative, receipt_relative = seed_accepted_batch_for_history_page(queue, urls[2], data)
    path = queue.root / relative
    row = pq.read_table(path).to_pylist()[0]
    if invalid == 'request':
        row['source_request_url'] = urls[1]
    elif invalid == 'raw_bytes':
        row['raw_response_body'] += b'changed'
    elif invalid == 'provider':
        row['provider'] = 'different-provider'
    atomic_write_parquet(path, table_from_records([row]), durable=True)
    receipt = json.loads((queue.root / receipt_relative).read_text())
    receipt['sha256'] = sha256_file(path)
    atomic_write_json(queue.root / receipt_relative, receipt, durable=True)
    with queue.db:
        queue.db.execute('UPDATE discovery SET response_sha256=? WHERE url=?', (row['response_sha256'], urls[2]))
    seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
    if invalid == 'request':  # A stale or poisoned candidate is still never proof.
        with queue.db:
            queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_candidates VALUES (?,?,?,?)',
                             (urls[2], row['response_sha256'], relative, receipt_relative))
    assert seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)['queued'] == 3
    backfill(queue, limit=8, max_seconds=10)
    outcome = queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work WHERE request_url=?',
                               (urls[2],)).fetchone()
    assert outcome['state'] == 'retained_invalid'
    assert json.loads(outcome['outcome_json'])['error'] == {
        'request': 'accepted_batch_matching_original_missing', 'raw_bytes': 'retained_original_bytes_or_private_scope_invalid',
        'restricted': 'restricted_retained_envelope_not_reused', 'page_identity': 'retained_source_page_identity_mismatch',
        'provider': 'accepted_batch_matching_original_missing'}[invalid]
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone() is None


def test_accepted_batch_scan_local_failure_retries_without_advancing_or_publisher_changes(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history, news_fulltext_queue as queue_module
    urls = seed_unindexed_nonzero_pages(queue)
    relative, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    original = queue_module.replay_source_path
    def unavailable(root, path):
        if path == relative:
            raise OSError(5, 'temporary local I/O')
        return original(root, path)
    with monkeypatch.context() as patch:
        patch.setattr(queue_module, 'replay_source_path', unavailable)
        scan = history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
        assert scan['blocked'] == 'local_retry_wait' and scan['checked'] == 0
        assert history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['blocked'] == 'local_retry_wait'
    assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before
    marker = history.accepted_batch_scan_key(queue.registry.sources['cnyes'])
    state = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0])
    assert state['path'] == '' and state['attempts'] == 1
    state['retry_after'] = time.time() + 60
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps(state), marker))
    waiting = backfill(queue, limit=8, max_seconds=10)
    assert waiting['exhausted'] and waiting['accepted_body_batch_scan']['blocked'] == 'local_retry_wait'
    assert waiting['unobserved_nonzero_page_scan']['blocked'] == 'waiting_accepted_batch_scan'
    assert history.maintenance_delay(waiting) >= 1
    state['retry_after'] = 0
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps(state), marker))
    assert history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['indexed'] == 1
    assert [r[0] for r in queue.db.execute('SELECT state FROM cnyes_history_accepted_batch_outcomes ORDER BY observed_at_utc')] == ['local_retry', 'recovered']
    assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before


def test_accepted_batch_scan_continues_past_local_io_and_recovers_after_restart(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history, news_fulltext_queue as queue_module
    urls = seed_unindexed_nonzero_pages(queue)
    first, _ = seed_accepted_batch_for_history_page(queue, urls[1],
        source_payload(range(31, 61), epoch=1388534400, page=2, total=61), batch_tag='a')
    second, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61), batch_tag='b')
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    original = queue_module.replay_source_path
    def unavailable(root, path):
        if path == first:
            raise OSError(5, 'temporary local I/O')
        return original(root, path)
    with monkeypatch.context() as patch:
        patch.setattr(queue_module, 'replay_source_path', unavailable)
        scan = history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
        assert scan['indexed'] == 1 and not scan['complete']
        assert queue.db.execute('SELECT parquet_path FROM cnyes_history_accepted_batch_candidates').fetchone()[0] == second
        assert not history.accepted_batch_inventory_complete(queue, queue.registry.sources['cnyes'])
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        assert [dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before
        assert not history.accepted_batch_inventory_complete(resumed, resumed.registry.sources['cnyes'])
        with resumed.db:
            resumed.db.execute('UPDATE cnyes_history_accepted_batch_local_retries SET not_before_epoch=0')
        scan = history.seed_accepted_body_batch_candidates(resumed, limit=8, max_seconds=10)
        assert scan['indexed'] == 1 and scan['complete']
        assert history.accepted_batch_inventory_complete(resumed, resumed.registry.sources['cnyes'])
        assert {r[0] for r in resumed.db.execute('SELECT parquet_path FROM cnyes_history_accepted_batch_candidates')} == {first, second}
        assert [r[0] for r in resumed.db.execute('SELECT state FROM cnyes_history_accepted_batch_outcomes ORDER BY observed_at_utc')] == ['local_retry', 'recovered']
        assert [dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before


def test_accepted_batch_scan_records_missing_file_without_forging_candidate(queue):
    from downloader.news_fulltext_cnyes_history import seed_accepted_body_batch_candidates
    with queue.db:
        queue.db.execute('INSERT INTO accepted_batches VALUES (?,?)', ('bodies/missing.parquet', 'receipts/missing.json'))
    scan = seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
    assert scan['complete'] and scan['indexed'] == 0
    outcome = queue.db.execute('SELECT state,reason FROM cnyes_history_accepted_batch_outcomes').fetchone()
    assert tuple(outcome) == ('unavailable', 'local_batch_file_missing')


def test_accepted_batch_index_falls_back_for_mixed_request_metadata(queue):
    import pyarrow.parquet as pq
    from downloader.news_fulltext_cnyes_history import seed_accepted_body_batch_candidates
    urls = seed_unindexed_nonzero_pages(queue)
    relative, receipt_relative = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    path = queue.root / relative
    original = pq.read_table(path).to_pylist()[0]
    other = {**original, 'source_request_url': urls[0], 'response_sha256': 'f' * 64, 'raw_response_body': None}
    atomic_write_parquet(path, table_from_records([original, other]), durable=True)
    receipt = json.loads((queue.root / receipt_relative).read_text())
    receipt.update(sha256=sha256_file(path), rows=2)
    atomic_write_json(queue.root / receipt_relative, receipt, durable=True)
    result = seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
    assert result['complete'] and result['indexed'] == 1
    assert queue.db.execute('SELECT request_url FROM cnyes_history_accepted_batch_candidates').fetchone()[0] == urls[2]


def test_nonzero_inventory_preserves_request_mismatch_and_rechecks_only_after_local_work(queue):
    import pyarrow.parquet as pq
    from downloader.news_fulltext_cnyes_history import seed_unobserved_nonzero_pages
    urls = seed_unindexed_nonzero_pages(queue)
    sha = queue.db.execute('SELECT response_sha256 FROM discovery WHERE url=?', (urls[1],)).fetchone()[0]
    path = queue.root / 'discovery_responses' / (sha + '.parquet')
    original = pq.read_table(path).to_pylist()[0]
    original['request_url'] = cnyes_url(BASE, 2, start=1388534300, end=1388534400)
    atomic_write_parquet(path, table_from_records([original]), durable=True)
    atomic_write_json(path.with_suffix('.parquet.receipt.json'), {
        'sha256': sha256_file(path), 'response_sha256': sha, 'publish': False}, durable=True)
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    assert seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)['complete']
    backfill(queue, limit=1, max_seconds=10)
    assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before
    backfill(queue, limit=8, max_seconds=10)
    outcome = queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work WHERE request_url=?',
                               (urls[1],)).fetchone()
    assert outcome['state'] == 'retained_request_mismatch'
    assert json.loads(outcome['outcome_json'])['error'] == 'retained_original_request_differs_not_relabelled'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 1
    assert head(queue, urls[0])['state'] == 'waiting_page_evidence'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[0],)).fetchone()) == before[0]
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0


def test_nonzero_inventory_invalid_cursor_is_preserved_and_restarted_without_duplicate_work(queue):
    from downloader.news_fulltext_cnyes_history import LEGACY_NONZERO_SCAN_PREFIX, seed_unobserved_nonzero_pages
    urls = seed_unindexed_nonzero_pages(queue)
    assert seed_unobserved_nonzero_pages(queue, limit=1, max_seconds=10)['queued'] == 1
    key = queue.db.execute('SELECT key FROM queue_settings WHERE key LIKE ?',
                           (LEGACY_NONZERO_SCAN_PREFIX + '%',)).fetchone()[0]
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', ('invalid-cursor-bytes', key))
    assert seed_unobserved_nonzero_pages(queue, limit=1, max_seconds=10)['restarted_invalid_checkpoint']
    for _ in range(10):
        scan = seed_unobserved_nonzero_pages(queue, limit=1, max_seconds=10)
        if scan['complete']:
            break
    assert scan['complete']
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key LIKE ?',
        (LEGACY_NONZERO_SCAN_PREFIX + 'invalid:%',)).fetchone()[0] == 'invalid-cursor-bytes'
    assert [row[0] for row in queue.db.execute('SELECT request_url FROM cnyes_history_retained_work ORDER BY request_url')] == sorted(urls[:2])
    assert queue.db.execute('SELECT scan_state FROM cnyes_history_local_inventory_scopes').fetchone()[0] == 'ready'


def test_nonzero_inventory_local_failure_does_not_wake_missing_publisher_pages(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history
    urls = seed_unindexed_nonzero_pages(queue)
    assert history.seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)['complete']
    sha = queue.db.execute('SELECT response_sha256 FROM discovery WHERE url=?', (urls[0],)).fetchone()[0]
    path = queue.root / 'quarantine' / (sha + '.parquet')
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    original = history.pq.read_table
    def unavailable(candidate, *args, **kwargs):
        if candidate == path:
            raise OSError(5, 'temporary local read failure')
        return original(candidate, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(history.pq, 'read_table', unavailable)
        assert backfill(queue, limit=8, max_seconds=10)['local_io_deferrals'] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (urls[0],)).fetchone()[0] == 'local_retry'
    assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 WHERE request_url=?', (urls[0],))
    backfill(queue, limit=8, max_seconds=10)
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (urls[0],)).fetchone()[0] == 'restored'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 2


def test_nonzero_inventory_changed_contract_releases_old_fence_without_erasing_identity(queue):
    from dataclasses import replace
    from downloader.news_fulltext_cnyes_history import seed_unobserved_nonzero_pages
    urls = seed_unindexed_nonzero_pages(queue)
    assert seed_unobserved_nonzero_pages(queue, limit=1, max_seconds=10)['queued'] == 1
    old = dict(queue.db.execute('SELECT * FROM cnyes_history_local_inventory_scopes').fetchone())
    queue.registry.sources['cnyes'] = replace(queue.registry.sources['cnyes'], authorization_fingerprint='new-authority')
    result = seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)
    current = dict(queue.db.execute('SELECT * FROM cnyes_history_local_inventory_scopes').fetchone())
    assert result['stale_fences_reconciled'] == 1
    assert current == {**old, 'scan_state': 'ready'}
    assert head(queue, urls[0])['state'] == 'waiting_page_evidence'
    backfill(queue, limit=8, max_seconds=10)
    outcome = queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work WHERE request_url=?',
                               (urls[0],)).fetchone()
    assert outcome['state'] == 'retained_invalid'
    assert json.loads(outcome['outcome_json'])['error'] == 'retained_original_authorization_mismatch'


def test_owner_continues_bounded_local_history_without_any_http_work(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as main, news_fulltext_cnyes_history as history
    from test_news_fulltext import http_fixture
    cfg['adaptive_rate'] = {'enabled': False}
    cfg['cached_cnyes_metadata_replay_enabled'] = False
    http, session = http_fixture(cfg, tmp_path, {})
    q = NewsQueue(tmp_path, http.registry)
    calls = []
    def local_slice(*args, **kwargs):
        calls.append(len(calls))
        return {'processed': 1, 'exhausted': len(calls) >= 3, 'next_local_retry_epoch': None,
                'unobserved_nonzero_page_scan': {'complete': len(calls) >= 3, 'checked': 1}}
    monkeypatch.setattr(history, 'backfill', local_slice)
    monkeypatch.setattr(main, 'seed_discovery', lambda q: None)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda q, config: False)
    try:
        result = main.run_cycle(q, http, cfg, max_articles=0, max_discovery_requests=0,
                                continuous_budgets=True, max_run_seconds=2)
        assert len(calls) == 3 and not session.calls
        assert result['cycle']['http_requests'].get('cnyes', 0) == 0
    finally:
        q.close()
        http.close()


def test_owner_continues_scope_repair_slices_without_any_http_work(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as main, news_fulltext_cnyes_history as history
    from test_news_fulltext import http_fixture
    cfg['adaptive_rate'] = {'enabled': False}
    cfg['cached_cnyes_metadata_replay_enabled'] = False
    http, session = http_fixture(cfg, tmp_path, {})
    q = NewsQueue(tmp_path, http.registry)
    calls = []

    def local_slice(*args, **kwargs):
        calls.append(1)
        return {'processed': 0, 'exhausted': len(calls) >= 3, 'next_local_retry_epoch': None,
                'repair_sweep_pending': len(calls) < 3,
                'evidence_repair': {'scopes_checked': int(len(calls) < 3)}}

    monkeypatch.setattr(history, 'backfill', local_slice)
    monkeypatch.setattr(main, 'seed_discovery', lambda q: None)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda q, config: False)
    try:
        result = main.run_cycle(q, http, cfg, max_articles=0, max_discovery_requests=0,
                                continuous_budgets=True, max_run_seconds=2)
        assert len(calls) == 3 and not session.calls
        assert result['cycle']['http_requests'].get('cnyes', 0) == 0
    finally:
        q.close()
        http.close()


def test_owner_services_other_provider_while_history_inventory_continues(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as main, news_fulltext_cnyes_history as history
    from test_news_fulltext import FakeResponse, article_html, config as publisher_config, http_fixture
    config = publisher_config.__wrapped__()
    config['providers'].append(deepcopy(cfg['providers'][0]))
    config['workers'] = 2
    config['cached_cnyes_metadata_replay_enabled'] = False
    events = []
    class VisibleResponse(FakeResponse):
        def iter_content(self, **kwargs):
            time.sleep(.02)
            events.append('publisher_response')
            yield self.body
    url = 'https://publisher.test/story/1'
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: VisibleResponse(article_html())})
    q = NewsQueue(tmp_path, http.registry)
    with q.db:
        q.add_articles([{'url': url}])
    calls = []
    def local_slice(*args, **kwargs):
        calls.append(len(calls))
        time.sleep(.01)
        events.append('local_slice')
        return {'processed': 1, 'exhausted': len(calls) >= 20, 'next_local_retry_epoch': None,
                'unobserved_nonzero_page_scan': {'complete': len(calls) >= 20, 'checked': 1}}
    monkeypatch.setattr(history, 'backfill', local_slice)
    monkeypatch.setattr(main, 'seed_discovery', lambda q: None)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda q, config: False)
    try:
        result = main.run_cycle(q, http, config, max_articles=1, max_discovery_requests=0,
                                continuous_budgets=True, max_run_seconds=3)
        assert len(calls) == 20 and result['states']['complete'] == 1
        assert events.index('publisher_response') < max(i for i, value in enumerate(events) if value == 'local_slice')
        assert [call[0] for call in session.calls] == ['https://publisher.test/robots.txt', url]
    finally:
        q.close()
        http.close()


def test_owner_stops_local_history_immediately_when_stop_requested(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as main, news_fulltext_cnyes_history as history
    from test_news_fulltext import http_fixture
    cfg['adaptive_rate'] = {'enabled': False}
    cfg['cached_cnyes_metadata_replay_enabled'] = False
    http, session = http_fixture(cfg, tmp_path, {})
    q = NewsQueue(tmp_path, http.registry)
    calls = []
    def local_slice(*args, **kwargs):
        calls.append(1)
        http.stop.set()
        return {'processed': 1, 'exhausted': False, 'next_local_retry_epoch': None,
                'unobserved_nonzero_page_scan': {'complete': False, 'checked': 1}}
    monkeypatch.setattr(history, 'backfill', local_slice)
    monkeypatch.setattr(main, 'seed_discovery', lambda q: None)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda q, config: False)
    try:
        result = main.run_cycle(q, http, cfg, max_articles=0, max_discovery_requests=0,
                                continuous_budgets=True, max_run_seconds=2)
        assert calls == [1] and not session.calls and result['activity'] == 'stopped'
    finally:
        q.close()
        http.close()


@pytest.mark.parametrize('change', ['source_bulk', 'authorization', 'metadata_version'])
def test_same_raw_bytes_new_contract_preserve_old_observation_and_current_head(queue, monkeypatch, change):
    from dataclasses import replace
    import downloader.news_fulltext_cnyes_history as history
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    r = capture(queue, url, source_payload([1], epoch=low))
    original = dict(queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone())
    source = queue.registry.sources['cnyes']
    if change == 'source_bulk':
        source = replace(source, cnyes_bulk_data_fingerprint='changed-source-bulk-contract')
    elif change == 'authorization':
        source = replace(source, authorization_fingerprint='changed-authorization')
    else:
        monkeypatch.setattr(history, 'CONTRACT_VERSION', CONTRACT_VERSION + 1)
    info, posts = cnyes_pagination(r)
    with queue.db:
        record_page(queue, r, info, posts, source, {'kind': 'actual_response_reference'})
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 2
    assert dict(queue.db.execute('SELECT * FROM cnyes_history_page_observations WHERE observation_sha256=?',
                                (original['observation_sha256'],)).fetchone()) == original
    current = head(queue, url)
    assert current['state'] == 'list_complete' and current['source_bulk_fingerprint'] == source.cnyes_bulk_data_fingerprint
    assert current['authorization_fingerprint'] == source.authorization_fingerprint
    assert current['evidence_contract_version'] == history.CONTRACT_VERSION


def legacy_missing_page(queue, *, failure=None):
    low = 1388534400
    first = cnyes_url(BASE, start=low, end=low + 100)
    second = cnyes_url(BASE, 2, start=low, end=low + 100)
    capture(queue, first, source_payload(range(1, 31), epoch=low, total=31))
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=9e9,etag=?,last_modified=?,error_code=?,attempts=?,'
            'response_sha256=?,last_checked_at_utc=? WHERE url=?',
            ('old-etag', 'old-last-modified', failure, int(failure is not None),
             'old-response', '2020-01-01T00:00:00+00:00', second))
    return first, second


def test_repair_scan_reports_a_fetched_scope_when_slice_expires_before_processing(queue, monkeypatch):
    from types import SimpleNamespace
    from downloader import news_fulltext_cnyes_history as history

    _, second = legacy_missing_page(queue)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    monotonic = iter((0.0, 2.0))
    with monkeypatch.context() as patch:
        patch.setattr(history, 'time', SimpleNamespace(monotonic=lambda: next(monotonic), time=time.time))
        result = history.repair_incomplete_scopes(queue, max_seconds=1)
    assert result['scopes_checked'] == 0 and result['unfinished_slice']
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before


def test_history_repair_budget_exhaustion_keeps_the_owner_pending(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    _, second = legacy_missing_page(queue)
    # Finish unrelated one-time scans so this test isolates the repair budget.
    for seed in (history.reconcile_legacy_request_mismatches, history.reconcile_legacy_unavailable,
                 history.seed_unobserved_empty_scopes, history.seed_accepted_body_batch_candidates,
                 history.seed_unobserved_nonzero_pages, history.seed_overflow_retained):
        assert seed(queue, limit=128, max_seconds=10)['complete']
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    original = history.seed_overflow_retained

    def slow_local_slice(*args, **kwargs):
        time.sleep(.02)
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(history, 'seed_overflow_retained', slow_local_slice)
        result = history.backfill(queue, max_seconds=.005)
    assert result['evidence_repair']['scopes_checked'] == 0
    assert result['repair_sweep_pending'] and not result['exhausted']
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before
    result = history.backfill(queue, max_seconds=10)
    assert result['evidence_repair']['page_checks_scheduled'] == 1
    assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (second,)).fetchone()[0] < before['due']


def test_history_repair_budget_exhaustion_does_not_spin_when_no_scopes_remain(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    for seed in (history.reconcile_legacy_request_mismatches, history.reconcile_legacy_unavailable,
                 history.seed_unobserved_empty_scopes, history.seed_accepted_body_batch_candidates,
                 history.seed_unobserved_nonzero_pages, history.seed_overflow_retained):
        assert seed(queue, limit=128, max_seconds=10)['complete']
    original = history.seed_overflow_retained

    def slow_local_slice(*args, **kwargs):
        time.sleep(.02)
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(history, 'seed_overflow_retained', slow_local_slice)
        result = history.backfill(queue, max_seconds=.005)
    assert result['exhausted'] and not result['repair_sweep_pending']
    assert history.maintenance_delay(result) == 300


def test_restart_repair_only_missing_successful_page_and_no_repeat_or_body_rewrite(queue):
    first, second = legacy_missing_page(queue)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (first,)).fetchone())
    bodies = [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    proof = head(queue, first)['proof_sha256']
    result = repair_incomplete_scopes(queue, max_seconds=10)
    assert result == {'scopes_checked': 1, 'retained_pages_queued': 0, 'page_checks_scheduled': 1}
    job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    assert job['due'] < 9e9 and job['etag'] is None and job['last_modified'] is None
    assert head(queue, first)['proof_sha256'] == proof
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (first,)).fetchone()) == before
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')] == bodies
    assert repair_incomplete_scopes(queue, max_seconds=10)['page_checks_scheduled'] == 0
    with queue.db:  # Same failed-to-finish evidence after an unchanged owner restart.
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (second,))
    assert repair_incomplete_scopes(queue, max_seconds=10)['page_checks_scheduled'] == 0
    capture(queue, second, source_payload([31], epoch=bounds(first)[0], page=2, total=31))
    assert head(queue, first)['state'] == 'list_complete'
    assert repair_incomplete_scopes(queue, max_seconds=10)['scopes_checked'] == 0
    assert queue.db.execute('SELECT 1 FROM cnyes_history_scope_proofs WHERE proof_sha256=?', (proof,)).fetchone()


@pytest.mark.parametrize('state', ['pending', 'overflow_pending', 'local_retry'])
def test_incomplete_repair_waits_for_local_page_work_before_source_recheck(queue, state):
    first, second = legacy_missing_page(queue)
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc,state) '
            'VALUES (?,?,2,?,31,?,\'old-clock\',?)',
            (second, cnyes_scope(first), 'a' * 64, 'b' * 64, state))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    result = repair_incomplete_scopes(queue, max_seconds=10)
    assert result['scopes_checked'] == 1 and result['page_checks_scheduled'] == 0
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before


def test_incomplete_repair_preserves_unreadable_candidate_for_local_retry(queue, monkeypatch):
    from pathlib import Path
    first, second = legacy_missing_page(queue)
    sha = 'a' * 64
    target = queue.root / 'quarantine' / (sha + '.parquet')
    with queue.db:
        queue.db.execute('INSERT INTO bulk_page_evidence VALUES (?,2,31,?,\'old-clock\')',
                         (cnyes_scope(first), 'b' * 64))
        queue.db.execute('UPDATE discovery SET response_sha256=? WHERE url=?', (sha, second))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    original = Path.is_file
    def unavailable(candidate):
        if candidate == target:
            raise OSError(5, 'temporary local error')
        return original(candidate)
    with monkeypatch.context() as patch:
        patch.setattr(Path, 'is_file', unavailable)
        result = repair_incomplete_scopes(queue, max_seconds=10)
        assert result['retained_pages_queued'] == 1 and result['page_checks_scheduled'] == 0
        assert backfill(queue, max_seconds=10)['local_io_deferrals'] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (second,)).fetchone()[0] == 'local_retry'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before


def test_incomplete_multi_page_scope_reuses_exact_discovery_envelope(queue):
    first, second = legacy_missing_page(queue)
    r = response(source_payload([31], epoch=bounds(first)[0], page=2, total=31), second)
    info, _ = cnyes_pagination(r)
    sha = hashlib.sha256(r.body).hexdigest()
    source = queue.registry.sources['cnyes']
    target = queue.root / 'discovery_responses' / (sha + '.parquet')
    atomic_write_parquet(target, table_from_records([{'request_url': second, 'raw_response_body': r.body,
        'response_sha256': sha, 'captured_at_utc': '2020-01-01T00:00:00+00:00',
        'authorization_fingerprint': source.authorization_fingerprint, 'publish': False}]), durable=True)
    atomic_write_json(target.with_suffix('.parquet.receipt.json'), {
        'sha256': sha256_file(target), 'response_sha256': sha, 'publish': False}, durable=True)
    with queue.db:
        queue.db.execute('INSERT INTO bulk_page_evidence VALUES (?,2,31,?,?)',
                         (cnyes_scope(first), info['ids_sha256'], '2020-01-01T00:00:00+00:00'))
        queue.db.execute('UPDATE discovery SET response_sha256=? WHERE url=?', (sha, second))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    bodies = [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    result = repair_incomplete_scopes(queue, max_seconds=10)
    assert result['retained_pages_queued'] == 1 and result['page_checks_scheduled'] == 0
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert head(queue, first)['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')] == bodies


@pytest.mark.parametrize('gate', ['http_403', 'http_429', 'transport_retry', 'split', 'due', 'inflight_article'])
def test_incomplete_repair_preserves_refusals_split_active_jobs_and_article_leases(queue, gate):
    first, second = legacy_missing_page(queue, failure=gate if gate.startswith(('http_', 'transport_')) else None)
    with queue.db:
        if gate == 'split':
            queue.db.execute('UPDATE discovery SET superseded_by_windows_json=? WHERE url=?', ('[]', second))
        elif gate == 'due':
            queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (second,))
        elif gate == 'inflight_article':
            queue.db.execute("UPDATE articles SET state='inflight',lease_owner='real-owner',lease_until=9e9 WHERE id=(SELECT id FROM articles LIMIT 1)")
    original = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    bodies = [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    result = repair_incomplete_scopes(queue, max_seconds=10)
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')] == bodies
    if gate != 'inflight_article':
        assert result['page_checks_scheduled'] == 0
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == original


def test_missing_list_evidence_reuses_verified_retained_envelope_without_http_or_body_reaccept(queue):
    from downloader.news_fulltext_cnyes_history import reconcile_scope
    url, _ = seed_retained_work(queue)
    with queue.db:
        queue.db.execute('DELETE FROM cnyes_history_retained_work')
        queue.db.execute('INSERT INTO bulk_page_evidence VALUES (?,1,2,?,?)',
            (cnyes_scope(url), cnyes_pagination(response(source_payload([1, 2], epoch=bounds(url)[0]), url))[0]['ids_sha256'],
             '2020-01-01T00:00:00+00:00'))
        reconcile_scope(queue, cnyes_scope(url), queue.registry.sources['cnyes'])
    original = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work').fetchone()[0] == 'pending'
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert head(queue, url)['state'] == 'list_complete'
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == original
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert observation['source_observed_at_utc'] == '2020-01-01T00:00:00+00:00'


def seed_legacy_overflow(queue):
    """Two real request identities share a saved ID hash but only one raw URL."""
    low = 1388534400
    urls = [cnyes_url(BASE, start=low + offset, end=low + 100 + offset) for offset in (0, 1)]
    r = response(source_payload(range(1, 31), epoch=low, total=931), urls[0])
    info, _ = cnyes_pagination(r)
    sha = hashlib.sha256(r.body).hexdigest()
    source = queue.registry.sources['cnyes']
    with queue.db:
        for url in urls:
            scope = cnyes_scope(url)
            queue.db.execute("INSERT INTO bulk_window_gaps VALUES (?,'cnyes','cnyes_single_day_page_cap','old-gap')", (scope,))
            queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',931,32,'old-coverage')", (scope,))
            queue.db.execute("INSERT INTO bulk_page_evidence VALUES (?,1,931,?,'old-page-clock')", (scope, info['ids_sha256']))
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due,response_sha256,last_checked_at_utc) "
                             "VALUES (?,'cnyes_news','cnyes',9e9,?,'2020-01-01T00:00:00+00:00')", (url, sha))
    target = queue.root / 'quarantine' / (sha + '.parquet')
    atomic_write_parquet(target, table_from_records([{'request_url': urls[0], 'raw_response_body': r.body,
        'response_sha256': sha, 'captured_at_utc': '2020-01-01T00:00:00+00:00',
        'authorization_fingerprint': source.authorization_fingerprint, 'publish': False}]), durable=True)
    atomic_write_json(target.with_suffix('.parquet.receipt.json'), {
        'sha256': sha256_file(target), 'response_sha256': sha, 'publish': False}, durable=True)
    return urls, target


def test_overflow_retained_scan_resumes_and_shares_only_proven_page_without_http(queue):
    urls, _ = seed_legacy_overflow(queue)
    source = queue.registry.sources['cnyes']
    assert progress(queue.db, source)['overflow_retained_recovery']['scan_state'] == 'not_started'
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    first = seed_overflow_retained(queue, limit=1, max_seconds=10)
    assert not first['complete'] and first['queued'] == 1
    local = progress(queue.db, source)['overflow_retained_recovery']
    assert local['scan_state'] == 'scanning' and local['pending_retained_pages'] == 1
    # Until all eligible scopes are indexed, overflow work is not replayed.
    assert backfill(queue, limit=1, max_seconds=10)['processed'] == 0
    assert queue.db.execute("SELECT count(*) FROM cnyes_history_page_heads").fetchone()[0] == 0
    resumed = backfill(queue, limit=1, max_seconds=10)
    assert resumed['overflow_retained_scan']['complete'] and resumed['processed'] == 1
    local = progress(queue.db, source)['overflow_retained_recovery']
    assert local['scan_state'] == 'complete' and local['pending_retained_pages'] == 0
    assert local['local_raw_only'] and not local['category_gap_completion_claimed'] and local['publisher_requests'] == 0
    observations = queue.db.execute('SELECT scope_url,page,observation_sha256 FROM cnyes_history_page_heads ORDER BY scope_url').fetchall()
    assert len(observations) == 2 and observations[0]['observation_sha256'] == observations[1]['observation_sha256']
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_ID_references').fetchone()[0] == 1
    original = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert original['request_url'] == urls[0] and original['source_observed_at_utc'] == '2020-01-01T00:00:00+00:00'
    assert queue.db.execute('SELECT count(*) FROM bulk_window_gaps').fetchone()[0] == 2
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_gap_retirements').fetchone()[0] == 0
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before
    assert backfill(queue, limit=2, max_seconds=10)['processed'] == 0


@pytest.mark.parametrize('loss', ['missing', 'damaged_receipt'])
def test_overflow_invalid_retained_page_never_schedules_an_extra_source_request(queue, loss):
    _, target = seed_legacy_overflow(queue)
    assert seed_overflow_retained(queue, limit=8, max_seconds=10)['complete']
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    if loss == 'missing':
        target.unlink()
    else:
        receipt = target.with_suffix('.parquet.receipt.json')
        value = json.loads(receipt.read_bytes())
        value['sha256'] = '0' * 64
        atomic_write_json(receipt, value, durable=True)
    assert backfill(queue, max_seconds=10)['processed'] == 1
    state = queue.db.execute('SELECT state FROM cnyes_history_retained_work').fetchone()[0]
    assert state == ('retained_unavailable' if loss == 'missing' else 'retained_invalid')
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 0
    assert queue.db.execute('SELECT count(*) FROM bulk_window_gaps').fetchone()[0] == 2


def test_overflow_scan_stop_preserves_unfinished_checkpoint_and_refusals(queue):
    import threading
    urls, _ = seed_legacy_overflow(queue)
    stop = threading.Event()
    stop.set()
    assert seed_overflow_retained(queue, stop=stop)['checked'] == 0
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_retained_work').fetchone()[0] == 0
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=2 WHERE url=?", (urls[0],))
    original = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[0],)).fetchone())
    assert seed_overflow_retained(queue, limit=8, max_seconds=10)['complete']
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[0],)).fetchone()) == original


def test_overflow_scan_checkpoint_survives_owner_restart(cfg, tmp_path):
    registry, _ = registry_http(cfg, tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as first:
        seed_legacy_overflow(first)
        assert not seed_overflow_retained(first, limit=1, max_seconds=10)['complete']
        assert first.db.execute("SELECT count(*) FROM cnyes_history_retained_work WHERE state='overflow_pending'").fetchone()[0] == 1
    with closing(NewsQueue(tmp_path, registry)) as resumed:
        for _ in range(5):
            result = backfill(resumed, limit=1, max_seconds=10)
            if result['exhausted']:
                break
        assert result['exhausted']
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 2
        assert resumed.db.execute('SELECT count(*) FROM bulk_window_gaps').fetchone()[0] == 2


def test_overflow_scan_retries_transient_storage_without_skipping_page(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history
    seed_legacy_overflow(queue)
    read = history.pq.read_table
    def unavailable(*args, **kwargs):
        if kwargs.get('columns') == ['request_url']:
            raise OSError('temporary local storage outage')
        return read(*args, **kwargs)
    monkeypatch.setattr(history.pq, 'read_table', unavailable)
    result = seed_overflow_retained(queue, limit=8, max_seconds=10)
    # Unreadable candidates are durably enqueued before the cursor advances;
    # identity is still checked later, so neither candidate is yet evidence.
    assert result['complete'] and result['queued'] == 2
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_retained_work').fetchone()[0] == 2
    monkeypatch.setattr(history.pq, 'read_table', read)
    assert backfill(queue, max_seconds=10)['processed'] == 2
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 2


def test_overflow_scan_enqueues_unverified_candidates_if_stat_is_temporarily_unavailable(queue, monkeypatch):
    from pathlib import Path
    urls, target = seed_legacy_overflow(queue)
    is_file = Path.is_file
    def unavailable(path):
        if path == target:
            raise OSError(5, 'temporary storage I/O failure')
        return is_file(path)
    monkeypatch.setattr(Path, 'is_file', unavailable)
    result = seed_overflow_retained(queue, limit=8, max_seconds=10)
    assert result['complete'] and result['queued'] == 2
    work = queue.db.execute('SELECT request_url,state FROM cnyes_history_retained_work ORDER BY request_url').fetchall()
    assert {row['request_url'] for row in work} == set(urls)
    assert all(row['state'] == 'overflow_pending' for row in work)
    monkeypatch.setattr(Path, 'is_file', is_file)
    assert backfill(queue, max_seconds=10)['processed'] == 2
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 2
    assert queue.db.execute('SELECT count(*) FROM bulk_window_gaps').fetchone()[0] == 2


def test_overflow_unverified_wrong_request_is_not_reported_as_source_corruption(queue, monkeypatch):
    from pathlib import Path
    import pyarrow.parquet as pq
    urls, target = seed_legacy_overflow(queue)
    row = pq.read_table(target).to_pylist()[0]
    row['request_url'] = urls[1]  # The later candidate is the real original request.
    atomic_write_parquet(target, table_from_records([row]), durable=True)
    atomic_write_json(target.with_suffix('.parquet.receipt.json'), {
        'sha256': sha256_file(target), 'response_sha256': row['response_sha256'], 'publish': False}, durable=True)
    original = Path.is_file
    with monkeypatch.context() as patch:
        def unavailable(candidate):
            if candidate == target:
                raise OSError(5, 'temporary local error')
            return original(candidate)
        patch.setattr(Path, 'is_file', unavailable)
        assert seed_overflow_retained(queue, limit=8, max_seconds=10)['queued'] == 2
    assert backfill(queue, limit=8, max_seconds=10)['processed'] == 2
    states = dict(queue.db.execute('SELECT request_url,state FROM cnyes_history_retained_work'))
    assert states == {urls[0]: 'forward_evidence_available', urls[1]: 'restored'}
    receipt = queue.db.execute('SELECT * FROM cnyes_history_retained_outcome_resolutions WHERE request_url=?', (urls[0],)).fetchone()
    assert receipt['original_state'] == 'retained_request_mismatch'
    assert receipt['evidence_kind'] == 'shared_page_id_reference'
    assert json.loads(receipt['original_outcome_json'])['error'] == 'retained_original_request_differs_not_relabelled'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 2
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_ID_references').fetchone()[0] == 1
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_gap_retirements').fetchone()[0] == 0


def test_overflow_local_retry_remains_in_pending_page_count(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history
    urls, target = seed_legacy_overflow(queue)
    assert seed_overflow_retained(queue, limit=8, max_seconds=10)['queued'] == 1
    original = history.pq.read_table
    with monkeypatch.context() as patch:
        def unavailable(candidate, *args, **kwargs):
            if candidate == target:
                raise OSError(5, 'temporary local error')
            return original(candidate, *args, **kwargs)
        patch.setattr(history.pq, 'read_table', unavailable)
        assert backfill(queue, max_seconds=10)['local_io_deferrals'] == 1
    local = progress(queue.db, queue.registry.sources['cnyes'])['overflow_retained_recovery']
    assert local['pending_retained_pages'] == local['local_retry_pages'] == 1
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 WHERE request_url=?', (urls[0],))
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert progress(queue.db, queue.registry.sources['cnyes'])['overflow_retained_recovery']['pending_retained_pages'] == 0


def test_retained_page_local_io_defers_without_remote_recheck_and_other_pages_continue(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history
    first, _ = seed_retained_work(queue)
    second, _ = seed_retained_work(queue, low=1388620800)
    source = queue.registry.sources['cnyes']
    target_sha = queue.db.execute('SELECT response_sha256 FROM discovery WHERE url=?', (first,)).fetchone()[0]
    read = history.pq.read_table
    def unavailable(path, *args, **kwargs):
        if path.name == target_sha + '.parquet':
            raise OSError(5, 'temporary storage I/O failure')
        return read(path, *args, **kwargs)
    monkeypatch.setattr(history.pq, 'read_table', unavailable)
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    result = backfill(queue, limit=8, max_seconds=10)
    assert result['local_io_deferrals'] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (first,)).fetchone()[0] == 'local_retry'
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (second,)).fetchone()[0] == 'restored'
    assert head(queue, second)['state'] == 'list_complete'
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before
    retry = queue.db.execute('SELECT not_before_epoch FROM cnyes_history_retained_local_retries WHERE request_url=?', (first,)).fetchone()
    assert retry and retry[0] > time.time()
    assert backfill(queue, limit=8, max_seconds=10)['processed'] == 0
    monkeypatch.setattr(history.pq, 'read_table', read)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 WHERE request_url=?', (first,))
    assert backfill(queue, limit=8, max_seconds=10)['processed'] == 1
    assert head(queue, first)['state'] == 'list_complete'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_retained_local_retries').fetchone()[0] == 0
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before
    assert progress(queue.db, source)['all_history_complete'] is False


@pytest.mark.parametrize('failure', ['stat', 'receipt', 'parquet', 'checksum', 'memory'])
def test_each_local_read_failure_is_retryable_without_publisher_recheck(queue, monkeypatch, failure):
    from pathlib import Path
    from downloader import news_fulltext_cnyes_history as history
    url, r = seed_retained_work(queue)
    path = queue.root / 'quarantine' / (hashlib.sha256(r.body).hexdigest() + '.parquet')
    before = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    error = MemoryError('local pressure') if failure == 'memory' else OSError(5, 'temporary local error')
    with monkeypatch.context() as patch:
        if failure == 'stat':
            original = Path.is_file
            def unavailable(candidate):
                if candidate == path:
                    raise error
                return original(candidate)
            patch.setattr(Path, 'is_file', unavailable)
        elif failure == 'receipt':
            original = Path.read_bytes
            def unavailable(candidate):
                if candidate == path.with_suffix('.parquet.receipt.json'):
                    raise error
                return original(candidate)
            patch.setattr(Path, 'read_bytes', unavailable)
        elif failure in ('parquet', 'memory'):
            original = history.pq.read_table
            def unavailable(candidate, *args, **kwargs):
                if candidate == path:
                    raise error
                return original(candidate, *args, **kwargs)
            patch.setattr(history.pq, 'read_table', unavailable)
        else:
            original = history.sha256_file
            def unavailable(candidate):
                if candidate == path:
                    raise error
                return original(candidate)
            patch.setattr(history, 'sha256_file', unavailable)
        result = backfill(queue, max_seconds=10)
    assert result['local_io_deferrals'] == 1 and result['exhausted']
    retry = queue.db.execute('SELECT * FROM cnyes_history_retained_local_retries WHERE request_url=?', (url,)).fetchone()
    assert retry['original_state'] == 'pending' and retry['local_attempts'] == 1
    assert retry['error_code'] == ('local_memory_unavailable' if failure == 'memory' else 'local_io_unavailable')
    assert retry['not_before_epoch'] > time.time()
    assert result['next_local_retry_epoch'] == retry['not_before_epoch']
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    assert progress(queue.db, queue.registry.sources['cnyes'])['overflow_retained_recovery']['local_retry_pages'] == 1
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 WHERE request_url=?', (url,))
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert head(queue, url)['state'] == 'list_complete'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_retained_local_retries').fetchone()[0] == 0
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_local_retry_survives_owner_restart_and_obeys_deadline(cfg, tmp_path, monkeypatch):
    from pathlib import Path
    registry, _ = registry_http(cfg, tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as first:
        url, r = seed_retained_work(first)
        path = first.root / 'quarantine' / (hashlib.sha256(r.body).hexdigest() + '.parquet')
        original = Path.is_file
        def unavailable(candidate):
            if candidate == path:
                raise OSError(5, 'local storage unavailable')
            return original(candidate)
        with monkeypatch.context() as patch:
            patch.setattr(Path, 'is_file', unavailable)
            result = backfill(first, max_seconds=10)
        deadline = result['next_local_retry_epoch']
        with first.db:
            first.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=? WHERE request_url=?',
                             (deadline + 60, url))
    with closing(NewsQueue(tmp_path, registry)) as resumed:
        result = backfill(resumed, max_seconds=10)
        assert result['processed'] == 0 and result['exhausted']
        assert result['next_local_retry_epoch'] == deadline + 60
        with resumed.db:
            resumed.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 WHERE request_url=?', (url,))
        assert backfill(resumed, max_seconds=10)['processed'] == 1
        assert head(resumed, url)['state'] == 'list_complete'


def test_due_local_retry_and_new_work_share_single_item_slices(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history
    first, r = seed_retained_work(queue)
    sha = hashlib.sha256(r.body).hexdigest()
    original = history.pq.read_table
    with monkeypatch.context() as patch:
        def unavailable(path, *args, **kwargs):
            if path.name == sha + '.parquet':
                raise OSError(5, 'temporary local error')
            return original(path, *args, **kwargs)
        patch.setattr(history.pq, 'read_table', unavailable)
        assert backfill(queue, limit=1, max_seconds=10)['local_io_deferrals'] == 1
    second, _ = seed_retained_work(queue, low=1388620800)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 WHERE request_url=?', (first,))
    assert backfill(queue, limit=1, max_seconds=10)['processed'] == 1
    assert backfill(queue, limit=1, max_seconds=10)['processed'] == 1
    assert head(queue, first)['state'] == head(queue, second)['state'] == 'list_complete'


def test_missing_source_endpoint_does_not_block_other_retained_pages(queue):
    first, _ = seed_retained_work(queue)
    second, _ = seed_retained_work(queue, low=1388620800)
    unavailable = first.replace(BASE, 'https://unregistered.invalid/news')
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_retained_work SET request_url=? WHERE request_url=?', (unavailable, first))
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    result = backfill(queue, max_seconds=10)
    assert result['source_scope_deferrals'] == 1 and result['local_io_deferrals'] == 0
    retry = queue.db.execute('SELECT * FROM cnyes_history_retained_local_retries WHERE request_url=?', (unavailable,)).fetchone()
    assert retry['error_code'] == 'source_endpoint_unavailable' and retry['not_before_epoch'] > time.time()
    assert head(queue, second)['state'] == 'list_complete'
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before


def test_history_maintenance_delay_honours_future_deadline_and_bounds():
    from downloader.news_fulltext_cnyes_history import maintenance_delay
    assert maintenance_delay({'exhausted': False, 'next_local_retry_epoch': 1000}, now_epoch=0) == 2
    assert maintenance_delay({'exhausted': False, 'processed': 1}, now_epoch=0) == 0
    assert maintenance_delay({'exhausted': False, 'processed': 0,
        'unobserved_nonzero_page_scan': {'complete': False, 'checked': 1}}, now_epoch=0) == 0
    assert maintenance_delay({'exhausted': False, 'processed': 0,
        'unobserved_nonzero_page_scan': {'complete': False, 'checked': 0}}, now_epoch=0) == 2
    assert maintenance_delay({'exhausted': True, 'next_local_retry_epoch': 105}, now_epoch=100) == 5
    assert maintenance_delay({'exhausted': True, 'next_local_retry_epoch': 99}, now_epoch=100) == 1
    assert maintenance_delay({'exhausted': True, 'next_local_retry_epoch': 1000}, now_epoch=100) == 300
    for invalid in (None, True, float('nan'), float('inf'), -1, '105'):
        assert maintenance_delay({'exhausted': True, 'next_local_retry_epoch': invalid}, now_epoch=100) == 300


def test_new_source_total_rechecks_only_conflicting_page_once_until_actual_source_converges(queue):
    low = 1388534400
    first = cnyes_url(BASE, start=low, end=low + 100)
    second = cnyes_url(BASE, 2, start=low, end=low + 100)
    capture(queue, first, source_payload(range(1, 31), epoch=low, total=31))
    old = head(queue, first)['proof_sha256']
    capture(queue, second, source_payload([31, 32], epoch=low, page=2, total=32))
    assert head(queue, first)['state'] == 'inconsistent_source_snapshot'
    assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (first,)).fetchone()[0] <= time.time()
    assert queue.db.execute('SELECT etag FROM discovery WHERE url=?', (first,)).fetchone()[0] is None
    markers = queue.db.execute("SELECT count(*) FROM queue_settings WHERE key LIKE 'cnyes_history_direct_page_recheck_v1:%'").fetchone()[0]
    assert markers == 1
    capture(queue, second, source_payload([31, 32], epoch=low, page=2, total=32))
    assert queue.db.execute("SELECT count(*) FROM queue_settings WHERE key LIKE 'cnyes_history_direct_page_recheck_v1:%'").fetchone()[0] == markers
    capture(queue, first, source_payload(range(1, 31), epoch=low, total=32))
    assert head(queue, first)['state'] == 'list_complete'
    assert queue.db.execute('SELECT 1 FROM cnyes_history_scope_proofs WHERE proof_sha256=?', (old,)).fetchone()


@pytest.fixture
def unresolved_successful_conflict(queue, monkeypatch):
    """An actual-shaped pre-fix dead end, not a failed/unfinished HTTP job."""
    import downloader.news_fulltext as main
    import downloader.news_fulltext_cnyes_history as history
    clock = [time.time()]
    stamp = lambda: datetime.fromtimestamp(clock[0], timezone.utc).isoformat()
    monkeypatch.setattr(history.time, 'time', lambda: clock[0])
    monkeypatch.setattr(history, 'now', stamp)
    monkeypatch.setattr(main, 'utc_now', stamp)
    low = 1388534400
    first = cnyes_url(BASE, start=low, end=low + 100)
    second = cnyes_url(BASE, 2, start=low, end=low + 100)

    def old_one_shot(value, url, source, *, scope, evidence_sha, confirmed_at):
        return history.schedule_original_page_check(value, url,
            reason='contradictory_list_page_evidence', evidence_sha=evidence_sha)

    def successful_but_still_conflicting():
        # Real acceptance code produces new page-confirmation and job clocks.
        # Retain v1 scheduling during seeding to model the old owner exactly.
        with monkeypatch.context() as old:
            old.setattr(history, 'schedule_consistency_recheck', old_one_shot)
            for url, ids, page, total in ((first, range(1, 31), 1, 31), (second, [31, 32], 2, 32)):
                clock[0] += 1
                capture(queue, url, source_payload(ids, epoch=low, page=page, total=total))

    successful_but_still_conflicting()
    successful_but_still_conflicting()
    assert head(queue, first)['state'] == 'inconsistent_source_snapshot'
    scope = cnyes_scope(first)
    source = queue.registry.sources['cnyes']
    stable = 'cnyes_history_consistency_recheck_v2:' + history.digest((
        first, history.cnyes_bulk_fingerprint(source), source.authorization_fingerprint))
    return {'first': first, 'second': second, 'scope': scope, 'source': source,
        'stable': stable, 'clock': clock, 'capture': successful_but_still_conflicting}


def consistency_control(queue, conflict):
    row = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (conflict['stable'],)).fetchone()
    return json.loads(row[0]) if row else None


def test_still_conflicting_success_recovers_only_unfinished_page_preserving_v1_body_and_clock(queue, unresolved_successful_conflict):
    conflict = unresolved_successful_conflict
    original = [tuple(r) for r in queue.db.execute("SELECT * FROM queue_settings WHERE key LIKE 'cnyes_history_direct_page_recheck_v1:%' ORDER BY key")]
    bodies = [tuple(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    healthy = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['second'],)).fetchone())
    old = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['first'],)).fetchone())
    assert repair_incomplete_scopes(queue, max_seconds=10)['page_checks_scheduled'] == 1
    control = consistency_control(queue, conflict)
    job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['first'],)).fetchone())
    assert control['attempts'] == 1 and control['spacing_seconds'] == conflict['source'].discovery_interval_seconds
    assert control['next_due_epoch'] == max(conflict['clock'][0],
        datetime.fromisoformat(old['last_checked_at_utc']).timestamp() + control['spacing_seconds'])
    assert {k: v for k, v in job.items() if k not in {'due', 'etag', 'last_modified'}} == {
        k: v for k, v in old.items() if k not in {'due', 'etag', 'last_modified'}}
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['second'],)).fetchone()) == healthy
    assert [tuple(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY id')] == bodies
    assert [tuple(r) for r in queue.db.execute("SELECT * FROM queue_settings WHERE key LIKE 'cnyes_history_direct_page_recheck_v1:%' ORDER BY key")] == original
    assert head(queue, conflict['first'])['state'] == 'inconsistent_source_snapshot'


def test_consistency_recheck_needs_new_response_not_restart_or_proof_change(queue, unresolved_successful_conflict):
    from downloader.news_fulltext_cnyes_history import schedule_consistency_recheck
    conflict = unresolved_successful_conflict
    assert repair_incomplete_scopes(queue, max_seconds=10)['page_checks_scheduled'] == 1
    control = consistency_control(queue, conflict)
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (conflict['first'],))
    confirmed = queue.db.execute('SELECT last_confirmed_at_utc FROM cnyes_history_page_heads WHERE scope_url=? AND page=1', (conflict['scope'],)).fetchone()[0]
    assert not schedule_consistency_recheck(queue, conflict['first'], conflict['source'],
        scope=conflict['scope'], evidence_sha='changed-proof-without-source-response', confirmed_at=confirmed)
    queue.close()  # A restart releases the existing owner, never opens a second collector.
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        assert repair_incomplete_scopes(resumed, max_seconds=10)['page_checks_scheduled'] == 0
        assert consistency_control(resumed, conflict) == control
        # A new successful job clock alone (e.g.304) cannot masquerade as a new
        # source-page confirmation. No actual body/list evidence was replaced.
        with resumed.db:
            resumed.db.execute('UPDATE discovery SET last_checked_at_utc=? WHERE url=?',
                (datetime.fromtimestamp(conflict['clock'][0] + 600, timezone.utc).isoformat(), conflict['first']))
        assert repair_incomplete_scopes(resumed, max_seconds=10)['page_checks_scheduled'] == 0
        assert consistency_control(resumed, conflict) == control


def test_repeated_successful_conflicts_back_off_at_source_cadence_not_permanent_one_shot(queue, unresolved_successful_conflict):
    conflict = unresolved_successful_conflict
    original = [tuple(r) for r in queue.db.execute("SELECT * FROM queue_settings WHERE key LIKE 'cnyes_history_direct_page_recheck_v1:%' ORDER BY key")]
    for count, factor in enumerate((1, 2, 4, 8, 16, 16), 1):
        assert repair_incomplete_scopes(queue, max_seconds=10)['page_checks_scheduled'] == 1
        control = consistency_control(queue, conflict)
        assert control['attempts'] == count
        assert control['spacing_seconds'] == factor * conflict['source'].discovery_interval_seconds
        assert repair_incomplete_scopes(queue, max_seconds=10)['page_checks_scheduled'] == 0
        conflict['clock'][0] = control['next_due_epoch'] + 1
        conflict['capture']()
    assert [tuple(r) for r in queue.db.execute("SELECT * FROM queue_settings WHERE key LIKE 'cnyes_history_direct_page_recheck_v1:%' ORDER BY key")] == original


@pytest.mark.parametrize('gate', ['http_403', 'http_429', 'transport_retry', 'attempts', 'split', 'due',
    'provider', 'kind', 'raw_hash', 'scope_complete', 'stale_contract', 'unconfirmed', 'disabled', 'before_first_recheck'])
def test_recurring_consistency_repair_never_overrides_other_source_evidence_or_schedules(queue, unresolved_successful_conflict, gate):
    from dataclasses import replace
    from downloader.news_fulltext_cnyes_history import schedule_consistency_recheck
    conflict = unresolved_successful_conflict
    assert repair_incomplete_scopes(queue, max_seconds=10)['page_checks_scheduled'] == 1
    prior_control = consistency_control(queue, conflict)
    conflict['clock'][0] = prior_control['next_due_epoch'] + 1
    conflict['capture']()
    source = conflict['source']
    with queue.db:
        if gate.startswith(('http_', 'transport_')):
            queue.db.execute('UPDATE discovery SET error_code=? WHERE url=?', (gate, conflict['first']))
        elif gate in {'attempts', 'due', 'kind', 'provider', 'raw_hash'}:
            field, value = {'attempts': ('attempts', 1), 'due': ('due', 0), 'kind': ('kind', 'index'),
                'provider': ('provider', 'not-cnyes'), 'raw_hash': ('response_sha256', 'unproven-hash')}[gate]
            queue.db.execute('UPDATE discovery SET ' + field + '=? WHERE url=?', (value, conflict['first']))
        elif gate == 'split':
            queue.db.execute('UPDATE discovery SET superseded_by_windows_json=? WHERE url=?', ('[]', conflict['first']))
        elif gate == 'scope_complete':
            queue.db.execute("UPDATE cnyes_history_scope_heads SET state='list_complete' WHERE scope_url=?", (conflict['scope'],))
        elif gate == 'stale_contract':
            queue.db.execute("UPDATE cnyes_history_scope_heads SET source_bulk_fingerprint='other-contract' WHERE scope_url=?", (conflict['scope'],))
        elif gate == 'unconfirmed':
            queue.db.execute('UPDATE cnyes_history_page_heads SET last_confirmed_at_utc=? WHERE scope_url=? AND page=1',
                (prior_control['page_confirmed_at_utc'], conflict['scope']))
        elif gate == 'before_first_recheck':
            original = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
                (prior_control['prior_v1_marker'],)).fetchone()[0])
            queue.db.execute('UPDATE cnyes_history_page_heads SET last_confirmed_at_utc=? WHERE scope_url=? AND page=1',
                (original['changed_at_utc'], conflict['scope']))
        elif gate == 'disabled':
            source = replace(source, allowed=False)
    job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['first'],)).fetchone())
    confirmed = queue.db.execute('SELECT last_confirmed_at_utc FROM cnyes_history_page_heads WHERE scope_url=? AND page=1', (conflict['scope'],)).fetchone()[0]
    assert not schedule_consistency_recheck(queue, conflict['first'], source,
        scope=conflict['scope'], evidence_sha='a-new-proof-cannot-reset-gates', confirmed_at=confirmed)
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['first'],)).fetchone()) == job
    assert consistency_control(queue, conflict) == prior_control


@pytest.mark.parametrize('corrupt', ['empty_list', 'empty_dict', 'bad_json', 'negative_count', 'zero_count', 'bool_count',
    'wrong_url', 'wrong_scope', 'wrong_version', 'missing_prior', 'changed_prior', 'naive_clock'])
def test_corrupt_consistency_witness_cannot_mint_new_retry_budget(queue, unresolved_successful_conflict, corrupt):
    conflict = unresolved_successful_conflict
    assert repair_incomplete_scopes(queue, max_seconds=10)['page_checks_scheduled'] == 1
    control = consistency_control(queue, conflict)
    conflict['clock'][0] = control['next_due_epoch'] + 1
    conflict['capture']()
    value = deepcopy(control)
    with queue.db:
        if corrupt == 'missing_prior':
            queue.db.execute('DELETE FROM queue_settings WHERE key=?', (control['prior_v1_marker'],))
        elif corrupt == 'changed_prior':
            old = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (control['prior_v1_marker'],)).fetchone()[0])
            old['prior_job']['url'] = 'https://example.invalid/another-source'
            queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps(old), control['prior_v1_marker']))
        else:
            if corrupt == 'empty_list': value = []
            elif corrupt == 'empty_dict': value = {}
            elif corrupt == 'negative_count': value['attempts'] = -1
            elif corrupt == 'zero_count': value['attempts'] = 0
            elif corrupt == 'bool_count': value['attempts'] = True
            elif corrupt == 'wrong_url': value['url'] = conflict['second']
            elif corrupt == 'wrong_scope': value['scope_url'] = 'not-this-scope'
            elif corrupt == 'wrong_version': value['contract_version'] = 999
            elif corrupt == 'naive_clock': value['page_confirmed_at_utc'] = '2026-10-23T00:00:00'
            queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?',
                ('{' if corrupt == 'bad_json' else json.dumps(value), conflict['stable']))
    old = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['first'],)).fetchone())
    assert repair_incomplete_scopes(queue, max_seconds=10)['page_checks_scheduled'] == 0
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['first'],)).fetchone()) == old


def test_source_convergence_closes_scope_without_revisiting_healthy_pages_or_erasing_old_control(queue, unresolved_successful_conflict):
    conflict = unresolved_successful_conflict
    assert repair_incomplete_scopes(queue, max_seconds=10)['page_checks_scheduled'] == 1
    control = consistency_control(queue, conflict)
    proof = head(queue, conflict['first'])['proof_sha256']
    conflict['clock'][0] = control['next_due_epoch'] + 1
    capture(queue, conflict['first'], source_payload(range(1, 31), epoch=bounds(conflict['first'])[0], total=32))
    assert head(queue, conflict['first'])['state'] == 'list_complete'
    assert repair_incomplete_scopes(queue, max_seconds=10)['page_checks_scheduled'] == 0
    assert consistency_control(queue, conflict) == control
    assert queue.db.execute('SELECT 1 FROM cnyes_history_scope_proofs WHERE proof_sha256=?', (proof,)).fetchone()


def test_source_page_count_growth_rechecks_contradictory_old_pages_while_new_pages_missing(queue, unresolved_successful_conflict):
    conflict = unresolved_successful_conflict
    old_proof = head(queue, conflict['first'])['proof_sha256']
    # Real source growth changes a seven-page contradiction into an incomplete
    # eleven-page scope. The smaller fixture preserves that exact dependency.
    conflict['clock'][0] += 1
    low, high = bounds(conflict['first'])
    capture(queue, conflict['second'], source_payload(range(31, 61), epoch=low, page=2, total=92))
    assert head(queue, conflict['first'])['state'] == 'waiting_page_evidence'
    assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (conflict['first'],)).fetchone()[0] <= conflict['clock'][0]
    assert queue.db.execute('SELECT due FROM discovery WHERE url=?',
        (cnyes_url(BASE, 3, start=low, end=high),)).fetchone()[0] <= conflict['clock'][0]
    capture(queue, conflict['first'], source_payload(range(1, 31), epoch=low, total=92))
    healthy = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['first'],)).fetchone())
    assert repair_incomplete_scopes(queue, max_seconds=10)['page_checks_scheduled'] == 0
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['first'],)).fetchone()) == healthy
    for page, ids in ((3, range(61, 91)), (4, [91, 92])):
        conflict['clock'][0] += 1
        capture(queue, cnyes_url(BASE, page, start=low, end=high), source_payload(ids, epoch=low, page=page, total=92))
    assert head(queue, conflict['first'])['state'] == 'list_complete'
    assert json.loads(head(queue, conflict['first'])['member_ids_json']) == list(range(1, 93))
    assert queue.db.execute('SELECT 1 FROM cnyes_history_scope_proofs WHERE proof_sha256=?', (old_proof,)).fetchone()


def test_repair_frontier_rotates_past_unresolved_scope_and_stops_without_cursor_mutation(queue):
    import threading
    from downloader.news_fulltext_cnyes_history import REPAIR_CURSOR
    for day in range(3):
        low = 1388534400 + day * 86400
        capture(queue, cnyes_url(BASE, start=low, end=low + 100), source_payload(range(1, 31), epoch=low, total=31))
    assert repair_incomplete_scopes(queue, limit=1, max_seconds=10)['scopes_checked'] == 1
    cursor = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (REPAIR_CURSOR,)).fetchone()[0]
    stop = threading.Event()
    stop.set()
    assert repair_incomplete_scopes(queue, stop=stop)['scopes_checked'] == 0
    assert repair_incomplete_scopes(queue, max_seconds=0)['scopes_checked'] == 0
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (REPAIR_CURSOR,)).fetchone()[0] == cursor
    assert repair_incomplete_scopes(queue, limit=1, max_seconds=10)['scopes_checked'] == 1
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (REPAIR_CURSOR,)).fetchone()[0] > cursor

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


def seed_retained_work(queue, *, failure=None, valid_raw=True):
    low = 1388534400
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
    assert work['state'] == 'retained_invalid'
    assert 'retained_original_request_differs_not_relabelled' in work['outcome_json']
    assert json.loads(work['outcome_json'])['one_successful_page_evidence_recheck_scheduled']
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 0


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


def test_legacy_zero_total_requires_actual_empty_envelope_not_inferred_observation(queue):
    from downloader.news_fulltext_cnyes_history import initialize
    parent, children = seed_partition(queue, total=1)
    capture(queue, children[0], source_payload([], epoch=bounds(children[0])[0]))
    with queue.db:
        for table in ('cnyes_history_page_heads', 'cnyes_history_page_observations', 'cnyes_history_scope_heads'):
            queue.db.execute('DELETE FROM ' + table)
        queue.db.execute("DELETE FROM queue_settings WHERE key='cnyes_history_evidence_initialization_v1'")
    initialize(queue)
    row = queue.db.execute('SELECT * FROM cnyes_history_retained_work').fetchone()
    assert row['expected_total'] == 0 and row['state'] == 'pending'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 0
    backfill(queue, max_seconds=10)
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_scope_heads').fetchone()[0] == 0
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(parent),)).fetchone()


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

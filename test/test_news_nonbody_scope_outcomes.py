"""Non-body diagnostics must not grant or fabricate finite article capture."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import time

import pytest

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext_cohorts import cohort_progress, membership_digest, record_reviewed_source_outcomes, review_nonbody_probes
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config
from scripts.verify_news_source_diagnostics import verify_recorded_nonbody_outcomes


@pytest.fixture
def reviewed(config, tmp_path):
    config['providers'][0].update(acquisition_mode='finite', article_path_pattern=r'^/article/[0-9]+$')
    registry = PolicyRegistry(config, base=tmp_path)
    q = NewsQueue(tmp_path / 'lake', registry)
    urls = ['https://publisher.test/article/2', 'https://publisher.test/article/3', 'https://publisher.test/about']
    with q.db:
        q.add_articles([{'url': 'https://publisher.test/article/1'}])
    q.sync_finite_cohorts()
    with q.db:
        q.add_articles([{'url': url, 'date': '2022-07-22'} for url in urls])
        # A pinned pre-admission importer already left this non-article waiting.
        q.db.execute("UPDATE articles SET state='waiting_cohort',error_code=NULL WHERE url=?", (urls[-1],))
    q.sync_finite_cohorts()
    folder = tmp_path / 'diagnosis'
    raw_path = folder / 'private-source-diagnostics' / 'about.parquet'
    raw = b'<html><title>Agency administrative regions</title><main>Regional geography, not news.</main></html>'
    raw_sha = hashlib.sha256(raw).hexdigest()
    atomic_write_parquet(raw_path, table_from_records([{'request_url': urls[-1], 'final_url': urls[-1],
        'provider': 'publisher', 'raw_response_body': raw, 'response_sha256': raw_sha,
        'diagnostic_only': True, 'accepted_as_article': False, 'publish': False}]))
    atomic_write_json(raw_path.with_suffix('.receipt.json'), {'sha256': sha256_file(raw_path),
        'diagnostic_only': True, 'accepted_as_article': False, 'publish': False})
    probes, traces = [], []
    for number, url in enumerate(urls):
        status, body = (410, b'gone') if number < 2 else (200, raw)
        sha = hashlib.sha256(body).hexdigest()
        probes.append({'url': url, 'provider': 'publisher', 'final_url': url, 'status': status,
                       'bytes': len(body), 'response_sha256': sha, 'error': None,
                       'private_diagnostic_path': str(raw_path) if number == 2 else None})
        traces.append({'requested_url': url, 'status': status, 'body_read': True,
                       'response_bytes': len(body), 'response_sha256': sha})
    report = {'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'probes': probes,
              'http_path_by_provider': {'publisher': traces}}
    report_path = folder / 'probes.json'
    atomic_write_json(report_path, report)
    try:
        yield q, urls, report, report_path
    finally:
        q.close()


def test_explicit_outcomes_keep_original_scope_dates_deadlines_and_body_counts(reviewed):
    q, urls, report, path = reviewed
    original = membership_digest(q.db, 'publisher')
    before = list(q.db.execute('SELECT url,metadata_date_raw,metadata_order_day,due,attempts,first_seen_utc,last_seen_utc '
                              'FROM articles ORDER BY url'))
    result = record_reviewed_source_outcomes(q, urls, path)
    assert result['http_requests'] == result['accepted_article_bodies'] == 0
    assert result['scope_extended'] is False
    assert membership_digest(q.db, 'publisher') == original
    assert not q.db.execute('SELECT 1 FROM finite_supplement_members').fetchone()
    assert list(q.db.execute('SELECT url,metadata_date_raw,metadata_order_day,due,attempts,first_seen_utc,last_seen_utc '
                            'FROM articles ORDER BY url')) == before
    assert q.db.execute("SELECT count(*) FROM articles WHERE state='source_not_found'").fetchone()[0] == 2
    assert q.db.execute("SELECT count(*) FROM articles WHERE state='unsupported_article_path'").fetchone()[0] == 1
    assert q.db.execute('SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL').fetchone()[0] == 0
    proof = q.root / result['receipt_path']
    assert sha256_file(proof) == result['receipt_sha256']
    receipt = json.loads(proof.read_text())
    assert receipt['contract_version'] == 1 and receipt['original_and_supplement_membership_unchanged']
    assert receipt['accepted_article_bodies'] == 0 and not receipt['publish']
    assert len(q.db.execute('SELECT * FROM finite_source_outcomes').fetchall()) == 3
    with pytest.raises(ValueError, match='unresolved unleased'):
        record_reviewed_source_outcomes(q, urls, path)


@pytest.mark.parametrize('status', [201, 304, 401, 403, 429, 500, 502, True])
def test_refusals_transients_and_nonterminal_statuses_are_not_absent_bodies(reviewed, status):
    q, urls, report, _ = reviewed
    report['probes'][0]['status'] = report['http_path_by_provider']['publisher'][0]['status'] = status
    with pytest.raises(ValueError):
        review_nonbody_probes(q.registry, report, [urls[0]])


@pytest.mark.parametrize('mutation', [
    {'final_url': 'https://publisher.test/article/22'}, {'provider': 'other'},
    {'error': 'http_429'}, {'sent': False}, {'response_sha256': '0' * 64}, {'bytes': True},
])
def test_unverified_or_indirect_response_cannot_update_frontier(reviewed, mutation):
    q, urls, report, path = reviewed
    report['probes'][0].update(mutation)
    atomic_write_json(path, report)
    with pytest.raises(ValueError):
        record_reviewed_source_outcomes(q, urls, path)
    assert not q.db.execute('SELECT 1 FROM finite_source_outcomes').fetchone()


@pytest.mark.parametrize('field,value', [('body_read', False), ('status', 200), ('response_bytes', 999),
                                      ('response_sha256', '0' * 64)])
def test_canonical_transport_trace_must_match_source_result(reviewed, field, value):
    q, urls, report, _ = reviewed
    report['http_path_by_provider']['publisher'][0][field] = value
    with pytest.raises(ValueError, match='canonical transport trace'):
        review_nonbody_probes(q.registry, report, [urls[0]])


@pytest.mark.parametrize('state', ['complete', 'retry', 'retry_cooldown', 'inflight', 'awaiting_authorization',
                                 'source_not_found', 'paywall_or_login', 'article_rights_or_scope_excluded'])
def test_existing_source_outcomes_and_ready_work_are_protected(reviewed, state):
    q, urls, _, path = reviewed
    with q.db:
        q.db.execute('UPDATE articles SET state=? WHERE url=?', (state, urls[1]))
    before = list(q.db.execute('SELECT url,state,error_code,last_checked_at_utc FROM articles ORDER BY url'))
    with pytest.raises(ValueError, match='unresolved unleased'):
        record_reviewed_source_outcomes(q, urls, path)
    assert list(q.db.execute('SELECT url,state,error_code,last_checked_at_utc FROM articles ORDER BY url')) == before


@pytest.mark.parametrize('field,value', [('body_sha256', 'old-body'), ('lease_owner', 'live-owner'),
                                      ('lease_until', 1), ('due', float('inf'))])
def test_heads_leases_and_future_deadlines_are_never_replaced(reviewed, field, value):
    q, urls, _, path = reviewed
    with q.db:
        q.db.execute(f'UPDATE articles SET {field}=? WHERE url=?', (value, urls[1]))
    with pytest.raises(ValueError, match='unresolved unleased'):
        record_reviewed_source_outcomes(q, urls, path)
    assert not q.db.execute('SELECT 1 FROM finite_source_outcomes').fetchone()


@pytest.mark.parametrize('age', [3601, -6])
def test_mutating_source_classification_requires_fresh_evidence(reviewed, age):
    q, urls, report, path = reviewed
    report['observed_at_utc'] = (datetime.now(timezone.utc) - timedelta(seconds=age)).isoformat()
    atomic_write_json(path, report)
    with pytest.raises(ValueError, match='recent aware'):
        record_reviewed_source_outcomes(q, urls, path)


def test_unaware_snapshot_is_not_a_source_observation(reviewed):
    q, urls, report, path = reviewed
    report['observed_at_utc'] = datetime.now().isoformat()
    atomic_write_json(path, report)
    with pytest.raises(ValueError, match='recent aware'):
        record_reviewed_source_outcomes(q, urls, path)


def test_nonarticle_raw_proof_cannot_escape_its_bounded_diagnostic_folder(reviewed):
    q, urls, report, path = reviewed
    with pytest.raises(ValueError, match='bounded private diagnostic scope'):
        review_nonbody_probes(q.registry, report, [urls[-1]], diagnostic_root=q.root)


def test_original_membership_cannot_be_reclassified_by_an_external_probe(reviewed):
    q, urls, _, path = reviewed
    with q.db:
        row = q.db.execute('SELECT id FROM articles WHERE url=?', (urls[0],)).fetchone()
        q.db.execute('INSERT INTO finite_supplement_members VALUES (?,?,?,?)', ('publisher', row[0], urls[0], 'unapproved'))
    with pytest.raises(ValueError, match='unresolved unleased'):
        record_reviewed_source_outcomes(q, urls, path)


def test_importer_rejects_nonarticle_namespace_before_finite_wait_and_preserves_old_states(reviewed):
    q, _, _, _ = reviewed
    with q.db:
        q.add_articles([{'url': 'https://publisher.test/another-about'}])
        q.db.execute("UPDATE articles SET state='retry_cooldown',due=?,error_code='http_429' WHERE url=?",
                     (time.time() + 300, 'https://publisher.test/another-about'))
    old = tuple(q.db.execute('SELECT state,due,error_code FROM articles WHERE url=?',
                            ('https://publisher.test/another-about',)).fetchone())
    with q.db:
        q.add_articles([{'url': 'https://publisher.test/another-about'}, {'url': 'https://publisher.test/non-news'}])
    assert tuple(q.db.execute('SELECT state,due,error_code FROM articles WHERE url=?',
                             ('https://publisher.test/another-about',)).fetchone()) == old
    assert tuple(q.db.execute('SELECT state,error_code FROM articles WHERE url=?',
                             ('https://publisher.test/non-news',)).fetchone()) == ('unsupported_article_path', 'unsupported_article_path')
    assert q.db.execute("SELECT value FROM queue_settings WHERE key='article_path_admission_contract_version'").fetchone()[0] == '1'


def test_read_only_outcome_verifier_replays_retained_proofs_not_article_bodies(reviewed):
    q, urls, _, path = reviewed
    record_reviewed_source_outcomes(q, urls, path)
    before = q.db.total_changes
    results = verify_recorded_nonbody_outcomes(q.root, urls, q.registry)
    assert q.db.total_changes == before
    assert len(results) == 3 and all(r['source_observation_receipt_verified'] for r in results)
    assert all(not r['parser_verified'] and not r['accepted_as_article'] for r in results)
    assert [r['diagnostic_integrity_verified'] for r in results] == [False, False, True]
    # Replays use immutable copies, not a newly edited engineering report.
    atomic_write_json(path, {'probes': []})
    assert verify_recorded_nonbody_outcomes(q.root, urls, q.registry) == results


def test_finite_outcome_progress_only_counts_current_matching_receipted_rows(reviewed):
    q, urls, _, path = reviewed
    record_reviewed_source_outcomes(q, urls, path)
    progress = cohort_progress(q.db, 'publisher')
    assert progress['new_urls_outside_cohort'] == 3
    assert progress['reviewed_nonbody_outside_cohort'] == {'contract_version': 1, 'urls': 3,
        'states': {'source_not_found': 2, 'unsupported_article_path': 1}, 'accepted_article_bodies': 0}
    assert progress['stored_bodies'] == 0
    with q.db:
        q.db.execute("UPDATE articles SET state='waiting_cohort' WHERE url=?", (urls[0],))
        q.db.execute("UPDATE finite_source_outcomes SET source_data_fingerprint='different-policy' WHERE article_id="
                     "(SELECT id FROM articles WHERE url=?)", (urls[1],))
    assert cohort_progress(q.db, 'publisher')['reviewed_nonbody_outside_cohort'] == {'contract_version': 1,
        'urls': 1, 'states': {'unsupported_article_path': 1}, 'accepted_article_bodies': 0}


@pytest.mark.parametrize('mutation', [None, 'source_provider', 'body', 'clock', 'error', 'fingerprint',
                                     'original_member', 'supplement_member', 'other_provider_member'])
def test_sparse_progress_query_matches_previous_oracle_with_all_guards_and_no_writes(reviewed, mutation):
    from downloader.news_fulltext_cohorts import _NONBODY_OUTCOME_PROGRESS_SQL, reviewed_nonbody_progress
    q, urls, _, path = reviewed
    record_reviewed_source_outcomes(q, urls, path)
    key = q.db.execute('SELECT id FROM articles WHERE url=?', (urls[0],)).fetchone()[0]
    with q.db:
        if mutation == 'source_provider':
            q.db.execute("UPDATE articles SET provider='different' WHERE id=?", (key,))
        elif mutation in {'body', 'clock', 'error'}:
            column = {'body': 'body_sha256', 'clock': 'last_checked_at_utc', 'error': 'error_code'}[mutation]
            q.db.execute(f'UPDATE articles SET {column}=? WHERE id=?', ('changed-fixture', key))
        elif mutation == 'fingerprint':
            q.db.execute("UPDATE finite_source_outcomes SET source_data_fingerprint='different' WHERE article_id=?", (key,))
        elif mutation in {'original_member', 'other_provider_member'}:
            # Membership exclusion is global, not merely the current provider;
            # optimizing it must not silently relax this admission boundary.
            provider = 'other' if mutation == 'other_provider_member' else 'publisher'
            q.db.execute('INSERT INTO finite_cohort_members VALUES (?,?,?)', (provider, key, urls[0]))
        elif mutation == 'supplement_member':
            q.db.execute('INSERT INTO finite_supplement_members VALUES (?,?,?,?)', ('publisher', key, urls[0], 'fixture'))
    legacy = _NONBODY_OUTCOME_PROGRESS_SQL.replace(
        'o CROSS JOIN articles a WHERE a.id=o.article_id AND', 'o JOIN articles a ON a.id=o.article_id WHERE')
    args = ('publisher', 'source_capture_fingerprint:publisher')
    before = q.db.total_changes
    result = reviewed_nonbody_progress(q.db, 'publisher')
    assert result == dict(q.db.execute(legacy, args))
    assert sum(result.values()) == (3 if mutation is None else 2)
    assert q.db.total_changes == before


def test_sparse_progress_plan_never_starts_with_the_whole_article_frontier(reviewed):
    from downloader.news_fulltext_cohorts import _NONBODY_OUTCOME_PROGRESS_SQL
    q, _, _, _ = reviewed
    plan = [r[3] for r in q.db.execute('EXPLAIN QUERY PLAN ' + _NONBODY_OUTCOME_PROGRESS_SQL,
                                     ('publisher', 'source_capture_fingerprint:publisher'))]
    outcome = next(i for i, item in enumerate(plan) if item.startswith(('SEARCH o ', 'SCAN o ')))
    article = next(i for i, item in enumerate(plan) if item.startswith('SEARCH a '))
    assert outcome < article
    assert not any(item.startswith('SCAN a ') for item in plan)


@pytest.mark.parametrize('field,value', [('state', 'waiting_cohort'), ('error_code', 'http_429'),
                                      ('body_sha256', 'new-head'), ('last_checked_at_utc', 'different-clock')])
def test_read_only_verifier_rejects_changed_current_source_outcome(reviewed, field, value):
    q, urls, _, path = reviewed
    record_reviewed_source_outcomes(q, urls, path)
    with q.db:
        q.db.execute(f'UPDATE articles SET {field}=? WHERE url=?', (value, urls[0]))
    with pytest.raises(ValueError, match='no longer matches'):
        verify_recorded_nonbody_outcomes(q.root, urls, q.registry)


@pytest.mark.parametrize('kind', ['approval', 'probe', 'raw', 'raw_receipt'])
def test_read_only_verifier_rejects_corrupted_immutable_proofs(reviewed, kind):
    q, urls, _, path = reviewed
    recorded = record_reviewed_source_outcomes(q, urls, path)
    approval_path = q.root / recorded['receipt_path']
    receipt = json.loads(approval_path.read_text())
    raw = next(r for r in receipt['outcomes'] if r.get('private_source_path'))
    target = {'approval': approval_path, 'probe': q.root / receipt['probe_path'],
              'raw': q.root / raw['private_source_path'],
              'raw_receipt': (q.root / raw['private_source_path']).with_suffix('.receipt.json')}[kind]
    atomic_write_bytes(target, b'corrupt synthetic proof')
    with pytest.raises(ValueError, match='integrity invalid|immutable original changed'):
        verify_recorded_nonbody_outcomes(q.root, urls, q.registry)


@pytest.mark.parametrize('field,value', [('diagnostic_only', False), ('accepted_as_article', True), ('publish', True)])
def test_nonarticle_source_flags_must_not_allow_body_acceptance(reviewed, field, value):
    import pyarrow.parquet as pq
    q, urls, report, path = reviewed
    raw_path = path.parent / 'private-source-diagnostics/about.parquet'
    raw = pq.read_table(raw_path).to_pylist()[0]
    raw[field] = value
    atomic_write_parquet(raw_path, table_from_records([raw]))
    atomic_write_json(raw_path.with_suffix('.receipt.json'), {'sha256': sha256_file(raw_path),
        'diagnostic_only': True, 'accepted_as_article': False, 'publish': False})
    with pytest.raises(ValueError, match='source identity invalid'):
        review_nonbody_probes(q.registry, report, [urls[-1]], diagnostic_root=raw_path.parent)


def test_nonarticle_raw_logical_grain_is_bounded_before_materialization(reviewed):
    import pyarrow.parquet as pq
    q, urls, report, path = reviewed
    raw_path = path.parent / 'private-source-diagnostics/about.parquet'
    raw = pq.read_table(raw_path).to_pylist()[0]
    atomic_write_parquet(raw_path, table_from_records([raw, raw]))
    atomic_write_json(raw_path.with_suffix('.receipt.json'), {'sha256': sha256_file(raw_path),
        'diagnostic_only': True, 'accepted_as_article': False, 'publish': False})
    with pytest.raises(ValueError, match='logical grain'):
        review_nonbody_probes(q.registry, report, [urls[-1]], diagnostic_root=raw_path.parent)


def test_membership_change_before_commit_rolls_back_all_selected_states(reviewed, monkeypatch):
    import downloader.news_fulltext_cohorts as cohorts
    q, urls, _, path = reviewed
    persist = cohorts._persist_immutable_proof
    def record_then_change_membership(root, relative, payload):
        persist(root, relative, payload)
        if relative.startswith('capture_cohorts/source_outcomes/') and not relative.endswith('.probe.json'):
            key = q.db.execute('SELECT id FROM articles WHERE url=?', (urls[1],)).fetchone()[0]
            with q.db:
                q.db.execute('INSERT INTO finite_supplement_members VALUES (?,?,?,?)',
                             ('publisher', key, urls[1], 'concurrent-membership'))
    monkeypatch.setattr(cohorts, '_persist_immutable_proof', record_then_change_membership)
    with pytest.raises(ValueError, match='frontier changed before commit'):
        record_reviewed_source_outcomes(q, urls, path)
    assert not q.db.execute('SELECT 1 FROM finite_source_outcomes').fetchone()
    assert all(q.db.execute('SELECT state FROM articles WHERE url=?', (url,)).fetchone()[0] == 'waiting_cohort'
               for url in urls)

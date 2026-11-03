"""New issuer comparisons reuse verified posts and never impersonate news bodies."""
from datetime import datetime, timezone
from copy import deepcopy
import hashlib
import json
import sqlite3

import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext_cnyes_history import digest
from downloader.news_fulltext_policy import PolicyRegistry, cnyes_bulk_fingerprint
from scripts import audit_news_cnyes_issuer_overlap as audit
from test_audit_news_issuer_overlap import head, inputs
from test_cnyes_news_bulk import cfg as cnyes_config, payload, response


def _post(number):
    post = payload(ids=(number,))['items']['data'][0]
    post.update(source='台灣證券交易所', stock=['1234'], market=[{'code': '1234', 'name': '測試公司'}],
                title='測試公司:公告董事會決議', content=None,
                publishAt=int(datetime(2013, 3, 28, 1, tzinfo=timezone.utc).timestamp()))
    return post


def _envelope(news, source, posts, marker):
    data = payload(ids=tuple(post['newsId'] for post in posts))
    data['items']['data'] = posts
    data['items']['fixture_marker'] = marker
    raw = response(data)
    sha = hashlib.sha256(raw.body).hexdigest()
    relative = 'quarantine/' + sha + '.parquet'
    path = news / relative
    capture = '2026-10-01T00:00:00+00:00'
    row = {'request_url': raw.url, 'raw_response_body': raw.body, 'response_sha256': sha,
           'captured_at_utc': capture, 'authorization_fingerprint': source.authorization_fingerprint,
           'authorization_json': json.dumps(source.authorization, sort_keys=True), 'publish': False}
    atomic_write_parquet(path, table_from_records([row]), compression='zstd')
    receipt = {'parquet_path': relative, 'sha256': sha256_file(path), 'response_sha256': sha,
               'article_count': len(posts), 'publish': False}
    atomic_write_json(path.with_suffix('.parquet.receipt.json'), receipt)
    evidence = [{'news_id': post['newsId'], 'source_post_sha256': digest(post), 'source_response_sha256': sha,
        'parquet_sha256': receipt['sha256'], 'source_observed_at_utc': capture,
        'head': {'state': 'source_article_empty_template', 'last_checked_at_utc': '2026-10-18T00:00:00+00:00'},
        'evidence_state': 'verified', 'outcome': 'bulk_body_not_provided'} for post in posts]
    return path, evidence


def _fixture(tmp_path, cfg, *, nonissuer=False, market_mismatch=False):
    heads = [head(number=i, url=f'https://news.cnyes.com/news/id/{i}', provider='cnyes',
                  state='source_article_empty_template') for i in (1, 2, 3)]
    news, finlab, db, part, receipt = inputs(tmp_path, heads=heads)
    registry = PolicyRegistry(cfg, base=tmp_path)
    source = registry.sources['cnyes']
    posts = [_post(i) for i in (1, 2, 3)]
    if nonissuer:
        posts[2]['source'] = '不同來源'
    if market_mismatch:
        posts[2]['market'][0]['name'] = '另一家公司'
    a, ea = _envelope(news, source, posts[:2], 'a')
    b, eb = _envelope(news, source, posts[1:], 'b')
    c, ec = _envelope(news, source, posts[2:], 'c')
    outcomes = ea + eb + ec
    checked = {'contract': 'cnyes_retained_missing_crosscheck_v1',
        'source_fingerprint': cnyes_bulk_fingerprint(source), 'candidate_pairs': len(outcomes),
        'response_failures': [], 'unfinished_responses': 0, 'target_outcomes': outcomes}
    prior = {'finlab_receipt_sha256': sha256_file(receipt), 'rows': [{'news_id': 1,
        'issuer_match_state': 'single_local_nonempty_info', 'observations': [{'evidence_state': 'verified',
            'source_post_sha256': digest(posts[0]), 'candidate_finlab_key': ['1234', '2013-03-28', '公告董事會決議']}]}]}
    return news, finlab, db, part, receipt, registry, checked, prior, (a, b, c)


def test_new_comparison_reads_one_covering_source_and_preserves_distinct_provenance(cnyes_config, tmp_path):
    news, finlab, db, part, receipt, registry, checked, prior, paths = _fixture(tmp_path, cnyes_config)
    before = {path: sha256_file(path) for path in (db, part, receipt, *paths)}
    result = audit.audit(news, finlab, registry, checked, prior, batch_rows=1)
    counts = result['counts']
    assert counts['previously_compared_pairs'] == 1
    assert counts['novel_post_identities'] == counts['novel_heads'] == 2
    assert counts['available_source_envelopes'] == 3 and counts['initial_source_cover'] == counts['source_envelopes_attempted'] == 1
    assert counts['match_states'] == {'single_local_nonempty_info': 2}
    assert result['accepted'] and result['publisher_requests'] == result['queue_writes'] == 0
    assert {path: sha256_file(path) for path in before} == before
    assert all(row['selected_response_sha256'] == paths[1].stem for row in result['rows'])
    assert all(len(row['source_observations']) == 2 for row in result['rows'])
    local = result['rows'][0]['local_record_candidates'][0]
    assert local['provider_event_time_literal'] == '2013-03-28T18:03:23' and local['provider_event_timezone'] is None
    assert result['rows'][0]['publisher_day_taipei'] == '2013-03-28'
    assert '測試公告說明，並非新聞正文。' not in json.dumps(result, ensure_ascii=False)
    assert not result['body_equivalence_verified'] and not result['publication_clock_verified_by_match']
    assert not result['news_training_injection'] and not result['all_history_complete']
    changed = {**prior, 'finlab_receipt_sha256': '0' * 64}
    assert audit.audit(news, finlab, registry, checked, changed)['counts']['novel_post_identities'] == 3


def test_new_proof_contract_requires_complete_original_request_identity(cnyes_config, tmp_path):
    news, finlab, db, part, receipt, registry, checked, prior, paths = _fixture(tmp_path, cnyes_config)
    checked = {**checked, 'contract': 'cnyes_retained_missing_crosscheck_v2',
               'unresolved_matching_observations': 0,
               'target_outcomes': [{**item, 'original_request_identity_checked': True}
                                   for item in checked['target_outcomes']]}
    assert audit.audit(news, finlab, registry, checked, prior)['accepted']
    for changed in ({**checked, 'unresolved_matching_observations': 1},
                    {**checked, 'target_outcomes': [{**item, 'original_request_identity_checked': False}
                                                    for item in checked['target_outcomes']]}):
        with pytest.raises(ValueError, match='invalid_cnyes_proof_scope'):
            audit.audit(news, finlab, registry, changed, prior)


def test_failed_selected_original_uses_alternatives_but_failure_remains_visible(cnyes_config, tmp_path):
    news, finlab, db, part, receipt, registry, checked, prior, paths = _fixture(tmp_path, cnyes_config)
    broken = paths[1].with_suffix('.parquet.receipt.json')
    altered = json.loads(broken.read_bytes()); altered['sha256'] = '0' * 64
    atomic_write_json(broken, altered)
    before = {path: sha256_file(path) for path in (db, part, receipt, *paths, broken)}
    result = audit.audit(news, finlab, registry, checked, prior)
    assert not result['accepted']
    assert result['counts']['source_envelopes_failed'] == 1
    assert result['counts']['source_envelopes_attempted'] == 3
    assert result['counts']['unverified_post_identities'] == 0
    assert result['counts']['match_states'] == {'single_local_nonempty_info': 2}
    assert {path: sha256_file(path) for path in before} == before


def test_changed_head_and_unknown_prior_do_not_remove_unchecked_work(cnyes_config, tmp_path):
    news, finlab, db, _, _, registry, checked, prior, _ = _fixture(tmp_path, cnyes_config)
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE articles SET state='complete',body_sha256='new',parquet_path='new.parquet' WHERE url=?",
                     ('https://news.cnyes.com/news/id/3',))
    result = audit.audit(news, finlab, registry, checked, prior)
    assert result['counts']['changed_head_pairs'] == 2
    assert result['counts']['novel_post_identities'] == 1
    mixed = {'finlab_receipt_sha256': prior['finlab_receipt_sha256'], 'rows': [{**prior['rows'][0],
        'issuer_match_state': 'no_unambiguous_issuer_key'}]}
    assert audit.previously_compared(mixed, prior['finlab_receipt_sha256']) == set()
    with pytest.raises(ValueError, match='invalid_cnyes_proof_scope'):
        audit.audit(news, finlab, registry, {**checked, 'candidate_pairs': 0}, prior)


def test_missing_exact_key_is_a_per_article_result_not_only_a_count(cnyes_config, tmp_path):
    news, finlab, _, _, _, registry, checked, prior, _ = _fixture(tmp_path, cnyes_config, nonissuer=True)
    result = audit.audit(news, finlab, registry, checked, prior)
    assert result['counts']['match_states'] == {'single_local_nonempty_info': 1, 'no_exact_issuer_key': 1}
    unknown = next(row for row in result['rows'] if row['match_state'] == 'no_exact_issuer_key')
    assert unknown['news_id'] == 3 and unknown['symbol_candidate'] is None
    assert unknown['literal_subject'] is None and unknown['local_record_candidates'] == []
    assert unknown['source_post_sha256'] and len(unknown['source_observations']) == 2


def test_complete_legacy_results_from_evidence_without_reopening_sources(cnyes_config, tmp_path, monkeypatch):
    news, finlab, _, _, _, registry, checked, prior, _ = _fixture(tmp_path, cnyes_config, nonissuer=True)
    completed = audit.audit(news, finlab, registry, checked, prior)
    old = {**completed, 'rows': [row for row in completed['rows'] if row['match_state'] != 'no_exact_issuer_key'],
           'input_audit_sha256': ['a' * 64, 'b' * 64],
           'implementation_sha256': {'scripts/audit_news_cnyes_issuer_overlap.py': audit.LEGACY_UNKEYED_REPORT_CODE_SHA256}}
    before = deepcopy(old)
    def must_not_read(*args, **kwargs):
        raise AssertionError('completed source was reopened')
    monkeypatch.setattr(audit, '_source_posts', must_not_read)
    monkeypatch.setattr(audit, 'verified_finlab', must_not_read)
    monkeypatch.setattr(audit.sqlite3, 'connect', must_not_read)
    derived = audit.complete_legacy_unkeyed_rows(checked, prior, old, ['a' * 64, 'b' * 64])
    assert old == before and derived['derived_unkeyed_rows'] == 1
    missing = next(row for row in derived['rows'] if row['match_state'] == 'no_exact_issuer_key')
    assert missing['news_id'] == 3 and len(missing['source_observations']) == 2
    assert missing['selected_response_sha256'] is None and missing['source_publish_epoch'] is None
    assert missing['provenance'] == 'derived_from_complete_legacy_coverage_and_keyed_rows'
    assert derived['current_queue_state_rechecked'] is False

    with pytest.raises(ValueError, match='identity_mismatch'):
        audit.complete_legacy_unkeyed_rows(checked, prior, old, ['different', 'b' * 64])
    changed = deepcopy(old)
    changed['implementation_sha256']['scripts/audit_news_cnyes_issuer_overlap.py'] = '0' * 64
    with pytest.raises(ValueError, match='identity_mismatch'):
        audit.complete_legacy_unkeyed_rows(checked, prior, changed, ['a' * 64, 'b' * 64])
    changed = deepcopy(old)
    changed['counts']['unverified_post_identities'] = 1
    with pytest.raises(ValueError, match='scope_incomplete'):
        audit.complete_legacy_unkeyed_rows(checked, prior, changed, ['a' * 64, 'b' * 64])
    changed = deepcopy(old)
    changed['rows'][0]['match_state'] = 'no_local_match'
    with pytest.raises(ValueError, match='rows_or_counts_inconsistent'):
        audit.complete_legacy_unkeyed_rows(checked, prior, changed, ['a' * 64, 'b' * 64])
    changed = deepcopy(old)
    changed['rows'][0]['selected_response_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='rows_or_counts_inconsistent'):
        audit.complete_legacy_unkeyed_rows(checked, prior, changed, ['a' * 64, 'b' * 64])


@pytest.mark.parametrize('change', [
    {'source': '其他來源'}, {'stock': ['1234', '5678']}, {'market': []},
    {'title': '其他公司:公告董事會決議'}, {'publishAt': True}, {'title': '測試公司:'},
])
def test_issuer_key_does_not_guess_source_symbol_time_or_title(change):
    assert audit.issuer_key({**_post(1), **change}) is None


def test_cover_only_removes_duplicate_identical_post_identities():
    pairs = {('1', 'same'), ('2', 'same'), ('3', 'same')}
    sources = {'a': {('1', 'same'), ('2', 'same')}, 'b': {('2', 'same'), ('3', 'same')},
               'c': {('3', 'same')}}
    chosen = audit.select_sources(sources, pairs)
    assert len(chosen) == 2 and set().union(*(sources[name] for name in chosen)) == pairs

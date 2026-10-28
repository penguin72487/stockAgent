from copy import deepcopy
import sqlite3

import pytest

from downloader.artifact_io import sha256_file
from scripts import audit_news_cnyes_issuer_overlap as prior_audit
from scripts import audit_news_cnyes_unkeyed as audit
from test_audit_news_cnyes_issuer_overlap import _fixture, _post
from test_cnyes_news_bulk import cfg as cnyes_config


def test_conflicting_market_name_is_a_tentative_match_and_reuses_prior_key(cnyes_config, tmp_path, monkeypatch):
    news, finlab, db, part, receipt, registry, checked, prior, paths = _fixture(tmp_path, cnyes_config, market_mismatch=True)
    previous = prior_audit.audit(news, finlab, registry, checked, prior)
    before = {p: sha256_file(p) for p in (db, part, receipt, *paths)}
    def unexpected_scan(*args, **kwargs):
        raise AssertionError('an identical FinLab key was needlessly rescanned')
    monkeypatch.setattr(audit, 'match_local_records', unexpected_scan)
    result = audit.audit(news, finlab, registry, checked, previous)
    assert result['accepted'] and result['counts']['unkeyed_targets'] == 1
    assert result['counts']['reused_keys'] == 1 and result['counts']['new_keys_scanned'] == 0
    assert result['counts']['rows_with_finlab_candidates'] == 1
    row, = result['rows']
    assert row['reason'] == 'title_market_name_mismatch'
    assert row['title_company_prefix_literal'] == '測試公司' and row['market_names_for_symbol'] == ['另一家公司']
    assert row['tentative_finlab_key'] == ('1234', '2013-03-28', '公告董事會決議')
    assert row['issuer_identity_verified'] is row['official_publication_time_verified'] is False
    assert row['finlab_record_candidates'][0]['nonempty_info']
    assert result['publisher_requests'] == result['queue_writes'] == 0
    assert {p: sha256_file(p) for p in before} == before


def test_other_publisher_has_no_tentative_twse_key(cnyes_config, tmp_path):
    news, finlab, _, _, _, registry, checked, prior, _ = _fixture(tmp_path, cnyes_config, nonissuer=True)
    previous = prior_audit.audit(news, finlab, registry, checked, prior)
    result = audit.audit(news, finlab, registry, checked, previous)
    row, = result['rows']
    assert result['accepted'] and row['reason'] == 'publisher_not_twse'
    assert row['tentative_finlab_key'] is None and row['finlab_record_candidates'] == []


def test_source_failure_falls_back_without_erasing_failure(cnyes_config, tmp_path, monkeypatch):
    news, finlab, _, _, _, registry, checked, prior, _ = _fixture(tmp_path, cnyes_config, market_mismatch=True)
    previous = prior_audit.audit(news, finlab, registry, checked, prior)
    unkeyed, = (r for r in previous['rows'] if r['match_state'] == 'no_exact_issuer_key')
    pair = unkeyed['news_id'], unkeyed['source_post_sha256']
    by_source = {source: {pair} for source in unkeyed['source_observations']}
    first = audit.select_sources(by_source, {pair})[0]
    original = audit._source_posts
    def failing_first(root, reg, source, sha, evidence, wanted):
        if sha == first:
            raise ValueError('fixture_source_corrupt')
        return original(root, reg, source, sha, evidence, wanted)
    monkeypatch.setattr(audit, '_source_posts', failing_first)
    result = audit.audit(news, finlab, registry, checked, previous)
    assert not result['accepted'] and result['counts']['source_envelopes_failed'] == 1
    assert result['counts']['source_envelopes_attempted'] == 2
    assert result['counts']['verified_post_identities'] == 1
    assert result['source_failures'][0]['response_sha256'] == first


def test_changed_head_and_conflicting_proof_fail_closed(cnyes_config, tmp_path):
    news, finlab, db, _, _, registry, checked, prior, _ = _fixture(tmp_path, cnyes_config, market_mismatch=True)
    previous = prior_audit.audit(news, finlab, registry, checked, prior)
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE articles SET state='complete',body_sha256='new',parquet_path='new.parquet' WHERE url=?",
                     ('https://news.cnyes.com/news/id/3',))
    result = audit.audit(news, finlab, registry, checked, previous)
    assert not result['accepted'] and result['counts']['changed_heads'] == 1
    assert result['counts']['source_envelopes_attempted'] == 0 and result['rows'] == []
    bad = deepcopy(checked)
    bad['source_fingerprint'] = '0' * 64
    with pytest.raises(ValueError, match='invalid_upstream_audit'):
        audit.audit(news, finlab, registry, bad, previous)


def test_candidate_does_not_invent_company_identity_or_timestamp():
    post = _post(8)
    post['market'] = None
    result = audit.candidate(post)
    assert result['reason'] == 'market_metadata_missing'
    assert result['title_company_prefix_literal'] == '測試公司'
    assert result['tentative_finlab_key'] == ('1234', '2013-03-28', '公告董事會決議')
    post['publishAt'] = True
    assert audit.candidate(post)['tentative_finlab_key'] is None
    post['publishAt'] = 1
    post['stock'] = ['1234', '5678']
    assert audit.candidate(post)['tentative_finlab_key'] is None


def test_conflicting_previously_compared_key_is_not_reused():
    row = {'match_state': 'no_local_match', 'symbol_candidate': '1234', 'publisher_day_taipei': '2013-03-28',
           'literal_subject': '公告', 'local_record_candidates': []}
    bad = {**row, 'match_state': 'single_local_nonempty_info', 'local_record_candidates': [{'nonempty_info': True}]}
    with pytest.raises(ValueError, match='conflicting_previous_exact_key_results'):
        audit._known_key_results([row, bad])

from copy import deepcopy
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from scripts.plan_news_cnyes_disclosure_candidates import is_43_1_candidate, plan, verify_readback


def _row(number, title, day='2024-05-10'):
    local = datetime.fromisoformat(day + 'T12:00:00').replace(tzinfo=ZoneInfo('Asia/Taipei'))
    return {'news_id': number, 'match_state': 'no_local_match', 'literal_subject': title,
        'symbol_candidate': '2330', 'publisher_day_taipei': day, 'source_publish_epoch': int(local.timestamp()),
        'source_post_sha256': 'a' * 64, 'selected_response_sha256': 'b' * 64,
        'source_observations': {'b' * 64: '2026-10-01T00:00:00+00:00'}, 'local_record_candidates': []}


def _inputs():
    rows = [_row(1, '依證券交易法第43條之1公告'),
            _row(2, '代取得人依證券交易法第４３ 條之１辦理'),
            _row(3, '依證交法第四十三條之一公告', '2024-05-11'),
            _row(4, '其他不相關的公告')]
    report = {'contract': 'cnyes_finlab_issuer_overlap_v1', 'accepted': True, 'source_failures': [],
        'input_audit_sha256': ['d' * 64, 'e' * 64], 'rows': rows,
        'counts': {'source_envelopes_failed': 0, 'unverified_post_identities': 0, 'novel_post_identities': 4,
                   'verified_post_identities': 4, 'match_states': {'no_local_match': 4}}}
    prior = {'contract_version': 1, 'source_audit_sha256': 'e' * 64, 'candidate_heads': 1,
             'candidate_days': 1, 'groups': [{'candidates': [{'cnyes_news_id': 1}]}]}
    return report, prior


@pytest.mark.parametrize('title', [
    '依證券交易法第43條之1公告', '依 證交法 第４３ 條之１公告',
    '依證交法第四十三條之一公告', '依證券交易法 43-1 公告',
])
def test_explicit_legal_reference_variants(title):
    assert is_43_1_candidate(title)


@pytest.mark.parametrize('title', [
    '依證券交易法第43條之10公告', '依證券交易法第43條之15公告',
    '依證交法第四十三條之一百公告', '證券交易法第43條之第1項', '其他重大訊息', None,
])
def test_does_not_turn_distinct_or_unclear_titles_into_candidates(title):
    assert not is_43_1_candidate(title)


def test_plans_only_new_heads_and_preserves_literal_source_evidence():
    report, prior = _inputs()
    before = deepcopy((report, prior))
    result = plan(report, prior)
    assert (report, prior) == before
    assert (result['candidate_heads'], result['candidate_days'], result['unique_issuer_day_candidates']) == (2, 2, 2)
    assert result['already_in_prior_worklist'] == 1
    item = result['groups'][0]['candidates'][0]
    assert item['cnyes_news_id'] == 2
    assert item['cnyes_subject_literal'] == '代取得人依證券交易法第４３ 條之１辦理'
    assert item['source_observations'] == report['rows'][1]['source_observations']
    assert item['official_record_identity_verified'] is False
    assert result['current_queue_state_rechecked'] is False
    assert result['source_requests'] == result['production_queue_writes'] == 0
    assert result['historical_gaps_retired'] is False


def test_request_scoped_worklist_keeps_equal_bytes_with_independent_clocks():
    report, prior = _inputs()
    report['contract'] = 'cnyes_finlab_issuer_overlap_v2'
    path = 'quarantine/' + 'b' * 64 + '.parquet'
    other = 'discovery_responses/by_request/aa/' + 'a' * 64 + '/' + 'b' * 64 + '.parquet'
    for row in report['rows']:
        row['selected_source_artifact_path'] = path
        row['source_observations'] = [
            {'source_response_sha256': 'b' * 64, 'source_artifact_path': path,
             'source_artifact_kind': 'canonical_quarantine', 'source_observed_at_utc': '2026-10-01T00:00:00+00:00'},
            {'source_response_sha256': 'b' * 64, 'source_artifact_path': other,
             'source_artifact_kind': 'cnyes_history_source_envelope', 'source_observed_at_utc': '2026-10-02T00:00:00+00:00'},
        ]
    result = plan(report, prior)
    assert result['contract'] == 'cnyes_43_1_incremental_candidates_v2'
    item = result['groups'][0]['candidates'][0]
    assert item['source_observations'] == report['rows'][1]['source_observations']
    assert item['selected_source_artifact_path'] == path
    altered = deepcopy(report)
    altered['rows'][1]['selected_source_artifact_path'] = 'discovery_responses/not-observed.parquet'
    with pytest.raises(ValueError, match='invalid_candidate_provenance'):
        plan(altered, prior)
    altered = deepcopy(report)
    altered['rows'][1]['source_observations'][1]['source_observed_at_utc'] = '2026-10-02T00:00:00'
    with pytest.raises(ValueError, match='invalid_candidate_provenance'):
        plan(altered, prior)


def test_accepted_batch_worklist_requires_exact_receipt_and_request():
    report, prior = _inputs()
    report['contract'] = 'cnyes_finlab_issuer_overlap_v3'
    path, receipt = 'bodies/fixture.parquet', 'receipts/fixture.json'
    for row in report['rows']:
        row.update(selected_source_artifact_path=path, selected_source_receipt_path=receipt,
                   selected_source_request_sha256='c' * 64, source_observations=[
            {'source_response_sha256': 'b' * 64, 'source_artifact_path': path,
             'source_artifact_kind': 'accepted_body_batch', 'source_receipt_path': receipt,
             'source_request_sha256': digest, 'source_observed_at_utc': clock}
            for digest, clock in [('c' * 64, '2026-10-01T00:00:00+00:00'),
                                  ('d' * 64, '2026-10-02T00:00:00+00:00')]])
    result = plan(report, prior)
    assert result['contract'] == 'cnyes_43_1_incremental_candidates_v3'
    row = result['groups'][0]['candidates'][0]
    assert row['selected_source_request_sha256'] == 'c' * 64 and len(row['source_observations']) == 2
    for field, value in [('selected_source_request_sha256', 'e' * 64),
                         ('selected_source_receipt_path', None),
                         ('selected_source_request_sha256', None)]:
        changed = deepcopy(report)
        changed['rows'][1][field] = value
        with pytest.raises(ValueError, match='invalid_candidate_provenance'):
            plan(changed, prior)
    changed = deepcopy(report)
    changed['rows'][1]['source_observations'][0]['source_receipt_path'] = '../other.json'
    with pytest.raises(ValueError, match='invalid_candidate_provenance'):
        plan(changed, prior)


def test_readback_records_title_access_separately_from_body_access():
    report, _ = _inputs()
    report['finlab_receipt_sha256'] = 'f' * 64
    report_sha = '1' * 64
    readback = {'input_sha256': ['a', 'b', 'c', report_sha], 'finlab_receipt_sha256': 'f' * 64,
        'unique_eligible_identities': 4, 'polars_inner_join_source_rows': 0,
        'source_text_columns_read': True, 'source_title_column_read': True,
        'source_body_info_column_read': False, 'publisher_http_requests': 0, 'production_queue_writes': 0}
    verify_readback(report, readback, report_sha)
    for key, value in (('source_text_columns_read', False), ('source_title_column_read', False),
                       ('source_body_info_column_read', True), ('polars_inner_join_source_rows', 1)):
        changed = {**readback, key: value}
        with pytest.raises(ValueError, match='independent_readback_mismatch'):
            verify_readback(report, changed, report_sha)
    with pytest.raises(ValueError, match='independent_readback_mismatch'):
        verify_readback(report, readback, '2' * 64)


@pytest.mark.parametrize('fault, expected', [
    ('count', 'issuer_audit_incomplete_or_inconsistent'),
    ('failed_source', 'issuer_audit_incomplete_or_inconsistent'),
    ('prior_identity', 'prior_worklist_identity_mismatch'),
    ('prior_duplicate', 'prior_worklist_duplicate_or_invalid_head'),
    ('duplicate_head', 'issuer_audit_duplicate_or_invalid_head'),
    ('day', 'candidate_day_does_not_match_source_epoch'),
    ('source', 'invalid_candidate_provenance'),
])
def test_inconsistent_evidence_never_becomes_work(fault, expected):
    report, prior = _inputs()
    if fault == 'count':
        report['counts']['verified_post_identities'] -= 1
    elif fault == 'failed_source':
        report['source_failures'] = ['failure']
    elif fault == 'prior_identity':
        prior['source_audit_sha256'] = '0' * 64
    elif fault == 'prior_duplicate':
        prior['groups'][0]['candidates'].append({'cnyes_news_id': 1})
        prior['candidate_heads'] = 2
    elif fault == 'duplicate_head':
        report['rows'][1]['news_id'] = 1
    elif fault == 'day':
        report['rows'][1]['publisher_day_taipei'] = '2024-05-11'
    else:
        report['rows'][1]['selected_response_sha256'] = '0' * 64
    with pytest.raises(ValueError, match=expected):
        plan(report, prior)

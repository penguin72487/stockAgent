from datetime import date

import polars as pl
import pytest

from scripts.audit_tw_futures_holiday_generation_terms import (
    build_worklist, diagnose_current_terminal_bindings, diagnose_terminal_values, match_term, match_terminal_candidate,
    trace_multi_boundary_candidate,
)


def _candidate():
    return {'product': 'OLD', 'contract': '201209', 'pre_date': '2012-01-18',
            'pre_physical_contract': 'OLD:201209@unsettled#generation=0',
            'settlement_date': '2012-09-19', 'pre_transfer_target_candidate': 'NEW',
            'pre_boundary_source_sha256s': ['official'], 'recorded_corporate_boundaries': ['2012-08-30']}


def _term(**changes):
    return {'product': 'NEW', 'from_product': 'OLD', 'contract': '201209',
            'effective_date': '2012-08-30', 'source_content_sha256s': ['official'],
            'point_in_time_verified': False, 'requires_phase_and_lifecycle_admission': True, **changes}


def test_exact_notice_identity_produces_only_a_source_investigation_worklist():
    result = match_term(_candidate(), [_term()])
    assert result['status'] == 'unique_candidate_term'
    assert result['term']['equity_cash_credit_twd'] is None
    assert result['term']['point_in_time_verified'] is False
    assert build_worklist([result]).splitlines()[1] == 'corporate_adjustment_reference,NEW,201209,2012-08-30'


def test_rights_fixing_flag_and_fixed_component_remain_distinct_in_audit():
    result = match_term(_candidate(), [_term(subscription_rights_at_final_settlement=True,
                                             fixed_subscription_rights_twd=0.0,
                                             fixed_subscription_rights_evidence=[])])
    assert result['term']['subscription_rights_at_final_settlement'] is True
    assert result['term']['fixed_subscription_rights_twd'] == 0.0
    assert result['term']['fixed_subscription_rights_evidence'] == []
    missing = match_term(_candidate(), [_term()])
    assert missing['term']['subscription_rights_at_final_settlement'] is None


def test_terminal_candidate_requires_exact_contract_notice_and_single_boundary():
    candidate = match_term(_candidate(), [_term(subscription_rights_at_final_settlement=True)])
    terminal = {'product': 'NEW', 'contract': '201209', 'date': date(2012, 9, 19),
                'fixing_date': date(2012, 9, 19), 'rights_twd': 0.0,
                'stock_close': 10.0, 'subscription_price': 11.0,
                'notice_content_sha256': 'official', 'subscription_source_sha256': 'subscription',
                'cash_source_sha256': 'cash'}
    result = match_terminal_candidate(candidate, [terminal])
    assert result['status'] == 'source_bound_terminal_candidate'
    assert result['rights_twd'] == 0.0
    assert match_terminal_candidate(candidate, [{**terminal, 'notice_content_sha256': 'other'}])['status'] == 'terminal_not_found'
    assert match_terminal_candidate(candidate, [terminal, terminal])['status'] == 'terminal_ambiguous'
    assert match_terminal_candidate(_candidate(), [terminal])['status'] == 'term_unmatched'
    no_flag = match_term(_candidate(), [_term()])
    assert match_terminal_candidate(no_flag, [terminal])['status'] == 'rights_flag_unknown'
    false_flag = match_term(_candidate(), [_term(subscription_rights_at_final_settlement=False)])
    assert match_terminal_candidate(false_flag, [terminal])['status'] == 'no_outstanding_rights_in_matched_term'
    multiple = {**candidate, 'recorded_corporate_boundaries': ['2012-08-30', '2012-09-01']}
    assert match_terminal_candidate(multiple, [terminal])['status'] == 'multi_boundary_source_chain_required'
    with pytest.raises(ValueError, match='invalid rights'):
        match_terminal_candidate(candidate, [{**terminal, 'fixing_date': date(2012, 9, 20)}])


@pytest.mark.parametrize('change', [dict(product='OTHER'), dict(from_product='OTHER'),
                                    dict(contract='201212'), dict(effective_date='2012-08-31'),
                                    dict(source_content_sha256s=['different'])])
def test_product_month_date_and_notice_all_have_to_match(change):
    result = match_term(_candidate(), [_term(**change)])
    assert result['status'] == 'term_not_found'
    assert len(build_worklist([result]).splitlines()) == 1


def test_duplicate_matches_are_ambiguous_and_never_sent_as_exact_scope():
    result = match_term(_candidate(), [_term(), _term()])
    assert result['status'] == 'ambiguous_candidate_terms'
    assert len(build_worklist([result]).splitlines()) == 1


def test_boundary_outside_the_original_holding_interval_is_rejected():
    candidate = _candidate()
    candidate['recorded_corporate_boundaries'] = ['2012-12-31']
    with pytest.raises(ValueError):
        match_term(candidate, [_term()])


def test_terminal_is_rebound_by_identity_and_notice_not_a_stored_row_number():
    candidate = match_term(_candidate(), [_term(subscription_rights_at_final_settlement=True)])
    terminal = {'product': 'NEW', 'contract': '201209', 'date': date(2012, 9, 19),
                'fixing_date': date(2012, 9, 19), 'rights_twd': 0.0,
                'stock_close': 10.0, 'subscription_price': 11.0,
                'notice_content_sha256': 'official', 'subscription_source_sha256': 'subscription',
                'cash_source_sha256': 'cash', 'corporate_term_id': 0}
    candidate['terminal_rights_candidate'] = match_terminal_candidate(candidate, [terminal])
    term = {'product': 'NEW', 'contract': '201209', 'effective_date': '2012-08-30',
            'valid_until_exclusive': None, 'contract_multiplier': 2000.0,
            'known_at': '2012-08-01T00:00:00+08:00', 'source_content_sha256s': ['official'],
            'deliverable_cash_twd': 0.0, 'fixed_subscription_rights_twd': 0.0,
            'subscription_rights_at_final_settlement': True}
    terms = pl.DataFrame([{**term, 'product': 'OTHER'}, term]).with_columns(
        pl.col('valid_until_exclusive').cast(pl.String))
    terminals = pl.DataFrame([terminal])
    counts = diagnose_current_terminal_bindings([candidate], terms, terminals)
    result = candidate['terminal_current_rule_diagnostic']
    assert counts == {'current_identity_matches_review_still_required': 1}
    assert (result['stored_corporate_term_id'], result['current_corporate_term_id']) == (0, 1)
    assert not result['stored_id_matches_current']
    assert result['notice_matches_current']
    assert result['financial_or_training_adopted'] is False

    # An existing terminal row cannot authorize an ended interval or a
    # different notice merely because its old integer id still exists.
    ended = terms.with_columns(pl.when(pl.col('product') == 'NEW')
                               .then(pl.lit('2012-09-01'))
                               .otherwise(pl.col('valid_until_exclusive'))
                               .alias('valid_until_exclusive'))
    assert diagnose_current_terminal_bindings([candidate], ended, terminals) == {
        'current_identity_unresolved': 1}
    assert candidate['terminal_current_rule_diagnostic']['terms_binding_status'] == 'terms_ended'
    other_notice = terms.with_columns(pl.when(pl.col('product') == 'NEW')
                                      .then(pl.lit(['other']))
                                      .otherwise(pl.col('source_content_sha256s'))
                                      .alias('source_content_sha256s'))
    assert diagnose_current_terminal_bindings([candidate], other_notice, terminals) == {
        'current_identity_unresolved': 1}
    assert not candidate['terminal_current_rule_diagnostic']['notice_matches_current']

    with pytest.raises(ValueError, match='ambiguous terminal identity'):
        diagnose_current_terminal_bindings([candidate, candidate], terms, terminals)


def test_terminal_value_comparison_uses_source_identity_and_never_adopts():
    candidate = match_term({**_candidate(), 'pre_transfer_target_candidate': 'NE1'},
                           [_term(product='NE1', subscription_rights_at_final_settlement=True)])
    terminal = {'product': 'NE1', 'contract': '201209', 'date': date(2012, 9, 19),
                'fixing_date': date(2012, 9, 19), 'rights_twd': 20.0,
                'stock_close': 12.0, 'subscription_price': 11.0,
                'notice_content_sha256': 'official', 'subscription_source_sha256': 'subscription',
                'cash_source_sha256': 'cash', 'corporate_term_id': 123,
                'derived_terminal_value_twd': 20020.0}
    candidate['terminal_rights_candidate'] = match_terminal_candidate(candidate, [terminal])
    term = {'product': 'NE1', 'contract': '201209', 'effective_date': '2012-08-30',
            'valid_until_exclusive': None, 'contract_multiplier': 2000.0,
            'known_at': '2012-08-01T00:00:00+08:00', 'source_content_sha256s': ['official'],
            'deliverable_cash_twd': 0.0, 'fixed_subscription_rights_twd': 0.0,
            'subscription_rights_at_final_settlement': True}
    terms = pl.DataFrame([term]).with_columns(pl.col('valid_until_exclusive').cast(pl.String))
    terminals = pl.DataFrame([terminal])
    finals = pl.DataFrame([{'settlement_date': date(2012, 9, 19), 'product': 'NE1',
                            'contract': '201209', 'final_settlement_price': 10.0,
                            'final_settlement_value': None}]).with_columns(
                                pl.col('final_settlement_value').cast(pl.Float64))
    counts = diagnose_terminal_values([candidate], terms, terminals, finals)
    assert counts == {'source_components_match_previous_candidate': 1}
    assert candidate['terminal_value_rebinding_diagnostic']['current_value_twd'] == 20020.0
    assert candidate['terminal_value_rebinding_diagnostic']['financial_or_training_adopted'] is False
    changed = terminals.with_columns(pl.lit(20021.0).alias('derived_terminal_value_twd'))
    assert diagnose_terminal_values([candidate], terms, changed, finals) == {
        'source_components_or_value_unresolved': 1}
    assert diagnose_terminal_values([candidate], terms, terminals, finals.head(0)) == {
        'official_final_missing': 1}
    with pytest.raises(ValueError, match='ambiguous official final'):
        diagnose_terminal_values([candidate], terms, terminals, pl.concat([finals, finals]))


def test_multi_boundary_trace_keeps_same_code_adjustments_and_rejects_reused_code_decoy():
    candidate = match_term({**_candidate(), 'recorded_corporate_boundaries': ['2012-02-10', '2012-09-18']},
                           [_term(effective_date='2012-02-10')])
    first = _term(effective_date='2012-02-10', valid_until_exclusive='2012-07-17',
                  known_at='2012-02-03T23:59:59+08:00')
    same_code = _term(from_product='NEW', product='NEW', effective_date='2012-07-17',
                      valid_until_exclusive='2012-09-18', known_at='2012-07-05T23:59:59+08:00',
                      source_content_sha256s=['dividend'], equity_cash_credit_twd=800.0)
    last = _term(from_product='NEW', product='NE2', effective_date='2012-09-18',
                 valid_until_exclusive=None, known_at='2012-09-04T23:59:59+08:00',
                 source_content_sha256s=['last'])
    reused_old = _term(effective_date='2012-09-18', valid_until_exclusive=None,
                       known_at='2012-09-04T23:59:59+08:00', source_content_sha256s=['last'])
    result = trace_multi_boundary_candidate(candidate, [first, same_code, last, reused_old])
    assert result['status'] == 'source_candidate_chain'
    assert [(x['from_product'], x['product']) for x in result['steps']] == [
        ('OLD', 'NEW'), ('NEW', 'NEW'), ('NEW', 'NE2')]
    assert result['steps'][1]['equity_cash_credit_twd'] == 800.0
    assert result['terminal_product'] == 'NE2'
    assert result['financial_or_training_adopted'] is False
    assert trace_multi_boundary_candidate(candidate, [first, last])['status'] == 'interval_or_generation_unresolved'
    assert trace_multi_boundary_candidate(candidate, [first, same_code, last, last])['status'] == 'next_boundary_not_unique'
    assert trace_multi_boundary_candidate(candidate, [first, same_code, {**last, 'known_at': '2012-09-18T09:00:00+08:00'}])['status'] == 'step_source_or_prior_publication_unresolved'

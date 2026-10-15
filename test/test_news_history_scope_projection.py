"""Keep canonical failed scopes visible without inventing list completeness."""
import json

import pytest

from stockagent.live.news_dashboard import _cnyes_history_list_progress
from test_news_dashboard_cnyes_list_progress import history_snapshot, gaps_snapshot


@pytest.mark.parametrize('state,group', [
    ('inconsistent_source_snapshot', 'inconsistent_source_scopes'),
    ('inconsistent_original_page_ID_reference', 'inconsistent_source_scopes'),
    ('inconsistent_source_identity_or_date', 'inconsistent_source_scopes'),
    ('inconsistent_partition_evidence', 'inconsistent_source_scopes'),
    ('waiting_original_page_recheck', 'waiting_list_scopes'),
    ('waiting_child_evidence', 'waiting_list_scopes'),
    ('source_page_cap_exceeded', 'page_cap_exceeded_scopes'),
])
def test_all_canonical_noncomplete_scopes_preserve_verified_counts(state, group):
    raw = history_snapshot()
    raw['current_source_contract_scope_states'][state] = 7
    result = _cnyes_history_list_progress(raw, gaps_snapshot())
    assert result and result['scope_outcome_projection_contract_version'] == 1
    assert result['verified_list_scopes'] == 4007
    assert result[group] == (10 if group == 'waiting_list_scopes' else 7)
    assert result['current_source_contract_scope_states'][state] == 7
    assert result['unresolved_category_scopes'] == 1518
    assert result['list_coverage_is_not_body_coverage'] and not result['all_history_complete']
    assert result['finish_eta_utc'] is result['progress_ratio'] is None
    assert 'PRIVATE_' not in json.dumps(result)


@pytest.mark.parametrize('value', [True, -1, '1', None, 2**53, 1.0])
@pytest.mark.parametrize('state', ['inconsistent_source_snapshot', 'waiting_child_evidence', 'source_page_cap_exceeded'])
def test_failed_scopes_keep_the_same_strict_count_guard(state, value):
    raw = history_snapshot()
    raw['current_source_contract_scope_states'][state] = value
    assert _cnyes_history_list_progress(raw, gaps_snapshot()) is None


def test_unknown_state_and_total_overflow_remain_unknown():
    raw = history_snapshot()
    raw['current_source_contract_scope_states']['future_or_private_reason'] = 1
    assert _cnyes_history_list_progress(raw, gaps_snapshot()) is None
    raw = history_snapshot()
    raw['current_source_contract_scope_states']['inconsistent_source_snapshot'] = 2**53 - 1
    assert _cnyes_history_list_progress(raw, gaps_snapshot()) is None

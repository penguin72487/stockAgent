"""Shared queue classification never grants access or invents completion."""
import json
from copy import deepcopy

import pytest

from stockagent.data.news_source_catalog import NEWS_OUTCOME_GROUPS, classify_news_outcomes
from stockagent.live.news_dashboard import build_news_public_status
from test_news_dashboard import NOW, _seed, _save


@pytest.mark.parametrize('group,state', [(group, state) for group, states in NEWS_OUTCOME_GROUPS.items() for state in sorted(states)])
def test_every_observed_outcome_keeps_its_kind_and_original_counts(group, state):
    values = {state: 3, 'complete': 2}
    original = deepcopy(values)
    result = classify_news_outcomes(values, registered=True, body_enabled=True)
    assert result['categories'][group] == 3 and result['categories']['complete_state'] == 2
    assert sum(result['categories'].values()) == result['known_urls'] == 5
    assert values == original
    assert result['definitively_unavailable_urls'] is result['closure_progress_ratio'] is None
    assert result['origin_absence_is_not_all_paths_exhausted']
    assert not result['queue_mutation_possible'] and not result['all_history_complete']


@pytest.mark.parametrize('bad', [None, [], {'retry': True}, {'retry': -1}, {'retry': 0.5}, {3: 1}])
def test_invalid_counts_are_unknown_not_fabricated_zero(bad):
    with pytest.raises(ValueError):
        classify_news_outcomes(bad, registered=True, body_enabled=True)


def test_missing_recipe_blocked_scope_and_unknown_error_are_different():
    original = {'pending': 2, 'awaiting_authorization': 3, 'unknown_failure': 4, 'complete': 1}
    absent = classify_news_outcomes(original, registered=False, body_enabled=False)['categories']
    disabled = classify_news_outcomes(original, registered=True, body_enabled=False)['categories']
    assert absent['registration_required'] == 5 and absent['unknown_outcome'] == 4
    assert disabled['scope_or_access_waiting'] == 5 and disabled['unknown_outcome'] == 4
    assert absent['complete_state'] == disabled['complete_state'] == 1
    assert absent['queued_or_retry'] == disabled['queued_or_retry'] == 0


def test_projection_reconciles_all_groups_and_keeps_terminal_origin_vs_parser(tmp_path):
    status = _seed(tmp_path)
    counts = {'complete': 2, 'body_not_found': 1, 'source_not_found': 1,
              'source_body_image_only': 1, 'source_document_landing_page': 1}
    status['providers']['cnyes']['states'] = counts
    status['states'] = {**counts, 'awaiting_authorization': 4}
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['snapshot_consistent']
    groups = result['outcome_progress']['categories']
    assert sum(groups.values()) == 10
    assert groups['repair_required'] == groups['origin_absent'] == groups['nontext_representation'] == groups['nonarticle_response'] == 1
    assert groups['registration_required'] == 4 and groups['complete_state'] == 2
    row = next(p for p in result['providers'] if p['id'] == 'cnyes')
    assert row['states']['source_document_landing_page'] == 1 and 'other' not in row['states']
    assert row['conditional_eta_seconds'] is row['conditional_finish_at_utc'] is None
    assert result['outcome_progress']['closure_progress_ratio'] is None


def test_stored_body_survives_retry_and_matches_global_not_complete_state(tmp_path):
    status = _seed(tmp_path)
    status['provider_stored_body_counter_contract_version'] = 1
    status['articles_with_stored_body'] = 3
    status['providers']['cnyes']['stored_body_urls'] = 3
    status['providers']['unregistered:example.org']['stored_body_urls'] = 0
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(p for p in result['providers'] if p['id'] == 'cnyes')
    assert result['snapshot_consistent']
    assert source['stored_body_urls'] == 3 and source['complete_urls'] == 2
    assert source['stored_body_progress_ratio'] == 3 / 6
    assert result['acquisition']['progress_ratio'] == 3 / 10
    assert sum(p['stored_body_urls'] for p in result['providers']) == 3


@pytest.mark.parametrize('value', [True, -1, 7, None, '2', 2.5])
def test_invalid_or_missing_provider_head_counter_invalidates_estimates(tmp_path, value):
    status = _seed(tmp_path)
    status['provider_stored_body_counter_contract_version'] = 1
    status['providers']['cnyes']['stored_body_urls'] = value
    status['providers']['unregistered:example.org']['stored_body_urls'] = 0
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert not result['snapshot_consistent'] and result['health'] == 'degraded'
    assert result['acquisition']['progress_ratio'] is result['acquisition']['eligible_eta_seconds'] is None
    assert all(p['conditional_eta_seconds'] is None for p in result['providers'])


def test_legacy_heartbeat_does_not_infer_stored_bodies_from_complete_states(tmp_path):
    _seed(tmp_path)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['snapshot_consistent'] and result['provider_stored_body_counter_contract_version'] is None
    assert all(p['stored_body_urls'] is None and not p['stored_body_counter_available'] for p in result['providers'])
    assert all(secret not in json.dumps(result) for secret in ('PRIVATE_BODY', 'PRIVATE_TOKEN', str(tmp_path)))

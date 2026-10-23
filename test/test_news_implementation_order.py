"""The selected work order retains every source and never enables transport."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts import plan_news_sources as planner
from scripts.plan_news_sources import plan_sources


POLICY = {'contract_version': 1, 'focus_provider': 'cnyes',
          'provider_order': 'known_urls_desc', 'deferred_providers': ['cna'],
          'retain_running_collectors': True}

PARALLEL_POLICY = {**POLICY, 'contract_version': 2,
                  'background_provider_order': ['medium', 'small', 'large'],
                  'background_execution': 'parallel_while_focus_waits',
                  'focus_preemption': 'return_on_verified_operational_blocker'}


def inventory():
    def source(known, stored=0, role='publisher_or_other_unverified', **extra):
        return {'source_role': role, 'known_urls': known, 'stored_bodies': stored, **extra}
    return {'observed_at_utc': '2026-10-12T05:35:00Z', 'queue_owner_id': 'existing-owner', 'providers': {
        'small': source(5), 'cnyes': source(100, 95, enabled=True),
        'cna': source(999999), 'large': source(999), 'medium': source(301),
        'all_stored': source(10000, 10000, enabled=True),
        'redirect': source(1200000, role='redirector'),
        'platform': source(2000, role='social_or_video'),
        'gov': source(4, role='government_origin'), 'fed_board': source(20, role='government_origin'),
        'literal': source(0, original_metadata={'literal_metadata_links': 2000000}),
        'unknown': source(0), 'tie_a': source(301), 'tie_z': source(301),
    }}


def test_selected_order_focus_then_descending_counts_not_old_small_first():
    result = plan_sources(inventory(), work_order=POLICY)
    assert result['contract_version'] == 6 and result['work_order_contract_version'] == 1
    rows = result['sources']
    assert rows[0]['provider'] == 'cnyes' and rows[-1]['provider'] == 'cna'
    phase1 = [row for row in rows if row['work_phase'] == 1]
    assert [row['known_urls'] for row in phase1] == sorted([row['known_urls'] for row in phase1], reverse=True)
    assert result['body_downloader_implementation_order'] == [
        'cnyes', 'large', 'medium', 'tie_a', 'tie_z', 'fed_board', 'small', 'gov']
    assert result['implementation_order'][0] == 'cnyes'
    assert 'cna' not in result['implementation_order'] and 'all_stored' not in result['implementation_order']
    assert not result['running_collectors_stopped'] and not result['work_order_grants_source_access']
    assert result['publisher_requests'] == 0 and not result['queue_writes']
    assert all(row['finish_eta_utc'] is None and not row['all_publisher_history_complete'] for row in rows)


def test_work_order_preserves_source_facts_grain_and_all_keys():
    before = inventory()
    original = deepcopy(before)
    legacy = plan_sources(before)
    selected = plan_sources(before, work_order=POLICY)
    assert before == original and selected['all_source_keys_planned']
    assert selected['source_count'] == len(before['providers'])
    assert selected['known_urls'] == legacy['known_urls']
    assert selected['stored_bodies'] == legacy['stored_bodies']
    assert selected['literal_metadata_links'] == legacy['literal_metadata_links']
    old = {row['provider']: row for row in legacy['sources']}
    for row in selected['sources']:
        assert all(row[key] == value for key, value in old[row['provider']].items())
    assert not selected['plan_is_acquisition_acceptance'] and not selected['all_history_complete']
    by_name = {row['provider']: row for row in selected['sources']}
    assert by_name['cna']['implementation_state'] == 'deferred_by_user'
    assert by_name['cna']['implementation_rank'] is None
    assert by_name['all_stored']['implementation_state'] == 'maintenance_only'
    assert by_name['literal']['known_urls'] == 0
    assert by_name['literal']['minimum_html_article_requests_if_no_bulk'] is None
    assert by_name['redirect']['strategy'] == 'resolve_original_publisher_and_deduplicate'
    assert not by_name['redirect']['body_downloader_candidate']
    assert not by_name['platform']['body_downloader_candidate']


def test_config_matches_user_selected_order():
    assert json.loads(Path('configs/news/work_order.json').read_bytes()) == {
        **PARALLEL_POLICY, 'background_provider_order': [
            'cmoney', 'udn', 'globalvoices', 'wantmedia', 'money_link', 'cmnews', 'moneydj', 'ltn']}


def test_parallel_background_order_is_attention_not_permission_or_global_http_gate():
    before = inventory()
    original = deepcopy(before)
    result = plan_sources(before, work_order=PARALLEL_POLICY)
    assert result['contract_version'] == 7 and result['work_order_contract_version'] == 2
    assert result['background_attention_order'] == ['medium', 'small', 'large']
    assert [row['provider'] for row in result['sources'][:4]] == ['cnyes', 'medium', 'small', 'large']
    assert not result['background_waits_for_focus_completion']
    assert result['focus_preemption'] == 'return_on_verified_operational_blocker'
    assert result['dispatch_scope'] == 'engineering_only_existing_collectors_remain_independent'
    assert result['background_access_is_not_granted_by_priority']
    assert not result['running_collectors_stopped'] and not result['work_order_grants_source_access']
    assert before == original and result['source_count'] == len(before['providers'])
    assert result['publisher_requests'] == 0 and not result['queue_writes']
    assert result['sources'][-1]['provider'] == 'cna'
    for rank, row in enumerate(result['sources'][1:4], 1):
        assert row['engineering_execution'] == 'parallel_while_focus_waits'
        assert row['background_attention_rank'] == rank
        assert not row['enabled']  # Ordering a disabled source never enables it.


def test_parallel_policy_preserves_every_legacy_source_fact_and_completed_maintenance():
    legacy = {row['provider']: row for row in plan_sources(inventory())['sources']}
    result = plan_sources(inventory(), work_order=PARALLEL_POLICY)
    for row in result['sources']:
        assert all(row[key] == value for key, value in legacy[row['provider']].items())
    remaining = [row for row in result['sources'] if row['work_phase'] == 2]
    assert [row['known_urls'] for row in remaining] == sorted(
        (row['known_urls'] for row in remaining), reverse=True)
    assert 'all_stored' not in result['implementation_order']
    assert not result['all_history_complete'] and not result['plan_is_acquisition_acceptance']


@pytest.mark.parametrize('field,value', [
    ('background_provider_order', []), ('background_provider_order', 'small'),
    ('background_provider_order', ['small', 'small']), ('background_provider_order', ['cna']),
    ('background_provider_order', ['cnyes']), ('background_provider_order', ['absent']),
    ('background_provider_order', [True]), ('background_execution', 'wait_until_complete'),
    ('focus_preemption', False), ('focus_preemption', 'cancel_inflight_requests'),
    ('retain_running_collectors', False), ('contract_version', True),
])
def test_invalid_parallel_policy_rejected(field, value):
    with pytest.raises(ValueError):
        plan_sources(inventory(), work_order={**PARALLEL_POLICY, field: value})


@pytest.mark.parametrize('field', ['background_provider_order', 'background_execution', 'focus_preemption'])
def test_parallel_semantics_must_be_complete_and_never_ignored(field):
    incomplete = dict(PARALLEL_POLICY)
    incomplete.pop(field)
    with pytest.raises(ValueError):
        plan_sources(inventory(), work_order=incomplete)
    with pytest.raises(ValueError):
        plan_sources(inventory(), work_order={**POLICY, field: PARALLEL_POLICY[field]})


@pytest.mark.parametrize('field,value', [
    ('contract_version', True), ('contract_version', 2), ('focus_provider', ''),
    ('focus_provider', 'cna'), ('focus_provider', 'absent'), ('provider_order', 'small_first'),
    ('deferred_providers', 'cna'), ('deferred_providers', ['cna', 'cna']),
    ('deferred_providers', ['cna', False]), ('retain_running_collectors', False),
    ('retain_running_collectors', 1),
])
def test_invalid_or_ambiguous_order_rejected(field, value):
    with pytest.raises(ValueError):
        plan_sources(inventory(), work_order={**POLICY, field: value})


@pytest.mark.parametrize('bad', [[], {}, {**POLICY, 'unused_knob': 123}])
def test_no_ignored_policy_or_missing_semantic_fields(bad):
    with pytest.raises(ValueError):
        plan_sources(inventory(), work_order=bad)


def test_completed_review_is_reused_without_hiding_missing_bodies_or_partial_review():
    before = inventory()
    original = deepcopy(before)
    reviews = {
        'large': {'capture_decision': 'permission_required'},
        'medium': {'capture_decision': 'review_incomplete'},
        'small': {'capture_decision': 'private_public_candidate'},
        'all_stored': {'capture_decision': 'reviewed_existing_scope'},
        'literal': {'capture_decision': 'permission_required'},
        'cna': {'capture_decision': 'authorized_api_configuration_required'},
    }
    result = plan_sources(before, reviews=reviews, work_order=PARALLEL_POLICY)
    by_name = {r['provider']: r for r in result['sources']}
    assert result['review_resume_contract_version'] == 2
    assert by_name['large']['implementation_state'] == 'waiting_source_permission'
    assert by_name['large']['strategy'] == 'retain_completed_review_wait_for_source_permission'
    assert by_name['large']['missing_body_urls'] == 999
    assert by_name['large']['source_review_work_state'] == 'completed_waiting_permission'
    assert by_name['large']['recorded_review_stage_complete']
    assert by_name['medium']['source_review_work_state'] == 'partially_completed'
    assert not by_name['medium']['recorded_review_stage_complete']
    assert by_name['small']['strategy'] == 'verify_remaining_adapter_within_recorded_scope'
    assert by_name['small']['implementation_action'] == 'verify_remaining_shared_adapter_within_recorded_scope'
    assert by_name['literal']['implementation_state'] == 'waiting_source_permission'
    assert not by_name['large']['body_downloader_candidate']
    assert 'large' in result['implementation_order']  # Still unfinished, never silently removed.
    assert 'large' not in result['actionable_implementation_order']
    assert 'large' not in result['body_downloader_implementation_order']
    assert result['waiting_source_permission_keys'] == ['large', 'literal']
    assert result['remaining_source_review_order'][0] == 'medium'
    for key in ('small', 'all_stored', 'large', 'literal', 'cna', 'redirect', 'platform'):
        assert key not in result['remaining_source_review_order']
    assert result['completed_review_is_not_completed_body_history']
    assert not result['all_history_complete']
    assert all(not r['review_annotation_grants_capture_access'] for r in result['sources'])
    assert before == original


def test_only_unfinished_questions_are_projected_without_promoting_partial_source_completion():
    reviews = {'medium': {'capture_decision': 'review_incomplete', 'current_scope_review': {
        'completed_questions': ['headline index is not a full article'],
        'remaining_questions': ['current actual archive and permitted export'],
    }}}
    original = deepcopy(reviews)
    result = plan_sources(inventory(), reviews=reviews, work_order=PARALLEL_POLICY)
    rows = {r['provider']: r for r in result['sources']}
    row = rows['medium']
    assert result['review_resume_contract_version'] == 2
    assert row['review_question_inventory_available']
    assert json.loads(row['completed_review_questions_json']) == ['headline index is not a full article']
    assert json.loads(row['remaining_review_questions_json']) == ['current actual archive and permitted export']
    assert not row['review_questions_are_exhaustive_body_work']
    assert row['source_review_work_state'] == 'partially_completed'
    assert not row['recorded_review_stage_complete'] and not row['review_annotation_grants_capture_access']
    assert row['missing_body_urls'] == 301 and 'medium' in result['remaining_source_review_order']
    assert not rows['large']['review_question_inventory_available']
    assert json.loads(rows['large']['remaining_review_questions_json']) == []
    assert rows['large']['source_review_work_state'] == 'not_started'
    assert not result['all_history_complete'] and reviews == original


@pytest.mark.parametrize('scope', [
    None, [], {'completed_questions': 'not a list'}, {'remaining_questions': [True]},
    {'remaining_questions': ['']}, {'remaining_questions': [' ']},
    {'remaining_questions': ['private\nquestion']}, {'remaining_questions': ['x' * 801]},
    {'remaining_questions': [str(i) for i in range(65)]},
    {'completed_questions': ['same', 'same']},
    {'remaining_questions': ['same', 'same']},
    {'completed_questions': ['same'], 'remaining_questions': ['same']},
])
def test_ambiguous_or_unbounded_review_question_inventory_fails_without_private_error_text(scope):
    with pytest.raises(ValueError, match='^invalid recorded source review questions$'):
        planner.review_resume_plan({}, {'capture_decision': 'review_incomplete', 'current_scope_review': scope})


def test_review_observations_never_disable_existing_collector_or_reopen_completed_work():
    before = inventory()
    result = plan_sources(before, reviews={
        'cnyes': {'capture_decision': 'permission_required'},
        'all_stored': {'capture_decision': 'permission_required'},
    }, work_order=PARALLEL_POLICY)
    by_name = {r['provider']: r for r in result['sources']}
    assert by_name['cnyes']['implementation_state'] == 'repair_or_backfill_known_urls'
    assert by_name['cnyes']['implementation_action'] == 'reuse_existing_pipeline'
    assert by_name['cnyes']['enabled'] and result['actionable_implementation_order'][0] == 'cnyes'
    assert by_name['all_stored']['implementation_state'] == 'maintenance_only'
    assert 'all_stored' not in result['actionable_implementation_order']
    assert 'all_stored' not in result['waiting_source_permission_keys']
    assert not result['running_collectors_stopped']


def _cli_inputs(tmp_path):
    config = tmp_path / 'config.json'
    catalog = tmp_path / 'catalog.json'
    reviews = tmp_path / 'reviews.json'
    observed = tmp_path / 'inventory.json'
    output = tmp_path / 'plan.json'
    atomic_write_json(config, {'providers': [], 'provider_catalog': 'catalog.json'})
    atomic_write_json(catalog, {'contract_version': 1, 'providers': [],
                              'planning_review_ledger': 'reviews.json'})
    atomic_write_json(reviews, {'contract_version': 1, 'providers': {
        'large': {'capture_decision': 'permission_required',
                  'evidence_urls': ['https://publisher.test/terms'],
                  'reviewed_at_utc': '2026-10-20T14:00:00Z'}}})
    atomic_write_json(observed, inventory())
    return config, catalog, reviews, observed, output


def test_cli_automatically_reuses_catalog_ledger_with_inputs_bound_and_no_source_io(tmp_path, monkeypatch):
    import httpx
    import sqlite3
    config, catalog, reviews, observed, output = _cli_inputs(tmp_path)
    def forbidden(*args, **kwargs):
        raise AssertionError('read-only snapshot planning must not open HTTP or SQLite')
    monkeypatch.setattr(httpx, 'Client', forbidden)
    monkeypatch.setattr(sqlite3, 'connect', forbidden)
    before = {str(p): sha256_file(p) for p in (config, catalog, reviews, observed)}
    assert planner.main(['--config', str(config), '--inventory', str(observed),
                         '--output', str(output)]) == 0
    result = json.loads(output.read_bytes())
    by_name = {r['provider']: r for r in result['sources']}
    assert result['review_ledger_selection'] == 'configured_catalog'
    assert by_name['large']['source_policy_review'] == 'permission_required'
    assert 'large' in result['completed_review_source_keys']
    assert 'large' not in result['remaining_source_review_order']
    assert result['planning_inputs_sha256'][str(reviews)] == before[str(reviews)]
    producer = Path(planner.__file__).resolve()
    assert result['planning_inputs_sha256'][str(producer)] == sha256_file(producer)
    assert result['input_sha256'] == before[str(observed)]
    assert before == {str(p): sha256_file(p) for p in (config, catalog, reviews, observed)}
    assert output.with_suffix('.csv').is_file()


def test_explicit_review_snapshot_override_remains_explicit_and_bound(tmp_path):
    config, _, _, observed, output = _cli_inputs(tmp_path)
    explicit = tmp_path / 'other-review.json'
    atomic_write_json(explicit, {'contract_version': 1, 'providers': {
        'large': {'capture_decision': 'review_incomplete', 'evidence_urls': []}}})
    assert planner.main(['--config', str(config), '--reviews', str(explicit),
                         '--inventory', str(observed), '--output', str(output)]) == 0
    result = json.loads(output.read_bytes())
    assert result['review_ledger_selection'] == 'explicit'
    assert result['planning_inputs_sha256'][str(explicit)] == sha256_file(explicit)
    assert next(r for r in result['sources'] if r['provider'] == 'large')[
        'source_review_work_state'] == 'partially_completed'


@pytest.mark.parametrize('invalid', ['missing', 'malformed', 'outside_catalog'])
def test_bad_configured_review_never_silently_becomes_unreviewed_work(tmp_path, invalid):
    config, catalog, reviews, observed, output = _cli_inputs(tmp_path)
    if invalid == 'missing':
        reviews.unlink()
    elif invalid == 'malformed':
        atomic_write_json(reviews, {'contract_version': 1, 'providers': []})
    else:
        atomic_write_json(catalog, {'contract_version': 1, 'providers': [],
                                  'planning_review_ledger': '../outside.json'})
    with pytest.raises(ValueError):
        planner.main(['--config', str(config), '--inventory', str(observed), '--output', str(output)])
    assert not output.exists() and not output.with_suffix('.csv').exists()


def test_review_changed_during_planning_is_rejected_before_publication(tmp_path, monkeypatch):
    config, _, reviews, observed, output = _cli_inputs(tmp_path)
    original = planner.plan_sources
    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        value = json.loads(reviews.read_bytes())
        value['providers']['large']['capture_decision'] = 'review_incomplete'
        atomic_write_json(reviews, value)
        return result
    monkeypatch.setattr(planner, 'plan_sources', changed)
    with pytest.raises(ValueError, match='review ledger changed'):
        planner.main(['--config', str(config), '--inventory', str(observed), '--output', str(output)])
    assert not output.exists()


def test_legacy_cli_without_catalog_keeps_unknown_review_explicit(tmp_path):
    _, _, _, observed, output = _cli_inputs(tmp_path)
    assert planner.main(['--inventory', str(observed), '--output', str(output)]) == 0
    result = json.loads(output.read_bytes())
    assert result['review_ledger_selection'] == 'none'
    assert next(r for r in result['sources'] if r['provider'] == 'large')[
        'source_review_work_state'] == 'not_started'

"""The selected work order retains every source and never enables transport."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from scripts.plan_news_sources import plan_sources


POLICY = {'contract_version': 1, 'focus_provider': 'cnyes',
          'provider_order': 'known_urls_desc', 'deferred_providers': ['cna'],
          'retain_running_collectors': True}


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
    assert json.loads(Path('configs/news/work_order.json').read_bytes()) == POLICY


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

"""Resolved local originals are work, not bodies, unique stories or permission."""
import pytest

from scripts.plan_news_sources import plan_sources


def inventory(*, known=0, stored=0, links=38, role='government_origin', enabled=False):
    return {'observed_at_utc': '2026-10-11T21:13:12+00:00',
            'inventory_scope_contract_version': 1, 'inventory_scope': 'all_known_source_keys',
            'all_known_source_keys_included': True, 'selected_source_keys': None, 'providers': {
        'unregistered:official.test': {
            'source_role': role, 'known_urls': known, 'stored_bodies': stored,
            'original_metadata': {'literal_metadata_links': links}, 'enabled': enabled,
        },
    }}


@pytest.mark.parametrize('links,stage,small', [(1, 1, True), (38, 1, True), (300, 1, True), (301, 2, False)])
def test_metadata_only_source_has_real_pending_review_not_zero_work(links, stage, small):
    result = plan_sources(inventory(links=links))
    row = result['sources'][0]
    assert result['contract_version'] == 11 and result['all_source_keys_planned']
    assert row['strategy'] == 'review_then_route_existing_literal_original_links'
    assert row['stage'] == stage
    assert row['literal_metadata_links'] == links and row['unqueued_literal_link_work']
    assert row['small_metadata_link_candidate'] is small
    assert row['known_urls'] == row['stored_bodies'] == row['missing_body_urls'] == 0
    assert row['minimum_html_article_requests_if_no_bulk'] is None
    assert row['finish_eta_utc'] is None
    assert not row['small_cohort_candidate'] and not row['enabled']
    assert not row['metadata_dates_are_publisher_clocks']
    assert not row['all_publisher_history_complete'] and not result['plan_is_acquisition_acceptance']


def test_literal_associations_do_not_inflate_existing_frontier_or_body_counts():
    result = plan_sources(inventory(known=10, stored=3, links=20000, enabled=True))
    row = result['sources'][0]
    assert row['strategy'] == 'reuse_active_canonical_collector'
    assert row['known_urls'] == 10 and row['stored_bodies'] == 3 and row['missing_body_urls'] == 7
    assert row['minimum_html_article_requests_if_no_bulk'] is None
    assert row['unqueued_literal_link_work'] is None
    assert not row['literal_target_coverage_verified']
    assert result['unknown_literal_target_coverage_source_keys'] == 1
    assert result['literal_metadata_links'] == 20000
    assert result['known_urls'] == 10 and result['stored_bodies'] == 3


def test_terminal_missing_bodies_are_not_promoted_to_required_http_requests():
    value = inventory(known=7, stored=0, links=0, enabled=True)
    item = value['providers']['unregistered:official.test']
    item['missing_body_work'] = 0
    item['states'] = {'pdf_image_only_or_body_not_found': 4, 'source_document_landing_page': 3}
    row = plan_sources(value)['sources'][0]
    assert row['missing_body_urls'] == 7
    assert row['queued_or_inflight_missing_body_urls'] == 0
    assert row['other_missing_body_urls'] == 7
    assert row['minimum_html_article_requests_if_no_bulk'] is None
    assert row['finish_eta_utc'] is None


def test_queue_state_count_is_not_a_dispatchable_or_request_count():
    value = inventory(known=10, stored=3, links=20000, enabled=True)
    value['providers']['unregistered:official.test']['missing_body_work'] = 2
    row = plan_sources(value)['sources'][0]
    assert (row['missing_body_urls'], row['queued_or_inflight_missing_body_urls'],
            row['other_missing_body_urls']) == (7, 2, 5)
    assert row['minimum_html_article_requests_if_no_bulk'] is None
    assert row['finish_eta_utc'] is None
    assert row['queued_or_inflight_missing_body_scope'] == (
        'snapshot states pending, retry, retry_cooldown or inflight without a body; '
        'not a readiness, permission, source request or completion guarantee')


def test_legacy_inventory_without_queue_state_count_keeps_partition_unknown():
    row = plan_sources(inventory(known=10, stored=3, links=0))['sources'][0]
    assert row['missing_body_urls'] == 7
    assert row['queued_or_inflight_missing_body_urls'] is None
    assert row['other_missing_body_urls'] is None
    assert row['minimum_html_article_requests_if_no_bulk'] is None


@pytest.mark.parametrize('field,value', [
    ('metadata_sort_days_present', True), ('metadata_sort_days_present', 11),
    ('metadata_sort_days_invalid', -1), ('metadata_sort_days_invalid', 6),
    ('metadata_sort_days_pre_1900', 6), ('metadata_sort_days_after_snapshot_utc', '1'),
])
def test_invalid_sort_day_quality_counts_are_not_projected(field, value):
    source = inventory(known=10, stored=3, links=0)
    source.update(metadata_sort_day_quality_requested=True, metadata_sort_day_quality_contract_version=1)
    item = source['providers']['unregistered:official.test']
    item.update(metadata_sort_days_present=5, metadata_sort_days_invalid=1,
                metadata_sort_days_pre_1900=1, metadata_sort_days_after_snapshot_utc=1)
    item[field] = value
    with pytest.raises(ValueError, match='metadata sort-day quality counts'):
        plan_sources(source)


def test_disjoint_sort_day_flags_cannot_exceed_present():
    source = inventory(known=10, stored=3, links=0)
    source.update(metadata_sort_day_quality_requested=True, metadata_sort_day_quality_contract_version=1)
    item = source['providers']['unregistered:official.test']
    item.update(metadata_sort_days_present=5, metadata_sort_days_invalid=1,
                metadata_sort_days_pre_1900=3, metadata_sort_days_after_snapshot_utc=2)
    with pytest.raises(ValueError, match='inconsistent metadata sort-day quality counts'):
        plan_sources(source)


@pytest.mark.parametrize('requested,version', [(True, None), (True, 2), (True, True), (False, 1), ('yes', 1)])
def test_unrecognized_sort_day_quality_contract_is_rejected(requested, version):
    source = inventory(known=10, stored=3, links=0)
    source.update(metadata_sort_day_quality_requested=requested, metadata_sort_day_quality_contract_version=version)
    with pytest.raises(ValueError, match='metadata sort-day quality'):
        plan_sources(source)


@pytest.mark.parametrize('count', [True, -1, 8, 1.5, '2'])
def test_invalid_missing_body_queue_count_cannot_create_false_progress(count):
    value = inventory(known=10, stored=3, links=0)
    value['providers']['unregistered:official.test']['missing_body_work'] = count
    with pytest.raises(ValueError, match='invalid inventory missing-body work count'):
        plan_sources(value)


@pytest.mark.parametrize('role,strategy', [
    ('redirector', 'resolve_original_publisher_and_deduplicate'),
    ('aggregator', 'resolve_original_publisher_and_deduplicate'),
    ('social_or_video', 'platform_supported_export_or_public_original_link'),
])
def test_metadata_cannot_override_platform_source_or_rights_boundaries(role, strategy):
    result = plan_sources(inventory(role=role))
    assert result['sources'][0]['strategy'] == strategy
    assert result['sources'][0]['stored_bodies'] == 0


def test_missing_legacy_original_counts_stay_unknown_without_invented_work():
    value = inventory(links=0)
    del value['providers']['unregistered:official.test']['original_metadata']
    row = plan_sources(value)['sources'][0]
    assert row['strategy'] == 'no_local_urls_review_discovery_only_if_useful'
    assert row['literal_metadata_links'] == 0 and not row['unqueued_literal_link_work']


@pytest.mark.parametrize('links', [True, -1, 1.5, '38', None])
def test_invalid_alias_counts_are_rejected(links):
    with pytest.raises(ValueError, match='invalid original metadata link counts'):
        plan_sources(inventory(links=links))


def coverage(*, unqueued=1):
    return {'contract_version': 1, 'alias_associations': 20000,
            'distinct_target_urls': 3, 'queued_target_urls': 3 - unqueued,
            'stored_body_target_urls': 1, 'unqueued_target_urls': unqueued}


@pytest.mark.parametrize('unqueued', [0, 1])
def test_complete_existing_heads_do_not_hide_verified_unqueued_targets(unqueued):
    from test_news_implementation_order import POLICY
    value = inventory(known=10, stored=10, links=20000, enabled=True)
    value['providers']['unregistered:official.test']['original_metadata']['target_url_coverage'] = coverage(unqueued=unqueued)
    result = plan_sources(value, work_order={**POLICY, 'focus_provider': 'unregistered:official.test'})
    row = result['sources'][0]
    assert row['literal_target_coverage_verified']
    assert row['distinct_target_urls'] == 3 and row['unqueued_target_urls'] == unqueued
    assert row['unqueued_literal_link_work'] is bool(unqueued)
    assert row['implementation_state'] == ('review_literal_original_links' if unqueued else 'maintenance_only')
    assert row['known_urls'] == row['stored_bodies'] == 10
    assert result['unqueued_literal_link_source_keys'] == unqueued
    assert not result['unknown_literal_target_coverage_source_keys']


def test_complete_existing_heads_with_unmeasured_overlap_remain_pending_verification():
    from test_news_implementation_order import POLICY
    result = plan_sources(inventory(known=10, stored=10, links=20000, enabled=True),
                          work_order={**POLICY, 'focus_provider': 'unregistered:official.test'})
    row = result['sources'][0]
    assert row['unqueued_literal_link_work'] is None
    assert row['implementation_state'] == 'verify_literal_target_coverage'
    assert row['provider'] in result['implementation_order']
    assert result['unknown_literal_target_coverage_source_keys'] == 1
    assert result['unqueued_literal_link_source_keys'] == 0


@pytest.mark.parametrize('change', [
    {'contract_version': True}, {'alias_associations': 19999}, {'distinct_target_urls': 20001},
    {'queued_target_urls': 4}, {'stored_body_target_urls': 3}, {'unqueued_target_urls': 0},
    {'distinct_target_urls': True}, {'unqueued_target_urls': -1},
])
def test_invalid_exact_coverage_cannot_mark_literal_work_complete(change):
    value = inventory(known=10, stored=3, links=20000)
    value['providers']['unregistered:official.test']['original_metadata']['target_url_coverage'] = coverage() | change
    with pytest.raises(ValueError, match='literal target coverage counts'):
        plan_sources(value)


def test_explicit_null_coverage_is_not_a_successful_or_absent_observation():
    value = inventory()
    value['providers']['unregistered:official.test']['original_metadata']['target_url_coverage'] = None
    with pytest.raises(ValueError, match='literal target coverage counts'):
        plan_sources(value)

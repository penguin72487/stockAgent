"""Resolved local originals are work, not bodies, unique stories or permission."""
import pytest

from scripts.plan_news_sources import plan_sources


def inventory(*, known=0, stored=0, links=38, role='government_origin', enabled=False):
    return {'observed_at_utc': '2026-10-11T21:13:12+00:00', 'providers': {
        'unregistered:official.test': {
            'source_role': role, 'known_urls': known, 'stored_bodies': stored,
            'original_metadata': {'literal_metadata_links': links}, 'enabled': enabled,
        },
    }}


@pytest.mark.parametrize('links,stage,small', [(1, 1, True), (38, 1, True), (300, 1, True), (301, 2, False)])
def test_metadata_only_source_has_real_pending_review_not_zero_work(links, stage, small):
    result = plan_sources(inventory(links=links))
    row = result['sources'][0]
    assert result['contract_version'] == 5 and result['all_source_keys_planned']
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
    assert row['minimum_html_article_requests_if_no_bulk'] == 7
    assert not row['unqueued_literal_link_work']
    assert result['literal_metadata_links'] == 20000
    assert result['known_urls'] == 10 and result['stored_bodies'] == 3


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

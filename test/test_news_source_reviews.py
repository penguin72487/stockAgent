"""Review observations never become source permissions or collector revisions."""
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path

import pytest

from downloader.artifact_io import atomic_write_json
from stockagent.data.news_source_catalog import (
    configuration_revision, load_news_configuration, load_source_review_ledger,
    planning_review_path,
)
from stockagent.live.news_dashboard import build_news_public_status, news_snapshot_revision
from test_news_dashboard import NOW, _seed


def _ledger():
    return {"contract_version": 1, "providers": {
        "moneydj": {"capture_decision": "permission_required",
                    "evidence_urls": ["https://example.org/terms"],
                    "observation": "PRIVATE_REVIEW_BODY_AND_KEY",
                    "reviewed_at_utc": NOW.isoformat()}},
        "provider_aliases": {"unregistered:example.org": "moneydj"}}


def test_inews_native_progress_does_not_promote_a_review_to_permission_or_whole_history():
    from scripts.plan_news_sources import review_resume_plan

    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))[
        'unregistered:inews.setn.com']
    scope = review['current_scope_review']
    assert review['capture_decision'] == 'review_incomplete'
    stage,live=scope['native_v8_review'],scope['live_canonical_snapshot']
    assert scope['known_URL_heads']==live['known_urls']==1636
    assert scope['stored_body_heads']==stage['accepted_body_heads']==live['stored_body_urls']
    assert stage['old_verified_heads_reused']+stage['new_verified_heads']==stage['accepted_body_heads']
    assert 0<stage['accepted_body_heads']<scope['known_URL_heads']
    assert stage['later_missing_only_targets']==stage['later_checked_after_admission']==sum(stage['later_outcomes'].values())
    assert sum(stage['remaining_states'].values())==scope['known_URL_heads']
    assert stage['prior_closed_questions_preserved']+stage['new_geometry_questions']+stage['new_positive_original_questions']==stage['current_closed_questions']
    assert stage['retained_originals']+stage['unretained_original_outcomes']==stage['new_positive_original_questions']
    assert stage['authorization_and_HTTP_control_identity_unchanged']
    assert stage['old_body_pins_redecoded_for_delta']==stage['source_HTTP_requests_by_readback']==stage['protected_service_restarts']==0
    assert not any(stage[key] for key in ('source_maximum_verified','whole_history_complete','whole_history_ETA_verified'))
    assert scope['collector_native_known_URL_scope_enabled'] and not scope['protected_service_restarts']
    assert not scope['source_authorization_granted'] and not scope['whole_publisher_history_complete']
    assert not scope['source_maximum_verified'] and not scope['official_numeric_rate_limit_verified']
    result = review_resume_plan({'enabled': False}, review)
    assert result['source_review_work_state'] == 'partially_completed'
    assert not result['recorded_review_stage_complete'] and not result['review_annotation_grants_capture_access']
    assert json.loads(result['completed_review_questions_json']) == scope['completed_questions']
    assert json.loads(result['remaining_review_questions_json']) == scope['remaining_questions']
    assert before == (configuration_revision(path), load_news_configuration(path))


@pytest.mark.parametrize('provider,decision,heads,hosts,new_bodies,restarts', [
    ('gvm', 'reviewed_existing_scope', 534, 4, 2, 1),
    ('mirrordaily', 'permission_required', 252, 1, 0, 0),
])
def test_review_reading_preserves_separately_admitted_or_disabled_source_configuration(
        provider, decision, heads, hosts, new_bodies, restarts):
    from scripts.plan_news_sources import review_resume_plan

    config_path = Path('configs/news_fulltext.json')
    before = configuration_revision(config_path), load_news_configuration(config_path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))[provider]
    scope = review['current_scope_review']
    assert review['capture_decision'] == decision
    assert (scope['known_URL_heads'], scope['observed_hosts']) == (heads, hosts)
    assert scope['new_private_originals'] == 0
    assert scope['stored_body_heads'] == scope['new_formal_bodies'] == new_bodies
    assert scope['services_restarted'] == restarts
    assert not scope['source_authorization_granted'] and not scope['whole_publisher_history_complete']
    state = review_resume_plan({'enabled': False}, review)
    assert state['recorded_review_stage_complete'] and not state['review_annotation_grants_capture_access']
    assert not state['review_questions_are_exhaustive_body_work']
    assert json.loads(state['completed_review_questions_json']) == scope['completed_questions']
    assert json.loads(state['remaining_review_questions_json']) == scope['remaining_questions']
    entry = next(p for p in before[1]['providers'] if p['id'] == provider)
    if provider == 'gvm':
        assert entry['enabled'] and entry['authorization']['kind'] == 'personal_research'
        assert entry['acquisition_mode'] == 'finite' and len(entry['finite_article_urls']) == 2
    else:
        assert not entry['enabled'] and entry['authorization']['kind'] == 'awaiting_authorization'
    assert before == (configuration_revision(config_path), load_news_configuration(config_path))


def test_gvm_private_pair_is_not_all_subsites_editorial_or_historical_clock_completion():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['gvm']
    scope, diagnostic = review['current_scope_review'], review['diagnostic_snapshot_review']
    assert sum(scope[k] for k in ('main_host_URL_heads', 'ESG_URL_heads', 'event_URL_heads',
        'city_URL_heads')) == scope['known_URL_heads']
    assert (scope['article_HTTP_requests'], scope['private_native_originals']) == (0, 2)
    assert (diagnostic['known_URL_heads'], diagnostic['article_HTTP_requests'],
            diagnostic['stored_body_heads']) == (207, 2, 0)
    assert scope['source_modified_clock_discrepancies'] == 2 and len(scope['remaining_questions']) == 1
    assert scope['derived_canonical_body_records_verified'] and scope['outside_finite_scope_URL_heads'] == 532
    assert scope['finite_cohort_URLs'] == scope['finite_cohort_stored_bodies'] == 2
    assert scope['subsequent_WSL_boot_observed'] and not scope['physical_Windows_cold_boot_claimed']
    assert scope['protected_services_restarted_for_admission'] == 0
    assert any('ABI3' in question for question in scope['completed_questions'])
    assert 'before production activation' not in scope['remaining_questions'][0]
    assert not any(scope[k] for k in ('main_host_review_covers_all_subsites',
        'forwarding_character_limit_is_HTTP_quota', 'private_original_is_accepted_canonical_article',
        'advertorial_is_independent_editorial', 'native_start_time_is_publication',
        'official_numeric_rate_limit_verified'))


def test_production_gvm_http_identity_and_literal_pair_are_exactly_closed():
    from downloader.news_fulltext_gvm import GVM_FINITE_HTTP_BINDING
    from downloader.news_fulltext_http import NewsHTTP
    from downloader.news_fulltext_policy import NewsError, PolicyRegistry

    registry = PolicyRegistry(load_news_configuration(Path('configs/news_fulltext.json')), base=Path('.'))
    source = registry.sources['gvm']
    assert (source.data_fingerprint, source.authorization_fingerprint,
            source.http_capacity_source_fingerprint,
            source.http_capacity_authorization_fingerprint) == GVM_FINITE_HTTP_BINDING
    assert source.finite_article_urls == (
        'https://www.gvm.com.tw/article/132737', 'https://www.gvm.com.tw/article/76333')
    # Transport validation alone: no HTTP owner, connection, or publisher probe.
    transport = object.__new__(NewsHTTP)
    transport.registry = registry
    for url in (*source.finite_article_urls, 'https://www.gvm.com.tw/robots.txt'):
        assert transport._require(url) is source
    for url in ('https://www.gvm.com.tw/article/99999', 'https://www.gvm.com.tw/',
                source.finite_article_urls[0] + '?extra=1'):
        with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
            transport._require(url)


def test_mirrordaily_partial_quote_and_contributor_terms_are_not_fulltext_access():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['mirrordaily']
    scope = review['current_scope_review']
    assert 'https://www.mirrordaily.news/story/5853' in review['evidence_urls']
    assert not any(scope[k] for k in ('personal_link_and_partial_quote_permission_is_fulltext_license',
        'contributor_license_grants_collector_rights', 'MirrorMedia_policy_is_MirrorDaily_policy',
        'policy_statement_is_collector_access_refusal', 'canonical_policy_HTTP_exchanges', 'article_HTTP_requests'))
    assert scope['remaining_questions'] == []


@pytest.mark.parametrize('host,heads,decision,article_requests', [
    ('hk.investing.com', 112, 'permission_required', 0),
    ('cn.investing.com', 7, 'permission_required', 0),
    ('m.hk.investing.com', 10, 'permission_required', 0),
    ('finance.eastmoney.com', 83, 'permission_required', 0),
    ('www.cw.com.tw', 128, 'reviewed_existing_scope', 0),
    ('crossing.cw.com.tw', 5, 'review_incomplete', 0),
    ('csr.cw.com.tw', 4, 'review_incomplete', 0),
])
def test_financial_source_reviews_keep_per_host_counts_and_only_unanswered_work(
        host, heads, decision, article_requests):
    from scripts.plan_news_sources import review_resume_plan

    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['unregistered:'+host]
    scope = review['current_scope_review']
    assert review['capture_decision'] == decision
    assert scope['known_URL_heads'] == heads and scope['observed_hosts'] == 1
    assert scope['article_HTTP_requests'] == article_requests
    assert scope['completed_questions']
    assert not any(scope[k] for k in ('source_authorization_granted',
        'official_numeric_rate_limit_verified', 'whole_publisher_history_complete'))
    if host == 'www.cw.com.tw':
        assert scope['stored_body_heads'] == scope['new_formal_bodies'] == scope['services_restarted'] == 1
        assert scope['new_private_originals'] == 0
    else:
        assert not any(scope[k] for k in ('stored_body_heads', 'new_formal_bodies', 'services_restarted'))
    state = review_resume_plan({'enabled': host == 'www.cw.com.tw'}, review)
    assert not state['review_annotation_grants_capture_access']
    assert json.loads(state['remaining_review_questions_json']) == scope['remaining_questions']
    assert bool(scope['remaining_questions']) is (decision != 'permission_required')
    from scripts.plan_news_sources import SOURCE_TOPICS
    assert SOURCE_TOPICS['unregistered:'+host] in {
        'financial_markets', 'technology_business', 'environment_energy'}
    assert before == (configuration_revision(path), load_news_configuration(path))


def test_current_financial_policies_do_not_conflate_personal_use_or_other_products():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    for host in ('hk.investing.com', 'cn.investing.com', 'm.hk.investing.com'):
        review = reviews['unregistered:'+host]
        scope = review['current_scope_review']
        assert scope['policy_version'] == 'August2026'
        assert not scope['personal_use_clause_grants_automated_extraction']
        assert 'https://cdn.investing.com/about-us/terms_and_conditions.html' in review['evidence_urls']
    eastmoney = reviews['unregistered:finance.eastmoney.com']['current_scope_review']
    assert not eastmoney['Choice_software_personal_use_is_news_capture_grant']
    assert not eastmoney['finance_review_covers_other_Eastmoney_products']


def test_cw_preserves_diagnostics_separately_from_one_admitted_body_and_subsite_work():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    assert reviews['cw'] == reviews['unregistered:www.cw.com.tw']
    review = reviews['cw']
    scope, diagnostic = review['current_scope_review'], review['diagnostic_snapshot_review']
    assert (diagnostic['canonical_policy_HTTP_exchanges'], diagnostic['article_HTTP_requests'],
            diagnostic['private_native_originals']) == (1, 2, 2)
    assert diagnostic['known_URL_heads'] == 79 and diagnostic['stored_body_heads'] == 0
    assert diagnostic['first_local_preflight_source_HTTP_requests'] == 0
    assert diagnostic['same_existing_private_HTTP_root_reused'] and not diagnostic['actual_redirects']
    assert diagnostic['first_original_has_third_party_source_credit']
    assert diagnostic['second_original_current_clock_differs_from_old_metadata']
    assert not any(scope[k] for k in ('private_original_is_accepted_canonical_article',
        'main_native_question_covers_subsite_layouts', 'policy_wording_is_legal_fair_use_finding',
        'public_content_licensing_rules_are_HTTP_quota', 'native_API_requested'))
    assert not any('finish only finite production' in question for question in scope['remaining_questions'])
    assert scope['offline_native_adapter_verified'] and scope['independent_lxml_body_parity']
    assert scope['offline_complete_public_body_candidates'] == 1
    assert scope['offline_excluded_preview_third_party_representations'] == 1
    assert scope['offline_complete_body_characters'] == 10384
    assert scope['canonical_shadow_receipt_reopen_idempotence_verified']
    assert scope['shadow_outside78_heads_preserved'] and scope['offline_verification_source_requests'] == 0
    assert diagnostic['formal_finite_receipt_owner_admission_pending'] and not diagnostic['live_HTTP_fix_reloaded']
    assert not scope['formal_finite_receipt_owner_admission_pending'] and scope['live_HTTP_fix_reloaded']
    assert scope['derived_canonical_record_independently_verified']
    assert scope['finite_cohort_URLs'] == scope['finite_cohort_stored_bodies'] == 1
    assert scope['finite_cohort_complete'] and scope['remaining_outside_finite_cohort_heads'] == 127
    assert scope['known_URL_heads'] == scope['admission_baseline_known_URL_heads'] + scope['new_local_metadata_heads_since_baseline'] == 128
    assert scope['new_local_metadata_states'] == {'waiting_cohort': 47, 'unsupported_article_path': 2}
    assert scope['source_key_lineage'] == {'unregistered:www.cw.com.tw': 'cw'}
    assert scope['old83_source_identities_unchanged'] and scope['old19_GVM2_and217_closed_questions_preserved']
    assert scope['original_origin_controls_preserved'] and scope['protected_services_unchanged']
    assert not scope['article_HTTP_requests'] and not scope['canonical_policy_HTTP_exchanges']
    assert any('10384' in question and 'offline' in question for question in scope['completed_questions'])
    for host in ('crossing.cw.com.tw', 'csr.cw.com.tw'):
        subsite = reviews['unregistered:'+host]['current_scope_review']
        assert not subsite['main_host_native_readback_proves_subsite_access']
        assert subsite['new_private_originals'] == 0


@pytest.mark.parametrize('provider,heads,hosts,decision', [
    ('ctwant', 909, 1, 'permission_required'),
    ('businessweekly', 849, 5, 'review_incomplete'),
    ('knews', 703, 2, 'review_incomplete'),
])
def test_new_source_scope_reviews_resume_only_open_questions_without_capture(provider, heads, hosts, decision):
    from scripts.plan_news_sources import review_resume_plan

    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))[provider]
    if provider in {'businessweekly', 'knews'}:
        # This completed no-HTTP stage remains testable after the explicitly
        # separate native-response stage, rather than rewriting old evidence.
        review = review['diagnostic_snapshot_review']
    assert review['capture_decision'] == decision
    assert review['reviewed_at_utc'] == '2026-10-22T02:38:50+00:00'
    scope = review['current_scope_review']
    assert scope['known_URL_heads'] == heads and scope['observed_hosts'] == hosts
    assert scope['completed_questions']
    assert not any(scope[k] for k in ('stored_body_heads', 'article_HTTP_requests',
        'source_authorization_granted', 'new_formal_bodies', 'new_private_originals',
        'services_restarted', 'whole_publisher_history_complete'))
    state = review_resume_plan({'enabled': False}, review)
    assert state['review_question_inventory_available']
    assert json.loads(state['completed_review_questions_json']) == scope['completed_questions']
    assert json.loads(state['remaining_review_questions_json']) == scope['remaining_questions']
    assert state['recorded_review_stage_complete'] is (decision == 'permission_required')
    assert not state['review_annotation_grants_capture_access']
    assert not state['review_questions_are_exhaustive_body_work']
    entry = next(p for p in before[1]['providers'] if p['id'] == provider)
    if provider == 'knews':
        assert entry['enabled'] and entry['acquisition_mode'] == 'finite'
        assert len(entry['finite_article_urls']) == 2
        assert entry['authorization']['kind'] == 'personal_research'
    else:
        assert not entry['enabled'] and entry['authorization']['kind'] == 'awaiting_authorization'
    assert before == (configuration_revision(path), load_news_configuration(path))


def test_ctwant_real_policy_reachability_is_not_body_permission_or_a_rate_measurement():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['ctwant']
    scope = review['current_scope_review']
    assert sum(scope[k] for k in ('article_route_heads', 'AMP_route_heads_not_proven_aliases',
        'video_route_heads')) == scope['known_URL_heads']
    assert scope['canonical_policy_HTTP_exchanges'] == 3
    assert scope['policy_contains_content_terms'] and scope['browsing_scope_stated_by_current_policy']
    assert not any(scope[k] for k in ('REP_allow_is_content_license',
        'privacy_label_means_no_content_rules', 'research_reader_error_is_collector_refusal',
        'official_numeric_rate_limit_verified'))
    assert scope['remaining_questions'] == []
    assert 'section13 requires prior rightsholder authorization' in review['observation']
    assert 'https://www.ctwant.com/privacy' in review['evidence_urls']


def test_businessweekly_group_host_and_community_grants_remain_separate():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['businessweekly']
    review = review['diagnostic_snapshot_review']
    scope = review['current_scope_review']
    assert sum(scope[k] for k in ('main_host_URL_heads', 'Smart_URL_heads', 'wealth_URL_heads',
        'health_URL_heads', 'smarttest_URL_heads')) == scope['known_URL_heads']
    assert scope['community_host_URL_heads'] == 0
    assert scope['advertising_route_heads_not_verified_articles'] == 25
    assert not scope['community_grant_covers_publisher_news']
    assert not scope['sample_syndication_proves_all_articles_are_originals']
    assert not scope['research_reader_error_is_collector_refusal']
    assert len(scope['remaining_questions']) == 2
    assert 'download-liability disclaimer is not a blanket grant' in review['observation']


def test_businessweekly_literal_native_diagnosis_is_not_body_admission_or_whole_host_completion():
    from scripts.plan_news_sources import review_resume_plan

    config_path = Path('configs/news_fulltext.json')
    before = configuration_revision(config_path), load_news_configuration(config_path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['businessweekly']
    previous = review['diagnostic_snapshot_review']
    review = review['previous_native_diagnostic_review']
    scope = review['current_scope_review']
    assert review['reviewed_at_utc'] > previous['reviewed_at_utc']
    assert previous['current_scope_review']['known_URL_heads'] == 849
    assert scope['known_URL_heads'] == 850
    assert sum(scope[k] for k in ('main_host_URL_heads','wealth_URL_heads','Smart_URL_heads',
        'health_URL_heads','smarttest_URL_heads')) == 850
    assert scope['actual_HTTP_requests'] == scope['robots_HTTP_attempts'] + scope['article_HTTP_requests'] == 6
    assert scope['article_HTTP_requests'] == scope['new_private_original_responses'] == 2
    assert scope['native_original_readback_complete'] and scope['shared_finite_query_ID_regression_tests'] == 323
    assert scope['existing84_production_source_identities_preserved']
    assert scope['query_ID_fix_loaded_by_private_diagnostics']
    assert not any(scope[k] for k in ('canonical_policy_document_HTTP_exchanges',
        'main_host_article_HTTP_requests','main_transport_failure_is_HTTP_refusal',
        'policy_document_actual_response_obtained','Smart_sample_is_publisher_original',
        'stored_body_heads','source_authorization_granted','new_formal_bodies',
        'production_queue_writes','services_restarted','whole_publisher_history_complete',
        'query_ID_fix_live_owner_reloaded'))
    state = review_resume_plan({'enabled':False},review)
    assert state['source_review_work_state'] == 'partially_completed'
    assert not state['recorded_review_stage_complete'] and not state['review_annotation_grants_capture_access']
    assert json.loads(state['completed_review_questions_json']) == scope['completed_questions']
    assert json.loads(state['remaining_review_questions_json']) == scope['remaining_questions']
    assert set(scope['completed_questions']).isdisjoint(scope['remaining_questions'])
    assert len(scope['remaining_questions']) == 4
    assert 'without another source fetch' in scope['remaining_questions'][0]
    assert 'do not repeat' in scope['remaining_questions'][1]
    entry = next(p for p in before[1]['providers'] if p['id']=='businessweekly')
    assert not entry['enabled'] and entry['authorization']['kind']=='awaiting_authorization'
    assert before == (configuration_revision(config_path),load_news_configuration(config_path))


def test_wealth_completed_original_is_not_repeated_or_counted_as_all_group_history():
    from scripts.plan_news_sources import review_resume_plan

    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    parent, child = reviews['businessweekly'], reviews['businessweekly_wealth']
    scope, admitted = parent['current_scope_review'], child['current_scope_review']
    assert parent['capture_decision'] == 'review_incomplete'
    assert scope['known_URL_heads'] == 358 and scope['wealth_URL_heads'] == 0
    assert scope['main_host_URL_heads'] + scope['Smart_URL_heads'] + scope['health_URL_heads'] + scope['smarttest_URL_heads'] == 358
    assert scope['separate_wealth_original_baseline_URL_heads'] + scope['known_URL_heads'] == 850
    assert parent['previous_native_diagnostic_review']['current_scope_review']['known_URL_heads'] == 850
    assert scope['native_adapter_and_exact_wealth_finite_admission_complete']
    assert scope['existing84_production_source_identities_preserved'] and scope['query_ID_fix_live_owner_reloaded']
    assert not scope['parent_source_authorization_granted'] and not scope['main_REP_transport_repaired']
    assert len(scope['remaining_questions']) == 3
    assert child['capture_decision'] == 'reviewed_existing_scope'
    assert admitted['known_URL_heads'] >= 492 and admitted['stored_body_heads'] == admitted['finite_stored_bodies'] == 1
    assert admitted['unreviewed_other_known_URL_heads'] == admitted['known_URL_heads'] - 1
    assert admitted['source_clock_precision'] == 'day'
    assert not any(admitted[k] for k in ('actual_new_publisher_HTTP_requests', 'source_intraday_publication_time_known',
        'historical_point_in_time', 'original_ownership_verified', 'remote_training_export', 'publish', 'whole_publisher_history_complete'))
    state = review_resume_plan({'enabled': True}, child)
    assert state['recorded_review_stage_complete'] and not state['review_annotation_grants_capture_access']
    assert len(json.loads(state['remaining_review_questions_json'])) == 2
    assert before == (configuration_revision(path), load_news_configuration(path))


def test_knews_news_shopping_member_and_search_scopes_are_not_interchangeable():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['knews']
    review = review['diagnostic_snapshot_review']
    scope = review['current_scope_review']
    assert scope['news_route_heads_not_verified_articles'] + scope['search_route_heads'] == scope['known_URL_heads']
    assert scope['www_host_URL_heads'] + scope['bare_host_URL_heads_not_proven_aliases'] == scope['known_URL_heads']
    assert not any(scope[k] for k in ('shopping_terms_are_news_capture_policy',
        'member_terms_apply_to_all_public_nonmembers_verified', 'editorial_corrections_are_capture_permission'))
    assert len(scope['remaining_questions']) == 2
    assert 'no registration, account use, form submission' in review['observation'].lower()


def test_knews_two_native_originals_do_not_close_the703_remaining_URL_heads():
    from scripts.plan_news_sources import review_resume_plan
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['knews']
    scope = review['current_scope_review']
    assert review['capture_decision'] == 'review_incomplete'
    assert review['diagnostic_snapshot_review']['reviewed_at_utc'] < review['reviewed_at_utc']
    assert scope['known_URL_heads'] == scope['www_host_URL_heads'] + scope['bare_host_URL_heads_not_proven_aliases'] == 705
    assert scope['known_URL_heads'] == scope['news_route_heads_not_verified_articles'] + scope['search_route_heads']
    assert scope['stored_body_heads'] == scope['new_formal_bodies'] == scope['finite_reviewed_URLs'] == 2
    assert scope['finite_scope_complete'] and scope['remaining_known_missing_heads'] == 703
    assert scope['canonical_total_HTTP_exchanges'] == scope['canonical_policy_HTTP_exchanges'] + scope['article_HTTP_requests'] == 3
    assert scope['publisher_requests_for_admission'] == 0 and scope['protected_services_restarted'] == 0
    assert scope['offline_native_adapter_verified'] and scope['independent_lxml_body_parity']
    assert scope['old19_and_finite4_and217_questions_preserved'] and scope['other703_capture_heads_unchanged']
    assert not any(scope[k] for k in ('shopping_terms_are_news_capture_policy',
        'member_terms_apply_to_all_public_nonmembers_verified', 'REP_allow_is_content_license',
        'header_and_printed_names_are_verified_aliases', 'inferred_historical_first_release',
        'official_maximum_rate_verified', 'whole_publisher_history_complete'))
    assert {(r['source_header_credit'], r['printed_reporter_name']) for r in scope['native_source_roles']} == {
        ('芮內', '吳珍儀'), ('克里夫', '蕭文康')}
    assert all(not r['different_names_are_verified_aliases'] for r in scope['native_source_roles'])
    state = review_resume_plan({'enabled': True}, review)
    assert state['source_review_work_state'] == 'partially_completed'
    assert len(json.loads(state['remaining_review_questions_json'])) == 2
    assert not state['review_annotation_grants_capture_access']


def test_sinotrade_scope_review_closes_only_unfinished_question_not_all_body_work():
    from scripts.plan_news_sources import review_resume_plan

    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['sinotrade_research']
    assert review['capture_decision'] == 'permission_required'
    previous, = review['prior_reviews']
    assert previous['capture_decision'] == 'review_incomplete'
    assert previous['reviewed_at_utc'] == '2026-10-09T03:32:00+00:00'
    assert previous['reviewed_at_utc'] < review['reviewed_at_utc']
    assert set(previous['evidence_urls']) <= set(review['evidence_urls'])
    scope = review['current_scope_review']
    assert scope['known_URL_heads'] == sum(scope[k] for k in (
        'news_route_heads', 'stock_route_heads', 'redirect_route_heads', 'other_article_or_video_route_heads_unclassified'))
    assert scope['known_URL_heads'] == 8039 and scope['stored_body_heads'] == 0
    assert scope['primary_evidence_kind'] == 'web_reader_not_canonical_collector_response'
    assert scope['source_permission_or_independent_permitted_scope_required']
    assert not any(scope[k] for k in ('all_news_routes_are_proven_Anue_originals',
        'all_heads_are_verified_articles', 'public_access_implies_fulltext_capture_rights',
        'collector_publisher_requests', 'new_formal_bodies', 'new_private_originals',
        'services_restarted', 'whole_publisher_history_complete'))
    state = review_resume_plan({'enabled': False}, review)
    assert state['recorded_review_stage_complete'] and not state['review_annotation_grants_capture_access']
    assert state['source_review_work_state'] == 'completed_waiting_permission'
    entry, = [p for p in before[1]['providers'] if p['id'] == 'sinotrade_research']
    assert not entry['enabled'] and entry['authorization']['kind'] == 'awaiting_authorization'
    for phrase in ('one credit cannot classify all5530', 'not verified full-body completeness',
                   'not proof that every historical page', 'Existing19 complete originals,217 native questions'):
        assert phrase in review['observation']
    assert before == (configuration_revision(path), load_news_configuration(path))


@pytest.mark.parametrize('provider,heads,hosts', [
    ('appledaily_archive', 6958, 6), ('setn', 3686, 8), ('sina_news', 3014, 21),
])
def test_partial_archive_and_host_reviews_keep_unfinished_work_without_enabling_capture(provider, heads, hosts):
    from scripts.plan_news_sources import review_resume_plan

    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))[provider]
    if provider == 'setn':
        review = review['diagnostic_snapshot_review']
    assert review['capture_decision'] == 'review_incomplete'
    assert review['reviewed_at_utc'] == '2026-10-22T02:18:14+00:00'
    scope = review['current_scope_review']
    assert scope['known_URL_heads'] == heads and scope['observed_hosts'] == hosts
    assert scope['stored_body_heads'] == scope['new_formal_bodies'] == scope['collector_publisher_requests'] == 0
    assert scope['completed_questions'] and scope['remaining_questions']
    assert scope['primary_evidence_kind'] == 'web_reader_not_canonical_collector_response'
    assert not scope['whole_publisher_history_complete']
    state = review_resume_plan({'enabled': False}, review)
    assert state['source_review_work_state'] == 'partially_completed'
    assert not state['recorded_review_stage_complete'] and not state['review_annotation_grants_capture_access']
    assert state['review_question_inventory_available']
    assert json.loads(state['completed_review_questions_json']) == scope['completed_questions']
    assert json.loads(state['remaining_review_questions_json']) == scope['remaining_questions']
    assert not state['review_questions_are_exhaustive_body_work']
    entry, = [p for p in before[1]['providers'] if p['id'] == provider]
    if provider == 'setn':
        from downloader.news_fulltext_setn import SETN_LAYOUT, SETN_REVIEWED_URLS
        assert entry['enabled'] and entry['authorization']['kind'] == 'personal_research'
        assert entry['acquisition_mode'] == 'finite' and entry['finite_article_urls'] == list(SETN_REVIEWED_URLS)
        assert entry['embedded_article_layout'] == SETN_LAYOUT
    else:
        assert not entry['enabled'] and entry['authorization']['kind'] == 'awaiting_authorization'
    assert before == (configuration_revision(path), load_news_configuration(path))


def test_apple_actual_archive_leads_do_not_turn_indexes_or_expired_trials_into_bodies():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['appledaily_archive']
    scope = review['current_scope_review']
    assert scope['TW_URL_heads'] + scope['HK_URL_heads'] == scope['known_URL_heads']
    assert not any(scope[k] for k in ('headline_index_is_full_body', 'expired_trial_is_current_entitlement',
        'historical_library_seats_are_request_rate', 'TW_print_holdings_cover_all_known_web_and_HK_URLs'))
    for phrase in ('trial expiry2021-05-31', 'not this collector\'s requests/second',
                   'general2002-onward database date does not prove Apple\'s individual earliest date',
                   'No institutional account/proxy or expired trial was used',
                   'NextApple branding and unlicensed third-party corpora are not accepted aliases/archives'):
        assert phrase in review['observation']
    assert 'https://www.xkm.com.tw/HTML/XKMsite/service.html' in review['evidence_urls']


def test_setn_new_member_privacy_lookup_preserves_completed_homepage_evidence_and_old_clock():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['setn']
    review = review['diagnostic_snapshot_review']
    old, = review['prior_reviews']
    assert old['capture_decision'] == 'review_incomplete'
    assert old['reviewed_at_utc'] == '2026-10-17T15:55:31.305429+00:00'
    assert '3641 frozen inventoried' in old['observation']
    assert 'returned200,147705 decoded bytes' in old['observation']
    assert set(old['evidence_urls']) <= set(review['evidence_urls'])
    scope = review['current_scope_review']
    assert not scope['privacy_policy_grants_news_capture']
    assert not scope['no_policy_found_proves_permission_or_prohibition']
    assert not scope['research_reader_error_is_collector_refusal']
    assert 'No account was created, form submitted or source contacted' in review['observation']
    assert 'not prohibited, completed or permanently unavailable' in old['observation']


def test_setn_two_formal_originals_keep_legacy_redirect_and_publication_offset_unresolved():
    from downloader.news_fulltext_setn import SETN_REVIEWED_URLS
    from scripts.plan_news_sources import review_resume_plan
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['setn']
    scope = review['current_scope_review']
    assert review['capture_decision'] == 'review_incomplete'
    assert review['diagnostic_snapshot_review']['reviewed_at_utc'] < review['reviewed_at_utc']
    assert scope['known_URL_heads'] == sum(scope['observed_host_URL_heads'].values())
    assert scope['observed_hosts'] == 8 and scope['known_URL_heads'] >= 3689
    assert scope['new_formal_bodies'] == scope['stored_body_heads'] == scope['finite_reviewed_URLs'] == 2
    assert scope['remaining_known_missing_URL_heads'] == scope['known_URL_heads'] - 2
    assert scope['finite_scope_complete'] and not scope['whole_publisher_history_complete']
    assert scope['canonical_total_HTTP_exchanges'] == 4 and scope['article_document_HTTP_requests'] == 3
    assert scope['canonical_robots_HTTP_exchanges'] == 1
    assert scope['legacy_301_is_LOCAL_scope_exclusion_not_source_refusal']
    assert scope['legacy_952211_unattempted'] and not scope['legacy_body_alias_created']
    assert scope['publisher_requests_for_admission'] == 0
    assert scope['independent_lxml_body_parity'] and scope['old19_and_finite6_and217_preserved']
    assert scope['unscoped_capture_heads_unchanged'] == scope['baseline_known_URL_heads'] - 2
    assert scope['new_metadata_only_URL_heads'] == scope['known_URL_heads'] - scope['baseline_known_URL_heads']
    assert scope['new_metadata_not_requested_or_materialized']
    assert sum(scope['new_metadata_only_state_counts'].values()) == scope['new_metadata_only_URL_heads']
    assert not scope['new_metadata_local_path_classification_is_source_refusal']
    assert not any(scope[k] for k in ('REP_allow_is_content_license', 'inferred_schema_offset_correction',
        'inferred_historical_first_release', 'official_maximum_rate_verified', 'protected_services_restarted'))
    assert set(scope['finite_scope_URLs']) == set(SETN_REVIEWED_URLS)
    for source_clock in scope['native_source_clocks']:
        assert source_clock['schema_timezone_offset_raw'] == '+00:00'
        assert source_clock['visible_clock_precision'] == 'second'
        assert source_clock['publication_offset_resolution'] == 'unresolved'
        assert not source_clock['exact_PIT_eligible']
    assert set(review['diagnostic_snapshot_review']['current_scope_review']['completed_questions']) <= set(scope['completed_questions'])
    state = review_resume_plan({'enabled': True}, review)
    assert state['source_review_work_state'] == 'partially_completed'
    assert json.loads(state['remaining_review_questions_json']) == scope['remaining_questions']
    assert not state['review_annotation_grants_capture_access']


def test_sina_mixed_operator_scope_cannot_transfer_cn_or_us_terms_to_tw_heads():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['sina_news']
    scope = review['current_scope_review']
    assert scope['TW_URL_heads'] == 2284 and scope['CN_URL_heads'] == 730
    assert scope['TW_URL_heads'] + scope['CN_URL_heads'] == scope['known_URL_heads']
    assert scope['TW_hosts'] + scope['CN_hosts'] == scope['observed_hosts']
    assert not any(scope[k] for k in ('CN_policy_applies_to_TW_or_US',
        'all_CN_products_policy_applicability_verified', 'research_reader_error_is_collector_refusal',
        'all_known_originals_proven_permanently_unavailable'))
    assert review['evidence_urls'] == ['https://corp.sina.com.cn/chn/sina_item.html']
    assert 'not permission-denied under a CN agreement' in review['observation']
    assert 'not all-history completion' in review['observation']


def test_cmnews_completed_originals_do_not_turn_into_native_body_or_question_completion():
    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['cmnews']
    proof = review['unfinished_work_audit_20261022']
    assert proof['formal_body_heads'] + proof['all_native_missing_bodies'] == proof['known_URL_heads']
    assert proof['completed_literal_originals_reused_in_this_seven_head_cohort'] == 2
    assert proof['unanswered_exact_original_locators_in_this_seven_head_cohort'] == 5
    assert proof['new_fixed19_HTTP200_observations'] == 7 and proof['duplicate_literal_questions_disclosed'] == 2
    assert proof['new_formal_bodies'] == proof['new_retained_raw_originals'] == 0
    assert proof['submit_and_claim_reuse_existing_literal_metadata']
    assert proof['long_percent_encoded_locator_bound_corrected']
    assert proof['prior210_questions_and29_TaiwanNews_bodies_preserved']
    assert proof['all937_UI_rows_fresh_and_consistent']
    assert not any(proof[key] for key in ('CMNews_alias_equivalence_accepted', 'closed19_questions_rearmed',
        'normal_body_identity_or_access_checks_relaxed', 'existing_four_originals_downloaded_again',
        'whole_publisher_history_complete'))
    assert proof['engineering_acceptance'].endswith('/cmnews-canonical-relation-v19-final.json')
    assert (configuration_revision(path), load_news_configuration(path)) == before


def test_cmoneylearning_new_literal_bodies_preserve_completed_scope_and_native_gaps():
    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['cmoneylearning']
    progress = review['native_progress']
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert progress['counter_scope'] == 'v4 phase deltas plus explicitly labelled current total scope'
    assert progress['closed_literal_url_scope'] == progress['stored_body_heads'] == 20
    assert progress['previous_complete_bodies_preserved'] == 19
    assert progress['new_formal_body_heads'] == progress['source_HTTP_requests_for_new_originals'] == 1
    assert progress['previous_complete_bodies_preserved']+progress['new_formal_body_heads']==progress['stored_body_heads']
    assert progress['original_frozen_cohort_urls'] == 2 and progress['reviewed_supplement_urls'] == 18
    assert progress['original_frozen_cohort_urls'] + progress['reviewed_supplement_urls'] == 20
    assert progress['retained_raw_response_bytes'] == 101504
    assert progress['new_body_UTF8_bytes'] == 7207
    assert progress['remaining_missing_heads_in_this_scope'] == 0
    assert progress['extra_HTTP_for_parser_repair_or_materialization'] == progress['old_complete_bodies_refetched'] == 0
    assert progress['all951_UI_rows_fresh_and_consistent']
    assert not any(progress[key] for key in ('closed267_questions_rearmed', 'source_clock_or_refusal_reset',
        'CMNews_alias_equivalence_accepted', 'whole_publisher_history_complete', 'maximum_throughput_verified'))
    # The previous v3 cohort remains evidenced, not replaced by a new capture.
    prior=review['prior_scope_review_evidence']
    assert prior['path'].endswith('/cmoneylearning_declared_v4_20261023/prior-review-v3.json')
    assert len(prior['sha256'])==64
    assert review['engineering_acceptance'].endswith('/cmoneylearning_declared_v4_20261023/ready.json')
    assert review['semantic_verification']['overlapping_test_counts_are_not_additive']
    entry = next(p for p in before[1]['providers'] if p['id'] == 'cmoneylearning')
    assert entry['embedded_article_layout'] == 'cmoneylearning_visible_article_v4'
    assert entry['acquisition_mode'] == 'finite' and entry['allow_reviewed_supplements']
    assert entry['authorization']['kind']=='personal_research'
    assert before == (configuration_revision(path), load_news_configuration(path))


def test_taiwannews_native_admission_reuses_finished_questions_and_retains_missing_scope():
    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['taiwannews']
    assert review == reviews['unregistered:www.taiwannews.com.tw']
    assert review['capture_decision'] == 'reviewed_existing_scope'
    previous = review['previous_review']
    assert previous['capture_decision'] == 'private_public_candidate'
    assert previous['reviewed_at_utc'] == '2026-10-21T23:07:07+00:00'
    assert previous['reviewed_at_utc'] < review['reviewed_at_utc']
    assert previous['engineering_acceptance'].endswith('-probe-readback.json')
    assert review['engineering_acceptance'].endswith('-handoff.json')
    assert review['body_readback'].endswith('-body-readback.json')
    progress = review['native_progress']
    assert (progress['accepted_body_heads'], progress['known_url_heads'],
            progress['remaining_missing_body_heads']) == (29, 119, 90)
    assert progress['native_rejections'] == {
        'article_identity_not_verified': 53, 'article_title_not_verified': 29,
        'article_rights_or_scope_excluded': 8}
    assert sum(progress['native_rejections'].values()) == progress['remaining_missing_body_heads']
    assert progress['whole_publisher_history_complete'] is progress['maximum_throughput_verified'] is False
    single = review['native_single_root_evidence_v18']
    assert single['evidence_contract_version'] == 18 and single['fixed_questions_completed'] == 1
    assert single['prior_questions_preserved'] == 209
    assert single['private_originals_retained'] == single['new_body_heads'] == 0
    assert single['production_body_parser_expanded'] is False
    assert single['engineering_acceptance'].endswith('/taiwannews-single-root-v18-closeout.json')
    recovery = review['native_normal_body_recovery']
    assert recovery['native_contract_version'] == 3
    assert (recovery['new_body_heads'], recovery['completed_body_heads'], recovery['remaining_missing_heads']) == (1, 29, 90)
    assert (recovery['old_body_heads_unchanged'], recovery['other_head_fields_unchanged'],
            recovery['closed_candidate_questions_unchanged']) == (28, 118, 210)
    assert recovery['diagnostic_questions_replayed'] == 0 and recovery['body_parser_expanded'] is False
    assert recovery['whole_publisher_history_complete'] is recovery['maximum_throughput_verified'] is False
    assert recovery['engineering_acceptance'].endswith('/taiwannews-single-root-v18-body-closeout.json')
    for phrase in ('same resident HTTP/writer', 'full-field reparse', 'without resetting', '29/119',
                   'Anue remains first', 'CNA deferred'):
        assert phrase in recovery['observation']
    body = review['native_inline_body_v3']
    assert body['native_contract_version'] == 3 and body['resident_replay_contract_version'] == 2
    assert (body['new_body_heads'], body['completed_body_heads'], body['remaining_missing_heads']) == (4, 28, 91)
    assert body['old_body_heads_unchanged'] == 24 and body['other_head_fields_unchanged'] == 115
    assert body['closed_candidate_questions_unchanged'] == 209 and body['publisher_requests_for_materialization'] == 0
    assert body['individual_author_inferred'] is body['historical_point_in_time'] is False
    assert body['whole_publisher_history_complete'] is body['maximum_throughput_verified'] is False
    assert body['engineering_acceptance'].endswith('/taiwannews-native-v3-closeout.json')
    for phrase in ('literal', 'original authorization JSON was not stored', 'no accepted original was replayed',
                   'UI937 rows fresh/consistent with28/119', 'only the capture/parser ABI changes'):
        assert phrase.lower() in body['observation'].lower()
    guard = review['native_order_guard']
    assert guard['question'] == 'taiwannews_synchronous_stream_order_negative_guard_v2'
    assert guard['observed_at_utc'] > review['reviewed_at_utc']
    assert guard['native_contract_version'] == 2
    assert (guard['existing_body_heads_verified'], guard['remaining_missing_heads_preserved']) == (24, 95)
    assert guard['new_body_heads'] == guard['operator_publisher_requests'] == guard['old_heads_or_raw_rewrites'] == 0
    assert guard['whole_publisher_history_complete'] is False
    assert guard['engineering_acceptance'].endswith('/taiwannews-order-v2-handoff.json')
    for boundary in ('not a new rights review', 'every nonidentity parser field equal',
                     'No generic parser reset', 'all83 effective HTTP/refusal identities',
                     'Four protected trading/dashboard services', 'Anue remains first', 'CNA deferred'):
        assert boundary in guard['observation']
    for boundary in ('not a new rights grant', 'not total network traffic', 'undeclared timezone',
                     'no historical PIT', 'all24 accepted current responses', 'new capture version',
                     'only the resident owner', 'CNA remains deferred', 'Anue remains first'):
        assert boundary in review['observation']
    source = next(s for s in before[1]['providers'] if s['id'] == 'taiwannews')
    assert source['enabled'] and source['domains'] == ['www.taiwannews.com.tw']
    assert source['private_known_url_scope'] == 'personal_noncommercial_originals_v1'
    assert source['interval_seconds'] == 2 and source['parallelism'] == 1
    assert source['embedded_article_layout'] == 'taiwannews_completed_public_original_v3'
    assert source['require_author_attribution'] and source['require_configured_container']
    assert not any(source.get(key) for key in ('feeds', 'sitemaps', 'wordpress_apis', 'fulltext_feeds'))
    assert before[1]['publish'] is before[1]['remote_training_export'] is False
    assert before == (configuration_revision(path), load_news_configuration(path))


def test_bastille_completed_admission_preserves_first_question_and_unfinished_scope():
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    revision = configuration_revision(Path('configs/news_fulltext.json'))
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    current = reviews['unregistered:www.bastillepost.com']
    assert reviews['bastillepost'] == current
    assert current['capture_decision'] == 'reviewed_existing_scope'
    review, previous_admission = current['prior_reviews']
    assert review['capture_decision'] == 'private_public_candidate'
    assert review['reviewed_at_utc'] == '2026-10-21T14:30:17.338242+00:00'
    assert 'https://www.bastillepost.com/hongkong/terms' in review['evidence_urls']
    for phrase in ('110 literal local URL heads', 'zero stored bodies', 'Official terms3b',
                   'Actual robots200 wildcard', 'five-second delays belong to four named search crawlers',
                   'same-host same-ID slug', 'unverified selector', 'not proof that the original body is absent',
                   'three actual HTTP requests', 'No body/raw retention',
                   '60 separately counted metadata associations', 'not repeat this same URL question'):
        assert phrase in review['observation']
    assert review['reviewed_at_utc'] < current['reviewed_at_utc']
    for phrase in ('133/169 literal known URL heads', 'Exactly36 heads remain',
                   'not unique stories or all publisher history', 'corporate declaration',
                   'Reused', 'CNA remains deferred'):
        assert phrase.lower() in previous_admission['observation'].lower()
    assert previous_admission['reviewed_at_utc'] < current['reviewed_at_utc']
    for phrase in ('149/169 literal known URL heads', 'Exactly20 missing heads remain',
                   '32 empty due heads', '4 explicitly credit Star Daily', 'six-hour Pocket revalidation',
                   'current_cat=138496', 'zero acceptance HTTP/queue writes', 'CNA remains deferred'):
        assert phrase in current['observation']
    diagnostic = current['native_boundary_diagnostics']
    assert diagnostic['question'] == 'bastillepost_written_body_boundary_v1'
    assert diagnostic['metadata_jobs'] == 3 and diagnostic['http_exchanges'] == 5
    assert diagnostic['new_article_bodies'] == 0 and diagnostic['remaining_missing_heads'] == 20
    assert diagnostic['stored_scope_review_heads'] == 4
    assert diagnostic['observed_at_utc'] > current['reviewed_at_utc']
    assert 'Null absolute resource origin does not prove src is absent' in diagnostic['observation']
    assert current['remaining_work'].endswith('/bastille-following-remaining-work.json')
    resources = current['native_active_resource_diagnostics']
    assert resources['question'] == 'bastillepost_active_resource_and_body_relationship_v1'
    assert resources['acceptance'].endswith('/bastille-active-acceptance.json')
    assert resources['previous_body_acceptance'].endswith('/bastille-v4-production-acceptance.json')
    assert resources['metadata_jobs'] == 4 and resources['http_exchanges'] == 5
    assert resources['response_bytes'] == 567040 and resources['questions_wall_seconds'] == 8.475944
    assert resources['new_article_bodies'] == 0 and resources['stored_body_heads'] == 163
    assert resources['known_URL_heads'] == 169 and resources['remaining_missing_heads'] == 6
    assert resources['stored_scope_review_heads'] == 4 and resources['source_requests_by_acceptance'] == 0
    for phrase in ('not body acceptance or a new rights review', 'shared closed layout registry',
                   'Four new fixed', 'without repeating prior13 original or5 written-boundary',
                   'All169 heads and58 old candidate jobs', 'No script/media/query values/raw/prose',
                   'Anue remains first', 'CNA remains deferred'):
        assert phrase in resources['observation']
    short = current['native_short_original_repair']
    assert short['original_questions'] == 13 and short['private_originals_verified'] == 12
    assert short['new_article_bodies'] == 12 and short['stored_body_heads'] == 161
    assert short['known_URL_heads'] == 169 and short['remaining_missing_heads'] == 8
    assert short['stored_scope_review_heads'] == 4
    assert short['body_capture_HTTP_exchanges'] == 18 and short['body_capture_response_bytes'] == 1297326
    assert short['source_requests_by_readback'] == 0 and short['observed_at_utc'] > diagnostic['observed_at_utc']
    assert current['engineering_acceptance'].endswith('/bastille-following-production-acceptance.json')
    assert short['previous_engineering_acceptance'].endswith('/bastille-v2-production-acceptance.json')
    for phrase in ('Engineering continuation only', 'not a new rights review', 'markers verbatim',
                   'never as person names', 'Only the12 exact unfinished heads', 'historical PIT'):
        assert phrase in short['observation']
    continuation = current['native_two_paragraph_and_literal_query_repair']
    assert continuation['previous_engineering_acceptance'].endswith('/bastille-v3-production-acceptance.json')
    assert continuation['new_article_bodies'] == 2 and continuation['stored_body_heads'] == 163
    assert continuation['known_URL_heads'] == 169 and continuation['remaining_missing_heads'] == 6
    assert continuation['stored_scope_review_heads'] == 4
    assert continuation['body_capture_HTTP_exchanges'] == 3 and continuation['body_capture_response_bytes'] == 229073
    assert continuation['source_requests_by_readback'] == 0
    assert continuation['observed_at_utc'] > short['observed_at_utc'] > current['reviewed_at_utc']
    assert resources['observed_at_utc'] > continuation['observed_at_utc']
    loader = current['native_nonexecuting_social_loader_repair']
    assert loader['previous_engineering_acceptance'] == resources['previous_body_acceptance']
    assert loader['resource_diagnostics_acceptance'] == resources['acceptance']
    assert loader['body_readback'].endswith('/bastille-embed-new-body-readback.json')
    assert loader['new_article_bodies'] == 2 and loader['stored_body_heads'] == 165
    assert loader['known_URL_heads'] == 169 and loader['remaining_missing_heads'] == 4
    assert loader['stored_scope_review_heads'] == 4
    assert loader['body_capture_HTTP_exchanges'] == 3 and loader['body_capture_response_bytes'] == 308299
    assert loader['source_requests_by_readback'] == 0 and loader['observed_at_utc'] > resources['observed_at_utc']
    for phrase in ('not a new rights review', 'Reused163 accepted heads', '803 and2257 characters',
                   'other4 missing heads and62 old diagnostic jobs', 'No external rendered-fulltext',
                   'without reparsing', 'never a downloader retry', 'Anue remains first', 'CNA remains deferred'):
        assert phrase in loader['observation']
    video = current['native_nonexecuting_video_and_private_continuation']
    assert video['previous_engineering_acceptance'].endswith('/bastille-embed-production-acceptance.json')
    assert video['body_and_original_readback'].endswith('/bastille-video-new-body-readback.json')
    assert video['offline_relationship_evidence'].endswith('/bastille-video-continuation-offline-boundary.json')
    assert video['new_article_bodies'] == 1 and video['stored_body_heads'] == 166
    assert video['known_URL_heads'] == 169 and video['remaining_missing_heads'] == 3
    assert video['stored_scope_review_heads'] == 4 and video['private_original_questions_completed'] == 1
    assert video['private_original_not_a_body'] is True
    assert video['actual_HTTP_exchanges_body_and_original'] == 2
    assert video['actual_response_bytes_body_and_original'] == 258741 and video['source_requests_by_readback'] == 0
    assert video['observed_at_utc'] > loader['observed_at_utc']
    for phrase in ('not a new rights review', 'Reused165 stored bodies,62 old diagnostic jobs',
                   'independently restoring1085 native characters', 'accepts no article body',
                   'DIFFERENT ID16562194', 'not yet a native-v7 article acceptance',
                   'Anue remains first', 'CNA remains deferred'):
        assert phrase in video['observation']
    following = current['native_following_story_and_audio_repair']
    assert following['previous_engineering_acceptance'].endswith('/bastille-video-production-acceptance.json')
    assert following['offline_plan'].endswith('/bastille-following-plan.json')
    assert following['body_readback'].endswith('/bastille-following-new-body-readback.json')
    assert following['new_article_bodies'] == 1 and following['stored_body_heads'] == 167
    assert following['known_URL_heads'] == 169 and following['remaining_missing_heads'] == 2
    assert following['stored_scope_review_heads'] == 4
    assert following['actual_HTTP_exchanges'] == 1 and following['actual_response_bytes'] == 122901
    assert following['new_original_questions'] == following['source_requests_by_readback'] == 0
    assert following['observed_at_utc'] > video['observed_at_utc']
    for phrase in ('not a new rights review', 'Reused166 stored heads and63 completed candidate jobs',
                   'whole761-character original', 'never an arbitrary first-body fallback',
                   'joining a related story', 'All166 previous heads, other2 missing heads,63 jobs',
                   'raw response keeps both containers', 'Anue remains first', 'CNA remains deferred'):
        assert phrase in following['observation']
    for phrase in ('not a new rights review', 'all161 stored heads', 'whole requested URL',
                   'require actual same-ID transport', 'without erasing attempts/due/errors/clocks',
                   'Shared closed version registry', 'normal graceful code deployment',
                   'six-hour WatchMedia', 'The4 stored Star Daily scope reviews remain unresolved',
                   'full937-source goal remains active'):
        assert phrase in continuation['observation']
    source, = [s for s in config['providers'] if 'www.bastillepost.com' in s['domains']]
    assert source['id'] == 'bastillepost' and source['enabled']
    assert source['private_known_url_scope'] == 'personal_noncommercial_originals_v1'
    from downloader.news_fulltext_policy import BASTILLE_LAYOUT_VERSIONS
    assert BASTILLE_LAYOUT_VERSIONS.get(source['embedded_article_layout'], 0) >= 7
    assert not any(source.get(key) for key in ('feeds','sitemaps','wordpress_apis','fulltext_feeds'))
    assert config['publish'] is config['remote_training_export'] is False
    assert configuration_revision(Path('configs/news_fulltext.json')) == revision


def test_videoland_review_alias_and_ithome_incomplete_scope_do_not_grant_training():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['videoland']
    assert reviews['unregistered:news.videoland.com.tw'] == review
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert 'https://www.videoland.com.tw/Privacy.htm' in review['evidence_urls']
    for boundary in ('9 old newspage.aspx', 'not independently verified',
                     'not open-license', 'all937-source acceptance remain separate'):
        assert boundary in review['observation']
    other = reviews['unregistered:www.ithome.com.tw']
    assert other['capture_decision'] == 'review_incomplete'
    assert 'member-service database' in other['observation']
    assert 'public anonymous-news versus member-database' in other['observation']
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    assert config['publish'] is config['remote_training_export'] is False
    source = next(source for source in config['providers'] if source['id']=='videoland')
    assert source['private_known_url_scope']=='personal_noncommercial_originals_v1'
    assert not any(source.get(key) for key in ('feeds','sitemaps','wordpress_apis','fulltext_feeds'))
    assert not any('www.ithome.com.tw' in source['domains'] and source.get('enabled')
                   for source in config['providers'])


def test_idn_completed_body_cohort_and_unresolved_native_cases_stay_separate():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['idn']
    assert reviews['unregistered:www.idn.com.tw'] == review
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert 'https://www.idn.com.tw/' in review['evidence_urls']
    text = review['observation']
    for current in ('195 current bodies', 'Exactly2 remain missing', '此文章已刪除',
                    'publication clocks NULL', 'without refetch/reparse',
                    'every IDN parser version', 'full937-source goal remain active'):
        assert current in text
    prior = review['prior_reviews'][0]
    assert prior['reviewed_at_utc'] < review['reviewed_at_utc']
    text = prior['observation']
    for previous in ('193 current bodies', 'Exactly4 remain unresolved',
                     'Earlier v1 milestone retained with its original clock'):
        assert previous in text
    for evidence in ('197-URL cohort', '187 current bodies', 'Exactly10 remain unresolved',
                     'not an inferred count of unique articles or all IDN history',
                     'current header calendar and URL filename are not release-time witnesses',
                     'a zero-HTTP import is not falsely claimed', 'not an official/theoretical',
                     'All78 previous source', 'full937-source goal remain active'):
        assert evidence in text


def _configure(root, *, ledger=None):
    _seed(root)
    config_path = root / "configs/news_fulltext.json"
    config = json.loads(config_path.read_text())
    atomic_write_json(config_path, {**config, "provider_catalog": "news/catalog.json"})
    atomic_write_json(root / "configs/news/catalog.json", {
        "contract_version": 1, "providers": [], "planning_review_ledger": "reviews.json"})
    path = root / "configs/news/reviews.json"
    atomic_write_json(path, ledger or _ledger())
    return config_path, path


def test_private_ledger_preserves_evidence_aliases_and_exact_review_precedence(tmp_path):
    value = _ledger()
    exact = {**value["providers"]["moneydj"], "capture_decision": "review_incomplete"}
    value["providers"]["unregistered:example.org"] = exact
    path = tmp_path / "reviews.json"
    atomic_write_json(path, value)
    reviews = load_source_review_ledger(path)
    assert reviews["moneydj"]["observation"] == "PRIVATE_REVIEW_BODY_AND_KEY"
    assert reviews["unregistered:example.org"] == exact


@pytest.mark.parametrize('provider,decision,reviewed_at', [
    ('unregistered:blog.fugle.tw', 'permission_required', '2026-10-11T15:16:52.543736+00:00'),
    ('unregistered:school.gugu.fund', 'review_incomplete', '2026-10-20T14:20:46+00:00'),
    ('unregistered:turnnewsapp.com', 'review_incomplete', '2026-10-20T15:19:58.796031+00:00'),
    ('unregistered:www.top1markets.com', 'permission_required', '2026-10-20T15:24:16+00:00'),
    ('unregistered:pinview.com.tw', 'review_incomplete', '2026-10-20T19:40:27+00:00'),
])
def test_indexed_source_reviews_do_not_create_collectors_or_rewrite_review_clocks(provider, decision, reviewed_at):
    """A completed policy diagnosis must remain discoverable without new GETs."""
    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))[provider]
    assert review['capture_decision'] == decision
    assert review['reviewed_at_utc'] == reviewed_at
    host = provider.removeprefix('unregistered:')
    assert not any(host in source['domains'] for source in before[1]['providers'])
    assert before == (configuration_revision(path), load_news_configuration(path))


def test_fugle_handoff_preserves_prior_review_not_new_body_or_entitlement():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['unregistered:blog.fugle.tw']
    assert 'https://www.fugle.tw/service' in review['evidence_urls']
    assert 'already completed2026-10-11' in review['observation']
    assert 'not an audit of every later known URL' in review['observation']
    assert 'no new article GET' in review['observation']


def test_mitrade_completed_admission_preserves_the_prior_offline_stage_and_remaining_scope():
    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['mitrade']
    assert reviews['unregistered:www.mitrade.com'] == review
    assert review['capture_decision'] == 'reviewed_existing_scope'
    v4_review, = [r for r in [review, *review.get('prior_reviews', [])]
                  if r['reviewed_at_utc'] == '2026-10-20T22:51:16.270879+00:00']
    previous = review['previous_review']
    assert previous['capture_decision'] == 'private_public_candidate'
    assert previous['reviewed_at_utc'] == '2026-10-20T21:56:37+00:00'
    for phrase in ('three literal article200', 'zero additional source requests', 'NUL controls',
                   'not historical PIT', '140 local heads', '19 other category/locale paths',
                   'production source enablefalse', 'guarded producer admission',
                   'do not redownload the three originals', 'not independently verified people'):
        assert phrase in previous['observation']
    for phrase in ('zero additional publisher requests', 'original observation clocks',
                   'all77', '937 source keys', 'regional policy review', 'not historical PIT',
                   'do not redownload the three accepted originals', '103 durable bodies',
                   'preserving all102 accepted complete heads', 'exhausted diagnostic budgets',
                   'do not assume all56 share the newly reviewed credit'):
        assert phrase.lower() in v4_review['observation'].lower()
    source = next(row for row in before[1]['providers'] if row['id'] == 'mitrade')
    assert source['enabled'] and source['private_known_url_scope'] == 'personal_noncommercial_originals_v1'
    assert not source['feeds'] and not source['sitemaps'] and not source['wordpress_apis']
    assert before == (configuration_revision(path), load_news_configuration(path))


def test_mitrade_v5_missing_only_scope_keeps_measurement_permission_and_eta_boundaries_separate():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    latest = reviews['mitrade']
    review, = [r for r in [latest, *latest.get('prior_reviews', [])]
               if r['reviewed_at_utc'] == '2026-10-20T23:09:03.688794+00:00']
    assert review['capture_decision'] == 'reviewed_existing_scope'
    for name in ('alison-ho', 'tony'):
        assert 'https://www.mitrade.com/zh/insights/profile/'+name in review['evidence_urls']
    for phrase in ('preserved120 complete heads byte-identically', 'only7 new bodies',
                   '127 accumulated durable bodies', 'Remaining31 native exclusions',
                   'one zh-au', 'not an independently verified person', 'all77 other',
                   'Current body authorization/provenance still uses the new policy',
                   'Persisted429 floors and Retry-After remain authoritative',
                   'old-owner ETA windows are not new-owner samples',
                   'not an official or theoretical maximum', 'no healthy-body refetch',
                   'CNYES priority and CNA deferral'):
        assert phrase in review['observation']


def test_mitrade_v6_selective_retry_reuses_completed_evidence_without_repeating_all_exclusions():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    latest = reviews['mitrade']
    review, = [r for r in [latest, *latest.get('prior_reviews', [])]
               if r['reviewed_at_utc'] == '2026-10-20T23:21:42.235814+00:00']
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert 'https://www.mitrade.com/zh/insights/profile/Invest' in review['evidence_urls']
    for phrase in ('requeued only the two inspected missing originals', 'not all31',
                   '127 healthy heads', '157 total unselected heads', 'all77 other',
                   '129 accumulated durable bodies', 'only2 new native pen-name bodies',
                   'without reparsing', 'Remaining29 native exclusions', 'one zh-au',
                   'not an official or theoretical maximum', 'actual new authorization',
                   'diagnostic budgets', 'empty new-owner ETA windows', 'AI assistance',
                   'not silent human attribution', 'CNYES priority and CNA deferral'):
        assert phrase in review['observation']


def test_mitrade_v7_separates_company_reviewer_AI_and_reuses_immutable_completed_work():
    latest = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['mitrade']
    review, = [r for r in [latest, *latest.get('prior_reviews', [])]
               if r['reviewed_at_utc'] == '2026-10-20T23:47:21.459880+00:00']
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert 'https://www.mitrade.com/zh/insights/profile/mitrade-team' in review['evidence_urls']
    assert len({r['reviewed_at_utc'] for r in latest['prior_reviews']}) == len(latest['prior_reviews'])
    for phrase in ('requeued only one inspected missing company article', 'not all29',
                   '129 healthy heads', '158 total unselected heads', 'all77 other',
                   '130 accumulated durable bodies', 'only1 new company body', 'without reparsing',
                   'literal schema Person', 'not verified people', 'AI-assisted/editorially proofread',
                   'saved separately from financial risk and prose', 'before any publisher request',
                   'Remaining28 native exclusions', 'one zh-au', 'diagnostic budgets',
                   'not an official or theoretical maximum', 'no all-history ETA',
                   'CNYES priority and CNA deferral'):
        assert phrase in review['observation']


def test_mitrade_legacy_missing_only_delta_preserves_v7_scope_and_completed_evidence():
    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['mitrade']
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert review['reviewed_at_utc'] == '2026-10-21T00:07:31.588240+00:00'
    assert len(review['prior_reviews']) == 4
    assert len({r['reviewed_at_utc'] for r in [review, *review['prior_reviews']]}) == 5
    for phrase in ('full-v7-fingerprint-pinned', 'pre-v6-clock', 'ABI1 semantic requests remain compatible',
                   'once-per-parser admission', 'without assuming their authors', 'preserved all78 source identities',
                   'Exactly28 missing candidates', '28 actual article requests', '28 new durable bodies',
                   '158 accumulated durable bodies', 'reusing130 immutable body/Parquet proofs without reparsing',
                   'only28 new bodies', 'All131 unselected full queue rows remained byte-identical',
                   '937 source keys,159 known URLs and158 stored bodies', 'one zh-au regional route',
                   'reload retained prior measured2-second pacing', 'ramped to a1-second candidate',
                   'This remains ramping', 'no healthy article was refetched for rate testing',
                   'not full-site history', 'not an official or theoretical maximum', 'No healthy-body refetch',
                   'CNYES priority and CNA deferral'):
        assert phrase in review['observation']
    assert before == (configuration_revision(path), load_news_configuration(path))


def test_digitimes_review_separates_personal_reading_member_database_and_training_scope():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['unregistered:www.digitimes.com.tw']
    assert review['capture_decision'] == 'review_incomplete'
    assert review['evidence_urls'] == ['https://www.digitimes.com.tw/svc/cprgt.asp',
        'https://www.digitimes.com.tw/svc/svc_abs.asp?showtype=106']
    for phrase in ('130 known main-host', 'member/API', 'database storage', 'AI or program-training',
                   'Public native articles still need', 'no blanket claim that all130 are paywalled',
                   'HTML-encoded query separators', 'not a guessed HTTPS/decoded alias',
                   'canonical article/REP reachability', 'source activationfalse', 'CNYES priority and CNA deferral'):
        assert phrase in review['observation']


def test_pocket_review_alias_is_not_a_capture_grant_or_body_completion():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    assert reviews['unregistered:www.pocket.tw'] == reviews['pocket']
    review = reviews['pocket']
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert 'no historical PIT is inferred' in review['observation']
    assert 'not production durable acceptance' in review['observation']


def test_pinview_native_review_preserves_prior_clock_and_empty_bare_host_scope():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['pinview']
    assert reviews['unregistered:www.pinview.com.tw'] == review
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert review['previous_review']['capture_decision'] == 'review_incomplete'
    assert review['previous_review']['reviewed_at_utc'] == '2026-10-20T15:39:49+00:00'
    for phrase in ('219 literal', 'All Rights Reserved', 'no timezone', 'no verified alias',
                   'does not prove actual canonical downloading', 'No new discovery'):
        assert phrase in review['observation']
    bare = reviews['unregistered:pinview.com.tw']
    assert bare != review and bare['capture_decision'] == 'review_incomplete'
    for phrase in ('zero decoded bytes', 'no redirect', 'No alternate-host rewrite'):
        assert phrase in bare['observation']


def test_pinview_current_index_observation_is_not_body_or_alias_acceptance():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['pinview']
    assert review['reviewed_at_utc'] == '2026-10-20T19:40:27+00:00'
    index = review['current_index_review']
    assert index['observed_at_utc'] == '2026-10-20T20:32:52.264536+00:00'
    assert index['evidence_urls'] == ['https://www.pinview.com.tw/Tags/20345.html']
    for phrase in ('9531 decoded bytes', 'zero qualifying', 'narrower than',
                   'No19 old URL was requested again', 'all76 source identities',
                   'Original source review clocks', 'rather than repeat'):
        assert phrase in index['observation']


def test_peoplemedia_review_is_native_scope_not_all_history_or_editorial_person():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['peoplemedia']
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert 'https://www.peoplemedia.tw/copyright' in review['evidence_urls']
    for phrase in ('nine already queued', 'not all local metadata', 'editorial-center labels',
                   'raw invalid strings', 'not prove production durable acceptance', 'not official or measured maxima'):
        assert phrase in review['observation']
    native = review['native_v2_repair_review']
    for phrase in ('metadata-only pass', 'outer ASCII spaces', 'other four heads are not requeued',
                   'all28 already accepted', 'exact v1 HTTP', 'not new production bodies'):
        assert phrase in native['observation']
    embedded = review['native_v3_embedded_review']
    for phrase in ('Four still-missing', 'all37 accepted heads', 'multimedia-not-complete',
                   'failed negative lookup', 'no obfuscated address is decoded', 'Only the four evidenced'):
        assert phrase in embedded['observation']


@pytest.mark.parametrize('provider,evidence,scope', [
    ('nextapple', 'https://news.nextapple.com/terms', 'automated bulk content grabbing'),
    ('ctinews', 'https://ctinews.com/terms', 'database form'),
    ('housefun', 'https://member.housefun.com.tw/Private/propertystatement.aspx', 'prior written consent'),
])
def test_primary_terms_reviews_preserve_disabled_collectors_and_known_work(provider, evidence, scope):
    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))[provider]
    assert review['capture_decision'] == 'permission_required'
    assert evidence in review['evidence_urls'] and scope in review['observation']
    assert 'legal fair-use verdict' in review['observation']
    assert 'No article' in review['observation']
    assert not next(source for source in before[1]['providers'] if source['id'] == provider)['enabled']
    assert before == (configuration_revision(path), load_news_configuration(path))


def test_watchmedia_review_preserves_prior_clock_and_native_nonbody_boundaries():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    assert reviews['unregistered:www.watchmedia01.com'] == reviews['watchmedia']
    review = reviews['watchmedia']
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert review['previous_review']['reviewed_at_utc'] == '2026-10-20T15:48:10+00:00'
    assert review['previous_review']['capture_decision'] == 'private_public_candidate'
    for phrase in ['all244', 'partner警政時報', 'no visible native author', 'not production durable acceptance',
                   'not an official or measured ceiling']:
        assert phrase in review['observation']


def test_gugu_path_inventory_is_not_story_or_content_classification():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['unregistered:school.gugu.fund']
    assert 'https://school.gugu.fund/policy_sc' in review['evidence_urls']
    assert 'Path families are discovery hints' in review['observation']
    assert 'not automatically a ban on every public news article' in review['observation']
    assert 'does not enable a collector' in review['observation']


@pytest.mark.parametrize('provider,known_heads,evidence,scope_boundary', [
    ('unregistered:www.4gtv.tv', 183,
     'https://www.4gtv.tv/explanation-legal-agreement.html',
     'including editorial content'),
    ('unregistered:www.hwa-guan.com.tw', 165,
     'https://www.hwa-guan.com.tw/Finance/Home/ServicePolicy',
     'one-third limit applies to online publication/forwarding'),
    ('unregistered:geneonline.news', 136,
     'https://geneonline.news/terms-conditions/',
     'not the separately governed international affiliate'),
])
def test_new_policy_reviews_preserve_known_work_without_collectors_or_false_absence(
        provider, known_heads, evidence, scope_boundary):
    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))[provider]
    assert review['capture_decision'] == 'permission_required'
    assert evidence in review['evidence_urls']
    assert f'Preserve{known_heads} frozen inventoried literal URL heads' in review['observation']
    assert scope_boundary in review['observation']
    assert 'not a legal fair-use verdict' in review['observation']
    assert 'permanent irrecoverability' in review['observation']
    assert review['reviewed_at_utc'] == '2026-10-20T21:35:02+00:00'
    host = provider.removeprefix('unregistered:')
    assert not any(host in source['domains'] for source in before[1]['providers'])
    assert before == (configuration_revision(path), load_news_configuration(path))
    assert before[1]['remote_training_export'] is False


def test_hwa_guan_policy_http_evidence_does_not_become_article_permission_or_not_found():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))[
        'unregistered:www.hwa-guan.com.tw']
    assert 'returned200,87406 decoded bytes' in review['observation']
    assert 'robots request returned404' in review['observation']
    assert 'neither article absence nor a content license' in review['observation']
    assert 'not an inferred private-download limit' in review['observation']
    assert 'Article requests0, production queue writes0, source activationfalse' in review['observation']
    assert 'https://www.hwa-guan.com.tw/Finance/Home/DecCopyright' in review['evidence_urls']


@pytest.mark.parametrize('provider,decision,evidence,scope_boundary', [
    ('unregistered:more-news.tw', 'review_incomplete', 'https://more-news.tw/robots.txt',
     'article HTTP0, production queue writes0, private article raw0'),
    ('unregistered:news.ustv.com.tw', 'permission_required', 'https://www.ustv.com.tw/service',
     'expressly include the USTV news site'),
    ('unregistered:inews.hket.com', 'review_incomplete', 'https://inews.hket.com/',
     'not a canonical NewsHTTP article or REP observation'),
    ('unregistered:www.epochtimes.com', 'review_incomplete',
     'https://www.epochtimes.com/b5/UserAgreement.htm',
     'incomplete live-document verification'),
])
def test_unfinished_policy_or_access_review_never_accepts_articles_or_erases_evidence(
        provider, decision, evidence, scope_boundary):
    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))[provider]
    assert review['capture_decision'] == decision
    assert evidence in review['evidence_urls']
    assert scope_boundary in review['observation']
    assert 'CNYES priority and CNA deferral remain unchanged' in review['observation']
    host = provider.removeprefix('unregistered:')
    assert not any(host in source['domains'] for source in before[1]['providers'])
    assert before == (configuration_revision(path), load_news_configuration(path))


def test_more_news_real_robots_refusal_is_not_a_reader_error_or_official_rate():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))[
        'unregistered:more-news.tw']
    assert 'returned403 at2026-10-20T21:37:53.913794+00:00' in review['observation']
    assert 'denial body was not read or retained' in review['observation']
    assert '900-second local cooldown' in review['observation']
    assert 'not an official quota or measured rate' in review['observation']
    assert 'complete-row before/after SHA agrees' in review['observation']
    assert 'do not reset or repeatedly probe the refusal' in review['observation']


@pytest.mark.parametrize("field,bad", [
    ("contract_version", True), ("contract_version", 2), ("providers", []),
    ("providers", {"bad\nname": {}}), ("providers", {"moneydj": {"capture_decision": []}}),
    ("provider_aliases", []), ("provider_aliases", {"x": "missing"}),
])
def test_bad_review_ledger_is_rejected_without_raw_error_text(tmp_path, field, bad):
    path = tmp_path / "reviews.json"
    atomic_write_json(path, {**_ledger(), field: bad})
    with pytest.raises(ValueError, match="invalid source review"):
        load_source_review_ledger(path)


@pytest.mark.parametrize("relative", ["/absolute.json", "../../outside.json", ".env", 1, ""])
def test_review_path_cannot_escape_catalog_or_read_env(tmp_path, relative):
    config_path, _ = _configure(tmp_path)
    catalog = tmp_path / "configs/news/catalog.json"
    value = json.loads(catalog.read_text())
    atomic_write_json(catalog, {**value, "planning_review_ledger": relative})
    with pytest.raises(ValueError):
        planning_review_path(config_path)


def test_review_symlink_escape_is_rejected(tmp_path):
    config_path, path = _configure(tmp_path)
    outside = tmp_path / "outside.json"
    atomic_write_json(outside, _ledger())
    link = path.parent / "symlink.json"
    link.symlink_to(outside)
    catalog = tmp_path / "configs/news/catalog.json"
    value = json.loads(catalog.read_text())
    atomic_write_json(catalog, {**value, "planning_review_ledger": "symlink.json"})
    with pytest.raises(ValueError, match="catalog-local"):
        planning_review_path(config_path)


def test_malformed_catalog_does_not_create_attribute_error(tmp_path):
    config_path, _ = _configure(tmp_path)
    atomic_write_json(tmp_path / "configs/news/catalog.json", [])
    with pytest.raises(ValueError, match="invalid news source catalog"):
        planning_review_path(config_path)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result["source_reviews_state"] == "unavailable"


def test_oversized_review_is_not_read_or_projected(tmp_path):
    config_path, path = _configure(tmp_path)
    atomic_write_json(path, {**_ledger(), "large_private_note": "x" * (2 << 20)})
    with pytest.raises(ValueError, match="oversized"):
        load_source_review_ledger(path)
    with pytest.raises(ValueError, match="bounded"):
        planning_review_path(config_path)
    assert build_news_public_status(tmp_path, now=NOW)["source_reviews_state"] == "unavailable"


def test_review_edit_refreshes_only_dashboard_not_owner_or_capture_configuration(tmp_path):
    config_path, path = _configure(tmp_path)
    before = configuration_revision(config_path), news_snapshot_revision(tmp_path), load_news_configuration(config_path)
    result = build_news_public_status(tmp_path, now=NOW)
    rows = {s["id"]: s for s in result["providers"]}
    assert rows["unregistered:example.org"]["source_policy_review"] == "permission_required"
    assert rows["unregistered:example.org"]["source_reviewed_at_utc"] == NOW.isoformat()
    assert not rows["unregistered:example.org"]["enabled"]
    assert rows["unregistered:example.org"]["conditional_eta_seconds"] is None
    assert rows["cnyes"]["source_policy_review"] == "active_existing_policy"
    assert rows["cnyes"]["enabled"]
    assert result["source_policy_review_counts"] == {"permission_required": 1, "active_existing_policy": 1}
    assert sum(result["source_policy_review_counts"].values()) == len(result["providers"])
    assert not result["scope"]["source_reviews_grant_capture_permission"]
    assert "PRIVATE_REVIEW" not in json.dumps(result)
    assert "https://example.org/terms" not in json.dumps(result)
    value = _ledger()
    value["providers"]["moneydj"]["capture_decision"] = "review_incomplete"
    atomic_write_json(path, value)
    assert before[0] == configuration_revision(config_path)
    assert before[1] != news_snapshot_revision(tmp_path)
    assert before[2] == load_news_configuration(config_path)
    rows = {s["id"]: s for s in build_news_public_status(tmp_path, now=NOW)["providers"]}
    assert rows["unregistered:example.org"]["source_policy_review"] == "review_incomplete"
    assert not rows["unregistered:example.org"]["enabled"]


def test_review_does_not_revoke_an_active_runtime_license(tmp_path):
    value = _ledger()
    value["providers"]["cnyes"] = deepcopy(value["providers"]["moneydj"])
    _configure(tmp_path, ledger=value)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(s for s in result["providers"] if s["id"] == "cnyes")
    assert source["enabled"] and source["source_policy_review"] == "permission_required"
    # Pending work without an inflight request is backfilling, not running.
    assert source["state"] == "backfilling" and source["known_urls"] == 6


@pytest.mark.parametrize("clock", [True, "not-a-clock", "2026-10-08T15:00:00", (NOW + timedelta(seconds=6)).isoformat()])
def test_review_with_unverified_clock_is_explicit_not_falsely_reviewed(tmp_path, clock):
    value = _ledger()
    value["providers"]["moneydj"]["reviewed_at_utc"] = clock
    _configure(tmp_path, ledger=value)
    result = build_news_public_status(tmp_path, now=NOW)
    row = next(s for s in result["providers"] if s["id"] == "unregistered:example.org")
    assert row["source_policy_review"] == "review_unverified"
    assert row["source_reviewed_at_utc"] is None and not row["enabled"]


def test_bad_review_file_preserves_successful_capture_counts_and_state(tmp_path):
    _, path = _configure(tmp_path)
    atomic_write_json(path, {"contract_version": True})
    result = build_news_public_status(tmp_path, now=NOW)
    assert result["health"] == "running" and result["snapshot_consistent"]
    assert result["acquisition"]["stored_body_articles"] == 2
    assert result["source_reviews_state"] == "unavailable"
    assert result["source_policy_review_counts"] == {"active_existing_policy": 1, "not_reviewed": 1}


def test_missing_review_configuration_is_explicit_and_does_not_call_publishers(tmp_path):
    _seed(tmp_path)
    assert planning_review_path(tmp_path / "configs/news_fulltext.json") is None
    result = build_news_public_status(tmp_path, now=NOW)
    assert result["source_reviews_state"] == "not_configured"
    assert not result["scope"]["publisher_calls_on_page_view"]


def test_wantmedia_primary_review_is_permission_work_not_an_implicit_enable():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['wantmedia']
    assert review['capture_decision'] == 'permission_required'
    assert review['evidence_urls'] == ['https://member.want-media.com/member-term-of-service.htm']
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == 'wantmedia')
    assert source['enabled'] is False
    assert source['authorization']['kind'] == 'awaiting_authorization'


@pytest.mark.parametrize('provider,decision,evidence', [
    ('vocus', 'permission_required', 'https://vocus.cc/terms/member'),
    ('mirrormedia', 'permission_required', 'https://www.mirrormedia.mg/story/service-rule'),
    ('storm', 'permission_required', 'https://www.storm.mg/terms'),
    ('setn', 'review_incomplete', 'https://www.setn.com/'),
])
def test_creator_and_publisher_policy_reviews_do_not_enable_unreviewed_downloads(provider, decision, evidence):
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews[provider]
    assert review['capture_decision'] == decision
    assert evidence in review['evidence_urls']
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == provider)
    if provider == 'setn':
        from downloader.news_fulltext_setn import SETN_LAYOUT, SETN_REVIEWED_URLS
        assert source['enabled'] is True and source['authorization']['kind'] == 'personal_research'
        assert source['acquisition_mode'] == 'finite' and source['finite_article_urls'] == list(SETN_REVIEWED_URLS)
        assert source['embedded_article_layout'] == SETN_LAYOUT
    else:
        assert source['enabled'] is False
        assert source['authorization']['kind'] == 'awaiting_authorization'
    assert not any(source.get(key) for key in (
        'cnyes_apis', 'wordpress_apis', 'fulltext_feeds', 'feeds', 'sitemaps', 'index_pages'))
    assert config['remote_training_export'] is False


def test_cmoney_official_metadata_review_never_registers_body_API_or_credentials():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['cmoney']
    assert review['capture_decision'] == 'review_incomplete'
    assert '30 distinct operations' in review['observation']
    prefix = 'https://www.cmoney.tw/cm-service/CMService.asmx?op='
    operations = [url.removeprefix(prefix) for url in review['evidence_urls'] if url.startswith(prefix)]
    assert len(operations) == 5 and set(operations) == {
        'GetStockRssNews', 'GetMoreStockRssNews', 'GetDailyHeadlineArticles',
        'GetStockRssArticles', 'GetStockRssArticlesWithFilterType'}
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == 'cmoney')
    assert not any(source.get(key) for key in ('wordpress_apis', 'cnyes_apis', 'fulltext_feeds'))


def test_cmoney_owl_RSS_contract_is_metadata_not_forum403_or_original_text_entitlement():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['cmoney']
    assert any(url.endswith('API%E8%A6%8F%E6%A0%BC.pdf') for url in review['evidence_urls'])
    for declaration in ('Microsoft-IIS/8.5', 'kong/3.9.0', '/OwlApi/auth',
                        '/OwlApi/api/rss/{product}/{count}',
                        '/OwlApi/api/rss/{product}/{stock}/{count}',
                        'creation-time', 'not full text', 'No Owl auth/product request sent'):
        assert declaration in review['observation']
    source = next(row for row in load_news_configuration(Path('configs/news_fulltext.json'))['providers']
                  if row['id'] == 'cmoney')
    assert not any(source.get(key) for key in ('wordpress_apis', 'cnyes_apis', 'fulltext_feeds'))


def test_udn_licensed_export_and_archive_clock_review_does_not_grant_capture_or_backdate():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['udn']
    assert review['capture_decision'] == 'permission_required'
    assert 'https://udndata.com/faq/marketing.html' in review['evidence_urls']
    assert 'https://www.udndata.com/faq/fulltext.html' in review['evidence_urls']
    for declaration in ('original database licensed export', 'data-analysis application',
                        'not each article\'s exact original publication', 'Before1999-09-01',
                        'not calls per hour', 'licensed-export schema', 'candidate adapter disabled'):
        assert declaration in review['observation']
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == 'udn')
    assert source['enabled'] is False and source['authorization']['kind'] == 'awaiting_authorization'
    assert config['remote_training_export'] is False


def test_global_voices_review_preserves_missing_byline_gate_and_original_pacing():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    assert reviews['globalvoices']['capture_decision'] == 'reviewed_existing_scope'
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == 'globalvoices')
    assert source['enabled'] and source['interval_seconds'] == 10
    assert source['require_author_attribution'] and not source['wordpress_apis']
    assert len(source['fulltext_feeds']) == 2


def test_global_voices_actual_embed_refusal_is_not_guessed_byline_or_API_activation():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    latest = reviews['globalvoices']
    # This is the dated API/network witness, not a requirement to overwrite
    # the latest offline article diagnosis with an old observation.
    review, = [r for r in [latest, *latest.get('prior_reviews', [])]
               if r['reviewed_at_utc'] == '2026-10-13T07:10:13+00:00']
    assert 'rest_cannot_access' in review['observation']
    assert 'All100 author embeds' in review['observation']
    assert 'zero embedded names' in review['observation']
    assert 'no raw mixed-response retention' in review['observation']
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == 'globalvoices')
    assert source['wordpress_apis'] == [] and source['require_author_attribution'] is True


def test_global_voices_editorial_AI_and_native_network_evidence_have_distinct_scopes():
    latest = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['globalvoices']
    review, = [r for r in [latest, *latest.get('prior_reviews', [])]
               if r['reviewed_at_utc'] == '2026-10-13T07:10:13+00:00']
    assert 'https://globalvoices.org/about/global-voices-policy-on-ai/' in review['evidence_urls']
    assert 'EHOSTUNREACH' in review['observation']
    assert 'not a new capture grant' in review['observation']
    assert 'network recovery is inferred' in review['observation']
    assert 'strictly post-failure complete captures' in review['observation']
    assert 'WP embedded-author permission remains refused' in review['observation']
    assert 'not all-history or all-provider recovery' in review['observation']
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == 'globalvoices')
    assert source['enabled'] is True and source['authorization']['kind'] == 'open_license'
    assert config['remote_training_export'] is False


def test_global_voices_completed_scope_question_is_not_rearmed_or_promoted_to_body():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['globalvoices']
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert 'https://globalvoices.org/2005/11/03/3474/' in review['evidence_urls']
    for fact in ('one directHTTP200', '172005 response bytes', 'visible empty native title',
                 'genuinely empty native entry', 'not rearmed or rewritten',
                 'not a new original/body', 'production queue commit',
                 '3currently empty-native', 'do not repeat these completed',
                 'independently reviewed historical representations', 'all83 source identities',
                 'only read-only acceptance repeated', 'planning observations grant no'):
        assert fact in review['observation']
    assert review['previous_review_evidence'].endswith('globalvoices-scope-3474-final.json')
    assert {r['reviewed_at_utc'] for r in review['prior_reviews']} >= {
        '2026-10-20T08:46:00+00:00', '2026-10-13T07:10:13+00:00'}
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == 'globalvoices')
    assert source['enabled'] and source['authorization']['kind'] == 'open_license'
    assert source['interval_seconds'] == 10 and not source['wordpress_apis']
    assert source['require_author_attribution'] and config['remote_training_export'] is False


def test_ltn_official_rss_permission_does_not_enable_html_or_mislabel_abstract_as_body():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['ltn']
    assert review['capture_decision'] == 'permission_required'
    assert review['evidence_urls'] == [
        'https://service.ltn.com.tw/copyright', 'https://service.ltn.com.tw/RSS',
        'https://news.ltn.com.tw/rss/business.xml', 'https://news.ltn.com.tw/rss/all.xml']
    assert 'RSS metadata/abstracts, not full articles' in review['observation']
    assert 'not blanket HTML/full-history/training permission' in review['observation']
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == 'ltn')
    assert source['enabled'] is False
    assert source['authorization']['kind'] == 'awaiting_authorization'
    assert not any(source.get(key) for key in ('fulltext_feeds', 'wordpress_apis', 'cnyes_apis'))


def test_money_link_public_footer_review_keeps_news_and_financial_products_separate():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['money_link']
    assert review['capture_decision'] == 'permission_required'
    assert 'https://tweb.money-link.com.tw/policy/declaration.aspx' in review['evidence_urls']
    assert 'https://ww2.money-link.com.tw/MemberCenter/MemberTerms.aspx?term=4' in review['evidence_urls']
    assert 'outside any login' in review['observation']
    assert 'editorial-content rights' in review['observation']
    assert 'not automatically treat that financial-data product paragraph as the entire news contract' in review['observation']
    assert 'does not prove that no licensed feed exists' in review['observation']
    assert 'planning review grants no source permission' in review['observation']
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == 'money_link')
    assert source['enabled'] is False and source['authorization']['kind'] == 'awaiting_authorization'


@pytest.mark.parametrize('provider,evidence', [
    ('unregistered:hd.stheadline.com', 'https://www.stheadline.com/terms/'),
    ('unregistered:sunmedia.tw', 'https://sunmedia.tw/copyright'),
    ('unregistered:www.peoplenews.tw', 'https://www.peoplenews.tw/%E7%89%88%E6%AC%8A%E8%81%B2%E6%98%8E'),
])
def test_new_restrictive_reviews_preserve_known_urls_without_granting_capture(provider, evidence):
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews[provider]
    assert review['capture_decision'] == 'permission_required'
    assert evidence in review['evidence_urls']
    assert review['reviewed_at_utc'].startswith('2026-10-14T')
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    assert not any(source['id'] == provider and source.get('enabled') for source in config['providers'])
    assert config['remote_training_export'] is False


def test_bnext_completed_policy_access_is_not_article_capture_or_private_automation_verdict():
    from scripts.plan_news_sources import review_resume_plan

    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['bnext']
    assert review['capture_decision'] == 'review_incomplete'
    assert review['previous_review']['reviewed_at_utc'] == '2026-10-09T03:30:00+00:00'
    assert review['previous_review']['reviewed_at_utc'] < review['reviewed_at_utc']
    assert 'https://www.bnext.com.tw/article/57040/content-strategy' in review['evidence_urls']
    scope = review['current_scope_review']
    assert scope['known_URL_heads'] == 385 and scope['main_homepage_status'] == 200
    assert scope['account_policy_status'] == 302 and scope['content_policy_final_status'] == 200
    assert scope['canonical_policy_HTTP_exchanges'] == (
        scope['canonical_robots_HTTP_exchanges'] + scope['canonical_nonrobots_policy_HTTP_exchanges']) == 7
    assert scope['policy_text_characters'] == 569 and len(scope['policy_text_sha256']) == 64
    assert scope['metadata_heading_mismatch_preserved'] and scope['policy_text_is_not_news_body']
    assert scope['republication_conditions_are_not_private_automation_verdict']
    assert scope['robots_allows_is_not_copyright_permission']
    for field in ('stored_body_heads', 'security_redirects_followed', 'article_HTTP_requests',
            'new_formal_bodies', 'new_private_originals', 'production_queue_writes',
            'source_authorization_granted', 'services_restarted_for_policy_review',
            'whole_publisher_history_complete', 'official_numeric_rate_verified'):
        assert not scope[field]
    state = review_resume_plan({'enabled': False}, review)
    assert state['source_review_work_state'] == 'partially_completed'
    assert json.loads(state['completed_review_questions_json']) == scope['completed_questions']
    assert json.loads(state['remaining_review_questions_json']) == scope['remaining_questions']
    assert not state['recorded_review_stage_complete'] and not state['review_annotation_grants_capture_access']
    source = next(r for r in load_news_configuration(Path('configs/news_fulltext.json'))['providers'] if r['id'] == 'bnext')
    assert not source['enabled'] and source['authorization']['kind'] == 'awaiting_authorization'


def test_taisounds_answered_robots_failure_is_not_terms_article_absence_or_permission():
    from scripts.plan_news_sources import review_resume_plan

    config_path = Path('configs/news_fulltext.json')
    before = configuration_revision(config_path), load_news_configuration(config_path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['taisounds']
    assert review['capture_decision'] == 'review_incomplete'
    assert review['previous_review']['reviewed_at_utc'] == '2026-10-10T19:43:00+00:00'
    assert review['previous_review']['reviewed_at_utc'] < review['reviewed_at_utc']
    assert 'https://static.taisounds.news/robots.txt' in review['evidence_urls']
    scope = review['current_scope_review']
    assert scope['known_URL_heads'] == 487 and scope['robots_status'] == 502
    assert scope['canonical_policy_HTTP_exchanges'] == (
        scope['canonical_robots_HTTP_exchanges'] + scope['canonical_nonrobots_policy_HTTP_exchanges']) == 1
    assert scope['canonical_nonrobots_policy_HTTP_exchanges'] == 0
    assert scope['indexed_policy_is_not_current_canonical_response']
    assert scope['origin_circuit_and_exact_diagnostic_identity_preserved']
    for field in ('stored_body_heads', 'robots_response_body_read', 'terms_document_requested',
            'homepage_requested', 'alternate_origin_requested_after_failure', 'article_HTTP_requests',
            'new_formal_bodies', 'new_private_originals', 'production_queue_writes',
            'source_authorization_granted', 'services_restarted_for_policy_review',
            'whole_publisher_history_complete', 'official_numeric_rate_verified'):
        assert not scope[field]
    state = review_resume_plan({'enabled': False}, review)
    assert state['source_review_work_state'] == 'partially_completed'
    assert len(json.loads(state['completed_review_questions_json'])) == 1
    assert json.loads(state['remaining_review_questions_json']) == scope['remaining_questions']
    assert not state['recorded_review_stage_complete'] and not state['review_annotation_grants_capture_access']
    source = next(r for r in before[1]['providers'] if r['id'] == 'taisounds')
    assert not source['enabled'] and source['authorization']['kind'] == 'awaiting_authorization'
    assert before == (configuration_revision(config_path), load_news_configuration(config_path))

"""Metadata/scope-only public projection; no publisher or queue access."""
import json
from urllib.parse import urlsplit
import threading

import pytest

from downloader.artifact_io import atomic_write_json
from stockagent.live.news_dashboard import build_news_public_status
from test_news_dashboard import NOW, _seed, _server


def index_snapshot():
    return {'contract_version': 1, 'enabled': True,
            'source_dates_observed': 12, 'verified_pages': 345, 'metadata_rows': 10218,
            'public_ID_row_observations': 10200, 'dates_all_reported_pages_observed': 8,
            'dates_with_changed_source_total': 1, 'article_bodies_accepted_by_index': 0,
            'all_history_complete': False, 'finish_eta_utc': None,
            'source_total_is_not_unique_story_count': True, 'category_gaps_preserved': True,
            'url': 'https://private.test/?key=PRIVATE_TOKEN', 'raw_response_body': 'PRIVATE_BODY',
            'receipt_path': '/PRIVATE_PATH', 'progress_ratio': .99}


def history_snapshot():
    return {'contract_version': 1, 'current_source_contract_scope_states': {
                'list_complete': 2805, 'list_complete_by_page_ID_equivalence': 86,
                'partition_list_complete': 1116, 'waiting_page_evidence': 3},
            'scope_states': {'PRIVATE_REASON': 999999}, 'retained_metadata_work': {'PRIVATE_WORK': 3},
            'retired_gap_receipts': 1116, 'list_coverage_is_not_body_coverage': True,
            'all_history_complete': False, 'finish_eta_utc': None,
            'unresolved_category_scopes': 0, 'progress_ratio': 1,
            'scope_url': 'https://private.test/?key=PRIVATE_TOKEN', 'raw_response_body': 'PRIVATE_BODY'}


def gaps_snapshot():
    return {'contract_version': 1, 'total_gaps': 1520, 'groups': [
                {'provider': 'cnyes', 'error_code': 'cnyes_single_day_page_cap', 'scopes': 1518},
                {'provider': 'other', 'error_code': 'PRIVATE_REASON', 'scopes': 2}],
            'sample_rows': 64, 'sample_truncated': True, 'all_gap_details_preserved': True,
            'historical_completeness_verified': False,
            'sample': [{'url': 'https://private.test/PRIVATE_BODY'}]}


def save(root, status):
    atomic_write_json(root / 'data_news_fulltext/status.json', status)
    return build_news_public_status(root, now=NOW)


def setup(root):
    status = _seed(root)
    status['providers']['cnyes'].update(public_index_progress=index_snapshot(),
                                      history_list_progress=history_snapshot())
    status['bulk_gap_projection'] = gaps_snapshot()
    return status


def cnyes(result):
    return next(row for row in result['providers'] if row['id'] == 'cnyes')


def test_lists_are_typed_scopes_not_body_progress_or_global_eta(tmp_path):
    status = setup(tmp_path)
    baseline = build_news_public_status(tmp_path, now=NOW)
    result = save(tmp_path, status)
    source = cnyes(result)
    assert result['cnyes_list_progress_projection_contract_version'] == 1
    index = source['public_index_progress']
    assert index['verified_pages'] == 345 and index['metadata_rows'] == 10218
    assert index['public_ID_row_observations'] == 10200 and index['source_dates_observed'] == 12
    assert index['dates_all_reported_pages_observed'] == 8
    assert index['article_bodies_accepted_by_index'] == 0
    assert index['source_total_is_not_unique_story_count'] and index['category_gaps_preserved']
    history = source['history_list_progress']
    assert history['verified_list_scopes'] == 4007
    assert history['current_source_contract_scope_states']['waiting_page_evidence'] == 3
    assert history['unresolved_category_scopes'] == 1518  # Not the 64-row sample or owner hint.
    assert history['retired_gap_receipts'] == 1116
    assert history['list_coverage_is_not_body_coverage']
    for section in (index, history):
        assert section['progress_ratio'] is section['finish_eta_utc'] is None
        assert section['all_history_complete'] is False
    assert source['known_urls'] == 6 and source['complete_urls'] == 2
    assert source['progress_ratio'] == 2 / 6 and source['remaining_urls'] == 4
    assert result['acquisition'] == baseline['acquisition']
    assert not result['scope']['publisher_calls_on_page_view']
    assert 'PRIVATE_' not in json.dumps(result)


@pytest.mark.parametrize('patch', [
    {'contract_version': True}, {'contract_version': 2}, {'enabled': False},
    {'source_dates_observed': True}, {'verified_pages': '345'}, {'metadata_rows': -1},
    {'public_ID_row_observations': 10219}, {'metadata_rows': 10351}, {'verified_pages': 11},
    {'dates_all_reported_pages_observed': 13}, {'dates_with_changed_source_total': 5},
    {'source_dates_observed': 0}, {'verified_pages': 2**53},
    {'article_bodies_accepted_by_index': True}, {'article_bodies_accepted_by_index': 1},
    {'all_history_complete': True}, {'finish_eta_utc': NOW.isoformat()},
    {'source_total_is_not_unique_story_count': False}, {'category_gaps_preserved': False},
])
def test_unreconciled_index_is_unknown_without_hiding_body_or_history(tmp_path, patch):
    status = setup(tmp_path)
    status['providers']['cnyes']['public_index_progress'].update(patch)
    source = cnyes(save(tmp_path, status))
    assert source['public_index_progress'] is None
    assert source['history_list_progress']['unresolved_category_scopes'] == 1518
    assert source['known_urls'] == 6 and source['complete_urls'] == 2


@pytest.mark.parametrize('patch', [
    {'contract_version': True}, {'contract_version': 2}, {'current_source_contract_scope_states': None},
    {'current_source_contract_scope_states': {'list_complete': True}},
    {'current_source_contract_scope_states': {'list_complete': -1}},
    {'current_source_contract_scope_states': {'PRIVATE_REASON': 1}},
    {'retired_gap_receipts': True}, {'retired_gap_receipts': '1116'},
    {'list_coverage_is_not_body_coverage': False}, {'all_history_complete': True},
    {'finish_eta_utc': NOW.isoformat()},
])
def test_unreconciled_history_is_unknown_without_hiding_index(tmp_path, patch):
    status = setup(tmp_path)
    status['providers']['cnyes']['history_list_progress'].update(patch)
    source = cnyes(save(tmp_path, status))
    assert source['history_list_progress'] is None
    assert source['public_index_progress']['verified_pages'] == 345


@pytest.mark.parametrize('patch', [
    {'contract_version': True}, {'contract_version': 2}, {'total_gaps': True}, {'total_gaps': 1},
    {'groups': None}, {'groups': [{'provider': 'cnyes', 'error_code': 'x', 'scopes': True}]},
    {'groups': [{'provider': 'cnyes', 'error_code': 'x', 'scopes': 1518}]},
    {'groups': [{'provider': 'cnyes', 'error_code': 'x', 'scopes': 760}] * 2},
    {'groups': [{'provider': 'PRIVATE_TOKEN/unsafe', 'error_code': 'x', 'scopes': 1520}]},
    {'all_gap_details_preserved': False}, {'historical_completeness_verified': True},
])
def test_inconsistent_full_gap_projection_does_not_fall_back_to_url_sample(tmp_path, patch):
    status = setup(tmp_path)
    status['bulk_gap_projection'].update(patch)
    history = cnyes(save(tmp_path, status))['history_list_progress']
    assert history['verified_list_scopes'] == 4007
    assert history['unresolved_category_scopes'] is None


@pytest.mark.parametrize('activity', ['running', 'stopped', 'draining', 'waiting_disk'])
def test_list_observations_remain_observations_not_live_completion_when_stopped(tmp_path, activity):
    status = setup(tmp_path)
    status['activity'] = activity
    result = save(tmp_path, status)
    source = cnyes(result)
    assert source['public_index_progress']['verified_pages'] == 345
    assert source['public_index_progress']['finish_eta_utc'] is None
    assert result['acquisition']['all_sources_eta_seconds'] is None
    if activity != 'running':
        assert source['conditional_eta_seconds'] is None


def test_legacy_disabled_empty_and_foreign_provider_do_not_fake_completion(tmp_path):
    status = _seed(tmp_path)
    result = save(tmp_path, status)
    assert cnyes(result)['public_index_progress'] is cnyes(result)['history_list_progress'] is None
    status['providers']['unregistered:example.org'].update(public_index_progress=index_snapshot(),
                                                         history_list_progress=history_snapshot())
    status['providers']['cnyes']['public_index_progress'] = {'enabled': False, 'article_bodies_accepted_by_index': 0}
    assert cnyes(save(tmp_path, status))['public_index_progress'] is None
    status['providers']['cnyes']['public_index_progress'] = {**index_snapshot(), **dict.fromkeys((
        'source_dates_observed', 'verified_pages', 'metadata_rows', 'public_ID_row_observations',
        'dates_all_reported_pages_observed', 'dates_with_changed_source_total'), 0)}
    status['providers']['cnyes']['history_list_progress'] = {**history_snapshot(), 'current_source_contract_scope_states': {}}
    status['bulk_gap_projection'] = {**gaps_snapshot(), 'groups': [], 'total_gaps': 0}
    result = save(tmp_path, status)
    source = cnyes(result)
    assert source['public_index_progress']['verified_pages'] == 0
    assert not source['public_index_progress']['all_history_complete']
    assert source['history_list_progress']['verified_list_scopes'] == 0
    assert source['history_list_progress']['unresolved_category_scopes'] == 0
    foreign = next(row for row in result['providers'] if row['id'] == 'unregistered:example.org')
    assert foreign['public_index_progress'] is foreign['history_list_progress'] is None


def test_new_list_projection_never_imports_collector_opens_queue_or_calls_network(tmp_path, monkeypatch):
    status = setup(tmp_path)
    import builtins
    import sqlite3
    import socket
    original_import = builtins.__import__

    def deny_import(name, *args, **kwargs):
        assert not name.startswith('downloader.news_fulltext')
        return original_import(name, *args, **kwargs)

    def deny_access(*args, **kwargs):
        pytest.fail('public list view must not access a queue or publisher')

    monkeypatch.setattr(builtins, '__import__', deny_import)
    monkeypatch.setattr(sqlite3, 'connect', deny_access)
    monkeypatch.setattr(socket, 'create_connection', deny_access)
    assert cnyes(save(tmp_path, status))['public_index_progress']['verified_pages'] == 345


@pytest.fixture(scope='module')
def browser():
    from playwright.sync_api import sync_playwright
    from scripts.probe_browser_runtime import browser_profile_launch_options
    with sync_playwright() as runtime:
        instance = runtime.chromium.launch(**browser_profile_launch_options('cpu-2d'), timeout=10000)
        try:
            yield instance
        finally:
            instance.close()


@pytest.mark.parametrize('case', ['legacy', 'invalid_index', 'invalid_history', 'observed'])
def test_actual_reader_renders_fixture_missingness_and_keeps_native_details(tmp_path, browser, case):
    """Loopback fixture server; this is not a live provider/UI acceptance."""
    from playwright.sync_api import expect
    status = setup(tmp_path)
    if case == 'legacy':
        status['providers']['cnyes'].pop('public_index_progress')
        status['providers']['cnyes'].pop('history_list_progress')
    elif case == 'invalid_index':
        status['providers']['cnyes']['public_index_progress']['metadata_rows'] = True
    elif case == 'invalid_history':
        status['providers']['cnyes']['history_list_progress']['retired_gap_receipts'] = True
    save(tmp_path, status)
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f'127.0.0.1:{server.server_address[1]}'
    page = browser.new_page(viewport={'width': 320, 'height': 900})
    errors, nonlocal_calls = [], []
    page.on('pageerror', lambda exc: errors.append(type(exc).__name__))
    page.on('request', lambda req: nonlocal_calls.append(req.url) if urlsplit(req.url).netloc != origin else None)
    try:
        page.goto(f'http://{origin}/news/', wait_until='domcontentloaded', timeout=15000)
        page.wait_for_function('() => state.latest?.schema_version === 1', timeout=15000)
        page.locator('a[href="#acq-catalog"]').click(timeout=10000)
        page.locator('#source-search').fill('cnyes')
        detail = page.locator('[data-news-list-provider="cnyes"]')
        expect(detail).to_have_count(1)
        detail.locator('summary').click(timeout=10000)
        expect(detail).to_have_attribute('open', '')
        expect(detail.locator('[data-news-list-eta]')).to_contain_text('完成時間未知')
        assert detail.locator('progress').count() == 0
        if case in ('legacy', 'invalid_index'):
            expect(detail).to_have_attribute('data-news-index-state', 'unavailable')
            expect(detail).to_contain_text('不補零、不算完成')
            expect(detail.locator('[data-news-index-count]')).to_have_count(0)
        else:
            expect(detail).to_have_attribute('data-news-index-state', 'observed')
            expect(detail.locator('[data-news-index-count="pages"]')).to_have_text('345')
            expect(detail.locator('[data-news-index-count="rows"]')).to_have_text('10,218')
        if case in ('legacy', 'invalid_history'):
            expect(detail).to_contain_text('缺口數未知，不視為已補齊')
            expect(detail.locator('[data-news-history-count]')).to_have_count(0)
        else:
            expect(detail.locator('[data-news-history-count="gaps"]')).to_have_text('1,518')
        page.locator('#source-search').fill('no-matching-source')
        expect(detail).to_have_count(0)
        page.locator('#source-search').fill('cnyes')
        expect(detail).to_have_attribute('open', '')
        detail.locator('summary').focus()
        page.keyboard.press('Enter')
        expect(detail).not_to_have_attribute('open', '')
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        assert not errors and not nonlocal_calls
    finally:
        page.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)

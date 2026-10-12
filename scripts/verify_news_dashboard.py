#!/usr/bin/env python3
"""Rendered, source-backed acceptance of the read-only news progress page.

Use native controls on the deployed page. No publisher requests, collector
restart, article content, force-click or synthetic DOM click is involved.
The explicit cpu-2d Chromium profile does not certify GPU/WebGL rendering.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import re
import sys
from urllib.parse import urljoin, urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from scripts.probe_browser_runtime import browser_profile_launch_options  # noqa: E402


def required_source_role(source: dict | None, *, api_setup: bool = False) -> str:
    """A configured API phase is not an enabled full-text collector."""
    assert source and source.get('configured') is True
    if not api_setup:
        assert source.get('enabled') is True
        return 'enabled_collector'
    setup = source.get('authorized_api_setup')
    assert source.get('enabled') is False and source.get('state') == 'authorized_api_pending'
    assert isinstance(setup, dict) and setup.get('user_authorization_declared') is True
    assert setup.get('phase') == 'capability_discovery_only'
    assert setup.get('connection_verified') is False and setup.get('quota_verified') is False
    assert setup.get('fulltext_pipeline_enabled') is False
    assert source.get('conditional_eta_seconds') is None
    return 'authorized_api_setup_only'


def required_provider_card(page, provider: str):
    """Search is intentionally fuzzy; acceptance must bind the exact source ID.

    Names may collide (for example CMoney and CMoney News), and a source ID
    can be a prefix of another ID. Never accept the first matching card.
    """
    identity = page.locator('.news-provider-identity small').filter(
        has_text=re.compile(r'^' + re.escape(provider) + r'$'))
    return page.locator('#pipeline-grid .pipeline-card').filter(has=identity)


def original_source_inventory_check(status):
    """Independent conservation check on the public, non-article incidence."""
    summary = status['original_source_inventory']
    assert status['original_source_inventory_contract_version'] == 1 and summary['available'] is True
    assert summary['body_completion_claimed'] is False and summary['metadata_identity_verified'] is False
    assert summary['publisher_requests'] == 0
    evidence = [row['original_metadata'] for row in status['providers'] if row.get('original_metadata')]
    assert summary['source_keys'] == len(evidence)
    assert summary['literal_metadata_links'] == sum(row['literal_metadata_links'] for row in evidence)
    assert summary['original_hosts'] == sum(row['original_hosts'] for row in evidence)
    original = status['original_url_resolution']
    assert original['snapshot_consistent'] is True
    assert summary['literal_metadata_links'] == original['literal_original_links']
    metadata_only = [row for row in status['providers'] if row.get('original_metadata') and row['known_urls'] == 0]
    for row in metadata_only:
        assert row['complete_urls'] == row['remaining_urls'] == 0 and not row['enabled']
        assert row['progress_ratio'] is row['conditional_eta_seconds'] is None
    return {'summary': summary, 'metadata_only_source_keys': len(metadata_only)}


def verify(url: str, output: Path, *, required_providers: tuple[str, ...] = (),
           required_api_setup_providers: tuple[str, ...] = (), require_original_url_progress: bool = False,
           require_source_reviews: bool = False, require_original_source_inventory: bool = False) -> dict:
    from playwright.sync_api import expect, sync_playwright

    output.mkdir(parents=True, exist_ok=True)
    origin = urlparse(url).netloc
    observations = []
    with sync_playwright() as runtime:
        browser = runtime.chromium.launch(**browser_profile_launch_options("cpu-2d"), timeout=10000)
        try:
            context = browser.new_context()
            try:
                response = context.request.get(urljoin(url, "api/status"), timeout=30000)
                assert response.status == 200
                status = response.json()
                assert status["read_only"] is True
                assert status["production_control_possible"] is False
                assert status["scope"]["body_exported"] is False
                assert status["scope"]["publisher_calls_on_page_view"] is False
                assert status["scope"]["historical_completeness_verified"] is False
                assert status["snapshot_consistent"] is True
                assert status["acquisition"]["all_sources_eta_seconds"] is None
                assert len(status["providers"]) > 0
                assert status["acquisition"]["known_urls"] == sum(row["known_urls"] for row in status["providers"])
                if require_original_source_inventory:
                    original_source_inventory_check(status)
                if require_source_reviews:
                    assert status['source_policy_review_contract_version'] == 1
                    assert status['source_reviews_state'] == 'loaded'
                    assert status['scope']['source_reviews_grant_capture_permission'] is False
                    assert sum(status['source_policy_review_counts'].values()) == len(status['providers'])
                assert response.headers.get("cache-control") == "no-store"
                assert context.request.head(urljoin(url, "api/status"), timeout=30000).status == 200
                # Explicitly rejected control request: no crawler operation.
                assert context.request.post(urljoin(url, "api/status"), timeout=30000).status == 405
                transport = {"status": response.status, "cache_control": response.headers.get("cache-control"),
                             "head_status": 200, "control_status": 405}
            finally:
                context.close()
            for name, width in (("desktop", 1440), ("tablet", 768), ("mobile", 390), ("small-mobile", 320)):
                page = browser.new_page(viewport={"width": width, "height": 900}, device_scale_factor=1)
                errors, remote_calls, failed_resources = [], [], []
                page.on("pageerror", lambda exc: errors.append(type(exc).__name__))
                page.on("request", lambda request: remote_calls.append(request.url)
                        if urlparse(request.url).scheme in {"http", "https"}
                        and urlparse(request.url).netloc != origin else None)
                page.on("response", lambda response: failed_resources.append({
                    "path": urlparse(response.url).path, "status": response.status}) if response.status >= 400 else None)
                response = page.goto(url, wait_until="domcontentloaded", timeout=30000)
                assert response and response.status == 200
                page.wait_for_function("() => state.latest?.schema_version === 1", timeout=30000)
                expect(page.locator('[data-dashboard-nav] a[aria-current="page"]')).to_have_count(1)
                expect(page.locator('[data-dashboard-nav] a[aria-current="page"]')).to_have_attribute("href", "/news/")
                expect(page.locator("#global-eta")).to_have_text("未知")
                expect(page.locator("#acq-overview")).to_be_visible()
                # Compare visible figures against the same render generation;
                # the live owner may publish a newer snapshot between requests.
                facts = page.evaluate("""() => ({
                    observed_at_utc: state.latest.observed_at_utc,
                    health: state.latest.health,
                    source_count: state.latest.providers.length,
                    known: state.latest.acquisition.known_urls,
                    stored: state.latest.acquisition.stored_body_articles,
                    remaining: state.latest.acquisition.remaining_urls,
                    stored_label: document.querySelector('#body-count').textContent,
                    known_label: document.querySelector('#known-count').textContent,
                    remaining_label: document.querySelector('#remaining-count').textContent,
                    actual_progress: document.querySelector('#download-progress').getAttribute('value'),
                    expected_progress: state.latest.acquisition.progress_ratio,
                    summary: document.querySelector('#acq-overview').textContent,
                    document_width: document.documentElement.scrollWidth
                })""")
                assert int(facts["stored_label"].replace(",", "")) == facts["stored"]
                assert int(facts["known_label"].replace(",", "")) == facts["known"]
                assert int(facts["remaining_label"].replace(",", "")) == facts["remaining"]
                assert abs(float(facts["actual_progress"]) - facts["expected_progress"]) < 1e-12
                assert facts["document_width"] <= width
                original = page.evaluate("state.latest.original_url_resolution")
                if require_original_url_progress:
                    assert original and original['snapshot_consistent'] is True
                original_checked = None
                if original:
                    page.locator('a[href="#acq-progress"]').click(timeout=10000)
                    expect(page.locator('#acq-progress')).to_have_attribute('open', '')
                    expect(page.locator('#original-url-panel')).to_be_visible()
                    page.locator('#original-url-panel > summary').click(timeout=10000)
                    expect(page.locator('#original-url-panel')).to_have_attribute('open', '')
                    expect(page.locator('#original-url-detail')).to_contain_text('非唯一原文篇數')
                    expect(page.locator('#original-url-panel')).to_contain_text('不是正文下載完成')
                    original_checked = page.evaluate("""() => ({
                        observed_at_utc: state.latest.observed_at_utc,
                        metadata: state.latest.original_url_resolution,
                        counts_label: document.querySelector('#original-url-count').textContent,
                        actual_progress: document.querySelector('#original-url-progress').getAttribute('value'),
                        eta_label: document.querySelector('#original-url-eta').textContent,
                        document_width: document.documentElement.scrollWidth
                    })""")
                    metadata = original_checked['metadata']
                    assert metadata['body_completion_claimed'] is False and metadata['metadata_identity_verified'] is False
                    assert metadata['publisher_requests'] == 0
                    assert metadata['examined_urls'] == sum(metadata[key] for key in (
                        'literal_original_links', 'opaque_links', 'unresolved_links'))
                    assert metadata['links_to_queued_targets'] <= metadata['literal_original_links']
                    expected_counts = '{:,} / {:,} 筆連結'.format(metadata['examined_urls'], metadata['known_redirect_urls'])
                    assert original_checked['counts_label'] == expected_counts
                    assert abs(float(original_checked['actual_progress']) - metadata['progress_ratio']) < 1e-12
                    assert original_checked['document_width'] <= width
                    page.locator('#original-url-panel').screenshot(path=str(output / f'{name}-original-urls.png'), timeout=10000)
                    page.locator('#original-url-panel > summary').click(timeout=10000)
                    page.locator('#acq-progress > summary').click(timeout=10000)
                original_sources_checked = None
                if require_original_source_inventory:
                    current = page.evaluate('state.latest')
                    original_sources_checked = original_source_inventory_check(current)
                    candidates = [row for row in current['providers'] if row.get('original_metadata') and row['known_urls'] == 0]
                    assert candidates
                    candidate = max(candidates, key=lambda row: row['original_metadata']['literal_metadata_links'])
                    page.locator('a[href="#acq-catalog"]').click(timeout=10000)
                    expect(page.locator('#acq-catalog')).to_have_attribute('open', '')
                    page.locator('[data-news-filter="all"]').click(timeout=10000)
                    page.locator('#source-search').fill(candidate['id'])
                    card = required_provider_card(page, candidate['id'])
                    expect(card).to_have_count(1)
                    expect(card.locator('.news-provider-facts dd').first).to_have_text('未入佇列')
                    expect(card).to_contain_text('新增正文速率尚未量測')
                    displayed = int(card.locator('[data-news-original-links]').text_content().replace(',', ''))
                    assert displayed == candidate['original_metadata']['literal_metadata_links']
                    assert card.locator('progress').get_attribute('value') is None
                    expect(card).to_contain_text('不是正文筆數、不授予抓取或使用權限')
                    assert page.evaluate('document.documentElement.scrollWidth') <= width
                    card.screenshot(path=str(output / f'{name}-original-source-only.png'), timeout=10000)
                    original_sources_checked.update(provider=candidate['id'], displayed_metadata_links=displayed,
                                                    unknown_progress_visible=True, access_permission_granted=False)
                    page.locator('#source-search').fill('')
                    page.locator('[data-news-filter="all"]').click(timeout=10000)
                    page.locator('#acq-catalog > summary').click(timeout=10000)
                page.screenshot(path=str(output / f"{name}-summary.png"), full_page=True, timeout=10000)
                for group in ("progress", "quota", "catalog", "method"):
                    page.locator(f'a[href="#acq-{group}"]').click(timeout=10000)
                    expect(page.locator(f"#acq-{group}")).to_have_attribute("open", "")
                # An open disclosure is not sufficient for an offscreen lazy
                # renderer. Visit the actual sections before checking its DOM.
                page.locator("#capture").scroll_into_view_if_needed(timeout=10000)
                expected_stages = page.evaluate("state.latest.stages.length")
                expect(page.locator("#stage-rows tr")).to_have_count(max(1, expected_stages))
                page.locator("#traffic").scroll_into_view_if_needed(timeout=10000)
                configured = page.evaluate("state.latest.providers.filter(source => source.configured).length")
                expect(page.locator("#traffic-rows tr")).to_have_count(configured)
                bounds = page.evaluate("""() => ['traffic', 'capture'].map(id => {
                    const section = document.getElementById(id).getBoundingClientRect();
                    const table = document.querySelector('#' + id + ' table').getBoundingClientRect();
                    const scroller = document.querySelector('#' + id + ' .table-scroll');
                    const box = scroller.getBoundingClientRect();
                    const overflow = getComputedStyle(scroller).overflowY;
                    const caption = document.querySelector('#' + id + ' caption').getBoundingClientRect();
                    return {id, section_bottom: section.bottom, table_bottom: table.bottom,
                        overflow_y: overflow,
                        caption_usable: caption.width >= table.width * 0.95 && caption.height < 160,
                        contained: section.bottom >= table.bottom ||
                            (['auto', 'scroll', 'hidden', 'clip'].includes(overflow) && section.bottom >= box.bottom)};
                })""")
                assert all(row["contained"] and row["caption_usable"] for row in bounds), bounds
                page.locator('[data-news-filter="enabled"]').click(timeout=10000)
                enabled = page.evaluate("state.latest.providers.filter(source => source.enabled).length")
                expect(page.locator("#pipeline-grid .pipeline-card")).to_have_count(min(enabled, 24))
                page.locator("#pipeline-grid").screenshot(path=str(output / f"{name}-enabled-sources.png"), timeout=10000)
                required_cards = []
                required_roles = [(p, False) for p in required_providers] + [(p, True) for p in required_api_setup_providers]
                for provider, api_setup in required_roles:
                    selected = page.evaluate("id => state.latest.providers.find(source => source.id === id)", provider)
                    role = required_source_role(selected, api_setup=api_setup)
                    filter_name = 'not_enabled' if api_setup else 'enabled'
                    page.locator(f'[data-news-filter="{filter_name}"]').click(timeout=10000)
                    page.locator("#source-search").fill(provider)
                    card = required_provider_card(page, provider)
                    expect(card).to_have_count(1)
                    expect(card.locator(".news-provider-identity small")).to_have_text(provider)
                    checked = page.evaluate("""id => {
                        const source = state.latest.providers.find(row => row.id === id);
                        const card = [...document.querySelectorAll('#pipeline-grid .pipeline-card')]
                            .find(node => node.querySelector('.news-provider-identity small')?.textContent === id);
                        const cohort = source.finite_cohort;
                        const displayed = card.querySelector('.news-provider-facts dd').textContent
                            .replaceAll(',', '').split('/').map(part => Number(part.trim()));
                        return {provider: id, observed_at_utc: state.latest.observed_at_utc,
                            stored_heads: cohort ? cohort.stored_bodies : source.complete_urls,
                            known_urls: cohort ? cohort.urls : source.known_urls,
                            displayed_counts: displayed,
                            progress_ratio: source.progress_ratio,
                            displayed_ratio: card.querySelector('progress').value,
                            finite_supplements: source.finite_supplements,
                            reviewed_nonbody_outside_cohort: cohort && cohort.reviewed_nonbody_outside_cohort,
                            unreviewed_new_urls: cohort && cohort.unreviewed_new_urls,
                            growth_waiting_urls: source.growth_waiting_urls,
                            cohort_note: cohort && card.querySelector('.news-provider-states p').textContent,
                            source_image_only: source.states.source_body_image_only || 0,
                            image_only_label_visible: card.textContent.includes('來源正文僅圖片，未做 OCR'),
                            source_embedded_only: source.states.source_body_embedded_only || 0,
                            embedded_only_label_visible: card.textContent.includes('來源正文僅外部嵌入，未取得文字'),
                            source_quote_only: source.states.source_body_quote_only || 0,
                            quote_only_label_visible: card.textContent.includes('原站只提供行情片段，未取得新聞正文'),
                            body_work_progress: source.body_work_progress,
                            robots_origin_policy_pause: source.robots_origin_policy_pause,
                            completion_forecast: source.completion_forecast,
                            displayed_forecast_rate: card.querySelector('[data-news-rate="completion"]')?.textContent,
                            expected_forecast_rate: decimal((source.completion_forecast
                                ? source.completion_forecast.new_url_heads_per_second
                                : source.owner_articles_per_second) == null ? null : (source.completion_forecast
                                ? source.completion_forecast.new_url_heads_per_second
                                : source.owner_articles_per_second) * 60),
                            forecast_state: card.querySelector('[data-news-forecast]')?.dataset.newsForecast,
                            partial_origin_pause_label_visible: card.textContent.includes('局部端點暫停'),
                            origin_probe_clock_visible: !!source.robots_origin_policy_pause?.next_policy_probe_at_utc &&
                                card.textContent.includes(stamp(source.robots_origin_policy_pause.next_policy_probe_at_utc)),
                            displayed_work_counts: [...card.querySelectorAll('[data-news-work]')].map(node => Number(node.textContent.replaceAll(',', '')))};
                    }""", provider)
                    assert checked["displayed_counts"] == [checked["stored_heads"], checked["known_urls"]], checked
                    assert checked["displayed_ratio"] == checked["progress_ratio"], checked
                    checked['verification_role'] = role
                    assert checked['displayed_forecast_rate'] == checked['expected_forecast_rate'], checked
                    forecast = checked.get('completion_forecast')
                    if forecast:
                        assert forecast['contract_version'] == 1 and not forecast['all_history_complete']
                        assert checked['forecast_state'] == forecast['state'], checked
                        expect(card).to_contain_text('與估時速率分開')
                        if forecast['state'] == 'recent_measured':
                            expect(card).to_contain_text(f"估時採近期 {forecast['sample_count']:,} 個完整視窗")
                            expect(card).to_contain_text('不挑最快視窗')
                    pause = checked.get('robots_origin_policy_pause')
                    if pause:
                        assert pause['contract_version'] == 1 and pause['scope'] == 'initial_origin_policy'
                        assert pause['permission_granted'] is False and pause['provider_recovery_verified'] is False
                        if pause['active_origin_count']:
                            expect(card).to_contain_text(f"局部端點暫停 {pause['active_origin_count']:,} 個 origin")
                            expect(card).to_contain_text('到期不代表恢復存取')
                            assert checked['partial_origin_pause_label_visible'] and checked['origin_probe_clock_visible'], checked
                    if provider == 'cnyes':
                        work = checked['body_work_progress']
                        assert work and work['contract_version'] == 1
                        assert not work['candidate_views_accepted_by_hint'] and not work['candidate_body_equivalence_verified']
                        assert checked['displayed_work_counts'] == [work[key] for key in
                            ('no_known_primary_body_urls', 'existing_primary_body_candidates', 'stored_body_recheck_urls')]
                        expect(card).to_contain_text('同 ID 是排程提示，不代表正文相同或已完成')
                    if api_setup:
                        expect(card).to_contain_text('授權 API 待接線')
                        expect(card).to_contain_text('CNA_API_KEY')
                        expect(card).to_contain_text('目前未開始 API 全文下載，完成時間未知')
                        expect(card).to_contain_text('官方 API 能力檢查已實作，全文管線未啟用')
                        expect(card).not_to_contain_text('共用下載器已接線')
                        expect(card).not_to_contain_text('未啟用／待確認')
                        checked['api_setup_note_verified'] = True
                    if checked["cohort_note"]:
                        supplement = checked["finite_supplements"]
                        if supplement:
                            expected = "已核對追加 {:,} / {:,} 正文".format(supplement['stored_bodies'], supplement['urls'])
                            assert expected in checked["cohort_note"], checked
                        expected = "未核對新增 {:,} 個網址（待排 {:,}）".format(
                            checked['unreviewed_new_urls'], checked['growth_waiting_urls'])
                        assert expected in checked["cohort_note"], checked
                        outcomes = checked['reviewed_nonbody_outside_cohort']
                        if outcomes and outcomes['urls']:
                            expected = '已核對非正文 {:,} 個網址（來源不存在 {:,}，非文章路徑 {:,}），不計入已存正文'.format(
                                outcomes['urls'], outcomes['states'].get('source_not_found', 0),
                                outcomes['states'].get('unsupported_article_path', 0))
                            assert expected in checked['cohort_note'], checked
                    if checked["source_image_only"]:
                        assert checked["image_only_label_visible"], checked
                    if checked["source_embedded_only"]:
                        assert checked["embedded_only_label_visible"], checked
                    if checked["source_quote_only"]:
                        assert checked["quote_only_label_visible"], checked
                    card.screenshot(path=str(output / f"{name}-required-{provider}.png"), timeout=10000)
                    required_cards.append(checked)
                page.locator("#source-search").fill("")
                source_reviews = None
                if require_source_reviews:
                    page.locator('[data-news-filter="permission"]').click(timeout=10000)
                    expected = page.evaluate("state.latest.providers.filter(s => s.source_policy_review === 'permission_required').length")
                    expect(page.locator('#pipeline-grid .pipeline-card')).to_have_count(min(expected, 24))
                    expect(page.locator('#source-results')).to_contain_text(f'{expected:,} 個符合來源')
                    expect(page.locator('#source-review-summary')).to_contain_text(f'需取得來源同意 {expected:,} 個')
                    if expected:
                        expect(page.locator('#pipeline-grid .pipeline-card').first).to_contain_text('需取得來源同意')
                        expect(page.locator('#pipeline-grid .pipeline-card').first).to_contain_text('核對 ')
                    source_reviews = {'permission_required_sources': expected,
                                      'count_reconciled': True, 'review_clock_visible': bool(expected),
                                      'filter_applied': True, 'capture_permission_granted': False}
                    page.locator('#pipeline-grid').screenshot(path=str(output / f'{name}-source-reviews.png'), timeout=10000)
                page.locator('[data-news-filter="all"]').click(timeout=10000)
                expect(page.locator("#pipeline-grid .pipeline-card")).to_have_count(min(facts["source_count"], 24))
                first_name = page.evaluate("state.latest.providers[0].name")
                page.locator("#source-search").fill(first_name)
                expect(page.locator("#pipeline-grid .pipeline-card").first).to_contain_text(first_name)
                page.locator("#source-search").fill("no-such-news-provider-acceptance")
                expect(page.locator("#pipeline-empty")).to_be_visible()
                expect(page.locator("#pipeline-grid .pipeline-card")).to_have_count(0)
                page.locator("#source-search").fill("")
                page.locator("#source-sort").select_option("remaining")
                page.wait_for_function("() => document.querySelectorAll('#pipeline-grid .pipeline-card').length > 0")
                assert page.evaluate("""() => {
                    const rows = state.latest.providers;
                    const maximum = Math.max(...rows.map(row => row.remaining_urls || 0));
                    return document.querySelector('#pipeline-grid .pipeline-card small').textContent
                        === rows.find(row => (row.remaining_urls || 0) === maximum).id;
                }""")
                page.locator("#source-sort").select_option("state")
                # Exercise every page through its normal next button. This
                # proves that the complete source list is reachable, not Top K.
                visited_pages = 1
                while page.locator("#source-next").is_enabled():
                    page.locator("#source-next").click(timeout=10000)
                    visited_pages += 1
                    assert visited_pages <= 500, "pagination did not converge"
                catalog = page.evaluate("""() => ({
                    total: state.latest.providers.length,
                    page: state.page,
                    page_label: document.querySelector('#source-page').textContent,
                    rendered: document.querySelectorAll('#pipeline-grid .pipeline-card').length,
                    traffic_rows: document.querySelectorAll('#traffic-rows tr').length,
                    configured: state.latest.providers.filter(source => source.configured).length,
                    stages: document.querySelectorAll('#stage-rows tr').length,
                    expected_stages: state.latest.stages.length,
                    horizontal_overflow: document.documentElement.scrollWidth > innerWidth
                })""")
                assert visited_pages == max(1, (catalog["total"] + 23) // 24)
                assert catalog["rendered"] == (catalog["total"] - 1) % 24 + 1
                assert catalog["traffic_rows"] == catalog["configured"]
                assert catalog["stages"] == max(1, catalog["expected_stages"])
                assert not catalog["horizontal_overflow"]
                page.locator('a[href="#acq-quota"]').click(timeout=10000)
                page.locator("#traffic").screenshot(path=str(output / f"{name}-traffic.png"), timeout=10000)
                refresh_failure = None
                if name == 'desktop' and original:
                    # A read-only local browser transport failure. Let the
                    # page's real timed refresh/retry execute; do not mutate
                    # the collector, status payload or DOM to fake acceptance.
                    page.route('**/api/status', lambda route: route.abort())
                    page.wait_for_function("() => state.latest?.health === 'stale'", timeout=30000)
                    assert page.locator('#original-url-eta').text_content() == '目前暫無有效估時'
                    refresh_failure = {'stale_metadata_eta_invalidated': True,
                                       'collector_state_changed': False, 'browser_status_request_only': True}
                    page.unroute('**/api/status')
                    page.wait_for_function("() => state.latest?.health !== 'stale' && !state.inFlight", timeout=30000)
                    refresh_failure['native_timed_retry_recovered'] = True
                assert not errors and not remote_calls and not failed_resources, (errors, remote_calls, failed_resources)
                observations.append({"viewport": name, "width": width, **facts, "catalog": catalog,
                                     "long_tables_contained": bounds,
                                     "original_url_progress": original_checked, "browser_refresh_failure": refresh_failure,
                                     "original_source_inventory": original_sources_checked,
                                     "source_reviews": source_reviews,
                                     "required_provider_cards": required_cards,
                                     "native_controls": "passed", "all_source_pages_reached": visited_pages,
                                     "javascript_errors": errors, "publisher_requests": remote_calls,
                                     "failed_resources": failed_resources})
                page.close()
        finally:
            browser.close()
    return {"schema_version": 1, "nontext_outcome_observation_contract_version": 1,
            "state": "accepted", "generated_at_utc": datetime.now(UTC).isoformat(),
            "page": url, "browser_profile": "cpu-2d", "gpu_webgl_verified": False,
            "transport": transport, "viewports": observations,
            "scope": "news read-only dashboard; not publisher history completeness or crawler repair"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="https://penguin72487.ddnsgeek.com/news/")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--require-provider", action="append", default=[], help="Enabled source ID whose displayed counts must reconcile")
    parser.add_argument("--require-api-setup-provider", action="append", default=[],
                        help="Configured, disabled API setup source; not accepted as an enabled body collector")
    parser.add_argument('--require-original-url-progress', action='store_true',
                        help='Require the separate metadata URL proof progress contract on every viewport')
    parser.add_argument('--require-source-reviews', action='store_true',
                        help='Verify separate review labels, clocks and native permission filter on every viewport')
    parser.add_argument('--require-original-source-inventory', action='store_true',
                        help='Reconcile all decoded original source keys and render metadata-only cards without 0/0 completion')
    args = parser.parse_args()
    url = args.base_url.rstrip("/") + "/"
    try:
        result = verify(url, args.output_dir, required_providers=tuple(dict.fromkeys(args.require_provider)),
                        required_api_setup_providers=tuple(dict.fromkeys(args.require_api_setup_provider)),
                        require_original_url_progress=args.require_original_url_progress,
                        require_source_reviews=args.require_source_reviews,
                        require_original_source_inventory=args.require_original_source_inventory)
    except Exception as exc:
        result = {"schema_version": 1, "state": "failed", "generated_at_utc": datetime.now(UTC).isoformat(),
                  "page": url, "error_type": type(exc).__name__, "scope": "news dashboard acceptance"}
        atomic_write_json(args.output_dir / "acceptance.json", result)
        raise
    atomic_write_json(args.output_dir / "acceptance.json", result)
    print(json.dumps({"state": result["state"], "viewports": len(result["viewports"]),
                      "acceptance": str(args.output_dir / "acceptance.json")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

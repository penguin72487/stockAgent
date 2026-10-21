"""Feed registration, response pages and accepted bodies have separate grains."""
from datetime import UTC, datetime
import json
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from playwright.sync_api import expect, sync_playwright

from downloader.artifact_io import atomic_write_json
from scripts.probe_browser_runtime import browser_profile_launch_options
from stockagent.live.news_dashboard import _fulltext_feed_inventory, build_news_public_status
from test_news_dashboard import _save, _seed


def feed_snapshot(root, now):
    status = _seed(root, now=now)
    status['providers']['globalvoices'] = status['providers'].pop('cnyes')
    detail = status['performance']['providers'].pop('cnyes')
    detail['counts'].update(http_endpoint_rss_fulltext_terminal_requests=12,
        http_endpoint_rss_fulltext_terminal_status_200=9,
        http_endpoint_rss_fulltext_terminal_status_304=2,
        durable_workload_rss_fulltext_new_heads=2,
        bulk_pages_validated=999999)  # Other bulk adapters are not RSS pages.
    status['performance']['providers']['globalvoices'] = detail
    _save(root, status)
    roots = ['https://globalvoices.org/feed/', 'https://zht.globalvoices.org/feed/']
    months = [f'https://globalvoices.org/{year}/{month:02d}/feed/'
              for year, month in [(2004, 10), (2004, 11), (2006, 9)]]
    atomic_write_json(root / 'configs/news_fulltext.json', {'providers': [{
        'id': 'globalvoices', 'name': 'Global Voices', 'enabled': True,
        'interval_seconds': 10, 'parallelism': 1,
        'fulltext_feeds': roots + roots + months[:1],  # Dedup merged recipes.
        'historical_fulltext_feeds': months + months[:1],
        'api_key': 'PRIVATE_TOKEN'}]})
    return build_news_public_status(root, now=now)


def test_feed_projection_is_allowlisted_and_does_not_infer_history_or_row_caps(tmp_path):
    result = feed_snapshot(tmp_path, datetime.now(UTC))
    source = next(row for row in result['providers'] if row['id'] == 'globalvoices')
    feed = source['fulltext_feed_inventory']
    assert feed == {
        'contract_version': 1, 'registered_root_feeds': 2,
        'registered_historical_feeds': 3, 'registered_monthly_feeds': 3,
        'registered_other_historical_feeds': 0,
        'first_registered_month': '2004-10', 'last_registered_month': '2006-09',
        'owner_terminal_requests': 12, 'owner_200_pages': 9, 'owner_304_pages': 2,
        'owner_new_stored_url_heads': 2,
        'registration_proves_history_complete': False,
        'response_pages_prove_accepted_bodies': False}
    assert source['registered_bulk_endpoints'] == 0
    assert source['observed_bulk_rows_per_request_cap'] is None
    encoded = json.dumps(result)
    assert 'PRIVATE' not in encoded and 'globalvoices.org/feed/' not in encoded
    assert all(row['fulltext_feed_inventory'] is None for row in result['providers'] if row['id'] != 'globalvoices')


@pytest.mark.parametrize('counts', [{}, {'http_endpoint_rss_fulltext_terminal_requests': -1,
    'http_endpoint_rss_fulltext_terminal_status_200': True,
    'durable_workload_rss_fulltext_new_heads': '2'}])
def test_missing_or_invalid_native_metrics_remain_unknown(counts):
    result = _fulltext_feed_inventory({'fulltext_feeds': ['https://example.org/feed/']}, counts)
    assert all(result[key] is None for key in (
        'owner_terminal_requests', 'owner_200_pages', 'owner_304_pages', 'owner_new_stored_url_heads'))


def test_nonmonthly_scopes_are_not_hidden_or_labeled_months():
    settings = {'historical_fulltext_feeds': ['https://example.org/archive/feed/',
        'https://example.org/2005/13/feed/', 'https://[/feed/']}
    result = _fulltext_feed_inventory(settings, {})
    assert result['registered_other_historical_feeds'] == 3
    assert result['registered_monthly_feeds'] == 0 and result['first_registered_month'] is None
    assert _fulltext_feed_inventory({'fulltext_feeds': None}, {}) is None


@pytest.mark.parametrize('width,height', [(1440, 1000), (390, 844)])
def test_real_page_shows_separate_feed_counts_and_preserves_expansion(tmp_path, width, height):
    payload = feed_snapshot(tmp_path, datetime.now(UTC))
    services = Path(__file__).resolve().parents[1] / 'services'
    with sync_playwright() as runtime:
        browser = runtime.chromium.launch(**browser_profile_launch_options('cpu-2d'), timeout=10000)
        try:
            page = browser.new_page(viewport={'width': width, 'height': height})
            errors, forbidden = [], []
            page.on('pageerror', lambda error: errors.append(str(error)))

            def route(request):
                parts = urlsplit(request.request.url)
                if parts.netloc != 'news-preview.test':
                    forbidden.append(parts.netloc)
                    request.abort()
                elif parts.path == '/news/api/status':
                    request.fulfill(content_type='application/json', body=json.dumps(payload))
                else:
                    prefix, _, suffix = parts.path.removeprefix('/').partition('/')
                    relative = 'news_dashboard/' + (suffix or 'index.html') if prefix == 'news' else 'public_dashboards/' + parts.path.removeprefix('/')
                    target = (services / relative).resolve()
                    if not target.is_relative_to(services) or not target.is_file():
                        request.fulfill(status=404, body='')
                    else:
                        mime = {'.js': 'text/javascript', '.css': 'text/css', '.html': 'text/html', '.svg': 'image/svg+xml'}.get(target.suffix, 'application/octet-stream')
                        request.fulfill(content_type=mime, body=target.read_bytes())

            page.route('**/*', route)
            page.goto('http://news-preview.test/news/', wait_until='networkidle', timeout=15000)
            page.locator('a[href="#acq-catalog"]').click(timeout=10000)
            page.locator('#source-search').fill('globalvoices')
            detail = page.locator('[data-news-feed-provider="globalvoices"]')
            detail.locator('summary').click()
            for key, value in [('roots', '2'), ('months', '3'), ('other', '0'),
                               ('requests', '12'), ('pages', '9'), ('unchanged', '2'), ('new-bodies', '2')]:
                expect(detail.locator(f'[data-news-feed-count="{key}"]')).to_have_text(value)
            expect(detail).to_contain_text('登記月份 2004-10～2006-09')
            expect(detail).to_contain_text('不代表歷史已抓齊')
            expect(detail).not_to_contain_text('30 則')
            page.locator('[data-news-filter="not_enabled"]').click()
            expect(page.locator('[data-news-feed-provider]')).to_have_count(0)
            page.locator('[data-news-filter="all"]').click()
            expect(page.locator('[data-news-feed-provider="globalvoices"]')).to_have_attribute('open', '')
            assert page.evaluate('document.documentElement.scrollWidth') <= width
            assert not errors and not forbidden
        finally:
            browser.close()

"""Fixture-only source-card identity guard, not a live dashboard acceptance."""
from playwright.sync_api import expect, sync_playwright
import json
import sys

import pytest

from scripts.probe_browser_runtime import browser_profile_launch_options
from scripts.verify_news_dashboard import required_provider_card


@pytest.mark.parametrize('base_url,expected', [
    (None, 'http://127.0.0.1:8771/news/'),
    ('http://127.0.0.1:8871/news', 'http://127.0.0.1:8871/news/'),
])
def test_cli_uses_owner_listener_by_default_and_preserves_explicit_target(monkeypatch, tmp_path, base_url, expected):
    from scripts import verify_news_dashboard as verifier

    calls = []

    def fixture_verify(url, output, **kwargs):
        calls.append((url, output))
        return {'state': 'accepted', 'viewports': []}

    argv = ['verify_news_dashboard', '--output-dir', str(tmp_path)]
    if base_url is not None:
        argv += ['--base-url', base_url]
    monkeypatch.setattr(sys, 'argv', argv)
    monkeypatch.setattr(verifier, 'verify', fixture_verify)
    assert verifier.main() == 0
    assert calls == [(expected, tmp_path)]
    assert json.loads((tmp_path / 'acceptance.json').read_text())['state'] == 'accepted'


def test_owner_transport_still_rejects_public_audience_and_public_caching():
    from scripts.verify_news_dashboard import require_owner_transport

    require_owner_transport({'x-stockagent-audience': 'owner', 'cache-control': 'private, no-store'})
    for headers in ({'x-stockagent-audience': 'public', 'cache-control': 'private, no-store'},
                    {'x-stockagent-audience': 'owner', 'cache-control': 'public, max-age=60'}):
        with pytest.raises(AssertionError):
            require_owner_transport(headers)


def test_required_source_card_is_exact_when_provider_names_or_prefixes_collide():
    with sync_playwright() as runtime:
        browser = runtime.chromium.launch(**browser_profile_launch_options('cpu-2d'), timeout=10000)
        try:
            page = browser.new_page()
            page.set_content('''<section id="pipeline-grid">
                <article class="pipeline-card"><div class="news-provider-identity"><strong>CMoney</strong><small>cmoney</small></div></article>
                <article class="pipeline-card"><div class="news-provider-identity"><strong>CMoney 新聞</strong><small>cmnews</small></div></article>
                <article class="pipeline-card"><div class="news-provider-identity"><strong>同名</strong><small>cmoney.api</small></div></article>
                <article class="pipeline-card"><div class="news-provider-identity"><strong>同名</strong><small>cmoney.api2</small></div></article>
                </section>''')
            for provider in ('cmoney', 'cmnews', 'cmoney.api', 'cmoney.api2'):
                card = required_provider_card(page, provider)
                expect(card).to_have_count(1)
                expect(card.locator('.news-provider-identity small')).to_have_text(provider)
            expect(required_provider_card(page, 'missing')).to_have_count(0)
            page.close()
        finally:
            browser.close()

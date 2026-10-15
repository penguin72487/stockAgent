"""Fixture-only source-card identity guard, not a live dashboard acceptance."""
from playwright.sync_api import expect, sync_playwright

from scripts.probe_browser_runtime import browser_profile_launch_options
from scripts.verify_news_dashboard import required_provider_card


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

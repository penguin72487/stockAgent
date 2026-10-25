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


def scope_card():
    return {'body_request_scope_progress': {'contract_version': 1, 'available': True,
        'active_missing_body_urls': 3, 'source_paused_missing_body_urls': 2,
        'other_missing_body_urls': 1, 'source_recovery_verified': False},
        'body_work_progress': {'active_missing_body_urls': 3},
        'displayed_request_scope_counts': [2, 1], 'request_scope_note_present': True,
        'conditional_eta_seconds': None, 'conditional_finish_at_utc': None,
        'conditional_eta_unavailable_reason': 'source_request_scope_paused'}


def test_request_scope_acceptance_checks_rendered_counts_and_withdrawn_countdown():
    from scripts.verify_news_dashboard import request_scope_progress_check

    assert request_scope_progress_check(scope_card())['accepted']
    assert request_scope_progress_check({}) is None
    assert request_scope_progress_check({'body_request_scope_progress': {'available': False}}) is None


@pytest.mark.parametrize('field,value', [
    ('displayed_request_scope_counts', [3, 0]), ('request_scope_note_present', False),
    ('conditional_eta_seconds', 30), ('conditional_finish_at_utc', '2026-10-24T00:00:00+00:00'),
    ('conditional_eta_unavailable_reason', None),
])
def test_request_scope_acceptance_rejects_stale_or_inconsistent_card(field, value):
    from scripts.verify_news_dashboard import request_scope_progress_check

    checked = scope_card(); checked[field] = value
    with pytest.raises(AssertionError):
        request_scope_progress_check(checked)


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

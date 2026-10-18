#!/usr/bin/env python3
"""Read-only browser acceptance for public/owner dashboard readers.

Use the two canonical loopback readers or an already authenticated proxy path.
No private identity is injected through headers/cookies by this verifier.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--public-url', default='http://127.0.0.1:8770')
    parser.add_argument('--owner-url', default='http://127.0.0.1:8771')
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    result = {'pass': False, 'cases': [], 'scope': 'real Chromium, two server-owned audiences; no real iPhone or remote VPN client'}
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            for audience, base in [('public', args.public_url.rstrip('/')), ('owner', args.owner_url.rstrip('/'))]:
                for width in [1280, 390]:
                    context = browser.new_context(viewport={'width': width, 'height': 900}, device_scale_factor=1)
                    page = context.new_page()
                    errors, requests = [], []
                    page.on('pageerror', lambda error: errors.append(str(error)))
                    page.on('request', lambda r: requests.append(urlsplit(r.url).path))
                    response = page.goto(base + '/', wait_until='domcontentloaded', timeout=45000)
                    assert response.status == 200
                    page.wait_for_function("window.StockAgentDashboard && document.querySelector('nav[data-dashboard-nav] a')", timeout=15000)
                    assert page.locator('meta[name="stockagent-audience"]').get_attribute('content') == audience
                    links = page.locator('nav[data-dashboard-nav] a').evaluate_all('(nodes) => nodes.map(n => new URL(n.href).pathname)')
                    cards = page.locator('a.dashboard-card').evaluate_all('(nodes) => nodes.map(n => new URL(n.href).pathname)')
                    expected = ['/', '/tw-day-trade/', '/tw-overnight/']
                    if audience == 'public':
                        assert links == expected, links
                        assert cards == expected[1:], cards
                        assert page.get_by_role('heading', name='資料抓取與來源總覽', exact=True).count() == 0
                        assert page.locator('#dashboards a.dashboard-card').count() == 2
                    else:
                        assert len(links) == 12 and len(cards) == 11
                    access_response = context.request.get(base + '/api/access')
                    assert access_response.status == 200 and access_response.json()['audience'] == audience
                    overview_response = context.request.get(base + '/api/overview', timeout=45000)
                    assert overview_response.status == 200
                    overview = overview_response.json()
                    if audience == 'public':
                        assert not set(overview).intersection({'taifex', 'shioaji', 'finlab', 'finmind', 'tej', 'openbb', 'data_monitor', 'traffic'})
                        assert not any(path.startswith(('/tej/', '/finlab/', '/traffic/', '/data-monitor/', '/taifex/')) for path in requests)
                    page.screenshot(path=str(args.output_dir / f'{audience}-home-{width}.png'), full_page=True)
                    home_overflow = page.evaluate('document.documentElement.scrollWidth > innerWidth + 1')
                    assert not home_overflow
                    private_response = context.request.get(base + '/tej/')
                    assert private_response.status == (403 if audience == 'public' else 200)
                    response = page.goto(base + '/tw-day-trade/', wait_until='domcontentloaded', timeout=45000)
                    assert response.status == 200
                    page.wait_for_function("window.StockAgentDashboard && document.querySelector('nav[data-dashboard-nav] a')", timeout=15000)
                    assert page.locator('nav[data-dashboard-nav] a').count() == (3 if audience == 'public' else 12)
                    page.wait_for_timeout(1000)
                    assert not page.evaluate('document.documentElement.scrollWidth > innerWidth + 1')
                    page.screenshot(path=str(args.output_dir / f'{audience}-tw-{width}.png'), full_page=False)
                    assert not errors, errors
                    result['cases'].append({'audience': audience, 'width': width, 'navigation': links,
                                             'cards': cards, 'overview_keys': sorted(overview), 'page_errors': errors,
                                             'horizontal_overflow': False, 'private_page_status': private_response.status})
                    context.close()
            browser.close()
        result['pass'] = True
    except Exception as error:
        result['error'] = type(error).__name__ + ': ' + str(error)
    (args.output_dir / 'acceptance.json').write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(json.dumps({'pass': result['pass'], 'cases': len(result['cases']),
                      'report': str(args.output_dir / 'acceptance.json'), 'error': result.get('error')}, ensure_ascii=False))
    return 0 if result['pass'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

"""Actual existing reader, two distinct progress grains and private projection."""
from datetime import UTC, datetime
import json
import threading
from urllib.parse import urlsplit

import pytest
from playwright.sync_api import expect

from downloader.artifact_io import atomic_write_json
from scripts.verify_news_dashboard import BODY_PROGRESS_SELECTOR, required_provider_card
from stockagent.live.news_dashboard import build_news_public_status
from test_news_dashboard import _seed, _server
from test_news_dashboard_cnyes_list_progress import browser  # noqa: F401 -- shared CPU browser fixture


def setup(root):
    now = datetime.now(UTC)
    status = _seed(root, now=now)
    status.update(provider_stored_body_counter_contract_version=1, provider_body_state_counter_contract_version=1,
        unique_articles=264, articles_with_stored_body=230,
        states={'complete': 230, 'pending': 4, 'awaiting_authorization': 4, 'unsupported_article_path': 26})
    status['providers']['cnyes'].update(stored_body_urls=2,
        body_state_counts={'missing': {'pending': 4}, 'stored': {'complete': 2}})
    status['providers']['unregistered:example.org'].update(stored_body_urls=0,
        body_state_counts={'missing': {'awaiting_authorization': 4}, 'stored': {}})
    status['providers']['pocket'] = {'states': {'complete': 228, 'unsupported_article_path': 26},
        'stored_body_urls': 228, 'automatic_collection_enabled': True,
        'authorization_kind': 'personal_research',
        'body_state_counts': {'missing': {'unsupported_article_path': 26}, 'stored': {'complete': 228}},
        'publisher_body_references': {'contract_version': 1, 'registered_urls': 11,
            'verified_reference_urls': 11, 'inactive_reference_urls': 0, 'new_article_bodies': 0,
            'raw_alias_response_retained': False, 'all_history_complete': False,
            'private_proof': 'PRIVATE_REFERENCE_PROOF'}}
    atomic_write_json(root/'data_news_fulltext/status.json', status)
    config_path = root/'configs/news_fulltext.json'
    config = json.loads(config_path.read_bytes())
    config['providers'].append({'id': 'pocket', 'name': '口袋學堂', 'enabled': True, 'interval_seconds': 2, 'parallelism': 1})
    atomic_write_json(config_path, config)
    return now


@pytest.mark.parametrize('width', [1440, 768, 390, 320])
def test_live_reader_distinguishes_reference_resolution_from_new_bodies(tmp_path, browser, width):
    now = setup(tmp_path)
    status = build_news_public_status(tmp_path, now=now)
    assert status['snapshot_consistent'] and 'PRIVATE_REFERENCE_PROOF' not in json.dumps(status)
    source = next(row for row in status['providers'] if row['id'] == 'pocket')
    assert source['publisher_reference_coverage']['unresolved_url_heads'] == 15
    assert source['stored_body_progress_ratio'] == 228/254
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f'127.0.0.1:{server.server_address[1]}'
    page = browser.new_page(viewport={'width': width, 'height': 900})
    external, errors = [], []
    page.on('pageerror', lambda e: errors.append(str(e)))
    page.on('request', lambda req: external.append(req.url) if urlsplit(req.url).netloc != origin else None)
    try:
        page.goto(f'http://{origin}/news/', wait_until='domcontentloaded', timeout=15000)
        page.wait_for_function('() => state.latest?.schema_version === 1', timeout=15000)
        page.locator('a[href="#acq-catalog"]').click(timeout=10000)
        page.locator('#source-search').fill('pocket')
        card = required_provider_card(page, 'pocket')
        expect(card).to_have_count(1)
        expect(card.locator('[data-news-verified-references]')).to_have_text('11')
        expect(card.locator('[data-news-unresolved-references]')).to_have_text('15')
        expect(card.locator('[data-news-inactive-references]')).to_have_text('0')
        expect(card.locator('[data-news-resolved-references]')).to_have_text('239 / 254')
        expect(card.locator('[data-news-missing-body-urls]')).to_have_text('26')
        expect(card.locator(BODY_PROGRESS_SELECTOR)).to_have_count(1)
        assert card.locator(BODY_PROGRESS_SELECTOR).evaluate('(node) => node.value') == 228/254
        assert card.locator('[data-news-reference-progress]').evaluate('(node) => node.value') == 239/254
        expect(card).to_contain_text('不新增文章、不複製正文或原頁')
        expect(card).to_contain_text('各自的轉址與抓取時間保持分開')
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        page.locator('#source-sort').select_option('remaining')
        assert card.locator('[data-news-reference-progress]').evaluate('(node) => node.value') == 239/254
        assert not external and not errors
    finally:
        page.close()
        server.shutdown(); server.server_close(); thread.join(timeout=5)

"""Historical queue sorting hints stay separate from publication and body claims."""
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from playwright.sync_api import expect, sync_playwright

from scripts.audit_news_providers import sort_day_summary
from scripts.probe_browser_runtime import browser_profile_launch_options
from scripts.publish_news_sort_day_status import publish
from stockagent.live.news_corpus_quality import QualityEvidenceError, public_sort_day_quality_status
from stockagent.live.news_dashboard import build_news_public_status, news_snapshot_revision
from test_news_dashboard import _save, _seed


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 10, 30, 1, tzinfo=UTC)


def _inventory():
    return {'inventory_scope': 'all_known_source_keys', 'all_known_source_keys_included': True,
            'metadata_sort_day_quality_requested': True, 'metadata_sort_day_quality_contract_version': 1,
            'publisher_requests': 0, 'production_queue_writes': False,
            'observed_at_utc': NOW.isoformat(), 'providers': {
                'cnyes': {'known_urls': 5, 'metadata_sort_days_present': 4, 'metadata_sort_days_invalid': 0,
                          'metadata_sort_days_pre_1900': 0, 'metadata_sort_days_after_snapshot_utc': 0},
                'fed_board': {'known_urls': 4, 'metadata_sort_days_present': 3, 'metadata_sort_days_invalid': 1,
                              'metadata_sort_days_pre_1900': 1, 'metadata_sort_days_after_snapshot_utc': 0},
                'empty': {'known_urls': 0, 'metadata_sort_days_present': 0, 'metadata_sort_days_invalid': 0,
                          'metadata_sort_days_pre_1900': 0, 'metadata_sort_days_after_snapshot_utc': 0}}}


def _audit(root):
    path = root / 'artifacts/data_quality/private/sort.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sort_day_summary(_inventory(), inventory_sha256='a' * 64)))
    return path


def test_projection_is_historical_and_has_no_private_content(tmp_path):
    audit = _audit(tmp_path)
    _save(tmp_path, _seed(tmp_path, now=NOW))
    old_revision = news_snapshot_revision(tmp_path)
    published = publish(tmp_path, audit, now=NOW)
    assert news_snapshot_revision(tmp_path) != old_revision
    assert published == {'state': 'historical_audit', 'observed_at_utc': NOW.isoformat(),
        'source_keys_with_heads': 2, 'known_heads': 9, 'present': 7, 'invalid': 1, 'pre_1900': 1,
        'after_snapshot_utc': 0, 'flagged_sources': [{'id': 'fed_board', 'invalid': 1, 'pre_1900': 1,
                                                       'after_snapshot_utc': 0}],
        'publication_times_verified': False, 'current_queue_claimed': False}
    assert build_news_public_status(tmp_path, now=NOW)['sort_day_quality'] == published
    assert str(tmp_path) not in json.dumps(published) and 'source_inventory_sha256' not in published


@pytest.mark.parametrize('field,value', [
    ('inventory_scope', 'selected_source_keys'), ('all_known_source_keys_included', False),
    ('metadata_sort_day_quality_requested', False), ('metadata_sort_day_quality_contract_version', True),
    ('publisher_requests', False), ('production_queue_writes', True),
])
def test_summary_rejects_non_full_or_invalid_inventory(field, value):
    inventory = _inventory()
    inventory[field] = value
    with pytest.raises(ValueError):
        sort_day_summary(inventory, inventory_sha256='a' * 64)


def test_summary_rejects_invalid_counts_and_inventory_hash():
    inventory = _inventory()
    inventory['providers']['cnyes']['metadata_sort_days_invalid'] = True
    with pytest.raises(ValueError):
        sort_day_summary(inventory, inventory_sha256='a' * 64)
    with pytest.raises(ValueError):
        sort_day_summary(_inventory(), inventory_sha256='bad')


@pytest.mark.parametrize('change', ['duplicate', 'total', 'flagged', 'clock', 'bool_count'])
def test_bad_evidence_is_unknown_and_cannot_be_published(tmp_path, change):
    path = _audit(tmp_path)
    audit = json.loads(path.read_text())
    if change == 'duplicate':
        audit['providers'].append(audit['providers'][0])
        audit['provider_keys_with_heads'] += 1
    elif change == 'total':
        audit['totals']['present'] += 1
    elif change == 'flagged':
        audit['flagged_providers'] = []
    elif change == 'clock':
        audit['observed_at_utc'] = (NOW + timedelta(days=1)).isoformat()
        audit['utc_comparison_day'] = '2026-10-31'
    else:
        audit['providers'][0]['invalid'] = False
    path.write_text(json.dumps(audit))
    with pytest.raises(QualityEvidenceError):
        publish(tmp_path, path, now=NOW)
    assert public_sort_day_quality_status(tmp_path, now=NOW) == {'state': 'unavailable'}


def test_changed_or_missing_evidence_is_not_a_clean_audit(tmp_path):
    path = _audit(tmp_path)
    expected = publish(tmp_path, path, now=NOW)
    original = path.read_bytes()
    revision = news_snapshot_revision(tmp_path)
    path.write_bytes(original + b' ')
    assert news_snapshot_revision(tmp_path) != revision
    assert public_sort_day_quality_status(tmp_path, now=NOW) == {'state': 'unavailable'}
    path.write_bytes(original)
    assert public_sort_day_quality_status(tmp_path, now=NOW) == expected
    path.unlink()
    assert public_sort_day_quality_status(tmp_path, now=NOW) == {'state': 'unavailable'}


def test_older_audit_cannot_replace_newer_published_reference(tmp_path):
    new_path = _audit(tmp_path)
    new = publish(tmp_path, new_path, now=NOW)
    older = json.loads(new_path.read_text())
    older['observed_at_utc'] = (NOW - timedelta(hours=1)).isoformat()
    older['utc_comparison_day'] = '2026-10-30' if NOW.hour else '2026-10-29'
    old_path = new_path.with_name('older.json')
    old_path.write_text(json.dumps(older))
    with pytest.raises(QualityEvidenceError, match='newer_sort_audit_already_published'):
        publish(tmp_path, old_path, now=NOW)
    assert public_sort_day_quality_status(tmp_path, now=NOW) == new


@pytest.mark.parametrize('width,height', [(390, 844), (1440, 900)])
def test_historical_audit_is_readable_and_withdrawn_on_fetch_failure(tmp_path, width, height):
    now = datetime.now(UTC)
    audit = _inventory()
    audit['observed_at_utc'] = now.isoformat()
    path = tmp_path / 'artifacts/data_quality/private/sort.json'
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(sort_day_summary(audit, inventory_sha256='a' * 64)))
    publish(tmp_path, path, now=now)
    _save(tmp_path, _seed(tmp_path, now=now))
    payload = [build_news_public_status(tmp_path, now=now)]
    services = ROOT / 'services'
    with sync_playwright() as runtime:
        browser = runtime.chromium.launch(**browser_profile_launch_options('cpu-2d'), timeout=10000)
        try:
            page = browser.new_page(viewport={'width': width, 'height': height})
            errors, external = [], []
            page.on('pageerror', lambda error: errors.append(str(error)))
            def route(request):
                parts = urlsplit(request.request.url)
                if parts.netloc != 'news-preview.test':
                    external.append(parts.netloc)
                    request.abort()
                elif parts.path == '/news/api/status':
                    request.fulfill(status=503, body='unavailable') if payload[0] is None else request.fulfill(
                        content_type='application/json', body=json.dumps(payload[0]))
                else:
                    mounts = {'news': 'news_dashboard', 'shioaji': 'shioaji_api_dashboard',
                              'finlab': 'finlab_dashboard', 'finmind': 'finmind_dashboard'}
                    prefix, _, suffix = parts.path.removeprefix('/').partition('/')
                    relative = (mounts[prefix] + '/' + (suffix or 'index.html') if prefix in mounts
                                else 'public_dashboards/' + parts.path.removeprefix('/'))
                    target = (services / relative).resolve()
                    if not target.is_relative_to(services) or not target.is_file():
                        request.fulfill(status=404, body='')
                    else:
                        mime = {'.js': 'text/javascript', '.css': 'text/css', '.html': 'text/html',
                                '.svg': 'image/svg+xml'}.get(target.suffix, 'application/octet-stream')
                        request.fulfill(content_type=mime, body=target.read_bytes())
            page.route('**/*', route)
            page.goto('http://news-preview.test/news/', wait_until='networkidle', timeout=15000)
            expect(page.locator('#sort-day-summary')).to_contain_text('2 個來源鍵')
            page.locator('#sort-day-quality > summary').click()
            expect(page.locator('#sort-day-detail')).to_contain_text('排程提示及待核對標記')
            expect(page.locator('#sort-day-detail')).to_contain_text('不是來源發布時間')
            expect(page.locator('#sort-day-sources')).to_contain_text('fed_board')
            assert page.evaluate('document.documentElement.scrollWidth') <= width
            payload[0] = None
            page.evaluate('refresh()')
            expect(page.locator('#sort-day-summary')).to_contain_text('未知')
            expect(page.locator('#sort-day-sources')).to_be_hidden()
            assert not errors and not external
        finally:
            browser.close()

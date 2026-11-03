"""Pinned quality evidence reaches the public page without exposing records."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
import socket
import sqlite3
from urllib.parse import urlsplit

import pytest
from playwright.sync_api import expect, sync_playwright

from scripts.probe_browser_runtime import browser_profile_launch_options
from scripts.publish_news_corpus_quality_status import publish
from stockagent.live.news_corpus_quality import (
    HEADLINE_REFERENCE, REFERENCE, QualityEvidenceError, public_corpus_quality_statuses,
    public_headline_quality_status, public_quality_status,
)
from stockagent.live.news_dashboard import build_news_public_status, news_snapshot_revision
from test_news_dashboard import _seed, _save


NOW = datetime(2026, 10, 30, 4, tzinfo=UTC)
ROOT = Path(__file__).resolve().parents[1]


def _bytes(value):
    return (json.dumps(value, sort_keys=True) + '\n').encode()


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _proofs(root, *, when=NOW, empty=False):
    folder = root / 'artifacts/data_quality/private'
    folder.mkdir(parents=True, exist_ok=True)
    manifest = root / 'data_news/manifest.json'
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest_raw = b'{"generation":"versions/example"}\n'
    manifest.write_bytes(manifest_raw)
    matches = [] if empty else [
        {'article_id': 'a' * 64, 'provider': 'cmnews', 'origin': 'private_staged',
         'corpus_part': 'PRIVATE_PART', 'corpus_part_sha256': 'b' * 64},
        {'article_id': 'c' * 64, 'provider': 'moneyweekly', 'origin': 'owner_admitted',
         'corpus_part': 'PRIVATE_PART', 'corpus_part_sha256': 'b' * 64},
    ]
    audit = {'contract': 'news_corpus_reviewed_body_positive_audit_v1',
             'observed_at_utc': (when - timedelta(seconds=1)).isoformat(),
             'corpus_generation': 'versions/example', 'corpus_manifest_sha256': _sha(manifest_raw),
             'source_snapshot': [[1, 2, 3, 4, 5], None, None],
             'confirmed_exact_versions': len(matches), 'targets': max(1, len(matches)),
             'unconfirmed_targets': 1 if empty else 0, 'matches': matches,
             'confirmed_by_origin': {} if empty else {'owner_admitted': 1, 'private_staged': 1},
             'confirmed_by_provider': {} if empty else {'cmnews': 1, 'moneyweekly': 1},
             'positive_part_checksums_verified': 0 if empty else 1,
             'negative_matches_prove_absence': False, 'all_corpus_parts_sha_verified': False,
             'publisher_requests': 0, 'production_queue_writes': 0, 'production_corpus_writes': 0,
             'PRIVATE_SECRET': 'PRIVATE_BODY_OR_URL'}
    audit_path = folder / 'audit.json'
    audit_path.write_bytes(_bytes(audit))
    readback = {'contract': 'news_corpus_reviewed_versions_independent_readback_v1',
                'observed_at_utc': when.isoformat(), 'audit_sha256': _sha(audit_path.read_bytes()),
                'corpus_generation': 'versions/example', 'corpus_manifest_sha256': _sha(manifest_raw),
                'source_snapshot': audit['source_snapshot'],
                'confirmed_exact_fulltext_versions': len(matches), 'positive_corpus_parts': 0 if empty else 1,
                'by_provider_and_review_origin': [] if empty else [
                    {'provider': 'cmnews', 'origin': 'private_staged', 'count': 1},
                    {'provider': 'moneyweekly', 'origin': 'owner_admitted', 'count': 1}],
                'publisher_requests': 0, 'production_queue_writes': 0, 'production_corpus_writes': 0}
    readback_path = folder / 'readback.json'
    readback_path.write_bytes(_bytes(readback))
    return manifest, audit_path, readback_path


def _headline_proofs(root, *, when=NOW, empty=False):
    folder = root / 'artifacts/data_quality/private'
    folder.mkdir(parents=True, exist_ok=True)
    manifest_raw = (root / 'data_news/manifest.json').read_bytes()
    urls = [] if empty else ['https://cmnews.com.tw/tag/private-one', 'http://cmnews.com.tw/tag/private-two']
    count = len(urls)
    identity = [1, 2, len(manifest_raw), 3, 4]
    audit = {'contract': 'cmnews_tag_corpus_projection_v1',
             'observed_at_utc': (when - timedelta(seconds=1)).isoformat(),
             'manifest_sha256': _sha(manifest_raw), 'manifest_identity': identity,
             'generation': 'versions/example', 'part_count': 3,
             'counts': {'headline_rows_examined': 6, 'fulltext_rows_pruned': 7,
                        'fulltext_rows_examined': 0},
             'matches_by_kind': {'cmnews_tag_literal': count} if count else {},
             'matches': [{'kind': 'cmnews_tag_literal', 'url': url} for url in urls],
             'source_identity_or_page_content_verified': False, 'body_columns_read': False,
             'part_sha256_reverified': False, 'part_stat_identity_not_a_filesystem_snapshot': True,
             'publisher_requests': 0, 'production_queue_writes': 0, 'corpus_writes': 0}
    audit_path = folder / 'headline-audit.json'
    audit_path.write_bytes(_bytes(audit))
    readback = {'contract': 'cmnews_tag_corpus_source_readback_v1',
                'observed_at_utc': when.isoformat(), 'input_report_sha256': _sha(audit_path.read_bytes()),
                'corpus_manifest_sha256': _sha(manifest_raw), 'direct_corpus_url_count': count,
                'source_head_facts': {key: count for key in (
                    'found_in_current_source', 'current_provider:cmnews',
                    'current_state:unsupported_article_path', 'current_body_pointer_absent',
                    'current_title_present', 'current_title_contains_collection_phrase')},
                'source_head_state_is_current_snapshot_not_historical_corpus_state': True,
                'source_page_identity_or_content_verified': False,
                'snapshot': {'manifest': identity}, 'snapshot_checks_are_not_global_filesystem_snapshot': True,
                'publisher_requests': 0, 'production_queue_writes': 0, 'corpus_writes': 0}
    readback_path = folder / 'headline-readback.json'
    readback_path.write_bytes(_bytes(readback))
    return audit_path, readback_path


def test_headline_and_body_projection_are_independent_and_share_one_manifest_read(tmp_path, monkeypatch):
    _, audit, readback = _proofs(tmp_path)
    body = publish(tmp_path, audit, readback, now=NOW)
    headline_audit, headline_readback = _headline_proofs(tmp_path, when=NOW - timedelta(hours=1))
    headline = publish(tmp_path, headline_audit, headline_readback, now=NOW, kind='cmnews-tag-headlines')
    assert headline == {'state': 'tag_headlines_detected',
                        'observed_at_utc': (NOW - timedelta(hours=1)).isoformat(),
                        'tag_headline_rows': 2, 'headline_rows_examined': 6,
                        'current_source_heads_confirmed': 2, 'source_page_content_verified': False,
                        'source_heads_are_current_snapshot': True, 'corpus_parts_sha_reverified': False,
                        'all_other_headlines_cleared': False}
    from stockagent.live import news_corpus_quality as module
    real_read, manifest_reads = module._read, []
    def counted(path, limit):
        if path == tmp_path / 'data_news/manifest.json':
            manifest_reads.append(path)
        return real_read(path, limit)
    monkeypatch.setattr(module, '_read', counted)
    def forbidden(*args, **kwargs):
        raise AssertionError('dashboard must not open the source queue or make publisher requests')
    monkeypatch.setattr(socket, 'socket', forbidden)
    monkeypatch.setattr(sqlite3, 'connect', forbidden)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['corpus_quality'] == body and result['corpus_headline_quality'] == headline
    assert len(manifest_reads) == 1
    assert 'private-one' not in json.dumps(result) and 'private-two' not in json.dumps(result)
    revision = news_snapshot_revision(tmp_path)
    headline_audit.write_bytes(headline_audit.read_bytes() + b' ')
    assert news_snapshot_revision(tmp_path) != revision
    body_after, headline_after = public_corpus_quality_statuses(tmp_path, now=NOW)
    assert body_after == body and headline_after == {'state': 'unavailable'}
    (tmp_path / HEADLINE_REFERENCE).unlink()
    assert public_corpus_quality_statuses(tmp_path, now=NOW) == (body, {'state': 'unavailable'})


def test_restoring_a_missing_headline_proof_invalidates_the_dashboard_cache(tmp_path):
    _proofs(tmp_path)
    audit, readback = _headline_proofs(tmp_path)
    publish(tmp_path, audit, readback, now=NOW, kind='cmnews-tag-headlines')
    saved = audit.read_bytes()
    audit.unlink()
    revision = news_snapshot_revision(tmp_path)
    assert public_headline_quality_status(tmp_path, now=NOW) == {'state': 'unavailable'}
    audit.write_bytes(saved)
    assert news_snapshot_revision(tmp_path) != revision
    assert public_headline_quality_status(tmp_path, now=NOW)['state'] == 'tag_headlines_detected'


@pytest.mark.parametrize('which,change', [
    ('audit', lambda d: d['matches'][1].update(url=d['matches'][0]['url'])),
    ('audit', lambda d: d['matches'][0].update(url='https://elsewhere.test/tag/a')),
    ('audit', lambda d: d['matches'][0].update(kind='other')),
    ('audit', lambda d: d.update(matches_by_kind={'cmnews_tag_literal': 3})),
    ('audit', lambda d: d['counts'].update(headline_rows_examined=1)),
    ('audit', lambda d: d['counts'].update(fulltext_rows_examined=1)),
    ('audit', lambda d: d.update(part_count=0)),
    ('audit', lambda d: d.update(publisher_requests=True)),
    ('audit', lambda d: d.update(source_identity_or_page_content_verified=True)),
    ('readback', lambda d: d.update(direct_corpus_url_count=3)),
    ('readback', lambda d: d['source_head_facts'].update(current_title_contains_collection_phrase=1)),
    ('readback', lambda d: d['snapshot'].update(manifest=[3, 2, 1, 0, 0])),
    ('readback', lambda d: d.update(source_page_identity_or_content_verified=True)),
    ('readback', lambda d: d.update(observed_at_utc=(NOW + timedelta(days=1)).isoformat())),
])
def test_headline_conflicting_evidence_never_publishes(tmp_path, which, change):
    _proofs(tmp_path)
    audit, readback = _headline_proofs(tmp_path)
    target = audit if which == 'audit' else readback
    value = json.loads(target.read_bytes())
    change(value)
    target.write_bytes(_bytes(value))
    if which == 'audit':
        independent = json.loads(readback.read_bytes())
        independent['input_report_sha256'] = _sha(audit.read_bytes())
        readback.write_bytes(_bytes(independent))
    with pytest.raises(QualityEvidenceError):
        publish(tmp_path, audit, readback, now=NOW, kind='cmnews-tag-headlines')
    assert public_headline_quality_status(tmp_path, now=NOW) == {'state': 'unavailable'}


def test_headline_zero_does_not_clear_other_headlines(tmp_path):
    _proofs(tmp_path)
    audit, readback = _headline_proofs(tmp_path, empty=True)
    result = publish(tmp_path, audit, readback, now=NOW, kind='cmnews-tag-headlines')
    assert result['state'] == 'no_tag_headlines_in_scope'
    assert result['tag_headline_rows'] == 0 and result['all_other_headlines_cleared'] is False


def test_manifest_switch_between_projections_invalidates_both(tmp_path, monkeypatch):
    manifest, audit, readback = _proofs(tmp_path)
    publish(tmp_path, audit, readback, now=NOW)
    headline_audit, headline_readback = _headline_proofs(tmp_path)
    publish(tmp_path, headline_audit, headline_readback, now=NOW, kind='cmnews-tag-headlines')
    from stockagent.live import news_corpus_quality as module
    real = module.public_headline_quality_status
    def switch(*args, **kwargs):
        manifest.write_bytes(b'{"generation":"versions/other"}\n')
        return real(*args, **kwargs)
    monkeypatch.setattr(module, 'public_headline_quality_status', switch)
    assert public_corpus_quality_statuses(tmp_path, now=NOW) == (
        {'state': 'unavailable'}, {'state': 'unavailable'})


def test_current_generation_projects_only_independently_reconciled_counts(tmp_path, monkeypatch):
    _, audit, readback = _proofs(tmp_path)
    published = publish(tmp_path, audit, readback, now=NOW)
    assert published == {
        'state': 'known_issues', 'observed_at_utc': NOW.isoformat(), 'known_affected_versions': 2,
        'owner_admitted_versions': 1, 'private_staged_versions': 1,
        'providers': [{'id': 'cmnews', 'versions': 1}, {'id': 'moneyweekly', 'versions': 1}],
        'unconfirmed_targets': 0, 'all_corpus_parts_verified': False, 'all_other_versions_cleared': False}
    def forbidden(*args, **kwargs):
        raise AssertionError('dashboard must not open a queue or make publisher requests')
    monkeypatch.setattr(socket, 'socket', forbidden)
    monkeypatch.setattr(sqlite3, 'connect', forbidden)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['corpus_quality'] == published
    assert 'PRIVATE_' not in json.dumps(result)
    assert not result['scope']['publisher_calls_on_page_view']


def test_pointer_manifest_and_proof_changes_invalidate_without_claiming_clean(tmp_path):
    manifest, audit, readback = _proofs(tmp_path)
    publish(tmp_path, audit, readback, now=NOW)
    original_manifest = manifest.read_bytes()
    revision = news_snapshot_revision(tmp_path)
    manifest.write_bytes(b'{"generation":"versions/new"}\n')
    assert news_snapshot_revision(tmp_path) != revision
    assert public_quality_status(tmp_path, now=NOW) == {'state': 'superseded'}
    manifest.write_bytes(original_manifest)
    assert public_quality_status(tmp_path, now=NOW)['state'] == 'known_issues'
    revision = news_snapshot_revision(tmp_path)
    audit.write_bytes(audit.read_bytes().replace(b'PRIVATE_PART', b'ALTERED_PART'))
    assert news_snapshot_revision(tmp_path) != revision
    assert public_quality_status(tmp_path, now=NOW) == {'state': 'unavailable'}
    (tmp_path / REFERENCE).unlink()
    assert public_quality_status(tmp_path, now=NOW) == {'state': 'unavailable'}


@pytest.mark.parametrize('which,change', [
    ('audit', lambda d: d.update(confirmed_exact_versions=3)),
    ('audit', lambda d: d.update(confirmed_by_origin={'owner_admitted': 2})),
    ('audit', lambda d: d.update(negative_matches_prove_absence=True)),
    ('audit', lambda d: d.update(publisher_requests=True)),
    ('audit', lambda d: d['matches'][1].update(article_id=d['matches'][0]['article_id'])),
    ('audit', lambda d: d['matches'][1].update(corpus_part_sha256='e' * 64)),
    ('readback', lambda d: d.update(confirmed_exact_fulltext_versions=3)),
    ('readback', lambda d: d['by_provider_and_review_origin'][0].update(count=2)),
    ('readback', lambda d: d.update(source_snapshot=['different'])),
    ('readback', lambda d: d.update(observed_at_utc=(NOW + timedelta(days=1)).isoformat())),
])
def test_conflicting_evidence_is_not_published(tmp_path, which, change):
    _, audit, readback = _proofs(tmp_path)
    target = audit if which == 'audit' else readback
    altered = json.loads(target.read_bytes())
    change(altered)
    target.write_bytes(_bytes(altered))
    if which == 'audit':
        independent = json.loads(readback.read_bytes())
        independent['audit_sha256'] = _sha(audit.read_bytes())
        readback.write_bytes(_bytes(independent))
    with pytest.raises(QualityEvidenceError):
        publish(tmp_path, audit, readback, now=NOW)
    assert public_quality_status(tmp_path, now=NOW) == {'state': 'unavailable'}


def test_zero_positive_is_not_corpus_clear(tmp_path):
    _, audit, readback = _proofs(tmp_path, empty=True)
    result = publish(tmp_path, audit, readback, now=NOW)
    assert result['state'] == 'no_positive_in_scope'
    assert result['known_affected_versions'] == 0 and result['unconfirmed_targets'] == 1
    assert not result['all_other_versions_cleared'] and not result['all_corpus_parts_verified']


def test_pointer_cannot_escape_artifacts_or_restore_older_observation(tmp_path):
    _, audit, readback = _proofs(tmp_path)
    publish(tmp_path, audit, readback, now=NOW)
    pointer = tmp_path / REFERENCE
    original = pointer.read_bytes()
    old_audit = json.loads(audit.read_bytes())
    old_audit['observed_at_utc'] = (NOW - timedelta(hours=2)).isoformat()
    older_audit = audit.with_name('older.json')
    older_audit.write_bytes(_bytes(old_audit))
    old_readback = json.loads(readback.read_bytes())
    old_readback.update(observed_at_utc=(NOW - timedelta(hours=1)).isoformat(),
                        audit_sha256=_sha(older_audit.read_bytes()))
    older_readback = readback.with_name('older-readback.json')
    older_readback.write_bytes(_bytes(old_readback))
    with pytest.raises(QualityEvidenceError, match='newer_current_audit'):
        publish(tmp_path, older_audit, older_readback, now=NOW)
    assert pointer.read_bytes() == original
    pointer.unlink()
    outside = tmp_path / 'outside.json'
    outside.write_bytes(original)
    pointer.symlink_to(outside)
    assert public_quality_status(tmp_path, now=NOW) == {'state': 'unavailable'}


def test_pointer_change_during_evidence_read_is_unknown(tmp_path, monkeypatch):
    _, audit, readback = _proofs(tmp_path)
    publish(tmp_path, audit, readback, now=NOW)
    from stockagent.live import news_corpus_quality as module
    real = module.inspect_reference
    def changed(*args, **kwargs):
        result = real(*args, **kwargs)
        (tmp_path / REFERENCE).write_bytes(b'{}')
        return result
    monkeypatch.setattr(module, 'inspect_reference', changed)
    assert public_quality_status(tmp_path, now=NOW) == {'state': 'unavailable'}


def test_manifest_change_during_evidence_read_is_unknown(tmp_path, monkeypatch):
    manifest, audit, readback = _proofs(tmp_path)
    publish(tmp_path, audit, readback, now=NOW)
    from stockagent.live import news_corpus_quality as module
    real = module._read
    def changed(path, limit):
        result = real(path, limit)
        if path == manifest:
            manifest.write_bytes(b'{"generation":"versions/new"}\n')
        return result
    monkeypatch.setattr(module, '_read', changed)
    assert public_quality_status(tmp_path, now=NOW) == {'state': 'unavailable'}


def test_referenced_proof_cannot_escape_quality_directory(tmp_path):
    _, audit, readback = _proofs(tmp_path)
    publish(tmp_path, audit, readback, now=NOW)
    pointer = tmp_path / REFERENCE
    reference = json.loads(pointer.read_bytes())
    outside = tmp_path / 'outside-audit.json'
    outside.write_bytes(audit.read_bytes())
    escaped = audit.with_name('escape.json')
    escaped.symlink_to(outside)
    reference['audit']['path'] = str(escaped.relative_to(tmp_path))
    pointer.write_bytes(_bytes(reference))
    assert public_quality_status(tmp_path, now=NOW) == {'state': 'unavailable'}
    news_snapshot_revision(tmp_path)
    reference['audit']['path'] = 'artifacts/data_quality/\u0000bad'
    pointer.write_bytes(_bytes(reference))
    assert public_quality_status(tmp_path, now=NOW) == {'state': 'unavailable'}
    news_snapshot_revision(tmp_path)


@pytest.mark.parametrize('width,height', [(1440, 1000), (390, 844), (320, 768)])
def test_browser_shows_version_scope_and_invalidates_on_manifest_change(tmp_path, width, height):
    now = datetime.now(UTC)
    manifest, audit, readback = _proofs(tmp_path, when=now)
    publish(tmp_path, audit, readback, now=now)
    headline_audit, headline_readback = _headline_proofs(tmp_path, when=now)
    publish(tmp_path, headline_audit, headline_readback, now=now, kind='cmnews-tag-headlines')
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
                    if payload[0] is None:
                        request.fulfill(status=503, body='unavailable')
                    else:
                        request.fulfill(content_type='application/json', body=json.dumps(payload[0]))
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
            expect(page.locator('#corpus-quality')).to_be_visible()
            expect(page.locator('#corpus-quality-title')).to_contain_text('2 個有品質問題的全文版本')
            expect(page.locator('#corpus-quality-detail')).to_contain_text('正式受理 1、待正式受理 1')
            expect(page.locator('#corpus-quality-detail')).to_contain_text('其他全文品質仍未知')
            expect(page.locator('#corpus-quality-providers')).to_contain_text('cmnews 1')
            expect(page.locator('#corpus-headline-quality')).to_contain_text('2 筆 CMNews 標籤網址列為標題')
            expect(page.locator('#corpus-headline-quality')).to_contain_text('來源頁內容未核驗')
            assert page.evaluate('document.documentElement.scrollWidth') <= width
            current = payload[0]
            original_body_quality = current['corpus_quality']
            current['corpus_quality'] = {'state': 'unavailable'}
            page.evaluate('refresh()')
            expect(page.locator('#corpus-quality-title')).to_contain_text('全文品質：目前版本尚無可用稽核')
            expect(page.locator('#corpus-headline-quality')).to_contain_text('2 筆 CMNews 標籤網址列為標題')
            current['corpus_quality'] = original_body_quality
            page.evaluate('refresh()')
            expect(page.locator('#corpus-quality-title')).to_contain_text('2 個有品質問題的全文版本')
            original_provider = current['corpus_quality']['providers'][0]['id']
            long_provider = 'unregistered:' + 'a' * 253
            current['corpus_quality']['providers'][0]['id'] = long_provider
            page.evaluate('refresh()')
            expect(page.locator('#corpus-quality-providers')).to_contain_text(long_provider)
            assert page.evaluate('document.documentElement.scrollWidth') <= width
            current['corpus_quality']['providers'][0]['id'] = original_provider
            original_headline_audit = headline_audit.read_bytes()
            headline_audit.write_bytes(original_headline_audit + b' ')
            payload[0] = build_news_public_status(tmp_path, now=now)
            page.evaluate('refresh()')
            expect(page.locator('#corpus-quality-title')).to_contain_text('2 個有品質問題的全文版本')
            expect(page.locator('#corpus-headline-quality')).to_contain_text('目前版本尚無可用稽核')
            headline_audit.write_bytes(original_headline_audit)
            current = build_news_public_status(tmp_path, now=now)
            payload[0] = None
            page.evaluate('refresh()')
            expect(page.locator('#corpus-quality-title')).to_contain_text('目前版本尚無可用稽核')
            expect(page.locator('#corpus-quality-providers')).to_be_hidden()
            expect(page.locator('#corpus-headline-quality')).to_contain_text('目前版本尚無可用稽核')
            payload[0] = current
            page.evaluate('refresh()')
            expect(page.locator('#corpus-quality-title')).to_contain_text('2 個有品質問題的全文版本')
            expect(page.locator('#corpus-headline-quality')).to_contain_text('2 筆 CMNews 標籤網址列為標題')
            manifest.write_bytes(b'{"generation":"versions/new"}\n')
            payload[0] = build_news_public_status(tmp_path, now=now)
            page.reload(wait_until='networkidle', timeout=15000)
            expect(page.locator('#corpus-quality-title')).to_contain_text('目前版本尚無可用稽核')
            expect(page.locator('#corpus-quality-detail')).to_contain_text('資料集版本已變更')
            expect(page.locator('#corpus-quality-providers')).to_be_hidden()
            expect(page.locator('#corpus-headline-quality')).to_contain_text('資料集版本已變更')
            assert not errors and not external
            page.close()
        finally:
            browser.close()

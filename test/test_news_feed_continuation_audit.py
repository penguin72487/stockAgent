"""Native RSS boundary acceptance is bounded, read-only and fail closed."""
import hashlib
from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import accept_discovery
from downloader.news_fulltext_feed import feed_records, feed_url
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_queue import utc_now
from scripts.audit_news_feed_continuation import audit, validate_scope
from test_news_fulltext import config
from test_news_provider_efficiency import feed_fixture, feed_item, rss


@pytest.mark.parametrize('mode,accepted', [('coherent', True), ('proof_head_changed', False),
    ('protected_quarantine', False), ('bad_checkpoint', False)])
def test_feed_boundary_proof_preserves_native_states_and_receipts(config, tmp_path, monkeypatch, mode, accepted):
    http, queue = feed_fixture(config, tmp_path)
    source = http.registry.sources['publisher']
    diagnostic, capture = {'probes': []}, {'observed_at_utc': utc_now(), 'jobs': [], 'native_capture_completed': True}
    try:
        for page, items in ((46, [feed_item(1, rights='All rights reserved'), feed_item(2)]),
                            (47, [feed_item(3), feed_item(4)])):
            url = feed_url(source.fulltext_feeds[0], page)
            with queue.db:
                queue.db.execute("INSERT OR IGNORE INTO discovery(url,kind,provider) VALUES (?,'rss_fulltext','publisher')", (url,))
            task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
            observed = utc_now()
            response = Response(url, 200, {}, rss(items))
            records, errors = feed_records(response, http, source)
            digest = hashlib.sha256(response.body).hexdigest()
            diagnostic['probes'].append({'url': url, 'status': 200, 'permitted_body_candidates': [
                {k: r[k] for k in ('requested_url', 'body_sha256', 'published_at_raw', 'authors_json')} for r in records]})
            count = accept_discovery(queue, task, response,
                [(r['requested_url'], r['title'], r['metadata_date_raw']) for r in records], records, errors)
            capture['jobs'].append({'url': url, 'source_status': 200, 'provider': 'publisher',
                'observed_at_utc': observed, 'response_sha256': digest, 'item_errors': errors, 'accepted_bodies': count})
        capture['finished_at_utc'] = utc_now()
        if mode == 'bad_checkpoint':
            with queue.db:
                queue.db.execute('UPDATE discovery SET response_sha256=? WHERE url=?',
                                 ('wrong', capture['jobs'][0]['url']))
    finally:
        queue.close()
        http.close()
    diagnostic_path, capture_path = tmp_path / 'probe.json', tmp_path / 'capture.json'
    atomic_write_json(diagnostic_path, diagnostic)
    capture['diagnostic_sha256'] = sha256_file(diagnostic_path)
    atomic_write_json(capture_path, capture)
    if mode == 'protected_quarantine':
        target = tmp_path / 'quarantine' / (capture['jobs'][0]['response_sha256'] + '.parquet')
        target.parent.mkdir(exist_ok=True)
        target.touch()
    if mode == 'proof_head_changed':
        from scripts.audit_news_feed_continuation import audit_bodies

        def different_head(*args, **kwargs):
            proof = audit_bodies(*args, **kwargs)
            proof['articles'][0]['body_sha256'] = 'a newer head outside the first pinned snapshot'
            return proof

        monkeypatch.setattr('scripts.audit_news_feed_continuation.audit_bodies', different_head)
    before = sha256_file(tmp_path / 'queue.sqlite3')
    result = audit(tmp_path, http.registry, diagnostic_path, capture_path)
    assert result['accepted'] is accepted
    assert result['publisher_requests'] == 0 and not result['source_states_mutated']
    assert not result['historical_completeness_verified'] and not result['publish']
    assert not result['training_injection_enabled']
    assert sha256_file(tmp_path / 'queue.sqlite3') == before
    if accepted:
        assert result['newly_committed_body_heads'] == result['selected_durable_body_heads'] == 2
        assert result['preexisting_body_heads_preserved'] == 0
    else:
        assert result['errors']


@pytest.mark.parametrize('mode', ['duplicate', 'incomplete', 'out_of_scope', 'protected_body_accepted', 'oversized'])
def test_feed_native_audit_rejects_invalid_or_unbounded_scope(mode):
    url = 'https://news.example.com/feed/?paged=46'
    registry = SimpleNamespace(require=lambda _: SimpleNamespace(provider='publisher', fulltext_feeds=('https://news.example.com/feed/',)))
    diagnostic = {'probes': [{'url': url, 'status': 200, 'permitted_body_candidates': []}]}
    capture = {'native_capture_completed': True, 'observed_at_utc': utc_now(), 'finished_at_utc': utc_now(),
        'jobs': [{'url': url, 'source_status': 200, 'provider': 'publisher', 'item_errors': [], 'accepted_bodies': 0}]}
    if mode == 'duplicate':
        diagnostic['probes'] *= 2
    elif mode == 'incomplete':
        capture['native_capture_completed'] = False
    elif mode == 'out_of_scope':
        diagnostic['probes'][0]['url'] = capture['jobs'][0]['url'] = 'https://news.example.com/undeclared/'
    elif mode == 'protected_body_accepted':
        capture['jobs'][0].update(item_errors=[{'error': 'paywall_or_login'}], accepted_bodies=1)
    elif mode == 'oversized':
        diagnostic['probes'][0]['permitted_body_candidates'] = [{}] * 101
    with pytest.raises(ValueError):
        validate_scope(diagnostic, capture, registry)

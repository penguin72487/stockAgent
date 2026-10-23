"""Replay original RSS with its own parser, never as invented HTML or a new request."""
import hashlib
import json

import pytest

from downloader.news_fulltext import accept_discovery, seed_discovery
from downloader.news_fulltext_feed import feed_records
from downloader.news_fulltext_http import Response
from scripts.audit_news_selected_articles import audit
from test_news_fulltext import config
from test_news_provider_efficiency import feed_fixture, feed_item, rss


def _capture(http, queue, payload, *, change=None, forged_mixed=False):
    source = http.registry.sources['publisher']
    seed_discovery(queue)
    task = dict(queue.db.execute('SELECT * FROM discovery').fetchone())
    response = Response(task['url'], 200, {'content-type': 'application/rss+xml'}, payload)
    records, errors = feed_records(response, http, source)
    if change is not None:
        change(records)
    if forged_mixed:
        # A self-consistent historical file/hash is still insufficient if
        # its source envelope contains an explicitly restricted neighbour.
        records[0]['raw_response_body'] = payload
        for record in records:
            record['source_response_article_id'] = records[0]['article_id']
        queue.add_articles([{'url': r['requested_url']} for r in records])
        queue.accept(records)
    else:
        assert accept_discovery(queue, task, response,
            [(r['requested_url'], r['title'], r['metadata_date_raw']) for r in records], records, errors) == len(records)
    return [r['requested_url'] for r in records]


def test_selected_original_rss_shared_envelope_is_replayed_once_without_http_or_writer(config, tmp_path, monkeypatch):
    http, queue = feed_fixture(config, tmp_path)
    try:
        urls = _capture(http, queue, rss([feed_item(1), feed_item(2)]))
        changes = queue.db.total_changes
        settings = tuple(queue.db.execute('SELECT key,value FROM queue_settings ORDER BY key'))
        calls = []
        original = feed_records
        monkeypatch.setattr('downloader.news_fulltext_feed.feed_records',
            lambda *args, **kwargs: (calls.append(args[0].url), original(*args, **kwargs))[1])
        monkeypatch.setattr(http, 'get', lambda *args, **kwargs: pytest.fail('audit may not fetch a publisher'))
        proof = audit(tmp_path, urls, registry=http.registry)
        assert proof['accepted']
        assert proof['contract_version'] == 11
        assert len(calls) == 1
        assert len(proof['articles']) == proof['batch_proof']['body_versions'] == 2
        assert all(r['current_source_replay_verified'] and r['capture_contract_matches_current_source']
                   and r['extraction_method'] == 'rss_content_encoded_v1' for r in proof['articles'])
        assert proof['articles'][0]['published_at_raw'] == 'Thu, 08 Oct 2026 13:20:00 -0400'
        assert json.loads(proof['articles'][0]['authors_json']) == ['Real Byline']
        assert all(r['license_verification_basis'] == 'publisher_default_license_feed; explicit_overrides_refused'
                   and not r['per_article_license_verified'] for r in proof['articles'])
        assert queue.db.total_changes == changes
        assert tuple(queue.db.execute('SELECT key,value FROM queue_settings ORDER BY key')) == settings
        assert 'Publisher original text' not in str(proof)
        assert not proof['all_history_complete'] and not proof['publish'] and not proof['remote_training_export']
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('field,value', [
    ('authors_json', '["Invented author"]'),
    ('published_at_raw', 'Thu, 08 Oct 2026 14:20:00 -0400'),
    ('modified_at_raw', '2099-01-01T00:00:00+00:00'),
    ('title', 'Adjacent story title'),
    ('declared_canonical_url', 'https://publisher.test/story/2'),
    ('license_verification_basis', 'unverified licence'),
    ('per_article_license_verified', True),
    ('declared_license_urls_json', '["https://creativecommons.org/licenses/by/4.0/"]'),
    ('published_time_basis', 'inferred from crawl'),
    ('body_completeness', 'summary_only'),
])
def test_selected_rss_replay_rejects_self_consistent_file_with_changed_source_fields(config, tmp_path, field, value):
    http, queue = feed_fixture(config, tmp_path)
    try:
        urls = _capture(http, queue, rss([feed_item(1)]), change=lambda records: records[0].update({field: value}))
        before = queue.db.total_changes
        proof = audit(tmp_path, urls, registry=http.registry)
        assert not proof['accepted'] and proof['errors']
        assert not proof['articles'][0]['current_source_replay_verified']
        assert queue.db.total_changes == before
        assert 'Publisher original text' not in str(proof)
    finally:
        queue.close()
        http.close()


def test_selected_rss_replay_rejects_changed_prose_even_with_new_coherent_sha(config, tmp_path):
    http, queue = feed_fixture(config, tmp_path)
    try:
        def change(records):
            records[0]['body'] += ' Invented prose not in the original feed.'
            records[0]['body_sha256'] = hashlib.sha256(records[0]['body'].encode()).hexdigest()
        urls = _capture(http, queue, rss([feed_item(1)]), change=change)
        proof = audit(tmp_path, urls, registry=http.registry)
        assert not proof['accepted']
        assert proof['errors'][0]['error'] == 'current_source_replay_changed_body_title_author_or_clock'
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('foreign', [False, True])
def test_selected_rss_replay_rejects_entire_restricted_or_foreign_envelope(config, tmp_path, foreign):
    http, queue = feed_fixture(config, tmp_path)
    try:
        other = (feed_item(2).replace('https://publisher.test/story/2', 'https://foreign.test/story/2')
                 if foreign else feed_item(2, rights='All rights reserved'))
        urls = _capture(http, queue, rss([feed_item(1), other]), forged_mixed=True)
        before = queue.db.total_changes
        proof = audit(tmp_path, urls, registry=http.registry)
        assert not proof['accepted']
        assert proof['errors'][0]['error'] == 'selected_feed_contains_restricted_text'
        assert queue.db.total_changes == before
    finally:
        queue.close()
        http.close()


def test_selected_rss_replay_preserves_valid_item_when_neighbour_only_has_abstract(config, tmp_path):
    http, queue = feed_fixture(config, tmp_path)
    try:
        urls = _capture(http, queue, rss([feed_item(1), feed_item(2, fulltext=False)]))
        proof = audit(tmp_path, urls, registry=http.registry)
        assert proof['accepted'] and len(proof['articles']) == 1
        assert not queue.db.execute("SELECT body_sha256 FROM articles WHERE url='https://publisher.test/story/2'").fetchone()
    finally:
        queue.close()
        http.close()


def test_selected_rss_replay_rejects_ambiguous_duplicate_selected_identity(config, tmp_path):
    http, queue = feed_fixture(config, tmp_path)
    try:
        urls = _capture(http, queue, rss([feed_item(1), feed_item(1, author='Conflicting byline')]))
        proof = audit(tmp_path, urls, registry=http.registry)
        assert not proof['accepted']
        assert proof['errors'][0]['error'] == 'selected_feed_item_not_replayable'
    finally:
        queue.close()
        http.close()


def test_selected_rss_replay_requires_registered_original_feed(config, tmp_path):
    http, queue = feed_fixture(config, tmp_path)
    try:
        urls = _capture(http, queue, rss([feed_item(1)]))
        config['providers'][0]['fulltext_feeds'] = []
        from downloader.news_fulltext_policy import PolicyRegistry
        changed = PolicyRegistry(config, base=tmp_path)
        proof = audit(tmp_path, urls, registry=changed)
        assert not proof['accepted']
        assert proof['errors'][0]['error'] == 'unregistered_bulk_feed'
    finally:
        queue.close()
        http.close()

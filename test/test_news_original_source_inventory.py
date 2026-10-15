"""Original source visibility is metadata evidence, never fetch permission."""

from datetime import UTC, datetime
import json
import sqlite3

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_urls import (
    backfill_original_urls, initialize_original_source_inventory, original_source_inventory,
)
from scripts import audit_news_providers
from scripts.verify_news_dashboard import original_source_inventory_check
from stockagent.live.news_dashboard import build_news_public_status
from test_news_fulltext import config
from test_news_original_queue import alias


@pytest.fixture
def queue(config, tmp_path):
    q = NewsQueue(tmp_path / 'data_news_fulltext', PolicyRegistry(config, base=tmp_path))
    atomic_write_json(tmp_path / 'configs/news_fulltext.json', config)
    try:
        yield q
    finally:
        q.close()


def seed(queue, url='https://new-origin.test/a?private=PRIVATE_TOKEN'):
    first = alias(url)
    with queue.db:
        queue.add_articles([first, {**first, 'url': first['url'] + '&hl=zh-TW'}], origin_id=42)
    assert backfill_original_urls(queue)['literal_links'] == 2


def source_inventory(queue):
    return original_source_inventory(queue.db, queue.registry)


def test_unqueued_originals_are_visible_without_adding_body_work_or_permission(queue, tmp_path):
    seed(queue)
    result = source_inventory(queue)
    assert result['available'] and result['source_keys'] == result['original_hosts'] == 1
    assert result['literal_metadata_links'] == 2
    assert result['sources']['unregistered:new-origin.test']['literal_metadata_links'] == 2
    status = queue.status()
    original = status['providers']['unregistered:new-origin.test']
    assert original['states'] == {} and original['text_bytes'] == 0
    assert not original['automatic_collection_enabled'] and original['authorization_kind'] == 'unreviewed'
    assert status['unique_articles'] == 2 and status['articles_with_stored_body'] == 0
    assert queue.db.execute('SELECT count(*) FROM source_links').fetchone()[0] == 2
    assert queue.db.execute('SELECT sum(target_queued) FROM original_url_resolutions').fetchone()[0] == 0
    assert result['publisher_requests'] == 0 and not result['body_acceptance_proven']
    public = build_news_public_status(tmp_path, now=datetime.now(UTC))
    row = next(p for p in public['providers'] if p['id'] == 'unregistered:new-origin.test')
    assert row['known_urls'] == row['complete_urls'] == row['remaining_urls'] == 0
    assert row['progress_ratio'] is row['conditional_eta_seconds'] is None
    assert row['state'] == 'not_enabled' and not row['enabled']
    assert row['original_metadata']['literal_metadata_links'] == 2
    assert public['original_source_inventory']['available']
    assert public['acquisition']['known_urls'] == 2
    assert original_source_inventory_check(public)['metadata_only_source_keys'] == 1
    assert 'PRIVATE_' not in json.dumps(public) and '?private=' not in json.dumps(public)


def test_alias_incidence_is_not_unique_articles_and_route_updates_do_not_double_count(queue):
    seed(queue, 'https://publisher.test/a')
    assert queue.db.execute("SELECT count(*) FROM articles WHERE provider='publisher'").fetchone()[0] == 1
    evidence = source_inventory(queue)['sources']['publisher']
    assert evidence == {'literal_metadata_links': 2, 'hosts': [{'host': 'publisher.test', 'metadata_links': 2}]}
    before = source_inventory(queue)
    with queue.db:
        queue.db.execute('UPDATE original_url_resolutions SET target_queued=0')
        queue.db.execute('UPDATE original_url_resolutions SET target_queued=1')
    assert source_inventory(queue) == before
    with queue.db:
        queue.db.execute('DELETE FROM original_url_resolutions WHERE alias_id=(SELECT min(alias_id) FROM original_url_resolutions)')
    assert source_inventory(queue)['literal_metadata_links'] == 1


def test_source_policy_matching_aggregates_hosts_not_subdomains_as_publishers(config, tmp_path):
    config['providers'][0]['domains'].append('secondary.test')
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with q.db:
            q.add_articles([alias('https://publisher.test/a'), alias('https://secondary.test/b')])
        backfill_original_urls(q)
        result = source_inventory(q)
        assert result['source_keys'] == 1 and result['original_hosts'] == 2
        assert result['sources']['publisher']['literal_metadata_links'] == 2
    finally:
        q.close()


def drop_summary(db):
    with db:
        for name in ('original_host_insert', 'original_host_delete', 'original_host_update'):
            db.execute('DROP TRIGGER ' + name)
        db.execute('DROP TABLE original_url_host_metrics')
        db.execute("DELETE FROM queue_settings WHERE key='original_source_inventory_v1'")


def canonical_rows(db):
    return {name: list(map(tuple, db.execute('SELECT * FROM ' + name + ' ORDER BY 1,2')))
            for name in ('articles', 'source_links', 'original_url_resolutions', 'original_url_metrics')}


def test_legacy_backfill_is_one_time_recoverable_and_preserves_canonical_rows(queue):
    seed(queue)
    before = canonical_rows(queue.db)
    drop_summary(queue.db)
    assert not source_inventory(queue)['available']
    statements = []
    queue.db.set_trace_callback(statements.append)
    initialize_original_source_inventory(queue.db)
    assert canonical_rows(queue.db) == before
    assert source_inventory(queue)['literal_metadata_links'] == 2
    assert sum(s.startswith('INSERT INTO original_url_host_metrics SELECT') for s in statements) == 1
    statements.clear()
    initialize_original_source_inventory(queue.db)
    assert not any(s.startswith('INSERT INTO original_url_host_metrics SELECT') for s in statements)
    queue.db.set_trace_callback(None)


def test_rebuild_failure_rolls_back_counts_and_marker_then_can_resume(queue):
    seed(queue)
    before = canonical_rows(queue.db)
    with queue.db:
        queue.db.execute("DELETE FROM queue_settings WHERE key='original_source_inventory_v1'")
        queue.db.execute('UPDATE original_url_host_metrics SET metadata_links=99')
    queue.db.set_authorizer(lambda op, table, *args: sqlite3.SQLITE_DENY
                            if op == sqlite3.SQLITE_INSERT and table == 'queue_settings' else sqlite3.SQLITE_OK)
    try:
        with pytest.raises(sqlite3.DatabaseError):
            initialize_original_source_inventory(queue.db)
    finally:
        queue.db.set_authorizer(None)
    assert queue.db.execute('SELECT metadata_links FROM original_url_host_metrics').fetchone()[0] == 99
    assert queue.db.execute("SELECT 1 FROM queue_settings WHERE key='original_source_inventory_v1'").fetchone() is None
    assert canonical_rows(queue.db) == before
    initialize_original_source_inventory(queue.db)
    assert source_inventory(queue)['literal_metadata_links'] == 2


def test_derived_count_mismatch_is_unavailable_then_repairs_only_summary(queue):
    seed(queue)
    before = canonical_rows(queue.db)
    with queue.db:
        queue.db.execute('UPDATE original_url_host_metrics SET metadata_links=99')
    assert source_inventory(queue)['unavailable_reason'] == 'original_source_summary_not_reconciled'
    initialize_original_source_inventory(queue.db)
    assert source_inventory(queue)['available'] and source_inventory(queue)['literal_metadata_links'] == 2
    assert canonical_rows(queue.db) == before


def test_real_owner_restart_preserves_summary_and_every_canonical_proof(config, tmp_path):
    root = tmp_path / 'source'
    q = NewsQueue(root, PolicyRegistry(config, base=tmp_path))
    try:
        seed(q)
        before = canonical_rows(q.db)
        summary = source_inventory(q)
        owner = q.owner
    finally:
        q.close()
    restarted = NewsQueue(root, PolicyRegistry(config, base=tmp_path))
    try:
        assert restarted.owner != owner
        assert source_inventory(restarted) == summary
        assert canonical_rows(restarted.db) == before
        assert restarted.status()['original_source_inventory']['available']
    finally:
        restarted.close()


def test_heartbeat_reads_host_summary_not_million_row_proof_frontier(queue):
    seed(queue)
    queue.db.set_authorizer(lambda op, table, *args: sqlite3.SQLITE_DENY
                            if op == sqlite3.SQLITE_READ and table == 'original_url_resolutions' else sqlite3.SQLITE_OK)
    try:
        assert source_inventory(queue)['literal_metadata_links'] == 2
        assert queue.status()['original_source_inventory']['available']
    finally:
        queue.db.set_authorizer(None)


def test_readonly_full_inventory_includes_unqueued_source_without_inflating_article_totals(queue, tmp_path, config, monkeypatch):
    seed(queue)
    config['root'] = 'data_news_fulltext'
    monkeypatch.setattr(audit_news_providers, 'load_config', lambda _: (config, queue.registry, tmp_path))
    before = canonical_rows(queue.db)
    result = audit_news_providers.inventory(tmp_path / 'configs/news_fulltext.json')
    original = result['providers']['unregistered:new-origin.test']
    assert original['known_urls'] == original['stored_bodies'] == 0
    assert original['original_metadata']['literal_metadata_links'] == 2
    assert original['source_role'] == 'publisher_or_other_unverified'
    assert original['authorization']['kind'] == 'unreviewed' and not original['enabled']
    assert result['inventory_totals']['known_urls'] == 2 and result['inventory_totals']['metadata_only_source_keys'] == 1
    assert canonical_rows(queue.db) == before


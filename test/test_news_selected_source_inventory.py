"""Selected-source work cannot disappear behind partial queue coverage."""
from copy import deepcopy
import csv
import json
import sqlite3

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_urls import backfill_original_urls
from scripts import audit_news_providers as audit
from scripts.plan_news_sources import plan_sources
from test_news_fulltext import config
from test_news_original_queue import alias


@pytest.fixture
def populated(config, tmp_path, monkeypatch):
    config['root'] = 'data_news_fulltext'
    template = config['providers'][0]
    blocked = deepcopy(template)
    blocked.update(id='blocked', domains=['blocked.test'], enabled=False,
                   feeds=['https://blocked.test/feed'])
    empty = deepcopy(template)
    empty.update(id='empty', domains=['empty.test'], enabled=False,
                 feeds=['https://empty.test/feed'])
    config['providers'].extend([blocked, empty])
    path = tmp_path / 'configs/news_fulltext.json'
    atomic_write_json(path, config)
    registry = PolicyRegistry(config, base=tmp_path)
    q = NewsQueue(tmp_path / config['root'], registry)
    try:
        a = alias('https://blocked.test/a')
        with q.db:
            q.add_articles([{'url': 'https://publisher.test/1'}, {'url': 'https://blocked.test/a'},
                            a, {**a, 'url': a['url'] + '&hl=zh-TW'},
                            alias('https://blocked.test/b'), alias('https://metadata-only.test/1')])
            q.db.execute("UPDATE articles SET state='retry',error_code='fixture_recheck',body_sha256=? "
                         "WHERE provider='blocked'", ('b' * 64,))
        backfill_original_urls(q)
        atomic_write_json(q.root / 'status.json', {'owner_id': q.owner,
            'providers': {'publisher': {'fixture': 1}, 'blocked': {'fixture': 2}},
            'unique_articles': 999})
        monkeypatch.setattr(audit, 'load_config', lambda _: (config, registry, tmp_path))
        yield q, path
    finally:
        q.close()


def test_selected_inventory_is_exact_subset_and_never_mutates_or_scans_other_heads(populated, monkeypatch):
    q, path = populated
    before = list(q.db.iterdump())
    full = audit.inventory(path)
    statements = []
    connect = sqlite3.connect

    def observed(*args, **kwargs):
        db = connect(*args, **kwargs)
        db.set_trace_callback(statements.append)
        return db

    monkeypatch.setattr(audit.sqlite3, 'connect', observed)
    chosen = ['unregistered:metadata-only.test', 'blocked', 'empty', 'blocked']
    result = audit.inventory(path, providers=chosen)
    assert result['providers'] == {p: full['providers'][p] for p in set(chosen)}
    assert result['inventory_scope'] == 'selected_source_keys'
    assert not result['all_known_source_keys_included']
    assert result['selected_source_keys'] == sorted(set(chosen))
    assert result['full_frontier_scan_contract_version'] is None
    assert full['full_frontier_scan_contract_version'] == 1
    assert full['all_known_source_keys_included']
    assert result['inventory_totals']['known_urls'] == 1
    assert result['inventory_totals']['configured_providers'] == 2
    assert result['inventory_totals']['configured_providers_overall'] == 3
    assert result['inventory_totals']['metadata_only_source_keys'] == 1
    assert result['original_source_inventory']['literal_metadata_links'] == 4
    assert result['original_source_inventory']['source_keys'] == 2
    assert set(result['status']['providers']) == {'blocked'}
    assert 'unique_articles' not in result['status']
    assert {r['provider'] for r in result['endpoints']} == {'blocked', 'empty'}
    frontier = [s for s in statements if 'FROM articles' in s]
    assert frontier and all('provider IN (' in s for s in frontier)
    assert all('NOT INDEXED' not in s for s in frontier)
    assert 'PRAGMA query_only=ON' in statements and 'BEGIN' in statements
    assert not result['publisher_requests'] and not result['production_queue_writes']
    assert list(q.db.iterdump()) == before


def test_exact_targets_count_duplicate_links_and_real_heads_not_queued_hint(populated):
    q, path = populated
    with q.db:
        # Deliberately stale receipt hints cannot stand in for current heads.
        q.db.execute('UPDATE original_url_resolutions SET target_queued=1')
    before = list(q.db.iterdump())
    result = audit.inventory(path, providers=['blocked', 'empty', 'unregistered:metadata-only.test'],
                             resolve_link_coverage=True)
    coverage = result['providers']['blocked']['original_metadata']['target_url_coverage']
    assert {k: coverage[k] for k in ('alias_associations', 'distinct_target_urls', 'queued_target_urls',
                                   'stored_body_target_urls', 'unqueued_target_urls')} == {
        'alias_associations': 3, 'distinct_target_urls': 2, 'queued_target_urls': 1,
        'stored_body_target_urls': 1, 'unqueued_target_urls': 1}
    metadata = result['providers']['unregistered:metadata-only.test']['original_metadata']['target_url_coverage']
    assert metadata['distinct_target_urls'] == metadata['unqueued_target_urls'] == 1
    empty = result['providers']['empty']['original_metadata']['target_url_coverage']
    assert empty['distinct_target_urls'] == empty['unqueued_target_urls'] == 0
    assert coverage['metadata_identity_verified'] is coverage['capture_access_granted'] is False
    planned = plan_sources(result)
    assert planned['all_input_source_keys_planned']
    assert not planned['all_source_keys_planned'] and not planned['all_sites_live_reviewed']
    assert planned['contract_version'] == 8
    row = next(r for r in planned['sources'] if r['provider'] == 'blocked')
    assert row['known_urls'] == row['stored_bodies'] == 1
    assert row['unqueued_literal_link_work'] and row['literal_target_coverage_verified']
    assert row['unqueued_target_urls'] == 1
    assert not row['enabled'] and not planned['plan_is_acquisition_acceptance']
    assert list(q.db.iterdump()) == before


def test_default_audit_and_endpoint_only_never_read_resolution_frontier(populated, monkeypatch):
    _, path = populated
    connect = sqlite3.connect

    def guarded(*args, **kwargs):
        db = connect(*args, **kwargs)
        db.set_authorizer(lambda op, table, *rest: sqlite3.SQLITE_DENY
                          if op == sqlite3.SQLITE_READ and table == 'original_url_resolutions' else sqlite3.SQLITE_OK)
        return db

    monkeypatch.setattr(audit.sqlite3, 'connect', guarded)
    assert audit.inventory(path)['inventory_scope'] == 'all_known_source_keys'
    selected = audit.inventory(path, providers=['blocked'])
    assert 'target_url_coverage' not in selected['providers']['blocked']['original_metadata']
    endpoints = audit.inventory_endpoints_only(path, providers=['empty'])
    assert {r['provider'] for r in endpoints['endpoints']} == {'empty'}
    assert not endpoints['provider_inventory_included']


def test_selected_cli_records_scope_and_exact_targets_in_json_and_csv(populated, tmp_path):
    _, path = populated
    output = tmp_path / 'output/selected.json'
    assert audit.main(['--config', str(path), '--output', str(output), '--provider', 'blocked',
                       '--resolve-link-coverage']) == 0
    result = json.loads(output.read_bytes())
    assert set(result['providers']) == {'blocked'}
    with output.with_suffix('.csv').open() as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 1 and rows[0]['inventory_scope'] == 'selected_source_keys'
    assert rows[0]['all_known_source_keys_included'] == 'False'
    assert json.loads(rows[0]['literal_target_coverage_json'])['unqueued_target_urls'] == 1


@pytest.mark.parametrize('providers', [[], 'blocked', [True], [''], [' blocked']])
def test_invalid_selection_cannot_fall_back_to_global_scan(populated, providers):
    _, path = populated
    with pytest.raises(ValueError, match='provider selection'):
        audit.inventory(path, providers=providers)


def test_unknown_key_or_implicit_global_target_join_is_rejected(populated):
    _, path = populated
    with pytest.raises(ValueError, match='unknown source key'):
        audit.inventory(path, providers=["unknown' OR 1=1 --"])
    with pytest.raises(ValueError, match='explicit provider selection'):
        audit.inventory(path, resolve_link_coverage=True)
    with pytest.raises(SystemExit) as exc:
        audit.main(['--output', 'unused.json', '--resolve-link-coverage'])
    assert exc.value.code == 2


def test_unavailable_summary_is_not_silently_zero_target_work(populated):
    q, path = populated
    with q.db:
        q.db.execute("DELETE FROM queue_settings WHERE key='original_source_inventory_v1'")
    with pytest.raises(ValueError, match='reconciled original-source summary'):
        audit.inventory(path, providers=['blocked'], resolve_link_coverage=True)


@pytest.mark.parametrize('change', [
    {'all_known_source_keys_included': True}, {'selected_source_keys': ['wrong']},
    {'selected_source_keys': []}, {'inventory_scope_contract_version': True},
    {'inventory_scope': 'registered_endpoint_capabilities_only'},
])
def test_false_global_or_mismatched_selected_claim_is_rejected(populated, change):
    _, path = populated
    result = audit.inventory(path, providers=['blocked'])
    result.update(change)
    with pytest.raises(ValueError, match='scope'):
        plan_sources(result)

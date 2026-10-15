"""Anue's metadata API must not disappear or masquerade as category bodies."""
from copy import deepcopy
from dataclasses import replace
import json
import sqlite3
import subprocess
import sys

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext_cnyes_index import ENDPOINT
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from scripts import audit_news_providers
from scripts.plan_news_sources import implementation_plan
from test_cnyes_news_bulk import BASE, cfg


def test_complete_inventory_lists_independent_public_metadata_API(cfg, tmp_path, monkeypatch):
    cfg['root'] = 'data_news_fulltext'
    atomic_write_json(tmp_path / 'configs/news_fulltext.json', cfg)
    registry = PolicyRegistry(cfg, base=tmp_path)
    queue = NewsQueue(tmp_path / cfg['root'], registry)
    try:
        monkeypatch.setattr(audit_news_providers, 'load_config', lambda _: (cfg, registry, tmp_path))
        before = list(queue.db.execute('SELECT * FROM discovery'))
        result = audit_news_providers.inventory(tmp_path / 'configs/news_fulltext.json')
        assert result['endpoint_inventory_contract_version'] == 2
        rows = [r for r in result['endpoints'] if r['endpoint_or_pattern'] == ENDPOINT]
        assert len(rows) == 1
        assert rows[0]['kind'] == 'cnyes_public_index'
        assert rows[0]['metadata_only'] is True
        assert rows[0]['page_number_cap_observed'] is None
        assert rows[0]['history_complete'] is False
        category = next(r for r in result['endpoints'] if r['endpoint_or_pattern'] == BASE)
        assert category['page_number_cap_observed'] == 30
        assert list(queue.db.execute('SELECT * FROM discovery')) == before
    finally:
        queue.close()


def test_implementation_plan_lists_metadata_separately_from_bulk_content(cfg):
    entry = cfg['providers'][0]
    original = deepcopy(entry)
    row = implementation_plan({'enabled': True, 'source_role': 'publisher_or_other_unverified'},
                              'cnyes', entry, {})
    endpoints = json.loads(row['known_endpoints_json'])
    assert endpoints['public_metadata_index'] == [ENDPOINT]
    assert endpoints['bulk_json'] == [BASE]
    assert row['endpoint_listing_contract_version'] == 2
    assert entry == original


def test_endpoint_catalog_is_shared_and_deduplicates_the_metadata_API(cfg, tmp_path):
    registry = PolicyRegistry(cfg, base=tmp_path)
    source = registry.sources['cnyes']
    registry.sources['cnyes'] = replace(source, index_pages=(*source.index_pages, ENDPOINT))
    status = {'http_capacity': {'providers': {'cnyes': {'effective_interval_seconds': .2, 'parallelism': 7}}},
              'performance': {'providers': {'cnyes': {'counts': {'endpoint_other_metadata_requests': 41,
                                                                'public_index_pages_verified': 40}}}}}
    rows = audit_news_providers.endpoint_inventory(registry, status=status, owner_matches=True)
    metadata = [r for r in rows if r['endpoint_or_pattern'] == ENDPOINT]
    assert len(metadata) == 1 and metadata[0]['kind'] == 'cnyes_public_index'
    assert metadata[0]['owner_requests_in_endpoint_family'] == 41
    assert metadata[0]['owner_verified_metadata_pages'] == 40
    assert metadata[0]['actual_client_interval_seconds'] == .2 and metadata[0]['parallelism'] == 7
    assert metadata[0]['engineering_guard_is_not_source_limit'] and metadata[0]['body_acceptance_by_contract'] == 0
    assert metadata[0]['official_rps_limit'] is None and not metadata[0]['source_maximum_verified']
    old_owner = next(r for r in audit_news_providers.endpoint_inventory(registry, status=status) if r['endpoint_or_pattern'] == ENDPOINT)
    assert old_owner['owner_requests_in_endpoint_family'] is old_owner['owner_verified_metadata_pages'] is None
    assert not old_owner['runtime_capacity_observed']


def test_disabled_registered_metadata_is_a_capability_not_permission(cfg, tmp_path):
    registry = PolicyRegistry(cfg, base=tmp_path)
    registry.sources['cnyes'] = replace(registry.sources['cnyes'], allowed=False)
    row = next(r for r in audit_news_providers.endpoint_inventory(registry) if r['endpoint_or_pattern'] == ENDPOINT)
    assert row['enabled'] is False and row['metadata_only'] is True
    assert row['history_complete'] is False


def test_endpoint_only_mode_does_not_scan_or_write_any_article_frontier(cfg, tmp_path, monkeypatch):
    cfg['root'] = 'data_news_fulltext'
    path = tmp_path / 'configs/news_fulltext.json'
    atomic_write_json(path, cfg)
    registry = PolicyRegistry(cfg, base=tmp_path)
    queue = NewsQueue(tmp_path / cfg['root'], registry)
    try:
        atomic_write_json(tmp_path / cfg['root'] / 'status.json', queue.status())
        monkeypatch.setattr(audit_news_providers, 'load_config', lambda _: (cfg, registry, tmp_path))
        native_connect, statements = sqlite3.connect, []
        def bounded_connect(*args, **kwargs):
            assert 'mode=ro' in args[0]
            db = native_connect(*args, **kwargs)
            db.set_trace_callback(statements.append)
            db.set_authorizer(lambda op, table, *args: sqlite3.SQLITE_DENY
                if op in {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}
                or op == sqlite3.SQLITE_READ and table != 'queue_settings' else sqlite3.SQLITE_OK)
            return db
        monkeypatch.setattr(audit_news_providers.sqlite3, 'connect', bounded_connect)
        result = audit_news_providers.inventory_endpoints_only(path)
        assert result['endpoint_inventory_contract_version'] == 2
        assert result['inventory_scope'] == 'registered_endpoint_capabilities_only'
        assert result['provider_inventory_included'] is result['body_count_or_history_coverage_claimed'] is False
        assert result['publisher_requests'] == 0 and not result['production_queue_writes']
        assert 'providers' not in result and 'stored_bodies' not in result
        assert len(statements) == 1 and 'queue_settings' in statements[0]
        assert any(r['endpoint_or_pattern'] == ENDPOINT for r in result['endpoints'])
    finally:
        queue.close()


@pytest.mark.parametrize('script', ['audit_news_providers.py', 'plan_news_sources.py'])
def test_documented_direct_script_entry_can_load_without_PYTHONPATH(script, tmp_path):
    from pathlib import Path
    import os
    repo = Path(__file__).resolve().parents[1]
    environment = dict(os.environ)
    environment.pop('PYTHONPATH', None)
    process = subprocess.run([sys.executable, str(repo / 'scripts' / script), '--help'],
        cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=20)
    assert process.returncode == 0, process.stderr
    assert '--output' in process.stdout


@pytest.mark.parametrize('change', ['foreign', 'unreviewed'])
def test_plan_metadata_registration_uses_the_original_validation(cfg, change):
    entry = deepcopy(cfg['providers'][0])
    provider = 'other' if change == 'foreign' else 'cnyes'
    if change == 'unreviewed':
        entry['cnyes_public_index']['endpoint'] = 'https://invalid.test/not-reviewed'
    with pytest.raises(ValueError, match='unreviewed Cnyes public index'):
        implementation_plan({'enabled': False, 'source_role': 'publisher_or_other_unverified'}, provider, entry, {})

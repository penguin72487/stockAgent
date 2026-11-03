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
    assert all('date(metadata_order_day' not in s for s in frontier)
    assert 'PRAGMA query_only=ON' in statements and 'BEGIN' in statements
    assert not result['publisher_requests'] and not result['production_queue_writes']
    assert list(q.db.iterdump()) == before


def test_optional_sort_day_quality_distinguishes_unknown_and_observed_cases(populated):
    q, path = populated
    samples = {
        'ordinary': '2020-02-29',
        'historical': '1899-12-30',
        'invalid': '2023-02-29',
        'invalid_month': '2023-99-01',
        'invalid_year': '0000-01-01',
        'malformed': 'not-a-calendar-day',
        'future': '9999-12-31',
        'missing': '',
    }
    with q.db:
        q.add_articles([{'url': f'https://publisher.test/{name}', 'date': 'source original'}
                        for name in samples])
        for name, day in samples.items():
            q.db.execute('UPDATE articles SET metadata_order_day=? WHERE url=?',
                         (day, f'https://publisher.test/{name}'))
    before = list(q.db.iterdump())
    default = audit.inventory(path, providers=['publisher'])
    default_row = next(row for row in plan_sources(default)['sources'] if row['provider'] == 'publisher')
    assert default_row['metadata_sort_days_present'] is None
    checked = audit.inventory(path, providers=['publisher', 'empty'], sort_day_quality=True)
    assert checked['metadata_sort_day_quality_contract_version'] == 1
    assert checked['metadata_sort_day_quality_requested'] is True
    item = checked['providers']['publisher']
    assert {key: item[key] for key in ('metadata_sort_days_present', 'metadata_sort_days_invalid',
                                       'metadata_sort_days_pre_1900', 'metadata_sort_days_after_snapshot_utc')} == {
        'metadata_sort_days_present': 7, 'metadata_sort_days_invalid': 4,
        'metadata_sort_days_pre_1900': 1, 'metadata_sort_days_after_snapshot_utc': 1}
    row = next(row for row in plan_sources(checked)['sources'] if row['provider'] == 'publisher')
    assert row['metadata_sort_days_invalid'] == 4 and row['metadata_sort_days_pre_1900'] == 1
    assert row['metadata_sort_days_after_snapshot_utc'] == 1
    assert checked['providers']['empty']['metadata_sort_days_present'] == 0
    assert list(q.db.iterdump()) == before


def test_full_inventory_cli_publishes_reusable_compact_sort_summary(populated, tmp_path):
    import hashlib
    from datetime import datetime, timezone
    from scripts.publish_news_sort_day_status import publish
    q, path = populated
    before = list(q.db.iterdump())
    output = tmp_path / 'artifacts/data_quality/fixture/inventory.json'
    summary_path = output.with_name('sort.json')
    assert audit.main(['--config', str(path), '--output', str(output), '--sort-day-quality',
                       '--sort-day-summary', str(summary_path)]) == 0
    full, summary = json.loads(output.read_text()), json.loads(summary_path.read_text())
    assert summary['source_inventory_sha256'] == hashlib.sha256(output.read_bytes()).hexdigest()
    assert summary['totals']['known_heads'] == full['inventory_totals']['known_urls']
    assert summary['provider_keys_with_heads'] == sum(item['known_urls'] > 0 for item in full['providers'].values())
    assert publish(tmp_path, summary_path, now=datetime.now(timezone.utc))['known_heads'] == summary['totals']['known_heads']
    assert list(q.db.iterdump()) == before


@pytest.mark.parametrize('extra', [[], ['--sort-day-quality', '--provider', 'publisher']])
def test_compact_summary_requires_a_full_quality_scan(populated, tmp_path, extra):
    _, path = populated
    with pytest.raises(SystemExit):
        audit.main(['--config', str(path), '--output', str(tmp_path / 'inventory.json'),
                    '--sort-day-summary', str(tmp_path / 'sort.json'), *extra])


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
    assert planned['contract_version'] == 11
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
                       '--resolve-link-coverage', '--sort-day-quality']) == 0
    result = json.loads(output.read_bytes())
    assert set(result['providers']) == {'blocked'}
    assert result['metadata_sort_day_quality_requested'] is True
    with output.with_suffix('.csv').open() as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 1 and rows[0]['inventory_scope'] == 'selected_source_keys'
    assert rows[0]['all_known_source_keys_included'] == 'False'
    assert rows[0]['metadata_sort_days_present'] == rows[0]['metadata_sort_days_invalid'] == '0'
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


def test_offline_immutable_inventory_matches_readonly_snapshot_without_writing(populated):
    q, path = populated
    q.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    normal = audit.inventory(path)
    files = [q.root / 'queue.sqlite3', q.root / 'queue.sqlite3-wal', q.root / 'status.json']
    before = [(p.stat().st_size, p.stat().st_mtime_ns, p.stat().st_ctime_ns) for p in files]
    result = audit.inventory(path, offline_immutable=True)
    after = [(p.stat().st_size, p.stat().st_mtime_ns, p.stat().st_ctime_ns) for p in files]
    assert result['providers'] == normal['providers']
    assert result['inventory_totals'] == normal['inventory_totals']
    assert result['offline_snapshot']['wal_empty']
    assert result['offline_snapshot']['rollback_journal_empty']
    assert str(path.resolve()) in result['offline_snapshot']['configuration_sha256']
    assert result['offline_snapshot']['writer_exclusion_proven'] is False
    assert before == after


def test_offline_immutable_rejects_uncheckpointed_wal_and_publishes_nothing(populated, tmp_path):
    q, path = populated
    q.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    with q.db:
        q.db.execute("INSERT INTO queue_settings(key,value) VALUES('fixture_uncheckpointed', '1')")
    assert (q.root / 'queue.sqlite3-wal').stat().st_size > 0
    output = tmp_path / 'output/unsafe.json'
    with pytest.raises(ValueError, match='WAL'):
        audit.main(['--config', str(path), '--output', str(output), '--offline-immutable'])
    assert not output.exists() and not output.with_suffix('.csv').exists()


def test_offline_immutable_rejects_nonempty_rollback_journal(populated, tmp_path):
    q, path = populated
    q.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    (q.root / 'queue.sqlite3-journal').write_bytes(b'unreplayed journal')
    output = tmp_path / 'output/unsafe_journal.json'
    with pytest.raises(ValueError, match='rollback journal'):
        audit.main(['--config', str(path), '--output', str(output), '--offline-immutable'])
    assert not output.exists() and not output.with_suffix('.csv').exists()


def test_offline_immutable_discards_result_when_snapshot_changes(populated, tmp_path, monkeypatch):
    q, path = populated
    q.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    original = audit._offline_snapshot_signature
    calls = []

    def changed(root):
        calls.append(1)
        if len(calls) == 2:
            (q.root / 'status.json').write_text('{}')
        return original(root)

    monkeypatch.setattr(audit, '_offline_snapshot_signature', changed)
    output = tmp_path / 'output/changed.json'
    with pytest.raises(ValueError, match='changed'):
        audit.main(['--config', str(path), '--output', str(output), '--offline-immutable'])
    assert not output.exists() and not output.with_suffix('.csv').exists()


def test_offline_immutable_discards_result_if_configuration_changes(populated, tmp_path, monkeypatch):
    q, path = populated
    q.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    original = audit._offline_snapshot_signature
    calls = []

    def changed(root):
        calls.append(1)
        if len(calls) == 2:
            path.write_text(path.read_text() + '\n')
        return original(root)

    monkeypatch.setattr(audit, '_offline_snapshot_signature', changed)
    output = tmp_path / 'output/changed_config.json'
    with pytest.raises(ValueError, match='configuration changed'):
        audit.main(['--config', str(path), '--output', str(output), '--offline-immutable'])
    assert not output.exists() and not output.with_suffix('.csv').exists()


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

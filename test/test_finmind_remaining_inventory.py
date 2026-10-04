"""Inventory resolves the owning row, never the latest observer as all history."""
from datetime import UTC, datetime

from scripts import audit_finmind_remaining as audit


def test_inventory_deduplicates_catalog_and_selects_canonical_owner(monkeypatch, tmp_path):
    sponsor = 'TaiwanStockPrice'
    complement = 'TaiwanFuturesKBar'
    contracts = {name: {'primary_owner': owner, 'query_shape': 'shape', 'configured_first_date': '2011-01-03'}
                 for name, owner in ((sponsor, 'sponsor'), (complement, 'complement'))}
    monkeypatch.setattr(audit, 'registry', lambda: contracts)
    rows = [
        {'id': sponsor, 'state': 'delegated', 'rows': 0},
        {'id': sponsor, 'state': 'observed', 'rows': 7},
        {'id': sponsor + ':all_market', 'state': 'complete', 'rows': 10000, 'first_data_date': '1994-10-01',
         'target_partitions': 100, 'update_monitor': {'release': {'hour': 17, 'minute': 30}}},
        {'id': complement + ':all_market', 'state': 'not_implemented', 'rows': 0},
        {'id': complement, 'state': 'backfilling', 'rows': 123, 'materialized_partitions': 120,
         'target_partitions': 1000000},
    ]
    monkeypatch.setattr(audit, 'build_finmind_public_status', lambda *args, **kwargs: {
        'datasets': rows, 'acquisition': {}, 'quota': {}})
    monkeypatch.setattr(audit, 'build_finmind_workload', lambda *args: {
        'datasets': [], 'summary': {'current_plan_requests': 0}})
    result = audit.inventory(tmp_path, datetime(2026, 10, 2, tzinfo=UTC))
    items = {row['dataset']: row for row in result['datasets']}
    assert result['catalog_rows'] == 5 and result['unique_datasets'] == 2
    assert items[sponsor]['rows'] == 10000 and items[sponsor]['first_observed_date'] == '1994-10-01'
    assert items[sponsor]['materialized_tasks'] == 100
    assert items[sponsor]['release_minute_taipei'] == 30
    assert items[complement]['rows'] == 123 and items[complement]['materialized_tasks'] == 120
    assert result['provider_calls'] == 0 and result['queue_writes'] == 0

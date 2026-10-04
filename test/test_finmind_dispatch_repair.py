"""Scheduling, minute receipts and dashboard must describe the same workload."""
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path

import pytest

from downloader import download_finmind_complement as worker
from downloader import finmind_account as account
from downloader import finmind_supplemental as supplemental
from downloader import finmind_eta_work as work
from scripts import snapshot_finmind_quota as sampler
from stockagent.live.finmind_dashboard import build_finmind_public_status


NOW = datetime(2026, 10, 2, 15, tzinfo=UTC)


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def test_ordered_history_retains_refresh_preemption_and_puts_us_minutes_last(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [
            ('TaiwanFuturesKBar', 'TX', '2026-10-01', 'id_day', 8),
            ('TaiwanStockKBar', '2330', '2026-10-01', 'id_day', 8),
            ('USStockPriceMinute', 'AAPL', '2026-10-01', 'id_day', 8),
            ('TaiwanFuturesTick', 'TX', '2026-10-01', 'id_day', 10),
        ])
        choices = [worker._next_task(conn, NOW, advance_cursor=True).dataset for _ in range(6)]
        assert choices == ['TaiwanStockKBar'] * 6
        cursor = conn.execute('SELECT * FROM dispatch_cursor').fetchall()
        for _ in range(3):
            worker._next_task(conn, NOW)
        assert conn.execute('SELECT * FROM dispatch_cursor').fetchall() == cursor
        worker._add_tasks(conn, [('TaiwanStockInfo', '', 'latest', 'snapshot', 0)])
        assert worker._next_task(conn, NOW, advance_cursor=True).dataset == 'TaiwanStockInfo'
        conn.execute("UPDATE tasks SET state='complete' WHERE priority=0 OR dataset='TaiwanStockKBar'")
        assert worker._next_task(conn, NOW, advance_cursor=True).dataset == 'TaiwanFuturesKBar'
        conn.execute("UPDATE tasks SET state='complete' WHERE dataset='TaiwanFuturesKBar'")
        assert worker._next_task(conn, NOW, advance_cursor=True).dataset == 'TaiwanFuturesTick'
        conn.execute("UPDATE tasks SET state='complete' WHERE dataset='TaiwanFuturesTick'")
        assert worker._next_task(conn, NOW).dataset == 'USStockPriceMinute'


def test_due_refresh_and_retry_preserve_cooldown_and_terminal_states(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [
            ('GoldPrice', '', '2014', 'year', 3),
            ('GoldPrice', '', '2015', 'year', 3),
            ('GoldPrice', '', '2016', 'year', 3),
            ('GoldPrice', '', '2017', 'year', 3),
        ])
        conn.execute("UPDATE tasks SET state='not_entitled' WHERE partition='2014'")
        conn.execute("UPDATE tasks SET state='failed',next_attempt_at_utc=? WHERE partition='2015'",
                     ((NOW + timedelta(seconds=1)).isoformat(),))
        conn.execute("UPDATE tasks SET state='complete',next_attempt_at_utc=? WHERE partition='2016'",
                     (NOW.isoformat(),))
        conn.execute("UPDATE tasks SET state='observed_empty',next_attempt_at_utc=? WHERE partition='2017'",
                     ((NOW + timedelta(seconds=1)).isoformat(),))
        assert worker._next_task(conn, NOW).partition == '2016'
        assert worker._next_task(conn, NOW + timedelta(seconds=1)).partition == '2015'


def test_delegated_priority_override_does_not_hide_next_allowed_override(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [
            ('TaiwanFuturesKBar', 'TX', '2026-10-02', 'id_day', 8),
            ('TaiwanStockKBar', '2330', '2026-10-01', 'id_day', 8),
            ('GoldPrice', '', '2000', 'year', 3),
            ('TaiwanStockPrice', '2330', 'history', 'id_history', 0),
        ])
        conn.execute('CREATE TABLE finmind_priority_tasks '
                     '(dataset TEXT,data_id TEXT,partition TEXT,PRIMARY KEY(dataset,data_id,partition))')
        conn.execute("INSERT INTO finmind_priority_tasks VALUES ('TaiwanFuturesKBar','TX','2026-10-02')")
        conn.execute("INSERT INTO finmind_priority_tasks VALUES ('TaiwanStockKBar','2330','2026-10-01')")
        assert worker._next_task(conn, NOW, delegated=frozenset(
            {'TaiwanFuturesKBar', 'TaiwanStockPrice'})).dataset == 'TaiwanStockKBar'


def test_dispatch_query_uses_partial_indexes_not_retained_success_scan(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [('GoldPrice', '', '2000', 'year', 3)])
        statements = []
        conn.set_trace_callback(statements.append)
        worker._next_task(conn, NOW)
        conn.set_trace_callback(None)
        query = next(row for row in statements if row.startswith('WITH eligible'))
        details = ' '.join(row[3] for row in conn.execute('EXPLAIN QUERY PLAN ' + query))
        assert 'idx_finmind_outstanding_dispatch' in details
        assert 'idx_finmind_due_refresh' in details
        # The small eligible CTE is scanned/sorted; both retained-table branches
        # must first SEARCH their partial indexes rather than scan all history.
        assert 'CO-ROUTINE eligible' in details
        assert details.count('SEARCH tasks USING INDEX') == 2


def test_last_result_is_preserved_across_idle_cycle_without_backdating(tmp_path, monkeypatch):
    last = {'dataset': 'GoldPrice', 'status': 'complete', 'rows': 3,
            'observed_at_utc': (NOW - timedelta(minutes=10)).isoformat()}
    write(tmp_path / 'status.json', {'last_task': last})
    monkeypatch.setattr(worker, '_now', lambda: NOW)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        status = worker._status(conn, tmp_path, state='current_queue')
        assert status['last_task'] == last
        assert status['observed_at_utc'] == NOW.isoformat()
        assert status['dispatch_contract_version'] == worker.DISPATCH_CONTRACT_VERSION


@pytest.mark.parametrize('grain,first,older,newer,before,after,backward,forward', [
    ('day', date(2026, 9, 1), '2026-09-01', '2026-09-30',
     datetime(2026, 10, 2, 7, 49, tzinfo=UTC), datetime(2026, 10, 2, 7, 50, tzinfo=UTC), 1, 2),
    ('month', date(2026, 8, 1), '2026-08-01', '2026-09-01',
     datetime(2026, 10, 1, 7, 49, tzinfo=UTC), datetime(2026, 10, 1, 7, 50, tzinfo=UTC), 1, 1),
])
def test_frontier_counts_unseeded_forward_only_after_release_without_double_count(
        tmp_path, monkeypatch, grain, first, older, newer, before, after, backward, forward):
    dataset = 'TaiwanStockKBar'
    spec = replace(supplemental.SOURCES[dataset], first=first, grain=grain)
    monkeypatch.setattr(supplemental, 'SOURCES', {dataset: spec})
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        conn.execute('CREATE TABLE finmind_source_frontiers '
                     '(dataset TEXT,data_id TEXT,older_than TEXT,newer_than TEXT,PRIMARY KEY(dataset,data_id))')
        conn.execute('INSERT INTO finmind_source_frontiers VALUES (?,?,?,?)', (dataset, '2330', older, newer))
        low = supplemental.frontier_status(conn, before)[dataset]
        high = supplemental.frontier_status(conn, after)[dataset]
        assert low['unseeded_forward_candidates'] == forward - 1
        assert high['unseeded_partition_candidates'] == backward + forward
        assert high['unseeded_history_candidates'] == backward
        conn.execute('UPDATE finmind_source_frontiers SET newer_than=?',
                     (after.date().replace(day=1).isoformat() if grain == 'month' else after.date().isoformat(),))
        assert supplemental.frontier_status(conn, after)[dataset]['unseeded_forward_candidates'] == 0


def test_workload_exposes_frontier_candidates_instead_of_calling_them_materialized(tmp_path, monkeypatch):
    dataset = 'TaiwanFuturesKBar'
    path = tmp_path / 'complement' / 'queue.sqlite3'
    with worker._db(path) as conn:
        worker._add_tasks(conn, [(dataset, 'TX', '2026-10-01', 'id_day', 8)])
        conn.execute('CREATE TABLE finmind_source_frontiers '
                     '(dataset TEXT,data_id TEXT,older_than TEXT,newer_than TEXT,PRIMARY KEY(dataset,data_id))')
        conn.execute('INSERT INTO finmind_source_frontiers VALUES (?,?,?,?)',
                     (dataset, 'TX', '2011-01-04', '2026-10-01'))
        conn.commit()
    monkeypatch.setattr(work, '_registry', lambda: {dataset: {
        'dataset': dataset, 'owners': ['complement'], 'primary_owner': 'complement',
        'owner_contracts': {'complement': {'query_shape': 'per_id_day'}},
    }})
    result = work.build_finmind_workload(tmp_path, NOW)
    row = result['datasets'][0]
    assert row['candidate_requests'] == 3  # two old dates plus one unseeded new date
    assert row['current_plan_requests'] == 4


def test_reusable_reservation_plan_rejects_wrong_observation_and_counts(tmp_path, monkeypatch):
    details = {'ready_requests': 0, 'reserve_requests': 7, 'queue_errors': [], 'observed_at_utc': NOW.isoformat()}
    verified = {'observed_at_utc': NOW.isoformat(), 'official_requests_per_hour': 6000, 'provider_used_in_hour': 20}
    monkeypatch.setattr('downloader.finmind_scheduling.incremental_reservation',
                        lambda *_a: pytest.fail('must reuse this exact plan'))
    assert account.backfill_budget(verified, tmp_path, fixed_incremental_requests=7, now=NOW,
                                  prioritize_due=True, reservation_plan=details)['allowed']
    for wrong in ({**details, 'observed_at_utc': (NOW - timedelta(seconds=1)).isoformat()},
                  {**details, 'reserve_requests': 6}):
        output = account.backfill_budget(verified, tmp_path, fixed_incremental_requests=7, now=NOW,
                                        prioritize_due=True, reservation_plan=wrong)
        assert not output['allowed'] and not output['schedule_verified']


def test_minute_dispatch_receipt_survives_eta_failure_and_has_no_extra_calls(tmp_path, monkeypatch):
    plan = {'observed_at_utc': NOW.isoformat(), 'ready_requests': 0, 'reserve_requests': 0, 'queue_errors': []}
    monkeypatch.setattr(sampler, 'incremental_reservation', lambda *_a: plan)
    verified = {'observed_at_utc': NOW.isoformat(), 'official_requests_per_hour': 6000, 'provider_used_in_hour': 20}
    receipt = sampler.sample_local_dispatch(tmp_path, verified, now=NOW)
    assert receipt['allocation']['allowed'] and receipt['extra_provider_calls'] == 0
    monkeypatch.setattr(sampler, 'snapshot_finmind_estimate', lambda *_a: (_ for _ in ()).throw(ValueError('error')))
    assert sampler.sample_local_eta(tmp_path) is None
    assert json.loads((tmp_path / 'dispatch_status.json').read_text()) == receipt


def test_dashboard_aggregates_workers_and_latest_result_and_separates_candidates(tmp_path):
    root = tmp_path / 'data_finmind'
    write(root / 'status.json', {'state': 'current', 'observed_at_utc': NOW.isoformat(),
                               'total_session_day_tasks': 2, 'complete_session_day_tasks': 2})
    write(root / 'complement/status.json', {
        'state': 'running', 'observed_at_utc': NOW.isoformat(),
        'series': {'TaiwanFuturesKBar': {'target': 10, 'complete': 1, 'observed_empty': 0}},
        'historical_frontiers': {'TaiwanFuturesKBar': {'unseeded_partition_candidates': 100}},
        'last_task': {'dataset': 'TaiwanFuturesKBar', 'data_id': 'TX', 'status': 'complete',
                      'rows': 270, 'observed_at_utc': NOW.isoformat(), 'token': 'NEVER_EXPORT'},
    })
    write(root / 'sponsor/status.json', {
        'state': 'current', 'observed_at_utc': NOW.isoformat(),
        'last_task': {'dataset': 'TaiwanStockPrice', 'status': 'complete',
                      'observed_at_utc': (NOW - timedelta(minutes=2)).isoformat()},
    })
    result = build_finmind_public_status(tmp_path, now=NOW)
    info = result['acquisition']
    assert info['free_state'] == 'current' and info['state'] == 'running'
    assert info['latest_result']['dataset'] == 'TaiwanFuturesKBar'
    assert info['unseeded_candidate_tasks'] == 100
    assert info['materialized_tasks'] + info['unseeded_candidate_tasks'] == info['total_tasks']
    assert 'NEVER_EXPORT' not in json.dumps(result)


def test_dashboard_uses_fresh_minute_dispatch_without_live_recalculation_then_expires(tmp_path, monkeypatch):
    root = tmp_path / 'data_finmind'
    write(root / 'account_status.json', {'observed_at_utc': NOW.isoformat(), 'tier': 'Sponsor',
                                       'official_requests_per_hour': 6000, 'provider_used_in_hour': 100})
    write(root / 'dispatch_status.json', {
        'schema_version': 1, 'observed_at_utc': NOW.isoformat(), 'official_requests_per_hour': 6000,
        'allocation': {'basis': 'provider_observation_plus_local_starts', 'allowed': True,
                       'remaining': 5800, 'reserve': 2, 'ready_incremental_requests': 0,
                       'schedule_verified': True, 'secret': 'NEVER_EXPORT'},
    })
    monkeypatch.setattr('stockagent.live.finmind_dashboard.backfill_budget',
                        lambda *_a, **_kw: pytest.fail('gateway must use minute receipt'))
    fresh = build_finmind_public_status(tmp_path, now=NOW)['quota']['backfill_allocation']
    assert fresh['remaining'] == 5800 and fresh['snapshot_state'] == 'observed'
    assert 'NEVER_EXPORT' not in json.dumps(fresh)
    stale = build_finmind_public_status(tmp_path, now=NOW + timedelta(seconds=181))['quota']['backfill_allocation']
    assert stale['snapshot_state'] == 'stale' and not stale['allowed']
    assert 'remaining' not in stale


def test_minute_traffic_skips_gateway_wal_and_expires_without_losing_official_limit(tmp_path, monkeypatch):
    root = tmp_path / 'data_finmind'
    write(root / 'account_status.json', {'observed_at_utc': NOW.isoformat(), 'tier': 'Sponsor',
                                       'official_requests_per_hour': 6000, 'provider_used_in_hour': 100})
    write(root / 'dispatch_status.json', {
        'schema_version': 2, 'observed_at_utc': NOW.isoformat(), 'official_requests_per_hour': 6000,
        'allocation': {'basis': 'provider_observation_plus_local_starts', 'allowed': True,
                       'remaining': 5800, 'reserve': 2},
        'traffic': {'state': 'complete_worker_window', 'observed_requests_60m': 5700,
                    'worker_headroom_60m': 300, 'secret': 'NEVER_EXPORT',
                    'history': [{'at_utc': NOW.isoformat(), 'observed_requests_60m': 5700, 'secret': 'NEVER_EXPORT'},
                                {'at_utc': (NOW + timedelta(seconds=1)).isoformat(), 'observed_requests_60m': 1}]},
    })
    monkeypatch.setattr('stockagent.live.finmind_dashboard._traffic',
                        lambda *_a: pytest.fail('no active WAL query on public path'))
    quota = build_finmind_public_status(tmp_path, now=NOW)['quota']
    assert quota['observed_requests_60m'] == 5700 and quota['worker_headroom_60m'] == 300
    assert len(quota['history']) == 1 and 'NEVER_EXPORT' not in json.dumps(quota)
    assert quota['observed_at_utc'] == quota['backfill_allocation']['observed_at_utc']
    stale = build_finmind_public_status(tmp_path, now=NOW + timedelta(seconds=181))['quota']
    assert stale['state'] == 'stale' and stale['observed_requests_60m'] is None
    assert stale['official_requests_per_hour'] == 6000


def test_unreadable_local_traffic_cannot_hide_independent_verified_official_limit(tmp_path, monkeypatch):
    from stockagent.live import finmind_dashboard as dashboard
    import sqlite3
    (tmp_path / 'request_traffic.sqlite3').touch()
    def unavailable(*_a, **_kw):
        raise sqlite3.OperationalError('gateway cannot open live WAL')
    monkeypatch.setattr(dashboard.sqlite3, 'connect', unavailable)
    quota = dashboard._traffic(tmp_path, NOW, 6000)
    assert quota['state'] == 'unavailable' and quota['official_requests_per_hour'] == 6000
    assert quota['observed_requests_60m'] is None

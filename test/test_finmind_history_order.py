"""The user's US-last sequence governs lazy frontiers, dispatch and ETA."""
from datetime import timedelta
import json

import pytest

from downloader import download_finmind_complement as worker
from downloader import finmind_supplemental as supplemental
from downloader.finmind_history_order import HISTORY_STAGES, STAGES, first_unfinished_dataset, metadata
from downloader.finmind_eta_stages import ordered_estimate, stage_workloads
from downloader.finmind_eta import SNAPSHOT_CONTRACT_VERSION
from stockagent.live.finmind_eta_projection import public_completion_estimate
from test_finmind_eta import NOW, evidence


EXPECTED = ('TaiwanStockKBar', 'TaiwanFuturesKBar', 'TaiwanStockTradingDailyReport',
            'TaiwanStockWarrantTradingDailyReport', 'TaiwanStockPriceTick', 'TaiwanFuturesTick',
            'TaiwanOptionTick', 'TaiwanFuturesSpreadTick', 'USStockPriceMinute')


def test_all_nine_in_reverse_insertion_dispatch_exact_sequence_and_preemption(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [(stage.dataset, '' if stage.dataset == 'TaiwanFuturesSpreadTick' else 'ID',
                                 '2026-09-25', 'id_day', supplemental.SOURCES[stage.dataset].priority)
                                for stage in reversed(HISTORY_STAGES)])
        choices = []
        for expected in EXPECTED:
            task = worker._next_task(conn, NOW, advance_cursor=True)
            assert task.dataset == expected
            # A due refresh can preempt even the four tick stages; it does not
            # change the retained cursor or complete an unfinished history.
            worker._add_tasks(conn, [('TaiwanStockInfo', '', 'latest', 'snapshot', 0)])
            conn.execute("UPDATE tasks SET state='pending',next_attempt_at_utc=NULL WHERE priority=0")
            assert worker._next_task(conn, NOW).dataset == 'TaiwanStockInfo'
            conn.execute("UPDATE tasks SET state='complete',next_attempt_at_utc=NULL WHERE priority=0")
            assert worker._next_task(conn, NOW).dataset == expected
            choices.append(task.dataset)
            conn.execute("UPDATE tasks SET state='complete' WHERE dataset=?", (expected,))
        assert tuple(choices) == EXPECTED
        assert worker._next_task(conn, NOW) is None


@pytest.mark.parametrize('direction', ['old', 'forward'])
def test_drained_working_set_does_not_skip_an_unseeded_frontier(tmp_path, direction):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [('TaiwanFuturesKBar', 'TX', '2026-09-25', 'id_day', 8)])
        conn.execute('CREATE TABLE finmind_source_frontiers ('
                     'dataset TEXT,data_id TEXT,older_than TEXT,newer_than TEXT,'
                     'PRIMARY KEY(dataset,data_id))')
        anchor = supplemental._eligible_anchor(supplemental.SOURCES['TaiwanStockKBar'], NOW)
        conn.execute('INSERT INTO finmind_source_frontiers VALUES (?,?,?,?)',
                     ('TaiwanStockKBar', '2330', '2026-09-25' if direction == 'old' else None,
                      (anchor - timedelta(days=int(direction == 'forward'))).isoformat()))
        assert first_unfinished_dataset(conn, NOW) == 'TaiwanStockKBar'
        assert worker._next_task(conn, NOW) is None
        # Bounded, explicitly selected work still honors its scope.
        assert worker._next_task(conn, NOW, datasets=('TaiwanFuturesKBar',)).dataset == 'TaiwanFuturesKBar'
        worker._add_tasks(conn, [('TaiwanStockInfo', '', 'latest', 'snapshot', 0)])
        assert worker._next_task(conn, NOW).dataset == 'TaiwanStockInfo'


def test_cooling_failure_keeps_later_stage_waiting_without_early_retry(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [('TaiwanStockKBar', '2330', '2026-09-25', 'id_day', 8),
                                 ('TaiwanFuturesKBar', 'TX', '2026-09-25', 'id_day', 8)])
        conn.execute("UPDATE tasks SET state='failed',next_attempt_at_utc=? WHERE dataset='TaiwanStockKBar'",
                     ((NOW + timedelta(seconds=60)).isoformat(),))
        assert worker._next_task(conn, NOW) is None
        assert worker._next_task(conn, NOW + timedelta(seconds=60)).dataset == 'TaiwanStockKBar'
        assert worker._next_task(conn, NOW, delegated=frozenset({'TaiwanStockKBar'})).dataset == 'TaiwanFuturesKBar'


def test_prior_completed_stage_maintenance_defers_to_missing_later_history(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [('TaiwanStockKBar', '2330', '2026-09-25', 'id_day', 8),
                                 ('TaiwanFuturesTick', 'TX', '2026-09-25', 'id_day', 10),
                                 ('USStockPriceMinute', 'AAPL', '2026-09-25', 'id_day', 8)])
        conn.execute("UPDATE tasks SET state='complete',next_attempt_at_utc=? WHERE dataset='TaiwanStockKBar'",
                     (NOW.isoformat(),))
        assert first_unfinished_dataset(conn, NOW) == 'TaiwanFuturesTick'
        assert worker._next_task(conn, NOW).dataset == 'TaiwanFuturesTick'
        # A due incremental head, unlike old partition maintenance, is still
        # the highest priority and must interrupt the missing history.
        conn.execute("UPDATE tasks SET priority=0 WHERE dataset='TaiwanStockKBar'")
        assert worker._next_task(conn, NOW).dataset == 'TaiwanStockKBar'


def nine_workload():
    rows = [dict(dataset=stage.dataset, state='observed', current_plan_requests=i * 100,
                 required_requests=i * 100, backfill_requests=i * 100, validation_requests=0,
                 unbatched_requests=i * 100, fastest_requests=i * 100, candidate_requests=i * 100)
            for i, stage in enumerate(HISTORY_STAGES, 1)]
    return {'state': 'observed', 'datasets': list(reversed(rows)), 'summary': dict(
        current_plan_requests=4500, unbatched_requests=4500, required_requests=4500,
        candidate_requests=4500, validation_requests=0, unknown_datasets=0)}


def test_disjoint_nine_stage_counts_and_us_last_arrivals_share_one_capacity(tmp_path):
    workload = nine_workload()
    _, telemetry = evidence()
    telemetry['quota']['recurring_forecast'] = dict(
        requests_per_hour_by_stage={'incremental': 100, **{stage.key: 10 for stage in HISTORY_STAGES}},
        new_partition_requests_per_hour_by_stage={'us_stock_minute': 5})
    result = ordered_estimate(workload, telemetry, NOW, day_is_protected=lambda _: False,
                              secondary_admission={'allowed': True})
    assert result['schema_version'] == SNAPSHOT_CONTRACT_VERSION
    assert tuple(item['dataset'] for item in result['stages'] if item.get('dataset')) == EXPECTED
    assert sum(item['workload']['planned_requests'] for item in result['stages']) == 4500
    history = result['stages'][2:-1]
    assert [item['workload']['candidate_requests'] for item in history] == list(range(100, 1000, 100))
    assert [item['rate_evidence']['future_recurring_requests_per_hour'] for item in history] == list(range(110, 200, 10))
    for scenario in ('fastest', 'central', 'slowest'):
        previous = NOW.isoformat()
        for item in history:
            row = item['scenarios'][scenario]
            assert row['stage_start_at_utc'] >= previous
            previous = row['estimated_complete_at_utc']
        assert history[-1]['scenarios'][scenario]['forecast_arrival_requests'] > 0
        assert result['scenarios'][scenario]['request_count'] == sum(
            item['scenarios'][scenario]['request_count'] for item in result['stages'])
    (tmp_path / 'eta_status.json').write_text(json.dumps({'schema_version': 1, 'estimate': result}))
    public = public_completion_estimate(tmp_path, NOW)
    assert [item['history_rank'] for item in public['stages'][2:-1]] == list(range(1, 10))
    assert tuple(item['dataset'] for item in public['stages'][2:-1]) == EXPECTED
    assert metadata()['stages'][-1]['dataset'] == 'USStockPriceMinute'


def test_legacy_coarse_arrivals_are_not_fabricated_per_dataset():
    _, telemetry = evidence()
    telemetry['quota']['recurring_forecast'] = dict(requests_per_hour_by_phase={'detail': 100},
        new_partition_requests_per_hour_by_phase={'detail': 50})
    result = ordered_estimate(nine_workload(), telemetry, NOW, day_is_protected=lambda _: False,
                              secondary_admission={'allowed': True})
    assert result['state'] == 'warming_up'
    assert all(item['scenarios']['central']['estimated_complete_at_utc'] is None
               for item in result['stages'][2:])


def test_source_request_and_receipt_contracts_are_not_changed_by_rank():
    assert tuple(stage.dataset for stage in HISTORY_STAGES) == EXPECTED
    assert [key for key, _ in STAGES][2:-1] == [stage.key for stage in HISTORY_STAGES]
    assert supplemental.SOURCES['USStockPriceMinute'].priority == 8
    assert supplemental.SOURCES['TaiwanFuturesSpreadTick'].priority == 10
    assert stage_workloads(nine_workload())[-1]['cumulative_planned_requests'] == 4500


def test_changed_sequence_invalidates_old_deadlines_instead_of_reordering_them(tmp_path):
    _, telemetry = evidence()
    telemetry['quota']['recurring_forecast'] = dict(requests_per_hour_by_stage={'incremental': 100})
    result = ordered_estimate(nine_workload(), telemetry, NOW, day_is_protected=lambda _: False,
                              secondary_admission={'allowed': True})
    result['history_order']['stages'] = list(reversed(result['history_order']['stages']))
    (tmp_path / 'eta_status.json').write_text(json.dumps({'schema_version': 1, 'estimate': result}))
    public = public_completion_estimate(tmp_path, NOW)
    assert public['state'] == 'unavailable'
    assert 'stages' not in public
    assert all(item['estimated_complete_at_utc'] is None for item in public['scenarios'].values())

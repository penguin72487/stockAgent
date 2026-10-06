"""Actual debt stays actual; closed-day and duplicate calls carry proof."""
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
import json
from types import SimpleNamespace

import pytest

from downloader import download_finmind_complement as worker
from downloader import finmind_eta_telemetry as telemetry
from downloader import finmind_supplemental as supplemental
from downloader.download_finmind_sponsor import OfficialSessions
from downloader.finmind_eta_stages import ordered_estimate
from downloader.finmind_history_calendar import arrival_density, load_closures
from downloader.finmind_scheduling import next_release_check
from stockagent.live.finmind_eta_projection import public_completion_estimate
from test_finmind_eta_stages import inputs

NOW = datetime(2026, 10, 3, 14, tzinfo=UTC)  # Saturday 22:00 Taipei


def test_holiday_release_skips_only_cash_and_keeps_unknown_and_derivatives():
    before = datetime(2026, 10, 8, 12, tzinfo=UTC)

    def closed(day, **kwargs):
        return SimpleNamespace(status='closed' if day == date(2026, 10, 9) else 'scheduled_open',
                               reason='official TWSE schedule as-of 2026-10-03')

    assert next_release_check('TaiwanStockPrice', before, day_decision=closed) == datetime(2026, 10, 12, 9, 30, tzinfo=UTC)
    assert next_release_check('TaiwanFuturesDaily', before, day_decision=closed) == datetime(2026, 10, 9, 8, 30, tzinfo=UTC)
    unknown = lambda *_a, **_k: SimpleNamespace(status='unknown', reason='missing')
    assert next_release_check('TaiwanStockPrice', before, day_decision=unknown) == datetime(2026, 10, 9, 9, 30, tzinfo=UTC)


def test_tail_closure_is_compact_reversible_and_does_not_apply_to_night_data(tmp_path, monkeypatch):
    cash, futures = 'TaiwanStockPriceTick', 'TaiwanFuturesTick'
    specs = {name: replace(supplemental.SOURCES[name], first=date(2026, 10, 2)) for name in (cash, futures)}
    monkeypatch.setattr(supplemental, 'SOURCES', specs)
    proof = OfficialSessions(date(2026, 10, 2), date(2026, 10, 2), frozenset({date(2026, 10, 2)}), 'a' * 64)
    closed = lambda day: SimpleNamespace(status='closed', reason='official TWSE schedule: weekend')
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        supplemental.seed(conn, {'stocks': ['2330', '2317'], 'futures': ['TX']}, NOW,
                          official_sessions=proof, day_decision=closed)
        assert load_closures(conn).days == ('2026-10-03',)
        assert conn.execute('SELECT count(*) FROM tasks WHERE dataset=? AND partition=?',
                            (cash, '2026-10-03')).fetchone()[0] == 0
        assert conn.execute('SELECT count(*) FROM tasks WHERE dataset=? AND partition=?',
                            (futures, '2026-10-03')).fetchone()[0] == 0  # Not published yet.
        assert futures not in load_closures(conn).datasets
        assert supplemental.frontier_status(conn, NOW)[cash]['unseeded_partition_candidates'] == 0
        unknown = lambda day: SimpleNamespace(status='unknown', reason='unverified')
        supplemental.seed(conn, {'stocks': ['2330', '2317'], 'futures': ['TX']}, NOW,
                          official_sessions=proof, day_decision=unknown)
        assert load_closures(conn).days == ()
        assert conn.execute('SELECT count(*) FROM tasks WHERE dataset=? AND partition=? AND state=?',
                            (cash, '2026-10-03', 'pending')).fetchone()[0] == 2
        supplemental.seed(conn, {'stocks': ['2330', '2317'], 'futures': ['TX']}, NOW + timedelta(hours=8),
                          official_sessions=proof, day_decision=unknown)
        # Once that physical date is eligible, the cash closure cannot prune
        # Saturday's night-session ticks or claim there were no trades.
        assert conn.execute('SELECT count(*) FROM tasks WHERE dataset=? AND partition=?',
                            (futures, '2026-10-03')).fetchone()[0] == 1


def test_old_recheck_cannot_steal_missing_partition_priority(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [('GoldPrice', '', '2000', 'year', 1),
                                 ('TaiwanStockKBar', '2330', '2026-10-02', 'id_day', 8)])
        conn.execute("UPDATE tasks SET state='complete',next_attempt_at_utc=? WHERE dataset='GoldPrice'", (NOW.isoformat(),))
        assert worker._next_task(conn, NOW).dataset == 'TaiwanStockKBar'
        conn.execute("UPDATE tasks SET state='complete' WHERE dataset='TaiwanStockKBar'")
        assert worker._next_task(conn, NOW).dataset == 'GoldPrice'
        conn.execute("UPDATE tasks SET state='failed' WHERE dataset='GoldPrice'")
        assert worker._next_task(conn, NOW).dataset == 'GoldPrice'


def test_retry_projection_preserves_debt_and_unknown_admission(tmp_path):
    work, obs = inputs()
    work['datasets'][0].update(retry_tasks=1, retry_tasks_by_class={'backfill': 1}, max_retry_wait_seconds=900)
    work['summary'].update(retry_tasks=1, max_retry_wait_seconds=900)
    original = deepcopy(work)
    estimate = ordered_estimate(work, obs, NOW, day_is_protected=lambda day: False, secondary_admission={'allowed': True})
    assert work == original
    assert estimate['state'] == 'waiting_retry'
    assert estimate['scenarios']['central']['estimated_complete_at_utc'] is None
    projection = estimate['retry_condition']
    assert projection['retry_tasks'] == 1 and not projection['is_guaranteed']
    assert projection['scenarios']['central']['base_request_count'] == estimate['workload']['planned_requests']
    assert datetime.fromisoformat(projection['scenarios']['central']['estimated_complete_at_utc']) > NOW + timedelta(seconds=900)
    (tmp_path / 'eta_status.json').write_text(json.dumps({'schema_version': 1, 'estimate': estimate}))
    public = public_completion_estimate(tmp_path, NOW)
    assert public['workload']['retry_tasks'] == 1 and public['state'] == 'waiting_retry'
    assert public['retry_condition']['scenarios']['central']['state'] == 'estimated'
    assert len(public['stages']) == 12
    assert 'retry_condition' not in public_completion_estimate(tmp_path, NOW + timedelta(minutes=6))
    rejected = ordered_estimate(work, obs, NOW, day_is_protected=lambda day: False,
                                secondary_admission={'allowed': False, 'reason': 'unknown'})
    assert rejected['retry_condition']['scenarios']['central']['estimated_complete_at_utc'] is None


def test_arrival_density_is_a_cash_only_model_not_a_future_calendar():
    from downloader.finmind_history_calendar import HistoryClosures
    start, end = date(2025, 10, 3), date(2026, 10, 2)
    days = tuple((start + timedelta(days=i)).isoformat() for i in range(365)
                 if (start + timedelta(days=i)).weekday() >= 5)
    proof = HistoryClosures(days, frozenset({'TaiwanStockKBar'}), 'a' * 64, start.isoformat(), end.isoformat())
    observed = arrival_density(proof, 'TaiwanStockKBar', NOW)
    assert observed['factor'] == pytest.approx((365 - len(days)) / 365)
    assert observed['state'] == 'modeled_from_verified_cash_year'
    assert arrival_density(proof, 'TaiwanFuturesKBar', NOW)['factor'] == 1
    assert arrival_density(proof, 'TaiwanStockKBar', NOW + timedelta(days=33))['factor'] == 1


def test_telemetry_reuses_one_reservation_snapshot(tmp_path, monkeypatch):
    calls = []
    plan = {'observed_at_utc': NOW.isoformat(), 'reserve_requests': 7, 'ready_requests': 3, 'queue_errors': []}
    (tmp_path / 'account_status.json').write_text(json.dumps({
        'observed_at_utc': NOW.isoformat(), 'official_requests_per_hour': 6000, 'provider_used_in_hour': 10}))

    def reserve(*args):
        calls.append(args)
        return plan

    monkeypatch.setattr(telemetry, 'incremental_reservation', reserve)
    result = telemetry.build_finmind_eta_telemetry(tmp_path, NOW)
    assert len(calls) == 1
    quota = result['quota']
    assert quota['reservation_plan'] == plan
    assert quota['reserved_requests_per_hour'] == quota['current_budget']['reserve'] == 9
    assert quota['current_budget']['priority_wait'] and quota['current_budget']['schedule_verified']

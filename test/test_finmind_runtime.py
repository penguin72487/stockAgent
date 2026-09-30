from datetime import UTC, datetime, timedelta
import json

import pytest

from downloader.download_finmind_complement import _db, _sponsor_delegated
from downloader.finmind_runtime import idle_heartbeat, next_cycle_delay, wait_for_next_cycle
from downloader.finmind_scheduling import _s


NOW = datetime(2026, 9, 28, 9, 59, 40, tzinfo=UTC)


def write_idle(root, status, *, age=0, until=None, **extra):
    root.mkdir(parents=True, exist_ok=True)
    (root / 'worker_status.json').write_text(json.dumps({
        'schema_version': 1, 'phase': 'waiting', 'reason': status['state'],
        'cycle_observed_at_utc': status['observed_at_utc'],
        'observed_at_utc': (NOW - timedelta(seconds=age)).isoformat(),
        'next_check_at_utc': (until or NOW + timedelta(minutes=10)).isoformat(), **extra,
    }))


@pytest.mark.parametrize('extra', [{}, {'age': 91}, {'phase': 'running'},
                                  {'reason': 'mismatched'}, {'cycle_observed_at_utc': None},
                                  {'until': NOW - timedelta(minutes=1)}, {'age': -60}])
def test_heartbeat_is_fresh_bounded_and_tied_to_exact_cycle(tmp_path, extra):
    status = {'state': 'current_queue', 'observed_at_utc': (NOW - timedelta(minutes=30)).isoformat()}
    write_idle(tmp_path, status, **extra)
    assert idle_heartbeat(tmp_path, status, NOW)['alive'] is (not extra)


@pytest.mark.parametrize('compact_result', [False, True])
def test_wait_heartbeats_without_touching_acquisition_status_or_hitting_api(tmp_path, compact_result):
    status = {'state': 'current_queue', 'observed_at_utc': NOW.isoformat()}
    original = json.dumps(status)
    (tmp_path / 'status.json').write_text(original)
    ticks = [0.0]
    observed = []
    def sleep(seconds):
        observed.append(json.loads((tmp_path / 'worker_status.json').read_text()))
        ticks[0] += seconds
    wait_for_next_cycle(tmp_path, {'state': status['state']} if compact_result else status, 95,
                        clock=lambda: NOW + timedelta(seconds=ticks[0]),
                        sleep=sleep, monotonic=lambda: ticks[0])
    assert ticks[0] == 95
    assert len(observed) == 4
    assert len({row['next_check_at_utc'] for row in observed}) == 1
    assert all(row['cycle_observed_at_utc'] == status['observed_at_utc'] for row in observed)
    assert (tmp_path / 'status.json').read_text() == original
    assert json.loads((tmp_path / 'worker_status.json').read_text())['phase'] == 'running'


def test_wait_wakes_when_admission_changes_without_waiting_for_ten_minute_poll(tmp_path):
    ticks = [0.0]
    wait_for_next_cycle(tmp_path, {'state': 'waiting_necessary_acquisition', 'observed_at_utc': NOW.isoformat()},
                        600, clock=lambda: NOW + timedelta(seconds=ticks[0]),
                        sleep=lambda seconds: ticks.__setitem__(0, ticks[0] + seconds),
                        monotonic=lambda: ticks[0], wake_if=lambda: ticks[0] >= 30)
    assert ticks[0] == 30


def test_new_primary_retry_wakes_even_when_secondary_gate_says_no(tmp_path):
    from downloader.download_finmind_sponsor import _ready_after_wait
    with _db(tmp_path / 'queue.sqlite3') as conn:
        conn.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,next_attempt_at_utc) "
                     "VALUES ('TaiwanStockWeekPrice','','2026-09-28','day',0,'observed_empty',?)",
                     ((NOW + timedelta(seconds=1)).isoformat(),))
    kwargs = {'secondary_admission': lambda: {'allowed': False}}
    assert not _ready_after_wait(tmp_path, NOW, **kwargs)
    assert _ready_after_wait(tmp_path, NOW + timedelta(seconds=1), **kwargs)


def test_sleeping_secondary_or_blocked_primary_never_triggers_busy_restart(tmp_path):
    from downloader.download_finmind_sponsor import _ready_after_wait
    with _db(tmp_path / 'queue.sqlite3') as conn:
        conn.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,next_attempt_at_utc) "
                     "VALUES ('TaiwanStockPrice','','2026-09-28','day',8,'pending',?)",
                     ((NOW + timedelta(seconds=1)).isoformat(),))
    kwargs = {'secondary_admission': lambda: {'allowed': True}}
    assert not _ready_after_wait(tmp_path, NOW, **kwargs)
    with _db(tmp_path / 'queue.sqlite3') as conn:
        conn.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
                     "VALUES ('TaiwanStockMarginPurchaseShortSale','','2026-09-28','day',0,'blocked')")
    assert not _ready_after_wait(tmp_path, NOW + timedelta(seconds=1), **kwargs)


def test_retry_and_release_deadlines_wake_at_the_actual_boundary(tmp_path):
    assert next_cycle_delay(tmp_path, NOW, 3600, sources=[_s('Test', '2020-01-01', 'day', 1, 18)]) == 20
    with _db(tmp_path / 'queue.sqlite3') as conn:
        conn.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,next_attempt_at_utc) "
                     "VALUES ('Test','','latest','snapshot',1,'pending',?)",
                     ((NOW + timedelta(seconds=10)).isoformat(),))
    assert next_cycle_delay(tmp_path, NOW, 3600) == 10


def test_idle_sponsor_does_not_handoff_to_duplicate_symbol_queries(tmp_path):
    root = tmp_path / 'sponsor'
    status = {'state': 'current_queue', 'tier': 'Sponsor',
              'observed_at_utc': (NOW - timedelta(minutes=30)).isoformat(),
              'series': {'TaiwanStockPrice': {'target': 2, 'blocked': 0}}}
    write_idle(root, status)
    (root / 'status.json').write_text(json.dumps(status))
    assert 'TaiwanStockPrice' in _sponsor_delegated(tmp_path / 'complement', NOW)
    assert not _sponsor_delegated(tmp_path / 'complement', NOW + timedelta(seconds=91))


def test_free_wakes_at_calendar_publication_even_when_hourly_cache_is_fresh(tmp_path):
    (tmp_path / 'calendar.json').write_text(json.dumps({
        'observed_at_utc': (NOW - timedelta(minutes=10)).isoformat(), 'dates': ['2026-09-28']}))
    assert next_cycle_delay(tmp_path, NOW, 3600) == 20


def test_priority_wait_releases_within_five_seconds_without_an_api_probe(tmp_path, monkeypatch):
    from downloader import finmind_runtime as runtime
    ticks = [0.0]
    monkeypatch.setattr(runtime, '_backfill_ready', lambda *_a: ticks[0] >= 5)
    wait_for_next_cycle(tmp_path, {'state': 'incremental_reserve', 'observed_at_utc': NOW.isoformat()},
                        60, clock=lambda: NOW + timedelta(seconds=ticks[0]),
                        sleep=lambda seconds: ticks.__setitem__(0, ticks[0] + seconds),
                        monotonic=lambda: ticks[0])
    assert ticks[0] == 5


def test_dashboard_distinguishes_idle_liveness_from_old_data_observation(tmp_path):
    from stockagent.live.finmind_dashboard import build_finmind_public_status
    root = tmp_path / 'data_finmind'
    status = {'state': 'current', 'observed_at_utc': (NOW - timedelta(minutes=30)).isoformat()}
    write_idle(root, status)
    (root / 'status.json').write_text(json.dumps(status))
    result = build_finmind_public_status(tmp_path, now=NOW)
    assert result['health'] == 'waiting'
    assert result['status_age_seconds'] == 1800
    assert result['acquisition']['workers']['free']['alive']
    assert build_finmind_public_status(tmp_path, now=NOW + timedelta(seconds=91))['health'] == 'stale'


@pytest.mark.parametrize('age,state,alive', [(0, 'running', True), (180, 'backfilling', True),
                                          (181, 'running', False), (-60, 'running', False),
                                          (0, 'current', False)])
def test_worker_liveness_uses_fresh_active_cycle_without_an_idle_lease(tmp_path, age, state, alive):
    from stockagent.live.finmind_dashboard import _worker_liveness
    stamp = (NOW - timedelta(seconds=age)).isoformat()
    status = {'state': state, 'observed_at_utc': stamp}
    worker = _worker_liveness(tmp_path, status, NOW)
    assert worker['alive'] is alive
    assert worker['next_check_at_utc'] is None
    if alive:
        assert worker['state'] == 'running'
        assert worker['observed_at_utc'] == stamp
    assert status == {'state': state, 'observed_at_utc': stamp}

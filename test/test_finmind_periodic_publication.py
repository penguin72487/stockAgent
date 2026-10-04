"""Provider publishing days are distinct from cash/derivative trading days."""
from contextlib import closing
from datetime import UTC, datetime, timedelta

import pytest

from downloader import download_finmind_complement as worker
from downloader import finmind_eta_telemetry as telemetry
from downloader.finmind_scheduling import incremental_reservation, next_release_check, release_details
from scripts import audit_finmind_query_ranges


DATASET = 'TaiwanFuturesFinalSettlementPrice'
SUNDAY = datetime(2026, 10, 3, 17, tzinfo=UTC)  # Oct 4 01:00 Taipei.
MONDAY = datetime(2026, 10, 4, 16, tzinfo=UTC)


@pytest.mark.parametrize('dataset', tuple(worker.PRODUCT_HISTORY_STARTS))
@pytest.mark.parametrize('stamp,expected', [
    ('2026-10-01T20:00:00+00:00', '2026-10-01T23:00:00+00:00'),  # Fri 04 -> 07.
    ('2026-10-02T14:00:00+00:00', '2026-10-04T16:00:00+00:00'),  # Fri 22 -> Mon 00.
    ('2026-10-03T17:00:00+00:00', '2026-10-04T16:00:00+00:00'),  # Sunday.
    ('2026-10-04T16:00:00+00:00', '2026-10-04T19:00:00+00:00'),  # Mon 00 -> 03.
])
def test_successful_renewal_uses_provider_weekdays_not_cash_calendar(dataset, stamp, expected):
    now = datetime.fromisoformat(stamp)
    task = worker.Task(dataset, 'TX', 'history', 'id_history', 0, 'complete')
    assert worker._next_refresh(task, now, empty=False) == expected
    assert next_release_check(dataset, now, day_decision=lambda *a, **k: pytest.fail('not cash sessions')) == datetime.fromisoformat(expected)
    assert release_details(dataset)['weekdays'] == list(range(5))
    assert release_details(dataset)['basis'] == 'official_interval_weekdays_not_actual_publication'


def _setup(root, rows):
    with closing(worker._db(root / 'complement/queue.sqlite3')) as conn, conn:
        worker._add_tasks(conn, [(DATASET, identifier, 'history', 'id_history', priority)
                                 for identifier, state, priority, due in rows])
        for identifier, state, priority, due in rows:
            conn.execute('UPDATE tasks SET state=?,rows=4,bytes=42,receipt_path=?,last_attempt_at_utc=?, '
                         'next_attempt_at_utc=? WHERE data_id=?',
                         (state, identifier + '.json', SUNDAY.isoformat(), due, identifier))


def test_existing_deadline_migration_keeps_all_receipt_evidence_and_retry_clocks(tmp_path):
    due = (SUNDAY + timedelta(hours=3)).isoformat()
    rows = [('TX', 'complete', 0, due), ('failed', 'failed', 0, due),
            ('pending', 'pending', 0, due), ('empty', 'observed_empty', 3, due)]
    _setup(tmp_path, rows)
    with closing(worker._db(tmp_path / 'complement/queue.sqlite3')) as conn, conn:
        before = conn.execute('SELECT * FROM tasks ORDER BY data_id').fetchall()
        assert worker._reconcile_periodic_publication(conn, SUNDAY) == 1
        assert worker._reconcile_periodic_publication(conn, SUNDAY) == 0
        after = conn.execute('SELECT * FROM tasks ORDER BY data_id').fetchall()
        for a, b in zip(before, after):
            assert a[:6] + a[7:] == b[:6] + b[7:]
            assert b[6] == (MONDAY.isoformat() if a[1] == 'TX' else a[6])
        assert conn.execute('SELECT prior_deadline,new_deadline FROM finmind_periodic_clock_migrations').fetchall() == [(due, MONDAY.isoformat())]


def test_missed_weekday_renewal_stays_due_and_unknown_clocks_stay_visible(tmp_path):
    _setup(tmp_path, [('TX', 'complete', 0, '2026-10-02T09:00:00+00:00'),
                      ('unknown', 'complete', 0, 'bad-clock'),
                      ('naive', 'complete', 0, '2026-10-04T02:00:00')])
    with closing(worker._db(tmp_path / 'complement/queue.sqlite3')) as conn, conn:
        conn.execute("UPDATE tasks SET last_attempt_at_utc='2026-10-02T06:00:00+00:00' WHERE data_id='TX'")
        before = conn.execute('SELECT * FROM tasks ORDER BY data_id').fetchall()
        assert worker._reconcile_periodic_publication(conn, SUNDAY) == 0
        assert conn.execute('SELECT * FROM tasks ORDER BY data_id').fetchall() == before
        assert worker._next_task(conn, SUNDAY, incremental_only=True).data_id == 'TX'


def test_migration_releases_weekend_reserve_without_hiding_due_failure(tmp_path, monkeypatch):
    from downloader import finmind_scheduling
    monkeypatch.setattr(finmind_scheduling, '_free_refresh_events', lambda *a: [])
    due = (SUNDAY + timedelta(minutes=20)).isoformat()
    _setup(tmp_path, [('TX', 'complete', 0, due), ('failed', 'failed', 0, SUNDAY.isoformat())])
    plan = incremental_reservation(tmp_path, SUNDAY, sources=())
    assert (plan['ready_requests'], plan['upcoming_requests']) == (1, 1)
    with closing(worker._db(tmp_path / 'complement/queue.sqlite3')) as conn, conn:
        worker._reconcile_periodic_publication(conn, SUNDAY)
    plan = incremental_reservation(tmp_path, SUNDAY, sources=())
    assert (plan['ready_requests'], plan['upcoming_requests']) == (1, 0)
    assert {row['owner'] for row in plan['events']} == {'complement'}


def test_first_backfill_and_overnight_derivative_work_remain_admitted(tmp_path):
    with closing(worker._db(tmp_path / 'queue.sqlite3')) as conn, conn:
        worker._add_tasks(conn, [(DATASET, 'TX', 'history', 'id_history', 3)])
        assert worker._next_task(conn, SUNDAY).dataset == DATASET
    night = worker.Task('TaiwanFuturesSpreadTrading', 'TX', '2026-10-02', 'year', 0, 'complete')
    assert worker._next_refresh(night, SUNDAY, empty=False) == (SUNDAY + timedelta(hours=3)).isoformat()


def test_average_and_event_forecasts_match_successful_publication_weekdays(tmp_path, monkeypatch):
    from test_finmind_eta_work import _database
    monkeypatch.setattr(audit_finmind_query_ranges, 'registry', lambda: {
        DATASET: {'primary_owner': 'complement', 'query_shape': 'per_product_date_range'}})
    _database(tmp_path, 'sponsor', [])
    _database(tmp_path, 'complement', [(DATASET, 'TX', 'history', 'id_history', 0, 'complete',
                                       (SUNDAY + timedelta(hours=3)).isoformat())])
    forecast = telemetry.recurring_forecast(tmp_path, SUNDAY)
    # Existing independent Free/snapshot maintenance remains in the total.
    baseline = 3 + 4 / 24
    assert forecast['requests_per_hour_by_phase']['incremental'] == pytest.approx(baseline + 1 / (3 * 7 / 5))
    events = telemetry.timed_incremental_forecast(tmp_path, SUNDAY)['events']
    settlement = [row for row in events if row.get('publishing_weekdays')]
    assert len(settlement) == 1
    assert settlement[0]['first_at_utc'] == MONDAY.isoformat()
    assert settlement[0]['publishing_weekdays'] == list(range(5))
    assert settlement[0]['interval_seconds'] == 10800

from dataclasses import replace
from datetime import UTC, date, datetime
import json

import pytest

from downloader import download_finmind_complement as worker
from downloader import finmind_supplemental as supplemental
from downloader.finmind_history_calendar import arrival_density, load_closures, reconcile_closures
from downloader.finmind_us_calendar import published_calendar
from downloader.download_finmind_sponsor import OfficialSessions


NOW = datetime(2026, 10, 3, 2, tzinfo=UTC)
FIRST, LAST = date(2026, 9, 24), date(2026, 10, 2)
US, TW, FUTURES = 'USStockPriceMinute', 'TaiwanStockPriceTick', 'TaiwanFuturesTick'


def specs(monkeypatch):
    value = {name: replace(supplemental.SOURCES[name], first=FIRST) for name in (US, TW, FUTURES)}
    monkeypatch.setattr(supplemental, 'SOURCES', value)
    return value


def tw_proof():
    days = frozenset(date(2026, 9, day) for day in (24, 26, 28, 29, 30)) | {date(2026, 10, 1), LAST}
    return OfficialSessions(FIRST, LAST, days, 'a' * 64)


def test_published_exceptions_early_closes_and_unverified_bounds():
    calendar = published_calendar(NOW)[US]
    for day in ('2021-06-18', '2021-12-31', '2024-07-03', '2025-12-24'):
        assert calendar.count(US, date.fromisoformat(day), date.fromisoformat(day)) == 0
    for day in ('2022-06-20', '2025-01-09', '2026-07-03', '2026-10-03'):
        assert calendar.count(US, date.fromisoformat(day), date.fromisoformat(day)) == 1
    assert calendar.count(TW, FIRST, LAST) == 0
    assert calendar.count(FUTURES, FIRST, LAST) == 0
    assert calendar.count(US, date(2027, 1, 1), date(2027, 1, 10)) == 0
    assert calendar.basis == 'official_published_us_cash_calendar'
    model = arrival_density(calendar, US, NOW)
    assert model['basis'] == 'published_schedule_not_observed_provider_sessions'


@pytest.mark.parametrize('hour,minute,expected', [(15, 59, '2026-10-05'), (16, 0, '2026-10-05'),
                                                (23, 59, '2026-10-05'), (0, 0, '2026-10-06')])
def test_us_date_anchor_does_not_seal_an_unfinished_session_at_taipei_midnight(hour, minute, expected):
    # UTC 16:00 is Taipei midnight; next morning's 08:00 is UTC 00:00.
    day = 7 if hour == 0 else 6
    stamp = datetime(2026, 10, day, hour, minute, tzinfo=UTC)
    assert supplemental._eligible_anchor(supplemental.SOURCES[US], stamp).isoformat() == expected


@pytest.mark.parametrize('body', ['{}', 'bad json'])
def test_unreadable_or_invalid_proof_excludes_nothing(tmp_path, body):
    path = tmp_path / 'bad.json'
    path.write_text(body)
    assert published_calendar(NOW, path=path) == {}
    assert published_calendar(NOW, path=tmp_path / 'missing.json') == {}


def test_scoped_cash_calendars_do_not_share_holidays_or_night_dates(tmp_path, monkeypatch):
    specs(monkeypatch)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        supplemental.seed(conn, {'us': ['AAPL'], 'stocks': ['2330'], 'futures': ['MTX']}, NOW,
                          official_sessions=tw_proof(), additional_calendars=published_calendar(NOW))
        retained = {(dataset, day) for dataset, day in conn.execute('SELECT dataset,partition FROM tasks')}
        assert (US, '2026-09-25') in retained
        assert (US, '2026-09-26') not in retained
        assert (TW, '2026-09-26') in retained
        assert (TW, '2026-09-25') not in retained
        assert (FUTURES, '2026-09-27') in retained
        closure = load_closures(conn)
        assert closure.scope(US).receipt_sha256 != closure.scope(TW).receipt_sha256
        assert closure.scope(US).basis == 'official_published_us_cash_calendar'
        stats = supplemental.frontier_status(conn, NOW)
        assert stats[US]['calendar_receipt_sha256'] == closure.scope(US).receipt_sha256
        assert stats[US]['calendar_basis'] == 'official_published_us_cash_calendar'


def test_loss_of_us_proof_reopens_only_us_candidates_preserving_receipts(tmp_path, monkeypatch):
    source = specs(monkeypatch)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        supplemental.seed(conn, {'us': ['AAPL'], 'stocks': ['2330']}, NOW,
                          official_sessions=tw_proof(), additional_calendars=published_calendar(NOW))
        conn.execute("UPDATE tasks SET state='complete',rows=1,receipt_path='immutable.json'")
        retained = conn.execute('SELECT * FROM tasks ORDER BY dataset,partition').fetchall()
        reconcile_closures(conn, source, tw_proof(), NOW, additional_calendars={})
        stats = supplemental.frontier_status(conn, NOW)
        assert stats[US]['unseeded_partition_candidates'] == 2
        assert stats[TW]['unseeded_partition_candidates'] == 0
        assert conn.execute('SELECT * FROM tasks ORDER BY dataset,partition').fetchall() == retained
        supplemental.seed(conn, {'us': ['AAPL'], 'stocks': ['2330']}, NOW, official_sessions=tw_proof())
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE dataset=? AND state='pending'", (US,)).fetchone()[0] == 2


def test_nonempty_us_conflict_disables_only_us_pruning_and_preserves_source(tmp_path, monkeypatch):
    specs(monkeypatch)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [(US, 'AAPL', '2026-09-26', 'id_day', 8)])
        conn.execute("UPDATE tasks SET state='complete',rows=2,receipt_path='original.json'")
        before = conn.execute('SELECT * FROM tasks').fetchone()
        supplemental.seed(conn, {'us': ['AAPL'], 'stocks': ['2330']}, NOW,
                          official_sessions=tw_proof(), additional_calendars=published_calendar(NOW))
        assert US not in load_closures(conn).datasets
        assert TW in load_closures(conn).datasets
        assert conn.execute('SELECT * FROM tasks WHERE dataset=? AND partition=?', (US, '2026-09-26')).fetchone() == before
        assert conn.execute('SELECT COUNT(*) FROM tasks WHERE dataset=?', (US,)).fetchone()[0] == 9


def test_existing_empty_closed_date_is_not_retried_or_claimed_complete(tmp_path, monkeypatch):
    source = specs(monkeypatch)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        conn.execute('CREATE TABLE finmind_source_frontiers (dataset TEXT,data_id TEXT,older_than TEXT,newer_than TEXT)')
        worker._add_tasks(conn, [(US, 'AAPL', '2026-09-26', 'id_day', 8)])
        conn.execute("UPDATE tasks SET state='observed_empty',receipt_path='empty.json',error_code='source_empty'")
        reconcile_closures(conn, source, None, NOW, additional_calendars=published_calendar(NOW))
        assert conn.execute('SELECT state,receipt_path,error_code FROM tasks').fetchone() == ('non_session', 'empty.json', 'source_empty')
        reconcile_closures(conn, source, None, NOW)
        assert conn.execute('SELECT state,receipt_path,error_code FROM tasks').fetchone() == ('pending', 'empty.json', 'source_empty')


def test_calendar_archives_semantic_changes_not_heartbeats(tmp_path, monkeypatch):
    source = specs(monkeypatch)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        supplemental.seed(conn, {'us': ['AAPL']}, NOW, additional_calendars=published_calendar(NOW))
        later = NOW.replace(minute=1)
        reconcile_closures(conn, source, None, later, additional_calendars=published_calendar(later))
        assert conn.execute('SELECT COUNT(*) FROM finmind_history_calendar_versions').fetchone()[0] == 1
        metadata = json.loads(conn.execute('SELECT metadata_json FROM finmind_history_calendar').fetchone()[0])
        assert metadata['contract_version'] == 3


def test_early_us_observation_is_retained_and_final_session_recheck_is_scheduled(tmp_path, monkeypatch):
    monkeypatch.setattr(supplemental, 'SOURCES', {US: supplemental.SOURCES[US]})
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [(US, 'AAPL', '2026-10-02', 'id_day', 8),
                                 (US, 'MSFT', '2026-10-02', 'id_day', 8)])
        conn.execute("UPDATE tasks SET state='complete',rows=100,receipt_path='early.json',"
                     "last_attempt_at_utc='2026-10-02T16:10:00+00:00' WHERE data_id='AAPL'")
        conn.execute("UPDATE tasks SET state='complete',rows=390,receipt_path='final.json',"
                     "last_attempt_at_utc='2026-10-03T00:10:00+00:00' WHERE data_id='MSFT'")
        supplemental.seed(conn, {'us': ['AAPL', 'MSFT']}, NOW, additional_calendars=published_calendar(NOW))
        assert conn.execute('SELECT state,rows,receipt_path,next_attempt_at_utc FROM tasks '
                            'WHERE dataset=? AND data_id=? AND partition=?', (US, 'AAPL', '2026-10-02')).fetchone() == (
                                'pending', 100, 'early.json', '2026-10-03T00:00:00+00:00')
        assert conn.execute('SELECT state,rows,receipt_path FROM tasks WHERE dataset=? AND data_id=? AND partition=?',
                            (US, 'MSFT', '2026-10-02')).fetchone() == ('complete', 390, 'final.json')

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from downloader import download_finmind_sponsor as sponsor
from downloader.download_finmind_complement import Task
from downloader.finmind_observation_dates import (
    EXCLUDED_STATE, PERIOD_DATASETS, is_observation_date,
    reconcile_observation_dates,
)


@pytest.mark.parametrize('dataset', sorted(PERIOD_DATASETS))
def test_period_plan_preserves_evidence_and_revisits_late_publication(tmp_path: Path, dataset: str):
    now = datetime(2026, 11, 10, 8, tzinfo=UTC)
    quarterly = dataset != 'TaiwanStockMonthRevenue'
    anchor = '2026-09-30' if quarterly else '2026-11-01'
    attempt = now - timedelta(days=5)
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        conn.executemany(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,rows,receipt_path,last_attempt_at_utc) "
            "VALUES (?,'',?,'day',1,?,0,'original-receipt',?)",
            [(dataset, anchor, 'observed_empty', attempt.isoformat()),
             (dataset, '2026-09-29', 'observed_empty', attempt.isoformat()),
             (dataset, '2026-09-28', 'pending', None)],
        )
        policy = reconcile_observation_dates(conn, now, {dataset})
        assert policy['datasets'][dataset]['excluded_calendar_dates'] == 2
        assert conn.execute('SELECT count(*) FROM observation_date_audit').fetchone()[0] == 2
        assert conn.execute("SELECT receipt_path FROM tasks WHERE state=?", (EXCLUDED_STATE,)).fetchall() == [('original-receipt',)] * 2
        # Period date has passed, but actual publication can be weeks later.
        assert sponsor._next(conn, now, incremental_only=True).partition == anchor
        status = sponsor._status(conn, tmp_path, 'fixture')
        assert status['series'][dataset]['target'] == 1
        assert status['series'][dataset]['not_observation_date'] == 2
        # No excluded date is claimed as successfully downloaded.
        assert status['series'][dataset]['complete'] == 0
        reconcile_observation_dates(conn, now, {dataset})
        assert conn.execute('SELECT count(*) FROM observation_date_audit').fetchone()[0] == 2


def test_nonempty_off_cycle_date_disables_entire_series_pruning(tmp_path: Path):
    dataset = 'TaiwanStockFinancialStatements'
    now = datetime(2026, 9, 27, tzinfo=UTC)
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        conn.executemany(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,rows) "
            "VALUES (?,'',?,'day',1,?,?)",
            [(dataset, '1990-03-01', 'complete', 12),
             (dataset, '1990-03-02', EXCLUDED_STATE, 0)],
        )
        policy = reconcile_observation_dates(conn, now, {dataset})
        assert policy['datasets'][dataset]['state'] == 'nonempty_date_conflict'
        assert conn.execute("SELECT state,rows FROM tasks ORDER BY partition").fetchall() == [('complete', 12), ('pending', 0)]


def test_do_not_infer_month_price_or_holdings_dates_from_frequency():
    assert is_observation_date('TaiwanStockMonthPrice', date(2024, 6, 21))
    assert is_observation_date('TaiwanStockHoldingSharesPer', date(2026, 9, 24))
    assert is_observation_date('TaiwanStockBalanceSheet', date(2024, 3, 31))
    assert not is_observation_date('TaiwanStockBalanceSheet', date(2024, 3, 1))
    assert is_observation_date('TaiwanStockMonthRevenue', date(2024, 6, 1))
    assert not is_observation_date('TaiwanStockMonthRevenue', date(2024, 6, 21))


def test_refresh_is_anchored_to_actual_attempt_not_repeated_seed(tmp_path: Path):
    dataset = 'TaiwanStockMonthRevenue'
    now = datetime(2026, 9, 27, tzinfo=UTC)
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        conn.execute(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,last_attempt_at_utc) "
            "VALUES (?,'','2026-09-01','day',1,'observed_empty',?)", (dataset, now.isoformat()),
        )
        for checked_at in (now, now + timedelta(hours=1)):
            reconcile_observation_dates(conn, checked_at, {dataset})
            row = conn.execute('SELECT next_attempt_at_utc,priority FROM tasks').fetchone()
            assert row == ((now + timedelta(hours=4)).isoformat(), 0)


def test_yesterday_empty_is_not_still_current_but_quarter_keeps_refreshing(tmp_path: Path, monkeypatch):
    now = datetime(2026, 9, 27, 2, tzinfo=UTC)
    monkeypatch.setattr(sponsor, '_store', lambda *_: {
        'status': 'observed_empty', 'rows': 0, 'receipt_path': 'test.json',
    })
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        for dataset, partition in [('TaiwanStockPrice', '2026-09-26'),
                                   ('TaiwanStockBalanceSheet', '2026-06-30')]:
            conn.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
                         "VALUES (?,'',?,'day',1,'inflight')", (dataset, partition))
            sponsor._finish(conn, tmp_path, Task(dataset, '', partition, 'day', 1, 'inflight'), [], now)
        assert conn.execute("SELECT next_attempt_at_utc FROM tasks WHERE dataset='TaiwanStockPrice'").fetchone() == (None,)
        assert conn.execute("SELECT next_attempt_at_utc FROM tasks WHERE dataset='TaiwanStockBalanceSheet'").fetchone() == ((now + timedelta(hours=4)).isoformat(),)


def test_failures_keep_retry_and_entitlement_boundaries(tmp_path: Path):
    now = datetime(2026, 9, 27, tzinfo=UTC)
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        conn.executemany("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
                         "VALUES ('TaiwanStockBalanceSheet','',?,'day',1,?)",
                         [('2026-06-30', 'blocked'), ('2026-03-31', 'failed')])
        reconcile_observation_dates(conn, now, {'TaiwanStockBalanceSheet'})
        assert conn.execute('SELECT state FROM tasks ORDER BY partition').fetchall() == [('failed',), ('blocked',)]


def test_unverified_ancient_dates_remain_required_but_anchors_run_first(tmp_path: Path):
    now = datetime(2026, 9, 27, tzinfo=UTC)
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        conn.executemany("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
                         "VALUES ('TaiwanStockFinancialStatements','',?,'day',3,'pending')",
                         [('1990-03-01',), ('1990-03-31',)])
        reconcile_observation_dates(conn, now, {'TaiwanStockFinancialStatements'})
        assert sponsor._next(conn, now).partition == '1990-03-31'
        assert sponsor._next(conn, now).partition == '1990-03-01'


def test_nullable_proof_disables_pruning_without_stopping_recent_refresh(tmp_path: Path):
    now = datetime(2026, 9, 27, tzinfo=UTC)
    dataset = 'TaiwanStockBalanceSheet'
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        conn.executemany("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,rows) "
                         "VALUES (?,'',?,'day',1,?,?)",
                         [(dataset, '2020-03-31', 'complete', 1),
                          (dataset, '2026-06-30', 'observed_empty', 0),
                          (dataset, '2026-06-29', 'pending', 0)])
        policy = reconcile_observation_dates(conn, now, {dataset})
        assert policy['datasets'][dataset]['state'] == 'nonempty_date_conflict'
        assert conn.execute("SELECT state FROM tasks WHERE partition='2026-06-29'").fetchone() == ('pending',)
        assert sponsor._next(conn, now, incremental_only=True).partition == '2026-06-30'


def test_empty_refresh_does_not_destroy_last_good_or_old_receipt(tmp_path: Path):
    from downloader.download_finmind_complement import SourceError
    now = datetime(2026, 9, 27, tzinfo=UTC)
    dataset, partition = 'TaiwanStockMonthRevenue', '2026-09-01'
    task = Task(dataset, '', partition, 'day', 0, 'inflight')
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        conn.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
                     "VALUES (?,'',?,'day',0,'inflight')", (dataset, partition))
        receipt = sponsor._finish(conn, tmp_path, task, [{'date': partition, 'revenue': 123}], now)
        original = (tmp_path / receipt['receipt_path']).read_bytes()
        with pytest.raises(SourceError, match='unexpected_empty_after_nonempty'):
            sponsor._finish(conn, tmp_path, task, [], now + timedelta(hours=4))
        assert (tmp_path / receipt['receipt_path']).read_bytes() == original
        assert conn.execute('SELECT rows FROM tasks').fetchone() == (1,)
        sponsor._finish(conn, tmp_path, task, [{'date': partition, 'revenue': 124}], now + timedelta(hours=8))
        history = list((tmp_path / 'receipt_history').rglob('*.json'))
        assert len(history) == 1 and history[0].read_bytes() == original


def test_real_session_empty_survives_midnight(tmp_path: Path, monkeypatch):
    from stockagent.live.market_status import TwStockDayDecision
    now = datetime(2026, 9, 24, 16, 5, tzinfo=UTC)  # 9/25 00:05 Taipei
    monkeypatch.setattr(sponsor, 'tw_stock_day_decision',
                        lambda *_a, **_kw: TwStockDayDecision('scheduled_open', 'fixture'))
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        conn.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,last_attempt_at_utc) "
                     "VALUES ('TaiwanStockPrice','','2026-09-24','day',1,'observed_empty',?)",
                     ((now - timedelta(hours=5)).isoformat(),))
        sponsor._reconcile_daily_refresh(conn, now)
        assert sponsor._next(conn, now).partition == '2026-09-24'


def test_release_minute_is_not_rounded_to_the_hour(tmp_path: Path, monkeypatch):
    spec = sponsor._s('TaiwanStockGovernmentBankBuySell', '2026-09-23', 'day', 2, 23, 30)
    monkeypatch.setattr(sponsor, 'SOURCES', (spec,))
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        sponsor._seed(conn, datetime(2026, 9, 23, 15, 29, tzinfo=UTC))
        assert conn.execute('SELECT count(*) FROM tasks').fetchone()[0] == 0
        sponsor._seed(conn, datetime(2026, 9, 23, 15, 30, tzinfo=UTC))
        assert conn.execute('SELECT partition FROM tasks').fetchone() == ('2026-09-23',)

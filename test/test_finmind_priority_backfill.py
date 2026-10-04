from datetime import UTC, datetime

from downloader import download_finmind_complement as worker


def test_temporary_priority_preserves_refresh_quota_and_restores_schedule(tmp_path):
    now = datetime(2026, 9, 30, tzinfo=UTC)
    with worker._db(tmp_path / 'queue.sqlite3') as db:
        worker._add_tasks(db, [
            ('TaiwanFuturesKBar', 'TX', '2026-09-24', 'id_day', 8),
            ('TaiwanStockInfo', '', 'latest', 'snapshot', 0),
            ('GoldPrice', '', '2010', 'year', 3),
        ])
        db.execute('CREATE TABLE finmind_priority_tasks '
                   '(dataset TEXT,data_id TEXT,partition TEXT,PRIMARY KEY(dataset,data_id,partition))')
        db.execute("INSERT INTO finmind_priority_tasks VALUES ('TaiwanFuturesKBar','TX','2026-09-24')")
        assert worker._next_task(db, now).dataset == 'TaiwanStockInfo'
        assert worker._next_task(db, now, incremental_only=True).dataset == 'TaiwanStockInfo'
        assert worker._next_task(db, now, background_only=True).dataset == 'TaiwanFuturesKBar'
        db.execute("UPDATE tasks SET state='complete' WHERE dataset='TaiwanStockInfo'")
        assert worker._next_task(db, now).dataset == 'TaiwanFuturesKBar'
        db.execute("UPDATE tasks SET state='observed_empty' WHERE dataset='TaiwanFuturesKBar'")
        assert worker._next_task(db, now).dataset == 'GoldPrice'
        db.execute("UPDATE tasks SET state='not_entitled' WHERE dataset='TaiwanFuturesKBar'")
        assert worker._next_task(db, now).dataset == 'GoldPrice'
        db.execute("UPDATE tasks SET state='failed',next_attempt_at_utc='2026-10-01T00:00:00+00:00' "
                   "WHERE dataset='TaiwanFuturesKBar'")
        assert worker._next_task(db, now).dataset == 'GoldPrice'
        assert db.execute("SELECT priority FROM tasks WHERE dataset='TaiwanFuturesKBar'").fetchone() == (8,)

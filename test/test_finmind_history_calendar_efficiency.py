"""History efficiency may remove redundant calls, never observations or scope."""
from dataclasses import replace
from datetime import UTC, date, datetime
import json

import pytest

from downloader import download_finmind_complement as worker
from downloader import finmind_supplemental as supplemental
from downloader.finmind_history_calendar import load_closures, reconcile_closures
from downloader.download_finmind_sponsor import OfficialSessions


NOW = datetime(2026, 10, 2, 16, tzinfo=UTC)
FIRST = date(2026, 9, 24)
LAST = date(2026, 10, 2)
CASH = 'TaiwanStockPriceTick'
FUTURES = 'TaiwanFuturesTick'
MARKET = 'TaiwanFuturesSpreadTick'


def proof(*, reopen=()):
    # Includes a Saturday session. Friday 25 and Sunday 27 are proven closed;
    # exclusion must follow this evidence, not a guessed weekday convention.
    days = frozenset(date(2026, 9, day) for day in (24, 26, 28, 29, 30)) | {
        date(2026, 10, 1), LAST, *reopen}
    return OfficialSessions(FIRST, LAST, frozenset(days), 'a' * 64 if not reopen else 'b' * 64)


def sources(monkeypatch, names=(CASH,)):
    specs = {name: replace(supplemental.SOURCES[name], first=FIRST) for name in names}
    monkeypatch.setattr(supplemental, 'SOURCES', specs)
    return specs


def test_compact_cash_calendar_preserves_saturday_and_futures_timestamp_days(tmp_path, monkeypatch):
    sources(monkeypatch, (CASH, FUTURES))
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        stats = supplemental.seed(conn, {'stocks': ['2330', '2317'], 'futures': ['MTX']}, NOW,
                                  official_sessions=proof())
        assert conn.execute('SELECT COUNT(*) FROM tasks WHERE dataset=?', (CASH,)).fetchone()[0] == 14
        # Physical tick dates are published on the next morning, so today's
        # still-open date is not a ninth historical request candidate.
        assert conn.execute('SELECT COUNT(*) FROM tasks WHERE dataset=?', (FUTURES,)).fetchone()[0] == 8
        assert conn.execute('SELECT COUNT(*) FROM tasks WHERE dataset=? AND partition=?',
                            (CASH, '2026-09-26')).fetchone()[0] == 2
        assert conn.execute('SELECT COUNT(*) FROM tasks WHERE dataset=? AND partition=?',
                            (CASH, '2026-09-27')).fetchone()[0] == 0
        assert conn.execute('SELECT COUNT(*) FROM tasks WHERE dataset=? AND partition=?',
                            (FUTURES, '2026-09-27')).fetchone()[0] == 1
        assert stats[CASH]['unseeded_partition_candidates'] == 0
        assert stats[FUTURES]['unseeded_partition_candidates'] == 0
        assert len(load_closures(conn).days) == 2  # not two rows per identity


def test_unknown_calendar_does_not_prune_weekends_or_unknown_lifetimes(tmp_path, monkeypatch):
    sources(monkeypatch)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        supplemental.seed(conn, {'stocks': ['2330', '2317']}, NOW)
        assert conn.execute('SELECT COUNT(*) FROM tasks').fetchone()[0] == 18
        assert load_closures(conn).datasets == frozenset()


def test_skipping_discarded_seed_summary_keeps_exact_queue_and_frontier_state(tmp_path, monkeypatch):
    sources(monkeypatch, (CASH, FUTURES))
    states = []
    for name, include in [('normal', True), ('worker', False)]:
        with worker._db(tmp_path / name / 'queue.sqlite3') as conn:
            if include:
                expected = supplemental.seed(conn, {'stocks': ['2330'], 'futures': ['TX']}, NOW,
                                             official_sessions=proof())
            else:
                original = supplemental.frontier_status
                monkeypatch.setattr(supplemental, 'frontier_status', lambda *a, **k: pytest.fail('discarded aggregation'))
                assert supplemental.seed(conn, {'stocks': ['2330'], 'futures': ['TX']}, NOW,
                                         official_sessions=proof(), include_status=False) == {}
                monkeypatch.setattr(supplemental, 'frontier_status', original)
                assert supplemental.frontier_status(conn, NOW) == expected
            states.append((conn.execute('SELECT * FROM tasks ORDER BY dataset,data_id,partition').fetchall(),
                           conn.execute('SELECT * FROM finmind_source_frontiers ORDER BY dataset,data_id').fetchall()))
    assert states[0] == states[1]


@pytest.mark.parametrize('revised', [None, 'reopened'])
def test_calendar_loss_or_revision_reopens_skipped_dates_and_keeps_receipts(tmp_path, monkeypatch, revised):
    specs = sources(monkeypatch)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        supplemental.seed(conn, {'stocks': ['2330', '2317']}, NOW, official_sessions=proof())
        conn.execute("UPDATE tasks SET state='complete',rows=1,receipt_path='original.json'")
        original = conn.execute('SELECT * FROM tasks ORDER BY data_id,partition').fetchall()
        next_proof = proof(reopen=(date(2026, 9, 25),)) if revised else None
        reconcile_closures(conn, specs, next_proof, NOW)
        before_seed = supplemental.frontier_status(conn, NOW)[CASH]
        expected = 2 if revised else 4
        assert before_seed['unseeded_partition_candidates'] == expected
        assert (before_seed['raw_unseeded_calendar_candidates'] == before_seed['unseeded_partition_candidates']
                + before_seed['excluded_calendar_candidates'] + before_seed['already_materialized_candidates'])
        supplemental.seed(conn, {'stocks': ['2330', '2317']}, NOW, official_sessions=next_proof)
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE state='pending'").fetchone()[0] == expected
        assert conn.execute("SELECT * FROM tasks WHERE state='complete' ORDER BY data_id,partition").fetchall() == original
        assert supplemental.frontier_status(conn, NOW)[CASH]['unseeded_partition_candidates'] == 0
        assert conn.execute('SELECT COUNT(*) FROM finmind_history_calendar_versions').fetchone()[0] == 2


def test_nonempty_calendar_conflict_disables_exclusion_without_touching_source(tmp_path, monkeypatch):
    sources(monkeypatch)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [(CASH, '2330', '2026-09-25', 'id_day', 10)])
        conn.execute("UPDATE tasks SET state='complete',rows=3,receipt_path='preserved.json'")
        original = conn.execute('SELECT * FROM tasks').fetchone()
        supplemental.seed(conn, {'stocks': ['2330']}, NOW, official_sessions=proof())
        assert load_closures(conn).datasets == frozenset()
        assert conn.execute('SELECT COUNT(*) FROM tasks').fetchone()[0] == 9
        assert conn.execute('SELECT * FROM tasks WHERE partition=?', ('2026-09-25',)).fetchone() == original


def test_calendar_revision_keeps_lagging_forward_and_history_ranges_disjoint(tmp_path, monkeypatch):
    specs = sources(monkeypatch)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        supplemental.seed(conn, {'stocks': ['2330']}, NOW, official_sessions=proof())
        conn.execute("DELETE FROM tasks")  # disposable fixture, never a source queue
        conn.execute('UPDATE finmind_source_frontiers SET older_than=NULL,newer_than=?', ('2026-09-26',))
        reconcile_closures(conn, specs, None, NOW)
        assert conn.execute('SELECT older_than,newer_than FROM finmind_source_frontiers').fetchone() == (
            '2026-09-26', '2026-09-26')
        stats = supplemental.frontier_status(conn, NOW)[CASH]
        assert stats['unseeded_history_candidates'] == 3
        assert stats['unseeded_forward_candidates'] == 6
        assert stats['unseeded_partition_candidates'] == 9  # not 10
        conn.execute('UPDATE finmind_source_frontiers SET older_than=NULL,newer_than=?', ('2026-09-24',))
        reconcile_closures(conn, specs, proof(), NOW)
        reconcile_closures(conn, specs, None, NOW)
        assert conn.execute('SELECT older_than FROM finmind_source_frontiers').fetchone() == (None,)


def test_frontier_subtracts_targeted_materialized_history_and_forward_once(tmp_path, monkeypatch):
    specs = sources(monkeypatch)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        supplemental.seed(conn, {'stocks': ['2330']}, NOW, official_sessions=proof())
        conn.execute("DELETE FROM tasks")  # synthetic queue only
        conn.execute('UPDATE finmind_source_frontiers SET older_than=?,newer_than=?',
                     ('2026-09-28', '2026-09-30'))
        worker._add_tasks(conn, [(CASH, '2330', day, 'id_day', 10) for day in (
            '2026-09-26', '2026-10-01', '2026-09-30', '2026-09-25')])
        reconcile_closures(conn, specs, proof(), NOW)
        row = supplemental.frontier_status(conn, NOW)[CASH]
        assert row['raw_unseeded_calendar_candidates'] == 7
        assert row['excluded_calendar_candidates'] == 2
        assert row['already_materialized_candidates'] == 2  # 25 already excluded; 30 outside frontier
        assert row['unseeded_history_candidates'] == 2
        assert row['unseeded_forward_candidates'] == 1
        assert row['unseeded_partition_candidates'] == 3


def test_market_day_migration_preserves_legacy_evidence_without_master_id_multiplier(tmp_path, monkeypatch):
    sources(monkeypatch, (MARKET,))
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [(MARKET, value, '2026-10-02', 'id_day', 10) for value in ('CAF', 'TX')])
        original = conn.execute('SELECT * FROM tasks ORDER BY data_id').fetchall()
        conn.execute('CREATE TABLE finmind_source_frontiers '
                     '(dataset TEXT,data_id TEXT,older_than TEXT,newer_than TEXT,PRIMARY KEY(dataset,data_id))')
        conn.executemany('INSERT INTO finmind_source_frontiers VALUES (?,?,?,?)',
                         [(MARKET, value, '2026-10-02', '2026-10-02') for value in ('CAF', 'TX')])
        stats = supplemental.seed(conn, {'futures': ['CAF', 'TX', 'new-product']}, NOW)
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE state='deprecated_query_shape'").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE data_id='' AND state='pending'").fetchone()[0] == 9
        assert conn.execute('SELECT COUNT(*) FROM finmind_source_frontiers').fetchone()[0] == 3
        assert stats[MARKET]['known_identifiers'] == 1
        assert stats[MARKET]['unseeded_partition_candidates'] == 0
        snapshots = conn.execute('SELECT prior_task_json FROM finmind_query_shape_migrations ORDER BY data_id').fetchall()
        assert [json.loads(row[0])['data_id'] for row in snapshots] == ['CAF', 'TX']
        assert [json.loads(row[0])['state'] for row in snapshots] == [row[5] for row in original]


def test_market_tick_contract_keeps_all_ids_dates_duplicates_and_signed_prices():
    url, params, metadata = supplemental.request_contract(MARKET, '', '2026-10-02', LAST)
    assert url.endswith('/data') and 'data_id' not in params and 'end_date' not in params
    assert metadata['query_shape'] == 'whole_market_day'
    row = {'date': '2026-10-02', 'futures_id': 'new-unlisted-master-id', 'price': -1, 'volume': 2}
    supplemental.validate_response(MARKET, '', '2026-10-02', LAST, [row, row])
    with pytest.raises(ValueError, match='market_history_requires_empty_identifier'):
        supplemental.request_contract(MARKET, 'TX', '2026-10-02', LAST)
    with pytest.raises(ValueError, match='outside_range'):
        supplemental.validate_response(MARKET, '', '2026-10-02', LAST, [{**row, 'date': '2026-10-03'}])


def test_forward_work_is_bounded_but_not_suppressed_by_full_history(tmp_path, monkeypatch):
    sources(monkeypatch)
    monkeypatch.setattr(supplemental, 'WORKING_SET', 2)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        supplemental.seed(conn, {'stocks': ['2330', '2317']}, NOW)
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE state='pending'").fetchone()[0] == 2
        next_day = datetime(2026, 10, 3, 16, tzinfo=UTC)
        supplemental.seed(conn, {'stocks': ['2330', '2317']}, next_day)
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE partition='2026-10-03'").fetchone()[0] == 2
        supplemental.seed(conn, {'stocks': ['2330', '2317']}, datetime(2026, 10, 4, 16, tzinfo=UTC))
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE state='pending'").fetchone()[0] == 4
        assert conn.execute('SELECT MAX(priority) FROM tasks').fetchone()[0] == 10


def test_proof_metadata_integrity_failure_is_not_silently_treated_as_zero(tmp_path, monkeypatch):
    sources(monkeypatch)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        supplemental.seed(conn, {'stocks': ['2330']}, NOW, official_sessions=proof())
        conn.execute("UPDATE finmind_history_calendar SET metadata_json='{}'")
        with pytest.raises(ValueError, match='history_calendar_metadata_integrity'):
            supplemental.frontier_status(conn, NOW)

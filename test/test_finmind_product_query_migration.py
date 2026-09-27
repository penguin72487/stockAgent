from datetime import UTC, date, datetime
import json

import pytest

from downloader import download_finmind_sponsor as sponsor
from downloader.download_finmind_complement import SourceError, Task
from downloader.finmind_scheduling import PRODUCT_HISTORY_STARTS
from stockagent.live.finmind_dashboard import build_finmind_public_status
from downloader.acquisition_policy import _finmind_required


def test_retire_only_unsupported_no_id_shape_with_exact_audit(tmp_path):
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        for dataset in PRODUCT_HISTORY_STARTS:
            conn.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,receipt_path) "
                         "VALUES (?,'','2025-01-01','year',2,'observed_empty','evidence.json')", (dataset,))
        conn.execute("PRAGMA user_version=5")
        conn.commit()
        evidence = tmp_path / 'evidence.json'
        evidence.write_bytes(b'{"status":"observed_empty"}\n')
        sponsor._migrate_query_shape(conn, tmp_path)
        assert evidence.read_bytes() == b'{"status":"observed_empty"}\n'
        assert conn.execute("SELECT DISTINCT state,error_code FROM tasks").fetchall() == [
            ('deprecated_query_shape', 'requires_product_id')]
        audited = conn.execute('SELECT old_row_json FROM product_query_migration_audit').fetchall()
        assert len(audited) == 2
        assert all(json.loads(row[0])['state'] == 'observed_empty' for row in audited)
        assert sponsor._next(conn, datetime.now(UTC)) is None
        sponsor._migrate_query_shape(conn, tmp_path)
        assert conn.execute('SELECT count(*) FROM product_query_migration_audit').fetchone()[0] == 2
        assert _finmind_required(str(tmp_path / 'queue.sqlite3'), 0) == 0


def test_unmigrated_product_shape_never_dispatched_or_fetched(tmp_path):
    dataset = next(iter(PRODUCT_HISTORY_STARTS))
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        conn.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
                     "VALUES (?,'','2025-01-01','year',2,'pending')", (dataset,))
        conn.commit()
        assert sponsor._next(conn, datetime.now(UTC)) is None
    with pytest.raises(SourceError, match='requires_product_id'):
        sponsor._fetch(Task(dataset, '', '2025-01-01', 'year', 2, 'pending'),
                       'unused', object(), tmp_path, date(2026, 9, 27))


def test_catalog_keeps_legacy_alias_delegated_not_false_complete(tmp_path):
    result = build_finmind_public_status(tmp_path)
    for dataset in PRODUCT_HISTORY_STARTS:
        row = next(r for r in result['datasets'] if r['id'] == dataset + ':all_market')
        assert row['state'] == 'delegated'
        assert row['source_status'] == 'delegated_to_complement_product_history'
        assert row['complete_partitions'] == 0


def test_excluded_observation_dates_do_not_block_extra_validation_forever(tmp_path):
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        conn.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
                     "VALUES ('TaiwanStockBalanceSheet','','2026-01-02','day',2,'not_observation_date')")
        conn.commit()
        assert _finmind_required(str(tmp_path / 'queue.sqlite3'), 1) == 0

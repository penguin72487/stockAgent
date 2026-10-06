from datetime import UTC, date, datetime
import json

import pytest

from downloader import download_finmind_complement as worker
from downloader.finmind_supplemental import request_contract, validate_response


TODAY = date(2026, 10, 6)
NOW = datetime(2026, 10, 6, 5, tzinfo=UTC)


@pytest.mark.parametrize('partition', ['2005-01-03', '2012-06-28', '2018-01-02'])
def test_taiex_requested_day_is_not_clamped_to_the_stock_history_floor(partition):
    endpoint, params, metadata = request_contract('TaiwanStockKBar', 'TAIEX', partition, TODAY)
    assert endpoint.endswith('/data')
    assert params == {'dataset': 'TaiwanStockKBar', 'data_id': 'TAIEX', 'start_date': partition}
    assert metadata['request_start_date'] == metadata['request_end_date'] == partition
    validate_response('TaiwanStockKBar', 'TAIEX', partition, TODAY,
                      [{'date': partition, 'stock_id': 'TAIEX', 'volume': 0}])


def test_identity_floor_does_not_expand_individual_stock_history_or_allow_wrong_days():
    with pytest.raises(ValueError, match='identity_history_floor'):
        request_contract('TaiwanStockKBar', '2330', '2018-01-02', TODAY)
    with pytest.raises(ValueError, match='outside_range'):
        validate_response('TaiwanStockKBar', 'TAIEX', '2018-01-02', TODAY,
                          [{'date': '2019-01-01', 'stock_id': 'TAIEX'}])


def receipt(root, partition, *, identifier='TAIEX', state='observed_empty', start='2019-01-01'):
    path = root / 'receipts' / f'{identifier}-{partition}.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({'dataset': 'TaiwanStockKBar', 'data_id': identifier,
                  'partition': partition, 'rows': 0, 'status': 'observed_empty',
                  'request': {'endpoint': 'data', 'request_start_date': start, 'request_end_date': partition}}))
    return str(path.relative_to(root)), path.read_bytes()


def test_only_proven_wrong_date_empty_tasks_are_requeued_once_without_touching_receipts(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        cases = [('TAIEX', '2018-01-02', 'observed_empty', '2019-01-01'),
                 ('TAIEX', '2018-01-03', 'observed_empty', '2018-01-03'),
                 ('TAIEX', '2018-01-04', 'retry_exhausted', '2019-01-01'),
                 ('2330', '2018-01-02', 'observed_empty', '2019-01-01')]
        originals = []
        for identifier, partition, state, start in cases:
            worker._add_tasks(conn, [('TaiwanStockKBar', identifier, partition, 'id_day', 8)])
            path, body = receipt(tmp_path, partition, identifier=identifier, start=start)
            conn.execute('UPDATE tasks SET state=?,receipt_path=? WHERE data_id=? AND partition=?',
                         (state, path, identifier, partition))
            originals.append((path, body))
        assert worker._repair_taiex_query_dates(conn, tmp_path, NOW) == 1
        states = conn.execute('SELECT data_id,partition,state FROM tasks ORDER BY data_id,partition').fetchall()
        assert states == [('2330', '2018-01-02', 'observed_empty'), ('TAIEX', '2018-01-02', 'pending'),
                          ('TAIEX', '2018-01-03', 'observed_empty'), ('TAIEX', '2018-01-04', 'retry_exhausted')]
        for path, body in originals:
            assert (tmp_path / path).read_bytes() == body
        assert conn.execute('SELECT COUNT(*) FROM finmind_taiex_query_date_repairs').fetchone()[0] == 1
        assert worker._repair_taiex_query_dates(conn, tmp_path, NOW) == 0


def test_invalid_receipt_proof_is_not_assumed_to_be_a_wrong_date_observation(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [('TaiwanStockKBar', 'TAIEX', '2018-01-02', 'id_day', 8)])
        conn.execute("UPDATE tasks SET state='observed_empty',receipt_path='../outside.json'")
        assert worker._repair_taiex_query_dates(conn, tmp_path, NOW) == 0
        assert conn.execute('SELECT state FROM tasks').fetchone() == ('observed_empty',)

from datetime import UTC, date, datetime, timedelta
import json
import sqlite3

import pytest

from downloader import finmind_updates as updates
from downloader.download_finmind_complement import Task

NOW = datetime(2026, 9, 30, 10, tzinfo=UTC)
TASK = Task('TaiwanStockPriceAdj', '', '2026-09-30', 'day', 0, 'pending')


def response(rows, now=NOW, request_id='first'):
    return updates.ResponseRows(rows, observation={
        'request_id': request_id, 'request_started_at_utc': (now - timedelta(seconds=2)).isoformat(),
        'response_received_at_utc': now.isoformat(), 'response_rows': len(rows), 'network_seconds': 2,
    })


def record(root, rows, now=NOW, task=TASK):
    return updates.record_success(root, task, rows, {
        'source_first_date': '2026-09-30' if rows else None,
        'source_last_date': '2026-09-30' if rows else None,
    }, now)


def test_fingerprint_is_order_independent_without_losing_duplicates_or_nulls():
    a, b = {'date': '2026-09-30', 'value': -3}, {'value': None, 'date': '2026-09-29'}
    assert updates.row_fingerprint([a, b]) == updates.row_fingerprint([b, dict(reversed(list(a.items())))])
    assert updates.row_fingerprint([a, a, b]) != updates.row_fingerprint([a, b])
    assert updates.row_fingerprint([b]) != updates.row_fingerprint([{'date': '2026-09-29'}])


def test_empty_unchanged_first_nonempty_revision_and_observed_bounds(tmp_path):
    first = record(tmp_path, response([]))
    assert first['event'] == 'first_empty' and first['last_changed_at_utc'] is None
    second_at = NOW + timedelta(minutes=1)
    unchanged = record(tmp_path, response([], second_at, 'second'), second_at)
    assert unchanged['event'] == 'unchanged' and unchanged['unchanged_checks'] == 1
    third_at = NOW + timedelta(minutes=3)
    third = record(tmp_path, response([{'date': '2026-09-30', 'close': 100}], third_at, 'third'), third_at)
    assert third['event'] == 'first_nonempty_after_empty'
    assert third['availability_after_utc'] == (second_at - timedelta(seconds=2)).isoformat()
    assert third['availability_by_utc'] == third_at.isoformat()
    assert third['provider_published_at_utc'] is None
    same_at = third_at + timedelta(minutes=5)
    same = record(tmp_path, response([{'close': 100, 'date': '2026-09-30'}], same_at, 'same'), same_at)
    assert same['last_changed_at_utc'] == third_at.isoformat()
    revised_at = same_at + timedelta(minutes=5)
    revised = record(tmp_path, response([{'close': 101, 'date': '2026-09-30'}], revised_at, 'revision'), revised_at)
    assert revised['event'] == 'revision'
    assert revised['availability_after_utc'] == (same_at - timedelta(seconds=2)).isoformat()
    head = updates.read_summary(tmp_path)['datasets'][TASK.dataset]
    assert head['checks'] == 5 and head['changes'] == 2 and head['unchanged'] == 2


def test_failed_check_does_not_erase_good_data_or_become_an_update(tmp_path):
    record(tmp_path, response([{'date': '2026-09-30', 'close': 1}]))
    updates.record_failure(tmp_path, TASK, 'http_503', NOW + timedelta(minutes=5))
    head = updates.read_summary(tmp_path)['datasets'][TASK.dataset]
    assert head['last_data_observed_at_utc'] == NOW.isoformat()
    assert head.get('last_changed_at_utc') is None
    assert head['checks'] == 1 and head['failed_checks'] == 1
    assert head['last_error_code'] == 'http_503'


def test_reverse_completion_of_different_partitions_preserves_newest_content_head(tmp_path):
    older_task = Task(TASK.dataset, '', '2026-09-29', 'day', 0, 'pending')
    for task, stamp, request_id, value in [
        (older_task, NOW, 'old-baseline', 1),
        (TASK, NOW + timedelta(minutes=1), 'new-baseline', 1),
        (TASK, NOW + timedelta(minutes=5), 'new-revision', 2),
        (older_task, NOW + timedelta(minutes=3), 'older-late-completion', 2),
    ]:
        record(tmp_path, response([{'close': value}], stamp, request_id), stamp, task=task)
    head = updates.read_summary(tmp_path)['datasets'][TASK.dataset]
    assert head['checked_at_utc'] == (NOW + timedelta(minutes=5)).isoformat()
    assert head['last_data_observed_at_utc'] == head['checked_at_utc']
    assert head['last_changed_at_utc'] == head['checked_at_utc']
    assert head['availability_after_utc'] == (NOW + timedelta(minutes=1, seconds=-2)).isoformat()
    assert head['availability_by_utc'] == head['checked_at_utc']
    assert head['checks'] == 4 and head['changes'] == 2


@pytest.mark.parametrize('failure_first', [True, False])
def test_success_failure_heads_are_order_independent(tmp_path, failure_first):
    record(tmp_path, response([{'close': 1}]))
    late_failure = NOW + timedelta(minutes=5)
    earlier_success = NOW + timedelta(minutes=3)
    def succeeded():
        record(tmp_path, response([{'close': 1}], earlier_success, 'earlier-success'), earlier_success)
    def failed():
        updates.record_failure(tmp_path, TASK, 'http_503', late_failure)
    for action in ([failed, succeeded] if failure_first else [succeeded, failed]):
        action()
    # An older failure arriving last cannot replace the later failure.
    updates.record_failure(tmp_path, TASK, 'http_500', NOW + timedelta(minutes=1))
    head = updates.read_summary(tmp_path)['datasets'][TASK.dataset]
    assert head['last_failed_check_at_utc'] == late_failure.isoformat()
    assert head['last_error_code'] == 'http_503' and head['failed_checks'] == 2
    recovered = NOW + timedelta(minutes=6)
    record(tmp_path, response([{'close': 1}], recovered, 'recovered'), recovered)
    head = updates.read_summary(tmp_path)['datasets'][TASK.dataset]
    assert head['last_error_code'] is None and head['last_failed_check_at_utc'] == late_failure.isoformat()
    updates.record_failure(tmp_path, TASK, 'http_502', NOW + timedelta(minutes=4))
    assert updates.read_summary(tmp_path)['datasets'][TASK.dataset]['last_error_code'] is None


def test_same_batch_request_is_idempotent_and_retains_transport_across_merge(tmp_path):
    original = response([{'date': '2026-09-30', 'close': 1}])
    merged = updates.retain_observation([{'date': '2026-09-29', 'close': 1}, *original], original)
    record(tmp_path, merged)
    record(tmp_path, merged)
    head = updates.read_summary(tmp_path)['datasets'][TASK.dataset]
    assert head['response_rows'] == 1 and head['rows'] == 2 and head['checks'] == 1


def test_symbols_are_independent_comparisons_and_derivations_are_not_api_updates(tmp_path):
    a = Task('USStockPrice', 'A', 'history', 'id_history', 0, 'pending')
    b = Task('USStockPrice', 'B', 'history', 'id_history', 0, 'pending')
    assert record(tmp_path, response([{'close': 1}]), task=a)['event'] == 'first_nonempty'
    assert record(tmp_path, response([{'close': 2}], request_id='b'), task=b)['event'] == 'first_nonempty'
    derived = Task('TaiwanStockInstitutionalInvestorsBuySellWide', '', '2026-09-30', 'derived', 0, 'pending')
    assert record(tmp_path, [{'buy': 2}], task=derived) is None
    historical_tick = Task('TaiwanStockPriceTick', '2330', '2018-12-07', 'id_day', 10, 'pending')
    assert record(tmp_path, [], task=historical_tick) is None


def test_public_metadata_allowlist_no_keys_paths_or_content(tmp_path):
    record(tmp_path, response([{'close': 1, 'description': 'SECRET'}]))
    with sqlite3.connect(tmp_path / updates.DB_NAME) as conn:
        head = json.loads(conn.execute('SELECT metadata_json FROM dataset_heads').fetchone()[0])
        head.update(token='SECRET', parquet_path='/private', raw_rows=['SECRET'])
        conn.execute('UPDATE dataset_heads SET metadata_json=?', (json.dumps(head),))
    public = json.dumps(updates.read_summary(tmp_path))
    assert 'SECRET' not in public and '/private' not in public
    assert 'fingerprint' not in public and 'request_id' not in public


def test_malformed_monitor_head_is_unreadable_not_a_dashboard_crash(tmp_path):
    record(tmp_path, response([{'close': 1}]))
    with sqlite3.connect(tmp_path / updates.DB_NAME) as conn:
        conn.execute("UPDATE dataset_heads SET metadata_json='null'")
    assert updates.read_summary(tmp_path) == {'state': 'unreadable', 'datasets': {}}


def test_empty_retry_detects_early_without_unbounded_polls():
    delays = [(updates.empty_retry(NOW, {'unchanged_checks': n}, default_seconds=900) - NOW).total_seconds()
              for n in range(7)]
    assert delays == [60, 120, 240, 480, 900, 900, 900]
    assert updates.empty_retry(NOW, None, default_seconds=900) == NOW + timedelta(minutes=15)


@pytest.mark.parametrize('now,expected', [
    (datetime(2026, 9, 30, 4, tzinfo=UTC), datetime(2026, 9, 30, 9, 30, tzinfo=UTC)),
    (datetime(2026, 9, 26, 4, tzinfo=UTC), datetime(2026, 9, 29, 9, 30, tzinfo=UTC)),
])
def test_current_empty_before_official_release_waits_for_boundary(tmp_path, monkeypatch, now, expected):
    from downloader import download_finmind_sponsor as sponsor
    from downloader.finmind_scheduling import TAIPEI
    from downloader.finmind_scheduling import next_release_check
    from types import SimpleNamespace
    # Deterministic official calendar fixture: Monday 9/28 is a holiday,
    # not a release merely because it is a weekday. No live repo calendar.
    def decision(day, **kwargs):
        return SimpleNamespace(status='closed' if day.isoformat() == '2026-09-28' else 'scheduled_open',
                               reason='official TWSE schedule fixture')
    monkeypatch.setattr(sponsor, 'next_release_check', lambda dataset, at: next_release_check(
        dataset, at, day_decision=decision))
    partition = now.astimezone(TAIPEI).date().isoformat()
    task = Task('TaiwanStockPrice', '', partition, 'day', 0, 'inflight')
    monkeypatch.setattr(sponsor, '_store', lambda *_args: {
        'status': 'observed_empty', 'rows': 0, 'receipt_path': 'receipts/fixture.json',
    })
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        conn.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
                     "VALUES ('TaiwanStockPrice','',?,'day',0,'inflight')", (partition,))
        sponsor._finish(conn, tmp_path, task, [], now)
        due = conn.execute("SELECT next_attempt_at_utc FROM tasks WHERE dataset='TaiwanStockPrice'").fetchone()[0]
    assert datetime.fromisoformat(due) == expected


def test_clock_regression_cannot_rewrite_observation(tmp_path):
    record(tmp_path, response([{'close': 1}]))
    with pytest.raises(ValueError, match='clock_regression'):
        record(tmp_path, response([{'close': 2}], NOW - timedelta(minutes=5), 'old'), NOW - timedelta(minutes=5))
    assert updates.read_summary(tmp_path)['datasets'][TASK.dataset]['checks'] == 1


def test_due_refresh_batch_does_not_pull_background_or_future_retries(tmp_path):
    from downloader import download_finmind_sponsor as sponsor
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        conn.executemany("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,next_attempt_at_utc) "
                         "VALUES ('TaiwanBusinessIndicator','',?,'year',?,?,?)", [
            ('2023-01-01', 2, 'pending', None),
            ('2024-01-01', 0, 'complete', NOW.isoformat()),
            ('2025-01-01', 0, 'complete', NOW.isoformat()),
            ('2026-01-01', 0, 'failed', (NOW + timedelta(hours=1)).isoformat()),
        ])
        seed = sponsor._next(conn, NOW, incremental_only=True)
        batch = sponsor._claim_batch(conn, seed, NOW, allow_history=False)
        assert [task.partition for task in batch.tasks] == ['2024-01-01', '2025-01-01']
        assert batch.params() == {'dataset': 'TaiwanBusinessIndicator', 'start_date': '2024-01-01', 'end_date': '2025-12-31'}
        assert conn.execute("SELECT state FROM tasks WHERE partition='2023-01-01'").fetchone()[0] == 'pending'
        assert conn.execute("SELECT state FROM tasks WHERE partition='2026-01-01'").fetchone()[0] == 'failed'


def test_official_release_clock_and_supplemental_minute_boundary(tmp_path):
    from downloader.finmind_scheduling import SPECS, next_release_check
    from downloader import download_finmind_complement as worker
    from downloader import finmind_supplemental as supplemental
    assert (SPECS['TaiwanStockPriceAdj'].release_hour, SPECS['TaiwanStockPriceAdj'].release_minute) == (17, 30)
    assert SPECS['TaiwanStockInstitutionalInvestorsBuySell'].release_hour == 20
    assert next_release_check('TaiwanStockPriceAdj', NOW) == datetime(2026, 10, 1, 9, 30, tzinfo=UTC)
    before = datetime(2026, 9, 30, 7, 49, tzinfo=UTC)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        supplemental.seed(conn, {'stocks': ['2330']}, before)
        assert conn.execute("SELECT 1 FROM tasks WHERE dataset='TaiwanStockKBar' AND partition='2026-09-30'").fetchone() is None
        supplemental.seed(conn, {'stocks': ['2330']}, before + timedelta(minutes=1))
        assert conn.execute("SELECT 1 FROM tasks WHERE dataset='TaiwanStockKBar' AND partition='2026-09-30'").fetchone()


def test_day_trading_final_phase_has_no_phantom_overnight_refresh():
    from downloader.finmind_scheduling import next_release_check, release_details
    assert next_release_check('TaiwanStockDayTrading', NOW) == datetime(2026, 9, 30, 13, 30, tzinfo=UTC)
    after_final = datetime(2026, 9, 30, 13, 37, tzinfo=UTC)
    assert next_release_check('TaiwanStockDayTrading', after_final) == datetime(2026, 10, 1, 10, tzinfo=UTC)
    assert release_details('TaiwanStockDayTrading')['phases'][0]['basis'] == 'existing_poll_policy_not_publication'


def test_final_phase_forecast_is_not_counted_again_as_four_hour_refresh(tmp_path, monkeypatch):
    from downloader.finmind_eta_telemetry import timed_incremental_forecast
    from scripts import audit_finmind_query_ranges
    from test_finmind_eta_work import _database
    dataset = 'TaiwanStockDayTrading'
    monkeypatch.setattr(audit_finmind_query_ranges, 'registry', lambda: {
        dataset: {'primary_owner': 'sponsor', 'query_shape': 'whole_market_day'},
    })
    after_final = datetime(2026, 9, 30, 13, 37, tzinfo=UTC)
    _database(tmp_path, 'sponsor', [(dataset, '', '2026-09-30', 'day', 0, 'complete',
                                   (after_final + timedelta(hours=4)).isoformat())])
    _database(tmp_path, 'complement', [])
    model = timed_incremental_forecast(tmp_path, after_final)
    assert model['state'] == 'modeled'
    phases = {row['first_at_utc']: row for row in model['events']}
    assert phases['2026-10-01T10:00:00+00:00']['requests'] == 1
    assert phases['2026-10-01T13:30:00+00:00']['requests'] == 1
    assert all(row['interval_seconds'] != 4 * 3600 for row in model['events'])


def test_live_deadline_migration_catches_final_fields_and_keeps_old_receipts(tmp_path):
    from downloader import download_finmind_sponsor as sponsor
    from downloader.finmind_scheduling import reconcile_release_deadlines
    now = datetime(2026, 9, 30, 13, tzinfo=UTC)  # 21:00 Taipei
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        conn.executemany("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,last_attempt_at_utc,next_attempt_at_utc,receipt_path) "
                         "VALUES (?,'',?,'day',0,?,?,?,'receipts/old.json')", [
            ('TaiwanStockDayTrading', '2026-09-30', 'complete', '2026-09-30T10:00:00+00:00', '2026-09-30T14:00:00+00:00'),
            ('TaiwanStockInstitutionalInvestorsBuySell', '2026-09-30', 'complete', '2026-09-30T10:00:00+00:00', '2026-09-30T14:00:00+00:00'),
            ('TaiwanStockPriceAdj', '2026-09-29', 'complete', '2026-09-29T10:00:00+00:00', None),
        ])
        reconcile_release_deadlines(conn, now)
        deadlines = dict(conn.execute('SELECT dataset,next_attempt_at_utc FROM tasks'))
        assert deadlines['TaiwanStockDayTrading'] == '2026-09-30T13:30:00+00:00'
        assert deadlines['TaiwanStockInstitutionalInvestorsBuySell'] == '2026-09-30T12:00:00+00:00'
        assert deadlines['TaiwanStockPriceAdj'] is None
        assert conn.execute('SELECT count(*) FROM finmind_release_clock_migrations').fetchone()[0] == 2
        assert {row[0] for row in conn.execute('SELECT receipt_path FROM tasks')} == {'receipts/old.json'}
        reconcile_release_deadlines(conn, now)
        assert conn.execute('SELECT count(*) FROM finmind_release_clock_migrations').fetchone()[0] == 2


def test_public_projection_has_complete_monitor_fields_without_provider_requests(tmp_path, monkeypatch):
    from stockagent.live.finmind_dashboard import build_finmind_public_status
    root = tmp_path / 'data_finmind' / 'sponsor'
    root.mkdir(parents=True)
    record(root, response([{'date': '2026-09-30', 'close': 1}]))
    import requests
    monkeypatch.setattr(requests.Session, 'get', lambda *_a, **_k: pytest.fail('dashboard made provider call'))
    result = build_finmind_public_status(tmp_path, now=NOW)
    assert all('update_monitor' in row for row in result['datasets'])
    row = next(row for row in result['datasets'] if row['id'] == 'TaiwanStockPriceAdj:all_market')
    assert row['update_monitor']['checked_at_utc'] == NOW.isoformat()
    assert row['update_monitor']['release']['label'] == '週一至五 17:30'
    assert result['update_monitor']['extra_provider_calls'] == 0
    assert 'request_id' not in json.dumps(result)


def test_delegated_alias_uses_actual_owner_and_local_derivation_is_not_api_update(tmp_path):
    from stockagent.live.finmind_dashboard import build_finmind_public_status
    root = tmp_path / 'data_finmind'
    (root / 'complement').mkdir(parents=True)
    (root / 'complement' / 'status.json').write_text(json.dumps({
        'delegated_to_sponsor': ['TaiwanStockPriceAdj', 'TaiwanStockInstitutionalInvestorsBuySellWide'],
    }))
    record(root / 'sponsor', response([{'date': '2026-09-30', 'close': 1}]))
    institution = Task('TaiwanStockInstitutionalInvestorsBuySell', '', '2026-09-30', 'day', 0, 'pending')
    record(root / 'sponsor', response([{'date': '2026-09-30', 'buy': 2}], request_id='institution'), task=institution)
    result = build_finmind_public_status(tmp_path, now=NOW)
    rows = {row['id']: row for row in result['datasets']}
    assert rows['TaiwanStockPriceAdj']['update_monitor'] == rows['TaiwanStockPriceAdj:all_market']['update_monitor']
    wide = rows['TaiwanStockInstitutionalInvestorsBuySellWide:all_market']['update_monitor']
    assert wide['state'] == 'derived_local' and wide['derived_from'] == institution.dataset
    assert wide['checked_at_utc'] == NOW.isoformat()
    assert wide['local_materialized_at_utc'] is None  # No synthetic derivation time.


def test_delegated_settlement_alias_uses_complement_product_owner(tmp_path):
    from stockagent.live.finmind_dashboard import build_finmind_public_status
    task = Task('TaiwanFuturesFinalSettlementPrice', 'TX', 'history', 'id_history', 0, 'pending')
    record(tmp_path / 'data_finmind' / 'complement', response([{'date': '2026-09-30', 'price': 1}]), task=task)
    result = build_finmind_public_status(tmp_path, now=NOW)
    rows = [row for row in result['datasets'] if row['id'].split(':')[0] == task.dataset]
    assert len(rows) >= 2
    assert all(row['update_monitor']['owner'] == 'complement' for row in rows)
    assert all(row['update_monitor']['checked_at_utc'] == NOW.isoformat() for row in rows)


def test_inventory_deadline_comes_from_queue_not_old_observation(tmp_path):
    from scripts.audit_finmind_query_ranges import build_inventory
    from downloader import download_finmind_sponsor as sponsor
    root = tmp_path / 'data_finmind'
    worker_root = root / 'sponsor'
    record(worker_root, response([{'date': '2026-09-30', 'close': 1}]))
    old = (NOW + timedelta(hours=4)).isoformat()
    new = datetime(2026, 10, 1, 9, 30, tzinfo=UTC).isoformat()
    updates.set_next_check(worker_root, TASK, old)
    with sponsor._db(worker_root / 'queue.sqlite3') as conn:
        conn.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,next_attempt_at_utc) "
                     "VALUES (?,'',?,'day',0,'complete',?)", (TASK.dataset, TASK.partition, new))
    report = build_inventory(root, NOW)
    row = next(row for row in report['datasets'] if row['dataset'] == TASK.dataset)
    assert row['next_check_at_utc'] == new
    assert row['update_observation']['next_check_at_utc'] == new

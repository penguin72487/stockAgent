from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
import json
import sqlite3

import pytest

from downloader import finmind_eta_telemetry as telemetry


NOW = datetime(2026, 9, 27, 4, 7, tzinfo=UTC)


def _traffic(root, stamps):
    with sqlite3.connect(root / "request_traffic.sqlite3") as conn:
        conn.execute("CREATE TABLE requests(started_at_utc TEXT,dataset TEXT)")
        conn.execute("CREATE INDEX idx_finmind_requests_started ON requests(started_at_utc)")
        conn.executemany("INSERT INTO requests VALUES (?,?)", [(stamp.isoformat(), "A") for stamp in stamps])


def _account(root, **changes):
    value = {"tier": "Sponsor", "observed_at_utc": NOW.isoformat(),
             "official_requests_per_hour": 6000, "provider_used_in_hour": 5000}
    value.update(changes)
    (root / "account_status.json").write_text(json.dumps(value))


@pytest.fixture(autouse=True)
def _fixed_demand(monkeypatch):
    monkeypatch.setattr(telemetry, "incremental_reservation", lambda root, now: {
        'reserve_requests': 10, 'ready_requests': 0, 'queue_errors': [],
        'observed_at_utc': now.isoformat()})


def test_complete_active_bins_keep_low_rates_and_report_idle(tmp_path):
    starts = [NOW - timedelta(days=2)]
    # The complete recent hour is 03:00--04:00, not the partial 04:00--04:07.
    for minute, count in [(0, 1), (15, 10), (30, 0), (45, 100)]:
        stamp = NOW.replace(hour=3, minute=minute, second=1)
        starts.extend([stamp] * count)
    starts.extend([NOW.replace(minute=1)] * 200)
    _traffic(tmp_path, starts)
    _account(tmp_path)
    out = telemetry.build_finmind_eta_telemetry(tmp_path, NOW)
    rates = out["traffic"]["rate_distributions"]["1h"]
    assert rates["complete_bin_count"] == 4
    assert rates["active_bin_count"] == 3
    assert rates["idle_bin_count"] == 1
    assert rates["minimum"] == 4
    assert rates["p50"] == 40
    assert rates["maximum"] == 400
    assert rates['wall_p10'] == pytest.approx(1.2)
    assert out['traffic']['windows']['1h']['attempts'] == 310
    assert out['traffic']['windows']['1h']['wall_requests_per_hour'] == 310
    assert out['traffic']['windows']['1h']['ended_at_utc'] == NOW.isoformat()
    assert out["quota"]["reserved_requests_per_hour"] == 12
    assert out["quota"]["backfill_capacity_requests_per_hour"] == 5988
    assert out["quota"]["paced_requests_per_hour"] == 6000
    assert out["traffic"]["success_rate"] is None


def test_short_instrumentation_does_not_extrapolate_partial_bin(tmp_path):
    _traffic(tmp_path, [NOW - timedelta(minutes=3), NOW - timedelta(minutes=1)])
    _account(tmp_path)
    out = telemetry.build_finmind_eta_telemetry(tmp_path, NOW)
    assert out["traffic"]["rate_distributions"]["1h"]["active_bin_count"] == 0
    assert out["traffic"]["rate_distributions"]["24h"]["p50"] is None
    assert out["traffic"]["windows"]["1h"]["complete_window"] is False
    assert out["traffic"]["windows"]["1h"]["attempts"] == 2


def test_recurring_model_separates_worker_priorities_and_new_daily_arrivals(tmp_path, monkeypatch):
    from scripts import audit_finmind_query_ranges
    from test_finmind_eta_work import _database

    datasets = ('TaiwanFuturesKBar', 'TaiwanFuturesTick')
    catalog = {name: {'primary_owner': 'complement', 'query_shape': 'per_id_day'} for name in datasets}
    catalog['TaiwanStockPrice'] = {'primary_owner': 'sponsor', 'query_shape': 'whole_market_day'}
    monkeypatch.setattr(audit_finmind_query_ranges, 'registry', lambda: catalog)
    _database(tmp_path, 'sponsor', [])
    path = _database(tmp_path, 'complement', [
        ('TaiwanFuturesKBar', 'TX', '2020-01-02', 'id_day', 8, 'complete', NOW.isoformat()),
        ('TaiwanFuturesTick', 'TX', '2020-01-02', 'id_day', 10, 'observed_empty', NOW.isoformat()),
    ])
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE finmind_source_frontiers(dataset TEXT,data_id TEXT)')
        conn.executemany('INSERT INTO finmind_source_frontiers VALUES (?,?)',
                         [('TaiwanFuturesKBar', 'TX'), ('TaiwanFuturesKBar', 'MTX'),
                          ('TaiwanFuturesTick', 'TX'), ('Delegated', 'TX')])
    before = path.read_bytes()
    model = telemetry.recurring_forecast(tmp_path)
    assert model['state'] == 'modeled'
    parts = model['requests_per_hour_by_phase']
    assert parts['incremental'] == pytest.approx(3 + 5 / 24)
    assert parts['core'] == pytest.approx(1 / (365 * 24) + 1 / (90 * 24))
    assert parts['detail'] == pytest.approx(2 / 24)
    assert parts['tick'] == pytest.approx(1 / 24)
    assert model['requests_per_hour_by_stage']['validation'] == parts['core']
    assert model['new_partition_requests_per_hour_by_phase'] == {'core': 0, 'detail': 2 / 24, 'tick': 1 / 24}
    assert path.read_bytes() == before


def test_recurring_market_day_ignores_archived_per_id_frontiers(tmp_path, monkeypatch):
    from scripts import audit_finmind_query_ranges
    from test_finmind_eta_work import _database

    dataset = 'TaiwanFuturesSpreadTick'
    monkeypatch.setattr(audit_finmind_query_ranges, 'registry', lambda: {
        dataset: {'primary_owner': 'complement', 'query_shape': 'whole_market_day'}})
    _database(tmp_path, 'sponsor', [])
    path = _database(tmp_path, 'complement', [])
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE finmind_source_frontiers(dataset TEXT,data_id TEXT)')
        conn.executemany('INSERT INTO finmind_source_frontiers VALUES (?,?)',
                         [(dataset, identifier) for identifier in ('', 'TX', 'MTX', 'CAF')])
    before = path.read_bytes()
    model = telemetry.recurring_forecast(tmp_path)
    assert model['new_partition_requests_per_hour_by_phase']['tick'] == 1 / 24
    assert model['requests_per_hour_by_phase']['tick'] == 1 / 24
    assert path.read_bytes() == before


def test_future_events_are_not_rate_evidence(tmp_path):
    _traffic(tmp_path, [NOW - timedelta(days=2), NOW + timedelta(minutes=1)])
    out = telemetry.build_finmind_eta_telemetry(tmp_path, NOW)
    assert out["traffic"]["windows"]["1h"]["attempts"] == 0
    assert out["traffic"]["future_timestamp_count"] == 1


def test_stale_account_has_no_usable_backfill_ceiling(tmp_path):
    _traffic(tmp_path, [NOW - timedelta(hours=2)])
    _account(tmp_path, observed_at_utc=(NOW - timedelta(minutes=6)).isoformat())
    out = telemetry.build_finmind_eta_telemetry(tmp_path, NOW)
    assert out["state"] == "insufficient_evidence"
    assert out["quota"]["official_requests_per_hour"] == 6000
    assert out["quota"]["backfill_capacity_requests_per_hour"] is None
    assert out["quota"]["current_budget"]["allowed"] is False


def test_missing_inputs_are_not_created(tmp_path):
    out = telemetry.build_finmind_eta_telemetry(tmp_path, NOW)
    assert out["state"] == "insufficient_evidence"
    assert out["traffic"]["state"] == "missing"
    assert out["workers"]["sponsor"]["state"] == "missing"
    assert list(tmp_path.iterdir()) == []


def test_latest_partition_outcomes_not_network_success_rate(tmp_path):
    root = tmp_path / "sponsor"
    root.mkdir()
    with sqlite3.connect(root / "queue.sqlite3") as conn:
        conn.execute("CREATE TABLE tasks(state TEXT,kind TEXT,rows INTEGER,bytes INTEGER,last_attempt_at_utc TEXT)")
        conn.executemany("INSERT INTO tasks VALUES (?,?,?,?,?)", [
            ("complete", "year", 10, 100, (NOW - timedelta(minutes=2)).isoformat()),
            ("complete", "derived", 20, 200, (NOW - timedelta(minutes=3)).isoformat()),
            ("observed_empty", "day", 0, 0, (NOW - timedelta(minutes=4)).isoformat()),
            ("failed", "day", 5, 50, (NOW - timedelta(minutes=5)).isoformat()),
        ])
        conn.execute("CREATE TABLE request_batch_failure_audit(error_code TEXT,failed_at_utc TEXT)")
        conn.execute("INSERT INTO request_batch_failure_audit VALUES (?,?)", ("http_504", NOW.isoformat()))
    out = telemetry.build_finmind_eta_telemetry(tmp_path, NOW)
    worker = out["workers"]["sponsor"]
    last_hour = worker["recent_outcomes_by_window"]["1h"]
    assert last_hour["successful_latest_partitions"] == 3
    assert last_hour["derived_latest_partitions"] == 1
    assert last_hour["failed_latest_partitions"] == 1
    assert last_hour["rows_in_successful_latest_partitions"] == 30
    assert last_hour["not_a_network_completion_count"] is True
    assert worker["batch_failure_audit"]["events_7d_by_code"] == {"http_504": 1}


def test_invalid_database_is_unknown_without_repair(tmp_path):
    path = tmp_path / "request_traffic.sqlite3"
    path.write_bytes(b"invalid database")
    before = path.read_bytes()
    out = telemetry.build_finmind_eta_telemetry(tmp_path, NOW)
    assert out["traffic"]["state"] == "unavailable"
    assert path.read_bytes() == before


def test_naive_now_rejected(tmp_path):
    with pytest.raises(ValueError, match="timezone aware"):
        telemetry.build_finmind_eta_telemetry(tmp_path, NOW.replace(tzinfo=None))


def test_unknown_account_fields_never_exposed(tmp_path):
    _account(tmp_path, token="do-not-echo", email="private@example.invalid", arbitrary="secret")
    rendered = json.dumps(telemetry.build_finmind_eta_telemetry(tmp_path, NOW))
    assert "do-not-echo" not in rendered
    assert "private@example.invalid" not in rendered
    assert "secret" not in rendered


@pytest.mark.parametrize("limit", [None, True, 0, -1, 100001, "6000"])
def test_invalid_quota_is_not_usable_evidence(tmp_path, limit):
    _traffic(tmp_path, [NOW - timedelta(days=1)])
    _account(tmp_path, official_requests_per_hour=limit)
    out = telemetry.build_finmind_eta_telemetry(tmp_path, NOW)
    assert out["state"] == "insufficient_evidence"
    assert out["quota"]["backfill_capacity_requests_per_hour"] is None


def test_read_connection_cannot_mutate_queue(tmp_path):
    _traffic(tmp_path, [NOW])
    with telemetry._connect(tmp_path / "request_traffic.sqlite3") as conn:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute("DELETE FROM requests")


def test_traffic_extrema_use_two_covering_index_seeks(tmp_path, monkeypatch):
    first, last = NOW - timedelta(days=300), NOW - timedelta(seconds=1)
    _traffic(tmp_path, [last, NOW + timedelta(days=1), first])
    statements = []
    original_connect = telemetry._connect

    @contextmanager
    def traced(path):
        with original_connect(path) as connection:
            connection.set_trace_callback(statements.append)
            yield connection

    monkeypatch.setattr(telemetry, "_connect", traced)
    result = telemetry.build_finmind_eta_telemetry(tmp_path, NOW)["traffic"]
    assert result["tracking_started_at_utc"] == first.isoformat()
    assert result["last_attempt_at_utc"] == last.isoformat()
    endpoint_queries = [sql for sql in statements
                        if sql.startswith("SELECT started_at_utc FROM requests") and "LIMIT 1" in sql]
    assert len(endpoint_queries) == 2
    with original_connect(tmp_path / "request_traffic.sqlite3") as connection:
        for query in endpoint_queries:
            plan = " ".join(row[3] for row in connection.execute("EXPLAIN QUERY PLAN " + query))
            assert "COVERING INDEX idx_finmind_requests_started" in plan
            assert "TEMP B-TREE" not in plan


def test_empty_existing_ledger_has_no_extrema(tmp_path):
    _traffic(tmp_path, [])
    result = telemetry.build_finmind_eta_telemetry(tmp_path, NOW)["traffic"]
    assert result["state"] == "empty"
    assert result["tracking_started_at_utc"] is None
    assert result["last_attempt_at_utc"] is None

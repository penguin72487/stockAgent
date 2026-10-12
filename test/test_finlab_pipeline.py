from datetime import UTC, date, datetime, timedelta
import json
from multiprocessing.pool import ThreadPool
import threading
import time
from unittest.mock import Mock, patch
from types import SimpleNamespace

import pandas as pd
import polars as pl
import pytest
from finlab.exceptions import DataError

from scripts import download_finlab_market_intraday as market
from scripts.download_finlab_intraday import (
    _partition_paths, _stored_receipt, fetch_partition, fetch_partitions, initialize_intraday_worker,
    missing_window_dates, persist_partition_frame, record_failed_partition, WorkerHTTPTransport,
)


def frame(symbol, days):
    return pd.DataFrame({
        "stock_id": [symbol] * len(days), "trade_date": [day.isoformat() for day in days],
        "timestamp": pd.to_datetime([f"{day}T09:00:00+08:00" for day in days]),
        "open": [100.0] * len(days), "high": [101.0] * len(days), "low": [99.0] * len(days),
        "close": [100.5] * len(days), "volume": [1000] * len(days),
    })


def test_range_uses_one_official_call_and_canonical_per_day_receipts(tmp_path):
    days = [date(2026, 6, 1), date(2026, 6, 2)]
    with patch("finlab.data.get", return_value=frame("2330", days)) as get:
        result = fetch_partitions(tmp_path, "tw_minute:2330", days, now=datetime.now(UTC))
    assert result["sdk_calls"] == 1 and result["fallback_calls"] == 0
    assert get.call_args.kwargs["start"] == "2026-06-01" and get.call_args.kwargs["end"] == "2026-06-02"
    assert get.call_args.kwargs["force_download"] is False
    for day, outcome in zip(days, result["outcomes"]):
        receipt = _stored_receipt(_partition_paths(tmp_path, "tw_minute:2330", day)[0], tmp_path, "tw_minute:2330", day)
        assert receipt == outcome["receipt"] and receipt["rows"] == 1


def test_missing_range_falls_back_without_marking_published_days_empty(tmp_path):
    days = [date(2026, 6, 1), date(2026, 6, 2)]
    with patch("finlab.data.get", side_effect=[
        DataError("not_ready: dates missing"), frame("2330", days[:1]), DataError("not_ready: missing"),
    ]):
        result = fetch_partitions(tmp_path, "tw_minute:2330", days, now=datetime.now(UTC))
    assert result["sdk_calls"] == 3 and result["fallback_calls"] == 2
    assert "receipt" in result["outcomes"][0]
    assert result["outcomes"][1]["failure"] == "partition_not_ready"


def test_window_auth_and_quota_failures_never_trigger_immediate_repeated_download(tmp_path):
    days = [date(2026, 6, 1), date(2026, 6, 2)]
    with patch("finlab.data.get", side_effect=DataError("quota exceeded")) as get:
        result = fetch_partitions(tmp_path, "tw_minute:2330", days, now=datetime.now(UTC))
    assert get.call_count == 1 and result["fallback_calls"] == 0
    assert all(outcome["failure"] == "quota_exhausted" for outcome in result["outcomes"])


@pytest.mark.parametrize("message", ["quota exceeded", "rate_limited: fixture shared cooldown"])
def test_fallback_account_or_source_error_preserves_unrequested_suffix(tmp_path, message):
    days = [date(2026, 6, 1) + timedelta(days=i) for i in range(3)]
    with patch("finlab.data.get", side_effect=[DataError("not_ready: missing"), DataError(message)]) as get:
        result = fetch_partitions(tmp_path, "tw_minute:2330", days, now=datetime.now(UTC))
    assert get.call_count == 2 and len(result["outcomes"]) == 1
    assert not _partition_paths(tmp_path, "tw_minute:2330", days[-1])[1].exists()


def test_range_rejects_unexpected_date_before_any_commit(tmp_path):
    days = [date(2026, 6, 1), date(2026, 6, 2)]
    with patch("finlab.data.get", return_value=frame("2330", [*days, date(2026, 6, 3)])):
        result = fetch_partitions(tmp_path, "tw_minute:2330", days, now=datetime.now(UTC))
    assert all("failure" in outcome for outcome in result["outcomes"])
    assert not (tmp_path / "intraday/receipts").exists()


def test_range_preserves_explicit_no_trade_evidence(tmp_path):
    days = [date(2026, 6, 1), date(2026, 6, 2)]
    source = frame("2330", days[:1])
    source.attrs["no_trade_dates"] = [days[1].isoformat()]
    with patch("finlab.data.get", return_value=source):
        result = fetch_partitions(tmp_path, "tw_minute:2330", days, now=datetime.now(UTC))
    assert result["outcomes"][1]["receipt"]["status"] == "verified_no_trade"


def test_explicit_missing_dates_rescue_ready_windows_not_every_individual_day(tmp_path):
    days = [date(2026, 6, 1) + timedelta(days=i) for i in range(5)]
    with patch("finlab.data.get", side_effect=[
        DataError("not_ready: source missing ['2026-06-03']"), frame("2330", days[:2]), frame("2330", days[3:]),
    ]) as get:
        result = fetch_partitions(tmp_path, "tw_minute:2330", days, now=datetime.now(UTC))
    assert get.call_count == result["sdk_calls"] == 3 and result["fallback_calls"] == 2
    assert sum("receipt" in item for item in result["outcomes"]) == 4
    assert result["outcomes"][2]["failure"] == "partition_not_ready"


@pytest.mark.parametrize("detail", ["['2026-06-99']", "['1999-01-01']", "[1]", "[]", "{bad}", "['2026-06-01'] trailing"])
def test_missing_date_parser_never_guesses_from_unverified_details(detail):
    assert missing_window_dates(DataError("not_ready: " + detail), [date(2026, 6, 1)]) is None


def test_range_values_and_per_day_contract_match_single_day_path(tmp_path):
    days = [date(2026, 6, 1), date(2026, 6, 2)]
    source = frame("2330", days)
    source.attrs = {"dataset": "tw_minute:2330", "volume_unit": "provider_native",
                    "closed_dates": ["2026-05-31"], "no_trade_dates": []}
    with patch("finlab.data.get", return_value=source):
        ranged = fetch_partitions(tmp_path / "range", "tw_minute:2330", days, now=datetime.now(UTC))
    for day, outcome in zip(days, ranged["outcomes"]):
        part = source.loc[source["trade_date"] == str(day)].copy()
        part.attrs = {**source.attrs, "closed_dates": []}  # Actual single-date SDK metadata.
        with patch("finlab.data.get", return_value=part):
            single = fetch_partition(tmp_path / "single", "tw_minute:2330", day, now=datetime.now(UTC))
        receipt = outcome["receipt"]
        pd.testing.assert_frame_equal(pd.read_parquet(tmp_path / "range" / receipt["parquet_path"]),
                                      pd.read_parquet(tmp_path / "single" / single["parquet_path"]))
        assert receipt["sha256"] == single["sha256"]
        assert receipt["publication_time_status"] == single["publication_time_status"]


def test_window_planner_obeys_calendar_cap_cached_holes_and_stock_day_budget():
    days = [date(2026, 6, 1) + timedelta(days=i) for i in range(40)]
    result = market.plan_minute_window(days[-1], days, lower_bound=days[0], calendar_days=31,
                                       limit=100, available=lambda day: True)
    assert len(result) == 31 and (max(result) - min(result)).days == 30
    assert market.plan_minute_window(days[-1], days, lower_bound=days[0], calendar_days=31,
                                    limit=2, available=lambda day: True) == days[-2:]
    assert market.plan_minute_window(days[-1], days, lower_bound=days[0], calendar_days=31,
                                    limit=100, available=lambda day: day != days[-3]) == days[-2:]


@pytest.fixture
def universe(tmp_path, monkeypatch):
    root, public = tmp_path / "finlab", tmp_path / "public"
    root.mkdir()
    stocks = public / "stocks"
    stocks.mkdir(parents=True)
    days = [date(2026, 9, 21) + timedelta(days=i) for i in range(5)]
    pl.DataFrame({"code": ["2330", "2317"], "name": ["A", "B"], "market": ["twse", "twse"],
                  "security_type": ["stock", "stock"], "source": ["official", "official"]}).write_csv(stocks / "symbols.csv")
    for symbol in ("2330", "2317"):
        pl.DataFrame({"date": [days[0], days[-1]]}).write_parquet(stocks / f"{symbol}_features.parquet")
    pl.DataFrame({"公司代號": ["2330", "2317"], "date": [str(days[-1])] * 2}).write_parquet(public / "twse_listed_company_basic.parquet")
    pl.DataFrame({"SecuritiesCompanyCode": ["9999"], "date": [str(days[-1])]}).write_parquet(public / "tpex_basic_company.parquet")
    for venue in ("twse", "tpex"):
        pl.DataFrame(schema={"symbol": pl.String, "date": pl.String}).write_parquet(public / f"{venue}_delisted_company.parquet")
    pl.DataFrame({"date": list(map(str, days))}).write_parquet(public / "twse_market_index.parquet")
    seed = persist_partition_frame(root, "tw_minute:2330", days[0], frame("2330", days[:1]))
    db = market._open_index(root / "intraday/market_index.sqlite3")
    market._record_source(db, root, "tw_minute", "2330", days[0], seed)
    db.close()
    monkeypatch.setattr(market, "credential_available", lambda: True)
    monkeypatch.setattr(market, "_general_work_admission", lambda *a, **kw: {"minute_residual_allowed": True})
    monkeypatch.setattr(market, "quota_room_mb", lambda: (700, 5000))
    monkeypatch.setattr(market, "_minute_pool", lambda workers, **kw: ThreadPool(workers))
    monkeypatch.setattr("finlab.data.get", lambda *a, **kw: pytest.fail("pool semantic tests must not call the provider"))
    return root, public, days


def fake_range(root, key, days, *, now):
    time.sleep(0.01)
    return {"outcomes": [{"trade_date": str(day), "receipt": persist_partition_frame(
        root, key, day, frame(key.split(":")[1], [day]))} for day in days],
        "sdk_calls": 1, "fallback_calls": 0, "source_seconds": 0.01, "persist_seconds": 0.01}


def run(universe, *, workers=4, limit=8, fetch=fake_range, queue_depth=1, range_days=31):
    root, public, days = universe
    return market.sync_market(root, public, start=days[0], end=days[-1], limit=limit, reserve_mb=50,
        minimum_free_gb=0, now=datetime.now(UTC), kind="tw_minute", workers=workers,
        range_calendar_days=range_days, range_fetch=fetch, queue_depth=queue_depth, download_threads=1)


def test_rolling_pool_refills_a_free_slot_before_slowest_stock_finishes(universe):
    events = []
    lock = threading.Lock()
    slow = ("tw_minute:2317", universe[2][-1])

    def fetch(root, key, days, **kw):
        with lock:
            events.append(("start", key, days[0], time.monotonic()))
        time.sleep(0.25 if (key, days[0]) == slow else 0.01)
        result = fake_range(root, key, days, **kw)
        with lock:
            events.append(("end", key, days[0], time.monotonic()))
        return result

    result = run(universe, workers=2, limit=8, queue_depth=4, range_days=1, fetch=fetch)
    assert result["successes_this_run"] == 8
    slow_end = next(at for kind, key, day, at in events if kind == "end" and (key, day) == slow)
    starts_before = [row for row in events if row[0] == "start" and row[3] < slow_end]
    assert len(starts_before) >= 3  # Not just the original two-job wave.
    root = universe[0]
    metrics = json.loads((root / "intraday/batch_metrics.json").read_text())["batches"][-1]
    assert metrics["processing_engine"] == "rolling_range_process_pool"
    assert metrics["peak_inflight_windows"] == 2 and metrics["queue_depth"] == 4
    assert metrics["worker_slot_seconds"] > 0


def test_rolling_preemption_preserves_unsent_frontier_and_reuses_completed_receipts(universe, monkeypatch):
    calls = []

    def admission(*a, **kw):
        return {"minute_residual_allowed": len(calls) < 2}

    def fetch(root, key, days, **kw):
        calls.extend((key, day) for day in days)
        return fake_range(root, key, days, **kw)

    monkeypatch.setattr(market, "_general_work_admission", admission)
    # Force an authoritative recheck at every dispatch, without real minute sleeps.
    original = market.datetime

    class Clock(datetime):
        counter = 0

        @classmethod
        def now(cls, tz=None):
            cls.counter += 1
            stamp = original.now(UTC) + timedelta(minutes=cls.counter)
            return stamp.astimezone(tz) if tz else stamp.replace(tzinfo=None)

    monkeypatch.setattr(market, "datetime", Clock)
    result = run(universe, workers=1, queue_depth=4, range_days=1, fetch=fetch)
    assert result["state"] == "waiting_required_general_data"
    assert 0 < result["successes_this_run"] < 8
    assert json.loads((universe[0] / "intraday/minute_cursor.json").read_text())["recent_queue"][0]["next"] == 0
    completed = set(calls)
    calls.clear()
    monkeypatch.setattr(market, "_general_work_admission", lambda *a, **kw: {"minute_residual_allowed": True})
    run(universe, workers=1, queue_depth=4, range_days=1, fetch=fetch)
    assert calls and len(calls) == len(set(calls)) and not completed.intersection(calls)


def test_persisted_source_cooldown_makes_no_provider_call(universe):
    root = universe[0]
    (root / "intraday/source_backoff.json").write_text(json.dumps({
        "contract_version": 1, "retry_at_utc": (datetime.now(UTC) + timedelta(minutes=15)).isoformat(),
    }))
    result = run(universe, queue_depth=4, fetch=lambda *a, **kw: pytest.fail("source cooldown"))
    assert result["state"] == "source_rate_limited" and result["attempted_this_run"] == 0


def test_explicit_serial_collector_does_not_inherit_deployed_parallel_queue(universe, monkeypatch):
    from stockagent.data.finlab_quota_policy import QuotaPolicy
    root, public, days = universe
    monkeypatch.setattr(market, "_minute_pool", lambda *a, **kw: pytest.fail("serial caller must not spawn SDK workers"))
    calls = []

    def fetch(root, key, day, **kw):
        calls.append((key, day))
        return persist_partition_frame(root, key, day, frame(key.split(":")[1], [day]))

    result = market.sync_market(root, public, start=days[0], end=days[-1], limit=4,
        reserve_mb=50, minimum_free_gb=0, now=datetime.now(UTC), kind="tw_minute", fetch=fetch,
        quota_policy=QuotaPolicy(minute_workers=32, minute_queue_depth=4, minute_authorization_slots=32))
    assert result["successes_this_run"] == 4 and len(calls) == 4


def test_conflict_repair_groups_oldest_written_days_into_one_range(universe):
    root, _, days = universe
    db = market._open_index(root / "intraday/market_index.sqlite3")
    for offset, day in enumerate(days):
        status = record_failed_partition(root, "tw_minute:2317", day, DataError("request_conflict: fixture"))
        path = _partition_paths(root, "tw_minute:2317", day)[1]
        attempt = json.loads(path.read_text())
        old = datetime.now(UTC) - timedelta(minutes=2) + timedelta(seconds=offset)
        attempt.update(attempted_at_utc=old.isoformat(), retry_at_utc=(old+timedelta(seconds=30)).isoformat())
        path.write_text(json.dumps(attempt))
        market._record(db, "tw_minute", "2317", day, status)
        db.execute("UPDATE partitions SET checked_at_utc=? WHERE kind='tw_minute' AND symbol='2317' AND trade_date=?",
                   (old.isoformat(), day.isoformat()))
        db.commit()
    db.close()
    calls = []

    def fetch(root, key, targets, **kw):
        calls.append((key, targets))
        return fake_range(root, key, targets, **kw)

    result = run(universe, limit=5, fetch=fetch, queue_depth=4)
    assert result["successes_this_run"] == 5
    assert calls == [("tw_minute:2317", days)]


def test_due_unpublished_days_share_one_native_range_without_retrying_fresh_days(universe):
    root, _, days = universe
    db = market._open_index(root / "intraday/market_index.sqlite3")
    for offset, day in enumerate(days):
        record_failed_partition(root, "tw_minute:2317", day, DataError("not_ready: fixture"))
        path = _partition_paths(root, "tw_minute:2317", day)[1]
        attempt = json.loads(path.read_text())
        # Expired source-cycle evidence is eligible even for an older fixture
        # date; the last day is fresh and must not be included in this retry.
        old = datetime.now(UTC) - timedelta(days=2) + timedelta(seconds=offset)
        at = datetime.now(UTC) if day == days[-1] else old
        attempt["attempted_at_utc"] = at.isoformat()
        path.write_text(json.dumps(attempt))
        market._record(db, "tw_minute", "2317", day, "partition_not_ready")
        db.execute("UPDATE partitions SET checked_at_utc=? WHERE kind='tw_minute' AND symbol='2317' AND trade_date=?",
                   (at.isoformat(), day.isoformat()))
        db.commit()
    db.close()
    calls = []

    def fetch(root, key, targets, **kw):
        calls.append((key, targets))
        return fake_range(root, key, targets, **kw)

    result = run(universe, limit=4, fetch=fetch, queue_depth=4)
    assert result["successes_this_run"] == 4
    assert calls == [("tw_minute:2317", days[:-1])]


def test_live_rate_limit_is_persisted_even_after_worker_exception(universe, monkeypatch):
    from stockagent.data.finlab_transport import RateGate
    shared = None

    def pool(workers, **kw):
        nonlocal shared
        shared = kw["rate_gate"]
        return ThreadPool(workers)

    def fail(root, key, days, **kw):
        RateGate(shared).defer(900)
        raise RuntimeError("fixture interrupted after Retry-After")

    monkeypatch.setattr(market, "_minute_pool", pool)
    with pytest.raises(RuntimeError):
        run(universe, fetch=fail, queue_depth=4)
    payload = json.loads((universe[0] / "intraday/source_backoff.json").read_text())
    assert datetime.fromisoformat(payload["retry_at_utc"]) > datetime.now(UTC) + timedelta(minutes=14)
    assert payload["rate_limit_events"] >= 1
    assert json.loads((universe[0] / "intraday/minute_cursor.json").read_text())["recent_queue"][0]["next"] == 0


def test_parallel_ranges_keep_single_owner_index_and_per_day_receipts(universe):
    root, _, _ = universe
    result = run(universe)
    assert result["attempted_this_run"] == 8 and result["successes_this_run"] == 8
    batch = json.loads((root / "intraday/batch_metrics.json").read_text())["batches"][-1]
    assert batch["sdk_calls"] < 8 and batch["pipeline_workers"] == 4
    assert batch["completed_attempts"] == 8 and batch["new_receipted_partitions"] == 8
    assert result["by_kind"]["tw_minute"]["receipted_partitions"] == 9


def test_last_partial_wave_is_not_discarded_when_frontier_is_exhausted(universe):
    root, _, _ = universe
    result = run(universe, limit=64)
    assert result["successes_this_run"] == 9
    assert result["by_kind"]["tw_minute"]["receipted_partitions"] == 10


def test_near_floor_uses_one_inflight_day_not_a_parallel_range(universe, monkeypatch):
    monkeypatch.setattr(market, "quota_room_mb", lambda: (55, 5000))
    calls = []

    def fetch(root, key, days, **kw):
        calls.append(days)
        return fake_range(root, key, days, **kw)

    result = run(universe, limit=4, fetch=fetch)
    assert result["successes_this_run"] == 4
    assert all(len(days) == 1 for days in calls) and len(calls) == 4


def test_interruption_keeps_pre_wave_cursor_and_reuses_worker_receipt(universe):
    root, _, _ = universe

    def crash(root, key, days, **kw):
        persist_partition_frame(root, key, days[0], frame(key.split(":")[1], days[:1]))
        raise RuntimeError("fixture interrupted worker")

    with pytest.raises(RuntimeError, match="fixture interrupted worker"):
        run(universe, workers=1, fetch=crash)
    cursor = json.loads((root / "intraday/minute_cursor.json").read_text())
    assert cursor["recent_queue"][0]["next"] == 0
    assert json.loads((root / "intraday/active_run.json").read_text())["state"] == "interrupted"
    called = []

    def resumed(root, key, days, **kw):
        assert all(_stored_receipt(_partition_paths(root, key, day)[0], root, key, day) is None for day in days)
        called.extend((key, day) for day in days)
        return fake_range(root, key, days, **kw)

    result = run(universe, fetch=resumed)
    assert result["successes_this_run"] == 8
    assert len(called) == len(set(called))


def test_sdk_worker_session_updates_stay_in_memory(monkeypatch):
    from finlab import auth
    monkeypatch.setattr(auth, "get_session", auth.get_session)
    monkeypatch.setattr(auth, "save_session", lambda value: pytest.fail("no shared credential writes"))
    original = {"session_id": "fixture", "id_token": "fixture-not-a-secret"}
    initialize_intraday_worker(original, keepalive=False)
    updated = {**auth.get_session(), "id_token": "new-fixture"}
    auth.save_session(updated)
    assert auth.get_session()["id_token"] == "new-fixture"
    assert original["id_token"] == "fixture-not-a-secret"


def test_worker_keepalive_reuses_one_session_preserves_timeout_and_has_no_extra_retries():
    session = Mock()
    adapter = Mock(return_value="fixture-adapter")
    requests = SimpleNamespace(Session=Mock(return_value=session), adapters=SimpleNamespace(HTTPAdapter=adapter),
                               exceptions="fixture-exceptions")
    transport = WorkerHTTPTransport(requests)
    transport.get("https://fixture.invalid/object", timeout=120)
    transport.post("https://fixture.invalid/manifest", json={"dataset": "fixture"}, timeout=60)
    requests.Session.assert_called_once_with()
    adapter.assert_called_once_with(pool_connections=4, pool_maxsize=1, pool_block=True, max_retries=0)
    session.get.assert_called_once_with("https://fixture.invalid/object", timeout=120)
    session.post.assert_called_once_with("https://fixture.invalid/manifest", json={"dataset": "fixture"}, timeout=60)
    assert session.cookies.clear.call_count == 2 and session.mount.call_count == 2
    assert transport.exceptions == "fixture-exceptions"


def test_partial_fallback_rewinds_frontier_without_retrying_received_days(universe):
    root, _, _ = universe
    called = []

    def partial(root, key, days, **kw):
        called.append((key, days))
        if len(days) == 1:
            return fake_range(root, key, days, **kw)
        first = fake_range(root, key, days[:1], **kw)
        first["outcomes"].append({"trade_date": str(days[1]), "failure": "quota_exhausted"})
        return first

    result = run(universe, workers=1, limit=9, fetch=partial)
    assert result["state"] == "quota_exhausted"
    assert any(len(days) > 2 for _, days in called)
    cursor = json.loads((root / "intraday/minute_cursor.json").read_text())
    assert cursor["newest"] < 10
    resumed = []

    def missing_only(root, key, days, **kw):
        assert all(_stored_receipt(_partition_paths(root, key, day)[0], root, key, day) is None for day in days)
        resumed.extend((key, day) for day in days)
        return fake_range(root, key, days, **kw)

    run(universe, fetch=missing_only)
    assert len(resumed) == len(set(resumed))
    assert any((key, day) in resumed for key, days in called for day in days[2:])


def test_account_failure_cannot_advance_frontier_even_with_all_failure_outcomes(universe):
    root, _, _ = universe

    def denied(root, key, days, **kw):
        return {"outcomes": [{"trade_date": str(day), "failure": "quota_exhausted"} for day in days],
                "sdk_calls": 1, "fallback_calls": 0, "source_seconds": 0.01, "persist_seconds": 0.0}

    result = run(universe, fetch=denied)
    assert result["state"] == "quota_exhausted"
    assert json.loads((root / "intraday/minute_cursor.json").read_text())["recent_queue"][0]["next"] == 0


def test_hung_window_terminates_pool_keeps_cursor_and_reuses_saved_receipt(universe, monkeypatch):
    root, _, _ = universe
    clock = [0.0]
    lifecycle = []

    class Pending:
        def ready(self):
            clock[0] += 200  # deterministic deadline; no real timeout wait
            return False

    class Pool:
        def apply_async(self, fetch, args, kwargs):
            store_root, key, days = args
            persist_partition_frame(store_root, key, days[0], frame(key.split(":")[1], days[:1]))
            return Pending()

        def terminate(self):
            lifecycle.append("terminate")

        def join(self):
            lifecycle.append("join")

    monkeypatch.setattr(market, "_minute_pool", lambda _, **kw: Pool())
    monkeypatch.setattr(market.time, "monotonic", lambda: clock[0])
    result = run(universe)
    assert result["state"] == "window_time_budget_reached" and result["attempted_this_run"] == 0
    assert lifecycle == ["terminate", "join"]
    assert json.loads((root / "intraday/minute_cursor.json").read_text())["recent_queue"][0]["next"] == 0
    assert any((root / "intraday/receipts").rglob("*.json"))


def test_worker_initialization_failure_cannot_commit_unrequested_frontier(universe, monkeypatch):
    root, _, _ = universe

    def unavailable(_, **kw):
        raise RuntimeError("fixture auth initialization failed")

    monkeypatch.setattr(market, "_minute_pool", unavailable)
    with pytest.raises(RuntimeError, match="fixture auth initialization failed"):
        run(universe)
    assert json.loads((root / "intraday/minute_cursor.json").read_text())["recent_queue"][0]["next"] == 0
    assert json.loads((root / "intraday/active_run.json").read_text())["state"] == "interrupted"


def test_due_general_work_preempts_next_wave_not_completed_results(universe, monkeypatch):
    wall = [datetime.now(UTC)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return wall[0].astimezone(tz) if tz else wall[0].replace(tzinfo=None)

    admissions = []

    def admission(*a, **kw):
        admissions.append(kw["now"])
        return {"minute_residual_allowed": len(admissions) == 1}

    def fetch(*a, **kw):
        result = fake_range(*a, **kw)
        wall[0] += timedelta(minutes=2)
        return result

    monkeypatch.setattr(market, "datetime", Clock)
    monkeypatch.setattr(market, "_general_work_admission", admission)
    result = run(universe, workers=1, limit=9, fetch=fetch)
    assert result["state"] == "waiting_required_general_data"
    assert result["successes_this_run"] == 1 and len(admissions) == 2
